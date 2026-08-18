//! 有界、去重且使用单 consumer 的仲裁事件调度器。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::ArbitrationMode;

use super::{AnalysisJob, AnalysisState, ArbitrationService};

const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const MAX_RETRY_EXPONENT: u32 = 16;

#[derive(Clone)]
pub struct ArbitrationScheduler {
    sender: mpsc::Sender<AnalysisJob>,
    scheduled: Arc<Mutex<BTreeSet<AnalysisJob>>>,
    waiting: Arc<Mutex<BTreeSet<AnalysisJob>>>,
    recovery_wake: Arc<Notify>,
    capacity: usize,
}

#[derive(Clone)]
struct ArbitrationQueueState {
    scheduled: Arc<Mutex<BTreeSet<AnalysisJob>>>,
    waiting: Arc<Mutex<BTreeSet<AnalysisJob>>>,
    recovery_wake: Arc<Notify>,
    capacity: usize,
}

impl ArbitrationScheduler {
    /// 返回 false 表示同一分析已经排队、正在执行或等待恢复重试；返回 true 表示
    /// 已直接入队，或队列已满但已用持久化 Analysis 唤醒同进程恢复扫描。
    ///
    /// 这里不能等待 channel 容量：调用方在 enqueue 前已经持久化 Analysis，若 HTTP
    /// future 因客户端取消而被 drop，磁盘记录仍必须由 scheduler 在本进程内接管。
    pub async fn enqueue(&self, job: AnalysisJob) -> anyhow::Result<bool> {
        let mut wake_on_cancel = DurableRecoveryWake::new(self);
        {
            let scheduled = self.scheduled.lock().await;
            let waiting = self.waiting.lock().await;
            if scheduled.contains(&job) || waiting.contains(&job) {
                wake_on_cancel.disarm();
                return Ok(false);
            }
            if scheduled.len() >= self.capacity {
                return Ok(true);
            }
        }

        let permit = match self.sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => return Ok(true),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                wake_on_cancel.disarm();
                return Err(anyhow::anyhow!("arbitration scheduler 已关闭"));
            }
        };
        let mut scheduled = self.scheduled.lock().await;
        let waiting = self.waiting.lock().await;
        if scheduled.contains(&job) || waiting.contains(&job) {
            wake_on_cancel.disarm();
            return Ok(false);
        }
        if scheduled.len() >= self.capacity {
            return Ok(true);
        }
        scheduled.insert(job.clone());
        permit.send(job);
        wake_on_cancel.disarm();
        Ok(true)
    }

    /// 唤醒同进程的持久 Analysis 扫描；调用方必须已经完成 Analysis 落盘。
    pub(crate) fn wake_durable_recovery(&self) {
        self.recovery_wake.notify_one();
    }
}

/// enqueue 在等待异步锁时可能随请求 future 一起被取消。只要尚未确认已有占位或完成
/// 直投，drop 就把已持久化 Analysis 交给恢复扫描，避免必须重启进程才能继续。
struct DurableRecoveryWake<'a> {
    scheduler: &'a ArbitrationScheduler,
    armed: bool,
}

impl<'a> DurableRecoveryWake<'a> {
    fn new(scheduler: &'a ArbitrationScheduler) -> Self {
        Self {
            scheduler,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DurableRecoveryWake<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.scheduler.wake_durable_recovery();
        }
    }
}

/// capacity 只限制待处理事件数；consumer 固定为一个，因此不会产生无界模型并发。
pub fn spawn_arbitration_scheduler(
    service: Arc<ArbitrationService>,
    capacity: usize,
    cancel: CancellationToken,
) -> (ArbitrationScheduler, JoinHandle<anyhow::Result<()>>) {
    spawn_scheduler(
        service,
        capacity,
        cancel,
        RetryPolicy::new(RETRY_INITIAL_DELAY, RETRY_MAX_DELAY),
    )
}

#[derive(Debug, Clone, Copy)]
struct RetryPolicy {
    initial_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            initial_delay,
            max_delay: max_delay.max(initial_delay),
        }
    }

    fn delay(self, retry_attempt: u32) -> Duration {
        let exponent = retry_attempt.min(MAX_RETRY_EXPONENT);
        let factor = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
        self.initial_delay
            .saturating_mul(factor)
            .min(self.max_delay)
    }
}

#[derive(Debug)]
struct PendingJob {
    job: AnalysisJob,
    retry_attempt: u32,
    holds_capacity: bool,
}

#[async_trait]
trait AnalysisJobProcessor: Send + Sync + 'static {
    async fn recoverable_jobs(&self) -> anyhow::Result<Vec<AnalysisJob>>;

    async fn process_analysis(
        &self,
        job: &AnalysisJob,
        cancel: &CancellationToken,
    ) -> anyhow::Result<AnalysisState>;

    async fn is_persistently_recoverable(&self, job: &AnalysisJob) -> anyhow::Result<bool>;

    async fn persisted_retry_delay(
        &self,
        _job: &AnalysisJob,
        _state: AnalysisState,
    ) -> anyhow::Result<Option<Duration>> {
        Ok(None)
    }
}

#[async_trait]
impl AnalysisJobProcessor for ArbitrationService {
    async fn recoverable_jobs(&self) -> anyhow::Result<Vec<AnalysisJob>> {
        ArbitrationService::recoverable_jobs(self).await
    }

    async fn process_analysis(
        &self,
        job: &AnalysisJob,
        cancel: &CancellationToken,
    ) -> anyhow::Result<AnalysisState> {
        ArbitrationService::process_analysis(self, job, cancel)
            .await
            .map(|analysis| analysis.state)
    }

    async fn is_persistently_recoverable(&self, job: &AnalysisJob) -> anyhow::Result<bool> {
        if !self.store().is_current_analysis_job(job).await? {
            return Ok(false);
        }
        let analysis = self.store().read_analysis(job).await?;
        Ok(analysis.state.is_recoverable()
            || (analysis.state == AnalysisState::Approved
                && analysis.mode == ArbitrationMode::Auto
                && analysis.adoption_blocked_reason.is_none()))
    }

    async fn persisted_retry_delay(
        &self,
        job: &AnalysisJob,
        state: AnalysisState,
    ) -> anyhow::Result<Option<Duration>> {
        ArbitrationService::persisted_retry_delay(self, job, state).await
    }
}

fn spawn_scheduler<P: AnalysisJobProcessor>(
    processor: Arc<P>,
    capacity: usize,
    cancel: CancellationToken,
    retry_policy: RetryPolicy,
) -> (ArbitrationScheduler, JoinHandle<anyhow::Result<()>>) {
    let capacity = capacity.max(1);
    let (sender, receiver) = mpsc::channel(capacity);
    let scheduled = Arc::new(Mutex::new(BTreeSet::new()));
    let waiting = Arc::new(Mutex::new(BTreeSet::new()));
    let recovery_wake = Arc::new(Notify::new());
    let scheduler = ArbitrationScheduler {
        sender,
        scheduled: scheduled.clone(),
        waiting: waiting.clone(),
        recovery_wake: recovery_wake.clone(),
        capacity,
    };
    let handle = tokio::spawn(run_scheduler(
        processor,
        receiver,
        ArbitrationQueueState {
            scheduled,
            waiting,
            recovery_wake,
            capacity,
        },
        cancel,
        retry_policy,
    ));
    (scheduler, handle)
}

async fn run_scheduler<P: AnalysisJobProcessor>(
    processor: Arc<P>,
    mut receiver: mpsc::Receiver<AnalysisJob>,
    queue: ArbitrationQueueState,
    cancel: CancellationToken,
    retry_policy: RetryPolicy,
) -> anyhow::Result<()> {
    let startup_result = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            shutdown(&mut receiver, &queue.scheduled, &queue.waiting).await;
            return Ok(());
        }
        jobs = processor.recoverable_jobs() => jobs,
    };
    let startup = match startup_result {
        Ok(jobs) => jobs,
        Err(error) => {
            shutdown(&mut receiver, &queue.scheduled, &queue.waiting).await;
            return Err(error);
        }
    };
    let mut ready = VecDeque::new();
    let mut recovery_requested = false;
    for job in startup {
        // report_dispute 可能在启动扫描期间已经把同一 job 放进 channel。
        // 只有成功取得 scheduled 占位的一方负责执行。
        let mut scheduled_jobs = queue.scheduled.lock().await;
        if scheduled_jobs.len() >= queue.capacity {
            recovery_requested = true;
            continue;
        }
        if scheduled_jobs.insert(job.clone()) {
            drop(scheduled_jobs);
            ready.push_back(PendingJob {
                job,
                retry_attempt: 0,
                holds_capacity: true,
            });
        }
    }
    let mut delayed: BTreeMap<Instant, VecDeque<PendingJob>> = BTreeMap::new();
    let mut receiver_open = true;

    loop {
        if cancel.is_cancelled() {
            break;
        }
        move_due_retries(&mut delayed, &mut ready);

        if recovery_requested && queue.scheduled.lock().await.len() < queue.capacity {
            recovery_requested = refill_from_durable(
                processor.as_ref(),
                &queue.scheduled,
                &queue.waiting,
                &mut ready,
                queue.capacity,
            )
            .await?;
        }

        if let Some(mut pending) = ready.pop_front() {
            if !pending.holds_capacity {
                if !claim_waiting_capacity(
                    &queue.scheduled,
                    &queue.waiting,
                    &pending.job,
                    queue.capacity,
                )
                .await
                {
                    delayed
                        .entry(Instant::now() + retry_policy.initial_delay)
                        .or_default()
                        .push_back(pending);
                    recovery_requested = true;
                    continue;
                }
                pending.holds_capacity = true;
            }
            let retry_delay = run_one(processor.as_ref(), &pending.job, &cancel).await;
            if cancel.is_cancelled() {
                break;
            }
            if let Some(persisted_delay) = retry_delay {
                let delay =
                    persisted_delay.unwrap_or_else(|| retry_policy.delay(pending.retry_attempt));
                pending.retry_attempt = pending.retry_attempt.saturating_add(1);
                if persisted_delay.is_some() {
                    park_persisted_wait(&queue.scheduled, &queue.waiting, &pending.job).await;
                    pending.holds_capacity = false;
                    recovery_requested = true;
                }
                log::warn!(
                    target: "maintainer_arbitration",
                    "dispute={} analysis={} 将在 {:?} 后恢复重试",
                    pending.job.dispute_id,
                    pending.job.analysis_id,
                    delay
                );
                delayed
                    .entry(Instant::now() + delay)
                    .or_default()
                    .push_back(pending);
            } else {
                queue.scheduled.lock().await.remove(&pending.job);
            }
            continue;
        }

        let next_retry = delayed.keys().next().copied();
        match (receiver_open, next_retry) {
            (true, Some(deadline)) => {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    job = receiver.recv() => match job {
                        Some(job) => ready.push_back(PendingJob { job, retry_attempt: 0, holds_capacity: true }),
                        None => receiver_open = false,
                    },
                    _ = queue.recovery_wake.notified() => recovery_requested = true,
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            (true, None) => {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    job = receiver.recv() => match job {
                        Some(job) => ready.push_back(PendingJob { job, retry_attempt: 0, holds_capacity: true }),
                        None => receiver_open = false,
                    },
                    _ = queue.recovery_wake.notified() => recovery_requested = true,
                }
            }
            (false, Some(deadline)) => {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = queue.recovery_wake.notified() => recovery_requested = true,
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            }
            (false, None) => break,
        }
    }

    shutdown(&mut receiver, &queue.scheduled, &queue.waiting).await;
    Ok(())
}

async fn refill_from_durable<P: AnalysisJobProcessor>(
    processor: &P,
    scheduled: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
    waiting: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
    ready: &mut VecDeque<PendingJob>,
    capacity: usize,
) -> anyhow::Result<bool> {
    let jobs = processor.recoverable_jobs().await?;
    let mut more_pending = false;
    for job in jobs {
        let mut scheduled_jobs = scheduled.lock().await;
        let waiting_jobs = waiting.lock().await;
        if scheduled_jobs.contains(&job) || waiting_jobs.contains(&job) {
            continue;
        }
        if scheduled_jobs.len() >= capacity {
            more_pending = true;
            continue;
        }
        scheduled_jobs.insert(job.clone());
        drop(scheduled_jobs);
        ready.push_back(PendingJob {
            job,
            retry_attempt: 0,
            holds_capacity: true,
        });
    }
    Ok(more_pending)
}

async fn park_persisted_wait(
    scheduled: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
    waiting: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
    job: &AnalysisJob,
) {
    let mut scheduled = scheduled.lock().await;
    let mut waiting = waiting.lock().await;
    scheduled.remove(job);
    waiting.insert(job.clone());
}

async fn claim_waiting_capacity(
    scheduled: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
    waiting: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
    job: &AnalysisJob,
    capacity: usize,
) -> bool {
    let mut scheduled = scheduled.lock().await;
    let mut waiting = waiting.lock().await;
    if scheduled.len() >= capacity {
        return false;
    }
    if !waiting.remove(job) {
        return false;
    }
    scheduled.insert(job.clone());
    true
}

fn move_due_retries(
    delayed: &mut BTreeMap<Instant, VecDeque<PendingJob>>,
    ready: &mut VecDeque<PendingJob>,
) {
    let now = Instant::now();
    let due: Vec<Instant> = delayed
        .range(..=now)
        .map(|(deadline, _)| *deadline)
        .collect();
    for deadline in due {
        if let Some(mut jobs) = delayed.remove(&deadline) {
            ready.append(&mut jobs);
        }
    }
}

async fn shutdown(
    receiver: &mut mpsc::Receiver<AnalysisJob>,
    scheduled: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
    waiting: &Arc<Mutex<BTreeSet<AnalysisJob>>>,
) {
    // close 会先唤醒所有等待 reserve 的 producer；清空集合保证关闭后的调用得到
    // “scheduler 已关闭”，而不是被遗留占位误报成重复 job。
    receiver.close();
    scheduled.lock().await.clear();
    waiting.lock().await.clear();
}

async fn run_one<P: AnalysisJobProcessor>(
    processor: &P,
    job: &AnalysisJob,
    cancel: &CancellationToken,
) -> Option<Option<Duration>> {
    match processor.process_analysis(job, cancel).await {
        Ok(state) if state.is_recoverable() => {
            match processor.persisted_retry_delay(job, state).await {
                Ok(delay) => Some(delay),
                Err(error) => {
                    log::warn!(target: "maintainer_arbitration", "读取 analysis 持久重试时间失败，将按恢复退避重试: {error:#}");
                    Some(None)
                }
            }
        }
        Ok(_) => None,
        Err(error) => {
            log::warn!(
                target: "maintainer_arbitration",
                "处理 dispute={} analysis={} 失败: {error:#}",
                job.dispute_id,
                job.analysis_id
            );
            if cancel.is_cancelled() {
                return None;
            }
            match processor.is_persistently_recoverable(job).await {
                Ok(true) => Some(None),
                Ok(false) => None,
                Err(read_error) => {
                    // 无法确认持久状态通常也是临时 I/O 故障。保留 scheduled 占位并
                    // 退避重试，避免一次磁盘抖动让可恢复 adoption 永久丢失。
                    log::warn!(
                        target: "maintainer_arbitration",
                        "确认 dispute={} analysis={} 恢复状态失败，将保守重试: {read_error:#}",
                        job.dispute_id,
                        job.analysis_id
                    );
                    Some(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::claim::DisputeId;
    use crate::maintainer::arbitration::ArbitrationAnalysisId;

    fn job() -> AnalysisJob {
        AnalysisJob {
            dispute_id: DisputeId::random(),
            analysis_id: ArbitrationAnalysisId::random(),
        }
    }

    #[tokio::test]
    async fn queue_is_bounded_and_deduplicates_scheduled_jobs() {
        let (sender, mut receiver) = mpsc::channel(1);
        let scheduled = Arc::new(Mutex::new(BTreeSet::new()));
        let scheduler = ArbitrationScheduler {
            sender,
            scheduled: scheduled.clone(),
            waiting: Arc::new(Mutex::new(BTreeSet::new())),
            recovery_wake: Arc::new(Notify::new()),
            capacity: 1,
        };
        let first = job();
        assert!(scheduler.enqueue(first.clone()).await.unwrap());
        assert!(!scheduler.enqueue(first.clone()).await.unwrap());

        let second = job();
        let accepted =
            tokio::time::timeout(Duration::from_millis(50), scheduler.enqueue(second.clone()))
                .await
                .expect("满队列不能阻塞 durable 请求")
                .unwrap();
        assert!(accepted);
        assert_eq!(scheduled.lock().await.len(), 1);
        assert!(!scheduled.lock().await.contains(&second));

        assert_eq!(receiver.recv().await, Some(first));
        scheduled.lock().await.clear();
        assert!(scheduler.enqueue(second.clone()).await.unwrap());
        assert_eq!(receiver.recv().await, Some(second));
        assert_eq!(scheduled.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn closed_scheduler_returns_error_without_a_dedupe_ghost() {
        let (sender, receiver) = mpsc::channel(1);
        let scheduled = Arc::new(Mutex::new(BTreeSet::new()));
        let scheduler = ArbitrationScheduler {
            sender,
            scheduled: scheduled.clone(),
            waiting: Arc::new(Mutex::new(BTreeSet::new())),
            recovery_wake: Arc::new(Notify::new()),
            capacity: 1,
        };
        drop(receiver);
        let pending = job();
        assert!(scheduler.enqueue(pending.clone()).await.is_err());
        assert!(!scheduled.lock().await.contains(&pending));
    }

    #[test]
    fn retry_delay_grows_exponentially_and_is_capped() {
        let policy = RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(4));
        assert_eq!(policy.delay(0), Duration::from_secs(1));
        assert_eq!(policy.delay(1), Duration::from_secs(2));
        assert_eq!(policy.delay(2), Duration::from_secs(4));
        assert_eq!(policy.delay(100), Duration::from_secs(4));
    }

    #[derive(Debug, Clone, Copy)]
    enum FakeStep {
        State(AnalysisState),
        Error,
    }

    struct FakeProcessor {
        startup: Vec<AnalysisJob>,
        steps: Mutex<VecDeque<FakeStep>>,
        calls: AtomicUsize,
        recoverable_after_error: AtomicBool,
    }

    impl FakeProcessor {
        fn new(startup: Vec<AnalysisJob>, steps: impl IntoIterator<Item = FakeStep>) -> Self {
            Self {
                startup,
                steps: Mutex::new(steps.into_iter().collect()),
                calls: AtomicUsize::new(0),
                recoverable_after_error: AtomicBool::new(true),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl AnalysisJobProcessor for FakeProcessor {
        async fn recoverable_jobs(&self) -> anyhow::Result<Vec<AnalysisJob>> {
            Ok(self.startup.clone())
        }

        async fn process_analysis(
            &self,
            _job: &AnalysisJob,
            _cancel: &CancellationToken,
        ) -> anyhow::Result<AnalysisState> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.steps.lock().await.pop_front() {
                Some(FakeStep::State(state)) => Ok(state),
                Some(FakeStep::Error) => anyhow::bail!("temporary failure"),
                None => Ok(AnalysisState::Adopted),
            }
        }

        async fn is_persistently_recoverable(&self, _job: &AnalysisJob) -> anyhow::Result<bool> {
            Ok(self.recoverable_after_error.load(Ordering::SeqCst))
        }
    }

    async fn wait_for_calls(processor: &FakeProcessor, expected: usize) {
        for _ in 0..100 {
            if processor.calls() >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "processor calls 未达到 {expected}，实际 {}",
            processor.calls()
        );
    }

    struct DurableBlockingProcessor {
        recoverable: Mutex<BTreeSet<AnalysisJob>>,
        calls: Mutex<Vec<AnalysisJob>>,
        recovery_scans: AtomicUsize,
        block_first: bool,
        first_started: Notify,
        release_first: Notify,
    }

    #[async_trait]
    impl AnalysisJobProcessor for DurableBlockingProcessor {
        async fn recoverable_jobs(&self) -> anyhow::Result<Vec<AnalysisJob>> {
            let jobs = self.recoverable.lock().await.iter().cloned().collect();
            self.recovery_scans.fetch_add(1, Ordering::SeqCst);
            Ok(jobs)
        }

        async fn process_analysis(
            &self,
            job: &AnalysisJob,
            _cancel: &CancellationToken,
        ) -> anyhow::Result<AnalysisState> {
            let is_first = {
                let mut calls = self.calls.lock().await;
                calls.push(job.clone());
                calls.len() == 1
            };
            if is_first {
                self.first_started.notify_one();
                if self.block_first {
                    self.release_first.notified().await;
                }
            }
            self.recoverable.lock().await.remove(job);
            Ok(AnalysisState::Adopted)
        }

        async fn is_persistently_recoverable(&self, job: &AnalysisJob) -> anyhow::Result<bool> {
            Ok(self.recoverable.lock().await.contains(job))
        }
    }

    #[tokio::test]
    async fn full_queue_defers_to_durable_backlog_and_recovers_without_restart() {
        let first = job();
        let second = job();
        let processor = Arc::new(DurableBlockingProcessor {
            recoverable: Mutex::new(BTreeSet::from([first.clone()])),
            calls: Mutex::new(Vec::new()),
            recovery_scans: AtomicUsize::new(0),
            block_first: true,
            first_started: Notify::new(),
            release_first: Notify::new(),
        });
        let cancel = CancellationToken::new();
        let (scheduler, handle) = spawn_scheduler(
            processor.clone(),
            1,
            cancel.clone(),
            RetryPolicy::new(Duration::from_millis(10), Duration::from_millis(20)),
        );
        processor.first_started.notified().await;

        // 模拟 HTTP handler 已经先把第二条 Analysis 持久化，再尝试唤醒满队列。
        processor.recoverable.lock().await.insert(second.clone());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), scheduler.enqueue(second.clone()))
                .await
                .expect("满队列的 enqueue 必须立即返回")
                .unwrap()
        );
        assert!(!scheduler.scheduled.lock().await.contains(&second));

        processor.release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if processor.calls.lock().await.len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("durable backlog 应在同一进程释放槽位后被扫描执行");
        assert_eq!(processor.calls.lock().await.as_slice(), &[first, second]);

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn aborted_enqueue_while_scheduled_lock_is_contended_wakes_durable_recovery() {
        let pending = job();
        let processor = Arc::new(DurableBlockingProcessor {
            recoverable: Mutex::new(BTreeSet::new()),
            calls: Mutex::new(Vec::new()),
            recovery_scans: AtomicUsize::new(0),
            block_first: false,
            first_started: Notify::new(),
            release_first: Notify::new(),
        });
        let cancel = CancellationToken::new();
        let (scheduler, handle) = spawn_scheduler(
            processor.clone(),
            1,
            cancel.clone(),
            RetryPolicy::new(Duration::from_millis(10), Duration::from_millis(20)),
        );
        while processor.recovery_scans.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }

        // 模拟 handler 已持久化 Analysis，却在等待 scheduled 锁时随请求一起被取消。
        let scheduled_guard = scheduler.scheduled.lock().await;
        processor.recoverable.lock().await.insert(pending.clone());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let enqueue_scheduler = scheduler.clone();
        let enqueue_job = pending.clone();
        let enqueue_handle = tokio::spawn(async move {
            let _ = started_tx.send(());
            enqueue_scheduler.enqueue(enqueue_job).await
        });
        started_rx.await.unwrap();
        enqueue_handle.abort();
        assert!(enqueue_handle.await.unwrap_err().is_cancelled());
        drop(scheduled_guard);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if processor.calls.lock().await.as_slice() == [pending.clone()] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("被取消的 enqueue 应唤醒同进程 durable backlog 扫描");

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn temporary_error_retries_in_process_after_bounded_backoff() {
        let pending = job();
        let processor = Arc::new(FakeProcessor::new(
            vec![pending.clone()],
            [FakeStep::Error, FakeStep::State(AnalysisState::Adopted)],
        ));
        let cancel = CancellationToken::new();
        let (scheduler, handle) = spawn_scheduler(
            processor.clone(),
            1,
            cancel.clone(),
            RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(4)),
        );

        wait_for_calls(&processor, 1).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.calls(), 1, "恢复态不能热循环");
        tokio::time::advance(Duration::from_secs(1)).await;
        wait_for_calls(&processor, 2).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!scheduler.scheduled.lock().await.contains(&pending));

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn adopting_delivery_pending_retries_without_hot_loop() {
        let pending = job();
        let processor = Arc::new(FakeProcessor::new(
            vec![pending.clone()],
            [
                FakeStep::State(AnalysisState::Adopting),
                FakeStep::State(AnalysisState::Adopted),
            ],
        ));
        let cancel = CancellationToken::new();
        let (scheduler, handle) = spawn_scheduler(
            processor.clone(),
            1,
            cancel.clone(),
            RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(4)),
        );

        wait_for_calls(&processor, 1).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.calls(), 1, "delivery pending 不能热循环");
        assert!(scheduler.scheduled.lock().await.contains(&pending));
        tokio::time::advance(Duration::from_secs(1)).await;
        wait_for_calls(&processor, 2).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!scheduler.scheduled.lock().await.contains(&pending));

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    struct PersistedDelayProcessor {
        delayed_job: AnalysisJob,
        ready_job: AnalysisJob,
        calls: Mutex<Vec<AnalysisJob>>,
    }

    #[async_trait]
    impl AnalysisJobProcessor for PersistedDelayProcessor {
        async fn recoverable_jobs(&self) -> anyhow::Result<Vec<AnalysisJob>> {
            Ok(vec![self.delayed_job.clone(), self.ready_job.clone()])
        }

        async fn process_analysis(
            &self,
            job: &AnalysisJob,
            _cancel: &CancellationToken,
        ) -> anyhow::Result<AnalysisState> {
            let mut calls = self.calls.lock().await;
            let delayed_call_count = calls
                .iter()
                .filter(|called| *called == &self.delayed_job)
                .count();
            calls.push(job.clone());
            if job == &self.delayed_job && delayed_call_count == 0 {
                Ok(AnalysisState::WaitingReanalysis)
            } else {
                Ok(AnalysisState::Adopted)
            }
        }

        async fn is_persistently_recoverable(&self, job: &AnalysisJob) -> anyhow::Result<bool> {
            Ok(job == &self.delayed_job)
        }

        async fn persisted_retry_delay(
            &self,
            job: &AnalysisJob,
            state: AnalysisState,
        ) -> anyhow::Result<Option<Duration>> {
            if job == &self.delayed_job && state == AnalysisState::WaitingReanalysis {
                Ok(Some(Duration::from_secs(5 * 60)))
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn persisted_reanalysis_wait_recovers_on_startup_without_blocking_ready_jobs() {
        let delayed_job = job();
        let ready_job = job();
        let processor = Arc::new(PersistedDelayProcessor {
            delayed_job: delayed_job.clone(),
            ready_job: ready_job.clone(),
            calls: Mutex::new(Vec::new()),
        });
        let cancel = CancellationToken::new();
        let (scheduler, handle) = spawn_scheduler(
            processor.clone(),
            1,
            cancel.clone(),
            RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(4)),
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if processor.calls.lock().await.len() >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            processor.calls.lock().await.as_slice(),
            &[delayed_job.clone(), ready_job]
        );
        assert!(!scheduler.scheduled.lock().await.contains(&delayed_job));
        assert!(scheduler.waiting.lock().await.contains(&delayed_job));

        tokio::time::advance(Duration::from_secs(5 * 60)).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if processor.calls.lock().await.len() >= 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(processor.calls.lock().await[2], delayed_job);
        assert!(scheduler.scheduled.lock().await.is_empty());
        assert!(scheduler.waiting.lock().await.is_empty());

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn analysis_terminal_states_are_not_retried() {
        for terminal in [
            AnalysisState::Approved,
            AnalysisState::Unresolved,
            AnalysisState::Failed,
            AnalysisState::Adopted,
        ] {
            let pending = job();
            let processor = Arc::new(FakeProcessor::new(
                vec![pending],
                [FakeStep::State(terminal)],
            ));
            let cancel = CancellationToken::new();
            let (_scheduler, handle) = spawn_scheduler(
                processor.clone(),
                1,
                cancel.clone(),
                RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(4)),
            );
            wait_for_calls(&processor, 1).await;
            tokio::time::advance(Duration::from_secs(30)).await;
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            assert_eq!(processor.calls(), 1);
            cancel.cancel();
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn process_error_with_persisted_terminal_state_is_not_retried() {
        let pending = job();
        let processor = Arc::new(FakeProcessor::new(
            vec![pending],
            [FakeStep::Error, FakeStep::State(AnalysisState::Adopted)],
        ));
        processor
            .recoverable_after_error
            .store(false, Ordering::SeqCst);
        let cancel = CancellationToken::new();
        let (_scheduler, handle) = spawn_scheduler(
            processor.clone(),
            1,
            cancel.clone(),
            RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(4)),
        );

        wait_for_calls(&processor, 1).await;
        tokio::time::advance(Duration::from_secs(30)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.calls(), 1);

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_delayed_retry_and_clears_dedupe_slot() {
        let pending = job();
        let processor = Arc::new(FakeProcessor::new(
            vec![pending.clone()],
            [FakeStep::Error, FakeStep::State(AnalysisState::Adopted)],
        ));
        let cancel = CancellationToken::new();
        let (scheduler, handle) = spawn_scheduler(
            processor.clone(),
            1,
            cancel.clone(),
            RetryPolicy::new(Duration::from_secs(1), Duration::from_secs(4)),
        );
        wait_for_calls(&processor, 1).await;

        cancel.cancel();
        handle.await.unwrap().unwrap();
        assert!(scheduler.scheduled.lock().await.is_empty());
        assert!(scheduler.waiting.lock().await.is_empty());
        assert!(scheduler.enqueue(pending).await.is_err());
        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(processor.calls(), 1);
    }
}
