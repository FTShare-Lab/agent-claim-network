use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;

use super::activity::DelegationActivityHub;
use super::store::{DelegationListPage, DelegationStore, DelegationStoreError};
use super::types::{
    DelegationArtifactRef, DelegationCompactionEventKind, DelegationCompactionState,
    DelegationCreateRequest, DelegationId, DelegationMetadata, DelegationResult, DelegationStatus,
    DelegationSteering, DelegationSummary, DelegationTranscriptEntry, DelegationUpdate,
};
use crate::claim::SessionId;
use crate::config::{
    DEFAULT_SESSION_DELEGATION_MAX_CONCURRENT,
    DEFAULT_SESSION_DELEGATION_WAIT_DEFAULT_TIMEOUT_SECS,
    DEFAULT_SESSION_DELEGATION_WAIT_MAX_TIMEOUT_SECS,
    DEFAULT_SESSION_DELEGATION_WAIT_MIN_TIMEOUT_SECS, DEFAULT_SESSION_DELEGATION_WALL_TIMEOUT_SECS,
};

const INITIAL_STEERING_READ_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelegationRunnerConfig {
    pub max_concurrent: usize,
    pub wall_timeout: Duration,
    pub wait: DelegationWaitConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelegationWaitConfig {
    pub default_timeout: Duration,
    pub min_timeout: Duration,
    pub max_timeout: Duration,
}

impl Default for DelegationWaitConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(
                DEFAULT_SESSION_DELEGATION_WAIT_DEFAULT_TIMEOUT_SECS,
            ),
            min_timeout: Duration::from_secs(DEFAULT_SESSION_DELEGATION_WAIT_MIN_TIMEOUT_SECS),
            max_timeout: Duration::from_secs(DEFAULT_SESSION_DELEGATION_WAIT_MAX_TIMEOUT_SECS),
        }
    }
}

impl Default for DelegationRunnerConfig {
    fn default() -> Self {
        Self {
            max_concurrent: DEFAULT_SESSION_DELEGATION_MAX_CONCURRENT,
            wall_timeout: Duration::from_secs(DEFAULT_SESSION_DELEGATION_WALL_TIMEOUT_SECS),
            wait: DelegationWaitConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DelegationExecutionContext {
    pub metadata: DelegationMetadata,
    pub initial_steering: Vec<DelegationSteering>,
}

#[derive(Debug, Clone)]
pub struct DelegationExecutionOutcome {
    pub summary: String,
    pub changed_files: Vec<String>,
    pub artifacts: Vec<DelegationArtifactRef>,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct DelegationExecutionError {
    pub message: String,
}

impl DelegationExecutionError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait DelegationExecutor: Send + Sync {
    /// 在任务执行前建立只属于该 delegation 的运行期 checkpoint。
    async fn begin_task(
        &self,
        _context: &DelegationExecutionContext,
    ) -> Result<(), DelegationExecutionError> {
        Ok(())
    }

    async fn execute(
        &self,
        context: DelegationExecutionContext,
        progress: DelegationProgressSink,
    ) -> Result<DelegationExecutionOutcome, DelegationExecutionError>;

    /// 只有 executor 成功且权威 terminal result 已落盘时 `committed=true`。
    async fn finish_task(
        &self,
        _context: &DelegationExecutionContext,
        _committed: bool,
    ) -> Result<(), DelegationExecutionError> {
        Ok(())
    }
}

struct DelegationTaskCheckpointOnDrop {
    executor: Arc<dyn DelegationExecutor>,
    context: DelegationExecutionContext,
    armed: bool,
}

impl DelegationTaskCheckpointOnDrop {
    fn new(executor: Arc<dyn DelegationExecutor>, context: DelegationExecutionContext) -> Self {
        Self {
            executor,
            context,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for DelegationTaskCheckpointOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            log::warn!(
                target: "delegation_runner",
                "delegation {} dropped without Tokio runtime; task checkpoint cannot roll back",
                self.context.metadata.id
            );
            return;
        };
        let executor = Arc::clone(&self.executor);
        let context = self.context.clone();
        runtime.spawn(async move {
            if let Err(error) = executor.finish_task(&context, false).await {
                log::warn!(
                    target: "delegation_runner",
                    "delegation {} drop 后回滚 task checkpoint 失败: {error}",
                    context.metadata.id
                );
            }
        });
    }
}

#[derive(Clone)]
pub struct DelegationProgressSink {
    store: DelegationStore,
    id: DelegationId,
    activity: DelegationActivityHub,
}

impl DelegationProgressSink {
    #[cfg(test)]
    pub(crate) fn for_test(store: DelegationStore, id: DelegationId) -> Self {
        Self {
            store,
            id,
            activity: DelegationActivityHub::new(),
        }
    }

    pub async fn update(
        &self,
        current_step: Option<String>,
        summary: impl Into<String>,
        artifacts: Vec<DelegationArtifactRef>,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        let metadata = self
            .store
            .update_progress(
                &self.id,
                DelegationUpdate {
                    current_step,
                    summary: summary.into(),
                    artifacts,
                },
            )
            .await?;
        self.activity.publish();
        Ok(metadata)
    }

    pub async fn steering_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<DelegationSteering>, DelegationStoreError> {
        self.store
            .read_steering_after(&self.id, after_seq, limit)
            .await
    }

    pub async fn record_event(
        &self,
        kind: super::types::DelegationEventKind,
    ) -> Result<(), DelegationStoreError> {
        self.store.append_event(&self.id, kind).await
    }

    pub async fn append_transcript_entry(
        &self,
        entry: DelegationTranscriptEntry,
    ) -> Result<(), DelegationStoreError> {
        self.store.append_transcript_entry(&self.id, entry).await
    }

    pub async fn read_compaction_state(
        &self,
    ) -> Result<Option<DelegationCompactionState>, DelegationStoreError> {
        self.store.read_compaction_state(&self.id).await
    }

    pub async fn write_compaction_state(
        &self,
        state: &DelegationCompactionState,
    ) -> Result<(), DelegationStoreError> {
        self.store.write_compaction_state(&self.id, state).await
    }

    pub async fn append_compaction_event(
        &self,
        kind: DelegationCompactionEventKind,
    ) -> Result<(), DelegationStoreError> {
        self.store.append_compaction_event(&self.id, kind).await
    }

    pub async fn write_compaction_checkpoint(
        &self,
        value: &serde_json::Value,
    ) -> Result<(), DelegationStoreError> {
        self.store
            .write_compaction_checkpoint(&self.id, value)
            .await
    }

    pub async fn clear_compaction_checkpoint(&self) -> Result<(), DelegationStoreError> {
        self.store.clear_compaction_checkpoint(&self.id).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DelegationRunnerError {
    #[error(transparent)]
    Store(#[from] DelegationStoreError),
    #[error("subagent runner max_concurrent 必须大于 0")]
    InvalidConcurrency,
    #[error("subagent wait timeout 配置必须满足 0 < min <= default <= max")]
    InvalidWaitTimeouts,
    #[error("subagent creation interrupted before registration")]
    Interrupted,
    #[error("subagent registration task stopped before completion")]
    RegistrationTaskStopped,
}

#[derive(Clone)]
pub struct DelegationRunner {
    inner: Arc<DelegationRunnerInner>,
}

struct DelegationRunnerInner {
    store: DelegationStore,
    executor: Arc<dyn DelegationExecutor>,
    config: DelegationRunnerConfig,
    activity: DelegationActivityHub,
    state: tokio::sync::Mutex<RunnerState>,
    pump_lock: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct RunnerState {
    queued: VecDeque<DelegationId>,
    starting: usize,
    running: BTreeMap<DelegationId, JoinHandle<()>>,
    abandon_generation: u64,
}

impl DelegationRunner {
    pub fn new(
        store: DelegationStore,
        executor: Arc<dyn DelegationExecutor>,
        config: DelegationRunnerConfig,
    ) -> Result<Self, DelegationRunnerError> {
        if config.max_concurrent == 0 {
            return Err(DelegationRunnerError::InvalidConcurrency);
        }
        if config.wait.min_timeout.is_zero()
            || config.wait.default_timeout < config.wait.min_timeout
            || config.wait.default_timeout > config.wait.max_timeout
        {
            return Err(DelegationRunnerError::InvalidWaitTimeouts);
        }
        Ok(Self {
            inner: Arc::new(DelegationRunnerInner {
                store,
                executor,
                config,
                activity: DelegationActivityHub::new(),
                state: tokio::sync::Mutex::new(RunnerState::default()),
                pump_lock: tokio::sync::Mutex::new(()),
            }),
        })
    }

    pub fn store(&self) -> &DelegationStore {
        &self.inner.store
    }

    pub fn wait_config(&self) -> DelegationWaitConfig {
        self.inner.config.wait
    }

    pub fn subscribe_activity(&self) -> watch::Receiver<u64> {
        self.inner.activity.subscribe()
    }

    pub async fn create(
        &self,
        request: DelegationCreateRequest,
    ) -> Result<DelegationMetadata, DelegationRunnerError> {
        self.create_until_registered(request, None).await
    }

    /// 创建 metadata 到进入 runner queue 之间是 subagent 的登记事务。调用 tool future
    /// 即使被 Esc/Ctrl-C force-abort，独立 task 仍会把已写入但尚未登记的 metadata
    /// abandon，避免留下不可发现的 queued subagent；一旦入队即视为成功登记，后续
    /// parent turn cancel 不再回滚它（D20）。
    pub async fn create_cancellable(
        &self,
        request: DelegationCreateRequest,
        cancellation: Option<CancellationToken>,
    ) -> Result<DelegationMetadata, DelegationRunnerError> {
        let (result_tx, mut result_rx) = oneshot::channel();
        let runner = self.clone();
        let task_cancellation = cancellation.clone();
        tokio::spawn(async move {
            let result = runner
                .create_until_registered(request, task_cancellation)
                .await;
            let _ = result_tx.send(result);
        });
        let result = if let Some(cancellation) = cancellation {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(DelegationRunnerError::Interrupted),
                result = &mut result_rx => result,
            }
        } else {
            result_rx.await
        };
        result.map_err(|_| DelegationRunnerError::RegistrationTaskStopped)?
    }

    async fn create_until_registered(
        &self,
        request: DelegationCreateRequest,
        cancellation: Option<CancellationToken>,
    ) -> Result<DelegationMetadata, DelegationRunnerError> {
        if cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(DelegationRunnerError::Interrupted);
        }
        let metadata = self.inner.store.create(request).await?;
        self.inner.activity.publish();
        if cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            self.abandon_unregistered_creation(&metadata.id).await;
            return Err(DelegationRunnerError::Interrupted);
        }
        {
            let mut state = self.inner.state.lock().await;
            // 队列写入是 registration 的线性化点。检查与 push 在同一 critical section
            // 完成；取消在这之后到达即按“已成功登记，subagent 继续运行”处理。
            if cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                drop(state);
                self.abandon_unregistered_creation(&metadata.id).await;
                return Err(DelegationRunnerError::Interrupted);
            }
            state.queued.push_back(metadata.id.clone());
        }
        // pump 不属于 caller tool future；入队后的 subagent 对 parent explicit cancel
        // 具有独立所有权，不能因调用者 force-abort 而丢失启动机会。
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            inner.pump_queue().await;
        });
        Ok(metadata)
    }

    async fn abandon_unregistered_creation(&self, id: &DelegationId) {
        match self
            .inner
            .store
            .abandon(
                id,
                "parent turn cancelled before subagent registration".into(),
            )
            .await
        {
            Ok(_) => self.inner.activity.publish(),
            Err(error) => {
                log::warn!(
                    target: "delegation_runner",
                    "cancelled subagent creation cleanup failed id={id}: {error:#}"
                );
            }
        }
    }

    pub async fn list(&self) -> Result<Vec<DelegationSummary>, DelegationRunnerError> {
        self.inner.store.list().await.map_err(Into::into)
    }

    pub async fn list_page(
        &self,
        limit: usize,
    ) -> Result<DelegationListPage, DelegationRunnerError> {
        self.inner.store.list_page(limit).await.map_err(Into::into)
    }

    pub async fn steer(
        &self,
        id: &DelegationId,
        instruction: String,
    ) -> Result<DelegationMetadata, DelegationRunnerError> {
        let metadata = self
            .inner
            .store
            .steer(id, instruction)
            .await
            .map_err(DelegationRunnerError::from)?;
        self.inner.activity.publish();
        Ok(metadata)
    }

    pub async fn abandon_unfinished_for_session(
        &self,
        session_id: &SessionId,
        reason: &str,
    ) -> Result<Vec<DelegationMetadata>, DelegationRunnerError> {
        let _pump_guard = self.inner.pump_lock.lock().await;
        let result = self
            .inner
            .store
            .abandon_unfinished_for_session(session_id, reason)
            .await;
        match result {
            Ok(updated) => {
                if !updated.is_empty() {
                    self.inner.activity.publish();
                }
                self.inner.abort_all_in_memory().await;
                Ok(updated)
            }
            Err(error) => {
                self.inner.prune_terminal_in_memory().await;
                Err(error.into())
            }
        }
    }

    pub async fn abandon_unfinished_for_session_best_effort(
        &self,
        session_id: &SessionId,
        reason: &str,
    ) -> Vec<DelegationMetadata> {
        let _pump_guard = self.inner.pump_lock.lock().await;
        let updated = self
            .inner
            .store
            .abandon_unfinished_for_session_best_effort(session_id, reason)
            .await;
        if !updated.is_empty() {
            self.inner.activity.publish();
        }
        self.inner.abort_all_in_memory().await;
        updated
    }

    #[cfg(test)]
    async fn wait_until_idle(&self) {
        for _ in 0..200usize {
            let idle = {
                let state = self.inner.state.lock().await;
                state.queued.is_empty()
                    && state.starting == 0
                    && state.running.values().all(JoinHandle::is_finished)
            };
            if idle {
                return;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        panic!("delegation runner did not become idle");
    }
}

impl DelegationRunnerInner {
    async fn abort_all_in_memory(&self) {
        let mut state = self.state.lock().await;
        for (_, handle) in std::mem::take(&mut state.running) {
            handle.abort();
        }
        state.queued.clear();
        state.starting = 0;
        state.abandon_generation = state.abandon_generation.saturating_add(1);
    }

    async fn prune_terminal_in_memory(&self) {
        let (running_ids, queued_ids) = {
            let state = self.state.lock().await;
            (
                state.running.keys().cloned().collect::<Vec<_>>(),
                state.queued.iter().cloned().collect::<Vec<_>>(),
            )
        };
        let mut terminal_ids = BTreeSet::new();
        for id in running_ids.iter().chain(queued_ids.iter()) {
            match self.store.load(id).await {
                Ok(metadata) if metadata.status.is_terminal() => {
                    terminal_ids.insert(id.clone());
                }
                Ok(_) => {}
                Err(err) => {
                    log::warn!(
                        target: "delegation_runner",
                        "delegation abandon 失败后检查内存状态失败 id={id}: {err:#}"
                    );
                }
            }
        }
        if terminal_ids.is_empty() {
            return;
        }
        let mut state = self.state.lock().await;
        state.queued.retain(|id| !terminal_ids.contains(id));
        state.running.retain(|id, handle| {
            if terminal_ids.contains(id) {
                handle.abort();
                false
            } else {
                true
            }
        });
    }

    async fn pump_queue(self: Arc<Self>) {
        let _guard = self.pump_lock.lock().await;
        loop {
            let (next_id, generation) = {
                let mut state = self.state.lock().await;
                state.running.retain(|_, handle| !handle.is_finished());
                if state.running.len().saturating_add(state.starting) >= self.config.max_concurrent
                {
                    return;
                }
                let next = state.queued.pop_front();
                if next.is_some() {
                    state.starting = state.starting.saturating_add(1);
                }
                (next, state.abandon_generation)
            };
            let Some(id) = next_id else {
                return;
            };

            match self.store.start(&id).await {
                Ok(metadata) => {
                    self.activity.publish();
                    let mut state = self.state.lock().await;
                    state.starting = state.starting.saturating_sub(1);
                    if state.abandon_generation != generation {
                        drop(state);
                        if self
                            .store
                            .abandon(&id, "session abandoned during subagent start".into())
                            .await
                            .is_ok()
                        {
                            self.activity.publish();
                        }
                        continue;
                    }
                    let handle = self.spawn_one(id.clone(), metadata);
                    state.running.insert(id, handle);
                }
                Err(err) => {
                    {
                        let mut state = self.state.lock().await;
                        state.starting = state.starting.saturating_sub(1);
                    }
                    let err_text = err.to_string();
                    log::warn!(
                        target: "delegation_runner",
                        "delegation start 失败: {err_text}"
                    );
                    if !matches!(err, DelegationStoreError::CannotTransition { .. }) {
                        let result = DelegationResult {
                            status: DelegationStatus::Failed,
                            summary: "subagent failed to start".to_string(),
                            changed_files: Vec::new(),
                            artifacts: Vec::new(),
                            error_summary: Some(err_text),
                            completed_at: Utc::now(),
                        };
                        if let Err(mark_err) = self.store.complete(&id, result).await {
                            log::warn!(
                                target: "delegation_runner",
                                "delegation start 失败后标记 failed 也失败 id={id}: {mark_err:#}"
                            );
                        } else {
                            self.activity.publish();
                        }
                    }
                }
            }
        }
    }

    fn spawn_one(
        self: &Arc<Self>,
        id: DelegationId,
        metadata: DelegationMetadata,
    ) -> JoinHandle<()> {
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            let run_result = AssertUnwindSafe(inner.run_one(id.clone(), metadata))
                .catch_unwind()
                .await;
            if run_result.is_err() {
                log::warn!(
                    target: "delegation_runner",
                    "delegation executor panic id={id}"
                );
                if let Err(err) = inner
                    .store
                    .complete(
                        &id,
                        DelegationResult {
                            status: DelegationStatus::Failed,
                            summary: "subagent failed".to_string(),
                            changed_files: Vec::new(),
                            artifacts: Vec::new(),
                            error_summary: Some("subagent executor panic".to_string()),
                            completed_at: Utc::now(),
                        },
                    )
                    .await
                {
                    log::warn!(
                        target: "delegation_runner",
                        "delegation panic 后标记 failed 失败 id={id}: {err:#}"
                    );
                } else {
                    inner.activity.publish();
                }
            }
            {
                let mut state = inner.state.lock().await;
                state.running.remove(&id);
            }
            Arc::clone(&inner).pump_queue().await;
        })
    }

    async fn run_one(&self, id: DelegationId, metadata: DelegationMetadata) {
        let progress = DelegationProgressSink {
            store: self.store.clone(),
            id: id.clone(),
            activity: self.activity.clone(),
        };
        let initial_steering = match self
            .store
            .read_steering_after(&id, 0, INITIAL_STEERING_READ_LIMIT)
            .await
        {
            Ok(steering) => steering,
            Err(err) => {
                log::warn!(
                    target: "delegation_runner",
                    "delegation 初始 steering 读取失败 id={id}: {err:#}"
                );
                Vec::new()
            }
        };
        let context = DelegationExecutionContext {
            metadata,
            initial_steering,
        };
        let (executed, mut task_checkpoint_guard) = match self.executor.begin_task(&context).await {
            Ok(()) => {
                let guard = DelegationTaskCheckpointOnDrop::new(
                    Arc::clone(&self.executor),
                    context.clone(),
                );
                (
                    time::timeout(
                        self.config.wall_timeout,
                        self.executor.execute(context.clone(), progress),
                    )
                    .await,
                    Some(guard),
                )
            }
            Err(error) => (Ok(Err(error)), None),
        };
        let execution_succeeded = matches!(&executed, Ok(Ok(_)));
        let now = Utc::now();
        let result = match executed {
            Ok(Ok(outcome)) => DelegationResult {
                status: DelegationStatus::Completed,
                summary: outcome.summary,
                changed_files: outcome.changed_files,
                artifacts: outcome.artifacts,
                error_summary: None,
                completed_at: now,
            },
            Ok(Err(err)) => DelegationResult {
                status: DelegationStatus::Failed,
                summary: "subagent failed".to_string(),
                changed_files: Vec::new(),
                artifacts: Vec::new(),
                error_summary: Some(err.message),
                completed_at: now,
            },
            Err(_) => DelegationResult {
                status: DelegationStatus::Failed,
                summary: "subagent timed out".to_string(),
                changed_files: Vec::new(),
                artifacts: Vec::new(),
                error_summary: Some(format!(
                    "subagent timed out after {}s",
                    self.config.wall_timeout.as_secs()
                )),
                completed_at: now,
            },
        };
        let terminal_persisted = match self.store.complete(&id, result).await {
            Ok(_) => {
                self.activity.publish();
                true
            }
            Err(err) => {
                log::warn!(
                    target: "delegation_runner",
                    "delegation 完成状态落盘失败 id={id}: {err:#}"
                );
                false
            }
        };
        if let Some(guard) = task_checkpoint_guard.as_mut() {
            if let Err(error) = self
                .executor
                .finish_task(&context, execution_succeeded && terminal_persisted)
                .await
            {
                log::warn!(
                    target: "delegation_runner",
                    "delegation task checkpoint 收束失败 id={id}: {error}"
                );
            }
            guard.disarm();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::claim::{AgentId, SessionId};

    struct RecordingExecutor {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        delay: Duration,
        release: Option<Arc<AtomicUsize>>,
        fail: bool,
        seen_initial_steering: Option<Arc<std::sync::Mutex<Vec<Vec<String>>>>>,
    }

    #[async_trait]
    impl DelegationExecutor for RecordingExecutor {
        async fn execute(
            &self,
            context: DelegationExecutionContext,
            progress: DelegationProgressSink,
        ) -> Result<DelegationExecutionOutcome, DelegationExecutionError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            if let Some(seen) = &self.seen_initial_steering {
                seen.lock().expect("steering lock").push(
                    context
                        .initial_steering
                        .iter()
                        .map(|item| item.instruction.clone())
                        .collect(),
                );
            }
            progress
                .update(
                    Some("working".into()),
                    format!("running {}", context.metadata.id),
                    Vec::new(),
                )
                .await
                .map_err(|err| DelegationExecutionError::new(err.to_string()))?;
            if let Some(release) = &self.release {
                while release.load(Ordering::SeqCst) == 0 {
                    time::sleep(Duration::from_millis(5)).await;
                }
            } else {
                time::sleep(self.delay).await;
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            if self.fail {
                return Err(DelegationExecutionError::new("planned failure"));
            }
            Ok(DelegationExecutionOutcome {
                summary: format!("done {}", context.metadata.id),
                changed_files: vec!["src/lib.rs".into()],
                artifacts: Vec::new(),
            })
        }
    }

    struct PanicExecutor;

    #[async_trait]
    impl DelegationExecutor for PanicExecutor {
        async fn execute(
            &self,
            _context: DelegationExecutionContext,
            _progress: DelegationProgressSink,
        ) -> Result<DelegationExecutionOutcome, DelegationExecutionError> {
            panic!("planned delegation panic")
        }
    }

    struct LifecycleRecordingExecutor {
        store: DelegationStore,
        observations: Arc<std::sync::Mutex<Vec<(bool, DelegationStatus)>>>,
        delay: Duration,
    }

    #[async_trait]
    impl DelegationExecutor for LifecycleRecordingExecutor {
        async fn execute(
            &self,
            context: DelegationExecutionContext,
            _progress: DelegationProgressSink,
        ) -> Result<DelegationExecutionOutcome, DelegationExecutionError> {
            time::sleep(self.delay).await;
            Ok(DelegationExecutionOutcome {
                summary: format!("done {}", context.metadata.id),
                changed_files: Vec::new(),
                artifacts: Vec::new(),
            })
        }

        async fn finish_task(
            &self,
            context: &DelegationExecutionContext,
            committed: bool,
        ) -> Result<(), DelegationExecutionError> {
            let status = self
                .store
                .load(&context.metadata.id)
                .await
                .map_err(|error| DelegationExecutionError::new(error.to_string()))?
                .status;
            self.observations
                .lock()
                .expect("lifecycle observations lock")
                .push((committed, status));
            Ok(())
        }
    }

    fn request(turn: &str) -> DelegationCreateRequest {
        DelegationCreateRequest {
            parent_session_id: SessionId::from_str("session_aaaaaaaa").expect("valid session id"),
            parent_turn_id: turn.into(),
            owner_agent_id: AgentId::new("agent-a").expect("valid agent id"),
            title: format!("task {turn}"),
            role: "worker".into(),
            objective: "do work".into(),
            constraints: Vec::new(),
        }
    }

    fn runner(
        store: DelegationStore,
        executor: RecordingExecutor,
        max_concurrent: usize,
        wall_timeout: Duration,
    ) -> DelegationRunner {
        DelegationRunner::new(
            store,
            Arc::new(executor),
            DelegationRunnerConfig {
                max_concurrent,
                wall_timeout,
                wait: DelegationWaitConfig::default(),
            },
        )
        .expect("runner")
    }

    #[tokio::test]
    async fn runner_limits_concurrency_and_drains_queue() {
        let dir = tempfile::tempdir().expect("temp dir");
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicUsize::new(0));
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::clone(&active),
                max_active: Arc::clone(&max_active),
                delay: Duration::from_millis(0),
                release: Some(Arc::clone(&release)),
                fail: false,
                seen_initial_steering: None,
            },
            4,
            Duration::from_secs(5),
        );

        for idx in 0..6 {
            runner
                .create(request(&format!("turn-{idx}")))
                .await
                .expect("create delegation");
        }
        for _ in 0..100usize {
            if max_active.load(Ordering::SeqCst) == 4 {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(max_active.load(Ordering::SeqCst), 4);
        release.store(1, Ordering::SeqCst);
        runner.wait_until_idle().await;

        let summaries = runner.list().await.expect("list");
        assert_eq!(summaries.len(), 6);
        assert!(summaries
            .iter()
            .all(|summary| summary.status == DelegationStatus::Completed));
        assert_eq!(max_active.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn cancelled_creation_before_registration_does_not_leave_queued_metadata() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_secs(1),
                release: None,
                fail: false,
                seen_initial_steering: None,
            },
            1,
            Duration::from_secs(5),
        );
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = runner
            .create_cancellable(
                request("turn-cancelled-before-registration"),
                Some(cancellation),
            )
            .await
            .expect_err("cancelled creation should not register a subagent");
        assert!(matches!(error, DelegationRunnerError::Interrupted));
        time::sleep(Duration::from_millis(30)).await;
        let summaries = runner.list().await.expect("list");
        assert!(
            summaries.iter().all(|summary| summary.status == DelegationStatus::Abandoned),
            "pre-registration cancellation may retain an audit record, but it must not leave a queued/running subagent: {summaries:?}"
        );
    }

    #[tokio::test]
    async fn cancellation_after_registration_does_not_roll_back_subagent() {
        let dir = tempfile::tempdir().expect("temp dir");
        let release = Arc::new(AtomicUsize::new(0));
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_millis(0),
                release: Some(Arc::clone(&release)),
                fail: false,
                seen_initial_steering: None,
            },
            1,
            Duration::from_secs(5),
        );
        let cancellation = CancellationToken::new();
        let metadata = runner
            .create_cancellable(
                request("turn-cancelled-after-registration"),
                Some(cancellation.clone()),
            )
            .await
            .expect("queue insertion is successful registration");
        cancellation.cancel();
        time::sleep(Duration::from_millis(30)).await;
        let persisted = runner.store().load(&metadata.id).await.expect("load");
        assert!(
            matches!(
                persisted.status,
                DelegationStatus::Queued | DelegationStatus::Running
            ),
            "registered subagent must survive parent explicit cancellation"
        );
        release.store(1, Ordering::SeqCst);
        runner.wait_until_idle().await;
    }

    #[tokio::test]
    async fn runner_records_failure_without_retry() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_millis(1),
                release: None,
                fail: true,
                seen_initial_steering: None,
            },
            1,
            Duration::from_secs(5),
        );
        let metadata = runner.create(request("turn-fail")).await.expect("create");
        runner.wait_until_idle().await;
        let metadata = runner.store().load(&metadata.id).await.expect("load");
        assert_eq!(metadata.status, DelegationStatus::Failed);
        assert_eq!(metadata.error_summary.as_deref(), Some("planned failure"));
    }

    #[tokio::test]
    async fn runner_marks_failed_and_drains_queue_after_executor_panic() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let runner = DelegationRunner::new(
            store,
            Arc::new(PanicExecutor),
            DelegationRunnerConfig {
                max_concurrent: 1,
                wall_timeout: Duration::from_secs(5),
                wait: DelegationWaitConfig::default(),
            },
        )
        .expect("runner");

        let metadata = runner.create(request("turn-panic")).await.expect("create");
        runner.wait_until_idle().await;

        let metadata = runner.store().load(&metadata.id).await.expect("load");
        assert_eq!(metadata.status, DelegationStatus::Failed);
        assert_eq!(
            metadata.error_summary.as_deref(),
            Some("subagent executor panic")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn runner_timeout_marks_failed() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_millis(80),
                release: None,
                fail: false,
                seen_initial_steering: None,
            },
            1,
            Duration::from_millis(10),
        );
        let metadata = runner
            .create(request("turn-timeout"))
            .await
            .expect("create");
        runner.wait_until_idle().await;
        let metadata = runner.store().load(&metadata.id).await.expect("load");
        assert_eq!(metadata.status, DelegationStatus::Failed);
        assert!(metadata
            .error_summary
            .as_deref()
            .unwrap_or_default()
            .contains("timed out"));
    }

    #[tokio::test]
    async fn runner_finalizes_task_checkpoint_after_terminal_status_is_persisted() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let runner = DelegationRunner::new(
            store.clone(),
            Arc::new(LifecycleRecordingExecutor {
                store,
                observations: Arc::clone(&observations),
                delay: Duration::ZERO,
            }),
            DelegationRunnerConfig {
                max_concurrent: 1,
                wall_timeout: Duration::from_secs(5),
                wait: DelegationWaitConfig::default(),
            },
        )
        .expect("runner");

        runner
            .create(request("turn-lifecycle-success"))
            .await
            .expect("create");
        runner.wait_until_idle().await;

        assert_eq!(
            observations
                .lock()
                .expect("lifecycle observations lock")
                .as_slice(),
            &[(true, DelegationStatus::Completed)]
        );
    }

    #[tokio::test]
    async fn runner_rolls_back_task_checkpoint_after_timeout_is_persisted() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let runner = DelegationRunner::new(
            store.clone(),
            Arc::new(LifecycleRecordingExecutor {
                store,
                observations: Arc::clone(&observations),
                delay: Duration::from_millis(50),
            }),
            DelegationRunnerConfig {
                max_concurrent: 1,
                wall_timeout: Duration::from_millis(5),
                wait: DelegationWaitConfig::default(),
            },
        )
        .expect("runner");

        runner
            .create(request("turn-lifecycle-timeout"))
            .await
            .expect("create");
        runner.wait_until_idle().await;

        assert_eq!(
            observations
                .lock()
                .expect("lifecycle observations lock")
                .as_slice(),
            &[(false, DelegationStatus::Failed)]
        );
    }

    #[tokio::test]
    async fn runner_abandons_unfinished_and_aborts_running_tasks() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_secs(5),
                release: None,
                fail: false,
                seen_initial_steering: None,
            },
            1,
            Duration::from_secs(10),
        );
        let first = runner.create(request("turn-a")).await.expect("create");
        let second = runner.create(request("turn-b")).await.expect("create");
        let abandoned = runner
            .abandon_unfinished_for_session(&first.parent_session_id, "session closed")
            .await
            .expect("abandon unfinished");
        assert_eq!(abandoned.len(), 2);

        let first = runner.store().load(&first.id).await.expect("load first");
        let second = runner.store().load(&second.id).await.expect("load second");
        assert_eq!(first.status, DelegationStatus::Abandoned);
        assert_eq!(second.status, DelegationStatus::Abandoned);
    }

    #[tokio::test]
    async fn runner_prunes_abandoned_tasks_when_hard_abandon_reports_corrupt_metadata() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_secs(5),
                release: None,
                fail: false,
                seen_initial_steering: None,
            },
            1,
            Duration::from_secs(10),
        );
        let first = runner.create(request("turn-a")).await.expect("create");
        let second = runner.create(request("turn-b")).await.expect("create");
        for _ in 0..50usize {
            if runner
                .store()
                .load(&first.id)
                .await
                .expect("load first")
                .status
                == DelegationStatus::Running
            {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
        let corrupt_dir = runner.store().delegations_dir().join("subagent_badbadbad");
        tokio::fs::create_dir_all(&corrupt_dir)
            .await
            .expect("corrupt dir");
        tokio::fs::write(corrupt_dir.join("delegation.yaml"), "{not yaml")
            .await
            .expect("corrupt metadata");

        let err = runner
            .abandon_unfinished_for_session(&first.parent_session_id, "session finalizing")
            .await
            .expect_err("corrupt metadata should still surface");

        assert!(err.to_string().contains("session"));
        let first = runner.store().load(&first.id).await.expect("load first");
        let second = runner.store().load(&second.id).await.expect("load second");
        assert_eq!(first.status, DelegationStatus::Abandoned);
        assert_eq!(second.status, DelegationStatus::Abandoned);
        let state = runner.inner.state.lock().await;
        assert!(state.running.is_empty());
        assert!(state.queued.is_empty());
    }

    #[tokio::test]
    async fn runner_drains_queue_when_executor_finishes_immediately() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_millis(0),
                release: None,
                fail: false,
                seen_initial_steering: None,
            },
            1,
            Duration::from_secs(5),
        );

        for idx in 0..4usize {
            runner
                .create(request(&format!("turn-fast-{idx}")))
                .await
                .expect("create");
        }
        runner.wait_until_idle().await;
        let summaries = runner.list().await.expect("list");
        assert_eq!(summaries.len(), 4);
        assert!(
            summaries
                .iter()
                .all(|summary| summary.status == DelegationStatus::Completed),
            "{summaries:?}"
        );
    }

    #[tokio::test]
    async fn queued_steering_is_visible_to_executor_context() {
        let dir = tempfile::tempdir().expect("temp dir");
        let release = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let runner = runner(
            DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa")),
            RecordingExecutor {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                delay: Duration::from_millis(0),
                release: Some(Arc::clone(&release)),
                fail: false,
                seen_initial_steering: Some(Arc::clone(&seen)),
            },
            1,
            Duration::from_secs(5),
        );
        let first = runner.create(request("turn-first")).await.expect("first");
        let second = runner.create(request("turn-second")).await.expect("second");
        runner
            .steer(&second.id, "use the narrowed scope".into())
            .await
            .expect("steer queued");

        for _ in 0..100usize {
            if runner.store().load(&first.id).await.expect("load").status
                == DelegationStatus::Running
            {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
        release.store(1, Ordering::SeqCst);
        runner.wait_until_idle().await;
        let seen = seen.lock().expect("steering lock");
        assert!(
            seen.iter()
                .any(|items| items.iter().any(|item| item == "use the narrowed scope")),
            "{seen:?}"
        );
    }
}
