//! Resolution 投递与收敛观测的有界事件调度器。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chrono::Duration as ChronoDuration;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::claim::{Claim, InboxId, SourceId};

use super::types::ARBITRATION_SCHEMA_VERSION;
use super::{
    AnalysisJob, AnalysisState, ArbitrationStore, ObservationService, PendingResolutionDelivery,
    ResolutionEventTarget, ResolutionService,
};

const RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ResolutionEvent {
    Deliver(ResolutionEventTarget),
    Refresh(ResolutionEventTarget),
    RecoverAdoption(AnalysisJob),
}

#[derive(Clone)]
struct ResolutionQueueState {
    scheduled: Arc<Mutex<BTreeSet<ResolutionEvent>>>,
    dirty_refreshes: Arc<Mutex<BTreeSet<ResolutionEvent>>>,
    recovery_wake: Arc<Notify>,
    capacity: usize,
    recover_adoptions: bool,
}

#[derive(Clone)]
pub struct ResolutionEventScheduler {
    sender: mpsc::Sender<ResolutionEvent>,
    scheduled: Arc<Mutex<BTreeSet<ResolutionEvent>>>,
    dirty_refreshes: Arc<Mutex<BTreeSet<ResolutionEvent>>>,
    recovery_wake: Arc<Notify>,
    store: ArbitrationStore,
}

impl ResolutionEventScheduler {
    async fn enqueue(&self, event: ResolutionEvent) -> anyhow::Result<bool> {
        let mut scheduled = self.scheduled.lock().await;
        if scheduled.contains(&event) {
            if let ResolutionEvent::Refresh(target) = &event {
                // 与完成路径共同持有 scheduled 锁，保证执行期间到达的刷新既留下
                // durable marker，也会被标为 dirty，不能被当前执行误删。
                self.store.write_pending_observation(target).await?;
                self.dirty_refreshes.lock().await.insert(event);
            }
            return Ok(false);
        }
        if let ResolutionEvent::Refresh(target) = &event {
            self.store.write_pending_observation(target).await?;
        }
        scheduled.insert(event.clone());
        match self.sender.try_send(event.clone()) {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Full(_)) => {
                scheduled.remove(&event);
                self.recovery_wake.notify_one();
                Ok(false)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                scheduled.remove(&event);
                anyhow::bail!("resolution event scheduler 已关闭")
            }
        }
    }

    pub async fn enqueue_pending_delivery(
        &self,
        target: ResolutionEventTarget,
    ) -> anyhow::Result<bool> {
        if self
            .store
            .read_pending_delivery(&target.resolution_id)
            .await?
            .is_none()
        {
            return Ok(false);
        }
        self.enqueue(ResolutionEvent::Deliver(target)).await
    }

    pub async fn refresh_inboxes(&self, inbox_ids: &[InboxId]) -> anyhow::Result<()> {
        for target in self.store.event_targets_for_inboxes(inbox_ids).await? {
            let _ = self.enqueue(ResolutionEvent::Refresh(target)).await?;
        }
        Ok(())
    }

    pub async fn refresh_claim(&self, claim: &Claim) -> anyhow::Result<()> {
        let policy_ids = claim
            .source_claim_ids
            .iter()
            .filter_map(|source| match source {
                SourceId::Policy(policy_id) => Some(policy_id.clone()),
                SourceId::Claim(_) => None,
            })
            .collect::<Vec<_>>();
        let mut targets = self.store.event_targets_for_claim(&claim.id).await?;
        targets.extend(self.store.event_targets_for_policies(&policy_ids).await?);
        // 直接使用当前上传请求冻结首次携带 CAU provenance 的结果，避免异步
        // Refresh 执行前 mirror 已被后续上传覆盖。该调用也为 Additional Claim
        // 建立 ClaimId 索引。
        targets.extend(self.store.capture_claim_adoption_candidates(claim).await?);
        targets.sort();
        targets.dedup();
        for target in targets {
            let _ = self.enqueue(ResolutionEvent::Refresh(target)).await?;
        }
        Ok(())
    }

    pub async fn refresh_resolution(&self, target: ResolutionEventTarget) -> anyhow::Result<bool> {
        self.enqueue(ResolutionEvent::Refresh(target)).await
    }

    /// 唤醒持久任务扫描。HTTP 请求在 Resolution 已落盘后被取消时使用此同步入口，
    /// 让 pending delivery/observation 在当前进程中继续恢复。
    pub(crate) fn wake_durable_recovery(&self) {
        self.recovery_wake.notify_one();
    }
}

pub fn spawn_resolution_event_scheduler(
    resolution_service: ResolutionService,
    observation_service: ObservationService,
    capacity: usize,
    recover_adoptions: bool,
    cancel: CancellationToken,
) -> (ResolutionEventScheduler, JoinHandle<anyhow::Result<()>>) {
    let capacity = capacity.max(1);
    let store = resolution_service.store().clone();
    let (sender, receiver) = mpsc::channel(capacity);
    let scheduled = Arc::new(Mutex::new(BTreeSet::new()));
    let dirty_refreshes = Arc::new(Mutex::new(BTreeSet::new()));
    let recovery_wake = resolution_service.event_wake();
    let scheduler = ResolutionEventScheduler {
        sender,
        scheduled: scheduled.clone(),
        dirty_refreshes: dirty_refreshes.clone(),
        recovery_wake: recovery_wake.clone(),
        store: store.clone(),
    };
    let handle = tokio::spawn(run(
        receiver,
        ResolutionQueueState {
            scheduled,
            dirty_refreshes,
            recovery_wake,
            capacity,
            recover_adoptions,
        },
        resolution_service,
        observation_service,
        cancel,
    ));
    (scheduler, handle)
}

async fn run(
    mut receiver: mpsc::Receiver<ResolutionEvent>,
    queue: ResolutionQueueState,
    resolution_service: ResolutionService,
    observation_service: ObservationService,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let store = resolution_service.store().clone();
    let mut ready = VecDeque::new();
    let mut delayed = BTreeMap::<Instant, VecDeque<ResolutionEvent>>::new();
    let mut recovery_requested = true;
    // disabled 模式不会在运行中创建新的 adopting Analysis；这里只在启动时（或受
    // capacity 限制的后续补页）消费既有持久任务，避免每个 ACK/Claim 事件都重扫。
    let mut recover_adoptions = queue.recover_adoptions;

    loop {
        if cancel.is_cancelled() {
            break;
        }
        move_due(&mut delayed, &mut ready);
        if recovery_requested && queue.scheduled.lock().await.len() < queue.capacity {
            recovery_requested = refill_pending(
                &store,
                &queue.scheduled,
                &mut ready,
                &mut delayed,
                queue.capacity,
                &mut recover_adoptions,
            )
            .await?;
        }
        if let Some(event) = ready.pop_front() {
            // Refresh 的 pending 文件在成功完成前始终保留，因此任意执行中崩溃都能
            // 由启动扫描恢复。执行期间到达的新事件另以 dirty 标记要求再刷新一次。
            let outcome =
                process_event(&store, &resolution_service, &observation_service, &event).await;
            match outcome {
                Ok(()) => {
                    match complete_event(&store, &queue.scheduled, &queue.dirty_refreshes, &event)
                        .await
                    {
                        Ok(_needs_refresh) => recovery_requested = true,
                        Err(error) => {
                            log::warn!(
                                target: "maintainer_arbitration",
                                "完成 Resolution 事件持久状态失败，将退避恢复: {error:#}"
                            );
                            if let Some(delay) = persist_retry(&store, &event).await? {
                                delayed
                                    .entry(Instant::now() + delay)
                                    .or_default()
                                    .push_back(event);
                            }
                        }
                    }
                }
                Err(error) => {
                    log::warn!(
                        target: "maintainer_arbitration",
                        "Resolution 事件处理失败，将退避恢复: {error:#}"
                    );
                    match persist_retry(&store, &event).await? {
                        Some(delay) => {
                            delayed
                                .entry(Instant::now() + delay)
                                .or_default()
                                .push_back(event);
                        }
                        None => {
                            queue.scheduled.lock().await.remove(&event);
                            recovery_requested = true;
                        }
                    }
                }
            }
            continue;
        }

        let next = delayed.keys().next().copied();
        match next {
            Some(deadline) => tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                event = receiver.recv() => match event {
                    Some(event) => ready.push_back(event),
                    None => break,
                },
                _ = queue.recovery_wake.notified() => recovery_requested = true,
                _ = tokio::time::sleep_until(deadline) => {}
            },
            None => tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                event = receiver.recv() => match event {
                    Some(event) => ready.push_back(event),
                    None => break,
                },
                _ = queue.recovery_wake.notified() => recovery_requested = true,
            },
        }
    }
    receiver.close();
    queue.scheduled.lock().await.clear();
    queue.dirty_refreshes.lock().await.clear();
    Ok(())
}

async fn complete_event(
    store: &ArbitrationStore,
    scheduled: &Arc<Mutex<BTreeSet<ResolutionEvent>>>,
    dirty_refreshes: &Arc<Mutex<BTreeSet<ResolutionEvent>>>,
    event: &ResolutionEvent,
) -> anyhow::Result<bool> {
    let mut scheduled = scheduled.lock().await;
    let needs_refresh = if let ResolutionEvent::Refresh(target) = event {
        let mut dirty = dirty_refreshes.lock().await;
        if dirty.remove(event) {
            true
        } else {
            store
                .remove_pending_observation(&target.resolution_id)
                .await?;
            false
        }
    } else {
        false
    };
    scheduled.remove(event);
    Ok(needs_refresh)
}

async fn process_event(
    store: &ArbitrationStore,
    resolution_service: &ResolutionService,
    observation_service: &ObservationService,
    event: &ResolutionEvent,
) -> anyhow::Result<()> {
    let target = match event {
        ResolutionEvent::Deliver(target) => {
            resolution_service.recover_pending_delivery(target).await?;
            resolution_service
                .complete_analysis_adoption_after_delivery(target, crate::time::now_seconds())
                .await?;
            return Ok(());
        }
        ResolutionEvent::Refresh(target) => target,
        ResolutionEvent::RecoverAdoption(job) => {
            let state = resolution_service
                .recover_analysis_adoption(job, crate::time::now_seconds())
                .await?;
            if state == AnalysisState::Adopting {
                // Resolution 与 pending delivery 已稳定；后续只由 Deliver 事件按其
                // 持久 retry_count 退避，避免两条恢复路径重复尝试 outbox。
                return Ok(());
            }
            return Ok(());
        }
    };
    let dispute = store.read_dispute(&target.dispute_id).await?;
    if dispute
        .resolution
        .as_ref()
        .map(|resolution| &resolution.resolution_id)
        != Some(&target.resolution_id)
    {
        return Ok(());
    }
    let record = store
        .read_resolution_record(&target.dispute_id, &target.resolution_id)
        .await?;
    if record.delivery_intent.is_some() {
        observation_service
            .refresh(&record, crate::time::now_seconds())
            .await?;
    }
    Ok(())
}

async fn refill_pending(
    store: &ArbitrationStore,
    scheduled: &Arc<Mutex<BTreeSet<ResolutionEvent>>>,
    ready: &mut VecDeque<ResolutionEvent>,
    delayed: &mut BTreeMap<Instant, VecDeque<ResolutionEvent>>,
    capacity: usize,
    recover_adoptions: &mut bool,
) -> anyhow::Result<bool> {
    let now = crate::time::now_seconds();
    let now_instant = Instant::now();
    let mut more_pending = false;
    let mut deliveries = store.list_pending_deliveries().await?;
    deliveries.sort_by(|left, right| left.target.cmp(&right.target));
    for pending in deliveries {
        let event = ResolutionEvent::Deliver(pending.target);
        let mut events = scheduled.lock().await;
        if events.contains(&event) {
            continue;
        }
        if events.len() >= capacity {
            more_pending = true;
            continue;
        }
        events.insert(event.clone());
        drop(events);
        let delay = pending
            .next_retry_at
            .and_then(|retry_at| retry_at.signed_duration_since(now).to_std().ok());
        if let Some(delay) = delay.filter(|delay| !delay.is_zero()) {
            delayed
                .entry(now_instant + delay)
                .or_default()
                .push_back(event);
        } else {
            ready.push_back(event);
        }
    }
    let mut observations = store.list_pending_observations().await?;
    observations.sort();
    for target in observations {
        let event = ResolutionEvent::Refresh(target);
        let mut events = scheduled.lock().await;
        if events.contains(&event) {
            continue;
        }
        if events.len() >= capacity {
            more_pending = true;
            continue;
        }
        events.insert(event.clone());
        drop(events);
        ready.push_back(event);
    }
    if *recover_adoptions {
        let mut more_adoptions = false;
        for job in store.recoverable_adoption_jobs().await? {
            let event = ResolutionEvent::RecoverAdoption(job);
            let mut events = scheduled.lock().await;
            if events.contains(&event) {
                continue;
            }
            if events.len() >= capacity {
                more_pending = true;
                more_adoptions = true;
                continue;
            }
            events.insert(event.clone());
            drop(events);
            ready.push_back(event);
        }
        *recover_adoptions = more_adoptions;
    }
    Ok(more_pending)
}

async fn persist_retry(
    store: &ArbitrationStore,
    event: &ResolutionEvent,
) -> anyhow::Result<Option<Duration>> {
    let target = match event {
        ResolutionEvent::Refresh(target) => {
            store.write_pending_observation(target).await?;
            return Ok(Some(RETRY_INITIAL_DELAY));
        }
        ResolutionEvent::Deliver(target) => target,
        ResolutionEvent::RecoverAdoption(_) => return Ok(Some(RETRY_INITIAL_DELAY)),
    };
    let mut pending = match store.read_pending_delivery(&target.resolution_id).await? {
        Some(pending) => pending,
        None => {
            // outbox create-or-verify 已成功后，Analysis 终态更新仍可能遇到瞬时 I/O
            // 故障。此时 pending delivery 已被消费；为让重试继续具备崩溃恢复依据，
            // 仅在 target 仍是当前 Resolution 时重建同一任务，不生成任何新 ID。
            let Some(record) = store
                .read_current_resolution_record(&target.dispute_id)
                .await?
                .filter(|record| {
                    record.resolution_id == target.resolution_id && record.delivery_intent.is_some()
                })
            else {
                return Ok(None);
            };
            let created_at = record.created_at;
            PendingResolutionDelivery {
                schema_version: ARBITRATION_SCHEMA_VERSION,
                target: target.clone(),
                resolution_record: Some(Box::new(record)),
                created_at,
                retry_count: 0,
                next_retry_at: None,
            }
        }
    };
    let exponent = pending.retry_count.min(5);
    let delay = RETRY_INITIAL_DELAY
        .saturating_mul(1_u32 << exponent)
        .min(RETRY_MAX_DELAY);
    pending.retry_count = pending.retry_count.saturating_add(1);
    pending.next_retry_at = ChronoDuration::from_std(delay)
        .ok()
        .map(|duration| crate::time::now_seconds() + duration);
    store.write_pending_delivery(&pending).await?;
    Ok(Some(delay))
}

fn move_due(
    delayed: &mut BTreeMap<Instant, VecDeque<ResolutionEvent>>,
    ready: &mut VecDeque<ResolutionEvent>,
) {
    let now = Instant::now();
    let due = delayed
        .range(..=now)
        .map(|(deadline, _)| *deadline)
        .collect::<Vec<_>>();
    for deadline in due {
        if let Some(mut events) = delayed.remove(&deadline) {
            ready.append(&mut events);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{
        AgentId, ArbitrationResolutionId, Claim, ClaimStatus, Confidence, Dispute,
        DisputeResolution, DisputeStatus, InboxMessage, InboxMessageKind, MaintainerActionId,
        Policy, PolicyId, PolicyMessageType, PolicyStatus, ResolutionBasis, ResolutionType,
        ResolvedBy,
    };
    use crate::config::ArbitrationMode;
    use crate::maintainer::Maintainer;
    use crate::storage::{paths, write_yaml_atomic};

    use super::super::resolution::CommitFailpoint;
    use super::super::types::{
        ArbitrationAnalysis, ArbitrationAnalysisId, ArbitrationResolutionRecord, DeliveryIntent,
        DeliveryTargetIntent, MaintainerDisputeRecord, PendingResolutionDelivery,
        ARBITRATION_PROMPT_VERSION, ARBITRATION_SCHEMA_VERSION,
        CURRENT_SEMANTIC_PROJECTION_VERSION,
    };

    struct EventFixture {
        _root: tempfile::TempDir,
        store: ArbitrationStore,
        resolution_service: ResolutionService,
        observation_service: ObservationService,
        record: ArbitrationResolutionRecord,
    }

    impl EventFixture {
        async fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let team_root = root.path().to_path_buf();
            let maintainer = Arc::new(Maintainer::new(
                team_root.clone(),
                ChronoDuration::days(30),
                ChronoDuration::days(60),
                8,
            ));
            let store = ArbitrationStore::new(team_root);
            let record = resolution_record("agent-a");
            store
                .write_dispute(&MaintainerDisputeRecord {
                    dispute: Dispute {
                        status: DisputeStatus::Resolved,
                        resolved_at: Some(record.resolution.resolved_at),
                        ..record.dispute_snapshot.clone()
                    },
                    resolution: Some(record.resolution.clone()),
                })
                .await
                .unwrap();
            store.write_resolution_record(&record).await.unwrap();
            store
                .write_pending_delivery(&PendingResolutionDelivery {
                    schema_version: ARBITRATION_SCHEMA_VERSION,
                    target: ResolutionEventTarget {
                        dispute_id: record.dispute_id.clone(),
                        resolution_id: record.resolution_id.clone(),
                    },
                    resolution_record: Some(Box::new(record.clone())),
                    created_at: record.created_at,
                    retry_count: 0,
                    next_retry_at: None,
                })
                .await
                .unwrap();
            let resolution_service = ResolutionService::new(maintainer.clone(), store.clone());
            let observation_service =
                ObservationService::new(store.clone(), maintainer.history_store().clone());
            Self {
                _root: root,
                store,
                resolution_service,
                observation_service,
                record,
            }
        }
    }

    fn resolution_record(agent: &str) -> ArbitrationResolutionRecord {
        let holder = AgentId::new(agent).unwrap();
        let claim = Claim {
            id: crate::claim::ClaimId::random(),
            name: "current_runtime_contract".into(),
            statement: "The current runtime contract is supported by a production trace.".into(),
            scope: "service / production".into(),
            holder: holder.clone(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "A reproducible production trace confirms the contract.".into(),
        };
        let dispute = Dispute {
            id: crate::claim::DisputeId::random(),
            name: "runtime_contract_conflict".into(),
            reporter_agent_id: holder.clone(),
            claims: vec![claim.id.clone()],
            summary: "Two runtime contracts conflict under the same production scope.".into(),
            status: DisputeStatus::Open,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let resolution_id = ArbitrationResolutionId::random();
        let resolution = DisputeResolution {
            resolution_id: resolution_id.clone(),
            resolved_by: ResolvedBy::Human,
            resolved_at: "2026-08-03T00:00:00Z".parse().unwrap(),
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::Evidence),
            conclusion: "Use the evidence-backed runtime contract.".into(),
            claim_assessments: Vec::new(),
            rejection_reason: None,
        };
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "Use the evidence-backed runtime contract.".into(),
            scope: "maintainer / dispute-arbitration".into(),
            status: PolicyStatus::Active,
            created_at: resolution.resolved_at,
            updated_at: None,
            target_agents: Some(vec![holder.clone()]),
        };
        let inbox_id = crate::claim::InboxId::random();
        let message = InboxMessage {
            id: inbox_id.clone(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: policy.clone(),
                arbitration_resolution: None,
            },
            handled_at: None,
        };
        ArbitrationResolutionRecord {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            resolution_id,
            dispute_id: dispute.id.clone(),
            created_at: resolution.resolved_at,
            resolution,
            dispute_snapshot: dispute,
            direct_claim_snapshots: vec![claim],
            semantic_fingerprint: None,
            context_snapshot_hash: None,
            analysis_source_id: None,
            legacy_source_attempt_id: None,
            delivery_intent: Some(DeliveryIntent {
                policy,
                maintainer_action_id: MaintainerActionId::random(),
                targets: vec![DeliveryTargetIntent {
                    inbox_id,
                    target_agent: holder,
                    inbox_message: message,
                }],
            }),
            snapshot_source_resolution_id: None,
        }
    }

    async fn wait_for_pending_retry(store: &ArbitrationStore, id: &ArbitrationResolutionId) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if store
                .read_pending_delivery(id)
                .await
                .unwrap()
                .is_some_and(|pending| pending.retry_count > 0)
            {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "pending delivery did not enter backoff"
            );
            tokio::task::yield_now().await;
        }
    }

    async fn wait_for_delivery_completion(store: &ArbitrationStore, id: &ArbitrationResolutionId) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if store.read_pending_delivery(id).await.unwrap().is_none() {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "pending delivery was not completed"
            );
            tokio::task::yield_now().await;
        }
    }

    async fn wait_for_observation_completion(
        store: &ArbitrationStore,
        id: &ArbitrationResolutionId,
    ) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match store.list_pending_observations().await {
                Ok(pending) if pending.iter().all(|target| &target.resolution_id != id) => {
                    return;
                }
                Ok(_) | Err(_) => {}
            }
            assert!(
                std::time::Instant::now() < deadline,
                "pending observation was not completed"
            );
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_scheduler_reads_only_persisted_event_tasks() {
        let root = tempfile::tempdir().unwrap();
        let team_root = root.path().to_path_buf();
        let dispute_dir = paths::team_store_disputes_dir(&team_root);
        tokio::fs::create_dir_all(&dispute_dir).await.unwrap();
        tokio::fs::write(dispute_dir.join("dispute_1234abcd.yaml"), b"not: [valid")
            .await
            .unwrap();
        let maintainer = Arc::new(Maintainer::new(
            team_root.clone(),
            ChronoDuration::days(30),
            ChronoDuration::days(60),
            8,
        ));
        let store = ArbitrationStore::new(team_root);
        let cancel = CancellationToken::new();
        let (_scheduler, handle) = spawn_resolution_event_scheduler(
            ResolutionService::new(maintainer.clone(), store.clone()),
            ObservationService::new(store, maintainer.history_store().clone()),
            4,
            false,
            cancel.clone(),
        );
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!handle.is_finished());
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn startup_recovers_only_the_persisted_pending_delivery() {
        let fixture = EventFixture::new().await;
        let cancel = CancellationToken::new();
        let (_scheduler, handle) = spawn_resolution_event_scheduler(
            fixture.resolution_service.clone(),
            fixture.observation_service.clone(),
            4,
            false,
            cancel.clone(),
        );
        wait_for_delivery_completion(&fixture.store, &fixture.record.resolution_id).await;
        assert_eq!(
            crate::maintainer::outbox_io::list(fixture.store.team_root())
                .await
                .unwrap()
                .len(),
            1
        );
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn startup_recovers_resolution_commit_from_pending_intent_before_current_record() {
        let fixture = EventFixture::new().await;
        let mut fixed_record = fixture.record.clone();
        fixed_record.delivery_intent = None;
        fixture
            .store
            .write_pending_delivery(&PendingResolutionDelivery {
                schema_version: ARBITRATION_SCHEMA_VERSION,
                target: ResolutionEventTarget {
                    dispute_id: fixed_record.dispute_id.clone(),
                    resolution_id: fixed_record.resolution_id.clone(),
                },
                resolution_record: Some(Box::new(fixed_record.clone())),
                created_at: fixed_record.created_at,
                retry_count: 0,
                next_retry_at: None,
            })
            .await
            .unwrap();
        let resolution_path = paths::team_store_arbitration_resolution_path(
            fixture.store.team_root(),
            &fixture.record.dispute_id,
        );
        tokio::fs::remove_file(&resolution_path).await.unwrap();
        fixture
            .store
            .write_dispute(&MaintainerDisputeRecord::from(
                fixture.record.dispute_snapshot.clone(),
            ))
            .await
            .unwrap();

        let cancel = CancellationToken::new();
        let (_scheduler, handle) = spawn_resolution_event_scheduler(
            fixture.resolution_service.clone(),
            fixture.observation_service.clone(),
            4,
            false,
            cancel.clone(),
        );
        wait_for_delivery_completion(&fixture.store, &fixture.record.resolution_id).await;

        let dispute = fixture
            .store
            .read_dispute(&fixture.record.dispute_id)
            .await
            .unwrap();
        assert_eq!(dispute.resolution, Some(fixed_record.resolution.clone()));
        assert_eq!(
            fixture
                .store
                .read_current_resolution_record(&fixture.record.dispute_id)
                .await
                .unwrap()
                .unwrap(),
            fixed_record
        );
        assert!(
            crate::maintainer::outbox_io::list(fixture.store.team_root())
                .await
                .unwrap()
                .is_empty()
        );
        let resolution_events = crate::maintainer::history::HistoryStore::with_defaults(
            fixture.store.team_root().to_path_buf(),
        )
        .list_dispute_resolution_events()
        .await
        .unwrap();
        assert_eq!(resolution_events.len(), 1);
        fixture
            .resolution_service
            .ensure_delivery(&fixed_record)
            .await
            .unwrap();
        assert_eq!(
            crate::maintainer::history::HistoryStore::with_defaults(
                fixture.store.team_root().to_path_buf(),
            )
            .list_dispute_resolution_events()
            .await
            .unwrap()
            .len(),
            1
        );

        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_to_observation_handoff_is_durable_across_restart() {
        let fixture = EventFixture::new().await;
        let interrupted = fixture
            .resolution_service
            .clone()
            .with_commit_failpoint(CommitFailpoint::ObservationHandoffStored);
        assert!(interrupted.ensure_delivery(&fixture.record).await.is_err());
        assert!(fixture
            .store
            .read_pending_delivery(&fixture.record.resolution_id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            fixture.store.list_pending_observations().await.unwrap(),
            vec![ResolutionEventTarget {
                dispute_id: fixture.record.dispute_id.clone(),
                resolution_id: fixture.record.resolution_id.clone(),
            }]
        );

        let cancel = CancellationToken::new();
        let (_scheduler, handle) = spawn_resolution_event_scheduler(
            fixture.resolution_service.clone(),
            fixture.observation_service.clone(),
            4,
            false,
            cancel.clone(),
        );
        wait_for_delivery_completion(&fixture.store, &fixture.record.resolution_id).await;
        wait_for_observation_completion(&fixture.store, &fixture.record.resolution_id).await;
        assert!(fixture
            .store
            .read_observation(&fixture.record.dispute_id, &fixture.record.resolution_id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            crate::maintainer::outbox_io::list(fixture.store.team_root())
                .await
                .unwrap()
                .len(),
            1
        );
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn disabled_mode_startup_recovers_a_persisted_adoption_intent() {
        let fixture = EventFixture::new().await;
        let resolution_path = paths::team_store_arbitration_resolution_path(
            fixture.store.team_root(),
            &fixture.record.dispute_id,
        );
        tokio::fs::remove_file(resolution_path).await.unwrap();
        fixture
            .store
            .remove_pending_delivery(&fixture.record.resolution_id)
            .await
            .unwrap();
        fixture
            .store
            .write_dispute(&MaintainerDisputeRecord::from(
                fixture.record.dispute_snapshot.clone(),
            ))
            .await
            .unwrap();

        let job = AnalysisJob {
            dispute_id: fixture.record.dispute_id.clone(),
            analysis_id: ArbitrationAnalysisId::random(),
        };
        let analysis = ArbitrationAnalysis {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            analysis_id: job.analysis_id.clone(),
            dispute_id: job.dispute_id.clone(),
            legacy_source: crate::maintainer::arbitration::types::LegacyAnalysisSource::Manual,
            report_snapshot: None,
            created_at: fixture.record.created_at,
            updated_at: fixture.record.created_at,
            prompt_version: ARBITRATION_PROMPT_VERSION.into(),
            mode: ArbitrationMode::Manual,
            model: "test-model".into(),
            confidence_threshold: 0.9,
            semantic_projection_version: CURRENT_SEMANTIC_PROJECTION_VERSION,
            semantic_fingerprint: None,
            context_snapshot_hash: None,
            context: None,
            state: AnalysisState::Adopting,
            analysis_round: 1,
            rounds: Vec::new(),
            context_change_count: 0,
            next_retry_at: None,
            context_change_reason: None,
            lease: None,
            proposal: None,
            verification: None,
            resolution_id: Some(fixture.record.resolution_id.clone()),
            pending_resolution: Some(fixture.record.clone()),
            error: None,
            delivery_error: None,
            adoption_blocked_reason: None,
            context_prepare_attempts: 0,
        };
        fixture.store.write_analysis(&analysis).await.unwrap();

        let cancel = CancellationToken::new();
        // recover_adoptions=true 模拟 enabled=false：没有 LLM/arbitration scheduler，
        // 只由 Resolution 事件队列接管已经固定的采用意图。
        let (_scheduler, handle) = spawn_resolution_event_scheduler(
            fixture.resolution_service.clone(),
            fixture.observation_service.clone(),
            4,
            true,
            cancel.clone(),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if fixture.store.read_analysis(&job).await.unwrap().state == AnalysisState::Adopted
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disabled 模式启动应恢复固定 adoption intent");
        let dispute = fixture.store.read_dispute(&job.dispute_id).await.unwrap();
        assert_eq!(dispute.dispute.status, DisputeStatus::Resolved);
        assert_eq!(
            dispute.resolution.unwrap().resolution_id,
            fixture.record.resolution_id
        );
        assert_eq!(
            crate::maintainer::outbox_io::list(fixture.store.team_root())
                .await
                .unwrap()
                .len(),
            1
        );
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_delivery_backs_off_and_recovers_without_reminting() {
        let fixture = EventFixture::new().await;
        let wanted_policy = fixture
            .record
            .delivery_intent
            .as_ref()
            .unwrap()
            .policy
            .clone();
        let mut conflicting_policy = wanted_policy.clone();
        conflicting_policy.statement = "conflicting immutable payload".into();
        let policy_path = paths::team_store_policies_dir(fixture.store.team_root())
            .join(format!("{}.yaml", wanted_policy.id));
        write_yaml_atomic(&policy_path, &conflicting_policy)
            .await
            .unwrap();
        let cancel = CancellationToken::new();
        let (_scheduler, handle) = spawn_resolution_event_scheduler(
            fixture.resolution_service.clone(),
            fixture.observation_service.clone(),
            4,
            false,
            cancel.clone(),
        );
        wait_for_pending_retry(&fixture.store, &fixture.record.resolution_id).await;
        let pending = fixture
            .store
            .read_pending_delivery(&fixture.record.resolution_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.retry_count, 1);
        assert!(pending.next_retry_at.is_some());

        write_yaml_atomic(&policy_path, &wanted_policy)
            .await
            .unwrap();
        tokio::time::advance(RETRY_INITIAL_DELAY).await;
        wait_for_delivery_completion(&fixture.store, &fixture.record.resolution_id).await;
        let entries = crate::maintainer::outbox_io::list(fixture.store.team_root())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].inbox_id,
            fixture.record.delivery_intent.as_ref().unwrap().targets[0].inbox_id
        );
        cancel.cancel();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn event_indices_and_duplicate_refreshes_are_targeted_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        let first = resolution_record("agent-a");
        let second = resolution_record("agent-b");
        store
            .register_resolution_event_targets(&first)
            .await
            .unwrap();
        store
            .register_resolution_event_targets(&second)
            .await
            .unwrap();
        store
            .register_resolution_event_targets(&first)
            .await
            .unwrap();
        store.write_resolution_record(&first).await.unwrap();
        store
            .write_dispute(&MaintainerDisputeRecord {
                dispute: Dispute {
                    status: DisputeStatus::Resolved,
                    resolved_at: Some(first.resolution.resolved_at),
                    ..first.dispute_snapshot.clone()
                },
                resolution: Some(first.resolution.clone()),
            })
            .await
            .unwrap();
        let first_inbox = &first.delivery_intent.as_ref().unwrap().targets[0].inbox_id;
        assert_eq!(
            store
                .event_targets_for_inboxes(std::slice::from_ref(first_inbox))
                .await
                .unwrap(),
            vec![ResolutionEventTarget {
                dispute_id: first.dispute_id.clone(),
                resolution_id: first.resolution_id.clone(),
            }]
        );
        assert_eq!(
            store
                .event_targets_for_claim(&second.direct_claim_snapshots[0].id)
                .await
                .unwrap(),
            vec![ResolutionEventTarget {
                dispute_id: second.dispute_id.clone(),
                resolution_id: second.resolution_id.clone(),
            }]
        );
        let first_policy = &first.delivery_intent.as_ref().unwrap().policy.id;
        assert_eq!(
            store
                .event_targets_for_policies(std::slice::from_ref(first_policy))
                .await
                .unwrap(),
            vec![ResolutionEventTarget {
                dispute_id: first.dispute_id.clone(),
                resolution_id: first.resolution_id.clone(),
            }]
        );

        let (sender, mut receiver) = mpsc::channel(1);
        let scheduler = ResolutionEventScheduler {
            sender,
            scheduled: Arc::new(Mutex::new(BTreeSet::new())),
            dirty_refreshes: Arc::new(Mutex::new(BTreeSet::new())),
            recovery_wake: Arc::new(Notify::new()),
            store: store.clone(),
        };
        let target = ResolutionEventTarget {
            dispute_id: first.dispute_id.clone(),
            resolution_id: first.resolution_id.clone(),
        };
        let mut additional_claim = first.direct_claim_snapshots[0].clone();
        additional_claim.id = crate::claim::ClaimId::random();
        additional_claim.statement = "first attributed result".into();
        additional_claim.source_claim_ids = vec![SourceId::Policy(first_policy.clone())];
        scheduler.refresh_claim(&additional_claim).await.unwrap();
        assert!(!scheduler.refresh_resolution(target.clone()).await.unwrap());
        assert_eq!(
            receiver.recv().await,
            Some(ResolutionEvent::Refresh(target))
        );
        assert_eq!(store.list_pending_observations().await.unwrap().len(), 1);

        let candidates = store
            .list_claim_adoption_candidates(&first.dispute_id, &first.resolution_id, first_policy)
            .await
            .unwrap();
        assert_eq!(candidates, vec![additional_claim.clone()]);
        assert_eq!(
            store
                .event_targets_for_claim(&additional_claim.id)
                .await
                .unwrap(),
            vec![ResolutionEventTarget {
                dispute_id: first.dispute_id.clone(),
                resolution_id: first.resolution_id.clone(),
            }]
        );

        // 后续上传不再携带 provenance，仍由 ClaimId 索引定向刷新；首次候选不改写。
        additional_claim.statement = "later unrelated edit".into();
        additional_claim.source_claim_ids.clear();
        scheduler.refresh_claim(&additional_claim).await.unwrap();
        let candidates = store
            .list_claim_adoption_candidates(&first.dispute_id, &first.resolution_id, first_policy)
            .await
            .unwrap();
        assert_eq!(candidates[0].statement, "first attributed result");
    }

    #[tokio::test]
    async fn refresh_arriving_during_an_in_flight_refresh_remains_pending() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        let record = resolution_record("agent-a");
        let target = ResolutionEventTarget {
            dispute_id: record.dispute_id,
            resolution_id: record.resolution_id,
        };
        let event = ResolutionEvent::Refresh(target.clone());
        let scheduled = Arc::new(Mutex::new(BTreeSet::from([event.clone()])));
        let dirty_refreshes = Arc::new(Mutex::new(BTreeSet::new()));
        let (sender, _receiver) = mpsc::channel(1);
        let scheduler = ResolutionEventScheduler {
            sender,
            scheduled: scheduled.clone(),
            dirty_refreshes: dirty_refreshes.clone(),
            recovery_wake: Arc::new(Notify::new()),
            store: store.clone(),
        };

        store.write_pending_observation(&target).await.unwrap();
        assert!(!scheduler.refresh_resolution(target.clone()).await.unwrap());

        // 当前 in-flight 执行成功只消费 dirty 状态并释放 scheduled 槽；执行期间
        // 重新确认的 durable marker 必须留给下一轮定向刷新。
        assert!(complete_event(&store, &scheduled, &dirty_refreshes, &event)
            .await
            .unwrap());
        assert_eq!(
            store.list_pending_observations().await.unwrap(),
            vec![target]
        );
    }

    #[tokio::test]
    async fn replaced_resolution_event_keeps_the_old_observation_frozen() {
        let fixture = EventFixture::new().await;
        let old_target = ResolutionEventTarget {
            dispute_id: fixture.record.dispute_id.clone(),
            resolution_id: fixture.record.resolution_id.clone(),
        };
        let old_observation = super::super::types::ResolutionObservation {
            resolution_id: old_target.resolution_id.clone(),
            dispute_id: old_target.dispute_id.clone(),
            observed_at: "2026-08-04T00:00:00Z".parse().unwrap(),
            holders: Vec::new(),
        };
        fixture
            .store
            .write_observation(&old_target.dispute_id, &old_observation)
            .await
            .unwrap();
        fixture
            .store
            .register_resolution_event_targets(&fixture.record)
            .await
            .unwrap();

        let mut replacement = resolution_record("agent-a");
        replacement.dispute_id = old_target.dispute_id.clone();
        replacement.dispute_snapshot.id = old_target.dispute_id.clone();
        fixture
            .store
            .write_resolution_record(&replacement)
            .await
            .unwrap();
        let mut dispute = fixture
            .store
            .read_dispute(&old_target.dispute_id)
            .await
            .unwrap();
        dispute.resolution = Some(replacement.resolution.clone());
        dispute.dispute.resolved_at = Some(replacement.resolution.resolved_at);
        fixture.store.write_dispute(&dispute).await.unwrap();

        let old_policy = &fixture.record.delivery_intent.as_ref().unwrap().policy.id;
        let mut late_old_claim = fixture.record.direct_claim_snapshots[0].clone();
        late_old_claim.source_claim_ids = vec![SourceId::Policy(old_policy.clone())];
        assert!(fixture
            .store
            .capture_claim_adoption_candidates(&late_old_claim)
            .await
            .unwrap()
            .is_empty());
        assert!(fixture
            .store
            .list_claim_adoption_candidates(
                &old_target.dispute_id,
                &old_target.resolution_id,
                old_policy,
            )
            .await
            .unwrap()
            .is_empty());

        process_event(
            &fixture.store,
            &fixture.resolution_service,
            &fixture.observation_service,
            &ResolutionEvent::Refresh(old_target.clone()),
        )
        .await
        .unwrap();
        assert_eq!(
            fixture
                .store
                .read_observation(&old_target.dispute_id, &old_target.resolution_id)
                .await
                .unwrap(),
            Some(old_observation)
        );
    }
}
