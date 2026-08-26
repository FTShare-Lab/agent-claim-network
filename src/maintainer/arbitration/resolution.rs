//! Analysis 采用、人工 Resolve 与 reject-and-replace 的统一提交边界。

use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::sync::Arc;

use anyhow::Context;
use chrono::{DateTime, Utc};
use tokio::sync::Notify;

use crate::claim::{
    AgentId, ArbitrationResolutionContext, ArbitrationResolutionId, ClaimAssessment, DisputeId,
    DisputeResolution, DisputeStatus, InboxMessage, InboxMessageKind, OutboxEntry, OutboxTarget,
    Policy, PolicyMessageType, PolicyStatus, ResolutionBasis, ResolutionType, ResolvedBy,
};
use crate::maintainer::history::{
    DisputeResolutionEventRecord, PolicyEventKind, PolicyEventRecord,
};
use crate::maintainer::{outbox_io, Maintainer};
use crate::storage::{paths, read_yaml, write_yaml_atomic, FileLockGuard, StorageError};

use super::context::{load_team_claims, resolve_direct_claims};
use super::store::ArbitrationStore;
use super::types::{
    AnalysisError, AnalysisJob, AnalysisState, ArbitrationAnalysis, ArbitrationResolutionRecord,
    DeliveryIntent, DeliveryTargetIntent, MaintainerDisputeRecord, PendingResolutionDelivery,
    ResolutionEventTarget, ARBITRATION_SCHEMA_VERSION,
};

const ARBITRATION_POLICY_NAME: &str = "dispute_arbitration";
const ARBITRATION_POLICY_SCOPE: &str = "maintainer / dispute-arbitration";

struct DeliveryContext {
    context_snapshot_hash: Option<String>,
    snapshot_source_resolution_id: Option<ArbitrationResolutionId>,
}

#[derive(Debug, Clone)]
pub struct HumanResolutionInput {
    pub conclusion: String,
    pub notify_affected_agents: bool,
    pub resolution_type: Option<ResolutionType>,
    pub resolution_basis: Option<ResolutionBasis>,
    pub claim_assessments: Vec<ClaimAssessment>,
}

#[derive(Debug, Clone)]
pub struct RejectResolutionInput {
    pub expected_resolution_id: ArbitrationResolutionId,
    pub rejection_reason: String,
    pub conclusion: String,
    pub resolution_type: Option<ResolutionType>,
    pub resolution_basis: Option<ResolutionBasis>,
    pub claim_assessments: Vec<ClaimAssessment>,
}

/// Resolution 已经关闭 Dispute 后，仍在运行或等待的 Analysis 只能作为审计记录保留，
/// 不能继续携带可调度状态。Adopting 拥有固定 Resolution intent，由其恢复路径处理。
pub(super) fn preempt_analysis_for_resolution(
    analysis: &mut ArbitrationAnalysis,
    now: DateTime<Utc>,
) -> bool {
    if !analysis.state.is_recoverable() || analysis.state == AnalysisState::Adopting {
        return false;
    }
    analysis.state = AnalysisState::Failed;
    analysis.lease = None;
    analysis.next_retry_at = None;
    analysis.error = Some(AnalysisError {
        code: "analysis_preempted".into(),
        message: "Dispute 已由 Resolution 处理".into(),
    });
    analysis.updated_at = now;
    true
}

#[derive(Clone)]
pub struct ResolutionService {
    maintainer: Arc<Maintainer>,
    store: ArbitrationStore,
    event_wake: Arc<Notify>,
    #[cfg(test)]
    commit_failpoint: Option<Arc<std::sync::Mutex<Option<CommitFailpoint>>>>,
    #[cfg(test)]
    adoption_persisted_barrier: Option<Arc<AdoptionPersistedBarrier>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommitFailpoint {
    IntentSaved,
    ResolutionWritten,
    ResolutionCommitted,
    FirstOutboxStored,
    ObservationHandoffStored,
}

#[cfg(test)]
pub(super) struct AdoptionPersistedBarrier {
    used: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
impl AdoptionPersistedBarrier {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            used: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        })
    }

    pub(super) async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    async fn pause_once(&self) {
        if !self.used.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

impl ResolutionService {
    pub fn new(maintainer: Arc<Maintainer>, store: ArbitrationStore) -> Self {
        Self {
            maintainer,
            store,
            event_wake: Arc::new(Notify::new()),
            #[cfg(test)]
            commit_failpoint: None,
            #[cfg(test)]
            adoption_persisted_barrier: None,
        }
    }

    pub(super) fn event_wake(&self) -> Arc<Notify> {
        self.event_wake.clone()
    }

    #[cfg(test)]
    pub(super) fn with_commit_failpoint(mut self, point: CommitFailpoint) -> Self {
        self.commit_failpoint = Some(Arc::new(std::sync::Mutex::new(Some(point))));
        self
    }

    #[cfg(test)]
    pub(super) fn with_adoption_persisted_barrier(
        mut self,
        barrier: Arc<AdoptionPersistedBarrier>,
    ) -> Self {
        self.adoption_persisted_barrier = Some(barrier);
        self
    }

    pub fn store(&self) -> &ArbitrationStore {
        &self.store
    }

    pub async fn resolve_human(
        &self,
        dispute_id: &DisputeId,
        input: HumanResolutionInput,
        now: DateTime<Utc>,
    ) -> anyhow::Result<ArbitrationResolutionRecord> {
        validate_human_input(&input)?;
        let _dispute_guard = self.store.lock_dispute(dispute_id).await?;
        let mut dispute = self.store.read_dispute(dispute_id).await?;
        validate_assessments(&dispute.dispute.claims, &input.claim_assessments)?;
        if dispute.dispute.status == DisputeStatus::Open {
            if let Some(record) = self
                .recover_human_resolution_locked(dispute_id, &mut dispute)
                .await?
            {
                self.preempt_current_analysis_locked(dispute_id, now).await;
                let same_input = human_resolution_matches_input(&record, &input);
                self.persist_pending_delivery(&record, record.created_at)
                    .await?;
                let delivery = self.ensure_delivery(&record).await;
                if !same_input {
                    if let Err(error) = delivery {
                        log::warn!(
                            target: "maintainer_arbitration",
                            "恢复 human resolution={} 后 holder 通知仍待恢复: {error:#}",
                            record.resolution_id
                        );
                    }
                    anyhow::bail!(
                        "已恢复 human resolution={}，本次输入与已持久化 resolution 不一致",
                        record.resolution_id
                    );
                }
                if let Err(error) = delivery {
                    log::warn!(
                        target: "maintainer_arbitration",
                        "human resolution={} 已恢复，holder 通知仍待后台恢复: {error:#}",
                        record.resolution_id
                    );
                }
                return Ok(record);
            }
        }
        if dispute.dispute.status != DisputeStatus::Open {
            anyhow::bail!("dispute={dispute_id} 已 resolved");
        }
        let snapshots = match self.load_direct_snapshots(&dispute.dispute.claims).await {
            Ok(snapshots) => snapshots,
            Err(_) if !input.notify_affected_agents && input.claim_assessments.is_empty() => {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        let resolution_id = self.mint_resolution_id(dispute_id).await?;
        let resolution = DisputeResolution {
            resolution_id: resolution_id.clone(),
            resolved_by: ResolvedBy::Human,
            resolved_at: now,
            resolution_type: input.resolution_type,
            resolution_basis: input.resolution_basis,
            conclusion: input.conclusion,
            claim_assessments: input.claim_assessments,
            rejection_reason: None,
        };
        let delivery_intent = if input.notify_affected_agents {
            Some(
                self.build_delivery_intent(
                    &dispute.dispute,
                    &snapshots,
                    &resolution,
                    DeliveryContext {
                        context_snapshot_hash: None,
                        snapshot_source_resolution_id: None,
                    },
                    now,
                )
                .await?,
            )
        } else {
            None
        };
        let record = ArbitrationResolutionRecord {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            resolution_id,
            dispute_id: dispute_id.clone(),
            created_at: now,
            resolution: resolution.clone(),
            dispute_snapshot: dispute.dispute.clone(),
            direct_claim_snapshots: snapshots,
            semantic_fingerprint: None,
            context_snapshot_hash: None,
            analysis_source_id: None,
            legacy_source_attempt_id: None,
            delivery_intent,
            snapshot_source_resolution_id: None,
        };
        self.persist_pending_delivery(&record, now).await?;
        self.store.write_resolution_record(&record).await?;
        #[cfg(test)]
        self.maybe_fail(CommitFailpoint::ResolutionWritten)?;
        dispute.dispute.status = DisputeStatus::Resolved;
        dispute.dispute.resolved_at = Some(now);
        dispute.resolution = Some(resolution);
        self.store.write_dispute(&dispute).await?;
        self.preempt_current_analysis_locked(dispute_id, now).await;
        if let Err(error) = self.ensure_delivery(&record).await {
            log::warn!(
                target: "maintainer_arbitration",
                "human resolution={} 已提交，holder 通知待后台恢复: {error:#}",
                record.resolution_id
            );
        }
        Ok(record)
    }

    pub async fn reject_and_replace(
        &self,
        dispute_id: &DisputeId,
        input: RejectResolutionInput,
        now: DateTime<Utc>,
    ) -> anyhow::Result<ArbitrationResolutionRecord> {
        validate_reject_input(&input)?;
        let _dispute_guard = self.store.lock_dispute(dispute_id).await?;
        let mut dispute = self.store.read_dispute(dispute_id).await?;
        let prior_resolution_id = dispute
            .resolution
            .as_ref()
            .map(|resolution| resolution.resolution_id.clone());
        if let Some(record) = self
            .recover_human_resolution_locked(dispute_id, &mut dispute)
            .await?
        {
            if prior_resolution_id.as_ref() != Some(&record.resolution_id) {
                self.preempt_current_analysis_locked(dispute_id, now).await;
                let same_input = replacement_resolution_matches_input(&record, &input);
                self.persist_pending_delivery(&record, record.created_at)
                    .await?;
                let delivery = self.ensure_delivery(&record).await;
                if !same_input {
                    if let Err(error) = delivery {
                        log::warn!(
                            target: "maintainer_arbitration",
                            "恢复 replacement resolution={} 后 holder 通知仍待恢复: {error:#}",
                            record.resolution_id
                        );
                    }
                    anyhow::bail!(
                        "已恢复 replacement resolution={}，本次输入与已持久化 resolution 不一致",
                        record.resolution_id
                    );
                }
                if let Err(error) = delivery {
                    log::warn!(
                        target: "maintainer_arbitration",
                        "replacement resolution={} 已恢复，holder 通知仍待后台恢复: {error:#}",
                        record.resolution_id
                    );
                }
                return Ok(record);
            }
        }
        let current = dispute
            .resolution
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("dispute={dispute_id} 没有可替换的结构化 resolution"))?;
        if current.resolution_id != input.expected_resolution_id {
            anyhow::bail!(
                "当前 resolution={} 与 expected={} 不一致",
                current.resolution_id,
                input.expected_resolution_id
            );
        }
        if current.resolved_by != ResolvedBy::Automatic {
            anyhow::bail!("仅允许驳回当前 automatic record");
        }
        validate_assessments(&dispute.dispute.claims, &input.claim_assessments)?;
        let superseded = self
            .store
            .read_resolution_record(dispute_id, &input.expected_resolution_id)
            .await?;
        let (snapshots, snapshot_source_resolution_id) = match self
            .load_direct_snapshots(&dispute.dispute.claims)
            .await
        {
            Ok(current) => (current, None),
            Err(error) => {
                log::warn!(
                    target: "maintainer_arbitration",
                    "读取替换时 mirror 失败，复用被替换 record 快照: dispute={} error={error:#}",
                    dispute_id
                );
                (
                    superseded.direct_claim_snapshots.clone(),
                    Some(superseded.resolution_id.clone()),
                )
            }
        };
        let resolution_id = self.mint_resolution_id(dispute_id).await?;
        let resolution = DisputeResolution {
            resolution_id: resolution_id.clone(),
            resolved_by: ResolvedBy::Human,
            resolved_at: now,
            resolution_type: input.resolution_type,
            resolution_basis: input.resolution_basis,
            conclusion: input.conclusion,
            claim_assessments: input.claim_assessments,
            rejection_reason: Some(input.rejection_reason),
        };
        let delivery_intent = Some(
            self.build_delivery_intent(
                &dispute.dispute,
                &snapshots,
                &resolution,
                DeliveryContext {
                    context_snapshot_hash: superseded.context_snapshot_hash.clone(),
                    snapshot_source_resolution_id: snapshot_source_resolution_id.clone(),
                },
                now,
            )
            .await?,
        );
        let record = ArbitrationResolutionRecord {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            resolution_id,
            dispute_id: dispute_id.clone(),
            created_at: now,
            resolution: resolution.clone(),
            dispute_snapshot: dispute.dispute.clone(),
            direct_claim_snapshots: snapshots,
            semantic_fingerprint: superseded.semantic_fingerprint.clone(),
            context_snapshot_hash: superseded.context_snapshot_hash.clone(),
            analysis_source_id: None,
            legacy_source_attempt_id: None,
            delivery_intent,
            snapshot_source_resolution_id,
        };
        self.persist_pending_delivery(&record, now).await?;
        self.store.write_resolution_record(&record).await?;
        #[cfg(test)]
        self.maybe_fail(CommitFailpoint::ResolutionWritten)?;
        dispute.dispute.status = DisputeStatus::Resolved;
        dispute.dispute.resolved_at = Some(now);
        dispute.resolution = Some(resolution);
        self.store.write_dispute(&dispute).await?;
        self.preempt_current_analysis_locked(dispute_id, now).await;
        if let Err(error) = self.ensure_delivery(&record).await {
            log::warn!(
                target: "maintainer_arbitration",
                "replacement resolution={} 已提交，holder 通知待后台恢复: {error:#}",
                record.resolution_id
            );
        }
        Ok(record)
    }

    /// 调用方持有 per-dispute lock。Resolution 已经 durable，因此 Analysis 审计状态
    /// 写入失败只记录 warning；启动恢复仍会在执行模型前再次终止它。
    async fn preempt_current_analysis_locked(&self, dispute_id: &DisputeId, now: DateTime<Utc>) {
        let result = async {
            let Some(mut analysis) = self.store.read_current_analysis(dispute_id).await? else {
                return Ok::<(), anyhow::Error>(());
            };
            if preempt_analysis_for_resolution(&mut analysis, now) {
                self.store.write_analysis(&analysis).await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = result {
            log::warn!(
                target: "maintainer_arbitration",
                "Resolution 已提交但终止 current analysis 失败: dispute={} error={error:#}",
                dispute_id
            );
        }
    }

    /// 调用方持有 per-dispute guard。
    pub(super) async fn begin_analysis_adoption_locked(
        &self,
        job: &AnalysisJob,
        resolved_by: ResolvedBy,
        dispute_guard: &FileLockGuard,
        now: DateTime<Utc>,
    ) -> anyhow::Result<ArbitrationResolutionRecord> {
        let _held_lock = dispute_guard;
        let mut analysis = self.store.read_analysis(job).await?;
        if analysis.state != AnalysisState::Approved {
            anyhow::bail!("analysis state={:?} 不能采用", analysis.state);
        }
        let mut dispute = self.store.read_dispute(&job.dispute_id).await?;
        if let Some(human) = self
            .recover_human_resolution_locked(&job.dispute_id, &mut dispute)
            .await?
        {
            if let Err(error) = self.ensure_delivery(&human).await {
                log::warn!(
                    target: "maintainer_arbitration",
                    "human resolution={} 已优先恢复，holder 通知仍待恢复: {error:#}",
                    human.resolution_id
                );
            }
            anyhow::bail!(
                "dispute 已由 human resolution={} Resolve",
                human.resolution_id
            );
        }
        if dispute.dispute.status != DisputeStatus::Open || dispute.resolution.is_some() {
            anyhow::bail!("dispute={} 已由其他 resolution 关闭", job.dispute_id);
        }
        let proposal = analysis
            .proposal
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("analysis 缺少 proposal"))?;
        if !proposal.resolution_type.is_resolved() {
            anyhow::bail!("unresolved analysis 不能采用");
        }
        let context = analysis
            .context
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("analysis 缺少冻结上下文"))?;
        let fingerprint = analysis
            .semantic_fingerprint
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("analysis 缺少 semantic fingerprint"))?;
        let snapshot_hash = analysis
            .context_snapshot_hash
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("analysis 缺少 context snapshot hash"))?;
        let resolution_id = self.mint_resolution_id(&job.dispute_id).await?;
        let resolution = DisputeResolution {
            resolution_id: resolution_id.clone(),
            resolved_by,
            resolved_at: now,
            resolution_type: Some(proposal.resolution_type),
            resolution_basis: Some(proposal.resolution_basis),
            conclusion: proposal.conclusion.clone(),
            claim_assessments: proposal.claim_assessments.clone(),
            rejection_reason: None,
        };
        let intent = self
            .build_delivery_intent(
                &context.dispute,
                &context.direct_claims,
                &resolution,
                DeliveryContext {
                    context_snapshot_hash: Some(snapshot_hash.clone()),
                    snapshot_source_resolution_id: None,
                },
                now,
            )
            .await?;
        let record = ArbitrationResolutionRecord {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            resolution_id: resolution_id.clone(),
            dispute_id: job.dispute_id.clone(),
            created_at: now,
            resolution: resolution.clone(),
            dispute_snapshot: context.dispute.clone(),
            direct_claim_snapshots: context.direct_claims.clone(),
            semantic_fingerprint: Some(fingerprint.clone()),
            context_snapshot_hash: Some(snapshot_hash.clone()),
            analysis_source_id: Some(analysis.analysis_id.clone()),
            legacy_source_attempt_id: None,
            delivery_intent: Some(intent),
            snapshot_source_resolution_id: None,
        };
        analysis.state = AnalysisState::Adopting;
        analysis.resolution_id = Some(resolution_id);
        analysis.pending_resolution = Some(record.clone());
        analysis.lease = None;
        analysis.updated_at = now;
        self.store.write_analysis(&analysis).await?;
        #[cfg(test)]
        if let Some(barrier) = self.adoption_persisted_barrier.as_ref() {
            barrier.pause_once().await;
        }
        #[cfg(test)]
        self.maybe_fail(CommitFailpoint::IntentSaved)?;
        self.finish_analysis_adoption_locked(&mut analysis, &mut dispute, &record, now)
            .await?;
        Ok(record)
    }

    pub async fn recover_analysis_adoption(
        &self,
        job: &AnalysisJob,
        now: DateTime<Utc>,
    ) -> anyhow::Result<AnalysisState> {
        let _dispute_guard = self.store.lock_dispute(&job.dispute_id).await?;
        let mut analysis = self.store.read_analysis(job).await?;
        if analysis.state != AnalysisState::Adopting {
            return Ok(analysis.state);
        }
        let resolution_record = self.fixed_analysis_resolution(job, &analysis).await?;
        if resolution_record.resolution.resolved_by == ResolvedBy::Automatic {
            if let Some(owner) = self
                .find_human_analysis_adoption_owner(&job.dispute_id)
                .await?
            {
                if owner != *job {
                    let reason = format!(
                        "human analysis={} 已固定 adoption intent，automatic adoption 终止",
                        owner.analysis_id
                    );
                    analysis.state = AnalysisState::Approved;
                    analysis.adoption_blocked_reason = Some(reason.clone());
                    analysis.error = Some(AnalysisError {
                        code: "adoption_preempted_by_human".into(),
                        message: reason,
                    });
                    analysis.updated_at = now;
                    self.store.write_analysis(&analysis).await?;
                    return Ok(AnalysisState::Approved);
                }
            }
        }
        let mut dispute = self.store.read_dispute(&job.dispute_id).await?;
        if let Some(current) = dispute.resolution.as_ref() {
            if current.resolution_id != resolution_record.resolution_id {
                let reason = format!(
                    "analysis adoption 已被 current resolution={} 抢先",
                    current.resolution_id
                );
                analysis.state = AnalysisState::Approved;
                analysis.pending_resolution = None;
                analysis.adoption_blocked_reason = Some(reason.clone());
                analysis.error = Some(AnalysisError {
                    code: "adoption_preempted".into(),
                    message: reason,
                });
                analysis.updated_at = now;
                self.store.write_analysis(&analysis).await?;
                return Ok(AnalysisState::Approved);
            }
        }
        self.finish_analysis_adoption_locked(&mut analysis, &mut dispute, &resolution_record, now)
            .await?;
        Ok(analysis.state)
    }

    /// 调用方持有 per-dispute guard。若显式 Adopt 已经固定 intent，重复请求只恢复
    /// 该 Resolution，不重建上下文或 mint ID。
    pub(super) async fn resume_matching_analysis_adoption_locked(
        &self,
        job: &AnalysisJob,
        expected_resolved_by: ResolvedBy,
        dispute_guard: &FileLockGuard,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Option<ArbitrationResolutionRecord>> {
        let _held_lock = dispute_guard;
        if let Some(owner) = self
            .find_human_analysis_adoption_owner(&job.dispute_id)
            .await?
        {
            if owner != *job {
                anyhow::bail!(
                    "dispute={} 已由 analysis={} 固定 human adoption intent",
                    job.dispute_id,
                    owner.analysis_id
                );
            }
        }
        let mut analysis = self.store.read_analysis(job).await?;
        if !matches!(
            analysis.state,
            AnalysisState::Adopting | AnalysisState::Adopted
        ) {
            return Ok(None);
        }
        let resolution_record = self.fixed_analysis_resolution(job, &analysis).await?;
        if resolution_record.resolution.resolved_by != expected_resolved_by {
            anyhow::bail!(
                "analysis={} 已由 {:?} adoption 使用",
                analysis.analysis_id,
                resolution_record.resolution.resolved_by
            );
        }
        let mut dispute = self.store.read_dispute(&job.dispute_id).await?;
        if analysis.state == AnalysisState::Adopted {
            if dispute
                .resolution
                .as_ref()
                .is_some_and(|current| current.resolution_id == resolution_record.resolution_id)
            {
                return Ok(Some(resolution_record));
            }
            anyhow::bail!(
                "analysis={} 已采用的 resolution={} 不再是当前 resolution",
                analysis.analysis_id,
                resolution_record.resolution_id
            );
        }
        if let Some(current) = dispute.resolution.as_ref() {
            if current.resolution_id != resolution_record.resolution_id {
                let reason = format!(
                    "analysis adoption 已被 current resolution={} 抢先",
                    current.resolution_id
                );
                analysis.state = AnalysisState::Approved;
                analysis.pending_resolution = None;
                analysis.adoption_blocked_reason = Some(reason.clone());
                analysis.error = Some(AnalysisError {
                    code: "adoption_preempted".into(),
                    message: reason,
                });
                analysis.updated_at = now;
                self.store.write_analysis(&analysis).await?;
                anyhow::bail!(
                    "analysis={} adoption 已被 resolution={} 抢先",
                    analysis.analysis_id,
                    current.resolution_id
                );
            }
        }
        self.finish_analysis_adoption_locked(&mut analysis, &mut dispute, &resolution_record, now)
            .await?;
        Ok(Some(resolution_record))
    }

    async fn find_human_analysis_adoption_owner(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<Option<AnalysisJob>> {
        let Some(analysis) = self.store.read_current_analysis(dispute_id).await? else {
            return Ok(None);
        };
        if analysis.state != AnalysisState::Adopting {
            return Ok(None);
        }
        let candidate = AnalysisJob {
            dispute_id: dispute_id.clone(),
            analysis_id: analysis.analysis_id.clone(),
        };
        let record = self
            .fixed_analysis_resolution(&candidate, &analysis)
            .await?;
        Ok((record.resolution.resolved_by == ResolvedBy::Human).then_some(candidate))
    }

    async fn fixed_analysis_resolution(
        &self,
        job: &AnalysisJob,
        analysis: &ArbitrationAnalysis,
    ) -> anyhow::Result<ArbitrationResolutionRecord> {
        let resolution_id = analysis
            .resolution_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("adopting analysis 缺少固定 resolution intent"))?;
        let record = match analysis.pending_resolution.clone() {
            Some(record) => record,
            None => {
                self.store
                    .read_resolution_record(&job.dispute_id, resolution_id)
                    .await?
            }
        };
        if &record.resolution_id != resolution_id {
            anyhow::bail!("adopting analysis 的 resolution intent ID 不一致");
        }
        Ok(record)
    }

    async fn finish_analysis_adoption_locked(
        &self,
        analysis: &mut ArbitrationAnalysis,
        dispute: &mut MaintainerDisputeRecord,
        resolution_record: &ArbitrationResolutionRecord,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.persist_pending_delivery(resolution_record, now)
            .await?;
        self.store
            .write_resolution_record(resolution_record)
            .await?;
        #[cfg(test)]
        self.maybe_fail(CommitFailpoint::ResolutionWritten)?;
        if dispute.dispute.status == DisputeStatus::Open {
            dispute.dispute.status = DisputeStatus::Resolved;
            dispute.dispute.resolved_at = Some(resolution_record.resolution.resolved_at);
            dispute.resolution = Some(resolution_record.resolution.clone());
            self.store.write_dispute(dispute).await?;
        }
        #[cfg(test)]
        self.maybe_fail(CommitFailpoint::ResolutionCommitted)?;
        match self.ensure_delivery(resolution_record).await {
            Ok(()) => {
                analysis.state = AnalysisState::Adopted;
                analysis.delivery_error = None;
                analysis.pending_resolution = None;
            }
            Err(error) => {
                log::warn!(
                    target: "maintainer_arbitration",
                    "resolution={} 投递待恢复: {error:#}",
                    resolution_record.resolution_id
                );
                analysis.state = AnalysisState::Adopting;
                analysis.delivery_error =
                    Some("holder 通知投递待恢复；详见 Maintainer 日志".into());
            }
        }
        analysis.updated_at = now;
        self.store.write_analysis(analysis).await?;
        Ok(())
    }

    pub async fn ensure_delivery(
        &self,
        resolution_record: &ArbitrationResolutionRecord,
    ) -> anyhow::Result<()> {
        if let Some(intent) = resolution_record.delivery_intent.as_ref() {
            let _process_guard = self.maintainer.outbox_lock.lock().await;
            let _file_guard = self.maintainer.lock_outbox_file().await?;
            ensure_policy(self.maintainer.team_root(), &intent.policy).await?;
            for (index, target) in intent.targets.iter().enumerate() {
                #[cfg(not(test))]
                let _ = index;
                ensure_outbox_entry(
                    self.maintainer.team_root(),
                    &OutboxEntry {
                        inbox_id: target.inbox_id.clone(),
                        maintainer_action_id: intent.maintainer_action_id.clone(),
                        target: OutboxTarget::Targeted {
                            target_agent: target.target_agent.clone(),
                        },
                        created_at: resolution_record.created_at,
                        offered_to: Vec::new(),
                        delivered_to: Vec::new(),
                        inbox_message: target.inbox_message.clone(),
                    },
                )
                .await?;
                if index == 0 {
                    #[cfg(test)]
                    self.maybe_fail(CommitFailpoint::FirstOutboxStored)?;
                }
            }
        }
        self.store
            .register_resolution_event_targets(resolution_record)
            .await?;
        self.store
            .write_pending_observation(&ResolutionEventTarget {
                dispute_id: resolution_record.dispute_id.clone(),
                resolution_id: resolution_record.resolution_id.clone(),
            })
            .await?;
        self.ensure_resolution_history(resolution_record).await?;
        // pending delivery 是前序恢复依据。必须先把 observation 的索引与 durable
        // marker 写稳，再消费它；任一中途崩溃都会从同一固定 intent 幂等重放。
        #[cfg(test)]
        self.maybe_fail(CommitFailpoint::ObservationHandoffStored)?;
        self.store
            .remove_pending_delivery(&resolution_record.resolution_id)
            .await?;
        self.event_wake.notify_one();
        Ok(())
    }

    async fn persist_pending_delivery(
        &self,
        record: &ArbitrationResolutionRecord,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let target = ResolutionEventTarget {
            dispute_id: record.dispute_id.clone(),
            resolution_id: record.resolution_id.clone(),
        };
        if let Some(existing) = self
            .store
            .read_pending_delivery(&record.resolution_id)
            .await?
        {
            if existing.target != target {
                anyhow::bail!(
                    "resolution={} pending delivery target 不一致",
                    record.resolution_id
                );
            }
            if existing
                .resolution_record
                .as_deref()
                .is_some_and(|stored| stored != record)
            {
                anyhow::bail!(
                    "resolution={} pending commit intent 内容不一致",
                    record.resolution_id
                );
            }
            if existing.resolution_record.is_none() {
                let mut upgraded = existing;
                upgraded.resolution_record = Some(Box::new(record.clone()));
                self.store.write_pending_delivery(&upgraded).await?;
            }
            // 恢复 adopting intent 时不能把事件调度器已经累计的退避重置为 0。
            self.event_wake.notify_one();
            return Ok(());
        }
        self.store
            .write_pending_delivery(&PendingResolutionDelivery {
                schema_version: ARBITRATION_SCHEMA_VERSION,
                target,
                resolution_record: Some(Box::new(record.clone())),
                created_at: now,
                retry_count: 0,
                next_retry_at: None,
            })
            .await?;
        self.event_wake.notify_one();
        Ok(())
    }

    pub async fn recover_pending_delivery(
        &self,
        target: &ResolutionEventTarget,
    ) -> anyhow::Result<()> {
        let _guard = self.store.lock_dispute(&target.dispute_id).await?;
        let pending = self
            .store
            .read_pending_delivery(&target.resolution_id)
            .await?;
        let current_record = self
            .store
            .read_current_resolution_record(&target.dispute_id)
            .await?;
        let record = pending
            .as_ref()
            .and_then(|pending| pending.resolution_record.as_deref().cloned())
            .or_else(|| {
                current_record
                    .clone()
                    .filter(|record| record.resolution_id == target.resolution_id)
            });
        let Some(record) = record else {
            self.store
                .remove_pending_delivery(&target.resolution_id)
                .await?;
            return Ok(());
        };
        if record.resolution_id != target.resolution_id || record.dispute_id != target.dispute_id {
            anyhow::bail!("pending resolution commit intent 与事件 target 不一致");
        }
        let mut dispute = self.store.read_dispute(&target.dispute_id).await?;
        let is_current = dispute
            .resolution
            .as_ref()
            .is_some_and(|resolution| resolution.resolution_id == target.resolution_id);
        let can_recover_commit = dispute.resolution.is_none()
            || (record.resolution.resolved_by == ResolvedBy::Human
                && record.resolution.rejection_reason.is_some()
                && dispute
                    .resolution
                    .as_ref()
                    .is_some_and(|current| current.resolved_by == ResolvedBy::Automatic));
        if !is_current && can_recover_commit {
            self.store.write_resolution_record(&record).await?;
            dispute.dispute.status = DisputeStatus::Resolved;
            dispute.dispute.resolved_at = Some(record.resolution.resolved_at);
            dispute.resolution = Some(record.resolution.clone());
            self.store.write_dispute(&dispute).await?;
        } else if !is_current {
            self.store
                .remove_pending_delivery(&target.resolution_id)
                .await?;
            return Ok(());
        }
        if current_record
            .as_ref()
            .is_none_or(|current| current.resolution_id != target.resolution_id)
        {
            self.store.write_resolution_record(&record).await?;
        }
        self.ensure_delivery(&record).await
    }

    async fn ensure_resolution_history(
        &self,
        record: &ArbitrationResolutionRecord,
    ) -> anyhow::Result<()> {
        let history = self.maintainer.history_store();
        if let Some(intent) = record.delivery_intent.as_ref() {
            history
                .ensure_policy_event(&PolicyEventRecord {
                    event_id: format!("resolution_policy:{}", record.resolution_id),
                    policy_id: intent.policy.id.clone(),
                    event_kind: PolicyEventKind::ClaimAttributeUpdatePublished,
                    occurred_at: intent.policy.updated_at.unwrap_or(intent.policy.created_at),
                    policy_name: intent.policy.name.clone(),
                    policy_scope: intent.policy.scope.clone(),
                    policy_status: intent.policy.status,
                    message_type: intent.policy.message_type,
                    target_agents: intent.policy.target_agents.clone().unwrap_or_default(),
                    statement: intent.policy.statement.clone(),
                })
                .await?;
        }
        history
            .ensure_dispute_resolution_event(&DisputeResolutionEventRecord {
                event_id: format!("resolution:{}", record.resolution_id),
                dispute_id: record.dispute_id.clone(),
                occurred_at: record.resolution.resolved_at,
                summary: Some(record.resolution.conclusion.clone()),
            })
            .await
    }

    /// pending delivery 已成功消费后，把来源 Analysis 从 adopting 收敛为 adopted。
    /// 这里只更新同一固定 intent 的执行状态，不重建 Resolution 或投递 ID。
    pub async fn complete_analysis_adoption_after_delivery(
        &self,
        target: &ResolutionEventTarget,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let _dispute_guard = self.store.lock_dispute(&target.dispute_id).await?;
        let Some(record) = self
            .store
            .read_current_resolution_record(&target.dispute_id)
            .await?
        else {
            return Ok(());
        };
        if record.resolution_id != target.resolution_id {
            return Ok(());
        }
        let Some(source_id) = record.analysis_source_id.as_ref() else {
            return Ok(());
        };
        let Some(mut analysis) = self.store.read_current_analysis(&target.dispute_id).await? else {
            return Ok(());
        };
        if analysis.analysis_id != *source_id {
            return Ok(());
        }
        if analysis.state != AnalysisState::Adopting {
            return Ok(());
        }
        if analysis.resolution_id.as_ref() != Some(&target.resolution_id) {
            anyhow::bail!(
                "analysis={} adopting resolution 与已投递 Resolution 不一致",
                analysis.analysis_id
            );
        }
        analysis.state = AnalysisState::Adopted;
        analysis.delivery_error = None;
        analysis.pending_resolution = None;
        analysis.updated_at = now;
        self.store.write_analysis(&analysis).await
    }

    #[cfg(test)]
    fn maybe_fail(&self, point: CommitFailpoint) -> anyhow::Result<()> {
        let Some(failpoint) = self.commit_failpoint.as_ref() else {
            return Ok(());
        };
        let mut armed = failpoint
            .lock()
            .map_err(|_| anyhow::anyhow!("commit failpoint lock poisoned"))?;
        if *armed == Some(point) {
            *armed = None;
            anyhow::bail!("test commit failpoint: {point:?}");
        }
        Ok(())
    }

    async fn recover_human_resolution_locked(
        &self,
        dispute_id: &DisputeId,
        dispute: &mut MaintainerDisputeRecord,
    ) -> anyhow::Result<Option<ArbitrationResolutionRecord>> {
        let current = self
            .store
            .read_current_resolution_record(dispute_id)
            .await?;
        let mut pending_human = self
            .store
            .list_pending_deliveries()
            .await?
            .into_iter()
            .filter_map(|pending| pending.resolution_record.map(|record| *record))
            .filter(|record| {
                record.dispute_id == *dispute_id
                    && record.resolution.resolved_by == ResolvedBy::Human
            });
        let pending = pending_human.next();
        if pending_human.next().is_some() {
            anyhow::bail!("dispute={dispute_id} 存在多个 human resolution commit intent");
        }
        let stored = pending.or(current);
        if let Some(record) = stored
            .as_ref()
            .filter(|record| record.resolution.resolved_by == ResolvedBy::Human)
        {
            let recovers_open = dispute.resolution.is_none();
            let recovers_replacement = record.resolution.rejection_reason.is_some()
                && dispute.resolution.as_ref().is_some_and(|current| {
                    current.resolved_by == ResolvedBy::Automatic
                        && current.resolution_id != record.resolution_id
                });
            if recovers_open || recovers_replacement {
                self.store.write_resolution_record(record).await?;
                dispute.dispute.status = DisputeStatus::Resolved;
                dispute.dispute.resolved_at = Some(record.resolution.resolved_at);
                dispute.resolution = Some(record.resolution.clone());
                self.store.write_dispute(dispute).await?;
            }
        }
        let Some(current) = dispute.resolution.as_ref() else {
            return Ok(None);
        };
        if current.resolved_by != ResolvedBy::Human {
            return Ok(None);
        }
        let record = stored.ok_or_else(|| anyhow::anyhow!("当前 human resolution 缺少持久记录"))?;
        if record.resolution_id != current.resolution_id {
            anyhow::bail!("Dispute 当前 resolution 与持久记录不一致");
        }
        Ok(Some(record))
    }

    async fn build_delivery_intent(
        &self,
        dispute: &crate::claim::Dispute,
        snapshots: &[crate::claim::Claim],
        resolution: &DisputeResolution,
        delivery_context: DeliveryContext,
        now: DateTime<Utc>,
    ) -> anyhow::Result<DeliveryIntent> {
        let holders: BTreeSet<AgentId> = snapshots
            .iter()
            .filter(|claim| dispute.claims.contains(&claim.id))
            .map(|claim| claim.holder.clone())
            .collect();
        if holders.is_empty() {
            anyhow::bail!("无法为 dispute={} 找到 holder", dispute.id);
        }
        let _process_guard = self.maintainer.outbox_lock.lock().await;
        let _file_guard = self.maintainer.lock_outbox_file().await?;
        let policy_id = self.maintainer.mint_policy_id().await?;
        let action_id = self.mint_action_id().await?;
        let target_agents: Vec<AgentId> = holders.iter().cloned().collect();
        let policy = Policy {
            id: policy_id,
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: ARBITRATION_POLICY_NAME.into(),
            statement: arbitration_policy_statement(dispute, snapshots, resolution),
            scope: ARBITRATION_POLICY_SCOPE.into(),
            status: PolicyStatus::Active,
            created_at: now,
            updated_at: None,
            target_agents: Some(target_agents.clone()),
        };
        let resolution_context = Some(Box::new(ArbitrationResolutionContext {
            dispute_id: dispute.id.clone(),
            resolution: resolution.clone(),
            context_snapshot_hash: delivery_context.context_snapshot_hash,
            dispute_snapshot: dispute.clone(),
            direct_claim_snapshots: snapshots.to_vec(),
            snapshot_source_resolution_id: delivery_context.snapshot_source_resolution_id,
        }));
        let mut targets = Vec::with_capacity(target_agents.len());
        for target_agent in target_agents {
            let inbox_id = self.maintainer.mint_inbox_id_for_outbox().await?;
            targets.push(DeliveryTargetIntent {
                inbox_id: inbox_id.clone(),
                target_agent,
                inbox_message: InboxMessage {
                    id: inbox_id,
                    kind: InboxMessageKind::ClaimAttributeUpdate {
                        policy: policy.clone(),
                        arbitration_resolution: resolution_context.clone(),
                    },
                    handled_at: None,
                },
            });
        }
        Ok(DeliveryIntent {
            policy,
            maintainer_action_id: action_id,
            targets,
        })
    }

    async fn mint_resolution_id(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<ArbitrationResolutionId> {
        for _ in 0..self.maintainer.id_mint_max_attempts.max(1) {
            let candidate = ArbitrationResolutionId::random();
            if !self
                .store
                .resolution_id_exists(dispute_id, &candidate)
                .await?
            {
                return Ok(candidate);
            }
        }
        anyhow::bail!("无法为 dispute={dispute_id} 生成唯一 resolution id")
    }

    async fn mint_action_id(&self) -> anyhow::Result<crate::claim::MaintainerActionId> {
        let mut used: BTreeSet<crate::claim::MaintainerActionId> =
            outbox_io::list(self.maintainer.team_root())
                .await?
                .into_iter()
                .map(|entry| entry.maintainer_action_id)
                .collect();
        for dispute in self.store.list_disputes().await? {
            if let Some(resolution) = self
                .store
                .read_current_resolution_record(&dispute.dispute.id)
                .await?
                .and_then(|record| record.delivery_intent)
            {
                used.insert(resolution.maintainer_action_id);
            }
        }
        for _ in 0..self.maintainer.id_mint_max_attempts.max(1) {
            let candidate = crate::claim::MaintainerActionId::random();
            if !used.contains(&candidate) {
                return Ok(candidate);
            }
        }
        anyhow::bail!("无法生成唯一 arbitration action id")
    }

    async fn load_direct_snapshots(
        &self,
        claim_ids: &[crate::claim::ClaimId],
    ) -> anyhow::Result<Vec<crate::claim::Claim>> {
        let all = load_team_claims(self.maintainer.team_root()).await?;
        resolve_direct_claims(claim_ids, &all)
    }
}

fn human_resolution_matches_input(
    record: &ArbitrationResolutionRecord,
    input: &HumanResolutionInput,
) -> bool {
    let resolution = &record.resolution;
    resolution.rejection_reason.is_none()
        && resolution.conclusion == input.conclusion
        && resolution.resolution_type == input.resolution_type
        && resolution.resolution_basis == input.resolution_basis
        && assessments_match(&resolution.claim_assessments, &input.claim_assessments)
        && record.delivery_intent.is_some() == input.notify_affected_agents
}

fn replacement_resolution_matches_input(
    record: &ArbitrationResolutionRecord,
    input: &RejectResolutionInput,
) -> bool {
    let resolution = &record.resolution;
    resolution.rejection_reason.as_ref() == Some(&input.rejection_reason)
        && resolution.conclusion == input.conclusion
        && resolution.resolution_type == input.resolution_type
        && resolution.resolution_basis == input.resolution_basis
        && assessments_match(&resolution.claim_assessments, &input.claim_assessments)
        && record.delivery_intent.is_some()
}

fn assessments_match(left: &[ClaimAssessment], right: &[ClaimAssessment]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .all(|assessment| right.iter().any(|candidate| candidate == assessment))
}

fn validate_human_input(input: &HumanResolutionInput) -> anyhow::Result<()> {
    if input.conclusion.trim().is_empty() {
        anyhow::bail!("human resolution conclusion 不能为空");
    }
    if input
        .resolution_type
        .is_some_and(|kind| !kind.is_resolved())
    {
        anyhow::bail!("human Resolve 不能使用 unresolved resolution_type");
    }
    if input.resolution_basis == Some(ResolutionBasis::InsufficientEvidence) {
        anyhow::bail!("human Resolve 不能使用 insufficient_evidence resolution_basis");
    }
    if !input.claim_assessments.is_empty() && !input.notify_affected_agents {
        anyhow::bail!("提供 claim_assessments 时必须通知 holder");
    }
    Ok(())
}

fn arbitration_policy_statement(
    dispute: &crate::claim::Dispute,
    snapshots: &[crate::claim::Claim],
    resolution: &DisputeResolution,
) -> String {
    let mut lines = vec![
        format!("Maintainer 已裁决 dispute {}。", dispute.id),
        format!("Resolution: {}", resolution.resolution_id),
        format!("Conclusion: {}", resolution.conclusion),
        format!("Original dispute: {}", dispute.summary),
        "Direct claims:".to_string(),
    ];
    for claim in snapshots {
        lines.push(format!(
            "- {} holder={} status={:?} scope={} statement={}",
            claim.id, claim.holder, claim.status, claim.scope, claim.statement
        ));
    }
    if !resolution.claim_assessments.is_empty() {
        lines.push("Assessments:".to_string());
        for assessment in &resolution.claim_assessments {
            lines.push(format!(
                "- {} recommended_status={:?} assessment={} reason={}",
                assessment.claim_id,
                assessment.recommended_status,
                assessment.assessment,
                assessment.reason
            ));
        }
    }
    lines.join("\n")
}

fn validate_reject_input(input: &RejectResolutionInput) -> anyhow::Result<()> {
    if input.rejection_reason.trim().is_empty() || input.conclusion.trim().is_empty() {
        anyhow::bail!("rejection_reason 和 conclusion 不能为空");
    }
    if input
        .resolution_type
        .is_some_and(|kind| !kind.is_resolved())
    {
        anyhow::bail!("replacement resolution 不能使用 unresolved resolution_type");
    }
    if input.resolution_basis == Some(ResolutionBasis::InsufficientEvidence) {
        anyhow::bail!("replacement resolution 不能使用 insufficient_evidence resolution_basis");
    }
    Ok(())
}

pub(super) fn validate_assessments(
    direct_claim_ids: &[crate::claim::ClaimId],
    assessments: &[ClaimAssessment],
) -> anyhow::Result<()> {
    if assessments.is_empty() {
        return Ok(());
    }
    let expected: BTreeSet<_> = direct_claim_ids.iter().cloned().collect();
    let actual: BTreeSet<_> = assessments
        .iter()
        .map(|assessment| assessment.claim_id.clone())
        .collect();
    if actual.len() != assessments.len() || actual != expected {
        anyhow::bail!("claim_assessments 必须完整且唯一覆盖全部直接 Claim");
    }
    for assessment in assessments {
        if assessment.assessment.trim().is_empty()
            || assessment.reason.trim().is_empty()
            || assessment
                .recommended_scope
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || assessment
                .recommended_statement
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            anyhow::bail!("claim assessment 的文本字段不能是空白字符串");
        }
    }
    Ok(())
}

async fn ensure_policy(team_root: &std::path::Path, policy: &Policy) -> anyhow::Result<()> {
    let path = paths::team_store_policies_dir(team_root).join(format!("{}.yaml", policy.id));
    match read_yaml::<Policy>(&path).await {
        Ok(existing) if existing == *policy => return Ok(()),
        Ok(_) => anyhow::bail!("policy id={} 已存在但内容不同", policy.id),
        Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {}
        Err(StorageError::Decode { .. }) if is_zero_length(&path).await? => {}
        Err(error) => return Err(error.into()),
    }
    write_yaml_atomic(&path, policy).await?;
    Ok(())
}

async fn ensure_outbox_entry(
    team_root: &std::path::Path,
    wanted: &OutboxEntry,
) -> anyhow::Result<()> {
    let path = paths::team_store_outbox_dir(team_root).join(format!("{}.yaml", wanted.inbox_id));
    match read_yaml::<OutboxEntry>(&path).await {
        Ok(existing) => {
            if existing.inbox_id != wanted.inbox_id
                || existing.maintainer_action_id != wanted.maintainer_action_id
                || existing.target != wanted.target
                || existing.created_at != wanted.created_at
                || existing.inbox_message != wanted.inbox_message
            {
                anyhow::bail!(
                    "outbox inbox_id={} 已存在但 immutable payload 不同",
                    wanted.inbox_id
                );
            }
            return Ok(());
        }
        Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {}
        Err(StorageError::Decode { .. }) if is_zero_length(&path).await? => {}
        Err(error) => return Err(error.into()),
    }
    outbox_io::write(team_root, wanted).await?;
    Ok(())
}

async fn is_zero_length(path: &std::path::Path) -> anyhow::Result<bool> {
    Ok(tokio::fs::metadata(path)
        .await
        .with_context(|| format!("读取占位文件 metadata 失败: {path:?}"))?
        .len()
        == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formal_resolutions_reject_insufficient_evidence_basis() {
        let human = HumanResolutionInput {
            conclusion: "人工结论".into(),
            notify_affected_agents: false,
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::InsufficientEvidence),
            claim_assessments: Vec::new(),
        };
        assert!(validate_human_input(&human)
            .unwrap_err()
            .to_string()
            .contains("insufficient_evidence"));

        let replacement = RejectResolutionInput {
            expected_resolution_id: ArbitrationResolutionId::random(),
            rejection_reason: "人工复核".into(),
            conclusion: "替换结论".into(),
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::InsufficientEvidence),
            claim_assessments: Vec::new(),
        };
        assert!(validate_reject_input(&replacement)
            .unwrap_err()
            .to_string()
            .contains("insufficient_evidence"));
    }
}
