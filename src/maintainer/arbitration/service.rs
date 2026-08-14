//! 单 automatic analysis、显式 manual analysis 与采用状态机。

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rand::RngCore;
use tokio_util::sync::CancellationToken;

use crate::claim::{ClaimId, Dispute, DisputeId, DisputeStatus, ResolutionType, ResolvedBy};
use crate::config::{ArbitrationMode, MaintainerArbitrationConfig};

use super::context::{is_context_not_ready, ArbitrationContextBuilder, BuiltArbitrationContext};
use super::evaluator::ArbitrationEvaluator;
use super::resolution::{validate_assessments, AdoptionGuards, ResolutionService};
use super::store::ArbitrationStore;
use super::types::{
    AnalysisError, AnalysisJob, AnalysisLease, AnalysisPhase, AnalysisSource, AnalysisState,
    ArbitrationAnalysis, ArbitrationAnalysisId, ArbitrationResolutionRecord,
    AutomaticAnalysisRound, FrozenArbitrationContext, MaintainerDisputeRecord, VerificationVerdict,
    ARBITRATION_PROMPT_VERSION, ARBITRATION_SCHEMA_VERSION, CURRENT_SEMANTIC_PROJECTION_VERSION,
};

const LEASE_SAFETY_MARGIN: Duration = Duration::from_secs(30);
const DEFAULT_CONTEXT_PREPARE_MAX_ATTEMPTS: usize = 6;
const DEFAULT_CONTEXT_PREPARE_RETRY_DELAY: Duration = Duration::from_millis(500);
const ADOPTION_SNAPSHOT_MAX_ATTEMPTS: usize = 3;
const FIRST_REANALYSIS_DELAY: Duration = Duration::from_secs(5 * 60);
const SECOND_REANALYSIS_DELAY: Duration = Duration::from_secs(15 * 60);
const ANALYSIS_PREEMPTION_CHECK_INTERVAL: Duration = Duration::from_millis(100);

pub trait ArbitrationClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Default)]
pub struct SystemArbitrationClock;

impl ArbitrationClock for SystemArbitrationClock {
    fn now(&self) -> DateTime<Utc> {
        crate::time::now_seconds()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AnalysisConflict(pub String);

pub fn is_analysis_conflict(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AnalysisConflict>().is_some()
}

fn conflict(message: impl Into<String>) -> anyhow::Error {
    AnalysisConflict(message.into()).into()
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct AnalysisRetry(pub String);

pub fn is_analysis_retry(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AnalysisRetry>().is_some()
}

fn retry(message: impl Into<String>) -> anyhow::Error {
    AnalysisRetry(message.into()).into()
}

#[derive(Debug, Clone)]
pub struct ReportDisputeResult {
    pub created: bool,
    pub automatic_analysis: Option<ArbitrationAnalysis>,
    pub should_enqueue: bool,
}

#[derive(Clone)]
pub struct ArbitrationService {
    store: ArbitrationStore,
    context_builder: Arc<ArbitrationContextBuilder>,
    evaluator: Arc<dyn ArbitrationEvaluator>,
    resolution_service: ResolutionService,
    config: MaintainerArbitrationConfig,
    model: String,
    lease_duration: Duration,
    clock: Arc<dyn ArbitrationClock>,
    context_prepare_max_attempts: usize,
    context_prepare_retry_delay: Duration,
    preemption_wake: Arc<tokio::sync::Notify>,
}

impl ArbitrationService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: ArbitrationStore,
        context_builder: ArbitrationContextBuilder,
        evaluator: Arc<dyn ArbitrationEvaluator>,
        resolution_service: ResolutionService,
        config: MaintainerArbitrationConfig,
        model: String,
        phase_timeout: Duration,
        _id_mint_max_attempts: usize,
        clock: Arc<dyn ArbitrationClock>,
    ) -> Self {
        Self {
            store,
            context_builder: Arc::new(context_builder),
            evaluator,
            resolution_service,
            config,
            model,
            lease_duration: phase_timeout.saturating_add(LEASE_SAFETY_MARGIN),
            clock,
            context_prepare_max_attempts: DEFAULT_CONTEXT_PREPARE_MAX_ATTEMPTS,
            context_prepare_retry_delay: DEFAULT_CONTEXT_PREPARE_RETRY_DELAY,
            preemption_wake: Arc::new(tokio::sync::Notify::new()),
        }
    }

    #[cfg(test)]
    fn with_context_retry(mut self, max_attempts: usize, delay: Duration) -> Self {
        self.context_prepare_max_attempts = max_attempts.max(1);
        self.context_prepare_retry_delay = delay;
        self
    }

    pub fn store(&self) -> &ArbitrationStore {
        &self.store
    }

    pub(crate) fn wake_preemption_checks(&self) {
        self.preemption_wake.notify_waiters();
    }

    pub(crate) fn resolution_service(&self) -> &ResolutionService {
        &self.resolution_service
    }

    pub(crate) async fn persisted_retry_delay(
        &self,
        job: &AnalysisJob,
        state: AnalysisState,
    ) -> anyhow::Result<Option<Duration>> {
        if state != AnalysisState::WaitingReanalysis {
            return Ok(None);
        }
        let analysis = self.store.read_analysis(job).await?;
        let Some(retry_at) = analysis.next_retry_at else {
            return Ok(Some(Duration::ZERO));
        };
        Ok(Some(
            retry_at
                .signed_duration_since(self.clock.now())
                .to_std()
                .unwrap_or(Duration::ZERO),
        ))
    }

    /// enabled 路径的 Dispute 上报入口：shadow/auto 把 pending 与原始
    /// Dispute 在同一 per-dispute 临界区内落盘；manual 只保存 Dispute。
    pub async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<ReportDisputeResult> {
        dispute.validate_agent_report()?;
        let _dispute_guard = self.store.lock_dispute(&dispute.id).await?;
        let semantic_guard = self.store.lock_semantic_inputs().await?;
        match self.store.read_dispute(&dispute.id).await {
            Ok(existing) => {
                if !crate::maintainer::same_report_payload(&existing.dispute, dispute) {
                    return Err(conflict(format!(
                        "dispute id={} 已存在但原始字段不同",
                        dispute.id
                    )));
                }
                if self.config.mode == ArbitrationMode::Manual {
                    return Ok(ReportDisputeResult {
                        created: false,
                        automatic_analysis: None,
                        should_enqueue: false,
                    });
                }
                let automatic_analysis = self.store.read_automatic_analysis(&dispute.id).await?;
                let should_enqueue = automatic_analysis
                    .as_ref()
                    .is_some_and(|analysis| analysis.state.is_recoverable());
                return Ok(ReportDisputeResult {
                    created: false,
                    automatic_analysis,
                    should_enqueue,
                });
            }
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(error),
        }
        if self.config.mode == ArbitrationMode::Manual {
            if let Some(existing) = self.store.read_automatic_analysis(&dispute.id).await? {
                let same_report = existing.report_snapshot.as_ref().is_some_and(|snapshot| {
                    crate::maintainer::same_report_payload(snapshot, dispute)
                });
                if !same_report {
                    return Err(conflict(format!(
                        "dispute id={} 的 orphan automatic analysis 与本次原始字段不一致",
                        dispute.id
                    )));
                }
            }
            self.store
                .bump_semantic_inputs_revision(&semantic_guard)
                .await?;
            self.store
                .write_dispute(&MaintainerDisputeRecord::from(dispute.clone()))
                .await?;
            return Ok(ReportDisputeResult {
                created: true,
                automatic_analysis: None,
                should_enqueue: false,
            });
        }
        // pending 先写可将进程崩溃窗口退化为不可见的孤儿记录；重放上报会复用它。
        let automatic_analysis = match self.store.read_automatic_analysis(&dispute.id).await? {
            Some(existing) => {
                let same_report = existing.report_snapshot.as_ref().is_some_and(|snapshot| {
                    crate::maintainer::same_report_payload(snapshot, dispute)
                });
                if !same_report {
                    return Err(conflict(format!(
                        "dispute id={} 的 orphan automatic analysis 与本次原始字段不一致",
                        dispute.id
                    )));
                }
                existing
            }
            None => {
                let mut analysis = self.new_analysis(&dispute.id, AnalysisSource::Automatic);
                analysis.report_snapshot = Some(dispute.clone());
                self.store.create_automatic_analysis(&analysis).await?;
                analysis
            }
        };
        self.store
            .bump_semantic_inputs_revision(&semantic_guard)
            .await?;
        self.store
            .write_dispute(&MaintainerDisputeRecord::from(dispute.clone()))
            .await?;
        Ok(ReportDisputeResult {
            created: true,
            automatic_analysis: Some(automatic_analysis),
            should_enqueue: true,
        })
    }

    /// 每次显式 Analyze 都覆盖当前 manual analysis；此操作不产生治理副作用。
    pub async fn create_manual_analysis(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<ArbitrationAnalysis> {
        let _guard = self.store.lock_dispute(dispute_id).await?;
        let dispute = self.store.read_dispute(dispute_id).await?;
        if dispute.dispute.status != DisputeStatus::Open || dispute.resolution.is_some() {
            return Err(conflict(format!("dispute={dispute_id} 已 resolved")));
        }
        if self
            .store
            .read_manual_analysis(dispute_id)
            .await?
            .is_some_and(|analysis| analysis.state == AnalysisState::Adopting)
        {
            return Err(conflict(format!(
                "dispute={dispute_id} 的 manual analysis 正在提交 resolution"
            )));
        }
        let analysis = self.new_analysis(dispute_id, AnalysisSource::Manual);
        self.store.create_manual_analysis(&analysis).await?;
        self.preemption_wake.notify_waiters();
        Ok(analysis)
    }

    pub async fn recoverable_jobs(&self) -> anyhow::Result<Vec<AnalysisJob>> {
        // auto 模式若恰好在 Approved 落盘后崩溃，启动恢复继续采用同一分析；shadow
        // 与 manual 的 Approved 都是有意的只读终态。
        self.store
            .recoverable_jobs(self.config.mode == ArbitrationMode::Auto)
            .await
    }

    /// 处理或恢复同一条持久化 analysis。终态直接返回，绝不因当前输入变化重建分析。
    pub async fn process_analysis(
        &self,
        job: &AnalysisJob,
        cancel: &CancellationToken,
    ) -> anyhow::Result<ArbitrationAnalysis> {
        loop {
            if cancel.is_cancelled() {
                return self.store.read_analysis(job).await;
            }
            let analysis = self.store.read_analysis(job).await?;
            match analysis.state {
                AnalysisState::WaitingReanalysis => {
                    if analysis
                        .next_retry_at
                        .is_some_and(|retry_at| retry_at > self.clock.now())
                    {
                        return Ok(analysis);
                    }
                    self.start_reanalysis_round(job).await?;
                }
                AnalysisState::Pending | AnalysisState::WaitingContext => {
                    let Some(token) = self.prepare_context(job, cancel).await? else {
                        return self.store.read_analysis(job).await;
                    };
                    if !self.run_proposal(job, &token, cancel).await? {
                        return self.store.read_analysis(job).await;
                    }
                }
                AnalysisState::Proposing => {
                    let Some(token) = self.acquire_phase(job, AnalysisPhase::Proposal).await?
                    else {
                        // 有效 lease 说明这一阶段尚未到 takeover 边界。把恢复态交回
                        // scheduler 做延迟重试，不能在唯一 consumer 中睡到 lease 过期。
                        return self.store.read_analysis(job).await;
                    };
                    if !self.run_proposal(job, &token, cancel).await? {
                        return self.store.read_analysis(job).await;
                    }
                }
                AnalysisState::Verifying => {
                    let Some(token) = self.acquire_phase(job, AnalysisPhase::Verification).await?
                    else {
                        return self.store.read_analysis(job).await;
                    };
                    if !self.run_verification(job, &token, cancel).await? {
                        return self.store.read_analysis(job).await;
                    }
                }
                AnalysisState::Approved
                    if job.source == AnalysisSource::Automatic
                        && analysis.mode == ArbitrationMode::Auto
                        && self.config.mode == ArbitrationMode::Auto
                        && analysis.adoption_blocked_reason.is_none() =>
                {
                    match self.adopt_approved(job, ResolvedBy::Automatic).await {
                        Ok(_) => return self.store.read_analysis(job).await,
                        Err(error) if is_analysis_conflict(&error) => {
                            log::info!(
                                target: "maintainer_arbitration",
                                "automatic analysis={} 未采用: {error:#}",
                                job.analysis_id
                            );
                            return self.store.read_analysis(job).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                AnalysisState::Adopting => {
                    self.resolution_service
                        .recover_analysis_adoption(job, self.clock.now())
                        .await?;
                    return self.store.read_analysis(job).await;
                }
                AnalysisState::Approved
                | AnalysisState::Unresolved
                | AnalysisState::Failed
                | AnalysisState::Adopted => return Ok(analysis),
            }
        }
    }

    /// 人工采用只消费已保存结果；分析输入已变化时要求重新 Analyze。
    pub async fn adopt_analysis(
        &self,
        job: &AnalysisJob,
    ) -> anyhow::Result<ArbitrationResolutionRecord> {
        self.adopt_approved(job, ResolvedBy::Human).await
    }

    async fn adopt_approved(
        &self,
        job: &AnalysisJob,
        resolved_by: ResolvedBy,
    ) -> anyhow::Result<ArbitrationResolutionRecord> {
        for attempt in 0..ADOPTION_SNAPSHOT_MAX_ATTEMPTS {
            // 第一段只在短临界区内确认可采用并记录语义版本。Router 检索随后在锁外
            // 进行；写入面不再被每个 scope 的 HTTP timeout/retry 阻塞。
            let baseline_revision = {
                let dispute_guard = self.store.lock_dispute(&job.dispute_id).await?;
                let semantic_guard = self.store.lock_semantic_inputs().await?;
                if let Some(resolution) = self
                    .resolution_service
                    .resume_matching_analysis_adoption_locked(
                        job,
                        resolved_by,
                        AdoptionGuards {
                            dispute: &dispute_guard,
                            semantic_inputs: &semantic_guard,
                        },
                        self.clock.now(),
                    )
                    .await
                    .map_err(map_adoption_commit_error)?
                {
                    return Ok(resolution);
                }
                let analysis = self.store.read_analysis(job).await?;
                let dispute = self.store.read_dispute(&job.dispute_id).await?;
                ensure_adoption_preconditions(job, &analysis, &dispute)?;
                self.store.read_semantic_inputs_revision().await?
            };

            let current = self
                .context_builder
                .build(&job.dispute_id, self.clock.now())
                .await;

            // 第二段按固定锁序重新取得提交边界。只要期间有 Claim、治理 Policy、
            // Dispute 或 Resolution 写入，revision 就会变化，本轮 Router 快照作废重试。
            let dispute_guard = self.store.lock_dispute(&job.dispute_id).await?;
            let semantic_guard = self.store.lock_semantic_inputs().await?;
            if let Some(resolution) = self
                .resolution_service
                .resume_matching_analysis_adoption_locked(
                    job,
                    resolved_by,
                    AdoptionGuards {
                        dispute: &dispute_guard,
                        semantic_inputs: &semantic_guard,
                    },
                    self.clock.now(),
                )
                .await
                .map_err(map_adoption_commit_error)?
            {
                return Ok(resolution);
            }
            let current_revision = self.store.read_semantic_inputs_revision().await?;
            if current_revision != baseline_revision {
                drop(semantic_guard);
                drop(dispute_guard);
                if attempt + 1 < ADOPTION_SNAPSHOT_MAX_ATTEMPTS {
                    continue;
                }
                return Err(retry("Adopt 最终复核期间团队知识持续变化，请稍后重试"));
            }

            let mut analysis = self.store.read_analysis(job).await?;
            let dispute = self.store.read_dispute(&job.dispute_id).await?;
            ensure_adoption_preconditions(job, &analysis, &dispute)?;
            let current = match current {
                Ok(current) => current,
                Err(error) if is_context_not_ready(&error) => {
                    let reason =
                        "Analysis 完成后 direct Claim 上下文已不完整，请修复 mirror 后重新 Analyze"
                            .to_string();
                    analysis.adoption_blocked_reason = Some(reason.clone());
                    analysis.updated_at = self.clock.now();
                    self.store.write_analysis(&analysis).await?;
                    return Err(conflict(reason));
                }
                Err(error) => return Err(error),
            };
            if analysis.semantic_fingerprint.as_deref()
                != Some(current.semantic_fingerprint.as_str())
            {
                let reason = self
                    .context_builder
                    .describe_changes(analysis.context.as_ref(), &current.frozen)?;
                if job.source == AnalysisSource::Automatic
                    && analysis.mode == ArbitrationMode::Auto
                    && self.config.mode == ArbitrationMode::Auto
                {
                    self.schedule_automatic_reanalysis(&mut analysis, reason)
                        .await?;
                    return Err(conflict("Automatic Analysis 的分析输入已变化"));
                }
                let message = format!("Analysis 的分析输入已变化，请重新 Analyze：{reason}");
                analysis.adoption_blocked_reason = Some(message.clone());
                analysis.context_change_reason = Some(reason);
                analysis.updated_at = self.clock.now();
                self.store.write_analysis(&analysis).await?;
                return Err(conflict(message));
            }
            return self
                .resolution_service
                .begin_analysis_adoption_locked(
                    job,
                    resolved_by,
                    AdoptionGuards {
                        dispute: &dispute_guard,
                        semantic_inputs: &semantic_guard,
                    },
                    self.clock.now(),
                )
                .await
                .map_err(map_adoption_commit_error);
        }
        Err(retry("Adopt 最终复核未能取得稳定团队知识快照"))
    }

    async fn schedule_automatic_reanalysis(
        &self,
        analysis: &mut ArbitrationAnalysis,
        reason: String,
    ) -> anyhow::Result<()> {
        let now = self.clock.now();
        analysis.context_change_count = analysis.context_change_count.saturating_add(1);
        analysis.context_change_reason = Some(reason.clone());
        if let Some(round) = analysis.rounds.last_mut() {
            round.context_change_reason = Some(reason.clone());
        }
        analysis.updated_at = now;
        analysis.lease = None;
        analysis.resolution_id = None;
        analysis.pending_resolution = None;
        analysis.delivery_error = None;
        analysis.error = None;
        match analysis.context_change_count {
            1 => {
                analysis.analysis_round = 2;
                analysis.state = AnalysisState::WaitingReanalysis;
                analysis.next_retry_at =
                    Some(now + chrono::Duration::from_std(FIRST_REANALYSIS_DELAY)?);
                analysis.adoption_blocked_reason = None;
            }
            2 => {
                analysis.analysis_round = 3;
                analysis.state = AnalysisState::WaitingReanalysis;
                analysis.next_retry_at =
                    Some(now + chrono::Duration::from_std(SECOND_REANALYSIS_DELAY)?);
                analysis.adoption_blocked_reason = None;
            }
            _ => {
                analysis.state = AnalysisState::Approved;
                analysis.next_retry_at = None;
                analysis.adoption_blocked_reason =
                    Some("分析输入连续变化，已停止自动处理，等待人工".into());
            }
        }
        self.store.write_analysis(analysis).await
    }

    async fn start_reanalysis_round(&self, job: &AnalysisJob) -> anyhow::Result<()> {
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let mut analysis = self.store.read_analysis(job).await?;
        if analysis.state != AnalysisState::WaitingReanalysis
            || analysis
                .next_retry_at
                .is_some_and(|retry_at| retry_at > self.clock.now())
        {
            return Ok(());
        }
        analysis.semantic_fingerprint = None;
        analysis.context_snapshot_hash = None;
        analysis.context = None;
        analysis.proposal = None;
        analysis.verification = None;
        analysis.lease = None;
        analysis.next_retry_at = None;
        analysis.state = AnalysisState::Pending;
        analysis.context_prepare_attempts = 0;
        analysis.updated_at = self.clock.now();
        self.store.write_analysis(&analysis).await
    }

    async fn prepare_context(
        &self,
        job: &AnalysisJob,
        cancel: &CancellationToken,
    ) -> anyhow::Result<Option<String>> {
        loop {
            let current = self.store.read_analysis(job).await?;
            let used = usize::try_from(current.context_prepare_attempts).unwrap_or(usize::MAX);
            if used >= self.context_prepare_max_attempts {
                self.finish_failed_without_lease(
                    job,
                    "context_build_failed",
                    &anyhow::anyhow!("直接 Claim mirror 在有限重试内仍未准备完整"),
                )
                .await?;
                return Ok(None);
            }
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(None),
                _ = self.wait_for_analysis_preemption(job) => return Ok(None),
                result = self.context_builder.build(&job.dispute_id, self.clock.now()) => result,
            };
            match result {
                Ok(built) => return self.save_prepared_context(job, built).await.map(Some),
                Err(error) if is_context_not_ready(&error) => {
                    let exhausted = self.record_waiting_context(job).await?
                        >= self.context_prepare_max_attempts;
                    if exhausted {
                        self.finish_failed_without_lease(job, "context_build_failed", &error)
                            .await?;
                        return Ok(None);
                    }
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(None),
                        _ = self.wait_for_analysis_preemption(job) => return Ok(None),
                        _ = tokio::time::sleep(self.context_prepare_retry_delay) => {}
                    }
                }
                Err(error) => {
                    self.finish_failed_without_lease(job, "context_build_failed", &error)
                        .await?;
                    return Ok(None);
                }
            }
        }
    }

    async fn record_waiting_context(&self, job: &AnalysisJob) -> anyhow::Result<usize> {
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let mut analysis = self.store.read_analysis(job).await?;
        if !matches!(
            analysis.state,
            AnalysisState::Pending | AnalysisState::WaitingContext
        ) {
            return Ok(usize::try_from(analysis.context_prepare_attempts).unwrap_or(usize::MAX));
        }
        analysis.state = AnalysisState::WaitingContext;
        analysis.context_prepare_attempts = analysis.context_prepare_attempts.saturating_add(1);
        analysis.updated_at = self.clock.now();
        self.store.write_analysis(&analysis).await?;
        Ok(usize::try_from(analysis.context_prepare_attempts).unwrap_or(usize::MAX))
    }

    async fn save_prepared_context(
        &self,
        job: &AnalysisJob,
        built: BuiltArbitrationContext,
    ) -> anyhow::Result<String> {
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let dispute = self.store.read_dispute(&job.dispute_id).await?;
        let mut analysis = self.store.read_analysis(job).await?;
        if dispute.dispute.status != DisputeStatus::Open || dispute.resolution.is_some() {
            analysis.state = AnalysisState::Failed;
            analysis.error = Some(AnalysisError {
                code: "analysis_preempted".into(),
                message: "Dispute 已由其他 resolution 处理".into(),
            });
            analysis.updated_at = self.clock.now();
            self.store.write_analysis(&analysis).await?;
            return Err(conflict("Dispute 已由其他 resolution 处理"));
        }
        if !matches!(
            analysis.state,
            AnalysisState::Pending | AnalysisState::WaitingContext
        ) {
            anyhow::bail!("analysis state={:?} 不能保存冻结上下文", analysis.state);
        }
        let now = self.clock.now();
        let lease = self.new_lease(AnalysisPhase::Proposal, now)?;
        let token = lease.token.clone();
        analysis.semantic_fingerprint = Some(built.semantic_fingerprint);
        analysis.context_snapshot_hash = Some(built.context_snapshot_hash);
        analysis.context = Some(built.frozen);
        analysis.state = AnalysisState::Proposing;
        analysis.lease = Some(lease);
        analysis.updated_at = now;
        analysis.error = None;
        self.store.write_analysis(&analysis).await?;
        Ok(token)
    }

    async fn acquire_phase(
        &self,
        job: &AnalysisJob,
        phase: AnalysisPhase,
    ) -> anyhow::Result<Option<String>> {
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let mut analysis = self.store.read_analysis(job).await?;
        let wanted_state = match phase {
            AnalysisPhase::Proposal => AnalysisState::Proposing,
            AnalysisPhase::Verification => AnalysisState::Verifying,
        };
        if analysis.state != wanted_state {
            return Ok(None);
        }
        let now = self.clock.now();
        if analysis
            .lease
            .as_ref()
            .is_some_and(|lease| lease.expires_at > now)
        {
            return Ok(None);
        }
        let lease = self.new_lease(phase, now)?;
        let token = lease.token.clone();
        analysis.lease = Some(lease);
        analysis.updated_at = now;
        self.store.write_analysis(&analysis).await?;
        Ok(Some(token))
    }

    async fn run_proposal(
        &self,
        job: &AnalysisJob,
        token: &str,
        cancel: &CancellationToken,
    ) -> anyhow::Result<bool> {
        let analysis = self.store.read_analysis(job).await?;
        let context = analysis
            .context
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("analysis 缺少冻结上下文"))?;
        let proposal = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(false),
            _ = self.wait_for_analysis_preemption(job) => return Ok(false),
            result = self.evaluator.propose(context) => result,
        };
        let proposal = match proposal {
            Ok(proposal) => proposal,
            Err(error) => {
                self.finish_failed(job, token, "proposal_failed", &error)
                    .await?;
                return Ok(false);
            }
        };
        if let Err(error) = validate_proposal(context, &proposal) {
            self.finish_failed(job, token, "proposal_invalid", &error)
                .await?;
            return Ok(false);
        }
        let verification_token = self.save_proposal(job, token, proposal).await?;
        self.run_verification(job, &verification_token, cancel)
            .await
    }

    async fn run_verification(
        &self,
        job: &AnalysisJob,
        token: &str,
        cancel: &CancellationToken,
    ) -> anyhow::Result<bool> {
        let analysis = self.store.read_analysis(job).await?;
        let context = analysis
            .context
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("analysis 缺少冻结上下文"))?;
        let proposal = analysis
            .proposal
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("analysis 缺少 proposal"))?;
        let verification = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(false),
            _ = self.wait_for_analysis_preemption(job) => return Ok(false),
            result = self.evaluator.verify(context, proposal) => result,
        };
        let verification = match verification {
            Ok(verification) => verification,
            Err(error) => {
                self.finish_failed(job, token, "verification_failed", &error)
                    .await?;
                return Ok(false);
            }
        };
        if let Err(error) = validate_verification(context, proposal, &verification) {
            self.finish_failed(job, token, "verification_invalid", &error)
                .await?;
            return Ok(false);
        }
        self.save_verification(job, token, verification).await?;
        Ok(true)
    }

    async fn save_proposal(
        &self,
        job: &AnalysisJob,
        token: &str,
        proposal: super::types::ArbitrationProposal,
    ) -> anyhow::Result<String> {
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let now = self.clock.now();
        let mut analysis = self.store.read_analysis(job).await?;
        ensure_lease(&analysis, token, AnalysisPhase::Proposal, now)?;
        let dispute = self.store.read_dispute(&job.dispute_id).await?;
        if dispute.dispute.status != DisputeStatus::Open || dispute.resolution.is_some() {
            analysis.state = AnalysisState::Failed;
            analysis.lease = None;
            analysis.error = Some(AnalysisError {
                code: "analysis_preempted".into(),
                message: "Dispute 已由 Resolution 处理".into(),
            });
            analysis.updated_at = now;
            self.store.write_analysis(&analysis).await?;
            return Err(conflict("Dispute 已由 Resolution 处理"));
        }
        analysis.proposal = Some(proposal);
        analysis.state = AnalysisState::Verifying;
        let mut lease = self.new_lease(AnalysisPhase::Verification, now)?;
        lease.token = token.to_string();
        analysis.lease = Some(lease);
        analysis.updated_at = now;
        self.store.write_analysis(&analysis).await?;
        Ok(token.to_string())
    }

    async fn save_verification(
        &self,
        job: &AnalysisJob,
        token: &str,
        verification: super::types::ArbitrationVerification,
    ) -> anyhow::Result<()> {
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let now = self.clock.now();
        let mut analysis = self.store.read_analysis(job).await?;
        ensure_lease(&analysis, token, AnalysisPhase::Verification, now)?;
        analysis.verification = Some(verification);
        analysis.state = if analysis_is_approved(&analysis, self.config.confidence_threshold) {
            AnalysisState::Approved
        } else {
            AnalysisState::Unresolved
        };
        analysis.lease = None;
        analysis.updated_at = now;
        if analysis.source == AnalysisSource::Automatic {
            if let (
                Some(fingerprint),
                Some(snapshot_hash),
                Some(proposal),
                Some(verification),
                Some(context),
            ) = (
                analysis.semantic_fingerprint.clone(),
                analysis.context_snapshot_hash.clone(),
                analysis.proposal.clone(),
                analysis.verification.clone(),
                analysis.context.as_ref(),
            ) {
                analysis.rounds.push(AutomaticAnalysisRound {
                    round: analysis.analysis_round,
                    started_at: context.generated_at,
                    completed_at: Some(now),
                    semantic_projection_version: analysis.semantic_projection_version,
                    semantic_fingerprint: fingerprint,
                    context_snapshot_hash: snapshot_hash,
                    proposal,
                    verification,
                    context_change_reason: analysis.context_change_reason.clone(),
                });
            }
        }
        self.store.write_analysis(&analysis).await
    }

    /// 覆盖当前 Analysis 或提交 Resolution 时，尽快丢弃正在等待的 provider future，
    /// 让唯一串行 consumer 可以处理下一条 Dispute。
    async fn wait_for_analysis_preemption(&self, job: &AnalysisJob) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(ANALYSIS_PREEMPTION_CHECK_INTERVAL) => {}
                _ = self.preemption_wake.notified() => {}
            }
            let current = self.store.is_current_analysis_job(job).await;
            if !matches!(current, Ok(true)) {
                return;
            }
            let dispute = self.store.read_dispute(&job.dispute_id).await;
            match dispute {
                Ok(record)
                    if record.dispute.status == DisputeStatus::Open
                        && record.resolution.is_none() => {}
                Ok(_) => {
                    self.mark_analysis_preempted(job).await;
                    return;
                }
                Err(_) => return,
            }
        }
    }

    async fn mark_analysis_preempted(&self, job: &AnalysisJob) {
        let Ok(_guard) = self.store.lock_dispute(&job.dispute_id).await else {
            return;
        };
        let Ok(mut analysis) = self.store.read_analysis(job).await else {
            return;
        };
        if !analysis.state.is_recoverable() {
            return;
        }
        analysis.state = AnalysisState::Failed;
        analysis.lease = None;
        analysis.error = Some(AnalysisError {
            code: "analysis_preempted".into(),
            message: "Dispute 已由 Resolution 处理".into(),
        });
        analysis.updated_at = self.clock.now();
        if let Err(error) = self.store.write_analysis(&analysis).await {
            log::warn!(
                target: "maintainer_arbitration",
                "记录 dispute={} analysis={} 被 Resolution 终止失败: {error:#}",
                job.dispute_id,
                job.analysis_id
            );
        }
    }

    async fn finish_failed(
        &self,
        job: &AnalysisJob,
        token: &str,
        code: &str,
        error: &anyhow::Error,
    ) -> anyhow::Result<()> {
        log_analysis_error(job, code, error);
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let now = self.clock.now();
        let mut analysis = self.store.read_analysis(job).await?;
        let phase = analysis
            .lease
            .as_ref()
            .map(|lease| lease.phase)
            .ok_or_else(|| anyhow::anyhow!("analysis lease 缺失"))?;
        ensure_lease(&analysis, token, phase, now)?;
        set_failed(&mut analysis, code, error, now);
        self.store.write_analysis(&analysis).await
    }

    async fn finish_failed_without_lease(
        &self,
        job: &AnalysisJob,
        code: &str,
        error: &anyhow::Error,
    ) -> anyhow::Result<()> {
        log_analysis_error(job, code, error);
        let _guard = self.store.lock_dispute(&job.dispute_id).await?;
        let mut analysis = self.store.read_analysis(job).await?;
        if analysis.state.is_terminal() {
            return Ok(());
        }
        set_failed(&mut analysis, code, error, self.clock.now());
        self.store.write_analysis(&analysis).await
    }

    fn new_analysis(&self, dispute_id: &DisputeId, source: AnalysisSource) -> ArbitrationAnalysis {
        let now = self.clock.now();
        ArbitrationAnalysis {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            analysis_id: ArbitrationAnalysisId::random(),
            dispute_id: dispute_id.clone(),
            source,
            report_snapshot: None,
            created_at: now,
            updated_at: now,
            prompt_version: ARBITRATION_PROMPT_VERSION.into(),
            mode: self.config.mode,
            model: self.model.clone(),
            confidence_threshold: self.config.confidence_threshold,
            semantic_projection_version: CURRENT_SEMANTIC_PROJECTION_VERSION,
            semantic_fingerprint: None,
            context_snapshot_hash: None,
            context: None,
            state: AnalysisState::Pending,
            analysis_round: 1,
            rounds: Vec::new(),
            context_change_count: 0,
            next_retry_at: None,
            context_change_reason: None,
            lease: None,
            proposal: None,
            verification: None,
            resolution_id: None,
            pending_resolution: None,
            error: None,
            delivery_error: None,
            adoption_blocked_reason: None,
            context_prepare_attempts: 0,
        }
    }

    fn new_lease(&self, phase: AnalysisPhase, now: DateTime<Utc>) -> anyhow::Result<AnalysisLease> {
        let delta = chrono::Duration::from_std(self.lease_duration)?;
        Ok(AnalysisLease {
            token: random_lease_token(),
            phase,
            expires_at: now + delta,
            renewed_at: now,
        })
    }
}

fn map_adoption_commit_error(error: anyhow::Error) -> anyhow::Error {
    let message = format!("{error:#}");
    if message.contains("已") || message.contains("不能采用") {
        conflict(message)
    } else {
        error
    }
}

fn ensure_adoption_preconditions(
    job: &AnalysisJob,
    analysis: &ArbitrationAnalysis,
    dispute: &MaintainerDisputeRecord,
) -> anyhow::Result<()> {
    if analysis.state != AnalysisState::Approved {
        return Err(conflict(format!(
            "analysis={} state={:?} 不可采用",
            analysis.analysis_id, analysis.state
        )));
    }
    if let Some(reason) = analysis.adoption_blocked_reason.as_ref() {
        return Err(conflict(reason.clone()));
    }
    if dispute.dispute.status != DisputeStatus::Open || dispute.resolution.is_some() {
        return Err(conflict(format!("dispute={} 已 resolved", job.dispute_id)));
    }
    Ok(())
}

fn ensure_lease(
    analysis: &ArbitrationAnalysis,
    expected_token: &str,
    expected_phase: AnalysisPhase,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let lease = analysis
        .lease
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("analysis lease 缺失"))?;
    if lease.phase != expected_phase || lease.token != expected_token || lease.expires_at <= now {
        anyhow::bail!("analysis lease 已过期或被接管");
    }
    Ok(())
}

fn validate_proposal(
    context: &FrozenArbitrationContext,
    proposal: &super::types::ArbitrationProposal,
) -> anyhow::Result<()> {
    if !proposal.confidence.is_finite() || !(0.0..=1.0).contains(&proposal.confidence) {
        anyhow::bail!("proposal confidence 必须在 [0,1]");
    }
    if proposal.conclusion.trim().is_empty() || proposal.reasoning.trim().is_empty() {
        anyhow::bail!("proposal conclusion/reasoning 不能为空");
    }
    let direct_ids: Vec<ClaimId> = context
        .direct_claims
        .iter()
        .map(|claim| claim.id.clone())
        .collect();
    if proposal.resolution_type == ResolutionType::Unresolved {
        if !proposal.claim_assessments.is_empty() {
            anyhow::bail!("unresolved proposal 不能包含 Claim 修改建议");
        }
    } else {
        if proposal.claim_assessments.is_empty() {
            anyhow::bail!("resolved proposal 必须完整且唯一覆盖全部直接 Claim");
        }
        validate_assessments(&direct_ids, &proposal.claim_assessments)?;
    }
    let visible = visible_evidence_ids(context);
    let evidence_refs: BTreeSet<&str> = proposal.evidence_refs.iter().map(String::as_str).collect();
    if evidence_refs.len() != proposal.evidence_refs.len() {
        anyhow::bail!("proposal evidence_refs 不能包含重复 ID");
    }
    let invalid: Vec<&str> = proposal
        .evidence_refs
        .iter()
        .map(String::as_str)
        .filter(|id| !visible.contains(*id))
        .collect();
    if !invalid.is_empty() {
        anyhow::bail!(
            "proposal evidence_refs 包含输入外 ID: {}",
            invalid.join(", ")
        );
    }
    let missing_direct: Vec<&str> = direct_ids
        .iter()
        .map(ClaimId::as_str)
        .filter(|id| !evidence_refs.contains(*id))
        .collect();
    if !missing_direct.is_empty() {
        anyhow::bail!(
            "proposal evidence_refs 必须覆盖全部直接 Claim，缺少: {}",
            missing_direct.join(", ")
        );
    }
    Ok(())
}

fn validate_verification(
    context: &FrozenArbitrationContext,
    proposal: &super::types::ArbitrationProposal,
    verification: &super::types::ArbitrationVerification,
) -> anyhow::Result<()> {
    if !verification.confidence.is_finite() || !(0.0..=1.0).contains(&verification.confidence) {
        anyhow::bail!("verification confidence 必须在 [0,1]");
    }
    if verification.reasoning.trim().is_empty() {
        anyhow::bail!("verification reasoning 不能为空");
    }
    if verification.verdict == VerificationVerdict::Unresolved {
        if !verification.claim_assessments.is_empty() {
            anyhow::bail!("unresolved verification 不能包含逐 Claim 建议");
        }
    } else {
        if proposal.resolution_type == ResolutionType::Unresolved {
            anyhow::bail!("unresolved proposal 不能被 verification approve");
        }
        let expected: BTreeSet<ClaimId> = context
            .direct_claims
            .iter()
            .map(|claim| claim.id.clone())
            .collect();
        let actual: BTreeSet<ClaimId> = verification
            .claim_assessments
            .iter()
            .map(|assessment| assessment.claim_id.clone())
            .collect();
        if actual.len() != verification.claim_assessments.len() || actual != expected {
            anyhow::bail!("approved verification 必须完整且唯一覆盖全部直接 Claim");
        }
        if verification
            .claim_assessments
            .iter()
            .any(|assessment| assessment.reason.trim().is_empty())
        {
            anyhow::bail!("verification 的每条 Claim assessment reason 不能为空");
        }
    }
    Ok(())
}

fn analysis_is_approved(analysis: &ArbitrationAnalysis, threshold: f64) -> bool {
    let (Some(proposal), Some(verification)) = (&analysis.proposal, &analysis.verification) else {
        return false;
    };
    proposal.resolution_type.is_resolved()
        && proposal.confidence >= threshold
        && verification.confidence >= threshold
        && verification.verdict == VerificationVerdict::Approve
        && verification.resolution_type_agreed
        && verification.resolution_basis_agreed
        && verification.conclusion_agreed
        && verification
            .claim_assessments
            .iter()
            .all(|assessment| assessment.agreed)
}

fn visible_evidence_ids(context: &FrozenArbitrationContext) -> BTreeSet<&str> {
    let mut visible = BTreeSet::new();
    visible.insert(context.dispute.id.as_str());
    for claim in context
        .direct_claims
        .iter()
        .chain(context.source_claims.iter())
    {
        visible.insert(claim.id.as_str());
    }
    for policy in &context.policies {
        visible.insert(policy.id.as_str());
    }
    for candidate in &context.router_candidate_claims {
        visible.insert(candidate.claim.id.as_str());
    }
    for dispute in &context.router_disputes {
        visible.insert(dispute.id.as_str());
    }
    for resolution in &context.prior_resolutions {
        visible.insert(resolution.resolution.resolution_id.as_str());
    }
    visible
}

fn random_lease_token() -> String {
    let mut bytes = [0_u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn set_failed(
    analysis: &mut ArbitrationAnalysis,
    code: &str,
    error: &anyhow::Error,
    now: DateTime<Utc>,
) {
    analysis.state = AnalysisState::Failed;
    analysis.lease = None;
    analysis.updated_at = now;
    analysis.error = Some(AnalysisError {
        code: code.into(),
        message: public_analysis_error(code, error),
    });
}

fn log_analysis_error(job: &AnalysisJob, code: &str, error: &anyhow::Error) {
    log::warn!(
        target: "maintainer_arbitration",
        "analysis 失败: dispute={} analysis={} code={} error={error:#}",
        job.dispute_id,
        job.analysis_id,
        code
    );
}

fn public_analysis_error(code: &str, error: &anyhow::Error) -> String {
    match code {
        "proposal_failed" => return "proposal 模型调用失败；详见 Maintainer 日志".into(),
        "verification_failed" => return "verification 模型调用失败；详见 Maintainer 日志".into(),
        "context_build_failed" => return "仲裁上下文构建失败；详见 Maintainer 日志".into(),
        _ => {}
    }
    format!("{error:#}").chars().take(1_000).collect()
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
        .any(|cause| cause.kind() == std::io::ErrorKind::NotFound)
        || format!("{error:#}").contains("No such file")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::super::resolution::{AdoptionPersistedBarrier, CommitFailpoint};
    use super::*;
    use crate::claim::{
        AgentId, Claim, ClaimAssessment, ClaimStatus, Confidence, ResolutionBasis, ResolutionType,
    };
    use crate::config::LlmChatConfig;
    use crate::maintainer::arbitration::{
        ArbitrationContextBuilder, ArbitrationProposal, ArbitrationVerification,
        ClaimAssessmentVerification, HumanResolutionInput, RejectResolutionInput,
        ResolutionService,
    };
    use crate::maintainer::Maintainer;
    use crate::router::{
        AgentQuery, DisputeRef, RouterClient, RouterQueryResult, ScopesOverviewSnapshot,
    };
    use crate::storage::{paths, write_yaml_atomic};

    #[derive(Default)]
    struct EmptyRouter;

    #[async_trait]
    impl RouterClient for EmptyRouter {
        async fn query(&self, _query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            Ok(RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: Vec::new(),
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            Ok(ScopesOverviewSnapshot::default())
        }
    }

    #[derive(Default)]
    struct CountingRouter {
        query_calls: AtomicUsize,
    }

    impl CountingRouter {
        fn calls(&self) -> usize {
            self.query_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RouterClient for CountingRouter {
        async fn query(&self, _query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            self.query_calls.fetch_add(1, Ordering::SeqCst);
            Ok(RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: Vec::new(),
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            Ok(ScopesOverviewSnapshot::default())
        }
    }

    struct LifecycleRouter {
        related_dispute_id: DisputeId,
        resolved: AtomicBool,
    }

    impl LifecycleRouter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                related_dispute_id: DisputeId::random(),
                resolved: AtomicBool::new(false),
            })
        }

        fn mark_resolved(&self) {
            self.resolved.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl RouterClient for LifecycleRouter {
        async fn query(&self, _query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            Ok(RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: vec![DisputeRef {
                    id: self.related_dispute_id.clone(),
                    name: "related lifecycle evidence".into(),
                    claim_ids: Vec::new(),
                    summary: "related dispute status is visible to arbitration".into(),
                    status: if self.resolved.load(Ordering::SeqCst) {
                        DisputeStatus::Resolved
                    } else {
                        DisputeStatus::Open
                    },
                }],
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            Ok(ScopesOverviewSnapshot::default())
        }
    }

    struct BarrierRouter {
        block_first_query: AtomicBool,
        entered: Notify,
        release: Notify,
    }

    impl BarrierRouter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                block_first_query: AtomicBool::new(true),
                entered: Notify::new(),
                release: Notify::new(),
            })
        }
    }

    #[async_trait]
    impl RouterClient for BarrierRouter {
        async fn query(&self, _query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            if self.block_first_query.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: Vec::new(),
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            Ok(ScopesOverviewSnapshot::default())
        }
    }

    struct ScriptedEvaluator {
        proposal_calls: AtomicUsize,
        verification_calls: AtomicUsize,
        resolution_type: ResolutionType,
        fail_proposal: AtomicBool,
        block_proposal: AtomicBool,
        proposal_entered: Notify,
        proposal_release: Notify,
    }

    impl ScriptedEvaluator {
        fn approved() -> Arc<Self> {
            Arc::new(Self {
                proposal_calls: AtomicUsize::new(0),
                verification_calls: AtomicUsize::new(0),
                resolution_type: ResolutionType::ConflictResolved,
                fail_proposal: AtomicBool::new(false),
                block_proposal: AtomicBool::new(false),
                proposal_entered: Notify::new(),
                proposal_release: Notify::new(),
            })
        }

        fn unresolved() -> Arc<Self> {
            Arc::new(Self {
                proposal_calls: AtomicUsize::new(0),
                verification_calls: AtomicUsize::new(0),
                resolution_type: ResolutionType::Unresolved,
                fail_proposal: AtomicBool::new(false),
                block_proposal: AtomicBool::new(false),
                proposal_entered: Notify::new(),
                proposal_release: Notify::new(),
            })
        }

        fn calls(&self) -> (usize, usize) {
            (
                self.proposal_calls.load(Ordering::SeqCst),
                self.verification_calls.load(Ordering::SeqCst),
            )
        }

        fn block_next_proposal(&self) {
            self.block_proposal.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ArbitrationEvaluator for ScriptedEvaluator {
        async fn propose(
            &self,
            context: &FrozenArbitrationContext,
        ) -> anyhow::Result<ArbitrationProposal> {
            self.proposal_calls.fetch_add(1, Ordering::SeqCst);
            if self.block_proposal.swap(false, Ordering::SeqCst) {
                self.proposal_entered.notify_one();
                self.proposal_release.notified().await;
            }
            if self.fail_proposal.load(Ordering::SeqCst) {
                anyhow::bail!("scripted provider failure");
            }
            let resolved = self.resolution_type.is_resolved();
            Ok(ArbitrationProposal {
                resolution_type: self.resolution_type,
                resolution_basis: if resolved {
                    ResolutionBasis::Evidence
                } else {
                    ResolutionBasis::InsufficientEvidence
                },
                conclusion: if resolved {
                    "采用当前证据支持的知识".into()
                } else {
                    "现有证据不足".into()
                },
                claim_assessments: if resolved {
                    context
                        .direct_claims
                        .iter()
                        .map(|claim| ClaimAssessment {
                            claim_id: claim.id.clone(),
                            recommended_status: claim.status,
                            assessment: "已检查".into(),
                            recommended_scope: None,
                            recommended_statement: None,
                            reason: "直接证据".into(),
                        })
                        .collect()
                } else {
                    Vec::new()
                },
                confidence: if resolved { 0.96 } else { 0.55 },
                evidence_refs: context
                    .direct_claims
                    .iter()
                    .map(|claim| claim.id.to_string())
                    .collect(),
                missing_evidence: if resolved {
                    Vec::new()
                } else {
                    vec!["缺少同环境复现实验".into()]
                },
                human_review_reason: None,
                reasoning: "独立比较直接 Claim".into(),
            })
        }

        async fn verify(
            &self,
            context: &FrozenArbitrationContext,
            proposal: &ArbitrationProposal,
        ) -> anyhow::Result<super::super::types::ArbitrationVerification> {
            self.verification_calls.fetch_add(1, Ordering::SeqCst);
            let approved = proposal.resolution_type.is_resolved();
            Ok(ArbitrationVerification {
                verdict: if approved {
                    VerificationVerdict::Approve
                } else {
                    VerificationVerdict::Unresolved
                },
                resolution_type_agreed: approved,
                resolution_basis_agreed: approved,
                conclusion_agreed: approved,
                claim_assessments: if approved {
                    context
                        .direct_claims
                        .iter()
                        .map(|claim| ClaimAssessmentVerification {
                            claim_id: claim.id.clone(),
                            agreed: true,
                            reason: "复核结果".into(),
                        })
                        .collect()
                } else {
                    Vec::new()
                },
                confidence: if approved { 0.95 } else { 0.50 },
                missing_evidence: proposal.missing_evidence.clone(),
                reasoning: "独立复核".into(),
            })
        }
    }

    struct Fixture {
        _root: tempfile::TempDir,
        service: Arc<ArbitrationService>,
        maintainer: Arc<Maintainer>,
        store: ArbitrationStore,
        evaluator: Arc<ScriptedEvaluator>,
        dispute: Dispute,
        claims: Vec<Claim>,
        clock: Arc<TestClock>,
    }

    struct TestClock(std::sync::Mutex<DateTime<Utc>>);

    impl TestClock {
        fn new(now: DateTime<Utc>) -> Arc<Self> {
            Arc::new(Self(std::sync::Mutex::new(now)))
        }

        fn advance(&self, duration: chrono::Duration) {
            let mut now = self.0.lock().unwrap();
            *now += duration;
        }
    }

    impl ArbitrationClock for TestClock {
        fn now(&self) -> DateTime<Utc> {
            *self.0.lock().unwrap()
        }
    }

    impl Fixture {
        async fn new(mode: ArbitrationMode, evaluator: Arc<ScriptedEvaluator>) -> Self {
            let root = tempfile::tempdir().unwrap();
            let team_root = root.path().to_path_buf();
            let maintainer = Arc::new(Maintainer::new(
                team_root.clone(),
                chrono::Duration::days(30),
                chrono::Duration::days(60),
                8,
            ));
            let store = ArbitrationStore::new(team_root.clone());
            let holder = AgentId::new("agent-a").unwrap();
            let claims = vec![claim(&holder, "current"), claim(&holder, "legacy")];
            let dispute = Dispute {
                id: DisputeId::random(),
                name: "knowledge_conflict".into(),
                reporter_agent_id: holder,
                claims: claims.iter().map(|claim| claim.id.clone()).collect(),
                summary: "同一环境下的知识冲突".into(),
                status: DisputeStatus::Open,
                created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
                resolved_at: None,
            };
            let config = MaintainerArbitrationConfig {
                enabled: true,
                mode,
                ..MaintainerArbitrationConfig::default()
            };
            let llm = LlmChatConfig::default();
            let context_builder = ArbitrationContextBuilder::new(
                store.clone(),
                Arc::new(EmptyRouter),
                config.clone(),
                llm,
            );
            let resolution_service = ResolutionService::new(maintainer.clone(), store.clone());
            let clock = TestClock::new("2026-08-12T00:00:00Z".parse().unwrap());
            let service = Arc::new(ArbitrationService::new(
                store.clone(),
                context_builder,
                evaluator.clone(),
                resolution_service,
                config,
                "test-model".into(),
                Duration::from_secs(2),
                8,
                clock.clone(),
            ));
            Self {
                _root: root,
                service,
                maintainer,
                store,
                evaluator,
                dispute,
                claims,
                clock,
            }
        }

        async fn seed_claims(&self) {
            for claim in &self.claims {
                let path =
                    paths::team_store_agent_claims_dir(self.store.team_root(), &claim.holder)
                        .join(format!("{}.yaml", claim.id));
                write_yaml_atomic(&path, claim).await.unwrap();
            }
        }

        fn rebuilt_service(&self, mode: ArbitrationMode) -> Arc<ArbitrationService> {
            self.rebuilt_service_with_resolution_service(
                mode,
                ResolutionService::new(self.maintainer.clone(), self.store.clone()),
            )
        }

        fn rebuilt_service_with_resolution_service(
            &self,
            mode: ArbitrationMode,
            resolution_service: ResolutionService,
        ) -> Arc<ArbitrationService> {
            self.rebuilt_service_with_router_and_resolution_service(
                mode,
                Arc::new(EmptyRouter),
                resolution_service,
            )
        }

        fn rebuilt_service_with_router_and_resolution_service(
            &self,
            mode: ArbitrationMode,
            router: Arc<dyn RouterClient>,
            resolution_service: ResolutionService,
        ) -> Arc<ArbitrationService> {
            let config = MaintainerArbitrationConfig {
                enabled: true,
                mode,
                ..MaintainerArbitrationConfig::default()
            };
            let context_builder = ArbitrationContextBuilder::new(
                self.store.clone(),
                router,
                config.clone(),
                LlmChatConfig::default(),
            );
            Arc::new(ArbitrationService::new(
                self.store.clone(),
                context_builder,
                self.evaluator.clone(),
                resolution_service,
                config,
                "test-model".into(),
                Duration::from_secs(2),
                8,
                Arc::new(SystemArbitrationClock),
            ))
        }

        async fn report(&self) -> AnalysisJob {
            let result = self.service.report_dispute(&self.dispute).await.unwrap();
            let analysis = result.automatic_analysis.unwrap();
            AnalysisJob {
                dispute_id: self.dispute.id.clone(),
                analysis_id: analysis.analysis_id,
                source: AnalysisSource::Automatic,
            }
        }

        async fn manual_job(&self) -> AnalysisJob {
            let analysis = self
                .service
                .create_manual_analysis(&self.dispute.id)
                .await
                .unwrap();
            AnalysisJob {
                dispute_id: self.dispute.id.clone(),
                analysis_id: analysis.analysis_id,
                source: AnalysisSource::Manual,
            }
        }
    }

    fn claim(holder: &AgentId, name: &str) -> Claim {
        Claim {
            id: crate::claim::ClaimId::random(),
            name: name.into(),
            statement: format!("{name} statement with concrete version and environment"),
            scope: "service / production".into(),
            holder: holder.clone(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "reproducible evidence".into(),
        }
    }

    fn human_input(conclusion: &str) -> HumanResolutionInput {
        HumanResolutionInput {
            conclusion: conclusion.into(),
            notify_affected_agents: false,
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::DirectAnalysis),
            claim_assessments: Vec::new(),
        }
    }

    fn replacement_input(
        expected_resolution_id: crate::claim::ArbitrationResolutionId,
        conclusion: &str,
    ) -> RejectResolutionInput {
        RejectResolutionInput {
            expected_resolution_id,
            rejection_reason: "人工复核发现自动结论不准确".into(),
            conclusion: conclusion.into(),
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::DirectAnalysis),
            claim_assessments: Vec::new(),
        }
    }

    #[tokio::test]
    async fn report_creates_one_automatic_and_replay_deduplicates() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        let first = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        let replay = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(first.created);
        assert!(!replay.created);
        assert!(replay.should_enqueue);
        assert_eq!(
            first.automatic_analysis.unwrap().analysis_id,
            replay.automatic_analysis.unwrap().analysis_id
        );
        assert!(fixture
            .store
            .list_manual_analysis(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn manual_mode_report_is_idempotent_without_automatic_analysis() {
        let fixture = Fixture::new(ArbitrationMode::Manual, ScriptedEvaluator::approved()).await;

        let first = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(first.created);
        assert!(first.automatic_analysis.is_none());
        assert!(!first.should_enqueue);

        let replay = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(!replay.created);
        assert!(replay.automatic_analysis.is_none());
        assert!(!replay.should_enqueue);
        assert!(fixture
            .store
            .read_automatic_analysis(&fixture.dispute.id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            fixture.store.read_semantic_inputs_revision().await.unwrap(),
            1
        );
        assert_eq!(fixture.evaluator.calls(), (0, 0));

        let mut changed = fixture.dispute.clone();
        changed.summary = "same id but changed report payload".into();
        let error = fixture.service.report_dispute(&changed).await.unwrap_err();
        assert!(is_analysis_conflict(&error));
        assert_eq!(fixture.store.list_disputes().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn manual_report_does_not_wake_existing_automatic_but_startup_recovery_keeps_it() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        let automatic_job = fixture.report().await;
        let manual_service = fixture.rebuilt_service(ArbitrationMode::Manual);

        let replay = manual_service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(!replay.created);
        assert!(replay.automatic_analysis.is_none());
        assert!(!replay.should_enqueue);

        let recoverable = manual_service.recoverable_jobs().await.unwrap();
        assert!(recoverable.contains(&automatic_job));
    }

    #[tokio::test]
    async fn manual_report_rejects_conflicting_orphan_automatic_analysis() {
        let fixture = Fixture::new(ArbitrationMode::Manual, ScriptedEvaluator::approved()).await;
        let mut orphan = fixture
            .service
            .new_analysis(&fixture.dispute.id, AnalysisSource::Automatic);
        orphan.report_snapshot = Some(fixture.dispute.clone());
        fixture
            .store
            .create_automatic_analysis(&orphan)
            .await
            .unwrap();

        let mut changed = fixture.dispute.clone();
        changed.summary = "different report payload".into();
        let error = fixture.service.report_dispute(&changed).await.unwrap_err();

        assert!(is_analysis_conflict(&error));
        assert!(fixture.store.list_disputes().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn orphan_automatic_analysis_only_accepts_identical_report_replay() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        let mut orphan = fixture
            .service
            .new_analysis(&fixture.dispute.id, AnalysisSource::Automatic);
        orphan.report_snapshot = Some(fixture.dispute.clone());
        fixture
            .store
            .create_automatic_analysis(&orphan)
            .await
            .unwrap();

        let mut changed = fixture.dispute.clone();
        changed.summary = "same id but changed report payload".into();
        let error = fixture.service.report_dispute(&changed).await.unwrap_err();
        assert!(is_analysis_conflict(&error));
        assert!(fixture.store.list_disputes().await.unwrap().is_empty());

        let recovered = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(recovered.created);
        assert_eq!(
            recovered.automatic_analysis.unwrap().analysis_id,
            orphan.analysis_id
        );
        assert_eq!(fixture.store.list_disputes().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn historical_report_without_analysis_remains_idempotent_and_does_not_enqueue() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture
            .maintainer
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        let replay = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(!replay.created);
        assert!(!replay.should_enqueue);
        assert!(replay.automatic_analysis.is_none());
    }

    #[tokio::test]
    async fn manual_analysis_overwrites_the_previous_result_without_side_effects() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture.report().await;
        let first = fixture.manual_job().await;
        let second = fixture.manual_job().await;
        assert_ne!(first.analysis_id, second.analysis_id);
        assert!(!fixture.store.is_current_analysis_job(&first).await.unwrap());
        assert!(fixture
            .store
            .is_current_analysis_job(&second)
            .await
            .unwrap());
        assert_eq!(
            fixture
                .store
                .list_manual_analysis(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        assert!(
            crate::maintainer::outbox_io::list(fixture.store.team_root())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn overwriting_manual_analysis_stops_the_old_provider_wait() {
        let evaluator = ScriptedEvaluator::approved();
        evaluator.block_next_proposal();
        let fixture = Fixture::new(ArbitrationMode::Manual, evaluator.clone()).await;
        fixture.seed_claims().await;
        let report = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(report.automatic_analysis.is_none());
        let first = fixture.manual_job().await;
        let service = fixture.service.clone();
        let first_for_task = first.clone();
        let task = tokio::spawn(async move {
            service
                .process_analysis(&first_for_task, &CancellationToken::new())
                .await
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            evaluator.proposal_entered.notified(),
        )
        .await
        .unwrap();

        let second = fixture.manual_job().await;
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("被覆盖的 Analysis 不应等待完整 provider timeout")
            .unwrap()
            .unwrap_err();
        assert!(!fixture.store.is_current_analysis_job(&first).await.unwrap());
        assert!(fixture
            .store
            .is_current_analysis_job(&second)
            .await
            .unwrap());
        assert_eq!(evaluator.calls(), (1, 0));
    }

    #[tokio::test]
    async fn overwriting_manual_analysis_stops_a_blocked_context_build() {
        let fixture = Fixture::new(ArbitrationMode::Manual, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        let router = BarrierRouter::new();
        let service = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Manual,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone()),
        );
        let first = service
            .create_manual_analysis(&fixture.dispute.id)
            .await
            .unwrap();
        let first_job = AnalysisJob {
            dispute_id: fixture.dispute.id.clone(),
            analysis_id: first.analysis_id,
            source: AnalysisSource::Manual,
        };
        let first_task = {
            let service = service.clone();
            let job = first_job.clone();
            tokio::spawn(async move {
                service
                    .process_analysis(&job, &CancellationToken::new())
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), router.entered.notified())
            .await
            .expect("旧 Analysis 应进入 Router 上下文构建");

        let second = service
            .create_manual_analysis(&fixture.dispute.id)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), first_task)
            .await
            .expect("被覆盖的 Analysis 不应等待 Router timeout")
            .unwrap()
            .unwrap_err();
        assert!(!fixture
            .store
            .is_current_analysis_job(&first_job)
            .await
            .unwrap());

        let second_job = AnalysisJob {
            dispute_id: fixture.dispute.id.clone(),
            analysis_id: second.analysis_id,
            source: AnalysisSource::Manual,
        };
        let completed = tokio::time::timeout(
            Duration::from_secs(1),
            service.process_analysis(&second_job, &CancellationToken::new()),
        )
        .await
        .expect("新 Analysis 应能立即使用已经释放的 consumer")
        .unwrap();
        assert_eq!(completed.state, AnalysisState::Approved);
        assert_eq!(fixture.evaluator.calls(), (1, 1));
    }

    #[tokio::test]
    async fn human_resolution_stops_a_blocked_context_build() {
        let fixture = Fixture::new(ArbitrationMode::Manual, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        let router = BarrierRouter::new();
        let service = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Manual,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone()),
        );
        let analysis = service
            .create_manual_analysis(&fixture.dispute.id)
            .await
            .unwrap();
        let job = AnalysisJob {
            dispute_id: fixture.dispute.id.clone(),
            analysis_id: analysis.analysis_id,
            source: AnalysisSource::Manual,
        };
        let task = {
            let service = service.clone();
            let job = job.clone();
            tokio::spawn(async move {
                service
                    .process_analysis(&job, &CancellationToken::new())
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), router.entered.notified())
            .await
            .expect("Analysis 应进入 Router 上下文构建");

        service
            .resolution_service()
            .resolve_human(
                &fixture.dispute.id,
                human_input("管理员直接解决冲突"),
                fixture.clock.now(),
            )
            .await
            .unwrap();
        service.wake_preemption_checks();

        let stopped = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("已解决的 Dispute 不应等待 Router timeout")
            .unwrap()
            .unwrap();
        assert_eq!(stopped.state, AnalysisState::Failed);
        assert_eq!(
            stopped.error.as_ref().map(|error| error.code.as_str()),
            Some("analysis_preempted")
        );
        assert_eq!(fixture.evaluator.calls(), (0, 0));
    }

    #[tokio::test]
    async fn unresolved_and_failed_are_terminal_and_never_rerun() {
        let unresolved =
            Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::unresolved()).await;
        unresolved.seed_claims().await;
        let unresolved_job = unresolved.report().await;
        let cancel = CancellationToken::new();
        let unresolved_analysis = unresolved
            .service
            .process_analysis(&unresolved_job, &cancel)
            .await
            .unwrap();
        assert_eq!(unresolved_analysis.state, AnalysisState::Unresolved);
        assert!(
            unresolved_analysis
                .proposal
                .as_ref()
                .unwrap()
                .claim_assessments
                .is_empty(),
            "unresolved proposal 不应建议修改 Claim"
        );
        assert!(
            unresolved_analysis
                .verification
                .as_ref()
                .unwrap()
                .claim_assessments
                .is_empty(),
            "unresolved verification 不应输出逐 Claim 建议"
        );
        let calls = unresolved.evaluator.calls();
        unresolved
            .service
            .process_analysis(&unresolved_job, &cancel)
            .await
            .unwrap();
        assert_eq!(unresolved.evaluator.calls(), calls);

        let evaluator = ScriptedEvaluator::approved();
        evaluator.fail_proposal.store(true, Ordering::SeqCst);
        let failed = Fixture::new(ArbitrationMode::Shadow, evaluator.clone()).await;
        failed.seed_claims().await;
        let failed_job = failed.report().await;
        assert_eq!(
            failed
                .service
                .process_analysis(&failed_job, &cancel)
                .await
                .unwrap()
                .state,
            AnalysisState::Failed
        );
        let calls = evaluator.calls();
        failed
            .service
            .process_analysis(&failed_job, &cancel)
            .await
            .unwrap();
        assert_eq!(evaluator.calls(), calls);
    }

    #[tokio::test]
    async fn unresolved_outputs_reject_claim_change_advice_but_resolved_outputs_keep_it() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::unresolved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        let analysis = fixture.store.read_analysis(&job).await.unwrap();
        let context = analysis.context.as_ref().unwrap();
        let claim = &context.direct_claims[0];

        let mut unresolved_proposal = fixture.evaluator.propose(context).await.unwrap();
        assert!(validate_proposal(context, &unresolved_proposal).is_ok());
        unresolved_proposal.claim_assessments.push(ClaimAssessment {
            claim_id: claim.id.clone(),
            recommended_status: ClaimStatus::Stale,
            assessment: "等待验证".into(),
            recommended_scope: None,
            recommended_statement: None,
            reason: "证据不足".into(),
        });
        assert!(validate_proposal(context, &unresolved_proposal)
            .unwrap_err()
            .to_string()
            .contains("不能包含 Claim 修改建议"));
        unresolved_proposal.claim_assessments.clear();

        let mut unresolved_verification = fixture
            .evaluator
            .verify(context, &unresolved_proposal)
            .await
            .unwrap();
        assert!(
            validate_verification(context, &unresolved_proposal, &unresolved_verification).is_ok()
        );
        unresolved_verification
            .claim_assessments
            .push(ClaimAssessmentVerification {
                claim_id: claim.id.clone(),
                agreed: false,
                reason: "等待人工".into(),
            });
        assert!(
            validate_verification(context, &unresolved_proposal, &unresolved_verification)
                .unwrap_err()
                .to_string()
                .contains("不能包含逐 Claim 建议")
        );

        let resolved_evaluator = ScriptedEvaluator::approved();
        let resolved_proposal = resolved_evaluator.propose(context).await.unwrap();
        let resolved_verification = resolved_evaluator
            .verify(context, &resolved_proposal)
            .await
            .unwrap();
        assert!(validate_proposal(context, &resolved_proposal).is_ok());
        assert!(validate_verification(context, &resolved_proposal, &resolved_verification).is_ok());
    }

    #[tokio::test]
    async fn proposal_evidence_refs_uniquely_cover_every_direct_claim() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        let analysis = fixture.store.read_analysis(&job).await.unwrap();
        let context = analysis.context.as_ref().unwrap();
        let mut proposal = fixture.evaluator.propose(context).await.unwrap();

        proposal.evidence_refs.clear();
        assert!(validate_proposal(context, &proposal)
            .unwrap_err()
            .to_string()
            .contains("必须覆盖全部直接 Claim"));

        proposal.evidence_refs = context
            .direct_claims
            .iter()
            .map(|claim| claim.id.to_string())
            .collect();
        proposal.evidence_refs.push(context.dispute.id.to_string());
        assert!(validate_proposal(context, &proposal).is_ok());

        proposal
            .evidence_refs
            .push(context.direct_claims[0].id.to_string());
        assert!(validate_proposal(context, &proposal)
            .unwrap_err()
            .to_string()
            .contains("不能包含重复 ID"));

        let mut unresolved = ScriptedEvaluator::unresolved()
            .propose(context)
            .await
            .unwrap();
        assert!(validate_proposal(context, &unresolved).is_ok());
        unresolved.evidence_refs.clear();
        assert!(validate_proposal(context, &unresolved)
            .unwrap_err()
            .to_string()
            .contains("必须覆盖全部直接 Claim"));
    }

    #[tokio::test]
    async fn context_waits_in_same_analysis_without_calling_model() {
        let mut fixture =
            Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.service = Arc::new(
            (*fixture.service)
                .clone()
                .with_context_retry(10, Duration::from_millis(20)),
        );
        let job = fixture.report().await;
        let service = fixture.service.clone();
        let running_job = job.clone();
        let task = tokio::spawn(async move {
            service
                .process_analysis(&running_job, &CancellationToken::new())
                .await
        });
        tokio::time::sleep(Duration::from_millis(45)).await;
        assert_eq!(fixture.evaluator.calls(), (0, 0));
        assert_eq!(
            fixture.store.read_analysis(&job).await.unwrap().state,
            AnalysisState::WaitingContext
        );
        fixture.seed_claims().await;
        assert_eq!(task.await.unwrap().unwrap().state, AnalysisState::Approved);
        assert_eq!(fixture.evaluator.calls(), (1, 1));
        assert_eq!(
            fixture
                .store
                .read_automatic_analysis(&fixture.dispute.id)
                .await
                .unwrap()
                .unwrap()
                .analysis_id,
            job.analysis_id
        );
    }

    #[tokio::test]
    async fn scheduler_cancellation_interrupts_context_wait_without_model_call() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        let cancel = CancellationToken::new();
        let (scheduler, handle) = crate::maintainer::arbitration::spawn_arbitration_scheduler(
            fixture.service.clone(),
            1,
            cancel.clone(),
        );
        let job = fixture.report().await;
        let _ = scheduler.enqueue(job.clone()).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if fixture.store.read_analysis(&job).await.unwrap().state
                    == AnalysisState::WaitingContext
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scheduler 应进入 context wait");
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("scheduler cancellation 不应等待完整 context retry delay")
            .unwrap()
            .unwrap();

        assert_eq!(
            fixture.store.read_analysis(&job).await.unwrap().state,
            AnalysisState::WaitingContext
        );
        assert_eq!(fixture.evaluator.calls(), (0, 0));
    }

    #[tokio::test]
    async fn manual_analysis_is_read_only_until_adopted_and_adopt_never_calls_model() {
        let fixture = Fixture::new(ArbitrationMode::Manual, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let report = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(report.automatic_analysis.is_none());
        assert!(!report.should_enqueue);
        let job = fixture.manual_job().await;
        let analyzed = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(analyzed.state, AnalysisState::Approved);
        assert_eq!(
            fixture
                .store
                .read_dispute(&fixture.dispute.id)
                .await
                .unwrap()
                .dispute
                .status,
            DisputeStatus::Open
        );
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        let calls = fixture.evaluator.calls();
        let resolution = fixture.service.adopt_analysis(&job).await.unwrap();
        assert_eq!(resolution.resolution.resolved_by, ResolvedBy::Human);
        assert_eq!(
            resolution.analysis_source_id.as_ref(),
            Some(&job.analysis_id)
        );
        assert_eq!(fixture.evaluator.calls(), calls);
        assert_eq!(
            fixture.store.read_analysis(&job).await.unwrap().state,
            AnalysisState::Adopted
        );
    }

    #[tokio::test]
    async fn manual_mode_allows_human_resolution_without_analysis() {
        let fixture = Fixture::new(ArbitrationMode::Manual, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let report = fixture
            .service
            .report_dispute(&fixture.dispute)
            .await
            .unwrap();
        assert!(report.automatic_analysis.is_none());

        let resolution = ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
            .resolve_human(
                &fixture.dispute.id,
                human_input("human conclusion without analysis"),
                crate::time::now_seconds(),
            )
            .await
            .unwrap();

        assert_eq!(resolution.resolution.resolved_by, ResolvedBy::Human);
        assert!(resolution.analysis_source_id.is_none());
        assert_eq!(
            fixture
                .store
                .read_dispute(&fixture.dispute.id)
                .await
                .unwrap()
                .dispute
                .status,
            DisputeStatus::Resolved
        );
        assert!(fixture
            .store
            .list_manual_analysis(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(fixture.evaluator.calls(), (0, 0));
    }

    #[tokio::test]
    async fn legacy_attempt_source_field_is_read_only_compatible() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture.report().await;
        let job = fixture.manual_job().await;
        fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        let resolution = fixture.service.adopt_analysis(&job).await.unwrap();
        let yaml = serde_yaml_ng::to_string(&resolution).unwrap();
        assert!(yaml.contains("analysis_source_id:"));
        assert!(!yaml.contains("source_attempt_id:"));

        let legacy = yaml.replace(
            &format!("analysis_source_id: {}", job.analysis_id),
            "source_attempt_id: attempt_1234567890abcdef",
        );
        let decoded: ArbitrationResolutionRecord = serde_yaml_ng::from_str(&legacy).unwrap();
        assert!(decoded.analysis_source_id.is_none());
        assert_eq!(
            decoded.legacy_source_attempt_id.as_deref(),
            Some("attempt_1234567890abcdef")
        );
        let reencoded = serde_yaml_ng::to_string(&decoded).unwrap();
        assert!(!reencoded.contains("source_attempt_id:"));
    }

    #[tokio::test]
    async fn adopt_rejects_outdated_analysis_and_deduplicates_concurrent_requests() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture.report().await;
        let outdated_job = fixture.manual_job().await;
        fixture
            .service
            .process_analysis(&outdated_job, &CancellationToken::new())
            .await
            .unwrap();
        let mut changed = fixture.claims[0].clone();
        changed.statement.push_str(" changed");
        let path = paths::team_store_agent_claims_dir(fixture.store.team_root(), &changed.holder)
            .join(format!("{}.yaml", changed.id));
        write_yaml_atomic(&path, &changed).await.unwrap();
        let outdated = fixture
            .service
            .adopt_analysis(&outdated_job)
            .await
            .unwrap_err();
        assert!(is_analysis_conflict(&outdated));
        write_yaml_atomic(&path, &fixture.claims[0]).await.unwrap();

        let fresh_job = fixture.manual_job().await;
        fixture
            .service
            .process_analysis(&fresh_job, &CancellationToken::new())
            .await
            .unwrap();

        let first = fixture.service.clone();
        let second = fixture.service.clone();
        let left_job = fresh_job.clone();
        let right_job = fresh_job;
        let (left, right) = tokio::join!(
            async move { first.adopt_analysis(&left_job).await },
            async move { second.adopt_analysis(&right_job).await }
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left.resolution_id, right.resolution_id);
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn adopt_rejects_router_dispute_lifecycle_change_without_creating_resolution() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture.report().await;
        let job = fixture.manual_job().await;
        let router = LifecycleRouter::new();
        let config = MaintainerArbitrationConfig {
            enabled: true,
            mode: ArbitrationMode::Shadow,
            ..MaintainerArbitrationConfig::default()
        };
        let service = ArbitrationService::new(
            fixture.store.clone(),
            ArbitrationContextBuilder::new(
                fixture.store.clone(),
                router.clone(),
                config.clone(),
                LlmChatConfig::default(),
            ),
            fixture.evaluator.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone()),
            config,
            "test-model".into(),
            Duration::from_secs(2),
            8,
            Arc::new(SystemArbitrationClock),
        );
        let analyzed = service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(analyzed.state, AnalysisState::Approved);

        router.mark_resolved();
        let outdated = service.adopt_analysis(&job).await.unwrap_err();

        assert!(is_analysis_conflict(&outdated));
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            fixture
                .store
                .read_dispute(&fixture.dispute.id)
                .await
                .unwrap()
                .dispute
                .status,
            DisputeStatus::Open
        );
    }

    #[tokio::test]
    async fn startup_recovery_reuses_the_same_manual_analysis() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture.report().await;
        let job = fixture.manual_job().await;
        let recoverable = fixture.service.recoverable_jobs().await.unwrap();
        assert!(recoverable.contains(&job));
        let recovered = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(recovered.analysis_id, job.analysis_id);
        assert_eq!(
            fixture
                .store
                .list_manual_analysis(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn valid_recovery_lease_yields_without_blocking_the_serial_scheduler() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        let job = fixture.report().await;
        let now = fixture.service.clock.now();
        let mut analysis = fixture.store.read_analysis(&job).await.unwrap();
        analysis.state = AnalysisState::Proposing;
        analysis.lease = Some(AnalysisLease {
            token: "still-owned-by-previous-runtime".into(),
            phase: AnalysisPhase::Proposal,
            expires_at: now + chrono::Duration::minutes(10),
            renewed_at: now,
        });
        fixture.store.write_analysis(&analysis).await.unwrap();

        let recovered = tokio::time::timeout(
            Duration::from_millis(100),
            fixture
                .service
                .process_analysis(&job, &CancellationToken::new()),
        )
        .await
        .expect("有效 lease 必须把恢复态立即交回 scheduler")
        .unwrap();

        assert_eq!(recovered.state, AnalysisState::Proposing);
        assert_eq!(recovered.lease, analysis.lease);
        assert_eq!(fixture.evaluator.calls(), (0, 0));
    }

    #[tokio::test]
    async fn adoption_router_query_does_not_hold_the_global_semantic_write_lock() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let approved = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(approved.state, AnalysisState::Approved);

        let router = BarrierRouter::new();
        let config = MaintainerArbitrationConfig {
            enabled: true,
            mode: ArbitrationMode::Shadow,
            ..MaintainerArbitrationConfig::default()
        };
        let adoption_service = Arc::new(ArbitrationService::new(
            fixture.store.clone(),
            ArbitrationContextBuilder::new(
                fixture.store.clone(),
                router.clone(),
                config.clone(),
                LlmChatConfig::default(),
            ),
            fixture.evaluator.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone()),
            config,
            "test-model".into(),
            Duration::from_secs(2),
            8,
            Arc::new(SystemArbitrationClock),
        ));
        let adoption = {
            let adoption_service = adoption_service.clone();
            let job = job.clone();
            tokio::spawn(async move { adoption_service.adopt_analysis(&job).await })
        };
        tokio::time::timeout(Duration::from_secs(1), router.entered.notified())
            .await
            .expect("Adopt 应进入锁外 Router 查询");

        let unrelated = claim(&fixture.dispute.reporter_agent_id, "unrelated");
        tokio::time::timeout(
            Duration::from_secs(1),
            fixture.maintainer.upload_claim(&unrelated),
        )
        .await
        .expect("Router 查询期间团队 Claim 写入不应被全局语义锁阻塞")
        .unwrap();
        router.release.notify_one();

        let resolution = adoption.await.unwrap().unwrap();
        assert_eq!(resolution.resolution.resolved_by, ResolvedBy::Human);
        assert_eq!(
            fixture.store.read_analysis(&job).await.unwrap().state,
            AnalysisState::Adopted
        );
    }

    #[tokio::test]
    async fn shadow_approved_is_never_promoted_when_config_changes_to_auto() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let approved = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(approved.state, AnalysisState::Approved);
        assert_eq!(approved.mode, ArbitrationMode::Shadow);

        let auto_service = fixture.rebuilt_service(ArbitrationMode::Auto);
        assert!(!auto_service
            .recoverable_jobs()
            .await
            .unwrap()
            .contains(&job));
        assert_eq!(
            auto_service
                .process_analysis(&job, &CancellationToken::new())
                .await
                .unwrap()
                .state,
            AnalysisState::Approved
        );
        assert_eq!(
            fixture
                .store
                .read_dispute(&fixture.dispute.id)
                .await
                .unwrap()
                .dispute
                .status,
            DisputeStatus::Open
        );
    }

    #[tokio::test]
    async fn auto_approved_crash_window_recovers_and_adopts_same_analysis() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let token = fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .run_proposal(&job, &token, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            fixture.store.read_analysis(&job).await.unwrap().state,
            AnalysisState::Approved
        );
        assert!(fixture
            .service
            .recoverable_jobs()
            .await
            .unwrap()
            .contains(&job));
        let recovered = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(recovered.analysis_id, job.analysis_id);
        assert_eq!(recovered.state, AnalysisState::Adopted);
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn automatic_context_changes_wait_five_then_fifteen_minutes_and_stop_after_three_rounds()
    {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let token = fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .run_proposal(&job, &token, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(fixture.evaluator.calls(), (1, 1));
        assert_eq!(
            fixture
                .store
                .read_analysis(&job)
                .await
                .unwrap()
                .rounds
                .len(),
            1
        );
        let mut changed = fixture.claims[0].clone();
        changed.statement.push_str(" substantively changed");
        let path = paths::team_store_agent_claims_dir(fixture.store.team_root(), &changed.holder)
            .join(format!("{}.yaml", changed.id));
        write_yaml_atomic(&path, &changed).await.unwrap();

        let blocked = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(blocked.state, AnalysisState::WaitingReanalysis);
        assert_eq!(blocked.analysis_round, 2);
        assert_eq!(blocked.context_change_count, 1);
        assert_eq!(
            blocked.next_retry_at,
            Some(fixture.clock.now() + chrono::Duration::minutes(5))
        );
        assert_eq!(fixture.evaluator.calls(), (1, 1));
        assert!(fixture
            .service
            .recoverable_jobs()
            .await
            .unwrap()
            .contains(&job));
        assert_eq!(
            fixture
                .service
                .persisted_retry_delay(&job, blocked.state)
                .await
                .unwrap(),
            Some(Duration::from_secs(5 * 60))
        );

        fixture.clock.advance(chrono::Duration::minutes(5));
        fixture.service.start_reanalysis_round(&job).await.unwrap();
        let token = fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .run_proposal(&job, &token, &CancellationToken::new())
            .await
            .unwrap();
        changed.statement.push_str(" changed again");
        write_yaml_atomic(&path, &changed).await.unwrap();
        let blocked = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(blocked.state, AnalysisState::WaitingReanalysis);
        assert_eq!(blocked.analysis_round, 3);
        assert_eq!(blocked.context_change_count, 2);
        assert_eq!(
            blocked.next_retry_at,
            Some(fixture.clock.now() + chrono::Duration::minutes(15))
        );
        assert_eq!(fixture.evaluator.calls(), (2, 2));

        fixture.clock.advance(chrono::Duration::minutes(15));
        fixture.service.start_reanalysis_round(&job).await.unwrap();
        let token = fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .run_proposal(&job, &token, &CancellationToken::new())
            .await
            .unwrap();
        changed.statement.push_str(" changed a third time");
        write_yaml_atomic(&path, &changed).await.unwrap();
        let stopped = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(stopped.state, AnalysisState::Approved);
        assert_eq!(stopped.analysis_round, 3);
        assert_eq!(stopped.context_change_count, 3);
        assert_eq!(stopped.next_retry_at, None);
        assert_eq!(
            stopped.adoption_blocked_reason.as_deref(),
            Some("分析输入连续变化，已停止自动处理，等待人工")
        );
        assert_eq!(stopped.rounds.len(), 3);
        assert_eq!(fixture.evaluator.calls(), (3, 3));
        fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(fixture.evaluator.calls(), (3, 3));
        assert_eq!(
            fixture
                .store
                .read_dispute(&fixture.dispute.id)
                .await
                .unwrap()
                .dispute
                .status,
            DisputeStatus::Open
        );
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        assert!(!fixture
            .service
            .recoverable_jobs()
            .await
            .unwrap()
            .contains(&job));
    }

    #[tokio::test]
    async fn missing_direct_context_blocks_automatic_and_manual_adoption() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let token = fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .run_proposal(&job, &token, &CancellationToken::new())
            .await
            .unwrap();
        let missing_claim = &fixture.claims[0];
        let path =
            paths::team_store_agent_claims_dir(fixture.store.team_root(), &missing_claim.holder)
                .join(format!("{}.yaml", missing_claim.id));
        tokio::fs::remove_file(path).await.unwrap();

        let blocked = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(blocked.state, AnalysisState::Approved);
        assert!(blocked
            .adoption_blocked_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("direct Claim 上下文已不完整")));
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());

        let error = fixture.service.adopt_analysis(&job).await.unwrap_err();
        assert!(is_analysis_conflict(&error));
        assert!(format!("{error:#}").contains("重新 Analyze"));
    }

    #[tokio::test]
    async fn adopting_failpoint_recovers_fixed_resolution_without_model_replay() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let config = MaintainerArbitrationConfig {
            enabled: true,
            mode: ArbitrationMode::Auto,
            ..MaintainerArbitrationConfig::default()
        };
        let crashing = Arc::new(ArbitrationService::new(
            fixture.store.clone(),
            ArbitrationContextBuilder::new(
                fixture.store.clone(),
                Arc::new(EmptyRouter),
                config.clone(),
                LlmChatConfig::default(),
            ),
            fixture.evaluator.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
                .with_commit_failpoint(CommitFailpoint::ResolutionWritten),
            config,
            "test-model".into(),
            Duration::from_secs(2),
            8,
            Arc::new(SystemArbitrationClock),
        ));
        let report = crashing.report_dispute(&fixture.dispute).await.unwrap();
        let analysis = report.automatic_analysis.unwrap();
        let job = AnalysisJob {
            dispute_id: fixture.dispute.id.clone(),
            analysis_id: analysis.analysis_id,
            source: AnalysisSource::Automatic,
        };
        assert!(crashing
            .process_analysis(&job, &CancellationToken::new())
            .await
            .is_err());
        let interrupted = fixture.store.read_analysis(&job).await.unwrap();
        assert_eq!(interrupted.state, AnalysisState::Adopting);
        let fixed_resolution_id = interrupted.resolution_id.clone().unwrap();
        let calls = fixture.evaluator.calls();

        let recovered = fixture
            .rebuilt_service(ArbitrationMode::Auto)
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(recovered.state, AnalysisState::Adopted);
        assert_eq!(recovered.resolution_id, Some(fixed_resolution_id));
        assert_eq!(fixture.evaluator.calls(), calls);
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn explicit_adopt_error_retry_recovers_fixed_resolution_idempotently() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let competing_job = fixture.manual_job().await;
        let approved = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(approved.state, AnalysisState::Approved);
        fixture
            .service
            .process_analysis(&competing_job, &CancellationToken::new())
            .await
            .unwrap();
        let router = Arc::new(CountingRouter::default());
        let service = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Shadow,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
                .with_commit_failpoint(CommitFailpoint::IntentSaved),
        );
        let calls = fixture.evaluator.calls();

        assert!(service.adopt_analysis(&job).await.is_err());
        let interrupted = fixture.store.read_analysis(&job).await.unwrap();
        assert_eq!(interrupted.state, AnalysisState::Adopting);
        let fixed_resolution_id = interrupted.resolution_id.unwrap();
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        let router_calls = router.calls();

        let competing = service.adopt_analysis(&competing_job).await.unwrap_err();
        assert!(is_analysis_conflict(&competing));
        assert!(format!("{competing:#}").contains(job.analysis_id.as_str()));
        assert_eq!(router.calls(), router_calls);
        let untouched = fixture.store.read_analysis(&competing_job).await.unwrap();
        assert_eq!(untouched.state, AnalysisState::Approved);
        assert!(untouched.resolution_id.is_none());

        let recovered = service.adopt_analysis(&job).await.unwrap();
        let repeated = service.adopt_analysis(&job).await.unwrap();
        assert_eq!(recovered.resolution_id, fixed_resolution_id);
        assert_eq!(repeated.resolution_id, fixed_resolution_id);
        assert_eq!(fixture.evaluator.calls(), calls);
        assert_eq!(router.calls(), router_calls);
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            fixture.store.read_analysis(&job).await.unwrap().state,
            AnalysisState::Adopted
        );
    }

    #[tokio::test]
    async fn cancelled_explicit_adopt_retry_recovers_fixed_pending_resolution() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let competing_job = fixture.manual_job().await;
        let approved = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(approved.state, AnalysisState::Approved);
        fixture
            .service
            .process_analysis(&competing_job, &CancellationToken::new())
            .await
            .unwrap();
        let barrier = AdoptionPersistedBarrier::new();
        let router = Arc::new(CountingRouter::default());
        let service = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Shadow,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
                .with_adoption_persisted_barrier(barrier.clone()),
        );
        let calls = fixture.evaluator.calls();
        let adoption = {
            let service = service.clone();
            let job = job.clone();
            tokio::spawn(async move { service.adopt_analysis(&job).await })
        };
        tokio::time::timeout(Duration::from_secs(1), barrier.wait_until_entered())
            .await
            .expect("Adopt 应在固定 resolution intent 后进入可取消窗口");
        let interrupted = fixture.store.read_analysis(&job).await.unwrap();
        assert_eq!(interrupted.state, AnalysisState::Adopting);
        let fixed_resolution_id = interrupted.resolution_id.unwrap();

        adoption.abort();
        assert!(adoption.await.unwrap_err().is_cancelled());
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        let router_calls = router.calls();
        let competing = service.adopt_analysis(&competing_job).await.unwrap_err();
        assert!(is_analysis_conflict(&competing));
        assert!(format!("{competing:#}").contains(job.analysis_id.as_str()));
        assert_eq!(router.calls(), router_calls);
        assert_eq!(
            fixture
                .store
                .read_analysis(&competing_job)
                .await
                .unwrap()
                .state,
            AnalysisState::Approved
        );
        let recovered = service.adopt_analysis(&job).await.unwrap();

        assert_eq!(recovered.resolution_id, fixed_resolution_id);
        assert_eq!(fixture.evaluator.calls(), calls);
        assert_eq!(router.calls(), router_calls);
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            fixture.store.read_analysis(&job).await.unwrap().state,
            AnalysisState::Adopted
        );
    }

    #[tokio::test]
    async fn active_human_adoption_preempts_competing_automatic_recovery() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let automatic_job = fixture.report().await;
        let human_job = fixture.manual_job().await;
        let human_approved = fixture
            .service
            .process_analysis(&human_job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(human_approved.state, AnalysisState::Approved);

        let router = Arc::new(CountingRouter::default());
        let automatic_barrier = AdoptionPersistedBarrier::new();
        let automatic_service = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Auto,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
                .with_adoption_persisted_barrier(automatic_barrier.clone()),
        );
        let automatic_task = {
            let service = automatic_service.clone();
            let job = automatic_job.clone();
            tokio::spawn(async move {
                service
                    .process_analysis(&job, &CancellationToken::new())
                    .await
            })
        };
        tokio::time::timeout(
            Duration::from_secs(1),
            automatic_barrier.wait_until_entered(),
        )
        .await
        .expect("automatic adoption 应在固定 intent 后暂停");
        let automatic_pending = fixture.store.read_analysis(&automatic_job).await.unwrap();
        assert_eq!(automatic_pending.state, AnalysisState::Adopting);
        assert_eq!(
            automatic_pending
                .pending_resolution
                .as_ref()
                .unwrap()
                .resolution
                .resolved_by,
            ResolvedBy::Automatic
        );
        automatic_task.abort();
        assert!(automatic_task.await.unwrap_err().is_cancelled());

        let human_barrier = AdoptionPersistedBarrier::new();
        let human_service = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Auto,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
                .with_adoption_persisted_barrier(human_barrier.clone()),
        );
        let human_task = {
            let service = human_service.clone();
            let job = human_job.clone();
            tokio::spawn(async move { service.adopt_analysis(&job).await })
        };
        tokio::time::timeout(Duration::from_secs(1), human_barrier.wait_until_entered())
            .await
            .expect("human adoption 应在固定 intent 后暂停");
        let human_pending = fixture.store.read_analysis(&human_job).await.unwrap();
        assert_eq!(human_pending.state, AnalysisState::Adopting);
        let human_resolution_id = human_pending.resolution_id.clone().unwrap();
        assert_eq!(
            human_pending
                .pending_resolution
                .as_ref()
                .unwrap()
                .resolution
                .resolved_by,
            ResolvedBy::Human
        );
        human_task.abort();
        assert!(human_task.await.unwrap_err().is_cancelled());
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());

        let evaluator_calls = fixture.evaluator.calls();
        let router_calls = router.calls();
        let recovery = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Auto,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone()),
        );
        let automatic_stopped = recovery
            .process_analysis(&automatic_job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(automatic_stopped.state, AnalysisState::Approved);
        assert!(automatic_stopped
            .adoption_blocked_reason
            .as_deref()
            .is_some_and(|reason| reason.contains(human_job.analysis_id.as_str())));
        assert!(fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            fixture
                .store
                .read_dispute(&fixture.dispute.id)
                .await
                .unwrap()
                .dispute
                .status,
            DisputeStatus::Open
        );
        let recoverable = recovery.recoverable_jobs().await.unwrap();
        assert!(!recoverable.contains(&automatic_job));
        assert!(recoverable.contains(&human_job));
        assert_eq!(fixture.evaluator.calls(), evaluator_calls);
        assert_eq!(router.calls(), router_calls);

        let adopted = recovery.adopt_analysis(&human_job).await.unwrap();
        assert_eq!(adopted.resolution_id, human_resolution_id);
        assert_eq!(adopted.resolution.resolved_by, ResolvedBy::Human);
        assert_eq!(fixture.evaluator.calls(), evaluator_calls);
        assert_eq!(router.calls(), router_calls);
        let resolutions = fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap();
        assert_eq!(resolutions.len(), 1);
        assert_eq!(resolutions[0].resolution_id, human_resolution_id);
        assert_eq!(resolutions[0].resolution.resolved_by, ResolvedBy::Human);
        let resolved = fixture
            .store
            .read_dispute(&fixture.dispute.id)
            .await
            .unwrap();
        assert_eq!(resolved.dispute.status, DisputeStatus::Resolved);
        assert_eq!(
            resolved.resolution.unwrap().resolution_id,
            human_resolution_id
        );
    }

    #[tokio::test]
    async fn resolved_human_adoption_terminally_blocks_late_automatic_recovery() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let automatic_job = fixture.report().await;
        let human_job = fixture.manual_job().await;
        fixture
            .service
            .process_analysis(&human_job, &CancellationToken::new())
            .await
            .unwrap();

        let router = Arc::new(CountingRouter::default());
        let automatic_barrier = AdoptionPersistedBarrier::new();
        let automatic_service = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Auto,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
                .with_adoption_persisted_barrier(automatic_barrier.clone()),
        );
        let automatic_task = {
            let service = automatic_service.clone();
            let job = automatic_job.clone();
            tokio::spawn(async move {
                service
                    .process_analysis(&job, &CancellationToken::new())
                    .await
            })
        };
        tokio::time::timeout(
            Duration::from_secs(1),
            automatic_barrier.wait_until_entered(),
        )
        .await
        .expect("automatic adoption 应在固定 intent 后暂停");
        automatic_task.abort();
        assert!(automatic_task.await.unwrap_err().is_cancelled());
        assert_eq!(
            fixture
                .store
                .read_analysis(&automatic_job)
                .await
                .unwrap()
                .state,
            AnalysisState::Adopting
        );

        let recovery = fixture.rebuilt_service_with_router_and_resolution_service(
            ArbitrationMode::Auto,
            router.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone()),
        );
        let human = recovery.adopt_analysis(&human_job).await.unwrap();
        assert_eq!(human.resolution.resolved_by, ResolvedBy::Human);
        let evaluator_calls = fixture.evaluator.calls();
        let router_calls = router.calls();

        let stopped = recovery
            .process_analysis(&automatic_job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(stopped.state, AnalysisState::Approved);
        assert!(stopped
            .adoption_blocked_reason
            .as_deref()
            .is_some_and(|reason| reason.contains(human.resolution_id.as_str())));
        assert!(!recovery
            .recoverable_jobs()
            .await
            .unwrap()
            .contains(&automatic_job));

        let repeated = recovery
            .process_analysis(&automatic_job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(repeated.state, AnalysisState::Approved);
        assert_eq!(
            repeated.adoption_blocked_reason,
            stopped.adoption_blocked_reason
        );
        assert_eq!(fixture.evaluator.calls(), evaluator_calls);
        assert_eq!(router.calls(), router_calls);
        let resolutions = fixture
            .store
            .list_resolution_records(&fixture.dispute.id)
            .await
            .unwrap();
        assert_eq!(resolutions.len(), 1);
        assert_eq!(resolutions[0].resolution_id, human.resolution_id);
        assert_eq!(resolutions[0].resolution.resolved_by, ResolvedBy::Human);
    }

    #[tokio::test]
    async fn delivery_pending_adoption_returns_without_spinning() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let config = MaintainerArbitrationConfig {
            enabled: true,
            mode: ArbitrationMode::Auto,
            ..MaintainerArbitrationConfig::default()
        };
        let service = Arc::new(ArbitrationService::new(
            fixture.store.clone(),
            ArbitrationContextBuilder::new(
                fixture.store.clone(),
                Arc::new(EmptyRouter),
                config.clone(),
                LlmChatConfig::default(),
            ),
            fixture.evaluator.clone(),
            ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
                .with_commit_failpoint(CommitFailpoint::FirstOutboxStored),
            config,
            "test-model".into(),
            Duration::from_secs(2),
            8,
            Arc::new(SystemArbitrationClock),
        ));
        let report = service.report_dispute(&fixture.dispute).await.unwrap();
        let analysis = report.automatic_analysis.unwrap();
        let job = AnalysisJob {
            dispute_id: fixture.dispute.id.clone(),
            analysis_id: analysis.analysis_id,
            source: AnalysisSource::Automatic,
        };

        let pending = tokio::time::timeout(
            Duration::from_secs(1),
            service.process_analysis(&job, &CancellationToken::new()),
        )
        .await
        .expect("投递失败后不能在同一 job 热循环")
        .unwrap();
        assert_eq!(pending.state, AnalysisState::Adopting);
        let fixed_resolution_id = pending.resolution_id.clone();
        let calls = fixture.evaluator.calls();

        let recovered = service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(recovered.state, AnalysisState::Adopted);
        assert_eq!(recovered.resolution_id, fixed_resolution_id);
        assert_eq!(fixture.evaluator.calls(), calls);
    }

    #[tokio::test]
    async fn human_resolution_wins_before_automatic_adoption() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let token = fixture
            .service
            .prepare_context(&job, &CancellationToken::new())
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .run_proposal(&job, &token, &CancellationToken::new())
            .await
            .unwrap();
        let human = ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
            .resolve_human(
                &fixture.dispute.id,
                HumanResolutionInput {
                    conclusion: "human conclusion".into(),
                    notify_affected_agents: false,
                    resolution_type: Some(ResolutionType::ConflictResolved),
                    resolution_basis: Some(ResolutionBasis::DirectAnalysis),
                    claim_assessments: Vec::new(),
                },
                crate::time::now_seconds(),
            )
            .await
            .unwrap();
        let automatic = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(automatic.state, AnalysisState::Approved);
        let current = fixture
            .store
            .read_dispute(&fixture.dispute.id)
            .await
            .unwrap();
        assert_eq!(
            current.resolution.unwrap().resolution_id,
            human.resolution_id
        );
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn human_resolve_orphan_only_replays_identical_input() {
        let same = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        same.report().await;
        let original = human_input("保留人工确认的原始结论");
        let crashing = ResolutionService::new(same.maintainer.clone(), same.store.clone())
            .with_commit_failpoint(CommitFailpoint::ResolutionWritten);
        assert!(crashing
            .resolve_human(
                &same.dispute.id,
                original.clone(),
                crate::time::now_seconds(),
            )
            .await
            .is_err());
        let recovered = ResolutionService::new(same.maintainer.clone(), same.store.clone())
            .resolve_human(&same.dispute.id, original, crate::time::now_seconds())
            .await
            .unwrap();
        assert_eq!(recovered.resolution.conclusion, "保留人工确认的原始结论");
        assert_eq!(
            same.store
                .list_resolution_records(&same.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );

        let changed = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        changed.report().await;
        let crashing = ResolutionService::new(changed.maintainer.clone(), changed.store.clone())
            .with_commit_failpoint(CommitFailpoint::ResolutionWritten);
        assert!(crashing
            .resolve_human(
                &changed.dispute.id,
                human_input("先落盘的固定结论"),
                crate::time::now_seconds(),
            )
            .await
            .is_err());
        let error = ResolutionService::new(changed.maintainer.clone(), changed.store.clone())
            .resolve_human(
                &changed.dispute.id,
                human_input("管理员重试时修改的结论"),
                crate::time::now_seconds(),
            )
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("输入与已持久化 resolution 不一致"));
        let current = changed
            .store
            .read_dispute(&changed.dispute.id)
            .await
            .unwrap();
        assert_eq!(current.resolution.unwrap().conclusion, "先落盘的固定结论");
        assert_eq!(
            changed
                .store
                .list_resolution_records(&changed.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn human_resolution_returns_success_when_delivery_needs_recovery() {
        let fixture = Fixture::new(ArbitrationMode::Shadow, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        fixture.report().await;
        let mut input = human_input("人工结论已提交");
        input.notify_affected_agents = true;
        let resolution = ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
            .with_commit_failpoint(CommitFailpoint::FirstOutboxStored)
            .resolve_human(&fixture.dispute.id, input, crate::time::now_seconds())
            .await
            .unwrap();

        assert_eq!(resolution.resolution.conclusion, "人工结论已提交");
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
        ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
            .recover_pending_delivery(&super::super::types::ResolutionEventTarget {
                dispute_id: fixture.dispute.id.clone(),
                resolution_id: resolution.resolution_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            fixture
                .maintainer
                .list_outbox_entries(None, None)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn reject_replacement_orphan_rejects_changed_retry_and_reuses_fixed_resolution() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let automatic = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(automatic.state, AnalysisState::Adopted);
        let automatic_resolution_id = automatic.resolution_id.unwrap();
        let original = replacement_input(automatic_resolution_id.clone(), "固定的人工替换结论");
        let crashing = ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
            .with_commit_failpoint(CommitFailpoint::ResolutionWritten);
        assert!(crashing
            .reject_and_replace(
                &fixture.dispute.id,
                original.clone(),
                crate::time::now_seconds(),
            )
            .await
            .is_err());

        let service = ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone());
        let changed = replacement_input(automatic_resolution_id, "重试时修改的人工替换结论");
        let error = service
            .reject_and_replace(&fixture.dispute.id, changed, crate::time::now_seconds())
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("输入与已持久化 resolution 不一致"));
        let current = fixture
            .store
            .read_dispute(&fixture.dispute.id)
            .await
            .unwrap();
        assert_eq!(current.resolution.unwrap().conclusion, "固定的人工替换结论");

        let outdated = service
            .reject_and_replace(&fixture.dispute.id, original, crate::time::now_seconds())
            .await
            .unwrap_err();
        assert!(format!("{outdated:#}").contains("不一致"));
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn reject_replacement_returns_success_when_delivery_needs_recovery() {
        let fixture = Fixture::new(ArbitrationMode::Auto, ScriptedEvaluator::approved()).await;
        fixture.seed_claims().await;
        let job = fixture.report().await;
        let automatic = fixture
            .service
            .process_analysis(&job, &CancellationToken::new())
            .await
            .unwrap();
        let automatic_resolution_id = automatic.resolution_id.unwrap();
        let replacement = ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
            .with_commit_failpoint(CommitFailpoint::FirstOutboxStored)
            .reject_and_replace(
                &fixture.dispute.id,
                replacement_input(automatic_resolution_id.clone(), "人工替换已经提交"),
                crate::time::now_seconds(),
            )
            .await
            .unwrap();

        assert_ne!(
            replacement.resolution.resolution_id,
            automatic_resolution_id
        );
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
        ResolutionService::new(fixture.maintainer.clone(), fixture.store.clone())
            .recover_pending_delivery(&super::super::types::ResolutionEventTarget {
                dispute_id: fixture.dispute.id.clone(),
                resolution_id: replacement.resolution_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            fixture
                .store
                .list_resolution_records(&fixture.dispute.id)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
