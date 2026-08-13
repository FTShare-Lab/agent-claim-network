use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::auth::{
    is_router_service_agent, AuthEnvelope, AuthPrincipal, AuthRequest, CreateAuthKeyResponse,
    PublicAuthKeyRecord, TeamAuthStoreError,
};
use crate::claim::{
    AgentId, ArbitrationResolutionId, Claim, ClaimAssessment, ClaimId, ClaimStatus, Dispute,
    DisputeId, InboxAckRequest, InboxId, InboxMessage, OutboxEntry, Policy, PolicyId,
    ResolutionBasis, ResolutionType,
};
use crate::maintainer::arbitration::{
    is_analysis_conflict, is_analysis_retry, AnalysisError, AnalysisJob, AnalysisPhase,
    AnalysisSource, AnalysisState, ArbitrationAnalysis, ArbitrationAnalysisId, ArbitrationProposal,
    ArbitrationResolutionRecord, ArbitrationStore, ArbitrationVerification,
    FrozenArbitrationContext, HumanResolutionInput, MaintainerDisputeRecord, ObservationService,
    ObservationState, RejectResolutionInput, ResolutionObservation, ResolutionService,
};
use crate::maintainer::history::{
    fresh_record_id, AgentActivityKind, AgentActivityRecord, HttpAuditRecord, PolicyEventKind,
    PolicyEventRecord, RouterQueryAuditRecord, SweepRunRecord,
};
use crate::maintainer::{
    ClaimSweepReport, InboxAckError, MaintainerActionRow, MaintainerStatusSnapshot, SendLogRow,
};
use crate::router::{AgentQuery, RouterQueryResult};
use crate::storage::read_yaml;
use crate::time::now_seconds;

use super::state::{AppState, SweepScheduleStatus};

#[derive(Debug, Clone, Deserialize)]
pub struct PullInboxRequest {
    pub agent_id: AgentId,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutboxQuery {
    pub limit: Option<usize>,
    pub open: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreatePolicyRequest {
    pub name: String,
    pub statement: String,
    pub scope: String,
    #[serde(default)]
    pub target_agents: Option<Vec<AgentId>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaimUpdateSuggestionRequest {
    pub statement: String,
    #[serde(default)]
    pub target_agents: Option<Vec<AgentId>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeprecatePolicyRequest {
    pub policy_id: PolicyId,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeprecatePolicyResponse {
    pub pushed: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResolveDisputeRequest {
    pub resolve_note: String,
    #[serde(default)]
    pub notify_affected_agents: bool,
    #[serde(default)]
    pub resolution_type: Option<ResolutionType>,
    #[serde(default)]
    pub resolution_basis: Option<ResolutionBasis>,
    #[serde(default)]
    pub claim_assessments: Vec<ClaimAssessment>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RejectResolutionRequest {
    pub expected_resolution_id: ArbitrationResolutionId,
    pub rejection_reason: String,
    pub conclusion: String,
    #[serde(default)]
    pub resolution_type: Option<ResolutionType>,
    #[serde(default)]
    pub resolution_basis: Option<ResolutionBasis>,
    #[serde(default)]
    pub claim_assessments: Vec<ClaimAssessment>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArbitrationAnalysisSummary {
    pub analysis_id: ArbitrationAnalysisId,
    pub source: AnalysisSource,
    pub state: AnalysisState,
    pub phase: Option<AnalysisPhase>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub semantic_fingerprint: Option<String>,
    pub proposal: Option<ArbitrationProposal>,
    pub resolution_id: Option<ArbitrationResolutionId>,
    pub error: Option<AnalysisError>,
    pub adoptable: bool,
    pub adoption_blocker: Option<String>,
    pub analysis_round: u32,
    pub context_change_count: u32,
    pub next_retry_at: Option<chrono::DateTime<chrono::Utc>>,
    pub context_change_reason: Option<String>,
}

impl ArbitrationAnalysisSummary {
    fn from_analysis(
        analysis: &ArbitrationAnalysis,
        dispute: &MaintainerDisputeRecord,
        adoption_enabled: bool,
    ) -> Self {
        let resolved_proposal = analysis
            .proposal
            .as_ref()
            .is_some_and(|proposal| proposal.resolution_type.is_resolved());
        let adoptable = analysis.state == AnalysisState::Approved
            && resolved_proposal
            && dispute.dispute.status == crate::claim::DisputeStatus::Open
            && dispute.resolution.is_none()
            && analysis.adoption_blocked_reason.is_none()
            && adoption_enabled;
        let adoption_blocker = if dispute.dispute.status != crate::claim::DisputeStatus::Open
            || dispute.resolution.is_some()
        {
            // Resolution 是治理终态；已消费的 Analysis 只保留来源审计，不能再向用户
            // 暗示它仍可采用或已被其他 Resolution 抢占。
            None
        } else if adoptable {
            None
        } else if let Some(reason) = analysis.adoption_blocked_reason.as_ref() {
            Some(reason.clone())
        } else if !adoption_enabled {
            Some("Maintainer 自裁决未启用".to_string())
        } else if analysis.state != AnalysisState::Approved {
            Some("只有 approved Analysis 可以采用".to_string())
        } else if !resolved_proposal {
            Some("unresolved Analysis 不能采用".to_string())
        } else {
            Some("当前 Analysis 不可采用".to_string())
        };
        Self {
            analysis_id: analysis.analysis_id.clone(),
            source: analysis.source,
            state: analysis.state,
            phase: analysis.lease.as_ref().map(|lease| lease.phase),
            created_at: analysis.created_at,
            updated_at: analysis.updated_at,
            semantic_fingerprint: analysis.semantic_fingerprint.clone(),
            proposal: analysis.proposal.clone(),
            resolution_id: analysis.resolution_id.clone(),
            error: analysis.error.clone(),
            adoptable,
            adoption_blocker,
            analysis_round: analysis.analysis_round,
            context_change_count: analysis.context_change_count,
            next_retry_at: analysis.next_retry_at,
            context_change_reason: analysis.context_change_reason.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DisputeListItem {
    #[serde(flatten)]
    pub record: MaintainerDisputeRecord,
}

#[derive(Debug, Clone, Serialize)]
pub struct DisputeDetail {
    #[serde(flatten)]
    pub record: MaintainerDisputeRecord,
    pub automatic_analysis: Option<ArbitrationAnalysisSummary>,
    pub manual_analysis: Option<ArbitrationAnalysisSummary>,
    pub holder_adoption: Option<HolderAdoptionView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArbitrationAnalysisDetail {
    #[serde(flatten)]
    pub summary: ArbitrationAnalysisSummary,
    pub frozen_context: Option<FrozenArbitrationContext>,
    pub verification: Option<ArbitrationVerification>,
    pub warnings: Vec<crate::maintainer::arbitration::ContextWarning>,
    pub validation_result: String,
    pub rounds: Vec<crate::maintainer::arbitration::AutomaticAnalysisRound>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArbitrationAnalysesResponse {
    pub automatic_analysis: Option<ArbitrationAnalysisSummary>,
    pub manual_analysis: Option<ArbitrationAnalysisSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HolderAdoptionSummary {
    pub notified_holders: usize,
    pub delivered: usize,
    pub converged: usize,
    pub diverged: usize,
    pub unobserved: usize,
    pub unknown: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct HolderClaimAdoptionView {
    pub claim_id: ClaimId,
    pub claim_name: String,
    pub recommended_status: ClaimStatus,
    pub current_status: Option<ClaimStatus>,
    pub recommended_scope: Option<String>,
    pub current_scope: Option<String>,
    pub recommended_statement: Option<String>,
    pub current_statement: Option<String>,
    pub policy_provenance_present: bool,
    pub matches: bool,
    pub mismatch_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HolderAdoptionTechnicalView {
    pub policy_id: PolicyId,
    pub inbox_id: InboxId,
    pub snapshot_source: Option<ArbitrationResolutionId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HolderAdoptionItem {
    pub agent_id: AgentId,
    pub delivery_state: String,
    pub observation_state: ObservationState,
    pub assessment_count: usize,
    pub matched_count: usize,
    pub reasons: Vec<String>,
    pub last_delivered_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_observed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub claims: Vec<HolderClaimAdoptionView>,
    pub technical: HolderAdoptionTechnicalView,
}

#[derive(Debug, Clone, Serialize)]
pub struct HolderAdoptionView {
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub summary: HolderAdoptionSummary,
    pub holders: Vec<HolderAdoptionItem>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateTeamAuthKeyRequest {
    pub agent_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamAuthStatusResponse {
    pub maintainer_team_auth_enabled: bool,
    pub router_team_auth_enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaimListQuery {
    pub agent: Option<String>,
    pub status: Option<String>,
    pub scope: Option<String>,
    pub keyword: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DisputeListQuery {
    pub status: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyListQuery {
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentListQuery {
    pub agent: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClaimView {
    pub claim: Claim,
    pub open_dispute_ids: Vec<DisputeId>,
    pub resolved_dispute_ids: Vec<DisputeId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PolicyRecordsResponse {
    pub policies: Vec<Policy>,
    pub outbox: Vec<OutboxEntry>,
    pub send_log: Vec<SendLogRow>,
    pub events: Vec<PolicyEventRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentView {
    pub agent_id: AgentId,
    pub mirror_claims: usize,
    pub active_claims: usize,
    pub stale_claims: usize,
    pub deprecated_claims: usize,
    pub last_source_ip: Option<String>,
    pub last_activity: Option<AgentActivityRecord>,
    pub recent_activities: Vec<AgentActivityRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OverviewResponse {
    pub snapshot: MaintainerStatusSnapshot,
    pub latest_sweep: Option<SweepRunRecord>,
    pub sweep_schedule: SweepScheduleStatus,
    pub recent_policy_events: Vec<PolicyEventRecord>,
    pub recent_agent_activities: Vec<AgentActivityRecord>,
    pub recent_http_audits: Vec<HttpAuditRecord>,
    pub recent_dispute_resolutions: Vec<crate::maintainer::history::DisputeResolutionEventRecord>,
}

pub async fn status_snapshot(
    State(state): State<AppState>,
) -> Result<Json<MaintainerStatusSnapshot>, (StatusCode, String)> {
    state
        .maintainer
        .status_snapshot(now_seconds())
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn actions(
    State(state): State<AppState>,
) -> Result<Json<Vec<MaintainerActionRow>>, (StatusCode, String)> {
    state
        .maintainer
        .list_actions()
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn send_log(
    State(state): State<AppState>,
) -> Result<Json<Vec<SendLogRow>>, (StatusCode, String)> {
    state
        .maintainer
        .list_send_log()
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn outbox(
    State(state): State<AppState>,
    Query(query): Query<OutboxQuery>,
) -> Result<Json<Vec<OutboxEntry>>, (StatusCode, String)> {
    state
        .maintainer
        .list_outbox_entries(query.limit, query.open)
        .await
        .map(Json)
        .map_err(internal_error)
}

pub async fn overview(
    State(state): State<AppState>,
) -> Result<Json<OverviewResponse>, (StatusCode, String)> {
    let snapshot = state
        .maintainer
        .status_snapshot(now_seconds())
        .await
        .map_err(internal_error)?;
    let mut sweeps = state
        .history_store
        .list_sweep_runs()
        .await
        .map_err(internal_error)?;
    sweeps.sort_by(|a, b| b.triggered_at.cmp(&a.triggered_at));
    let mut policy_events = state
        .history_store
        .list_policy_events()
        .await
        .map_err(internal_error)?;
    policy_events.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));
    let mut activities = state
        .history_store
        .list_agent_activity_events()
        .await
        .map_err(internal_error)?;
    activities.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));
    let mut audits = state
        .history_store
        .list_http_audit_logs()
        .await
        .map_err(internal_error)?;
    audits.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));
    let mut dispute_resolutions = state
        .history_store
        .list_dispute_resolution_events()
        .await
        .map_err(internal_error)?;
    dispute_resolutions.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));
    Ok(Json(OverviewResponse {
        snapshot,
        latest_sweep: sweeps.into_iter().next(),
        sweep_schedule: state.sweep_scheduler.status().await,
        recent_policy_events: policy_events.into_iter().take(8).collect(),
        recent_agent_activities: activities.into_iter().take(10).collect(),
        recent_http_audits: audits.into_iter().take(12).collect(),
        recent_dispute_resolutions: dispute_resolutions.into_iter().take(8).collect(),
    }))
}

pub async fn list_disputes(
    State(state): State<AppState>,
    Query(query): Query<DisputeListQuery>,
) -> Result<Json<Vec<DisputeListItem>>, (StatusCode, String)> {
    let store = arbitration_store(&state);
    let mut records = store.list_disputes().await.map_err(internal_error)?;
    records.sort_by(|a, b| b.dispute.created_at.cmp(&a.dispute.created_at));
    let mut result = Vec::new();
    for record in records
        .into_iter()
        .filter(|record| match query.status.as_deref() {
            Some("open") => record.dispute.status == crate::claim::DisputeStatus::Open,
            Some("resolved") => record.dispute.status == crate::claim::DisputeStatus::Resolved,
            _ => true,
        })
    {
        result.push(DisputeListItem { record });
    }
    Ok(Json(result))
}

pub async fn get_dispute(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<DisputeDetail>, (StatusCode, String)> {
    let dispute_id = DisputeId::from_str(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("非法 dispute id: {e}")))?;
    let store = arbitration_store(&state);
    let record = store
        .read_dispute(&dispute_id)
        .await
        .map_err(|error| arbitration_read_error(error, &format!("未找到 dispute: {id}")))?;
    let automatic_analysis = store
        .read_automatic_analysis(&dispute_id)
        .await
        .map_err(internal_error)?;
    let manual_analysis = store
        .read_manual_analysis(&dispute_id)
        .await
        .map_err(internal_error)?;
    let current_resolution = match record.resolution.as_ref() {
        Some(resolution) => Some(
            store
                .read_resolution_record(&dispute_id, &resolution.resolution_id)
                .await
                .map_err(internal_error)?,
        ),
        None => None,
    };
    let holder_adoption = if let Some(resolution_record) = current_resolution
        .as_ref()
        .filter(|record| record.delivery_intent.is_some())
    {
        let observation = ObservationService::new(store.clone(), state.history_store.clone())
            .refresh(resolution_record, now_seconds())
            .await
            .map_err(internal_error)?;
        Some(holder_adoption_view(resolution_record, &observation))
    } else {
        None
    };
    Ok(Json(DisputeDetail {
        automatic_analysis: automatic_analysis.as_ref().map(|analysis| {
            ArbitrationAnalysisSummary::from_analysis(
                analysis,
                &record,
                state.arbitration.is_some(),
            )
        }),
        manual_analysis: manual_analysis.as_ref().map(|analysis| {
            ArbitrationAnalysisSummary::from_analysis(
                analysis,
                &record,
                state.arbitration.is_some(),
            )
        }),
        holder_adoption,
        record,
    }))
}

pub async fn list_dispute_analyses(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ArbitrationAnalysesResponse>, (StatusCode, String)> {
    let dispute_id = parse_dispute_id(&id)?;
    let store = arbitration_store(&state);
    let dispute = store
        .read_dispute(&dispute_id)
        .await
        .map_err(|error| arbitration_read_error(error, "未找到 dispute"))?;
    let automatic_analysis = store
        .read_automatic_analysis(&dispute_id)
        .await
        .map_err(internal_error)?
        .as_ref()
        .map(|analysis| {
            ArbitrationAnalysisSummary::from_analysis(
                analysis,
                &dispute,
                state.arbitration.is_some(),
            )
        });
    let manual_analysis = store
        .read_manual_analysis(&dispute_id)
        .await
        .map_err(internal_error)?
        .as_ref()
        .map(|analysis| {
            ArbitrationAnalysisSummary::from_analysis(
                analysis,
                &dispute,
                state.arbitration.is_some(),
            )
        });
    Ok(Json(ArbitrationAnalysesResponse {
        automatic_analysis,
        manual_analysis,
    }))
}

pub async fn get_dispute_analysis(
    State(state): State<AppState>,
    Path((id, analysis_id)): Path<(String, String)>,
) -> Result<Json<ArbitrationAnalysisDetail>, (StatusCode, String)> {
    let dispute_id = parse_dispute_id(&id)?;
    let analysis_id = ArbitrationAnalysisId::from_str(&analysis_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let store = arbitration_store(&state);
    let dispute = store
        .read_dispute(&dispute_id)
        .await
        .map_err(|error| arbitration_read_error(error, "未找到 dispute"))?;
    let analysis = read_analysis_by_id(&store, &dispute_id, &analysis_id).await?;
    let warnings = analysis
        .context
        .as_ref()
        .map(|context| context.warnings.clone())
        .unwrap_or_default();
    Ok(Json(ArbitrationAnalysisDetail {
        summary: ArbitrationAnalysisSummary::from_analysis(
            &analysis,
            &dispute,
            state.arbitration.is_some(),
        ),
        frozen_context: analysis.context.clone(),
        verification: analysis.verification,
        warnings,
        validation_result: if analysis.error.is_some() {
            "failed".into()
        } else {
            "valid".into()
        },
        rounds: analysis.rounds,
    }))
}

async fn read_analysis_by_id(
    store: &ArbitrationStore,
    dispute_id: &DisputeId,
    analysis_id: &ArbitrationAnalysisId,
) -> Result<ArbitrationAnalysis, (StatusCode, String)> {
    if let Some(automatic) = store
        .read_automatic_analysis(dispute_id)
        .await
        .map_err(internal_error)?
    {
        if automatic.analysis_id == *analysis_id {
            return Ok(automatic);
        }
    }
    let manual = store
        .read_manual_analysis(dispute_id)
        .await
        .map_err(internal_error)?;
    manual
        .filter(|analysis| analysis.analysis_id == *analysis_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "未找到 arbitration analysis".to_string(),
            )
        })
}

fn holder_adoption_view(
    resolution_record: &ArbitrationResolutionRecord,
    observation: &ResolutionObservation,
) -> HolderAdoptionView {
    let Some(intent) = resolution_record.delivery_intent.as_ref() else {
        return HolderAdoptionView {
            observed_at: observation.observed_at,
            summary: HolderAdoptionSummary {
                notified_holders: 0,
                delivered: 0,
                converged: 0,
                diverged: 0,
                unobserved: 0,
                unknown: 0,
            },
            holders: Vec::new(),
        };
    };
    let holders: Vec<HolderAdoptionItem> = observation
        .holders
        .iter()
        .filter_map(|holder| {
            let target = intent
                .targets
                .iter()
                .find(|target| target.target_agent == holder.agent_id)?;
            Some(HolderAdoptionItem {
                agent_id: holder.agent_id.clone(),
                delivery_state: if holder.delivery_observed {
                    "delivered".to_string()
                } else {
                    "not_delivered".to_string()
                },
                observation_state: holder.state,
                assessment_count: holder.assessment_count,
                matched_count: holder.matched_count,
                reasons: holder.reasons.clone(),
                last_delivered_at: holder.delivered_at,
                last_observed_at: holder.last_observed_at.or(Some(observation.observed_at)),
                claims: holder
                    .claims
                    .iter()
                    .map(|claim| HolderClaimAdoptionView {
                        claim_id: claim.claim_id.clone(),
                        claim_name: claim.claim_name.clone(),
                        recommended_status: claim.recommended_status,
                        current_status: claim.current_status,
                        recommended_scope: claim.recommended_scope.clone(),
                        current_scope: claim.current_scope.clone(),
                        recommended_statement: claim.recommended_statement.clone(),
                        current_statement: claim.current_statement.clone(),
                        policy_provenance_present: claim.policy_provenance_present,
                        matches: claim.matched,
                        mismatch_reasons: claim.mismatch_reasons.clone(),
                    })
                    .collect(),
                technical: HolderAdoptionTechnicalView {
                    policy_id: intent.policy.id.clone(),
                    inbox_id: target.inbox_id.clone(),
                    snapshot_source: resolution_record.snapshot_source_resolution_id.clone(),
                },
            })
        })
        .collect();
    HolderAdoptionView {
        observed_at: observation.observed_at,
        summary: HolderAdoptionSummary {
            notified_holders: intent.targets.len(),
            delivered: holders
                .iter()
                .filter(|holder| holder.delivery_state == "delivered")
                .count(),
            converged: holders
                .iter()
                .filter(|holder| holder.observation_state == ObservationState::ObservedConverged)
                .count(),
            diverged: holders
                .iter()
                .filter(|holder| holder.observation_state == ObservationState::ObservedDiverged)
                .count(),
            unobserved: holders
                .iter()
                .filter(|holder| holder.observation_state == ObservationState::DeliveredUnobserved)
                .count(),
            unknown: holders
                .iter()
                .filter(|holder| holder.observation_state == ObservationState::Unknown)
                .count(),
        },
        holders,
    }
}

pub async fn create_manual_analysis(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<ArbitrationAnalysisSummary>), (StatusCode, String)> {
    let dispute_id = parse_dispute_id(&id)?;
    let service = state.arbitration.as_ref().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            "maintainer arbitration is disabled".to_string(),
        )
    })?;
    let scheduler = state.arbitration_scheduler.as_ref().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            "maintainer arbitration scheduler is unavailable".to_string(),
        )
    })?;
    // Manual Analysis 先落盘、后入有界队列。请求若恰在两步之间被取消，Drop
    // 只能同步唤醒持久恢复扫描，不能 await enqueue。
    let mut recovery_wake = AnalysisRecoveryWakeGuard::new(Some(scheduler));
    let analysis = service
        .create_manual_analysis(&dispute_id)
        .await
        .map_err(arbitration_mutation_error)?;
    if let Err(error) = scheduler
        .enqueue(AnalysisJob {
            dispute_id: dispute_id.clone(),
            analysis_id: analysis.analysis_id.clone(),
            source: AnalysisSource::Manual,
        })
        .await
    {
        // Manual Analysis 已经持久化，是这次显式请求的稳定结果。调度器故障不能把
        // 客户端诱导到重试并额外 mint 一条 Analysis；启动恢复会重新提交 pending job。
        log::warn!(
            target: "maintainer_arbitration",
            "manual analysis={} 唤醒失败，等待启动恢复: {error:#}",
            analysis.analysis_id
        );
    }
    recovery_wake.disarm();
    let dispute = arbitration_store(&state)
        .read_dispute(&dispute_id)
        .await
        .map_err(internal_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ArbitrationAnalysisSummary::from_analysis(
            &analysis, &dispute, true,
        )),
    ))
}

pub async fn adopt_analysis(
    State(state): State<AppState>,
    Path((id, analysis_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<ArbitrationResolutionRecord>), (StatusCode, String)> {
    let dispute_id = parse_dispute_id(&id)?;
    let analysis_id = ArbitrationAnalysisId::from_str(&analysis_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let service = state.arbitration.as_ref().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            "maintainer arbitration is disabled".to_string(),
        )
    })?;
    let analysis = read_analysis_by_id(service.store(), &dispute_id, &analysis_id).await?;
    let job = AnalysisJob {
        dispute_id: dispute_id.clone(),
        analysis_id,
        source: analysis.source,
    };
    // Adopt 会先固定 Resolution 与投递意图，再提交 Dispute/outbox。若客户端在这个
    // 窗口断开，两条持久恢复队列都必须在当前进程被唤醒。
    let mut recovery_wake = AdoptionRecoveryWakeGuard::new(
        state.arbitration_scheduler.as_ref(),
        state.resolution_events.as_ref(),
    );
    let resolution_record = service
        .adopt_analysis(&job)
        .await
        .map_err(arbitration_mutation_error)?;
    service.wake_preemption_checks();
    resume_adopting_analysis(&state, service.store(), &job).await;
    recovery_wake.disarm();
    if let Some(events) = state.resolution_events.as_ref() {
        let target = crate::maintainer::arbitration::ResolutionEventTarget {
            dispute_id: dispute_id.clone(),
            resolution_id: resolution_record.resolution_id.clone(),
        };
        let _ = events.enqueue_pending_delivery(target.clone()).await;
        let _ = events.refresh_resolution(target).await;
    }
    Ok((StatusCode::CREATED, Json(resolution_record)))
}

struct AdoptionRecoveryWakeGuard {
    scheduler: Option<crate::maintainer::arbitration::ArbitrationScheduler>,
    resolution_events: Option<crate::maintainer::arbitration::ResolutionEventScheduler>,
}

struct AnalysisRecoveryWakeGuard {
    scheduler: Option<crate::maintainer::arbitration::ArbitrationScheduler>,
}

impl AnalysisRecoveryWakeGuard {
    fn new(scheduler: Option<&crate::maintainer::arbitration::ArbitrationScheduler>) -> Self {
        Self {
            scheduler: scheduler.cloned(),
        }
    }

    fn disarm(&mut self) {
        self.scheduler = None;
    }
}

impl Drop for AnalysisRecoveryWakeGuard {
    fn drop(&mut self) {
        if let Some(scheduler) = &self.scheduler {
            scheduler.wake_durable_recovery();
        }
    }
}

impl AdoptionRecoveryWakeGuard {
    fn new(
        scheduler: Option<&crate::maintainer::arbitration::ArbitrationScheduler>,
        resolution_events: Option<&crate::maintainer::arbitration::ResolutionEventScheduler>,
    ) -> Self {
        Self {
            scheduler: scheduler.cloned(),
            resolution_events: resolution_events.cloned(),
        }
    }

    fn disarm(&mut self) {
        self.scheduler = None;
        self.resolution_events = None;
    }
}

impl Drop for AdoptionRecoveryWakeGuard {
    fn drop(&mut self) {
        if let Some(scheduler) = &self.scheduler {
            scheduler.wake_durable_recovery();
        }
        if let Some(events) = &self.resolution_events {
            events.wake_durable_recovery();
        }
    }
}

async fn resume_adopting_analysis(state: &AppState, store: &ArbitrationStore, job: &AnalysisJob) {
    let analysis = match store.read_analysis(job).await {
        Ok(analysis) => analysis,
        Err(error) => {
            // adopt 已成功返回 durable Resolution；补偿读取失败不能诱导客户端重试。
            log::warn!(
                target: "maintainer_arbitration",
                "读取 dispute={} analysis={} 的 adoption 恢复状态失败，等待启动恢复: {error:#}",
                job.dispute_id,
                job.analysis_id
            );
            return;
        }
    };
    if analysis.state != AnalysisState::Adopting {
        return;
    }
    let Some(scheduler) = state.arbitration_scheduler.as_ref() else {
        log::warn!(
            target: "maintainer_arbitration",
            "dispute={} analysis={} 投递待恢复，但 arbitration scheduler 不可用，等待启动恢复",
            job.dispute_id,
            job.analysis_id
        );
        return;
    };
    if let Err(error) = scheduler.enqueue(job.clone()).await {
        // 与上面的读取一样，这里发生在 durable Resolution 之后，只能降级为恢复告警。
        log::warn!(
            target: "maintainer_arbitration",
            "dispute={} analysis={} adoption 恢复唤醒失败，等待启动恢复: {error:#}",
            job.dispute_id,
            job.analysis_id
        );
    }
}

pub async fn reject_dispute_resolution(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(request): Json<RejectResolutionRequest>,
) -> Result<(StatusCode, Json<ArbitrationResolutionRecord>), (StatusCode, String)> {
    let dispute_id = parse_dispute_id(&id)?;
    let _recovery_wake = ResolutionEventRecoveryWakeGuard::new(state.resolution_events.as_ref());
    let service = ResolutionService::new(state.maintainer.clone(), arbitration_store(&state));
    let resolution_record = service
        .reject_and_replace(
            &dispute_id,
            RejectResolutionInput {
                expected_resolution_id: request.expected_resolution_id,
                rejection_reason: request.rejection_reason,
                conclusion: request.conclusion,
                resolution_type: request.resolution_type,
                resolution_basis: request.resolution_basis,
                claim_assessments: request.claim_assessments,
            },
            now_seconds(),
        )
        .await
        .map_err(arbitration_mutation_error)?;
    if let Some(arbitration) = state.arbitration.as_ref() {
        arbitration.wake_preemption_checks();
    }
    if let Some(events) = state.resolution_events.as_ref() {
        let target = crate::maintainer::arbitration::ResolutionEventTarget {
            dispute_id: dispute_id.clone(),
            resolution_id: resolution_record.resolution_id.clone(),
        };
        let _ = events.enqueue_pending_delivery(target.clone()).await;
        let _ = events.refresh_resolution(target).await;
    }
    Ok((StatusCode::CREATED, Json(resolution_record)))
}

pub async fn list_claims(
    State(state): State<AppState>,
    Query(query): Query<ClaimListQuery>,
) -> Result<Json<Vec<ClaimView>>, (StatusCode, String)> {
    let claims = state
        .maintainer
        .list_all_claims()
        .await
        .map_err(internal_error)?;
    let disputes = state
        .maintainer
        .list_disputes()
        .await
        .map_err(internal_error)?;
    let dispute_map = build_dispute_map(&disputes);
    let status_filter = parse_claim_status(query.status.as_deref())?;

    let views = claims
        .into_iter()
        .map(|(_, claim)| {
            let (open_dispute_ids, resolved_dispute_ids) =
                dispute_map.get(&claim.id).cloned().unwrap_or_default();
            ClaimView {
                claim,
                open_dispute_ids,
                resolved_dispute_ids,
            }
        })
        .filter(|view| match query.agent.as_deref() {
            Some(agent) => view.claim.holder.as_str() == agent,
            None => true,
        })
        .filter(|view| match status_filter {
            Some(status) => view.claim.status == status,
            None => true,
        })
        .filter(|view| contains_ci(&view.claim.scope, query.scope.as_deref()))
        .filter(|view| {
            if let Some(keyword) = query.keyword.as_deref() {
                let combined = format!(
                    "{} {} {} {}",
                    view.claim.name,
                    view.claim.statement,
                    view.claim.scope,
                    view.claim.evidence_summary
                );
                combined.to_lowercase().contains(&keyword.to_lowercase())
            } else {
                true
            }
        })
        .collect();
    Ok(Json(views))
}

pub async fn get_claim(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ClaimView>, (StatusCode, String)> {
    let claim_id = ClaimId::from_str(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("非法 claim id: {e}")))?;
    let claims = state
        .maintainer
        .list_all_claims()
        .await
        .map_err(internal_error)?;
    let disputes = state
        .maintainer
        .list_disputes()
        .await
        .map_err(internal_error)?;
    let dispute_map = build_dispute_map(&disputes);
    let (_, claim) = claims
        .into_iter()
        .find(|(_, claim)| claim.id == claim_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("未找到 claim: {id}")))?;
    let (open_dispute_ids, resolved_dispute_ids) =
        dispute_map.get(&claim.id).cloned().unwrap_or_default();
    Ok(Json(ClaimView {
        claim,
        open_dispute_ids,
        resolved_dispute_ids,
    }))
}

pub async fn list_policies(
    State(state): State<AppState>,
    Query(query): Query<PolicyListQuery>,
) -> Result<Json<PolicyRecordsResponse>, (StatusCode, String)> {
    let mut policies = state
        .maintainer
        .list_policies()
        .await
        .map_err(internal_error)?;
    policies.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let outbox = state
        .maintainer
        .list_outbox_entries(None, None)
        .await
        .map_err(internal_error)?;
    let send_log = state
        .maintainer
        .list_send_log()
        .await
        .map_err(internal_error)?;
    let mut events = state
        .history_store
        .list_policy_events()
        .await
        .map_err(internal_error)?;
    events.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));

    if let Some(kind) = query.kind.as_deref() {
        events.retain(|event| match kind {
            "policy_update" => event.message_type == crate::claim::PolicyMessageType::PolicyUpdate,
            "claim_attribute_update" => {
                event.message_type == crate::claim::PolicyMessageType::ClaimAttributeUpdate
            }
            _ => true,
        });
    }

    Ok(Json(PolicyRecordsResponse {
        policies,
        outbox,
        send_log,
        events,
    }))
}

pub async fn list_agents(
    State(state): State<AppState>,
    Query(query): Query<AgentListQuery>,
) -> Result<Json<Vec<AgentView>>, (StatusCode, String)> {
    let snapshot = state
        .maintainer
        .status_snapshot(now_seconds())
        .await
        .map_err(internal_error)?;
    let mut activities = state
        .history_store
        .list_agent_activity_events()
        .await
        .map_err(internal_error)?;
    activities.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));
    let mut audits = state
        .history_store
        .list_http_audit_logs()
        .await
        .map_err(internal_error)?;
    audits.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));

    let views = snapshot
        .agents
        .into_iter()
        .filter(|agent| match query.agent.as_deref() {
            Some(filter) => agent.agent_id.as_str().contains(filter),
            None => true,
        })
        .map(|agent| {
            let recent_activities: Vec<AgentActivityRecord> = activities
                .iter()
                .filter(|item| item.agent_id == agent.agent_id)
                .take(8)
                .cloned()
                .collect();
            let last_activity = recent_activities.first().cloned();
            let last_source_ip = audits
                .iter()
                .find(|audit| {
                    audit.source_ip.is_some()
                        && ((audit.path == "/inbox/pull"
                            && audit.request_body.contains(agent.agent_id.as_str()))
                            || (audit.path == "/claims/upload"
                                && audit.request_body.contains(agent.agent_id.as_str()))
                            || (audit.path == "/disputes/report"
                                && audit.request_body.contains(agent.agent_id.as_str())))
                })
                .and_then(|audit| audit.source_ip.clone());
            AgentView {
                agent_id: agent.agent_id,
                mirror_claims: agent.mirror_claims,
                active_claims: agent.active_claims,
                stale_claims: agent.stale_claims,
                deprecated_claims: agent.deprecated_claims,
                last_source_ip,
                last_activity,
                recent_activities,
            }
        })
        .collect();
    Ok(Json(views))
}

pub async fn list_sweeps(
    State(state): State<AppState>,
) -> Result<Json<Vec<SweepRunRecord>>, (StatusCode, String)> {
    let mut runs = state
        .history_store
        .list_sweep_runs()
        .await
        .map_err(internal_error)?;
    runs.sort_by(|a, b| b.triggered_at.cmp(&a.triggered_at));
    Ok(Json(runs))
}

pub async fn list_http_audits(
    State(state): State<AppState>,
) -> Result<Json<Vec<HttpAuditRecord>>, (StatusCode, String)> {
    let mut audits = state
        .history_store
        .list_http_audit_logs()
        .await
        .map_err(internal_error)?;
    audits.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));
    Ok(Json(audits))
}

pub async fn get_http_audit(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<HttpAuditRecord>, (StatusCode, String)> {
    let audit = state
        .history_store
        .list_http_audit_logs()
        .await
        .map_err(internal_error)?
        .into_iter()
        .find(|item| item.audit_id == id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("未找到 audit: {id}")))?;
    Ok(Json(audit))
}

pub async fn list_team_auth_keys(
    State(state): State<AppState>,
) -> Result<Json<Vec<PublicAuthKeyRecord>>, (StatusCode, String)> {
    require_team_auth_admin_enabled(&state)?;
    state
        .auth_store
        .list_public()
        .await
        .map(Json)
        .map_err(team_auth_error)
}

pub async fn team_auth_status(State(state): State<AppState>) -> Json<TeamAuthStatusResponse> {
    Json(TeamAuthStatusResponse {
        maintainer_team_auth_enabled: state.maintainer_team_auth_enabled,
        router_team_auth_enabled: state.router_team_auth_enabled,
    })
}

pub async fn create_team_auth_key(
    State(state): State<AppState>,
    Json(req): Json<CreateTeamAuthKeyRequest>,
) -> Result<Json<CreateAuthKeyResponse>, (StatusCode, String)> {
    require_team_auth_admin_enabled(&state)?;
    let created = state
        .auth_store
        .create_key(&req.agent_id)
        .await
        .map_err(team_auth_error)?;
    state
        .auth
        .replace_active_keys_from_store(&state.auth_store, state.maintainer_team_auth_enabled)
        .await
        .map_err(team_auth_error)?;
    Ok(Json(created.response))
}

pub async fn revoke_team_auth_key(
    State(state): State<AppState>,
    Path(key_id): Path<String>,
) -> Result<Json<PublicAuthKeyRecord>, (StatusCode, String)> {
    require_team_auth_admin_enabled(&state)?;
    let revoked = state
        .auth_store
        .revoke_key(&key_id)
        .await
        .map(Json)
        .map_err(team_auth_error)?;
    state
        .auth
        .replace_active_keys_from_store(&state.auth_store, state.maintainer_team_auth_enabled)
        .await
        .map_err(team_auth_error)?;
    Ok(revoked)
}

fn require_team_auth_admin_enabled(state: &AppState) -> Result<(), (StatusCode, String)> {
    if state.admin_auth.is_some() {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        "maintainer admin auth must be enabled to manage team auth keys".to_string(),
    ))
}

pub async fn pull_inbox(
    State(state): State<AppState>,
    Json(req): Json<AuthRequest<PullInboxRequest>>,
) -> Result<Json<Vec<InboxMessage>>, (StatusCode, String)> {
    let AuthRequest { auth, data: req } = req;
    verify_agent_bound_request(&state, &auth, &req.agent_id)?;
    let messages = state
        .maintainer
        .pull_inbox(&req.agent_id)
        .await
        .map_err(internal_error)?;
    log_history_error(
        state
            .history_store
            .write_agent_activity(&AgentActivityRecord {
                event_id: fresh_record_id("agent_activity"),
                agent_id: req.agent_id,
                activity_kind: AgentActivityKind::InboxPulled,
                occurred_at: now_seconds(),
                summary: format!("inbox_pulled offered_messages={}", messages.len()),
            })
            .await,
        "写 inbox pull activity history",
    );
    Ok(Json(messages))
}

pub async fn ack_inbox(
    State(state): State<AppState>,
    Json(req): Json<AuthRequest<InboxAckRequest>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let AuthRequest { auth, data: req } = req;
    verify_agent_bound_request(&state, &auth, &req.agent_id)?;
    state
        .maintainer
        .ack_inbox(&req.agent_id, &req.inbox_ids)
        .await
        .map_err(inbox_ack_error)?;
    if let Some(events) = state.resolution_events.as_ref() {
        events
            .refresh_inboxes(&req.inbox_ids)
            .await
            .map_err(internal_error)?;
    }
    Ok(StatusCode::OK)
}

pub async fn upload_claim(
    State(state): State<AppState>,
    Json(claim): Json<AuthRequest<Claim>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let AuthRequest { auth, data: claim } = claim;
    verify_agent_bound_request(&state, &auth, &claim.holder)?;
    state
        .maintainer
        .upload_claim(&claim)
        .await
        .map_err(internal_error)?;
    if let Some(events) = state.resolution_events.as_ref() {
        events
            .refresh_claim(&claim.id)
            .await
            .map_err(internal_error)?;
    }
    log_history_error(
        state
            .history_store
            .write_agent_activity(&AgentActivityRecord {
                event_id: fresh_record_id("agent_activity"),
                agent_id: claim.holder.clone(),
                activity_kind: AgentActivityKind::ClaimUploaded,
                occurred_at: now_seconds(),
                summary: format!("claim_uploaded {}", claim.id),
            })
            .await,
        "写 claim upload history",
    );
    Ok(StatusCode::OK)
}

pub async fn report_dispute(
    State(state): State<AppState>,
    Json(dispute): Json<AuthRequest<Dispute>>,
) -> Result<StatusCode, (StatusCode, String)> {
    let AuthRequest {
        auth,
        data: dispute,
    } = dispute;
    verify_agent_bound_request(&state, &auth, &dispute.reporter_agent_id)?;
    dispute
        .validate_agent_report()
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    if let Some(service) = state.arbitration.as_ref() {
        // Automatic Analysis 与 Dispute 的 create-once 状态先于 enqueue 持久化。
        // 客户端取消不能让 pending 记录在当前进程中失去唤醒来源。
        let mut recovery_wake =
            AnalysisRecoveryWakeGuard::new(state.arbitration_scheduler.as_ref());
        let result = service
            .report_dispute(&dispute)
            .await
            .map_err(arbitration_mutation_error)?;
        if result.should_enqueue {
            if let (Some(scheduler), Some(analysis)) = (
                state.arbitration_scheduler.as_ref(),
                result.automatic_analysis.as_ref(),
            ) {
                if let Err(error) = scheduler
                    .enqueue(AnalysisJob {
                        dispute_id: dispute.id.clone(),
                        analysis_id: analysis.analysis_id.clone(),
                        source: AnalysisSource::Automatic,
                    })
                    .await
                {
                    // Dispute 与 pending analysis 已经持久化；不能把安全重放窗口伪装成
                    // report 失败。启动恢复会重新提交这条 job。
                    log::warn!(
                        target: "maintainer_arbitration",
                        "dispute={} 自动分析唤醒失败，等待启动恢复: {error:#}",
                        dispute.id
                    );
                }
            }
        }
        recovery_wake.disarm();
    } else {
        state
            .maintainer
            .report_dispute(&dispute)
            .await
            .map_err(internal_error)?;
    }
    log_history_error(
        state
            .history_store
            .write_agent_activity(&AgentActivityRecord {
                event_id: fresh_record_id("agent_activity"),
                agent_id: dispute.reporter_agent_id.clone(),
                activity_kind: AgentActivityKind::DisputeReported,
                occurred_at: dispute.created_at,
                summary: format!(
                    "dispute_reported {} claims={}",
                    dispute.id,
                    dispute.claims.len()
                ),
            })
            .await,
        "写 dispute reported history",
    );
    Ok(StatusCode::OK)
}

pub async fn create_policy(
    State(state): State<AppState>,
    Json(req): Json<CreatePolicyRequest>,
) -> Result<Json<Policy>, (StatusCode, String)> {
    validate_target_agents(state.maintainer.team_root(), req.target_agents.as_ref()).await?;
    let (policy_id, _pushed) = state
        .maintainer
        .publish_new_policy(
            req.name,
            req.statement,
            req.scope,
            now_seconds(),
            req.target_agents,
        )
        .await
        .map_err(internal_error)?;
    let policy = read_policy(&state, &policy_id).await?;
    log_history_error(
        state
            .history_store
            .write_policy_event(&build_policy_event_record(
                &policy,
                PolicyEventKind::PolicyUpdatePublished,
            ))
            .await,
        "写 create policy history",
    );
    Ok(Json(policy))
}

pub async fn claim_update_suggestion(
    State(state): State<AppState>,
    Json(req): Json<ClaimUpdateSuggestionRequest>,
) -> Result<Json<Policy>, (StatusCode, String)> {
    validate_target_agents(state.maintainer.team_root(), req.target_agents.as_ref()).await?;
    let (policy_id, _pushed) = state
        .maintainer
        .claim_update_suggestion(req.statement, now_seconds(), req.target_agents)
        .await
        .map_err(internal_error)?;
    let policy = read_policy(&state, &policy_id).await?;
    log_history_error(
        state
            .history_store
            .write_policy_event(&build_policy_event_record(
                &policy,
                PolicyEventKind::ClaimAttributeUpdatePublished,
            ))
            .await,
        "写 claim update suggestion history",
    );
    Ok(Json(policy))
}

pub async fn deprecate_policy(
    State(state): State<AppState>,
    Json(req): Json<DeprecatePolicyRequest>,
) -> Result<Json<DeprecatePolicyResponse>, (StatusCode, String)> {
    let existing_policy = read_policy(&state, &req.policy_id).await?;
    if existing_policy.status == crate::claim::PolicyStatus::Deprecated {
        return Err((
            StatusCode::CONFLICT,
            format!("policy 已废弃: {}", req.policy_id),
        ));
    }
    let pushed = state
        .maintainer
        .deprecate_policy(&req.policy_id, now_seconds())
        .await
        .map_err(internal_error)?;
    let policy = read_policy(&state, &req.policy_id).await?;
    log_history_error(
        state
            .history_store
            .write_policy_event(&build_policy_event_record(
                &policy,
                PolicyEventKind::PolicyDeprecated,
            ))
            .await,
        "写 deprecate policy history",
    );
    Ok(Json(DeprecatePolicyResponse { pushed }))
}

pub async fn run_sweep(
    State(state): State<AppState>,
) -> Result<Json<ClaimSweepReport>, (StatusCode, String)> {
    let report = state
        .maintainer
        .run_stale_sweep_with_trigger(now_seconds(), "manual")
        .await
        .map_err(internal_error)?;
    Ok(Json(report))
}

pub async fn resolve_dispute(
    State(state): State<AppState>,
    Path(dispute_id): Path<String>,
    Json(req): Json<ResolveDisputeRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let dispute_id = parse_dispute_id(&dispute_id)?;
    let _recovery_wake = ResolutionEventRecoveryWakeGuard::new(state.resolution_events.as_ref());
    let resolve_note = req.resolve_note.trim();
    if resolve_note.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Resolve Note 不能为空".to_string()));
    }
    let occurred_at = now_seconds();
    let resolution_record =
        ResolutionService::new(state.maintainer.clone(), arbitration_store(&state))
            .resolve_human(
                &dispute_id,
                HumanResolutionInput {
                    conclusion: resolve_note.to_string(),
                    notify_affected_agents: req.notify_affected_agents,
                    resolution_type: req.resolution_type,
                    resolution_basis: req.resolution_basis,
                    claim_assessments: req.claim_assessments,
                },
                occurred_at,
            )
            .await
            .map_err(arbitration_mutation_error)?;
    if let Some(arbitration) = state.arbitration.as_ref() {
        arbitration.wake_preemption_checks();
    }
    if let Some(events) = state.resolution_events.as_ref() {
        let target = crate::maintainer::arbitration::ResolutionEventTarget {
            dispute_id: dispute_id.clone(),
            resolution_id: resolution_record.resolution_id.clone(),
        };
        let _ = events.enqueue_pending_delivery(target.clone()).await;
        let _ = events.refresh_resolution(target).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

struct ResolutionEventRecoveryWakeGuard {
    scheduler: Option<crate::maintainer::arbitration::ResolutionEventScheduler>,
}

impl ResolutionEventRecoveryWakeGuard {
    fn new(scheduler: Option<&crate::maintainer::arbitration::ResolutionEventScheduler>) -> Self {
        Self {
            scheduler: scheduler.cloned(),
        }
    }
}

impl Drop for ResolutionEventRecoveryWakeGuard {
    fn drop(&mut self) {
        if let Some(scheduler) = &self.scheduler {
            scheduler.wake_durable_recovery();
        }
    }
}

pub async fn router_query(
    State(state): State<AppState>,
    Json(query): Json<AgentQuery>,
) -> Result<Json<RouterQueryResult>, (StatusCode, String)> {
    let result = state
        .router_client
        .query(&query)
        .await
        .map_err(internal_error)?;
    log_history_error(
        state
            .history_store
            .write_router_query_audit(&RouterQueryAuditRecord {
                query_id: fresh_record_id("router_query"),
                occurred_at: now_seconds(),
                scope: query.scope.clone(),
                semantic_query: query.semantic_query.clone(),
                result: result.clone(),
            })
            .await,
        "写 router query audit",
    );
    Ok(Json(result))
}

async fn read_policy(
    state: &AppState,
    policy_id: &PolicyId,
) -> Result<Policy, (StatusCode, String)> {
    let path = crate::storage::paths::team_store_policies_dir(state.maintainer.team_root())
        .join(format!("{policy_id}.yaml"));
    if !fs::try_exists(&path)
        .await
        .map_err(|err| internal_error(err.into()))?
    {
        return Err((StatusCode::NOT_FOUND, format!("未找到 policy: {policy_id}")));
    }
    read_yaml(&path)
        .await
        .map_err(|err| internal_error(err.into()))
}

async fn validate_target_agents(
    team_root: &std::path::Path,
    target_agents: Option<&Vec<AgentId>>,
) -> Result<(), (StatusCode, String)> {
    let Some(target_agents) = target_agents else {
        return Ok(());
    };
    for agent_id in target_agents {
        let path = crate::storage::paths::team_store_agent_claims_dir(team_root, agent_id);
        let exists = fs::try_exists(&path)
            .await
            .map_err(|err| internal_error(err.into()))?;
        if !exists {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("未知 target agent: {agent_id}"),
            ));
        }
    }
    Ok(())
}

fn build_dispute_map(disputes: &[Dispute]) -> FxHashMap<ClaimId, (Vec<DisputeId>, Vec<DisputeId>)> {
    let mut map: FxHashMap<ClaimId, (Vec<DisputeId>, Vec<DisputeId>)> = FxHashMap::default();
    for dispute in disputes {
        for claim_id in &dispute.claims {
            let entry = map
                .entry(claim_id.clone())
                .or_insert_with(|| (Vec::new(), Vec::new()));
            match dispute.status {
                crate::claim::DisputeStatus::Open => entry.0.push(dispute.id.clone()),
                crate::claim::DisputeStatus::Resolved => entry.1.push(dispute.id.clone()),
            }
        }
    }
    map
}

fn parse_claim_status(raw: Option<&str>) -> Result<Option<ClaimStatus>, (StatusCode, String)> {
    match raw {
        Some("active") => Ok(Some(ClaimStatus::Active)),
        Some("stale") => Ok(Some(ClaimStatus::Stale)),
        Some("deprecated") => Ok(Some(ClaimStatus::Deprecated)),
        Some(other) => Err((
            StatusCode::BAD_REQUEST,
            format!("非法 claim status: {other}"),
        )),
        None => Ok(None),
    }
}

fn contains_ci(text: &str, needle: Option<&str>) -> bool {
    match needle {
        Some(needle) => text.to_lowercase().contains(&needle.to_lowercase()),
        None => true,
    }
}

fn build_policy_event_record(policy: &Policy, event_kind: PolicyEventKind) -> PolicyEventRecord {
    PolicyEventRecord {
        event_id: fresh_record_id("policy_event"),
        policy_id: policy.id.clone(),
        event_kind,
        occurred_at: policy.updated_at.unwrap_or(policy.created_at),
        policy_name: policy.name.clone(),
        policy_scope: policy.scope.clone(),
        policy_status: policy.status,
        message_type: policy.message_type,
        target_agents: policy.target_agents.clone().unwrap_or_default(),
        statement: policy.statement.clone(),
    }
}

fn log_history_error(result: anyhow::Result<()>, action: &str) {
    if let Err(err) = result {
        log::warn!(target: "maintainer_http_server", "{action}失败: {err:#}");
    }
}

fn verify_agent_bound_request(
    state: &AppState,
    auth: &AuthEnvelope,
    request_agent: &AgentId,
) -> Result<(), (StatusCode, String)> {
    if is_router_service_agent(&auth.agent_id) {
        return Err((StatusCode::FORBIDDEN, "forbidden".to_string()));
    }
    match state
        .auth
        .verify_envelope(Some(auth))
        .map_err(|err| err.into_http_response())?
    {
        Some(principal) => require_same_agent(&principal, request_agent)?,
        None => require_same_agent_id(&auth.agent_id, request_agent)?,
    }
    Ok(())
}

fn require_same_agent(
    principal: &AuthPrincipal,
    request_agent: &AgentId,
) -> Result<(), (StatusCode, String)> {
    require_same_agent_id(&principal.agent_id, request_agent)
}

fn require_same_agent_id(
    auth_agent: &AgentId,
    request_agent: &AgentId,
) -> Result<(), (StatusCode, String)> {
    if auth_agent == request_agent {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, "forbidden".to_string()))
    }
}

fn team_auth_error(err: TeamAuthStoreError) -> (StatusCode, String) {
    match err {
        TeamAuthStoreError::ActiveKeyConflict { agent_id, key_id } => (
            StatusCode::CONFLICT,
            format!(
                "Agent '{}' has active key '{}'! Revoke before creating new ones.",
                agent_id.as_str(),
                key_id
            ),
        ),
        TeamAuthStoreError::KeyNotFound { .. } => (StatusCode::NOT_FOUND, "not found".to_string()),
        TeamAuthStoreError::InvalidAgentId(_) => (
            StatusCode::BAD_REQUEST,
            "Agent id 不合法，请修改为 ^[a-z0-9_-]+$ 组合！".to_string(),
        ),
        TeamAuthStoreError::ReservedAgentId { agent_id } => (
            StatusCode::BAD_REQUEST,
            format!(
                "Agent id '{}' 是系统保留身份；请更换其他 agent id尝试。",
                agent_id.as_str()
            ),
        ),
        TeamAuthStoreError::KeyIdCollision => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to generate key id".to_string(),
        ),
        TeamAuthStoreError::Storage(err) => internal_error(err.into()),
        TeamAuthStoreError::Config(err) => internal_error(err.into()),
    }
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:#}"))
}

fn arbitration_store(state: &AppState) -> ArbitrationStore {
    ArbitrationStore::new(state.maintainer.team_root().to_path_buf())
}

fn parse_dispute_id(raw: &str) -> Result<DisputeId, (StatusCode, String)> {
    DisputeId::from_str(raw)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("非法 dispute id: {error}")))
}

fn arbitration_read_error(error: anyhow::Error, not_found: &str) -> (StatusCode, String) {
    if arbitration_error_is_not_found(&error) {
        (StatusCode::NOT_FOUND, not_found.to_string())
    } else {
        internal_error(error)
    }
}

fn arbitration_error_is_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
        || cause
            .downcast_ref::<crate::storage::StorageError>()
            .is_some_and(|storage| {
                matches!(storage, crate::storage::StorageError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound)
            })
    })
}

fn arbitration_mutation_error(error: anyhow::Error) -> (StatusCode, String) {
    let not_found = arbitration_error_is_not_found(&error);
    let conflict = is_analysis_conflict(&error) || is_analysis_retry(&error);
    let message = format!("{error:#}");
    let lowered = message.to_ascii_lowercase();
    let status = if not_found {
        StatusCode::NOT_FOUND
    } else if conflict {
        StatusCode::CONFLICT
    } else if lowered.contains("not found") || message.contains("未找到") {
        StatusCode::NOT_FOUND
    } else if message.contains("已 resolved")
        || message.contains("不一致")
        || message.contains("仅允许")
        || message.contains("已被")
        || message.contains("不再")
        || message.contains("没有可替换")
        || message.contains("分析输入已变化")
        || lowered.contains("approved")
    {
        StatusCode::CONFLICT
    } else if message.contains("不能")
        || message.contains("必须")
        || message.contains("缺少")
        || message.contains("无法为 dispute")
    {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, message)
}

fn inbox_ack_error(err: InboxAckError) -> (StatusCode, String) {
    let status = match err {
        // 客户端用 route-level 404/405 识别 legacy server，领域内未知 ID 不能复用 404。
        InboxAckError::UnknownInbox { .. } => StatusCode::BAD_REQUEST,
        InboxAckError::TargetMismatch { .. } => StatusCode::FORBIDDEN,
        InboxAckError::NotOffered { .. } => StatusCode::CONFLICT,
        InboxAckError::ReadOutbox(_)
        | InboxAckError::LockOutbox(_)
        | InboxAckError::PersistAck { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use chrono::Duration;
    use chrono::Utc;
    use tempfile::TempDir;

    use crate::claim::{
        ArbitrationResolutionContext, DisputeResolution, InboxMessageKind, MaintainerActionId,
        PolicyStatus, ResolvedBy,
    };
    use crate::claim::{Confidence, PolicyMessageType};
    use crate::config::{
        ArbitrationMode, LlmChatConfig, MaintainerAdminAuthConfig, MaintainerArbitrationConfig,
    };
    use crate::maintainer::arbitration::{
        ArbitrationAnalysis, ArbitrationContextBuilder, ArbitrationEvaluator, ArbitrationService,
        DeliveryIntent, SystemArbitrationClock,
    };
    use crate::maintainer::server::auth::AdminAuth;
    use crate::maintainer::Maintainer;
    use crate::router::Router;
    use crate::storage::{paths, write_yaml_atomic};

    fn test_admin_auth() -> Option<AdminAuth> {
        AdminAuth::from_config(&MaintainerAdminAuthConfig {
            enabled: true,
            username: "admin".to_string(),
            password_env: "TEST_ADMIN_PASSWORD".to_string(),
            password: Some("secret".to_string()),
        })
        .unwrap()
    }

    fn build_state() -> (AppState, TempDir, TempDir) {
        let team = tempfile::tempdir().unwrap();
        let homes = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(Maintainer::new(
            team.path().to_path_buf(),
            Duration::days(7),
            Duration::days(30),
            4,
        ));
        let router_client = Arc::new(Router::new(team.path().to_path_buf()));
        (
            AppState {
                history_store: maintainer.history_store().clone(),
                maintainer,
                arbitration: None,
                arbitration_scheduler: None,
                resolution_events: None,
                router_client,
                auth: crate::auth::AuthVerifier::disabled(),
                auth_store: crate::auth::TeamAuthStore::new(paths::team_store_auth_keys_path(
                    team.path(),
                )),
                maintainer_team_auth_enabled: true,
                router_team_auth_enabled: false,
                frontend_dist_dir: std::path::PathBuf::from("./frontend/maintainer-workbench/dist"),
                sweep_scheduler: crate::maintainer::server::SweepScheduler::new(86_400),
                admin_auth: test_admin_auth(),
            },
            team,
            homes,
        )
    }

    struct UnusedEvaluator;

    #[async_trait::async_trait]
    impl ArbitrationEvaluator for UnusedEvaluator {
        async fn propose(
            &self,
            _context: &FrozenArbitrationContext,
        ) -> anyhow::Result<ArbitrationProposal> {
            anyhow::bail!("test evaluator must not be called")
        }

        async fn verify(
            &self,
            _context: &FrozenArbitrationContext,
            _proposal: &ArbitrationProposal,
        ) -> anyhow::Result<ArbitrationVerification> {
            anyhow::bail!("test evaluator must not be called")
        }
    }

    fn test_arbitration_service_with_mode(
        state: &AppState,
        mode: ArbitrationMode,
    ) -> Arc<ArbitrationService> {
        let store = ArbitrationStore::new(state.maintainer.team_root().to_path_buf());
        let config = MaintainerArbitrationConfig {
            enabled: true,
            mode,
            ..MaintainerArbitrationConfig::default()
        };
        let service = ArbitrationService::new(
            store.clone(),
            ArbitrationContextBuilder::new(
                store.clone(),
                state.router_client.clone(),
                config.clone(),
                LlmChatConfig::default(),
            ),
            Arc::new(UnusedEvaluator),
            ResolutionService::new(state.maintainer.clone(), store),
            config,
            "test-model".to_string(),
            std::time::Duration::from_secs(1),
            4,
            Arc::new(SystemArbitrationClock),
        );
        Arc::new(service)
    }

    fn test_arbitration_service(state: &AppState) -> Arc<ArbitrationService> {
        test_arbitration_service_with_mode(state, ArbitrationMode::Shadow)
    }

    fn enable_test_arbitration(state: &mut AppState) {
        let service = test_arbitration_service(state);
        let (scheduler, _worker) = crate::maintainer::arbitration::spawn_arbitration_scheduler(
            service.clone(),
            1,
            tokio_util::sync::CancellationToken::new(),
        );
        state.arbitration = Some(service);
        state.arbitration_scheduler = Some(scheduler);
    }

    fn enable_test_arbitration_mode(state: &mut AppState, mode: ArbitrationMode) {
        let service = test_arbitration_service_with_mode(state, mode);
        let (scheduler, _worker) = crate::maintainer::arbitration::spawn_arbitration_scheduler(
            service.clone(),
            1,
            tokio_util::sync::CancellationToken::new(),
        );
        state.arbitration = Some(service);
        state.arbitration_scheduler = Some(scheduler);
    }

    async fn persist_test_analysis(
        service: &ArbitrationService,
        dispute_id: &DisputeId,
        state: AnalysisState,
    ) -> ArbitrationAnalysis {
        let mut analysis = service.create_manual_analysis(dispute_id).await.unwrap();
        analysis.state = state;
        service.store().write_analysis(&analysis).await.unwrap();
        analysis
    }

    async fn wait_until_scheduler_processed(
        scheduler: &crate::maintainer::arbitration::ArbitrationScheduler,
        job: &AnalysisJob,
    ) {
        assert!(scheduler.enqueue(job.clone()).await.unwrap());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                tokio::task::yield_now().await;
                if scheduler.enqueue(job.clone()).await.unwrap() {
                    return;
                }
            }
        })
        .await
        .expect("scheduler 应处理完屏障 job");
    }

    fn sample_claim(
        agent: &AgentId,
        status: ClaimStatus,
        created_at: chrono::DateTime<Utc>,
    ) -> Claim {
        Claim {
            id: ClaimId::random(),
            name: "claim-name".into(),
            statement: "claim statement".into(),
            scope: "order-system / batch-order-submit".into(),
            holder: agent.clone(),
            confidence: Confidence::High,
            status,
            created_at,
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "summary".into(),
        }
    }

    async fn seed_claim(team_root: &std::path::Path, claim: &Claim) {
        let path = paths::team_store_agent_claims_dir(team_root, &claim.holder)
            .join(format!("{}.yaml", claim.id));
        write_yaml_atomic(&path, claim).await.unwrap();
    }

    #[tokio::test]
    async fn list_claims_filters_by_agent_and_status() {
        let (state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        seed_claim(
            state.maintainer.team_root(),
            &sample_claim(&agent_a, ClaimStatus::Active, now_seconds()),
        )
        .await;
        seed_claim(
            state.maintainer.team_root(),
            &sample_claim(&agent_b, ClaimStatus::Stale, now_seconds()),
        )
        .await;

        let Json(result) = list_claims(
            State(state),
            Query(ClaimListQuery {
                agent: Some("agent-b".into()),
                status: Some("stale".into()),
                scope: None,
                keyword: None,
            }),
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].claim.holder, agent_b);
        assert_eq!(result[0].claim.status, ClaimStatus::Stale);
    }

    #[tokio::test]
    async fn run_sweep_records_history() {
        let (state, ..) = build_state();
        let agent = AgentId::new("agent-a").unwrap();
        seed_claim(
            state.maintainer.team_root(),
            &sample_claim(
                &agent,
                ClaimStatus::Active,
                now_seconds() - Duration::days(10),
            ),
        )
        .await;

        let Json(report) = run_sweep(State(state.clone())).await.unwrap();
        assert_eq!(report.stale_claims.len(), 1);

        let runs = state.history_store.list_sweep_runs().await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].report, report);
        assert_eq!(runs[0].trigger, "manual");
    }

    #[tokio::test]
    async fn overview_includes_sweep_schedule_status() {
        let (state, ..) = build_state();
        let now = now_seconds();
        state
            .sweep_scheduler
            .mark_auto_sweep_finished(now, "maintainer_startup")
            .await;
        state
            .sweep_scheduler
            .mark_next_after(now, std::time::Duration::from_secs(86_400))
            .await;

        let Json(response) = overview(State(state.clone())).await.unwrap();

        assert_eq!(response.sweep_schedule.tick_interval_secs, 86_400);
        assert_eq!(response.sweep_schedule.last_auto_sweep_at, Some(now));
        assert_eq!(
            response.sweep_schedule.next_sweep_at,
            Some(now + Duration::days(1))
        );
        assert_eq!(
            response.sweep_schedule.last_auto_trigger.as_deref(),
            Some("maintainer_startup")
        );
    }

    #[tokio::test]
    async fn router_query_returns_candidate_claims() {
        let (state, ..) = build_state();
        let agent = AgentId::new("agent-a").unwrap();
        let claim = sample_claim(&agent, ClaimStatus::Active, now_seconds());
        seed_claim(state.maintainer.team_root(), &claim).await;

        let Json(result) = router_query(
            State(state.clone()),
            Json(AgentQuery::from_task(
                "order-system / batch-order-submit",
                "处理批量订单拆分",
            )),
        )
        .await
        .unwrap();

        assert_eq!(result.candidate_claims.len(), 1);
        assert_eq!(result.candidate_claims[0].claim.id, claim.id);

        let audits = state
            .history_store
            .list_router_query_audits()
            .await
            .unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].scope, "order-system / batch-order-submit");
    }

    #[tokio::test]
    async fn router_query_stays_available_when_router_team_auth_enabled() {
        let (mut state, ..) = build_state();
        state.router_team_auth_enabled = true;
        let agent = AgentId::new("agent-a").unwrap();
        let claim = sample_claim(&agent, ClaimStatus::Active, now_seconds());
        seed_claim(state.maintainer.team_root(), &claim).await;

        let Json(result) = router_query(
            State(state),
            Json(AgentQuery::from_task(
                "order-system / batch-order-submit",
                "处理批量订单拆分",
            )),
        )
        .await
        .unwrap();

        assert_eq!(result.candidate_claims.len(), 1);
    }

    #[tokio::test]
    async fn upload_claim_records_agent_activity() {
        let (state, ..) = build_state();
        let agent = AgentId::new("agent-a").unwrap();
        let claim = sample_claim(&agent, ClaimStatus::Active, now_seconds());

        let status = upload_claim(
            State(state.clone()),
            Json(envelope(&agent, "", claim.clone())),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);

        let activities = state
            .history_store
            .list_agent_activity_events()
            .await
            .unwrap();
        assert_eq!(activities.len(), 1);
        assert_eq!(activities[0].agent_id, claim.holder);
        assert_eq!(
            activities[0].activity_kind,
            AgentActivityKind::ClaimUploaded
        );
    }

    #[tokio::test]
    async fn report_dispute_records_agent_activity() {
        let (state, ..) = build_state();
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "reported".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "open".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };

        let status = report_dispute(
            State(state.clone()),
            Json(envelope(&dispute.reporter_agent_id, "", dispute.clone())),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);

        let stored: Dispute = read_yaml(
            &paths::team_store_disputes_dir(state.maintainer.team_root())
                .join(format!("{}.yaml", dispute.id)),
        )
        .await
        .unwrap();
        assert_eq!(stored.reporter_agent_id, dispute.reporter_agent_id);

        let activities = state
            .history_store
            .list_agent_activity_events()
            .await
            .unwrap();
        assert_eq!(activities.len(), 1);
        assert_eq!(activities[0].agent_id, dispute.reporter_agent_id);
        assert_eq!(
            activities[0].activity_kind,
            AgentActivityKind::DisputeReported
        );
        assert!(activities[0].summary.contains(dispute.id.as_str()));
    }

    #[tokio::test]
    async fn manual_mode_report_exposes_no_automatic_analysis() {
        let (mut state, ..) = build_state();
        enable_test_arbitration_mode(&mut state, ArbitrationMode::Manual);
        let reporter = AgentId::new("agent-a").unwrap();
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "manual-only".into(),
            reporter_agent_id: reporter.clone(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "wait for an administrator".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };

        assert_eq!(
            report_dispute(
                State(state.clone()),
                Json(envelope(&reporter, "", dispute.clone())),
            )
            .await
            .unwrap(),
            StatusCode::OK
        );

        let Json(analyses) =
            list_dispute_analyses(State(state.clone()), Path(dispute.id.to_string()))
                .await
                .unwrap();
        assert!(analyses.automatic_analysis.is_none());
        assert!(analyses.manual_analysis.is_none());
        let Json(detail) = get_dispute(State(state), Path(dispute.id.to_string()))
            .await
            .unwrap();
        assert!(detail.automatic_analysis.is_none());
        assert!(detail.manual_analysis.is_none());
        assert_eq!(
            detail.record.dispute.status,
            crate::claim::DisputeStatus::Open
        );
    }

    #[tokio::test]
    async fn report_dispute_replay_with_changed_payload_returns_conflict() {
        let (mut state, ..) = build_state();
        enable_test_arbitration(&mut state);
        let reporter = AgentId::new("agent-a").unwrap();
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "reported".into(),
            reporter_agent_id: reporter.clone(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "original report".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };
        assert_eq!(
            report_dispute(
                State(state.clone()),
                Json(envelope(&reporter, "", dispute.clone())),
            )
            .await
            .unwrap(),
            StatusCode::OK
        );

        let mut changed = dispute;
        changed.summary = "changed report with the same id".into();
        let error = report_dispute(State(state), Json(envelope(&reporter, "", changed)))
            .await
            .unwrap_err();

        assert_eq!(error.0, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn report_dispute_rejects_maintainer_owned_resolution_fields() {
        let (state, ..) = build_state();
        let reporter = AgentId::new("agent-a").unwrap();
        let mut dispute = Dispute {
            id: DisputeId::random(),
            name: "invalid_report".into(),
            reporter_agent_id: reporter.clone(),
            claims: vec![ClaimId::random()],
            summary: "invalid".into(),
            status: crate::claim::DisputeStatus::Resolved,
            created_at: now_seconds(),
            resolved_at: None,
        };

        let error = report_dispute(
            State(state.clone()),
            Json(envelope(&reporter, "", dispute.clone())),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);

        dispute.status = crate::claim::DisputeStatus::Open;
        dispute.resolved_at = Some(now_seconds());
        let error = report_dispute(
            State(state.clone()),
            Json(envelope(&reporter, "", dispute.clone())),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::BAD_REQUEST);

        let path = paths::team_store_disputes_dir(state.maintainer.team_root())
            .join(format!("{}.yaml", dispute.id));
        assert!(!path.exists());
        assert!(state
            .history_store
            .list_agent_activity_events()
            .await
            .unwrap()
            .is_empty());
    }

    fn enable_auth_for(state: &mut AppState, agent: &AgentId, key: &str) {
        state.auth = crate::auth::AuthVerifier::from_config(&crate::auth::AuthConfig {
            enabled: true,
            api_keys: vec![crate::auth::AuthApiKeyConfig {
                key_id: "key_test".into(),
                agent_id: agent.clone(),
                key_hash: format!("sha256:{}", crate::auth::sha256_hex(key)),
                generated_time: "2026-06-26T12:00:00Z".parse().unwrap(),
                status: crate::auth::AuthKeyStatus::Active,
            }],
        })
        .unwrap();
    }

    fn envelope<T>(agent: &AgentId, key: &str, data: T) -> AuthRequest<T> {
        crate::auth::AuthRequest {
            auth: AuthEnvelope {
                agent_id: agent.clone(),
                acn_key: key.into(),
            },
            data,
        }
    }

    #[tokio::test]
    async fn disabled_team_auth_still_binds_auth_agent_to_body_agent() {
        let (state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();

        let forbidden = pull_inbox(
            State(state.clone()),
            Json(envelope(
                &agent_a,
                "",
                PullInboxRequest {
                    agent_id: agent_b.clone(),
                },
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);

        let claim = sample_claim(&agent_b, ClaimStatus::Active, now_seconds());
        let forbidden = upload_claim(State(state.clone()), Json(envelope(&agent_a, "", claim)))
            .await
            .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);

        let dispute = Dispute {
            id: DisputeId::random(),
            name: "reported".into(),
            reporter_agent_id: agent_b,
            claims: vec![ClaimId::random()],
            summary: "open".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };
        let forbidden = report_dispute(State(state), Json(envelope(&agent_a, "", dispute)))
            .await
            .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn agent_facing_endpoints_reject_router_service_identity() {
        let (state, ..) = build_state();
        let service = AgentId::new(crate::auth::ROUTER_SERVICE_AGENT_ID).unwrap();

        let forbidden = pull_inbox(
            State(state.clone()),
            Json(envelope(
                &service,
                "",
                PullInboxRequest {
                    agent_id: service.clone(),
                },
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);

        let claim = sample_claim(&service, ClaimStatus::Active, now_seconds());
        let forbidden = upload_claim(State(state.clone()), Json(envelope(&service, "", claim)))
            .await
            .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);

        let dispute = Dispute {
            id: DisputeId::random(),
            name: "reported".into(),
            reporter_agent_id: service.clone(),
            claims: vec![ClaimId::random()],
            summary: "open".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };
        let forbidden = report_dispute(State(state), Json(envelope(&service, "", dispute)))
            .await
            .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn pull_inbox_binds_body_agent_to_principal() {
        let (mut state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        enable_auth_for(&mut state, &agent_a, "secret");

        // principal=agent-a 但 body=agent-b → 对象级越权，必须 403
        let forbidden = pull_inbox(
            State(state.clone()),
            Json(envelope(
                &agent_a,
                "secret",
                PullInboxRequest { agent_id: agent_b },
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);

        // principal 与 body 一致 → 放行（lazy register agent-a）
        let Json(messages) = pull_inbox(
            State(state.clone()),
            Json(envelope(
                &agent_a,
                "secret",
                PullInboxRequest {
                    agent_id: agent_a.clone(),
                },
            )),
        )
        .await
        .unwrap();
        assert!(messages.is_empty());

        let activities = state
            .history_store
            .list_agent_activity_events()
            .await
            .unwrap();
        assert_eq!(activities.len(), 1);
        assert_eq!(activities[0].agent_id, agent_a);
        assert_eq!(activities[0].activity_kind, AgentActivityKind::InboxPulled);
        assert_eq!(activities[0].summary, "inbox_pulled offered_messages=0");

        let Json(agents) = list_agents(State(state), Query(AgentListQuery { agent: None }))
            .await
            .unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(
            agents[0]
                .last_activity
                .as_ref()
                .map(|activity| &activity.activity_kind),
            Some(&AgentActivityKind::InboxPulled)
        );
    }

    #[tokio::test]
    async fn ack_inbox_binds_principal_and_maps_domain_errors_without_legacy_404() {
        let (state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();

        let forbidden = ack_inbox(
            State(state.clone()),
            Json(envelope(
                &agent_a,
                "",
                InboxAckRequest {
                    agent_id: agent_b.clone(),
                    inbox_ids: vec![],
                },
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);

        let unknown = ack_inbox(
            State(state.clone()),
            Json(envelope(
                &agent_a,
                "",
                InboxAckRequest {
                    agent_id: agent_a.clone(),
                    inbox_ids: vec![crate::claim::InboxId::random()],
                },
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(unknown.0, StatusCode::BAD_REQUEST);

        let (policy_id, _) = state
            .maintainer
            .publish_new_policy(
                "policy".into(),
                "statement".into(),
                "scope".into(),
                now_seconds(),
                None,
            )
            .await
            .unwrap();
        let unoffered_id = state
            .maintainer
            .list_outbox_entries(None, None)
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.inbox_message.policy_id() == &policy_id)
            .unwrap()
            .inbox_id;
        let unoffered = ack_inbox(
            State(state.clone()),
            Json(envelope(
                &agent_a,
                "",
                InboxAckRequest {
                    agent_id: agent_a.clone(),
                    inbox_ids: vec![unoffered_id],
                },
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(unoffered.0, StatusCode::CONFLICT);

        let (targeted_policy_id, _) = state
            .maintainer
            .publish_new_policy(
                "targeted".into(),
                "statement".into(),
                "scope".into(),
                now_seconds(),
                Some(vec![agent_a.clone()]),
            )
            .await
            .unwrap();
        let targeted_id = state
            .maintainer
            .list_outbox_entries(None, None)
            .await
            .unwrap()
            .into_iter()
            .find(|entry| entry.inbox_message.policy_id() == &targeted_policy_id)
            .unwrap()
            .inbox_id;
        let wrong_target = ack_inbox(
            State(state),
            Json(envelope(
                &agent_b,
                "",
                InboxAckRequest {
                    agent_id: agent_b.clone(),
                    inbox_ids: vec![targeted_id],
                },
            )),
        )
        .await
        .unwrap_err();
        assert_eq!(wrong_target.0, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn ack_inbox_confirms_offered_message_and_stops_redelivery() {
        let (state, ..) = build_state();
        let agent = AgentId::new("agent-a").unwrap();
        state
            .maintainer
            .publish_new_policy(
                "policy".into(),
                "statement".into(),
                "scope".into(),
                now_seconds(),
                None,
            )
            .await
            .unwrap();
        let pulled = state.maintainer.pull_inbox(&agent).await.unwrap();
        assert_eq!(pulled.len(), 1);

        let status = ack_inbox(
            State(state.clone()),
            Json(envelope(
                &agent,
                "",
                InboxAckRequest {
                    agent_id: agent.clone(),
                    inbox_ids: vec![pulled[0].id.clone()],
                },
            )),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(state
            .maintainer
            .pull_inbox(&agent)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn upload_claim_auth_binds_holder_to_key_agent() {
        let (mut state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        enable_auth_for(&mut state, &agent_a, "secret");

        let claim = sample_claim(&agent_b, ClaimStatus::Active, now_seconds());
        let forbidden = upload_claim(State(state), Json(envelope(&agent_a, "secret", claim)))
            .await
            .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn report_dispute_auth_binds_reporter_to_key_agent() {
        let (mut state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        enable_auth_for(&mut state, &agent_a, "secret");

        let dispute = Dispute {
            id: DisputeId::random(),
            name: "reported".into(),
            reporter_agent_id: agent_b,
            claims: vec![ClaimId::random()],
            summary: "open".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };
        let forbidden = report_dispute(State(state), Json(envelope(&agent_a, "secret", dispute)))
            .await
            .unwrap_err();
        assert_eq!(forbidden.0, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn team_auth_key_api_creates_lists_and_revokes_without_hash() {
        let (state, ..) = build_state();

        let Json(created) = create_team_auth_key(
            State(state.clone()),
            Json(CreateTeamAuthKeyRequest {
                agent_id: "agent-a".into(),
            }),
        )
        .await
        .unwrap();
        assert!(created.acn_key.starts_with("acn_"));
        assert_eq!(created.key.agent_id.as_str(), "agent-a");
        assert_eq!(created.key.status, crate::auth::AuthKeyStatus::Active);

        let Json(rows) = list_team_auth_keys(State(state.clone())).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key_id, created.key.key_id);
        let serialized = serde_json::to_string(&rows).unwrap();
        assert!(!serialized.contains(&created.acn_key));
        assert!(!serialized.contains("key_hash"));

        let duplicate = create_team_auth_key(
            State(state.clone()),
            Json(CreateTeamAuthKeyRequest {
                agent_id: "agent-a".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(duplicate.0, StatusCode::CONFLICT);
        assert_eq!(
            duplicate.1,
            format!(
                "Agent 'agent-a' has active key '{}'! Revoke before creating new ones.",
                created.key.key_id
            )
        );

        let agent = AgentId::new("agent-a").unwrap();
        let claim = sample_claim(&agent, ClaimStatus::Active, now_seconds());
        let status = upload_claim(
            State(state.clone()),
            Json(envelope(&agent, &created.acn_key, claim)),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);

        let Json(revoked) = revoke_team_auth_key(State(state.clone()), Path(created.key.key_id))
            .await
            .unwrap();
        assert_eq!(revoked.status, crate::auth::AuthKeyStatus::Revoked);

        let Json(replacement) = create_team_auth_key(
            State(state.clone()),
            Json(CreateTeamAuthKeyRequest {
                agent_id: "agent-a".into(),
            }),
        )
        .await
        .unwrap();
        let claim = sample_claim(&agent, ClaimStatus::Active, now_seconds());
        let status = upload_claim(
            State(state),
            Json(envelope(&agent, &replacement.acn_key, claim)),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn team_auth_key_api_requires_admin_auth_enabled() {
        let (mut state, ..) = build_state();
        state.admin_auth = None;

        let err = list_team_auth_keys(State(state.clone())).await.unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1.contains("admin auth"));

        let err = create_team_auth_key(
            State(state),
            Json(CreateTeamAuthKeyRequest {
                agent_id: "agent-a".into(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
        assert!(err.1.contains("admin auth"));
    }

    #[tokio::test]
    async fn team_auth_key_api_rejects_router_service_agent_id() {
        let (state, ..) = build_state();

        let err = create_team_auth_key(
            State(state),
            Json(CreateTeamAuthKeyRequest {
                agent_id: crate::auth::ROUTER_SERVICE_AGENT_ID.into(),
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("系统保留身份"));
        assert!(err.1.contains(crate::auth::ROUTER_SERVICE_AGENT_ID));
    }

    #[tokio::test]
    async fn team_auth_key_api_returns_readable_invalid_agent_id_message() {
        let (state, ..) = build_state();

        let err = create_team_auth_key(
            State(state),
            Json(CreateTeamAuthKeyRequest {
                agent_id: "agent.A".into(),
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert_eq!(err.1, "Agent id 不合法，请修改为 ^[a-z0-9_-]+$ 组合！");
    }

    #[tokio::test]
    async fn create_and_deprecate_policy_record_history() {
        let (state, ..) = build_state();
        let Json(policy) = create_policy(
            State(state.clone()),
            Json(CreatePolicyRequest {
                name: "p".into(),
                statement: "s".into(),
                scope: "scope".into(),
                target_agents: None,
            }),
        )
        .await
        .unwrap();

        let events = state.history_store.list_policy_events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].policy_id, policy.id);
        assert_eq!(events[0].event_kind, PolicyEventKind::PolicyUpdatePublished);

        let Json(_resp) = deprecate_policy(
            State(state.clone()),
            Json(DeprecatePolicyRequest {
                policy_id: policy.id.clone(),
            }),
        )
        .await
        .unwrap();
        let events = state.history_store.list_policy_events().await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .any(|event| event.event_kind == PolicyEventKind::PolicyDeprecated));
    }

    #[tokio::test]
    async fn create_policy_accepts_registered_target_agents() {
        let (state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        seed_claim(
            state.maintainer.team_root(),
            &sample_claim(&agent_a, ClaimStatus::Active, now_seconds()),
        )
        .await;

        let Json(policy) = create_policy(
            State(state.clone()),
            Json(CreatePolicyRequest {
                name: "targeted-policy".into(),
                statement: "statement".into(),
                scope: "scope".into(),
                target_agents: Some(vec![agent_a.clone()]),
            }),
        )
        .await
        .unwrap();

        assert_eq!(policy.target_agents, Some(vec![agent_a]));
    }

    #[tokio::test]
    async fn create_policy_rejects_unknown_target_agents() {
        let (state, ..) = build_state();
        let err = create_policy(
            State(state),
            Json(CreatePolicyRequest {
                name: "targeted-policy".into(),
                statement: "statement".into(),
                scope: "scope".into(),
                target_agents: Some(vec![AgentId::new("agent-missing").unwrap()]),
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("未知 target agent"));
    }

    #[tokio::test]
    async fn claim_update_suggestion_empty_target_agents_stays_broadcast() {
        let (state, ..) = build_state();
        let Json(policy) = claim_update_suggestion(
            State(state),
            Json(ClaimUpdateSuggestionRequest {
                statement: "update statement".into(),
                target_agents: Some(vec![]),
            }),
        )
        .await
        .unwrap();

        assert_eq!(policy.target_agents, None);
    }

    #[tokio::test]
    async fn claim_update_suggestion_rejects_unknown_target_agents() {
        let (state, ..) = build_state();
        let err = claim_update_suggestion(
            State(state),
            Json(ClaimUpdateSuggestionRequest {
                statement: "update statement".into(),
                target_agents: Some(vec![AgentId::new("agent-missing").unwrap()]),
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("未知 target agent"));
    }

    #[tokio::test]
    async fn deprecate_policy_returns_not_found_for_missing_policy() {
        let (state, ..) = build_state();
        let err = deprecate_policy(
            State(state),
            Json(DeprecatePolicyRequest {
                policy_id: PolicyId::random(),
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::NOT_FOUND);
        assert!(err.1.contains("未找到 policy"));
    }

    #[tokio::test]
    async fn deprecate_policy_returns_conflict_for_already_deprecated_policy() {
        let (state, ..) = build_state();
        let Json(policy) = create_policy(
            State(state.clone()),
            Json(CreatePolicyRequest {
                name: "p".into(),
                statement: "s".into(),
                scope: "scope".into(),
                target_agents: None,
            }),
        )
        .await
        .unwrap();

        let Json(_resp) = deprecate_policy(
            State(state.clone()),
            Json(DeprecatePolicyRequest {
                policy_id: policy.id.clone(),
            }),
        )
        .await
        .unwrap();

        let err = deprecate_policy(
            State(state),
            Json(DeprecatePolicyRequest {
                policy_id: policy.id,
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(err.0, StatusCode::CONFLICT);
        assert!(err.1.contains("已废弃"));
    }

    #[tokio::test]
    async fn resolve_dispute_records_history() {
        let (state, ..) = build_state();
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "d".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "open".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };
        write_yaml_atomic(
            &paths::team_store_disputes_dir(state.maintainer.team_root())
                .join(format!("{}.yaml", dispute.id)),
            &dispute,
        )
        .await
        .unwrap();

        let status = resolve_dispute(
            State(state.clone()),
            Path(dispute.id.to_string()),
            Json(ResolveDisputeRequest {
                resolve_note: "resolved".into(),
                notify_affected_agents: false,
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        let stored: Dispute = read_yaml(
            &paths::team_store_disputes_dir(state.maintainer.team_root())
                .join(format!("{}.yaml", dispute.id)),
        )
        .await
        .unwrap();
        assert_eq!(stored.status, crate::claim::DisputeStatus::Resolved);
        assert_eq!(stored.summary, "open");

        let events = state
            .history_store
            .list_dispute_resolution_events()
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].dispute_id, dispute.id);
        assert_eq!(events[0].summary.as_deref(), Some("resolved"));
    }

    #[tokio::test]
    async fn resolve_dispute_with_notify_targets_related_claim_holders() {
        let (state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let claim_a = sample_claim(&agent_a, ClaimStatus::Active, now_seconds());
        let claim_b = sample_claim(&agent_b, ClaimStatus::Active, now_seconds());
        seed_claim(state.maintainer.team_root(), &claim_a).await;
        seed_claim(state.maintainer.team_root(), &claim_b).await;
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "d".into(),
            reporter_agent_id: agent_a.clone(),
            claims: vec![claim_a.id.clone(), claim_b.id.clone()],
            summary: "original dispute".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };
        write_yaml_atomic(
            &paths::team_store_disputes_dir(state.maintainer.team_root())
                .join(format!("{}.yaml", dispute.id)),
            &dispute,
        )
        .await
        .unwrap();

        let status = resolve_dispute(
            State(state.clone()),
            Path(dispute.id.to_string()),
            Json(ResolveDisputeRequest {
                resolve_note: "scope differs".into(),
                notify_affected_agents: true,
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        let policies = state.maintainer.list_policies().await.unwrap();
        assert_eq!(policies.len(), 1);
        let policy = &policies[0];
        assert_eq!(policy.message_type, PolicyMessageType::ClaimAttributeUpdate);
        assert_eq!(
            policy.target_agents,
            Some(vec![agent_a.clone(), agent_b.clone()])
        );
        assert!(policy.statement.contains(dispute.id.as_str()));
        assert!(policy.statement.contains("Conclusion: scope differs"));
        assert!(policy
            .statement
            .contains("Original dispute: original dispute"));
        assert!(policy.statement.contains(claim_a.id.as_str()));
        assert!(policy.statement.contains(claim_b.id.as_str()));

        let outbox = state
            .maintainer
            .list_outbox_entries(None, None)
            .await
            .unwrap();
        assert_eq!(outbox.len(), 2);
        for entry in outbox {
            let InboxMessageKind::ClaimAttributeUpdate {
                arbitration_resolution,
                ..
            } = entry.inbox_message.kind
            else {
                panic!("human Resolution 通知必须使用 ClaimAttributeUpdate");
            };
            let context = arbitration_resolution
                .expect("即使 assessments 为空，通知也必须携带结构化 Resolution");
            assert_eq!(context.dispute_id, dispute.id);
            assert_eq!(context.resolution.conclusion, "scope differs");
            assert!(context.resolution.claim_assessments.is_empty());
        }

        let events = state.history_store.list_policy_events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].event_kind,
            PolicyEventKind::ClaimAttributeUpdatePublished
        );
    }

    #[tokio::test]
    async fn resolve_dispute_rejects_empty_note_and_already_resolved() {
        let (state, ..) = build_state();
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "d".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "open".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: now_seconds(),
            resolved_at: None,
        };
        write_yaml_atomic(
            &paths::team_store_disputes_dir(state.maintainer.team_root())
                .join(format!("{}.yaml", dispute.id)),
            &dispute,
        )
        .await
        .unwrap();

        let empty_note = resolve_dispute(
            State(state.clone()),
            Path(dispute.id.to_string()),
            Json(ResolveDisputeRequest {
                resolve_note: "  ".into(),
                notify_affected_agents: false,
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(empty_note.0, StatusCode::BAD_REQUEST);

        let status = resolve_dispute(
            State(state.clone()),
            Path(dispute.id.to_string()),
            Json(ResolveDisputeRequest {
                resolve_note: "resolved".into(),
                notify_affected_agents: false,
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        let already_resolved = resolve_dispute(
            State(state),
            Path(dispute.id.to_string()),
            Json(ResolveDisputeRequest {
                resolve_note: "again".into(),
                notify_affected_agents: false,
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(already_resolved.0, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn manual_analysis_returns_conflict_when_arbitration_is_disabled() {
        let (state, ..) = build_state();

        let error = create_manual_analysis(State(state), Path(DisputeId::random().to_string()))
            .await
            .unwrap_err();

        assert_eq!(error.0, StatusCode::CONFLICT);
        assert!(error.1.contains("disabled"));
    }

    #[tokio::test]
    async fn cancelled_analysis_request_wakes_the_persisted_job_without_restart() {
        let (mut state, ..) = build_state();
        let store = ArbitrationStore::new(state.maintainer.team_root().to_path_buf());
        let service = test_arbitration_service_with_mode(&state, ArbitrationMode::Manual);
        let (scheduler, _worker) = crate::maintainer::arbitration::spawn_arbitration_scheduler(
            service.clone(),
            1,
            tokio_util::sync::CancellationToken::new(),
        );
        state.arbitration = Some(service.clone());
        state.arbitration_scheduler = Some(scheduler.clone());

        // 先穿过一个 terminal job，保证 startup scan 已完成；随后落盘的 Pending
        // Analysis 只能由请求取消 guard 的 durable wake 被发现。
        let barrier_dispute = Dispute {
            id: DisputeId::random(),
            name: "scheduler barrier".into(),
            reporter_agent_id: AgentId::new("agent-barrier").unwrap(),
            claims: Vec::new(),
            summary: "terminal analysis used as scheduler barrier".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        store
            .write_dispute(&MaintainerDisputeRecord::from(barrier_dispute.clone()))
            .await
            .unwrap();
        let barrier = persist_test_analysis(
            service.as_ref(),
            &barrier_dispute.id,
            AnalysisState::Approved,
        )
        .await;
        wait_until_scheduler_processed(
            &scheduler,
            &AnalysisJob {
                dispute_id: barrier_dispute.id,
                analysis_id: barrier.analysis_id,
                source: AnalysisSource::Manual,
            },
        )
        .await;

        let holder = AgentId::new("agent-a").unwrap();
        let claim_a = sample_claim(&holder, ClaimStatus::Active, now_seconds());
        let claim_b = sample_claim(&holder, ClaimStatus::Active, now_seconds());
        seed_claim(state.maintainer.team_root(), &claim_a).await;
        seed_claim(state.maintainer.team_root(), &claim_b).await;
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "cancelled manual analysis".into(),
            reporter_agent_id: holder,
            claims: vec![claim_a.id, claim_b.id],
            summary: "the request is cancelled immediately after its durable write".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        store
            .write_dispute(&MaintainerDisputeRecord::from(dispute.clone()))
            .await
            .unwrap();
        let analysis = service.create_manual_analysis(&dispute.id).await.unwrap();
        let job = AnalysisJob {
            dispute_id: dispute.id,
            analysis_id: analysis.analysis_id,
            source: AnalysisSource::Manual,
        };

        drop(AnalysisRecoveryWakeGuard::new(Some(&scheduler)));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if store.read_analysis(&job).await.unwrap().state != AnalysisState::Pending {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("同一进程应接管已经持久化的 Analysis");
    }

    #[tokio::test]
    async fn cancelled_adopt_drop_guard_recovers_delivery_without_restart() {
        let (mut state, ..) = build_state();
        let store = ArbitrationStore::new(state.maintainer.team_root().to_path_buf());
        let service = test_arbitration_service(&state);

        // 先用一个 terminal job 穿过 scheduler，保证后面创建的 Adopting 记录不可能被
        // startup recovery 扫到；其收敛只能来自显式 Adopt 的补偿入队。
        let sentinel_dispute = Dispute {
            id: DisputeId::random(),
            name: "scheduler barrier".into(),
            reporter_agent_id: AgentId::new("agent-sentinel").unwrap(),
            claims: vec![ClaimId::random()],
            summary: "terminal analysis used as scheduler barrier".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        store
            .write_dispute(&MaintainerDisputeRecord::from(sentinel_dispute.clone()))
            .await
            .unwrap();
        let sentinel = persist_test_analysis(
            service.as_ref(),
            &sentinel_dispute.id,
            AnalysisState::Approved,
        )
        .await;
        let sentinel_job = AnalysisJob {
            dispute_id: sentinel_dispute.id,
            analysis_id: sentinel.analysis_id,
            source: AnalysisSource::Manual,
        };
        let (scheduler, _worker) = crate::maintainer::arbitration::spawn_arbitration_scheduler(
            service.clone(),
            1,
            tokio_util::sync::CancellationToken::new(),
        );
        state.arbitration = Some(service.clone());
        state.arbitration_scheduler = Some(scheduler.clone());
        wait_until_scheduler_processed(&scheduler, &sentinel_job).await;

        let holder = AgentId::new("agent-a").unwrap();
        let claim_a = sample_claim(&holder, ClaimStatus::Active, now_seconds());
        let claim_b = sample_claim(&holder, ClaimStatus::Deprecated, now_seconds());
        seed_claim(state.maintainer.team_root(), &claim_a).await;
        seed_claim(state.maintainer.team_root(), &claim_b).await;
        let original_dispute = Dispute {
            id: DisputeId::random(),
            name: "delivery recovery".into(),
            reporter_agent_id: holder.clone(),
            claims: vec![claim_a.id.clone(), claim_b.id.clone()],
            summary: "resolution 已提交，但 holder 通知发生瞬时故障".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        store
            .write_dispute(&MaintainerDisputeRecord::from(original_dispute.clone()))
            .await
            .unwrap();
        let mut analysis = service
            .create_manual_analysis(&original_dispute.id)
            .await
            .unwrap();
        let resolution_id = ArbitrationResolutionId::random();
        let resolved_at = "2026-08-03T00:00:00Z".parse().unwrap();
        let assessments = vec![
            ClaimAssessment {
                claim_id: claim_a.id.clone(),
                recommended_status: ClaimStatus::Active,
                assessment: "保留当前知识".into(),
                recommended_scope: None,
                recommended_statement: None,
                reason: "生产证据仍有效".into(),
            },
            ClaimAssessment {
                claim_id: claim_b.id.clone(),
                recommended_status: ClaimStatus::Deprecated,
                assessment: "保留 deprecated".into(),
                recommended_scope: None,
                recommended_statement: None,
                reason: "历史边界已失效".into(),
            },
        ];
        let resolution = DisputeResolution {
            resolution_id: resolution_id.clone(),
            resolved_by: ResolvedBy::Human,
            resolved_at,
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::Evidence),
            conclusion: "采用有生产证据支持的知识".into(),
            claim_assessments: assessments,
            rejection_reason: None,
        };
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration_result".into(),
            statement: "请 holder 内化已采用的裁决".into(),
            scope: "maintainer / dispute arbitration".into(),
            status: PolicyStatus::Active,
            created_at: resolved_at,
            updated_at: None,
            target_agents: Some(vec![holder.clone()]),
        };
        let inbox_id = InboxId::random();
        let inbox_message = InboxMessage {
            id: inbox_id.clone(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: policy.clone(),
                arbitration_resolution: Some(Box::new(ArbitrationResolutionContext {
                    dispute_id: original_dispute.id.clone(),
                    resolution: resolution.clone(),
                    context_snapshot_hash: None,
                    dispute_snapshot: original_dispute.clone(),
                    direct_claim_snapshots: vec![claim_a.clone(), claim_b.clone()],
                    snapshot_source_resolution_id: None,
                })),
            },
            handled_at: None,
        };
        // DeliveryTargetIntent 是 Maintainer 私有恢复细节；通过其稳定序列化协议构造
        // 一份真实 delivery intent，避免测试越过模块可见性边界。
        let delivery_intent: DeliveryIntent = serde_json::from_value(serde_json::json!({
            "policy": policy,
            "maintainer_action_id": MaintainerActionId::random(),
            "targets": [{
                "inbox_id": inbox_id,
                "target_agent": holder,
                "inbox_message": inbox_message,
            }],
        }))
        .unwrap();
        let resolution_record = ArbitrationResolutionRecord {
            schema_version: analysis.schema_version,
            resolution_id: resolution_id.clone(),
            dispute_id: original_dispute.id.clone(),
            created_at: resolved_at,
            resolution: resolution.clone(),
            dispute_snapshot: original_dispute.clone(),
            direct_claim_snapshots: vec![claim_a, claim_b],
            semantic_fingerprint: None,
            context_snapshot_hash: None,
            analysis_source_id: Some(analysis.analysis_id.clone()),
            legacy_source_attempt_id: None,
            delivery_intent: Some(delivery_intent),
            snapshot_source_resolution_id: None,
        };
        store
            .write_resolution_record(&resolution_record)
            .await
            .unwrap();
        let mut resolved_dispute = original_dispute;
        resolved_dispute.status = crate::claim::DisputeStatus::Resolved;
        resolved_dispute.resolved_at = Some(resolved_at);
        store
            .write_dispute(&MaintainerDisputeRecord {
                dispute: resolved_dispute,
                resolution: Some(resolution),
            })
            .await
            .unwrap();
        analysis.state = AnalysisState::Adopting;
        analysis.resolution_id = Some(resolution_id);
        analysis.pending_resolution = Some(resolution_record);
        analysis.delivery_error = Some("holder 通知投递待恢复；详见 Maintainer 日志".into());
        store.write_analysis(&analysis).await.unwrap();
        let job = AnalysisJob {
            dispute_id: analysis.dispute_id.clone(),
            analysis_id: analysis.analysis_id.clone(),
            source: AnalysisSource::Manual,
        };

        // 模拟 HTTP Adopt 在 fixed intent 落盘后被取消：future drop 只能执行同步
        // guard，不能 await enqueue。durable wake 必须让同进程 scheduler 接管。
        drop(AdoptionRecoveryWakeGuard::new(
            state.arbitration_scheduler.as_ref(),
            state.resolution_events.as_ref(),
        ));

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if store.read_analysis(&job).await.unwrap().state == AnalysisState::Adopted {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("同一进程内的 scheduler 应完成 delivery 恢复");
        let outbox = state
            .maintainer
            .list_outbox_entries(None, None)
            .await
            .unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].target,
            crate::claim::OutboxTarget::Targeted {
                target_agent: AgentId::new("agent-a").unwrap(),
            }
        );
    }

    #[tokio::test]
    async fn dispute_mutations_return_not_found_for_missing_dispute() {
        let (mut state, ..) = build_state();
        let dispute_id = DisputeId::random();

        let resolve_error = resolve_dispute(
            State(state.clone()),
            Path(dispute_id.to_string()),
            Json(ResolveDisputeRequest {
                resolve_note: "human conclusion".into(),
                notify_affected_agents: false,
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(resolve_error.0, StatusCode::NOT_FOUND);

        let reject_error = reject_dispute_resolution(
            State(state.clone()),
            Path(dispute_id.to_string()),
            Json(RejectResolutionRequest {
                expected_resolution_id: ArbitrationResolutionId::random(),
                rejection_reason: "new evidence".into(),
                conclusion: "replacement conclusion".into(),
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(reject_error.0, StatusCode::NOT_FOUND);

        enable_test_arbitration(&mut state);
        let analyze_error = create_manual_analysis(State(state), Path(dispute_id.to_string()))
            .await
            .unwrap_err();
        assert_eq!(analyze_error.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn reject_returns_conflict_for_legacy_resolution_without_structured_record() {
        let (state, ..) = build_state();
        let agent = AgentId::new("agent-a").unwrap();
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "legacy resolution".into(),
            reporter_agent_id: agent,
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "historical resolved dispute without structured resolution".into(),
            status: crate::claim::DisputeStatus::Resolved,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: Some("2026-08-02T00:00:00Z".parse().unwrap()),
        };
        ArbitrationStore::new(state.maintainer.team_root().to_path_buf())
            .write_dispute(&MaintainerDisputeRecord {
                dispute: dispute.clone(),
                resolution: None,
            })
            .await
            .unwrap();

        let error = reject_dispute_resolution(
            State(state),
            Path(dispute.id.to_string()),
            Json(RejectResolutionRequest {
                expected_resolution_id: ArbitrationResolutionId::random(),
                rejection_reason: "human review".into(),
                conclusion: "replacement".into(),
                resolution_type: None,
                resolution_basis: None,
                claim_assessments: Vec::new(),
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(error.0, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn reject_replaces_automatic_resolution_and_fences_outdated_request() {
        let (state, ..) = build_state();
        let agent_a = AgentId::new("agent-a").unwrap();
        let agent_b = AgentId::new("agent-b").unwrap();
        let claim_a = sample_claim(&agent_a, ClaimStatus::Active, now_seconds());
        let claim_b = sample_claim(&agent_b, ClaimStatus::Deprecated, now_seconds());
        seed_claim(state.maintainer.team_root(), &claim_a).await;
        seed_claim(state.maintainer.team_root(), &claim_b).await;
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "automatic resolution".into(),
            reporter_agent_id: agent_a,
            claims: vec![claim_a.id.clone(), claim_b.id.clone()],
            summary: "original summary".into(),
            status: crate::claim::DisputeStatus::Resolved,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: Some("2026-08-02T00:00:00Z".parse().unwrap()),
        };
        let resolution_id = ArbitrationResolutionId::random();
        let resolution = crate::claim::DisputeResolution {
            resolution_id: resolution_id.clone(),
            resolved_by: crate::claim::ResolvedBy::Automatic,
            resolved_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::Evidence),
            conclusion: "automatic conclusion".into(),
            claim_assessments: Vec::new(),
            rejection_reason: None,
        };
        let store = ArbitrationStore::new(state.maintainer.team_root().to_path_buf());
        store
            .write_dispute(&MaintainerDisputeRecord {
                dispute: dispute.clone(),
                resolution: Some(resolution.clone()),
            })
            .await
            .unwrap();
        store
            .write_resolution_record(&ArbitrationResolutionRecord {
                schema_version: 2,
                resolution_id: resolution_id.clone(),
                dispute_id: dispute.id.clone(),
                created_at: resolution.resolved_at,
                resolution,
                dispute_snapshot: dispute.clone(),
                direct_claim_snapshots: vec![claim_a, claim_b],
                semantic_fingerprint: Some("sha256-v1:test".into()),
                context_snapshot_hash: Some("sha256-v1:snapshot".into()),
                analysis_source_id: None,
                legacy_source_attempt_id: None,
                delivery_intent: None,
                snapshot_source_resolution_id: None,
            })
            .await
            .unwrap();
        let request = RejectResolutionRequest {
            expected_resolution_id: resolution_id.clone(),
            rejection_reason: "human evidence supersedes it".into(),
            conclusion: "reviewed conclusion".into(),
            resolution_type: Some(ResolutionType::Coexist),
            resolution_basis: Some(ResolutionBasis::DirectAnalysis),
            claim_assessments: Vec::new(),
        };

        let (status, Json(replacement)) = reject_dispute_resolution(
            State(state.clone()),
            Path(dispute.id.to_string()),
            Json(request.clone()),
        )
        .await
        .unwrap();

        assert_eq!(status, StatusCode::CREATED);
        assert_ne!(replacement.resolution.resolution_id, resolution_id);
        assert_eq!(
            replacement.resolution.resolved_by,
            crate::claim::ResolvedBy::Human
        );
        assert_eq!(
            store
                .read_dispute(&dispute.id)
                .await
                .unwrap()
                .dispute
                .summary,
            "original summary"
        );

        let outdated_request =
            reject_dispute_resolution(State(state), Path(dispute.id.to_string()), Json(request))
                .await
                .unwrap_err();
        assert_eq!(outdated_request.0, StatusCode::CONFLICT);
    }
}
