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
    AgentId, Claim, ClaimId, ClaimStatus, Dispute, DisputeId, InboxAckRequest, InboxMessage,
    OutboxEntry, Policy, PolicyId,
};
use crate::maintainer::history::{
    fresh_record_id, AgentActivityKind, AgentActivityRecord, DisputeResolutionEventRecord,
    HttpAuditRecord, PolicyEventKind, PolicyEventRecord, RouterQueryAuditRecord, SweepRunRecord,
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
) -> Result<Json<Vec<Dispute>>, (StatusCode, String)> {
    let mut disputes = state
        .maintainer
        .list_disputes()
        .await
        .map_err(internal_error)?;
    disputes.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    let disputes = disputes
        .into_iter()
        .filter(|dispute| match query.status.as_deref() {
            Some("open") => dispute.status == crate::claim::DisputeStatus::Open,
            Some("resolved") => dispute.status == crate::claim::DisputeStatus::Resolved,
            _ => true,
        })
        .collect();
    Ok(Json(disputes))
}

pub async fn get_dispute(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Dispute>, (StatusCode, String)> {
    let dispute_id = DisputeId::from_str(&id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("非法 dispute id: {e}")))?;
    let dispute = state
        .maintainer
        .list_disputes()
        .await
        .map_err(internal_error)?
        .into_iter()
        .find(|item| item.id == dispute_id)
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("未找到 dispute: {id}")))?;
    Ok(Json(dispute))
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
    state
        .maintainer
        .report_dispute(&dispute)
        .await
        .map_err(internal_error)?;
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
    let dispute_id = DisputeId::from_str(&dispute_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("非法 dispute id: {e}")))?;
    let resolve_note = req.resolve_note.trim();
    if resolve_note.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Resolve Note 不能为空".to_string()));
    }

    let dispute = state
        .maintainer
        .list_disputes()
        .await
        .map_err(internal_error)?
        .into_iter()
        .find(|item| item.id == dispute_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("未找到 dispute: {dispute_id}"),
            )
        })?;
    if dispute.status == crate::claim::DisputeStatus::Resolved {
        return Err((
            StatusCode::CONFLICT,
            format!("dispute 已 resolve: {dispute_id}"),
        ));
    }
    let notification = if req.notify_affected_agents {
        let all_claims = state
            .maintainer
            .list_all_claims()
            .await
            .map_err(internal_error)?;
        let related_claims = related_claims_for_dispute(&dispute, &all_claims);
        if related_claims.len() != dispute.claims.len() {
            let missing: Vec<String> = dispute
                .claims
                .iter()
                .filter(|claim_id| !related_claims.iter().any(|claim| &claim.id == *claim_id))
                .map(ToString::to_string)
                .collect();
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "无法通知 affected agents，缺少相关 claim mirror: {}",
                    missing.join(", ")
                ),
            ));
        }
        let target_agents = affected_agents_from_claims(&related_claims);
        if target_agents.is_empty() {
            None
        } else {
            validate_target_agents(state.maintainer.team_root(), Some(&target_agents)).await?;
            let statement =
                build_dispute_resolve_cau_statement(&dispute, resolve_note, &related_claims);
            Some((target_agents, statement))
        }
    } else {
        None
    };

    let occurred_at = now_seconds();
    let resolved_summary = append_resolve_note_to_summary(&dispute.summary, resolve_note);
    state
        .maintainer
        .resolve_dispute(&dispute_id, Some(resolved_summary), occurred_at)
        .await
        .map_err(internal_error)?;
    if let Some((target_agents, statement)) = notification {
        let (policy_id, _pushed) = state
            .maintainer
            .claim_update_suggestion(statement, occurred_at, Some(target_agents))
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
            "写 dispute resolve claim update suggestion history",
        );
    }
    log_history_error(
        state
            .history_store
            .write_dispute_resolution_event(&DisputeResolutionEventRecord {
                event_id: fresh_record_id("dispute_resolution"),
                dispute_id,
                occurred_at,
                summary: Some(resolve_note.to_string()),
            })
            .await,
        "写 resolve dispute history",
    );
    Ok(StatusCode::NO_CONTENT)
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

fn related_claims_for_dispute(dispute: &Dispute, all_claims: &[(AgentId, Claim)]) -> Vec<Claim> {
    let mut out = Vec::with_capacity(dispute.claims.len());
    for claim_id in &dispute.claims {
        if let Some((_, claim)) = all_claims.iter().find(|(_, claim)| &claim.id == claim_id) {
            out.push(claim.clone());
        }
    }
    out
}

fn affected_agents_from_claims(claims: &[Claim]) -> Vec<AgentId> {
    let mut agents: Vec<AgentId> = claims.iter().map(|claim| claim.holder.clone()).collect();
    agents.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    agents.dedup();
    agents
}

fn append_resolve_note_to_summary(summary: &str, resolve_note: &str) -> String {
    let summary = summary.trim_end();
    if summary.is_empty() {
        format!("**来自团队 Maintainer 的 resolve 结论：**{resolve_note}")
    } else {
        format!("{summary}\n\n**来自团队 Maintainer 的 resolve 结论：**{resolve_note}")
    }
}

fn build_dispute_resolve_cau_statement(
    dispute: &Dispute,
    resolve_note: &str,
    related_claims: &[Claim],
) -> String {
    let mut lines = vec![
        format!("Maintainer 已 resolve dispute {}。", dispute.id),
        String::new(),
        "Resolve Note:".to_string(),
        resolve_note.to_string(),
        String::new(),
        "该 dispute 涉及以下 claim:".to_string(),
    ];
    for claim in related_claims {
        lines.push(format!(
            "- {} holder={} name={} scope={} statement={}",
            claim.id,
            claim.holder,
            one_line(&claim.name),
            one_line(&claim.scope),
            one_line(&claim.statement)
        ));
    }
    lines.extend([
        String::new(),
        "原Dispute 内容:".to_string(),
        dispute.summary.clone(),
        String::new(),
        "请检查你本地是否持有上述 claim。如果你认为本次 resolve 结论影响了你的本地 claim，请在 'updated_claims' 中更新该 claim 的相应字段，使其反映 maintainer 的结论；如果你的本地 claim 不受影响，请跳过。不要仅因为收到本消息就生成新 claim；只有当 resolve 结论形成了新的、可复用的团队知识时才生成新 claim。".to_string(),
    ]);
    lines.join("\n")
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
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

    use crate::claim::{Confidence, PolicyMessageType};
    use crate::config::MaintainerAdminAuthConfig;
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
        assert!(stored
            .summary
            .contains("**来自团队 Maintainer 的 resolve 结论：**resolved"));

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
        assert!(policy.statement.contains("Resolve Note:\nscope differs"));
        assert!(policy
            .statement
            .contains("原Dispute 内容:\noriginal dispute"));
        assert!(policy.statement.contains(claim_a.id.as_str()));
        assert!(policy.statement.contains(claim_b.id.as_str()));

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
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(already_resolved.0, StatusCode::CONFLICT);
    }
}
