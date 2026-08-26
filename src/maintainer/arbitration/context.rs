//! 仲裁冻结上下文、稳定排序与双哈希构建。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::fs;

use crate::claim::{
    AgentId, Claim, ClaimAssessment, ClaimId, Confidence, DisputeId, Policy, PolicyMessageType,
    PolicyStatus, ResolutionBasis, ResolutionType, ResolvedBy, SourceId,
};
use crate::config::{LlmChatConfig, LlmProvider, MaintainerArbitrationConfig, ReasoningEffort};
use crate::router::{AgentQuery, CandidateClaim, DisputeRef, RouterClient};
use crate::storage::{paths, read_yaml};

use super::store::{versioned_sha256, ArbitrationStore};
use super::types::{
    ArbitrationRouterCandidate, ContextWarning, FrozenArbitrationContext, PriorResolutionContext,
    ARBITRATION_PROMPT_VERSION, CURRENT_SEMANTIC_PROJECTION_VERSION,
};

#[derive(Debug, thiserror::Error)]
#[error("直接 Claim mirror 尚未准备完整: {detail}")]
pub struct ContextNotReadyError {
    detail: String,
}

pub fn is_context_not_ready(error: &anyhow::Error) -> bool {
    error.downcast_ref::<ContextNotReadyError>().is_some()
}

#[derive(Debug, Clone)]
pub struct BuiltArbitrationContext {
    pub frozen: FrozenArbitrationContext,
    pub semantic_fingerprint: String,
    pub context_snapshot_hash: String,
}

pub struct ArbitrationContextBuilder {
    store: ArbitrationStore,
    router: Arc<dyn RouterClient>,
    arbitration: MaintainerArbitrationConfig,
    llm: LlmChatConfig,
}

impl ArbitrationContextBuilder {
    pub fn new(
        store: ArbitrationStore,
        router: Arc<dyn RouterClient>,
        arbitration: MaintainerArbitrationConfig,
        llm: LlmChatConfig,
    ) -> Self {
        Self {
            store,
            router,
            arbitration,
            llm,
        }
    }

    pub async fn build(
        &self,
        dispute_id: &DisputeId,
        generated_at: DateTime<Utc>,
    ) -> anyhow::Result<BuiltArbitrationContext> {
        let record = self.store.read_dispute(dispute_id).await?;
        let all_claims = load_team_claims(self.store.team_root()).await?;
        let mut warnings = Vec::new();
        let direct_claims =
            resolve_direct_claims(&record.dispute.claims, &all_claims).map_err(|error| {
                ContextNotReadyError {
                    detail: error.to_string(),
                }
            })?;
        let source_claims = load_source_graph(
            &direct_claims,
            &all_claims,
            self.arbitration.max_source_claims,
            &mut warnings,
        );
        let policies = load_governance_policies(self.store.team_root()).await?;
        let (router_candidate_claims, router_disputes) = self
            .load_router_evidence(&record.dispute, &direct_claims, &mut warnings)
            .await;
        let prior_resolutions = self
            .load_prior_resolutions(&record.dispute.claims, dispute_id)
            .await?;

        let frozen = FrozenArbitrationContext {
            generated_at,
            dispute: record.dispute,
            direct_claims,
            source_claims,
            policies,
            router_candidate_claims: router_candidate_claims
                .into_iter()
                .map(|candidate| ArbitrationRouterCandidate {
                    claim: candidate.claim,
                })
                .collect(),
            router_disputes,
            prior_resolutions,
            warnings,
        };
        let semantic = SemanticInputV5::from_frozen(&frozen, &self.arbitration, &self.llm);
        let semantic_fingerprint = versioned_sha256(&semantic)?;
        let context_snapshot_hash = versioned_sha256(&frozen)?;
        Ok(BuiltArbitrationContext {
            frozen,
            semantic_fingerprint,
            context_snapshot_hash,
        })
    }

    pub fn describe_changes(
        &self,
        previous: Option<&FrozenArbitrationContext>,
        current: &FrozenArbitrationContext,
    ) -> anyhow::Result<String> {
        let Some(previous) = previous else {
            return Ok("上一轮冻结上下文不可用".into());
        };
        let mut changed = Vec::new();
        let previous_policies = governance_policy_ids(previous);
        let current_policies = governance_policy_ids(current);
        if semantic_dispute(&previous.dispute) != semantic_dispute(&current.dispute) {
            changed.push("目标 Dispute");
        }
        if semantic_claims(&previous.direct_claims, &previous_policies)
            != semantic_claims(&current.direct_claims, &current_policies)
        {
            changed.push("direct Claims");
        }
        if semantic_claims(&previous.source_claims, &previous_policies)
            != semantic_claims(&current.source_claims, &current_policies)
        {
            changed.push("source Claims");
        }
        if semantic_policies(&previous.policies) != semantic_policies(&current.policies) {
            changed.push("治理 Policy");
        }
        if semantic_router_candidates(&previous.router_candidate_claims, &previous_policies)
            != semantic_router_candidates(&current.router_candidate_claims, &current_policies)
        {
            changed.push("Router candidate Claims");
        }
        if semantic_router_disputes(&previous.router_disputes)
            != semantic_router_disputes(&current.router_disputes)
        {
            changed.push("Router Disputes");
        }
        if changed.is_empty() {
            changed.push("仲裁配置或上下文可用性");
        }
        Ok(changed.join("、"))
    }

    async fn load_router_evidence(
        &self,
        dispute: &crate::claim::Dispute,
        direct_claims: &[Claim],
        warnings: &mut Vec<ContextWarning>,
    ) -> (Vec<CandidateClaim>, Vec<DisputeRef>) {
        let query_text = arbitration_query_text(dispute, direct_claims);
        let scopes: BTreeSet<String> = direct_claims
            .iter()
            .map(|claim| claim.scope.clone())
            .collect();
        let mut candidates = BTreeMap::<ClaimId, CandidateClaim>::new();
        let mut disputes = BTreeMap::<DisputeId, DisputeRef>::new();
        for scope in scopes {
            let query = AgentQuery::from_task(scope.clone(), query_text.clone());
            match self.router.query(&query).await {
                Ok(result) => {
                    for candidate in result.candidate_claims {
                        let claim = candidate.claim;
                        match candidates.get(&claim.id) {
                            Some(existing) if existing.claim != claim => {
                                warnings.push(ContextWarning {
                                    code: "router_candidate_conflict".into(),
                                    detail: format!(
                                    "Router 对 claim={} 返回不一致快照，保留稳定排序后的首次结果",
                                    claim.id
                                ),
                                })
                            }
                            Some(_) => {}
                            None => {
                                candidates.insert(
                                    claim.id.clone(),
                                    CandidateClaim {
                                        claim,
                                        open_dispute_ids: Vec::new(),
                                        resolved_dispute_ids: Vec::new(),
                                    },
                                );
                            }
                        }
                    }
                    for related in result.disputes {
                        match disputes.get(&related.id) {
                            Some(existing) if existing != &related => {
                                warnings.push(ContextWarning {
                                    code: "router_dispute_conflict".into(),
                                    detail: format!(
                                    "Router 对 dispute={} 返回不一致摘要，保留稳定排序后的首次结果",
                                    related.id
                                ),
                                })
                            }
                            Some(_) => {}
                            None => {
                                disputes.insert(related.id.clone(), related);
                            }
                        }
                    }
                }
                Err(error) => {
                    log::warn!(
                        target: "maintainer_arbitration",
                        "Router 补充查询失败 scope={scope:?}: {error:#}"
                    );
                    warnings.push(ContextWarning {
                        code: "router_query_failed".into(),
                        detail: format!("scope={scope:?} 的 Router 补充查询失败"),
                    })
                }
            }
        }
        (
            candidates.into_values().collect(),
            disputes.into_values().collect(),
        )
    }

    async fn load_prior_resolutions(
        &self,
        claim_ids: &[ClaimId],
        current_dispute_id: &DisputeId,
    ) -> anyhow::Result<Vec<PriorResolutionContext>> {
        let wanted = normalized_claim_ids(claim_ids);
        let mut prior = Vec::new();
        for record in self.store.list_disputes().await? {
            if normalized_claim_ids(&record.dispute.claims) != wanted {
                continue;
            }
            if &record.dispute.id == current_dispute_id {
                continue;
            }
            if let Some(resolution) = record.resolution {
                prior.push(PriorResolutionContext {
                    dispute_id: record.dispute.id,
                    resolution,
                });
            }
        }
        prior.sort_by(|left, right| {
            left.resolution
                .resolved_at
                .cmp(&right.resolution.resolved_at)
                .then_with(|| {
                    left.resolution
                        .resolution_id
                        .as_str()
                        .cmp(right.resolution.resolution_id.as_str())
                })
        });
        Ok(prior)
    }
}

fn governance_policy_ids(context: &FrozenArbitrationContext) -> BTreeSet<&str> {
    context
        .policies
        .iter()
        .map(|policy| policy.id.as_str())
        .collect()
}

fn semantic_dispute(dispute: &crate::claim::Dispute) -> SemanticDisputeV2 {
    let mut claims = dispute
        .claims
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    claims.sort();
    claims.dedup();
    SemanticDisputeV2 {
        id: dispute.id.to_string(),
        name: dispute.name.clone(),
        reporter_agent_id: dispute.reporter_agent_id.to_string(),
        claims,
        summary: dispute.summary.clone(),
        created_at: dispute.created_at,
    }
}

#[derive(Serialize)]
struct SemanticInputV5 {
    schema_version: u32,
    prompt_version: &'static str,
    dispute: SemanticDisputeV2,
    direct_claims: Vec<SemanticClaimV2>,
    source_claims: Vec<SemanticClaimV2>,
    policies: Vec<SemanticPolicyV2>,
    router_candidate_claims: Vec<SemanticClaimV2>,
    router_disputes: Vec<SemanticRouterDisputeV3>,
    prior_resolutions: Vec<SemanticPriorResolutionV2>,
    warning_codes: Vec<String>,
    evaluator: SemanticEvaluatorConfigV2,
}

#[derive(Serialize, PartialEq, Eq)]
struct SemanticDisputeV2 {
    id: String,
    name: String,
    reporter_agent_id: String,
    claims: Vec<String>,
    summary: String,
    created_at: DateTime<Utc>,
}

#[derive(Serialize, PartialEq, Eq)]
struct SemanticClaimV2 {
    id: String,
    name: String,
    statement: String,
    scope: String,
    holder: String,
    confidence: Confidence,
    status: crate::claim::ClaimStatus,
    created_at: DateTime<Utc>,
    source_claim_ids: Vec<String>,
    evidence_summary: String,
}

#[derive(Serialize, PartialEq, Eq)]
struct SemanticPolicyV2 {
    id: String,
    message_type: PolicyMessageType,
    name: String,
    statement: String,
    scope: String,
    status: PolicyStatus,
    created_at: DateTime<Utc>,
    target_agents: Option<Vec<String>>,
}

#[derive(Serialize, PartialEq, Eq)]
struct SemanticRouterDisputeV3 {
    id: String,
    name: String,
    claim_ids: Vec<String>,
    summary: String,
    status: crate::claim::DisputeStatus,
}

#[derive(Serialize, PartialEq, Eq)]
struct SemanticPriorResolutionV2 {
    dispute_id: String,
    resolved_by: ResolvedBy,
    resolution_type: Option<ResolutionType>,
    resolution_basis: Option<ResolutionBasis>,
    conclusion: String,
    claim_assessments: Vec<ClaimAssessment>,
    rejection_reason: Option<String>,
}

#[derive(Serialize)]
struct SemanticEvaluatorConfigV2 {
    provider: LlmProvider,
    model: String,
    reasoning_effort: ReasoningEffort,
    max_tokens: u32,
    context_window: usize,
    confidence_threshold: f64,
    max_source_claims: usize,
}

impl SemanticInputV5 {
    fn from_frozen(
        frozen: &FrozenArbitrationContext,
        arbitration: &MaintainerArbitrationConfig,
        llm: &LlmChatConfig,
    ) -> Self {
        let governance_policy_ids = governance_policy_ids(frozen);
        let mut warning_codes = frozen
            .warnings
            .iter()
            .map(|warning| warning.code.clone())
            .collect::<Vec<_>>();
        warning_codes.sort();
        warning_codes.dedup();
        let mut prior_resolutions = frozen
            .prior_resolutions
            .iter()
            .map(|record| {
                let mut claim_assessments = record.resolution.claim_assessments.clone();
                claim_assessments.sort_by(|left, right| left.claim_id.cmp(&right.claim_id));
                SemanticPriorResolutionV2 {
                    dispute_id: record.dispute_id.to_string(),
                    resolved_by: record.resolution.resolved_by,
                    resolution_type: record.resolution.resolution_type,
                    resolution_basis: record.resolution.resolution_basis,
                    conclusion: record.resolution.conclusion.clone(),
                    claim_assessments,
                    rejection_reason: record.resolution.rejection_reason.clone(),
                }
            })
            .collect::<Vec<_>>();
        prior_resolutions.dedup();
        Self {
            schema_version: CURRENT_SEMANTIC_PROJECTION_VERSION,
            prompt_version: ARBITRATION_PROMPT_VERSION,
            dispute: semantic_dispute(&frozen.dispute),
            direct_claims: semantic_claims(&frozen.direct_claims, &governance_policy_ids),
            source_claims: semantic_claims(&frozen.source_claims, &governance_policy_ids),
            policies: semantic_policies(&frozen.policies),
            router_candidate_claims: semantic_router_candidates(
                &frozen.router_candidate_claims,
                &governance_policy_ids,
            ),
            router_disputes: semantic_router_disputes(&frozen.router_disputes),
            prior_resolutions,
            warning_codes,
            evaluator: SemanticEvaluatorConfigV2 {
                provider: llm.provider,
                model: llm.model.clone(),
                reasoning_effort: llm.reasoning_effort,
                max_tokens: llm.max_tokens,
                context_window: llm.context_window,
                confidence_threshold: arbitration.confidence_threshold,
                max_source_claims: arbitration.max_source_claims,
            },
        }
    }
}

fn semantic_claims(
    claims: &[Claim],
    governance_policy_ids: &BTreeSet<&str>,
) -> Vec<SemanticClaimV2> {
    let mut projected = claims
        .iter()
        .map(|claim| semantic_claim(claim, governance_policy_ids))
        .collect::<Vec<_>>();
    projected.sort_by(|left, right| left.id.cmp(&right.id));
    projected
}

fn semantic_claim(claim: &Claim, governance_policy_ids: &BTreeSet<&str>) -> SemanticClaimV2 {
    let mut source_claim_ids = claim
        .source_claim_ids
        .iter()
        .filter(|source| match source {
            SourceId::Claim(_) => true,
            SourceId::Policy(policy_id) => governance_policy_ids.contains(policy_id.as_str()),
        })
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    source_claim_ids.sort();
    source_claim_ids.dedup();
    SemanticClaimV2 {
        id: claim.id.to_string(),
        name: claim.name.clone(),
        statement: claim.statement.clone(),
        scope: claim.scope.clone(),
        holder: claim.holder.to_string(),
        confidence: claim.confidence,
        status: claim.status,
        created_at: claim.created_at,
        source_claim_ids,
        evidence_summary: claim.evidence_summary.clone(),
    }
}

fn semantic_router_candidates(
    candidates: &[ArbitrationRouterCandidate],
    governance_policy_ids: &BTreeSet<&str>,
) -> Vec<SemanticClaimV2> {
    let mut projected = candidates
        .iter()
        .map(|candidate| semantic_claim(&candidate.claim, governance_policy_ids))
        .collect::<Vec<_>>();
    projected.sort_by(|left, right| left.id.cmp(&right.id));
    projected
}

fn semantic_policies(policies: &[Policy]) -> Vec<SemanticPolicyV2> {
    let mut projected = policies
        .iter()
        .map(|policy| {
            let target_agents = policy.target_agents.as_ref().map(|targets| {
                let mut targets = targets.iter().map(ToString::to_string).collect::<Vec<_>>();
                targets.sort();
                targets.dedup();
                targets
            });
            SemanticPolicyV2 {
                id: policy.id.to_string(),
                message_type: policy.message_type,
                name: policy.name.clone(),
                statement: policy.statement.clone(),
                scope: policy.scope.clone(),
                status: policy.status,
                created_at: policy.created_at,
                target_agents,
            }
        })
        .collect::<Vec<_>>();
    projected.sort_by(|left, right| left.id.cmp(&right.id));
    projected
}

fn semantic_router_disputes(disputes: &[DisputeRef]) -> Vec<SemanticRouterDisputeV3> {
    let mut projected = disputes
        .iter()
        .map(|dispute| {
            let mut claim_ids = dispute
                .claim_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            claim_ids.sort();
            claim_ids.dedup();
            SemanticRouterDisputeV3 {
                id: dispute.id.to_string(),
                name: dispute.name.clone(),
                claim_ids,
                summary: dispute.summary.clone(),
                status: dispute.status,
            }
        })
        .collect::<Vec<_>>();
    projected.sort_by(|left, right| left.id.cmp(&right.id));
    projected
}

pub async fn load_team_claims(team_root: &Path) -> anyhow::Result<Vec<(AgentId, Claim)>> {
    let root = paths::team_store_agents_root(team_root);
    if !fs::try_exists(&root).await.unwrap_or(false) {
        return Ok(Vec::new());
    }
    let mut claims = Vec::new();
    let mut agents = fs::read_dir(&root).await?;
    while let Some(agent_entry) = agents.next_entry().await? {
        if !agent_entry.file_type().await?.is_dir() {
            continue;
        }
        let Ok(agent_name) = agent_entry.file_name().into_string() else {
            continue;
        };
        let Ok(agent_id) = AgentId::new(agent_name) else {
            continue;
        };
        let dir = paths::team_store_agent_claims_dir(team_root, &agent_id);
        if !fs::try_exists(&dir).await.unwrap_or(false) {
            continue;
        }
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.ends_with(".yaml") || name.contains(".tmp.") {
                continue;
            }
            let claim: Claim = read_yaml(&path).await?;
            claims.push((agent_id.clone(), claim));
        }
    }
    claims.sort_by(|left, right| {
        left.1
            .id
            .cmp(&right.1.id)
            .then_with(|| left.0.as_str().cmp(right.0.as_str()))
    });
    Ok(claims)
}

pub fn resolve_direct_claims(
    ids: &[ClaimId],
    all_claims: &[(AgentId, Claim)],
) -> anyhow::Result<Vec<Claim>> {
    let mut direct = Vec::with_capacity(ids.len());
    let mut seen = BTreeSet::new();
    for id in ids {
        if !seen.insert(id.clone()) {
            anyhow::bail!("direct claim={} 在 dispute 中重复", id);
        }
        let matches: Vec<&(AgentId, Claim)> = all_claims
            .iter()
            .filter(|(_, claim)| &claim.id == id)
            .collect();
        if matches.len() != 1 {
            anyhow::bail!(
                "direct claim={} 必须唯一解析，实际 mirror 数量={}",
                id,
                matches.len()
            );
        }
        let (path_agent, claim) = matches[0];
        if path_agent != &claim.holder {
            anyhow::bail!(
                "direct claim={} 的镜像目录 agent={} 与 holder={} 不一致",
                id,
                path_agent,
                claim.holder
            );
        }
        direct.push(claim.clone());
    }
    direct.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(direct)
}

fn load_source_graph(
    direct_claims: &[Claim],
    all_claims: &[(AgentId, Claim)],
    max_source_claims: usize,
    warnings: &mut Vec<ContextWarning>,
) -> Vec<Claim> {
    let mut index = BTreeMap::<ClaimId, Vec<&(AgentId, Claim)>>::new();
    for entry in all_claims {
        index.entry(entry.1.id.clone()).or_default().push(entry);
    }
    let mut seen: BTreeSet<ClaimId> = direct_claims.iter().map(|claim| claim.id.clone()).collect();
    let mut frontier = BTreeSet::new();
    for claim in direct_claims {
        for source in &claim.source_claim_ids {
            if let crate::claim::SourceId::Claim(id) = source {
                frontier.insert(id.clone());
            }
        }
    }
    let mut source_claims = Vec::new();
    'levels: while !frontier.is_empty() {
        let mut next = BTreeSet::new();
        for id in std::mem::take(&mut frontier) {
            if !seen.insert(id.clone()) {
                continue;
            }
            if source_claims.len() >= max_source_claims {
                warnings.push(ContextWarning {
                    code: "source_graph_truncated".into(),
                    detail: format!("source graph 达到 max_source_claims={max_source_claims}"),
                });
                break 'levels;
            }
            let Some(matches) = index.get(&id) else {
                warnings.push(ContextWarning {
                    code: "source_claim_missing".into(),
                    detail: format!("source claim={id} 缺少 team mirror"),
                });
                continue;
            };
            if matches.len() != 1 || matches[0].0 != matches[0].1.holder {
                warnings.push(ContextWarning {
                    code: "source_claim_ambiguous".into(),
                    detail: format!("source claim={id} 的 team mirror 不唯一或 holder 不一致"),
                });
                continue;
            }
            let claim = matches[0].1.clone();
            for source in &claim.source_claim_ids {
                if let crate::claim::SourceId::Claim(source_id) = source {
                    next.insert(source_id.clone());
                }
            }
            source_claims.push(claim);
        }
        frontier = next;
    }
    source_claims
}

async fn load_governance_policies(team_root: &Path) -> anyhow::Result<Vec<Policy>> {
    let dir = paths::team_store_policies_dir(team_root);
    if !fs::try_exists(&dir).await.unwrap_or(false) {
        return Ok(Vec::new());
    }
    let mut policies = Vec::new();
    let mut entries = fs::read_dir(&dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.ends_with(".yaml") || name.contains(".tmp.") {
            continue;
        }
        let metadata = entry.metadata().await?;
        if metadata.len() == 0 {
            continue;
        }
        let policy: Policy = read_yaml(&path).await?;
        if policy.message_type == PolicyMessageType::PolicyUpdate
            && policy.status == PolicyStatus::Active
        {
            policies.push(policy);
        }
    }
    policies.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(policies)
}

fn arbitration_query_text(dispute: &crate::claim::Dispute, claims: &[Claim]) -> String {
    let claims = claims
        .iter()
        .map(|claim| {
            format!(
                "{} | {} | {} | {}",
                claim.id, claim.name, claim.statement, claim.evidence_summary
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("{}\n{}\n{}", dispute.name, dispute.summary, claims)
}

fn normalized_claim_ids(ids: &[ClaimId]) -> Vec<ClaimId> {
    let mut ids = ids.to_vec();
    ids.sort();
    ids.dedup();
    ids
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::claim::{
        ArbitrationResolutionId, ClaimAssessment, ClaimStatus, Confidence, DisputeResolution,
        PolicyId, PolicyMessageType, PolicyStatus, SourceId,
    };
    use crate::storage::write_yaml_atomic;

    struct BulkRouter {
        candidates: Vec<CandidateClaim>,
        disputes: Vec<DisputeRef>,
        scopes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl RouterClient for BulkRouter {
        async fn query(
            &self,
            query: &AgentQuery,
        ) -> anyhow::Result<crate::router::RouterQueryResult> {
            self.scopes.lock().unwrap().push(query.scope.clone());
            Ok(crate::router::RouterQueryResult {
                candidate_claims: self.candidates.clone(),
                disputes: self.disputes.clone(),
                retrieval_debug: Some(crate::router::RetrievalDebug::default()),
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<crate::router::ScopesOverviewSnapshot> {
            Ok(crate::router::ScopesOverviewSnapshot::default())
        }
    }

    fn claim(holder: &AgentId, name: &str) -> Claim {
        Claim {
            id: ClaimId::random(),
            name: name.into(),
            statement: format!("{name} statement"),
            scope: "scope".into(),
            holder: holder.clone(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "evidence".into(),
        }
    }

    #[test]
    fn direct_claims_reject_duplicate_ids_and_ambiguous_mirrors() {
        let holder = AgentId::new("agent-a").unwrap();
        let direct = claim(&holder, "direct");
        let mirrors = vec![(holder.clone(), direct.clone())];

        assert!(
            resolve_direct_claims(&[direct.id.clone(), direct.id.clone()], &mirrors)
                .unwrap_err()
                .to_string()
                .contains("重复")
        );

        let duplicated = vec![
            (holder.clone(), direct.clone()),
            (holder.clone(), direct.clone()),
        ];
        assert!(
            resolve_direct_claims(std::slice::from_ref(&direct.id), &duplicated)
                .unwrap_err()
                .to_string()
                .contains("必须唯一解析")
        );
    }

    #[test]
    fn source_graph_keeps_bfs_depth_before_global_claim_id_order() {
        let holder = AgentId::new("agent-a").unwrap();
        let mut direct = claim(&holder, "direct");
        let mut depth_one = claim(&holder, "depth-one");
        depth_one.id = "claim_ffffffff".parse().unwrap();
        let mut depth_two = claim(&holder, "depth-two");
        depth_two.id = "claim_00000001".parse().unwrap();
        direct.source_claim_ids = vec![SourceId::Claim(depth_one.id.clone())];
        depth_one.source_claim_ids = vec![SourceId::Claim(depth_two.id.clone())];
        let mirrors = vec![
            (holder.clone(), direct.clone()),
            (holder.clone(), depth_one.clone()),
            (holder, depth_two.clone()),
        ];
        let mut warnings = Vec::new();

        let loaded = load_source_graph(&[direct], &mirrors, 20, &mut warnings);

        assert_eq!(
            loaded.iter().map(|claim| &claim.id).collect::<Vec<_>>(),
            vec![&depth_one.id, &depth_two.id]
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn semantic_v5_ignores_router_lifecycle_metadata_but_tracks_knowledge() {
        let holder = AgentId::new("agent-a").unwrap();
        let direct = claim(&holder, "direct");
        let policy = Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::PolicyUpdate,
            name: "team baseline".into(),
            statement: "use the current supported path".into(),
            scope: "scope".into(),
            status: PolicyStatus::Active,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: Some(vec![holder.clone()]),
        };
        let dispute = crate::claim::Dispute {
            id: DisputeId::random(),
            name: "semantic boundary".into(),
            reporter_agent_id: holder.clone(),
            claims: vec![direct.id.clone()],
            summary: "claims need governance".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let prior_resolution_id = ArbitrationResolutionId::random();
        let prior = PriorResolutionContext {
            dispute_id: DisputeId::random(),
            resolution: DisputeResolution {
                resolution_id: prior_resolution_id,
                resolved_by: ResolvedBy::Human,
                resolved_at: "2026-08-03T00:00:00Z".parse().unwrap(),
                resolution_type: Some(ResolutionType::ConflictResolved),
                resolution_basis: Some(ResolutionBasis::Evidence),
                conclusion: "use the supported path".into(),
                claim_assessments: vec![ClaimAssessment {
                    claim_id: direct.id.clone(),
                    recommended_status: ClaimStatus::Active,
                    assessment: "supported".into(),
                    recommended_scope: None,
                    recommended_statement: None,
                    reason: "evidence".into(),
                }],
                rejection_reason: None,
            },
        };
        let base = FrozenArbitrationContext {
            generated_at: "2026-08-04T00:00:00Z".parse().unwrap(),
            dispute,
            direct_claims: vec![direct.clone()],
            source_claims: Vec::new(),
            policies: vec![policy.clone()],
            router_candidate_claims: vec![ArbitrationRouterCandidate { claim: direct }],
            router_disputes: vec![DisputeRef {
                id: DisputeId::random(),
                name: "related".into(),
                claim_ids: Vec::new(),
                summary: "related evidence".into(),
                status: crate::claim::DisputeStatus::Open,
            }],
            prior_resolutions: vec![prior],
            warnings: vec![ContextWarning {
                code: "router_query_failed".into(),
                detail: "scope=a".into(),
            }],
        };
        let arbitration = MaintainerArbitrationConfig::default();
        let llm = LlmChatConfig::default();
        let fingerprint = |context: &FrozenArbitrationContext| {
            versioned_sha256(&SemanticInputV5::from_frozen(context, &arbitration, &llm)).unwrap()
        };

        let mut runtime_churn = base.clone();
        runtime_churn.generated_at += chrono::Duration::hours(1);
        runtime_churn.direct_claims[0].updated_at = Some(runtime_churn.generated_at);
        runtime_churn.direct_claims[0]
            .source_claim_ids
            .push(SourceId::Policy("policy_deadbeef".parse().unwrap()));
        runtime_churn.policies[0].updated_at = Some(runtime_churn.generated_at);
        runtime_churn.router_candidate_claims[0].claim.updated_at =
            Some(runtime_churn.generated_at);
        runtime_churn.warnings[0].detail = "scope=b".into();
        runtime_churn.prior_resolutions[0].resolution.resolution_id =
            ArbitrationResolutionId::random();
        runtime_churn.prior_resolutions[0].resolution.resolved_at = runtime_churn.generated_at;

        assert_eq!(fingerprint(&base), fingerprint(&runtime_churn));
        assert_ne!(
            versioned_sha256(&base).unwrap(),
            versioned_sha256(&runtime_churn).unwrap()
        );

        let mut direct_change = runtime_churn.clone();
        direct_change.direct_claims[0]
            .statement
            .push_str(" with a new constraint");
        assert_ne!(fingerprint(&base), fingerprint(&direct_change));

        let mut router_change = runtime_churn.clone();
        router_change.router_candidate_claims[0]
            .claim
            .evidence_summary
            .push_str(" and new evidence");
        assert_ne!(fingerprint(&base), fingerprint(&router_change));

        let mut policy_change = runtime_churn.clone();
        policy_change.policies[0].status = PolicyStatus::Deprecated;
        assert_ne!(fingerprint(&base), fingerprint(&policy_change));

        let mut related_dispute_lifecycle_change = runtime_churn;
        related_dispute_lifecycle_change.router_disputes[0].status =
            crate::claim::DisputeStatus::Resolved;
        assert_ne!(
            fingerprint(&base),
            fingerprint(&related_dispute_lifecycle_change)
        );
    }

    #[test]
    fn source_graph_handles_cycles_and_applies_only_source_cap() {
        let holder = AgentId::new("agent-a").unwrap();
        let mut direct = claim(&holder, "direct");
        let mut first = claim(&holder, "first");
        let mut second = claim(&holder, "second");
        direct.source_claim_ids = vec![SourceId::Claim(first.id.clone())];
        first.source_claim_ids = vec![SourceId::Claim(second.id.clone())];
        second.source_claim_ids = vec![SourceId::Claim(first.id.clone())];
        let mirrors = vec![
            (holder.clone(), direct.clone()),
            (holder.clone(), first),
            (holder, second),
        ];
        let mut warnings = Vec::new();

        let sources = load_source_graph(&[direct], &mirrors, 1, &mut warnings);

        assert_eq!(sources.len(), 1);
        assert_eq!(
            warnings
                .iter()
                .filter(|warning| warning.code == "source_graph_truncated")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn governance_context_loads_only_active_policy_updates_and_excludes_cau() {
        let root = tempfile::tempdir().unwrap();
        let dir = paths::team_store_policies_dir(root.path());
        for index in 0..25 {
            let policy = Policy {
                id: PolicyId::random(),
                message_type: PolicyMessageType::PolicyUpdate,
                name: format!("governance-{index}"),
                statement: "human governance policy".into(),
                scope: "all".into(),
                status: if index % 2 == 0 {
                    PolicyStatus::Active
                } else {
                    PolicyStatus::Deprecated
                },
                created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
                updated_at: None,
                target_agents: None,
            };
            write_yaml_atomic(&dir.join(format!("{}.yaml", policy.id)), &policy)
                .await
                .unwrap();
        }
        for index in 0..3 {
            let policy = Policy {
                id: PolicyId::random(),
                message_type: PolicyMessageType::ClaimAttributeUpdate,
                name: format!("resolution-{index}"),
                statement: "arbitration delivery".into(),
                scope: "all".into(),
                status: PolicyStatus::Active,
                created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
                updated_at: None,
                target_agents: None,
            };
            write_yaml_atomic(&dir.join(format!("{}.yaml", policy.id)), &policy)
                .await
                .unwrap();
        }

        let policies = load_governance_policies(root.path()).await.unwrap();

        assert_eq!(policies.len(), 13);
        assert!(policies.iter().all(|policy| {
            policy.message_type == PolicyMessageType::PolicyUpdate
                && policy.status == PolicyStatus::Active
        }));
        assert!(policies.windows(2).all(|pair| pair[0].id < pair[1].id));
    }

    #[tokio::test]
    async fn router_evidence_is_merged_without_maintainer_truncation_or_debug() {
        let holder = AgentId::new("agent-a").unwrap();
        let mut direct_a = claim(&holder, "direct-a");
        direct_a.scope = "scope-b".into();
        let mut direct_b = claim(&holder, "direct-b");
        direct_b.scope = "scope-a".into();
        let candidates = (0..30)
            .map(|index| CandidateClaim {
                claim: claim(&holder, &format!("candidate-{index}")),
                open_dispute_ids: Vec::new(),
                resolved_dispute_ids: Vec::new(),
            })
            .collect::<Vec<_>>();
        let router = Arc::new(BulkRouter {
            candidates,
            disputes: Vec::new(),
            scopes: Mutex::new(Vec::new()),
        });
        let root = tempfile::tempdir().unwrap();
        let builder = ArbitrationContextBuilder::new(
            ArbitrationStore::new(root.path().to_path_buf()),
            router.clone(),
            MaintainerArbitrationConfig::default(),
            LlmChatConfig::default(),
        );
        let dispute = crate::claim::Dispute {
            id: DisputeId::random(),
            name: "query".into(),
            reporter_agent_id: holder,
            claims: vec![direct_a.id.clone(), direct_b.id.clone()],
            summary: "semantic context".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let mut warnings = Vec::new();

        let (merged, related) = builder
            .load_router_evidence(&dispute, &[direct_a, direct_b], &mut warnings)
            .await;

        assert_eq!(merged.len(), 30);
        assert!(related.is_empty());
        assert!(warnings.is_empty());
        assert_eq!(
            router.scopes.lock().unwrap().as_slice(),
            &["scope-a".to_string(), "scope-b".to_string()]
        );
    }

    #[tokio::test]
    async fn router_candidate_lifecycle_metadata_is_removed_before_freezing() {
        let holder = AgentId::new("agent-a").unwrap();
        let direct = claim(&holder, "direct");
        let candidate_claim = claim(&holder, "candidate");
        let related = DisputeRef {
            id: DisputeId::random(),
            name: "related".into(),
            claim_ids: vec![candidate_claim.id.clone()],
            summary: "related knowledge".into(),
            status: crate::claim::DisputeStatus::Open,
        };
        let router = Arc::new(BulkRouter {
            candidates: vec![CandidateClaim {
                claim: candidate_claim.clone(),
                open_dispute_ids: vec![related.id.clone()],
                resolved_dispute_ids: vec![DisputeId::random()],
            }],
            disputes: vec![related.clone()],
            scopes: Mutex::new(Vec::new()),
        });
        let root = tempfile::tempdir().unwrap();
        let builder = ArbitrationContextBuilder::new(
            ArbitrationStore::new(root.path().to_path_buf()),
            router,
            MaintainerArbitrationConfig::default(),
            LlmChatConfig::default(),
        );
        let dispute = crate::claim::Dispute {
            id: DisputeId::random(),
            name: "query".into(),
            reporter_agent_id: holder,
            claims: vec![direct.id.clone()],
            summary: "semantic context".into(),
            status: crate::claim::DisputeStatus::Open,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let mut warnings = Vec::new();

        let (candidates, disputes) = builder
            .load_router_evidence(&dispute, &[direct], &mut warnings)
            .await;

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].claim, candidate_claim);
        assert!(candidates[0].open_dispute_ids.is_empty());
        assert!(candidates[0].resolved_dispute_ids.is_empty());
        assert_eq!(disputes, vec![related]);
    }
}
