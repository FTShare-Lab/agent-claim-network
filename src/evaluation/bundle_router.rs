//! 评测 attempt 的冻结 claim bundle router。
//!
//! 它实现正常 `RouterClient` 查询契约，但只读取启动时加载的一份 bundle，
//! 不访问远程 router 或运行期 team store，确保 B_claim 的可见知识可复现。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::claim::{Claim, ClaimStatus};
use crate::router::{
    lexical::query_match_score, AgentQuery, CandidateClaim, RetrievalDocument, RouterClient,
    RouterQueryResult, ScopeOverviewItem, ScopesOverviewSnapshot,
};

use super::EVALUATION_SCHEMA_VERSION;

/// 冻结 bundle 在单个 attempt 内的唯一交付方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrozenClaimDeliveryPolicy {
    /// 仅供通用单元测试和非正式调用；正式评测不会使用。
    Unrestricted,
    /// A/B_empty 不允许通过 router 获得 claim。
    Disabled,
    /// B_claim 可读取一次 system overview，并执行一次 query。
    OnDemandOnce,
    /// B_forced_claim 由 harness 一次性交付完整 bundle，模型侧查询被禁用。
    ForcedOnce,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenClaimBundle {
    pub schema_version: u32,
    #[serde(default)]
    pub claims: Vec<Claim>,
}

/// 只读的本地 bundle 查询实现。
#[derive(Debug)]
pub struct FrozenClaimBundleRouter {
    claims: Vec<Claim>,
    overview: ScopesOverviewSnapshot,
    attempt_id: String,
    bundle_hash: Option<String>,
    evidence: Mutex<Vec<RouterEvidence>>,
    evidence_sequence: AtomicU64,
    evidence_audit_incomplete: AtomicBool,
    delivery_policy: FrozenClaimDeliveryPolicy,
    overview_delivered: AtomicBool,
    delivery_consumed: AtomicBool,
}

impl FrozenClaimBundleRouter {
    pub fn new(
        bundle: FrozenClaimBundle,
        attempt_id: String,
        bundle_hash: Option<String>,
    ) -> anyhow::Result<Self> {
        if bundle.schema_version != EVALUATION_SCHEMA_VERSION {
            anyhow::bail!(
                "frozen claim bundle schema_version 不支持: expected={} actual={}",
                EVALUATION_SCHEMA_VERSION,
                bundle.schema_version
            );
        }
        if let Some(claim) = bundle
            .claims
            .iter()
            .find(|claim| claim.status != ClaimStatus::Active)
        {
            anyhow::bail!(
                "frozen claim bundle 只允许 active claims: claim_id={} status={:?}",
                claim.id,
                claim.status
            );
        }
        if attempt_id.trim().is_empty() {
            anyhow::bail!("frozen claim bundle attempt_id 不能为空");
        }
        if let Some(bundle_hash) = &bundle_hash {
            if !is_sha256_hex(bundle_hash) {
                anyhow::bail!("frozen claim bundle hash 必须是 64 位小写 hex");
            }
        }
        let overview = build_overview(&bundle.claims);
        Ok(Self {
            claims: bundle.claims,
            overview,
            attempt_id,
            bundle_hash,
            evidence: Mutex::new(Vec::new()),
            evidence_sequence: AtomicU64::new(0),
            evidence_audit_incomplete: AtomicBool::new(false),
            delivery_policy: FrozenClaimDeliveryPolicy::Unrestricted,
            overview_delivered: AtomicBool::new(false),
            delivery_consumed: AtomicBool::new(false),
        })
    }

    pub fn with_delivery_policy(mut self, policy: FrozenClaimDeliveryPolicy) -> Self {
        self.delivery_policy = policy;
        self
    }

    /// B_forced_claim 的唯一交付入口：一次返回完整冻结 bundle，同时写入一条 router evidence。
    pub fn deliver_forced_claims_once(
        &self,
        task_prompt: &str,
    ) -> anyhow::Result<Vec<CandidateClaim>> {
        if self.delivery_policy != FrozenClaimDeliveryPolicy::ForcedOnce {
            anyhow::bail!("stage=router 当前 attempt 不允许 forced claim 交付");
        }
        self.consume_delivery("forced claim bundle 已交付，拒绝重复交付")?;
        let candidates = self
            .claims
            .iter()
            .cloned()
            .map(|claim| CandidateClaim {
                claim,
                open_dispute_ids: Vec::new(),
                resolved_dispute_ids: Vec::new(),
            })
            .collect::<Vec<_>>();
        let query = AgentQuery::from_task("evaluation/forced_claims", task_prompt);
        self.record_evidence(&query, &candidates)?;
        Ok(candidates)
    }

    pub fn take_evidence(&self) -> Vec<RouterEvidence> {
        match self.evidence.lock() {
            Ok(mut evidence) => std::mem::take(&mut *evidence),
            Err(poisoned) => {
                self.evidence_audit_incomplete
                    .store(true, Ordering::Release);
                std::mem::take(&mut *poisoned.into_inner())
            }
        }
    }

    pub fn audit_is_incomplete(&self) -> bool {
        self.evidence_audit_incomplete.load(Ordering::Acquire)
    }

    fn consume_delivery(&self, duplicate_message: &str) -> anyhow::Result<()> {
        self.delivery_consumed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| anyhow::anyhow!(duplicate_message.to_owned()))?;
        Ok(())
    }

    fn record_evidence(
        &self,
        agent_query: &AgentQuery,
        candidate_claims: &[CandidateClaim],
    ) -> anyhow::Result<()> {
        let candidate_claim_ids = candidate_claims
            .iter()
            .map(|candidate| candidate.claim.id.to_string())
            .collect::<Vec<_>>();
        let injected_content_hashes = candidate_claims
            .iter()
            .map(|candidate| claim_content_hash(&candidate.claim))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let query_hash =
            sha256_hex(&serde_json::to_vec(agent_query).map_err(|error| anyhow::anyhow!(error))?);
        let sequence = self
            .evidence_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                anyhow::anyhow!(
                    "stage=router query evidence sequence 已耗尽: attempt_id={} scope={}",
                    self.attempt_id,
                    agent_query.scope
                )
            })?
            + 1;
        let evidence = RouterEvidence {
            schema_version: EVALUATION_SCHEMA_VERSION,
            evidence_id: format!("router-{sequence:08x}"),
            attempt_id: self.attempt_id.clone(),
            bundle_hash: self.bundle_hash.clone(),
            query_hash,
            candidate_claim_ids: candidate_claim_ids.clone(),
            selected_claim_ids: candidate_claim_ids.clone(),
            injected_claim_ids: candidate_claim_ids,
            injected_content_hashes,
            timestamp_utc: Utc::now(),
        };
        match self.evidence.lock() {
            Ok(mut records) => records.push(evidence),
            Err(_) => {
                self.evidence_audit_incomplete
                    .store(true, Ordering::Release);
                anyhow::bail!(
                    "stage=router query evidence mutex 已损坏: attempt_id={} scope={}",
                    self.attempt_id,
                    agent_query.scope
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RouterClient for FrozenClaimBundleRouter {
    async fn query(&self, agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        match self.delivery_policy {
            FrozenClaimDeliveryPolicy::Disabled => {
                anyhow::bail!("stage=router 当前 attempt 禁止 claim 查询")
            }
            FrozenClaimDeliveryPolicy::ForcedOnce => {
                anyhow::bail!("stage=router frozen claims 已由 harness 完整交付，禁止重复查询")
            }
            FrozenClaimDeliveryPolicy::OnDemandOnce => {
                self.consume_delivery("stage=router B_claim 只允许一次 query，拒绝重复查询")?;
            }
            FrozenClaimDeliveryPolicy::Unrestricted => {}
        }
        let mut candidate_claims = self
            .claims
            .iter()
            .cloned()
            .filter_map(|claim| {
                let document = RetrievalDocument::from_claim(&claim, Vec::new(), Vec::new());
                let score = query_match_score(agent_query, &document, claim.status)?;
                Some((
                    score,
                    CandidateClaim {
                        claim,
                        open_dispute_ids: Vec::new(),
                        resolved_dispute_ids: Vec::new(),
                    },
                ))
            })
            .collect::<Vec<_>>();
        candidate_claims.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| {
                    right
                        .1
                        .claim
                        .effective_updated_at()
                        .cmp(&left.1.claim.effective_updated_at())
                })
                .then_with(|| left.1.claim.id.as_str().cmp(right.1.claim.id.as_str()))
        });
        let candidate_claims = candidate_claims
            .into_iter()
            .map(|(_, candidate)| candidate)
            .collect::<Vec<_>>();
        self.record_evidence(agent_query, &candidate_claims)?;
        Ok(RouterQueryResult {
            candidate_claims,
            disputes: Vec::new(),
            retrieval_debug: None,
        })
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        let visible = match self.delivery_policy {
            FrozenClaimDeliveryPolicy::Disabled => false,
            FrozenClaimDeliveryPolicy::ForcedOnce => {
                !self.delivery_consumed.load(Ordering::Acquire)
            }
            FrozenClaimDeliveryPolicy::OnDemandOnce => {
                !self.overview_delivered.swap(true, Ordering::AcqRel)
            }
            FrozenClaimDeliveryPolicy::Unrestricted => true,
        };
        Ok(if visible {
            self.overview.clone()
        } else {
            ScopesOverviewSnapshot { scopes: Vec::new() }
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RouterEvidence {
    pub schema_version: u32,
    pub evidence_id: String,
    pub attempt_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_hash: Option<String>,
    pub query_hash: String,
    pub candidate_claim_ids: Vec<String>,
    pub selected_claim_ids: Vec<String>,
    pub injected_claim_ids: Vec<String>,
    pub injected_content_hashes: Vec<String>,
    pub timestamp_utc: DateTime<Utc>,
}

fn claim_content_hash(claim: &Claim) -> anyhow::Result<String> {
    let value = serde_json::to_value(claim).map_err(|error| anyhow::anyhow!(error))?;
    let canonical = serde_json::to_string(&value)?;
    Ok(sha256_hex(canonical.as_bytes()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(ring::digest::digest(&ring::digest::SHA256, bytes).as_ref())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn build_overview(claims: &[Claim]) -> ScopesOverviewSnapshot {
    let mut rows = std::collections::BTreeMap::<String, ScopeCounts>::new();
    for claim in claims {
        let row = match rows.entry(claim.scope.clone()) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => entry.insert(ScopeCounts {
                active_claims: 0,
                latest_claim_created_at: claim.created_at,
            }),
        };
        row.active_claims += 1;
        row.latest_claim_created_at = row.latest_claim_created_at.max(claim.created_at);
    }
    ScopesOverviewSnapshot {
        scopes: rows
            .into_iter()
            .map(|(scope, counts)| ScopeOverviewItem {
                scope,
                active_claims: counts.active_claims,
                stale_claims: 0,
                open_disputes: 0,
                resolved_disputes: 0,
                latest_claim_created_at: counts.latest_claim_created_at,
            })
            .collect(),
    }
}

struct ScopeCounts {
    active_claims: usize,
    latest_claim_created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{AgentId, ClaimId, Confidence};
    use crate::router::RouterClient;

    fn sample_claim(id: &str, name: &str, statement: &str) -> Claim {
        Claim {
            id: id.parse::<ClaimId>().unwrap(),
            name: name.into(),
            statement: statement.into(),
            scope: "billing/payment".into(),
            holder: AgentId::new("eval_test").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-07-26T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "fixture".into(),
        }
    }

    #[test]
    fn claim_content_hash_uses_python_compatible_canonical_json_golden() {
        let claim = Claim {
            id: "claim_1234abcd".parse::<ClaimId>().unwrap(),
            name: "golden".into(),
            statement: "full claim content".into(),
            scope: "scope".into(),
            holder: AgentId::new("eval_test").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-07-26T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "fixture".into(),
        };
        let canonical = serde_json::to_string(&serde_json::to_value(&claim).unwrap()).unwrap();

        assert_eq!(
            canonical,
            r#"{"confidence":"high","created_at":"2026-07-26T00:00:00Z","evidence_summary":"fixture","holder":"eval_test","id":"claim_1234abcd","name":"golden","scope":"scope","source_claim_ids":[],"statement":"full claim content","status":"active"}"#
        );
        assert_eq!(
            claim_content_hash(&claim).unwrap(),
            "948c01be082f4f10013fd8d379fb53c364d13bfab7e00f2636c6e673c8bbf9d4"
        );
    }

    #[tokio::test]
    async fn semantic_query_recalls_and_ranks_claim_outside_scope() {
        let matching = sample_claim(
            "claim_11111111",
            "payment timeout",
            "connection pool exhaustion causes payment timeout",
        );
        let mut other = sample_claim(
            "claim_22222222",
            "payment timeout incident",
            "payment timeout was caused by a stale connection pool",
        );
        other.scope = "operations/incidents".into();
        let router = FrozenClaimBundleRouter::new(
            FrozenClaimBundle {
                schema_version: EVALUATION_SCHEMA_VERSION,
                claims: vec![other.clone(), matching.clone()],
            },
            "attempt-001".into(),
            None,
        )
        .unwrap();

        let result = router
            .query(&AgentQuery::from_task("billing/payment", "payment timeout"))
            .await
            .unwrap();

        assert_eq!(result.candidate_claims.len(), 2);
        assert_eq!(result.candidate_claims[0].claim, matching);
        assert_eq!(result.candidate_claims[1].claim, other);
        let evidence = router.take_evidence();
        assert_eq!(evidence[0].bundle_hash, None);
    }

    #[tokio::test]
    async fn on_demand_policy_exposes_overview_and_query_only_once() {
        let claim = sample_claim(
            "claim_11111111",
            "payment timeout",
            "connection pool exhaustion causes payment timeout",
        );
        let router = FrozenClaimBundleRouter::new(
            FrozenClaimBundle {
                schema_version: EVALUATION_SCHEMA_VERSION,
                claims: vec![claim],
            },
            "attempt-001".into(),
            None,
        )
        .unwrap()
        .with_delivery_policy(FrozenClaimDeliveryPolicy::OnDemandOnce);

        assert_eq!(router.scopes_overview().await.unwrap().scopes.len(), 1);
        assert!(router.scopes_overview().await.unwrap().scopes.is_empty());
        let query = AgentQuery::from_task("billing/payment", "payment timeout");
        assert_eq!(
            router.query(&query).await.unwrap().candidate_claims.len(),
            1
        );
        assert!(router
            .query(&query)
            .await
            .unwrap_err()
            .to_string()
            .contains("只允许一次"));
        assert_eq!(router.take_evidence().len(), 1);
    }

    #[tokio::test]
    async fn forced_policy_delivers_complete_bundle_once_and_hides_router_afterwards() {
        let first = sample_claim("claim_11111111", "first", "first fact");
        let mut second = sample_claim("claim_22222222", "second", "unrelated fact");
        second.scope = "operations/incidents".into();
        let router = FrozenClaimBundleRouter::new(
            FrozenClaimBundle {
                schema_version: EVALUATION_SCHEMA_VERSION,
                claims: vec![first, second],
            },
            "attempt-001".into(),
            None,
        )
        .unwrap()
        .with_delivery_policy(FrozenClaimDeliveryPolicy::ForcedOnce);

        let delivered = router
            .deliver_forced_claims_once("fix payment timeout")
            .unwrap();

        assert_eq!(delivered.len(), 2);
        assert!(router.scopes_overview().await.unwrap().scopes.is_empty());
        assert!(router
            .query(&AgentQuery::from_task("billing/payment", "payment timeout"))
            .await
            .unwrap_err()
            .to_string()
            .contains("完整交付"));
        assert!(router
            .deliver_forced_claims_once("fix payment timeout")
            .unwrap_err()
            .to_string()
            .contains("拒绝重复交付"));
        let evidence = router.take_evidence();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].injected_claim_ids.len(), 2);
    }
}
