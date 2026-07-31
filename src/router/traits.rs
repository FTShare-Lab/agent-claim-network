//! `RouterClient` trait + 响应类型。
//!
//! 重要协议约束：
//! - `query` 响应直接附带候选 claim **完整内容**，不暴露 claim 文件路径
//! - 没有 `fetch_claim_by_id` 接口；任何"按 id 单独取内容"的请求都不允许
//! - dispute 仅返回轻量 `DisputeRef`（id/name/claims/summary/status），不内嵌完整文件

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::claim::{Claim, ClaimId, Dispute, DisputeId, DisputeStatus};
use crate::router::{RetrievalDebug, ScopesOverviewSnapshot};

/// Agent 发起的检索意图。
///
/// `scope` 作为主召回边界，`semantic_query` 承载当前任务的语义查询文本。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentQuery {
    /// 当前任务的 scope，作为主召回边界。
    pub scope: String,
    /// 可选任务原文。router 可用它对 claim 的 name/statement/evidence 做轻量补充排序。
    #[serde(default)]
    pub semantic_query: Option<String>,
}

impl AgentQuery {
    pub fn from_scope(scope: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
            semantic_query: None,
        }
    }

    pub fn from_task(scope: impl Into<String>, semantic_query: impl Into<String>) -> Self {
        Self {
            scope: scope.into(),
            semantic_query: Some(semantic_query.into()),
        }
    }
}

#[async_trait]
pub trait RouterClient: Send + Sync {
    /// 按 agent query 查询候选 claim 与相关 dispute。
    /// 使用 `AgentQuery.scope` 做 scope 相关性召回。
    async fn query(&self, agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult>;

    /// 读取 router scope 聚合快照，供 session system prompt 冻结注入。
    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot>;
}

pub struct TimeoutRouterClient {
    inner: Arc<dyn RouterClient>,
    timeout: Duration,
}

impl TimeoutRouterClient {
    pub fn new(inner: Arc<dyn RouterClient>, timeout: Duration) -> Self {
        Self { inner, timeout }
    }
}

#[async_trait]
impl RouterClient for TimeoutRouterClient {
    async fn query(&self, agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
        tokio::time::timeout(self.timeout, self.inner.query(agent_query))
            .await
            .map_err(|_| anyhow::anyhow!("router.query 超时（timeout={:?}）", self.timeout))?
    }

    async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
        tokio::time::timeout(self.timeout, self.inner.scopes_overview())
            .await
            .map_err(|_| {
                anyhow::anyhow!("router.scopes_overview 超时（timeout={:?}）", self.timeout)
            })?
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouterQueryResult {
    pub candidate_claims: Vec<CandidateClaim>,
    pub disputes: Vec<DisputeRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retrieval_debug: Option<RetrievalDebug>,
}

/// 候选 claim：claim 本体 + dispute 关联（关联字段不入 claim 文件本体，仅查询时附带）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateClaim {
    #[serde(flatten)]
    pub claim: Claim,
    #[serde(default)]
    pub open_dispute_ids: Vec<DisputeId>,
    #[serde(default)]
    pub resolved_dispute_ids: Vec<DisputeId>,
}

/// dispute 轻量摘要
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisputeRef {
    pub id: DisputeId,
    pub name: String,
    pub claim_ids: Vec<ClaimId>,
    pub summary: String,
    pub status: DisputeStatus,
}

impl DisputeRef {
    pub fn from_dispute(d: &Dispute) -> Self {
        Self {
            id: d.id.clone(),
            name: d.name.clone(),
            claim_ids: d.claims.clone(),
            summary: d.summary.clone(),
            status: d.status,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{AgentId, ClaimStatus, Confidence};

    fn sample_claim() -> Claim {
        Claim {
            id: ClaimId::random(),
            name: "n".into(),
            statement: "s".into(),
            scope: "scope".into(),
            holder: AgentId::new("agent-b").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-04-10T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "e".into(),
        }
    }

    #[test]
    fn router_query_result_round_trip_keeps_retrieval_debug() {
        let r = RouterQueryResult {
            candidate_claims: vec![CandidateClaim {
                claim: sample_claim(),
                open_dispute_ids: vec![DisputeId::random()],
                resolved_dispute_ids: vec![],
            }],
            disputes: vec![DisputeRef {
                id: DisputeId::random(),
                name: "d1".into(),
                claim_ids: vec![ClaimId::random()],
                summary: "x".into(),
                status: DisputeStatus::Open,
            }],
            retrieval_debug: Some(crate::router::RetrievalDebug {
                mode: "lexical_only".into(),
                lexical_hits: 1,
                candidates: vec![crate::router::ClaimRetrievalDebug {
                    claim_id: sample_claim().id.into_string(),
                    hit_sources: "lexical".into(),
                    lexical_score: 10,
                    vector_score: 0,
                    rank_before_rerank: 1,
                    rank_after_rerank: 1,
                    vector_status: "ready".into(),
                }],
                ..Default::default()
            }),
        };
        let yaml = serde_yaml_ng::to_string(&r).unwrap();
        let back: RouterQueryResult = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn dispute_ref_from_dispute_keeps_fields() {
        let d = Dispute {
            id: DisputeId::random(),
            name: "n".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "summary".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-10T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let r = DisputeRef::from_dispute(&d);
        assert_eq!(r.id, d.id);
        assert_eq!(r.claim_ids, d.claims);
        assert_eq!(r.summary, d.summary);
        assert_eq!(r.status, d.status);
    }

    /// 慢 router：每次 query 阻塞 `delay` 后再返回空结果，用于触发 TimeoutRouterClient 的超时分支
    struct SlowRouter {
        delay: Duration,
    }

    #[async_trait]
    impl RouterClient for SlowRouter {
        async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            tokio::time::sleep(self.delay).await;
            Ok(RouterQueryResult {
                candidate_claims: vec![],
                disputes: vec![],
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            tokio::time::sleep(self.delay).await;
            Ok(ScopesOverviewSnapshot::default())
        }
    }

    /// 验证 inner.query 超时 → TimeoutRouterClient 返回带 timeout 字样的错误
    #[tokio::test]
    async fn timeout_router_returns_timeout_error_when_inner_too_slow() {
        let inner: Arc<dyn RouterClient> = Arc::new(SlowRouter {
            delay: Duration::from_millis(200),
        });
        let client = TimeoutRouterClient::new(inner, Duration::from_millis(20));
        let err = client
            .query(&AgentQuery::from_scope("any"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("超时"), "实际错误: {err}");
    }

    /// 反向用例：inner 在 timeout 之前完成 → 正常返回
    #[tokio::test]
    async fn timeout_router_passes_through_when_inner_fast_enough() {
        let inner: Arc<dyn RouterClient> = Arc::new(SlowRouter {
            delay: Duration::from_millis(5),
        });
        let client = TimeoutRouterClient::new(inner, Duration::from_millis(500));
        let r = client.query(&AgentQuery::from_scope("any")).await.unwrap();
        assert!(r.candidate_claims.is_empty());
        assert!(r.disputes.is_empty());
    }
}
