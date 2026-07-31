//! router lexical retrieval document。
//!
//! 该模块负责把 claim 与 dispute 关联派生成查询期可直接读取的检索文档。

use serde::{Deserialize, Serialize};

use crate::claim::{Claim, ClaimId, DisputeId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrievalDocument {
    pub claim_id: ClaimId,
    pub scope_text: String,
    pub search_text: String,
    #[serde(default)]
    pub open_dispute_ids: Vec<DisputeId>,
    #[serde(default)]
    pub resolved_dispute_ids: Vec<DisputeId>,
}

impl RetrievalDocument {
    /// 从 claim 与当前 dispute 关联派生 lexical 检索文档。
    pub fn from_claim(claim: &Claim, open: Vec<DisputeId>, resolved: Vec<DisputeId>) -> Self {
        let search_text = format!(
            "{}\n{}\n{}\n{}",
            claim.name, claim.statement, claim.scope, claim.evidence_summary
        );
        Self {
            claim_id: claim.id.clone(),
            scope_text: claim.scope.clone(),
            search_text,
            open_dispute_ids: open,
            resolved_dispute_ids: resolved,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::claim::{AgentId, Claim, ClaimId, ClaimStatus, Confidence};

    #[tokio::test]
    async fn rebuild_retrieval_doc_creates_search_text_for_claim() {
        let claim = Claim {
            id: ClaimId::random(),
            name: "payment_timeout_root_cause".into(),
            statement: "payment timeout is caused by connection pool exhaustion".into(),
            scope: "order-system / payment-service / prod".into(),
            holder: AgentId::new("agent-b").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "timeout logs point to pool exhaustion".into(),
        };

        let doc = super::RetrievalDocument::from_claim(&claim, vec![], vec![]);
        assert!(doc.search_text.contains(&claim.statement));
        assert!(doc.search_text.contains(&claim.scope));
    }

    #[test]
    fn retrieval_doc_yaml_round_trip_keeps_dispute_ids() {
        let claim = Claim {
            id: ClaimId::random(),
            name: "payment_timeout_root_cause".into(),
            statement: "payment timeout is caused by connection pool exhaustion".into(),
            scope: "order-system / payment-service / prod".into(),
            holder: AgentId::new("agent-b").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "timeout logs point to pool exhaustion".into(),
        };
        let open_id = crate::claim::DisputeId::random();
        let resolved_id = crate::claim::DisputeId::random();

        let doc = super::RetrievalDocument::from_claim(
            &claim,
            vec![open_id.clone()],
            vec![resolved_id.clone()],
        );
        let yaml = serde_yaml_ng::to_string(&doc).unwrap();
        let back: super::RetrievalDocument = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(back.claim_id, claim.id);
        assert_eq!(back.open_dispute_ids, vec![open_id]);
        assert_eq!(back.resolved_dispute_ids, vec![resolved_id]);
    }
}
