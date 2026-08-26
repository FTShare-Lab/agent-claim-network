//! Maintainer 仲裁与 holder inbox 共享的结构化协议。
//!
//! 这里只定义跨模块传输与 Dispute 当前 resolution 所需的稳定类型；Analysis lease、
//! 恢复状态与 provider 观测仍属于 Maintainer 私有实现。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ArbitrationResolutionId, Claim, ClaimId, ClaimStatus, Dispute, DisputeId};
use crate::time::serde_utc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionType {
    Coexist,
    LifecycleUpdate,
    ConflictResolved,
    Unresolved,
}

impl ResolutionType {
    pub const fn is_resolved(self) -> bool {
        !matches!(self, Self::Unresolved)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionBasis {
    DirectAnalysis,
    PriorResolution,
    Policy,
    Evidence,
    InsufficientEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedBy {
    Automatic,
    Human,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimAssessment {
    pub claim_id: ClaimId,
    pub recommended_status: ClaimStatus,
    pub assessment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_statement: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisputeResolution {
    #[serde(alias = "decision_id")]
    pub resolution_id: ArbitrationResolutionId,
    #[serde(alias = "decided_by")]
    pub resolved_by: ResolvedBy,
    #[serde(with = "serde_utc", alias = "decided_at")]
    pub resolved_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_type: Option<ResolutionType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_basis: Option<ResolutionBasis>,
    pub conclusion: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claim_assessments: Vec<ClaimAssessment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArbitrationResolutionContext {
    pub dispute_id: DisputeId,
    #[serde(flatten)]
    pub resolution: DisputeResolution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_snapshot_hash: Option<String>,
    pub dispute_snapshot: Dispute,
    pub direct_claim_snapshots: Vec<Claim>,
    #[serde(
        default,
        alias = "snapshot_source_decision_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub snapshot_source_resolution_id: Option<ArbitrationResolutionId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{AgentId, Confidence, DisputeStatus};

    #[test]
    fn resolution_context_round_trip() {
        let claim = Claim {
            id: ClaimId::random(),
            name: "current_timeout".into(),
            statement: "current timeout is 60s".into(),
            scope: "service / production".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "configuration".into(),
        };
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "timeout_conflict".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![claim.id.clone()],
            summary: "conflict".into(),
            status: DisputeStatus::Open,
            created_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let context = ArbitrationResolutionContext {
            dispute_id: dispute.id.clone(),
            resolution: DisputeResolution {
                resolution_id: ArbitrationResolutionId::random(),
                resolved_by: ResolvedBy::Automatic,
                resolved_at: "2026-08-03T00:00:00Z".parse().unwrap(),
                resolution_type: Some(ResolutionType::ConflictResolved),
                resolution_basis: Some(ResolutionBasis::Evidence),
                conclusion: "use current configuration".into(),
                claim_assessments: vec![ClaimAssessment {
                    claim_id: claim.id.clone(),
                    recommended_status: ClaimStatus::Active,
                    assessment: "supported".into(),
                    recommended_scope: None,
                    recommended_statement: None,
                    reason: "configuration".into(),
                }],
                rejection_reason: None,
            },
            context_snapshot_hash: Some("sha256-v1:abcd".into()),
            dispute_snapshot: dispute,
            direct_claim_snapshots: vec![claim],
            snapshot_source_resolution_id: None,
        };

        let yaml = serde_yaml_ng::to_string(&context).unwrap();
        let decoded: ArbitrationResolutionContext = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(decoded, context);
    }
}
