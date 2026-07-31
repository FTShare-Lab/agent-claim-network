//! Dispute 实体：标记 claim 之间的不兼容或冲突。
//! 由 agent 主动写入，maintainer 改 status；router 仅作为派生 view 反映。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::id::{AgentId, ClaimId, DisputeId};
use crate::time::{serde_utc, serde_utc_opt};

fn legacy_reporter_agent_id() -> AgentId {
    AgentId::new("legacy-agent").expect("legacy-agent 是合法 AgentId")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DisputeStatus {
    Open,
    Resolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DisputeReportValidationError {
    #[error("agent 上报 dispute 时 status 必须为 open 且 resolved_at 必须为空")]
    InvalidLifecycleState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dispute {
    pub id: DisputeId,
    pub name: String,
    #[serde(default = "legacy_reporter_agent_id")]
    pub reporter_agent_id: AgentId,
    pub claims: Vec<ClaimId>,
    pub summary: String,
    pub status: DisputeStatus,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(
        default,
        with = "serde_utc_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub resolved_at: Option<DateTime<Utc>>,
}

impl Dispute {
    /// 校验 agent 上报时不得写入由 maintainer 管理的解决状态。
    pub fn validate_agent_report(&self) -> Result<(), DisputeReportValidationError> {
        if self.status != DisputeStatus::Open || self.resolved_at.is_some() {
            return Err(DisputeReportValidationError::InvalidLifecycleState);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispute_yaml_round_trip_open() {
        let d = Dispute {
            id: DisputeId::random(),
            name: "payment_batch_timeout_vs_success".into(),
            reporter_agent_id: AgentId::new("agent-b").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "一个说>100条可能超时，另一个说200条成功；需确认环境和版本是否不同".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-12T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };
        let yaml = serde_yaml_ng::to_string(&d).unwrap();
        let back: Dispute = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn dispute_yaml_round_trip_resolved() {
        let d = Dispute {
            id: DisputeId::random(),
            name: "payment_batch_timeout_scope".into(),
            reporter_agent_id: AgentId::new("agent-c").unwrap(),
            claims: vec![ClaimId::random(), ClaimId::random()],
            summary: "已确认 prod 与 staging 配置不同，两个 claim scope 不同，不构成事实冲突"
                .into(),
            status: DisputeStatus::Resolved,
            created_at: "2026-04-12T00:00:00Z".parse().unwrap(),
            resolved_at: Some("2026-04-15T10:00:00Z".parse().unwrap()),
        };
        let yaml = serde_yaml_ng::to_string(&d).unwrap();
        let back: Dispute = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn dispute_yaml_without_reporter_uses_legacy_default_for_compat() {
        let yaml = r#"
id: dispute_1234abcd
name: payment_batch_timeout_scope
claims:
  - claim_1234abcd
  - claim_5678efab
summary: legacy dispute
status: open
created_at: 2026-04-12T00:00:00Z
"#;
        let back: Dispute = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(back.reporter_agent_id.as_str(), "legacy-agent");
    }

    #[test]
    fn agent_report_only_accepts_open_without_resolved_at() {
        let mut dispute = Dispute {
            id: DisputeId::random(),
            name: "report_validation".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![ClaimId::random()],
            summary: "open".into(),
            status: DisputeStatus::Open,
            created_at: "2026-04-12T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        };

        assert_eq!(dispute.validate_agent_report(), Ok(()));

        dispute.status = DisputeStatus::Resolved;
        assert_eq!(
            dispute.validate_agent_report(),
            Err(DisputeReportValidationError::InvalidLifecycleState)
        );

        dispute.status = DisputeStatus::Open;
        dispute.resolved_at = Some("2026-04-15T10:00:00Z".parse().unwrap());
        assert_eq!(
            dispute.validate_agent_report(),
            Err(DisputeReportValidationError::InvalidLifecycleState)
        );
    }
}
