//! Claim 实体定义。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::id::{AgentId, ClaimId, SourceId};
use crate::time::{serde_utc, serde_utc_opt};

// 注：source_claim_ids 用 Vec<SourceId>（enum: Claim(ClaimId) | Policy(PolicyId)），
// 序列化为单字符串，磁盘上仍是 `[claim_xxx, policy_yyy]` 列表，与旧 Vec<String>
// 形态完全兼容；但反序列化会按前缀校验，杜绝 LLM 把任意字符串塞进 source 链路。
// SourceId 不暴露其它前缀（trace_/dispute_）——这两类不是 claim 来源，落盘即视为非法。

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClaimStatus {
    Active,
    Stale,
    Deprecated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub id: ClaimId,
    pub name: String,
    pub statement: String,
    pub scope: String,
    pub holder: AgentId,
    pub confidence: Confidence,
    pub status: ClaimStatus,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(
        default,
        with = "serde_utc_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_claim_ids: Vec<SourceId>,
    pub evidence_summary: String,
}

impl Claim {
    /// 返回 claim 最近一次语义更新时间；旧数据回退到创建时间。
    pub fn effective_updated_at(&self) -> DateTime<Utc> {
        self.updated_at.unwrap_or(self.created_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Claim {
        Claim {
            id: ClaimId::random(),
            name: "payment_batch_timeout".into(),
            statement: "批量订单超过约100条时，payment-service 可能触发30s超时".into(),
            scope: "order-system / payment-service / prod".into(),
            holder: AgentId::new("agent-b").unwrap(),
            confidence: Confidence::High,
            status: ClaimStatus::Active,
            created_at: "2026-04-10T12:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "订单#12345日志显示 payment-service 在30s后返回 timeout".into(),
        }
    }

    #[test]
    fn claim_yaml_round_trip() {
        let c = sample();
        let yaml = serde_yaml_ng::to_string(&c).unwrap();
        let back: Claim = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn confidence_serialized_lowercase() {
        let c = sample();
        let yaml = serde_yaml_ng::to_string(&c).unwrap();
        assert!(yaml.contains("confidence: high"));
        assert!(yaml.contains("status: active"));
        assert!(!yaml.contains("updated_at"));
    }

    #[test]
    fn legacy_claim_without_updated_at_uses_created_at_for_freshness() {
        let yaml = serde_yaml_ng::to_string(&sample()).unwrap();
        let claim: Claim = serde_yaml_ng::from_str(&yaml).unwrap();

        assert_eq!(claim.updated_at, None);
        assert_eq!(claim.effective_updated_at(), claim.created_at);
    }
}
