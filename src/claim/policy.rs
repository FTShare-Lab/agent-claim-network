//! Policy 实体定义。Policy 由 Maintainer 发布，仅描述行动约束、不等同于事实判断。
//!
//! `created_at` 一旦写入不再修改；`updated_at` 仅在 status 变更（active → deprecated）时
//! 由 maintainer 写入，首次发布时为 `None`。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::id::{AgentId, PolicyId};
use crate::time::{serde_utc, serde_utc_opt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyMessageType {
    PolicyUpdate,
    ClaimAttributeUpdate,
}

fn default_policy_message_type() -> PolicyMessageType {
    PolicyMessageType::PolicyUpdate
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyStatus {
    Active,
    Deprecated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Policy {
    pub id: PolicyId,
    #[serde(default = "default_policy_message_type")]
    pub message_type: PolicyMessageType,
    pub name: String,
    pub statement: String,
    pub scope: String,
    pub status: PolicyStatus,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(
        default,
        with = "serde_utc_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_agents: Option<Vec<AgentId>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Policy {
        Policy {
            id: PolicyId::random(),
            message_type: PolicyMessageType::PolicyUpdate,
            name: "batch_order_chunking_limit_50".into(),
            statement: "批量订单提交时必须按每批不超过50条分片".into(),
            scope: "order-system / batch-order-submit".into(),
            status: PolicyStatus::Active,
            created_at: "2026-04-21T10:00:00Z".parse().unwrap(),
            updated_at: None,
            target_agents: None,
        }
    }

    #[test]
    fn policy_yaml_round_trip_no_update() {
        let p = sample();
        let yaml = serde_yaml_ng::to_string(&p).unwrap();
        // 未更新时 updated_at 不写出
        assert!(yaml.contains("message_type: policy_update"));
        assert!(!yaml.contains("updated_at"));
        assert!(!yaml.contains("target_agents"));
        let back: Policy = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn policy_yaml_without_target_agents_defaults_to_broadcast() {
        let yaml = r#"
id: policy_1234abcd
message_type: policy_update
name: batch_order_chunking_limit_50
statement: 批量订单提交时必须按每批不超过50条分片
scope: order-system / batch-order-submit
status: active
created_at: 2026-04-21T10:00:00Z
"#;
        let p: Policy = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(p.target_agents, None);
    }

    #[test]
    fn legacy_policy_yaml_without_message_type_defaults_to_policy_update() {
        let yaml = r#"
id: policy_1234abcd
name: batch_order_chunking_limit_50
statement: 批量订单提交时必须按每批不超过50条分片
scope: order-system / batch-order-submit
status: active
created_at: 2026-04-21T10:00:00Z
"#;
        let p: Policy = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(p.message_type, PolicyMessageType::PolicyUpdate);
    }

    #[test]
    fn policy_yaml_round_trip_with_update() {
        let mut p = sample();
        p.status = PolicyStatus::Deprecated;
        p.updated_at = Some("2026-04-22T08:30:00Z".parse().unwrap());
        let yaml = serde_yaml_ng::to_string(&p).unwrap();
        assert!(yaml.contains("updated_at: 2026-04-22T08:30:00Z"));
        assert!(yaml.contains("created_at: 2026-04-21T10:00:00Z"));
        let back: Policy = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(p, back);
    }
}
