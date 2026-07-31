//! Inbox 消息实体：team store → agent 的下行消息。
//!
//! 当前支持 policy 内化、claim 属性更新建议两类下行。
//!
//! 设计要点：消息**自包含**——`PolicyUpdate` 内嵌完整 `Policy`，
//! 而不是只带 `policy_id`。原因：agent 不允许读 `maintainer/policies/`
//! （见 `docs/core_behavior.md` 的 Inbox 边界），inbox 必须能让 agent
//! 不依赖任何 team store 侧文件就完成处理。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::id::{InboxId, PolicyId};
use super::policy::{Policy, PolicyMessageType};
use crate::time::serde_utc_opt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxMessage {
    pub id: InboxId,
    #[serde(flatten)]
    pub kind: InboxMessageKind,
    #[serde(
        default,
        with = "serde_utc_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub handled_at: Option<DateTime<Utc>>,
}

impl InboxMessage {
    /// 业务事件时间，用于按时间序排消息。
    /// updated_at 仅在 policy status 变更时设置；首次发布回退到 created_at。
    pub fn event_at(&self) -> DateTime<Utc> {
        let policy = self.policy();
        policy.updated_at.unwrap_or(policy.created_at)
    }

    /// 取 inbox message 关联的 policy 引用（两类 message 都内嵌 Policy）。
    pub fn policy(&self) -> &Policy {
        match &self.kind {
            InboxMessageKind::PolicyUpdate { policy }
            | InboxMessageKind::ClaimAttributeUpdate { policy } => policy,
        }
    }

    /// 关联 policy 的 id 快捷取值。
    pub fn policy_id(&self) -> &PolicyId {
        &self.policy().id
    }

    /// 与 policy.message_type 一致的派生取值。
    pub fn message_type(&self) -> PolicyMessageType {
        match &self.kind {
            InboxMessageKind::PolicyUpdate { .. } => PolicyMessageType::PolicyUpdate,
            InboxMessageKind::ClaimAttributeUpdate { .. } => {
                PolicyMessageType::ClaimAttributeUpdate
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "message_type", rename_all = "snake_case")]
pub enum InboxMessageKind {
    PolicyUpdate { policy: Policy },
    ClaimAttributeUpdate { policy: Policy },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{PolicyId, PolicyMessageType, PolicyStatus};

    fn sample_policy() -> Policy {
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
    fn policy_update_message_round_trip() {
        let msg = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::PolicyUpdate {
                policy: sample_policy(),
            },
            handled_at: None,
        };
        let yaml = serde_yaml_ng::to_string(&msg).unwrap();
        assert!(yaml.contains("message_type: policy_update"));
        assert!(yaml.contains("policy:"));
        let back: InboxMessage = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(msg, back);
    }

    #[test]
    fn claim_attribute_update_message_round_trip() {
        let msg = InboxMessage {
            id: InboxId::random(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: sample_policy(),
            },
            handled_at: None,
        };
        let yaml = serde_yaml_ng::to_string(&msg).unwrap();
        assert!(yaml.contains("message_type: claim_attribute_update"));
        assert!(yaml.contains("policy:"));
        let back: InboxMessage = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(msg, back);
    }
}
