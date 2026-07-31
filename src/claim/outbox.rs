//! Outbox 实体定义。
//!
//! Maintainer 端的待投递台账：每条 entry 表示"一条 inbox message 的一份投递记录"。
//! - broadcast 类 entry：单条记录覆盖所有 agent，通过 `offered_to` / `delivered_to`
//!   区分可重投的提供尝试与已持久收件
//! - targeted 类 entry：每个目标 agent 各占一条独立 entry
//!
//! 同一次 maintainer 对外动作（publish / deprecate / claim_update_suggestion）产生的所有
//! entry 共享同一个 `maintainer_action_id`，便于审计回溯。
//!
//! 设计要点：
//! - `inbox_message` 字段是创建 entry 那一刻的完整快照；之后 policy 文件状态变化也不影响
//!   该 entry 的内容
//! - `offered_to` 记录 pull 尝试，不是终态；ACK 前会重复提供同一稳定 `inbox_id`
//! - `delivered_to` append-only：同一 (inbox_id, agent_id) 只会出现一次；仅在 Agent
//!   本地持久落盘后由显式 ACK 推进，list_send_log 直接从这里 flatten

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::id::{AgentId, InboxId, MaintainerActionId};
use super::inbox::InboxMessage;
use crate::time::serde_utc;

/// Maintainer 投递台账中的一条记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub inbox_id: InboxId,
    pub maintainer_action_id: MaintainerActionId,
    #[serde(flatten)]
    pub target: OutboxTarget,
    #[serde(with = "serde_utc")]
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub offered_to: Vec<OfferedMark>,
    #[serde(default)]
    pub delivered_to: Vec<DeliveredMark>,
    pub inbox_message: InboxMessage,
}

/// 单个 Agent 的可重投提供记录。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferedMark {
    pub agent_id: AgentId,
    #[serde(with = "serde_utc")]
    pub first_offered_at: DateTime<Utc>,
    #[serde(with = "serde_utc")]
    pub last_offered_at: DateTime<Utc>,
    pub attempts: u64,
}

/// 投递目标维度。
///
/// 序列化形态（与 `#[serde(flatten)]` 配合）：
/// - Broadcast → `target_kind: broadcast`
/// - Targeted(a) → `target_kind: targeted` + `target_agent: a`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "target_kind", rename_all = "snake_case")]
pub enum OutboxTarget {
    Broadcast,
    Targeted { target_agent: AgentId },
}

/// 单条 (agent, sent_at) 投递事实，append-only。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveredMark {
    pub agent_id: AgentId,
    #[serde(with = "serde_utc")]
    pub sent_at: DateTime<Utc>,
}

/// Agent 确认已把一批消息持久写入本地 Inbox 的请求。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxAckRequest {
    pub agent_id: AgentId,
    pub inbox_ids: Vec<InboxId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{
        InboxMessage, InboxMessageKind, Policy, PolicyId, PolicyMessageType, PolicyStatus,
    };

    fn sample_inbox_message(inbox_id: InboxId) -> InboxMessage {
        InboxMessage {
            id: inbox_id,
            kind: InboxMessageKind::PolicyUpdate {
                policy: Policy {
                    id: PolicyId::random(),
                    message_type: PolicyMessageType::PolicyUpdate,
                    name: "p".into(),
                    statement: "stmt".into(),
                    scope: "sc".into(),
                    status: PolicyStatus::Active,
                    created_at: "2026-04-21T00:00:00Z".parse().unwrap(),
                    updated_at: None,
                    target_agents: None,
                },
            },
            handled_at: None,
        }
    }

    #[test]
    fn broadcast_entry_round_trips_without_target_agent() {
        let mid = InboxId::random();
        let entry = OutboxEntry {
            inbox_id: mid.clone(),
            maintainer_action_id: MaintainerActionId::random(),
            target: OutboxTarget::Broadcast,
            created_at: "2026-05-14T10:00:00Z".parse().unwrap(),
            offered_to: vec![OfferedMark {
                agent_id: AgentId::new("agent-a").unwrap(),
                first_offered_at: "2026-05-14T10:00:30Z".parse().unwrap(),
                last_offered_at: "2026-05-14T10:00:45Z".parse().unwrap(),
                attempts: 2,
            }],
            delivered_to: vec![DeliveredMark {
                agent_id: AgentId::new("agent-a").unwrap(),
                sent_at: "2026-05-14T10:01:00Z".parse().unwrap(),
            }],
            inbox_message: sample_inbox_message(mid),
        };
        let yaml = serde_yaml_ng::to_string(&entry).unwrap();
        assert!(yaml.contains("target_kind: broadcast"));
        assert!(
            !yaml.contains("target_agent"),
            "broadcast 不应序列化 target_agent: {yaml}"
        );
        let back: OutboxEntry = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn targeted_entry_round_trips_with_target_agent() {
        let mid = InboxId::random();
        let agent = AgentId::new("agent-b").unwrap();
        let entry = OutboxEntry {
            inbox_id: mid.clone(),
            maintainer_action_id: MaintainerActionId::random(),
            target: OutboxTarget::Targeted {
                target_agent: agent.clone(),
            },
            created_at: "2026-05-14T10:00:00Z".parse().unwrap(),
            offered_to: vec![],
            delivered_to: vec![],
            inbox_message: sample_inbox_message(mid),
        };
        let yaml = serde_yaml_ng::to_string(&entry).unwrap();
        assert!(yaml.contains("target_kind: targeted"));
        assert!(yaml.contains("target_agent: agent-b"));
        let back: OutboxEntry = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(entry, back);
    }

    #[test]
    fn delivered_to_default_empty_when_missing() {
        let mid = InboxId::random();
        let entry = OutboxEntry {
            inbox_id: mid.clone(),
            maintainer_action_id: MaintainerActionId::random(),
            target: OutboxTarget::Broadcast,
            created_at: "2026-05-14T10:00:00Z".parse().unwrap(),
            offered_to: vec![],
            delivered_to: vec![],
            inbox_message: sample_inbox_message(mid),
        };
        let yaml = serde_yaml_ng::to_string(&entry).unwrap();
        let back: OutboxEntry = serde_yaml_ng::from_str(&yaml).unwrap();
        assert!(back.delivered_to.is_empty());
    }

    #[test]
    fn offered_to_defaults_empty_for_legacy_entry() {
        let mid = InboxId::random();
        let entry = OutboxEntry {
            inbox_id: mid.clone(),
            maintainer_action_id: MaintainerActionId::random(),
            target: OutboxTarget::Broadcast,
            created_at: "2026-05-14T10:00:00Z".parse().unwrap(),
            offered_to: vec![],
            delivered_to: vec![],
            inbox_message: sample_inbox_message(mid),
        };
        let yaml = serde_yaml_ng::to_string(&entry).unwrap();
        let mut value: serde_yaml_ng::Value = serde_yaml_ng::from_str(&yaml).unwrap();
        value
            .as_mapping_mut()
            .unwrap()
            .remove(serde_yaml_ng::Value::String("offered_to".into()));
        let legacy_yaml = serde_yaml_ng::to_string(&value).unwrap();
        let back: OutboxEntry = serde_yaml_ng::from_str(&legacy_yaml).unwrap();
        assert!(back.offered_to.is_empty());
    }
}
