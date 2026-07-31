//! `derived_views.yaml` 内 `claim_index` 字段的内存表示 + 序列化结构。
//!
//! 设计要点：
//! - **不复制 claim 内容**，仅存 id / 相对 path / dispute 关联
//! - `path` 使用相对 `team_root` 的相对路径，便于人读 + 跨机器迁移
//! - dispute 关联字段是派生 view，由 Router 刷新流程整体重算

use serde::{Deserialize, Serialize};

use crate::claim::{ClaimId, DisputeId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimIndexEntry {
    pub id: ClaimId,
    /// 相对 team_root 的路径，例如 `agents/agent-b/claims/claim_a3f7c91d.yaml`
    pub path: String,
    #[serde(default)]
    pub open_dispute_ids: Vec<DisputeId>,
    #[serde(default)]
    pub resolved_dispute_ids: Vec<DisputeId>,
}

/// 整体即一个 YAML 列表（`#[serde(transparent)]`）
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClaimIndex(pub Vec<ClaimIndexEntry>);

impl ClaimIndex {
    pub fn entries(&self) -> &[ClaimIndexEntry] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_index_yaml_round_trip() {
        let idx = ClaimIndex(vec![
            ClaimIndexEntry {
                id: ClaimId::random(),
                path: "agents/agent-b/claims/claim_xxxxxxxx.yaml".into(),
                open_dispute_ids: vec![DisputeId::random()],
                resolved_dispute_ids: vec![],
            },
            ClaimIndexEntry {
                id: ClaimId::random(),
                path: "agents/agent-d/claims/claim_yyyyyyyy.yaml".into(),
                open_dispute_ids: vec![],
                resolved_dispute_ids: vec![DisputeId::random()],
            },
        ]);
        let yaml = serde_yaml_ng::to_string(&idx).unwrap();
        // transparent 包装：顶层就是 list
        assert!(yaml.starts_with("- ") || yaml.starts_with("-\n"));
        let back: ClaimIndex = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(idx, back);
    }

    #[test]
    fn empty_claim_index_serializes_to_empty_seq() {
        let idx = ClaimIndex::default();
        let yaml = serde_yaml_ng::to_string(&idx).unwrap();
        let back: ClaimIndex = serde_yaml_ng::from_str(&yaml).unwrap();
        assert!(back.entries().is_empty());
    }
}
