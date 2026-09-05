//! Router scope overview 派生快照。
//!
//! 这里只定义 router 自己维护的 scope 聚合结构：
//! - `ScopeOverviewItem`：单个 scope 的 claim/dispute 计数
//! - `ScopesOverviewSnapshot`：整体快照，供 HTTP overview 端点与冻结评测目录使用

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::claim::{AgentId, ClaimId, Confidence};
use crate::time::serde_utc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterClaimSummaryText {
    pub text: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterClaimSummary {
    pub id: ClaimId,
    pub name: RouterClaimSummaryText,
    pub scope: RouterClaimSummaryText,
    pub holder: AgentId,
    pub confidence: Confidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterClaimSummaryCatalog {
    #[serde(default)]
    pub items: Vec<RouterClaimSummary>,
    pub omitted: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeOverviewItem {
    pub scope: String,
    pub active_claims: usize,
    pub stale_claims: usize,
    pub open_disputes: usize,
    pub resolved_disputes: usize,
    #[serde(with = "serde_utc")]
    pub latest_claim_created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopesOverviewSnapshot {
    #[serde(default)]
    pub scopes: Vec<ScopeOverviewItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_summaries: Option<RouterClaimSummaryCatalog>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_scope_only_snapshot_stays_compatible_and_omits_empty_catalog_field() {
        let decoded: ScopesOverviewSnapshot = serde_json::from_str(r#"{"scopes":[]}"#).unwrap();
        assert_eq!(decoded, ScopesOverviewSnapshot::default());
        assert_eq!(serde_json::to_string(&decoded).unwrap(), r#"{"scopes":[]}"#);
    }
}
