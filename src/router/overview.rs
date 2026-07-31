//! Router scope overview 派生快照。
//!
//! 这里只定义 router 自己维护的 scope 聚合结构：
//! - `ScopeOverviewItem`：单个 scope 的 claim/dispute 计数
//! - `ScopesOverviewSnapshot`：整体快照，供 HTTP overview 端点直接返回

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::time::serde_utc;

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
}
