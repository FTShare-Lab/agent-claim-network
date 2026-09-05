//! Router 单文件派生快照。
//!
//! `derived_views.yaml` 将 claim 索引与 scope 总览作为同一个原子发布对象：读取方只会
//! 看到完整的旧快照或完整的新快照，永远不会把两次刷新的不同结果拼在一起。

use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{ClaimIndex, ScopesOverviewSnapshot};
use crate::storage::StorageError;
use crate::time::{now_seconds, serde_utc};

const DERIVED_VIEWS_SCHEMA_VERSION: u64 = 1;

/// 先宽松探测版本号，避免未来 schema 因新增字段被误判为普通解码损坏。
#[derive(Debug, Deserialize)]
struct SchemaProbe {
    schema_version: Option<u64>,
}

/// Router 查询与 scope 总览共享的唯一持久化派生快照。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RouterDerivedViewsSnapshot {
    schema_version: u64,
    #[serde(with = "serde_utc")]
    generated_at: DateTime<Utc>,
    claim_index: ClaimIndex,
    scopes_overview: ScopesOverviewSnapshot,
}

/// 读取派生快照后，调用方可据此决定是否从权威 Claim / Dispute 重建。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DerivedViewsRead {
    Missing,
    RecoverableCorrupt,
    Current(RouterDerivedViewsSnapshot),
}

/// schema 不兼容或底层存储失败。
#[derive(Debug, thiserror::Error)]
pub(crate) enum DerivedViewsError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("Router 派生快照 schema_version={found} 新于当前支持版本 {supported}，拒绝覆盖")]
    FutureSchema { found: u64, supported: u64 },
    #[error("Router 派生快照 schema_version={found} 不受当前版本支持")]
    UnsupportedSchema { found: u64 },
}

impl RouterDerivedViewsSnapshot {
    /// 以本次权威扫描的两个派生结果构造完整快照。
    pub(crate) fn new(claim_index: ClaimIndex, scopes_overview: ScopesOverviewSnapshot) -> Self {
        Self {
            schema_version: DERIVED_VIEWS_SCHEMA_VERSION,
            generated_at: now_seconds(),
            claim_index,
            scopes_overview,
        }
    }

    pub(crate) const fn claim_index(&self) -> &ClaimIndex {
        &self.claim_index
    }

    pub(crate) const fn scopes_overview(&self) -> &ScopesOverviewSnapshot {
        &self.scopes_overview
    }

    #[cfg(test)]
    pub(crate) const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[cfg(test)]
    pub(crate) const fn generated_at(&self) -> &DateTime<Utc> {
        &self.generated_at
    }

    fn validate_schema(self) -> Result<Self, DerivedViewsError> {
        if self.schema_version > DERIVED_VIEWS_SCHEMA_VERSION {
            return Err(DerivedViewsError::FutureSchema {
                found: self.schema_version,
                supported: DERIVED_VIEWS_SCHEMA_VERSION,
            });
        }
        if self.schema_version != DERIVED_VIEWS_SCHEMA_VERSION {
            return Err(DerivedViewsError::UnsupportedSchema {
                found: self.schema_version,
            });
        }
        Ok(self)
    }
}

/// 读取固定单文件快照；结构损坏可由权威数据重建，未来 schema 则明确失败关闭。
pub(crate) async fn read_derived_views(path: &Path) -> Result<DerivedViewsRead, DerivedViewsError> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(DerivedViewsRead::Missing);
        }
        Err(source) => {
            return Err(StorageError::Io {
                path: path.to_path_buf(),
                source,
            }
            .into());
        }
    };
    let probe = match serde_yaml_ng::from_slice::<SchemaProbe>(&bytes) {
        Ok(probe) => probe,
        Err(_) => return Ok(DerivedViewsRead::RecoverableCorrupt),
    };
    let Some(schema_version) = probe.schema_version else {
        return Ok(DerivedViewsRead::RecoverableCorrupt);
    };
    if schema_version > DERIVED_VIEWS_SCHEMA_VERSION {
        return Err(DerivedViewsError::FutureSchema {
            found: schema_version,
            supported: DERIVED_VIEWS_SCHEMA_VERSION,
        });
    }
    if schema_version != DERIVED_VIEWS_SCHEMA_VERSION {
        return Err(DerivedViewsError::UnsupportedSchema {
            found: schema_version,
        });
    }

    match serde_yaml_ng::from_slice::<RouterDerivedViewsSnapshot>(&bytes) {
        Ok(snapshot) => Ok(DerivedViewsRead::Current(snapshot.validate_schema()?)),
        Err(_) => Ok(DerivedViewsRead::RecoverableCorrupt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::{ClaimIndexEntry, ScopeOverviewItem};

    fn snapshot() -> RouterDerivedViewsSnapshot {
        RouterDerivedViewsSnapshot::new(
            ClaimIndex(vec![ClaimIndexEntry {
                id: "claim_1234abcd".parse().unwrap(),
                path: "agents/agent-a/claims/claim_1234abcd.yaml".into(),
                open_dispute_ids: Vec::new(),
                resolved_dispute_ids: Vec::new(),
            }]),
            ScopesOverviewSnapshot {
                scopes: vec![ScopeOverviewItem {
                    scope: "payments".into(),
                    active_claims: 1,
                    stale_claims: 0,
                    open_disputes: 0,
                    resolved_disputes: 0,
                    latest_claim_created_at: "2026-07-13T00:00:00Z".parse().unwrap(),
                }],
                claim_summaries: None,
            },
        )
    }

    #[test]
    fn v1_snapshot_round_trips_as_one_complete_document() {
        let snapshot = snapshot();
        let yaml = serde_yaml_ng::to_string(&snapshot).unwrap();
        let decoded: RouterDerivedViewsSnapshot = serde_yaml_ng::from_str(&yaml).unwrap();

        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.schema_version(), 1);
        assert_eq!(decoded.claim_index().entries().len(), 1);
        assert_eq!(decoded.scopes_overview().scopes.len(), 1);
        assert_eq!(decoded.generated_at().timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn v1_snapshot_rejects_unknown_nested_fields() {
        let yaml = "schema_version: 1\ngenerated_at: 2026-07-13T00:00:00Z\nclaim_index: []\nscopes_overview:\n  scopes: []\n  unexpected: true\n";

        assert!(serde_yaml_ng::from_str::<RouterDerivedViewsSnapshot>(yaml).is_err());
    }
}
