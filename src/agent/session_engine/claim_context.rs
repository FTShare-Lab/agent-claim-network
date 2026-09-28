//! 会话独立的 Claim 投影：固定 system 快照作为基线，增量快照沿用 ModelContext 的 WAL 和去重。
use super::SessionEngine;
use crate::claim::{Claim, ClaimId, ClaimStatus};
use crate::storage::{read_yaml, write_yaml_atomic, FileLockGuard, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ClaimIdentity {
    name: String,
    scope: String,
    status: ClaimStatus,
    hash: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClaimRuntimeProjection {
    pub text: String,
    pub has_changes: bool,
    present_ids: BTreeSet<String>,
}

impl ClaimRuntimeProjection {
    pub(super) fn with_previous(&self, previous: Option<&str>) -> String {
        let Some(previous) = previous else {
            return self.text.clone();
        };
        let parse = |text: &str| {
            text.lines()
                .next()
                .and_then(|line| line.strip_prefix("claim_changes_since_system_prompt: "))
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        };
        let (Some(mut current), Some(previous)) = (parse(&self.text), parse(previous)) else {
            return self.text.clone();
        };
        let previous_changes = previous
            .get("changes")
            .and_then(serde_json::Value::as_array);
        if let (Some(changes), Some(previous_changes)) = (
            current
                .get_mut("changes")
                .and_then(serde_json::Value::as_array_mut),
            previous_changes,
        ) {
            for item in previous_changes {
                let Some(id) = item.get("id").and_then(serde_json::Value::as_str) else {
                    continue;
                };
                if !self.present_ids.contains(id)
                    && !changes
                        .iter()
                        .any(|c| c.get("id").and_then(serde_json::Value::as_str) == Some(id))
                {
                    let mut removed = item.clone();
                    removed["change"] = json!("removed");
                    changes.push(removed);
                }
            }
            changes.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        }
        format!("claim_changes_since_system_prompt: {}\nThis cumulative identity snapshot supersedes earlier claim notices. Assess relevance from name/scope; use read_claim for current content only when the current task needs to cite or rely on that Claim. Do not read every changed Claim merely because it is listed. Deprecated/removed entries must not be relied on; no content read is needed to stop using them.",current)
    }
}

pub(super) type ClaimIndex = BTreeMap<ClaimId, ClaimIdentity>;

pub(crate) fn content_hash<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(ring::digest::digest(&ring::digest::SHA256, &bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
pub(super) fn claim_index(claims: &[Claim]) -> anyhow::Result<ClaimIndex> {
    claims
        .iter()
        .map(|c| {
            Ok((
                c.id.clone(),
                ClaimIdentity {
                    name: c.name.clone(),
                    scope: c.scope.clone(),
                    status: c.status,
                    hash: content_hash(c)?,
                },
            ))
        })
        .collect()
}

impl SessionEngine {
    pub(super) async fn claim_runtime_context(
        &self,
        session_dir: &std::path::Path,
    ) -> anyhow::Result<Option<ClaimRuntimeProjection>> {
        // Finalize/Inbox 持锁时下一 turn 再观察，不阻塞用户等待后台模型。
        let lock = crate::storage::paths::agent_home_knowledge_apply_lock_path(
            self.runner.maintainer_upload_queue.agent_home(),
        );
        let Some(_guard) = FileLockGuard::try_lock_exclusive(&lock).await? else {
            return Ok(None);
        };
        if tokio::fs::try_exists(super::dream_execution::pending_path(
            self.runner.maintainer_upload_queue.agent_home(),
        ))
        .await?
        {
            return Ok(None);
        }
        let current = claim_index(&self.agent.claim_store.list_local_claims().await?)?;
        drop(_guard);
        let path = session_dir.join("claim_prompt_baseline.yaml");
        let (baseline, legacy): (ClaimIndex, bool) = match read_yaml(&path).await {
            Ok(index) => (index, false),
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                (BTreeMap::new(), true)
            }
            Err(e) => return Err(e.into()),
        };
        let changes = project_changes(&baseline, &current);
        // 累积的是 system 快照之后的差异，最新投影完整替代旧投影。
        // 不提前推进“已读”游标，失败、取消、resume 和 compaction 均由已有 WAL 语义覆盖。
        let revision = content_hash(&current)?;
        let has_changes = !changes.is_empty();
        let notice = json!({"revision":revision,"refresh_all":legacy,"changes":changes});
        Ok(Some(ClaimRuntimeProjection {
            text: format!("claim_changes_since_system_prompt: {}\nThis cumulative identity snapshot supersedes earlier claim notices. Assess relevance from name/scope; use read_claim for current content only when the current task needs to cite or rely on that Claim. Do not read every changed Claim merely because it is listed. Deprecated/removed entries must not be relied on; no content read is needed to stop using them.", serde_json::to_string(&notice)?),
            has_changes,
            present_ids: current.keys().map(ToString::to_string).collect(),
        }))
    }

    pub(super) async fn save_claim_prompt_baseline(
        &self,
        session_dir: &std::path::Path,
        claims: &[Claim],
    ) -> anyhow::Result<()> {
        write_yaml_atomic(
            &session_dir.join("claim_prompt_baseline.yaml"),
            &claim_index(claims)?,
        )
        .await?;
        Ok(())
    }
}

fn project_changes(baseline: &ClaimIndex, current: &ClaimIndex) -> Vec<serde_json::Value> {
    let mut changes = Vec::new();
    for (id, item) in current {
        let previous = baseline.get(id);
        if previous.is_some_and(|p| p.hash == item.hash) {
            continue;
        }
        let kind = if item.status == ClaimStatus::Deprecated {
            "deprecated"
        } else if previous.is_none() {
            "created"
        } else {
            "updated"
        };
        changes.push(json!({"change":kind,"id":id,"name":item.name,"scope":item.scope}));
    }
    for (id, item) in baseline {
        if !current.contains_key(id) {
            changes.push(json!({"change":"removed","id":id,"name":item.name,"scope":item.scope}));
        }
    }
    changes
}
