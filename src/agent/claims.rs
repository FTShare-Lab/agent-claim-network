//! 当前 agent 自有 Claim 与 Trace 的浏览、CAS 编辑和同步编排。

use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use super::runner::AgentRunner;
use crate::claim::{AgentId, Claim, ClaimId, ClaimStatus, Confidence, SourceId, TraceId};
use crate::memory_safety::scan_memory_content;
use crate::storage::{paths, read_yaml, write_yaml_atomic, FileLockGuard, StorageError};
use crate::time::now_seconds;

pub const DEFAULT_CLAIM_LIST_LIMIT: usize = 20;
pub const MAX_CLAIM_PAGE_LIMIT: usize = 100;
pub const DEFAULT_TRACE_TASK_PAGE_LIMIT: usize = 4_000;
pub const MAX_TRACE_TASK_PAGE_LIMIT: usize = 16_000;
const CLAIM_SUMMARY_NAME_CHARS: usize = 120;
const CLAIM_SUMMARY_SCOPE_CHARS: usize = 240;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BoundedText {
    pub text: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimSummary {
    pub id: ClaimId,
    pub name: BoundedText,
    pub scope: BoundedText,
    pub confidence: Confidence,
    pub status: ClaimStatus,
    #[serde(with = "crate::time::serde_utc")]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimListPage {
    pub items: Vec<ClaimSummary>,
    pub offset: usize,
    pub limit: usize,
    pub omitted: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimDetail {
    pub claim: Claim,
    pub revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimUpdate {
    pub id: ClaimId,
    pub expected_revision: String,
    pub name: Option<String>,
    pub statement: Option<String>,
    pub scope: Option<String>,
    pub evidence_summary: Option<String>,
    pub confidence: Option<Confidence>,
    pub status: Option<ClaimStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimUpdateResult {
    pub claim: Claim,
    pub revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_warning: Option<String>,
    pub sync_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingClaimEdit {
    expected_revision: String,
    target: Claim,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceSummary {
    pub id: TraceId,
    pub name: String,
    #[serde(with = "crate::time::serde_utc")]
    pub created_at: DateTime<Utc>,
    pub input_claims: Vec<SourceId>,
    pub output_claims: Vec<ClaimId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceListPage {
    pub items: Vec<TraceSummary>,
    pub offset: usize,
    pub limit: usize,
    pub omitted: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceDetail {
    pub id: TraceId,
    pub name: String,
    pub agent: AgentId,
    #[serde(with = "crate::time::serde_utc")]
    pub created_at: DateTime<Utc>,
    pub input_claims: Vec<SourceId>,
    pub output_claims: Vec<ClaimId>,
    pub task: String,
    pub task_offset: usize,
    pub task_limit: usize,
    pub task_omitted: usize,
    pub next_task_offset: Option<usize>,
}

impl AgentRunner {
    pub async fn list_claims(
        &self,
        query: Option<&str>,
        include_deprecated: bool,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<ClaimListPage> {
        validate_page_limit(limit)?;
        let query = query.map(str::trim).filter(|query| !query.is_empty());
        let query = query.map(str::to_lowercase);
        self.recover_pending_claim_edit().await?;
        let mut claims = self.claim_store.list_local_claims().await?;
        claims.retain(|claim| {
            claim.holder == self.agent_id
                && (include_deprecated || claim.status != ClaimStatus::Deprecated)
                && query.as_ref().is_none_or(|query| {
                    claim.name.to_lowercase().contains(query)
                        || claim.scope.to_lowercase().contains(query)
                        || claim.statement.to_lowercase().contains(query)
                })
        });
        claims.sort_by(|left, right| {
            right
                .effective_updated_at()
                .cmp(&left.effective_updated_at())
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        let total = claims.len();
        let items = claims
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|claim| {
                let updated_at = claim.effective_updated_at();
                ClaimSummary {
                    id: claim.id,
                    name: bounded_text(&claim.name, CLAIM_SUMMARY_NAME_CHARS),
                    scope: bounded_text(&claim.scope, CLAIM_SUMMARY_SCOPE_CHARS),
                    confidence: claim.confidence,
                    status: claim.status,
                    updated_at,
                }
            })
            .collect::<Vec<_>>();
        let (omitted, next_offset) = page_tail(offset, items.len(), total);
        Ok(ClaimListPage {
            items,
            offset,
            limit,
            omitted,
            next_offset,
        })
    }

    pub async fn read_claim(&self, id: &ClaimId) -> anyhow::Result<ClaimDetail> {
        self.recover_pending_claim_edit().await?;
        let claim = self.read_owned_claim(id).await?;
        Ok(ClaimDetail {
            revision: claim_revision(&claim)?,
            claim,
        })
    }

    pub async fn update_claim(&self, update: ClaimUpdate) -> anyhow::Result<ClaimUpdateResult> {
        validate_update(&update)?;
        let lock_path =
            paths::agent_home_knowledge_apply_lock_path(self.maintainer_upload_queue.agent_home());
        let _guard = FileLockGuard::lock_exclusive(lock_path).await?;
        self.recover_pending_claim_edit_locked().await?;
        let current = self.read_owned_claim(&update.id).await?;
        let current_revision = claim_revision(&current)?;
        if update.expected_revision != current_revision {
            anyhow::bail!(
                "claim revision conflict: id={} expected={} current={}",
                update.id,
                update.expected_revision,
                current_revision
            );
        }

        let mut next = current.clone();
        if let Some(value) = update.name {
            next.name = value;
        }
        if let Some(value) = update.statement {
            next.statement = value;
        }
        if let Some(value) = update.scope {
            next.scope = value;
        }
        if let Some(value) = update.evidence_summary {
            next.evidence_summary = value;
        }
        if let Some(value) = update.confidence {
            next.confidence = value;
        }
        if let Some(value) = update.status {
            next.status = value;
        }
        validate_shared_claim_text(&next)?;
        next.updated_at = Some(std::cmp::max(
            now_seconds(),
            current.effective_updated_at() + Duration::seconds(1),
        ));

        if self.team_services_configured() {
            let pending = PendingClaimEdit {
                expected_revision: current_revision,
                target: next.clone(),
            };
            write_yaml_atomic(
                &paths::agent_home_claim_edit_pending_path(
                    self.maintainer_upload_queue.agent_home(),
                ),
                &pending,
            )
            .await?;
        }
        self.claim_store.write_claim(&next).await?;
        if self.team_services_configured() {
            self.stage_maintainer_batch_with_durable_claims(vec![next.clone()], Vec::new())
                .await?;
            clear_pending_claim_edit(self.maintainer_upload_queue.agent_home()).await?;
        }
        drop(_guard);
        Ok(ClaimUpdateResult {
            revision: claim_revision(&next)?,
            claim: next,
            sync_warning: self
                .team_services_configured()
                .then(|| "Claim 已保存到本地，等待下一次团队同步。".to_string()),
            sync_pending: self.team_services_configured(),
        })
    }

    pub async fn list_traces(
        &self,
        claim_id: Option<&ClaimId>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<TraceListPage> {
        validate_page_limit(limit)?;
        self.recover_pending_claim_edit().await?;
        let mut traces = self.claim_store.list_local_traces().await?;
        traces.retain(|trace| {
            trace.agent == self.agent_id
                && claim_id.is_none_or(|claim_id| {
                    trace.output_claims.contains(claim_id)
                        || trace
                            .input_claims
                            .contains(&SourceId::Claim(claim_id.clone()))
                })
        });
        traces.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        let total = traces.len();
        let items = traces
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|trace| TraceSummary {
                id: trace.id,
                name: trace.name,
                created_at: trace.created_at,
                input_claims: trace.input_claims,
                output_claims: trace.output_claims,
            })
            .collect::<Vec<_>>();
        let (omitted, next_offset) = page_tail(offset, items.len(), total);
        Ok(TraceListPage {
            items,
            offset,
            limit,
            omitted,
            next_offset,
        })
    }

    pub async fn read_trace(
        &self,
        id: &TraceId,
        task_offset: usize,
        task_limit: usize,
    ) -> anyhow::Result<TraceDetail> {
        if !(1..=MAX_TRACE_TASK_PAGE_LIMIT).contains(&task_limit) {
            anyhow::bail!("task_limit 必须在 1..={MAX_TRACE_TASK_PAGE_LIMIT} 范围内");
        }
        self.recover_pending_claim_edit().await?;
        let trace = self
            .claim_store
            .read_trace(id)
            .await
            .with_context(|| format!("读取 trace {id} 失败"))?;
        if trace.agent != self.agent_id {
            anyhow::bail!("trace {id} 不属于当前 agent {}", self.agent_id);
        }
        let total = trace.task.chars().count();
        let task = trace
            .task
            .chars()
            .skip(task_offset)
            .take(task_limit)
            .collect();
        let returned_end = task_offset.saturating_add(task_limit).min(total);
        Ok(TraceDetail {
            id: trace.id,
            name: trace.name,
            agent: trace.agent,
            created_at: trace.created_at,
            input_claims: trace.input_claims,
            output_claims: trace.output_claims,
            task,
            task_offset,
            task_limit,
            task_omitted: total.saturating_sub(returned_end),
            next_task_offset: (returned_end < total).then_some(returned_end),
        })
    }

    async fn read_owned_claim(&self, id: &ClaimId) -> anyhow::Result<Claim> {
        let claim = self
            .claim_store
            .read_claim(id)
            .await
            .with_context(|| format!("读取 claim {id} 失败"))?;
        if claim.holder != self.agent_id {
            anyhow::bail!("claim {id} 不属于当前 agent {}", self.agent_id);
        }
        Ok(claim)
    }

    pub async fn recover_pending_claim_edit(&self) -> anyhow::Result<()> {
        if !self.team_services_configured() {
            return Ok(());
        }
        let pending_path =
            paths::agent_home_claim_edit_pending_path(self.maintainer_upload_queue.agent_home());
        if !tokio::fs::try_exists(&pending_path).await? {
            return Ok(());
        }
        let _guard = FileLockGuard::lock_exclusive(paths::agent_home_knowledge_apply_lock_path(
            self.maintainer_upload_queue.agent_home(),
        ))
        .await?;
        self.recover_pending_claim_edit_locked().await
    }

    /// 调用方必须已持有 `agent_home_knowledge_apply_lock_path` 的独占锁。
    pub async fn recover_pending_claim_edit_locked(&self) -> anyhow::Result<()> {
        if !self.team_services_configured() {
            return Ok(());
        }
        let path =
            paths::agent_home_claim_edit_pending_path(self.maintainer_upload_queue.agent_home());
        let pending: PendingClaimEdit = match read_yaml(&path).await {
            Ok(pending) => pending,
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        validate_shared_claim_text(&pending.target)?;
        if pending.target.holder != self.agent_id {
            anyhow::bail!(
                "pending claim edit {} 的 holder 不属于当前 agent {}",
                pending.target.id,
                self.agent_id
            );
        }
        let current = self.read_owned_claim(&pending.target.id).await?;
        let current_revision = claim_revision(&current)?;
        let target_revision = claim_revision(&pending.target)?;
        let staged = if current_revision == pending.expected_revision {
            self.claim_store.write_claim(&pending.target).await?;
            pending.target
        } else if current_revision == target_revision
            || current.effective_updated_at() > pending.target.effective_updated_at()
        {
            current
        } else {
            anyhow::bail!(
                "pending claim edit {} 与当前同新版本冲突，保留恢复记录等待处理",
                pending.target.id
            );
        };
        self.stage_maintainer_batch_with_durable_claims(vec![staged], Vec::new())
            .await?;
        clear_pending_claim_edit(self.maintainer_upload_queue.agent_home()).await
    }
}

fn validate_page_limit(limit: usize) -> anyhow::Result<()> {
    if !(1..=MAX_CLAIM_PAGE_LIMIT).contains(&limit) {
        anyhow::bail!("limit 必须在 1..={MAX_CLAIM_PAGE_LIMIT} 范围内");
    }
    Ok(())
}

fn validate_update(update: &ClaimUpdate) -> anyhow::Result<()> {
    if update.name.is_none()
        && update.statement.is_none()
        && update.scope.is_none()
        && update.evidence_summary.is_none()
        && update.confidence.is_none()
        && update.status.is_none()
    {
        anyhow::bail!("claim update 至少需要一个可编辑字段");
    }
    Ok(())
}

fn validate_shared_claim_text(claim: &Claim) -> anyhow::Result<()> {
    for (field, value) in [
        ("name", claim.name.as_str()),
        ("statement", claim.statement.as_str()),
        ("scope", claim.scope.as_str()),
        ("evidence_summary", claim.evidence_summary.as_str()),
    ] {
        if value.trim().is_empty() {
            anyhow::bail!("claim {field} 不得为空");
        }
        scan_memory_content(value).with_context(|| format!("claim {field} safety scan 失败"))?;
    }
    Ok(())
}

pub fn claim_revision(claim: &Claim) -> anyhow::Result<String> {
    let canonical = serde_json::to_string(claim)?;
    Ok(format!("sha256-v1:{}", crate::auth::sha256_hex(&canonical)))
}

async fn clear_pending_claim_edit(agent_home: &std::path::Path) -> anyhow::Result<()> {
    match tokio::fs::remove_file(paths::agent_home_claim_edit_pending_path(agent_home)).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn bounded_text(value: &str, limit: usize) -> BoundedText {
    let text: String = value.chars().take(limit).collect();
    BoundedText {
        truncated: value.chars().count() > limit,
        text,
    }
}

fn page_tail(offset: usize, returned: usize, total: usize) -> (usize, Option<usize>) {
    let returned_end = offset.saturating_add(returned).min(total);
    (
        total.saturating_sub(returned_end),
        (returned_end < total).then_some(returned_end),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tempfile::TempDir;

    use super::*;
    use crate::agent::fs::LocalFsClaimStore;
    use crate::agent::inbox::InboxJsonGenerator;
    use crate::agent::maintainer_upload::LocalFsMaintainerUploadQueue;
    use crate::agent::traits::{InboxReader, MemoryStore, ReportedDisputeClaimSetStore};
    use crate::api::{InboxInternalizeKind, InternalizeRequest, ProviderTransport};
    use crate::claim::{DisputeId, InboxId, InboxMessage};
    use crate::maintainer::traits::MaintainerClient;
    use crate::memory::{MemoryApplyReport, MemoryOp, MemorySnapshot};
    use crate::router::{AgentQuery, RouterClient, RouterQueryResult, ScopesOverviewSnapshot};

    struct UnusedInboxGenerator;
    #[async_trait]
    impl InboxJsonGenerator for UnusedInboxGenerator {
        async fn generate_json(
            &self,
            _kind: InboxInternalizeKind,
            _request: InternalizeRequest,
            _preferred_transport: Option<ProviderTransport>,
        ) -> anyhow::Result<serde_json::Value> {
            anyhow::bail!("unused")
        }
    }

    struct EmptyInbox;
    #[async_trait]
    impl InboxReader for EmptyInbox {
        async fn list_pending(&self) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(Vec::new())
        }
        async fn ack(&self, _msg_id: &InboxId) -> anyhow::Result<()> {
            Ok(())
        }
        async fn accept_pulled(&self, _msg: &InboxMessage) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct EmptyMemory;
    #[async_trait]
    impl MemoryStore for EmptyMemory {
        async fn read_memory(&self) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn read_user(&self) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn read_snapshot(&self) -> anyhow::Result<MemorySnapshot> {
            anyhow::bail!("unused")
        }
        async fn apply_ops(&self, _ops: &[MemoryOp]) -> anyhow::Result<MemoryApplyReport> {
            anyhow::bail!("unused")
        }
    }

    struct EmptyReported;
    #[async_trait]
    impl ReportedDisputeClaimSetStore for EmptyReported {
        async fn contains_claim_set(&self, _claims: &[ClaimId]) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn record_claim_set(
            &self,
            _claims: &[ClaimId],
            _dispute_id: &DisputeId,
            _reported_at: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct EmptyRouter;
    #[async_trait]
    impl RouterClient for EmptyRouter {
        async fn query(&self, _query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            anyhow::bail!("unused")
        }
        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            anyhow::bail!("unused")
        }
    }

    struct EmptyMaintainer;
    #[async_trait]
    impl MaintainerClient for EmptyMaintainer {
        async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(Vec::new())
        }
        async fn ack_inbox(&self, _agent_id: &AgentId, _ids: &[InboxId]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn upload_claim(&self, _claim: &Claim) -> anyhow::Result<()> {
            Ok(())
        }
        async fn report_dispute(&self, _dispute: &crate::claim::Dispute) -> anyhow::Result<()> {
            Ok(())
        }
    }

    pub(crate) fn runner(dir: &TempDir) -> Arc<AgentRunner> {
        let agent_id = AgentId::new("agent-a").unwrap();
        let home = dir.path().to_path_buf();
        Arc::new(AgentRunner::new_local(
            agent_id,
            Arc::new(UnusedInboxGenerator),
            Arc::new(LocalFsClaimStore::new(home.clone())),
            Arc::new(EmptyReported),
            Arc::new(EmptyInbox),
            Arc::new(EmptyMemory),
            Arc::new(LocalFsMaintainerUploadQueue::new(home)),
            0,
            Vec::new(),
        ))
    }

    fn team_runner(dir: &TempDir) -> Arc<AgentRunner> {
        let agent_id = AgentId::new("agent-a").unwrap();
        let home = dir.path().to_path_buf();
        Arc::new(AgentRunner::new(
            agent_id,
            Arc::new(UnusedInboxGenerator),
            Arc::new(LocalFsClaimStore::new(home.clone())),
            Arc::new(EmptyReported),
            Arc::new(EmptyInbox),
            Arc::new(EmptyMemory),
            Arc::new(EmptyRouter),
            Arc::new(EmptyMaintainer),
            Arc::new(LocalFsMaintainerUploadQueue::new(home)),
            0,
            Vec::new(),
        ))
    }

    fn sample_claim(id: ClaimId, holder: AgentId) -> Claim {
        Claim {
            id,
            name: "claim name".into(),
            statement: "statement".into(),
            scope: "scope".into(),
            holder,
            confidence: Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "evidence".into(),
        }
    }

    #[tokio::test]
    async fn concurrent_updates_with_same_revision_allow_only_one_writer() {
        let dir = TempDir::new().unwrap();
        let runner = runner(&dir);
        let claim = sample_claim(ClaimId::random(), runner.agent_id().clone());
        runner.claim_store.write_claim(&claim).await.unwrap();
        let revision = runner.read_claim(&claim.id).await.unwrap().revision;
        let update = |name: &str| ClaimUpdate {
            id: claim.id.clone(),
            expected_revision: revision.clone(),
            name: Some(name.into()),
            statement: None,
            scope: None,
            evidence_summary: None,
            confidence: None,
            status: None,
        };
        let (left, right) = tokio::join!(
            runner.update_claim(update("left")),
            runner.update_claim(update("right")),
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let stored = runner.read_claim(&claim.id).await.unwrap().claim;
        assert_eq!(stored.holder, claim.holder);
        assert_eq!(stored.created_at, claim.created_at);
        assert_eq!(stored.source_claim_ids, claim.source_claim_ids);
        assert!(stored.updated_at.unwrap() > claim.created_at);
        assert!(
            !tokio::fs::try_exists(paths::agent_home_claim_edit_pending_path(dir.path()))
                .await
                .unwrap()
        );
        assert!(
            !tokio::fs::try_exists(paths::agent_home_pending_maintainer_uploads_path(
                dir.path()
            ))
            .await
            .unwrap()
        );

        let detail = runner.read_claim(&claim.id).await.unwrap();
        let invalid = ClaimUpdate {
            id: claim.id.clone(),
            expected_revision: detail.revision.clone(),
            name: Some("   ".into()),
            statement: None,
            scope: None,
            evidence_summary: None,
            confidence: None,
            status: None,
        };
        assert!(runner.update_claim(invalid).await.is_err());
        assert_eq!(
            runner.read_claim(&claim.id).await.unwrap().revision,
            detail.revision
        );
    }

    #[tokio::test]
    async fn trace_task_is_paginated_by_chars_and_filter_finds_output_relation() {
        let dir = TempDir::new().unwrap();
        let runner = runner(&dir);
        let claim_id = ClaimId::random();
        let trace = crate::claim::Trace {
            id: TraceId::random(),
            name: "trace".into(),
            task: "甲乙丙丁".into(),
            agent: runner.agent_id().clone(),
            input_claims: Vec::new(),
            output_claims: vec![claim_id.clone()],
            created_at: "2026-01-02T00:00:00Z".parse().unwrap(),
        };
        runner.claim_store.write_trace(&trace).await.unwrap();
        let page = runner.list_traces(Some(&claim_id), 0, 20).await.unwrap();
        assert_eq!(page.items.len(), 1);
        let detail = runner.read_trace(&trace.id, 1, 2).await.unwrap();
        assert_eq!(detail.task, "乙丙");
        assert_eq!(detail.next_task_offset, Some(3));
        assert_eq!(detail.task_omitted, 1);
    }

    #[tokio::test]
    async fn read_without_pending_edit_does_not_wait_for_knowledge_lock() {
        let dir = TempDir::new().unwrap();
        let runner = runner(&dir);
        let claim = sample_claim(ClaimId::random(), runner.agent_id().clone());
        runner.claim_store.write_claim(&claim).await.unwrap();
        let _guard =
            FileLockGuard::lock_exclusive(paths::agent_home_knowledge_apply_lock_path(dir.path()))
                .await
                .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            runner.read_claim(&claim.id),
        )
        .await
        .expect("没有 pending edit 时只读不得等待 knowledge lock")
        .unwrap();
    }

    #[tokio::test]
    async fn pending_edit_recovery_fills_local_and_queue_gap_without_overwriting_newer_claim() {
        let dir = TempDir::new().unwrap();
        let runner = team_runner(&dir);
        let original = sample_claim(ClaimId::random(), runner.agent_id().clone());
        runner.claim_store.write_claim(&original).await.unwrap();
        let mut target = original.clone();
        target.name = "target".into();
        target.updated_at = Some(original.created_at + Duration::seconds(1));
        let pending = PendingClaimEdit {
            expected_revision: claim_revision(&original).unwrap(),
            target: target.clone(),
        };
        write_yaml_atomic(
            &paths::agent_home_claim_edit_pending_path(dir.path()),
            &pending,
        )
        .await
        .unwrap();

        runner.recover_pending_claim_edit().await.unwrap();
        assert_eq!(
            runner.claim_store.read_claim(&original.id).await.unwrap(),
            target
        );
        let queued: crate::agent::maintainer_upload::PendingMaintainerUploads = read_yaml(
            &paths::agent_home_pending_maintainer_uploads_path(dir.path()),
        )
        .await
        .unwrap();
        assert_eq!(queued.claims, vec![target.clone()]);
        assert!(queued.durable_claim_ids.contains(&target.id));

        // 模拟本地 target 已写、尚未 stage 就崩溃。
        tokio::fs::remove_file(paths::agent_home_pending_maintainer_uploads_path(
            dir.path(),
        ))
        .await
        .unwrap();
        write_yaml_atomic(
            &paths::agent_home_claim_edit_pending_path(dir.path()),
            &pending,
        )
        .await
        .unwrap();
        runner.recover_pending_claim_edit().await.unwrap();
        let queued: crate::agent::maintainer_upload::PendingMaintainerUploads = read_yaml(
            &paths::agent_home_pending_maintainer_uploads_path(dir.path()),
        )
        .await
        .unwrap();
        assert_eq!(queued.claims, vec![target.clone()]);

        // 模拟 durable stage 已完成、尚未清 pending 就崩溃；重放不得重复条目。
        write_yaml_atomic(
            &paths::agent_home_claim_edit_pending_path(dir.path()),
            &pending,
        )
        .await
        .unwrap();
        runner.recover_pending_claim_edit().await.unwrap();
        let queued: crate::agent::maintainer_upload::PendingMaintainerUploads = read_yaml(
            &paths::agent_home_pending_maintainer_uploads_path(dir.path()),
        )
        .await
        .unwrap();
        assert_eq!(queued.claims, vec![target.clone()]);

        let mut newer = target.clone();
        newer.name = "newer".into();
        newer.updated_at = Some(target.effective_updated_at() + Duration::seconds(1));
        runner.claim_store.write_claim(&newer).await.unwrap();
        write_yaml_atomic(
            &paths::agent_home_claim_edit_pending_path(dir.path()),
            &pending,
        )
        .await
        .unwrap();
        runner.recover_pending_claim_edit().await.unwrap();
        assert_eq!(
            runner.claim_store.read_claim(&original.id).await.unwrap(),
            newer
        );
        let queued: crate::agent::maintainer_upload::PendingMaintainerUploads = read_yaml(
            &paths::agent_home_pending_maintainer_uploads_path(dir.path()),
        )
        .await
        .unwrap();
        assert_eq!(queued.claims, vec![newer]);
        assert!(
            !tokio::fs::try_exists(paths::agent_home_claim_edit_pending_path(dir.path()))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn team_pending_survives_solo_reads_and_recovers_after_team_returns() {
        for local_target_written in [false, true] {
            let dir = TempDir::new().unwrap();
            let team = team_runner(&dir);
            let solo = runner(&dir);
            let original = sample_claim(ClaimId::random(), team.agent_id().clone());
            team.claim_store.write_claim(&original).await.unwrap();
            let mut target = original.clone();
            target.name = "target".into();
            target.updated_at = Some(original.created_at + Duration::seconds(1));
            let pending = PendingClaimEdit {
                expected_revision: claim_revision(&original).unwrap(),
                target: target.clone(),
            };
            write_yaml_atomic(
                &paths::agent_home_claim_edit_pending_path(dir.path()),
                &pending,
            )
            .await
            .unwrap();
            if local_target_written {
                team.claim_store.write_claim(&target).await.unwrap();
            }

            let knowledge_guard = FileLockGuard::lock_exclusive(
                paths::agent_home_knowledge_apply_lock_path(dir.path()),
            )
            .await
            .unwrap();
            let solo_detail = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                solo.read_claim(&original.id),
            )
            .await
            .expect("solo 读取历史 team pending 不得等待 knowledge lock")
            .unwrap();
            assert_eq!(
                solo_detail.claim,
                if local_target_written {
                    target.clone()
                } else {
                    original.clone()
                }
            );
            drop(knowledge_guard);
            assert!(
                tokio::fs::try_exists(paths::agent_home_claim_edit_pending_path(dir.path()))
                    .await
                    .unwrap()
            );
            assert!(
                !tokio::fs::try_exists(paths::agent_home_pending_maintainer_uploads_path(
                    dir.path()
                ))
                .await
                .unwrap()
            );

            team.recover_pending_claim_edit().await.unwrap();
            assert_eq!(
                team.claim_store.read_claim(&original.id).await.unwrap(),
                target
            );
            let queued: crate::agent::maintainer_upload::PendingMaintainerUploads = read_yaml(
                &paths::agent_home_pending_maintainer_uploads_path(dir.path()),
            )
            .await
            .unwrap();
            assert_eq!(queued.claims.len(), 1);
            assert_eq!(queued.claims[0].id, original.id);
            assert!(queued.durable_claim_ids.contains(&original.id));
            assert!(
                !tokio::fs::try_exists(paths::agent_home_claim_edit_pending_path(dir.path()))
                    .await
                    .unwrap()
            );
        }
    }
}
