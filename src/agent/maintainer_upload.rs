//! maintainer 上传补偿队列。
//!
//! 本模块只处理 agent 本地 pending 文件和上传重试编排：
//! claims/disputes 先落本地业务文件，再尽力同步 maintainer。暂时性失败保留在
//! `<agent_home>/maintainer_uploads/pending.yaml`，下次上传触发时一起补传。
//! Claim 的鉴权失败不进入自动重试队列，因为本地 claim 文件仍是权威数据源；
//! Dispute 没有独立本地实体文件，鉴权失败也必须保留 pending，避免治理记录丢失。

use std::collections::BTreeMap;
use std::path::PathBuf;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::sync::Mutex;

use super::runner::AgentRunner;
use crate::claim::{Claim, ClaimId, Dispute, DisputeId};
use crate::maintainer::traits::MaintainerClientError;
use crate::storage::{paths, read_yaml, write_yaml_atomic, StorageError};

const MAINTAINER_UPLOAD_MAX_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PendingMaintainerUploads {
    #[serde(default)]
    pub claims: Vec<Claim>,
    #[serde(default)]
    pub disputes: Vec<Dispute>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct MaintainerUploadReport {
    pub pending_claims: usize,
    pub pending_disputes: usize,
    pub warning: Option<String>,
}

pub(crate) struct LocalFsMaintainerUploadQueue {
    path: PathBuf,
    lock_path: PathBuf,
    lock: Mutex<()>,
}

impl LocalFsMaintainerUploadQueue {
    pub fn new(agent_home: PathBuf) -> Self {
        Self {
            path: paths::agent_home_pending_maintainer_uploads_path(&agent_home),
            lock_path: paths::agent_home_pending_maintainer_uploads_lock_path(&agent_home),
            lock: Mutex::new(()),
        }
    }

    async fn read(&self) -> anyhow::Result<PendingMaintainerUploads> {
        match read_yaml(&self.path).await {
            Ok(file) => Ok(file),
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(PendingMaintainerUploads::default())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn write_or_clear(&self, pending: &PendingMaintainerUploads) -> anyhow::Result<()> {
        if pending.claims.is_empty() && pending.disputes.is_empty() {
            match fs::remove_file(&self.path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(Into::into(err)),
            }
            return Ok(());
        }
        write_yaml_atomic(&self.path, pending).await?;
        Ok(())
    }

    async fn merged_with(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
    ) -> anyhow::Result<PendingMaintainerUploads> {
        let pending = self.read().await?;
        Ok(merge_pending_uploads(pending, claims, disputes))
    }
}

impl AgentRunner {
    pub(super) async fn upload_maintainer_batch(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
    ) -> anyhow::Result<MaintainerUploadReport> {
        let Some(maintainer_client) = self.maintainer_client.clone() else {
            return Ok(MaintainerUploadReport::default());
        };
        let attempted = {
            let _guard = self.maintainer_upload_queue.lock.lock().await;
            let _file_guard = crate::storage::FileLockGuard::lock_exclusive(
                &self.maintainer_upload_queue.lock_path,
            )
            .await?;
            let merged = self
                .maintainer_upload_queue
                .merged_with(claims, disputes)
                .await?;
            if merged.claims.is_empty() && merged.disputes.is_empty() {
                return Ok(MaintainerUploadReport::default());
            }
            self.maintainer_upload_queue.write_or_clear(&merged).await?;
            merged
        };

        if attempted.claims.is_empty() && attempted.disputes.is_empty() {
            return Ok(MaintainerUploadReport::default());
        }

        let mut failed = PendingMaintainerUploads::default();
        let mut timeout_secs: Option<u64> = None;
        let mut timed_out = false;
        let mut auth_failures = 0usize;
        let mut forbidden_failures = 0usize;
        let mut forbidden_sample: Option<String> = None;
        let mut retryable_failures = 0usize;
        let mut rejected_failures = 0usize;
        let mut rejected_sample: Option<String> = None;

        let mut claim_results =
            futures::stream::iter(attempted.claims.clone().into_iter().map(|claim| {
                let maintainer = maintainer_client.clone();
                async move {
                    let result = maintainer.upload_claim(&claim).await;
                    (claim, result)
                }
            }))
            .buffer_unordered(MAINTAINER_UPLOAD_MAX_CONCURRENCY);
        while let Some((claim, result)) = claim_results.next().await {
            if let Err(err) = result {
                match classify_upload_error(&err) {
                    UploadErrorKind::Retryable => {
                        timeout_secs = timeout_secs.or_else(|| upload_error_timeout_secs(&err));
                        timed_out |= upload_error_timed_out(&err);
                        retryable_failures += 1;
                        failed.claims.push(claim);
                    }
                    UploadErrorKind::Auth => {
                        auth_failures += 1;
                    }
                    UploadErrorKind::Forbidden => {
                        forbidden_failures += 1;
                        forbidden_sample.get_or_insert_with(|| err.to_string());
                    }
                    // 其他 client/未知错误继续保留本地待传并记 warning，绝不中断会话。
                    UploadErrorKind::Client | UploadErrorKind::Unknown => {
                        rejected_failures += 1;
                        rejected_sample.get_or_insert_with(|| err.to_string());
                        failed.claims.push(claim);
                    }
                }
            }
        }

        let mut dispute_results =
            futures::stream::iter(attempted.disputes.clone().into_iter().map(|dispute| {
                let maintainer = maintainer_client.clone();
                async move {
                    let result = maintainer.report_dispute(&dispute).await;
                    (dispute, result)
                }
            }))
            .buffer_unordered(MAINTAINER_UPLOAD_MAX_CONCURRENCY);
        while let Some((dispute, result)) = dispute_results.next().await {
            if let Err(err) = result {
                match classify_upload_error(&err) {
                    UploadErrorKind::Retryable => {
                        timeout_secs = timeout_secs.or_else(|| upload_error_timeout_secs(&err));
                        timed_out |= upload_error_timed_out(&err);
                        retryable_failures += 1;
                        failed.disputes.push(dispute);
                    }
                    UploadErrorKind::Auth => {
                        auth_failures += 1;
                        failed.disputes.push(dispute);
                    }
                    UploadErrorKind::Forbidden => {
                        forbidden_failures += 1;
                        forbidden_sample.get_or_insert_with(|| err.to_string());
                        failed.disputes.push(dispute);
                    }
                    // 其他 client/未知错误继续保留本地待传并记 warning，绝不中断会话。
                    UploadErrorKind::Client | UploadErrorKind::Unknown => {
                        rejected_failures += 1;
                        rejected_sample.get_or_insert_with(|| err.to_string());
                        failed.disputes.push(dispute);
                    }
                }
            }
        }

        {
            let _guard = self.maintainer_upload_queue.lock.lock().await;
            let _file_guard = crate::storage::FileLockGuard::lock_exclusive(
                &self.maintainer_upload_queue.lock_path,
            )
            .await?;
            let current = self.maintainer_upload_queue.read().await?;
            let next = reconcile_pending_uploads_after_attempt(current, &attempted, &failed);
            self.maintainer_upload_queue.write_or_clear(&next).await?;
        }

        let mut report = MaintainerUploadReport {
            pending_claims: failed.claims.len(),
            pending_disputes: failed.disputes.len(),
            warning: None,
        };
        // 任何上传/上报失败都只降级为 warning，绝不向调用方返回 Err 中断会话。
        let mut warnings: Vec<String> = Vec::new();
        if auth_failures > 0 {
            warnings.push(format!(
                "Maintainer upload unauthorized for {auth_failures} items. Team sync was skipped; fix current upstream acn_key_env before retrying from local source."
            ));
        }
        if forbidden_failures > 0 {
            let detail = forbidden_sample.as_deref().unwrap_or("forbidden");
            warnings.push(format!(
                "Maintainer rejected {forbidden_failures} items as forbidden ({detail}). Team sync was skipped; fix object ownership before retrying from local source."
            ));
        }
        if rejected_failures > 0 {
            let detail = rejected_sample.as_deref().unwrap_or("client error");
            warnings.push(format!(
                "Maintainer rejected {rejected_failures} items ({detail}). Local data remains pending; team sync was skipped."
            ));
        }
        if retryable_failures > 0 {
            let timeout = timeout_secs
                .map(|secs| format!("{secs}s"))
                .unwrap_or_else(|| "configured timeout".to_string());
            warnings.push(if timed_out {
                format!(
                    "Upload timeout after {timeout} with {retryable_failures} items. Will retry in next connection."
                )
            } else {
                format!(
                    "Upload retryable failure (timeout {timeout}) with {retryable_failures} items. Will retry in next connection."
                )
            });
        }
        if !warnings.is_empty() {
            let warning = warnings.join(" / ");
            log::warn!(target: "agent", "{warning}");
            report.warning = Some(warning);
        }
        Ok(report)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadErrorKind {
    Auth,
    Forbidden,
    Retryable,
    Client,
    Unknown,
}

fn classify_upload_error(err: &anyhow::Error) -> UploadErrorKind {
    match err.downcast_ref::<MaintainerClientError>() {
        Some(err) if err.is_auth() => UploadErrorKind::Auth,
        Some(err) if err.is_retryable() => UploadErrorKind::Retryable,
        Some(MaintainerClientError::Client { status: 403, .. }) => UploadErrorKind::Forbidden,
        Some(MaintainerClientError::Client { .. }) => UploadErrorKind::Client,
        Some(_) | None => UploadErrorKind::Unknown,
    }
}

fn upload_error_timeout_secs(err: &anyhow::Error) -> Option<u64> {
    err.downcast_ref::<MaintainerClientError>()
        .and_then(MaintainerClientError::timeout_secs)
}

fn upload_error_timed_out(err: &anyhow::Error) -> bool {
    err.downcast_ref::<MaintainerClientError>()
        .is_some_and(MaintainerClientError::timed_out)
}

fn merge_pending_uploads(
    pending: PendingMaintainerUploads,
    claims: Vec<Claim>,
    disputes: Vec<Dispute>,
) -> PendingMaintainerUploads {
    let mut claims_by_id: BTreeMap<ClaimId, Claim> = BTreeMap::new();
    for claim in pending.claims {
        claims_by_id.insert(claim.id.clone(), claim);
    }
    for claim in claims {
        let should_replace = claims_by_id
            .get(&claim.id)
            .is_none_or(|existing| claim.effective_updated_at() >= existing.effective_updated_at());
        if should_replace {
            claims_by_id.insert(claim.id.clone(), claim);
        }
    }

    let mut disputes_by_id: BTreeMap<DisputeId, Dispute> = BTreeMap::new();
    for dispute in pending.disputes {
        disputes_by_id.insert(dispute.id.clone(), dispute);
    }
    for dispute in disputes {
        let should_replace = disputes_by_id
            .get(&dispute.id)
            .is_none_or(|existing| dispute.created_at >= existing.created_at);
        if should_replace {
            disputes_by_id.insert(dispute.id.clone(), dispute);
        }
    }

    PendingMaintainerUploads {
        claims: claims_by_id.into_values().collect(),
        disputes: disputes_by_id.into_values().collect(),
    }
}

fn reconcile_pending_uploads_after_attempt(
    current: PendingMaintainerUploads,
    attempted: &PendingMaintainerUploads,
    failed: &PendingMaintainerUploads,
) -> PendingMaintainerUploads {
    let failed_claims = failed
        .claims
        .iter()
        .map(|claim| (claim.id.clone(), claim))
        .collect::<BTreeMap<_, _>>();
    let failed_disputes = failed
        .disputes
        .iter()
        .map(|dispute| (dispute.id.clone(), dispute))
        .collect::<BTreeMap<_, _>>();

    let mut claims_by_id = current
        .claims
        .into_iter()
        .map(|claim| (claim.id.clone(), claim))
        .collect::<BTreeMap<_, _>>();
    for claim in &attempted.claims {
        if failed_claims.contains_key(&claim.id) {
            if claims_by_id
                .get(&claim.id)
                .is_none_or(|current| current == claim)
            {
                claims_by_id.insert(claim.id.clone(), claim.clone());
            }
            continue;
        }
        if claims_by_id
            .get(&claim.id)
            .is_some_and(|current| current == claim)
        {
            claims_by_id.remove(&claim.id);
        }
    }

    let mut disputes_by_id = current
        .disputes
        .into_iter()
        .map(|dispute| (dispute.id.clone(), dispute))
        .collect::<BTreeMap<_, _>>();
    for dispute in &attempted.disputes {
        if failed_disputes.contains_key(&dispute.id) {
            if disputes_by_id
                .get(&dispute.id)
                .is_none_or(|current| current == dispute)
            {
                disputes_by_id.insert(dispute.id.clone(), dispute.clone());
            }
            continue;
        }
        if disputes_by_id
            .get(&dispute.id)
            .is_some_and(|current| current == dispute)
        {
            disputes_by_id.remove(&dispute.id);
        }
    }

    PendingMaintainerUploads {
        claims: claims_by_id.into_values().collect(),
        disputes: disputes_by_id.into_values().collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::agent::fs::{
        LocalFsClaimStore, LocalFsInboxReader, LocalFsMemoryStore,
        LocalFsReportedDisputeClaimSetStore,
    };
    use crate::agent::inbox::InboxJsonGenerator;
    use crate::agent::traits::{
        InboxReader, LocalClaimStore, MemoryStore, ReportedDisputeClaimSetStore,
    };
    use crate::api::{InboxInternalizeKind, InternalizeRequest};
    use crate::claim::{AgentId, ClaimStatus, Confidence, DisputeStatus};
    use crate::router::{AgentQuery, RouterClient, RouterQueryResult, ScopesOverviewSnapshot};
    use crate::skill::SkillSummary;
    use chrono::{DateTime, Utc};

    #[derive(Default)]
    struct TestMaintainerClient {
        retryable_claims: Mutex<BTreeSet<String>>,
        auth_error_claims: Mutex<BTreeSet<String>>,
        forbidden_claims: Mutex<BTreeSet<String>>,
        auth_error_disputes: Mutex<BTreeSet<String>>,
        forbidden_disputes: Mutex<BTreeSet<String>>,
        client_error_claims: Mutex<BTreeSet<String>>,
        unknown_error_claims: Mutex<BTreeSet<String>>,
        uploaded_claims: Mutex<Vec<String>>,
        uploaded_disputes: Mutex<Vec<String>>,
        pending_path_to_observe: Mutex<Option<PathBuf>>,
        observed_write_ahead: Mutex<bool>,
        pull_retryable: Mutex<bool>,
        pull_auth: Mutex<bool>,
        pull_forbidden: Mutex<bool>,
    }

    #[async_trait]
    impl crate::maintainer::traits::MaintainerClient for TestMaintainerClient {
        async fn pull_inbox(
            &self,
            _agent_id: &AgentId,
        ) -> anyhow::Result<Vec<crate::claim::InboxMessage>> {
            if *self.pull_auth.lock().unwrap() {
                return Err(MaintainerClientError::Auth {
                    operation: "POST /inbox/pull".into(),
                    status: 401,
                }
                .into());
            }
            if *self.pull_forbidden.lock().unwrap() {
                return Err(MaintainerClientError::Client {
                    operation: "POST /inbox/pull".into(),
                    status: 403,
                    body: "forbidden".into(),
                }
                .into());
            }
            if *self.pull_retryable.lock().unwrap() {
                return Err(MaintainerClientError::Retryable {
                    operation: "POST /inbox/pull".into(),
                    timeout_secs: 7,
                    timed_out: true,
                    message: "request deadline exceeded".into(),
                }
                .into());
            }
            Ok(vec![])
        }

        async fn ack_inbox(
            &self,
            _agent_id: &AgentId,
            _inbox_ids: &[crate::claim::InboxId],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn upload_claim(&self, claim: &Claim) -> anyhow::Result<()> {
            let id = claim.id.to_string();
            let pending_path = self.pending_path_to_observe.lock().unwrap().clone();
            if let Some(path) = pending_path {
                if fs::try_exists(path).await.unwrap_or(false) {
                    *self.observed_write_ahead.lock().unwrap() = true;
                }
            }
            if self.retryable_claims.lock().unwrap().contains(&id) {
                return Err(MaintainerClientError::Retryable {
                    operation: "claims/upload".into(),
                    timeout_secs: 7,
                    timed_out: true,
                    message: "timeout".into(),
                }
                .into());
            }
            if self.auth_error_claims.lock().unwrap().contains(&id) {
                return Err(MaintainerClientError::Auth {
                    operation: "claims/upload".into(),
                    status: 401,
                }
                .into());
            }
            if self.forbidden_claims.lock().unwrap().contains(&id) {
                return Err(MaintainerClientError::Client {
                    operation: "claims/upload".into(),
                    status: 403,
                    body: "forbidden".into(),
                }
                .into());
            }
            if self.client_error_claims.lock().unwrap().contains(&id) {
                return Err(MaintainerClientError::Client {
                    operation: "claims/upload".into(),
                    status: 400,
                    body: "bad claim".into(),
                }
                .into());
            }
            if self.unknown_error_claims.lock().unwrap().contains(&id) {
                return Err(anyhow::anyhow!("unknown upload failure"));
            }
            self.uploaded_claims.lock().unwrap().push(id);
            Ok(())
        }

        async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<()> {
            let id = dispute.id.to_string();
            if self.auth_error_disputes.lock().unwrap().contains(&id) {
                return Err(MaintainerClientError::Auth {
                    operation: "disputes/report".into(),
                    status: 401,
                }
                .into());
            }
            if self.forbidden_disputes.lock().unwrap().contains(&id) {
                return Err(MaintainerClientError::Client {
                    operation: "disputes/report".into(),
                    status: 403,
                    body: "forbidden".into(),
                }
                .into());
            }
            self.uploaded_disputes.lock().unwrap().push(id);
            Ok(())
        }
    }

    struct EmptyRouterClient;

    #[async_trait]
    impl RouterClient for EmptyRouterClient {
        async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            Ok(RouterQueryResult {
                candidate_claims: vec![],
                disputes: vec![],
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            Ok(ScopesOverviewSnapshot { scopes: vec![] })
        }
    }

    struct NoopInboxGenerator;

    #[async_trait]
    impl InboxJsonGenerator for NoopInboxGenerator {
        async fn generate_json(
            &self,
            _kind: InboxInternalizeKind,
            _request: InternalizeRequest,
            _preferred_transport: Option<crate::api::ProviderTransport>,
        ) -> anyhow::Result<serde_json::Value> {
            Ok(serde_json::json!({}))
        }
    }

    fn build_runner(dir: &tempfile::TempDir, maintainer: Arc<TestMaintainerClient>) -> AgentRunner {
        let claim_store: Arc<dyn LocalClaimStore> =
            Arc::new(LocalFsClaimStore::new(dir.path().to_path_buf()));
        let reported_store: Arc<dyn ReportedDisputeClaimSetStore> = Arc::new(
            LocalFsReportedDisputeClaimSetStore::new(dir.path().to_path_buf()),
        );
        let inbox: Arc<dyn InboxReader> =
            Arc::new(LocalFsInboxReader::new(dir.path().to_path_buf()));
        let memory_store: Arc<dyn MemoryStore> = Arc::new(LocalFsMemoryStore::new(
            dir.path().to_path_buf(),
            1600,
            1000,
            false,
        ));
        AgentRunner::new(
            AgentId::new("agent-a").unwrap(),
            Arc::new(NoopInboxGenerator),
            claim_store,
            reported_store,
            inbox,
            memory_store,
            Arc::new(EmptyRouterClient),
            maintainer,
            Arc::new(LocalFsMaintainerUploadQueue::new(dir.path().to_path_buf())),
            0,
            Vec::<SkillSummary>::new(),
        )
    }

    fn build_local_runner(dir: &tempfile::TempDir) -> AgentRunner {
        let claim_store: Arc<dyn LocalClaimStore> =
            Arc::new(LocalFsClaimStore::new(dir.path().to_path_buf()));
        let reported_store: Arc<dyn ReportedDisputeClaimSetStore> = Arc::new(
            LocalFsReportedDisputeClaimSetStore::new(dir.path().to_path_buf()),
        );
        let inbox: Arc<dyn InboxReader> =
            Arc::new(LocalFsInboxReader::new(dir.path().to_path_buf()));
        let memory_store: Arc<dyn MemoryStore> = Arc::new(LocalFsMemoryStore::new(
            dir.path().to_path_buf(),
            1600,
            1000,
            false,
        ));
        AgentRunner::new_local(
            AgentId::new("agent-a").unwrap(),
            Arc::new(NoopInboxGenerator),
            claim_store,
            reported_store,
            inbox,
            memory_store,
            Arc::new(LocalFsMaintainerUploadQueue::new(dir.path().to_path_buf())),
            0,
            Vec::<SkillSummary>::new(),
        )
    }

    fn claim(id: &str, status: ClaimStatus, created_at: &str) -> Claim {
        Claim {
            id: id.parse().unwrap(),
            name: "claim".into(),
            statement: "statement".into(),
            scope: "scope".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: Confidence::High,
            status,
            created_at: created_at.parse().unwrap(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "evidence".into(),
        }
    }

    fn dispute(id: &str, created_at: &str) -> Dispute {
        Dispute {
            id: id.parse().unwrap(),
            name: "dispute".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![
                "claim_11111111".parse().unwrap(),
                "claim_22222222".parse().unwrap(),
            ],
            summary: "summary".into(),
            status: DisputeStatus::Open,
            created_at: created_at.parse().unwrap(),
            resolved_at: None,
        }
    }

    #[test]
    fn merge_pending_uploads_keeps_newer_claim_and_dispute() {
        let pending = PendingMaintainerUploads {
            claims: vec![claim(
                "claim_11111111",
                ClaimStatus::Active,
                "2026-06-22T00:00:00Z",
            )],
            disputes: vec![dispute("dispute_11111111", "2026-06-22T00:00:00Z")],
        };

        let merged = merge_pending_uploads(
            pending,
            vec![claim(
                "claim_11111111",
                ClaimStatus::Deprecated,
                "2026-06-22T00:00:00Z",
            )],
            vec![dispute("dispute_11111111", "2026-06-23T00:00:00Z")],
        );

        assert_eq!(merged.claims.len(), 1);
        assert_eq!(merged.claims[0].status, ClaimStatus::Deprecated);
        assert_eq!(merged.disputes.len(), 1);
        assert_eq!(
            merged.disputes[0].created_at,
            "2026-06-23T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn merge_pending_uploads_compares_claim_updated_at() {
        let mut pending_claim = claim(
            "claim_11111111",
            ClaimStatus::Active,
            "2026-06-20T00:00:00Z",
        );
        pending_claim.updated_at = Some("2026-06-23T00:00:00Z".parse().unwrap());
        let mut older_update = claim(
            "claim_11111111",
            ClaimStatus::Deprecated,
            "2026-06-20T00:00:00Z",
        );
        older_update.updated_at = Some("2026-06-22T00:00:00Z".parse().unwrap());

        let merged = merge_pending_uploads(
            PendingMaintainerUploads {
                claims: vec![pending_claim],
                disputes: Vec::new(),
            },
            vec![older_update],
            Vec::new(),
        );

        assert_eq!(merged.claims[0].status, ClaimStatus::Active);
        assert_eq!(
            merged.claims[0].updated_at,
            Some("2026-06-23T00:00:00Z".parse().unwrap())
        );
    }

    #[test]
    fn reconcile_pending_uploads_preserves_concurrent_newer_items() {
        let attempted = PendingMaintainerUploads {
            claims: vec![claim(
                "claim_11111111",
                ClaimStatus::Active,
                "2026-06-22T00:00:00Z",
            )],
            disputes: vec![dispute("dispute_11111111", "2026-06-22T00:00:00Z")],
        };
        let current = PendingMaintainerUploads {
            claims: vec![
                claim(
                    "claim_11111111",
                    ClaimStatus::Deprecated,
                    "2026-06-23T00:00:00Z",
                ),
                claim(
                    "claim_22222222",
                    ClaimStatus::Active,
                    "2026-06-22T00:00:00Z",
                ),
            ],
            disputes: vec![dispute("dispute_22222222", "2026-06-22T00:00:00Z")],
        };

        let next = reconcile_pending_uploads_after_attempt(
            current,
            &attempted,
            &PendingMaintainerUploads::default(),
        );

        assert_eq!(
            next.claims
                .iter()
                .map(|claim| claim.id.to_string())
                .collect::<Vec<_>>(),
            vec!["claim_11111111", "claim_22222222"]
        );
        assert_eq!(
            next.disputes
                .iter()
                .map(|dispute| dispute.id.to_string())
                .collect::<Vec<_>>(),
            vec!["dispute_22222222"]
        );
    }

    #[tokio::test]
    async fn solo_mode_does_not_create_maintainer_pending_queue() {
        let dir = tempfile::tempdir().unwrap();
        let runner = build_local_runner(&dir);
        let local_claim = claim(
            "claim_77777777",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );

        let report = runner
            .upload_maintainer_batch(
                vec![local_claim],
                vec![dispute("dispute_77777777", "2026-06-22T00:00:00Z")],
            )
            .await
            .unwrap();

        assert_eq!(report, MaintainerUploadReport::default());
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_keeps_retryable_failures_and_clears_after_success() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_11111111",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        maintainer
            .retryable_claims
            .lock()
            .unwrap()
            .insert(pending_claim.id.to_string());

        let report = runner
            .upload_maintainer_batch(vec![pending_claim.clone()], Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 1);
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        let pending: PendingMaintainerUploads = read_yaml(&path).await.unwrap();
        assert_eq!(pending.claims, vec![pending_claim.clone()]);

        maintainer.retryable_claims.lock().unwrap().clear();
        let report = runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 0);
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_writes_pending_before_network_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_66666666",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        *maintainer.pending_path_to_observe.lock().unwrap() = Some(path.clone());

        let report = runner
            .upload_maintainer_batch(vec![pending_claim], Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 0);
        assert!(*maintainer.observed_write_ahead.lock().unwrap());
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_keeps_client_error_pending_without_breaking_session() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let bad_claim = claim(
            "claim_33333333",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        maintainer
            .client_error_claims
            .lock()
            .unwrap()
            .insert(bad_claim.id.to_string());

        // 400 这类普通 client error 仍保留待传，避免本地数据静默丢失。
        let report = runner
            .upload_maintainer_batch(vec![bad_claim.clone()], Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 1);
        assert!(report.warning.unwrap().contains("rejected"));
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        let pending: PendingMaintainerUploads = read_yaml(&path).await.unwrap();
        assert_eq!(pending.claims, vec![bad_claim]);
    }

    #[tokio::test]
    async fn upload_batch_drops_forbidden_failures_from_retry_queue_without_error() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let forbidden_claim = claim(
            "claim_55555555",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        maintainer
            .forbidden_claims
            .lock()
            .unwrap()
            .insert(forbidden_claim.id.to_string());

        let report = runner
            .upload_maintainer_batch(vec![forbidden_claim], Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 0);
        assert!(report.warning.unwrap().contains("forbidden"));
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_drops_auth_failures_from_retry_queue_without_error() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_44444444",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        maintainer
            .auth_error_claims
            .lock()
            .unwrap()
            .insert(pending_claim.id.to_string());

        let report = runner
            .upload_maintainer_batch(vec![pending_claim.clone()], Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 0);
        assert!(report.warning.unwrap().contains("unauthorized"));
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_keeps_auth_rejected_disputes_pending_until_identity_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let unauthorized = dispute("dispute_33333333", "2026-06-22T00:00:00Z");
        let forbidden = dispute("dispute_44444444", "2026-06-22T00:00:00Z");
        maintainer
            .auth_error_disputes
            .lock()
            .unwrap()
            .insert(unauthorized.id.to_string());
        maintainer
            .forbidden_disputes
            .lock()
            .unwrap()
            .insert(forbidden.id.to_string());

        let report = runner
            .upload_maintainer_batch(Vec::new(), vec![unauthorized.clone(), forbidden.clone()])
            .await
            .unwrap();

        assert_eq!(report.pending_disputes, 2);
        let warning = report.warning.unwrap();
        assert!(warning.contains("unauthorized"));
        assert!(warning.contains("forbidden"));
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        let pending: PendingMaintainerUploads = read_yaml(&path).await.unwrap();
        assert_eq!(
            pending.disputes,
            vec![unauthorized.clone(), forbidden.clone()]
        );

        maintainer.auth_error_disputes.lock().unwrap().clear();
        maintainer.forbidden_disputes.lock().unwrap().clear();
        let report = runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_disputes, 0);
        assert!(!fs::try_exists(path).await.unwrap());
        let uploaded = maintainer
            .uploaded_disputes
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            uploaded,
            BTreeSet::from([unauthorized.id.to_string(), forbidden.id.to_string()])
        );
    }

    #[tokio::test]
    async fn upload_batch_keeps_unknown_failures_pending_without_breaking_session() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_55555555",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        maintainer
            .unknown_error_claims
            .lock()
            .unwrap()
            .insert(pending_claim.id.to_string());

        // 未知错误同样降级：返回 Ok、保留本地待传、记 warning，不中断会话。
        let report = runner
            .upload_maintainer_batch(vec![pending_claim.clone()], Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 1);
        assert!(report.warning.unwrap().contains("rejected"));
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        let pending: PendingMaintainerUploads = read_yaml(&path).await.unwrap();
        assert_eq!(pending.claims, vec![pending_claim]);
    }

    #[tokio::test]
    async fn process_inbox_drains_pending_uploads_after_successful_pull() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_44444444",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        write_yaml_atomic(
            &path,
            &PendingMaintainerUploads {
                claims: vec![pending_claim.clone()],
                disputes: Vec::new(),
            },
        )
        .await
        .unwrap();

        let report = runner.process_inbox().await.unwrap();

        assert_eq!(report.total, 0);
        assert_eq!(
            maintainer.uploaded_claims.lock().unwrap().as_slice(),
            &[pending_claim.id.to_string()]
        );
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn process_inbox_continues_local_flow_when_pull_fails() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        *maintainer.pull_retryable.lock().unwrap() = true;
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_77777777",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        write_yaml_atomic(
            &path,
            &PendingMaintainerUploads {
                claims: vec![pending_claim.clone()],
                disputes: Vec::new(),
            },
        )
        .await
        .unwrap();

        let report = runner.process_inbox().await.unwrap();

        assert_eq!(report.total, 0);
        assert_eq!(report.warnings.len(), 1);
        assert_eq!(
            report.warnings[0],
            "Maintainer inbox 拉取失败，已跳过远端拉取并继续处理本地 inbox：maintainer POST /inbox/pull 暂时不可用: timeout=7s request deadline exceeded"
        );
        assert!(maintainer.uploaded_claims.lock().unwrap().is_empty());
        let pending: PendingMaintainerUploads = read_yaml(&path).await.unwrap();
        assert_eq!(pending.claims, vec![pending_claim]);
    }

    #[tokio::test]
    async fn process_inbox_reports_pull_auth_failure_separately() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        *maintainer.pull_auth.lock().unwrap() = true;
        let runner = build_runner(&dir, maintainer);

        let report = runner.process_inbox().await.unwrap();

        assert_eq!(report.total, 0);
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("Maintainer inbox 拉取鉴权失败"));
        assert!(report.warnings[0].contains("status=401"));
        assert!(!report.warnings[0].contains("拉取失败"));
    }

    #[tokio::test]
    async fn process_inbox_reports_pull_forbidden_separately() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        *maintainer.pull_forbidden.lock().unwrap() = true;
        let runner = build_runner(&dir, maintainer);

        let report = runner.process_inbox().await.unwrap();

        assert_eq!(report.total, 0);
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("Maintainer inbox 拉取被拒绝"));
        assert!(report.warnings[0].contains("status=403"));
        assert!(!report.warnings[0].contains("拉取失败"));
    }
}
