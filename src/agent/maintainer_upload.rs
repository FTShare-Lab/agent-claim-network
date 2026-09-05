//! maintainer 上传补偿队列。
//!
//! 本模块只处理 agent 本地 pending 文件和上传重试编排：
//! claims/disputes 先落本地业务文件，再尽力同步 maintainer。暂时性失败保留在
//! `<agent_home>/maintainer_uploads/pending.yaml`，下次上传触发时一起补传。
//! 普通 Claim 的鉴权失败不进入自动重试队列，因为本地 claim 文件仍是权威数据源；
//! 仲裁内化 Claim 在 Maintainer mirror 收敛前属于 durable delivery，鉴权恢复后继续补传；
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
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub durable_claim_ids: std::collections::BTreeSet<ClaimId>,
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
    agent_home: PathBuf,
    path: PathBuf,
    lock_path: PathBuf,
    lock: Mutex<()>,
    delivery_lock_path: PathBuf,
    delivery_lock: Mutex<()>,
}

impl LocalFsMaintainerUploadQueue {
    pub fn new(agent_home: PathBuf) -> Self {
        Self {
            agent_home: agent_home.clone(),
            path: paths::agent_home_pending_maintainer_uploads_path(&agent_home),
            lock_path: paths::agent_home_pending_maintainer_uploads_lock_path(&agent_home),
            lock: Mutex::new(()),
            delivery_lock_path: paths::agent_home_maintainer_upload_delivery_lock_path(&agent_home),
            delivery_lock: Mutex::new(()),
        }
    }

    pub(super) fn agent_home(&self) -> &std::path::Path {
        &self.agent_home
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
        durable_claims: bool,
    ) -> anyhow::Result<PendingMaintainerUploads> {
        let mut pending = self.read().await?;
        if durable_claims {
            pending
                .durable_claim_ids
                .extend(claims.iter().map(|claim| claim.id.clone()));
        }
        Ok(merge_pending_uploads(pending, claims, disputes))
    }
}

impl AgentRunner {
    pub(super) async fn stage_maintainer_batch(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
    ) -> anyhow::Result<()> {
        self.stage_maintainer_batch_inner(claims, disputes, false)
            .await
    }

    pub(super) async fn stage_maintainer_batch_with_durable_claims(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
    ) -> anyhow::Result<()> {
        self.stage_maintainer_batch_inner(claims, disputes, true)
            .await
    }

    async fn stage_maintainer_batch_inner(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
        durable_claims: bool,
    ) -> anyhow::Result<()> {
        if self.maintainer_client.is_none() {
            return Ok(());
        }
        let _guard = self.maintainer_upload_queue.lock.lock().await;
        let _file_guard =
            crate::storage::FileLockGuard::lock_exclusive(&self.maintainer_upload_queue.lock_path)
                .await?;
        let merged = self
            .maintainer_upload_queue
            .merged_with(claims, disputes, durable_claims)
            .await?;
        self.maintainer_upload_queue.write_or_clear(&merged).await
    }

    pub(super) async fn upload_maintainer_batch(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
    ) -> anyhow::Result<MaintainerUploadReport> {
        self.upload_maintainer_batch_inner(claims, disputes, false)
            .await
    }

    pub(super) async fn upload_maintainer_batch_with_durable_claims(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
    ) -> anyhow::Result<MaintainerUploadReport> {
        self.upload_maintainer_batch_inner(claims, disputes, true)
            .await
    }

    async fn upload_maintainer_batch_inner(
        &self,
        claims: Vec<Claim>,
        disputes: Vec<Dispute>,
        durable_claims: bool,
    ) -> anyhow::Result<MaintainerUploadReport> {
        let Some(maintainer_client) = self.maintainer_client.clone() else {
            return Ok(MaintainerUploadReport::default());
        };
        // 新批次先进入 durable pending；网络交付再按 agent 单飞。这样后到的新版本可在
        // 旧请求进行时继续落盘，但不会先于旧请求写入 Maintainer mirror。
        self.stage_maintainer_batch_inner(claims, disputes, durable_claims)
            .await?;
        let _delivery_guard = self.maintainer_upload_queue.delivery_lock.lock().await;
        let _delivery_file_guard = crate::storage::FileLockGuard::lock_exclusive(
            &self.maintainer_upload_queue.delivery_lock_path,
        )
        .await?;
        let attempted = {
            let _guard = self.maintainer_upload_queue.lock.lock().await;
            let _file_guard = crate::storage::FileLockGuard::lock_exclusive(
                &self.maintainer_upload_queue.lock_path,
            )
            .await?;
            let pending = self.maintainer_upload_queue.read().await?;
            if pending.claims.is_empty() && pending.disputes.is_empty() {
                return Ok(MaintainerUploadReport::default());
            }
            pending
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
        let mut conflicting_dispute_ids = Vec::new();
        let mut deprecated_direct_claim_conflicts = 0usize;

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
                        let durable = attempted.durable_claim_ids.contains(&claim.id);
                        retain_failed_claim(&mut failed, claim, durable);
                    }
                    UploadErrorKind::Auth => {
                        auth_failures += 1;
                        if attempted.durable_claim_ids.contains(&claim.id) {
                            retain_failed_claim(&mut failed, claim, true);
                        }
                    }
                    UploadErrorKind::Forbidden => {
                        forbidden_failures += 1;
                        forbidden_sample.get_or_insert_with(|| err.to_string());
                        if attempted.durable_claim_ids.contains(&claim.id) {
                            retain_failed_claim(&mut failed, claim, true);
                        }
                    }
                    // 其他 client/未知错误继续保留本地待传并记 warning，绝不中断会话。
                    UploadErrorKind::Conflict
                    | UploadErrorKind::Client
                    | UploadErrorKind::Unknown => {
                        rejected_failures += 1;
                        rejected_sample.get_or_insert_with(|| err.to_string());
                        let durable = attempted.durable_claim_ids.contains(&claim.id);
                        retain_failed_claim(&mut failed, claim, durable);
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
                    UploadErrorKind::Conflict => {
                        if is_deprecated_direct_claim_conflict(&err) {
                            deprecated_direct_claim_conflicts += 1;
                        } else {
                            conflicting_dispute_ids.push(dispute.id.to_string());
                        }
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
        if !conflicting_dispute_ids.is_empty() {
            conflicting_dispute_ids.sort();
            conflicting_dispute_ids.dedup();
            // 用户侧提示保持无需行动的措辞；具体 ID 只进日志，便于排查内容分歧。
            log::debug!(
                "dispute 上报与团队既有记录 ID 冲突，已保留团队版本: {}",
                conflicting_dispute_ids.join(", ")
            );
            warnings.push(format!(
                "{} dispute report(s) already exist in the team with the same ID. The team version was kept, so no action is needed.",
                conflicting_dispute_ids.len()
            ));
        }
        if deprecated_direct_claim_conflicts > 0 {
            warnings.push(format!(
                "Maintainer rejected {deprecated_direct_claim_conflicts} dispute report(s) because a direct Claim is deprecated. They were not queued for retry."
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

fn retain_failed_claim(failed: &mut PendingMaintainerUploads, claim: Claim, durable: bool) {
    if durable {
        failed.durable_claim_ids.insert(claim.id.clone());
    }
    failed.claims.push(claim);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadErrorKind {
    Auth,
    Forbidden,
    Retryable,
    Conflict,
    Client,
    Unknown,
}

fn classify_upload_error(err: &anyhow::Error) -> UploadErrorKind {
    match err.downcast_ref::<MaintainerClientError>() {
        Some(err) if err.is_auth() => UploadErrorKind::Auth,
        Some(err) if err.is_retryable() => UploadErrorKind::Retryable,
        Some(MaintainerClientError::Client { status: 403, .. }) => UploadErrorKind::Forbidden,
        Some(MaintainerClientError::Client { status: 409, .. }) => UploadErrorKind::Conflict,
        Some(MaintainerClientError::Client { .. }) => UploadErrorKind::Client,
        Some(_) | None => UploadErrorKind::Unknown,
    }
}

fn is_deprecated_direct_claim_conflict(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<MaintainerClientError>(),
        Some(MaintainerClientError::Client { status: 409, body, .. })
            if body.contains("deprecated direct claim")
    )
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
    let mut durable_claim_ids = pending.durable_claim_ids;
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
    durable_claim_ids.retain(|claim_id| claims_by_id.contains_key(claim_id));

    PendingMaintainerUploads {
        claims: claims_by_id.into_values().collect(),
        durable_claim_ids,
        disputes: disputes_by_id.into_values().collect(),
    }
}

fn reconcile_pending_uploads_after_attempt(
    current: PendingMaintainerUploads,
    attempted: &PendingMaintainerUploads,
    failed: &PendingMaintainerUploads,
) -> PendingMaintainerUploads {
    let mut durable_claim_ids = current.durable_claim_ids;
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
            if failed.durable_claim_ids.contains(&claim.id) {
                durable_claim_ids.insert(claim.id.clone());
            }
            continue;
        }
        if claims_by_id
            .get(&claim.id)
            .is_some_and(|current| current == claim)
        {
            claims_by_id.remove(&claim.id);
            durable_claim_ids.remove(&claim.id);
        }
    }
    durable_claim_ids.retain(|claim_id| claims_by_id.contains_key(claim_id));

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
        durable_claim_ids,
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
        conflicting_disputes: Mutex<BTreeSet<String>>,
        deprecated_direct_claim_disputes: Mutex<BTreeSet<String>>,
        client_error_claims: Mutex<BTreeSet<String>>,
        unknown_error_claims: Mutex<BTreeSet<String>>,
        uploaded_claims: Mutex<Vec<String>>,
        uploaded_claim_statements: Mutex<Vec<String>>,
        uploaded_disputes: Mutex<Vec<String>>,
        reported_dispute_attempts: Mutex<Vec<String>>,
        pending_path_to_observe: Mutex<Option<PathBuf>>,
        observed_write_ahead: Mutex<bool>,
        pull_retryable: Mutex<bool>,
        pull_auth: Mutex<bool>,
        pull_forbidden: Mutex<bool>,
        blocked_claim_statement: Mutex<Option<String>>,
        blocked_upload_started: tokio::sync::Notify,
        release_blocked_upload: tokio::sync::Notify,
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
            let should_block = self.blocked_claim_statement.lock().unwrap().as_deref()
                == Some(claim.statement.as_str());
            if should_block {
                self.blocked_upload_started.notify_one();
                self.release_blocked_upload.notified().await;
            }
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
            self.uploaded_claim_statements
                .lock()
                .unwrap()
                .push(claim.statement.clone());
            Ok(())
        }

        async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<()> {
            let id = dispute.id.to_string();
            self.reported_dispute_attempts
                .lock()
                .unwrap()
                .push(id.clone());
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
            if self.conflicting_disputes.lock().unwrap().contains(&id) {
                return Err(MaintainerClientError::Client {
                    operation: "disputes/report".into(),
                    status: 409,
                    body: "dispute payload conflict".into(),
                }
                .into());
            }
            if self
                .deprecated_direct_claim_disputes
                .lock()
                .unwrap()
                .contains(&id)
            {
                return Err(MaintainerClientError::Client {
                    operation: "disputes/report".into(),
                    status: 409,
                    body: "deprecated direct claim".into(),
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
            Ok(ScopesOverviewSnapshot::default())
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
            durable_claim_ids: Default::default(),
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
                durable_claim_ids: Default::default(),
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
            durable_claim_ids: Default::default(),
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
            durable_claim_ids: Default::default(),
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
    async fn concurrent_uploads_deliver_same_claim_versions_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        *maintainer.blocked_claim_statement.lock().unwrap() = Some("old".into());
        let old_runner = build_runner(&dir, maintainer.clone());
        let new_runner = build_runner(&dir, maintainer.clone());
        let mut old = claim(
            "claim_11111111",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        old.statement = "old".into();
        let mut new = old.clone();
        new.statement = "new".into();
        new.updated_at = Some("2026-06-23T00:00:00Z".parse().unwrap());

        let old_task = tokio::spawn(async move {
            old_runner
                .upload_maintainer_batch(vec![old], Vec::new())
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            maintainer.blocked_upload_started.notified(),
        )
        .await
        .expect("old upload should reach the network");

        let new_task = tokio::spawn(async move {
            new_runner
                .upload_maintainer_batch(vec![new], Vec::new())
                .await
        });
        let pending_path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Ok(pending) = read_yaml::<PendingMaintainerUploads>(&pending_path).await {
                    if pending.claims.iter().any(|claim| claim.statement == "new") {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("new version should be staged while old upload is in flight");
        assert!(maintainer
            .uploaded_claim_statements
            .lock()
            .unwrap()
            .is_empty());

        maintainer.release_blocked_upload.notify_one();
        old_task.await.unwrap().unwrap();
        new_task.await.unwrap().unwrap();

        assert_eq!(
            maintainer
                .uploaded_claim_statements
                .lock()
                .unwrap()
                .as_slice(),
            &["old".to_string(), "new".to_string()]
        );
        assert!(!fs::try_exists(pending_path).await.unwrap());
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
    async fn upload_batch_reports_dispute_after_claim_attempt_even_when_claim_failed() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_11111111",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        let ready_claim = claim(
            "claim_22222222",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        let pending_dispute = dispute("dispute_11111111", "2026-06-22T00:00:00Z");
        maintainer
            .retryable_claims
            .lock()
            .unwrap()
            .insert(pending_claim.id.to_string());

        let report = runner
            .upload_maintainer_batch(
                vec![pending_claim.clone(), ready_claim.clone()],
                vec![pending_dispute.clone()],
            )
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 1);
        assert_eq!(report.pending_disputes, 0);
        assert_eq!(
            maintainer.uploaded_claims.lock().unwrap().as_slice(),
            &[ready_claim.id.to_string()]
        );
        assert_eq!(
            maintainer.uploaded_disputes.lock().unwrap().as_slice(),
            &[pending_dispute.id.to_string()]
        );
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        let pending: PendingMaintainerUploads = read_yaml(&path).await.unwrap();
        assert_eq!(pending.claims, vec![pending_claim.clone()]);
        assert!(pending.disputes.is_empty());

        maintainer.retryable_claims.lock().unwrap().clear();
        let recovered = runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await
            .unwrap();

        assert_eq!(recovered.pending_claims, 0);
        assert_eq!(recovered.pending_disputes, 0);
        assert_eq!(
            maintainer.uploaded_disputes.lock().unwrap().as_slice(),
            &[pending_dispute.id.to_string()]
        );
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_does_not_retain_ordinary_auth_failed_claim_for_dispute() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_11111111",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        let pending_dispute = dispute("dispute_11111111", "2026-06-22T00:00:00Z");
        maintainer
            .auth_error_claims
            .lock()
            .unwrap()
            .insert(pending_claim.id.to_string());

        let report = runner
            .upload_maintainer_batch(vec![pending_claim.clone()], vec![pending_dispute.clone()])
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 0);
        assert_eq!(report.pending_disputes, 0);
        assert_eq!(
            maintainer.uploaded_disputes.lock().unwrap().as_slice(),
            &[pending_dispute.id.to_string()]
        );
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_does_not_block_dispute_unrelated_to_failed_claim() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let unrelated_claim = claim(
            "claim_33333333",
            ClaimStatus::Active,
            "2026-06-22T00:00:00Z",
        );
        let ready_dispute = dispute("dispute_11111111", "2026-06-22T00:00:00Z");
        maintainer
            .retryable_claims
            .lock()
            .unwrap()
            .insert(unrelated_claim.id.to_string());

        let report = runner
            .upload_maintainer_batch(vec![unrelated_claim], vec![ready_dispute.clone()])
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 1);
        assert_eq!(report.pending_disputes, 0);
        assert_eq!(
            maintainer.uploaded_disputes.lock().unwrap().as_slice(),
            &[ready_dispute.id.to_string()]
        );
    }

    #[tokio::test]
    async fn upload_batch_does_not_add_referenced_claims_from_the_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let pending_claim = claim(
            "claim_11111111",
            ClaimStatus::Active,
            "2026-06-20T00:00:00Z",
        );
        let ready_claim = claim(
            "claim_22222222",
            ClaimStatus::Active,
            "2026-06-20T00:00:00Z",
        );
        let unrelated_claim = claim(
            "claim_33333333",
            ClaimStatus::Active,
            "2026-06-20T00:00:00Z",
        );
        for local_claim in [&pending_claim, &ready_claim, &unrelated_claim] {
            runner.claim_store.write_claim(local_claim).await.unwrap();
        }
        let pending_dispute = dispute("dispute_11111111", "2026-06-22T00:00:00Z");
        let report = runner
            .upload_maintainer_batch(Vec::new(), vec![pending_dispute.clone()])
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 0);
        assert_eq!(report.pending_disputes, 0);
        assert!(maintainer.uploaded_claims.lock().unwrap().is_empty());
        assert_eq!(
            maintainer.uploaded_disputes.lock().unwrap().as_slice(),
            &[pending_dispute.id.to_string()]
        );
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        assert!(!fs::try_exists(path).await.unwrap());
        assert!(!maintainer
            .uploaded_claims
            .lock()
            .unwrap()
            .contains(&pending_claim.id.to_string()));
        assert!(!maintainer
            .uploaded_claims
            .lock()
            .unwrap()
            .contains(&ready_claim.id.to_string()));
        assert!(!maintainer
            .uploaded_claims
            .lock()
            .unwrap()
            .contains(&unrelated_claim.id.to_string()));
    }

    #[tokio::test]
    async fn upload_batch_does_not_scan_local_storage_for_dispute_claims() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let mut remote_claim = claim(
            "claim_11111111",
            ClaimStatus::Active,
            "2026-06-20T00:00:00Z",
        );
        remote_claim.holder = AgentId::new("agent-b").unwrap();
        let local_claim = claim(
            "claim_22222222",
            ClaimStatus::Active,
            "2026-06-20T00:00:00Z",
        );
        runner.claim_store.write_claim(&remote_claim).await.unwrap();
        runner.claim_store.write_claim(&local_claim).await.unwrap();

        let report = runner
            .upload_maintainer_batch(
                Vec::new(),
                vec![dispute("dispute_11111111", "2026-06-22T00:00:00Z")],
            )
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 0);
        assert!(maintainer.uploaded_claims.lock().unwrap().is_empty());
        assert_eq!(
            maintainer.uploaded_disputes.lock().unwrap().as_slice(),
            &["dispute_11111111"]
        );
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
    async fn durable_claim_survives_forbidden_until_identity_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let durable_claim = claim(
            "claim_88888888",
            ClaimStatus::Deprecated,
            "2026-06-22T00:00:00Z",
        );
        maintainer
            .forbidden_claims
            .lock()
            .unwrap()
            .insert(durable_claim.id.to_string());

        let report = runner
            .upload_maintainer_batch_with_durable_claims(vec![durable_claim.clone()], Vec::new())
            .await
            .unwrap();

        assert_eq!(report.pending_claims, 1);
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        let pending: PendingMaintainerUploads = read_yaml(&path).await.unwrap();
        assert_eq!(pending.claims, vec![durable_claim.clone()]);
        assert_eq!(
            pending.durable_claim_ids,
            BTreeSet::from([durable_claim.id.clone()])
        );

        let repeated = runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await
            .unwrap();
        assert_eq!(repeated.pending_claims, 1);
        assert!(fs::try_exists(&path).await.unwrap());

        maintainer.forbidden_claims.lock().unwrap().clear();
        let recovered = runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await
            .unwrap();

        assert_eq!(recovered.pending_claims, 0);
        assert!(!fs::try_exists(path).await.unwrap());
        assert_eq!(
            maintainer.uploaded_claims.lock().unwrap().as_slice(),
            &[durable_claim.id.to_string()]
        );
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
    async fn upload_batch_drops_conflicting_dispute_after_one_warning() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let conflicting = dispute("dispute_55555555", "2026-06-22T00:00:00Z");
        maintainer
            .conflicting_disputes
            .lock()
            .unwrap()
            .insert(conflicting.id.to_string());

        let report = runner
            .upload_maintainer_batch(Vec::new(), vec![conflicting.clone()])
            .await
            .unwrap();

        assert_eq!(report.pending_disputes, 0);
        let warning = report.warning.unwrap();
        // 面向非编程用户的提示不暴露内部 ID，只说明团队版本已保留且无需处理。
        assert!(!warning.contains(conflicting.id.as_str()));
        assert!(warning.contains("already exist in the team"));
        assert!(warning.contains("no action is needed"));
        let path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        assert!(!fs::try_exists(&path).await.unwrap());

        let repeated = runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await
            .unwrap();
        assert_eq!(repeated, MaintainerUploadReport::default());
        assert!(!fs::try_exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn upload_batch_drops_deprecated_direct_claim_dispute_without_retry() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(TestMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let rejected = dispute("dispute_66666666", "2026-06-22T00:00:00Z");
        maintainer
            .deprecated_direct_claim_disputes
            .lock()
            .unwrap()
            .insert(rejected.id.to_string());

        let report = runner
            .upload_maintainer_batch(Vec::new(), vec![rejected.clone()])
            .await
            .unwrap();

        assert_eq!(report.pending_disputes, 0);
        let warning = report.warning.unwrap();
        assert!(warning.contains("direct Claim is deprecated"));
        assert!(warning.contains("not queued for retry"));
        assert_eq!(
            maintainer
                .reported_dispute_attempts
                .lock()
                .unwrap()
                .as_slice(),
            &[rejected.id.to_string()]
        );
        let pending_path = paths::agent_home_pending_maintainer_uploads_path(dir.path());
        assert!(!fs::try_exists(&pending_path).await.unwrap());

        let repeated = runner
            .upload_maintainer_batch(Vec::new(), Vec::new())
            .await
            .unwrap();
        assert_eq!(repeated, MaintainerUploadReport::default());
        assert_eq!(
            maintainer.reported_dispute_attempts.lock().unwrap().len(),
            1
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
                durable_claim_ids: Default::default(),
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
                durable_claim_ids: Default::default(),
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
