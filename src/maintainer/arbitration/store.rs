//! 仲裁 YAML store 与 per-dispute 跨进程锁。

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::Context;
use ring::digest::{digest, SHA256};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::fs;

use crate::claim::{ArbitrationResolutionId, ClaimId, DisputeId, DisputeStatus, InboxId};
use crate::config::ArbitrationMode;
use crate::storage::{paths, read_yaml, write_yaml_atomic, FileLockGuard, StorageError};

use super::types::{
    AnalysisJob, AnalysisSource, AnalysisState, ArbitrationAnalysis, ArbitrationResolutionRecord,
    MaintainerDisputeRecord, PendingResolutionDelivery, ResolutionEventTarget,
    ResolutionObservation, ARBITRATION_SCHEMA_VERSION,
};

#[derive(Debug, Clone)]
pub struct ArbitrationStore {
    team_root: PathBuf,
}

impl ArbitrationStore {
    pub fn new(team_root: PathBuf) -> Self {
        Self { team_root }
    }

    pub fn team_root(&self) -> &Path {
        &self.team_root
    }

    pub async fn lock_dispute(&self, dispute_id: &DisputeId) -> anyhow::Result<FileLockGuard> {
        let path = paths::team_store_arbitration_lock_path(&self.team_root, dispute_id);
        FileLockGuard::lock_exclusive(&path)
            .await
            .with_context(|| format!("获取 dispute 仲裁锁失败: {path:?}"))
    }

    pub async fn lock_semantic_inputs(&self) -> anyhow::Result<FileLockGuard> {
        let path = paths::team_store_arbitration_semantic_inputs_lock_path(&self.team_root);
        FileLockGuard::lock_exclusive(&path)
            .await
            .with_context(|| format!("获取仲裁语义输入锁失败: {path:?}"))
    }

    /// 读取全局语义输入版本。旧 team root 没有版本文件时从 0 开始。
    pub async fn read_semantic_inputs_revision(&self) -> anyhow::Result<u64> {
        let path = paths::team_store_arbitration_semantic_inputs_revision_path(&self.team_root);
        Ok(read_optional_yaml::<u64>(&path).await?.unwrap_or(0))
    }

    /// 调用方必须已经持有 semantic-inputs.lock；先递增版本再写权威数据，崩溃时
    /// 最多产生一次保守的 context_changed/retry，不会漏掉已经发生的语义变化。
    pub async fn bump_semantic_inputs_revision(
        &self,
        _guard: &FileLockGuard,
    ) -> anyhow::Result<u64> {
        let current = self.read_semantic_inputs_revision().await?;
        let next = current
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("仲裁语义输入 revision 已耗尽"))?;
        let path = paths::team_store_arbitration_semantic_inputs_revision_path(&self.team_root);
        write_yaml_atomic(&path, &next)
            .await
            .with_context(|| format!("写仲裁语义输入 revision 失败: {path:?}"))?;
        Ok(next)
    }

    pub async fn read_dispute(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<MaintainerDisputeRecord> {
        let path = self.dispute_path(dispute_id);
        read_yaml(&path)
            .await
            .with_context(|| format!("读取 Maintainer dispute 失败: {path:?}"))
    }

    pub async fn list_disputes(&self) -> anyhow::Result<Vec<MaintainerDisputeRecord>> {
        list_yaml(&paths::team_store_disputes_dir(&self.team_root)).await
    }

    pub async fn write_dispute(&self, record: &MaintainerDisputeRecord) -> anyhow::Result<()> {
        validate_dispute_record(record)?;
        let path = self.dispute_path(&record.dispute.id);
        write_yaml_atomic(&path, record)
            .await
            .with_context(|| format!("写 Maintainer dispute 失败: {path:?}"))
    }

    pub async fn read_automatic_analysis(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<Option<ArbitrationAnalysis>> {
        read_optional_yaml(&paths::team_store_arbitration_automatic_analysis_path(
            &self.team_root,
            dispute_id,
        ))
        .await
    }

    pub async fn create_automatic_analysis(
        &self,
        analysis: &ArbitrationAnalysis,
    ) -> anyhow::Result<()> {
        if analysis.source != AnalysisSource::Automatic {
            anyhow::bail!("automatic analysis 的 source 必须为 automatic");
        }
        create_once_yaml(
            &paths::team_store_arbitration_automatic_analysis_path(
                &self.team_root,
                &analysis.dispute_id,
            ),
            analysis,
            "automatic analysis",
        )
        .await
    }

    pub async fn read_manual_analysis(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<Option<ArbitrationAnalysis>> {
        read_optional_yaml(&paths::team_store_arbitration_manual_analysis_path(
            &self.team_root,
            dispute_id,
        ))
        .await
    }

    #[cfg(test)]
    pub async fn list_manual_analysis(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<Vec<ArbitrationAnalysis>> {
        Ok(self
            .read_manual_analysis(dispute_id)
            .await?
            .into_iter()
            .collect())
    }

    pub async fn create_manual_analysis(
        &self,
        analysis: &ArbitrationAnalysis,
    ) -> anyhow::Result<()> {
        if analysis.source != AnalysisSource::Manual {
            anyhow::bail!("manual analysis 的 source 必须为 manual");
        }
        write_yaml_atomic(
            &paths::team_store_arbitration_manual_analysis_path(
                &self.team_root,
                &analysis.dispute_id,
            ),
            analysis,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn read_analysis(&self, job: &AnalysisJob) -> anyhow::Result<ArbitrationAnalysis> {
        let analysis = match job.source {
            AnalysisSource::Automatic => self
                .read_automatic_analysis(&job.dispute_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("未找到 automatic analysis"))?,
            AnalysisSource::Manual => self
                .read_manual_analysis(&job.dispute_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("未找到 manual analysis"))?,
        };
        if analysis.analysis_id != job.analysis_id || analysis.source != job.source {
            anyhow::bail!("analysis job 与持久化记录不一致");
        }
        Ok(analysis)
    }

    pub async fn is_current_analysis_job(&self, job: &AnalysisJob) -> anyhow::Result<bool> {
        let current = match job.source {
            AnalysisSource::Automatic => self.read_automatic_analysis(&job.dispute_id).await?,
            AnalysisSource::Manual => self.read_manual_analysis(&job.dispute_id).await?,
        };
        Ok(current.is_some_and(|analysis| {
            analysis.analysis_id == job.analysis_id && analysis.source == job.source
        }))
    }

    pub async fn write_analysis(&self, analysis: &ArbitrationAnalysis) -> anyhow::Result<()> {
        validate_analysis(analysis)?;
        let path = match analysis.source {
            AnalysisSource::Automatic => paths::team_store_arbitration_automatic_analysis_path(
                &self.team_root,
                &analysis.dispute_id,
            ),
            AnalysisSource::Manual => paths::team_store_arbitration_manual_analysis_path(
                &self.team_root,
                &analysis.dispute_id,
            ),
        };
        write_yaml_atomic(&path, analysis)
            .await
            .with_context(|| format!("写 arbitration analysis 失败: {path:?}"))
    }

    /// 启动时只返回已持久化且处于恢复态的分析，不扫描没有分析记录的 open Dispute。
    ///
    /// 这是唯一一条容错读路径：单个损坏的 Dispute / Analysis 会被隔离并记录日志，
    /// 不影响其他恢复任务。面向 HTTP 的 list/read 仍保持严格报错语义。
    pub async fn recoverable_jobs(
        &self,
        include_auto_approved: bool,
    ) -> anyhow::Result<Vec<AnalysisJob>> {
        let mut jobs = Vec::new();
        let disputes_dir = paths::team_store_disputes_dir(&self.team_root);
        if !fs::try_exists(&disputes_dir)
            .await
            .with_context(|| format!("检查 dispute 恢复目录失败: {disputes_dir:?}"))?
        {
            return Ok(jobs);
        }
        let mut disputes = fs::read_dir(&disputes_dir)
            .await
            .with_context(|| format!("读取 dispute 恢复目录失败: {disputes_dir:?}"))?;
        loop {
            let entry = match disputes.next_entry().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(error) => {
                    log_recovery_scan_error(&disputes_dir, "dispute 目录项", &error);
                    break;
                }
            };
            let dispute_path = entry.path();
            if !is_yaml_data_path(&dispute_path) {
                continue;
            }
            let dispute = match read_yaml::<MaintainerDisputeRecord>(&dispute_path).await {
                Ok(dispute) => dispute,
                Err(error) => {
                    log_recovery_scan_error(&dispute_path, "dispute YAML", &error);
                    continue;
                }
            };
            if let Err(error) = validate_dispute_record(&dispute) {
                log_recovery_scan_error(&dispute_path, "dispute 记录", &error);
                continue;
            }
            if dispute_path.file_stem().and_then(|stem| stem.to_str())
                != Some(dispute.dispute.id.as_str())
            {
                log_recovery_scan_error(
                    &dispute_path,
                    "dispute 记录",
                    &anyhow::anyhow!("Dispute ID 与文件名不一致"),
                );
                continue;
            }

            let dispute_id = dispute.dispute.id;
            let automatic_path =
                paths::team_store_arbitration_automatic_analysis_path(&self.team_root, &dispute_id);
            match read_optional_yaml::<ArbitrationAnalysis>(&automatic_path).await {
                Ok(Some(analysis)) => push_recoverable_analysis(
                    &mut jobs,
                    analysis,
                    &automatic_path,
                    &dispute_id,
                    AnalysisSource::Automatic,
                    include_auto_approved && dispute.dispute.status == DisputeStatus::Open,
                ),
                Ok(None) => {}
                Err(error) => {
                    log_recovery_scan_error(&automatic_path, "automatic analysis YAML", &error)
                }
            }

            let manual_path =
                paths::team_store_arbitration_manual_analysis_path(&self.team_root, &dispute_id);
            match read_optional_yaml::<ArbitrationAnalysis>(&manual_path).await {
                Ok(Some(analysis)) => push_recoverable_analysis(
                    &mut jobs,
                    analysis,
                    &manual_path,
                    &dispute_id,
                    AnalysisSource::Manual,
                    false,
                ),
                Ok(None) => {}
                Err(error) => log_recovery_scan_error(&manual_path, "manual analysis YAML", &error),
            }
        }
        jobs.sort();
        jobs.dedup();
        Ok(jobs)
    }

    /// 返回已经固定 Resolution intent、但尚未完成采用的持久任务。
    ///
    /// 该恢复路径不需要 LLM，因此即使 arbitration.enabled=false 也能补齐升级前或
    /// 上次运行留下的 outbox 投递，并沿用同一 Analysis/Resolution/Policy/inbox ID。
    pub async fn recoverable_adoption_jobs(&self) -> anyhow::Result<Vec<AnalysisJob>> {
        let jobs = self.recoverable_jobs(false).await?;
        let mut adopting = Vec::new();
        for job in jobs {
            match self.read_analysis(&job).await {
                Ok(analysis) if analysis.state == AnalysisState::Adopting => adopting.push(job),
                Ok(_) => {}
                Err(error) => {
                    log::warn!(
                        target: "maintainer_arbitration",
                        "读取 adopting analysis 恢复记录失败，已隔离 dispute={} analysis={}: {error:#}",
                        job.dispute_id,
                        job.analysis_id
                    );
                }
            }
        }
        Ok(adopting)
    }

    pub async fn write_resolution_record(
        &self,
        record: &ArbitrationResolutionRecord,
    ) -> anyhow::Result<()> {
        if record.schema_version != ARBITRATION_SCHEMA_VERSION {
            anyhow::bail!(
                "resolution schema_version={} 不受支持",
                record.schema_version
            );
        }
        if record.resolution_id != record.resolution.resolution_id {
            anyhow::bail!("resolution id 与 resolution.resolution_id 不一致");
        }
        write_yaml_atomic(
            &paths::team_store_arbitration_resolution_path(&self.team_root, &record.dispute_id),
            record,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn resolution_id_exists(
        &self,
        dispute_id: &DisputeId,
        resolution_id: &ArbitrationResolutionId,
    ) -> anyhow::Result<bool> {
        if self
            .read_current_resolution_record(dispute_id)
            .await?
            .is_some_and(|record| record.resolution_id == *resolution_id)
        {
            return Ok(true);
        }
        let legacy =
            paths::team_store_arbitration_legacy_decisions_dir(&self.team_root, dispute_id)
                .join(format!("{resolution_id}.yaml"));
        fs::try_exists(&legacy)
            .await
            .with_context(|| format!("检查旧 resolution id 失败: {legacy:?}"))
    }

    pub async fn read_resolution_record(
        &self,
        dispute_id: &DisputeId,
        resolution_id: &ArbitrationResolutionId,
    ) -> anyhow::Result<ArbitrationResolutionRecord> {
        let current_path =
            paths::team_store_arbitration_resolution_path(&self.team_root, dispute_id);
        let current: ArbitrationResolutionRecord = match read_yaml(&current_path).await {
            Ok(record) => record,
            Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {
                let legacy =
                    paths::team_store_arbitration_legacy_decisions_dir(&self.team_root, dispute_id)
                        .join(format!("{resolution_id}.yaml"));
                read_yaml(&legacy).await?
            }
            Err(error) => return Err(error.into()),
        };
        if current.resolution_id != *resolution_id {
            anyhow::bail!("当前 resolution id 与请求不一致");
        }
        Ok(current)
    }

    pub async fn read_current_resolution_record(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<Option<ArbitrationResolutionRecord>> {
        read_optional_yaml(&paths::team_store_arbitration_resolution_path(
            &self.team_root,
            dispute_id,
        ))
        .await
    }

    #[cfg(test)]
    pub async fn list_resolution_records(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<Vec<ArbitrationResolutionRecord>> {
        Ok(self
            .read_current_resolution_record(dispute_id)
            .await?
            .into_iter()
            .collect())
    }

    pub async fn write_observation(
        &self,
        dispute_id: &DisputeId,
        observation: &ResolutionObservation,
    ) -> anyhow::Result<()> {
        let path = paths::team_store_arbitration_observations_dir(&self.team_root, dispute_id)
            .join(format!("{}.yaml", observation.resolution_id));
        write_yaml_atomic(&path, observation).await?;
        Ok(())
    }

    pub async fn write_pending_delivery(
        &self,
        task: &PendingResolutionDelivery,
    ) -> anyhow::Result<()> {
        write_yaml_atomic(
            &paths::team_store_arbitration_pending_delivery_path(
                &self.team_root,
                &task.target.resolution_id,
            ),
            task,
        )
        .await?;
        Ok(())
    }

    pub async fn read_pending_delivery(
        &self,
        resolution_id: &ArbitrationResolutionId,
    ) -> anyhow::Result<Option<PendingResolutionDelivery>> {
        read_optional_yaml(&paths::team_store_arbitration_pending_delivery_path(
            &self.team_root,
            resolution_id,
        ))
        .await
    }

    pub async fn remove_pending_delivery(
        &self,
        resolution_id: &ArbitrationResolutionId,
    ) -> anyhow::Result<()> {
        let path =
            paths::team_store_arbitration_pending_delivery_path(&self.team_root, resolution_id);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(error).with_context(|| format!("删除 pending delivery 失败: {path:?}"))
            }
        }
    }

    pub async fn list_pending_deliveries(&self) -> anyhow::Result<Vec<PendingResolutionDelivery>> {
        list_yaml(&paths::team_store_arbitration_pending_deliveries_dir(
            &self.team_root,
        ))
        .await
    }

    pub async fn write_pending_observation(
        &self,
        target: &ResolutionEventTarget,
    ) -> anyhow::Result<()> {
        write_yaml_atomic(
            &paths::team_store_arbitration_pending_observation_path(
                &self.team_root,
                &target.resolution_id,
            ),
            target,
        )
        .await?;
        Ok(())
    }

    pub async fn remove_pending_observation(
        &self,
        resolution_id: &ArbitrationResolutionId,
    ) -> anyhow::Result<()> {
        let path =
            paths::team_store_arbitration_pending_observation_path(&self.team_root, resolution_id);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => {
                Err(error).with_context(|| format!("删除 pending observation 失败: {path:?}"))
            }
        }
    }

    pub async fn list_pending_observations(&self) -> anyhow::Result<Vec<ResolutionEventTarget>> {
        list_yaml(&paths::team_store_arbitration_pending_observations_dir(
            &self.team_root,
        ))
        .await
    }

    pub async fn register_resolution_event_targets(
        &self,
        record: &ArbitrationResolutionRecord,
    ) -> anyhow::Result<()> {
        let target = ResolutionEventTarget {
            dispute_id: record.dispute_id.clone(),
            resolution_id: record.resolution_id.clone(),
        };
        if let Some(intent) = record.delivery_intent.as_ref() {
            for delivery in &intent.targets {
                write_yaml_atomic(
                    &paths::team_store_arbitration_event_inbox_index_path(
                        &self.team_root,
                        &delivery.inbox_id,
                    ),
                    &target,
                )
                .await?;
            }
        }
        for claim in &record.direct_claim_snapshots {
            let path =
                paths::team_store_arbitration_event_claim_index_dir(&self.team_root, &claim.id)
                    .join(format!("{}.yaml", record.dispute_id));
            write_yaml_atomic(&path, &target).await?;
        }
        Ok(())
    }

    pub async fn event_targets_for_inboxes(
        &self,
        inbox_ids: &[InboxId],
    ) -> anyhow::Result<Vec<ResolutionEventTarget>> {
        let mut targets = Vec::new();
        for inbox_id in inbox_ids {
            if let Some(target) = read_optional_yaml(
                &paths::team_store_arbitration_event_inbox_index_path(&self.team_root, inbox_id),
            )
            .await?
            {
                targets.push(target);
            }
        }
        targets.sort();
        targets.dedup();
        Ok(targets)
    }

    pub async fn event_targets_for_claim(
        &self,
        claim_id: &ClaimId,
    ) -> anyhow::Result<Vec<ResolutionEventTarget>> {
        let mut targets: Vec<ResolutionEventTarget> = list_yaml(
            &paths::team_store_arbitration_event_claim_index_dir(&self.team_root, claim_id),
        )
        .await?;
        targets.sort();
        targets.dedup();
        Ok(targets)
    }

    pub async fn read_observation(
        &self,
        dispute_id: &DisputeId,
        resolution_id: &ArbitrationResolutionId,
    ) -> anyhow::Result<Option<ResolutionObservation>> {
        read_optional_yaml(
            &paths::team_store_arbitration_observations_dir(&self.team_root, dispute_id)
                .join(format!("{resolution_id}.yaml")),
        )
        .await
    }

    fn dispute_path(&self, dispute_id: &DisputeId) -> PathBuf {
        paths::team_store_disputes_dir(&self.team_root).join(format!("{dispute_id}.yaml"))
    }
}

fn push_recoverable_analysis(
    jobs: &mut Vec<AnalysisJob>,
    analysis: ArbitrationAnalysis,
    path: &Path,
    dispute_id: &DisputeId,
    expected_source: AnalysisSource,
    include_auto_approved: bool,
) {
    if let Err(error) = validate_analysis(&analysis) {
        log_recovery_scan_error(path, "analysis 记录", &error);
        return;
    }
    if analysis.dispute_id != *dispute_id || analysis.source != expected_source {
        log_recovery_scan_error(
            path,
            "analysis 记录",
            &anyhow::anyhow!("analysis 与所在 Dispute/source 不一致"),
        );
        return;
    }
    let recoverable = analysis.state.is_recoverable()
        || (include_auto_approved
            && expected_source == AnalysisSource::Automatic
            && analysis.state == AnalysisState::Approved
            && analysis.mode == ArbitrationMode::Auto
            && analysis.adoption_blocked_reason.is_none());
    if recoverable {
        jobs.push(AnalysisJob {
            dispute_id: dispute_id.clone(),
            analysis_id: analysis.analysis_id,
            source: expected_source,
        });
    }
}

fn is_yaml_data_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".yaml") && !name.contains(".tmp."))
}

fn log_recovery_scan_error(
    path: &Path,
    record_kind: &str,
    error: &(impl std::fmt::Display + ?Sized),
) {
    log::warn!(
        target: "maintainer_arbitration",
        "启动恢复已隔离损坏的{record_kind}: path={} error={error}",
        path.display()
    );
}

pub fn versioned_sha256<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(value)?;
    Ok(format!(
        "sha256-v2:{}",
        hex::encode(digest(&SHA256, &encoded).as_ref())
    ))
}

fn validate_analysis(analysis: &ArbitrationAnalysis) -> anyhow::Result<()> {
    if analysis.schema_version != ARBITRATION_SCHEMA_VERSION {
        anyhow::bail!(
            "analysis schema_version={} 不受支持",
            analysis.schema_version
        );
    }
    if analysis.context.is_some()
        != (analysis.semantic_fingerprint.is_some() && analysis.context_snapshot_hash.is_some())
    {
        anyhow::bail!("analysis context 与 fingerprint/hash 必须同时存在");
    }
    if (analysis.state == AnalysisState::WaitingReanalysis) != analysis.next_retry_at.is_some() {
        anyhow::bail!("waiting_reanalysis 与 next_retry_at 必须同时存在");
    }
    if analysis.analysis_round == 0 || analysis.analysis_round > 3 {
        anyhow::bail!("analysis_round 必须在 1..=3");
    }
    if let Some(snapshot) = analysis.report_snapshot.as_ref() {
        if analysis.source != AnalysisSource::Automatic || snapshot.id != analysis.dispute_id {
            anyhow::bail!("analysis report snapshot 与 automatic dispute 不一致");
        }
    }
    Ok(())
}

fn validate_dispute_record(record: &MaintainerDisputeRecord) -> anyhow::Result<()> {
    match record.dispute.status {
        DisputeStatus::Open => {
            if record.dispute.resolved_at.is_some() || record.resolution.is_some() {
                anyhow::bail!("open dispute 不能带 resolved_at 或 resolution");
            }
        }
        DisputeStatus::Resolved => {
            if record.dispute.resolved_at.is_none() {
                anyhow::bail!("resolved dispute 必须带 resolved_at");
            }
            if let Some(resolution) = record.resolution.as_ref() {
                if resolution
                    .resolution_type
                    .is_some_and(|kind| !kind.is_resolved())
                {
                    anyhow::bail!("resolved dispute 的 resolution_type 不能是 unresolved");
                }
                if resolution.conclusion.trim().is_empty() {
                    anyhow::bail!("resolution conclusion 不能为空");
                }
            }
        }
    }
    Ok(())
}

async fn create_once_yaml<T>(path: &Path, value: &T, label: &str) -> anyhow::Result<()>
where
    T: Serialize + DeserializeOwned + PartialEq,
{
    match read_yaml::<T>(path).await {
        Ok(existing) if existing == *value => return Ok(()),
        Ok(_) => anyhow::bail!("{label} 已存在但内容不同: {path:?}"),
        Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    write_yaml_atomic(path, value)
        .await
        .with_context(|| format!("创建 {label} 失败: {path:?}"))
}

async fn read_optional_yaml<T: DeserializeOwned>(path: &Path) -> anyhow::Result<Option<T>> {
    match read_yaml(path).await {
        Ok(value) => Ok(Some(value)),
        Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn list_yaml<T: DeserializeOwned>(dir: &Path) -> anyhow::Result<Vec<T>> {
    if !fs::try_exists(dir).await.unwrap_or(false) {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.ends_with(".yaml") || name.contains(".tmp.") {
            continue;
        }
        values.push(read_yaml(&path).await?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{AgentId, Dispute, DisputeStatus};
    use crate::maintainer::arbitration::ArbitrationAnalysisId;

    fn dispute() -> MaintainerDisputeRecord {
        MaintainerDisputeRecord::from(Dispute {
            id: DisputeId::random(),
            name: "legacy".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims: vec![],
            summary: "summary".into(),
            status: DisputeStatus::Open,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: None,
        })
    }

    fn analysis(
        dispute_id: &DisputeId,
        source: AnalysisSource,
        state: AnalysisState,
        mode: ArbitrationMode,
    ) -> ArbitrationAnalysis {
        let now = "2026-08-01T00:00:00Z".parse().unwrap();
        ArbitrationAnalysis {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            analysis_id: ArbitrationAnalysisId::random(),
            dispute_id: dispute_id.clone(),
            source,
            report_snapshot: None,
            created_at: now,
            updated_at: now,
            prompt_version: super::super::types::ARBITRATION_PROMPT_VERSION.into(),
            mode,
            model: "test-model".into(),
            confidence_threshold: 0.9,
            semantic_projection_version: super::super::types::CURRENT_SEMANTIC_PROJECTION_VERSION,
            semantic_fingerprint: None,
            context_snapshot_hash: None,
            context: None,
            state,
            analysis_round: 1,
            rounds: Vec::new(),
            context_change_count: 0,
            next_retry_at: None,
            context_change_reason: None,
            lease: None,
            proposal: None,
            verification: None,
            resolution_id: None,
            pending_resolution: None,
            error: None,
            delivery_error: None,
            adoption_blocked_reason: None,
            context_prepare_attempts: 0,
        }
    }

    #[tokio::test]
    async fn legacy_dispute_round_trips_without_resolution() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        let record = dispute();
        store.write_dispute(&record).await.unwrap();
        assert_eq!(
            store.read_dispute(&record.dispute.id).await.unwrap(),
            record
        );
    }

    #[tokio::test]
    async fn semantic_input_revision_starts_at_zero_and_increments_under_lock() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        assert_eq!(store.read_semantic_inputs_revision().await.unwrap(), 0);

        let guard = store.lock_semantic_inputs().await.unwrap();
        assert_eq!(
            store.bump_semantic_inputs_revision(&guard).await.unwrap(),
            1
        );
        drop(guard);
        assert_eq!(store.read_semantic_inputs_revision().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_scan_isolates_corrupt_dispute_and_does_not_schedule_orphan_or_empty_open() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());

        let valid = dispute();
        store.write_dispute(&valid).await.unwrap();
        let valid_analysis = analysis(
            &valid.dispute.id,
            AnalysisSource::Automatic,
            AnalysisState::Pending,
            ArbitrationMode::Shadow,
        );
        store
            .create_automatic_analysis(&valid_analysis)
            .await
            .unwrap();

        let empty_open = dispute();
        store.write_dispute(&empty_open).await.unwrap();

        let corrupt = dispute();
        fs::write(store.dispute_path(&corrupt.dispute.id), "not: [valid yaml")
            .await
            .unwrap();
        let orphan = analysis(
            &corrupt.dispute.id,
            AnalysisSource::Automatic,
            AnalysisState::Pending,
            ArbitrationMode::Auto,
        );
        store.create_automatic_analysis(&orphan).await.unwrap();

        assert_eq!(
            store.recoverable_jobs(true).await.unwrap(),
            vec![AnalysisJob {
                dispute_id: valid.dispute.id,
                analysis_id: valid_analysis.analysis_id,
                source: AnalysisSource::Automatic,
            }]
        );
        assert!(store
            .read_automatic_analysis(&empty_open.dispute.id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn recovery_scan_isolates_corrupt_manual_and_filters_auto_approved_by_mode() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        let record = dispute();
        store.write_dispute(&record).await.unwrap();

        let automatic = analysis(
            &record.dispute.id,
            AnalysisSource::Automatic,
            AnalysisState::Approved,
            ArbitrationMode::Auto,
        );
        store.create_automatic_analysis(&automatic).await.unwrap();
        let manual = analysis(
            &record.dispute.id,
            AnalysisSource::Manual,
            AnalysisState::WaitingContext,
            ArbitrationMode::Shadow,
        );
        store.create_manual_analysis(&manual).await.unwrap();
        let corrupt_record = dispute();
        store.write_dispute(&corrupt_record).await.unwrap();
        let corrupt_manual_path = paths::team_store_arbitration_manual_analysis_path(
            store.team_root(),
            &corrupt_record.dispute.id,
        );
        fs::create_dir_all(corrupt_manual_path.parent().unwrap())
            .await
            .unwrap();
        fs::write(&corrupt_manual_path, "state: definitely-not-an-analysis")
            .await
            .unwrap();

        assert_eq!(
            store.recoverable_jobs(false).await.unwrap(),
            vec![AnalysisJob {
                dispute_id: record.dispute.id.clone(),
                analysis_id: manual.analysis_id.clone(),
                source: AnalysisSource::Manual,
            }]
        );

        let mut expected = vec![
            AnalysisJob {
                dispute_id: record.dispute.id.clone(),
                analysis_id: automatic.analysis_id,
                source: AnalysisSource::Automatic,
            },
            AnalysisJob {
                dispute_id: record.dispute.id,
                analysis_id: manual.analysis_id,
                source: AnalysisSource::Manual,
            },
        ];
        expected.sort();
        assert_eq!(store.recoverable_jobs(true).await.unwrap(), expected);
    }
}
