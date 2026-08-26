//! 仲裁 YAML store 与 per-dispute 跨进程锁。

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::Context;
use ring::digest::{digest, SHA256};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::fs;

use crate::claim::{
    ArbitrationResolutionId, Claim, ClaimId, DisputeId, DisputeStatus, InboxId, PolicyId,
    ResolutionBasis, SourceId,
};
use crate::config::ArbitrationMode;
use crate::storage::{paths, read_yaml, write_yaml_atomic, FileLockGuard, StorageError};

use super::types::{
    AnalysisJob, AnalysisState, ArbitrationAnalysis, ArbitrationResolutionRecord,
    LegacyAnalysisSource, MaintainerDisputeRecord, PendingResolutionDelivery,
    ResolutionEventTarget, ResolutionObservation, ARBITRATION_SCHEMA_VERSION,
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

    pub async fn read_current_analysis(
        &self,
        dispute_id: &DisputeId,
    ) -> anyhow::Result<Option<ArbitrationAnalysis>> {
        let current_path =
            paths::team_store_arbitration_current_analysis_path(&self.team_root, dispute_id);
        if let Some(current) = read_optional_yaml(&current_path).await? {
            return Ok(Some(normalize_legacy_adoption_mode(current, None)));
        }
        let automatic = read_optional_yaml(
            &paths::team_store_arbitration_legacy_automatic_analysis_path(
                &self.team_root,
                dispute_id,
            ),
        )
        .await?;
        let manual = read_optional_yaml(
            &paths::team_store_arbitration_legacy_manual_analysis_path(&self.team_root, dispute_id),
        )
        .await?;
        Ok(select_legacy_current_analysis(automatic, manual))
    }

    pub async fn create_report_analysis(
        &self,
        analysis: &ArbitrationAnalysis,
    ) -> anyhow::Result<()> {
        create_once_yaml(
            &paths::team_store_arbitration_current_analysis_path(
                &self.team_root,
                &analysis.dispute_id,
            ),
            analysis,
            "current analysis",
        )
        .await
    }

    pub async fn replace_current_analysis(
        &self,
        analysis: &ArbitrationAnalysis,
    ) -> anyhow::Result<()> {
        write_yaml_atomic(
            &paths::team_store_arbitration_current_analysis_path(
                &self.team_root,
                &analysis.dispute_id,
            ),
            analysis,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn read_analysis(&self, job: &AnalysisJob) -> anyhow::Result<ArbitrationAnalysis> {
        let analysis = self
            .read_current_analysis(&job.dispute_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("未找到 current analysis"))?;
        if analysis.analysis_id != job.analysis_id {
            anyhow::bail!("analysis job 已被新的 current analysis 替换");
        }
        Ok(analysis)
    }

    pub async fn is_current_analysis_job(&self, job: &AnalysisJob) -> anyhow::Result<bool> {
        let current = self.read_current_analysis(&job.dispute_id).await?;
        Ok(current.is_some_and(|analysis| analysis.analysis_id == job.analysis_id))
    }

    pub async fn write_analysis(&self, analysis: &ArbitrationAnalysis) -> anyhow::Result<()> {
        validate_analysis(analysis)?;
        let path = paths::team_store_arbitration_current_analysis_path(
            &self.team_root,
            &analysis.dispute_id,
        );
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
            let current_path =
                paths::team_store_arbitration_current_analysis_path(&self.team_root, &dispute_id);
            let current = match read_optional_yaml::<ArbitrationAnalysis>(&current_path).await {
                Ok(Some(analysis)) => Some(normalize_legacy_adoption_mode(analysis, None)),
                Ok(None) => {
                    let automatic_path =
                        paths::team_store_arbitration_legacy_automatic_analysis_path(
                            &self.team_root,
                            &dispute_id,
                        );
                    let manual_path = paths::team_store_arbitration_legacy_manual_analysis_path(
                        &self.team_root,
                        &dispute_id,
                    );
                    let automatic = match read_optional_yaml(&automatic_path).await {
                        Ok(analysis) => analysis,
                        Err(error) => {
                            log_recovery_scan_error(
                                &automatic_path,
                                "legacy analysis YAML",
                                &error,
                            );
                            None
                        }
                    };
                    let manual = match read_optional_yaml(&manual_path).await {
                        Ok(analysis) => analysis,
                        Err(error) => {
                            log_recovery_scan_error(&manual_path, "legacy analysis YAML", &error);
                            None
                        }
                    };
                    select_legacy_current_analysis(automatic, manual)
                }
                Err(error) => {
                    log_recovery_scan_error(&current_path, "current analysis YAML", &error);
                    None
                }
            };
            if let Some(analysis) = current {
                push_recoverable_analysis(
                    &mut jobs,
                    analysis,
                    &current_path,
                    &dispute_id,
                    include_auto_approved && dispute.dispute.status == DisputeStatus::Open,
                );
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
            write_yaml_atomic(
                &paths::team_store_arbitration_event_policy_index_path(
                    &self.team_root,
                    &intent.policy.id,
                ),
                &target,
            )
            .await?;
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

    pub async fn event_targets_for_policies(
        &self,
        policy_ids: &[PolicyId],
    ) -> anyhow::Result<Vec<ResolutionEventTarget>> {
        let mut targets = Vec::new();
        for policy_id in policy_ids {
            if let Some(target) = read_optional_yaml(
                &paths::team_store_arbitration_event_policy_index_path(&self.team_root, policy_id),
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

    /// 在 Claim 上传请求仍携带 CAU provenance 时冻结首次归因结果。
    ///
    /// mirror 随后可以被同一 Agent 的下一次上传覆盖，因此候选必须直接取自当前
    /// 请求，并在对应 Dispute 锁内 create-once。只有当前 Resolution 可以新增候选；
    /// 被替换 Resolution 的 Policy 索引即使仍存在，也不会继续接收观测数据。
    pub async fn capture_claim_adoption_candidates(
        &self,
        claim: &Claim,
    ) -> anyhow::Result<Vec<ResolutionEventTarget>> {
        let mut policy_ids = claim
            .source_claim_ids
            .iter()
            .filter_map(|source| match source {
                SourceId::Policy(policy_id) => Some(policy_id.clone()),
                SourceId::Claim(_) => None,
            })
            .collect::<Vec<_>>();
        policy_ids.sort();
        policy_ids.dedup();

        let mut targets = Vec::new();
        for policy_id in policy_ids {
            let Some(target) = read_optional_yaml::<ResolutionEventTarget>(
                &paths::team_store_arbitration_event_policy_index_path(&self.team_root, &policy_id),
            )
            .await?
            else {
                continue;
            };
            let _guard = self.lock_dispute(&target.dispute_id).await?;
            let current = self.read_dispute(&target.dispute_id).await?;
            if current
                .resolution
                .as_ref()
                .map(|resolution| &resolution.resolution_id)
                != Some(&target.resolution_id)
            {
                continue;
            }
            let record = self
                .read_resolution_record(&target.dispute_id, &target.resolution_id)
                .await?;
            let is_intended_holder = record.delivery_intent.as_ref().is_some_and(|intent| {
                intent.policy.id == policy_id
                    && intent
                        .targets
                        .iter()
                        .any(|delivery| delivery.target_agent == claim.holder)
            });
            if !is_intended_holder {
                continue;
            }

            let candidate_path = paths::team_store_arbitration_adoption_candidate_path(
                &self.team_root,
                &target.dispute_id,
                &target.resolution_id,
                &policy_id,
                &claim.id,
            );
            create_first_yaml(&candidate_path, claim, "Claim adoption candidate").await?;

            // candidate 落盘后立即留下恢复 marker。即使进程在 enqueue 前退出，
            // 启动恢复也会只刷新这个 Resolution，并由候选初始化归因快照。
            self.write_pending_observation(&target).await?;

            // Additional Claim 首次归因后也要拥有 ClaimId -> Resolution 索引，
            // 后续上传即使已移除 Policy provenance，仍能定向刷新当前 Resolution。
            let claim_index =
                paths::team_store_arbitration_event_claim_index_dir(&self.team_root, &claim.id)
                    .join(format!("{}.yaml", target.dispute_id));
            write_yaml_atomic(&claim_index, &target).await?;
            targets.push(target);
        }
        targets.sort();
        targets.dedup();
        Ok(targets)
    }

    pub async fn list_claim_adoption_candidates(
        &self,
        dispute_id: &DisputeId,
        resolution_id: &ArbitrationResolutionId,
        policy_id: &PolicyId,
    ) -> anyhow::Result<Vec<Claim>> {
        let mut claims: Vec<Claim> =
            list_yaml(&paths::team_store_arbitration_adoption_candidates_dir(
                &self.team_root,
                dispute_id,
                resolution_id,
                policy_id,
            ))
            .await?;
        claims.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(claims)
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
    include_auto_approved: bool,
) {
    if let Err(error) = validate_analysis(&analysis) {
        log_recovery_scan_error(path, "analysis 记录", &error);
        return;
    }
    if analysis.dispute_id != *dispute_id {
        log_recovery_scan_error(
            path,
            "analysis 记录",
            &anyhow::anyhow!("analysis 与所在 Dispute 不一致"),
        );
        return;
    }
    let recoverable = analysis.state.is_recoverable()
        || (include_auto_approved
            && analysis.state == AnalysisState::Approved
            && analysis.mode == ArbitrationMode::Auto
            && analysis.adoption_blocked_reason.is_none());
    if recoverable {
        jobs.push(AnalysisJob {
            dispute_id: dispute_id.clone(),
            analysis_id: analysis.analysis_id,
        });
    }
}

fn select_legacy_current_analysis(
    automatic: Option<ArbitrationAnalysis>,
    manual: Option<ArbitrationAnalysis>,
) -> Option<ArbitrationAnalysis> {
    let automatic = automatic.map(|analysis| {
        normalize_legacy_adoption_mode(analysis, Some(LegacyAnalysisSource::Automatic))
    });
    let manual = manual.map(|analysis| {
        normalize_legacy_adoption_mode(analysis, Some(LegacyAnalysisSource::Manual))
    });
    match (automatic, manual) {
        (None, None) => None,
        (Some(analysis), None) | (None, Some(analysis)) => Some(analysis),
        (Some(automatic), Some(manual)) => {
            match (
                automatic.state == AnalysisState::Adopting,
                manual.state == AnalysisState::Adopting,
            ) {
                (true, false) => Some(automatic),
                (false, true) => Some(manual),
                _ => Some(
                    if (&manual.created_at, &manual.updated_at, &manual.analysis_id)
                        > (
                            &automatic.created_at,
                            &automatic.updated_at,
                            &automatic.analysis_id,
                        )
                    {
                        manual
                    } else {
                        automatic
                    },
                ),
            }
        }
    }
}

fn normalize_legacy_adoption_mode(
    mut analysis: ArbitrationAnalysis,
    source_hint: Option<LegacyAnalysisSource>,
) -> ArbitrationAnalysis {
    // 历史 manual 记录可能在 auto 配置下创建。把“必须显式采用”折叠进仍会持久化的
    // mode，避免 source 兼容字段在下一次写入时省略后改变采用语义。
    let is_manual = analysis.legacy_source == LegacyAnalysisSource::Manual
        || source_hint == Some(LegacyAnalysisSource::Manual);
    if is_manual {
        analysis.legacy_source = LegacyAnalysisSource::Manual;
        analysis.mode = ArbitrationMode::Manual;
    } else if let Some(source) = source_hint {
        analysis.legacy_source = source;
    }
    analysis
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
        if snapshot.id != analysis.dispute_id {
            anyhow::bail!("analysis report snapshot 与 dispute 不一致");
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
                if resolution.resolution_basis == Some(ResolutionBasis::InsufficientEvidence) {
                    anyhow::bail!(
                        "resolved dispute 的 resolution_basis 不能是 insufficient_evidence"
                    );
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

/// 调用方必须持有覆盖该路径的业务锁。候选记录采用 first-write-wins：后续同一
/// Claim 的上传用于更新 current mirror，不能改写首次归因快照。
async fn create_first_yaml<T>(path: &Path, value: &T, label: &str) -> anyhow::Result<()>
where
    T: Serialize + DeserializeOwned,
{
    match read_yaml::<T>(path).await {
        Ok(_) => return Ok(()),
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
    use crate::maintainer::arbitration::types::LegacyAnalysisSource;
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
        source: LegacyAnalysisSource,
        state: AnalysisState,
        mode: ArbitrationMode,
    ) -> ArbitrationAnalysis {
        let now = "2026-08-01T00:00:00Z".parse().unwrap();
        ArbitrationAnalysis {
            schema_version: ARBITRATION_SCHEMA_VERSION,
            analysis_id: ArbitrationAnalysisId::random(),
            dispute_id: dispute_id.clone(),
            legacy_source: source,
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

    #[test]
    fn current_analysis_omits_legacy_source_but_reads_old_yaml() {
        let record = dispute();
        let current = analysis(
            &record.dispute.id,
            LegacyAnalysisSource::Automatic,
            AnalysisState::Pending,
            ArbitrationMode::Shadow,
        );
        let yaml = serde_yaml_ng::to_string(&current).unwrap();
        assert!(!yaml.lines().any(|line| line.starts_with("source:")));

        let legacy_yaml = format!("source: manual\n{yaml}");
        let decoded: ArbitrationAnalysis = serde_yaml_ng::from_str(&legacy_yaml).unwrap();
        assert_eq!(decoded.legacy_source, LegacyAnalysisSource::Manual);
        assert!(!serde_yaml_ng::to_string(&decoded)
            .unwrap()
            .lines()
            .any(|line| line.starts_with("source:")));
    }

    #[tokio::test]
    async fn legacy_manual_approved_analysis_keeps_explicit_adoption_after_restart() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        let record = dispute();
        store.write_dispute(&record).await.unwrap();
        let manual = analysis(
            &record.dispute.id,
            LegacyAnalysisSource::Manual,
            AnalysisState::Approved,
            ArbitrationMode::Auto,
        );
        let path = paths::team_store_arbitration_legacy_manual_analysis_path(
            store.team_root(),
            &record.dispute.id,
        );
        write_yaml_atomic(&path, &manual).await.unwrap();

        let normalized = store
            .read_current_analysis(&record.dispute.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(normalized.mode, ArbitrationMode::Manual);
        assert!(store.recoverable_jobs(true).await.unwrap().is_empty());

        store.write_analysis(&normalized).await.unwrap();
        let restarted = ArbitrationStore::new(root.path().to_path_buf())
            .read_current_analysis(&record.dispute.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restarted.mode, ArbitrationMode::Manual);
    }

    #[tokio::test]
    async fn legacy_manual_pending_analysis_recovers_only_as_read_only() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        let record = dispute();
        store.write_dispute(&record).await.unwrap();
        let manual = analysis(
            &record.dispute.id,
            LegacyAnalysisSource::Manual,
            AnalysisState::Pending,
            ArbitrationMode::Auto,
        );
        let path = paths::team_store_arbitration_legacy_manual_analysis_path(
            store.team_root(),
            &record.dispute.id,
        );
        write_yaml_atomic(&path, &manual).await.unwrap();

        let normalized = store
            .read_current_analysis(&record.dispute.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(normalized.mode, ArbitrationMode::Manual);
        assert_eq!(
            store.recoverable_jobs(true).await.unwrap(),
            vec![AnalysisJob {
                dispute_id: record.dispute.id,
                analysis_id: manual.analysis_id,
            }]
        );
    }

    #[tokio::test]
    async fn recovery_scan_isolates_corrupt_dispute_and_does_not_schedule_orphan_or_empty_open() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());

        let valid = dispute();
        store.write_dispute(&valid).await.unwrap();
        let valid_analysis = analysis(
            &valid.dispute.id,
            LegacyAnalysisSource::Automatic,
            AnalysisState::Pending,
            ArbitrationMode::Shadow,
        );
        store.create_report_analysis(&valid_analysis).await.unwrap();

        let empty_open = dispute();
        store.write_dispute(&empty_open).await.unwrap();

        let corrupt = dispute();
        fs::write(store.dispute_path(&corrupt.dispute.id), "not: [valid yaml")
            .await
            .unwrap();
        let orphan = analysis(
            &corrupt.dispute.id,
            LegacyAnalysisSource::Automatic,
            AnalysisState::Pending,
            ArbitrationMode::Auto,
        );
        store.create_report_analysis(&orphan).await.unwrap();

        assert_eq!(
            store.recoverable_jobs(true).await.unwrap(),
            vec![AnalysisJob {
                dispute_id: valid.dispute.id,
                analysis_id: valid_analysis.analysis_id,
            }]
        );
        assert!(store
            .read_current_analysis(&empty_open.dispute.id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn recovery_scan_selects_one_legacy_current_analysis_and_isolates_corruption() {
        let root = tempfile::tempdir().unwrap();
        let store = ArbitrationStore::new(root.path().to_path_buf());
        let record = dispute();
        store.write_dispute(&record).await.unwrap();

        let automatic = analysis(
            &record.dispute.id,
            LegacyAnalysisSource::Automatic,
            AnalysisState::Approved,
            ArbitrationMode::Auto,
        );
        let automatic_path = paths::team_store_arbitration_legacy_automatic_analysis_path(
            store.team_root(),
            &record.dispute.id,
        );
        write_yaml_atomic(&automatic_path, &automatic)
            .await
            .unwrap();
        let mut manual = analysis(
            &record.dispute.id,
            LegacyAnalysisSource::Manual,
            AnalysisState::WaitingContext,
            ArbitrationMode::Shadow,
        );
        manual.created_at = "2026-08-01T00:00:01Z".parse().unwrap();
        manual.updated_at = manual.created_at;
        let manual_path = paths::team_store_arbitration_legacy_manual_analysis_path(
            store.team_root(),
            &record.dispute.id,
        );
        write_yaml_atomic(&manual_path, &manual).await.unwrap();
        let corrupt_record = dispute();
        store.write_dispute(&corrupt_record).await.unwrap();
        let corrupt_manual_path = paths::team_store_arbitration_legacy_manual_analysis_path(
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
            }]
        );

        assert_eq!(
            store.recoverable_jobs(true).await.unwrap(),
            vec![AnalysisJob {
                dispute_id: record.dispute.id,
                analysis_id: manual.analysis_id,
            }]
        );
    }
}
