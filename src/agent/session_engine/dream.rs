//! Dream 的有界探索、结构化校验和可恢复提交；不处理会话游标或 dispute。
use super::claim_context::content_hash;
use super::dream_audit::Audit;
use super::dream_plan::{reaches, Plan, PreparedGroup};
use super::dream_review::ReviewRecord;
use super::{SessionEngine, SessionFinalizeReport};
use crate::claim::{Claim, ClaimId, ClaimStatus, SourceId, Trace, TraceId};
use crate::storage::{paths, read_yaml, write_yaml_atomic, FileLockGuard, StorageError};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::sync::CancellationToken;

const INPUT_TOKENS: usize = 64_000;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct DreamState {
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_input_snapshot_at: Option<DateTime<Utc>>,
    pub known_versions: BTreeMap<ClaimId, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Checkpoint {
    pub snapshot_at: DateTime<Utc>,
    pub input_versions: BTreeMap<ClaimId, String>,
    pub coverage: Value,
    pub plan: Plan,
    pub receipts: BTreeMap<String, Value>,
    /// 当前探索上下文中工具实际返回的记录数；不是查证通过的 Claim 数量。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exploration_receipts: Option<usize>,
    pub groups: Vec<PreparedGroup>,
    #[serde(default)]
    pub safety_reviews: Vec<Value>,
    #[serde(default)]
    pub self_reviews: Vec<ReviewRecord>,
    pub trace_id: TraceId,
    #[serde(default)]
    pub applied: bool,
    #[serde(default)]
    pub success_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub self_versions: BTreeMap<ClaimId, String>,
    #[serde(default)]
    pub operation: Option<super::dream_execution::ExecutionMeta>,
}

#[derive(Debug, thiserror::Error)]
#[error("Dream yielded before Prepared to higher priority work")]
pub(crate) struct DreamYield;

#[derive(Debug, thiserror::Error)]
#[error("Dream stopped at a recoverable boundary")]
pub(crate) struct DreamStopped;

/// 与 supervisor Stop 共用生命周期锁，只保护进入 Prepared，不锁住后续落盘或网络。
#[derive(Clone, Default)]
pub(crate) struct DreamControl {
    pub(super) stop: CancellationToken,
    prepare_gate: Arc<Mutex<()>>,
}

impl DreamControl {
    pub(crate) fn new(stop: CancellationToken, prepare_gate: Arc<Mutex<()>>) -> Self {
        Self { stop, prepare_gate }
    }

    pub(super) fn check(&self) -> anyhow::Result<()> {
        if self.stop.is_cancelled() {
            return Err(DreamStopped.into());
        }
        Ok(())
    }

    pub(super) async fn prepare_guard(&self) -> anyhow::Result<MutexGuard<'_, ()>> {
        let guard = self.prepare_gate.lock().await;
        self.check()?;
        Ok(guard)
    }

    pub(super) async fn snapshot_guard(&self, path: &Path) -> anyhow::Result<FileLockGuard> {
        loop {
            self.check()?;
            // 前台 Inbox 可能持锁等待模型；不能留下无法取消的 blocking 锁等待线程。
            if let Some(guard) = FileLockGuard::try_lock_exclusive(path).await? {
                self.check()?;
                return Ok(guard);
            }
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => return Err(DreamStopped.into()),
                _ = tokio::time::sleep(Duration::from_millis(50)) => {},
            }
        }
    }
}

pub(super) fn conservative_tokens(text: &str) -> usize {
    // 非 ASCII 按每字符最多三 token 估计；ASCII 按字节计，留足工具与输出余量。
    text.chars().map(|c| if c.is_ascii() { 1 } else { 3 }).sum()
}
pub(super) fn claim_input(
    mut claims: Vec<Claim>,
    budget: usize,
) -> anyhow::Result<(Vec<Claim>, usize)> {
    claims.sort_by(|a, b| {
        b.effective_updated_at()
            .cmp(&a.effective_updated_at())
            .then_with(|| b.created_at.cmp(&a.created_at))
            .then_with(|| a.id.cmp(&b.id))
    });
    let mut used = 0usize;
    let mut loaded = Vec::new();
    for claim in claims {
        let cost = conservative_tokens(&serde_json::to_string(&claim)?);
        if cost > budget {
            continue;
        }
        if used.saturating_add(cost) > budget {
            break;
        }
        used += cost;
        loaded.push(claim);
    }
    Ok((loaded, used))
}
async fn optional_yaml<T: serde::de::DeserializeOwned + Default>(path: &Path) -> anyhow::Result<T> {
    match read_yaml(path).await {
        Ok(value) => Ok(value),
        Err(StorageError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok(T::default())
        }
        Err(e) => Err(e.into()),
    }
}
fn versions(claims: &[Claim]) -> anyhow::Result<BTreeMap<ClaimId, String>> {
    claims
        .iter()
        .map(|c| Ok((c.id.clone(), content_hash(c)?)))
        .collect()
}

impl SessionEngine {
    pub fn with_dream_config(mut self, config: crate::config::AgentDreamConfig) -> Self {
        self.dream_config = config;
        self
    }
    pub(crate) fn dream_enabled(&self) -> bool {
        self.dream_config.enable
    }
    pub(crate) fn dream_workspace(&self) -> &Path {
        &self.workspace_root
    }
    fn dream_state_path(&self) -> PathBuf {
        self.runner
            .maintainer_upload_queue
            .agent_home()
            .join("runtime")
            .join("supervisor")
            .join("dream_state.yaml")
    }
    pub(crate) async fn dream_input_fingerprint(&self) -> anyhow::Result<String> {
        let claims = self.owned_dream_claims().await?;
        content_hash(&versions(&claims)?)
    }
    pub(super) async fn owned_dream_claims(&self) -> anyhow::Result<Vec<Claim>> {
        let mut claims = self.agent.claim_store.list_local_claims().await?;
        claims.retain(|c| c.holder == self.agent.agent_id);
        Ok(claims)
    }
    pub(crate) async fn dream_eligible(&self, manual: bool) -> anyhow::Result<bool> {
        if !self.dream_enabled() {
            return Ok(false);
        }
        let claims = self.owned_dream_claims().await?;
        if !claims.iter().any(|c| c.status != ClaimStatus::Deprecated) {
            return Ok(false);
        }
        if manual {
            return Ok(true);
        }
        let state: DreamState = optional_yaml(&self.dream_state_path()).await?;
        if state.last_success_at.is_some_and(|last| {
            (Utc::now() - last).to_std().unwrap_or_default()
                < Duration::from_secs(self.dream_config.min_interval_hours.saturating_mul(3600))
        }) {
            return Ok(false);
        }
        let current = versions(&claims)?;
        Ok(current
            .iter()
            .filter(|(id, hash)| state.known_versions.get(*id) != Some(*hash))
            .count()
            >= 5)
    }

    #[cfg(test)]
    pub(crate) async fn run_dream_job(
        &self,
        job_id: &str,
        workspace: &Path,
        jobs_dir: &Path,
        manual: bool,
        first_attempt: bool,
    ) -> anyhow::Result<SessionFinalizeReport> {
        self.run_dream_job_with_control(
            job_id,
            workspace,
            jobs_dir,
            manual,
            first_attempt,
            DreamControl::default(),
        )
        .await
    }

    pub(crate) async fn run_dream_job_with_control(
        &self,
        job_id: &str,
        workspace: &Path,
        jobs_dir: &Path,
        manual: bool,
        first_attempt: bool,
        control: DreamControl,
    ) -> anyhow::Result<SessionFinalizeReport> {
        self.recover_pending_dream_execution().await?;
        let checkpoint_path = jobs_dir
            .parent()
            .context("Dream jobs directory has no parent")?
            .join("dream")
            .join(format!("{job_id}.yaml"));
        if tokio::fs::try_exists(&checkpoint_path).await? {
            return self
                .apply_dream_checkpoint_with_control(&checkpoint_path, &control)
                .await;
        }
        control.check()?;
        // 停止/让位不消耗 attempt；已开始的同一轮不能被其自身修改后的触发门槛截断。
        let resuming = tokio::fs::try_exists(
            self.runner
                .maintainer_upload_queue
                .agent_home()
                .join("dream")
                .join(job_id)
                .join("origin.yaml"),
        )
        .await?;
        if !self.dream_enabled()
            || (first_attempt && !resuming && !self.dream_eligible(manual).await?)
        {
            return Ok(SessionFinalizeReport::default());
        }
        let lock = paths::agent_home_knowledge_apply_lock_path(
            self.runner.maintainer_upload_queue.agent_home(),
        );
        let guard = control.snapshot_guard(&lock).await?;
        let snapshot_at = Utc::now();
        let all = self.owned_dream_claims().await?;
        let input_versions = versions(&all)?;
        drop(guard);
        let original_claims = all.clone();
        let visible: Vec<_> = all
            .into_iter()
            .filter(|c| c.status != ClaimStatus::Deprecated)
            .collect();
        if visible.is_empty() && first_attempt && !resuming {
            return Ok(SessionFinalizeReport::default());
        }
        let total = visible.len();
        let system_prompt = self.prompt_registry.render("claim_dream", json!({}))?;
        let budget = INPUT_TOKENS.min(
            self.context_window
                .saturating_sub(conservative_tokens(&system_prompt))
                .saturating_sub(usize::try_from(self.turn_loop.max_tokens())?)
                .saturating_sub(24_000),
        );
        let (loaded, used) = claim_input(visible, budget)?;
        let coverage = json!({"total":total,"included":loaded.len(),"omitted":total-loaded.len(),"order":"effective_updated_at descending; oldest omitted first","estimated_tokens":used,"budget_tokens":budget});
        let previous: DreamState = optional_yaml(&self.dream_state_path()).await?;
        let mut input = json!({"dream_context":{"agent_id":self.agent.agent_id,"snapshot_at":snapshot_at,"workspace_root":workspace,"last_success_at":previous.last_success_at,"trigger":if manual {"manual"} else {"automatic"}},"coverage":coverage,"claims":loaded,"policy_context":{"available":false,"complete":false,"note":"Policy provenance may appear in claim source_claim_ids; no independent active Policy cache is available. Do not infer missing policy content."},"evidence_access":{"tools":["read_claim","read_trace","file_read","code_run","dream_record_candidates","dream_keep_candidate","dream_stage_group","dream_read_draft","dream_validate","dream_apply_group","dream_review_sync","dream_read_history","dream_finish"],"workspace_root":workspace}});
        let tools = Arc::new(
            self.turn_loop
                .tool_registry()
                .as_ref()
                .clone()
                .for_dream(workspace.to_path_buf()),
        );
        input["evidence_access"]["command_environment"] = tools.dream_command_environment().await;
        input["pending_consolidation_sync"] = json!(self.runner.pending_consolidations().await?.iter().map(|group| json!({"source_id":group.retired.id,"carrier_ids":group.carriers.iter().map(|c| &c.id).collect::<Vec<_>>() })).collect::<Vec<_>>());
        let execution = Arc::new(
            super::dream_execution::Execution::new(
                self.clone(),
                job_id,
                jobs_dir,
                original_claims,
                coverage,
                snapshot_at,
            )
            .await?
            .with_control(control.clone()),
        );
        execution.recover().await?;
        input["dream_context"]["eligibility"] = execution.eligibility_context(&loaded);
        let normalized = execution.normalized_versions(input_versions).await?;
        let result = self
            .explore_dream(
                jobs_dir,
                system_prompt,
                input,
                &normalized,
                tools.clone(),
                execution.clone(),
            )
            .await;
        let (plan, _, _) = match result {
            Ok(result) => result,
            Err(error) => {
                if error.is::<DreamYield>() || error.is::<DreamStopped>() {
                    return Err(error);
                }
                let context = execution.context().await?;
                return Err(anyhow::anyhow!(format!(
                    "Dream 未完成：{error:#}；已生效操作保留，恢复时不得重放。执行状态：{}",
                    serde_json::to_string(&context["executions"])?
                )));
            }
        };
        let mut checkpoint = execution.final_checkpoint(plan.review).await?;
        checkpoint.receipts = tools.dream_evidence().await.0;
        checkpoint.exploration_receipts = Some(checkpoint.receipts.len());
        let guard = control.prepare_guard().await?;
        write_yaml_atomic(&checkpoint_path, &checkpoint).await?;
        drop(guard);
        self.apply_dream_checkpoint_with_control(&checkpoint_path, &control)
            .await
    }

    pub(crate) async fn recover_pending_dream_execution(&self) -> anyhow::Result<()> {
        let pending =
            super::dream_execution::pending_path(self.runner.maintainer_upload_queue.agent_home());
        let path: PathBuf = match read_yaml(&pending).await {
            Ok(path) => path,
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(())
            }
            Err(error) => return Err(error.into()),
        };
        // 恢复只保证本地提交完整；远端交付沿用持久上传队列，不阻塞其他后台任务。
        self.apply_dream_checkpoint_locally(&path).await?;
        Ok(())
    }

    pub(super) async fn apply_dream_checkpoint_with_control(
        &self,
        path: &Path,
        control: &DreamControl,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let report = self.apply_dream_checkpoint_locally(path).await?;
        // 正常 Dream 提交继续尝试同步；失败或停止时由已有持久上传队列保留。
        let upload = tokio::select! {
            biased;
            _ = control.stop.cancelled() => Ok(()),
            result = self.runner.upload_maintainer_batch_with_durable_claims(vec![], vec![]) => result.map(|_| ()),
        };
        if let Err(e) = upload {
            log::warn!(target:"agent", "Dream upload pending: {e}");
        }
        Ok(report)
    }

    async fn apply_dream_checkpoint_locally(
        &self,
        path: &Path,
    ) -> anyhow::Result<SessionFinalizeReport> {
        let lock = paths::agent_home_knowledge_apply_lock_path(
            self.runner.maintainer_upload_queue.agent_home(),
        );
        let guard = FileLockGuard::lock_exclusive(&lock).await?;
        // IPC 检查可能与正在执行的组重叠；拿锁后重读，不能使用等待前的检查点。
        let mut checkpoint: Checkpoint = read_yaml(path).await?;
        let pending =
            super::dream_execution::pending_path(self.runner.maintainer_upload_queue.agent_home());
        let pending_operation: Option<PathBuf> = match read_yaml(&pending).await {
            Ok(path) => Some(path),
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                None
            }
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(
            pending_operation
                .as_deref()
                .is_none_or(|pending| pending == path),
            "Another Dream group needs recovery before this checkpoint"
        );
        if checkpoint.operation.is_some() && checkpoint.applied && pending_operation.is_none() {
            return Ok(SessionFinalizeReport {
                trace_id: Some(checkpoint.trace_id),
                updated_claim_ids: checkpoint.self_versions.keys().cloned().collect(),
                ..Default::default()
            });
        }
        let legacy_job_id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .context("Invalid Dream checkpoint path")?;
        let job_id = checkpoint
            .operation
            .as_ref()
            .map(|op| op.job_id.as_str())
            .unwrap_or(legacy_job_id);
        let audit = Audit::new(self.runner.maintainer_upload_queue.agent_home(), job_id)?;
        audit.checkpoint(json!(checkpoint)).await?;
        if !checkpoint.applied {
            if checkpoint.operation.is_some() {
                write_yaml_atomic(
                    &super::dream_execution::pending_path(
                        self.runner.maintainer_upload_queue.agent_home(),
                    ),
                    &path,
                )
                .await?;
            }
            for index in 0..checkpoint.groups.len() {
                if checkpoint.groups[index].completed {
                    continue;
                }
                let group = &checkpoint.groups[index];
                // 恢复时允许目标版本已写入；第三个版本绝不覆盖，也不继续废弃输入。
                let current: BTreeMap<_, _> = self
                    .owned_dream_claims()
                    .await?
                    .into_iter()
                    .map(|c| (c.id.clone(), c))
                    .collect();
                let mut conflict = group
                    .before
                    .iter()
                    .zip(&group.after)
                    .any(|(before, after)| {
                        current
                            .get(&before.id)
                            .is_none_or(|actual| actual != before && actual != after)
                    });
                let mut graph = current.clone();
                for after in &group.after {
                    graph.insert(after.id.clone(), after.clone());
                }
                for (before, after) in group.before.iter().zip(&group.after) {
                    for source in &after.source_claim_ids {
                        if !before.source_claim_ids.contains(source) {
                            if let SourceId::Claim(id) = source {
                                conflict |= reaches(id, &after.id, &graph, &mut BTreeSet::new());
                            }
                        }
                    }
                }
                for id in &checkpoint.plan.groups[index].evidence_ids {
                    if let Some(receipt) = checkpoint.receipts.get(id) {
                        if !crate::tool::dream_evidence_is_current(receipt).await? {
                            conflict = true;
                        }
                    }
                }
                // 兼容已进入 Prepared 的旧任务：旧复核额外读取仍是提交依赖。
                for review in &checkpoint.safety_reviews {
                    if review["input"]["after"] == json!(checkpoint.plan.groups[index].updates) {
                        for receipt in review["evidence"]
                            .as_object()
                            .into_iter()
                            .flat_map(|m| m.values())
                        {
                            if !crate::tool::dream_evidence_is_current(receipt).await? {
                                conflict = true;
                            }
                        }
                    }
                }
                let mut staged = Vec::new();
                if !conflict {
                    // 先写完整整合结果，再废弃已承接的来源；崩溃恢复不会先丢失信息。
                    let mut after = group.after.clone();
                    after.sort_by_key(|c| c.status == ClaimStatus::Deprecated);
                    for claim in after {
                        anyhow::ensure!(
                            claim.holder == self.agent.agent_id,
                            "Dream checkpoint owner mismatch"
                        );
                        if current.get(&claim.id) != Some(&claim) {
                            self.agent.claim_store.write_claim(&claim).await?;
                        }
                        staged.push(claim);
                    }
                } else {
                    staged.extend(
                        group
                            .after
                            .iter()
                            .filter(|c| current.get(&c.id) == Some(*c))
                            .cloned(),
                    );
                }
                let mut dependencies = Vec::new();
                if checkpoint.plan.groups[index].kind == super::dream_plan::Kind::Consolidation {
                    for retired in staged
                        .iter()
                        .filter(|c| c.status == ClaimStatus::Deprecated)
                    {
                        let before = group
                            .before
                            .iter()
                            .find(|c| c.id == retired.id)
                            .context("Missing consolidation source")?;
                        let carriers = checkpoint.plan.groups[index]
                            .coverage
                            .iter()
                            .filter(|c| c.input_id == retired.id)
                            .flat_map(|c| &c.output_ids)
                            .filter_map(|id| {
                                group
                                    .after
                                    .iter()
                                    .find(|c| &c.id == id && c.status != ClaimStatus::Deprecated)
                            })
                            .cloned()
                            .collect();
                        dependencies.push(
                            crate::agent::consolidation_delivery::ConsolidationDelivery {
                                before: before.clone(),
                                retired: retired.clone(),
                                carriers,
                            },
                        );
                    }
                }
                self.runner
                    .stage_consolidation_batch(staged.clone(), dependencies)
                    .await?;
                for claim in staged {
                    if group.before.iter().any(|before| before == &claim) {
                        continue;
                    }
                    checkpoint
                        .self_versions
                        .insert(claim.id.clone(), content_hash(&claim)?);
                }
                checkpoint.groups[index].skipped = conflict;
                checkpoint.groups[index].completed = true;
                write_yaml_atomic(path, &checkpoint).await?;
                audit.checkpoint(json!(checkpoint)).await?;
            }
            let trace = Trace {
                id: checkpoint.trace_id.clone(),
                name: "dream_claim_review".into(),
                task: serde_json::to_string(
                    &json!({"kind":"dream","coverage":checkpoint.coverage,"review":checkpoint.plan,"evidence":checkpoint.receipts,"groups":checkpoint.groups,"safety_reviews":checkpoint.safety_reviews,"self_reviews":checkpoint.self_reviews}),
                )?,
                agent: self.agent.agent_id.clone(),
                input_claims: checkpoint
                    .groups
                    .iter()
                    .flat_map(|g| g.before.iter().map(|c| SourceId::Claim(c.id.clone())))
                    .collect(),
                output_claims: checkpoint.self_versions.keys().cloned().collect(),
                created_at: crate::time::now_seconds(),
            };
            self.agent.claim_store.write_trace(&trace).await?;
            checkpoint.applied = true;
            checkpoint.success_at = Some(Utc::now());
            write_yaml_atomic(path, &checkpoint).await?;
        }
        audit.checkpoint(json!(checkpoint)).await?;
        let mut known_versions = checkpoint.input_versions;
        known_versions.extend(checkpoint.self_versions.clone());
        let previous: DreamState = optional_yaml(&self.dream_state_path()).await?;
        if checkpoint.operation.is_some() {
            let mut state = previous;
            state
                .known_versions
                .extend(checkpoint.self_versions.clone());
            write_yaml_atomic(&self.dream_state_path(), &state).await?;
            match tokio::fs::remove_file(super::dream_execution::pending_path(
                self.runner.maintainer_upload_queue.agent_home(),
            ))
            .await
            {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        } else if previous.last_success_at <= checkpoint.success_at {
            write_yaml_atomic(
                &self.dream_state_path(),
                &DreamState {
                    last_success_at: checkpoint.success_at,
                    last_input_snapshot_at: Some(checkpoint.snapshot_at),
                    known_versions,
                },
            )
            .await?;
        }
        drop(guard);
        Ok(SessionFinalizeReport {
            trace_id: Some(checkpoint.trace_id),
            updated_claim_ids: checkpoint.self_versions.keys().cloned().collect(),
            ..Default::default()
        })
    }
}
