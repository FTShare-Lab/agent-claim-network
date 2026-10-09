//! Dream 工具探索与可恢复草案；不写 Claim，也不代替 Prepared 提交。
use super::claim_context::content_hash;
use super::dream::{DreamControl, DreamStopped, DreamYield};
use super::dream_audit::Audit;
use super::dream_compaction::{Compactor, Summary};
use super::dream_draft::{Draft, Reviewer};
use super::dream_execution::Execution;
use super::dream_history::History;
use super::dream_plan::{validate_plan, Plan};
use super::dream_review::ReviewRecord;
use super::SessionEngine;
use crate::api::{
    estimate_provider_request_context_tokens, SessionTurnContentBlock, SessionTurnEvent,
    SessionTurnMessage, SessionTurnPreflight, SessionTurnRequest, ToolBoundaryControl,
    ToolCallSkipReason,
};
use crate::claim::{Claim, ClaimId};
use crate::storage::{read_yaml, write_yaml_atomic, StorageError};
use crate::tool::{DreamPlanTools, ToolRegistry};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
struct Exploration {
    fingerprint: String,
    snapshot_at: DateTime<Utc>,
    messages: Vec<SessionTurnMessage>,
    receipts: BTreeMap<String, Value>,
    observed: BTreeMap<ClaimId, Claim>,
    #[serde(default)]
    draft: Draft,
    last_error: Option<String>,
    #[serde(default)]
    summary: Option<Summary>,
}

#[derive(Debug, thiserror::Error)]
#[error("Dream structured plan is ready")]
struct PlanReady;

struct ReviewHarness {
    control: DreamControl,
    path: PathBuf,
    jobs_dir: PathBuf,
    context_window: usize,
    tools: Arc<ToolRegistry>,
    progress: Exploration,
    reviewer: Arc<Reviewer>,
    output_reserve: usize,
    skip_initial_save: bool,
    compactor: Compactor,
    history_replaced: bool,
    audit: Arc<Audit>,
}

impl ReviewHarness {
    async fn save(&mut self, messages: &[SessionTurnMessage]) -> anyhow::Result<()> {
        self.progress.messages = messages.to_vec();
        self.progress.draft = self.reviewer.snapshot().await;
        (self.progress.receipts, self.progress.observed) = self.tools.dream_evidence().await;
        write_yaml_atomic(&self.path, &self.progress).await?;
        Ok(())
    }

    fn request(&self, system_prompt: &str, job_id: &str, user_text: String) -> SessionTurnRequest {
        SessionTurnRequest {
            current_session_id: None,
            current_turn_id: Some(job_id.into()),
            system_prompt: system_prompt.into(),
            history: self.progress.messages.clone(),
            user_text,
            user_attachments: vec![],
            skill_instructions: vec![],
        }
    }
}

#[async_trait]
impl SessionTurnPreflight for ReviewHarness {
    fn history_replacement_expected(&self, system: &str, messages: &[SessionTurnMessage]) -> bool {
        let definitions = self
            .tools
            .definitions()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        self.compactor.needed(system, messages, &definitions)
    }

    fn take_history_replaced_since_last_check(&mut self) -> bool {
        std::mem::take(&mut self.history_replaced)
    }

    async fn before_provider_request(
        &mut self,
        system: &mut String,
        messages: &mut Vec<SessionTurnMessage>,
        _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        if let Some(error) = self.reviewer.take_fatal_error().await {
            self.save(messages).await?;
            return Err(error);
        }
        if self.control.stop.is_cancelled() {
            self.save(messages).await?;
            return Err(DreamStopped.into());
        }
        if crate::supervisor::dream_has_priority_work(&self.jobs_dir).await? {
            self.save(messages).await?;
            return Err(DreamYield.into());
        }
        // 工具暂存与对应证据在完整 request 边界一起持久化。失败草案不会覆盖已接受组。
        if self.reviewer.finished().await.is_some() {
            self.save(messages).await?;
            return Err(PlanReady.into());
        }
        // 与前台使用同一请求估算器，避免序列化整个内部历史时把 canonical / replay
        // 算两次，或把 JSON 转义和中文按输入筛选的最坏上界再次放大。
        let definitions = self
            .tools
            .definitions()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        if self.compactor.needed(system, messages, &definitions) {
            self.save(messages).await?;
            let receipts = self.progress.receipts.iter().map(|(id, receipt)| (id, serde_json::json!({
                "tool":receipt["tool"], "input":receipt["input"], "file_version":receipt["output"]["file_version"]
            }))).collect::<BTreeMap<_, _>>();
            let state = serde_json::json!({
                "draft":self.reviewer.resume_input(&self.progress.observed).await,
                "additional_claims":self.progress.observed,"evidence_index":receipts,
                "last_error":self.progress.last_error
            });
            let compact = self.compactor.compact(
                system,
                messages,
                &definitions,
                state,
                self.progress.summary.as_ref(),
            );
            let (compacted, summary, archive_id) = tokio::select! {
                biased;
                _ = self.control.stop.cancelled() => return Err(DreamStopped.into()),
                result = compact => result?,
            };
            self.audit.event(serde_json::json!({"kind":"context_compacted","archive_id":archive_id,"summary":summary,
                "before_messages":messages.len(),"after_messages":compacted.len()})).await?;
            self.progress.summary = Some(summary);
            self.save(&compacted).await?;
            *messages = compacted;
            self.history_replaced = true;
            if crate::supervisor::dream_has_priority_work(&self.jobs_dir).await? {
                return Err(DreamYield.into());
            }
        }
        let cost =
            estimate_provider_request_context_tokens(system, messages, &definitions).used_tokens;
        anyhow::ensure!(
            cost.saturating_add(self.output_reserve) <= self.context_window,
            "Dream context budget exceeded; earlier committed operations remain recorded"
        );
        // 无进展的 provider 重试不把同一条错误反馈反复堆入基线。
        if !std::mem::take(&mut self.skip_initial_save) {
            self.save(messages).await?;
        }
        self.control.check()
    }

    // 内部 continuation 可能尚有未执行的 tool_use；不把它保存为可恢复的完整回合。
    async fn provider_response_checkpoint(
        &mut self,
        _: &[SessionTurnMessage],
        _: usize,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn provider_response_ready(
        &mut self,
        _: &[SessionTurnMessage],
        _: usize,
    ) -> anyhow::Result<()> {
        // 无效普通文本由审计完整归档，只将错误反馈加入下一次模型输入。
        self.control.check()
    }
}

fn progress_path(jobs: &Path, job_id: &str) -> anyhow::Result<PathBuf> {
    Ok(jobs
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Dream jobs directory has no parent"))?
        .join("dream_exploration")
        .join(format!("{job_id}.yaml")))
}

async fn reusable_progress(path: &Path, fingerprint: &str) -> anyhow::Result<Option<Exploration>> {
    let saved: Exploration = match read_yaml(path).await {
        Ok(saved) => saved,
        Err(StorageError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None)
        }
        Err(error) => return Err(error.into()),
    };
    if saved.fingerprint != fingerprint {
        return Ok(None);
    }
    for receipt in saved.receipts.values() {
        if !crate::tool::dream_evidence_is_current(receipt).await? {
            return Ok(None);
        }
    }
    Ok(Some(saved))
}

impl SessionEngine {
    pub(super) async fn explore_dream(
        &self,
        jobs_dir: &Path,
        system_prompt: String,
        input: Value,
        input_versions: &BTreeMap<ClaimId, String>,
        tools: Arc<ToolRegistry>,
        execution: Arc<Execution>,
    ) -> anyhow::Result<(Plan, DateTime<Utc>, Vec<ReviewRecord>)> {
        let result = self
            .explore_dream_inner(
                jobs_dir,
                system_prompt,
                input,
                input_versions,
                tools.clone(),
                execution,
            )
            .await;
        // cleanup_owner 会永久关闭该所有者；协议修复仍属同一次 attempt，退出后才清理。
        tools.cleanup_dream_processes().await;
        result
    }

    async fn explore_dream_inner(
        &self,
        jobs_dir: &Path,
        system_prompt: String,
        input: Value,
        input_versions: &BTreeMap<ClaimId, String>,
        tools: Arc<ToolRegistry>,
        execution: Arc<Execution>,
    ) -> anyhow::Result<(Plan, DateTime<Utc>, Vec<ReviewRecord>)> {
        let job_id = execution.job_id();
        let path = progress_path(jobs_dir, job_id)?;
        let fingerprint = content_hash(&(
            input_versions,
            &system_prompt,
            tools.workspace_root(),
            self.context_window,
            self.turn_loop.max_tokens(),
            self.turn_loop.history_replay_identity(),
        ))?;
        let audit = Arc::new(
            Audit::new(self.runner.maintainer_upload_queue.agent_home(), job_id)?
                .with_fingerprint(fingerprint.clone()),
        );
        audit.input(&fingerprint, &serde_json::json!({"context":input,"system_prompt":system_prompt,"input_versions":input_versions})).await?;
        let saved = reusable_progress(&path, &fingerprint).await?;
        let resuming = saved
            .as_ref()
            .is_some_and(|saved| !saved.messages.is_empty());
        let user_text = if resuming {
            "继续同一个 Dream；已保存的 Claim 和文件版本仍有效，复用已读证据，不重复目录探索。未完整读完的额外 Claim 必须从第一页重新读取。".into()
        } else {
            serde_json::to_string(&input)?
        };
        let progress = saved.unwrap_or(Exploration {
            fingerprint,
            snapshot_at: serde_json::from_value(input["dream_context"]["snapshot_at"].clone())?,
            messages: vec![],
            receipts: BTreeMap::new(),
            observed: BTreeMap::new(),
            draft: Draft::default(),
            last_error: None,
            summary: None,
        });
        tools
            .restore_dream_evidence(progress.receipts.clone(), progress.observed.clone())
            .await;
        let history = Arc::new(History::new(audit.history_root()));
        let reviewer = Arc::new(
            Reviewer::new(
                self.agent.agent_id.clone(),
                serde_json::from_value(input["claims"].clone())?,
                progress.draft.clone(),
                Some(audit.clone()),
            )
            .with_history(history.clone())
            .with_execution(execution.clone()),
        );
        reviewer.reconcile_executions().await?;
        let tools = Arc::new(
            tools
                .as_ref()
                .clone()
                .with_dream_plan_tools(reviewer.clone()),
        );
        let mut harness = ReviewHarness {
            control: execution.control.clone(),
            path,
            jobs_dir: jobs_dir.into(),
            context_window: self.context_window,
            tools: tools.clone(),
            progress,
            reviewer: reviewer.clone(),
            output_reserve: usize::try_from(self.turn_loop.max_tokens())?,
            skip_initial_save: resuming,
            history_replaced: false,
            audit: audit.clone(),
            compactor: Compactor {
                history,
                caller: self.json_caller.with_output_limit(4096),
                prompt: self
                    .prompt_registry
                    .render("dream_compaction", serde_json::json!({}))?,
                input: input.clone(),
                context_window: self.context_window,
                output_reserve: usize::try_from(self.turn_loop.max_tokens())?,
            },
        };
        let mut instruction = user_text;
        instruction.push_str(&format!(
            "\n当前执行状态（优先于初始快照和旧历史）：{}",
            execution.context().await?
        ));
        if let Some(error) = &harness.progress.last_error {
            instruction.push_str(&format!("\n上一份输出未通过宿主校验：{error}。已接受的组仍在草案中，使用 dream_stage_group 修复具体组，再调用 dream_finish；探索工具仍可使用。"));
        }
        if !reviewer.is_empty().await {
            instruction.push_str(&format!(
                "\n已接受的草案（未提交）：{}",
                serde_json::to_string(&reviewer.resume_input(&harness.progress.observed).await)?
            ));
        }
        let loop_ = self.turn_loop.for_dream(tools.clone());
        let mut request = harness.request(&system_prompt, job_id, instruction);
        let mut protocol_repairs = 0;
        let plan = loop {
            let boundary = ToolBoundaryControl::new();
            let result = {
                let mut emit = |_| {};
                let run = loop_.run_session_turn_with_hooks(
                    request,
                    &mut emit,
                    Some(boundary.clone()),
                    None,
                    Some(&mut harness),
                );
                tokio::pin!(run);
                tokio::select! {
                    biased;
                    _ = execution.control.stop.cancelled() => {
                        // 中断模型等待和后续派发；正在提交的工具必须自行完成，不能 drop 整个 turn。
                        boundary.cancel_after_running_tools(ToolCallSkipReason::TurnCancelledBeforeDispatch);
                        run.await
                    }
                    result = &mut run => result,
                }
            };
            if execution.control.stop.is_cancelled() {
                // 只保留完整请求边界；当前工具产生的草案和提交回执作为准确状态恢复。
                let messages = harness.progress.messages.clone();
                harness.save(&messages).await?;
                if let Some(error) = reviewer.take_fatal_error().await {
                    return Err(error);
                }
                audit.event(serde_json::json!({"kind":"supervisor_stopped","draft":reviewer.snapshot().await})).await?;
                return Err(DreamStopped.into());
            }
            let plan = match result {
                Err(error) if error.is::<PlanReady>() => reviewer
                    .finished()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("Dream finished without plan"))?,
                Err(error) => {
                    audit.event(serde_json::json!({"kind":"attempt_stopped","error":format!("{error:#}"),"draft":reviewer.snapshot().await,"receipts":tools.dream_evidence().await.0})).await?;
                    return Err(error);
                }
                Ok(turn) => {
                    // 兼容旧任务的完整 JSON；有分组草案时禁止文字输出悄悄丢弃已接受组。
                    let text = turn
                        .messages
                        .iter()
                        .rev()
                        .find(|message| message.role == "assistant")
                        .map(|message| {
                            message
                                .content
                                .iter()
                                .filter_map(|block| match block {
                                    SessionTurnContentBlock::Text { text } => Some(text.as_str()),
                                    _ => None,
                                })
                                .collect::<String>()
                        })
                        .unwrap_or_default();
                    let draft_empty = reviewer.is_empty().await;
                    let parsed = (|| -> anyhow::Result<Plan> {
                        anyhow::ensure!(draft_empty, "Use dream_finish to complete staged groups");
                        let plan: Plan = serde_json::from_str(text.trim()).map_err(|error| anyhow::anyhow!("invalid Dream plan JSON: {error}; use dream_stage_group and dream_finish"))?;
                        let (receipts, observed) = (
                            harness.progress.receipts.clone(),
                            harness.progress.observed.clone(),
                        );
                        let mut readable: BTreeMap<_, _> =
                            serde_json::from_value::<Vec<Claim>>(input["claims"].clone())?
                                .into_iter()
                                .map(|c| (c.id.clone(), c))
                                .collect();
                        readable.extend(observed);
                        validate_plan(
                            &plan,
                            &readable,
                            &self.agent.agent_id,
                            &receipts,
                            crate::time::now_seconds(),
                        )?;
                        Ok(plan)
                    })();
                    match parsed {
                        Ok(plan) => {
                            let finish = reviewer.import_legacy(plan).await;
                            let (receipts, observed) = tools.dream_evidence().await;
                            let feedback = reviewer
                                .call("dream_finish", finish, receipts, observed)
                                .await;
                            // 旧协议的失败提案也持久为可修订草稿；下一次 attempt 继续工具协议。
                            harness.progress.draft = reviewer.snapshot().await;
                            (harness.progress.receipts, harness.progress.observed) =
                                tools.dream_evidence().await;
                            if let Some(error) = reviewer.take_fatal_error().await {
                                write_yaml_atomic(&harness.path, &harness.progress).await?;
                                return Err(error);
                            }
                            if let Some(plan) = reviewer.finished().await {
                                plan
                            } else {
                                harness.progress.last_error =
                                    Some(serde_json::to_string(&feedback)?);
                                write_yaml_atomic(&harness.path, &harness.progress).await?;
                                request = harness.request(&system_prompt, job_id, format!("旧 JSON 计划已导入草案，尚未提交。本次结束反馈：{feedback}。有修改时调用 dream_validate 后根据原文和目标自复核，再 dream_apply_group 执行；无修改时按反馈说明保留理由；全部处理完再 dream_finish。当前草案：{}", reviewer.resume_input(&harness.progress.observed).await));
                                continue;
                            }
                        }
                        Err(error) => {
                            audit.event(serde_json::json!({"kind":"invalid_legacy_plan","proposal":text,"error":format!("{error:#}"),"draft":reviewer.snapshot().await,"receipts":tools.dream_evidence().await.0})).await?;
                            harness.progress.last_error = Some(format!("{error:#}"));
                            write_yaml_atomic(&harness.path, &harness.progress).await?;
                            // 先在同一上下文修复收尾协议；连续无效收尾仍由 supervisor 限制。
                            if protocol_repairs < 2 {
                                protocol_repairs += 1;
                                request = harness.request(&system_prompt, job_id, format!("上一条总结没有完成 Dream：{error:#}。不要重复普通文本收尾。继续使用工具修复；逐组 dream_validate、dream_apply_group，然后 dream_finish。无变化也必须调用 dream_finish；已经接受的草稿和复核仍保留。"));
                                continue;
                            }
                            return Err(error);
                        }
                    }
                }
            };
            break plan;
        };
        Ok((
            plan,
            harness.progress.snapshot_at,
            reviewer.approved().await,
        ))
    }
}
