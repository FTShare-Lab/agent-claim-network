//! 交互式 session 的运行引擎。
//!
//! SessionEngine 是多轮 session 生命周期的入口：负责启动准备、单轮 turn、
//! session 级 finalize 与运行时事件投影。它复用 AgentRunner 已有的存储、LLM、
//! maintainer 与 inbox 能力，但交互式 session 的 LLM 调用只走 provider-neutral 组件。

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::context::AgentContext;
use super::inbox::InboxJsonGenerator;
use super::runner::{AgentRunner, InboxProcessReport};
use super::runner_trace::trace_name_from_task;
use super::user_shell::{
    format_user_shell_command_record, run_user_shell_command as execute_user_shell_command,
};
use crate::api::{
    context_recovery_protected_tail_from_marker, ensure_compaction_request_within_context_window,
    estimate_session_turn_messages_tokens, project_compaction_input_media,
    project_turn_message_for_safe_transcript, trailing_model_context_segments, AgentTurnLoop,
    CompletedSessionTurnMessage, ContextUsageSnapshot, ContextUsageSource, InboxInternalizeKind,
    InternalizeRequest, MemoryReviewLoop, ModelContextSource, ProviderReplayIdentity,
    SessionAttachment, SessionCompactionOutcome, SessionTurn, SessionTurnContentBlock,
    SessionTurnContextAppender, SessionTurnEvent, SessionTurnEventRecorder, SessionTurnHooks,
    SessionTurnInterrupted, SessionTurnMessage, SessionTurnPreflight, SessionTurnRequest,
    StructuredJsonAttemptRequest, StructuredJsonCaller, ToolBoundaryControl, TurnMessage,
};
use crate::claim::{AgentId, Claim, ClaimId, DisputeId, SessionId, SourceId, TraceId};
use crate::config::{
    AgentSessionSkillConfig, AgentSessionTurnJournalConfig, AttachmentConfig,
    SessionCompactionConfig, UserShellConfig, COMPACTION_ASSET_REFERENCES_PER_TURN_MAX,
    COMPACTION_RETRY_SUMMARY_DIVISOR, DEFAULT_FORK_MEMORY_REVIEW_INTERVAL_TURNS,
    DEFAULT_SESSION_SEARCH_SQLITE_BUSY_TIMEOUT_MS,
};
use crate::delegation::{DelegationId, DelegationStatus, DelegationStore, DelegationSummary};
use crate::mcp::connection_manager::McpConnectionManager;
use crate::prompt::PromptRegistry;
use crate::session::{
    canonical_user_content_hash, read_session_turn_journal, replay_turn_journal,
    turn_journal_recovery_context_for_chain, ActiveTurnCompactionCursor, CompactedProviderHistory,
    CompactionAppliedReport, CompactionCheckpoint, CompactionCheckpointStatus, FinalizeCheckpoint,
    NewSessionMessage, PendingProviderHistoryTurn, RecoveryContextLimits, SessionCompactionState,
    SessionContentBlock, SessionHandle, SessionMessage, SessionMessageRole, SessionStatus,
    SessionStore, SessionStoreError, TurnJournalEventKind, TurnJournalFlush, TurnJournalStatus,
    TurnJournalTurn,
};
use crate::skill::{resolve_explicit_skill_instructions, SkillInjectionLimits, SkillInstructions};
use crate::storage::FileLockGuard;
use crate::tool::{BackgroundProcessEvent, ProcessCompletion, ToolRegistry};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;

mod compaction_assets;
mod compaction_projection;
mod events;
mod finalize;
mod memory_review;
mod prompts;
mod transcript;
mod turn_control;
mod turn_journal;

pub use events::{SessionEvent, SessionRuntimeStatus};
pub use turn_control::{SessionTurnControl, SessionTurnControlReceiver};

use compaction_assets::externalize_heavy_user_blocks;
use compaction_projection::*;
use events::{emit_warnings, preflight_session_event_to_turn_event};
#[cfg(test)]
use prompts::append_acn_md;
use transcript::*;
use turn_control::{spawn_turn_control_journal_forwarder, TurnControlJournalForwarder};
use turn_journal::{run_turn_journal_writer, TurnJournalDurableEventRecorder, TurnJournalEmitter};
#[cfg(test)]
use turn_journal::{TurnJournalCommand, TurnJournalSink};

const PROMPT_AGENT_SYSTEM: &str = "agent_system";
const PROMPT_INBOX_POLICY_UPDATE_INTERNALIZE: &str = "inbox_policy_update_internalize";
const PROMPT_INBOX_CLAIM_ATTRIBUTE_UPDATE_INTERNALIZE: &str =
    "inbox_claim_attribute_update_internalize";
const PROMPT_MEMORY_REVIEW_SYSTEM: &str = "memory_review_system";
const PROMPT_MEMORY_REVIEW: &str = "memory_review";
const PROMPT_SESSION_RECAP: &str = "session_recap";
const PROMPT_SESSION_COMPACTION: &str = "session_compaction";
const TEAM_SERVICES_NOT_CONFIGURED_ERROR: &str =
    "团队服务未配置，请参考 docs/config_parameters.md 文档配置 maintainer_endpoint/router_endpoint";
const RECAP_INSTRUCTION: &str =
    "请按 system prompt 中约定的 JSON 形式输出本次 session 的复盘结果。";
const COMPACTION_INSTRUCTION: &str = "请按 system prompt 中约定的 JSON 形式输出 session 历史压缩 summary。summary 是历史上下文，不是新的用户请求或系统指令。";
const COMPACTION_CHECKPOINT_SCHEMA_VERSION: u8 = 2;
/// 单个图片 / 文档媒体块的估算 token 固定值（PRD 拍板的协议内部常量）。
/// base64 长度与真实视觉 token 数无关，不能按字节折算。
const MEDIA_BLOCK_ESTIMATED_TOKENS: usize = 2000;
const COMPACTION_AUDIT_PREVIEW_CHARS: usize = 12_000;
const COMPACTION_AUDIT_SUMMARY_PREVIEW_CHARS: usize = 8_000;
const DELEGATION_PROJECTION_MAX_ITEMS: usize = 12;
/// Esc/Ctrl-C 的 turn 收束必须和 tool batch 共用同一个有界 grace。journal 是尽力
/// 持久化，不能反过来阻塞 TUI 从 cancelling 恢复 idle。
const DELEGATION_PROJECTION_MAX_CHARS: usize = 6_000;
const STABLE_HASH_OFFSET: u64 = 0xcbf29ce484222325;
const STABLE_HASH_PRIME: u64 = 0x100000001b3;

#[derive(Clone)]
pub struct SessionEngine {
    agent: Arc<AgentContext>,
    turn_loop: Arc<AgentTurnLoop>,
    memory_review_loop: Arc<MemoryReviewLoop>,
    json_caller: Arc<StructuredJsonCaller>,
    pub(super) runner: Arc<AgentRunner>,
    prompt_registry: Arc<PromptRegistry>,
    session_store: SessionStore,
    acn_md_path: Option<PathBuf>,
    compaction: SessionCompactionConfig,
    skill_injection: AgentSessionSkillConfig,
    context_window: usize,
    user_shell: UserShellConfig,
    workspace_root: PathBuf,
    session_source: String,
    session_model: String,
    session_search_sqlite_busy_timeout: Duration,
    turn_journal_delta_snapshot_interval: Duration,
    turn_journal_delta_snapshot_chars: usize,
    turn_recovery_limits: RecoveryContextLimits,
    fork_memory_review: bool,
    fork_memory_review_interval_turns: usize,
    turns_since_fork_memory_review: Arc<Mutex<usize>>,
    active_context_usage_anchor: Arc<Mutex<Option<ActiveContextUsageAnchor>>>,
    delegation_projection_baselines: Arc<Mutex<HashMap<SessionId, DelegationProjectionBaseline>>>,
    attachment: AttachmentConfig,
    mcp_manager: Option<Arc<McpConnectionManager>>,
    subagent_max_concurrent: usize,
    runtime_profile: SessionRuntimeProfile,
}

/// session 的运行边界。评测模式不读取 inbox、私有 memory 或 ACN.md。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionRuntimeProfile {
    Interactive,
    Evaluation,
}

/// 运行时直接 abort 会跳过 turn 的正常收束路径；此 guard 只回滚本轮新增或扩展的
/// 文件读取许可，不尝试回滚真实文件、进程或网络副作用。
struct FileReadStateCheckpointOnDrop {
    turn_loop: Arc<AgentTurnLoop>,
    session_id: SessionId,
    turn_id: String,
    armed: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("session turn 已写入 canonical transcript，但提交后清理失败: {source}")]
struct SessionTurnCommittedPostCommitError {
    #[source]
    source: anyhow::Error,
}

impl FileReadStateCheckpointOnDrop {
    fn new(turn_loop: Arc<AgentTurnLoop>, session_id: SessionId, turn_id: String) -> Self {
        Self {
            turn_loop,
            session_id,
            turn_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for FileReadStateCheckpointOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            log::warn!(
                target: "agent",
                "turn {} dropped without Tokio runtime; file read state checkpoint cannot roll back",
                self.turn_id
            );
            return;
        };
        let turn_loop = Arc::clone(&self.turn_loop);
        let session_id = self.session_id.clone();
        let turn_id = self.turn_id.clone();
        runtime.spawn(async move {
            if let Err(error) = turn_loop
                .rollback_file_read_state_checkpoint(&session_id, &turn_id)
                .await
            {
                log::warn!(
                    target: "agent",
                    "turn {turn_id} drop 后回滚 file read state checkpoint 失败: {error}"
                );
                turn_loop.clear_parent_file_read_state(&session_id).await;
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct SessionEngineOptions {
    pub compaction: SessionCompactionConfig,
    pub skills: AgentSessionSkillConfig,
    pub context_window: usize,
    pub user_shell: UserShellConfig,
    pub workspace_root: PathBuf,
    pub turn_journal: AgentSessionTurnJournalConfig,
    pub subagent_max_concurrent: usize,
    pub runtime_profile: SessionRuntimeProfile,
}

#[derive(Debug, Clone)]
pub struct SessionStartReport {
    pub session: SessionHandle,
    pub inbox_report: InboxProcessReport,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SessionFinalizeReport {
    pub trace_id: Option<TraceId>,
    pub new_claim_ids: Vec<ClaimId>,
    pub updated_claim_ids: Vec<ClaimId>,
    pub used_claim_ids: Vec<ClaimId>,
    pub new_dispute_ids: Vec<DisputeId>,
    pub advanced_recapped_until: bool,
    pub finalized_unrecapped_messages: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct AppliedCompactionOutcome {
    state: SessionCompactionState,
    report: SessionFinalizeReport,
    audit_ids: Vec<String>,
    recovered: bool,
    preflight_projection: Option<ProviderProjection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionCompactionNoopReason {
    NothingNew,
    RawTailWithinBudget,
}

#[derive(Debug, Clone)]
pub enum SessionCompactionResult {
    Compacted(SessionCompactionState),
    Noop(SessionCompactionNoopReason),
}

#[derive(Debug, Clone)]
enum ManualCompactionOutcome {
    Compacted(Box<AppliedCompactionOutcome>),
    Noop(SessionCompactionNoopReason),
}

struct PreparedSessionTurn {
    previous_message_count: usize,
    turn: SessionTurn,
    provider_context_used_tokens: Option<usize>,
    compacted_provider_history: Option<Vec<SessionTurnMessage>>,
    provider_replay_identity: Option<ProviderReplayIdentity>,
}

struct CommittedSessionTurn {
    message_count: usize,
    provider_context_usage_observed: bool,
}

struct RunTurnInnerRequest {
    turn_id: String,
    recovered_model_context: Vec<CompletedSessionTurnMessage>,
    user_text: String,
    user_attachments: Vec<SessionAttachment>,
    skill_instructions: Vec<SkillInstructions>,
    tool_boundary_control: Option<ToolBoundaryControl>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveContextUsageAnchor {
    session_id: SessionId,
    message_count: usize,
    used_tokens: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProviderContextUsageAnchor {
    provider_message_count: usize,
    used_tokens: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreflightRuntimeProjectionBudget {
    runtime_projection_tokens: usize,
    provider_projection: ProviderProjectionBudget,
}

struct PreflightCompactionRequest<'a> {
    base_system_prompt: &'a str,
    active_suffix: Vec<SessionTurnMessage>,
    turn_id: &'a str,
    base_message_count: usize,
    active_projection_compacted: bool,
    runtime_projection_tokens: usize,
    protected_active_tail_segments: usize,
}

#[derive(Clone, Copy)]
struct PreflightProjectionInputs<'a> {
    base_system_prompt: &'a str,
    session_messages: &'a [SessionMessage],
    active_suffix: &'a [SessionTurnMessage],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveTurnPlan {
    summary_start_segment: usize,
    summary_end_segment: usize,
    cursor: ActiveTurnCompactionCursor,
    transcript: Vec<TurnMessage>,
    transcript_with_large_tool_results_omitted: Vec<TurnMessage>,
    transcript_with_tool_results_omitted: Vec<TurnMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreflightCompactionPlan {
    ranges: CompactionRanges,
    committed_transcript: Option<Vec<TurnMessage>>,
    committed_transcript_with_large_tool_results_omitted: Option<Vec<TurnMessage>>,
    committed_transcript_with_tool_results_omitted: Option<Vec<TurnMessage>>,
    active_turn: Option<ActiveTurnPlan>,
    prior_active_turn_summary: Option<String>,
    prior_active_turn_cursor: Option<ActiveTurnCompactionCursor>,
    turn_id: String,
    base_message_count: usize,
    runtime_budget: PreflightRuntimeProjectionBudget,
    protected_active_tail_segments: usize,
}

struct PreflightCompactor<'a> {
    engine: &'a SessionEngine,
    session: &'a mut SessionHandle,
    active_start_index: usize,
    turn_id: String,
    base_message_count: usize,
    active_projection_compacted: bool,
    provider_context_anchor: Option<ProviderContextUsageAnchor>,
    context_window_recovery_requested: bool,
    context_window_recovery_tail_marker: Option<SessionTurnMessage>,
    history_replaced_since_last_check: bool,
    frozen_provider_history_prefix_len: usize,
    capture_provider_history: bool,
    last_compacted_provider_history: Option<Vec<SessionTurnMessage>>,
    provider_compaction_before_pending_request: Option<Option<SessionCompactionState>>,
    background_completion_delivery_seq: Arc<AtomicU64>,
    provider_replay_identity: Option<ProviderReplayIdentity>,
}

struct MainModelContextAppender {
    tools: Arc<ToolRegistry>,
    session_id: SessionId,
    session_dir: PathBuf,
    delegation_activity: Option<tokio::sync::watch::Receiver<u64>>,
    delegation_projection_baselines: Arc<Mutex<HashMap<SessionId, DelegationProjectionBaseline>>>,
    observed_delegation_baseline: Option<DelegationProjectionBaseline>,
    background_completion_delivery_ids: Vec<crate::tool::ProcessCompletionDeliveryReceipt>,
    background_completion_until_seq: u64,
    background_completion_delivery_seq: Arc<AtomicU64>,
}

#[derive(Clone)]
struct DelegationProjectionBaseline {
    activity_revision: Option<u64>,
    message: SessionTurnMessage,
}

#[async_trait]
impl SessionTurnContextAppender for MainModelContextAppender {
    async fn observe_context(
        &mut self,
        provider_messages: &[SessionTurnMessage],
    ) -> anyhow::Result<Vec<SessionTurnMessage>> {
        let mut pending = Vec::new();

        self.tools
            .rollback_process_deliveries_for_owner(&self.session_id, None)
            .await;
        let (delivery_ids, frozen_completions) = self
            .tools
            .begin_background_completion_delivery_for_owner(&self.session_id, None)
            .await;
        self.background_completion_delivery_ids = delivery_ids;
        persist_main_background_process_completions(
            self.tools.as_ref(),
            &self.session_id,
            &self.session_dir,
            &frozen_completions,
        )
        .await?;
        let journal = read_session_turn_journal(&self.session_dir.join("turn_events.jsonl")).await;
        for warning in &journal.warnings {
            log::warn!(
                target: "agent",
                "background completion journal 读取降级 session={} line={:?}: {}",
                self.session_id,
                warning.line,
                warning.message
            );
        }
        let owner = self.tools.process_owner_for_session(&self.session_id, None);
        let mut persisted_completions = Vec::new();
        let mut journaled_terminal_instances = BTreeSet::new();
        let mut delivered_through = self.background_completion_until_seq;
        for event in journal.events {
            let TurnJournalEventKind::BackgroundProcessCompleted {
                tool_use_id,
                process_id,
                instance_id,
                status,
                exit_code,
                signal,
                success,
            } = event.kind
            else {
                continue;
            };
            journaled_terminal_instances.insert((process_id.clone(), instance_id));
            if event.seq <= self.background_completion_until_seq {
                continue;
            }
            delivered_through = delivered_through.max(event.seq);
            persisted_completions.push(ProcessCompletion {
                root_session_id: self.session_id.to_string(),
                owner: owner.clone(),
                process_id,
                originating_turn_id: Some(event.turn_id),
                originating_tool_use_id: Some(tool_use_id),
                instance_id,
                status,
                exit_code,
                signal,
                success,
                finished_at: SystemTime::now(),
                elapsed_minutes: 0,
            });
        }
        let background = self
            .tools
            .background_process_projection_for_owner_with_journaled_terminals(
                &self.session_id,
                None,
                persisted_completions,
                &journaled_terminal_instances,
            )
            .await;
        if delivered_through > self.background_completion_until_seq {
            self.background_completion_delivery_seq
                .store(delivered_through, Ordering::Release);
        }
        match background {
            Some(text) => pending.push(SessionTurnMessage::model_context(
                ModelContextSource::BackgroundProcess,
                text,
            )),
            None => pending.push(SessionTurnMessage::model_context(
                ModelContextSource::BackgroundProcess,
                ToolRegistry::empty_background_process_projection(),
            )),
        }

        let activity_revision = self
            .delegation_activity
            .as_ref()
            .map(|receiver| *receiver.borrow());
        let cached_baseline = match self.observed_delegation_baseline.as_ref() {
            Some(baseline) => Some(baseline.clone()),
            None => self
                .delegation_projection_baselines
                .lock()
                .map_err(|_| anyhow::anyhow!("delegation projection baseline lock poisoned"))?
                .get(&self.session_id)
                .cloned(),
        };
        if let Some(baseline) =
            cached_baseline.filter(|baseline| baseline.activity_revision == activity_revision)
        {
            // Compaction 可能保留了一份更早的 delegation snapshot、却压掉最新一份。
            // revision 未变时直接复用已获 provider 确认的精确快照；不能只检查 source
            // 是否存在，否则新 compact window 会退回陈旧状态。
            pending.push(baseline.message.clone());
            if !latest_model_context_matches(provider_messages, &baseline.message) {
                self.observed_delegation_baseline = Some(baseline);
            }
        } else {
            let text = delegation_summary_projection(&self.session_dir)
                .await?
                .unwrap_or(empty_delegation_summary_projection()?);
            let message = SessionTurnMessage::model_context(ModelContextSource::Delegation, text);
            self.observed_delegation_baseline = Some(DelegationProjectionBaseline {
                activity_revision,
                message: message.clone(),
            });
            pending.push(message);
        }

        Ok(pending)
    }

    async fn after_provider_response_success(&mut self) -> anyhow::Result<()> {
        if let Some(baseline) = self.observed_delegation_baseline.take() {
            self.delegation_projection_baselines
                .lock()
                .map_err(|_| anyhow::anyhow!("delegation projection baseline lock poisoned"))?
                .insert(self.session_id.clone(), baseline);
        }
        if !self.background_completion_delivery_ids.is_empty() {
            self.tools
                .commit_completion_notification_delivery_for_owner(
                    &self.session_id,
                    None,
                    &self.background_completion_delivery_ids,
                )
                .await;
            self.background_completion_delivery_ids.clear();
        }
        let delivered_through = self
            .background_completion_delivery_seq
            .swap(0, Ordering::AcqRel);
        self.background_completion_until_seq =
            self.background_completion_until_seq.max(delivered_through);
        Ok(())
    }
}

/// completion 只有在 journal 获得稳定 seq 后才允许进入 main provider projection。
/// 独立锁把 TUI heartbeat 与 provider preflight 的“查重 + append + ack”线性化。
async fn persist_main_background_process_completions(
    tools: &ToolRegistry,
    session_id: &SessionId,
    session_dir: &Path,
    completions: &[ProcessCompletion],
) -> anyhow::Result<()> {
    if completions.is_empty() {
        return Ok(());
    }
    let _guard = FileLockGuard::lock_exclusive(session_dir.join("background_completion.lock"))
        .await
        .context("获取 background completion journal 锁失败")?;
    let journal_path = session_dir.join("turn_events.jsonl");
    let journal = read_session_turn_journal(&journal_path).await;
    let mut journaled_instances = journal
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            TurnJournalEventKind::BackgroundProcessCompleted {
                process_id,
                instance_id,
                ..
            } => Some((process_id.clone(), *instance_id)),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut writer = crate::session::TurnJournalWriter::open(journal_path).await?;
    for completion in completions {
        let allocation = (completion.process_id.clone(), completion.instance_id);
        if !journaled_instances.contains(&allocation) {
            let turn_id = completion
                .originating_turn_id
                .as_ref()
                .context("main background completion 缺少 originating turn")?;
            let tool_use_id = completion
                .originating_tool_use_id
                .as_ref()
                .context("main background completion 缺少 originating tool use")?;
            writer
                .append(
                    turn_id.clone(),
                    Utc::now(),
                    TurnJournalEventKind::BackgroundProcessCompleted {
                        tool_use_id: tool_use_id.clone(),
                        process_id: completion.process_id.clone(),
                        instance_id: completion.instance_id,
                        status: completion.status.clone(),
                        exit_code: completion.exit_code,
                        signal: completion.signal,
                        success: completion.success,
                    },
                    TurnJournalFlush::Immediate,
                )
                .await?;
            journaled_instances.insert(allocation);
        }
        tools
            .acknowledge_process_completion_for_root_session(session_id, completion.instance_id)
            .await;
    }
    Ok(())
}

fn latest_model_context_matches(
    messages: &[SessionTurnMessage],
    expected: &SessionTurnMessage,
) -> bool {
    let Some((expected_source, expected_fingerprint, expected_text)) =
        expected.model_context_snapshot()
    else {
        return false;
    };
    messages
        .iter()
        .rev()
        .find_map(|message| {
            let snapshot = message.model_context_snapshot()?;
            (*snapshot.0 == *expected_source).then_some(snapshot)
        })
        .is_some_and(|(_, fingerprint, text)| {
            fingerprint == expected_fingerprint && text == expected_text
        })
}

#[async_trait]
impl SessionTurnPreflight for PreflightCompactor<'_> {
    fn frozen_provider_history_prefix_len(&self) -> usize {
        self.frozen_provider_history_prefix_len
    }

    async fn before_provider_request(
        &mut self,
        system_prompt: &mut String,
        provider_messages: &mut Vec<SessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        let forced_context_recovery = std::mem::take(&mut self.context_window_recovery_requested);
        let Some(active_suffix_raw) = provider_messages
            .get(self.active_start_index..)
            .map(|messages| messages.to_vec())
        else {
            return Ok(());
        };
        let active_suffix = active_suffix_raw;
        let trigger_threshold = auto_compact_trigger_threshold_tokens(
            self.engine.context_window,
            self.engine.compaction.auto_compact_ctx_ratio,
        );
        if trigger_threshold == 0 {
            if forced_context_recovery {
                anyhow::bail!("模型上下文已满，但自动压缩已关闭。请启用自动压缩或新建会话。");
            }
            return Ok(());
        }
        let trigger_tokens = self.trigger_context_tokens(system_prompt, provider_messages);
        if !forced_context_recovery
            && !auto_compact_should_trigger(trigger_tokens, trigger_threshold)
        {
            return Ok(());
        }
        let projected_base_system_prompt = system_prompt.clone();
        let segments = active_provider_safe_segments(&active_suffix);
        let recovery_protected_tail_segments =
            match self.context_window_recovery_tail_marker.as_ref() {
                Some(marker) => {
                    context_recovery_protected_tail_from_marker(&active_suffix, &segments, marker)
                        .context("上下文续写状态异常，无法自动恢复")?
                }
                None => 0,
            };
        if forced_context_recovery && recovery_protected_tail_segments == 0 {
            anyhow::bail!("上下文续写状态异常，无法自动恢复");
        }
        let protected_active_tail_segments = recovery_protected_tail_segments
            .max(trailing_model_context_segments(&active_suffix, &segments));
        let projection = match self
            .engine
            .compact_provider_preflight(
                self.session,
                PreflightCompactionRequest {
                    base_system_prompt: &projected_base_system_prompt,
                    active_suffix,
                    turn_id: &self.turn_id,
                    base_message_count: self.base_message_count,
                    active_projection_compacted: self.active_projection_compacted,
                    // 持久化 context 已经在 provider_messages 中计入，不再另做 runtime reserve。
                    runtime_projection_tokens: 0,
                    protected_active_tail_segments,
                },
                emit,
            )
            .await
        {
            Ok(Some(projection)) => projection,
            Ok(None) => {
                if forced_context_recovery {
                    anyhow::bail!("模型上下文已满，但没有可安全压缩的历史。请简化任务后重试。");
                }
                return Ok(());
            }
            Err(error) => {
                if forced_context_recovery {
                    return Err(error.context("模型上下文已满，自动压缩失败。请重试或新建会话。"));
                }
                let Some(recoverable) =
                    error.downcast_ref::<RecoverableCompactionPreparationError>()
                else {
                    return Err(error);
                };
                if self.raw_request_with_output_fits_context(trigger_tokens) {
                    let warning = recoverable.continuation_warning();
                    self.engine
                        .append_session_event_log(self.session, "WARN", &warning)
                        .await;
                    emit(SessionTurnEvent::CompactionSkipped { warning });
                    return Ok(());
                }
                let message = recoverable.blocking_message();
                return Err(error.context(message));
            }
        };
        *system_prompt = projection.system_prompt;
        *provider_messages = projection.messages;
        // compact 已替换 parent 的旧正文；child 上下文未变化，不能撤销其独立许可。
        self.engine
            .turn_loop
            .clear_parent_file_read_state(&self.session.metadata.id)
            .await;
        self.active_start_index = projection.active_start_index;
        self.active_projection_compacted = true;
        self.history_replaced_since_last_check = true;
        self.capture_provider_history = true;
        self.provider_context_anchor = None;
        self.engine
            .clear_active_context_usage_anchor(&self.session.metadata.id);
        if forced_context_recovery {
            let projected_tokens = self
                .engine
                .turn_loop
                .estimate_context_tokens(system_prompt, provider_messages);
            if projected_tokens >= trigger_tokens {
                log::warn!(
                    target: "agent",
                    "context recovery compaction 未缩小请求：压缩前估算 {trigger_tokens} tokens，压缩后估算 {projected_tokens} tokens"
                );
                anyhow::bail!("自动压缩未能释放上下文空间。请简化任务或新建会话。");
            }
        }
        Ok(())
    }

    fn request_context_window_recovery(
        &mut self,
        assistant_marker: &SessionTurnMessage,
    ) -> anyhow::Result<()> {
        if self.engine.compaction.auto_compact_ctx_ratio == 0.0 {
            anyhow::bail!("模型上下文已满，但自动压缩已关闭。请启用自动压缩或新建会话。");
        }
        self.context_window_recovery_tail_marker
            .get_or_insert_with(|| assistant_marker.clone());
        self.context_window_recovery_requested = true;
        Ok(())
    }

    fn observe_provider_context_usage(
        &mut self,
        provider_message_count: usize,
        usage: ContextUsageSnapshot,
    ) {
        if usage.source == ContextUsageSource::Provider {
            self.provider_context_anchor = Some(ProviderContextUsageAnchor {
                provider_message_count,
                used_tokens: usage.used_tokens,
            });
        }
    }

    fn clear_provider_context_usage(&mut self) {
        self.provider_context_anchor = None;
    }

    fn history_replacement_expected(
        &self,
        system_prompt: &str,
        provider_messages: &[SessionTurnMessage],
    ) -> bool {
        let trigger_threshold = auto_compact_trigger_threshold_tokens(
            self.engine.context_window,
            self.engine.compaction.auto_compact_ctx_ratio,
        );
        trigger_threshold != 0
            && (self.context_window_recovery_requested
                || auto_compact_should_trigger(
                    self.trigger_context_tokens(system_prompt, provider_messages),
                    trigger_threshold,
                ))
    }

    fn take_history_replaced_since_last_check(&mut self) -> bool {
        std::mem::take(&mut self.history_replaced_since_last_check)
    }

    async fn provider_request_ready(
        &mut self,
        provider_messages: &[SessionTurnMessage],
        canonical_tail_count: usize,
    ) -> anyhow::Result<()> {
        self.persist_provider_history(
            provider_messages,
            canonical_tail_count,
            provider_messages.len(),
        )
        .await
    }

    async fn provider_response_ready(
        &mut self,
        provider_messages: &[SessionTurnMessage],
        canonical_tail_count: usize,
    ) -> anyhow::Result<()> {
        let metadata = self.session.read_metadata().await?;
        let provider_history = metadata
            .compaction
            .as_ref()
            .and_then(|compaction| compaction.provider_history.as_ref())
            .context("Provider response 固化前缺少 request WAL")?;
        let pending = provider_history
            .pending_turn
            .as_ref()
            .filter(|pending| pending.turn_id == self.turn_id)
            .context("Provider response 固化前 request WAL 不属于当前 turn")?;
        let provider_request_message_count = pending
            .provider_request_message_count
            .unwrap_or(provider_history.messages.len());
        if provider_request_message_count > provider_messages.len() {
            anyhow::bail!(
                "Provider response history 短于最后一次请求: request={}, response={}",
                provider_request_message_count,
                provider_messages.len()
            );
        }
        self.persist_provider_history(
            provider_messages,
            canonical_tail_count,
            provider_request_message_count,
        )
        .await
    }

    fn provider_request_started(
        &mut self,
        _provider_messages: &[SessionTurnMessage],
    ) -> anyhow::Result<()> {
        // 网络发送一旦开始，结果在 crash/cancel 下就可能已被上游接受；保守保留 WAL。
        self.provider_compaction_before_pending_request = None;
        Ok(())
    }

    async fn provider_request_abandoned_before_send(&mut self) -> anyhow::Result<()> {
        let Some(previous) = self.provider_compaction_before_pending_request.take() else {
            return Ok(());
        };
        match previous {
            Some(compaction) => self.session.update_compaction(compaction).await?,
            None => self.session.clear_compaction().await?,
        }
        self.last_compacted_provider_history = self
            .session
            .read_metadata()
            .await?
            .compaction
            .and_then(|state| state.provider_history)
            .map(|history| history.messages);
        Ok(())
    }

    async fn after_provider_response_success(&mut self) -> anyhow::Result<()> {
        let delivered_through = self
            .background_completion_delivery_seq
            .load(Ordering::Acquire);
        if delivered_through > 0 {
            self.session
                .advance_provider_background_completion_until(delivered_through)
                .await?;
        }
        Ok(())
    }
}

impl PreflightCompactor<'_> {
    async fn persist_provider_history(
        &mut self,
        provider_messages: &[SessionTurnMessage],
        canonical_tail_count: usize,
        provider_request_message_count: usize,
    ) -> anyhow::Result<()> {
        if !self.capture_provider_history {
            return Ok(());
        }
        let canonical_message_until = self
            .base_message_count
            .checked_add(canonical_tail_count)
            .context("compacted provider history canonical cursor 溢出")?;
        if provider_request_message_count > provider_messages.len() {
            anyhow::bail!(
                "Provider request boundary 越界: request={}, history={}",
                provider_request_message_count,
                provider_messages.len()
            );
        }
        let metadata = self.session.read_metadata().await?;
        // 稳定 Provider 窗口同时是所有 main request 的 WAL，不能把
        // “是否曾发生过语义 compaction”当成恢复正确性的开关。
        // 尚无 compaction state 时复用空 summary 的同一有界窗口，
        // 不建立另一个持久化事实源。
        self.provider_compaction_before_pending_request = Some(metadata.compaction.clone());
        let mut compaction = metadata.compaction.unwrap_or_else(|| {
            SessionCompactionState::from_committed_summary(0, String::new(), Utc::now())
        });
        let messages = provider_messages.to_vec();
        compaction.provider_history = Some(Box::new(CompactedProviderHistory {
            replay_identity: self.provider_replay_identity.clone(),
            pending_turn: Some(PendingProviderHistoryTurn {
                turn_id: self.turn_id.clone(),
                base_message_count: self.base_message_count,
                provider_request_message_count: Some(provider_request_message_count),
            }),
            canonical_message_until,
            messages: messages.clone(),
        }));
        self.session.update_compaction(compaction).await?;
        self.last_compacted_provider_history = Some(messages);
        Ok(())
    }

    fn raw_request_with_output_fits_context(&self, input_tokens: usize) -> bool {
        let output_tokens =
            usize::try_from(self.engine.turn_loop.max_tokens()).unwrap_or(usize::MAX);
        input_tokens.saturating_add(output_tokens) <= self.engine.context_window
    }

    fn trigger_context_tokens(
        &self,
        system_prompt: &str,
        provider_messages: &[SessionTurnMessage],
    ) -> usize {
        if let Some(anchor) = self
            .provider_context_anchor
            .as_ref()
            .filter(|anchor| anchor.provider_message_count <= provider_messages.len())
        {
            let anchored_tokens =
                anchor
                    .used_tokens
                    .saturating_add(estimate_session_turn_messages_tokens(
                        &provider_messages[anchor.provider_message_count..],
                    ));
            return anchored_tokens.max(
                self.engine
                    .turn_loop
                    .estimate_context_tokens(system_prompt, provider_messages),
            );
        }
        if let Some(anchor) = self
            .engine
            .active_context_usage_anchor(&self.session.metadata.id, self.base_message_count)
        {
            if let Some(active_suffix) = provider_messages.get(self.active_start_index..) {
                let anchored_tokens = anchor
                    .used_tokens
                    .saturating_add(estimate_session_turn_messages_tokens(active_suffix));
                return anchored_tokens.max(
                    self.engine
                        .turn_loop
                        .estimate_context_tokens(system_prompt, provider_messages),
                );
            }
        }
        self.engine
            .turn_loop
            .estimate_context_tokens(system_prompt, provider_messages)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactionRanges {
    summary_start_index: usize,
    summary_end_index: usize,
    recap_start_index: usize,
    recap_end_index: usize,
}

#[derive(Debug, Serialize)]
struct SessionRecapPayload<'a> {
    instruction: &'a str,
    transcript: &'a [TurnMessage],
    #[serde(skip_serializing_if = "Option::is_none")]
    background_process_completions: Option<&'a SessionRecapBackgroundProcessProjection>,
    local_claims: &'a [Claim],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SessionRecapBackgroundProcessProjection {
    #[serde(skip)]
    consumed_through_seq: u64,
    omitted_older_count: usize,
    items: Vec<SessionRecapBackgroundProcessCompletion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SessionRecapBackgroundProcessCompletion {
    turn_id: String,
    tool_use_id: String,
    process_id: String,
    status: String,
    exit_code: Option<i32>,
    signal: Option<i32>,
    success: bool,
}

#[derive(Debug, Serialize)]
struct SessionCompactionPayload<'a> {
    instruction: &'a str,
    agent_id: &'a str,
    committed_start_index: Option<usize>,
    committed_end_index: Option<usize>,
    prior_committed_summary: Option<&'a str>,
    committed_transcript: Option<&'a [TurnMessage]>,
    prior_active_turn_summary: Option<&'a str>,
    active_turn_user_anchor: Option<&'a SessionTurnMessage>,
    active_turn_start_segment: Option<usize>,
    active_turn_end_segment: Option<usize>,
    active_turn_transcript: Option<&'a [TurnMessage]>,
    summary_max_chars: usize,
}

#[derive(Clone, Copy)]
struct CompactionSummaryInputs<'a> {
    audit: CompactionAuditSummaryContext<'a>,
    committed_start_index: Option<usize>,
    committed_end_index: Option<usize>,
    prior_committed_summary: Option<&'a str>,
    committed_transcript: Option<&'a [TurnMessage]>,
    committed_transcript_with_large_tool_results_omitted: Option<&'a [TurnMessage]>,
    committed_transcript_with_tool_results_omitted: Option<&'a [TurnMessage]>,
    prior_active_turn_summary: Option<&'a str>,
    active_turn_user_anchor: Option<&'a SessionTurnMessage>,
    active_turn_start_segment: Option<usize>,
    active_turn_end_segment: Option<usize>,
    active_turn_transcript: Option<&'a [TurnMessage]>,
    active_turn_transcript_with_large_tool_results_omitted: Option<&'a [TurnMessage]>,
    active_turn_transcript_with_tool_results_omitted: Option<&'a [TurnMessage]>,
    summary_max_chars: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CompactionAuditTrigger {
    AutoPreflight,
    ManualCheckpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CompactionAuditScope {
    Committed,
    ActiveTurn,
    Mixed,
}

#[derive(Debug, Clone, Copy)]
struct CompactionAuditSummaryContext<'a> {
    trigger: CompactionAuditTrigger,
    scope: CompactionAuditScope,
    turn_id: Option<&'a str>,
    base_message_count: Option<usize>,
    ranges: CompactionRanges,
}

#[derive(Debug, Clone)]
struct GeneratedCompactionSummary {
    outcome: SessionCompactionOutcome,
    audit_id: String,
}

/// 已完成本地预算预检、但尚未发给 provider 的压缩摘要请求。
///
/// 将预检与模型调用分开后，已确认可发起的摘要和 recap 可以并行；预检失败时
/// 则不会启动任何 recap 请求。
#[derive(Debug)]
struct PreparedCompactionSummaryRequest {
    system_prompt: String,
    provider_messages: Vec<SessionTurnMessage>,
    payload_preview: CompactionAuditTextPreview,
}

#[derive(Debug, thiserror::Error)]
#[error("{field} exceeds summary_max_chars: actual_chars={actual_chars}, max_chars={max_chars}")]
struct CompactionSummaryTooLong {
    field: &'static str,
    actual_chars: usize,
    max_chars: usize,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "Compacted provider projection still exceeds hard tail budget: estimated raw tail tokens={raw_tail_tokens}, runtime projection tokens={runtime_projection_tokens}, combined tail tokens={projected_tokens}, hard tail budget={hard_limit}."
)]
struct CompactionProjectionTooLarge {
    raw_tail_tokens: usize,
    runtime_projection_tokens: usize,
    projected_tokens: usize,
    hard_limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoverableCompactionPreparationKind {
    SummaryTooLong { max_chars: usize, attempts: u32 },
    Other,
}

#[derive(Debug, thiserror::Error)]
#[error("{source:#}")]
pub(super) struct RecoverableCompactionPreparationError {
    pub(super) kind: RecoverableCompactionPreparationKind,
    #[source]
    source: anyhow::Error,
}

impl RecoverableCompactionPreparationError {
    fn from_summary_call(source: anyhow::Error, attempts: u32) -> Self {
        let kind = source
            .downcast_ref::<CompactionSummaryTooLong>()
            .map(
                |error| RecoverableCompactionPreparationKind::SummaryTooLong {
                    max_chars: error.max_chars,
                    attempts,
                },
            )
            .unwrap_or(RecoverableCompactionPreparationKind::Other);
        Self { kind, source }
    }

    pub(super) fn other(source: anyhow::Error) -> Self {
        Self {
            kind: RecoverableCompactionPreparationKind::Other,
            source,
        }
    }

    fn from_projection_failure(source: anyhow::Error) -> anyhow::Error {
        if source
            .downcast_ref::<CompactionProjectionTooLarge>()
            .is_some()
        {
            Self::other(source).into()
        } else {
            source
        }
    }

    fn blocking_message(&self) -> String {
        match self.kind {
            RecoverableCompactionPreparationKind::SummaryTooLong {
                max_chars,
                attempts,
            } => format!(
                "Context compaction failed: the generated summary exceeded {} characters after {attempts} attempts. Run /compact to retry.",
                format_count(max_chars),
            ),
            RecoverableCompactionPreparationKind::Other => format!(
                "Context compaction failed: {:#}. Run /compact to retry.",
                self.source
            ),
        }
    }

    fn continuation_warning(&self) -> String {
        match self.kind {
            RecoverableCompactionPreparationKind::SummaryTooLong { attempts, .. } => format!(
                "Automatic compaction failed after {attempts} attempts; continuing with full history."
            ),
            RecoverableCompactionPreparationKind::Other => format!(
                "Automatic compaction failed; continuing with full history. Details: {:#}",
                self.source
            ),
        }
    }
}

#[derive(Debug, Serialize)]
struct CompactionAuditEvent {
    created_at: DateTime<Utc>,
    #[serde(flatten)]
    kind: CompactionAuditEventKind,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CompactionAuditEventKind {
    Started {
        audit_id: String,
        trigger: CompactionAuditTrigger,
        scope: CompactionAuditScope,
        compact_start_index: usize,
        compact_end_index: usize,
        recap_start_index: usize,
        recap_end_index: usize,
        turn_id: Option<String>,
        base_message_count: Option<usize>,
        payload: CompactionAuditTextPreview,
    },
    ModelAttempt {
        audit_id: String,
        attempt: u32,
        retry_total: u32,
        raw_text: Option<CompactionAuditTextPreview>,
        parsed_json: Option<CompactionAuditTextPreview>,
        error: Option<String>,
        will_retry: bool,
    },
    ProjectionExternalized {
        audit_id: String,
        asset_count: usize,
        retained_block_count: usize,
        raw_tail_tokens_before: usize,
        raw_tail_tokens_after: usize,
    },
    Completed {
        audit_id: String,
        recovered: bool,
        compacted_until: usize,
        recapped_until: usize,
        committed_summary: Option<CompactionAuditTextPreview>,
        active_turn_summary: Option<CompactionAuditTextPreview>,
        active_turn: Option<ActiveTurnCompactionCursor>,
        new_claim_ids: Vec<ClaimId>,
        updated_claim_ids: Vec<ClaimId>,
        new_dispute_ids: Vec<DisputeId>,
    },
    Failed {
        audit_id: String,
        error: String,
    },
}

#[derive(Debug, Serialize)]
struct CompactionAuditTextPreview {
    chars: usize,
    hash: String,
    preview: String,
    truncated: bool,
}

struct SessionInboxJsonGenerator<'a> {
    prompt_registry: &'a PromptRegistry,
    json_caller: &'a StructuredJsonCaller,
    fallback_scope: crate::api::ProviderRuntimeFallbackScope,
}

#[async_trait]
impl InboxJsonGenerator for SessionInboxJsonGenerator<'_> {
    async fn generate_json(
        &self,
        kind: InboxInternalizeKind,
        request: InternalizeRequest,
        preferred_transport: Option<crate::api::ProviderTransport>,
    ) -> anyhow::Result<serde_json::Value> {
        let prompt_name = match kind {
            InboxInternalizeKind::PolicyUpdate => PROMPT_INBOX_POLICY_UPDATE_INTERNALIZE,
            InboxInternalizeKind::ClaimAttributeUpdate => {
                PROMPT_INBOX_CLAIM_ATTRIBUTE_UPDATE_INTERNALIZE
            }
        };
        let system_prompt = self
            .prompt_registry
            .render(prompt_name, ())
            .with_context(|| format!("渲染 {prompt_name} prompt 失败"))?;
        let user_text = serde_json::to_string_pretty(&request)?;
        self.json_caller
            .generate_json_streaming_once(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
                crate::api::BufferedProviderRuntime::new(self.fallback_scope.clone()),
                preferred_transport,
            )
            .await
    }
}

/// explicit cancel 已经允许放弃未完成的 tool future，但不能放弃用于恢复判定的 journal
/// 终态。控制事件、Cancelled marker 与 writer ack 共用固定 durability deadline；失败时
/// 当前 run 必须以持久化错误收束，绝不据此宣称外部工具副作用被回滚。
async fn finish_cancelled_turn_journal(
    emitter: TurnJournalEmitter,
    writer: JoinHandle<anyhow::Result<()>>,
    control_forwarder: Option<TurnControlJournalForwarder>,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + crate::session::TURN_JOURNAL_DURABILITY_TIMEOUT;
    if let Some(forwarder) = control_forwarder {
        forwarder.set_drain_on_shutdown(true);
        forwarder.shutdown.cancel();
        let mut handle = forwarder.handle;
        match time::timeout_at(deadline, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                anyhow::bail!("cancelled turn control journal forwarder failed: {error:#}");
            }
            Err(_) => {
                handle.abort();
                anyhow::bail!(
                    "cancelled turn journal durability exceeded {}s while draining control events",
                    crate::session::TURN_JOURNAL_DURABILITY_TIMEOUT.as_secs()
                );
            }
        }
    }

    let finish = emitter.finish(TurnJournalStatus::Cancelled);
    tokio::pin!(finish);
    if time::timeout_at(deadline, &mut finish).await.is_err() {
        anyhow::bail!(
            "cancelled turn journal durability exceeded {}s while enqueueing terminal marker",
            crate::session::TURN_JOURNAL_DURABILITY_TIMEOUT.as_secs()
        );
    }

    let mut writer = writer;
    if Instant::now() >= deadline {
        writer.abort();
        anyhow::bail!(
            "cancelled turn journal durability exceeded {}s before writer ack",
            crate::session::TURN_JOURNAL_DURABILITY_TIMEOUT.as_secs()
        );
    }
    match time::timeout_at(deadline, &mut writer).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(error.context("cancelled turn journal write failed")),
        Ok(Err(error)) => Err(anyhow::anyhow!(
            "cancelled turn journal writer task failed: {error:#}"
        )),
        Err(_) => {
            writer.abort();
            anyhow::bail!(
                "cancelled turn journal durability exceeded {}s before writer ack",
                crate::session::TURN_JOURNAL_DURABILITY_TIMEOUT.as_secs()
            );
        }
    }
}

fn journal_failure_overrides_turn_result(turn_succeeded: bool, turn_interrupted: bool) -> bool {
    turn_succeeded || turn_interrupted
}

impl SessionEngine {
    pub fn new(
        runner: Arc<AgentRunner>,
        turn_loop: Arc<AgentTurnLoop>,
        memory_review_loop: Arc<MemoryReviewLoop>,
        json_caller: Arc<StructuredJsonCaller>,
        prompt_registry: Arc<PromptRegistry>,
        session_store: SessionStore,
        options: SessionEngineOptions,
    ) -> Self {
        if let Err(error) = crate::session::TurnJournalWriter::initialize_executor() {
            log::error!(
                target: "agent",
                "turn journal dedicated writer failed to start: {error}"
            );
        }
        let agent = runner.context();
        Self {
            agent,
            turn_loop,
            memory_review_loop,
            json_caller,
            runner,
            prompt_registry,
            session_store,
            acn_md_path: None,
            compaction: options.compaction,
            skill_injection: options.skills,
            context_window: options.context_window,
            user_shell: options.user_shell,
            workspace_root: options.workspace_root,
            session_source: "tui".into(),
            session_model: "unknown".into(),
            session_search_sqlite_busy_timeout: Duration::from_millis(
                DEFAULT_SESSION_SEARCH_SQLITE_BUSY_TIMEOUT_MS,
            ),
            turn_journal_delta_snapshot_interval: Duration::from_millis(
                options.turn_journal.delta_snapshot_interval_ms,
            ),
            turn_journal_delta_snapshot_chars: options.turn_journal.delta_snapshot_chars,
            turn_recovery_limits: RecoveryContextLimits {
                original_user_request_max_chars: options
                    .turn_journal
                    .recovery_original_user_request_max_chars,
                partial_assistant_max_chars: options
                    .turn_journal
                    .recovery_partial_assistant_max_chars,
                tool_input_max_chars: options.turn_journal.recovery_tool_input_max_chars,
                tool_output_max_chars: options.turn_journal.recovery_tool_output_max_chars,
                user_steer_max_chars: options.turn_journal.recovery_user_steer_max_chars,
            },
            fork_memory_review: false,
            fork_memory_review_interval_turns: DEFAULT_FORK_MEMORY_REVIEW_INTERVAL_TURNS,
            turns_since_fork_memory_review: Arc::new(Mutex::new(0)),
            active_context_usage_anchor: Arc::new(Mutex::new(None)),
            delegation_projection_baselines: Arc::new(Mutex::new(HashMap::new())),
            attachment: AttachmentConfig::default(),
            mcp_manager: None,
            subagent_max_concurrent: options.subagent_max_concurrent,
            runtime_profile: options.runtime_profile,
        }
    }

    pub fn agent_id(&self) -> &AgentId {
        self.runner.agent_id()
    }

    pub fn session_model(&self) -> &str {
        &self.session_model
    }

    pub fn context_window(&self) -> usize {
        self.context_window
    }

    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    pub fn mcp_manager(&self) -> Option<Arc<McpConnectionManager>> {
        self.mcp_manager.clone()
    }

    pub(crate) async fn process_snapshots_for_session(
        &self,
        session_id: &SessionId,
    ) -> Vec<crate::tool::ProcessSnapshot> {
        self.turn_loop
            .tool_registry()
            .process_snapshots_for_root_session(session_id)
            .await
    }

    /// 把后台 watcher 的独立完成事件持久化到 session journal，并交给当前控制面渲染。
    pub(crate) async fn drain_background_process_completions(
        &self,
        session: &SessionHandle,
    ) -> Vec<SessionEvent> {
        let lifecycle_events = self
            .turn_loop
            .tool_registry()
            .take_background_events_for_root_session(&session.metadata.id)
            .await;
        let completions = self
            .turn_loop
            .tool_registry()
            .pending_process_completions_for_root_session(&session.metadata.id)
            .await;
        let mut events = Vec::with_capacity(lifecycle_events.len() + completions.len());
        for lifecycle_event in lifecycle_events {
            match lifecycle_event {
                BackgroundProcessEvent::Started { process_id, owner } => {
                    let owner_subagent = owner.subagent_id.as_deref().unwrap_or("main");
                    self.append_session_event_log(
                        session,
                        "INFO",
                        format!(
                            "Background process started: process_id={} owner_agent={} owner_root_session={} owner_subagent={}",
                            process_id,
                            owner.owner_agent_id,
                            owner.root_session_id,
                            owner_subagent,
                        ),
                    )
                    .await;
                    events.push(SessionEvent::BackgroundProcessStarted {
                        process_id,
                        owner_agent_id: owner.owner_agent_id,
                        owner_root_session_id: owner.root_session_id,
                        owner_subagent_id: owner.subagent_id,
                    });
                }
                BackgroundProcessEvent::Output { process_id, owner } => {
                    events.push(SessionEvent::BackgroundProcessOutput {
                        process_id,
                        owner_agent_id: owner.owner_agent_id,
                        owner_root_session_id: owner.root_session_id,
                        owner_subagent_id: owner.subagent_id,
                    });
                }
                BackgroundProcessEvent::StateChanged {
                    process_id,
                    owner,
                    status,
                } => {
                    let owner_subagent = owner.subagent_id.as_deref().unwrap_or("main");
                    self.append_session_event_log(
                        session,
                        "INFO",
                        format!(
                            "Background process state changed: process_id={} owner_agent={} owner_root_session={} owner_subagent={} status={}",
                            process_id,
                            owner.owner_agent_id,
                            owner.root_session_id,
                            owner_subagent,
                            status,
                        ),
                    )
                    .await;
                    events.push(SessionEvent::BackgroundProcessStateChanged {
                        process_id,
                        owner_agent_id: owner.owner_agent_id,
                        owner_root_session_id: owner.root_session_id,
                        owner_subagent_id: owner.subagent_id,
                        status,
                    });
                }
            }
        }
        if let Err(error) = persist_main_background_process_completions(
            self.turn_loop.tool_registry().as_ref(),
            &session.metadata.id,
            &session.paths.dir,
            &completions,
        )
        .await
        {
            log::warn!(
                target: "agent",
                "background completion journal 写入失败 session={}: {error:#}",
                session.metadata.id,
            );
            // durable obligation 原样保留；下个 heartbeat、provider preflight 或 finalize
            // 会在同一把 completion lock 下重试。
            return events;
        }
        for completion in completions {
            let owner_subagent = completion.owner.subagent_id.as_deref().unwrap_or("main");
            let termination = completion
                .signal
                .map(|signal| format!("signal={signal}"))
                .or_else(|| completion.exit_code.map(|code| format!("exit_code={code}")))
                .unwrap_or_else(|| "exit_code=unknown".into());
            self.append_session_event_log(
                session,
                "INFO",
                format!(
                    "Background process completed: process_id={} turn_id={} tool_use_id={} owner_agent={} owner_root_session={} owner_subagent={} status={} {}",
                    completion.process_id,
                    completion.originating_turn_id.as_deref().unwrap_or("unknown"),
                    completion.originating_tool_use_id.as_deref().unwrap_or("unknown"),
                    completion.owner.owner_agent_id,
                    completion.owner.root_session_id,
                    owner_subagent,
                    completion.status,
                    termination,
                ),
            )
            .await;
            events.push(SessionEvent::BackgroundProcessCompleted {
                process_id: completion.process_id,
                originating_turn_id: completion.originating_turn_id,
                originating_tool_use_id: completion.originating_tool_use_id,
                owner_agent_id: completion.owner.owner_agent_id,
                owner_root_session_id: completion.owner.root_session_id,
                owner_subagent_id: completion.owner.subagent_id,
                status: completion.status,
                exit_code: completion.exit_code,
                signal: completion.signal,
                success: completion.success,
            });
        }
        events
    }

    pub(crate) async fn terminate_process_for_session(
        &self,
        session_id: &SessionId,
        process_id: &str,
        subagent_id: Option<&str>,
        instance_id: u64,
    ) -> Result<(), crate::tool::ToolError> {
        self.turn_loop
            .tool_registry()
            .terminate_process_for_root_session(session_id, process_id, subagent_id, instance_id)
            .await
    }

    pub(crate) async fn cleanup_processes_for_session(&self, session_id: &SessionId) {
        self.turn_loop
            .tool_registry()
            .cleanup_processes_for_session(session_id)
            .await;
    }

    async fn settle_processes_for_session_finalization<F>(
        &self,
        session: &SessionHandle,
        emit: &mut F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SessionEvent),
    {
        // 先持久化 watcher 已经登记但 heartbeat 尚未消费的终态，再终止 live entry。
        for event in self.drain_background_process_completions(session).await {
            emit(event);
        }
        self.turn_loop
            .tool_registry()
            .settle_processes_for_session(&session.metadata.id, Duration::from_secs(5))
            .await;
        for event in self.drain_background_process_completions(session).await {
            emit(event);
        }

        let pending = self
            .turn_loop
            .tool_registry()
            .pending_process_completions_for_root_session(&session.metadata.id)
            .await;
        if !pending.is_empty() {
            anyhow::bail!(
                "{} background process completion(s) are still awaiting durable journal persistence",
                pending.len()
            );
        }
        self.cleanup_processes_for_session(&session.metadata.id)
            .await;
        Ok(())
    }

    /// TUI/CLI runtime 退出时关闭全部受管 terminal，避免子进程泄漏到宿主退出之后。
    pub(crate) async fn shutdown_background_processes(&self) {
        self.turn_loop
            .tool_registry()
            .shutdown_background_processes()
            .await;
    }

    /// 设置 ACN 全局 Markdown 指令路径。缺失文件按空内容处理。
    pub fn with_acn_md_path(mut self, path: PathBuf) -> Self {
        self.acn_md_path = Some(path);
        self
    }

    pub fn with_mcp_manager(mut self, mcp_manager: Arc<McpConnectionManager>) -> Self {
        self.mcp_manager = Some(mcp_manager);
        self
    }

    pub fn available_skills(&self) -> &[crate::skill::SkillSummary] {
        &self.agent.available_skills
    }

    #[cfg(test)]
    fn estimated_message_tokens<'a>(
        messages: impl IntoIterator<Item = &'a SessionMessage>,
    ) -> usize {
        estimated_session_message_tokens_projected(
            messages,
            None,
            Some(crate::api::ProviderReplayIdentity {
                protocol: crate::api::ProviderReplayProtocol::OpenAiResponses,
                model: "test-model".into(),
            }),
        )
    }

    #[cfg(test)]
    fn estimated_projected_message_tokens<'a>(
        messages: impl IntoIterator<Item = &'a SessionMessage>,
        tool_result_raw_max_chars: usize,
    ) -> usize {
        estimated_session_message_tokens_projected(
            messages,
            Some(tool_result_raw_max_chars),
            Some(crate::api::ProviderReplayIdentity {
                protocol: crate::api::ProviderReplayProtocol::OpenAiResponses,
                model: "test-model".into(),
            }),
        )
    }

    fn compaction_summary_end_index(
        &self,
        messages: &[SessionMessage],
        summary_start: usize,
        end: usize,
    ) -> usize {
        self.compaction_summary_end_index_with_tail_limit(
            messages,
            summary_start,
            end,
            self.compaction_tail_token_limit(),
        )
    }

    fn compaction_summary_end_index_with_tail_limit(
        &self,
        messages: &[SessionMessage],
        summary_start: usize,
        end: usize,
        tail_token_limit: usize,
    ) -> usize {
        select_compaction_summary_end_index(
            messages,
            summary_start,
            end,
            tail_token_limit,
            self.compaction.tail_previous_real_user_turns,
            self.compaction.tool_result_raw_max_chars,
            self.turn_loop.history_replay_identity(),
        )
    }

    fn compaction_tail_token_limit(&self) -> usize {
        compaction_tail_token_limit(self.context_window, self.compaction.tail_target_ctx_ratio)
    }

    fn compaction_hard_tail_token_limit(&self) -> usize {
        auto_compact_trigger_threshold_tokens(
            self.context_window,
            self.compaction.tail_hard_ctx_ratio,
        )
    }

    fn provider_projection_budget(
        &self,
        runtime_projection_tokens: usize,
    ) -> ProviderProjectionBudget {
        ProviderProjectionBudget {
            tail_token_limit: self
                .compaction_tail_token_limit()
                .saturating_sub(runtime_projection_tokens),
            tail_hard_token_limit: self
                .compaction_hard_tail_token_limit()
                .saturating_sub(runtime_projection_tokens),
            tail_previous_real_user_turns: self.compaction.tail_previous_real_user_turns,
            tool_result_raw_max_chars: self.compaction.tool_result_raw_max_chars,
        }
    }

    fn preflight_runtime_projection_budget(
        &self,
        runtime_projection_tokens: usize,
    ) -> PreflightRuntimeProjectionBudget {
        PreflightRuntimeProjectionBudget {
            runtime_projection_tokens,
            provider_projection: self.provider_projection_budget(runtime_projection_tokens),
        }
    }

    fn active_context_usage_anchor(
        &self,
        session_id: &SessionId,
        message_count: usize,
    ) -> Option<ActiveContextUsageAnchor> {
        self.active_context_usage_anchor
            .lock()
            .ok()
            .and_then(|anchor| anchor.clone())
            .filter(|anchor| {
                &anchor.session_id == session_id && anchor.message_count == message_count
            })
    }

    fn set_active_context_usage_anchor(
        &self,
        session_id: SessionId,
        message_count: usize,
        used_tokens: usize,
    ) {
        if let Ok(mut anchor) = self.active_context_usage_anchor.lock() {
            *anchor = Some(ActiveContextUsageAnchor {
                session_id,
                message_count,
                used_tokens,
            });
        }
    }

    fn clear_active_context_usage_anchor(&self, session_id: &SessionId) {
        if let Ok(mut anchor) = self.active_context_usage_anchor.lock() {
            if anchor
                .as_ref()
                .is_some_and(|anchor| &anchor.session_id == session_id)
            {
                *anchor = None;
            }
        }
    }

    fn ensure_provider_projection_within_hard_budget(
        &self,
        projection: &ProviderProjection,
        runtime_projection_tokens: usize,
    ) -> anyhow::Result<()> {
        let hard_limit = self.compaction_hard_tail_token_limit();
        let raw_tail_tokens = estimate_session_turn_messages_tokens(&projection.messages);
        let projected_tokens = raw_tail_tokens.saturating_add(runtime_projection_tokens);
        if hard_limit == 0 || projected_tokens > hard_limit {
            return Err(CompactionProjectionTooLarge {
                raw_tail_tokens,
                runtime_projection_tokens,
                projected_tokens,
                hard_limit,
            }
            .into());
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "preflight provider 投影需显式携带 session、active turn、预算与受保护恢复边界"
    )]
    fn preflight_projection(
        &self,
        base_system_prompt: &str,
        state: &SessionCompactionState,
        session_messages: &[SessionMessage],
        active_suffix: &[SessionTurnMessage],
        active_context: ActiveProjectionContext<'_>,
        budget: ProviderProjectionBudget,
        protected_active_tail_segments: usize,
    ) -> ProviderProjection {
        project_provider_context(
            base_system_prompt,
            state,
            session_messages,
            active_suffix.to_vec(),
            active_context,
            budget,
            self.turn_loop.history_media_policy(),
            self.turn_loop.history_replay_identity(),
            protected_active_tail_segments,
            self.turn_loop.tool_registry().file_edit_authority_enabled(),
        )
    }

    async fn externalize_and_validate_preflight_projection(
        &self,
        session: &SessionHandle,
        projection: ProviderProjection,
        runtime_projection_tokens: usize,
        turn_id: &str,
        audit_id: Option<&str>,
    ) -> anyhow::Result<ProviderProjection> {
        let raw_tail_tokens_before = estimate_session_turn_messages_tokens(&projection.messages);
        let externalized = externalize_heavy_user_blocks(
            projection,
            &session.paths.compaction_assets_dir,
            self.turn_loop.history_media_policy(),
        )
        .await;
        let raw_tail_tokens_after =
            estimate_session_turn_messages_tokens(&externalized.projection.messages);
        if let Some(audit_id) = audit_id {
            self.append_compaction_audit_event(
                session,
                CompactionAuditEventKind::ProjectionExternalized {
                    audit_id: audit_id.to_string(),
                    asset_count: externalized.assets.len(),
                    retained_block_count: externalized.retained_block_count,
                    raw_tail_tokens_before,
                    raw_tail_tokens_after,
                },
            )
            .await;
        }
        if !externalized.assets.is_empty() {
            let mut writer = session.open_turn_journal_writer().await?;
            writer
                .append(
                    turn_id,
                    Utc::now(),
                    TurnJournalEventKind::CompactionAssetsExternalized {
                        assets: externalized
                            .assets
                            .iter()
                            .take(COMPACTION_ASSET_REFERENCES_PER_TURN_MAX)
                            .cloned()
                            .collect(),
                    },
                    TurnJournalFlush::Immediate,
                )
                .await?;
            self.append_session_event_log(
                session,
                "INFO",
                format!(
                    "Compaction provider projection externalized {} heavy block(s), retained {} block(s) after asset failures",
                    externalized.assets.len(),
                    externalized.retained_block_count
                ),
            )
            .await;
        }
        self.ensure_provider_projection_within_hard_budget(
            &externalized.projection,
            runtime_projection_tokens,
        )?;
        Ok(externalized.projection)
    }

    fn compaction_ranges(
        &self,
        metadata: &crate::session::SessionMetadata,
        messages: &[SessionMessage],
    ) -> CompactionRanges {
        let summary_start_index = metadata
            .compaction
            .as_ref()
            .map(SessionCompactionState::committed_message_until)
            .unwrap_or(0);
        CompactionRanges {
            summary_start_index,
            summary_end_index: self.compaction_summary_end_index(
                messages,
                summary_start_index,
                metadata.message_count,
            ),
            recap_start_index: metadata.recapped_until,
            recap_end_index: metadata.message_count,
        }
    }

    async fn compaction_ranges_for_checkpoint_or_current(
        &self,
        session: &SessionHandle,
        metadata: &crate::session::SessionMetadata,
        messages: &[SessionMessage],
    ) -> anyhow::Result<CompactionRanges> {
        if let Some(checkpoint) = session.read_compaction_checkpoint().await? {
            if checkpoint.schema_version == Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION) {
                match recoverable_checkpoint_ranges(metadata, &checkpoint) {
                    Ok(Some(ranges)) => return Ok(ranges),
                    Ok(None) => {}
                    Err(error) => {
                        return self
                            .fail_compaction_audit(session, &checkpoint.audit_ids, error)
                            .await;
                    }
                }
            }
        }
        Ok(self.compaction_ranges(metadata, messages))
    }

    pub fn with_session_metadata(
        mut self,
        source: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        self.session_source = source.into();
        self.session_model = model.into();
        self
    }

    pub fn with_session_search_sqlite_busy_timeout(mut self, busy_timeout: Duration) -> Self {
        self.session_search_sqlite_busy_timeout = busy_timeout;
        self
    }

    pub fn with_fork_memory_review(mut self, enabled: bool) -> Self {
        self.fork_memory_review = enabled;
        self.reset_fork_memory_review_turns();
        self
    }

    pub fn with_fork_memory_review_interval_turns(mut self, interval_turns: usize) -> Self {
        self.fork_memory_review_interval_turns = interval_turns;
        self.reset_fork_memory_review_turns();
        self
    }

    pub fn with_attachment_config(mut self, attachment: AttachmentConfig) -> Self {
        self.attachment = attachment;
        self
    }

    pub fn attachment_config(&self) -> &AttachmentConfig {
        &self.attachment
    }

    async fn start_turn_journal(
        &self,
        session: &mut SessionHandle,
        user_text: &str,
        skill_instructions: &[SkillInstructions],
    ) -> anyhow::Result<(
        String,
        Option<String>,
        Vec<CompletedSessionTurnMessage>,
        TurnJournalEmitter,
        JoinHandle<anyhow::Result<()>>,
    )> {
        let journal_read = session.read_turn_journal().await;
        for warning in &journal_read.warnings {
            log::warn!(
                target: "agent",
                "turn journal 读取降级 session={} line={:?}: {}",
                session.metadata.id,
                warning.line,
                warning.message
            );
        }
        let projection = replay_turn_journal(journal_read);
        let turn_id = next_turn_journal_turn_id(&projection);
        let canonical_messages = session.read_messages().await?;
        self.reconcile_pending_provider_history(session, &projection, &canonical_messages)
            .await?;
        let recovery_turns = recovery_turn_chain(&projection, &canonical_messages);
        let recovered_model_context = recovered_model_context(&recovery_turns);
        let recovery_context = turn_journal_recovery_context_for_chain(
            recovery_turns.iter().copied(),
            self.turn_recovery_limits,
        );
        let mut initial_writer = session.open_turn_journal_writer().await?;
        initial_writer
            .append(
                turn_id.clone(),
                Utc::now(),
                TurnJournalEventKind::TurnStarted,
                TurnJournalFlush::Immediate,
            )
            .await?;
        if !skill_instructions.is_empty() {
            initial_writer
                .append(
                    turn_id.clone(),
                    Utc::now(),
                    TurnJournalEventKind::SkillInstructionsResolved {
                        skills: skill_instructions.to_vec(),
                    },
                    TurnJournalFlush::Immediate,
                )
                .await?;
        }
        initial_writer
            .append(
                turn_id.clone(),
                Utc::now(),
                TurnJournalEventKind::UserInputAccepted {
                    text: user_text.to_string(),
                },
                TurnJournalFlush::Immediate,
            )
            .await?;
        let (tx, rx) = mpsc::unbounded_channel();
        let writer_path = session.paths.turn_events_jsonl.clone();
        let writer_turn_id = turn_id.clone();
        let writer =
            tokio::spawn(
                async move { run_turn_journal_writer(writer_path, writer_turn_id, rx).await },
            );
        let emitter = TurnJournalEmitter::new(
            tx,
            self.turn_journal_delta_snapshot_interval,
            self.turn_journal_delta_snapshot_chars,
        );
        Ok((
            turn_id,
            recovery_context,
            recovered_model_context,
            emitter,
            writer,
        ))
    }

    /// 在新 turn 建立前，把上一 turn 的 provider-history WAL 游标与 journal/canonical
    /// 事实对齐。失败或取消的 turn 没有提交其预计 canonical tail；此后追加的 shell
    /// record 等消息必须从原 base 继续投影，不能被未来游标跳过。
    async fn reconcile_pending_provider_history(
        &self,
        session: &mut SessionHandle,
        projection: &crate::session::TurnJournalProjection,
        canonical_messages: &[SessionMessage],
    ) -> anyhow::Result<()> {
        let metadata = session.read_metadata().await?;
        let Some(mut compaction) = metadata.compaction else {
            return Ok(());
        };
        let Some(provider_history) = compaction.provider_history.as_mut() else {
            return Ok(());
        };
        let Some(pending) = provider_history.pending_turn.clone() else {
            return Ok(());
        };
        if pending.base_message_count > canonical_messages.len() {
            anyhow::bail!(
                "pending provider history base cursor 越界: base={}, canonical={}",
                pending.base_message_count,
                canonical_messages.len()
            );
        }

        let pending_turn_committed = projection
            .turns
            .iter()
            .find(|turn| turn.turn_id == pending.turn_id)
            .is_some_and(|turn| {
                turn.status == Some(TurnJournalStatus::Committed)
                    || journal_turn_is_already_canonical(turn, canonical_messages)
            });
        if pending_turn_committed {
            if provider_history.canonical_message_until > canonical_messages.len() {
                anyhow::bail!(
                    "已提交 pending provider history cursor 越界: until={}, canonical={}",
                    provider_history.canonical_message_until,
                    canonical_messages.len()
                );
            }
        } else {
            if let Some(provider_request_message_count) = pending.provider_request_message_count {
                if provider_request_message_count > provider_history.messages.len() {
                    anyhow::bail!(
                        "未提交 pending provider request boundary 越界: request={}, history={}",
                        provider_request_message_count,
                        provider_history.messages.len()
                    );
                }
                provider_history
                    .messages
                    .truncate(provider_request_message_count);
            }
            provider_history.canonical_message_until = pending.base_message_count;
        }
        provider_history.pending_turn = None;
        session.update_compaction(compaction).await?;
        Ok(())
    }

    async fn finish_turn_journal(
        &self,
        session: &SessionHandle,
        emitter: TurnJournalEmitter,
        writer: JoinHandle<anyhow::Result<()>>,
        control_forwarder: Option<TurnControlJournalForwarder>,
        status: TurnJournalStatus,
    ) -> anyhow::Result<()> {
        if status == TurnJournalStatus::Cancelled {
            return finish_cancelled_turn_journal(emitter, writer, control_forwarder).await;
        }
        if let Some(forwarder) = control_forwarder {
            forwarder.set_drain_on_shutdown(status != TurnJournalStatus::Committed);
            forwarder.shutdown.cancel();
            if let Err(e) = forwarder.handle.await {
                let message = format!("Turn control journal forwarder failed: {e:#}");
                log::warn!(target: "agent", "{message}");
                self.append_session_event_log(session, "WARN", message)
                    .await;
            }
        }
        emitter.finish(status).await;
        match writer.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                let message = format!("Turn journal write failed: {e:#}");
                log::warn!(target: "agent", "{message}");
                Err(e.context(message))
            }
            Err(e) => {
                let message = format!("Turn journal writer task failed: {e:#}");
                log::warn!(target: "agent", "{message}");
                Err(anyhow::anyhow!(message))
            }
        }
    }

    async fn reject_late_turn_control_before_commit(
        &self,
        session: &SessionHandle,
        control_forwarder: &mut Option<TurnControlJournalForwarder>,
    ) {
        let Some(forwarder) = control_forwarder.take() else {
            return;
        };
        forwarder.set_drain_on_shutdown(false);
        forwarder.shutdown.cancel();
        if let Err(e) = forwarder.handle.await {
            let message = format!("Turn control commit barrier failed: {e:#}");
            log::warn!(target: "agent", "{message}");
            self.append_session_event_log(session, "WARN", message)
                .await;
        }
    }

    async fn append_session_event_log(
        &self,
        session: &SessionHandle,
        level: &'static str,
        message: impl AsRef<str>,
    ) {
        if let Err(e) = session.append_event_log(level, message).await {
            log::warn!(
                target: "agent",
                "session {} 写入事件日志失败: {e:#}",
                session.metadata.id
            );
        }
    }

    async fn append_session_warnings_log(&self, session: &SessionHandle, warnings: &[String]) {
        for warning in warnings {
            self.append_session_event_log(session, "WARN", warning)
                .await;
        }
    }

    async fn append_compaction_audit_event(
        &self,
        session: &SessionHandle,
        kind: CompactionAuditEventKind,
    ) {
        let event = CompactionAuditEvent {
            created_at: Utc::now(),
            kind,
        };
        if let Err(e) =
            append_compaction_audit_jsonl(&session.paths.compaction_events_jsonl, &event).await
        {
            let message = format!("Compaction audit write failed: {e:#}");
            log::warn!(target: "agent", "{message}");
            self.append_session_event_log(session, "WARN", message)
                .await;
        }
    }

    async fn append_compaction_audit_completed(
        &self,
        session: &SessionHandle,
        audit_ids: &[String],
        outcome: &AppliedCompactionOutcome,
        recapped_until: usize,
        recovered: bool,
    ) {
        for audit_id in audit_ids {
            self.append_compaction_audit_event(
                session,
                CompactionAuditEventKind::Completed {
                    audit_id: audit_id.clone(),
                    recovered,
                    compacted_until: outcome.state.committed_message_until(),
                    recapped_until,
                    committed_summary: non_empty_preview(
                        outcome.state.committed_summary(),
                        COMPACTION_AUDIT_SUMMARY_PREVIEW_CHARS,
                    ),
                    active_turn_summary: outcome.state.active_turn_summary.as_deref().and_then(
                        |summary| {
                            non_empty_preview(summary, COMPACTION_AUDIT_SUMMARY_PREVIEW_CHARS)
                        },
                    ),
                    active_turn: outcome.state.frontier.active_turn.clone(),
                    new_claim_ids: outcome.report.new_claim_ids.clone(),
                    updated_claim_ids: outcome.report.updated_claim_ids.clone(),
                    new_dispute_ids: outcome.report.new_dispute_ids.clone(),
                },
            )
            .await;
        }
    }

    async fn append_compaction_audit_failed(
        &self,
        session: &SessionHandle,
        audit_id: impl Into<String>,
        error: impl Into<String>,
    ) {
        self.append_compaction_audit_event(
            session,
            CompactionAuditEventKind::Failed {
                audit_id: audit_id.into(),
                error: error.into(),
            },
        )
        .await;
    }

    async fn fail_compaction_audit<T>(
        &self,
        session: &SessionHandle,
        audit_ids: &[String],
        error: anyhow::Error,
    ) -> anyhow::Result<T> {
        let error_text = error.to_string();
        for audit_id in audit_ids {
            self.append_compaction_audit_failed(session, audit_id.clone(), error_text.clone())
                .await;
        }
        Err(error)
    }

    pub async fn start_session<F>(
        &self,
        max_attempts: usize,
        emit: F,
    ) -> anyhow::Result<SessionStartReport>
    where
        F: FnMut(SessionEvent) + Send,
    {
        self.start_session_with_id_factory(SessionId::random, max_attempts, emit)
            .await
    }

    pub async fn list_resumable_sessions(
        &self,
    ) -> anyhow::Result<Vec<crate::session::ResumedSessionSummary>> {
        Ok(self
            .session_store
            .list_resumable_sessions(&self.agent.agent_id)
            .await?)
    }

    pub async fn reopen_existing_session(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<SessionHandle> {
        // read state 是单次运行期安全状态；resume 后必须重新建立所需读取许可。
        self.turn_loop.clear_file_read_state(session_id).await;
        let session = self
            .session_store
            .with_session_cleanup_lock(&self.agent.agent_id, || async {
                let mut session = self
                    .session_store
                    .open_existing_session(&self.agent.agent_id, session_id)
                    .await?;
                self.abandon_session_delegations_best_effort(
                    &session,
                    "session restored after runtime exit",
                )
                .await;
                session.mark_open(Utc::now()).await?;
                Ok::<SessionHandle, anyhow::Error>(session)
            })
            .await?;
        self.append_session_event_log(&session, "INFO", "Session resumed")
            .await;
        Ok(session)
    }

    /// resume 时刷新 inbox；单人模式只处理本地 pending，不发起团队网络请求。
    pub async fn process_inbox_for_resume(
        &self,
        session: &SessionHandle,
    ) -> anyhow::Result<InboxProcessReport> {
        if self.runtime_profile == SessionRuntimeProfile::Evaluation {
            anyhow::bail!("evaluation session 不支持 inbox 同步");
        }
        let inbox_generator = SessionInboxJsonGenerator {
            prompt_registry: &self.prompt_registry,
            json_caller: &self.json_caller,
            fallback_scope: session.inbox_fallback_scope_for_request(),
        };
        let report = self.runner.process_inbox_with(&inbox_generator).await?;
        self.append_session_warnings_log(session, &report.warnings)
            .await;
        self.append_session_event_log(
            session,
            "INFO",
            format!("Resume inbox sync completed: processed={}", report.total),
        )
        .await;
        Ok(report)
    }

    pub async fn load_existing_session(
        &self,
        session_id: &SessionId,
    ) -> anyhow::Result<SessionHandle> {
        Ok(self
            .session_store
            .load_existing_session(&self.agent.agent_id, session_id)
            .await?)
    }

    pub async fn delete_empty_session(&self, session_id: &SessionId) -> anyhow::Result<bool> {
        Ok(self
            .session_store
            .delete_empty_session(&self.agent.agent_id, session_id)
            .await?)
    }

    pub async fn start_session_with_id_factory<F, E>(
        &self,
        id_factory: F,
        max_attempts: usize,
        mut emit: E,
    ) -> anyhow::Result<SessionStartReport>
    where
        F: FnMut() -> SessionId,
        E: FnMut(SessionEvent) + Send,
    {
        self.reset_fork_memory_review_turns();
        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Initializing,
        });
        let session_fallback_scope = crate::api::ProviderRuntimeFallbackScope::new_root();
        let inbox_report = match self.runtime_profile {
            SessionRuntimeProfile::Interactive => {
                emit(SessionEvent::StartupProgress {
                    label: "syncing active policies...".into(),
                });
                emit(SessionEvent::StartupProgress {
                    label: "processing inbox...".into(),
                });
                let inbox_generator = SessionInboxJsonGenerator {
                    prompt_registry: &self.prompt_registry,
                    json_caller: &self.json_caller,
                    fallback_scope: session_fallback_scope.clone(),
                };
                let report = self.runner.process_inbox_with(&inbox_generator).await?;
                emit(SessionEvent::TeamServicesConnectionUpdated {
                    status: report.team_services,
                });
                emit_warnings(&report.warnings, &mut emit);
                self.emit_local_claims_updated(&mut emit).await;
                report
            }
            SessionRuntimeProfile::Evaluation => InboxProcessReport::default(),
        };
        emit(SessionEvent::StartupProgress {
            label: "preparing session prompt...".into(),
        });
        let system_prompt = self
            .render_session_system_prompt_for_inbox(&inbox_report)
            .await?;
        emit(SessionEvent::StartupProgress {
            label: "creating session...".into(),
        });
        let mut session = self
            .session_store
            .create_with_metadata_id_factory(
                &self.runner.agent_id,
                &system_prompt,
                self.session_source.clone(),
                self.session_model.clone(),
                id_factory,
                max_attempts,
            )
            .await?;
        session.replace_runtime_fallback_root(session_fallback_scope);
        emit(SessionEvent::SessionStarted {
            session_id: session.metadata.id.clone(),
            agent_id: self.agent.agent_id.clone(),
        });
        self.append_session_event_log(&session, "INFO", "Session started")
            .await;
        self.append_session_warnings_log(&session, &inbox_report.warnings)
            .await;
        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Open,
        });
        Ok(SessionStartReport {
            session,
            inbox_report,
        })
    }

    pub async fn run_turn<F>(
        &self,
        session: &mut SessionHandle,
        user_text: impl Into<String>,
        emit: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SessionEvent) + Send,
    {
        self.run_turn_with_attachments(session, user_text, Vec::new(), emit)
            .await
    }

    pub async fn run_turn_with_attachments<F>(
        &self,
        session: &mut SessionHandle,
        user_text: impl Into<String>,
        user_attachments: Vec<SessionAttachment>,
        emit: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SessionEvent) + Send,
    {
        self.run_turn_with_attachments_controlled(session, user_text, user_attachments, None, emit)
            .await
    }

    pub async fn run_turn_with_attachments_controlled<F>(
        &self,
        session: &mut SessionHandle,
        user_text: impl Into<String>,
        user_attachments: Vec<SessionAttachment>,
        turn_control: Option<SessionTurnControlReceiver>,
        emit: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SessionEvent) + Send,
    {
        self.run_turn_with_attachments_and_skill_source_controlled(
            session,
            user_text,
            user_attachments,
            None,
            turn_control,
            emit,
        )
        .await
    }

    /// `skill_source_text` 是未展开粘贴占位符的可见 composer 文本；省略时退回用户正文。
    /// 它同时作为 `UserMessageAccepted` 的展示文本，模型仍接收完整的 `user_text`。
    pub async fn run_turn_with_attachments_and_skill_source_controlled<F>(
        &self,
        session: &mut SessionHandle,
        user_text: impl Into<String>,
        user_attachments: Vec<SessionAttachment>,
        skill_source_text: Option<String>,
        turn_control: Option<SessionTurnControlReceiver>,
        mut emit: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SessionEvent) + Send,
    {
        let user_text = user_text.into();
        match session.read_metadata().await {
            Ok(metadata) if session_is_not_open(&metadata) => {
                let error = format!(
                    "session {} 当前状态为 {:?}，不能继续执行 turn",
                    metadata.id, metadata.status
                );
                self.append_session_event_log(session, "ERROR", format!("Turn rejected: {error}"))
                    .await;
                emit(SessionEvent::TurnFailed {
                    error: error.clone(),
                });
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                anyhow::bail!(error);
            }
            Ok(_) => {}
            Err(e) => {
                let error = e.to_string();
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Turn metadata read failed: {error}"),
                )
                .await;
                emit(SessionEvent::TurnFailed { error });
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                return Err(e.into());
            }
        }
        let visible_user_text = skill_source_text.unwrap_or_else(|| user_text.clone());
        let skill_instructions = resolve_explicit_skill_instructions(
            &visible_user_text,
            &self.agent.available_skills,
            SkillInjectionLimits {
                max_body_bytes: self.skill_injection.max_body_bytes,
                max_per_turn: self.skill_injection.max_per_turn,
            },
        )
        .await?;
        // recovery snapshot 必须排在独立 watcher 的 durable completion 之后。否则用户在
        // 下一个 heartbeat 前立刻提交新 turn 时，本轮模型会冻结并看到陈旧的
        // `ProcessRunning`，即使 completion 随后才写入上一 turn journal。
        let background_events = self.drain_background_process_completions(session).await;
        for event in background_events {
            emit(event);
        }
        let pending_background_completions = self
            .turn_loop
            .tool_registry()
            .pending_process_completions_for_root_session(&session.metadata.id)
            .await;
        if !pending_background_completions.is_empty() {
            anyhow::bail!(
                "cannot start a new turn while {} background process completion(s) await durable journal persistence",
                pending_background_completions.len()
            );
        }
        let (
            turn_id,
            recovery_context,
            recovered_model_context,
            mut journal_emitter,
            journal_writer,
        ) = self
            .start_turn_journal(session, &user_text, &skill_instructions)
            .await?;
        let checkpoint_result = self
            .turn_loop
            .begin_file_read_state_checkpoint(&session.metadata.id, &turn_id)
            .await
            .context("建立本轮 file read state checkpoint 失败");
        let mut checkpoint_guard = checkpoint_result.as_ref().ok().map(|_| {
            FileReadStateCheckpointOnDrop::new(
                Arc::clone(&self.turn_loop),
                session.metadata.id.clone(),
                turn_id.clone(),
            )
        });
        let tool_boundary_control = turn_control
            .as_ref()
            .map(SessionTurnControlReceiver::tool_boundary_control);
        let tool_boundary_interrupt_status = turn_control
            .as_ref()
            .map(SessionTurnControlReceiver::interrupt_status_cell);
        let mut control_forwarder = turn_control
            .map(|receiver| spawn_turn_control_journal_forwarder(journal_emitter.sink(), receiver));
        if let Some(forwarder) = control_forwarder.as_mut() {
            forwarder.wait_initial_drain().await;
        }
        let model_user_text =
            user_text_with_recovery_context(user_text.clone(), recovery_context.as_deref());
        emit(SessionEvent::TurnStarted {
            turn_id: turn_id.clone(),
        });
        emit(SessionEvent::UserMessageAccepted {
            text: visible_user_text,
        });
        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Running,
        });
        let mut durable_recorder = TurnJournalDurableEventRecorder {
            sink: journal_emitter.sink(),
            assistant_delta_flusher: journal_emitter.assistant_delta_flusher(),
        };
        let runtime_chain_id = session.runtime_chain_id();
        let mut result = async {
            checkpoint_result?;
            let mut turn_emit = |event| match event {
                SessionTurnEvent::Warning { message } => {
                    emit(SessionEvent::Warning { message });
                }
                SessionTurnEvent::ContextUsageUpdated { usage } => {
                    emit(SessionEvent::ContextUsageUpdated {
                        used_tokens: usage.used_tokens,
                    });
                }
                SessionTurnEvent::CompactionStarted {
                    compact_start_index,
                    compact_end_index,
                    recap_start_index,
                    recap_end_index,
                } => {
                    emit(SessionEvent::CompactionStarted {
                        compact_start_index,
                        compact_end_index,
                        recap_start_index,
                        recap_end_index,
                    });
                }
                SessionTurnEvent::CompactionCompleted {
                    compacted_until,
                    recapped_until,
                    new_claim_ids,
                    updated_claim_ids,
                    used_claim_ids,
                    new_dispute_ids,
                } => {
                    emit(SessionEvent::CompactionCompleted {
                        compacted_until,
                        recapped_until,
                        new_claim_ids,
                        updated_claim_ids,
                        used_claim_ids,
                        new_dispute_ids,
                    });
                    emit(SessionEvent::StatusChanged {
                        status: SessionRuntimeStatus::Running,
                    });
                }
                SessionTurnEvent::CompactionSkipped { warning } => {
                    emit(SessionEvent::Warning { message: warning });
                    emit(SessionEvent::StatusChanged {
                        status: SessionRuntimeStatus::Running,
                    });
                }
                SessionTurnEvent::CompactionFailed { error } => {
                    emit(SessionEvent::CompactionFailed { error });
                }
                SessionTurnEvent::AssistantTextDelta { text } => {
                    journal_emitter.assistant_delta(text.clone());
                    emit(SessionEvent::AssistantTextDelta { text });
                }
                SessionTurnEvent::AssistantMessageCompleted { text } => {
                    journal_emitter.flush_assistant_delta();
                    emit(SessionEvent::AssistantMessageCompleted { text });
                }
                SessionTurnEvent::NonStreamingFallbackAttemptStarted {
                    attempt,
                    max_attempts,
                    previous_error: _,
                } => {
                    emit(SessionEvent::NonStreamingFallbackAttemptStarted {
                        attempt,
                        max_attempts,
                    });
                }
                SessionTurnEvent::NonStreamingFallbackAttemptFailed {
                    attempt: _,
                    max_attempts: _,
                    error: _,
                } => {}
                SessionTurnEvent::NonStreamingFallbackSucceeded { text, .. } => {
                    emit(SessionEvent::NonStreamingFallbackSucceeded { text });
                }
                SessionTurnEvent::ToolCallStarted {
                    id,
                    name,
                    summary,
                    input_preview: _,
                    input_truncated: _,
                } => {
                    emit(SessionEvent::ToolCallStarted { id, name, summary });
                }
                SessionTurnEvent::ToolCallSkipped {
                    id,
                    name,
                    summary,
                    input_preview: _,
                    input_truncated: _,
                    reason,
                } => {
                    emit(SessionEvent::ToolCallSkipped {
                        id,
                        name,
                        summary,
                        reason,
                    });
                }
                SessionTurnEvent::ToolCallProgress { id, summary } => {
                    journal_emitter.send_buffered(TurnJournalEventKind::ToolCallProgress {
                        tool_use_id: id.clone(),
                        summary: summary.clone(),
                    });
                    emit(SessionEvent::ToolCallProgress { id, summary });
                }
                SessionTurnEvent::ToolCallCompleted {
                    id,
                    summary,
                    outcome,
                    output_preview: _,
                    output_truncated: _,
                    file_change,
                } => {
                    emit(SessionEvent::ToolCallCompleted {
                        id,
                        summary,
                        file_change,
                        outcome,
                    });
                }
                SessionTurnEvent::ToolCallInterrupted { id, summary } => {
                    emit(SessionEvent::ToolCallInterrupted { id, summary });
                }
            };
            let prepared = self
                .run_turn_inner(
                    session,
                    RunTurnInnerRequest {
                        turn_id: turn_id.clone(),
                        recovered_model_context,
                        user_text: model_user_text,
                        user_attachments,
                        skill_instructions,
                        tool_boundary_control: tool_boundary_control.clone(),
                    },
                    &mut turn_emit,
                    Some(&mut durable_recorder),
                )
                .await;
            match prepared {
                Ok(prepared) => {
                    self.reject_late_turn_control_before_commit(session, &mut control_forwarder)
                        .await;
                    if tool_boundary_control
                        .as_ref()
                        .is_some_and(ToolBoundaryControl::is_cancelled)
                        && !tool_boundary_control
                            .as_ref()
                            .is_some_and(ToolBoundaryControl::should_commit_successful_response)
                    {
                        Err(SessionTurnInterrupted.into())
                    } else {
                        self.record_canonical_user_message(
                            &journal_emitter,
                            &prepared.turn.messages,
                        )
                        .await?;
                        self.commit_prepared_session_turn(session, prepared).await
                    }
                }
                Err(e) => Err(e),
            }
        }
        .await;
        let turn_interrupted = result
            .as_ref()
            .err()
            .is_some_and(|e| e.downcast_ref::<SessionTurnInterrupted>().is_some());
        let canonical_messages_committed_error = result
            .as_ref()
            .err()
            .is_some_and(is_canonical_messages_committed_error);
        if result.is_err() && !canonical_messages_committed_error {
            self.turn_loop.discard_runtime_chain(runtime_chain_id).await;
        }
        let interrupted_status = if turn_interrupted {
            tool_boundary_interrupt_status
                .as_ref()
                .and_then(|status| status.lock().ok().and_then(|status| *status))
                .unwrap_or(TurnJournalStatus::InterruptedByUser)
        } else {
            TurnJournalStatus::Failed
        };
        let journal_status = if result.is_ok() || canonical_messages_committed_error {
            TurnJournalStatus::Committed
        } else if turn_interrupted {
            interrupted_status
        } else {
            TurnJournalStatus::Failed
        };
        drop(durable_recorder);
        let journal_finish_result = self
            .finish_turn_journal(
                session,
                journal_emitter,
                journal_writer,
                control_forwarder,
                journal_status,
            )
            .await;
        if let Err(journal_error) = journal_finish_result {
            if journal_failure_overrides_turn_result(result.is_ok(), turn_interrupted) {
                result = Err(journal_error.context("turn journal 未能可靠持久化；当前运行已停止"));
            } else {
                log::warn!(
                    target: "agent",
                    "turn 失败后的 journal 收束也失败 (session={}): {journal_error:#}",
                    session.metadata.id
                );
            }
        }
        if let Some(checkpoint_guard) = checkpoint_guard.as_mut() {
            let checkpoint_finalize = if journal_status == TurnJournalStatus::Committed {
                self.turn_loop
                    .commit_file_read_state_checkpoint(&session.metadata.id, &turn_id)
                    .await
            } else {
                self.turn_loop
                    .rollback_file_read_state_checkpoint(&session.metadata.id, &turn_id)
                    .await
            };
            if let Err(e) = checkpoint_finalize {
                log::warn!(
                    target: "agent",
                    "收束 file read state checkpoint 失败，保守清空 session 许可 \
                     (session={}, turn={}, status={}): {e:#}",
                    session.metadata.id,
                    turn_id,
                    journal_status.as_str(),
                );
                self.turn_loop
                    .clear_parent_file_read_state(&session.metadata.id)
                    .await;
            }
            checkpoint_guard.disarm();
        }
        if journal_status != TurnJournalStatus::Committed {
            if let Err(e) = self.clear_active_compaction(session).await {
                log::warn!(
                    target: "agent",
                    "turn 未提交后清理 active compaction 失败 (session={}): {e:#}",
                    session.metadata.id
                );
            }
        }
        let result_mapped = match result {
            Ok(committed) => {
                emit(SessionEvent::TurnCommitted {
                    message_count: committed.message_count,
                });
                self.append_session_event_log(
                    session,
                    "INFO",
                    format!("Turn committed: message_count={}", committed.message_count),
                )
                .await;
                if !committed.provider_context_usage_observed {
                    match self.estimate_session_context_tokens(session).await {
                        Ok(used_tokens) => {
                            emit(SessionEvent::ContextUsageUpdated { used_tokens });
                        }
                        Err(e) => {
                            log::warn!(
                                target: "agent",
                                "session {} turn commit 后估算 ctx 失败: {e:#}",
                                session.metadata.id
                            );
                        }
                    }
                }
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Open,
                });
                if self.fork_memory_review_cadence_reached() {
                    self.spawn_memory_review(session).await;
                }
                Ok(())
            }
            Err(e) if e.downcast_ref::<SessionTurnInterrupted>().is_some() => {
                if journal_status == TurnJournalStatus::Cancelled {
                    emit(SessionEvent::TurnCancelled {
                        reason: "user cancelled turn".into(),
                    });
                } else {
                    emit(SessionEvent::TurnInterrupted {
                        reason: "user steer pending".into(),
                    });
                }
                self.append_session_event_log(
                    session,
                    "INFO",
                    format!(
                        "Turn interrupted at tool boundary: {}",
                        journal_status.as_str()
                    ),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Open,
                });
                Ok(())
            }
            Err(e) => {
                let error = e.to_string();
                emit(SessionEvent::TurnFailed {
                    error: error.clone(),
                });
                self.append_session_event_log(session, "ERROR", format!("Turn failed: {error}"))
                    .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                Err(e)
            }
        };

        result_mapped
    }

    pub async fn run_user_shell_command<F>(
        &self,
        session: &mut SessionHandle,
        command: impl Into<String>,
        cancel: CancellationToken,
        mut emit: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(SessionEvent) + Send,
    {
        let command = command.into();
        match session.read_metadata().await {
            Ok(metadata) if session_is_not_open(&metadata) => {
                let error = format!(
                    "session {} 当前状态为 {:?}，不能执行 shell command",
                    metadata.id, metadata.status
                );
                emit(SessionEvent::UserShellCommandFailed {
                    command: command.clone(),
                    error: error.clone(),
                });
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Shell command rejected: {error}"),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                anyhow::bail!(error);
            }
            Ok(_) => {}
            Err(e) => {
                let error = e.to_string();
                emit(SessionEvent::UserShellCommandFailed {
                    command: command.clone(),
                    error: error.clone(),
                });
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Shell command metadata read failed: {error}"),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                return Err(e.into());
            }
        }

        emit(SessionEvent::UserShellCommandStarted {
            command: command.clone(),
        });
        self.append_session_event_log(session, "INFO", format!("Shell command started: {command}"))
            .await;
        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Running,
        });

        let output = match execute_user_shell_command(
            &self.user_shell,
            &self.workspace_root,
            &command,
            cancel,
        )
        .await
        {
            Ok(output) => output,
            Err(e) => {
                let error = e.to_string();
                emit(SessionEvent::UserShellCommandFailed {
                    command: command.clone(),
                    error: error.clone(),
                });
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Shell command failed: {error}"),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Open,
                });
                anyhow::bail!(error);
            }
        };

        let record = format_user_shell_command_record(&command, &output);
        if let Err(e) = session
            .append_messages(&[NewSessionMessage::text_with_model(
                SessionMessageRole::User,
                record,
                self.session_model.clone(),
            )])
            .await
        {
            let error = e.to_string();
            emit(SessionEvent::UserShellCommandFailed {
                command: command.clone(),
                error: error.clone(),
            });
            self.append_session_event_log(
                session,
                "ERROR",
                format!("Shell command persist failed: {error}"),
            )
            .await;
            emit(SessionEvent::StatusChanged {
                status: SessionRuntimeStatus::Error,
            });
            anyhow::bail!(error);
        }
        let message_count = match session.read_metadata().await {
            Ok(metadata) => metadata.message_count,
            Err(e) => {
                let error = e.to_string();
                emit(SessionEvent::UserShellCommandFailed {
                    command: command.clone(),
                    error: error.clone(),
                });
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Shell command metadata read failed: {error}"),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                anyhow::bail!(error);
            }
        };
        let status = output.status;
        let exit_code = output.exit_code;
        let duration_ms = output.duration_ms;
        let truncated = output.truncated;
        self.append_session_event_log(
            session,
            "INFO",
            format!(
                "Shell command completed: status={status:?} exit_code={} duration_ms={duration_ms} message_count={message_count}",
                exit_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "None".into())
            ),
        )
        .await;
        emit(SessionEvent::UserShellCommandCompleted {
            command,
            status,
            exit_code,
            duration_ms,
            stdout: output.stdout,
            stderr: output.stderr,
            truncated,
            message_count,
        });
        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Open,
        });
        Ok(())
    }

    pub async fn process_inbox_during_session<F>(
        &self,
        session: &SessionHandle,
        mut emit: F,
    ) -> anyhow::Result<InboxProcessReport>
    where
        F: FnMut(SessionEvent) + Send,
    {
        if self.runtime_profile == SessionRuntimeProfile::Evaluation {
            let error = "evaluation session 不支持 inbox 同步".to_string();
            emit(SessionEvent::InboxFailed {
                error: error.clone(),
            });
            anyhow::bail!(error);
        }
        match session.read_metadata().await {
            Ok(metadata) if session_is_not_open(&metadata) => {
                let error = format!(
                    "session {} 当前状态为 {:?}，不能处理 inbox",
                    metadata.id, metadata.status
                );
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Inbox sync rejected: {error}"),
                )
                .await;
                emit(SessionEvent::InboxFailed {
                    error: error.clone(),
                });
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                anyhow::bail!(error);
            }
            Ok(_) => {}
            Err(e) => {
                let error = e.to_string();
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Inbox metadata read failed: {error}"),
                )
                .await;
                emit(SessionEvent::InboxFailed { error });
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                return Err(e.into());
            }
        }

        if !self.runner.team_services_configured() {
            let error = TEAM_SERVICES_NOT_CONFIGURED_ERROR.to_string();
            self.append_session_event_log(
                session,
                "ERROR",
                format!("Inbox sync rejected: {error}"),
            )
            .await;
            emit(SessionEvent::InboxFailed {
                error: error.clone(),
            });
            anyhow::bail!(error);
        }

        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::SyncingInbox,
        });
        emit(SessionEvent::InboxStarted);
        self.append_session_event_log(session, "INFO", "Inbox sync started")
            .await;
        let inbox_generator = SessionInboxJsonGenerator {
            prompt_registry: &self.prompt_registry,
            json_caller: &self.json_caller,
            fallback_scope: session.inbox_fallback_scope_for_request(),
        };
        let result = self.runner.process_inbox_with(&inbox_generator).await;
        match result {
            Ok(report) => {
                emit(SessionEvent::TeamServicesConnectionUpdated {
                    status: report.team_services,
                });
                emit_warnings(&report.warnings, &mut emit);
                self.append_session_warnings_log(session, &report.warnings)
                    .await;
                emit(SessionEvent::InboxCompleted {
                    processed: report.total,
                    new_claim_ids: report.new_claim_ids.clone(),
                    updated_claim_ids: report.updated_claim_ids.clone(),
                    new_dispute_ids: report.new_dispute_ids.clone(),
                    deprecated_claim_ids: report.deprecated_claim_ids.clone(),
                });
                self.append_session_event_log(
                    session,
                    "INFO",
                    format!(
                        "Inbox sync completed: processed={} new_claims={} updated_claims={} deprecated_claims={} new_disputes={}",
                        report.total,
                        report.new_claim_ids.len(),
                        report.updated_claim_ids.len(),
                        report.deprecated_claim_ids.len(),
                        report.new_dispute_ids.len()
                    ),
                )
                .await;
                self.emit_local_claims_updated(&mut emit).await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Open,
                });
                Ok(report)
            }
            Err(e) => {
                let error = e.to_string();
                emit(SessionEvent::InboxFailed {
                    error: error.clone(),
                });
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Inbox sync failed: {error}"),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                Err(e)
            }
        }
    }

    async fn run_turn_inner(
        &self,
        session: &mut SessionHandle,
        request: RunTurnInnerRequest,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        durable_recorder: Option<&mut dyn SessionTurnEventRecorder>,
    ) -> anyhow::Result<PreparedSessionTurn> {
        let metadata = session.read_metadata().await?;
        if session_is_not_open(&metadata) {
            anyhow::bail!(
                "session {} 当前状态为 {:?}，不能继续执行 turn",
                metadata.id,
                metadata.status
            );
        }
        let metadata = session.read_metadata().await?;

        let base_system_prompt = tokio::fs::read_to_string(&session.paths.system_prompt)
            .await
            .with_context(|| {
                format!(
                    "读取 session system prompt 失败: {}",
                    session.paths.system_prompt.display()
                )
            })?;
        let all_messages = session.read_messages().await?;
        let previous_message_count = all_messages.len();
        validate_session_compaction_state(&metadata, all_messages.len())?;
        let provider_replay_identity = self.turn_loop.history_replay_identity();
        // 每次 main Provider 请求都必须先推进 write-ahead 窗口。
        // 未压缩 session 也可能在 tool loop/失败/取消前送出尚未进入
        // canonical transcript 的 suffix，因而不能依赖“已有 compaction”作为开关。
        let capture_provider_history = true;
        let frozen_provider_history_prefix_len = replayable_compacted_provider_history(
            &metadata,
            all_messages.len(),
            provider_replay_identity.as_ref(),
        )
        .map(|history| history.messages.len())
        .unwrap_or(0);
        let (system_prompt, history) = compacted_context_for_turn(
            &base_system_prompt,
            &metadata,
            all_messages,
            self.compaction_tail_token_limit(),
            self.compaction_hard_tail_token_limit(),
            self.compaction.tail_previous_real_user_turns,
            self.compaction.tool_result_raw_max_chars,
            self.turn_loop.history_media_policy(),
            provider_replay_identity.clone(),
            self.turn_loop.tool_registry().file_edit_authority_enabled(),
        )?;
        let active_start_index = history.len();
        let runtime_chain_id = session.runtime_chain_id();
        let runtime_fallback_scope = session.runtime_fallback_scope();
        let turn_id_for_tools = request.turn_id.clone();
        let tools = self.turn_loop.tool_registry();
        tools
            .bind_delegation_fallback_root_for_session(&metadata.id, session.inbox_fallback_scope())
            .await
            .context("绑定 subagent fallback scope 失败")?;
        let delegation_activity = tools
            .subscribe_delegation_activity_for_session(&metadata.id)
            .context("订阅 subagent activity 失败")?;
        let background_completion_delivery_seq = Arc::new(AtomicU64::new(0));
        let mut context_appender = MainModelContextAppender {
            tools,
            session_id: metadata.id.clone(),
            session_dir: session.paths.dir.clone(),
            delegation_activity,
            delegation_projection_baselines: Arc::clone(&self.delegation_projection_baselines),
            observed_delegation_baseline: None,
            background_completion_delivery_ids: Vec::new(),
            background_completion_until_seq: metadata
                .provider_background_completion_until_seq
                .unwrap_or(0),
            background_completion_delivery_seq: Arc::clone(&background_completion_delivery_seq),
        };
        let mut preflight = PreflightCompactor {
            engine: self,
            session,
            active_start_index,
            turn_id: request.turn_id,
            base_message_count: previous_message_count,
            active_projection_compacted: false,
            provider_context_anchor: None,
            context_window_recovery_requested: false,
            context_window_recovery_tail_marker: None,
            history_replaced_since_last_check: false,
            frozen_provider_history_prefix_len,
            capture_provider_history,
            last_compacted_provider_history: None,
            provider_compaction_before_pending_request: None,
            background_completion_delivery_seq,
            provider_replay_identity: provider_replay_identity.clone(),
        };
        let turn = self
            .turn_loop
            .run_session_turn_with_context_and_runtime_chain_hooks(
                SessionTurnRequest {
                    current_session_id: Some(metadata.id.clone()),
                    current_turn_id: Some(turn_id_for_tools),
                    system_prompt,
                    history,
                    user_text: request.user_text,
                    user_attachments: request.user_attachments,
                    skill_instructions: request.skill_instructions,
                },
                request.recovered_model_context,
                runtime_chain_id,
                runtime_fallback_scope,
                emit,
                request.tool_boundary_control,
                SessionTurnHooks::new(
                    durable_recorder,
                    Some(&mut context_appender),
                    Some(&mut preflight),
                ),
            )
            .await?;
        Ok(PreparedSessionTurn {
            previous_message_count,
            turn,
            provider_context_used_tokens: preflight
                .provider_context_anchor
                .map(|anchor| anchor.used_tokens),
            compacted_provider_history: preflight.last_compacted_provider_history,
            provider_replay_identity,
        })
    }

    async fn commit_prepared_session_turn(
        &self,
        session: &mut SessionHandle,
        prepared: PreparedSessionTurn,
    ) -> anyhow::Result<CommittedSessionTurn> {
        let metadata = session.read_metadata().await?;
        let compacted_provider_history = prepared.compacted_provider_history.clone();
        let provider_replay_identity = prepared.provider_replay_identity.clone();
        if compacted_provider_history.is_some()
            && prepared
                .turn
                .messages
                .last()
                .is_none_or(|message| message.role != "assistant")
        {
            anyhow::bail!("compacted turn 必须以最终 assistant message 结束");
        }
        let expected_message_count = prepared
            .previous_message_count
            .saturating_add(prepared.turn.messages.len());
        let message_count = match session
            .append_session_turn_messages(&prepared.turn.messages, &self.session_model)
            .await
        {
            Ok(()) => match session.read_metadata().await {
                Ok(metadata) => metadata.message_count,
                Err(e) => {
                    log::warn!(
                        target: "agent",
                        "turn messages 已提交，但读取 metadata 失败，使用期望 message_count={expected_message_count}: {e:#}"
                    );
                    expected_message_count
                }
            },
            Err(SessionStoreError::MessagesCommittedMetadataUpdateFailed {
                message_count,
                model,
                source,
            }) => {
                log::warn!(
                    target: "agent",
                    "turn messages 已提交，但 metadata 更新失败，尝试修复 metadata: {source:#}"
                );
                if let Err(repair_error) = session
                    .repair_committed_message_metadata(message_count, model.clone())
                    .await
                {
                    log::error!(
                        target: "agent",
                        "turn messages 已提交，但 metadata 修复失败: original={source:#}, repair={repair_error:#}"
                    );
                    return Err(SessionStoreError::MessagesCommittedMetadataUpdateFailed {
                        message_count,
                        model,
                        source: Box::new(repair_error),
                    }
                    .into());
                }
                message_count
            }
            Err(e) => return Err(e.into()),
        };
        if let Some(agent_home) = agent_home_from_session_dir(&session.paths.dir) {
            crate::session_search::best_effort_index_session_from_files(
                agent_home,
                metadata.id.clone(),
                self.session_search_sqlite_busy_timeout,
                message_count,
            )
            .await;
        }
        let provider_context_usage_observed = prepared.provider_context_used_tokens.is_some();
        if let Some(used_tokens) = prepared.provider_context_used_tokens {
            self.set_active_context_usage_anchor(metadata.id.clone(), message_count, used_tokens);
        } else {
            self.clear_active_context_usage_anchor(&metadata.id);
        }
        self.finalize_compaction_after_committed_turn(
            session,
            compacted_provider_history,
            provider_replay_identity,
            message_count,
        )
        .await
        .map_err(|source| SessionTurnCommittedPostCommitError { source })?;
        Ok(CommittedSessionTurn {
            message_count,
            provider_context_usage_observed,
        })
    }

    /// 在 canonical transcript 提交前，把同一条 user message 的稳定哈希持久化到 journal。
    async fn record_canonical_user_message(
        &self,
        journal_emitter: &TurnJournalEmitter,
        messages: &[crate::api::CompletedSessionTurnMessage],
    ) -> anyhow::Result<()> {
        let Some(user_message) = messages.iter().find(|message| {
            message.role == "user"
                && message.model_context_snapshot().is_none()
                && !message
                    .content
                    .iter()
                    .any(|block| matches!(block, SessionTurnContentBlock::ToolResult { .. }))
        }) else {
            anyhow::bail!("prepared session turn 缺少真实 user message");
        };
        let content = user_message
            .content
            .clone()
            .into_iter()
            .map(SessionContentBlock::from)
            .collect::<Vec<_>>();
        let content_hash = canonical_user_content_hash(&content)?;
        journal_emitter
            .sink()
            .send_immediate_durable(TurnJournalEventKind::CanonicalUserMessage {
                content_hash: Some(content_hash),
                content: None,
            })
            .await
    }

    async fn clear_active_compaction(&self, session: &mut SessionHandle) -> anyhow::Result<()> {
        let metadata = session.read_metadata().await?;
        let Some(mut compaction) = metadata.compaction else {
            return Ok(());
        };
        if compaction.active_turn_summary.is_none() && compaction.frontier.active_turn.is_none() {
            return Ok(());
        }
        compaction.active_turn_summary = None;
        compaction.frontier.active_turn = None;
        session.update_compaction(compaction).await?;
        Ok(())
    }

    async fn finalize_compaction_after_committed_turn(
        &self,
        session: &mut SessionHandle,
        provider_history: Option<Vec<SessionTurnMessage>>,
        replay_identity: Option<ProviderReplayIdentity>,
        message_count: usize,
    ) -> anyhow::Result<()> {
        let metadata = session.read_metadata().await?;
        let Some(mut compaction) = metadata.compaction else {
            if provider_history.is_some() {
                anyhow::bail!(
                    "turn 捕获了 compacted provider history，但 session 缺少 compaction state"
                );
            }
            return Ok(());
        };
        if let Some(messages) = provider_history {
            compaction.provider_history = Some(Box::new(CompactedProviderHistory {
                replay_identity,
                pending_turn: None,
                canonical_message_until: message_count,
                messages,
            }));
        }
        compaction.active_turn_summary = None;
        compaction.frontier.active_turn = None;
        session.update_compaction(compaction).await?;
        Ok(())
    }

    pub async fn estimate_session_context_tokens(
        &self,
        session: &SessionHandle,
    ) -> anyhow::Result<usize> {
        let metadata = session.read_metadata().await?;
        let system_prompt = tokio::fs::read_to_string(&session.paths.system_prompt)
            .await
            .with_context(|| {
                format!(
                    "读取 session system prompt 失败: {}",
                    session.paths.system_prompt.display()
                )
            })?;
        let all_messages = session.read_messages().await?;
        validate_session_compaction_state(&metadata, all_messages.len())?;
        let (system_prompt, mut history) = compacted_context_for_turn(
            &system_prompt,
            &metadata,
            all_messages,
            self.compaction_tail_token_limit(),
            self.compaction_hard_tail_token_limit(),
            self.compaction.tail_previous_real_user_turns,
            self.compaction.tool_result_raw_max_chars,
            self.turn_loop.history_media_policy(),
            self.turn_loop.history_replay_identity(),
            self.turn_loop.tool_registry().file_edit_authority_enabled(),
        )?;
        let delegation = SessionTurnMessage::model_context(
            ModelContextSource::Delegation,
            delegation_summary_projection(&session.paths.dir)
                .await?
                .unwrap_or(empty_delegation_summary_projection()?),
        );
        if !latest_model_context_matches(&history, &delegation) {
            history.push(delegation);
        }
        Ok(self
            .turn_loop
            .estimate_context_tokens(&system_prompt, &history))
    }

    pub(super) async fn abandon_session_delegations(
        &self,
        session: &SessionHandle,
        reason: &str,
    ) -> anyhow::Result<usize> {
        match self
            .turn_loop
            .abandon_delegations_for_session(&session.metadata.id, reason)
            .await
        {
            Ok(0) => Ok(0),
            Ok(count) => {
                self.append_session_event_log(
                    session,
                    "INFO",
                    format!("Abandoned {count} subagent(s): {reason}"),
                )
                .await;
                Ok(count)
            }
            Err(error) => {
                log::warn!(
                    target: "agent",
                    "session {} abandon delegation failed: {error:#}",
                    session.metadata.id
                );
                self.append_session_event_log(
                    session,
                    "WARN",
                    format!("Abandon subagents failed: {error:#}"),
                )
                .await;
                Err(anyhow::anyhow!(error))
            }
        }
    }

    pub(super) async fn abandon_session_delegations_best_effort(
        &self,
        session: &SessionHandle,
        reason: &str,
    ) -> usize {
        let count = self
            .turn_loop
            .abandon_delegations_for_session_best_effort(&session.metadata.id, reason)
            .await;
        if count > 0 {
            self.append_session_event_log(
                session,
                "INFO",
                format!("Best-effort abandoned {count} subagent(s): {reason}"),
            )
            .await;
        }
        count
    }

    async fn compact_provider_preflight(
        &self,
        session: &mut SessionHandle,
        request: PreflightCompactionRequest<'_>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<Option<ProviderProjection>> {
        let PreflightCompactionRequest {
            base_system_prompt,
            active_suffix,
            turn_id,
            base_message_count,
            active_projection_compacted,
            runtime_projection_tokens,
            protected_active_tail_segments,
        } = request;
        let runtime_budget = self.preflight_runtime_projection_budget(runtime_projection_tokens);
        let projection_budget = runtime_budget.provider_projection;
        let active_context = ActiveProjectionContext {
            turn_id,
            base_message_count,
        };
        let recovered_state = if let Some(outcome) = self
            .recover_matching_compaction_checkpoint(
                session,
                Some(active_context),
                Some(&active_suffix),
            )
            .await?
        {
            log::info!(
                target: "agent",
                "session {} recovered compaction checkpoint before preflight planning",
                session.metadata.id
            );
            let recapped_until = session.read_metadata().await?.recapped_until;
            self.append_compaction_audit_completed(
                session,
                &outcome.audit_ids,
                &outcome,
                recapped_until,
                outcome.recovered,
            )
            .await;
            Some(outcome.state)
        } else {
            None
        };
        let metadata = session.read_metadata().await?;
        let session_messages = session.read_messages().await?;
        validate_session_compaction_state(&metadata, session_messages.len())?;
        let plan = self.build_preflight_compaction_plan(
            &metadata,
            &session_messages,
            &active_suffix,
            active_context,
            active_projection_compacted,
            runtime_budget,
            protected_active_tail_segments,
        )?;
        if plan.committed_transcript.is_none() && plan.active_turn.is_none() {
            if let Some(state) = recovered_state {
                let full_projection = self.preflight_projection(
                    base_system_prompt,
                    &state,
                    &session_messages,
                    &active_suffix,
                    active_context,
                    projection_budget,
                    protected_active_tail_segments,
                );
                let projection = if self
                    .ensure_provider_projection_within_hard_budget(
                        &full_projection,
                        runtime_projection_tokens,
                    )
                    .is_ok()
                {
                    full_projection
                } else {
                    self.externalize_and_validate_preflight_projection(
                        session,
                        full_projection,
                        runtime_projection_tokens,
                        turn_id,
                        None,
                    )
                    .await
                    .map_err(RecoverableCompactionPreparationError::from_projection_failure)?
                };
                self.clear_active_context_usage_anchor(&session.metadata.id);
                return Ok(Some(projection));
            }
            return Ok(None);
        }
        emit(SessionTurnEvent::CompactionStarted {
            compact_start_index: plan.ranges.summary_start_index,
            compact_end_index: plan.ranges.summary_end_index,
            recap_start_index: plan.ranges.recap_start_index,
            recap_end_index: plan.ranges.recap_end_index,
        });
        let mut session_emit = |event| {
            if let Some(event) = preflight_session_event_to_turn_event(event) {
                emit(event);
            }
        };
        let outcome = match self
            .apply_preflight_compaction_plan(
                session,
                metadata,
                PreflightProjectionInputs {
                    base_system_prompt,
                    session_messages: &session_messages,
                    active_suffix: &active_suffix,
                },
                plan,
                &mut session_emit,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(e) => {
                if e.downcast_ref::<RecoverableCompactionPreparationError>()
                    .is_none()
                {
                    emit(SessionTurnEvent::CompactionFailed {
                        error: e.to_string(),
                    });
                }
                return Err(e);
            }
        };
        let recapped_until = match session.read_metadata().await {
            Ok(metadata) => metadata.recapped_until,
            Err(e) => {
                emit(SessionTurnEvent::CompactionFailed {
                    error: e.to_string(),
                });
                return Err(e.into());
            }
        };
        self.append_compaction_audit_completed(
            session,
            &outcome.audit_ids,
            &outcome,
            recapped_until,
            outcome.recovered,
        )
        .await;
        emit(SessionTurnEvent::CompactionCompleted {
            compacted_until: outcome.state.committed_message_until(),
            recapped_until,
            new_claim_ids: outcome.report.new_claim_ids.clone(),
            updated_claim_ids: outcome.report.updated_claim_ids.clone(),
            used_claim_ids: outcome.report.used_claim_ids.clone(),
            new_dispute_ids: outcome.report.new_dispute_ids.clone(),
        });
        self.clear_active_context_usage_anchor(&session.metadata.id);
        let state = outcome.state;
        let projection = outcome.preflight_projection.unwrap_or_else(|| {
            project_provider_context(
                base_system_prompt,
                &state,
                &session_messages,
                active_suffix,
                active_context,
                projection_budget,
                self.turn_loop.history_media_policy(),
                self.turn_loop.history_replay_identity(),
                protected_active_tail_segments,
                self.turn_loop.tool_registry().file_edit_authority_enabled(),
            )
        });
        log::info!(
            target: "agent",
            "session {} preflight compaction advanced committed_until={} active_segment={:?}",
            session.metadata.id,
            state.committed_message_until(),
            state
                .frontier
                .active_turn
                .as_ref()
                .map(|cursor| cursor.compacted_until_segment)
        );
        Ok(Some(projection))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "preflight 规划需显式携带 session、active turn、runtime budget 与受保护尾段边界"
    )]
    fn build_preflight_compaction_plan(
        &self,
        metadata: &crate::session::SessionMetadata,
        session_messages: &[SessionMessage],
        active_suffix: &[SessionTurnMessage],
        active_context: ActiveProjectionContext<'_>,
        active_projection_compacted: bool,
        runtime_budget: PreflightRuntimeProjectionBudget,
        protected_active_tail_segments: usize,
    ) -> anyhow::Result<PreflightCompactionPlan> {
        let projection_budget = runtime_budget.provider_projection;

        let active_turn = self.build_active_turn_plan(
            metadata,
            active_suffix,
            active_context.turn_id,
            active_context.base_message_count,
            projection_budget.tail_token_limit,
            protected_active_tail_segments,
        )?;
        let prior_active_turn = matching_active_turn_compaction(
            metadata,
            active_suffix,
            active_context.turn_id,
            active_context.base_message_count,
            active_projection_compacted,
        )?;
        let active_compacted_until = active_turn
            .as_ref()
            .map(|active| active.cursor.compacted_until_segment)
            .or_else(|| {
                prior_active_turn
                    .as_ref()
                    .filter(|prior| prior.cursor_matches_active_suffix)
                    .map(|prior| prior.cursor.compacted_until_segment)
            })
            .unwrap_or(0);
        let active_projection = project_active_suffix(
            active_suffix.to_vec(),
            active_compacted_until,
            self.compaction.tool_result_raw_max_chars,
            prior_active_turn
                .as_ref()
                .map(|prior| prior.summary.as_str()),
            protected_active_tail_segments,
            self.turn_loop.tool_registry().file_edit_authority_enabled(),
        )
        .messages;
        let summary_start = metadata
            .compaction
            .as_ref()
            .map(SessionCompactionState::committed_message_until)
            .unwrap_or(0);
        let committed_summary_tokens = metadata
            .compaction
            .as_ref()
            .map(SessionCompactionState::committed_summary)
            .filter(|summary| !summary.trim().is_empty())
            .map(|summary| {
                estimate_compacted_committed_summary_message_tokens(
                    summary,
                    self.turn_loop.tool_registry().file_edit_authority_enabled(),
                )
            })
            .unwrap_or(0);
        let committed_tail_limit = raw_preserve_budget_after_mandatory(
            projection_budget.tail_token_limit,
            projection_budget.tail_hard_token_limit,
            committed_summary_tokens
                .saturating_add(estimate_session_turn_messages_tokens(&active_projection)),
        );
        let summary_end = self.compaction_summary_end_index_with_tail_limit(
            session_messages,
            summary_start,
            metadata.message_count,
            committed_tail_limit,
        );
        let committed_transcripts = if summary_end > summary_start {
            let projection = session_compaction_transcript_projection_with_memory_mode(
                session_messages
                    .get(summary_start..summary_end)
                    .with_context(|| {
                        format!(
                            "session compact summary 范围越界: [{summary_start}, {summary_end})"
                        )
                    })?,
                self.compaction.tool_result_raw_max_chars,
                self.turn_loop.tool_registry().memory_enabled(),
            );
            (!projection.full.is_empty()).then_some(projection)
        } else {
            None
        };
        let committed_transcript = committed_transcripts
            .as_ref()
            .map(|projection| projection.full.clone());
        let committed_transcript_with_large_tool_results_omitted = committed_transcripts
            .as_ref()
            .map(|projection| projection.large_tool_results_omitted.clone());
        let committed_transcript_with_tool_results_omitted =
            committed_transcripts.map(|projection| projection.tool_results_omitted);
        Ok(PreflightCompactionPlan {
            ranges: CompactionRanges {
                summary_start_index: summary_start,
                summary_end_index: summary_end,
                recap_start_index: metadata.recapped_until,
                recap_end_index: if committed_transcript.is_some() {
                    metadata.message_count
                } else {
                    metadata.recapped_until
                },
            },
            committed_transcript,
            committed_transcript_with_large_tool_results_omitted,
            committed_transcript_with_tool_results_omitted,
            active_turn,
            prior_active_turn_summary: prior_active_turn
                .as_ref()
                .map(|prior| prior.summary.clone()),
            prior_active_turn_cursor: prior_active_turn.map(|prior| prior.cursor),
            turn_id: active_context.turn_id.to_string(),
            base_message_count: active_context.base_message_count,
            runtime_budget,
            protected_active_tail_segments,
        })
    }

    fn build_active_turn_plan(
        &self,
        metadata: &crate::session::SessionMetadata,
        active_suffix: &[SessionTurnMessage],
        turn_id: &str,
        base_message_count: usize,
        tail_token_limit: usize,
        protected_tail_segments: usize,
    ) -> anyhow::Result<Option<ActiveTurnPlan>> {
        let segments = active_provider_safe_segments(active_suffix);
        if segments.is_empty() {
            return Ok(None);
        }
        let compactable_segments = segments.len().saturating_sub(protected_tail_segments);
        let current_coverage = metadata
            .compaction
            .as_ref()
            .and_then(|state| state.frontier.active_turn.as_ref())
            .filter(|cursor| {
                cursor.turn_id == turn_id
                    && cursor.base_message_count == base_message_count
                    && cursor.compacted_until_segment <= segments.len()
                    && active_segments_hash(
                        active_suffix,
                        &segments[..cursor.compacted_until_segment],
                    )
                    .is_ok_and(|hash| hash == cursor.source_hash)
            })
            .map(|cursor| cursor.compacted_until_segment)
            .unwrap_or(0);
        if current_coverage >= compactable_segments {
            return Ok(None);
        }
        let protected_tail_tokens = if protected_tail_segments > 0 {
            let protected_start = segments[compactable_segments].start;
            estimate_session_turn_messages_tokens(&active_suffix[protected_start..])
        } else {
            0
        };
        let summary_start_segment = current_coverage;
        let summary_end_segment = self.active_summary_end_segment(
            active_suffix,
            &segments[..compactable_segments],
            current_coverage,
            tail_token_limit,
            protected_tail_tokens,
        );
        if summary_end_segment <= summary_start_segment {
            return Ok(None);
        }
        let summary_messages = active_segment_messages(
            active_suffix,
            &segments[summary_start_segment..summary_end_segment],
        );
        let transcript_projection = compaction_transcript_projection_with_memory_mode(
            summary_messages.into_iter().cloned().collect(),
            self.compaction.tool_result_raw_max_chars,
            self.turn_loop.tool_registry().memory_enabled(),
        );
        if transcript_projection.full.is_empty() {
            log::debug!(
                target: "agent",
                "active-turn compaction 跳过空有效投影: turn_id={turn_id} segments=[{summary_start_segment}, {summary_end_segment})"
            );
            return Ok(None);
        }
        let source_hash = active_segments_hash(active_suffix, &segments[..summary_end_segment])?;
        Ok(Some(ActiveTurnPlan {
            summary_start_segment,
            summary_end_segment,
            cursor: ActiveTurnCompactionCursor {
                turn_id: turn_id.to_string(),
                base_message_count,
                compacted_until_segment: summary_end_segment,
                safe_until_event_seq: 0,
                source_hash,
            },
            transcript: transcript_projection.full,
            transcript_with_large_tool_results_omitted: transcript_projection
                .large_tool_results_omitted,
            transcript_with_tool_results_omitted: transcript_projection.tool_results_omitted,
        }))
    }

    fn active_summary_end_segment(
        &self,
        active_suffix: &[SessionTurnMessage],
        segments: &[MessageRange],
        current_coverage: usize,
        tail_token_limit: usize,
        protected_tail_tokens: usize,
    ) -> usize {
        let anchor_end = crate::api::provider_anchor_end_index(active_suffix);
        let anchor_tokens = estimate_session_turn_messages_tokens(&active_suffix[..anchor_end]);
        let mut remaining_raw_tail_budget = tail_token_limit
            .saturating_sub(anchor_tokens)
            .saturating_sub(protected_tail_tokens);
        let mut preserve_start = segments.len();
        for segment_index in (current_coverage..segments.len()).rev() {
            let segment = &segments[segment_index];
            if active_segment_has_large_tool_result(
                active_suffix,
                segment,
                self.compaction.tool_result_raw_max_chars,
            ) {
                break;
            }
            let segment_tokens = estimated_projected_active_segment_tokens(
                active_suffix,
                segment,
                self.compaction.tool_result_raw_max_chars,
            );
            if segment_tokens > remaining_raw_tail_budget {
                break;
            }
            remaining_raw_tail_budget = remaining_raw_tail_budget.saturating_sub(segment_tokens);
            preserve_start = segment_index;
        }
        preserve_start
    }

    fn preflight_state_from_summary(
        &self,
        metadata: &crate::session::SessionMetadata,
        plan: &PreflightCompactionPlan,
        outcome: SessionCompactionOutcome,
        prior_committed_summary: Option<&str>,
        prior_active_summary: Option<&str>,
        summary_max_chars: usize,
    ) -> anyhow::Result<SessionCompactionState> {
        let committed_summary = match (
            plan.committed_transcript.is_some(),
            outcome.committed_summary,
        ) {
            (true, Some(summary)) => {
                validate_compaction_summary_text(summary, "committed_summary", summary_max_chars)?
            }
            (true, None) => anyhow::bail!("compaction summary missing committed_summary"),
            (false, summary) => summary
                .or_else(|| prior_committed_summary.map(ToOwned::to_owned))
                .unwrap_or_default(),
        };
        let active_turn_summary = match (plan.active_turn.as_ref(), outcome.active_turn_summary) {
            (Some(_), Some(summary)) => Some(validate_compaction_summary_text(
                summary,
                "active_turn_summary",
                summary_max_chars,
            )?),
            (Some(_), None) => anyhow::bail!("compaction summary missing active_turn_summary"),
            (None, summary) => summary.or_else(|| prior_active_summary.map(ToOwned::to_owned)),
        };
        let active_turn_cursor = active_turn_summary.as_ref().and_then(|_| {
            plan.active_turn
                .as_ref()
                .map(|active| active.cursor.clone())
                .or_else(|| plan.prior_active_turn_cursor.clone())
        });
        if plan.committed_transcript.is_none() {
            let mut state = metadata.compaction.clone().unwrap_or_else(|| {
                SessionCompactionState::from_committed_summary(0, String::new(), Utc::now())
            });
            // active-only compaction 本身就是允许替换历史的缓存断点。旧的精确 Provider
            // 窗口已经包含当前 active suffix，不能再作为新投影前缀参与拼接。
            state.provider_history = None;
            state.active_turn_summary = active_turn_summary;
            state.frontier.active_turn = active_turn_cursor;
            state.summary_updated_at = Utc::now();
            return Ok(state);
        }
        let mut state = SessionCompactionState::from_committed_summary(
            plan.ranges.summary_end_index,
            committed_summary,
            Utc::now(),
        );
        state.active_turn_summary = active_turn_summary;
        state.frontier.active_turn = active_turn_cursor;
        Ok(state)
    }

    async fn apply_preflight_compaction_plan<F>(
        &self,
        session: &mut SessionHandle,
        metadata: crate::session::SessionMetadata,
        projection_inputs: PreflightProjectionInputs<'_>,
        plan: PreflightCompactionPlan,
        emit: &mut F,
    ) -> anyhow::Result<AppliedCompactionOutcome>
    where
        F: FnMut(SessionEvent),
    {
        let PreflightProjectionInputs {
            base_system_prompt,
            session_messages,
            active_suffix,
        } = projection_inputs;
        let projection_budget = plan.runtime_budget.provider_projection;
        let runtime_projection_tokens = plan.runtime_budget.runtime_projection_tokens;
        let active_context = ActiveProjectionContext {
            turn_id: &plan.turn_id,
            base_message_count: plan.base_message_count,
        };
        let prior_committed_summary = metadata
            .compaction
            .as_ref()
            .map(SessionCompactionState::committed_summary)
            .filter(|summary| !summary.trim().is_empty())
            .map(ToOwned::to_owned);
        let prior_active_summary = plan.prior_active_turn_summary.clone();
        let audit_scope = compaction_audit_scope(
            plan.committed_transcript.is_some(),
            plan.active_turn.is_some(),
        );
        let active_turn_user_anchor = plan
            .active_turn
            .as_ref()
            .and_then(|_| {
                let anchor_end = crate::api::provider_anchor_end_index(active_suffix);
                active_suffix[..anchor_end]
                    .iter()
                    .rev()
                    .find(|message| message.model_context_snapshot().is_none())
            })
            .cloned()
            .map(project_turn_message_for_safe_transcript);
        let summary_inputs = CompactionSummaryInputs {
            audit: CompactionAuditSummaryContext {
                trigger: CompactionAuditTrigger::AutoPreflight,
                scope: audit_scope,
                turn_id: Some(&plan.turn_id),
                base_message_count: Some(plan.base_message_count),
                ranges: plan.ranges,
            },
            committed_start_index: plan
                .committed_transcript
                .as_ref()
                .map(|_| plan.ranges.summary_start_index),
            committed_end_index: plan
                .committed_transcript
                .as_ref()
                .map(|_| plan.ranges.summary_end_index),
            prior_committed_summary: prior_committed_summary.as_deref(),
            committed_transcript: plan.committed_transcript.as_deref(),
            committed_transcript_with_large_tool_results_omitted: plan
                .committed_transcript_with_large_tool_results_omitted
                .as_deref(),
            committed_transcript_with_tool_results_omitted: plan
                .committed_transcript_with_tool_results_omitted
                .as_deref(),
            prior_active_turn_summary: prior_active_summary.as_deref(),
            active_turn_user_anchor: active_turn_user_anchor.as_ref(),
            active_turn_start_segment: plan
                .active_turn
                .as_ref()
                .map(|active| active.summary_start_segment),
            active_turn_end_segment: plan
                .active_turn
                .as_ref()
                .map(|active| active.summary_end_segment),
            active_turn_transcript: plan
                .active_turn
                .as_ref()
                .map(|active| active.transcript.as_slice()),
            active_turn_transcript_with_large_tool_results_omitted: plan
                .active_turn
                .as_ref()
                .map(|active| active.transcript_with_large_tool_results_omitted.as_slice()),
            active_turn_transcript_with_tool_results_omitted: plan
                .active_turn
                .as_ref()
                .map(|active| active.transcript_with_tool_results_omitted.as_slice()),
            summary_max_chars: self.compaction.summary_max_chars,
        };
        let (generated_compaction, prepared_recap) = if plan.committed_transcript.is_some()
            && plan.ranges.recap_start_index < plan.ranges.recap_end_index
        {
            let recap_segment = session_messages
                .get(plan.ranges.recap_start_index..plan.ranges.recap_end_index)
                .with_context(|| {
                    format!(
                        "session compact recap 范围越界: [{}, {})",
                        plan.ranges.recap_start_index, plan.ranges.recap_end_index
                    )
                })?;
            let prepared_summary = self.prepare_compaction_summary_request(&summary_inputs)?;
            let (summary_result, recap_result) = tokio::join!(
                self.generate_prepared_compaction_summary(
                    session,
                    &summary_inputs,
                    prepared_summary,
                    emit,
                ),
                self.prepare_finalize_segment(recap_segment, session.runtime_fallback_scope()),
            );
            let generated_compaction = summary_result?;
            let prepared_recap = match recap_result {
                Ok(prepared) => prepared,
                Err(error) => {
                    let audit_ids = vec![generated_compaction.audit_id.clone()];
                    return self.fail_compaction_audit(session, &audit_ids, error).await;
                }
            };
            (generated_compaction, Some(prepared_recap))
        } else {
            (
                self.generate_compaction_summary(session, &summary_inputs, emit)
                    .await?,
                None,
            )
        };
        let mut audit_ids = vec![generated_compaction.audit_id.clone()];
        macro_rules! audit_try {
            ($expr:expr) => {
                match $expr {
                    Ok(value) => value,
                    Err(error) => {
                        return self
                            .fail_compaction_audit(session, &audit_ids, error.into())
                            .await;
                    }
                }
            };
        }
        let mut candidate_state = audit_try!(self.preflight_state_from_summary(
            &metadata,
            &plan,
            generated_compaction.outcome,
            prior_committed_summary.as_deref(),
            prior_active_summary.as_deref(),
            self.compaction.summary_max_chars,
        ));
        let full_projection = self.preflight_projection(
            base_system_prompt,
            &candidate_state,
            session_messages,
            active_suffix,
            active_context,
            projection_budget,
            plan.protected_active_tail_segments,
        );
        let preflight_projection = if self
            .ensure_provider_projection_within_hard_budget(
                &full_projection,
                runtime_projection_tokens,
            )
            .is_ok()
        {
            full_projection
        } else {
            match self
                .externalize_and_validate_preflight_projection(
                    session,
                    full_projection,
                    runtime_projection_tokens,
                    &plan.turn_id,
                    audit_ids.last().map(String::as_str),
                )
                .await
            {
                Ok(projection) => projection,
                Err(first_projection_error)
                    if first_projection_error
                        .downcast_ref::<CompactionProjectionTooLarge>()
                        .is_some() =>
                {
                    let retry_summary_max_chars = self
                        .compaction
                        .summary_max_chars
                        .checked_div(COMPACTION_RETRY_SUMMARY_DIVISOR)
                        .unwrap_or(0)
                        .max(1);
                    self.append_session_event_log(
                        session,
                        "INFO",
                        format!(
                            "Compaction reference-only projection still exceeded hard tail; retrying summary with max_chars={retry_summary_max_chars}: {first_projection_error:#}"
                        ),
                    )
                    .await;
                    let retry_summary_inputs = CompactionSummaryInputs {
                        summary_max_chars: retry_summary_max_chars,
                        ..summary_inputs
                    };
                    let retry_generated = match self
                        .generate_compaction_summary(session, &retry_summary_inputs, emit)
                        .await
                    {
                        Ok(generated) => generated,
                        Err(error) => {
                            return self.fail_compaction_audit(session, &audit_ids, error).await;
                        }
                    };
                    audit_ids.push(retry_generated.audit_id.clone());
                    candidate_state = audit_try!(self.preflight_state_from_summary(
                        &metadata,
                        &plan,
                        retry_generated.outcome,
                        prior_committed_summary.as_deref(),
                        prior_active_summary.as_deref(),
                        retry_summary_max_chars,
                    ));
                    let retry_projection = self.preflight_projection(
                        base_system_prompt,
                        &candidate_state,
                        session_messages,
                        active_suffix,
                        active_context,
                        projection_budget,
                        plan.protected_active_tail_segments,
                    );
                    let final_projection = self
                        .externalize_and_validate_preflight_projection(
                            session,
                            retry_projection,
                            runtime_projection_tokens,
                            &plan.turn_id,
                            audit_ids.last().map(String::as_str),
                        )
                        .await
                        .map_err(|error| {
                            let details = format!(
                                "Compaction could not fit the mandatory context after externalizing reusable Skill/attachment blocks and retrying with a half-size summary. Split the current plain-text request or start a new session, then retry. Details: {error:#}"
                            );
                            error.context(details)
                        })
                        .map_err(
                            RecoverableCompactionPreparationError::from_projection_failure,
                        );
                    audit_try!(final_projection)
                }
                Err(error) => return self.fail_compaction_audit(session, &audit_ids, error).await,
            }
        };

        if plan.committed_transcript.is_none() {
            audit_try!(session.update_compaction(candidate_state.clone()).await);
            return Ok(AppliedCompactionOutcome {
                state: candidate_state,
                report: SessionFinalizeReport::default(),
                audit_ids,
                recovered: false,
                preflight_projection: Some(preflight_projection),
            });
        }
        let committed_summary = candidate_state.committed_summary().to_string();
        let active_turn_summary = candidate_state.active_turn_summary.clone();
        let active_turn_cursor = candidate_state.frontier.active_turn.clone();

        let summary_segment = audit_try!(session_messages
            .get(plan.ranges.summary_start_index..plan.ranges.summary_end_index)
            .with_context(|| {
                format!(
                    "session compact summary 范围越界: [{}, {})",
                    plan.ranges.summary_start_index, plan.ranges.summary_end_index
                )
            }));
        let recap_segment = audit_try!(session_messages
            .get(plan.ranges.recap_start_index..plan.ranges.recap_end_index)
            .with_context(|| {
                format!(
                    "session compact recap 范围越界: [{}, {})",
                    plan.ranges.recap_start_index, plan.ranges.recap_end_index
                )
            }));
        let summary_segment_hash = audit_try!(hash_session_segment(summary_segment));
        let recap_segment_hash = audit_try!(hash_session_segment(recap_segment));
        let (used_claim_ids, prepared_claims, prepared_disputes) = match prepared_recap {
            Some(prepared) => prepared,
            None => audit_try!(
                self.prepare_finalize_segment(recap_segment, session.runtime_fallback_scope())
                    .await
            ),
        };
        let trace_text = session_trace_text(recap_segment);
        let trace_created_at = Utc::now();
        let trace_id = checkpoint_trace_id(
            &trace_text,
            &used_claim_ids,
            &prepared_claims,
            trace_created_at,
        );
        let checkpoint = CompactionCheckpoint {
            schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
            audit_ids: audit_ids.clone(),
            summary_start_index: plan.ranges.summary_start_index,
            summary_end_index: plan.ranges.summary_end_index,
            summary_segment_hash,
            recap_start_index: plan.ranges.recap_start_index,
            recap_end_index: plan.ranges.recap_end_index,
            recap_segment_hash,
            summary: committed_summary,
            active_turn_summary,
            active_turn: active_turn_cursor,
            prepared_claims,
            prepared_disputes,
            used_claim_ids,
            trace_text,
            trace_created_at,
            trace_id,
            applied_report: None,
            status: CompactionCheckpointStatus::Prepared,
        };
        audit_try!(session.write_compaction_checkpoint(&checkpoint).await);
        let mut outcome = audit_try!(
            self.apply_prepared_compaction_checkpoint(
                session,
                checkpoint,
                session_messages,
                Some(active_context),
                Some(active_suffix),
                false,
            )
            .await
        );
        outcome.preflight_projection = Some(preflight_projection);
        Ok(outcome)
    }

    pub async fn compact_session_checkpoint<F>(
        &self,
        session: &mut SessionHandle,
        mut emit: F,
    ) -> anyhow::Result<SessionCompactionResult>
    where
        F: FnMut(SessionEvent),
    {
        match self
            .compact_session_checkpoint_with_events(session, &mut emit)
            .await
        {
            Ok(ManualCompactionOutcome::Compacted(outcome)) => {
                let outcome = *outcome;
                Ok(SessionCompactionResult::Compacted(outcome.state))
            }
            Ok(ManualCompactionOutcome::Noop(reason)) => Ok(SessionCompactionResult::Noop(reason)),
            Err(e) => {
                let error = match e
                    .downcast_ref::<RecoverableCompactionPreparationError>()
                    .map(|error| error.kind)
                {
                    Some(RecoverableCompactionPreparationKind::SummaryTooLong { .. }) => {
                        "Compaction failed repeatedly. You can run /compact to try again or start a new session."
                            .to_string()
                    }
                    _ => e.to_string(),
                };
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Error,
                });
                emit(SessionEvent::CompactionFailed {
                    error: error.clone(),
                });
                self.append_session_event_log(
                    session,
                    "ERROR",
                    format!("Compaction failed: {error}"),
                )
                .await;
                Err(e)
            }
        }
    }

    async fn compact_session_checkpoint_with_events<F>(
        &self,
        session: &mut SessionHandle,
        emit: &mut F,
    ) -> anyhow::Result<ManualCompactionOutcome>
    where
        F: FnMut(SessionEvent),
    {
        let metadata = session.read_metadata().await?;
        let session_messages = session.read_messages().await?;
        validate_session_compaction_state(&metadata, session_messages.len())?;
        if session_is_not_open(&metadata) {
            let error = format!(
                "session {} 当前状态为 {:?}，不能压缩",
                metadata.id, metadata.status
            );
            anyhow::bail!(error);
        }
        let ranges = self
            .compaction_ranges_for_checkpoint_or_current(session, &metadata, &session_messages)
            .await?;
        let has_recoverable_checkpoint = match session.read_compaction_checkpoint().await? {
            Some(checkpoint)
                if checkpoint.schema_version == Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION) =>
            {
                match recoverable_checkpoint_ranges(&metadata, &checkpoint) {
                    Ok(ranges) => ranges.is_some(),
                    Err(error) => {
                        return self
                            .fail_compaction_audit(session, &checkpoint.audit_ids, error)
                            .await;
                    }
                }
            }
            _ => false,
        };
        if !has_recoverable_checkpoint {
            let summary_transcript_is_empty =
                if ranges.summary_end_index > ranges.summary_start_index {
                    let segment = session_messages
                        .get(ranges.summary_start_index..ranges.summary_end_index)
                        .with_context(|| {
                            format!(
                                "session compact summary 范围越界: [{}, {})",
                                ranges.summary_start_index, ranges.summary_end_index
                            )
                        })?;
                    session_compaction_transcript_projection_with_memory_mode(
                        segment,
                        self.compaction.tool_result_raw_max_chars,
                        self.turn_loop.tool_registry().memory_enabled(),
                    )
                    .full
                    .is_empty()
                } else {
                    true
                };
            if summary_transcript_is_empty {
                log::debug!(
                    target: "agent",
                    "session {} manual compaction 跳过空有效投影: compact=[{}, {})",
                    metadata.id,
                    ranges.summary_start_index,
                    ranges.summary_end_index
                );
                return Ok(ManualCompactionOutcome::Noop(compaction_noop_reason(
                    &metadata, &ranges,
                )));
            }
        }
        emit(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Compacting,
        });
        emit(SessionEvent::CompactionStarted {
            compact_start_index: ranges.summary_start_index,
            compact_end_index: ranges.summary_end_index,
            recap_start_index: ranges.recap_start_index,
            recap_end_index: ranges.recap_end_index,
        });
        self.append_session_event_log(
            session,
            "INFO",
            format!(
                "Compaction started: compact=[{}, {}) recap=[{}, {})",
                ranges.summary_start_index,
                ranges.summary_end_index,
                ranges.recap_start_index,
                ranges.recap_end_index
            ),
        )
        .await;
        let result = self
            .compact_session_checkpoint_inner(session, metadata, session_messages, ranges, emit)
            .await;
        match result {
            Ok(outcome) => {
                let recapped_until = session.read_metadata().await?.recapped_until;
                emit_warnings(&outcome.report.warnings, emit);
                self.append_session_warnings_log(session, &outcome.report.warnings)
                    .await;
                self.append_compaction_audit_completed(
                    session,
                    &outcome.audit_ids,
                    &outcome,
                    recapped_until,
                    outcome.recovered,
                )
                .await;
                // 手动 compact 已用摘要替换模型可见正文，旧读取许可不能跨该边界继续使用。
                self.turn_loop
                    .clear_parent_file_read_state(&session.metadata.id)
                    .await;
                emit(SessionEvent::CompactionCompleted {
                    compacted_until: outcome.state.committed_message_until(),
                    recapped_until,
                    new_claim_ids: outcome.report.new_claim_ids.clone(),
                    updated_claim_ids: outcome.report.updated_claim_ids.clone(),
                    used_claim_ids: outcome.report.used_claim_ids.clone(),
                    new_dispute_ids: outcome.report.new_dispute_ids.clone(),
                });
                self.append_session_event_log(
                    session,
                    "INFO",
                    format!(
                        "Compaction completed: compacted_until={} recapped_until={} new_claims={} updated_claims={} new_disputes={}",
                        outcome.state.committed_message_until(),
                        recapped_until,
                        outcome.report.new_claim_ids.len(),
                        outcome.report.updated_claim_ids.len(),
                        outcome.report.new_dispute_ids.len()
                    ),
                )
                .await;
                emit(SessionEvent::StatusChanged {
                    status: SessionRuntimeStatus::Open,
                });
                self.clear_active_context_usage_anchor(&session.metadata.id);
                match self.estimate_session_context_tokens(session).await {
                    Ok(used_tokens) => {
                        emit(SessionEvent::ContextUsageUpdated { used_tokens });
                    }
                    Err(e) => {
                        log::warn!(
                            target: "agent",
                            "session {} compact 后估算 ctx 失败: {e:#}",
                            session.metadata.id
                        );
                    }
                }
                self.emit_local_claims_updated(emit).await;
                Ok(ManualCompactionOutcome::Compacted(Box::new(outcome)))
            }
            Err(e) => Err(e),
        }
    }

    async fn compact_session_checkpoint_inner<F>(
        &self,
        session: &mut SessionHandle,
        metadata: crate::session::SessionMetadata,
        session_messages: Vec<SessionMessage>,
        ranges: CompactionRanges,
        emit: &mut F,
    ) -> anyhow::Result<AppliedCompactionOutcome>
    where
        F: FnMut(SessionEvent),
    {
        let summary_segment = session_messages
            .get(ranges.summary_start_index..ranges.summary_end_index)
            .with_context(|| {
                format!(
                    "session compact summary 范围越界: [{}, {})",
                    ranges.summary_start_index, ranges.summary_end_index
                )
            })?;
        let recap_segment = session_messages
            .get(ranges.recap_start_index..ranges.recap_end_index)
            .with_context(|| {
                format!(
                    "session compact recap 范围越界: [{}, {})",
                    ranges.recap_start_index, ranges.recap_end_index
                )
            })?;
        let summary_segment_hash = hash_session_segment(summary_segment)?;
        let recap_segment_hash = hash_session_segment(recap_segment)?;
        let mut generated_audit_ids = Vec::<String>::new();
        macro_rules! audit_try {
            ($expr:expr) => {
                match $expr {
                    Ok(value) => value,
                    Err(error) => {
                        let error = error.into();
                        if generated_audit_ids.is_empty() {
                            return Err(error);
                        }
                        return self
                            .fail_compaction_audit(session, &generated_audit_ids, error)
                            .await;
                    }
                }
            };
        }
        let mut recovered_checkpoint = false;
        let checkpoint = match session.read_compaction_checkpoint().await? {
            Some(checkpoint)
                if checkpoint.summary_start_index == ranges.summary_start_index
                    && checkpoint.summary_end_index == ranges.summary_end_index
                    && checkpoint.recap_start_index == ranges.recap_start_index
                    && checkpoint.recap_end_index == ranges.recap_end_index
                    && checkpoint.schema_version == Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION)
                    && checkpoint.status == CompactionCheckpointStatus::Prepared =>
            {
                recovered_checkpoint = true;
                if let Err(error) = validate_compaction_checkpoint_segments(
                    &checkpoint,
                    &summary_segment_hash,
                    &recap_segment_hash,
                ) {
                    return self
                        .fail_compaction_audit(session, &checkpoint.audit_ids, error)
                        .await;
                }
                checkpoint
            }
            Some(checkpoint)
                if checkpoint.summary_start_index == ranges.summary_start_index
                    && checkpoint.summary_end_index == ranges.summary_end_index
                    && checkpoint.recap_start_index == ranges.recap_start_index
                    && checkpoint.recap_end_index == ranges.recap_end_index
                    && checkpoint.schema_version == Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION)
                    && checkpoint.status == CompactionCheckpointStatus::Applied =>
            {
                if let Err(error) = validate_compaction_checkpoint_segments(
                    &checkpoint,
                    &summary_segment_hash,
                    &recap_segment_hash,
                ) {
                    return self
                        .fail_compaction_audit(session, &checkpoint.audit_ids, error)
                        .await;
                }
                let state = match self
                    .commit_applied_compaction_checkpoint(session, &checkpoint, None, None)
                    .await
                {
                    Ok(state) => state,
                    Err(error) => {
                        return self
                            .fail_compaction_audit(session, &checkpoint.audit_ids, error)
                            .await;
                    }
                };
                let report = report_from_compaction_checkpoint(&checkpoint, Vec::new());
                return Ok(AppliedCompactionOutcome {
                    state,
                    report,
                    audit_ids: checkpoint.audit_ids.clone(),
                    recovered: true,
                    preflight_projection: None,
                });
            }
            _ => {
                let prior_summary = metadata.compaction.as_ref().and_then(|c| {
                    (!c.committed_summary().trim().is_empty())
                        .then(|| c.committed_summary().to_string())
                });
                let has_summary_work = ranges.summary_start_index < ranges.summary_end_index;
                let has_recap_work = ranges.recap_start_index < ranges.recap_end_index;
                let (used_claim_ids, prepared_claims, prepared_disputes, summary, audit_ids) =
                    match (has_recap_work, has_summary_work) {
                        (true, true) => {
                            let summary_transcript =
                                session_compaction_transcript_projection_with_memory_mode(
                                    summary_segment,
                                    self.compaction.tool_result_raw_max_chars,
                                    self.turn_loop.tool_registry().memory_enabled(),
                                );
                            let summary_inputs = CompactionSummaryInputs {
                                audit: CompactionAuditSummaryContext {
                                    trigger: CompactionAuditTrigger::ManualCheckpoint,
                                    scope: CompactionAuditScope::Committed,
                                    turn_id: None,
                                    base_message_count: None,
                                    ranges,
                                },
                                committed_start_index: Some(ranges.summary_start_index),
                                committed_end_index: Some(ranges.summary_end_index),
                                prior_committed_summary: prior_summary.as_deref(),
                                committed_transcript: Some(&summary_transcript.full),
                                committed_transcript_with_large_tool_results_omitted: Some(
                                    &summary_transcript.large_tool_results_omitted,
                                ),
                                committed_transcript_with_tool_results_omitted: Some(
                                    &summary_transcript.tool_results_omitted,
                                ),
                                prior_active_turn_summary: None,
                                active_turn_user_anchor: None,
                                active_turn_start_segment: None,
                                active_turn_end_segment: None,
                                active_turn_transcript: None,
                                active_turn_transcript_with_large_tool_results_omitted: None,
                                active_turn_transcript_with_tool_results_omitted: None,
                                summary_max_chars: self.compaction.summary_max_chars,
                            };
                            let prepared_summary =
                                self.prepare_compaction_summary_request(&summary_inputs)?;
                            let (summary_result, recap_result) = tokio::join!(
                                self.generate_prepared_compaction_summary(
                                    session,
                                    &summary_inputs,
                                    prepared_summary,
                                    emit,
                                ),
                                self.prepare_finalize_segment(
                                    recap_segment,
                                    session.runtime_fallback_scope(),
                                ),
                            );
                            let generated_compaction = summary_result?;
                            generated_audit_ids.push(generated_compaction.audit_id.clone());
                            let compaction = generated_compaction.outcome;
                            let (used_claim_ids, prepared_claims, prepared_disputes) =
                                audit_try!(recap_result);
                            let summary = audit_try!(validate_compaction_summary_text(
                                audit_try!(compaction.committed_summary.with_context(|| {
                                    "compaction summary missing committed_summary"
                                })),
                                "committed_summary",
                                self.compaction.summary_max_chars,
                            ));
                            (
                                used_claim_ids,
                                prepared_claims,
                                prepared_disputes,
                                summary,
                                vec![generated_compaction.audit_id],
                            )
                        }
                        (true, false) => {
                            let (used_claim_ids, prepared_claims, prepared_disputes) = self
                                .prepare_finalize_segment(
                                    recap_segment,
                                    session.runtime_fallback_scope(),
                                )
                                .await?;
                            let summary = metadata
                                .compaction
                                .as_ref()
                                .map(|c| c.committed_summary().to_string())
                                .unwrap_or_default();
                            (
                                used_claim_ids,
                                prepared_claims,
                                prepared_disputes,
                                summary,
                                Vec::new(),
                            )
                        }
                        (false, true) => {
                            let summary_transcript =
                                session_compaction_transcript_projection_with_memory_mode(
                                    summary_segment,
                                    self.compaction.tool_result_raw_max_chars,
                                    self.turn_loop.tool_registry().memory_enabled(),
                                );
                            let summary_inputs = CompactionSummaryInputs {
                                audit: CompactionAuditSummaryContext {
                                    trigger: CompactionAuditTrigger::ManualCheckpoint,
                                    scope: CompactionAuditScope::Committed,
                                    turn_id: None,
                                    base_message_count: None,
                                    ranges,
                                },
                                committed_start_index: Some(ranges.summary_start_index),
                                committed_end_index: Some(ranges.summary_end_index),
                                prior_committed_summary: prior_summary.as_deref(),
                                committed_transcript: Some(&summary_transcript.full),
                                committed_transcript_with_large_tool_results_omitted: Some(
                                    &summary_transcript.large_tool_results_omitted,
                                ),
                                committed_transcript_with_tool_results_omitted: Some(
                                    &summary_transcript.tool_results_omitted,
                                ),
                                prior_active_turn_summary: None,
                                active_turn_user_anchor: None,
                                active_turn_start_segment: None,
                                active_turn_end_segment: None,
                                active_turn_transcript: None,
                                active_turn_transcript_with_large_tool_results_omitted: None,
                                active_turn_transcript_with_tool_results_omitted: None,
                                summary_max_chars: self.compaction.summary_max_chars,
                            };
                            let generated_compaction = self
                                .generate_compaction_summary(session, &summary_inputs, emit)
                                .await?;
                            generated_audit_ids.push(generated_compaction.audit_id.clone());
                            let compaction = generated_compaction.outcome;
                            let summary = audit_try!(validate_compaction_summary_text(
                                audit_try!(compaction.committed_summary.with_context(|| {
                                    "compaction summary missing committed_summary"
                                })),
                                "committed_summary",
                                self.compaction.summary_max_chars,
                            ));
                            (
                                Vec::new(),
                                Vec::new(),
                                Vec::new(),
                                summary,
                                vec![generated_compaction.audit_id],
                            )
                        }
                        (false, false) => unreachable!("compact noop should be handled by caller"),
                    };
                let trace_text = session_trace_text(recap_segment);
                let trace_created_at = Utc::now();
                let trace_id = checkpoint_trace_id(
                    &trace_text,
                    &used_claim_ids,
                    &prepared_claims,
                    trace_created_at,
                );
                let checkpoint = CompactionCheckpoint {
                    schema_version: Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION),
                    audit_ids: audit_ids.clone(),
                    summary_start_index: ranges.summary_start_index,
                    summary_end_index: ranges.summary_end_index,
                    summary_segment_hash: summary_segment_hash.clone(),
                    recap_start_index: ranges.recap_start_index,
                    recap_end_index: ranges.recap_end_index,
                    recap_segment_hash: recap_segment_hash.clone(),
                    summary,
                    active_turn_summary: None,
                    active_turn: None,
                    prepared_claims,
                    prepared_disputes,
                    used_claim_ids,
                    trace_text,
                    trace_created_at,
                    trace_id,
                    applied_report: None,
                    status: CompactionCheckpointStatus::Prepared,
                };
                audit_try!(session.write_compaction_checkpoint(&checkpoint).await);
                checkpoint
            }
        };
        let checkpoint_audit_ids = checkpoint.audit_ids.clone();
        let outcome = match self
            .apply_prepared_compaction_checkpoint(
                session,
                checkpoint,
                &session_messages,
                None,
                None,
                recovered_checkpoint,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return self
                    .fail_compaction_audit(session, &checkpoint_audit_ids, error)
                    .await;
            }
        };
        Ok(outcome)
    }

    async fn apply_prepared_compaction_checkpoint(
        &self,
        session: &mut SessionHandle,
        checkpoint: CompactionCheckpoint,
        session_messages: &[SessionMessage],
        active_context: Option<ActiveProjectionContext<'_>>,
        active_suffix: Option<&[SessionTurnMessage]>,
        recovered: bool,
    ) -> anyhow::Result<AppliedCompactionOutcome> {
        let recap_segment = session_messages
            .get(checkpoint.recap_start_index..checkpoint.recap_end_index)
            .with_context(|| {
                format!(
                    "session compact recap checkpoint 范围越界: [{}, {})",
                    checkpoint.recap_start_index, checkpoint.recap_end_index
                )
            })?;
        let report = self
            .apply_prepared_finalize_batch(
                finalize::FinalizeTraceInput::Messages(recap_segment),
                checkpoint.used_claim_ids.clone(),
                checkpoint.prepared_claims.clone(),
                checkpoint.prepared_disputes.clone(),
                checkpoint.trace_created_at,
                checkpoint.trace_id.clone(),
            )
            .await?;
        self.append_session_warnings_log(session, &report.warnings)
            .await;
        let applied_checkpoint = CompactionCheckpoint {
            status: CompactionCheckpointStatus::Applied,
            applied_report: Some(compaction_applied_report_from_finalize_report(&report)),
            ..checkpoint
        };
        session
            .write_compaction_checkpoint(&applied_checkpoint)
            .await?;
        let state = self
            .commit_applied_compaction_checkpoint(
                session,
                &applied_checkpoint,
                active_context,
                active_suffix,
            )
            .await?;
        Ok(AppliedCompactionOutcome {
            state,
            report,
            audit_ids: applied_checkpoint.audit_ids.clone(),
            recovered,
            preflight_projection: None,
        })
    }

    async fn commit_applied_compaction_checkpoint(
        &self,
        session: &mut SessionHandle,
        checkpoint: &CompactionCheckpoint,
        active_context: Option<ActiveProjectionContext<'_>>,
        active_suffix: Option<&[SessionTurnMessage]>,
    ) -> anyhow::Result<SessionCompactionState> {
        if checkpoint.summary_start_index < checkpoint.summary_end_index {
            non_empty_summary(checkpoint.summary.clone(), "committed_summary")?;
        }
        if checkpoint.active_turn.is_some() {
            non_empty_summary(
                checkpoint.active_turn_summary.clone().unwrap_or_default(),
                "active_turn_summary",
            )?;
        }
        let metadata = session.read_metadata().await?;
        let summary_updated_at = if checkpoint.summary_start_index == checkpoint.summary_end_index {
            metadata
                .compaction
                .as_ref()
                .map(|compaction| compaction.summary_updated_at)
                .unwrap_or_else(Utc::now)
        } else {
            Utc::now()
        };
        let mut state = SessionCompactionState::from_committed_summary(
            checkpoint.summary_end_index,
            checkpoint.summary.clone(),
            summary_updated_at,
        );
        let active_turn_is_live = match (
            checkpoint.active_turn.as_ref(),
            active_context,
            active_suffix,
        ) {
            (Some(cursor), Some(context), Some(active_suffix)) => {
                cursor.turn_id == context.turn_id
                    && cursor.base_message_count == context.base_message_count
                    && active_cursor_matches_suffix(cursor, active_suffix)
            }
            _ => false,
        };
        if active_turn_is_live {
            state.active_turn_summary = checkpoint.active_turn_summary.clone();
            state.frontier.active_turn = checkpoint.active_turn.clone();
        }
        session
            .update_compaction_and_recapped_until(state.clone(), checkpoint.recap_end_index)
            .await?;
        Ok(state)
    }

    async fn recover_matching_compaction_checkpoint(
        &self,
        session: &mut SessionHandle,
        active_context: Option<ActiveProjectionContext<'_>>,
        active_suffix: Option<&[SessionTurnMessage]>,
    ) -> anyhow::Result<Option<AppliedCompactionOutcome>> {
        let Some(checkpoint) = session.read_compaction_checkpoint().await? else {
            return Ok(None);
        };
        if checkpoint.schema_version != Some(COMPACTION_CHECKPOINT_SCHEMA_VERSION) {
            return Ok(None);
        }
        let checkpoint_audit_ids = checkpoint.audit_ids.clone();
        macro_rules! checkpoint_audit_try {
            ($expr:expr) => {
                match $expr {
                    Ok(value) => value,
                    Err(error) => {
                        return self
                            .fail_compaction_audit(session, &checkpoint_audit_ids, error.into())
                            .await;
                    }
                }
            };
        }
        let metadata = session.read_metadata().await?;
        let session_messages = session.read_messages().await?;
        checkpoint_audit_try!(validate_session_compaction_state(
            &metadata,
            session_messages.len()
        ));
        let Some(_ranges) =
            checkpoint_audit_try!(recoverable_checkpoint_ranges(&metadata, &checkpoint))
        else {
            return Ok(None);
        };
        let summary_segment = checkpoint_audit_try!(session_messages
            .get(checkpoint.summary_start_index..checkpoint.summary_end_index)
            .with_context(|| {
                format!(
                    "session compact checkpoint summary 范围越界: [{}, {})",
                    checkpoint.summary_start_index, checkpoint.summary_end_index
                )
            }));
        let recap_segment = checkpoint_audit_try!(session_messages
            .get(checkpoint.recap_start_index..checkpoint.recap_end_index)
            .with_context(|| {
                format!(
                    "session compact checkpoint recap 范围越界: [{}, {})",
                    checkpoint.recap_start_index, checkpoint.recap_end_index
                )
            }));
        let summary_hash = checkpoint_audit_try!(hash_session_segment(summary_segment));
        let recap_hash = checkpoint_audit_try!(hash_session_segment(recap_segment));
        checkpoint_audit_try!(validate_compaction_checkpoint_segments(
            &checkpoint,
            &summary_hash,
            &recap_hash
        ));
        let advanced_recapped_until = checkpoint.recap_end_index > metadata.recapped_until;
        let mut outcome = match checkpoint.status {
            CompactionCheckpointStatus::Prepared => checkpoint_audit_try!(
                self.apply_prepared_compaction_checkpoint(
                    session,
                    checkpoint,
                    &session_messages,
                    active_context,
                    active_suffix,
                    true,
                )
                .await
            ),
            CompactionCheckpointStatus::Applied => {
                let report = report_from_compaction_checkpoint(&checkpoint, Vec::new());
                let state = checkpoint_audit_try!(
                    self.commit_applied_compaction_checkpoint(
                        session,
                        &checkpoint,
                        active_context,
                        active_suffix,
                    )
                    .await
                );
                AppliedCompactionOutcome {
                    state,
                    report,
                    audit_ids: checkpoint.audit_ids.clone(),
                    recovered: true,
                    preflight_projection: None,
                }
            }
        };
        outcome.report.advanced_recapped_until |= advanced_recapped_until;
        Ok(Some(outcome))
    }

    pub async fn local_claim_count(&self) -> anyhow::Result<usize> {
        Ok(self.runner.claim_store.list_local_claims().await?.len())
    }

    async fn emit_local_claims_updated<E>(&self, emit: &mut E)
    where
        E: FnMut(SessionEvent),
    {
        match self.local_claim_count().await {
            Ok(total) => emit(SessionEvent::LocalClaimsUpdated { total }),
            Err(e) => log::warn!(
                target: "agent",
                "刷新 local claim 计数失败: {e:#}"
            ),
        }
    }

    async fn generate_compaction_summary<F>(
        &self,
        session: &SessionHandle,
        inputs: &CompactionSummaryInputs<'_>,
        emit: &mut F,
    ) -> anyhow::Result<GeneratedCompactionSummary>
    where
        F: FnMut(SessionEvent),
    {
        let prepared = self.prepare_compaction_summary_request(inputs)?;
        self.generate_prepared_compaction_summary(session, inputs, prepared, emit)
            .await
    }

    /// 构造并验证压缩摘要请求，但不进行 provider 调用。
    ///
    /// 该阶段必须在 recap 前完成，避免 compact 已确定无法执行时仍消耗一次
    /// recap 模型调用。通过后调用方可安全地并发执行摘要与 recap。
    fn prepare_compaction_summary_request(
        &self,
        inputs: &CompactionSummaryInputs<'_>,
    ) -> anyhow::Result<PreparedCompactionSummaryRequest> {
        if inputs
            .committed_transcript
            .is_some_and(<[TurnMessage]>::is_empty)
        {
            anyhow::bail!("compaction committed transcript must not be an empty collection");
        }
        if inputs
            .active_turn_transcript
            .is_some_and(<[TurnMessage]>::is_empty)
        {
            anyhow::bail!("compaction active-turn transcript must not be an empty collection");
        }
        if inputs.committed_transcript.is_none() && inputs.active_turn_transcript.is_none() {
            anyhow::bail!("compaction summary request requires at least one non-empty transcript");
        }
        let system_prompt = self
            .prompt_registry
            .render(
                PROMPT_SESSION_COMPACTION,
                serde_json::json!({
                    "summary_max_chars": inputs.summary_max_chars,
                    "file_edit_authority_enabled": self
                        .turn_loop
                        .tool_registry()
                        .file_edit_authority_enabled(),
                }),
            )
            .context("渲染 session_compaction prompt 失败")?;
        let active_turn_user_anchor = inputs
            .active_turn_user_anchor
            .cloned()
            .map(project_compaction_input_media);
        let mut payload = SessionCompactionPayload {
            instruction: COMPACTION_INSTRUCTION,
            agent_id: self.runner.agent_id.as_str(),
            committed_start_index: inputs.committed_start_index,
            committed_end_index: inputs.committed_end_index,
            prior_committed_summary: inputs.prior_committed_summary,
            committed_transcript: inputs.committed_transcript,
            prior_active_turn_summary: inputs.prior_active_turn_summary,
            active_turn_user_anchor: active_turn_user_anchor.as_ref(),
            active_turn_start_segment: inputs.active_turn_start_segment,
            active_turn_end_segment: inputs.active_turn_end_segment,
            active_turn_transcript: inputs.active_turn_transcript,
            summary_max_chars: inputs.summary_max_chars,
        };
        let mut user_text = serde_json::to_string_pretty(&payload)?;
        let mut provider_messages = vec![SessionTurnMessage::user_text(user_text.clone())];
        if let Err(full_error) = ensure_compaction_request_within_context_window(
            &system_prompt,
            &provider_messages,
            self.context_window,
            self.json_caller.max_tokens(),
        ) {
            payload.committed_transcript =
                inputs.committed_transcript_with_large_tool_results_omitted;
            payload.active_turn_transcript =
                inputs.active_turn_transcript_with_large_tool_results_omitted;
            user_text = serde_json::to_string_pretty(&payload)?;
            provider_messages = vec![SessionTurnMessage::user_text(user_text.clone())];
            if let Err(large_omission_error) = ensure_compaction_request_within_context_window(
                &system_prompt,
                &provider_messages,
                self.context_window,
                self.json_caller.max_tokens(),
            ) {
                payload.committed_transcript =
                    inputs.committed_transcript_with_tool_results_omitted;
                payload.active_turn_transcript =
                    inputs.active_turn_transcript_with_tool_results_omitted;
                user_text = serde_json::to_string_pretty(&payload)?;
                provider_messages = vec![SessionTurnMessage::user_text(user_text.clone())];
                let final_budget = ensure_compaction_request_within_context_window(
                    &system_prompt,
                    &provider_messages,
                    self.context_window,
                    self.json_caller.max_tokens(),
                )
                .with_context(|| {
                    format!(
                        "compaction summary request remains over budget after omitting all tool results; full input error: {full_error:#}; large-tool-result omission error: {large_omission_error:#}"
                    )
                });
                if let Err(source) = final_budget {
                    return Err(RecoverableCompactionPreparationError::other(source).into());
                }
            }
        }
        Ok(PreparedCompactionSummaryRequest {
            system_prompt,
            provider_messages,
            payload_preview: audit_text_preview(&user_text, COMPACTION_AUDIT_PREVIEW_CHARS),
        })
    }

    async fn generate_prepared_compaction_summary<F>(
        &self,
        session: &SessionHandle,
        inputs: &CompactionSummaryInputs<'_>,
        prepared: PreparedCompactionSummaryRequest,
        emit: &mut F,
    ) -> anyhow::Result<GeneratedCompactionSummary>
    where
        F: FnMut(SessionEvent),
    {
        let PreparedCompactionSummaryRequest {
            system_prompt,
            provider_messages,
            payload_preview,
        } = prepared;
        let audit_id = compaction_audit_id(session, &inputs.audit, &payload_preview.hash);
        self.append_compaction_audit_event(
            session,
            CompactionAuditEventKind::Started {
                audit_id: audit_id.clone(),
                trigger: inputs.audit.trigger,
                scope: inputs.audit.scope,
                compact_start_index: inputs.audit.ranges.summary_start_index,
                compact_end_index: inputs.audit.ranges.summary_end_index,
                recap_start_index: inputs.audit.ranges.recap_start_index,
                recap_end_index: inputs.audit.ranges.recap_end_index,
                turn_id: inputs.audit.turn_id.map(ToOwned::to_owned),
                base_message_count: inputs.audit.base_message_count,
                payload: payload_preview,
            },
        )
        .await;
        let mut retry_warnings = Vec::new();
        let result = self
            .json_caller
            .generate_json_validated_with_guarded_attempts(
                StructuredJsonAttemptRequest::compaction_streaming(
                    system_prompt,
                    provider_messages,
                    crate::api::BufferedProviderRuntime::new(session.runtime_fallback_scope()),
                ),
                |value| parse_compaction_summary_outcome(value, inputs),
                |retry_index, retry_total, e| {
                    let message = format!(
                        "compaction summary output invalid, retrying ({retry_index}/{retry_total}): {e:#}"
                    );
                    // reasoning-only / no-consumable 属于可自动恢复的 provider
                    // 业务重试：保留日志与 compaction audit，只在最终失败时
                    // 向 TUI 报错，避免成功恢复后仍留下误导性 Warning。
                    if should_emit_compaction_retry_warning(e) {
                        emit(SessionEvent::Warning {
                            message: message.clone(),
                        });
                    }
                    retry_warnings.push(message);
                },
                |attempt| {
                    let kind = CompactionAuditEventKind::ModelAttempt {
                        audit_id: audit_id.clone(),
                        attempt: attempt.attempt,
                        retry_total: attempt.retry_total,
                        raw_text: attempt
                            .raw_text
                            .as_deref()
                            .map(|text| audit_text_preview(text, COMPACTION_AUDIT_PREVIEW_CHARS)),
                        parsed_json: attempt.parsed_json.as_ref().map(|value| {
                            audit_text_preview(&value.to_string(), COMPACTION_AUDIT_PREVIEW_CHARS)
                        }),
                        error: attempt.error,
                        will_retry: attempt.will_retry,
                    };
                    async move {
                        self.append_compaction_audit_event(session, kind).await;
                    }
                },
                |system_prompt, attempt_messages| {
                    ensure_compaction_request_within_context_window(
                        system_prompt,
                        attempt_messages,
                        self.context_window,
                        self.json_caller.max_tokens(),
                    )
                    .context("compaction summary provider attempt exceeds context window")
                },
            )
            .await;
        for message in retry_warnings {
            self.append_session_event_log(session, "WARN", &message)
                .await;
        }
        match result {
            Ok(outcome) => Ok(GeneratedCompactionSummary { outcome, audit_id }),
            Err(error) => {
                self.append_compaction_audit_failed(session, audit_id, error.to_string())
                    .await;
                Err(RecoverableCompactionPreparationError::from_summary_call(
                    error,
                    self.json_caller.max_attempts(),
                )
                .into())
            }
        }
    }
}

fn should_emit_compaction_retry_warning(error: &anyhow::Error) -> bool {
    crate::api::structured_json_no_consumable_transport(error).is_none()
}

fn compaction_noop_reason(
    metadata: &crate::session::SessionMetadata,
    ranges: &CompactionRanges,
) -> SessionCompactionNoopReason {
    if metadata.message_count > ranges.summary_start_index {
        SessionCompactionNoopReason::RawTailWithinBudget
    } else {
        SessionCompactionNoopReason::NothingNew
    }
}

fn parse_compaction_summary_outcome(
    value: serde_json::Value,
    inputs: &CompactionSummaryInputs<'_>,
) -> anyhow::Result<SessionCompactionOutcome> {
    let object = value
        .as_object()
        .context("compaction summary response must be a JSON object")?;
    if !object.contains_key("committed_summary") {
        anyhow::bail!("committed_summary key must be present");
    }
    if !object.contains_key("active_turn_summary") {
        anyhow::bail!("active_turn_summary key must be present");
    }
    let outcome: SessionCompactionOutcome =
        serde_json::from_value(value).context("compaction summary response shape invalid")?;
    if inputs.committed_transcript.is_some() && outcome.committed_summary.is_none() {
        anyhow::bail!("committed_summary must be a string when committed_transcript is present");
    }
    if inputs.committed_transcript.is_some()
        && outcome
            .committed_summary
            .as_deref()
            .is_some_and(|summary| summary.trim().is_empty())
    {
        anyhow::bail!("committed_summary must not be empty when committed_transcript is present");
    }
    if inputs.committed_transcript.is_none() && outcome.committed_summary.is_some() {
        anyhow::bail!("committed_summary must be null when committed_transcript is null");
    }
    if inputs.active_turn_transcript.is_some() && outcome.active_turn_summary.is_none() {
        anyhow::bail!(
            "active_turn_summary must be a string when active_turn_transcript is present"
        );
    }
    if inputs.active_turn_transcript.is_some()
        && outcome
            .active_turn_summary
            .as_deref()
            .is_some_and(|summary| summary.trim().is_empty())
    {
        anyhow::bail!(
            "active_turn_summary must not be empty when active_turn_transcript is present"
        );
    }
    if inputs.active_turn_transcript.is_none() && outcome.active_turn_summary.is_some() {
        anyhow::bail!("active_turn_summary must be null when active_turn_transcript is null");
    }
    if let Some(summary) = outcome.committed_summary.as_deref() {
        validate_compaction_summary_chars(summary, "committed_summary", inputs.summary_max_chars)?;
    }
    if let Some(summary) = outcome.active_turn_summary.as_deref() {
        validate_compaction_summary_chars(
            summary,
            "active_turn_summary",
            inputs.summary_max_chars,
        )?;
    }
    Ok(outcome)
}

fn agent_home_from_session_dir(session_dir: &std::path::Path) -> Option<std::path::PathBuf> {
    session_dir
        .parent()?
        .parent()
        .map(std::path::Path::to_path_buf)
}

#[derive(Serialize)]
struct DelegationProjectionPayload {
    subagents: Vec<DelegationContextSummary>,
    omitted: usize,
    note: &'static str,
}

#[derive(Serialize)]
struct DelegationContextSummary {
    id: DelegationId,
    title: String,
    role: String,
    status: DelegationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    terminal_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_ref: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    changed_files: Vec<String>,
}

impl From<DelegationSummary> for DelegationContextSummary {
    fn from(summary: DelegationSummary) -> Self {
        let terminal_summary = summary
            .status
            .is_terminal()
            .then_some(summary.progress_summary)
            .flatten();
        Self {
            id: summary.id,
            title: summary.title,
            role: summary.role,
            status: summary.status,
            terminal_summary,
            error_summary: summary.error_summary,
            result_ref: summary.result_ref,
            changed_files: summary.changed_files,
        }
    }
}

async fn delegation_summary_projection(session_dir: &Path) -> anyhow::Result<Option<String>> {
    let page = DelegationStore::new(session_dir.to_path_buf())
        .list_page(DELEGATION_PROJECTION_MAX_ITEMS)
        .await
        .context("读取 subagent summary projection 失败")?;
    let summaries = page
        .summaries
        .into_iter()
        .map(DelegationContextSummary::from)
        .collect::<Vec<_>>();
    if summaries.is_empty() {
        return Ok(None);
    }
    let mut omitted = page.omitted;
    let start_tag = "<subagent_summary_projection>\n";
    let end_tag = "\n</subagent_summary_projection>";
    let json_budget = DELEGATION_PROJECTION_MAX_CHARS
        .saturating_sub(start_tag.chars().count())
        .saturating_sub(end_tag.chars().count())
        .max(256);
    let mut payload = DelegationProjectionPayload {
        subagents: summaries,
        omitted,
        note: "Authoritative bounded collaboration state. Use list_subagents/read_subagent for explicit progress. Full subagent transcript, ordinary progress, and event logs are intentionally omitted.",
    };
    let mut json = serde_json::to_string_pretty(&payload)?;
    while json.chars().count() > json_budget && payload.subagents.len() > 1 {
        payload.subagents.pop();
        omitted = omitted.saturating_add(1);
        payload.omitted = omitted;
        json = serde_json::to_string_pretty(&payload)?;
    }
    if json.chars().count() > json_budget {
        let dropped = payload.subagents.len();
        payload.subagents.clear();
        payload.omitted = payload.omitted.saturating_add(dropped);
        payload.note = "Authoritative collaboration snapshot exceeded the hard budget; subagent details omitted. Use list_subagents/read_subagent for explicit details.";
        json = serde_json::to_string_pretty(&payload)?;
    }
    Ok(Some(format!("{start_tag}{json}{end_tag}")))
}

fn empty_delegation_summary_projection() -> anyhow::Result<String> {
    let payload = DelegationProjectionPayload {
        subagents: Vec::new(),
        omitted: 0,
        note: "Authoritative bounded collaboration state. No subagents are currently registered. Use list_subagents/read_subagent for explicit details.",
    };
    Ok(format!(
        "<subagent_summary_projection>\n{}\n</subagent_summary_projection>",
        serde_json::to_string_pretty(&payload)?
    ))
}

fn next_turn_journal_turn_id(projection: &crate::session::TurnJournalProjection) -> String {
    format!("turn_{}", projection.turns.len().saturating_add(1))
}

fn recovery_turn_chain<'a>(
    projection: &'a crate::session::TurnJournalProjection,
    messages: &[SessionMessage],
) -> Vec<&'a TurnJournalTurn> {
    let last_resolved_index = projection.turns.iter().rposition(|turn| {
        turn.status == Some(TurnJournalStatus::Committed)
            || journal_turn_is_already_canonical(turn, messages)
    });
    projection
        .turns
        .iter()
        .enumerate()
        .filter(|(index, turn)| {
            last_resolved_index.is_none_or(|resolved| *index > resolved)
                && turn.status != Some(TurnJournalStatus::Committed)
        })
        .map(|(_, turn)| turn)
        .collect()
}

fn recovered_model_context(turns: &[&TurnJournalTurn]) -> Vec<CompletedSessionTurnMessage> {
    let mut snapshots = Vec::new();
    for turn in turns {
        let common_prefix = snapshots
            .iter()
            .zip(&turn.model_context)
            .take_while(|(left, right)| same_model_context_snapshot(left, right))
            .count();
        if common_prefix == snapshots.len().min(turn.model_context.len()) {
            snapshots.extend(turn.model_context.iter().skip(common_prefix).cloned());
        } else {
            // 新 turn 正常会先重放此前完整 context 链，再记录本轮增量。若 journal
            // 损坏或来自旧实现而不满足此前缀关系，宁可保留其完整顺序，也不能猜测
            // 某个同 fingerprint 的非相邻状态是重复项。
            snapshots.extend(turn.model_context.iter().cloned());
        }
    }
    snapshots
        .into_iter()
        .map(|snapshot| {
            CompletedSessionTurnMessage::new(
                SessionTurnMessage {
                    role: "user".into(),
                    content: vec![SessionTurnContentBlock::ModelContext {
                        source: snapshot.source,
                        fingerprint: snapshot.fingerprint.clone(),
                        text: snapshot.text.clone(),
                    }],
                    provider_replay: None,
                },
                snapshot.appended_at,
            )
        })
        .collect()
}

fn same_model_context_snapshot(
    left: &crate::session::TurnJournalModelContext,
    right: &crate::session::TurnJournalModelContext,
) -> bool {
    left.source == right.source && left.fingerprint == right.fingerprint && left.text == right.text
}

fn user_text_with_recovery_context(user_text: String, recovery_context: Option<&str>) -> String {
    let Some(recovery_context) = recovery_context else {
        return user_text;
    };
    let payload = tag_safe_json_payload(&serde_json::json!({ "text": user_text }));
    format!("{recovery_context}\n\n<current_user_request>\n{payload}\n</current_user_request>")
}

fn is_canonical_messages_committed_error(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<SessionStoreError>(),
        Some(SessionStoreError::MessagesCommittedMetadataUpdateFailed { .. })
    ) || error
        .downcast_ref::<SessionTurnCommittedPostCommitError>()
        .is_some()
}

fn journal_turn_is_already_canonical(turn: &TurnJournalTurn, messages: &[SessionMessage]) -> bool {
    if matches!(
        turn.status,
        Some(
            TurnJournalStatus::Failed
                | TurnJournalStatus::Cancelled
                | TurnJournalStatus::InterruptedByUser
        )
    ) {
        return false;
    }
    let Some((user_index, user_message)) = last_real_user_message(messages) else {
        return false;
    };
    let Some(started_at) = turn.accepted_at.or(turn.started_at) else {
        return false;
    };
    if user_message.created_at < started_at {
        return false;
    }
    if let Some(journal_hash) = turn.canonical_user_content_hash.as_deref() {
        let Ok(canonical_hash) = canonical_user_content_hash(&user_message.content) else {
            return false;
        };
        if canonical_hash != journal_hash {
            return false;
        }
    } else {
        let Some(original_request) = turn.original_user_request.as_deref() else {
            return false;
        };
        let canonical_user_text =
            first_text_session_content(&user_message.content).unwrap_or_default();
        if canonical_user_request_text(canonical_user_text).as_ref() != original_request {
            return false;
        }
    }
    let canonical_assistant = assistant_text_after(messages, user_index);
    if canonical_assistant.trim().is_empty() {
        return false;
    }
    let assistant_text = turn.assistant_text.trim();
    if assistant_text.is_empty() {
        return !canonical_assistant.trim().is_empty();
    }
    let canonical = canonical_assistant.trim();
    !canonical.is_empty()
        && (canonical.contains(assistant_text) || assistant_text.contains(canonical))
}

fn last_real_user_message(messages: &[SessionMessage]) -> Option<(usize, &SessionMessage)> {
    messages
        .iter()
        .enumerate()
        .rev()
        .find(|(_, message)| {
            message.role == SessionMessageRole::User
                && !message
                    .content
                    .iter()
                    .any(|block| matches!(block, SessionContentBlock::ModelContext { .. }))
                && message.content.iter().any(|block| {
                    matches!(block, SessionContentBlock::Text { text } if !text.starts_with("<user_shell_command>"))
                })
                && !message
                    .content
                    .iter()
                    .any(|block| matches!(block, SessionContentBlock::ToolResult { .. }))
        })
}

fn assistant_text_after(messages: &[SessionMessage], user_index: usize) -> String {
    messages
        .iter()
        .skip(user_index.saturating_add(1))
        .take_while(|message| {
            message.role == SessionMessageRole::Assistant
                || is_independent_model_context_message(message)
                || message
                    .content
                    .iter()
                    .any(|block| matches!(block, SessionContentBlock::ToolResult { .. }))
        })
        .filter(|message| message.role == SessionMessageRole::Assistant)
        .map(|message| flatten_session_content_lossy(&message.content))
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_independent_model_context_message(message: &SessionMessage) -> bool {
    message.role == SessionMessageRole::User
        && !message.content.is_empty()
        && message
            .content
            .iter()
            .all(|block| matches!(block, SessionContentBlock::ModelContext { .. }))
}

fn canonical_user_request_text(text: &str) -> Cow<'_, str> {
    extract_current_user_request(text).unwrap_or(Cow::Borrowed(text))
}

fn first_text_session_content(blocks: &[SessionContentBlock]) -> Option<&str> {
    blocks.iter().find_map(|block| match block {
        SessionContentBlock::Text { text } => Some(text.as_str()),
        SessionContentBlock::SkillInstructions { .. }
        | SessionContentBlock::ModelContext { .. } => None,
        SessionContentBlock::Image { .. }
        | SessionContentBlock::Document { .. }
        | SessionContentBlock::ToolUse { .. }
        | SessionContentBlock::ToolResult { .. } => None,
    })
}

fn extract_current_user_request(text: &str) -> Option<Cow<'_, str>> {
    let start_tag = "<current_user_request>";
    let end_tag = "</current_user_request>";
    let start = text.find(start_tag)?.saturating_add(start_tag.len());
    let tail = &text[start..];
    let end = tail.find(end_tag)?;
    let payload = tail[..end].trim_matches('\n');
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) {
        if let Some(text) = value.get("text").and_then(serde_json::Value::as_str) {
            return Some(Cow::Owned(text.to_string()));
        }
    }
    Some(Cow::Borrowed(payload))
}

fn tag_safe_json_payload(value: &serde_json::Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "{}".into())
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
}

fn session_is_not_open(metadata: &crate::session::SessionMetadata) -> bool {
    metadata.status != SessionStatus::Open || metadata.closed_at.is_some()
}

fn recoverable_checkpoint_ranges(
    metadata: &crate::session::SessionMetadata,
    checkpoint: &CompactionCheckpoint,
) -> anyhow::Result<Option<CompactionRanges>> {
    let summary_start_index = metadata
        .compaction
        .as_ref()
        .map(SessionCompactionState::committed_message_until)
        .unwrap_or(0);
    if checkpoint.summary_start_index != summary_start_index
        || checkpoint.recap_start_index != metadata.recapped_until
    {
        return Ok(None);
    }
    if checkpoint.summary_end_index < checkpoint.summary_start_index {
        anyhow::bail!(
            "session compact checkpoint summary 范围非法: [{}, {})",
            checkpoint.summary_start_index,
            checkpoint.summary_end_index
        );
    }
    if checkpoint.recap_end_index < checkpoint.recap_start_index {
        anyhow::bail!(
            "session compact checkpoint recap 范围非法: [{}, {})",
            checkpoint.recap_start_index,
            checkpoint.recap_end_index
        );
    }
    if checkpoint.summary_end_index > metadata.message_count {
        anyhow::bail!(
            "session compact checkpoint summary_end_index={} 大于 message_count={}",
            checkpoint.summary_end_index,
            metadata.message_count
        );
    }
    if checkpoint.recap_end_index > metadata.message_count {
        anyhow::bail!(
            "session compact checkpoint recap_end_index={} 大于 message_count={}",
            checkpoint.recap_end_index,
            metadata.message_count
        );
    }
    if checkpoint.summary_start_index == checkpoint.summary_end_index
        && checkpoint.recap_start_index == checkpoint.recap_end_index
    {
        return Ok(None);
    }
    Ok(Some(CompactionRanges {
        summary_start_index: checkpoint.summary_start_index,
        summary_end_index: checkpoint.summary_end_index,
        recap_start_index: checkpoint.recap_start_index,
        recap_end_index: checkpoint.recap_end_index,
    }))
}

fn checkpoint_trace_id(
    trace_text: &str,
    used_claim_ids: &[ClaimId],
    prepared_claims: &[Claim],
    trace_created_at: DateTime<Utc>,
) -> Option<TraceId> {
    if used_claim_ids.is_empty() && prepared_claims.is_empty() {
        return None;
    }
    let input_claims = used_claim_ids
        .iter()
        .cloned()
        .map(SourceId::Claim)
        .collect::<Vec<_>>();
    let output_claim_ids = prepared_claims
        .iter()
        .map(|claim| claim.id.clone())
        .collect::<Vec<_>>();
    Some(TraceId::from_trace_parts(
        trace_created_at,
        &trace_name_from_task(trace_text),
        &input_claims,
        &output_claim_ids,
    ))
}

fn validate_compaction_summary_chars(
    summary: &str,
    field: &'static str,
    summary_max_chars: usize,
) -> anyhow::Result<()> {
    let actual_chars = summary.chars().count();
    if actual_chars > summary_max_chars {
        return Err(CompactionSummaryTooLong {
            field,
            actual_chars,
            max_chars: summary_max_chars,
        }
        .into());
    }
    Ok(())
}

fn validate_compaction_summary_text(
    summary: String,
    field: &'static str,
    summary_max_chars: usize,
) -> anyhow::Result<String> {
    non_empty_summary(summary.clone(), field)?;
    validate_compaction_summary_chars(&summary, field, summary_max_chars)?;
    Ok(summary)
}

fn non_empty_summary(summary: String, field: &str) -> anyhow::Result<String> {
    if summary.trim().is_empty() {
        anyhow::bail!("{field} must not be empty");
    }
    Ok(summary)
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(ch);
    }
    formatted
}

fn validate_compaction_checkpoint_segments(
    checkpoint: &CompactionCheckpoint,
    summary_segment_hash: &str,
    recap_segment_hash: &str,
) -> anyhow::Result<()> {
    if checkpoint.summary_segment_hash != summary_segment_hash {
        anyhow::bail!(
            "session compact checkpoint summary_segment_hash 不匹配: checkpoint={} actual={}",
            checkpoint.summary_segment_hash,
            summary_segment_hash
        );
    }
    if checkpoint.recap_segment_hash != recap_segment_hash {
        anyhow::bail!(
            "session compact checkpoint recap_segment_hash 不匹配: checkpoint={} actual={}",
            checkpoint.recap_segment_hash,
            recap_segment_hash
        );
    }
    Ok(())
}

fn validate_finalize_checkpoint_segment(
    checkpoint: &FinalizeCheckpoint,
    recap_segment_hash: &str,
) -> anyhow::Result<()> {
    if checkpoint.recap_segment_hash != recap_segment_hash {
        anyhow::bail!(
            "session finalize checkpoint recap_segment_hash 不匹配: checkpoint={} actual={}",
            checkpoint.recap_segment_hash,
            recap_segment_hash
        );
    }
    Ok(())
}

fn report_from_finalize_checkpoint(
    checkpoint: &FinalizeCheckpoint,
    warnings: Vec<String>,
) -> SessionFinalizeReport {
    let (new_claim_ids, updated_claim_ids) =
        partition_prepared_claim_ids(&checkpoint.prepared_claims);
    SessionFinalizeReport {
        trace_id: checkpoint.trace_id.clone(),
        new_claim_ids,
        updated_claim_ids,
        used_claim_ids: checkpoint.used_claim_ids.clone(),
        new_dispute_ids: checkpoint
            .prepared_disputes
            .iter()
            .map(|dispute| dispute.id.clone())
            .collect(),
        advanced_recapped_until: false,
        finalized_unrecapped_messages: false,
        warnings,
    }
}

fn compaction_applied_report_from_finalize_report(
    report: &SessionFinalizeReport,
) -> CompactionAppliedReport {
    CompactionAppliedReport {
        trace_id: report.trace_id.clone(),
        new_claim_ids: report.new_claim_ids.clone(),
        updated_claim_ids: report.updated_claim_ids.clone(),
        used_claim_ids: report.used_claim_ids.clone(),
        new_dispute_ids: report.new_dispute_ids.clone(),
        warnings: report.warnings.clone(),
    }
}

fn report_from_compaction_checkpoint(
    checkpoint: &CompactionCheckpoint,
    mut warnings: Vec<String>,
) -> SessionFinalizeReport {
    if let Some(applied_report) = checkpoint.applied_report.as_ref() {
        let mut stored_warnings = applied_report.warnings.clone();
        stored_warnings.append(&mut warnings);
        return SessionFinalizeReport {
            trace_id: applied_report.trace_id.clone(),
            new_claim_ids: applied_report.new_claim_ids.clone(),
            updated_claim_ids: applied_report.updated_claim_ids.clone(),
            used_claim_ids: applied_report.used_claim_ids.clone(),
            new_dispute_ids: applied_report.new_dispute_ids.clone(),
            advanced_recapped_until: false,
            finalized_unrecapped_messages: false,
            warnings: stored_warnings,
        };
    }

    let (new_claim_ids, updated_claim_ids) =
        partition_prepared_claim_ids(&checkpoint.prepared_claims);
    SessionFinalizeReport {
        trace_id: checkpoint.trace_id.clone(),
        new_claim_ids,
        updated_claim_ids,
        used_claim_ids: checkpoint.used_claim_ids.clone(),
        new_dispute_ids: checkpoint
            .prepared_disputes
            .iter()
            .map(|dispute| dispute.id.clone())
            .collect(),
        advanced_recapped_until: false,
        finalized_unrecapped_messages: false,
        warnings,
    }
}

fn merge_finalize_reports(
    mut first: SessionFinalizeReport,
    mut second: SessionFinalizeReport,
) -> SessionFinalizeReport {
    first.trace_id = second.trace_id.take().or(first.trace_id);
    first.new_claim_ids.extend(second.new_claim_ids);
    first.updated_claim_ids.extend(second.updated_claim_ids);
    first.used_claim_ids.extend(second.used_claim_ids);
    first.new_dispute_ids.extend(second.new_dispute_ids);
    first.advanced_recapped_until |= second.advanced_recapped_until;
    first.finalized_unrecapped_messages |= second.finalized_unrecapped_messages;
    first.warnings.extend(second.warnings);
    first
}

fn partition_prepared_claim_ids(claims: &[Claim]) -> (Vec<ClaimId>, Vec<ClaimId>) {
    let mut new_claim_ids = Vec::new();
    let mut updated_claim_ids = Vec::new();
    for claim in claims {
        if claim.updated_at.is_some() {
            updated_claim_ids.push(claim.id.clone());
        } else {
            new_claim_ids.push(claim.id.clone());
        }
    }
    (new_claim_ids, updated_claim_ids)
}

async fn append_compaction_audit_jsonl(
    path: &Path,
    event: &CompactionAuditEvent,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("创建 compact audit 目录失败: {}", parent.display()))?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("打开 compact audit JSONL 失败: {}", path.display()))?;
    let mut line = serde_json::to_string(event)?;
    line.push('\n');
    file.write_all(line.as_bytes())
        .await
        .with_context(|| format!("写入 compact audit JSONL 失败: {}", path.display()))?;
    file.flush()
        .await
        .with_context(|| format!("flush compact audit JSONL 失败: {}", path.display()))?;
    Ok(())
}

fn compaction_audit_scope(
    has_committed_summary_work: bool,
    has_active_turn_summary_work: bool,
) -> CompactionAuditScope {
    match (has_committed_summary_work, has_active_turn_summary_work) {
        (true, true) => CompactionAuditScope::Mixed,
        (true, false) => CompactionAuditScope::Committed,
        (false, true) => CompactionAuditScope::ActiveTurn,
        (false, false) => CompactionAuditScope::Committed,
    }
}

fn compaction_audit_id(
    session: &SessionHandle,
    context: &CompactionAuditSummaryContext<'_>,
    payload_hash: &str,
) -> String {
    let mut hash = STABLE_HASH_OFFSET;
    stable_hash_update(&mut hash, session.metadata.id.as_str().as_bytes());
    stable_hash_update(&mut hash, Utc::now().to_rfc3339().as_bytes());
    stable_hash_update(&mut hash, payload_hash.as_bytes());
    stable_hash_update(
        &mut hash,
        format!(
            "{:?}:{:?}:{}:{}:{}:{}",
            context.trigger,
            context.scope,
            context.ranges.summary_start_index,
            context.ranges.summary_end_index,
            context.ranges.recap_start_index,
            context.ranges.recap_end_index
        )
        .as_bytes(),
    );
    format!("compact_{hash:016x}")
}

fn non_empty_preview(text: &str, max_chars: usize) -> Option<CompactionAuditTextPreview> {
    (!text.trim().is_empty()).then(|| audit_text_preview(text, max_chars))
}

fn audit_text_preview(text: &str, max_chars: usize) -> CompactionAuditTextPreview {
    let (preview, truncated) = truncate_audit_preview(text, max_chars);
    CompactionAuditTextPreview {
        chars: text.chars().count(),
        hash: stable_hash_text(text),
        preview,
        truncated,
    }
}

fn truncate_audit_preview(text: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    for (count, ch) in text.chars().enumerate() {
        if count >= max_chars {
            return (out, true);
        }
        out.push(ch);
    }
    (out, false)
}

fn stable_hash_text(text: &str) -> String {
    let mut hash = STABLE_HASH_OFFSET;
    stable_hash_update(&mut hash, text.as_bytes());
    format!("{hash:016x}")
}

fn hash_session_segment(messages: &[SessionMessage]) -> anyhow::Result<String> {
    let mut hash = STABLE_HASH_OFFSET;
    for message in messages {
        stable_hash_json(&mut hash, message)?;
    }
    Ok(format!("{hash:016x}"))
}

fn stable_hash_json<T: Serialize>(hash: &mut u64, value: &T) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(value)?;
    stable_hash_update(hash, &bytes);
    stable_hash_update(hash, b"\n");
    Ok(())
}

fn stable_hash_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(STABLE_HASH_PRIME);
    }
}

#[cfg(test)]
mod tests;
