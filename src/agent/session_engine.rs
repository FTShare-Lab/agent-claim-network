//! 交互式 session 的运行引擎。
//!
//! SessionEngine 是多轮 session 生命周期的入口：负责启动准备、单轮 turn、
//! session 级 finalize 与运行时事件投影。它复用 AgentRunner 已有的存储、LLM、
//! maintainer 与 inbox 能力，但交互式 session 的 LLM 调用只走 provider-neutral 组件。

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::Error as _, Serialize};

use super::context::AgentContext;
use super::inbox::InboxJsonGenerator;
use super::runner::{AgentRunner, InboxProcessReport};
use super::runner_trace::trace_name_from_task;
use super::user_shell::{
    format_user_shell_command_record, run_user_shell_command as execute_user_shell_command,
};
use crate::api::{
    estimate_session_turn_messages_tokens, AgentTurnLoop, ContextUsageSnapshot, ContextUsageSource,
    InboxInternalizeKind, InternalizeRequest, MemoryReviewLoop, SessionAttachment,
    SessionCompactionOutcome, SessionTurn, SessionTurnEvent, SessionTurnEventRecorder,
    SessionTurnInterrupted, SessionTurnMessage, SessionTurnPreflight, SessionTurnRequest,
    StructuredJsonCaller, ToolBoundaryControl, TurnMessage,
};
use crate::claim::{AgentId, Claim, ClaimId, DisputeId, SessionId, SourceId, TraceId};
use crate::config::{
    AgentSessionSkillConfig, AgentSessionTurnJournalConfig, AttachmentConfig,
    SessionCompactionConfig, UserShellConfig, COMPACTION_ASSET_REFERENCES_PER_TURN_MAX,
    COMPACTION_RETRY_SUMMARY_DIVISOR, DEFAULT_FORK_MEMORY_REVIEW_INTERVAL_TURNS,
    DEFAULT_SESSION_SEARCH_SQLITE_BUSY_TIMEOUT_MS,
};
use crate::delegation::{DelegationStore, DelegationSummary};
use crate::mcp::connection_manager::McpConnectionManager;
use crate::prompt::PromptRegistry;
use crate::session::{
    canonical_user_content_hash, replay_turn_journal, turn_journal_recovery_context_for_chain,
    ActiveTurnCompactionCursor, CompactionAppliedReport, CompactionCheckpoint,
    CompactionCheckpointStatus, FinalizeCheckpoint, NewSessionMessage, RecoveryContextLimits,
    SessionCompactionState, SessionContentBlock, SessionHandle, SessionMessage, SessionMessageRole,
    SessionStatus, SessionStore, SessionStoreError, TurnJournalEventKind, TurnJournalFlush,
    TurnJournalStatus, TurnJournalTurn,
};
use crate::skill::{resolve_explicit_skill_instructions, SkillInjectionLimits, SkillInstructions};
use crate::tool::BackgroundProcessEvent;
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
const EXPLICIT_CANCEL_JOURNAL_SETTLE_GRACE: Duration = Duration::from_millis(100);
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
    attachment: AttachmentConfig,
    mcp_manager: Option<Arc<McpConnectionManager>>,
    subagent_max_concurrent: usize,
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
}

struct CommittedSessionTurn {
    message_count: usize,
    provider_context_usage_observed: bool,
}

struct RunTurnInnerRequest {
    turn_id: String,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreflightCompactionPlan {
    ranges: CompactionRanges,
    committed_transcript: Option<Vec<TurnMessage>>,
    active_turn: Option<ActiveTurnPlan>,
    prior_active_turn_summary: Option<String>,
    prior_active_turn_cursor: Option<ActiveTurnCompactionCursor>,
    turn_id: String,
    base_message_count: usize,
    runtime_budget: PreflightRuntimeProjectionBudget,
}

struct PreflightCompactor<'a> {
    engine: &'a SessionEngine,
    session: &'a mut SessionHandle,
    active_start_index: usize,
    turn_id: String,
    base_message_count: usize,
    active_projection_compacted: bool,
    provider_context_anchor: Option<ProviderContextUsageAnchor>,
    delegation_projection_loaded: bool,
    delegation_projection: Option<String>,
    delegation_projection_inserted: bool,
    background_projection: Option<String>,
    background_projection_insert_index: Option<usize>,
    background_completion_delivery_ids: Vec<crate::tool::ProcessCompletionDeliveryReceipt>,
}

#[async_trait]
impl SessionTurnPreflight for PreflightCompactor<'_> {
    async fn before_provider_request(
        &mut self,
        system_prompt: &mut String,
        provider_messages: &mut Vec<SessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        self.remove_background_projection(provider_messages);
        self.engine
            .turn_loop
            .tool_registry()
            .rollback_process_deliveries_for_owner(&self.session.metadata.id, None)
            .await;
        let (projection, delivery_ids) = self
            .engine
            .turn_loop
            .tool_registry()
            .begin_background_process_projection_delivery_for_owner(&self.session.metadata.id, None)
            .await;
        self.background_projection = projection;
        self.background_completion_delivery_ids = delivery_ids;
        self.load_delegation_projection().await?;
        self.insert_delegation_projection(provider_messages);
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
            self.insert_background_projection(provider_messages).await;
            return Ok(());
        }
        let trigger_tokens = self
            .trigger_context_tokens(system_prompt, provider_messages)
            .saturating_add(self.background_projection_tokens());
        if !auto_compact_should_trigger(trigger_tokens, trigger_threshold) {
            self.insert_background_projection(provider_messages).await;
            return Ok(());
        }
        let projected_base_system_prompt = system_prompt.clone();
        let Some(projection) = self
            .engine
            .compact_provider_preflight(
                self.session,
                PreflightCompactionRequest {
                    base_system_prompt: &projected_base_system_prompt,
                    active_suffix,
                    turn_id: &self.turn_id,
                    base_message_count: self.base_message_count,
                    active_projection_compacted: self.active_projection_compacted,
                    runtime_projection_tokens: self.runtime_projection_tokens(),
                },
                emit,
            )
            .await?
        else {
            self.insert_background_projection(provider_messages).await;
            return Ok(());
        };
        *system_prompt = projection.system_prompt;
        *provider_messages = projection.messages;
        // compact 已替换旧正文；不追踪逐 block 可见性，直接保守撤销该 session 的许可。
        self.engine
            .turn_loop
            .clear_file_read_state(&self.session.metadata.id)
            .await;
        self.active_start_index = projection.active_start_index;
        self.delegation_projection_inserted = false;
        self.background_projection_insert_index = None;
        self.insert_delegation_projection(provider_messages);
        self.insert_background_projection(provider_messages).await;
        self.active_projection_compacted = true;
        self.provider_context_anchor = None;
        self.engine
            .clear_active_context_usage_anchor(&self.session.metadata.id);
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

    async fn after_provider_response_success(&mut self) -> anyhow::Result<()> {
        if !self.background_completion_delivery_ids.is_empty() {
            self.engine
                .turn_loop
                .tool_registry()
                .commit_completion_notification_delivery_for_owner(
                    &self.session.metadata.id,
                    None,
                    &self.background_completion_delivery_ids,
                )
                .await;
            self.background_completion_delivery_ids.clear();
        }
        Ok(())
    }
}

impl PreflightCompactor<'_> {
    async fn load_delegation_projection(&mut self) -> anyhow::Result<()> {
        if self.delegation_projection_loaded {
            return Ok(());
        }
        self.delegation_projection = delegation_summary_projection(&self.session.paths.dir).await?;
        self.delegation_projection_loaded = true;
        Ok(())
    }

    fn insert_delegation_projection(&mut self, provider_messages: &mut Vec<SessionTurnMessage>) {
        if self.delegation_projection_inserted {
            return;
        }
        let Some(projection) = self.delegation_projection.clone() else {
            return;
        };
        let insert_index = self.active_start_index.min(provider_messages.len());
        provider_messages.insert(insert_index, SessionTurnMessage::user_text(projection));
        self.active_start_index = insert_index.saturating_add(1);
        self.delegation_projection_inserted = true;
    }

    fn remove_background_projection(&mut self, provider_messages: &mut Vec<SessionTurnMessage>) {
        let Some(index) = self.background_projection_insert_index.take() else {
            return;
        };
        if index < provider_messages.len() {
            provider_messages.remove(index);
            if index < self.active_start_index {
                self.active_start_index = self.active_start_index.saturating_sub(1);
            }
            // provider usage 是包含该 runtime-only message 的实测值。删除它后不能只把
            // message_count 原样沿用，否则下一轮把新 tool result 当成 anchor 之前的内容，
            // 从而低估 context；重算虽稍保守，但不会越过 compaction safety budget。
            self.provider_context_anchor = None;
        }
    }

    async fn insert_background_projection(
        &mut self,
        provider_messages: &mut Vec<SessionTurnMessage>,
    ) {
        let Some(projection) = self.background_projection.clone() else {
            return;
        };
        let index = self.active_start_index.min(provider_messages.len());
        provider_messages.insert(index, SessionTurnMessage::user_text(projection));
        self.active_start_index = index.saturating_add(1);
        self.background_projection_insert_index = Some(index);
    }

    fn background_projection_tokens(&self) -> usize {
        self.background_projection.as_ref().map_or(0, |projection| {
            estimate_session_turn_messages_tokens(&[SessionTurnMessage::user_text(projection)])
        })
    }

    fn delegation_projection_tokens(&self) -> usize {
        self.delegation_projection.as_ref().map_or(0, |projection| {
            estimate_session_turn_messages_tokens(&[SessionTurnMessage::user_text(projection)])
        })
    }

    /// compact 期间暂时从 raw projection 拿掉、校验后再插回的全部 runtime-only 内容。
    fn runtime_projection_tokens(&self) -> usize {
        self.background_projection_tokens()
            .saturating_add(self.delegation_projection_tokens())
    }
}

impl PreflightCompactor<'_> {
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
    local_claims: &'a [Claim],
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
    prior_active_turn_summary: Option<&'a str>,
    active_turn_user_anchor: Option<&'a SessionTurnMessage>,
    active_turn_start_segment: Option<usize>,
    active_turn_end_segment: Option<usize>,
    active_turn_transcript: Option<&'a [TurnMessage]>,
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
}

#[async_trait]
impl InboxJsonGenerator for SessionInboxJsonGenerator<'_> {
    async fn generate_json(
        &self,
        kind: InboxInternalizeKind,
        request: InternalizeRequest,
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
            .generate_json(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
            )
            .await
    }
}

/// explicit cancel 已经允许放弃未完成的 tool future；turn journal 的 forwarder / writer
/// 同样不能无限等待。尽量在同一个 100ms 窗口内写完 PendingCancel 和 TurnFinished，超时后
/// abort 本地 task 即可，绝不据此宣称外部工具副作用被回滚。
async fn finish_cancelled_turn_journal(
    emitter: TurnJournalEmitter,
    writer: JoinHandle<anyhow::Result<()>>,
    control_forwarder: Option<TurnControlJournalForwarder>,
) {
    let deadline = Instant::now() + EXPLICIT_CANCEL_JOURNAL_SETTLE_GRACE;
    if let Some(forwarder) = control_forwarder {
        forwarder.set_drain_on_shutdown(true);
        forwarder.shutdown.cancel();
        let mut handle = forwarder.handle;
        match time::timeout_at(deadline, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log::warn!(
                    target: "agent",
                    "cancelled turn control journal forwarder failed: {error:#}"
                );
            }
            Err(_) => {
                handle.abort();
                log::warn!(
                    target: "agent",
                    "cancelled turn control journal forwarder exceeded bounded settle grace"
                );
            }
        }
    }

    if Instant::now() < deadline {
        let finish = emitter.finish(TurnJournalStatus::Cancelled);
        tokio::pin!(finish);
        if time::timeout_at(deadline, &mut finish).await.is_err() {
            log::warn!(
                target: "agent",
                "cancelled turn journal emitter exceeded bounded settle grace"
            );
        }
    }

    let mut writer = writer;
    if Instant::now() >= deadline {
        writer.abort();
        log::warn!(
            target: "agent",
            "cancelled turn journal writer skipped after bounded settle grace"
        );
        return;
    }
    match time::timeout_at(deadline, &mut writer).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            log::warn!(target: "agent", "cancelled turn journal write failed: {error:#}");
        }
        Ok(Err(error)) => {
            log::warn!(
                target: "agent",
                "cancelled turn journal writer task failed: {error:#}"
            );
        }
        Err(_) => {
            writer.abort();
            log::warn!(
                target: "agent",
                "cancelled turn journal writer exceeded bounded settle grace"
            );
        }
    }
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
            attachment: AttachmentConfig::default(),
            mcp_manager: None,
            subagent_max_concurrent: options.subagent_max_concurrent,
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
            .take_process_completions_for_root_session(&session.metadata.id)
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
                    "Background process completed: process_id={} owner_agent={} owner_root_session={} owner_subagent={} status={} {}",
                    completion.process_id,
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
                owner_agent_id: completion.owner.owner_agent_id,
                owner_root_session_id: completion.owner.root_session_id,
                owner_subagent_id: completion.owner.subagent_id,
                status: completion.status,
                exit_code: completion.exit_code,
                signal: completion.signal,
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

    /// TUI/CLI runtime 退出时收束当前 engine 共享 registry 的全部受管 terminal。
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
        estimated_session_message_tokens_projected(messages, None)
    }

    #[cfg(test)]
    fn estimated_projected_message_tokens<'a>(
        messages: impl IntoIterator<Item = &'a SessionMessage>,
        tool_result_raw_max_chars: usize,
    ) -> usize {
        estimated_session_message_tokens_projected(messages, Some(tool_result_raw_max_chars))
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
            anyhow::bail!(
                "Compacted provider projection still exceeds hard tail budget: estimated raw tail tokens={raw_tail_tokens}, runtime projection tokens={runtime_projection_tokens}, combined tail tokens={projected_tokens}, hard tail budget={hard_limit}."
            );
        }
        Ok(())
    }

    fn preflight_projection(
        &self,
        base_system_prompt: &str,
        state: &SessionCompactionState,
        session_messages: &[SessionMessage],
        active_suffix: &[SessionTurnMessage],
        active_context: ActiveProjectionContext<'_>,
        budget: ProviderProjectionBudget,
    ) -> ProviderProjection {
        project_provider_context(
            base_system_prompt,
            state,
            session_messages,
            active_suffix.to_vec(),
            active_context,
            budget,
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
        let externalized =
            externalize_heavy_user_blocks(projection, &session.paths.compaction_assets_dir).await;
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
        session: &SessionHandle,
        user_text: &str,
        skill_instructions: &[SkillInstructions],
    ) -> anyhow::Result<(
        String,
        Option<String>,
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
        let recovery_turns = recovery_turn_chain(&projection, &canonical_messages);
        let recovery_context =
            turn_journal_recovery_context_for_chain(recovery_turns, self.turn_recovery_limits);
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
        Ok((turn_id, recovery_context, emitter, writer))
    }

    async fn finish_turn_journal(
        &self,
        session: &SessionHandle,
        emitter: TurnJournalEmitter,
        writer: JoinHandle<anyhow::Result<()>>,
        control_forwarder: Option<TurnControlJournalForwarder>,
        status: TurnJournalStatus,
    ) {
        if status == TurnJournalStatus::Cancelled {
            finish_cancelled_turn_journal(emitter, writer, control_forwarder).await;
            return;
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
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let message = format!("Turn journal write failed: {e:#}");
                log::warn!(target: "agent", "{message}");
                self.append_session_event_log(session, "WARN", message)
                    .await;
            }
            Err(e) => {
                let message = format!("Turn journal writer task failed: {e:#}");
                log::warn!(target: "agent", "{message}");
                self.append_session_event_log(session, "WARN", message)
                    .await;
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
        let inbox_generator = SessionInboxJsonGenerator {
            prompt_registry: &self.prompt_registry,
            json_caller: &self.json_caller,
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
        emit(SessionEvent::StartupProgress {
            label: "syncing active policies...".into(),
        });
        emit(SessionEvent::StartupProgress {
            label: "processing inbox...".into(),
        });
        let inbox_generator = SessionInboxJsonGenerator {
            prompt_registry: &self.prompt_registry,
            json_caller: &self.json_caller,
        };
        let inbox_report = self.runner.process_inbox_with(&inbox_generator).await?;
        emit(SessionEvent::TeamServicesConnectionUpdated {
            status: inbox_report.team_services,
        });
        emit_warnings(&inbox_report.warnings, &mut emit);
        self.emit_local_claims_updated(&mut emit).await;
        emit(SessionEvent::StartupProgress {
            label: "preparing session prompt...".into(),
        });
        let system_prompt = self
            .render_session_system_prompt_for_inbox(&inbox_report)
            .await?;
        emit(SessionEvent::StartupProgress {
            label: "creating session...".into(),
        });
        let session = self
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
        let (turn_id, recovery_context, mut journal_emitter, journal_writer) = self
            .start_turn_journal(session, &user_text, &skill_instructions)
            .await?;
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
        let result = {
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
                    new_dispute_ids,
                } => {
                    emit(SessionEvent::CompactionCompleted {
                        compacted_until,
                        recapped_until,
                        new_claim_ids,
                        updated_claim_ids,
                        new_dispute_ids,
                    });
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
        };
        let turn_interrupted = result
            .as_ref()
            .err()
            .is_some_and(|e| e.downcast_ref::<SessionTurnInterrupted>().is_some());
        let messages_committed_metadata_error = result
            .as_ref()
            .err()
            .is_some_and(is_messages_committed_metadata_update_failed);
        let interrupted_status = if turn_interrupted {
            tool_boundary_interrupt_status
                .as_ref()
                .and_then(|status| status.lock().ok().and_then(|status| *status))
                .unwrap_or(TurnJournalStatus::InterruptedByUser)
        } else {
            TurnJournalStatus::Failed
        };
        let journal_status = if result.is_ok() || messages_committed_metadata_error {
            TurnJournalStatus::Committed
        } else if turn_interrupted {
            interrupted_status
        } else {
            TurnJournalStatus::Failed
        };
        drop(durable_recorder);
        self.finish_turn_journal(
            session,
            journal_emitter,
            journal_writer,
            control_forwarder,
            journal_status,
        )
        .await;
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
        let (system_prompt, history) = compacted_context_for_turn(
            &base_system_prompt,
            &metadata,
            all_messages,
            self.compaction_tail_token_limit(),
            self.compaction_hard_tail_token_limit(),
            self.compaction.tail_previous_real_user_turns,
            self.compaction.tool_result_raw_max_chars,
        )?;
        let active_start_index = history.len();
        let turn_id_for_tools = request.turn_id.clone();
        let mut preflight = PreflightCompactor {
            engine: self,
            session,
            active_start_index,
            turn_id: request.turn_id,
            base_message_count: previous_message_count,
            active_projection_compacted: false,
            provider_context_anchor: None,
            delegation_projection_loaded: false,
            delegation_projection: None,
            delegation_projection_inserted: false,
            background_projection: None,
            background_projection_insert_index: None,
            background_completion_delivery_ids: Vec::new(),
        };
        let turn = self
            .turn_loop
            .run_session_turn_with_hooks(
                SessionTurnRequest {
                    current_session_id: Some(metadata.id.clone()),
                    current_turn_id: Some(turn_id_for_tools),
                    system_prompt,
                    history,
                    user_text: request.user_text,
                    user_attachments: request.user_attachments,
                    skill_instructions: request.skill_instructions,
                },
                emit,
                request.tool_boundary_control,
                durable_recorder,
                Some(&mut preflight),
            )
            .await?;
        Ok(PreparedSessionTurn {
            previous_message_count,
            turn,
            provider_context_used_tokens: preflight
                .provider_context_anchor
                .map(|anchor| anchor.used_tokens),
        })
    }

    async fn commit_prepared_session_turn(
        &self,
        session: &mut SessionHandle,
        prepared: PreparedSessionTurn,
    ) -> anyhow::Result<CommittedSessionTurn> {
        let metadata = session.read_metadata().await?;
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
        self.clear_active_compaction(session).await?;
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
        let Some(user_message) = messages.first().filter(|message| message.role == "user") else {
            anyhow::bail!("prepared session turn 缺少首条 user message");
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
        )?;
        if let Some(projection) = delegation_summary_projection(&session.paths.dir).await? {
            history.push(SessionTurnMessage::user_text(projection));
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
                    .await?
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
                emit(SessionTurnEvent::CompactionFailed {
                    error: e.to_string(),
                });
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

    fn build_preflight_compaction_plan(
        &self,
        metadata: &crate::session::SessionMetadata,
        session_messages: &[SessionMessage],
        active_suffix: &[SessionTurnMessage],
        active_context: ActiveProjectionContext<'_>,
        active_projection_compacted: bool,
        runtime_budget: PreflightRuntimeProjectionBudget,
    ) -> anyhow::Result<PreflightCompactionPlan> {
        let projection_budget = runtime_budget.provider_projection;

        let active_turn = self.build_active_turn_plan(
            metadata,
            active_suffix,
            active_context.turn_id,
            active_context.base_message_count,
            projection_budget.tail_token_limit,
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
        );
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
            .map(estimate_compacted_committed_summary_message_tokens)
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
        let committed_transcript = if summary_end > summary_start {
            Some(session_messages_to_turn_transcript(
                session_messages
                    .get(summary_start..summary_end)
                    .with_context(|| {
                        format!(
                            "session compact summary 范围越界: [{summary_start}, {summary_end})"
                        )
                    })?,
            ))
        } else {
            None
        };
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
            active_turn,
            prior_active_turn_summary: prior_active_turn
                .as_ref()
                .map(|prior| prior.summary.clone()),
            prior_active_turn_cursor: prior_active_turn.map(|prior| prior.cursor),
            turn_id: active_context.turn_id.to_string(),
            base_message_count: active_context.base_message_count,
            runtime_budget,
        })
    }

    fn build_active_turn_plan(
        &self,
        metadata: &crate::session::SessionMetadata,
        active_suffix: &[SessionTurnMessage],
        turn_id: &str,
        base_message_count: usize,
        tail_token_limit: usize,
    ) -> anyhow::Result<Option<ActiveTurnPlan>> {
        let segments = active_provider_safe_segments(active_suffix);
        if segments.is_empty() {
            return Ok(None);
        }
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
        if current_coverage >= segments.len() {
            return Ok(None);
        }
        let summary_start_segment = current_coverage;
        let summary_end_segment = self.active_summary_end_segment(
            active_suffix,
            &segments,
            current_coverage,
            tail_token_limit,
        );
        if summary_end_segment <= summary_start_segment {
            return Ok(None);
        }
        let summary_messages = active_segment_messages(
            active_suffix,
            &segments[summary_start_segment..summary_end_segment],
        );
        let transcript = turn_messages_to_transcript(summary_messages);
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
            transcript,
        }))
    }

    fn active_summary_end_segment(
        &self,
        active_suffix: &[SessionTurnMessage],
        segments: &[MessageRange],
        current_coverage: usize,
        tail_token_limit: usize,
    ) -> usize {
        let anchor_tokens = active_suffix
            .first()
            .map(|message| estimate_session_turn_messages_tokens(std::slice::from_ref(message)))
            .unwrap_or(0);
        let mut remaining_raw_tail_budget = tail_token_limit.saturating_sub(anchor_tokens);
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
            (true, Some(summary)) => non_empty_summary(
                enforce_summary_max_chars(summary, summary_max_chars),
                "committed_summary",
            )?,
            (true, None) => anyhow::bail!("compaction summary missing committed_summary"),
            (false, summary) => summary
                .or_else(|| prior_committed_summary.map(ToOwned::to_owned))
                .unwrap_or_default(),
        };
        let active_turn_summary = match (plan.active_turn.as_ref(), outcome.active_turn_summary) {
            (Some(_), Some(summary)) => Some(non_empty_summary(
                enforce_summary_max_chars(summary, summary_max_chars),
                "active_turn_summary",
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
            prior_active_turn_summary: prior_active_summary.as_deref(),
            active_turn_user_anchor: plan
                .active_turn
                .as_ref()
                .and_then(|_| active_suffix.first()),
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
            let (summary_result, recap_result) = tokio::join!(
                self.generate_compaction_summary(session, &summary_inputs, emit),
                self.prepare_finalize_segment(recap_segment)
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
                Err(first_projection_error) => {
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
                            anyhow::anyhow!(
                                "Compaction could not fit the mandatory context after externalizing reusable Skill/attachment blocks and retrying with a half-size summary. Split the current plain-text request or start a new session, then retry. Details: {error:#}"
                            )
                        });
                    audit_try!(final_projection)
                }
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
            None => audit_try!(self.prepare_finalize_segment(recap_segment).await),
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
                let error = e.to_string();
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
        if ranges.summary_end_index <= ranges.summary_start_index && !has_recoverable_checkpoint {
            return Ok(ManualCompactionOutcome::Noop(compaction_noop_reason(
                &metadata, &ranges,
            )));
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
                    .clear_file_read_state(&session.metadata.id)
                    .await;
                emit(SessionEvent::CompactionCompleted {
                    compacted_until: outcome.state.committed_message_until(),
                    recapped_until,
                    new_claim_ids: outcome.report.new_claim_ids.clone(),
                    updated_claim_ids: outcome.report.updated_claim_ids.clone(),
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
                                session_messages_to_turn_transcript(summary_segment);
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
                                committed_transcript: Some(&summary_transcript),
                                prior_active_turn_summary: None,
                                active_turn_user_anchor: None,
                                active_turn_start_segment: None,
                                active_turn_end_segment: None,
                                active_turn_transcript: None,
                                summary_max_chars: self.compaction.summary_max_chars,
                            };
                            let (summary_result, recap_result) = tokio::join!(
                                self.generate_compaction_summary(session, &summary_inputs, emit),
                                self.prepare_finalize_segment(recap_segment)
                            );
                            let generated_compaction = summary_result?;
                            generated_audit_ids.push(generated_compaction.audit_id.clone());
                            let compaction = generated_compaction.outcome;
                            let (used_claim_ids, prepared_claims, prepared_disputes) =
                                audit_try!(recap_result);
                            let summary = enforce_summary_max_chars(
                                audit_try!(compaction.committed_summary.with_context(|| {
                                    "compaction summary missing committed_summary"
                                })),
                                self.compaction.summary_max_chars,
                            );
                            (
                                used_claim_ids,
                                prepared_claims,
                                prepared_disputes,
                                audit_try!(non_empty_summary(summary, "committed_summary")),
                                vec![generated_compaction.audit_id],
                            )
                        }
                        (true, false) => {
                            let (used_claim_ids, prepared_claims, prepared_disputes) =
                                self.prepare_finalize_segment(recap_segment).await?;
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
                                session_messages_to_turn_transcript(summary_segment);
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
                                committed_transcript: Some(&summary_transcript),
                                prior_active_turn_summary: None,
                                active_turn_user_anchor: None,
                                active_turn_start_segment: None,
                                active_turn_end_segment: None,
                                active_turn_transcript: None,
                                summary_max_chars: self.compaction.summary_max_chars,
                            };
                            let generated_compaction = self
                                .generate_compaction_summary(session, &summary_inputs, emit)
                                .await?;
                            generated_audit_ids.push(generated_compaction.audit_id.clone());
                            let compaction = generated_compaction.outcome;
                            let summary = enforce_summary_max_chars(
                                audit_try!(compaction.committed_summary.with_context(|| {
                                    "compaction summary missing committed_summary"
                                })),
                                self.compaction.summary_max_chars,
                            );
                            (
                                Vec::new(),
                                Vec::new(),
                                Vec::new(),
                                audit_try!(non_empty_summary(summary, "committed_summary")),
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
                recap_segment,
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
        let system_prompt = self
            .prompt_registry
            .render(
                PROMPT_SESSION_COMPACTION,
                serde_json::json!({
                    "summary_max_chars": inputs.summary_max_chars,
                }),
            )
            .context("渲染 session_compaction prompt 失败")?;
        let payload = SessionCompactionPayload {
            instruction: COMPACTION_INSTRUCTION,
            agent_id: self.runner.agent_id.as_str(),
            committed_start_index: inputs.committed_start_index,
            committed_end_index: inputs.committed_end_index,
            prior_committed_summary: inputs.prior_committed_summary,
            committed_transcript: inputs.committed_transcript,
            prior_active_turn_summary: inputs.prior_active_turn_summary,
            active_turn_user_anchor: inputs.active_turn_user_anchor,
            active_turn_start_segment: inputs.active_turn_start_segment,
            active_turn_end_segment: inputs.active_turn_end_segment,
            active_turn_transcript: inputs.active_turn_transcript,
            summary_max_chars: inputs.summary_max_chars,
        };
        let user_text = serde_json::to_string_pretty(&payload)?;
        let payload_preview = audit_text_preview(&user_text, COMPACTION_AUDIT_PREVIEW_CHARS);
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
            .generate_json_validated_with_attempt_notice(
                system_prompt,
                vec![SessionTurnMessage::user_text(user_text)],
                |value| parse_compaction_summary_outcome(value, inputs).map_err(anyhow::Error::from),
                |retry_index, retry_total, e| {
                    let message = format!(
                        "compaction summary JSON invalid, retrying ({retry_index}/{retry_total}): {e:#}"
                    );
                    emit(SessionEvent::Warning {
                        message: message.clone(),
                    });
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
                Err(error)
            }
        }
    }
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
) -> serde_json::Result<SessionCompactionOutcome> {
    let object = value.as_object().ok_or_else(|| {
        serde_json::Error::custom("compaction summary response must be a JSON object")
    })?;
    if !object.contains_key("committed_summary") {
        return Err(serde_json::Error::custom(
            "committed_summary key must be present",
        ));
    }
    if !object.contains_key("active_turn_summary") {
        return Err(serde_json::Error::custom(
            "active_turn_summary key must be present",
        ));
    }
    let outcome: SessionCompactionOutcome = serde_json::from_value(value)?;
    if inputs.committed_transcript.is_some() && outcome.committed_summary.is_none() {
        return Err(serde_json::Error::custom(
            "committed_summary must be a string when committed_transcript is present",
        ));
    }
    if inputs.committed_transcript.is_some()
        && outcome
            .committed_summary
            .as_deref()
            .is_some_and(|summary| summary.trim().is_empty())
    {
        return Err(serde_json::Error::custom(
            "committed_summary must not be empty when committed_transcript is present",
        ));
    }
    if inputs.committed_transcript.is_none() && outcome.committed_summary.is_some() {
        return Err(serde_json::Error::custom(
            "committed_summary must be null when committed_transcript is null",
        ));
    }
    if inputs.active_turn_transcript.is_some() && outcome.active_turn_summary.is_none() {
        return Err(serde_json::Error::custom(
            "active_turn_summary must be a string when active_turn_transcript is present",
        ));
    }
    if inputs.active_turn_transcript.is_some()
        && outcome
            .active_turn_summary
            .as_deref()
            .is_some_and(|summary| summary.trim().is_empty())
    {
        return Err(serde_json::Error::custom(
            "active_turn_summary must not be empty when active_turn_transcript is present",
        ));
    }
    if inputs.active_turn_transcript.is_none() && outcome.active_turn_summary.is_some() {
        return Err(serde_json::Error::custom(
            "active_turn_summary must be null when active_turn_transcript is null",
        ));
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
    subagents: Vec<DelegationSummary>,
    omitted: usize,
    note: &'static str,
}

async fn delegation_summary_projection(session_dir: &Path) -> anyhow::Result<Option<String>> {
    let page = DelegationStore::new(session_dir.to_path_buf())
        .list_page(DELEGATION_PROJECTION_MAX_ITEMS)
        .await
        .context("读取 subagent summary projection 失败")?;
    let summaries = page.summaries;
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
        note: "Runtime-only bounded projection. Use list_subagents/read_subagent for explicit details. Full subagent transcript and event logs are intentionally omitted.",
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
        payload.note = "Runtime-only bounded projection exceeded the hard budget; subagent details omitted. Use list_subagents/read_subagent for explicit details.";
        json = serde_json::to_string_pretty(&payload)?;
    }
    Ok(Some(format!("{start_tag}{json}{end_tag}")))
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

fn user_text_with_recovery_context(user_text: String, recovery_context: Option<&str>) -> String {
    let Some(recovery_context) = recovery_context else {
        return user_text;
    };
    let payload = tag_safe_json_payload(&serde_json::json!({ "text": user_text }));
    format!("{recovery_context}\n\n<current_user_request>\n{payload}\n</current_user_request>")
}

fn is_messages_committed_metadata_update_failed(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<SessionStoreError>(),
        Some(SessionStoreError::MessagesCommittedMetadataUpdateFailed { .. })
    )
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

fn canonical_user_request_text(text: &str) -> Cow<'_, str> {
    extract_current_user_request(text).unwrap_or(Cow::Borrowed(text))
}

fn first_text_session_content(blocks: &[SessionContentBlock]) -> Option<&str> {
    blocks.iter().find_map(|block| match block {
        SessionContentBlock::Text { text } => Some(text.as_str()),
        SessionContentBlock::SkillInstructions { .. } => None,
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

fn enforce_summary_max_chars(summary: String, summary_max_chars: usize) -> String {
    if summary.chars().count() <= summary_max_chars {
        return summary;
    }
    summary.chars().take(summary_max_chars).collect()
}

fn non_empty_summary(summary: String, field: &str) -> anyhow::Result<String> {
    if summary.trim().is_empty() {
        anyhow::bail!("{field} must not be empty");
    }
    Ok(summary)
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
