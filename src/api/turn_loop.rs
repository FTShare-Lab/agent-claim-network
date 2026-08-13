//! provider-neutral 的 session turn/tool loop。
//!
//! 本模块承接单轮用户输入后的模型调用、工具执行和 tool_result 回灌。
//! 它只理解 canonical session message，不关心 Anthropic/OpenAI 等后端协议细节。

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::{DateTime, Local, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use tokio::time::Instant;

use super::continuation::{append_with_overlap_dedupe, CONTINUATION_TRIGGER};
use super::provider::{
    ProviderNoConsumableOutput, ProviderRequestObserver, ProviderRequestPreparationFailure,
    ProviderStreamFailure, ProviderTerminalFailure,
};
use crate::api::{
    estimate_provider_request_context_tokens, CompletedSessionTurnMessage, ContextUsageSnapshot,
    ModelContextSource, ProviderAdapter, ProviderEvent, ProviderHistoryMediaPolicy,
    ProviderRecoveryInterrupt, ProviderReplayIdentity, ProviderReplayState, ProviderRequest,
    ProviderResponse, ProviderRuntimeChainId, ProviderStop, SessionAttachment, SessionTurn,
    SessionTurnContentBlock, SessionTurnEvent, SessionTurnInterrupted, SessionTurnMessage,
    SessionTurnRequest, ToolBoundaryControl, ToolCallSkipReason, ToolExecutionOutcome,
};
use crate::attachment::{AttachmentKind, AttachmentLimits, NormalizedMedia, FILE_READ_MEDIA_KEY};
use crate::claim::SessionId;
use crate::mcp::tool::McpToolRoute;
use crate::skill::SkillInstructions;
use crate::tool::diff::{take_file_change, FileChange};
use crate::tool::{
    ProcessDeliveryReceipt, ToolDispatchContext, ToolError, ToolProgressUpdate, ToolRegistry,
};
use tokio_util::sync::CancellationToken;

const DEFAULT_TOOL_INPUT_JOURNAL_PREVIEW_CHARS: usize = 2048;
const DEFAULT_TOOL_OUTPUT_JOURNAL_PREVIEW_CHARS: usize = 4096;
const NON_STREAMING_FALLBACK_MAX_ATTEMPTS: u32 = 5;
const NON_STREAMING_FALLBACK_ERROR_MAX_CHARS: usize = 4096;
// 产品要求退避保持内部实现细节，不进入 config.toml。
const NON_STREAMING_FALLBACK_BASE_DELAY: Duration = Duration::from_millis(250);
const NON_STREAMING_FALLBACK_MAX_DELAY: Duration = Duration::from_secs(4);
const MAX_CONTEXT_WINDOW_RECOVERIES: usize = 2;
// Provider WAL 是发起网络 I/O 前的内部保护边界，不与用户可配置的
// LLM 请求超时共用几分钟的等待时间。
const PROVIDER_WAL_PREPARATION_TIMEOUT: Duration = Duration::from_secs(10);

struct ProviderCallOutcome {
    response: ProviderResponse,
    /// 成功 attempt 最后一次实际发送的精确请求历史。
    request_messages: Vec<SessionTurnMessage>,
    /// 相对 `request_messages` 的本次响应 suffix；不重复 adapter
    /// 内部 continuation 已经放入请求的 partial replay。
    provider_assistant_message: SessionTurnMessage,
    recovered_with_non_streaming: bool,
    /// 与该次逻辑 sampling 的 tool definitions 同时冻结；内部 retry、fallback、
    /// continuation 不得刷新，工具派发也不得落到 replacement generation。
    provider_mcp_routes: Arc<BTreeMap<String, McpToolRoute>>,
}

/// 跟踪一次 turn-loop Provider call 内部实际发送的最新请求。
///
/// adapter continuation 只能追加 replay message；main 路径在更新内存基线前
/// 先通过 preflight 写 WAL，child 路径则至少在当前 execution 内保留同一前缀。
struct ProviderRequestProgress<'a> {
    latest_messages: Vec<SessionTurnMessage>,
    preflight: Option<&'a mut dyn SessionTurnPreflight>,
    canonical_tail_count: usize,
    preparing_write_ahead: Arc<AtomicBool>,
}

impl<'a> ProviderRequestProgress<'a> {
    fn new(
        latest_messages: Vec<SessionTurnMessage>,
        preflight: Option<&'a mut dyn SessionTurnPreflight>,
        canonical_tail_count: usize,
    ) -> Self {
        Self {
            latest_messages,
            preflight,
            canonical_tail_count,
            preparing_write_ahead: Arc::new(AtomicBool::new(false)),
        }
    }

    fn latest_messages(&self) -> &[SessionTurnMessage] {
        &self.latest_messages
    }

    fn take_preflight(&mut self) -> Option<&'a mut dyn SessionTurnPreflight> {
        self.preflight.take()
    }

    fn begin_provider_attempt(&self) {
        self.preparing_write_ahead.store(false, Ordering::Release);
    }

    fn write_ahead_phase(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.preparing_write_ahead)
    }
}

#[async_trait]
impl ProviderRequestObserver for ProviderRequestProgress<'_> {
    async fn before_provider_request(
        &mut self,
        messages: &[SessionTurnMessage],
    ) -> anyhow::Result<()> {
        if messages == self.latest_messages {
            return Ok(());
        }
        self.preparing_write_ahead.store(true, Ordering::Release);
        if !messages.starts_with(&self.latest_messages) {
            return Err(ProviderRequestPreparationFailure::new(format!(
                "adapter continuation 改写了已发送 Provider 前缀: previous={}, current={}",
                self.latest_messages.len(),
                messages.len()
            ))
            .into());
        }
        if let Some(preflight) = self.preflight.as_deref_mut() {
            match time::timeout(
                PROVIDER_WAL_PREPARATION_TIMEOUT,
                preflight.provider_request_ready(messages, self.canonical_tail_count),
            )
            .await
            {
                Ok(result) => {
                    result.map_err(ProviderRequestPreparationFailure::from_error)?;
                }
                Err(_) => {
                    return Err(ProviderRequestPreparationFailure::new(
                        "Provider 请求状态保存超时（10 秒）",
                    )
                    .into());
                }
            }
        }
        self.latest_messages = messages.to_vec();
        self.preparing_write_ahead.store(false, Ordering::Release);
        Ok(())
    }
}

#[derive(Default)]
struct ContextWindowContinuation {
    merged_text: String,
    replay_model: Option<String>,
    replay_messages: Vec<Value>,
}

impl ContextWindowContinuation {
    fn has_pending(&self) -> bool {
        self.replay_model.is_some()
    }

    fn fallback_replacement_text(&self, current: &str) -> String {
        let mut merged = self.merged_text.clone();
        append_with_overlap_dedupe(&mut merged, current);
        merged
    }

    fn absorb_partial(&mut self, message: &SessionTurnMessage) -> anyhow::Result<()> {
        append_with_overlap_dedupe(&mut self.merged_text, &assistant_message_text(message));
        let Some(ProviderReplayState::AnthropicMessages { model, messages }) =
            message.provider_replay.as_ref()
        else {
            anyhow::bail!("模型返回上下文截断，但缺少 Anthropic 续写状态，无法自动恢复");
        };
        if let Some(existing_model) = self.replay_model.as_ref() {
            if existing_model != model {
                anyhow::bail!("上下文恢复期间模型发生变化，无法继续本轮");
            }
        } else {
            self.replay_model = Some(model.clone());
        }
        self.replay_messages.extend(messages.iter().cloned());
        Ok(())
    }

    fn push_internal_continuation(&mut self) {
        self.replay_messages.push(json!({
            "role": "user",
            "content": [{"type": "text", "text": CONTINUATION_TRIGGER}],
        }));
    }

    fn merge_into(self, message: &mut SessionTurnMessage) -> anyhow::Result<()> {
        let Some(replay_model) = self.replay_model else {
            return Ok(());
        };
        let mut merged_text = self.merged_text;
        append_with_overlap_dedupe(&mut merged_text, &assistant_message_text(message));
        let mut non_text = message
            .content
            .drain(..)
            .filter(|block| !matches!(block, SessionTurnContentBlock::Text { .. }))
            .collect::<Vec<_>>();
        if merged_text.trim().is_empty() {
            message.content = non_text;
        } else {
            let mut content = Vec::with_capacity(non_text.len().saturating_add(1));
            content.push(SessionTurnContentBlock::text(merged_text));
            content.append(&mut non_text);
            message.content = content;
        }

        let Some(ProviderReplayState::AnthropicMessages { model, messages }) =
            message.provider_replay.take()
        else {
            anyhow::bail!("模型续写完成，但缺少 Anthropic 历史状态，无法保存完整结果");
        };
        if model != replay_model {
            anyhow::bail!("模型续写前后的 Anthropic 历史状态不一致，无法保存完整结果");
        }
        let mut replay_messages = self.replay_messages;
        replay_messages.extend(messages);
        message.provider_replay = Some(ProviderReplayState::AnthropicMessages {
            model,
            messages: replay_messages,
        });
        Ok(())
    }
}

/// 把进程输出回执绑定到生成它的原始 tool_result。只有该原文实际进入 provider
/// request，成功响应后才能推进输出 cursor。
struct PendingProcessDelivery {
    receipt: ProcessDeliveryReceipt,
    tool_use_id: String,
    tool_result_content: String,
}

enum ToolBatchOutcome {
    Completed,
    Interrupted,
}

struct ToolBatchCompletion {
    source_index: usize,
    tool_use: CanonicalToolUse,
    executed: Result<ExecutedToolUse, ToolUseInterrupted>,
}

const MAX_PROGRESS_EVENTS_PER_DRAIN: usize = 64;

#[async_trait]
pub trait SessionTurnEventRecorder: Send {
    async fn record(&mut self, event: SessionTurnEvent) -> anyhow::Result<()>;

    /// 在包含该快照的 provider request 发出前写入 owner 的现有 durable transcript。
    async fn record_completed_message(
        &mut self,
        _message: &CompletedSessionTurnMessage,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// 只读观察外部运行态并返回待追加快照；历史替换仍只属于 `SessionTurnPreflight`
/// 的 compaction 边界。
#[async_trait]
pub trait SessionTurnContextAppender: Send {
    async fn observe_context(
        &mut self,
        provider_messages: &[SessionTurnMessage],
    ) -> anyhow::Result<Vec<SessionTurnMessage>>;

    async fn after_provider_response_success(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait SessionTurnPreflight: Send {
    /// 本轮初始 history 中已经由此前真实 Provider 请求确认、必须原样重放的前缀长度。
    ///
    /// turn loop 只会在此前缀之后规范化新后缀，避免 adjacent-user merge 等 wire
    /// 投影跨过稳定缓存边界、改写上一请求。compaction 显式替换 history 后会清空边界。
    fn frozen_provider_history_prefix_len(&self) -> usize {
        0
    }

    /// 在本次逻辑 Provider 请求观察 runtime/background/delegation 之前执行。
    ///
    /// 这里只用于必须先行且本身也是 append-only 的输入（当前为 child steering）；
    /// compaction 等会替换 history 的动作仍放在 `before_provider_request`，确保预算
    /// 覆盖本次刚冻结的 context snapshot。
    async fn before_context_observation(
        &mut self,
        _system_prompt: &mut String,
        _provider_messages: &mut Vec<SessionTurnMessage>,
        _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn before_provider_request(
        &mut self,
        system_prompt: &mut String,
        provider_messages: &mut Vec<SessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<()>;

    /// 当前原始请求是否已经满足 history replacement 的触发条件。
    ///
    /// turn loop 据此临时追加一组完整、已冻结的 context baseline，使 compactor
    /// 的预算和保护边界覆盖新窗口必须重建的 authoritative state。
    fn history_replacement_expected(
        &self,
        _system_prompt: &str,
        _provider_messages: &[SessionTurnMessage],
    ) -> bool {
        false
    }

    /// 返回并清除本次 preflight 是否实际替换了 provider history。
    fn take_history_replaced_since_last_check(&mut self) -> bool {
        false
    }

    /// 在 adapter retry 边界外、请求发出前 write-ahead 本次逻辑请求的精确
    /// provider-neutral 历史。`canonical_tail_count` 是本 active turn 已完成、
    /// 且包含在该请求中的 canonical message 数量。
    async fn provider_request_ready(
        &mut self,
        _provider_messages: &[SessionTurnMessage],
        _canonical_tail_count: usize,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// Provider 已成功返回最终 assistant 后，write-ahead 包含该响应的精确
    /// provider-neutral 历史。这样 canonical assistant 含有 continuation replay 时，
    /// 后续请求无需把已经出现在上一请求中的 replay 再追加一次。
    async fn provider_response_ready(
        &mut self,
        _provider_messages: &[SessionTurnMessage],
        _canonical_tail_count: usize,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    /// 下一次 provider request 必须先执行上下文窗口恢复压缩。
    /// 默认实现拒绝恢复，避免没有 compaction owner 的调用方原样重放满窗口请求。
    fn request_context_window_recovery(
        &mut self,
        _assistant_marker: &SessionTurnMessage,
    ) -> anyhow::Result<()> {
        anyhow::bail!("模型上下文已满，但当前 ACN 调用链未接入自动恢复")
    }

    fn observe_provider_context_usage(
        &mut self,
        _provider_message_count: usize,
        _usage: ContextUsageSnapshot,
    ) {
    }

    fn clear_provider_context_usage(&mut self) {}

    /// provider 已经成功返回并通过基本协议校验。runtime-only state 可在这里提交
    /// "本 request 已实际交付" 的有界消费；失败、取消和 retry 不会调用它。
    async fn after_provider_response_success(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// 一次 turn 调用可选的持久化、上下文观察与压缩 hook 集合。
pub(crate) struct SessionTurnHooks<'recorder, 'context, 'preflight> {
    durable_recorder: Option<&'recorder mut dyn SessionTurnEventRecorder>,
    context_appender: Option<&'context mut dyn SessionTurnContextAppender>,
    preflight: Option<&'preflight mut dyn SessionTurnPreflight>,
}

impl<'recorder, 'context, 'preflight> SessionTurnHooks<'recorder, 'context, 'preflight> {
    pub(crate) fn new(
        durable_recorder: Option<&'recorder mut dyn SessionTurnEventRecorder>,
        context_appender: Option<&'context mut dyn SessionTurnContextAppender>,
        preflight: Option<&'preflight mut dyn SessionTurnPreflight>,
    ) -> Self {
        Self {
            durable_recorder,
            context_appender,
            preflight,
        }
    }
}

#[cfg(test)]
#[async_trait]
trait ToolDispatchReservationHook: Send + Sync {
    async fn before_try_reserve_dispatch(&self);
}

pub struct AgentTurnLoop {
    provider: Arc<dyn ProviderAdapter>,
    tools: Arc<ToolRegistry>,
    max_tool_loop_turns: Option<usize>,
    max_tokens: u32,
    attachment_limits: AttachmentLimits,
    tool_input_journal_preview_chars: usize,
    tool_output_journal_preview_chars: usize,
    now: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    runtime_context: Arc<dyn Fn(DateTime<Utc>) -> String + Send + Sync>,
    #[cfg(test)]
    before_tool_dispatch_reservation: Option<Arc<dyn ToolDispatchReservationHook>>,
}

impl AgentTurnLoop {
    pub fn new(
        provider: Arc<dyn ProviderAdapter>,
        tools: Arc<ToolRegistry>,
        max_tokens: u32,
    ) -> Self {
        Self {
            provider,
            tools,
            // 主 session 不设 tool 回环次数上限；长程交互由用户取消或上下文管理收束。
            max_tool_loop_turns: None,
            max_tokens,
            attachment_limits: AttachmentLimits::default(),
            tool_input_journal_preview_chars: DEFAULT_TOOL_INPUT_JOURNAL_PREVIEW_CHARS,
            tool_output_journal_preview_chars: DEFAULT_TOOL_OUTPUT_JOURNAL_PREVIEW_CHARS,
            now: Arc::new(Utc::now),
            runtime_context: Arc::new(runtime_context_text),
            #[cfg(test)]
            before_tool_dispatch_reservation: None,
        }
    }

    /// 为子代理等非交互执行器设置内部 tool 回环上限。
    pub fn with_max_tool_loop_turns(mut self, max_tool_loop_turns: usize) -> Self {
        self.max_tool_loop_turns = Some(max_tool_loop_turns);
        self
    }

    pub(crate) fn tool_registry(&self) -> Arc<ToolRegistry> {
        Arc::clone(&self.tools)
    }

    pub(crate) fn max_tokens(&self) -> u32 {
        self.max_tokens
    }

    pub(crate) fn history_media_policy(&self) -> ProviderHistoryMediaPolicy {
        self.provider.history_media_policy()
    }

    pub(crate) fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        self.provider.history_replay_identity()
    }

    pub(crate) async fn discard_runtime_chain(&self, chain_id: ProviderRuntimeChainId) {
        self.provider.discard_runtime_chain(chain_id).await;
    }

    pub fn with_attachment_limits(mut self, limits: AttachmentLimits) -> Self {
        self.attachment_limits = limits;
        self
    }

    pub fn with_tool_journal_preview_limits(
        mut self,
        input_max_chars: usize,
        output_max_chars: usize,
    ) -> Self {
        self.tool_input_journal_preview_chars = input_max_chars;
        self.tool_output_journal_preview_chars = output_max_chars;
        self
    }

    #[cfg(test)]
    fn with_now_fn<F>(mut self, now: F) -> Self
    where
        F: Fn() -> DateTime<Utc> + Send + Sync + 'static,
    {
        self.now = Arc::new(now);
        self
    }

    #[cfg(test)]
    fn with_runtime_context_fn<F>(mut self, runtime_context: F) -> Self
    where
        F: Fn(DateTime<Utc>) -> String + Send + Sync + 'static,
    {
        self.runtime_context = Arc::new(runtime_context);
        self
    }

    #[cfg(test)]
    fn with_tool_dispatch_reservation_hook(
        mut self,
        hook: Arc<dyn ToolDispatchReservationHook>,
    ) -> Self {
        self.before_tool_dispatch_reservation = Some(hook);
        self
    }

    fn now(&self) -> DateTime<Utc> {
        (self.now)()
    }

    async fn observe_model_context(
        &self,
        provider_messages: &[SessionTurnMessage],
        context_appender: &mut Option<&mut dyn SessionTurnContextAppender>,
    ) -> anyhow::Result<Vec<CompletedSessionTurnMessage>> {
        let observed_at = self.now();
        let mut candidates = vec![SessionTurnMessage::model_context(
            ModelContextSource::Runtime,
            (self.runtime_context)(observed_at),
        )];
        if let Some(context_appender) = context_appender.as_mut() {
            candidates.extend(context_appender.observe_context(provider_messages).await?);
        }
        let candidates = candidates
            .into_iter()
            .map(|message| CompletedSessionTurnMessage::new(message, observed_at))
            .collect::<Vec<_>>();
        Ok(candidates)
    }

    async fn append_frozen_model_context(
        &self,
        provider_messages: &mut Vec<SessionTurnMessage>,
        committed: &mut Vec<CompletedSessionTurnMessage>,
        durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
        candidates: impl IntoIterator<Item = CompletedSessionTurnMessage>,
    ) -> anyhow::Result<()> {
        for completed in candidates {
            let (source, fingerprint, text) = completed
                .model_context_snapshot()
                .context("context appender 只能返回独立 ModelContext user message")?;
            let expected = SessionTurnMessage::model_context(*source, text.to_string());
            let Some((_, expected_fingerprint, _)) = expected.model_context_snapshot() else {
                anyhow::bail!("内部 ModelContext 构造失败");
            };
            if fingerprint != expected_fingerprint {
                anyhow::bail!("context appender 返回了与正文不匹配的 fingerprint");
            }
            if latest_model_context(provider_messages, *source).is_some_and(
                |(latest_fingerprint, latest_text)| {
                    latest_fingerprint == fingerprint && latest_text == text
                },
            ) {
                continue;
            }
            if let Some(recorder) = durable_recorder.as_deref_mut() {
                recorder.record_completed_message(&completed).await?;
            }
            provider_messages.push(completed.message.clone());
            committed.push(completed);
        }
        Ok(())
    }

    /// failed/cancelled turn 的 journal context 即使已经存在于 write-ahead Provider
    /// 窗口，也仍需在下一次成功 turn 中 materialize 到 canonical transcript；但不能
    /// 为此在精确 Provider 前缀后重复追加同一份历史快照。
    async fn materialize_recovered_model_context(
        &self,
        provider_messages: &mut Vec<SessionTurnMessage>,
        committed: &mut Vec<CompletedSessionTurnMessage>,
        durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
        recovered: impl IntoIterator<Item = CompletedSessionTurnMessage>,
    ) -> anyhow::Result<()> {
        let recovered = recovered.into_iter().collect::<Vec<_>>();
        for completed in &recovered {
            let (source, fingerprint, text) = completed
                .model_context_snapshot()
                .context("recovered context 必须是独立 ModelContext user message")?;
            let expected = SessionTurnMessage::model_context(*source, text.to_string());
            let Some((_, expected_fingerprint, _)) = expected.model_context_snapshot() else {
                anyhow::bail!("内部 ModelContext 构造失败");
            };
            if fingerprint != expected_fingerprint {
                anyhow::bail!("recovered context 的 fingerprint 与正文不匹配");
            }
        }
        let visible_context = provider_messages
            .iter()
            .filter_map(SessionTurnMessage::model_context_snapshot)
            .collect::<Vec<_>>();
        let recovered_context = recovered
            .iter()
            .filter_map(|completed| completed.model_context_snapshot())
            .collect::<Vec<_>>();
        let already_visible_prefix = (0..=recovered_context.len())
            .rev()
            .find(|prefix_len| visible_context.ends_with(&recovered_context[..*prefix_len]))
            .unwrap_or(0);

        for (index, completed) in recovered.into_iter().enumerate() {
            if let Some(recorder) = durable_recorder.as_deref_mut() {
                recorder.record_completed_message(&completed).await?;
            }
            if index >= already_visible_prefix {
                provider_messages.push(completed.message.clone());
            }
            committed.push(completed);
        }
        Ok(())
    }

    async fn persist_frozen_model_context(
        &self,
        committed: &mut Vec<CompletedSessionTurnMessage>,
        durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
        candidates: impl IntoIterator<Item = CompletedSessionTurnMessage>,
    ) -> anyhow::Result<()> {
        for completed in candidates {
            completed
                .model_context_snapshot()
                .context("只能持久化独立 ModelContext user message")?;
            if let Some(recorder) = durable_recorder.as_deref_mut() {
                recorder.record_completed_message(&completed).await?;
            }
            committed.push(completed);
        }
        Ok(())
    }

    /// 按 source order 切分并执行工具；并发 task 只回传结果，所有事件与 journal 写入均由此协调器串行完成。
    #[allow(
        clippy::too_many_arguments,
        reason = "工具协调器需要同时携带本轮 Provider catalog、journal 与取消边界"
    )]
    async fn execute_tool_uses_in_batches(
        &self,
        tool_uses: &[CanonicalToolUse],
        provider_mcp_routes: &Arc<BTreeMap<String, McpToolRoute>>,
        current_session_id: &Option<SessionId>,
        current_turn_id: &Option<String>,
        tool_boundary_control: Option<&ToolBoundaryControl>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
    ) -> anyhow::Result<Option<Vec<ExecutedToolUse>>> {
        // 同一条 provider 回复可能包含多个独立 tool_use。write_stdin 即使因显式
        // cursor 或空输出不生成 delivery receipt，也不能在后续同进程调用中重复产生
        // 写入、interrupt 或 terminate 副作用，因此在调度前按 source order 去重。
        let mut seen_write_stdin_process_ids = HashSet::new();
        let duplicate_write_stdin = tool_uses
            .iter()
            .map(|tool_use| {
                if tool_use.name != "write_stdin" {
                    return false;
                }
                let Some(process_id) = tool_use.input.get("process_id").and_then(Value::as_str)
                else {
                    return false;
                };
                !seen_write_stdin_process_ids.insert(process_id.to_string())
            })
            .collect::<Vec<_>>();
        let concurrency_safe = tool_uses
            .iter()
            .map(|tool_use| {
                self.tools
                    .is_concurrency_safe(&tool_use.name, &tool_use.input)
            })
            .collect::<Vec<_>>();
        let mut executions = (0..tool_uses.len())
            .map(|_| None)
            .collect::<Vec<Option<ExecutedToolUse>>>();
        let max_parallel = self.tools.max_parallel_tool_calls();
        let failed_file_write_paths = Arc::new(Mutex::new(BTreeSet::new()));
        let mut batch_start = 0usize;

        while batch_start < tool_uses.len() {
            if tool_boundary_is_cancelled(tool_boundary_control) {
                emit_skipped_tool_calls(
                    &tool_uses[batch_start..],
                    emit,
                    durable_recorder,
                    self.tool_input_journal_preview_chars,
                    tool_boundary_skip_reason_value(tool_boundary_control),
                )
                .await?;
                return Ok(None);
            }

            let is_safe_batch = concurrency_safe[batch_start];
            let mut batch_end = batch_start.saturating_add(1);
            if is_safe_batch {
                while batch_end < tool_uses.len() && concurrency_safe[batch_end] {
                    batch_end = batch_end.saturating_add(1);
                }
            }
            let active_limit = if is_safe_batch { max_parallel } else { 1 };
            let outcome = self
                .execute_tool_batch(
                    tool_uses,
                    &duplicate_write_stdin,
                    batch_start,
                    batch_end,
                    active_limit,
                    is_safe_batch,
                    current_session_id,
                    current_turn_id,
                    provider_mcp_routes,
                    tool_boundary_control,
                    emit,
                    durable_recorder,
                    &mut executions,
                    Arc::clone(&failed_file_write_paths),
                )
                .await?;
            if matches!(outcome, ToolBatchOutcome::Interrupted) {
                return Ok(None);
            }
            batch_start = batch_end;
        }

        if tool_boundary_is_cancelled(tool_boundary_control) {
            return Ok(None);
        }

        let mut ordered = Vec::with_capacity(executions.len());
        for execution in executions {
            let Some(execution) = execution else {
                anyhow::bail!("并发工具调度结束时缺少已完成调用结果");
            };
            ordered.push(execution);
        }
        Ok(Some(ordered))
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "此协调器需要持有单一事件/journal sink，参数均为本轮既有上下文"
    )]
    async fn execute_tool_batch(
        &self,
        all_tool_uses: &[CanonicalToolUse],
        duplicate_write_stdin: &[bool],
        batch_start: usize,
        batch_end: usize,
        active_limit: usize,
        require_concurrency_safe: bool,
        current_session_id: &Option<SessionId>,
        current_turn_id: &Option<String>,
        provider_mcp_routes: &Arc<BTreeMap<String, McpToolRoute>>,
        tool_boundary_control: Option<&ToolBoundaryControl>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
        executions: &mut [Option<ExecutedToolUse>],
        failed_file_write_paths: Arc<Mutex<BTreeSet<std::path::PathBuf>>>,
    ) -> anyhow::Result<ToolBatchOutcome> {
        let batch = &all_tool_uses[batch_start..batch_end];
        let (progress_tx, mut progress_rx) = mpsc::unbounded_channel();
        let mut running = FuturesUnordered::new();
        let mut next_to_start = 0usize;
        let mut stop_reason = None;
        let mut pending_skipped = false;
        let mut hard_cancel_deadline = None::<Instant>;
        let mut terminal_tool_use_ids = HashSet::new();
        // hard-cancel 的 100ms grace 结束后会 drop 仍未协作收束的 future；在 drop 前保留
        // 已 Started 调用的身份，确保每个 tool_use 仍有且仅有一个 terminal event。
        let mut in_flight_tool_uses = BTreeMap::<usize, CanonicalToolUse>::new();
        // durable recorder 失败后不能直接退出并 drop 已 Started 的 future；必须先把它们全部收束。
        let mut recorder_error = None;
        let cancellation = tool_boundary_control.map(ToolBoundaryControl::cancellation_token);

        loop {
            if stop_reason.is_some()
                && hard_cancel_deadline.is_none()
                && tool_boundary_control.is_some_and(ToolBoundaryControl::is_explicit_cancel)
            {
                hard_cancel_deadline = Some(Instant::now() + Duration::from_millis(100));
            }
            if stop_reason.is_none() && recorder_error.is_none() {
                while running.len() < active_limit && next_to_start < batch.len() {
                    if tool_boundary_is_cancelled(tool_boundary_control) {
                        stop_reason = Some(tool_boundary_skip_reason_value(tool_boundary_control));
                        if tool_boundary_control
                            .is_some_and(ToolBoundaryControl::is_explicit_cancel)
                        {
                            hard_cancel_deadline =
                                Some(Instant::now() + Duration::from_millis(100));
                        }
                        break;
                    }
                    let source_index = batch_start.saturating_add(next_to_start);
                    let tool_use = batch[next_to_start].clone();
                    #[cfg(test)]
                    if let Some(hook) = &self.before_tool_dispatch_reservation {
                        hook.before_try_reserve_dispatch().await;
                    }
                    if let Some(tool_boundary_control) = tool_boundary_control {
                        if let Err(reason) = tool_boundary_control.try_reserve_dispatch() {
                            stop_reason = Some(reason);
                            if tool_boundary_control.is_explicit_cancel() {
                                hard_cancel_deadline =
                                    Some(Instant::now() + Duration::from_millis(100));
                            }
                            break;
                        }
                    }

                    // reservation 成功后先持久化 Started；持久化失败时不能执行一个 recovery
                    // 无法识别的调用。此前已成功持久化的 Started task 仍会被 drain 到真实终态。
                    let (input_preview, input_truncated) = tool_input_preview(
                        &tool_use.name,
                        &tool_use.input,
                        self.tool_input_journal_preview_chars,
                    );
                    let started_event = SessionTurnEvent::ToolCallStarted {
                        id: tool_use.id.clone(),
                        name: tool_use.name.clone(),
                        summary: tool_started_summary(&tool_use.name, &tool_use.input),
                        input_preview,
                        input_truncated,
                    };
                    match record_durable_event_while_tool_batch_active(
                        durable_recorder,
                        started_event.clone(),
                        cancellation.as_ref(),
                        hard_cancel_deadline,
                    )
                    .await
                    {
                        Ok(DurableRecordOutcome::Recorded { deadline: None }) => {}
                        Ok(DurableRecordOutcome::Recorded {
                            deadline: Some(deadline),
                        }) => {
                            // Started 的 durable write 与 Esc/Ctrl-C 竞争时，不能在
                            // journal 已确认后仍启动外部工具。它尚未真正 dispatch，故 TUI
                            // 只能看到 Skipped；journal 中的预写 Started 由该 terminal event 收束。
                            hard_cancel_deadline = hard_cancel_deadline.or(Some(deadline));
                            let undispatched =
                                &all_tool_uses[batch_start.saturating_add(next_to_start)..];
                            let emitted = emit_skipped_tool_calls_until(
                                undispatched,
                                emit,
                                durable_recorder,
                                self.tool_input_journal_preview_chars,
                                tool_boundary_skip_reason_value(tool_boundary_control),
                                hard_cancel_deadline,
                            )
                            .await?;
                            emit_skipped_tool_calls_without_recording(
                                &undispatched[emitted..],
                                emit,
                                self.tool_input_journal_preview_chars,
                                tool_boundary_skip_reason_value(tool_boundary_control),
                            );
                            return Ok(ToolBatchOutcome::Interrupted);
                        }
                        Ok(DurableRecordOutcome::Abandoned { deadline }) => {
                            stop_reason =
                                Some(tool_boundary_skip_reason_value(tool_boundary_control));
                            hard_cancel_deadline = hard_cancel_deadline
                                .or(deadline)
                                .or(Some(Instant::now() + Duration::from_millis(100)));
                            break;
                        }
                        Err(error) => {
                            recorder_error = Some(error);
                            break;
                        }
                    }
                    emit(started_event);
                    let tool_context = ToolDispatchContext {
                        current_session_id: current_session_id.clone(),
                        current_turn_id: current_turn_id.clone(),
                        tool_use_id: Some(tool_use.id.clone()),
                        progress_tx: Some(progress_tx.clone()),
                        cancellation: cancellation.clone(),
                        provider_mcp_routes: Some(Arc::clone(provider_mcp_routes)),
                        failed_file_write_paths: Some(Arc::clone(&failed_file_write_paths)),
                    };
                    let tools = Arc::clone(&self.tools);
                    let execution_name = tool_use.name.clone();
                    let execution_input = tool_use.input.clone();
                    let dispatch_rejection = duplicate_write_stdin
                        .get(source_index)
                        .copied()
                        .unwrap_or(false)
                        .then(|| {
                            "write_stdin was already called for this process in the current assistant response; retry after the next provider response".to_string()
                        });
                    let in_flight_tool_use = tool_use.clone();
                    running.push(async move {
                        let executed = execute_tool_use(
                            tools.as_ref(),
                            &execution_name,
                            execution_input,
                            tool_context,
                            require_concurrency_safe,
                            dispatch_rejection,
                        )
                        .await;
                        ToolBatchCompletion {
                            source_index,
                            tool_use,
                            executed,
                        }
                    });
                    in_flight_tool_uses.insert(source_index, in_flight_tool_use);
                    next_to_start = next_to_start.saturating_add(1);
                }
            }

            // 取消与 completion 同时 ready 时，先结算已完成调用；只有当前没有 ready
            // completion 才把尚未派发的调用收束为 Skipped。
            let ready_completion =
                if stop_reason.is_some() && !pending_skipped && !running.is_empty() {
                    running.next().now_or_never().flatten()
                } else {
                    None
                };

            if let Some(reason) = stop_reason {
                if ready_completion.is_none() && !pending_skipped {
                    let undispatched = &all_tool_uses[batch_start.saturating_add(next_to_start)..];
                    let skipped_within_grace = emit_skipped_tool_calls_until(
                        undispatched,
                        emit,
                        durable_recorder,
                        self.tool_input_journal_preview_chars,
                        reason,
                        hard_cancel_deadline,
                    )
                    .await;
                    match skipped_within_grace {
                        Ok(emitted) if emitted == undispatched.len() => {}
                        Ok(emitted) => {
                            hard_cancel_deadline = hard_cancel_deadline
                                .or(Some(Instant::now() + Duration::from_millis(100)));
                            // 已经发出 Completed，但它的 durable 写入被显式取消截断。
                            // 当前调用仍保留 Completed；尚未派发的调用必须按 D20 向 TUI
                            // 明确收束为 Skipped，不能随着 future abort 静默消失。
                            emit_skipped_tool_calls_without_recording(
                                &undispatched[emitted..],
                                emit,
                                self.tool_input_journal_preview_chars,
                                tool_boundary_skip_reason_value(tool_boundary_control),
                            );
                            running.clear();
                            emit_forced_abort_interrupts(
                                &mut in_flight_tool_uses,
                                &mut terminal_tool_use_ids,
                                self.tools.as_ref(),
                                current_session_id,
                                current_turn_id,
                                emit,
                                durable_recorder,
                                hard_cancel_deadline,
                            )
                            .await?;
                            return Ok(ToolBatchOutcome::Interrupted);
                        }
                        Err(error) => {
                            if recorder_error.is_none() {
                                recorder_error = Some(error);
                            }
                        }
                    }
                    pending_skipped = true;
                }
                if ready_completion.is_none() && running.is_empty() {
                    for _ in 0..MAX_PROGRESS_EVENTS_PER_DRAIN {
                        let Ok(progress) = progress_rx.try_recv() else {
                            break;
                        };
                        emit_tool_progress_if_active(progress, &terminal_tool_use_ids, emit);
                    }
                    if let Some(error) = recorder_error {
                        return Err(error);
                    }
                    return Ok(ToolBatchOutcome::Interrupted);
                }
                if hard_cancel_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    // 强制 abort 表示不再等待/轮询未协作收束的 future；drop 由各工具自身的
                    // cancellation/drop guard 负责，已登记 ProcessManager 的后台进程不受影响。
                    running.clear();
                    emit_forced_abort_interrupts(
                        &mut in_flight_tool_uses,
                        &mut terminal_tool_use_ids,
                        self.tools.as_ref(),
                        current_session_id,
                        current_turn_id,
                        emit,
                        durable_recorder,
                        hard_cancel_deadline,
                    )
                    .await?;
                    return Ok(ToolBatchOutcome::Interrupted);
                }
            } else if running.is_empty() {
                if let Some(error) = recorder_error {
                    return Err(error);
                }
                return Ok(ToolBatchOutcome::Completed);
            }

            let completion = if let Some(completion) = ready_completion {
                completion
            } else if let Some(cancellation) = cancellation.as_ref() {
                if let Some(deadline) = hard_cancel_deadline {
                    tokio::select! {
                        biased;
                        Some(completion) = running.next() => completion,
                        _ = tokio::time::sleep_until(deadline) => {
                            running.clear();
                            emit_forced_abort_interrupts(
                                &mut in_flight_tool_uses,
                                &mut terminal_tool_use_ids,
                                self.tools.as_ref(),
                                current_session_id,
                                current_turn_id,
                                emit,
                                durable_recorder,
                                Some(deadline),
                            ).await?;
                            return Ok(ToolBatchOutcome::Interrupted);
                        }
                        Some(progress) = progress_rx.recv() => {
                            emit_tool_progress_if_active(progress, &terminal_tool_use_ids, emit);
                            continue;
                        }
                    }
                } else {
                    tokio::select! {
                        biased;
                        Some(completion) = running.next() => completion,
                        _ = cancellation.cancelled(), if stop_reason.is_none() => {
                            stop_reason = Some(tool_boundary_skip_reason_value(tool_boundary_control));
                            continue;
                        }
                        Some(progress) = progress_rx.recv() => {
                            emit_tool_progress_if_active(progress, &terminal_tool_use_ids, emit);
                            continue;
                        }
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    Some(completion) = running.next() => completion,
                    Some(progress) = progress_rx.recv() => {
                        emit_tool_progress_if_active(progress, &terminal_tool_use_ids, emit);
                        continue;
                    }
                }
            };

            // 尽量先把 task 已发送的进度落到终态之前，保持既有 progress → completed 顺序。
            for _ in 0..MAX_PROGRESS_EVENTS_PER_DRAIN {
                let Ok(progress) = progress_rx.try_recv() else {
                    break;
                };
                emit_tool_progress_if_active(progress, &terminal_tool_use_ids, emit);
            }
            terminal_tool_use_ids.insert(completion.tool_use.id.clone());
            in_flight_tool_uses.remove(&completion.source_index);
            match completion.executed {
                Ok(executed) => {
                    let (output_preview, output_truncated) = tool_journal_output_preview(
                        &executed.output_preview,
                        self.tool_output_journal_preview_chars,
                    );
                    let completed_event = SessionTurnEvent::ToolCallCompleted {
                        id: completion.tool_use.id.clone(),
                        summary: tool_completed_summary(
                            &completion.tool_use.name,
                            executed.outcome,
                            &executed.output_preview,
                        ),
                        outcome: executed.outcome,
                        output_preview,
                        output_truncated,
                        file_change: executed.file_change.clone(),
                    };
                    emit(completed_event.clone());
                    match record_durable_event_while_tool_batch_active(
                        durable_recorder,
                        completed_event,
                        cancellation.as_ref(),
                        hard_cancel_deadline,
                    )
                    .await
                    {
                        Ok(DurableRecordOutcome::Recorded { deadline }) => {
                            hard_cancel_deadline = hard_cancel_deadline.or(deadline);
                        }
                        Ok(DurableRecordOutcome::Abandoned { deadline }) => {
                            hard_cancel_deadline = hard_cancel_deadline
                                .or(deadline)
                                .or(Some(Instant::now() + Duration::from_millis(100)));
                            // 当前调用已经有唯一的 Completed 终态；余下尚未派发调用
                            // 仍须可见地标成 Skipped。
                            emit_skipped_tool_calls_without_recording(
                                &all_tool_uses[batch_start.saturating_add(next_to_start)..],
                                emit,
                                self.tool_input_journal_preview_chars,
                                tool_boundary_skip_reason_value(tool_boundary_control),
                            );
                            running.clear();
                            emit_forced_abort_interrupts(
                                &mut in_flight_tool_uses,
                                &mut terminal_tool_use_ids,
                                self.tools.as_ref(),
                                current_session_id,
                                current_turn_id,
                                emit,
                                durable_recorder,
                                hard_cancel_deadline,
                            )
                            .await?;
                            return Ok(ToolBatchOutcome::Interrupted);
                        }
                        Err(error) => {
                            if recorder_error.is_none() {
                                recorder_error = Some(error);
                            }
                        }
                    }
                    if let Some(slot) = executions.get_mut(completion.source_index) {
                        *slot = Some(executed);
                    } else if recorder_error.is_none() {
                        recorder_error =
                            Some(anyhow::anyhow!("并发工具调度收到了无效 source index"));
                    }
                }
                Err(ToolUseInterrupted {
                    continuing_process_id,
                }) => {
                    let summary = match continuing_process_id {
                        Some(process_id) => {
                            format!("Interrupted · process {process_id} continues in background")
                        }
                        None => format!("tool {} interrupted", completion.tool_use.name),
                    };
                    let interrupted_event = SessionTurnEvent::ToolCallInterrupted {
                        id: completion.tool_use.id,
                        summary,
                    };
                    emit(interrupted_event.clone());
                    match record_durable_event_while_tool_batch_active(
                        durable_recorder,
                        interrupted_event,
                        cancellation.as_ref(),
                        hard_cancel_deadline,
                    )
                    .await
                    {
                        Ok(DurableRecordOutcome::Recorded { deadline }) => {
                            hard_cancel_deadline = hard_cancel_deadline.or(deadline);
                        }
                        Ok(DurableRecordOutcome::Abandoned { deadline }) => {
                            hard_cancel_deadline = hard_cancel_deadline
                                .or(deadline)
                                .or(Some(Instant::now() + Duration::from_millis(100)));
                            // 当前调用已经有唯一的 Interrupted 终态；余下尚未派发调用
                            // 仍须可见地标成 Skipped。
                            emit_skipped_tool_calls_without_recording(
                                &all_tool_uses[batch_start.saturating_add(next_to_start)..],
                                emit,
                                self.tool_input_journal_preview_chars,
                                tool_boundary_skip_reason_value(tool_boundary_control),
                            );
                            running.clear();
                            emit_forced_abort_interrupts(
                                &mut in_flight_tool_uses,
                                &mut terminal_tool_use_ids,
                                self.tools.as_ref(),
                                current_session_id,
                                current_turn_id,
                                emit,
                                durable_recorder,
                                hard_cancel_deadline,
                            )
                            .await?;
                            return Ok(ToolBatchOutcome::Interrupted);
                        }
                        Err(error) => {
                            if recorder_error.is_none() {
                                recorder_error = Some(error);
                            }
                        }
                    }
                    stop_reason = Some(tool_boundary_skip_reason_value(tool_boundary_control));
                }
            }
            // 完成事件先行；若它与取消竞争，保留 Completed，再停止尚未派发的调用。
            if stop_reason.is_none() && tool_boundary_is_cancelled(tool_boundary_control) {
                stop_reason = Some(tool_boundary_skip_reason_value(tool_boundary_control));
                if tool_boundary_control.is_some_and(ToolBoundaryControl::is_explicit_cancel) {
                    hard_cancel_deadline = Some(Instant::now() + Duration::from_millis(100));
                }
            }
        }
    }

    pub async fn run_session_turn(
        &self,
        request: SessionTurnRequest,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> anyhow::Result<SessionTurn> {
        self.run_session_turn_with_tool_boundary_control(request, emit, None)
            .await
    }

    pub(crate) async fn run_session_turn_with_tool_boundary_control(
        &self,
        request: SessionTurnRequest,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        tool_boundary_control: Option<ToolBoundaryControl>,
    ) -> anyhow::Result<SessionTurn> {
        self.run_session_turn_with_tool_boundary_control_and_recorder(
            request,
            emit,
            tool_boundary_control,
            None,
        )
        .await
    }

    pub(crate) async fn run_session_turn_with_tool_boundary_control_and_recorder(
        &self,
        request: SessionTurnRequest,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        tool_boundary_control: Option<ToolBoundaryControl>,
        durable_recorder: Option<&mut dyn SessionTurnEventRecorder>,
    ) -> anyhow::Result<SessionTurn> {
        self.run_session_turn_with_hooks(
            request,
            emit,
            tool_boundary_control,
            durable_recorder,
            None,
        )
        .await
    }

    pub(crate) async fn run_session_turn_with_hooks(
        &self,
        request: SessionTurnRequest,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        tool_boundary_control: Option<ToolBoundaryControl>,
        durable_recorder: Option<&mut dyn SessionTurnEventRecorder>,
        preflight: Option<&mut dyn SessionTurnPreflight>,
    ) -> anyhow::Result<SessionTurn> {
        self.run_session_turn_with_context_hooks(
            request,
            Vec::new(),
            emit,
            tool_boundary_control,
            SessionTurnHooks::new(durable_recorder, None, preflight),
        )
        .await
    }

    pub(crate) async fn run_session_turn_with_context_hooks(
        &self,
        request: SessionTurnRequest,
        recovered_model_context: Vec<CompletedSessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        tool_boundary_control: Option<ToolBoundaryControl>,
        hooks: SessionTurnHooks<'_, '_, '_>,
    ) -> anyhow::Result<SessionTurn> {
        let fallback_root = crate::api::ProviderRuntimeFallbackScope::new_root();
        self.run_session_turn_with_context_and_runtime_chain_hooks(
            request,
            recovered_model_context,
            ProviderRuntimeChainId::new(),
            fallback_root.new_child(),
            emit,
            tool_boundary_control,
            hooks,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn run_session_turn_with_runtime_chain_hooks(
        &self,
        request: SessionTurnRequest,
        runtime_chain_id: ProviderRuntimeChainId,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        tool_boundary_control: Option<ToolBoundaryControl>,
        durable_recorder: Option<&mut dyn SessionTurnEventRecorder>,
        preflight: Option<&mut dyn SessionTurnPreflight>,
    ) -> anyhow::Result<SessionTurn> {
        let fallback_root = crate::api::ProviderRuntimeFallbackScope::new_root();
        self.run_session_turn_with_context_and_runtime_chain_hooks(
            request,
            Vec::new(),
            runtime_chain_id,
            fallback_root.new_child(),
            emit,
            tool_boundary_control,
            SessionTurnHooks::new(durable_recorder, None, preflight),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "turn runtime 需显式携带 continuation chain、fallback scope 与 hooks"
    )]
    pub(crate) async fn run_session_turn_with_context_and_runtime_chain_hooks(
        &self,
        request: SessionTurnRequest,
        recovered_model_context: Vec<CompletedSessionTurnMessage>,
        runtime_chain_id: ProviderRuntimeChainId,
        runtime_fallback_scope: crate::api::ProviderRuntimeFallbackScope,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        tool_boundary_control: Option<ToolBoundaryControl>,
        hooks: SessionTurnHooks<'_, '_, '_>,
    ) -> anyhow::Result<SessionTurn> {
        let rollback_context = ToolDispatchContext {
            current_session_id: request.current_session_id.clone(),
            current_turn_id: request.current_turn_id.clone(),
            ..ToolDispatchContext::default()
        };
        let result = self
            .run_session_turn_with_hooks_inner(
                request,
                recovered_model_context,
                emit,
                tool_boundary_control,
                hooks,
                runtime_chain_id,
                runtime_fallback_scope,
            )
            .await;
        if result.is_err() {
            self.provider.discard_runtime_chain(runtime_chain_id).await;
            self.tools
                .rollback_uncommitted_process_deliveries_for_context(&rollback_context)
                .await;
        }
        result
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "turn runtime 需显式携带 continuation chain、fallback scope 与 hooks"
    )]
    async fn run_session_turn_with_hooks_inner(
        &self,
        request: SessionTurnRequest,
        recovered_model_context: Vec<CompletedSessionTurnMessage>,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        tool_boundary_control: Option<ToolBoundaryControl>,
        hooks: SessionTurnHooks<'_, '_, '_>,
        runtime_chain_id: ProviderRuntimeChainId,
        runtime_fallback_scope: crate::api::ProviderRuntimeFallbackScope,
    ) -> anyhow::Result<SessionTurn> {
        let SessionTurnHooks {
            mut durable_recorder,
            mut context_appender,
            mut preflight,
        } = hooks;
        if self.max_tool_loop_turns == Some(0) {
            anyhow::bail!("run_session_turn max_turns 必须大于 0");
        }

        let SessionTurnRequest {
            current_session_id,
            current_turn_id,
            mut system_prompt,
            history,
            user_text,
            user_attachments,
            skill_instructions,
        } = request;
        let mut provider_messages = history;
        let frozen_provider_history_prefix_len = preflight
            .as_deref()
            .map(SessionTurnPreflight::frozen_provider_history_prefix_len)
            .unwrap_or(0);
        let mut frozen_provider_prefix = FrozenProviderRequestPrefix::new(
            &provider_messages,
            frozen_provider_history_prefix_len,
        )?;
        let mut committed = Vec::new();
        let (user_message, text_attachment_reads, attachment_warnings) = session_user_message(
            user_text,
            user_attachments,
            skill_instructions,
            &self.attachment_limits,
        )
        .await?;
        for message in attachment_warnings {
            emit(SessionTurnEvent::Warning { message });
        }
        self.materialize_recovered_model_context(
            &mut provider_messages,
            &mut committed,
            &mut durable_recorder,
            recovered_model_context,
        )
        .await?;
        let initial_context = self
            .observe_model_context(&provider_messages, &mut context_appender)
            .await?;
        self.append_frozen_model_context(
            &mut provider_messages,
            &mut committed,
            &mut durable_recorder,
            initial_context,
        )
        .await?;
        let turn_started_at = self.now();
        provider_messages.push(user_message.clone());
        let completed_user = CompletedSessionTurnMessage::new(user_message, turn_started_at);
        if let Some(recorder) = durable_recorder.as_deref_mut() {
            recorder.record_completed_message(&completed_user).await?;
        }
        committed.push(completed_user);
        let mut seen_tool_use_ids = HashSet::new();
        let mut pending_process_deliveries = Vec::<PendingProcessDelivery>::new();
        let mut context_continuation = ContextWindowContinuation::default();
        let mut context_window_recoveries = 0usize;

        let mut turn_idx = 0usize;
        let mut provider_request_idx = 0usize;
        loop {
            if turn_idx > 0 && tool_boundary_is_cancelled(tool_boundary_control.as_ref()) {
                return Err(SessionTurnInterrupted.into());
            }
            if let Some(preflight) = preflight.as_mut() {
                preflight
                    .before_context_observation(&mut system_prompt, &mut provider_messages, emit)
                    .await?;
            }
            let frozen_context = self
                .observe_model_context(&provider_messages, &mut context_appender)
                .await?;
            let changed_context_start = provider_messages.len();
            append_new_model_context_messages(
                &mut provider_messages,
                frozen_context.iter().map(|completed| &completed.message),
            )?;
            let history_replacement_expected = preflight.as_deref().is_some_and(|preflight| {
                preflight.history_replacement_expected(&system_prompt, &provider_messages)
            });
            let context_rebaseline_start = if history_replacement_expected {
                provider_messages.truncate(changed_context_start);
                let start = changed_context_start;
                provider_messages.extend(
                    frozen_context
                        .iter()
                        .map(|completed| completed.message.clone()),
                );
                Some(start)
            } else {
                provider_messages.truncate(changed_context_start);
                self.append_frozen_model_context(
                    &mut provider_messages,
                    &mut committed,
                    &mut durable_recorder,
                    frozen_context.iter().cloned(),
                )
                .await?;
                None
            };
            if let Some(preflight) = preflight.as_mut() {
                preflight
                    .before_provider_request(&mut system_prompt, &mut provider_messages, emit)
                    .await?;
            }
            let history_replaced = preflight
                .as_deref_mut()
                .is_some_and(|preflight| preflight.take_history_replaced_since_last_check());
            match (context_rebaseline_start, history_replaced) {
                (Some(_), true) => {
                    let frozen_messages = frozen_context
                        .iter()
                        .map(|completed| completed.message.clone())
                        .collect::<Vec<_>>();
                    if !provider_messages.ends_with(&frozen_messages) {
                        anyhow::bail!("compaction 未保留冻结的 context baseline");
                    }
                    self.persist_frozen_model_context(
                        &mut committed,
                        &mut durable_recorder,
                        frozen_context,
                    )
                    .await?;
                }
                (Some(start), false) => {
                    provider_messages.truncate(start);
                    self.append_frozen_model_context(
                        &mut provider_messages,
                        &mut committed,
                        &mut durable_recorder,
                        frozen_context,
                    )
                    .await?;
                }
                (None, true) => {
                    anyhow::bail!("preflight 未声明压缩预期却替换了 provider history");
                }
                (None, false) => {}
            }
            if history_replaced {
                // compaction 是允许替换旧 history 的显式缓存断点；其首个新请求重新建立边界。
                frozen_provider_prefix.clear();
                // 同时废弃 connection-local previous_response_id；健康 socket 与
                // runtime-chain sticky HTTP 状态仍可保留，下一请求必须发送完整新窗口。
                self.provider.discard_runtime_chain(runtime_chain_id).await;
            }
            // steering、context 观察和 compaction 都可能让出执行权；若此时已收到
            // steer 或显式取消，就不能再发起一个新的 provider request。
            if tool_boundary_is_cancelled(tool_boundary_control.as_ref()) {
                return Err(SessionTurnInterrupted.into());
            }
            if provider_request_idx == 0 {
                let attachment_context = ToolDispatchContext {
                    current_session_id: current_session_id.clone(),
                    current_turn_id: current_turn_id.clone(),
                    ..ToolDispatchContext::default()
                };
                for attachment in text_attachment_reads
                    .iter()
                    .filter(|attachment| attachment.is_visible_in(&provider_messages))
                {
                    self.tools
                        .record_text_attachment_read(
                            &attachment_context,
                            attachment.canonical_path.clone(),
                            &attachment.content,
                        )
                        .await;
                }
            }
            provider_request_idx = provider_request_idx
                .checked_add(1)
                .context("run_session_turn provider 请求轮数溢出")?;
            let mut process_deliveries_for_request = Vec::new();
            let mut process_deliveries_not_in_request = Vec::new();
            for pending in &pending_process_deliveries {
                if provider_request_contains_process_tool_result(&provider_messages, pending) {
                    process_deliveries_for_request.push(pending.receipt.clone());
                } else {
                    process_deliveries_not_in_request.push(pending.receipt.clone());
                }
            }
            // compaction/runtime projection 可以合法替换或移除大 tool_result。被替换的
            // 原始输出并未交给 provider，必须立即释放 pending cursor，等待后续显式重读。
            self.tools
                .rollback_process_deliveries(&process_deliveries_not_in_request)
                .await;
            self.tools
                .begin_process_deliveries(&process_deliveries_for_request)
                .await;
            let request_messages = frozen_provider_prefix.project(&provider_messages)?;
            if let Some(preflight) = preflight.as_mut() {
                match time::timeout(
                    PROVIDER_WAL_PREPARATION_TIMEOUT,
                    preflight.provider_request_ready(&request_messages, committed.len()),
                )
                .await
                {
                    Ok(result) => result?,
                    Err(_) => {
                        return Err(ProviderRequestPreparationFailure::new(
                            "Provider 请求状态保存超时（10 秒）",
                        )
                        .into());
                    }
                }
            }
            let mut latest_provider_context_usage = None;
            let provider_call = {
                let mut request_progress = ProviderRequestProgress::new(
                    request_messages.clone(),
                    preflight.take(),
                    committed.len(),
                );
                let mut provider_emit = |event| {
                    if let SessionTurnEvent::ContextUsageUpdated { usage } = &event {
                        if usage.source == crate::api::ContextUsageSource::Provider {
                            latest_provider_context_usage = Some(*usage);
                        }
                    }
                    emit(event);
                };
                let provider_interrupt = tool_boundary_control
                    .as_ref()
                    .map(ToolBoundaryControl::cancellation_token);
                let provider_recovery_interrupt = tool_boundary_control
                    .as_ref()
                    .map(ToolBoundaryControl::recovery_cancellation_token);
                let result = self
                    .call_provider(
                        &system_prompt,
                        &request_messages,
                        &context_continuation,
                        &mut provider_emit,
                        &mut durable_recorder,
                        &seen_tool_use_ids,
                        provider_interrupt.as_ref(),
                        provider_recovery_interrupt.as_ref(),
                        runtime_chain_id,
                        &runtime_fallback_scope,
                        &mut request_progress,
                    )
                    .await;
                preflight = request_progress.take_preflight();
                result?
            };
            let ProviderCallOutcome {
                response: provider_response,
                request_messages: successful_request_messages,
                provider_assistant_message,
                recovered_with_non_streaming,
                provider_mcp_routes,
            } = provider_call;
            if recovered_with_non_streaming {
                // HTTP replacement 不属于旧 WebSocket connection-local history；即使
                // strict prefix 会在下轮拒绝它，也应在 fallback 成功后立即废弃旧链。
                self.provider.discard_runtime_chain(runtime_chain_id).await;
            }
            let stop = provider_response.stop;
            let context_window_recovery_marker = if stop == ProviderStop::ContextWindowExceeded {
                let continuation_suffix = successful_request_messages
                    .strip_prefix(request_messages.as_slice())
                    .context("context-window continuation 未保留本轮 Provider 请求前缀")?;
                Some(
                    continuation_suffix
                        .first()
                        .cloned()
                        .unwrap_or_else(|| provider_assistant_message.clone()),
                )
            } else {
                None
            };
            if successful_request_messages != request_messages {
                // adapter 已经完成一次或多次 max-token continuation。
                // raw history 仍要保留 compactor 的 active boundary；只把 adapter
                // 新增的 replay suffix 接到尾部。真实发送的规范化 history 由
                // frozen_provider_prefix 单独保存，不能用 wire vector 覆盖 raw vector。
                append_adapter_continuation_suffix_to_raw_history(
                    &mut provider_messages,
                    &request_messages,
                    &successful_request_messages,
                )?;
            }
            // Provider 已接受这份精确请求；后续 tool loop 只能在其后追加并规范化 suffix。
            frozen_provider_prefix.advance(&provider_messages, successful_request_messages);
            let assistant_message = provider_response.assistant_message;
            validate_assistant_message(&assistant_message)?;
            let tool_uses = collect_tool_uses(&assistant_message)?;
            validate_provider_response_terminal_semantics(stop, &tool_uses)?;
            if stop == ProviderStop::ContextWindowExceeded {
                if context_window_recoveries >= MAX_CONTEXT_WINDOW_RECOVERIES {
                    anyhow::bail!(
                        "模型上下文已满；自动压缩并续写 {MAX_CONTEXT_WINDOW_RECOVERIES} 次后仍未完成。请简化任务或新建会话重试。"
                    );
                }
                let Some(preflight) = preflight.as_mut() else {
                    anyhow::bail!("模型上下文已满，但当前 ACN 调用链未接入自动恢复");
                };
                preflight.request_context_window_recovery(
                    context_window_recovery_marker
                        .as_ref()
                        .context("context-window response 缺少 recovery marker")?,
                )?;
                context_window_recoveries = context_window_recoveries.saturating_add(1);
            }
            self.tools
                .commit_process_deliveries(&process_deliveries_for_request)
                .await;
            pending_process_deliveries.clear();
            if let Some(preflight) = preflight.as_mut() {
                preflight.after_provider_response_success().await?;
            }
            if let Some(context_appender) = context_appender.as_mut() {
                context_appender.after_provider_response_success().await?;
            }
            for tool_use in &tool_uses {
                if !seen_tool_use_ids.insert(tool_use.id.clone()) {
                    anyhow::bail!(
                        "provider 在同一 session turn 内重复 tool_use id: {}",
                        tool_use.id
                    );
                }
            }
            let assistant_text = assistant_message_text(&assistant_message);
            if stop != ProviderStop::ContextWindowExceeded
                && !recovered_with_non_streaming
                && !assistant_text.trim().is_empty()
            {
                let completed_text =
                    context_continuation.fallback_replacement_text(&assistant_text);
                record_durable_event(
                    &mut durable_recorder,
                    SessionTurnEvent::AssistantMessageCompleted {
                        text: completed_text,
                    },
                )
                .await?;
            }

            if tool_boundary_is_cancelled(tool_boundary_control.as_ref()) {
                if !tool_uses.is_empty() {
                    let deadline = tool_boundary_control
                        .as_ref()
                        .filter(|control| control.is_explicit_cancel())
                        .map(|_| Instant::now() + Duration::from_millis(100));
                    let emitted = emit_skipped_tool_calls_until(
                        &tool_uses,
                        emit,
                        &mut durable_recorder,
                        self.tool_input_journal_preview_chars,
                        tool_boundary_skip_reason_value(tool_boundary_control.as_ref()),
                        deadline,
                    )
                    .await?;
                    if emitted < tool_uses.len() {
                        emit_skipped_tool_calls_without_recording(
                            &tool_uses[emitted..],
                            emit,
                            self.tool_input_journal_preview_chars,
                            tool_boundary_skip_reason_value(tool_boundary_control.as_ref()),
                        );
                    }
                }
                return Err(SessionTurnInterrupted.into());
            }

            if stop == ProviderStop::ContextWindowExceeded && tool_uses.is_empty() {
                context_continuation.absorb_partial(&assistant_message)?;
                provider_messages.push(provider_assistant_message);
                if let Some(preflight) = preflight.as_mut() {
                    if let Some(usage) = latest_provider_context_usage {
                        preflight.observe_provider_context_usage(provider_messages.len(), usage);
                    } else {
                        preflight.clear_provider_context_usage();
                    }
                }
                provider_messages.push(SessionTurnMessage::user_text(CONTINUATION_TRIGGER));
                context_continuation.push_internal_continuation();
                continue;
            }

            let mut canonical_assistant_message = assistant_message;
            if context_continuation.has_pending() {
                std::mem::take(&mut context_continuation)
                    .merge_into(&mut canonical_assistant_message)?;
            }
            provider_messages.push(provider_assistant_message);
            if let Some(preflight) = preflight.as_mut() {
                if let Some(usage) = latest_provider_context_usage {
                    preflight.observe_provider_context_usage(provider_messages.len(), usage);
                } else {
                    preflight.clear_provider_context_usage();
                }
            }
            let completed_assistant =
                CompletedSessionTurnMessage::new(canonical_assistant_message, self.now());
            if let Some(recorder) = durable_recorder.as_deref_mut() {
                recorder
                    .record_completed_message(&completed_assistant)
                    .await?;
            }
            committed.push(completed_assistant);

            if tool_uses.is_empty() {
                let completed_provider_history =
                    frozen_provider_prefix.project(&provider_messages)?;
                if let Some(preflight) = preflight.as_mut() {
                    match time::timeout(
                        PROVIDER_WAL_PREPARATION_TIMEOUT,
                        preflight
                            .provider_response_ready(&completed_provider_history, committed.len()),
                    )
                    .await
                    {
                        Ok(result) => result?,
                        Err(_) => {
                            return Err(ProviderRequestPreparationFailure::new(
                                "Provider 响应状态保存超时（10 秒）",
                            )
                            .into());
                        }
                    }
                }
                return Ok(SessionTurn {
                    messages: committed,
                });
            }
            if let Some(max_turns) = self.max_tool_loop_turns {
                if turn_idx.saturating_add(1) == max_turns {
                    anyhow::bail!("run_session_turn 达到最大 tool 循环轮数: {max_turns}");
                }
            }

            let Some(executed_tool_uses) = self
                .execute_tool_uses_in_batches(
                    &tool_uses,
                    &provider_mcp_routes,
                    &current_session_id,
                    &current_turn_id,
                    tool_boundary_control.as_ref(),
                    emit,
                    &mut durable_recorder,
                )
                .await?
            else {
                return Err(SessionTurnInterrupted.into());
            };

            let mut tool_results = Vec::with_capacity(tool_uses.len());
            let mut canonical_tool_results = Vec::with_capacity(tool_uses.len());
            let mut media_blocks = Vec::new();
            let mut canonical_media_blocks = Vec::new();
            for (tool_use, executed) in tool_uses.iter().zip(executed_tool_uses) {
                if let Some(receipt) = executed.process_delivery_receipt.clone() {
                    pending_process_deliveries.push(PendingProcessDelivery {
                        receipt,
                        tool_use_id: tool_use.id.clone(),
                        tool_result_content: executed.content.clone(),
                    });
                }
                let tool_use_id = tool_use.id.clone();
                tool_results.push(SessionTurnContentBlock::ToolResult {
                    tool_use_id: tool_use_id.clone(),
                    content: executed.content,
                });
                canonical_tool_results.push(SessionTurnContentBlock::ToolResult {
                    tool_use_id,
                    content: executed.canonical_content,
                });
                canonical_media_blocks.extend(executed.media_blocks.clone());
                media_blocks.extend(executed.media_blocks);
            }

            if tool_boundary_is_cancelled(tool_boundary_control.as_ref()) {
                return Err(SessionTurnInterrupted.into());
            }
            // 媒体块（file_read 读到的图片 / PDF）跟在 tool_result 之后同一条 user
            // message 里：Anthropic 要求 tool_result 打头且 user/assistant 轮替，
            // OpenAI 适配器会把它拆成 tool message + 后续 user message。
            tool_results.extend(media_blocks);
            let tool_result_message = SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: tool_results,
            };
            canonical_tool_results.extend(canonical_media_blocks);
            let canonical_tool_result_message = SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: canonical_tool_results,
            };
            provider_messages.push(tool_result_message.clone());
            let completed_tool_result =
                CompletedSessionTurnMessage::new(canonical_tool_result_message, self.now());
            if let Some(recorder) = durable_recorder.as_deref_mut() {
                recorder
                    .record_completed_message(&completed_tool_result)
                    .await?;
            }
            committed.push(completed_tool_result);
            if tool_boundary_is_cancelled(tool_boundary_control.as_ref()) {
                return Err(SessionTurnInterrupted.into());
            }
            turn_idx = turn_idx
                .checked_add(1)
                .context("run_session_turn tool 循环轮数溢出")?;
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "provider 调用需同时携带 fallback 展示前缀、durable recorder 与中断边界"
    )]
    async fn call_provider(
        &self,
        system_prompt: &str,
        messages: &[SessionTurnMessage],
        context_continuation: &ContextWindowContinuation,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
        seen_tool_use_ids: &HashSet<String>,
        provider_interrupt: Option<&CancellationToken>,
        provider_recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        runtime_chain_id: ProviderRuntimeChainId,
        runtime_fallback_scope: &crate::api::ProviderRuntimeFallbackScope,
        request_progress: &mut ProviderRequestProgress<'_>,
    ) -> anyhow::Result<ProviderCallOutcome> {
        let (tool_definitions, provider_mcp_routes) = self.tools.definitions_with_mcp_routes();
        let provider_mcp_routes = Arc::new(provider_mcp_routes);
        let tools = tool_definitions
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        if self.provider.emit_preflight_context_estimate() {
            emit(SessionTurnEvent::ContextUsageUpdated {
                usage: estimate_provider_request_context_tokens(system_prompt, messages, &tools),
            });
        }
        let mut emitted_assistant_text = false;
        let mut streaming_emit = |event| match event {
            ProviderEvent::ContextUsageUpdated { usage } => {
                emit(SessionTurnEvent::ContextUsageUpdated { usage });
            }
            ProviderEvent::AssistantTextDelta { text } => {
                emitted_assistant_text |= !text.is_empty();
                emit(SessionTurnEvent::AssistantTextDelta { text });
            }
            ProviderEvent::AssistantMessageCompleted { text } => {
                emit(SessionTurnEvent::AssistantMessageCompleted {
                    text: context_continuation.fallback_replacement_text(&text),
                });
            }
        };

        let streaming_attempt_base = request_progress.latest_messages().to_vec();
        let request = ProviderRequest {
            system_prompt: system_prompt.to_string(),
            messages: streaming_attempt_base.clone(),
            tools: tools.clone(),
            max_tokens: self.max_tokens,
            stream: true,
            stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
            runtime_chain_id: Some(runtime_chain_id),
            runtime_fallback_scope: Some(runtime_fallback_scope.clone()),
            recovery_interrupt: provider_recovery_interrupt.cloned(),
            retry_count_override: None,
        };
        let streaming_result = self
            .send_provider_request_interruptible(
                request,
                &mut streaming_emit,
                provider_interrupt,
                request_progress,
            )
            .await;
        match streaming_result {
            Ok(response) => {
                let provider_assistant_message = provider_assistant_suffix_for_latest_request(
                    &response.assistant_message,
                    &streaming_attempt_base,
                    request_progress.latest_messages(),
                )?;
                Ok(ProviderCallOutcome {
                    response,
                    request_messages: request_progress.latest_messages().to_vec(),
                    provider_assistant_message,
                    recovered_with_non_streaming: false,
                    provider_mcp_routes: Arc::clone(&provider_mcp_routes),
                })
            }
            Err(_error)
                if provider_recovery_interrupt
                    .is_some_and(ProviderRecoveryInterrupt::is_cancelled) =>
            {
                Err(SessionTurnInterrupted.into())
            }
            Err(error)
                if error.downcast_ref::<ProviderTerminalFailure>().is_some()
                    || error.downcast_ref::<SessionTurnInterrupted>().is_some()
                    || error
                        .downcast_ref::<ProviderRequestPreparationFailure>()
                        .is_some() =>
            {
                Err(error)
            }
            Err(mut previous_error)
                if emitted_assistant_text
                    || previous_error
                        .downcast_ref::<ProviderStreamFailure>()
                        .is_some()
                    || previous_error
                        .downcast_ref::<ProviderNoConsumableOutput>()
                        .is_some() =>
            {
                for attempt in 1..=NON_STREAMING_FALLBACK_MAX_ATTEMPTS {
                    // 上一轮失败或 durable 写入期间可能收到 steer/cancel；此时不能
                    // 虚构下一次已开始，也不能为必定丢弃的 turn 继续计费。
                    if provider_recovery_interrupt
                        .is_some_and(ProviderRecoveryInterrupt::is_cancelled)
                    {
                        return Err(SessionTurnInterrupted.into());
                    }
                    let (previous_error_text, _) = truncate_chars(
                        &format!("{previous_error:#}"),
                        NON_STREAMING_FALLBACK_ERROR_MAX_CHARS,
                    );
                    let started_event = SessionTurnEvent::NonStreamingFallbackAttemptStarted {
                        attempt,
                        max_attempts: NON_STREAMING_FALLBACK_MAX_ATTEMPTS,
                        previous_error: previous_error_text,
                    };
                    record_durable_event(durable_recorder, started_event.clone()).await?;
                    if provider_recovery_interrupt
                        .is_some_and(ProviderRecoveryInterrupt::is_cancelled)
                    {
                        return Err(SessionTurnInterrupted.into());
                    }
                    emit(started_event);
                    self.wait_for_non_streaming_fallback(attempt, provider_recovery_interrupt)
                        .await?;

                    if provider_recovery_interrupt
                        .is_some_and(ProviderRecoveryInterrupt::is_cancelled)
                    {
                        return Err(SessionTurnInterrupted.into());
                    }

                    let fallback_attempt_base = request_progress.latest_messages().to_vec();
                    let fallback_request = ProviderRequest {
                        system_prompt: system_prompt.to_string(),
                        messages: fallback_attempt_base.clone(),
                        tools: tools.clone(),
                        max_tokens: self.max_tokens,
                        stream: false,
                        stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                        runtime_chain_id: None,
                        runtime_fallback_scope: None,
                        recovery_interrupt: provider_recovery_interrupt.cloned(),
                        // TUI 的 N/5 必须严格对应一次 provider-call attempt，禁止 adapter 再嵌套 retry。
                        retry_count_override: Some(0),
                    };
                    let mut fallback_emit = |event| match event {
                        ProviderEvent::ContextUsageUpdated { usage } => {
                            emit(SessionTurnEvent::ContextUsageUpdated { usage });
                        }
                        ProviderEvent::AssistantTextDelta { .. }
                        | ProviderEvent::AssistantMessageCompleted { .. } => {}
                    };
                    let fallback_result = self
                        .send_provider_request_interruptible(
                            fallback_request,
                            &mut fallback_emit,
                            provider_interrupt,
                            request_progress,
                        )
                        .await
                        .and_then(|response| {
                            validate_non_streaming_fallback_response(&response, seen_tool_use_ids)?;
                            Ok(response)
                        });
                    match fallback_result {
                        Ok(response) => {
                            let provider_assistant_message =
                                provider_assistant_suffix_for_latest_request(
                                    &response.assistant_message,
                                    &fallback_attempt_base,
                                    request_progress.latest_messages(),
                                )?;
                            let mut response = response;
                            merge_prior_attempt_continuation_text(
                                &mut response.assistant_message,
                                messages,
                                &fallback_attempt_base,
                            )?;
                            let replacement_text = context_continuation.fallback_replacement_text(
                                &assistant_message_text(&response.assistant_message),
                            );
                            let succeeded_event = SessionTurnEvent::NonStreamingFallbackSucceeded {
                                attempt,
                                max_attempts: NON_STREAMING_FALLBACK_MAX_ATTEMPTS,
                                text: replacement_text,
                            };
                            record_durable_event(durable_recorder, succeeded_event.clone()).await?;
                            emit(succeeded_event);
                            return Ok(ProviderCallOutcome {
                                response,
                                request_messages: request_progress.latest_messages().to_vec(),
                                provider_assistant_message,
                                recovered_with_non_streaming: true,
                                provider_mcp_routes: Arc::clone(&provider_mcp_routes),
                            });
                        }
                        Err(error)
                            if error.downcast_ref::<SessionTurnInterrupted>().is_some()
                                || error
                                    .downcast_ref::<ProviderRequestPreparationFailure>()
                                    .is_some() =>
                        {
                            return Err(error);
                        }
                        Err(error) if error.downcast_ref::<ProviderTerminalFailure>().is_some() => {
                            let (error_text, _) = truncate_chars(
                                &format!("{error:#}"),
                                NON_STREAMING_FALLBACK_ERROR_MAX_CHARS,
                            );
                            let failed_event =
                                SessionTurnEvent::NonStreamingFallbackAttemptFailed {
                                    attempt,
                                    max_attempts: NON_STREAMING_FALLBACK_MAX_ATTEMPTS,
                                    error: error_text,
                                };
                            record_durable_event(durable_recorder, failed_event.clone()).await?;
                            emit(failed_event);
                            return Err(error);
                        }
                        Err(error) => {
                            let (error_text, _) = truncate_chars(
                                &format!("{error:#}"),
                                NON_STREAMING_FALLBACK_ERROR_MAX_CHARS,
                            );
                            let failed_event =
                                SessionTurnEvent::NonStreamingFallbackAttemptFailed {
                                    attempt,
                                    max_attempts: NON_STREAMING_FALLBACK_MAX_ATTEMPTS,
                                    error: error_text,
                                };
                            record_durable_event(durable_recorder, failed_event.clone()).await?;
                            emit(failed_event);
                            previous_error = error;
                        }
                    }
                }
                Err(anyhow::anyhow!(
                    "non-streaming fallback exhausted after {}/{}: {previous_error:#}",
                    NON_STREAMING_FALLBACK_MAX_ATTEMPTS,
                    NON_STREAMING_FALLBACK_MAX_ATTEMPTS
                ))
            }
            Err(error) => Err(error),
        }
    }

    async fn send_provider_request_interruptible(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        provider_interrupt: Option<&CancellationToken>,
        request_progress: &mut ProviderRequestProgress<'_>,
    ) -> anyhow::Result<ProviderResponse> {
        let request_is_streaming = request.stream;
        let request_timeout = self.provider.request_timeout();
        request_progress.begin_provider_attempt();
        let write_ahead_phase = request_progress.write_ahead_phase();
        let provider_send =
            self.provider
                .send_with_request_observer(request, emit, request_progress);
        let provider_call = async {
            match request_timeout {
                Some(timeout) => match time::timeout(timeout, provider_send).await {
                    Ok(result) => result,
                    Err(_) if write_ahead_phase.load(Ordering::Acquire) => {
                        Err(ProviderRequestPreparationFailure::new(format!(
                            "Provider continuation WAL timeout after {}ms",
                            timeout.as_millis()
                        ))
                        .into())
                    }
                    Err(_) if request_is_streaming => Err(ProviderStreamFailure::new(format!(
                        "LLM provider streaming call timeout after {}ms",
                        timeout.as_millis()
                    ))
                    .into()),
                    Err(_) => Err(anyhow::anyhow!(
                        "LLM provider call timeout after {}ms",
                        timeout.as_millis()
                    )),
                },
                None => provider_send.await,
            }
        };
        tokio::pin!(provider_call);
        match provider_interrupt {
            Some(interrupt) if interrupt.is_cancelled() => Err(SessionTurnInterrupted.into()),
            Some(interrupt) => {
                tokio::select! {
                    biased;
                    response = &mut provider_call => response,
                    _ = interrupt.cancelled() => Err(SessionTurnInterrupted.into()),
                }
            }
            None => provider_call.await,
        }
    }

    async fn wait_for_non_streaming_fallback(
        &self,
        attempt: u32,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
    ) -> anyhow::Result<()> {
        let delay = non_streaming_fallback_delay(attempt);
        log::warn!(
            target: "api",
            "streaming response failed after visible output; falling back to non-streaming in {}ms ({}/{})",
            delay.as_millis(),
            attempt,
            NON_STREAMING_FALLBACK_MAX_ATTEMPTS
        );
        let sleep = time::sleep(delay);
        tokio::pin!(sleep);
        match recovery_interrupt {
            Some(interrupt) if interrupt.is_cancelled() => Err(SessionTurnInterrupted.into()),
            Some(interrupt) => {
                tokio::select! {
                    biased;
                    _ = interrupt.cancelled() => Err(SessionTurnInterrupted.into()),
                    _ = &mut sleep => Ok(()),
                }
            }
            None => {
                sleep.await;
                Ok(())
            }
        }
    }

    pub fn estimate_context_tokens(
        &self,
        system_prompt: &str,
        messages: &[SessionTurnMessage],
    ) -> usize {
        let messages = normalize_provider_messages(messages);
        let tools = self
            .tools
            .definitions()
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        estimate_provider_request_context_tokens(system_prompt, &messages, &tools).used_tokens
    }

    pub async fn abandon_delegations_for_session(
        &self,
        session_id: &SessionId,
        reason: &str,
    ) -> anyhow::Result<usize> {
        self.tools
            .abandon_delegations_for_session(session_id, reason)
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn abandon_delegations_for_session_best_effort(
        &self,
        session_id: &SessionId,
        reason: &str,
    ) -> usize {
        self.tools
            .abandon_delegations_for_session_best_effort(session_id, reason)
            .await
    }

    /// resume 不继承上一次运行期的 file_read 写入许可。
    pub async fn clear_file_read_state(&self, session_id: &SessionId) {
        self.tools.clear_file_read_state(session_id).await;
    }

    pub(crate) async fn clear_parent_file_read_state(&self, session_id: &SessionId) {
        self.tools.clear_parent_file_read_state(session_id).await;
    }

    pub(crate) async fn begin_file_read_state_checkpoint(
        &self,
        session_id: &SessionId,
        turn_id: &str,
    ) -> anyhow::Result<()> {
        self.tools
            .begin_file_read_state_checkpoint(session_id, turn_id)
            .await
            .map_err(anyhow::Error::msg)
    }

    pub(crate) async fn commit_file_read_state_checkpoint(
        &self,
        session_id: &SessionId,
        turn_id: &str,
    ) -> anyhow::Result<()> {
        self.tools
            .commit_file_read_state_checkpoint(session_id, turn_id)
            .await
            .map_err(anyhow::Error::msg)
    }

    pub(crate) async fn rollback_file_read_state_checkpoint(
        &self,
        session_id: &SessionId,
        turn_id: &str,
    ) -> anyhow::Result<()> {
        self.tools
            .rollback_file_read_state_checkpoint(session_id, turn_id)
            .await
            .map_err(anyhow::Error::msg)
    }
}

fn non_streaming_fallback_delay(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10);
    let factor = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    NON_STREAMING_FALLBACK_BASE_DELAY
        .saturating_mul(factor)
        .min(NON_STREAMING_FALLBACK_MAX_DELAY)
}

fn normalize_provider_messages(messages: &[SessionTurnMessage]) -> Vec<SessionTurnMessage> {
    let mut out: Vec<SessionTurnMessage> = Vec::with_capacity(messages.len());
    for message in messages {
        if let Some(text) = pure_user_text(message) {
            if let Some(last) = out.last_mut() {
                if let Some(last_text) = pure_user_text(last) {
                    *last = SessionTurnMessage::user_text(format!("{last_text}\n\n{text}"));
                    continue;
                }
            }
        }
        out.push(message.clone());
    }
    out
}

/// 保存上一份真实发送请求在 raw history 与 wire history 中的对应边界。
///
/// raw history 继续服务 compaction/canonical cursor；wire history 则原样冻结，后续只对
/// raw suffix 应用既有规范化，因而不会为了合并相邻 user 而重写已缓存前缀。
#[derive(Debug, Default)]
struct FrozenProviderRequestPrefix {
    raw_messages: Vec<SessionTurnMessage>,
    wire_messages: Vec<SessionTurnMessage>,
}

impl FrozenProviderRequestPrefix {
    fn new(messages: &[SessionTurnMessage], frozen_prefix_len: usize) -> anyhow::Result<Self> {
        let raw_messages = messages
            .get(..frozen_prefix_len)
            .with_context(|| {
                format!(
                    "冻结 provider history 前缀越界: prefix={frozen_prefix_len}, history={}",
                    messages.len()
                )
            })?
            .to_vec();
        Ok(Self {
            wire_messages: raw_messages.clone(),
            raw_messages,
        })
    }

    fn clear(&mut self) {
        self.raw_messages.clear();
        self.wire_messages.clear();
    }

    fn project(&self, messages: &[SessionTurnMessage]) -> anyhow::Result<Vec<SessionTurnMessage>> {
        if !messages.starts_with(&self.raw_messages) {
            anyhow::bail!(
                "provider history 在非 compaction 边界改写了已发送前缀: frozen={}, current={}",
                self.raw_messages.len(),
                messages.len()
            );
        }
        let mut projected = Vec::with_capacity(
            self.wire_messages
                .len()
                .saturating_add(messages.len().saturating_sub(self.raw_messages.len())),
        );
        projected.extend(self.wire_messages.iter().cloned());
        projected.extend(normalize_provider_messages(
            &messages[self.raw_messages.len()..],
        ));
        Ok(projected)
    }

    fn advance(
        &mut self,
        raw_messages: &[SessionTurnMessage],
        wire_messages: Vec<SessionTurnMessage>,
    ) {
        self.raw_messages = raw_messages.to_vec();
        self.wire_messages = wire_messages;
    }
}

/// adapter continuation 的最新请求基于 wire history，而 compactor 继续使用 raw
/// history 的 message boundary。两者前缀已经由 `FrozenProviderRequestPrefix` 对齐；
/// 这里只追加 adapter 新产生的 replay suffix，避免 normalization 缩短前缀后令
/// active-start index 漂移。
fn append_adapter_continuation_suffix_to_raw_history(
    raw_messages: &mut Vec<SessionTurnMessage>,
    outer_request: &[SessionTurnMessage],
    latest_request: &[SessionTurnMessage],
) -> anyhow::Result<()> {
    let suffix = latest_request
        .strip_prefix(outer_request)
        .context("adapter continuation 的最终请求未保留外层 Provider 请求前缀")?;
    raw_messages.extend_from_slice(suffix);
    Ok(())
}

/// 把 adapter 返回的完整 continuation replay 裁成“最后一次请求之后”
/// 的响应 suffix。已经出现在 latest request 中的 partial/trigger 不能再放入
/// provider history，否则下一请求会重复 replay。
fn provider_assistant_suffix_for_latest_request(
    assistant: &SessionTurnMessage,
    attempt_base: &[SessionTurnMessage],
    latest_request: &[SessionTurnMessage],
) -> anyhow::Result<SessionTurnMessage> {
    let Some(continuation_suffix) = latest_request.strip_prefix(attempt_base) else {
        anyhow::bail!(
            "adapter continuation 的最终请求未保留 attempt 前缀: base={}, latest={}",
            attempt_base.len(),
            latest_request.len()
        );
    };
    let mut assistant = assistant.clone();
    for message in continuation_suffix {
        if message.role != "assistant" {
            anyhow::bail!("adapter continuation suffix 包含非 assistant neutral message");
        }
        let prefix = message
            .provider_replay
            .as_ref()
            .context("adapter continuation suffix 缺少 provider replay")?;
        let replay = assistant
            .provider_replay
            .as_mut()
            .context("adapter continuation response 缺少 provider replay")?;
        strip_provider_replay_prefix(replay, prefix)?;
    }
    Ok(assistant)
}

fn strip_provider_replay_prefix(
    replay: &mut ProviderReplayState,
    prefix: &ProviderReplayState,
) -> anyhow::Result<()> {
    let (items, prefix_items, protocol) = match (replay, prefix) {
        (
            ProviderReplayState::OpenAiResponses { model, items },
            ProviderReplayState::OpenAiResponses {
                model: prefix_model,
                items: prefix_items,
            },
        ) if model == prefix_model => (items, prefix_items, "openai_responses"),
        (
            ProviderReplayState::OpenAiChatCompletions { model, messages },
            ProviderReplayState::OpenAiChatCompletions {
                model: prefix_model,
                messages: prefix_messages,
            },
        ) if model == prefix_model => (messages, prefix_messages, "openai_chat_completions"),
        (
            ProviderReplayState::AnthropicMessages { model, messages },
            ProviderReplayState::AnthropicMessages {
                model: prefix_model,
                messages: prefix_messages,
            },
        ) if model == prefix_model => (messages, prefix_messages, "anthropic_messages"),
        _ => anyhow::bail!("adapter continuation replay 协议或 model 在同一请求内发生变化"),
    };
    if !items.starts_with(prefix_items) {
        anyhow::bail!(
            "{protocol} continuation response 未保留已发送 replay 前缀: prefix={}, response={}",
            prefix_items.len(),
            items.len()
        );
    }
    items.drain(..prefix_items.len());
    Ok(())
}

/// non-streaming fallback 从最新 continuation request 继续时，新响应文本只是
/// 未完成 suffix。canonical assistant 需要合并此前已完成 partial，而精确
/// Provider history 仍由 `provider_assistant_suffix_for_latest_request` 分开保存。
fn merge_prior_attempt_continuation_text(
    assistant: &mut SessionTurnMessage,
    initial_request: &[SessionTurnMessage],
    fallback_base: &[SessionTurnMessage],
) -> anyhow::Result<()> {
    let Some(prior_continuations) = fallback_base.strip_prefix(initial_request) else {
        anyhow::bail!(
            "fallback 请求未保留初始 Provider 前缀: initial={}, fallback={}",
            initial_request.len(),
            fallback_base.len()
        );
    };
    let mut prior_text = String::new();
    for message in prior_continuations {
        if message.role != "assistant" || message.provider_replay.is_none() {
            anyhow::bail!("fallback continuation suffix 不是 adapter 上报的 replay message");
        }
        append_with_overlap_dedupe(&mut prior_text, &assistant_message_text(message));
    }
    if prior_text.trim().is_empty() {
        return Ok(());
    }
    append_with_overlap_dedupe(&mut prior_text, &assistant_message_text(assistant));
    let first_text_index = assistant
        .content
        .iter()
        .position(|block| matches!(block, SessionTurnContentBlock::Text { .. }))
        .unwrap_or(0);
    let mut non_text = std::mem::take(&mut assistant.content)
        .into_iter()
        .filter(|block| !matches!(block, SessionTurnContentBlock::Text { .. }))
        .collect::<Vec<_>>();
    non_text.insert(
        first_text_index.min(non_text.len()),
        SessionTurnContentBlock::text(prior_text),
    );
    assistant.content = non_text;
    Ok(())
}

fn assistant_message_text(message: &SessionTurnMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            SessionTurnContentBlock::Text { text } => Some(text.as_str()),
            SessionTurnContentBlock::SkillInstructions { .. }
            | SessionTurnContentBlock::ModelContext { .. } => None,
            SessionTurnContentBlock::Image { .. }
            | SessionTurnContentBlock::Document { .. }
            | SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn runtime_context_text(now: DateTime<Utc>) -> String {
    // 这里需要模型看到用户本地日历语义，不能只用 UTC 日期。
    let local_now = now.with_timezone(&Local);
    let current_date = local_now.format("%Y-%m-%d %A");
    let timezone = local_timezone_name(&local_now);
    format!(
        "<runtime_context>\ncurrent_date: {current_date}\ntimezone: {timezone}\n</runtime_context>"
    )
}

fn latest_model_context(
    messages: &[SessionTurnMessage],
    source: ModelContextSource,
) -> Option<(&str, &str)> {
    messages.iter().rev().find_map(|message| {
        let (candidate_source, fingerprint, text) = message.model_context_snapshot()?;
        (*candidate_source == source).then_some((fingerprint, text))
    })
}

fn append_new_model_context_messages<'a>(
    messages: &mut Vec<SessionTurnMessage>,
    candidates: impl IntoIterator<Item = &'a SessionTurnMessage>,
) -> anyhow::Result<()> {
    for candidate in candidates {
        let (source, fingerprint, text) = candidate
            .model_context_snapshot()
            .context("context appender 只能返回独立 ModelContext user message")?;
        let expected = SessionTurnMessage::model_context(*source, text.to_string());
        let Some((_, expected_fingerprint, _)) = expected.model_context_snapshot() else {
            anyhow::bail!("内部 ModelContext 构造失败");
        };
        if fingerprint != expected_fingerprint {
            anyhow::bail!("context appender 返回了与正文不匹配的 fingerprint");
        }
        if latest_model_context(messages, *source).is_some_and(
            |(latest_fingerprint, latest_text)| {
                latest_fingerprint == fingerprint && latest_text == text
            },
        ) {
            continue;
        }
        messages.push(candidate.clone());
    }
    Ok(())
}

fn local_timezone_name(local_now: &DateTime<Local>) -> String {
    iana_time_zone::get_timezone().unwrap_or_else(|_| format!("UTC{}", local_now.format("%:z")))
}

fn pure_user_text(message: &SessionTurnMessage) -> Option<String> {
    if message.role != "user" {
        return None;
    }
    let mut parts = Vec::new();
    for block in &message.content {
        match block {
            SessionTurnContentBlock::Text { text } => parts.push(text.as_str()),
            SessionTurnContentBlock::SkillInstructions { .. }
            | SessionTurnContentBlock::ModelContext { .. } => return None,
            SessionTurnContentBlock::Image { .. } | SessionTurnContentBlock::Document { .. } => {
                return None
            }
            SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => {
                return None;
            }
        }
    }
    Some(parts.join("\n"))
}

async fn session_user_message(
    user_text: String,
    attachments: Vec<SessionAttachment>,
    skill_instructions: Vec<SkillInstructions>,
    limits: &AttachmentLimits,
) -> anyhow::Result<(SessionTurnMessage, Vec<TextAttachmentRead>, Vec<String>)> {
    if !limits.enabled && !attachments.is_empty() {
        anyhow::bail!("附件功能已禁用");
    }
    if attachments.len() > limits.max_files_per_turn {
        anyhow::bail!(
            "单轮附件数量超限: {} 个，最多 {} 个",
            attachments.len(),
            limits.max_files_per_turn
        );
    }
    let mut blocks = Vec::with_capacity(
        attachments
            .len()
            .saturating_add(skill_instructions.len())
            .saturating_add(1),
    );
    blocks.extend(
        skill_instructions
            .into_iter()
            .map(SessionTurnContentBlock::skill_instructions),
    );
    blocks.push(SessionTurnContentBlock::text(user_text));
    let mut text_reads = Vec::<TextAttachmentRead>::new();
    let mut warnings = Vec::<String>::new();
    for attachment in attachments {
        let (block, text_read, warning) = session_attachment_block(attachment, limits).await?;
        blocks.push(block);
        if let Some(text_read) = text_read {
            text_reads.push(text_read);
        }
        if let Some(warning) = warning {
            warnings.push(warning);
        }
    }
    Ok((
        SessionTurnMessage::user_content(blocks),
        text_reads,
        warnings,
    ))
}

async fn session_attachment_block(
    attachment: SessionAttachment,
    limits: &AttachmentLimits,
) -> anyhow::Result<(
    SessionTurnContentBlock,
    Option<TextAttachmentRead>,
    Option<String>,
)> {
    match attachment {
        SessionAttachment::LocalImage { path } => {
            let media = crate::attachment::read_image_attachment(&path, limits)
                .await
                .context("读取图片附件失败")?;
            Ok((
                SessionTurnContentBlock::image(media.media_type, media.data),
                None,
                None,
            ))
        }
        SessionAttachment::InlineImage { data, .. } => {
            let bytes = BASE64_STANDARD
                .decode(data.as_bytes())
                .context("内联图片附件 base64 解码失败")?;
            let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if actual > limits.max_file_bytes {
                anyhow::bail!(
                    "附件过大: inline image 为 {} bytes，超过上限 {} bytes",
                    actual,
                    limits.max_file_bytes
                );
            }
            let media = crate::attachment::normalize_image_attachment_with_limits(
                bytes,
                "inline image",
                limits,
            )
            .await
            .context("校验内联图片附件失败")?;
            Ok((
                SessionTurnContentBlock::image(media.media_type, media.data),
                None,
                None,
            ))
        }
        SessionAttachment::TextFile { path } => read_text_file_block(&path, limits).await,
        SessionAttachment::DocumentFile { path, media_type } => {
            if media_type != "application/pdf" {
                anyhow::bail!(
                    "暂不支持的文档附件 media_type: {} ({})",
                    media_type,
                    path.display()
                );
            }
            let media = crate::attachment::read_document_attachment(&path, limits)
                .await
                .context("读取 PDF 附件失败")?;
            Ok((
                SessionTurnContentBlock::document_named(
                    media.media_type,
                    media.data,
                    media.source_name,
                ),
                None,
                None,
            ))
        }
    }
}

async fn read_text_file_block(
    path: &Path,
    limits: &AttachmentLimits,
) -> anyhow::Result<(
    SessionTurnContentBlock,
    Option<TextAttachmentRead>,
    Option<String>,
)> {
    // 先固定并校验真实目标，再从同一 canonical 路径读取。这样符号链接在读取后
    // 被切换时，不会让正文来自受保护文件、许可却绑定到另一个目标。
    let canonical_path = tokio::fs::canonicalize(path)
        .await
        .context("解析文本附件真实路径失败")?;
    if crate::attachment::is_protected_memory_path(&canonical_path) {
        anyhow::bail!("MEMORY.md / USER.md 必须通过 memory 工具访问");
    }
    let content = crate::attachment::read_text_attachment(&canonical_path, limits)
        .await
        .context("读取文本附件失败")?;
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("attachment");
    let actual_chars = content.chars().count();
    if actual_chars > limits.max_text_chars {
        let provider_text = format!(
            "Attached text file body omitted: {name}\nPath: {}\nCharacters: \
             {actual_chars}\nThe file exceeds the per-file text attachment limit of {} \
             characters. Use file_read with this path to inspect it before editing.",
            path.display(),
            limits.max_text_chars,
        );
        let warning = format!(
            "文本附件 `{}` 共 {} 个字符，超过单文件上限 {}；已仅向模型提供路径，\
             未注入正文，也未授予读取许可。",
            path.display(),
            actual_chars,
            limits.max_text_chars,
        );
        return Ok((
            SessionTurnContentBlock::text(provider_text),
            None,
            Some(warning),
        ));
    }
    let provider_text = format!(
        "Attached file: {name}\nPath: {}\n\n{content}",
        path.display()
    );
    Ok((
        SessionTurnContentBlock::text(provider_text.clone()),
        Some(TextAttachmentRead {
            canonical_path,
            content,
            provider_text,
        }),
        None,
    ))
}

#[derive(Debug)]
struct TextAttachmentRead {
    canonical_path: std::path::PathBuf,
    content: String,
    provider_text: String,
}

impl TextAttachmentRead {
    /// preflight 可以压缩或外置附件；只有完整正文仍在即将发送的请求中才登记读取许可。
    fn is_visible_in(&self, messages: &[SessionTurnMessage]) -> bool {
        messages.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(block, SessionTurnContentBlock::Text { text } if text == &self.provider_text)
            })
        })
    }
}

#[derive(Debug, Clone)]
struct CanonicalToolUse {
    id: String,
    name: String,
    input: Value,
}

fn validate_assistant_message(message: &SessionTurnMessage) -> anyhow::Result<()> {
    if message.role != "assistant" {
        anyhow::bail!("provider response role 必须是 assistant: {}", message.role);
    }
    if message.content.iter().any(|block| {
        matches!(
            block,
            SessionTurnContentBlock::ToolResult { .. }
                | SessionTurnContentBlock::ModelContext { .. }
        )
    }) {
        anyhow::bail!("assistant message 不允许包含 ToolResult 或 ModelContext block");
    }
    Ok(())
}

fn collect_tool_uses(message: &SessionTurnMessage) -> anyhow::Result<Vec<CanonicalToolUse>> {
    let mut tool_uses = Vec::new();
    for block in &message.content {
        if let SessionTurnContentBlock::ToolUse { id, name, input } = block {
            if id.trim().is_empty() {
                anyhow::bail!("tool_use id 不能为空");
            }
            if name.trim().is_empty() {
                anyhow::bail!("tool_use name 不能为空");
            }
            if !input.is_object() {
                anyhow::bail!("tool_use input 必须是 JSON object: {name}");
            }
            tool_uses.push(CanonicalToolUse {
                id: id.clone(),
                name: name.clone(),
                input: input.clone(),
            });
        }
    }
    Ok(tool_uses)
}

/// 确认 fallback 响应可安全替换已显示 partial，并进入既有工具循环。
fn validate_non_streaming_fallback_response(
    response: &ProviderResponse,
    seen_tool_use_ids: &HashSet<String>,
) -> anyhow::Result<()> {
    validate_assistant_message(&response.assistant_message)
        .context("non-streaming fallback assistant message 校验失败")?;
    let tool_uses = collect_tool_uses(&response.assistant_message)
        .context("non-streaming fallback tool use 校验失败")?;
    validate_new_tool_use_ids(&tool_uses, seen_tool_use_ids)
        .context("non-streaming fallback tool use id 校验失败")?;
    validate_provider_response_terminal_semantics(response.stop, &tool_uses)
        .context("non-streaming fallback provider stop 校验失败")?;
    Ok(())
}

/// 检查候选响应不会与同一 turn 已使用或自身的 tool_use id 冲突。
fn validate_new_tool_use_ids(
    tool_uses: &[CanonicalToolUse],
    seen_tool_use_ids: &HashSet<String>,
) -> anyhow::Result<()> {
    let mut candidate_ids = HashSet::with_capacity(tool_uses.len());
    for tool_use in tool_uses {
        if seen_tool_use_ids.contains(&tool_use.id) || !candidate_ids.insert(&tool_use.id) {
            anyhow::bail!(
                "provider 在同一 session turn 内重复 tool_use id: {}",
                tool_use.id
            );
        }
    }
    Ok(())
}

/// 复用 turn loop 对完整 provider 响应的终态安全约束。
fn validate_provider_response_terminal_semantics(
    stop: ProviderStop,
    tool_uses: &[CanonicalToolUse],
) -> anyhow::Result<()> {
    match (tool_uses.is_empty(), stop) {
        (true, ProviderStop::Done) | (false, ProviderStop::ToolUse) => Ok(()),
        (true, ProviderStop::ToolUse) => {
            anyhow::bail!("provider stop=ToolUse 但 assistant message 没有 ToolUse block")
        }
        (false, ProviderStop::Done) => {
            anyhow::bail!("provider stop=Done 但 assistant message 包含 ToolUse block")
        }
        (true, ProviderStop::MaxTokens) => {
            anyhow::bail!("provider stop=MaxTokens，无法安全完成 session turn")
        }
        (false, ProviderStop::MaxTokens) => {
            anyhow::bail!("provider stop=MaxTokens 且包含 ToolUse，拒绝执行半截工具调用")
        }
        (true, ProviderStop::ContextWindowExceeded)
        | (false, ProviderStop::ContextWindowExceeded) => Ok(()),
    }
}

async fn emit_skipped_tool_calls(
    tool_uses: &[CanonicalToolUse],
    emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
    input_preview_max_chars: usize,
    reason: ToolCallSkipReason,
) -> anyhow::Result<()> {
    let _ = emit_skipped_tool_calls_until(
        tool_uses,
        emit,
        durable_recorder,
        input_preview_max_chars,
        reason,
        None,
    )
    .await?;
    Ok(())
}

/// hard-cancel 的 grace 已耗尽时，journal 不再允许阻塞 TUI 收束；但未派发调用
/// 仍必须向内存/TUI 事件流显式标为 Skipped。
fn emit_skipped_tool_calls_without_recording(
    tool_uses: &[CanonicalToolUse],
    emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    input_preview_max_chars: usize,
    reason: ToolCallSkipReason,
) {
    for tool_use in tool_uses {
        let (input_preview, input_truncated) =
            tool_input_preview(&tool_use.name, &tool_use.input, input_preview_max_chars);
        emit(SessionTurnEvent::ToolCallSkipped {
            id: tool_use.id.clone(),
            name: tool_use.name.clone(),
            summary: tool_started_summary(&tool_use.name, &tool_use.input),
            input_preview,
            input_truncated,
            reason,
        });
    }
}

/// 显式取消期间 journal 只能在 grace deadline 前尝试写入；TUI 事件仍完整发出，deadline
/// 到期后调用方会立即 drop 尚未结束的工具 future。
async fn emit_skipped_tool_calls_until(
    tool_uses: &[CanonicalToolUse],
    emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
    input_preview_max_chars: usize,
    reason: ToolCallSkipReason,
    deadline: Option<Instant>,
) -> anyhow::Result<usize> {
    let mut emitted = 0usize;
    let mut first_error = None;
    for tool_use in tool_uses {
        let (input_preview, input_truncated) =
            tool_input_preview(&tool_use.name, &tool_use.input, input_preview_max_chars);
        let event = SessionTurnEvent::ToolCallSkipped {
            id: tool_use.id.clone(),
            name: tool_use.name.clone(),
            summary: tool_started_summary(&tool_use.name, &tool_use.input),
            input_preview,
            input_truncated,
            reason,
        };
        emit(event.clone());
        emitted = emitted.saturating_add(1);
        match record_durable_event_until(durable_recorder, event, deadline).await {
            Ok(true) => {}
            Ok(false) => return Ok(emitted),
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(emitted)
}

/// Esc/Ctrl-C 的 grace 到期后，仍在执行的 future 会被 drop；先为每个已 Started
/// 调用持久化唯一的 Interrupted 终态，避免 TUI/journal 留下无终态 tool cell。
#[allow(
    clippy::too_many_arguments,
    reason = "强制中断必须同时持有当前 batch 的工具身份、owner 上下文和唯一事件/journal sink"
)]
async fn emit_forced_abort_interrupts(
    in_flight: &mut BTreeMap<usize, CanonicalToolUse>,
    terminal_tool_use_ids: &mut HashSet<String>,
    tools: &ToolRegistry,
    current_session_id: &Option<SessionId>,
    current_turn_id: &Option<String>,
    emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
    deadline: Option<Instant>,
) -> anyhow::Result<()> {
    let pending = std::mem::take(in_flight);
    let mut first_error = None;
    for (_, tool_use) in pending {
        if !terminal_tool_use_ids.insert(tool_use.id.clone()) {
            continue;
        }
        let continuing_process_ids = if tool_use.name == "code_run" {
            tools
                .live_process_ids_for_tool_use(&ToolDispatchContext {
                    current_session_id: current_session_id.clone(),
                    current_turn_id: current_turn_id.clone(),
                    tool_use_id: Some(tool_use.id.clone()),
                    progress_tx: None,
                    cancellation: None,
                    provider_mcp_routes: None,
                    failed_file_write_paths: None,
                })
                .await
        } else {
            Vec::new()
        };
        let summary = match continuing_process_ids.as_slice() {
            [] => format!("tool {} interrupted", tool_use.name),
            [process_id] => {
                format!("Interrupted · process {process_id} continues in background")
            }
            process_ids => format!(
                "Interrupted · processes {} continue in background",
                process_ids.join(" / ")
            ),
        };
        let event = SessionTurnEvent::ToolCallInterrupted {
            id: tool_use.id,
            summary,
        };
        emit(event.clone());
        match record_durable_event_until(durable_recorder, event, deadline).await {
            Ok(true) | Ok(false) => {}
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(())
}

fn emit_tool_progress_if_active(
    progress: ToolProgressUpdate,
    terminal_tool_use_ids: &HashSet<String>,
    emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
) {
    if !terminal_tool_use_ids.contains(&progress.id) {
        emit(SessionTurnEvent::ToolCallProgress {
            id: progress.id,
            summary: progress.summary,
        });
    }
}

fn tool_boundary_is_cancelled(control: Option<&ToolBoundaryControl>) -> bool {
    control.is_some_and(ToolBoundaryControl::is_cancelled)
}

fn tool_boundary_skip_reason_value(control: Option<&ToolBoundaryControl>) -> ToolCallSkipReason {
    control
        .and_then(ToolBoundaryControl::cancel_reason)
        // 未受 SessionTurnControl 管理的调用没有显式原因，按 interrupted 的保守语义落盘。
        .unwrap_or(ToolCallSkipReason::TurnInterruptedBeforeDispatch)
}

async fn record_durable_event(
    durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
    event: SessionTurnEvent,
) -> anyhow::Result<()> {
    if let Some(recorder) = durable_recorder.as_deref_mut() {
        recorder.record(event).await?;
    }
    Ok(())
}

/// deadline 存在时，durable recorder 不得让显式取消越过 100ms grace。drop 一个未完成的
/// recorder future 是有意的 bounded journal flush；外部副作用不因此回滚。
async fn record_durable_event_until(
    durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
    event: SessionTurnEvent,
    deadline: Option<Instant>,
) -> anyhow::Result<bool> {
    let Some(deadline) = deadline else {
        record_durable_event(durable_recorder, event).await?;
        return Ok(true);
    };
    if Instant::now() >= deadline {
        return Ok(false);
    }
    tokio::select! {
        result = record_durable_event(durable_recorder, event) => {
            result?;
            Ok(true)
        }
        _ = tokio::time::sleep_until(deadline) => Ok(false),
    }
}

/// 工具 batch 内的 durable 写入也必须服从 Esc/Ctrl-C：若用户在写入期间显式取消，
/// 立即放弃这次写入并回到 100ms 收束路径；steer 不会取消这里的写入。
enum DurableRecordOutcome {
    /// `deadline` 表示取消是在本次 write 期间到达；之后所有收束必须复用它。
    Recorded { deadline: Option<Instant> },
    /// 当前 journal flush 已消耗的取消 deadline；调用方必须复用它，不能再起一轮 grace。
    Abandoned { deadline: Option<Instant> },
}

async fn record_durable_event_while_tool_batch_active(
    durable_recorder: &mut Option<&mut dyn SessionTurnEventRecorder>,
    event: SessionTurnEvent,
    explicit_cancel: Option<&CancellationToken>,
    deadline: Option<Instant>,
) -> anyhow::Result<DurableRecordOutcome> {
    if let Some(deadline) = deadline {
        return record_durable_event_until(durable_recorder, event, Some(deadline))
            .await
            .map(|recorded| {
                if recorded {
                    DurableRecordOutcome::Recorded {
                        deadline: Some(deadline),
                    }
                } else {
                    DurableRecordOutcome::Abandoned {
                        deadline: Some(deadline),
                    }
                }
            });
    }
    let Some(explicit_cancel) = explicit_cancel else {
        record_durable_event(durable_recorder, event).await?;
        return Ok(DurableRecordOutcome::Recorded { deadline: None });
    };
    // 取消已经在线性化点之前到达时，不应为了新的 journal 写入重新延长 turn；只有已经
    // 开始 poll 的写入才会获得 D20 规定的有界收尾时间。
    if explicit_cancel.is_cancelled() {
        return Ok(DurableRecordOutcome::Abandoned { deadline: None });
    }
    let record = record_durable_event(durable_recorder, event);
    tokio::pin!(record);
    tokio::select! {
        biased;
        // Esc/Ctrl-C 已经在线性化点到达时必须胜过同一 poll 内刚好完成的 Started
        // journal write；否则会在取消已关闭 dispatch gate 后仍错误启动工具。
        _ = explicit_cancel.cancelled() => {
            // 不 drop 已经进入 recorder 的 future：它可能已经拿走了内部的 release
            // handle。给它与普通工具相同的 100ms grace，超时才放弃。
            let deadline = Instant::now() + Duration::from_millis(100);
            tokio::select! {
                result = &mut record => {
                    result?;
                    Ok(DurableRecordOutcome::Recorded {
                        deadline: Some(deadline),
                    })
                }
                _ = tokio::time::sleep_until(deadline) => Ok(DurableRecordOutcome::Abandoned {
                    deadline: Some(deadline),
                }),
            }
        }
        result = &mut record => {
            result?;
            Ok(DurableRecordOutcome::Recorded { deadline: None })
        }
    }
}

struct ExecutedToolUse {
    content: String,
    canonical_content: String,
    outcome: ToolExecutionOutcome,
    output_preview: String,
    /// file_read 等工具读到的图片 / PDF 内容块，随 tool_result 一起回灌给模型。
    media_blocks: Vec<SessionTurnContentBlock>,
    /// file 类工具修改成功时采集的 diff；从工具输出剥离，不回灌模型。
    file_change: Option<FileChange>,
    process_delivery_receipt: Option<ProcessDeliveryReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ToolUseInterrupted {
    continuing_process_id: Option<String>,
}

async fn execute_tool_use(
    tools: &ToolRegistry,
    name: &str,
    input: Value,
    context: ToolDispatchContext,
    require_concurrency_safe: bool,
    dispatch_rejection: Option<String>,
) -> Result<ExecutedToolUse, ToolUseInterrupted> {
    let dispatched = if let Some(error) = dispatch_rejection {
        Err(ToolError::InvalidArgs(error))
    } else if require_concurrency_safe {
        tools
            .dispatch_concurrency_safe_with_context(name, input, context)
            .await
    } else {
        tools.dispatch_with_context(name, input, context).await
    };
    match dispatched {
        Ok(mut execution) => {
            // 媒体内容从 tool_result 文本中剥离，避免 base64 进入文本通道。
            let media_blocks = take_media_blocks(&mut execution.output);
            // file diff 只服务于 TUI 展示与 journal，回灌模型前同样剥离。
            let file_change = take_file_change(&mut execution.output);
            let raw_output_preview = tool_output_preview(name, &execution.output);
            let output_preview = raw_output_preview.clone();
            let payload = json!({
                "ok": execution.outcome.is_success(),
                "outcome": execution.outcome,
                "output": execution.output,
            });
            let content = serde_json::to_string(&payload)
                .context("序列化 tool_result")
                .unwrap_or_else(|e| {
                    json!({
                        "ok": false,
                        "outcome": ToolExecutionOutcome::DispatchFailure,
                        "error": e.to_string(),
                    })
                    .to_string()
                });
            Ok(ExecutedToolUse {
                canonical_content: content.clone(),
                content,
                outcome: execution.outcome,
                output_preview,
                media_blocks,
                file_change,
                process_delivery_receipt: execution.process_delivery_receipt,
            })
        }
        Err(ToolError::Interrupted) => Err(ToolUseInterrupted {
            continuing_process_id: None,
        }),
        Err(ToolError::ProcessContinuesInBackground { process_id }) => Err(ToolUseInterrupted {
            continuing_process_id: Some(process_id),
        }),
        Err(err) => {
            let error = err.to_string();
            let outcome = ToolExecutionOutcome::DispatchFailure;
            let content = serde_json::to_string(&json!({
                "ok": false,
                "outcome": outcome,
                "error": error.clone(),
            }))
            .unwrap_or_else(|_| {
                r#"{"ok":false,"outcome":{"kind":"dispatch_failure"},"error":"tool result serialization failed"}"#.into()
            });
            Ok(ExecutedToolUse {
                content: content.clone(),
                canonical_content: content,
                outcome,
                output_preview: error,
                media_blocks: Vec::new(),
                file_change: None,
                process_delivery_receipt: None,
            })
        }
    }
}

/// 从工具输出 JSON 中取走保留键 `media`，转换为媒体内容块（前置一条说明文本）。
fn take_media_blocks(output: &mut Value) -> Vec<SessionTurnContentBlock> {
    let Some(media_value) = output
        .as_object_mut()
        .and_then(|object| object.remove(FILE_READ_MEDIA_KEY))
    else {
        return Vec::new();
    };
    let Some(media) = NormalizedMedia::from_json(&media_value) else {
        // 保留键存在但结构不合法：媒体被丢弃属于可降级异常，必须留痕。
        // 只记 source_name，避免 base64 数据进日志。
        let source_name = media_value
            .get("source_name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        log::warn!(
            target: "turn_loop",
            "tool 输出的 media 附件结构不合法，已丢弃: source_name={source_name}"
        );
        return Vec::new();
    };
    let block = match media.kind {
        AttachmentKind::Image => SessionTurnContentBlock::image(media.media_type, media.data),
        AttachmentKind::Pdf => SessionTurnContentBlock::document_named(
            media.media_type,
            media.data,
            media.source_name.clone(),
        ),
        AttachmentKind::Text => return Vec::new(),
    };
    vec![
        SessionTurnContentBlock::text(format!("[file_read attachment] {}", media.source_name)),
        block,
    ]
}

fn tool_output_preview(name: &str, value: &Value) -> String {
    if name == "consult_router" {
        if let Some(preview) = consult_router_output_preview(value) {
            return preview;
        }
    }
    value.to_string()
}

fn consult_router_output_preview(value: &Value) -> Option<String> {
    match value.get("mode").and_then(Value::as_str) {
        Some("overview") => {
            let scopes = value.get("scopes")?.as_array()?.len();
            Some(format!("scopes={scopes}"))
        }
        Some("query") | None => {
            let claims = value.get("candidate_claims")?.as_array()?.len();
            let disputes = value
                .get("disputes")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            Some(format!("claims={claims} disputes={disputes}"))
        }
        Some(other) => Some(format!("mode={other}")),
    }
}

fn tool_started_summary(name: &str, input: &Value) -> String {
    format!("tool {name} {}", one_line_preview(&input.to_string(), 160))
}

fn tool_input_preview(_name: &str, input: &Value, max_chars: usize) -> (String, bool) {
    truncate_chars(&input.to_string(), max_chars)
}

fn tool_completed_summary(
    name: &str,
    outcome: ToolExecutionOutcome,
    output_preview: &str,
) -> String {
    let status = match outcome {
        ToolExecutionOutcome::Completed => "ok".to_string(),
        ToolExecutionOutcome::DispatchFailure => "dispatch_failed".to_string(),
        ToolExecutionOutcome::BusinessFailure => "business_failed".to_string(),
        ToolExecutionOutcome::ProcessExit {
            exit_code, success, ..
        } => match exit_code {
            Some(exit_code) => format!("exit_code={exit_code}"),
            None if success => "exit_success".to_string(),
            None => "exit_failed".to_string(),
        },
        ToolExecutionOutcome::ProcessTerminated { signal } => signal.map_or_else(
            || "process_terminated".to_string(),
            |signal| format!("terminated_signal={signal}"),
        ),
        ToolExecutionOutcome::ProcessRunning => "process_running".to_string(),
        ToolExecutionOutcome::HttpResponse { http_status } => {
            format!("http_status={http_status}")
        }
    };
    let preview = one_line_preview(output_preview, 160);
    if preview.is_empty() {
        format!("tool {name} {status}")
    } else {
        format!("tool {name} {status} {preview}")
    }
}

fn tool_journal_output_preview(output_preview: &str, max_chars: usize) -> (String, bool) {
    truncate_chars(output_preview, max_chars)
}

fn one_line_preview(raw: &str, limit: usize) -> String {
    let compact = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= limit {
        return compact;
    }
    let mut out = compact.chars().take(limit).collect::<String>();
    out.push_str("...");
    out
}

fn truncate_chars(raw: &str, limit: usize) -> (String, bool) {
    if raw.chars().count() <= limit {
        return (raw.to_string(), false);
    }
    let mut out = raw.chars().take(limit).collect::<String>();
    out.push_str("...");
    (out, true)
}

fn provider_request_contains_process_tool_result(
    messages: &[SessionTurnMessage],
    pending: &PendingProcessDelivery,
) -> bool {
    messages
        .iter()
        .rev()
        .flat_map(|message| message.content.iter().rev())
        .find_map(|block| match block {
            SessionTurnContentBlock::ToolResult {
                tool_use_id,
                content,
            } if tool_use_id == &pending.tool_use_id => Some(content),
            SessionTurnContentBlock::Text { .. }
            | SessionTurnContentBlock::ModelContext { .. }
            | SessionTurnContentBlock::SkillInstructions { .. }
            | SessionTurnContentBlock::Image { .. }
            | SessionTurnContentBlock::Document { .. }
            | SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => None,
        })
        .is_some_and(|content| content == &pending.tool_result_content)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet, VecDeque};
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;
    use chrono::{DateTime, Local, Utc};
    use serde_json::{json, Value};
    use tokio::sync::{oneshot, watch, Mutex, Notify, Semaphore};
    use tokio::time::{sleep, timeout, Duration};

    use super::{
        assistant_message_text, provider_assistant_suffix_for_latest_request,
        ProviderNoConsumableOutput, ProviderRequestPreparationFailure, ProviderStreamFailure,
        ProviderTerminalFailure, CONTINUATION_TRIGGER,
    };
    use crate::agent::fs::LocalFsMemoryStore;
    use crate::api::{
        estimate_provider_request_context_tokens, AgentTurnLoop, CompletedSessionTurnMessage,
        ContextUsageSource, ModelContextSource, ProviderAdapter, ProviderEvent,
        ProviderReplayState, ProviderRequest, ProviderRequestObserver, ProviderResponse,
        ProviderRuntimeChainId, ProviderStop, SessionTurn, SessionTurnContentBlock,
        SessionTurnContextAppender, SessionTurnEvent, SessionTurnEventRecorder, SessionTurnHooks,
        SessionTurnInterrupted, SessionTurnMessage, SessionTurnPreflight, SessionTurnRequest,
        ToolBoundaryControl, ToolCallSkipReason, ToolExecutionOutcome,
    };
    use crate::attachment::AttachmentLimits;
    use crate::config::ToolConfig;
    use crate::tool::{ToolDispatchContext, ToolProgressUpdate, ToolRegistry};

    struct FakeProvider {
        responses: Mutex<VecDeque<ProviderResponse>>,
        requests: Mutex<Vec<ProviderRequest>>,
        events: Vec<ProviderEvent>,
        emit_preflight_context_estimate: bool,
        cancel_after_response: Option<ToolBoundaryControl>,
    }

    struct ScriptedProviderAttempt {
        events: Vec<ProviderEvent>,
        result: Result<ProviderResponse, String>,
    }

    struct ScriptedProvider {
        attempts: Mutex<VecDeque<ScriptedProviderAttempt>>,
        requests: Mutex<Vec<ProviderRequest>>,
    }

    struct TerminalFailureProvider {
        requests: Mutex<Vec<ProviderRequest>>,
    }

    struct FallbackTerminalFailureProvider {
        requests: Mutex<Vec<ProviderRequest>>,
    }

    struct ZeroTextRecoverableProvider {
        requests: Mutex<Vec<ProviderRequest>>,
        failure_kind: ZeroTextFailureKind,
        discarded_chains: AtomicUsize,
    }

    #[derive(Clone, Copy)]
    enum ContinuationWalTimeoutMode {
        Streaming,
        NonStreamingFallback,
    }

    struct ContinuationWalTimeoutProvider {
        mode: ContinuationWalTimeoutMode,
        transport_requests: Mutex<Vec<bool>>,
    }

    struct BlockingContinuationWalPreflight {
        provider_request_ready_calls: Arc<AtomicUsize>,
    }

    struct BlockingInitialWalPreflight;

    struct BlockingResponseWalPreflight;

    #[derive(Clone, Copy)]
    enum ZeroTextFailureKind {
        StreamFailure,
        NoConsumableOutput,
        Ordinary,
        Timeout,
    }

    struct ReplaceProcessToolResultPreflight {
        calls: usize,
        tool_use_id: String,
        replacement: String,
    }

    struct ClearingFileReadPreflight {
        tools: Arc<ToolRegistry>,
        session_id: crate::claim::SessionId,
        cleared: bool,
    }

    struct ExternalizingTextAttachmentPreflight;

    #[derive(Default)]
    struct RecordingContextRecoveryPreflight {
        requested: bool,
        applied: usize,
    }

    #[async_trait]
    impl SessionTurnPreflight for RecordingContextRecoveryPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            _provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            if self.requested {
                self.requested = false;
                self.applied = self.applied.saturating_add(1);
            }
            Ok(())
        }

        fn request_context_window_recovery(
            &mut self,
            _assistant_marker: &SessionTurnMessage,
        ) -> anyhow::Result<()> {
            self.requested = true;
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for ReplaceProcessToolResultPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            self.calls = self.calls.saturating_add(1);
            if self.calls != 2 {
                return Ok(());
            }
            let content = provider_messages
                .iter_mut()
                .rev()
                .flat_map(|message| message.content.iter_mut().rev())
                .find_map(|block| match block {
                    SessionTurnContentBlock::ToolResult {
                        tool_use_id,
                        content,
                    } if tool_use_id == &self.tool_use_id => Some(content),
                    _ => None,
                })
                .ok_or_else(|| anyhow::anyhow!("missing process tool_result to replace"))?;
            *content = self.replacement.clone();
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for ClearingFileReadPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            _provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            if !self.cleared {
                self.tools.clear_file_read_state(&self.session_id).await;
                self.cleared = true;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for ExternalizingTextAttachmentPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            for message in provider_messages {
                for block in &mut message.content {
                    if matches!(
                        block,
                        SessionTurnContentBlock::Text { text }
                            if text.starts_with("Attached file: attached.txt\n")
                    ) {
                        *block = SessionTurnContentBlock::text(
                            "<externalized_compaction_asset>read with file_read</externalized_compaction_asset>",
                        );
                    }
                }
            }
            Ok(())
        }
    }

    impl ScriptedProvider {
        fn new(attempts: Vec<ScriptedProviderAttempt>) -> Self {
            Self {
                attempts: Mutex::new(VecDeque::from(attempts)),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ProviderAdapter for ScriptedProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            let attempt = self
                .attempts
                .lock()
                .await
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("scripted provider attempt exhausted"))?;
            for event in attempt.events {
                emit(event);
            }
            attempt.result.map_err(anyhow::Error::msg)
        }
    }

    #[async_trait]
    impl ProviderAdapter for TerminalFailureProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            emit(ProviderEvent::AssistantTextDelta {
                text: "partial".into(),
            });
            Err(ProviderTerminalFailure::new("provider refused request").into())
        }
    }

    #[async_trait]
    impl ProviderAdapter for FallbackTerminalFailureProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            let is_streaming = request.stream;
            self.requests.lock().await.push(request);
            if is_streaming {
                emit(ProviderEvent::AssistantTextDelta {
                    text: "partial".into(),
                });
                return Err(anyhow::anyhow!("stream transport failed"));
            }
            Err(ProviderTerminalFailure::new("provider refused request").into())
        }
    }

    #[async_trait]
    impl ProviderAdapter for ZeroTextRecoverableProvider {
        fn request_timeout(&self) -> Option<Duration> {
            matches!(self.failure_kind, ZeroTextFailureKind::Timeout)
                .then_some(Duration::from_millis(1))
        }

        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            let is_streaming = request.stream;
            self.requests.lock().await.push(request);
            if !is_streaming {
                return Ok(response(
                    vec![SessionTurnContentBlock::text("fallback complete")],
                    ProviderStop::Done,
                ));
            }
            match self.failure_kind {
                ZeroTextFailureKind::StreamFailure => {
                    Err(ProviderStreamFailure::new("stream ended before terminal event").into())
                }
                ZeroTextFailureKind::NoConsumableOutput => Err(ProviderNoConsumableOutput::new(
                    "provider returned no consumable output",
                )
                .into()),
                ZeroTextFailureKind::Ordinary => {
                    Err(anyhow::anyhow!("ordinary zero-text provider error"))
                }
                ZeroTextFailureKind::Timeout => {
                    sleep(Duration::from_secs(60)).await;
                    Ok(response(
                        vec![SessionTurnContentBlock::text("late streaming response")],
                        ProviderStop::Done,
                    ))
                }
            }
        }

        async fn discard_runtime_chain(&self, _chain_id: ProviderRuntimeChainId) {
            self.discarded_chains.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl ProviderAdapter for ContinuationWalTimeoutProvider {
        async fn send(
            &self,
            _request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            anyhow::bail!("test adapter must use observed send")
        }

        async fn send_with_request_observer(
            &self,
            request: ProviderRequest,
            emit: &mut (dyn FnMut(ProviderEvent) + Send),
            observer: &mut (dyn ProviderRequestObserver + Send),
        ) -> anyhow::Result<ProviderResponse> {
            observer.before_provider_request(&request.messages).await?;
            let request_is_streaming = request.stream;
            self.transport_requests
                .lock()
                .await
                .push(request_is_streaming);

            if matches!(self.mode, ContinuationWalTimeoutMode::NonStreamingFallback)
                && request_is_streaming
            {
                emit(ProviderEvent::AssistantTextDelta {
                    text: "stream partial".into(),
                });
                return Err(ProviderStreamFailure::new("force non-streaming fallback").into());
            }

            emit(ProviderEvent::AssistantTextDelta {
                text: "continuation partial".into(),
            });
            let mut continued = request.messages;
            continued.push(SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::text("continuation partial")],
                provider_replay: Some(ProviderReplayState::OpenAiResponses {
                    model: Some("test-model".into()),
                    items: vec![
                        json!({"type":"message", "id":"partial"}),
                        json!({"type":"message", "id":"continue"}),
                    ],
                }),
            });
            observer.before_provider_request(&continued).await?;
            self.transport_requests
                .lock()
                .await
                .push(request_is_streaming);
            Ok(response(
                vec![SessionTurnContentBlock::text("must not complete")],
                ProviderStop::Done,
            ))
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for BlockingContinuationWalPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            _provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn provider_request_ready(
            &mut self,
            _provider_messages: &[SessionTurnMessage],
            _canonical_tail_count: usize,
        ) -> anyhow::Result<()> {
            let call = self
                .provider_request_ready_calls
                .fetch_add(1, Ordering::SeqCst)
                .saturating_add(1);
            if call >= 2 {
                pending::<()>().await;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for BlockingInitialWalPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            _provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn provider_request_ready(
            &mut self,
            _provider_messages: &[SessionTurnMessage],
            _canonical_tail_count: usize,
        ) -> anyhow::Result<()> {
            pending::<()>().await;
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for BlockingResponseWalPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            _provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn provider_response_ready(
            &mut self,
            _provider_messages: &[SessionTurnMessage],
            _canonical_tail_count: usize,
        ) -> anyhow::Result<()> {
            pending::<()>().await;
            Ok(())
        }
    }

    impl FakeProvider {
        fn new(responses: Vec<ProviderResponse>) -> Self {
            Self {
                responses: Mutex::new(VecDeque::from(responses)),
                requests: Mutex::new(Vec::new()),
                events: Vec::new(),
                emit_preflight_context_estimate: true,
                cancel_after_response: None,
            }
        }

        fn with_events(mut self, events: Vec<ProviderEvent>) -> Self {
            self.events = events;
            self
        }

        fn without_preflight_context_estimate(mut self) -> Self {
            self.emit_preflight_context_estimate = false;
            self
        }

        fn with_cancel_after_response(mut self, control: ToolBoundaryControl) -> Self {
            self.cancel_after_response = Some(control);
            self
        }
    }

    #[async_trait]
    impl ProviderAdapter for FakeProvider {
        fn emit_preflight_context_estimate(&self) -> bool {
            self.emit_preflight_context_estimate
        }

        async fn send(
            &self,
            request: ProviderRequest,
            emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            for event in &self.events {
                emit(event.clone());
            }
            let response = self
                .responses
                .lock()
                .await
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("fake provider response exhausted"))?;
            if let Some(control) = &self.cancel_after_response {
                control.cancel(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
            }
            Ok(response)
        }
    }

    struct SlowProvider {
        requests: Mutex<Vec<ProviderRequest>>,
        started: Mutex<Option<oneshot::Sender<()>>>,
    }

    struct BlockingSubagentExecutor {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct PauseBeforeDispatchReservation {
        entered: StdMutex<Option<oneshot::Sender<()>>>,
        release: StdMutex<Option<oneshot::Receiver<()>>>,
    }

    #[derive(Clone)]
    struct ParallelFetchState {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        started: Arc<Mutex<Vec<String>>>,
        started_tx: watch::Sender<usize>,
        gates: Arc<BTreeMap<String, Arc<Semaphore>>>,
    }

    struct ParallelFetchServer {
        base_url: String,
        state: ParallelFetchState,
        started_rx: watch::Receiver<usize>,
        task: tokio::task::JoinHandle<()>,
    }

    impl ParallelFetchServer {
        async fn start(names: &[&str]) -> Self {
            use axum::routing::get;
            use axum::Router;
            use tokio::net::TcpListener;

            let mut gates = BTreeMap::new();
            for name in names {
                gates.insert((*name).to_string(), Arc::new(Semaphore::new(0)));
            }
            let (started_tx, started_rx) = watch::channel(0usize);
            let state = ParallelFetchState {
                active: Arc::new(AtomicUsize::new(0)),
                max_active: Arc::new(AtomicUsize::new(0)),
                started: Arc::new(Mutex::new(Vec::new())),
                started_tx,
                gates: Arc::new(gates),
            };
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("parallel fetch test listener should bind");
            let base_url = format!(
                "http://127.0.0.1:{}",
                listener
                    .local_addr()
                    .expect("parallel fetch test listener should have an address")
                    .port()
            );
            let app = Router::new()
                .route("/{name}", get(parallel_fetch_handler))
                .with_state(state.clone());
            let task = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            Self {
                base_url,
                state,
                started_rx,
                task,
            }
        }

        fn url(&self, name: &str) -> String {
            format!("{}/{}", self.base_url, name)
        }

        fn release(&self, name: &str) {
            self.state
                .gates
                .get(name)
                .expect("test route should have a release gate")
                .add_permits(1);
        }

        async fn wait_for_starts(&mut self, expected: usize) {
            loop {
                let current = *self.started_rx.borrow_and_update();
                if current >= expected {
                    return;
                }
                self.started_rx
                    .changed()
                    .await
                    .expect("parallel fetch server should remain available");
            }
        }

        fn started_count(&self) -> usize {
            *self.started_rx.borrow()
        }

        fn max_active(&self) -> usize {
            self.state.max_active.load(Ordering::SeqCst)
        }
    }

    impl Drop for ParallelFetchServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn parallel_fetch_handler(
        axum::extract::State(state): axum::extract::State<ParallelFetchState>,
        axum::extract::Path(name): axum::extract::Path<String>,
    ) -> (axum::http::StatusCode, String) {
        let active = state
            .active
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        state.max_active.fetch_max(active, Ordering::SeqCst);
        let started_count = {
            let mut started = state.started.lock().await;
            started.push(name.clone());
            started.len()
        };
        let _ = state.started_tx.send(started_count);

        let Some(gate) = state.gates.get(&name) else {
            state.active.fetch_sub(1, Ordering::SeqCst);
            return (
                axum::http::StatusCode::NOT_FOUND,
                "unexpected test route".into(),
            );
        };
        let Ok(permit) = Arc::clone(gate).acquire_owned().await else {
            state.active.fetch_sub(1, Ordering::SeqCst);
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "test route gate closed".into(),
            );
        };
        drop(permit);
        state.active.fetch_sub(1, Ordering::SeqCst);
        (axum::http::StatusCode::OK, name)
    }

    fn started_tool_ids(events: &[SessionTurnEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|event| match event {
                SessionTurnEvent::ToolCallStarted { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    fn completed_tool_ids(events: &[SessionTurnEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|event| match event {
                SessionTurnEvent::ToolCallCompleted { id, .. } => Some(id.clone()),
                _ => None,
            })
            .collect()
    }

    #[async_trait]
    impl super::ToolDispatchReservationHook for PauseBeforeDispatchReservation {
        async fn before_try_reserve_dispatch(&self) {
            let entered = match self.entered.lock() {
                Ok(mut entered) => entered.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            let release = match self.release.lock() {
                Ok(mut release) => release.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(release) = release {
                let _ = release.await;
            }
        }
    }

    #[async_trait]
    impl crate::delegation::DelegationExecutor for BlockingSubagentExecutor {
        async fn execute(
            &self,
            _context: crate::delegation::DelegationExecutionContext,
            _progress: crate::delegation::DelegationProgressSink,
        ) -> Result<
            crate::delegation::DelegationExecutionOutcome,
            crate::delegation::DelegationExecutionError,
        > {
            self.started.notify_one();
            self.release.notified().await;
            Ok(crate::delegation::DelegationExecutionOutcome {
                summary: "released".into(),
                changed_files: Vec::new(),
                artifacts: Vec::new(),
            })
        }
    }

    #[async_trait]
    impl ProviderAdapter for SlowProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().await.push(request);
            if let Some(started) = self.started.lock().await.take() {
                let _ = started.send(());
            }
            sleep(Duration::from_secs(60)).await;
            Ok(response(
                vec![SessionTurnContentBlock::text("too late")],
                ProviderStop::Done,
            ))
        }
    }

    enum BlockingRecordTarget {
        AssistantCompleted,
        ToolCompleted,
        ToolSkipped,
    }

    struct BlockingCompletedRecorder {
        target: BlockingRecordTarget,
        completed_seen: Option<oneshot::Sender<()>>,
        release_completed: Option<oneshot::Receiver<()>>,
    }

    struct BlockingStartedRecorder {
        started_seen: Option<oneshot::Sender<()>>,
        release_started: Option<oneshot::Receiver<()>>,
    }

    struct CancelOnFallbackFailureRecorder {
        control: ToolBoundaryControl,
        events: Vec<SessionTurnEvent>,
    }

    enum FailingRecorderTarget {
        Started(&'static str),
        Completed(&'static str),
        Skipped(&'static str),
    }

    struct FailingRecorder {
        target: FailingRecorderTarget,
        failed: bool,
    }

    struct RecordingCompletedMessageRecorder {
        messages: Vec<CompletedSessionTurnMessage>,
    }

    struct StaticContextAppender {
        messages: Vec<SessionTurnMessage>,
    }

    struct ChangingContextAppender {
        observations: usize,
    }

    #[derive(Default)]
    struct ContextAwarePreflight {
        latest_background_texts: Vec<String>,
    }

    #[derive(Default)]
    struct ReplacingContextPreflight {
        replaced: bool,
    }

    #[async_trait]
    impl SessionTurnEventRecorder for RecordingCompletedMessageRecorder {
        async fn record(&mut self, _event: SessionTurnEvent) -> anyhow::Result<()> {
            Ok(())
        }

        async fn record_completed_message(
            &mut self,
            message: &CompletedSessionTurnMessage,
        ) -> anyhow::Result<()> {
            self.messages.push(message.clone());
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnContextAppender for StaticContextAppender {
        async fn observe_context(
            &mut self,
            _provider_messages: &[SessionTurnMessage],
        ) -> anyhow::Result<Vec<SessionTurnMessage>> {
            Ok(self.messages.clone())
        }
    }

    #[async_trait]
    impl SessionTurnContextAppender for ChangingContextAppender {
        async fn observe_context(
            &mut self,
            _provider_messages: &[SessionTurnMessage],
        ) -> anyhow::Result<Vec<SessionTurnMessage>> {
            self.observations = self.observations.saturating_add(1);
            let state = if self.observations < 3 {
                "running"
            } else {
                "completed"
            };
            Ok(vec![SessionTurnMessage::model_context(
                ModelContextSource::BackgroundProcess,
                format!("<background_processes>state={state}</background_processes>"),
            )])
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for ContextAwarePreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            let text = provider_messages
                .iter()
                .rev()
                .find_map(|message| {
                    let (source, _, text) = message.model_context_snapshot()?;
                    (*source == ModelContextSource::BackgroundProcess).then(|| text.to_string())
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("preflight 应看到本次已经冻结的 background snapshot")
                })?;
            self.latest_background_texts.push(text);
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnPreflight for ReplacingContextPreflight {
        async fn before_provider_request(
            &mut self,
            _system_prompt: &mut String,
            provider_messages: &mut Vec<SessionTurnMessage>,
            _emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        ) -> anyhow::Result<()> {
            if !self.history_replacement_expected("", provider_messages) {
                return Ok(());
            }
            let baseline_start = provider_messages.len().saturating_sub(2);
            *provider_messages = provider_messages[baseline_start..].to_vec();
            self.replaced = true;
            Ok(())
        }

        fn history_replacement_expected(
            &self,
            _system_prompt: &str,
            provider_messages: &[SessionTurnMessage],
        ) -> bool {
            !self.replaced
                && provider_messages.iter().rev().any(|message| {
                    message
                        .model_context_snapshot()
                        .is_some_and(|(source, _, text)| {
                            *source == ModelContextSource::BackgroundProcess
                                && text.contains("state=completed")
                        })
                })
        }

        fn take_history_replaced_since_last_check(&mut self) -> bool {
            std::mem::take(&mut self.replaced)
        }
    }

    #[async_trait]
    impl SessionTurnEventRecorder for BlockingCompletedRecorder {
        async fn record(&mut self, event: SessionTurnEvent) -> anyhow::Result<()> {
            let matched = match self.target {
                BlockingRecordTarget::AssistantCompleted => {
                    matches!(event, SessionTurnEvent::AssistantMessageCompleted { .. })
                }
                BlockingRecordTarget::ToolCompleted => {
                    matches!(event, SessionTurnEvent::ToolCallCompleted { .. })
                }
                BlockingRecordTarget::ToolSkipped => {
                    matches!(event, SessionTurnEvent::ToolCallSkipped { .. })
                }
            };
            if matched {
                if let Some(completed_seen) = self.completed_seen.take() {
                    let _ = completed_seen.send(());
                }
                if let Some(release_completed) = self.release_completed.take() {
                    release_completed
                        .await
                        .map_err(|_| anyhow::anyhow!("completed recorder release dropped"))?;
                }
            }
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnEventRecorder for BlockingStartedRecorder {
        async fn record(&mut self, event: SessionTurnEvent) -> anyhow::Result<()> {
            if matches!(event, SessionTurnEvent::ToolCallStarted { .. }) {
                if let Some(started_seen) = self.started_seen.take() {
                    let _ = started_seen.send(());
                }
                if let Some(release_started) = self.release_started.take() {
                    release_started
                        .await
                        .map_err(|_| anyhow::anyhow!("started recorder release dropped"))?;
                }
            }
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnEventRecorder for CancelOnFallbackFailureRecorder {
        async fn record(&mut self, event: SessionTurnEvent) -> anyhow::Result<()> {
            if matches!(
                event,
                SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
            ) {
                self.control
                    .cancel(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
            }
            self.events.push(event);
            Ok(())
        }
    }

    #[async_trait]
    impl SessionTurnEventRecorder for FailingRecorder {
        async fn record(&mut self, event: SessionTurnEvent) -> anyhow::Result<()> {
            let matches_target = match (&self.target, &event) {
                (
                    FailingRecorderTarget::Started(expected_id),
                    SessionTurnEvent::ToolCallStarted { id, .. },
                ) => id == expected_id,
                (
                    FailingRecorderTarget::Completed(expected_id),
                    SessionTurnEvent::ToolCallCompleted { id, .. },
                ) => id == expected_id,
                (
                    FailingRecorderTarget::Skipped(expected_id),
                    SessionTurnEvent::ToolCallSkipped { id, .. },
                ) => id == expected_id,
                _ => false,
            };
            if !self.failed && matches_target {
                self.failed = true;
                anyhow::bail!("intentional durable recorder failure")
            }
            Ok(())
        }
    }

    fn response(content: Vec<SessionTurnContentBlock>, stop: ProviderStop) -> ProviderResponse {
        ProviderResponse {
            assistant_message: SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content,
            },
            stop,
        }
    }

    fn anthropic_response(
        content: Vec<SessionTurnContentBlock>,
        raw_content: Vec<Value>,
        stop: ProviderStop,
    ) -> ProviderResponse {
        ProviderResponse {
            assistant_message: SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: Some(ProviderReplayState::AnthropicMessages {
                    model: "test-model".into(),
                    messages: vec![json!({
                        "role": "assistant",
                        "content": raw_content,
                    })],
                }),
                content,
            },
            stop,
        }
    }

    fn request() -> SessionTurnRequest {
        SessionTurnRequest {
            system_prompt: "system".into(),
            history: vec![SessionTurnMessage::assistant_text("prior")],
            user_text: "hello".into(),
            user_attachments: vec![],
            skill_instructions: vec![],
            current_session_id: None,
            current_turn_id: None,
        }
    }

    fn fixed_now() -> DateTime<Utc> {
        "2026-06-29T12:34:56Z".parse().unwrap()
    }

    fn scripted_failure(events: Vec<ProviderEvent>, error: &str) -> ScriptedProviderAttempt {
        ScriptedProviderAttempt {
            events,
            result: Err(error.into()),
        }
    }

    fn scripted_success(text: &str) -> ScriptedProviderAttempt {
        ScriptedProviderAttempt {
            events: vec![ProviderEvent::AssistantMessageCompleted { text: text.into() }],
            result: Ok(response(
                vec![SessionTurnContentBlock::text(text)],
                ProviderStop::Done,
            )),
        }
    }

    fn text_blocks(message: &SessionTurnMessage) -> Vec<&str> {
        message
            .content
            .iter()
            .filter_map(|block| match block {
                SessionTurnContentBlock::Text { text } => Some(text.as_str()),
                SessionTurnContentBlock::SkillInstructions { .. }
                | SessionTurnContentBlock::ModelContext { .. } => None,
                SessionTurnContentBlock::Image { .. }
                | SessionTurnContentBlock::Document { .. }
                | SessionTurnContentBlock::ToolUse { .. }
                | SessionTurnContentBlock::ToolResult { .. } => None,
            })
            .collect()
    }

    fn non_context_messages(turn: &SessionTurn) -> Vec<&SessionTurnMessage> {
        turn.messages
            .iter()
            .filter(|message| message.model_context_snapshot().is_none())
            .map(|message| &message.message)
            .collect()
    }

    fn tool_loop(provider: Arc<dyn ProviderAdapter>) -> AgentTurnLoop {
        let tools = ToolRegistry::new(&ToolConfig::default()).unwrap();
        tool_loop_with_tools(provider, Arc::new(tools))
    }

    fn tool_loop_with_tools(
        provider: Arc<dyn ProviderAdapter>,
        tools: Arc<ToolRegistry>,
    ) -> AgentTurnLoop {
        AgentTurnLoop::new(provider, tools, 1024).with_max_tool_loop_turns(4)
    }

    fn tool_use(id: &str, name: &str, input: Value) -> SessionTurnContentBlock {
        SessionTurnContentBlock::ToolUse {
            id: id.into(),
            name: name.into(),
            input,
        }
    }

    fn tool_result_content(message: &SessionTurnMessage, tool_use_id: &str) -> Value {
        let content = message
            .content
            .iter()
            .find_map(|block| match block {
                SessionTurnContentBlock::ToolResult {
                    tool_use_id: id,
                    content,
                } if id == tool_use_id => Some(content.as_str()),
                _ => None,
            })
            .unwrap();
        serde_json::from_str(content).unwrap()
    }

    async fn wait_for_process_to_become_terminal(
        tools: &ToolRegistry,
        context: &ToolDispatchContext,
        process_id: &str,
    ) {
        timeout(Duration::from_secs(10), async {
            loop {
                let process_list = tools
                    .dispatch_with_context("process_list", json!({}), context.clone())
                    .await
                    .expect("process_list should remain available while awaiting test process");
                let is_live = process_list.output["processes"]
                    .as_array()
                    .expect("process_list processes should be an array")
                    .iter()
                    .any(|process| process["process_id"] == process_id);
                if !is_live {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("test process should become terminal within 10 seconds");
    }

    #[test]
    fn delegation_tool_use_inputs_are_preserved_for_canonical_transcript() {
        let message = SessionTurnMessage {
            role: "assistant".into(),
            provider_replay: None,
            content: vec![tool_use(
                "toolu_1",
                "create_subagent",
                json!({
                    "title": "deep scan",
                    "objective": "very private objective that should not be copied verbatim",
                }),
            )],
        };

        let SessionTurnContentBlock::ToolUse { input, .. } = &message.content[0] else {
            panic!("expected tool use");
        };

        assert_eq!(input["title"], "deep scan");
        assert!(input.to_string().contains("very private objective"));
        assert!(!input
            .to_string()
            .contains("details_omitted_from_transcript"));
    }

    #[test]
    fn delegation_tool_results_are_preserved_for_canonical_transcript() {
        let raw_content = json!({
            "ok": true,
            "output": {
                "summary": {
                    "id": "subagent_11111111",
                    "status": "completed",
                    "title": "scan",
                },
                "result_markdown": "large private delegation result",
                "truncated": false,
            }
        })
        .to_string();
        let canonical: Value = serde_json::from_str(&raw_content).unwrap();

        assert!(canonical.to_string().contains("subagent_11111111"));
        assert!(canonical
            .to_string()
            .contains("large private delegation result"));
        assert!(!canonical
            .to_string()
            .contains("details_omitted_from_transcript"));
        let preview = super::tool_output_preview(
            "read_subagent",
            &json!({
                "summary": {
                    "id": "subagent_11111111",
                    "status": "completed",
                    "title": "scan",
                },
                "result_markdown": "large private delegation result",
                "truncated": false,
            }),
        );

        assert!(preview.contains("large private delegation result"));
    }

    #[tokio::test]
    async fn delegation_tool_error_preview_uses_regular_tool_error() {
        let tools = ToolRegistry::new(&ToolConfig::default()).unwrap();

        let executed = super::execute_tool_use(
            &tools,
            "read_subagent",
            json!({"id": "subagent_11111111", "private": "keep out"}),
            ToolDispatchContext::default(),
            false,
            None,
        )
        .await
        .expect("ordinary tool errors should become completed dispatch failures");

        assert!(!executed.output_preview.is_empty());
        let canonical: Value = serde_json::from_str(&executed.canonical_content).unwrap();
        assert_eq!(canonical["ok"], false);
        assert_eq!(canonical["outcome"]["kind"], "dispatch_failure");
        assert!(canonical["error"]
            .as_str()
            .is_some_and(|error| !error.is_empty()));
        assert!(!executed
            .canonical_content
            .contains("details_omitted_from_transcript"));
        assert_eq!(executed.content, executed.canonical_content);
    }

    #[tokio::test]
    async fn file_change_is_emitted_but_never_returned_to_model() {
        let dir = tempfile::tempdir().unwrap();
        let tools = ToolRegistry::new(&ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..ToolConfig::default()
        })
        .unwrap();

        let executed = super::execute_tool_use(
            &tools,
            "file_write",
            json!({"path": "new.txt", "content": "hello\n"}),
            ToolDispatchContext::default(),
            false,
            None,
        )
        .await
        .expect("file_write should not be interrupted");

        assert!(executed.file_change.is_some());
        assert!(!executed
            .canonical_content
            .contains(crate::tool::diff::FILE_CHANGE_KEY));
        assert!(!executed
            .output_preview
            .contains(crate::tool::diff::FILE_CHANGE_KEY));
    }

    #[test]
    fn delegation_tool_journal_input_preview_uses_regular_preview() {
        let raw = json!({
            "title": "scan",
            "objective": "private objective should stay out of journal",
        });

        let (preview, truncated) = super::tool_input_preview("create_subagent", &raw, 256);
        let summary = super::tool_started_summary("create_subagent", &raw);

        assert!(!truncated);
        assert!(preview.contains("scan"));
        assert!(preview.contains("private objective"));
        assert!(summary.contains("scan"));
        assert!(summary.contains("private objective"));
    }

    #[test]
    fn consult_router_preview_uses_stable_counts() {
        let preview = super::tool_output_preview(
            "consult_router",
            &json!({
                "mode": "query",
                "candidate_claims": [{"id": "claim_00000001"}, {"id": "claim_00000002"}],
                "disputes": [{"id": "dispute_00000001"}],
            }),
        );

        assert_eq!(preview, "claims=2 disputes=1");
    }

    #[test]
    fn consult_router_preview_handles_overview() {
        let preview = super::tool_output_preview(
            "consult_router",
            &json!({
                "mode": "overview",
                "scopes": [{"scope": "router/tool"}, {"scope": "agent/session"}],
            }),
        );

        assert_eq!(preview, "scopes=2");
    }

    #[test]
    fn tool_journal_previews_use_configured_limits() {
        let (input_preview, input_truncated) =
            super::tool_input_preview("file_read", &json!({"value":"abcdef"}), 10);
        assert!(input_truncated);
        assert_eq!(input_preview.chars().count(), 13);
        assert!(input_preview.ends_with("..."));

        let (output_preview, output_truncated) = super::tool_journal_output_preview("abcdef", 3);
        assert!(output_truncated);
        assert_eq!(output_preview, "abc...");
    }

    fn tiny_png_bytes() -> Vec<u8> {
        let img = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
        let mut out = Vec::new();
        img.write_to(&mut std::io::Cursor::new(&mut out), image::ImageFormat::Png)
            .expect("编码测试 PNG 不应失败");
        out
    }

    struct RejectingProvider;

    #[async_trait]
    impl ProviderAdapter for RejectingProvider {
        async fn send(
            &self,
            _request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            // 模拟上游 provider / model 拒收媒体块（如 image content not supported）
            anyhow::bail!(
                "400 invalid_request_error: image content blocks are not supported by this model"
            )
        }
    }

    #[tokio::test]
    async fn upstream_media_rejection_error_is_passed_through_verbatim() {
        let turn_loop = tool_loop(Arc::new(RejectingProvider));

        let err = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap_err();

        // 上游核心错误信息必须保留，不允许静默降级
        assert!(err
            .to_string()
            .contains("image content blocks are not supported"));
    }

    #[tokio::test(start_paused = true)]
    async fn partial_stream_failure_falls_back_to_non_streaming_and_replaces_response() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "partial".into(),
                }],
                "stream closed before message_stop",
            ),
            scripted_success("complete replacement"),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(non_context_messages(&turn).len(), 2);
        assert_eq!(
            non_context_messages(&turn)[1],
            &SessionTurnMessage::assistant_text("complete replacement")
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].system_prompt, requests[1].system_prompt);
        assert_eq!(requests[0].messages, requests[1].messages);
        assert_eq!(requests[0].tools, requests[1].tools);
        assert!(requests[0].stream);
        assert_eq!(requests[0].retry_count_override, None);
        assert!(!requests[1].stream);
        assert_eq!(requests[1].retry_count_override, Some(0));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted {
                attempt: 1,
                max_attempts: 5,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded {
                attempt: 1,
                max_attempts: 5,
                text,
            } if text == "complete replacement"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::AssistantMessageCompleted { text }
                if text == "complete replacement"
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_text_stream_failure_falls_back_to_non_streaming() {
        let provider = Arc::new(ZeroTextRecoverableProvider {
            requests: Mutex::new(Vec::new()),
            failure_kind: ZeroTextFailureKind::StreamFailure,
            discarded_chains: AtomicUsize::new(0),
        });
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(
            non_context_messages(&turn)[1],
            &SessionTurnMessage::assistant_text("fallback complete")
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
        drop(requests);
        assert_eq!(provider.discarded_chains.load(Ordering::SeqCst), 1);
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { attempt: 1, .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_text_stream_timeout_falls_back_to_non_streaming() {
        let provider = Arc::new(ZeroTextRecoverableProvider {
            requests: Mutex::new(Vec::new()),
            failure_kind: ZeroTextFailureKind::Timeout,
            discarded_chains: AtomicUsize::new(0),
        });
        let turn_loop = tool_loop(provider.clone());

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        assert_eq!(
            non_context_messages(&turn)[1],
            &SessionTurnMessage::assistant_text("fallback complete")
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
    }

    #[tokio::test(start_paused = true)]
    async fn no_consumable_output_falls_back_without_visible_text() {
        let provider = Arc::new(ZeroTextRecoverableProvider {
            requests: Mutex::new(Vec::new()),
            failure_kind: ZeroTextFailureKind::NoConsumableOutput,
            discarded_chains: AtomicUsize::new(0),
        });
        let turn_loop = tool_loop(provider.clone());

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        assert_eq!(
            non_context_messages(&turn)[1],
            &SessionTurnMessage::assistant_text("fallback complete")
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
    }

    #[tokio::test]
    async fn ordinary_zero_text_error_does_not_enter_fallback() {
        let provider = Arc::new(ZeroTextRecoverableProvider {
            requests: Mutex::new(Vec::new()),
            failure_kind: ZeroTextFailureKind::Ordinary,
            discarded_chains: AtomicUsize::new(0),
        });
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("ordinary zero-text provider error"));
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                | SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn initial_request_wal_timeout_prevents_provider_send() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("must not be sent")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone());
        let mut preflight = BlockingInitialWalPreflight;

        let error = turn_loop
            .run_session_turn_with_hooks(request(), &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .expect_err("timed-out request WAL must reject the turn");

        assert!(error
            .downcast_ref::<ProviderRequestPreparationFailure>()
            .is_some());
        assert!(error.to_string().contains("请求状态保存超时"));
        assert!(provider.requests.lock().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn final_response_wal_timeout_rejects_completed_turn() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("complete but not durable")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone());
        let mut preflight = BlockingResponseWalPreflight;

        let error = turn_loop
            .run_session_turn_with_hooks(request(), &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .expect_err("timed-out response WAL must reject the turn");

        assert!(error
            .downcast_ref::<ProviderRequestPreparationFailure>()
            .is_some());
        assert!(error.to_string().contains("响应状态保存超时"));
        assert_eq!(provider.requests.lock().await.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn streaming_continuation_wal_timeout_is_preparation_failure_without_fallback() {
        let provider = Arc::new(ContinuationWalTimeoutProvider {
            mode: ContinuationWalTimeoutMode::Streaming,
            transport_requests: Mutex::new(Vec::new()),
        });
        let ready_calls = Arc::new(AtomicUsize::new(0));
        let mut preflight = BlockingContinuationWalPreflight {
            provider_request_ready_calls: Arc::clone(&ready_calls),
        };
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn_with_hooks(
                request(),
                &mut |event| events.push(event),
                None,
                None,
                Some(&mut preflight),
            )
            .await
            .expect_err("timed-out continuation WAL must reject the turn");

        assert!(error
            .downcast_ref::<ProviderRequestPreparationFailure>()
            .is_some());
        assert!(error.to_string().contains("请求状态保存超时"));
        assert_eq!(ready_calls.load(Ordering::SeqCst), 2);
        assert_eq!(*provider.transport_requests.lock().await, vec![true]);
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                | SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn non_streaming_continuation_wal_timeout_stops_fallback_attempts() {
        let provider = Arc::new(ContinuationWalTimeoutProvider {
            mode: ContinuationWalTimeoutMode::NonStreamingFallback,
            transport_requests: Mutex::new(Vec::new()),
        });
        let ready_calls = Arc::new(AtomicUsize::new(0));
        let mut preflight = BlockingContinuationWalPreflight {
            provider_request_ready_calls: Arc::clone(&ready_calls),
        };
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn_with_hooks(
                request(),
                &mut |event| events.push(event),
                None,
                None,
                Some(&mut preflight),
            )
            .await
            .expect_err("timed-out fallback WAL must reject the turn");

        assert!(error
            .downcast_ref::<ProviderRequestPreparationFailure>()
            .is_some());
        assert_eq!(ready_calls.load(Ordering::SeqCst), 2);
        assert_eq!(*provider.transport_requests.lock().await, vec![true, false]);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                ))
                .count(),
            1
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                | SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn fifth_non_streaming_fallback_can_succeed_after_four_failures() {
        let mut attempts = vec![scripted_failure(
            vec![ProviderEvent::AssistantTextDelta {
                text: "partial".into(),
            }],
            "stream failed",
        )];
        for index in 1..=4 {
            attempts.push(scripted_failure(
                Vec::new(),
                &format!("fallback {index} failed"),
            ));
        }
        attempts.push(scripted_success("fifth attempt completed"));
        let provider = Arc::new(ScriptedProvider::new(attempts));
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(
            non_context_messages(&turn)[1],
            &SessionTurnMessage::assistant_text("fifth attempt completed")
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 6);
        assert!(requests[0].stream);
        assert!(requests[1..]
            .iter()
            .all(|request| { !request.stream && request.retry_count_override == Some(0) }));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                ))
                .count(),
            4
        );
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { attempt: 5, .. }
        )));
    }

    #[tokio::test]
    async fn terminal_provider_failure_after_visible_delta_does_not_fallback() {
        let provider = Arc::new(TerminalFailureProvider {
            requests: Mutex::new(Vec::new()),
        });
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("provider refused request"));
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                | SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_provider_failure_stops_non_streaming_fallback_after_current_attempt() {
        let provider = Arc::new(FallbackTerminalFailureProvider {
            requests: Mutex::new(Vec::new()),
        });
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("provider refused request"));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
        drop(requests);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                ))
                .count(),
            1
        );
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptFailed {
                attempt: 1,
                error,
                ..
            } if error.contains("provider refused request")
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { attempt: 2, .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptFailed { attempt: 2, .. }
                | SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn context_stop_during_non_streaming_fallback_switches_to_forced_preflight() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "streaming-partial".into(),
                }],
                "stream transport failed",
            ),
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(anthropic_response(
                    vec![SessionTurnContentBlock::text("fallback-context-")],
                    vec![
                        json!({
                            "type":"thinking",
                            "thinking":"private-fallback-context",
                            "signature":"sig-fallback-context"
                        }),
                        json!({"type":"text", "text":"fallback-context-"}),
                    ],
                    ProviderStop::ContextWindowExceeded,
                )),
            },
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(anthropic_response(
                    vec![SessionTurnContentBlock::text("final")],
                    vec![json!({"type":"text", "text":"final"})],
                    ProviderStop::Done,
                )),
            },
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut preflight = RecordingContextRecoveryPreflight::default();
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn_with_hooks(
                request(),
                &mut |event| events.push(event),
                None,
                None,
                Some(&mut preflight),
            )
            .await
            .unwrap();

        assert_eq!(
            assistant_message_text(non_context_messages(&turn)[1]),
            "fallback-context-final"
        );
        assert_eq!(preflight.applied, 1);
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
        assert!(requests[2].stream);
        drop(requests);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                ))
                .count(),
            1
        );
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { attempt: 1, .. }
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { attempt: 2, .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptFailed { attempt: 2, .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn continuation_fallback_replacement_includes_prior_context_partial() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(anthropic_response(
                    vec![SessionTurnContentBlock::text("context-prefix-")],
                    vec![json!({"type":"text", "text":"context-prefix-"})],
                    ProviderStop::ContextWindowExceeded,
                )),
            },
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "failed-continuation-partial".into(),
                }],
                "continuation stream failed",
            ),
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(anthropic_response(
                    vec![SessionTurnContentBlock::text("final")],
                    vec![json!({"type":"text", "text":"final"})],
                    ProviderStop::Done,
                )),
            },
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut preflight = RecordingContextRecoveryPreflight::default();
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn_with_hooks(
                request(),
                &mut |event| events.push(event),
                None,
                None,
                Some(&mut preflight),
            )
            .await
            .unwrap();

        assert_eq!(
            assistant_message_text(non_context_messages(&turn)[1]),
            "context-prefix-final"
        );
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { text, .. }
                if text == "context-prefix-final"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { text, .. }
                if text == "final"
        )));
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 3);
        assert!(requests[0].stream);
        assert!(requests[1].stream);
        assert!(!requests[2].stream);
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_non_streaming_response_is_failed_before_next_attempt_succeeds() {
        let invalid_response = ProviderResponse {
            assistant_message: SessionTurnMessage::user_text("invalid role"),
            stop: ProviderStop::Done,
        };
        let provider = Arc::new(ScriptedProvider::new(vec![
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "partial".into(),
                }],
                "stream failed",
            ),
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(invalid_response),
            },
            scripted_success("validated replacement"),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(
            non_context_messages(&turn)[1],
            &SessionTurnMessage::assistant_text("validated replacement")
        );
        assert_eq!(provider.requests.lock().await.len(), 3);
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptFailed {
                attempt: 1,
                error,
                ..
            } if error.contains("assistant message 校验失败")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { attempt: 2, .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn max_tokens_non_streaming_response_is_failed_before_next_attempt_succeeds() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "partial".into(),
                }],
                "stream failed",
            ),
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(response(
                    vec![SessionTurnContentBlock::text("truncated fallback")],
                    ProviderStop::MaxTokens,
                )),
            },
            scripted_success("complete replacement"),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(
            non_context_messages(&turn)[1],
            &SessionTurnMessage::assistant_text("complete replacement")
        );
        assert_eq!(provider.requests.lock().await.len(), 3);
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptFailed {
                attempt: 1,
                error,
                ..
            } if error.contains("provider stop 校验失败")
                && error.contains("provider stop=MaxTokens")
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { text, .. }
                if text == "truncated fallback"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded {
                attempt: 2,
                text,
                ..
            } if text == "complete replacement"
        )));
    }

    #[test]
    fn fallback_response_validation_rejects_duplicate_tool_use_ids() {
        let response = response(
            vec![
                tool_use("toolu_duplicate", "missing_tool", json!({})),
                tool_use("toolu_duplicate", "missing_tool", json!({})),
            ],
            ProviderStop::ToolUse,
        );

        let error = super::validate_non_streaming_fallback_response(&response, &HashSet::new())
            .unwrap_err();

        assert!(format!("{error:#}").contains("重复 tool_use id: toolu_duplicate"));
    }

    #[test]
    fn fallback_response_validation_rejects_unsafe_terminal_stops() {
        let cases = vec![
            (
                response(
                    vec![SessionTurnContentBlock::text("missing tool block")],
                    ProviderStop::ToolUse,
                ),
                "provider stop=ToolUse",
            ),
            (
                response(
                    vec![tool_use("toolu_partial", "missing_tool", json!({}))],
                    ProviderStop::MaxTokens,
                ),
                "provider stop=MaxTokens",
            ),
            (
                response(
                    vec![tool_use("toolu_done", "missing_tool", json!({}))],
                    ProviderStop::Done,
                ),
                "provider stop=Done",
            ),
        ];

        for (response, expected_error) in cases {
            let error = super::validate_non_streaming_fallback_response(&response, &HashSet::new())
                .unwrap_err();
            assert!(format!("{error:#}").contains(expected_error));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tool_only_non_streaming_fallback_clears_partial_then_enters_tool_loop() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "partial that must clear".into(),
                }],
                "stream failed",
            ),
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(response(
                    vec![tool_use("toolu_fallback", "missing_tool", json!({}))],
                    ProviderStop::ToolUse,
                )),
            },
            scripted_success("after fallback tool"),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(provider.requests.lock().await.len(), 3);
        assert_eq!(
            text_blocks(&turn.messages.last().expect("final assistant").message),
            vec!["after fallback tool"]
        );
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { text, .. } if text.is_empty()
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::ToolCallStarted { id, .. } if id == "toolu_fallback"
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_tool_use_cancelled_before_dispatch_emits_only_skipped() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "partial that must clear".into(),
                }],
                "stream failed",
            ),
            ScriptedProviderAttempt {
                events: Vec::new(),
                result: Ok(response(
                    vec![tool_use(
                        "toolu_fallback",
                        "working_note",
                        json!({"action": "add", "note": "must not run"}),
                    )],
                    ProviderStop::ToolUse,
                )),
            },
        ]));
        let turn_loop = tool_loop(provider.clone());
        let control = ToolBoundaryControl::new();
        let control_from_event = control.clone();
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn_with_tool_boundary_control(
                request(),
                &mut |event| {
                    if matches!(
                        event,
                        SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
                    ) {
                        control_from_event
                            .cancel(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
                    }
                    events.push(event);
                },
                Some(control),
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert!(requests[0].stream);
        assert!(!requests[1].stream);
        assert_eq!(requests[1].retry_count_override, Some(0));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::ToolCallSkipped { id, reason, .. }
                if id == "toolu_fallback"
                    && *reason == ToolCallSkipReason::TurnInterruptedBeforeDispatch
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::ToolCallStarted { id, .. } if id == "toolu_fallback"
        )));
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_fallback"
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn five_non_streaming_fallback_failures_exhaust_without_canonical_response() {
        let mut attempts = vec![scripted_failure(
            vec![ProviderEvent::AssistantTextDelta {
                text: "partial".into(),
            }],
            "stream failed",
        )];
        for index in 1..=5 {
            attempts.push(scripted_failure(
                Vec::new(),
                &format!("fallback {index} failed"),
            ));
        }
        let provider = Arc::new(ScriptedProvider::new(attempts));
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("non-streaming fallback exhausted after 5/5"));
        assert!(error.to_string().contains("fallback 5 failed"));
        assert_eq!(provider.requests.lock().await.len(), 6);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                ))
                .count(),
            5
        );
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::ToolCallStarted { .. } | SessionTurnEvent::ToolCallSkipped { .. }
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                ))
                .count(),
            5
        );
    }

    #[tokio::test(start_paused = true)]
    async fn user_interrupt_during_fallback_backoff_stops_before_non_streaming_request() {
        let provider = Arc::new(ScriptedProvider::new(vec![scripted_failure(
            vec![ProviderEvent::AssistantTextDelta {
                text: "partial".into(),
            }],
            "stream failed",
        )]));
        let turn_loop = tool_loop(provider.clone());
        let control = ToolBoundaryControl::new();
        let control_from_event = control.clone();

        let error = turn_loop
            .run_session_turn_with_tool_boundary_control(
                request(),
                &mut |event| {
                    if matches!(
                        event,
                        SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                    ) {
                        control_from_event
                            .cancel(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
                    }
                },
                Some(control),
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn safe_steer_after_stream_failure_stops_before_fallback() {
        let provider = Arc::new(ScriptedProvider::new(vec![scripted_failure(
            vec![ProviderEvent::AssistantTextDelta {
                text: "partial".into(),
            }],
            "stream failed",
        )]));
        let turn_loop = tool_loop(provider.clone());
        let control = ToolBoundaryControl::new();
        let control_from_event = control.clone();
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn_with_tool_boundary_control(
                request(),
                &mut |event| {
                    if matches!(event, SessionTurnEvent::AssistantTextDelta { .. }) {
                        control_from_event
                            .cancel_if_open(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
                    }
                    events.push(event);
                },
                Some(control),
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_after_fallback_failure_does_not_start_a_phantom_next_attempt() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            scripted_failure(
                vec![ProviderEvent::AssistantTextDelta {
                    text: "partial".into(),
                }],
                "stream failed",
            ),
            scripted_failure(Vec::new(), "first fallback failed"),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut recorder = CancelOnFallbackFailureRecorder {
            control: ToolBoundaryControl::new(),
            events: Vec::new(),
        };

        let error = turn_loop
            .run_session_turn_with_tool_boundary_control_and_recorder(
                request(),
                &mut |_| {},
                Some(recorder.control.clone()),
                Some(&mut recorder),
            )
            .await
            .unwrap_err();

        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 2);
        assert_eq!(
            recorder
                .events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            recorder
                .events
                .iter()
                .filter(|event| matches!(
                    event,
                    SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                ))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn turn_loop_rejects_too_many_user_attachments() {
        let provider = Arc::new(FakeProvider::new(vec![]));
        let turn_loop = tool_loop(provider);
        let mut over_limit = request();
        over_limit.user_attachments = (0..6)
            .map(|index| crate::api::SessionAttachment::TextFile {
                path: std::path::PathBuf::from(format!("f{index}.txt")),
            })
            .collect();

        let err = turn_loop
            .run_session_turn(over_limit, &mut |_| {})
            .await
            .unwrap_err();

        assert!(err.to_string().contains("附件数量超限"));
    }

    #[tokio::test]
    async fn turn_loop_rejects_user_attachments_when_disabled() {
        let provider = Arc::new(FakeProvider::new(vec![]));
        let turn_loop = tool_loop(provider).with_attachment_limits(AttachmentLimits {
            enabled: false,
            ..AttachmentLimits::default()
        });
        let mut request = request();
        request.user_attachments = vec![crate::api::SessionAttachment::InlineImage {
            media_type: "image/png".into(),
            data: "QUJD".into(),
        }];

        let err = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap_err();

        assert!(err.to_string().contains("附件功能已禁用"));
    }

    #[tokio::test]
    async fn turn_loop_validates_inline_image_attachment_bytes() {
        let provider = Arc::new(FakeProvider::new(vec![]));
        let turn_loop = tool_loop(provider);
        let mut request = request();
        request.user_attachments = vec![crate::api::SessionAttachment::InlineImage {
            media_type: "image/png".into(),
            data: "QUJD".into(),
        }];

        let err = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap_err();

        assert!(err.to_string().contains("校验内联图片附件失败"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn text_attachment_checks_canonical_memory_target_before_reading() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let memories = dir.path().join("memories");
        tokio::fs::create_dir(&memories).await.unwrap();
        let protected = memories.join("MEMORY.md");
        tokio::fs::write(&protected, "private\n").await.unwrap();
        tokio::fs::set_permissions(&protected, std::fs::Permissions::from_mode(0o000))
            .await
            .unwrap();
        let alias = dir.path().join("attached.txt");
        tokio::fs::symlink(&protected, &alias).await.unwrap();
        let provider = Arc::new(FakeProvider::new(Vec::new()));
        let turn_loop = tool_loop(provider.clone());
        let mut request = request();
        request.user_attachments = vec![crate::api::SessionAttachment::TextFile { path: alias }];

        let error = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("MEMORY.md / USER.md 必须通过 memory 工具访问"));
        assert!(provider.requests.lock().await.is_empty());
    }

    #[tokio::test]
    async fn text_attachment_at_character_limit_is_fully_inlined() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unicode.txt");
        tokio::fs::write(&path, "你好").await.unwrap();
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("完成")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone()).with_attachment_limits(AttachmentLimits {
            max_text_chars: 2,
            ..AttachmentLimits::default()
        });
        let mut request = request();
        request.user_attachments =
            vec![crate::api::SessionAttachment::TextFile { path: path.clone() }];
        let mut events = Vec::new();

        turn_loop
            .run_session_turn(request, &mut |event| events.push(event))
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        assert!(requests[0]
            .messages
            .iter()
            .flat_map(text_blocks)
            .any(|text| text.contains("Attached file: unicode.txt") && text.contains("你好")));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::Warning { .. })));
    }

    #[tokio::test]
    async fn text_attachments_apply_character_limit_per_file_not_per_turn() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.txt");
        let second = dir.path().join("second.txt");
        tokio::fs::write(&first, "甲乙丙丁").await.unwrap();
        tokio::fs::write(&second, "一二三四").await.unwrap();
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("完成")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone()).with_attachment_limits(AttachmentLimits {
            max_text_chars: 4,
            ..AttachmentLimits::default()
        });
        let mut request = request();
        request.user_attachments = vec![
            crate::api::SessionAttachment::TextFile { path: first },
            crate::api::SessionAttachment::TextFile { path: second },
        ];
        let mut events = Vec::new();

        turn_loop
            .run_session_turn(request, &mut |event| events.push(event))
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        let text = requests[0]
            .messages
            .iter()
            .flat_map(text_blocks)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("甲乙丙丁"));
        assert!(text.contains("一二三四"));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::Warning { .. })));
    }

    #[tokio::test]
    async fn oversized_text_attachment_degrades_to_path_without_read_permission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.txt");
        let original = "敏感正文不应内联";
        tokio::fs::write(&path, original).await.unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_write",
                    "file_write",
                    json!({"path": "oversized.txt", "content": "after\n"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("未修改")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools).with_attachment_limits(
            AttachmentLimits {
                max_text_chars: 4,
                ..AttachmentLimits::default()
            },
        );
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());
        request.current_turn_id = Some("turn_oversized_attachment".into());
        request.user_attachments =
            vec![crate::api::SessionAttachment::TextFile { path: path.clone() }];
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request, &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(tokio::fs::read_to_string(&path).await.unwrap(), original);
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_write");
        assert_eq!(result["outcome"]["kind"], "business_failure");
        let requests = provider.requests.lock().await;
        let first_request_text = requests[0]
            .messages
            .iter()
            .flat_map(text_blocks)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(first_request_text.contains(&path.display().to_string()));
        assert!(first_request_text.contains("Characters: 8"));
        assert!(first_request_text.contains("Use file_read"));
        assert!(!first_request_text.contains(original));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::Warning { message }
                    if message.contains("超过单文件上限 4")
                        && message.contains("未授予读取许可")
            )
        }));
    }

    #[tokio::test]
    async fn text_character_limit_does_not_apply_to_pdf_attachment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("brief.pdf");
        tokio::fs::write(&path, b"%PDF-1.7 fake").await.unwrap();
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("完成")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone()).with_attachment_limits(AttachmentLimits {
            max_text_chars: 1,
            ..AttachmentLimits::default()
        });
        let mut request = request();
        request.user_attachments = vec![crate::api::SessionAttachment::DocumentFile {
            path,
            media_type: "application/pdf".into(),
        }];
        let mut events = Vec::new();

        turn_loop
            .run_session_turn(request, &mut |event| events.push(event))
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        assert!(requests[0].messages.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(
                    block,
                    SessionTurnContentBlock::Document { media_type, .. }
                        if media_type == "application/pdf"
                )
            })
        }));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::Warning { .. })));
    }

    #[tokio::test]
    async fn complete_text_attachment_authorizes_existing_file_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attached.txt");
        tokio::fs::write(&path, "before\n").await.unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_write",
                    "file_write",
                    json!({"path": "attached.txt", "content": "after\n"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("完成")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());
        request.current_turn_id = Some("turn_attachment".into());
        request.user_attachments =
            vec![crate::api::SessionAttachment::TextFile { path: path.clone() }];

        let turn = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap();

        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "after\n");
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_write");
        assert_eq!(result["outcome"]["kind"], "completed");
        let requests = provider.requests.lock().await;
        assert!(requests[0]
            .messages
            .iter()
            .flat_map(|message| text_blocks(message))
            .any(|text| text.contains("Attached file: attached.txt") && text.contains("before\n")));
    }

    #[tokio::test]
    async fn current_text_attachment_is_registered_after_first_preflight_clear() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attached.txt");
        tokio::fs::write(&path, "before\n").await.unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_write",
                    "file_write",
                    json!({"path": "attached.txt", "content": "after\n"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("完成")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, Arc::clone(&tools));
        let session_id: crate::claim::SessionId = "session_aaaaaaaa".parse().unwrap();
        let mut request = request();
        request.current_session_id = Some(session_id.clone());
        request.current_turn_id = Some("turn_attachment_compact".into());
        request.user_attachments =
            vec![crate::api::SessionAttachment::TextFile { path: path.clone() }];
        let mut preflight = ClearingFileReadPreflight {
            tools,
            session_id,
            cleared: false,
        };

        let turn = turn_loop
            .run_session_turn_with_hooks(request, &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .unwrap();

        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "after\n");
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_write");
        assert_eq!(result["outcome"]["kind"], "completed");
    }

    #[tokio::test]
    async fn externalized_text_attachment_does_not_authorize_existing_file_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("attached.txt");
        tokio::fs::write(&path, "before\n").await.unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_write",
                    "file_write",
                    json!({"path": "attached.txt", "content": "after\n"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("未修改")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());
        request.current_turn_id = Some("turn_attachment_externalized".into());
        request.user_attachments =
            vec![crate::api::SessionAttachment::TextFile { path: path.clone() }];
        let mut preflight = ExternalizingTextAttachmentPreflight;

        let turn = turn_loop
            .run_session_turn_with_hooks(request, &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .unwrap();

        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "before\n");
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_write");
        assert_eq!(result["outcome"]["kind"], "business_failure");
        let requests = provider.requests.lock().await;
        assert!(requests[0]
            .messages
            .iter()
            .flat_map(text_blocks)
            .any(|text| text.contains("<externalized_compaction_asset>")));
        assert!(!requests[0]
            .messages
            .iter()
            .flat_map(text_blocks)
            .any(|text| text.contains("Attached file: attached.txt") && text.contains("before\n")));
    }

    #[tokio::test]
    async fn handwritten_attachment_wrapper_does_not_authorize_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        tokio::fs::write(&path, "before\n").await.unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_write",
                    "file_write",
                    json!({"path": "note.txt", "content": "after\n"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("未修改")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());
        request.current_turn_id = Some("turn_spoof".into());
        request.user_text = "Attached file: note.txt\nPath: note.txt\n\nbefore\n".into();

        let turn = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap();

        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "before\n");
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_write");
        assert_eq!(result["outcome"]["kind"], "business_failure");
    }

    #[tokio::test]
    async fn file_reads_in_one_assistant_response_each_use_per_call_char_limit() {
        let dir = tempfile::tempdir().unwrap();
        let large = "aaaa\n".repeat(30_000);
        tokio::fs::write(dir.path().join("first.txt"), &large)
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("second.txt"), &large)
            .await
            .unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_first",
                        "file_read",
                        json!({"path": "first.txt", "count": 30_000, "show_linenos": false}),
                    ),
                    tool_use(
                        "toolu_second",
                        "file_read",
                        json!({"path": "second.txt", "count": 30_000, "show_linenos": false}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("完成")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());

        let mut events = Vec::new();
        let turn = turn_loop
            .run_session_turn(request, &mut |event| events.push(event))
            .await
            .unwrap();
        let first_completion = events
            .iter()
            .position(|event| matches!(event, SessionTurnEvent::ToolCallCompleted { .. }))
            .expect("file_read 应产生完成事件");
        for id in ["toolu_first", "toolu_second"] {
            let started = events
                .iter()
                .position(|event| {
                    matches!(event, SessionTurnEvent::ToolCallStarted { id: started_id, .. } if started_id == id)
                })
                .expect("两个 file_read 都应启动");
            assert!(
                started < first_completion,
                "不同文件的 file_read 应进入同一并发批次"
            );
        }
        let first = tool_result_content(non_context_messages(&turn)[2], "toolu_first");
        let second = tool_result_content(non_context_messages(&turn)[2], "toolu_second");
        for result in [first, second] {
            assert_eq!(
                result["output"]["content"]
                    .as_str()
                    .unwrap()
                    .chars()
                    .count(),
                crate::config::DEFAULT_FILE_READ_MAX_CHARS
            );
            assert_eq!(result["output"]["page"]["returned_end"], 20_000);
            assert_eq!(result["output"]["page"]["stop_reason"], "max_chars");
        }
    }

    #[tokio::test]
    async fn same_response_read_then_local_patch_uses_immediate_read_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        tokio::fs::write(&path, "before\ntarget\nafter\n")
            .await
            .unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_read",
                        "file_read",
                        json!({"path": "note.txt", "start": 2, "count": 1, "show_linenos": false}),
                    ),
                    tool_use(
                        "toolu_patch",
                        "file_patch",
                        json!({"path": "note.txt", "old_content": "target", "new_content": "done"}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("完成")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());

        let turn = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap();
        let patch = tool_result_content(non_context_messages(&turn)[2], "toolu_patch");
        assert_eq!(patch["outcome"]["kind"], "completed");
        assert_eq!(
            tokio::fs::read_to_string(path).await.unwrap(),
            "before\ndone\nafter\n"
        );
    }

    #[tokio::test]
    async fn later_same_path_write_is_skipped_after_business_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        tokio::fs::write(&path, "original\n").await.unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_first",
                        "file_write",
                        json!({"path": "note.txt", "content": "first\n"}),
                    ),
                    tool_use(
                        "toolu_second",
                        "file_write",
                        json!({"path": "note.txt", "content": "second\n"}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("未修改")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());

        let turn = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap();
        let first = tool_result_content(non_context_messages(&turn)[2], "toolu_first");
        let second = tool_result_content(non_context_messages(&turn)[2], "toolu_second");
        assert_eq!(first["outcome"]["kind"], "business_failure");
        assert_eq!(second["output"]["status"], "skipped");
        assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), "original\n");
    }

    #[tokio::test]
    async fn file_read_image_appends_media_block_without_base64_in_tool_result() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("shot.png"), tiny_png_bytes())
            .await
            .unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "file_read",
                    json!({"path": "shot.png"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("看到了")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_1");
        assert_eq!(result["ok"], true);
        assert_eq!(result["outcome"]["kind"], "completed");
        assert_eq!(result["output"]["kind"], "image");
        // base64 媒体不进 tool_result 文本通道
        assert!(result["output"].get("media").is_none());
        assert!(matches!(
            &non_context_messages(&turn)[2].content[1],
            SessionTurnContentBlock::Text { text } if text == "[file_read attachment] shot.png"
        ));
        assert!(matches!(
            &non_context_messages(&turn)[2].content[2],
            SessionTurnContentBlock::Image { media_type, data }
                if media_type == "image/png" && !data.is_empty()
        ));
    }

    #[tokio::test]
    async fn file_read_pdf_appends_document_block_with_filename() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("brief.pdf"), b"%PDF-1.7 fake")
            .await
            .unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "file_read",
                    json!({"path": "brief.pdf"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("读完了")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_1");
        assert_eq!(result["output"]["kind"], "pdf");
        assert!(result["output"].get("media").is_none());
        assert!(matches!(
            &non_context_messages(&turn)[2].content[2],
            SessionTurnContentBlock::Document { media_type, filename, .. }
                if media_type == "application/pdf"
                    && filename.as_deref() == Some("brief.pdf")
        ));
    }

    #[tokio::test]
    async fn user_image_attachment_is_normalized_into_image_block() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shot.png");
        tokio::fs::write(&path, tiny_png_bytes()).await.unwrap();
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("done")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider);
        let mut with_image = request();
        with_image.user_attachments = vec![crate::api::SessionAttachment::LocalImage { path }];

        let turn = turn_loop
            .run_session_turn(with_image, &mut |_| {})
            .await
            .unwrap();

        assert!(matches!(
            &non_context_messages(&turn)[0].content[1],
            SessionTurnContentBlock::Image { media_type, data }
                if media_type == "image/png" && !data.is_empty()
        ));
    }

    #[tokio::test]
    async fn turn_loop_done_without_tools() {
        let provider = Arc::new(
            FakeProvider::new(vec![response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            )])
            .with_events(vec![
                ProviderEvent::AssistantTextDelta { text: "do".into() },
                ProviderEvent::AssistantMessageCompleted {
                    text: "done".into(),
                },
            ]),
        );
        let turn_loop = tool_loop(provider.clone()).with_now_fn(fixed_now);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(
            non_context_messages(&turn),
            vec![
                &SessionTurnMessage::user_text("hello"),
                &SessionTurnMessage::assistant_text("done"),
            ]
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 1);
        let expected_provider_messages = vec![
            SessionTurnMessage::assistant_text("prior"),
            SessionTurnMessage::model_context(
                ModelContextSource::Runtime,
                super::runtime_context_text(fixed_now()),
            ),
            SessionTurnMessage::user_text("hello"),
        ];
        assert_eq!(
            events,
            vec![
                SessionTurnEvent::ContextUsageUpdated {
                    usage: estimate_provider_request_context_tokens(
                        "system",
                        &expected_provider_messages,
                        &requests[0].tools,
                    )
                },
                SessionTurnEvent::AssistantTextDelta { text: "do".into() },
                SessionTurnEvent::AssistantMessageCompleted {
                    text: "done".into()
                },
            ]
        );
        assert_eq!(requests[0].system_prompt, "system");
        assert_eq!(requests[0].messages, expected_provider_messages);
        assert_eq!(requests[0].max_tokens, 1024);
        assert!(requests[0].stream);
        assert!(requests[0]
            .tools
            .iter()
            .any(|tool| tool.name == "working_note"));
    }

    #[tokio::test]
    async fn turn_loop_rejects_done_with_tool_use_before_dispatch() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![tool_use(
                "toolu_inconsistent",
                "working_note",
                json!({"action": "add", "note": "must not run"}),
            )],
            ProviderStop::Done,
        )]));
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = tool_loop_with_tools(provider.clone(), Arc::clone(&tools));
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .expect_err("Done + ToolUse must fail before dispatch");

        assert!(format!("{error:#}").contains("provider stop=Done"));
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(!events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::ToolCallStarted { .. }
                | SessionTurnEvent::ToolCallCompleted { .. }
                | SessionTurnEvent::ToolCallInterrupted { .. }
                | SessionTurnEvent::ToolCallSkipped { .. }
        )));
        let notes = tools
            .dispatch("working_note", json!({"action": "list"}))
            .await
            .unwrap();
        assert_eq!(notes.output["notes"], json!([]));
    }

    #[tokio::test]
    async fn preflight_context_estimate_is_marked_as_estimate() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("done")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider);
        let mut events = Vec::new();

        let _turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert!(matches!(
            events.first(),
            Some(SessionTurnEvent::ContextUsageUpdated { usage })
                if usage.source == ContextUsageSource::Estimate
        ));
    }

    #[tokio::test]
    async fn turn_loop_can_skip_preflight_context_estimate() {
        let provider = Arc::new(
            FakeProvider::new(vec![response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            )])
            .without_preflight_context_estimate()
            .with_events(vec![
                ProviderEvent::ContextUsageUpdated {
                    usage: crate::api::ContextUsageSnapshot {
                        used_tokens: 42,
                        source: ContextUsageSource::Provider,
                    },
                },
                ProviderEvent::AssistantMessageCompleted {
                    text: "done".into(),
                },
            ]),
        );
        let turn_loop = tool_loop(provider);
        let mut events = Vec::new();

        let _turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SessionTurnEvent::ContextUsageUpdated { .. }))
                .count(),
            1
        );
        assert!(matches!(
            events.first(),
            Some(SessionTurnEvent::ContextUsageUpdated { usage })
                if usage.used_tokens == 42
        ));
    }

    #[test]
    fn primary_streaming_providers_skip_preflight_context_estimate() {
        use crate::api::AnthropicProviderAdapter;
        use crate::api::OpenAiCompatibleChatProviderAdapter;
        use std::time::Duration;

        let anthropic = AnthropicProviderAdapter::new(
            "key".into(),
            "https://llm.example.com".into(),
            "model".into(),
            128,
            Duration::from_secs(1),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let openai = OpenAiCompatibleChatProviderAdapter::new(
            "key".into(),
            "https://llm.example.com/v1".into(),
            "model".into(),
            Duration::from_secs(1),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        assert!(!anthropic.emit_preflight_context_estimate());
        assert!(!openai.emit_preflight_context_estimate());
    }

    #[test]
    fn normalize_provider_messages_merges_adjacent_pure_user_text() {
        let messages = vec![
            SessionTurnMessage::assistant_text("prior"),
            SessionTurnMessage::user_text("<user_shell_command>...</user_shell_command>"),
            SessionTurnMessage::user_text("next prompt"),
        ];

        let normalized = super::normalize_provider_messages(&messages);

        assert_eq!(
            normalized,
            vec![
                SessionTurnMessage::assistant_text("prior"),
                SessionTurnMessage::user_text(
                    "<user_shell_command>...</user_shell_command>\n\nnext prompt"
                ),
            ]
        );
    }

    #[test]
    fn normalize_provider_messages_does_not_merge_tool_result_user_messages() {
        let messages = vec![
            SessionTurnMessage::user_text("before tool"),
            SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "toolu_1".into(),
                    content: "tool output".into(),
                }],
            },
            SessionTurnMessage::user_text("after tool"),
        ];

        let normalized = super::normalize_provider_messages(&messages);

        assert_eq!(normalized, messages);
    }

    #[test]
    fn frozen_provider_prefix_blocks_cross_boundary_user_merge() {
        let messages = vec![
            SessionTurnMessage::user_text("already sent user"),
            SessionTurnMessage::user_text("new suffix one"),
            SessionTurnMessage::user_text("new suffix two"),
        ];
        let prefix = super::FrozenProviderRequestPrefix::new(&messages, 1).unwrap();

        let projected = prefix.project(&messages).unwrap();

        assert_eq!(
            projected,
            vec![
                SessionTurnMessage::user_text("already sent user"),
                SessionTurnMessage::user_text("new suffix one\n\nnew suffix two"),
            ]
        );
    }

    #[test]
    fn adapter_continuation_preserves_raw_active_boundary_after_wire_normalization() {
        let runtime = SessionTurnMessage::model_context(
            ModelContextSource::Runtime,
            "<runtime_context>stable</runtime_context>",
        );
        let mut raw_messages = vec![
            SessionTurnMessage::user_text("compaction summary"),
            SessionTurnMessage::user_text("preserved raw user"),
            runtime.clone(),
            SessionTurnMessage::user_text("current request"),
        ];
        let active_start_index = 2;
        let mut frozen = super::FrozenProviderRequestPrefix::new(&raw_messages, 0).unwrap();
        let outer_request = frozen.project(&raw_messages).unwrap();
        assert_eq!(outer_request.len(), 3, "前两条纯 user 应只在 wire 合并");

        let continuation = SessionTurnMessage::assistant_text("partial continuation");
        let mut latest_request = outer_request.clone();
        latest_request.push(continuation.clone());
        super::append_adapter_continuation_suffix_to_raw_history(
            &mut raw_messages,
            &outer_request,
            &latest_request,
        )
        .unwrap();

        assert_eq!(raw_messages[active_start_index], runtime);
        assert_eq!(raw_messages.last(), Some(&continuation));
        frozen.advance(&raw_messages, latest_request.clone());
        assert_eq!(frozen.project(&raw_messages).unwrap(), latest_request);
    }

    #[tokio::test]
    async fn provider_request_uses_normalized_adjacent_user_messages() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("done")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone()).with_now_fn(fixed_now);
        let mut request = request();
        request.history = vec![SessionTurnMessage::user_text(
            "<user_shell_command>...</user_shell_command>",
        )];

        let _turn = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].messages,
            vec![
                SessionTurnMessage::user_text("<user_shell_command>...</user_shell_command>"),
                SessionTurnMessage::model_context(
                    ModelContextSource::Runtime,
                    super::runtime_context_text(fixed_now()),
                ),
                SessionTurnMessage::user_text("hello"),
            ]
        );
    }

    #[tokio::test]
    async fn provider_request_persists_independent_runtime_context() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("done")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone()).with_now_fn(fixed_now);

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        let (_, _, user_text) = requests[0].messages[1]
            .model_context_snapshot()
            .expect("runtime context message");
        let expected_date = fixed_now()
            .with_timezone(&Local)
            .format("%Y-%m-%d %A")
            .to_string();
        assert!(user_text.starts_with("<runtime_context>"));
        assert!(user_text.contains(&format!("current_date: {expected_date}")));
        assert!(user_text.contains("timezone: "));
        assert!(!user_text.contains("current_datetime"));
        assert!(!user_text.contains("not a user request"));
        assert!(user_text.ends_with("</runtime_context>"));
        assert_eq!(turn.messages[0].message, requests[0].messages[1]);
        assert_eq!(
            non_context_messages(&turn)[0],
            &SessionTurnMessage::user_text("hello")
        );
    }

    #[tokio::test]
    async fn completed_message_recorder_matches_provider_history_order() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("done")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider.clone()).with_now_fn(fixed_now);
        let background = SessionTurnMessage::model_context(
            ModelContextSource::BackgroundProcess,
            "<background_processes>empty</background_processes>",
        );
        let delegation = SessionTurnMessage::model_context(
            ModelContextSource::Delegation,
            "<subagent_summary_projection>{\"subagents\":[]}</subagent_summary_projection>",
        );
        let mut appender = StaticContextAppender {
            messages: vec![background.clone(), delegation.clone()],
        };
        let mut recorder = RecordingCompletedMessageRecorder {
            messages: Vec::new(),
        };

        let turn = turn_loop
            .run_session_turn_with_context_hooks(
                request(),
                Vec::new(),
                &mut |_| {},
                None,
                SessionTurnHooks::new(Some(&mut recorder), Some(&mut appender), None),
            )
            .await
            .unwrap();

        assert_eq!(recorder.messages, turn.messages);
        let recorded = recorder
            .messages
            .iter()
            .map(|message| message.message.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            recorded,
            vec![
                SessionTurnMessage::model_context(
                    ModelContextSource::Runtime,
                    super::runtime_context_text(fixed_now()),
                ),
                background,
                delegation,
                SessionTurnMessage::user_text("hello"),
                SessionTurnMessage::assistant_text("done"),
            ]
        );
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].messages[1..], recorded[..4]);
    }

    #[tokio::test]
    async fn recovered_context_dedup_matches_only_the_trailing_snapshot_sequence() {
        let provider = Arc::new(FakeProvider::new(Vec::new()));
        let turn_loop = tool_loop(provider);
        let runtime_a = SessionTurnMessage::model_context(
            ModelContextSource::Runtime,
            "<runtime_context>state=A</runtime_context>",
        );
        let runtime_b = SessionTurnMessage::model_context(
            ModelContextSource::Runtime,
            "<runtime_context>state=B</runtime_context>",
        );
        let recovered_a = CompletedSessionTurnMessage::new(runtime_a.clone(), fixed_now());

        let mut historical_match = vec![runtime_a.clone(), runtime_b.clone()];
        let mut committed = Vec::new();
        let mut recorder: Option<&mut dyn SessionTurnEventRecorder> = None;
        turn_loop
            .materialize_recovered_model_context(
                &mut historical_match,
                &mut committed,
                &mut recorder,
                vec![recovered_a.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            historical_match,
            vec![runtime_a.clone(), runtime_b, runtime_a.clone()]
        );
        assert_eq!(committed, vec![recovered_a.clone()]);

        let mut write_ahead_tail = historical_match;
        let mut committed = Vec::new();
        turn_loop
            .materialize_recovered_model_context(
                &mut write_ahead_tail,
                &mut committed,
                &mut recorder,
                vec![recovered_a.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            write_ahead_tail,
            vec![
                runtime_a.clone(),
                SessionTurnMessage::model_context(
                    ModelContextSource::Runtime,
                    "<runtime_context>state=B</runtime_context>",
                ),
                runtime_a,
            ]
        );
        assert_eq!(committed, vec![recovered_a]);
    }

    #[tokio::test]
    async fn runtime_context_is_not_added_to_tool_result_messages() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "working_note",
                    json!({"action": "add", "note": "remember"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider.clone()).with_now_fn(fixed_now);

        let _turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        let runtime_context_mentions = requests[1]
            .messages
            .iter()
            .filter(|message| {
                message
                    .model_context_snapshot()
                    .is_some_and(|(source, _, _)| *source == ModelContextSource::Runtime)
            })
            .count();
        assert_eq!(runtime_context_mentions, 1);
        assert!(matches!(
            requests[1]
                .messages
                .last()
                .map(|message| message.content.as_slice()),
            Some([SessionTurnContentBlock::ToolResult { .. }])
        ));
    }

    #[tokio::test]
    async fn runtime_context_appends_once_on_date_or_timezone_change() {
        let cases = [
            (
                "<runtime_context>\ncurrent_date: 2026-06-29 Monday\ntimezone: Asia/Shanghai\n</runtime_context>",
                "<runtime_context>\ncurrent_date: 2026-06-30 Tuesday\ntimezone: Asia/Shanghai\n</runtime_context>",
            ),
            (
                "<runtime_context>\ncurrent_date: 2026-06-29 Monday\ntimezone: Asia/Shanghai\n</runtime_context>",
                "<runtime_context>\ncurrent_date: 2026-06-29 Monday\ntimezone: Europe/London\n</runtime_context>",
            ),
        ];

        for (before, after) in cases {
            let provider = Arc::new(FakeProvider::new(vec![
                response(
                    vec![tool_use(
                        "toolu_1",
                        "working_note",
                        json!({"action": "add", "note": "remember"}),
                    )],
                    ProviderStop::ToolUse,
                ),
                response(
                    vec![SessionTurnContentBlock::text("done")],
                    ProviderStop::Done,
                ),
            ]));
            let observation = Arc::new(AtomicUsize::new(0));
            let turn_loop = {
                let observation = Arc::clone(&observation);
                let before = before.to_string();
                let after = after.to_string();
                tool_loop(provider.clone()).with_runtime_context_fn(move |_| {
                    if observation.fetch_add(1, Ordering::SeqCst) < 2 {
                        before.clone()
                    } else {
                        after.clone()
                    }
                })
            };

            let turn = turn_loop
                .run_session_turn(request(), &mut |_| {})
                .await
                .unwrap();

            let requests = provider.requests.lock().await;
            assert_eq!(requests.len(), 2);
            assert!(requests[1].messages.starts_with(&requests[0].messages));
            let runtime_texts = requests[1]
                .messages
                .iter()
                .filter_map(|message| {
                    let (source, _, text) = message.model_context_snapshot()?;
                    (*source == ModelContextSource::Runtime).then_some(text)
                })
                .collect::<Vec<_>>();
            assert_eq!(runtime_texts, vec![before, after]);
            assert_eq!(
                turn.messages
                    .iter()
                    .filter(|message| {
                        message
                            .model_context_snapshot()
                            .is_some_and(|(source, _, _)| *source == ModelContextSource::Runtime)
                    })
                    .count(),
                2
            );
        }
    }

    #[tokio::test]
    async fn context_observation_precedes_compaction_preflight_on_each_tool_loop() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "working_note",
                    json!({"action": "add", "note": "observe changed context"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider.clone()).with_now_fn(fixed_now);
        let mut appender = ChangingContextAppender { observations: 0 };
        let mut preflight = ContextAwarePreflight::default();

        turn_loop
            .run_session_turn_with_context_hooks(
                request(),
                Vec::new(),
                &mut |_| {},
                None,
                SessionTurnHooks::new(None, Some(&mut appender), Some(&mut preflight)),
            )
            .await
            .unwrap();

        assert_eq!(
            preflight.latest_background_texts,
            vec![
                "<background_processes>state=running</background_processes>",
                "<background_processes>state=completed</background_processes>",
            ]
        );
        let requests = provider.requests.lock().await;
        assert!(requests[1].messages.starts_with(&requests[0].messages));
        assert!(requests[1].messages.last().is_some_and(|message| message
            .model_context_snapshot()
            .is_some_and(
                |(source, _, text)| *source == ModelContextSource::BackgroundProcess
                    && text.contains("state=completed")
            )));
    }

    #[tokio::test]
    async fn changed_context_and_same_request_compaction_emit_one_rebaseline_snapshot() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "working_note",
                    json!({"action": "add", "note": "trigger replacement"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider.clone()).with_now_fn(fixed_now);
        let mut appender = ChangingContextAppender { observations: 0 };
        let mut preflight = ReplacingContextPreflight::default();
        let mut recorder = RecordingCompletedMessageRecorder {
            messages: Vec::new(),
        };

        let turn = turn_loop
            .run_session_turn_with_context_hooks(
                request(),
                Vec::new(),
                &mut |_| {},
                None,
                SessionTurnHooks::new(
                    Some(&mut recorder),
                    Some(&mut appender),
                    Some(&mut preflight),
                ),
            )
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        for source in [
            ModelContextSource::Runtime,
            ModelContextSource::BackgroundProcess,
        ] {
            assert_eq!(
                requests[1]
                    .messages
                    .iter()
                    .filter(|message| message
                        .model_context_snapshot()
                        .is_some_and(|(candidate, _, _)| *candidate == source))
                    .count(),
                1,
                "compact window 中每个 source 只能有一份 authoritative baseline"
            );
        }
        assert_eq!(
            turn.messages
                .iter()
                .filter(|message| message
                    .model_context_snapshot()
                    .is_some_and(|(source, _, text)| *source
                        == ModelContextSource::BackgroundProcess
                        && text.contains("state=completed")))
                .count(),
            1
        );
        assert_eq!(recorder.messages, turn.messages);
    }

    #[tokio::test]
    async fn turn_loop_executes_tool_use_then_continues_until_done() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    SessionTurnContentBlock::text("noting"),
                    tool_use(
                        "toolu_1",
                        "working_note",
                        json!({"action": "add", "note": "remember this"}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("finished")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();

        assert_eq!(non_context_messages(&turn).len(), 4);
        assert_eq!(
            non_context_messages(&turn)[0],
            &SessionTurnMessage::user_text("hello")
        );
        assert_eq!(non_context_messages(&turn)[1].content.len(), 2);
        assert_eq!(non_context_messages(&turn)[2].role, "user");
        let tool_result = tool_result_content(non_context_messages(&turn)[2], "toolu_1");
        assert_eq!(tool_result["ok"], true);
        assert_eq!(tool_result["outcome"]["kind"], "completed");
        assert_eq!(
            non_context_messages(&turn)[3],
            &SessionTurnMessage::assistant_text("finished")
        );
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ToolCallStarted { id, name, .. }
                    if id == "toolu_1" && name == "working_note"
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ToolCallCompleted { id, summary, .. }
                    if id == "toolu_1" && summary.contains("ok")
            )
        }));
    }

    #[tokio::test]
    async fn turn_loop_waits_for_tool_completed_durable_record_before_returning() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "working_note",
                    json!({"action": "add", "note": "remember this"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("finished")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider);
        let (completed_seen_tx, completed_seen_rx) = oneshot::channel();
        let (release_completed_tx, release_completed_rx) = oneshot::channel();
        let mut recorder = BlockingCompletedRecorder {
            target: BlockingRecordTarget::ToolCompleted,
            completed_seen: Some(completed_seen_tx),
            release_completed: Some(release_completed_rx),
        };
        let mut events = Vec::new();
        let mut emit = |event| events.push(event);
        let running = turn_loop.run_session_turn_with_tool_boundary_control_and_recorder(
            request(),
            &mut emit,
            None,
            Some(&mut recorder),
        );
        tokio::pin!(running);

        tokio::select! {
            seen = completed_seen_rx => seen.unwrap(),
            result = &mut running => panic!(
                "turn returned before completed event reached durable recorder: {result:?}"
            ),
        }
        tokio::select! {
            _ = sleep(Duration::from_millis(20)) => {}
            result = &mut running => panic!(
                "turn returned before completed event durable ack: {result:?}"
            ),
        }
        release_completed_tx.send(()).unwrap();

        let turn = running.await.unwrap();
        assert_eq!(non_context_messages(&turn).len(), 4);
        assert_eq!(
            non_context_messages(&turn)[3],
            &SessionTurnMessage::assistant_text("finished")
        );
    }

    #[tokio::test]
    async fn explicit_cancel_drops_blocked_tool_completed_durable_record_within_grace() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![tool_use(
                "toolu_1",
                "working_note",
                json!({"action": "add", "note": "must not hold cancel"}),
            )],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop(provider);
        let control = ToolBoundaryControl::new();
        let (completed_seen_tx, completed_seen_rx) = oneshot::channel();
        let (_release_completed_tx, release_completed_rx) = oneshot::channel();
        let mut recorder = BlockingCompletedRecorder {
            target: BlockingRecordTarget::ToolCompleted,
            completed_seen: Some(completed_seen_tx),
            release_completed: Some(release_completed_rx),
        };
        let mut events = Vec::new();
        let mut emit = |event| events.push(event);
        let mut running = Box::pin(
            turn_loop.run_session_turn_with_tool_boundary_control_and_recorder(
                request(),
                &mut emit,
                Some(control.clone()),
                Some(&mut recorder),
            ),
        );

        tokio::select! {
            seen = completed_seen_rx => seen.expect(
                "tool completion should reach durable recorder before cancellation"
            ),
            result = running.as_mut() => panic!(
                "turn finished before tool completion reached durable recorder: {result:?}"
            ),
        }
        let started = std::time::Instant::now();
        control.cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
        let error = timeout(Duration::from_millis(400), running.as_mut())
            .await
            .expect("explicit cancel must not wait for a blocked durable recorder")
            .expect_err("cancelled turn should not return a normal tool-result batch");

        // 同一个 Esc/Ctrl-C 只有一段 100ms grace；留出 CI 调度余量，但不能接受旧实现
        // 把阻塞 Completed flush 与 forced-abort 各自再等 100ms 的约 200ms 路径。
        assert!(started.elapsed() < Duration::from_millis(180));
        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        drop(running);
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_1")
        }));
    }

    #[tokio::test]
    async fn explicit_cancel_after_provider_response_bounds_skipped_journal_recording() {
        let control = ToolBoundaryControl::new();
        let provider = Arc::new(
            FakeProvider::new(vec![response(
                vec![
                    tool_use(
                        "toolu_skip_1",
                        "working_note",
                        json!({"action": "add", "note": "must not run"}),
                    ),
                    tool_use(
                        "toolu_skip_2",
                        "working_note",
                        json!({"action": "add", "note": "must not run"}),
                    ),
                    tool_use(
                        "toolu_skip_3",
                        "working_note",
                        json!({"action": "add", "note": "must not run"}),
                    ),
                ],
                ProviderStop::ToolUse,
            )])
            .with_cancel_after_response(control.clone()),
        );
        let turn_loop = tool_loop(provider);
        let (skipped_seen_tx, skipped_seen_rx) = oneshot::channel();
        let (_release_skipped_tx, release_skipped_rx) = oneshot::channel();
        let mut recorder = BlockingCompletedRecorder {
            target: BlockingRecordTarget::ToolSkipped,
            completed_seen: Some(skipped_seen_tx),
            release_completed: Some(release_skipped_rx),
        };
        let mut events = Vec::new();
        let started = std::time::Instant::now();
        let error = {
            let mut emit = |event| events.push(event);
            let mut running = Box::pin(
                turn_loop.run_session_turn_with_tool_boundary_control_and_recorder(
                    request(),
                    &mut emit,
                    Some(control),
                    Some(&mut recorder),
                ),
            );

            tokio::select! {
                seen = skipped_seen_rx => seen.expect("skipped event should reach recorder before grace expires"),
                result = running.as_mut() => panic!("turn finished before skipped recorder blocked: {result:?}"),
            }
            let error = timeout(Duration::from_millis(250), running.as_mut())
                .await
                .expect("post-provider explicit cancel must not wait for skipped journal recorder")
                .expect_err("cancelled turn should not commit");
            drop(running);
            error
        };
        assert!(started.elapsed() < Duration::from_millis(180));
        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    SessionTurnEvent::ToolCallSkipped { id, .. } => Some(id.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["toolu_skip_1", "toolu_skip_2", "toolu_skip_3"],
            "deadline fallback may only emit calls that the bounded recorder did not emit"
        );
    }

    #[tokio::test]
    async fn explicit_cancel_during_started_durable_record_does_not_dispatch_tool() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![tool_use(
                "toolu_started",
                "working_note",
                json!({"action": "add", "note": "must not run"}),
            )],
            ProviderStop::ToolUse,
        )]));
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = tool_loop_with_tools(provider, Arc::clone(&tools));
        let control = ToolBoundaryControl::new();
        let (started_seen_tx, started_seen_rx) = oneshot::channel();
        let (release_started_tx, release_started_rx) = oneshot::channel();
        let mut recorder = BlockingStartedRecorder {
            started_seen: Some(started_seen_tx),
            release_started: Some(release_started_rx),
        };
        let mut events = Vec::new();
        let mut emit = |event| events.push(event);
        let mut running = Box::pin(
            turn_loop.run_session_turn_with_tool_boundary_control_and_recorder(
                request(),
                &mut emit,
                Some(control.clone()),
                Some(&mut recorder),
            ),
        );

        tokio::select! {
            seen = started_seen_rx => seen.expect("Started should reach durable recorder"),
            result = running.as_mut() => panic!("turn finished before Started recorder blocked: {result:?}"),
        }
        let started = std::time::Instant::now();
        control.cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
        release_started_tx
            .send(())
            .expect("Started recorder should still await release");
        let error = timeout(Duration::from_millis(250), running.as_mut())
            .await
            .expect("Esc/Ctrl-C must use the original grace deadline")
            .expect_err("cancelled turn should not dispatch a Started-only tool");

        assert!(started.elapsed() < Duration::from_millis(180));
        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        drop(running);
        let notes = tools
            .dispatch("working_note", json!({"action": "list"}))
            .await
            .unwrap();
        assert_eq!(notes.output["notes"], json!([]));
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallSkipped { id, reason, .. }
                if id == "toolu_started"
                    && *reason == ToolCallSkipReason::TurnCancelledBeforeDispatch)
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallStarted { id, .. }
                | SessionTurnEvent::ToolCallCompleted { id, .. }
                | SessionTurnEvent::ToolCallInterrupted { id, .. }
                if id == "toolu_started")
        }));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn cancelled_turn_rolls_back_unsubmitted_terminal_process_output() {
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap(),
        );
        let session_id: crate::claim::SessionId = "session_1234abcd".parse().unwrap();
        let context = ToolDispatchContext {
            current_session_id: Some(session_id.clone()),
            ..ToolDispatchContext::default()
        };
        let started = tools
            .dispatch_with_context(
                "code_run",
                json!({"script": "sleep 0.5; printf final", "yield_time_ms": 250}),
                context.clone(),
            )
            .await
            .unwrap();
        let process_id = started.output["process_id"].as_str().unwrap().to_string();
        let initial_receipt = started
            .process_delivery_receipt
            .expect("direct fixture must acknowledge the initial code_run page");
        tools
            .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        tools
            .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        wait_for_process_to_become_terminal(tools.as_ref(), &context, &process_id).await;

        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![tool_use(
                "toolu_final",
                "write_stdin",
                json!({"process_id": process_id, "chars": "", "yield_time_ms": 1}),
            )],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop_with_tools(provider, Arc::clone(&tools));
        let control = ToolBoundaryControl::new();
        let cancel = control.clone();
        let mut request = request();
        request.current_session_id = Some(session_id);
        let mut emit = |event| {
            if matches!(event, SessionTurnEvent::ToolCallCompleted { ref id, .. } if id == "toolu_final")
            {
                cancel.cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
            }
        };
        let error = turn_loop
            .run_session_turn_with_tool_boundary_control(request, &mut emit, Some(control))
            .await
            .expect_err("cancellation before the next provider request must interrupt the turn");
        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());

        let retry = tools
            .dispatch_with_context(
                "write_stdin",
                json!({"process_id": process_id, "chars": "", "yield_time_ms": 1}),
                context,
            )
            .await
            .expect("unsubmitted terminal output must remain readable after cancellation");
        assert_eq!(retry.output["stdout"], "final");
    }

    #[tokio::test]
    async fn turn_loop_waits_for_assistant_completed_durable_record_before_returning() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("finished")],
            ProviderStop::Done,
        )]));
        let turn_loop = tool_loop(provider);
        let (completed_seen_tx, completed_seen_rx) = oneshot::channel();
        let (release_completed_tx, release_completed_rx) = oneshot::channel();
        let mut recorder = BlockingCompletedRecorder {
            target: BlockingRecordTarget::AssistantCompleted,
            completed_seen: Some(completed_seen_tx),
            release_completed: Some(release_completed_rx),
        };
        let mut events = Vec::new();
        let mut emit = |event| events.push(event);
        let running = turn_loop.run_session_turn_with_tool_boundary_control_and_recorder(
            request(),
            &mut emit,
            None,
            Some(&mut recorder),
        );
        tokio::pin!(running);

        tokio::select! {
            seen = completed_seen_rx => seen.unwrap(),
            result = &mut running => panic!(
                "turn returned before assistant completed event reached durable recorder: {result:?}"
            ),
        }
        tokio::select! {
            _ = sleep(Duration::from_millis(20)) => {}
            result = &mut running => panic!(
                "turn returned before assistant completed durable ack: {result:?}"
            ),
        }
        release_completed_tx.send(()).unwrap();

        let turn = running.await.unwrap();
        assert_eq!(
            turn.messages.last().map(|message| message.message.clone()),
            Some(SessionTurnMessage::assistant_text("finished"))
        );
    }

    #[tokio::test]
    async fn turn_loop_interrupts_after_provider_response_before_tools() {
        let control = ToolBoundaryControl::new();
        let provider = Arc::new(
            FakeProvider::new(vec![
                response(
                    vec![
                        tool_use(
                            "toolu_1",
                            "working_note",
                            json!({"action": "add", "note": "remember this"}),
                        ),
                        tool_use(
                            "toolu_2",
                            "working_note",
                            json!({"action": "add", "note": "second planned tool"}),
                        ),
                    ],
                    ProviderStop::ToolUse,
                ),
                response(
                    vec![SessionTurnContentBlock::text("should not run")],
                    ProviderStop::Done,
                ),
            ])
            .with_cancel_after_response(control.clone()),
        );
        let turn_loop = tool_loop(provider.clone());
        let mut events = Vec::new();

        let err = turn_loop
            .run_session_turn_with_tool_boundary_control(
                request(),
                &mut |event| events.push(event),
                Some(control),
            )
            .await
            .unwrap_err();

        assert!(err.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
        for id in ["toolu_1", "toolu_2"] {
            assert!(events.iter().any(|event| {
                matches!(
                    event,
                    SessionTurnEvent::ToolCallSkipped { id: skipped_id, reason, .. }
                        if skipped_id == id
                            && *reason == ToolCallSkipReason::TurnInterruptedBeforeDispatch
                )
            }));
            assert!(!events.iter().any(|event| {
                matches!(event, SessionTurnEvent::ToolCallStarted { id: started_id, .. } if started_id == id)
            }));
        }
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_1")));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_2")));
    }

    #[tokio::test]
    async fn skipped_recorder_failure_still_emits_a_terminal_event_for_every_queued_call() {
        let control = ToolBoundaryControl::new();
        let provider = Arc::new(
            FakeProvider::new(vec![response(
                vec![
                    tool_use("toolu_1", "working_note", json!({"action": "list"})),
                    tool_use("toolu_2", "working_note", json!({"action": "list"})),
                    tool_use("toolu_3", "working_note", json!({"action": "list"})),
                ],
                ProviderStop::ToolUse,
            )])
            .with_cancel_after_response(control.clone()),
        );
        let turn_loop = tool_loop(provider.clone());
        let mut recorder = FailingRecorder {
            target: FailingRecorderTarget::Skipped("toolu_1"),
            failed: false,
        };
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn_with_tool_boundary_control_and_recorder(
                request(),
                &mut |event| events.push(event),
                Some(control),
                Some(&mut recorder),
            )
            .await
            .expect_err("the first durable recorder failure must reject the turn");

        assert!(error
            .to_string()
            .contains("intentional durable recorder failure"));
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert_eq!(
            events
                .iter()
                .filter_map(|event| match event {
                    SessionTurnEvent::ToolCallSkipped { id, .. } => Some(id.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["toolu_1", "toolu_2", "toolu_3"]
        );
        assert!(!events
            .iter()
            .any(|event| matches!(event, SessionTurnEvent::ToolCallStarted { .. })));
    }

    #[test]
    fn late_progress_is_ignored_after_a_tool_reaches_its_terminal_state() {
        let terminal = HashSet::from(["toolu_done".to_string()]);
        let mut events = Vec::new();

        super::emit_tool_progress_if_active(
            ToolProgressUpdate {
                id: "toolu_done".into(),
                summary: "late".into(),
            },
            &terminal,
            &mut |event| events.push(event),
        );
        super::emit_tool_progress_if_active(
            ToolProgressUpdate {
                id: "toolu_running".into(),
                summary: "current".into(),
            },
            &terminal,
            &mut |event| events.push(event),
        );

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            SessionTurnEvent::ToolCallProgress { id, summary }
                if id == "toolu_running" && summary == "current"
        ));
    }

    #[tokio::test]
    async fn cancellation_between_legacy_check_and_dispatch_reservation_only_skips_tool() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![tool_use(
                "toolu_race",
                "working_note",
                json!({"action": "add", "note": "must stay absent"}),
            )],
            ProviderStop::ToolUse,
        )]));
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let hook = Arc::new(PauseBeforeDispatchReservation {
            entered: StdMutex::new(Some(entered_tx)),
            release: StdMutex::new(Some(release_rx)),
        });
        let turn_loop = tool_loop_with_tools(provider.clone(), tools.clone())
            .with_tool_dispatch_reservation_hook(hook);
        let control = ToolBoundaryControl::new();
        let mut events = Vec::new();
        let error = {
            let mut emit = |event| events.push(event);
            let running = turn_loop.run_session_turn_with_tool_boundary_control(
                request(),
                &mut emit,
                Some(control.clone()),
            );
            tokio::pin!(running);

            tokio::select! {
                entered = entered_rx => {
                    entered.expect("turn loop should pause before dispatch reservation");
                }
                result = &mut running => {
                    panic!("turn finished before dispatch reservation pause: {result:?}");
                }
            }
            control.cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
            release_tx
                .send(())
                .expect("reservation hook should still await release");
            running
                .as_mut()
                .await
                .expect_err("cancelled turn should not commit")
        };

        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ToolCallSkipped { id, reason, .. }
                    if id == "toolu_race"
                        && *reason == ToolCallSkipReason::TurnCancelledBeforeDispatch
            )
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallStarted { id, .. } if id == "toolu_race")
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_race")
        }));
        let notes = tools
            .dispatch("working_note", json!({"action": "list"}))
            .await
            .unwrap();
        assert_eq!(notes.output["notes"], json!([]));
    }

    #[tokio::test]
    async fn dispatch_reservation_wins_before_cancellation_and_skips_only_later_tools() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![
                tool_use(
                    "toolu_reserved",
                    "working_note",
                    json!({"action": "add", "note": "reserved note"}),
                ),
                tool_use(
                    "toolu_later",
                    "working_note",
                    json!({"action": "add", "note": "must stay absent"}),
                ),
            ],
            ProviderStop::ToolUse,
        )]));
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = tool_loop_with_tools(provider.clone(), tools.clone());
        let control = ToolBoundaryControl::new();
        let control_from_started = control.clone();
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn_with_tool_boundary_control(
                request(),
                &mut |event| {
                    if matches!(
                        event,
                        SessionTurnEvent::ToolCallStarted { ref id, .. } if id == "toolu_reserved"
                    ) {
                        control_from_started
                            .cancel(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
                    }
                    events.push(event);
                },
                Some(control),
            )
            .await
            .expect_err("cancelled turn should not commit");

        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallStarted { id, .. } if id == "toolu_reserved")
        }));
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_reserved")
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ToolCallSkipped { id, reason, .. }
                    if id == "toolu_later"
                        && *reason == ToolCallSkipReason::TurnInterruptedBeforeDispatch
            )
        }));
        let completed_position = events
            .iter()
            .position(|event| {
                matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_reserved")
            })
            .expect("reserved call should complete");
        let skipped_position = events
            .iter()
            .position(|event| {
                matches!(event, SessionTurnEvent::ToolCallSkipped { id, .. } if id == "toolu_later")
            })
            .expect("later call should be skipped");
        assert!(
            completed_position < skipped_position,
            "a ready completion must win its cancellation race before queued calls are skipped"
        );
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallStarted { id, .. } if id == "toolu_later")
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_later")
        }));
        let notes = tools
            .dispatch("working_note", json!({"action": "list"}))
            .await
            .unwrap();
        assert_eq!(notes.output["notes"], json!(["reserved note"]));
    }

    #[tokio::test]
    async fn steer_reservation_stays_linearized_while_started_durable_ack_waits() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![
                tool_use(
                    "toolu_reserved",
                    "working_note",
                    json!({"action": "add", "note": "reserved during ack"}),
                ),
                tool_use(
                    "toolu_later",
                    "working_note",
                    json!({"action": "add", "note": "must stay absent"}),
                ),
            ],
            ProviderStop::ToolUse,
        )]));
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = tool_loop_with_tools(provider.clone(), Arc::clone(&tools));
        let control = ToolBoundaryControl::new();
        let task_control = control.clone();
        let events = Arc::new(StdMutex::new(Vec::new()));
        let task_events = Arc::clone(&events);
        let (started_seen_tx, started_seen_rx) = oneshot::channel();
        let (release_started_tx, release_started_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut recorder = BlockingStartedRecorder {
                started_seen: Some(started_seen_tx),
                release_started: Some(release_started_rx),
            };
            let mut emit = move |event| {
                task_events
                    .lock()
                    .expect("event collector lock should not poison")
                    .push(event);
            };
            turn_loop
                .run_session_turn_with_tool_boundary_control_and_recorder(
                    request(),
                    &mut emit,
                    Some(task_control),
                    Some(&mut recorder),
                )
                .await
        });

        started_seen_rx
            .await
            .expect("Started durable record should reach the blocking ack");
        // 已 reserve 的调用属于 steer 的安全边界，Started journal 落盘后仍须真实执行；
        // Esc/Ctrl-C 的显式取消则由上面的 dedicated test 覆盖，不能复用这条语义。
        control.cancel_if_open(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
        release_started_tx
            .send(())
            .expect("Started durable record should still await release");

        let error = task
            .await
            .expect("turn worker should not panic")
            .expect_err("cancelled turn must not continue provider loop");
        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
        {
            let events = events
                .lock()
                .expect("event collector lock should not poison");
            let completed_position = events
                .iter()
                .position(|event| {
                    matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_reserved")
                })
                .expect("the reserved call should complete");
            let skipped_position = events
                .iter()
                .position(|event| {
                    matches!(event, SessionTurnEvent::ToolCallSkipped { id, .. } if id == "toolu_later")
                })
                .expect("the later call should be skipped");
            assert!(completed_position < skipped_position);
        }
        let notes = tools
            .dispatch("working_note", json!({"action": "list"}))
            .await
            .unwrap();
        assert_eq!(notes.output["notes"], json!(["reserved during ack"]));
    }

    #[tokio::test]
    async fn turn_loop_interrupts_provider_call_before_response() {
        let (started_tx, started_rx) = oneshot::channel();
        let provider = Arc::new(SlowProvider {
            requests: Mutex::new(Vec::new()),
            started: Mutex::new(Some(started_tx)),
        });
        let turn_loop = tool_loop(provider.clone());
        let control = ToolBoundaryControl::new();
        let cancel = control.clone();
        let mut events = Vec::new();
        let err = {
            let mut emit = |event| events.push(event);
            let running = turn_loop.run_session_turn_with_tool_boundary_control(
                request(),
                &mut emit,
                Some(control),
            );
            tokio::pin!(running);

            tokio::select! {
                started = started_rx => started.unwrap(),
                result = &mut running => panic!(
                    "turn finished before slow provider started: {result:?}"
                ),
            }
            cancel.cancel(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
            timeout(Duration::from_secs(1), &mut running)
                .await
                .unwrap()
                .unwrap_err()
        };

        assert!(err.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(events
            .iter()
            .all(|event| !matches!(event, SessionTurnEvent::AssistantMessageCompleted { .. })));
        assert!(events
            .iter()
            .all(|event| !matches!(event, SessionTurnEvent::ToolCallStarted { .. })));
    }

    #[tokio::test]
    async fn turn_loop_interrupts_wait_subagents_without_recording_a_normal_tool_result() {
        let dir = tempfile::tempdir().unwrap();
        let agents_root = dir.path().join("agents");
        let agent_id = crate::claim::AgentId::new("agent-a").unwrap();
        let session_id: crate::claim::SessionId = "session_1234abcd".parse().unwrap();
        crate::session::SessionStore::new(agents_root.clone())
            .create_with_id_factory(&agent_id, "system", || session_id.clone(), 1)
            .await
            .unwrap();
        let executor = Arc::new(BlockingSubagentExecutor {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        });
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap()
            .with_delegation_executor(
                agents_root.join("agent-a"),
                agent_id,
                executor.clone(),
                crate::delegation::DelegationRunnerConfig::default(),
            ),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_create",
                    "create_subagent",
                    json!({
                        "title": "blocking",
                        "role": "wait test",
                        "objective": "remain running until test cleanup"
                    }),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![
                    tool_use("toolu_wait", "wait_subagents", json!({"timeout_secs": 10})),
                    tool_use(
                        "toolu_after_wait",
                        "working_note",
                        json!({"action": "add", "note": "must not run"}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools.clone());
        let control = ToolBoundaryControl::new();
        let mut request = request();
        request.current_session_id = Some(session_id.clone());
        request.current_turn_id = Some("turn_wait".into());
        let (err, events) = {
            let (wait_started_tx, wait_started_rx) = oneshot::channel();
            let mut wait_started_tx = Some(wait_started_tx);
            let mut events = Vec::new();
            let mut emit = |event| {
                if matches!(
                    event,
                    SessionTurnEvent::ToolCallStarted { ref name, .. } if name == "wait_subagents"
                ) {
                    if let Some(tx) = wait_started_tx.take() {
                        let _ = tx.send(());
                    }
                }
                events.push(event);
            };
            let mut running = Box::pin(turn_loop.run_session_turn_with_tool_boundary_control(
                request,
                &mut emit,
                Some(control.clone()),
            ));

            tokio::select! {
                received = wait_started_rx => received.expect("wait_subagents start signal should be sent"),
                result = &mut running => panic!("turn finished before wait_subagents started: {result:?}"),
                _ = tokio::time::sleep(Duration::from_secs(1)) => panic!("wait_subagents should start"),
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
            control.cancel(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
            let err = tokio::time::timeout(Duration::from_secs(1), running.as_mut())
                .await
                .expect("wait_subagents should react to the turn cancellation")
                .unwrap_err();
            drop(running);
            (err, events)
        };
        assert!(err.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallStarted { id, name, .. }
                if id == "toolu_wait" && name == "wait_subagents")
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_wait")
        }));
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallInterrupted { id, summary }
                if id == "toolu_wait" && summary == "tool wait_subagents interrupted")
        }));
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallSkipped { id, .. }
                if id == "toolu_after_wait")
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallStarted { id, .. }
                if id == "toolu_after_wait")
        }));

        let abandoned = tools
            .abandon_delegations_for_session(&session_id, "test cleanup")
            .await
            .unwrap();
        assert_eq!(abandoned, 1);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn ordinary_tool_completes_before_pending_interrupt_ends_turn() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![
                tool_use(
                    "toolu_code",
                    "code_run",
                    json!({"script": "sleep 0.1; printf DONE", "yield_time_ms": 1000}),
                ),
                tool_use(
                    "toolu_after_code",
                    "working_note",
                    json!({"action": "add", "note": "must not run"}),
                ),
            ],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop(provider);
        let control = ToolBoundaryControl::new();
        let (err, events) = {
            let (started_tx, started_rx) = oneshot::channel();
            let mut started_tx = Some(started_tx);
            let mut events = Vec::new();
            let mut emit = |event| {
                if matches!(
                    event,
                    SessionTurnEvent::ToolCallStarted { ref id, .. } if id == "toolu_code"
                ) {
                    if let Some(tx) = started_tx.take() {
                        let _ = tx.send(());
                    }
                }
                events.push(event);
            };
            let mut running = Box::pin(turn_loop.run_session_turn_with_tool_boundary_control(
                request(),
                &mut emit,
                Some(control.clone()),
            ));

            tokio::select! {
                received = started_rx => received.expect("code_run start signal should be sent"),
                result = &mut running => panic!("turn finished before code_run started: {result:?}"),
            }
            control.cancel_if_open(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
            let err = tokio::time::timeout(Duration::from_secs(2), running.as_mut())
                .await
                .expect("ordinary tool should finish before the turn is interrupted")
                .unwrap_err();
            drop(running);
            (err, events)
        };

        assert!(err.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. }
                if id == "toolu_code")
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallInterrupted { id, .. }
                if id == "toolu_code")
        }));
        assert!(events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallSkipped { id, .. }
                if id == "toolu_after_code")
        }));
        assert!(!events.iter().any(|event| {
            matches!(event, SessionTurnEvent::ToolCallStarted { id, .. }
                if id == "toolu_after_code")
        }));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn provider_success_commits_final_write_stdin_delivery_and_removes_terminal_entry() {
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap(),
        );
        let session_id: crate::claim::SessionId = "session_1234abcd".parse().unwrap();
        let context = ToolDispatchContext {
            current_session_id: Some(session_id.clone()),
            ..ToolDispatchContext::default()
        };
        let running = tools
            .dispatch_with_context(
                "code_run",
                json!({"script": "sleep 0.5; printf final", "yield_time_ms": 250}),
                context.clone(),
            )
            .await
            .unwrap();
        let process_id = running.output["process_id"].as_str().unwrap().to_string();
        let initial_receipt = running
            .process_delivery_receipt
            .expect("direct fixture must acknowledge the initial code_run page");
        tools
            .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        tools
            .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        wait_for_process_to_become_terminal(tools.as_ref(), &context, &process_id).await;

        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_final",
                    "write_stdin",
                    json!({"process_id": process_id, "chars": "", "yield_time_ms": 250}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("final result consumed")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, Arc::clone(&tools));
        let mut request = request();
        request.current_session_id = Some(session_id);
        let turn = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap();
        assert_eq!(
            tool_result_content(non_context_messages(&turn)[2], "toolu_final")["output"]["stdout"],
            "final"
        );

        let error = tools
            .dispatch_with_context(
                "write_stdin",
                json!({"process_id": process_id, "chars": ""}),
                context,
            )
            .await
            .expect_err("successful provider response should commit and remove terminal entry");
        assert!(error.to_string().contains("not a live process owned"));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn projected_process_tool_result_does_not_commit_unseen_output() {
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap(),
        );
        let session_id: crate::claim::SessionId = "session_1234abcd".parse().unwrap();
        let context = ToolDispatchContext {
            current_session_id: Some(session_id.clone()),
            ..ToolDispatchContext::default()
        };
        let running = tools
            .dispatch_with_context(
                "code_run",
                json!({"script": "sleep 0.5; printf final", "yield_time_ms": 250}),
                context.clone(),
            )
            .await
            .unwrap();
        let process_id = running.output["process_id"].as_str().unwrap().to_string();
        let initial_receipt = running
            .process_delivery_receipt
            .expect("direct fixture must acknowledge the initial code_run page");
        tools
            .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        tools
            .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        wait_for_process_to_become_terminal(tools.as_ref(), &context, &process_id).await;

        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_final",
                    "write_stdin",
                    json!({"process_id": process_id, "chars": "", "yield_time_ms": 250}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("projected result consumed")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), Arc::clone(&tools));
        let replacement = "[large tool_result omitted from raw compact tail]".to_string();
        let mut preflight = ReplaceProcessToolResultPreflight {
            calls: 0,
            tool_use_id: "toolu_final".into(),
            replacement: replacement.clone(),
        };
        let mut request = request();
        request.current_session_id = Some(session_id);
        turn_loop
            .run_session_turn_with_hooks(request, &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        let projected_content = requests[1]
            .messages
            .iter()
            .rev()
            .flat_map(|message| message.content.iter().rev())
            .find_map(|block| match block {
                SessionTurnContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } if tool_use_id == "toolu_final" => Some(content.as_str()),
                _ => None,
            })
            .unwrap();
        assert_eq!(projected_content, replacement);
        drop(requests);

        let retry = tools
            .dispatch_with_context(
                "write_stdin",
                json!({"process_id": process_id, "chars": "", "yield_time_ms": 1}),
                context,
            )
            .await
            .expect("provider 未看到原始 tool_result 时，终态输出必须可原样重读");
        assert_eq!(retry.output["stdout"], "final");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn duplicate_process_polls_cannot_overwrite_a_projected_out_first_page() {
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap(),
        );
        let session_id: crate::claim::SessionId = "session_1234abcd".parse().unwrap();
        let context = ToolDispatchContext {
            current_session_id: Some(session_id.clone()),
            ..ToolDispatchContext::default()
        };
        let running = tools
            .dispatch_with_context(
                "code_run",
                json!({"script": "sleep 0.5; printf 0123456789", "yield_time_ms": 250}),
                context.clone(),
            )
            .await
            .unwrap();
        let process_id = running.output["process_id"].as_str().unwrap().to_string();
        let initial_receipt = running
            .process_delivery_receipt
            .expect("direct fixture must acknowledge the initial code_run page");
        tools
            .begin_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        tools
            .commit_process_deliveries(std::slice::from_ref(&initial_receipt))
            .await;
        wait_for_process_to_become_terminal(tools.as_ref(), &context, &process_id).await;

        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_page_1",
                        "write_stdin",
                        json!({
                            "process_id": process_id,
                            "chars": "",
                            "yield_time_ms": 1,
                            "max_output_chars": 3,
                        }),
                    ),
                    tool_use(
                        "toolu_page_2",
                        "write_stdin",
                        json!({
                            "process_id": process_id,
                            "chars": "",
                            "yield_time_ms": 1,
                            "max_output_chars": 3,
                        }),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("projected batch consumed")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), Arc::clone(&tools));
        let replacement = "[large tool_result omitted from raw compact tail]".to_string();
        let mut preflight = ReplaceProcessToolResultPreflight {
            calls: 0,
            tool_use_id: "toolu_page_1".into(),
            replacement: replacement.clone(),
        };
        let mut request = request();
        request.current_session_id = Some(session_id);
        turn_loop
            .run_session_turn_with_hooks(request, &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .unwrap();

        let requests = provider.requests.lock().await;
        let second_request = &requests[1].messages;
        let first_content = second_request
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                SessionTurnContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } if tool_use_id == "toolu_page_1" => Some(content.as_str()),
                _ => None,
            })
            .unwrap();
        assert_eq!(first_content, replacement);
        let second_content = second_request
            .iter()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                SessionTurnContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } if tool_use_id == "toolu_page_2" => Some(content.as_str()),
                _ => None,
            })
            .unwrap();
        assert!(second_content.contains("already called for this process"));
        drop(requests);

        let retry = tools
            .dispatch_with_context(
                "write_stdin",
                json!({
                    "process_id": process_id,
                    "chars": "",
                    "yield_time_ms": 1,
                    "max_output_chars": 3,
                }),
                context,
            )
            .await
            .expect("projected-out first page must remain available after the provider response");
        assert_eq!(retry.output["stdout"], "012");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn duplicate_explicit_cursor_poll_is_rejected_but_another_process_is_allowed() {
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap(),
        );
        let session_id: crate::claim::SessionId = "session_1234abcd".parse().unwrap();
        let context = ToolDispatchContext {
            current_session_id: Some(session_id.clone()),
            ..ToolDispatchContext::default()
        };

        let first = tools
            .dispatch_with_context(
                "code_run",
                json!({"script": "printf abc; sleep 5", "yield_time_ms": 250}),
                context.clone(),
            )
            .await
            .unwrap();
        let first_process_id = first.output["process_id"].as_str().unwrap().to_string();
        let first_receipt = first
            .process_delivery_receipt
            .expect("initial first-process output must be acknowledged");
        tools
            .begin_process_deliveries(std::slice::from_ref(&first_receipt))
            .await;
        tools
            .commit_process_deliveries(std::slice::from_ref(&first_receipt))
            .await;

        let second = tools
            .dispatch_with_context(
                "code_run",
                json!({"script": "printf xyz; sleep 5", "yield_time_ms": 250}),
                context,
            )
            .await
            .unwrap();
        let second_process_id = second.output["process_id"].as_str().unwrap().to_string();
        let second_receipt = second
            .process_delivery_receipt
            .expect("initial second-process output must be acknowledged");
        tools
            .begin_process_deliveries(std::slice::from_ref(&second_receipt))
            .await;
        tools
            .commit_process_deliveries(std::slice::from_ref(&second_receipt))
            .await;

        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_first_poll",
                        "write_stdin",
                        json!({
                            "process_id": first_process_id.clone(),
                            "stdout_cursor": 3,
                            "stderr_cursor": 0,
                            "yield_time_ms": 1,
                        }),
                    ),
                    tool_use(
                        "toolu_duplicate_poll",
                        "write_stdin",
                        json!({
                            "process_id": first_process_id,
                            "stdout_cursor": 3,
                            "stderr_cursor": 0,
                            "yield_time_ms": 1,
                        }),
                    ),
                    tool_use(
                        "toolu_other_process_poll",
                        "write_stdin",
                        json!({
                            "process_id": second_process_id,
                            "stdout_cursor": 3,
                            "stderr_cursor": 0,
                            "yield_time_ms": 1,
                        }),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("polls consumed")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, Arc::clone(&tools));
        let mut request = request();
        request.current_session_id = Some(session_id.clone());
        let turn = turn_loop
            .run_session_turn(request, &mut |_| {})
            .await
            .unwrap();

        let tool_results = turn
            .messages
            .iter()
            .flat_map(|message| message.content.iter())
            .filter_map(|block| match block {
                SessionTurnContentBlock::ToolResult {
                    tool_use_id,
                    content,
                } => Some((
                    tool_use_id.as_str(),
                    serde_json::from_str::<Value>(content).unwrap(),
                )),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(tool_results["toolu_first_poll"]["ok"], true);
        assert_eq!(tool_results["toolu_duplicate_poll"]["ok"], false);
        assert!(tool_results["toolu_duplicate_poll"]["error"]
            .as_str()
            .unwrap()
            .contains("already called for this process"));
        assert_eq!(tool_results["toolu_other_process_poll"]["ok"], true);

        tools.cleanup_processes_for_session(&session_id).await;
    }

    #[tokio::test]
    async fn turn_loop_assigns_completed_at_when_each_message_is_complete() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "working_note",
                    json!({"action": "add", "note": "remember this"}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("finished")],
                ProviderStop::Done,
            ),
        ]));
        let clock_inputs = vec![
            "2026-06-17T09:33:03.718103Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
            "2026-06-17T09:33:04.000001Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
            "2026-06-17T09:33:05.000002Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
            "2026-06-17T09:33:06.123456Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
            "2026-06-17T09:33:07.000003Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
            "2026-06-17T09:33:08.000004Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
            "2026-06-17T09:33:09.000005Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        ];
        let expected_times = vec![
            clock_inputs[0],
            clock_inputs[1],
            clock_inputs[3],
            clock_inputs[4],
            clock_inputs[6],
        ];
        let clock_values = Arc::new(StdMutex::new(VecDeque::from(clock_inputs)));
        let turn_loop = {
            let clock_values = Arc::clone(&clock_values);
            tool_loop(provider).with_now_fn(move || {
                clock_values
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("test clock exhausted")
            })
        };

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        let actual_times = turn
            .messages
            .iter()
            .map(|message| message.completed_at)
            .collect::<Vec<_>>();
        assert_eq!(actual_times, expected_times);
        assert_eq!(
            non_context_messages(&turn)[0],
            &SessionTurnMessage::user_text("hello")
        );
        assert_eq!(non_context_messages(&turn)[1].role, "assistant");
        assert_eq!(non_context_messages(&turn)[2].role, "user");
        assert_eq!(
            non_context_messages(&turn)[3],
            &SessionTurnMessage::assistant_text("finished")
        );
        assert!(clock_values.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn turn_loop_groups_multiple_tool_results_in_one_user_message() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_1",
                        "working_note",
                        json!({"action": "add", "note": "a"}),
                    ),
                    tool_use(
                        "toolu_2",
                        "working_note",
                        json!({"action": "add", "note": "b"}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider);

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        assert_eq!(non_context_messages(&turn).len(), 4);
        assert_eq!(non_context_messages(&turn)[2].role, "user");
        assert_eq!(non_context_messages(&turn)[2].content.len(), 2);
        assert_eq!(
            tool_result_content(non_context_messages(&turn)[2], "toolu_1")["ok"],
            true
        );
        assert_eq!(
            tool_result_content(non_context_messages(&turn)[2], "toolu_2")["ok"],
            true
        );
    }

    #[tokio::test]
    async fn adjacent_safe_tools_respect_parallel_limit_and_started_source_order() {
        let mut server = ParallelFetchServer::start(&["one", "two", "three"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                max_parallel_tool_calls: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use("toolu_one", "web_fetch", json!({"url": server.url("one")})),
                    tool_use("toolu_two", "web_fetch", json!({"url": server.url("two")})),
                    tool_use(
                        "toolu_three",
                        "web_fetch",
                        json!({"url": server.url("three")}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let events = Arc::new(StdMutex::new(Vec::new()));
        let events_for_emit = Arc::clone(&events);
        let task = tokio::spawn(async move {
            let mut emit = move |event| {
                events_for_emit
                    .lock()
                    .expect("event collector lock should not poison")
                    .push(event);
            };
            turn_loop.run_session_turn(request(), &mut emit).await
        });

        timeout(Duration::from_secs(1), server.wait_for_starts(2))
            .await
            .expect("the first two safe calls should start");
        assert_eq!(server.max_active(), 2);
        assert_eq!(server.started_count(), 2);
        assert_eq!(
            started_tool_ids(
                &events
                    .lock()
                    .expect("event collector lock should not poison"),
            ),
            vec!["toolu_one", "toolu_two"]
        );
        assert!(
            timeout(Duration::from_millis(80), server.wait_for_starts(3))
                .await
                .is_err(),
            "the queued third safe call must not be dispatched before a slot is released"
        );

        server.release("one");
        server.release("two");
        timeout(Duration::from_secs(1), server.wait_for_starts(3))
            .await
            .expect("the third safe call should start after a slot is released");
        server.release("three");
        let turn = timeout(Duration::from_secs(2), task)
            .await
            .expect("the bounded batch should complete")
            .expect("turn worker should not panic")
            .unwrap();

        assert_eq!(server.max_active(), 2);
        assert_eq!(provider.requests.lock().await.len(), 2);
        let events = events
            .lock()
            .expect("event collector lock should not poison");
        assert_eq!(
            started_tool_ids(&events),
            vec!["toolu_one", "toolu_two", "toolu_three"]
        );
        assert_eq!(completed_tool_ids(&events).len(), 3);
        let result_ids = non_context_messages(&turn)[2]
            .content
            .iter()
            .filter_map(|block| match block {
                SessionTurnContentBlock::ToolResult { tool_use_id, .. } => {
                    Some(tool_use_id.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(result_ids, vec!["toolu_one", "toolu_two", "toolu_three"]);
    }

    #[tokio::test]
    async fn barrier_waits_for_prior_safe_batch_and_keeps_web_request_serial() {
        let mut server = ParallelFetchServer::start(&["safe", "barrier", "after"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                max_parallel_tool_calls: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_safe",
                        "web_fetch",
                        json!({"url": server.url("safe")}),
                    ),
                    tool_use(
                        "toolu_barrier",
                        "web_request",
                        json!({"method": "GET", "url": server.url("barrier")}),
                    ),
                    tool_use(
                        "toolu_after",
                        "web_fetch",
                        json!({"url": server.url("after")}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let events = Arc::new(StdMutex::new(Vec::new()));
        let events_for_emit = Arc::clone(&events);
        let task = tokio::spawn(async move {
            let mut emit = move |event| {
                events_for_emit
                    .lock()
                    .expect("event collector lock should not poison")
                    .push(event);
            };
            turn_loop.run_session_turn(request(), &mut emit).await
        });

        timeout(Duration::from_secs(1), server.wait_for_starts(1))
            .await
            .expect("the initial safe call should start");
        assert_eq!(
            started_tool_ids(
                &events
                    .lock()
                    .expect("event collector lock should not poison"),
            ),
            vec!["toolu_safe"]
        );
        assert!(
            timeout(Duration::from_millis(80), server.wait_for_starts(2))
                .await
                .is_err(),
            "a web_request GET is a Barrier and must not start before the prior batch ends"
        );

        server.release("safe");
        timeout(Duration::from_secs(1), server.wait_for_starts(2))
            .await
            .expect("the barrier should start after the safe call completes");
        server.release("barrier");
        timeout(Duration::from_secs(1), server.wait_for_starts(3))
            .await
            .expect("the following safe batch should start after the barrier completes");
        server.release("after");
        timeout(Duration::from_secs(2), task)
            .await
            .expect("the barrier sequence should complete")
            .expect("turn worker should not panic")
            .unwrap();

        let events = events
            .lock()
            .expect("event collector lock should not poison");
        assert_eq!(
            started_tool_ids(&events),
            vec!["toolu_safe", "toolu_barrier", "toolu_after"]
        );
        let completed_safe = events
            .iter()
            .position(|event| {
                matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_safe")
            })
            .unwrap();
        let started_barrier = events
            .iter()
            .position(|event| {
                matches!(event, SessionTurnEvent::ToolCallStarted { id, .. } if id == "toolu_barrier")
            })
            .unwrap();
        let completed_barrier = events
            .iter()
            .position(|event| {
                matches!(event, SessionTurnEvent::ToolCallCompleted { id, .. } if id == "toolu_barrier")
            })
            .unwrap();
        let started_after = events
            .iter()
            .position(|event| {
                matches!(event, SessionTurnEvent::ToolCallStarted { id, .. } if id == "toolu_after")
            })
            .unwrap();
        assert!(completed_safe < started_barrier);
        assert!(completed_barrier < started_after);
    }

    #[tokio::test]
    async fn concurrent_completion_can_be_out_of_order_but_results_return_in_source_order() {
        let mut server = ParallelFetchServer::start(&["slow", "fast"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                max_parallel_tool_calls: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_slow",
                        "web_fetch",
                        json!({"url": server.url("slow")}),
                    ),
                    tool_use(
                        "toolu_fast",
                        "web_fetch",
                        json!({"url": server.url("fast")}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let events = Arc::new(StdMutex::new(Vec::new()));
        let events_for_emit = Arc::clone(&events);
        let task = tokio::spawn(async move {
            let mut emit = move |event| {
                events_for_emit
                    .lock()
                    .expect("event collector lock should not poison")
                    .push(event);
            };
            turn_loop.run_session_turn(request(), &mut emit).await
        });

        timeout(Duration::from_secs(1), server.wait_for_starts(2))
            .await
            .expect("both safe calls should start together");
        server.release("fast");
        timeout(Duration::from_secs(1), async {
            loop {
                let completed = completed_tool_ids(
                    &events
                        .lock()
                        .expect("event collector lock should not poison"),
                );
                if completed == ["toolu_fast"] {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("fast completion should be visible before slow is released");
        assert_eq!(provider.requests.lock().await.len(), 1);

        server.release("slow");
        let turn = timeout(Duration::from_secs(2), task)
            .await
            .expect("both calls should complete")
            .expect("turn worker should not panic")
            .unwrap();

        let events = events
            .lock()
            .expect("event collector lock should not poison");
        assert_eq!(
            completed_tool_ids(&events),
            vec!["toolu_fast", "toolu_slow"]
        );
        let result_ids = non_context_messages(&turn)[2]
            .content
            .iter()
            .filter_map(|block| match block {
                SessionTurnContentBlock::ToolResult { tool_use_id, .. } => {
                    Some(tool_use_id.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(result_ids, vec!["toolu_slow", "toolu_fast"]);
    }

    #[tokio::test]
    async fn safe_tool_failure_does_not_cancel_other_calls_in_its_batch() {
        let mut server = ParallelFetchServer::start(&["good"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                max_parallel_tool_calls: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_bad_url",
                        "web_fetch",
                        json!({"url": "ftp://not-supported.example"}),
                    ),
                    tool_use(
                        "toolu_good_url",
                        "web_fetch",
                        json!({"url": server.url("good")}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let task = tokio::spawn(async move {
            let mut emit = |_| {};
            turn_loop.run_session_turn(request(), &mut emit).await
        });

        timeout(Duration::from_secs(1), server.wait_for_starts(1))
            .await
            .expect("the healthy call should start despite its sibling failure");
        server.release("good");
        let turn = timeout(Duration::from_secs(2), task)
            .await
            .expect("the batch should settle after both outcomes")
            .expect("turn worker should not panic")
            .unwrap();

        assert_eq!(provider.requests.lock().await.len(), 2);
        assert_eq!(
            tool_result_content(non_context_messages(&turn)[2], "toolu_bad_url")["ok"],
            false
        );
        assert_eq!(
            tool_result_content(non_context_messages(&turn)[2], "toolu_good_url")["ok"],
            true
        );
    }

    #[tokio::test]
    async fn cancelling_a_parallel_batch_skips_queued_calls_and_drains_started_calls() {
        let mut server = ParallelFetchServer::start(&["one", "two", "three"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                max_parallel_tool_calls: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![
                tool_use("toolu_one", "web_fetch", json!({"url": server.url("one")})),
                tool_use("toolu_two", "web_fetch", json!({"url": server.url("two")})),
                tool_use(
                    "toolu_three",
                    "web_fetch",
                    json!({"url": server.url("three")}),
                ),
            ],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let control = ToolBoundaryControl::new();
        let control_for_turn = control.clone();
        let events = Arc::new(StdMutex::new(Vec::new()));
        let events_for_emit = Arc::clone(&events);
        let task = tokio::spawn(async move {
            let mut emit = move |event| {
                events_for_emit
                    .lock()
                    .expect("event collector lock should not poison")
                    .push(event);
            };
            turn_loop
                .run_session_turn_with_tool_boundary_control(
                    request(),
                    &mut emit,
                    Some(control_for_turn),
                )
                .await
        });

        timeout(Duration::from_secs(1), server.wait_for_starts(2))
            .await
            .expect("two calls should occupy the configured slots");
        control.cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
        timeout(Duration::from_secs(1), async {
            loop {
                if events.lock().expect("event collector lock should not poison").iter().any(|event| {
                    matches!(event, SessionTurnEvent::ToolCallSkipped { id, reason, .. }
                        if id == "toolu_three" && *reason == ToolCallSkipReason::TurnCancelledBeforeDispatch)
                }) {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the queued call should become skipped immediately after cancellation");
        assert!(!started_tool_ids(
            &events
                .lock()
                .expect("event collector lock should not poison"),
        )
        .contains(&"toolu_three".to_string()));

        server.release("one");
        server.release("two");
        let error = timeout(Duration::from_secs(2), task)
            .await
            .expect("started non-cooperative calls should settle before cancellation returns")
            .expect("turn worker should not panic")
            .expect_err("cancelled turn must not commit a tool-result loop");
        assert!(error.downcast_ref::<SessionTurnInterrupted>().is_some());
        assert_eq!(provider.requests.lock().await.len(), 1);
        let events = events
            .lock()
            .expect("event collector lock should not poison");
        let completed = completed_tool_ids(&events);
        assert!(completed.contains(&"toolu_one".to_string()));
        assert!(completed.contains(&"toolu_two".to_string()));
    }

    #[tokio::test]
    async fn durable_started_record_failure_drains_prior_started_calls_without_dispatching_failed_one(
    ) {
        let mut server = ParallelFetchServer::start(&["one", "two"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                max_parallel_tool_calls: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use("toolu_one", "web_fetch", json!({"url": server.url("one")})),
                    tool_use("toolu_two", "web_fetch", json!({"url": server.url("two")})),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("must not be requested")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let events = Arc::new(StdMutex::new(Vec::new()));
        let events_for_emit = Arc::clone(&events);
        let task = tokio::spawn(async move {
            let mut recorder = FailingRecorder {
                target: FailingRecorderTarget::Started("toolu_two"),
                failed: false,
            };
            let mut emit = move |event| {
                events_for_emit
                    .lock()
                    .expect("event collector lock should not poison")
                    .push(event);
            };
            turn_loop
                .run_session_turn_with_tool_boundary_control_and_recorder(
                    request(),
                    &mut emit,
                    None,
                    Some(&mut recorder),
                )
                .await
        });

        timeout(Duration::from_secs(1), server.wait_for_starts(1))
            .await
            .expect("the durably started call should still be drained");
        sleep(Duration::from_millis(30)).await;
        assert_eq!(
            server.started_count(),
            1,
            "the call whose Started record failed must not execute"
        );
        server.release("one");
        let error = timeout(Duration::from_secs(2), task)
            .await
            .expect("started calls should settle before recorder failure returns")
            .expect("turn worker should not panic")
            .expect_err("durable recorder failure should reject provider continuation");
        assert!(error
            .to_string()
            .contains("intentional durable recorder failure"));
        assert_eq!(provider.requests.lock().await.len(), 1);
        let events = events
            .lock()
            .expect("event collector lock should not poison");
        assert_eq!(started_tool_ids(&events), vec!["toolu_one"]);
        assert_eq!(completed_tool_ids(&events), vec!["toolu_one"]);
    }

    #[tokio::test]
    async fn first_durable_started_record_failure_prevents_barrier_dispatch() {
        let server = ParallelFetchServer::start(&["one"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![tool_use(
                "toolu_one",
                "web_request",
                json!({"method": "GET", "url": server.url("one")}),
            )],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let mut recorder = FailingRecorder {
            target: FailingRecorderTarget::Started("toolu_one"),
            failed: false,
        };
        let events = Arc::new(StdMutex::new(Vec::new()));
        let events_for_emit = Arc::clone(&events);
        let mut emit = move |event| {
            events_for_emit
                .lock()
                .expect("event collector lock should not poison")
                .push(event);
        };

        let error = turn_loop
            .run_session_turn_with_tool_boundary_control_and_recorder(
                request(),
                &mut emit,
                None,
                Some(&mut recorder),
            )
            .await
            .expect_err("durable Started failure should reject the turn");

        assert!(error
            .to_string()
            .contains("intentional durable recorder failure"));
        sleep(Duration::from_millis(30)).await;
        assert_eq!(server.started_count(), 0);
        assert_eq!(provider.requests.lock().await.len(), 1);
        assert!(started_tool_ids(
            &events
                .lock()
                .expect("event collector lock should not poison")
        )
        .is_empty());
    }

    #[tokio::test]
    async fn durable_completed_record_failure_waits_for_other_started_parallel_calls() {
        let mut server = ParallelFetchServer::start(&["slow", "fast"]).await;
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                max_parallel_tool_calls: 2,
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    tool_use(
                        "toolu_slow",
                        "web_fetch",
                        json!({"url": server.url("slow")}),
                    ),
                    tool_use(
                        "toolu_fast",
                        "web_fetch",
                        json!({"url": server.url("fast")}),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("must not be requested")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider.clone(), tools);
        let events = Arc::new(StdMutex::new(Vec::new()));
        let events_for_emit = Arc::clone(&events);
        let task = tokio::spawn(async move {
            let mut recorder = FailingRecorder {
                target: FailingRecorderTarget::Completed("toolu_fast"),
                failed: false,
            };
            let mut emit = move |event| {
                events_for_emit
                    .lock()
                    .expect("event collector lock should not poison")
                    .push(event);
            };
            turn_loop
                .run_session_turn_with_tool_boundary_control_and_recorder(
                    request(),
                    &mut emit,
                    None,
                    Some(&mut recorder),
                )
                .await
        });

        timeout(Duration::from_secs(1), server.wait_for_starts(2))
            .await
            .expect("both calls should start");
        server.release("fast");
        timeout(Duration::from_secs(1), async {
            loop {
                if completed_tool_ids(
                    &events
                        .lock()
                        .expect("event collector lock should not poison"),
                ) == ["toolu_fast"]
                {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the failed durable completed write should happen after fast completion");
        assert!(
            !task.is_finished(),
            "the slow Started call must not be dropped when another terminal write fails"
        );

        server.release("slow");
        let error = timeout(Duration::from_secs(2), task)
            .await
            .expect("slow call should settle before recorder failure returns")
            .expect("turn worker should not panic")
            .expect_err("durable recorder failure should reject provider continuation");
        assert!(error
            .to_string()
            .contains("intentional durable recorder failure"));
        assert_eq!(provider.requests.lock().await.len(), 1);
        let events = events
            .lock()
            .expect("event collector lock should not poison");
        assert_eq!(
            completed_tool_ids(&events),
            vec!["toolu_fast", "toolu_slow"]
        );
    }

    #[tokio::test]
    async fn turn_loop_turns_tool_failure_into_error_tool_result() {
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use("toolu_1", "missing_tool", json!({}))],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider);

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_1");

        assert_eq!(result["ok"], false);
        assert_eq!(result["outcome"]["kind"], "dispatch_failure");
        assert!(result["error"].as_str().unwrap().contains("工具不存在"));
    }

    #[tokio::test]
    async fn turn_loop_stops_when_max_tool_loop_turns_is_reached() {
        let responses = (0..20)
            .map(|index| {
                response(
                    vec![tool_use(
                        &format!("toolu_{index}"),
                        "working_note",
                        json!({"action": "add", "note": format!("note {index}")}),
                    )],
                    ProviderStop::ToolUse,
                )
            })
            .collect::<Vec<_>>();
        let provider = Arc::new(FakeProvider::new(responses));
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop =
            AgentTurnLoop::new(provider.clone(), tools, 1024).with_max_tool_loop_turns(20);
        let mut events = Vec::new();

        let err = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("run_session_turn 达到最大 tool 循环轮数: 20"));
        assert_eq!(provider.requests.lock().await.len(), 20);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SessionTurnEvent::ToolCallStarted { .. }))
                .count(),
            19
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SessionTurnEvent::ToolCallCompleted { .. }))
                .count(),
            19
        );
    }

    #[tokio::test]
    async fn turn_loop_without_limit_continues_past_legacy_default() {
        let mut responses = (0..33)
            .map(|index| {
                response(
                    vec![tool_use(
                        &format!("toolu_{index}"),
                        "working_note",
                        json!({"action": "add", "note": format!("note {index}")}),
                    )],
                    ProviderStop::ToolUse,
                )
            })
            .collect::<Vec<_>>();
        responses.push(response(
            vec![SessionTurnContentBlock::text("完成")],
            ProviderStop::Done,
        ));
        let provider = Arc::new(FakeProvider::new(responses));
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(provider.clone(), tools, 1024);

        let turn = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap();

        assert_eq!(provider.requests.lock().await.len(), 34);
        assert_eq!(non_context_messages(&turn).len(), 68);
    }

    #[tokio::test]
    async fn turn_loop_preserves_typed_business_failure_output() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("note.txt"), "alpha\n")
            .await
            .unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![
                    // 先 file_read 建立写前 read state，再 file_patch 命中 0 匹配失败。
                    tool_use("toolu_0", "file_read", json!({ "path": "note.txt" })),
                    tool_use(
                        "toolu_1",
                        "file_patch",
                        json!({
                            "path": "note.txt",
                            "old_content": "missing",
                            "new_content": "beta",
                        }),
                    ),
                ],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let mut events = Vec::new();
        let mut request = request();
        request.current_session_id = Some("session_aaaaaaaa".parse().unwrap());

        let turn = turn_loop
            .run_session_turn(request, &mut |event| events.push(event))
            .await
            .unwrap();
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_1");

        assert_eq!(result["ok"], false);
        assert_eq!(result["outcome"]["kind"], "business_failure");
        assert!(result["output"]["msg"]
            .as_str()
            .unwrap()
            .contains("未找到匹配"));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ToolCallCompleted { id, summary, outcome, .. }
                    if id == "toolu_1"
                        && summary.contains("business_failed")
                        && *outcome == ToolExecutionOutcome::BusinessFailure
            )
        }));
    }

    #[tokio::test]
    async fn turn_loop_preserves_structured_memory_business_failure() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFsMemoryStore::new(
            dir.path().to_path_buf(),
            10,
            100,
            true,
        ));
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap()
            .with_memory_store(store),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "memory",
                    json!({
                        "action": "add",
                        "target": "memory",
                        "content": "safe but too long",
                    }),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_1");

        assert_eq!(result["ok"], false);
        assert_eq!(result["outcome"]["kind"], "business_failure");
        assert_eq!(result["output"]["success"], false);
        assert_eq!(result["output"]["cap"], 10);
        assert!(result["output"]["need_free"].is_number());
        assert!(result["output"]["current_entries"].is_array());
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ToolCallCompleted {
                    outcome: ToolExecutionOutcome::BusinessFailure,
                    ..
                }
            )
        }));
    }

    #[tokio::test]
    async fn turn_loop_uses_same_nonzero_process_outcome_for_model_and_event() {
        let dir = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: dir.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        );
        let provider = Arc::new(FakeProvider::new(vec![
            response(
                vec![tool_use(
                    "toolu_1",
                    "code_run",
                    json!({"script": "printf diagnostic >&2; exit 7", "yield_time_ms": 1000}),
                )],
                ProviderStop::ToolUse,
            ),
            response(
                vec![SessionTurnContentBlock::text("done")],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop_with_tools(provider, tools);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(request(), &mut |event| events.push(event))
            .await
            .unwrap();
        let result = tool_result_content(non_context_messages(&turn)[2], "toolu_1");

        assert_eq!(result["ok"], false);
        assert_eq!(result["outcome"]["kind"], "process_exit");
        assert_eq!(result["outcome"]["exit_code"], 7);
        assert_eq!(result["output"]["exit_code"], 7);
        assert_eq!(result["output"]["stderr"], "diagnostic");
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ToolCallCompleted {
                    outcome: ToolExecutionOutcome::ProcessExit {
                        exit_code: Some(7),
                        success: false,
                    },
                    ..
                }
            )
        }));
    }

    #[tokio::test]
    async fn turn_loop_rejects_non_object_tool_input() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![tool_use("toolu_1", "working_note", json!("bad"))],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop(provider);

        let err = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap_err();

        assert!(err
            .to_string()
            .contains("tool_use input 必须是 JSON object"));
    }

    #[tokio::test]
    async fn turn_loop_rejects_duplicate_tool_use_ids() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![
                tool_use("toolu_1", "working_note", json!({"action": "list"})),
                tool_use("toolu_1", "working_note", json!({"action": "list"})),
            ],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop(provider);

        let error = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .expect_err("重复 tool_use id 必须拒绝");

        assert!(error.to_string().contains("重复 tool_use id"));
    }

    #[tokio::test]
    async fn turn_loop_rejects_tool_use_stop_without_tool_use_block() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("no tools")],
            ProviderStop::ToolUse,
        )]));
        let turn_loop = tool_loop(provider);

        let err = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap_err();

        assert!(err.to_string().contains("stop=ToolUse"));
    }

    #[tokio::test]
    async fn context_window_stop_compacts_then_commits_merged_anthropic_replay() {
        let provider = Arc::new(FakeProvider::new(vec![
            anthropic_response(
                vec![SessionTurnContentBlock::text("first ")],
                vec![
                    json!({"type":"thinking", "thinking":"private", "signature":"sig"}),
                    json!({"type":"text", "text":"first "}),
                ],
                ProviderStop::ContextWindowExceeded,
            ),
            anthropic_response(
                vec![SessionTurnContentBlock::text("second")],
                vec![json!({"type":"text", "text":"second"})],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut preflight = RecordingContextRecoveryPreflight::default();
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn_with_hooks(
                request(),
                &mut |event| events.push(event),
                None,
                None,
                Some(&mut preflight),
            )
            .await
            .unwrap();

        assert_eq!(non_context_messages(&turn).len(), 2);
        assert_eq!(
            assistant_message_text(non_context_messages(&turn)[1]),
            "first second"
        );
        let Some(ProviderReplayState::AnthropicMessages { messages, .. }) =
            non_context_messages(&turn)[1].provider_replay.as_ref()
        else {
            panic!("merged assistant must preserve Anthropic replay");
        };
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"][0]["text"], CONTINUATION_TRIGGER);
        assert_eq!(messages[2]["role"], "assistant");
        assert_eq!(preflight.applied, 1);
        let requests = provider.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].messages.last().unwrap().role, "user");
        assert!(assistant_message_text(requests[1].messages.last().unwrap())
            .contains(CONTINUATION_TRIGGER));
    }

    #[tokio::test]
    async fn context_continuation_completed_event_contains_full_visible_response() {
        let provider = Arc::new(ScriptedProvider::new(vec![
            ScriptedProviderAttempt {
                events: vec![
                    ProviderEvent::AssistantTextDelta {
                        text: "first ".into(),
                    },
                    ProviderEvent::AssistantMessageCompleted {
                        text: "first ".into(),
                    },
                ],
                result: Ok(anthropic_response(
                    vec![SessionTurnContentBlock::text("first ")],
                    vec![json!({"type":"text", "text":"first "})],
                    ProviderStop::ContextWindowExceeded,
                )),
            },
            ScriptedProviderAttempt {
                events: vec![
                    ProviderEvent::AssistantTextDelta {
                        text: "second".into(),
                    },
                    ProviderEvent::AssistantMessageCompleted {
                        text: "second".into(),
                    },
                ],
                result: Ok(anthropic_response(
                    vec![SessionTurnContentBlock::text("second")],
                    vec![json!({"type":"text", "text":"second"})],
                    ProviderStop::Done,
                )),
            },
        ]));
        let turn_loop = tool_loop(provider);
        let mut preflight = RecordingContextRecoveryPreflight::default();
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn_with_hooks(
                request(),
                &mut |event| events.push(event),
                None,
                None,
                Some(&mut preflight),
            )
            .await
            .unwrap();

        assert_eq!(
            assistant_message_text(non_context_messages(&turn)[1]),
            "first second"
        );
        assert!(events.iter().any(|event| matches!(
            event,
            SessionTurnEvent::AssistantMessageCompleted { text }
                if text == "first second"
        )));
    }

    #[tokio::test]
    async fn reasoning_only_context_partial_is_replayed_without_empty_success() {
        let provider = Arc::new(FakeProvider::new(vec![
            anthropic_response(
                Vec::new(),
                vec![json!({
                    "type":"thinking",
                    "thinking":"private",
                    "signature":"sig"
                })],
                ProviderStop::ContextWindowExceeded,
            ),
            anthropic_response(
                vec![SessionTurnContentBlock::text("visible")],
                vec![json!({"type":"text", "text":"visible"})],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider);
        let mut preflight = RecordingContextRecoveryPreflight::default();

        let turn = turn_loop
            .run_session_turn_with_hooks(request(), &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .unwrap();

        assert_eq!(
            assistant_message_text(non_context_messages(&turn)[1]),
            "visible"
        );
        let Some(ProviderReplayState::AnthropicMessages { messages, .. }) =
            non_context_messages(&turn)[1].provider_replay.as_ref()
        else {
            panic!("reasoning-only partial must survive in final replay");
        };
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0]["content"][0]["type"], "thinking");
        assert_eq!(preflight.applied, 1);
    }

    #[tokio::test]
    async fn context_window_recovery_has_independent_two_attempt_limit() {
        let context_response = || {
            anthropic_response(
                vec![SessionTurnContentBlock::text("partial")],
                vec![json!({"type":"text", "text":"partial"})],
                ProviderStop::ContextWindowExceeded,
            )
        };
        let provider = Arc::new(FakeProvider::new(vec![
            context_response(),
            context_response(),
            context_response(),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut preflight = RecordingContextRecoveryPreflight::default();

        let error = turn_loop
            .run_session_turn_with_hooks(request(), &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("自动压缩并续写 2 次后仍未完成"));
        assert_eq!(provider.requests.lock().await.len(), 3);
        assert_eq!(preflight.applied, 2);
    }

    #[tokio::test]
    async fn complete_tool_use_at_context_limit_executes_before_forced_compaction() {
        let provider = Arc::new(FakeProvider::new(vec![
            anthropic_response(
                vec![tool_use("toolu_context", "missing_tool", json!({}))],
                vec![json!({
                    "type":"tool_use",
                    "id":"toolu_context",
                    "name":"missing_tool",
                    "input":{}
                })],
                ProviderStop::ContextWindowExceeded,
            ),
            anthropic_response(
                vec![SessionTurnContentBlock::text("after tool")],
                vec![json!({"type":"text", "text":"after tool"})],
                ProviderStop::Done,
            ),
        ]));
        let turn_loop = tool_loop(provider.clone());
        let mut preflight = RecordingContextRecoveryPreflight::default();

        let turn = turn_loop
            .run_session_turn_with_hooks(request(), &mut |_| {}, None, None, Some(&mut preflight))
            .await
            .unwrap();

        assert_eq!(non_context_messages(&turn).len(), 4);
        assert!(matches!(
            non_context_messages(&turn)[1].content.as_slice(),
            [SessionTurnContentBlock::ToolUse { id, .. }] if id == "toolu_context"
        ));
        assert_eq!(non_context_messages(&turn)[2].role, "user");
        assert_eq!(
            tool_result_content(non_context_messages(&turn)[2], "toolu_context")["ok"],
            false
        );
        assert_eq!(
            assistant_message_text(non_context_messages(&turn)[3]),
            "after tool"
        );
        assert_eq!(provider.requests.lock().await.len(), 2);
        assert_eq!(preflight.applied, 1);
    }

    #[test]
    fn provider_response_suffix_strips_sent_internal_replay_for_all_protocols() {
        let cases = vec![
            (
                ProviderReplayState::OpenAiResponses {
                    model: Some("test-model".into()),
                    items: vec![json!({"id":"partial"}), json!({"id":"continue"})],
                },
                ProviderReplayState::OpenAiResponses {
                    model: Some("test-model".into()),
                    items: vec![
                        json!({"id":"partial"}),
                        json!({"id":"continue"}),
                        json!({"id":"final"}),
                    ],
                },
                ProviderReplayState::OpenAiResponses {
                    model: Some("test-model".into()),
                    items: vec![json!({"id":"final"})],
                },
            ),
            (
                ProviderReplayState::OpenAiChatCompletions {
                    model: "test-model".into(),
                    messages: vec![json!({"id":"partial"}), json!({"id":"continue"})],
                },
                ProviderReplayState::OpenAiChatCompletions {
                    model: "test-model".into(),
                    messages: vec![
                        json!({"id":"partial"}),
                        json!({"id":"continue"}),
                        json!({"id":"final"}),
                    ],
                },
                ProviderReplayState::OpenAiChatCompletions {
                    model: "test-model".into(),
                    messages: vec![json!({"id":"final"})],
                },
            ),
            (
                ProviderReplayState::AnthropicMessages {
                    model: "test-model".into(),
                    messages: vec![json!({"id":"partial"}), json!({"id":"continue"})],
                },
                ProviderReplayState::AnthropicMessages {
                    model: "test-model".into(),
                    messages: vec![
                        json!({"id":"partial"}),
                        json!({"id":"continue"}),
                        json!({"id":"final"}),
                    ],
                },
                ProviderReplayState::AnthropicMessages {
                    model: "test-model".into(),
                    messages: vec![json!({"id":"final"})],
                },
            ),
        ];

        for (request_replay, response_replay, expected_replay) in cases {
            let base = vec![SessionTurnMessage::user_text("task")];
            let mut latest = base.clone();
            latest.push(SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::text("partial")],
                provider_replay: Some(request_replay),
            });
            let response = SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::text("partial final")],
                provider_replay: Some(response_replay),
            };

            let suffix =
                provider_assistant_suffix_for_latest_request(&response, &base, &latest).unwrap();

            assert_eq!(suffix.provider_replay, Some(expected_replay));
            assert_eq!(assistant_message_text(&suffix), "partial final");
        }
    }

    #[tokio::test]
    async fn turn_loop_rejects_max_tokens_stop() {
        let provider = Arc::new(FakeProvider::new(vec![response(
            vec![SessionTurnContentBlock::text("partial")],
            ProviderStop::MaxTokens,
        )]));
        let turn_loop = tool_loop(provider);

        let err = turn_loop
            .run_session_turn(request(), &mut |_| {})
            .await
            .unwrap_err();

        assert!(err.to_string().contains("MaxTokens"));
    }
}
