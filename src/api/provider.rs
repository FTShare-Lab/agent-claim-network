//! provider-neutral LLM 协议接口。
//!
//! 本模块定义 `AgentTurnLoop` 与具体模型后端之间的最小协议边界：
//! 上层传入 canonical session message 和工具 schema，provider adapter 只负责
//! HTTP/streaming 与协议形状转换，不执行工具、不解释业务 JSON。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::api::{SessionTurnContentBlock, SessionTurnMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHistoryMediaPolicy {
    Placeholder,
    Preserve,
}

/// 当前 adapter 可原样回放的 provider 私有历史协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderReplayProtocol {
    OpenAiResponses,
    OpenAiChatCompletions,
    AnthropicMessages,
}

/// provider 私有 replay 的绑定身份。
///
/// 原样 replay 只允许回到相同 wire protocol 与精确配置 model；切换任一项都从
/// canonical history 开始新的 replay 代际，避免跨模型误传私有状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderReplayIdentity {
    pub protocol: ProviderReplayProtocol,
    pub model: String,
}

#[async_trait]
pub trait ProviderAdapter: Send + Sync {
    /// 已落盘、尚未 compact 的历史附件如何进入主 session provider context。
    fn history_media_policy(&self) -> ProviderHistoryMediaPolicy {
        ProviderHistoryMediaPolicy::Placeholder
    }

    /// 只允许相同协议与精确 model 的 replay 进入请求及 token/compaction 预算。
    fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        None
    }

    /// 是否在 provider 调用前先发本地粗估 ctx。
    fn emit_preflight_context_estimate(&self) -> bool {
        true
    }

    /// 单次逻辑 provider call 的总 deadline；覆盖内部 max_tokens continuation。
    fn request_timeout(&self) -> Option<Duration> {
        None
    }

    async fn send(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse>;

    /// 在 adapter 每一个真实逻辑请求发送前上报其精确
    /// provider-neutral history。默认 adapter 只有一次请求；内部实现
    /// max-token continuation 的 adapter 必须覆盖此方法并逐次上报。
    async fn send_with_request_observer(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        observer
            .before_provider_request(&request.messages)
            .await
            .map_err(ProviderRequestPreparationFailure::from_error)?;
        observer
            .provider_request_started(&request.messages)
            .map_err(ProviderRequestPreparationFailure::from_error)?;
        self.send(request, emit).await
    }

    /// 丢弃未提交 logical turn 对应的 transport 私有状态；HTTP adapter 默认为空操作。
    async fn discard_runtime_chain(&self, _chain_id: ProviderRuntimeChainId) {}
}

/// adapter 内部 continuation 与上层 WAL 之间的最小边界。
///
/// 上报的 message vector 必须是实际将转成 wire input/messages 的同一份
/// 规范化历史，并且只能在上一次之后追加 continuation replay suffix。
#[async_trait]
pub trait ProviderRequestObserver: Send {
    async fn before_provider_request(
        &mut self,
        messages: &[SessionTurnMessage],
    ) -> anyhow::Result<()>;

    /// adapter 已完成本地恢复检查，即将第一次发起该逻辑请求的网络 I/O。
    /// 该边界用于区分“WAL 已准备但请求确定未发送”和“发送结果存在歧义”。
    fn provider_request_started(&mut self, messages: &[SessionTurnMessage]) -> anyhow::Result<()> {
        let _ = messages;
        Ok(())
    }

    /// adapter 已准备内部 continuation，但在任何 transport send-started 之前决定
    /// 放弃；owner 必须回滚该 continuation 的 request WAL。
    async fn provider_request_abandoned_before_send(
        &mut self,
        _messages: &[SessionTurnMessage],
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

pub(crate) struct NoopProviderRequestObserver;

#[async_trait]
impl ProviderRequestObserver for NoopProviderRequestObserver {
    async fn before_provider_request(
        &mut self,
        _messages: &[SessionTurnMessage],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn provider_request_started(&mut self, _messages: &[SessionTurnMessage]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn provider_request_abandoned_before_send(
        &mut self,
        _messages: &[SessionTurnMessage],
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// 仅用于当前进程内隔离 WebSocket continuation 的调用链身份。
///
/// 它不发送给上游，也不写入 session；fresh session、resume 与每个 delegation
/// 都会得到新的值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProviderRuntimeChainId(u64);

impl ProviderRuntimeChainId {
    pub fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for ProviderRuntimeChainId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct ProviderFallbackState {
    id: u64,
    websocket_sticky: AtomicBool,
}

impl ProviderFallbackState {
    fn new() -> Arc<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Arc::new(Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            websocket_sticky: AtomicBool::new(false),
        })
    }
}

/// 当前进程内的 WebSocket 降级作用域。
///
/// session root 只由 Inbox 使用；主 Agent 与各 Subagent 持有独立 local state，
/// 同时动态观察同一个 root。该状态不持久化，resume 会重新创建。
#[derive(Debug, Clone)]
pub struct ProviderRuntimeFallbackScope {
    local: Arc<ProviderFallbackState>,
    inherited_root: Option<Arc<ProviderFallbackState>>,
}

impl ProviderRuntimeFallbackScope {
    pub fn new_root() -> Self {
        Self {
            local: ProviderFallbackState::new(),
            inherited_root: None,
        }
    }

    /// 创建只继承 session root、但不继承当前 local sticky 的 actor scope。
    pub fn new_child(&self) -> Self {
        let root = self
            .inherited_root
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.local));
        Self {
            local: ProviderFallbackState::new(),
            inherited_root: Some(root),
        }
    }

    pub(crate) fn websocket_sticky(&self) -> bool {
        self.local.websocket_sticky.load(Ordering::Acquire)
            || self
                .inherited_root
                .as_ref()
                .is_some_and(|root| root.websocket_sticky.load(Ordering::Acquire))
    }

    pub(crate) fn mark_websocket_sticky(&self) {
        self.local.websocket_sticky.store(true, Ordering::Release);
    }
}

impl Default for ProviderRuntimeFallbackScope {
    fn default() -> Self {
        Self::new_root()
    }
}

impl PartialEq for ProviderRuntimeFallbackScope {
    fn eq(&self, other: &Self) -> bool {
        self.local.id == other.local.id
            && self.inherited_root.as_ref().map(|state| state.id)
                == other.inherited_root.as_ref().map(|state| state.id)
    }
}

impl Eq for ProviderRuntimeFallbackScope {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProviderStreamOutputMode {
    /// 增量事件已经对前台可见，收到 partial 后不能从头重放同一 transport。
    #[default]
    Live,
    /// 调用方只在完整终态后消费结果，partial 可以丢弃并安全重放。
    Buffered,
}

/// steer/cancel 之后阻止尚未开始的 provider 恢复动作，但不打断当前 request。
///
/// 独立 ID 仅用于保持 `ProviderRequest` 的可比较性；实际中断与成功 partial
/// 收束状态由 clone 共享。
#[derive(Debug, Clone)]
pub struct ProviderRecoveryInterrupt {
    id: u64,
    token: CancellationToken,
    preserve_successful_response: Arc<AtomicBool>,
}

impl ProviderRecoveryInterrupt {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            token: CancellationToken::new(),
            preserve_successful_response: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn cancel(&self) {
        self.token.cancel();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    /// 当前请求已经成功返回可提交的 max-token partial；safe steer 只阻止尚未发送的
    /// continuation，不能让公共 turn 边界再把这份成功响应整体丢弃。
    pub(crate) fn preserve_successful_response(&self) {
        self.preserve_successful_response
            .store(true, Ordering::Release);
    }

    pub(crate) fn should_preserve_successful_response(&self) -> bool {
        self.preserve_successful_response.load(Ordering::Acquire)
    }
}

impl PartialEq for ProviderRecoveryInterrupt {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ProviderRecoveryInterrupt {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRequest {
    pub system_prompt: String,
    pub messages: Vec<SessionTurnMessage>,
    pub tools: Vec<ToolSpec>,
    pub max_tokens: u32,
    pub stream: bool,
    /// streaming delta 是直接可见还是只在内部缓冲。
    pub stream_output_mode: ProviderStreamOutputMode,
    /// streaming transport 的进程内 chain 身份；非流式内部调用保持 `None`。
    pub runtime_chain_id: Option<ProviderRuntimeChainId>,
    /// WebSocket sticky 的运行期作用域，与 continuation chain 相互独立。
    pub runtime_fallback_scope: Option<ProviderRuntimeFallbackScope>,
    /// 只阻止尚未开始的 retry、continuation 与 fallback；不能取消当前正常 request。
    pub recovery_interrupt: Option<ProviderRecoveryInterrupt>,
    /// 覆盖 adapter 内部的额外 HTTP retry 次数；`None` 使用 provider 配置。
    pub retry_count_override: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponse {
    pub assistant_message: SessionTurnMessage,
    pub stop: ProviderStop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextUsageSnapshot {
    pub used_tokens: usize,
    pub source: ContextUsageSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextUsageSource {
    Provider,
    Estimate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderStop {
    Done,
    ToolUse,
    MaxTokens,
    /// Provider 返回了完整、有效但因模型上下文窗口耗尽而截断的响应。
    /// 上层必须先压缩上下文再续写，不能按 transport 故障 fallback。
    ContextWindowExceeded,
}

/// Provider 已返回明确的非成功终态；重放同一请求不会变成完整响应。
///
/// Adapter 用该类型阻止 turn loop 把拒绝、暂停或上下文截断误当作
/// transport streaming 故障并自动切到 non-streaming。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ProviderTerminalFailure {
    message: String,
}

/// Provider request 尚未发送时，其 write-ahead 准备已失败。
///
/// 该错误不能进入 streaming fallback，否则会绕过同一条 WAL 不变量。
#[derive(Debug, thiserror::Error)]
#[error("准备 Provider request 失败: {message}")]
pub(crate) struct ProviderRequestPreparationFailure {
    message: String,
}

impl ProviderRequestPreparationFailure {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub(crate) fn from_error(error: anyhow::Error) -> Self {
        Self::new(format!("{error:#}"))
    }
}

/// Streaming response 已损坏或未完整结束，可以安全放弃本次 attempt 并换路径重放。
///
/// 该标记只能由 streaming client 在完成边界产生；普通 non-streaming schema 错误
/// 不能借此进入 provider-neutral fallback。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ProviderStreamFailure {
    message: String,
}

impl ProviderStreamFailure {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Provider 实际完成一次请求时使用的 transport。
///
/// 该信息只用于内部重试选择和日志诊断，不进入用户可见错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProviderTransport {
    ResponsesWebSocket,
    ResponsesSse,
    ResponsesNonStreaming,
    ChatSse,
    ChatNonStreaming,
    AnthropicSse,
    AnthropicNonStreaming,
}

impl ProviderTransport {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ResponsesWebSocket => "responses_websocket",
            Self::ResponsesSse => "responses_sse",
            Self::ResponsesNonStreaming => "responses_non_streaming",
            Self::ChatSse => "chat_sse",
            Self::ChatNonStreaming => "chat_non_streaming",
            Self::AnthropicSse => "anthropic_sse",
            Self::AnthropicNonStreaming => "anthropic_non_streaming",
        }
    }

    pub(crate) const fn is_streaming(self) -> bool {
        !matches!(
            self,
            Self::ResponsesNonStreaming | Self::ChatNonStreaming | Self::AnthropicNonStreaming
        )
    }

    pub(crate) fn retry_fallback_scope(
        self,
        base: &ProviderRuntimeFallbackScope,
    ) -> ProviderRuntimeFallbackScope {
        if self == Self::ResponsesSse {
            let scope = base.new_child();
            scope.mark_websocket_sticky();
            scope
        } else {
            base.clone()
        }
    }
}

impl std::fmt::Display for ProviderTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Provider 正常结束，但没有 ACN 可以提交的非空文本或完整工具调用。
///
/// 该结果没有产生可提交的 provider replay 或工具副作用。内部任务会清除本次
/// continuation，并使用独立业务预算在相同实际 transport 上原样重试；显式拒绝、
/// token limit 和上下文窗口恢复不能映射为此类型。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ProviderNoConsumableOutput {
    transport: ProviderTransport,
    message: String,
}

impl ProviderNoConsumableOutput {
    pub(crate) fn new(transport: ProviderTransport, message: impl Into<String>) -> Self {
        Self {
            transport,
            message: message.into(),
        }
    }

    pub(crate) const fn transport(&self) -> ProviderTransport {
        self.transport
    }
}

impl ProviderTerminalFailure {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderEvent {
    ContextUsageUpdated { usage: ContextUsageSnapshot },
    AssistantTextDelta { text: String },
    AssistantMessageCompleted { text: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

pub fn context_usage_from_openai_usage(usage: &Value) -> Option<ContextUsageSnapshot> {
    let total = usage.get("total_tokens")?.as_u64()?;
    Some(ContextUsageSnapshot {
        used_tokens: usize::try_from(total).ok()?,
        source: ContextUsageSource::Provider,
    })
}

pub fn context_usage_from_anthropic_input_usage(usage: &Value) -> Option<ContextUsageSnapshot> {
    Some(ContextUsageSnapshot {
        used_tokens: anthropic_input_tokens(usage)?,
        source: ContextUsageSource::Provider,
    })
}

pub fn context_usage_from_anthropic_committed_usage(usage: &Value) -> Option<ContextUsageSnapshot> {
    let input_tokens = anthropic_input_tokens(usage)?;
    let output_tokens = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    Some(ContextUsageSnapshot {
        used_tokens: input_tokens.saturating_add(output_tokens),
        source: ContextUsageSource::Provider,
    })
}

fn anthropic_input_tokens(usage: &Value) -> Option<usize> {
    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    let cache_creation = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    let cache_read = usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    Some(
        input_tokens
            .saturating_add(cache_creation)
            .saturating_add(cache_read),
    )
}

/// 从 assistant message 提取结构化 JSON 场景需要的纯文本。
pub fn assistant_text_from_message(message: &SessionTurnMessage) -> anyhow::Result<String> {
    if message.role != "assistant" {
        anyhow::bail!("provider response role 必须是 assistant: {}", message.role);
    }

    let mut text = String::new();
    for block in &message.content {
        match block {
            SessionTurnContentBlock::Text { text: part } => text.push_str(part),
            SessionTurnContentBlock::ModelContext { .. } => {
                anyhow::bail!("结构化文本响应不能包含 ModelContext block");
            }
            SessionTurnContentBlock::SkillInstructions { .. } => {
                anyhow::bail!("结构化文本响应不能包含 SkillInstructions block");
            }
            SessionTurnContentBlock::Image { .. } | SessionTurnContentBlock::Document { .. } => {
                anyhow::bail!("结构化文本响应不能包含附件 block");
            }
            SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => {
                anyhow::bail!("结构化文本响应只能包含 Text block");
            }
        }
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn openai_usage_prefers_total_tokens_for_committed_context() {
        let usage = json!({
            "prompt_tokens": 100,
            "completion_tokens": 25,
            "total_tokens": 125
        });

        assert_eq!(
            context_usage_from_openai_usage(&usage),
            Some(ContextUsageSnapshot {
                used_tokens: 125,
                source: ContextUsageSource::Provider
            })
        );
    }

    #[test]
    fn anthropic_input_usage_includes_cache_buckets() {
        let usage = json!({
            "input_tokens": 100,
            "cache_creation_input_tokens": 20,
            "cache_read_input_tokens": 30,
            "output_tokens": 9
        });

        assert_eq!(
            context_usage_from_anthropic_input_usage(&usage),
            Some(ContextUsageSnapshot {
                used_tokens: 150,
                source: ContextUsageSource::Provider
            })
        );
        assert_eq!(
            context_usage_from_anthropic_committed_usage(&usage),
            Some(ContextUsageSnapshot {
                used_tokens: 159,
                source: ContextUsageSource::Provider
            })
        );
    }

    #[test]
    fn assistant_text_rejects_internal_model_context_blocks() {
        let message = SessionTurnMessage {
            role: "assistant".into(),
            content: vec![SessionTurnContentBlock::ModelContext {
                source: crate::api::ModelContextSource::Runtime,
                fingerprint: "sha256-v1:invalid-provider-output".into(),
                text: "<runtime_context>must not be provider output</runtime_context>".into(),
            }],
            provider_replay: None,
        };

        let error = assistant_text_from_message(&message).unwrap_err();
        assert!(error.to_string().contains("ModelContext"));
    }

    #[test]
    fn fallback_scope_inherits_only_session_root() {
        let root = ProviderRuntimeFallbackScope::new_root();
        let main = root.new_child();
        let subagent = main.new_child();

        main.mark_websocket_sticky();
        assert!(main.websocket_sticky());
        assert!(!root.websocket_sticky());
        assert!(!subagent.websocket_sticky());

        root.mark_websocket_sticky();
        assert!(main.websocket_sticky());
        assert!(subagent.websocket_sticky());
        assert!(root.new_child().websocket_sticky());
    }
}
