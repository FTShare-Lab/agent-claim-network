//! Anthropic Messages API 低层 client 与 provider adapter。
//!
//! ## 分层
//! - `AnthropicMessagesClient`：只负责 HTTP/SSE、retry、continuation 与 Messages API
//!   协议收发。
//! - `AnthropicProviderAdapter`：实现 provider-neutral `ProviderAdapter`，供
//!   `AgentTurnLoop`、`StructuredJsonCaller`、`MemoryReviewLoop` 复用。
//!
//! ## 重试 / JSON 输出
//! - HTTP 层重试封装在 `send_with_retry`：5xx / 429 / 网络错误退避后重试
//! - JSON 出参解析失败也算可重试（模型偶尔吐脏字符），见 `is_retryable`
//! - 部分模型即便要求"只 JSON"也会包 ```json fence；本地一次宽容剥壳
//! - JSON 阶段也支持 `max_tokens` continuation：先收齐完整 assistant 文本再解析

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use serde_json::{json, Value};

use super::context_usage_from_anthropic_committed_usage;
use super::continuation::{
    append_with_overlap_dedupe, CONTINUATION_TRIGGER, MAX_CONTINUATION_TURNS,
};
use super::endpoint::{resolve_llm_endpoint, LlmEndpointKind};
use super::llm_http::{read_llm_error_body, LlmHttpError, LlmHttpPhase};
use super::provider::{
    NoopProviderRequestObserver, ProviderAdapter, ProviderEvent, ProviderHistoryMediaPolicy,
    ProviderNoConsumableOutput, ProviderReplayIdentity, ProviderReplayProtocol, ProviderRequest,
    ProviderRequestObserver, ProviderRequestPreparationFailure, ProviderResponse, ProviderStop,
    ProviderStreamFailure, ProviderTerminalFailure, ProviderTransport, ToolSpec,
};
use super::redact_media_error_body;
use super::types::{SessionTurnContentBlock, SessionTurnEvent, SessionTurnMessage};
use crate::config::{AnthropicThinking, ReasoningEffort};
use crate::prompt::PromptError;

mod protocol;
mod streaming;

use protocol::*;

#[derive(thiserror::Error)]
pub enum AnthropicError {
    #[error("{0}")]
    Http(#[from] LlmHttpError),
    #[error("LLM provider authentication failed (401): {0}")]
    Auth(String),
    #[error("LLM provider returned HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("LLM response JSON parse failed: {0}")]
    ResponseJson(#[from] serde_json::Error),
    #[error("Anthropic Messages endpoint 配置无效: {0}")]
    InvalidEndpoint(String),
    #[error("LLM 输出不符合预期 schema: {reason}")]
    OutputShape { reason: String, raw: String },
    #[error("Anthropic streaming 响应损坏或未完整结束: {reason}")]
    StreamFailure { reason: String, raw: String },
    #[error("Anthropic 没有可消费输出: {reason}")]
    NoConsumableOutput { reason: String },
    #[error("Anthropic streaming 返回确定性错误: {reason}")]
    TerminalFailure { reason: String },
    #[error("准备 Anthropic continuation request 失败: {reason}")]
    RequestPreparation { reason: String },
    #[error("prompt 渲染失败: {0}")]
    Prompt(#[from] PromptError),
    #[error(
        "当前模型可能不支持图片 / PDF 附件输入，请确认模型多模态能力或移除附件后重试。上游原始错误: {source}"
    )]
    MediaRejected {
        #[source]
        source: Box<AnthropicError>,
    },
}

impl std::fmt::Debug for AnthropicError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // anyhow / test debug 链也只能看到已脱敏的 Display，不能展开 raw replay。
        std::fmt::Display::fmt(self, formatter)
    }
}

pub struct AnthropicMessagesClient {
    http: reqwest::Client,
    api_key: Arc<String>,
    endpoint: Arc<String>,
    model: Arc<String>,
    retry_count: u32,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
    timeout: Duration,
    reasoning_effort: ReasoningEffort,
    thinking: AnthropicThinking,
    thinking_budget_tokens: Option<u32>,
}

pub struct AnthropicProviderAdapter {
    client: AnthropicMessagesClient,
}

/// 把 Anthropic adapter 内部的每次 max-token continuation 投影为
/// provider-neutral 只追加 suffix，供 turn loop 在真实请求前写 WAL。
struct AnthropicContinuationRequestObserver<'a> {
    messages: Vec<SessionTurnMessage>,
    model: String,
    observer: &'a mut (dyn ProviderRequestObserver + Send),
}

impl AnthropicContinuationRequestObserver<'_> {
    async fn before_request(&mut self) -> Result<(), AnthropicError> {
        self.observer
            .before_provider_request(&self.messages)
            .await
            .map_err(|error| AnthropicError::RequestPreparation {
                reason: format!("{error:#}"),
            })
    }

    fn push_round(&mut self, replay_messages: Vec<Value>, text: String) {
        let content = if text.trim().is_empty() {
            Vec::new()
        } else {
            vec![SessionTurnContentBlock::text(text)]
        };
        self.messages.push(SessionTurnMessage {
            role: "assistant".into(),
            content,
            provider_replay: Some(crate::api::ProviderReplayState::AnthropicMessages {
                model: self.model.clone(),
                messages: replay_messages,
            }),
        });
    }
}

impl AnthropicMessagesClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_key: String,
        endpoint: String,
        model: String,
        _max_tokens: u32,
        timeout: Duration,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Result<Self, AnthropicError> {
        let endpoint = resolve_llm_endpoint(&endpoint, LlmEndpointKind::AnthropicMessages)
            .map_err(|error| AnthropicError::InvalidEndpoint(error.to_string()))?;
        let http = crate::http_client_builder_for_endpoint(&endpoint)
            .timeout(timeout)
            .build()
            .map_err(|error| {
                AnthropicError::Http(LlmHttpError::new(
                    error,
                    LlmHttpPhase::BuildClient,
                    Some(timeout),
                ))
            })?;
        Ok(Self {
            http,
            api_key: Arc::new(api_key),
            endpoint: Arc::new(endpoint),
            model: Arc::new(model),
            retry_count,
            retry_base_delay,
            retry_max_delay,
            timeout,
            reasoning_effort: ReasoningEffort::None,
            thinking: AnthropicThinking::Auto,
            thinking_budget_tokens: None,
        })
    }

    fn request_for(
        &self,
        system: &str,
        messages: Vec<ApiMessage>,
        tools: Option<Vec<ApiToolDefinition>>,
        max_tokens: u32,
        stream: Option<bool>,
    ) -> CreateMessageRequest {
        CreateMessageRequest {
            model: self.model.as_str().to_owned(),
            max_tokens,
            messages,
            system: system.to_owned(),
            output_config: (self.reasoning_effort != ReasoningEffort::None).then_some(
                ApiOutputConfig {
                    effort: self.reasoning_effort,
                },
            ),
            thinking: match self.thinking {
                AnthropicThinking::Auto => None,
                AnthropicThinking::Enabled => Some(ApiThinkingConfig {
                    kind: "enabled".into(),
                    budget_tokens: self.thinking_budget_tokens,
                }),
                AnthropicThinking::Adaptive => Some(ApiThinkingConfig {
                    kind: "adaptive".into(),
                    budget_tokens: None,
                }),
                AnthropicThinking::Disabled => Some(ApiThinkingConfig {
                    kind: "disabled".into(),
                    budget_tokens: None,
                }),
            },
            tools,
            stream,
        }
    }

    fn http_error(&self, error: reqwest::Error, phase: LlmHttpPhase) -> AnthropicError {
        AnthropicError::Http(LlmHttpError::new(error, phase, Some(self.timeout)))
    }

    async fn send_with_retry_count(
        &self,
        body: &CreateMessageRequest,
        retry_count: u32,
    ) -> Result<Value, AnthropicError> {
        let mut last_retryable: Option<AnthropicError> = None;
        for attempt in 0..=retry_count {
            match self.send_once(body).await {
                Ok(v) => return Ok(v),
                Err(e) if is_retryable(&e) && attempt < retry_count => {
                    let backoff =
                        compute_backoff(attempt, self.retry_base_delay, self.retry_max_delay);
                    log::warn!(
                        target: "api",
                        "Anthropic 调用失败，{}ms 后重试 ({}/{})：{}",
                        backoff.as_millis(),
                        attempt + 1,
                        retry_count,
                        e
                    );
                    last_retryable = Some(e);
                    tokio::time::sleep(backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(
            last_retryable.unwrap_or_else(|| AnthropicError::OutputShape {
                reason: "retry loop 未返回结果".into(),
                raw: String::new(),
            }),
        )
    }

    async fn send_once(&self, body: &CreateMessageRequest) -> Result<Value, AnthropicError> {
        let resp = self
            .http
            .post(self.endpoint.as_str())
            .header("x-api-key", self.api_key.as_str())
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::SendRequest))?;

        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let body = read_llm_error_body(resp, self.timeout).await;
            return Err(AnthropicError::Auth(redact_anthropic_error_body(&body)));
        }
        if !status.is_success() {
            let body = read_llm_error_body(resp, self.timeout).await;
            return Err(AnthropicError::Status {
                status: status.as_u16(),
                body: redact_anthropic_error_body(&body),
            });
        }

        resp.json()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::DecodeJsonBody))
    }

    #[cfg(test)]
    async fn send_text_with_continuation_for_provider_with_retry_count(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        self.send_text_with_continuation_with_policy(
            system,
            messages,
            tools,
            max_tokens,
            retry_count,
            false,
            None,
        )
        .await
    }

    async fn send_text_with_continuation_for_provider_with_retry_count_observed(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        observer: &mut AnthropicContinuationRequestObserver<'_>,
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        self.send_text_with_continuation_with_policy(
            system,
            messages,
            tools,
            max_tokens,
            retry_count,
            false,
            Some(observer),
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "continuation 协议、retry 与 request observer 需显式穿过同一边界"
    )]
    async fn send_text_with_continuation_with_policy(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        error_on_unresolved_max_tokens: bool,
        mut request_observer: Option<&mut AnthropicContinuationRequestObserver<'_>>,
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let mut merged_text = String::new();
        let mut last_response: Option<Value> = None;
        let mut last_blocks = Vec::new();
        let mut last_stop_reason = String::from("end_turn");
        let mut replay_messages = Vec::new();

        for round in 0..=MAX_CONTINUATION_TURNS {
            if let Some(observer) = request_observer.as_deref_mut() {
                observer.before_request().await?;
            }
            let body = self.request_for(system, messages.clone(), tools.clone(), max_tokens, None);
            let response = self.send_with_retry_count(&body, retry_count).await?;
            let assistant_blocks =
                content_blocks(&response).ok_or_else(|| AnthropicError::OutputShape {
                    reason: "缺少 content blocks".into(),
                    raw: response.to_string(),
                })?;
            let stop_reason = required_anthropic_stop_reason(&response)?;
            let assistant_replay = json!({
                "role": "assistant",
                "content": assistant_blocks.clone(),
            });
            messages.push(ApiMessage::raw(assistant_replay.clone()));
            replay_messages.push(assistant_replay.clone());
            let round_text = extract_text_blocks(&response).unwrap_or_default();
            append_with_overlap_dedupe(&mut merged_text, &round_text);
            last_stop_reason = stop_reason.clone();
            last_blocks = assistant_blocks;
            last_response = Some(response);

            if stop_reason != "max_tokens" {
                break;
            }
            if has_tool_use_block(&last_blocks) {
                // Anthropic 协议要求 tool_use 后必须立刻跟 tool_result，不能先插 user continuation。
                // 遇到 "max_tokens + tool_use" 时提前返回给 provider turn loop，由外层先执行工具回环。
                break;
            }
            if round == MAX_CONTINUATION_TURNS && error_on_unresolved_max_tokens {
                return Err(AnthropicError::OutputShape {
                    reason: format!(
                        "assistant max_tokens continuation 超过上限: {}",
                        MAX_CONTINUATION_TURNS + 1
                    ),
                    raw: merged_text,
                });
            }
            if round == MAX_CONTINUATION_TURNS {
                break;
            }
            let continuation = json!({"role": "user", "content": CONTINUATION_TRIGGER});
            messages.push(ApiMessage::raw(continuation.clone()));
            replay_messages.push(continuation.clone());
            if let Some(observer) = request_observer.as_deref_mut() {
                observer.push_round(vec![assistant_replay, continuation], round_text);
            }
        }

        let final_response = last_response.ok_or_else(|| AnthropicError::OutputShape {
            reason: "空响应：未获得 assistant 回合".into(),
            raw: String::new(),
        })?;
        Ok(ContinuedAssistantTurn {
            final_response,
            final_blocks: last_blocks,
            final_stop_reason: last_stop_reason,
            merged_text,
            replay_messages,
        })
    }
}

#[async_trait]
impl ProviderAdapter for AnthropicProviderAdapter {
    fn history_media_policy(&self) -> ProviderHistoryMediaPolicy {
        ProviderHistoryMediaPolicy::Preserve
    }

    fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        Some(ProviderReplayIdentity {
            protocol: ProviderReplayProtocol::AnthropicMessages,
            model: self.client.model.as_str().to_owned(),
        })
    }

    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    fn request_timeout(&self) -> Option<Duration> {
        Some(self.client.timeout)
    }

    async fn send(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        let mut observer = NoopProviderRequestObserver;
        self.send_observed(request, emit, &mut observer).await
    }

    async fn send_with_request_observer(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        self.send_observed(request, emit, observer).await
    }
}

impl AnthropicProviderAdapter {
    async fn send_observed(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        let transport = if request.stream {
            ProviderTransport::AnthropicSse
        } else {
            ProviderTransport::AnthropicNonStreaming
        };
        let retry_count = request
            .retry_count_override
            .unwrap_or(self.client.retry_count);
        let retry_after_partial =
            request.stream_output_mode == crate::api::ProviderStreamOutputMode::Buffered;
        let base_messages = request.messages;
        let request_has_media = base_messages.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(
                    block,
                    SessionTurnContentBlock::Image { .. }
                        | SessionTurnContentBlock::Document { .. }
                )
            })
        });
        let mut api_messages =
            session_turn_messages_to_api(base_messages.clone(), self.client.model.as_str());
        let api_tools = tool_specs_to_api(request.tools);
        let mut request_observer = AnthropicContinuationRequestObserver {
            messages: base_messages,
            model: self.client.model.as_str().to_owned(),
            observer,
        };

        let turn_result = if request.stream {
            let mut provider_emit = |event| match event {
                SessionTurnEvent::ContextUsageUpdated { usage } => {
                    emit(ProviderEvent::ContextUsageUpdated { usage });
                }
                SessionTurnEvent::AssistantTextDelta { text } => {
                    emit(ProviderEvent::AssistantTextDelta { text });
                }
                SessionTurnEvent::AssistantMessageCompleted { .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                | SessionTurnEvent::NonStreamingFallbackAttemptFailed { .. }
                | SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
                | SessionTurnEvent::Warning { .. }
                | SessionTurnEvent::CompactionStarted { .. }
                | SessionTurnEvent::CompactionCompleted { .. }
                | SessionTurnEvent::CompactionSkipped { .. }
                | SessionTurnEvent::CompactionFailed { .. }
                | SessionTurnEvent::ToolCallStarted { .. }
                | SessionTurnEvent::ToolCallSkipped { .. }
                | SessionTurnEvent::ToolCallProgress { .. }
                | SessionTurnEvent::ToolCallCompleted { .. }
                | SessionTurnEvent::ToolCallInterrupted { .. } => {}
            };
            self.client
                .send_text_with_continuation_streaming_for_provider_with_retry_count_observed(
                    &request.system_prompt,
                    &mut api_messages,
                    api_tools,
                    request.max_tokens,
                    retry_count,
                    retry_after_partial,
                    &mut provider_emit,
                    &mut request_observer,
                )
                .await
        } else {
            self.client
                .send_text_with_continuation_for_provider_with_retry_count_observed(
                    &request.system_prompt,
                    &mut api_messages,
                    api_tools,
                    request.max_tokens,
                    retry_count,
                    &mut request_observer,
                )
                .await
        };
        let turn = match turn_result {
            Ok(turn) => turn,
            Err(AnthropicError::RequestPreparation { reason }) => {
                return Err(ProviderRequestPreparationFailure::new(reason).into());
            }
            Err(error) => {
                if request.stream && anthropic_adapter_stream_failure(&error) {
                    return Err(ProviderStreamFailure::new(error.to_string()).into());
                }
                if let AnthropicError::TerminalFailure { reason } = &error {
                    return Err(ProviderTerminalFailure::new(reason.clone()).into());
                }
                return Err(wrap_media_rejection(error, request_has_media).into());
            }
        };
        if !request.stream {
            if let Some(usage) = turn
                .final_response
                .get("usage")
                .and_then(context_usage_from_anthropic_committed_usage)
            {
                emit(ProviderEvent::ContextUsageUpdated { usage });
            }
        }

        let stop = provider_stop_from_turn(&turn)?;
        if !turn.merged_text.trim().is_empty() {
            emit(ProviderEvent::AssistantMessageCompleted {
                text: turn.merged_text.clone(),
            });
        }

        let assistant_message = match assistant_turn_message(&turn, self.client.model.as_str()) {
            Ok(message) => message,
            Err(AnthropicError::NoConsumableOutput { reason }) => {
                return Err(ProviderNoConsumableOutput::new(transport, reason).into());
            }
            Err(error) => return Err(error.into()),
        };
        Ok(ProviderResponse {
            stop,
            assistant_message,
        })
    }
}

fn anthropic_adapter_stream_failure(error: &AnthropicError) -> bool {
    is_stream_retryable(error)
}

impl AnthropicProviderAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_key: String,
        endpoint: String,
        model: String,
        max_tokens: u32,
        timeout: Duration,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Result<Self, AnthropicError> {
        Ok(Self {
            client: AnthropicMessagesClient::new(
                api_key,
                endpoint,
                model,
                max_tokens,
                timeout,
                retry_count,
                retry_base_delay,
                retry_max_delay,
            )?,
        })
    }

    /// 设置 Messages 请求的推理强度；`none` 会在序列化时省略 `output_config`。
    pub fn with_reasoning_effort(mut self, reasoning_effort: ReasoningEffort) -> Self {
        self.client.reasoning_effort = reasoning_effort;
        self
    }

    /// 设置 Anthropic Messages 的显式 thinking 请求配置。
    pub fn with_thinking(
        mut self,
        thinking: AnthropicThinking,
        budget_tokens: Option<u32>,
    ) -> Self {
        self.client.thinking = thinking;
        self.client.thinking_budget_tokens = budget_tokens;
        self
    }
}

fn session_turn_messages_to_api(messages: Vec<SessionTurnMessage>, model: &str) -> Vec<ApiMessage> {
    let mut out = Vec::new();
    for message in messages {
        if let Some(crate::api::ProviderReplayState::AnthropicMessages {
            model: replay_model,
            messages,
        }) = message.provider_replay
        {
            if replay_model == model {
                out.extend(messages.into_iter().map(ApiMessage::raw));
                continue;
            }
        }
        let content = message
            .content
            .into_iter()
            .filter_map(session_turn_block_to_api)
            .collect::<Vec<_>>();
        if content.is_empty() {
            continue;
        }
        // Messages API 会在服务端合并连续的同角色 turn。本地保持一条 neutral
        // message 对应一条 wire message，避免跨已冻结请求边界改写缓存前缀。
        out.push(ApiMessage::structured(message.role, content));
    }
    out
}

fn session_turn_block_to_api(block: SessionTurnContentBlock) -> Option<Value> {
    match block {
        SessionTurnContentBlock::Text { text }
        | SessionTurnContentBlock::ModelContext { text, .. } => {
            Some(json!({"type": "text", "text": text}))
        }
        SessionTurnContentBlock::SkillInstructions { instruction } => Some(json!({
            "type": "text",
            "text": crate::skill::render_skill_instructions(&instruction),
        })),
        SessionTurnContentBlock::Image { media_type, data } => Some(json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": data
            }
        })),
        // PRD 拍板：Anthropic document 块只带 base64 source，filename 仅服务 OpenAI file part。
        SessionTurnContentBlock::Document {
            media_type, data, ..
        } => Some(json!({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": data
            }
        })),
        SessionTurnContentBlock::ToolUse { id, name, input } => {
            Some(json!({"type": "tool_use", "id": id, "name": name, "input": input}))
        }
        SessionTurnContentBlock::ToolResult {
            tool_use_id,
            content,
        } => Some(json!({"type": "tool_result", "tool_use_id": tool_use_id, "content": content})),
    }
}

fn tool_specs_to_api(tools: Vec<ToolSpec>) -> Option<Vec<ApiToolDefinition>> {
    if tools.is_empty() {
        return None;
    }
    Some(
        tools
            .into_iter()
            .map(|spec| ApiToolDefinition {
                name: spec.name,
                description: spec.description,
                input_schema: spec.input_schema,
            })
            .collect(),
    )
}

fn assistant_turn_message(
    turn: &ContinuedAssistantTurn,
    model: &str,
) -> Result<SessionTurnMessage, AnthropicError> {
    let content = if has_tool_use_block(&turn.final_blocks) {
        let mut content = Vec::new();
        if !turn.merged_text.trim().is_empty() {
            content.push(SessionTurnContentBlock::text(turn.merged_text.clone()));
        }
        content.extend(
            turn.final_blocks
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
                .filter_map(|block| api_block_to_session_turn_block(block).ok()),
        );
        content
    } else if !turn.merged_text.trim().is_empty() {
        vec![SessionTurnContentBlock::text(turn.merged_text.clone())]
    } else {
        assistant_content_blocks_without_thinking(turn)
    };
    let has_tool_use = content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::ToolUse { .. }));
    if turn.final_stop_reason == "tool_use" && !has_tool_use {
        return Err(AnthropicError::NoConsumableOutput {
            reason: "Anthropic tool_use 终态没有完整 tool_use block".into(),
        });
    }
    if content.is_empty() {
        if turn.final_stop_reason == "model_context_window_exceeded" {
            return Ok(SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: Some(crate::api::ProviderReplayState::AnthropicMessages {
                    model: model.to_string(),
                    messages: turn.replay_messages.clone(),
                }),
                content,
            });
        }
        let only_reasoning = !turn.final_blocks.is_empty()
            && turn.final_blocks.iter().all(|block| {
                matches!(
                    block.get("type").and_then(Value::as_str),
                    Some("thinking") | Some("redacted_thinking")
                )
            });
        let reason = if only_reasoning {
            "Anthropic 响应仅包含 thinking/redacted_thinking，没有可消费的 text 或 tool_use".into()
        } else {
            "Anthropic 响应没有可消费的 text 或 tool_use".into()
        };
        if matches!(
            turn.final_stop_reason.as_str(),
            "end_turn" | "stop_sequence" | "tool_use"
        ) {
            return Err(AnthropicError::NoConsumableOutput { reason });
        }
        return Err(AnthropicError::OutputShape {
            reason,
            raw: String::new(),
        });
    }
    Ok(SessionTurnMessage {
        role: "assistant".into(),
        provider_replay: Some(crate::api::ProviderReplayState::AnthropicMessages {
            model: model.to_string(),
            messages: turn.replay_messages.clone(),
        }),
        content,
    })
}

fn provider_stop_from_turn(
    turn: &ContinuedAssistantTurn,
) -> Result<ProviderStop, ProviderTerminalFailure> {
    match turn.final_stop_reason.as_str() {
        "max_tokens" if has_tool_use_block(&turn.final_blocks) => {
            // 完整 tool_use 后协议要求紧跟 tool_result；不能先插内部 continuation。
            Ok(ProviderStop::ToolUse)
        }
        "max_tokens" => Ok(ProviderStop::MaxTokens),
        "tool_use" => Ok(ProviderStop::ToolUse),
        "end_turn" | "stop_sequence" => Ok(ProviderStop::Done),
        "model_context_window_exceeded" => Ok(ProviderStop::ContextWindowExceeded),
        "pause_turn" => Err(ProviderTerminalFailure::new(
            "Anthropic 响应要求暂停并继续 server tool turn，当前 ACN 不支持该终态",
        )),
        "refusal" => Err(ProviderTerminalFailure::new("Anthropic 模型拒绝了本次请求")),
        _ => Err(ProviderTerminalFailure::new(
            "Anthropic 返回不支持的 stop_reason",
        )),
    }
}

fn required_anthropic_stop_reason(response: &Value) -> Result<String, AnthropicError> {
    validated_anthropic_stop_reason(response.get("stop_reason").and_then(Value::as_str))
}

fn validated_anthropic_stop_reason(reason: Option<&str>) -> Result<String, AnthropicError> {
    reason
        .filter(|reason| !reason.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| AnthropicError::OutputShape {
            reason: "缺少有效 stop_reason".into(),
            raw: String::new(),
        })
}

fn assistant_content_blocks_without_thinking(
    turn: &ContinuedAssistantTurn,
) -> Vec<SessionTurnContentBlock> {
    let mut content = Vec::new();
    for block in &turn.final_blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("thinking") | Some("redacted_thinking") => continue,
            Some("text")
                if block
                    .get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.trim().is_empty()) =>
            {
                continue;
            }
            Some("text") | Some("tool_use") => {
                if let Ok(block) = api_block_to_session_turn_block(block) {
                    content.push(block);
                }
            }
            _ => continue,
        }
    }
    if content.is_empty() && !turn.merged_text.trim().is_empty() {
        vec![SessionTurnContentBlock::text(turn.merged_text.clone())]
    } else {
        content
    }
}

fn api_block_to_session_turn_block(
    block: &Value,
) -> Result<SessionTurnContentBlock, AnthropicError> {
    match block.get("type").and_then(Value::as_str) {
        Some("text") => Ok(SessionTurnContentBlock::Text {
            text: block
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        Some("image") => {
            let source = block
                .get("source")
                .ok_or_else(|| AnthropicError::OutputShape {
                    reason: "image block 缺少 source".into(),
                    raw: block.to_string(),
                })?;
            Ok(SessionTurnContentBlock::Image {
                media_type: source
                    .get("media_type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                data: source
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            })
        }
        Some("document") => {
            let source = block
                .get("source")
                .ok_or_else(|| AnthropicError::OutputShape {
                    reason: "document block 缺少 source".into(),
                    raw: block.to_string(),
                })?;
            Ok(SessionTurnContentBlock::Document {
                media_type: source
                    .get("media_type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                data: source
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                filename: None,
            })
        }
        Some("tool_use") => Ok(SessionTurnContentBlock::ToolUse {
            id: block
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            name: block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            input: block.get("input").cloned().unwrap_or_else(|| json!({})),
        }),
        Some("tool_result") => Ok(SessionTurnContentBlock::ToolResult {
            tool_use_id: block
                .get("tool_use_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            content: block
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        other => Err(AnthropicError::OutputShape {
            reason: format!("未知 content block type: {other:?}"),
            raw: block.to_string(),
        }),
    }
}

/// 请求携带媒体块时，上游 4xx（鉴权 / 限流除外）大概率是模型不支持多模态：
/// 这类网关常报"参数 / 格式不对"之类的误导性文案，这里补上明确提示，
/// 同时在 Display 中保留上游核心错误信息（PRD 要求不静默降级）。
fn wrap_media_rejection(error: AnthropicError, request_has_media: bool) -> AnthropicError {
    if !request_has_media {
        return error;
    }
    match error {
        AnthropicError::Status { status, body }
            if (400..500).contains(&status) && status != 401 && status != 429 =>
        {
            AnthropicError::MediaRejected {
                source: Box::new(AnthropicError::Status {
                    status,
                    body: redact_media_error_body(&body),
                }),
            }
        }
        other => other,
    }
}

const REDACTED_ANTHROPIC_PAYLOAD: &str = "[redacted Anthropic request/replay payload]";
const ANTHROPIC_PRIVATE_KEYS: &[&str] = &[
    "request",
    "request_body",
    "messages",
    "input",
    "system",
    "content",
    "thinking",
    "signature",
    "encrypted_content",
    "data",
];

fn redact_anthropic_error_body(body: &str) -> String {
    let redacted = match serde_json::from_str::<Value>(body) {
        Ok(mut value) => {
            redact_anthropic_json_value(&mut value);
            value.to_string()
        }
        Err(_) if contains_anthropic_private_key(body) => REDACTED_ANTHROPIC_PAYLOAD.into(),
        Err(_) => body.to_string(),
    };
    redact_media_error_body(&redacted)
}

fn redact_anthropic_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if ANTHROPIC_PRIVATE_KEYS
                    .iter()
                    .any(|private_key| key.eq_ignore_ascii_case(private_key))
                {
                    *child = Value::String(REDACTED_ANTHROPIC_PAYLOAD.into());
                } else {
                    redact_anthropic_json_value(child);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_anthropic_json_value(item);
            }
        }
        Value::String(text) => {
            if let Ok(mut embedded) = serde_json::from_str::<Value>(text) {
                redact_anthropic_json_value(&mut embedded);
                *text = embedded.to_string();
            } else if contains_anthropic_private_key(text) {
                *text = REDACTED_ANTHROPIC_PAYLOAD.into();
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn contains_anthropic_private_key(text: &str) -> bool {
    let compact = text
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    ANTHROPIC_PRIVATE_KEYS.iter().any(|key| {
        [
            format!("\"{key}\":"),
            format!("\"{key}\"="),
            format!("\\\"{key}\\\":"),
            format!("\\\"{key}\\\"="),
            format!("'{key}':"),
            format!("'{key}'="),
        ]
        .iter()
        .any(|pattern| compact.contains(pattern))
            || contains_unquoted_private_key(text, key)
    })
}

fn contains_unquoted_private_key(text: &str, key: &str) -> bool {
    let lowercase = text.to_ascii_lowercase();
    lowercase.match_indices(key).any(|(start, _)| {
        let previous_is_identifier = lowercase[..start]
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_');
        if previous_is_identifier {
            return false;
        }
        matches!(
            lowercase[start + key.len()..].trim_start().chars().next(),
            Some(':' | '=')
        )
    })
}

fn is_retryable(e: &AnthropicError) -> bool {
    match e {
        AnthropicError::Http(error) => error.is_retryable(),
        AnthropicError::Status { status, .. } => *status == 429 || *status >= 500,
        AnthropicError::ResponseJson(_)
        | AnthropicError::OutputShape { .. }
        | AnthropicError::StreamFailure { .. } => true,
        AnthropicError::Auth(_)
        | AnthropicError::InvalidEndpoint(_)
        | AnthropicError::Prompt(_)
        | AnthropicError::TerminalFailure { .. }
        | AnthropicError::RequestPreparation { .. }
        | AnthropicError::NoConsumableOutput { .. } => false,
        // 多模态拒收是确定性 4xx，重试无意义
        AnthropicError::MediaRejected { .. } => false,
    }
}

fn is_stream_retryable(error: &AnthropicError) -> bool {
    match error {
        AnthropicError::Http(error) => error.is_retryable(),
        AnthropicError::Status { status, .. } => *status == 429 || *status >= 500,
        AnthropicError::StreamFailure { .. } => true,
        AnthropicError::Auth(_)
        | AnthropicError::ResponseJson(_)
        | AnthropicError::InvalidEndpoint(_)
        | AnthropicError::OutputShape { .. }
        | AnthropicError::NoConsumableOutput { .. }
        | AnthropicError::TerminalFailure { .. }
        | AnthropicError::RequestPreparation { .. }
        | AnthropicError::Prompt(_)
        | AnthropicError::MediaRejected { .. } => false,
    }
}

/// 指数退避 + ±50% 随机抖动；attempt 从 0 开始。
/// 抖动避免多个客户端同时重试形成 thundering herd。
fn compute_backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    let factor: u32 = 1u32.checked_shl(attempt.min(10)).unwrap_or(u32::MAX);
    let raw = base.saturating_mul(factor);
    let capped = raw.min(max);
    let center = u64::try_from(capped.as_millis()).unwrap_or(u64::MAX);
    if center == 0 {
        return Duration::ZERO;
    }
    let half = center / 2;
    let low = center.saturating_sub(half);
    let high = center.saturating_add(half);
    let jittered = rand::thread_rng().gen_range(low..=high);
    let max_ms = u64::try_from(max.as_millis()).unwrap_or(u64::MAX);
    Duration::from_millis(jittered.min(max_ms))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Default)]
    struct RecordingRequestObserver {
        requests: Vec<Vec<SessionTurnMessage>>,
    }

    #[async_trait]
    impl ProviderRequestObserver for RecordingRequestObserver {
        async fn before_provider_request(
            &mut self,
            messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.requests.push(messages.to_vec());
            Ok(())
        }
    }

    fn client_with_reasoning_effort(reasoning_effort: ReasoningEffort) -> AnthropicMessagesClient {
        let mut client = AnthropicMessagesClient::new(
            "key".into(),
            "http://127.0.0.1:1".into(),
            "test-model".into(),
            128,
            Duration::from_secs(1),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        client.reasoning_effort = reasoning_effort;
        client
    }

    #[test]
    fn history_media_policy_preserves_uncompacted_images_and_documents() {
        let adapter = AnthropicProviderAdapter {
            client: client_with_reasoning_effort(ReasoningEffort::None),
        };

        assert_eq!(
            adapter.history_media_policy(),
            ProviderHistoryMediaPolicy::Preserve
        );
    }

    #[test]
    fn model_context_keeps_anthropic_wire_prefix_stable_without_local_role_merge() {
        let prefix = vec![
            SessionTurnMessage::model_context(
                crate::api::ModelContextSource::Runtime,
                "<runtime_context>stable</runtime_context>",
            ),
            SessionTurnMessage::model_context(
                crate::api::ModelContextSource::BackgroundProcess,
                "<background_processes>empty</background_processes>",
            ),
            SessionTurnMessage::user_text("first request"),
        ];
        let first =
            serde_json::to_value(session_turn_messages_to_api(prefix.clone(), "test-model"))
                .unwrap();
        let mut extended = prefix;
        extended.push(SessionTurnMessage::assistant_text("first answer"));
        extended.push(SessionTurnMessage::user_text("second request"));
        let second =
            serde_json::to_value(session_turn_messages_to_api(extended, "test-model")).unwrap();
        let first_messages = first.as_array().unwrap();
        let second_messages = second.as_array().unwrap();

        assert!(second_messages.starts_with(first_messages));
        assert_eq!(first_messages.len(), 3);
        assert!(first_messages
            .iter()
            .all(|message| message["role"] == "user"));
        assert_eq!(
            first_messages[0]["content"][0]["text"],
            "<runtime_context>stable</runtime_context>"
        );
        assert_eq!(
            first_messages[1]["content"][0]["text"],
            "<background_processes>empty</background_processes>"
        );
        assert_eq!(first_messages[2]["content"][0]["text"], "first request");
        assert!(!first.to_string().contains("sha256-v1"));
    }

    #[test]
    fn model_context_after_tool_result_keeps_distinct_anthropic_wire_message() {
        let messages = vec![
            SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "file_read".into(),
                    input: json!({"path":"README.md"}),
                }],
                provider_replay: None,
            },
            SessionTurnMessage::user_content(vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "done".into(),
            }]),
            SessionTurnMessage::model_context(
                crate::api::ModelContextSource::BackgroundProcess,
                "<background_processes>changed</background_processes>",
            ),
        ];

        let body =
            serde_json::to_value(session_turn_messages_to_api(messages, "test-model")).unwrap();
        let api_messages = body.as_array().unwrap();

        assert_eq!(api_messages.len(), 3);
        assert_eq!(api_messages[0]["role"], "assistant");
        assert_eq!(api_messages[1]["role"], "user");
        assert_eq!(api_messages[1]["content"][0]["type"], "tool_result");
        assert_eq!(api_messages[1]["content"][0]["tool_use_id"], "toolu_1");
        assert_eq!(api_messages[2]["role"], "user");
        assert_eq!(api_messages[2]["content"][0]["type"], "text");
        assert_eq!(
            api_messages[2]["content"][0]["text"],
            "<background_processes>changed</background_processes>"
        );
    }

    #[test]
    fn none_reasoning_effort_omits_anthropic_output_config() {
        let client = client_with_reasoning_effort(ReasoningEffort::None);
        let request = client.request_for("system", Vec::new(), None, 128, None);
        let body = serde_json::to_value(request).unwrap();

        assert!(body.get("output_config").is_none());
        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn configured_reasoning_effort_is_nested_for_anthropic_streaming_and_non_streaming() {
        let client = client_with_reasoning_effort(ReasoningEffort::Xhigh);

        for stream in [None, Some(true)] {
            let request = client.request_for("system", Vec::new(), None, 128, stream);
            let body = serde_json::to_value(request).unwrap();
            assert_eq!(body.get("output_config"), Some(&json!({"effort": "xhigh"})));
            assert!(body.get("reasoning_effort").is_none());
        }
    }

    #[test]
    fn anthropic_thinking_request_modes_are_serialized_without_beta_header_contract() {
        let cases = [
            (AnthropicThinking::Auto, None, None),
            (
                AnthropicThinking::Enabled,
                Some(4096),
                Some(json!({"type":"enabled", "budget_tokens":4096})),
            ),
            (
                AnthropicThinking::Enabled,
                None,
                Some(json!({"type":"enabled"})),
            ),
            (
                AnthropicThinking::Adaptive,
                Some(4096),
                Some(json!({"type":"adaptive"})),
            ),
            (
                AnthropicThinking::Disabled,
                Some(4096),
                Some(json!({"type":"disabled"})),
            ),
        ];

        for (mode, budget, expected) in cases {
            let mut client = client_with_reasoning_effort(ReasoningEffort::None);
            client.thinking = mode;
            client.thinking_budget_tokens = budget;
            for stream in [None, Some(true)] {
                let body = serde_json::to_value(client.request_for(
                    "system",
                    Vec::new(),
                    None,
                    128,
                    stream,
                ))
                .unwrap();
                assert_eq!(body.get("thinking").cloned(), expected);
            }
        }
    }

    #[test]
    fn anthropic_replay_messages_win_over_canonical_and_preserve_unknown_fields() {
        let replay = vec![
            json!({
                "role":"assistant",
                "content":[{
                    "type":"thinking",
                    "thinking":"private",
                    "signature":"opaque",
                    "vendor_extension":{"future":true}
                }]
            }),
            json!({"role":"user", "content":"Continue from where you left off."}),
            json!({
                "role":"assistant",
                "content":[{"type":"text", "text":"visible"}]
            }),
        ];
        let messages = session_turn_messages_to_api(
            vec![SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::text(
                    "canonical must not duplicate",
                )],
                provider_replay: Some(crate::api::ProviderReplayState::AnthropicMessages {
                    model: "test-model".into(),
                    messages: replay.clone(),
                }),
            }],
            "test-model",
        );

        assert_eq!(serde_json::to_value(messages).unwrap(), json!(replay));
    }

    #[test]
    fn anthropic_wrong_model_replay_falls_back_to_canonical_projection() {
        let messages = session_turn_messages_to_api(
            vec![SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::text("canonical")],
                provider_replay: Some(crate::api::ProviderReplayState::AnthropicMessages {
                    model: "other-model".into(),
                    messages: vec![json!({
                        "role":"assistant",
                        "content":[{"type":"thinking", "thinking":"private"}]
                    })],
                }),
            }],
            "test-model",
        );
        let body = serde_json::to_value(messages).unwrap();

        assert_eq!(body[0]["content"][0]["text"], "canonical");
        assert!(!body.to_string().contains("private"));
    }

    #[test]
    fn anthropic_tool_result_follows_full_private_assistant_replay() {
        let assistant = json!({
            "role":"assistant",
            "content":[
                {"type":"thinking", "thinking":"private", "signature":"opaque"},
                {"type":"tool_use", "id":"toolu_1", "name":"file_read", "input":{"path":"README.md"}}
            ]
        });
        let messages = session_turn_messages_to_api(
            vec![
                SessionTurnMessage {
                    role: "assistant".into(),
                    content: vec![SessionTurnContentBlock::ToolUse {
                        id: "toolu_1".into(),
                        name: "file_read".into(),
                        input: json!({"path":"README.md"}),
                    }],
                    provider_replay: Some(crate::api::ProviderReplayState::AnthropicMessages {
                        model: "test-model".into(),
                        messages: vec![assistant.clone()],
                    }),
                },
                SessionTurnMessage {
                    role: "user".into(),
                    content: vec![SessionTurnContentBlock::ToolResult {
                        tool_use_id: "toolu_1".into(),
                        content: "file contents".into(),
                    }],
                    provider_replay: None,
                },
            ],
            "test-model",
        );
        let body = serde_json::to_value(messages).unwrap();

        assert_eq!(body[0], assistant);
        assert_eq!(body[1]["role"], "user");
        assert_eq!(body[1]["content"][0]["type"], "tool_result");
        assert_eq!(body[1]["content"][0]["tool_use_id"], "toolu_1");
    }

    #[test]
    fn anthropic_assistant_keeps_raw_thinking_but_only_canonicalizes_text_and_tools() {
        let replay_message = json!({
            "role":"assistant",
            "content":[
                {"type":"thinking", "thinking":"private", "signature":"sig"},
                {"type":"future_private", "opaque":"keep"},
                {"type":"text", "text":"answer"},
                {"type":"tool_use", "id":"toolu_1", "name":"file_read", "input":{"path":"README.md"}}
            ]
        });
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: replay_message["content"].as_array().unwrap().clone(),
            final_stop_reason: "tool_use".into(),
            merged_text: "answer".into(),
            replay_messages: vec![replay_message.clone()],
        };

        let message = assistant_turn_message(&turn, "test-model").unwrap();

        assert_eq!(message.content.len(), 2);
        assert!(matches!(
            message.content[0],
            SessionTurnContentBlock::Text { .. }
        ));
        assert!(matches!(
            message.content[1],
            SessionTurnContentBlock::ToolUse { .. }
        ));
        assert_eq!(
            message.provider_replay,
            Some(crate::api::ProviderReplayState::AnthropicMessages {
                model: "test-model".into(),
                messages: vec![replay_message],
            })
        );
    }

    #[test]
    fn anthropic_reasoning_only_success_is_rejected_without_exposing_payload() {
        let secret = "private-thinking";
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: vec![json!({
                "type":"thinking", "thinking":secret, "signature":"opaque-signature"
            })],
            final_stop_reason: "end_turn".into(),
            merged_text: String::new(),
            replay_messages: Vec::new(),
        };

        let error = assistant_turn_message(&turn, "test-model").unwrap_err();

        assert!(matches!(error, AnthropicError::NoConsumableOutput { .. }));
        assert!(error.to_string().contains("仅包含 thinking"));
        assert!(!error.to_string().contains(secret));
        assert!(!error.to_string().contains("opaque-signature"));
    }

    #[test]
    fn anthropic_empty_tool_terminal_has_no_consumable_content() {
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: Vec::new(),
            final_stop_reason: "tool_use".into(),
            merged_text: String::new(),
            replay_messages: Vec::new(),
        };

        let error = assistant_turn_message(&turn, "test-model").unwrap_err();

        assert!(matches!(error, AnthropicError::NoConsumableOutput { .. }));
    }

    #[test]
    fn anthropic_text_only_tool_terminal_has_no_consumable_content() {
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: vec![json!({"type":"text","text":"我来查询"})],
            final_stop_reason: "tool_use".into(),
            merged_text: "我来查询".into(),
            replay_messages: Vec::new(),
        };

        let error = assistant_turn_message(&turn, "test-model").unwrap_err();

        assert!(matches!(error, AnthropicError::NoConsumableOutput { .. }));
    }

    #[test]
    fn anthropic_stream_failure_is_exposed_to_provider_turn_loop() {
        assert!(anthropic_adapter_stream_failure(
            &AnthropicError::StreamFailure {
                reason: "missing message_stop".into(),
                raw: String::new(),
            }
        ));
        assert!(!anthropic_adapter_stream_failure(
            &AnthropicError::OutputShape {
                reason: "non-stream shape".into(),
                raw: String::new(),
            }
        ));
    }

    #[test]
    fn anthropic_reasoning_with_empty_text_is_not_committed_as_empty_success() {
        let secret = "private-thinking";
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: vec![
                json!({
                    "type":"thinking", "thinking":secret, "signature":"opaque-signature"
                }),
                json!({"type":"text", "text":"  "}),
            ],
            final_stop_reason: "end_turn".into(),
            merged_text: String::new(),
            replay_messages: Vec::new(),
        };

        let error = assistant_turn_message(&turn, "test-model").unwrap_err();

        assert!(error.to_string().contains("没有可消费的 text 或 tool_use"));
        assert!(!error.to_string().contains(secret));
        assert!(!error.to_string().contains("opaque-signature"));
    }

    #[test]
    fn anthropic_error_redaction_removes_nested_and_embedded_private_replay() {
        let secret = "opaque-thinking-payload";
        let body = json!({
            "error": {
                "code":"invalid_request",
                "message": json!({
                    "MeSsAgEs":[{"role":"assistant", "content":[{
                        "type":"thinking", "thinking":secret, "signature":"opaque-signature"
                    }]}]
                }).to_string()
            }
        })
        .to_string();

        let redacted = redact_anthropic_error_body(&body);

        assert!(redacted.contains("invalid_request"));
        assert!(redacted.contains(REDACTED_ANTHROPIC_PAYLOAD));
        assert!(!redacted.contains(secret));
        assert!(!redacted.contains("opaque-signature"));
    }

    #[test]
    fn anthropic_error_redaction_removes_echoed_request_fields_but_keeps_diagnostics() {
        let input_secret = "private-user-input";
        let system_secret = "private-system-prompt";
        let content_secret = "private-content-block";
        let body = json!({
            "error": {
                "code":"invalid_request",
                "message":"safe-diagnostic",
                "details": {
                    "InPuT":{"prompt":input_secret},
                    "SYSTEM":system_secret,
                    "CoNtEnT":[{"type":"text", "text":content_secret}],
                    "parameter":"messages.2.content"
                }
            }
        })
        .to_string();

        let redacted = redact_anthropic_error_body(&body);

        assert!(redacted.contains("invalid_request"));
        assert!(redacted.contains("safe-diagnostic"));
        assert!(redacted.contains("messages.2.content"));
        assert!(!redacted.contains(input_secret));
        assert!(!redacted.contains(system_secret));
        assert!(!redacted.contains(content_secret));
    }

    #[test]
    fn anthropic_error_redaction_hides_non_json_request_echo() {
        let secret = "private-user-input";
        let body = format!("invalid request: input: {secret}");

        let redacted = redact_anthropic_error_body(&body);

        assert_eq!(redacted, REDACTED_ANTHROPIC_PAYLOAD);
        assert!(!redacted.contains(secret));

        let quoted_body = format!(r#"invalid request: \"InPuT\" = \"{secret}\""#);
        let quoted_redacted = redact_anthropic_error_body(&quoted_body);
        assert_eq!(quoted_redacted, REDACTED_ANTHROPIC_PAYLOAD);
        assert!(!quoted_redacted.contains(secret));
    }

    #[test]
    fn anthropic_error_redaction_hides_unquoted_echo_inside_json_message() {
        let secret = "private-system-prompt";
        let body = json!({
            "error": {
                "code":"invalid_request",
                "message":format!("validation failed; SyStEm = {secret}")
            }
        })
        .to_string();

        let redacted = redact_anthropic_error_body(&body);

        assert!(redacted.contains("invalid_request"));
        assert!(redacted.contains(REDACTED_ANTHROPIC_PAYLOAD));
        assert!(!redacted.contains(secret));
        assert!(!contains_anthropic_private_key(
            "invalid_input: missing required field"
        ));
    }

    #[tokio::test]
    async fn non_streaming_max_token_continuation_preserves_private_message_sequence() {
        let responses = vec![
            json!({
                "content":[
                    {"type":"thinking", "thinking":"private-one", "signature":"sig-one"},
                    {"type":"text", "text":"first "}
                ],
                "stop_reason":"max_tokens",
                "usage":{"input_tokens":1,"output_tokens":2}
            }),
            json!({
                "content":[
                    {"type":"redacted_thinking", "data":"opaque-two"},
                    {"type":"text", "text":"second"}
                ],
                "stop_reason":"end_turn",
                "usage":{"input_tokens":3,"output_tokens":4}
            }),
        ];
        let (endpoint, requests) = spawn_json_server(responses).await;
        let client = AnthropicMessagesClient::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut messages = vec![ApiMessage::structured(
            "user",
            vec![json!({"type":"text", "text":"hello"})],
        )];

        let turn = client
            .send_text_with_continuation_for_provider_with_retry_count(
                "system",
                &mut messages,
                None,
                128,
                0,
            )
            .await
            .unwrap();

        assert_eq!(turn.merged_text, "first second");
        assert_eq!(turn.replay_messages.len(), 3);
        assert_eq!(
            turn.replay_messages[0]["content"][0]["thinking"],
            "private-one"
        );
        assert_eq!(turn.replay_messages[1]["content"], CONTINUATION_TRIGGER);
        assert_eq!(turn.replay_messages[2]["content"][0]["data"], "opaque-two");
        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 2);
        assert_eq!(messages.len(), 4);
        let replayed_messages = serde_json::to_value(&messages[1..]).unwrap();
        assert!(replayed_messages.to_string().contains("private-one"));
        assert!(replayed_messages.to_string().contains(CONTINUATION_TRIGGER));
    }

    #[tokio::test]
    async fn non_streaming_continuation_observer_matches_exact_anthropic_messages() {
        let responses = vec![
            json!({
                "content":[
                    {"type":"thinking", "thinking":"private-one", "signature":"sig-one"},
                    {"type":"text", "text":"first "}
                ],
                "stop_reason":"max_tokens",
                "usage":{"input_tokens":1,"output_tokens":2}
            }),
            json!({
                "content":[{"type":"text", "text":"second"}],
                "stop_reason":"end_turn",
                "usage":{"input_tokens":3,"output_tokens":4}
            }),
        ];
        let (endpoint, requests) = spawn_json_server(responses).await;
        let adapter = AnthropicProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut observer = RecordingRequestObserver::default();

        adapter
            .send_with_request_observer(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 128,
                    stream: false,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: None,
                    runtime_fallback_scope: None,
                    recovery_interrupt: None,
                    retry_count_override: None,
                },
                &mut |_| {},
                &mut observer,
            )
            .await
            .unwrap();

        assert_eq!(observer.requests.len(), 2);
        assert!(observer.requests[1].starts_with(&observer.requests[0]));
        let observed_second = serde_json::to_value(session_turn_messages_to_api(
            observer.requests[1].clone(),
            "test-model",
        ))
        .unwrap();
        let captured = requests.lock().unwrap();
        let captured_second: Value = serde_json::from_str(
            captured[1]
                .split_once("\r\n\r\n")
                .map(|(_, body)| body)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            observed_second, captured_second["messages"],
            "observer 上报的 neutral history 必须映射为同一份 Anthropic messages"
        );
    }

    #[tokio::test]
    async fn non_streaming_context_window_stop_returns_valid_partial_without_internal_retry() {
        let responses = vec![json!({
            "content":[
                {"type":"thinking", "thinking":"private-context", "signature":"sig-context"},
                {"type":"text", "text":"visible partial"}
            ],
            "stop_reason":"model_context_window_exceeded",
            "usage":{"input_tokens":120,"output_tokens":8}
        })];
        let (endpoint, requests) = spawn_json_server(responses).await;
        let client = AnthropicMessagesClient::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut messages = vec![ApiMessage::structured(
            "user",
            vec![json!({"type":"text", "text":"hello"})],
        )];

        let turn = client
            .send_text_with_continuation_for_provider_with_retry_count(
                "system",
                &mut messages,
                None,
                128,
                0,
            )
            .await
            .unwrap();
        let assistant = assistant_turn_message(&turn, "test-model").unwrap();

        assert_eq!(turn.merged_text, "visible partial");
        assert_eq!(turn.replay_messages.len(), 1);
        assert_eq!(
            turn.replay_messages[0]["content"][0]["signature"],
            "sig-context"
        );
        assert_eq!(
            provider_stop_from_turn(&turn).unwrap(),
            ProviderStop::ContextWindowExceeded
        );
        assert!(matches!(
            assistant.provider_replay,
            Some(crate::api::ProviderReplayState::AnthropicMessages { .. })
        ));
        assert_eq!(requests.lock().unwrap().len(), 1);
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn non_streaming_response_requires_explicit_stop_reason() {
        for response in [
            json!({"content": []}),
            json!({"content": [], "stop_reason": null}),
            json!({"content": [], "stop_reason": ""}),
            json!({"content": [], "stop_reason": 3}),
        ] {
            let error = required_anthropic_stop_reason(&response)
                .expect_err("missing or malformed stop_reason must fail closed");
            assert!(error.to_string().contains("缺少有效 stop_reason"));
        }

        assert_eq!(
            required_anthropic_stop_reason(&json!({
                "content": [],
                "stop_reason": "end_turn"
            }))
            .unwrap(),
            "end_turn"
        );
    }

    #[test]
    fn provider_stop_uses_explicit_reason_instead_of_inferring_from_tool_blocks() {
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: vec![json!({
                "type": "tool_use",
                "id": "toolu_1",
                "name": "file_read",
                "input": {"path": "README.md"}
            })],
            final_stop_reason: "end_turn".into(),
            merged_text: String::new(),
            replay_messages: Vec::new(),
        };

        assert_eq!(provider_stop_from_turn(&turn).unwrap(), ProviderStop::Done);
    }

    #[test]
    fn max_tokens_with_complete_tool_use_enters_tool_loop_before_continuation() {
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: vec![json!({
                "type": "tool_use",
                "id": "toolu_1",
                "name": "file_read",
                "input": {"path": "README.md"}
            })],
            final_stop_reason: "max_tokens".into(),
            merged_text: String::new(),
            replay_messages: Vec::new(),
        };

        assert_eq!(
            provider_stop_from_turn(&turn).unwrap(),
            ProviderStop::ToolUse
        );
    }

    #[test]
    fn provider_stop_maps_context_window_exhaustion_to_recoverable_stop() {
        let turn = ContinuedAssistantTurn {
            final_response: json!({}),
            final_blocks: vec![json!({"type":"text", "text":"partial"})],
            final_stop_reason: "model_context_window_exceeded".into(),
            merged_text: "partial".into(),
            replay_messages: Vec::new(),
        };

        assert_eq!(
            provider_stop_from_turn(&turn).unwrap(),
            ProviderStop::ContextWindowExceeded
        );
    }

    #[test]
    fn provider_stop_rejects_non_success_terminal_reasons() {
        for (reason, expected_error) in [
            ("pause_turn", "server tool turn"),
            ("refusal", "拒绝"),
            ("vendor_extension", "不支持的 stop_reason"),
        ] {
            let turn = ContinuedAssistantTurn {
                final_response: json!({}),
                final_blocks: vec![json!({"type":"text", "text":"partial"})],
                final_stop_reason: reason.into(),
                merged_text: "partial".into(),
                replay_messages: Vec::new(),
            };

            let error = provider_stop_from_turn(&turn).unwrap_err();

            assert!(error.to_string().contains(expected_error));
            assert!(!error.to_string().contains("partial"));
        }
    }

    #[test]
    fn image_block_maps_to_base64_source() {
        let block = session_turn_block_to_api(SessionTurnContentBlock::image("image/png", "QUJD"))
            .expect("image block 应有 API 映射");
        assert_eq!(
            block,
            json!({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/png", "data": "QUJD"}
            })
        );
    }

    #[test]
    fn document_block_maps_to_base64_source_without_filename() {
        let block = session_turn_block_to_api(SessionTurnContentBlock::document_named(
            "application/pdf",
            "QUJD",
            "brief.pdf",
        ))
        .expect("document block 应有 API 映射");
        // PRD 拍板：Anthropic document 只发 source，filename 不进请求体
        assert_eq!(
            block,
            json!({
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf", "data": "QUJD"}
            })
        );
    }

    #[test]
    fn responses_replay_is_ignored_when_projecting_to_anthropic() {
        let messages = session_turn_messages_to_api(
            vec![
                SessionTurnMessage::assistant_text("canonical text").with_provider_replay(
                    crate::api::ProviderReplayState::OpenAiResponses {
                        model: Some("test-model".into()),
                        items: vec![json!({
                            "type":"reasoning","encrypted_content":"opaque-anthropic-must-ignore"
                        })],
                    },
                ),
            ],
            "test-model",
        );

        let body = serde_json::to_string(&messages).unwrap();
        assert!(body.contains("canonical text"));
        assert!(!body.contains("opaque-anthropic-must-ignore"));
    }

    #[test]
    fn media_request_4xx_wraps_hint_and_preserves_upstream_body() {
        let error = wrap_media_rejection(
            AnthropicError::Status {
                status: 400,
                body: "invalid_request_error: unsupported content type".into(),
            },
            true,
        );
        let text = error.to_string();
        assert!(text.contains("可能不支持图片 / PDF 附件"));
        assert!(text.contains("unsupported content type"));
        assert!(!is_retryable(&error));
    }

    #[test]
    fn media_request_4xx_redacts_echoed_media_payload() {
        let echoed = serde_json::json!({
            "error": {
                "message": "unsupported image",
                "request": {
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "A".repeat(300)
                    }
                }
            }
        })
        .to_string();
        let error = wrap_media_rejection(
            AnthropicError::Status {
                status: 400,
                body: echoed,
            },
            true,
        );
        let text = error.to_string();
        assert!(text.contains("unsupported image"));
        assert!(text.contains("[redacted media payload]"));
        assert!(!text.contains(&"A".repeat(300)));
    }

    #[test]
    fn media_rejection_hint_skips_non_media_auth_and_rate_limit_errors() {
        let error = wrap_media_rejection(
            AnthropicError::Status {
                status: 400,
                body: "bad".into(),
            },
            false,
        );
        assert!(!error.to_string().contains("可能不支持图片"));
        for status in [401u16, 429] {
            let error = wrap_media_rejection(
                AnthropicError::Status {
                    status,
                    body: "x".into(),
                },
                true,
            );
            assert!(!error.to_string().contains("可能不支持图片"));
        }
    }

    async fn spawn_json_server(responses: Vec<Value>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_task = Arc::clone(&requests);
        tokio::spawn(async move {
            for response_value in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0; 16_384];
                let read = socket.read(&mut buffer).await.unwrap();
                requests_for_task
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buffer[..read]).to_string());
                let body = response_value.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{addr}"), requests)
    }
}
