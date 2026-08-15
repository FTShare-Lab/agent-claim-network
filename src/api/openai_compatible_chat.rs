//! OpenAI-compatible Chat Completions provider adapter。
//!
//! 本模块只负责 canonical session message 与 Chat Completions 协议互转。
//! HTTP/SSE、重试和基础 DTO 复用 `chat_completions` 模块，供主 LLM 与 router rerank
//! 共用同一套 chat-compatible 实现。

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use super::chat_completions::{
    is_stream_failure, ChatCompletionChoice, ChatCompletionMessage, ChatCompletionRequest,
    ChatCompletionResponse, ChatCompletionsClient, ChatCompletionsError, ChatContentPart,
    ChatFinishReason, ChatMessage, ChatMessageContent, ChatStreamEvent, ChatStreamOptions,
    ChatTool, ChatToolCall,
};
use super::context_usage_from_openai_usage;
use super::continuation::{
    append_with_overlap_dedupe, CONTINUATION_TRIGGER, MAX_CONTINUATION_TURNS,
};
use super::provider::{
    NoopProviderRequestObserver, ProviderAdapter, ProviderEvent, ProviderHistoryMediaPolicy,
    ProviderNoConsumableOutput, ProviderRecoveryInterrupt, ProviderReplayIdentity,
    ProviderReplayProtocol, ProviderRequest, ProviderRequestObserver,
    ProviderRequestPreparationFailure, ProviderResponse, ProviderStop, ProviderStreamFailure,
    ProviderTerminalFailure, ProviderTransport, ToolSpec,
};
use super::redact_media_error_body;
use super::types::{ProviderReplayState, SessionTurnContentBlock, SessionTurnMessage};
use crate::api::SessionTurnInterrupted;
use crate::config::ReasoningEffort;

#[derive(Debug, thiserror::Error)]
pub enum OpenAiCompatibleChatError {
    #[error(transparent)]
    Client(#[from] ChatCompletionsError),
    #[error("Chat Completions 输出不符合预期: {reason}; raw={raw}")]
    OutputShape { reason: String, raw: String },
    #[error("Chat Completions 没有可消费输出: {reason}")]
    NoConsumableOutput { reason: String },
    #[error("Chat Completions 返回确定性终态: {reason}")]
    TerminalFailure { reason: String },
    #[error("准备 Chat continuation request 失败: {reason}")]
    RequestPreparation { reason: String },
    #[error(
        "当前模型可能不支持图片 / PDF 附件输入，请确认模型多模态能力或移除附件后重试。上游原始错误: {source}"
    )]
    MediaRejected {
        #[source]
        source: ChatCompletionsError,
    },
}

pub struct OpenAiCompatibleChatProviderAdapter {
    client: ChatCompletionsClient,
    model: String,
    reasoning_effort: ReasoningEffort,
    temperature: Option<f32>,
}

impl OpenAiCompatibleChatProviderAdapter {
    pub fn new(
        api_key: String,
        endpoint: String,
        model: String,
        timeout: Duration,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Result<Self, OpenAiCompatibleChatError> {
        Ok(Self {
            client: ChatCompletionsClient::new(
                endpoint,
                api_key,
                timeout,
                retry_count,
                retry_base_delay,
                retry_max_delay,
            )?,
            model,
            reasoning_effort: ReasoningEffort::None,
            temperature: None,
        })
    }

    /// 设置 Chat Completions 请求的推理强度；`none` 会在序列化时省略。
    pub fn with_reasoning_effort(mut self, reasoning_effort: ReasoningEffort) -> Self {
        self.reasoning_effort = reasoning_effort;
        self
    }

    /// 为内部确定性任务设置采样温度；普通 Agent 请求保持 provider 默认值。
    pub(crate) fn with_temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }

    fn request_for(
        &self,
        system_prompt: &str,
        messages: Vec<ChatMessage>,
        tools: Vec<ChatTool>,
        max_tokens: u32,
        stream: bool,
    ) -> ChatCompletionRequest {
        let mut chat_messages = Vec::with_capacity(messages.len() + 1);
        if !system_prompt.trim().is_empty() {
            chat_messages.push(ChatMessage::system(system_prompt.to_string()));
        }
        chat_messages.extend(messages);
        ChatCompletionRequest {
            model: self.model.clone(),
            messages: chat_messages,
            reasoning_effort: (self.reasoning_effort != ReasoningEffort::None)
                .then_some(self.reasoning_effort),
            tools: if tools.is_empty() { None } else { Some(tools) },
            max_tokens,
            stream,
            stream_options: stream.then_some(ChatStreamOptions {
                include_usage: true,
            }),
            temperature: self.temperature,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "provider continuation 需同时携带 stream 与 retry 策略，保持请求语义显式"
    )]
    async fn send_with_continuation(
        &self,
        system_prompt: &str,
        messages: &mut Vec<ChatMessage>,
        base_messages: &[SessionTurnMessage],
        tools: Vec<ChatTool>,
        max_tokens: u32,
        stream: bool,
        retry_count: u32,
        retry_after_partial: bool,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> Result<ContinuedChatTurn, OpenAiCompatibleChatError> {
        let replay_start = messages.len();
        let mut merged_text = String::new();
        let mut last_message = None;
        let mut last_finish_reason = Some(ChatFinishReason::Stop);
        let mut continuation_requests_started = 0usize;
        let mut provider_messages = base_messages.to_vec();

        for round in 0..=MAX_CONTINUATION_TURNS {
            if let Some(interrupt) = recovery_interrupt.filter(|interrupt| interrupt.is_cancelled())
            {
                if last_finish_reason == Some(ChatFinishReason::Length) && last_message.is_some() {
                    discard_pending_chat_continuation(messages, &mut provider_messages)?;
                    interrupt.preserve_successful_response();
                    last_finish_reason = Some(ChatFinishReason::Stop);
                    break;
                }
                return Err(ChatCompletionsError::RecoveryInterrupted.into());
            }
            observer
                .before_provider_request(&provider_messages)
                .await
                .map_err(|error| OpenAiCompatibleChatError::RequestPreparation {
                    reason: format!("{error:#}"),
                })?;
            if let Some(interrupt) = recovery_interrupt.filter(|interrupt| interrupt.is_cancelled())
            {
                if last_finish_reason == Some(ChatFinishReason::Length) && last_message.is_some() {
                    observer
                        .provider_request_abandoned_before_send(&provider_messages)
                        .await
                        .map_err(|error| OpenAiCompatibleChatError::RequestPreparation {
                            reason: format!("{error:#}"),
                        })?;
                    discard_pending_chat_continuation(messages, &mut provider_messages)?;
                    interrupt.preserve_successful_response();
                    last_finish_reason = Some(ChatFinishReason::Stop);
                    break;
                }
                return Err(ChatCompletionsError::RecoveryInterrupted.into());
            }
            let request = self.request_for(
                system_prompt,
                messages.clone(),
                tools.clone(),
                max_tokens,
                stream,
            );
            let mut request_start_recorded = false;
            let response_result = {
                let mut request_started = || {
                    if request_start_recorded {
                        return Ok(());
                    }
                    observer
                        .provider_request_started(&provider_messages)
                        .map_err(|error| ChatCompletionsError::RequestPreparation {
                            reason: format!("{error:#}"),
                        })?;
                    request_start_recorded = true;
                    if round > 0 {
                        continuation_requests_started =
                            continuation_requests_started.saturating_add(1);
                    }
                    Ok(())
                };
                if stream {
                    let mut chat_emit = |event| match event {
                        ChatStreamEvent::ContentDelta { text } => {
                            emit(ProviderEvent::AssistantTextDelta { text });
                        }
                    };
                    self.client
                        .send_with_retry_count_and_mode_and_interrupt_and_start_hook(
                            &request,
                            retry_count,
                            retry_after_partial,
                            recovery_interrupt,
                            &mut chat_emit,
                            &mut request_started,
                        )
                        .await
                } else {
                    let mut noop = |_event: ChatStreamEvent| {};
                    self.client
                        .send_with_retry_count_and_mode_and_interrupt_and_start_hook(
                            &request,
                            retry_count,
                            false,
                            recovery_interrupt,
                            &mut noop,
                            &mut request_started,
                        )
                        .await
                }
            };
            let response = match response_result {
                Ok(response) => response,
                Err(ChatCompletionsError::RecoveryInterrupted)
                    if !request_start_recorded
                        && last_finish_reason == Some(ChatFinishReason::Length)
                        && last_message.is_some() =>
                {
                    observer
                        .provider_request_abandoned_before_send(&provider_messages)
                        .await
                        .map_err(|error| OpenAiCompatibleChatError::RequestPreparation {
                            reason: format!("{error:#}"),
                        })?;
                    discard_pending_chat_continuation(messages, &mut provider_messages)?;
                    recovery_interrupt
                        .expect("RecoveryInterrupted requires a recovery interrupt")
                        .preserve_successful_response();
                    last_finish_reason = Some(ChatFinishReason::Stop);
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(usage) = response
                .usage
                .as_ref()
                .and_then(context_usage_from_openai_usage)
            {
                emit(ProviderEvent::ContextUsageUpdated { usage });
            }
            let choice = first_choice(response)?;
            let assistant = choice.message;
            let finish_reason = require_finish_reason(choice.finish_reason)?;
            reject_unsupported_finish_reason(&finish_reason)?;
            if let Some(text) = assistant.content.as_deref() {
                append_with_overlap_dedupe(&mut merged_text, text);
            }
            let round_text = assistant.content.clone().unwrap_or_default();
            let assistant_replay = message_from_response(&assistant);
            messages.push(assistant_replay.clone());
            let has_tool_calls = !assistant.tool_calls.is_empty();
            last_message = Some(assistant);
            last_finish_reason = Some(finish_reason.clone());

            if finish_reason != ChatFinishReason::Length {
                break;
            }
            if has_tool_calls {
                break;
            }
            if let Some(interrupt) = recovery_interrupt.filter(|interrupt| interrupt.is_cancelled())
            {
                interrupt.preserve_successful_response();
                last_finish_reason = Some(ChatFinishReason::Stop);
                break;
            }
            if round == MAX_CONTINUATION_TURNS {
                break;
            }
            let continuation = ChatMessage::user(CONTINUATION_TRIGGER.to_string());
            messages.push(continuation.clone());
            let replay_messages = [assistant_replay, continuation]
                .into_iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| OpenAiCompatibleChatError::OutputShape {
                    reason: format!("序列化 Chat continuation replay 失败: {error}"),
                    raw: String::new(),
                })?;
            let content = if round_text.trim().is_empty() {
                Vec::new()
            } else {
                vec![SessionTurnContentBlock::text(round_text)]
            };
            provider_messages.push(SessionTurnMessage {
                role: "assistant".into(),
                content,
                provider_replay: Some(ProviderReplayState::OpenAiChatCompletions {
                    model: self.model.clone(),
                    messages: replay_messages,
                }),
            });
        }

        let message = last_message.ok_or_else(|| OpenAiCompatibleChatError::OutputShape {
            reason: "空响应：未获得 assistant message".into(),
            raw: String::new(),
        })?;
        Ok(ContinuedChatTurn {
            message,
            finish_reason: last_finish_reason,
            merged_text,
            replay_messages: if continuation_requests_started > 0 {
                Some(
                    messages[replay_start..]
                        .iter()
                        .map(serde_json::to_value)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|error| OpenAiCompatibleChatError::OutputShape {
                            reason: format!("序列化 Chat continuation replay 失败: {error}"),
                            raw: String::new(),
                        })?,
                )
            } else {
                None
            },
        })
    }
}

fn discard_pending_chat_continuation(
    messages: &mut Vec<ChatMessage>,
    provider_messages: &mut Vec<SessionTurnMessage>,
) -> Result<(), OpenAiCompatibleChatError> {
    messages
        .pop()
        .ok_or_else(|| OpenAiCompatibleChatError::OutputShape {
            reason: "safe steer 收束时缺少未发送的 Chat continuation".into(),
            raw: String::new(),
        })?;
    provider_messages
        .pop()
        .ok_or_else(|| OpenAiCompatibleChatError::OutputShape {
            reason: "safe steer 收束时缺少未发送的 Chat neutral replay".into(),
            raw: String::new(),
        })?;
    Ok(())
}

#[async_trait]
impl ProviderAdapter for OpenAiCompatibleChatProviderAdapter {
    fn history_media_policy(&self) -> ProviderHistoryMediaPolicy {
        ProviderHistoryMediaPolicy::Preserve
    }

    fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        Some(ProviderReplayIdentity {
            protocol: ProviderReplayProtocol::OpenAiChatCompletions,
            model: self.model.clone(),
        })
    }

    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    fn request_timeout(&self) -> Option<Duration> {
        Some(self.client.timeout())
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

impl OpenAiCompatibleChatProviderAdapter {
    async fn send_observed(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        let transport = if request.stream {
            ProviderTransport::ChatSse
        } else {
            ProviderTransport::ChatNonStreaming
        };
        let retry_count = request
            .retry_count_override
            .unwrap_or(self.client.retry_count());
        let retry_after_partial =
            request.stream_output_mode == crate::api::ProviderStreamOutputMode::Buffered;
        let recovery_interrupt = request.recovery_interrupt.clone();
        let base_messages = request.messages;
        let mut messages = session_turn_messages_to_chat(base_messages.clone(), &self.model)?;
        let request_has_media = messages_contain_media(&messages);
        let tools = tool_specs_to_chat(request.tools);
        let turn = match self
            .send_with_continuation(
                &request.system_prompt,
                &mut messages,
                &base_messages,
                tools,
                request.max_tokens,
                request.stream,
                retry_count,
                retry_after_partial,
                recovery_interrupt.as_ref(),
                emit,
                observer,
            )
            .await
        {
            Ok(turn) => turn,
            Err(OpenAiCompatibleChatError::RequestPreparation { reason }) => {
                return Err(ProviderRequestPreparationFailure::new(reason).into());
            }
            Err(error) => {
                if matches!(
                    &error,
                    OpenAiCompatibleChatError::Client(ChatCompletionsError::RecoveryInterrupted)
                ) {
                    return Err(SessionTurnInterrupted.into());
                }
                if request.stream && chat_adapter_stream_failure(&error) {
                    return Err(ProviderStreamFailure::new(error.to_string()).into());
                }
                if let OpenAiCompatibleChatError::TerminalFailure { reason } = &error {
                    return Err(ProviderTerminalFailure::new(reason.clone()).into());
                }
                return Err(wrap_media_rejection(error, request_has_media).into());
            }
        };
        if !turn.merged_text.trim().is_empty() {
            emit(ProviderEvent::AssistantMessageCompleted {
                text: turn.merged_text.clone(),
            });
        }
        match provider_response_from_turn(turn, &self.model) {
            Ok(response) => Ok(response),
            Err(OpenAiCompatibleChatError::NoConsumableOutput { reason }) => {
                Err(ProviderNoConsumableOutput::new(transport, reason).into())
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn chat_adapter_stream_failure(error: &OpenAiCompatibleChatError) -> bool {
    matches!(error, OpenAiCompatibleChatError::Client(error) if is_stream_failure(error))
}

struct ContinuedChatTurn {
    message: ChatCompletionMessage,
    finish_reason: Option<ChatFinishReason>,
    merged_text: String,
    replay_messages: Option<Vec<Value>>,
}

fn session_turn_messages_to_chat(
    messages: Vec<SessionTurnMessage>,
    model: &str,
) -> Result<Vec<ChatMessage>, OpenAiCompatibleChatError> {
    let mut out = Vec::new();
    for message in messages {
        if let Some(ProviderReplayState::OpenAiChatCompletions {
            model: replay_model,
            messages,
        }) = message.provider_replay
        {
            if replay_model == model {
                let replay = messages
                    .into_iter()
                    .map(|message| {
                        serde_json::from_value::<ChatMessage>(message).map_err(|error| {
                            OpenAiCompatibleChatError::OutputShape {
                                reason: format!("Chat continuation replay 反序列化失败: {error}"),
                                raw: String::new(),
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                out.extend(replay);
                continue;
            }
        }
        match message.role.as_str() {
            "user" => push_user_message(&mut out, message.content)?,
            "assistant" => out.push(assistant_message_to_chat(message.content)?),
            role => {
                return Err(OpenAiCompatibleChatError::OutputShape {
                    reason: format!("不支持的 canonical role: {role}"),
                    raw: String::new(),
                });
            }
        }
    }
    Ok(out)
}

fn push_user_message(
    out: &mut Vec<ChatMessage>,
    blocks: Vec<SessionTurnContentBlock>,
) -> Result<(), OpenAiCompatibleChatError> {
    let mut parts: Vec<ChatContentPart> = Vec::new();
    let mut has_media = false;
    let mut tool_results = Vec::new();
    for block in blocks {
        match block {
            SessionTurnContentBlock::Text { text }
            | SessionTurnContentBlock::ModelContext { text, .. } => {
                parts.push(ChatContentPart::text(text));
            }
            SessionTurnContentBlock::SkillInstructions { instruction } => parts.push(
                ChatContentPart::text(crate::skill::render_skill_instructions(&instruction)),
            ),
            SessionTurnContentBlock::Image { media_type, data } => {
                has_media = true;
                parts.push(ChatContentPart::image_data_url(&media_type, &data));
            }
            SessionTurnContentBlock::Document {
                media_type,
                data,
                filename,
            } => {
                has_media = true;
                parts.push(ChatContentPart::file_data_url(filename, &media_type, &data));
            }
            SessionTurnContentBlock::ToolResult {
                tool_use_id,
                content,
            } => tool_results.push((tool_use_id, content)),
            SessionTurnContentBlock::ToolUse { .. } => {
                return Err(OpenAiCompatibleChatError::OutputShape {
                    reason: "user message 不允许包含 ToolUse".into(),
                    raw: String::new(),
                });
            }
        }
    }
    // tool_result 必须紧跟 assistant 的 tool_calls；canonical message 中混排的
    // 文本 / 媒体（如 file_read 附带的图片块）转为其后的独立 user message。
    for (tool_use_id, content) in tool_results {
        out.push(ChatMessage::tool(tool_use_id, content));
    }
    let text_is_empty = parts.iter().all(|part| match part {
        ChatContentPart::Text { text } => text.trim().is_empty(),
        _ => false,
    });
    if has_media {
        out.push(ChatMessage::user_parts(parts));
    } else if !text_is_empty {
        // 纯文本沿用字符串 content，兼容不支持 parts 数组的 chat-compatible 网关。
        let content = parts
            .into_iter()
            .filter_map(|part| match part {
                ChatContentPart::Text { text } => Some(text),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        out.push(ChatMessage::user(content));
    }
    Ok(())
}

fn assistant_message_to_chat(
    blocks: Vec<SessionTurnContentBlock>,
) -> Result<ChatMessage, OpenAiCompatibleChatError> {
    let mut text_parts = Vec::new();
    let mut tool_calls = Vec::new();
    for block in blocks {
        match block {
            SessionTurnContentBlock::Text { text } => text_parts.push(text),
            SessionTurnContentBlock::ModelContext { .. } => {
                return Err(OpenAiCompatibleChatError::OutputShape {
                    reason: "assistant message 不允许包含 ModelContext".into(),
                    raw: String::new(),
                });
            }
            SessionTurnContentBlock::SkillInstructions { .. } => {
                return Err(OpenAiCompatibleChatError::OutputShape {
                    reason: "assistant message 不允许包含 SkillInstructions".into(),
                    raw: String::new(),
                });
            }
            SessionTurnContentBlock::Image { media_type, data } => {
                text_parts.push(format!(
                    "[Assistant image omitted: media_type={media_type}, base64_bytes={}]",
                    data.len()
                ));
            }
            SessionTurnContentBlock::Document {
                media_type, data, ..
            } => {
                text_parts.push(format!(
                    "[Assistant document omitted: media_type={media_type}, base64_bytes={}]",
                    data.len()
                ));
            }
            SessionTurnContentBlock::ToolUse { id, name, input } => {
                tool_calls.push(ChatToolCall::function(id, name, input.to_string()));
            }
            SessionTurnContentBlock::ToolResult { .. } => {
                return Err(OpenAiCompatibleChatError::OutputShape {
                    reason: "assistant message 不允许包含 ToolResult".into(),
                    raw: String::new(),
                });
            }
        }
    }
    let content = if text_parts.is_empty() {
        None
    } else {
        Some(text_parts.join("\n"))
    };
    Ok(ChatMessage::assistant(content, tool_calls))
}

fn tool_specs_to_chat(tools: Vec<ToolSpec>) -> Vec<ChatTool> {
    tools
        .into_iter()
        .map(|spec| ChatTool::function(spec.name, spec.description, spec.input_schema))
        .collect()
}

fn messages_contain_media(messages: &[ChatMessage]) -> bool {
    messages.iter().any(|message| {
        matches!(
            &message.content,
            Some(ChatMessageContent::Parts(parts))
                if parts.iter().any(|part| matches!(
                    part,
                    ChatContentPart::ImageUrl { .. } | ChatContentPart::File { .. }
                ))
        )
    })
}

/// 请求携带媒体 parts 时，上游 4xx（鉴权 / 限流除外）大概率是模型不支持多模态：
/// 不支持多模态的网关常报"参数 / 格式不对"之类的误导性文案，这里补上明确提示，
/// 同时在 Display 中保留上游核心错误信息（PRD 要求不静默降级）。
fn wrap_media_rejection(
    error: OpenAiCompatibleChatError,
    request_has_media: bool,
) -> OpenAiCompatibleChatError {
    if !request_has_media {
        return error;
    }
    match error {
        OpenAiCompatibleChatError::Client(ChatCompletionsError::Status { status, body })
            if (400..500).contains(&status) && status != 401 && status != 429 =>
        {
            OpenAiCompatibleChatError::MediaRejected {
                source: ChatCompletionsError::Status {
                    status,
                    body: redact_media_error_body(&body),
                },
            }
        }
        other => other,
    }
}

fn first_choice(
    response: ChatCompletionResponse,
) -> Result<ChatCompletionChoice, OpenAiCompatibleChatError> {
    response
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| OpenAiCompatibleChatError::OutputShape {
            reason: "缺少 choices[0]".into(),
            raw: json!({"choices":[]}).to_string(),
        })
}

fn message_from_response(message: &ChatCompletionMessage) -> ChatMessage {
    ChatMessage::assistant(message.content.clone(), message.tool_calls.clone())
}

fn provider_stop_from_turn(turn: &ContinuedChatTurn) -> ProviderStop {
    if turn.finish_reason == Some(ChatFinishReason::Length) {
        ProviderStop::MaxTokens
    } else if !turn.message.tool_calls.is_empty()
        || matches!(
            turn.finish_reason,
            Some(ChatFinishReason::ToolCalls | ChatFinishReason::FunctionCall)
        )
    {
        ProviderStop::ToolUse
    } else {
        ProviderStop::Done
    }
}

fn provider_response_from_turn(
    turn: ContinuedChatTurn,
    model: &str,
) -> Result<ProviderResponse, OpenAiCompatibleChatError> {
    let stop = provider_stop_from_turn(&turn);
    let assistant_message = assistant_turn_message(turn, model)?;
    let has_tool_use = assistant_message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::ToolUse { .. }));
    if stop == ProviderStop::ToolUse && !has_tool_use {
        return Err(OpenAiCompatibleChatError::NoConsumableOutput {
            reason: "Chat 工具终态没有完整 tool call".into(),
        });
    }
    if stop == ProviderStop::Done && assistant_message.content.is_empty() {
        return Err(OpenAiCompatibleChatError::NoConsumableOutput {
            reason: "Chat 响应没有可消费的 text 或 tool call".into(),
        });
    }
    Ok(ProviderResponse {
        stop,
        assistant_message,
    })
}

fn reject_unsupported_finish_reason(
    finish_reason: &ChatFinishReason,
) -> Result<(), OpenAiCompatibleChatError> {
    match finish_reason {
        ChatFinishReason::Stop
        | ChatFinishReason::ToolCalls
        | ChatFinishReason::Length
        | ChatFinishReason::FunctionCall => Ok(()),
        ChatFinishReason::ContentFilter => Err(OpenAiCompatibleChatError::TerminalFailure {
            reason: "finish_reason=content_filter，拒绝把被过滤输出当作完整 assistant 回合".into(),
        }),
        ChatFinishReason::Other => Err(OpenAiCompatibleChatError::TerminalFailure {
            reason: "未知 finish_reason，拒绝静默当作 Done".into(),
        }),
    }
}

fn require_finish_reason(
    finish_reason: Option<ChatFinishReason>,
) -> Result<ChatFinishReason, OpenAiCompatibleChatError> {
    finish_reason.ok_or_else(|| OpenAiCompatibleChatError::OutputShape {
        reason: "OpenAI-compatible 响应缺少 finish_reason".into(),
        raw: String::new(),
    })
}

fn assistant_turn_message(
    turn: ContinuedChatTurn,
    model: &str,
) -> Result<SessionTurnMessage, OpenAiCompatibleChatError> {
    let ContinuedChatTurn {
        message,
        merged_text,
        replay_messages,
        ..
    } = turn;
    let mut content = Vec::new();
    let ChatCompletionMessage {
        content: message_content,
        tool_calls,
        ..
    } = message;
    if !merged_text.trim().is_empty() {
        content.push(SessionTurnContentBlock::text(merged_text));
    } else if let Some(text) = message_content {
        if !text.trim().is_empty() {
            content.push(SessionTurnContentBlock::text(text));
        }
    }
    for tool_call in tool_calls {
        if tool_call.kind != "function" {
            return Err(OpenAiCompatibleChatError::OutputShape {
                reason: format!("不支持的 tool_call type: {}", tool_call.kind),
                raw: String::new(),
            });
        }
        content.push(SessionTurnContentBlock::ToolUse {
            id: tool_call.id,
            name: tool_call.function.name,
            input: parse_tool_arguments(&tool_call.function.arguments)?,
        });
    }
    let mut message = SessionTurnMessage {
        role: "assistant".into(),
        provider_replay: None,
        content,
    };
    if let Some(messages) = replay_messages {
        message.provider_replay = Some(ProviderReplayState::OpenAiChatCompletions {
            model: model.to_string(),
            messages,
        });
    }
    Ok(message)
}

fn parse_tool_arguments(raw: &str) -> Result<Value, OpenAiCompatibleChatError> {
    if raw.trim().is_empty() {
        return Ok(json!({}));
    }
    let value =
        serde_json::from_str::<Value>(raw).map_err(|e| OpenAiCompatibleChatError::OutputShape {
            reason: format!("tool_call.arguments 不是合法 JSON: {e}"),
            raw: raw.to_string(),
        })?;
    if !value.is_object() {
        return Err(OpenAiCompatibleChatError::OutputShape {
            reason: "tool_call.arguments 必须是 JSON object".into(),
            raw: raw.to_string(),
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::chat_completions::ChatMessageContent;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[derive(Default)]
    struct RecordingRequestObserver {
        requests: Vec<Vec<SessionTurnMessage>>,
    }

    struct CancellingStartedObserver {
        interrupt: ProviderRecoveryInterrupt,
        requests: Vec<Vec<SessionTurnMessage>>,
        started: usize,
    }

    struct CancellingContinuationPreflightObserver {
        interrupt: ProviderRecoveryInterrupt,
        requests: Vec<Vec<SessionTurnMessage>>,
        started: usize,
        abandoned: usize,
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

    #[async_trait]
    impl ProviderRequestObserver for CancellingStartedObserver {
        async fn before_provider_request(
            &mut self,
            messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.requests.push(messages.to_vec());
            Ok(())
        }

        fn provider_request_started(
            &mut self,
            _messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.started += 1;
            self.interrupt.cancel();
            Ok(())
        }
    }

    #[async_trait]
    impl ProviderRequestObserver for CancellingContinuationPreflightObserver {
        async fn before_provider_request(
            &mut self,
            messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.requests.push(messages.to_vec());
            if self.requests.len() == 2 {
                self.interrupt.cancel();
            }
            Ok(())
        }

        fn provider_request_started(
            &mut self,
            _messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            self.started += 1;
            Ok(())
        }

        async fn provider_request_abandoned_before_send(
            &mut self,
            messages: &[SessionTurnMessage],
        ) -> anyhow::Result<()> {
            assert_eq!(Some(messages), self.requests.last().map(Vec::as_slice));
            self.abandoned += 1;
            Ok(())
        }
    }

    fn adapter() -> OpenAiCompatibleChatProviderAdapter {
        adapter_with_reasoning_effort(ReasoningEffort::None)
    }

    fn adapter_with_reasoning_effort(
        reasoning_effort: ReasoningEffort,
    ) -> OpenAiCompatibleChatProviderAdapter {
        OpenAiCompatibleChatProviderAdapter {
            client: ChatCompletionsClient::new(
                "http://127.0.0.1:1".into(),
                "key".into(),
                Duration::from_secs(1),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
            model: "test-model".into(),
            reasoning_effort,
            temperature: None,
        }
    }

    #[test]
    fn history_media_policy_preserves_uncompacted_images_and_documents() {
        assert_eq!(
            adapter().history_media_policy(),
            ProviderHistoryMediaPolicy::Preserve
        );
    }

    #[test]
    fn canonical_tool_use_maps_to_chat_tool_call() {
        let messages = session_turn_messages_to_chat(
            vec![SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![
                    SessionTurnContentBlock::text("先查"),
                    SessionTurnContentBlock::ToolUse {
                        id: "call_1".into(),
                        name: "file_read".into(),
                        input: json!({"path":"a.txt"}),
                    },
                ],
            }],
            "test-model",
        )
        .unwrap();
        let calls = messages[0].tool_calls.as_ref().unwrap();
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].function.name, "file_read");
        assert_eq!(calls[0].function.arguments, r#"{"path":"a.txt"}"#);
    }

    #[test]
    fn canonical_tool_result_maps_to_chat_tool_message() {
        let messages = session_turn_messages_to_chat(
            vec![SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "call_1".into(),
                    content: r#"{"ok":true}"#.into(),
                }],
            }],
            "test-model",
        )
        .unwrap();
        assert_eq!(messages[0].role, "tool");
        assert_eq!(messages[0].tool_call_id.as_deref(), Some("call_1"));
    }

    #[test]
    fn mixed_tool_result_and_media_splits_into_tool_then_user_message() {
        let messages = session_turn_messages_to_chat(
            vec![SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![
                    SessionTurnContentBlock::ToolResult {
                        tool_use_id: "call_1".into(),
                        content: r#"{"ok":true}"#.into(),
                    },
                    SessionTurnContentBlock::text("[file_read attachment] a.png"),
                    SessionTurnContentBlock::image("image/png", "QUJD"),
                ],
            }],
            "test-model",
        )
        .unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "tool");
        assert_eq!(messages[0].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(messages[1].role, "user");
        let Some(ChatMessageContent::Parts(parts)) = &messages[1].content else {
            panic!("媒体混排应产出 content parts");
        };
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn user_image_block_maps_to_image_url_data_url() {
        let messages = session_turn_messages_to_chat(
            vec![SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![
                    SessionTurnContentBlock::text("看这张图"),
                    SessionTurnContentBlock::image("image/png", "QUJD"),
                ],
            }],
            "test-model",
        )
        .unwrap();

        let Some(ChatMessageContent::Parts(parts)) = &messages[0].content else {
            panic!("带图片的 user message 应产出 content parts");
        };
        assert!(matches!(
            &parts[0],
            ChatContentPart::Text { text } if text == "看这张图"
        ));
        assert!(matches!(
            &parts[1],
            ChatContentPart::ImageUrl { image_url } if image_url.url == "data:image/png;base64,QUJD"
        ));
    }

    #[test]
    fn user_document_block_maps_to_file_part_with_filename() {
        let messages = session_turn_messages_to_chat(
            vec![SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::document_named(
                    "application/pdf",
                    "QUJD",
                    "brief.pdf",
                )],
            }],
            "test-model",
        )
        .unwrap();

        let Some(ChatMessageContent::Parts(parts)) = &messages[0].content else {
            panic!("带文档的 user message 应产出 content parts");
        };
        assert!(matches!(
            &parts[0],
            ChatContentPart::File { file }
                if file.filename.as_deref() == Some("brief.pdf")
                    && file.file_data == "data:application/pdf;base64,QUJD"
        ));
    }

    #[test]
    fn pure_text_user_message_keeps_string_content() {
        let messages = session_turn_messages_to_chat(
            vec![SessionTurnMessage::user_text("你好")],
            "test-model",
        )
        .unwrap();

        assert_eq!(
            messages[0].content,
            Some(ChatMessageContent::Text("你好".into()))
        );
    }

    #[test]
    fn model_context_maps_to_stable_chat_user_messages_and_preserves_prefix() {
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
        let first = session_turn_messages_to_chat(prefix.clone(), "test-model").unwrap();
        let mut extended = prefix;
        extended.push(SessionTurnMessage::assistant_text("first answer"));
        extended.push(SessionTurnMessage::user_text("second request"));
        let second = session_turn_messages_to_chat(extended, "test-model").unwrap();
        let first_json = serde_json::to_value(&first).unwrap();
        let second_json = serde_json::to_value(&second).unwrap();
        let first_messages = first_json.as_array().unwrap();
        let second_messages = second_json.as_array().unwrap();

        assert!(second_messages.starts_with(first_messages));
        assert_eq!(first_messages[0]["role"], "user");
        assert_eq!(
            first_messages[0]["content"],
            "<runtime_context>stable</runtime_context>"
        );
        assert_eq!(
            first_messages[1]["content"],
            "<background_processes>empty</background_processes>"
        );
        assert!(!first_json.to_string().contains("sha256-v1"));
    }

    #[test]
    fn responses_replay_is_ignored_when_projecting_to_chat() {
        let messages = session_turn_messages_to_chat(
            vec![
                SessionTurnMessage::assistant_text("canonical text").with_provider_replay(
                    crate::api::ProviderReplayState::OpenAiResponses {
                        model: Some("test-model".into()),
                        items: vec![json!({
                            "type":"reasoning","encrypted_content":"opaque-chat-must-ignore"
                        })],
                    },
                ),
            ],
            "test-model",
        )
        .unwrap();

        assert_eq!(
            messages[0].content,
            Some(ChatMessageContent::Text("canonical text".into()))
        );
        assert!(!serde_json::to_string(&messages)
            .unwrap()
            .contains("opaque-chat-must-ignore"));
    }

    #[tokio::test]
    async fn max_token_continuation_replay_keeps_second_request_as_third_prefix() {
        let bodies = vec![
            json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "partial "},
                    "finish_reason": "length"
                }]
            }),
            json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "answer"},
                    "finish_reason": "stop"
                }]
            }),
            json!({
                "choices": [{
                    "message": {"role": "assistant", "content": "next answer"},
                    "finish_reason": "stop"
                }]
            }),
        ];
        let (endpoint, captured) = spawn_chat_json_sequence(bodies).await;
        let adapter = OpenAiCompatibleChatProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut observer = RecordingRequestObserver::default();
        let first = adapter
            .send_with_request_observer(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("first question")],
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
        assert!(matches!(
            first.assistant_message.provider_replay.as_ref(),
            Some(ProviderReplayState::OpenAiChatCompletions { .. })
        ));
        let first_assistant = first.assistant_message;

        adapter
            .send(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![
                        SessionTurnMessage::user_text("first question"),
                        first_assistant,
                        SessionTurnMessage::user_text("second question"),
                    ],
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
            )
            .await
            .unwrap();

        let requests = captured.await.unwrap();
        assert_eq!(requests.len(), 3);
        let second = requests[1]["messages"].as_array().unwrap();
        let third = requests[2]["messages"].as_array().unwrap();
        assert!(third.starts_with(second));
        assert_eq!(second.last().unwrap()["role"], "user");
        assert_eq!(second.last().unwrap()["content"], CONTINUATION_TRIGGER);
        assert_eq!(observer.requests.len(), 2);
        assert!(observer.requests[1].starts_with(&observer.requests[0]));
        let observed_second = serde_json::to_value(
            session_turn_messages_to_chat(observer.requests[1].clone(), "test-model").unwrap(),
        )
        .unwrap();
        assert_eq!(
            observed_second.as_array().unwrap(),
            &second[1..],
            "observer 上报的 neutral history 必须映射为同一份 Chat messages（除 system）"
        );
    }

    #[tokio::test]
    async fn safe_steer_after_send_keeps_successful_length_response_without_continuation() {
        let (endpoint, captured) = spawn_chat_json_sequence(vec![json!({
            "choices": [{
                "message": {"role": "assistant", "content": "partial-answer"},
                "finish_reason": "length"
            }]
        })])
        .await;
        let adapter = OpenAiCompatibleChatProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let interrupt = ProviderRecoveryInterrupt::new();
        let mut observer = CancellingStartedObserver {
            interrupt: interrupt.clone(),
            requests: Vec::new(),
            started: 0,
        };

        let response = adapter
            .send_with_request_observer(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 32,
                    stream: false,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: None,
                    runtime_fallback_scope: None,
                    recovery_interrupt: Some(interrupt),
                    retry_count_override: None,
                },
                &mut |_| {},
                &mut observer,
            )
            .await
            .unwrap();

        assert_eq!(response.stop, ProviderStop::Done);
        assert!(matches!(
            response.assistant_message.content.as_slice(),
            [SessionTurnContentBlock::Text { text }] if text == "partial-answer"
        ));
        assert_eq!(observer.started, 1);
        assert_eq!(observer.requests.len(), 1);
        assert_eq!(captured.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn safe_steer_during_continuation_wal_keeps_chat_partial() {
        let (endpoint, captured) = spawn_chat_json_sequence(vec![json!({
            "choices": [{
                "message": {"role": "assistant", "content": "partial-answer"},
                "finish_reason": "length"
            }]
        })])
        .await;
        let adapter = OpenAiCompatibleChatProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let interrupt = ProviderRecoveryInterrupt::new();
        let mut observer = CancellingContinuationPreflightObserver {
            interrupt: interrupt.clone(),
            requests: Vec::new(),
            started: 0,
            abandoned: 0,
        };

        let response = adapter
            .send_with_request_observer(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 32,
                    stream: false,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: None,
                    runtime_fallback_scope: None,
                    recovery_interrupt: Some(interrupt.clone()),
                    retry_count_override: None,
                },
                &mut |_| {},
                &mut observer,
            )
            .await
            .unwrap();

        assert_eq!(response.stop, ProviderStop::Done);
        assert!(matches!(
            response.assistant_message.content.as_slice(),
            [SessionTurnContentBlock::Text { text }] if text == "partial-answer"
        ));
        assert!(interrupt.should_preserve_successful_response());
        assert_eq!(observer.requests.len(), 2);
        assert_eq!(observer.started, 1);
        assert_eq!(observer.abandoned, 1);
        assert_eq!(captured.await.unwrap().len(), 1);
    }

    #[test]
    fn chat_tool_call_maps_to_canonical_tool_use() {
        let turn = ContinuedChatTurn {
            message: ChatCompletionMessage {
                role: Some("assistant".into()),
                content: Some("准备调用".into()),
                tool_calls: vec![ChatToolCall::function(
                    "call_1",
                    "workspace_read",
                    r#"{"path":"note.md"}"#,
                )],
            },
            finish_reason: Some(ChatFinishReason::ToolCalls),
            merged_text: "准备调用".into(),
            replay_messages: None,
        };
        let message = assistant_turn_message(turn, "test-model").unwrap();
        assert_eq!(message.content.len(), 2);
        assert!(matches!(
            &message.content[1],
            SessionTurnContentBlock::ToolUse { name, .. } if name == "workspace_read"
        ));
    }

    #[test]
    fn request_uses_chat_message_shape() {
        let req = adapter().request_for("system", Vec::new(), Vec::new(), 128, false);
        assert_eq!(req.model, "test-model");
        assert_eq!(req.messages[0].role, "system");
    }

    #[test]
    fn streaming_request_includes_usage_options() {
        let req = adapter().request_for("system", Vec::new(), Vec::new(), 128, true);

        assert!(req.stream);
        assert_eq!(
            req.stream_options
                .as_ref()
                .map(|options| options.include_usage),
            Some(true)
        );
    }

    #[test]
    fn none_reasoning_effort_is_omitted_from_request_body() {
        let req = adapter().request_for("system", Vec::new(), Vec::new(), 128, false);
        let body = serde_json::to_value(req).unwrap();

        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn configured_temperature_is_sent_without_enabling_reasoning() {
        let adapter = adapter().with_temperature(0.0);
        let req = adapter.request_for("system", Vec::new(), Vec::new(), 128, true);
        let body = serde_json::to_value(req).unwrap();

        assert_eq!(body.get("temperature"), Some(&json!(0.0)));
        assert!(body.get("reasoning_effort").is_none());
    }

    #[test]
    fn configured_reasoning_effort_is_sent_for_streaming_and_non_streaming_requests() {
        let adapter = adapter_with_reasoning_effort(ReasoningEffort::High);

        for stream in [false, true] {
            let req = adapter.request_for("system", Vec::new(), Vec::new(), 128, stream);
            let body = serde_json::to_value(req).unwrap();
            assert_eq!(body.get("reasoning_effort"), Some(&json!("high")));
        }
    }

    #[test]
    fn provider_stop_length_is_max_tokens() {
        let turn = ContinuedChatTurn {
            message: ChatCompletionMessage {
                role: Some("assistant".into()),
                content: Some("cut".into()),
                tool_calls: Vec::new(),
            },
            finish_reason: Some(ChatFinishReason::Length),
            merged_text: "cut".into(),
            replay_messages: None,
        };
        assert_eq!(provider_stop_from_turn(&turn), ProviderStop::MaxTokens);
    }

    #[test]
    fn content_filter_finish_reason_is_rejected() {
        let err = reject_unsupported_finish_reason(&ChatFinishReason::ContentFilter).unwrap_err();

        assert!(matches!(
            err,
            OpenAiCompatibleChatError::TerminalFailure { .. }
        ));
        assert!(err.to_string().contains("content_filter"));
    }

    #[test]
    fn chat_stream_failure_is_exposed_to_provider_turn_loop() {
        let error = OpenAiCompatibleChatError::Client(ChatCompletionsError::StreamFailure {
            reason: "missing finish reason".into(),
            raw: String::new(),
        });

        assert!(chat_adapter_stream_failure(&error));
        assert!(!chat_adapter_stream_failure(
            &OpenAiCompatibleChatError::OutputShape {
                reason: "non-stream shape".into(),
                raw: String::new(),
            }
        ));
    }

    #[test]
    fn completed_empty_chat_turn_has_no_consumable_content() {
        let turn = ContinuedChatTurn {
            message: ChatCompletionMessage {
                role: Some("assistant".into()),
                content: None,
                tool_calls: Vec::new(),
            },
            finish_reason: Some(ChatFinishReason::Stop),
            merged_text: String::new(),
            replay_messages: None,
        };

        let error = provider_response_from_turn(turn, "test-model").unwrap_err();

        assert!(matches!(
            error,
            OpenAiCompatibleChatError::NoConsumableOutput { .. }
        ));
    }

    #[test]
    fn empty_chat_tool_terminal_has_no_consumable_content() {
        let turn = ContinuedChatTurn {
            message: ChatCompletionMessage {
                role: Some("assistant".into()),
                content: None,
                tool_calls: Vec::new(),
            },
            finish_reason: Some(ChatFinishReason::ToolCalls),
            merged_text: String::new(),
            replay_messages: None,
        };

        let error = provider_response_from_turn(turn, "test-model").unwrap_err();

        assert!(matches!(
            error,
            OpenAiCompatibleChatError::NoConsumableOutput { .. }
        ));
    }

    #[test]
    fn text_only_chat_tool_terminal_has_no_consumable_content() {
        let turn = ContinuedChatTurn {
            message: ChatCompletionMessage {
                role: Some("assistant".into()),
                content: Some("我来查询".into()),
                tool_calls: Vec::new(),
            },
            finish_reason: Some(ChatFinishReason::ToolCalls),
            merged_text: "我来查询".into(),
            replay_messages: None,
        };

        let error = provider_response_from_turn(turn, "test-model").unwrap_err();

        assert!(matches!(
            error,
            OpenAiCompatibleChatError::NoConsumableOutput { .. }
        ));
    }

    #[test]
    fn missing_non_streaming_finish_reason_is_rejected() {
        let error = require_finish_reason(None).unwrap_err();

        assert!(error.to_string().contains("finish_reason"));
    }

    #[test]
    fn media_request_4xx_wraps_hint_and_preserves_upstream_body() {
        let error = wrap_media_rejection(
            OpenAiCompatibleChatError::Client(ChatCompletionsError::Status {
                status: 400,
                body: "1210: 文件格式不正确".into(),
            }),
            true,
        );
        let text = error.to_string();
        assert!(text.contains("可能不支持图片 / PDF 附件"));
        // 上游核心错误信息必须保留
        assert!(text.contains("1210: 文件格式不正确"));
        assert!(text.contains("HTTP 400"));
    }

    #[test]
    fn media_request_4xx_redacts_echoed_data_url_payload() {
        let error = wrap_media_rejection(
            OpenAiCompatibleChatError::Client(ChatCompletionsError::Status {
                status: 400,
                body: format!("bad data:image/png;base64,{}", "A".repeat(300)),
            }),
            true,
        );
        let text = error.to_string();
        assert!(text.contains("data:image/png;base64,[redacted media payload]"));
        assert!(!text.contains(&"A".repeat(300)));
    }

    #[test]
    fn media_rejection_hint_skips_non_media_auth_and_rate_limit_errors() {
        // 请求不带媒体：原样透传
        let error = wrap_media_rejection(
            OpenAiCompatibleChatError::Client(ChatCompletionsError::Status {
                status: 400,
                body: "bad request".into(),
            }),
            false,
        );
        assert!(!error.to_string().contains("可能不支持图片"));
        // 401 / 429 与多模态无关：原样透传
        for status in [401u16, 429] {
            let error = wrap_media_rejection(
                OpenAiCompatibleChatError::Client(ChatCompletionsError::Status {
                    status,
                    body: "x".into(),
                }),
                true,
            );
            assert!(!error.to_string().contains("可能不支持图片"));
        }
    }

    #[test]
    fn messages_contain_media_detects_image_and_file_parts() {
        assert!(messages_contain_media(&[ChatMessage::user_parts(vec![
            ChatContentPart::image_data_url("image/png", "QUJD"),
        ])]));
        assert!(messages_contain_media(&[ChatMessage::user_parts(vec![
            ChatContentPart::file_data_url(Some("a.pdf".into()), "application/pdf", "QUJD"),
        ])]));
        assert!(!messages_contain_media(&[
            ChatMessage::user("纯文本"),
            ChatMessage::user_parts(vec![ChatContentPart::text("parts 里只有文本")]),
        ]));
    }

    async fn spawn_chat_json_sequence(
        bodies: Vec<Value>,
    ) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(bodies.len());
            for body in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_chat_http_request(&mut socket).await;
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                let body_start = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                requests.push(serde_json::from_slice(&request[body_start..]).unwrap());
            }
            requests
        });
        (format!("http://{address}/v1"), handle)
    }

    async fn read_chat_http_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end + 4]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
        request
    }
}
