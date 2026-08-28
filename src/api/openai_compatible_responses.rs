//! OpenAI-compatible Responses provider adapter。
//!
//! 本模块负责 canonical session message 与 Responses input/output items 互转；
//! HTTP JSON/SSE、底层 retry 与终态校验由 `responses` 模块负责。

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use super::context_usage_from_openai_usage;
use super::continuation::{
    append_with_overlap_dedupe, CONTINUATION_TRIGGER, MAX_CONTINUATION_TURNS,
};
use super::provider::{
    NoopProviderRequestObserver, ProviderAdapter, ProviderEvent, ProviderHistoryMediaPolicy,
    ProviderNoConsumableOutput, ProviderRecoveryInterrupt, ProviderReplayIdentity,
    ProviderReplayProtocol, ProviderRequest, ProviderRequestObserver,
    ProviderRequestPreparationFailure, ProviderRequestTooLarge, ProviderResponse,
    ProviderRuntimeChainId, ProviderStop, ProviderStreamFailure, ProviderTerminalFailure,
    ProviderTransport, ToolSpec,
};
use super::redact_media_error_body;
use super::responses::{
    is_stream_recovery_failure, ResponsesClient, ResponsesError, ResponsesReasoning,
    ResponsesRequest, ResponsesStreamEvent, ResponsesTerminal, ResponsesTool,
};
use super::types::{ProviderReplayState, SessionTurnContentBlock, SessionTurnMessage};
use super::SessionTurnInterrupted;
use crate::config::ReasoningEffort;

// 旧 session 的 Document 可能没有 filename；Responses 内联 file_data 仍需要
// 一个可识别的文件名，因此使用不包含本地路径或用户信息的中性 PDF 名称。
const FALLBACK_DOCUMENT_FILENAME: &str = "attachment.pdf";

#[derive(Debug, thiserror::Error)]
pub enum OpenAiCompatibleResponsesError {
    #[error(transparent)]
    Client(#[from] ResponsesError),
    #[error("Responses 输出不符合预期: {reason}")]
    OutputShape { reason: String },
    #[error("Responses 没有可消费输出: {reason}")]
    NoConsumableOutput { reason: String },
    #[error("准备 Responses continuation request 失败: {reason}")]
    RequestPreparation { reason: String },
    #[error(
        "当前模型可能不支持图片 / PDF 附件输入，请确认模型多模态能力或移除附件后重试。上游原始错误: {source}"
    )]
    MediaRejected {
        #[source]
        source: ResponsesError,
    },
}

pub struct OpenAiCompatibleResponsesProviderAdapter {
    client: ResponsesClient,
    model: String,
    reasoning_effort: ReasoningEffort,
    include_reasoning_replay: bool,
    temperature: Option<f64>,
    top_p: Option<f64>,
}

impl OpenAiCompatibleResponsesProviderAdapter {
    #[allow(
        clippy::too_many_arguments,
        reason = "provider client 构造参数与其他 LLM adapter 保持一致"
    )]
    pub fn new(
        api_key: String,
        endpoint: String,
        model: String,
        timeout: Duration,
        retry_count: u32,
        retry_base_delay: Duration,
        retry_max_delay: Duration,
    ) -> Result<Self, OpenAiCompatibleResponsesError> {
        Ok(Self {
            client: ResponsesClient::new(
                endpoint,
                api_key,
                timeout,
                retry_count,
                retry_base_delay,
                retry_max_delay,
            )?,
            model,
            reasoning_effort: ReasoningEffort::None,
            include_reasoning_replay: true,
            temperature: None,
            top_p: None,
        })
    }

    /// 设置 Responses `reasoning.effort`；`none` 会省略整个 reasoning 字段。
    pub fn with_reasoning_effort(mut self, reasoning_effort: ReasoningEffort) -> Self {
        self.reasoning_effort = reasoning_effort;
        self
    }

    pub(crate) fn with_reasoning_replay(mut self, enabled: bool) -> Self {
        self.include_reasoning_replay = enabled;
        self
    }

    /// 设置 Agent 请求的可选采样参数；`None` 会在序列化时省略。
    pub(crate) fn with_sampling_parameters(
        mut self,
        temperature: Option<f64>,
        top_p: Option<f64>,
    ) -> Self {
        self.temperature = temperature;
        self.top_p = top_p;
        self
    }

    pub fn with_websockets(
        mut self,
        enabled: bool,
        pool_capacity: usize,
    ) -> Result<Self, OpenAiCompatibleResponsesError> {
        if enabled {
            self.client = self.client.with_websockets(pool_capacity)?;
        }
        Ok(self)
    }

    fn request_for(
        &self,
        system_prompt: &str,
        input: Vec<Value>,
        tools: Vec<ResponsesTool>,
        max_tokens: u32,
        stream: bool,
    ) -> ResponsesRequest {
        ResponsesRequest {
            model: self.model.clone(),
            instructions: system_prompt.to_string(),
            input,
            tools,
            max_output_tokens: max_tokens,
            stream,
            store: false,
            include: self
                .include_reasoning_replay
                .then(|| vec!["reasoning.encrypted_content".into()]),
            reasoning: reasoning_effort_name(self.reasoning_effort).map(|effort| {
                ResponsesReasoning {
                    effort: effort.to_string(),
                }
            }),
            temperature: self.temperature,
            top_p: self.top_p,
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Responses continuation 需显式携带 input/replay、stream 与 retry 策略"
    )]
    async fn send_with_continuation(
        &self,
        system_prompt: &str,
        mut input: Vec<Value>,
        base_messages: &[SessionTurnMessage],
        tools: Vec<ResponsesTool>,
        max_tokens: u32,
        stream: bool,
        retry_count: u32,
        allow_continuation: bool,
        runtime_chain_id: Option<ProviderRuntimeChainId>,
        runtime_fallback_scope: Option<&crate::api::ProviderRuntimeFallbackScope>,
        retry_after_partial: bool,
        recovery_interrupt: Option<&ProviderRecoveryInterrupt>,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> Result<ContinuedResponsesTurn, OpenAiCompatibleResponsesError> {
        let mut merged_text = String::new();
        let mut replay_items = Vec::new();
        let mut last_function_calls = Vec::new();
        let mut last_terminal = ResponsesTerminal::Completed;
        let mut last_transport = if stream {
            ProviderTransport::ResponsesSse
        } else {
            ProviderTransport::ResponsesNonStreaming
        };
        let mut provider_messages = base_messages.to_vec();
        let max_continuation_turns = if allow_continuation {
            MAX_CONTINUATION_TURNS
        } else {
            0
        };

        for round in 0..=max_continuation_turns {
            if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
                if last_terminal == ResponsesTerminal::MaxOutputTokens
                    && last_function_calls.is_empty()
                {
                    discard_pending_responses_continuation(
                        &mut input,
                        &mut replay_items,
                        &mut provider_messages,
                    )?;
                    recovery_interrupt
                        .expect("cancelled recovery interrupt must be present")
                        .preserve_successful_response();
                    last_terminal = ResponsesTerminal::Completed;
                    break;
                }
                return Err(ResponsesError::RecoveryInterrupted.into());
            }
            observer
                .before_provider_request(&provider_messages)
                .await
                .map_err(|error| OpenAiCompatibleResponsesError::RequestPreparation {
                    reason: format!("{error:#}"),
                })?;
            if recovery_interrupt.is_some_and(ProviderRecoveryInterrupt::is_cancelled) {
                if last_terminal == ResponsesTerminal::MaxOutputTokens
                    && last_function_calls.is_empty()
                {
                    observer
                        .provider_request_abandoned_before_send(&provider_messages)
                        .await
                        .map_err(|error| OpenAiCompatibleResponsesError::RequestPreparation {
                            reason: format!("{error:#}"),
                        })?;
                    discard_pending_responses_continuation(
                        &mut input,
                        &mut replay_items,
                        &mut provider_messages,
                    )?;
                    recovery_interrupt
                        .expect("cancelled recovery interrupt must be present")
                        .preserve_successful_response();
                    last_terminal = ResponsesTerminal::Completed;
                    break;
                }
                return Err(ResponsesError::RecoveryInterrupted.into());
            }
            let request = self.request_for(
                system_prompt,
                input.clone(),
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
                        .map_err(|error| ResponsesError::RequestPreparation {
                            reason: format!("{error:#}"),
                        })?;
                    request_start_recorded = true;
                    Ok(())
                };
                if stream {
                    let mut responses_emit = |event| match event {
                        ResponsesStreamEvent::TextDelta { text } => {
                            emit(ProviderEvent::AssistantTextDelta { text });
                        }
                    };
                    self.client
                        .send_with_retry_count_for_runtime_scope_and_transport_and_start_hook(
                            &request,
                            retry_count,
                            runtime_chain_id,
                            runtime_fallback_scope,
                            retry_after_partial,
                            recovery_interrupt,
                            &mut responses_emit,
                            &mut request_started,
                        )
                        .await
                } else {
                    let mut noop = |_event: ResponsesStreamEvent| {};
                    self.client
                        .send_with_retry_count_for_runtime_scope_and_transport_and_start_hook(
                            &request,
                            retry_count,
                            None,
                            None,
                            false,
                            recovery_interrupt,
                            &mut noop,
                            &mut request_started,
                        )
                        .await
                }
            };
            let (response, transport) = match response_result {
                Ok(response) => response,
                Err(ResponsesError::RecoveryInterrupted)
                    if !request_start_recorded
                        && last_terminal == ResponsesTerminal::MaxOutputTokens
                        && last_function_calls.is_empty() =>
                {
                    observer
                        .provider_request_abandoned_before_send(&provider_messages)
                        .await
                        .map_err(|error| OpenAiCompatibleResponsesError::RequestPreparation {
                            reason: format!("{error:#}"),
                        })?;
                    discard_pending_responses_continuation(
                        &mut input,
                        &mut replay_items,
                        &mut provider_messages,
                    )?;
                    recovery_interrupt
                        .expect("RecoveryInterrupted requires a recovery interrupt")
                        .preserve_successful_response();
                    last_terminal = ResponsesTerminal::Completed;
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            last_transport = transport;
            if let Some(usage) = response
                .usage
                .as_ref()
                .and_then(context_usage_from_openai_usage)
            {
                emit(ProviderEvent::ContextUsageUpdated { usage });
            }
            let round_text = response.output_text.clone();
            let mut round_replay_items = response.output_items.clone();
            append_with_overlap_dedupe(&mut merged_text, &round_text);
            input.extend(round_replay_items.iter().cloned());
            replay_items.extend(round_replay_items.iter().cloned());
            last_terminal = response.terminal;
            last_function_calls = response.function_calls;

            if response.terminal != ResponsesTerminal::MaxOutputTokens {
                break;
            }
            if !last_function_calls.is_empty() {
                break;
            }
            if let Some(interrupt) = recovery_interrupt.filter(|interrupt| interrupt.is_cancelled())
            {
                interrupt.preserve_successful_response();
                last_terminal = ResponsesTerminal::Completed;
                break;
            }
            if round == max_continuation_turns {
                break;
            }
            let continuation = user_text_item(CONTINUATION_TRIGGER);
            input.push(continuation.clone());
            replay_items.push(continuation.clone());
            round_replay_items.push(continuation);
            let content = if round_text.trim().is_empty() {
                Vec::new()
            } else {
                vec![SessionTurnContentBlock::text(round_text)]
            };
            provider_messages.push(SessionTurnMessage {
                role: "assistant".into(),
                content,
                provider_replay: Some(ProviderReplayState::OpenAiResponses {
                    model: Some(self.model.clone()),
                    items: round_replay_items,
                }),
            });
        }

        Ok(ContinuedResponsesTurn {
            merged_text,
            replay_items,
            function_calls: last_function_calls,
            terminal: last_terminal,
            transport: last_transport,
        })
    }
}

fn discard_pending_responses_continuation(
    input: &mut Vec<Value>,
    replay_items: &mut Vec<Value>,
    provider_messages: &mut Vec<SessionTurnMessage>,
) -> Result<(), OpenAiCompatibleResponsesError> {
    input
        .pop()
        .ok_or_else(|| OpenAiCompatibleResponsesError::OutputShape {
            reason: "safe steer 收束时缺少未发送的 Responses continuation".into(),
        })?;
    replay_items
        .pop()
        .ok_or_else(|| OpenAiCompatibleResponsesError::OutputShape {
            reason: "safe steer 收束时缺少未发送的 Responses replay item".into(),
        })?;
    provider_messages
        .pop()
        .ok_or_else(|| OpenAiCompatibleResponsesError::OutputShape {
            reason: "safe steer 收束时缺少未发送的 Responses neutral replay".into(),
        })?;
    Ok(())
}

#[async_trait]
impl ProviderAdapter for OpenAiCompatibleResponsesProviderAdapter {
    fn history_media_policy(&self) -> ProviderHistoryMediaPolicy {
        ProviderHistoryMediaPolicy::Preserve
    }

    fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        Some(ProviderReplayIdentity {
            protocol: ProviderReplayProtocol::OpenAiResponses,
            model: self.model.clone(),
        })
    }

    fn emit_preflight_context_estimate(&self) -> bool {
        false
    }

    fn request_timeout(&self) -> Option<Duration> {
        // WebSocket transport 必须先把 request timeout 归类为 retry/sticky/SSE
        // outcome；若外层使用同长 timer，会更早取消内部 future。启用 WS 时
        // 每个真实 WS/HTTP request 由 Responses client 自己使用同一 timeout。
        (!self.client.websockets_enabled()).then(|| self.client.timeout())
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

    async fn discard_runtime_chain(&self, chain_id: ProviderRuntimeChainId) {
        self.client.discard_runtime_chain(chain_id).await;
    }
}

impl OpenAiCompatibleResponsesProviderAdapter {
    async fn send_observed(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
        observer: &mut (dyn ProviderRequestObserver + Send),
    ) -> anyhow::Result<ProviderResponse> {
        let retry_count = request
            .retry_count_override
            .unwrap_or(self.client.retry_count());
        let retry_after_partial =
            request.stream_output_mode == crate::api::ProviderStreamOutputMode::Buffered;
        let allow_continuation = request.allow_continuation;
        let base_messages = request.messages;
        let input = session_turn_messages_to_responses(base_messages.clone(), &self.model)?;
        let recovery_interrupt = request.recovery_interrupt.clone();
        let request_has_media = input.has_media;
        let turn = match self
            .send_with_continuation(
                &request.system_prompt,
                input.items,
                &base_messages,
                tool_specs_to_responses(request.tools),
                request.max_tokens,
                request.stream,
                retry_count,
                allow_continuation,
                request.runtime_chain_id,
                request.runtime_fallback_scope.as_ref(),
                retry_after_partial,
                recovery_interrupt.as_ref(),
                emit,
                observer,
            )
            .await
        {
            Ok(turn) => turn,
            Err(OpenAiCompatibleResponsesError::RequestPreparation { reason }) => {
                return Err(ProviderRequestPreparationFailure::new(reason).into());
            }
            Err(error) => {
                if matches!(
                    &error,
                    OpenAiCompatibleResponsesError::Client(ResponsesError::RecoveryInterrupted)
                ) {
                    return Err(SessionTurnInterrupted.into());
                }
                if let Some(error) = classify_request_too_large(&error) {
                    return Err(error.into());
                }
                if request.stream && responses_adapter_stream_failure(&error) {
                    return Err(ProviderStreamFailure::new(error.to_string()).into());
                }
                let error = wrap_media_rejection(error, request_has_media);
                if responses_adapter_terminal_failure(&error) {
                    return Err(ProviderTerminalFailure::new(error.to_string()).into());
                }
                return Err(error.into());
            }
        };
        let transport = turn.transport;
        let response = match provider_response_from_turn(turn, &self.model) {
            Ok(response) => response,
            Err(OpenAiCompatibleResponsesError::NoConsumableOutput { reason }) => {
                return Err(ProviderNoConsumableOutput::new(transport, reason).into());
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(text) =
            response
                .assistant_message
                .content
                .iter()
                .find_map(|block| match block {
                    SessionTurnContentBlock::Text { text } => Some(text),
                    _ => None,
                })
        {
            emit(ProviderEvent::AssistantMessageCompleted { text: text.clone() });
        }
        Ok(response)
    }
}

fn responses_adapter_stream_failure(error: &OpenAiCompatibleResponsesError) -> bool {
    matches!(error, OpenAiCompatibleResponsesError::Client(error) if is_stream_recovery_failure(error))
}

fn responses_adapter_terminal_failure(error: &OpenAiCompatibleResponsesError) -> bool {
    match error {
        OpenAiCompatibleResponsesError::Client(
            ResponsesError::Auth(_)
            | ResponsesError::InvalidEndpoint(_)
            | ResponsesError::Failed { .. }
            | ResponsesError::Incomplete { .. }
            | ResponsesError::RequestPreparation { .. },
        )
        | OpenAiCompatibleResponsesError::MediaRejected { .. }
        | OpenAiCompatibleResponsesError::RequestPreparation { .. } => true,
        OpenAiCompatibleResponsesError::Client(ResponsesError::Status { status, .. }) => {
            *status != 429 && *status < 500
        }
        OpenAiCompatibleResponsesError::Client(
            ResponsesError::Http(_)
            | ResponsesError::StreamFailure { .. }
            | ResponsesError::ResponseJson(_)
            | ResponsesError::OutputShape { .. }
            | ResponsesError::RecoveryInterrupted,
        )
        | OpenAiCompatibleResponsesError::OutputShape { .. }
        | OpenAiCompatibleResponsesError::NoConsumableOutput { .. } => false,
    }
}

struct ResponsesInput {
    items: Vec<Value>,
    has_media: bool,
}

struct ContinuedResponsesTurn {
    merged_text: String,
    replay_items: Vec<Value>,
    function_calls: Vec<super::responses::ResponsesFunctionCall>,
    terminal: ResponsesTerminal,
    transport: ProviderTransport,
}

fn session_turn_messages_to_responses(
    messages: Vec<SessionTurnMessage>,
    model: &str,
) -> Result<ResponsesInput, OpenAiCompatibleResponsesError> {
    let mut items = Vec::new();
    let mut has_media = false;
    for message in messages {
        if let Some(ProviderReplayState::OpenAiResponses {
            model: Some(replay_model),
            items: replay,
        }) = message.provider_replay
        {
            if replay_model == model {
                items.extend(replay);
                continue;
            }
        }
        match message.role.as_str() {
            "user" => push_user_items(&mut items, message.content, &mut has_media)?,
            "assistant" => push_assistant_items(&mut items, message.content)?,
            role => {
                return Err(output_shape(format!("不支持的 canonical role: {role}")));
            }
        }
    }
    Ok(ResponsesInput { items, has_media })
}

fn push_user_items(
    items: &mut Vec<Value>,
    blocks: Vec<SessionTurnContentBlock>,
    has_media: &mut bool,
) -> Result<(), OpenAiCompatibleResponsesError> {
    let mut content = Vec::new();
    let mut tool_outputs = Vec::new();
    for block in blocks {
        match block {
            SessionTurnContentBlock::Text { text }
            | SessionTurnContentBlock::ModelContext { text, .. } => {
                if !text.trim().is_empty() {
                    content.push(json!({"type":"input_text","text":text}));
                }
            }
            SessionTurnContentBlock::SkillInstructions { instruction } => {
                content.push(json!({
                    "type":"input_text",
                    "text":crate::skill::render_skill_instructions(&instruction),
                }));
            }
            SessionTurnContentBlock::Image { media_type, data } => {
                *has_media = true;
                content.push(json!({
                    "type":"input_image",
                    "image_url":format!("data:{media_type};base64,{data}"),
                }));
            }
            SessionTurnContentBlock::Document {
                media_type,
                data,
                filename,
            } => {
                *has_media = true;
                content.push(json!({
                    "type":"input_file",
                    "filename":filename.unwrap_or_else(|| FALLBACK_DOCUMENT_FILENAME.into()),
                    "file_data":format!("data:{media_type};base64,{data}"),
                }));
            }
            SessionTurnContentBlock::ToolResult {
                tool_use_id,
                content,
            } => tool_outputs.push(json!({
                "type":"function_call_output",
                "call_id":tool_use_id,
                "output":content,
            })),
            SessionTurnContentBlock::ToolUse { .. } => {
                return Err(output_shape("user message 不允许包含 ToolUse"));
            }
        }
    }
    // function_call_output 必须先回灌；工具附带的文本/媒体形成其后的 user message。
    items.extend(tool_outputs);
    if !content.is_empty() {
        items.push(json!({"type":"message","role":"user","content":content}));
    }
    Ok(())
}

fn push_assistant_items(
    items: &mut Vec<Value>,
    blocks: Vec<SessionTurnContentBlock>,
) -> Result<(), OpenAiCompatibleResponsesError> {
    let mut text_parts = Vec::new();
    for block in blocks {
        match block {
            SessionTurnContentBlock::Text { text } => text_parts.push(text),
            SessionTurnContentBlock::ModelContext { .. } => {
                return Err(output_shape("assistant message 不允许包含 ModelContext"));
            }
            SessionTurnContentBlock::Image { media_type, data } => text_parts.push(format!(
                "[Assistant image omitted: media_type={media_type}, base64_bytes={}]",
                data.len()
            )),
            SessionTurnContentBlock::Document {
                media_type, data, ..
            } => text_parts.push(format!(
                "[Assistant document omitted: media_type={media_type}, base64_bytes={}]",
                data.len()
            )),
            SessionTurnContentBlock::ToolUse { id, name, input } => {
                flush_assistant_text(items, &mut text_parts);
                if !input.is_object() {
                    return Err(output_shape("ToolUse.input 必须是 JSON object"));
                }
                items.push(json!({
                    "type":"function_call",
                    "call_id":id,
                    "name":name,
                    "arguments":input.to_string(),
                }));
            }
            SessionTurnContentBlock::SkillInstructions { .. } => {
                return Err(output_shape(
                    "assistant message 不允许包含 SkillInstructions",
                ));
            }
            SessionTurnContentBlock::ToolResult { .. } => {
                return Err(output_shape("assistant message 不允许包含 ToolResult"));
            }
        }
    }
    flush_assistant_text(items, &mut text_parts);
    Ok(())
}

fn flush_assistant_text(items: &mut Vec<Value>, text_parts: &mut Vec<String>) {
    if text_parts.is_empty() {
        return;
    }
    let text = std::mem::take(text_parts).join("\n");
    if !text.trim().is_empty() {
        items.push(json!({
            "type":"message",
            "role":"assistant",
            "content":[{"type":"output_text","text":text}],
        }));
    }
}

fn user_text_item(text: &str) -> Value {
    json!({
        "type":"message",
        "role":"user",
        "content":[{"type":"input_text","text":text}],
    })
}

fn tool_specs_to_responses(tools: Vec<ToolSpec>) -> Vec<ResponsesTool> {
    tools
        .into_iter()
        .map(|spec| ResponsesTool {
            kind: "function".into(),
            name: spec.name,
            description: spec.description,
            parameters: spec.input_schema,
            strict: false,
        })
        .collect()
}

fn provider_response_from_turn(
    turn: ContinuedResponsesTurn,
    model: &str,
) -> Result<ProviderResponse, OpenAiCompatibleResponsesError> {
    let mut content = Vec::new();
    if !turn.merged_text.trim().is_empty() {
        content.push(SessionTurnContentBlock::text(turn.merged_text));
    }
    for call in turn.function_calls {
        content.push(SessionTurnContentBlock::ToolUse {
            id: call.call_id,
            name: call.name,
            input: parse_tool_arguments(&call.arguments)?,
        });
    }
    if content.is_empty() {
        let item_types = replay_item_types(&turn.replay_items);
        if turn.terminal == ResponsesTerminal::Completed {
            return Err(OpenAiCompatibleResponsesError::NoConsumableOutput {
                reason: format!(
                    "Responses 响应没有可消费的 output_text 或 function_call；output item types={item_types}"
                ),
            });
        }
        return Err(output_shape(format!(
            "Responses token-limit 响应没有可继续的 output_text 或 function_call；output item types={item_types}"
        )));
    }
    let stop = match turn.terminal {
        ResponsesTerminal::MaxOutputTokens => ProviderStop::MaxTokens,
        ResponsesTerminal::Completed if content.iter().any(is_tool_use) => ProviderStop::ToolUse,
        ResponsesTerminal::Completed => ProviderStop::Done,
    };
    Ok(ProviderResponse {
        assistant_message: SessionTurnMessage {
            role: "assistant".into(),
            content,
            provider_replay: Some(ProviderReplayState::OpenAiResponses {
                model: Some(model.to_string()),
                items: turn.replay_items,
            }),
        },
        stop,
    })
}

fn is_tool_use(block: &SessionTurnContentBlock) -> bool {
    matches!(block, SessionTurnContentBlock::ToolUse { .. })
}

fn parse_tool_arguments(raw: &str) -> Result<Value, OpenAiCompatibleResponsesError> {
    if raw.trim().is_empty() {
        return Ok(json!({}));
    }
    let value = serde_json::from_str::<Value>(raw)
        .map_err(|error| output_shape(format!("function_call.arguments 不是合法 JSON: {error}")))?;
    if !value.is_object() {
        return Err(output_shape("function_call.arguments 必须是 JSON object"));
    }
    Ok(value)
}

fn replay_item_types(items: &[Value]) -> String {
    let mut types = items
        .iter()
        .filter_map(|item| item.get("type").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<Vec<_>>();
    types.sort();
    types.dedup();
    if types.is_empty() {
        "[]".into()
    } else {
        format!("[{}]", types.join(", "))
    }
}

fn reasoning_effort_name(effort: ReasoningEffort) -> Option<&'static str> {
    match effort {
        ReasoningEffort::None => None,
        ReasoningEffort::Low => Some("low"),
        ReasoningEffort::Medium => Some("medium"),
        ReasoningEffort::High => Some("high"),
        ReasoningEffort::Xhigh => Some("xhigh"),
        ReasoningEffort::Max => Some("max"),
    }
}

fn wrap_media_rejection(
    error: OpenAiCompatibleResponsesError,
    request_has_media: bool,
) -> OpenAiCompatibleResponsesError {
    if !request_has_media {
        return error;
    }
    match error {
        OpenAiCompatibleResponsesError::Client(ResponsesError::Status { status, body })
            if (400..500).contains(&status) && status != 401 && status != 429 =>
        {
            OpenAiCompatibleResponsesError::MediaRejected {
                source: ResponsesError::Status {
                    status,
                    body: redact_media_error_body(&body),
                },
            }
        }
        other => other,
    }
}

fn classify_request_too_large(
    error: &OpenAiCompatibleResponsesError,
) -> Option<ProviderRequestTooLarge> {
    let OpenAiCompatibleResponsesError::Client(ResponsesError::Status { status: 413, .. }) = error
    else {
        return None;
    };
    Some(ProviderRequestTooLarge::new())
}

fn output_shape(reason: impl Into<String>) -> OpenAiCompatibleResponsesError {
    OpenAiCompatibleResponsesError::OutputShape {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::api::{
        AgentTurnLoop, CompletedSessionTurnMessage, ModelContextSource, SessionTurnContextAppender,
        SessionTurnEvent, SessionTurnEventRecorder, SessionTurnHooks, SessionTurnRequest,
    };
    use crate::config::ToolConfig;
    use crate::tool::ToolRegistry;

    fn adapter_with_reasoning_effort(
        reasoning_effort: ReasoningEffort,
    ) -> OpenAiCompatibleResponsesProviderAdapter {
        OpenAiCompatibleResponsesProviderAdapter {
            client: ResponsesClient::new(
                "http://127.0.0.1:1".into(),
                "test-key".into(),
                Duration::from_secs(1),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
            model: "test-model".into(),
            reasoning_effort,
            include_reasoning_replay: true,
            temperature: None,
            top_p: None,
        }
    }

    struct StaticContextAppender {
        messages: Vec<SessionTurnMessage>,
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

    #[derive(Default)]
    struct CompletedMessageRecorder {
        messages: Vec<CompletedSessionTurnMessage>,
    }

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

    #[derive(Default)]
    struct FailingInternalRequestPreflight {
        ready_calls: usize,
    }

    #[async_trait]
    impl crate::api::SessionTurnPreflight for FailingInternalRequestPreflight {
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
            self.ready_calls += 1;
            if self.ready_calls == 2 {
                anyhow::bail!("internal request WAL unavailable");
            }
            Ok(())
        }
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

    #[async_trait]
    impl SessionTurnEventRecorder for CompletedMessageRecorder {
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

    #[test]
    fn history_media_policy_preserves_uncompacted_images_and_documents() {
        assert_eq!(
            adapter_with_reasoning_effort(ReasoningEffort::None).history_media_policy(),
            ProviderHistoryMediaPolicy::Preserve
        );
    }

    #[test]
    fn matching_replay_wins_over_canonical_projection() {
        let raw = json!({
            "type":"reasoning","id":"rs_1","encrypted_content":"opaque","future":true
        });
        let input = session_turn_messages_to_responses(
            vec![SessionTurnMessage {
                role: "assistant".into(),
                content: vec![SessionTurnContentBlock::text("不要重复")],
                provider_replay: Some(ProviderReplayState::OpenAiResponses {
                    model: Some("test-model".into()),
                    items: vec![raw.clone()],
                }),
            }],
            "test-model",
        )
        .unwrap();

        assert_eq!(input.items, vec![raw]);
    }

    #[test]
    fn unbound_or_wrong_model_replay_falls_back_to_canonical_projection() {
        for replay_model in [None, Some("other-model".to_string())] {
            let input = session_turn_messages_to_responses(
                vec![SessionTurnMessage {
                    role: "assistant".into(),
                    content: vec![SessionTurnContentBlock::text("canonical")],
                    provider_replay: Some(ProviderReplayState::OpenAiResponses {
                        model: replay_model,
                        items: vec![json!({"type":"reasoning", "private":true})],
                    }),
                }],
                "test-model",
            )
            .unwrap();

            assert_eq!(input.items.len(), 1);
            assert_eq!(input.items[0]["role"], "assistant");
            assert_eq!(input.items[0]["content"][0]["text"], "canonical");
        }
    }

    #[test]
    fn canonical_media_and_tool_results_map_to_responses_items() {
        let input = session_turn_messages_to_responses(
            vec![SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![
                    SessionTurnContentBlock::ToolResult {
                        tool_use_id: "call_1".into(),
                        content: "ok".into(),
                    },
                    SessionTurnContentBlock::text("看附件"),
                    SessionTurnContentBlock::image("image/png", "QUJD"),
                    SessionTurnContentBlock::document_named("application/pdf", "REVG", "brief.pdf"),
                ],
            }],
            "test-model",
        )
        .unwrap();

        assert!(input.has_media);
        assert_eq!(input.items[0]["type"], "function_call_output");
        assert_eq!(input.items[0]["call_id"], "call_1");
        assert_eq!(input.items[1]["content"][1]["type"], "input_image");
        assert_eq!(
            input.items[1]["content"][1]["image_url"],
            "data:image/png;base64,QUJD"
        );
        assert_eq!(input.items[1]["content"][2]["type"], "input_file");
        assert_eq!(input.items[1]["content"][2]["filename"], "brief.pdf");
        assert_eq!(
            input.items[1]["content"][2]["file_data"],
            "data:application/pdf;base64,REVG"
        );
    }

    #[test]
    fn canonical_assistant_text_and_tool_use_preserve_source_order() {
        let input = session_turn_messages_to_responses(
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
                    SessionTurnContentBlock::text("再说"),
                ],
            }],
            "test-model",
        )
        .unwrap();

        assert_eq!(input.items[0]["type"], "message");
        assert_eq!(input.items[1]["type"], "function_call");
        assert_eq!(input.items[1]["arguments"], r#"{"path":"a.txt"}"#);
        assert_eq!(input.items[2]["type"], "message");
    }

    #[test]
    fn model_context_serializes_as_stable_user_input_and_preserves_prefix() {
        let prefix_messages = vec![
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
        let first = session_turn_messages_to_responses(prefix_messages.clone(), "test-model")
            .unwrap()
            .items;
        let mut extended = prefix_messages;
        extended.push(SessionTurnMessage::assistant_text("first answer"));
        extended.push(SessionTurnMessage::user_text("second request"));
        let second = session_turn_messages_to_responses(extended, "test-model")
            .unwrap()
            .items;

        assert!(second.starts_with(&first));
        assert_eq!(first[0]["role"], "user");
        assert_eq!(
            first[0]["content"][0]["text"],
            "<runtime_context>stable</runtime_context>"
        );
        assert_eq!(first[1]["role"], "user");
        assert_eq!(
            first[1]["content"][0]["text"],
            "<background_processes>empty</background_processes>"
        );
        assert!(!serde_json::to_string(&first).unwrap().contains("sha256-v1"));
    }

    #[tokio::test]
    async fn consecutive_main_like_turns_keep_exact_responses_wire_prefix() {
        let first_item = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"first answer"}]
        });
        let second_item = json!({
            "type":"message","id":"msg_2","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"second answer"}]
        });
        let (endpoint, requests) = spawn_raw_sequence(vec![
            sse_response(&[first_item], Some("first answer")),
            sse_response(&[second_item], Some("second answer")),
        ])
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);

        let first_turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "stable system".into(),
                    history: Vec::new(),
                    user_text: "first request".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |_| {},
            )
            .await
            .unwrap();
        turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "stable system".into(),
                    history: first_turn
                        .messages
                        .into_iter()
                        .map(|message| message.message)
                        .collect(),
                    user_text: "second request".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |_| {},
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["instructions"], requests[1]["instructions"]);
        assert_eq!(requests[0]["tools"], requests[1]["tools"]);
        let first_input = requests[0]["input"].as_array().unwrap();
        let second_input = requests[1]["input"].as_array().unwrap();
        assert!(second_input.starts_with(first_input));
        assert_eq!(first_input[0]["role"], "user");
        assert!(first_input[0]["content"][0]["text"]
            .as_str()
            .is_some_and(|text| text.starts_with("<runtime_context>")));
    }

    #[test]
    fn request_uses_store_false_strict_false_and_optional_reasoning() {
        let adapter = adapter_with_reasoning_effort(ReasoningEffort::High);
        let request = adapter.request_for(
            "system",
            vec![user_text_item("hello")],
            tool_specs_to_responses(vec![ToolSpec {
                name: "file_read".into(),
                description: "read".into(),
                input_schema: json!({"type":"object"}),
            }]),
            123,
            true,
        );
        let value = serde_json::to_value(request).unwrap();

        assert_eq!(value["store"], false);
        assert_eq!(value["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(value["stream"], true);
        assert_eq!(value["max_output_tokens"], 123);
        assert_eq!(value["reasoning"]["effort"], "high");
        assert!(value.get("temperature").is_none());
        assert!(value.get("top_p").is_none());
        assert!(value["reasoning"].get("summary").is_none());
        assert_eq!(value["tools"][0]["strict"], false);
        assert_eq!(value["tools"][0]["type"], "function");

        let none = adapter_with_reasoning_effort(ReasoningEffort::None).request_for(
            "",
            Vec::new(),
            Vec::new(),
            1,
            false,
        );
        assert!(serde_json::to_value(none)
            .unwrap()
            .get("reasoning")
            .is_none());
    }

    #[test]
    fn configured_sampling_parameters_are_sent_for_streaming_and_non_streaming_requests() {
        let adapter = adapter_with_reasoning_effort(ReasoningEffort::None)
            .with_sampling_parameters(Some(0.6), Some(0.85));

        for stream in [false, true] {
            let request = adapter.request_for("system", Vec::new(), Vec::new(), 128, stream);
            let value = serde_json::to_value(request).unwrap();
            assert_eq!(value.get("temperature"), Some(&json!(0.6)));
            assert_eq!(value.get("top_p"), Some(&json!(0.85)));
        }
    }

    #[test]
    fn completed_turn_projects_text_tool_and_full_replay() {
        let reasoning = json!({"type":"reasoning","encrypted_content":"opaque"});
        let turn = ContinuedResponsesTurn {
            merged_text: "done".into(),
            replay_items: vec![reasoning.clone()],
            function_calls: vec![super::super::responses::ResponsesFunctionCall {
                call_id: "call_1".into(),
                name: "file_read".into(),
                arguments: r#"{"path":"a.txt"}"#.into(),
            }],
            terminal: ResponsesTerminal::Completed,
            transport: ProviderTransport::ResponsesSse,
        };

        let response = provider_response_from_turn(turn, "test-model").unwrap();

        assert_eq!(response.stop, ProviderStop::ToolUse);
        assert_eq!(response.assistant_message.content.len(), 2);
        assert_eq!(
            response.assistant_message.provider_replay,
            Some(ProviderReplayState::OpenAiResponses {
                model: Some("test-model".into()),
                items: vec![reasoning]
            })
        );
    }

    #[test]
    fn unknown_only_success_is_rejected_without_dumping_raw_payload() {
        let secret = "A".repeat(400);
        let error = provider_response_from_turn(
            ContinuedResponsesTurn {
                merged_text: String::new(),
                replay_items: vec![json!({"type":"output_image","data":secret})],
                function_calls: Vec::new(),
                terminal: ResponsesTerminal::Completed,
                transport: ProviderTransport::ResponsesSse,
            },
            "test-model",
        )
        .unwrap_err();

        let display = error.to_string();
        assert!(display.contains("output_image"));
        assert!(!display.contains(&secret));
    }

    #[test]
    fn invalid_function_arguments_error_does_not_echo_arguments() {
        let raw = format!(r#"{{"payload":"{}""#, "A".repeat(400));
        let error = parse_tool_arguments(&raw).unwrap_err();

        assert!(!error.to_string().contains(&"A".repeat(400)));
    }

    #[test]
    fn media_4xx_gets_hint_without_protocol_fallback() {
        let error = wrap_media_rejection(
            OpenAiCompatibleResponsesError::Client(ResponsesError::Status {
                status: 400,
                body: "bad input".into(),
            }),
            true,
        );

        assert!(error.to_string().contains("可能不支持图片 / PDF 附件"));
        assert!(error.to_string().contains("bad input"));
    }

    #[test]
    fn http_413_is_classified_as_request_too_large() {
        let error = OpenAiCompatibleResponsesError::Client(ResponsesError::Status {
            status: 413,
            body: "request body exceeds gateway limit".into(),
        });

        let classified = classify_request_too_large(&error).expect("HTTP 413 classification");

        assert!(classified.to_string().contains("HTTP 413"));
        assert!(!classified.to_string().contains("gateway limit"));
        assert!(
            classify_request_too_large(&OpenAiCompatibleResponsesError::Client(
                ResponsesError::Status {
                    status: 429,
                    body: "rate limited".into(),
                }
            ))
            .is_none()
        );
    }

    #[test]
    fn terminal_stop_mapping_prefers_max_tokens_and_tools() {
        let max = provider_response_from_turn(
            ContinuedResponsesTurn {
                merged_text: "partial".into(),
                replay_items: vec![json!({"type":"message"})],
                function_calls: Vec::new(),
                terminal: ResponsesTerminal::MaxOutputTokens,
                transport: ProviderTransport::ResponsesSse,
            },
            "test-model",
        )
        .unwrap();
        assert_eq!(max.stop, ProviderStop::MaxTokens);
    }

    #[test]
    fn responses_stream_failure_is_exposed_to_provider_turn_loop() {
        let error = OpenAiCompatibleResponsesError::Client(ResponsesError::StreamFailure {
            reason: "missing terminal event".into(),
        });

        assert!(responses_adapter_stream_failure(&error));
        assert!(!responses_adapter_stream_failure(
            &OpenAiCompatibleResponsesError::OutputShape {
                reason: "non-stream shape".into(),
            }
        ));
    }

    #[test]
    fn completed_reasoning_only_response_is_recoverable_empty_output() {
        let error = provider_response_from_turn(
            ContinuedResponsesTurn {
                merged_text: String::new(),
                replay_items: vec![json!({
                    "type":"reasoning","id":"rs_1","encrypted_content":"opaque"
                })],
                function_calls: Vec::new(),
                terminal: ResponsesTerminal::Completed,
                transport: ProviderTransport::ResponsesSse,
            },
            "test-model",
        )
        .unwrap_err();

        assert!(matches!(
            error,
            OpenAiCompatibleResponsesError::NoConsumableOutput { .. }
        ));
    }

    #[tokio::test]
    async fn max_token_continuation_replays_every_round_and_internal_input() {
        let first_output = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"incomplete",
            "content":[{"type":"output_text","text":"hello"}]
        });
        let reasoning = json!({
            "type":"reasoning","id":"rs_2","encrypted_content":"opaque","future":"kept"
        });
        let second_output = json!({
            "type":"message","id":"msg_2","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"lo world"}]
        });
        let (endpoint, requests) = spawn_json_sequence(vec![
            json!({
                "status":"incomplete",
                "incomplete_details":{"reason":"max_output_tokens"},
                "output":[first_output.clone()],
                "usage":{"total_tokens":10}
            }),
            json!({
                "status":"completed",
                "output":[reasoning.clone(),second_output.clone()],
                "usage":{"total_tokens":15}
            }),
        ])
        .await;
        let adapter = OpenAiCompatibleResponsesProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut events = Vec::new();
        let mut observer = RecordingRequestObserver::default();

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
                    recovery_interrupt: None,
                    allow_continuation: true,
                    retry_count_override: None,
                },
                &mut |event| events.push(event),
                &mut observer,
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(response.stop, ProviderStop::Done);
        assert!(matches!(
            &response.assistant_message.content[0],
            SessionTurnContentBlock::Text { text } if text == "hello world"
        ));
        let Some(ProviderReplayState::OpenAiResponses { items, .. }) =
            response.assistant_message.provider_replay
        else {
            panic!("Responses assistant 应保存 replay")
        };
        assert_eq!(items.len(), 4);
        assert_eq!(items[0], first_output);
        assert_eq!(items[1]["role"], "user");
        assert_eq!(items[1]["content"][0]["text"], CONTINUATION_TRIGGER);
        assert_eq!(items[2], reasoning);
        assert_eq!(items[3], second_output);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["store"], false);
        assert_eq!(
            requests[0]["include"],
            json!(["reasoning.encrypted_content"])
        );
        assert_eq!(
            requests[1]["include"],
            json!(["reasoning.encrypted_content"])
        );
        assert_eq!(
            requests[1]["input"].as_array().unwrap().len(),
            requests[0]["input"].as_array().unwrap().len() + 2
        );
        assert_eq!(requests[1]["input"][1], first_output);
        assert_eq!(
            requests[1]["input"][2]["content"][0]["text"],
            CONTINUATION_TRIGGER
        );
        assert_eq!(observer.requests.len(), 2);
        assert!(observer.requests[1].starts_with(&observer.requests[0]));
        let observed_second =
            session_turn_messages_to_responses(observer.requests[1].clone(), "test-model").unwrap();
        assert_eq!(
            observed_second.items,
            requests[1]["input"].as_array().unwrap().clone(),
            "observer 上报的 neutral history 必须投影为同一份真实 Responses input"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, ProviderEvent::ContextUsageUpdated { .. }))
                .count(),
            2
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::AssistantMessageCompleted { text } if text == "hello world"
        )));
    }

    #[tokio::test]
    async fn max_token_response_does_not_continue_when_request_disables_it() {
        let (endpoint, requests) = spawn_json_sequence(vec![json!({
            "status":"incomplete",
            "incomplete_details":{"reason":"max_output_tokens"},
            "output":[{
                "type":"message","id":"msg_1","role":"assistant","status":"incomplete",
                "content":[{"type":"output_text","text":"partial"}]
            }]
        })])
        .await;
        let adapter = OpenAiCompatibleResponsesProviderAdapter::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            Duration::from_secs(5),
            0,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();

        let response = adapter
            .send(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 32,
                    stream: false,
                    stream_output_mode: crate::api::ProviderStreamOutputMode::Live,
                    runtime_chain_id: None,
                    runtime_fallback_scope: None,
                    recovery_interrupt: None,
                    allow_continuation: false,
                    retry_count_override: Some(0),
                },
                &mut |_| {},
            )
            .await
            .unwrap();

        assert_eq!(response.stop, ProviderStop::MaxTokens);
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn safe_steer_after_send_keeps_successful_incomplete_response_without_continuation() {
        let output = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"incomplete",
            "content":[{"type":"output_text","text":"partial-answer"}]
        });
        let (endpoint, requests) = spawn_json_sequence(vec![json!({
            "status":"incomplete",
            "incomplete_details":{"reason":"max_output_tokens"},
            "output":[output]
        })])
        .await;
        let adapter = OpenAiCompatibleResponsesProviderAdapter::new(
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
                    allow_continuation: true,
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
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn safe_steer_during_continuation_wal_keeps_responses_partial() {
        let first_output = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"incomplete",
            "content":[{"type":"output_text","text":"partial-answer"}]
        });
        let (endpoint, requests) = spawn_json_sequence(vec![json!({
            "status":"incomplete",
            "incomplete_details":{"reason":"max_output_tokens"},
            "output":[first_output.clone()],
            "usage":{"total_tokens":10}
        })])
        .await;
        let adapter = OpenAiCompatibleResponsesProviderAdapter::new(
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
                    allow_continuation: true,
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
        assert!(matches!(
            response.assistant_message.provider_replay,
            Some(ProviderReplayState::OpenAiResponses { items, .. }) if items == vec![first_output]
        ));
        assert!(interrupt.should_preserve_successful_response());
        assert_eq!(observer.requests.len(), 2);
        assert_eq!(observer.started, 1);
        assert_eq!(observer.abandoned, 1);
        assert_eq!(requests.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn internal_request_wal_failure_stops_before_second_http_without_fallback() {
        let partial_item = json!({
            "type":"message","id":"msg_partial","role":"assistant","status":"incomplete",
            "content":[{"type":"output_text","text":"partial"}]
        });
        let first = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({
                "type":"response.output_item.done",
                "output_index":0,
                "item":partial_item,
            }),
            json!({
                "type":"response.incomplete",
                "response":{
                    "status":"incomplete",
                    "incomplete_details":{"reason":"max_output_tokens"},
                    "output":[partial_item],
                },
            })
        );
        let (endpoint, requests) = spawn_raw_sequence(vec![first]).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);
        let mut preflight = FailingInternalRequestPreflight::default();

        let error = turn_loop
            .run_session_turn_with_context_hooks(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "continue internally".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                Vec::new(),
                &mut |_| {},
                None,
                SessionTurnHooks::new(None, None, Some(&mut preflight)),
            )
            .await
            .unwrap_err();
        let requests = requests.await.unwrap();

        assert!(error
            .to_string()
            .contains("internal request WAL unavailable"));
        assert_eq!(preflight.ready_calls, 2);
        assert_eq!(requests.len(), 1);
    }

    #[tokio::test]
    async fn responses_child_like_tool_loop_preserves_wire_prefix_and_context_order() {
        let first_items = vec![
            json!({"type":"reasoning","id":"rs_1","encrypted_content":"opaque"}),
            json!({"type":"function_call","id":"fc_1","call_id":"call_1","name":"file_read","arguments":"{\"path\":\"a.txt\"}","status":"completed"}),
            json!({"type":"function_call","id":"fc_2","call_id":"call_2","name":"file_read","arguments":"{\"path\":\"b.txt\"}","status":"completed"}),
        ];
        let final_item = json!({
            "type":"message","id":"msg_2","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"both read"}]
        });
        let first_sse = sse_response(&first_items, None);
        let second_sse = sse_response(&[final_item], Some("both read"));
        let (endpoint, requests) = spawn_raw_sequence(vec![first_sse, second_sse]).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("a.txt"), "alpha").unwrap();
        std::fs::write(workspace.path().join("b.txt"), "beta").unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap()
            .for_delegation(None),
        );
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128).with_max_tool_loop_turns(4);
        let background = SessionTurnMessage::model_context(
            ModelContextSource::BackgroundProcess,
            "<background_processes>\nProcesses:\n- none\n</background_processes>",
        );
        let mut appender = StaticContextAppender {
            messages: vec![background.clone()],
        };
        let mut recorder = CompletedMessageRecorder::default();

        let turn = turn_loop
            .run_session_turn_with_context_hooks(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "read both".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                Vec::new(),
                &mut |_| {},
                None,
                SessionTurnHooks::new(Some(&mut recorder), Some(&mut appender), None),
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert!(turn.messages.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(
                    block,
                    SessionTurnContentBlock::Text { text } if text == "both read"
                )
            })
        }));
        assert_eq!(requests.len(), 2);
        assert_eq!(recorder.messages, turn.messages);
        assert!(matches!(
            recorder
                .messages
                .first()
                .and_then(|message| message.model_context_snapshot()),
            Some((ModelContextSource::Runtime, _, _))
        ));
        assert_eq!(recorder.messages[1].message, background);
        assert_eq!(
            recorder
                .messages
                .iter()
                .filter(|message| {
                    message
                        .model_context_snapshot()
                        .is_some_and(|(source, _, _)| {
                            *source == ModelContextSource::BackgroundProcess
                        })
                })
                .count(),
            1,
            "child tool loop 的 unchanged background baseline 不得重复追加"
        );
        assert!(requests
            .iter()
            .all(|request| { request["include"] == json!(["reasoning.encrypted_content"]) }));
        let second_input = requests[1]["input"].as_array().unwrap();
        let first_input = requests[0]["input"].as_array().unwrap();
        assert!(second_input.starts_with(first_input));
        let output_positions = second_input
            .iter()
            .filter(|item| item["type"] == "function_call_output")
            .map(|item| item["call_id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(output_positions, vec!["call_1", "call_2"]);
        assert!(second_input
            .iter()
            .any(|item| { item["type"] == "reasoning" && item["encrypted_content"] == "opaque" }));
    }

    #[tokio::test]
    async fn turn_loop_never_executes_a_terminal_only_tool_item() {
        let done_message = json!({
            "type":"message","id":"msg_1","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"safe response"}]
        });
        let terminal_tool = json!({
            "type":"function_call","id":"fc_terminal","call_id":"call_terminal",
            "name":"file_write","arguments":"{\"path\":\"terminal-only.txt\",\"content\":\"must not exist\"}",
            "status":"completed"
        });
        let body = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({
                "type":"response.output_item.done","output_index":0,"item":done_message
            }),
            json!({
                "type":"response.completed",
                "response":{"status":"completed","output":[done_message,terminal_tool]}
            })
        );
        let (endpoint, requests) = spawn_raw_sequence(vec![body]).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap(),
        );
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "respond safely".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 1);
        assert!(!workspace.path().join("terminal-only.txt").exists());
        assert!(events
            .iter()
            .all(|event| !matches!(event, crate::api::SessionTurnEvent::ToolCallStarted { .. })));
        let assistant = turn
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .unwrap();
        assert!(matches!(
            &assistant.content[0],
            SessionTurnContentBlock::Text { text } if text == "safe response"
        ));
        assert_eq!(
            assistant.provider_replay,
            Some(ProviderReplayState::OpenAiResponses {
                model: Some("test-model".into()),
                items: vec![done_message]
            })
        );
    }

    #[tokio::test]
    async fn partial_stream_falls_back_to_json_without_committing_partial_replay() {
        let final_item = json!({
            "type":"message","id":"msg_ok","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"complete replacement"}]
        });
        let (endpoint, requests) = spawn_mixed_sequence(vec![
            (
                "text/event-stream",
                format!(
                    "data: {}\n\n",
                    json!({"type":"response.output_text.delta","delta":"partial"})
                ),
            ),
            (
                "application/json",
                json!({"status":"completed","output":[final_item.clone()] }).to_string(),
            ),
        ])
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["stream"], true);
        assert_eq!(requests[1]["stream"], false);
        let assistant = turn
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .unwrap();
        assert!(matches!(
            &assistant.content[0],
            SessionTurnContentBlock::Text { text } if text == "complete replacement"
        ));
        assert_eq!(
            assistant.provider_replay,
            Some(ProviderReplayState::OpenAiResponses {
                model: Some("test-model".into()),
                items: vec![final_item]
            })
        );
        assert!(!serde_json::to_string(&assistant.provider_replay)
            .unwrap()
            .contains("partial"));
        assert!(events.iter().any(|event| matches!(
            event,
            crate::api::SessionTurnEvent::NonStreamingFallbackSucceeded { .. }
        )));
    }

    #[tokio::test]
    async fn failed_internal_continuation_fallback_resumes_latest_exact_input() {
        let partial_item = json!({
            "type":"message","id":"msg_partial","role":"assistant","status":"incomplete",
            "content":[{"type":"output_text","text":"hello"}]
        });
        let final_item = json!({
            "type":"message","id":"msg_final","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":" world"}]
        });
        let first = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({
                "type":"response.output_item.done",
                "output_index":0,
                "item":partial_item,
            }),
            json!({
                "type":"response.incomplete",
                "response":{
                    "status":"incomplete",
                    "incomplete_details":{"reason":"max_output_tokens"},
                    "output":[partial_item],
                },
            })
        );
        let failed_second = format!(
            "data: {}\n\n",
            json!({"type":"response.output_text.delta","delta":"discarded"})
        );
        let (endpoint, requests) = spawn_mixed_sequence(vec![
            ("text/event-stream", first),
            ("text/event-stream", failed_second),
            (
                "application/json",
                json!({"status":"completed","output":[final_item]}).to_string(),
            ),
        ])
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |_| {},
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1]["input"], requests[2]["input"]);
        assert_eq!(requests[1]["stream"], true);
        assert_eq!(requests[2]["stream"], false);
        assert_eq!(
            requests[1]["input"].as_array().unwrap().len(),
            requests[0]["input"].as_array().unwrap().len() + 2
        );
        let assistant = turn
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .unwrap();
        assert!(matches!(
            assistant.content.first(),
            Some(SessionTurnContentBlock::Text { text }) if text == "hello world"
        ));
    }

    #[tokio::test]
    async fn zero_text_reasoning_stream_without_terminal_falls_back_to_json() {
        let failed_reasoning = json!({
            "type":"reasoning","id":"rs_failed","encrypted_content":"must-not-persist"
        });
        let final_item = json!({
            "type":"message","id":"msg_ok","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"fallback answer"}]
        });
        let (endpoint, requests) = spawn_mixed_sequence(vec![
            (
                "text/event-stream",
                format!(
                    "data: {}\n\n",
                    json!({
                        "type":"response.output_item.done",
                        "output_index":0,
                        "item":failed_reasoning
                    })
                ),
            ),
            (
                "application/json",
                json!({"status":"completed","output":[final_item.clone()] }).to_string(),
            ),
        ])
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["stream"], true);
        assert_eq!(requests[1]["stream"], false);
        let assistant = turn
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .unwrap();
        assert!(matches!(
            &assistant.content[0],
            SessionTurnContentBlock::Text { text } if text == "fallback answer"
        ));
        let replay = serde_json::to_string(&assistant.provider_replay).unwrap();
        assert!(!replay.contains("must-not-persist"));
        assert!(events.iter().any(|event| matches!(
            event,
            crate::api::SessionTurnEvent::NonStreamingFallbackSucceeded { attempt: 1, .. }
        )));
    }

    #[tokio::test]
    async fn completed_reasoning_only_stream_falls_back_without_persisting_reasoning() {
        let failed_reasoning = json!({
            "type":"reasoning","id":"rs_empty","encrypted_content":"must-not-persist"
        });
        let final_item = json!({
            "type":"message","id":"msg_ok","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"fallback answer"}]
        });
        let (endpoint, requests) = spawn_mixed_sequence(vec![
            ("text/event-stream", sse_response(&[failed_reasoning], None)),
            (
                "application/json",
                json!({"status":"completed","output":[final_item] }).to_string(),
            ),
        ])
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |_| {},
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["stream"], true);
        assert_eq!(requests[1]["stream"], false);
        let assistant = turn
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .unwrap();
        assert!(!serde_json::to_string(&assistant.message.provider_replay)
            .unwrap()
            .contains("must-not-persist"));
    }

    #[tokio::test]
    async fn zero_text_incomplete_tool_stream_never_executes_draft() {
        let failed_tool = json!({
            "type":"function_call","id":"fc_failed","call_id":"call_failed",
            "name":"file_write",
            "arguments":"{\"path\":\"must-not-exist.txt\",\"content\":\"unsafe\"}",
            "status":"completed"
        });
        let final_item = json!({
            "type":"message","id":"msg_ok","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"safe fallback"}]
        });
        let (endpoint, requests) = spawn_mixed_sequence(vec![
            (
                "text/event-stream",
                format!(
                    "data: {}\n\n",
                    json!({
                        "type":"response.output_item.done",
                        "output_index":0,
                        "item":failed_tool
                    })
                ),
            ),
            (
                "application/json",
                json!({"status":"completed","output":[final_item] }).to_string(),
            ),
        ])
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let workspace = tempfile::tempdir().unwrap();
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig {
                workspace_root: workspace.path().to_path_buf(),
                ..ToolConfig::default()
            })
            .unwrap(),
        );
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);
        let mut events = Vec::new();

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
        assert!(!workspace.path().join("must-not-exist.txt").exists());
        assert!(events.iter().all(|event| !matches!(
            event,
            crate::api::SessionTurnEvent::ToolCallStarted { id, .. } if id == "call_failed"
        )));
        let assistant = turn
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .unwrap();
        assert!(matches!(
            &assistant.message.content[0],
            SessionTurnContentBlock::Text { text } if text == "safe fallback"
        ));
    }

    #[tokio::test]
    async fn deterministic_failed_stream_does_not_fallback_after_text_delta() {
        let body = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({"type":"response.output_text.delta","delta":"partial"}),
            json!({
                "type":"response.failed",
                "response":{"error":{"message":"deterministic failure"}}
            })
        );
        let (endpoint, requests) = spawn_raw_sequence(vec![body]).await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap_err();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 1);
        assert!(error.to_string().contains("deterministic failure"));
        assert!(events.iter().all(|event| !matches!(
            event,
            crate::api::SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
        )));
    }

    #[tokio::test]
    async fn deterministic_incomplete_fallback_stops_remaining_attempts() {
        let (endpoint, requests) = spawn_mixed_sequence(vec![
            (
                "text/event-stream",
                format!(
                    "data: {}\n\n",
                    json!({"type":"response.output_text.delta","delta":"partial"})
                ),
            ),
            (
                "application/json",
                json!({
                    "status":"incomplete",
                    "incomplete_details":{"reason":"content_filter"},
                    "output":[]
                })
                .to_string(),
            ),
        ])
        .await;
        let adapter = Arc::new(
            OpenAiCompatibleResponsesProviderAdapter::new(
                "test-key".into(),
                endpoint,
                "test-model".into(),
                Duration::from_secs(5),
                0,
                Duration::ZERO,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let tools = Arc::new(ToolRegistry::new(&ToolConfig::default()).unwrap());
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128);
        let mut events = Vec::new();

        let error = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "hello".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |event| events.push(event),
            )
            .await
            .unwrap_err();
        let requests = requests.await.unwrap();

        assert_eq!(requests.len(), 2);
        assert!(error.to_string().contains("content_filter"));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    crate::api::SessionTurnEvent::NonStreamingFallbackAttemptStarted { .. }
                ))
                .count(),
            1
        );
    }

    async fn spawn_json_sequence(
        bodies: Vec<Value>,
    ) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(bodies.len());
            for body in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut socket).await;
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

    async fn spawn_raw_sequence(
        bodies: Vec<String>,
    ) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(bodies.len());
            for body in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut socket).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
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

    async fn spawn_mixed_sequence(
        responses: Vec<(&'static str, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for (content_type, body) in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_http_request(&mut socket).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
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

    fn sse_response(items: &[Value], delta: Option<&str>) -> String {
        let mut body = String::new();
        if let Some(delta) = delta {
            body.push_str(&format!(
                "data: {}\n\n",
                json!({"type":"response.output_text.delta","delta":delta})
            ));
        }
        for (index, item) in items.iter().enumerate() {
            body.push_str(&format!(
                "data: {}\n\n",
                json!({
                    "type":"response.output_item.done",
                    "output_index":index,
                    "item":item,
                })
            ));
        }
        body.push_str(&format!(
            "data: {}\n\n",
            json!({
                "type":"response.completed",
                "response":{"status":"completed","output":items,"usage":{"total_tokens":12}},
            })
        ));
        body
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
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
