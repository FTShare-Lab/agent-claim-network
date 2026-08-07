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
    ProviderAdapter, ProviderEvent, ProviderHistoryMediaPolicy, ProviderNoConsumableOutput,
    ProviderReplayIdentity, ProviderReplayProtocol, ProviderRequest, ProviderResponse,
    ProviderStop, ProviderStreamFailure, ProviderTerminalFailure, ToolSpec,
};
use super::redact_media_error_body;
use super::responses::{
    is_stream_failure, ResponsesClient, ResponsesError, ResponsesReasoning, ResponsesRequest,
    ResponsesStreamEvent, ResponsesTerminal, ResponsesTool,
};
use super::types::{ProviderReplayState, SessionTurnContentBlock, SessionTurnMessage};
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
        })
    }

    /// 设置 Responses `reasoning.effort`；`none` 会省略整个 reasoning 字段。
    pub fn with_reasoning_effort(mut self, reasoning_effort: ReasoningEffort) -> Self {
        self.reasoning_effort = reasoning_effort;
        self
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
            include: Some(vec!["reasoning.encrypted_content".into()]),
            reasoning: reasoning_effort_name(self.reasoning_effort).map(|effort| {
                ResponsesReasoning {
                    effort: effort.to_string(),
                }
            }),
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
        tools: Vec<ResponsesTool>,
        max_tokens: u32,
        stream: bool,
        retry_count: u32,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> Result<ContinuedResponsesTurn, OpenAiCompatibleResponsesError> {
        let mut merged_text = String::new();
        let mut replay_items = Vec::new();
        let mut last_function_calls = Vec::new();
        let mut last_terminal = ResponsesTerminal::Completed;

        for round in 0..=MAX_CONTINUATION_TURNS {
            let request = self.request_for(
                system_prompt,
                input.clone(),
                tools.clone(),
                max_tokens,
                stream,
            );
            let response = if stream {
                let mut responses_emit = |event| match event {
                    ResponsesStreamEvent::TextDelta { text } => {
                        emit(ProviderEvent::AssistantTextDelta { text });
                    }
                };
                self.client
                    .send_with_retry_count(&request, retry_count, &mut responses_emit)
                    .await?
            } else {
                let mut noop = |_event: ResponsesStreamEvent| {};
                self.client
                    .send_with_retry_count(&request, retry_count, &mut noop)
                    .await?
            };
            if let Some(usage) = response
                .usage
                .as_ref()
                .and_then(context_usage_from_openai_usage)
            {
                emit(ProviderEvent::ContextUsageUpdated { usage });
            }
            append_with_overlap_dedupe(&mut merged_text, &response.output_text);
            input.extend(response.output_items.iter().cloned());
            replay_items.extend(response.output_items.iter().cloned());
            last_terminal = response.terminal;
            last_function_calls = response.function_calls;

            if response.terminal != ResponsesTerminal::MaxOutputTokens {
                break;
            }
            if !last_function_calls.is_empty() || round == MAX_CONTINUATION_TURNS {
                break;
            }
            let continuation = user_text_item(CONTINUATION_TRIGGER);
            input.push(continuation.clone());
            replay_items.push(continuation);
        }

        Ok(ContinuedResponsesTurn {
            merged_text,
            replay_items,
            function_calls: last_function_calls,
            terminal: last_terminal,
        })
    }
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
        Some(self.client.timeout())
    }

    async fn send(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        let retry_count = request
            .retry_count_override
            .unwrap_or(self.client.retry_count());
        let input = session_turn_messages_to_responses(request.messages, &self.model)?;
        let request_has_media = input.has_media;
        let turn = match self
            .send_with_continuation(
                &request.system_prompt,
                input.items,
                tool_specs_to_responses(request.tools),
                request.max_tokens,
                request.stream,
                retry_count,
                emit,
            )
            .await
        {
            Ok(turn) => turn,
            Err(error) => {
                if request.stream && responses_adapter_stream_failure(&error) {
                    return Err(ProviderStreamFailure::new(error.to_string()).into());
                }
                let error = wrap_media_rejection(error, request_has_media);
                if matches!(
                    &error,
                    OpenAiCompatibleResponsesError::Client(ResponsesError::Failed { .. })
                        | OpenAiCompatibleResponsesError::Client(ResponsesError::Incomplete { .. })
                ) {
                    return Err(ProviderTerminalFailure::new(error.to_string()).into());
                }
                return Err(error.into());
            }
        };
        let response = match provider_response_from_turn(turn, &self.model) {
            Ok(response) => response,
            Err(OpenAiCompatibleResponsesError::NoConsumableOutput { reason }) => {
                return Err(ProviderNoConsumableOutput::new(reason).into());
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
    matches!(error, OpenAiCompatibleResponsesError::Client(error) if is_stream_failure(error))
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
            SessionTurnContentBlock::Text { text } => {
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

fn output_shape(reason: impl Into<String>) -> OpenAiCompatibleResponsesError {
    OpenAiCompatibleResponsesError::OutputShape {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::api::{AgentTurnLoop, SessionTurnRequest};
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
    fn terminal_stop_mapping_prefers_max_tokens_and_tools() {
        let max = provider_response_from_turn(
            ContinuedResponsesTurn {
                merged_text: "partial".into(),
                replay_items: vec![json!({"type":"message"})],
                function_calls: Vec::new(),
                terminal: ResponsesTerminal::MaxOutputTokens,
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

        let response = adapter
            .send(
                ProviderRequest {
                    system_prompt: "system".into(),
                    messages: vec![SessionTurnMessage::user_text("hello")],
                    tools: Vec::new(),
                    max_tokens: 32,
                    stream: false,
                    retry_count_override: None,
                },
                &mut |event| events.push(event),
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
        assert_eq!(requests[1]["input"].as_array().unwrap().len(), 3);
        assert_eq!(requests[1]["input"][1], first_output);
        assert_eq!(
            requests[1]["input"][2]["content"][0]["text"],
            CONTINUATION_TRIGGER
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
    async fn turn_loop_executes_parallel_calls_and_returns_ordered_outputs() {
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
            .unwrap(),
        );
        let turn_loop = AgentTurnLoop::new(adapter, tools, 128).with_max_tool_loop_turns(4);

        let turn = turn_loop
            .run_session_turn(
                SessionTurnRequest {
                    current_session_id: None,
                    current_turn_id: None,
                    system_prompt: "system".into(),
                    history: Vec::new(),
                    user_text: "read both".into(),
                    user_attachments: Vec::new(),
                    skill_instructions: Vec::new(),
                },
                &mut |_| {},
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
        assert!(requests
            .iter()
            .all(|request| { request["include"] == json!(["reasoning.encrypted_content"]) }));
        let second_input = requests[1]["input"].as_array().unwrap();
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
