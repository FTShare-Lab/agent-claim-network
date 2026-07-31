//! OpenAI-compatible Chat Completions provider adapter。
//!
//! 本模块只负责 canonical session message 与 Chat Completions 协议互转。
//! HTTP/SSE、重试和基础 DTO 复用 `chat_completions` 模块，供主 LLM 与 router rerank
//! 共用同一套 chat-compatible 实现。

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use super::chat_completions::{
    ChatCompletionChoice, ChatCompletionMessage, ChatCompletionRequest, ChatCompletionResponse,
    ChatCompletionsClient, ChatCompletionsError, ChatContentPart, ChatFinishReason, ChatMessage,
    ChatMessageContent, ChatStreamEvent, ChatStreamOptions, ChatTool, ChatToolCall,
};
use super::context_usage_from_openai_usage;
use super::continuation::{
    append_with_overlap_dedupe, CONTINUATION_TRIGGER, MAX_CONTINUATION_TURNS,
};
use super::provider::{
    ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop, ToolSpec,
};
use super::redact_media_error_body;
use super::types::{SessionTurnContentBlock, SessionTurnMessage};
use crate::config::ReasoningEffort;

#[derive(Debug, thiserror::Error)]
pub enum OpenAiCompatibleChatError {
    #[error(transparent)]
    Client(#[from] ChatCompletionsError),
    #[error("Chat Completions 输出不符合预期: {reason}; raw={raw}")]
    OutputShape { reason: String, raw: String },
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
        })
    }

    /// 设置 Chat Completions 请求的推理强度；`none` 会在序列化时省略。
    pub fn with_reasoning_effort(mut self, reasoning_effort: ReasoningEffort) -> Self {
        self.reasoning_effort = reasoning_effort;
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
            temperature: None,
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
        tools: Vec<ChatTool>,
        max_tokens: u32,
        stream: bool,
        retry_count: u32,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> Result<ContinuedChatTurn, OpenAiCompatibleChatError> {
        let mut merged_text = String::new();
        let mut last_message = None;
        let mut last_finish_reason = Some(ChatFinishReason::Stop);

        for round in 0..=MAX_CONTINUATION_TURNS {
            let request = self.request_for(
                system_prompt,
                messages.clone(),
                tools.clone(),
                max_tokens,
                stream,
            );
            let response = if stream {
                let mut chat_emit = |event| match event {
                    ChatStreamEvent::ContentDelta { text } => {
                        emit(ProviderEvent::AssistantTextDelta { text });
                    }
                };
                self.client
                    .send_with_retry_count(&request, retry_count, &mut chat_emit)
                    .await?
            } else {
                let mut noop = |_event: ChatStreamEvent| {};
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
            let choice = first_choice(response)?;
            let assistant = choice.message;
            let finish_reason = require_finish_reason(choice.finish_reason)?;
            reject_unsupported_finish_reason(&finish_reason)?;
            if let Some(text) = assistant.content.as_deref() {
                append_with_overlap_dedupe(&mut merged_text, text);
            }
            messages.push(message_from_response(&assistant));
            let has_tool_calls = !assistant.tool_calls.is_empty();
            last_message = Some(assistant);
            last_finish_reason = Some(finish_reason.clone());

            if finish_reason != ChatFinishReason::Length {
                break;
            }
            if has_tool_calls || round == MAX_CONTINUATION_TURNS {
                break;
            }
            messages.push(ChatMessage::user(CONTINUATION_TRIGGER.to_string()));
        }

        let message = last_message.ok_or_else(|| OpenAiCompatibleChatError::OutputShape {
            reason: "空响应：未获得 assistant message".into(),
            raw: String::new(),
        })?;
        Ok(ContinuedChatTurn {
            message,
            finish_reason: last_finish_reason,
            merged_text,
        })
    }
}

#[async_trait]
impl ProviderAdapter for OpenAiCompatibleChatProviderAdapter {
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
        let mut messages = session_turn_messages_to_chat(request.messages)?;
        let request_has_media = messages_contain_media(&messages);
        let tools = tool_specs_to_chat(request.tools);
        let turn = match self
            .send_with_continuation(
                &request.system_prompt,
                &mut messages,
                tools,
                request.max_tokens,
                request.stream,
                retry_count,
                emit,
            )
            .await
        {
            Ok(turn) => turn,
            Err(error) => return Err(wrap_media_rejection(error, request_has_media).into()),
        };
        if !turn.merged_text.trim().is_empty() {
            emit(ProviderEvent::AssistantMessageCompleted {
                text: turn.merged_text.clone(),
            });
        }
        Ok(ProviderResponse {
            stop: provider_stop_from_turn(&turn),
            assistant_message: assistant_turn_message(turn)?,
        })
    }
}

struct ContinuedChatTurn {
    message: ChatCompletionMessage,
    finish_reason: Option<ChatFinishReason>,
    merged_text: String,
}

fn session_turn_messages_to_chat(
    messages: Vec<SessionTurnMessage>,
) -> Result<Vec<ChatMessage>, OpenAiCompatibleChatError> {
    let mut out = Vec::new();
    for message in messages {
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
            SessionTurnContentBlock::Text { text } => parts.push(ChatContentPart::text(text)),
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

fn reject_unsupported_finish_reason(
    finish_reason: &ChatFinishReason,
) -> Result<(), OpenAiCompatibleChatError> {
    match finish_reason {
        ChatFinishReason::Stop
        | ChatFinishReason::ToolCalls
        | ChatFinishReason::Length
        | ChatFinishReason::FunctionCall => Ok(()),
        ChatFinishReason::ContentFilter => Err(OpenAiCompatibleChatError::OutputShape {
            reason: "finish_reason=content_filter，拒绝把被过滤输出当作完整 assistant 回合".into(),
            raw: String::new(),
        }),
        ChatFinishReason::Other => Err(OpenAiCompatibleChatError::OutputShape {
            reason: "未知 finish_reason，拒绝静默当作 Done".into(),
            raw: String::new(),
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
) -> Result<SessionTurnMessage, OpenAiCompatibleChatError> {
    let mut content = Vec::new();
    if !turn.merged_text.trim().is_empty() {
        content.push(SessionTurnContentBlock::text(turn.merged_text));
    } else if let Some(text) = turn.message.content {
        if !text.trim().is_empty() {
            content.push(SessionTurnContentBlock::text(text));
        }
    }
    for tool_call in turn.message.tool_calls {
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
    Ok(SessionTurnMessage {
        role: "assistant".into(),
        content,
    })
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
        }
    }

    #[test]
    fn canonical_tool_use_maps_to_chat_tool_call() {
        let messages = session_turn_messages_to_chat(vec![SessionTurnMessage {
            role: "assistant".into(),
            content: vec![
                SessionTurnContentBlock::text("先查"),
                SessionTurnContentBlock::ToolUse {
                    id: "call_1".into(),
                    name: "file_read".into(),
                    input: json!({"path":"a.txt"}),
                },
            ],
        }])
        .unwrap();
        let calls = messages[0].tool_calls.as_ref().unwrap();
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].function.name, "file_read");
        assert_eq!(calls[0].function.arguments, r#"{"path":"a.txt"}"#);
    }

    #[test]
    fn canonical_tool_result_maps_to_chat_tool_message() {
        let messages = session_turn_messages_to_chat(vec![SessionTurnMessage {
            role: "user".into(),
            content: vec![SessionTurnContentBlock::ToolResult {
                tool_use_id: "call_1".into(),
                content: r#"{"ok":true}"#.into(),
            }],
        }])
        .unwrap();
        assert_eq!(messages[0].role, "tool");
        assert_eq!(messages[0].tool_call_id.as_deref(), Some("call_1"));
    }

    #[test]
    fn mixed_tool_result_and_media_splits_into_tool_then_user_message() {
        let messages = session_turn_messages_to_chat(vec![SessionTurnMessage {
            role: "user".into(),
            content: vec![
                SessionTurnContentBlock::ToolResult {
                    tool_use_id: "call_1".into(),
                    content: r#"{"ok":true}"#.into(),
                },
                SessionTurnContentBlock::text("[file_read attachment] a.png"),
                SessionTurnContentBlock::image("image/png", "QUJD"),
            ],
        }])
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
        let messages = session_turn_messages_to_chat(vec![SessionTurnMessage {
            role: "user".into(),
            content: vec![
                SessionTurnContentBlock::text("看这张图"),
                SessionTurnContentBlock::image("image/png", "QUJD"),
            ],
        }])
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
        let messages = session_turn_messages_to_chat(vec![SessionTurnMessage {
            role: "user".into(),
            content: vec![SessionTurnContentBlock::document_named(
                "application/pdf",
                "QUJD",
                "brief.pdf",
            )],
        }])
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
        let messages =
            session_turn_messages_to_chat(vec![SessionTurnMessage::user_text("你好")]).unwrap();

        assert_eq!(
            messages[0].content,
            Some(ChatMessageContent::Text("你好".into()))
        );
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
        };
        let message = assistant_turn_message(turn).unwrap();
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
        };
        assert_eq!(provider_stop_from_turn(&turn), ProviderStop::MaxTokens);
    }

    #[test]
    fn content_filter_finish_reason_is_rejected() {
        let err = reject_unsupported_finish_reason(&ChatFinishReason::ContentFilter).unwrap_err();

        assert!(err.to_string().contains("content_filter"));
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
}
