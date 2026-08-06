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
    ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop, ToolSpec,
};
use super::redact_media_error_body;
use super::types::{SessionTurnContentBlock, SessionTurnEvent, SessionTurnMessage};
use crate::config::ReasoningEffort;
use crate::prompt::PromptError;

mod protocol;
mod streaming;

use protocol::*;

#[derive(Debug, thiserror::Error)]
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
    #[error("LLM 输出不符合预期 schema: {reason}; raw={raw}")]
    OutputShape { reason: String, raw: String },
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
}

pub struct AnthropicProviderAdapter {
    client: AnthropicMessagesClient,
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
        let http = reqwest::Client::builder()
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
            endpoint: Arc::new(
                resolve_llm_endpoint(&endpoint, LlmEndpointKind::AnthropicMessages)
                    .map_err(|error| AnthropicError::InvalidEndpoint(error.to_string()))?,
            ),
            model: Arc::new(model),
            retry_count,
            retry_base_delay,
            retry_max_delay,
            timeout,
            reasoning_effort: ReasoningEffort::None,
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
            return Err(AnthropicError::Auth(body));
        }
        if !status.is_success() {
            let body = read_llm_error_body(resp, self.timeout).await;
            return Err(AnthropicError::Status {
                status: status.as_u16(),
                body,
            });
        }

        resp.json()
            .await
            .map_err(|error| self.http_error(error, LlmHttpPhase::DecodeJsonBody))
    }

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
        )
        .await
    }

    async fn send_text_with_continuation_with_policy(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        error_on_unresolved_max_tokens: bool,
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let mut merged_text = String::new();
        let mut last_response: Option<Value> = None;
        let mut last_blocks = Vec::new();
        let mut last_stop_reason = String::from("end_turn");

        for round in 0..=MAX_CONTINUATION_TURNS {
            let body = self.request_for(system, messages.clone(), tools.clone(), max_tokens, None);
            let response = self.send_with_retry_count(&body, retry_count).await?;
            let assistant_blocks =
                content_blocks(&response).ok_or_else(|| AnthropicError::OutputShape {
                    reason: "缺少 content blocks".into(),
                    raw: response.to_string(),
                })?;
            let stop_reason = required_anthropic_stop_reason(&response)?;
            messages.push(ApiMessage {
                role: "assistant".into(),
                content: ApiContent::Blocks(assistant_blocks.clone()),
            });
            if let Some(text) = extract_text_block(&response) {
                append_with_overlap_dedupe(&mut merged_text, text);
            }
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
            messages.push(ApiMessage {
                role: "user".into(),
                content: ApiContent::Text(CONTINUATION_TRIGGER.into()),
            });
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
        })
    }
}

#[async_trait]
impl ProviderAdapter for AnthropicProviderAdapter {
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
        let retry_count = request
            .retry_count_override
            .unwrap_or(self.client.retry_count);
        let request_has_media = request.messages.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(
                    block,
                    SessionTurnContentBlock::Image { .. }
                        | SessionTurnContentBlock::Document { .. }
                )
            })
        });
        let mut api_messages = session_turn_messages_to_api(request.messages);
        let api_tools = tool_specs_to_api(request.tools);

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
                .send_text_with_continuation_streaming_for_provider_with_retry_count(
                    &request.system_prompt,
                    &mut api_messages,
                    api_tools,
                    request.max_tokens,
                    retry_count,
                    &mut provider_emit,
                )
                .await
        } else {
            self.client
                .send_text_with_continuation_for_provider_with_retry_count(
                    &request.system_prompt,
                    &mut api_messages,
                    api_tools,
                    request.max_tokens,
                    retry_count,
                )
                .await
        };
        let turn = match turn_result {
            Ok(turn) => turn,
            Err(error) => return Err(wrap_media_rejection(error, request_has_media).into()),
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

        if !turn.merged_text.trim().is_empty() {
            emit(ProviderEvent::AssistantMessageCompleted {
                text: turn.merged_text.clone(),
            });
        }

        Ok(ProviderResponse {
            stop: provider_stop_from_turn(&turn),
            assistant_message: assistant_turn_message(&turn),
        })
    }
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
}

fn session_turn_messages_to_api(messages: Vec<SessionTurnMessage>) -> Vec<ApiMessage> {
    messages
        .into_iter()
        .filter_map(|message| {
            let content = message
                .content
                .into_iter()
                .filter_map(session_turn_block_to_api)
                .collect::<Vec<_>>();
            if content.is_empty() {
                None
            } else {
                Some(ApiMessage {
                    role: message.role,
                    content: ApiContent::Blocks(content),
                })
            }
        })
        .collect()
}

fn session_turn_block_to_api(block: SessionTurnContentBlock) -> Option<Value> {
    match block {
        SessionTurnContentBlock::Text { text } => Some(json!({"type": "text", "text": text})),
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

fn assistant_turn_message(turn: &ContinuedAssistantTurn) -> SessionTurnMessage {
    let content = if !has_tool_use_block(&turn.final_blocks) && !turn.merged_text.trim().is_empty()
    {
        vec![SessionTurnContentBlock::text(turn.merged_text.clone())]
    } else {
        assistant_content_blocks_without_thinking(turn)
    };
    SessionTurnMessage {
        role: "assistant".into(),
        content,
    }
}

fn provider_stop_from_turn(turn: &ContinuedAssistantTurn) -> ProviderStop {
    if turn.final_stop_reason == "max_tokens" {
        ProviderStop::MaxTokens
    } else if turn.final_stop_reason == "tool_use" {
        ProviderStop::ToolUse
    } else {
        ProviderStop::Done
    }
}

fn required_anthropic_stop_reason(response: &Value) -> Result<String, AnthropicError> {
    response
        .get("stop_reason")
        .and_then(Value::as_str)
        .filter(|reason| !reason.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| AnthropicError::OutputShape {
            reason: "缺少有效 stop_reason".into(),
            raw: response.to_string(),
        })
}

fn assistant_content_blocks_without_thinking(
    turn: &ContinuedAssistantTurn,
) -> Vec<SessionTurnContentBlock> {
    let mut content = Vec::new();
    for block in &turn.final_blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("thinking") | Some("redacted_thinking") => continue,
            _ => match api_block_to_session_turn_block(block) {
                Ok(block) => content.push(block),
                Err(_) => return vec![SessionTurnContentBlock::text(turn.merged_text.clone())],
            },
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

fn is_retryable(e: &AnthropicError) -> bool {
    match e {
        AnthropicError::Http(error) => error.is_retryable(),
        AnthropicError::Status { status, .. } => *status == 429 || *status >= 500,
        AnthropicError::ResponseJson(_) | AnthropicError::OutputShape { .. } => true,
        AnthropicError::Auth(_)
        | AnthropicError::InvalidEndpoint(_)
        | AnthropicError::Prompt(_) => false,
        // 多模态拒收是确定性 4xx，重试无意义
        AnthropicError::MediaRejected { .. } => false,
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
    use super::*;

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
        };

        assert_eq!(provider_stop_from_turn(&turn), ProviderStop::Done);
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
}
