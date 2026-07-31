//! Anthropic Messages SSE 流式响应解析。
//!
//! 本模块把 text_delta / input_json_delta 累积成既可展示的运行时事件，
//! 也可持久化到 session transcript 的完整 assistant content blocks。
//! 主 `anthropic.rs` 只负责 Anthropic 协议转换；工具回环由 provider-neutral
//! `AgentTurnLoop` 编排。

use futures::StreamExt;
use serde_json::{json, Value};

use super::protocol::{
    has_tool_use_block, ApiContent, ApiMessage, ContinuedAssistantTurn, CreateMessageRequest,
};
use super::{
    compute_backoff, is_retryable, AnthropicError, AnthropicMessagesClient, CONTINUATION_TRIGGER,
    MAX_CONTINUATION_TURNS,
};
use crate::api::continuation::append_with_overlap_dedupe;
use crate::api::llm_http::{read_llm_error_body, LlmHttpPhase};
use crate::api::SessionTurnEvent;
use crate::api::{
    context_usage_from_anthropic_committed_usage, context_usage_from_anthropic_input_usage,
};

impl AnthropicMessagesClient {
    #[cfg(test)]
    pub(super) async fn send_text_with_continuation_streaming_for_provider(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<super::protocol::ApiToolDefinition>>,
        max_tokens: u32,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        self.send_text_with_continuation_streaming_for_provider_with_retry_count(
            system,
            messages,
            tools,
            max_tokens,
            self.retry_count,
            emit,
        )
        .await
    }

    pub(super) async fn send_text_with_continuation_streaming_for_provider_with_retry_count(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<super::protocol::ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        self.send_text_with_continuation_streaming_with_policy(
            system,
            messages,
            tools,
            max_tokens,
            retry_count,
            emit,
            false,
        )
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "continuation 策略参数需显式穿过协议边界，避免隐藏 retry 语义"
    )]
    async fn send_text_with_continuation_streaming_with_policy(
        &self,
        system: &str,
        messages: &mut Vec<ApiMessage>,
        tools: Option<Vec<super::protocol::ApiToolDefinition>>,
        max_tokens: u32,
        retry_count: u32,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
        error_on_unresolved_max_tokens: bool,
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let mut merged_text = String::new();
        let mut last_response: Option<Value> = None;
        let mut last_blocks = Vec::new();
        let mut last_stop_reason = String::from("end_turn");

        for round in 0..=MAX_CONTINUATION_TURNS {
            let body = self.request_for(
                system,
                messages.clone(),
                tools.clone(),
                max_tokens,
                Some(true),
            );
            let response_turn = self
                .send_stream_with_retry(&body, retry_count, emit)
                .await?;
            messages.push(ApiMessage {
                role: "assistant".into(),
                content: ApiContent::Blocks(response_turn.final_blocks.clone()),
            });
            append_with_overlap_dedupe(&mut merged_text, &response_turn.merged_text);
            last_stop_reason = response_turn.final_stop_reason.clone();
            last_blocks = response_turn.final_blocks;
            last_response = Some(response_turn.final_response);

            if last_stop_reason != "max_tokens" {
                break;
            }
            if has_tool_use_block(&last_blocks) {
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

    async fn send_stream_once(
        &self,
        body: &CreateMessageRequest,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let resp = self
            .http
            .post(self.endpoint.as_str())
            .header("x-api-key", self.api_key.as_str())
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
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

        let mut sse_buffer = Vec::new();
        let mut builder = StreamingAssistantTurn::default();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| self.http_error(error, LlmHttpPhase::ReadStreamBody))?;
            sse_buffer.extend_from_slice(&chunk);
            for frame in drain_sse_frames(&mut sse_buffer) {
                if let Some(data) = sse_frame_data(&frame)? {
                    if data.trim() == "[DONE]" {
                        continue;
                    }
                    let event = serde_json::from_str::<Value>(&data)?;
                    builder.apply_event(&event, emit)?;
                }
            }
        }
        if !sse_buffer.is_empty() {
            if let Some(data) = sse_frame_data(&sse_buffer)? {
                if data.trim() != "[DONE]" {
                    let event = serde_json::from_str::<Value>(&data)?;
                    builder.apply_event(&event, emit)?;
                }
            }
        }
        builder.finish()
    }

    async fn send_stream_with_retry(
        &self,
        body: &CreateMessageRequest,
        retry_count: u32,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<ContinuedAssistantTurn, AnthropicError> {
        let mut last_retryable: Option<AnthropicError> = None;
        for attempt in 0..=retry_count {
            let mut replay_blocking_event_emitted = false;
            let result = {
                let mut tracking_emit = |event| {
                    if event_blocks_stream_retry(&event) {
                        replay_blocking_event_emitted = true;
                    }
                    emit(event);
                };
                self.send_stream_once(body, &mut tracking_emit).await
            };
            match result {
                Ok(turn) => return Ok(turn),
                Err(e)
                    if !replay_blocking_event_emitted
                        && is_retryable(&e)
                        && attempt < retry_count =>
                {
                    let backoff =
                        compute_backoff(attempt, self.retry_base_delay, self.retry_max_delay);
                    log::warn!(
                        target: "api",
                        "Anthropic stream 调用失败，{}ms 后重试 ({}/{}): {}",
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
                reason: "stream retry loop 未返回结果".into(),
                raw: String::new(),
            }),
        )
    }
}

fn event_blocks_stream_retry(event: &SessionTurnEvent) -> bool {
    !matches!(event, SessionTurnEvent::ContextUsageUpdated { .. })
}

#[derive(Debug)]
struct StreamingBlock {
    value: Value,
    input_json: String,
}

#[derive(Debug)]
struct StreamingAssistantTurn {
    blocks: Vec<Option<StreamingBlock>>,
    stop_reason: String,
    merged_text: String,
    usage: Option<Value>,
    saw_message_stop: bool,
}

impl Default for StreamingAssistantTurn {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            stop_reason: "end_turn".into(),
            merged_text: String::new(),
            usage: None,
            saw_message_stop: false,
        }
    }
}

impl StreamingAssistantTurn {
    fn apply_event(
        &mut self,
        event: &Value,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<(), AnthropicError> {
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                if let Some(usage) = event
                    .get("message")
                    .and_then(|message| message.get("usage"))
                    .cloned()
                {
                    if let Some(snapshot) = context_usage_from_anthropic_input_usage(&usage) {
                        emit(SessionTurnEvent::ContextUsageUpdated { usage: snapshot });
                    }
                    merge_usage(&mut self.usage, usage);
                }
            }
            Some("content_block_start") => {
                let index = sse_index(event)?;
                let block = event.get("content_block").cloned().ok_or_else(|| {
                    AnthropicError::OutputShape {
                        reason: "content_block_start 缺少 content_block".into(),
                        raw: event.to_string(),
                    }
                })?;
                self.start_block(index, block)?;
            }
            Some("content_block_delta") => {
                let index = sse_index(event)?;
                let delta = event
                    .get("delta")
                    .ok_or_else(|| AnthropicError::OutputShape {
                        reason: "content_block_delta 缺少 delta".into(),
                        raw: event.to_string(),
                    })?;
                self.apply_content_delta(index, delta, emit)?;
            }
            Some("content_block_stop") => {
                let index = sse_index(event)?;
                self.finish_block(index)?;
            }
            Some("message_delta") => {
                if let Some(stop_reason) = event
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    self.stop_reason = stop_reason.to_string();
                }
                if let Some(usage) = event.get("usage").cloned() {
                    merge_usage(&mut self.usage, usage);
                }
            }
            Some("message_stop") => {
                self.saw_message_stop = true;
                if let Some(snapshot) = self
                    .usage
                    .as_ref()
                    .and_then(context_usage_from_anthropic_committed_usage)
                {
                    emit(SessionTurnEvent::ContextUsageUpdated { usage: snapshot });
                }
            }
            Some("ping") => {}
            Some("error") => {
                return Err(AnthropicError::OutputShape {
                    reason: "Anthropic stream 返回 error event".into(),
                    raw: event.to_string(),
                });
            }
            other => {
                return Err(AnthropicError::OutputShape {
                    reason: format!("未知 Anthropic stream event: {other:?}"),
                    raw: event.to_string(),
                });
            }
        }
        Ok(())
    }

    fn apply_content_delta(
        &mut self,
        index: usize,
        delta: &Value,
        emit: &mut (dyn FnMut(SessionTurnEvent) + Send),
    ) -> Result<(), AnthropicError> {
        let block = self.block_mut(index)?;
        match delta.get("type").and_then(Value::as_str) {
            Some("text_delta") => {
                let text = delta
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                append_json_string_field(&mut block.value, "text", text);
                self.merged_text.push_str(text);
                if !text.is_empty() {
                    emit(SessionTurnEvent::AssistantTextDelta {
                        text: text.to_string(),
                    });
                }
            }
            Some("input_json_delta") => {
                let partial = delta
                    .get("partial_json")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                block.input_json.push_str(partial);
            }
            Some("thinking_delta") => {
                let thinking = delta
                    .get("thinking")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                append_json_string_field(&mut block.value, "thinking", thinking);
            }
            Some("signature_delta") => {
                let signature = delta
                    .get("signature")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                append_json_string_field(&mut block.value, "signature", signature);
            }
            other => {
                return Err(AnthropicError::OutputShape {
                    reason: format!("未知 content_block_delta type: {other:?}"),
                    raw: delta.to_string(),
                });
            }
        }
        Ok(())
    }

    fn finish_block(&mut self, index: usize) -> Result<(), AnthropicError> {
        let block = self.block_mut(index)?;
        if block.value.get("type").and_then(Value::as_str) == Some("tool_use")
            && !block.input_json.trim().is_empty()
        {
            let input = serde_json::from_str::<Value>(&block.input_json).map_err(|e| {
                AnthropicError::OutputShape {
                    reason: format!("tool_use input_json_delta 解析失败: {e}"),
                    raw: block.input_json.clone(),
                }
            })?;
            if let Some(object) = block.value.as_object_mut() {
                object.insert("input".into(), input);
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<ContinuedAssistantTurn, AnthropicError> {
        if !self.saw_message_stop {
            return Err(AnthropicError::OutputShape {
                reason: "stream closed before message_stop".into(),
                raw: String::new(),
            });
        }
        let final_blocks = self
            .blocks
            .into_iter()
            .map(|block| {
                block
                    .map(|block| block.value)
                    .ok_or_else(|| AnthropicError::OutputShape {
                        reason: "stream content block index 不连续".into(),
                        raw: String::new(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let final_response = json!({
            "content": final_blocks,
            "stop_reason": self.stop_reason,
        });
        Ok(ContinuedAssistantTurn {
            final_blocks,
            final_stop_reason: self.stop_reason,
            merged_text: self.merged_text,
            final_response,
        })
    }

    fn start_block(&mut self, index: usize, block: Value) -> Result<(), AnthropicError> {
        let expected = self.blocks.len();
        if index != expected {
            return Err(AnthropicError::OutputShape {
                reason: format!(
                    "stream content_block_start index 不连续: expected={expected}, actual={index}"
                ),
                raw: block.to_string(),
            });
        }
        self.blocks.push(Some(StreamingBlock {
            value: block,
            input_json: String::new(),
        }));
        Ok(())
    }

    fn block_mut(&mut self, index: usize) -> Result<&mut StreamingBlock, AnthropicError> {
        self.blocks
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or_else(|| AnthropicError::OutputShape {
                reason: format!("stream delta 引用了未开始的 content block: {index}"),
                raw: String::new(),
            })
    }
}

fn merge_usage(current: &mut Option<Value>, update: Value) {
    let Some(update_object) = update.as_object() else {
        return;
    };
    let target = current.get_or_insert_with(|| json!({}));
    let Some(target_object) = target.as_object_mut() else {
        *target = Value::Object(update_object.clone());
        return;
    };
    for (key, value) in update_object {
        target_object.insert(key.clone(), value.clone());
    }
}

fn append_json_string_field(value: &mut Value, field: &str, suffix: &str) {
    let current = value
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if let Some(object) = value.as_object_mut() {
        object.insert(field.into(), Value::String(format!("{current}{suffix}")));
    }
}

fn sse_index(event: &Value) -> Result<usize, AnthropicError> {
    let raw =
        event
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| AnthropicError::OutputShape {
                reason: "stream event 缺少 index".into(),
                raw: event.to_string(),
            })?;
    usize::try_from(raw).map_err(|_| AnthropicError::OutputShape {
        reason: "stream event index 超出 usize 范围".into(),
        raw: event.to_string(),
    })
}

fn drain_sse_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while let Some((frame_end, separator_len)) = find_sse_frame_separator(buffer) {
        let frame = buffer[..frame_end].to_vec();
        buffer.drain(..frame_end + separator_len);
        frames.push(frame);
    }
    frames
}

fn find_sse_frame_separator(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf < crlf => Some((lf, 2)),
        (Some(_), Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

fn sse_frame_data(frame: &[u8]) -> Result<Option<String>, AnthropicError> {
    let frame = std::str::from_utf8(frame).map_err(|e| AnthropicError::OutputShape {
        reason: format!("SSE frame 不是合法 UTF-8: {e}"),
        raw: String::new(),
    })?;
    let mut data_lines = Vec::new();
    for raw_line in frame.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            data_lines.push(data.strip_prefix(' ').unwrap_or(data));
        }
    }
    if data_lines.is_empty() {
        Ok(None)
    } else {
        Ok(Some(data_lines.join("\n")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn content_block_start_rejects_sparse_huge_index_without_allocating() {
        let mut turn = StreamingAssistantTurn::default();
        let huge_index = usize::MAX as u64;
        let event = json!({
            "type": "content_block_start",
            "index": huge_index,
            "content_block": {"type": "text", "text": ""}
        });

        let error = turn.apply_event(&event, &mut |_| {}).unwrap_err();

        assert!(error.to_string().contains("index 不连续"));
        assert!(turn.blocks.is_empty());
    }

    #[test]
    fn content_block_start_requires_next_consecutive_index() {
        let mut turn = StreamingAssistantTurn::default();
        let first = json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        });
        turn.apply_event(&first, &mut |_| {}).unwrap();
        let skipped = json!({
            "type": "content_block_start",
            "index": 2,
            "content_block": {"type": "text", "text": ""}
        });

        let error = turn.apply_event(&skipped, &mut |_| {}).unwrap_err();

        assert!(error.to_string().contains("expected=1, actual=2"));
        assert_eq!(turn.blocks.len(), 1);
    }

    #[tokio::test]
    async fn ctx_usage_only_event_does_not_block_stream_retry() {
        let first_body = sse_response(vec![json!({
            "type": "message_start",
            "message": {
                "usage": {
                    "input_tokens": 1,
                    "cache_creation_input_tokens": 2,
                    "cache_read_input_tokens": 3
                }
            }
        })]);
        let second_body = sse_response(vec![
            json!({
                "type": "message_start",
                "message": {
                    "usage": {
                        "input_tokens": 1,
                        "cache_creation_input_tokens": 2,
                        "cache_read_input_tokens": 3
                    }
                }
            }),
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""}
            }),
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "ok"}
            }),
            json!({"type": "content_block_stop", "index": 0}),
            json!({
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn"},
                "usage": {"output_tokens": 2}
            }),
            json!({"type": "message_stop"}),
        ]);
        let (endpoint, requests) = spawn_sse_server(vec![first_body, second_body]).await;
        let client = AnthropicMessagesClient::new(
            "test-key".into(),
            endpoint,
            "test-model".into(),
            128,
            Duration::from_secs(2),
            1,
            Duration::ZERO,
            Duration::ZERO,
        )
        .unwrap();
        let mut messages = vec![ApiMessage {
            role: "user".into(),
            content: ApiContent::Text("hello".into()),
        }];
        let mut events = Vec::new();

        let turn = client
            .send_text_with_continuation_streaming_for_provider(
                "system",
                &mut messages,
                None,
                128,
                &mut |event| events.push(event),
            )
            .await
            .unwrap();

        assert_eq!(turn.merged_text, "ok");
        assert_eq!(requests.lock().unwrap().len(), 2);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ContextUsageUpdated { usage }
                    if usage.used_tokens == 6
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::ContextUsageUpdated { usage }
                    if usage.used_tokens == 8
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                SessionTurnEvent::AssistantTextDelta { text }
                    if text == "ok"
            )
        }));
    }

    #[tokio::test]
    async fn stream_body_error_names_stream_phase() {
        let body = sse_response(vec![
            json!({
                "type": "message_start",
                "message": {
                    "usage": {
                        "input_tokens": 1
                    }
                }
            }),
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""}
            }),
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "partial"}
            }),
        ]);
        let endpoint = spawn_raw_response(format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
            body.len() + 64,
            body
        ))
        .await;
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
        let mut messages = vec![ApiMessage {
            role: "user".into(),
            content: ApiContent::Text("hello".into()),
        }];

        let result = client
            .send_text_with_continuation_streaming_for_provider(
                "system",
                &mut messages,
                None,
                128,
                &mut |_| {},
            )
            .await;
        let error = match result {
            Ok(_) => panic!("expected stream body error"),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("LLM response body"));
        assert!(error.contains("reading LLM stream response body"));
    }

    async fn spawn_sse_server(bodies: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let requests_for_task = requests.clone();
        tokio::spawn(async move {
            for body in bodies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                requests_for_task
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&buf[..n]).to_string());
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (format!("http://{addr}"), requests)
    }

    async fn spawn_raw_response(response: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let _ = socket.read(&mut buf).await.unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn sse_response(events: Vec<Value>) -> String {
        let mut body = String::new();
        for event in events {
            body.push_str("data: ");
            body.push_str(&event.to_string());
            body.push_str("\n\n");
        }
        body
    }
}
