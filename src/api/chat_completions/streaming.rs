use rustc_hash::FxHashMap;
use serde_json::Value;

use super::client::{ChatCompletionsError, ChatStreamEvent};
use super::protocol::{
    ChatCompletionChoice, ChatCompletionMessage, ChatCompletionResponse, ChatFinishReason,
    ChatStreamFrame, ChatToolCall, ChatToolCallFunction,
};

pub(super) fn drain_sse_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while let Some((pos, delimiter_len)) = find_frame_end(buffer) {
        let frame = buffer.drain(..pos).collect::<Vec<_>>();
        buffer.drain(..delimiter_len);
        frames.push(frame);
    }
    frames
}

pub(super) fn sse_frame_data(frame: &[u8]) -> Result<Option<String>, ChatCompletionsError> {
    let text = std::str::from_utf8(frame).map_err(|e| ChatCompletionsError::OutputShape {
        reason: format!("SSE frame 不是 UTF-8: {e}"),
        raw: String::from_utf8_lossy(frame).to_string(),
    })?;
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            data.push(rest.trim_start());
        }
    }
    if data.is_empty() {
        Ok(None)
    } else {
        Ok(Some(data.join("\n")))
    }
}

fn find_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|w| w == b"\n\n");
    let crlf = buffer.windows(4).position(|w| w == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf < crlf => Some((lf, 2)),
        (Some(_), Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

#[derive(Default)]
pub(super) struct ChatStreamAccumulator {
    role: Option<String>,
    content: String,
    tool_calls: FxHashMap<usize, ToolCallDraft>,
    finish_reason: Option<ChatFinishReason>,
    usage: Option<Value>,
    model: Option<String>,
}

impl ChatStreamAccumulator {
    pub(super) fn apply_frame(
        &mut self,
        data: &str,
        emit: &mut (dyn FnMut(ChatStreamEvent) + Send),
    ) -> Result<(), ChatCompletionsError> {
        if data.trim() == "[DONE]" {
            return Ok(());
        }
        let frame = serde_json::from_str::<ChatStreamFrame>(data)
            .map_err(ChatCompletionsError::ResponseJson)?;
        if let Some(usage) = frame.usage {
            self.usage = Some(usage);
        }
        if let Some(model) = frame.model {
            self.model = Some(model);
        }
        for choice in frame.choices {
            if let Some(reason) = choice.finish_reason {
                self.finish_reason = Some(reason);
            }
            if self.role.is_none() {
                self.role = choice.delta.role;
            }
            if let Some(content) = choice.delta.content {
                self.content.push_str(&content);
                if !content.is_empty() {
                    emit(ChatStreamEvent::ContentDelta { text: content });
                }
            }
            for tool_call in choice.delta.tool_calls {
                let index = tool_call.index.unwrap_or(self.tool_calls.len());
                let draft = self.tool_calls.entry(index).or_default();
                if let Some(id) = tool_call.id.filter(|id| !id.trim().is_empty()) {
                    draft.id = Some(id);
                }
                if let Some(kind) = tool_call.kind.filter(|kind| !kind.trim().is_empty()) {
                    draft.kind = Some(kind);
                }
                if let Some(function) = tool_call.function {
                    if let Some(name) = function.name.filter(|name| !name.trim().is_empty()) {
                        draft.name = Some(name);
                    }
                    if let Some(arguments) = function.arguments {
                        draft.arguments.push_str(&arguments);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<ChatCompletionResponse, ChatCompletionsError> {
        let finish_reason =
            self.finish_reason
                .ok_or_else(|| ChatCompletionsError::OutputShape {
                    reason: "stream 在明确 finish_reason 前结束".into(),
                    raw: String::new(),
                })?;
        let mut indices = self.tool_calls.keys().copied().collect::<Vec<_>>();
        indices.sort_unstable();
        let mut tool_calls = Vec::with_capacity(indices.len());
        for index in indices {
            let draft =
                self.tool_calls
                    .get(&index)
                    .ok_or_else(|| ChatCompletionsError::OutputShape {
                        reason: format!("stream tool_call index={index} 缺少 draft"),
                        raw: String::new(),
                    })?;
            tool_calls.push(draft.to_tool_call(index)?);
        }
        let message = ChatCompletionMessage {
            role: Some(self.role.unwrap_or_else(|| "assistant".into())),
            content: if self.content.is_empty() {
                None
            } else {
                Some(self.content)
            },
            tool_calls,
        };
        Ok(ChatCompletionResponse {
            choices: vec![ChatCompletionChoice {
                message,
                finish_reason: Some(finish_reason),
            }],
            usage: self.usage,
            model: self.model,
        })
    }
}

#[derive(Default)]
struct ToolCallDraft {
    id: Option<String>,
    kind: Option<String>,
    name: Option<String>,
    arguments: String,
}

impl ToolCallDraft {
    fn to_tool_call(&self, index: usize) -> Result<ChatToolCall, ChatCompletionsError> {
        let id = self
            .id
            .clone()
            .ok_or_else(|| ChatCompletionsError::OutputShape {
                reason: format!("stream tool_call index={index} 缺少 id"),
                raw: String::new(),
            })?;
        let name = self
            .name
            .clone()
            .ok_or_else(|| ChatCompletionsError::OutputShape {
                reason: format!("stream tool_call index={index} 缺少 function.name"),
                raw: String::new(),
            })?;
        Ok(ChatToolCall {
            id,
            kind: self.kind.clone().unwrap_or_else(|| "function".into()),
            function: ChatToolCallFunction {
                name,
                arguments: self.arguments.clone(),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_sse_frames_supports_crlf_delimiters() {
        let mut buffer = b"data: {\"a\":1}\r\n\r\ndata: {\"b\":2}\r\n\r\n".to_vec();
        let frames = drain_sse_frames(&mut buffer);

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], b"data: {\"a\":1}");
        assert_eq!(frames[1], b"data: {\"b\":2}");
        assert!(buffer.is_empty());
    }

    #[test]
    fn drain_sse_frames_supports_lf_delimiters() {
        let mut buffer = b"data: {\"a\":1}\n\ndata: {\"b\":2}\n\n".to_vec();
        let frames = drain_sse_frames(&mut buffer);

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], b"data: {\"a\":1}");
        assert_eq!(frames[1], b"data: {\"b\":2}");
        assert!(buffer.is_empty());
    }

    #[test]
    fn stream_tool_call_empty_id_delta_preserves_initial_id() {
        let mut accumulator = ChatStreamAccumulator::default();
        let mut emit = |_event| {};
        accumulator
            .apply_frame(
                r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"web_search","arguments":"{\"query\""}}]}}]}"#,
                &mut emit,
            )
            .unwrap();
        accumulator
            .apply_frame(
                r#"{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"","type":"function","function":{"arguments":":\"LA jobs\"}"}}]},"finish_reason":"tool_calls"}]}"#,
                &mut emit,
            )
            .unwrap();

        let response = accumulator.finish().unwrap();

        assert_eq!(response.choices[0].message.tool_calls[0].id, "call_abc");
        assert_eq!(
            response.choices[0].message.tool_calls[0].function.arguments,
            r#"{"query":"LA jobs"}"#
        );
    }

    #[test]
    fn stream_empty_content_delta_is_accumulated_but_not_emitted() {
        let mut accumulator = ChatStreamAccumulator::default();
        let mut events = Vec::new();
        accumulator
            .apply_frame(
                r#"{"choices":[{"delta":{"role":"assistant","content":""}}]}"#,
                &mut |event| events.push(event),
            )
            .unwrap();
        accumulator
            .apply_frame(
                r#"{"choices":[{"delta":{"content":"你好"},"finish_reason":"stop"}]}"#,
                &mut |event| events.push(event),
            )
            .unwrap();

        let response = accumulator.finish().unwrap();

        assert_eq!(response.choices[0].message.content.as_deref(), Some("你好"));
        assert_eq!(
            events,
            vec![ChatStreamEvent::ContentDelta {
                text: "你好".into()
            }]
        );
    }

    #[test]
    fn stream_response_retains_reported_model() {
        let mut accumulator = ChatStreamAccumulator::default();
        accumulator
            .apply_frame(
                r#"{"model":"actual-model","choices":[{"delta":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}"#,
                &mut |_event| {},
            )
            .unwrap();

        assert_eq!(
            accumulator.finish().unwrap().model.as_deref(),
            Some("actual-model")
        );
    }

    #[test]
    fn stream_only_empty_content_delta_finishes_without_visible_delta() {
        let mut accumulator = ChatStreamAccumulator::default();
        let mut events = Vec::new();
        accumulator
            .apply_frame(
                r#"{"choices":[{"delta":{"role":"assistant","content":""}}]}"#,
                &mut |event| events.push(event),
            )
            .unwrap();
        accumulator
            .apply_frame(
                r#"{"choices":[{"delta":{"content":""},"finish_reason":"stop"}]}"#,
                &mut |event| events.push(event),
            )
            .unwrap();

        let response = accumulator.finish().unwrap();

        assert_eq!(response.choices[0].message.content, None);
        assert!(events.is_empty());
    }

    #[test]
    fn stream_with_complete_tool_arguments_but_no_finish_reason_is_rejected() {
        let mut accumulator = ChatStreamAccumulator::default();
        accumulator
            .apply_frame(
                r#"{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"web_fetch","arguments":"{\"url\":\"https://example.com\"}"}}]}}]}"#,
                &mut |_event| {},
            )
            .unwrap();

        let error = accumulator.finish().unwrap_err();
        assert!(error.to_string().contains("finish_reason"));
    }
}
