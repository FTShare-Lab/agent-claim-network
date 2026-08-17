use std::collections::HashSet;

use serde::Serialize;
use serde_json::Value;

use super::{redact_responses_error_body, ResponsesError};

#[derive(Debug, Clone, Serialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub instructions: String,
    pub input: Vec<Value>,
    pub tools: Vec<ResponsesTool>,
    pub max_output_tokens: u32,
    pub stream: bool,
    pub store: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ResponsesReasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResponsesReasoning {
    pub effort: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResponsesTool {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub description: String,
    pub parameters: Value,
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponsesFunctionCall {
    pub call_id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponsesTerminal {
    Completed,
    MaxOutputTokens,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducedResponses {
    pub response_id: Option<String>,
    pub output_items: Vec<Value>,
    pub output_text: String,
    pub function_calls: Vec<ResponsesFunctionCall>,
    pub usage: Option<Value>,
    pub terminal: ResponsesTerminal,
}

pub fn reduce_response_value(value: Value) -> Result<ReducedResponses, ResponsesError> {
    let object = value
        .as_object()
        .ok_or_else(|| shape_error("response 不是 JSON object"))?;
    let status = object
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| shape_error("response 缺少 status"))?;
    let terminal = match status {
        "completed" => ResponsesTerminal::Completed,
        "incomplete" => {
            let reason = object
                .get("incomplete_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if reason == "max_output_tokens" {
                ResponsesTerminal::MaxOutputTokens
            } else {
                return Err(ResponsesError::Incomplete {
                    reason: redact_responses_error_body(reason),
                });
            }
        }
        "failed" => {
            return Err(ResponsesError::Failed {
                message: response_error_message(object.get("error")),
            });
        }
        other => {
            return Err(shape_error(format!(
                "response.status 不支持或尚未到达终态: {other}"
            )));
        }
    };
    let output_items = object
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| shape_error("response 缺少 output array"))?;
    let mut output_text = String::new();
    let mut function_calls = Vec::new();
    let mut call_ids = HashSet::new();
    for (index, item) in output_items.iter().enumerate() {
        let item_object = item
            .as_object()
            .ok_or_else(|| shape_error(format!("output[{index}] 不是 object")))?;
        let kind = item_object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| shape_error(format!("output[{index}] 缺少 type")))?;
        match kind {
            "message" => {
                validate_consumable_item_status(terminal, index, item_object)?;
                reduce_message_item(index, item_object, &mut output_text)?;
            }
            "function_call" => {
                validate_consumable_item_status(terminal, index, item_object)?;
                let call_id = required_item_string(item_object, index, "call_id")?;
                if !call_ids.insert(call_id.clone()) {
                    return Err(shape_error(format!(
                        "output[{index}] function_call.call_id 重复: {call_id}"
                    )));
                }
                function_calls.push(ResponsesFunctionCall {
                    call_id,
                    name: required_item_string(item_object, index, "name")?,
                    arguments: required_item_string(item_object, index, "arguments")?,
                });
            }
            // reasoning 与未知 item 都由 raw output_items 原样保存；上层只消费 text/tool。
            _ => {}
        }
    }
    Ok(ReducedResponses {
        response_id: object
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .map(str::to_string),
        output_items,
        output_text,
        function_calls,
        usage: object
            .get("usage")
            .filter(|value| !value.is_null())
            .cloned(),
        terminal,
    })
}

fn validate_consumable_item_status(
    terminal: ResponsesTerminal,
    index: usize,
    item: &serde_json::Map<String, Value>,
) -> Result<(), ResponsesError> {
    if terminal != ResponsesTerminal::Completed {
        return Ok(());
    }
    let Some(status) = item.get("status") else {
        // 兼容省略 item status 的 Responses-compatible 实现。
        return Ok(());
    };
    let status = status
        .as_str()
        .ok_or_else(|| shape_error(format!("output[{index}].status 不是 string")))?;
    if status != "completed" {
        return Err(shape_error(format!(
            "completed response 包含未完成的 output[{index}] item: status={status}"
        )));
    }
    Ok(())
}

fn reduce_message_item(
    item_index: usize,
    item: &serde_json::Map<String, Value>,
    output_text: &mut String,
) -> Result<(), ResponsesError> {
    let role = item
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(|| shape_error(format!("output[{item_index}] message 缺少 role")))?;
    if role != "assistant" {
        return Err(shape_error(format!(
            "output[{item_index}] message.role 必须是 assistant"
        )));
    }
    let content = item
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| shape_error(format!("output[{item_index}] message 缺少 content array")))?;
    for (content_index, part) in content.iter().enumerate() {
        let part_object = part.as_object().ok_or_else(|| {
            shape_error(format!(
                "output[{item_index}].content[{content_index}] 不是 object"
            ))
        })?;
        let kind = part_object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                shape_error(format!(
                    "output[{item_index}].content[{content_index}] 缺少 type"
                ))
            })?;
        let visible_text = match kind {
            "output_text" => Some(("text", "output_text")),
            "refusal" => Some(("refusal", "refusal")),
            _ => None,
        };
        if let Some((field, label)) = visible_text {
            let text = part_object
                .get(field)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    shape_error(format!(
                        "output[{item_index}].content[{content_index}] {label} 缺少 {field}"
                    ))
                })?;
            output_text.push_str(text);
        }
    }
    Ok(())
}

fn required_item_string(
    item: &serde_json::Map<String, Value>,
    index: usize,
    field: &str,
) -> Result<String, ResponsesError> {
    item.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| shape_error(format!("output[{index}] function_call 缺少 {field}")))
}

fn response_error_message(error: Option<&Value>) -> String {
    let message = error
        .and_then(Value::as_object)
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("upstream response failed");
    redact_responses_error_body(message)
}

fn shape_error(reason: impl Into<String>) -> ResponsesError {
    ResponsesError::OutputShape {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn reducer_projects_text_tools_and_preserves_all_raw_items() {
        let value = json!({
            "status": "completed",
            "output": [
                {"type":"reasoning","id":"rs_1","encrypted_content":"opaque","future":true},
                {"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[
                    {"type":"output_text","text":"hello","annotations":[],"future_part":1}
                ]},
                {"type":"function_call","id":"fc_1","call_id":"call_1","name":"file_read","arguments":"{\"path\":\"README.md\"}","status":"completed"},
                {"type":"future_observation","payload":{"x":1}}
            ],
            "usage": {"total_tokens": 42}
        });

        let reduced = reduce_response_value(value.clone()).unwrap();

        assert_eq!(reduced.output_text, "hello");
        assert_eq!(reduced.function_calls.len(), 1);
        assert_eq!(reduced.function_calls[0].call_id, "call_1");
        assert_eq!(
            reduced.output_items,
            value["output"].as_array().unwrap().clone()
        );
        assert_eq!(reduced.usage, Some(json!({"total_tokens":42})));
        assert_eq!(reduced.terminal, ResponsesTerminal::Completed);
    }

    #[test]
    fn reducer_projects_refusal_as_visible_assistant_text() {
        let refusal = json!({
            "type":"message",
            "id":"msg_refusal",
            "role":"assistant",
            "status":"completed",
            "content":[{"type":"refusal","refusal":"I cannot help with that."}]
        });
        let reduced = reduce_response_value(json!({
            "status":"completed",
            "output":[refusal.clone()]
        }))
        .unwrap();

        assert_eq!(reduced.output_text, "I cannot help with that.");
        assert_eq!(reduced.output_items, vec![refusal]);
        assert_eq!(reduced.terminal, ResponsesTerminal::Completed);
    }

    #[test]
    fn reducer_accepts_max_output_tokens_incomplete() {
        let reduced = reduce_response_value(json!({
            "status":"incomplete",
            "incomplete_details":{"reason":"max_output_tokens"},
            "output":[{"type":"message","role":"assistant","status":"incomplete","content":[{"type":"output_text","text":"partial"}]}]
        }))
        .unwrap();

        assert_eq!(reduced.output_text, "partial");
        assert_eq!(reduced.terminal, ResponsesTerminal::MaxOutputTokens);
    }

    #[test]
    fn reducer_rejects_other_incomplete_reason_and_failed_status() {
        let incomplete = reduce_response_value(json!({
            "status":"incomplete",
            "incomplete_details":{"reason":"content_filter"},
            "output":[]
        }))
        .unwrap_err();
        assert!(matches!(
            incomplete,
            ResponsesError::Incomplete { ref reason } if reason == "content_filter"
        ));

        let failed = reduce_response_value(json!({
            "status":"failed",
            "error":{"message":"request rejected"},
            "output":[]
        }))
        .unwrap_err();
        assert!(matches!(
            failed,
            ResponsesError::Failed { ref message } if message == "request rejected"
        ));
    }

    #[test]
    fn reducer_requires_assistant_role_for_message_items() {
        for item in [
            json!({"type":"message","content":[]}),
            json!({"type":"message","role":"user","content":[]}),
        ] {
            let error = reduce_response_value(json!({
                "status":"completed",
                "output":[item]
            }))
            .unwrap_err();
            assert!(error.to_string().contains("role"));
        }
    }

    #[test]
    fn reducer_rejects_explicitly_unfinished_consumable_items_in_completed_response() {
        for item in [
            json!({
                "type":"message","role":"assistant","status":"in_progress",
                "content":[{"type":"output_text","text":"partial"}]
            }),
            json!({
                "type":"function_call","status":"incomplete","call_id":"call_1",
                "name":"file_write","arguments":"{}"
            }),
        ] {
            let error = reduce_response_value(json!({
                "status":"completed",
                "output":[item]
            }))
            .unwrap_err();

            assert!(error.to_string().contains("未完成"));
        }
    }

    #[test]
    fn request_omits_reasoning_when_not_configured_and_flattens_tools() {
        let request = ResponsesRequest {
            model: "test-model".into(),
            instructions: "system".into(),
            input: vec![json!({"role":"user","content":[{"type":"input_text","text":"hi"}]})],
            tools: vec![ResponsesTool {
                kind: "function".into(),
                name: "file_read".into(),
                description: "Read a file".into(),
                parameters: json!({"type":"object"}),
                strict: false,
            }],
            max_output_tokens: 1024,
            stream: true,
            store: false,
            include: None,
            reasoning: None,
            temperature: None,
            top_p: None,
        };

        let value = serde_json::to_value(request).unwrap();

        assert_eq!(value["store"], false);
        assert!(value.get("reasoning").is_none());
        assert_eq!(value["tools"][0]["strict"], false);
        assert_eq!(value["tools"][0]["name"], "file_read");
        assert!(value["tools"][0].get("function").is_none());
    }
}
