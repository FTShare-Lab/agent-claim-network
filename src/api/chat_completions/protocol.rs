use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::ReasoningEffort;

#[derive(Debug, Clone, Serialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ChatTool>>,
    pub max_tokens: u32,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<ChatStreamOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub content: Option<ChatMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tool_calls: Option<Vec<ChatToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tool_call_id: Option<String>,
}

/// Chat Completions 的 message content：纯文本沿用字符串形态（最大兼容），
/// 携带图片 / 文档时用 content parts 数组。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ChatMessageContent {
    Text(String),
    Parts(Vec<ChatContentPart>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatContentPart {
    Text { text: String },
    ImageUrl { image_url: ChatImageUrl },
    File { file: ChatFileData },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatImageUrl {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatFileData {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub filename: Option<String>,
    pub file_data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatStreamOptions {
    pub include_usage: bool,
}

impl ChatContentPart {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    /// 以 data URL 形式内联图片（统一 base64，不走文件上传）。
    pub fn image_data_url(media_type: &str, base64_data: &str) -> Self {
        Self::ImageUrl {
            image_url: ChatImageUrl {
                url: format!("data:{media_type};base64,{base64_data}"),
            },
        }
    }

    /// 以 data URL 形式内联文档（PDF）。
    pub fn file_data_url(filename: Option<String>, media_type: &str, base64_data: &str) -> Self {
        Self::File {
            file: ChatFileData {
                filename,
                file_data: format!("data:{media_type};base64,{base64_data}"),
            },
        }
    }
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: "system".into(),
            content: Some(ChatMessageContent::Text(content.into())),
            tool_calls: None,
            tool_call_id: None,
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: "user".into(),
            content: Some(ChatMessageContent::Text(content.into())),
            tool_calls: None,
            tool_call_id: None,
        }
    }

    pub fn user_parts(parts: Vec<ChatContentPart>) -> Self {
        Self {
            role: "user".into(),
            content: Some(ChatMessageContent::Parts(parts)),
            tool_calls: None,
            tool_call_id: None,
        }
    }

    pub fn assistant(content: Option<String>, tool_calls: Vec<ChatToolCall>) -> Self {
        Self {
            role: "assistant".into(),
            content: content.map(ChatMessageContent::Text),
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            },
            tool_call_id: None,
        }
    }

    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: "tool".into(),
            content: Some(ChatMessageContent::Text(content.into())),
            tool_calls: None,
            tool_call_id: Some(tool_call_id.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: ChatToolCallFunction,
}

impl ChatToolCall {
    pub fn function(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            kind: "function".into(),
            function: ChatToolCallFunction {
                name: name.into(),
                arguments: arguments.into(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatToolCallFunction {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatTool {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: ChatToolFunction,
}

impl ChatTool {
    pub fn function(name: String, description: String, parameters: Value) -> Self {
        Self {
            kind: "function".into(),
            function: ChatToolFunction {
                name,
                description,
                parameters,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatToolFunction {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionResponse {
    #[serde(default)]
    pub choices: Vec<ChatCompletionChoice>,
    #[serde(default)]
    pub usage: Option<Value>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionChoice {
    pub message: ChatCompletionMessage,
    #[serde(default)]
    pub finish_reason: Option<ChatFinishReason>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionMessage {
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub tool_calls: Vec<ChatToolCall>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatFinishReason {
    Stop,
    ToolCalls,
    Length,
    ContentFilter,
    FunctionCall,
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChatStreamFrame {
    #[serde(default)]
    pub choices: Vec<ChatStreamChoice>,
    #[serde(default)]
    pub usage: Option<Value>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChatStreamChoice {
    #[serde(default)]
    pub delta: ChatStreamDelta,
    #[serde(default)]
    pub finish_reason: Option<ChatFinishReason>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct ChatStreamDelta {
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub tool_calls: Vec<ChatStreamToolCallDelta>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChatStreamToolCallDelta {
    #[serde(default)]
    pub index: Option<usize>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub function: Option<ChatStreamToolCallFunctionDelta>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChatStreamToolCallFunctionDelta {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub arguments: Option<String>,
}
