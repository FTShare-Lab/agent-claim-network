//! Anthropic Messages API 的协议结构和纯解析逻辑。
//!
//! 本模块只放 request/response DTO 与 content block 解析。
//! 它不持有 HTTP client，也不执行工具，避免和主流程互相缠绕。

use serde::Serialize;
use serde_json::Value;

use crate::config::ReasoningEffort;

#[derive(Debug, Serialize)]
pub(super) struct CreateMessageRequest {
    pub(super) model: String,
    pub(super) max_tokens: u32,
    pub(super) messages: Vec<ApiMessage>,
    pub(super) system: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) output_config: Option<ApiOutputConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) tools: Option<Vec<ApiToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) stream: Option<bool>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct ApiOutputConfig {
    pub(super) effort: ReasoningEffort,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ApiMessage {
    pub(super) role: String,
    pub(super) content: ApiContent,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(super) enum ApiContent {
    Text(String),
    Blocks(Vec<Value>),
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ApiToolDefinition {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) input_schema: Value,
}

pub(super) struct ContinuedAssistantTurn {
    pub(super) final_response: Value,
    pub(super) final_blocks: Vec<Value>,
    pub(super) final_stop_reason: String,
    pub(super) merged_text: String,
}

pub(super) fn extract_text_block(v: &Value) -> Option<&str> {
    v.get("content")?.as_array()?.iter().find_map(|block| {
        let block_type = block.get("type")?.as_str()?;
        if block_type == "text" {
            block.get("text")?.as_str()
        } else {
            None
        }
    })
}

pub(super) fn content_blocks(v: &Value) -> Option<Vec<Value>> {
    Some(v.get("content")?.as_array()?.clone())
}

pub(super) fn has_tool_use_block(blocks: &[Value]) -> bool {
    blocks
        .iter()
        .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
}
