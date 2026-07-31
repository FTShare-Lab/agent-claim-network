//! 上下文 token 粗估。
//!
//! 本模块只提供 provider-neutral 的本地估算口径，用于没有上游 usage 时的
//! statusline / compact fallback，以及 compact tail 切分。真实 provider usage
//! 仍由各 provider adapter 解析后通过 `ContextUsageSnapshot` 上报。

use serde_json::Value;

use super::provider::{ContextUsageSnapshot, ContextUsageSource, ToolSpec};
use super::types::{SessionTurnContentBlock, SessionTurnMessage};

const APPROX_CHARS_PER_TOKEN: usize = 4;
const APPROX_MEDIA_BLOCK_TOKENS: usize = 2_000;

pub fn estimate_text_tokens(text: &str) -> usize {
    text.chars().count().div_ceil(APPROX_CHARS_PER_TOKEN)
}

pub fn estimate_json_tokens(value: &Value) -> usize {
    estimate_text_tokens(&value.to_string())
}

pub fn estimate_provider_request_context_tokens(
    system_prompt: &str,
    messages: &[SessionTurnMessage],
    tools: &[ToolSpec],
) -> ContextUsageSnapshot {
    let used_tokens = estimate_text_tokens(system_prompt)
        .saturating_add(estimate_session_turn_messages_tokens(messages))
        .saturating_add(estimate_tool_specs_tokens(tools));
    ContextUsageSnapshot {
        used_tokens,
        source: ContextUsageSource::Estimate,
    }
}

pub fn estimate_session_turn_messages_tokens(messages: &[SessionTurnMessage]) -> usize {
    messages
        .iter()
        .map(estimate_session_turn_message_tokens)
        .fold(0usize, usize::saturating_add)
}

fn estimate_session_turn_message_tokens(message: &SessionTurnMessage) -> usize {
    estimate_text_tokens(&message.role).saturating_add(
        message
            .content
            .iter()
            .map(estimate_session_turn_content_block_tokens)
            .fold(0usize, usize::saturating_add),
    )
}

fn estimate_session_turn_content_block_tokens(block: &SessionTurnContentBlock) -> usize {
    match block {
        SessionTurnContentBlock::Text { text } => estimate_text_tokens(text),
        SessionTurnContentBlock::SkillInstructions { instruction } => {
            estimate_text_tokens(&crate::skill::render_skill_instructions(instruction))
        }
        SessionTurnContentBlock::Image { .. } | SessionTurnContentBlock::Document { .. } => {
            APPROX_MEDIA_BLOCK_TOKENS
        }
        SessionTurnContentBlock::ToolUse { name, input, .. } => {
            estimate_text_tokens(name).saturating_add(estimate_json_tokens(input))
        }
        SessionTurnContentBlock::ToolResult { content, .. } => estimate_text_tokens(content),
    }
}

fn estimate_tool_specs_tokens(tools: &[ToolSpec]) -> usize {
    tools.iter().fold(0usize, |total, tool| {
        total
            .saturating_add(estimate_text_tokens(&tool.name))
            .saturating_add(estimate_text_tokens(&tool.description))
            .saturating_add(estimate_json_tokens(&tool.input_schema))
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn local_context_estimate_counts_prompt_messages_and_tools() {
        let messages = vec![SessionTurnMessage::user_text("hello")];
        let tools = vec![ToolSpec {
            name: "t".into(),
            description: "tool".into(),
            input_schema: json!({"type":"object"}),
        }];

        let snapshot = estimate_provider_request_context_tokens("system", &messages, &tools);

        assert!(snapshot.used_tokens > 0);
    }

    #[test]
    fn local_context_estimate_counts_media_by_fixed_budget() {
        let messages = vec![SessionTurnMessage::user_content(vec![
            SessionTurnContentBlock::text("hello"),
            SessionTurnContentBlock::image("image/png", "AAAA"),
            SessionTurnContentBlock::document("application/pdf", "BBBB"),
        ])];

        let snapshot = estimate_provider_request_context_tokens("system", &messages, &[]);

        assert!(snapshot.used_tokens >= APPROX_MEDIA_BLOCK_TOKENS * 2);
        assert_eq!(snapshot.source, ContextUsageSource::Estimate);
    }
}
