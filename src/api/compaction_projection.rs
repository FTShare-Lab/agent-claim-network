//! provider history 压缩投影的通用纯逻辑。
//!
//! 本模块只处理 `SessionTurnMessage` 层面的安全切段、tool_result 投影和
//! token 估算，不读写 session/delegation，也不理解 recap 或 claim 语义。

use serde::Serialize;

use super::{estimate_session_turn_messages_tokens, SessionTurnContentBlock, SessionTurnMessage};

const STABLE_HASH_OFFSET: u64 = 0xcbf29ce484222325;
const STABLE_HASH_PRIME: u64 = 0x100000001b3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderProjectionBudget {
    pub tail_token_limit: usize,
    pub tail_hard_token_limit: usize,
    pub tail_previous_real_user_turns: usize,
    pub tool_result_raw_max_chars: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageRange {
    pub start: usize,
    pub end: usize,
}

pub fn project_turn_message_tool_results(
    mut message: SessionTurnMessage,
    tool_result_raw_max_chars: usize,
) -> SessionTurnMessage {
    for block in &mut message.content {
        if let SessionTurnContentBlock::ToolResult { content, .. } = block {
            if content.chars().count() > tool_result_raw_max_chars {
                *content = large_tool_result_omission_text(content.chars().count());
            }
        }
    }
    message
}

pub fn project_turn_messages_tool_results(
    messages: impl IntoIterator<Item = SessionTurnMessage>,
    tool_result_raw_max_chars: usize,
) -> Vec<SessionTurnMessage> {
    messages
        .into_iter()
        .map(|message| project_turn_message_tool_results(message, tool_result_raw_max_chars))
        .collect()
}

pub fn large_tool_result_omission_text(original_chars: usize) -> String {
    format!(
        "[large tool_result omitted from raw compact tail; original_chars={original_chars}. The compaction summary keeps the key facts. Re-call the tool if exact output is needed.]"
    )
}

pub fn provider_safe_segments(active_suffix: &[SessionTurnMessage]) -> Vec<MessageRange> {
    let mut ranges = Vec::new();
    let mut index = 1;
    while index < active_suffix.len() {
        let message = &active_suffix[index];
        if message.role == "assistant" {
            let tool_use_ids = message
                .content
                .iter()
                .filter_map(|block| match block {
                    SessionTurnContentBlock::ToolUse { id, .. } => Some(id.as_str()),
                    SessionTurnContentBlock::Text { .. }
                    | SessionTurnContentBlock::SkillInstructions { .. }
                    | SessionTurnContentBlock::Image { .. }
                    | SessionTurnContentBlock::Document { .. }
                    | SessionTurnContentBlock::ToolResult { .. } => None,
                })
                .collect::<Vec<_>>();
            if tool_use_ids.is_empty() {
                ranges.push(MessageRange {
                    start: index,
                    end: index + 1,
                });
                index += 1;
                continue;
            }
            let Some(tool_result) = active_suffix.get(index + 1) else {
                break;
            };
            if tool_result.role != "user" || !tool_result_contains_all(tool_result, &tool_use_ids) {
                break;
            }
            ranges.push(MessageRange {
                start: index,
                end: index + 2,
            });
            index += 2;
            continue;
        }

        if message.role == "user" && !message_contains_tool_result(message) {
            ranges.push(MessageRange {
                start: index,
                end: index + 1,
            });
            index += 1;
            continue;
        }

        break;
    }
    ranges
}

pub fn tool_result_contains_all(message: &SessionTurnMessage, tool_use_ids: &[&str]) -> bool {
    tool_use_ids.iter().all(|id| {
        message.content.iter().any(|block| {
            matches!(
                block,
                SessionTurnContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == id
            )
        })
    })
}

pub fn message_contains_tool_result(message: &SessionTurnMessage) -> bool {
    message
        .content
        .iter()
        .any(|block| matches!(block, SessionTurnContentBlock::ToolResult { .. }))
}

pub fn active_segment_messages<'a>(
    active_suffix: &'a [SessionTurnMessage],
    segments: &[MessageRange],
) -> Vec<&'a SessionTurnMessage> {
    segments
        .iter()
        .flat_map(|range| active_suffix[range.start..range.end].iter())
        .collect()
}

pub fn active_segments_hash(
    active_suffix: &[SessionTurnMessage],
    segments: &[MessageRange],
) -> anyhow::Result<String> {
    let mut hash = STABLE_HASH_OFFSET;
    for message in active_segment_messages(active_suffix, segments) {
        stable_hash_json(&mut hash, message)?;
    }
    Ok(format!("{hash:016x}"))
}

pub fn estimated_projected_segment_tokens(
    active_suffix: &[SessionTurnMessage],
    segment: &MessageRange,
    tool_result_raw_max_chars: usize,
) -> usize {
    let projected = active_suffix[segment.start..segment.end]
        .iter()
        .cloned()
        .map(|message| project_turn_message_tool_results(message, tool_result_raw_max_chars))
        .collect::<Vec<_>>();
    estimate_session_turn_messages_tokens(&projected)
}

pub fn active_segment_has_large_tool_result(
    active_suffix: &[SessionTurnMessage],
    segment: &MessageRange,
    tool_result_raw_max_chars: usize,
) -> bool {
    active_suffix[segment.start..segment.end]
        .iter()
        .flat_map(|message| message.content.iter())
        .any(|block| {
            matches!(
                block,
                SessionTurnContentBlock::ToolResult { content, .. }
                    if content.chars().count() > tool_result_raw_max_chars
            )
        })
}

fn stable_hash_json<T: Serialize>(hash: &mut u64, value: &T) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(value)?;
    stable_hash_update(hash, &bytes);
    stable_hash_update(hash, b"\n");
    Ok(())
}

fn stable_hash_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(STABLE_HASH_PRIME);
    }
}
