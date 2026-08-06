//! SessionEngine transcript 与消息投影辅助。
//!
//! 本模块负责 session 落盘消息、provider turn message 与 recap/search 用纯文本
//! transcript 之间的转换。这里不执行 LLM、I/O 或提交逻辑，只保留可测试的
//! 结构转换、flatten 与最近窗口选择。

use rustc_hash::FxHashMap;

use crate::api::{
    ProviderHistoryMediaPolicy, ProviderReplayProtocol, ProviderReplayState,
    SessionTurnContentBlock, SessionTurnMessage, TurnMessage,
};
use crate::session::{SessionContentBlock, SessionMessage, SessionMessageRole};

use super::compaction_projection::validate_session_compaction_state;

pub(super) fn turn_messages_to_transcript(messages: Vec<&SessionTurnMessage>) -> Vec<TurnMessage> {
    messages
        .into_iter()
        .map(|message| TurnMessage {
            role: message.role.clone(),
            content: flatten_turn_content(&message.content),
        })
        .collect()
}

pub(super) fn flatten_turn_content(blocks: &[SessionTurnContentBlock]) -> String {
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            SessionTurnContentBlock::Text { text } => parts.push(text.clone()),
            SessionTurnContentBlock::SkillInstructions { instruction } => {
                parts.push(format!("[explicit skill /{}]", instruction.name));
            }
            SessionTurnContentBlock::Image { media_type, data } => {
                parts.push(format!(
                    "[image attachment media_type={media_type} base64_bytes={}]",
                    data.len()
                ));
            }
            SessionTurnContentBlock::Document {
                media_type,
                data,
                filename,
            } => match filename {
                Some(filename) => parts.push(format!(
                    "[document attachment media_type={media_type} filename={filename} base64_bytes={}]",
                    data.len()
                )),
                None => parts.push(format!(
                    "[document attachment media_type={media_type} base64_bytes={}]",
                    data.len()
                )),
            },
            SessionTurnContentBlock::ToolUse { name, input, .. } => {
                parts.push(format!("[tool_use {name} {input}]"));
            }
            SessionTurnContentBlock::ToolResult { content, .. } => parts.push(content.clone()),
        }
    }
    parts.join("\n")
}

pub(super) fn build_memory_review_transcript(
    metadata: &crate::session::SessionMetadata,
    messages: Vec<SessionMessage>,
    window_turns: usize,
) -> anyhow::Result<Vec<SessionTurnMessage>> {
    validate_session_compaction_state(metadata, messages.len())?;
    let start_index = memory_review_window_start_index(&messages, window_turns);
    let mut window = Vec::new();
    if let Some(compaction) = metadata
        .compaction
        .as_ref()
        .filter(|compaction| compaction.committed_message_until() > start_index)
    {
        window.push(SessionTurnMessage::assistant_text(format!(
            "Compacted prior session context:\n\n{}",
            compaction.committed_summary()
        )));
        let committed_message_until = compaction.committed_message_until();
        window.extend(
            messages
                .into_iter()
                .filter(|message| message.index >= committed_message_until)
                .map(session_message_to_turn_message),
        );
    } else {
        window.extend(
            messages
                .into_iter()
                .filter(|message| message.index >= start_index)
                .map(session_message_to_turn_message),
        );
    }
    Ok(window)
}

pub(super) fn memory_review_window_start_index(
    messages: &[SessionMessage],
    window_turns: usize,
) -> usize {
    if window_turns == 0 {
        return messages.len();
    }
    messages
        .iter()
        .rev()
        .filter(|message| is_memory_review_user_turn(message))
        .nth(window_turns.saturating_sub(1))
        .map(|message| message.index)
        .unwrap_or(0)
}

pub(super) fn is_memory_review_user_turn(message: &SessionMessage) -> bool {
    is_real_user_turn(message)
}

pub(super) fn is_real_user_turn(message: &SessionMessage) -> bool {
    message.role == SessionMessageRole::User
        && !is_user_shell_command_turn(message)
        && !message
            .content
            .iter()
            .any(|block| matches!(block, SessionContentBlock::ToolResult { .. }))
}

pub(super) fn is_user_shell_command_turn(message: &SessionMessage) -> bool {
    if message.role != SessionMessageRole::User {
        return false;
    }
    let text = session_text_from_blocks(&message.content);
    let trimmed = text.trim_start();
    trimmed.starts_with("<user_shell_command>") && trimmed.contains("</user_shell_command>")
}

pub(super) fn session_text_from_blocks(blocks: &[SessionContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            SessionContentBlock::Text { text } => Some(text.as_str()),
            SessionContentBlock::SkillInstructions { .. } => None,
            SessionContentBlock::Image { .. }
            | SessionContentBlock::Document { .. }
            | SessionContentBlock::ToolUse { .. }
            | SessionContentBlock::ToolResult { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
pub(super) fn session_messages_to_turn_messages(
    messages: Vec<SessionMessage>,
) -> Vec<SessionTurnMessage> {
    messages
        .into_iter()
        .map(session_message_to_turn_message)
        .collect()
}

pub(super) fn session_message_to_turn_message(message: SessionMessage) -> SessionTurnMessage {
    session_message_to_turn_message_with_policy(
        message,
        ProviderHistoryMediaPolicy::Placeholder,
        None,
    )
}

pub(super) fn session_messages_to_provider_turn_messages(
    messages: Vec<SessionMessage>,
    media_policy: ProviderHistoryMediaPolicy,
    replay_protocol: Option<ProviderReplayProtocol>,
) -> Vec<SessionTurnMessage> {
    messages
        .into_iter()
        .map(|message| {
            session_message_to_turn_message_with_policy(message, media_policy, replay_protocol)
        })
        .collect()
}

pub(super) fn session_message_to_provider_turn_message(
    message: SessionMessage,
    media_policy: ProviderHistoryMediaPolicy,
    replay_protocol: Option<ProviderReplayProtocol>,
) -> SessionTurnMessage {
    session_message_to_turn_message_with_policy(message, media_policy, replay_protocol)
}

fn session_message_to_turn_message_with_policy(
    message: SessionMessage,
    media_policy: ProviderHistoryMediaPolicy,
    replay_protocol: Option<ProviderReplayProtocol>,
) -> SessionTurnMessage {
    let provider_replay = replay_for_protocol(message.provider_replay, replay_protocol);
    SessionTurnMessage {
        role: message.role.to_string(),
        content: message
            .content
            .into_iter()
            .map(|block| session_block_to_turn_with_policy(block, media_policy))
            .collect(),
        provider_replay,
    }
}

fn replay_for_protocol(
    replay: Option<ProviderReplayState>,
    protocol: Option<ProviderReplayProtocol>,
) -> Option<ProviderReplayState> {
    match (protocol, replay) {
        (
            Some(ProviderReplayProtocol::OpenAiResponses),
            replay @ Some(ProviderReplayState::OpenAiResponses { .. }),
        ) => replay,
        _ => None,
    }
}

fn session_block_to_turn_with_policy(
    block: SessionContentBlock,
    media_policy: ProviderHistoryMediaPolicy,
) -> SessionTurnContentBlock {
    match block {
        SessionContentBlock::Text { text } => SessionTurnContentBlock::Text { text },
        SessionContentBlock::SkillInstructions { instruction } => {
            SessionTurnContentBlock::SkillInstructions { instruction }
        }
        SessionContentBlock::Image { media_type, data } => match media_policy {
            ProviderHistoryMediaPolicy::Placeholder => SessionTurnContentBlock::Text {
                text: format!(
                    "[image attachment media_type={media_type} base64_bytes={}]",
                    data.len()
                ),
            },
            ProviderHistoryMediaPolicy::Preserve => {
                SessionTurnContentBlock::Image { media_type, data }
            }
        },
        SessionContentBlock::Document {
            media_type,
            data,
            filename,
        } => match media_policy {
            ProviderHistoryMediaPolicy::Placeholder => {
                let text = match filename {
                    Some(filename) => format!(
                        "[document attachment media_type={media_type} filename={filename} base64_bytes={}]",
                        data.len()
                    ),
                    None => format!(
                        "[document attachment media_type={media_type} base64_bytes={}]",
                        data.len()
                    ),
                };
                SessionTurnContentBlock::Text { text }
            }
            ProviderHistoryMediaPolicy::Preserve => SessionTurnContentBlock::Document {
                media_type,
                data,
                filename,
            },
        },
        SessionContentBlock::ToolUse { id, name, input } => {
            SessionTurnContentBlock::ToolUse { id, name, input }
        }
        SessionContentBlock::ToolResult {
            tool_use_id,
            content,
        } => SessionTurnContentBlock::ToolResult {
            tool_use_id,
            content,
        },
    }
}

pub(super) fn session_messages_to_turn_transcript(messages: &[SessionMessage]) -> Vec<TurnMessage> {
    let mut tool_names_by_id = FxHashMap::default();
    for message in messages {
        for block in &message.content {
            if let SessionContentBlock::ToolUse { id, name, .. } = block {
                tool_names_by_id.insert(id.as_str(), name.as_str());
            }
        }
    }

    messages
        .iter()
        .map(|message| TurnMessage {
            role: message.role.to_string(),
            content: flatten_session_content(&message.content, &tool_names_by_id),
        })
        .collect()
}

pub(super) fn flatten_session_content(
    blocks: &[SessionContentBlock],
    tool_names_by_id: &FxHashMap<&str, &str>,
) -> String {
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            SessionContentBlock::Text { text } => parts.push(text.clone()),
            SessionContentBlock::SkillInstructions { instruction } => {
                parts.push(format!("[explicit skill /{}]", instruction.name));
            }
            SessionContentBlock::Image { media_type, data } => {
                parts.push(format!(
                    "[image attachment media_type={media_type} base64_bytes={}]",
                    data.len()
                ));
            }
            SessionContentBlock::Document {
                media_type,
                data,
                filename,
            } => match filename {
                Some(filename) => parts.push(format!(
                    "[document attachment media_type={media_type} filename={filename} base64_bytes={}]",
                    data.len()
                )),
                None => parts.push(format!(
                    "[document attachment media_type={media_type} base64_bytes={}]",
                    data.len()
                )),
            },
            SessionContentBlock::ToolUse { name, input, .. } => {
                if name == "memory" {
                    parts.push(format!(
                        "[tool_use {name} input omitted from recap transcript]"
                    ));
                } else {
                    parts.push(format!("[tool_use {name} {input}]"));
                }
            }
            SessionContentBlock::ToolResult {
                tool_use_id,
                content,
            } => {
                if tool_names_by_id
                    .get(tool_use_id.as_str())
                    .is_some_and(|name| *name == "memory")
                {
                    parts.push("[tool_result memory output omitted from recap transcript]".into());
                } else {
                    parts.push(content.clone());
                }
            }
        }
    }
    parts.join("\n")
}

pub(super) fn memory_review_should_run(messages: &[SessionMessage]) -> bool {
    messages
        .iter()
        .rev()
        .find(|message| message.role == SessionMessageRole::Assistant)
        .is_some_and(|message| {
            !flatten_session_content_lossy(&message.content)
                .trim()
                .is_empty()
        })
}

pub(super) fn session_trace_text(messages: &[SessionMessage]) -> String {
    let text = messages
        .iter()
        .filter(|message| message.role == SessionMessageRole::User)
        .map(|message| flatten_session_content_lossy(&message.content))
        .collect::<Vec<_>>()
        .join("\n");
    if text.trim().is_empty() {
        "session".into()
    } else {
        text
    }
}

pub(super) fn flatten_session_content_lossy(blocks: &[SessionContentBlock]) -> String {
    let empty = FxHashMap::default();
    flatten_session_content(blocks, &empty)
}
