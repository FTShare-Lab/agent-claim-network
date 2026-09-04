//! 将 session transcript 渲染为 session_search 的派生索引文本。

use std::collections::HashMap;

use crate::session::{SessionContentBlock, SessionMessage};

use super::types::SessionSearchMessage;

const MAX_TEXT_BLOCK_CHARS: usize = 4_000;
const MAX_TOOL_RESULT_CHARS: usize = 4_000;

pub(crate) fn searchable_texts_for_messages(messages: &[SessionMessage]) -> Vec<String> {
    let mut tool_names = HashMap::new();
    messages
        .iter()
        .map(|message| searchable_text_for_message(message, &mut tool_names))
        .collect()
}

fn searchable_text_for_message(
    message: &SessionMessage,
    tool_names: &mut HashMap<String, String>,
) -> String {
    message
        .content
        .iter()
        .map(|block| searchable_text_for_block(block, tool_names))
        .filter(|text| !text.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn searchable_text_for_block(
    block: &SessionContentBlock,
    tool_names: &mut HashMap<String, String>,
) -> String {
    match block {
        SessionContentBlock::Text { text } => text.clone(),
        SessionContentBlock::ModelContext { .. } => String::new(),
        SessionContentBlock::SkillInstructions { instruction } => {
            format!("[explicit skill /{}]", instruction.name)
        }
        SessionContentBlock::Image { media_type, data } => {
            format!(
                "[image attachment media_type={media_type} base64_bytes={}]",
                data.len()
            )
        }
        SessionContentBlock::Document {
            media_type,
            data,
            filename,
        } => match filename {
            Some(filename) => format!(
                "[document attachment media_type={media_type} filename={filename} base64_bytes={}]",
                data.len()
            ),
            None => format!(
                "[document attachment media_type={media_type} base64_bytes={}]",
                data.len()
            ),
        },
        SessionContentBlock::ToolUse { id, name, input } => {
            tool_names.insert(id.clone(), name.clone());
            format!("[tool_use {name} {input}]")
        }
        SessionContentBlock::InvalidToolUse { id, name, error } => {
            tool_names.insert(id.clone(), name.clone());
            format!("[invalid_tool_use {name} {id}] {error}")
        }
        SessionContentBlock::ToolResult {
            tool_use_id,
            content,
        } => match tool_names.get(tool_use_id) {
            Some(name) => format!("[tool_result {name} {tool_use_id}] {content}"),
            None => format!("[tool_result {tool_use_id}] {content}"),
        },
    }
}

pub(crate) fn evidence_message_for_session_message(
    message: &SessionMessage,
    tool_names: &HashMap<String, String>,
    include_tool_results: bool,
    anchor: bool,
) -> SessionSearchMessage {
    let mut parts = Vec::new();
    let mut truncated = false;
    let mut tool_results_omitted = 0;
    for block in &message.content {
        match evidence_text_for_block(block, tool_names, include_tool_results) {
            EvidenceBlock::Text {
                text,
                was_truncated,
            } => {
                if !text.trim().is_empty() {
                    parts.push(text);
                }
                truncated |= was_truncated;
            }
            EvidenceBlock::OmittedToolResult { marker } => {
                parts.push(marker);
                tool_results_omitted += 1;
            }
        }
    }
    SessionSearchMessage {
        index: message.index,
        role: message.role.to_string(),
        model: message.model.clone(),
        content: parts.join("\n"),
        anchor,
        tool_results_omitted,
        truncated,
    }
}

pub(crate) fn tool_name_map(messages: &[SessionMessage]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for message in messages {
        for block in &message.content {
            match block {
                SessionContentBlock::ToolUse { id, name, .. }
                | SessionContentBlock::InvalidToolUse { id, name, .. } => {
                    out.insert(id.clone(), name.clone());
                }
                _ => {}
            }
        }
    }
    out
}

pub(crate) fn first_user_preview(messages: &[SessionMessage], max_chars: usize) -> String {
    let tool_names = tool_name_map(messages);
    messages
        .iter()
        .filter(|message| message.role.to_string() == "user" && !message_has_tool_results(message))
        .filter_map(|message| {
            let content =
                evidence_message_for_session_message(message, &tool_names, false, false).content;
            if content.trim().is_empty() {
                None
            } else {
                Some(content)
            }
        })
        .next()
        .map(|text| truncate_chars(&text, max_chars).0)
        .unwrap_or_default()
}

pub(crate) fn message_has_tool_results(message: &SessionMessage) -> bool {
    message
        .content
        .iter()
        .any(|block| matches!(block, SessionContentBlock::ToolResult { .. }))
}

enum EvidenceBlock {
    Text { text: String, was_truncated: bool },
    OmittedToolResult { marker: String },
}

fn evidence_text_for_block(
    block: &SessionContentBlock,
    tool_names: &HashMap<String, String>,
    include_tool_results: bool,
) -> EvidenceBlock {
    match block {
        SessionContentBlock::Text { text } => {
            let (text, was_truncated) = truncate_chars(text, MAX_TEXT_BLOCK_CHARS);
            EvidenceBlock::Text {
                text,
                was_truncated,
            }
        }
        SessionContentBlock::ModelContext { .. } => EvidenceBlock::Text {
            text: String::new(),
            was_truncated: false,
        },
        SessionContentBlock::SkillInstructions { instruction } => EvidenceBlock::Text {
            text: format!("[explicit skill /{}]", instruction.name),
            was_truncated: false,
        },
        SessionContentBlock::Image { media_type, .. } => EvidenceBlock::Text {
            text: format!("[image attachment media_type={media_type}]"),
            was_truncated: false,
        },
        SessionContentBlock::Document {
            media_type,
            filename,
            ..
        } => EvidenceBlock::Text {
            text: match filename {
                Some(filename) => {
                    format!("[document attachment media_type={media_type} filename={filename}]")
                }
                None => format!("[document attachment media_type={media_type}]"),
            },
            was_truncated: false,
        },
        SessionContentBlock::ToolUse { id, name, input } => {
            let raw = format!("[tool_use {name} {id}] {input}");
            let (text, was_truncated) = truncate_chars(&raw, MAX_TEXT_BLOCK_CHARS);
            EvidenceBlock::Text {
                text,
                was_truncated,
            }
        }
        SessionContentBlock::InvalidToolUse { id, name, error } => EvidenceBlock::Text {
            text: format!("[invalid_tool_use {name} {id}] {error}"),
            was_truncated: false,
        },
        SessionContentBlock::ToolResult {
            tool_use_id,
            content,
        } => {
            let tool_name = tool_names
                .get(tool_use_id)
                .map(String::as_str)
                .unwrap_or("unknown");
            if !include_tool_results {
                return EvidenceBlock::OmittedToolResult {
                    marker: format!("[tool_result {tool_name} {tool_use_id} omitted]"),
                };
            }
            let raw = format!("[tool_result {tool_name} {tool_use_id}] {content}");
            let (text, was_truncated) = truncate_chars(&raw, MAX_TOOL_RESULT_CHARS);
            EvidenceBlock::Text {
                text,
                was_truncated,
            }
        }
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    for (count, ch) in text.chars().enumerate() {
        if count >= max_chars {
            out.push_str("\n[truncated]");
            return (out, true);
        }
        out.push(ch);
    }
    (out, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ModelContextSource;
    use crate::session::{SessionContentBlock, SessionMessageRole};
    use chrono::Utc;

    #[test]
    fn searchable_text_replaces_media_with_placeholder_without_base64() {
        let huge_base64 = "QUJD".repeat(10_000);
        let message = SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![
                SessionContentBlock::image("image/png", huge_base64.clone()),
                SessionContentBlock::Document {
                    media_type: "application/pdf".into(),
                    data: huge_base64.clone(),
                    filename: Some("brief.pdf".into()),
                },
            ],
            created_at: Utc::now(),
            model: "test-model".into(),
            provider_replay: None,
        };
        let text = searchable_texts_for_messages(&[message]).remove(0);
        assert!(text.contains("[image attachment media_type=image/png"));
        assert!(text.contains("filename=brief.pdf"));
        assert!(!text.contains(&huge_base64));
    }

    #[test]
    fn searchable_text_includes_tool_result_content() {
        let message = SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![
                SessionContentBlock::text("hello"),
                SessionContentBlock::tool_result("toolu_1", "docker networking clue"),
            ],
            created_at: Utc::now(),
            model: "test-model".into(),
            provider_replay: None,
        };
        let text = searchable_texts_for_messages(&[message]).remove(0);
        assert!(text.contains("hello"));
        assert!(text.contains("docker networking clue"));
    }

    #[test]
    fn searchable_text_adds_tool_name_to_matching_tool_result() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::tool_use(
                    "toolu_1",
                    "session_search",
                    serde_json::json!({"query":"docker"}),
                )],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::tool_result("toolu_1", "docker clue")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        let rendered = searchable_texts_for_messages(&messages);
        assert!(rendered[1].contains("tool_result session_search toolu_1"));
        assert!(rendered[1].contains("docker clue"));
    }

    #[test]
    fn evidence_message_omits_tool_result_by_default() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::tool_use(
                    "toolu_1",
                    "code_run",
                    serde_json::json!({"cmd":"cargo test"}),
                )],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::tool_result("toolu_1", "long output")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];
        let tool_names = tool_name_map(&messages);
        let rendered = evidence_message_for_session_message(&messages[1], &tool_names, false, true);

        assert_eq!(rendered.tool_results_omitted, 1);
        assert!(rendered
            .content
            .contains("tool_result code_run toolu_1 omitted"));
        assert!(!rendered.content.contains("long output"));
        assert!(rendered.anchor);
    }

    #[test]
    fn first_user_preview_skips_empty_user_messages() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("   ")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("useful preview")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        assert_eq!(first_user_preview(&messages, 100), "useful preview");
    }

    #[test]
    fn model_context_is_not_searchable_or_used_as_first_user_preview() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::ModelContext {
                    source: ModelContextSource::Runtime,
                    fingerprint: "sha256-v1:test".into(),
                    text: "hidden runtime needle".into(),
                }],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("real user preview")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        let rendered = searchable_texts_for_messages(&messages);
        assert!(rendered[0].is_empty());
        assert!(!rendered.join("\n").contains("hidden runtime needle"));
        assert_eq!(first_user_preview(&messages, 100), "real user preview");
    }

    #[test]
    fn first_user_preview_skips_tool_callback_with_media_blocks() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![
                    SessionContentBlock::tool_result("toolu_1", "Read image file"),
                    SessionContentBlock::text("[file_read attachment] image.png"),
                    SessionContentBlock::image("image/png", "QUJD"),
                ],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("real user preview")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        assert_eq!(first_user_preview(&messages, 100), "real user preview");
    }
}
