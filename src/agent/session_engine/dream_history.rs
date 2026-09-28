//! 当前 Dream 的不可覆盖历史快照与分页查询；历史读取不生成证据收据。
use super::claim_context::content_hash;
use crate::api::{project_turn_message_for_safe_transcript, SessionTurnMessage};
use crate::storage::write_text_atomic_if_unchanged;
use crate::tool::ToolDefinition;
use anyhow::Context;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;

pub(super) struct History {
    root: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    archive_id: Option<String>,
    message_index: Option<usize>,
    keyword: Option<String>,
    tool_call_id: Option<String>,
    start: Option<usize>,
    count: Option<usize>,
    char_offset: Option<usize>,
    max_chars: Option<usize>,
}

pub(super) fn definition() -> ToolDefinition {
    ToolDefinition {
        name: "dream_read_history".into(),
        description: "Read this Dream's archived context. Omit archive_id to list snapshots; provide archive_id to search messages by keyword/tool_call_id with 1-based start/count pagination; add 1-based message_index for exact message text with 0-based char_offset/max_chars pagination. Follow next_start/next_char_offset. Archives are historical data, not current draft or new evidence. Use dream_read_draft for accepted changes; reread changed files for new evidence.".into(),
        input_schema: json!({"type":"object","properties":{
            "archive_id":{"type":"string"},"message_index":{"type":"integer","minimum":1},
            "keyword":{"type":"string"},"tool_call_id":{"type":"string"},
            "start":{"type":"integer","minimum":1},"count":{"type":"integer","minimum":1,"maximum":20},
            "char_offset":{"type":"integer","minimum":0},"max_chars":{"type":"integer","minimum":1,"maximum":16000}
        },"additionalProperties":false}),
    }
}

impl History {
    pub(super) fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub(super) async fn archive(&self, messages: &[SessionTurnMessage]) -> anyhow::Result<String> {
        let id = content_hash(&messages)?;
        write_text_atomic_if_unchanged(
            &self.root.join(format!("{id}.json")),
            &serde_json::to_vec(&messages)?,
            None,
        )
        .await?;
        Ok(id)
    }

    pub(super) async fn read(&self, input: Value) -> anyhow::Result<Value> {
        let query: Query = serde_json::from_value(input)?;
        let start = query.start.unwrap_or(1);
        let count = query.count.unwrap_or(10);
        let max_chars = query.max_chars.unwrap_or(8000);
        anyhow::ensure!(
            start > 0 && (1..=20).contains(&count) && (1..=16000).contains(&max_chars),
            "Invalid history page bounds"
        );
        let Some(id) = query.archive_id else {
            let mut ids = Vec::new();
            match tokio::fs::read_dir(&self.root).await {
                Ok(mut entries) => {
                    while let Some(entry) = entries.next_entry().await? {
                        if let Some(id) = entry
                            .path()
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .filter(|s| valid_id(s))
                        {
                            ids.push(id.to_owned());
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            ids.sort();
            let next = start.saturating_sub(1).saturating_add(count);
            return Ok(
                json!({"archives":ids.iter().skip(start-1).take(count).collect::<Vec<_>>(),"total":ids.len(),"next_start":(next < ids.len()).then_some(next+1),"note":"Only this job and input version are accessible. Older snapshots remain available after repeated compaction."}),
            );
        };
        anyhow::ensure!(valid_id(&id), "Invalid history archive_id");
        let messages: Vec<SessionTurnMessage> =
            serde_json::from_slice(&tokio::fs::read(self.root.join(format!("{id}.json"))).await?)?;
        if let Some(index) = query.message_index {
            let message = index
                .checked_sub(1)
                .and_then(|i| messages.get(i))
                .context("Unknown history message_index")?;
            let text = message_text(message)?;
            let total = text.chars().count();
            let offset = query.char_offset.unwrap_or(0);
            anyhow::ensure!(offset <= total, "char_offset exceeds message length");
            let content: String = text.chars().skip(offset).take(max_chars).collect();
            let end = offset + content.chars().count();
            return Ok(
                json!({"archive_id":id,"message_index":index,"content":content,"total_chars":total,"next_char_offset":(end < total).then_some(end),"historical_only":true}),
            );
        }
        let mut matches = Vec::new();
        for (index, message) in messages.iter().enumerate() {
            let text = message_text(message)?;
            if query.keyword.as_ref().is_some_and(|q| !text.contains(q))
                || query.tool_call_id.as_ref().is_some_and(|q| {
                    !message.content.iter().any(|block| match block {
                        crate::api::SessionTurnContentBlock::ToolUse { id, .. }
                        | crate::api::SessionTurnContentBlock::InvalidToolUse { id, .. } => id == q,
                        crate::api::SessionTurnContentBlock::ToolResult { tool_use_id, .. } => {
                            tool_use_id == q
                        }
                        _ => false,
                    })
                })
            {
                continue;
            }
            matches.push(json!({"message_index":index+1,"role":message.role,"preview":text.chars().take(240).collect::<String>(),"total_chars":text.chars().count()}));
        }
        let next = start.saturating_sub(1).saturating_add(count);
        Ok(
            json!({"archive_id":id,"messages":matches.iter().skip(start-1).take(count).collect::<Vec<_>>(),"total_matches":matches.len(),"next_start":(next < matches.len()).then_some(next+1)}),
        )
    }
}

fn valid_id(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|c| c.is_ascii_hexdigit())
}

pub(super) fn message_text(message: &SessionTurnMessage) -> anyhow::Result<String> {
    // provider replay 不作为模型可查询正文，媒体沿用已有安全投影。
    Ok(serde_json::to_string(
        &project_turn_message_for_safe_transcript(message.clone()),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::SessionTurnContentBlock;

    #[tokio::test]
    async fn dream_history_retains_old_snapshots_and_pages_exact_unicode() {
        let dir = tempfile::tempdir().unwrap();
        let history = History::new(dir.path().join("job/input-version"));
        let messages = vec![
            SessionTurnMessage::user_text("task"),
            SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: "read_1".into(),
                    name: "file_read".into(),
                    input: json!({"path":"queue.rs"}),
                }],
            },
            SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: "read_1".into(),
                    content: "确切证据：不是任务验收。".repeat(100),
                }],
            },
        ];
        let id = history.archive(&messages).await.unwrap();
        assert_eq!(history.archive(&messages).await.unwrap(), id);
        history
            .archive(&[SessionTurnMessage::user_text("next snapshot")])
            .await
            .unwrap();
        let list = history.read(json!({})).await.unwrap();
        assert_eq!(list["total"], 2);
        let search = history
            .read(json!({"archive_id":id,"tool_call_id":"read_1","count":1}))
            .await
            .unwrap();
        assert_eq!(search["total_matches"], 2);
        assert_eq!(search["next_start"], 2);
        let search = history
            .read(json!({"archive_id":id,"keyword":"确切证据"}))
            .await
            .unwrap();
        assert_eq!(search["messages"][0]["message_index"], 3);
        let mut text = String::new();
        let mut offset = 0;
        loop {
            let page = history
                .read(
                    json!({"archive_id":id,"message_index":3,"char_offset":offset,"max_chars":97}),
                )
                .await
                .unwrap();
            text.push_str(page["content"].as_str().unwrap());
            let Some(next) = page["next_char_offset"].as_u64() else {
                break;
            };
            offset = next;
        }
        assert_eq!(text, message_text(&messages[2]).unwrap());
        assert!(history
            .read(json!({"archive_id":"../../other-agent"}))
            .await
            .is_err());
        assert!(history
            .read(json!({"archive_id":id,"count":0}))
            .await
            .is_err());
        let other_job = History::new(dir.path().join("other-job/input-version"));
        assert!(other_job.read(json!({"archive_id":id})).await.is_err());
    }
}
