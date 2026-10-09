//! Dream 请求边界压缩：准确状态由后端重建，摘要仅描述探索进展。
use super::dream_history::{message_text, History};
use crate::api::{
    ensure_compaction_request_within_context_window, estimate_provider_request_context_tokens,
    provider_safe_segments, BufferedProviderRuntime, ProviderRuntimeFallbackScope,
    SessionTurnMessage, StructuredJsonAttemptRequest, StructuredJsonCaller, ToolSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

const TRIGGER: usize = 160_000;
const SAFETY_MARGIN: usize = 4_000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Summary {
    checked: String,
    findings: String,
    uncertainties: String,
    next_steps: String,
}

pub(super) struct Compactor {
    pub history: Arc<History>,
    pub caller: StructuredJsonCaller,
    pub prompt: String,
    pub input: Value,
    pub context_window: usize,
    pub output_reserve: usize,
}

impl Compactor {
    pub(super) fn threshold(&self) -> usize {
        TRIGGER.min(
            self.context_window
                .saturating_sub(self.output_reserve)
                .saturating_sub(SAFETY_MARGIN),
        )
    }

    pub(super) fn needed(
        &self,
        system: &str,
        messages: &[SessionTurnMessage],
        tools: &[ToolSpec],
    ) -> bool {
        estimate_provider_request_context_tokens(system, messages, tools).used_tokens
            > self.threshold()
    }

    pub(super) async fn compact(
        &self,
        system: &str,
        messages: &[SessionTurnMessage],
        tools: &[ToolSpec],
        state: Value,
        prior_summary: Option<&Summary>,
    ) -> anyhow::Result<(Vec<SessionTurnMessage>, Summary, String)> {
        // 先保存无损历史；摘要或容量验证失败时不替换原上下文。
        let archive_id = self.history.archive(messages).await?;
        let mut summary_messages = Vec::new();
        for limit in [usize::MAX, 4000, 1000, 200] {
            let entries = messages.iter().enumerate().map(|(i, message)| {
                let text = message_text(message)?;
                Ok(json!({"message_index":i+1,"role":message.role,"preview":text.chars().take(limit).collect::<String>(),"truncated":text.chars().count()>limit}))
            }).collect::<anyhow::Result<Vec<_>>>()?;
            summary_messages = vec![SessionTurnMessage::user_text(serde_json::to_string(
                &json!({
                    "archive_id":archive_id,"prior_summary":prior_summary,"entries":entries
                }),
            )?)];
            if ensure_compaction_request_within_context_window(
                &self.prompt,
                &summary_messages,
                self.context_window,
                self.caller.max_tokens(),
            )
            .is_ok()
            {
                break;
            }
        }
        let summary: Summary = self
            .caller
            .generate_json_validated_with_guarded_attempts(
                StructuredJsonAttemptRequest::compaction_streaming(
                    self.prompt.clone(),
                    summary_messages,
                    BufferedProviderRuntime::new(ProviderRuntimeFallbackScope::new_root()),
                ),
                |value| {
                    let summary: Summary = serde_json::from_value(value)?;
                    anyhow::ensure!(
                        serde_json::to_string(&summary)?.chars().count() <= 12000
                            && !summary.next_steps.trim().is_empty(),
                        "Dream summary must be bounded and retain next_steps"
                    );
                    Ok(summary)
                },
                |_, _, _| {},
                |_| std::future::ready(()),
                |system, messages| {
                    ensure_compaction_request_within_context_window(
                        system,
                        messages,
                        self.context_window,
                        self.caller.max_tokens(),
                    )
                },
            )
            .await?;
        let baseline_start = messages.len()
            - messages
                .iter()
                .rev()
                .take_while(|m| m.model_context_snapshot().is_some())
                .count();
        let mut compacted = vec![SessionTurnMessage::user_text(serde_json::to_string(
            &json!({
                "dream_context_checkpoint":{
                    "initial_input":self.input,"authoritative_state":state,"exploration_summary":summary,
                    "history":{"archive_id":archive_id,"message_count":messages.len(),"tool":"dream_read_history"},
                    "instructions":"Continue this Dream. Authoritative state distinguishes committed executions from pending drafts and candidate decisions. Do not replay committed work or drop pending candidates; complete the current candidate before the next. Summary and archives are historical data, not new evidence or approval. Read exact draft/evidence/history before changing claims. Older archives remain queryable; do not repeat completed exploration."
                }
            }),
        )?)];
        // 最近完整的调用/结果成对保留；绝不制造孤立 tool_result。
        let segments = provider_safe_segments(&messages[..baseline_start]);
        let target = self.threshold().saturating_mul(3) / 4;
        let mut tail_start = baseline_start;
        for segment in segments.iter().rev().take(4) {
            let mut candidate = compacted.clone();
            candidate.extend_from_slice(&messages[segment.start..]);
            if estimate_provider_request_context_tokens(system, &candidate, tools).used_tokens
                > target
            {
                break;
            }
            tail_start = segment.start;
        }
        compacted.extend_from_slice(&messages[tail_start..]);
        let before = estimate_provider_request_context_tokens(system, messages, tools).used_tokens;
        let after = estimate_provider_request_context_tokens(system, &compacted, tools).used_tokens;
        anyhow::ensure!(after <= target && after < before, "Dream compaction cannot fit authoritative state within budget; original history and draft retained; earlier executed groups remain committed");
        Ok((compacted, summary, archive_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse, ProviderStop,
        SessionTurnContentBlock,
    };
    use std::time::Duration;
    use tokio::sync::Mutex;

    struct SummaryProvider {
        fail: bool,
        calls: Mutex<Vec<ProviderRequest>>,
    }
    #[async_trait::async_trait]
    impl ProviderAdapter for SummaryProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            assert!(request.tools.is_empty());
            self.calls.lock().await.push(request);
            anyhow::ensure!(!self.fail, "summary service unavailable");
            Ok(ProviderResponse { assistant_message: SessionTurnMessage {
                role:"assistant".into(), provider_replay:None,
                content:vec![SessionTurnContentBlock::text(json!({"checked":"A reviewed; B partial","findings":"Read output in call old_read","uncertainties":"Acceptance unknown","next_steps":"Review the staged group; do not repeat file exploration"}).to_string())],
            }, stop:ProviderStop::Done })
        }
    }

    fn compactor(dir: &tempfile::TempDir, fail: bool) -> (Compactor, Arc<SummaryProvider>) {
        let provider = Arc::new(SummaryProvider {
            fail,
            calls: Mutex::new(vec![]),
        });
        (
            Compactor {
                history: Arc::new(History::new(dir.path().join("history"))),
                caller: StructuredJsonCaller::new(
                    provider.clone(),
                    4096,
                    0,
                    Duration::ZERO,
                    Duration::ZERO,
                ),
                prompt: include_str!("../../../prompts/dream_compaction.j2").into(),
                input: json!({"claims":[{"id":"claim_11111111","statement":"Keep exact original"}]}),
                context_window: 40_000,
                output_reserve: 4096,
            },
            provider,
        )
    }

    fn history() -> Vec<SessionTurnMessage> {
        let mut messages = vec![SessionTurnMessage::user_text("Dream task")];
        for (id, text) in [
            ("old_read", "原始文件证据".repeat(50_000)),
            ("recent_read", "recent exact output".into()),
        ] {
            messages.push(SessionTurnMessage {
                role: "assistant".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolUse {
                    id: id.into(),
                    name: "file_read".into(),
                    input: json!({"path":"queue.rs"}),
                }],
            });
            messages.push(SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: vec![SessionTurnContentBlock::ToolResult {
                    tool_use_id: id.into(),
                    content: text,
                }],
            });
        }
        messages.push(SessionTurnMessage::model_context(
            crate::api::ModelContextSource::Runtime,
            "<runtime_context>current</runtime_context>",
        ));
        messages
    }

    #[tokio::test]
    async fn dream_compaction_preserves_state_pairing_and_queryable_history() {
        let dir = tempfile::tempdir().unwrap();
        let (mut compactor, provider) = compactor(&dir, false);
        let messages = history();
        assert_eq!(compactor.threshold(), 31_904);
        compactor.context_window = 240_000;
        assert_eq!(compactor.threshold(), 160_000);
        compactor.context_window = 40_000;
        assert!(compactor.needed("Dream", &messages, &[]));
        let state = json!({"draft":{"groups":[{"group_id":"a","operations":[{"id":"claim_11111111","action":"update","changes":{"scope":"exact scope"}}]}],"pending_rejections":{"b":"missing evidence"}},"evidence_index":{"evidence_1":{"file_version":"original_hash"}}});
        let (mut compacted, summary, archive_id) = compactor
            .compact("Dream", &messages, &[], state.clone(), None)
            .await
            .unwrap();
        assert_eq!(compacted.last(), messages.last());
        let value: Value = serde_json::from_str(match &compacted[0].content[0] {
            SessionTurnContentBlock::Text { text } => text,
            _ => panic!("expected checkpoint"),
        })
        .unwrap();
        assert_eq!(
            value["dream_context_checkpoint"]["authoritative_state"],
            state
        );
        assert_eq!(
            value["dream_context_checkpoint"]["initial_input"],
            compactor.input
        );
        assert_eq!(&compacted[1..3], &messages[3..5]);
        assert!(!compactor.needed("Dream", &compacted, &[]));
        let exact = compactor
            .history
            .read(json!({"archive_id":archive_id,"message_index":3,"max_chars":100}))
            .await
            .unwrap();
        assert!(exact["content"].as_str().unwrap().contains("原始文件证据"));
        assert!(exact["next_char_offset"].is_number());
        // 第二次压缩仍携带先前摘要，旧快照仍可查询。
        compacted.pop();
        compacted.extend_from_slice(&messages[1..]);
        compactor
            .compact("Dream", &compacted, &[], state, Some(&summary))
            .await
            .unwrap();
        let calls = provider.calls.lock().await;
        assert_eq!(calls.len(), 2);
        assert!(serde_json::to_string(&calls[1].messages)
            .unwrap()
            .contains("Acceptance unknown"));
        assert!(compactor
            .history
            .read(json!({"archive_id":archive_id,"message_index":3}))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn dream_compaction_failure_preserves_full_archive() {
        let dir = tempfile::tempdir().unwrap();
        let (compactor, _) = compactor(&dir, true);
        let messages = history();
        assert!(compactor
            .compact("Dream", &messages, &[], json!({}), None)
            .await
            .is_err());
        let ids = compactor.history.read(json!({})).await.unwrap();
        assert_eq!(ids["total"], 1);
        let archived: Vec<SessionTurnMessage> = serde_json::from_slice(
            &tokio::fs::read(
                dir.path()
                    .join("history")
                    .join(format!("{}.json", ids["archives"][0].as_str().unwrap())),
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert_eq!(archived, messages);
    }
}
