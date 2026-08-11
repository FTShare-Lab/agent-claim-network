//! provider-neutral 的后台 memory review tool loop。
//!
//! 本模块只服务交互式 session 的后台记忆审阅：它复用 `ProviderAdapter`
//! 和 canonical tool_use 消息，但只暴露并执行 `memory` 工具，不写 session history。

use std::sync::Arc;

use anyhow::Context;
use serde_json::{json, Value};

use crate::api::{
    MemoryReviewRequest, ProviderAdapter, ProviderEvent, ProviderRequest, ProviderResponse,
    ProviderStop, SessionTurnContentBlock, SessionTurnMessage, ToolExecutionOutcome, ToolSpec,
};
use crate::tool::ToolRegistry;

/// 后台记忆审阅的独立内部 tool 回环上限，不对用户配置开放。
pub const MEMORY_REVIEW_MAX_TOOL_LOOP_TURNS: usize = 32;

pub struct MemoryReviewLoop {
    provider: Arc<dyn ProviderAdapter>,
    tools: Arc<ToolRegistry>,
    max_turns: usize,
    max_tokens: u32,
}

impl MemoryReviewLoop {
    pub fn new(
        provider: Arc<dyn ProviderAdapter>,
        tools: Arc<ToolRegistry>,
        max_turns: usize,
        max_tokens: u32,
    ) -> Self {
        Self {
            provider,
            tools,
            max_turns,
            max_tokens,
        }
    }

    pub async fn run(
        &self,
        request: MemoryReviewRequest,
        review_prompt: String,
    ) -> anyhow::Result<()> {
        if self.max_turns == 0 {
            anyhow::bail!("review_memory max_turns 必须大于 0");
        }
        let memory_tools = self.memory_tool_specs();
        if memory_tools.is_empty() {
            log::debug!(
                target: "api",
                "background memory review 跳过：当前工具 registry 未暴露 memory 工具"
            );
            return Ok(());
        }

        let mut messages = request.transcript;
        messages.push(SessionTurnMessage::user_text(review_prompt));

        for turn_idx in 0..self.max_turns {
            let provider_response = self
                .call_provider(&request.system_prompt, &messages, &memory_tools)
                .await?;
            let assistant_message = provider_response.assistant_message;
            validate_assistant_message(&assistant_message)?;
            let tool_uses = collect_tool_uses(&assistant_message)?;

            if tool_uses.is_empty() {
                return match provider_response.stop {
                    ProviderStop::Done => Ok(()),
                    ProviderStop::ToolUse => {
                        anyhow::bail!(
                            "review_memory provider stop=ToolUse 但 assistant message 没有 ToolUse block"
                        )
                    }
                    ProviderStop::MaxTokens => {
                        anyhow::bail!("review_memory provider stop=MaxTokens，无法安全完成")
                    }
                    ProviderStop::ContextWindowExceeded => {
                        anyhow::bail!("Memory review 上下文已满，本次后台整理已停止。")
                    }
                };
            }

            match provider_response.stop {
                ProviderStop::MaxTokens => {
                    anyhow::bail!(
                        "review_memory provider stop=MaxTokens 且包含 ToolUse，拒绝执行半截工具调用"
                    );
                }
                ProviderStop::ContextWindowExceeded => {
                    anyhow::bail!("Memory review 上下文已满，本次后台整理已停止，未执行工具。");
                }
                ProviderStop::Done | ProviderStop::ToolUse => {}
            }
            if turn_idx + 1 == self.max_turns {
                anyhow::bail!("review_memory 达到最大 tool 循环轮数: {}", self.max_turns);
            }

            messages.push(assistant_message);
            let mut tool_results = Vec::with_capacity(tool_uses.len());
            for tool_use in tool_uses {
                let content =
                    execute_memory_tool_use(&self.tools, &tool_use.name, tool_use.input).await;
                tool_results.push(SessionTurnContentBlock::ToolResult {
                    tool_use_id: tool_use.id,
                    content,
                });
            }
            messages.push(SessionTurnMessage {
                role: "user".into(),
                provider_replay: None,
                content: tool_results,
            });
        }

        anyhow::bail!("review_memory 达到最大 tool 循环轮数: {}", self.max_turns)
    }

    fn memory_tool_specs(&self) -> Vec<ToolSpec> {
        self.tools
            .definitions()
            .into_iter()
            .filter(|definition| definition.name == "memory")
            .map(Into::into)
            .collect()
    }

    async fn call_provider(
        &self,
        system_prompt: &str,
        messages: &[SessionTurnMessage],
        tools: &[ToolSpec],
    ) -> anyhow::Result<ProviderResponse> {
        let mut emit = |_event: ProviderEvent| {};
        self.provider
            .send(
                ProviderRequest {
                    system_prompt: system_prompt.to_string(),
                    messages: messages.to_vec(),
                    tools: tools.to_vec(),
                    max_tokens: self.max_tokens,
                    stream: false,
                    retry_count_override: None,
                },
                &mut emit,
            )
            .await
    }
}

#[derive(Debug, Clone)]
struct CanonicalToolUse {
    id: String,
    name: String,
    input: Value,
}

fn validate_assistant_message(message: &SessionTurnMessage) -> anyhow::Result<()> {
    if message.role != "assistant" {
        anyhow::bail!("provider response role 必须是 assistant: {}", message.role);
    }
    if message.content.iter().any(|block| {
        matches!(
            block,
            SessionTurnContentBlock::ToolResult { .. }
                | SessionTurnContentBlock::ModelContext { .. }
        )
    }) {
        anyhow::bail!("assistant message 不允许包含 ToolResult 或 ModelContext block");
    }
    Ok(())
}

fn collect_tool_uses(message: &SessionTurnMessage) -> anyhow::Result<Vec<CanonicalToolUse>> {
    let mut tool_uses = Vec::new();
    for block in &message.content {
        if let SessionTurnContentBlock::ToolUse { id, name, input } = block {
            if id.trim().is_empty() {
                anyhow::bail!("tool_use id 不能为空");
            }
            if name.trim().is_empty() {
                anyhow::bail!("tool_use name 不能为空");
            }
            if !input.is_object() {
                anyhow::bail!("tool_use input 必须是 JSON object: {name}");
            }
            tool_uses.push(CanonicalToolUse {
                id: id.clone(),
                name: name.clone(),
                input: input.clone(),
            });
        }
    }
    Ok(tool_uses)
}

async fn execute_memory_tool_use(tools: &ToolRegistry, name: &str, input: Value) -> String {
    if name != "memory" {
        return serialize_tool_result(json!({
            "ok": false,
            "outcome": ToolExecutionOutcome::DispatchFailure,
            "error": format!("background memory review 禁止调用非 memory 工具: {name}"),
        }));
    }

    match tools.dispatch(name, input).await {
        Ok(execution) => serialize_tool_result(json!({
            "ok": execution.outcome.is_success(),
            "outcome": execution.outcome,
            "output": execution.output,
        })),
        Err(err) => serialize_tool_result(json!({
            "ok": false,
            "outcome": ToolExecutionOutcome::DispatchFailure,
            "error": err.to_string(),
        })),
    }
}

fn serialize_tool_result(payload: Value) -> String {
    serde_json::to_string(&payload)
        .context("序列化 memory review tool_result")
        .unwrap_or_else(|e| json!({"ok": false, "error": e.to_string()}).to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::agent::fs::LocalFsMemoryStore;
    use crate::api::{ProviderEvent, ProviderResponse};
    use crate::config::{ToolConfig, DEFAULT_MEMORY_CHAR_LIMIT, DEFAULT_USER_CHAR_LIMIT};
    use crate::tool::ToolRegistry;

    struct RecordingProvider {
        requests: Arc<Mutex<Vec<ProviderRequest>>>,
        responses: Mutex<VecDeque<ProviderResponse>>,
    }

    #[async_trait]
    impl ProviderAdapter for RecordingProvider {
        async fn send(
            &self,
            request: ProviderRequest,
            _emit: &mut (dyn FnMut(ProviderEvent) + Send),
        ) -> anyhow::Result<ProviderResponse> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| anyhow::anyhow!("missing test response"))
        }
    }

    #[tokio::test]
    async fn memory_review_loop_returns_error_tool_result_for_non_memory_tool_and_continues() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let provider: Arc<dyn ProviderAdapter> = Arc::new(RecordingProvider {
            requests: requests.clone(),
            responses: Mutex::new(VecDeque::from([
                ProviderResponse {
                    assistant_message: SessionTurnMessage {
                        role: "assistant".into(),
                        provider_replay: None,
                        content: vec![SessionTurnContentBlock::ToolUse {
                            id: "toolu_file_1".into(),
                            name: "file_read".into(),
                            input: json!({"path": "secret.txt"}),
                        }],
                    },
                    stop: ProviderStop::ToolUse,
                },
                ProviderResponse {
                    assistant_message: SessionTurnMessage::assistant_text("Nothing to save."),
                    stop: ProviderStop::Done,
                },
            ])),
        });
        let home = tempfile::tempdir().unwrap();
        let memory_store = Arc::new(LocalFsMemoryStore::new(
            home.path().to_path_buf(),
            DEFAULT_MEMORY_CHAR_LIMIT,
            DEFAULT_USER_CHAR_LIMIT,
            false,
        ));
        let tools = Arc::new(
            ToolRegistry::new(&ToolConfig::default())
                .unwrap()
                .with_memory_store(memory_store),
        );
        let review_loop = MemoryReviewLoop::new(provider, tools, 4, 1024);

        review_loop
            .run(
                MemoryReviewRequest {
                    system_prompt: "review system".into(),
                    transcript: vec![SessionTurnMessage::user_text("hello")],
                },
                "review prompt".into(),
            )
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(!requests[0].stream);
        assert_eq!(requests[0].tools.len(), 1);
        assert_eq!(requests[0].tools[0].name, "memory");
        let last = requests[1].messages.last().unwrap();
        assert_eq!(last.role, "user");
        let SessionTurnContentBlock::ToolResult { content, .. } = &last.content[0] else {
            panic!("expected tool result");
        };
        assert!(content.contains("background memory review 禁止调用非 memory 工具"));
        assert!(content.contains("file_read"));
    }
}
