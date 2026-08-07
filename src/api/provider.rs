//! provider-neutral LLM 协议接口。
//!
//! 本模块定义 `AgentTurnLoop` 与具体模型后端之间的最小协议边界：
//! 上层传入 canonical session message 和工具 schema，provider adapter 只负责
//! HTTP/streaming 与协议形状转换，不执行工具、不解释业务 JSON。

use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

use crate::api::{SessionTurnContentBlock, SessionTurnMessage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHistoryMediaPolicy {
    Placeholder,
    Preserve,
}

/// 当前 adapter 可原样回放的 provider 私有历史协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderReplayProtocol {
    OpenAiResponses,
    AnthropicMessages,
}

/// provider 私有 replay 的绑定身份。
///
/// 原样 replay 只允许回到相同 wire protocol 与精确配置 model；切换任一项都从
/// canonical history 开始新的 replay 代际，避免跨模型误传私有状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderReplayIdentity {
    pub protocol: ProviderReplayProtocol,
    pub model: String,
}

#[async_trait]
pub trait ProviderAdapter: Send + Sync {
    /// 已落盘、尚未 compact 的历史附件如何进入主 session provider context。
    fn history_media_policy(&self) -> ProviderHistoryMediaPolicy {
        ProviderHistoryMediaPolicy::Placeholder
    }

    /// 只允许相同协议与精确 model 的 replay 进入请求及 token/compaction 预算。
    fn history_replay_identity(&self) -> Option<ProviderReplayIdentity> {
        None
    }

    /// 是否在 provider 调用前先发本地粗估 ctx。
    fn emit_preflight_context_estimate(&self) -> bool {
        true
    }

    /// 单次逻辑 provider call 的总 deadline；覆盖内部 max_tokens continuation。
    fn request_timeout(&self) -> Option<Duration> {
        None
    }

    async fn send(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRequest {
    pub system_prompt: String,
    pub messages: Vec<SessionTurnMessage>,
    pub tools: Vec<ToolSpec>,
    pub max_tokens: u32,
    pub stream: bool,
    /// 覆盖 adapter 内部的额外 HTTP retry 次数；`None` 使用 provider 配置。
    pub retry_count_override: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResponse {
    pub assistant_message: SessionTurnMessage,
    pub stop: ProviderStop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextUsageSnapshot {
    pub used_tokens: usize,
    pub source: ContextUsageSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextUsageSource {
    Provider,
    Estimate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderStop {
    Done,
    ToolUse,
    MaxTokens,
    /// Provider 返回了完整、有效但因模型上下文窗口耗尽而截断的响应。
    /// 上层必须先压缩上下文再续写，不能按 transport 故障 fallback。
    ContextWindowExceeded,
}

/// Provider 已返回明确的非成功终态；重放同一请求不会变成完整响应。
///
/// Adapter 用该类型阻止 turn loop 把拒绝、暂停或上下文截断误当作
/// transport streaming 故障并自动切到 non-streaming。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ProviderTerminalFailure {
    message: String,
}

/// Streaming response 已损坏或未完整结束，可以安全放弃本次 attempt 并换路径重放。
///
/// 该标记只能由 streaming client 在完成边界产生；普通 non-streaming schema 错误
/// 不能借此进入 provider-neutral fallback。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ProviderStreamFailure {
    message: String,
}

impl ProviderStreamFailure {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Provider 正常结束，但没有 ACN 可以提交的非空文本或完整工具调用。
///
/// 该结果没有产生 provider replay 或工具副作用，允许直接切换 non-streaming 重试；
/// 显式拒绝、token limit 和上下文窗口恢复不能映射为此类型。
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(crate) struct ProviderNoConsumableOutput {
    message: String,
}

impl ProviderNoConsumableOutput {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl ProviderTerminalFailure {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderEvent {
    ContextUsageUpdated { usage: ContextUsageSnapshot },
    AssistantTextDelta { text: String },
    AssistantMessageCompleted { text: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

pub fn context_usage_from_openai_usage(usage: &Value) -> Option<ContextUsageSnapshot> {
    let total = usage.get("total_tokens")?.as_u64()?;
    Some(ContextUsageSnapshot {
        used_tokens: usize::try_from(total).ok()?,
        source: ContextUsageSource::Provider,
    })
}

pub fn context_usage_from_anthropic_input_usage(usage: &Value) -> Option<ContextUsageSnapshot> {
    Some(ContextUsageSnapshot {
        used_tokens: anthropic_input_tokens(usage)?,
        source: ContextUsageSource::Provider,
    })
}

pub fn context_usage_from_anthropic_committed_usage(usage: &Value) -> Option<ContextUsageSnapshot> {
    let input_tokens = anthropic_input_tokens(usage)?;
    let output_tokens = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    Some(ContextUsageSnapshot {
        used_tokens: input_tokens.saturating_add(output_tokens),
        source: ContextUsageSource::Provider,
    })
}

fn anthropic_input_tokens(usage: &Value) -> Option<usize> {
    let input_tokens = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    let cache_creation = usage
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    let cache_read = usage
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0);
    Some(
        input_tokens
            .saturating_add(cache_creation)
            .saturating_add(cache_read),
    )
}

/// 从 assistant message 提取结构化 JSON 场景需要的纯文本。
pub fn assistant_text_from_message(message: &SessionTurnMessage) -> anyhow::Result<String> {
    if message.role != "assistant" {
        anyhow::bail!("provider response role 必须是 assistant: {}", message.role);
    }

    let mut text = String::new();
    for block in &message.content {
        match block {
            SessionTurnContentBlock::Text { text: part } => text.push_str(part),
            SessionTurnContentBlock::SkillInstructions { .. } => {
                anyhow::bail!("结构化文本响应不能包含 SkillInstructions block");
            }
            SessionTurnContentBlock::Image { .. } | SessionTurnContentBlock::Document { .. } => {
                anyhow::bail!("结构化文本响应不能包含附件 block");
            }
            SessionTurnContentBlock::ToolUse { .. }
            | SessionTurnContentBlock::ToolResult { .. } => {
                anyhow::bail!("结构化文本响应只能包含 Text block");
            }
        }
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn openai_usage_prefers_total_tokens_for_committed_context() {
        let usage = json!({
            "prompt_tokens": 100,
            "completion_tokens": 25,
            "total_tokens": 125
        });

        assert_eq!(
            context_usage_from_openai_usage(&usage),
            Some(ContextUsageSnapshot {
                used_tokens: 125,
                source: ContextUsageSource::Provider
            })
        );
    }

    #[test]
    fn anthropic_input_usage_includes_cache_buckets() {
        let usage = json!({
            "input_tokens": 100,
            "cache_creation_input_tokens": 20,
            "cache_read_input_tokens": 30,
            "output_tokens": 9
        });

        assert_eq!(
            context_usage_from_anthropic_input_usage(&usage),
            Some(ContextUsageSnapshot {
                used_tokens: 150,
                source: ContextUsageSource::Provider
            })
        );
        assert_eq!(
            context_usage_from_anthropic_committed_usage(&usage),
            Some(ContextUsageSnapshot {
                used_tokens: 159,
                source: ContextUsageSource::Provider
            })
        );
    }
}
