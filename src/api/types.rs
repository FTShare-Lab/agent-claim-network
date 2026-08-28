//! LLM 调用的输入 / 输出 DTO。
//!
//! 与领域实体（`Claim` / `Dispute` / `Trace`）解耦：LLM 看到的是 session transcript、
//! inbox 消息、候选摘要等协议 DTO，agent 自己负责校验和落库。
//!
//! ## id 字段为什么用 String 而不是 ClaimId
//! agent 在线模式下 LLM 不能发明真实 id。它在一次响应里用 `$new_claim_N$` /
//! `$new_dispute_N$` 占位符自指（详见 `placeholder` 模块），由 runner 调用
//! `resolve_placeholders` 替换为真实 id。无论替换前还是替换后，这些字段在 DTO 层都
//! 用 String 表达：
//!
//! - 替换前：值是占位符字符串
//! - 替换后：值是真实 id 字符串
//!
//! Runner 在构造领域对象时统一用 `ClaimId::from_str` / `DisputeId::from_str`
//! 校验格式，给出更精准的错误上下文（哪条 claim 哪个字段挂了）。
//!
//! ## InboxMessage 直接复用领域实体
//! inbox 内化请求把完整 `InboxMessage` 交给 Agent 自己的模型，而不是改成
//! PolicySummary。连续 ClaimAttributeUpdate 在入模边界批量提供；普通建议只提供
//! conclusion，带 Resolution 的建议再补充结构化裁决、Dispute 与 direct Claim 快照。

use std::ops::Deref;
use std::path::PathBuf;
use std::{error::Error, fmt};

use chrono::{DateTime, Utc};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::ContextUsageSnapshot;
use crate::claim::{
    AgentId, Claim, ClaimId, Confidence, Dispute, DisputeResolution, InboxMessage, SessionId,
};
use crate::skill::SkillInstructions;
use crate::tool::diff::FileChange;

/// inbox 内化请求对应的业务类型，用来选择类型专用 prompt。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboxInternalizeKind {
    PolicyUpdate,
    ClaimAttributeUpdate,
}

/// agent 当前可见 skill 的摘要
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvailableSkill {
    pub name: String,
    pub description: String,
    pub spec_path: String,
}

/// 结构化 recap / compact 请求里使用的一条纯文本 transcript 消息。
///
/// 交互式 session 的 transcript 使用 `SessionTurnMessage`，以便保留
/// Anthropic content block 中的 tool_use / tool_result 结构。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnMessage {
    /// "user" 或 "assistant"（与 Anthropic Messages API 的 role 对齐）
    pub role: String,
    pub content: String,
}

/// 交互式 session turn 的请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTurnRequest {
    pub current_session_id: Option<SessionId>,
    pub current_turn_id: Option<String>,
    pub system_prompt: String,
    pub history: Vec<SessionTurnMessage>,
    pub user_text: String,
    pub user_attachments: Vec<SessionAttachment>,
    pub skill_instructions: Vec<SkillInstructions>,
}

/// 交互式 session 用户输入中随文本一起提交的本地附件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionAttachment {
    LocalImage { path: PathBuf },
    InlineImage { media_type: String, data: String },
    TextFile { path: PathBuf },
    DocumentFile { path: PathBuf, media_type: String },
}

/// 后台 memory review fork 的请求。
///
/// `system_prompt` 是 review agent 专用 prompt，不能复用或泄露主 session 的
/// system prompt；`transcript` 是主流程当前 turn 提交后的最近有效消息窗口。
/// review fork 只能通过原生 `memory` 工具写入持久记忆。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryReviewRequest {
    pub system_prompt: String,
    pub transcript: Vec<SessionTurnMessage>,
}

/// session_search 对候选历史 session 的无副作用摘要请求。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSearchSummaryRequest {
    pub query: String,
    pub session_id: SessionId,
    pub when: String,
    pub source: String,
    pub model: String,
    pub conversation_text: String,
    pub summary_max_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSearchSummaryOutcome {
    pub summary: String,
}

/// 工具调用跨模型回灌、事件、journal 与 TUI 共享的执行语义。
///
/// `Completed` 只表示工具正常完成；进程退出与 HTTP 响应保留各自协议状态，
/// 避免再从工具输出中的任意 `status` 字段猜测成功与否。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolExecutionOutcome {
    Completed,
    DispatchFailure,
    BusinessFailure,
    ProcessExit {
        exit_code: Option<i32>,
        success: bool,
    },
    ProcessTerminated {
        signal: Option<i32>,
    },
    ProcessRunning,
    HttpResponse {
        http_status: u16,
    },
}

/// 已收到完整 tool_use、但尚未交给工具调度器时的跳过原因。
///
/// 这不是工具执行结果：对应调用从未启动，因此不能复用
/// `ToolExecutionOutcome` 或 `ToolCallInterrupted`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallSkipReason {
    TurnCancelledBeforeDispatch,
    TurnInterruptedBeforeDispatch,
}

impl ToolCallSkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TurnCancelledBeforeDispatch => "Turn cancelled before dispatch",
            Self::TurnInterruptedBeforeDispatch => "Turn interrupted before dispatch",
        }
    }
}

impl ToolExecutionOutcome {
    /// 返回该结果是否应作为工具业务成功展示。
    pub fn is_success(self) -> bool {
        match self {
            Self::Completed => true,
            Self::DispatchFailure | Self::BusinessFailure => false,
            Self::ProcessExit { success, .. } => success,
            Self::ProcessTerminated { .. } => true,
            Self::ProcessRunning => true,
            Self::HttpResponse { http_status } => (200..300).contains(&http_status),
        }
    }
}

/// 交互式 session turn 的结果，包含本轮应该提交到 transcript 的消息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTurn {
    pub messages: Vec<CompletedSessionTurnMessage>,
}

/// provider / 工具安全边界触发的 turn 中断。
///
/// 这个错误只表示“当前 turn 按用户 steer / cancel 要求停止在安全边界”，
/// 不是 provider 或工具失败；调用方据此写 interrupted journal，但不提交
/// canonical transcript。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTurnInterrupted;

impl fmt::Display for SessionTurnInterrupted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("session turn interrupted at safe boundary")
    }
}

impl Error for SessionTurnInterrupted {}

/// 本轮已经完整生成、等待 commit 落盘的一条 transcript 消息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedSessionTurnMessage {
    pub message: SessionTurnMessage,
    pub completed_at: DateTime<Utc>,
}

impl CompletedSessionTurnMessage {
    pub fn new(message: SessionTurnMessage, completed_at: DateTime<Utc>) -> Self {
        Self {
            message,
            completed_at,
        }
    }
}

impl Deref for CompletedSessionTurnMessage {
    type Target = SessionTurnMessage;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl PartialEq<SessionTurnMessage> for CompletedSessionTurnMessage {
    fn eq(&self, other: &SessionTurnMessage) -> bool {
        self.message == *other
    }
}

/// session 历史压缩请求。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCompactionRequest {
    pub agent_id: AgentId,
    pub start_index: usize,
    pub end_index: usize,
    pub prior_summary: Option<String>,
    pub summary_max_chars: usize,
}

/// session 历史压缩结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCompactionOutcome {
    pub committed_summary: Option<String>,
    pub active_turn_summary: Option<String>,
}

/// session turn 执行过程中的运行时事件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionTurnEvent {
    Warning {
        message: String,
    },
    ContextUsageUpdated {
        usage: ContextUsageSnapshot,
    },
    CompactionStarted {
        compact_start_index: usize,
        compact_end_index: usize,
        recap_start_index: usize,
        recap_end_index: usize,
    },
    CompactionCompleted {
        compacted_until: usize,
    },
    RecapRequested {
        session_id: SessionId,
        recap_end_index: usize,
    },
    CompactionSkipped {
        warning: String,
    },
    CompactionFailed {
        error: String,
    },
    AssistantTextDelta {
        text: String,
    },
    AssistantMessageCompleted {
        text: String,
    },
    NonStreamingFallbackAttemptStarted {
        attempt: u32,
        max_attempts: u32,
        previous_error: String,
    },
    NonStreamingFallbackAttemptFailed {
        attempt: u32,
        max_attempts: u32,
        error: String,
    },
    NonStreamingFallbackSucceeded {
        attempt: u32,
        max_attempts: u32,
        text: String,
    },
    ToolCallStarted {
        id: String,
        name: String,
        summary: String,
        input_preview: String,
        input_truncated: bool,
    },
    ToolCallSkipped {
        id: String,
        name: String,
        summary: String,
        input_preview: String,
        input_truncated: bool,
        reason: ToolCallSkipReason,
    },
    ToolCallProgress {
        id: String,
        summary: String,
    },
    ToolCallCompleted {
        id: String,
        summary: String,
        outcome: ToolExecutionOutcome,
        output_preview: String,
        output_truncated: bool,
        /// file 类工具修改成功时采集的 diff，随事件透传给 TUI 与 turn journal。
        file_change: Option<FileChange>,
    },
    ToolCallInterrupted {
        id: String,
        summary: String,
    },
}

/// provider 私有、只用于同协议历史重放的完整状态。
///
/// 该状态不参与 transcript、Memory 或跨协议语义投影；未知 item 字段通过
/// `serde_json::Value` 原样保存，避免协议适配层丢失 reasoning 等连续性信息。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum ProviderReplayState {
    #[serde(rename = "openai_responses")]
    OpenAiResponses {
        /// 当前分支早期落盘未携带 model；缺失时按未绑定旧 replay 处理。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        items: Vec<Value>,
    },
    #[serde(rename = "openai_chat_completions")]
    OpenAiChatCompletions {
        model: String,
        /// max-token continuation 期间实际进入后续请求的 Chat message 序列。
        messages: Vec<Value>,
    },
    #[serde(rename = "anthropic_messages")]
    AnthropicMessages {
        model: String,
        /// 完整、按顺序保存的 provider-private Messages API message。
        messages: Vec<Value>,
    },
}

impl ProviderReplayState {
    pub fn matches_identity(&self, identity: &super::ProviderReplayIdentity) -> bool {
        match self {
            Self::OpenAiResponses {
                model: Some(model), ..
            } => {
                identity.protocol == super::ProviderReplayProtocol::OpenAiResponses
                    && model == &identity.model
            }
            Self::OpenAiChatCompletions { model, .. } => {
                identity.protocol == super::ProviderReplayProtocol::OpenAiChatCompletions
                    && model == &identity.model
            }
            Self::AnthropicMessages { model, .. } => {
                identity.protocol == super::ProviderReplayProtocol::AnthropicMessages
                    && model == &identity.model
            }
            Self::OpenAiResponses { model: None, .. } => false,
        }
    }
}

/// provider-neutral session message。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTurnMessage {
    pub role: String,
    pub content: Vec<SessionTurnContentBlock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_replay: Option<ProviderReplayState>,
}

impl SessionTurnMessage {
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: "user".into(),
            content: vec![SessionTurnContentBlock::text(text)],
            provider_replay: None,
        }
    }

    pub fn user_content(content: Vec<SessionTurnContentBlock>) -> Self {
        Self {
            role: "user".into(),
            content,
            provider_replay: None,
        }
    }

    pub fn assistant_text(text: impl Into<String>) -> Self {
        Self {
            role: "assistant".into(),
            content: vec![SessionTurnContentBlock::text(text)],
            provider_replay: None,
        }
    }

    /// 构造一条可持久化、provider 可见但不属于真实用户输入的上下文快照。
    pub fn model_context(source: ModelContextSource, text: impl Into<String>) -> Self {
        let text = text.into();
        let fingerprint = model_context_fingerprint(source, &text);
        Self {
            role: "user".into(),
            content: vec![SessionTurnContentBlock::ModelContext {
                source,
                fingerprint,
                text,
            }],
            provider_replay: None,
        }
    }

    pub fn model_context_snapshot(&self) -> Option<(&ModelContextSource, &str, &str)> {
        if self.role != "user" || self.content.len() != 1 {
            return None;
        }
        match &self.content[0] {
            SessionTurnContentBlock::ModelContext {
                source,
                fingerprint,
                text,
            } => Some((source, fingerprint, text)),
            _ => None,
        }
    }

    pub fn with_provider_replay(mut self, provider_replay: ProviderReplayState) -> Self {
        self.provider_replay = Some(provider_replay);
        self
    }

    pub fn without_provider_replay(mut self) -> Self {
        self.provider_replay = None;
        self
    }
}

/// 模型可见、但不属于真实用户输入的动态上下文来源。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelContextSource {
    Runtime,
    BackgroundProcess,
    Delegation,
}

impl ModelContextSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::BackgroundProcess => "background_process",
            Self::Delegation => "delegation",
        }
    }
}

fn model_context_fingerprint(source: ModelContextSource, text: &str) -> String {
    let mut input = Vec::with_capacity(source.as_str().len().saturating_add(text.len() + 1));
    input.extend_from_slice(source.as_str().as_bytes());
    input.push(0);
    input.extend_from_slice(text.as_bytes());
    format!(
        "sha256-v1:{}",
        hex::encode(digest(&SHA256, &input).as_ref())
    )
}

/// Provider-neutral session content block。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionTurnContentBlock {
    Text {
        text: String,
    },
    /// 动态运行态的完整有界快照。adapter 只把 `text` 映射到 provider；其余字段用于
    /// 稳定去重、持久化恢复和真实用户 turn 过滤。
    ModelContext {
        source: ModelContextSource,
        fingerprint: String,
        text: String,
    },
    /// 用户显式调用的 Skill 正文快照，不同于用户自由文本。
    SkillInstructions {
        instruction: SkillInstructions,
    },
    Image {
        media_type: String,
        data: String,
    },
    Document {
        media_type: String,
        data: String,
        /// 文档原始文件名，OpenAI Chat 的 file part 需要；旧 transcript 无此字段。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

impl SessionTurnContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }

    pub fn skill_instructions(instruction: SkillInstructions) -> Self {
        Self::SkillInstructions { instruction }
    }

    pub fn image(media_type: impl Into<String>, data: impl Into<String>) -> Self {
        Self::Image {
            media_type: media_type.into(),
            data: data.into(),
        }
    }

    pub fn document(media_type: impl Into<String>, data: impl Into<String>) -> Self {
        Self::Document {
            media_type: media_type.into(),
            data: data.into(),
            filename: None,
        }
    }

    pub fn document_named(
        media_type: impl Into<String>,
        data: impl Into<String>,
        filename: impl Into<String>,
    ) -> Self {
        Self::Document {
            media_type: media_type.into(),
            data: data.into(),
            filename: Some(filename.into()),
        }
    }
}

/// 批量 PolicyUpdate 内化请求：把同类型 inbox 消息和 agent 自己的本地 claim
/// 一并喂给 LLM，由 LLM 决定要不要新增 / 更新 claim、是否产生 dispute。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalizeRequest {
    pub agent_id: AgentId,
    #[serde(default)]
    pub inbox_messages: Vec<InboxMessage>,
    #[serde(default)]
    pub local_claims: Vec<Claim>,
}

/// 单条 ClaimAttributeUpdate 的规范化上下文。
///
/// `conclusion` 对所有 CAU 都存在：普通 CAU 取自 `policy.statement`，结构化裁决
/// 取自 Resolution。其余裁决与 Dispute 字段按消息实际携带的上下文增量提供。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimAttributeUpdateInternalizeItem {
    pub claim_attribute_update: InboxMessage,
    pub conclusion: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<DisputeResolution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispute: Option<Dispute>,
    #[serde(default)]
    pub direct_claims: Vec<Claim>,
}

/// 连续 ClaimAttributeUpdate 的批量内化输入。
///
/// `claim_attribute_updates` 保持 inbox 顺序；本地 Claim 只发送一次，由模型综合
/// 本批建议后返回一份最终知识变更。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimAttributeUpdateInternalizeRequest {
    pub agent_id: AgentId,
    #[serde(default)]
    pub claim_attribute_updates: Vec<ClaimAttributeUpdateInternalizeItem>,
    #[serde(default)]
    pub local_claims: Vec<Claim>,
}

/// session recap / finalize 请求：补充 agent 当前完整本地 claim。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecapRequest {
    pub agent_id: AgentId,
    #[serde(default)]
    pub local_claims: Vec<Claim>,
}

/// session recap / finalize 后的产物。
///
/// 调用方会先执行 `resolve_placeholders`，再反序列化成此结构并做领域校验。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecapOutcome {
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub new_claims: Vec<ClaimDraft>,
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub updated_claims: Vec<ClaimDraft>,
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub used_claim_ids: Vec<ClaimId>,
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub new_disputes: Vec<DisputeDraft>,
}

/// inbox 内化后的产物
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalizeOutcome {
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub new_claims: Vec<ClaimDraft>,
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub updated_claims: Vec<ClaimDraft>,
    #[serde(default, deserialize_with = "crate::serde_util::null_as_default")]
    pub new_disputes: Vec<DisputeDraft>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimDraft {
    /// 替换后的真实 ClaimId 字符串（替换前是 `$new_claim_N$` 占位符；DTO 层不区分两者）
    pub id: String,
    pub name: String,
    pub statement: String,
    pub scope: String,
    pub confidence: Confidence,
    /// new_claims 中即使误传也会被后端忽略；updated_claims 中必须显式提供。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub evidence_summary: String,
    /// 引用的来源 ID；可含真实 claim/policy id，以及同批新 claim 的占位符。
    #[serde(default)]
    pub source_claim_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisputeDraft {
    /// 替换后的真实 DisputeId 字符串
    pub id: String,
    pub name: String,
    pub claims: Vec<String>,
    pub summary: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::PolicyId;

    #[test]
    fn available_skill_round_trip_json() {
        let skill = AvailableSkill {
            name: "flag_dispute".into(),
            description: "识别潜在 dispute".into(),
            spec_path: "<acn_home>/skills/flag_dispute/SKILL.md".into(),
        };
        let json = serde_json::to_string(&skill).unwrap();
        let back: AvailableSkill = serde_json::from_str(&json).unwrap();
        assert_eq!(skill, back);
    }

    #[test]
    fn recap_outcome_default_is_empty() {
        let o = RecapOutcome::default();
        assert!(o.new_claims.is_empty());
        assert!(o.updated_claims.is_empty());
        assert!(o.used_claim_ids.is_empty());
        assert!(o.new_disputes.is_empty());
    }

    #[test]
    fn recap_outcome_round_trip_json() {
        let o = RecapOutcome {
            new_claims: vec![ClaimDraft {
                id: ClaimId::random().into_string(),
                name: "batch_retry_idempotency".into(),
                statement: "批量订单分片重试必须保持幂等性".into(),
                scope: "order-system / batch-order-submit".into(),
                confidence: Confidence::Medium,
                status: None,
                evidence_summary: "基于借用的 timeout claim 推导".into(),
                source_claim_ids: vec![ClaimId::random().into_string()],
            }],
            updated_claims: vec![],
            used_claim_ids: vec![ClaimId::random()],
            new_disputes: vec![],
        };
        let json = serde_json::to_string(&o).unwrap();
        let back: RecapOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(o, back);
    }

    #[test]
    fn recap_outcome_treats_top_level_null_arrays_as_empty() {
        let json = r#"{
            "new_claims": null,
            "updated_claims": null,
            "used_claim_ids": null,
            "new_disputes": null
        }"#;

        let back: RecapOutcome = serde_json::from_str(json).unwrap();

        assert!(back.new_claims.is_empty());
        assert!(back.updated_claims.is_empty());
        assert!(back.used_claim_ids.is_empty());
        assert!(back.new_disputes.is_empty());
    }

    #[test]
    fn internalize_outcome_round_trip_supports_updated_claims() {
        let o = InternalizeOutcome {
            new_claims: vec![],
            updated_claims: vec![ClaimDraft {
                id: ClaimId::random().into_string(),
                name: "updated_timeout_threshold".into(),
                statement: "支付超时阈值已更新为 45s".into(),
                scope: "order-system / payment-service / prod".into(),
                confidence: Confidence::High,
                status: Some("active".into()),
                evidence_summary: "来自 claim_attribute_update 建议".into(),
                source_claim_ids: vec![PolicyId::random().into_string()],
            }],
            new_disputes: vec![],
        };
        let json = serde_json::to_string(&o).unwrap();
        let back: InternalizeOutcome = serde_json::from_str(&json).unwrap();
        assert_eq!(o, back);
    }

    #[test]
    fn internalize_outcome_treats_top_level_null_arrays_as_empty() {
        let json = r#"{
            "new_claims": null,
            "updated_claims": null,
            "new_disputes": null
        }"#;

        let back: InternalizeOutcome = serde_json::from_str(json).unwrap();

        assert!(back.new_claims.is_empty());
        assert!(back.updated_claims.is_empty());
        assert!(back.new_disputes.is_empty());
    }

    #[test]
    fn dispute_draft_string_id_and_claims_round_trip() {
        let d = DisputeDraft {
            id: "dispute_aabbccdd".into(),
            name: "n".into(),
            claims: vec!["claim_11111111".into(), "claim_22222222".into()],
            summary: "x".into(),
        };
        let json = serde_json::to_string(&d).unwrap();
        let back: DisputeDraft = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);
    }

    #[test]
    fn turn_message_round_trip() {
        let m = TurnMessage {
            role: "assistant".into(),
            content: "...".into(),
        };
        let json = serde_json::to_string(&m).unwrap();
        let back: TurnMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(m, back);
    }
}
