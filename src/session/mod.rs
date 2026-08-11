//! session 模块。
//!
//! 本模块只负责多轮 Session 的本地存储骨架：元数据、路径、system prompt 文件与
//! provider-neutral JSONL transcript。它不执行 turn、不调用 router，也不做 finalize；
//! 交互生命周期由 `SessionEngine` 编排。

use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::api::{
    CompletedSessionTurnMessage, ModelContextSource, ProviderReplayIdentity, ProviderReplayState,
    SessionTurnContentBlock, SessionTurnMessage,
};
use crate::claim::{AgentId, Claim, ClaimId, Dispute, DisputeId, SessionId, TraceId};
use crate::skill::SkillInstructions;
use crate::storage::{
    paths, read_yaml, write_text_atomic, write_yaml_atomic, FileLockGuard, StorageError,
};

mod cleanup;
mod turn_journal;

pub use cleanup::{
    cleanup_old_sessions, SessionCleanupAbortCheck, SessionCleanupConfig, SessionCleanupEntry,
    SessionCleanupOutcome, SessionCleanupReport,
};
pub use turn_journal::{
    canonical_user_content_hash, replay_turn_journal, turn_journal_recovery_context,
    turn_journal_recovery_context_for_chain, CompactionAssetKind, CompactionAssetReference,
    RecoveryContextLimits, TurnJournalEvent, TurnJournalEventKind, TurnJournalFlush,
    TurnJournalModelContext, TurnJournalNonStreamingFallback, TurnJournalNonStreamingFallbackState,
    TurnJournalProjection, TurnJournalRead, TurnJournalStatus, TurnJournalTimelineItem,
    TurnJournalToolCall, TurnJournalTurn, TurnJournalWarning, TurnJournalWriter,
};

#[derive(Debug, thiserror::Error)]
pub enum SessionStoreError {
    #[error("session 存储 I/O 失败 ({path:?}): {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("session JSONL 序列化失败: {0}")]
    JsonEncode(#[from] serde_json::Error),
    #[error("session JSONL 第 {line} 行解析失败: {source}")]
    JsonLine {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("session id 尝试 {max_attempts} 次仍碰撞（最近一次候选 id={last_id}），目录={sessions_dir:?}")]
    IdCollisionExhausted {
        max_attempts: usize,
        last_id: String,
        sessions_dir: PathBuf,
    },
    #[error("session id 生成最大尝试次数必须 >= 1")]
    InvalidMaxAttempts,
    #[error("未知 session message role: {0}")]
    UnknownRole(String),
    #[error("session {0} 已关闭，不能继续写入消息")]
    Closed(String),
    #[error("session {session_id} 不属于 agent {agent_id}")]
    WrongAgent {
        session_id: String,
        agent_id: String,
    },
    #[error("session {session_id} 当前状态为 {status:?}，不能 resume")]
    NotClosed {
        session_id: String,
        status: SessionStatus,
    },
    #[error("session {session_id} message_count={message_count} 与 messages.jsonl 行数 {actual_count} 不一致")]
    MessageCountMismatch {
        session_id: String,
        message_count: usize,
        actual_count: usize,
    },
    #[error(
        "session {session_id} recapped_until={recapped_until} 超过 message_count={message_count}"
    )]
    RecappedUntilOutOfBounds {
        session_id: String,
        recapped_until: usize,
        message_count: usize,
    },
    #[error(
        "session {session_id} compacted_until={compacted_until} 超过 message_count={message_count}"
    )]
    CompactedUntilOutOfBounds {
        session_id: String,
        compacted_until: usize,
        message_count: usize,
    },
    #[error("session {session_id} messages.jsonl 第 {line} 条 index={actual_index}，期望 {expected_index}")]
    MessageIndexMismatch {
        session_id: String,
        line: usize,
        expected_index: usize,
        actual_index: usize,
    },
    #[error(
        "session messages 已写入 canonical transcript，但 metadata 更新失败: message_count={message_count}, model={model}: {source}"
    )]
    MessagesCommittedMetadataUpdateFailed {
        message_count: usize,
        model: String,
        #[source]
        source: Box<SessionStoreError>,
    },
    #[error(
        "session messages 追加失败且无法回滚到 offset={original_len} ({path:?}): append={append_source}; rollback={rollback_source}"
    )]
    MessagesAppendRollbackFailed {
        path: PathBuf,
        original_len: u64,
        append_source: std::io::Error,
        rollback_source: std::io::Error,
    },
    #[error("session cleanup lock 失败 ({path:?}): {source}")]
    CleanupLock {
        path: PathBuf,
        #[source]
        source: anyhow::Error,
    },
    #[error("session 写锁失败 ({path:?}): {source}")]
    SessionLock {
        path: PathBuf,
        #[source]
        source: anyhow::Error,
    },
}

impl SessionStoreError {
    fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionCheckpointStatus {
    Prepared,
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionCheckpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_ids: Vec<String>,
    pub summary_start_index: usize,
    pub summary_end_index: usize,
    pub summary_segment_hash: String,
    pub recap_start_index: usize,
    pub recap_end_index: usize,
    pub recap_segment_hash: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn: Option<ActiveTurnCompactionCursor>,
    #[serde(default)]
    pub prepared_claims: Vec<Claim>,
    #[serde(default)]
    pub prepared_disputes: Vec<Dispute>,
    #[serde(default)]
    pub used_claim_ids: Vec<ClaimId>,
    pub trace_text: String,
    pub trace_created_at: DateTime<Utc>,
    pub trace_id: Option<TraceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_report: Option<CompactionAppliedReport>,
    pub status: CompactionCheckpointStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionAppliedReport {
    pub trace_id: Option<TraceId>,
    #[serde(default)]
    pub new_claim_ids: Vec<ClaimId>,
    #[serde(default)]
    pub updated_claim_ids: Vec<ClaimId>,
    #[serde(default)]
    pub used_claim_ids: Vec<ClaimId>,
    #[serde(default)]
    pub new_dispute_ids: Vec<DisputeId>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinalizeCheckpointStatus {
    Prepared,
    Applied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizeCheckpoint {
    pub recap_start_index: usize,
    pub recap_end_index: usize,
    pub recap_segment_hash: String,
    #[serde(default)]
    pub prepared_claims: Vec<Claim>,
    #[serde(default)]
    pub prepared_disputes: Vec<Dispute>,
    #[serde(default)]
    pub used_claim_ids: Vec<ClaimId>,
    pub trace_text: String,
    pub trace_created_at: DateTime<Utc>,
    pub trace_id: Option<TraceId>,
    pub status: FinalizeCheckpointStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionCompactionState {
    pub committed_summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn_summary: Option<String>,
    pub summary_updated_at: DateTime<Utc>,
    pub frontier: CompactionFrontier,
    /// 最近一次实际发送的 provider-neutral 历史窗口。
    /// 普通 main request 与 compaction 后请求共用该有界 WAL；后续请求
    /// 从稳定基线与 canonical cursor 继续追加，禁止重新投影旧前缀。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_history: Option<Box<CompactedProviderHistory>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactedProviderHistory {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_identity: Option<ProviderReplayIdentity>,
    /// `None` 表示 cursor 已随 canonical commit 确认；`Some` 表示这是当前 turn
    /// 的 write-ahead 窗口。它可以是最后一次实际请求，也可以暂存尚待 canonical
    /// commit 接受的 response-inclusive history，cursor 可暂时领先 messages.jsonl。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_turn: Option<PendingProviderHistoryTurn>,
    pub canonical_message_until: usize,
    pub messages: Vec<SessionTurnMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingProviderHistoryTurn {
    pub turn_id: String,
    pub base_message_count: usize,
    /// `messages[..provider_request_message_count]` 是本 turn 最后一次真正发给
    /// Provider 的请求。其后的 response suffix 只有 canonical commit 后才能成为
    /// 稳定历史；失败、取消或 crash 恢复时必须裁掉。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_request_message_count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionFrontier {
    pub committed_message_until: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_turn: Option<ActiveTurnCompactionCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveTurnCompactionCursor {
    pub turn_id: String,
    pub base_message_count: usize,
    pub compacted_until_segment: usize,
    pub safe_until_event_seq: u64,
    pub source_hash: String,
}

impl SessionCompactionState {
    pub fn from_committed_summary(
        committed_message_until: usize,
        committed_summary: String,
        summary_updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            committed_summary,
            active_turn_summary: None,
            summary_updated_at,
            frontier: CompactionFrontier {
                committed_message_until,
                active_turn: None,
            },
            provider_history: None,
        }
    }

    pub fn committed_message_until(&self) -> usize {
        self.frontier.committed_message_until
    }

    pub fn committed_summary(&self) -> &str {
        &self.committed_summary
    }

    pub fn normalize_active_turn(&mut self) {
        let has_active_summary = self
            .active_turn_summary
            .as_deref()
            .is_some_and(|summary| !summary.trim().is_empty());
        if !has_active_summary || self.frontier.active_turn.is_none() {
            self.active_turn_summary = None;
            self.frontier.active_turn = None;
        }
    }
}

impl<'de> Deserialize<'de> for SessionCompactionState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(default)]
            committed_summary: Option<String>,
            #[serde(default)]
            active_turn_summary: Option<String>,
            summary_updated_at: DateTime<Utc>,
            #[serde(default)]
            frontier: Option<CompactionFrontier>,
            #[serde(default)]
            compacted_until: Option<usize>,
            #[serde(default)]
            summary: Option<String>,
            #[serde(default)]
            provider_history: Option<Box<CompactedProviderHistory>>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let committed_message_until = wire
            .frontier
            .as_ref()
            .map(|frontier| frontier.committed_message_until)
            .or(wire.compacted_until)
            .unwrap_or(0);
        let frontier = wire.frontier.unwrap_or(CompactionFrontier {
            committed_message_until,
            active_turn: None,
        });
        let mut state = Self {
            committed_summary: wire.committed_summary.or(wire.summary).unwrap_or_default(),
            active_turn_summary: wire.active_turn_summary,
            summary_updated_at: wire.summary_updated_at,
            frontier,
            provider_history: wire.provider_history,
        };
        state.normalize_active_turn();
        Ok(state)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub id: SessionId,
    pub agent_id: AgentId,
    pub status: SessionStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    #[serde(default = "default_session_source")]
    pub source: String,
    #[serde(default = "default_session_model")]
    pub model: String,
    pub system_prompt_path: String,
    pub message_count: usize,
    pub finalized_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub recapped_until: usize,
    #[serde(default)]
    pub compaction: Option<SessionCompactionState>,
}

fn default_session_source() -> String {
    "tui".to_string()
}

fn default_session_model() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Open,
    Finalizing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPaths {
    pub dir: PathBuf,
    /// compact 超限降级使用的不可变内容寻址资产；跟随 session 生命周期清理。
    pub compaction_assets_dir: PathBuf,
    pub session_yaml: PathBuf,
    pub system_prompt: PathBuf,
    pub messages_jsonl: PathBuf,
    pub turn_events_jsonl: PathBuf,
    pub compaction_events_jsonl: PathBuf,
    pub compaction_checkpoint_yaml: PathBuf,
    pub finalize_checkpoint_yaml: PathBuf,
    pub finalize_lock: PathBuf,
    pub session_lock: PathBuf,
    pub session_events_log: PathBuf,
}

impl SessionPaths {
    pub(crate) fn new(agent_home: &Path, session_id: &SessionId) -> Self {
        let dir = paths::agent_home_session_dir(agent_home, session_id);
        Self {
            compaction_assets_dir: dir.join("compaction_assets"),
            session_yaml: dir.join("session.yaml"),
            system_prompt: dir.join("system_prompt.md"),
            messages_jsonl: dir.join("messages.jsonl"),
            turn_events_jsonl: dir.join("turn_events.jsonl"),
            compaction_events_jsonl: dir.join("compaction_events.jsonl"),
            compaction_checkpoint_yaml: dir.join("compaction_checkpoint.yaml"),
            finalize_checkpoint_yaml: dir.join("finalize_checkpoint.yaml"),
            finalize_lock: dir.join("finalize.lock"),
            session_lock: dir.join("session.lock"),
            session_events_log: dir.join("session_events.log"),
            dir,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMessage {
    pub index: usize,
    pub role: SessionMessageRole,
    pub content: Vec<SessionContentBlock>,
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_session_model")]
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_replay: Option<ProviderReplayState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionMessageRole {
    User,
    Assistant,
}

impl TryFrom<&str> for SessionMessageRole {
    type Error = SessionStoreError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "user" => Ok(Self::User),
            "assistant" => Ok(Self::Assistant),
            other => Err(SessionStoreError::UnknownRole(other.to_string())),
        }
    }
}

impl fmt::Display for SessionMessageRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User => f.write_str("user"),
            Self::Assistant => f.write_str("assistant"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionContentBlock {
    Text {
        text: String,
    },
    ModelContext {
        source: ModelContextSource,
        fingerprint: String,
        text: String,
    },
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
        /// 文档原始文件名；旧 JSONL transcript 无此字段，反序列化默认 None。
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

impl SessionContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
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

    pub fn tool_use(id: impl Into<String>, name: impl Into<String>, input: Value) -> Self {
        Self::ToolUse {
            id: id.into(),
            name: name.into(),
            input,
        }
    }

    pub fn tool_result(tool_use_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self::ToolResult {
            tool_use_id: tool_use_id.into(),
            content: content.into(),
        }
    }
}

impl From<SessionTurnContentBlock> for SessionContentBlock {
    fn from(block: SessionTurnContentBlock) -> Self {
        match block {
            SessionTurnContentBlock::Text { text } => Self::Text { text },
            SessionTurnContentBlock::ModelContext {
                source,
                fingerprint,
                text,
            } => Self::ModelContext {
                source,
                fingerprint,
                text,
            },
            SessionTurnContentBlock::SkillInstructions { instruction } => {
                Self::SkillInstructions { instruction }
            }
            SessionTurnContentBlock::Image { media_type, data } => Self::Image { media_type, data },
            SessionTurnContentBlock::Document {
                media_type,
                data,
                filename,
            } => Self::Document {
                media_type,
                data,
                filename,
            },
            SessionTurnContentBlock::ToolUse { id, name, input } => {
                Self::ToolUse { id, name, input }
            }
            SessionTurnContentBlock::ToolResult {
                tool_use_id,
                content,
            } => Self::ToolResult {
                tool_use_id,
                content,
            },
        }
    }
}

impl From<SessionContentBlock> for SessionTurnContentBlock {
    fn from(block: SessionContentBlock) -> Self {
        match block {
            SessionContentBlock::Text { text } => Self::Text { text },
            SessionContentBlock::ModelContext {
                source,
                fingerprint,
                text,
            } => Self::ModelContext {
                source,
                fingerprint,
                text,
            },
            SessionContentBlock::SkillInstructions { instruction } => {
                Self::SkillInstructions { instruction }
            }
            SessionContentBlock::Image { media_type, data } => Self::Image { media_type, data },
            SessionContentBlock::Document {
                media_type,
                data,
                filename,
            } => Self::Document {
                media_type,
                data,
                filename,
            },
            SessionContentBlock::ToolUse { id, name, input } => Self::ToolUse { id, name, input },
            SessionContentBlock::ToolResult {
                tool_use_id,
                content,
            } => Self::ToolResult {
                tool_use_id,
                content,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSessionMessage {
    pub role: SessionMessageRole,
    pub content: Vec<SessionContentBlock>,
    pub created_at: DateTime<Utc>,
    pub model: String,
    pub provider_replay: Option<ProviderReplayState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumedSessionSummary {
    pub metadata: SessionMetadata,
    pub last_user_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalTurn {
    pub user_text: String,
    pub assistant_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalTimelineTurn {
    pub user_text: String,
    /// canonical user content 的稳定哈希；新 journal 用它与 `messages.jsonl` 精确对齐。
    pub canonical_user_content_hash: Option<String>,
    pub assistant_text: Option<String>,
    pub assistant_completed: bool,
    pub status: Option<TurnJournalStatus>,
    pub tool_calls: Vec<TurnJournalToolCall>,
    pub timeline_items: Vec<TurnJournalTimelineItem>,
    pub user_steers: Vec<String>,
    /// journal 降级恢复信息；用于明确标注无法确定的关联或 timeline 位置。
    pub recovery_notice: Option<String>,
    /// resume 时替代泛化 turn status 的稳定详情文案。
    pub turn_status_detail: Option<String>,
}

impl From<HistoricalTurn> for HistoricalTimelineTurn {
    fn from(turn: HistoricalTurn) -> Self {
        Self {
            user_text: turn.user_text,
            canonical_user_content_hash: None,
            assistant_completed: turn.assistant_text.is_some(),
            assistant_text: turn.assistant_text,
            status: Some(TurnJournalStatus::Committed),
            tool_calls: Vec::new(),
            timeline_items: Vec::new(),
            user_steers: Vec::new(),
            recovery_notice: None,
            turn_status_detail: None,
        }
    }
}

impl NewSessionMessage {
    pub fn new(role: SessionMessageRole, content: Vec<SessionContentBlock>) -> Self {
        Self {
            role,
            content,
            created_at: Utc::now(),
            model: default_session_model(),
            provider_replay: None,
        }
    }

    pub fn text(role: SessionMessageRole, text: impl Into<String>) -> Self {
        Self::new(role, vec![SessionContentBlock::text(text)])
    }

    pub fn with_created_at(
        role: SessionMessageRole,
        content: Vec<SessionContentBlock>,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self::with_created_at_and_model(role, content, created_at, default_session_model())
    }

    pub fn with_model(
        role: SessionMessageRole,
        content: Vec<SessionContentBlock>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            role,
            content,
            created_at: Utc::now(),
            model: model.into(),
            provider_replay: None,
        }
    }

    pub fn text_with_model(
        role: SessionMessageRole,
        text: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self::with_model(role, vec![SessionContentBlock::text(text)], model)
    }

    pub fn with_created_at_and_model(
        role: SessionMessageRole,
        content: Vec<SessionContentBlock>,
        created_at: DateTime<Utc>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            role,
            content,
            created_at,
            model: model.into(),
            provider_replay: None,
        }
    }

    pub fn with_provider_replay(mut self, provider_replay: ProviderReplayState) -> Self {
        self.provider_replay = Some(provider_replay);
        self
    }
}

#[derive(Debug, Clone)]
pub struct SessionStore {
    agents_root: PathBuf,
}

impl SessionStore {
    pub fn new(agents_root: PathBuf) -> Self {
        Self { agents_root }
    }

    pub async fn create(
        &self,
        agent_id: &AgentId,
        system_prompt: &str,
        max_attempts: usize,
    ) -> Result<SessionHandle, SessionStoreError> {
        self.create_with_id_factory(agent_id, system_prompt, SessionId::random, max_attempts)
            .await
    }

    pub async fn create_with_id_factory<F>(
        &self,
        agent_id: &AgentId,
        system_prompt: &str,
        id_factory: F,
        max_attempts: usize,
    ) -> Result<SessionHandle, SessionStoreError>
    where
        F: FnMut() -> SessionId,
    {
        self.create_with_metadata_id_factory(
            agent_id,
            system_prompt,
            "tui",
            "unknown",
            id_factory,
            max_attempts,
        )
        .await
    }

    pub async fn create_with_metadata_id_factory<F>(
        &self,
        agent_id: &AgentId,
        system_prompt: &str,
        source: impl Into<String>,
        model: impl Into<String>,
        mut id_factory: F,
        max_attempts: usize,
    ) -> Result<SessionHandle, SessionStoreError>
    where
        F: FnMut() -> SessionId,
    {
        if max_attempts == 0 {
            return Err(SessionStoreError::InvalidMaxAttempts);
        }

        let agent_home = self.agents_root.join(agent_id.as_str());
        let sessions_dir = paths::agent_home_sessions_dir(&agent_home);
        fs::create_dir_all(&sessions_dir)
            .await
            .map_err(|e| SessionStoreError::io(&sessions_dir, e))?;

        let mut last_id = None;
        let source = source.into();
        let model = model.into();
        for _ in 0..max_attempts {
            let session_id = id_factory();
            let paths = SessionPaths::new(&agent_home, &session_id);
            match fs::create_dir(&paths.dir).await {
                Ok(()) => {
                    let now = Utc::now();
                    let metadata = SessionMetadata {
                        id: session_id,
                        agent_id: agent_id.clone(),
                        status: SessionStatus::Open,
                        created_at: now,
                        updated_at: now,
                        closed_at: None,
                        source: source.clone(),
                        model: model.clone(),
                        system_prompt_path: "system_prompt.md".to_string(),
                        message_count: 0,
                        finalized_at: None,
                        recapped_until: 0,
                        compaction: None,
                    };
                    write_yaml_atomic(&paths.session_yaml, &metadata).await?;
                    write_text_atomic(&paths.system_prompt, system_prompt.as_bytes()).await?;
                    write_text_atomic(&paths.messages_jsonl, b"").await?;
                    write_text_atomic(&paths.turn_events_jsonl, b"").await?;
                    write_text_atomic(&paths.compaction_events_jsonl, b"").await?;
                    return Ok(SessionHandle { metadata, paths });
                }
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                    last_id = Some(session_id.into_string());
                    continue;
                }
                Err(e) => return Err(SessionStoreError::io(&paths.dir, e)),
            }
        }

        Err(SessionStoreError::IdCollisionExhausted {
            max_attempts,
            last_id: last_id.unwrap_or_else(|| "?".to_string()),
            sessions_dir,
        })
    }

    pub async fn list_resumable_sessions(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<ResumedSessionSummary>, SessionStoreError> {
        let agent_home = self.agents_root.join(agent_id.as_str());
        let lock_path = paths::agent_home_session_cleanup_lock_path(&agent_home);
        let _guard = FileLockGuard::lock_exclusive(&lock_path)
            .await
            .map_err(|source| SessionStoreError::CleanupLock {
                path: lock_path,
                source,
            })?;
        let sessions_dir = paths::agent_home_sessions_dir(&agent_home);
        let mut entries = match fs::read_dir(&sessions_dir).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(SessionStoreError::io(&sessions_dir, e)),
        };
        let mut sessions = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SessionStoreError::io(&sessions_dir, e))?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(|e| SessionStoreError::io(&entry.path(), e))?;
            if !file_type.is_dir() {
                continue;
            }
            let Some(session_id_text) = entry.file_name().to_str().map(ToOwned::to_owned) else {
                continue;
            };
            let Ok(session_id) = SessionId::from_str(&session_id_text) else {
                continue;
            };
            let paths = SessionPaths::new(&agent_home, &session_id);
            let metadata: SessionMetadata = match read_yaml(&paths.session_yaml).await {
                Ok(metadata) => metadata,
                Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {
                    log::warn!(
                        target: "session",
                        "跳过缺少 session.yaml 的半初始化 session 目录: {:?}",
                        paths.dir
                    );
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if metadata.status != SessionStatus::Closed || metadata.agent_id != *agent_id {
                continue;
            }
            let handle = SessionHandle { metadata, paths };
            let messages = handle.read_messages().await?;
            let last_user_text = match extract_last_user_text(&messages) {
                Some(text) => Some(text),
                None => resumable_journal_last_user_text(&handle.paths).await,
            };
            let Some(last_user_text) = last_user_text else {
                continue;
            };
            sessions.push(ResumedSessionSummary {
                last_user_text: Some(last_user_text),
                metadata: handle.metadata,
            });
        }
        sessions.sort_by(|a, b| {
            b.metadata
                .closed_at
                .cmp(&a.metadata.closed_at)
                .then_with(|| b.metadata.updated_at.cmp(&a.metadata.updated_at))
                .then_with(|| b.metadata.id.as_str().cmp(a.metadata.id.as_str()))
        });
        Ok(sessions)
    }

    pub async fn delete_empty_session(
        &self,
        agent_id: &AgentId,
        session_id: &SessionId,
    ) -> Result<bool, SessionStoreError> {
        let agent_home = self.agents_root.join(agent_id.as_str());
        let paths = SessionPaths::new(&agent_home, session_id);
        let metadata: SessionMetadata = match read_yaml(&paths.session_yaml).await {
            Ok(metadata) => metadata,
            Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        };
        if metadata.agent_id != *agent_id {
            return Err(SessionStoreError::WrongAgent {
                session_id: session_id.to_string(),
                agent_id: agent_id.to_string(),
            });
        }
        let handle = SessionHandle { metadata, paths };
        let messages = handle.read_messages().await?;
        if handle.metadata.message_count != 0
            || !messages.is_empty()
            || turn_journal_has_nonempty_content(&handle.paths.turn_events_jsonl).await?
            || session_delegations_dir_has_content(&handle.paths.dir).await?
        {
            return Ok(false);
        }
        match fs::remove_dir_all(&handle.paths.dir).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(SessionStoreError::io(&handle.paths.dir, e)),
        }
    }

    pub async fn open_existing_session(
        &self,
        agent_id: &AgentId,
        session_id: &SessionId,
    ) -> Result<SessionHandle, SessionStoreError> {
        let handle = self.load_existing_session(agent_id, session_id).await?;
        if handle.metadata.status != SessionStatus::Closed {
            return Err(SessionStoreError::NotClosed {
                session_id: session_id.to_string(),
                status: handle.metadata.status,
            });
        }
        Ok(handle)
    }

    pub async fn load_existing_session(
        &self,
        agent_id: &AgentId,
        session_id: &SessionId,
    ) -> Result<SessionHandle, SessionStoreError> {
        let agent_home = self.agents_root.join(agent_id.as_str());
        let paths = SessionPaths::new(&agent_home, session_id);
        let metadata: SessionMetadata = read_yaml(&paths.session_yaml).await?;
        if metadata.agent_id != *agent_id {
            return Err(SessionStoreError::WrongAgent {
                session_id: session_id.to_string(),
                agent_id: agent_id.to_string(),
            });
        }
        let mut handle = SessionHandle { metadata, paths };
        let messages = handle.read_messages().await?;
        validate_and_reconcile_resume_metadata(&mut handle, &messages).await?;
        Ok(handle)
    }

    pub async fn reopen_existing_session(
        &self,
        agent_id: &AgentId,
        session_id: &SessionId,
    ) -> Result<SessionHandle, SessionStoreError> {
        self.with_session_cleanup_lock(agent_id, || async {
            let mut handle = self.open_existing_session(agent_id, session_id).await?;
            handle.mark_open(Utc::now()).await?;
            Ok(handle)
        })
        .await
    }

    pub async fn with_session_cleanup_lock<T, E, F, Fut>(
        &self,
        agent_id: &AgentId,
        operation: F,
    ) -> Result<T, E>
    where
        E: From<SessionStoreError>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let agent_home = self.agents_root.join(agent_id.as_str());
        let lock_path = paths::agent_home_session_cleanup_lock_path(&agent_home);
        let _guard = match FileLockGuard::lock_exclusive(&lock_path).await {
            Ok(guard) => guard,
            Err(source) => {
                return Err(E::from(SessionStoreError::CleanupLock {
                    path: lock_path,
                    source,
                }));
            }
        };
        operation().await
    }
}

pub fn extract_last_n_turns(messages: &[SessionMessage], n: usize) -> Vec<HistoricalTurn> {
    if n == 0 {
        return Vec::new();
    }
    let mut turns = Vec::new();
    let mut current: Option<HistoricalTurn> = None;
    for message in messages {
        match message.role {
            SessionMessageRole::User if is_real_user_message(message) => {
                if let Some(turn) = current.take() {
                    turns.push(turn);
                }
                current = Some(HistoricalTurn {
                    user_text: user_display_text_from_blocks(&message.content),
                    assistant_text: None,
                });
            }
            SessionMessageRole::Assistant => {
                if let Some(turn) = current.as_mut() {
                    let text = text_from_blocks(&message.content);
                    if !text.is_empty() {
                        match &mut turn.assistant_text {
                            Some(existing) => {
                                existing.push('\n');
                                existing.push_str(&text);
                            }
                            None => turn.assistant_text = Some(text),
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(turn) = current {
        turns.push(turn);
    }
    let start = turns.len().saturating_sub(n);
    turns.split_off(start)
}

pub fn extract_last_n_timeline_turns(
    messages: &[SessionMessage],
    n: usize,
) -> Vec<HistoricalTimelineTurn> {
    let mut turns = extract_last_n_turns(messages, n)
        .into_iter()
        .map(Into::into)
        .collect::<Vec<HistoricalTimelineTurn>>();
    let hashes = messages
        .iter()
        .filter(|message| is_real_user_message(message))
        .map(|message| canonical_user_content_hash(&message.content).ok())
        .collect::<Vec<_>>();
    let hash_start = hashes.len().saturating_sub(turns.len());
    for (turn, hash) in turns.iter_mut().zip(hashes.into_iter().skip(hash_start)) {
        turn.canonical_user_content_hash = hash;
    }
    turns
}

pub fn extract_last_n_timeline_turns_from_journal(
    projection: &TurnJournalProjection,
    n: usize,
) -> Vec<HistoricalTimelineTurn> {
    if n == 0 {
        return Vec::new();
    }
    let mut turns = projection
        .turns
        .iter()
        .filter_map(|turn| {
            let user_text = journal_user_display_text(turn)?;
            let assistant_text =
                (!turn.assistant_text.trim().is_empty()).then(|| turn.assistant_text.clone());
            Some(HistoricalTimelineTurn {
                user_text,
                canonical_user_content_hash: turn.canonical_user_content_hash.clone(),
                assistant_text,
                assistant_completed: turn.assistant_completed,
                status: turn.status,
                tool_calls: turn.tool_calls.clone(),
                timeline_items: turn.timeline_items.clone(),
                user_steers: turn.user_steers.clone(),
                recovery_notice: None,
                turn_status_detail: turn_status_detail(turn),
            })
        })
        .collect::<Vec<_>>();
    let start = turns.len().saturating_sub(n);
    turns.split_off(start)
}

fn turn_status_detail(turn: &TurnJournalTurn) -> Option<String> {
    let fallback = turn.non_streaming_fallbacks.last()?;
    let error = fallback
        .last_error
        .as_deref()
        .unwrap_or("unknown provider error");
    match (turn.status, fallback.state) {
        (Some(TurnJournalStatus::Failed), TurnJournalNonStreamingFallbackState::AttemptFailed)
            if fallback.attempt >= fallback.max_attempts =>
        {
            Some(format!(
                "Turn failed after non-streaming retries ({}/{}): {error}",
                fallback.attempt, fallback.max_attempts
            ))
        }
        (None, TurnJournalNonStreamingFallbackState::InProgress) => Some(format!(
            "Turn interrupted during non-streaming fallback (attempt {}/{})",
            fallback.attempt, fallback.max_attempts
        )),
        (None, TurnJournalNonStreamingFallbackState::AttemptFailed) => Some(format!(
            "Turn interrupted after non-streaming fallback attempt {}/{} failed: {error}",
            fallback.attempt, fallback.max_attempts
        )),
        (None, TurnJournalNonStreamingFallbackState::Succeeded) => Some(format!(
            "Turn interrupted after non-streaming fallback succeeded (attempt {}/{})",
            fallback.attempt, fallback.max_attempts
        )),
        _ => None,
    }
}

pub fn count_real_user_turns(messages: &[SessionMessage]) -> usize {
    messages
        .iter()
        .filter(|message| is_real_user_message(message))
        .count()
}

async fn resumable_journal_last_user_text(paths: &SessionPaths) -> Option<String> {
    let read = turn_journal::read_turn_journal(&paths.turn_events_jsonl).await;
    if read.events.is_empty() {
        return None;
    }
    let projection = replay_turn_journal(read);
    projection
        .turns
        .iter()
        .rev()
        .filter_map(journal_user_display_text)
        .map(|text| truncate_for_resume_table(&text, 80))
        .find(|text| !text.is_empty())
}

async fn turn_journal_has_nonempty_content(path: &Path) -> Result<bool, SessionStoreError> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(SessionStoreError::io(path, e)),
    };
    Ok(bytes.iter().any(|byte| !byte.is_ascii_whitespace()))
}

async fn session_delegations_dir_has_content(
    session_dir: &Path,
) -> Result<bool, SessionStoreError> {
    let delegations_dir = session_dir.join("delegations");
    let mut entries = match fs::read_dir(&delegations_dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(SessionStoreError::io(&delegations_dir, e)),
    };
    entries
        .next_entry()
        .await
        .map(|entry| entry.is_some())
        .map_err(|e| SessionStoreError::io(&delegations_dir, e))
}

async fn validate_and_reconcile_resume_metadata(
    handle: &mut SessionHandle,
    messages: &[SessionMessage],
) -> Result<(), SessionStoreError> {
    validate_message_indexes(&handle.metadata.id, messages)?;
    let actual_count = messages.len();
    if handle.metadata.message_count > actual_count {
        return Err(SessionStoreError::MessageCountMismatch {
            session_id: handle.metadata.id.to_string(),
            message_count: handle.metadata.message_count,
            actual_count,
        });
    }
    validate_resume_metadata_bounds(&handle.metadata, actual_count)?;
    if handle.metadata.message_count < actual_count {
        let mut metadata = handle.read_metadata().await?;
        metadata.message_count = actual_count;
        if let Some(last) = messages.last() {
            metadata.model = last.model.clone();
        }
        metadata.updated_at = Utc::now();
        write_yaml_atomic(&handle.paths.session_yaml, &metadata).await?;
        handle.metadata = metadata;
    }
    Ok(())
}

fn validate_resume_metadata_bounds(
    metadata: &SessionMetadata,
    actual_count: usize,
) -> Result<(), SessionStoreError> {
    if metadata.recapped_until > actual_count {
        return Err(SessionStoreError::RecappedUntilOutOfBounds {
            session_id: metadata.id.to_string(),
            recapped_until: metadata.recapped_until,
            message_count: actual_count,
        });
    }
    if let Some(compaction) = &metadata.compaction {
        let compacted_until = compaction.committed_message_until();
        if compacted_until > actual_count {
            return Err(SessionStoreError::CompactedUntilOutOfBounds {
                session_id: metadata.id.to_string(),
                compacted_until,
                message_count: actual_count,
            });
        }
    }
    Ok(())
}

fn validate_message_indexes(
    session_id: &SessionId,
    messages: &[SessionMessage],
) -> Result<(), SessionStoreError> {
    for (expected_index, message) in messages.iter().enumerate() {
        if message.index != expected_index {
            return Err(SessionStoreError::MessageIndexMismatch {
                session_id: session_id.to_string(),
                line: expected_index + 1,
                expected_index,
                actual_index: message.index,
            });
        }
    }
    Ok(())
}

fn extract_last_user_text(messages: &[SessionMessage]) -> Option<String> {
    messages
        .iter()
        .rev()
        .find(|message| is_real_user_message(message))
        .map(|message| {
            let text = user_display_text_from_blocks(&message.content);
            truncate_for_resume_table(&text, 80)
        })
        .filter(|text| !text.is_empty())
}

fn is_real_user_message(message: &SessionMessage) -> bool {
    message.role == SessionMessageRole::User
        && !is_user_shell_command_message(message)
        && !message
            .content
            .iter()
            .any(|block| matches!(block, SessionContentBlock::ModelContext { .. }))
        && !message
            .content
            .iter()
            .any(|block| matches!(block, SessionContentBlock::ToolResult { .. }))
}

fn is_user_shell_command_message(message: &SessionMessage) -> bool {
    if message.role != SessionMessageRole::User {
        return false;
    }
    let text = text_from_blocks(&message.content);
    let trimmed = text.trim_start();
    trimmed.starts_with("<user_shell_command>") && trimmed.contains("</user_shell_command>")
}

fn canonical_user_display_text(text: &str) -> Cow<'_, str> {
    let current_request = extract_current_user_request(text).unwrap_or(Cow::Borrowed(text));
    strip_appended_directory_context(current_request)
}

/// 用户气泡只显示真实输入：取第一段正文，并移除 TUI 自动追加的目录列表上下文。
fn user_display_text_from_blocks(blocks: &[SessionContentBlock]) -> String {
    blocks
        .iter()
        .find_map(|block| match block {
            SessionContentBlock::Text { text } => Some(canonical_user_display_text(text)),
            _ => None,
        })
        .unwrap_or_default()
        .into_owned()
}

fn journal_user_display_text(turn: &TurnJournalTurn) -> Option<String> {
    turn.canonical_user_first_text
        .as_deref()
        .map(canonical_user_display_text)
        .map(Cow::into_owned)
        .filter(|text| !text.is_empty())
        .or_else(|| {
            turn.original_user_request
                .as_deref()
                .map(canonical_user_display_text)
                .map(Cow::into_owned)
        })
}

fn strip_appended_directory_context(text: Cow<'_, str>) -> Cow<'_, str> {
    let marker = "\n\n[Referenced directory: ";
    match text.find(marker) {
        Some(index) => Cow::Owned(text[..index].to_string()),
        None => text,
    }
}

fn extract_current_user_request(text: &str) -> Option<Cow<'_, str>> {
    let start_tag = "<current_user_request>";
    let end_tag = "</current_user_request>";
    let start = text.find(start_tag)?.saturating_add(start_tag.len());
    let tail = &text[start..];
    let end = tail.find(end_tag)?;
    let payload = tail[..end].trim_matches('\n');
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) {
        if let Some(text) = value.get("text").and_then(serde_json::Value::as_str) {
            return Some(Cow::Owned(text.to_string()));
        }
    }
    Some(Cow::Borrowed(payload))
}

fn text_from_blocks(blocks: &[SessionContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|block| match block {
            SessionContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn truncate_for_resume_table(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = String::new();
    for ch in normalized.chars().take(max_chars) {
        out.push(ch);
    }
    out
}

#[derive(Debug, Clone)]
pub struct SessionHandle {
    pub metadata: SessionMetadata,
    pub paths: SessionPaths,
}

impl SessionHandle {
    async fn lock_session(&self) -> Result<FileLockGuard, SessionStoreError> {
        FileLockGuard::lock_exclusive(&self.paths.session_lock)
            .await
            .map_err(|source| SessionStoreError::SessionLock {
                path: self.paths.session_lock.clone(),
                source,
            })
    }

    pub async fn read_metadata(&self) -> Result<SessionMetadata, SessionStoreError> {
        Ok(read_yaml(&self.paths.session_yaml).await?)
    }

    pub async fn read_messages(&self) -> Result<Vec<SessionMessage>, SessionStoreError> {
        let raw = fs::read(&self.paths.messages_jsonl)
            .await
            .map_err(|e| SessionStoreError::io(&self.paths.messages_jsonl, e))?;
        let mut messages = Vec::new();
        let has_unterminated_tail = !raw.is_empty() && !raw.ends_with(b"\n");
        let mut lines = raw.split(|byte| *byte == b'\n').enumerate().peekable();
        while let Some((line_no, line)) = lines.next() {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let message: SessionMessage = match serde_json::from_slice(line) {
                Ok(message) => message,
                Err(_) if has_unterminated_tail && lines.peek().is_none() => {
                    log::warn!(
                        target: "session",
                        "忽略 messages.jsonl 未完整写入的末尾残行: path={:?} line={}",
                        self.paths.messages_jsonl,
                        line_no + 1
                    );
                    break;
                }
                Err(source) => {
                    return Err(SessionStoreError::JsonLine {
                        line: line_no + 1,
                        source,
                    });
                }
            };
            messages.push(message);
        }
        Ok(messages)
    }

    /// 为增量追加规范化 JSONL 尾部。
    ///
    /// 正常文件只读取最后一个字节。只有发现文件没有以换行结束时，才读取异常尾部：
    /// 合法 JSON 保留，并要求本次追加先补换行；非法 JSON 视为未提交残行并截断。
    async fn prepare_messages_tail_for_append(&self) -> Result<bool, SessionStoreError> {
        let path = &self.paths.messages_jsonl;
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .await
            .map_err(|e| SessionStoreError::io(path, e))?;
        let len = file
            .metadata()
            .await
            .map_err(|e| SessionStoreError::io(path, e))?
            .len();
        if len == 0 {
            return Ok(false);
        }

        file.seek(SeekFrom::End(-1))
            .await
            .map_err(|e| SessionStoreError::io(path, e))?;
        let mut last_byte = [0u8; 1];
        file.read_exact(&mut last_byte)
            .await
            .map_err(|e| SessionStoreError::io(path, e))?;
        if last_byte[0] == b'\n' {
            return Ok(false);
        }

        let raw = fs::read(path)
            .await
            .map_err(|e| SessionStoreError::io(path, e))?;
        let tail_start = raw
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        let tail = &raw[tail_start..];
        let valid_tail = std::str::from_utf8(tail).ok().is_some_and(|line| {
            line.as_bytes().iter().all(u8::is_ascii_whitespace)
                || serde_json::from_str::<SessionMessage>(line).is_ok()
        });
        if valid_tail {
            return Ok(true);
        }

        let truncate_to = u64::try_from(tail_start).map_err(|_| {
            SessionStoreError::io(
                path,
                std::io::Error::other("messages.jsonl 尾部偏移无法转换为 u64"),
            )
        })?;
        file.set_len(truncate_to)
            .await
            .map_err(|e| SessionStoreError::io(path, e))?;
        file.sync_all()
            .await
            .map_err(|e| SessionStoreError::io(path, e))?;
        log::warn!(
            target: "session",
            "追加前截断 messages.jsonl 未完整写入的末尾残行: path={path:?} offset={truncate_to}"
        );
        Ok(false)
    }

    async fn rollback_messages_append(
        &self,
        file: &mut fs::File,
        original_len: u64,
        append_source: std::io::Error,
    ) -> SessionStoreError {
        let path = &self.paths.messages_jsonl;
        let rollback_result = async {
            file.set_len(original_len).await?;
            file.sync_all().await
        }
        .await;
        match rollback_result {
            Ok(()) => {
                log::warn!(
                    target: "session",
                    "messages.jsonl 追加失败，已回滚到提交前长度: path={path:?} offset={original_len} error={append_source}"
                );
                SessionStoreError::io(path, append_source)
            }
            Err(rollback_source) => SessionStoreError::MessagesAppendRollbackFailed {
                path: path.clone(),
                original_len,
                append_source,
                rollback_source,
            },
        }
    }

    pub async fn append_event_log(
        &self,
        level: impl AsRef<str>,
        message: impl AsRef<str>,
    ) -> Result<(), SessionStoreError> {
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
        let level = level.as_ref();
        let body = message.as_ref().lines().collect::<Vec<_>>().join("\n    ");
        let line = format!("[{timestamp}] {level} {body}\n");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.paths.session_events_log)
            .await
            .map_err(|e| SessionStoreError::io(&self.paths.session_events_log, e))?;
        file.write_all(line.as_bytes())
            .await
            .map_err(|e| SessionStoreError::io(&self.paths.session_events_log, e))?;
        file.flush()
            .await
            .map_err(|e| SessionStoreError::io(&self.paths.session_events_log, e))?;
        Ok(())
    }

    pub async fn open_turn_journal_writer(&self) -> Result<TurnJournalWriter, SessionStoreError> {
        TurnJournalWriter::open(self.paths.turn_events_jsonl.clone()).await
    }

    pub async fn read_turn_journal(&self) -> TurnJournalRead {
        turn_journal::read_turn_journal(&self.paths.turn_events_jsonl).await
    }

    pub async fn append_messages(
        &mut self,
        messages: &[NewSessionMessage],
    ) -> Result<(), SessionStoreError> {
        if messages.is_empty() {
            return Ok(());
        }

        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        if self.metadata.status != SessionStatus::Open || self.metadata.closed_at.is_some() {
            return Err(SessionStoreError::Closed(self.metadata.id.to_string()));
        }

        let start_index = self.metadata.message_count;
        let mut jsonl = String::new();
        for (offset, message) in messages.iter().enumerate() {
            let stored = SessionMessage {
                index: start_index + offset,
                role: message.role,
                content: message.content.clone(),
                created_at: message.created_at,
                model: message.model.clone(),
                provider_replay: message.provider_replay.clone(),
            };
            jsonl.push_str(&serde_json::to_string(&stored)?);
            jsonl.push('\n');
        }

        if self.prepare_messages_tail_for_append().await? {
            jsonl.insert(0, '\n');
        }
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.paths.messages_jsonl)
            .await
            .map_err(|e| SessionStoreError::io(&self.paths.messages_jsonl, e))?;
        let original_len = file
            .metadata()
            .await
            .map_err(|e| SessionStoreError::io(&self.paths.messages_jsonl, e))?
            .len();
        if let Err(source) = file.write_all(jsonl.as_bytes()).await {
            return Err(self
                .rollback_messages_append(&mut file, original_len, source)
                .await);
        }
        if let Err(source) = file.flush().await {
            return Err(self
                .rollback_messages_append(&mut file, original_len, source)
                .await);
        }
        if let Err(source) = file.sync_all().await {
            return Err(self
                .rollback_messages_append(&mut file, original_len, source)
                .await);
        }

        let message_count = start_index + messages.len();
        let metadata_result = async {
            self.metadata = self.read_metadata().await?;
            self.metadata.message_count = message_count;
            if let Some(message) = messages.last() {
                self.metadata.model = message.model.clone();
            }
            self.metadata.updated_at = Utc::now();
            write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
            Ok::<(), SessionStoreError>(())
        }
        .await;
        if let Err(source) = metadata_result {
            return Err(SessionStoreError::MessagesCommittedMetadataUpdateFailed {
                message_count,
                model: messages
                    .last()
                    .map(|message| message.model.clone())
                    .unwrap_or_else(|| self.metadata.model.clone()),
                source: Box::new(source),
            });
        }
        Ok(())
    }

    pub async fn repair_committed_message_metadata(
        &mut self,
        message_count: usize,
        model: impl Into<String>,
    ) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        self.metadata.message_count = message_count;
        self.metadata.model = model.into();
        self.metadata.updated_at = Utc::now();
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn append_session_turn_messages(
        &mut self,
        messages: &[CompletedSessionTurnMessage],
        model: impl AsRef<str>,
    ) -> Result<(), SessionStoreError> {
        let mut next = Vec::with_capacity(messages.len());
        let model = model.as_ref();
        for message in messages {
            let role = SessionMessageRole::try_from(message.message.role.as_str())?;
            let mut next_message = NewSessionMessage::with_created_at_and_model(
                role,
                message
                    .message
                    .content
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect(),
                message.completed_at,
                model,
            );
            next_message.provider_replay = message.message.provider_replay.clone();
            next.push(next_message);
        }
        self.append_messages(&next).await
    }

    pub async fn read_compaction_checkpoint(
        &self,
    ) -> Result<Option<CompactionCheckpoint>, SessionStoreError> {
        match read_yaml(&self.paths.compaction_checkpoint_yaml).await {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_compaction_checkpoint(
        &self,
        checkpoint: &CompactionCheckpoint,
    ) -> Result<(), SessionStoreError> {
        write_yaml_atomic(&self.paths.compaction_checkpoint_yaml, checkpoint).await?;
        Ok(())
    }

    pub async fn read_finalize_checkpoint(
        &self,
    ) -> Result<Option<FinalizeCheckpoint>, SessionStoreError> {
        match read_yaml(&self.paths.finalize_checkpoint_yaml).await {
            Ok(checkpoint) => Ok(Some(checkpoint)),
            Err(StorageError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_finalize_checkpoint(
        &self,
        checkpoint: &FinalizeCheckpoint,
    ) -> Result<(), SessionStoreError> {
        write_yaml_atomic(&self.paths.finalize_checkpoint_yaml, checkpoint).await?;
        Ok(())
    }

    pub async fn update_compaction(
        &mut self,
        compaction: SessionCompactionState,
    ) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        let mut compaction = compaction;
        compaction.normalize_active_turn();
        self.metadata = self.read_metadata().await?;
        self.metadata.compaction = Some(compaction);
        self.metadata.updated_at = Utc::now();
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn update_compaction_and_recapped_until(
        &mut self,
        compaction: SessionCompactionState,
        recapped_until: usize,
    ) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        let mut compaction = compaction;
        compaction.normalize_active_turn();
        self.metadata = self.read_metadata().await?;
        self.metadata.compaction = Some(compaction);
        self.metadata.recapped_until = self.metadata.recapped_until.max(recapped_until);
        self.metadata.updated_at = Utc::now();
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn advance_recapped_until(
        &mut self,
        recapped_until: usize,
    ) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        self.metadata.recapped_until = self.metadata.recapped_until.max(recapped_until);
        self.metadata.updated_at = Utc::now();
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn recap_and_mark_finalized(
        &mut self,
        recapped_until: usize,
        finalized_at: DateTime<Utc>,
    ) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        self.metadata.recapped_until = self.metadata.recapped_until.max(recapped_until);
        self.metadata.status = SessionStatus::Closed;
        self.metadata.finalized_at = Some(finalized_at);
        self.metadata.closed_at = Some(finalized_at);
        self.metadata.updated_at = finalized_at;
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn mark_finalized(
        &mut self,
        finalized_at: DateTime<Utc>,
    ) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        self.metadata.status = SessionStatus::Closed;
        self.metadata.finalized_at = Some(finalized_at);
        self.metadata.closed_at = Some(finalized_at);
        self.metadata.updated_at = finalized_at;
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn mark_finalizing(
        &mut self,
        updated_at: DateTime<Utc>,
    ) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        if self.metadata.finalized_at.is_some()
            || self.metadata.closed_at.is_some()
            || self.metadata.status != SessionStatus::Open
        {
            return Err(SessionStoreError::Closed(self.metadata.id.to_string()));
        }
        self.metadata.status = SessionStatus::Finalizing;
        self.metadata.updated_at = updated_at;
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn mark_open(&mut self, updated_at: DateTime<Utc>) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        if self.metadata.status == SessionStatus::Finalizing {
            return Err(SessionStoreError::Closed(self.metadata.id.to_string()));
        }
        if self.metadata.finalized_at.is_some() && self.metadata.status != SessionStatus::Closed {
            return Err(SessionStoreError::Closed(self.metadata.id.to_string()));
        }
        self.metadata.status = SessionStatus::Open;
        self.metadata.closed_at = None;
        self.metadata.finalized_at = None;
        self.metadata.updated_at = updated_at;
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }

    pub async fn mark_closed(&mut self, closed_at: DateTime<Utc>) -> Result<(), SessionStoreError> {
        let _guard = self.lock_session().await?;
        self.metadata = self.read_metadata().await?;
        self.metadata.status = SessionStatus::Closed;
        self.metadata.closed_at = Some(closed_at);
        self.metadata.updated_at = closed_at;
        write_yaml_atomic(&self.paths.session_yaml, &self.metadata).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::time::Duration;

    fn agent_id(value: &str) -> AgentId {
        AgentId::new(value).unwrap()
    }

    fn session_id(value: &str) -> SessionId {
        SessionId::from_str(value).unwrap()
    }

    async fn create_session(
        store: &SessionStore,
        agent: &AgentId,
        id: SessionId,
        created_at: DateTime<Utc>,
        messages: &[NewSessionMessage],
        close: bool,
    ) -> SessionHandle {
        let mut handle = store
            .create_with_id_factory(agent, "system", || id.clone(), 1)
            .await
            .unwrap();
        handle.append_messages(messages).await.unwrap();
        handle.metadata.created_at = created_at;
        handle.metadata.updated_at = created_at;
        write_yaml_atomic(&handle.paths.session_yaml, &handle.metadata)
            .await
            .unwrap();
        if close {
            handle.mark_closed(created_at).await.unwrap();
        }
        handle
    }

    #[test]
    fn compaction_state_reads_legacy_fields_and_round_trips_new_schema() {
        let legacy: SessionCompactionState = serde_yaml_ng::from_str(
            r#"
compacted_until: 3
summary: old summary
summary_updated_at: 2026-06-30T00:00:00Z
"#,
        )
        .unwrap();
        assert_eq!(legacy.committed_message_until(), 3);
        assert_eq!(legacy.committed_summary(), "old summary");
        assert!(legacy.active_turn_summary.is_none());
        assert!(legacy.frontier.active_turn.is_none());

        let mut state =
            SessionCompactionState::from_committed_summary(4, "new summary".into(), Utc::now());
        state.active_turn_summary = Some("active summary".into());
        state.frontier.active_turn = Some(ActiveTurnCompactionCursor {
            turn_id: "turn_1".into(),
            base_message_count: 4,
            compacted_until_segment: 2,
            safe_until_event_seq: 10,
            source_hash: "abc".into(),
        });

        let encoded = serde_yaml_ng::to_string(&state).unwrap();
        let decoded: SessionCompactionState = serde_yaml_ng::from_str(&encoded).unwrap();

        assert_eq!(decoded, state);
    }

    #[test]
    fn compaction_state_drops_unpaired_active_summary_or_cursor() {
        let summary_without_cursor: SessionCompactionState = serde_yaml_ng::from_str(
            r#"
committed_summary: old summary
active_turn_summary: stale active summary
summary_updated_at: 2026-06-30T00:00:00Z
frontier:
  committed_message_until: 3
"#,
        )
        .unwrap();
        assert!(summary_without_cursor.active_turn_summary.is_none());
        assert!(summary_without_cursor.frontier.active_turn.is_none());

        let cursor_without_summary: SessionCompactionState = serde_yaml_ng::from_str(
            r#"
committed_summary: old summary
summary_updated_at: 2026-06-30T00:00:00Z
frontier:
  committed_message_until: 3
  active_turn:
    turn_id: turn_1
    base_message_count: 3
    compacted_until_segment: 1
    safe_until_event_seq: 10
    source_hash: abc
"#,
        )
        .unwrap();
        assert!(cursor_without_summary.active_turn_summary.is_none());
        assert!(cursor_without_summary.frontier.active_turn.is_none());
    }

    #[test]
    fn resume_table_text_excludes_media_blocks() {
        let blocks = vec![
            SessionContentBlock::text("看下这张图"),
            SessionContentBlock::image("image/png", "QUJDQUJD".repeat(1000)),
            SessionContentBlock::document("application/pdf", "REVG".repeat(1000)),
        ];
        let text = text_from_blocks(&blocks);
        assert_eq!(text, "看下这张图");
    }

    #[test]
    fn document_block_filename_round_trips_and_old_jsonl_stays_compatible() {
        let block = SessionContentBlock::Document {
            media_type: "application/pdf".into(),
            data: "QUJD".into(),
            filename: Some("brief.pdf".into()),
        };
        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains(r#""filename":"brief.pdf""#));
        let back: SessionContentBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(back, block);

        // 旧 transcript 没有 filename 字段，必须能反序列化
        let legacy = r#"{"type":"document","media_type":"application/pdf","data":"QUJD"}"#;
        let back: SessionContentBlock = serde_json::from_str(legacy).unwrap();
        assert_eq!(
            back,
            SessionContentBlock::document("application/pdf", "QUJD")
        );
        // filename 为 None 时序列化省略字段，与旧格式一致
        assert_eq!(serde_json::to_string(&back).unwrap(), legacy);
    }

    #[tokio::test]
    async fn append_session_turn_messages_persists_each_message_completed_at() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        let user_completed_at: DateTime<Utc> = "2026-06-17T09:33:03.718103Z".parse().unwrap();
        let assistant_completed_at: DateTime<Utc> = "2026-06-17T09:33:05.123456Z".parse().unwrap();
        let messages = vec![
            CompletedSessionTurnMessage::new(
                crate::api::SessionTurnMessage::user_text("hello"),
                user_completed_at,
            ),
            CompletedSessionTurnMessage::new(
                crate::api::SessionTurnMessage::assistant_text("done"),
                assistant_completed_at,
            ),
        ];

        handle
            .append_session_turn_messages(&messages, "test-model")
            .await
            .unwrap();

        let stored = handle.read_messages().await.unwrap();
        assert_eq!(stored[0].created_at, user_completed_at);
        assert_eq!(stored[1].created_at, assistant_completed_at);
        let raw = tokio::fs::read_to_string(&handle.paths.messages_jsonl)
            .await
            .unwrap();
        assert!(raw.contains(r#""created_at":"2026-06-17T09:33:03.718103Z""#));
        assert!(raw.contains(r#""created_at":"2026-06-17T09:33:05.123456Z""#));
    }

    #[tokio::test]
    async fn provider_replay_round_trips_with_unknown_item_fields() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaab"), 1)
            .await
            .unwrap();
        let items = vec![serde_json::json!({
            "type": "reasoning",
            "id": "rs_1",
            "encrypted_content": "opaque-value",
            "vendor_extension": {"future": true}
        })];
        let message = crate::api::SessionTurnMessage::assistant_text("done").with_provider_replay(
            ProviderReplayState::OpenAiResponses {
                model: Some("test-model".into()),
                items: items.clone(),
            },
        );

        handle
            .append_session_turn_messages(
                &[CompletedSessionTurnMessage::new(message, Utc::now())],
                "test-model",
            )
            .await
            .unwrap();

        let stored = handle.read_messages().await.unwrap();
        assert_eq!(
            stored[0].provider_replay,
            Some(ProviderReplayState::OpenAiResponses {
                model: Some("test-model".into()),
                items: items.clone()
            })
        );
        let raw = tokio::fs::read_to_string(&handle.paths.messages_jsonl)
            .await
            .unwrap();
        assert!(raw.contains(r#""protocol":"openai_responses""#));
        assert!(raw.contains(r#""vendor_extension""#));
    }

    #[tokio::test]
    async fn anthropic_provider_replay_round_trips_raw_messages() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaac"), 1)
            .await
            .unwrap();
        let messages = vec![serde_json::json!({
            "role":"assistant",
            "content":[{
                "type":"thinking",
                "thinking":"private",
                "signature":"opaque",
                "vendor_extension":{"future":true}
            }]
        })];
        let message = crate::api::SessionTurnMessage::assistant_text("done").with_provider_replay(
            ProviderReplayState::AnthropicMessages {
                model: "test-model".into(),
                messages: messages.clone(),
            },
        );

        handle
            .append_session_turn_messages(
                &[CompletedSessionTurnMessage::new(message, Utc::now())],
                "test-model",
            )
            .await
            .unwrap();

        let stored = handle.read_messages().await.unwrap();
        assert_eq!(
            stored[0].provider_replay,
            Some(ProviderReplayState::AnthropicMessages {
                model: "test-model".into(),
                messages,
            })
        );
    }

    #[test]
    fn legacy_responses_replay_without_model_remains_readable_but_unbound() {
        let raw = r#"{"index":0,"role":"assistant","content":[{"type":"text","text":"done"}],"provider_replay":{"protocol":"openai_responses","items":[{"type":"reasoning","encrypted_content":"opaque"}]},"created_at":"2026-06-17T09:33:05Z","model":"test-model"}"#;

        let message: SessionMessage = serde_json::from_str(raw).unwrap();

        assert!(matches!(
            message.provider_replay,
            Some(ProviderReplayState::OpenAiResponses { model: None, .. })
        ));
    }

    #[test]
    fn legacy_session_message_without_provider_replay_stays_readable() {
        let legacy = r#"{"index":0,"role":"assistant","content":[{"type":"text","text":"done"}],"created_at":"2026-06-17T09:33:05Z","model":"test-model"}"#;

        let message: SessionMessage = serde_json::from_str(legacy).unwrap();

        assert_eq!(message.provider_replay, None);
        assert!(!serde_json::to_string(&message)
            .unwrap()
            .contains("provider_replay"));
    }

    #[tokio::test]
    async fn append_messages_keeps_existing_jsonl_and_appends_new_lines() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();

        handle
            .append_messages(&[NewSessionMessage::text(SessionMessageRole::User, "first")])
            .await
            .unwrap();
        let first_raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        #[cfg(unix)]
        let first_inode = {
            use std::os::unix::fs::MetadataExt;
            tokio::fs::metadata(&handle.paths.messages_jsonl)
                .await
                .unwrap()
                .ino()
        };

        handle
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "second",
            )])
            .await
            .unwrap();

        let final_raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        assert!(final_raw.starts_with(&first_raw));
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let final_inode = tokio::fs::metadata(&handle.paths.messages_jsonl)
                .await
                .unwrap()
                .ino();
            assert_eq!(final_inode, first_inode);
        }
        let stored = handle.read_messages().await.unwrap();
        assert_eq!(
            stored
                .iter()
                .map(|message| message.index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(handle.read_metadata().await.unwrap().message_count, 2);
    }

    #[tokio::test]
    async fn append_messages_preserves_valid_unterminated_legacy_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        handle
            .append_messages(&[NewSessionMessage::text(SessionMessageRole::User, "legacy")])
            .await
            .unwrap();
        let mut legacy_raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        assert_eq!(legacy_raw.pop(), Some(b'\n'));
        tokio::fs::write(&handle.paths.messages_jsonl, &legacy_raw)
            .await
            .unwrap();

        assert_eq!(handle.read_messages().await.unwrap().len(), 1);
        handle
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "new",
            )])
            .await
            .unwrap();

        let final_raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        assert!(final_raw.starts_with(&legacy_raw));
        assert!(final_raw.ends_with(b"\n"));
        let stored = handle.read_messages().await.unwrap();
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].index, 0);
        assert_eq!(stored[1].index, 1);
    }

    #[tokio::test]
    async fn append_messages_truncates_invalid_unterminated_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        handle
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "committed",
            )])
            .await
            .unwrap();
        let committed_raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        let mut damaged_raw = committed_raw.clone();
        damaged_raw.extend_from_slice(&[b'{', b'"', b'x', b'"', b':', b'"', 0xf0, 0x9f]);
        tokio::fs::write(&handle.paths.messages_jsonl, &damaged_raw)
            .await
            .unwrap();

        let before_repair = handle.read_messages().await.unwrap();
        assert_eq!(before_repair.len(), 1);
        handle
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "after repair",
            )])
            .await
            .unwrap();

        let final_raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        assert!(final_raw.starts_with(&committed_raw));
        assert!(!final_raw.windows(2).any(|bytes| bytes == [0xf0, 0x9f]));
        let stored = handle.read_messages().await.unwrap();
        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].index, 0);
        assert_eq!(stored[1].index, 1);
    }

    #[tokio::test]
    async fn rollback_messages_append_restores_committed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        handle
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "committed",
            )])
            .await
            .unwrap();
        let committed_raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        let original_len = u64::try_from(committed_raw.len()).unwrap();
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&handle.paths.messages_jsonl)
            .await
            .unwrap();
        file.write_all(b"{\"partial\":").await.unwrap();
        file.flush().await.unwrap();

        let err = handle
            .rollback_messages_append(
                &mut file,
                original_len,
                std::io::Error::other("injected append failure"),
            )
            .await;

        assert!(matches!(
            err,
            SessionStoreError::Io { path, source }
                if path == handle.paths.messages_jsonl
                    && source.to_string() == "injected append failure"
        ));
        assert_eq!(
            tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap(),
            committed_raw
        );
        assert_eq!(handle.read_metadata().await.unwrap().message_count, 1);
    }

    #[tokio::test]
    async fn append_messages_rejects_missing_jsonl_without_recreating_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        tokio::fs::remove_file(&handle.paths.messages_jsonl)
            .await
            .unwrap();

        let err = handle
            .append_messages(&[NewSessionMessage::text(SessionMessageRole::User, "lost")])
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            SessionStoreError::Io { path, source }
                if path == handle.paths.messages_jsonl && source.kind() == ErrorKind::NotFound
        ));
        assert!(!handle.paths.messages_jsonl.exists());
    }

    #[tokio::test]
    async fn read_messages_rejects_invalid_middle_line() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        handle
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "committed",
            )])
            .await
            .unwrap();
        let mut raw = tokio::fs::read(&handle.paths.messages_jsonl).await.unwrap();
        let valid_line = raw.clone();
        raw.extend_from_slice(b"{\"incomplete\":true\n");
        raw.extend_from_slice(&valid_line);
        tokio::fs::write(&handle.paths.messages_jsonl, raw)
            .await
            .unwrap();

        let err = handle.read_messages().await.unwrap_err();

        assert!(matches!(err, SessionStoreError::JsonLine { line: 2, .. }));
    }

    #[tokio::test]
    async fn create_session_persists_source_and_model_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let handle = store
            .create_with_metadata_id_factory(
                &agent,
                "system",
                "tui",
                "test-model",
                || session_id("session_eeeeeeee"),
                1,
            )
            .await
            .unwrap();

        let metadata = handle.read_metadata().await.unwrap();
        assert_eq!(metadata.source, "tui");
        assert_eq!(metadata.model, "test-model");
    }

    #[tokio::test]
    async fn append_event_log_writes_timestamped_session_log() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();

        handle
            .append_event_log("WARN", "Maintainer inbox 拉取失败：timeout\nbody=None")
            .await
            .unwrap();
        handle
            .append_event_log("INFO", "Session resumed")
            .await
            .unwrap();

        let raw = tokio::fs::read_to_string(&handle.paths.session_events_log)
            .await
            .unwrap();
        assert!(raw.contains("] WARN Maintainer inbox 拉取失败：timeout\n    body=None\n"));
        assert!(raw.contains("] INFO Session resumed\n"));
        assert_eq!(raw.lines().filter(|line| line.starts_with('[')).count(), 2);
    }

    #[tokio::test]
    async fn finalize_checkpoint_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        let checkpoint = FinalizeCheckpoint {
            recap_start_index: 0,
            recap_end_index: 2,
            recap_segment_hash: "hash".into(),
            prepared_claims: Vec::new(),
            prepared_disputes: Vec::new(),
            used_claim_ids: vec![ClaimId::random()],
            trace_text: "trace text".into(),
            trace_created_at: Utc::now(),
            trace_id: Some(TraceId::random()),
            status: FinalizeCheckpointStatus::Prepared,
        };

        handle.write_finalize_checkpoint(&checkpoint).await.unwrap();
        let stored = handle.read_finalize_checkpoint().await.unwrap();

        assert_eq!(stored, Some(checkpoint));
    }

    #[tokio::test]
    async fn append_messages_rejects_finalizing_session() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();

        handle.mark_finalizing(Utc::now()).await.unwrap();
        let err = handle
            .append_messages(&[NewSessionMessage::text(SessionMessageRole::User, "late")])
            .await
            .unwrap_err();

        assert!(matches!(err, SessionStoreError::Closed(_)));
    }

    #[tokio::test]
    async fn append_messages_waits_for_session_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        let guard = handle.lock_session().await.unwrap();
        let mut blocked = handle.clone();
        let append = tokio::spawn(async move {
            blocked
                .append_messages(&[NewSessionMessage::text(SessionMessageRole::User, "late")])
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !append.is_finished(),
            "append should wait while session.lock is held"
        );

        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), append)
            .await
            .expect("append should unblock")
            .expect("append task should not panic")
            .expect("append should succeed");
        let messages = handle.read_messages().await.unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn mark_finalizing_waits_for_session_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let handle = store
            .create_with_id_factory(&agent, "system", || session_id("session_aaaaaaaa"), 1)
            .await
            .unwrap();
        let guard = handle.lock_session().await.unwrap();
        let mut blocked = handle.clone();
        let mark = tokio::spawn(async move { blocked.mark_finalizing(Utc::now()).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !mark.is_finished(),
            "finalizing should wait while session.lock is held"
        );

        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), mark)
            .await
            .expect("finalizing should unblock")
            .expect("finalizing task should not panic")
            .expect("finalizing should succeed");
        let mut late_append = handle.clone();
        let err = late_append
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "too late",
            )])
            .await
            .unwrap_err();
        assert!(matches!(err, SessionStoreError::Closed(_)));
    }

    #[tokio::test]
    async fn mark_open_reopens_finalized_session_and_preserves_recap_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = create_session(
            &store,
            &agent,
            session_id("session_aaaaaaaa"),
            Utc::now(),
            &[
                NewSessionMessage::text(SessionMessageRole::User, "first"),
                NewSessionMessage::text(SessionMessageRole::Assistant, "answer"),
            ],
            false,
        )
        .await;
        handle
            .update_compaction_and_recapped_until(
                SessionCompactionState::from_committed_summary(1, "summary".into(), Utc::now()),
                2,
            )
            .await
            .unwrap();
        handle.mark_finalized(Utc::now()).await.unwrap();

        handle.mark_open(Utc::now()).await.unwrap();

        let metadata = handle.read_metadata().await.unwrap();
        assert!(metadata.finalized_at.is_none());
        assert!(metadata.closed_at.is_none());
        assert_eq!(metadata.status, SessionStatus::Open);
        assert_eq!(metadata.recapped_until, 2);
    }

    #[tokio::test]
    async fn mark_finalizing_rechecks_terminal_state_under_session_lock() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let mut handle = create_session(
            &store,
            &agent,
            session_id("session_aaaaaaaa"),
            Utc::now(),
            &[NewSessionMessage::text(SessionMessageRole::User, "first")],
            true,
        )
        .await;
        handle.mark_finalized(Utc::now()).await.unwrap();

        let err = handle
            .mark_finalizing(Utc::now())
            .await
            .expect_err("finalized session should not enter finalizing");

        assert!(matches!(err, SessionStoreError::Closed(_)));
        let metadata = handle.read_metadata().await.unwrap();
        assert!(metadata.finalized_at.is_some());
        assert_eq!(metadata.status, SessionStatus::Closed);
    }

    #[tokio::test]
    async fn list_resumable_sessions_includes_finalized_closed_and_filters_open_empty_finalizing_other_agent(
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent_a = agent_id("agent-a");
        let agent_b = agent_id("agent-b");
        let now = Utc::now();
        create_session(
            &store,
            &agent_a,
            session_id("session_aaaaaaaa"),
            now,
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "closed a",
            )],
            true,
        )
        .await;
        create_session(
            &store,
            &agent_a,
            session_id("session_bbbbbbbb"),
            now,
            &[NewSessionMessage::text(SessionMessageRole::User, "open a")],
            false,
        )
        .await;
        let mut finalizing = create_session(
            &store,
            &agent_a,
            session_id("session_eeeeeeee"),
            now,
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "finalizing a",
            )],
            false,
        )
        .await;
        finalizing.mark_finalizing(now).await.unwrap();
        let mut finalized = create_session(
            &store,
            &agent_a,
            session_id("session_ffffffff"),
            now,
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "finalized a",
            )],
            true,
        )
        .await;
        finalized.mark_finalized(now).await.unwrap();
        let mut inconsistent_open_finalized = create_session(
            &store,
            &agent_a,
            session_id("session_99999999"),
            now,
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "inconsistent finalized open",
            )],
            false,
        )
        .await;
        inconsistent_open_finalized.metadata.finalized_at = Some(now);
        write_yaml_atomic(
            &inconsistent_open_finalized.paths.session_yaml,
            &inconsistent_open_finalized.metadata,
        )
        .await
        .unwrap();
        create_session(
            &store,
            &agent_b,
            session_id("session_cccccccc"),
            now,
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "closed b",
            )],
            true,
        )
        .await;
        create_session(
            &store,
            &agent_a,
            session_id("session_dddddddd"),
            now,
            &[],
            true,
        )
        .await;

        let sessions = store.list_resumable_sessions(&agent_a).await.unwrap();

        let session_ids = sessions
            .iter()
            .map(|session| session.metadata.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            session_ids,
            vec![
                session_id("session_ffffffff"),
                session_id("session_aaaaaaaa")
            ]
        );
        assert_eq!(sessions[0].last_user_text.as_deref(), Some("finalized a"));
        assert_eq!(sessions[1].last_user_text.as_deref(), Some("closed a"));
        assert!(!session_ids
            .iter()
            .any(|id| id == &session_id("session_99999999")));
        assert!(!session_ids
            .iter()
            .any(|id| id == &session_id("session_bbbbbbbb")));
    }

    #[tokio::test]
    async fn list_resumable_sessions_skips_valid_named_directory_missing_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let resumable_id = session_id("session_aaaaaaaa");
        create_session(
            &store,
            &agent,
            resumable_id.clone(),
            Utc::now(),
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "可恢复会话",
            )],
            true,
        )
        .await;

        let incomplete_id = session_id("session_bbbbbbbb");
        let agent_home = dir.path().join(agent.as_str());
        let incomplete_paths = SessionPaths::new(&agent_home, &incomplete_id);
        fs::create_dir_all(&incomplete_paths.dir).await.unwrap();

        let sessions = store.list_resumable_sessions(&agent).await.unwrap();

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].metadata.id, resumable_id);
    }

    #[tokio::test]
    async fn list_resumable_sessions_propagates_malformed_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let broken_id = session_id("session_bbbbbbbb");
        let agent_home = dir.path().join(agent.as_str());
        let broken_paths = SessionPaths::new(&agent_home, &broken_id);
        fs::create_dir_all(&broken_paths.dir).await.unwrap();
        fs::write(&broken_paths.session_yaml, "status: [")
            .await
            .unwrap();

        let error = store.list_resumable_sessions(&agent).await.unwrap_err();

        assert!(matches!(
            error,
            SessionStoreError::Storage(StorageError::Decode { .. })
        ));
    }

    #[tokio::test]
    async fn list_resumable_sessions_includes_journal_only_interrupted_session() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let now = Utc::now();
        let session = create_session(
            &store,
            &agent,
            session_id("session_aaaaaaaa"),
            now,
            &[],
            true,
        )
        .await;
        let mut writer = session.open_turn_journal_writer().await.unwrap();
        writer
            .append(
                "turn_1",
                now,
                TurnJournalEventKind::TurnStarted,
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();
        writer
            .append(
                "turn_1",
                now,
                TurnJournalEventKind::UserInputAccepted {
                    text: "journal only request".into(),
                },
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();
        writer
            .append(
                "turn_1",
                now,
                TurnJournalEventKind::TurnFinished {
                    status: TurnJournalStatus::InterruptedByUser,
                },
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();

        let sessions = store.list_resumable_sessions(&agent).await.unwrap();

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].metadata.id, session_id("session_aaaaaaaa"));
        assert_eq!(
            sessions[0].last_user_text.as_deref(),
            Some("journal only request")
        );
    }

    #[tokio::test]
    async fn list_resumable_sessions_filters_open_journal_only_session_and_direct_open_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let now = Utc::now();
        let session = create_session(
            &store,
            &agent,
            session_id("session_aaaaaaaa"),
            now,
            &[],
            false,
        )
        .await;
        let mut writer = session.open_turn_journal_writer().await.unwrap();
        writer
            .append(
                "turn_1",
                now,
                TurnJournalEventKind::UserInputAccepted {
                    text: "open session request".into(),
                },
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();

        let sessions = store.list_resumable_sessions(&agent).await.unwrap();
        let err = store
            .open_existing_session(&agent, &session_id("session_aaaaaaaa"))
            .await
            .unwrap_err();

        assert!(sessions.is_empty());
        assert!(matches!(
            err,
            SessionStoreError::NotClosed {
                status: SessionStatus::Open,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn list_resumable_sessions_sorts_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let older = DateTime::parse_from_rfc3339("2026-05-20T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let newer = DateTime::parse_from_rfc3339("2026-05-21T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        create_session(
            &store,
            &agent,
            session_id("session_aaaaaaaa"),
            older,
            &[NewSessionMessage::text(SessionMessageRole::User, "older")],
            true,
        )
        .await;
        create_session(
            &store,
            &agent,
            session_id("session_bbbbbbbb"),
            newer,
            &[NewSessionMessage::text(SessionMessageRole::User, "newer")],
            true,
        )
        .await;

        let sessions = store.list_resumable_sessions(&agent).await.unwrap();

        assert_eq!(sessions[0].metadata.id, session_id("session_bbbbbbbb"));
        assert_eq!(sessions[1].metadata.id, session_id("session_aaaaaaaa"));
    }

    #[tokio::test]
    async fn open_existing_session_rejects_open_session() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        create_session(&store, &agent, id.clone(), Utc::now(), &[], false).await;

        let err = store.open_existing_session(&agent, &id).await.unwrap_err();

        assert!(matches!(err, SessionStoreError::NotClosed { .. }));
    }

    #[tokio::test]
    async fn open_existing_session_rejects_inconsistent_open_finalized_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let mut handle = create_session(
            &store,
            &agent,
            id.clone(),
            Utc::now(),
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "crash turn",
            )],
            false,
        )
        .await;
        handle.metadata.finalized_at = Some(Utc::now());
        write_yaml_atomic(&handle.paths.session_yaml, &handle.metadata)
            .await
            .unwrap();

        let err = store.open_existing_session(&agent, &id).await.unwrap_err();

        assert!(matches!(err, SessionStoreError::NotClosed { .. }));
    }

    #[tokio::test]
    async fn open_existing_session_rejects_wrong_agent() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent_a = agent_id("agent-a");
        let agent_b = agent_id("agent-b");
        let id = session_id("session_aaaaaaaa");
        create_session(&store, &agent_a, id.clone(), Utc::now(), &[], true).await;
        let wrong_agent_store = SessionStore::new(dir.path().to_path_buf());
        let agent_b_home = dir.path().join(agent_b.as_str());
        let agent_b_paths = SessionPaths::new(&agent_b_home, &id);
        fs::create_dir_all(&agent_b_paths.dir).await.unwrap();
        fs::copy(
            dir.path()
                .join(agent_a.as_str())
                .join("sessions")
                .join(id.as_str())
                .join("session.yaml"),
            &agent_b_paths.session_yaml,
        )
        .await
        .unwrap();
        write_text_atomic(&agent_b_paths.messages_jsonl, b"")
            .await
            .unwrap();

        let err = wrong_agent_store
            .open_existing_session(&agent_b, &id)
            .await
            .unwrap_err();

        assert!(matches!(err, SessionStoreError::WrongAgent { .. }));
    }

    #[tokio::test]
    async fn reopen_existing_session_marks_open_and_preserves_pointers() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let mut handle = create_session(
            &store,
            &agent,
            id.clone(),
            Utc::now(),
            &[
                NewSessionMessage::text(SessionMessageRole::User, "first"),
                NewSessionMessage::text(SessionMessageRole::Assistant, "answer"),
            ],
            true,
        )
        .await;
        handle
            .update_compaction_and_recapped_until(
                SessionCompactionState::from_committed_summary(1, "summary".into(), Utc::now()),
                2,
            )
            .await
            .unwrap();
        handle.mark_closed(Utc::now()).await.unwrap();

        let reopened = store.reopen_existing_session(&agent, &id).await.unwrap();

        assert_eq!(reopened.metadata.status, SessionStatus::Open);
        assert!(reopened.metadata.closed_at.is_none());
        assert!(reopened.metadata.finalized_at.is_none());
        assert_eq!(reopened.metadata.recapped_until, 2);
        assert_eq!(
            reopened
                .metadata
                .compaction
                .as_ref()
                .unwrap()
                .committed_message_until(),
            1
        );
    }

    #[tokio::test]
    async fn reopen_existing_session_allows_finalized_session_and_preserves_recap_pointer() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let mut handle = create_session(
            &store,
            &agent,
            id.clone(),
            Utc::now(),
            &[
                NewSessionMessage::text(SessionMessageRole::User, "first"),
                NewSessionMessage::text(SessionMessageRole::Assistant, "answer"),
            ],
            true,
        )
        .await;
        handle
            .update_compaction_and_recapped_until(
                SessionCompactionState::from_committed_summary(1, "summary".into(), Utc::now()),
                2,
            )
            .await
            .unwrap();
        let checkpoint = FinalizeCheckpoint {
            recap_start_index: 0,
            recap_end_index: 2,
            recap_segment_hash: "hash".into(),
            prepared_claims: Vec::new(),
            prepared_disputes: Vec::new(),
            used_claim_ids: vec![ClaimId::random()],
            trace_text: "trace result".into(),
            trace_created_at: Utc::now(),
            trace_id: Some(TraceId::random()),
            status: FinalizeCheckpointStatus::Applied,
        };
        handle.write_finalize_checkpoint(&checkpoint).await.unwrap();
        handle.mark_finalized(Utc::now()).await.unwrap();

        let reopened = store.reopen_existing_session(&agent, &id).await.unwrap();

        assert_eq!(reopened.metadata.status, SessionStatus::Open);
        assert!(reopened.metadata.closed_at.is_none());
        assert!(reopened.metadata.finalized_at.is_none());
        assert_eq!(reopened.metadata.recapped_until, 2);
        assert_eq!(
            reopened
                .metadata
                .compaction
                .as_ref()
                .unwrap()
                .committed_message_until(),
            1
        );
        assert_eq!(
            reopened.read_finalize_checkpoint().await.unwrap(),
            Some(checkpoint)
        );
    }

    #[tokio::test]
    async fn open_existing_session_rejects_message_index_gap() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let handle = create_session(
            &store,
            &agent,
            id.clone(),
            Utc::now(),
            &[
                NewSessionMessage::text(SessionMessageRole::User, "first"),
                NewSessionMessage::text(SessionMessageRole::Assistant, "answer"),
            ],
            true,
        )
        .await;
        let mut messages = handle.read_messages().await.unwrap();
        messages[1].index = 3;
        let raw = messages
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        write_text_atomic(&handle.paths.messages_jsonl, format!("{raw}\n").as_bytes())
            .await
            .unwrap();

        let err = store.open_existing_session(&agent, &id).await.unwrap_err();

        assert!(matches!(
            err,
            SessionStoreError::MessageIndexMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn open_existing_session_repairs_stale_low_message_count() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let handle = create_session(
            &store,
            &agent,
            id.clone(),
            Utc::now(),
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "committed",
            )],
            true,
        )
        .await;
        let mut metadata = handle.read_metadata().await.unwrap();
        metadata.message_count = 0;
        write_yaml_atomic(&handle.paths.session_yaml, &metadata)
            .await
            .unwrap();

        let opened = store.open_existing_session(&agent, &id).await.unwrap();

        assert_eq!(opened.metadata.message_count, 1);
        assert_eq!(
            opened.read_metadata().await.unwrap().message_count,
            opened.read_messages().await.unwrap().len()
        );
    }

    #[tokio::test]
    async fn read_messages_defaults_missing_legacy_model() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let handle = create_session(&store, &agent, id, Utc::now(), &[], false).await;
        write_text_atomic(
            &handle.paths.messages_jsonl,
            br#"{"index":0,"role":"user","content":[{"type":"text","text":"legacy"}],"created_at":"2026-06-15T00:00:00Z"}
"#,
        )
        .await
        .unwrap();

        let messages = handle.read_messages().await.unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].model, "unknown");
    }

    #[tokio::test]
    async fn list_resumable_sessions_filters_without_real_user_message() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let now = Utc::now();
        create_session(
            &store,
            &agent,
            session_id("session_aaaaaaaa"),
            now,
            &[NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "assistant only",
            )],
            true,
        )
        .await;
        create_session(
            &store,
            &agent,
            session_id("session_bbbbbbbb"),
            now,
            &[NewSessionMessage::new(
                SessionMessageRole::User,
                vec![SessionContentBlock::tool_result("toolu_1", "tool output")],
            )],
            true,
        )
        .await;
        create_session(
            &store,
            &agent,
            session_id("session_cccccccc"),
            now,
            &[NewSessionMessage::text(
                SessionMessageRole::User,
                "real user",
            )],
            true,
        )
        .await;

        let sessions = store.list_resumable_sessions(&agent).await.unwrap();

        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].metadata.id, session_id("session_cccccccc"));
        assert_eq!(sessions[0].last_user_text.as_deref(), Some("real user"));
    }

    #[tokio::test]
    async fn delete_empty_session_removes_empty_session_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let handle = create_session(&store, &agent, id.clone(), Utc::now(), &[], false).await;

        let deleted = store.delete_empty_session(&agent, &id).await.unwrap();

        assert!(deleted);
        assert!(!handle.paths.dir.exists());
    }

    #[tokio::test]
    async fn delete_empty_session_keeps_journal_only_session() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let handle = create_session(&store, &agent, id.clone(), Utc::now(), &[], false).await;
        let mut writer = handle.open_turn_journal_writer().await.unwrap();
        writer
            .append(
                "turn_1",
                Utc::now(),
                TurnJournalEventKind::UserInputAccepted {
                    text: "do not delete".into(),
                },
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();

        let deleted = store.delete_empty_session(&agent, &id).await.unwrap();

        assert!(!deleted);
        assert!(handle.paths.dir.exists());
    }

    #[tokio::test]
    async fn delete_empty_session_keeps_delegation_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let handle = create_session(&store, &agent, id.clone(), Utc::now(), &[], false).await;
        tokio::fs::create_dir_all(handle.paths.dir.join("delegations/subagent_aaaaaaaa"))
            .await
            .unwrap();

        let deleted = store.delete_empty_session(&agent, &id).await.unwrap();

        assert!(!deleted);
        assert!(handle.paths.dir.exists());
    }

    #[tokio::test]
    async fn delete_empty_session_keeps_non_empty_session() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let agent = agent_id("agent-a");
        let id = session_id("session_aaaaaaaa");
        let handle = create_session(
            &store,
            &agent,
            id.clone(),
            Utc::now(),
            &[NewSessionMessage::text(SessionMessageRole::User, "hello")],
            false,
        )
        .await;

        let deleted = store.delete_empty_session(&agent, &id).await.unwrap();

        assert!(!deleted);
        assert!(handle.paths.dir.exists());
    }

    #[test]
    fn extract_last_user_text_skips_tool_results() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("real user")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::tool_result("toolu_1", "tool output")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        assert_eq!(
            extract_last_user_text(&messages).as_deref(),
            Some("real user")
        );
    }

    #[test]
    fn extract_last_user_text_skips_shell_command_records() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("real user")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text(
                    "<user_shell_command>\n<command>\necho hi\n</command>\n</user_shell_command>",
                )],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        assert_eq!(
            extract_last_user_text(&messages).as_deref(),
            Some("real user")
        );
    }

    #[test]
    fn count_real_user_turns_skips_tool_result_user_messages() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("first")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::text("assistant")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 2,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::tool_result("toolu_1", "tool output")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 3,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("second")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        assert_eq!(count_real_user_turns(&messages), 2);
    }

    #[test]
    fn model_context_is_not_a_real_or_resumable_user_turn() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("real user")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::ModelContext {
                    source: ModelContextSource::Runtime,
                    fingerprint: "sha256-v1:test".into(),
                    text: "<runtime_context>hidden</runtime_context>".into(),
                }],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        assert_eq!(count_real_user_turns(&messages), 1);
        assert_eq!(
            extract_last_user_text(&messages).as_deref(),
            Some("real user")
        );
        let turns = extract_last_n_timeline_turns(&messages, 5);
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].user_text, "real user");
    }

    #[test]
    fn count_real_user_turns_skips_shell_command_records() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("first")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text(
                    "<user_shell_command>\n<command>\npwd\n</command>\n</user_shell_command>",
                )],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 2,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("second")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        assert_eq!(count_real_user_turns(&messages), 2);
    }

    #[test]
    fn extract_last_n_turns_returns_at_most_n() {
        let mut messages = Vec::new();
        for i in 0..6 {
            messages.push(SessionMessage {
                index: i * 2,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text(format!("user {i}"))],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            });
            messages.push(SessionMessage {
                index: i * 2 + 1,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::text(format!("assistant {i}"))],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            });
        }

        let turns = extract_last_n_turns(&messages, 5);

        assert_eq!(turns.len(), 5);
        assert_eq!(turns[0].user_text, "user 1");
        assert_eq!(turns[4].assistant_text.as_deref(), Some("assistant 5"));
    }

    #[test]
    fn journal_timeline_derives_fallback_resume_status_detail() {
        let projection = TurnJournalProjection {
            warnings: Vec::new(),
            turns: vec![TurnJournalTurn {
                turn_id: "turn_1".into(),
                started_at: None,
                accepted_at: None,
                finished_at: None,
                status: Some(TurnJournalStatus::Failed),
                original_user_request: Some("failed user".into()),
                canonical_user_content_hash: None,
                canonical_user_first_text: None,
                model_context: Vec::new(),
                skill_instructions: Vec::new(),
                compaction_assets: Vec::new(),
                assistant_text: "partial assistant".into(),
                assistant_completed: false,
                tool_calls: Vec::new(),
                timeline_items: Vec::new(),
                user_steers: Vec::new(),
                non_streaming_fallbacks: vec![TurnJournalNonStreamingFallback {
                    attempt: 5,
                    max_attempts: 5,
                    state: TurnJournalNonStreamingFallbackState::AttemptFailed,
                    last_error: Some("network down".into()),
                }],
            }],
        };

        let turns = extract_last_n_timeline_turns_from_journal(&projection, 5);

        assert_eq!(
            turns[0].turn_status_detail.as_deref(),
            Some("Turn failed after non-streaming retries (5/5): network down")
        );
    }

    #[test]
    fn user_display_uses_first_text_block_and_hides_attachment_content() {
        let messages = vec![SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![
                SessionContentBlock::text("请检查 @src/lib.rs"),
                SessionContentBlock::text(
                    "Attached file: lib.rs\nPath: /workspace/src/lib.rs\n\nfn huge() {}",
                ),
            ],
            created_at: Utc::now(),
            model: "test-model".into(),
            provider_replay: None,
        }];

        let turns = extract_last_n_turns(&messages, 1);
        assert_eq!(turns[0].user_text, "请检查 @src/lib.rs");
        assert!(!turns[0].user_text.contains("fn huge"));
    }

    #[test]
    fn user_display_hides_appended_directory_context() {
        let messages = vec![SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![SessionContentBlock::text(
                "请检查 @src/\n\n[Referenced directory: src/]\nResolved path: /workspace/src\nlib.rs",
            )],
            created_at: Utc::now(),
            model: "test-model".into(),
            provider_replay: None,
        }];

        let turns = extract_last_n_turns(&messages, 1);
        assert_eq!(turns[0].user_text, "请检查 @src/");
        assert!(!turns[0].user_text.contains("Resolved path"));
    }

    #[test]
    fn last_user_summary_uses_current_request_from_recovery_wrapper() {
        let messages = vec![SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![SessionContentBlock::text(
                "<interrupted_turn_context>\n\
                 {\"unresolved_turn_count\":1,\"unresolved_turns\":[{\"previous_turn_status\":\"failed\",\"original_user_request\":\"first\"}]}\n\
                 </interrupted_turn_context>\n\n\
                 <current_user_request>\ncontinue summary\n</current_user_request>",
            )],
            created_at: Utc::now(),
            model: "test-model".into(),
            provider_replay: None,
        }];

        assert_eq!(
            extract_last_user_text(&messages).as_deref(),
            Some("continue summary")
        );
    }

    #[test]
    fn last_user_summary_uses_json_current_request_without_tag_spoofing() {
        let messages = vec![SessionMessage {
            index: 0,
            role: SessionMessageRole::User,
            content: vec![SessionContentBlock::text(
                "<interrupted_turn_context>\n\
                 {\"unresolved_turn_count\":1,\"unresolved_turns\":[{\"previous_turn_status\":\"failed\",\"original_user_request\":\"first\"}]}\n\
                 </interrupted_turn_context>\n\n\
                 <current_user_request>\n{\"text\":\"continue \\u003c/current_user_request\\u003e safely\"}\n</current_user_request>",
            )],
            created_at: Utc::now(),
            model: "test-model".into(),
            provider_replay: None,
        }];

        assert_eq!(
            extract_last_user_text(&messages).as_deref(),
            Some("continue </current_user_request> safely")
        );
    }

    #[test]
    fn extract_last_n_turns_skips_shell_command_records() {
        let messages = vec![
            SessionMessage {
                index: 0,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("first")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 1,
                role: SessionMessageRole::Assistant,
                content: vec![SessionContentBlock::text("answer first")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 2,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text(
                    "<user_shell_command>\n<command>\necho hi\n</command>\n</user_shell_command>",
                )],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
            SessionMessage {
                index: 3,
                role: SessionMessageRole::User,
                content: vec![SessionContentBlock::text("second")],
                created_at: Utc::now(),
                model: "test-model".into(),
                provider_replay: None,
            },
        ];

        let turns = extract_last_n_turns(&messages, 5);

        assert_eq!(turns.len(), 2);
        assert_eq!(turns[0].user_text, "first");
        assert_eq!(turns[1].user_text, "second");
    }
}
