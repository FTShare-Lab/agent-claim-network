use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::api::{SessionTurnMessage, ToolExecutionOutcome};
use crate::claim::{AgentId, SessionId};
use crate::tool::diff::FileChange;

pub const DELEGATION_SCHEMA_VERSION: u8 = 1;
pub const SUMMARY_TEXT_LIMIT: usize = 1_200;
pub const SUMMARY_FIELD_LIMIT: usize = 320;
pub const SUMMARY_CHANGED_FILES_LIMIT: usize = 8;
pub const SUMMARY_CHANGED_FILE_LIMIT: usize = 160;
pub const READ_TEXT_MAX_CHARS: usize = 12_000;
pub const MAX_EVENT_TAIL_LIMIT: usize = 50;
pub const DEFAULT_TRANSCRIPT_TAIL_LIMIT: usize = 50;
pub const MAX_TRANSCRIPT_TAIL_LIMIT: usize = 200;
pub const DEFAULT_TRANSCRIPT_TAIL_MAX_CHARS: usize = 20_000;
pub const MAX_TRANSCRIPT_TAIL_MAX_CHARS: usize = 80_000;

const DEFAULT_EVENT_TAIL_LIMIT: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum DelegationIdError {
    #[error("subagent id 前缀错误: 期望 subagent_，实际 {0}")]
    WrongPrefix(String),
    #[error("subagent id 后缀格式错误: {0}（必须是 8 位小写 hex）")]
    BadSuffix(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct DelegationId(String);

impl DelegationId {
    pub const PREFIX: &'static str = "subagent_";

    pub fn random() -> Self {
        let mut buf = [0u8; 4];
        rand::thread_rng().fill_bytes(&mut buf);
        Self(format!("{}{}", Self::PREFIX, hex::encode(buf)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for DelegationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for DelegationId {
    type Err = DelegationIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let suffix = s
            .strip_prefix(Self::PREFIX)
            .ok_or_else(|| DelegationIdError::WrongPrefix(s.to_string()))?;
        if suffix.len() != 8
            || !suffix
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        {
            return Err(DelegationIdError::BadSuffix(suffix.to_string()));
        }
        Ok(Self(s.to_string()))
    }
}

impl<'de> Deserialize<'de> for DelegationId {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegationStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Abandoned,
}

impl DelegationStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Abandoned)
    }

    pub fn sort_rank(self) -> u8 {
        match self {
            Self::Running => 0,
            Self::Queued => 1,
            Self::Failed => 2,
            Self::Completed => 3,
            Self::Abandoned => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationCreateRequest {
    pub parent_session_id: SessionId,
    pub parent_turn_id: String,
    pub owner_agent_id: AgentId,
    pub title: String,
    pub role: String,
    pub objective: String,
    #[serde(default)]
    pub constraints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationMetadata {
    pub schema_version: u8,
    pub id: DelegationId,
    pub parent_session_id: SessionId,
    pub parent_turn_id: String,
    pub owner_agent_id: AgentId,
    pub title: String,
    pub role: String,
    pub objective: String,
    #[serde(default)]
    pub constraints: Vec<String>,
    pub status: DelegationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(default)]
    pub changed_files: Vec<String>,
}

impl DelegationMetadata {
    pub fn new(id: DelegationId, request: DelegationCreateRequest, now: DateTime<Utc>) -> Self {
        Self {
            schema_version: DELEGATION_SCHEMA_VERSION,
            id,
            parent_session_id: request.parent_session_id,
            parent_turn_id: request.parent_turn_id,
            owner_agent_id: request.owner_agent_id,
            title: request.title,
            role: request.role,
            objective: request.objective,
            constraints: request.constraints,
            status: DelegationStatus::Queued,
            created_at: now,
            updated_at: now,
            started_at: None,
            completed_at: None,
            current_step: None,
            progress_summary: None,
            error_summary: None,
            result_ref: None,
            changed_files: Vec::new(),
        }
    }

    pub fn summary(&self) -> DelegationSummary {
        DelegationSummary {
            id: self.id.clone(),
            title: truncate_text(&self.title, SUMMARY_FIELD_LIMIT),
            role: truncate_text(&self.role, SUMMARY_FIELD_LIMIT),
            status: self.status,
            current_step: self
                .current_step
                .as_deref()
                .map(|value| truncate_text(value, SUMMARY_FIELD_LIMIT)),
            created_at: self.created_at,
            updated_at: self.updated_at,
            started_at: self.started_at,
            completed_at: self.completed_at,
            error_summary: self
                .error_summary
                .as_deref()
                .map(|value| truncate_text(value, SUMMARY_FIELD_LIMIT)),
            progress_summary: self
                .progress_summary
                .as_deref()
                .map(|value| truncate_text(value, SUMMARY_TEXT_LIMIT)),
            result_ref: self
                .result_ref
                .as_deref()
                .map(|value| truncate_text(value, SUMMARY_FIELD_LIMIT)),
            changed_files: self
                .changed_files
                .iter()
                .take(SUMMARY_CHANGED_FILES_LIMIT)
                .map(|path| truncate_text(path, SUMMARY_CHANGED_FILE_LIMIT))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationSummary {
    pub id: DelegationId,
    pub title: String,
    pub role: String,
    pub status: DelegationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_ref: Option<String>,
    #[serde(default)]
    pub changed_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationProgress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    pub summary: String,
    #[serde(default)]
    pub artifacts: Vec<DelegationArtifactRef>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationArtifactRef {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationResult {
    pub status: DelegationStatus,
    pub summary: String,
    #[serde(default)]
    pub changed_files: Vec<String>,
    #[serde(default)]
    pub artifacts: Vec<DelegationArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_summary: Option<String>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationEvent {
    pub seq: u64,
    pub at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: DelegationEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationSteering {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub instruction: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DelegationEventKind {
    Created,
    Queued,
    Started,
    StatusChanged {
        from: DelegationStatus,
        to: DelegationStatus,
    },
    ProgressUpdated {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        current_step: Option<String>,
        summary: String,
        #[serde(default)]
        artifacts: Vec<DelegationArtifactRef>,
    },
    Steered {
        instruction: String,
    },
    ToolStarted {
        tool_name: String,
        summary: String,
    },
    ToolCompleted {
        tool_name: String,
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolExecutionOutcome>,
    },
    CompactionFailed {
        error: String,
    },
    Completed {
        summary: String,
        #[serde(default)]
        changed_files: Vec<String>,
    },
    Failed {
        error: String,
    },
    Abandoned {
        reason: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum DelegationReadMode {
    #[default]
    Summary,
    Result,
    EventsTail {
        limit: usize,
    },
    TranscriptTail {
        limit: usize,
        max_chars: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DelegationRead {
    Summary {
        summary: DelegationSummary,
        progress: Option<DelegationProgress>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compaction_summary: Option<String>,
    },
    Result {
        summary: DelegationSummary,
        result_markdown: Option<String>,
        truncated: bool,
    },
    EventsTail {
        summary: DelegationSummary,
        events: Vec<DelegationEvent>,
    },
    TranscriptTail {
        summary: DelegationSummary,
        entries: Vec<DelegationTranscriptEntry>,
        truncated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationTranscriptEntry {
    pub at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: DelegationTranscriptKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DelegationTranscriptKind {
    Message {
        source: DelegationTranscriptMessageSource,
        message: SessionTurnMessage,
    },
    ToolStarted {
        id: String,
        name: String,
        summary: String,
        input_preview: String,
        input_truncated: bool,
    },
    ToolCompleted {
        id: String,
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcome: Option<ToolExecutionOutcome>,
        output_preview: String,
        output_truncated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_change: Option<FileChange>,
    },
    CompactionBoundary {
        compacted_until: usize,
        summary: String,
    },
    CompactionFailed {
        error: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegationTranscriptMessageSource {
    Objective,
    Steering,
    ModelContext,
    Assistant,
    ToolResult,
    CompactionSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationCompactionState {
    pub schema_version: u8,
    pub compacted_until: usize,
    pub summary: String,
    pub summary_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DelegationCompactionEvent {
    pub at: DateTime<Utc>,
    #[serde(flatten)]
    pub kind: DelegationCompactionEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DelegationCompactionEventKind {
    Started {
        compact_start_index: usize,
        compact_end_index: usize,
        reason: String,
    },
    Completed {
        compacted_until: usize,
        summary_chars: usize,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationUpdate {
    pub current_step: Option<String>,
    pub summary: String,
    pub artifacts: Vec<DelegationArtifactRef>,
}

impl DelegationUpdate {
    pub fn progress(self, updated_at: DateTime<Utc>) -> DelegationProgress {
        DelegationProgress {
            current_step: self.current_step,
            summary: self.summary,
            artifacts: self.artifacts,
            updated_at,
        }
    }
}

pub fn default_event_tail_limit() -> usize {
    DEFAULT_EVENT_TAIL_LIMIT
}

pub fn clamp_event_tail_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_EVENT_TAIL_LIMIT)
}

pub fn default_transcript_tail_limit() -> usize {
    DEFAULT_TRANSCRIPT_TAIL_LIMIT
}

pub fn default_transcript_tail_max_chars() -> usize {
    DEFAULT_TRANSCRIPT_TAIL_MAX_CHARS
}

pub fn clamp_transcript_tail_limit(limit: usize) -> usize {
    limit.clamp(1, MAX_TRANSCRIPT_TAIL_LIMIT)
}

pub fn clamp_transcript_tail_max_chars(max_chars: usize) -> usize {
    max_chars.clamp(1, MAX_TRANSCRIPT_TAIL_MAX_CHARS)
}

pub fn truncate_text(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut out = String::new();
    for ch in value.chars().take(max_chars) {
        out.push(ch);
    }
    if out.chars().count() == value.chars().count() {
        return out;
    }
    out.push_str("...");
    out
}

pub fn truncate_text_with_flag(value: &str, max_chars: usize) -> (String, bool) {
    if value.chars().count() <= max_chars {
        return (value.to_string(), false);
    }
    (truncate_text(value, max_chars), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subagent_id_uses_only_the_public_subagent_prefix() {
        let generated = DelegationId::random();
        assert!(generated.as_str().starts_with("subagent_"));
        assert_eq!(generated.as_str().len(), "subagent_".len() + 8);
        assert!(DelegationId::from_str("subagent_1234abcd").is_ok());
        assert!(matches!(
            DelegationId::from_str("deleg_1234abcd"),
            Err(DelegationIdError::WrongPrefix(_))
        ));
    }

    #[test]
    fn legacy_tool_completed_transcript_without_file_change_still_deserializes() {
        let entry: DelegationTranscriptEntry = serde_json::from_value(serde_json::json!({
            "at": "2026-07-14T00:00:00Z",
            "type": "tool_completed",
            "id": "toolu_1",
            "summary": "file_write ok",
            "outcome": {"kind": "completed"},
            "output_preview": "ok",
            "output_truncated": false
        }))
        .expect("legacy transcript entry should remain compatible");

        assert!(matches!(
            entry.kind,
            DelegationTranscriptKind::ToolCompleted {
                file_change: None,
                ..
            }
        ));
    }
}
