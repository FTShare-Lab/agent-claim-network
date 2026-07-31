use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::claim::SessionId;
use crate::config::{
    ToolConfig, DEFAULT_SESSION_SEARCH_DEFAULT_LIMIT, DEFAULT_SESSION_SEARCH_MAX_LIMIT,
    DEFAULT_SESSION_SEARCH_SQLITE_BUSY_TIMEOUT_MS,
};
use crate::session::{SessionMessage, SessionMetadata};

const DEFAULT_SCROLL_WINDOW: usize = 5;
const MAX_SCROLL_WINDOW: usize = 20;

#[derive(Debug, Clone)]
pub struct SessionSearchConfig {
    pub default_limit: usize,
    pub max_limit: usize,
    pub sqlite_busy_timeout: Duration,
}

impl Default for SessionSearchConfig {
    fn default() -> Self {
        Self {
            default_limit: DEFAULT_SESSION_SEARCH_DEFAULT_LIMIT,
            max_limit: DEFAULT_SESSION_SEARCH_MAX_LIMIT,
            sqlite_busy_timeout: Duration::from_millis(
                DEFAULT_SESSION_SEARCH_SQLITE_BUSY_TIMEOUT_MS,
            ),
        }
    }
}

impl From<&ToolConfig> for SessionSearchConfig {
    fn from(cfg: &ToolConfig) -> Self {
        Self {
            default_limit: cfg.session_search_default_limit,
            max_limit: cfg.session_search_max_limit,
            sqlite_busy_timeout: Duration::from_millis(cfg.session_search_sqlite_busy_timeout_ms),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SessionSearchSort {
    #[default]
    Relevance,
    Newest,
    Oldest,
}

impl SessionSearchSort {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None | Some("") | Some("relevance") => Ok(Self::Relevance),
            Some("newest") => Ok(Self::Newest),
            Some("oldest") => Ok(Self::Oldest),
            Some(other) => Err(format!(
                "invalid sort {other:?}; expected relevance, newest, or oldest"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionSearchRequest {
    pub query: Option<String>,
    pub limit: Option<usize>,
    pub sort: SessionSearchSort,
    pub session_id: Option<SessionId>,
    pub around_message_index: Option<usize>,
    pub window: Option<usize>,
    pub include_tool_results: bool,
}

impl SessionSearchRequest {
    pub fn discovery(query: String, limit: Option<usize>) -> Self {
        Self {
            query: Some(query),
            limit,
            sort: SessionSearchSort::Relevance,
            session_id: None,
            around_message_index: None,
            window: None,
            include_tool_results: false,
        }
    }

    pub(crate) fn normalized_window(&self) -> usize {
        self.window
            .unwrap_or(DEFAULT_SCROLL_WINDOW)
            .clamp(1, MAX_SCROLL_WINDOW)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSearchResponse {
    pub success: bool,
    pub mode: String,
    pub query: String,
    pub results: Vec<SessionSearchResult>,
    pub count: usize,
    pub sessions_searched: usize,
    pub index_incomplete: bool,
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_meta: Option<SessionSearchSessionMeta>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<SessionSearchMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub around_message_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages_before: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages_after: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSearchResult {
    pub session_id: String,
    pub when: String,
    pub source: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_message_index: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bookend_start: Vec<SessionSearchMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<SessionSearchMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bookend_end: Vec<SessionSearchMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages_before: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages_after: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSearchSessionMeta {
    pub when: String,
    pub source: String,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionSearchMessage {
    pub index: usize,
    pub role: String,
    pub model: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub anchor: bool,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub tool_results_omitted: usize,
    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct IndexedSessionCandidate {
    pub session_id: SessionId,
    pub when: String,
    pub source: String,
    pub model: String,
    pub message_count: usize,
    pub matched_role: String,
    pub match_message_index: usize,
    pub snippet: String,
    pub bookend_start: Vec<SessionSearchMessage>,
    pub messages: Vec<SessionSearchMessage>,
    pub bookend_end: Vec<SessionSearchMessage>,
    pub messages_before: usize,
    pub messages_after: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionReadView {
    pub session_id: SessionId,
    pub when: String,
    pub source: String,
    pub model: String,
    pub message_count: usize,
    pub truncated: bool,
    pub messages: Vec<SessionSearchMessage>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionScrollView {
    pub session_id: SessionId,
    pub when: String,
    pub source: String,
    pub model: String,
    pub around_message_index: usize,
    pub window: usize,
    pub messages: Vec<SessionSearchMessage>,
    pub messages_before: usize,
    pub messages_after: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct BrowseSession {
    pub session_id: SessionId,
    pub when: String,
    pub source: String,
    pub model: String,
    pub message_count: usize,
    pub preview: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RepairReport {
    pub index_incomplete: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionDiskData {
    pub metadata: SessionMetadata,
    pub messages: Vec<SessionMessage>,
    pub session_path: PathBuf,
}

pub(crate) fn normalize_limit(limit: Option<usize>, config: &SessionSearchConfig) -> usize {
    let default = config.default_limit.max(1);
    let max = config.max_limit.max(1);
    limit.unwrap_or(default).clamp(1, max)
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}
