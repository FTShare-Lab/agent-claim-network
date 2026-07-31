//! session_search 派生索引与检索服务。
//!
//! 权威数据仍是每个 session 目录下的 `session.yaml` 与 `messages.jsonl`。
//! 本模块只维护 agent 级 SQLite FTS5 缓存，并在 tool 调用时做懒修复、召回、
//! browse/read/scroll 与有界原文 evidence；summarizer adapter 仅保留、尚未对外调用。

use std::path::PathBuf;
use std::sync::Arc;

use crate::api::{SessionSearchSummaryOutcome, SessionSearchSummaryRequest};
use crate::claim::{AgentId, SessionId};
use async_trait::async_trait;

mod disk;
mod index;
mod query;
mod render;
mod search_query;
mod sqlite;
mod types;
mod view;

pub use index::{
    best_effort_index_session_from_files, list_orphaned_sessions_in_index,
    purge_orphaned_sessions_from_index, purge_session_from_index, OrphanedIndexPurge,
};
pub use types::{
    SessionSearchConfig, SessionSearchRequest, SessionSearchResponse, SessionSearchResult,
    SessionSearchSort,
};

use index::{
    browse_index, is_invalid_query_error, read_session, repair_index_for_agent, scroll_session,
    search_index,
};
use query::sanitize_fts5_query;
use types::{
    normalize_limit, BrowseSession, RepairReport, SessionReadView, SessionScrollView,
    SessionSearchSessionMeta,
};

#[async_trait]
pub trait SessionSearchSummarizer: Send + Sync {
    async fn summarize_session_search(
        &self,
        request: SessionSearchSummaryRequest,
    ) -> anyhow::Result<SessionSearchSummaryOutcome>;
}

#[derive(Clone)]
pub struct SessionSearchService {
    agent_id: AgentId,
    agent_home: PathBuf,
    #[allow(
        dead_code,
        reason = "summary path is kept internally but not exposed in tool schema"
    )]
    summarizer: Arc<dyn SessionSearchSummarizer>,
    config: SessionSearchConfig,
}

impl SessionSearchService {
    pub fn new(
        agent_id: AgentId,
        agent_home: PathBuf,
        _model: String,
        summarizer: Arc<dyn SessionSearchSummarizer>,
        config: SessionSearchConfig,
    ) -> Self {
        Self {
            agent_id,
            agent_home,
            summarizer,
            config,
        }
    }

    pub async fn search(
        &self,
        query: String,
        limit: Option<usize>,
        current_session_id: Option<SessionId>,
    ) -> SessionSearchResponse {
        self.run(
            SessionSearchRequest::discovery(query, limit),
            current_session_id,
        )
        .await
    }

    pub async fn run(
        &self,
        request: SessionSearchRequest,
        current_session_id: Option<SessionId>,
    ) -> SessionSearchResponse {
        let limit = normalize_limit(request.limit, &self.config);
        let repair = self.repair_index(current_session_id.clone()).await;
        let warnings = repair.warnings;
        let index_incomplete = repair.index_incomplete;

        if request.session_id.is_none() && request.around_message_index.is_some() {
            return failure_for_mode(
                "scroll",
                append_warning(warnings, "around_message_index requires session_id"),
                index_incomplete,
            );
        }

        if let Some(session_id) = request.session_id.clone() {
            if let Some(around_message_index) = request.around_message_index {
                return self
                    .scroll(
                        session_id,
                        around_message_index,
                        request.normalized_window(),
                        request.include_tool_results,
                        index_incomplete,
                        warnings,
                    )
                    .await;
            }
            return self
                .read(
                    session_id,
                    request.include_tool_results,
                    index_incomplete,
                    warnings,
                )
                .await;
        }

        let query = request.query.as_deref().unwrap_or("").trim().to_string();
        if query.is_empty() {
            return self
                .browse(limit, current_session_id, index_incomplete, warnings)
                .await;
        }
        self.discover(
            request,
            query,
            limit,
            current_session_id,
            index_incomplete,
            warnings,
        )
        .await
    }

    async fn discover(
        &self,
        request: SessionSearchRequest,
        query: String,
        limit: usize,
        current_session_id: Option<SessionId>,
        index_incomplete: bool,
        mut warnings: Vec<String>,
    ) -> SessionSearchResponse {
        if query.is_empty() {
            return failure_for_discover(
                query,
                append_warning(warnings, "query must not be empty"),
                index_incomplete,
            );
        }
        let query = sanitize_fts5_query(&query);
        if query.is_empty() {
            return failure_for_discover(
                query,
                append_warning(warnings, "query has no searchable FTS5 terms"),
                index_incomplete,
            );
        }

        let search = search_index(
            self.agent_home.clone(),
            query.clone(),
            limit,
            current_session_id,
            request.sort,
            request.include_tool_results,
            self.config.sqlite_busy_timeout,
        )
        .await;
        let candidates = match search {
            Ok(candidates) => candidates,
            Err(e) => {
                if is_invalid_query_error(&e) {
                    warnings.push(e.to_string());
                    return SessionSearchResponse {
                        success: false,
                        mode: "discover".into(),
                        query,
                        results: Vec::new(),
                        count: 0,
                        sessions_searched: 0,
                        index_incomplete,
                        warnings,
                        session_id: None,
                        session_meta: None,
                        messages: Vec::new(),
                        message_count: None,
                        truncated: None,
                        around_message_index: None,
                        window: None,
                        messages_before: None,
                        messages_after: None,
                    };
                }
                warnings.push(format!("session search index is unavailable: {e}"));
                return SessionSearchResponse {
                    success: false,
                    mode: "discover".into(),
                    query,
                    results: Vec::new(),
                    count: 0,
                    sessions_searched: 0,
                    index_incomplete: true,
                    warnings,
                    session_id: None,
                    session_meta: None,
                    messages: Vec::new(),
                    message_count: None,
                    truncated: None,
                    around_message_index: None,
                    window: None,
                    messages_before: None,
                    messages_after: None,
                };
            }
        };

        let sessions_searched = candidates.len();
        let results = candidates
            .into_iter()
            .map(|candidate| SessionSearchResult {
                session_id: candidate.session_id.to_string(),
                when: candidate.when,
                source: candidate.source,
                model: candidate.model,
                message_count: Some(candidate.message_count),
                preview: None,
                matched_role: Some(candidate.matched_role),
                match_message_index: Some(candidate.match_message_index),
                snippet: Some(candidate.snippet),
                bookend_start: candidate.bookend_start,
                messages: candidate.messages,
                bookend_end: candidate.bookend_end,
                messages_before: Some(candidate.messages_before),
                messages_after: Some(candidate.messages_after),
            })
            .collect::<Vec<_>>();
        SessionSearchResponse {
            success: true,
            mode: "discover".into(),
            query,
            count: results.len(),
            results,
            sessions_searched,
            index_incomplete,
            warnings,
            session_id: None,
            session_meta: None,
            messages: Vec::new(),
            message_count: None,
            truncated: None,
            around_message_index: None,
            window: None,
            messages_before: None,
            messages_after: None,
        }
    }

    async fn browse(
        &self,
        limit: usize,
        current_session_id: Option<SessionId>,
        index_incomplete: bool,
        mut warnings: Vec<String>,
    ) -> SessionSearchResponse {
        let sessions = match browse_index(
            self.agent_home.clone(),
            limit,
            current_session_id,
            self.config.sqlite_busy_timeout,
        )
        .await
        {
            Ok(sessions) => sessions,
            Err(e) => {
                warnings.push(format!("session search index is unavailable: {e}"));
                return failure_for_mode("browse", warnings, true);
            }
        };
        let sessions_searched = sessions.len();
        let results = sessions
            .into_iter()
            .map(result_from_browse)
            .collect::<Vec<_>>();
        SessionSearchResponse {
            success: true,
            mode: "browse".into(),
            query: String::new(),
            count: results.len(),
            results,
            sessions_searched,
            index_incomplete,
            warnings,
            session_id: None,
            session_meta: None,
            messages: Vec::new(),
            message_count: None,
            truncated: None,
            around_message_index: None,
            window: None,
            messages_before: None,
            messages_after: None,
        }
    }

    async fn read(
        &self,
        session_id: SessionId,
        include_tool_results: bool,
        index_incomplete: bool,
        mut warnings: Vec<String>,
    ) -> SessionSearchResponse {
        match read_session(
            self.agent_home.clone(),
            session_id.clone(),
            include_tool_results,
            self.config.sqlite_busy_timeout,
        )
        .await
        {
            Ok(Some(view)) => response_from_read(view, index_incomplete, warnings),
            Ok(None) => {
                warnings.push(format!("session_id not found: {session_id}"));
                failure_for_mode("read", warnings, index_incomplete)
            }
            Err(e) => {
                warnings.push(format!("failed to read session: {e}"));
                failure_for_mode("read", warnings, true)
            }
        }
    }

    async fn scroll(
        &self,
        session_id: SessionId,
        around_message_index: usize,
        window: usize,
        include_tool_results: bool,
        index_incomplete: bool,
        mut warnings: Vec<String>,
    ) -> SessionSearchResponse {
        match scroll_session(
            self.agent_home.clone(),
            session_id.clone(),
            around_message_index,
            window,
            include_tool_results,
            self.config.sqlite_busy_timeout,
        )
        .await
        {
            Ok(Some(view)) => response_from_scroll(view, index_incomplete, warnings),
            Ok(None) => {
                warnings.push(format!(
                    "around_message_index {around_message_index} not in session_id {session_id}"
                ));
                failure_for_mode("scroll", warnings, index_incomplete)
            }
            Err(e) => {
                warnings.push(format!("failed to scroll session: {e}"));
                failure_for_mode("scroll", warnings, true)
            }
        }
    }

    async fn repair_index(&self, current_session_id: Option<SessionId>) -> RepairReport {
        match repair_index_for_agent(
            self.agent_home.clone(),
            self.agent_id.clone(),
            current_session_id,
            self.config.sqlite_busy_timeout,
        )
        .await
        {
            Ok(report) => report,
            Err(e) => RepairReport {
                index_incomplete: true,
                warnings: vec![format!("session search index repair failed: {e}")],
            },
        }
    }
}

fn result_from_browse(session: BrowseSession) -> SessionSearchResult {
    SessionSearchResult {
        session_id: session.session_id.to_string(),
        when: session.when,
        source: session.source,
        model: session.model,
        message_count: Some(session.message_count),
        preview: Some(session.preview),
        matched_role: None,
        match_message_index: None,
        snippet: None,
        bookend_start: Vec::new(),
        messages: Vec::new(),
        bookend_end: Vec::new(),
        messages_before: None,
        messages_after: None,
    }
}

fn response_from_read(
    view: SessionReadView,
    index_incomplete: bool,
    warnings: Vec<String>,
) -> SessionSearchResponse {
    SessionSearchResponse {
        success: true,
        mode: "read".into(),
        query: String::new(),
        results: Vec::new(),
        count: 0,
        sessions_searched: 0,
        index_incomplete,
        warnings,
        session_id: Some(view.session_id.to_string()),
        session_meta: Some(SessionSearchSessionMeta {
            when: view.when,
            source: view.source,
            model: view.model,
        }),
        messages: view.messages,
        message_count: Some(view.message_count),
        truncated: Some(view.truncated),
        around_message_index: None,
        window: None,
        messages_before: None,
        messages_after: None,
    }
}

fn response_from_scroll(
    view: SessionScrollView,
    index_incomplete: bool,
    warnings: Vec<String>,
) -> SessionSearchResponse {
    SessionSearchResponse {
        success: true,
        mode: "scroll".into(),
        query: String::new(),
        results: Vec::new(),
        count: 0,
        sessions_searched: 0,
        index_incomplete,
        warnings,
        session_id: Some(view.session_id.to_string()),
        session_meta: Some(SessionSearchSessionMeta {
            when: view.when,
            source: view.source,
            model: view.model,
        }),
        messages: view.messages,
        message_count: None,
        truncated: None,
        around_message_index: Some(view.around_message_index),
        window: Some(view.window),
        messages_before: Some(view.messages_before),
        messages_after: Some(view.messages_after),
    }
}

fn failure_for_mode(
    mode: &str,
    warnings: Vec<String>,
    index_incomplete: bool,
) -> SessionSearchResponse {
    SessionSearchResponse {
        success: false,
        mode: mode.into(),
        query: String::new(),
        results: Vec::new(),
        count: 0,
        sessions_searched: 0,
        index_incomplete,
        warnings,
        session_id: None,
        session_meta: None,
        messages: Vec::new(),
        message_count: None,
        truncated: None,
        around_message_index: None,
        window: None,
        messages_before: None,
        messages_after: None,
    }
}

fn failure_for_discover(
    query: String,
    warnings: Vec<String>,
    index_incomplete: bool,
) -> SessionSearchResponse {
    SessionSearchResponse {
        success: false,
        mode: "discover".into(),
        query,
        results: Vec::new(),
        count: 0,
        sessions_searched: 0,
        index_incomplete,
        warnings,
        session_id: None,
        session_meta: None,
        messages: Vec::new(),
        message_count: None,
        truncated: None,
        around_message_index: None,
        window: None,
        messages_before: None,
        messages_after: None,
    }
}

fn append_warning(mut warnings: Vec<String>, warning: impl Into<String>) -> Vec<String> {
    warnings.push(warning.into());
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::SessionSearchSummaryOutcome;
    use crate::session::{
        NewSessionMessage, SessionMessageRole, SessionStore, TurnJournalEventKind, TurnJournalFlush,
    };
    use anyhow::Result;
    use std::str::FromStr;

    #[derive(Debug)]
    struct SummaryLlm {
        fail: bool,
    }

    #[async_trait]
    impl SessionSearchSummarizer for SummaryLlm {
        async fn summarize_session_search(
            &self,
            request: SessionSearchSummaryRequest,
        ) -> Result<SessionSearchSummaryOutcome> {
            if self.fail {
                anyhow::bail!("summary failed");
            }
            Ok(SessionSearchSummaryOutcome {
                summary: format!("summary: {}", request.conversation_text),
            })
        }
    }

    #[tokio::test]
    async fn session_search_excludes_current_session_and_returns_match_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut historical = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_11111111").unwrap(),
                1,
            )
            .await
            .unwrap();
        historical
            .append_messages(&[
                NewSessionMessage::text(SessionMessageRole::User, "docker networking debug"),
                NewSessionMessage::text(SessionMessageRole::Assistant, "bridge mode details"),
            ])
            .await
            .unwrap();
        let mut current = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_22222222").unwrap(),
                1,
            )
            .await
            .unwrap();
        current
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "docker networking current session",
            )])
            .await
            .unwrap();

        let service = SessionSearchService::new(
            agent.clone(),
            dir.path().join(agent.as_str()),
            "test-model".into(),
            Arc::new(SummaryLlm { fail: false }),
            SessionSearchConfig::default(),
        );
        let response = service
            .search(
                "docker".into(),
                Some(5),
                Some(SessionId::from_str("session_22222222").unwrap()),
            )
            .await;

        assert!(response.success, "{response:?}");
        assert_eq!(response.mode, "discover");
        assert_eq!(response.count, 1);
        assert_eq!(response.results[0].session_id, "session_11111111");
        let evidence = response.results[0]
            .messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(evidence.contains("bridge mode details"));
    }

    #[tokio::test]
    async fn session_search_ignores_unresolved_turn_journal_tail() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let journal_only = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_33333333").unwrap(),
                1,
            )
            .await
            .unwrap();
        let mut writer = journal_only.open_turn_journal_writer().await.unwrap();
        writer
            .append(
                "turn_1",
                chrono::Utc::now(),
                TurnJournalEventKind::TurnStarted,
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();
        writer
            .append(
                "turn_1",
                chrono::Utc::now(),
                TurnJournalEventKind::UserInputAccepted {
                    text: "journal-only search needle".into(),
                },
                TurnJournalFlush::Immediate,
            )
            .await
            .unwrap();

        let service = SessionSearchService::new(
            agent,
            dir.path().join("agent-a"),
            "test-model".into(),
            Arc::new(SummaryLlm { fail: false }),
            SessionSearchConfig::default(),
        );
        let response = service.search("journal-only".into(), Some(5), None).await;

        assert!(response.success, "{response:?}");
        assert_eq!(response.count, 0);
    }

    #[tokio::test]
    async fn discovery_does_not_call_summary_path() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut historical = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_33333333").unwrap(),
                1,
            )
            .await
            .unwrap();
        historical
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "docker networking debug",
            )])
            .await
            .unwrap();

        let service = SessionSearchService::new(
            agent.clone(),
            dir.path().join(agent.as_str()),
            "test-model".into(),
            Arc::new(SummaryLlm { fail: true }),
            SessionSearchConfig::default(),
        );
        let response = service.search("docker".into(), Some(3), None).await;

        assert!(response.success);
        assert_eq!(response.sessions_searched, 1);
        assert!(!response.index_incomplete);
        assert!(response.warnings.is_empty(), "{response:?}");
    }

    #[tokio::test]
    async fn dangling_boolean_query_is_sanitized() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut historical = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_44444444").unwrap(),
                1,
            )
            .await
            .unwrap();
        historical
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "docker networking debug",
            )])
            .await
            .unwrap();

        let service = SessionSearchService::new(
            agent.clone(),
            dir.path().join(agent.as_str()),
            "test-model".into(),
            Arc::new(SummaryLlm { fail: false }),
            SessionSearchConfig::default(),
        );
        let response = service.search("docker OR".into(), Some(3), None).await;

        assert!(response.success, "{response:?}");
        assert_eq!(response.query, "docker");
        assert_eq!(response.count, 1);
        assert!(!response.index_incomplete, "{response:?}");
    }

    #[tokio::test]
    async fn empty_query_browses_recent_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut historical = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_55555555").unwrap(),
                1,
            )
            .await
            .unwrap();
        historical
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "browse me",
            )])
            .await
            .unwrap();
        let service = SessionSearchService::new(
            agent.clone(),
            dir.path().join(agent.as_str()),
            "test-model".into(),
            Arc::new(SummaryLlm { fail: false }),
            SessionSearchConfig::default(),
        );
        let response = service.search("  ".into(), Some(3), None).await;

        assert!(response.success);
        assert_eq!(response.mode, "browse");
        assert_eq!(response.count, 1);
        assert!(!response.index_incomplete, "{response:?}");
    }

    #[tokio::test]
    async fn read_and_scroll_return_original_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut historical = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_66666666").unwrap(),
                1,
            )
            .await
            .unwrap();
        historical
            .append_messages(&[
                NewSessionMessage::text_with_model(
                    SessionMessageRole::User,
                    "read start",
                    "model-a",
                ),
                NewSessionMessage::text_with_model(
                    SessionMessageRole::Assistant,
                    "middle anchor",
                    "model-b",
                ),
                NewSessionMessage::text_with_model(SessionMessageRole::User, "read end", "model-b"),
            ])
            .await
            .unwrap();
        let service = SessionSearchService::new(
            agent.clone(),
            dir.path().join(agent.as_str()),
            "test-model".into(),
            Arc::new(SummaryLlm { fail: false }),
            SessionSearchConfig::default(),
        );

        let read = service
            .run(
                SessionSearchRequest {
                    query: None,
                    limit: None,
                    sort: SessionSearchSort::Relevance,
                    session_id: Some(SessionId::from_str("session_66666666").unwrap()),
                    around_message_index: None,
                    window: None,
                    include_tool_results: false,
                },
                None,
            )
            .await;
        assert!(read.success, "{read:?}");
        assert_eq!(read.mode, "read");
        assert_eq!(read.message_count, Some(3));
        assert!(read
            .messages
            .iter()
            .any(|message| message.content.contains("read start")));
        assert!(read
            .messages
            .iter()
            .any(|message| message.content.contains("read start") && message.model == "model-a"));

        let scroll = service
            .run(
                SessionSearchRequest {
                    query: None,
                    limit: None,
                    sort: SessionSearchSort::Relevance,
                    session_id: Some(SessionId::from_str("session_66666666").unwrap()),
                    around_message_index: Some(1),
                    window: Some(1),
                    include_tool_results: false,
                },
                None,
            )
            .await;
        assert!(scroll.success, "{scroll:?}");
        assert_eq!(scroll.mode, "scroll");
        assert_eq!(scroll.window, Some(1));
        assert!(scroll
            .messages
            .iter()
            .any(|message| message.anchor && message.content.contains("middle anchor")));
        assert!(scroll.messages.iter().any(|message| message.anchor
            && message.content.contains("middle anchor")
            && message.model == "model-b"));
    }

    #[tokio::test]
    async fn around_message_index_requires_session_id() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let service = SessionSearchService::new(
            agent.clone(),
            dir.path().join(agent.as_str()),
            "test-model".into(),
            Arc::new(SummaryLlm { fail: false }),
            SessionSearchConfig::default(),
        );

        let response = service
            .run(
                SessionSearchRequest {
                    query: None,
                    limit: None,
                    sort: SessionSearchSort::Relevance,
                    session_id: None,
                    around_message_index: Some(1),
                    window: None,
                    include_tool_results: false,
                },
                None,
            )
            .await;

        assert!(!response.success);
        assert_eq!(response.mode, "scroll");
        assert!(response
            .warnings
            .iter()
            .any(|warning| warning.contains("requires session_id")));
    }
}
