//! agent 侧旧 session 清理。
//!
//! 本模块只基于 session 权威文件判断和删除旧 session 目录；session_search
//! SQLite 是派生索引，目录删除成功后再做 best-effort purge。

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::fs;

use crate::claim::{AgentId, SessionId};
use crate::session_search::{
    list_orphaned_sessions_in_index, purge_orphaned_sessions_from_index, purge_session_from_index,
    OrphanedIndexPurge,
};
use crate::storage::{paths, read_yaml};

use super::{SessionMessage, SessionMetadata, SessionPaths, SessionStatus};

pub type SessionCleanupAbortCheck = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone)]
pub struct SessionCleanupConfig {
    pub agent_id: AgentId,
    pub agent_home: PathBuf,
    pub cutoff: DateTime<Utc>,
    pub apply: bool,
    pub sqlite_busy_timeout: Duration,
    pub abort_check: Option<SessionCleanupAbortCheck>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionCleanupReport {
    pub scanned: usize,
    pub eligible: usize,
    pub deleted: usize,
    pub skipped: usize,
    pub sqlite_purged: usize,
    pub errors: usize,
    pub aborted: bool,
    pub entries: Vec<SessionCleanupEntry>,
}

impl SessionCleanupReport {
    fn push(&mut self, entry: SessionCleanupEntry) {
        match entry.outcome {
            SessionCleanupOutcome::DryRun => {
                self.eligible += 1;
            }
            SessionCleanupOutcome::Deleted => {
                self.eligible += 1;
                self.deleted += 1;
            }
            SessionCleanupOutcome::DeletedWithIndexError => {
                self.eligible += 1;
                self.deleted += 1;
                self.errors += 1;
            }
            SessionCleanupOutcome::Skipped => {
                self.skipped += 1;
            }
            SessionCleanupOutcome::Error => {
                self.errors += 1;
            }
            SessionCleanupOutcome::IndexPurged => {
                self.eligible += 1;
            }
            SessionCleanupOutcome::Aborted => {
                self.aborted = true;
            }
        }
        if entry.sqlite_purged {
            self.sqlite_purged += 1;
        }
        self.entries.push(entry);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCleanupEntry {
    pub session_id: Option<SessionId>,
    pub session_path: PathBuf,
    pub outcome: SessionCleanupOutcome,
    pub reason: String,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub sqlite_purged: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionCleanupOutcome {
    DryRun,
    Deleted,
    DeletedWithIndexError,
    Skipped,
    Error,
    IndexPurged,
    Aborted,
}

#[derive(Debug, Clone)]
struct CleanupCandidate {
    session_id: SessionId,
    paths: SessionPaths,
    last_activity_at: DateTime<Utc>,
    reason: String,
}

pub async fn cleanup_old_sessions(config: SessionCleanupConfig) -> Result<SessionCleanupReport> {
    let sessions_dir = paths::agent_home_sessions_dir(&config.agent_home);
    let mut report = SessionCleanupReport::default();
    let mut dir = match fs::read_dir(&sessions_dir).await {
        Ok(dir) => dir,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            if config.apply {
                if cleanup_abort_requested(&config) {
                    report.aborted = true;
                } else {
                    purge_orphaned_indexes(&config, &mut report).await;
                    if cleanup_abort_requested(&config) {
                        report.aborted = true;
                    }
                }
            } else {
                list_orphaned_indexes(&config, &mut report).await;
            }
            return Ok(report);
        }
        Err(e) => {
            return Err(e)
                .with_context(|| format!("读取 sessions 目录: {}", sessions_dir.display()))
        }
    };

    while let Some(entry) = dir
        .next_entry()
        .await
        .with_context(|| format!("遍历 sessions 目录下一项失败: {}", sessions_dir.display()))?
    {
        if config.apply && cleanup_abort_requested(&config) {
            report.aborted = true;
            break;
        }
        let file_type = entry
            .file_type()
            .await
            .with_context(|| format!("读取 session 目录项类型: {}", entry.path().display()))?;
        if !file_type.is_dir() {
            continue;
        }
        report.scanned += 1;
        let session_path = entry.path();
        match evaluate_candidate(&config, &session_path).await {
            CandidateDecision::Eligible(candidate) => {
                let candidate = *candidate;
                if config.apply {
                    if cleanup_abort_requested(&config) {
                        report.aborted = true;
                        break;
                    }
                    let entry = delete_candidate(&config, candidate).await;
                    let aborted = entry.outcome == SessionCleanupOutcome::Aborted;
                    report.push(entry);
                    if aborted {
                        break;
                    }
                } else {
                    report.push(SessionCleanupEntry {
                        session_id: Some(candidate.session_id),
                        session_path: candidate.paths.dir,
                        outcome: SessionCleanupOutcome::DryRun,
                        reason: candidate.reason,
                        last_activity_at: Some(candidate.last_activity_at),
                        sqlite_purged: false,
                    });
                }
            }
            CandidateDecision::Skipped(entry) => report.push(entry),
        }
        if config.apply && (report.aborted || cleanup_abort_requested(&config)) {
            report.aborted = true;
            break;
        }
    }

    if config.apply {
        if report.aborted || cleanup_abort_requested(&config) {
            report.aborted = true;
        } else {
            purge_orphaned_indexes(&config, &mut report).await;
            if cleanup_abort_requested(&config) {
                report.aborted = true;
            }
        }
    } else {
        list_orphaned_indexes(&config, &mut report).await;
    }

    Ok(report)
}

async fn evaluate_candidate(
    config: &SessionCleanupConfig,
    session_path: &Path,
) -> CandidateDecision {
    let Some(session_id_text) = session_path.file_name().and_then(|name| name.to_str()) else {
        return CandidateDecision::Skipped(skipped(
            None,
            session_path,
            "invalid session directory name",
        ));
    };
    let Ok(session_id) = session_id_text.parse::<SessionId>() else {
        return CandidateDecision::Skipped(skipped(
            None,
            session_path,
            "invalid session id directory name",
        ));
    };
    let paths = SessionPaths::new(&config.agent_home, &session_id);
    let metadata = match read_yaml::<SessionMetadata>(&paths.session_yaml).await {
        Ok(metadata) => metadata,
        Err(e) => {
            return CandidateDecision::Skipped(skipped(
                Some(session_id),
                session_path,
                format!("metadata unreadable: {e}"),
            ));
        }
    };
    if metadata.id != session_id {
        return CandidateDecision::Skipped(skipped(
            Some(session_id.clone()),
            session_path,
            format!(
                "metadata id mismatch: expected {session_id}, found {}",
                metadata.id
            ),
        ));
    }
    if metadata.agent_id != config.agent_id {
        return CandidateDecision::Skipped(skipped(
            Some(session_id),
            session_path,
            "different agent",
        ));
    }
    if metadata.status != SessionStatus::Closed {
        return CandidateDecision::Skipped(skipped(
            Some(session_id),
            session_path,
            format!("status is {:?}", metadata.status),
        ));
    }

    let (last_activity_at, reason) =
        match last_activity_at(&paths.messages_jsonl, session_path).await {
            Ok(last_activity) => last_activity,
            Err(reason) => {
                return CandidateDecision::Skipped(skipped(Some(session_id), session_path, reason));
            }
        };

    if last_activity_at >= config.cutoff {
        return CandidateDecision::Skipped(SessionCleanupEntry {
            session_id: Some(session_id),
            session_path: session_path.to_path_buf(),
            outcome: SessionCleanupOutcome::Skipped,
            reason: "Action exists within cutoff time.".to_string(),
            last_activity_at: Some(last_activity_at),
            sqlite_purged: false,
        });
    }

    CandidateDecision::Eligible(Box::new(CleanupCandidate {
        session_id,
        paths,
        last_activity_at,
        reason,
    }))
}

async fn delete_candidate(
    config: &SessionCleanupConfig,
    candidate: CleanupCandidate,
) -> SessionCleanupEntry {
    let candidate = match evaluate_candidate(config, &candidate.paths.dir).await {
        CandidateDecision::Eligible(candidate) => *candidate,
        CandidateDecision::Skipped(mut entry) => {
            entry.reason = format!("pre-delete {}", entry.reason);
            return entry;
        }
    };
    if cleanup_abort_requested(config) {
        return SessionCleanupEntry {
            session_id: Some(candidate.session_id),
            session_path: candidate.paths.dir,
            outcome: SessionCleanupOutcome::Aborted,
            reason: "cleanup aborted before deleting session directory".to_string(),
            last_activity_at: Some(candidate.last_activity_at),
            sqlite_purged: false,
        };
    }

    match fs::remove_dir_all(&candidate.paths.dir).await {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return SessionCleanupEntry {
                session_id: Some(candidate.session_id),
                session_path: candidate.paths.dir,
                outcome: SessionCleanupOutcome::Skipped,
                reason: "session directory already removed".to_string(),
                last_activity_at: Some(candidate.last_activity_at),
                sqlite_purged: false,
            };
        }
        Err(e) => {
            return SessionCleanupEntry {
                session_id: Some(candidate.session_id),
                session_path: candidate.paths.dir,
                outcome: SessionCleanupOutcome::Error,
                reason: format!("remove session directory failed: {e}"),
                last_activity_at: Some(candidate.last_activity_at),
                sqlite_purged: false,
            };
        }
    }

    match purge_session_from_index(
        config.agent_home.clone(),
        candidate.session_id.clone(),
        config.sqlite_busy_timeout,
    )
    .await
    {
        Ok(sqlite_purged) => SessionCleanupEntry {
            session_id: Some(candidate.session_id),
            session_path: candidate.paths.dir,
            outcome: SessionCleanupOutcome::Deleted,
            reason: candidate.reason,
            last_activity_at: Some(candidate.last_activity_at),
            sqlite_purged,
        },
        Err(e) => SessionCleanupEntry {
            session_id: Some(candidate.session_id),
            session_path: candidate.paths.dir,
            outcome: SessionCleanupOutcome::DeletedWithIndexError,
            reason: format!("{}; sqlite purge failed: {e}", candidate.reason),
            last_activity_at: Some(candidate.last_activity_at),
            sqlite_purged: false,
        },
    }
}

async fn purge_orphaned_indexes(config: &SessionCleanupConfig, report: &mut SessionCleanupReport) {
    match purge_orphaned_sessions_from_index(config.agent_home.clone(), config.sqlite_busy_timeout)
        .await
    {
        Ok(orphaned_sessions) => {
            push_orphaned_index_entries(report, orphaned_sessions, true);
        }
        Err(e) => {
            report.errors += 1;
            report.entries.push(SessionCleanupEntry {
                session_id: None,
                session_path: config.agent_home.clone(),
                outcome: SessionCleanupOutcome::Error,
                reason: format!("orphan sqlite purge failed: {e}"),
                last_activity_at: None,
                sqlite_purged: false,
            });
        }
    }
}

async fn list_orphaned_indexes(config: &SessionCleanupConfig, report: &mut SessionCleanupReport) {
    match list_orphaned_sessions_in_index(config.agent_home.clone(), config.sqlite_busy_timeout)
        .await
    {
        Ok(orphaned_sessions) => {
            push_orphaned_index_entries(report, orphaned_sessions, false);
        }
        Err(e) => {
            report.errors += 1;
            report.entries.push(SessionCleanupEntry {
                session_id: None,
                session_path: config.agent_home.clone(),
                outcome: SessionCleanupOutcome::Error,
                reason: format!("orphan sqlite dry-run failed: {e}"),
                last_activity_at: None,
                sqlite_purged: false,
            });
        }
    }
}

fn push_orphaned_index_entries(
    report: &mut SessionCleanupReport,
    orphaned_sessions: Vec<OrphanedIndexPurge>,
    apply: bool,
) {
    for orphan in orphaned_sessions {
        let raw_session_id = orphan.session_id.clone();
        report.push(SessionCleanupEntry {
            session_id: orphan.session_id.parse::<SessionId>().ok(),
            session_path: PathBuf::from(orphan.session_path),
            outcome: if apply {
                SessionCleanupOutcome::IndexPurged
            } else {
                SessionCleanupOutcome::DryRun
            },
            reason: if apply {
                format!("orphan sqlite rows purged for session {raw_session_id}")
            } else {
                format!("orphan sqlite rows eligible for purge for session {raw_session_id}")
            },
            last_activity_at: None,
            sqlite_purged: apply,
        });
    }
}

fn cleanup_abort_requested(config: &SessionCleanupConfig) -> bool {
    config.abort_check.as_ref().is_some_and(|check| check())
}

enum CandidateDecision {
    Eligible(Box<CleanupCandidate>),
    Skipped(SessionCleanupEntry),
}

async fn last_message_created_at(path: &Path) -> Result<Option<DateTime<Utc>>> {
    let raw = fs::read_to_string(path)
        .await
        .with_context(|| format!("读取 messages.jsonl: {}", path.display()))?;
    let mut last = None;
    for (line_no, line) in raw.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let message: SessionMessage = serde_json::from_str(line).with_context(|| {
            format!(
                "解析 messages.jsonl 第 {} 行: {}",
                line_no + 1,
                path.display()
            )
        })?;
        last = Some(message.created_at);
    }
    Ok(last)
}

async fn last_activity_at(
    messages_path: &Path,
    session_path: &Path,
) -> Result<(DateTime<Utc>, String), String> {
    match last_message_created_at(messages_path).await {
        Ok(Some(created_at)) => Ok((created_at, "last canonical message".to_string())),
        Ok(None) => {
            match directory_modified_at(session_path).await {
                Ok(modified_at) => Ok((
                    modified_at,
                    "empty transcript fallback to directory mtime".to_string(),
                )),
                Err(e) => Err(format!(
                    "last activity unavailable: empty transcript and directory mtime unreadable: {e}"
                )),
            }
        }
        Err(e) => {
            match directory_modified_at(session_path).await {
                Ok(modified_at) => Ok((
                    modified_at,
                    format!("messages unreadable fallback to directory mtime: {e}"),
                )),
                Err(mtime_error) => Err(format!(
                    "last activity unavailable: messages unreadable ({e}) and directory mtime unreadable ({mtime_error})"
                )),
            }
        }
    }
}

async fn directory_modified_at(path: &Path) -> Result<DateTime<Utc>> {
    let metadata = fs::metadata(path)
        .await
        .with_context(|| format!("读取 session 目录 metadata: {}", path.display()))?;
    let modified = metadata
        .modified()
        .with_context(|| format!("读取 session 目录 mtime: {}", path.display()))?;
    Ok(DateTime::<Utc>::from(modified))
}

fn skipped(
    session_id: Option<SessionId>,
    session_path: impl AsRef<Path>,
    reason: impl Into<String>,
) -> SessionCleanupEntry {
    SessionCleanupEntry {
        session_id,
        session_path: session_path.as_ref().to_path_buf(),
        outcome: SessionCleanupOutcome::Skipped,
        reason: reason.into(),
        last_activity_at: None,
        sqlite_purged: false,
    }
}

#[cfg(test)]
mod tests {
    use std::fs::FileTimes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration as StdDuration, SystemTime};

    use super::*;
    use crate::session::{NewSessionMessage, SessionMessageRole, SessionStore};
    use crate::storage::{write_text_atomic, write_yaml_atomic};

    fn agent_id(value: &str) -> AgentId {
        AgentId::new(value).unwrap()
    }

    fn session_id(value: &str) -> SessionId {
        value.parse().unwrap()
    }

    async fn old_closed_session(
        store: &SessionStore,
        agent: &AgentId,
        id: &str,
    ) -> super::super::SessionHandle {
        let old = Utc::now() - chrono::Duration::days(45);
        let mut session = store
            .create_with_id_factory(agent, "system", || session_id(id), 1)
            .await
            .unwrap();
        session
            .append_messages(&[NewSessionMessage::with_created_at(
                SessionMessageRole::User,
                vec![super::super::SessionContentBlock::text("old user")],
                old,
            )])
            .await
            .unwrap();
        session.mark_closed(old).await.unwrap();
        session
    }

    fn config(agent: AgentId, agent_home: PathBuf, apply: bool) -> SessionCleanupConfig {
        SessionCleanupConfig {
            agent_id: agent,
            agent_home,
            cutoff: Utc::now() - chrono::Duration::days(30),
            apply,
            sqlite_busy_timeout: Duration::from_millis(500),
            abort_check: None,
        }
    }

    #[tokio::test]
    async fn cleanup_deletes_old_closed_session() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let store = SessionStore::new(dir.path().to_path_buf());
        let session = old_closed_session(&store, &agent, "session_11111111").await;
        let agent_home = dir.path().join(agent.as_str());

        let report = cleanup_old_sessions(config(agent, agent_home, true))
            .await
            .unwrap();

        assert_eq!(report.scanned, 1);
        assert_eq!(report.eligible, 1);
        assert_eq!(report.deleted, 1);
        assert!(!session.paths.dir.exists());
    }

    #[tokio::test]
    async fn cleanup_skips_open_and_finalizing_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let store = SessionStore::new(dir.path().to_path_buf());
        let open = old_closed_session(&store, &agent, "session_22222222").await;
        let mut finalizing = store
            .create_with_id_factory(&agent, "system", || session_id("session_33333333"), 1)
            .await
            .unwrap();
        finalizing
            .append_messages(&[NewSessionMessage::with_created_at(
                super::super::SessionMessageRole::User,
                vec![super::super::SessionContentBlock::text("finalizing user")],
                Utc::now() - chrono::Duration::days(45),
            )])
            .await
            .unwrap();
        let now = Utc::now();
        let mut reopened = open.clone();
        reopened.mark_open(now).await.unwrap();
        finalizing.mark_finalizing(now).await.unwrap();
        let agent_home = dir.path().join(agent.as_str());

        let report = cleanup_old_sessions(config(agent, agent_home, true))
            .await
            .unwrap();

        assert_eq!(report.scanned, 2);
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped, 2);
        assert!(open.paths.dir.exists());
        assert!(finalizing.paths.dir.exists());
    }

    #[tokio::test]
    async fn cleanup_skips_unreadable_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let agent_home = dir.path().join(agent.as_str());
        let session_dir =
            paths::agent_home_session_dir(&agent_home, &session_id("session_44444444"));
        fs::create_dir_all(&session_dir).await.unwrap();
        write_text_atomic(&session_dir.join("session.yaml"), b"not: [valid")
            .await
            .unwrap();

        let report = cleanup_old_sessions(config(agent, agent_home, true))
            .await
            .unwrap();

        assert_eq!(report.scanned, 1);
        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped, 1);
        assert!(session_dir.exists());
        assert!(report.entries[0].reason.contains("metadata unreadable"));
    }

    #[tokio::test]
    async fn cleanup_falls_back_to_directory_mtime_when_messages_are_bad() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let store = SessionStore::new(dir.path().to_path_buf());
        let session = old_closed_session(&store, &agent, "session_55555555").await;
        write_text_atomic(&session.paths.messages_jsonl, b"{bad json}\n")
            .await
            .unwrap();
        let old_system_time = SystemTime::now() - StdDuration::from_secs(45 * 24 * 60 * 60);
        let times = FileTimes::new().set_modified(old_system_time);
        std::fs::File::open(&session.paths.dir)
            .unwrap()
            .set_times(times)
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());

        let report = cleanup_old_sessions(config(agent, agent_home, true))
            .await
            .unwrap();

        assert_eq!(report.deleted, 1);
        assert!(report.entries[0]
            .reason
            .contains("messages unreadable fallback"));
        assert!(!session.paths.dir.exists());
    }

    #[tokio::test]
    async fn last_activity_errors_when_messages_and_directory_mtime_are_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let missing_session_dir = dir.path().join("missing_session");
        let missing_messages = missing_session_dir.join("messages.jsonl");

        let err = last_activity_at(&missing_messages, &missing_session_dir)
            .await
            .unwrap_err();

        assert!(err.contains("last activity unavailable"));
        assert!(err.contains("messages unreadable"));
        assert!(err.contains("directory mtime unreadable"));
    }

    #[tokio::test]
    async fn cleanup_uses_last_canonical_message_when_metadata_is_recent() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = old_closed_session(&store, &agent, "session_77777777").await;
        let now = Utc::now();
        session.mark_closed(now).await.unwrap();
        let agent_home = dir.path().join(agent.as_str());

        let report = cleanup_old_sessions(config(agent, agent_home, true))
            .await
            .unwrap();

        assert_eq!(report.deleted, 1);
        assert!(!session.paths.dir.exists());
        assert!(report.entries[0].reason.contains("last canonical message"));
    }

    #[tokio::test]
    async fn cleanup_skips_metadata_id_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let store = SessionStore::new(dir.path().to_path_buf());
        let session = old_closed_session(&store, &agent, "session_77777778").await;
        let mut metadata = session.metadata.clone();
        metadata.id = session_id("session_77777779");
        write_yaml_atomic(&session.paths.session_yaml, &metadata)
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());

        let report = cleanup_old_sessions(config(agent, agent_home, true))
            .await
            .unwrap();

        assert_eq!(report.deleted, 0);
        assert_eq!(report.skipped, 1);
        assert!(session.paths.dir.exists());
        assert!(report.entries[0].reason.contains("metadata id mismatch"));
    }

    #[tokio::test]
    async fn delete_candidate_rechecks_metadata_before_removing() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = old_closed_session(&store, &agent, "session_66666666").await;
        let candidate = CleanupCandidate {
            session_id: session.metadata.id.clone(),
            paths: session.paths.clone(),
            last_activity_at: Utc::now() - chrono::Duration::days(45),
            reason: "test".to_string(),
        };
        session.mark_open(Utc::now()).await.unwrap();
        let agent_home = dir.path().join(agent.as_str());

        let entry = delete_candidate(&config(agent, agent_home, true), candidate).await;

        assert_eq!(entry.outcome, SessionCleanupOutcome::Skipped);
        assert!(session.paths.dir.exists());
        assert!(entry.reason.contains("pre-delete status is Open"));
    }

    #[tokio::test]
    async fn cleanup_aborts_after_pre_delete_recheck_before_removing() {
        let dir = tempfile::tempdir().unwrap();
        let agent = agent_id("agent-a");
        let store = SessionStore::new(dir.path().to_path_buf());
        let session = old_closed_session(&store, &agent, "session_66666667").await;
        let agent_home = dir.path().join(agent.as_str());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_check = Arc::clone(&calls);
        let mut config = config(agent, agent_home, true);
        config.abort_check = Some(Arc::new(move || {
            calls_for_check.fetch_add(1, Ordering::SeqCst) >= 2
        }));

        let report = cleanup_old_sessions(config).await.unwrap();

        assert!(report.aborted);
        assert_eq!(report.deleted, 0);
        assert!(session.paths.dir.exists());
        assert!(report
            .entries
            .iter()
            .any(|entry| entry.outcome == SessionCleanupOutcome::Aborted));
    }
}
