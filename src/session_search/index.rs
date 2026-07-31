use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use rand::Rng;
use tokio::task;

use super::search_query::select_matching_messages;
use super::sqlite::{is_busy_error, Connection, SqlValue};
use super::types::{
    BrowseSession, IndexedSessionCandidate, RepairReport, SessionDiskData, SessionReadView,
    SessionScrollView, SessionSearchSort,
};
use super::view::{load_browse_session, load_candidate, load_read_view, load_scroll_view};
use super::{
    disk::{list_session_dirs, read_session_disk_data, read_session_metadata, session_id_from_dir},
    render::searchable_texts_for_messages,
};
use crate::claim::{AgentId, SessionId};
use crate::storage::paths;

const SQLITE_WRITE_RETRY_COUNT: usize = 4;
const SQLITE_WRITE_RETRY_MIN_MS: u64 = 20;
const SQLITE_WRITE_RETRY_MAX_MS: u64 = 150;
const INDEX_VERSION: &str = "session_search_v3";

const INDEX_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS sessions(
  session_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  created_at TEXT,
  updated_at TEXT,
  source TEXT,
  model TEXT,
  session_path TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages(
  session_id TEXT NOT NULL,
  message_index INTEGER NOT NULL,
  role TEXT NOT NULL,
  model TEXT NOT NULL,
  content_text TEXT NOT NULL,
  created_at TEXT,
  PRIMARY KEY(session_id, message_index)
);

CREATE TABLE IF NOT EXISTS indexed_sessions(
  session_id TEXT PRIMARY KEY,
  message_count INTEGER NOT NULL,
  updated_at TEXT NOT NULL,
  session_path TEXT NOT NULL,
  index_version TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  session_id UNINDEXED,
  message_index UNINDEXED,
  role UNINDEXED,
  content_text
);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts_trigram USING fts5(
  session_id UNINDEXED,
  message_index UNINDEXED,
  role UNINDEXED,
  content_text,
  tokenize='trigram'
);
"#;

pub async fn best_effort_index_session_from_files(
    agent_home: PathBuf,
    session_id: SessionId,
    busy_timeout: Duration,
    preferred_start_index: usize,
) {
    let result = task::spawn_blocking(move || {
        let session_dir = paths::agent_home_session_dir(&agent_home, &session_id);
        let data = read_session_disk_data(&session_dir)?;
        let mut conn = open_index(&agent_home, busy_timeout)?;
        match indexed_message_count(&conn, &data.metadata.id)? {
            None => upsert_session(&mut conn, &data, 0)?,
            Some(count) if count > data.messages.len() => {
                rebuild_session(&mut conn, &data)?;
            }
            Some(count) if count < data.messages.len() => {
                let start_index = count.min(preferred_start_index);
                upsert_session(&mut conn, &data, start_index)?;
            }
            Some(count) => {
                if !session_index_complete(&conn, &data.metadata.id, count)? {
                    rebuild_session(&mut conn, &data)?;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => log::warn!(target: "session_search", "session_search 增量索引跳过: {e}"),
        Err(e) => log::warn!(target: "session_search", "session_search 增量索引任务失败: {e}"),
    }
}

pub async fn purge_session_from_index(
    agent_home: PathBuf,
    session_id: SessionId,
    busy_timeout: Duration,
) -> Result<bool> {
    task::spawn_blocking(move || {
        let db_path = paths::agent_home_session_search_index_path(&agent_home);
        if !db_path.try_exists()? {
            return Ok(false);
        }
        let mut conn = open_index(&agent_home, busy_timeout)?;
        run_immediate_transaction(&mut conn, |conn| {
            delete_session_index_rows(conn, &session_id)
        })
    })
    .await?
}

pub async fn purge_orphaned_sessions_from_index(
    agent_home: PathBuf,
    busy_timeout: Duration,
) -> Result<Vec<OrphanedIndexPurge>> {
    task::spawn_blocking(move || {
        let db_path = paths::agent_home_session_search_index_path(&agent_home);
        if !db_path.try_exists()? {
            return Ok(Vec::new());
        }
        let mut conn = open_index(&agent_home, busy_timeout)?;
        run_immediate_transaction(&mut conn, purge_orphaned_session_index_rows)
    })
    .await?
}

pub async fn list_orphaned_sessions_in_index(
    agent_home: PathBuf,
    busy_timeout: Duration,
) -> Result<Vec<OrphanedIndexPurge>> {
    task::spawn_blocking(move || {
        let db_path = paths::agent_home_session_search_index_path(&agent_home);
        if !db_path.try_exists()? {
            return Ok(Vec::new());
        }
        let conn = open_index_read_only(&agent_home, busy_timeout)?;
        orphaned_session_index_rows(&conn)
    })
    .await?
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanedIndexPurge {
    pub session_id: String,
    pub session_path: String,
}

pub(crate) async fn repair_index_for_agent(
    agent_home: PathBuf,
    agent_id: AgentId,
    current_session_id: Option<SessionId>,
    busy_timeout: Duration,
) -> Result<RepairReport> {
    task::spawn_blocking(move || {
        let mut report = RepairReport::default();
        let mut conn = open_index(&agent_home, busy_timeout)?;
        run_immediate_transaction(&mut conn, purge_orphaned_session_index_rows)?;
        let sessions = list_session_dirs(&agent_home)?;
        for session_dir in sessions {
            let session_id = match session_id_from_dir(&session_dir) {
                Ok(session_id) => session_id,
                Err(e) => {
                    report.index_incomplete = true;
                    report.warnings.push(format!(
                        "repair skipped invalid session dir {}: {e}",
                        session_dir.display()
                    ));
                    continue;
                }
            };
            if current_session_id
                .as_ref()
                .is_some_and(|current| current == &session_id)
            {
                continue;
            }
            let metadata = match read_session_metadata(&session_dir) {
                Ok(metadata) => metadata,
                Err(e) => {
                    report.index_incomplete = true;
                    report
                        .warnings
                        .push(format!("repair skipped {}: {e}", session_dir.display()));
                    continue;
                }
            };
            if metadata.agent_id != agent_id {
                continue;
            }
            let indexed_count = match indexed_message_count_i64(&conn, &metadata.id) {
                Ok(Some(count)) if count < 0 => {
                    report.index_incomplete = true;
                    report.warnings.push(format!(
                        "session {} indexed_sessions.message_count={} is invalid, rebuilding",
                        metadata.id, count
                    ));
                    None
                }
                Ok(count) => count
                    .map(usize::try_from)
                    .transpose()
                    .context("negative SQLite count")?,
                Err(e) => {
                    report.index_incomplete = true;
                    report
                        .warnings
                        .push(format!("repair skipped session {}: {e}", metadata.id));
                    continue;
                }
            };
            let data = match read_session_disk_data(&session_dir) {
                Ok(data) => data,
                Err(e) => {
                    report.index_incomplete = true;
                    report
                        .warnings
                        .push(format!("repair skipped {}: {e}", session_dir.display()));
                    continue;
                }
            };
            if data.metadata.message_count != data.messages.len() {
                report.index_incomplete = true;
                report.warnings.push(format!(
                    "session {} metadata.message_count={} but messages.jsonl has {}",
                    data.metadata.id,
                    data.metadata.message_count,
                    data.messages.len()
                ));
            }
            if indexed_count == Some(data.messages.len())
                && session_index_complete(&conn, &metadata.id, data.messages.len())?
            {
                continue;
            }
            if let Err(e) = repair_one_session(&mut conn, &data, indexed_count) {
                report.index_incomplete = true;
                report
                    .warnings
                    .push(format!("repair skipped session {}: {e}", data.metadata.id));
            }
        }
        Ok(report)
    })
    .await?
}

pub(crate) async fn search_index(
    agent_home: PathBuf,
    query: String,
    limit: usize,
    exclude_session_id: Option<SessionId>,
    sort: SessionSearchSort,
    include_tool_results: bool,
    busy_timeout: Duration,
) -> Result<Vec<IndexedSessionCandidate>> {
    task::spawn_blocking(move || {
        let conn = open_index_read_only(&agent_home, busy_timeout)?;
        let matches = select_matching_messages(
            &conn,
            &query,
            limit,
            exclude_session_id.as_ref(),
            sort,
            include_tool_results,
        )
        .map_err(|e| {
            if sqlite_error_is_invalid_fts_query(&e) {
                anyhow::anyhow!("invalid session_search query: {e}")
            } else {
                e
            }
        })?;
        let mut out = Vec::new();
        for hit in matches {
            if exclude_session_id
                .as_ref()
                .is_some_and(|exclude| exclude == &hit.session_id)
            {
                continue;
            }
            if out
                .iter()
                .any(|candidate: &IndexedSessionCandidate| candidate.session_id == hit.session_id)
            {
                continue;
            }
            if let Some(candidate) = load_candidate(&conn, hit, include_tool_results)? {
                out.push(candidate);
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    })
    .await?
}

pub(crate) async fn browse_index(
    agent_home: PathBuf,
    limit: usize,
    exclude_session_id: Option<SessionId>,
    busy_timeout: Duration,
) -> Result<Vec<BrowseSession>> {
    task::spawn_blocking(move || {
        let conn = open_index_read_only(&agent_home, busy_timeout)?;
        let fetch_limit = limit.saturating_add(5);
        let fetch_limit_i64 = usize_to_i64(fetch_limit, "session_search browse limit")?;
        let rows = conn.query_strings(
            "SELECT session_id FROM sessions
             ORDER BY updated_at DESC, created_at DESC
             LIMIT ?1;",
            &[SqlValue::Integer(fetch_limit_i64)],
        )?;
        let mut out = Vec::new();
        for raw in rows {
            let session_id = SessionId::from_str(&raw)?;
            if exclude_session_id
                .as_ref()
                .is_some_and(|exclude| exclude == &session_id)
            {
                continue;
            }
            if let Some(session) = load_browse_session(&conn, session_id)? {
                out.push(session);
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    })
    .await?
}

pub(crate) async fn read_session(
    agent_home: PathBuf,
    session_id: SessionId,
    include_tool_results: bool,
    busy_timeout: Duration,
) -> Result<Option<SessionReadView>> {
    task::spawn_blocking(move || {
        let conn = open_index_read_only(&agent_home, busy_timeout)?;
        load_read_view(&conn, session_id, include_tool_results)
    })
    .await?
}

pub(crate) async fn scroll_session(
    agent_home: PathBuf,
    session_id: SessionId,
    around_message_index: usize,
    window: usize,
    include_tool_results: bool,
    busy_timeout: Duration,
) -> Result<Option<SessionScrollView>> {
    task::spawn_blocking(move || {
        let conn = open_index_read_only(&agent_home, busy_timeout)?;
        load_scroll_view(
            &conn,
            session_id,
            around_message_index,
            window,
            include_tool_results,
        )
    })
    .await?
}

pub(crate) fn is_invalid_query_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .starts_with("invalid session_search query:")
}

fn repair_one_session(
    conn: &mut Connection,
    data: &SessionDiskData,
    indexed_count: Option<usize>,
) -> Result<()> {
    match indexed_count {
        None => upsert_session(conn, data, 0)?,
        Some(count) if count < data.messages.len() => upsert_session(conn, data, count)?,
        Some(count) if count > data.messages.len() => rebuild_session(conn, data)?,
        Some(count)
            if count == data.messages.len()
                && !session_index_complete(conn, &data.metadata.id, count)? =>
        {
            rebuild_session(conn, data)?;
        }
        Some(_) => {}
    }
    Ok(())
}

fn open_index(agent_home: &Path, busy_timeout: Duration) -> Result<Connection> {
    std::fs::create_dir_all(agent_home)?;
    let db_path = paths::agent_home_session_search_index_path(agent_home);
    let conn = Connection::open(&db_path)
        .with_context(|| format!("打开 session_search SQLite: {}", db_path.display()))?;
    conn.busy_timeout(busy_timeout)?;
    conn.enable_wal_with_delete_fallback()?;
    conn.execute_batch(INDEX_SCHEMA)
        .context("初始化 session_search SQLite schema")?;
    ensure_schema_migrations(&conn)?;
    Ok(conn)
}

fn ensure_schema_migrations(conn: &Connection) -> Result<()> {
    match conn.execute(
        "ALTER TABLE indexed_sessions ADD COLUMN index_version TEXT;",
        &[],
    ) {
        Ok(()) => Ok(()),
        Err(e)
            if e.chain()
                .any(|cause| cause.to_string().contains("duplicate column name")) =>
        {
            Ok(())
        }
        Err(e) => Err(e).context("迁移 indexed_sessions.index_version"),
    }?;
    match conn.execute(
        "ALTER TABLE messages ADD COLUMN model TEXT NOT NULL DEFAULT 'unknown';",
        &[],
    ) {
        Ok(()) => Ok(()),
        Err(e)
            if e.chain()
                .any(|cause| cause.to_string().contains("duplicate column name")) =>
        {
            Ok(())
        }
        Err(e) => Err(e).context("迁移 messages.model"),
    }
}

fn open_index_read_only(agent_home: &Path, busy_timeout: Duration) -> Result<Connection> {
    let db_path = paths::agent_home_session_search_index_path(agent_home);
    let conn = Connection::open_read_only(&db_path)
        .with_context(|| format!("只读打开 session_search SQLite: {}", db_path.display()))?;
    conn.busy_timeout(busy_timeout)?;
    Ok(conn)
}

fn indexed_message_count(conn: &Connection, session_id: &SessionId) -> Result<Option<usize>> {
    indexed_message_count_i64(conn, session_id)?
        .map(i64_to_usize)
        .transpose()
}

fn indexed_message_count_i64(conn: &Connection, session_id: &SessionId) -> Result<Option<i64>> {
    conn.query_one_i64(
        "SELECT message_count FROM indexed_sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id.as_str())],
    )
    .context("读取 indexed_sessions.message_count")
}

fn indexed_session_version(conn: &Connection, session_id: &SessionId) -> Result<Option<String>> {
    conn.query_one_string(
        "SELECT index_version FROM indexed_sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id.as_str())],
    )
    .context("读取 indexed_sessions.index_version")
}

fn session_index_complete(
    conn: &Connection,
    session_id: &SessionId,
    expected_message_count: usize,
) -> Result<bool> {
    let expected = usize_to_i64(expected_message_count, "session message_count")?;
    let messages = session_table_row_count(conn, "messages", session_id)?;
    let fts = session_table_row_count(conn, "messages_fts", session_id)?;
    let trigram = session_table_row_count(conn, "messages_fts_trigram", session_id)?;
    let version = indexed_session_version(conn, session_id)?;
    Ok(messages == expected
        && fts == expected
        && trigram == expected
        && version.as_deref() == Some(INDEX_VERSION))
}

fn session_table_row_count(
    conn: &Connection,
    table: &'static str,
    session_id: &SessionId,
) -> Result<i64> {
    conn.query_one_i64(
        &format!("SELECT count(*) FROM {table} WHERE session_id = ?1;"),
        &[SqlValue::Text(session_id.as_str())],
    )
    .with_context(|| format!("读取 {table}.row_count"))?
    .context("SQLite count(*) unexpectedly returned no row")
}

fn rebuild_session(conn: &mut Connection, data: &SessionDiskData) -> Result<()> {
    run_immediate_transaction(conn, |conn| {
        delete_session_index_rows(conn, &data.metadata.id)?;
        upsert_session_rows(conn, data, 0)
    })
}

fn upsert_session(conn: &mut Connection, data: &SessionDiskData, start_index: usize) -> Result<()> {
    run_immediate_transaction(conn, |conn| upsert_session_rows(conn, data, start_index))
}

fn upsert_session_rows(
    conn: &Connection,
    data: &SessionDiskData,
    start_index: usize,
) -> Result<()> {
    let session_id = data.metadata.id.as_str();
    let start_index_i64 = usize_to_i64(start_index, "session message start_index")?;
    let message_count_i64 = usize_to_i64(data.messages.len(), "session message_count")?;
    let session_path = data.session_path.to_string_lossy().to_string();
    let created_at = data.metadata.created_at.to_rfc3339();
    let updated_at = data.metadata.updated_at.to_rfc3339();
    conn.execute(
        "INSERT INTO sessions(session_id, agent_id, created_at, updated_at, source, model, session_path)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(session_id) DO UPDATE SET
               agent_id = excluded.agent_id,
               created_at = excluded.created_at,
               updated_at = excluded.updated_at,
               source = excluded.source,
               model = excluded.model,
               session_path = excluded.session_path;",
        &[
            SqlValue::Text(session_id),
            SqlValue::Text(data.metadata.agent_id.as_str()),
            SqlValue::Text(&created_at),
            SqlValue::Text(&updated_at),
            SqlValue::Text(data.metadata.source.as_str()),
            SqlValue::Text(data.metadata.model.as_str()),
            SqlValue::Text(session_path.as_str()),
        ],
    )?;
    conn.execute(
        "DELETE FROM messages WHERE session_id = ?1 AND message_index >= ?2;",
        &[
            SqlValue::Text(session_id),
            SqlValue::Integer(start_index_i64),
        ],
    )?;
    conn.execute(
        "DELETE FROM messages_fts WHERE session_id = ?1 AND message_index >= ?2;",
        &[
            SqlValue::Text(session_id),
            SqlValue::Integer(start_index_i64),
        ],
    )?;
    conn.execute(
        "DELETE FROM messages_fts_trigram WHERE session_id = ?1 AND message_index >= ?2;",
        &[
            SqlValue::Text(session_id),
            SqlValue::Integer(start_index_i64),
        ],
    )?;
    let searchable_texts = searchable_texts_for_messages(&data.messages);
    for (message, content_text) in data
        .messages
        .iter()
        .zip(searchable_texts.iter())
        .filter(|(message, _)| message.index >= start_index)
    {
        let message_index = usize_to_i64(message.index, "session message_index")?;
        let role = message.role.to_string();
        let model = message.model.as_str();
        let created_at = message.created_at.to_rfc3339();
        conn.execute(
            "INSERT INTO messages(session_id, message_index, role, model, content_text, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(session_id, message_index) DO UPDATE SET
                   role = excluded.role,
                   model = excluded.model,
                   content_text = excluded.content_text,
                   created_at = excluded.created_at;",
            &[
                SqlValue::Text(session_id),
                SqlValue::Integer(message_index),
                SqlValue::Text(&role),
                SqlValue::Text(model),
                SqlValue::Text(content_text),
                SqlValue::Text(&created_at),
            ],
        )?;
        conn.execute(
            "INSERT INTO messages_fts(session_id, message_index, role, content_text)
                 VALUES(?1, ?2, ?3, ?4);",
            &[
                SqlValue::Text(session_id),
                SqlValue::Integer(message_index),
                SqlValue::Text(&role),
                SqlValue::Text(content_text),
            ],
        )?;
        conn.execute(
            "INSERT INTO messages_fts_trigram(session_id, message_index, role, content_text)
                 VALUES(?1, ?2, ?3, ?4);",
            &[
                SqlValue::Text(session_id),
                SqlValue::Integer(message_index),
                SqlValue::Text(&role),
                SqlValue::Text(content_text),
            ],
        )?;
    }
    let indexed_at = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO indexed_sessions(session_id, message_count, updated_at, session_path, index_version)
             VALUES(?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(session_id) DO UPDATE SET
               message_count = excluded.message_count,
               updated_at = excluded.updated_at,
               session_path = excluded.session_path,
               index_version = excluded.index_version;",
        &[
            SqlValue::Text(session_id),
            SqlValue::Integer(message_count_i64),
            SqlValue::Text(&indexed_at),
            SqlValue::Text(session_path.as_str()),
            SqlValue::Text(INDEX_VERSION),
        ],
    )?;
    Ok(())
}

fn delete_session_index_rows(conn: &Connection, session_id: &SessionId) -> Result<bool> {
    delete_session_index_rows_by_id(conn, session_id.as_str())
}

fn delete_session_index_rows_by_id(conn: &Connection, session_id: &str) -> Result<bool> {
    let existed = session_index_rows_exist(conn, session_id)?;
    conn.execute(
        "DELETE FROM messages WHERE session_id = ?1;",
        &[SqlValue::Text(session_id)],
    )?;
    conn.execute(
        "DELETE FROM messages_fts WHERE session_id = ?1;",
        &[SqlValue::Text(session_id)],
    )?;
    conn.execute(
        "DELETE FROM messages_fts_trigram WHERE session_id = ?1;",
        &[SqlValue::Text(session_id)],
    )?;
    conn.execute(
        "DELETE FROM indexed_sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id)],
    )?;
    conn.execute(
        "DELETE FROM sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id)],
    )?;
    Ok(existed)
}

fn session_index_rows_exist(conn: &Connection, session_id: &str) -> Result<bool> {
    for table in [
        "sessions",
        "messages",
        "messages_fts",
        "messages_fts_trigram",
        "indexed_sessions",
    ] {
        let count = conn
            .query_one_i64(
                &format!("SELECT count(*) FROM {table} WHERE session_id = ?1;"),
                &[SqlValue::Text(session_id)],
            )?
            .unwrap_or(0);
        if count > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn purge_orphaned_session_index_rows(conn: &Connection) -> Result<Vec<OrphanedIndexPurge>> {
    let orphaned = orphaned_session_index_rows(conn)?;
    let mut purged = Vec::new();
    for orphan in orphaned {
        if delete_session_index_rows_by_id(conn, &orphan.session_id)? {
            purged.push(orphan);
        }
    }
    Ok(purged)
}

fn orphaned_session_index_rows(conn: &Connection) -> Result<Vec<OrphanedIndexPurge>> {
    let rows = conn.query_string_quads(
        "SELECT session_id, session_path, '', '' FROM sessions;",
        &[],
    )?;
    let mut orphaned = Vec::new();
    for (session_id, session_path, _, _) in rows {
        match std::fs::metadata(PathBuf::from(&session_path)) {
            Ok(metadata) if metadata.is_dir() => continue,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                log::warn!(
                    target: "session_search",
                    "session_search orphan purge skipped path with metadata error ({}): {e}",
                    session_path
                );
                continue;
            }
        }
        orphaned.push(OrphanedIndexPurge {
            session_id,
            session_path,
        });
    }
    Ok(orphaned)
}

fn sqlite_error_is_invalid_fts_query(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("fts5: syntax error")
        || message.contains("unterminated string")
        || message.contains("malformed MATCH expression")
        || message.contains("no such column:")
}

fn run_immediate_transaction<T>(
    conn: &mut Connection,
    mut operation: impl FnMut(&Connection) -> Result<T>,
) -> Result<T> {
    let mut rng = rand::thread_rng();
    for attempt in 0..=SQLITE_WRITE_RETRY_COUNT {
        match conn.execute_batch("BEGIN IMMEDIATE;") {
            Ok(()) => {
                let output = match operation(conn) {
                    Ok(output) => output,
                    Err(e) => {
                        let _ = conn.execute_batch("ROLLBACK;");
                        return Err(e);
                    }
                };
                return match conn.execute_batch("COMMIT;") {
                    Ok(()) => Ok(output),
                    Err(e) => {
                        let _ = conn.execute_batch("ROLLBACK;");
                        Err(e).context("commit session_search SQLite transaction")
                    }
                };
            }
            Err(e) if is_busy_error(&e) && attempt < SQLITE_WRITE_RETRY_COUNT => {
                let sleep_ms = rng.gen_range(SQLITE_WRITE_RETRY_MIN_MS..=SQLITE_WRITE_RETRY_MAX_MS);
                thread::sleep(Duration::from_millis(sleep_ms));
            }
            Err(e) => {
                return Err(e).context("begin immediate session_search SQLite transaction");
            }
        }
    }
    anyhow::bail!("session_search SQLite write retry exhausted")
}

fn i64_to_usize(value: i64) -> Result<usize> {
    usize::try_from(value).context("negative SQLite count")
}

fn usize_to_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} exceeds i64 range"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{
        NewSessionMessage, SessionContentBlock, SessionMessageRole, SessionStore,
    };

    async fn test_search(
        agent_home: PathBuf,
        query: &str,
        limit: usize,
        include_tool_results: bool,
        timeout: Duration,
    ) -> Vec<IndexedSessionCandidate> {
        search_index(
            agent_home,
            query.into(),
            limit,
            None,
            SessionSearchSort::Relevance,
            include_tool_results,
            timeout,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn repair_records_indexed_message_count_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_33333333").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[
                NewSessionMessage::text_with_model(SessionMessageRole::User, "alpha", "model-a"),
                NewSessionMessage::text_with_model(
                    SessionMessageRole::Assistant,
                    "beta",
                    "model-b",
                ),
            ])
            .await
            .unwrap();

        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent.clone(), None, timeout)
            .await
            .unwrap();
        let conn = open_index(&agent_home, timeout).unwrap();
        let updated_at_before = conn
            .query_one_string(
                "SELECT updated_at FROM indexed_sessions WHERE session_id = ?1;",
                &[SqlValue::Text("session_33333333")],
            )
            .unwrap()
            .unwrap();
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let conn = open_index(&agent_home, timeout).unwrap();
        let count = indexed_message_count(&conn, &SessionId::from_str("session_33333333").unwrap())
            .unwrap();
        assert_eq!(count, Some(2));
        let latest_message_model = conn
            .query_one_string(
                "SELECT model FROM messages WHERE session_id = ?1 AND message_index = 1;",
                &[SqlValue::Text("session_33333333")],
            )
            .unwrap()
            .unwrap();
        assert_eq!(latest_message_model, "model-b");
        let updated_at_after = conn
            .query_one_string(
                "SELECT updated_at FROM indexed_sessions WHERE session_id = ?1;",
                &[SqlValue::Text("session_33333333")],
            )
            .unwrap()
            .unwrap();
        assert_eq!(updated_at_after, updated_at_before);
    }

    #[tokio::test]
    async fn repair_lagging_index_adds_new_messages() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_44444444").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "docker baseline",
            )])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent.clone(), None, timeout)
            .await
            .unwrap();

        session
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::Assistant,
                "kubernetes followup",
            )])
            .await
            .unwrap();
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let hits = test_search(agent_home, "kubernetes", 3, false, timeout).await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].session_id.as_str(), "session_44444444");
        assert!(hits[0]
            .messages
            .iter()
            .any(|message| message.content.contains("kubernetes followup")));
    }

    #[tokio::test]
    async fn repair_dirty_index_rebuilds_session() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_55555555").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "dirty index rebuild",
            )])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent.clone(), None, timeout)
            .await
            .unwrap();
        let conn = open_index(&agent_home, timeout).unwrap();
        conn.execute(
            "UPDATE indexed_sessions SET message_count = 99 WHERE session_id = 'session_55555555';",
            &[],
        )
        .unwrap();

        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let conn = open_index(&agent_home, timeout).unwrap();
        let count = indexed_message_count(&conn, &SessionId::from_str("session_55555555").unwrap())
            .unwrap();
        assert_eq!(count, Some(1));
    }

    #[tokio::test]
    async fn purge_session_from_index_removes_tables_and_fts_rows() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_12345678").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[
                NewSessionMessage::text(SessionMessageRole::User, "中文索引 token"),
                NewSessionMessage::text(SessionMessageRole::Assistant, "answer token"),
            ])
            .await
            .unwrap();
        let session_id = SessionId::from_str("session_12345678").unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        purge_session_from_index(agent_home.clone(), session_id.clone(), timeout)
            .await
            .unwrap();

        let conn = open_index(&agent_home, timeout).unwrap();
        assert_eq!(
            session_table_row_count(&conn, "messages", &session_id).unwrap(),
            0
        );
        assert_eq!(
            session_table_row_count(&conn, "messages_fts", &session_id).unwrap(),
            0
        );
        assert_eq!(
            session_table_row_count(&conn, "messages_fts_trigram", &session_id).unwrap(),
            0
        );
        assert_eq!(
            conn.query_one_i64(
                "SELECT count(*) FROM indexed_sessions WHERE session_id = ?1;",
                &[SqlValue::Text(session_id.as_str())],
            )
            .unwrap(),
            Some(0)
        );
        assert_eq!(
            conn.query_one_i64(
                "SELECT count(*) FROM sessions WHERE session_id = ?1;",
                &[SqlValue::Text(session_id.as_str())],
            )
            .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn purge_session_from_index_noops_when_index_db_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let agent_home = dir.path().join("agent-a");
        let session_id = SessionId::from_str("session_12345679").unwrap();
        let timeout = Duration::from_millis(500);

        let purged = purge_session_from_index(agent_home.clone(), session_id, timeout)
            .await
            .unwrap();

        assert!(!purged);
        assert!(!paths::agent_home_session_search_index_path(&agent_home).exists());
    }

    #[tokio::test]
    async fn purge_orphaned_sessions_from_index_removes_missing_session_dir_rows() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_1234abcd").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[
                NewSessionMessage::text(SessionMessageRole::User, "orphan 中文 token"),
                NewSessionMessage::text(SessionMessageRole::Assistant, "orphan answer"),
            ])
            .await
            .unwrap();
        let session_id = SessionId::from_str("session_1234abcd").unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();
        tokio::fs::remove_dir_all(&session.paths.dir).await.unwrap();

        let purged = purge_orphaned_sessions_from_index(agent_home.clone(), timeout)
            .await
            .unwrap();

        assert_eq!(purged.len(), 1);
        assert_eq!(purged[0].session_id, session_id.as_str());
        let conn = open_index(&agent_home, timeout).unwrap();
        assert_eq!(
            session_table_row_count(&conn, "messages", &session_id).unwrap(),
            0
        );
        assert_eq!(
            session_table_row_count(&conn, "messages_fts", &session_id).unwrap(),
            0
        );
        assert_eq!(
            session_table_row_count(&conn, "messages_fts_trigram", &session_id).unwrap(),
            0
        );
        assert_eq!(
            conn.query_one_i64(
                "SELECT count(*) FROM indexed_sessions WHERE session_id = ?1;",
                &[SqlValue::Text(session_id.as_str())],
            )
            .unwrap(),
            Some(0)
        );
        assert_eq!(
            conn.query_one_i64(
                "SELECT count(*) FROM sessions WHERE session_id = ?1;",
                &[SqlValue::Text(session_id.as_str())],
            )
            .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn purge_orphaned_sessions_from_index_removes_non_directory_session_path_rows() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_1234abcf").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "orphan file path token",
            )])
            .await
            .unwrap();
        let session_id = SessionId::from_str("session_1234abcf").unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();
        tokio::fs::remove_dir_all(&session.paths.dir).await.unwrap();
        tokio::fs::write(&session.paths.dir, b"not a directory")
            .await
            .unwrap();

        let purged = purge_orphaned_sessions_from_index(agent_home.clone(), timeout)
            .await
            .unwrap();

        assert_eq!(purged.len(), 1);
        assert_eq!(purged[0].session_id, session_id.as_str());
        let conn = open_index(&agent_home, timeout).unwrap();
        assert_eq!(
            session_table_row_count(&conn, "messages", &session_id).unwrap(),
            0
        );
        tokio::fs::remove_file(&session.paths.dir).await.unwrap();
    }

    #[tokio::test]
    async fn repair_index_purges_orphaned_session_rows() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_1234abce").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "repair orphan token",
            )])
            .await
            .unwrap();
        let session_id = SessionId::from_str("session_1234abce").unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent.clone(), None, timeout)
            .await
            .unwrap();
        tokio::fs::remove_dir_all(&session.paths.dir).await.unwrap();

        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let conn = open_index(&agent_home, timeout).unwrap();
        assert_eq!(
            session_table_row_count(&conn, "messages_fts_trigram", &session_id).unwrap(),
            0
        );
        assert_eq!(
            conn.query_one_i64(
                "SELECT count(*) FROM sessions WHERE session_id = ?1;",
                &[SqlValue::Text(session_id.as_str())],
            )
            .unwrap(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn search_aggregates_by_session_before_limit() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut noisy = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_66666666").unwrap(),
                1,
            )
            .await
            .unwrap();
        let noisy_messages = (0..50)
            .map(|idx| {
                NewSessionMessage::text(
                    SessionMessageRole::User,
                    format!("docker repeated hit {idx}"),
                )
            })
            .collect::<Vec<_>>();
        noisy.append_messages(&noisy_messages).await.unwrap();

        let mut other = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_77777777").unwrap(),
                1,
            )
            .await
            .unwrap();
        other
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "docker second session",
            )])
            .await
            .unwrap();

        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let hits = test_search(agent_home, "docker", 2, false, timeout).await;
        let hit_ids = hits
            .iter()
            .map(|hit| hit.session_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(hit_ids.len(), 2);
        assert!(hit_ids.contains(&"session_66666666"));
        assert!(hit_ids.contains(&"session_77777777"));
    }

    #[tokio::test]
    async fn repair_continues_after_one_session_index_error() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut broken = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_88888888").unwrap(),
                1,
            )
            .await
            .unwrap();
        broken
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "broken docker session",
            )])
            .await
            .unwrap();

        let mut healthy = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_99999999").unwrap(),
                1,
            )
            .await
            .unwrap();
        healthy
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "healthy kubernetes session",
            )])
            .await
            .unwrap();

        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        let conn = open_index(&agent_home, timeout).unwrap();
        conn.execute(
            "INSERT INTO indexed_sessions(session_id, message_count, updated_at, session_path)
             VALUES('session_88888888', -1, '2026-01-01T00:00:00Z', '/tmp/broken');",
            &[],
        )
        .unwrap();

        let report = repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        assert!(report.index_incomplete);
        assert!(report
            .warnings
            .iter()
            .any(|warning| warning.contains("session_88888888")));
        let hits = test_search(agent_home, "kubernetes", 2, false, timeout).await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].session_id.as_str(), "session_99999999");
    }

    #[tokio::test]
    async fn cjk_trigram_matches_long_chinese_substring() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_aaaa1111").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "昨天和其他Agent的聊天记录，记忆断裂问题复现了",
            )])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let hits = test_search(agent_home, "记忆断裂", 3, false, timeout).await;

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].session_id.as_str(), "session_aaaa1111");
        assert!(hits[0].snippet.contains("记忆断裂"));
    }

    #[tokio::test]
    async fn repair_rebuilds_when_trigram_index_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_aabb1111").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "这个旧索引升级后需要补齐三元索引",
            )])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent.clone(), None, timeout)
            .await
            .unwrap();

        {
            let conn = open_index(&agent_home, timeout).unwrap();
            conn.execute(
                "DELETE FROM messages_fts_trigram WHERE session_id = ?1;",
                &[SqlValue::Text("session_aabb1111")],
            )
            .unwrap();
            assert_eq!(
                session_table_row_count(
                    &conn,
                    "messages_fts_trigram",
                    &SessionId::from_str("session_aabb1111").unwrap()
                )
                .unwrap(),
                0
            );
        }

        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();
        let hits = test_search(agent_home, "三元索引", 3, false, timeout).await;

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].session_id.as_str(), "session_aabb1111");
    }

    #[tokio::test]
    async fn cjk_like_matches_short_or_terms() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut first = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_bbbb1111").unwrap(),
                1,
            )
            .await
            .unwrap();
        first
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "广西是个好地方，去过桂林",
            )])
            .await
            .unwrap();
        let mut second = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_bbbb2222").unwrap(),
                1,
            )
            .await
            .unwrap();
        second
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "漓江风景很美，值得旅游",
            )])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let hits = test_search(
            agent_home,
            "广西 OR 桂林 OR 漓江 OR 旅游",
            5,
            false,
            timeout,
        )
        .await;
        let ids = hits
            .iter()
            .map(|hit| hit.session_id.as_str())
            .collect::<Vec<_>>();

        assert!(ids.contains(&"session_bbbb1111"));
        assert!(ids.contains(&"session_bbbb2222"));
    }

    #[tokio::test]
    async fn cjk_like_preserves_not_terms() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut excluded = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_bbbb3333").unwrap(),
                1,
            )
            .await
            .unwrap();
        excluded
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "北京和上海都有相关记录",
            )])
            .await
            .unwrap();
        let mut included = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_bbbb4444").unwrap(),
                1,
            )
            .await
            .unwrap();
        included
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "北京和广州留下了相关记录",
            )])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let hits = test_search(agent_home, "北京 NOT 上海", 5, false, timeout).await;
        let ids = hits
            .iter()
            .map(|hit| hit.session_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["session_bbbb4444"]);
    }

    #[tokio::test]
    async fn cjk_like_handles_short_ascii_terms() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut with_id = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_bbbb5555").unwrap(),
                1,
            )
            .await
            .unwrap();
        with_id
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "服务器 id 排查记录",
            )])
            .await
            .unwrap();
        let mut without_id = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_bbbb6666").unwrap(),
                1,
            )
            .await
            .unwrap();
        without_id
            .append_messages(&[NewSessionMessage::text(
                SessionMessageRole::User,
                "服务器 状态排查记录",
            )])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let hits = test_search(agent_home, "服务器 AND id", 5, false, timeout).await;
        let ids = hits
            .iter()
            .map(|hit| hit.session_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["session_bbbb5555"]);
    }

    #[tokio::test]
    async fn discovery_excludes_tool_result_matches_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let agent = AgentId::new("agent-a").unwrap();
        let store = SessionStore::new(dir.path().to_path_buf());
        let mut session = store
            .create_with_id_factory(
                &agent,
                "system",
                || SessionId::from_str("session_cccc1111").unwrap(),
                1,
            )
            .await
            .unwrap();
        session
            .append_messages(&[
                NewSessionMessage::new(
                    SessionMessageRole::Assistant,
                    vec![SessionContentBlock::tool_use(
                        "toolu_1",
                        "code_run",
                        serde_json::json!({"cmd":"echo secret_tool_output"}),
                    )],
                ),
                NewSessionMessage::new(
                    SessionMessageRole::User,
                    vec![SessionContentBlock::tool_result(
                        "toolu_1",
                        "unique_tool_result_token",
                    )],
                ),
            ])
            .await
            .unwrap();
        let agent_home = dir.path().join(agent.as_str());
        let timeout = Duration::from_millis(500);
        repair_index_for_agent(agent_home.clone(), agent, None, timeout)
            .await
            .unwrap();

        let hidden = test_search(
            agent_home.clone(),
            "unique_tool_result_token",
            3,
            false,
            timeout,
        )
        .await;
        let visible = test_search(agent_home, "unique_tool_result_token", 3, true, timeout).await;

        assert!(hidden.is_empty());
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].match_message_index, 1);
        assert!(visible[0].messages.iter().any(|message| {
            message.content.contains("unique_tool_result_token") && !message.truncated
        }));
    }
}
