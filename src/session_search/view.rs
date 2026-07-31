//! session_search 原文 evidence 视图构造。
//!
//! 本模块从 SQLite session 索引定位 session 目录，再回读 JSONL 权威 transcript，
//! 构造 discovery/bookend、scroll、read 和 browse 需要的轻量原文窗口。

use std::path::Path;

use anyhow::{Context, Result};

use super::disk::read_session_disk_data;
use super::render::{
    evidence_message_for_session_message, first_user_preview, message_has_tool_results,
    tool_name_map,
};
use super::search_query::SearchHit;
use super::sqlite::{Connection, SqlValue};
use super::types::{
    BrowseSession, IndexedSessionCandidate, SessionDiskData, SessionReadView, SessionScrollView,
    SessionSearchMessage, SessionSearchSessionMeta,
};
use crate::claim::SessionId;

const DISCOVERY_WINDOW: usize = 5;
const DISCOVERY_BOOKEND: usize = 3;
const READ_HEAD_MESSAGES: usize = 20;
const READ_TAIL_MESSAGES: usize = 10;
const BROWSE_PREVIEW_CHARS: usize = 240;

pub(crate) fn load_candidate(
    conn: &Connection,
    hit: SearchHit,
    include_tool_results: bool,
) -> Result<Option<IndexedSessionCandidate>> {
    let meta = conn
        .query_three_optional_strings(
            "SELECT created_at, source, model FROM sessions WHERE session_id = ?1;",
            &[SqlValue::Text(hit.session_id.as_str())],
        )
        .context("读取 session_search candidate metadata")?;
    let Some((created_at, source, model)) = meta else {
        return Ok(None);
    };

    let Some(data) = read_session_data_for_id(conn, &hit.session_id)? else {
        return Ok(None);
    };
    let view = anchored_view(&data.messages, hit.message_index, include_tool_results);
    Ok(Some(IndexedSessionCandidate {
        session_id: hit.session_id,
        when: created_at.unwrap_or_default(),
        source: source.unwrap_or_else(|| "tui".into()),
        model: model.unwrap_or_else(|| "unknown".into()),
        message_count: data.messages.len(),
        matched_role: hit.role,
        match_message_index: hit.message_index,
        snippet: hit.snippet,
        bookend_start: view.bookend_start,
        messages: view.messages,
        bookend_end: view.bookend_end,
        messages_before: view.messages_before,
        messages_after: view.messages_after,
    }))
}

pub(crate) fn load_browse_session(
    conn: &Connection,
    session_id: SessionId,
) -> Result<Option<BrowseSession>> {
    let Some((created_at, source, model)) = conn.query_three_optional_strings(
        "SELECT created_at, source, model FROM sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id.as_str())],
    )?
    else {
        return Ok(None);
    };
    let (message_count, preview) = match read_session_data_for_id(conn, &session_id)? {
        Some(data) => (
            data.messages.len(),
            first_user_preview(&data.messages, BROWSE_PREVIEW_CHARS),
        ),
        None => (
            indexed_message_count(conn, &session_id)?.unwrap_or(0),
            String::new(),
        ),
    };
    Ok(Some(BrowseSession {
        session_id,
        when: created_at.unwrap_or_default(),
        source: source.unwrap_or_else(|| "tui".into()),
        model: model.unwrap_or_else(|| "unknown".into()),
        message_count,
        preview,
    }))
}

pub(crate) fn load_read_view(
    conn: &Connection,
    session_id: SessionId,
    include_tool_results: bool,
) -> Result<Option<SessionReadView>> {
    let Some((meta, data)) = read_meta_and_data_for_id(conn, &session_id)? else {
        return Ok(None);
    };
    let total = data.messages.len();
    let truncated = total > READ_HEAD_MESSAGES + READ_TAIL_MESSAGES;
    let mut selected = Vec::new();
    if truncated {
        selected.extend(data.messages.iter().take(READ_HEAD_MESSAGES));
        selected.extend(
            data.messages
                .iter()
                .skip(total.saturating_sub(READ_TAIL_MESSAGES)),
        );
    } else {
        selected.extend(data.messages.iter());
    }
    let tool_names = tool_name_map(&data.messages);
    let messages = selected
        .into_iter()
        .map(|message| {
            evidence_message_for_session_message(message, &tool_names, include_tool_results, false)
        })
        .filter(|message| !message.content.trim().is_empty())
        .collect::<Vec<_>>();
    Ok(Some(SessionReadView {
        session_id,
        when: meta.when,
        source: meta.source,
        model: meta.model,
        message_count: total,
        truncated,
        messages,
    }))
}

pub(crate) fn load_scroll_view(
    conn: &Connection,
    session_id: SessionId,
    around_message_index: usize,
    window: usize,
    include_tool_results: bool,
) -> Result<Option<SessionScrollView>> {
    let Some((meta, data)) = read_meta_and_data_for_id(conn, &session_id)? else {
        return Ok(None);
    };
    if data
        .messages
        .iter()
        .all(|message| message.index != around_message_index)
    {
        return Ok(None);
    }
    let tool_names = tool_name_map(&data.messages);
    let start = around_message_index.saturating_sub(window);
    let end = around_message_index.saturating_add(window);
    let messages = data
        .messages
        .iter()
        .filter(|message| message.index >= start && message.index <= end)
        .map(|message| {
            evidence_message_for_session_message(
                message,
                &tool_names,
                include_tool_results,
                message.index == around_message_index,
            )
        })
        .filter(|message| !message.content.trim().is_empty())
        .collect::<Vec<_>>();
    let messages_before = data
        .messages
        .iter()
        .filter(|message| message.index < around_message_index)
        .count()
        .min(window);
    let messages_after = data
        .messages
        .iter()
        .filter(|message| message.index > around_message_index)
        .count()
        .min(window);
    Ok(Some(SessionScrollView {
        session_id,
        when: meta.when,
        source: meta.source,
        model: meta.model,
        around_message_index,
        window,
        messages,
        messages_before,
        messages_after,
    }))
}

struct AnchoredView {
    bookend_start: Vec<SessionSearchMessage>,
    messages: Vec<SessionSearchMessage>,
    bookend_end: Vec<SessionSearchMessage>,
    messages_before: usize,
    messages_after: usize,
}

fn anchored_view(
    messages: &[crate::session::SessionMessage],
    anchor_index: usize,
    include_tool_results: bool,
) -> AnchoredView {
    let tool_names = tool_name_map(messages);
    let window_start = anchor_index.saturating_sub(DISCOVERY_WINDOW);
    let window_end = anchor_index.saturating_add(DISCOVERY_WINDOW);
    let render = |message: &crate::session::SessionMessage| {
        evidence_message_for_session_message(
            message,
            &tool_names,
            include_tool_results,
            message.index == anchor_index,
        )
    };
    let messages_window = messages
        .iter()
        .filter(|message| message.index >= window_start && message.index <= window_end)
        .map(render)
        .filter(|message| !message.content.trim().is_empty())
        .collect::<Vec<_>>();
    let bookend_start = messages
        .iter()
        .filter(|message| message.index < window_start)
        .filter(|message| !message_has_tool_results(message))
        .map(render)
        .filter(|message| !message.content.trim().is_empty())
        .take(DISCOVERY_BOOKEND)
        .collect::<Vec<_>>();
    let mut bookend_end = messages
        .iter()
        .rev()
        .filter(|message| message.index > window_end)
        .filter(|message| !message_has_tool_results(message))
        .map(render)
        .filter(|message| !message.content.trim().is_empty())
        .take(DISCOVERY_BOOKEND)
        .collect::<Vec<_>>();
    bookend_end.reverse();
    let messages_before = messages
        .iter()
        .filter(|message| message.index < anchor_index)
        .count()
        .min(DISCOVERY_WINDOW);
    let messages_after = messages
        .iter()
        .filter(|message| message.index > anchor_index)
        .count()
        .min(DISCOVERY_WINDOW);
    AnchoredView {
        bookend_start,
        messages: messages_window,
        bookend_end,
        messages_before,
        messages_after,
    }
}

fn read_meta_and_data_for_id(
    conn: &Connection,
    session_id: &SessionId,
) -> Result<Option<(SessionSearchSessionMeta, SessionDiskData)>> {
    let Some((when, source, model)) = conn.query_three_optional_strings(
        "SELECT created_at, source, model FROM sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id.as_str())],
    )?
    else {
        return Ok(None);
    };
    let Some(data) = read_session_data_for_id(conn, session_id)? else {
        return Ok(None);
    };
    Ok(Some((
        SessionSearchSessionMeta {
            when: when.unwrap_or_default(),
            source: source.unwrap_or_else(|| "tui".into()),
            model: model.unwrap_or_else(|| "unknown".into()),
        },
        data,
    )))
}

fn read_session_data_for_id(
    conn: &Connection,
    session_id: &SessionId,
) -> Result<Option<SessionDiskData>> {
    let Some(session_path) = conn.query_one_string(
        "SELECT session_path FROM sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id.as_str())],
    )?
    else {
        return Ok(None);
    };
    read_session_disk_data(Path::new(&session_path)).map(Some)
}

fn indexed_message_count(conn: &Connection, session_id: &SessionId) -> Result<Option<usize>> {
    conn.query_one_i64(
        "SELECT message_count FROM indexed_sessions WHERE session_id = ?1;",
        &[SqlValue::Text(session_id.as_str())],
    )
    .context("读取 indexed_sessions.message_count")?
    .map(i64_to_usize)
    .transpose()
}

fn i64_to_usize(value: i64) -> Result<usize> {
    usize::try_from(value).context("negative SQLite count")
}
