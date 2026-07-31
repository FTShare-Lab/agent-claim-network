//! session_search 查询选择与 SQLite MATCH/LIKE 召回。
//!
//! 本模块只负责把 query 映射到普通 FTS5、CJK trigram FTS5 或 LIKE fallback，
//! 并返回去重前的命中锚点。窗口和原文 evidence 由 `view` 模块负责。

use std::str::FromStr;

use anyhow::{Context, Result};

use super::sqlite::{Connection, SqlValue};
use super::types::SessionSearchSort;
use crate::claim::SessionId;

#[derive(Debug, Clone)]
pub(crate) struct SearchHit {
    pub session_id: SessionId,
    pub message_index: usize,
    pub role: String,
    pub snippet: String,
}

#[derive(Clone, Copy)]
enum FtsTable {
    Unicode,
    Trigram,
}

impl FtsTable {
    fn name(self) -> &'static str {
        match self {
            Self::Unicode => "messages_fts",
            Self::Trigram => "messages_fts_trigram",
        }
    }
}

pub(crate) fn select_matching_messages(
    conn: &Connection,
    query: &str,
    limit: usize,
    exclude_session_id: Option<&SessionId>,
    sort: SessionSearchSort,
    include_tool_results: bool,
) -> Result<Vec<SearchHit>> {
    if contains_cjk(query) {
        return select_cjk_matches(
            conn,
            query,
            limit,
            exclude_session_id,
            sort,
            include_tool_results,
        );
    }
    select_fts_matches(
        conn,
        FtsTable::Unicode,
        query,
        limit,
        exclude_session_id,
        sort,
        include_tool_results,
    )
}

fn select_cjk_matches(
    conn: &Connection,
    query: &str,
    limit: usize,
    exclude_session_id: Option<&SessionId>,
    sort: SessionSearchSort,
    include_tool_results: bool,
) -> Result<Vec<SearchHit>> {
    let raw_query = query.trim_matches('"').trim();
    let query_terms = raw_query
        .split_whitespace()
        .filter(|token| !is_boolean_operator(token))
        .collect::<Vec<_>>();
    let any_short_term = query_terms.iter().any(|token| token.chars().count() < 3);
    if count_cjk(raw_query) >= 3 && !any_short_term {
        let trigram_query = raw_query
            .split_whitespace()
            .map(|token| {
                if is_boolean_operator(token) {
                    token.to_ascii_uppercase()
                } else {
                    format!("\"{}\"", token.replace('"', "\"\""))
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        return select_fts_matches(
            conn,
            FtsTable::Trigram,
            &trigram_query,
            limit,
            exclude_session_id,
            sort,
            include_tool_results,
        );
    }
    select_like_matches(
        conn,
        raw_query,
        limit,
        exclude_session_id,
        sort,
        include_tool_results,
    )
}

fn select_fts_matches(
    conn: &Connection,
    table: FtsTable,
    query: &str,
    limit: usize,
    exclude_session_id: Option<&SessionId>,
    sort: SessionSearchSort,
    include_tool_results: bool,
) -> Result<Vec<SearchHit>> {
    let table = table.name();
    let limit_i64 = usize_to_i64(limit.saturating_mul(20).max(limit), "session_search limit")?;
    let order_by = match sort {
        SessionSearchSort::Relevance => "rank",
        SessionSearchSort::Newest => "sessions.updated_at DESC, rank",
        SessionSearchSort::Oldest => "sessions.created_at ASC, rank",
    };
    let mut sql = format!(
        "SELECT {table}.session_id, {table}.message_index, {table}.role,
                snippet({table}, 3, '>>>', '<<<', '...', 40) AS snippet
         FROM {table}
         JOIN sessions ON sessions.session_id = {table}.session_id
         WHERE {table} MATCH ?1"
    );
    let mut params = vec![SqlValue::Text(query)];
    let mut next_param = 2;
    let exclude_raw;
    if let Some(exclude) = exclude_session_id {
        exclude_raw = exclude.as_str();
        sql.push_str(&format!(" AND {table}.session_id <> ?{next_param}"));
        params.push(SqlValue::Text(exclude_raw));
        next_param += 1;
    }
    if !include_tool_results {
        sql.push_str(&format!(
            " AND {table}.content_text NOT LIKE '[tool_result %'"
        ));
    }
    sql.push_str(&format!(" ORDER BY {order_by} LIMIT ?{next_param};"));
    params.push(SqlValue::Integer(limit_i64));

    select_search_hits(conn, &sql, &params, limit)
}

fn select_like_matches(
    conn: &Connection,
    query: &str,
    limit: usize,
    exclude_session_id: Option<&SessionId>,
    sort: SessionSearchSort,
    include_tool_results: bool,
) -> Result<Vec<SearchHit>> {
    let expression = like_expression(query);
    let Some(first_token) = expression.first_positive.as_deref() else {
        return Ok(Vec::new());
    };
    let mut sql = String::from(
        "SELECT messages.session_id, messages.message_index, messages.role,
                substr(messages.content_text, max(1, instr(messages.content_text, ?1) - 40), 120) AS snippet
         FROM messages
         JOIN sessions ON sessions.session_id = messages.session_id
         WHERE (",
    );
    let mut params = vec![SqlValue::Text(first_token)];
    let mut next_param = 2;
    for (idx, term) in expression.positive_terms.iter().enumerate() {
        if idx > 0 {
            sql.push_str(match term.connector {
                LikeConnector::And => " AND ",
                LikeConnector::Or => " OR ",
            });
        }
        sql.push_str(&format!(
            "messages.content_text LIKE ?{next_param} ESCAPE '\\'"
        ));
        let pattern = format!("%{}%", escape_like(&term.text));
        params.push(SqlValue::TextOwned(pattern));
        next_param += 1;
    }
    sql.push(')');
    for token in &expression.negative_terms {
        sql.push_str(&format!(
            " AND messages.content_text NOT LIKE ?{next_param} ESCAPE '\\'"
        ));
        let pattern = format!("%{}%", escape_like(token));
        params.push(SqlValue::TextOwned(pattern));
        next_param += 1;
    }
    let exclude_raw;
    if let Some(exclude) = exclude_session_id {
        exclude_raw = exclude.as_str();
        sql.push_str(&format!(" AND messages.session_id <> ?{next_param}"));
        params.push(SqlValue::Text(exclude_raw));
        next_param += 1;
    }
    if !include_tool_results {
        sql.push_str(" AND messages.content_text NOT LIKE '[tool_result %'");
    }
    let order_by = match sort {
        SessionSearchSort::Relevance | SessionSearchSort::Newest => {
            "sessions.updated_at DESC, messages.message_index ASC"
        }
        SessionSearchSort::Oldest => "sessions.created_at ASC, messages.message_index ASC",
    };
    sql.push_str(&format!(" ORDER BY {order_by} LIMIT ?{next_param};"));
    let fetch_limit = limit.saturating_mul(20).max(limit);
    params.push(SqlValue::Integer(usize_to_i64(
        fetch_limit,
        "session_search LIKE limit",
    )?));

    select_search_hits(conn, &sql, &params, limit)
}

struct LikeExpression {
    first_positive: Option<String>,
    positive_terms: Vec<LikeTerm>,
    negative_terms: Vec<String>,
}

struct LikeTerm {
    connector: LikeConnector,
    text: String,
}

#[derive(Clone, Copy)]
enum LikeConnector {
    And,
    Or,
}

fn like_expression(query: &str) -> LikeExpression {
    let mut positive_terms = Vec::new();
    let mut negative_terms = Vec::new();
    let mut connector = LikeConnector::Or;
    let mut negate_next = false;
    for token in query.split_whitespace() {
        match token.to_ascii_uppercase().as_str() {
            "AND" => {
                connector = LikeConnector::And;
            }
            "OR" => {
                connector = LikeConnector::Or;
            }
            "NOT" => {
                negate_next = true;
                connector = LikeConnector::And;
            }
            _ if negate_next => {
                negative_terms.push(token.to_string());
                negate_next = false;
                connector = LikeConnector::And;
            }
            _ => {
                positive_terms.push(LikeTerm {
                    connector,
                    text: token.to_string(),
                });
                negate_next = false;
                connector = LikeConnector::Or;
            }
        }
    }
    let first_positive = positive_terms.first().map(|term| term.text.clone());
    LikeExpression {
        first_positive,
        positive_terms,
        negative_terms,
    }
}

fn select_search_hits(
    conn: &Connection,
    sql: &str,
    params: &[SqlValue<'_>],
    limit: usize,
) -> Result<Vec<SearchHit>> {
    let rows = conn.query_string_quads(sql, params)?;
    let mut out = Vec::new();
    for (session_id, message_index, role, snippet) in rows {
        let session_id = SessionId::from_str(&session_id)?;
        if out
            .iter()
            .any(|hit: &SearchHit| hit.session_id == session_id)
        {
            continue;
        }
        out.push(SearchHit {
            session_id,
            message_index: message_index
                .parse::<usize>()
                .context("解析 session_search message_index")?,
            role,
            snippet,
        });
        if out.len() >= limit {
            break;
        }
    }
    Ok(out)
}

fn contains_cjk(text: &str) -> bool {
    text.chars().any(|ch| is_cjk_codepoint(u32::from(ch)))
}

fn count_cjk(text: &str) -> usize {
    text.chars()
        .filter(|ch| is_cjk_codepoint(u32::from(*ch)))
        .count()
}

fn is_cjk_codepoint(cp: u32) -> bool {
    matches!(
        cp,
        0x4E00..=0x9FFF
            | 0x3400..=0x4DBF
            | 0x20000..=0x2A6DF
            | 0x3000..=0x303F
            | 0x3040..=0x309F
            | 0x30A0..=0x30FF
            | 0xAC00..=0xD7AF
    )
}

fn is_boolean_operator(token: &str) -> bool {
    matches!(token.to_ascii_uppercase().as_str(), "AND" | "OR" | "NOT")
}

fn escape_like(token: &str) -> String {
    token
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn usize_to_i64(value: usize, label: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{label} exceeds i64 range"))
}
