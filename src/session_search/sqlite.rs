//! session_search 专用 SQLite 最小封装。
//!
//! 本模块只暴露 session_search 需要的少量 SQLite 操作，底层使用
//! `rusqlite` + bundled SQLite，避免依赖系统 `sqlite3` CLI 或系统
//! `libsqlite3` 的 FTS5 编译选项。

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{
    params_from_iter,
    types::{ToSql, ToSqlOutput, Value, ValueRef},
    Error, ErrorCode, OpenFlags,
};

pub enum SqlValue<'a> {
    Text(&'a str),
    TextOwned(String),
    Integer(i64),
}

impl ToSql for SqlValue<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Self::Text(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes()))),
            Self::TextOwned(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes()))),
            Self::Integer(value) => Ok(ToSqlOutput::Owned(Value::Integer(*value))),
        }
    }
}

pub type OptionalStringTriple = (Option<String>, Option<String>, Option<String>);

pub struct Connection {
    raw: rusqlite::Connection,
}

impl Connection {
    pub fn open(path: &Path) -> Result<Self> {
        let raw = rusqlite::Connection::open(path)
            .with_context(|| format!("打开 SQLite 数据库: {}", path.display()))?;
        Ok(Self { raw })
    }

    pub fn open_read_only(path: &Path) -> Result<Self> {
        let raw = rusqlite::Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("只读打开 SQLite 数据库: {}", path.display()))?;
        Ok(Self { raw })
    }

    pub fn busy_timeout(&self, duration: Duration) -> Result<()> {
        self.raw
            .busy_timeout(duration)
            .context("设置 SQLite busy_timeout")
    }

    pub fn enable_wal_with_delete_fallback(&self) -> Result<()> {
        match self.query_one_string("PRAGMA journal_mode=WAL;", &[]) {
            Ok(Some(mode)) if mode.eq_ignore_ascii_case("wal") => Ok(()),
            Ok(Some(mode)) => {
                log::warn!(
                    target: "session_search",
                    "session_search SQLite WAL 未启用（mode={mode}），回退到 DELETE journal"
                );
                self.query_one_string("PRAGMA journal_mode=DELETE;", &[])
                    .context("session_search SQLite journal_mode DELETE fallback")?;
                Ok(())
            }
            Ok(None) => {
                log::warn!(
                    target: "session_search",
                    "session_search SQLite WAL 未返回 journal mode，回退到 DELETE journal"
                );
                self.query_one_string("PRAGMA journal_mode=DELETE;", &[])
                    .context("session_search SQLite journal_mode DELETE fallback")?;
                Ok(())
            }
            Err(e) => {
                log::warn!(
                    target: "session_search",
                    "session_search SQLite WAL 启用失败，回退到 DELETE journal: {e}"
                );
                self.query_one_string("PRAGMA journal_mode=DELETE;", &[])
                    .context("session_search SQLite journal_mode DELETE fallback")?;
                Ok(())
            }
        }
    }

    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.raw.execute_batch(sql).context("执行 SQLite batch SQL")
    }

    pub fn execute(&self, sql: &str, params: &[SqlValue<'_>]) -> Result<()> {
        self.raw
            .execute(sql, params_from_iter(params.iter()))
            .with_context(|| format!("执行 SQLite SQL: {sql}"))?;
        Ok(())
    }

    pub fn query_one_i64(&self, sql: &str, params: &[SqlValue<'_>]) -> Result<Option<i64>> {
        let mut stmt = self.prepare(sql)?;
        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        match rows.next()? {
            Some(row) => row.get(0).map(Some).context("读取 SQLite i64 列"),
            None => Ok(None),
        }
    }

    pub fn query_one_string(&self, sql: &str, params: &[SqlValue<'_>]) -> Result<Option<String>> {
        let mut stmt = self.prepare(sql)?;
        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        match rows.next()? {
            Some(row) => row
                .get::<_, Option<String>>(0)
                .context("读取 SQLite string 列"),
            None => Ok(None),
        }
    }

    pub fn query_three_optional_strings(
        &self,
        sql: &str,
        params: &[SqlValue<'_>],
    ) -> Result<Option<OptionalStringTriple>> {
        let mut stmt = self.prepare(sql)?;
        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        match rows.next()? {
            Some(row) => Ok(Some((
                row.get::<_, Option<String>>(0)
                    .context("读取 SQLite string 列 0")?,
                row.get::<_, Option<String>>(1)
                    .context("读取 SQLite string 列 1")?,
                row.get::<_, Option<String>>(2)
                    .context("读取 SQLite string 列 2")?,
            ))),
            None => Ok(None),
        }
    }

    pub fn query_strings(&self, sql: &str, params: &[SqlValue<'_>]) -> Result<Vec<String>> {
        let mut stmt = self.prepare(sql)?;
        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            if let Some(value) = row
                .get::<_, Option<String>>(0)
                .context("读取 SQLite string 列")?
            {
                out.push(value);
            }
        }
        Ok(out)
    }

    pub fn query_string_quads(
        &self,
        sql: &str,
        params: &[SqlValue<'_>],
    ) -> Result<Vec<(String, String, String, String)>> {
        let mut stmt = self.prepare(sql)?;
        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let first = row
                .get::<_, Option<String>>(0)
                .context("读取 SQLite string 列 0")?
                .unwrap_or_default();
            let second = match row.get_ref(1).context("读取 SQLite 列 1")? {
                ValueRef::Integer(value) => value.to_string(),
                ValueRef::Text(value) => String::from_utf8_lossy(value).to_string(),
                ValueRef::Null => String::new(),
                other => anyhow::bail!("SQLite 列 1 类型不支持: {other:?}"),
            };
            let third = row
                .get::<_, Option<String>>(2)
                .context("读取 SQLite string 列 2")?
                .unwrap_or_default();
            let fourth = row
                .get::<_, Option<String>>(3)
                .context("读取 SQLite string 列 3")?
                .unwrap_or_default();
            out.push((first, second, third, fourth));
        }
        Ok(out)
    }

    fn prepare(&self, sql: &str) -> Result<rusqlite::Statement<'_>> {
        self.raw
            .prepare(sql)
            .with_context(|| format!("准备 SQLite SQL: {sql}"))
    }
}

pub fn is_busy_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|error| error.downcast_ref::<Error>())
        .any(|error| match error {
            Error::SqliteFailure(failure, _) => matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            ),
            _ => false,
        })
}
