//! 从权威 session 文件读取 session_search 修复索引所需数据。

use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};

use super::types::SessionDiskData;
use crate::claim::SessionId;
use crate::session::{SessionMessage, SessionMetadata};
use crate::storage::paths;

pub(crate) fn list_session_dirs(agent_home: &Path) -> Result<Vec<PathBuf>> {
    let dir = paths::agent_home_sessions_dir(agent_home);
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Ok(out);
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

pub(crate) fn read_session_disk_data(session_dir: &Path) -> Result<SessionDiskData> {
    let messages_path = session_dir.join("messages.jsonl");
    let metadata = read_session_metadata(session_dir)?;
    let raw_messages = std::fs::read_to_string(&messages_path)
        .with_context(|| format!("读取 {}", messages_path.display()))?;
    let mut messages: Vec<SessionMessage> = Vec::new();
    for (line_no, line) in raw_messages.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let message = serde_json::from_str(line)
            .with_context(|| format!("解析 {} 第 {} 行", messages_path.display(), line_no + 1))?;
        messages.push(message);
    }
    Ok(SessionDiskData {
        metadata,
        messages,
        session_path: session_dir.to_path_buf(),
    })
}

pub(crate) fn read_session_metadata(session_dir: &Path) -> Result<SessionMetadata> {
    let metadata_path = session_dir.join("session.yaml");
    serde_yaml_ng::from_str(
        &std::fs::read_to_string(&metadata_path)
            .with_context(|| format!("读取 {}", metadata_path.display()))?,
    )
    .with_context(|| format!("解析 {}", metadata_path.display()))
}

pub(crate) fn session_id_from_dir(session_dir: &Path) -> Result<SessionId> {
    let name = session_dir
        .file_name()
        .and_then(|name| name.to_str())
        .context("session dir missing UTF-8 file name")?;
    SessionId::from_str(name).with_context(|| format!("解析 session dir name {name}"))
}
