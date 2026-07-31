//! JSONL 追加、读取与按大小滚动归档工具。

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Copy)]
pub struct JsonlRotationConfig {
    pub max_file_bytes: u64,
    pub backup_count: usize,
}

pub async fn append_jsonl_record<T: Serialize>(
    path: &Path,
    value: &T,
    rotation: JsonlRotationConfig,
) -> anyhow::Result<()> {
    rotate_if_needed(path, rotation).await?;
    let Some(parent) = path.parent() else {
        anyhow::bail!("JSONL path 缺少父目录: {}", path.display());
    };
    fs::create_dir_all(parent).await?;
    let created_file = !fs::try_exists(path).await?;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("打开 JSONL 文件失败: {}", path.display()))?;
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    file.write_all(&line).await?;
    file.sync_data().await?;
    drop(file);
    if created_file {
        sync_dir(parent).await?;
    }
    rotate_if_needed(path, rotation).await?;
    Ok(())
}

pub async fn read_jsonl_records<T: DeserializeOwned>(dir: &Path) -> anyhow::Result<Vec<T>> {
    let paths = jsonl_paths(dir).await?;
    let mut out = Vec::new();
    for path in paths {
        let mut text = String::new();
        match fs::File::open(&path).await {
            Ok(mut file) => {
                file.read_to_string(&mut text).await?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.into()),
        }
        for (idx, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str(line) {
                Ok(record) => out.push(record),
                Err(err) => log::warn!(
                    target: "storage_jsonl",
                    "跳过坏 JSONL 行 path={} line={} err={err:#}",
                    path.display(),
                    idx + 1
                ),
            }
        }
    }
    Ok(out)
}

async fn rotate_if_needed(path: &Path, rotation: JsonlRotationConfig) -> anyhow::Result<()> {
    if rotation.max_file_bytes == 0 || rotation.backup_count == 0 {
        anyhow::bail!("JSONL rotation 配置必须大于 0");
    }
    let Ok(metadata) = fs::metadata(path).await else {
        return Ok(());
    };
    if metadata.len() < rotation.max_file_bytes {
        return Ok(());
    }
    let Some(parent) = path.parent() else {
        anyhow::bail!("JSONL path 缺少父目录: {}", path.display());
    };
    fs::create_dir_all(parent).await?;
    let archive = unique_archive_path(parent).await?;
    fs::rename(path, &archive)
        .await
        .with_context(|| format!("滚动 JSONL 归档失败: {}", path.display()))?;
    sync_dir(parent).await?;
    cleanup_archives(parent, rotation.backup_count).await
}

async fn unique_archive_path(dir: &Path) -> anyhow::Result<PathBuf> {
    for attempt in 0..100u32 {
        let suffix = if attempt == 0 {
            String::new()
        } else {
            format!("_{attempt}")
        };
        let path = dir.join(format!(
            "archive_{}{}.jsonl",
            Utc::now().timestamp_millis(),
            suffix
        ));
        if !fs::try_exists(&path).await? {
            return Ok(path);
        }
    }
    anyhow::bail!("生成 JSONL archive 文件名失败: {}", dir.display())
}

async fn cleanup_archives(dir: &Path, backup_count: usize) -> anyhow::Result<()> {
    let mut archives = archive_paths(dir).await?;
    if archives.len() <= backup_count {
        return Ok(());
    }
    archives.sort_by_key(|path| archive_sort_key(path));
    let remove_count = archives.len() - backup_count;
    let mut removed = false;
    for path in archives.into_iter().take(remove_count) {
        if let Err(err) = fs::remove_file(&path).await {
            if err.kind() != std::io::ErrorKind::NotFound {
                return Err(err.into());
            }
        } else {
            removed = true;
        }
    }
    if removed {
        sync_dir(dir).await?;
    }
    Ok(())
}

async fn jsonl_paths(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = archive_paths(dir).await?;
    paths.sort_by_key(|path| archive_sort_key(path));
    let current = dir.join("current.jsonl");
    if fs::try_exists(&current).await.unwrap_or(false) {
        paths.push(current);
    }
    Ok(paths)
}

async fn archive_paths(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    if !fs::try_exists(dir).await.unwrap_or(false) {
        return Ok(vec![]);
    }
    let mut rd = fs::read_dir(dir).await?;
    let mut paths = Vec::new();
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if file_name.starts_with("archive_") && file_name.ends_with(".jsonl") {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn archive_sort_key(path: &Path) -> (i64, u32, String) {
    let file_name = path.file_name().and_then(OsStr::to_str).unwrap_or_default();
    let Some(stem) = file_name
        .strip_prefix("archive_")
        .and_then(|name| name.strip_suffix(".jsonl"))
    else {
        return (i64::MAX, u32::MAX, file_name.to_owned());
    };
    let (timestamp, suffix) = stem.split_once('_').unwrap_or((stem, "0"));
    (
        timestamp.parse().unwrap_or(i64::MAX),
        suffix.parse().unwrap_or(u32::MAX),
        file_name.to_owned(),
    )
}

async fn sync_dir(dir: &Path) -> anyhow::Result<()> {
    let dir_file = fs::File::open(dir)
        .await
        .with_context(|| format!("打开目录失败: {}", dir.display()))?;
    dir_file.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Row {
        id: String,
        text: String,
    }

    #[tokio::test]
    async fn append_and_read_jsonl_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("current.jsonl");
        let rotation = JsonlRotationConfig {
            max_file_bytes: 1024,
            backup_count: 2,
        };
        append_jsonl_record(
            &path,
            &Row {
                id: "a".into(),
                text: "hello".into(),
            },
            rotation,
        )
        .await
        .unwrap();
        append_jsonl_record(
            &path,
            &Row {
                id: "b".into(),
                text: "world".into(),
            },
            rotation,
        )
        .await
        .unwrap();

        let rows: Vec<Row> = read_jsonl_records(dir.path()).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "a");
        assert_eq!(rows[1].id, "b");
    }

    #[tokio::test]
    async fn read_jsonl_skips_bad_lines() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("current.jsonl"),
            br#"{"id":"a","text":"ok"}
not-json
{"id":"b","text":"ok"}
"#,
        )
        .await
        .unwrap();

        let rows: Vec<Row> = read_jsonl_records(dir.path()).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "a");
        assert_eq!(rows[1].id, "b");
    }

    #[tokio::test]
    async fn append_rotates_and_keeps_backup_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("current.jsonl");
        let rotation = JsonlRotationConfig {
            max_file_bytes: 1,
            backup_count: 2,
        };
        for idx in 0..5 {
            append_jsonl_record(
                &path,
                &Row {
                    id: idx.to_string(),
                    text: "payload".repeat(10),
                },
                rotation,
            )
            .await
            .unwrap();
        }

        let archives = archive_paths(dir.path()).await.unwrap();
        assert!(archives.len() <= 2);
        let rows: Vec<Row> = read_jsonl_records(dir.path()).await.unwrap();
        assert!(!rows.is_empty());
    }

    #[tokio::test]
    async fn read_jsonl_orders_same_timestamp_archives_by_numeric_suffix() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("archive_1000_10.jsonl"),
            br#"{"id":"10","text":"archive"}
"#,
        )
        .await
        .unwrap();
        fs::write(
            dir.path().join("archive_1000_2.jsonl"),
            br#"{"id":"2","text":"archive"}
"#,
        )
        .await
        .unwrap();
        fs::write(
            dir.path().join("current.jsonl"),
            br#"{"id":"current","text":"current"}
"#,
        )
        .await
        .unwrap();

        let rows: Vec<Row> = read_jsonl_records(dir.path()).await.unwrap();
        assert_eq!(rows[0].id, "2");
        assert_eq!(rows[1].id, "10");
        assert_eq!(rows[2].id, "current");
    }
}
