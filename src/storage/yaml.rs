//! YAML 读写 + 原子写。
//!
//! 原子写流程为：写临时文件 → `fsync` → `rename` 覆盖目标，
//! 避免进程崩溃留下半写文件。业务级并发锁由调用方按各自协议持有。

use std::path::{Path, PathBuf};

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::AsyncWriteExt;

const TEMP_FILE_CREATE_ATTEMPTS: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("文件 I/O 失败 ({path:?}): {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("YAML 序列化失败: {0}")]
    Encode(#[from] serde_yaml_ng::Error),
    #[error("YAML 反序列化失败 ({path:?}): {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },
}

impl StorageError {
    fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }

    /// 提取内部的 `std::io::Error`，路径信息丢失。
    pub fn into_io_error(self) -> std::io::Error {
        match self {
            Self::Io { source, .. } => source,
            Self::Encode(e) => std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            Self::Decode { source, .. } => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, source)
            }
        }
    }
}

/// 异步读取并解析 YAML 文件
pub async fn read_yaml<T: DeserializeOwned>(path: &Path) -> Result<T, StorageError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| StorageError::io(path, e))?;
    serde_yaml_ng::from_slice(&bytes).map_err(|source| StorageError::Decode {
        path: path.to_path_buf(),
        source,
    })
}

/// 原子写 YAML：先写到 `<path>.tmp.<rand>`，fsync 后 rename 覆盖。
/// 父目录不存在会自动创建。
pub async fn write_yaml_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), StorageError> {
    let yaml = serde_yaml_ng::to_string(value)?;

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| StorageError::io(parent, e))?;
    }

    let (tmp_path, mut f) = create_unique_temp_file(path).await?;
    {
        f.write_all(yaml.as_bytes())
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
        f.flush()
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
        f.sync_all()
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
    }
    tokio::fs::rename(&tmp_path, path)
        .await
        .map_err(|e| StorageError::io(path, e))?;
    Ok(())
}

/// 原子写纯文本文件：与 `write_yaml_atomic` 同流程（tmp → fsync → rename），
/// 但不做 YAML 序列化，直接写原始 bytes。供 tool 模块等非 YAML 写入场景使用。
/// 父目录不存在会自动创建。
pub async fn write_text_atomic(path: &Path, content: &[u8]) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| StorageError::io(parent, e))?;
    }

    let (tmp_path, mut f) = create_unique_temp_file(path).await?;
    {
        f.write_all(content)
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
        f.flush()
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
        f.sync_all()
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
    }
    tokio::fs::rename(&tmp_path, path)
        .await
        .map_err(|e| StorageError::io(path, e))?;
    Ok(())
}

/// 原子写纯文本，并在 rename 前执行 best-effort stale guard。
///
/// `expected=None` 表示期望目标不存在。返回 `false` 表示目标在准备临时文件期间
/// 已被检测到变化，未执行 rename。调用方可用跨进程锁串行化协作写入；未使用相同
/// 锁协议的外部写入仍可能落入最终校验与 rename 之间的窄竞态窗口。
pub async fn write_text_atomic_if_unchanged(
    path: &Path,
    content: &[u8],
    expected: Option<&[u8]>,
) -> Result<bool, StorageError> {
    let preserved_permissions = if expected.is_some() {
        match tokio::fs::symlink_metadata(path).await {
            Ok(metadata) if metadata.file_type().is_file() => Some(metadata.permissions()),
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(StorageError::io(path, error)),
        }
    } else {
        None
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| StorageError::io(parent, e))?;
    }

    let (tmp_path, mut file) = create_unique_temp_file(path).await?;
    {
        file.write_all(content)
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
        file.flush()
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
        if let Some(permissions) = preserved_permissions.as_ref() {
            file.set_permissions(permissions.clone())
                .await
                .map_err(|e| StorageError::io(&tmp_path, e))?;
        }
        file.sync_all()
            .await
            .map_err(|e| StorageError::io(&tmp_path, e))?;
    }

    let unchanged =
        match target_matches_expected(path, expected, preserved_permissions.as_ref()).await {
            Ok(unchanged) => unchanged,
            Err(error) => {
                remove_temp_file(&tmp_path).await?;
                return Err(error);
            }
        };
    if !unchanged {
        remove_temp_file(&tmp_path).await?;
        return Ok(false);
    }

    tokio::fs::rename(&tmp_path, path)
        .await
        .map_err(|e| StorageError::io(path, e))?;
    Ok(true)
}

async fn target_matches_expected(
    path: &Path,
    expected: Option<&[u8]>,
    expected_permissions: Option<&std::fs::Permissions>,
) -> Result<bool, StorageError> {
    let Some(expected) = expected else {
        return match tokio::fs::symlink_metadata(path).await {
            Ok(_) => Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(StorageError::io(path, error)),
        };
    };
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => return Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(StorageError::io(path, error)),
    };
    if expected_permissions.is_some_and(|permissions| metadata.permissions() != permissions.clone())
    {
        return Ok(false);
    }
    if metadata.len() != u64::try_from(expected.len()).unwrap_or(u64::MAX) {
        return Ok(false);
    }
    let current = tokio::fs::read(path)
        .await
        .map_err(|error| StorageError::io(path, error))?;
    Ok(current == expected)
}

async fn remove_temp_file(path: &Path) -> Result<(), StorageError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::io(path, error)),
    }
}

async fn create_unique_temp_file(
    target_path: &Path,
) -> Result<(PathBuf, tokio::fs::File), StorageError> {
    let mut last_collision = None;
    for _ in 0..TEMP_FILE_CREATE_ATTEMPTS {
        let tmp_path = tmp_sibling(target_path);
        match open_temp_file_exclusive(&tmp_path).await {
            Ok(file) => return Ok((tmp_path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                last_collision = Some(tmp_path);
            }
            Err(error) => return Err(StorageError::io(&tmp_path, error)),
        }
    }

    let path = last_collision.unwrap_or_else(|| tmp_sibling(target_path));
    Err(StorageError::io(
        &path,
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "多次尝试后仍无法独占创建原子写临时文件",
        ),
    ))
}

async fn open_temp_file_exclusive(path: &Path) -> std::io::Result<tokio::fs::File> {
    tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
}

fn tmp_sibling(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    use rand::RngCore;
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    name.push(format!(".tmp.{}", hex::encode(buf)));
    match path.parent() {
        Some(p) => p.join(name),
        None => PathBuf::from(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Sample {
        name: String,
        n: u32,
    }

    #[tokio::test]
    async fn round_trip_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sub").join("a.yaml");
        let s = Sample {
            name: "x".into(),
            n: 42,
        };
        write_yaml_atomic(&p, &s).await.unwrap();
        let back: Sample = read_yaml(&p).await.unwrap();
        assert_eq!(s, back);
    }

    #[tokio::test]
    async fn atomic_write_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.yaml");
        write_yaml_atomic(
            &p,
            &Sample {
                name: "v1".into(),
                n: 1,
            },
        )
        .await
        .unwrap();
        write_yaml_atomic(
            &p,
            &Sample {
                name: "v2".into(),
                n: 2,
            },
        )
        .await
        .unwrap();
        let back: Sample = read_yaml(&p).await.unwrap();
        assert_eq!(back.name, "v2");
    }

    #[tokio::test]
    async fn read_missing_file_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("missing.yaml");
        let r: Result<Sample, _> = read_yaml(&p).await;
        assert!(matches!(r, Err(StorageError::Io { .. })));
    }

    #[tokio::test]
    async fn checked_atomic_write_rejects_changed_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        tokio::fs::write(&path, b"external").await.unwrap();

        let written = write_text_atomic_if_unchanged(&path, b"tool", Some(b"original"))
            .await
            .unwrap();

        assert!(!written);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"external");
    }

    #[tokio::test]
    async fn checked_atomic_write_rejects_concurrent_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        tokio::fs::write(&path, b"external").await.unwrap();

        let written = write_text_atomic_if_unchanged(&path, b"tool", None)
            .await
            .unwrap();

        assert!(!written);
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"external");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checked_atomic_write_preserves_existing_unix_mode() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("script.sh");
        tokio::fs::write(&path, b"old").await.unwrap();
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .await
            .unwrap();

        let written = write_text_atomic_if_unchanged(&path, b"new", Some(b"old"))
            .await
            .unwrap();

        assert!(written);
        let mode = tokio::fs::metadata(path)
            .await
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o755);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exclusive_temp_creation_rejects_preexisting_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim.txt");
        let temp_link = dir.path().join("note.txt.tmp.fixed");
        tokio::fs::write(&victim, b"keep").await.unwrap();
        tokio::fs::symlink(&victim, &temp_link).await.unwrap();

        let error = open_temp_file_exclusive(&temp_link)
            .await
            .expect_err("独占创建不得跟随已存在的 symlink");

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(tokio::fs::read(victim).await.unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checked_target_snapshot_rejects_unix_mode_change() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("script.sh");
        tokio::fs::write(&path, b"old").await.unwrap();
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .await
            .unwrap();
        let original_permissions = tokio::fs::metadata(&path).await.unwrap().permissions();
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .await
            .unwrap();

        let matches = target_matches_expected(&path, Some(b"old"), Some(&original_permissions))
            .await
            .unwrap();

        assert!(!matches, "内容未变但 mode 变化也必须触发 stale guard");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn checked_atomic_write_rejects_target_replaced_by_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        let replacement = dir.path().join("replacement.txt");
        tokio::fs::write(&replacement, b"old").await.unwrap();
        tokio::fs::symlink(&replacement, &path).await.unwrap();

        let written = write_text_atomic_if_unchanged(&path, b"new", Some(b"old"))
            .await
            .unwrap();

        assert!(!written);
        assert!(tokio::fs::symlink_metadata(&path)
            .await
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(tokio::fs::read(replacement).await.unwrap(), b"old");
    }
}
