//! 跨进程文件锁。
//!
//! 本模块只负责基于 lock 文件提供独占锁；业务层仍决定锁保护的临界区。
//! 获取锁可能阻塞，统一放入 `spawn_blocking`，避免卡住 async runtime。

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use fs2::FileExt;
use tokio::time::{sleep, Instant};

/// 持有一个跨进程独占文件锁；drop 时自动释放。
pub struct FileLockGuard {
    file: File,
    path: PathBuf,
}

impl FileLockGuard {
    /// 获取 `path` 对应的独占锁，必要时创建父目录和 lock 文件。
    pub async fn lock_exclusive(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            file.lock_exclusive()?;
            Ok(Self { file, path })
        })
        .await?
    }

    /// 在指定时间内轮询尝试获取独占锁，超时后返回错误。
    pub async fn lock_exclusive_timeout(
        path: impl AsRef<Path>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let deadline = Instant::now() + timeout;
        loop {
            match Self::try_lock_exclusive(&path).await? {
                Some(guard) => return Ok(guard),
                None if Instant::now() >= deadline => {
                    anyhow::bail!("获取文件锁超时: {}", path.display());
                }
                None => sleep(Duration::from_millis(50)).await,
            }
        }
    }

    /// 非阻塞尝试获取独占锁；锁已被其他进程持有时返回 `None`。
    pub async fn try_lock_exclusive(path: impl AsRef<Path>) -> anyhow::Result<Option<Self>> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            match file.try_lock_exclusive() {
                Ok(()) => Ok(Some(Self { file, path })),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
                Err(err) => Err(err.into()),
            }
        })
        .await?
    }
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        if let Err(err) = self.file.unlock() {
            log::warn!(
                target: "storage",
                "释放文件锁失败 ({}): {err}",
                self.path.display()
            );
        }
    }
}
