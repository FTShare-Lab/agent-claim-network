//! file 类工具的运行期 read state。
//!
//! 记录被"完整 file_read"过的文件内容与 mtime，供 file_write / file_patch 在写前做
//! read-before-write 与 stale 校验，避免模型基于旧内容覆盖用户或 formatter 的改动。
//! 仅存活于当前进程运行期，不随 journal 持久化；resume 后需重新 file_read。

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use tokio::sync::Mutex;

use crate::claim::SessionId;

/// read state 缓存的最大条目数；超出后按写入顺序淘汰最旧条目。淘汰只会让下次写入
/// 需要重新 file_read（fail-safe），不影响正确性，因此用简单 FIFO 即可。
const READ_STATE_MAX_ENTRIES: usize = 1024;

/// 写前校验所需的运行期读取状态。
#[derive(Debug, Clone)]
enum ReadState {
    /// 一次完整 file_read 或成功写入后的文件快照。
    Complete {
        content: String,
        mtime: Option<SystemTime>,
    },
    /// 读取覆盖整文件时受 file_read_max_chars 限制，不能授予写权限。
    ConfigTruncated { max_chars: usize },
}

/// 写前校验结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStateVerdict {
    /// 没有该文件的完整 read state：要求先 file_read。
    Missing,
    /// read state 与磁盘当前状态一致：可安全写入。
    Fresh,
    /// 文件在上次 file_read 之后被改动：拒绝写入，要求重新 file_read。
    Stale,
    /// 整文件读取被当前 file_read_max_chars 配置截断，必须由用户提高配置后重启。
    ConfigTruncated { max_chars: usize },
}

/// 进程内 read state 存储；通过 `Arc` 在 `ToolRegistry` 的 clone 间共享。
#[derive(Debug, Default)]
pub struct ReadStateStore {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    map: HashMap<ReadStateKey, ReadState>,
    order: VecDeque<ReadStateKey>,
}

/// read state 的调用方隔离域；parent 按 session 共享，delegation child 再按 id 隔离。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadStateScope {
    session_id: Option<SessionId>,
    caller_id: Option<String>,
}

impl ReadStateScope {
    pub fn new(session_id: Option<SessionId>, caller_id: Option<String>) -> Self {
        Self {
            session_id,
            caller_id,
        }
    }
}

type ReadStateKey = (ReadStateScope, PathBuf);

impl ReadStateStore {
    /// 记录 / 更新一次完整读取（file_read 成功且非窗口 / 截断 / 附件）或写入后的最新内容。
    pub async fn record(
        &self,
        scope: &ReadStateScope,
        path: PathBuf,
        content: String,
        mtime: Option<SystemTime>,
    ) {
        let key = (scope.clone(), path);
        let mut inner = self.inner.lock().await;
        if inner
            .map
            .insert(key.clone(), ReadState::Complete { content, mtime })
            .is_none()
        {
            // 仅首次插入登记淘汰顺序；更新已有 key 不改变其顺序位置。
            inner.order.push_back(key);
            while inner.order.len() > READ_STATE_MAX_ENTRIES {
                if let Some(evicted) = inner.order.pop_front() {
                    inner.map.remove(&evicted);
                }
            }
        }
    }

    /// 记录被 file_read_max_chars 截断的整文件读取。
    ///
    /// 已经存在完整快照时保持其授权不变；一次后续分页读取不能撤销已经获得的写权限。
    pub async fn record_config_truncated(
        &self,
        scope: &ReadStateScope,
        path: PathBuf,
        max_chars: usize,
    ) {
        let key = (scope.clone(), path);
        let mut inner = self.inner.lock().await;
        if inner.map.contains_key(&key) {
            return;
        }
        inner
            .map
            .insert(key.clone(), ReadState::ConfigTruncated { max_chars });
        inner.order.push_back(key);
        while inner.order.len() > READ_STATE_MAX_ENTRIES {
            if let Some(evicted) = inner.order.pop_front() {
                inner.map.remove(&evicted);
            }
        }
    }

    /// 用磁盘当前内容 / mtime 判断该 key 的 read state 是否仍然新鲜。
    pub async fn evaluate(
        &self,
        scope: &ReadStateScope,
        path: &Path,
        current_content: &str,
        current_mtime: Option<SystemTime>,
    ) -> ReadStateVerdict {
        let inner = self.inner.lock().await;
        let key = (scope.clone(), path.to_path_buf());
        let Some(state) = inner.map.get(&key) else {
            return ReadStateVerdict::Missing;
        };
        match state {
            ReadState::ConfigTruncated { max_chars } => ReadStateVerdict::ConfigTruncated {
                max_chars: *max_chars,
            },
            ReadState::Complete { content, mtime } => {
                // 内容比对是权威判定：即使粗粒度文件系统的 mtime 未变，内容变化也必须拒绝写入。
                if content != current_content {
                    return ReadStateVerdict::Stale;
                }
                if *mtime != current_mtime {
                    log::debug!(
                        target: "tool_read_state",
                        "file read state mtime changed but content stayed identical: {}",
                        path.display()
                    );
                }
                ReadStateVerdict::Fresh
            }
        }
    }

    /// resume 同一 session 时清空其运行期写入许可，强制重新完整 file_read。
    pub async fn clear_session(&self, session_id: &SessionId) {
        let mut inner = self.inner.lock().await;
        inner
            .map
            .retain(|(scope, _), _| scope.session_id.as_ref() != Some(session_id));
        inner
            .order
            .retain(|(scope, _)| scope.session_id.as_ref() != Some(session_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn key(name: &str) -> PathBuf {
        PathBuf::from(name)
    }

    fn session() -> SessionId {
        "session_aaaaaaaa".parse().expect("测试 session id 合法")
    }

    fn scope() -> ReadStateScope {
        ReadStateScope::new(Some(session()), None)
    }

    #[tokio::test]
    async fn missing_without_record() {
        let store = ReadStateStore::default();
        assert_eq!(
            store.evaluate(&scope(), &key("a"), "content", None).await,
            ReadStateVerdict::Missing
        );
    }

    #[tokio::test]
    async fn fresh_when_content_matches() {
        let store = ReadStateStore::default();
        store.record(&scope(), key("a"), "hello".into(), None).await;
        assert_eq!(
            store.evaluate(&scope(), &key("a"), "hello", None).await,
            ReadStateVerdict::Fresh
        );
    }

    #[tokio::test]
    async fn stale_when_content_changed() {
        let store = ReadStateStore::default();
        store.record(&scope(), key("a"), "hello".into(), None).await;
        assert_eq!(
            store
                .evaluate(&scope(), &key("a"), "hello world", None)
                .await,
            ReadStateVerdict::Stale
        );
    }

    #[tokio::test]
    async fn same_mtime_with_different_content_is_stale() {
        let store = ReadStateStore::default();
        let now = SystemTime::now();
        store
            .record(&scope(), key("a"), "hello".into(), Some(now))
            .await;
        // coarse timestamp 或外部程序保留 mtime 时，内容差异仍必须触发 stale guard。
        assert_eq!(
            store
                .evaluate(&scope(), &key("a"), "anything", Some(now))
                .await,
            ReadStateVerdict::Stale
        );
    }

    #[tokio::test]
    async fn mtime_changed_but_same_content_is_fresh() {
        let store = ReadStateStore::default();
        let t0 = SystemTime::now();
        let t1 = t0 + Duration::from_secs(5);
        store
            .record(&scope(), key("a"), "hello".into(), Some(t0))
            .await;
        assert_eq!(
            store.evaluate(&scope(), &key("a"), "hello", Some(t1)).await,
            ReadStateVerdict::Fresh
        );
    }

    #[tokio::test]
    async fn config_truncated_read_requires_config_change_until_complete_read_replaces_it() {
        let store = ReadStateStore::default();
        store.record_config_truncated(&scope(), key("a"), 123).await;
        assert_eq!(
            store.evaluate(&scope(), &key("a"), "hello", None).await,
            ReadStateVerdict::ConfigTruncated { max_chars: 123 }
        );

        store.record(&scope(), key("a"), "hello".into(), None).await;
        assert_eq!(
            store.evaluate(&scope(), &key("a"), "hello", None).await,
            ReadStateVerdict::Fresh
        );
    }

    #[tokio::test]
    async fn config_truncated_read_does_not_replace_existing_complete_read() {
        let store = ReadStateStore::default();
        store.record(&scope(), key("a"), "hello".into(), None).await;
        store.record_config_truncated(&scope(), key("a"), 123).await;

        assert_eq!(
            store.evaluate(&scope(), &key("a"), "hello", None).await,
            ReadStateVerdict::Fresh
        );
    }

    #[tokio::test]
    async fn eviction_drops_oldest() {
        let store = ReadStateStore::default();
        for idx in 0..(READ_STATE_MAX_ENTRIES + 1) {
            store
                .record(&scope(), key(&format!("f{idx}")), "x".into(), None)
                .await;
        }
        // 最旧的 f0 被淘汰 → Missing；最新的仍在。
        assert_eq!(
            store.evaluate(&scope(), &key("f0"), "x", None).await,
            ReadStateVerdict::Missing
        );
        assert_eq!(
            store
                .evaluate(
                    &scope(),
                    &key(&format!("f{READ_STATE_MAX_ENTRIES}")),
                    "x",
                    None,
                )
                .await,
            ReadStateVerdict::Fresh
        );
    }
}
