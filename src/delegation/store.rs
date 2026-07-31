use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

use crate::claim::SessionId;
use crate::storage::{
    read_yaml, write_text_atomic, write_yaml_atomic, FileLockGuard, StorageError,
};

use super::types::{
    clamp_event_tail_limit, clamp_transcript_tail_limit, clamp_transcript_tail_max_chars,
    default_event_tail_limit, default_transcript_tail_limit, default_transcript_tail_max_chars,
    truncate_text_with_flag, DelegationArtifactRef, DelegationCompactionEvent,
    DelegationCompactionEventKind, DelegationCompactionState, DelegationCreateRequest,
    DelegationEvent, DelegationEventKind, DelegationId, DelegationMetadata, DelegationProgress,
    DelegationRead, DelegationReadMode, DelegationResult, DelegationStatus, DelegationSteering,
    DelegationSummary, DelegationTranscriptEntry, DelegationUpdate, READ_TEXT_MAX_CHARS,
    SUMMARY_CHANGED_FILES_LIMIT, SUMMARY_CHANGED_FILE_LIMIT, SUMMARY_FIELD_LIMIT,
    SUMMARY_TEXT_LIMIT,
};

const DELEGATION_YAML: &str = "delegation.yaml";
const EVENTS_JSONL: &str = "events.jsonl";
const EVENTS_SEQ: &str = "events.seq";
const DELEGATION_LOCK: &str = "delegation.lock";
const STEERING_JSONL: &str = "steering.jsonl";
const PROGRESS_JSON: &str = "progress.json";
const RESULT_MD: &str = "result.md";
const TRANSCRIPT_JSONL: &str = "transcript.jsonl";
const COMPACTION_JSON: &str = "compaction.json";
const COMPACTION_EVENTS_JSONL: &str = "compaction_events.jsonl";
const COMPACTION_CHECKPOINT_JSON: &str = "compaction_checkpoint.json";
const ID_MAX_ATTEMPTS: usize = 100;
const DEFAULT_STEERING_READ_LIMIT: usize = 64;
const DEFAULT_LIST_LIMIT: usize = 64;
const EVENT_TAIL_READ_WINDOW_BYTES: u64 = 256 * 1024;
const EVENT_TAIL_REPAIR_CHUNK_BYTES: u64 = 64 * 1024;
const EVENT_TAIL_REPAIR_MAX_LINE_BYTES: u64 = 256 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum DelegationStoreError {
    #[error("subagent 存储 I/O 失败 ({path:?}): {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("subagent 文件锁失败 ({path:?}): {source}")]
    FileLock {
        path: PathBuf,
        #[source]
        source: anyhow::Error,
    },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("subagent JSON 序列化失败: {0}")]
    JsonEncode(#[from] serde_json::Error),
    #[error("subagent 事件第 {line} 行解析失败: {source}")]
    JsonLine {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("subagent {0} 不存在")]
    NotFound(String),
    #[error("subagent id 尝试 {max_attempts} 次仍碰撞（最近一次候选 id={last_id}），目录={delegations_dir:?}")]
    IdCollisionExhausted {
        max_attempts: usize,
        last_id: String,
        delegations_dir: PathBuf,
    },
    #[error("subagent {id} 当前状态为 {status:?}，不能追加指令")]
    CannotSteer {
        id: DelegationId,
        status: DelegationStatus,
    },
    #[error("subagent {id} 当前状态为 {from:?}，不能转换到 {to:?}")]
    CannotTransition {
        id: DelegationId,
        from: DelegationStatus,
        to: DelegationStatus,
    },
    #[error("subagent {id} complete 结果必须是终态，实际为 {status:?}")]
    NonTerminalResult {
        id: DelegationId,
        status: DelegationStatus,
    },
    #[error("subagent {id} 当前状态为 {status:?}，不能更新进度")]
    CannotUpdateProgress {
        id: DelegationId,
        status: DelegationStatus,
    },
    #[error("subagent read mode 参数非法: {0}")]
    InvalidReadMode(String),
    #[error("subagent parent session 不匹配: store={expected} request={actual}")]
    ParentSessionMismatch {
        expected: SessionId,
        actual: SessionId,
    },
    #[error("session {session_id} 仍有 subagent 未能 abandoned: {failures}")]
    AbandonIncomplete {
        session_id: SessionId,
        failures: String,
    },
    #[error("subagent store 内部锁异常")]
    LockPoisoned,
}

impl DelegationStoreError {
    fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DelegationStore {
    session_dir: PathBuf,
    expected_session_id: Option<SessionId>,
    locks: Arc<StdMutex<BTreeMap<DelegationId, Arc<Mutex<()>>>>>,
    #[cfg(test)]
    inject_create_failure_after_metadata: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegationListPage {
    pub summaries: Vec<DelegationSummary>,
    pub omitted: usize,
}

impl DelegationStore {
    pub fn new(session_dir: PathBuf) -> Self {
        let expected_session_id = session_dir
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<SessionId>().ok());
        Self::new_with_session_id(session_dir, expected_session_id)
    }

    pub fn new_for_session(session_dir: PathBuf, session_id: SessionId) -> Self {
        Self::new_with_session_id(session_dir, Some(session_id))
    }

    fn new_with_session_id(session_dir: PathBuf, expected_session_id: Option<SessionId>) -> Self {
        Self {
            session_dir,
            expected_session_id,
            locks: Arc::new(StdMutex::new(BTreeMap::new())),
            #[cfg(test)]
            inject_create_failure_after_metadata: false,
        }
    }

    #[cfg(test)]
    fn with_create_failure_after_metadata(mut self) -> Self {
        self.inject_create_failure_after_metadata = true;
        self
    }

    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    pub fn delegations_dir(&self) -> PathBuf {
        self.session_dir.join("delegations")
    }

    pub fn delegation_dir(&self, id: &DelegationId) -> PathBuf {
        self.delegations_dir().join(id.as_str())
    }

    pub async fn create(
        &self,
        request: DelegationCreateRequest,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        self.create_with_id_factory(request, DelegationId::random)
            .await
    }

    pub async fn create_with_id_factory<F>(
        &self,
        request: DelegationCreateRequest,
        mut id_factory: F,
    ) -> Result<DelegationMetadata, DelegationStoreError>
    where
        F: FnMut() -> DelegationId,
    {
        self.ensure_parent_session_matches(&request.parent_session_id)?;
        let delegations_dir = self.delegations_dir();
        fs::create_dir_all(&delegations_dir)
            .await
            .map_err(|err| DelegationStoreError::io(&delegations_dir, err))?;

        let mut last_id = None;
        for _ in 0..ID_MAX_ATTEMPTS {
            let id = id_factory();
            let dir = self.delegation_dir(&id);
            match fs::create_dir(&dir).await {
                Ok(()) => {
                    let now = Utc::now();
                    let metadata = DelegationMetadata::new(id, request, now);
                    if let Err(err) = self.initialize_created_delegation(&metadata, now).await {
                        return self.cleanup_failed_create_dir(&dir, err).await;
                    }
                    return Ok(metadata);
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    last_id = Some(id.into_string());
                }
                Err(err) => return Err(DelegationStoreError::io(&dir, err)),
            }
        }

        Err(DelegationStoreError::IdCollisionExhausted {
            max_attempts: ID_MAX_ATTEMPTS,
            last_id: last_id.unwrap_or_else(|| "?".to_string()),
            delegations_dir,
        })
    }

    pub async fn load(
        &self,
        id: &DelegationId,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        let path = self.metadata_path(id);
        read_yaml(&path).await.map_err(|err| match err {
            StorageError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound => {
                DelegationStoreError::NotFound(id.to_string())
            }
            other => other.into(),
        })
    }

    pub async fn list(&self) -> Result<Vec<DelegationSummary>, DelegationStoreError> {
        Ok(self
            .list_page(DEFAULT_LIST_LIMIT)
            .await?
            .summaries
            .into_iter()
            .collect::<Vec<_>>())
    }

    pub async fn list_strict(&self) -> Result<Vec<DelegationSummary>, DelegationStoreError> {
        Ok(self
            .list_page_strict(DEFAULT_LIST_LIMIT)
            .await?
            .summaries
            .into_iter()
            .collect::<Vec<_>>())
    }

    pub async fn list_page(
        &self,
        limit: usize,
    ) -> Result<DelegationListPage, DelegationStoreError> {
        let metadata = self.list_metadata().await?;
        Ok(Self::list_page_from_metadata(metadata, limit))
    }

    pub async fn list_page_strict(
        &self,
        limit: usize,
    ) -> Result<DelegationListPage, DelegationStoreError> {
        let metadata = self.list_metadata_inner(true).await?;
        Ok(Self::list_page_from_metadata(metadata, limit))
    }

    fn list_page_from_metadata(
        metadata: Vec<DelegationMetadata>,
        limit: usize,
    ) -> DelegationListPage {
        let total = metadata.len();
        let limit = limit.max(1);
        let summaries = metadata
            .into_iter()
            .take(limit)
            .map(|item| item.summary())
            .collect::<Vec<_>>();
        DelegationListPage {
            omitted: total.saturating_sub(summaries.len()),
            summaries,
        }
    }

    pub async fn list_metadata(&self) -> Result<Vec<DelegationMetadata>, DelegationStoreError> {
        self.list_metadata_inner(false).await
    }

    async fn list_metadata_inner(
        &self,
        strict: bool,
    ) -> Result<Vec<DelegationMetadata>, DelegationStoreError> {
        let dir = self.delegations_dir();
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(DelegationStoreError::io(&dir, err)),
        };
        let mut out = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|err| DelegationStoreError::io(&dir, err))?
        {
            let metadata_path = entry.path().join(DELEGATION_YAML);
            let metadata = match read_yaml::<DelegationMetadata>(&metadata_path).await {
                Ok(metadata) => metadata,
                Err(err) => {
                    if strict {
                        return Err(err.into());
                    }
                    log::warn!(
                        target: "delegation_store",
                        "跳过无法读取的 delegation metadata path={} err={err:#}",
                        metadata_path.display()
                    );
                    continue;
                }
            };
            out.push(metadata);
        }
        out.sort_by(sort_metadata);
        Ok(out)
    }

    async fn list_metadata_with_failures(
        &self,
    ) -> Result<(Vec<DelegationMetadata>, Vec<String>), DelegationStoreError> {
        let dir = self.delegations_dir();
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok((Vec::new(), Vec::new()));
            }
            Err(err) => return Err(DelegationStoreError::io(&dir, err)),
        };
        let mut out = Vec::new();
        let mut failures = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|err| DelegationStoreError::io(&dir, err))?
        {
            let metadata_path = entry.path().join(DELEGATION_YAML);
            match read_yaml::<DelegationMetadata>(&metadata_path).await {
                Ok(metadata) => out.push(metadata),
                Err(err) => {
                    failures.push(format!("{}: {err}", metadata_path.display()));
                }
            }
        }
        out.sort_by(sort_metadata);
        Ok((out, failures))
    }

    pub async fn read(
        &self,
        id: &DelegationId,
        mode: DelegationReadMode,
    ) -> Result<DelegationRead, DelegationStoreError> {
        let metadata = self.load(id).await?;
        let summary = metadata.summary();
        match mode {
            DelegationReadMode::Summary => {
                let progress = self.read_progress(id).await?.map(bound_progress);
                let compaction_summary = self
                    .read_compaction_state(id)
                    .await?
                    .map(|state| state.summary)
                    .filter(|summary| !summary.trim().is_empty());
                Ok(DelegationRead::Summary {
                    summary,
                    progress,
                    compaction_summary,
                })
            }
            DelegationReadMode::Result => {
                let (result_markdown, truncated) = self
                    .read_optional_text_bounded(&self.result_path(id), READ_TEXT_MAX_CHARS)
                    .await?;
                Ok(DelegationRead::Result {
                    summary,
                    result_markdown,
                    truncated,
                })
            }
            DelegationReadMode::EventsTail { limit } => {
                let events = self.read_events_tail(id, limit).await?;
                Ok(DelegationRead::EventsTail { summary, events })
            }
            DelegationReadMode::TranscriptTail { limit, max_chars } => {
                let (entries, truncated) = self.read_transcript_tail(id, limit, max_chars).await?;
                Ok(DelegationRead::TranscriptTail {
                    summary,
                    entries,
                    truncated,
                })
            }
        }
    }

    pub async fn start(
        &self,
        id: &DelegationId,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        self.transition(id, DelegationStatus::Running, None).await
    }

    pub async fn update_progress(
        &self,
        id: &DelegationId,
        update: DelegationUpdate,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        let _file_guard = self.file_lock_for(id).await?;
        let now = Utc::now();
        let progress = bound_progress(update.clone().progress(now));
        let mut metadata = self.load(id).await?;
        if metadata.status != DelegationStatus::Running {
            return Err(DelegationStoreError::CannotUpdateProgress {
                id: id.clone(),
                status: metadata.status,
            });
        }
        write_text_atomic(
            &self.progress_path(id),
            serde_json::to_string_pretty(&progress)?.as_bytes(),
        )
        .await?;

        metadata.current_step = progress.current_step.clone();
        metadata.progress_summary = Some(progress.summary.clone());
        metadata.updated_at = now;
        self.write_metadata(&metadata).await?;
        self.append_event_unlocked(
            id,
            DelegationEventKind::ProgressUpdated {
                current_step: progress.current_step,
                summary: progress.summary,
                artifacts: progress.artifacts,
            },
            now,
        )
        .await?;
        Ok(metadata)
    }

    pub async fn steer(
        &self,
        id: &DelegationId,
        instruction: String,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        let _file_guard = self.file_lock_for(id).await?;
        let mut metadata = self.load(id).await?;
        if metadata.status.is_terminal() {
            return Err(DelegationStoreError::CannotSteer {
                id: id.clone(),
                status: metadata.status,
            });
        }
        let now = Utc::now();
        metadata.updated_at = now;
        self.write_metadata(&metadata).await?;
        let instruction = super::types::truncate_text(&instruction, SUMMARY_TEXT_LIMIT);
        let event = self
            .append_event_unlocked(
                id,
                DelegationEventKind::Steered {
                    instruction: instruction.clone(),
                },
                now,
            )
            .await?;
        self.append_steering_unlocked(
            id,
            &DelegationSteering {
                seq: event.seq,
                at: event.at,
                instruction,
            },
        )
        .await?;
        Ok(metadata)
    }

    pub async fn complete(
        &self,
        id: &DelegationId,
        result: DelegationResult,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        let _file_guard = self.file_lock_for(id).await?;
        let result = bound_result(result);
        let now = result.completed_at;
        let mut metadata = self.load(id).await?;
        let previous = metadata.status;
        if !result.status.is_terminal() {
            return Err(DelegationStoreError::NonTerminalResult {
                id: id.clone(),
                status: result.status,
            });
        }
        if previous.is_terminal() {
            return Err(DelegationStoreError::CannotTransition {
                id: id.clone(),
                from: previous,
                to: result.status,
            });
        }
        let legal_complete_edge = previous == DelegationStatus::Running
            || (previous == DelegationStatus::Queued
                && result.status != DelegationStatus::Completed);
        if !legal_complete_edge {
            return Err(DelegationStoreError::CannotTransition {
                id: id.clone(),
                from: previous,
                to: result.status,
            });
        }
        metadata.status = result.status;
        metadata.updated_at = now;
        metadata.completed_at = Some(now);
        metadata.progress_summary = Some(super::types::truncate_text(
            &result.summary,
            SUMMARY_TEXT_LIMIT,
        ));
        metadata.error_summary = result.error_summary.clone();
        metadata.result_ref = Some(RESULT_MD.to_string());
        metadata.changed_files = result.changed_files.clone();
        self.write_result_markdown(id, &result).await?;
        self.write_metadata(&metadata).await?;
        self.append_status_change_unlocked(id, previous, metadata.status, now)
            .await?;
        let kind = match result.status {
            DelegationStatus::Completed => DelegationEventKind::Completed {
                summary: result.summary,
                changed_files: result.changed_files,
            },
            DelegationStatus::Failed => DelegationEventKind::Failed {
                error: result
                    .error_summary
                    .unwrap_or_else(|| "subagent failed".to_string()),
            },
            DelegationStatus::Abandoned => DelegationEventKind::Abandoned {
                reason: result
                    .error_summary
                    .unwrap_or_else(|| "subagent abandoned".to_string()),
            },
            DelegationStatus::Queued | DelegationStatus::Running => {
                unreachable!("non-terminal delegation result rejected before event construction")
            }
        };
        self.append_event_unlocked(id, kind, now).await?;
        Ok(metadata)
    }

    pub async fn abandon(
        &self,
        id: &DelegationId,
        reason: String,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        self.transition(id, DelegationStatus::Abandoned, Some(reason))
            .await
    }

    pub async fn abandon_unfinished_for_session(
        &self,
        parent_session_id: &SessionId,
        reason: &str,
    ) -> Result<Vec<DelegationMetadata>, DelegationStoreError> {
        let mut updated = Vec::new();
        let directory_scoped_to_parent = self
            .expected_session_id
            .as_ref()
            .is_some_and(|expected| expected == parent_session_id);
        let (metadata_list, mut failures) = self.list_metadata_with_failures().await?;
        for metadata in metadata_list {
            let belongs_to_parent =
                &metadata.parent_session_id == parent_session_id || directory_scoped_to_parent;
            if belongs_to_parent && !metadata.status.is_terminal() {
                match self.abandon(&metadata.id, reason.to_string()).await {
                    Ok(metadata) if metadata.status == DelegationStatus::Abandoned => {
                        updated.push(metadata);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        log::warn!(
                            target: "delegation_store",
                            "abandon delegation {} failed during session {} cleanup: {error:#}",
                            metadata.id,
                            parent_session_id
                        );
                        failures.push(format!("{}: {error}", metadata.id));
                    }
                }
            }
        }
        if !failures.is_empty() {
            return Err(DelegationStoreError::AbandonIncomplete {
                session_id: parent_session_id.clone(),
                failures: failures.join("; "),
            });
        }
        Ok(updated)
    }

    pub async fn abandon_unfinished_for_session_best_effort(
        &self,
        parent_session_id: &SessionId,
        reason: &str,
    ) -> Vec<DelegationMetadata> {
        let mut updated = Vec::new();
        let directory_scoped_to_parent = self
            .expected_session_id
            .as_ref()
            .is_some_and(|expected| expected == parent_session_id);
        let metadata = match self.list_metadata().await {
            Ok(metadata) => metadata,
            Err(error) => {
                log::warn!(
                    target: "delegation_store",
                    "best-effort abandon failed to list delegations for session {}: {error:#}",
                    parent_session_id
                );
                return updated;
            }
        };
        for metadata in metadata {
            let belongs_to_parent =
                &metadata.parent_session_id == parent_session_id || directory_scoped_to_parent;
            if belongs_to_parent && !metadata.status.is_terminal() {
                match self.abandon(&metadata.id, reason.to_string()).await {
                    Ok(metadata) if metadata.status == DelegationStatus::Abandoned => {
                        updated.push(metadata);
                    }
                    Ok(_) => {}
                    Err(error) => {
                        log::warn!(
                            target: "delegation_store",
                            "best-effort abandon delegation {} failed during session {} cleanup: {error:#}",
                            metadata.id,
                            parent_session_id
                        );
                    }
                }
            }
        }
        updated
    }

    pub async fn read_progress(
        &self,
        id: &DelegationId,
    ) -> Result<Option<DelegationProgress>, DelegationStoreError> {
        let path = self.progress_path(id);
        match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(DelegationStoreError::JsonEncode),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(DelegationStoreError::io(&path, err)),
        }
    }

    pub async fn read_events_tail(
        &self,
        id: &DelegationId,
        limit: usize,
    ) -> Result<Vec<DelegationEvent>, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        self.read_events_tail_unlocked(id, limit).await
    }

    async fn read_events_tail_unlocked(
        &self,
        id: &DelegationId,
        limit: usize,
    ) -> Result<Vec<DelegationEvent>, DelegationStoreError> {
        let path = self.events_path(id);
        let mut file = match fs::File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(DelegationStoreError::io(&path, err)),
        };
        let limit = clamp_event_tail_limit(limit);
        let mut tail = VecDeque::with_capacity(limit);
        let len = file
            .metadata()
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?
            .len();
        let start = len.saturating_sub(EVENT_TAIL_READ_WINDOW_BYTES);
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        let bytes = if start > 0 {
            raw.iter()
                .position(|byte| *byte == b'\n')
                .map(|idx| &raw[idx + 1..])
                .unwrap_or(&[])
        } else {
            &raw[..]
        };
        let text = String::from_utf8_lossy(bytes);
        let trailing_maybe_partial = !text.ends_with('\n');
        let mut lines = text.lines().enumerate().peekable();
        while let Some((idx, line)) = lines.next() {
            if line.trim().is_empty() {
                continue;
            }
            let event = match serde_json::from_str::<DelegationEvent>(line) {
                Ok(event) => event,
                Err(_source) if trailing_maybe_partial && lines.peek().is_none() => {
                    log::warn!(
                        target: "delegation_store",
                        "忽略 delegation events 尾部不完整 JSONL 行 path={} line={}",
                        path.display(),
                        idx + 1
                    );
                    break;
                }
                Err(source) => {
                    return Err(DelegationStoreError::JsonLine {
                        line: idx + 1,
                        source,
                    });
                }
            };
            if tail.len() == limit {
                tail.pop_front();
            }
            tail.push_back(event);
        }
        Ok(tail.into_iter().collect())
    }

    pub async fn read_events(
        &self,
        id: &DelegationId,
    ) -> Result<Vec<DelegationEvent>, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        self.read_events_unlocked(id).await
    }

    pub async fn append_transcript_entry(
        &self,
        id: &DelegationId,
        entry: DelegationTranscriptEntry,
    ) -> Result<(), DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        self.append_jsonl_value(&self.transcript_path(id), &entry)
            .await
    }

    pub async fn read_transcript_entries(
        &self,
        id: &DelegationId,
    ) -> Result<Vec<DelegationTranscriptEntry>, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        self.read_jsonl_values(&self.transcript_path(id)).await
    }

    pub async fn read_transcript_tail(
        &self,
        id: &DelegationId,
        limit: usize,
        max_chars: usize,
    ) -> Result<(Vec<DelegationTranscriptEntry>, bool), DelegationStoreError> {
        let limit = clamp_transcript_tail_limit(limit);
        let max_chars = clamp_transcript_tail_max_chars(max_chars);
        let entries = self.read_transcript_entries(id).await?;
        let mut tail = VecDeque::with_capacity(limit);
        let mut chars = 0usize;
        let mut truncated = false;
        for entry in entries.into_iter().rev() {
            let entry_chars = serde_json::to_string(&entry)?.chars().count();
            if tail.len() >= limit || chars.saturating_add(entry_chars) > max_chars {
                truncated = true;
                break;
            }
            chars = chars.saturating_add(entry_chars);
            tail.push_front(entry);
        }
        Ok((tail.into_iter().collect(), truncated))
    }

    pub async fn read_compaction_state(
        &self,
        id: &DelegationId,
    ) -> Result<Option<DelegationCompactionState>, DelegationStoreError> {
        let path = self.compaction_path(id);
        match fs::read(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(DelegationStoreError::JsonEncode),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(DelegationStoreError::io(&path, err)),
        }
    }

    pub async fn write_compaction_state(
        &self,
        id: &DelegationId,
        state: &DelegationCompactionState,
    ) -> Result<(), DelegationStoreError> {
        write_text_atomic(
            &self.compaction_path(id),
            serde_json::to_string_pretty(state)?.as_bytes(),
        )
        .await
        .map_err(Into::into)
    }

    pub async fn append_compaction_event(
        &self,
        id: &DelegationId,
        kind: DelegationCompactionEventKind,
    ) -> Result<(), DelegationStoreError> {
        let entry = DelegationCompactionEvent {
            at: Utc::now(),
            kind,
        };
        self.append_jsonl_value(&self.compaction_events_path(id), &entry)
            .await
    }

    pub async fn write_compaction_checkpoint(
        &self,
        id: &DelegationId,
        value: &Value,
    ) -> Result<(), DelegationStoreError> {
        write_text_atomic(
            &self.compaction_checkpoint_path(id),
            serde_json::to_string_pretty(value)?.as_bytes(),
        )
        .await
        .map_err(Into::into)
    }

    pub async fn clear_compaction_checkpoint(
        &self,
        id: &DelegationId,
    ) -> Result<(), DelegationStoreError> {
        let path = self.compaction_checkpoint_path(id);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(DelegationStoreError::io(&path, err)),
        }
    }

    pub async fn append_event(
        &self,
        id: &DelegationId,
        kind: DelegationEventKind,
    ) -> Result<(), DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        let _file_guard = self.file_lock_for(id).await?;
        let metadata = self.load(id).await?;
        if metadata.status.is_terminal() {
            return Ok(());
        }
        self.append_event_unlocked(id, kind, Utc::now())
            .await
            .map(|_| ())
    }

    pub async fn read_steering_after(
        &self,
        id: &DelegationId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<DelegationSteering>, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        let path = self.steering_path(id);
        let file = match fs::File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return self
                    .read_steering_from_legacy_events_unlocked(id, after_seq, limit)
                    .await;
            }
            Err(err) => return Err(DelegationStoreError::io(&path, err)),
        };
        let limit = limit.clamp(1, DEFAULT_STEERING_READ_LIMIT);
        let mut out = Vec::with_capacity(limit);
        let mut lines = BufReader::new(file).lines();
        let mut line_no = 0usize;
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?
        {
            line_no += 1;
            if line.trim().is_empty() {
                continue;
            }
            let item = serde_json::from_str::<DelegationSteering>(&line).map_err(|source| {
                DelegationStoreError::JsonLine {
                    line: line_no,
                    source,
                }
            })?;
            if item.seq <= after_seq {
                continue;
            }
            out.push(item);
            if out.len() >= limit {
                break;
            }
        }
        if out.len() < limit {
            self.merge_steering_from_events_unlocked(id, after_seq, limit, &mut out)
                .await?;
            out.sort_by_key(|item| item.seq);
            out.truncate(limit);
        }
        Ok(out)
    }

    async fn read_steering_from_legacy_events_unlocked(
        &self,
        id: &DelegationId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<DelegationSteering>, DelegationStoreError> {
        let limit = limit.clamp(1, DEFAULT_STEERING_READ_LIMIT);
        let mut out = Vec::with_capacity(limit);
        self.merge_steering_from_events_unlocked(id, after_seq, limit, &mut out)
            .await?;
        Ok(out)
    }

    async fn merge_steering_from_events_unlocked(
        &self,
        id: &DelegationId,
        after_seq: u64,
        limit: usize,
        out: &mut Vec<DelegationSteering>,
    ) -> Result<(), DelegationStoreError> {
        let events = self
            .read_events_tail_unlocked(id, DEFAULT_STEERING_READ_LIMIT)
            .await?;
        for event in events {
            if event.seq <= after_seq {
                continue;
            }
            if let DelegationEventKind::Steered { instruction } = event.kind {
                if out.iter().any(|item| item.seq == event.seq) {
                    continue;
                }
                out.push(DelegationSteering {
                    seq: event.seq,
                    at: event.at,
                    instruction,
                });
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn read_events_unlocked(
        &self,
        id: &DelegationId,
    ) -> Result<Vec<DelegationEvent>, DelegationStoreError> {
        let path = self.events_path(id);
        let file = match fs::File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(DelegationStoreError::io(&path, err)),
        };
        let mut lines = BufReader::new(file).lines();
        let mut out = Vec::new();
        let mut line_no = 0usize;
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?
        {
            line_no += 1;
            if line.trim().is_empty() {
                continue;
            }
            let event = serde_json::from_str::<DelegationEvent>(&line).map_err(|source| {
                DelegationStoreError::JsonLine {
                    line: line_no,
                    source,
                }
            })?;
            out.push(event);
        }
        Ok(out)
    }

    async fn transition(
        &self,
        id: &DelegationId,
        status: DelegationStatus,
        reason: Option<String>,
    ) -> Result<DelegationMetadata, DelegationStoreError> {
        let lock = self.lock_for(id)?;
        let _guard = lock.lock().await;
        let _file_guard = self.file_lock_for(id).await?;
        let mut metadata = self.load(id).await?;
        let previous = metadata.status;
        if previous.is_terminal() {
            if status == DelegationStatus::Abandoned {
                return Ok(metadata);
            }
            return Err(DelegationStoreError::CannotTransition {
                id: id.clone(),
                from: previous,
                to: status,
            });
        }
        if previous == status {
            return Ok(metadata);
        }
        let now = Utc::now();
        metadata.status = status;
        metadata.updated_at = now;
        match status {
            DelegationStatus::Running => {
                metadata.started_at.get_or_insert(now);
            }
            DelegationStatus::Completed
            | DelegationStatus::Failed
            | DelegationStatus::Abandoned => {
                metadata.completed_at = Some(now);
            }
            DelegationStatus::Queued => {}
        }
        let reason_text = reason.unwrap_or_else(|| match status {
            DelegationStatus::Failed => "failed".to_string(),
            DelegationStatus::Abandoned => "abandoned".to_string(),
            _ => String::new(),
        });
        if matches!(
            status,
            DelegationStatus::Failed | DelegationStatus::Abandoned
        ) {
            metadata.error_summary = Some(super::types::truncate_text(
                &reason_text,
                SUMMARY_FIELD_LIMIT,
            ));
            metadata.result_ref = Some(RESULT_MD.to_string());
        }
        let progress = if matches!(
            status,
            DelegationStatus::Failed | DelegationStatus::Abandoned
        ) {
            self.read_progress(id).await.ok().flatten()
        } else {
            None
        };
        if let Some(progress) = &progress {
            metadata.progress_summary = Some(progress.summary.clone());
        }
        if matches!(
            status,
            DelegationStatus::Failed | DelegationStatus::Abandoned
        ) {
            let summary = progress.as_ref().map_or_else(
                || format!("{status:?}: {reason_text}"),
                |progress| progress.summary.clone(),
            );
            let artifacts = progress.map_or_else(Vec::new, |progress| progress.artifacts);
            let result = bound_result(DelegationResult {
                status,
                summary,
                changed_files: metadata.changed_files.clone(),
                artifacts,
                error_summary: Some(reason_text.clone()),
                completed_at: now,
            });
            self.write_result_markdown(id, &result).await?;
        }
        self.write_metadata(&metadata).await?;
        self.append_status_change_unlocked(id, previous, status, now)
            .await?;
        match status {
            DelegationStatus::Running => {
                self.append_event_unlocked(id, DelegationEventKind::Started, now)
                    .await?;
            }
            DelegationStatus::Abandoned => {
                self.append_event_unlocked(
                    id,
                    DelegationEventKind::Abandoned {
                        reason: reason_text,
                    },
                    now,
                )
                .await?;
            }
            DelegationStatus::Failed => {
                self.append_event_unlocked(
                    id,
                    DelegationEventKind::Failed { error: reason_text },
                    now,
                )
                .await?;
            }
            DelegationStatus::Completed | DelegationStatus::Queued => {}
        }
        Ok(metadata)
    }

    async fn append_status_change_unlocked(
        &self,
        id: &DelegationId,
        from: DelegationStatus,
        to: DelegationStatus,
        at: DateTime<Utc>,
    ) -> Result<(), DelegationStoreError> {
        if from != to {
            self.append_event_unlocked(id, DelegationEventKind::StatusChanged { from, to }, at)
                .await?;
        }
        Ok(())
    }

    async fn append_event_unlocked(
        &self,
        id: &DelegationId,
        kind: DelegationEventKind,
        at: DateTime<Utc>,
    ) -> Result<DelegationEvent, DelegationStoreError> {
        let path = self.events_path(id);
        self.repair_events_tail_unlocked(id).await?;
        let seq = self.next_event_seq_unlocked(id).await?;
        let event = DelegationEvent {
            seq,
            at,
            kind: bound_event_kind(kind),
        };
        let Some(parent) = path.parent() else {
            return Err(DelegationStoreError::io(
                &path,
                std::io::Error::other("delegation event path 缺少父目录"),
            ));
        };
        fs::create_dir_all(parent)
            .await
            .map_err(|err| DelegationStoreError::io(parent, err))?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        let mut line = serde_json::to_vec(&event)?;
        line.push(b'\n');
        file.write_all(&line)
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        file.sync_data()
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        self.write_event_seq_unlocked(id, seq).await?;
        Ok(event)
    }

    async fn repair_events_tail_unlocked(
        &self,
        id: &DelegationId,
    ) -> Result<(), DelegationStoreError> {
        let path = self.events_path(id);
        let mut file = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(DelegationStoreError::io(&path, err)),
        };
        let len = file
            .metadata()
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?
            .len();
        if len == 0 {
            return Ok(());
        }
        let mut last = [0u8; 1];
        file.seek(SeekFrom::Start(len.saturating_sub(1)))
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        file.read_exact(&mut last)
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        if last[0] == b'\n' {
            return Ok(());
        }

        let tail_start = find_last_newline_offset(&mut file, &path, len)
            .await?
            .map(|offset| offset.saturating_add(1))
            .unwrap_or(0);
        let tail_len = len.saturating_sub(tail_start);
        if tail_len > EVENT_TAIL_REPAIR_MAX_LINE_BYTES {
            truncate_events_tail(&mut file, &path, tail_start).await?;
            log::warn!(
                target: "delegation_store",
                "修复 delegation events 过长尾部 JSONL path={} truncate_to={} tail_len={}",
                path.display(),
                tail_start,
                tail_len
            );
            return Ok(());
        }

        file.seek(SeekFrom::Start(tail_start))
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        let mut tail = Vec::with_capacity(usize::try_from(tail_len).unwrap_or(usize::MAX));
        file.read_to_end(&mut tail)
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        if serde_json::from_slice::<DelegationEvent>(&tail).is_ok() {
            file.seek(SeekFrom::End(0))
                .await
                .map_err(|err| DelegationStoreError::io(&path, err))?;
            file.write_all(b"\n")
                .await
                .map_err(|err| DelegationStoreError::io(&path, err))?;
            file.sync_data()
                .await
                .map_err(|err| DelegationStoreError::io(&path, err))?;
            return Ok(());
        }
        truncate_events_tail(&mut file, &path, tail_start).await?;
        log::warn!(
            target: "delegation_store",
            "修复 delegation events 尾部不完整 JSONL path={} truncate_to={}",
            path.display(),
            tail_start
        );
        Ok(())
    }

    async fn append_steering_unlocked(
        &self,
        id: &DelegationId,
        item: &DelegationSteering,
    ) -> Result<(), DelegationStoreError> {
        let path = self.steering_path(id);
        let Some(parent) = path.parent() else {
            return Err(DelegationStoreError::io(
                &path,
                std::io::Error::other("delegation steering path 缺少父目录"),
            ));
        };
        fs::create_dir_all(parent)
            .await
            .map_err(|err| DelegationStoreError::io(parent, err))?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        let mut line = serde_json::to_vec(item)?;
        line.push(b'\n');
        file.write_all(&line)
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        file.sync_data()
            .await
            .map_err(|err| DelegationStoreError::io(&path, err))?;
        Ok(())
    }

    async fn next_event_seq_unlocked(
        &self,
        id: &DelegationId,
    ) -> Result<u64, DelegationStoreError> {
        let sidecar_seq = self.read_event_seq_unlocked(id).await?;
        let last_event_seq = self.read_last_event_seq_unlocked(id).await?;
        match (sidecar_seq, last_event_seq) {
            (_, Some(last_event_seq)) => {
                if sidecar_seq != Some(last_event_seq) {
                    self.write_event_seq_unlocked(id, last_event_seq).await?;
                }
                Ok(last_event_seq.saturating_add(1))
            }
            (Some(_), None) => Ok(1),
            (None, None) => Ok(1),
        }
    }

    async fn read_event_seq_unlocked(
        &self,
        id: &DelegationId,
    ) -> Result<Option<u64>, DelegationStoreError> {
        let path = self.event_seq_path(id);
        let text = match fs::read_to_string(&path).await {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(DelegationStoreError::io(&path, err)),
        };
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        match trimmed.parse::<u64>() {
            Ok(seq) => Ok(Some(seq)),
            Err(source) => Err(DelegationStoreError::Io {
                path,
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
            }),
        }
    }

    async fn write_event_seq_unlocked(
        &self,
        id: &DelegationId,
        seq: u64,
    ) -> Result<(), DelegationStoreError> {
        write_text_atomic(&self.event_seq_path(id), format!("{seq}\n").as_bytes())
            .await
            .map_err(Into::into)
    }

    async fn read_last_event_seq_unlocked(
        &self,
        id: &DelegationId,
    ) -> Result<Option<u64>, DelegationStoreError> {
        Ok(self
            .read_events_tail_unlocked(id, 1)
            .await?
            .last()
            .map(|event| event.seq))
    }

    fn lock_for(&self, id: &DelegationId) -> Result<Arc<Mutex<()>>, DelegationStoreError> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| DelegationStoreError::LockPoisoned)?;
        Ok(locks
            .entry(id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone())
    }

    async fn file_lock_for(
        &self,
        id: &DelegationId,
    ) -> Result<FileLockGuard, DelegationStoreError> {
        let path = self.delegation_lock_path(id);
        FileLockGuard::lock_exclusive(&path)
            .await
            .map_err(|source| DelegationStoreError::FileLock { path, source })
    }

    async fn write_metadata(
        &self,
        metadata: &DelegationMetadata,
    ) -> Result<(), DelegationStoreError> {
        write_yaml_atomic(&self.metadata_path(&metadata.id), metadata)
            .await
            .map_err(Into::into)
    }

    async fn initialize_created_delegation(
        &self,
        metadata: &DelegationMetadata,
        now: DateTime<Utc>,
    ) -> Result<(), DelegationStoreError> {
        self.write_metadata(metadata).await?;
        #[cfg(test)]
        if self.inject_create_failure_after_metadata {
            return Err(DelegationStoreError::io(
                &self.delegation_dir(&metadata.id),
                std::io::Error::other("injected create failure after metadata"),
            ));
        }
        self.append_event_unlocked(&metadata.id, DelegationEventKind::Created, now)
            .await?;
        self.append_event_unlocked(&metadata.id, DelegationEventKind::Queued, now)
            .await?;
        Ok(())
    }

    async fn cleanup_failed_create_dir<T>(
        &self,
        dir: &Path,
        source_error: DelegationStoreError,
    ) -> Result<T, DelegationStoreError> {
        if let Err(cleanup_error) = fs::remove_dir_all(dir).await {
            log::warn!(
                target: "delegation_store",
                "delegation create 失败后清理目录失败 path={} err={cleanup_error:#}",
                dir.display()
            );
        }
        Err(source_error)
    }

    async fn write_result_markdown(
        &self,
        id: &DelegationId,
        result: &DelegationResult,
    ) -> Result<(), DelegationStoreError> {
        let mut text = String::new();
        text.push_str("# Subagent Result\n\n");
        text.push_str(&format!("status: {:?}\n\n", result.status));
        text.push_str(&result.summary);
        text.push('\n');
        if !result.changed_files.is_empty() {
            text.push_str("\nchanged_files:\n");
            for path in &result.changed_files {
                text.push_str("- ");
                text.push_str(path);
                text.push('\n');
            }
        }
        if !result.artifacts.is_empty() {
            text.push_str("\nartifacts:\n");
            for artifact in &result.artifacts {
                text.push_str("- ");
                text.push_str(&artifact.path);
                if let Some(description) = &artifact.description {
                    text.push_str(": ");
                    text.push_str(description);
                }
                text.push('\n');
            }
        }
        if let Some(error) = &result.error_summary {
            text.push_str("\nerror_summary:\n");
            text.push_str(error);
            text.push('\n');
        }
        write_text_atomic(&self.result_path(id), text.as_bytes())
            .await
            .map_err(Into::into)
    }

    async fn read_optional_text_bounded(
        &self,
        path: &Path,
        max_chars: usize,
    ) -> Result<(Option<String>, bool), DelegationStoreError> {
        match self.read_text_bounded(path, max_chars).await {
            Ok((text, truncated)) => Ok((Some(text), truncated)),
            Err(DelegationStoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok((None, false))
            }
            Err(err) => Err(err),
        }
    }

    async fn read_text_bounded(
        &self,
        path: &Path,
        max_chars: usize,
    ) -> Result<(String, bool), DelegationStoreError> {
        let file = fs::File::open(path)
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?;
        let byte_limit = max_chars.saturating_mul(4).saturating_add(16).max(16);
        let mut reader = file.take(u64::try_from(byte_limit).unwrap_or(u64::MAX));
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?;
        let file_truncated = fs::metadata(path)
            .await
            .map(|metadata| metadata.len() > u64::try_from(bytes.len()).unwrap_or(u64::MAX))
            .unwrap_or(false);
        let text = String::from_utf8_lossy(&bytes);
        let (text, text_truncated) = truncate_text_with_flag(&text, max_chars);
        Ok((text, file_truncated || text_truncated))
    }

    async fn append_jsonl_value<T: serde::Serialize>(
        &self,
        path: &Path,
        value: &T,
    ) -> Result<(), DelegationStoreError> {
        let Some(parent) = path.parent() else {
            return Err(DelegationStoreError::io(
                path,
                std::io::Error::other("delegation JSON path 缺少父目录"),
            ));
        };
        fs::create_dir_all(parent)
            .await
            .map_err(|err| DelegationStoreError::io(parent, err))?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?;
        let mut line = serde_json::to_vec(value)?;
        line.push(b'\n');
        file.write_all(&line)
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?;
        file.sync_data()
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?;
        Ok(())
    }

    async fn read_jsonl_values<T: serde::de::DeserializeOwned>(
        &self,
        path: &Path,
    ) -> Result<Vec<T>, DelegationStoreError> {
        let file = match fs::File::open(path).await {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(DelegationStoreError::io(path, err)),
        };
        let mut out = Vec::new();
        let mut line_no = 0usize;
        let mut lines = BufReader::new(file).lines();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?
        {
            line_no = line_no.saturating_add(1);
            if line.trim().is_empty() {
                continue;
            }
            out.push(serde_json::from_str::<T>(&line).map_err(|source| {
                DelegationStoreError::JsonLine {
                    line: line_no,
                    source,
                }
            })?);
        }
        Ok(out)
    }

    fn metadata_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(DELEGATION_YAML)
    }

    fn delegation_lock_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(DELEGATION_LOCK)
    }

    fn events_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(EVENTS_JSONL)
    }

    fn steering_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(STEERING_JSONL)
    }

    fn event_seq_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(EVENTS_SEQ)
    }

    fn progress_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(PROGRESS_JSON)
    }

    fn result_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(RESULT_MD)
    }

    fn transcript_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(TRANSCRIPT_JSONL)
    }

    fn compaction_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(COMPACTION_JSON)
    }

    fn compaction_events_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(COMPACTION_EVENTS_JSONL)
    }

    fn compaction_checkpoint_path(&self, id: &DelegationId) -> PathBuf {
        self.delegation_dir(id).join(COMPACTION_CHECKPOINT_JSON)
    }

    fn ensure_parent_session_matches(
        &self,
        actual: &SessionId,
    ) -> Result<(), DelegationStoreError> {
        let Some(expected) = &self.expected_session_id else {
            return Ok(());
        };
        if expected == actual {
            return Ok(());
        }
        Err(DelegationStoreError::ParentSessionMismatch {
            expected: expected.clone(),
            actual: actual.clone(),
        })
    }
}

async fn find_last_newline_offset(
    file: &mut fs::File,
    path: &Path,
    len: u64,
) -> Result<Option<u64>, DelegationStoreError> {
    let mut remaining = len;
    while remaining > 0 {
        let read_len = remaining.min(EVENT_TAIL_REPAIR_CHUNK_BYTES);
        let start = remaining.saturating_sub(read_len);
        let mut chunk = vec![0u8; usize::try_from(read_len).unwrap_or(usize::MAX)];
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?;
        file.read_exact(&mut chunk)
            .await
            .map_err(|err| DelegationStoreError::io(path, err))?;
        if let Some(idx) = chunk.iter().rposition(|byte| *byte == b'\n') {
            return Ok(Some(start.saturating_add(u64::try_from(idx).unwrap_or(0))));
        }
        remaining = start;
    }
    Ok(None)
}

async fn truncate_events_tail(
    file: &mut fs::File,
    path: &Path,
    truncate_to: u64,
) -> Result<(), DelegationStoreError> {
    file.set_len(truncate_to)
        .await
        .map_err(|err| DelegationStoreError::io(path, err))?;
    file.sync_data()
        .await
        .map_err(|err| DelegationStoreError::io(path, err))
}

fn sort_metadata(a: &DelegationMetadata, b: &DelegationMetadata) -> Ordering {
    a.status
        .sort_rank()
        .cmp(&b.status.sort_rank())
        .then_with(|| b.created_at.cmp(&a.created_at))
        .then_with(|| a.id.cmp(&b.id))
}

pub fn read_mode_from_json(input: &Value) -> Result<DelegationReadMode, DelegationStoreError> {
    let mode = input
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("summary");
    match mode {
        "summary" => {
            reject_unexpected_read_fields(input, &["id", "mode", "limit"])?;
            Ok(DelegationReadMode::Summary)
        }
        "result" => {
            reject_unexpected_read_fields(input, &["id", "mode", "limit"])?;
            Ok(DelegationReadMode::Result)
        }
        "events_tail" => {
            reject_unexpected_read_fields(input, &["id", "mode", "limit"])?;
            let limit = match input.get("limit") {
                Some(value) => {
                    let Some(limit) = value.as_u64().and_then(|value| usize::try_from(value).ok())
                    else {
                        return Err(DelegationStoreError::InvalidReadMode(
                            "events_tail.limit 必须是正整数".to_string(),
                        ));
                    };
                    if limit == 0 {
                        return Err(DelegationStoreError::InvalidReadMode(
                            "events_tail.limit 必须大于 0".to_string(),
                        ));
                    }
                    limit
                }
                None => default_event_tail_limit(),
            };
            Ok(DelegationReadMode::EventsTail {
                limit: clamp_event_tail_limit(limit),
            })
        }
        "transcript_tail" => {
            reject_unexpected_read_fields(input, &["id", "mode", "limit", "max_chars"])?;
            let limit = read_positive_usize_field(input, "limit", "transcript_tail.limit")?
                .unwrap_or_else(default_transcript_tail_limit);
            let max_chars =
                read_positive_usize_field(input, "max_chars", "transcript_tail.max_chars")?
                    .unwrap_or_else(default_transcript_tail_max_chars);
            Ok(DelegationReadMode::TranscriptTail {
                limit: clamp_transcript_tail_limit(limit),
                max_chars: clamp_transcript_tail_max_chars(max_chars),
            })
        }
        other => Err(DelegationStoreError::InvalidReadMode(format!(
            "未知 mode: {other}"
        ))),
    }
}

fn read_positive_usize_field(
    input: &Value,
    field: &str,
    label: &str,
) -> Result<Option<usize>, DelegationStoreError> {
    let Some(value) = input.get(field) else {
        return Ok(None);
    };
    let Some(parsed) = value.as_u64().and_then(|value| usize::try_from(value).ok()) else {
        return Err(DelegationStoreError::InvalidReadMode(format!(
            "{label} 必须是正整数"
        )));
    };
    if parsed == 0 {
        return Err(DelegationStoreError::InvalidReadMode(format!(
            "{label} 必须大于 0"
        )));
    }
    Ok(Some(parsed))
}

fn reject_unexpected_read_fields(
    input: &Value,
    allowed: &[&str],
) -> Result<(), DelegationStoreError> {
    let Some(object) = input.as_object() else {
        return Ok(());
    };
    for key in object.keys() {
        if !allowed.iter().any(|allowed| *allowed == key) {
            return Err(DelegationStoreError::InvalidReadMode(format!(
                "{key} 不能用于当前 read_subagent mode"
            )));
        }
    }
    Ok(())
}

fn bound_progress(mut progress: DelegationProgress) -> DelegationProgress {
    progress.current_step = progress
        .current_step
        .as_deref()
        .map(|value| super::types::truncate_text(value, SUMMARY_FIELD_LIMIT));
    progress.summary = super::types::truncate_text(&progress.summary, SUMMARY_TEXT_LIMIT);
    progress.artifacts.truncate(8);
    for artifact in &mut progress.artifacts {
        artifact.path = super::types::truncate_text(&artifact.path, SUMMARY_FIELD_LIMIT);
        artifact.description = artifact
            .description
            .as_deref()
            .map(|value| super::types::truncate_text(value, SUMMARY_FIELD_LIMIT));
    }
    progress
}

fn bound_artifacts(artifacts: &mut Vec<DelegationArtifactRef>) {
    artifacts.truncate(SUMMARY_CHANGED_FILES_LIMIT);
    for artifact in artifacts {
        artifact.path = super::types::truncate_text(&artifact.path, SUMMARY_FIELD_LIMIT);
        artifact.description = artifact
            .description
            .as_deref()
            .map(|value| super::types::truncate_text(value, SUMMARY_FIELD_LIMIT));
    }
}

fn bound_changed_files(changed_files: &mut Vec<String>) {
    changed_files.truncate(SUMMARY_CHANGED_FILES_LIMIT);
    for path in changed_files {
        *path = super::types::truncate_text(path, SUMMARY_CHANGED_FILE_LIMIT);
    }
}

fn bound_result(mut result: DelegationResult) -> DelegationResult {
    result.summary = super::types::truncate_text(&result.summary, READ_TEXT_MAX_CHARS);
    result.error_summary = result
        .error_summary
        .as_deref()
        .map(|value| super::types::truncate_text(value, SUMMARY_FIELD_LIMIT));
    bound_changed_files(&mut result.changed_files);
    bound_artifacts(&mut result.artifacts);
    result
}

fn bound_event_kind(kind: DelegationEventKind) -> DelegationEventKind {
    match kind {
        DelegationEventKind::ProgressUpdated {
            current_step,
            summary,
            mut artifacts,
        } => {
            bound_artifacts(&mut artifacts);
            DelegationEventKind::ProgressUpdated {
                current_step: current_step
                    .as_deref()
                    .map(|value| super::types::truncate_text(value, SUMMARY_FIELD_LIMIT)),
                summary: super::types::truncate_text(&summary, SUMMARY_TEXT_LIMIT),
                artifacts,
            }
        }
        DelegationEventKind::Steered { instruction } => DelegationEventKind::Steered {
            instruction: super::types::truncate_text(&instruction, SUMMARY_TEXT_LIMIT),
        },
        DelegationEventKind::ToolStarted { tool_name, summary } => {
            DelegationEventKind::ToolStarted {
                tool_name: super::types::truncate_text(&tool_name, SUMMARY_FIELD_LIMIT),
                summary: super::types::truncate_text(&summary, SUMMARY_TEXT_LIMIT),
            }
        }
        DelegationEventKind::ToolCompleted {
            tool_name,
            summary,
            outcome,
        } => DelegationEventKind::ToolCompleted {
            tool_name: super::types::truncate_text(&tool_name, SUMMARY_FIELD_LIMIT),
            summary: super::types::truncate_text(&summary, SUMMARY_TEXT_LIMIT),
            outcome,
        },
        DelegationEventKind::Completed {
            summary,
            mut changed_files,
        } => {
            bound_changed_files(&mut changed_files);
            DelegationEventKind::Completed {
                summary: super::types::truncate_text(&summary, SUMMARY_TEXT_LIMIT),
                changed_files,
            }
        }
        DelegationEventKind::Failed { error } => DelegationEventKind::Failed {
            error: super::types::truncate_text(&error, SUMMARY_FIELD_LIMIT),
        },
        DelegationEventKind::CompactionFailed { error } => DelegationEventKind::CompactionFailed {
            error: super::types::truncate_text(&error, SUMMARY_FIELD_LIMIT),
        },
        DelegationEventKind::Abandoned { reason } => DelegationEventKind::Abandoned {
            reason: super::types::truncate_text(&reason, SUMMARY_FIELD_LIMIT),
        },
        DelegationEventKind::Created
        | DelegationEventKind::Queued
        | DelegationEventKind::Started
        | DelegationEventKind::StatusChanged { .. } => kind,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::str::FromStr;

    use crate::claim::{AgentId, SessionId};
    use crate::delegation::{
        DelegationArtifactRef, DelegationTranscriptKind, DelegationTranscriptMessageSource,
    };
    use chrono::TimeZone;
    use tokio::io::AsyncWriteExt as _;

    use super::*;

    fn request() -> DelegationCreateRequest {
        DelegationCreateRequest {
            parent_session_id: SessionId::from_str("session_aaaaaaaa").expect("valid session id"),
            parent_turn_id: "turn-1".into(),
            owner_agent_id: AgentId::new("agent-a").expect("valid agent id"),
            title: "查代码路径".into(),
            role: "code explorer".into(),
            objective: "找出 session 存储路径".into(),
            constraints: vec!["只读摘要".into()],
        }
    }

    #[tokio::test]
    async fn create_list_and_read_summary_roundtrip() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store
            .create_with_id_factory(request(), || {
                DelegationId::from_str("subagent_11111111").expect("valid delegation id")
            })
            .await
            .expect("create delegation");

        assert_eq!(metadata.status, DelegationStatus::Queued);
        assert!(store
            .delegation_dir(&metadata.id)
            .join(DELEGATION_YAML)
            .exists());
        let list = store.list().await.expect("list delegations");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, metadata.id);
        assert_eq!(list[0].created_at, metadata.created_at);
        assert_eq!(list[0].updated_at, metadata.updated_at);
        assert_eq!(list[0].started_at, None);
        assert_eq!(list[0].completed_at, None);

        let read = store
            .read(&metadata.id, DelegationReadMode::Summary)
            .await
            .expect("read summary");
        match read {
            DelegationRead::Summary {
                summary,
                progress,
                compaction_summary,
            } => {
                assert_eq!(summary.title, "查代码路径");
                assert_eq!(summary.created_at, metadata.created_at);
                assert_eq!(summary.updated_at, metadata.updated_at);
                assert_eq!(summary.started_at, metadata.started_at);
                assert_eq!(summary.completed_at, metadata.completed_at);
                assert!(progress.is_none());
                assert!(compaction_summary.is_none());
            }
            other => panic!("unexpected read mode: {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_sorts_by_status_then_newer_created_at() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let ids = [
            "subagent_11111111",
            "subagent_22222222",
            "subagent_33333333",
            "subagent_44444444",
        ];
        for id in ids {
            store
                .create_with_id_factory(request(), || {
                    DelegationId::from_str(id).expect("valid delegation id")
                })
                .await
                .expect("create delegation");
        }

        let base = Utc.with_ymd_and_hms(2026, 7, 9, 0, 0, 0).unwrap();
        let updates = [
            ("subagent_11111111", DelegationStatus::Completed, 30),
            ("subagent_22222222", DelegationStatus::Running, 10),
            ("subagent_33333333", DelegationStatus::Running, 20),
            ("subagent_44444444", DelegationStatus::Queued, 40),
        ];
        for (id, status, seconds) in updates {
            let id = DelegationId::from_str(id).expect("valid delegation id");
            let mut metadata = store.load(&id).await.expect("load metadata");
            metadata.status = status;
            metadata.created_at = base + chrono::Duration::seconds(seconds);
            metadata.updated_at = base + chrono::Duration::seconds(100 - seconds);
            store
                .write_metadata(&metadata)
                .await
                .expect("write metadata");
        }

        let list = store.list().await.expect("list delegations");
        let ids = list
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            vec![
                "subagent_33333333",
                "subagent_22222222",
                "subagent_44444444",
                "subagent_11111111"
            ]
        );
    }

    #[tokio::test]
    async fn create_rejects_parent_session_mismatch() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let mut request = request();
        request.parent_session_id =
            SessionId::from_str("session_bbbbbbbb").expect("valid session id");

        let err = store
            .create_with_id_factory(request, || {
                DelegationId::from_str("subagent_11111111").expect("valid delegation id")
            })
            .await
            .expect_err("mismatched parent session should fail");

        assert!(matches!(
            err,
            DelegationStoreError::ParentSessionMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn list_page_reports_omitted_count() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        for idx in 0..3usize {
            store
                .create_with_id_factory(request(), move || {
                    DelegationId::from_str(&format!("subagent_{idx:08x}"))
                        .expect("valid delegation id")
                })
                .await
                .expect("create delegation");
        }

        let page = store.list_page(2).await.expect("list page");

        assert_eq!(page.summaries.len(), 2);
        assert_eq!(page.omitted, 1);
    }

    #[tokio::test]
    async fn strict_list_reports_corrupt_metadata_instead_of_hiding_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        let corrupt_dir = store.delegations_dir().join("subagent_badbadbad");
        fs::create_dir_all(&corrupt_dir).await.expect("corrupt dir");
        fs::write(corrupt_dir.join(DELEGATION_YAML), "{not yaml")
            .await
            .expect("corrupt metadata");

        let loose = store.list().await.expect("loose list");
        assert_eq!(loose.len(), 1);
        assert_eq!(loose[0].id, metadata.id);
        let err = store
            .list_strict()
            .await
            .expect_err("strict list should surface corrupt metadata");
        let err_text = err.to_string();
        assert!(
            err_text.contains("YAML") || err_text.contains("yaml"),
            "unexpected strict list error: {err_text}"
        );
    }

    #[tokio::test]
    async fn progress_and_failed_state_remain_readable() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start delegation");
        store
            .update_progress(
                &metadata.id,
                DelegationUpdate {
                    current_step: Some("reading files".into()),
                    summary: "found session paths".into(),
                    artifacts: Vec::new(),
                },
            )
            .await
            .expect("update progress");
        store
            .complete(
                &metadata.id,
                DelegationResult {
                    status: DelegationStatus::Failed,
                    summary: "partial result exists".into(),
                    changed_files: Vec::new(),
                    artifacts: Vec::new(),
                    error_summary: Some("tool failed".into()),
                    completed_at: Utc::now(),
                },
            )
            .await
            .expect("complete failed");

        let read = store
            .read(&metadata.id, DelegationReadMode::Summary)
            .await
            .expect("read failed summary");
        match read {
            DelegationRead::Summary {
                summary, progress, ..
            } => {
                assert_eq!(summary.status, DelegationStatus::Failed);
                assert_eq!(summary.error_summary.as_deref(), Some("tool failed"));
                assert_eq!(
                    progress.expect("progress").current_step.as_deref(),
                    Some("reading files")
                );
            }
            other => panic!("unexpected read mode: {other:?}"),
        }
    }

    #[tokio::test]
    async fn events_tail_is_bounded() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        for idx in 0..5 {
            store
                .steer(&metadata.id, format!("step {idx}"))
                .await
                .expect("steer delegation");
        }

        let events = store
            .read_events_tail(&metadata.id, 3)
            .await
            .expect("read events tail");
        assert_eq!(events.len(), 3);
        assert!(events
            .iter()
            .all(|event| matches!(event.kind, DelegationEventKind::Steered { .. })));
    }

    #[tokio::test]
    async fn events_tail_preserves_typed_tool_outcomes_and_reads_legacy_events() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        let process_outcome = crate::api::ToolExecutionOutcome::ProcessExit {
            exit_code: Some(9),
            success: false,
        };
        let terminated_outcome =
            crate::api::ToolExecutionOutcome::ProcessTerminated { signal: Some(9) };
        let http_outcome = crate::api::ToolExecutionOutcome::HttpResponse { http_status: 404 };
        for (tool_name, outcome) in [
            ("code_run", process_outcome),
            ("write_stdin", terminated_outcome),
            ("web_request", http_outcome),
        ] {
            store
                .append_event(
                    &metadata.id,
                    DelegationEventKind::ToolCompleted {
                        tool_name: tool_name.into(),
                        summary: "completed with protocol status".into(),
                        outcome: Some(outcome),
                    },
                )
                .await
                .expect("append typed tool event");
        }

        let path = store.events_path(&metadata.id);
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .expect("open events for legacy fixture");
        let mut legacy_line = serde_json::to_vec(&serde_json::json!({
            "seq": 5,
            "at": Utc::now(),
            "type": "tool_completed",
            "tool_name": "legacy_tool",
            "summary": "legacy event without typed outcome"
        }))
        .expect("serialize legacy event");
        legacy_line.push(b'\n');
        file.write_all(&legacy_line)
            .await
            .expect("append legacy event");
        file.sync_data().await.expect("sync legacy event");
        drop(file);

        let read = store
            .read(&metadata.id, DelegationReadMode::EventsTail { limit: 4 })
            .await
            .expect("read events tail");
        let DelegationRead::EventsTail { events, .. } = read else {
            panic!("expected events tail");
        };
        assert_eq!(events.len(), 4);
        let outcomes = events
            .iter()
            .map(|event| match &event.kind {
                DelegationEventKind::ToolCompleted { outcome, .. } => *outcome,
                other => panic!("unexpected event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes,
            vec![
                Some(process_outcome),
                Some(terminated_outcome),
                Some(http_outcome),
                None
            ]
        );
        let legacy_roundtrip = serde_json::to_value(&events[3]).expect("serialize legacy event");
        assert!(legacy_roundtrip.get("outcome").is_none());
    }

    #[tokio::test]
    async fn append_event_repairs_partial_jsonl_tail_before_writing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        let path = store.events_path(&metadata.id);
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .expect("open events");
        file.write_all(br#"{"seq":999,"#)
            .await
            .expect("write partial json");
        file.sync_data().await.expect("sync partial json");
        drop(file);

        store
            .append_event(
                &metadata.id,
                DelegationEventKind::ToolStarted {
                    tool_name: "file_read".into(),
                    summary: "reading".into(),
                },
            )
            .await
            .expect("append after partial tail");

        let events = store.read_events(&metadata.id).await.expect("read events");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[1].seq, 2);
        assert_eq!(events[2].seq, 3);
        assert!(matches!(
            events[2].kind,
            DelegationEventKind::ToolStarted { .. }
        ));
    }

    #[tokio::test]
    async fn append_event_repairs_valid_jsonl_tail_without_newline() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        let path = store.events_path(&metadata.id);
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .await
            .expect("open events");
        let len = file.metadata().await.expect("events metadata").len();
        file.set_len(len.saturating_sub(1))
            .await
            .expect("drop trailing newline");
        file.sync_data().await.expect("sync truncated newline");
        drop(file);

        store
            .append_event(
                &metadata.id,
                DelegationEventKind::ToolCompleted {
                    tool_name: "file_read".into(),
                    summary: "done".into(),
                    outcome: Some(crate::api::ToolExecutionOutcome::Completed),
                },
            )
            .await
            .expect("append after valid no-newline tail");

        let events = store.read_events(&metadata.id).await.expect("read events");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].seq, 1);
        assert_eq!(events[1].seq, 2);
        assert_eq!(events[2].seq, 3);
        assert!(matches!(
            events[2].kind,
            DelegationEventKind::ToolCompleted { .. }
        ));
    }

    #[tokio::test]
    async fn terminal_delegation_rejects_steer() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store
            .abandon(&metadata.id, "session closed".into())
            .await
            .expect("abandon delegation");
        let err = store
            .steer(&metadata.id, "late".into())
            .await
            .expect_err("terminal delegation rejects steering");
        assert!(matches!(err, DelegationStoreError::CannotSteer { .. }));
    }

    #[tokio::test]
    async fn create_cleans_new_delegation_dir_when_sidecar_write_fails() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"))
            .with_create_failure_after_metadata();
        let id = DelegationId::from_str("subagent_11111111").expect("valid id");
        let err = store
            .create_with_id_factory(request(), || id.clone())
            .await
            .expect_err("injected create failure should surface");

        assert!(err.to_string().contains("injected create failure"));
        assert!(
            !store.delegation_dir(&id).exists(),
            "failed create should remove the just-created delegation dir"
        );
    }

    #[tokio::test]
    async fn hard_abandon_reports_unreadable_metadata() {
        let dir = tempfile::tempdir().expect("temp dir");
        let session_id = SessionId::from_str("session_aaaaaaaa").expect("valid session id");
        let store = DelegationStore::new_for_session(
            dir.path().join("sessions/session_aaaaaaaa"),
            session_id.clone(),
        );
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start delegation");
        let corrupt_dir = store.delegations_dir().join("subagent_badbadbad");
        fs::create_dir_all(&corrupt_dir).await.expect("corrupt dir");
        fs::write(corrupt_dir.join(DELEGATION_YAML), "{not yaml")
            .await
            .expect("corrupt metadata");

        let err = store
            .abandon_unfinished_for_session(&session_id, "session finalizing")
            .await
            .expect_err("hard abandon should surface unreadable metadata");

        assert!(err.to_string().contains("subagent"));
        let metadata = store
            .load(&metadata.id)
            .await
            .expect("load readable metadata");
        assert_eq!(metadata.status, DelegationStatus::Abandoned);
    }

    #[tokio::test]
    async fn best_effort_abandon_skips_corrupt_metadata_and_abandons_readable() {
        let dir = tempfile::tempdir().expect("temp dir");
        let session_id = SessionId::from_str("session_aaaaaaaa").expect("valid session id");
        let store = DelegationStore::new_for_session(
            dir.path().join("sessions/session_aaaaaaaa"),
            session_id.clone(),
        );
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start delegation");
        let corrupt_dir = store.delegations_dir().join("subagent_badbadbad");
        fs::create_dir_all(&corrupt_dir).await.expect("corrupt dir");
        fs::write(corrupt_dir.join(DELEGATION_YAML), "{not yaml")
            .await
            .expect("corrupt metadata");

        let updated = store
            .abandon_unfinished_for_session_best_effort(&session_id, "session restored")
            .await;

        assert_eq!(updated.len(), 1);
        let metadata = store.load(&metadata.id).await.expect("load metadata");
        assert_eq!(metadata.status, DelegationStatus::Abandoned);
    }

    #[tokio::test]
    async fn terminal_delegation_rejects_late_progress_and_complete() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start delegation");
        store
            .abandon(&metadata.id, "session closed".into())
            .await
            .expect("abandon delegation");

        let progress_err = store
            .update_progress(
                &metadata.id,
                DelegationUpdate {
                    current_step: Some("late".into()),
                    summary: "late progress".into(),
                    artifacts: Vec::new(),
                },
            )
            .await
            .expect_err("terminal progress should fail");
        assert!(matches!(
            progress_err,
            DelegationStoreError::CannotUpdateProgress { .. }
        ));

        let complete_err = store
            .complete(
                &metadata.id,
                DelegationResult {
                    status: DelegationStatus::Completed,
                    summary: "late completion".into(),
                    changed_files: Vec::new(),
                    artifacts: Vec::new(),
                    error_summary: None,
                    completed_at: Utc::now(),
                },
            )
            .await
            .expect_err("terminal complete should fail");
        assert!(matches!(
            complete_err,
            DelegationStoreError::CannotTransition { .. }
        ));
        let metadata = store.load(&metadata.id).await.expect("load");
        assert_eq!(metadata.status, DelegationStatus::Abandoned);
        assert_eq!(metadata.progress_summary.as_deref(), None);
    }

    #[tokio::test]
    async fn terminal_delegation_ignores_late_public_events() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start delegation");
        store
            .abandon(&metadata.id, "session closed".into())
            .await
            .expect("abandon delegation");
        let before = store
            .read_events(&metadata.id)
            .await
            .expect("events before")
            .len();

        store
            .append_event(
                &metadata.id,
                DelegationEventKind::ToolCompleted {
                    tool_name: "file_read".into(),
                    summary: "late".into(),
                    outcome: Some(crate::api::ToolExecutionOutcome::Completed),
                },
            )
            .await
            .expect("late public event is ignored");

        let after = store
            .read_events(&metadata.id)
            .await
            .expect("events after")
            .len();
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn complete_rejects_non_terminal_result_status() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start delegation");

        let err = store
            .complete(
                &metadata.id,
                DelegationResult {
                    status: DelegationStatus::Running,
                    summary: "not terminal".into(),
                    changed_files: Vec::new(),
                    artifacts: Vec::new(),
                    error_summary: None,
                    completed_at: Utc::now(),
                },
            )
            .await
            .expect_err("non-terminal complete should fail");

        assert!(matches!(
            err,
            DelegationStoreError::NonTerminalResult { .. }
        ));
        let metadata = store.load(&metadata.id).await.expect("load");
        assert_eq!(metadata.status, DelegationStatus::Running);
        assert!(metadata.result_ref.is_none());
    }

    #[tokio::test]
    async fn queued_delegation_rejects_progress_and_completed_result() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");

        let progress_err = store
            .update_progress(
                &metadata.id,
                DelegationUpdate {
                    current_step: Some("too early".into()),
                    summary: "queued progress".into(),
                    artifacts: Vec::new(),
                },
            )
            .await
            .expect_err("queued progress should fail");
        assert!(matches!(
            progress_err,
            DelegationStoreError::CannotUpdateProgress { .. }
        ));

        let complete_err = store
            .complete(
                &metadata.id,
                DelegationResult {
                    status: DelegationStatus::Completed,
                    summary: "not started".into(),
                    changed_files: Vec::new(),
                    artifacts: Vec::new(),
                    error_summary: None,
                    completed_at: Utc::now(),
                },
            )
            .await
            .expect_err("queued completed result should fail");
        assert!(matches!(
            complete_err,
            DelegationStoreError::CannotTransition { .. }
        ));
    }

    #[tokio::test]
    async fn concurrent_event_appends_keep_unique_sequence_numbers() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        let mut handles = Vec::new();
        for idx in 0..20usize {
            let store = store.clone();
            let id = metadata.id.clone();
            handles.push(tokio::spawn(async move {
                store
                    .steer(&id, format!("steer {idx}"))
                    .await
                    .expect("steer")
            }));
        }
        for handle in handles {
            handle.await.expect("join");
        }

        let events = store.read_events(&metadata.id).await.expect("events");
        let seqs = events.iter().map(|event| event.seq).collect::<Vec<_>>();
        let unique = seqs.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), seqs.len());
        assert_eq!(
            seqs,
            (1..=u64::try_from(seqs.len()).unwrap()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn event_seq_sidecar_is_repaired_from_events_tail() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store
            .steer(&metadata.id, "first".into())
            .await
            .expect("first steer");
        store
            .write_event_seq_unlocked(&metadata.id, 99)
            .await
            .expect("write stale sidecar");
        store
            .steer(&metadata.id, "second".into())
            .await
            .expect("second steer");

        let events = store.read_events(&metadata.id).await.expect("events");
        let seqs = events.iter().map(|event| event.seq).collect::<Vec<_>>();
        assert_eq!(
            seqs,
            (1..=u64::try_from(seqs.len()).unwrap()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn steering_read_is_bounded_and_uses_sequence_cursor() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        for idx in 0..80usize {
            store
                .steer(&metadata.id, format!("steer {idx}"))
                .await
                .expect("steer");
        }

        let first = store
            .read_steering_after(&metadata.id, 0, 5)
            .await
            .expect("first steering batch");
        assert_eq!(first.len(), 5);
        assert_eq!(first[0].instruction, "steer 0");
        let last_seq = first.last().expect("last steering").seq;
        let second = store
            .read_steering_after(&metadata.id, last_seq, 5)
            .await
            .expect("second steering batch");
        assert_eq!(second.len(), 5);
        assert_eq!(second[0].instruction, "steer 5");
    }

    #[tokio::test]
    async fn steering_read_falls_back_to_legacy_events_tail() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        for idx in 0..5usize {
            store
                .steer(&metadata.id, format!("legacy steer {idx}"))
                .await
                .expect("steer");
        }
        fs::remove_file(store.steering_path(&metadata.id))
            .await
            .expect("remove steering sidecar");

        let first = store
            .read_steering_after(&metadata.id, 0, 3)
            .await
            .expect("legacy steering batch");

        assert_eq!(first.len(), 3);
        assert_eq!(first[0].instruction, "legacy steer 0");
    }

    #[tokio::test]
    async fn read_result_and_summary_are_bounded() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start");
        let changed_files = (0..20usize)
            .map(|idx| format!("src/generated/{idx}-{}.rs", "x".repeat(260)))
            .collect::<Vec<_>>();
        store
            .complete(
                &metadata.id,
                DelegationResult {
                    status: DelegationStatus::Completed,
                    summary: "s".repeat(READ_TEXT_MAX_CHARS + 1_000),
                    changed_files,
                    artifacts: vec![DelegationArtifactRef {
                        path: "artifact.txt".into(),
                        description: Some("artifact description".into()),
                    }],
                    error_summary: None,
                    completed_at: Utc::now(),
                },
            )
            .await
            .expect("complete");

        let metadata = store.load(&metadata.id).await.expect("metadata");
        assert!(metadata
            .progress_summary
            .as_deref()
            .is_some_and(|summary| summary.chars().count() <= SUMMARY_TEXT_LIMIT + 3));
        assert!(metadata.changed_files.len() <= SUMMARY_CHANGED_FILES_LIMIT);
        assert!(metadata
            .changed_files
            .iter()
            .all(|path| path.chars().count() <= SUMMARY_CHANGED_FILE_LIMIT + 3));

        let summary_read = store
            .read(&metadata.id, DelegationReadMode::Summary)
            .await
            .expect("summary");
        let DelegationRead::Summary { summary, .. } = summary_read else {
            panic!("expected summary");
        };
        assert!(summary.changed_files.len() <= super::super::types::SUMMARY_CHANGED_FILES_LIMIT);
        assert!(summary.changed_files.iter().all(
            |path| path.chars().count() <= super::super::types::SUMMARY_CHANGED_FILE_LIMIT + 3
        ));

        let result_read = store
            .read(&metadata.id, DelegationReadMode::Result)
            .await
            .expect("result");
        let DelegationRead::Result {
            result_markdown,
            truncated,
            ..
        } = result_read
        else {
            panic!("expected result");
        };
        assert!(truncated);
        assert!(
            result_markdown.expect("result markdown").chars().count() <= READ_TEXT_MAX_CHARS + 3
        );
    }

    #[tokio::test]
    async fn progress_is_bounded_before_metadata_and_events_are_written() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start");
        store
            .update_progress(
                &metadata.id,
                DelegationUpdate {
                    current_step: Some("step".repeat(400)),
                    summary: "summary".repeat(400),
                    artifacts: (0..20)
                        .map(|idx| DelegationArtifactRef {
                            path: format!("artifact-{idx}-{}", "x".repeat(500)),
                            description: Some("desc".repeat(200)),
                        })
                        .collect(),
                },
            )
            .await
            .expect("progress");

        let metadata = store.load(&metadata.id).await.expect("metadata");
        assert!(metadata
            .current_step
            .as_deref()
            .is_some_and(|value| value.chars().count() <= SUMMARY_FIELD_LIMIT + 3));
        assert!(metadata
            .progress_summary
            .as_deref()
            .is_some_and(|value| value.chars().count() <= SUMMARY_TEXT_LIMIT + 3));
        let progress = store
            .read_progress(&metadata.id)
            .await
            .expect("progress")
            .expect("progress exists");
        assert!(progress.artifacts.len() <= SUMMARY_CHANGED_FILES_LIMIT);
        assert!(progress
            .artifacts
            .iter()
            .all(|artifact| artifact.path.chars().count() <= SUMMARY_FIELD_LIMIT + 3));
    }

    #[tokio::test]
    async fn abandon_is_idempotent_and_writes_result_summary() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store.start(&metadata.id).await.expect("start");
        store
            .update_progress(
                &metadata.id,
                DelegationUpdate {
                    current_step: Some("step".into()),
                    summary: "partial result".into(),
                    artifacts: Vec::new(),
                },
            )
            .await
            .expect("progress");

        let abandoned = store
            .abandon(&metadata.id, "session finalizing".into())
            .await
            .expect("abandon");
        assert_eq!(abandoned.status, DelegationStatus::Abandoned);
        assert_eq!(abandoned.result_ref.as_deref(), Some(RESULT_MD));
        let again = store
            .abandon(&metadata.id, "late abandon".into())
            .await
            .expect("second abandon should be idempotent");
        assert_eq!(again.status, DelegationStatus::Abandoned);
        let read = store
            .read(&metadata.id, DelegationReadMode::Result)
            .await
            .expect("result");
        let DelegationRead::Result {
            result_markdown, ..
        } = read
        else {
            panic!("expected result read");
        };
        let result_markdown = result_markdown.expect("result markdown");
        assert!(result_markdown.contains("partial result"));
        assert!(result_markdown.contains("session finalizing"));
    }

    #[tokio::test]
    async fn transcript_tail_and_compaction_summary_roundtrip() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = DelegationStore::new(dir.path().join("sessions/session_aaaaaaaa"));
        let metadata = store.create(request()).await.expect("create delegation");
        store
            .append_transcript_entry(
                &metadata.id,
                DelegationTranscriptEntry {
                    at: Utc::now(),
                    kind: DelegationTranscriptKind::Message {
                        source: DelegationTranscriptMessageSource::Objective,
                        message: crate::api::SessionTurnMessage::user_text("objective"),
                    },
                },
            )
            .await
            .expect("append transcript");
        store
            .append_transcript_entry(
                &metadata.id,
                DelegationTranscriptEntry {
                    at: Utc::now(),
                    kind: DelegationTranscriptKind::ToolCompleted {
                        id: "toolu_1".into(),
                        summary: "file_write ok".into(),
                        outcome: Some(crate::api::ToolExecutionOutcome::Completed),
                        output_preview: "ok".into(),
                        output_truncated: false,
                        file_change: crate::tool::diff::compute_file_change(
                            "src/lib.rs",
                            crate::tool::diff::FileChangeKind::Modified,
                            "old\n",
                            "new\n",
                            20,
                        ),
                    },
                },
            )
            .await
            .expect("append transcript");
        store
            .write_compaction_state(
                &metadata.id,
                &DelegationCompactionState {
                    schema_version: 1,
                    compacted_until: 2,
                    summary: "compressed earlier work".into(),
                    summary_updated_at: Utc::now(),
                },
            )
            .await
            .expect("write compaction");

        let summary = store
            .read(&metadata.id, DelegationReadMode::Summary)
            .await
            .expect("summary");
        let DelegationRead::Summary {
            compaction_summary, ..
        } = summary
        else {
            panic!("expected summary");
        };
        assert_eq!(
            compaction_summary.as_deref(),
            Some("compressed earlier work")
        );

        let tail = store
            .read(
                &metadata.id,
                DelegationReadMode::TranscriptTail {
                    limit: 1,
                    max_chars: 1000,
                },
            )
            .await
            .expect("transcript tail");
        let DelegationRead::TranscriptTail {
            entries, truncated, ..
        } = tail
        else {
            panic!("expected transcript tail");
        };
        assert_eq!(entries.len(), 1);
        assert!(truncated);
        assert!(matches!(
            &entries[0].kind,
            DelegationTranscriptKind::ToolCompleted {
                file_change: Some(change),
                ..
            } if change.path == "src/lib.rs"
        ));
    }

    #[test]
    fn read_mode_rejects_invalid_mode_arguments() {
        let zero_limit = read_mode_from_json(&serde_json::json!({
            "mode": "events_tail",
            "limit": 0
        }))
        .expect_err("zero limit should fail");
        assert!(matches!(
            zero_limit,
            DelegationStoreError::InvalidReadMode(_)
        ));

        let summary_with_path = read_mode_from_json(&serde_json::json!({
            "id": "subagent_12345678",
            "mode": "summary",
            "path": "result.md"
        }))
        .expect_err("summary should reject path");
        assert!(matches!(
            summary_with_path,
            DelegationStoreError::InvalidReadMode(_)
        ));

        let summary_with_limit = read_mode_from_json(&serde_json::json!({
            "mode": "summary",
            "limit": 5
        }))
        .expect("summary should ignore limit");
        assert_eq!(summary_with_limit, DelegationReadMode::Summary);

        let result_with_limit = read_mode_from_json(&serde_json::json!({
            "mode": "result",
            "limit": 5
        }))
        .expect("result should ignore limit");
        assert_eq!(result_with_limit, DelegationReadMode::Result);

        let events_with_path = read_mode_from_json(&serde_json::json!({
            "mode": "events_tail",
            "path": "progress.md"
        }))
        .expect_err("events_tail should reject path");
        assert!(matches!(
            events_with_path,
            DelegationStoreError::InvalidReadMode(_)
        ));

        let transcript_mode = read_mode_from_json(&serde_json::json!({
            "mode": "transcript_tail",
            "limit": 2,
            "max_chars": 1024
        }))
        .expect("transcript_tail should accept limit and max_chars");
        assert_eq!(
            transcript_mode,
            DelegationReadMode::TranscriptTail {
                limit: 2,
                max_chars: 1024
            }
        );
    }
}
