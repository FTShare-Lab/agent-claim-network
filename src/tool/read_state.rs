//! file 类工具的运行期读取证据与分级写入许可。
//!
//! 状态只保存内容摘要、逻辑行范围和 EOF 信息，不缓存文件全文。状态不持久化，
//! resume 或上下文压缩后会保守清理。

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use ring::digest::{digest, SHA256};
use tokio::sync::Mutex;

use crate::claim::SessionId;

const READ_STATE_MAX_ENTRIES: usize = 1024;
const READ_STATE_MAX_RANGES_PER_FILE: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ContentRevision {
    sha256: String,
    byte_len: u64,
}

impl ContentRevision {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            sha256: hex::encode(digest(&SHA256, bytes).as_ref()),
            byte_len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        }
    }

    pub(crate) fn from_text(text: &str) -> Self {
        Self::from_bytes(text.as_bytes())
    }

    pub(crate) fn from_sha256(sha256: String, byte_len: u64) -> Self {
        Self { sha256, byte_len }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LineRange {
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl LineRange {
    pub(crate) fn new(start: usize, end: usize) -> Option<Self> {
        (start > 0 && start <= end).then_some(Self { start, end })
    }

    fn touches(self, other: Self) -> bool {
        self.start <= other.end.saturating_add(1) && other.start <= self.end.saturating_add(1)
    }

    fn merge(self, other: Self) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    pub(crate) fn contains(self, start: usize, end: usize) -> bool {
        self.start <= start && end <= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadEvidence {
    pub(crate) path: PathBuf,
    pub(crate) revision: ContentRevision,
    pub(crate) total_lines: usize,
    pub(crate) ends_with_newline: bool,
    pub(crate) ranges: Vec<LineRange>,
    pub(crate) complete: bool,
}

impl ReadEvidence {
    pub(crate) fn complete_text(path: PathBuf, text: &str) -> Self {
        let total_lines = logical_line_count(text);
        let ranges = LineRange::new(1, total_lines).into_iter().collect();
        Self {
            path,
            revision: ContentRevision::from_text(text),
            total_lines,
            ends_with_newline: text.as_bytes().last() == Some(&b'\n'),
            ranges,
            complete: true,
        }
    }

    pub(crate) fn known_ranges(
        path: PathBuf,
        text: &str,
        ranges: Vec<LineRange>,
        complete: bool,
    ) -> Self {
        Self {
            path,
            revision: ContentRevision::from_text(text),
            total_lines: logical_line_count(text),
            ends_with_newline: text.as_bytes().last() == Some(&b'\n'),
            ranges,
            complete,
        }
    }

    pub(crate) fn scanned(
        path: PathBuf,
        revision: ContentRevision,
        total_lines: usize,
        ends_with_newline: bool,
        ranges: Vec<LineRange>,
        complete: bool,
    ) -> Self {
        Self {
            path,
            revision,
            total_lines,
            ends_with_newline,
            ranges,
            complete,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileReadState {
    revision: ContentRevision,
    total_lines: usize,
    ends_with_newline: bool,
    ranges: Vec<LineRange>,
    complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadAuthority {
    pub(crate) total_lines: usize,
    pub(crate) ends_with_newline: bool,
    pub(crate) ranges: Vec<LineRange>,
    pub(crate) complete: bool,
}

impl ReadAuthority {
    pub(crate) fn covers(&self, start: usize, end: usize) -> bool {
        self.complete || self.ranges.iter().any(|range| range.contains(start, end))
    }

    pub(crate) fn has_eof(&self) -> bool {
        self.complete
            || (self.total_lines > 0
                && self
                    .ranges
                    .iter()
                    .any(|range| range.end == self.total_lines))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadStateVerdict {
    Missing,
    Fresh(ReadAuthority),
    Stale,
}

#[derive(Debug, Default)]
pub struct ReadStateStore {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    map: HashMap<ReadStateKey, FileReadState>,
    order: VecDeque<ReadStateKey>,
    checkpoints: HashMap<ReadStateScope, ReadStateCheckpoint>,
}

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

#[derive(Debug)]
struct ReadStateCheckpoint {
    turn_id: String,
    before: HashMap<ReadStateKey, Option<FileReadState>>,
}

impl ReadStateStore {
    pub(crate) async fn begin_checkpoint(
        &self,
        scope: &ReadStateScope,
        turn_id: &str,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.checkpoints.get(scope) {
            return Err(format!(
                "file read state scope 已有活动 checkpoint: {}",
                existing.turn_id
            ));
        }
        inner.checkpoints.insert(
            scope.clone(),
            ReadStateCheckpoint {
                turn_id: turn_id.to_owned(),
                before: HashMap::new(),
            },
        );
        Ok(())
    }

    pub(crate) async fn commit_checkpoint(
        &self,
        scope: &ReadStateScope,
        turn_id: &str,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        ensure_checkpoint_owner(&inner, scope, turn_id)?;
        inner.checkpoints.remove(scope);
        Ok(())
    }

    pub(crate) async fn rollback_checkpoint(
        &self,
        scope: &ReadStateScope,
        turn_id: &str,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock().await;
        ensure_checkpoint_owner(&inner, scope, turn_id)?;
        let Some(checkpoint) = inner.checkpoints.remove(scope) else {
            return Ok(());
        };
        for key in checkpoint.before.keys() {
            remove_key(&mut inner, key);
        }
        for (key, state) in checkpoint.before {
            if let Some(state) = state {
                insert_state_untracked(&mut inner, key, state);
            }
        }
        enforce_capacity(&mut inner);
        Ok(())
    }

    pub(crate) async fn record(&self, scope: &ReadStateScope, evidence: ReadEvidence) {
        let mut inner = self.inner.lock().await;
        let key = (scope.clone(), evidence.path);
        capture_before(&mut inner, scope, &key);
        let mut state = match inner.map.get(&key).cloned() {
            Some(existing) if existing.revision == evidence.revision => existing,
            _ => FileReadState {
                revision: evidence.revision,
                total_lines: evidence.total_lines,
                ends_with_newline: evidence.ends_with_newline,
                ranges: Vec::new(),
                complete: false,
            },
        };
        state.total_lines = evidence.total_lines;
        state.ends_with_newline = evidence.ends_with_newline;
        state.complete |= evidence.complete;
        state.ranges.extend(evidence.ranges);
        merge_ranges(&mut state.ranges);
        if !state.complete && covers_whole_file(&state.ranges, state.total_lines) {
            state.complete = true;
        }
        if state.ranges.len() > READ_STATE_MAX_RANGES_PER_FILE {
            remove_key(&mut inner, &key);
            return;
        }
        insert_state(&mut inner, scope, key, state);
    }

    pub(crate) async fn evaluate(
        &self,
        scope: &ReadStateScope,
        path: &Path,
        revision: &ContentRevision,
    ) -> ReadStateVerdict {
        let mut inner = self.inner.lock().await;
        let key = (scope.clone(), path.to_path_buf());
        let Some(state) = inner.map.get(&key) else {
            return ReadStateVerdict::Missing;
        };
        if &state.revision != revision {
            capture_before(&mut inner, scope, &key);
            remove_key(&mut inner, &key);
            return ReadStateVerdict::Stale;
        }
        ReadStateVerdict::Fresh(ReadAuthority {
            total_lines: state.total_lines,
            ends_with_newline: state.ends_with_newline,
            ranges: state.ranges.clone(),
            complete: state.complete,
        })
    }

    pub(crate) async fn clear_path(&self, scope: &ReadStateScope, path: &Path) {
        let mut inner = self.inner.lock().await;
        let key = (scope.clone(), path.to_path_buf());
        capture_before(&mut inner, scope, &key);
        remove_key(&mut inner, &key);
    }

    pub(crate) async fn clear_scope(&self, scope: &ReadStateScope) {
        let mut inner = self.inner.lock().await;
        inner.map.retain(|(candidate, _), _| candidate != scope);
        inner.order.retain(|(candidate, _)| candidate != scope);
        clear_checkpoint_before_images(&mut inner, |(candidate, _)| candidate == scope);
    }

    pub async fn clear_session(&self, session_id: &SessionId) {
        let mut inner = self.inner.lock().await;
        inner
            .map
            .retain(|(scope, _), _| scope.session_id.as_ref() != Some(session_id));
        inner
            .order
            .retain(|(scope, _)| scope.session_id.as_ref() != Some(session_id));
        clear_checkpoint_before_images(&mut inner, |(scope, _)| {
            scope.session_id.as_ref() == Some(session_id)
        });
    }
}

fn ensure_checkpoint_owner(
    inner: &Inner,
    scope: &ReadStateScope,
    turn_id: &str,
) -> Result<(), String> {
    match inner.checkpoints.get(scope) {
        Some(checkpoint) if checkpoint.turn_id == turn_id => Ok(()),
        Some(checkpoint) => Err(format!(
            "file read state checkpoint owner 不匹配: expected={}, actual={turn_id}",
            checkpoint.turn_id
        )),
        None => Err(format!(
            "file read state scope 没有活动 checkpoint: {turn_id}"
        )),
    }
}

fn capture_before(inner: &mut Inner, owner_scope: &ReadStateScope, key: &ReadStateKey) {
    let before = inner.map.get(key).cloned();
    if let Some(checkpoint) = inner.checkpoints.get_mut(owner_scope) {
        checkpoint.before.entry(key.clone()).or_insert(before);
    }
}

fn clear_checkpoint_before_images(
    inner: &mut Inner,
    mut matches: impl FnMut(&ReadStateKey) -> bool,
) {
    for checkpoint in inner.checkpoints.values_mut() {
        checkpoint.before.retain(|key, _| !matches(key));
    }
}

fn insert_state(
    inner: &mut Inner,
    owner_scope: &ReadStateScope,
    key: ReadStateKey,
    state: FileReadState,
) {
    insert_state_untracked(inner, key, state);
    while inner.order.len() > READ_STATE_MAX_ENTRIES {
        if let Some(evicted) = inner.order.front().cloned() {
            capture_before(inner, owner_scope, &evicted);
            remove_key(inner, &evicted);
        }
    }
}

fn insert_state_untracked(inner: &mut Inner, key: ReadStateKey, state: FileReadState) {
    let is_new = !inner.map.contains_key(&key);
    inner.map.insert(key.clone(), state);
    if is_new {
        inner.order.push_back(key);
    }
}

fn enforce_capacity(inner: &mut Inner) {
    while inner.order.len() > READ_STATE_MAX_ENTRIES {
        if let Some(evicted) = inner.order.pop_front() {
            inner.map.remove(&evicted);
        }
    }
}

fn remove_key(inner: &mut Inner, key: &ReadStateKey) {
    inner.map.remove(key);
    inner.order.retain(|candidate| candidate != key);
}

fn merge_ranges(ranges: &mut Vec<LineRange>) {
    ranges.sort_by_key(|range| (range.start, range.end));
    let mut merged = Vec::<LineRange>::with_capacity(ranges.len());
    for range in ranges.drain(..) {
        if let Some(last) = merged.last_mut() {
            if last.touches(range) {
                *last = last.merge(range);
                continue;
            }
        }
        merged.push(range);
    }
    *ranges = merged;
}

fn covers_whole_file(ranges: &[LineRange], total_lines: usize) -> bool {
    total_lines > 0 && ranges.len() == 1 && ranges[0].start == 1 && ranges[0].end == total_lines
}

pub(crate) fn logical_line_count(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    text.as_bytes()
        .iter()
        .filter(|byte| **byte == b'\n')
        .count()
        .saturating_add(usize::from(!text.ends_with('\n')))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionId {
        "session_aaaaaaaa".parse().expect("测试 session id 合法")
    }

    fn scope() -> ReadStateScope {
        ReadStateScope::new(Some(session()), None)
    }

    fn evidence(path: &str, text: &str, range: LineRange) -> ReadEvidence {
        ReadEvidence::scanned(
            PathBuf::from(path),
            ContentRevision::from_text(text),
            logical_line_count(text),
            text.ends_with('\n'),
            vec![range],
            false,
        )
    }

    #[test]
    fn line_count_preserves_terminal_newline_semantics() {
        assert_eq!(logical_line_count(""), 0);
        assert_eq!(logical_line_count("a"), 1);
        assert_eq!(logical_line_count("a\n"), 1);
        assert_eq!(logical_line_count("a\n\n"), 2);
        assert_eq!(logical_line_count("\n"), 1);
    }

    #[tokio::test]
    async fn pages_merge_to_complete_only_without_gaps() {
        let store = ReadStateStore::default();
        let text = "1\n2\n3\n4\n";
        store
            .record(&scope(), evidence("a", text, LineRange::new(3, 4).unwrap()))
            .await;
        store
            .record(&scope(), evidence("a", text, LineRange::new(1, 2).unwrap()))
            .await;
        let verdict = store
            .evaluate(&scope(), Path::new("a"), &ContentRevision::from_text(text))
            .await;
        assert!(matches!(
            verdict,
            ReadStateVerdict::Fresh(ReadAuthority { complete: true, .. })
        ));
    }

    #[tokio::test]
    async fn rollback_removes_state_created_in_uncommitted_turn() {
        let store = ReadStateStore::default();
        let scope = scope();
        let revision = ContentRevision::from_text("one\n");
        store.begin_checkpoint(&scope, "turn-1").await.unwrap();
        store
            .record(
                &scope,
                ReadEvidence::complete_text(PathBuf::from("a"), "one\n"),
            )
            .await;

        store.rollback_checkpoint(&scope, "turn-1").await.unwrap();

        assert_eq!(
            store.evaluate(&scope, Path::new("a"), &revision).await,
            ReadStateVerdict::Missing
        );
    }

    #[tokio::test]
    async fn rollback_restores_only_pre_turn_coverage() {
        let store = ReadStateStore::default();
        let scope = scope();
        let text = "1\n2\n3\n4\n";
        let revision = ContentRevision::from_text(text);
        store
            .record(&scope, evidence("a", text, LineRange::new(1, 2).unwrap()))
            .await;
        store.begin_checkpoint(&scope, "turn-1").await.unwrap();
        store
            .record(&scope, evidence("a", text, LineRange::new(3, 4).unwrap()))
            .await;
        assert!(matches!(
            store.evaluate(&scope, Path::new("a"), &revision).await,
            ReadStateVerdict::Fresh(ReadAuthority { complete: true, .. })
        ));

        store.rollback_checkpoint(&scope, "turn-1").await.unwrap();

        assert_eq!(
            store.evaluate(&scope, Path::new("a"), &revision).await,
            ReadStateVerdict::Fresh(ReadAuthority {
                total_lines: 4,
                ends_with_newline: true,
                ranges: vec![LineRange::new(1, 2).unwrap()],
                complete: false,
            })
        );
    }

    #[tokio::test]
    async fn commit_keeps_turn_state() {
        let store = ReadStateStore::default();
        let scope = scope();
        let revision = ContentRevision::from_text("one\n");
        store.begin_checkpoint(&scope, "turn-1").await.unwrap();
        store
            .record(
                &scope,
                ReadEvidence::complete_text(PathBuf::from("a"), "one\n"),
            )
            .await;

        store.commit_checkpoint(&scope, "turn-1").await.unwrap();

        assert!(matches!(
            store.evaluate(&scope, Path::new("a"), &revision).await,
            ReadStateVerdict::Fresh(ReadAuthority { complete: true, .. })
        ));
    }

    #[tokio::test]
    async fn lifecycle_clear_prevents_rollback_from_restoring_old_authority() {
        let store = ReadStateStore::default();
        let scope = scope();
        let revision = ContentRevision::from_text("one\n");
        store
            .record(
                &scope,
                ReadEvidence::complete_text(PathBuf::from("a"), "one\n"),
            )
            .await;
        store.begin_checkpoint(&scope, "turn-1").await.unwrap();
        store
            .record(
                &scope,
                ReadEvidence::complete_text(PathBuf::from("b"), "two\n"),
            )
            .await;
        store.clear_scope(&scope).await;
        store
            .record(
                &scope,
                ReadEvidence::complete_text(PathBuf::from("c"), "three\n"),
            )
            .await;

        store.rollback_checkpoint(&scope, "turn-1").await.unwrap();

        assert_eq!(
            store.evaluate(&scope, Path::new("a"), &revision).await,
            ReadStateVerdict::Missing
        );
        assert_eq!(
            store
                .evaluate(
                    &scope,
                    Path::new("c"),
                    &ContentRevision::from_text("three\n")
                )
                .await,
            ReadStateVerdict::Missing
        );
    }

    #[tokio::test]
    async fn parent_lifecycle_clear_does_not_touch_child_checkpoint() {
        let store = ReadStateStore::default();
        let parent = scope();
        let child = ReadStateScope::new(Some(session()), Some("subagent-1".into()));
        let child_text = "one\ntwo\n";
        let child_revision = ContentRevision::from_text(child_text);
        store
            .record(
                &child,
                evidence("child.txt", child_text, LineRange::new(1, 1).unwrap()),
            )
            .await;
        store.begin_checkpoint(&child, "subagent-1").await.unwrap();
        store
            .record(
                &child,
                evidence("child.txt", child_text, LineRange::new(2, 2).unwrap()),
            )
            .await;

        store.clear_scope(&parent).await;
        store
            .rollback_checkpoint(&child, "subagent-1")
            .await
            .unwrap();

        assert_eq!(
            store
                .evaluate(&child, Path::new("child.txt"), &child_revision)
                .await,
            ReadStateVerdict::Fresh(ReadAuthority {
                total_lines: 2,
                ends_with_newline: true,
                ranges: vec![LineRange::new(1, 1).unwrap()],
                complete: false,
            })
        );
    }

    #[tokio::test]
    async fn rollback_restores_state_evicted_by_turn_capacity_change() {
        let store = ReadStateStore::default();
        let scope = scope();
        let oldest_text = "oldest\n";
        let oldest_revision = ContentRevision::from_text(oldest_text);
        for index in 0..READ_STATE_MAX_ENTRIES {
            store
                .record(
                    &scope,
                    ReadEvidence::complete_text(
                        PathBuf::from(format!("file-{index}")),
                        if index == 0 { oldest_text } else { "other\n" },
                    ),
                )
                .await;
        }
        store.begin_checkpoint(&scope, "turn-1").await.unwrap();
        store
            .record(
                &scope,
                ReadEvidence::complete_text(PathBuf::from("new-file"), "new\n"),
            )
            .await;
        assert_eq!(
            store
                .evaluate(&scope, Path::new("file-0"), &oldest_revision)
                .await,
            ReadStateVerdict::Missing
        );

        store.rollback_checkpoint(&scope, "turn-1").await.unwrap();

        assert!(matches!(
            store
                .evaluate(&scope, Path::new("file-0"), &oldest_revision)
                .await,
            ReadStateVerdict::Fresh(ReadAuthority { complete: true, .. })
        ));
        assert_eq!(
            store
                .evaluate(
                    &scope,
                    Path::new("new-file"),
                    &ContentRevision::from_text("new\n")
                )
                .await,
            ReadStateVerdict::Missing
        );
    }

    #[tokio::test]
    async fn new_revision_replaces_old_ranges() {
        let store = ReadStateStore::default();
        store
            .record(
                &scope(),
                evidence("a", "one\n", LineRange::new(1, 1).unwrap()),
            )
            .await;
        store
            .record(
                &scope(),
                evidence("a", "two\n", LineRange::new(1, 1).unwrap()),
            )
            .await;
        assert!(matches!(
            store
                .evaluate(
                    &scope(),
                    Path::new("a"),
                    &ContentRevision::from_text("two\n")
                )
                .await,
            ReadStateVerdict::Fresh(_)
        ));
        assert_eq!(
            store
                .evaluate(
                    &scope(),
                    Path::new("a"),
                    &ContentRevision::from_text("one\n")
                )
                .await,
            ReadStateVerdict::Stale
        );
    }

    #[tokio::test]
    async fn revision_change_is_stale_and_removes_state() {
        let store = ReadStateStore::default();
        store
            .record(
                &scope(),
                ReadEvidence::complete_text(PathBuf::from("a"), "one\n"),
            )
            .await;
        assert_eq!(
            store
                .evaluate(
                    &scope(),
                    Path::new("a"),
                    &ContentRevision::from_text("two\n")
                )
                .await,
            ReadStateVerdict::Stale
        );
        assert_eq!(
            store
                .evaluate(
                    &scope(),
                    Path::new("a"),
                    &ContentRevision::from_text("one\n")
                )
                .await,
            ReadStateVerdict::Missing
        );
    }

    #[tokio::test]
    async fn clearing_child_scope_keeps_parent_authority() {
        let store = ReadStateStore::default();
        let parent = scope();
        let child = ReadStateScope::new(Some(session()), Some("child-a".into()));
        let revision = ContentRevision::from_text("one\n");
        store
            .record(
                &parent,
                ReadEvidence::complete_text(PathBuf::from("a"), "one\n"),
            )
            .await;
        store
            .record(
                &child,
                ReadEvidence::complete_text(PathBuf::from("a"), "one\n"),
            )
            .await;

        store.clear_scope(&child).await;

        assert!(matches!(
            store.evaluate(&parent, Path::new("a"), &revision).await,
            ReadStateVerdict::Fresh(_)
        ));
        assert_eq!(
            store.evaluate(&child, Path::new("a"), &revision).await,
            ReadStateVerdict::Missing
        );
    }

    #[tokio::test]
    async fn excessive_fragmentation_drops_file_state_fail_safe() {
        let store = ReadStateStore::default();
        let text = "x\n".repeat(
            READ_STATE_MAX_RANGES_PER_FILE
                .saturating_mul(2)
                .saturating_add(1),
        );
        for index in 0..=READ_STATE_MAX_RANGES_PER_FILE {
            let line = index.saturating_mul(2).saturating_add(1);
            store
                .record(
                    &scope(),
                    evidence("a", &text, LineRange::new(line, line).unwrap()),
                )
                .await;
        }

        assert_eq!(
            store
                .evaluate(&scope(), Path::new("a"), &ContentRevision::from_text(&text))
                .await,
            ReadStateVerdict::Missing
        );
    }

    #[tokio::test]
    async fn global_capacity_evicts_oldest_state() {
        let store = ReadStateStore::default();
        for index in 0..=READ_STATE_MAX_ENTRIES {
            store
                .record(
                    &scope(),
                    ReadEvidence::complete_text(PathBuf::from(format!("f{index}")), "x\n"),
                )
                .await;
        }

        let revision = ContentRevision::from_text("x\n");
        assert_eq!(
            store.evaluate(&scope(), Path::new("f0"), &revision).await,
            ReadStateVerdict::Missing
        );
        assert!(matches!(
            store
                .evaluate(
                    &scope(),
                    Path::new(&format!("f{READ_STATE_MAX_ENTRIES}")),
                    &revision,
                )
                .await,
            ReadStateVerdict::Fresh(_)
        ));
    }
}
