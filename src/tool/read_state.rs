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

impl ReadStateStore {
    pub(crate) async fn record(&self, scope: &ReadStateScope, evidence: ReadEvidence) {
        let mut inner = self.inner.lock().await;
        let key = (scope.clone(), evidence.path);
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
        insert_state(&mut inner, key, state);
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
        remove_key(&mut inner, &(scope.clone(), path.to_path_buf()));
    }

    pub(crate) async fn clear_scope(&self, scope: &ReadStateScope) {
        let mut inner = self.inner.lock().await;
        inner.map.retain(|(candidate, _), _| candidate != scope);
        inner.order.retain(|(candidate, _)| candidate != scope);
    }

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

fn insert_state(inner: &mut Inner, key: ReadStateKey, state: FileReadState) {
    let is_new = !inner.map.contains_key(&key);
    inner.map.insert(key.clone(), state);
    if is_new {
        inner.order.push_back(key);
    }
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
