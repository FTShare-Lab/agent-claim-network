//! file 类工具修改文件后的 diff 采集模型。
//!
//! 对外提供 `FileChange`（路径、变更类型、增删统计、截断后的 diff 行）与
//! `compute_file_change` 采集入口。tool 层把结果塞进输出 JSON 的保留键
//! `FILE_CHANGE_KEY`，turn loop 在回灌模型前用 `take_file_change` 剥离，
//! 再随事件透传给 TUI 渲染与 turn journal 持久化。

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use similar::{ChangeTag, TextDiff};

/// 工具输出 JSON 中携带 FileChange 的保留键；只用于进程内透传，不回灌模型。
pub const FILE_CHANGE_KEY: &str = "__acn_file_change";

/// diff hunk 两侧保留的上下文行数（固定小窗口）。
const DIFF_CONTEXT_RADIUS: usize = 3;

/// 单行收录的最大字符数，避免 minified JSON 等内容撑爆 journal。
const FILE_DIFF_MAX_LINE_CHARS: usize = 4 * 1024;

/// context 只用于定位，单行限额更小，避免挤占真正 +/- 内容。
const FILE_DIFF_MAX_CONTEXT_LINE_CHARS: usize = 512;

/// changed/context 独立的 JSON 编码内容预算，保证修改行优先。
const FILE_DIFF_MAX_CHANGED_CONTENT_BYTES: usize = 24 * 1024;
const FILE_DIFF_MAX_CONTEXT_CONTENT_BYTES: usize = 8 * 1024;

/// FileChange 的设计级序列化上限，供回归测试锁定。
#[cfg(test)]
const FILE_DIFF_MAX_SERIALIZED_BYTES: usize = 128 * 1024;

/// diff 算法的内部时间上限；超时时 similar 会返回完整的近似 diff。
const FILE_DIFF_TIMEOUT: Duration = Duration::from_millis(250);

/// 小文本使用无 deadline 的精确 diff；超大输入启用可控近似并显式标记。
const FILE_DIFF_EXACT_MAX_BYTES: usize = 1024 * 1024;

/// 新旧 journal 都使用的结构行数绝对上限。
const FILE_DIFF_MAX_RENDER_LINES: usize = 400;

const fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeKind {
    Created,
    Modified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileDiffLineKind {
    Context,
    Add,
    Remove,
    /// hunk 之间的省略分隔行。
    Gap,
}

/// 原文行的换行符；`Unknown` 仅用于兼容旧 journal。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileLineEnding {
    #[default]
    Unknown,
    Lf,
    CrLf,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDiffLine {
    pub kind: FileDiffLineKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_line: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_line: Option<usize>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content: String,
    #[serde(default)]
    pub line_ending: FileLineEnding,
    #[serde(default, skip_serializing_if = "is_false")]
    pub content_truncated: bool,
}

/// 一段连续 diff；hunk 之间的未变更区域不进入事件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDiffHunk {
    pub lines: Vec<FileDiffLine>,
}

/// 一次 file 工具调用对单个文件的修改摘要；diff 行在采集时已按改动行上限截断。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileChange {
    pub path: String,
    pub kind: FileChangeKind,
    /// 全量新增行数（不受截断影响）。
    pub added_lines: usize,
    /// 全量删除行数（不受截断影响）。
    pub removed_lines: usize,
    pub hunks: Vec<FileDiffHunk>,
    /// 因超出展示上限而未收录的改动行数（新增 + 删除），0 表示完整。
    pub truncated_changed_lines: usize,
    /// 已收录行的内容因内部 payload 上限而被截断。
    #[serde(default, skip_serializing_if = "is_false")]
    pub content_truncated: bool,
    /// 超大输入使用了带 deadline 的近似 diff，统计可能非最小编辑集。
    #[serde(default, skip_serializing_if = "is_false")]
    pub approximate: bool,
}

#[derive(Deserialize)]
struct FileChangeWire {
    path: String,
    kind: FileChangeKind,
    added_lines: usize,
    removed_lines: usize,
    #[serde(default)]
    hunks: Vec<FileDiffHunk>,
    #[serde(default)]
    diff_lines: Vec<FileDiffLine>,
    #[serde(default)]
    truncated_changed_lines: usize,
    #[serde(default)]
    content_truncated: bool,
    #[serde(default)]
    approximate: bool,
}

impl<'de> Deserialize<'de> for FileChange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FileChangeWire::deserialize(deserializer)?;
        let hunks = if wire.hunks.is_empty() && !wire.diff_lines.is_empty() {
            vec![FileDiffHunk {
                lines: wire.diff_lines,
            }]
        } else {
            wire.hunks
        };
        Ok(normalize_file_change(Self {
            path: wire.path,
            kind: wire.kind,
            added_lines: wire.added_lines,
            removed_lines: wire.removed_lines,
            hunks,
            truncated_changed_lines: wire.truncated_changed_lines,
            content_truncated: wire.content_truncated,
            approximate: wire.approximate,
        }))
    }
}

fn normalize_file_change(mut change: FileChange) -> FileChange {
    change.path = change.path.chars().take(FILE_DIFF_MAX_LINE_CHARS).collect();
    let mut changed_content_bytes_left = FILE_DIFF_MAX_CHANGED_CONTENT_BYTES;
    let mut context_content_bytes_left = FILE_DIFF_MAX_CONTEXT_CONTENT_BYTES;
    let mut normalized_hunks = Vec::new();
    let mut kept_lines = 0usize;
    let mut omitted_changed = 0usize;
    let mut content_truncated = change.content_truncated;
    for hunk in change.hunks {
        let mut normalized_lines = Vec::new();
        for mut line in hunk.lines {
            let is_changed = matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove);
            if kept_lines >= FILE_DIFF_MAX_RENDER_LINES {
                omitted_changed = omitted_changed.saturating_add(usize::from(is_changed));
                continue;
            }
            let (max_line_chars, content_bytes_left) = if is_changed {
                (FILE_DIFF_MAX_LINE_CHARS, &mut changed_content_bytes_left)
            } else {
                (
                    FILE_DIFF_MAX_CONTEXT_LINE_CHARS,
                    &mut context_content_bytes_left,
                )
            };
            let (content, truncated) =
                truncate_line(&line.content, max_line_chars, content_bytes_left);
            line.content = content;
            line.content_truncated |= truncated;
            content_truncated |= truncated;
            normalized_lines.push(line);
            kept_lines = kept_lines.saturating_add(1);
        }
        if !normalized_lines.is_empty() {
            normalized_hunks.push(FileDiffHunk {
                lines: normalized_lines,
            });
        }
    }
    change.hunks = normalized_hunks;
    change.truncated_changed_lines = change
        .truncated_changed_lines
        .saturating_add(omitted_changed);
    change.content_truncated = content_truncated;
    change
}

/// 计算 before → after 的行级 diff。仅在内容未变时返回 None。
///
/// 截断按**改动行**（+/-）计数：最多收录 `max_changed_lines` 行改动，上下文行不占额度；
/// 另有一个总渲染行数兜底，避免上下文过多把事件 / journal 撑爆。
pub fn compute_file_change(
    path: impl Into<String>,
    kind: FileChangeKind,
    before: &str,
    after: &str,
    max_changed_lines: usize,
) -> Option<FileChange> {
    if before == after {
        return None;
    }
    let max_changed = max_changed_lines.max(1);
    // changed line 永远优先：最多收录结构上限数量的 +/- 行，上下文只使用剩余配额。
    let changed_capture_limit = max_changed.min(FILE_DIFF_MAX_RENDER_LINES);
    let approximate = before.len().max(after.len()) > FILE_DIFF_EXACT_MAX_BYTES;
    let diff = if approximate {
        TextDiff::configure()
            .timeout(FILE_DIFF_TIMEOUT)
            .diff_lines(before, after)
    } else {
        TextDiff::from_lines(before, after)
    };
    let mut added_lines = 0usize;
    let mut removed_lines = 0usize;
    let mut changed_shown = 0usize;
    let mut changed_content_bytes_left = FILE_DIFF_MAX_CHANGED_CONTENT_BYTES;
    let mut context_content_bytes_left = FILE_DIFF_MAX_CONTEXT_CONTENT_BYTES;
    let mut hunks = Vec::new();
    for group in diff.grouped_ops(DIFF_CONTEXT_RADIUS) {
        let mut hunk_lines = Vec::new();
        let mut capture_open = changed_shown < changed_capture_limit;
        for op in group {
            for change in diff.iter_changes(&op) {
                let line_kind = match change.tag() {
                    ChangeTag::Equal => FileDiffLineKind::Context,
                    ChangeTag::Delete => {
                        removed_lines = removed_lines.saturating_add(1);
                        FileDiffLineKind::Remove
                    }
                    ChangeTag::Insert => {
                        added_lines = added_lines.saturating_add(1);
                        FileDiffLineKind::Add
                    }
                };
                let is_changed =
                    matches!(line_kind, FileDiffLineKind::Add | FileDiffLineKind::Remove);
                if !capture_open {
                    continue;
                }
                if is_changed && changed_shown >= changed_capture_limit {
                    // 下一个 +/- 表示最后一个已收录改动的 trailing context 已结束。
                    capture_open = false;
                    continue;
                }
                if is_changed {
                    changed_shown = changed_shown.saturating_add(1);
                }
                let (raw_content, line_ending) = split_line_ending(change.value());
                let (max_line_chars, content_bytes_left) = if is_changed {
                    (FILE_DIFF_MAX_LINE_CHARS, &mut changed_content_bytes_left)
                } else {
                    (
                        FILE_DIFF_MAX_CONTEXT_LINE_CHARS,
                        &mut context_content_bytes_left,
                    )
                };
                let (content, line_truncated) =
                    truncate_line(raw_content, max_line_chars, content_bytes_left);
                hunk_lines.push(FileDiffLine {
                    kind: line_kind,
                    old_line: change.old_index().map(|idx| idx.saturating_add(1)),
                    new_line: change.new_index().map(|idx| idx.saturating_add(1)),
                    content,
                    line_ending,
                    content_truncated: line_truncated,
                });
            }
        }
        if !hunk_lines.is_empty() {
            hunks.push(FileDiffHunk { lines: hunk_lines });
        }
    }
    let hunks = compact_hunks_preserving_changes(hunks, FILE_DIFF_MAX_RENDER_LINES);
    let content_truncated = hunks
        .iter()
        .flat_map(|hunk| &hunk.lines)
        .any(|line| line.content_truncated);
    let truncated_changed_lines = added_lines
        .saturating_add(removed_lines)
        .saturating_sub(changed_shown);
    Some(FileChange {
        path: path.into(),
        kind,
        added_lines,
        removed_lines,
        hunks,
        truncated_changed_lines,
        content_truncated,
        approximate,
    })
}

/// 将结构行数压到硬上限，但不让 context 挤掉已收录的 +/- 行。
fn compact_hunks_preserving_changes(
    hunks: Vec<FileDiffHunk>,
    max_lines: usize,
) -> Vec<FileDiffHunk> {
    let changed_lines = hunks
        .iter()
        .flat_map(|hunk| &hunk.lines)
        .filter(|line| matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove))
        .count();
    let mut context_lines_left = max_lines.saturating_sub(changed_lines);
    let mut compacted = Vec::new();
    for hunk in hunks {
        let mut lines = Vec::new();
        for line in hunk.lines {
            let is_changed = matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove);
            if is_changed {
                lines.push(line);
            } else if context_lines_left > 0 {
                context_lines_left = context_lines_left.saturating_sub(1);
                lines.push(line);
            }
        }
        // grouped_ops 产出的 hunk 必须有改动；压缩后再守住该不变量。
        if lines
            .iter()
            .any(|line| matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove))
        {
            compacted.push(FileDiffHunk { lines });
        }
    }
    compacted
}

/// 把 FileChange 挂到工具输出 JSON 的保留键上；输出不是对象时静默跳过。
pub fn attach_file_change(output: &mut Value, change: &FileChange) {
    let Some(object) = output.as_object_mut() else {
        return;
    };
    match serde_json::to_value(change) {
        Ok(value) => {
            object.insert(FILE_CHANGE_KEY.into(), value);
        }
        Err(err) => {
            log::warn!(target: "tool_diff", "FileChange 序列化失败，跳过 diff 透传: {err}");
        }
    }
}

/// 从工具输出 JSON 中取走保留键并反序列化；键缺失或结构不合法都返回 None。
pub fn take_file_change(output: &mut Value) -> Option<FileChange> {
    let value = output.as_object_mut()?.remove(FILE_CHANGE_KEY)?;
    match serde_json::from_value(value) {
        Ok(change) => Some(change),
        Err(err) => {
            log::warn!(target: "tool_diff", "FileChange 反序列化失败，已丢弃: {err}");
            None
        }
    }
}

fn split_line_ending(raw: &str) -> (&str, FileLineEnding) {
    if let Some(content) = raw.strip_suffix("\r\n") {
        (content, FileLineEnding::CrLf)
    } else if let Some(content) = raw.strip_suffix('\n') {
        (content, FileLineEnding::Lf)
    } else {
        (raw, FileLineEnding::None)
    }
}

fn truncate_line(raw: &str, max_chars: usize, content_bytes_left: &mut usize) -> (String, bool) {
    let mut result = String::new();
    let mut chars = 0usize;
    for ch in raw.chars() {
        let encoded_bytes = json_encoded_char_bytes(ch);
        if chars >= max_chars || encoded_bytes > *content_bytes_left {
            return (result, true);
        }
        result.push(ch);
        chars = chars.saturating_add(1);
        *content_bytes_left = content_bytes_left.saturating_sub(encoded_bytes);
    }
    (result, false)
}

fn json_encoded_char_bytes(ch: char) -> usize {
    match ch {
        '"' | '\\' => 2,
        ch if ch.is_control() => 6,
        ch => ch.len_utf8(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn all_lines(change: &FileChange) -> impl Iterator<Item = &FileDiffLine> {
        change.hunks.iter().flat_map(|hunk| hunk.lines.iter())
    }

    #[test]
    fn modified_diff_counts_and_line_numbers() {
        let before = "a\nb\nc\nd\n";
        let after = "a\nB\nc\nd\ne\n";
        let change = compute_file_change("x.txt", FileChangeKind::Modified, before, after, 100)
            .expect("需产出 diff");
        assert_eq!(change.kind, FileChangeKind::Modified);
        assert_eq!(change.added_lines, 2);
        assert_eq!(change.removed_lines, 1);
        assert_eq!(change.truncated_changed_lines, 0);
        assert!(!change.approximate);
        let removed = all_lines(&change)
            .find(|line| line.kind == FileDiffLineKind::Remove)
            .expect("有删除行");
        assert_eq!(removed.old_line, Some(2));
        assert_eq!(removed.content, "b");
        let added = all_lines(&change)
            .find(|line| line.kind == FileDiffLineKind::Add)
            .expect("有新增行");
        assert_eq!(added.new_line, Some(2));
        assert_eq!(added.content, "B");
    }

    #[test]
    fn created_diff_is_all_adds() {
        let change = compute_file_change("new.txt", FileChangeKind::Created, "", "x\ny\n", 100)
            .expect("需产出 diff");
        assert_eq!(change.kind, FileChangeKind::Created);
        assert_eq!(change.added_lines, 2);
        assert_eq!(change.removed_lines, 0);
        assert!(all_lines(&change).all(|line| line.kind == FileDiffLineKind::Add));
    }

    #[test]
    fn unchanged_modified_returns_none() {
        assert!(
            compute_file_change("x.txt", FileChangeKind::Modified, "a\n", "a\n", 100).is_none()
        );
    }

    #[test]
    fn truncation_counts_remaining_change_lines() {
        let before = "a\nb\nc\nd\ne\nf\n";
        let after = "A\nB\nC\nD\nE\nF\n";
        let change = compute_file_change("x.txt", FileChangeKind::Modified, before, after, 3)
            .expect("需产出 diff");
        assert_eq!(all_lines(&change).count(), 3);
        assert_eq!(change.added_lines, 6);
        assert_eq!(change.removed_lines, 6);
        // 12 行改动只展示了 3 行，剩余 9 行计入截断。
        assert_eq!(change.truncated_changed_lines, 9);
    }

    #[test]
    fn context_lines_included_and_not_counted_as_changed() {
        let before: String = (1..=8).map(|n| format!("line{n}\n")).collect();
        let after = format!("{before}line9\n");
        let change = compute_file_change("x.txt", FileChangeKind::Modified, &before, &after, 20)
            .expect("需产出 diff");
        assert_eq!(change.added_lines, 1);
        assert_eq!(change.removed_lines, 0);
        assert_eq!(change.truncated_changed_lines, 0);
        // 上下文行不占改动预算，应随 diff 一并展示。
        assert!(all_lines(&change).any(|line| line.kind == FileDiffLineKind::Context));
        assert!(all_lines(&change).any(|line| line.kind == FileDiffLineKind::Add));
    }

    #[test]
    fn distant_hunks_are_separated_by_gap() {
        let before: String = (1..=30).map(|n| format!("line{n}\n")).collect();
        let after = before
            .replace("line2\n", "line2x\n")
            .replace("line28\n", "line28x\n");
        let change = compute_file_change("x.txt", FileChangeKind::Modified, &before, &after, 100)
            .expect("需产出 diff");
        assert_eq!(change.hunks.len(), 2);
    }

    #[test]
    fn attach_and_take_roundtrip() {
        let change = compute_file_change("x.txt", FileChangeKind::Modified, "a\n", "b\n", 100)
            .expect("需产出 diff");
        let mut output = json!({"status": "success"});
        attach_file_change(&mut output, &change);
        assert!(output.get(FILE_CHANGE_KEY).is_some());
        let taken = take_file_change(&mut output).expect("需取回 FileChange");
        assert_eq!(taken, change);
        assert!(output.get(FILE_CHANGE_KEY).is_none());
        assert!(take_file_change(&mut output).is_none());
    }

    #[test]
    fn large_single_line_diff_is_structured_and_payload_bounded() {
        let before = format!("{}old", "a".repeat(512 * 1024));
        let after = format!("{}new", "b".repeat(512 * 1024));

        let change =
            compute_file_change("large.json", FileChangeKind::Modified, &before, &after, 20)
                .expect("超长单行仍应产出有界摘要");

        assert_eq!(change.added_lines, 1);
        assert_eq!(change.removed_lines, 1);
        assert!(change.content_truncated, "超长行必须带结构化截断标记");
        assert_eq!(change.hunks.len(), 1);
        assert!(change
            .hunks
            .iter()
            .flat_map(|hunk| &hunk.lines)
            .all(|line| line.content.chars().count() <= FILE_DIFF_MAX_LINE_CHARS));
        let encoded = serde_json::to_vec(&change).expect("FileChange 应可序列化");
        assert!(
            encoded.len() <= FILE_DIFF_MAX_SERIALIZED_BYTES,
            "payload 过大: {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn oversized_diff_keeps_bounded_structured_summary() {
        let before = format!("{}\n", "a".repeat(2 * 1024 * 1024));
        let after = format!("{}\n", "b".repeat(2 * 1024 * 1024));

        let change = compute_file_change(
            "oversized.json",
            FileChangeKind::Modified,
            &before,
            &after,
            20,
        )
        .expect("超过旧 2 MiB 阈值也不能伪装成无变化");

        assert_eq!(change.added_lines, 1);
        assert_eq!(change.removed_lines, 1);
        assert!(change.content_truncated);
        assert!(change.approximate);
        assert!(
            serde_json::to_vec(&change).expect("序列化").len() <= FILE_DIFF_MAX_SERIALIZED_BYTES
        );
    }

    #[test]
    fn line_ending_only_changes_remain_explicit() {
        let eof_change = compute_file_change("eof.txt", FileChangeKind::Modified, "a\n", "a", 20)
            .expect("EOF newline 变化应可见");
        let eof_endings = eof_change
            .hunks
            .iter()
            .flat_map(|hunk| &hunk.lines)
            .filter(|line| matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove))
            .map(|line| line.line_ending)
            .collect::<Vec<_>>();
        assert!(eof_endings.contains(&FileLineEnding::Lf));
        assert!(eof_endings.contains(&FileLineEnding::None));

        let crlf_change =
            compute_file_change("crlf.txt", FileChangeKind::Modified, "a\r\n", "a\n", 20)
                .expect("CRLF/LF 变化应可见");
        let crlf_endings = crlf_change
            .hunks
            .iter()
            .flat_map(|hunk| &hunk.lines)
            .filter(|line| matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove))
            .map(|line| line.line_ending)
            .collect::<Vec<_>>();
        assert!(crlf_endings.contains(&FileLineEnding::CrLf));
        assert!(crlf_endings.contains(&FileLineEnding::Lf));
    }

    #[test]
    fn legacy_flat_diff_lines_deserialize_into_one_hunk() {
        let legacy = json!({
            "path": "legacy.txt",
            "kind": "modified",
            "added_lines": 1,
            "removed_lines": 1,
            "diff_lines": [
                {"kind": "remove", "old_line": 1, "content": "old"},
                {"kind": "add", "new_line": 1, "content": "new"}
            ],
            "truncated_changed_lines": 0
        });

        let change: FileChange = serde_json::from_value(legacy).expect("旧 journal 需兼容");
        assert_eq!(change.hunks.len(), 1);
        assert_eq!(change.hunks[0].lines.len(), 2);
        assert_eq!(
            change.hunks[0].lines[0].line_ending,
            FileLineEnding::Unknown
        );
        assert!(!change.approximate);
    }

    #[test]
    fn changed_budget_keeps_last_change_trailing_context() {
        let before = "before1\nbefore2\nold\nafter1\nafter2\nafter3\n";
        let after = "before1\nbefore2\nnew\nafter1\nafter2\nafter3\n";

        let change = compute_file_change("x.txt", FileChangeKind::Modified, before, after, 2)
            .expect("需产出 diff");

        assert_eq!(change.truncated_changed_lines, 0);
        assert!(all_lines(&change).any(|line| line.content == "after3"));
    }

    #[test]
    fn long_context_does_not_consume_changed_content_budget() {
        let long_context = "c".repeat(FILE_DIFF_MAX_LINE_CHARS);
        let long_change = "x".repeat(FILE_DIFF_MAX_LINE_CHARS);
        let before = format!("{long_context}\nold\n{long_context}\n");
        let after = format!("{long_context}\n{long_change}\n{long_context}\n");

        let change = compute_file_change("x.txt", FileChangeKind::Modified, &before, &after, 20)
            .expect("需产出 diff");
        let added = all_lines(&change)
            .find(|line| line.kind == FileDiffLineKind::Add)
            .expect("应有新增行");

        assert_eq!(added.content.chars().count(), FILE_DIFF_MAX_LINE_CHARS);
        assert!(!added.content_truncated);
    }

    #[test]
    fn dispersed_context_does_not_consume_changed_line_budget_or_create_orphan_hunks() {
        let mut before = String::new();
        let mut after = String::new();
        for index in 1..=320 {
            let line = format!("line-{index}\n");
            before.push_str(&line);
            after.push_str(&line);
            if index % 15 == 0 && index <= 300 {
                after.push_str(&format!("insert-{index}\n"));
            }
        }

        let change = compute_file_change("x.txt", FileChangeKind::Modified, &before, &after, 20)
            .expect("需产出 diff");

        assert_eq!(change.added_lines, 20);
        assert_eq!(change.removed_lines, 0);
        assert_eq!(change.truncated_changed_lines, 0);
        assert_eq!(
            all_lines(&change)
                .filter(|line| line.kind == FileDiffLineKind::Add)
                .count(),
            20
        );
        assert!(all_lines(&change).count() <= FILE_DIFF_MAX_RENDER_LINES);
        assert!(change
            .hunks
            .iter()
            .all(|hunk| hunk.lines.iter().any(|line| {
                matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove)
            })));
    }

    #[test]
    fn legacy_payload_is_normalized_to_current_bounds() {
        let huge = "x".repeat(FILE_DIFF_MAX_LINE_CHARS * 4);
        let legacy = json!({
            "path": "legacy.txt",
            "kind": "modified",
            "added_lines": 500,
            "removed_lines": 0,
            "diff_lines": (0..500).map(|index| json!({
                "kind": "add",
                "new_line": index + 1,
                "content": huge,
            })).collect::<Vec<_>>(),
            "truncated_changed_lines": 0
        });

        let change: FileChange = serde_json::from_value(legacy).expect("旧 journal 需兼容");

        assert!(all_lines(&change).count() <= FILE_DIFF_MAX_RENDER_LINES);
        assert!(
            all_lines(&change).all(|line| line.content.chars().count() <= FILE_DIFF_MAX_LINE_CHARS)
        );
        assert_eq!(change.truncated_changed_lines, 100);
        assert!(change.content_truncated);
        assert!(
            serde_json::to_vec(&change).expect("序列化").len() <= FILE_DIFF_MAX_SERIALIZED_BYTES
        );
    }

    #[test]
    fn escaped_control_content_stays_within_serialized_budget() {
        let controls = "\u{1b}".repeat(FILE_DIFF_MAX_LINE_CHARS);
        let before = format!("{controls}\nold\n");
        let after = format!("{controls}\nnew\n");

        let change =
            compute_file_change("control.txt", FileChangeKind::Modified, &before, &after, 20)
                .expect("需产出 diff");

        assert!(
            serde_json::to_vec(&change).expect("序列化").len() <= FILE_DIFF_MAX_SERIALIZED_BYTES
        );
    }
}
