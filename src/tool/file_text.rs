//! 文本文件的有界分页扫描，以及局部写入后的已知字节范围迁移。

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use ring::digest::{Context as DigestContext, SHA256};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;

use super::{
    bounded_text_byte_limit, ContentRevision, LineRange, ReadAuthority, ReadEvidence, ToolError,
};

const READ_BUFFER_BYTES: usize = 16 * 1024;

pub(super) struct TextPageRequest<'a> {
    pub(super) display_path: &'a str,
    pub(super) canonical_path: PathBuf,
    pub(super) start: usize,
    pub(super) count: usize,
    pub(super) keyword: Option<&'a str>,
    pub(super) show_linenos: bool,
    pub(super) max_chars: usize,
}

pub(super) struct TextPageResult {
    pub(super) output: Value,
    pub(super) evidence: Option<ReadEvidence>,
}

#[derive(Debug)]
struct ScannedLine {
    number: usize,
    text: Option<String>,
}

#[derive(Debug)]
struct ScanResult {
    revision: ContentRevision,
    total_lines: usize,
    ends_with_newline: bool,
    selected: Vec<ScannedLine>,
    keyword_match_line: Option<usize>,
    blocking_long_line: Option<usize>,
    selection_limited: bool,
}

#[derive(Debug)]
struct PageCollector {
    start: usize,
    count: usize,
    keyword: Option<String>,
    before: VecDeque<ScannedLine>,
    selected: Vec<ScannedLine>,
    matched_line: Option<usize>,
    blocking_long_line: Option<usize>,
    buffered_bytes: usize,
    before_bytes: usize,
    max_buffered_bytes: usize,
    selection_limited: bool,
}

impl PageCollector {
    fn new(request: &TextPageRequest<'_>) -> Self {
        // 即使模型传入极大的 count，候选行也不能随文件行数无界增长。
        // 每条完整逻辑行至少贡献一个换行字符（末行例外），
        // 因此 max_chars + 1 已足够供渲染阶段判断截断原因。
        let bounded_count = request
            .count
            .min(request.max_chars.saturating_add(1))
            .max(1);
        let max_buffered_bytes = bounded_text_byte_limit(request.max_chars)
            .saturating_mul(2)
            .max(8);
        Self {
            start: request.start,
            count: bounded_count,
            keyword: request.keyword.map(str::to_ascii_lowercase),
            before: VecDeque::new(),
            selected: Vec::new(),
            matched_line: None,
            blocking_long_line: None,
            buffered_bytes: 0,
            before_bytes: 0,
            max_buffered_bytes,
            selection_limited: false,
        }
    }

    fn observe(&mut self, line: ScannedLine) {
        if self.keyword.is_none() {
            let end = self.start.saturating_add(self.count.saturating_sub(1));
            if (self.start..=end).contains(&line.number) {
                self.push_selected(line);
            }
            return;
        }

        if self.matched_line.is_some() {
            self.push_selected(line);
            return;
        }
        if self.blocking_long_line.is_some() {
            // 早先的超长行无法完整检查 keyword，继续越过它寻找后续命中会让
            // 返回范围产生未经观察的缺口，因此保守停在该行。
            return;
        }
        if line.number < self.start {
            if line.text.is_some() {
                self.push_before(line);
            }
            return;
        }
        let Some(text) = line.text.as_deref() else {
            // 无法完整返回的超长行也无法安全判定 keyword 是否命中；不要越过它谎报 miss。
            self.blocking_long_line = Some(line.number);
            return;
        };
        let matches = text
            .to_ascii_lowercase()
            .contains(self.keyword.as_deref().unwrap_or_default());
        if matches {
            self.matched_line = Some(line.number);
            let match_bytes = scanned_line_bytes(&line);
            while self.before.len().saturating_add(1) > self.count
                || self
                    .before_bytes
                    .saturating_add(match_bytes)
                    .saturating_add(self.buffered_bytes)
                    > self.max_buffered_bytes
            {
                let Some(dropped) = self.before.pop_front() else {
                    break;
                };
                self.before_bytes = self
                    .before_bytes
                    .saturating_sub(scanned_line_bytes(&dropped));
            }
            for context in self.before.drain(..) {
                self.buffered_bytes = self
                    .buffered_bytes
                    .saturating_add(scanned_line_bytes(&context));
                self.selected.push(context);
            }
            self.before_bytes = 0;
            self.push_selected(line);
        } else {
            self.push_before(line);
        }
    }

    fn push_before(&mut self, line: ScannedLine) {
        let keep = self.count / 3;
        if keep == 0 {
            return;
        }
        self.before_bytes = self.before_bytes.saturating_add(scanned_line_bytes(&line));
        self.before.push_back(line);
        while self.before.len() > keep || self.before_bytes > self.max_buffered_bytes {
            let Some(dropped) = self.before.pop_front() else {
                break;
            };
            self.before_bytes = self
                .before_bytes
                .saturating_sub(scanned_line_bytes(&dropped));
        }
    }

    fn push_selected(&mut self, line: ScannedLine) {
        if self.selected.len() >= self.count {
            return;
        }
        let bytes = scanned_line_bytes(&line);
        if !self.selected.is_empty()
            && self.buffered_bytes.saturating_add(bytes) > self.max_buffered_bytes
        {
            self.selection_limited = true;
            return;
        }
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes);
        self.selected.push(line);
    }
}

fn scanned_line_bytes(line: &ScannedLine) -> usize {
    line.text.as_ref().map_or(0, String::len)
}

#[derive(Default)]
struct Utf8Validator {
    pending: Vec<u8>,
}

impl Utf8Validator {
    fn push(&mut self, bytes: &[u8]) -> Result<(), ToolError> {
        if self.pending.is_empty() {
            return self.validate(bytes);
        }
        let mut combined = std::mem::take(&mut self.pending);
        combined.extend_from_slice(bytes);
        self.validate(&combined)
    }

    fn validate(&mut self, bytes: &[u8]) -> Result<(), ToolError> {
        match std::str::from_utf8(bytes) {
            Ok(_) => Ok(()),
            Err(error) if error.error_len().is_none() => {
                self.pending
                    .extend_from_slice(&bytes[error.valid_up_to()..]);
                Ok(())
            }
            Err(error) => Err(invalid_utf8(error)),
        }
    }

    fn finish(self) -> Result<(), ToolError> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(ToolError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "文本文件末尾包含不完整的 UTF-8 字符",
            )))
        }
    }
}

fn invalid_utf8(error: std::str::Utf8Error) -> ToolError {
    ToolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

pub(super) async fn read_text_page(
    path: &Path,
    request: TextPageRequest<'_>,
) -> Result<TextPageResult, ToolError> {
    let scan = scan_text_file(path, &request).await?;
    let mut content = String::new();
    let mut returned_start = None;
    let mut returned_end = None;
    let mut stop_reason = None::<&'static str>;
    let mut content_chars = 0usize;

    for line in &scan.selected {
        let Some(text) = line.text.as_deref() else {
            stop_reason = Some("single_line_too_long");
            break;
        };
        let rendered = if request.show_linenos {
            format!("{}|{text}", line.number)
        } else {
            text.to_string()
        };
        let rendered_chars = rendered.chars().count();
        let next_chars = content_chars.saturating_add(rendered_chars);
        if next_chars > request.max_chars {
            stop_reason = Some(if content.is_empty() {
                "single_line_too_long"
            } else {
                "max_chars"
            });
            break;
        }
        returned_start.get_or_insert(line.number);
        returned_end = Some(line.number);
        content_chars = next_chars;
        content.push_str(&rendered);
    }

    if stop_reason.is_none() && returned_end.is_none() {
        stop_reason = if let Some(line) = scan.blocking_long_line {
            if line >= request.start {
                Some("single_line_too_long")
            } else {
                None
            }
        } else if request.start > scan.total_lines && !(scan.total_lines == 0 && request.start == 1)
        {
            Some("start_after_eof")
        } else if request.keyword.is_some() && scan.keyword_match_line.is_none() {
            Some("keyword_not_found")
        } else {
            Some("eof")
        };
    }

    if stop_reason.is_none() && scan.selection_limited {
        stop_reason = Some(if returned_end.is_some() {
            "max_chars"
        } else {
            "single_line_too_long"
        });
    }

    let reaches_eof = returned_end == Some(scan.total_lines)
        || (scan.total_lines == 0
            && request.start == 1
            && request.keyword.is_none()
            && stop_reason == Some("eof"));
    if stop_reason.is_none() {
        stop_reason = Some(if reaches_eof { "eof" } else { "count" });
    }
    let stop_reason = stop_reason.unwrap_or("eof");
    let next_start = match stop_reason {
        "keyword_not_found" | "start_after_eof" | "eof" => None,
        "single_line_too_long" if returned_end.is_none() => scan
            .blocking_long_line
            .or_else(|| scan.selected.first().map(|line| line.number)),
        _ => returned_end
            .map(|line| line.saturating_add(1))
            .or(Some(request.start)),
    };
    let truncated = !reaches_eof && !matches!(stop_reason, "keyword_not_found" | "start_after_eof");
    let page = json!({
        "returned_start": returned_start,
        "returned_end": returned_end,
        "total_lines": scan.total_lines,
        "next_start": next_start,
        "reaches_eof": reaches_eof,
        "ends_with_newline": reaches_eof.then_some(scan.ends_with_newline),
        "keyword_match_line": scan.keyword_match_line,
        "stop_reason": stop_reason,
    });
    let output = json!({
        "path": request.display_path,
        "content": content,
        "truncated": truncated,
        "page": page,
    });
    let range = returned_start
        .zip(returned_end)
        .and_then(|(start, end)| LineRange::new(start, end));
    let complete = reaches_eof && (scan.total_lines == 0 || returned_start == Some(1));
    let evidence = if matches!(stop_reason, "keyword_not_found" | "start_after_eof") {
        None
    } else if range.is_some() || complete {
        Some(ReadEvidence::scanned(
            request.canonical_path,
            scan.revision,
            scan.total_lines,
            scan.ends_with_newline,
            range.into_iter().collect(),
            complete,
        ))
    } else {
        None
    };

    Ok(TextPageResult { output, evidence })
}

async fn scan_text_file(
    path: &Path,
    request: &TextPageRequest<'_>,
) -> Result<ScanResult, ToolError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut buffer = vec![0u8; READ_BUFFER_BYTES];
    let mut digest = DigestContext::new(&SHA256);
    let mut validator = Utf8Validator::default();
    let mut collector = PageCollector::new(request);
    let capture_limit = bounded_text_byte_limit(request.max_chars).max(4);
    let mut current = Vec::<u8>::new();
    let mut current_too_long = false;
    let mut total_bytes = 0u64;
    let mut line_number = 1usize;
    let mut saw_any = false;
    let mut ends_with_newline = false;

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let chunk = &buffer[..read];
        saw_any = true;
        total_bytes = total_bytes.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        digest.update(chunk);
        validator.push(chunk)?;
        for byte in chunk {
            if !current_too_long {
                if current.len() < capture_limit {
                    current.push(*byte);
                } else {
                    current.clear();
                    current_too_long = true;
                }
            }
            if *byte == b'\n' {
                let text = if current_too_long {
                    None
                } else {
                    Some(String::from_utf8(current).map_err(|error| {
                        ToolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
                    })?)
                };
                collector.observe(ScannedLine {
                    number: line_number,
                    text,
                });
                line_number = line_number.saturating_add(1);
                current = Vec::new();
                current_too_long = false;
                ends_with_newline = true;
            } else {
                ends_with_newline = false;
            }
        }
    }
    validator.finish()?;
    if saw_any && !ends_with_newline {
        let text = if current_too_long {
            None
        } else {
            Some(String::from_utf8(current).map_err(|error| {
                ToolError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
            })?)
        };
        collector.observe(ScannedLine {
            number: line_number,
            text,
        });
    }
    let total_lines = if !saw_any {
        0
    } else if ends_with_newline {
        line_number.saturating_sub(1)
    } else {
        line_number
    };
    let sha256 = hex::encode(digest.finish().as_ref());
    Ok(ScanResult {
        revision: ContentRevision::from_sha256(sha256, total_bytes),
        total_lines,
        ends_with_newline,
        selected: collector.selected,
        keyword_match_line: collector.matched_line,
        blocking_long_line: collector.blocking_long_line,
        selection_limited: collector.selection_limited,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CoverageMigration {
    pub(super) ranges: Vec<LineRange>,
    pub(super) complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteSpan {
    start: usize,
    end: usize,
}

pub(super) fn migrate_edit_coverage(
    before: &str,
    after: &str,
    authority: &ReadAuthority,
    edit_start: usize,
    edit_end: usize,
    replacement_len: usize,
) -> Result<CoverageMigration, LineRange> {
    if authority.complete {
        return Ok(CoverageMigration {
            ranges: LineRange::new(1, super::read_state::logical_line_count(after))
                .into_iter()
                .collect(),
            complete: true,
        });
    }
    let before_lines = line_byte_spans(before);
    let affected = line_range_for_byte_range(&before_lines, edit_start, edit_end)
        .unwrap_or_else(|| LineRange::new(1, authority.total_lines.max(1)).expect("合法范围"));
    if !authority.covers(affected.start, affected.end) {
        return Err(expand_required_range(affected, authority.total_lines));
    }

    let known_before = known_byte_spans(&before_lines, &authority.ranges);
    if !span_is_covered(&known_before, edit_start, edit_end) {
        return Err(expand_required_range(affected, authority.total_lines));
    }
    let mut known_after = transform_spans(&known_before, edit_start, edit_end, replacement_len);
    merge_byte_spans(&mut known_after);
    let after_lines = line_byte_spans(after);
    let new_end = edit_start.saturating_add(replacement_len);
    for line in touched_result_lines(&after_lines, edit_start, new_end) {
        if !span_is_covered(&known_after, line.start, line.end) {
            return Err(expand_required_range(affected, authority.total_lines));
        }
    }
    let ranges = project_known_line_ranges(&after_lines, &known_after);
    let complete = if after.is_empty() {
        true
    } else {
        ranges.len() == 1 && ranges[0].start == 1 && ranges[0].end == after_lines.len()
    };
    Ok(CoverageMigration { ranges, complete })
}

pub(super) fn suggested_read_range(text: &str, edit_start: usize, edit_end: usize) -> LineRange {
    let lines = line_byte_spans(text);
    let total_lines = lines.len();
    let affected = line_range_for_byte_range(&lines, edit_start, edit_end)
        .unwrap_or_else(|| LineRange::new(1, total_lines.max(1)).expect("合法范围"));
    expand_required_range(affected, total_lines)
}

fn line_byte_spans(text: &str) -> Vec<ByteSpan> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut spans = Vec::new();
    let mut start = 0usize;
    for (index, byte) in text.bytes().enumerate() {
        if byte == b'\n' {
            spans.push(ByteSpan {
                start,
                end: index.saturating_add(1),
            });
            start = index.saturating_add(1);
        }
    }
    if start < text.len() {
        spans.push(ByteSpan {
            start,
            end: text.len(),
        });
    }
    spans
}

fn line_range_for_byte_range(lines: &[ByteSpan], start: usize, end: usize) -> Option<LineRange> {
    if lines.is_empty() {
        return None;
    }
    let first = lines
        .iter()
        .position(|line| start < line.end)
        .unwrap_or(lines.len().saturating_sub(1));
    let last_byte = end.saturating_sub(1).max(start);
    let last = lines
        .iter()
        .position(|line| last_byte < line.end)
        .unwrap_or(lines.len().saturating_sub(1));
    LineRange::new(first.saturating_add(1), last.saturating_add(1))
}

fn known_byte_spans(lines: &[ByteSpan], ranges: &[LineRange]) -> Vec<ByteSpan> {
    let mut spans = Vec::new();
    for range in ranges {
        let Some(first) = lines.get(range.start.saturating_sub(1)) else {
            continue;
        };
        let Some(last) = lines.get(range.end.saturating_sub(1)) else {
            continue;
        };
        spans.push(ByteSpan {
            start: first.start,
            end: last.end,
        });
    }
    merge_byte_spans(&mut spans);
    spans
}

fn transform_spans(
    spans: &[ByteSpan],
    edit_start: usize,
    edit_end: usize,
    replacement_len: usize,
) -> Vec<ByteSpan> {
    let removed = edit_end.saturating_sub(edit_start);
    let mut result = Vec::new();
    for span in spans {
        if span.start < edit_start {
            result.push(ByteSpan {
                start: span.start,
                end: span.end.min(edit_start),
            });
        }
        if span.end > edit_end {
            let suffix_start = span.start.max(edit_end);
            result.push(ByteSpan {
                start: shift_offset(suffix_start, removed, replacement_len),
                end: shift_offset(span.end, removed, replacement_len),
            });
        }
    }
    if replacement_len > 0 {
        result.push(ByteSpan {
            start: edit_start,
            end: edit_start.saturating_add(replacement_len),
        });
    }
    result.retain(|span| span.start < span.end);
    result
}

fn shift_offset(offset: usize, removed: usize, replacement_len: usize) -> usize {
    offset
        .saturating_sub(removed)
        .saturating_add(replacement_len)
}

fn merge_byte_spans(spans: &mut Vec<ByteSpan>) {
    spans.sort_by_key(|span| (span.start, span.end));
    let mut merged = Vec::<ByteSpan>::new();
    for span in spans.drain(..) {
        if let Some(last) = merged.last_mut() {
            if span.start <= last.end {
                last.end = last.end.max(span.end);
                continue;
            }
        }
        merged.push(span);
    }
    *spans = merged;
}

fn span_is_covered(spans: &[ByteSpan], start: usize, end: usize) -> bool {
    start == end
        || spans
            .iter()
            .any(|span| span.start <= start && end <= span.end)
}

fn touched_result_lines(lines: &[ByteSpan], start: usize, end: usize) -> Vec<ByteSpan> {
    lines
        .iter()
        .copied()
        .filter(|line| {
            if start == end {
                line.start <= start && start <= line.end
            } else {
                line.start < end && start < line.end
            }
        })
        .collect()
}

fn project_known_line_ranges(lines: &[ByteSpan], spans: &[ByteSpan]) -> Vec<LineRange> {
    let mut ranges = Vec::<LineRange>::new();
    for (index, line) in lines.iter().enumerate() {
        if span_is_covered(spans, line.start, line.end) {
            let number = index.saturating_add(1);
            if let Some(last) = ranges.last_mut() {
                if last.end.saturating_add(1) == number {
                    last.end = number;
                    continue;
                }
            }
            if let Some(range) = LineRange::new(number, number) {
                ranges.push(range);
            }
        }
    }
    ranges
}

fn expand_required_range(range: LineRange, total_lines: usize) -> LineRange {
    LineRange {
        start: range.start.saturating_sub(1).max(1),
        end: range.end.saturating_add(1).min(total_lines.max(1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn page_preserves_crlf_and_reports_eof() {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let path = dir.path().join("sample.txt");
        tokio::fs::write(&path, b"one\r\ntwo\r\n")
            .await
            .expect("写入测试文件");
        let result = read_text_page(
            &path,
            TextPageRequest {
                display_path: "sample.txt",
                canonical_path: path.clone(),
                start: 1,
                count: 2,
                keyword: None,
                show_linenos: false,
                max_chars: 100,
            },
        )
        .await
        .expect("读取成功");
        assert_eq!(result.output["content"], "one\r\ntwo\r\n");
        assert_eq!(result.output["page"]["total_lines"], 2);
        assert_eq!(result.output["page"]["reaches_eof"], true);
        assert_eq!(result.output["page"]["ends_with_newline"], true);
        assert!(result.evidence.expect("有证据").complete);
    }

    #[tokio::test]
    async fn page_reports_keyword_miss_and_start_after_eof_without_evidence() {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let path = dir.path().join("sample.txt");
        tokio::fs::write(&path, b"one\ntwo\n")
            .await
            .expect("写入测试文件");
        let keyword = read_text_page(
            &path,
            TextPageRequest {
                display_path: "sample.txt",
                canonical_path: path.clone(),
                start: 1,
                count: 2,
                keyword: Some("absent"),
                show_linenos: false,
                max_chars: 100,
            },
        )
        .await
        .expect("读取成功");
        assert_eq!(keyword.output["content"], "");
        assert_eq!(keyword.output["page"]["stop_reason"], "keyword_not_found");
        assert!(keyword.evidence.is_none());

        let keyword_after_eof = read_text_page(
            &path,
            TextPageRequest {
                display_path: "sample.txt",
                canonical_path: path.clone(),
                start: 3,
                count: 1,
                keyword: Some("absent"),
                show_linenos: false,
                max_chars: 100,
            },
        )
        .await
        .expect("读取成功");
        assert_eq!(
            keyword_after_eof.output["page"]["stop_reason"],
            "start_after_eof"
        );
        assert!(keyword_after_eof.evidence.is_none());

        let after_eof = read_text_page(
            &path,
            TextPageRequest {
                display_path: "sample.txt",
                canonical_path: path.clone(),
                start: 3,
                count: 1,
                keyword: None,
                show_linenos: false,
                max_chars: 100,
            },
        )
        .await
        .expect("读取成功");
        assert_eq!(after_eof.output["page"]["stop_reason"], "start_after_eof");
        assert_eq!(after_eof.output["truncated"], false);
        assert!(after_eof.evidence.is_none());
    }

    #[tokio::test]
    async fn empty_file_is_complete_only_from_first_line() {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let path = dir.path().join("empty.txt");
        tokio::fs::write(&path, b"").await.expect("写入测试文件");
        let first = read_text_page(
            &path,
            TextPageRequest {
                display_path: "empty.txt",
                canonical_path: path.clone(),
                start: 1,
                count: 1,
                keyword: None,
                show_linenos: false,
                max_chars: 100,
            },
        )
        .await
        .expect("读取成功");
        assert_eq!(first.output["page"]["total_lines"], 0);
        assert_eq!(first.output["page"]["reaches_eof"], true);
        assert!(first.evidence.expect("空文件完整证据").complete);

        let after = read_text_page(
            &path,
            TextPageRequest {
                display_path: "empty.txt",
                canonical_path: path.clone(),
                start: 2,
                count: 1,
                keyword: None,
                show_linenos: false,
                max_chars: 100,
            },
        )
        .await
        .expect("读取成功");
        assert_eq!(after.output["page"]["stop_reason"], "start_after_eof");
        assert!(after.evidence.is_none());
    }

    #[tokio::test]
    async fn invalid_utf8_is_rejected_even_beyond_requested_page() {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let path = dir.path().join("invalid.txt");
        tokio::fs::write(&path, b"valid\n\xff\n")
            .await
            .expect("写入测试文件");
        let result = read_text_page(
            &path,
            TextPageRequest {
                display_path: "invalid.txt",
                canonical_path: path.clone(),
                start: 1,
                count: 1,
                keyword: None,
                show_linenos: false,
                max_chars: 100,
            },
        )
        .await;
        assert!(
            matches!(result, Err(ToolError::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData)
        );
    }

    #[tokio::test]
    async fn scan_candidate_memory_is_bounded_by_page_limit() {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let path = dir.path().join("many-wide-lines.txt");
        let content = format!("{}\n", "a".repeat(30)).repeat(100);
        tokio::fs::write(&path, content)
            .await
            .expect("写入测试文件");
        let request = TextPageRequest {
            display_path: "many-wide-lines.txt",
            canonical_path: path.clone(),
            start: 1,
            count: 100,
            keyword: None,
            show_linenos: false,
            max_chars: 10,
        };

        let scan = scan_text_file(&path, &request).await.expect("扫描成功");

        assert!(scan.selection_limited);
        assert!(scan.selected.len() <= 3);
    }

    #[test]
    fn deleting_newline_next_to_unknown_line_is_rejected() {
        let before = "known\nunknown\n";
        let after = "knownunknown\n";
        let authority = ReadAuthority {
            total_lines: 2,
            ends_with_newline: true,
            ranges: vec![LineRange::new(1, 1).expect("合法范围")],
            complete: false,
        };
        let result = migrate_edit_coverage(before, after, &authority, 5, 6, 0);
        assert_eq!(result, Err(LineRange::new(1, 2).expect("合法范围")));
    }

    #[test]
    fn local_edit_preserves_unrelated_known_ranges() {
        let before = "one\ntwo\nthree\nfour\n";
        let after = "one\nTWO\nthree\nfour\n";
        let authority = ReadAuthority {
            total_lines: 4,
            ends_with_newline: true,
            ranges: vec![
                LineRange::new(1, 2).expect("合法范围"),
                LineRange::new(4, 4).expect("合法范围"),
            ],
            complete: false,
        };
        let migrated =
            migrate_edit_coverage(before, after, &authority, 4, 7, 3).expect("局部修改可迁移");
        assert_eq!(
            migrated.ranges,
            vec![
                LineRange::new(1, 2).expect("合法范围"),
                LineRange::new(4, 4).expect("合法范围")
            ]
        );
        assert!(!migrated.complete);
    }
}
