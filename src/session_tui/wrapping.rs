//! TUI 文本折行工具。
//!
//! 本模块把 raw UTF-8 文本按终端可用宽度切成 visual line byte ranges。
//! 输入框和 transcript user cell 共用这里的结果，避免逻辑行与实际渲染行分叉。

use std::ops::Range;

use ratatui::text::{Line, Span};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct VisualLine {
    pub(super) range: Range<usize>,
    pub(super) logical_line_index: usize,
    pub(super) is_wrapped_continuation: bool,
}

pub(super) fn wrap_text_to_visual_lines(
    text: &str,
    terminal_width: u16,
    reserved_cols: usize,
) -> Vec<VisualLine> {
    let content_width = usize::from(terminal_width)
        .saturating_sub(reserved_cols)
        .max(1);
    let mut lines = Vec::new();

    for (logical_line_index, logical_range) in logical_line_ranges(text).into_iter().enumerate() {
        push_wrapped_logical_line(
            text,
            logical_range,
            logical_line_index,
            content_width,
            &mut lines,
        );
    }

    if lines.is_empty() {
        lines.push(VisualLine {
            range: 0..0,
            logical_line_index: 0,
            is_wrapped_continuation: false,
        });
    }
    lines
}

fn logical_line_ranges(text: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut start = 0usize;
    for (idx, ch) in text.char_indices() {
        if ch == '\n' {
            ranges.push(start..idx);
            start = idx.saturating_add(ch.len_utf8());
        }
    }
    ranges.push(start..text.len());
    ranges
}

pub(super) fn hard_wrap_styled_lines(
    lines: Vec<Line<'static>>,
    width: usize,
) -> Vec<Line<'static>> {
    let width = width.max(1);
    let mut wrapped = Vec::new();
    for line in lines {
        wrapped.extend(hard_wrap_styled_line(line, width));
    }
    wrapped
}

fn hard_wrap_styled_line(line: Line<'static>, width: usize) -> Vec<Line<'static>> {
    if line.width() <= width || line.spans.is_empty() {
        return vec![line];
    }

    let line_style = line.style;
    let mut out = Vec::new();
    let mut current_spans = Vec::new();
    let mut current_width = 0usize;

    for span in line.spans {
        let style = span.style;
        let content = span.content.into_owned();
        let mut chunk = String::new();
        for grapheme in content.graphemes(true) {
            let grapheme_width = UnicodeWidthStr::width(grapheme);
            if current_width > 0 && current_width.saturating_add(grapheme_width) > width {
                if !chunk.is_empty() {
                    current_spans.push(Span::styled(std::mem::take(&mut chunk), style));
                }
                out.push(Line::from(std::mem::take(&mut current_spans)).style(line_style));
                current_width = 0;
            }
            chunk.push_str(grapheme);
            current_width = current_width.saturating_add(grapheme_width);
        }
        if !chunk.is_empty() {
            current_spans.push(Span::styled(chunk, style));
        }
    }

    if current_spans.is_empty() {
        out.push(Line::default().style(line_style));
    } else {
        out.push(Line::from(current_spans).style(line_style));
    }
    out
}

fn push_wrapped_logical_line(
    text: &str,
    range: Range<usize>,
    logical_line_index: usize,
    content_width: usize,
    out: &mut Vec<VisualLine>,
) {
    if range.is_empty() {
        out.push(VisualLine {
            range,
            logical_line_index,
            is_wrapped_continuation: false,
        });
        return;
    }

    let mut line_start = range.start;
    let mut line_width = 0usize;
    let mut emitted_for_logical_line = false;

    for (relative_idx, grapheme) in text[range.clone()].grapheme_indices(true) {
        let byte_idx = range.start + relative_idx;
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if line_width > 0 && line_width.saturating_add(grapheme_width) > content_width {
            out.push(VisualLine {
                range: line_start..byte_idx,
                logical_line_index,
                is_wrapped_continuation: emitted_for_logical_line,
            });
            emitted_for_logical_line = true;
            line_start = byte_idx;
            line_width = 0;
        }
        line_width = line_width.saturating_add(grapheme_width);
    }

    out.push(VisualLine {
        range: line_start..range.end,
        logical_line_index,
        is_wrapped_continuation: emitted_for_logical_line,
    });
}
