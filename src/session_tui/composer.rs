//! TUI 输入框状态。
//!
//! `ComposerState` 维护当前输入文本、UTF-8 光标位置和按终端宽度折行后的
//! visual line 缓存。发送、排队等 session 行为由上层状态机处理。

use std::cell::RefCell;

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::word_nav::{next_word_boundary, prev_word_boundary};
use super::wrapping::{wrap_text_to_visual_lines, VisualLine};

#[derive(Debug, Clone, Default)]
pub(super) struct ComposerState {
    text: String,
    cursor: usize,
    revision: u64,
    wrap_cache: RefCell<Option<ComposerWrapCache>>,
}

#[derive(Debug, Clone)]
struct ComposerWrapCache {
    revision: u64,
    width: u16,
    lines: Vec<VisualLine>,
}

impl ComposerState {
    pub(super) fn input(&self) -> &str {
        &self.text
    }

    /// 光标在输入文本中的字节偏移（附件命中判定用）。
    pub(super) fn cursor_byte_index(&self) -> usize {
        self.cursor
    }

    pub(super) fn push_char(&mut self, c: char) {
        self.text.insert(self.cursor, c);
        self.cursor += c.len_utf8();
        self.mark_dirty();
    }

    pub(super) fn push_text(&mut self, text: &str) {
        self.text.insert_str(self.cursor, text);
        self.cursor += text.len();
        self.mark_dirty();
    }

    pub(super) fn push_newline(&mut self) {
        self.push_char('\n');
    }

    /// 替换 UTF-8 字节区间，并把光标放到替换文本末尾。
    pub(super) fn replace_range(
        &mut self,
        range: std::ops::Range<usize>,
        replacement: &str,
    ) -> bool {
        if range.start > range.end
            || range.end > self.text.len()
            || !self.text.is_char_boundary(range.start)
            || !self.text.is_char_boundary(range.end)
        {
            return false;
        }
        self.text.replace_range(range.clone(), replacement);
        self.cursor = range.start.saturating_add(replacement.len());
        self.mark_dirty();
        true
    }

    pub(super) fn pop_char(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let Some((previous, _)) = self.text[..self.cursor].char_indices().last() else {
            return;
        };
        self.text.drain(previous..self.cursor);
        self.cursor = previous;
        self.mark_dirty();
    }

    pub(super) fn delete_char(&mut self) {
        if self.cursor >= self.text.len() {
            return;
        }
        let next = self.next_cursor_position();
        self.text.drain(self.cursor..next);
        self.mark_dirty();
    }

    pub(super) fn move_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        if let Some((previous, _)) = self.text[..self.cursor].char_indices().last() {
            self.cursor = previous;
        }
    }

    pub(super) fn move_right(&mut self) {
        self.cursor = self.next_cursor_position();
    }

    /// Option+←：跳到光标左侧最近的词首（词边界规则见 `word_nav`）。
    pub(super) fn move_word_left(&mut self) {
        self.cursor = prev_word_boundary(&self.text, self.cursor);
    }

    /// Option+→：跳到光标右侧最近的词首，末词之后落到文本末尾。
    pub(super) fn move_word_right(&mut self) {
        self.cursor = next_word_boundary(&self.text, self.cursor);
    }

    pub(super) fn move_home(&mut self) {
        self.cursor = 0;
    }

    pub(super) fn move_end(&mut self) {
        self.cursor = self.text.len();
    }

    pub(super) fn move_up(&mut self, width: u16) -> bool {
        self.move_vertical(width, -1)
    }

    pub(super) fn move_down(&mut self, width: u16) -> bool {
        self.move_vertical(width, 1)
    }

    pub(super) fn cursor_at_end(&self) -> bool {
        self.cursor >= self.text.len()
    }

    pub(super) fn take(&mut self) -> String {
        self.cursor = 0;
        self.mark_dirty();
        std::mem::take(&mut self.text)
    }

    pub(super) fn set_text(&mut self, text: String) {
        self.cursor = text.len();
        self.text = text;
        self.mark_dirty();
    }

    pub(super) fn wrapped_lines(&self, width: u16) -> Vec<VisualLine> {
        let width = width.max(1);
        if let Some(cache) = self.wrap_cache.borrow().as_ref() {
            if cache.revision == self.revision && cache.width == width {
                return cache.lines.clone();
            }
        }

        let lines = wrap_text_to_visual_lines(&self.text, width, 2);
        *self.wrap_cache.borrow_mut() = Some(ComposerWrapCache {
            revision: self.revision,
            width,
            lines: lines.clone(),
        });
        lines
    }

    pub(super) fn cursor_visual_position(&self, width: u16) -> (usize, usize) {
        let lines = self.wrapped_lines(width);
        let line_index = lines
            .iter()
            .enumerate()
            .find_map(|(index, line)| {
                if line.range.is_empty() {
                    (self.cursor == line.range.start).then_some(index)
                } else {
                    (self.cursor >= line.range.start && self.cursor < line.range.end)
                        .then_some(index)
                }
            })
            .or_else(|| {
                lines
                    .iter()
                    .enumerate()
                    .find_map(|(index, line)| (self.cursor == line.range.end).then_some(index))
            })
            .unwrap_or_else(|| lines.len().saturating_sub(1));
        let line = match lines.get(line_index) {
            Some(line) => line,
            None => return (0, 0),
        };
        let cursor = self.cursor.clamp(line.range.start, line.range.end);
        let col = UnicodeWidthStr::width(&self.text[line.range.start..cursor]);
        (line_index, col)
    }

    fn next_cursor_position(&self) -> usize {
        if self.cursor >= self.text.len() {
            return self.text.len();
        }
        let mut chars = self.text[self.cursor..].char_indices();
        let _current = chars.next();
        chars
            .next()
            .map(|(offset, _)| self.cursor + offset)
            .unwrap_or(self.text.len())
    }

    fn move_vertical(&mut self, width: u16, delta: isize) -> bool {
        let lines = self.wrapped_lines(width);
        let (line_index, visual_col) = self.cursor_visual_position(width);
        let target_index = if delta < 0 {
            line_index.checked_sub(1)
        } else {
            line_index
                .checked_add(1)
                .filter(|index| *index < lines.len())
        };
        let Some(target_index) = target_index else {
            return false;
        };
        let Some(target_line) = lines.get(target_index) else {
            return false;
        };
        let next_cursor = self.cursor_for_visual_col(target_line, visual_col);
        if next_cursor == self.cursor {
            return false;
        }
        self.cursor = next_cursor;
        true
    }

    fn cursor_for_visual_col(&self, line: &VisualLine, visual_col: usize) -> usize {
        let Some(text) = self.text.get(line.range.clone()) else {
            return line.range.end;
        };
        let mut used = 0usize;
        let mut cursor = line.range.start;
        for (relative_idx, grapheme) in text.grapheme_indices(true) {
            let byte_idx = line.range.start.saturating_add(relative_idx);
            let grapheme_width = UnicodeWidthStr::width(grapheme);
            if used.saturating_add(grapheme_width) > visual_col {
                return byte_idx;
            }
            used = used.saturating_add(grapheme_width);
            cursor = byte_idx.saturating_add(grapheme.len());
            if used >= visual_col {
                return cursor;
            }
        }
        cursor.min(line.range.end)
    }

    fn mark_dirty(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        *self.wrap_cache.get_mut() = None;
    }
}
