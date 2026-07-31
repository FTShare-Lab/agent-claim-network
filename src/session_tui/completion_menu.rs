//! 通用输入候选菜单：选择状态、5 行滚动窗口与单行渲染。
//!
//! Slash command 与 `@path` 文件候选共用本模块，业务侧只需提供主标签和说明文本；
//! 候选查询、接受后的文本替换仍由各自模块负责。

use ratatui::style::Modifier;
use ratatui::text::{Line, Span};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use super::theme::{blue_style, muted_style};

/// 输入候选菜单一次最多显示的行数。
pub(super) const COMPLETION_MENU_MAX_VISIBLE: usize = 5;

/// 可由通用候选菜单渲染的条目。
pub(super) trait CompletionMenuEntry {
    fn label(&self) -> &str;
    fn description(&self) -> &str;
}

/// 候选菜单当前选择与可见窗口状态。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct CompletionMenuState {
    selected_index: usize,
    window_start: usize,
}

impl CompletionMenuState {
    pub(super) fn selected_index(&self) -> usize {
        self.selected_index
    }

    pub(super) fn window_start(&self) -> usize {
        self.window_start
    }

    pub(super) fn reset(&mut self) {
        self.selected_index = 0;
        self.window_start = 0;
    }

    pub(super) fn select_previous(&mut self, item_count: usize) -> bool {
        if item_count == 0 {
            return false;
        }
        self.selected_index = if self.selected_index == 0 {
            item_count.saturating_sub(1)
        } else {
            self.selected_index.saturating_sub(1)
        };
        self.follow_selection(item_count);
        true
    }

    pub(super) fn select_next(&mut self, item_count: usize) -> bool {
        if item_count == 0 {
            return false;
        }
        self.selected_index = (self.selected_index + 1) % item_count;
        self.follow_selection(item_count);
        true
    }

    pub(super) fn selected<'a, T>(&self, items: &'a [T]) -> Option<&'a T> {
        items.get(self.selected_index.min(items.len().saturating_sub(1)))
    }

    fn follow_selection(&mut self, item_count: usize) {
        self.window_start =
            completion_menu_window_start(item_count, self.selected_index, self.window_start);
    }
}

/// 把当前选中项收进候选菜单的可见窗口。
pub(super) fn completion_menu_window_start(
    item_count: usize,
    selected_index: usize,
    window_start: usize,
) -> usize {
    if item_count <= COMPLETION_MENU_MAX_VISIBLE {
        return 0;
    }
    let max_start = item_count - COMPLETION_MENU_MAX_VISIBLE;
    let start = window_start.min(max_start);
    if selected_index < start {
        selected_index
    } else if selected_index >= start + COMPLETION_MENU_MAX_VISIBLE {
        selected_index + 1 - COMPLETION_MENU_MAX_VISIBLE
    } else {
        start
    }
}

/// 用统一样式渲染候选菜单，并保证每个候选只占一行。
pub(super) fn render_completion_menu<T: CompletionMenuEntry>(
    items: &[T],
    state: &CompletionMenuState,
    width: u16,
) -> Vec<Line<'static>> {
    if items.is_empty() {
        return Vec::new();
    }

    let selected_index = state.selected_index().min(items.len().saturating_sub(1));
    let window_start =
        completion_menu_window_start(items.len(), selected_index, state.window_start());
    let visible = items
        .iter()
        .enumerate()
        .skip(window_start)
        .take(COMPLETION_MENU_MAX_VISIBLE)
        .collect::<Vec<_>>();
    // 主标签不能把候选折成多行；说明列只使用剩余宽度。
    let menu_content_width = usize::from(width).saturating_sub(2);
    let label_width = visible
        .iter()
        .map(|(_, entry)| UnicodeWidthStr::width(entry.label()))
        .max()
        .unwrap_or(0)
        .min(menu_content_width);
    let label_description_gap = menu_content_width.saturating_sub(label_width).min(2);
    let description_width = menu_content_width
        .saturating_sub(label_width)
        .saturating_sub(label_description_gap);

    visible
        .into_iter()
        .map(|(index, entry)| {
            let label_style = if index == selected_index {
                blue_style().add_modifier(Modifier::BOLD)
            } else {
                muted_style()
            };
            let label = truncate_to_width(entry.label(), label_width);
            let label_padding =
                " ".repeat(label_width.saturating_sub(UnicodeWidthStr::width(label.as_str())));
            let description = truncate_to_width(entry.description(), description_width);
            Line::from(vec![
                Span::styled("  ", muted_style()),
                Span::styled(format!("{label}{label_padding}"), label_style),
                Span::styled(" ".repeat(label_description_gap), muted_style()),
                Span::styled(description, muted_style()),
            ])
        })
        .collect()
}

pub(super) fn truncate_to_width(text: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    if UnicodeWidthStr::width(text) <= max_width {
        return text.to_string();
    }
    if max_width <= 1 {
        return "…".into();
    }
    let target = max_width.saturating_sub(1);
    let mut out = String::new();
    let mut width = 0usize;
    for ch in text.chars() {
        let ch_width = ch.width().unwrap_or(0);
        if width.saturating_add(ch_width) > target {
            break;
        }
        out.push(ch);
        width = width.saturating_add(ch_width);
    }
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Entry(String);

    impl CompletionMenuEntry for Entry {
        fn label(&self) -> &str {
            &self.0
        }

        fn description(&self) -> &str {
            "candidate"
        }
    }

    #[test]
    fn window_follows_selection() {
        assert_eq!(completion_menu_window_start(10, 0, 0), 0);
        assert_eq!(completion_menu_window_start(10, 4, 0), 0);
        assert_eq!(completion_menu_window_start(10, 5, 0), 1);
        assert_eq!(completion_menu_window_start(10, 9, 4), 5);
        assert_eq!(completion_menu_window_start(10, 2, 4), 2);
        assert_eq!(completion_menu_window_start(4, 3, 2), 0);
    }

    #[test]
    fn selection_wraps_and_keeps_selected_item_visible() {
        let entries = (0..8)
            .map(|index| Entry(format!("item-{index}")))
            .collect::<Vec<_>>();
        let mut state = CompletionMenuState::default();
        assert!(state.select_previous(entries.len()));
        assert_eq!(state.selected_index(), 7);
        assert_eq!(state.window_start(), 3);

        let lines = render_completion_menu(&entries, &state, 40);
        assert_eq!(lines.len(), COMPLETION_MENU_MAX_VISIBLE);
        assert!(lines.iter().any(|line| line.to_string().contains("item-7")));
    }
}
