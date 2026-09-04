//! /resume session 选择器。
//!
//! 本模块只维护可恢复 session 列表的键盘选择与渲染；真正的 session 打开由 App 状态机
//! 接收事件后异步执行，避免 UI 组件直接读写存储。

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::UnicodeWidthStr;

use crate::session::ResumedSessionSummary;

use super::app_event::{AppEvent, AppEventSender};

const ID_COLUMN_WIDTH: usize = 18;
const CLOSED_AT_COLUMN_WIDTH: usize = 20;
const STATUS_COLUMN_WIDTH: usize = 11;
const COLUMN_GAP: &str = "  ";
const TABLE_FIXED_WIDTH: usize =
    2 + ID_COLUMN_WIDTH + CLOSED_AT_COLUMN_WIDTH + STATUS_COLUMN_WIDTH + 3 * COLUMN_GAP.len();

pub(super) struct SessionPickerState {
    sessions: Vec<ResumedSessionSummary>,
    selected: usize,
    inline_error: Option<SessionPickerInlineError>,
    event_tx: AppEventSender,
}

struct SessionPickerInlineError {
    session_id: crate::claim::SessionId,
    message: String,
}

impl SessionPickerState {
    pub(super) fn new(sessions: Vec<ResumedSessionSummary>, event_tx: AppEventSender) -> Self {
        Self {
            sessions,
            selected: 0,
            inline_error: None,
            event_tx,
        }
    }

    pub(super) fn set_selected_inline_error(&mut self, message: impl Into<String>) {
        self.inline_error =
            self.sessions
                .get(self.selected)
                .map(|session| SessionPickerInlineError {
                    session_id: session.id.clone(),
                    message: message.into(),
                });
    }

    pub(super) fn clear_inline_error(&mut self) {
        self.inline_error = None;
    }

    pub(super) fn handle_key_event(&mut self, key: KeyEvent) {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return;
        }
        match key.code {
            KeyCode::Up => self.selected = self.selected.saturating_sub(1),
            KeyCode::Down => {
                let max_selected = self.sessions.len().saturating_sub(1);
                self.selected = self.selected.saturating_add(1).min(max_selected);
            }
            KeyCode::Enter => {
                if let Some(session) = self.sessions.get(self.selected) {
                    self.event_tx
                        .send(AppEvent::PickerSessionSelected(session.id.clone()));
                }
            }
            KeyCode::Esc => self.event_tx.send(AppEvent::PickerCancelled),
            _ => {}
        }
    }

    pub(super) fn render_inline_lines(&self, width: u16) -> Vec<Line<'static>> {
        let table_width = usize::from(width.saturating_sub(4)).max(1);
        let mut lines = vec![Line::from("Session Resume"), Line::from("")];
        if self.sessions.is_empty() {
            lines.push(Line::from("No resumable sessions. Press Esc to return."));
            return lines;
        }

        lines.push(Line::from(format_table_row(
            " ",
            "id",
            "closed_at",
            "status",
            "last_message",
        )));
        for (index, session) in self.sessions.iter().enumerate() {
            let id = session.id.as_str();
            let closed_at = session
                .closed_at
                .map(|time| time.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .unwrap_or_else(|| "-".into());
            let status = match session.status {
                crate::session::SessionStatus::Open => "Interrupted",
                crate::session::SessionStatus::Closed => "Closed",
                crate::session::SessionStatus::Finalizing => "Finalizing",
            };
            let last_user = truncate_to_width(
                session.last_user_text.as_deref().unwrap_or(""),
                table_width.saturating_sub(TABLE_FIXED_WIDTH),
            );
            let marker = if index == self.selected { "›" } else { " " };
            let line = format_table_row(marker, id, &closed_at, status, &last_user);
            let style = if index == self.selected {
                Style::default().add_modifier(Modifier::BOLD | Modifier::REVERSED)
            } else {
                Style::default().fg(ratatui::style::Color::DarkGray)
            };
            lines.push(padded_line(line, table_width, style));
            if let Some(error) = self
                .inline_error
                .as_ref()
                .filter(|error| error.session_id == session.id)
            {
                const ERROR_PREFIX: &str = "      Error: ";
                let message = truncate_to_width(
                    &error.message,
                    table_width.saturating_sub(ERROR_PREFIX.width()),
                );
                let line = format!("{ERROR_PREFIX}{message}");
                lines.push(padded_line(
                    line,
                    table_width,
                    Style::default().fg(Color::Red),
                ));
            }
        }
        lines.push(Line::from(""));
        lines.push(Line::from("↑↓ Navigate  Enter Select  Esc Cancel"));
        lines
    }

    #[cfg(test)]
    fn selected(&self) -> usize {
        self.selected
    }
}

fn format_table_row(
    marker: &str,
    id: &str,
    closed_at: &str,
    status: &str,
    last_message: &str,
) -> String {
    format!(
        "{marker} {id:<id_width$}{gap}{closed_at:<closed_at_width$}{gap}{status:<status_width$}{gap}{last_message}",
        gap = COLUMN_GAP,
        id_width = ID_COLUMN_WIDTH,
        closed_at_width = CLOSED_AT_COLUMN_WIDTH,
        status_width = STATUS_COLUMN_WIDTH,
    )
}

fn padded_line(text: String, width: usize, style: Style) -> Line<'static> {
    let mut line = Line::styled(text, style);
    let current_width = line.width();
    if current_width < width {
        line.push_span(Span::styled(" ".repeat(width - current_width), style));
    }
    line
}

fn truncate_to_width(text: &str, max_width: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out = String::new();
    let mut width: usize = 0;
    for ch in normalized.chars() {
        let ch_width = ch.to_string().width();
        if width.saturating_add(ch_width) > max_width {
            break;
        }
        out.push(ch);
        width += ch_width;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::SessionId;
    use crate::session::SessionStatus;
    use chrono::Utc;
    use crossterm::event::{KeyEvent, KeyEventKind, KeyModifiers};
    use std::str::FromStr;

    fn summary(id: &str) -> ResumedSessionSummary {
        let now = Utc::now();
        ResumedSessionSummary {
            id: SessionId::from_str(id).unwrap(),
            status: SessionStatus::Closed,
            updated_at: now,
            closed_at: Some(now),
            last_user_text: Some("hello".into()),
        }
    }

    #[test]
    fn picker_up_down_clamps_at_boundaries() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let mut picker = SessionPickerState::new(
            vec![summary("session_aaaaaaaa"), summary("session_bbbbbbbb")],
            event_tx,
        );

        picker.handle_key_event(KeyEvent::from(KeyCode::Up));
        assert_eq!(picker.selected(), 0);
        picker.handle_key_event(KeyEvent::from(KeyCode::Down));
        picker.handle_key_event(KeyEvent::from(KeyCode::Down));
        assert_eq!(picker.selected(), 1);
    }

    #[test]
    fn picker_ignores_key_release_events() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let mut picker = SessionPickerState::new(
            vec![summary("session_aaaaaaaa"), summary("session_bbbbbbbb")],
            event_tx,
        );

        picker.handle_key_event(KeyEvent::new_with_kind(
            KeyCode::Down,
            KeyModifiers::NONE,
            KeyEventKind::Release,
        ));

        assert_eq!(picker.selected(), 0);
    }

    #[test]
    fn picker_render_header_uses_truncated_closed_at_and_last_message_name() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let picker = SessionPickerState::new(vec![summary("session_aaaaaaaa")], event_tx);
        let rendered = picker
            .render_inline_lines(100)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("last_message"));
        assert!(!rendered.contains("last message"));
        assert!(!rendered.contains("+00:00"));
    }

    #[test]
    fn picker_header_and_rows_share_column_boundaries() {
        let header = format_table_row(" ", "id", "closed_at", "status", "last_message");
        let row = format_table_row("›", "session_aaaaaaaa", "-", "Interrupted", "hello");
        let display_column = |line: &str, needle: &str| {
            let byte_index = line.find(needle).unwrap();
            line[..byte_index].width()
        };

        assert_eq!(
            display_column(&header, "id"),
            display_column(&row, "session_aaaaaaaa")
        );
        assert_eq!(
            display_column(&header, "closed_at"),
            display_column(&row, "-")
        );
        assert_eq!(
            display_column(&header, "status"),
            display_column(&row, "Interrupted")
        );
        assert_eq!(
            display_column(&header, "last_message"),
            display_column(&row, "hello")
        );
        assert!(row.contains("Interrupted  hello"));
    }

    #[test]
    fn picker_derives_interrupted_label_from_open_status() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let mut interrupted = summary("session_aaaaaaaa");
        interrupted.status = SessionStatus::Open;
        interrupted.closed_at = None;
        let picker = SessionPickerState::new(vec![interrupted], event_tx);

        let rendered = picker
            .render_inline_lines(100)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Interrupted"));
        assert!(!rendered.contains("Finalizing"));
    }

    #[test]
    fn picker_renders_finalizing_candidate_with_existing_status_label() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let mut finalizing = summary("session_aaaaaaaa");
        finalizing.status = SessionStatus::Finalizing;
        finalizing.closed_at = None;
        let picker = SessionPickerState::new(vec![finalizing], event_tx);

        let rendered = picker
            .render_inline_lines(100)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Finalizing"));
        assert!(!rendered.contains("Interrupted"));
    }

    #[test]
    fn picker_render_marks_selected_row_with_visible_marker() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let mut picker = SessionPickerState::new(
            vec![summary("session_aaaaaaaa"), summary("session_bbbbbbbb")],
            event_tx,
        );

        let initial = picker
            .render_inline_lines(100)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>();
        assert!(initial
            .iter()
            .any(|line| line.starts_with("› session_aaaaaaaa")));

        picker.handle_key_event(KeyEvent::from(KeyCode::Down));
        let moved = picker
            .render_inline_lines(100)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>();
        assert!(moved
            .iter()
            .any(|line| line.starts_with("› session_bbbbbbbb")));
    }

    #[test]
    fn picker_renders_red_error_directly_below_target_without_affecting_selection() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let mut picker = SessionPickerState::new(
            vec![summary("session_aaaaaaaa"), summary("session_bbbbbbbb")],
            event_tx,
        );
        picker.set_selected_inline_error("still finalizing");

        let lines = picker.render_inline_lines(100);
        let target_index = lines
            .iter()
            .position(|line| line.to_string().starts_with("› session_aaaaaaaa"))
            .unwrap();
        assert_eq!(
            lines[target_index + 1].to_string().trim_end(),
            "      Error: still finalizing"
        );
        assert_eq!(lines[target_index + 1].style.fg, Some(Color::Red));
        assert!(lines[target_index + 2]
            .to_string()
            .starts_with("  session_bbbbbbbb"));
        assert_eq!(picker.selected(), 0);

        picker.handle_key_event(KeyEvent::from(KeyCode::Down));
        assert_eq!(picker.selected(), 1);
    }

    #[test]
    fn new_picker_does_not_retain_previous_inline_error() {
        let (event_tx, _event_rx) = AppEventSender::channel();
        let mut picker =
            SessionPickerState::new(vec![summary("session_aaaaaaaa")], event_tx.clone());
        picker.set_selected_inline_error("still finalizing");

        let reopened = SessionPickerState::new(vec![summary("session_aaaaaaaa")], event_tx);
        let rendered = reopened
            .render_inline_lines(100)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(!rendered.contains("still finalizing"));
    }

    #[test]
    fn picker_enter_sends_selected_session_id() {
        let (event_tx, mut event_rx) = AppEventSender::channel();
        let mut picker = SessionPickerState::new(vec![summary("session_aaaaaaaa")], event_tx);

        picker.handle_key_event(KeyEvent::from(KeyCode::Enter));

        assert_eq!(
            event_rx.try_recv().unwrap(),
            AppEvent::PickerSessionSelected(SessionId::from_str("session_aaaaaaaa").unwrap())
        );
    }

    #[test]
    fn picker_esc_sends_cancelled() {
        let (event_tx, mut event_rx) = AppEventSender::channel();
        let mut picker = SessionPickerState::new(vec![summary("session_aaaaaaaa")], event_tx);

        picker.handle_key_event(KeyEvent::from(KeyCode::Esc));

        assert_eq!(event_rx.try_recv().unwrap(), AppEvent::PickerCancelled);
    }

    #[test]
    fn picker_empty_list_does_not_send_on_enter() {
        let (event_tx, mut event_rx) = AppEventSender::channel();
        let mut picker = SessionPickerState::new(Vec::new(), event_tx);

        picker.handle_key_event(KeyEvent::from(KeyCode::Enter));

        assert!(event_rx.try_recv().is_err());
    }
}
