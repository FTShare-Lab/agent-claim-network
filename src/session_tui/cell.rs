//! TUI transcript cell 的展示模型。
//!
//! 本模块把 session event 累积后的历史项渲染为纯文本或 styled lines。
//! 状态机只负责创建 cell，具体展示细节集中在这里维护。

use std::time::{Duration, Instant};

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::UnicodeWidthStr;

use crate::agent::UserShellCommandStatus;
use crate::api::{ToolCallSkipReason, ToolExecutionOutcome};
use crate::mcp::name::parse_mcp_visible_tool_name;
use crate::tool::diff::{
    FileChange, FileChangeKind, FileDiffLine, FileDiffLineKind, FileLineEnding,
};

use super::at_path::{scan_at_path_tokens, split_at_path_segments};
use super::markdown::append_markdown_agent;
use super::theme::{
    diff_added_style, diff_removed_style, CODE_CONTENT_FG, DIFF_ADDED_FG, DIFF_REMOVED_FG, MUTED_FG,
};
use super::wrapping::{hard_wrap_styled_lines, wrap_text_to_visual_lines};

/// transcript 用户气泡里 `@path` 的高亮前景色（灰色加粗，背景沿用灰条）。
const USER_AT_PATH_FG: Color = Color::DarkGray;

pub(super) trait HistoryCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HistoryEntry {
    User(UserCell),
    Assistant(AssistantCell),
    Tool(ToolCell),
    ShellCommand(ShellCommandCell),
    Status(StatusCell),
    Error(ErrorCell),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UserCell {
    pub(super) text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AssistantCell {
    pub(super) text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ToolCell {
    /// tool_use_id 只在单 turn 唯一；后台终态回写必须同时匹配此字段。
    pub(super) turn_id: Option<String>,
    pub(super) id: String,
    pub(super) name: String,
    pub(super) summary: String,
    pub(super) started_summary: Option<String>,
    pub(super) progress_summary: Option<String>,
    pub(super) outcome: Option<ToolExecutionOutcome>,
    pub(super) completed: bool,
    pub(super) interrupted: bool,
    pub(super) skipped: bool,
    pub(super) skip_reason: Option<ToolCallSkipReason>,
    pub(super) started_at: Instant,
    pub(super) file_change: Option<FileChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ShellCommandCell {
    pub(super) command: String,
    pub(super) status: ShellCommandCellStatus,
    pub(super) exit_code: Option<i32>,
    pub(super) duration_ms: Option<u128>,
    pub(super) stdout: String,
    pub(super) stderr: String,
    pub(super) truncated: bool,
    pub(super) error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ShellCommandCellStatus {
    Running,
    Completed,
    TimedOut,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StatusCell {
    pub(super) text: String,
    /// 前一条 User 已单独 flush 且未保留间隔时，是否需要在本状态前补一行。
    pub(super) leading_gap_after_flushed_user: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ErrorCell {
    pub(super) text: String,
    /// 前一条 User 已单独 flush 且未保留间隔时，是否需要在本错误前补一行。
    pub(super) leading_gap_after_flushed_user: bool,
}

impl HistoryEntry {
    #[cfg(test)]
    pub(super) fn plain_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        match self {
            HistoryEntry::User(cell) => {
                push_prefixed_plain(&mut lines, "› ", "  ", &cell.text);
                lines.push(String::new());
            }
            HistoryEntry::Assistant(cell) => {
                push_multiline_plain(&mut lines, &cell.text);
                lines.push(String::new());
            }
            HistoryEntry::Tool(cell) => {
                lines.push(format!("• {}", cell.header_text()));
                if let Some(detail) = cell.detail_text() {
                    push_prefixed_plain(&mut lines, "  └ ", "    ", &detail);
                }
                lines.push(String::new());
            }
            HistoryEntry::ShellCommand(cell) => {
                lines.push(format!("• {}", cell.header_text()));
                push_prefixed_plain(&mut lines, "  └ ", "    ", &cell.detail_text());
            }
            HistoryEntry::Status(cell) => push_prefixed_plain(&mut lines, "  ", "  ", &cell.text),
            HistoryEntry::Error(cell) => lines.push(format!("  Error {}", cell.text)),
        }
        lines
    }

    #[cfg(test)]
    pub(super) fn display_lines_with_width(&self, width: Option<usize>) -> Vec<Line<'static>> {
        let width = width
            .and_then(|width| u16::try_from(width).ok())
            .unwrap_or(u16::MAX);
        self.display_lines(width)
    }

    pub(super) fn live_status_lines(&self, width: u16) -> Vec<Line<'static>> {
        match self {
            HistoryEntry::Tool(cell) => cell.live_status_lines(width),
            _ => self.display_lines(width),
        }
    }
}

impl HistoryCell for HistoryEntry {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        match self {
            HistoryEntry::User(cell) => {
                let base_style = Style::default()
                    .fg(Color::Black)
                    .bg(Color::Gray)
                    .add_modifier(Modifier::BOLD);
                push_wrapped_user_render(
                    &mut lines,
                    "› ",
                    "  ",
                    &cell.text,
                    width,
                    base_style,
                    base_style.fg(USER_AT_PATH_FG),
                );
                trim_trailing_blank_render_lines_keep_one(&mut lines);
                pad_all_lines_to_width(
                    &mut lines,
                    usize::from(width),
                    Style::default().fg(Color::Black).bg(Color::Gray),
                );
                lines.push(Line::default());
            }
            HistoryEntry::Assistant(cell) => {
                append_markdown_agent(&cell.text, Some(usize::from(width)), &mut lines);
                trim_trailing_blank_render_lines_keep_one(&mut lines);
                lines.push(Line::default());
            }
            HistoryEntry::Tool(cell) => {
                lines.extend(cell.display_lines(width));
            }
            HistoryEntry::ShellCommand(cell) => {
                let bullet_style = match cell.completed_ok() {
                    Some(true) => Style::default()
                        .fg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD),
                    Some(false) => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    None => Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                };
                lines.push(Line::from(vec![
                    Span::styled("•", bullet_style),
                    Span::raw(" "),
                    Span::styled(
                        "shell ",
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(cell.command.clone(), Style::default().fg(CODE_CONTENT_FG)),
                ]));
                push_prefixed_render_wrapped(
                    &mut lines,
                    "  └ ",
                    "    ",
                    &cell.detail_text(),
                    width,
                    Style::default().fg(Color::DarkGray),
                );
                lines.push(Line::default());
            }
            HistoryEntry::Status(cell) => {
                if let Some(help_lines) = help_status_display_lines(&cell.text, width) {
                    lines.extend(help_lines);
                } else if let Some(skills_lines) = skills_status_display_lines(&cell.text, width) {
                    lines.extend(skills_lines);
                } else {
                    push_prefixed_render_wrapped(
                        &mut lines,
                        "  ",
                        "  ",
                        &cell.text,
                        width,
                        Style::default().fg(Color::DarkGray),
                    );
                }
            }
            HistoryEntry::Error(cell) => push_prefixed_render_wrapped(
                &mut lines,
                "  Error ",
                "        ",
                &cell.text,
                width,
                Style::default().fg(Color::Red),
            ),
        }
        lines
    }
}

const HELP_COMMAND_COL_WIDTH: usize = 12;
const HELP_ENTRIES: &[(&str, &str)] = &[
    ("/compact", "compact session history"),
    ("/copy", "copy the last Assistant response"),
    ("/exit", "finalize and exit"),
    ("/help", "show this help"),
    ("/inbox", "sync maintainer messages"),
    ("/mcp", "inspect MCP servers and tools"),
    ("/ps", "inspect and manage background processes"),
    ("/resume", "reopen a previous session"),
    ("/skills", "list available skills"),
    ("/subagents", "inspect current session subagents"),
    ("!cmd", "run a local shell command"),
    (
        "@path",
        "attaches image, PDF, or UTF-8 text file (supports @\"a b.png\" / @a\\ b.png)",
    ),
    ("Ctrl+V", "attaches clipboard image (text paste uses Cmd+V)"),
    (
        "Ctrl+O",
        "opens attachments in the default app (cursor on one picks it, otherwise all)",
    ),
    (
        "Ctrl+Enter",
        "interrupts the running turn and sends text as steer",
    ),
    ("Shift+Enter", "inserts a new line."),
];

pub(super) fn help_status_text() -> String {
    let mut text = String::from("ACN commands");
    for (command, description) in HELP_ENTRIES {
        text.push('\n');
        text.push_str(&format!("{command:<HELP_COMMAND_COL_WIDTH$}{description}"));
    }
    text
}

pub(super) fn user_text_display_lines(text: &str, width: u16) -> Vec<Line<'static>> {
    HistoryEntry::User(UserCell {
        text: text.to_string(),
    })
    .display_lines(width)
}

fn help_status_display_lines(text: &str, width: u16) -> Option<Vec<Line<'static>>> {
    if text != help_status_text() {
        return None;
    }
    let base_style = Style::default().fg(Color::DarkGray);
    let command_style = base_style.add_modifier(Modifier::BOLD);
    let mut lines = vec![Line::styled("  ACN commands", base_style)];
    for (command, description) in HELP_ENTRIES {
        push_help_entry_lines(
            &mut lines,
            command,
            description,
            width,
            base_style,
            command_style,
        );
    }
    Some(lines)
}

fn skills_status_display_lines(text: &str, width: u16) -> Option<Vec<Line<'static>>> {
    if !text.starts_with("Available skills\n") {
        return None;
    }
    let base_style = Style::default().fg(Color::DarkGray);
    let header_style = base_style.add_modifier(Modifier::BOLD);
    let mut lines = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let style = if idx <= 1 { header_style } else { base_style };
        push_prefixed_render_wrapped(&mut lines, "  ", "  ", line, width, style);
    }
    Some(lines)
}

fn push_help_entry_lines(
    out: &mut Vec<Line<'static>>,
    command: &str,
    description: &str,
    width: u16,
    base_style: Style,
    command_style: Style,
) {
    let first_prefix_width = 2 + HELP_COMMAND_COL_WIDTH;
    for visual_line in wrap_text_to_visual_lines(description, width, first_prefix_width) {
        let body = description.get(visual_line.range).unwrap_or_default();
        if visual_line.logical_line_index == 0 && !visual_line.is_wrapped_continuation {
            out.push(Line::from(vec![
                Span::styled("  ", base_style),
                Span::styled(format!("{command:<HELP_COMMAND_COL_WIDTH$}"), command_style),
                Span::styled(body.to_string(), base_style),
            ]));
        } else {
            out.push(Line::from(vec![
                Span::styled("  ", base_style),
                Span::styled(" ".repeat(HELP_COMMAND_COL_WIDTH), base_style),
                Span::styled(body.to_string(), base_style),
            ]));
        }
    }
}

impl ToolCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        self.render_lines(self.detail_text(), self.scrollback_diff_lines(width))
    }

    fn live_status_lines(&self, _width: u16) -> Vec<Line<'static>> {
        // live 虚线框内不渲染 diff；diff 只在 turn 落定后的 scrollback 展示。
        self.render_lines(self.live_detail_text(), Vec::new())
    }

    fn render_lines(
        &self,
        detail: Option<String>,
        diff_lines: Vec<Line<'static>>,
    ) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        let bullet_style = if self.skipped {
            Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            match self.completed_ok() {
                Some(true) => Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
                Some(false) => Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                None => Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            }
        };
        lines.push(Line::from(vec![
            Span::styled("•", bullet_style),
            Span::raw(" "),
            Span::styled(
                self.header_text(),
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
        if let Some(detail) = detail {
            push_prefixed_render(
                &mut lines,
                "  └ ",
                "    ",
                &detail,
                Style::default().fg(Color::DarkGray),
            );
            trim_trailing_blank_render_lines_keep_one(&mut lines);
        }
        lines.extend(diff_lines);
        lines.push(Line::default());
        lines
    }

    /// diff 块渲染；仅在工具成功完成且携带 FileChange 时产出。
    fn scrollback_diff_lines(&self, width: u16) -> Vec<Line<'static>> {
        if self.completed_ok() != Some(true)
            || !matches!(self.name.as_str(), "file_write" | "file_patch")
        {
            return Vec::new();
        }
        let Some(change) = &self.file_change else {
            return Vec::new();
        };
        render_file_change_lines(change, width)
    }

    fn header_text(&self) -> String {
        let verb = if self.skipped {
            "Skipped"
        } else if self.interrupted {
            "Interrupted"
        } else if self.completed {
            "Called"
        } else {
            "Calling"
        };
        format!("{verb} {}", tool_display_name(&self.name))
    }

    fn detail_text(&self) -> Option<String> {
        if self.skipped {
            return combine_tool_details(
                self.started_input(),
                self.skip_reason
                    .map(|reason| format!("Reason: {}", reason.as_str())),
            );
        }
        if !self.completed {
            return self.started_input();
        }

        combine_tool_details(
            self.started_input_from_original_summary(),
            self.completed_error_text(),
        )
    }

    fn live_detail_text(&self) -> Option<String> {
        if self.completed {
            return self.detail_text();
        }
        let running_detail = combine_tool_details(
            self.progress_text(),
            Some(format!(
                "elapsed {}",
                format_elapsed(self.started_at.elapsed())
            )),
        );
        combine_tool_details(self.detail_text(), running_detail)
    }

    fn completed_ok(&self) -> Option<bool> {
        self.completed
            .then(|| self.outcome.map(ToolExecutionOutcome::is_success))
            .flatten()
    }

    fn started_input(&self) -> Option<String> {
        self.started_input_from_summary(&self.summary)
    }

    fn started_input_from_original_summary(&self) -> Option<String> {
        self.started_summary
            .as_deref()
            .and_then(|summary| self.started_input_from_summary(summary))
    }

    fn started_input_from_summary(&self, summary: &str) -> Option<String> {
        let prefix = format!("tool {} ", self.name);
        summary
            .strip_prefix(&prefix)
            .map(str::trim)
            .filter(|input| !input.is_empty())
            .map(ToString::to_string)
    }

    fn progress_text(&self) -> Option<String> {
        let prefix = format!("tool {} progress ", self.name);
        self.progress_summary
            .as_deref()
            .and_then(|summary| summary.strip_prefix(&prefix))
            .map(str::trim)
            .filter(|progress| !progress.is_empty())
            .map(|progress| format!("progress {progress}"))
    }

    fn completed_detail(&self) -> String {
        let prefix = format!("tool {} ", self.name);
        let raw = self
            .summary
            .strip_prefix(&prefix)
            .unwrap_or(self.summary.as_str())
            .trim();
        raw.split_once(' ')
            .map_or("", |(_, detail)| detail)
            .trim()
            .to_string()
    }

    fn completed_error_text(&self) -> Option<String> {
        let outcome = self.outcome?;
        let detail = self.completed_detail();
        match outcome {
            ToolExecutionOutcome::Completed
                if parse_mcp_visible_tool_name(&self.name).is_some() =>
            {
                Some("Result: ok".into())
            }
            ToolExecutionOutcome::Completed => None,
            ToolExecutionOutcome::DispatchFailure => {
                Some(with_optional_detail("Error: Dispatch failed", &detail))
            }
            ToolExecutionOutcome::BusinessFailure => Some(with_optional_detail("Error", &detail)),
            ToolExecutionOutcome::ProcessExit {
                exit_code, success, ..
            } => {
                let label = exit_code.map_or_else(
                    || "Process exit: unavailable".to_string(),
                    |code| format!("Process exit code: {code}"),
                );
                if success {
                    Some(label)
                } else {
                    Some(with_optional_detail(&label, &detail))
                }
            }
            ToolExecutionOutcome::ProcessTerminated { signal } => Some(signal.map_or_else(
                || "Process terminated".to_string(),
                |signal| format!("Process terminated: signal {signal}"),
            )),
            ToolExecutionOutcome::ProcessRunning => Some("Process running in background".into()),
            ToolExecutionOutcome::HttpResponse { http_status } => {
                Some(format!("HTTP status: {http_status}"))
            }
        }
    }
}

fn with_optional_detail(label: &str, detail: &str) -> String {
    if detail.is_empty() {
        label.to_string()
    } else {
        format!("{label}: {detail}")
    }
}

fn tool_display_name(name: &str) -> String {
    if let Some((server, tool)) = parse_mcp_visible_tool_name(name) {
        return format!("mcp {server}/{tool}");
    }
    name.to_string()
}

/// diff 块统一缩进，与 `└` 之后的续行前缀对齐。
const DIFF_INDENT: &str = "    ";

/// 单个工具 diff 允许占用的最多可视行，防止窄屏折行撑爆 history。
const MAX_DIFF_VISUAL_ROWS: usize = 256;

/// 渲染边界的独立单行上限，兼容未经新采集器的旧 journal。
const MAX_DIFF_RENDER_TEXT_CHARS: usize = 4 * 1024;

fn render_file_change_lines(change: &FileChange, width: u16) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    // visual cap 也可能隐藏整个 +/- 逻辑行，因此始终为精确计数、内容和可视截断预留 footer。
    let footer_reserve = 3;
    let body_limit = MAX_DIFF_VISUAL_ROWS.saturating_sub(footer_reserve);
    let mut visual_truncated = false;
    let mut render_content_truncated = false;
    let captured_changed_lines = change
        .hunks
        .iter()
        .flat_map(|hunk| &hunk.lines)
        .filter(|line| matches!(line.kind, FileDiffLineKind::Add | FileDiffLineKind::Remove))
        .count();
    let mut rendered_changed_lines = 0usize;
    let label_style = Style::default()
        .fg(Color::DarkGray)
        .add_modifier(Modifier::BOLD);
    // 用 Added / Edited 区分新建与修改，便于快速识别文件操作类型。
    let verb = match change.kind {
        FileChangeKind::Created => "Added",
        FileChangeKind::Modified => "Edited",
    };
    let (safe_path, path_truncated) = sanitize_terminal_text(&change.path);
    render_content_truncated |= path_truncated;
    let mut header = vec![
        Span::styled(format!("{DIFF_INDENT}{verb} {safe_path} "), label_style),
        Span::styled("(", label_style),
        Span::styled(
            format!("+{}", change.added_lines),
            Style::default().fg(DIFF_ADDED_FG),
        ),
        Span::styled(" ", label_style),
        Span::styled(
            format!("-{}", change.removed_lines),
            Style::default().fg(DIFF_REMOVED_FG),
        ),
        Span::styled(")", label_style),
    ];
    if change.approximate {
        header.push(Span::styled(" ~approx", Style::default().fg(MUTED_FG)));
    }
    let header_lines = hard_wrap_styled_lines(vec![Line::from(header)], usize::from(width.max(1)));
    if !append_diff_rows(&mut lines, header_lines, body_limit).0 {
        visual_truncated = true;
    }
    let num_width = diff_line_number_width(change);
    'hunks: for (hunk_index, hunk) in change.hunks.iter().enumerate() {
        if visual_truncated {
            break;
        }
        if hunk_index > 0 && !append_diff_rows(&mut lines, vec![diff_gap_line(width)], body_limit).0
        {
            visual_truncated = true;
            break;
        }
        for diff_line in &hunk.lines {
            let (rendered, content_truncated) = render_diff_line(diff_line, num_width, width);
            render_content_truncated |= content_truncated;
            let (complete, appended_rows) = append_diff_rows(&mut lines, rendered, body_limit);
            if appended_rows > 0
                && matches!(
                    diff_line.kind,
                    FileDiffLineKind::Add | FileDiffLineKind::Remove
                )
            {
                rendered_changed_lines = rendered_changed_lines.saturating_add(1);
            }
            if !complete {
                visual_truncated = true;
                break 'hunks;
            }
        }
    }
    let omitted_changed_lines = change
        .truncated_changed_lines
        .saturating_add(captured_changed_lines.saturating_sub(rendered_changed_lines));
    if omitted_changed_lines > 0 {
        lines.push(Line::from(Span::styled(
            format!("{DIFF_INDENT}⋮ 其余 {} 行改动未展示", omitted_changed_lines),
            Style::default().fg(MUTED_FG),
        )));
    }
    if change.content_truncated || render_content_truncated {
        lines.push(Line::from(Span::styled(
            format!("{DIFF_INDENT}⋮ 过长行内容已截断"),
            Style::default().fg(MUTED_FG),
        )));
    }
    if visual_truncated {
        lines.push(Line::from(Span::styled(
            format!("{DIFF_INDENT}⋮ diff 显示已截断"),
            Style::default().fg(MUTED_FG),
        )));
    }
    lines
}

fn append_diff_rows(
    lines: &mut Vec<Line<'static>>,
    mut additional: Vec<Line<'static>>,
    limit: usize,
) -> (bool, usize) {
    let remaining = limit.saturating_sub(lines.len());
    let complete = additional.len() <= remaining;
    additional.truncate(remaining);
    let appended = additional.len();
    lines.extend(additional);
    (complete, appended)
}

fn diff_gap_line(width: u16) -> Line<'static> {
    let prefix = if usize::from(width) > UnicodeWidthStr::width(DIFF_INDENT) {
        DIFF_INDENT
    } else {
        ""
    };
    Line::from(Span::styled(
        format!("{prefix}⋮"),
        Style::default().fg(MUTED_FG),
    ))
}

fn render_diff_line(
    line: &FileDiffLine,
    num_width: usize,
    width: u16,
) -> (Vec<Line<'static>>, bool) {
    let (marker, style) = match line.kind {
        FileDiffLineKind::Context => (' ', Style::default().fg(MUTED_FG)),
        FileDiffLineKind::Add => ('+', diff_added_style()),
        FileDiffLineKind::Remove => ('-', diff_removed_style()),
        FileDiffLineKind::Gap => {
            return (vec![diff_gap_line(width)], false);
        }
    };
    let number = display_line_number(line);
    let full_prefix = format!("{DIFF_INDENT}{number:>num_width$} {marker} ");
    let prefix = if usize::from(width) > UnicodeWidthStr::width(full_prefix.as_str()) {
        full_prefix
    } else if width > 2 {
        marker.to_string()
    } else {
        return (
            vec![Line::from(Span::styled(marker.to_string(), style)).style(style)],
            !line.content.is_empty(),
        );
    };
    let continuation_prefix = " ".repeat(UnicodeWidthStr::width(prefix.as_str()));
    let (mut content, render_truncated) = sanitize_terminal_text(&line.content);
    if line.content_truncated || render_truncated {
        content.push('…');
    }
    content.push_str(line_ending_annotation(line.line_ending));
    let rendered =
        wrap_text_to_visual_lines(&content, width, UnicodeWidthStr::width(prefix.as_str()))
            .into_iter()
            .map(|visual_line| {
                let body = content.get(visual_line.range).unwrap_or_default();
                let line_prefix = if visual_line.is_wrapped_continuation {
                    &continuation_prefix
                } else {
                    &prefix
                };
                Line::from(Span::styled(format!("{line_prefix}{body}"), style)).style(style)
            })
            .collect();
    (rendered, render_truncated)
}

fn line_ending_annotation(ending: FileLineEnding) -> &'static str {
    match ending {
        FileLineEnding::CrLf => " ⟪CRLF⟫",
        FileLineEnding::None => " ⟪no newline⟫",
        FileLineEnding::Unknown | FileLineEnding::Lf => "",
    }
}

fn sanitize_terminal_text(value: &str) -> (String, bool) {
    let mut output = String::new();
    let mut output_chars = 0usize;
    for ch in value.chars() {
        let visible = match ch {
            '\t' => "⇥".to_string(),
            ch if ch.is_control() && u32::from(ch) <= 0xff => {
                format!("\\x{:02x}", u32::from(ch))
            }
            ch if ch.is_control() => format!("\\u{{{:x}}}", u32::from(ch)),
            ch => ch.to_string(),
        };
        let visible_chars = visible.chars().count();
        if output_chars.saturating_add(visible_chars) > MAX_DIFF_RENDER_TEXT_CHARS {
            return (output, true);
        }
        output.push_str(&visible);
        output_chars = output_chars.saturating_add(visible_chars);
    }
    (output, false)
}

/// 单列行号：新增/上下文取新文件行号，删除取旧文件行号。
fn display_line_number(line: &FileDiffLine) -> String {
    let number = match line.kind {
        FileDiffLineKind::Remove => line.old_line,
        _ => line.new_line.or(line.old_line),
    };
    number.map(|n| n.to_string()).unwrap_or_default()
}

fn diff_line_number_width(change: &FileChange) -> usize {
    change
        .hunks
        .iter()
        .flat_map(|hunk| hunk.lines.iter())
        .map(|line| display_line_number(line).len())
        .max()
        .unwrap_or(1)
        .max(1)
}

fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    if seconds < 60 {
        format!("{seconds}s")
    } else {
        format!("{}m{:02}s", seconds / 60, seconds % 60)
    }
}

fn combine_tool_details(input: Option<String>, result: Option<String>) -> Option<String> {
    match (input, result) {
        (Some(input), Some(result)) => Some(format!("{input}\n{result}")),
        (Some(input), None) => Some(input),
        (None, result) => result,
    }
}

impl ShellCommandCell {
    pub(super) fn running(command: String) -> Self {
        Self {
            command,
            status: ShellCommandCellStatus::Running,
            exit_code: None,
            duration_ms: None,
            stdout: String::new(),
            stderr: String::new(),
            truncated: false,
            error: None,
        }
    }

    pub(super) fn complete(
        &mut self,
        status: UserShellCommandStatus,
        exit_code: Option<i32>,
        duration_ms: u128,
        stdout: String,
        stderr: String,
        truncated: bool,
    ) {
        self.status = match status {
            UserShellCommandStatus::Completed => ShellCommandCellStatus::Completed,
            UserShellCommandStatus::TimedOut => ShellCommandCellStatus::TimedOut,
            UserShellCommandStatus::Cancelled => ShellCommandCellStatus::Cancelled,
        };
        self.exit_code = exit_code;
        self.duration_ms = Some(duration_ms);
        self.stdout = stdout;
        self.stderr = stderr;
        self.truncated = truncated;
        self.error = None;
    }

    pub(super) fn fail(&mut self, error: String) {
        self.status = ShellCommandCellStatus::Failed;
        self.error = Some(error);
    }

    #[cfg(test)]
    fn header_text(&self) -> String {
        format!("shell {}", self.command)
    }

    fn detail_text(&self) -> String {
        let mut lines = Vec::new();
        match self.status {
            ShellCommandCellStatus::Running => lines.push("Running...".into()),
            ShellCommandCellStatus::Completed => {
                lines.push(format!(
                    "Exit {} in {}",
                    self.exit_code
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "none".into()),
                    format_duration(self.duration_ms)
                ));
            }
            ShellCommandCellStatus::TimedOut => {
                lines.push(format!(
                    "Timed out after {}",
                    format_duration(self.duration_ms)
                ));
            }
            ShellCommandCellStatus::Cancelled => lines.push("Cancelled".into()),
            ShellCommandCellStatus::Failed => lines.push(format!(
                "Failed{}",
                self.error
                    .as_ref()
                    .filter(|error| !error.trim().is_empty())
                    .map(|error| format!(": {error}"))
                    .unwrap_or_default()
            )),
        }
        if !self.stdout.is_empty() {
            lines.push("stdout".into());
            lines.extend(self.stdout.lines().map(ToString::to_string));
        }
        if !self.stderr.is_empty() {
            lines.push("stderr".into());
            lines.extend(self.stderr.lines().map(ToString::to_string));
        }
        if self.truncated {
            lines.push("... Output truncated".into());
        }
        lines.join("\n")
    }

    fn completed_ok(&self) -> Option<bool> {
        match self.status {
            ShellCommandCellStatus::Running => None,
            ShellCommandCellStatus::Completed => Some(self.exit_code == Some(0)),
            ShellCommandCellStatus::TimedOut
            | ShellCommandCellStatus::Cancelled
            | ShellCommandCellStatus::Failed => Some(false),
        }
    }

    pub(super) fn is_finalized(&self) -> bool {
        self.status != ShellCommandCellStatus::Running
    }
}

fn format_duration(duration_ms: Option<u128>) -> String {
    let Some(duration_ms) = duration_ms else {
        return "unknown".into();
    };
    let secs = duration_ms / 1000;
    let millis = duration_ms % 1000;
    format!("{secs}.{millis:03}s")
}

#[cfg(test)]
fn push_prefixed_plain(out: &mut Vec<String>, first_prefix: &str, next_prefix: &str, text: &str) {
    for (idx, line) in text.lines().enumerate() {
        let prefix = if idx == 0 { first_prefix } else { next_prefix };
        out.push(format!("{prefix}{line}"));
    }
    if text.is_empty() {
        out.push(first_prefix.trim_end().to_string());
    }
}

#[cfg(test)]
fn push_multiline_plain(out: &mut Vec<String>, text: &str) {
    if text.is_empty() {
        out.push(String::new());
        return;
    }
    out.extend(text.lines().map(ToString::to_string));
}

fn push_prefixed_render(
    out: &mut Vec<Line<'static>>,
    first_prefix: &str,
    next_prefix: &str,
    text: &str,
    style: Style,
) {
    for (idx, line) in text.lines().enumerate() {
        let prefix = if idx == 0 { first_prefix } else { next_prefix };
        out.push(Line::styled(format!("{prefix}{line}"), style));
    }
    if text.is_empty() {
        out.push(Line::styled(first_prefix.trim_end().to_string(), style));
    }
}

fn push_prefixed_render_wrapped(
    out: &mut Vec<Line<'static>>,
    first_prefix: &str,
    next_prefix: &str,
    text: &str,
    width: u16,
    style: Style,
) {
    let reserved_cols = UnicodeWidthStr::width(first_prefix);
    let mut wrapped = Vec::new();
    let mut visual_lines = wrap_text_to_visual_lines(text, width, reserved_cols);
    if !text.is_empty() && text.ends_with('\n') {
        if let Some(last) = visual_lines.last() {
            if last.range.is_empty() {
                visual_lines.pop();
            }
        }
    }
    for visual_line in visual_lines {
        let prefix = if visual_line.logical_line_index == 0 && !visual_line.is_wrapped_continuation
        {
            first_prefix
        } else {
            next_prefix
        };
        let body = text.get(visual_line.range.clone()).unwrap_or_default();
        wrapped.push(Line::styled(format!("{prefix}{body}"), style));
    }
    out.extend(hard_wrap_styled_lines(wrapped, usize::from(width.max(1))));
}

/// 用户气泡渲染：折行 + 前缀，并把 `@path` 片段单独上高亮样式。
fn push_wrapped_user_render(
    out: &mut Vec<Line<'static>>,
    first_prefix: &str,
    next_prefix: &str,
    text: &str,
    width: u16,
    style: Style,
    at_path_style: Style,
) {
    let reserved_cols = UnicodeWidthStr::width(first_prefix);
    let at_token_ranges = scan_at_path_tokens(text)
        .into_iter()
        .map(|token| token.range)
        .collect::<Vec<_>>();
    for visual_line in wrap_text_to_visual_lines(text, width, reserved_cols) {
        let prefix = if visual_line.logical_line_index == 0 && !visual_line.is_wrapped_continuation
        {
            first_prefix
        } else {
            next_prefix
        };
        let body = text.get(visual_line.range.clone()).unwrap_or_default();
        if at_token_ranges.is_empty() {
            out.push(Line::styled(format!("{prefix}{body}"), style));
            continue;
        }
        let mut spans = vec![Span::styled(prefix.to_string(), style)];
        for (segment, hit_index) in
            split_at_path_segments(body, visual_line.range.start, &at_token_ranges)
        {
            let segment_style = if hit_index.is_some() {
                at_path_style
            } else {
                style
            };
            spans.push(Span::styled(segment, segment_style));
        }
        out.push(Line::from(spans).style(style));
    }
}

fn pad_all_lines_to_width(lines: &mut [Line<'static>], width: usize, style: Style) {
    for line in lines {
        pad_single_line_to_width(line, width, style);
    }
}

fn pad_single_line_to_width(line: &mut Line<'static>, width: usize, style: Style) {
    let current_width = line.width();
    if current_width < width {
        line.push_span(Span::styled(" ".repeat(width - current_width), style));
    }
    line.style = style;
}

fn trim_trailing_blank_render_lines_keep_one(lines: &mut Vec<Line<'static>>) {
    while lines.len() > 1 && lines.last().is_some_and(line_is_visually_blank) {
        lines.pop();
    }
}

fn line_is_visually_blank(line: &Line<'_>) -> bool {
    line.spans.is_empty() || line.to_string().trim().is_empty()
}

#[cfg(test)]
mod tests {
    use ratatui::{buffer::Buffer, layout::Rect, style::Color, widgets::Widget};

    use super::{
        render_diff_line, render_file_change_lines, HistoryEntry, ShellCommandCell, StatusCell,
        ToolCell, UserCell,
    };
    use crate::agent::UserShellCommandStatus;
    use crate::api::ToolExecutionOutcome;
    use crate::session_tui::theme::{
        apply_surface_style, CODE_CONTENT_FG, DIFF_ADDED_FG, DIFF_REMOVED_FG, MUTED_FG, SURFACE_BG,
    };
    use crate::tool::diff::{
        FileChange, FileChangeKind, FileDiffHunk, FileDiffLine, FileDiffLineKind, FileLineEnding,
    };

    fn test_file_change(
        path: impl Into<String>,
        kind: FileChangeKind,
        added_lines: usize,
        removed_lines: usize,
        diff_lines: Vec<FileDiffLine>,
        truncated_changed_lines: usize,
    ) -> FileChange {
        FileChange {
            path: path.into(),
            kind,
            added_lines,
            removed_lines,
            hunks: vec![FileDiffHunk { lines: diff_lines }],
            truncated_changed_lines,
            content_truncated: false,
            approximate: false,
        }
    }

    fn is_unsafe_terminal_control(ch: char) -> bool {
        matches!(ch, '\u{1b}' | '\u{7}' | '\r' | '\u{8}') || ('\u{80}'..='\u{9f}').contains(&ch)
    }

    fn long_added_file_change() -> FileChange {
        test_file_change(
            "minified.js",
            FileChangeKind::Modified,
            1,
            0,
            vec![FileDiffLine {
                kind: FileDiffLineKind::Add,
                old_line: None,
                new_line: Some(7),
                content: "界".repeat(20_000),
                line_ending: FileLineEnding::Lf,
                content_truncated: false,
            }],
            0,
        )
    }

    #[test]
    fn file_diff_sanitizes_terminal_controls_in_path_and_content_spans() {
        let c1_controls = ('\u{80}'..='\u{9f}').collect::<String>();
        let controls = format!("\u{1b}]52;c;SGVsbG8=\u{7}\r\u{8}\t{c1_controls}");
        let change = test_file_change(
            format!("safe{controls}.txt"),
            FileChangeKind::Modified,
            1,
            0,
            vec![FileDiffLine {
                kind: FileDiffLineKind::Add,
                old_line: None,
                new_line: Some(1),
                content: format!("payload{controls}tail"),
                line_ending: FileLineEnding::Lf,
                content_truncated: false,
            }],
            0,
        );

        let lines = render_file_change_lines(&change, 80);
        for (line_index, line) in lines.iter().enumerate() {
            for (span_index, span) in line.spans.iter().enumerate() {
                assert!(
                    !span.content.chars().any(is_unsafe_terminal_control),
                    "第 {line_index} 行第 {span_index} 个 Span 泄漏原始终端控制字符: {:?}",
                    span.content
                );
            }
        }
        let rendered = lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("safe"));
        assert!(rendered.contains("payload"));
        assert!(rendered.contains("tail"));
        assert!(rendered.contains("\\x1b"));
        assert!(rendered.contains('⇥'));
    }

    #[test]
    fn long_file_diff_line_keeps_content_indent_on_narrow_terminal() {
        let change = long_added_file_change();
        let lines = render_file_change_lines(&change, 24);
        let first_add_index = lines
            .iter()
            .position(|line| line.to_string().contains("7 + "))
            .expect("应渲染新增行的行号与标记");
        let first_add = lines[first_add_index].to_string();
        let content_column = first_add.find('界').expect("首行应包含 diff 内容");
        let continuation_prefix = " ".repeat(content_column);
        let continuation_lines = lines
            .iter()
            .skip(first_add_index.saturating_add(1))
            .filter(|line| {
                line.spans
                    .iter()
                    .any(|span| span.style.fg == Some(DIFF_ADDED_FG))
            })
            .collect::<Vec<_>>();
        assert!(!continuation_lines.is_empty(), "测试内容应触发折行");
        assert!(
            continuation_lines
                .iter()
                .all(|line| line.to_string().starts_with(&continuation_prefix)),
            "diff 续行必须与首行内容列对齐"
        );
    }

    #[test]
    fn changed_diff_rows_keep_background_across_wrap_and_narrow_width() {
        let add_bg = Color::Rgb(225, 242, 228);
        let remove_bg = Color::Rgb(249, 228, 225);
        for (kind, expected_bg, expected_fg) in [
            (FileDiffLineKind::Add, add_bg, DIFF_ADDED_FG),
            (FileDiffLineKind::Remove, remove_bg, DIFF_REMOVED_FG),
        ] {
            let line = FileDiffLine {
                kind,
                old_line: Some(7),
                new_line: Some(7),
                content: "changed content ".repeat(8),
                line_ending: FileLineEnding::Lf,
                content_truncated: false,
            };
            let (wrapped, _) = render_diff_line(&line, 1, 24);
            assert!(wrapped.len() > 1, "测试内容应触发折行");
            assert!(wrapped.iter().all(|row| row.style.bg == Some(expected_bg)));
            assert!(wrapped.iter().all(|row| row.style.fg == Some(expected_fg)));

            let (marker_only, _) = render_diff_line(&line, 1, 1);
            assert_eq!(marker_only.len(), 1);
            assert_eq!(marker_only[0].style.bg, Some(expected_bg));
            assert_eq!(marker_only[0].style.fg, Some(expected_fg));
        }

        let context = FileDiffLine {
            kind: FileDiffLineKind::Context,
            old_line: Some(7),
            new_line: Some(7),
            content: "unchanged".into(),
            line_ending: FileLineEnding::Lf,
            content_truncated: false,
        };
        assert!(render_diff_line(&context, 1, 24)
            .0
            .iter()
            .all(|row| row.style.bg.is_none()));
    }

    #[test]
    fn changed_diff_row_background_fills_render_area() {
        let area = Rect::new(0, 0, 32, 1);
        for (kind, expected_bg) in [
            (FileDiffLineKind::Add, Color::Rgb(225, 242, 228)),
            (FileDiffLineKind::Remove, Color::Rgb(249, 228, 225)),
            (FileDiffLineKind::Context, SURFACE_BG),
            (FileDiffLineKind::Gap, SURFACE_BG),
        ] {
            let line = FileDiffLine {
                kind,
                old_line: Some(1),
                new_line: Some(1),
                content: "x".into(),
                line_ending: FileLineEnding::Lf,
                content_truncated: false,
            };
            let (rows, _) = render_diff_line(&line, 1, area.width);
            let mut buffer = Buffer::empty(area);
            apply_surface_style(rows.into_iter().next().expect("应渲染一行"))
                .render(area, &mut buffer);
            for x in area.left()..area.right() {
                assert_eq!(
                    buffer[(x, area.top())].bg,
                    expected_bg,
                    "{kind:?} 在 x={x} 未覆盖整行背景"
                );
            }
        }
    }

    #[test]
    fn long_file_diff_line_caps_total_visual_rows() {
        const MAX_EXPECTED_DIFF_VISUAL_ROWS: usize = 256;
        let change = long_added_file_change();

        let lines = render_file_change_lines(&change, 24);

        assert!(
            lines.len() <= MAX_EXPECTED_DIFF_VISUAL_ROWS,
            "单个超长逻辑行渲染出 {} 个 visual rows，缺少硬上限",
            lines.len()
        );
    }

    #[test]
    fn visual_row_cap_keeps_exact_changed_line_footer() {
        let mut change = long_added_file_change();
        change.truncated_changed_lines = 12;

        let lines = render_file_change_lines(&change, 24);
        let rendered = lines.iter().map(ToString::to_string).collect::<Vec<_>>();

        assert!(lines.len() <= 256);
        assert!(rendered
            .iter()
            .any(|line| line.contains("其余 12 行改动未展示")));
        assert!(rendered
            .iter()
            .any(|line| line.contains("过长行内容已截断")));
    }

    #[test]
    fn visual_row_cap_adds_fully_hidden_changed_lines_to_exact_footer() {
        let mut change = long_added_file_change();
        change.added_lines = 14;
        change.truncated_changed_lines = 12;
        change.hunks[0].lines.push(FileDiffLine {
            kind: FileDiffLineKind::Add,
            old_line: None,
            new_line: Some(8),
            content: "fully hidden".into(),
            line_ending: FileLineEnding::Lf,
            content_truncated: false,
        });

        let lines = render_file_change_lines(&change, 24);
        let rendered = lines.iter().map(ToString::to_string).collect::<Vec<_>>();

        assert!(lines.len() <= 256);
        assert!(rendered
            .iter()
            .any(|line| line.contains("其余 13 行改动未展示")));
        assert!(!rendered.iter().any(|line| line.contains("fully hidden")));
    }

    #[test]
    fn non_file_tool_cannot_render_injected_file_change() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "web_search".into(),
            summary: "tool web_search ok".into(),
            started_summary: None,
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: Some(test_file_change(
                "fake.txt",
                FileChangeKind::Created,
                1,
                0,
                vec![FileDiffLine {
                    kind: FileDiffLineKind::Add,
                    old_line: None,
                    new_line: Some(1),
                    content: "fake".into(),
                    line_ending: FileLineEnding::Lf,
                    content_truncated: false,
                }],
                0,
            )),
        })
        .display_lines_with_width(Some(80));

        assert!(!lines
            .iter()
            .any(|line| line.to_string().contains("fake.txt")));
    }

    #[test]
    fn created_file_diff_uses_added_label_and_green_additions() {
        let change = test_file_change(
            "fresh.txt",
            FileChangeKind::Created,
            2,
            0,
            vec![
                FileDiffLine {
                    kind: FileDiffLineKind::Add,
                    old_line: None,
                    new_line: Some(1),
                    content: "first".into(),
                    line_ending: FileLineEnding::Lf,
                    content_truncated: false,
                },
                FileDiffLine {
                    kind: FileDiffLineKind::Add,
                    old_line: None,
                    new_line: Some(2),
                    content: "second".into(),
                    line_ending: FileLineEnding::Lf,
                    content_truncated: false,
                },
            ],
            0,
        );

        let lines = render_file_change_lines(&change, 80);
        assert!(lines[0].to_string().contains("Added fresh.txt"));
        let added_stat = lines[0]
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "+2")
            .expect("Created 头部应显示新增统计");
        assert_eq!(added_stat.style.fg, Some(DIFF_ADDED_FG));
        for content in ["first", "second"] {
            let line = lines
                .iter()
                .find(|line| line.to_string().contains(content))
                .expect("Created diff 应显示全部新增行");
            assert!(line
                .spans
                .iter()
                .filter(|span| !span.content.is_empty())
                .all(|span| span.style.fg == Some(DIFF_ADDED_FG)));
        }
    }

    #[test]
    fn file_diff_context_and_truncation_notice_use_muted_theme_color() {
        let change = test_file_change(
            "note.txt",
            FileChangeKind::Modified,
            4,
            0,
            vec![
                FileDiffLine {
                    kind: FileDiffLineKind::Context,
                    old_line: Some(1),
                    new_line: Some(1),
                    content: "context".into(),
                    line_ending: FileLineEnding::Lf,
                    content_truncated: false,
                },
                FileDiffLine {
                    kind: FileDiffLineKind::Add,
                    old_line: None,
                    new_line: Some(2),
                    content: "changed".into(),
                    line_ending: FileLineEnding::Lf,
                    content_truncated: false,
                },
            ],
            3,
        );

        let lines = render_file_change_lines(&change, 80);
        let context = lines
            .iter()
            .find(|line| line.to_string().contains("context"))
            .expect("应渲染上下文行");
        assert!(context
            .spans
            .iter()
            .filter(|span| !span.content.is_empty())
            .all(|span| span.style.fg == Some(MUTED_FG)));
        let truncation = lines
            .iter()
            .find(|line| line.to_string().contains("其余 3 行改动未展示"))
            .expect("应渲染截断提示");
        assert!(truncation
            .spans
            .iter()
            .filter(|span| !span.content.is_empty())
            .all(|span| span.style.fg == Some(MUTED_FG)));
    }

    #[test]
    fn user_input_renders_as_high_contrast_gray_bar() {
        let lines = HistoryEntry::User(UserCell {
            text: "你好".into(),
        })
        .display_lines_with_width(None);

        assert_eq!(lines[0].style.fg, Some(Color::Black));
        assert_eq!(lines[0].style.bg, Some(Color::Gray));
    }

    #[test]
    fn user_input_gray_bar_fills_available_width() {
        let lines = HistoryEntry::User(UserCell {
            text: "你好".into(),
        })
        .display_lines_with_width(Some(12));

        assert_eq!(lines[0].width(), 12);
        assert_eq!(lines[0].spans.last().unwrap().style.bg, Some(Color::Gray));
    }

    #[test]
    fn user_input_highlights_at_path_span_only() {
        let lines = HistoryEntry::User(UserCell {
            text: "看 @a.txt 内容".into(),
        })
        .display_lines_with_width(Some(40));

        let at_span = lines[0]
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "@a.txt")
            .expect("@path 应渲染为独立 span");
        assert_eq!(at_span.style.fg, Some(super::USER_AT_PATH_FG));
        let plain_span = lines[0]
            .spans
            .iter()
            .find(|span| span.content.as_ref().contains("内容"))
            .expect("普通文本 span 应存在");
        assert_eq!(plain_span.style.fg, Some(Color::Black));
    }

    #[test]
    fn multiline_user_input_gray_bar_fills_every_line() {
        let lines = HistoryEntry::User(UserCell {
            text: "你好\n我是谁".into(),
        })
        .display_lines_with_width(Some(12));

        assert_eq!(lines[0].width(), 12);
        assert_eq!(lines[1].width(), 12);
        assert_eq!(lines[0].spans.last().unwrap().style.bg, Some(Color::Gray));
        assert_eq!(lines[1].spans.last().unwrap().style.bg, Some(Color::Gray));
    }

    #[test]
    fn successful_tool_call_does_not_use_green_transcript_accent() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "web_search".into(),
            summary: "tool web_search ok".into(),
            started_summary: None,
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        })
        .display_lines_with_width(None);

        assert_ne!(lines[0].spans[0].style.fg, Some(Color::Green));
    }

    #[test]
    fn completed_tool_call_keeps_started_input_detail() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "web_search".into(),
            summary: "tool web_search ok".into(),
            started_summary: Some(r#"tool web_search {"query":"今日 美股 收盘"}"#.into()),
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        })
        .display_lines_with_width(None);
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Called web_search"));
        assert!(rendered.contains(r#"{"query":"今日 美股 收盘"}"#));
        assert!(!rendered.contains("tool web_search"));
    }

    #[test]
    fn mcp_tool_call_uses_compact_display_name() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "mcp__pal__ask".into(),
            summary: "tool mcp__pal__ask ok".into(),
            started_summary: Some(r#"tool mcp__pal__ask {"q":"hi"}"#.into()),
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        })
        .display_lines_with_width(None);
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Called mcp pal/ask"));
        assert!(rendered.contains("Result: ok"));
        assert!(!rendered.contains("Called mcp__pal__ask"));
    }

    #[test]
    fn running_mcp_tool_call_renders_latest_progress() {
        let lines = ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "mcp__pal__ask".into(),
            summary: r#"tool mcp__pal__ask {"q":"hi"}"#.into(),
            started_summary: Some(r#"tool mcp__pal__ask {"q":"hi"}"#.into()),
            progress_summary: Some("tool mcp__pal__ask progress 1/2 half".into()),
            outcome: None,
            completed: false,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        }
        .live_status_lines(80);
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Calling mcp pal/ask"));
        assert!(rendered.contains("progress 1/2 half"));
        assert!(rendered.contains("elapsed"));
    }

    #[test]
    fn failed_tool_call_keeps_input_before_error_detail() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "file_read".into(),
            summary: "tool file_read failed permission denied".into(),
            started_summary: Some(r#"tool file_read {"path":"missing"}"#.into()),
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::BusinessFailure),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        })
        .display_lines_with_width(None);
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains(r#"{"path":"missing"}"#));
        assert!(rendered.contains("Error: permission denied"));
    }

    #[test]
    fn typed_outcome_controls_tool_state_instead_of_summary_text() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "file_read".into(),
            summary: "tool file_read failed misleading legacy text".into(),
            started_summary: None,
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        })
        .display_lines_with_width(None);

        assert_ne!(lines[0].spans[0].style.fg, Some(Color::Red));
        assert!(!lines.iter().any(|line| line.to_string().contains("Error:")));
    }

    #[test]
    fn non_success_http_outcome_is_visible_and_failed() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "web_fetch".into(),
            summary: "tool web_fetch http_status=404".into(),
            started_summary: Some(
                r#"tool web_fetch {"url":"https://example.test/missing"}"#.into(),
            ),
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::HttpResponse { http_status: 404 }),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        })
        .display_lines_with_width(None);
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert_eq!(lines[0].spans[0].style.fg, Some(Color::Red));
        assert!(rendered.contains("HTTP status: 404"));
    }

    #[test]
    fn successful_hard_termination_renders_signal_without_error_state() {
        let lines = HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id: "toolu_1".into(),
            name: "write_stdin".into(),
            summary: "tool write_stdin terminated_signal=9".into(),
            started_summary: Some(
                r#"tool write_stdin {"process_id":"deadbeef","terminate":true}"#.into(),
            ),
            progress_summary: None,
            outcome: Some(ToolExecutionOutcome::ProcessTerminated { signal: Some(9) }),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: std::time::Instant::now(),
            file_change: None,
        })
        .display_lines_with_width(None);
        let rendered = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert_ne!(lines[0].spans[0].style.fg, Some(Color::Red));
        assert!(rendered.contains("Process terminated: signal 9"));
        assert!(!rendered.contains("Process exit: unavailable"));
    }

    #[test]
    fn shell_command_cell_renders_output_without_xml_record() {
        let mut cell = ShellCommandCell::running("echo hi".into());
        cell.complete(
            UserShellCommandStatus::Completed,
            Some(0),
            12,
            "hi\n".into(),
            String::new(),
            false,
        );
        let lines = HistoryEntry::ShellCommand(cell).display_lines_with_width(None);
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("shell echo hi"));
        assert!(rendered.contains("stdout"));
        assert!(rendered.contains("hi"));
        assert!(!rendered.contains("<user_shell_command>"));
    }

    #[test]
    fn shell_command_cell_renders_command_with_code_color() {
        let lines = HistoryEntry::ShellCommand(ShellCommandCell::running("echo hi".into()))
            .display_lines_with_width(None);
        let command_span = lines[0]
            .spans
            .iter()
            .find(|span| span.content == "echo hi")
            .expect("Command span should render separately");

        assert_eq!(command_span.style.fg, Some(CODE_CONTENT_FG));
    }

    #[test]
    fn shell_command_cell_renders_cancelled() {
        let mut cell = ShellCommandCell::running("sleep 999".into());
        cell.complete(
            UserShellCommandStatus::Cancelled,
            None,
            33,
            String::new(),
            String::new(),
            false,
        );
        let lines = HistoryEntry::ShellCommand(cell).display_lines_with_width(None);
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Cancelled"));
    }

    #[test]
    fn status_cell_preserves_explicit_line_breaks() {
        let lines = HistoryEntry::Status(StatusCell {
            text: "one\ntwo\nthree".into(),
            leading_gap_after_flushed_user: false,
        })
        .display_lines_with_width(None);

        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>();
        assert_eq!(rendered, vec!["  one", "  two", "  three"]);
    }

    #[test]
    fn status_cell_wraps_long_warning_to_terminal_width() {
        let lines = HistoryEntry::Status(StatusCell {
            text: "Warning: Maintainer inbox 拉取失败，已继续处理本地 inbox：timeout=20s body=None"
                .into(),
            leading_gap_after_flushed_user: false,
        })
        .display_lines_with_width(Some(32));

        assert!(lines.len() > 1);
        assert!(lines.iter().all(|line| line.width() <= 32));
        assert!(lines[0].to_string().starts_with("  Warning:"));
        assert!(lines[1].to_string().starts_with("  "));
    }

    #[test]
    fn status_and_error_cells_do_not_exceed_extremely_narrow_widths() {
        let status_lines = HistoryEntry::Status(StatusCell {
            text: "x".into(),
            leading_gap_after_flushed_user: false,
        })
        .display_lines_with_width(Some(2));
        let error_lines = HistoryEntry::Error(super::ErrorCell {
            text: "x".into(),
            leading_gap_after_flushed_user: true,
        })
        .display_lines_with_width(Some(8));

        assert!(status_lines.iter().all(|line| line.width() <= 2));
        assert!(error_lines.iter().all(|line| line.width() <= 8));
    }

    #[test]
    fn status_cell_does_not_add_blank_line_for_trailing_newline() {
        let lines = HistoryEntry::Status(StatusCell {
            text: "one\n".into(),
            leading_gap_after_flushed_user: false,
        })
        .display_lines_with_width(Some(20));
        let rendered = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>();

        assert_eq!(rendered, vec!["  one"]);
    }
}
