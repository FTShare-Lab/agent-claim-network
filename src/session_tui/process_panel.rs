//! `/ps` 聚合进程面板：只展示 root session 的 live entry，并提供全页面终止确认视图。

use std::cell::Cell;
use std::time::SystemTime;

use chrono::{DateTime, Local};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::tool::ProcessSnapshot;

use super::markdown::render_code_block_lines;
use super::theme::{accent_style, blue_style, muted_style, surface_style};
use super::wrapping::hard_wrap_styled_lines;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ProcessPanelKeyAction {
    None,
    Refresh,
    Terminate { target: ProcessTerminationTarget },
}

/// 从 `/ps` snapshot 取出的终止目标。instance_id 不是模型协议字段；它只用于保证确认页
/// 在 logical process_id 被回收并复用后，不会误终止新 allocation。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProcessTerminationTarget {
    pub(super) process_id: String,
    pub(super) subagent_id: Option<String>,
    pub(super) instance_id: u64,
}

impl ProcessTerminationTarget {
    fn from_row(row: &ProcessSnapshot) -> Self {
        Self {
            process_id: row.process_id.clone(),
            subagent_id: row.subagent_id.clone(),
            instance_id: row.instance_id,
        }
    }

    fn matches_row(&self, row: &ProcessSnapshot) -> bool {
        self.process_id == row.process_id
            && self.subagent_id == row.subagent_id
            && self.instance_id == row.instance_id
    }
}

#[derive(Debug, Clone)]
enum ProcessPanelView {
    List,
    TerminateConfirm {
        target: ProcessTerminationTarget,
        command_scroll: Cell<usize>,
    },
}

#[derive(Debug, Clone)]
pub(super) struct ProcessPanelState {
    visible: bool,
    rows: Vec<ProcessSnapshot>,
    selected: usize,
    list_offset: Cell<usize>,
    view: ProcessPanelView,
    notice: Option<String>,
}

impl Default for ProcessPanelState {
    fn default() -> Self {
        Self {
            visible: false,
            rows: Vec::new(),
            selected: 0,
            list_offset: Cell::new(0),
            view: ProcessPanelView::List,
            notice: None,
        }
    }
}

impl ProcessPanelState {
    pub(super) fn open(&mut self) {
        self.visible = true;
        self.view = ProcessPanelView::List;
        self.notice = None;
        self.clamp_selection();
    }

    pub(super) fn close(&mut self) {
        self.visible = false;
        self.view = ProcessPanelView::List;
        self.notice = None;
    }

    pub(super) fn visible(&self) -> bool {
        self.visible
    }

    pub(super) fn update(&mut self, rows: Vec<ProcessSnapshot>) -> bool {
        // `/ps` 是用户控制面，协议只允许显示 running / terminating；即使异步 snapshot
        // 恰好看见 reserve→attach 的 Starting 窗口，也不能暴露一个无颜色且不可 terminate 的第三态。
        let rows = rows
            .into_iter()
            .filter(|row| matches!(row.status.as_str(), "running" | "terminating"))
            .collect::<Vec<_>>();
        if self.rows == rows {
            return false;
        }
        let selected_target = self.selected_row().map(ProcessTerminationTarget::from_row);
        self.rows = rows;
        if let Some(selected_target) = selected_target {
            if let Some(index) = self
                .rows
                .iter()
                .position(|row| selected_target.matches_row(row))
            {
                self.selected = index;
            }
        }
        if let ProcessPanelView::TerminateConfirm { target, .. } = &self.view {
            if !self.rows.iter().any(|row| target.matches_row(row)) {
                self.view = ProcessPanelView::List;
                self.notice = Some("Already exited".into());
            }
        }
        self.clamp_selection();
        true
    }

    /// `/ps` 与底栏共享同一份 live snapshot，避免两处的进程数量或状态不一致。
    pub(super) fn background_status_text(&self, compact: bool) -> Option<String> {
        let running = self
            .rows
            .iter()
            .filter(|row| row.status == "running")
            .count();
        let terminating = self.rows.len().saturating_sub(running);
        if running == 0 && terminating == 0 {
            return None;
        }
        let mut parts = Vec::new();
        if running > 0 {
            let label = if compact { "run" } else { "running" };
            parts.push(format!("{running} {label}"));
        }
        if terminating > 0 {
            let label = if compact { "stopping" } else { "terminating" };
            parts.push(format!("{terminating} {label}"));
        }
        Some(format!("Processes: {} · /ps", parts.join(" · ")))
    }

    pub(super) fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
    }

    pub(super) fn handle_key(&mut self, key: KeyEvent) -> ProcessPanelKeyAction {
        match &mut self.view {
            ProcessPanelView::List => match key.code {
                KeyCode::Esc => self.close(),
                KeyCode::Up => self.selected = self.selected.saturating_sub(1),
                KeyCode::Down => {
                    self.selected = self
                        .selected
                        .saturating_add(1)
                        .min(self.rows.len().saturating_sub(1));
                }
                KeyCode::Char('t') | KeyCode::Char('T') => {
                    if let Some(row) = self.selected_row() {
                        if row.status == "running" {
                            self.view = ProcessPanelView::TerminateConfirm {
                                target: ProcessTerminationTarget::from_row(row),
                                command_scroll: Cell::new(0),
                            };
                        }
                    }
                }
                _ => {}
            },
            ProcessPanelView::TerminateConfirm {
                target,
                command_scroll,
            } => match key.code {
                KeyCode::Up if key.modifiers.is_empty() => {
                    command_scroll.set(command_scroll.get().saturating_sub(1))
                }
                KeyCode::Down if key.modifiers.is_empty() => {
                    // 实际上限取决于当前终端宽度下 code renderer 的 wrap 结果，只能在 render
                    // 阶段精确计算；这里先递增，render_confirm 会把它 clamp 到可见末尾。
                    command_scroll.set(command_scroll.get().saturating_add(1));
                }
                KeyCode::Char('y' | 'Y')
                    if matches!(key.modifiers, KeyModifiers::NONE | KeyModifiers::SHIFT) =>
                {
                    let target = target.clone();
                    self.view = ProcessPanelView::List;
                    return ProcessPanelKeyAction::Terminate { target };
                }
                KeyCode::Char('n' | 'N')
                    if matches!(key.modifiers, KeyModifiers::NONE | KeyModifiers::SHIFT) =>
                {
                    self.view = ProcessPanelView::List;
                    self.clamp_selection();
                    return ProcessPanelKeyAction::Refresh;
                }
                KeyCode::Esc if key.modifiers.is_empty() => {
                    self.view = ProcessPanelView::List;
                    self.clamp_selection();
                    return ProcessPanelKeyAction::Refresh;
                }
                _ => {}
            },
        }
        self.clamp_selection();
        ProcessPanelKeyAction::None
    }

    pub(super) fn render_lines(&self, width: u16, height: u16) -> Vec<Line<'static>> {
        match &self.view {
            ProcessPanelView::List => self.render_list(width, height),
            ProcessPanelView::TerminateConfirm {
                target,
                command_scroll,
            } => self.render_confirm(width, height, target, command_scroll),
        }
    }

    fn render_list(&self, width: u16, height: u16) -> Vec<Line<'static>> {
        let width = usize::from(width.max(1));
        let budget = usize::from(height.max(1));
        let mut lines = vec![Line::from(vec![
            Span::styled(
                "Processes",
                accent_style().add_modifier(Modifier::UNDERLINED),
            ),
            Span::styled(
                " · ",
                muted_style()
                    .add_modifier(Modifier::BOLD)
                    .add_modifier(Modifier::UNDERLINED),
            ),
            Span::styled(
                "live processes",
                blue_style().add_modifier(Modifier::UNDERLINED),
            ),
        ])];
        if let Some(notice) = &self.notice {
            lines.push(Line::styled(truncate(notice, width), muted_style()));
        }
        lines.push(Line::default());
        if self.rows.is_empty() {
            lines.push(Line::styled(
                truncate("No live managed processes.", width),
                muted_style(),
            ));
        } else {
            let layout = ProcessListLayout::for_width(width);
            lines.push(Line::styled(
                truncate(&layout.header(), width),
                muted_style(),
            ));
            let fixed = lines.len().saturating_add(1);
            let row_height = layout.row_height();
            let viewport = budget
                .saturating_sub(fixed)
                .checked_div(row_height)
                .unwrap_or_default();
            self.ensure_selected_visible(viewport);
            for (index, row) in self
                .rows
                .iter()
                .enumerate()
                .skip(self.list_offset.get())
                .take(viewport)
            {
                let marker = if index == self.selected { "› " } else { "  " };
                let owner = row.subagent_id.as_deref().unwrap_or("main");
                lines.extend(layout.row(row, owner, marker, index == self.selected, width));
            }
        }
        place_footer_at_bottom(
            &mut lines,
            Line::styled(
                truncate("↑/↓ select · t terminate · Esc close", width),
                muted_style(),
            ),
            budget,
        );
        lines
    }

    fn render_confirm(
        &self,
        width: u16,
        height: u16,
        target: &ProcessTerminationTarget,
        command_scroll: &Cell<usize>,
    ) -> Vec<Line<'static>> {
        let width_usize = usize::from(width.max(1));
        let budget = usize::from(height.max(1));
        let Some(row) = self.rows.iter().find(|row| target.matches_row(row)) else {
            return vec![Line::styled(
                truncate("Process already exited. Press Esc.", width_usize),
                muted_style(),
            )];
        };
        let owner = row.subagent_id.as_deref().unwrap_or("main");
        let mut lines = confirmation_metadata_lines(row, owner, width_usize);
        let command_lines = row
            .command
            .lines()
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let wrapped = hard_wrap_styled_lines(
            render_code_block_lines(Some(&row.code_type), &command_lines, Some(width_usize)),
            width_usize,
        );
        let footer_text = if width_usize >= 42 {
            "[y] Yes                         [n/Esc] No"
        } else {
            "[y] Yes · [n/Esc] No"
        };
        let footer = Line::styled(truncate(footer_text, width_usize), muted_style());
        let content_budget = budget.saturating_sub(1);
        // 普通布局保留完整 metadata；高度不足以同时容纳 metadata、至少一行 Command body 和
        // footer 时改用压缩 metadata。这样 resize 到 8 行这类窄 viewport 时，用户仍可通过
        // ↑/↓ 阅读完整 command，而不是只看见不能滚动的空确认页。
        if lines.len().saturating_add(1) > content_budget {
            lines = compact_confirmation_metadata_lines(row, owner, width_usize);
        }
        if lines.len() >= content_budget {
            // 低于最小可用高度时物理上不可能同时显示完整 metadata、body 与 footer；优先
            // 保留 Command 标签、至少一行 body 与最后一行确认键，metadata 会压成一行。
            lines = minimal_confirmation_metadata_lines(row, owner, width_usize);
        }
        let metadata_budget = content_budget.saturating_sub(1);
        if lines.len() > metadata_budget {
            lines.truncate(metadata_budget);
        }
        let body_budget = content_budget.saturating_sub(lines.len());
        let start = command_scroll
            .get()
            .min(wrapped.len().saturating_sub(body_budget));
        command_scroll.set(start);
        lines.extend(wrapped.into_iter().skip(start).take(body_budget));
        place_footer_at_bottom(&mut lines, footer, budget);
        lines
    }

    fn selected_row(&self) -> Option<&ProcessSnapshot> {
        self.rows.get(self.selected)
    }

    pub(super) fn mark_terminating(&mut self, target: &ProcessTerminationTarget) {
        if let Some(row) = self.rows.iter_mut().find(|row| target.matches_row(row)) {
            row.status = "terminating".into();
        }
    }

    fn clamp_selection(&mut self) {
        self.selected = self.selected.min(self.rows.len().saturating_sub(1));
        self.list_offset.set(
            self.list_offset
                .get()
                .min(self.rows.len().saturating_sub(1)),
        );
    }

    fn ensure_selected_visible(&self, viewport: usize) {
        let mut offset = self.list_offset.get();
        if self.selected < offset {
            offset = self.selected;
        } else if self.selected >= offset.saturating_add(viewport) {
            offset = self.selected.saturating_add(1).saturating_sub(viewport);
        }
        self.list_offset.set(offset);
    }
}

fn confirmation_metadata_lines(
    row: &ProcessSnapshot,
    owner: &str,
    width: usize,
) -> Vec<Line<'static>> {
    vec![
        terminate_title_line(),
        Line::default(),
        confirmation_prompt_line(row, width),
        Line::default(),
        Line::from(truncate(&format!("Process ID: {}", row.process_id), width)),
        Line::from(truncate(&format!("Owner: {owner}"), width)),
        confirmation_status_line(&row.status, width),
        Line::from(truncate(
            &format!("Started: {}", format_started(row.started_at)),
            width,
        )),
        Line::from(truncate(
            &format!("Elapsed: {}", format_elapsed(row.started_at)),
            width,
        )),
        Line::from(truncate("Command:", width)),
    ]
}

fn compact_confirmation_metadata_lines(
    row: &ProcessSnapshot,
    owner: &str,
    width: usize,
) -> Vec<Line<'static>> {
    vec![
        terminate_title_line(),
        Line::default(),
        confirmation_prompt_line(row, width),
        Line::default(),
        Line::from(truncate(
            &format!("Process ID: {} · Owner: {owner}", row.process_id),
            width,
        )),
        confirmation_status_line(&row.status, width),
        Line::from(truncate(
            &format!(
                "Started: {} · Elapsed: {}",
                format_started(row.started_at),
                format_elapsed(row.started_at)
            ),
            width,
        )),
        Line::from(truncate("Command:", width)),
    ]
}

fn minimal_confirmation_metadata_lines(
    row: &ProcessSnapshot,
    owner: &str,
    width: usize,
) -> Vec<Line<'static>> {
    vec![
        confirmation_prompt_line(row, width),
        Line::from(truncate("Command:", width)),
        Line::from(truncate(
            &format!(
                "{} · {} · {} · {}",
                row.process_id,
                owner,
                row.status,
                format_elapsed(row.started_at)
            ),
            width,
        )),
    ]
}

fn terminate_title_line() -> Line<'static> {
    Line::from(vec![
        Span::styled(
            "Processes",
            accent_style().add_modifier(Modifier::UNDERLINED),
        ),
        Span::styled(
            " · ",
            muted_style()
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::UNDERLINED),
        ),
        Span::styled("terminate", blue_style().add_modifier(Modifier::UNDERLINED)),
    ])
}

fn confirmation_prompt_line(row: &ProcessSnapshot, width: usize) -> Line<'static> {
    Line::styled(
        truncate(
            &format!("Confirm to terminate process {}?", row.process_id),
            width,
        ),
        surface_style().fg(Color::Red),
    )
}

fn confirmation_status_line(status: &str, width: usize) -> Line<'static> {
    let prefix = truncate("Status: ", width);
    let status_width = width.saturating_sub(UnicodeWidthStr::width(prefix.as_str()));
    Line::from(vec![
        Span::raw(prefix),
        Span::styled(truncate(status, status_width), status_style(status)),
    ])
}

#[derive(Debug, Clone, Copy)]
enum ProcessListLayout {
    Table { show_cwd: bool },
    Compact,
}

const ELAPSED_COLUMN_WIDTH: usize = 10;

impl ProcessListLayout {
    fn for_width(width: usize) -> Self {
        // CWD 与 COMMAND 固定放在 primary row 之外，避免一条超长命令压缩所有核心状态。
        // 79 列以下即便移除 TTY，ID、OWNER、STATUS、STARTED 和 ELAPSED 的主表也不再
        // 宽松；此时才隐藏 CWD，并把 OWNER 放入第二条 detail row。
        if width < 79 {
            return Self::Compact;
        }
        Self::Table { show_cwd: true }
    }

    fn header(self) -> String {
        match self {
            Self::Table { .. } => {
                let mut columns = vec![
                    column("PROCESS ID", 10),
                    column("OWNER", 14),
                    column("STATUS", 11),
                    column("TTY", 3),
                ];
                columns.extend([
                    column("STARTED", 11),
                    column("ELAPSED", ELAPSED_COLUMN_WIDTH),
                ]);
                format!("  {}", columns.join(" | "))
            }
            Self::Compact => "  PROCESS ID | STATUS | STARTED | ELAPSED".into(),
        }
    }

    fn row_height(self) -> usize {
        // 每个 managed process 始终占用固定三条视觉行，保证 list_offset 和 ↑/↓ 的
        // 语义始终按“进程条目”而非其 detail 行移动。
        3
    }

    fn row(
        self,
        row: &ProcessSnapshot,
        owner: &str,
        marker: &str,
        selected: bool,
        width: usize,
    ) -> Vec<Line<'static>> {
        let selected_style = if selected {
            Style::default().add_modifier(Modifier::REVERSED)
        } else {
            Style::default()
        };
        match self {
            Self::Table { show_cwd } => {
                let mut spans = vec![
                    Span::styled(marker.to_string(), selected_style),
                    Span::styled(column(&row.process_id, 10), selected_style),
                    Span::styled(" | ", selected_style),
                    Span::styled(column(owner, 14), selected_style),
                    Span::styled(" | ", selected_style),
                    // 选中状态不覆盖 STATUS 的绿色/黄色；确认页复用同一 helper。
                    Span::styled(column(&row.status, 11), status_style(&row.status)),
                ];
                spans.extend([
                    Span::styled(" | ", selected_style),
                    Span::styled(
                        column(if row.tty { "yes" } else { "no" }, 3),
                        selected_style,
                    ),
                ]);
                spans.extend([
                    Span::styled(" | ", selected_style),
                    Span::styled(column(&format_started(row.started_at), 11), selected_style),
                    Span::styled(" | ", selected_style),
                    Span::styled(
                        column(&format_elapsed(row.started_at), ELAPSED_COLUMN_WIDTH),
                        selected_style,
                    ),
                ]);
                vec![
                    Line::from(spans),
                    process_detail_line(
                        if show_cwd { "cwd" } else { "owner" },
                        if show_cwd { &row.cwd } else { owner },
                        width,
                        selected,
                    ),
                    process_detail_line(
                        "command",
                        &row.command.replace('\n', " "),
                        width,
                        selected,
                    ),
                ]
            }
            Self::Compact => {
                let first = Line::from(vec![
                    Span::styled(marker.to_string(), selected_style),
                    Span::styled("id=", selected_style),
                    Span::styled(row.process_id.clone(), selected_style),
                    Span::styled(" | ", selected_style),
                    Span::styled(row.status.clone(), status_style(&row.status)),
                    Span::styled(" | ", selected_style),
                    Span::styled(format_started(row.started_at), selected_style),
                    Span::styled(" | ", selected_style),
                    Span::styled(format_elapsed(row.started_at), selected_style),
                ]);
                vec![
                    first,
                    process_detail_line("owner", owner, width, selected),
                    process_detail_line(
                        "command",
                        &row.command.replace('\n', " "),
                        width,
                        selected,
                    ),
                ]
            }
        }
    }
}

/// 将 CWD/COMMAND 等次级信息限制在单个视觉行内，避免 ChatWidget 再次 wrap 后破坏
/// `/ps` 的固定三行条目与按 entry 滚动语义。
fn process_detail_line(label: &str, value: &str, width: usize, selected: bool) -> Line<'static> {
    // primary row 的内容起始于 selection marker 之后；多留两格才会像 Subagents 面板的
    // secondary detail 一样相对主内容缩进，而不是与 process ID 齐头。
    let prefix = truncate(&format!("    {label}: "), width);
    let value_width = width.saturating_sub(UnicodeWidthStr::width(prefix.as_str()));
    let mut style = muted_style();
    if selected {
        style = style.add_modifier(Modifier::REVERSED);
    }
    Line::styled(format!("{prefix}{}", truncate(value, value_width)), style)
}

fn column(value: &str, width: usize) -> String {
    let value = truncate(value, width);
    format!("{value:<width$}")
}

fn status_style(status: &str) -> Style {
    match status {
        "running" => Style::default()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        "terminating" => Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
        _ => muted_style(),
    }
}

fn format_started(time: SystemTime) -> String {
    DateTime::<Local>::from(time)
        .format("%m-%d %H:%M")
        .to_string()
}

fn format_elapsed(time: SystemTime) -> String {
    let seconds = time
        .elapsed()
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    format_elapsed_seconds(seconds)
}

fn place_footer_at_bottom(lines: &mut Vec<Line<'static>>, footer: Line<'static>, budget: usize) {
    let budget = budget.max(1);
    if budget == 1 {
        lines.truncate(1);
        return;
    }
    lines.truncate(budget.saturating_sub(1));
    while lines.len().saturating_add(1) < budget {
        lines.push(Line::default());
    }
    lines.push(footer);
}

fn format_elapsed_seconds(seconds: u64) -> String {
    const MINUTE_SECS: u64 = 60;
    const HOUR_SECS: u64 = 60 * MINUTE_SECS;
    const DAY_SECS: u64 = 24 * HOUR_SECS;

    let days = seconds / DAY_SECS;
    let hours = (seconds % DAY_SECS) / HOUR_SECS;
    let minutes = (seconds % HOUR_SECS) / MINUTE_SECS;
    let seconds = seconds % MINUTE_SECS;

    if days > 0 {
        format!("{days}d{hours}h{minutes}m")
    } else if hours > 0 {
        format!("{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}s")
    } else {
        format!("{seconds}s")
    }
}

fn truncate(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    if UnicodeWidthStr::width(value) <= width {
        return value.to_string();
    }
    if width <= 1 {
        return "…".into();
    }
    let mut out = String::new();
    for ch in value.chars() {
        if UnicodeWidthStr::width(out.as_str()).saturating_add(ch.width().unwrap_or(0)) >= width {
            break;
        }
        out.push(ch);
    }
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_tui::theme::{BLUE_FG, BORDER_FG, MUTED_FG};
    use crossterm::event::KeyModifiers;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn row(index: usize, status: &str) -> ProcessSnapshot {
        ProcessSnapshot {
            process_id: format!("{index:08x}"),
            instance_id: u64::try_from(index).unwrap_or_default(),
            root_session_id: "session_aaaaaaaa".into(),
            subagent_id: (index % 2 == 1).then(|| format!("subagent-{index}")),
            status: status.into(),
            tty: index.is_multiple_of(2),
            command: format!("long command {index}\nwith a second line"),
            code_type: "bash".into(),
            cwd: "/workspace/example".into(),
            started_at: SystemTime::now(),
        }
    }

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    fn display_column(text: &str, needle: &str) -> Option<usize> {
        text.find(needle)
            .map(|byte_index| UnicodeWidthStr::width(&text[..byte_index]))
    }

    #[test]
    fn elapsed_keeps_seconds_and_displays_at_most_three_largest_units() {
        assert_eq!(format_elapsed_seconds(0), "0s");
        assert_eq!(format_elapsed_seconds(10), "10s");
        assert_eq!(format_elapsed_seconds(12 * 60 + 34), "12m34s");
        assert_eq!(
            format_elapsed_seconds(12 * 60 * 60 + 34 * 60 + 56),
            "12h34m56s"
        );
        assert_eq!(
            format_elapsed_seconds(2 * 24 * 60 * 60 + 8 * 60 * 60 + 50 * 60 + 59),
            "2d8h50m"
        );
        assert_eq!(
            column("12h34m56s", ELAPSED_COLUMN_WIDTH).trim_end(),
            "12h34m56s",
            "Wide process table must not truncate the longest sub-day example"
        );
    }

    #[test]
    fn elapsed_keeps_zero_lower_units_inside_the_selected_precision() {
        assert_eq!(format_elapsed_seconds(60), "1m0s");
        assert_eq!(format_elapsed_seconds(60 * 60), "1h0m0s");
        assert_eq!(format_elapsed_seconds(24 * 60 * 60), "1d0h0m");
    }

    #[test]
    fn list_scrolls_to_keep_selection_inside_viewport() {
        let mut panel = ProcessPanelState::default();
        panel.update((0..8).map(|index| row(index, "running")).collect());
        panel.open();
        for _ in 0..7 {
            panel.handle_key(key(KeyCode::Down));
        }

        let lines = panel.render_lines(110, 7);
        assert!(panel.list_offset.get() > 0);
        assert!(lines
            .iter()
            .map(line_text)
            .any(|text| text.contains("00000007")));
    }

    #[test]
    fn list_uses_fixed_three_visual_rows_and_hides_cwd_only_in_compact_mode() {
        for (width, height, expects_tty) in [(96, 8, true), (80, 8, true), (48, 8, false)] {
            let mut panel = ProcessPanelState::default();
            panel.update((0..8).map(|index| row(index, "running")).collect());
            panel.open();
            for _ in 0..7 {
                panel.handle_key(key(KeyCode::Down));
            }

            let lines = panel.render_lines(width, height);
            assert!(
                lines.iter().all(|line| line.width() <= usize::from(width)),
                "{width}-column list emitted a line that the ChatWidget would wrap"
            );
            assert!(lines.len() <= usize::from(height));
            assert!(lines
                .iter()
                .map(line_text)
                .any(|text| text.contains("00000007")));
            let text = lines.iter().map(line_text).collect::<Vec<_>>().join("\n");
            assert_eq!(
                text.contains("    owner:"),
                width < 79,
                "{width}-column list should use owner as the CWD fallback"
            );
            assert!(text.contains("    command:"), "{width}-column list");
            assert_eq!(
                text.contains("    cwd:"),
                width >= 79,
                "CWD preview should remain until the primary table becomes compact"
            );
            assert_eq!(
                text.contains("TTY"),
                expects_tty,
                "TTY is retained until compact mode"
            );
            assert_eq!(
                line_text(lines.last().expect("List help footer")),
                truncate("↑/↓ select · t terminate · Esc close", usize::from(width))
            );
        }
    }

    #[test]
    fn wide_table_header_reserves_selection_marker_and_matches_row_columns() {
        let process = row(0, "running");
        let layout = ProcessListLayout::for_width(132);
        let header = layout.header();
        let rendered_rows = layout.row(&process, "main", "› ", true, 132);
        assert_eq!(rendered_rows.len(), 3);
        let rendered_row = rendered_rows[0].to_string();

        assert!(UnicodeWidthStr::width(header.as_str()) <= 132);
        assert_eq!(
            UnicodeWidthStr::width(header.as_str()),
            UnicodeWidthStr::width(rendered_row.as_str())
        );
        let started = format_started(process.started_at);
        let elapsed = format_elapsed(process.started_at);
        for (header_label, row_value) in [
            ("PROCESS ID", "00000000"),
            ("OWNER", "main"),
            ("STATUS", "running"),
            ("TTY", "yes"),
            ("STARTED", started.as_str()),
            ("ELAPSED", elapsed.as_str()),
        ] {
            assert_eq!(
                display_column(&header, header_label),
                display_column(&rendered_row, row_value),
                "{header_label} should start at the same column as {row_value}"
            );
        }
        assert!(
            line_text(&rendered_rows[1]).starts_with("    cwd: /workspace/example"),
            "CWD should occupy the entry's second visual row"
        );
        assert!(
            line_text(&rendered_rows[2])
                .starts_with("    command: long command 0 with a second line"),
            "COMMAND should occupy the entry's third visual row"
        );
    }

    #[test]
    fn detail_rows_are_selected_as_part_of_their_process_entry() {
        let process = row(0, "running");
        let rows = ProcessListLayout::for_width(132).row(&process, "main", "› ", true, 132);

        assert!(rows[1..]
            .iter()
            .all(|line| line.style.add_modifier.contains(Modifier::REVERSED)));
        assert!(rows[1..].iter().all(|line| line.style.fg == Some(MUTED_FG)));
        assert_eq!(rows[0].spans[5].style.fg, Some(Color::Green));
    }

    #[test]
    fn list_title_and_header_follow_mcp_panel_visual_language() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(0, "running")]);
        panel.open();

        let lines = panel.render_lines(132, 12);
        let title = lines.first().expect("Process title");
        assert_eq!(line_text(title), "Processes · live processes");
        assert_eq!(title.spans[0].style.fg, Some(BORDER_FG));
        assert_eq!(title.spans[1].style.fg, Some(MUTED_FG));
        assert_eq!(title.spans[2].style.fg, Some(BLUE_FG));
        assert!(title
            .spans
            .iter()
            .all(|span| span.style.add_modifier.contains(Modifier::BOLD)));
        assert!(title
            .spans
            .iter()
            .all(|span| span.style.add_modifier.contains(Modifier::UNDERLINED)));

        let header = lines
            .iter()
            .find(|line| line_text(line).contains("PROCESS ID"))
            .expect("Process table header");
        assert_eq!(header.style.fg, Some(MUTED_FG));
        assert!(!header.style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn empty_list_omits_the_table_header() {
        let mut panel = ProcessPanelState::default();
        panel.open();

        let text = panel
            .render_lines(132, 12)
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("No live managed processes."));
        assert!(!text.contains("PROCESS ID"));
        assert!(!text.contains("COMMAND"));
    }

    #[test]
    fn terminating_row_ignores_terminate_key() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(1, "terminating")]);
        panel.open();

        assert_eq!(
            panel.handle_key(key(KeyCode::Char('t'))),
            ProcessPanelKeyAction::None
        );
        assert!(matches!(panel.view, ProcessPanelView::List));
    }

    #[test]
    fn optimistic_terminating_row_renders_before_authoritative_refresh_removes_it() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(1, "running")]);
        panel.open();

        panel.mark_terminating(&ProcessTerminationTarget::from_row(&row(1, "running")));
        let pending = panel
            .render_lines(96, 10)
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(pending.contains("terminating"));

        panel.update(Vec::new());
        let refreshed = panel
            .render_lines(96, 10)
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!refreshed.contains("00000001"));
    }

    #[test]
    fn list_never_renders_unmanaged_starting_status() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(1, "starting"), row(2, "running")]);
        panel.open();

        let text = panel
            .render_lines(96, 10)
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("00000002"));
        assert!(!text.contains("00000001"));
        assert!(!text.contains("starting"));
    }

    #[test]
    fn confirmation_is_full_page_with_fixed_footer_and_ignores_other_keys() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(1, "running")]);
        panel.open();
        panel.handle_key(key(KeyCode::Char('t')));
        assert!(matches!(
            panel.view,
            ProcessPanelView::TerminateConfirm { .. }
        ));

        assert_eq!(
            panel.handle_key(key(KeyCode::Char('x'))),
            ProcessPanelKeyAction::None
        );
        assert!(matches!(
            panel.view,
            ProcessPanelView::TerminateConfirm { .. }
        ));
        let lines = panel.render_lines(32, 8);
        assert_eq!(
            line_text(lines.last().expect("Confirmation footer")),
            "[y] Yes · [n/Esc] No"
        );
        assert_eq!(
            panel.handle_key(key(KeyCode::Esc)),
            ProcessPanelKeyAction::Refresh
        );
        assert!(matches!(panel.view, ProcessPanelView::List));
    }

    #[test]
    fn confirmation_uses_the_process_panel_title_and_explicit_red_prompt() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(1, "running")]);
        panel.open();
        panel.handle_key(key(KeyCode::Char('t')));

        let lines = panel.render_lines(120, 30);
        let title = lines.first().expect("Confirmation title");
        assert_eq!(line_text(title), "Processes · terminate");
        assert_eq!(title.spans[0].style.fg, Some(BORDER_FG));
        assert_eq!(title.spans[1].style.fg, Some(MUTED_FG));
        assert_eq!(title.spans[2].style.fg, Some(BLUE_FG));
        assert!(title
            .spans
            .iter()
            .all(|span| span.style.add_modifier.contains(Modifier::BOLD)));
        assert!(title
            .spans
            .iter()
            .all(|span| span.style.add_modifier.contains(Modifier::UNDERLINED)));
        assert!(line_text(&lines[1]).is_empty());
        assert_eq!(
            line_text(&lines[2]),
            "Confirm to terminate process 00000001?"
        );
        assert_eq!(lines[2].style.fg, Some(Color::Red));
        assert!(line_text(&lines[3]).is_empty());
        assert_eq!(line_text(&lines[4]), "Process ID: 00000001");
    }

    #[test]
    fn confirmation_ignores_modified_confirmation_keys() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(1, "running")]);
        panel.open();
        panel.handle_key(key(KeyCode::Char('t')));

        for modifiers in [KeyModifiers::CONTROL, KeyModifiers::ALT] {
            assert_eq!(
                panel.handle_key(KeyEvent::new(KeyCode::Char('y'), modifiers)),
                ProcessPanelKeyAction::None
            );
            assert!(matches!(
                panel.view,
                ProcessPanelView::TerminateConfirm { .. }
            ));
            assert_eq!(
                panel.handle_key(KeyEvent::new(KeyCode::Char('n'), modifiers)),
                ProcessPanelKeyAction::None
            );
            assert!(matches!(
                panel.view,
                ProcessPanelView::TerminateConfirm { .. }
            ));
        }

        assert!(matches!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('Y'), KeyModifiers::SHIFT)),
            ProcessPanelKeyAction::Terminate { .. }
        ));
    }

    #[test]
    fn natural_exit_after_confirmation_returns_to_list_without_retargeting_a_new_row() {
        let mut panel = ProcessPanelState::default();
        panel.update(vec![row(1, "running"), row(2, "running")]);
        panel.open();
        panel.handle_key(key(KeyCode::Char('t')));
        assert!(matches!(
            panel.view,
            ProcessPanelView::TerminateConfirm { .. }
        ));

        // 模拟用户确认后、runtime 发 SIGKILL 前目标自然退出。刷新只能移除原 process，
        // 不能让确认态错误地落到同一 index 的新行。
        let target = ProcessTerminationTarget::from_row(&row(1, "running"));
        assert_eq!(
            panel.handle_key(key(KeyCode::Char('y'))),
            ProcessPanelKeyAction::Terminate {
                target: target.clone()
            }
        );
        panel.mark_terminating(&target);
        // App 的 runtime terminate worker 会把“Already exited”连同 authoritative snapshot
        // 一起回灌；这里模拟该错误路径，而不是让状态机猜测任意失败原因。
        panel.set_notice("Already exited");
        panel.update(vec![row(2, "running")]);

        assert!(matches!(panel.view, ProcessPanelView::List));
        let text = panel
            .render_lines(96, 10)
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("00000002"));
        assert!(!text.contains("00000001"));
        assert!(text.contains("Already exited"));
    }

    #[test]
    fn confirmation_does_not_retarget_a_reused_process_id() {
        let mut panel = ProcessPanelState::default();
        let original = row(1, "running");
        panel.update(vec![original.clone()]);
        panel.open();
        panel.handle_key(key(KeyCode::Char('t')));

        let mut replacement = original;
        replacement.instance_id = replacement.instance_id.saturating_add(1);
        panel.update(vec![replacement]);

        assert!(matches!(panel.view, ProcessPanelView::List));
        assert_eq!(panel.notice.as_deref(), Some("Already exited"));
        assert_eq!(
            panel.handle_key(key(KeyCode::Char('y'))),
            ProcessPanelKeyAction::None,
            "A reused logical ID must not remain an actionable confirmation target"
        );
    }

    #[test]
    fn short_confirmation_viewport_keeps_command_body_scrollable_and_footer_fixed() {
        let mut panel = ProcessPanelState::default();
        let mut process = row(1, "running");
        process.command = "first command line\nsecond command line\nthird command line".into();
        panel.update(vec![process]);
        panel.open();
        panel.handle_key(key(KeyCode::Char('t')));

        let initial = panel.render_lines(36, 8);
        let initial_text = initial.iter().map(line_text).collect::<Vec<_>>().join("\n");
        assert!(initial_text.contains("Command:"));
        assert!(initial_text.contains("first command line"));
        assert_eq!(
            line_text(initial.last().expect("Fixed confirmation footer")),
            "[y] Yes · [n/Esc] No"
        );

        panel.handle_key(key(KeyCode::Down));
        let scrolled = panel.render_lines(36, 8);
        let scrolled_text = scrolled
            .iter()
            .map(line_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(scrolled_text.contains("second command line"));
        assert_eq!(
            line_text(scrolled.last().expect("Fixed confirmation footer")),
            "[y] Yes · [n/Esc] No"
        );
    }

    #[test]
    fn confirmation_never_needs_second_visual_wrap_at_narrow_widths() {
        let mut panel = ProcessPanelState::default();
        let mut process = row(1, "running");
        process.subagent_id = Some("subagent-with-a-long-owner-name".into());
        process.command =
            "very-long-command-token-that-must-wrap-visually\nsecond-long-command-token".into();
        panel.update(vec![process]);
        panel.open();
        panel.handle_key(key(KeyCode::Char('t')));

        for (width, height) in [(1, 7), (2, 7), (8, 7), (12, 8), (24, 10)] {
            let lines = panel.render_lines(width, height);
            assert!(
                lines.iter().all(|line| line.width() <= usize::from(width)),
                "Confirmation emitted a logical line wider than {width} columns"
            );
            assert!(lines.len() <= usize::from(height));
            assert_eq!(
                line_text(lines.last().expect("Fixed footer")),
                truncate(
                    if width >= 42 {
                        "[y] Yes                         [n/Esc] No"
                    } else {
                        "[y] Yes · [n/Esc] No"
                    },
                    usize::from(width),
                )
            );
        }
    }

    #[test]
    fn running_and_terminating_statuses_use_prd_colors_and_bold() {
        let running = status_style("running");
        assert_eq!(running.fg, Some(Color::Green));
        assert!(running.add_modifier.contains(Modifier::BOLD));

        let terminating = status_style("terminating");
        assert_eq!(terminating.fg, Some(Color::Yellow));
        assert!(terminating.add_modifier.contains(Modifier::BOLD));
    }
}
