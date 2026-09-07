//! TUI transcript 状态与历史 cell 管理。
//!
//! 本模块维护对话历史、活动 assistant/tool cell 与活动提示。
//! finalized 历史可逐条落到终端 scrollback，活动内容保留给 live region 重绘。

use std::time::Instant;

use ratatui::text::Line;

use super::cell::{
    help_status_text, AssistantCell, ErrorCell, HistoryCell, HistoryEntry, ShellCommandCell,
    StatusCell, ToolCell, UserCell,
};
use super::wrapping::hard_wrap_styled_lines;
use crate::agent::UserShellCommandStatus;
use crate::api::{ToolCallSkipReason, ToolExecutionOutcome};
use crate::tool::diff::FileChange;

pub(super) struct ShellCommandCompletion {
    pub(super) status: UserShellCommandStatus,
    pub(super) exit_code: Option<i32>,
    pub(super) duration_ms: u128,
    pub(super) stdout: String,
    pub(super) stderr: String,
    pub(super) truncated: bool,
}

enum ActiveTimelineChunk {
    Assistant(Vec<Line<'static>>),
    Fixed(Vec<Line<'static>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BackgroundToolUpdate {
    Updated { was_flushed: bool },
    AwaitingToolResult,
    Ignored,
}

#[derive(Debug, Clone, Default)]
pub(super) struct TranscriptState {
    history: Vec<HistoryEntry>,
    active_user: Option<usize>,
    active_assistant: Option<usize>,
    active_assistant_accepted_bytes: usize,
    activity: Option<String>,
    flushed_until: usize,
}

impl TranscriptState {
    pub(super) fn set_activity(&mut self, activity: Option<String>) {
        self.activity = activity;
    }

    #[cfg(test)]
    pub(super) fn transcript_text(&self) -> String {
        self.plain_lines().join("\n")
    }

    #[cfg(test)]
    pub(super) fn plain_lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for entry in &self.history {
            lines.extend(entry.plain_lines());
        }
        trim_trailing_blank(&mut lines);
        lines
    }

    pub(super) fn render_lines_with_width(&self, width: u16) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        for (index, entry) in self.history.iter().enumerate() {
            push_history_entry_lines(&self.history, index, entry, width, &mut lines);
        }
        if let Some(activity) = &self.activity {
            lines.push(activity_line(activity));
        }
        trim_trailing_blank_lines(&mut lines);
        lines
    }

    pub(super) fn scrollback_lines(&self, width: u16) -> ScrollbackLines {
        let mut lines = Vec::new();
        let mut entry_count = 0usize;
        let starts_at_history_beginning = self.flushed_until == 0;
        let mut index = self.flushed_until;
        let mut ended_at_active_user = false;
        while index < self.history.len() {
            if self
                .active_user
                .is_some_and(|active_user| index > active_user)
                || Some(index) == self.active_assistant
            {
                break;
            }
            let Some(entry) = self.history.get(index) else {
                break;
            };
            if !entry_is_finalized(entry) {
                break;
            }
            push_history_entry_lines(&self.history, index, entry, width, &mut lines);
            entry_count = entry_count.saturating_add(1);
            if Some(index) == self.active_user {
                ended_at_active_user = true;
                break;
            }
            index = index.saturating_add(1);
        }
        if ended_at_active_user {
            trim_trailing_blank_lines_keep_one(&mut lines);
        } else {
            trim_trailing_blank_lines(&mut lines);
        }
        ScrollbackLines {
            lines,
            entry_count,
            starts_at_history_beginning,
        }
    }

    pub(super) fn mark_scrollback_flushed(&mut self, entry_count: usize) {
        self.flushed_until = self.flushed_until.saturating_add(entry_count);
    }

    pub(super) fn reset_scrollback_flushed(&mut self) {
        self.flushed_until = 0;
    }

    pub(super) fn last_committed_assistant_text(&self) -> Option<&str> {
        self.history
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, entry)| {
                if Some(index) == self.active_assistant {
                    return None;
                }
                match entry {
                    HistoryEntry::Assistant(cell) => Some(cell.text.as_str()),
                    _ => None,
                }
            })
            .filter(|text| !text.trim().is_empty())
    }

    pub(super) fn clear(&mut self) {
        self.history.clear();
        self.active_user = None;
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
        self.activity = None;
        self.flushed_until = 0;
    }

    /// 返回当前 turn 的完整 live timeline；上层在最终换行后统一按虚线框高度裁剪。
    pub(super) fn active_timeline_lines(&self, width: u16) -> Vec<Line<'static>> {
        let chunks = self.active_timeline_chunks(width);
        render_active_timeline(chunks)
    }

    /// 测试与非高度受限投影使用的完整 current-turn timeline。
    #[cfg(test)]
    pub(super) fn active_assistant_lines(&self, width: u16) -> Vec<Line<'static>> {
        self.active_timeline_lines(width)
    }

    fn active_timeline_chunks(&self, width: u16) -> Vec<ActiveTimelineChunk> {
        let has_active_turn = self.active_user.is_some() || self.active_assistant.is_some();
        let start = self
            .active_user
            .map(|index| index.saturating_add(1))
            .or(self.active_assistant)
            .unwrap_or(self.flushed_until);
        let mut chunks = Vec::new();
        for (index, entry) in self.history.iter().enumerate().skip(start) {
            match entry {
                HistoryEntry::Assistant(_) if has_active_turn => {
                    let mut lines = entry.display_lines(width);
                    if needs_blank_before_entry(&self.history, index) {
                        lines.insert(0, Line::default());
                    }
                    chunks.push(ActiveTimelineChunk::Assistant(lines))
                }
                HistoryEntry::Status(_) if has_active_turn => {
                    chunks.push(fixed_timeline_chunk(entry.display_lines(width), width))
                }
                HistoryEntry::Tool(_) if has_active_turn => {
                    let mut lines = entry.live_status_lines(width);
                    if needs_blank_before_entry(&self.history, index) {
                        lines.insert(0, Line::default());
                    }
                    chunks.push(fixed_timeline_chunk(lines, width))
                }
                HistoryEntry::Tool(cell) if !cell.completed => {
                    chunks.push(fixed_timeline_chunk(entry.live_status_lines(width), width))
                }
                HistoryEntry::ShellCommand(cell) if !cell.is_finalized() => {
                    chunks.push(fixed_timeline_chunk(entry.display_lines(width), width))
                }
                _ => {}
            }
        }
        if let Some(activity) = &self.activity {
            let mut lines = vec![activity_line(activity)];
            // activity 不是 history entry，无法经过统一的 entry gap 判断；
            // Warning / Status 后需在 live region 主动补回恰好一行间隔。
            if matches!(self.history.last(), Some(HistoryEntry::Status(_))) {
                lines.insert(0, Line::default());
            }
            chunks.push(fixed_timeline_chunk(lines, width));
        }
        if let Some(last_chunk) = chunks.last_mut() {
            match last_chunk {
                ActiveTimelineChunk::Assistant(lines) | ActiveTimelineChunk::Fixed(lines) => {
                    trim_trailing_blank_lines(lines);
                }
            }
        }
        chunks
    }

    pub(super) fn has_active_user(&self) -> bool {
        self.active_user.is_some()
    }

    pub(super) fn push_help(&mut self) {
        self.push_system(help_status_text());
    }

    pub(super) fn push_error(&mut self, message: impl Into<String>) {
        self.history.push(HistoryEntry::Error(ErrorCell {
            text: message.into(),
            leading_gap_after_flushed_user: true,
            indent_continuations: true,
        }));
    }

    pub(super) fn push_startup_error(&mut self, message: impl Into<String>) {
        self.history.push(HistoryEntry::Error(ErrorCell {
            text: message.into(),
            leading_gap_after_flushed_user: true,
            indent_continuations: false,
        }));
    }

    pub(super) fn push_turn_error(&mut self, message: impl Into<String>) {
        self.history.push(HistoryEntry::Error(ErrorCell {
            text: message.into(),
            // 活动 User flush 时已经保留一行；Turn failed 在下一批不能再补第二行。
            leading_gap_after_flushed_user: false,
            indent_continuations: true,
        }));
    }

    pub(super) fn push_system(&mut self, message: impl Into<String>) {
        self.history.push(HistoryEntry::Status(StatusCell {
            text: message.into(),
            leading_gap_after_flushed_user: false,
        }));
    }

    pub(super) fn push_warning(&mut self, message: impl Into<String>) {
        let leading_gap_after_flushed_user = self.active_user.is_none()
            && matches!(self.history.last(), Some(HistoryEntry::User(_)));
        self.history.push(HistoryEntry::Status(StatusCell {
            text: message.into(),
            leading_gap_after_flushed_user,
        }));
    }

    pub(super) fn push_system_after_flushed_user(&mut self, message: impl Into<String>) {
        self.history.push(HistoryEntry::Status(StatusCell {
            text: message.into(),
            leading_gap_after_flushed_user: true,
        }));
    }

    pub(super) fn push_user(&mut self, text: String) {
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
        self.history.push(HistoryEntry::User(UserCell { text }));
    }

    pub(super) fn push_active_user(&mut self, text: String) {
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
        let index = self.history.len();
        self.history.push(HistoryEntry::User(UserCell { text }));
        self.active_user = Some(index);
    }

    pub(super) fn push_assistant_delta(&mut self, text: String) {
        if let Some(index) = self.active_assistant {
            if let Some(HistoryEntry::Assistant(cell)) = self.history.get_mut(index) {
                cell.text.push_str(&text);
                return;
            }
        }

        let index = self.history.len();
        self.history
            .push(HistoryEntry::Assistant(AssistantCell { text }));
        self.active_assistant = Some(index);
        self.active_assistant_accepted_bytes = 0;
    }

    pub(super) fn push_historical_assistant(&mut self, text: String) {
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
        self.history
            .push(HistoryEntry::Assistant(AssistantCell { text }));
    }

    pub(super) fn complete_assistant_message(&mut self, text: String) {
        if let Some(index) = self.active_assistant {
            if let Some(HistoryEntry::Assistant(cell)) = self.history.get_mut(index) {
                cell.text = text;
                self.active_assistant_accepted_bytes = cell.text.len();
                if self.active_user.is_none() {
                    self.active_assistant = None;
                    self.active_assistant_accepted_bytes = 0;
                }
                return;
            }
        }

        let accepted_bytes = text.len();
        let index = self.history.len();
        self.history
            .push(HistoryEntry::Assistant(AssistantCell { text }));
        if self.active_user.is_some() {
            self.active_assistant = Some(index);
            self.active_assistant_accepted_bytes = accepted_bytes;
        }
    }

    pub(super) fn accept_active_assistant_output(&mut self) {
        let Some(index) = self.active_assistant else {
            return;
        };
        if let Some(HistoryEntry::Assistant(cell)) = self.history.get(index) {
            self.active_assistant_accepted_bytes = cell.text.len();
        }
    }

    pub(super) fn discard_active_assistant(&mut self) {
        let Some(index) = self.active_assistant else {
            return;
        };
        if self.active_assistant_accepted_bytes > 0 {
            if let Some(HistoryEntry::Assistant(cell)) = self.history.get_mut(index) {
                cell.text.truncate(self.active_assistant_accepted_bytes);
            }
            return;
        }
        self.active_assistant = None;
        if index.saturating_add(1) == self.history.len()
            && matches!(self.history.last(), Some(HistoryEntry::Assistant(_)))
        {
            self.history.pop();
        } else if let Some(HistoryEntry::Assistant(cell)) = self.history.get_mut(index) {
            cell.text.clear();
        }
    }

    pub(super) fn push_tool_started(
        &mut self,
        turn_id: Option<String>,
        id: String,
        name: String,
        summary: String,
    ) {
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
        self.history.push(HistoryEntry::Tool(ToolCell {
            turn_id,
            id,
            name,
            summary: summary.clone(),
            started_summary: Some(summary),
            progress_summary: None,
            outcome: None,
            completed: false,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: Instant::now(),
            file_change: None,
        }));
    }

    pub(super) fn update_tool_progress(&mut self, id: String, summary: String) {
        for entry in self.history.iter_mut().rev() {
            if let HistoryEntry::Tool(cell) = entry {
                if cell.id == id && !cell.completed {
                    cell.progress_summary = Some(summary);
                    return;
                }
            }
        }
    }

    pub(super) fn complete_tool(
        &mut self,
        id: String,
        summary: String,
        file_change: Option<FileChange>,
        outcome: ToolExecutionOutcome,
    ) {
        for entry in self.history.iter_mut().rev() {
            if let HistoryEntry::Tool(cell) = entry {
                if cell.id == id && !cell.completed {
                    cell.summary = summary;
                    cell.outcome = Some(outcome);
                    cell.completed = true;
                    cell.interrupted = false;
                    cell.skipped = false;
                    cell.skip_reason = None;
                    cell.file_change = file_change;
                    return;
                }
            }
        }

        self.history.push(HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id,
            name: "tool".into(),
            summary,
            started_summary: None,
            progress_summary: None,
            outcome: Some(outcome),
            completed: true,
            interrupted: false,
            skipped: false,
            skip_reason: None,
            started_at: Instant::now(),
            file_change,
        }));
    }

    /// watcher 的终态不是第二个模型 tool result，只更新原 code_run 的展示投影。
    /// 完成事件可能抢在原 ToolCallCompleted / ToolCallInterrupted 前到达，调用方需在
    /// `AwaitingToolResult` 时暂存并于原 tool result 落地后重试。
    pub(super) fn complete_background_tool(
        &mut self,
        turn_id: &str,
        id: &str,
        outcome: ToolExecutionOutcome,
    ) -> BackgroundToolUpdate {
        let Some((index, cell)) =
            self.history
                .iter_mut()
                .enumerate()
                .rev()
                .find_map(|(index, entry)| match entry {
                    HistoryEntry::Tool(cell)
                        if cell.turn_id.as_deref() == Some(turn_id) && cell.id == id =>
                    {
                        Some((index, cell))
                    }
                    _ => None,
                })
        else {
            return BackgroundToolUpdate::AwaitingToolResult;
        };
        if !cell.completed {
            return BackgroundToolUpdate::AwaitingToolResult;
        }
        if !cell.interrupted && !matches!(cell.outcome, Some(ToolExecutionOutcome::ProcessRunning))
        {
            return BackgroundToolUpdate::Ignored;
        }

        cell.summary = format!("tool {} background_completed", cell.name);
        cell.outcome = Some(outcome);
        cell.progress_summary = None;
        BackgroundToolUpdate::Updated {
            was_flushed: index < self.flushed_until,
        }
    }

    pub(super) fn interrupt_tool(&mut self, id: String, summary: String) {
        for entry in self.history.iter_mut().rev() {
            if let HistoryEntry::Tool(cell) = entry {
                if cell.id == id && !cell.completed {
                    cell.summary = summary;
                    cell.outcome = None;
                    cell.completed = true;
                    cell.interrupted = true;
                    cell.skipped = false;
                    cell.skip_reason = None;
                    return;
                }
            }
        }

        self.history.push(HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id,
            name: "tool".into(),
            summary,
            started_summary: None,
            progress_summary: None,
            outcome: None,
            completed: true,
            interrupted: true,
            skipped: false,
            skip_reason: None,
            started_at: Instant::now(),
            file_change: None,
        }));
    }

    pub(super) fn push_tool_skipped(
        &mut self,
        id: String,
        name: String,
        summary: String,
        reason: ToolCallSkipReason,
    ) {
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
        self.history.push(HistoryEntry::Tool(ToolCell {
            turn_id: None,
            id,
            name,
            summary,
            started_summary: None,
            progress_summary: None,
            outcome: None,
            completed: true,
            interrupted: false,
            skipped: true,
            skip_reason: Some(reason),
            started_at: Instant::now(),
            file_change: None,
        }));
    }

    pub(super) fn push_shell_started(&mut self, command: String) {
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
        self.history
            .push(HistoryEntry::ShellCommand(ShellCommandCell::running(
                command,
            )));
    }

    pub(super) fn complete_shell(&mut self, command: String, completion: ShellCommandCompletion) {
        for entry in self.history.iter_mut().rev() {
            if let HistoryEntry::ShellCommand(cell) = entry {
                if cell.command == command && !cell.is_finalized() {
                    cell.complete(
                        completion.status,
                        completion.exit_code,
                        completion.duration_ms,
                        completion.stdout,
                        completion.stderr,
                        completion.truncated,
                    );
                    return;
                }
            }
        }

        let mut cell = ShellCommandCell::running(command);
        cell.complete(
            completion.status,
            completion.exit_code,
            completion.duration_ms,
            completion.stdout,
            completion.stderr,
            completion.truncated,
        );
        self.history.push(HistoryEntry::ShellCommand(cell));
    }

    pub(super) fn fail_shell(&mut self, command: String, error: String) {
        for entry in self.history.iter_mut().rev() {
            if let HistoryEntry::ShellCommand(cell) = entry {
                if cell.command == command && !cell.is_finalized() {
                    cell.fail(error);
                    return;
                }
            }
        }

        let mut cell = ShellCommandCell::running(command);
        cell.fail(error);
        self.history.push(HistoryEntry::ShellCommand(cell));
    }

    pub(super) fn clear_active_assistant(&mut self) {
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
    }

    pub(super) fn commit_active_turn(&mut self) {
        self.active_user = None;
        self.active_assistant = None;
        self.active_assistant_accepted_bytes = 0;
    }

    pub(super) fn release_active_user(&mut self) {
        self.active_user = None;
    }
}

pub(super) struct ScrollbackLines {
    pub(super) lines: Vec<Line<'static>>,
    pub(super) entry_count: usize,
    pub(super) starts_at_history_beginning: bool,
}

fn entry_is_finalized(entry: &HistoryEntry) -> bool {
    match entry {
        HistoryEntry::Tool(cell) => cell.completed,
        HistoryEntry::ShellCommand(cell) => cell.is_finalized(),
        _ => true,
    }
}

fn push_history_entry_lines(
    history: &[HistoryEntry],
    index: usize,
    entry: &HistoryEntry,
    width: u16,
    lines: &mut Vec<Line<'static>>,
) {
    if needs_blank_before_entry(history, index) && !lines.last().is_some_and(line_is_blank) {
        lines.push(Line::default());
    }
    lines.extend(entry.display_lines(width));
}

fn needs_blank_before_entry(history: &[HistoryEntry], index: usize) -> bool {
    if index == 0 {
        return false;
    }

    matches!(history.get(index), Some(HistoryEntry::ShellCommand(_)))
        // Assistant 可能已在上一批写入终端 scrollback，尾部空行也随该批次被裁掉。
        // 此后才产生的本地错误需要显式补 gap，避免附件预检等错误紧贴上一条回答。
        || matches!(
            (history.get(index.saturating_sub(1)), history.get(index)),
            (
                Some(HistoryEntry::Assistant(_)),
                Some(HistoryEntry::Error(_))
            )
        )
        // 本地预检失败的 User 若已单独 flush 且尾部被裁掉，需要在错误前补 gap；
        // Turn failed 的活动 User 已经保留一行，不能在下一批重复补第二行。
        || matches!(
            (history.get(index.saturating_sub(1)), history.get(index)),
            (
                Some(HistoryEntry::User(_)),
                Some(HistoryEntry::Error(ErrorCell {
                    leading_gap_after_flushed_user: true,
                    ..
                }))
            )
        )
        // `/inbox` 的命令 echo 可能先单独 flush，User cell 自带的尾部空行会被裁掉。
        // 仅明确标记的异步状态补回这一行，避免影响已保留间隔的普通 User。
        || matches!(
            (history.get(index.saturating_sub(1)), history.get(index)),
            (
                Some(HistoryEntry::User(_)),
                Some(HistoryEntry::Status(StatusCell {
                    leading_gap_after_flushed_user: true,
                    ..
                }))
            )
        )
        || matches!(
            (history.get(index.saturating_sub(1)), history.get(index)),
            (
                Some(HistoryEntry::Status(_)),
                Some(
                    HistoryEntry::Assistant(_) | HistoryEntry::Tool(_) | HistoryEntry::Error(_)
                )
            )
        )
        || matches!(
            (history.get(index.saturating_sub(1)), history.get(index)),
            (
                Some(
                    HistoryEntry::Assistant(_)
                        | HistoryEntry::ShellCommand(_)
                        | HistoryEntry::Status(_)
                        | HistoryEntry::Error(_)
                        // slash command echo 也是 User cell。它在完成后若没有 status（例如
                        // 成功 /compact），下一次用户输入会从已 flush 的 scrollback 继续写入；
                        // 这里仍要显式补 gap，避免两条灰色 user bar 贴在一起。
                        | HistoryEntry::User(_),
                ),
                Some(HistoryEntry::User(_))
            )
        )
}

fn line_is_blank(line: &Line<'_>) -> bool {
    line.spans.is_empty() || line.to_string().trim().is_empty()
}

fn activity_line(activity: &str) -> Line<'static> {
    Line::styled(
        format!("  {activity}"),
        ratatui::style::Style::default().fg(ratatui::style::Color::DarkGray),
    )
}

fn fixed_timeline_chunk(lines: Vec<Line<'static>>, width: u16) -> ActiveTimelineChunk {
    ActiveTimelineChunk::Fixed(hard_wrap_styled_lines(lines, usize::from(width.max(1))))
}

fn render_active_timeline(chunks: Vec<ActiveTimelineChunk>) -> Vec<Line<'static>> {
    let mut rendered = Vec::new();

    for chunk in chunks {
        match chunk {
            ActiveTimelineChunk::Assistant(lines) | ActiveTimelineChunk::Fixed(lines) => {
                rendered.extend(lines);
            }
        }
    }
    trim_trailing_blank_lines(&mut rendered);
    rendered
}

fn trim_trailing_blank_lines(lines: &mut Vec<Line<'static>>) {
    while matches!(lines.last(), Some(line) if line_is_blank(line)) {
        lines.pop();
    }
}

fn trim_trailing_blank_lines_keep_one(lines: &mut Vec<Line<'static>>) {
    let mut trailing_blank_count = 0usize;
    for line in lines.iter().rev() {
        if line_is_blank(line) {
            trailing_blank_count = trailing_blank_count.saturating_add(1);
        } else {
            break;
        }
    }
    let remove_count = trailing_blank_count.saturating_sub(1);
    for _ in 0..remove_count {
        lines.pop();
    }
}

#[cfg(test)]
fn trim_trailing_blank(lines: &mut Vec<String>) {
    while matches!(lines.last(), Some(line) if line.is_empty()) {
        lines.pop();
    }
}
