//! session TUI 聊天主视图。
//!
//! `ChatWidget` 是 TUI 的交互表面：它消费 `SessionEvent` 更新展示状态，
//! 将按键转换成 `AppEvent` 意图，并负责状态栏、transcript 与 bottom pane 渲染。

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::agent::{SessionEvent, SessionRuntimeStatus};
use crate::config::{AgentSessionTuiConfig, DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES};

use super::app_event::AppEventSender;
use super::at_path::scan_at_path_tokens;
use super::attachment::{read_clipboard_image_blocking, resolve_at_paths};
use super::bottom_pane::{
    classify_input, input_accepts_text, is_shift_enter_newline, InputAction, PreviewHit,
};
use super::input_queue::QueuedInput;
use super::mcp_panel::McpPanelKeyAction;
use super::process_panel::ProcessPanelKeyAction;
use super::state::{
    ContributionKind, SessionTuiState, ATTACHMENT_STEER_QUEUE_NOTICE,
    SLASH_COMMAND_STEER_QUEUE_NOTICE,
};
use super::theme::{apply_surface_style, muted_style, BLUE_FG, SURFACE_BG};
use super::turn_animation::{BOARD_WIDTH, TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS};
use super::welcome::startup_welcome_lines;
use super::wrapping::hard_wrap_styled_lines;

const FOOTER_SEPARATOR: &str = "  ·  ";
// 底部栏 slogan 文案与其触发的最小终端宽度（够宽才在 footer 末尾补这句）。
const FOOTER_SLOGAN: &str = "让判断流动，让知识沉淀";
const FOOTER_SLOGAN_MIN_WIDTH: u16 = 150;
const TURN_ANIMATION_SIDECAR_WIDTH: usize = BOARD_WIDTH + 2;
const TURN_ANIMATION_MIN_TEXT_WIDTH: usize = 16;
const COMPOSER_TOP_SPACER_ROWS: usize = 1;
const LIVE_BOX_LEFT_BORDER: &str = "┆ ";
const LIVE_BOX_RIGHT_BORDER: &str = " ┆";
const FOOTER_LABEL_FG: Color = Color::DarkGray;
const FOOTER_MODEL_FG: Color = Color::Rgb(188, 90, 62);
const FOOTER_CWD_FG: Color = Color::Rgb(34, 72, 136);
const FOOTER_BRANCH_FG: Color = Color::Rgb(130, 68, 176);
const FOOTER_CTX_FG: Color = Color::Rgb(182, 54, 54);
const FOOTER_FOCUS_FG: Color = Color::Rgb(148, 108, 34);
const AUTO_LIVE_RESPONSE_PREVIEW_MAX_LINES: usize = usize::MAX;

pub(super) struct ChatWidget {
    state: SessionTuiState,
    app_event_tx: AppEventSender,
    live_response_preview_max_lines: usize,
}

impl ChatWidget {
    pub(super) fn new(app_event_tx: AppEventSender) -> Self {
        Self {
            state: SessionTuiState::new(),
            app_event_tx,
            live_response_preview_max_lines: resolve_live_response_preview_max_lines(
                DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES,
            ),
        }
    }

    pub(super) fn state(&self) -> &SessionTuiState {
        &self.state
    }

    pub(super) fn state_mut(&mut self) -> &mut SessionTuiState {
        &mut self.state
    }

    pub(super) fn event_tx(&self) -> AppEventSender {
        self.app_event_tx.clone()
    }

    pub(super) fn set_tui_config(&mut self, config: AgentSessionTuiConfig) {
        self.live_response_preview_max_lines =
            resolve_live_response_preview_max_lines(config.live_response_preview_max_lines);
    }

    pub(super) fn refresh_at_path_completion(&mut self) {
        let Some((generation, directory, max_entries)) = self.state.begin_at_path_scan() else {
            return;
        };
        self.app_event_tx
            .at_path_directory_scan(generation, directory, max_entries);
    }

    pub(super) fn handle_session_event(&mut self, event: SessionEvent) {
        self.state.apply_event(event);
        self.app_event_tx.request_render();
    }

    pub(super) fn handle_paste(&mut self, pasted: String) {
        if self.state.mcp_panel_visible() || self.state.process_panel_visible() {
            self.app_event_tx.request_render();
            return;
        }
        if self.state.delegation_panel_visible() {
            self.app_event_tx.request_render();
            return;
        }
        if self.state.input_accepts_text() {
            self.state.record_user_focus_activity();
            // 粘贴一律按普通文本插入；附件入口只有 `@path` 与 Ctrl+V 剪贴板图片。
            self.state.push_pasted_text(&pasted);
            self.refresh_input_completion_and_render();
        }
    }

    #[cfg(test)]
    pub(super) fn handle_key_event(&mut self, key: KeyEvent) {
        self.handle_key_event_for_width(key, u16::MAX);
    }

    pub(super) fn handle_key_event_for_width(&mut self, key: KeyEvent, width: u16) {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return;
        }

        if self.state.process_panel_visible() {
            let action = self.state.handle_process_panel_key(key);
            if !matches!(action, ProcessPanelKeyAction::None) {
                self.app_event_tx.process_panel_action(action);
            }
            if self.state.process_panel_visible() {
                self.app_event_tx.request_render();
            } else {
                // 满屏 live panel 会把历史推到终端 viewport 上方；关闭时必须像 resize 一样
                // hard-clear 并重排完整历史，普通增量重绘只会替换底部 live region。
                self.app_event_tx.request_resize_render();
            }
            return;
        }

        // 管理面板在 active turn 中优先消费全部按键。`/ps` 确认页还会在 panel 内
        // 进一步实行白名单。
        if self.state.mcp_panel_visible() {
            if let McpPanelKeyAction::Request(request) = self.state.handle_mcp_panel_key(key) {
                self.app_event_tx.mcp_panel_request(request);
            }
            if self.state.mcp_panel_visible() {
                self.app_event_tx.request_render();
            } else {
                self.app_event_tx.request_resize_render();
            }
            return;
        }

        if self.state.delegation_panel_visible() {
            if is_ctrl_c(key) {
                if self.state.has_interruptible_task_in_flight() {
                    self.app_event_tx.interrupt();
                } else if !self.state.input().is_empty() {
                    self.state.clear_input();
                    self.app_event_tx.request_render();
                } else if ctrl_c_requests_exit(self.state.status) {
                    self.app_event_tx.request_exit();
                }
                return;
            }
            match key.code {
                KeyCode::Esc => {
                    self.state.close_delegation_panel();
                    self.app_event_tx.request_resize_render();
                    return;
                }
                KeyCode::Up => self.state.scroll_delegation_panel_up(1),
                KeyCode::Down => self.state.scroll_delegation_panel_down(1),
                KeyCode::PageUp => self.state.scroll_delegation_panel_up(10),
                KeyCode::PageDown => self.state.scroll_delegation_panel_down(10),
                KeyCode::Home => self.state.scroll_delegation_panel_home(),
                KeyCode::End => self.state.scroll_delegation_panel_end(),
                _ => {}
            }
            self.app_event_tx.request_render();
            return;
        }

        if self.state.input_accepts_text() {
            self.state.record_user_focus_activity();
        }

        if is_ctrl_c(key) {
            if !self.state.input().is_empty() {
                self.state.clear_input();
                self.app_event_tx.request_render();
            } else if self.state.has_interruptible_task_in_flight() {
                self.app_event_tx.interrupt();
            } else if ctrl_c_requests_exit(self.state.status) {
                self.app_event_tx.request_exit();
            }
            return;
        }

        if self.state.input_accepts_text() && is_ctrl_v(key) {
            // Ctrl+V：剪贴板图片挂成 [Image #N] 附件。读取 / 规格化是阻塞操作，
            // 放进 spawn_blocking，结果经 AppEvent::ClipboardImageRead 回灌。
            match self.state.begin_clipboard_image_read() {
                Ok(Some((limits, input_revision))) => {
                    self.state.mark_clipboard_image_read_started();
                    let interaction_generation = self.state.interaction_generation();
                    let tx = self.app_event_tx.clone();
                    tokio::spawn(async move {
                        let result = tokio::task::spawn_blocking(move || {
                            read_clipboard_image_blocking(&limits).map_err(|e| e.to_string())
                        })
                        .await
                        .unwrap_or_else(|e| Err(format!("剪贴板读取任务失败: {e}")));
                        tx.clipboard_image_read(interaction_generation, input_revision, result);
                    });
                }
                Ok(None) => {
                    let message = if self.state.attachments_enabled() {
                        "图片附件已禁用：启用请设置 agent.attachment.clipboard_image_enabled = true    agent.attachment.enabled = true。"
                    } else {
                        "附件功能已禁用：启用请设置 agent.attachment.enabled = true。"
                    };
                    self.state.push_system(message);
                    self.app_event_tx.request_render();
                }
                Err(error) => {
                    self.state
                        .push_error(format!("Clipboard attach failed: {error}"));
                    self.app_event_tx.request_render();
                }
            }
            return;
        }

        if self.state.input_accepts_text() && is_ctrl_o(key) {
            // Ctrl+O：预览附件（光标命中的一个，否则全部）。
            // 临时落盘与 `open` 拉起由 App 层处理。
            match self.state.preview_target_at_cursor() {
                PreviewHit::Targets(targets) => self
                    .app_event_tx
                    .preview_attachment(self.state.interaction_generation(), targets),
                PreviewHit::NoAttachments => {
                    self.state
                        .push_system("输入框里没有可预览的附件（@path 或 [Image #N]）");
                    self.app_event_tx.request_render();
                }
            }
            return;
        }

        match key.code {
            KeyCode::Enter if self.state.input_accepts_text() && is_ctrl_enter(key) => {
                self.submit_current_input(true);
            }
            KeyCode::Enter if self.state.input_accepts_text() && is_shift_enter_newline(key) => {
                self.state.push_input_newline();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Char(c)
                if self.state.input_accepts_text()
                    && !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                self.state.push_input_char(c);
                self.refresh_input_completion_and_render();
            }
            KeyCode::Backspace if self.state.input_accepts_text() => {
                self.state.pop_input_char();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Delete if self.state.input_accepts_text() => {
                self.state.delete_input_char();
                self.refresh_input_completion_and_render();
            }
            // 只有单独的 Option+←/→ 做词跳转（CSI 1;3D 风格，crossterm 解析为 Alt 修饰键）。
            // Ctrl、Shift 等叠加修饰键不复用该语义，保留各自方向键组合的扩展空间。
            KeyCode::Left
                if self.state.input_accepts_text() && key.modifiers == KeyModifiers::ALT =>
            {
                self.state.move_input_word_left();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Right
                if self.state.input_accepts_text() && key.modifiers == KeyModifiers::ALT =>
            {
                self.state.move_input_word_right();
                self.refresh_input_completion_and_render();
            }
            // Terminal.app / iTerm2 默认把 Option+←/→ 发成 ESC b / ESC f（readline 风格）。
            KeyCode::Char('b')
                if self.state.input_accepts_text() && key.modifiers == KeyModifiers::ALT =>
            {
                self.state.move_input_word_left();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Char('f')
                if self.state.input_accepts_text() && key.modifiers == KeyModifiers::ALT =>
            {
                self.state.move_input_word_right();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Left if self.state.input_accepts_text() => {
                self.state.move_input_left();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Right if self.state.input_accepts_text() => {
                self.state.move_input_right();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Home if self.state.input_accepts_text() => {
                self.state.move_input_home();
                self.refresh_input_completion_and_render();
            }
            KeyCode::End if self.state.input_accepts_text() => {
                self.state.move_input_end();
                self.refresh_input_completion_and_render();
            }
            KeyCode::Tab if self.state.input_accepts_text() && self.state.slash_menu_visible() => {
                if self.state.accept_slash_completion() {
                    self.refresh_input_completion_and_render();
                }
            }
            KeyCode::Tab
                if input_accepts_text(self.state.status) && self.state.at_path_menu_visible() =>
            {
                if self.state.accept_at_path_completion() {
                    self.refresh_input_completion_and_render();
                } else {
                    self.app_event_tx.request_render();
                }
            }
            // 行中 `空白 + /前缀` 唯一匹配时，Tab 接受浅色 ghost 补全。
            KeyCode::Tab if self.state.input_accepts_text() => {
                if self.state.accept_inline_slash_hint() {
                    self.refresh_input_completion_and_render();
                }
            }
            KeyCode::Enter if self.state.input_accepts_text() => {
                self.submit_current_input(false);
            }
            KeyCode::Up
                if self.state.input_accepts_text() && key.modifiers == KeyModifiers::NONE =>
            {
                if self.state.slash_menu_visible() {
                    if self.state.select_previous_slash_completion() {
                        self.app_event_tx.request_render();
                    }
                } else if self.state.at_path_menu_visible() {
                    if self.state.select_previous_at_path_completion() {
                        self.app_event_tx.request_render();
                    }
                } else if self.state.input_cursor_at_end() {
                    if self.state.recall_previous_input() {
                        self.refresh_input_completion_and_render();
                    }
                } else if self.state.move_input_up(width) {
                    self.refresh_input_completion_and_render();
                }
            }
            KeyCode::Down
                if self.state.input_accepts_text() && key.modifiers == KeyModifiers::NONE =>
            {
                if self.state.slash_menu_visible() {
                    if self.state.select_next_slash_completion() {
                        self.app_event_tx.request_render();
                    }
                } else if self.state.at_path_menu_visible() {
                    if self.state.select_next_at_path_completion() {
                        self.app_event_tx.request_render();
                    }
                } else if self.state.input_cursor_at_end() {
                    if self.state.recall_next_input() {
                        self.refresh_input_completion_and_render();
                    }
                } else if self.state.move_input_down(width) {
                    self.refresh_input_completion_and_render();
                }
            }
            KeyCode::Esc if self.state.at_path_menu_visible() => {
                if self.state.dismiss_at_path_menu() {
                    self.app_event_tx.request_render();
                }
            }
            KeyCode::Esc => {
                // queued input 的取回优先于当前 session task 的中断能力。compact、inbox
                // 等非 turn task 同样允许排队输入；Finalizing/Closed 的输入锁则必须连
                // Esc 取回一起禁用，避免把目标 session 队列移回旧页面甚至覆盖丢失。
                if self.state.input_accepts_text()
                    && self.state.restore_latest_queued_input_to_composer()
                {
                    self.app_event_tx.request_render();
                } else {
                    self.app_event_tx.interrupt();
                }
            }
            _ => {}
        }
    }

    fn refresh_input_completion_and_render(&mut self) {
        self.refresh_at_path_completion();
        self.app_event_tx.request_render();
    }

    fn submit_current_input(&mut self, steer: bool) {
        if self.state.slash_menu_visible() && self.state.accept_slash_completion() {
            self.refresh_input_completion_and_render();
            return;
        }
        if self.state.at_path_menu_visible() {
            if self.state.accept_at_path_completion() {
                self.refresh_input_completion_and_render();
            } else {
                self.app_event_tx.request_render();
            }
            return;
        }
        let draft = self.state.take_input_draft();
        let expanded_input = draft.expanded_text();
        if expanded_input.trim().is_empty() {
            return;
        }
        // `@path` 只在会发给模型的普通对话输入（classify 为 Send）里解析，
        // 与 slash 命令分类口径保持一致。
        let input_action = classify_input(draft.visible_text(), self.state.slash_catalog());
        let wants_at_paths = self.state.attachments_enabled()
            && matches!(input_action, InputAction::Send(_))
            && !scan_at_path_tokens(draft.visible_text()).is_empty();
        let has_inline_attachments = !draft.session_attachments().is_empty();
        let steer_running_turn = steer && self.state.has_turn_in_flight();
        let force_queue_for_attachments =
            steer_running_turn && (wants_at_paths || has_inline_attachments);
        let force_queue_for_slash_command = steer
            && self.state.slash_steer_notice_should_queue()
            && input_action_is_slash_command(&input_action)
            && !matches!(
                input_action,
                InputAction::Mcp | InputAction::Ps | InputAction::Subagents
            );
        if force_queue_for_attachments {
            self.state.set_status_notice(ATTACHMENT_STEER_QUEUE_NOTICE);
            self.state.push_system(ATTACHMENT_STEER_QUEUE_NOTICE);
            self.app_event_tx.request_render();
        } else if force_queue_for_slash_command {
            self.state
                .set_status_notice(SLASH_COMMAND_STEER_QUEUE_NOTICE);
            self.state.push_system(SLASH_COMMAND_STEER_QUEUE_NOTICE);
            self.app_event_tx.request_render();
        }
        let sequence = self.state.next_input_submission_sequence();
        if !wants_at_paths {
            let input = QueuedInput::new(expanded_input, draft);
            if steer_running_turn
                && !force_queue_for_attachments
                && !force_queue_for_slash_command
                && !matches!(
                    input_action,
                    InputAction::Mcp | InputAction::Ps | InputAction::Subagents
                )
            {
                self.app_event_tx.steer_input(sequence, input);
            } else {
                self.app_event_tx.submit_input(sequence, input);
            }
            return;
        }
        // @path 文件预检与目录一级列表读取都包含文件系统访问，放 spawn_blocking；
        // 解析成功后经 AppEvent::AtPathResolved 回灌再真正提交（失败则恢复草稿）。
        let limits = self.state.attachment_limits();
        let workspace_root = self.state.at_path_workspace_root().to_path_buf();
        let existing_attachments = draft.session_attachments().len();
        let visible_text = draft.visible_text().to_string();
        let tx = self.app_event_tx.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                resolve_at_paths(
                    &visible_text,
                    &workspace_root,
                    &limits,
                    existing_attachments,
                )
                .map_err(|e| e.to_string())
            })
            .await
            .unwrap_or_else(|e| Err(format!("附件解析任务失败: {e}")));
            tx.at_path_resolved(sequence, expanded_input, draft, result);
        });
    }

    pub(super) fn turn_animation_height_budget(
        &self,
        terminal_width: u16,
        terminal_height: u16,
    ) -> usize {
        turn_animation_height_budget_for_state(
            &self.state,
            terminal_width,
            terminal_height,
            self.live_response_preview_max_lines,
        )
    }

    pub(super) fn render_inline(&self, width: u16, height: u16) -> InlineRender {
        let render_width = terminal_render_width(width);
        let scrollback = self.state.scrollback_lines(render_width);
        let flush_start_separator = self.state.start_separator_pending();
        let live = self.live_lines(render_width, height, width);
        let mut scrollback_lines: Vec<Line<'static>> = Vec::new();
        if flush_start_separator {
            scrollback_lines.extend(
                startup_welcome_lines(render_width, &self.state)
                    .into_iter()
                    .map(apply_surface_style),
            );
        }
        if scrollback.starts_at_history_beginning && !scrollback.lines.is_empty() {
            scrollback_lines.push(apply_surface_style(Line::default()));
        }
        scrollback_lines.extend(scrollback.lines.into_iter().map(apply_surface_style));
        let live_lines: Vec<_> = live.lines.into_iter().map(apply_surface_style).collect();
        let cursor =
            (self.state.input_accepts_text() && live.cursor_base_row.is_some()).then(|| {
                let row = live.cursor_base_row.unwrap_or_default();
                let x = composer_cursor_x_for_width(&self.state, 0, render_width);
                let y = composer_cursor_y_for_width(
                    &self.state,
                    u16::try_from(row).unwrap_or(u16::MAX),
                    render_width,
                );
                (x, y)
            });
        InlineRender {
            scrollback_lines,
            flushed_entry_count: scrollback.entry_count,
            start_separator_flushed: flush_start_separator,
            live_lines,
            cursor,
        }
    }

    /// 判断欢迎卡片中的团队状态行是否仍位于当前终端可见区域。
    pub(super) fn welcome_team_status_is_visible(&self, width: u16, height: u16) -> bool {
        let render_width = terminal_render_width(width);
        let welcome = startup_welcome_lines(render_width, &self.state);
        let Some(status_row) = welcome
            .iter()
            .position(|line| line.to_string().contains("Maintainer "))
        else {
            return false;
        };
        let history_rows = self
            .state
            .history_render_lines_with_width(render_width)
            .len();
        let live_rows = self.live_lines(render_width, height, width).lines.len();
        let total_rows = welcome
            .len()
            .saturating_add(history_rows)
            .saturating_add(live_rows);
        let rows_scrolled_above_viewport = total_rows.saturating_sub(usize::from(height));
        status_row >= rows_scrolled_above_viewport
    }

    fn live_lines(&self, width: u16, height: u16, terminal_width: u16) -> LiveRender {
        if let Some(mut panel_lines) = self
            .state
            .process_panel_lines(width, height.saturating_sub(1).max(1))
        {
            panel_lines.push(status_line(&self.state, width));
            let lines = hard_wrap_styled_lines(panel_lines, usize::from(width.max(1)));
            return LiveRender {
                lines,
                cursor_base_row: None,
            };
        }
        if let Some(mut panel_lines) = self
            .state
            .mcp_panel_lines(width, height.saturating_sub(1).max(1))
        {
            panel_lines.push(status_line(&self.state, width));
            let lines = hard_wrap_styled_lines(panel_lines, usize::from(width.max(1)));
            return LiveRender {
                lines,
                cursor_base_row: None,
            };
        }

        if let Some(mut panel_lines) = self
            .state
            .delegation_panel_lines(width, height.saturating_sub(2))
        {
            if height >= 2 {
                panel_lines.push(delegation_panel_help_line(width));
            }
            panel_lines.push(status_line(&self.state, width));
            let lines = hard_wrap_styled_lines(panel_lines, usize::from(width.max(1)));
            return LiveRender {
                lines,
                cursor_base_row: None,
            };
        }

        let mut lines = Vec::new();
        let composer_lines = composer_lines_with_width(&self.state, width);
        let hint_index = composer_lines.len().saturating_sub(1);
        let has_active_user = self.state.has_active_user();
        let mut cursor_base_row = None;

        if !has_active_user {
            cursor_base_row = Some(lines.len().saturating_add(COMPOSER_TOP_SPACER_ROWS));
            lines.push(Line::default());
            lines.extend(composer_lines[..hint_index].iter().cloned());
        }

        let box_content_max_lines = live_box_content_max_lines_for_state(
            &self.state,
            terminal_width,
            height,
            self.live_response_preview_max_lines,
        );
        let animation_height_budget = turn_animation_height_budget_for_state(
            &self.state,
            terminal_width,
            height,
            self.live_response_preview_max_lines,
        );
        let animation_lines = self
            .state
            .running_turn_animation_lines(terminal_width, animation_height_budget);
        let has_animation_sidecar = !animation_lines.is_empty();
        let box_content_width = live_box_text_width(width, has_animation_sidecar);
        let status_notice_lines = status_notice_lines(&self.state, width);
        let network_status_lines = network_status_lines(&self.state, box_content_width);
        let pending_steer_lines = self.state.pending_tool_boundary_steer_lines(width);
        let has_pending_steer = !pending_steer_lines.is_empty();
        let composer_top_spacer_after_box = if has_active_user && has_pending_steer {
            COMPOSER_TOP_SPACER_ROWS
        } else {
            0
        };
        let mut activity_lines = self.state.active_timeline_lines(box_content_width);
        if self.state.status == SessionRuntimeStatus::SyncingInbox
            && activity_lines
                .first()
                .is_some_and(|line| line.to_string().trim().is_empty())
        {
            // `Inbox started` 已经留在 scrollback；它与 activity 之间的历史间隔
            // 不能落进 live box，框内第一行应直接显示当前同步活动。
            activity_lines.remove(0);
        }
        activity_lines.extend(network_status_lines);
        let show_live_box = has_animation_sidecar
            || !activity_lines.is_empty()
            || self.state.status == SessionRuntimeStatus::Compacting;
        if show_live_box {
            lines.extend(live_box_lines(
                &live_box_title(&self.state),
                &activity_lines,
                width,
                &animation_lines,
                box_content_max_lines,
            ));
        } else if let Some(idle) = idle_box_content(&self.state) {
            lines.extend(live_box_lines(
                idle.title,
                &idle.lines,
                width,
                &[],
                box_content_max_lines,
            ));
        }

        lines.extend(status_notice_lines);
        if !pending_steer_lines.is_empty() {
            lines.push(Line::default());
            lines.extend(pending_steer_lines);
        }
        if has_active_user {
            cursor_base_row = Some(lines.len().saturating_add(composer_top_spacer_after_box));
            if has_pending_steer {
                lines.push(Line::default());
            }
            lines.extend(composer_lines[..hint_index].iter().cloned());
        }
        lines.extend(composer_lines[hint_index..].iter().cloned());
        lines.push(status_line(&self.state, width));
        let lines = hard_wrap_styled_lines(lines, usize::from(width.max(1)));
        LiveRender {
            lines,
            cursor_base_row,
        }
    }
}

/// Config 校验已保证只有 `-1` 或正数；`-1` 在渲染层映射为无穷大上限。
fn resolve_live_response_preview_max_lines(configured_max_lines: i64) -> usize {
    usize::try_from(configured_max_lines).unwrap_or(AUTO_LIVE_RESPONSE_PREVIEW_MAX_LINES)
}

fn terminal_render_width(width: u16) -> u16 {
    width.saturating_sub(1).max(1)
}

fn turn_animation_height_budget_for_state(
    state: &SessionTuiState,
    terminal_width: u16,
    terminal_height: u16,
    live_response_preview_limit: usize,
) -> usize {
    let width = terminal_render_width(terminal_width);
    let content_height_budget = live_box_content_max_lines_for_state(
        state,
        terminal_width,
        terminal_height,
        live_response_preview_limit,
    );
    if terminal_width < TURN_ANIMATION_MIN_WIDTH
        || live_box_text_width_for_sidecar(width).is_none()
        || content_height_budget < TURN_ANIMATION_RENDER_ROWS
    {
        return 0;
    }
    TURN_ANIMATION_RENDER_ROWS
}

fn live_box_content_max_lines_for_state(
    state: &SessionTuiState,
    terminal_width: u16,
    terminal_height: u16,
    configured_max_lines: usize,
) -> usize {
    let width = terminal_render_width(terminal_width);
    let composer_lines = composer_lines_with_width(state, width);
    let hint_index = composer_lines.len().saturating_sub(1);
    let has_active_user = state.has_active_user();
    let pre_box_lines = if has_active_user {
        0
    } else {
        composer_lines[..hint_index]
            .len()
            .saturating_add(COMPOSER_TOP_SPACER_ROWS)
    };
    let pending_steer_lines = state.pending_tool_boundary_steer_lines(width);
    let has_pending_steer = !pending_steer_lines.is_empty();
    let composer_top_spacer_after_box = if has_active_user && has_pending_steer {
        COMPOSER_TOP_SPACER_ROWS
    } else {
        0
    };
    let composer_lines_after_box = if has_active_user {
        composer_lines
            .len()
            .saturating_add(composer_top_spacer_after_box)
    } else {
        composer_lines.len().saturating_sub(hint_index)
    };
    let lines_outside_box = pre_box_lines
        .saturating_add(composer_lines_after_box)
        .saturating_add(pending_tool_boundary_steer_rows(&pending_steer_lines))
        .saturating_add(status_notice_lines(state, width).len())
        .saturating_add(1);
    let viewport_max_lines = usize::from(terminal_height)
        .saturating_sub(lines_outside_box)
        .saturating_sub(2)
        .max(1);
    configured_max_lines.max(1).min(viewport_max_lines)
}

fn status_notice_lines(state: &SessionTuiState, width: u16) -> Vec<Line<'static>> {
    hard_wrap_styled_lines(
        state.background_status_lines(usize::from(width.max(1))),
        usize::from(width.max(1)),
    )
}

fn delegation_panel_help_line(width: u16) -> Line<'static> {
    Line::styled(
        truncate_label("↑/↓ to navigate  · Esc to back", usize::from(width.max(1))),
        muted_style(),
    )
}

fn input_action_is_slash_command(action: &InputAction) -> bool {
    matches!(
        action,
        InputAction::Compact
            | InputAction::Copy
            | InputAction::Exit
            | InputAction::Help
            | InputAction::Inbox
            | InputAction::Mcp
            | InputAction::Ps
            | InputAction::Resume
            | InputAction::Skills
            | InputAction::Subagents
            | InputAction::Unknown(_)
    )
}

fn pending_tool_boundary_steer_rows(lines: &[Line<'static>]) -> usize {
    if lines.is_empty() {
        0
    } else {
        lines.len().saturating_add(1)
    }
}

struct LiveRender {
    lines: Vec<Line<'static>>,
    cursor_base_row: Option<usize>,
}

pub(super) struct InlineRender {
    pub(super) scrollback_lines: Vec<Line<'static>>,
    pub(super) flushed_entry_count: usize,
    pub(super) start_separator_flushed: bool,
    pub(super) live_lines: Vec<Line<'static>>,
    pub(super) cursor: Option<(u16, u16)>,
}

fn is_ctrl_c(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
        && key.modifiers.contains(KeyModifiers::CONTROL)
}

fn is_ctrl_v(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('v') | KeyCode::Char('V'))
        && key.modifiers.contains(KeyModifiers::CONTROL)
}

fn is_ctrl_o(key: KeyEvent) -> bool {
    matches!(key.code, KeyCode::Char('o') | KeyCode::Char('O'))
        && key.modifiers.contains(KeyModifiers::CONTROL)
}

fn is_ctrl_enter(key: KeyEvent) -> bool {
    key.code == KeyCode::Enter && key.modifiers.contains(KeyModifiers::CONTROL)
}

fn ctrl_c_requests_exit(status: SessionRuntimeStatus) -> bool {
    matches!(
        status,
        SessionRuntimeStatus::Initializing
            | SessionRuntimeStatus::Open
            | SessionRuntimeStatus::Running
            | SessionRuntimeStatus::SyncingInbox
            | SessionRuntimeStatus::Compacting
            | SessionRuntimeStatus::Error
            | SessionRuntimeStatus::Finalizing
    )
}

fn status_line(state: &SessionTuiState, width: u16) -> Line<'static> {
    let limit = usize::from(width.max(1));
    let status_text = state.status_label();
    let status_style = status_style(state.status);
    if limit < 48 {
        return Line::from(vec![Span::styled(
            truncate_label(status_text, limit),
            status_style,
        )]);
    }

    let status_width = UnicodeWidthStr::width(status_text);
    let reserved_for_status = FOOTER_SEPARATOR
        .width()
        .saturating_add(status_width)
        .min(limit);
    let Some(model_budget) = limit.checked_sub(reserved_for_status) else {
        return Line::from(vec![Span::styled(
            truncate_label(status_text, limit),
            status_style,
        )]);
    };
    if model_budget < "model …".width() {
        return Line::from(vec![Span::styled(
            truncate_label(status_text, limit),
            status_style,
        )]);
    }

    let mut spans = Vec::new();
    let model_value_budget = model_budget.saturating_sub("model ".width()).min(18);
    let model_segment = footer_metadata_segment(
        "model ",
        truncate_label(
            state.model_name.as_deref().unwrap_or("--"),
            model_value_budget,
        ),
        Style::default().fg(FOOTER_MODEL_FG),
    );
    let model = model_segment.text.clone();
    push_footer_segment(&mut spans, model_segment.spans);

    let mut candidates = vec![
        FooterCandidate::metadata(
            1,
            3,
            "cwd ",
            truncate_label(state.workspace_label(), 18),
            Style::default().fg(FOOTER_CWD_FG),
        ),
        FooterCandidate::metadata(
            2,
            4,
            "branch ",
            truncate_label(state.branch_label(), 20),
            Style::default().fg(FOOTER_BRANCH_FG),
        ),
        FooterCandidate::metadata(
            3,
            1,
            "ctx ",
            truncate_label(&state.context_label(), 14),
            Style::default().fg(FOOTER_CTX_FG),
        ),
        FooterCandidate::metadata(
            4,
            2,
            "focus ",
            state.focus_label(),
            Style::default().fg(FOOTER_FOCUS_FG),
        ),
    ];

    if width >= FOOTER_SLOGAN_MIN_WIDTH {
        candidates.push(FooterCandidate::plain(
            5,
            5,
            FOOTER_SLOGAN.into(),
            muted_style(),
        ));
    }
    let selected = selected_footer_candidates(&model, candidates, status_text, limit);
    for candidate in selected {
        push_footer_segment(&mut spans, candidate.spans);
    }
    push_footer_segment(
        &mut spans,
        vec![Span::styled(status_text.to_string(), status_style)],
    );
    Line::from(spans)
}

#[derive(Debug, Clone)]
struct FooterCandidate {
    order: usize,
    priority: usize,
    text: String,
    spans: Vec<Span<'static>>,
}

impl FooterCandidate {
    fn plain(order: usize, priority: usize, text: String, style: Style) -> Self {
        let spans = vec![Span::styled(text.clone(), style)];
        Self {
            order,
            priority,
            text,
            spans,
        }
    }

    fn metadata(
        order: usize,
        priority: usize,
        label: &'static str,
        value: String,
        value_style: Style,
    ) -> Self {
        let segment = footer_metadata_segment(label, value, value_style);
        Self {
            order,
            priority,
            text: segment.text,
            spans: segment.spans,
        }
    }
}

struct FooterSegment {
    text: String,
    spans: Vec<Span<'static>>,
}

fn footer_metadata_segment(
    label: &'static str,
    value: String,
    value_style: Style,
) -> FooterSegment {
    let text = format!("{label}{value}");
    let spans = vec![
        Span::styled(label, Style::default().fg(FOOTER_LABEL_FG)),
        Span::styled(value, value_style),
    ];
    FooterSegment { text, spans }
}

fn selected_footer_candidates(
    model: &str,
    mut candidates: Vec<FooterCandidate>,
    status: &str,
    limit: usize,
) -> Vec<FooterCandidate> {
    candidates.sort_by_key(|candidate| candidate.priority);
    let mut selected = Vec::new();
    for candidate in candidates {
        let mut trial = selected.clone();
        trial.push(candidate.clone());
        trial.sort_by_key(|candidate| candidate.order);
        if footer_width_with_candidates(model, &trial, status) <= limit {
            selected = trial;
        }
    }
    selected.sort_by_key(|candidate| candidate.order);
    selected
}

fn footer_width_with_candidates(
    model: &str,
    candidates: &[FooterCandidate],
    status: &str,
) -> usize {
    let mut width = model.width();
    for candidate in candidates {
        width = width
            .saturating_add(FOOTER_SEPARATOR.width())
            .saturating_add(candidate.text.width());
    }
    width
        .saturating_add(FOOTER_SEPARATOR.width())
        .saturating_add(status.width())
}

fn push_footer_segment(spans: &mut Vec<Span<'static>>, segment: Vec<Span<'static>>) {
    if !spans.is_empty() {
        spans.push(Span::raw(FOOTER_SEPARATOR));
    }
    spans.extend(segment);
}

/// 按**显示列宽**（而非字符数）截断，单位与 footer 的所有宽度预算一致。
/// CJK/emoji 等全宽字符占 2 列，按字符数截断会击穿 footer 宽度保证导致溢出。
fn truncate_label(value: &str, max_cols: usize) -> String {
    if UnicodeWidthStr::width(value) <= max_cols {
        return value.to_string();
    }
    if max_cols <= 1 {
        return "…".into();
    }
    let target = max_cols.saturating_sub(1); // 为省略号预留 1 列
    let mut out = String::new();
    let mut width = 0usize;
    for ch in value.chars() {
        let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
        if width.saturating_add(ch_width) > target {
            break;
        }
        out.push(ch);
        width = width.saturating_add(ch_width);
    }
    out.push('…');
    out
}

pub(super) fn composer_lines_with_width(state: &SessionTuiState, width: u16) -> Vec<Line<'static>> {
    let pending_preview = state.pending_input_preview();
    state.bottom_pane().lines_with_width(
        state.status,
        state.running_task_label(),
        &pending_preview,
        state.queued_count_for_render(),
        state.session_id.as_deref(),
        width,
    )
}

#[cfg(test)]
pub(super) fn composer_hint(state: &SessionTuiState) -> String {
    state.bottom_pane().hint(
        state.status,
        state.running_task_label(),
        state.queued_count_for_render(),
        state.session_id.as_deref(),
    )
}

fn live_box_title(state: &SessionTuiState) -> String {
    match state.status {
        SessionRuntimeStatus::Initializing => format!(
            "Initializing · Syncing inbox · {}s",
            state.foreground_task_elapsed_secs()
        ),
        SessionRuntimeStatus::Running if state.shell_in_flight() => format!(
            "Shell · Running command · {}s",
            state.foreground_task_elapsed_secs()
        ),
        SessionRuntimeStatus::Running => format!(
            "Working · Streaming response · {}s",
            state.foreground_task_elapsed_secs()
        ),
        SessionRuntimeStatus::SyncingInbox => format!(
            "Inbox · Syncing updates · {}s",
            state.foreground_task_elapsed_secs()
        ),
        SessionRuntimeStatus::Compacting => format!(
            "Compacting · Session history · {}s",
            state.foreground_task_elapsed_secs()
        ),
        SessionRuntimeStatus::Finalizing => format!(
            "Finalizing · Committing contribution · {}s",
            state.foreground_task_elapsed_secs()
        ),
        SessionRuntimeStatus::Open => "Idle".into(),
        SessionRuntimeStatus::Error => "Attention · Last turn failed".into(),
        SessionRuntimeStatus::Closed => "Session closed".into(),
    }
}

struct IdleBoxContent {
    title: &'static str,
    lines: Vec<Line<'static>>,
}

fn idle_box_content(state: &SessionTuiState) -> Option<IdleBoxContent> {
    match state.status {
        SessionRuntimeStatus::Open => Some(IdleBoxContent {
            title: "ready · claim network",
            lines: {
                let mut lines = vec![Line::from(vec![
                    Span::styled("agent ", Style::default().fg(Color::DarkGray)),
                    Span::raw(state.agent_id.as_deref().unwrap_or("--").to_string()),
                    Span::styled(" · session ", Style::default().fg(Color::DarkGray)),
                    Span::raw(short_session_id(state.session_id.as_deref())),
                    Span::styled(" · turns ", Style::default().fg(Color::DarkGray)),
                    Span::raw(state.turn_count.to_string()),
                ])];
                lines.extend(network_status_lines(state, u16::MAX));
                lines
            },
        }),
        SessionRuntimeStatus::Error => Some(IdleBoxContent {
            title: "Attention · Last turn failed",
            lines: vec![Line::styled(
                "Edit the prompt, retry, or /exit to finalize",
                Style::default().fg(Color::DarkGray),
            )],
        }),
        SessionRuntimeStatus::Initializing
        | SessionRuntimeStatus::Running
        | SessionRuntimeStatus::SyncingInbox
        | SessionRuntimeStatus::Compacting
        | SessionRuntimeStatus::Finalizing
        | SessionRuntimeStatus::Closed => None,
    }
}

fn network_status_lines(state: &SessionTuiState, width: u16) -> Vec<Line<'static>> {
    let snapshot = state.network_snapshot();
    let has_network_status = snapshot.local_claims_total.is_some()
        || snapshot.last_router_lookup.is_some()
        || snapshot.last_contribution.is_some();
    if !has_network_status {
        return Vec::new();
    }

    let mut lines = Vec::new();
    if let Some(total) = snapshot.local_claims_total {
        let label = if width < 48 {
            "local "
        } else {
            "local claims "
        };
        lines.push(Line::from(vec![
            Span::styled(label, Style::default().fg(Color::DarkGray)),
            Span::raw(total.to_string()),
        ]));
    }

    if let Some(router) = &snapshot.last_router_lookup {
        let prefix = if width < 48 {
            "router "
        } else {
            "last router consult "
        };
        lines.push(Line::from(vec![
            Span::styled(prefix, Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("claims {}", router.candidate_claims),
                Style::default().fg(Color::Green),
            ),
            Span::styled(" · disputes ", Style::default().fg(Color::DarkGray)),
            Span::raw(router.disputes.to_string()),
        ]));
    }

    if let Some(contribution) = &snapshot.last_contribution {
        lines.push(match contribution.kind {
            ContributionKind::Inbox => {
                let processed = contribution
                    .processed
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "--".into());
                Line::from(vec![
                    Span::styled("inbox ", Style::default().fg(Color::DarkGray)),
                    Span::raw(processed),
                    Span::styled(" · claims +", Style::default().fg(Color::DarkGray)),
                    Span::raw(contribution.new_claims.to_string()),
                    Span::styled(" / ~", Style::default().fg(Color::DarkGray)),
                    Span::raw(contribution.updated_claims.to_string()),
                    Span::styled(" / -", Style::default().fg(Color::DarkGray)),
                    Span::raw(contribution.deprecated_claims.to_string()),
                    Span::styled(" · disputes +", Style::default().fg(Color::DarkGray)),
                    Span::raw(contribution.new_disputes.to_string()),
                ])
            }
            ContributionKind::Finalize => {
                let label = "finalize";
                Line::from(vec![
                    Span::styled(
                        format!("{label} · claims +"),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::raw(contribution.new_claims.to_string()),
                    Span::styled(" / ~", Style::default().fg(Color::DarkGray)),
                    Span::raw(contribution.updated_claims.to_string()),
                    Span::styled(" / -", Style::default().fg(Color::DarkGray)),
                    Span::raw(contribution.deprecated_claims.to_string()),
                    Span::styled(" · disputes +", Style::default().fg(Color::DarkGray)),
                    Span::raw(contribution.new_disputes.to_string()),
                ])
            }
        });
    }

    lines
}

fn short_session_id(session_id: Option<&str>) -> String {
    let Some(session_id) = session_id else {
        return "--".into();
    };
    session_id
        .rsplit_once('_')
        .map(|(_, suffix)| suffix)
        .unwrap_or(session_id)
        .to_string()
}

fn tail_preview_lines(mut lines: Vec<Line<'static>>, max_lines: usize) -> Vec<Line<'static>> {
    if lines.len() <= max_lines {
        return lines;
    }
    if max_lines <= 1 {
        let keep_from = lines.len().saturating_sub(1);
        return lines.drain(keep_from..).collect();
    }
    let kept_tail_lines = max_lines.saturating_sub(1);
    let keep_from = lines.len().saturating_sub(kept_tail_lines);
    let mut preview = vec![Line::styled("  ...", muted_style())];
    preview.extend(lines.drain(keep_from..));
    preview
}

fn live_box_lines(
    title: &str,
    content: &[Line<'static>],
    width: u16,
    sidecar: &[Line<'static>],
    max_content_lines: usize,
) -> Vec<Line<'static>> {
    let width = usize::from(width.max(1));
    if width < 4 {
        return content.to_vec();
    }
    let inner_width = width.saturating_sub(4);
    let has_sidecar = !sidecar.is_empty()
        && inner_width
            >= TURN_ANIMATION_SIDECAR_WIDTH.saturating_add(TURN_ANIMATION_MIN_TEXT_WIDTH);
    let text_width = if has_sidecar {
        inner_width.saturating_sub(TURN_ANIMATION_SIDECAR_WIDTH)
    } else {
        inner_width
    };
    let bottom = format!("└{}┘", "╌".repeat(inner_width.saturating_add(2)));
    let mut lines = vec![live_box_top_border(title, inner_width)];
    let mut wrapped_content = Vec::new();
    wrapped_content.extend(content.iter().cloned());
    let mut wrapped_lines = hard_wrap_styled_lines(wrapped_content, text_width);
    if has_sidecar && wrapped_lines.len() < TURN_ANIMATION_RENDER_ROWS {
        wrapped_lines.resize_with(TURN_ANIMATION_RENDER_ROWS, Line::default);
    }
    // 配置限制的是最终框内视觉行：所有内容与空行先完成换行/动画补齐，再统一逐行取尾部。
    let wrapped_lines = tail_preview_lines(wrapped_lines, max_content_lines);
    for (index, line) in wrapped_lines.into_iter().enumerate() {
        let sidecar_line = has_sidecar.then(|| sidecar.get(index)).flatten();
        lines.push(boxed_text_line(
            line.spans.clone(),
            text_width,
            has_sidecar,
            sidecar_line,
        ));
    }
    lines.push(Line::styled(bottom, muted_style()));
    lines
}

fn live_box_top_border(title: &str, inner_width: usize) -> Line<'static> {
    let border_width = inner_width.saturating_add(2);
    let title_text = format!(" {title} ");
    let title_width = title_text.width();
    if title_width >= border_width {
        return Line::styled(format!("┌{}┐", "╌".repeat(border_width)), muted_style());
    }
    Line::from(vec![
        Span::styled("┌ ", muted_style()),
        Span::styled(
            title.to_string(),
            Style::default()
                .fg(BLUE_FG)
                .bg(SURFACE_BG)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" {}", "╌".repeat(border_width.saturating_sub(title_width))),
            muted_style(),
        ),
        Span::styled("┐", muted_style()),
    ])
}

fn boxed_text_line(
    spans: Vec<Span<'static>>,
    text_width: usize,
    has_sidecar: bool,
    sidecar: Option<&Line<'static>>,
) -> Line<'static> {
    let mut line = Line::from(vec![Span::styled(LIVE_BOX_LEFT_BORDER, muted_style())]);
    for span in spans {
        line.push_span(span);
    }
    let used_width = line.width().saturating_sub(2);
    if used_width < text_width {
        line.push_span(Span::raw(" ".repeat(text_width - used_width)));
    }
    if has_sidecar {
        push_sidecar(&mut line, sidecar);
    }
    line.push_span(Span::styled(LIVE_BOX_RIGHT_BORDER, muted_style()));
    line
}

fn push_sidecar(line: &mut Line<'static>, sidecar: Option<&Line<'static>>) {
    line.push_span(Span::styled(" ", Style::default().bg(SURFACE_BG)));
    let mut used_width = 0usize;
    if let Some(sidecar) = sidecar {
        for span in sidecar.spans.iter().cloned() {
            used_width = used_width.saturating_add(span.width());
            line.push_span(span);
        }
    }
    if used_width < BOARD_WIDTH {
        line.push_span(Span::styled(
            " ".repeat(BOARD_WIDTH - used_width),
            Style::default().bg(SURFACE_BG),
        ));
    }
    line.push_span(Span::styled(" ", Style::default().bg(SURFACE_BG)));
}

fn live_box_text_width(width: u16, has_sidecar: bool) -> u16 {
    let inner_width = usize::from(width.max(1)).saturating_sub(4);
    let text_width = if has_sidecar {
        live_box_text_width_for_sidecar(width).unwrap_or(inner_width)
    } else {
        inner_width
    };
    u16::try_from(text_width.max(1)).unwrap_or(u16::MAX)
}

fn live_box_text_width_for_sidecar(width: u16) -> Option<usize> {
    let inner_width = usize::from(width.max(1)).saturating_sub(4);
    if inner_width < TURN_ANIMATION_SIDECAR_WIDTH.saturating_add(TURN_ANIMATION_MIN_TEXT_WIDTH) {
        return None;
    }
    Some(inner_width.saturating_sub(TURN_ANIMATION_SIDECAR_WIDTH))
}

fn status_style(status: SessionRuntimeStatus) -> Style {
    match status {
        SessionRuntimeStatus::Initializing => Style::default().fg(Color::Blue),
        SessionRuntimeStatus::Open => Style::default().fg(Color::Green),
        SessionRuntimeStatus::Running => Style::default().fg(Color::Yellow),
        SessionRuntimeStatus::SyncingInbox => Style::default().fg(Color::Cyan),
        SessionRuntimeStatus::Compacting => Style::default().fg(Color::Yellow),
        SessionRuntimeStatus::Finalizing => Style::default().fg(Color::Magenta),
        SessionRuntimeStatus::Error => Style::default().fg(Color::Red),
        SessionRuntimeStatus::Closed => Style::default().fg(Color::DarkGray),
    }
}

#[cfg(test)]
pub(super) fn inline_cursor_for_width(state: &SessionTuiState, width: u16) -> Option<(u16, u16)> {
    ChatWidget {
        state: state.clone(),
        app_event_tx: AppEventSender::channel().0,
        live_response_preview_max_lines: resolve_live_response_preview_max_lines(
            DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES,
        ),
    }
    .render_inline(width, u16::MAX)
    .cursor
}

#[cfg(test)]
pub(super) fn inline_start_separator_lines_with_width(width: u16) -> Vec<Line<'static>> {
    startup_welcome_lines(width, &SessionTuiState::new())
}

#[cfg(test)]
pub(super) fn history_render_lines_with_width(
    state: &SessionTuiState,
    width: u16,
) -> Vec<Line<'static>> {
    state.history_render_lines_with_width(width)
}

#[cfg(test)]
pub(super) fn inline_live_lines_with_width(
    state: &SessionTuiState,
    width: u16,
) -> Vec<Line<'static>> {
    ChatWidget {
        state: state.clone(),
        app_event_tx: AppEventSender::channel().0,
        live_response_preview_max_lines: resolve_live_response_preview_max_lines(
            DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES,
        ),
    }
    .render_inline(width, u16::MAX)
    .live_lines
}

#[cfg(test)]
pub(super) fn inline_scrollback_lines_with_width(
    state: &SessionTuiState,
    width: u16,
) -> Vec<Line<'static>> {
    ChatWidget {
        state: state.clone(),
        app_event_tx: AppEventSender::channel().0,
        live_response_preview_max_lines: resolve_live_response_preview_max_lines(
            DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES,
        ),
    }
    .render_inline(width, u16::MAX)
    .scrollback_lines
}

#[cfg(test)]
pub(super) fn inline_live_lines_with_size(
    state: &SessionTuiState,
    width: u16,
    height: u16,
) -> Vec<Line<'static>> {
    inline_live_lines_with_size_and_preview_max(
        state,
        width,
        height,
        DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES,
    )
}

#[cfg(test)]
pub(super) fn inline_live_lines_with_size_and_preview_max(
    state: &SessionTuiState,
    width: u16,
    height: u16,
    live_response_preview_max_lines: i64,
) -> Vec<Line<'static>> {
    ChatWidget {
        state: state.clone(),
        app_event_tx: AppEventSender::channel().0,
        live_response_preview_max_lines: resolve_live_response_preview_max_lines(
            live_response_preview_max_lines,
        ),
    }
    .render_inline(width, height)
    .live_lines
}

#[cfg(test)]
pub(super) fn turn_animation_height_budget_for_test(
    state: &SessionTuiState,
    width: u16,
    height: u16,
) -> usize {
    turn_animation_height_budget_for_state(
        state,
        width,
        height,
        resolve_live_response_preview_max_lines(DEFAULT_LIVE_RESPONSE_PREVIEW_MAX_LINES),
    )
}

#[cfg(test)]
pub(super) fn composer_cursor_x(state: &SessionTuiState, area_x: u16) -> u16 {
    state.bottom_pane().cursor_x(area_x)
}

pub(super) fn composer_cursor_x_for_width(state: &SessionTuiState, area_x: u16, width: u16) -> u16 {
    state.bottom_pane().cursor_x_for_width(area_x, width)
}

#[cfg(test)]
pub(super) fn composer_cursor_y(state: &SessionTuiState, area_y: u16) -> u16 {
    state.bottom_pane().cursor_y(area_y)
}

pub(super) fn composer_cursor_y_for_width(state: &SessionTuiState, area_y: u16, width: u16) -> u16 {
    state.bottom_pane().cursor_y_for_width(area_y, width)
}

#[cfg(test)]
pub(super) fn composer_height(state: &SessionTuiState) -> u16 {
    let pending_preview = state.pending_input_preview();
    state.bottom_pane().height(&pending_preview)
}

#[cfg(test)]
pub(super) fn composer_height_for_width(state: &SessionTuiState, width: u16) -> u16 {
    let pending_preview = state.pending_input_preview();
    state
        .bottom_pane()
        .height_for_width(&pending_preview, width)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;
    use crate::agent::SessionEvent;
    use crate::api::SessionAttachment;
    use crate::session_tui::app_event::{AppEvent, AppEventSender};
    use crate::session_tui::input_queue::QueuedInput;

    fn ctrl_c() -> KeyEvent {
        KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)
    }

    fn ctrl_enter() -> KeyEvent {
        KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL)
    }

    fn esc() -> KeyEvent {
        KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)
    }

    /// 模拟 app 按 submission sequence 接纳输入后才写入历史。
    fn record_accepted_submission(chat: &mut ChatWidget, event: &AppEvent) {
        let AppEvent::SubmitInput { input, .. } = event else {
            panic!("expected submitted input");
        };
        chat.state_mut()
            .record_submitted_draft(input.draft().clone());
    }

    #[test]
    fn at_path_menu_keys_select_complete_and_escape_without_submitting() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().set_at_path_completion_config(
            std::path::PathBuf::from("/workspace"),
            super::super::at_path_completion::AtPathCompletionLimits::default(),
        );
        chat.state_mut().push_input_text("@");
        let (generation, directory, _) = chat.state_mut().begin_at_path_scan().unwrap();
        assert!(chat.state_mut().apply_at_path_directory_read(
            generation,
            directory,
            Ok(vec![
                super::super::at_path_completion::AtPathDirectoryEntry {
                    file_name: std::ffi::OsString::from("a.txt"),
                    kind: super::super::at_path_completion::AtPathCandidateKind::File,
                    protected: false,
                },
                super::super::at_path_completion::AtPathDirectoryEntry {
                    file_name: std::ffi::OsString::from("b.txt"),
                    kind: super::super::at_path_completion::AtPathCandidateKind::File,
                    protected: false,
                },
            ]),
        ));

        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        chat.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "@b.txt");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());

        chat.state_mut().clear_input();
        chat.state_mut().push_input_text("@");
        let (generation, directory, _) = chat.state_mut().begin_at_path_scan().unwrap();
        assert!(chat.state_mut().apply_at_path_directory_read(
            generation,
            directory,
            Ok(vec![
                super::super::at_path_completion::AtPathDirectoryEntry {
                    file_name: std::ffi::OsString::from("a.txt"),
                    kind: super::super::at_path_completion::AtPathCandidateKind::File,
                    protected: false,
                }
            ]),
        ));
        chat.handle_key_event(esc());
        assert_eq!(chat.state().input(), "@");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_c_clears_non_empty_composer_before_exit_or_interrupt() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("draft");

        chat.handle_key_event(ctrl_c());

        assert_eq!(chat.state().input(), "");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn finalize_failure_disables_all_composer_input_but_keeps_ctrl_c_exit() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().apply_event(SessionEvent::FinalizeFailed {
            error: "provider timeout".into(),
        });
        chat.state_mut().apply_event(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Error,
        });

        chat.handle_key_event(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        chat.handle_paste("hidden paste".into());

        assert_eq!(chat.state().input(), "");
        assert_eq!(composer_hint(chat.state()), "Finalize failed · Ctrl+C quit");
        assert_eq!(chat.render_inline(96, 36).cursor, None);
        assert!(rx.try_recv().is_err());

        chat.handle_key_event(ctrl_c());
        assert_eq!(rx.try_recv().unwrap(), AppEvent::ExitRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn delegation_panel_blocks_text_and_paste_input() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().open_delegation_panel();

        chat.handle_key_event(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);

        chat.handle_paste("hidden paste".into());
        assert_eq!(chat.state().input(), "");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
    }

    #[test]
    fn esc_closing_delegation_panel_requests_history_reflow() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        chat.state_mut().open_delegation_panel();
        assert!(chat.state().delegation_panel_visible());

        chat.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));

        assert!(!chat.state().delegation_panel_visible());
        assert_eq!(rx.try_recv().unwrap(), AppEvent::ResizeRenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn esc_closing_process_panel_requests_history_reflow() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        chat.state_mut().open_process_panel();
        assert!(chat.state().process_panel_visible());

        chat.handle_key_event(esc());

        assert!(!chat.state().process_panel_visible());
        assert_eq!(rx.try_recv().unwrap(), AppEvent::ResizeRenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn esc_closing_mcp_panel_requests_history_reflow() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        chat.state_mut().open_mcp_panel();
        assert!(chat.state().mcp_panel_visible());

        chat.handle_key_event(esc());

        assert!(!chat.state().mcp_panel_visible());
        assert_eq!(rx.try_recv().unwrap(), AppEvent::ResizeRenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn management_panels_fill_live_height_over_existing_history() {
        let (mcp_sender, _) = AppEventSender::channel();
        let mut mcp_chat = ChatWidget::new(mcp_sender);
        mcp_chat.state_mut().push_help();
        let mcp_scrollback = mcp_chat.state().scrollback_lines(96);
        mcp_chat
            .state_mut()
            .mark_scrollback_flushed(mcp_scrollback.entry_count);
        mcp_chat.state_mut().mark_start_separator_flushed();
        mcp_chat.state_mut().open_mcp_panel();
        let mcp_render = mcp_chat.render_inline(96, 40);
        assert!(mcp_render.scrollback_lines.is_empty());
        assert_eq!(mcp_render.live_lines.len(), 40);
        assert!(mcp_render.live_lines[0]
            .to_string()
            .contains("MCP · servers"));
        assert!(mcp_render.live_lines[38]
            .to_string()
            .contains("Esc to close"));

        let (process_sender, _) = AppEventSender::channel();
        let mut process_chat = ChatWidget::new(process_sender);
        process_chat.state_mut().push_help();
        let process_scrollback = process_chat.state().scrollback_lines(96);
        process_chat
            .state_mut()
            .mark_scrollback_flushed(process_scrollback.entry_count);
        process_chat.state_mut().mark_start_separator_flushed();
        process_chat.state_mut().open_process_panel();
        let process_render = process_chat.render_inline(96, 40);
        assert!(process_render.scrollback_lines.is_empty());
        assert_eq!(process_render.live_lines.len(), 40);
        assert_eq!(
            process_render.live_lines[0].to_string(),
            "Processes · live processes"
        );
        assert_eq!(
            process_render.live_lines[38].to_string(),
            "↑/↓ select · t terminate · Esc close"
        );

        let (active_sender, _) = AppEventSender::channel();
        let mut active_chat = ChatWidget::new(active_sender);
        active_chat
            .state_mut()
            .begin_pending_turn("active prompt".into());
        let active_scrollback = active_chat.state().scrollback_lines(96);
        assert_eq!(
            active_scrollback.lines.last().map(|line| line.to_string()),
            Some("".into())
        );
        active_chat
            .state_mut()
            .mark_scrollback_flushed(active_scrollback.entry_count);
        active_chat.state_mut().mark_start_separator_flushed();
        active_chat.state_mut().open_mcp_panel();
        let active_render = active_chat.render_inline(96, 40);
        assert!(active_render.scrollback_lines.is_empty());
        assert_eq!(active_render.live_lines.len(), 40);
        assert!(active_render.live_lines[0]
            .to_string()
            .contains("MCP · servers"));
    }

    #[test]
    fn team_status_visibility_tracks_scrollback_position() {
        let (sender, _) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        assert!(chat.welcome_team_status_is_visible(96, 24));

        for index in 0..40 {
            chat.state_mut().push_system(format!("history row {index}"));
        }
        assert!(!chat.welcome_team_status_is_visible(96, 24));
    }

    #[test]
    fn ctrl_c_interrupts_parent_turn_when_delegation_panel_is_open() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("hello".into());
        chat.state_mut().open_delegation_panel();
        assert!(chat.state().delegation_panel_visible());

        chat.handle_key_event(ctrl_c());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_c_interrupts_parent_turn_before_clearing_hidden_draft_in_delegation_panel() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("hello".into());
        chat.state_mut().push_input_text("hidden draft");
        chat.state_mut().open_delegation_panel();

        chat.handle_key_event(ctrl_c());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert_eq!(chat.state().input(), "hidden draft");
    }

    #[test]
    fn ctrl_c_interrupts_running_turn_when_composer_is_empty() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("hello".into());

        chat.handle_key_event(ctrl_c());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_enter_submits_text_as_steer_input() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut().push_input_text("steer this way");

        chat.handle_key_event(ctrl_enter());

        let AppEvent::SteerInput {
            input: submitted, ..
        } = rx.try_recv().unwrap()
        else {
            panic!("Expected steer input");
        };
        assert_eq!(submitted.text(), "steer this way");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_enter_with_slash_command_warns_and_submits_for_queue() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut().push_input_text("/help");

        chat.handle_key_event(ctrl_enter());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(chat
            .state()
            .transcript_text()
            .contains(SLASH_COMMAND_STEER_QUEUE_NOTICE));
        assert_eq!(
            chat.state()
                .status_notice_line()
                .map(|line| line.to_string())
                .as_deref(),
            Some(SLASH_COMMAND_STEER_QUEUE_NOTICE)
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            AppEvent::SubmitInput {
                sequence: 0,
                input: QueuedInput::from_text("/help"),
            }
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_enter_with_mcp_slash_command_opens_immediately_during_active_turn() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut().push_input_text("/mcp");

        chat.handle_key_event(ctrl_enter());

        assert_eq!(
            rx.try_recv().unwrap(),
            AppEvent::SubmitInput {
                sequence: 0,
                input: QueuedInput::from_text("/mcp"),
            }
        );
        assert!(chat.state().status_notice_line().is_none());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_enter_with_slash_command_warns_after_turn_committed_before_worker_finish() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut()
            .apply_event(SessionEvent::TurnCommitted { message_count: 2 });
        chat.state_mut().push_input_text("/help");

        chat.handle_key_event(ctrl_enter());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert_eq!(
            chat.state()
                .status_notice_line()
                .map(|line| line.to_string())
                .as_deref(),
            Some(SLASH_COMMAND_STEER_QUEUE_NOTICE)
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            AppEvent::SubmitInput {
                sequence: 0,
                input: QueuedInput::from_text("/help"),
            }
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_enter_with_slash_text_prompt_still_steers() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut().push_input_text("/help me");

        chat.handle_key_event(ctrl_enter());

        let AppEvent::SteerInput {
            input: submitted, ..
        } = rx.try_recv().unwrap()
        else {
            panic!("Expected steer input");
        };
        assert_eq!(submitted.text(), "/help me");
        assert!(chat.state().status_notice_line().is_none());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn ctrl_enter_with_at_path_warns_and_uses_normal_attachment_submission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notes.md");
        tokio::fs::write(&path, "attachment body").await.unwrap();
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut()
            .push_input_text(&format!("read @{}", path.display()));

        chat.handle_key_event(ctrl_enter());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(chat
            .state()
            .transcript_text()
            .contains("附件输入已排队，不能打断注入当前 turn。"));
        assert_eq!(
            chat.state()
                .status_notice_line()
                .map(|line| line.to_string())
                .as_deref(),
            Some("附件输入已排队，不能打断注入当前 turn。")
        );
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let AppEvent::AtPathResolved { result, .. } = event else {
            panic!("Expected at-path resolution");
        };
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn ctrl_enter_with_at_path_while_idle_submits_without_interrupt_warning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notes.md");
        tokio::fs::write(&path, "attachment body").await.unwrap();
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut()
            .push_input_text(&format!("read @{}", path.display()));

        chat.handle_key_event(ctrl_enter());

        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let AppEvent::AtPathResolved { result, .. } = event else {
            panic!("Expected at-path resolution");
        };
        assert!(result.is_ok());
        assert!(!chat
            .state()
            .transcript_text()
            .contains("附件输入已排队，不能打断注入当前 turn。"));
    }

    #[test]
    fn ctrl_c_clears_running_draft_before_interrupting_turn() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("hello".into());
        chat.state_mut().queue_pending_turn("queued");
        chat.state_mut().push_input_text("draft");

        chat.handle_key_event(ctrl_c());

        assert_eq!(chat.state().input(), "");
        assert_eq!(chat.state().queued_count(), 1);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());

        chat.handle_key_event(ctrl_c());

        assert_eq!(chat.state().input(), "");
        assert_eq!(chat.state().queued_count(), 1);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn esc_restores_latest_queued_input_before_interrupting_running_turn() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut().queue_pending_turn("first queued");
        chat.state_mut().queue_pending_turn("second queued");
        chat.state_mut()
            .push_input_text("draft that will be overwritten");

        chat.handle_key_event(esc());

        assert_eq!(chat.state().input(), "second queued");
        assert_eq!(chat.state().queued_count(), 1);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());

        chat.handle_key_event(esc());

        assert_eq!(chat.state().input(), "first queued");
        assert_eq!(chat.state().queued_count(), 0);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());

        chat.handle_key_event(esc());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn esc_restores_latest_queued_input_before_interrupting_compaction() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().status = SessionRuntimeStatus::Compacting;
        chat.state_mut().queue_pending_turn("first queued");
        chat.state_mut().queue_pending_turn("second queued");

        chat.handle_key_event(esc());

        assert_eq!(chat.state().input(), "second queued");
        assert_eq!(chat.state().queued_count(), 1);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());

        chat.handle_key_event(esc());

        assert_eq!(chat.state().input(), "first queued");
        assert_eq!(chat.state().queued_count(), 0);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());

        chat.handle_key_event(esc());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn esc_during_finalizing_keeps_target_queue_untouched() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().status = SessionRuntimeStatus::Finalizing;
        chat.state_mut().queue_pending_turn("first target input");
        chat.state_mut().queue_pending_turn("second target input");

        chat.handle_key_event(esc());
        chat.handle_key_event(esc());

        assert_eq!(chat.state().input(), "");
        assert_eq!(chat.state().queued_count(), 2);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert_eq!(rx.try_recv().unwrap(), AppEvent::InterruptRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_c_requests_exit_when_idle_and_composer_is_empty() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        chat.handle_key_event(ctrl_c());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::ExitRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_c_requests_exit_while_finalize_is_already_running() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.handle_session_event(SessionEvent::StatusChanged {
            status: SessionRuntimeStatus::Finalizing,
        });
        let _ = rx.try_recv();

        chat.handle_key_event(ctrl_c());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::ExitRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn up_and_down_recall_submitted_input_history() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("first prompt");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let first_submission = rx.try_recv().unwrap();
        assert_eq!(
            first_submission,
            AppEvent::SubmitInput {
                sequence: 0,
                input: QueuedInput::from_text("first prompt"),
            }
        );
        record_accepted_submission(&mut chat, &first_submission);
        chat.state_mut().push_input_text("second prompt");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let second_submission = rx.try_recv().unwrap();
        assert_eq!(
            second_submission,
            AppEvent::SubmitInput {
                sequence: 1,
                input: QueuedInput::from_text("second prompt"),
            }
        );
        record_accepted_submission(&mut chat, &second_submission);

        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "second prompt");
        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "first prompt");
        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "second prompt");
        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "");
    }

    #[test]
    fn up_and_down_move_inside_composer_when_cursor_is_not_at_end() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let _ = rx.try_recv();
        chat.state_mut().push_input_text("abc\ndef");
        chat.state_mut().move_input_home();
        chat.state_mut().move_input_right();

        chat.handle_key_event_for_width(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 80);
        assert_eq!(chat.state().input(), "abc\ndef");
        chat.handle_key_event_for_width(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), 80);
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "abc\ndXef");
    }

    #[test]
    fn down_moves_inside_wrapped_composer_line_when_cursor_is_not_at_end() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let _ = rx.try_recv();
        chat.state_mut().push_input_text("abcdefghijk");
        chat.state_mut().move_input_home();
        chat.state_mut().move_input_right();
        chat.state_mut().move_input_right();

        chat.handle_key_event_for_width(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), 8);
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "abcdefghXijk");
    }

    #[test]
    fn down_moves_inside_cjk_emoji_composer_when_cursor_is_not_at_end() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let _ = rx.try_recv();
        chat.state_mut().push_input_text("你好🙂世界ab");
        chat.state_mut().move_input_home();
        chat.state_mut().move_input_right();
        chat.state_mut().move_input_right();

        chat.handle_key_event_for_width(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), 8);
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "你好🙂世界Xab");
    }

    #[test]
    fn history_navigation_restores_draft_after_browsing() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let submitted = rx.try_recv().unwrap();
        record_accepted_submission(&mut chat, &submitted);
        chat.state_mut().push_input_text("draft");

        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "draft");
    }

    #[test]
    fn editing_recalled_history_exits_history_navigation() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let submitted = rx.try_recv().unwrap();
        record_accepted_submission(&mut chat, &submitted);
        chat.state_mut().push_input_text("draft");

        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('!'), KeyModifiers::NONE));
        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "remembered!");
    }

    #[test]
    fn arrow_up_inside_composer_moves_cursor_instead_of_recalling_history() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let submitted = rx.try_recv().unwrap();
        record_accepted_submission(&mut chat, &submitted);
        chat.state_mut().push_input_text("ab\ncd");
        chat.state_mut().move_input_left();

        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "aXb\ncd");
    }

    #[test]
    fn arrow_down_inside_composer_moves_cursor_to_next_visual_line() {
        let (sender, _rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("ab\ncd");
        chat.state_mut().move_input_home();
        chat.state_mut().move_input_right();

        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "ab\ncXd");
    }

    #[test]
    fn arrow_up_at_multiline_end_recalls_history() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("remembered");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let submitted = rx.try_recv().unwrap();
        record_accepted_submission(&mut chat, &submitted);
        chat.state_mut().push_input_text("ab\ncd");

        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "remembered");
    }

    #[test]
    fn arrow_vertical_navigation_uses_passed_width() {
        let (sender, _rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("abcdefg");
        chat.state_mut().move_input_left();

        chat.handle_key_event_for_width(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 8);
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "Xabcdefg");

        let (sender, _rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("abcdefg");
        chat.state_mut().move_input_left();

        chat.handle_key_event_for_width(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 12);
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "abcdefXg");
    }

    #[test]
    fn arrow_up_at_wrapped_line_start_uses_next_visual_line() {
        let (sender, _rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("abcdefg");
        chat.state_mut().move_input_left();

        chat.handle_key_event_for_width(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), 8);
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "Xabcdefg");
    }

    #[test]
    fn arrow_down_inside_composer_keeps_cjk_cursor_on_char_boundary() {
        let (sender, _rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("你好\n世界");
        chat.state_mut().move_input_home();
        chat.state_mut().move_input_right();

        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "你好\n世X界");
    }

    #[test]
    fn slash_commands_are_submitted_as_raw_input() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("/help");
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let submitted = rx.try_recv().unwrap();
        assert_eq!(
            submitted,
            AppEvent::SubmitInput {
                sequence: 0,
                input: QueuedInput::from_text("/help"),
            }
        );
        record_accepted_submission(&mut chat, &submitted);

        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "/help");
    }

    #[test]
    fn mcp_slash_command_enter_while_running_is_submitted_as_immediate_panel_action() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().begin_pending_turn("active".into());
        chat.state_mut().push_input_text("/mcp");

        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(
            rx.try_recv().unwrap(),
            AppEvent::SubmitInput {
                sequence: 0,
                input: QueuedInput::from_text("/mcp"),
            }
        );
        assert!(chat.state().status_notice_line().is_none());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn slash_completion_enter_fills_command_before_submit() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("/re");

        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "/resume");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());

        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(
            rx.try_recv().unwrap(),
            AppEvent::SubmitInput {
                sequence: 0,
                input: QueuedInput::from_text("/resume"),
            }
        );
    }

    #[test]
    fn skill_slash_completion_enter_fills_skill_before_submit() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut()
            .set_slash_skills([("tui-smoke-test-with-tmux", "tmux 冒烟测试")]);
        chat.state_mut().push_input_text("/tui-");

        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "/tui-smoke-test-with-tmux");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn slash_completion_arrows_select_and_tab_accepts() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("/");

        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        chat.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));

        assert_eq!(chat.state().input(), "/copy");
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn footer_status_line_stays_within_terminal_width() {
        let mut state = SessionTuiState::new();
        state.model_name = Some("example-chat-model".into());

        for width in [48, 60, 72, 88, 96, 120] {
            let line = status_line(&state, width);
            assert!(
                line.width() <= usize::from(width),
                "Footer width {} exceeded terminal width {}: {}",
                line.width(),
                width,
                line
            );
        }
    }

    #[test]
    fn footer_status_line_stays_within_width_with_cjk_labels() {
        // CJK 全宽字符按显示列宽截断，不能击穿 footer 宽度（truncate_label 列宽化回归）。
        let mut state = SessionTuiState::new();
        state.model_name = Some("模型名称很长很长很长很长很长".into());
        state.set_workspace_context(
            "中文目录名称很长很长".into(),
            "feature/中文分支名很长很长".into(),
        );

        for width in [48, 60, 72, 96, 120, 160] {
            let line = status_line(&state, width);
            assert!(
                line.width() <= usize::from(width),
                "CJK footer width {} exceeded terminal width {}: {}",
                line.width(),
                width,
                line
            );
        }
    }

    #[test]
    fn footer_status_line_colors_each_metadata_segment() {
        let mut state = SessionTuiState::new();
        state.model_name = Some("example-chat-model".into());
        state.set_workspace_context("agent-claim-network".into(), "feat/tetris_style".into());
        state.set_focus_duration_for_test(Duration::from_secs(37));

        let line = status_line(&state, 140);
        let span_color = |content: &str| {
            line.spans
                .iter()
                .find(|span| span.content == content)
                .and_then(|span| span.style.fg)
        };
        let value_color_after = |label: &str| {
            line.spans
                .windows(2)
                .find(|pair| pair[0].content == label)
                .and_then(|pair| pair[1].style.fg)
        };

        assert_eq!(span_color("model "), Some(FOOTER_LABEL_FG));
        assert_eq!(value_color_after("model "), Some(FOOTER_MODEL_FG));
        assert_eq!(span_color("cwd "), Some(FOOTER_LABEL_FG));
        assert_eq!(value_color_after("cwd "), Some(FOOTER_CWD_FG));
        assert_eq!(span_color("branch "), Some(FOOTER_LABEL_FG));
        assert_eq!(value_color_after("branch "), Some(FOOTER_BRANCH_FG));
        assert_eq!(span_color("ctx "), Some(FOOTER_LABEL_FG));
        assert_eq!(value_color_after("ctx "), Some(FOOTER_CTX_FG));
        assert_eq!(span_color("focus "), Some(FOOTER_LABEL_FG));
        assert_eq!(value_color_after("focus "), Some(FOOTER_FOCUS_FG));
        assert_eq!(span_color("open"), Some(Color::Green));
    }

    #[test]
    fn large_paste_uses_visible_placeholder_and_submits_full_text() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        let pasted = format!("//! {}\n{}", "x".repeat(1200), "fn main() {}");

        chat.handle_paste(pasted.clone());

        assert!(chat.state().input().starts_with("[Pasted Content "));
        assert!(!chat.state().input().contains("fn main"));
        let _ = rx.try_recv();

        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let AppEvent::SubmitInput {
            input: submitted, ..
        } = rx.try_recv().unwrap()
        else {
            panic!("Expected submitted paste");
        };
        assert_eq!(submitted.text(), pasted);
        assert!(submitted
            .draft()
            .visible_text()
            .starts_with("[Pasted Content "));
        assert!(!submitted.draft().visible_text().contains("fn main"));
        chat.state_mut()
            .record_submitted_draft(submitted.draft().clone());

        chat.handle_key_event(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));

        assert!(chat.state().input().starts_with("[Pasted Content "));
        assert!(!chat.state().input().contains("fn main"));
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        let AppEvent::SubmitInput {
            input: submitted, ..
        } = rx.try_recv().unwrap()
        else {
            panic!("Expected submitted paste from history");
        };
        assert_eq!(submitted.text(), pasted);
        assert!(submitted
            .draft()
            .visible_text()
            .starts_with("[Pasted Content "));
    }

    #[tokio::test]
    async fn at_path_text_file_resolves_attachment_and_keeps_inline_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notes.md");
        std::fs::write(&path, "hello attachment").unwrap();
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        let input = format!("总结一下 @{}", path.display());
        chat.state_mut().push_input_text(&input);
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let AppEvent::AtPathResolved {
            sequence,
            expanded_input,
            result,
            ..
        } = rx.recv().await.unwrap()
        else {
            panic!("Expected async at-path resolution");
        };
        assert_eq!(sequence, 0);
        assert_eq!(expanded_input, input);
        assert_eq!(
            result,
            Ok(super::super::attachment::ResolvedAtPaths {
                attachments: vec![SessionAttachment::TextFile { path }],
                directory_context: String::new(),
            })
        );
        // 解析期间输入框已清空，等待回灌提交
        assert_eq!(chat.state().input(), "");
    }

    #[tokio::test]
    async fn disabled_attachments_send_at_path_as_plain_text_without_resolving() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut()
            .set_attachment_config(crate::config::AttachmentConfig {
                enabled: false,
                ..Default::default()
            });
        let input = "请看 @src/".to_string();
        chat.state_mut().push_input_text(&input);

        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let AppEvent::SubmitInput {
            input: submitted, ..
        } = rx.recv().await.unwrap()
        else {
            panic!("附件总开关关闭时应按普通文本直接提交");
        };
        assert_eq!(submitted.text(), input);
    }

    #[tokio::test]
    async fn at_path_pdf_resolves_document_attachment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("brief.pdf");
        std::fs::write(&path, b"%PDF-1.7").unwrap();
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        chat.state_mut()
            .push_input_text(&format!("@{}", path.display()));
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let AppEvent::AtPathResolved { result, .. } = rx.recv().await.unwrap() else {
            panic!("Expected async at-path resolution");
        };
        assert_eq!(
            result,
            Ok(super::super::attachment::ResolvedAtPaths {
                attachments: vec![SessionAttachment::DocumentFile {
                    path,
                    media_type: "application/pdf".into(),
                }],
                directory_context: String::new(),
            })
        );
    }

    #[tokio::test]
    async fn at_path_missing_file_reports_error_with_original_draft() {
        let dir = tempfile::tempdir().unwrap();
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        let input = format!("看下 @{}/missing.txt", dir.path().display());
        chat.state_mut().push_input_text(&input);
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let AppEvent::AtPathResolved { draft, result, .. } = rx.recv().await.unwrap() else {
            panic!("Expected async at-path resolution");
        };
        // 解析失败：不提交；原始草稿交给 app 层做 UI-only 回显与输入召回记录。
        assert!(result.unwrap_err().contains("不存在"));
        assert_eq!(draft.visible_text(), input);
    }

    #[tokio::test]
    async fn at_path_count_includes_existing_clipboard_attachments() {
        let dir = tempfile::tempdir().unwrap();
        let mut at_text = String::new();
        for index in 0..5 {
            let path = dir.path().join(format!("f{index}.txt"));
            std::fs::write(&path, "x").unwrap();
            at_text.push_str(&format!("@{} ", path.display()));
        }
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        // 1 张剪贴板图片 + 5 个 @path = 6 > max_files_per_turn(5)
        chat.state_mut().apply_clipboard_image_read(
            0,
            Ok(Some(crate::attachment::NormalizedMedia {
                media_type: "image/png".into(),
                data: "QUJD".into(),
                kind: crate::attachment::AttachmentKind::Image,
                source_name: "clipboard image".into(),
            })),
        );
        chat.state_mut().push_input_text(&format!(" {at_text}"));
        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let AppEvent::AtPathResolved { result, .. } = rx.recv().await.unwrap() else {
            panic!("Expected async at-path resolution");
        };
        assert!(result.unwrap_err().contains("数量超限"));
    }

    #[test]
    fn ctrl_o_requests_preview_for_at_path_under_cursor() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().bump_interaction_generation();
        chat.state_mut().push_input_text("看下 @docs/a.md");

        chat.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));

        assert_eq!(
            rx.try_recv().unwrap(),
            AppEvent::PreviewAttachment {
                interaction_generation: 1,
                targets: vec![super::super::attachment::PreviewTarget::AtPath {
                    raw_path: "docs/a.md".into()
                }]
            }
        );
    }

    #[test]
    fn ctrl_o_without_attachments_gives_hint_only() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut().push_input_text("普通文本");

        chat.handle_key_event(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL));

        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn ctrl_v_with_attachments_disabled_gives_hint_without_attaching() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut()
            .set_attachment_config(crate::config::AttachmentConfig {
                enabled: false,
                ..Default::default()
            });

        chat.handle_key_event(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::CONTROL));

        // 只提示重绘（transcript 中有轻提示），不产生附件、不读剪贴板
        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());
        assert_eq!(chat.state().input(), "");
        assert!(chat
            .state()
            .transcript_text()
            .contains("附件功能已禁用：启用请设置 agent.attachment.enabled = true。"));
    }

    #[test]
    fn ctrl_v_with_clipboard_images_disabled_gives_hint_without_attaching() {
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);
        chat.state_mut()
            .set_attachment_config(crate::config::AttachmentConfig {
                clipboard_image_enabled: false,
                ..Default::default()
            });

        chat.handle_key_event(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::CONTROL));

        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert!(rx.try_recv().is_err());
        assert_eq!(chat.state().input(), "");
        assert!(chat.state().transcript_text().contains(
            "图片附件已禁用：启用请设置 agent.attachment.clipboard_image_enabled = true    agent.attachment.enabled = true。"
        ));
    }

    #[test]
    fn pasted_path_stays_plain_text_without_attachment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("diagram.png");
        std::fs::write(&path, b"fake png").unwrap();
        let (sender, mut rx) = AppEventSender::channel();
        let mut chat = ChatWidget::new(sender);

        chat.handle_paste(path.display().to_string());

        assert_eq!(rx.try_recv().unwrap(), AppEvent::RenderRequested);
        assert_eq!(chat.state().input(), path.display().to_string());

        chat.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let AppEvent::SubmitInput {
            input: submitted, ..
        } = rx.try_recv().unwrap()
        else {
            panic!("Expected plain text submission");
        };
        assert!(submitted.attachments().is_empty());
    }
}
