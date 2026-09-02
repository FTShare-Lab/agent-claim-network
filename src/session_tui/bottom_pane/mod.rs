//! TUI 底部输入区状态。
//!
//! 本模块封装 composer 编辑、slash 命令分类、排队输入预览和 footer hint。
//! 更高层的提交、退出、中断等业务意图通过 `AppEvent` 处理。

use std::path::PathBuf;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::UnicodeWidthStr;

use crate::agent::SessionRuntimeStatus;
use crate::api::SessionAttachment;

use super::at_path::{active_at_path_token, scan_at_path_tokens, split_at_path_segments};
use super::at_path_completion::{
    completion_context, encoded_at_path, matching_candidates, AtPathCandidate, AtPathCandidateKind,
    AtPathCompletionContext, AtPathCompletionLimits, AtPathDirectoryEntry,
};
use super::attachment::{InputAttachment, PreviewTarget};
use super::completion_menu::{render_completion_menu, truncate_to_width, CompletionMenuState};
use super::composer::ComposerState;
use super::input_queue::PendingInputPreview;
use super::slash_command::{
    is_slash_command_like, render_slash_menu, SlashCommandAction, SlashCommandCatalog,
    SlashCommandEntryKind,
};
use super::theme::{muted_style, CODE_CONTENT_FG};
use super::wrapping::VisualLine;

const MAX_COMPOSER_INPUT_ROWS: usize = 8;
const LARGE_PASTE_CHAR_THRESHOLD: usize = 1000;
const QUEUED_PREVIEW_CHAR_LIMIT: usize = 120;
const INPUT_BG: Color = Color::Rgb(226, 224, 219);
const INPUT_FG: Color = Color::Rgb(35, 35, 33);
const PLACEHOLDER_FG: Color = Color::Rgb(145, 141, 136);
/// `@path` 在输入框中的高亮前景色（灰色加粗，背景沿用输入条）。
const AT_PATH_FG: Color = Color::Rgb(121, 116, 110);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputAction {
    Send(String),
    ShellCommand(String),
    Help,
    Inbox,
    Mcp,
    New,
    Ps,
    Compact,
    Copy,
    Resume,
    Skills,
    Subagents,
    Exit,
    Unknown(String),
    Ignore,
}

/// Ctrl+O 预览的命中结果：光标在某个附件上时只取它，
/// 否则取输入框里的全部附件（按出现顺序，一并交给系统默认应用打开）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PreviewHit {
    Targets(Vec<PreviewTarget>),
    NoAttachments,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct InputDraft {
    pub(super) text: String,
    pub(super) pending_pastes: Vec<(String, String)>,
    pub(super) attachments: Vec<InputAttachment>,
}

impl InputDraft {
    pub(super) fn new(text: String) -> Self {
        Self {
            text,
            pending_pastes: Vec::new(),
            attachments: Vec::new(),
        }
    }

    pub(super) fn visible_text(&self) -> &str {
        &self.text
    }

    pub(super) fn expanded_text(&self) -> String {
        expand_pending_pastes(self.text.clone(), &self.pending_pastes)
    }

    /// 收集仍出现在文本中的附件（删掉占位符即等于撤销该附件）。
    pub(super) fn session_attachments(&self) -> Vec<SessionAttachment> {
        self.attachments
            .iter()
            .filter(|attachment| self.text.contains(&attachment.placeholder))
            .map(InputAttachment::to_session_attachment)
            .collect()
    }

    pub(super) fn is_visible_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub(super) fn append_with_newline(&mut self, next: InputDraft) {
        if self.text.is_empty() {
            *self = next;
            return;
        }
        if next.text.is_empty() {
            return;
        }

        self.text.push('\n');
        let mut next_text = next.text;
        let mut pending_pastes = next.pending_pastes;
        let mut reserved_placeholders = self
            .pending_pastes
            .iter()
            .map(|(placeholder, _)| placeholder.clone())
            .collect::<Vec<_>>();
        reserved_placeholders.extend(
            pending_pastes
                .iter()
                .map(|(placeholder, _)| placeholder.clone()),
        );
        let mut replacements = Vec::new();
        for (placeholder, _) in &mut pending_pastes {
            let final_placeholder = if self
                .pending_pastes
                .iter()
                .any(|(used_placeholder, _)| used_placeholder == placeholder)
            {
                unique_placeholder_for_merge(placeholder, &reserved_placeholders)
            } else {
                placeholder.clone()
            };
            reserved_placeholders.push(final_placeholder.clone());
            replacements.push((placeholder.clone(), final_placeholder.clone()));
            *placeholder = final_placeholder;
        }
        replacements.sort_by_key(|(placeholder, _)| std::cmp::Reverse(placeholder.len()));
        for (placeholder, final_placeholder) in replacements {
            next_text = next_text.replace(&placeholder, &final_placeholder);
        }
        self.pending_pastes.extend(pending_pastes);
        let mut attachments = next.attachments;
        let mut reserved_attachment_placeholders = self
            .attachments
            .iter()
            .map(|attachment| attachment.placeholder.clone())
            .collect::<Vec<_>>();
        reserved_attachment_placeholders.extend(
            attachments
                .iter()
                .map(|attachment| attachment.placeholder.clone()),
        );
        let mut attachment_replacements = Vec::new();
        for attachment in &mut attachments {
            let final_placeholder = if self
                .attachments
                .iter()
                .any(|used| used.placeholder == attachment.placeholder)
            {
                unique_placeholder_for_merge(
                    &attachment.placeholder,
                    &reserved_attachment_placeholders,
                )
            } else {
                attachment.placeholder.clone()
            };
            reserved_attachment_placeholders.push(final_placeholder.clone());
            attachment_replacements
                .push((attachment.placeholder.clone(), final_placeholder.clone()));
            attachment.placeholder = final_placeholder;
        }
        attachment_replacements
            .sort_by_key(|(placeholder, _)| std::cmp::Reverse(placeholder.len()));
        for (placeholder, final_placeholder) in attachment_replacements {
            next_text = next_text.replace(&placeholder, &final_placeholder);
        }
        self.attachments.extend(attachments);
        self.text.push_str(&next_text);
    }
}

impl From<String> for InputDraft {
    fn from(text: String) -> Self {
        Self::new(text)
    }
}

impl From<&str> for InputDraft {
    fn from(text: &str) -> Self {
        Self::new(text.to_string())
    }
}

#[derive(Debug, Clone, Default)]
struct AtPathMenuState {
    completion: CompletionMenuState,
    context: Option<AtPathCompletionContext>,
    loaded_directory: Option<PathBuf>,
    entries: Vec<AtPathDirectoryEntry>,
    loading: bool,
    error: Option<String>,
    retry_blocked_directory: Option<PathBuf>,
    dismissed_for_context: Option<(std::ops::Range<usize>, String)>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct BottomPane {
    composer: ComposerState,
    input_history: InputHistory,
    pending_pastes: Vec<(String, String)>,
    attachments: Vec<InputAttachment>,
    slash_menu: CompletionMenuState,
    slash_catalog: SlashCommandCatalog,
    at_path_menu: AtPathMenuState,
    at_path_workspace_root: PathBuf,
    at_path_limits: AtPathCompletionLimits,
    /// 附件功能被配置关闭时置 true，输入框不再高亮或补全 `@path`。
    at_path_highlight_disabled: bool,
    /// Finalize 失败后 session 已进入不可恢复终态；composer 只保留只读展示。
    finalize_failed: bool,
}

impl BottomPane {
    pub(super) fn input(&self) -> &str {
        self.composer.input()
    }

    pub(super) fn at_path_workspace_root(&self) -> &std::path::Path {
        &self.at_path_workspace_root
    }

    pub(super) fn current_draft(&self) -> InputDraft {
        InputDraft {
            text: self.composer.input().to_string(),
            pending_pastes: self.pending_pastes.clone(),
            attachments: self.attachments.clone(),
        }
    }

    pub(super) fn set_at_path_highlight(&mut self, enabled: bool) {
        self.at_path_highlight_disabled = !enabled;
        if !enabled {
            self.reset_at_path_menu();
        }
    }

    pub(super) fn set_at_path_completion_config(
        &mut self,
        workspace_root: PathBuf,
        limits: AtPathCompletionLimits,
    ) {
        self.at_path_workspace_root = workspace_root;
        self.at_path_limits = limits;
        self.refresh_at_path_context();
    }

    /// 注入 workspace skills，重建 slash 命令目录（skills 字母序在原生命令前）。
    pub(super) fn set_slash_skills<'a>(
        &mut self,
        skills: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) {
        self.slash_catalog = SlashCommandCatalog::with_skills(skills);
        self.slash_menu.reset();
    }

    pub(super) fn slash_catalog(&self) -> &SlashCommandCatalog {
        &self.slash_catalog
    }

    pub(super) fn push_char(&mut self, c: char) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.composer.push_char(c);
        self.refresh_at_path_context();
    }

    #[cfg(test)]
    pub(super) fn push_text(&mut self, text: &str) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.composer.push_text(text);
        self.refresh_at_path_context();
    }

    pub(super) fn push_paste_text(&mut self, text: &str) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        let pasted = normalize_pasted_text(text);
        let char_count = pasted.chars().count();
        if char_count > LARGE_PASTE_CHAR_THRESHOLD {
            let placeholder = self.next_large_paste_placeholder(char_count);
            self.composer.push_text(&placeholder);
            self.pending_pastes.push((placeholder, pasted));
        } else {
            self.composer.push_text(&pasted);
        }
        self.refresh_at_path_context();
    }

    /// 把规格化完成的剪贴板图片挂成 `[Image #N]` 占位附件。
    pub(super) fn push_clipboard_image(&mut self, media: crate::attachment::NormalizedMedia) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        let placeholder = format!("[Image #{}]", self.attachments.len().saturating_add(1));
        let needs_space_before = !self.composer.input().is_empty()
            && !self
                .composer
                .input()
                .ends_with(|ch: char| ch.is_whitespace());
        if needs_space_before {
            self.composer.push_char(' ');
        }
        self.composer.push_text(&placeholder);
        self.attachments
            .push(InputAttachment::clipboard_image(placeholder, media));
        self.refresh_at_path_context();
    }

    /// 仍出现在输入文本中的剪贴板附件数（删除占位符即撤销附件）。
    pub(super) fn effective_attachment_count(&self) -> usize {
        self.attachments
            .iter()
            .filter(|attachment| self.composer.input().contains(&attachment.placeholder))
            .count()
    }

    /// 输入框中所有附件标记的高亮区间：`@path` 词法 token（含解析失败的，
    /// 便于发现写错的路径）+ 仍在文本中的 `[Image #N]` 占位符，按出现位置排序。
    fn attachment_highlight_ranges(&self) -> Vec<std::ops::Range<usize>> {
        let input = self.composer.input();
        let mut ranges = scan_at_path_tokens(input)
            .into_iter()
            .map(|token| token.range)
            .collect::<Vec<_>>();
        for attachment in &self.attachments {
            if let Some(start) = input.find(&attachment.placeholder) {
                ranges.push(start..start + attachment.placeholder.len());
            }
        }
        ranges.sort_by_key(|range| range.start);
        ranges
    }

    /// Ctrl+O 命中判定：光标落在某个附件标记上（含紧贴标记末尾）时只取它；
    /// 否则取输入框里的全部附件，按出现顺序交给 Quick Look 一次预览。
    pub(super) fn preview_target_at_cursor(&self) -> PreviewHit {
        let input = self.composer.input();
        let cursor = self.composer.cursor_byte_index();
        let mut candidates: Vec<(std::ops::Range<usize>, PreviewTarget)> = Vec::new();
        for token in scan_at_path_tokens(input) {
            if let Ok(raw_path) = token.parsed {
                candidates.push((token.range, PreviewTarget::AtPath { raw_path }));
            }
        }
        for attachment in &self.attachments {
            if let Some(start) = input.find(&attachment.placeholder) {
                candidates.push((
                    start..start + attachment.placeholder.len(),
                    attachment.to_preview_target(),
                ));
            }
        }
        if candidates.is_empty() {
            return PreviewHit::NoAttachments;
        }
        candidates.sort_by_key(|(range, _)| range.start);
        if let Some((_, target)) = candidates
            .iter()
            .find(|(range, _)| range.start <= cursor && cursor <= range.end)
        {
            return PreviewHit::Targets(vec![target.clone()]);
        }
        PreviewHit::Targets(candidates.into_iter().map(|(_, target)| target).collect())
    }

    pub(super) fn push_newline(&mut self) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.composer.push_newline();
        self.refresh_at_path_context();
    }

    pub(super) fn pop_char(&mut self) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.composer.pop_char();
        self.refresh_at_path_context();
    }

    pub(super) fn delete_char(&mut self) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.composer.delete_char();
        self.refresh_at_path_context();
    }

    pub(super) fn move_left(&mut self) {
        self.composer.move_left();
        self.refresh_at_path_context();
    }

    pub(super) fn move_right(&mut self) {
        self.composer.move_right();
        self.refresh_at_path_context();
    }

    pub(super) fn move_word_left(&mut self) {
        self.composer.move_word_left();
        self.refresh_at_path_context();
    }

    pub(super) fn move_word_right(&mut self) {
        self.composer.move_word_right();
        self.refresh_at_path_context();
    }

    pub(super) fn move_home(&mut self) {
        self.composer.move_home();
        self.refresh_at_path_context();
    }

    pub(super) fn move_end(&mut self) {
        self.composer.move_end();
        self.refresh_at_path_context();
    }

    pub(super) fn move_up(&mut self, width: u16) -> bool {
        let moved = self.composer.move_up(width);
        if moved {
            self.refresh_at_path_context();
        }
        moved
    }

    pub(super) fn move_down(&mut self, width: u16) -> bool {
        let moved = self.composer.move_down(width);
        if moved {
            self.refresh_at_path_context();
        }
        moved
    }

    pub(super) fn cursor_at_end(&self) -> bool {
        self.composer.cursor_at_end()
    }

    pub(super) fn take_draft(&mut self) -> InputDraft {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.reset_at_path_menu();
        let visible_text = self.composer.take();
        let pending_pastes = std::mem::take(&mut self.pending_pastes);
        let attachments = std::mem::take(&mut self.attachments);
        InputDraft {
            text: visible_text,
            pending_pastes,
            attachments,
        }
    }

    #[cfg(test)]
    pub(super) fn take(&mut self) -> String {
        self.take_draft().expanded_text()
    }

    pub(super) fn clear_input(&mut self) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.reset_at_path_menu();
        let _ = self.composer.take();
        self.pending_pastes.clear();
        self.attachments.clear();
    }

    pub(super) fn set_draft(&mut self, draft: InputDraft) {
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.composer.set_text(draft.text);
        self.pending_pastes = draft.pending_pastes;
        self.attachments = draft.attachments;
        self.refresh_at_path_context();
    }

    /// 记录本次提交自身的原始草稿，不能依赖全局“最后取走的草稿”。
    ///
    /// `@path` 预检是异步的，多个输入可能在解析完成前已被取走；历史必须与
    /// 当前按 sequence flush 的 `QueuedInput` 一一对应。
    pub(super) fn record_submitted_draft(&mut self, draft: InputDraft) {
        self.input_history.record(draft);
    }

    pub(super) fn set_finalize_failed(&mut self, failed: bool) {
        self.finalize_failed = failed;
        if failed {
            self.input_history.reset_navigation();
            self.slash_menu.reset();
            self.reset_at_path_menu();
        }
    }

    pub(super) fn finalize_failed(&self) -> bool {
        self.finalize_failed
    }

    pub(super) fn recall_previous_input(&mut self) -> bool {
        let current = self.current_draft();
        if let Some(entry) = self.input_history.previous(current) {
            self.set_history_entry(entry);
            true
        } else {
            false
        }
    }

    pub(super) fn recall_next_input(&mut self) -> bool {
        if let Some(entry) = self.input_history.next() {
            self.set_history_entry(entry);
            true
        } else {
            false
        }
    }

    pub(super) fn slash_menu_visible(&self) -> bool {
        self.slash_catalog.should_show_menu(self.input())
    }

    pub(super) fn at_path_menu_visible(&self) -> bool {
        if self.at_path_highlight_disabled || self.slash_menu_visible() {
            return false;
        }
        let Some(context) = &self.at_path_menu.context else {
            return false;
        };
        !self
            .at_path_menu
            .dismissed_for_context
            .as_ref()
            .is_some_and(|dismissed| {
                dismissed.0 == context.token.range && dismissed.1 == context.token.raw_path
            })
            && (self.at_path_menu.loading
                || self.at_path_menu.error.is_some()
                || !self.at_path_candidates().is_empty())
    }

    pub(super) fn at_path_scan_request(&self) -> Option<(PathBuf, usize)> {
        if self.at_path_highlight_disabled {
            return None;
        }
        let context = self.at_path_menu.context.as_ref()?;
        if self.at_path_menu.loading {
            return None;
        }
        if self.at_path_menu.retry_blocked_directory.as_ref() == Some(&context.scan_dir) {
            return None;
        }
        if self.at_path_menu.loaded_directory.as_ref() == Some(&context.scan_dir) {
            return None;
        }
        Some((
            context.scan_dir.clone(),
            self.at_path_limits.max_directory_entries,
        ))
    }

    pub(super) fn mark_at_path_scan_started(&mut self, directory: &PathBuf) -> bool {
        let Some(context) = &self.at_path_menu.context else {
            return false;
        };
        if &context.scan_dir != directory {
            return false;
        }
        self.at_path_menu.loading = true;
        self.at_path_menu.error = None;
        true
    }

    pub(super) fn apply_at_path_directory_read(
        &mut self,
        directory: PathBuf,
        result: Result<Vec<AtPathDirectoryEntry>, String>,
    ) -> bool {
        let Some(context) = &self.at_path_menu.context else {
            return false;
        };
        if context.scan_dir != directory {
            return false;
        }
        self.at_path_menu.loading = false;
        match result {
            Ok(entries) => {
                self.at_path_menu.loaded_directory = Some(directory);
                self.at_path_menu.entries = entries;
                self.at_path_menu.error = None;
                self.at_path_menu.retry_blocked_directory = None;
            }
            Err(error) => {
                self.at_path_menu.loaded_directory = None;
                self.at_path_menu.entries.clear();
                self.at_path_menu.error = Some(error);
                self.at_path_menu.retry_blocked_directory = Some(directory);
            }
        }
        self.at_path_menu.completion.reset();
        true
    }

    pub(super) fn select_previous_at_path_completion(&mut self) -> bool {
        let count = self.at_path_candidates().len();
        self.at_path_menu.completion.select_previous(count)
    }

    pub(super) fn select_next_at_path_completion(&mut self) -> bool {
        let count = self.at_path_candidates().len();
        self.at_path_menu.completion.select_next(count)
    }

    pub(super) fn accept_at_path_completion(&mut self) -> bool {
        let candidates = self.at_path_candidates();
        let Some(candidate) = self.at_path_menu.completion.selected(&candidates).cloned() else {
            return false;
        };
        let Some(context) = self.at_path_menu.context.clone() else {
            return false;
        };
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        let replacement = encoded_at_path(&candidate.raw_path);
        if !self
            .composer
            .replace_range(context.token.range, &replacement)
        {
            return false;
        }
        self.at_path_menu.dismissed_for_context = None;
        self.refresh_at_path_context();
        // 目录补全后继续展示下一级；只有文件补全（路径已闭合）才关闭菜单。
        // 引用目录本身由用户再输入空格结束 token。
        if candidate.kind == AtPathCandidateKind::File {
            if let Some(context) = &self.at_path_menu.context {
                self.at_path_menu.dismissed_for_context =
                    Some((context.token.range.clone(), context.token.raw_path.clone()));
            }
        }
        true
    }

    pub(super) fn dismiss_at_path_menu(&mut self) -> bool {
        let Some(context) = &self.at_path_menu.context else {
            return false;
        };
        if !self.at_path_menu_visible() {
            return false;
        }
        self.at_path_menu.dismissed_for_context =
            Some((context.token.range.clone(), context.token.raw_path.clone()));
        true
    }

    pub(super) fn select_previous_slash_completion(&mut self) -> bool {
        let match_count = self.slash_catalog.matching(self.input()).len();
        self.slash_menu.select_previous(match_count)
    }

    pub(super) fn select_next_slash_completion(&mut self) -> bool {
        let match_count = self.slash_catalog.matching(self.input()).len();
        self.slash_menu.select_next(match_count)
    }

    pub(super) fn accept_slash_completion(&mut self) -> bool {
        let matches = self.slash_catalog.matching(self.input());
        let Some(entry) = self.slash_menu.selected(&matches) else {
            return false;
        };
        self.input_history.reset_navigation();
        self.composer.set_text(entry.command.clone());
        self.pending_pastes.clear();
        self.slash_menu.reset();
        self.reset_at_path_menu();
        true
    }

    /// 行中 `空白 + /前缀` 的浅色 skill 补全提示：光标在输入末尾、且该前缀在用户
    /// skill 中唯一匹配时，返回缺失后缀（Tab 接受）。行首整段 slash 输入走菜单，不在此列。
    pub(super) fn inline_slash_hint(&self) -> Option<String> {
        let input = self.composer.input();
        if self.composer.cursor_byte_index() != input.len() {
            return None;
        }
        let token_start = input
            .char_indices()
            .rev()
            .find(|(_, ch)| ch.is_whitespace())
            .map(|(index, ch)| index + ch.len_utf8())?;
        let token = &input[token_start..];
        self.slash_catalog.unique_skill_completion_suffix(token)
    }

    pub(super) fn accept_inline_slash_hint(&mut self) -> bool {
        let Some(suffix) = self.inline_slash_hint() else {
            return false;
        };
        self.input_history.reset_navigation();
        self.slash_menu.reset();
        self.composer.push_text(&suffix);
        self.refresh_at_path_context();
        true
    }

    pub(super) fn lines_with_width(
        &self,
        status: SessionRuntimeStatus,
        running_task_label: Option<&str>,
        pending_preview: &PendingInputPreview,
        queued_count: usize,
        session_id: Option<&str>,
        width: u16,
    ) -> Vec<Line<'static>> {
        if self.finalize_failed {
            let hint_style = Style::default().fg(Color::DarkGray);
            return vec![session_hint_line(
                session_id,
                "Finalize failed · Ctrl+C quit",
                hint_style,
                width,
            )];
        }
        let input_enabled = input_accepts_text(status);
        let input_fg = if shell_command_input_is_active(self.input()) {
            CODE_CONTENT_FG
        } else {
            INPUT_FG
        };
        let input_bar_style = if input_enabled {
            Style::default().fg(input_fg).bg(INPUT_BG)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let prompt_style = if input_enabled {
            input_bar_style.add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let input_style = if input_enabled {
            input_bar_style
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let all_input_lines = self.composer.wrapped_lines(width);
        let (cursor_line_index, _) = self.composer.cursor_visual_position(width);
        let visible_start = self.visible_input_start_line(all_input_lines.len(), cursor_line_index);
        let visible_end = all_input_lines
            .len()
            .min(visible_start.saturating_add(MAX_COMPOSER_INPUT_ROWS));
        let input_lines = &all_input_lines[visible_start..visible_end];
        // 附件标记高亮区间（@path token + [Image #N] 占位符）基于整段输入文本，
        // 渲染时与每个折行片段求交；光标所在的标记叠加下划线，提示 Ctrl+O 的作用对象。
        let attachment_ranges = if input_enabled && !self.at_path_highlight_disabled {
            self.attachment_highlight_ranges()
        } else {
            Vec::new()
        };
        let cursor_byte = self.composer.cursor_byte_index();
        let cursor_range_index = attachment_ranges
            .iter()
            .position(|range| range.start <= cursor_byte && cursor_byte <= range.end);
        let at_path_style = input_style.fg(AT_PATH_FG).add_modifier(Modifier::BOLD);
        // 行中唯一匹配的 slash 补全提示：以浅色 ghost 文本接在光标（输入末尾）之后。
        let inline_hint = if input_enabled {
            self.inline_slash_hint()
        } else {
            None
        };
        let mut lines = input_lines
            .iter()
            .enumerate()
            .map(|(line_index, visual_line)| {
                let prompt = if visual_line.logical_line_index == 0
                    && !visual_line.is_wrapped_continuation
                {
                    "› "
                } else {
                    "  "
                };
                let input_text = self.visual_line_text(visual_line);
                let mut spans = vec![Span::styled(prompt, prompt_style)];
                if input_text.is_empty()
                    && self.input().is_empty()
                    && visual_line.logical_line_index == 0
                    && !visual_line.is_wrapped_continuation
                {
                    let placeholder_style = if input_enabled {
                        Style::default().fg(PLACEHOLDER_FG).bg(INPUT_BG)
                    } else {
                        Style::default().fg(Color::DarkGray)
                    };
                    spans.push(Span::styled("Whisper your wish here...", placeholder_style));
                } else if attachment_ranges.is_empty() {
                    spans.push(Span::styled(input_text, input_style));
                } else {
                    for (segment, hit_index) in split_at_path_segments(
                        &input_text,
                        visual_line.range.start,
                        &attachment_ranges,
                    ) {
                        let style = match hit_index {
                            Some(index) if Some(index) == cursor_range_index => {
                                at_path_style.add_modifier(Modifier::UNDERLINED)
                            }
                            Some(_) => at_path_style,
                            None => input_style,
                        };
                        spans.push(Span::styled(segment, style));
                    }
                }
                let mut input_line = Line::from(spans).style(input_bar_style);
                let is_last_input_line = visible_start.saturating_add(line_index).saturating_add(1)
                    == all_input_lines.len();
                if let (true, Some(hint)) = (is_last_input_line, &inline_hint) {
                    let remaining = usize::from(width).saturating_sub(input_line.width());
                    let ghost = truncate_to_width(hint, remaining);
                    if !ghost.is_empty() {
                        input_line.push_span(Span::styled(
                            ghost,
                            Style::default().fg(PLACEHOLDER_FG).bg(INPUT_BG),
                        ));
                    }
                }
                pad_line_to_width(&mut input_line, usize::from(width), input_bar_style);
                input_line
            })
            .collect::<Vec<_>>();

        if !pending_preview.is_empty() {
            lines.push(Line::styled(
                queued_preview_text(pending_preview),
                Style::default().fg(Color::DarkGray),
            ));
        }

        if input_enabled && self.slash_menu_visible() {
            lines.extend(render_slash_menu(
                &self.slash_catalog,
                self.input(),
                &self.slash_menu,
                width,
            ));
        } else if input_enabled && self.at_path_menu_visible() {
            let candidates = self.at_path_candidates();
            if !candidates.is_empty() {
                lines.extend(render_completion_menu(
                    &candidates,
                    &self.at_path_menu.completion,
                    width,
                ));
            } else if self.at_path_menu.loading {
                lines.push(Line::styled("  loading files...", muted_style()));
            } else if let Some(error) = &self.at_path_menu.error {
                lines.push(Line::styled(
                    truncate_to_width(error, usize::from(width)),
                    Style::default().fg(Color::Red),
                ));
            }
        }

        lines.push(self.hint_line_for_width(
            status,
            running_task_label,
            queued_count,
            session_id,
            width,
        ));
        lines
    }

    fn hint_line_for_width(
        &self,
        status: SessionRuntimeStatus,
        running_task_label: Option<&str>,
        queued_count: usize,
        session_id: Option<&str>,
        width: u16,
    ) -> Line<'static> {
        let hint_style = Style::default().fg(Color::DarkGray);
        let inline_slash_hint_visible = self.inline_slash_hint().is_some();
        let body_width = hint_body_width(width, session_id);
        let mut hint = if self.finalize_failed {
            "Finalize failed · Ctrl+C quit".to_string()
        } else if self.slash_menu_visible() {
            "↑↓ select · Tab/Enter complete".to_string()
        } else if self.at_path_menu_visible() {
            "↑↓ select · Tab/Enter complete · Esc close".to_string()
        } else {
            match status {
                SessionRuntimeStatus::Open | SessionRuntimeStatus::Error
                    if running_task_label.is_none() =>
                {
                    if body_width < 48 {
                        "type /".to_string()
                    } else {
                        "type / for commands · Enter sends".to_string()
                    }
                }
                SessionRuntimeStatus::Running => running_hint_for_width(body_width).to_string(),
                _ => self.hint_body(status, running_task_label, queued_count),
            }
        };
        if inline_slash_hint_visible {
            hint.push_str(" · Tab completes");
        }
        session_hint_line(session_id, &hint, hint_style, width)
    }

    fn hint_body(
        &self,
        status: SessionRuntimeStatus,
        running_task_label: Option<&str>,
        queued_count: usize,
    ) -> String {
        if self.finalize_failed {
            return "Finalize failed · Ctrl+C quit".into();
        }
        match status {
            SessionRuntimeStatus::Initializing => "initializing session...".into(),
            SessionRuntimeStatus::Open | SessionRuntimeStatus::Error
                if running_task_label.is_some() =>
            {
                format!(
                    "{} committing... queued={queued_count}",
                    running_task_label.unwrap_or("task")
                )
            }
            SessionRuntimeStatus::Open | SessionRuntimeStatus::Error => {
                "type / for commands · Enter sends".into()
            }
            SessionRuntimeStatus::Running => {
                "Enter queues · Ctrl+Enter steers · Esc recalls queue/cancels · Ctrl+C cancels"
                    .into()
            }
            SessionRuntimeStatus::SyncingInbox if queued_count > 0 => {
                format!("syncing inbox... queued={queued_count}")
            }
            SessionRuntimeStatus::SyncingInbox => "syncing inbox... input will be queued".into(),
            SessionRuntimeStatus::Compacting if queued_count > 0 => {
                format!("input will be queued · queued={queued_count}")
            }
            SessionRuntimeStatus::Compacting => "input will be queued".into(),
            SessionRuntimeStatus::Resuming if queued_count > 0 => {
                format!("waiting for target finalization... inputs queued={queued_count}")
            }
            SessionRuntimeStatus::Resuming => {
                "waiting for target finalization... inputs will be queued".into()
            }
            SessionRuntimeStatus::Finalizing => "finalizing session...".into(),
            SessionRuntimeStatus::Closed => "session closed".into(),
        }
    }

    #[cfg(test)]
    pub(super) fn hint(
        &self,
        status: SessionRuntimeStatus,
        running_task_label: Option<&str>,
        queued_count: usize,
        session_id: Option<&str>,
    ) -> String {
        let hint = self.hint_body(status, running_task_label, queued_count);
        match session_id.filter(|id| !id.is_empty()) {
            Some(session_id) => format!("{session_id} {hint}"),
            None => hint,
        }
    }

    #[cfg(test)]
    pub(super) fn cursor_x(&self, area_x: u16) -> u16 {
        self.cursor_x_for_width(area_x, u16::MAX)
    }

    pub(super) fn cursor_x_for_width(&self, area_x: u16, width: u16) -> u16 {
        let (_, visual_col) = self.composer.cursor_visual_position(width);
        let input_width = u16::try_from(visual_col).unwrap_or(u16::MAX);
        area_x.saturating_add(2).saturating_add(input_width)
    }

    #[cfg(test)]
    pub(super) fn cursor_y(&self, area_y: u16) -> u16 {
        self.cursor_y_for_width(area_y, u16::MAX)
    }

    pub(super) fn cursor_y_for_width(&self, area_y: u16, width: u16) -> u16 {
        let all_input_lines = self.composer.wrapped_lines(width);
        let (cursor_line_index, _) = self.composer.cursor_visual_position(width);
        let visible_start = self.visible_input_start_line(all_input_lines.len(), cursor_line_index);
        let visible_line_index = cursor_line_index
            .saturating_sub(visible_start)
            .min(MAX_COMPOSER_INPUT_ROWS.saturating_sub(1));
        area_y.saturating_add(u16::try_from(visible_line_index).unwrap_or(u16::MAX))
    }

    #[cfg(test)]
    pub(super) fn height(&self, pending_preview: &PendingInputPreview) -> u16 {
        self.height_for_width(pending_preview, u16::MAX)
    }

    #[cfg(test)]
    pub(super) fn height_for_width(
        &self,
        pending_preview: &PendingInputPreview,
        width: u16,
    ) -> u16 {
        if self.finalize_failed {
            return 1;
        }
        let input_line_count = self
            .composer
            .wrapped_lines(width)
            .len()
            .min(MAX_COMPOSER_INPUT_ROWS);
        let preview_line_count = usize::from(!pending_preview.is_empty());
        let slash_menu_line_count = if self.slash_menu_visible() {
            render_slash_menu(&self.slash_catalog, self.input(), &self.slash_menu, width).len()
        } else if self.at_path_menu_visible() {
            let candidates = self.at_path_candidates();
            if candidates.is_empty() {
                usize::from(self.at_path_menu.loading || self.at_path_menu.error.is_some())
            } else {
                render_completion_menu(&candidates, &self.at_path_menu.completion, width).len()
            }
        } else {
            0
        };
        u16::try_from(
            input_line_count
                .saturating_add(preview_line_count)
                .saturating_add(slash_menu_line_count)
                .saturating_add(1),
        )
        .unwrap_or(u16::MAX)
    }

    fn visible_input_start_line(&self, input_line_count: usize, cursor_line_index: usize) -> usize {
        if input_line_count <= MAX_COMPOSER_INPUT_ROWS {
            return 0;
        }
        cursor_line_index
            .saturating_add(1)
            .saturating_sub(MAX_COMPOSER_INPUT_ROWS)
    }

    fn visual_line_text(&self, visual_line: &VisualLine) -> String {
        self.input()
            .get(visual_line.range.clone())
            .unwrap_or_default()
            .to_string()
    }

    fn next_large_paste_placeholder(&self, char_count: usize) -> String {
        let base = format!("[Pasted Content {char_count} chars]");
        let mut max_suffix = 0usize;
        for (placeholder, _) in &self.pending_pastes {
            if placeholder == &base {
                max_suffix = max_suffix.max(1);
            } else if let Some(suffix) = placeholder
                .strip_prefix(&format!("[Pasted Content {char_count} chars #"))
                .and_then(|value| value.strip_suffix(']'))
                .and_then(|value| value.parse::<usize>().ok())
            {
                max_suffix = max_suffix.max(suffix);
            }
        }
        if max_suffix == 0 {
            base
        } else {
            suffixed_placeholder(&base, max_suffix + 1)
        }
    }

    fn set_history_entry(&mut self, entry: InputDraft) {
        self.slash_menu.reset();
        self.composer.set_text(entry.text);
        self.pending_pastes = entry.pending_pastes;
        self.attachments = entry.attachments;
        self.refresh_at_path_context();
    }

    fn at_path_candidates(&self) -> Vec<AtPathCandidate> {
        let Some(context) = &self.at_path_menu.context else {
            return Vec::new();
        };
        if self.at_path_menu.loaded_directory.as_ref() != Some(&context.scan_dir) {
            return Vec::new();
        }
        matching_candidates(
            &self.at_path_menu.entries,
            context,
            self.at_path_limits.max_candidates,
        )
    }

    fn refresh_at_path_context(&mut self) {
        if self.at_path_highlight_disabled || self.slash_menu_visible() {
            self.reset_at_path_menu();
            return;
        }
        let next_context =
            active_at_path_token(self.composer.input(), self.composer.cursor_byte_index())
                .map(|token| completion_context(token, &self.at_path_workspace_root));
        let same_context = self.at_path_menu.context.as_ref() == next_context.as_ref();
        if same_context {
            return;
        }
        let same_directory = self
            .at_path_menu
            .context
            .as_ref()
            .zip(next_context.as_ref())
            .is_some_and(|(previous, next)| previous.scan_dir == next.scan_dir);
        self.at_path_menu.context = next_context;
        self.at_path_menu.completion.reset();
        self.at_path_menu.dismissed_for_context = None;
        self.at_path_menu.error = None;
        self.at_path_menu.retry_blocked_directory = None;
        if !same_directory {
            self.at_path_menu.loaded_directory = None;
            self.at_path_menu.entries.clear();
            self.at_path_menu.loading = false;
        }
    }

    fn reset_at_path_menu(&mut self) {
        self.at_path_menu = AtPathMenuState::default();
    }
}

fn running_hint_for_width(width: u16) -> &'static str {
    match width {
        78.. => "Enter queues · Ctrl+Enter steers · Esc recalls queue/cancels · Ctrl+C cancels",
        56.. => "Enter queues · Ctrl+Enter steers · Esc recalls/cancels",
        44.. => "Enter queues · Ctrl+Enter steers · Esc recalls",
        _ => "Ctrl+Enter steers · Esc recalls",
    }
}

fn hint_body_width(width: u16, session_id: Option<&str>) -> u16 {
    let Some(session_id) = session_id.filter(|id| !id.is_empty()) else {
        return width;
    };
    let prefix_width = UnicodeWidthStr::width(session_id).saturating_add(1);
    width.saturating_sub(u16::try_from(prefix_width).unwrap_or(u16::MAX))
}

fn session_hint_line(
    session_id: Option<&str>,
    hint: &str,
    hint_style: Style,
    width: u16,
) -> Line<'static> {
    let width = usize::from(width);
    let Some(session_id) = session_id.filter(|id| !id.is_empty()) else {
        return Line::styled(truncate_to_width(hint, width), hint_style);
    };
    let visible_session_id = truncate_to_width(session_id, width);
    let session_width = UnicodeWidthStr::width(visible_session_id.as_str());
    let mut spans = vec![Span::styled(
        visible_session_id,
        hint_style.add_modifier(Modifier::BOLD),
    )];
    let remaining = width.saturating_sub(session_width);
    if remaining > 0 && !hint.is_empty() {
        spans.push(Span::styled(
            truncate_to_width(&format!(" {hint}"), remaining),
            hint_style,
        ));
    }
    Line::from(spans)
}

fn queued_preview_text(preview: &PendingInputPreview) -> String {
    let Some(first) = preview.queued_inputs.first() else {
        return String::new();
    };
    if preview.queued_inputs.len() == 1 {
        format!("queued: {}", queued_preview_item(first))
    } else {
        format!(
            "queued({}): {}",
            preview.queued_inputs.len(),
            preview
                .queued_inputs
                .iter()
                .map(|input| queued_preview_item(input))
                .collect::<Vec<_>>()
                .join(" | ")
        )
    }
}

fn queued_preview_item(input: &str) -> String {
    let first_line = input.lines().next().unwrap_or_default();
    let mut preview = first_line
        .chars()
        .take(QUEUED_PREVIEW_CHAR_LIMIT)
        .collect::<String>();
    if first_line.chars().count() > QUEUED_PREVIEW_CHAR_LIMIT || input.contains('\n') {
        preview.push_str("...");
    }
    preview
}

#[derive(Debug, Clone, Default)]
struct InputHistory {
    entries: Vec<InputDraft>,
    cursor: Option<usize>,
    saved_draft: Option<InputDraft>,
}

impl InputHistory {
    fn record(&mut self, entry: InputDraft) {
        if entry.expanded_text().trim().is_empty() {
            return;
        }
        self.reset_navigation();
        if self.entries.last().is_some_and(|last| last == &entry) {
            return;
        }
        self.entries.push(entry);
    }

    fn previous(&mut self, current: InputDraft) -> Option<InputDraft> {
        if self.entries.is_empty() {
            return None;
        }

        let next_index = match self.cursor {
            Some(0) => 0,
            Some(index) => index.saturating_sub(1),
            None => {
                self.saved_draft = Some(current);
                self.entries.len().saturating_sub(1)
            }
        };
        self.cursor = Some(next_index);
        self.entries.get(next_index).cloned()
    }

    fn next(&mut self) -> Option<InputDraft> {
        let cursor = self.cursor?;
        let next_index = cursor.saturating_add(1);
        if next_index < self.entries.len() {
            self.cursor = Some(next_index);
            return self.entries.get(next_index).cloned();
        }

        self.cursor = None;
        Some(self.saved_draft.take().unwrap_or_default())
    }

    fn reset_navigation(&mut self) {
        self.cursor = None;
        self.saved_draft = None;
    }
}

pub fn classify_input(raw: &str, catalog: &SlashCommandCatalog) -> InputAction {
    if raw.trim().is_empty() {
        return InputAction::Ignore;
    }
    if let Some(command) = raw.strip_prefix('!') {
        return InputAction::ShellCommand(command.trim().to_string());
    }
    if let Some(entry) = catalog.exact_entry(raw) {
        return match entry.kind {
            SlashCommandEntryKind::Native(action) => match action {
                SlashCommandAction::Compact => InputAction::Compact,
                SlashCommandAction::Copy => InputAction::Copy,
                SlashCommandAction::Exit => InputAction::Exit,
                SlashCommandAction::Help => InputAction::Help,
                SlashCommandAction::Inbox => InputAction::Inbox,
                SlashCommandAction::Mcp => InputAction::Mcp,
                SlashCommandAction::New => InputAction::New,
                SlashCommandAction::Ps => InputAction::Ps,
                SlashCommandAction::Resume => InputAction::Resume,
                SlashCommandAction::Skills => InputAction::Skills,
                SlashCommandAction::Subagents => InputAction::Subagents,
            },
            // skill 命令按普通消息发给模型，由 agent 侧的 skill 机制接管。
            SlashCommandEntryKind::Skill => InputAction::Send(raw.to_string()),
        };
    }
    if catalog.has_leading_skill_invocation(raw) {
        return InputAction::Send(raw.to_string());
    }
    if catalog.has_leading_native_invocation(raw) {
        return InputAction::Send(raw.to_string());
    }
    if is_slash_command_like(raw) {
        return InputAction::Unknown(raw.to_string());
    }
    if super::slash_command::leading_slash_command(raw).is_some() {
        return InputAction::Unknown(raw.to_string());
    }
    InputAction::Send(raw.to_string())
}

fn shell_command_input_is_active(input: &str) -> bool {
    input
        .strip_prefix('!')
        .and_then(|rest| rest.chars().next())
        .is_some_and(|ch| !ch.is_whitespace())
}

pub(super) fn input_accepts_text(status: SessionRuntimeStatus) -> bool {
    matches!(
        status,
        SessionRuntimeStatus::Initializing
            | SessionRuntimeStatus::Open
            | SessionRuntimeStatus::Running
            | SessionRuntimeStatus::SyncingInbox
            | SessionRuntimeStatus::Compacting
            | SessionRuntimeStatus::Resuming
            | SessionRuntimeStatus::Error
    )
}

pub(super) fn is_shift_enter_newline(key: KeyEvent) -> bool {
    key.code == KeyCode::Enter && key.modifiers.contains(KeyModifiers::SHIFT)
}

fn pad_line_to_width(line: &mut Line<'static>, width: usize, style: Style) {
    let current_width = line.width();
    if current_width < width {
        line.push_span(Span::styled(" ".repeat(width - current_width), style));
    }
}

fn normalize_pasted_text(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn expand_pending_pastes(visible_text: String, pending_pastes: &[(String, String)]) -> String {
    let mut expanded = String::new();
    let mut remaining = visible_text.as_str();
    while !remaining.is_empty() {
        if let Some((placeholder, pasted)) = pending_pastes
            .iter()
            .filter(|(placeholder, _)| remaining.starts_with(placeholder))
            .max_by_key(|(placeholder, _)| placeholder.len())
        {
            expanded.push_str(pasted);
            remaining = &remaining[placeholder.len()..];
        } else if let Some(ch) = remaining.chars().next() {
            expanded.push(ch);
            remaining = &remaining[ch.len_utf8()..];
        }
    }
    expanded
}

fn unique_placeholder_for_merge(placeholder: &str, reserved_placeholders: &[String]) -> String {
    let mut suffix = 2usize;
    loop {
        let candidate = suffixed_placeholder(placeholder, suffix);
        if reserved_placeholders
            .iter()
            .all(|used_placeholder| used_placeholder != &candidate)
        {
            return candidate;
        }
        suffix = suffix.saturating_add(1);
    }
}

fn suffixed_placeholder(placeholder: &str, suffix: usize) -> String {
    if let Some(prefix) = placeholder.strip_suffix(']') {
        format!("{prefix} #{suffix}]")
    } else {
        format!("{placeholder} #{suffix}")
    }
}

#[cfg(test)]
mod tests {
    use super::super::at_path_completion::AtPathCandidateKind;
    use super::*;
    use crate::api::SessionAttachment;
    use crate::attachment::{AttachmentKind, NormalizedMedia};

    fn input_color_for(text: &str) -> Option<Color> {
        let mut pane = BottomPane::default();
        pane.push_text(text);
        pane.lines_with_width(
            SessionRuntimeStatus::Open,
            None,
            &PendingInputPreview::default(),
            0,
            None,
            80,
        )
        .first()
        .and_then(|line| line.spans.get(1))
        .and_then(|span| span.style.fg)
    }

    fn clipboard_media(data: &str) -> NormalizedMedia {
        NormalizedMedia {
            media_type: "image/png".into(),
            data: data.into(),
            kind: AttachmentKind::Image,
            source_name: "clipboard image".into(),
        }
    }

    #[test]
    fn clipboard_image_inserts_numbered_placeholder_and_counts() {
        let mut pane = BottomPane::default();
        pane.push_char('看');
        pane.push_clipboard_image(clipboard_media("QUJD"));
        pane.push_clipboard_image(clipboard_media("REVG"));

        assert_eq!(pane.input(), "看 [Image #1] [Image #2]");
        assert_eq!(pane.effective_attachment_count(), 2);

        let draft = pane.take_draft();
        assert_eq!(
            draft.session_attachments(),
            vec![
                SessionAttachment::InlineImage {
                    media_type: "image/png".into(),
                    data: "QUJD".into(),
                },
                SessionAttachment::InlineImage {
                    media_type: "image/png".into(),
                    data: "REVG".into(),
                },
            ]
        );
    }

    #[test]
    fn deleting_placeholder_revokes_clipboard_attachment() {
        let mut pane = BottomPane::default();
        pane.push_clipboard_image(clipboard_media("QUJD"));
        for _ in 0.."[Image #1]".len() {
            pane.pop_char();
        }

        assert_eq!(pane.effective_attachment_count(), 0);
        assert!(pane.take_draft().session_attachments().is_empty());
    }

    #[test]
    fn ordered_history_keeps_async_draft_before_later_plain_submission() {
        let mut pane = BottomPane::default();
        pane.push_text("检查 @slow-directory");
        let first_draft = pane.take_draft();
        pane.push_text("继续检查普通输入");
        let second_draft = pane.take_draft();

        // 模拟 A 的 @path 预检尚未完成时普通 B 已提交；B 必须留在同一个 sequence
        // 队列中，等 A resolve 后再按 A、B 的顺序写历史。
        pane.record_submitted_draft(first_draft);
        pane.record_submitted_draft(second_draft);

        assert_eq!(
            pane.input_history
                .entries
                .iter()
                .map(InputDraft::visible_text)
                .collect::<Vec<_>>(),
            vec!["检查 @slow-directory", "继续检查普通输入"]
        );
        assert!(pane.recall_previous_input());
        assert_eq!(pane.input(), "继续检查普通输入");
        assert!(pane.recall_previous_input());
        assert_eq!(pane.input(), "检查 @slow-directory");
    }

    fn inline_target(name: &str, data: &str) -> PreviewTarget {
        PreviewTarget::InlineImage {
            name: name.into(),
            media_type: "image/png".into(),
            data: data.into(),
        }
    }

    #[test]
    fn at_path_menu_filters_selects_and_enters_directories() {
        let workspace = PathBuf::from("/workspace");
        let mut pane = BottomPane::default();
        pane.set_at_path_completion_config(workspace.clone(), AtPathCompletionLimits::default());
        pane.push_text("请看 @sr");

        let (directory, _) = pane
            .at_path_scan_request()
            .expect("应请求 workspace 根目录");
        assert_eq!(directory, workspace);
        assert!(pane.mark_at_path_scan_started(&directory));
        assert!(pane.apply_at_path_directory_read(
            directory,
            Ok(vec![
                AtPathDirectoryEntry {
                    file_name: std::ffi::OsString::from("src"),
                    kind: AtPathCandidateKind::Directory,
                    protected: false,
                },
                AtPathDirectoryEntry {
                    file_name: std::ffi::OsString::from("README.md"),
                    kind: AtPathCandidateKind::File,
                    protected: false,
                },
            ]),
        ));
        assert!(pane.at_path_menu_visible());
        assert_eq!(pane.at_path_candidates().len(), 1);
        assert!(pane.accept_at_path_completion());
        assert_eq!(
            pane.input(),
            format!("请看 @src{}", std::path::MAIN_SEPARATOR)
        );
        let child_dir = PathBuf::from("/workspace/src");
        assert_eq!(
            pane.at_path_scan_request().map(|(path, _)| path),
            Some(child_dir.clone())
        );
        // 目录补全后菜单保持打开，继续展示下一级
        assert!(pane.mark_at_path_scan_started(&child_dir));
        assert!(pane.apply_at_path_directory_read(
            child_dir,
            Ok(vec![
                AtPathDirectoryEntry {
                    file_name: std::ffi::OsString::from("lib.rs"),
                    kind: AtPathCandidateKind::File,
                    protected: false,
                },
                AtPathDirectoryEntry {
                    file_name: std::ffi::OsString::from("main.rs"),
                    kind: AtPathCandidateKind::File,
                    protected: false,
                },
            ]),
        ));
        assert!(pane.at_path_menu_visible());
        assert_eq!(
            pane.at_path_candidates()
                .into_iter()
                .map(|c| c.raw_path)
                .collect::<Vec<_>>(),
            vec![
                format!("src{}lib.rs", std::path::MAIN_SEPARATOR),
                format!("src{}main.rs", std::path::MAIN_SEPARATOR),
            ]
        );
    }

    #[test]
    fn directory_completion_then_space_finishes_reference_token() {
        let workspace = PathBuf::from("/workspace");
        let mut pane = BottomPane::default();
        pane.set_at_path_completion_config(workspace.clone(), AtPathCompletionLimits::default());
        pane.push_text("@sr");
        let (directory, _) = pane.at_path_scan_request().unwrap();
        assert!(pane.mark_at_path_scan_started(&directory));
        assert!(pane.apply_at_path_directory_read(
            directory,
            Ok(vec![AtPathDirectoryEntry {
                file_name: std::ffi::OsString::from("src"),
                kind: AtPathCandidateKind::Directory,
                protected: false,
            }]),
        ));
        assert!(pane.accept_at_path_completion());
        assert_eq!(pane.input(), format!("@src{}", std::path::MAIN_SEPARATOR));
        // 未输入空格前仍视为活动 token，应继续请求下一级目录扫描
        let child_dir = PathBuf::from("/workspace/src");
        assert_eq!(
            pane.at_path_scan_request().map(|(path, _)| path),
            Some(child_dir.clone())
        );
        assert!(pane.mark_at_path_scan_started(&child_dir));
        assert!(pane.apply_at_path_directory_read(
            child_dir,
            Ok(vec![AtPathDirectoryEntry {
                file_name: std::ffi::OsString::from("lib.rs"),
                kind: AtPathCandidateKind::File,
                protected: false,
            }]),
        ));
        assert!(pane.at_path_menu_visible());

        // 只有空格才结束目录引用
        pane.push_char(' ');
        assert_eq!(pane.input(), format!("@src{} ", std::path::MAIN_SEPARATOR));
        assert!(!pane.at_path_menu_visible());
        assert_eq!(
            scan_at_path_tokens(pane.input())
                .into_iter()
                .filter_map(|token| token.parsed.ok())
                .collect::<Vec<_>>(),
            vec![format!("src{}", std::path::MAIN_SEPARATOR)]
        );
    }

    #[test]
    fn at_path_file_completion_replaces_only_active_token_and_closes_menu() {
        let mut pane = BottomPane::default();
        pane.set_at_path_completion_config(
            PathBuf::from("/workspace"),
            AtPathCompletionLimits::default(),
        );
        pane.push_text("对比 @docs/a 和 @src/li");
        let (directory, _) = pane.at_path_scan_request().unwrap();
        assert_eq!(directory, PathBuf::from("/workspace/src"));
        assert!(pane.mark_at_path_scan_started(&directory));
        assert!(pane.apply_at_path_directory_read(
            directory,
            Ok(vec![AtPathDirectoryEntry {
                file_name: std::ffi::OsString::from("lib.rs"),
                kind: AtPathCandidateKind::File,
                protected: false,
            }]),
        ));
        assert!(pane.accept_at_path_completion());
        assert_eq!(pane.input(), "对比 @docs/a 和 @src/lib.rs");
        assert!(!pane.at_path_menu_visible());
    }

    #[test]
    fn at_path_menu_uses_shared_five_line_window() {
        let mut pane = BottomPane::default();
        pane.set_at_path_completion_config(
            PathBuf::from("/workspace"),
            AtPathCompletionLimits::default(),
        );
        pane.push_text("@");
        let (directory, _) = pane.at_path_scan_request().unwrap();
        assert!(pane.mark_at_path_scan_started(&directory));
        let entries = (0..8)
            .map(|index| AtPathDirectoryEntry {
                file_name: std::ffi::OsString::from(format!("file-{index}.rs")),
                kind: AtPathCandidateKind::File,
                protected: false,
            })
            .collect();
        assert!(pane.apply_at_path_directory_read(directory, Ok(entries)));
        for _ in 0..7 {
            assert!(pane.select_next_at_path_completion());
        }
        let lines = pane.lines_with_width(
            SessionRuntimeStatus::Open,
            None,
            &PendingInputPreview::default(),
            0,
            None,
            80,
        );
        let text = lines
            .iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("file-7.rs"));
        assert!(!text.contains("file-0.rs"));
    }

    #[test]
    fn preview_hit_prefers_token_under_cursor() {
        let mut pane = BottomPane::default();
        for ch in "看 @a.txt 和".chars() {
            pane.push_char(ch);
        }
        pane.push_clipboard_image(clipboard_media("QUJD"));
        // 光标在末尾，紧贴 [Image #1] → 只命中剪贴板图片
        assert_eq!(
            pane.preview_target_at_cursor(),
            PreviewHit::Targets(vec![inline_target("[Image #1]", "QUJD")])
        );
        // 光标移到 @a.txt 内部 → 只命中 @path
        for _ in 0.."和 [Image #1]".chars().count() + 2 {
            pane.move_left();
        }
        assert_eq!(
            pane.preview_target_at_cursor(),
            PreviewHit::Targets(vec![PreviewTarget::AtPath {
                raw_path: "a.txt".into()
            }])
        );
    }

    #[test]
    fn preview_hit_second_clipboard_image_under_cursor_picks_second() {
        // 回归：两张剪贴板图片，光标贴着第二张 → 必须命中第二张
        let mut pane = BottomPane::default();
        pane.push_clipboard_image(clipboard_media("QUJD"));
        pane.push_clipboard_image(clipboard_media("REVG"));
        assert_eq!(pane.input(), "[Image #1] [Image #2]");

        assert_eq!(
            pane.preview_target_at_cursor(),
            PreviewHit::Targets(vec![inline_target("[Image #2]", "REVG")])
        );
        // 光标移进 [Image #1] → 命中第一张
        pane.move_home();
        for _ in 0..3 {
            pane.move_right();
        }
        assert_eq!(
            pane.preview_target_at_cursor(),
            PreviewHit::Targets(vec![inline_target("[Image #1]", "QUJD")])
        );
    }

    #[test]
    fn preview_hit_off_token_cursor_returns_all_attachments_in_order() {
        let mut pane = BottomPane::default();
        assert_eq!(pane.preview_target_at_cursor(), PreviewHit::NoAttachments);

        for ch in "@a.txt 中间文字 @b.pdf".chars() {
            pane.push_char(ch);
        }
        pane.push_clipboard_image(clipboard_media("QUJD"));
        // 光标移到"中间文字"上（不在任何标记上）→ 全部附件按出现顺序
        for _ in 0.." @b.pdf [Image #1]".chars().count() + 2 {
            pane.move_left();
        }
        assert_eq!(
            pane.preview_target_at_cursor(),
            PreviewHit::Targets(vec![
                PreviewTarget::AtPath {
                    raw_path: "a.txt".into()
                },
                PreviewTarget::AtPath {
                    raw_path: "b.pdf".into()
                },
                inline_target("[Image #1]", "QUJD"),
            ])
        );
    }

    #[test]
    fn composer_highlights_at_path_token_as_bold_gray() {
        let mut pane = BottomPane::default();
        for ch in "看 @a.txt 内容".chars() {
            pane.push_char(ch);
        }

        let lines = pane.lines_with_width(
            crate::agent::SessionRuntimeStatus::Open,
            None,
            &PendingInputPreview::default(),
            0,
            None,
            80,
        );
        let at_span = lines[0]
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "@a.txt")
            .expect("@path 应渲染为独立 span");
        assert_eq!(at_span.style.fg, Some(AT_PATH_FG));
        assert!(at_span.style.add_modifier.contains(Modifier::BOLD));
        // 周围普通文本不带高亮
        let plain_span = lines[0]
            .spans
            .iter()
            .find(|span| span.content.as_ref().contains("内容"))
            .expect("普通文本 span 应存在");
        assert_ne!(plain_span.style.fg, Some(AT_PATH_FG));
    }

    fn rendered_input_line(pane: &BottomPane) -> Line<'static> {
        pane.lines_with_width(
            crate::agent::SessionRuntimeStatus::Open,
            None,
            &PendingInputPreview::default(),
            0,
            None,
            80,
        )
        .into_iter()
        .next()
        .expect("输入行应存在")
    }

    #[test]
    fn shell_command_input_uses_code_color() {
        assert_eq!(input_color_for("!echo hi"), Some(CODE_CONTENT_FG));
    }

    #[test]
    fn normal_input_keeps_default_input_color() {
        assert_eq!(input_color_for("hello"), Some(INPUT_FG));
    }

    #[test]
    fn bang_without_immediate_command_keeps_default_input_color() {
        assert_eq!(input_color_for("! echo hi"), Some(INPUT_FG));
        assert_eq!(input_color_for("!"), Some(INPUT_FG));
    }

    #[test]
    fn cursor_inside_attachment_token_underlines_it() {
        let mut pane = BottomPane::default();
        for ch in "看 @a.txt 和 ".chars() {
            pane.push_char(ch);
        }
        pane.push_clipboard_image(clipboard_media("QUJD"));

        // 光标紧贴 [Image #1] 末尾 → 占位符带下划线，@a.txt 不带
        let line = rendered_input_line(&pane);
        let image_span = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "[Image #1]")
            .expect("[Image #1] 应渲染为独立 span");
        assert!(image_span.style.add_modifier.contains(Modifier::UNDERLINED));
        assert_eq!(image_span.style.fg, Some(AT_PATH_FG));
        let at_span = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "@a.txt")
            .expect("@a.txt 应渲染为独立 span");
        assert!(!at_span.style.add_modifier.contains(Modifier::UNDERLINED));

        // 光标移进 @a.txt → 下划线跟着切换
        for _ in 0.."和 [Image #1]".chars().count() + 3 {
            pane.move_left();
        }
        let line = rendered_input_line(&pane);
        let at_span = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "@a.txt")
            .expect("@a.txt 应渲染为独立 span");
        assert!(at_span.style.add_modifier.contains(Modifier::UNDERLINED));
        let image_span = line
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "[Image #1]")
            .expect("[Image #1] 应渲染为独立 span");
        assert!(!image_span.style.add_modifier.contains(Modifier::UNDERLINED));
    }

    #[test]
    fn inline_slash_hint_renders_ghost_and_completes_with_tab() {
        let mut pane = BottomPane::default();
        pane.set_slash_skills([
            ("tui-smoke-test-with-tmux", "tmux 冒烟测试"),
            ("verify", "运行完整验证"),
        ]);
        pane.push_text("你是谁 /tui-sm");

        assert_eq!(
            pane.inline_slash_hint().as_deref(),
            Some("oke-test-with-tmux")
        );
        let lines = pane.lines_with_width(
            SessionRuntimeStatus::Open,
            None,
            &PendingInputPreview::default(),
            0,
            None,
            80,
        );
        let ghost = lines[0]
            .spans
            .iter()
            .find(|span| span.content.as_ref() == "oke-test-with-tmux")
            .expect("Ghost 后缀应渲染为独立 span");
        assert_eq!(ghost.style.fg, Some(PLACEHOLDER_FG));

        assert!(pane.accept_inline_slash_hint());
        assert_eq!(pane.input(), "你是谁 /tui-smoke-test-with-tmux");
        assert_eq!(pane.inline_slash_hint(), None);
    }

    #[test]
    fn inline_slash_hint_appends_to_the_regular_footer() {
        let mut pane = BottomPane::default();
        pane.set_slash_skills([("verify", "运行完整验证")]);
        pane.push_text("检查 /veri");

        let lines = pane.lines_with_width(
            SessionRuntimeStatus::Open,
            None,
            &PendingInputPreview::default(),
            0,
            Some("session_bdb82521"),
            100,
        );
        let footer = lines.last().expect("应渲染输入提示行");
        assert_eq!(
            footer.to_string(),
            "session_bdb82521 type / for commands · Enter sends · Tab completes"
        );
        assert!(footer.spans[0].style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn inline_slash_hint_requires_unique_match_end_cursor_and_mid_input() {
        let mut pane = BottomPane::default();
        pane.set_slash_skills([("verify", "运行完整验证")]);

        // 行首整段 slash 输入归菜单管，不出 ghost
        pane.push_text("/veri");
        assert_eq!(pane.inline_slash_hint(), None);
        pane.clear_input();

        // 原生命令即使唯一匹配也不出 ghost；句中只补全用户 skill。
        pane.push_text("说 /compa");
        assert_eq!(pane.inline_slash_hint(), None);
        pane.clear_input();

        // 唯一匹配出 ghost；光标离开末尾即收起
        pane.push_text("说 /veri");
        assert_eq!(pane.inline_slash_hint().as_deref(), Some("fy"));
        pane.move_left();
        assert_eq!(pane.inline_slash_hint(), None);
        pane.move_right();

        // 已是完整命令 → 无缺失后缀，Tab 不再改动
        pane.push_text("fy");
        assert_eq!(pane.inline_slash_hint(), None);
        assert!(!pane.accept_inline_slash_hint());
    }

    #[test]
    fn at_path_highlight_disabled_renders_single_plain_span() {
        let mut pane = BottomPane::default();
        pane.set_at_path_highlight(false);
        for ch in "看 @a.txt".chars() {
            pane.push_char(ch);
        }

        let lines = pane.lines_with_width(
            crate::agent::SessionRuntimeStatus::Open,
            None,
            &PendingInputPreview::default(),
            0,
            None,
            80,
        );
        assert!(lines[0]
            .spans
            .iter()
            .all(|span| span.content.as_ref() != "@a.txt"));
    }
}
