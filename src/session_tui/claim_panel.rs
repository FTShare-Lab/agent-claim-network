//! `/claim` 本地 claim 浏览与编辑面板。

use std::cell::Cell;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::text::{Line, Span};

use crate::agent::claims::{ClaimDetail, ClaimSummary, TraceDetail, TraceSummary};
use crate::claim::{ClaimId, ClaimStatus, Confidence, TraceId};

use super::composer::ComposerState;
use super::theme::{accent_style, blue_style, muted_style, surface_style};
use super::wrapping::hard_wrap_styled_lines;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ClaimPanelAction {
    None,
    LoadList {
        query: String,
        include_deprecated: bool,
        offset: usize,
    },
    LoadClaim(ClaimId),
    LoadTraces {
        claim_id: ClaimId,
        offset: usize,
    },
    LoadTrace {
        trace_id: TraceId,
        task_offset: usize,
    },
    Save(ClaimPanelSave),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClaimPanelSave {
    pub(super) id: ClaimId,
    pub(super) expected_revision: String,
    pub(super) name: String,
    pub(super) statement: String,
    pub(super) scope: String,
    pub(super) evidence_summary: String,
    pub(super) confidence: Confidence,
    pub(super) status: ClaimStatus,
}

#[derive(Debug, Clone)]
enum ClaimPanelView {
    List,
    Detail(ClaimDetail),
    Traces {
        claim: ClaimDetail,
        rows: Vec<TraceSummary>,
    },
    Trace {
        claim: ClaimDetail,
        trace: TraceDetail,
    },
    Edit(ClaimEditState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EditField {
    Name,
    Statement,
    Scope,
    Evidence,
    Confidence,
    Status,
}

impl EditField {
    const ALL: [Self; 6] = [
        Self::Name,
        Self::Statement,
        Self::Scope,
        Self::Evidence,
        Self::Confidence,
        Self::Status,
    ];
    fn label(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Statement => "statement",
            Self::Scope => "scope",
            Self::Evidence => "evidence_summary",
            Self::Confidence => "confidence",
            Self::Status => "status",
        }
    }
}

#[derive(Debug, Clone)]
struct ClaimEditState {
    original: ClaimDetail,
    values: [String; 6],
    selected: usize,
    composer: ComposerState,
    error: Option<String>,
}

impl ClaimEditState {
    fn new(original: ClaimDetail) -> Self {
        let claim = &original.claim;
        let values = [
            claim.name.clone(),
            claim.statement.clone(),
            claim.scope.clone(),
            claim.evidence_summary.clone(),
            confidence_label(claim.confidence).into(),
            status_label(claim.status).into(),
        ];
        let mut composer = ComposerState::default();
        composer.set_text(values[0].clone());
        Self {
            original,
            values,
            selected: 0,
            composer,
            error: None,
        }
    }

    fn switch(&mut self, delta: isize) {
        self.values[self.selected] = self.composer.input().to_string();
        self.selected = if delta < 0 {
            self.selected
                .checked_sub(1)
                .unwrap_or(EditField::ALL.len() - 1)
        } else {
            (self.selected + 1) % EditField::ALL.len()
        };
        self.composer.set_text(self.values[self.selected].clone());
        self.error = None;
    }

    fn save(&mut self) -> Result<ClaimPanelSave, String> {
        self.values[self.selected] = self.composer.input().to_string();
        let confidence = match self.values[4].trim().to_ascii_lowercase().as_str() {
            "high" => Confidence::High,
            "medium" => Confidence::Medium,
            "low" => Confidence::Low,
            _ => return Err("confidence 必须是 high、medium 或 low".into()),
        };
        let status = match self.values[5].trim().to_ascii_lowercase().as_str() {
            "active" => ClaimStatus::Active,
            "stale" => ClaimStatus::Stale,
            "deprecated" => ClaimStatus::Deprecated,
            _ => return Err("status 必须是 active、stale 或 deprecated".into()),
        };
        Ok(ClaimPanelSave {
            id: self.original.claim.id.clone(),
            expected_revision: self.original.revision.clone(),
            name: self.values[0].clone(),
            statement: self.values[1].clone(),
            scope: self.values[2].clone(),
            evidence_summary: self.values[3].clone(),
            confidence,
            status,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct ClaimPanelState {
    visible: bool,
    loading: bool,
    rows: Vec<ClaimSummary>,
    selected: usize,
    trace_selected: usize,
    scroll: Cell<usize>,
    view: ClaimPanelView,
    notice: Option<String>,
    query: String,
    include_deprecated: bool,
    search: Option<ComposerState>,
    claim_next_offset: Option<usize>,
    trace_next_offset: Option<usize>,
    claim_omitted: usize,
    trace_omitted: usize,
}

impl Default for ClaimPanelState {
    fn default() -> Self {
        Self {
            visible: false,
            loading: false,
            rows: Vec::new(),
            selected: 0,
            trace_selected: 0,
            scroll: Cell::new(0),
            view: ClaimPanelView::List,
            notice: None,
            query: String::new(),
            include_deprecated: false,
            search: None,
            claim_next_offset: None,
            trace_next_offset: None,
            claim_omitted: 0,
            trace_omitted: 0,
        }
    }
}

impl ClaimPanelState {
    pub(super) fn handle_paste(&mut self, pasted: &str) -> bool {
        if self.loading {
            return false;
        }
        if let Some(search) = &mut self.search {
            search.push_text(pasted);
            return true;
        }
        if let ClaimPanelView::Edit(edit) = &mut self.view {
            edit.composer.push_text(pasted);
            return true;
        }
        false
    }
    pub(super) fn visible(&self) -> bool {
        self.visible
    }
    pub(super) fn open(&mut self) {
        *self = Self::default();
        self.visible = true;
        self.loading = true;
    }
    pub(super) fn close(&mut self) {
        self.visible = false;
        self.loading = false;
        self.view = ClaimPanelView::List;
        self.notice = None;
    }
    pub(super) fn set_claim_page(&mut self, page: crate::agent::claims::ClaimListPage) {
        if page.offset == 0 {
            self.rows = page.items;
        } else {
            self.rows.extend(page.items);
        }
        self.claim_next_offset = page.next_offset;
        self.claim_omitted = page.omitted;
        self.selected = self.selected.min(self.rows.len().saturating_sub(1));
        self.loading = false;
        self.notice = None;
    }
    pub(super) fn set_claim(&mut self, claim: ClaimDetail) {
        self.loading = false;
        self.scroll.set(0);
        self.view = ClaimPanelView::Detail(claim);
        self.notice = None;
    }
    pub(super) fn set_trace_page(&mut self, page: crate::agent::claims::TraceListPage) {
        if page.offset == 0 {
            if let ClaimPanelView::Detail(claim) = &self.view {
                self.view = ClaimPanelView::Traces {
                    claim: claim.clone(),
                    rows: page.items,
                };
            }
            self.trace_selected = 0;
        } else if let ClaimPanelView::Traces { rows, .. } = &mut self.view {
            rows.extend(page.items);
        }
        self.trace_next_offset = page.next_offset;
        self.trace_omitted = page.omitted;
        self.loading = false;
        self.scroll.set(0);
        self.notice = None;
    }
    pub(super) fn set_trace(&mut self, trace: TraceDetail) {
        if trace.task_offset > 0 {
            if let ClaimPanelView::Trace { trace: current, .. } = &mut self.view {
                current.task.push_str(&trace.task);
                current.task_omitted = trace.task_omitted;
                current.next_task_offset = trace.next_task_offset;
            }
        } else if let ClaimPanelView::Traces { claim, .. } = &self.view {
            self.view = ClaimPanelView::Trace {
                claim: claim.clone(),
                trace,
            };
        }
        self.loading = false;
        self.scroll.set(0);
        self.notice = None;
    }
    pub(super) fn fail(&mut self, message: impl Into<String>) {
        self.loading = false;
        self.notice = Some(message.into());
    }
    pub(super) fn finish_save(&mut self, claim: ClaimDetail, notice: Option<String>) {
        self.loading = false;
        self.view = ClaimPanelView::Detail(claim);
        self.notice = notice.or_else(|| Some("Claim 已保存。".into()));
    }

    pub(super) fn handle_key(&mut self, key: KeyEvent) -> ClaimPanelAction {
        if self.loading {
            if key.code == KeyCode::Esc
                || (matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
                    && key.modifiers.contains(KeyModifiers::CONTROL))
            {
                self.close();
            }
            return ClaimPanelAction::None;
        }
        if let Some(search) = &mut self.search {
            match key.code {
                KeyCode::Esc => self.search = None,
                KeyCode::Enter => {
                    self.query = search.input().trim().to_string();
                    self.search = None;
                    self.loading = true;
                    return ClaimPanelAction::LoadList {
                        query: self.query.clone(),
                        include_deprecated: self.include_deprecated,
                        offset: 0,
                    };
                }
                KeyCode::Char(c)
                    if key.modifiers == KeyModifiers::NONE
                        || key.modifiers == KeyModifiers::SHIFT =>
                {
                    search.push_char(c)
                }
                KeyCode::Backspace => search.pop_char(),
                KeyCode::Delete => search.delete_char(),
                KeyCode::Left => search.move_left(),
                KeyCode::Right => search.move_right(),
                KeyCode::Home => search.move_home(),
                KeyCode::End => search.move_end(),
                _ => {}
            }
            return ClaimPanelAction::None;
        }
        match &mut self.view {
            ClaimPanelView::List => match key.code {
                KeyCode::Esc => self.close(),
                KeyCode::Char('/') => {
                    let mut composer = ComposerState::default();
                    composer.set_text(self.query.clone());
                    self.search = Some(composer);
                }
                KeyCode::Char('d') if key.modifiers == KeyModifiers::NONE => {
                    self.include_deprecated = !self.include_deprecated;
                    self.loading = true;
                    return ClaimPanelAction::LoadList {
                        query: self.query.clone(),
                        include_deprecated: self.include_deprecated,
                        offset: 0,
                    };
                }
                KeyCode::Char('n') if key.modifiers == KeyModifiers::NONE => {
                    if let Some(offset) = self.claim_next_offset {
                        self.loading = true;
                        return ClaimPanelAction::LoadList {
                            query: self.query.clone(),
                            include_deprecated: self.include_deprecated,
                            offset,
                        };
                    }
                }
                KeyCode::Up => self.selected = self.selected.saturating_sub(1),
                KeyCode::Down => {
                    self.selected = (self.selected + 1).min(self.rows.len().saturating_sub(1))
                }
                KeyCode::Enter => {
                    if let Some(row) = self.rows.get(self.selected) {
                        self.loading = true;
                        return ClaimPanelAction::LoadClaim(row.id.clone());
                    }
                }
                _ => {}
            },
            ClaimPanelView::Detail(claim) => match key.code {
                KeyCode::Esc => {
                    self.view = ClaimPanelView::List;
                    self.scroll.set(0);
                }
                KeyCode::Char('e') if key.modifiers == KeyModifiers::NONE => {
                    self.view = ClaimPanelView::Edit(ClaimEditState::new(claim.clone()))
                }
                KeyCode::Char('t') if key.modifiers == KeyModifiers::NONE => {
                    let claim_id = claim.claim.id.clone();
                    self.loading = true;
                    return ClaimPanelAction::LoadTraces {
                        claim_id,
                        offset: 0,
                    };
                }
                _ => scroll_key(&self.scroll, key),
            },
            ClaimPanelView::Traces { claim, rows } => match key.code {
                KeyCode::Esc => self.view = ClaimPanelView::Detail(claim.clone()),
                KeyCode::Up => self.trace_selected = self.trace_selected.saturating_sub(1),
                KeyCode::Down => {
                    self.trace_selected =
                        (self.trace_selected + 1).min(rows.len().saturating_sub(1))
                }
                KeyCode::Enter => {
                    if let Some(trace) = rows.get(self.trace_selected) {
                        self.loading = true;
                        return ClaimPanelAction::LoadTrace {
                            trace_id: trace.id.clone(),
                            task_offset: 0,
                        };
                    }
                }
                KeyCode::Char('n') if key.modifiers == KeyModifiers::NONE => {
                    if let Some(offset) = self.trace_next_offset {
                        let claim_id = claim.claim.id.clone();
                        self.loading = true;
                        return ClaimPanelAction::LoadTraces { claim_id, offset };
                    }
                }
                _ => {}
            },
            ClaimPanelView::Trace { claim, trace } => match key.code {
                KeyCode::Esc => {
                    let claim = claim.clone();
                    self.view = ClaimPanelView::Detail(claim);
                }
                KeyCode::Char('n') if key.modifiers == KeyModifiers::NONE => {
                    if let Some(task_offset) = trace.next_task_offset {
                        let trace_id = trace.id.clone();
                        self.loading = true;
                        return ClaimPanelAction::LoadTrace {
                            trace_id,
                            task_offset,
                        };
                    }
                }
                _ => scroll_key(&self.scroll, key),
            },
            ClaimPanelView::Edit(edit) => {
                if key.modifiers.contains(KeyModifiers::CONTROL)
                    && matches!(key.code, KeyCode::Char('s') | KeyCode::Char('S'))
                {
                    match edit.save() {
                        Ok(save) => {
                            edit.error = None;
                            self.notice = None;
                            self.loading = true;
                            return ClaimPanelAction::Save(save);
                        }
                        Err(error) => edit.error = Some(error),
                    }
                } else {
                    match key.code {
                        KeyCode::Esc => self.view = ClaimPanelView::Detail(edit.original.clone()),
                        KeyCode::Tab => edit.switch(1),
                        KeyCode::BackTab => edit.switch(-1),
                        KeyCode::Char(c)
                            if key.modifiers == KeyModifiers::NONE
                                || key.modifiers == KeyModifiers::SHIFT =>
                        {
                            edit.composer.push_char(c)
                        }
                        KeyCode::Backspace => edit.composer.pop_char(),
                        KeyCode::Delete => edit.composer.delete_char(),
                        KeyCode::Left => edit.composer.move_left(),
                        KeyCode::Right => edit.composer.move_right(),
                        KeyCode::Home => edit.composer.move_home(),
                        KeyCode::End => edit.composer.move_end(),
                        KeyCode::Enter => edit.composer.push_newline(),
                        _ => {}
                    }
                }
            }
        }
        ClaimPanelAction::None
    }

    pub(super) fn render_lines(&self, width: u16, height: u16) -> Vec<Line<'static>> {
        let mut lines = vec![Line::from(vec![
            Span::styled(" Claims ", accent_style()),
            Span::styled(
                match &self.view {
                    ClaimPanelView::List => "local list",
                    ClaimPanelView::Detail(_) => "detail",
                    ClaimPanelView::Traces { .. } => "related traces",
                    ClaimPanelView::Trace { .. } => "trace task",
                    ClaimPanelView::Edit(_) => "edit",
                },
                muted_style(),
            ),
        ])];
        if let Some(search) = &self.search {
            let mut text = search.input().to_string();
            text.insert(search.cursor_byte_index(), '▏');
            lines.push(Line::from(vec![
                Span::styled("Search: ", blue_style()),
                Span::raw(text),
                Span::styled("  Enter apply · Esc cancel", muted_style()),
            ]));
        }
        if self.loading {
            lines.push(Line::styled(
                "Loading... · Esc/Ctrl+C close (save continues in background)",
                muted_style(),
            ));
        }
        match &self.view {
            ClaimPanelView::List => {
                lines.push(Line::styled(
                    format!(
                        "filter: {} · deprecated: {}",
                        if self.query.is_empty() {
                            "(none)"
                        } else {
                            &self.query
                        },
                        if self.include_deprecated {
                            "shown"
                        } else {
                            "hidden"
                        }
                    ),
                    muted_style(),
                ));
                if self.rows.is_empty() && !self.loading {
                    lines.push(Line::styled("No local claims.", muted_style()));
                }
                for (index, row) in self.rows.iter().enumerate() {
                    lines.push(Line::from(vec![
                        Span::styled(
                            if index == self.selected { "> " } else { "  " },
                            if index == self.selected {
                                blue_style()
                            } else {
                                muted_style()
                            },
                        ),
                        Span::styled(
                            truncate_chars(
                                &row.name.text,
                                usize::from(width).saturating_sub(24).max(8),
                            ),
                            surface_style(),
                        ),
                        Span::styled(
                            format!(
                                "  {}  {}",
                                confidence_label(row.confidence),
                                status_label(row.status)
                            ),
                            muted_style(),
                        ),
                    ]));
                }
                lines.push(Line::styled(
                    format!("/ search · d toggle deprecated · n next page ({} remaining) · ↑↓ select · Enter details · Esc close", self.claim_omitted),
                    muted_style(),
                ));
            }
            ClaimPanelView::Detail(row) => {
                render_claim(&mut lines, row);
                lines.push(Line::styled(
                    "e edit · t related traces · ↑↓/Pg scroll · Esc back",
                    muted_style(),
                ));
            }
            ClaimPanelView::Traces { rows, .. } => {
                if rows.is_empty() {
                    lines.push(Line::styled("No related traces.", muted_style()));
                }
                for (index, trace) in rows.iter().enumerate() {
                    lines.push(Line::from(vec![
                        Span::styled(
                            if index == self.trace_selected {
                                "> "
                            } else {
                                "  "
                            },
                            if index == self.trace_selected {
                                blue_style()
                            } else {
                                muted_style()
                            },
                        ),
                        Span::raw(truncate_chars(
                            &trace.name,
                            usize::from(width).saturating_sub(28).max(8),
                        )),
                        Span::styled(format!("  {}", trace.created_at), muted_style()),
                    ]));
                }
                lines.push(Line::styled(
                    format!(
                        "↑↓ select · Enter task · n next page ({} remaining) · Esc claim",
                        self.trace_omitted
                    ),
                    muted_style(),
                ));
            }
            ClaimPanelView::Trace { trace, .. } => {
                field(&mut lines, "id", trace.id.to_string());
                field(&mut lines, "name", trace.name.clone());
                field(&mut lines, "created_at", trace.created_at.to_string());
                field(&mut lines, "task", trace.task.clone());
                field(
                    &mut lines,
                    "input_claims",
                    trace
                        .input_claims
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                field(
                    &mut lines,
                    "output_claims",
                    trace
                        .output_claims
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                lines.push(Line::styled(
                    "Trace 是历史任务记录，不代表修改后的验证证据。 · n next page · Esc back",
                    muted_style(),
                ));
            }
            ClaimPanelView::Edit(edit) => {
                field(
                    &mut lines,
                    "id (read-only)",
                    edit.original.claim.id.to_string(),
                );
                for (index, field_name) in EditField::ALL.iter().enumerate() {
                    let value = if index == edit.selected {
                        edit.composer.input()
                    } else {
                        &edit.values[index]
                    };
                    let mut shown = value.to_string();
                    if index == edit.selected {
                        shown.insert(edit.composer.cursor_byte_index(), '▏');
                    }
                    edit_field(
                        &mut lines,
                        field_name.label(),
                        &shown,
                        index == edit.selected,
                    );
                }
                lines.push(Line::styled(
                    "Tab/Shift+Tab field · Ctrl+S save · Esc cancel",
                    muted_style(),
                ));
            }
        }
        let wrapped = hard_wrap_styled_lines(lines, usize::from(width.max(1)));
        let max = usize::from(height.max(1));
        let fixed_notice = match &self.view {
            ClaimPanelView::Edit(edit) => edit.error.as_ref().map(|error| {
                Line::styled(
                    error.clone(),
                    ratatui::style::Style::default().fg(ratatui::style::Color::Red),
                )
            }),
            _ => None,
        }
        .or_else(|| {
            self.notice
                .as_ref()
                .map(|notice| Line::styled(notice.clone(), muted_style()))
        });
        let footer = fixed_notice
            .map(|line| hard_wrap_styled_lines(vec![line], usize::from(width.max(1))))
            .unwrap_or_default();
        let body_max = max.saturating_sub(footer.len().min(max));
        let requested = match &self.view {
            ClaimPanelView::List => self.selected.saturating_sub(body_max.saturating_sub(5)),
            ClaimPanelView::Traces { .. } => self
                .trace_selected
                .saturating_sub(body_max.saturating_sub(4)),
            ClaimPanelView::Edit(_) => wrapped
                .iter()
                .position(|line| line.to_string().contains('▏'))
                .unwrap_or(0)
                .saturating_sub(body_max / 2),
            _ => self.scroll.get(),
        };
        let offset = requested.min(wrapped.len().saturating_sub(body_max));
        self.scroll.set(offset);
        let mut visible = wrapped
            .into_iter()
            .skip(offset)
            .take(body_max)
            .collect::<Vec<_>>();
        let footer_room = max.saturating_sub(visible.len());
        visible.extend(footer.into_iter().take(footer_room));
        visible
    }
}

fn render_claim(lines: &mut Vec<Line<'static>>, row: &ClaimDetail) {
    let c = &row.claim;
    field(lines, "id", c.id.to_string());
    field(lines, "revision", row.revision.clone());
    field(lines, "name", c.name.clone());
    field(lines, "statement", c.statement.clone());
    field(lines, "scope", c.scope.clone());
    field(lines, "evidence_summary", c.evidence_summary.clone());
    field(lines, "confidence", confidence_label(c.confidence).into());
    field(lines, "status", status_label(c.status).into());
    field(lines, "holder", c.holder.to_string());
    field(
        lines,
        "source_claim_ids",
        c.source_claim_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
    );
    field(lines, "created_at", c.created_at.to_string());
}
fn field(lines: &mut Vec<Line<'static>>, label: &str, value: String) {
    let mut parts = value.split('\n');
    lines.push(Line::from(vec![
        Span::styled(format!("{label}: "), muted_style()),
        Span::raw(parts.next().unwrap_or_default().to_string()),
    ]));
    lines.extend(parts.map(|part| {
        Line::from(vec![
            Span::styled("  ", muted_style()),
            Span::raw(part.to_string()),
        ])
    }));
}
fn edit_field(lines: &mut Vec<Line<'static>>, label: &str, value: &str, selected: bool) {
    let style = if selected {
        blue_style()
    } else {
        muted_style()
    };
    let mut parts = value.split('\n');
    lines.push(Line::from(vec![
        Span::styled(if selected { "> " } else { "  " }, style),
        Span::styled(format!("{label}: "), muted_style()),
        Span::raw(parts.next().unwrap_or_default().to_string()),
    ]));
    lines.extend(parts.map(|part| {
        Line::from(vec![
            Span::styled("    ", muted_style()),
            Span::raw(part.to_string()),
        ])
    }));
}
fn truncate_chars(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let mut text = value
        .chars()
        .take(max.saturating_sub(1))
        .collect::<String>();
    text.push('…');
    text
}
fn confidence_label(value: Confidence) -> &'static str {
    match value {
        Confidence::High => "high",
        Confidence::Medium => "medium",
        Confidence::Low => "low",
    }
}
fn status_label(value: ClaimStatus) -> &'static str {
    match value {
        ClaimStatus::Active => "active",
        ClaimStatus::Stale => "stale",
        ClaimStatus::Deprecated => "deprecated",
    }
}
fn scroll_key(scroll: &Cell<usize>, key: KeyEvent) {
    match key.code {
        KeyCode::Up => scroll.set(scroll.get().saturating_sub(1)),
        KeyCode::Down => scroll.set(scroll.get().saturating_add(1)),
        KeyCode::PageUp => scroll.set(scroll.get().saturating_sub(10)),
        KeyCode::PageDown => scroll.set(scroll.get().saturating_add(10)),
        KeyCode::Home => scroll.set(0),
        KeyCode::End => scroll.set(usize::MAX),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::*;
    use crate::agent::claims::{BoundedText, ClaimListPage};
    use crate::claim::{AgentId, Claim};

    fn detail() -> ClaimDetail {
        ClaimDetail {
            claim: Claim {
                id: ClaimId::random(),
                name: "alpha".into(),
                statement: "statement".into(),
                scope: "scope".into(),
                holder: AgentId::new("agent-a").unwrap(),
                confidence: Confidence::High,
                status: ClaimStatus::Active,
                created_at: Utc::now(),
                updated_at: None,
                source_claim_ids: Vec::new(),
                evidence_summary: "evidence".into(),
            },
            revision: "rev-1".into(),
        }
    }

    #[test]
    fn list_opens_detail_then_trace_request() {
        let detail = detail();
        let mut panel = ClaimPanelState::default();
        panel.open();
        panel.set_claim_page(ClaimListPage {
            items: vec![ClaimSummary {
                id: detail.claim.id.clone(),
                name: BoundedText {
                    text: detail.claim.name.clone(),
                    truncated: false,
                },
                scope: BoundedText {
                    text: detail.claim.scope.clone(),
                    truncated: false,
                },
                confidence: detail.claim.confidence,
                status: detail.claim.status,
                updated_at: detail.claim.created_at,
            }],
            offset: 0,
            limit: 20,
            omitted: 0,
            next_offset: None,
        });
        assert!(matches!(
            panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            ClaimPanelAction::LoadClaim(_)
        ));
        panel.set_claim(detail.clone());
        assert_eq!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE)),
            ClaimPanelAction::LoadTraces {
                claim_id: detail.claim.id,
                offset: 0
            }
        );
    }

    #[test]
    fn edit_cancel_and_failed_save_keep_expected_state() {
        let mut panel = ClaimPanelState::default();
        panel.open();
        panel.set_claim(detail());
        panel.handle_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        let action = panel.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL));
        assert!(matches!(action, ClaimPanelAction::Save(_)));
        panel.fail("revision conflict");
        assert!(matches!(panel.view, ClaimPanelView::Edit(_)));
        panel.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(matches!(panel.view, ClaimPanelView::Detail(_)));
    }

    #[test]
    fn loading_escape_closes_panel_so_stale_response_is_not_visible() {
        let mut panel = ClaimPanelState::default();
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(!panel.visible());
    }

    #[test]
    fn multiline_fields_render_as_separate_terminal_lines() {
        let mut lines = Vec::new();
        field(&mut lines, "statement", "first\nsecond".into());
        assert_eq!(lines.len(), 2);
        assert!(lines[0].to_string().contains("first"));
        assert!(lines[1].to_string().contains("second"));
    }

    #[test]
    fn long_edit_keeps_server_failure_notice_visible() {
        let mut detail = detail();
        detail.claim.statement = (0..40)
            .map(|index| format!("row {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut panel = ClaimPanelState::default();
        panel.open();
        panel.set_claim(detail);
        panel.handle_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE));
        panel.fail("revision conflict");
        let text = panel
            .render_lines(60, 8)
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("revision conflict"));
        assert!(text.contains('▏'));
        assert!(text.contains("name:"));
    }

    #[test]
    fn success_notice_stays_fixed_while_home_scrolls_long_detail() {
        let mut detail = detail();
        detail.claim.statement = (0..40)
            .map(|index| format!("row {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut panel = ClaimPanelState::default();
        panel.open();
        panel.finish_save(detail, Some("saved".into()));
        panel.handle_key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
        let bottom = panel.render_lines(60, 8);
        assert!(bottom.iter().any(|line| line.to_string().contains("saved")));
        panel.handle_key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
        let home = panel.render_lines(60, 8);
        let text = home
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("id:"));
        assert!(text.contains("saved"));
    }

    #[test]
    fn corrected_validation_error_does_not_hide_later_save_failure() {
        let mut panel = ClaimPanelState::default();
        panel.open();
        panel.set_claim(detail());
        panel.handle_key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE));
        for _ in 0..4 {
            panel.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
        }
        panel.handle_key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));
        assert!(matches!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL)),
            ClaimPanelAction::None
        ));
        panel.handle_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        assert!(matches!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('s'), KeyModifiers::CONTROL)),
            ClaimPanelAction::Save(_)
        ));
        panel.fail("revision conflict");
        let text = panel
            .render_lines(60, 8)
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(text.contains("revision conflict"));
        assert!(!text.contains("confidence 必须"));
        assert!(matches!(panel.view, ClaimPanelView::Edit(_)));
    }

    #[test]
    fn reopening_resets_filter_and_pagination_state() {
        let mut panel = ClaimPanelState::default();
        panel.query = "old".into();
        panel.include_deprecated = true;
        panel.claim_next_offset = Some(20);
        panel.close();
        panel.open();
        assert!(panel.query.is_empty());
        assert!(!panel.include_deprecated);
        assert_eq!(panel.claim_next_offset, None);
    }
}
