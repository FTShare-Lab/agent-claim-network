//! MCP 诊断面板状态与渲染。
//!
//! `/mcp` 是不写 transcript 的 live 诊断面板，可在 turn 运行期间打开；这里只维护面板导航、
//! server/tool 快照展示和 Enable/Disable/Reconnect 的 UI 意图。

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use serde_json::Value;
use unicode_width::UnicodeWidthStr;

use crate::mcp::config::McpTransportKind;
use crate::mcp::connection_manager::{
    McpRuntimeState, McpServerSnapshot, McpServerStatus, McpToolExposure, McpToolFilterReason,
    McpToolSnapshot, McpToolUnsupportedReason,
};
use crate::mcp::redact::redact_mcp_sensitive_text;
use crate::mcp::tool::tool_catalog;

use super::runtime::McpOperationOutcome;
use super::theme::{accent_style, blue_style, muted_style, surface_style};
use super::wrapping::hard_wrap_styled_lines;

const TOOL_NAME_COL: usize = 28;
const TOOL_STATUS_COL: usize = 13;
const TOOL_FULL_NAME_COL: usize = 42;
const DETAIL_LABEL_COL: usize = 18;
const TOOL_LIST_MIN_SUMMARY_COL: usize = 12;
const SERVER_INDEX_COL: usize = 3;
const SERVER_NAME_COL: usize = 28;
const SERVER_STATUS_COL: usize = 13;
const SERVER_TRANSPORT_COL: usize = 17;
const SERVER_TOOLS_COL: usize = 9;

#[derive(Debug, Clone, Copy)]
struct ToolTableLayout {
    name_col: usize,
    status_col: usize,
    full_name_col: usize,
    summary_col: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum McpPanelRequest {
    Reconnect { server_name: String },
    SetEnabled { server_name: String, enabled: bool },
}

impl McpPanelRequest {
    pub(super) fn server_name(&self) -> &str {
        match self {
            Self::Reconnect { server_name } | Self::SetEnabled { server_name, .. } => server_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum McpPanelKeyAction {
    None,
    Request(McpPanelRequest),
}

#[derive(Debug, Clone)]
pub(super) struct McpPanelState {
    visible: bool,
    config_path: Option<PathBuf>,
    snapshot: McpRuntimeState,
    view: McpPanelView,
    selected_server: usize,
    selected_tool: usize,
    server_list_offset: Cell<usize>,
    tool_list_offset: Cell<usize>,
    detail_scroll: usize,
    busy_servers: BTreeMap<String, u64>,
    next_operation_id: u64,
    notice: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpPanelView {
    ServerList,
    ServerDetail,
    ToolList,
    ToolDetail,
}

impl Default for McpPanelState {
    fn default() -> Self {
        Self {
            visible: false,
            config_path: None,
            snapshot: McpRuntimeState::default(),
            view: McpPanelView::ServerList,
            selected_server: 0,
            selected_tool: 0,
            server_list_offset: Cell::new(0),
            tool_list_offset: Cell::new(0),
            detail_scroll: 0,
            busy_servers: BTreeMap::new(),
            next_operation_id: 0,
            notice: None,
        }
    }
}

impl McpPanelState {
    pub(super) fn set_runtime(&mut self, config_path: PathBuf, snapshot: McpRuntimeState) {
        self.config_path = Some(config_path);
        self.snapshot = snapshot;
        self.clamp_selection();
    }

    pub(super) fn open(&mut self) {
        self.visible = true;
        self.view = McpPanelView::ServerList;
        self.detail_scroll = 0;
        self.notice = None;
        self.clamp_selection();
    }

    pub(super) fn close(&mut self) {
        self.visible = false;
        self.notice = None;
    }

    pub(super) fn visible(&self) -> bool {
        self.visible
    }

    pub(super) fn set_notice(&mut self, notice: impl Into<String>) {
        self.notice = Some(notice.into());
    }

    pub(super) fn begin_request(&mut self, request: &McpPanelRequest) -> u64 {
        let server_name = request.server_name().to_string();
        self.next_operation_id = self.next_operation_id.saturating_add(1);
        let operation_id = self.next_operation_id;
        self.busy_servers.insert(server_name.clone(), operation_id);
        match request {
            McpPanelRequest::Reconnect { .. } => {
                self.set_server_status(&server_name, McpServerStatus::Reconnecting);
                self.notice = Some(format!("Reconnect started for {server_name}"));
            }
            McpPanelRequest::SetEnabled { enabled: true, .. } => {
                self.set_server_status(&server_name, McpServerStatus::Reconnecting);
                self.notice = Some(format!("Enable started for {server_name}"));
            }
            McpPanelRequest::SetEnabled { enabled: false, .. } => {
                self.set_server_status(&server_name, McpServerStatus::Disabled);
                self.notice = Some(format!("Disable requested for {server_name}"));
            }
        }
        operation_id
    }

    pub(super) fn finish_request(
        &mut self,
        server_name: &str,
        operation_id: u64,
        outcome: McpOperationOutcome,
    ) -> bool {
        if self.busy_servers.get(server_name) != Some(&operation_id) {
            return false;
        }
        self.busy_servers.remove(server_name);
        self.snapshot = outcome.snapshot;
        match outcome.error {
            Some(error) => {
                self.notice = Some(format!(
                    "MCP server {server_name} failed: {}",
                    redact_mcp_sensitive_text(&error)
                ));
            }
            None => {
                let failed = self
                    .snapshot
                    .servers
                    .get(server_name)
                    .filter(|server| server.status == McpServerStatus::Failed)
                    .and_then(|server| server.last_error.as_deref());
                if let Some(error) = failed {
                    self.notice = Some(format!(
                        "MCP server {server_name} failed: {}",
                        redact_mcp_sensitive_text(error)
                    ));
                } else {
                    self.notice = Some(format!("MCP server {server_name} updated"));
                }
            }
        }
        self.clamp_selection();
        true
    }

    pub(super) fn handle_key(&mut self, key: KeyEvent) -> McpPanelKeyAction {
        match key.code {
            KeyCode::Esc => {
                match self.view {
                    McpPanelView::ServerList => self.close(),
                    McpPanelView::ServerDetail => self.view = McpPanelView::ServerList,
                    McpPanelView::ToolList => self.view = McpPanelView::ServerDetail,
                    McpPanelView::ToolDetail => self.view = McpPanelView::ToolList,
                }
                self.detail_scroll = 0;
                McpPanelKeyAction::None
            }
            KeyCode::Up => {
                self.move_selection_up();
                McpPanelKeyAction::None
            }
            KeyCode::Down => {
                self.move_selection_down();
                McpPanelKeyAction::None
            }
            KeyCode::Enter => {
                match self.view {
                    McpPanelView::ServerList if self.selected_server().is_some() => {
                        self.view = McpPanelView::ServerDetail;
                    }
                    McpPanelView::ToolList if self.selected_tool().is_some() => {
                        self.view = McpPanelView::ToolDetail;
                    }
                    McpPanelView::ServerDetail
                    | McpPanelView::ToolList
                    | McpPanelView::ToolDetail
                    | McpPanelView::ServerList => {}
                }
                self.detail_scroll = 0;
                McpPanelKeyAction::None
            }
            KeyCode::Char('v') | KeyCode::Char('V') => {
                if matches!(self.view, McpPanelView::ServerDetail) {
                    if self
                        .selected_server()
                        .is_some_and(|server| self.server_tools_are_viewable(server))
                    {
                        self.view = McpPanelView::ToolList;
                        self.selected_tool = 0;
                        self.server_list_offset.set(0);
                        self.tool_list_offset.set(0);
                        self.detail_scroll = 0;
                    } else if let Some(server) = self.selected_server() {
                        self.notice = Some(
                            view_tools_unavailable_notice(
                                server,
                                self.busy_servers.contains_key(&server.name),
                            )
                            .into(),
                        );
                    }
                }
                McpPanelKeyAction::None
            }
            KeyCode::Char('r') | KeyCode::Char('R')
                if matches!(
                    self.view,
                    McpPanelView::ServerList | McpPanelView::ServerDetail
                ) =>
            {
                self.reconnect_action()
            }
            KeyCode::Char('d') | KeyCode::Char('D')
                if matches!(
                    self.view,
                    McpPanelView::ServerList | McpPanelView::ServerDetail
                ) =>
            {
                self.toggle_enabled_action()
            }
            _ => McpPanelKeyAction::None,
        }
    }

    pub(super) fn render_lines(&self, width: u16, height: u16) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        lines.push(fit_spans_to_width(
            vec![
                Span::styled("MCP", accent_style().add_modifier(Modifier::UNDERLINED)),
                Span::styled(
                    " · ",
                    muted_style()
                        .add_modifier(Modifier::BOLD)
                        .add_modifier(Modifier::UNDERLINED),
                ),
                Span::styled(
                    self.view_label(),
                    blue_style().add_modifier(Modifier::UNDERLINED),
                ),
            ],
            width,
        ));
        if let Some(notice) = &self.notice {
            lines.push(Line::styled(truncate_width(notice, width), notice_style()));
        }
        lines.push(Line::default());

        let budget = usize::from(height.max(1));
        let list_viewport = budget.saturating_sub(lines.len().saturating_add(2)).max(1);
        match self.view {
            McpPanelView::ServerList => self.render_server_list(&mut lines, width, list_viewport),
            McpPanelView::ServerDetail => self.render_server_detail(&mut lines, width),
            McpPanelView::ToolList => self.render_tool_list(&mut lines, width, list_viewport),
            McpPanelView::ToolDetail => self.render_tool_detail(&mut lines, width),
        }

        let help_text = self.help_text();
        place_footer_at_bottom(&mut lines, help_line(&help_text, width), budget);
        lines
    }

    fn render_server_list(&self, lines: &mut Vec<Line<'static>>, width: u16, viewport: usize) {
        let names = self.server_names();
        if names.is_empty() {
            lines.push(Line::styled(
                truncate_width("No MCP servers configured.", width),
                muted_style(),
            ));
            return;
        }
        lines.push(server_table_header(width));
        let row_viewport = viewport.saturating_sub(1).max(1);
        self.ensure_server_selected_visible(row_viewport, names.len());
        for (index, name) in names
            .iter()
            .enumerate()
            .skip(self.server_list_offset.get())
            .take(row_viewport)
        {
            let Some(server) = self.snapshot.servers.get(name) else {
                continue;
            };
            let marker = selection_marker(index == self.selected_server);
            let tools = format!(
                "{}/{}",
                server.exposed_tool_count(),
                server.discovered_tool_count()
            );
            let mut spans = vec![
                Span::styled(marker, muted_style()),
                Span::styled(
                    pad_width(&(index.saturating_add(1)).to_string(), SERVER_INDEX_COL),
                    selected_style(index == self.selected_server),
                ),
                Span::styled("  ", muted_style()),
                Span::styled(
                    pad_width(name, SERVER_NAME_COL),
                    selected_style(index == self.selected_server),
                ),
                Span::styled("  ", muted_style()),
                Span::styled(
                    pad_width(server.status.as_str(), SERVER_STATUS_COL),
                    status_style(server.status),
                ),
                Span::styled("  ", muted_style()),
                Span::styled(
                    pad_width(&format_transport(server.transport), SERVER_TRANSPORT_COL),
                    muted_style(),
                ),
                Span::styled("  ", muted_style()),
                Span::styled(pad_width(&tools, SERVER_TOOLS_COL), muted_style()),
            ];
            if self.busy_servers.contains_key(name) {
                spans.push(Span::styled("  updating", notice_style()));
            } else if let Some(error) = &server.last_error {
                let error = redact_mcp_sensitive_text(error);
                spans.push(Span::styled("  ", muted_style()));
                spans.push(Span::styled(
                    truncate_width(&error, width.saturating_div(3)),
                    error_style(),
                ));
            }
            lines.push(fit_spans_to_width(spans, width));
        }
    }

    fn render_server_detail(&self, lines: &mut Vec<Line<'static>>, width: u16) {
        let Some(server) = self.selected_server() else {
            lines.push(Line::styled(
                truncate_width("No server selected.", width),
                muted_style(),
            ));
            return;
        };
        let mut detail = vec![label_value_line("server", &server.name, width)];
        detail.extend(server_install_lines(
            server,
            self.snapshot.workspace_root.as_deref(),
            width,
        ));
        let scroll = self.detail_scroll.min(detail.len().saturating_sub(1));
        lines.extend(detail.into_iter().skip(scroll));
    }

    fn render_tool_list(&self, lines: &mut Vec<Line<'static>>, width: u16, viewport: usize) {
        let Some(server) = self.selected_server() else {
            lines.push(Line::styled(
                truncate_width("No server selected.", width),
                muted_style(),
            ));
            return;
        };
        lines.push(prefix_value_line(
            "Tools for MCP server: ",
            &server.name,
            width,
        ));
        if server.tools.is_empty() {
            lines.push(Line::styled(
                truncate_width("No tools discovered for this server.", width),
                muted_style(),
            ));
            return;
        }
        let layout = tool_table_layout(width);
        lines.push(tool_table_header(layout, width));
        let catalog = tool_catalog(&self.snapshot);
        let row_viewport = viewport.saturating_sub(2).max(1);
        self.ensure_tool_selected_visible(row_viewport, server.tools.len());
        for (index, tool) in server
            .tools
            .iter()
            .enumerate()
            .skip(self.tool_list_offset.get())
            .take(row_viewport)
        {
            let marker = selection_marker(index == self.selected_tool);
            let full_name = catalog
                .visible_name_for(&server.name, &tool.raw_name)
                .unwrap_or("-");
            let summary = tool
                .title
                .as_deref()
                .or(tool.description.as_deref())
                .unwrap_or("");
            lines.push(fit_spans_to_width(
                vec![
                    Span::styled(marker, muted_style()),
                    Span::styled(
                        pad_width(&tool.raw_name, layout.name_col),
                        selected_style(index == self.selected_tool),
                    ),
                ]
                .into_iter()
                .chain(tool_status_spans(&tool.exposure, layout))
                .chain(tool_full_name_spans(full_name, layout))
                .chain(tool_summary_spans(summary, layout))
                .collect::<Vec<_>>(),
                width,
            ));
        }
    }

    fn render_tool_detail(&self, lines: &mut Vec<Line<'static>>, width: u16) {
        let Some(server) = self.selected_server() else {
            lines.push(Line::styled(
                truncate_width("No server selected.", width),
                muted_style(),
            ));
            return;
        };
        let Some(tool) = self.selected_tool() else {
            lines.push(Line::styled(
                truncate_width("No tool selected.", width),
                muted_style(),
            ));
            return;
        };
        let catalog = tool_catalog(&self.snapshot);
        let full_name = catalog
            .visible_name_for(&server.name, &tool.raw_name)
            .unwrap_or("-");
        let mut detail = vec![
            label_value_line("Tool name", &tool.raw_name, width),
            label_value_line("Full name", full_name, width),
            fit_spans_to_width(
                vec![
                    Span::styled(pad_width("Status", DETAIL_LABEL_COL), muted_style()),
                    exposure_span(&tool.exposure),
                    Span::styled(
                        exposure_reason(&tool.exposure)
                            .map(|reason| format!("  {reason}"))
                            .unwrap_or_default(),
                        muted_style(),
                    ),
                ],
                width,
            ),
        ];
        if let Some(title) = &tool.title {
            detail.push(label_value_line("Title", title, width));
        }
        if let Some(description) = &tool.description {
            detail.push(label_value_line("Description", description, width));
        }
        detail.extend(parameter_lines(tool, width));
        detail.extend(json_block_lines(
            "Input schema",
            Value::Object(tool.raw_tool.input_schema.as_ref().clone()),
            width,
        ));
        if let Some(output_schema) = &tool.raw_tool.output_schema {
            detail.extend(json_block_lines(
                "Output schema",
                Value::Object(output_schema.as_ref().clone()),
                width,
            ));
        }
        if let Some(annotations) = &tool.raw_tool.annotations {
            detail.extend(json_block_lines(
                "Annotations",
                serde_json::to_value(annotations).unwrap_or(Value::Null),
                width,
            ));
        }
        let scroll = self.detail_scroll.min(detail.len().saturating_sub(1));
        lines.extend(detail.into_iter().skip(scroll));
    }

    fn reconnect_action(&mut self) -> McpPanelKeyAction {
        let Some(server) = self.selected_server() else {
            return McpPanelKeyAction::None;
        };
        if self.server_is_reconnect_blocked(server) {
            self.notice = Some(
                reconnect_unavailable_notice(server, self.busy_servers.contains_key(&server.name))
                    .into(),
            );
            return McpPanelKeyAction::None;
        }
        McpPanelKeyAction::Request(McpPanelRequest::Reconnect {
            server_name: server.name.clone(),
        })
    }

    fn toggle_enabled_action(&mut self) -> McpPanelKeyAction {
        let Some(server) = self.selected_server() else {
            return McpPanelKeyAction::None;
        };
        if self.busy_servers.contains_key(&server.name)
            && server.status == McpServerStatus::Disabled
        {
            self.notice = Some("Enable is unavailable while this server is updating.".into());
            return McpPanelKeyAction::None;
        }
        let enabled = server.status == McpServerStatus::Disabled;
        McpPanelKeyAction::Request(McpPanelRequest::SetEnabled {
            server_name: server.name.clone(),
            enabled,
        })
    }

    fn move_selection_up(&mut self) {
        match self.view {
            McpPanelView::ServerList => {
                self.selected_server = self.selected_server.saturating_sub(1);
            }
            McpPanelView::ToolList => {
                self.selected_tool = self.selected_tool.saturating_sub(1);
            }
            McpPanelView::ToolDetail => {
                self.detail_scroll = self.detail_scroll.saturating_sub(1);
            }
            McpPanelView::ServerDetail => {
                self.detail_scroll = self.detail_scroll.saturating_sub(1);
            }
        }
    }

    fn move_selection_down(&mut self) {
        match self.view {
            McpPanelView::ServerList => {
                self.selected_server = self
                    .selected_server
                    .saturating_add(1)
                    .min(self.server_names().len().saturating_sub(1));
            }
            McpPanelView::ToolList => {
                let max = self
                    .selected_server()
                    .map(|server| server.tools.len().saturating_sub(1))
                    .unwrap_or(0);
                self.selected_tool = self.selected_tool.saturating_add(1).min(max);
            }
            McpPanelView::ToolDetail => {
                self.detail_scroll = self.detail_scroll.saturating_add(1);
            }
            McpPanelView::ServerDetail => {
                self.detail_scroll = self.detail_scroll.saturating_add(1);
            }
        }
    }

    fn clamp_selection(&mut self) {
        self.selected_server = self
            .selected_server
            .min(self.snapshot.servers.len().saturating_sub(1));
        self.selected_tool = self.selected_tool.min(
            self.selected_server()
                .map_or(0, |server| server.tools.len().saturating_sub(1)),
        );
        self.server_list_offset.set(
            self.server_list_offset
                .get()
                .min(self.snapshot.servers.len().saturating_sub(1)),
        );
        self.tool_list_offset.set(
            self.tool_list_offset.get().min(
                self.selected_server()
                    .map_or(0, |server| server.tools.len().saturating_sub(1)),
            ),
        );
    }

    fn ensure_server_selected_visible(&self, viewport: usize, len: usize) {
        let mut offset = self.server_list_offset.get();
        if self.selected_server < offset {
            offset = self.selected_server;
        } else if self.selected_server >= offset.saturating_add(viewport) {
            offset = self
                .selected_server
                .saturating_add(1)
                .saturating_sub(viewport);
        }
        self.server_list_offset
            .set(offset.min(len.saturating_sub(1)));
    }

    fn ensure_tool_selected_visible(&self, viewport: usize, len: usize) {
        let mut offset = self.tool_list_offset.get();
        if self.selected_tool < offset {
            offset = self.selected_tool;
        } else if self.selected_tool >= offset.saturating_add(viewport) {
            offset = self
                .selected_tool
                .saturating_add(1)
                .saturating_sub(viewport);
        }
        self.tool_list_offset.set(offset.min(len.saturating_sub(1)));
    }

    fn selected_server(&self) -> Option<&McpServerSnapshot> {
        let name = self.server_names().get(self.selected_server)?.clone();
        self.snapshot.servers.get(&name)
    }

    fn selected_tool(&self) -> Option<&McpToolSnapshot> {
        self.selected_server()?.tools.get(self.selected_tool)
    }

    fn server_names(&self) -> Vec<String> {
        self.snapshot.servers.keys().cloned().collect()
    }

    fn set_server_status(&mut self, server_name: &str, status: McpServerStatus) {
        if let Some(server) = self.snapshot.servers.get_mut(server_name) {
            server.status = status;
            if status != McpServerStatus::Ready {
                server.tools.clear();
            }
        }
    }

    fn server_tools_are_viewable(&self, server: &McpServerSnapshot) -> bool {
        !self.busy_servers.contains_key(&server.name) && server.status == McpServerStatus::Ready
    }

    fn server_is_reconnect_blocked(&self, server: &McpServerSnapshot) -> bool {
        self.busy_servers.contains_key(&server.name)
            || matches!(
                server.status,
                McpServerStatus::Disabled
                    | McpServerStatus::Starting
                    | McpServerStatus::Reconnecting
            )
    }

    fn view_label(&self) -> &'static str {
        match self.view {
            McpPanelView::ServerList => "servers",
            McpPanelView::ServerDetail => "server detail",
            McpPanelView::ToolList => "tools",
            McpPanelView::ToolDetail => "tool detail",
        }
    }

    fn help_text(&self) -> String {
        match self.view {
            McpPanelView::ServerList if self.snapshot.servers.is_empty() => "Esc to close".into(),
            McpPanelView::ServerList => {
                "↑/↓ to navigate · Enter to open · r reconnect · d enable/disable · Esc to close"
                    .into()
            }
            McpPanelView::ServerDetail => self.server_detail_help_text(),
            McpPanelView::ToolList => "↑/↓ to navigate · Enter to open · Esc to back".into(),
            McpPanelView::ToolDetail => "↑/↓ to scroll · Esc to back".into(),
        }
    }

    fn server_detail_help_text(&self) -> String {
        let Some(server) = self.selected_server() else {
            return "Esc to back".into();
        };
        let navigation_hint = if server.last_error.is_some() {
            "↑/↓ scroll · "
        } else {
            "↑/↓ to navigate · "
        };
        if self.busy_servers.contains_key(&server.name) {
            return if server.status == McpServerStatus::Disabled {
                format!("{navigation_hint}updating · Esc to back")
            } else {
                format!("{navigation_hint}d disable · Esc to back")
            };
        }
        if self.server_tools_are_viewable(server) {
            return format!(
                "{navigation_hint}v view tools · r reconnect · d disable · Esc to back"
            );
        }
        match server.status {
            McpServerStatus::Disabled => format!("{navigation_hint}d enable · Esc to back"),
            McpServerStatus::Failed => {
                format!("{navigation_hint}r reconnect · d disable · Esc to back")
            }
            McpServerStatus::Starting | McpServerStatus::Reconnecting => {
                format!("{navigation_hint}d disable · Esc to back")
            }
            McpServerStatus::Ready => {
                format!("{navigation_hint}r reconnect · d disable · Esc to back")
            }
        }
    }
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

fn server_install_lines(
    server: &McpServerSnapshot,
    workspace_root: Option<&Path>,
    width: u16,
) -> Vec<Line<'static>> {
    let mut lines = vec![
        fit_spans_to_width(
            vec![
                Span::styled(pad_width("status", DETAIL_LABEL_COL), muted_style()),
                status_span(server.status),
            ],
            width,
        ),
        label_value_line(
            "tools",
            &format!(
                "{} exposed / {} discovered",
                server.exposed_tool_count(),
                server.discovered_tool_count()
            ),
            width,
        ),
        label_value_line("transport", &format_transport(server.transport), width),
        label_value_line(
            "startup timeout",
            &format!("{}s", server.config.startup_timeout_secs()),
            width,
        ),
        label_value_line(
            "tool timeout",
            &format!("{}s", server.config.tool_timeout_secs()),
            width,
        ),
    ];
    match server.transport {
        Some(McpTransportKind::Stdio) => {
            lines.push(label_value_line(
                "command",
                server.config.command.as_deref().unwrap_or("-"),
                width,
            ));
            lines.push(label_value_line(
                "args",
                &redacted_args(server.config.args.as_deref().unwrap_or(&[])),
                width,
            ));
            lines.push(label_value_line(
                "cwd",
                &effective_cwd(&server.config.cwd, workspace_root),
                width,
            ));
            lines.push(label_value_line(
                "env",
                &redacted_keys(server.config.env.as_ref().map(|env| env.keys())),
                width,
            ));
            lines.push(label_value_line(
                "env_vars",
                &string_list(server.config.env_vars.as_deref().unwrap_or(&[])),
                width,
            ));
        }
        Some(McpTransportKind::StreamableHttp) => {
            lines.push(label_value_line(
                "url",
                &redacted_url(server.config.url.as_deref().unwrap_or("-")),
                width,
            ));
            lines.push(label_value_line(
                "bearer token env",
                server.config.bearer_token_env_var.as_deref().unwrap_or("-"),
                width,
            ));
        }
        None => {}
    }
    lines.push(label_value_line(
        "enabled_tools",
        &tool_allowlist_summary(server.config.enabled_tools.as_deref().unwrap_or(&[])),
        width,
    ));
    lines.push(label_value_line(
        "disabled_tools",
        &tool_denylist_summary(server.config.disabled_tools.as_deref().unwrap_or(&[])),
        width,
    ));
    lines.push(label_value_line(
        "last connected",
        &server
            .last_connected_at
            .map(|time| time.to_rfc3339())
            .unwrap_or_else(|| "-".into()),
        width,
    ));
    if let Some(error) = &server.last_error {
        lines.extend(label_value_wrapped_lines(
            "Last error",
            &redact_mcp_sensitive_text(error),
            width,
            error_style(),
        ));
    }
    lines
}

fn parameter_lines(tool: &McpToolSnapshot, width: u16) -> Vec<Line<'static>> {
    let schema = Value::Object(tool.raw_tool.input_schema.as_ref().clone());
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return vec![Line::styled(
            truncate_width("Parameters: none", width),
            muted_style(),
        )];
    };
    if properties.is_empty() {
        return vec![Line::styled(
            truncate_width("Parameters: none", width),
            muted_style(),
        )];
    }
    let mut lines = vec![Line::styled(
        truncate_width("Parameters", width),
        muted_style(),
    )];
    for (name, value) in properties {
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let req = if required.contains(name.as_str()) {
            "required"
        } else {
            "optional"
        };
        let desc = value
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("");
        let default = value
            .get("default")
            .map(short_schema_value)
            .filter(|value| !value.is_empty())
            .map(|value| format!(" default={value}"))
            .unwrap_or_default();
        let enum_values = value
            .get("enum")
            .and_then(Value::as_array)
            .map(|values| short_enum_values(values.as_slice()))
            .filter(|value| !value.is_empty())
            .map(|value| format!(" enum={value}"))
            .unwrap_or_default();
        let meta = format!("{kind} {req}{default}{enum_values}");
        let prefix = format!("  - {name} {meta} ");
        let prefix_width = UnicodeWidthStr::width(prefix.as_str());
        let width_usize = usize::from(width.max(1));
        if prefix_width >= width_usize {
            lines.push(Line::styled(truncate_width(&prefix, width), muted_style()));
        } else {
            lines.push(Line::from(vec![
                Span::styled("  - ", muted_style()),
                Span::styled(name.clone(), blue_style()),
                Span::styled(format!(" {meta} "), muted_style()),
                Span::raw(truncate_width(
                    desc,
                    width.saturating_sub(u16::try_from(prefix_width).unwrap_or(u16::MAX)),
                )),
            ]));
        }
    }
    lines
}

fn short_schema_value(value: &Value) -> String {
    match value {
        Value::String(text) => format!("{text:?}"),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn short_enum_values(values: &[Value]) -> String {
    let joined = values
        .iter()
        .take(6)
        .map(short_schema_value)
        .collect::<Vec<_>>()
        .join("|");
    if values.len() > 6 {
        format!("{joined}|...")
    } else {
        joined
    }
}

fn json_block_lines(label: &str, value: Value, width: u16) -> Vec<Line<'static>> {
    let raw = serde_json::to_string_pretty(&value).unwrap_or_else(|err| format!("<json: {err}>"));
    if width < 8 {
        return raw
            .lines()
            .take(80)
            .map(|line| Line::styled(truncate_width(line, width), muted_style()))
            .collect();
    }
    let block_width = usize::from(width.max(8));
    let inner_width = block_width.saturating_sub(4).max(1);
    let title = truncate_width(
        &format!(" {label} "),
        u16::try_from(block_width.saturating_sub(3)).unwrap_or(u16::MAX),
    );
    let top_fill =
        "─".repeat(block_width.saturating_sub(3 + UnicodeWidthStr::width(title.as_str())));
    let mut lines = vec![Line::from(vec![
        Span::styled("╭─", muted_style()),
        Span::styled(title, blue_style()),
        Span::styled(format!("{top_fill}╮"), muted_style()),
    ])];
    lines.extend(raw.lines().take(80).map(|line| {
        let content = pad_width(
            &truncate_width(line, u16::try_from(inner_width).unwrap_or(u16::MAX)),
            inner_width,
        );
        Line::from(vec![
            Span::styled("│ ", muted_style()),
            Span::styled(content, muted_style()),
            Span::styled(" │", muted_style()),
        ])
    }));
    let bottom_fill = "─".repeat(block_width.saturating_sub(2));
    lines.push(Line::styled(format!("╰{bottom_fill}╯"), muted_style()));
    lines
}

fn label_value_line(label: &str, value: &str, width: u16) -> Line<'static> {
    let width_usize = usize::from(width.max(1));
    if width_usize <= DETAIL_LABEL_COL {
        return Line::styled(
            truncate_width(&format!("{label} {value}"), width),
            muted_style(),
        );
    }
    let label_text = pad_width(label, DETAIL_LABEL_COL);
    let label_width = u16::try_from(DETAIL_LABEL_COL).unwrap_or(u16::MAX);
    Line::from(vec![
        Span::styled(label_text, muted_style()),
        Span::raw(truncate_width(value, width.saturating_sub(label_width))),
    ])
}

fn label_value_wrapped_lines(
    label: &str,
    value: &str,
    width: u16,
    value_style: Style,
) -> Vec<Line<'static>> {
    let width_usize = usize::from(width.max(1));
    if width_usize <= DETAIL_LABEL_COL {
        return hard_wrap_styled_lines(
            vec![Line::styled(format!("{label} {value}"), value_style)],
            width_usize,
        );
    }

    let value_width = width_usize.saturating_sub(DETAIL_LABEL_COL).max(1);
    let value_lines = wrap_text_prefer_words(value, value_width);
    if value_lines.is_empty() {
        return vec![label_value_line(label, "", width)];
    }

    let label_text = pad_width(label, DETAIL_LABEL_COL);
    value_lines
        .into_iter()
        .enumerate()
        .map(|(index, value_line)| {
            let label_span = if index == 0 {
                Span::styled(label_text.clone(), muted_style())
            } else {
                Span::styled(" ".repeat(DETAIL_LABEL_COL), muted_style())
            };
            let spans = vec![label_span, Span::styled(value_line, value_style)];
            fit_spans_to_width(spans, width)
        })
        .collect()
}

fn wrap_text_prefer_words(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    for logical in text.split('\n') {
        let mut current = String::new();
        for word in logical.split_whitespace() {
            let word_width = UnicodeWidthStr::width(word);
            if current.is_empty() {
                push_word_or_chunks(word, width, &mut lines, &mut current);
                continue;
            }
            let current_width = UnicodeWidthStr::width(current.as_str());
            if current_width.saturating_add(1).saturating_add(word_width) <= width {
                current.push(' ');
                current.push_str(word);
            } else {
                lines.push(std::mem::take(&mut current));
                push_word_or_chunks(word, width, &mut lines, &mut current);
            }
        }
        lines.push(std::mem::take(&mut current));
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

fn push_word_or_chunks(word: &str, width: usize, lines: &mut Vec<String>, current: &mut String) {
    if UnicodeWidthStr::width(word) <= width {
        current.push_str(word);
        return;
    }
    let mut chunks = hard_wrap_styled_lines(vec![Line::raw(word.to_string())], width)
        .into_iter()
        .map(line_text)
        .collect::<Vec<_>>();
    if let Some(last) = chunks.pop() {
        lines.extend(chunks);
        current.push_str(&last);
    }
}

fn line_text(line: Line<'static>) -> String {
    line.spans
        .into_iter()
        .map(|span| span.content.into_owned())
        .collect::<String>()
}

fn prefix_value_line(prefix: &str, value: &str, width: u16) -> Line<'static> {
    let prefix_width = UnicodeWidthStr::width(prefix);
    let width_usize = usize::from(width.max(1));
    if prefix_width >= width_usize {
        return Line::styled(truncate_width(prefix, width), muted_style());
    }
    Line::from(vec![
        Span::styled(prefix.to_string(), muted_style()),
        Span::raw(truncate_width(
            value,
            width.saturating_sub(u16::try_from(prefix_width).unwrap_or(u16::MAX)),
        )),
    ])
}

fn server_table_header(width: u16) -> Line<'static> {
    fit_spans_to_width(
        vec![
            Span::styled("  ", muted_style()),
            Span::styled(pad_width("#", SERVER_INDEX_COL), muted_style()),
            Span::raw("  "),
            Span::styled(pad_width("Server", SERVER_NAME_COL), muted_style()),
            Span::raw("  "),
            Span::styled(pad_width("Status", SERVER_STATUS_COL), muted_style()),
            Span::raw("  "),
            Span::styled(pad_width("Transport", SERVER_TRANSPORT_COL), muted_style()),
            Span::raw("  "),
            Span::styled(pad_width("Tools", SERVER_TOOLS_COL), muted_style()),
        ],
        width,
    )
}

fn fit_spans_to_width(spans: Vec<Span<'static>>, width: u16) -> Line<'static> {
    let max_width = usize::from(width.max(1));
    let mut used = 0usize;
    let mut fitted = Vec::new();
    for span in spans {
        let content = span.content.as_ref();
        let span_width = UnicodeWidthStr::width(content);
        if used.saturating_add(span_width) <= max_width {
            used = used.saturating_add(span_width);
            fitted.push(span);
            continue;
        }
        let remaining = max_width.saturating_sub(used);
        if remaining > 0 {
            fitted.push(Span::styled(
                truncate_width(content, u16::try_from(remaining).unwrap_or(u16::MAX)),
                span.style,
            ));
        }
        break;
    }
    Line::from(fitted)
}

fn tool_table_layout(width: u16) -> ToolTableLayout {
    let total = usize::from(width.max(1));
    if total < 24 {
        return ToolTableLayout {
            name_col: total.saturating_sub(2).max(1),
            status_col: 0,
            full_name_col: 0,
            summary_col: 0,
        };
    }

    let full_layout_width = 2
        + TOOL_NAME_COL
        + 2
        + TOOL_STATUS_COL
        + 2
        + TOOL_FULL_NAME_COL
        + 2
        + TOOL_LIST_MIN_SUMMARY_COL;
    if total >= full_layout_width {
        return ToolTableLayout {
            name_col: TOOL_NAME_COL,
            status_col: TOOL_STATUS_COL,
            full_name_col: TOOL_FULL_NAME_COL,
            summary_col: total.saturating_sub(
                2 + TOOL_NAME_COL + 2 + TOOL_STATUS_COL + 2 + TOOL_FULL_NAME_COL + 2,
            ),
        };
    }

    let available = total.saturating_sub(2 + 2 + 2);
    let status_col = TOOL_STATUS_COL.min(available).min(13);
    let remaining = available.saturating_sub(status_col);
    let name_col = if remaining >= 36 {
        TOOL_NAME_COL.min(remaining.saturating_sub(12))
    } else {
        remaining.saturating_div(2).clamp(1, TOOL_NAME_COL)
    };
    let full_name_col = remaining.saturating_sub(name_col).min(TOOL_FULL_NAME_COL);
    ToolTableLayout {
        name_col,
        status_col,
        full_name_col,
        summary_col: 0,
    }
}

fn tool_table_header(layout: ToolTableLayout, width: u16) -> Line<'static> {
    let mut spans = vec![
        Span::styled("  ", muted_style()),
        Span::styled(pad_width("Tool name", layout.name_col), muted_style()),
    ];
    if layout.status_col > 0 {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            pad_width("Status", layout.status_col),
            muted_style(),
        ));
    }
    if layout.full_name_col > 0 {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            pad_width("Full name", layout.full_name_col),
            muted_style(),
        ));
    }
    if layout.summary_col > 0 {
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            truncate_width(
                "Summary",
                u16::try_from(layout.summary_col).unwrap_or(u16::MAX),
            ),
            muted_style(),
        ));
    }
    fit_spans_to_width(spans, width)
}

fn tool_status_spans(exposure: &McpToolExposure, layout: ToolTableLayout) -> Vec<Span<'static>> {
    if layout.status_col == 0 {
        return Vec::new();
    }
    vec![
        Span::raw("  "),
        Span::styled(
            pad_width(exposure.label(), layout.status_col),
            exposure_style(exposure),
        ),
    ]
}

fn tool_full_name_spans(full_name: &str, layout: ToolTableLayout) -> Vec<Span<'static>> {
    if layout.full_name_col == 0 {
        return Vec::new();
    }
    vec![
        Span::raw("  "),
        Span::styled(pad_width(full_name, layout.full_name_col), muted_style()),
    ]
}

fn tool_summary_spans(summary: &str, layout: ToolTableLayout) -> Vec<Span<'static>> {
    if layout.summary_col == 0 {
        return Vec::new();
    }
    vec![
        Span::raw("  "),
        Span::raw(truncate_width(
            summary,
            u16::try_from(layout.summary_col).unwrap_or(u16::MAX),
        )),
    ]
}

fn help_line(text: &str, width: u16) -> Line<'static> {
    Line::styled(truncate_width(text, width), muted_style())
}

fn view_tools_unavailable_notice(server: &McpServerSnapshot, busy: bool) -> &'static str {
    if busy {
        return "View tools is unavailable while this server is updating.";
    }
    match server.status {
        McpServerStatus::Disabled => "Enable this MCP server before viewing tools.",
        McpServerStatus::Failed => "Reconnect this MCP server before viewing tools.",
        McpServerStatus::Starting | McpServerStatus::Reconnecting => {
            "View tools is unavailable while this server is updating."
        }
        McpServerStatus::Ready => "View tools is unavailable for this server right now.",
    }
}

fn reconnect_unavailable_notice(server: &McpServerSnapshot, busy: bool) -> &'static str {
    if busy {
        return "Reconnect is unavailable while this server is updating.";
    }
    match server.status {
        McpServerStatus::Disabled => "Enable this MCP server before reconnecting.",
        McpServerStatus::Starting | McpServerStatus::Reconnecting => {
            "Reconnect is unavailable while this server is updating."
        }
        McpServerStatus::Ready | McpServerStatus::Failed => {
            "Reconnect is unavailable for this server right now."
        }
    }
}

fn effective_cwd(configured: &Option<PathBuf>, workspace_root: Option<&Path>) -> String {
    match (configured, workspace_root) {
        (Some(path), Some(root)) if path.is_relative() => root.join(path).display().to_string(),
        (Some(path), _) => path.display().to_string(),
        (None, Some(root)) => root.display().to_string(),
        (None, None) => "-".into(),
    }
}

fn format_transport(kind: Option<McpTransportKind>) -> String {
    match kind {
        Some(McpTransportKind::Stdio) => "stdio".into(),
        Some(McpTransportKind::StreamableHttp) => "streamable_http".into(),
        None => "-".into(),
    }
}

fn status_span(status: McpServerStatus) -> Span<'static> {
    Span::styled(status.as_str().to_string(), status_style(status))
}

fn status_style(status: McpServerStatus) -> Style {
    match status {
        McpServerStatus::Ready => surface_style()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        McpServerStatus::Failed => error_style(),
        McpServerStatus::Disabled => muted_style(),
        McpServerStatus::Starting => notice_style(),
        McpServerStatus::Reconnecting => blue_style(),
    }
}

fn exposure_span(exposure: &McpToolExposure) -> Span<'static> {
    Span::styled(exposure.label().to_string(), exposure_style(exposure))
}

fn exposure_style(exposure: &McpToolExposure) -> Style {
    match exposure {
        McpToolExposure::Exposed => surface_style()
            .fg(Color::Green)
            .add_modifier(Modifier::BOLD),
        McpToolExposure::Filtered { .. } => notice_style(),
        McpToolExposure::Unsupported { .. } => error_style(),
    }
}

fn exposure_reason(exposure: &McpToolExposure) -> Option<&'static str> {
    match exposure {
        McpToolExposure::Exposed => None,
        McpToolExposure::Filtered {
            reason: McpToolFilterReason::DisabledTools,
        } => Some("disabled_tools"),
        McpToolExposure::Filtered {
            reason: McpToolFilterReason::NotInEnabledTools,
        } => Some("not_in_enabled_tools"),
        McpToolExposure::Unsupported {
            reason: McpToolUnsupportedReason::InvalidSchema,
        } => Some("invalid_schema"),
    }
}

fn notice_style() -> Style {
    surface_style()
        .fg(Color::Yellow)
        .add_modifier(Modifier::BOLD)
}

fn error_style() -> Style {
    surface_style().fg(Color::Red).add_modifier(Modifier::BOLD)
}

fn selected_style(selected: bool) -> Style {
    if selected {
        blue_style()
    } else {
        surface_style()
    }
}

fn selection_marker(selected: bool) -> &'static str {
    if selected {
        "> "
    } else {
        "  "
    }
}

fn string_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".into()
    } else {
        values.join(",")
    }
}

fn tool_allowlist_summary(values: &[String]) -> String {
    if values.is_empty() {
        "all tools".into()
    } else {
        values.join(",")
    }
}

fn tool_denylist_summary(values: &[String]) -> String {
    if values.is_empty() {
        "none".into()
    } else {
        values.join(",")
    }
}

fn redacted_args(values: &[String]) -> String {
    if values.is_empty() {
        return "-".into();
    }
    let mut redact_next = false;
    values
        .iter()
        .map(|arg| {
            if redact_next {
                redact_next = false;
                return "<redacted>".to_string();
            }
            let lower = arg.to_ascii_lowercase();
            if lower.contains("authorization:") || lower.contains("bearer ") {
                return "<redacted>".to_string();
            }
            if is_sensitive_flag(arg) {
                redact_next = true;
                return arg.clone();
            }
            redact_arg_value(arg)
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn redact_arg_value(arg: &str) -> String {
    if let Some((key, value)) = arg.split_once('=') {
        let normalized = key.trim_start_matches('-');
        if is_sensitive_name(normalized) {
            return format!("{key}=<redacted>");
        }
        let redacted_value = redact_mcp_sensitive_text(value);
        if redacted_value != value {
            return format!("{key}={redacted_value}");
        }
    }
    redact_mcp_sensitive_text(arg)
}

fn is_sensitive_flag(arg: &str) -> bool {
    let normalized = arg.trim_start_matches('-');
    !normalized.contains('=') && is_sensitive_name(normalized)
}

fn is_sensitive_name(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase().replace('-', "_");
    lowered.contains("token")
        || lowered.contains("api_key")
        || lowered.contains("apikey")
        || lowered.contains("secret")
        || lowered.contains("password")
        || lowered == "key"
        || lowered.ends_with("_key")
        || lowered.contains("bearer")
        || lowered.contains("auth")
}

fn redacted_url(url: &str) -> String {
    if url == "-" {
        return "-".into();
    }
    redact_mcp_sensitive_text(url)
}

fn redacted_keys<'a>(keys: Option<impl Iterator<Item = &'a String>>) -> String {
    let Some(keys) = keys else {
        return "-".into();
    };
    let values = keys
        .map(|key| format!("{key}=<redacted>"))
        .collect::<Vec<_>>();
    if values.is_empty() {
        "-".into()
    } else {
        values.join(",")
    }
}

fn truncate_width(text: &str, width: u16) -> String {
    let width = usize::from(width.max(1));
    if UnicodeWidthStr::width(text) <= width {
        return text.to_string();
    }
    if width <= 1 {
        return "…".into();
    }
    let mut out = String::new();
    let mut used = 0usize;
    for ch in text.chars() {
        let next = unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0);
        if used.saturating_add(next) >= width {
            break;
        }
        out.push(ch);
        used = used.saturating_add(next);
    }
    out.push('…');
    out
}

fn pad_width(text: &str, width: usize) -> String {
    let mut out = truncate_width(text, u16::try_from(width).unwrap_or(u16::MAX));
    let used = UnicodeWidthStr::width(out.as_str());
    if used < width {
        out.push_str(&" ".repeat(width - used));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crossterm::event::{KeyEvent, KeyModifiers};
    use rmcp::model::Tool;
    use serde_json::json;

    use super::*;
    use crate::mcp::config::McpServerConfig;
    use crate::session_tui::theme::{BLUE_FG, BORDER_FG, MUTED_FG};

    #[test]
    fn server_list_title_is_bold_underlined_with_the_existing_accent_colors() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();

        let title = panel
            .render_lines(120, 40)
            .into_iter()
            .next()
            .expect("MCP title");
        assert_eq!(title.to_string(), "MCP · servers");
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
    }

    #[test]
    fn empty_server_list_omits_the_table_header() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), McpRuntimeState::default());
        panel.open();

        let text = render_text(&panel.render_lines(140, 40));
        assert!(text.contains("No MCP servers configured."));
        assert!(!text.contains("#    Server"));
        assert!(!text.contains("Transport"));
    }

    #[test]
    fn panel_renders_exposure_labels_with_full_names() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));

        let text = render_text(&panel.render_lines(120, 40));

        assert!(text.contains("exposed"));
        assert!(text.contains("filtered"));
        assert!(text.contains("unsupported"));
        assert!(text.contains("mcp__pal__ask"));
    }

    #[test]
    fn server_detail_renders_tools_summary_and_effective_workspace_cwd() {
        let mut snapshot = snapshot_with_tools();
        let server = snapshot.servers.get_mut("pal").unwrap();
        server.transport = Some(McpTransportKind::Stdio);
        server.config = McpServerConfig::stdio(
            "npx".into(),
            vec!["-y".into(), "@modelcontextprotocol/server-memory".into()],
            Default::default(),
            Vec::new(),
        );
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot);
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let text = render_text(&panel.render_lines(140, 40));

        assert!(text.contains("server            pal"));
        assert!(text.contains("tools             1 exposed / 3 discovered"));
        assert!(text.contains("cwd               /workspace/acn"));
        assert!(text.contains("enabled_tools     all tools"));
        assert!(text.contains("disabled_tools    none"));
        assert!(
            text.contains("↑/↓ to navigate · v view tools · r reconnect · d disable · Esc to back")
        );
    }

    #[test]
    fn server_detail_resolves_relative_configured_cwd() {
        let mut snapshot = snapshot_with_tools();
        let server = snapshot.servers.get_mut("pal").unwrap();
        let mut config = McpServerConfig::stdio(
            "uvx".into(),
            vec!["pal-mcp-server".into()],
            Default::default(),
            Vec::new(),
        );
        config.cwd = Some(PathBuf::from("servers/pal"));
        server.transport = Some(McpTransportKind::Stdio);
        server.config = config;
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot);
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let text = render_text(&panel.render_lines(140, 40));

        assert!(text.contains("cwd               /workspace/acn/servers/pal"));
    }

    #[test]
    fn server_list_and_detail_narrow_width_do_not_overflow() {
        let mut snapshot = snapshot_with_tools();
        let server = snapshot.servers.remove("pal").unwrap();
        let long_name = "very_long_streamable_http_server_name_for_layout".to_string();
        snapshot.servers.insert(long_name.clone(), server);
        snapshot.servers.get_mut(&long_name).unwrap().name = long_name.clone();
        snapshot.servers.values_mut().next().unwrap().last_error = Some(
            "Authorization: Bearer secret-token at https://user:pass@example.test/mcp?token=abc"
                .into(),
        );
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot);
        panel.open();

        let list_lines = panel.render_lines(48, 40);
        assert_lines_fit(&list_lines, 48);

        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let detail_lines = panel.render_lines(48, 40);
        assert_lines_fit(&detail_lines, 48);
    }

    #[test]
    fn server_list_renders_table_header_with_index() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();

        let text = render_text(&panel.render_lines(140, 40));

        assert!(text.contains("#    Server"));
        assert!(text.contains("Status"));
        assert!(text.contains("Transport"));
        assert!(text.contains("Tools"));
        assert!(text.contains("> 1    pal"));
        assert!(!text.contains("servers 1"));
    }

    #[test]
    fn tool_list_renders_aligned_header_and_bottom_help() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));

        let text = render_text(&panel.render_lines(160, 40));

        assert!(text.contains("Tool name"));
        assert!(text.contains("Status"));
        assert!(text.contains("Full name"));
        assert!(text.contains("Summary"));
        assert!(text.contains("↑/↓ to navigate · Enter to open · Esc to back"));
    }

    #[test]
    fn tool_list_narrow_width_does_not_overflow() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));

        let lines = panel.render_lines(48, 40);

        assert_lines_fit(&lines, 48);
        let text = render_text(&lines);
        assert!(text.contains("Tool name"));
        assert!(text.contains("Status"));
        assert!(text.contains("Full name"));
        assert!(!text.contains("Summary"));
    }

    #[test]
    fn tool_detail_renders_schema_as_code_block() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let text = render_text(&panel.render_lines(120, 60));

        assert!(text.contains("╭─ Input schema"));
        assert!(text.contains("│ {"));
        assert!(text.contains("╰"));
        assert!(text.contains("↑/↓ to scroll · Esc to back"));
    }

    #[test]
    fn tool_detail_narrow_width_does_not_overflow() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let lines = panel.render_lines(48, 60);

        assert_lines_fit(&lines, 48);
        let text = render_text(&lines);
        assert!(text.contains("╭─ Input schema"));
    }

    #[test]
    fn disabled_server_does_not_open_empty_tool_list() {
        let mut snapshot = snapshot_with_tools();
        let server = snapshot.servers.get_mut("pal").unwrap();
        server.status = McpServerStatus::Disabled;
        server.config.enabled = Some(false);
        server.tools.clear();
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot);
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let before = render_text(&panel.render_lines(120, 40));
        assert!(before.contains("↑/↓ to navigate · d enable · Esc to back"));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));
        let after = render_text(&panel.render_lines(120, 40));

        assert!(after.contains("server detail"));
        assert!(after.contains("Enable this MCP server before viewing tools."));
        assert!(!after.contains("No tools discovered for this server."));
    }

    #[test]
    fn disabling_server_pending_state_does_not_offer_enable() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.begin_request(&McpPanelRequest::SetEnabled {
            server_name: "pal".into(),
            enabled: false,
        });

        let text = render_text(&panel.render_lines(120, 40));

        assert!(text.contains("↑/↓ to navigate · updating · Esc to back"));
        assert!(!text.contains("↑/↓ to navigate · d enable · Esc to back"));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));
        let text = render_text(&panel.render_lines(120, 40));
        assert!(text.contains("View tools is unavailable while this server is updating."));
    }

    #[test]
    fn tool_views_ignore_reconnect_and_disable_keys() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));

        assert_eq!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE)),
            McpPanelKeyAction::None
        );
        assert_eq!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE)),
            McpPanelKeyAction::None
        );
        let text = render_text(&panel.render_lines(120, 40));
        assert!(text.contains("MCP · tools"));
        assert_eq!(
            panel.selected_server().map(|server| server.status),
            Some(McpServerStatus::Ready)
        );

        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE)),
            McpPanelKeyAction::None
        );
        assert_eq!(
            panel.handle_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE)),
            McpPanelKeyAction::None
        );
        let text = render_text(&panel.render_lines(120, 40));
        assert!(text.contains("MCP · tool detail"));
        assert_eq!(
            panel.selected_server().map(|server| server.status),
            Some(McpServerStatus::Ready)
        );
    }

    #[test]
    fn extremely_narrow_width_does_not_overflow_mcp_views() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        assert_lines_fit(&panel.render_lines(2, 40), 2);

        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_lines_fit(&panel.render_lines(2, 40), 2);

        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));
        assert_lines_fit(&panel.render_lines(2, 40), 2);

        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_lines_fit(&panel.render_lines(2, 40), 2);
    }

    #[test]
    fn stale_detail_views_with_missing_selection_do_not_overflow() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), McpRuntimeState::default());
        assert_lines_fit(&panel.render_lines(2, 40), 2);

        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), McpRuntimeState::default());
        assert_lines_fit(&panel.render_lines(2, 40), 2);

        let mut no_tool_snapshot = snapshot_with_tools();
        no_tool_snapshot
            .servers
            .get_mut("pal")
            .unwrap()
            .tools
            .clear();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE));
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), no_tool_snapshot);
        assert_lines_fit(&panel.render_lines(2, 40), 2);
    }

    #[test]
    fn reconnect_key_emits_request_for_ready_server() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();

        let action = panel.handle_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));

        assert_eq!(
            action,
            McpPanelKeyAction::Request(McpPanelRequest::Reconnect {
                server_name: "pal".into()
            })
        );
    }

    #[test]
    fn failed_operation_restores_runtime_snapshot() {
        let mut panel = McpPanelState::default();
        let snapshot = snapshot_with_tools();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot.clone());
        panel.open();
        let operation_id = panel.begin_request(&McpPanelRequest::SetEnabled {
            server_name: "pal".into(),
            enabled: false,
        });

        assert!(panel.finish_request(
            "pal",
            operation_id,
            McpOperationOutcome {
                snapshot,
                error: Some("write failed".into()),
            },
        ));

        let text = render_text(&panel.render_lines(120, 40));
        assert!(text.contains("ready"));
        assert!(text.contains("write failed"));
        assert!(text.contains("1/3"));
    }

    #[test]
    fn stale_operation_finish_does_not_restore_ready_snapshot() {
        let mut panel = McpPanelState::default();
        let ready_snapshot = snapshot_with_tools();
        let mut disabled_snapshot = snapshot_with_tools();
        let disabled = disabled_snapshot.servers.get_mut("pal").unwrap();
        disabled.status = McpServerStatus::Disabled;
        disabled.config.enabled = Some(false);
        disabled.tools.clear();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), ready_snapshot.clone());
        panel.open();
        let stale_operation_id = panel.begin_request(&McpPanelRequest::Reconnect {
            server_name: "pal".into(),
        });
        let active_operation_id = panel.begin_request(&McpPanelRequest::SetEnabled {
            server_name: "pal".into(),
            enabled: false,
        });

        assert!(!panel.finish_request(
            "pal",
            stale_operation_id,
            McpOperationOutcome {
                snapshot: ready_snapshot,
                error: None,
            },
        ));
        let text = render_text(&panel.render_lines(120, 40));
        assert!(text.contains("disabled"));
        assert!(!text.contains("ready"));

        assert!(panel.finish_request(
            "pal",
            active_operation_id,
            McpOperationOutcome {
                snapshot: disabled_snapshot,
                error: None,
            },
        ));
    }

    #[test]
    fn operation_error_notice_is_redacted() {
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot_with_tools());
        panel.open();
        let operation_id = panel.begin_request(&McpPanelRequest::Reconnect {
            server_name: "pal".into(),
        });

        assert!(panel.finish_request(
            "pal",
            operation_id,
            McpOperationOutcome {
                snapshot: snapshot_with_tools(),
                error: Some("Authorization: Bearer secret-token".into()),
            },
        ));

        let text = render_text(&panel.render_lines(120, 40));
        assert!(text.contains("<redacted>"));
        assert!(!text.contains("secret-token"));
    }

    #[test]
    fn server_detail_last_error_is_redacted() {
        let mut snapshot = snapshot_with_tools();
        snapshot.servers.get_mut("pal").unwrap().last_error =
            Some("url=\"https://user:pass@example.test/mcp?token=abc\"".into());
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot);
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let text = render_text(&panel.render_lines(120, 40));
        assert!(text.contains("https://<redacted>@example.test/mcp?<redacted>"));
        assert!(!text.contains("user:pass"));
        assert!(!text.contains("token=abc"));
    }

    #[test]
    fn server_detail_last_error_wraps_without_truncating_tail() {
        let mut snapshot = snapshot_with_tools();
        let server = snapshot.servers.get_mut("pal").unwrap();
        server.status = McpServerStatus::Failed;
        server.last_error = Some(
            "MCP server 'dlptest' initialize 失败: Send message error Transport [rmcp::transport::worker::WorkerTransport<rmcp::transport::streamable_http_client::StreamableHttpClientWorker<agent_claim_network::mcp::client::AcnMcpHttpClient>>] error: Unexpected content type: Some(\"text/html\"), when send initialize request"
                .into(),
        );
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot);
        panel.open();
        panel.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let lines = panel.render_lines(84, 80);
        let text = render_text(&lines);

        assert_lines_fit(&lines, 84);
        assert!(text.contains("Last error        MCP server 'dlptest' initialize"));
        assert!(text.contains("Unexpected content type"));
        assert!(text.contains("Some(\"text/html\")"));
        assert!(text.contains("when send initialize request"));
        assert!(text.contains("↑/↓ scroll · r reconnect · d disable · Esc to back"));
        assert!(!text.contains("streamab…"));
    }

    #[test]
    fn server_list_last_error_is_redacted() {
        let mut snapshot = snapshot_with_tools();
        snapshot.servers.get_mut("pal").unwrap().last_error =
            Some("X-API-Key:sk-test url=https://user:pass@example.test/mcp?token=abc".into());
        let mut panel = McpPanelState::default();
        panel.set_runtime(PathBuf::from("/tmp/.mcp.json"), snapshot);
        panel.open();

        let text = render_text(&panel.render_lines(240, 40));

        assert!(text.contains("X-API-Key:<redacted>"));
        assert!(text.contains("https://<redacted>@example.test/mcp?<redacted>"));
        assert!(!text.contains("sk-test"));
        assert!(!text.contains("user:pass"));
        assert!(!text.contains("token=abc"));
    }

    #[test]
    fn redacts_sensitive_args_and_urls() {
        let args = vec![
            "--api-key".to_string(),
            "secret-value".to_string(),
            "--token=abc".to_string(),
            "--endpoint=https://user:pass@example.test/mcp?token=abc".to_string(),
            "Authorization: Bearer abc".to_string(),
            "MODEL=auto".to_string(),
            "OPENAI_API_KEY=abc".to_string(),
        ];

        let rendered_args = redacted_args(&args);
        let rendered_url = redacted_url("https://user:pass@example.test/mcp?token=abc#frag");

        assert!(!rendered_args.contains("secret-value"));
        assert!(!rendered_args.contains("token=abc"));
        assert!(!rendered_args.contains("OPENAI_API_KEY=abc"));
        assert!(!rendered_args.contains("user:pass"));
        assert!(!rendered_args.contains("Bearer abc"));
        assert!(rendered_args.contains("MODEL=auto"));
        assert_eq!(
            rendered_url,
            "https://<redacted>@example.test/mcp?<redacted>"
        );
    }

    fn snapshot_with_tools() -> McpRuntimeState {
        let server = McpServerSnapshot {
            name: "pal".into(),
            config: McpServerConfig::streamable_http("https://example.test/mcp".into(), None),
            transport: Some(McpTransportKind::StreamableHttp),
            status: McpServerStatus::Ready,
            tools: vec![
                tool("ask", McpToolExposure::Exposed),
                tool(
                    "hidden",
                    McpToolExposure::Filtered {
                        reason: McpToolFilterReason::DisabledTools,
                    },
                ),
                tool(
                    "invalid_schema",
                    McpToolExposure::Unsupported {
                        reason: McpToolUnsupportedReason::InvalidSchema,
                    },
                ),
            ],
            server_info: None,
            last_connected_at: None,
            last_error: None,
            stderr_excerpt: None,
        };
        McpRuntimeState {
            servers: std::collections::BTreeMap::from([("pal".into(), server)]),
            generations: std::collections::BTreeMap::from([("pal".into(), 1)]),
            startup_error: None,
            workspace_root: Some(PathBuf::from("/workspace/acn")),
        }
    }

    fn tool(name: &'static str, exposure: McpToolExposure) -> McpToolSnapshot {
        McpToolSnapshot {
            raw_name: name.into(),
            title: None,
            description: Some("test tool".into()),
            exposure,
            raw_tool: Tool::new(
                name,
                "test tool",
                Arc::new(
                    json!({"type": "object", "properties": {"q": {"type": "string"}}})
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                ),
            ),
        }
    }

    fn render_text(lines: &[Line<'static>]) -> String {
        lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn assert_lines_fit(lines: &[Line<'static>], width: u16) {
        let max_width = usize::from(width.max(1));
        for line in lines {
            let text = line
                .spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<String>();
            assert!(
                UnicodeWidthStr::width(text.as_str()) <= max_width,
                "Line exceeds width {width}: {text:?}"
            );
        }
    }
}
