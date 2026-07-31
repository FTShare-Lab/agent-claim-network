//! 启动欢迎卡片。
//!
//! 这块内容作为 scrollback 起始分隔符刷在当前会话第一条消息之前，
//! hard clear / resize 时会按新宽度重绘。

use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::agent::TeamServiceConnectionStatus;

use super::state::SessionTuiState;
use super::theme::{accent_style, blue_style, key_style, muted_style, surface_style};
use super::wrapping::hard_wrap_styled_lines;

const RIGHT_KEY_WIDTH: usize = 10;
const RIGHT_VALUE_GAP: usize = 2;

pub(super) fn startup_welcome_lines(width: u16, state: &SessionTuiState) -> Vec<Line<'static>> {
    let width = usize::from(width.max(1));
    if width < 48 {
        return compact_startup_lines(width, state);
    }

    let title = " Agent Claim Network ";
    let inner_width = width.saturating_sub(2);
    let has_right_column = width >= 72;
    let left_width = if has_right_column {
        let ratio = if width >= 104 { 40 } else { 38 };
        inner_width.saturating_mul(ratio) / 100
    } else {
        inner_width
    };
    let right_width = inner_width
        .saturating_sub(left_width)
        .saturating_sub(usize::from(has_right_column));

    let model = state.model_name.as_deref().unwrap_or("not set");
    let agent = state.agent_id.as_deref().unwrap_or("not set");
    let workspace = state.workspace_label();
    let branch = state.branch_label();
    let team_status = state.team_services_connection_status();
    let left_rows = [
        heading_cell("  ", "Runtime Metadata", blue_style()),
        key_value_cell("  ", "Model", " ", model),
        key_value_cell("  ", "Agent", " ", agent),
        key_value_cell("  ", "Cwd", " ", workspace),
        key_value_cell("  ", "Branch", " ", branch),
        team_status_cell(
            "  ",
            team_service_status_icon(team_status.maintainer),
            team_service_status_icon(team_status.router),
        ),
    ];
    let right_rows = [
        heading_cell("", "ACN 工作流", accent_style()),
        aligned_key_value_cell("", "Roles", "Agent · Router · Maintainer"),
        aligned_key_value_cell("", "Memory", "偏好与经验沉淀 → 私有记忆"),
        aligned_key_value_cell("", "Claim", "可协作的判断对象 → 团队可见"),
        aligned_key_value_cell("", "Router", "团队信息检索器"),
        aligned_key_value_cell("", "Maintainer", "团队管理与台账"),
    ];

    let mut lines = vec![border_line("╭", "╮", title, width)];
    if !has_right_column {
        let single_rows = left_rows
            .iter()
            .cloned()
            .chain(right_rows.iter().cloned())
            .collect::<Vec<_>>();
        for row in &single_rows {
            lines.push(startup_row(row, left_width, None));
        }
    } else {
        for (idx, left) in left_rows.iter().enumerate() {
            lines.push(startup_row(
                left,
                left_width,
                Some((&right_rows[idx], right_width)),
            ));
        }
    }
    lines.push(border_line("╰", "╯", "", width));
    lines
}

fn compact_startup_lines(width: usize, state: &SessionTuiState) -> Vec<Line<'static>> {
    let model = state.model_name.as_deref().unwrap_or("not set");
    let agent = state.agent_id.as_deref().unwrap_or("not set");
    let workspace = state.workspace_label();
    let branch = state.branch_label();
    let team_status = state.team_services_connection_status();
    let mut lines = hard_wrap_styled_lines(
        vec![
            Line::styled(
                "Agent Claim Network",
                accent_style().add_modifier(Modifier::BOLD),
            ),
            styled_cell_line(heading_cell("", "Runtime Metadata", blue_style())),
            styled_cell_line(key_value_cell("", "Model", " ", model)),
            styled_cell_line(key_value_cell("", "Agent", " ", agent)),
            styled_cell_line(key_value_cell("", "Cwd", " ", workspace)),
            styled_cell_line(key_value_cell("", "Branch", " ", branch)),
            styled_cell_line(team_status_cell(
                "",
                team_service_status_icon(team_status.maintainer),
                team_service_status_icon(team_status.router),
            )),
            styled_cell_line(heading_cell("", "ACN 工作流", accent_style())),
            styled_cell_line(aligned_key_value_cell(
                "",
                "Roles",
                "Agent · Router · Maintainer",
            )),
            styled_cell_line(aligned_key_value_cell(
                "",
                "Memory",
                "偏好与经验沉淀 → 私有记忆",
            )),
            styled_cell_line(aligned_key_value_cell(
                "",
                "Claim",
                "可协作的判断对象 → 团队可见",
            )),
            styled_cell_line(aligned_key_value_cell("", "Router", "团队信息检索器")),
            styled_cell_line(aligned_key_value_cell("", "Maintainer", "团队管理与台账")),
        ],
        width,
    );
    lines.push(Line::styled("─".repeat(width), muted_style()));
    lines
}

fn border_line(left: &str, right: &str, title: &str, width: usize) -> Line<'static> {
    let title_prefix = if title.is_empty() {
        String::new()
    } else {
        format!("─{title}")
    };
    let used = text_width(left)
        .saturating_add(text_width(&title_prefix))
        .saturating_add(text_width(right));
    let fill = "─".repeat(width.saturating_sub(used));
    Line::styled(format!("{left}{title_prefix}{fill}{right}"), accent_style())
}

fn startup_row(
    left: &[Span<'static>],
    left_width: usize,
    right: Option<(&[Span<'static>], usize)>,
) -> Line<'static> {
    let mut spans = vec![Span::styled("│", accent_style())];
    spans.extend(fit_styled_cell(left, left_width));
    if let Some((right, right_width)) = right {
        spans.push(Span::styled("│", muted_style()));
        spans.extend(fit_styled_cell(right, right_width));
    }
    spans.push(Span::styled("│", accent_style()));
    Line::from(spans).style(surface_style())
}

fn heading_cell(prefix: &str, heading: &str, style: Style) -> Vec<Span<'static>> {
    vec![
        Span::styled(prefix.to_string(), surface_style()),
        Span::styled(heading.to_string(), style),
    ]
}

fn key_value_cell(prefix: &str, key: &str, separator: &str, value: &str) -> Vec<Span<'static>> {
    vec![
        Span::styled(prefix.to_string(), surface_style()),
        Span::styled(key.to_string(), key_style()),
        Span::styled(separator.to_string(), surface_style()),
        Span::styled(value.to_string(), muted_style()),
    ]
}

fn aligned_key_value_cell(prefix: &str, key: &str, value: &str) -> Vec<Span<'static>> {
    let separator_width = RIGHT_KEY_WIDTH
        .saturating_sub(text_width(key))
        .saturating_add(RIGHT_VALUE_GAP);
    key_value_cell(prefix, key, &" ".repeat(separator_width), value)
}

fn team_status_cell(
    prefix: &str,
    maintainer_status: &str,
    router_status: &str,
) -> Vec<Span<'static>> {
    vec![
        Span::styled(prefix.to_string(), surface_style()),
        Span::styled("Maintainer", key_style()),
        Span::styled(" ", surface_style()),
        Span::styled(maintainer_status.to_string(), muted_style()),
        Span::styled("  ", surface_style()),
        Span::styled("Router", key_style()),
        Span::styled(" ", surface_style()),
        Span::styled(router_status.to_string(), muted_style()),
    ]
}

fn styled_cell_line(spans: Vec<Span<'static>>) -> Line<'static> {
    Line::from(spans).style(surface_style())
}

fn team_service_status_icon(status: TeamServiceConnectionStatus) -> &'static str {
    match status {
        TeamServiceConnectionStatus::Unknown => "❓",
        TeamServiceConnectionStatus::Connected => "✅",
        TeamServiceConnectionStatus::Failed => "❌",
    }
}

fn fit_styled_cell(parts: &[Span<'static>], width: usize) -> Vec<Span<'static>> {
    if width == 0 {
        return Vec::new();
    }

    let content_width = parts
        .iter()
        .map(|part| text_width(part.content.as_ref()))
        .sum::<usize>();
    if content_width <= width {
        let mut fitted = parts.to_vec();
        fitted.push(Span::styled(
            " ".repeat(width.saturating_sub(content_width)),
            surface_style(),
        ));
        return fitted;
    }

    let ellipsis = "…";
    let ellipsis_width = text_width(ellipsis);
    let target_width = width.saturating_sub(ellipsis_width);
    let mut fitted = Vec::new();
    let mut used = 0usize;
    let mut ellipsis_style = surface_style();
    for part in parts {
        let mut segment = String::new();
        let mut part_was_truncated = false;
        for ch in part.content.chars() {
            let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
            if used.saturating_add(ch_width) > target_width {
                part_was_truncated = true;
                break;
            }
            segment.push(ch);
            used = used.saturating_add(ch_width);
        }
        if !segment.is_empty() {
            ellipsis_style = part.style;
            fitted.push(Span::styled(segment, part.style));
        }
        if used >= target_width {
            break;
        }
        if part_was_truncated {
            break;
        }
    }
    if ellipsis_width <= width {
        fitted.push(Span::styled(ellipsis, ellipsis_style));
    }
    let fitted_width = used.saturating_add(ellipsis_width.min(width));
    fitted.push(Span::styled(
        " ".repeat(width.saturating_sub(fitted_width)),
        surface_style(),
    ));
    fitted
}

fn text_width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_tui::state::SessionTuiState;

    #[test]
    fn startup_welcome_uses_injected_workspace_label() {
        let mut state = SessionTuiState::new();
        state.set_workspace_context("~/Workspace/ft".into(), "main".into());

        let rendered = startup_welcome_lines(96, &state)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n");

        assert!(rendered.contains("Cwd ~/Workspace/ft"));
        assert!(rendered.contains("Branch main"));
        assert!(rendered.contains("Maintainer ❓  Router ❓"));
    }

    #[test]
    fn startup_welcome_styles_headings_keys_and_values_as_requested() {
        let lines = startup_welcome_lines(96, &SessionTuiState::new());

        assert_eq!(span_style(&lines[1], "Runtime Metadata"), blue_style());
        assert_eq!(span_style(&lines[1], "ACN 工作流"), accent_style());
        assert_ne!(key_style().fg, surface_style().fg);
        assert_ne!(key_style().fg, muted_style().fg);

        for (line_index, key, value) in [
            (2, "Model", "not set"),
            (2, "Roles", "Agent · Router · Maintainer"),
            (3, "Agent", "not set"),
            (3, "Memory", "偏好与经验沉淀 → 私有记忆"),
            (4, "Cwd", "--"),
            (4, "Claim", "可协作的判断对象 → 团队可见"),
            (5, "Branch", "--"),
            (5, "Router", "团队信息检索器"),
            (6, "Maintainer", "❓"),
            (6, "Router", "❓"),
            (6, "Maintainer", "团队管理与台账"),
        ] {
            assert_eq!(span_style(&lines[line_index], key), key_style());
            assert_eq!(span_style(&lines[line_index], value), muted_style());
        }
    }

    #[test]
    fn startup_welcome_aligns_right_column_values() {
        let lines = startup_welcome_lines(144, &SessionTuiState::new());
        let value_columns = [
            (2, "Agent · Router · Maintainer"),
            (3, "偏好与经验沉淀 → 私有记忆"),
            (4, "可协作的判断对象 → 团队可见"),
            (5, "团队信息检索器"),
            (6, "团队管理与台账"),
        ]
        .map(|(line_index, value)| span_column(&lines[line_index], value));

        assert!(value_columns
            .iter()
            .all(|column| *column == value_columns[0]));
    }

    fn span_style(line: &Line<'static>, text: &str) -> Style {
        line.spans
            .iter()
            .find(|span| span.content == text)
            .map(|span| span.style)
            .expect("测试文本应存在于欢迎页 span 中")
    }

    fn span_column(line: &Line<'static>, text: &str) -> usize {
        let mut column = 0usize;
        for span in &line.spans {
            if span.content == text {
                return column;
            }
            column = column.saturating_add(text_width(span.content.as_ref()));
        }
        panic!("测试文本应存在于欢迎页 span 中");
    }
}
