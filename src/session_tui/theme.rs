//! TUI 视觉主题。
//!
//! 集中维护 ACN TUI 的浅色 surface、弱化文字与品牌强调色。
//! 渲染层通过这些 helper 给每行补齐背景色，避免主题色散落在各组件中。

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;

pub(super) const SURFACE_BG: Color = Color::Rgb(250, 248, 242);
pub(super) const SURFACE_FG: Color = Color::Rgb(35, 35, 33);
pub(super) const KEY_FG: Color = Color::Rgb(72, 68, 64);
pub(super) const MUTED_FG: Color = Color::Rgb(101, 96, 91);
pub(super) const BORDER_FG: Color = Color::Rgb(206, 122, 88);
pub(super) const BLUE_FG: Color = Color::Rgb(53, 113, 177);
pub(super) const CODE_CONTENT_FG: Color = Color::Rgb(126, 70, 32);
/// 新增行使用低饱和绿底，保证浅色 surface 上醒目但不刺眼。
pub(super) const DIFF_ADDED_BG: Color = Color::Rgb(225, 242, 228);
/// 深绿前景避免依赖终端 ANSI palette，在 macOS 浅色主题上保持稳定对比度。
pub(super) const DIFF_ADDED_FG: Color = Color::Rgb(30, 96, 49);
/// 删除行使用低饱和红底，与新增行形成稳定的语义区分。
pub(super) const DIFF_REMOVED_BG: Color = Color::Rgb(249, 228, 225);
/// 深红前景与淡红背景配对，保证正文和 marker 清晰可读。
pub(super) const DIFF_REMOVED_FG: Color = Color::Rgb(151, 50, 45);

pub(super) fn surface_style() -> Style {
    Style::default().fg(SURFACE_FG).bg(SURFACE_BG)
}

pub(super) fn muted_style() -> Style {
    surface_style().fg(MUTED_FG)
}

pub(super) fn key_style() -> Style {
    surface_style().fg(KEY_FG).add_modifier(Modifier::BOLD)
}

pub(super) fn accent_style() -> Style {
    surface_style().fg(BORDER_FG).add_modifier(Modifier::BOLD)
}

pub(super) fn blue_style() -> Style {
    surface_style().fg(BLUE_FG).add_modifier(Modifier::BOLD)
}

pub(super) fn diff_added_style() -> Style {
    Style::default().fg(DIFF_ADDED_FG).bg(DIFF_ADDED_BG)
}

pub(super) fn diff_removed_style() -> Style {
    Style::default().fg(DIFF_REMOVED_FG).bg(DIFF_REMOVED_BG)
}

pub(super) fn apply_surface_style(mut line: Line<'static>) -> Line<'static> {
    line.style = surface_style().patch(line.style);
    line
}
