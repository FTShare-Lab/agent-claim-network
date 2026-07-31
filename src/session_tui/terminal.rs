//! TUI 终端生命周期与输入事件 reader。
//!
//! 本模块封装 raw mode、bracketed paste 与 inline live region 绘制，并用单独
//! blocking reader 把 crossterm 输入事件转发给 async 主循环。

use std::borrow::Cow;
use std::fmt;
use std::io::{self, Stdout, Write};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::Context;
use crossterm::cursor::{Hide, MoveDown, MoveTo, MoveToColumn, MoveUp, Show};
use crossterm::event::{
    DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind, KeyModifiers,
    KeyboardEnhancementFlags, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::style::{
    Attribute, Color as CrosstermColor, Colored, Print, ResetColor, SetAttribute,
    SetBackgroundColor, SetForegroundColor,
};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, BeginSynchronizedUpdate, Clear, ClearType, DisableLineWrap,
    EnableLineWrap, EndSynchronizedUpdate,
};
use crossterm::Command;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use tokio::sync::mpsc;
use unicode_width::UnicodeWidthStr;

use super::theme::{DIFF_ADDED_BG, DIFF_REMOVED_BG, SURFACE_BG};

const PASTE_BURST_CHAR_INTERVAL: Duration = Duration::from_millis(8);
const PASTE_BURST_MIN_EVENTS: usize = 3;
const PASTE_BURST_MAX_EVENTS: usize = 20_000;
const PASTE_BURST_MIN_SINGLE_LINE_CHARS: usize = 16;

pub(super) enum TerminalEvent {
    Input(Event),
    Error(String),
}

pub(super) struct TerminalGuard {
    stdout: Stdout,
    frame_buffer: Vec<u8>,
    bracketed_paste_enabled: bool,
    keyboard_enhancement_enabled: bool,
    modify_other_keys_enabled: bool,
    live_region_line_widths: Vec<usize>,
    live_region_cursor: Option<(usize, usize)>,
    // live region 上一帧绘制时所用的终端宽度。清除旧 live region 必须用“画它时的宽度”
    // 而非现查宽度，否则 resize 改变宽度后折行行数算错，MoveUp 次数与实际占用行不符。
    live_region_terminal_width: u16,
}

impl TerminalGuard {
    pub(super) fn enter() -> anyhow::Result<Self> {
        enable_raw_mode().context("启用 TUI raw mode 失败")?;
        let mut stdout = io::stdout();
        if let Err(e) = execute!(stdout, EnableBracketedPaste, DisableLineWrap) {
            let _ = execute!(stdout, DisableBracketedPaste, EnableLineWrap);
            let _ = disable_raw_mode();
            return Err(e).context("初始化 TUI 终端模式失败");
        }
        let keyboard_enhancement_enabled = enable_keyboard_enhancement();
        let modify_other_keys_enabled =
            keyboard_enhancement_enabled && enable_modify_other_keys_if_needed();
        // 进入 TUI 时一次性清屏并清掉终端原生 scrollback 缓冲（ClearType::Purge = ESC[3J），
        // 给 inline 渲染一个干净起点：往回翻只会看到本会话的 welcome + 对话，不再混入进入前
        // 残留的内容（很多终端的 `clear` 只发 ESC[2J，并不清 scrollback）。清屏失败不致命。
        let _ = clear_screen_with_surface_background(&mut stdout);
        Ok(Self {
            stdout,
            frame_buffer: Vec::new(),
            bracketed_paste_enabled: true,
            keyboard_enhancement_enabled,
            modify_other_keys_enabled,
            live_region_line_widths: Vec::new(),
            live_region_cursor: None,
            live_region_terminal_width: 0,
        })
    }

    pub(super) fn draw(
        &mut self,
        terminal_width: u16,
        scrollback_lines: &[Line<'static>],
        live_lines: &[Line<'static>],
        cursor: Option<(u16, u16)>,
        hard_clear: bool,
    ) -> anyhow::Result<()> {
        // execute! 会刷新目标 writer。若直接写 stdout，tmux 可能在 Begin/End 之间转发出
        // “只清完旧画面、尚未画出新画面”的中间态。整帧先写入内存（Vec::flush 是空操作），
        // 再一次性提交给终端；DEC 2026 则继续为支持它的终端提供额外的原子呈现保证。
        let mut frame = std::mem::take(&mut self.frame_buffer);
        frame.clear();
        let render_result = render_synchronized_frame(&mut frame, |frame| {
            self.draw_body(
                frame,
                terminal_width,
                scrollback_lines,
                live_lines,
                cursor,
                hard_clear,
            )
        });
        if let Err(error) = render_result {
            self.frame_buffer = frame;
            return Err(error);
        }

        let commit_result = commit_frame(&mut self.stdout, &frame).context("提交 TUI 帧失败");
        self.frame_buffer = frame;
        commit_result
    }

    fn draw_body(
        &mut self,
        stdout: &mut impl Write,
        terminal_width: u16,
        scrollback_lines: &[Line<'static>],
        live_lines: &[Line<'static>],
        cursor: Option<(u16, u16)>,
        hard_clear: bool,
    ) -> anyhow::Result<()> {
        // 清除旧 live region 必须用它“被绘制时”的宽度，而非现查 terminal_width；
        // resize 改变宽度后两者不同，用新宽度反算旧 live region 行数会错位（吞历史/留残骸）。
        let previous_width = self.live_region_terminal_width;
        let previous_live_region_height =
            live_region_visual_height(&self.live_region_line_widths, usize::from(previous_width));
        let can_reuse_live_region = !hard_clear
            && scrollback_lines.is_empty()
            && previous_live_region_height > 0
            && !live_lines.is_empty();
        if hard_clear {
            // ClearType::All(ESC[2J) 只清可见屏；必须配合 ClearType::Purge(ESC[3J) 清掉
            // 向上滚动的 scrollback 缓冲，否则随后按新宽度重排的历史会与旧副本叠加成重复历史。
            clear_screen_with_surface_background(stdout).context("清理 TUI 屏幕失败")?;
            self.live_region_line_widths.clear();
            self.live_region_cursor = None;
        } else {
            clear_live_region(
                stdout,
                &self.live_region_line_widths,
                self.live_region_cursor,
                previous_width,
            )
            .context("清理 TUI live region 失败")?;
        }
        for line in scrollback_lines {
            write_line(stdout, line).context("写入 TUI scrollback 失败")?;
        }
        if can_reuse_live_region {
            write_live_region_in_place(stdout, live_lines, previous_live_region_height)
                .context("绘制 TUI live region 失败")?;
            move_cursor_after_in_place_live_region(
                stdout,
                live_lines.len(),
                previous_live_region_height.max(live_lines.len()),
                cursor,
            )
            .context("移动 TUI 光标失败")?;
        } else {
            for line in live_lines {
                write_line(stdout, line).context("绘制 TUI live region 失败")?;
            }
            move_cursor_after_appended_live_region(stdout, live_lines.len(), cursor)
                .context("移动 TUI 光标失败")?;
        }
        let reserved_live_rows = if can_reuse_live_region {
            previous_live_region_height.max(live_lines.len())
        } else {
            live_lines.len()
        };
        self.live_region_line_widths = live_lines.iter().map(printed_line_width).collect();
        self.live_region_line_widths.extend(std::iter::repeat_n(
            0,
            reserved_live_rows.saturating_sub(live_lines.len()),
        ));
        self.live_region_cursor =
            cursor.map(|(column, row)| (usize::from(column), usize::from(row)));
        // 记录这一帧 live region 所用的宽度，供下一帧 clear_live_region 精确清除。
        self.live_region_terminal_width = terminal_width;
        Ok(())
    }

    fn disable_keyboard_enhancement(&mut self) {
        if self.keyboard_enhancement_enabled {
            let _ = execute!(self.stdout, PopKeyboardEnhancementFlags);
            self.keyboard_enhancement_enabled = false;
        }
    }

    fn disable_modify_other_keys(&mut self) {
        if self.modify_other_keys_enabled {
            let _ = execute!(self.stdout, DisableModifyOtherKeys);
            self.modify_other_keys_enabled = false;
        }
    }

    fn disable_bracketed_paste(&mut self) {
        if self.bracketed_paste_enabled {
            let _ = execute!(self.stdout, DisableBracketedPaste);
            self.bracketed_paste_enabled = false;
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        // 用 live region 被绘制时的宽度清除它（现查 size 可能已因 resize 改变，导致行数算错）。
        let _ = clear_live_region(
            &mut self.stdout,
            &self.live_region_line_widths,
            self.live_region_cursor,
            self.live_region_terminal_width,
        );
        self.live_region_line_widths.clear();
        self.live_region_cursor = None;
        self.disable_keyboard_enhancement();
        self.disable_modify_other_keys();
        self.disable_bracketed_paste();
        drain_pending_terminal_events();
        let _ = disable_raw_mode();
        let _ = execute!(
            self.stdout,
            // 兜底离开同步更新态：若某次 draw 在 Begin/End 之间 panic unwind，
            // 这里确保退出时一定发出 ESC[?2026l，避免终端卡在同步更新、后续输出不可见。
            EndSynchronizedUpdate,
            EnableLineWrap,
            ResetColor,
            SetAttribute(Attribute::Reset),
            Show
        );
        let _ = self.stdout.flush();
    }
}

fn render_synchronized_frame(
    frame: &mut Vec<u8>,
    render: impl FnOnce(&mut Vec<u8>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    execute!(frame, BeginSynchronizedUpdate).context("开始 TUI 同步帧失败")?;
    let render_result = render(frame);
    let end_result = execute!(frame, EndSynchronizedUpdate).context("结束 TUI 同步帧失败");
    render_result.and(end_result)
}

fn commit_frame(stdout: &mut impl Write, frame: &[u8]) -> io::Result<()> {
    stdout.write_all(frame)?;
    stdout.flush()
}

fn write_line(stdout: &mut impl Write, line: &Line<'static>) -> io::Result<()> {
    execute!(stdout, MoveToColumn(0))?;
    apply_style(stdout, line.style)?;
    execute!(stdout, Clear(ClearType::CurrentLine))?;
    let spans = trailing_space_trimmed_spans(line);
    for span in spans {
        write_span(stdout, line.style.patch(span.style), span.content.as_ref())?;
    }
    execute!(
        stdout,
        ResetColor,
        SetAttribute(Attribute::Reset),
        Print("\r\n")
    )
}

fn clear_screen_with_surface_background(stdout: &mut impl Write) -> io::Result<()> {
    set_surface_background(stdout)?;
    execute!(
        stdout,
        Clear(ClearType::All),
        Clear(ClearType::Purge),
        MoveTo(0, 0)
    )
}

fn set_surface_background(stdout: &mut impl Write) -> io::Result<()> {
    if let Color::Rgb(r, g, b) = SURFACE_BG {
        // surface 背景承担遮住终端原底色的布局职责，不能被 NO_COLOR 抑制。
        write!(stdout, "\x1b[48;2;{r};{g};{b}m")?;
    } else if let Some(bg) = to_crossterm_color(SURFACE_BG) {
        execute!(stdout, SetBackgroundColor(bg))?;
    }
    Ok(())
}

fn write_live_region_in_place(
    stdout: &mut impl Write,
    lines: &[Line<'static>],
    previous_rows: usize,
) -> io::Result<()> {
    let desired_rows = lines.len().max(1);
    ensure_live_region_rows(stdout, previous_rows, desired_rows)?;
    for (index, line) in lines.iter().enumerate() {
        if index > 0 {
            execute!(stdout, MoveDown(1))?;
        }
        write_line_in_place(stdout, line)?;
    }
    Ok(())
}

fn ensure_live_region_rows(
    stdout: &mut impl Write,
    previous_rows: usize,
    desired_rows: usize,
) -> io::Result<()> {
    if desired_rows <= previous_rows {
        return Ok(());
    }

    if previous_rows > 0 {
        execute!(
            stdout,
            MoveDown(u16::try_from(previous_rows - 1).unwrap_or(u16::MAX))
        )?;
    }
    for _ in 0..desired_rows.saturating_sub(previous_rows) {
        execute!(stdout, Print("\r\n"))?;
    }
    if desired_rows > 1 {
        execute!(
            stdout,
            MoveUp(u16::try_from(desired_rows - 1).unwrap_or(u16::MAX))
        )?;
    }
    execute!(stdout, MoveToColumn(0))
}

fn write_line_in_place(stdout: &mut impl Write, line: &Line<'static>) -> io::Result<()> {
    execute!(stdout, MoveToColumn(0))?;
    apply_style(stdout, line.style)?;
    execute!(stdout, Clear(ClearType::CurrentLine))?;
    let spans = trailing_space_trimmed_spans(line);
    for span in spans {
        write_span(stdout, line.style.patch(span.style), span.content.as_ref())?;
    }
    execute!(stdout, ResetColor, SetAttribute(Attribute::Reset))
}

fn move_cursor_after_appended_live_region(
    stdout: &mut impl Write,
    live_line_count: usize,
    cursor: Option<(u16, u16)>,
) -> io::Result<()> {
    if let Some((column, row)) = cursor {
        let up = live_line_count.saturating_sub(usize::from(row));
        if up > 0 {
            execute!(stdout, MoveUp(u16::try_from(up).unwrap_or(u16::MAX)))?;
        }
        execute!(stdout, MoveToColumn(column), Show)?;
    } else if live_line_count > 0 {
        // None（无 composer 光标）：append 末行的 \r\n 把光标留在 region 下方一行。上移回底行，
        // 让下一帧 clear_live_region 有确定且在屏内的起点（满屏时不依赖会被 clamp 的越界位置）。
        execute!(stdout, MoveUp(1), MoveToColumn(0), Hide)?;
    }
    Ok(())
}

fn move_cursor_after_in_place_live_region(
    stdout: &mut impl Write,
    live_line_count: usize,
    reserved_rows: usize,
    cursor: Option<(u16, u16)>,
) -> io::Result<()> {
    let current_row = live_line_count.saturating_sub(1);
    if let Some((column, row)) = cursor {
        let target_row = usize::from(row);
        if current_row > target_row {
            execute!(
                stdout,
                MoveUp(u16::try_from(current_row - target_row).unwrap_or(u16::MAX))
            )?;
        } else if target_row > current_row {
            execute!(
                stdout,
                MoveDown(u16::try_from(target_row - current_row).unwrap_or(u16::MAX))
            )?;
        }
        execute!(stdout, MoveToColumn(column), Show)?;
    } else {
        // None：落到 region 底行（reserved 区最后一行）而非其下方，避免满屏时 MoveDown 越界被
        // clamp，从而给下一帧 clear_live_region 一个确定且在屏内的起点。
        let bottom_row = reserved_rows.saturating_sub(1);
        if bottom_row > current_row {
            execute!(
                stdout,
                MoveDown(u16::try_from(bottom_row - current_row).unwrap_or(u16::MAX))
            )?;
        }
        execute!(stdout, MoveToColumn(0), Hide)?;
    }
    Ok(())
}

fn trailing_space_trimmed_spans(line: &Line<'static>) -> Vec<ratatui::text::Span<'static>> {
    let mut spans = Vec::new();
    let mut trimming = true;
    for span in line.spans.iter().rev() {
        let content = span.content.as_ref();
        let trimmed = if trimming {
            content.trim_end_matches(' ')
        } else {
            content
        };
        if trimmed.is_empty() {
            continue;
        }
        trimming = false;
        spans.push(ratatui::text::Span::styled(trimmed.to_string(), span.style));
    }
    spans.reverse();
    spans
}

/// 与 `trailing_space_trimmed_spans` 同语义（尾部跳过纯空格 span、首个非空 span 去尾随空格），
/// 但只累加显示宽度、不构造 Span/Line/String —— 这是每帧重绘的热路径，避免一次性分配。
fn printed_line_width(line: &Line<'static>) -> usize {
    let mut width = 0usize;
    let mut trimming = true;
    for span in line.spans.iter().rev() {
        let content = span.content.as_ref();
        let counted = if trimming {
            content.trim_end_matches(' ')
        } else {
            content
        };
        if counted.is_empty() {
            continue;
        }
        trimming = false;
        width = width.saturating_add(UnicodeWidthStr::width(
            terminal_safe_content(counted).as_ref(),
        ));
    }
    width
}

fn write_span(stdout: &mut impl Write, style: Style, content: &str) -> io::Result<()> {
    apply_style(stdout, style)?;
    // 业务文本（尤其是文件名）可包含 ESC、CR 或 Tab；绝不能把它们原样交给终端解释。
    // 保持这个统一输出边界，可覆盖补全、composer、历史消息与模型/工具文本。
    let safe_content = terminal_safe_content(content);
    execute!(stdout, Print(safe_content.as_ref()))?;
    execute!(stdout, ResetColor, SetAttribute(Attribute::Reset))
}

/// 移除会被终端解释的 C0/C1 控制字符，其他 Unicode 文本保持原样。
///
/// 普通文本走借用快路径，不为每次绘制分配；控制字符在 Unicode 宽度模型中为零宽，
/// 因此删除后仍与 composer 的光标和折行坐标一致。
fn terminal_safe_content(content: &str) -> Cow<'_, str> {
    let Some((first_control_byte, _)) = content.char_indices().find(|(_, ch)| ch.is_control())
    else {
        return Cow::Borrowed(content);
    };

    let mut safe = String::with_capacity(content.len());
    safe.push_str(&content[..first_control_byte]);
    safe.extend(
        content[first_control_byte..]
            .chars()
            .filter(|ch| !ch.is_control()),
    );
    Cow::Owned(safe)
}

fn apply_style(stdout: &mut impl Write, style: Style) -> io::Result<()> {
    if let Some(fg) = style.fg.and_then(to_crossterm_color) {
        execute!(stdout, SetForegroundColor(fg))?;
    }
    if let Some(bg) = style.bg {
        if bg == SURFACE_BG
            || (Colored::ansi_color_disabled_memoized()
                && matches!(bg, DIFF_ADDED_BG | DIFF_REMOVED_BG))
        {
            set_surface_background(stdout)?;
        } else if let Some(bg) = to_crossterm_color(bg) {
            execute!(stdout, SetBackgroundColor(bg))?;
        }
    }
    if style.add_modifier.contains(Modifier::BOLD) {
        execute!(stdout, SetAttribute(Attribute::Bold))?;
    }
    if style.add_modifier.contains(Modifier::ITALIC) {
        execute!(stdout, SetAttribute(Attribute::Italic))?;
    }
    if style.add_modifier.contains(Modifier::UNDERLINED) {
        execute!(stdout, SetAttribute(Attribute::Underlined))?;
    }
    Ok(())
}

fn to_crossterm_color(color: Color) -> Option<CrosstermColor> {
    match color {
        Color::Reset => Some(CrosstermColor::Reset),
        Color::Black => Some(CrosstermColor::Black),
        Color::Red => Some(CrosstermColor::DarkRed),
        Color::Green => Some(CrosstermColor::DarkGreen),
        Color::Yellow => Some(CrosstermColor::DarkYellow),
        Color::Blue => Some(CrosstermColor::DarkBlue),
        Color::Magenta => Some(CrosstermColor::DarkMagenta),
        Color::Cyan => Some(CrosstermColor::DarkCyan),
        Color::Gray => Some(CrosstermColor::Grey),
        Color::DarkGray => Some(CrosstermColor::DarkGrey),
        Color::LightRed => Some(CrosstermColor::Red),
        Color::LightGreen => Some(CrosstermColor::Green),
        Color::LightYellow => Some(CrosstermColor::Yellow),
        Color::LightBlue => Some(CrosstermColor::Blue),
        Color::LightMagenta => Some(CrosstermColor::Magenta),
        Color::LightCyan => Some(CrosstermColor::Cyan),
        Color::White => Some(CrosstermColor::White),
        Color::Rgb(r, g, b) => Some(CrosstermColor::Rgb { r, g, b }),
        Color::Indexed(index) => Some(CrosstermColor::AnsiValue(index)),
    }
}

fn clear_live_region(
    stdout: &mut impl Write,
    line_widths: &[usize],
    cursor: Option<(usize, usize)>,
    terminal_width: u16,
) -> io::Result<()> {
    if line_widths.is_empty() {
        return Ok(());
    }

    let terminal_width = usize::from(terminal_width.max(1));
    let height = live_region_visual_height(line_widths, terminal_width);
    // 先把光标移到 live region 的**底行**（而非其下方一行），再自下而上逐行清除。
    //
    // 关键：满屏时 live region 底部就是屏幕最后一行，若移到“下方一行”会越界、被终端 clamp
    // 回屏底（MoveDown/CUD 只停在底行、不滚屏），导致随后整段清除偏移一行——漏清最底部的
    // footer/状态栏行、并误清上方一行历史。流式重绘走就地路径不会 append 覆盖这些残留行，
    // 于是状态栏被“显示两次”。统一落到底行（始终在屏内）即可消除该偏移。
    //
    // 上一帧物理光标相对 region 顶行的位置 physical_row：
    // - 有 composer 光标（Some）：停在光标可视行（region 中部）。
    // - 无光标（None，如 session picker / finalizing）：move_cursor_* 已把光标收尾到 region
    //   **底行**（始终在屏内、无 clamp 歧义），故为 bottom_row。
    let physical_row = match cursor {
        Some((column, row)) => {
            live_region_cursor_visual_row(line_widths, column, row, terminal_width)
        }
        None => height.saturating_sub(1),
    };
    let bottom_row = height.saturating_sub(1);
    if physical_row > bottom_row {
        // None：光标在 region 下方，上移回底行。
        execute!(
            stdout,
            MoveUp(u16::try_from(physical_row - bottom_row).unwrap_or(u16::MAX))
        )?;
    } else if bottom_row > physical_row {
        // Some：光标在 region 中部，下移到底行。
        execute!(
            stdout,
            MoveDown(u16::try_from(bottom_row - physical_row).unwrap_or(u16::MAX))
        )?;
    }
    // 自底行起：先清当前行，再上移；最后一次不再上移，光标停在 region 顶行（与原行为一致）。
    for row_from_bottom in 0..height {
        set_surface_background(stdout)?;
        execute!(stdout, MoveToColumn(0), Clear(ClearType::CurrentLine))?;
        if row_from_bottom + 1 < height {
            execute!(stdout, MoveUp(1))?;
        }
    }
    Ok(())
}

fn live_region_visual_height(line_widths: &[usize], terminal_width: usize) -> usize {
    line_widths
        .iter()
        .map(|width| visual_rows_for_width(*width, terminal_width))
        .sum()
}

fn live_region_cursor_visual_row(
    line_widths: &[usize],
    cursor_column: usize,
    cursor_row: usize,
    terminal_width: usize,
) -> usize {
    let before_cursor = line_widths
        .iter()
        .take(cursor_row)
        .map(|width| visual_rows_for_width(*width, terminal_width))
        .sum::<usize>();
    before_cursor.saturating_add(cursor_column / terminal_width)
}

fn visual_rows_for_width(line_width: usize, terminal_width: usize) -> usize {
    line_width.max(1).div_ceil(terminal_width.max(1))
}

fn enable_keyboard_enhancement() -> bool {
    execute!(
        io::stdout(),
        DisableModifyOtherKeys,
        PushKeyboardEnhancementFlags(
            KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                | KeyboardEnhancementFlags::REPORT_EVENT_TYPES
                | KeyboardEnhancementFlags::REPORT_ALTERNATE_KEYS
        )
    )
    .is_ok()
}

fn enable_modify_other_keys_if_needed() -> bool {
    if !tmux_should_enable_modify_other_keys() {
        return false;
    }

    execute!(io::stdout(), EnableModifyOtherKeys).is_ok()
}

fn tmux_should_enable_modify_other_keys() -> bool {
    tmux_should_enable_modify_other_keys_for(
        running_in_tmux_session(),
        read_tmux_extended_keys_format().as_deref(),
    )
}

fn tmux_should_enable_modify_other_keys_for(
    running_in_tmux_session: bool,
    extended_keys_format: Option<&str>,
) -> bool {
    running_in_tmux_session && matches!(extended_keys_format, Some("csi-u") | None)
}

fn running_in_tmux_session() -> bool {
    std::env::var_os("TMUX").is_some() || std::env::var_os("TMUX_PANE").is_some()
}

fn read_tmux_extended_keys_format() -> Option<String> {
    for args in [
        ["display-message", "-p", "#{extended-keys-format}"],
        ["show-options", "-gqv", "extended-keys-format"],
    ] {
        let output = std::process::Command::new("tmux")
            .args(args)
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .ok()?;

        if !output.status.success() {
            continue;
        }

        if let Some(value) = String::from_utf8(output.stdout)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            return Some(value);
        }
    }

    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EnableModifyOtherKeys;

impl Command for EnableModifyOtherKeys {
    fn write_ansi(&self, f: &mut impl fmt::Write) -> fmt::Result {
        f.write_str("\x1b[>4;2m")
    }

    #[cfg(windows)]
    fn execute_winapi(&self) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "ModifyOtherKeys enable is not implemented for the legacy Windows API",
        ))
    }

    #[cfg(windows)]
    fn is_ansi_code_supported(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DisableModifyOtherKeys;

impl Command for DisableModifyOtherKeys {
    fn write_ansi(&self, f: &mut impl fmt::Write) -> fmt::Result {
        f.write_str("\x1b[>4;0m")
    }

    #[cfg(windows)]
    fn execute_winapi(&self) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "ModifyOtherKeys reset is not implemented for the legacy Windows API",
        ))
    }

    #[cfg(windows)]
    fn is_ansi_code_supported(&self) -> bool {
        false
    }
}

pub(super) fn spawn_terminal_event_reader(
    terminal_tx: mpsc::UnboundedSender<TerminalEvent>,
    stop: Arc<AtomicBool>,
) {
    tokio::task::spawn_blocking(move || {
        while !stop.load(Ordering::SeqCst) {
            match crossterm::event::poll(Duration::from_millis(50)) {
                Ok(true) => match crossterm::event::read() {
                    Ok(event) => match read_terminal_event_batch(event) {
                        Ok(events) => {
                            for event in coalesce_paste_burst(events) {
                                if terminal_tx.send(TerminalEvent::Input(event)).is_err() {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            let _ = terminal_tx.send(TerminalEvent::Error(e));
                            break;
                        }
                    },
                    Err(e) => {
                        let _ = terminal_tx.send(TerminalEvent::Error(e.to_string()));
                        break;
                    }
                },
                Ok(false) => {}
                Err(e) => {
                    let _ = terminal_tx.send(TerminalEvent::Error(e.to_string()));
                    break;
                }
            }
        }
    });
}

fn read_terminal_event_batch(first: Event) -> Result<Vec<Event>, String> {
    let mut events = vec![first];
    if event_to_plain_paste_char(&events[0]).is_none() {
        return Ok(events);
    }

    while events.len() < PASTE_BURST_MAX_EVENTS
        && crossterm::event::poll(PASTE_BURST_CHAR_INTERVAL).map_err(|e| e.to_string())?
    {
        let next = crossterm::event::read().map_err(|e| e.to_string())?;
        let is_plain = event_to_plain_paste_char(&next).is_some();
        events.push(next);
        if !is_plain {
            break;
        }
    }
    Ok(events)
}

fn coalesce_paste_burst(events: Vec<Event>) -> Vec<Event> {
    if events.len() < PASTE_BURST_MIN_EVENTS {
        return events;
    }

    let mut pasted = String::new();
    for event in &events {
        let Some(ch) = event_to_plain_paste_char(event) else {
            return events;
        };
        pasted.push(ch);
    }

    let has_newline = pasted.contains('\n');
    let is_short_single_line =
        !has_newline && pasted.chars().count() < PASTE_BURST_MIN_SINGLE_LINE_CHARS;
    let is_short_command_submit = pasted.ends_with('\n')
        && pasted[..pasted.len().saturating_sub(1)]
            .find('\n')
            .is_none()
        && pasted.chars().count() < PASTE_BURST_MIN_SINGLE_LINE_CHARS;
    if is_short_single_line || is_short_command_submit {
        return events;
    }

    vec![Event::Paste(pasted)]
}

fn event_to_plain_paste_char(event: &Event) -> Option<char> {
    let Event::Key(key) = event else {
        return None;
    };
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return None;
    }
    match key.code {
        KeyCode::Char(ch)
            if !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            Some(ch)
        }
        KeyCode::Enter if key.modifiers == KeyModifiers::NONE => Some('\n'),
        _ => None,
    }
}

pub(super) struct TerminalReaderGuard {
    stop: Arc<AtomicBool>,
}

impl TerminalReaderGuard {
    pub(super) fn new(stop: Arc<AtomicBool>) -> Self {
        Self { stop }
    }
}

impl Drop for TerminalReaderGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

fn drain_pending_terminal_events() {
    while crossterm::event::poll(Duration::from_millis(0)).unwrap_or(false) {
        let _ = crossterm::event::read();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
    use crossterm::style::Colored;
    use crossterm::terminal::{BeginSynchronizedUpdate, EndSynchronizedUpdate};
    use crossterm::Command;
    use ratatui::style::{Color, Style};
    use ratatui::text::{Line, Span};

    use super::{
        clear_screen_with_surface_background, coalesce_paste_burst, commit_frame,
        live_region_cursor_visual_row, live_region_visual_height, printed_line_width,
        render_synchronized_frame, set_surface_background, terminal_safe_content,
        tmux_should_enable_modify_other_keys_for, trailing_space_trimmed_spans,
        visual_rows_for_width, write_line, DisableModifyOtherKeys, EnableModifyOtherKeys,
    };
    use crate::session_tui::theme::{DIFF_ADDED_BG, DIFF_REMOVED_BG};

    fn ansi_for(command: impl Command) -> String {
        let mut output = String::new();
        command.write_ansi(&mut output).unwrap();
        output
    }

    #[derive(Default)]
    struct RecordingWriter {
        bytes: Vec<u8>,
        write_calls: usize,
        flush_calls: usize,
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.write_calls += 1;
            self.bytes.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flush_calls += 1;
            Ok(())
        }
    }

    #[test]
    fn synchronized_frame_contains_complete_render_between_markers() {
        let mut frame = Vec::new();

        render_synchronized_frame(&mut frame, |frame| {
            write!(frame, "first\r\nsecond")?;
            Ok(())
        })
        .unwrap();

        let ansi = String::from_utf8(frame).unwrap();
        assert_eq!(
            ansi,
            format!(
                "{}first\r\nsecond{}",
                ansi_for(BeginSynchronizedUpdate),
                ansi_for(EndSynchronizedUpdate)
            )
        );
    }

    #[test]
    fn synchronized_frame_closes_marker_when_rendering_fails() {
        let mut frame = Vec::new();

        let error = render_synchronized_frame(&mut frame, |frame| {
            write!(frame, "partial")?;
            anyhow::bail!("render failed");
        })
        .unwrap_err();

        assert!(error.to_string().contains("render failed"));
        assert!(String::from_utf8(frame)
            .unwrap()
            .ends_with(&ansi_for(EndSynchronizedUpdate)));
    }

    #[test]
    fn complete_frame_is_committed_with_one_write_and_one_flush() {
        let frame = b"\x1b[?2026hfirst\r\nsecond\x1b[?2026l";
        let mut output = RecordingWriter::default();

        commit_frame(&mut output, frame).unwrap();

        assert_eq!(output.bytes, frame);
        assert_eq!(output.write_calls, 1);
        assert_eq!(output.flush_calls, 1);
    }

    #[test]
    fn tmux_modify_other_keys_requests_csi_u_or_unknown_format() {
        assert!(tmux_should_enable_modify_other_keys_for(
            true,
            Some("csi-u")
        ));
        assert!(tmux_should_enable_modify_other_keys_for(true, None));
        assert!(!tmux_should_enable_modify_other_keys_for(
            true,
            Some("xterm")
        ));
        assert!(!tmux_should_enable_modify_other_keys_for(
            false,
            Some("csi-u")
        ));
        assert!(!tmux_should_enable_modify_other_keys_for(false, None));
    }

    #[test]
    fn modify_other_keys_commands_emit_xterm_keyboard_reporting_sequences() {
        assert_eq!(ansi_for(EnableModifyOtherKeys), "\x1b[>4;2m");
        assert_eq!(ansi_for(DisableModifyOtherKeys), "\x1b[>4;0m");
    }

    #[test]
    fn surface_background_is_set_before_clearing_screen() {
        let mut output = Vec::new();

        clear_screen_with_surface_background(&mut output).unwrap();

        let ansi = String::from_utf8(output).unwrap();
        let background = ansi
            .find("\x1b[48;2;250;248;242m")
            .expect("Clear should set ACN surface background first");
        let clear = ansi.find("\x1b[2J").expect("Clear should erase screen");
        assert!(background < clear);
        assert!(ansi.contains("\x1b[3J"));
    }

    #[test]
    fn surface_background_command_uses_theme_rgb_even_when_no_color_is_set() {
        let mut output = Vec::new();

        set_surface_background(&mut output).unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "\x1b[48;2;250;248;242m");
    }

    #[test]
    fn changed_line_renderer_honors_color_policy_before_full_line_clear() {
        for (background, ansi_background) in [
            (DIFF_ADDED_BG, "\x1b[48;2;225;242;228m"),
            (DIFF_REMOVED_BG, "\x1b[48;2;249;228;225m"),
        ] {
            let line = Line::from(Span::raw("+ changed")).style(Style::default().bg(background));
            let mut output = Vec::new();

            write_line(&mut output, &line).unwrap();

            let ansi = String::from_utf8(output).unwrap();
            let clear_at = ansi.find("\x1b[2K").expect("Diff 行应清除整条物理行");
            if Colored::ansi_color_disabled_memoized() {
                assert!(!ansi.contains(ansi_background));
                let surface_at = ansi
                    .find("\x1b[48;2;250;248;242m")
                    .expect("无色模式下 diff 行应回退到 surface 背景");
                assert!(surface_at < clear_at);
            } else {
                let background_at = ansi
                    .find(ansi_background)
                    .expect("彩色模式下 diff 行应设置语义背景色");
                assert!(background_at < clear_at);
            }
            assert!(ansi.contains("+ changed"));
            assert!(ansi.ends_with("\r\n"));
        }
    }

    #[test]
    fn trailing_space_trim_keeps_internal_spacing_and_visible_styles() {
        let prompt_style = Style::default().fg(Color::Blue);
        let text_style = Style::default().fg(Color::White);
        let padding_style = Style::default().bg(Color::Gray);
        let line = Line::from(vec![
            Span::styled("›  ", prompt_style),
            Span::styled("hello", text_style),
            Span::styled("   ", padding_style),
        ]);

        let spans = trailing_space_trimmed_spans(&line);

        assert_eq!(
            spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect::<Vec<_>>(),
            vec!["›  ", "hello"]
        );
        assert_eq!(spans[0].style, prompt_style);
        assert_eq!(spans[1].style, text_style);
    }

    #[test]
    fn trailing_space_trim_omits_all_padding_line() {
        let line = Line::from(vec![Span::styled(
            "     ",
            Style::default().bg(Color::Gray),
        )]);

        assert!(trailing_space_trimmed_spans(&line).is_empty());
    }

    #[test]
    fn printed_line_width_ignores_trimmed_padding() {
        let line = Line::from(vec![
            Span::raw("› "),
            Span::raw("hello"),
            Span::styled("          ", Style::default().bg(Color::Gray)),
        ]);

        assert_eq!(printed_line_width(&line), 7);
    }

    #[test]
    fn terminal_output_removes_control_characters_without_changing_printable_path_text() {
        let unsafe_path = "references/evil\x1b[2J\t\r\x07\u{009b}file.rs";
        let safe_path = "references/evil[2Jfile.rs";

        assert_eq!(terminal_safe_content(unsafe_path), safe_path);
        assert_eq!(terminal_safe_content("资料/普通😀.rs"), "资料/普通😀.rs");

        let line = Line::from(unsafe_path);
        assert_eq!(
            printed_line_width(&line),
            unicode_width::UnicodeWidthStr::width(safe_path)
        );

        let mut output = Vec::new();
        write_line(&mut output, &line).unwrap();
        let ansi = String::from_utf8(output).unwrap();
        assert!(ansi.contains(safe_path));
        assert!(!ansi.contains(unsafe_path));
    }

    #[test]
    fn live_region_clear_height_accounts_for_resize_reflow() {
        assert_eq!(visual_rows_for_width(0, 80), 1);
        assert_eq!(visual_rows_for_width(79, 80), 1);
        assert_eq!(visual_rows_for_width(80, 80), 1);
        assert_eq!(visual_rows_for_width(81, 80), 2);

        let old_line_widths = [95, 2, 95];
        assert_eq!(live_region_visual_height(&old_line_widths, 80), 5);
        assert_eq!(live_region_cursor_visual_row(&old_line_widths, 0, 1, 80), 2);
    }

    #[test]
    fn fast_plain_key_batch_coalesces_into_paste_event() {
        let events = vec![
            Event::Key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('!'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('u'), KeyModifiers::NONE)),
        ];

        assert_eq!(
            coalesce_paste_burst(events),
            vec![Event::Paste("//!\nu".into())]
        );
    }

    #[test]
    fn modified_key_batch_is_not_coalesced_as_paste() {
        let events = vec![
            Event::Key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
        ];

        assert!(matches!(
            coalesce_paste_burst(events).last(),
            Some(Event::Key(key)) if key.modifiers.contains(KeyModifiers::CONTROL)
        ));
    }

    #[test]
    fn short_slash_command_submit_is_not_coalesced_as_paste() {
        let events = vec![
            Event::Key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('h'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE)),
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
        ];

        assert!(matches!(
            coalesce_paste_burst(events).last(),
            Some(Event::Key(key)) if key.code == KeyCode::Enter
        ));
    }
}
