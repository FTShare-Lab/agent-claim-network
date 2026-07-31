//! running turn 的微型俄罗斯方块隐喻动画。
//!
//! 本模块维护 4x6 逻辑棋盘，并用半块字符压缩成 4x3 终端图形。
//! 掉落脚本固定使用标准四连块的旋转形态；主 TUI 只关心 tick 与渲染结果。
//! 渲染时下半格颜色一律由背景色承担（满块直接背景铺满），
//! 避免行距 >1 的终端（如 macOS Terminal.app）在块与块之间露出底色细缝。

use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

use super::theme::{BLUE_FG, SURFACE_BG};

pub(super) const BOARD_WIDTH: usize = 4;
pub(super) const BOARD_HEIGHT: usize = 6;
pub(super) const TURN_ANIMATION_MIN_WIDTH: u16 = 48;
pub(super) const TURN_ANIMATION_RENDER_ROWS: usize = BOARD_HEIGHT / 2;

const HIGHLIGHT_TICKS: u8 = 4;
const FALLING_FG: Color = Color::Rgb(204, 74, 66);
const CLEARING_FG: Color = Color::Rgb(136, 136, 136);

#[derive(Debug, Clone, Default)]
pub(super) struct TurnAnimationState {
    settled: Board,
    active: Option<ActiveDrop>,
    completed_turns: usize,
}

type Board = [[bool; BOARD_WIDTH]; BOARD_HEIGHT];

#[derive(Debug, Clone)]
struct ActiveDrop {
    script: DropScript,
    row: i8,
    phase: DropPhase,
    finalizing: bool,
}

#[derive(Debug, Clone, Copy)]
enum DropPhase {
    Falling,
    Highlight {
        remaining_ticks: u8,
        clear_rows: [bool; BOARD_HEIGHT],
    },
}

#[derive(Debug, Clone, Copy)]
struct DropScript {
    name: &'static str,
    kind: &'static str,
    x: i8,
    cells: &'static [(i8, i8)],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Paint {
    Settled,
    Falling,
    Clearing,
}

const OPENING_L: &[(i8, i8)] = &[(0, 0), (1, 0), (2, 0), (2, 1)];
const J_HOOK: &[(i8, i8)] = &[(0, 0), (0, 1), (0, 2), (1, 2)];
const Z_RIGHT: &[(i8, i8)] = &[(0, 1), (1, 0), (1, 1), (2, 0)];
const T_LEFT: &[(i8, i8)] = &[(0, 1), (1, 0), (1, 1), (2, 1)];
const I_VERTICAL: &[(i8, i8)] = &[(0, 0), (1, 0), (2, 0), (3, 0)];
const S_MID: &[(i8, i8)] = &[(0, 1), (0, 2), (1, 0), (1, 1)];
const L_LEFT: &[(i8, i8)] = &[(0, 0), (0, 1), (1, 1), (2, 1)];
const T_FLAT: &[(i8, i8)] = &[(0, 1), (1, 0), (1, 1), (1, 2)];
const L_MID: &[(i8, i8)] = &[(0, 0), (0, 1), (0, 2), (1, 0)];
const J_FLAT: &[(i8, i8)] = &[(0, 1), (1, 1), (2, 0), (2, 1)];

const FIRST_SCRIPT: DropScript = DropScript {
    name: "opening_l",
    kind: "L",
    x: 0,
    cells: OPENING_L,
};

const FOLLOWUP_SCRIPTS: [DropScript; 16] = [
    DropScript {
        name: "j_hook",
        kind: "J",
        x: 1,
        cells: J_HOOK,
    },
    DropScript {
        name: "z_right",
        kind: "Z",
        x: 2,
        cells: Z_RIGHT,
    },
    DropScript {
        name: "t_left",
        kind: "T",
        x: 0,
        cells: T_LEFT,
    },
    DropScript {
        name: "z_right_repeat",
        kind: "Z",
        x: 2,
        cells: Z_RIGHT,
    },
    DropScript {
        name: "i_left",
        kind: "I",
        x: 0,
        cells: I_VERTICAL,
    },
    DropScript {
        name: "s_mid",
        kind: "S",
        x: 1,
        cells: S_MID,
    },
    DropScript {
        name: "l_left",
        kind: "L",
        x: 0,
        cells: L_LEFT,
    },
    DropScript {
        name: "t_right",
        kind: "T",
        x: 2,
        cells: T_LEFT,
    },
    DropScript {
        name: "t_mid",
        kind: "T",
        x: 1,
        cells: T_LEFT,
    },
    DropScript {
        name: "i_left_repeat",
        kind: "I",
        x: 0,
        cells: I_VERTICAL,
    },
    DropScript {
        name: "l_mid",
        kind: "L",
        x: 1,
        cells: L_MID,
    },
    DropScript {
        name: "t_flat",
        kind: "T",
        x: 1,
        cells: T_FLAT,
    },
    DropScript {
        name: "i_right",
        kind: "I",
        x: 3,
        cells: I_VERTICAL,
    },
    DropScript {
        name: "j_left",
        kind: "J",
        x: 0,
        cells: J_HOOK,
    },
    DropScript {
        name: "j_flat",
        kind: "J",
        x: 0,
        cells: J_FLAT,
    },
    DropScript {
        name: "i_left_cycle",
        kind: "I",
        x: 0,
        cells: I_VERTICAL,
    },
];

impl TurnAnimationState {
    pub(super) fn begin_turn(&mut self) {
        self.complete_finalizing_drop();
        let script = self.next_script();
        debug_assert!(!script.name.is_empty());
        debug_assert!(matches!(
            script.kind,
            "I" | "O" | "T" | "L" | "J" | "S" | "Z"
        ));
        self.active = Some(ActiveDrop {
            script,
            row: starting_row(script),
            phase: DropPhase::Falling,
            finalizing: false,
        });
    }

    pub(super) fn finish_success(&mut self) {
        if let Some(active) = &mut self.active {
            active.finalizing = true;
        }
    }

    pub(super) fn cancel_turn(&mut self) {
        self.active = None;
    }

    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(super) fn is_active(&self) -> bool {
        self.active.is_some()
    }

    pub(super) fn has_visible_board(&self) -> bool {
        self.active.is_some() || self.settled.iter().flatten().any(|filled| *filled)
    }

    pub(super) fn tick(&mut self) -> bool {
        let Some(mut active) = self.active.take() else {
            return false;
        };
        match active.phase {
            DropPhase::Falling => {
                let next_row = active.row.saturating_add(1);
                if can_place(&self.settled, active.script, next_row) {
                    active.row = next_row;
                } else {
                    let clear_rows =
                        clear_rows_after_drop(&self.settled, active.script, active.row);
                    active.phase = DropPhase::Highlight {
                        remaining_ticks: HIGHLIGHT_TICKS,
                        clear_rows,
                    };
                }
                self.active = Some(active);
            }
            DropPhase::Highlight {
                remaining_ticks,
                clear_rows,
            } => {
                if remaining_ticks > 1 {
                    active.phase = DropPhase::Highlight {
                        remaining_ticks: remaining_ticks.saturating_sub(1),
                        clear_rows,
                    };
                    self.active = Some(active);
                } else if active.finalizing {
                    self.commit_drop(active.script, active.row, clear_rows);
                } else {
                    active.row = starting_row(active.script);
                    active.phase = DropPhase::Falling;
                    self.active = Some(active);
                }
            }
        }
        true
    }

    pub(super) fn tick_if_visible(&mut self, width: u16, height_budget: usize) -> bool {
        if width < TURN_ANIMATION_MIN_WIDTH || height_budget < TURN_ANIMATION_RENDER_ROWS {
            return false;
        }
        self.tick()
    }

    pub(super) fn render_lines(&self, width: u16, height_budget: usize) -> Vec<Line<'static>> {
        if width < TURN_ANIMATION_MIN_WIDTH
            || height_budget < TURN_ANIMATION_RENDER_ROWS
            || !self.has_visible_board()
        {
            return Vec::new();
        }
        let cells = self.visible_cells();
        let mut lines = Vec::with_capacity(TURN_ANIMATION_RENDER_ROWS);
        for row in 0..TURN_ANIMATION_RENDER_ROWS {
            let top = row * 2;
            let bottom = top + 1;
            let mut line = Line::default();
            #[allow(
                clippy::needless_range_loop,
                reason = "同一列索引同时读取上下两行 cells"
            )]
            for column in 0..BOARD_WIDTH {
                line.push_span(cell_span(cells[top][column], cells[bottom][column]));
            }
            lines.push(line);
        }
        lines
    }

    fn next_script(&self) -> DropScript {
        if self.completed_turns == 0 {
            return FIRST_SCRIPT;
        }
        FOLLOWUP_SCRIPTS[(self.completed_turns - 1) % FOLLOWUP_SCRIPTS.len()]
    }

    fn visible_cells(&self) -> [[Option<Paint>; BOARD_WIDTH]; BOARD_HEIGHT] {
        let mut cells = [[None; BOARD_WIDTH]; BOARD_HEIGHT];
        for (row, settled_row) in self.settled.iter().enumerate() {
            for (column, filled) in settled_row.iter().copied().enumerate() {
                if filled {
                    cells[row][column] = Some(Paint::Settled);
                }
            }
        }
        if let Some(active) = &self.active {
            for (row, column) in active.cells() {
                cells[row][column] = Some(Paint::Falling);
            }
            if let DropPhase::Highlight { clear_rows, .. } = active.phase {
                for (row, clear) in clear_rows.iter().copied().enumerate() {
                    if clear {
                        #[allow(clippy::needless_range_loop, reason = "按列写入固定宽度行")]
                        for column in 0..BOARD_WIDTH {
                            cells[row][column] = Some(Paint::Clearing);
                        }
                    }
                }
            }
        }
        cells
    }

    fn commit_drop(&mut self, script: DropScript, row: i8, clear_rows: [bool; BOARD_HEIGHT]) {
        let mut board = self.settled;
        for (cell_row, column) in visible_script_cells(script, row) {
            board[cell_row][column] = true;
        }
        self.settled = collapse_cleared_rows(board, clear_rows);
        self.active = None;
        self.completed_turns = self.completed_turns.saturating_add(1);
    }

    pub(super) fn complete_finalizing_drop(&mut self) {
        let Some(active) = self.active.take() else {
            return;
        };
        if !active.finalizing {
            self.active = Some(active);
            return;
        }
        let row = landing_row(&self.settled, active.script);
        let clear_rows = clear_rows_after_drop(&self.settled, active.script, row);
        self.commit_drop(active.script, row, clear_rows);
    }
}

impl ActiveDrop {
    fn cells(&self) -> Vec<(usize, usize)> {
        visible_script_cells(self.script, self.row)
    }
}

impl DropScript {
    fn height(self) -> usize {
        self.cells
            .iter()
            .filter_map(|(row, _)| usize::try_from(*row).ok())
            .max()
            .map_or(0, |row| row.saturating_add(1))
    }
}

fn starting_row(script: DropScript) -> i8 {
    let height = i8::try_from(script.height()).unwrap_or(i8::MAX);
    1_i8.saturating_sub(height)
}

fn visible_script_cells(script: DropScript, row: i8) -> Vec<(usize, usize)> {
    let mut cells = Vec::new();
    for (row_offset, column_offset) in script.cells.iter().copied() {
        let cell_row = row.saturating_add(row_offset);
        let cell_column = script.x.saturating_add(column_offset);
        let (Ok(cell_row), Ok(cell_column)) =
            (usize::try_from(cell_row), usize::try_from(cell_column))
        else {
            continue;
        };
        if cell_row < BOARD_HEIGHT && cell_column < BOARD_WIDTH {
            cells.push((cell_row, cell_column));
        }
    }
    cells
}

fn can_place(settled: &Board, script: DropScript, row: i8) -> bool {
    for (row_offset, column_offset) in script.cells.iter().copied() {
        let cell_column = script.x.saturating_add(column_offset);
        let Ok(cell_column) = usize::try_from(cell_column) else {
            return false;
        };
        if cell_column >= BOARD_WIDTH {
            return false;
        }
        let cell_row = row.saturating_add(row_offset);
        if cell_row < 0 {
            continue;
        }
        let Ok(cell_row) = usize::try_from(cell_row) else {
            return false;
        };
        if cell_row >= BOARD_HEIGHT || settled[cell_row][cell_column] {
            return false;
        }
    }
    true
}

fn landing_row(settled: &Board, script: DropScript) -> i8 {
    let mut row = starting_row(script);
    loop {
        let next_row = row.saturating_add(1);
        if can_place(settled, script, next_row) {
            row = next_row;
        } else {
            return row;
        }
    }
}

fn clear_rows_after_drop(settled: &Board, script: DropScript, row: i8) -> [bool; BOARD_HEIGHT] {
    let mut board = *settled;
    for (cell_row, column) in visible_script_cells(script, row) {
        board[cell_row][column] = true;
    }
    let mut clear_rows = [false; BOARD_HEIGHT];
    for (index, board_row) in board.iter().enumerate() {
        clear_rows[index] = board_row.iter().all(|filled| *filled);
    }
    clear_rows
}

fn collapse_cleared_rows(board: Board, clear_rows: [bool; BOARD_HEIGHT]) -> Board {
    let mut next = [[false; BOARD_WIDTH]; BOARD_HEIGHT];
    let mut write_row = BOARD_HEIGHT;
    for row in (0..BOARD_HEIGHT).rev() {
        if clear_rows[row] {
            continue;
        }
        write_row = write_row.saturating_sub(1);
        next[write_row] = board[row];
    }
    next
}

/// 把上下两个逻辑格压缩成一个终端 cell 的 span。
///
/// 终端背景色会铺满整个单元格（含行距撑出的空隙），而字形只有字体高度。
/// 对 macOS Terminal.app 的像素级探针测量表明：行距 >1 时空隙几乎全部加在
/// 字形**上方**（28px 格中 █ 仅覆盖 offset 5..26），所以上半格颜色一律交给
/// 背景色承担（背景含顶部空隙铺满整格），字形只用贴底的 `▄` 负责下半格；
/// 上下同色时直接用空格 + 背景色铺满整格，完全不依赖字形。
fn cell_span(top: Option<Paint>, bottom: Option<Paint>) -> Span<'static> {
    let top_color = top.map_or(SURFACE_BG, paint_color);
    let bottom_color = bottom.map_or(SURFACE_BG, paint_color);
    if top_color == bottom_color {
        return Span::styled(" ", Style::default().bg(top_color));
    }
    Span::styled("▄", Style::default().fg(bottom_color).bg(top_color))
}

fn paint_color(paint: Paint) -> Color {
    match paint {
        Paint::Settled => BLUE_FG,
        Paint::Falling => FALLING_FG,
        Paint::Clearing => CLEARING_FG,
    }
}

/// 测试辅助：判断 span 是否绘制了动画里的彩色 cell（含背景色铺满的整块）。
#[cfg(test)]
pub(super) fn span_paints_cell(span: &Span<'_>) -> bool {
    let is_paint = |color: Color| {
        [Paint::Settled, Paint::Falling, Paint::Clearing]
            .into_iter()
            .any(|paint| paint_color(paint) == color)
    };
    span.style.bg.is_some_and(is_paint)
        || (span.content.contains('▄') && span.style.fg.is_some_and(is_paint))
}

/// 测试辅助：把 cell_span 的样式反推回字符画（'█'/'▀'/'▄'/' '），便于断言棋盘形状。
/// 上半是否着色恒看 bg（背景承担上半格），下半看字形 fg（空格满块看 bg）；
/// 颜色只与 SURFACE_BG 区分即可，调色板互异性由 palette 测试钉住。
#[cfg(test)]
pub(super) fn span_char_art(span: &Span<'_>) -> char {
    let top_painted = span.style.bg != Some(SURFACE_BG);
    let bottom_painted = if span.content.as_ref() == " " {
        span.style.bg != Some(SURFACE_BG)
    } else {
        span.style.fg != Some(SURFACE_BG)
    };
    match (top_painted, bottom_painted) {
        (true, true) => '█',
        (true, false) => '▀',
        (false, true) => '▄',
        (false, false) => ' ',
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn rendered_text(lines: &[Line<'static>]) -> String {
        lines
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn settled_text(state: &TurnAnimationState) -> Vec<String> {
        state
            .settled
            .iter()
            .map(|row| {
                row.iter()
                    .map(|filled| if *filled { '1' } else { '0' })
                    .collect()
            })
            .collect()
    }

    fn run_successful_turn(state: &mut TurnAnimationState) {
        state.begin_turn();
        state.finish_success();
        while state.is_active() {
            assert!(state.tick());
        }
    }

    fn script_is_connected(script: DropScript) -> bool {
        let mut seen = vec![false; script.cells.len()];
        let mut stack = vec![0_usize];
        seen[0] = true;
        while let Some(index) = stack.pop() {
            let (row, column) = script.cells[index];
            for (next_index, (next_row, next_column)) in script.cells.iter().copied().enumerate() {
                if seen[next_index] {
                    continue;
                }
                let vertical = column == next_column && row.abs_diff(next_row) == 1;
                let horizontal = row == next_row && column.abs_diff(next_column) == 1;
                if vertical || horizontal {
                    seen[next_index] = true;
                    stack.push(next_index);
                }
            }
        }
        seen.into_iter().all(|cell_seen| cell_seen)
    }

    fn script_has_duplicate_cells(script: DropScript) -> bool {
        script.cells.iter().enumerate().any(|(index, cell)| {
            script
                .cells
                .iter()
                .skip(index.saturating_add(1))
                .any(|next| next == cell)
        })
    }

    fn all_scripts() -> Vec<DropScript> {
        let mut scripts = vec![FIRST_SCRIPT];
        scripts.extend(FOLLOWUP_SCRIPTS);
        scripts
    }

    fn normalize_cells(cells: &[(i8, i8)]) -> Vec<(i8, i8)> {
        let min_row = cells.iter().map(|(row, _)| *row).min().unwrap_or(0);
        let min_column = cells.iter().map(|(_, column)| *column).min().unwrap_or(0);
        let mut normalized = cells
            .iter()
            .map(|(row, column)| {
                (
                    row.saturating_sub(min_row),
                    column.saturating_sub(min_column),
                )
            })
            .collect::<Vec<_>>();
        normalized.sort_unstable();
        normalized
    }

    fn cell_key(cells: &[(i8, i8)]) -> String {
        normalize_cells(cells)
            .into_iter()
            .map(|(row, column)| format!("{row}:{column}"))
            .collect::<Vec<_>>()
            .join("|")
    }

    fn rotate_cells(cells: &[(i8, i8)]) -> Vec<(i8, i8)> {
        normalize_cells(cells)
            .into_iter()
            .map(|(row, column)| (column.saturating_neg(), row))
            .collect()
    }

    fn standard_tetromino_keys() -> BTreeSet<String> {
        let bases: [&[(i8, i8)]; 7] = [
            &[(0, 0), (0, 1), (0, 2), (0, 3)],
            &[(0, 0), (0, 1), (1, 0), (1, 1)],
            &[(0, 0), (0, 1), (0, 2), (1, 1)],
            &[(0, 0), (1, 0), (2, 0), (2, 1)],
            &[(0, 1), (1, 1), (2, 0), (2, 1)],
            &[(0, 1), (0, 2), (1, 0), (1, 1)],
            &[(0, 0), (0, 1), (1, 1), (1, 2)],
        ];
        let mut keys = BTreeSet::new();
        for base in bases {
            let mut rotated = base.to_vec();
            for _ in 0..4 {
                keys.insert(cell_key(&rotated));
                rotated = rotate_cells(&rotated);
            }
        }
        keys
    }

    fn board_has_floating_cell(board: &Board) -> bool {
        for row in 0..BOARD_HEIGHT.saturating_sub(1) {
            #[allow(clippy::needless_range_loop, reason = "同列比较相邻两行")]
            for column in 0..BOARD_WIDTH {
                if board[row][column] && !board[row.saturating_add(1)][column] {
                    return true;
                }
            }
        }
        false
    }

    #[test]
    fn cell_span_mapping_covers_empty_top_bottom_full_and_mixed() {
        let empty = cell_span(None, None);
        assert_eq!(empty.content.as_ref(), " ");
        assert_eq!(empty.style.bg, Some(SURFACE_BG));

        let top_only = cell_span(Some(Paint::Falling), None);
        assert_eq!(top_only.content.as_ref(), "▄");
        assert_eq!(top_only.style.fg, Some(SURFACE_BG));
        assert_eq!(top_only.style.bg, Some(FALLING_FG));

        let bottom_only = cell_span(None, Some(Paint::Settled));
        assert_eq!(bottom_only.content.as_ref(), "▄");
        assert_eq!(bottom_only.style.fg, Some(BLUE_FG));
        assert_eq!(bottom_only.style.bg, Some(SURFACE_BG));

        let full = cell_span(Some(Paint::Settled), Some(Paint::Settled));
        assert_eq!(full.content.as_ref(), " ");
        assert_eq!(full.style.bg, Some(BLUE_FG));

        let mixed = cell_span(Some(Paint::Falling), Some(Paint::Settled));
        assert_eq!(mixed.content.as_ref(), "▄");
        assert_eq!(mixed.style.fg, Some(BLUE_FG));
        assert_eq!(mixed.style.bg, Some(FALLING_FG));
    }

    #[test]
    fn palette_colors_are_pairwise_distinct() {
        // cell_span 的同色折叠与 span_char_art 的反解码都依赖调色板颜色两两互异：
        // 若某 Paint 色与 SURFACE_BG 撞色，对应半格会静默消失且无测试报警。
        let colors = [SURFACE_BG, BLUE_FG, FALLING_FG, CLEARING_FG];
        for (index, first) in colors.iter().enumerate() {
            for second in colors.iter().skip(index + 1) {
                assert_ne!(first, second);
            }
        }
    }

    #[test]
    fn cell_span_never_relies_on_glyph_for_top_half() {
        let paints = [
            None,
            Some(Paint::Settled),
            Some(Paint::Falling),
            Some(Paint::Clearing),
        ];
        for top in paints {
            for bottom in paints {
                let span = cell_span(top, bottom);
                // 上半格颜色必须由背景色承担：Terminal.app 把行距空隙加在字形上方，
                // 只有背景能铺满含顶部空隙的整格；依赖 `█` / `▀` 字形会整体下坠露缝。
                assert_eq!(span.style.bg, Some(top.map_or(SURFACE_BG, paint_color)));
                assert!(!span.content.contains('█'));
                assert!(!span.content.contains('▀'));
            }
        }
    }

    #[test]
    fn scripted_pieces_are_single_standard_tetrominoes() {
        let standard_keys = standard_tetromino_keys();
        for script in all_scripts() {
            assert!(
                !script.name.is_empty(),
                "Script names make review failures readable"
            );
            assert_eq!(
                script.cells.len(),
                4,
                "{} should be one standard tetromino",
                script.name
            );
            assert!(
                !script_has_duplicate_cells(script),
                "{} should not paint the same cell twice",
                script.name
            );
            assert!(
                visible_script_cells(script, 0)
                    .into_iter()
                    .all(|(_, column)| column < BOARD_WIDTH),
                "{} should stay inside the 4-column board",
                script.name
            );
            assert!(
                script_is_connected(script),
                "{} should be one 4-neighbor connected component",
                script.name
            );
            assert!(
                standard_keys.contains(&cell_key(script.cells)),
                "{} should be a standard I/O/T/L/J/S/Z rotation",
                script.name
            );
        }
    }

    #[test]
    fn first_successful_turn_uses_l_piece_and_leaves_residue_without_clear() {
        let mut state = TurnAnimationState::default();
        run_successful_turn(&mut state);

        assert_eq!(state.completed_turns, 1);
        assert_eq!(
            settled_text(&state),
            vec!["0000", "0000", "0000", "1000", "1000", "1100"]
        );
        assert!(!state.is_active());
        assert!(state.has_visible_board());
        assert!(!board_has_floating_cell(&state.settled));
        assert_eq!(
            rendered_text(
                &state.render_lines(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS)
            ),
            "    \n▄   \n ▄  "
        );
    }

    #[test]
    fn every_followup_script_clears_one_line_and_cycles_residue() {
        let mut state = TurnAnimationState::default();
        let expected_residue = [
            vec!["0000", "0000", "0000", "1000", "1000", "1100"],
            vec!["0000", "0000", "0000", "0000", "1000", "1101"],
            vec!["0000", "0000", "0000", "0000", "0001", "1011"],
            vec!["0000", "0000", "0000", "0000", "0100", "1101"],
            vec!["0000", "0000", "0000", "0000", "0001", "0111"],
            vec!["0000", "0000", "0000", "1000", "1000", "1001"],
            vec!["0000", "0000", "0000", "0000", "1000", "1011"],
            vec!["0000", "0000", "0000", "0000", "1100", "1100"],
            vec!["0000", "0000", "0000", "0000", "0001", "1101"],
            vec!["0000", "0000", "0000", "0000", "0010", "0111"],
            vec!["0000", "0000", "0000", "1000", "1000", "1010"],
            vec!["0000", "0000", "0000", "0000", "1000", "1110"],
            vec!["0000", "0000", "0000", "0000", "0010", "1110"],
            vec!["0000", "0000", "0000", "0001", "0001", "0011"],
            vec!["0000", "0000", "0000", "0000", "0011", "0011"],
            vec!["0000", "0000", "0000", "0000", "0100", "0111"],
            vec!["0000", "0000", "0000", "1000", "1000", "1100"],
        ];

        for (index, expected) in expected_residue.into_iter().enumerate() {
            let script = state.next_script();
            let row = landing_row(&state.settled, script);
            let clear_rows = clear_rows_after_drop(&state.settled, script, row);
            if state.completed_turns == 0 {
                assert!(
                    clear_rows.iter().all(|clear| !*clear),
                    "The opening piece starts from empty without clearing"
                );
            } else {
                assert_eq!(
                    clear_rows.iter().filter(|clear| **clear).count(),
                    1,
                    "{} should clear exactly one row",
                    script.name
                );
            }
            run_successful_turn(&mut state);
            assert_eq!(state.completed_turns, index + 1);
            assert_eq!(settled_text(&state), expected);
            assert!(
                !board_has_floating_cell(&state.settled),
                "{} should leave gravity-supported residue",
                script.name
            );
        }
        assert_eq!(state.next_script().name, FOLLOWUP_SCRIPTS[0].name);
    }

    #[test]
    fn running_loop_highlights_without_committing_settled_board() {
        let mut state = TurnAnimationState::default();
        state.begin_turn();
        for _ in 0..6 {
            assert!(state.tick());
        }

        let lines = state.render_lines(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS);
        assert!(!lines.is_empty());
        assert!(lines
            .iter()
            .flat_map(|line| line.spans.iter())
            .any(span_paints_cell));
        assert!(state.settled.iter().flatten().all(|filled| !*filled));
        assert_eq!(state.completed_turns, 0);
    }

    #[test]
    fn cancelled_turn_discards_falling_piece_without_advancing_script() {
        let mut state = TurnAnimationState::default();
        state.begin_turn();
        assert!(state.tick());
        state.cancel_turn();

        assert!(!state.is_active());
        assert!(!state.has_visible_board());
        assert_eq!(state.completed_turns, 0);
        assert!(state.settled.iter().flatten().all(|filled| !*filled));
    }

    #[test]
    fn cancelled_followup_turn_keeps_previous_residue_visible() {
        let mut state = TurnAnimationState::default();
        run_successful_turn(&mut state);
        let previous = state.render_lines(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS);

        state.begin_turn();
        assert!(state.tick());
        state.cancel_turn();

        assert!(!state.is_active());
        assert!(state.has_visible_board());
        assert_eq!(
            state.render_lines(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS),
            previous
        );
        assert_eq!(state.completed_turns, 1);
    }

    #[test]
    fn narrow_or_short_viewport_hides_animation_lines_and_freezes_tick() {
        let mut state = TurnAnimationState::default();
        state.begin_turn();
        let before = state.render_lines(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS);

        assert!(state
            .render_lines(TURN_ANIMATION_MIN_WIDTH - 1, TURN_ANIMATION_RENDER_ROWS)
            .is_empty());
        assert!(state
            .render_lines(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS - 1)
            .is_empty());
        assert!(!state.tick_if_visible(TURN_ANIMATION_MIN_WIDTH - 1, TURN_ANIMATION_RENDER_ROWS));
        assert!(!state.tick_if_visible(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS - 1));
        assert_eq!(
            state.render_lines(TURN_ANIMATION_MIN_WIDTH, TURN_ANIMATION_RENDER_ROWS),
            before
        );
    }

    #[test]
    fn reset_clears_residue_and_active_drop() {
        let mut state = TurnAnimationState::default();
        run_successful_turn(&mut state);

        state.reset();

        assert!(!state.is_active());
        assert!(!state.has_visible_board());
        assert_eq!(state.completed_turns, 0);
        assert!(state.settled.iter().flatten().all(|filled| !*filled));
    }
}
