//! 交互式 session 的终端界面层。
//!
//! 本模块对外只暴露 `run_session_tui`。内部按职责分层：App 负责事件循环，
//! ChatWidget 负责聊天状态投影，BottomPane 负责输入，Tui 负责终端事件和绘制。

mod app;
mod app_event;
mod at_path;
mod at_path_completion;
mod attachment;
mod bottom_pane;
mod cell;
mod chat_widget;
mod claim_panel;
mod cleanup_housekeeping;
mod completion_menu;
mod composer;
mod input_queue;
mod markdown;
mod mcp_panel;
mod process_panel;
mod runtime;
mod session_picker;
mod slash_command;
mod state;
mod terminal;
mod theme;
mod transcript;
mod tui;
mod turn_animation;
mod welcome;
mod word_nav;
mod wrapping;

pub use app::{
    run_session_tui, run_session_tui_with_resume, run_session_tui_with_resume_and_cleanup,
    StartupResume,
};
pub use cleanup_housekeeping::{
    SessionCleanupHousekeepingConfig, SessionCleanupHousekeepingTiming,
};

#[cfg(test)]
use bottom_pane::{classify_input, input_accepts_text, is_shift_enter_newline, InputAction};
#[cfg(test)]
use chat_widget::{
    composer_cursor_x, composer_cursor_x_for_width, composer_cursor_y, composer_cursor_y_for_width,
    composer_height, composer_height_for_width, composer_hint, composer_lines_with_width,
    history_render_lines_with_width, inline_cursor_for_width, inline_live_lines_with_size,
    inline_live_lines_with_size_and_preview_max, inline_live_lines_with_width,
    inline_scrollback_lines_with_width, inline_start_separator_lines_with_width,
    turn_animation_height_budget_for_test,
};
#[cfg(test)]
use runtime::{ActiveSessionTask, ActiveTurn, SessionTaskState};
#[cfg(test)]
use state::SessionTuiState as TuiState;

#[cfg(test)]
mod tests;
