//! TUI 终端壳与事件流。
//!
//! `Tui` 封装 terminal guard、输入 reader 和轻量 render requester。主 app 只消费
//! `TuiEvent`，不直接依赖 crossterm 原始事件。

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crossterm::event::{Event, KeyEvent};
use tokio::sync::mpsc;

use super::chat_widget::ChatWidget;
use super::session_picker::SessionPickerState;
use super::terminal::{
    spawn_terminal_event_reader, TerminalEvent, TerminalGuard, TerminalReaderGuard,
};
use super::theme::apply_surface_style;

#[derive(Debug, Clone)]
pub(super) enum TuiEvent {
    Key(KeyEvent),
    Paste(String),
    Resize,
    Render,
}

#[derive(Clone, Debug)]
pub(super) struct RenderRequester {
    tx: mpsc::UnboundedSender<()>,
    pending: Arc<AtomicBool>,
}

impl RenderRequester {
    fn new(tx: mpsc::UnboundedSender<()>) -> Self {
        Self {
            tx,
            pending: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn schedule_render(&self) {
        if self.pending.swap(true, Ordering::SeqCst) {
            return;
        }
        if self.tx.send(()).is_err() {
            self.pending.store(false, Ordering::SeqCst);
        }
    }

    fn mark_render_received(&self) {
        self.pending.store(false, Ordering::SeqCst);
    }
}

pub(super) struct Tui {
    terminal: TerminalGuard,
    terminal_rx: mpsc::UnboundedReceiver<TerminalEvent>,
    render_rx: mpsc::UnboundedReceiver<()>,
    render_requester: RenderRequester,
    terminal_reader_stop: Arc<AtomicBool>,
    _terminal_reader_guard: TerminalReaderGuard,
}

impl Tui {
    pub(super) fn enter() -> anyhow::Result<Self> {
        let terminal = TerminalGuard::enter()?;
        let (terminal_tx, terminal_rx) = mpsc::unbounded_channel::<TerminalEvent>();
        let terminal_reader_stop = Arc::new(AtomicBool::new(false));
        spawn_terminal_event_reader(terminal_tx, terminal_reader_stop.clone());
        let terminal_reader_guard = TerminalReaderGuard::new(terminal_reader_stop.clone());
        let (render_tx, render_rx) = mpsc::unbounded_channel();
        let render_requester = RenderRequester::new(render_tx);
        Ok(Self {
            terminal,
            terminal_rx,
            render_rx,
            render_requester,
            terminal_reader_stop,
            _terminal_reader_guard: terminal_reader_guard,
        })
    }

    pub(super) fn render_requester(&self) -> RenderRequester {
        self.render_requester.clone()
    }

    pub(super) fn render_width(&self) -> anyhow::Result<u16> {
        let (width, _) = crossterm::terminal::size()?;
        Ok(width.saturating_sub(1).max(1))
    }

    pub(super) fn terminal_size(&self) -> anyhow::Result<(u16, u16)> {
        let (width, height) = crossterm::terminal::size()?;
        Ok((width.max(1), height.max(1)))
    }

    pub(super) fn draw(
        &mut self,
        chat_widget: &mut ChatWidget,
        session_picker: Option<&SessionPickerState>,
    ) -> anyhow::Result<()> {
        self.draw_with_options(chat_widget, session_picker, false, false)
    }

    pub(super) fn draw_after_state_reload(
        &mut self,
        chat_widget: &mut ChatWidget,
        session_picker: Option<&SessionPickerState>,
    ) -> anyhow::Result<()> {
        self.draw_with_options(chat_widget, session_picker, true, true)
    }

    /// resize 后重绘：走 hard_clear 全屏重排，按新宽度把整段历史重新 reflow 写入 scrollback，
    /// 避免“增量擦除 + 旧宽度几何”导致的重复行 / 渲染紊乱。
    pub(super) fn draw_after_resize(
        &mut self,
        chat_widget: &mut ChatWidget,
        session_picker: Option<&SessionPickerState>,
    ) -> anyhow::Result<()> {
        self.draw_with_options(chat_widget, session_picker, true, true)
    }

    fn draw_with_options(
        &mut self,
        chat_widget: &mut ChatWidget,
        session_picker: Option<&SessionPickerState>,
        hard_clear: bool,
        reset_flush_state: bool,
    ) -> anyhow::Result<()> {
        let size = crossterm::terminal::size()?;
        let width = size.0.max(1);
        let height = size.1.max(1);
        if reset_flush_state {
            chat_widget.state_mut().reset_flushed_for_hard_clear();
        }
        chat_widget.state_mut().refresh_focus_timer();
        let render = chat_widget.render_inline(width, height);
        let (live_lines, cursor) = if let Some(picker) = session_picker {
            (
                picker
                    .render_inline_lines(width)
                    .into_iter()
                    .map(apply_surface_style)
                    .collect(),
                None,
            )
        } else {
            (render.live_lines, render.cursor)
        };
        self.terminal.draw(
            width,
            &render.scrollback_lines,
            &live_lines,
            cursor,
            hard_clear,
        )?;
        chat_widget
            .state_mut()
            .mark_scrollback_flushed(render.flushed_entry_count);
        if render.start_separator_flushed {
            chat_widget.state_mut().mark_start_separator_flushed();
        }
        Ok(())
    }

    pub(super) fn stop_reader(&self) {
        self.terminal_reader_stop.store(true, Ordering::SeqCst);
    }

    pub(super) async fn recv_event(&mut self) -> anyhow::Result<TuiEvent> {
        loop {
            tokio::select! {
                render = self.render_rx.recv() => {
                    if render.is_some() {
                        self.render_requester.mark_render_received();
                        return Ok(TuiEvent::Render);
                    }
                }
                maybe_terminal_event = self.terminal_rx.recv() => {
                    match maybe_terminal_event {
                        Some(TerminalEvent::Input(event)) => {
                            if let Some(mapped) = map_terminal_event(event) {
                                return Ok(mapped);
                            }
                        }
                        Some(TerminalEvent::Error(error)) => anyhow::bail!(error),
                        None => anyhow::bail!("TUI 输入事件 reader 已结束"),
                    }
                }
            }
        }
    }
}

fn map_terminal_event(event: Event) -> Option<TuiEvent> {
    match event {
        Event::Key(key) => Some(TuiEvent::Key(key)),
        Event::Paste(text) => Some(TuiEvent::Paste(text)),
        Event::Resize(_, _) => Some(TuiEvent::Resize),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_requester_coalesces_pending_draw_requests() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let requester = RenderRequester::new(tx);

        requester.schedule_render();
        requester.schedule_render();
        requester.schedule_render();

        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }
}
