//! SessionEngine 的 tool-boundary steer/cancel 控制面。
//!
//! 本模块维护用户在工具安全边界上的中断请求、durable journal ack，
//! 以及 turn control 事件到 turn journal 事件的转发。
//! 它不决定 turn 是否提交，只向 facade 暴露控制 token 和状态。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::api::{ToolBoundaryControl, ToolCallSkipReason};
use crate::session::{TurnJournalEventKind, TurnJournalStatus};

use super::turn_journal::TurnJournalSink;

#[derive(Clone)]
pub struct SessionTurnControl {
    tool_boundary_control: ToolBoundaryControl,
    interrupt_status: Arc<Mutex<Option<TurnJournalStatus>>>,
    tx: mpsc::UnboundedSender<SessionTurnControlEvent>,
}

pub struct SessionTurnControlReceiver {
    tool_boundary_control: ToolBoundaryControl,
    interrupt_status: Arc<Mutex<Option<TurnJournalStatus>>>,
    rx: mpsc::UnboundedReceiver<SessionTurnControlEvent>,
}

enum SessionTurnControlEvent {
    PendingSteer {
        text: String,
        reason: String,
        ack: oneshot::Sender<bool>,
    },
    PendingCancel {
        reason: String,
        ack: oneshot::Sender<bool>,
    },
}

impl SessionTurnControl {
    pub fn channel() -> (Self, SessionTurnControlReceiver) {
        let tool_boundary_control = ToolBoundaryControl::new();
        let interrupt_status = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tool_boundary_control: tool_boundary_control.clone(),
                interrupt_status: Arc::clone(&interrupt_status),
                tx,
            },
            SessionTurnControlReceiver {
                tool_boundary_control,
                interrupt_status,
                rx,
            },
        )
    }

    #[cfg(test)]
    pub fn acknowledged_for_test() -> Self {
        let (control, mut receiver) = Self::channel();
        let tool_boundary_control = receiver.tool_boundary_control.clone();
        let interrupt_status = Arc::clone(&receiver.interrupt_status);
        tokio::spawn(async move {
            while let Some(event) = receiver.rx.recv().await {
                match event {
                    SessionTurnControlEvent::PendingSteer { ack, .. } => {
                        set_interrupt_status_if_empty(
                            &interrupt_status,
                            TurnJournalStatus::InterruptedByUser,
                        );
                        tool_boundary_control
                            .cancel_if_open(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
                        let _ = ack.send(true);
                    }
                    SessionTurnControlEvent::PendingCancel { ack, .. } => {
                        set_interrupt_status(&interrupt_status, TurnJournalStatus::Cancelled);
                        tool_boundary_control
                            .cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
                        let _ = ack.send(true);
                    }
                }
            }
        });
        control
    }

    pub async fn request_tool_boundary_steer(&self, text: impl Into<String>) -> bool {
        let was_pending = self.tool_boundary_control.is_cancelled();
        let reason = if was_pending {
            "additional user steer pending"
        } else {
            "user steer pending"
        };
        let (ack, durable) = oneshot::channel();
        let sent = self
            .tx
            .send(SessionTurnControlEvent::PendingSteer {
                text: text.into(),
                reason: reason.to_string(),
                ack,
            })
            .is_ok();
        if !sent || !matches!(durable.await, Ok(true)) {
            return false;
        }
        true
    }

    pub fn request_tool_boundary_cancel_now(&self, reason: impl Into<String>) -> bool {
        let (ack, durable) = oneshot::channel();
        let sent = self
            .tx
            .send(SessionTurnControlEvent::PendingCancel {
                reason: reason.into(),
                ack,
            })
            .is_ok();
        if !sent {
            return false;
        }
        set_interrupt_status(&self.interrupt_status, TurnJournalStatus::Cancelled);
        self.tool_boundary_control
            .cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
        drop(durable);
        true
    }

    pub async fn request_tool_boundary_cancel(&self, reason: impl Into<String>) -> bool {
        self.request_tool_boundary_cancel_now(reason)
    }
}

impl SessionTurnControlReceiver {
    pub(super) fn tool_boundary_control(&self) -> ToolBoundaryControl {
        self.tool_boundary_control.clone()
    }

    pub(super) fn interrupt_status_cell(&self) -> Arc<Mutex<Option<TurnJournalStatus>>> {
        Arc::clone(&self.interrupt_status)
    }
}

fn set_interrupt_status_if_empty(
    interrupt_status: &Arc<Mutex<Option<TurnJournalStatus>>>,
    status: TurnJournalStatus,
) {
    if let Ok(mut current) = interrupt_status.lock() {
        if current.is_none() {
            *current = Some(status);
        }
    }
}

fn set_interrupt_status(
    interrupt_status: &Arc<Mutex<Option<TurnJournalStatus>>>,
    status: TurnJournalStatus,
) {
    if let Ok(mut current) = interrupt_status.lock() {
        *current = Some(status);
    }
}

pub(super) struct TurnControlJournalForwarder {
    pub(super) shutdown: CancellationToken,
    drain_on_shutdown: Arc<AtomicBool>,
    initial_drain: Option<oneshot::Receiver<()>>,
    pub(super) handle: JoinHandle<()>,
}

impl TurnControlJournalForwarder {
    pub(super) fn set_drain_on_shutdown(&self, drain: bool) {
        self.drain_on_shutdown.store(drain, Ordering::Relaxed);
    }

    pub(super) async fn wait_initial_drain(&mut self) {
        if let Some(initial_drain) = self.initial_drain.take() {
            let _ = initial_drain.await;
        }
    }
}

pub(super) fn spawn_turn_control_journal_forwarder(
    sink: TurnJournalSink,
    receiver: SessionTurnControlReceiver,
) -> TurnControlJournalForwarder {
    let shutdown = CancellationToken::new();
    let shutdown_task = shutdown.clone();
    let drain_on_shutdown = Arc::new(AtomicBool::new(true));
    let drain_on_shutdown_task = Arc::clone(&drain_on_shutdown);
    let tool_boundary_control = receiver.tool_boundary_control.clone();
    let interrupt_status = Arc::clone(&receiver.interrupt_status);
    let mut rx = receiver.rx;
    let (initial_drain_tx, initial_drain_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        while let Ok(event) = rx.try_recv() {
            forward_turn_control_journal_event(
                &sink,
                &tool_boundary_control,
                &interrupt_status,
                event,
            )
            .await;
        }
        let _ = initial_drain_tx.send(());
        loop {
            tokio::select! {
                maybe_event = rx.recv() => {
                    let Some(event) = maybe_event else {
                        break;
                    };
                    if drain_on_shutdown_task.load(Ordering::Relaxed) {
                        forward_turn_control_journal_event(
                            &sink,
                            &tool_boundary_control,
                            &interrupt_status,
                            event,
                        )
                        .await;
                    } else {
                        reject_turn_control_journal_event(event);
                    }
                }
                _ = shutdown_task.cancelled() => {
                    if drain_on_shutdown_task.load(Ordering::Relaxed) {
                        while let Ok(event) = rx.try_recv() {
                            forward_turn_control_journal_event(
                                &sink,
                                &tool_boundary_control,
                                &interrupt_status,
                                event,
                            )
                            .await;
                        }
                    } else {
                        while let Ok(event) = rx.try_recv() {
                            reject_turn_control_journal_event(event);
                        }
                    }
                    break;
                }
            }
        }
    });
    TurnControlJournalForwarder {
        shutdown,
        drain_on_shutdown,
        initial_drain: Some(initial_drain_rx),
        handle,
    }
}

fn reject_turn_control_journal_event(event: SessionTurnControlEvent) {
    match event {
        SessionTurnControlEvent::PendingSteer { ack, .. }
        | SessionTurnControlEvent::PendingCancel { ack, .. } => {
            let _ = ack.send(false);
        }
    }
}

async fn forward_turn_control_journal_event(
    sink: &TurnJournalSink,
    tool_boundary_control: &ToolBoundaryControl,
    interrupt_status: &Arc<Mutex<Option<TurnJournalStatus>>>,
    event: SessionTurnControlEvent,
) {
    match event {
        SessionTurnControlEvent::PendingSteer { text, reason, ack } => {
            let result = async {
                sink.send_immediate_durable(TurnJournalEventKind::UserSteerSubmitted { text })
                    .await?;
                sink.send_immediate_durable(TurnJournalEventKind::InterruptRequested {
                    reason: Some(reason.clone()),
                })
                .await?;
                sink.send_immediate_durable(TurnJournalEventKind::InterruptPending {
                    reason: Some(reason),
                })
                .await
            }
            .await;
            let ok = result.is_ok();
            if ok {
                set_interrupt_status_if_empty(
                    interrupt_status,
                    TurnJournalStatus::InterruptedByUser,
                );
                tool_boundary_control
                    .cancel_if_open(ToolCallSkipReason::TurnInterruptedBeforeDispatch);
            }
            let _ = ack.send(ok);
        }
        SessionTurnControlEvent::PendingCancel { reason, ack } => {
            let result = async {
                sink.send_immediate_durable(TurnJournalEventKind::InterruptRequested {
                    reason: Some(reason.clone()),
                })
                .await?;
                sink.send_immediate_durable(TurnJournalEventKind::InterruptPending {
                    reason: Some(reason),
                })
                .await
            }
            .await;
            let ok = result.is_ok();
            if ok {
                set_interrupt_status(interrupt_status, TurnJournalStatus::Cancelled);
                tool_boundary_control.cancel(ToolCallSkipReason::TurnCancelledBeforeDispatch);
            }
            let _ = ack.send(ok);
        }
    }
}
