//! SessionEngine turn journal 运行期桥接。
//!
//! `crate::session` 负责 journal 文件格式和 replay；本模块只负责单个 turn
//! 运行期间的异步写入、delta 缓冲和 durable event recorder。
//! canonical transcript 的提交语义仍在 SessionEngine facade 中。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::api::{CompletedSessionTurnMessage, SessionTurnEvent, SessionTurnEventRecorder};
use crate::session::{
    TurnJournalEventKind, TurnJournalFlush, TurnJournalStatus, TurnJournalWriter,
};

pub(super) struct TurnJournalCommand {
    pub(super) created_at: DateTime<Utc>,
    pub(super) kind: TurnJournalEventKind,
    pub(super) flush: TurnJournalFlush,
    pub(super) ack: Option<oneshot::Sender<Result<(), String>>>,
}

#[derive(Clone)]
pub(super) struct TurnJournalSink {
    pub(super) tx: mpsc::UnboundedSender<TurnJournalCommand>,
}

/// 让 durable event recorder 在写关键状态前先落盘当前已显示的 assistant delta。
#[derive(Clone)]
pub(super) struct TurnJournalDeltaFlusher {
    tx: mpsc::UnboundedSender<TurnJournalCommand>,
    delta_buffer: Arc<Mutex<TurnJournalDeltaBuffer>>,
    delta_send_lock: Arc<Mutex<()>>,
}

impl TurnJournalDeltaFlusher {
    fn flush(&self) {
        flush_turn_journal_assistant_delta(&self.tx, &self.delta_buffer, &self.delta_send_lock);
    }
}

impl TurnJournalSink {
    pub(super) async fn send_immediate_durable(
        &self,
        kind: TurnJournalEventKind,
    ) -> anyhow::Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(TurnJournalCommand {
                created_at: Utc::now(),
                kind,
                flush: TurnJournalFlush::Immediate,
                ack: Some(ack_tx),
            })
            .map_err(|_| anyhow::anyhow!("turn journal writer is closed"))?;
        match ack_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => anyhow::bail!(error),
            Err(_) => anyhow::bail!("turn journal writer stopped before durable ack"),
        }
    }
}

pub(super) struct TurnJournalDurableEventRecorder {
    pub(super) sink: TurnJournalSink,
    pub(super) assistant_delta_flusher: TurnJournalDeltaFlusher,
}

#[async_trait]
impl SessionTurnEventRecorder for TurnJournalDurableEventRecorder {
    async fn record(&mut self, event: SessionTurnEvent) -> anyhow::Result<()> {
        match event {
            SessionTurnEvent::Warning { .. }
            | SessionTurnEvent::CompactionStarted { .. }
            | SessionTurnEvent::CompactionCompleted { .. }
            | SessionTurnEvent::CompactionSkipped { .. }
            | SessionTurnEvent::CompactionFailed { .. } => Ok(()),
            SessionTurnEvent::NonStreamingFallbackAttemptStarted {
                attempt,
                max_attempts,
                previous_error,
            } => {
                self.assistant_delta_flusher.flush();
                self.sink
                    .send_immediate_durable(
                        TurnJournalEventKind::NonStreamingFallbackAttemptStarted {
                            attempt,
                            max_attempts,
                            previous_error,
                        },
                    )
                    .await
            }
            SessionTurnEvent::NonStreamingFallbackAttemptFailed {
                attempt,
                max_attempts,
                error,
            } => {
                self.assistant_delta_flusher.flush();
                self.sink
                    .send_immediate_durable(
                        TurnJournalEventKind::NonStreamingFallbackAttemptFailed {
                            attempt,
                            max_attempts,
                            error,
                        },
                    )
                    .await
            }
            SessionTurnEvent::NonStreamingFallbackSucceeded {
                attempt,
                max_attempts,
                text,
            } => {
                self.assistant_delta_flusher.flush();
                self.sink
                    .send_immediate_durable(TurnJournalEventKind::NonStreamingFallbackSucceeded {
                        attempt,
                        max_attempts,
                        text,
                    })
                    .await
            }
            SessionTurnEvent::ToolCallStarted {
                id,
                name,
                summary,
                input_preview,
                input_truncated,
            } => {
                self.sink
                    .send_immediate_durable(TurnJournalEventKind::ToolCallStarted {
                        tool_use_id: id,
                        name,
                        summary,
                        input_preview,
                        input_truncated,
                    })
                    .await
            }
            SessionTurnEvent::ToolCallSkipped {
                id,
                name,
                summary,
                input_preview,
                input_truncated,
                reason,
            } => {
                self.sink
                    .send_immediate_durable(TurnJournalEventKind::ToolCallSkipped {
                        tool_use_id: id,
                        name,
                        summary,
                        input_preview,
                        input_truncated,
                        reason,
                    })
                    .await
            }
            SessionTurnEvent::ToolCallCompleted {
                id,
                summary,
                outcome,
                output_preview,
                output_truncated,
                file_change,
            } => {
                self.sink
                    .send_immediate_durable(TurnJournalEventKind::ToolCallCompleted {
                        tool_use_id: id,
                        summary,
                        outcome: Some(outcome),
                        output_preview,
                        output_truncated,
                        file_change,
                    })
                    .await
            }
            SessionTurnEvent::ToolCallInterrupted { id, summary } => {
                self.sink
                    .send_immediate_durable(TurnJournalEventKind::ToolCallInterrupted {
                        tool_use_id: id,
                        summary,
                    })
                    .await
            }
            SessionTurnEvent::AssistantMessageCompleted { text } => {
                self.sink
                    .send_immediate_durable(TurnJournalEventKind::AssistantCompleted { text })
                    .await
            }
            SessionTurnEvent::ContextUsageUpdated { .. }
            | SessionTurnEvent::AssistantTextDelta { .. }
            | SessionTurnEvent::ToolCallProgress { .. } => Ok(()),
        }
    }

    async fn record_completed_message(
        &mut self,
        message: &CompletedSessionTurnMessage,
    ) -> anyhow::Result<()> {
        let Some((source, fingerprint, text)) = message.model_context_snapshot() else {
            return Ok(());
        };
        self.sink
            .send_immediate_durable(TurnJournalEventKind::ModelContextAppended {
                source: *source,
                fingerprint: fingerprint.to_string(),
                text: text.to_string(),
            })
            .await
    }
}

pub(super) struct TurnJournalEmitter {
    tx: mpsc::UnboundedSender<TurnJournalCommand>,
    delta_buffer: Arc<Mutex<TurnJournalDeltaBuffer>>,
    delta_send_lock: Arc<Mutex<()>>,
    delta_snapshot_interval: Duration,
    delta_snapshot_chars: usize,
    delta_flush_shutdown: CancellationToken,
    delta_flush_handle: Option<JoinHandle<()>>,
}

struct TurnJournalDeltaBuffer {
    text: String,
    last_flush: Instant,
}

impl TurnJournalEmitter {
    pub(super) fn new(
        tx: mpsc::UnboundedSender<TurnJournalCommand>,
        delta_snapshot_interval: Duration,
        delta_snapshot_chars: usize,
    ) -> Self {
        let delta_buffer = Arc::new(Mutex::new(TurnJournalDeltaBuffer {
            text: String::new(),
            last_flush: Instant::now(),
        }));
        let delta_send_lock = Arc::new(Mutex::new(()));
        let delta_flush_shutdown = CancellationToken::new();
        let delta_flush_handle = spawn_turn_journal_delta_flush_task(
            tx.clone(),
            Arc::clone(&delta_buffer),
            Arc::clone(&delta_send_lock),
            delta_snapshot_interval,
            delta_flush_shutdown.clone(),
        );
        Self {
            tx,
            delta_buffer,
            delta_send_lock,
            delta_snapshot_interval,
            delta_snapshot_chars,
            delta_flush_shutdown,
            delta_flush_handle: Some(delta_flush_handle),
        }
    }

    pub(super) fn send(&self, kind: TurnJournalEventKind, flush: TurnJournalFlush) {
        let _ = self.tx.send(TurnJournalCommand {
            created_at: Utc::now(),
            kind,
            flush,
            ack: None,
        });
    }

    pub(super) fn send_immediate(&self, kind: TurnJournalEventKind) {
        self.send(kind, TurnJournalFlush::Immediate);
    }

    pub(super) fn send_buffered(&self, kind: TurnJournalEventKind) {
        self.send(kind, TurnJournalFlush::Buffered);
    }

    pub(super) fn sink(&self) -> TurnJournalSink {
        TurnJournalSink {
            tx: self.tx.clone(),
        }
    }

    pub(super) fn assistant_delta_flusher(&self) -> TurnJournalDeltaFlusher {
        TurnJournalDeltaFlusher {
            tx: self.tx.clone(),
            delta_buffer: Arc::clone(&self.delta_buffer),
            delta_send_lock: Arc::clone(&self.delta_send_lock),
        }
    }

    pub(super) fn assistant_delta(&mut self, text: String) {
        let should_flush = {
            let Ok(mut buffer) = self.delta_buffer.lock() else {
                return;
            };
            buffer.text.push_str(&text);
            buffer.text.chars().count() >= self.delta_snapshot_chars
                || buffer.last_flush.elapsed() >= self.delta_snapshot_interval
        };
        if should_flush {
            self.flush_assistant_delta();
        }
    }

    pub(super) fn flush_assistant_delta(&mut self) {
        flush_turn_journal_assistant_delta(&self.tx, &self.delta_buffer, &self.delta_send_lock);
    }

    pub(super) async fn finish(mut self, status: TurnJournalStatus) {
        self.delta_flush_shutdown.cancel();
        if let Some(handle) = self.delta_flush_handle.take() {
            let _ = handle.await;
        }
        self.flush_assistant_delta();
        self.send_immediate(TurnJournalEventKind::TurnFinished { status });
    }
}

impl Drop for TurnJournalEmitter {
    fn drop(&mut self) {
        self.delta_flush_shutdown.cancel();
    }
}

fn spawn_turn_journal_delta_flush_task(
    tx: mpsc::UnboundedSender<TurnJournalCommand>,
    delta_buffer: Arc<Mutex<TurnJournalDeltaBuffer>>,
    delta_send_lock: Arc<Mutex<()>>,
    delta_snapshot_interval: Duration,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = time::interval(delta_snapshot_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    flush_turn_journal_assistant_delta(&tx, &delta_buffer, &delta_send_lock);
                }
                _ = shutdown.cancelled() => break,
            }
        }
    })
}

fn take_turn_journal_delta_buffer(
    delta_buffer: &Arc<Mutex<TurnJournalDeltaBuffer>>,
) -> Option<String> {
    let Ok(mut buffer) = delta_buffer.lock() else {
        return None;
    };
    if buffer.text.is_empty() {
        return None;
    }
    buffer.last_flush = Instant::now();
    Some(std::mem::take(&mut buffer.text))
}

fn flush_turn_journal_assistant_delta(
    tx: &mpsc::UnboundedSender<TurnJournalCommand>,
    delta_buffer: &Arc<Mutex<TurnJournalDeltaBuffer>>,
    delta_send_lock: &Arc<Mutex<()>>,
) {
    let Ok(_guard) = delta_send_lock.lock() else {
        return;
    };
    let Some(text) = take_turn_journal_delta_buffer(delta_buffer) else {
        return;
    };
    let _ = tx.send(TurnJournalCommand {
        created_at: Utc::now(),
        kind: TurnJournalEventKind::AssistantDelta { text },
        flush: TurnJournalFlush::Buffered,
        ack: None,
    });
}

pub(super) async fn run_turn_journal_writer(
    path: PathBuf,
    turn_id: String,
    mut rx: mpsc::UnboundedReceiver<TurnJournalCommand>,
) -> anyhow::Result<()> {
    let mut writer = TurnJournalWriter::open(path).await?;
    while let Some(command) = rx.recv().await {
        let result = writer
            .append(
                turn_id.clone(),
                command.created_at,
                command.kind,
                command.flush,
            )
            .await
            .map(|_| ());
        if let Some(ack) = command.ack {
            let _ = ack.send(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}")),
            );
        }
        result?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn fallback_durable_event_flushes_visible_partial_before_waiting_for_ack() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut emitter = TurnJournalEmitter::new(tx, Duration::from_secs(3600), usize::MAX);
        emitter.delta_flush_shutdown.cancel();
        if let Some(handle) = emitter.delta_flush_handle.take() {
            let _ = handle.await;
        }
        emitter.assistant_delta("partial".into());
        let mut recorder = TurnJournalDurableEventRecorder {
            sink: emitter.sink(),
            assistant_delta_flusher: emitter.assistant_delta_flusher(),
        };

        let record = tokio::spawn(async move {
            recorder
                .record(SessionTurnEvent::NonStreamingFallbackAttemptStarted {
                    attempt: 1,
                    max_attempts: 5,
                    previous_error: "stream failed".into(),
                })
                .await
        });

        let delta = rx.recv().await.expect("flushed assistant delta");
        assert!(matches!(
            delta.kind,
            TurnJournalEventKind::AssistantDelta { text } if text == "partial"
        ));
        assert_eq!(delta.flush, TurnJournalFlush::Buffered);
        assert!(delta.ack.is_none());

        let fallback = rx.recv().await.expect("durable fallback state");
        assert!(matches!(
            fallback.kind,
            TurnJournalEventKind::NonStreamingFallbackAttemptStarted {
                attempt: 1,
                max_attempts: 5,
                previous_error,
            } if previous_error == "stream failed"
        ));
        assert_eq!(fallback.flush, TurnJournalFlush::Immediate);
        let ack = fallback.ack.expect("fallback state requires durable ack");
        ack.send(Ok(())).expect("record task still waits for ack");
        assert!(record.await.expect("record task join").is_ok());
    }
}
