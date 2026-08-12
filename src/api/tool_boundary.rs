//! Session turn 的工具派发边界控制。
//!
//! `ToolBoundaryControl` 把取消原因、派发 reservation 与运行中工具的取消 token
//! 放在同一个 session-turn 私有 handle 中，避免检查取消后再派发的 TOCTOU 窗口。

use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use super::{ProviderRecoveryInterrupt, ToolCallSkipReason};

#[derive(Debug, Default)]
struct DispatchState {
    cancelled: Option<ToolCallSkipReason>,
    explicit_cancel: bool,
}

#[derive(Debug)]
struct ToolBoundaryControlInner {
    dispatch: Mutex<DispatchState>,
    cancellation: CancellationToken,
    recovery_cancellation: ProviderRecoveryInterrupt,
}

/// 一个 session turn 独占的工具派发线性化控制面。
#[derive(Clone, Debug)]
pub(crate) struct ToolBoundaryControl {
    inner: Arc<ToolBoundaryControlInner>,
}

impl ToolBoundaryControl {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ToolBoundaryControlInner {
                dispatch: Mutex::new(DispatchState::default()),
                cancellation: CancellationToken::new(),
                recovery_cancellation: ProviderRecoveryInterrupt::new(),
            }),
        }
    }

    /// 线性化 Esc/Ctrl-C 取消请求，并通知已正式派发的运行中工具。
    pub(crate) fn cancel(&self, reason: ToolCallSkipReason) {
        self.set_cancel_reason(reason, true);
    }

    /// 仅在尚未取消时记录中断原因。
    ///
    /// steer 是较弱的中断请求；若用户已经明确 cancel，不能由延迟完成的
    /// steer journal ACK 覆盖最终的 cancelled 语义。
    pub(crate) fn cancel_if_open(&self, reason: ToolCallSkipReason) {
        self.set_cancel_reason(reason, false);
    }

    /// 预留一次工具派发。
    ///
    /// 成功返回即是该工具的 dispatch linearization point；之后的取消只能中断
    /// 已派发调用，不能把它回溯改写为 skipped。
    pub(crate) fn try_reserve_dispatch(&self) -> Result<(), ToolCallSkipReason> {
        let dispatch = lock_dispatch_state(&self.inner.dispatch);
        dispatch.cancelled.map_or(Ok(()), Err)
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel_reason().is_some()
    }

    /// Esc/Ctrl-C 的 hard-cancel 路径可在 grace 后放弃未协作收束的工具 future；steer 不可复用它。
    pub(crate) fn is_explicit_cancel(&self) -> bool {
        lock_dispatch_state(&self.inner.dispatch).explicit_cancel
    }

    pub(crate) fn cancel_reason(&self) -> Option<ToolCallSkipReason> {
        lock_dispatch_state(&self.inner.dispatch).cancelled
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    /// steer 与显式取消都会关闭后续 provider retry、continuation 和 fallback，
    /// 但该 token 不用于打断已经在正常运行的 provider request 或工具调用。
    pub(crate) fn recovery_cancellation_token(&self) -> ProviderRecoveryInterrupt {
        self.inner.recovery_cancellation.clone()
    }

    fn set_cancel_reason(&self, reason: ToolCallSkipReason, replace_existing: bool) {
        let changed = {
            let mut dispatch = lock_dispatch_state(&self.inner.dispatch);
            if dispatch.cancelled.is_none() || replace_existing {
                dispatch.cancelled = Some(reason);
                dispatch.explicit_cancel = replace_existing;
                true
            } else {
                false
            }
        };
        if changed {
            // steer 不打断当前 request/工具，但不能在它失败或结束后再启动 retry、
            // continuation 或 fallback。显式取消额外打断当前运行单元。
            self.inner.recovery_cancellation.cancel();
            if replace_existing {
                self.inner.cancellation.cancel();
            }
        }
    }
}

fn lock_dispatch_state(
    dispatch: &Mutex<DispatchState>,
) -> std::sync::MutexGuard<'_, DispatchState> {
    match dispatch.lock() {
        Ok(dispatch) => dispatch,
        // 此 mutex 只保存 Copy 的取消状态；即使此前持锁代码 panic，也可安全读取
        // 已写入的状态并继续收束当前 turn。
        Err(poisoned) => poisoned.into_inner(),
    }
}
