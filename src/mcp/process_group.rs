//! stdio MCP 的 Unix 进程组所有权与收束。
//!
//! MCP child 仍由 `McpConnectionManager` 独占，不进入 Tool `ProcessManager`。本模块只复用
//! 已验证的 PGID 原语，确保 wrapper root 退出后，同组后代在 root 被 reap 前完成清理。

use process_wrap::tokio::CommandWrap;
use tokio::process::Command;

/// 把 stdio command 转成 rmcp 可接受的包装命令；非 Unix 保持原有直接 child 语义。
pub(super) fn wrap_stdio_command(command: Command) -> CommandWrap {
    let mut wrapped = CommandWrap::from(command);
    #[cfg(unix)]
    wrapped.wrap(McpProcessGroup);
    wrapped
}

#[cfg(unix)]
mod unix {
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::process::ExitStatus;

    use process_wrap::tokio::{ChildWrapper, CommandWrap, CommandWrapper};
    use tokio::process::Command;

    use crate::tool::{
        configure_process_group, observe_child_exit_without_reap, reap_direct_child_blocking,
        spawn_direct_child_reaper, terminate_process_group,
    };

    /// 为 root 建立独立 PGID，并把 rmcp 的 kill/wait 扩展到同组后代。
    #[derive(Debug)]
    pub(super) struct McpProcessGroup;

    impl CommandWrapper for McpProcessGroup {
        fn pre_spawn(&mut self, command: &mut Command, _core: &CommandWrap) -> io::Result<()> {
            configure_process_group(command);
            Ok(())
        }

        fn wrap_child(
            &mut self,
            child: Box<dyn ChildWrapper>,
            _core: &CommandWrap,
        ) -> io::Result<Box<dyn ChildWrapper>> {
            let process_group_id = child
                .id()
                .ok_or_else(|| io::Error::other("stdio MCP child 没有可用 PID"))
                .and_then(|pid| {
                    i32::try_from(pid)
                        .map_err(|_| io::Error::other("stdio MCP child PID 超出 i32 范围"))
                })?;
            Ok(Box::new(McpProcessGroupChild {
                inner: Some(child),
                process_group_id,
                exit_status: None,
                cleanup_armed: true,
            }))
        }
    }

    #[derive(Debug)]
    struct McpProcessGroupChild {
        inner: Option<Box<dyn ChildWrapper>>,
        process_group_id: i32,
        exit_status: Option<ExitStatus>,
        cleanup_armed: bool,
    }

    impl ChildWrapper for McpProcessGroupChild {
        fn inner(&self) -> &dyn ChildWrapper {
            // `inner` 只会在消费整个 wrapper 的 `into_inner` 中取走；该调用之后 self
            // 已不再可访问，所以所有借用式 ChildWrapper 方法都必然处于 Some 状态。
            self.inner
                .as_deref()
                .expect("stdio MCP child wrapper inner 必须在消费前存在")
                .inner()
        }

        fn inner_mut(&mut self) -> &mut dyn ChildWrapper {
            self.inner
                .as_deref_mut()
                .expect("stdio MCP child wrapper inner 必须在消费前存在")
                .inner_mut()
        }

        fn into_inner(mut self: Box<Self>) -> Box<dyn ChildWrapper> {
            self.cleanup_armed = false;
            self.inner
                .take()
                .expect("stdio MCP child wrapper inner 只能消费一次")
                .into_inner()
        }

        fn start_kill(&mut self) -> io::Result<()> {
            terminate_process_group(self.process_group_id, libc::SIGKILL).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "终止 stdio MCP 进程组 {} 失败: {error}",
                        self.process_group_id
                    ),
                )
            })
        }

        fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            // 不能调用底层 `try_wait`，它会先 reap root 并释放 PGID。保守返回 running，
            // 唯一 owner 随后的 `wait` 会完成无竞态的退出观察和清理。
            Ok(self.exit_status)
        }

        fn wait(&mut self) -> Pin<Box<dyn Future<Output = io::Result<ExitStatus>> + Send + '_>> {
            Box::pin(async move {
                if let Some(status) = self.exit_status {
                    return Ok(status);
                }

                let process_group_id = self.process_group_id;
                let observed = observe_child_exit_without_reap(process_group_id).await;

                if let Err(error) = observed {
                    // 观察失败时仍优先清理整组并回收 root，避免把诊断错误升级成进程泄漏。
                    let _ = terminate_process_group(process_group_id, libc::SIGKILL);
                    if let Ok(status) = self.inner_mut().wait().await {
                        self.exit_status = Some(status);
                        self.cleanup_armed = false;
                    }
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "观察 stdio MCP root child {} 退出失败: {error}",
                            self.process_group_id
                        ),
                    ));
                }

                // root 仍以 zombie 形式占有 PID/PGID；此时 killpg 不可能命中复用后的无关组。
                let cleanup = match terminate_process_group(process_group_id, libc::SIGKILL) {
                    Ok(()) => Ok(()),
                    // macOS 在组内只剩不可 signal 的 zombie leader 时可能返回 EPERM，
                    // 而不是 Linux 常见的 ESRCH。root 已由 waitid 确认退出且仍未 reap，
                    // 该结果表示当前组没有 ACN 能继续清理的存活成员。
                    Err(error) if error.kind() == io::ErrorKind::PermissionDenied => Ok(()),
                    Err(error) => Err(io::Error::new(
                        error.kind(),
                        format!("清理 stdio MCP 残留进程组 {process_group_id} 失败: {error}"),
                    )),
                };
                let status = self.inner_mut().wait().await.map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("回收 stdio MCP root child {process_group_id} 失败: {error}"),
                    )
                })?;
                // root 已真正 reap；必须先解除 Drop guard，再传播残留 group 的诊断错误，
                // 否则析构时旧 PGID 可能已经被复用并误伤无关进程。
                self.exit_status = Some(status);
                self.cleanup_armed = false;
                cleanup?;
                Ok(status)
            })
        }

        fn signal(&self, signal: i32) -> io::Result<()> {
            terminate_process_group(self.process_group_id, signal).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "向 stdio MCP 进程组 {} 发送信号 {signal} 失败: {error}",
                        self.process_group_id
                    ),
                )
            })
        }
    }

    impl Drop for McpProcessGroupChild {
        fn drop(&mut self) {
            if !self.cleanup_armed {
                return;
            }
            self.cleanup_armed = false;

            // future 取消和 runtime shutdown 都不能依赖另一个 Tokio task 继续 poll：
            // 先同步杀整个组，再把直属 root 交给独立 OS thread 回收。
            if let Err(error) = terminate_process_group(self.process_group_id, libc::SIGKILL) {
                log::warn!(
                    target: "mcp",
                    "Drop 清理 stdio MCP 进程组 {} 失败: {error}",
                    self.process_group_id
                );
            }
            if let Err(error) = spawn_direct_child_reaper(self.process_group_id) {
                log::warn!(
                    target: "mcp",
                    "启动 stdio MCP root {} 独立 reaper 失败: {error}; 改为当前线程回收",
                    self.process_group_id
                );
                reap_direct_child_blocking(self.process_group_id);
            }
        }
    }
}

#[cfg(unix)]
use unix::McpProcessGroup;

#[cfg(all(test, unix))]
mod tests {
    use std::process::Stdio;
    use std::time::Duration;

    use tokio::process::Command;
    use tokio::time;

    use super::wrap_stdio_command;

    #[tokio::test]
    async fn cancelled_wait_can_still_kill_and_reap_process_group() {
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 30"]);
        let mut child = wrap_stdio_command(command).spawn().unwrap();

        let first_wait = time::timeout(Duration::from_millis(50), child.wait()).await;
        assert!(first_wait.is_err(), "fixture root 不应在超时前退出");

        Box::into_pin(child.kill()).await.unwrap();
    }

    #[tokio::test]
    async fn dropping_child_kills_process_group_and_reaps_root() {
        let dir = tempfile::tempdir().unwrap();
        let descendant_pid_path = dir.path().join("descendant.pid");
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "sh -c 'trap \"\" HUP TERM; exec sleep 30' & \
                 printf '%s\\n' \"$!\" > \"$MCP_DESCENDANT_PID_FILE\"; \
                 while :; do sleep 1; done",
            ])
            .env("MCP_DESCENDANT_PID_FILE", &descendant_pid_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = wrap_stdio_command(command).spawn().unwrap();
        let root_pid = child.id().unwrap();
        let descendant_pid = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(pid) = tokio::fs::read_to_string(&descendant_pid_path).await {
                    break pid.trim().to_string();
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();

        drop(child);

        for pid in [root_pid.to_string(), descendant_pid] {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let status = Command::new("kill")
                        .args(["-0", &pid])
                        .stderr(Stdio::null())
                        .status()
                        .await;
                    if !matches!(status, Ok(status) if status.success()) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("Drop 后 stdio MCP PID {pid} 仍存活或未回收"));
        }
    }
}
