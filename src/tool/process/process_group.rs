use tokio::process::Command;

const EXIT_OBSERVER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(50);

/// 向受管 PGID 发送信号后的内核确认结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessGroupSignalResult {
    Delivered,
    AlreadyExited,
}

/// 为 Unix command 建立独立 process group，供受管 session 的 interrupt/cleanup 使用。
pub(crate) fn configure_process_group(command: &mut Command) {
    #[cfg(unix)]
    {
        // Tokio 在 Unix 上将该值传给 setpgid；root child 会成为独立组长。
        command.process_group(0);
    }
}

/// 异步等待直属 root child 退出但保留其 waitable 状态。
///
/// Linux 优先通过 pidfd 接入 Tokio reactor；其他 Unix 或 pidfd 不可用时，以
/// `waitid(WNOHANG | WNOWAIT)` 低频轮询。两条路径都不会长期占用 blocking worker。
pub(crate) async fn observe_child_exit_without_reap(process_id: i32) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        match observe_child_exit_with_pidfd(process_id).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                log::debug!(
                    target: "tool",
                    "pidfd observer unavailable for child {process_id}; falling back to waitid polling: {error}"
                );
            }
        }
    }

    #[cfg(unix)]
    loop {
        if child_exit_waitable(process_id)? {
            return Ok(());
        }
        tokio::time::sleep(EXIT_OBSERVER_POLL_INTERVAL).await;
    }

    #[cfg(not(unix))]
    {
        let _ = process_id;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "background process groups are only supported on Unix",
        ))
    }
}

#[cfg(unix)]
fn child_exit_waitable(process_id: i32) -> std::io::Result<bool> {
    let process_id_as_id = libc::id_t::try_from(process_id).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "root child PID cannot be represented as libc::id_t",
        )
    })?;
    loop {
        let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        // SAFETY: process_id 是 ACN 直属 child。WNOHANG 保证这里不会阻塞；WNOWAIT
        // 保留 waitable 状态，后续仍由持有 Child handle 的 watcher 唯一 reap。
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                process_id_as_id,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
            )
        };
        if result == 0 {
            // POSIX 要求 WNOHANG 未观察到状态时不返回 child 事件。缓冲区预先清零，
            // 因而 si_signo!=0 表示本次确实取得了 SIGCHLD 状态。
            let info = unsafe { info.assume_init() };
            return Ok(info.si_signo != 0);
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(error);
    }
}

#[cfg(target_os = "linux")]
async fn observe_child_exit_with_pidfd(process_id: i32) -> std::io::Result<()> {
    use std::os::fd::{FromRawFd, OwnedFd};

    // SAFETY: pidfd_open 不转移其他 fd 的所有权；成功返回的新 fd 立即交给 OwnedFd。
    let raw_fd = unsafe { libc::syscall(libc::SYS_pidfd_open, process_id, 0) };
    if raw_fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let raw_fd = i32::try_from(raw_fd)
        .map_err(|_| std::io::Error::other("pidfd cannot be represented as a raw fd"))?;
    // SAFETY: raw_fd 是本函数刚刚成功创建且尚未被任何 owner 接管的 pidfd。
    let pidfd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    let pidfd = tokio::io::unix::AsyncFd::new(pidfd)?;
    loop {
        let mut ready = pidfd.readable().await?;
        if child_exit_waitable(process_id)? {
            return Ok(());
        }
        // 极少数伪就绪时重新向 reactor 注册；真正退出后的 pidfd 会持续 readable。
        ready.clear_ready();
    }
}

/// 在独立 OS thread 中回收一个直属 child。Drop 路径不能依赖 Tokio runtime 仍会继续 poll，
/// 因此在已经同步终止进程组后，用该 worker 确保 root zombie 也能被 waitpid 回收。
pub(crate) fn spawn_direct_child_reaper(process_id: i32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::thread::Builder::new()
            .name("acn-process-reaper".into())
            .spawn(move || reap_direct_child_blocking(process_id))
            .map(|_| ())
    }
    #[cfg(not(unix))]
    {
        let _ = process_id;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "background process groups are only supported on Unix",
        ))
    }
}

/// 同步回收一个已被终止的直属 child。仅用于 detached reaper 线程无法创建时的兜底；
/// 此时继续依赖即将关闭的 Tokio runtime 会留下 zombie，因此宁可在 Drop 路径短暂等待。
pub(crate) fn reap_direct_child_blocking(process_id: i32) {
    #[cfg(unix)]
    loop {
        let mut status = 0;
        // SAFETY: process_id 来自 ACN 直属 spawn 的 child；调用方保证此前尚未把它交给
        // 其他 reaper。killpg 已先同步发出 SIGKILL，因此这里只等待该 child 的退出/回收。
        let result = unsafe { libc::waitpid(process_id, &mut status, 0) };
        if result == process_id {
            return;
        }
        if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        // ECHILD 表示 child 已被系统/唯一所有者回收；两者都无需重试。
        return;
    }
    #[cfg(not(unix))]
    {
        let _ = process_id;
    }
}

/// 向受管 PGID 发送信号，并保留 `ESRCH` 供调用方区分自然退出。
pub(crate) fn signal_process_group(
    process_group_id: i32,
    signal: i32,
) -> std::io::Result<ProcessGroupSignalResult> {
    #[cfg(unix)]
    {
        // SAFETY: negative PID 是 POSIX `kill` 对由 ACN 创建且已登记的 process group
        // 的规范寻址方式；调用方只传入进程管理器保存的 group leader PID。
        let result = unsafe { libc::kill(-process_group_id, signal) };
        if result == -1 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(ProcessGroupSignalResult::AlreadyExited);
            }
            return Err(error);
        }
        Ok(ProcessGroupSignalResult::Delivered)
    }
    #[cfg(not(unix))]
    {
        let _ = (process_group_id, signal);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "background process groups are only supported on Unix",
        ))
    }
}

/// 清理路径把目标已自然退出视为成功，避免 shutdown/eviction 因竞态失败。
pub(crate) fn terminate_process_group(process_group_id: i32, signal: i32) -> std::io::Result<()> {
    let _ = signal_process_group(process_group_id, signal)?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::process::Stdio;
    use std::time::Duration;

    use tokio::process::Command;

    use super::{
        configure_process_group, observe_child_exit_without_reap, reap_direct_child_blocking,
        signal_process_group, ProcessGroupSignalResult,
    };

    #[tokio::test]
    async fn exit_observation_keeps_pgid_owned_until_residual_cleanup() {
        let mut command = Command::new("bash");
        command
            .args(["-lc", "sleep 300 & exit 0"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_process_group(&mut command);
        let mut child = command.spawn().expect("fixture should spawn");
        let process_group_id = child
            .id()
            .and_then(|pid| i32::try_from(pid).ok())
            .expect("Unix child PID should fit the managed PGID type");

        observe_child_exit_without_reap(process_group_id)
            .await
            .expect("waitid WNOWAIT should preserve the root child for later reap");

        assert_eq!(
            signal_process_group(process_group_id, libc::SIGKILL)
                .expect("the still-owned PGID should accept residual cleanup"),
            ProcessGroupSignalResult::Delivered
        );
        let _ = child
            .wait()
            .await
            .expect("root child should remain reapable");

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                // SAFETY: this test owns the group created by configure_process_group and only
                // probes until its explicitly signalled residual child has exited.
                let result = unsafe { libc::kill(-process_group_id, 0) };
                if result != 0
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("residual process group should be gone after cleanup");
    }

    #[test]
    fn async_exit_observer_does_not_depend_on_tokio_blocking_capacity() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("fixture runtime should build");
        runtime.block_on(async {
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                let _ = release_rx.recv();
            });
            tokio::task::yield_now().await;

            let mut command = Command::new("bash");
            command
                .args(["-lc", "exit 0"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            configure_process_group(&mut command);
            let mut child = command.spawn().expect("fixture should spawn");
            let process_id = child
                .id()
                .and_then(|pid| i32::try_from(pid).ok())
                .expect("fixture PID should fit");

            tokio::time::timeout(
                Duration::from_secs(3),
                observe_child_exit_without_reap(process_id),
            )
            .await
            .expect("observer should not wait for blocking capacity")
            .expect("observer should see child exit");
            child.wait().await.expect("child should remain reapable");

            let _ = release_tx.send(());
            occupied
                .await
                .expect("blocking capacity fixture should stop");
        });
    }

    #[tokio::test]
    async fn inline_reaper_collects_child_when_detached_reaper_cannot_start() {
        let child = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("fixture child should spawn");
        let process_id = child
            .id()
            .and_then(|pid| i32::try_from(pid).ok())
            .expect("fixture child PID should fit the managed PID type");

        // SAFETY: this test owns the direct child and signals only its recorded PID.
        assert_eq!(unsafe { libc::kill(process_id, libc::SIGKILL) }, 0);
        tokio::task::spawn_blocking(move || reap_direct_child_blocking(process_id))
            .await
            .expect("inline reaper worker should join");
        // SAFETY: the helper above is this test's only waiter for the direct child; ECHILD proves
        // it did not merely signal the child but actually collected the zombie.
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(process_id, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
        drop(child);
    }
}
