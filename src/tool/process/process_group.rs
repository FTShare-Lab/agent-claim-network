use tokio::process::Command;

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

/// 等待直属 root child 退出但保留其 waitable 状态。调用方可在随后 `Child::wait()` 真正
/// reap 前安全地向同组后代发信号：root zombie 仍占用原 PID/PGID，不会命中后来复用该数值
/// 的无关进程组。
pub(crate) fn wait_for_child_exit_without_reap(process_id: i32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let process_id = libc::id_t::try_from(process_id).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "root child PID cannot be represented as libc::id_t",
            )
        })?;
        loop {
            // SAFETY: `process_id` is the PID returned by a child process spawned directly by
            // ACN. WNOWAIT asks the kernel to leave it waitable; no other code waits this child
            // before the caller later invokes the owning Child::wait().
            let result = unsafe {
                let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
                libc::waitid(
                    libc::P_PID,
                    process_id,
                    info.as_mut_ptr(),
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            if result == 0 {
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error);
        }
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
        configure_process_group, reap_direct_child_blocking, signal_process_group,
        wait_for_child_exit_without_reap, ProcessGroupSignalResult,
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

        tokio::task::spawn_blocking(move || wait_for_child_exit_without_reap(process_group_id))
            .await
            .expect("exit observer worker should join")
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
