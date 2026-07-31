//! portable-pty 的最小封装：只在创建时设置固定逻辑行列数。

use std::fs::File;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use portable_pty::{native_pty_system, CommandBuilder, MasterPty, PtySize};

use super::process_group::{reap_direct_child_blocking, terminate_process_group};

/// 已创建但尚未交由 watcher 接管的 PTY 资源。
pub(crate) struct PtySpawned {
    child: Option<Box<dyn portable_pty::Child + Send + Sync>>,
    master: Option<Box<dyn MasterPty + Send>>,
    /// 独立、非阻塞的 master fd 副本。watcher 通过 io_stop 主动结束 polling，不能
    /// 依赖 abort spawn_blocking 或只 drop 原 master 来回收它们。
    reader: Option<File>,
    writer: Option<File>,
    pub(crate) io_stop: Arc<AtomicBool>,
    pub(crate) process_group_id: Option<i32>,
    cleanup_armed: bool,
}

/// watcher 接管 PTY 后持有的资源。只有成功构造它才会解除 `PtySpawned` 的 spawn guard。
pub(crate) struct PtyWatcherParts {
    pub(crate) child: Box<dyn portable_pty::Child + Send + Sync>,
    pub(crate) master: Box<dyn MasterPty + Send>,
    pub(crate) reader: File,
    pub(crate) writer: File,
    pub(crate) io_stop: Arc<AtomicBool>,
    pub(crate) process_group_id: Option<i32>,
}

impl std::fmt::Debug for PtySpawned {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PtySpawned")
            .field("process_group_id", &self.process_group_id)
            .finish()
    }
}

impl PtySpawned {
    /// 失败路径显式转交 child 给 reaper；之后 Drop 只关闭其余 PTY fd。
    pub(crate) fn into_unregistered_child(
        mut self,
    ) -> Option<(Box<dyn portable_pty::Child + Send + Sync>, Option<i32>)> {
        self.cleanup_armed = false;
        self.child
            .take()
            .map(|child| (child, self.process_group_id))
    }

    /// watcher 已经取得 child、master 与 I/O fd，此时才可解除 spawn guard。
    pub(crate) fn into_watcher_parts(mut self) -> Option<PtyWatcherParts> {
        let child = self.child.take()?;
        let master = self.master.take()?;
        let reader = self.reader.take()?;
        let writer = self.writer.take()?;
        self.cleanup_armed = false;
        Some(PtyWatcherParts {
            child,
            master,
            reader,
            writer,
            io_stop: Arc::clone(&self.io_stop),
            process_group_id: self.process_group_id,
        })
    }
}

impl Drop for PtySpawned {
    fn drop(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        self.io_stop
            .store(true, std::sync::atomic::Ordering::Release);
        let Some(mut child) = self.child.take() else {
            return;
        };
        let process_group_id = self.process_group_id;
        let child_process_id = child.process_id().and_then(|pid| i32::try_from(pid).ok());
        // runtime shutdown 时 spawn_blocking 可能不会再被 poll；必须先同步把整个组杀掉，
        // 再交给独立 OS thread 回收 root child。
        if let Some(process_group_id) = process_group_id {
            let _ = terminate_process_group(process_group_id, libc::SIGKILL);
        }
        let cleanup = move || {
            let _ = child.wait();
        };
        if let Err(error) = std::thread::Builder::new()
            .name("acn-pty-reaper".into())
            .spawn(cleanup)
        {
            log::warn!(
                target: "tool",
                "failed to start detached PTY child reaper after synchronous killpg: {error}; reaping inline"
            );
            if let Some(child_process_id) = child_process_id {
                // thread 已经确认创建失败，closure 中的 portable-pty handle 会被 drop；PID
                // 仍是本次直属 child，必须直接 waitpid，不能把 zombie 留给 runtime shutdown。
                reap_direct_child_blocking(child_process_id);
            }
        }
    }
}

/// 创建 Unix PTY。调用方在登记成功前仍必须持有进程组 kill guard。
pub(crate) fn spawn_pty(
    program: &str,
    args: &[String],
    cwd: &Path,
    environment: &[(String, String)],
    removed_environment: &[String],
    rows: u16,
    cols: u16,
) -> Result<PtySpawned, String> {
    #[cfg(not(unix))]
    {
        let _ = (
            program,
            args,
            cwd,
            environment,
            removed_environment,
            rows,
            cols,
        );
        return Err("tty code_run is only supported on Unix in this release".into());
    }

    #[cfg(unix)]
    {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|error| format!("open PTY failed: {error}"))?;
        let mut command = CommandBuilder::new(program);
        command.cwd(cwd);
        for arg in args {
            command.arg(arg);
        }
        for (key, value) in environment {
            command.env(key, value);
        }
        for key in removed_environment {
            command.env_remove(key);
        }
        let mut child = pair
            .slave
            .spawn_command(command)
            .map_err(|error| format!("spawn PTY child failed: {error}"))?;
        let process_group_id = child.process_id().and_then(|pid| i32::try_from(pid).ok());
        let master_fd = match pair.master.as_raw_fd() {
            Some(master_fd) => master_fd,
            None => {
                kill_and_reap_pty_child(&mut *child, process_group_id);
                return Err("PTY master does not expose a Unix file descriptor".into());
            }
        };
        let reader = match duplicate_nonblocking_fd(master_fd, "reader") {
            Ok(reader) => reader,
            Err(error) => {
                kill_and_reap_pty_child(&mut *child, process_group_id);
                return Err(error);
            }
        };
        let writer = match duplicate_nonblocking_fd(master_fd, "writer") {
            Ok(writer) => writer,
            Err(error) => {
                kill_and_reap_pty_child(&mut *child, process_group_id);
                return Err(error);
            }
        };
        Ok(PtySpawned {
            child: Some(child),
            master: Some(pair.master),
            reader: Some(reader),
            writer: Some(writer),
            io_stop: Arc::new(AtomicBool::new(false)),
            process_group_id,
            cleanup_armed: true,
        })
    }
}

#[cfg(unix)]
fn kill_and_reap_pty_child(
    child: &mut (dyn portable_pty::Child + Send + Sync),
    process_group_id: Option<i32>,
) {
    if let Some(process_group_id) = process_group_id {
        let _ = terminate_process_group(process_group_id, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(unix)]
fn duplicate_nonblocking_fd(fd: std::os::fd::RawFd, label: &str) -> Result<File, String> {
    // F_DUPFD_CLOEXEC 原子地复制并设置 close-on-exec。普通 code_run 后续会 fork/exec；
    // 不能让它继承其他 owner 仍在运行的 PTY reader/writer fd。
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(format!(
            "duplicate PTY {label} fd failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: duplicate 是上方成功返回的唯一 Unix fd，File 取得它的关闭所有权。
    let file = unsafe { std::os::fd::FromRawFd::from_raw_fd(duplicate) };
    let flags = unsafe { libc::fcntl(std::os::fd::AsRawFd::as_raw_fd(&file), libc::F_GETFL) };
    if flags < 0 {
        return Err(format!(
            "read PTY {label} fd flags failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    // file 仍持有 duplicate；失败返回时 Drop 会关闭它。
    let set_result = unsafe {
        libc::fcntl(
            std::os::fd::AsRawFd::as_raw_fd(&file),
            libc::F_SETFL,
            flags | libc::O_NONBLOCK,
        )
    };
    if set_result < 0 {
        return Err(format!(
            "set PTY {label} fd nonblocking failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(file)
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    use tokio::time::{sleep, timeout};

    use super::{duplicate_nonblocking_fd, spawn_pty};

    #[test]
    fn duplicated_pty_io_fd_is_close_on_exec() {
        let (source, _peer) = UnixStream::pair().expect("Unix stream fixture should open");
        let duplicate = duplicate_nonblocking_fd(source.as_raw_fd(), "test")
            .expect("PTY fd duplication should succeed");
        let descriptor_flags = unsafe { libc::fcntl(duplicate.as_raw_fd(), libc::F_GETFD) };
        assert!(descriptor_flags >= 0, "F_GETFD should succeed");
        assert_ne!(
            descriptor_flags & libc::FD_CLOEXEC,
            0,
            "duplicated PTY descriptor must not survive a later code_run exec"
        );
    }

    #[tokio::test]
    async fn dropping_unregistered_pty_reaps_its_process_group() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("pty.pid");
        let script = format!("echo $$ > '{}'; sleep 300", pid_path.display());
        let spawned = spawn_pty(
            "bash",
            &["-lc".into(), script],
            dir.path(),
            &[],
            &[],
            24,
            80,
        )
        .unwrap();
        let process_group_id = spawned
            .process_group_id
            .expect("Unix PTY child should be its own process group leader");

        drop(spawned);

        timeout(Duration::from_secs(3), async {
            loop {
                let result = unsafe { libc::kill(-process_group_id, 0) };
                if result != 0
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("dropping an unregistered PTY must reap its process group");
    }
}
