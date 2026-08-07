//! DeepSWE/Pier 调用的非交互单 attempt 入口。
//!
//! 模型 key 由 Pier 注入初始环境；启动时先经匿名 pipe 原位 re-exec，随后才让
//! ACN config 的 `[agent.llm].api_key_env` 读取。这样 key 不会留在 `/proc/*/environ`。

#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::PathBuf;
#[cfg(unix)]
use std::process::Command;

use agent_claim_network::evaluation::{
    load_attempt_config, run_attempt, EvaluationResult, EVALUATION_MODEL_KEY_ENV,
};
use anyhow::Context;

const MODEL_KEY_FD_ENV: &str = "ACN_EVAL_MODEL_KEY_FD";
// POSIX 至少保证 PIPE_BUF 为 512 bytes；保持在此上限内可在 re-exec 前一次写入空 pipe。
const MAX_MODEL_KEY_BYTES: usize = 512;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = parse_cli(std::env::args().collect())?;
    bootstrap_model_key()?;
    let config = load_attempt_config(&config_path).await?;
    let result = run_attempt(config).await?;
    print_result_path(&result)?;
    if result.exit_type != "completed" {
        anyhow::bail!("评测 attempt 失败；详见 result JSON（不含 credential）");
    }
    Ok(())
}

#[cfg(unix)]
fn bootstrap_model_key() -> anyhow::Result<()> {
    match std::env::var_os(MODEL_KEY_FD_ENV) {
        Some(fd) => install_model_key_from_fd(fd),
        None => reexec_without_model_key(),
    }
}

#[cfg(not(unix))]
fn bootstrap_model_key() -> anyhow::Result<()> {
    anyhow::bail!("acn_eval DeepSWE credential handoff requires Unix")
}

#[cfg(unix)]
fn reexec_without_model_key() -> anyhow::Result<()> {
    let model_key = std::env::var(EVALUATION_MODEL_KEY_ENV)
        .with_context(|| format!("缺少 {EVALUATION_MODEL_KEY_ENV}，无法启动 DeepSWE attempt"))?;
    validate_model_key(&model_key)?;

    let reader = model_key_pipe(model_key.as_bytes())?;
    let args = std::env::args_os().collect::<Vec<_>>();
    let mut command = sanitized_reexec_command(reader.as_raw_fd(), &args)?;
    use std::os::unix::process::CommandExt;
    let error = command.exec();
    Err(error).context("acn_eval credential sanitization re-exec 失败")
}

#[cfg(unix)]
fn install_model_key_from_fd(fd_value: OsString) -> anyhow::Result<()> {
    let fd_text = fd_value
        .to_str()
        .context("ACN_EVAL_MODEL_KEY_FD 不是 UTF-8")?;
    let fd = parse_model_key_fd(fd_text)?;
    // FD 标识只用于这一次交接；后续子进程不应继承它。
    std::env::remove_var(MODEL_KEY_FD_ENV);
    let model_key = read_model_key_from_fd(fd)?;
    validate_model_key(&model_key)?;
    disable_process_dumping()?;
    std::env::set_var(EVALUATION_MODEL_KEY_ENV, model_key);
    Ok(())
}

#[cfg(target_os = "linux")]
fn disable_process_dumping() -> anyhow::Result<()> {
    // SAFETY: PR_SET_DUMPABLE 仅修改当前 acn_eval 进程的 dumpable 标志；0 不需要指针参数。
    if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
        return Err(std::io::Error::last_os_error()).context("关闭 acn_eval 进程 dump 权限失败");
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn disable_process_dumping() -> anyhow::Result<()> {
    anyhow::bail!("acn_eval DeepSWE credential handoff requires Linux PR_SET_DUMPABLE support")
}

#[cfg(unix)]
fn parse_model_key_fd(value: &str) -> anyhow::Result<libc::c_int> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        anyhow::bail!("ACN_EVAL_MODEL_KEY_FD 无效")
    }
    let fd = value
        .parse::<libc::c_int>()
        .context("ACN_EVAL_MODEL_KEY_FD 无效")?;
    if fd < 3 {
        anyhow::bail!("ACN_EVAL_MODEL_KEY_FD 无效")
    }
    Ok(fd)
}

#[cfg(unix)]
fn model_key_pipe(model_key: &[u8]) -> anyhow::Result<File> {
    if model_key.is_empty() || model_key.len() > MAX_MODEL_KEY_BYTES {
        anyhow::bail!("模型 key 为空或超过安全交接长度限制")
    }
    let mut fds = [-1; 2];
    // SAFETY: fds 指向两个 `c_int` 的有效可写空间；成功后立即由 File 接管两个 fd。
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("创建模型 key pipe 失败");
    }
    // SAFETY: pipe 成功返回两个唯一的有效 fd，下面的 File 取得关闭所有权。
    let reader = unsafe { File::from_raw_fd(fds[0]) };
    // SAFETY: 与 reader 相同，fds[1] 是 pipe 的独立写端。
    let mut writer = unsafe { File::from_raw_fd(fds[1]) };
    make_fd_inheritable(reader.as_raw_fd())?;
    writer
        .write_all(model_key)
        .context("写入模型 key pipe 失败")?;
    drop(writer);
    Ok(reader)
}

#[cfg(unix)]
fn make_fd_inheritable(fd: libc::c_int) -> anyhow::Result<()> {
    // SAFETY: fd 是本函数调用方持有的有效 reader fd；fcntl 不会取得其所有权。
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error()).context("读取模型 key pipe fd 标志失败");
    }
    // pipe 的读端必须跨越这一次 exec；显式清除 close-on-exec，避免平台或调用方状态影响。
    // SAFETY: fd 仍由调用方持有，F_SETFD 仅更新该 fd 的 close-on-exec 标志。
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } != 0 {
        return Err(std::io::Error::last_os_error()).context("设置模型 key pipe fd 可继承失败");
    }
    Ok(())
}

#[cfg(unix)]
fn sanitized_reexec_command(fd: libc::c_int, args: &[OsString]) -> anyhow::Result<Command> {
    if fd < 3 {
        anyhow::bail!("模型 key pipe fd 无效")
    }
    let executable = std::env::current_exe().context("无法定位 acn_eval 可执行文件")?;
    let mut command = Command::new(executable);
    command.args(args.iter().skip(1));
    command.env_remove(EVALUATION_MODEL_KEY_ENV);
    command.env(MODEL_KEY_FD_ENV, fd.to_string());
    Ok(command)
}

#[cfg(unix)]
fn read_model_key_from_fd(fd: libc::c_int) -> anyhow::Result<String> {
    if fd < 3 {
        anyhow::bail!("ACN_EVAL_MODEL_KEY_FD 无效")
    }
    ensure_model_key_pipe(fd)?;
    // SAFETY: fd 仅来自上一步创建并传递的 pipe；File 现在负责在读取后立即关闭它。
    let reader = unsafe { File::from_raw_fd(fd) };
    let mut bytes = Vec::new();
    let mut limited_reader = reader.take((MAX_MODEL_KEY_BYTES + 1) as u64);
    limited_reader
        .read_to_end(&mut bytes)
        .context("读取模型 key pipe 失败")?;
    if bytes.len() > MAX_MODEL_KEY_BYTES {
        anyhow::bail!("模型 key 超过安全交接长度限制")
    }
    String::from_utf8(bytes).context("模型 key 不是 UTF-8")
}

#[cfg(unix)]
fn ensure_model_key_pipe(fd: libc::c_int) -> anyhow::Result<()> {
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: status 是 libc::stat 的有效可写空间；成功时 fstat 会完整初始化它。
    if unsafe { libc::fstat(fd, status.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("ACN_EVAL_MODEL_KEY_FD 无效");
    }
    // SAFETY: 上方 fstat 已成功完整写入 status。
    let status = unsafe { status.assume_init() };
    if status.st_mode & libc::S_IFMT != libc::S_IFIFO {
        anyhow::bail!("ACN_EVAL_MODEL_KEY_FD 必须引用 pipe")
    }
    Ok(())
}

fn validate_model_key(model_key: &str) -> anyhow::Result<()> {
    if model_key.is_empty() || model_key.len() > MAX_MODEL_KEY_BYTES || model_key.contains('\0') {
        anyhow::bail!("模型 key 为空或超过安全交接长度限制")
    }
    Ok(())
}

fn parse_cli(args: Vec<String>) -> anyhow::Result<PathBuf> {
    let mut config = None;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                config = Some(PathBuf::from(
                    args.get(index).context("--config 缺少绝对路径")?,
                ));
            }
            "--help" | "-h" => {
                println!("Usage: acn_eval --config <absolute attempt.toml>");
                std::process::exit(0);
            }
            other => anyhow::bail!("未知参数: {other}"),
        }
        index += 1;
    }
    let config = config.context("必须传入 --config <absolute attempt.toml>")?;
    if !config.is_absolute() {
        anyhow::bail!("--config 必须是绝对路径: {}", config.display());
    }
    Ok(config)
}

fn print_result_path(result: &EvaluationResult) -> anyhow::Result<()> {
    let parent = result
        .event_ledger_path
        .parent()
        .context("result event ledger path 缺少父目录")?;
    println!("{}", parent.join("result.json").display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::ffi::OsStr;
    #[cfg(unix)]
    use std::os::fd::IntoRawFd;

    #[test]
    fn cli_requires_an_absolute_config_path() {
        assert!(parse_cli(vec![
            "acn_eval".into(),
            "--config".into(),
            "run.toml".into()
        ])
        .is_err());
        assert!(parse_cli(vec!["acn_eval".into()]).is_err());
        assert!(parse_cli(vec![
            "acn_eval".into(),
            "--capability-file".into(),
            "/tmp/cap".into()
        ])
        .is_err());
        assert_eq!(
            parse_cli(vec![
                "acn_eval".into(),
                "--config".into(),
                "/tmp/run.toml".into()
            ])
            .unwrap(),
            PathBuf::from("/tmp/run.toml")
        );
    }

    #[cfg(unix)]
    #[test]
    fn pipe_round_trip_reads_and_closes_the_model_key() {
        let pipe = model_key_pipe(b"test-credential").unwrap();
        // SAFETY: pipe 仍由当前测试持有；F_GETFD 仅读取其 descriptor 标志。
        assert_eq!(
            unsafe { libc::fcntl(pipe.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let model_key = read_model_key_from_fd(pipe.into_raw_fd()).unwrap();
        assert_eq!(model_key, "test-credential");
    }

    #[cfg(unix)]
    #[test]
    fn reexec_command_removes_key_and_only_passes_fd_marker() {
        let args = vec![
            OsString::from("acn_eval"),
            OsString::from("--config"),
            OsString::from("/tmp/attempt.toml"),
        ];
        let command = sanitized_reexec_command(7, &args).unwrap();
        let environment = command
            .get_envs()
            .map(|(key, value)| (key.to_os_string(), value.map(OsStr::to_os_string)))
            .collect::<Vec<_>>();

        assert!(environment
            .iter()
            .any(|(key, value)| { key == EVALUATION_MODEL_KEY_ENV && value.is_none() }));
        assert!(environment.iter().any(|(key, value)| {
            key == MODEL_KEY_FD_ENV && value.as_deref() == Some(OsStr::new("7"))
        }));
        assert_eq!(
            command
                .get_args()
                .map(OsStr::to_os_string)
                .collect::<Vec<_>>(),
            args[1..]
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_handoff_rejects_invalid_state() {
        assert!(model_key_pipe(&[]).is_err());
        assert!(model_key_pipe(&vec![b'x'; MAX_MODEL_KEY_BYTES + 1]).is_err());
        assert!(parse_model_key_fd("2").is_err());
        assert!(parse_model_key_fd("invalid").is_err());
        assert!(read_model_key_from_fd(999_999).is_err());
        let invalid_utf8 = model_key_pipe(&[0xff]).unwrap();
        assert!(read_model_key_from_fd(invalid_utf8.into_raw_fd()).is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sanitized_process_disables_core_dumping() {
        disable_process_dumping().unwrap();
        // SAFETY: PR_GET_DUMPABLE 仅读取当前测试进程的 dumpable 标志。
        assert_eq!(unsafe { libc::prctl(libc::PR_GET_DUMPABLE) }, 0);
    }
}
