//! ACN 自更新流程。
//!
//! 本模块负责远端 branch 校验、固定 Rust toolchain 构建、Cargo 安装，以及安装期间
//! 对当前配置下 supervisor 的安全预检和生命周期锁定。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::Context;
use serde::Deserialize;
use tokio::process::Command;

use crate::build_info::PACKAGE_VERSION;
use crate::claim::AgentId;
use crate::config::{validate_upstream_name, Config, AGENT_ID_PLACEHOLDER};
use crate::storage::paths;
use crate::supervisor::{self, SupervisorShutdownGuard, VerifiedSupervisorState};

pub const DEFAULT_UPDATE_BRANCH: &str = "main";
pub const DEFAULT_UPDATE_REPOSITORY_URL: &str =
    "https://github.com/FTShare-Lab/agent-claim-network.git";

/// `acn update` 已完成 CLI 解析的输入。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateOptions {
    pub url: String,
    pub branch: String,
    pub config_path: Option<PathBuf>,
    pub retry_command: String,
}

#[derive(Debug, Clone)]
struct SupervisorTarget {
    labels: Vec<String>,
    agent_home: PathBuf,
}

#[derive(Debug, Clone)]
struct SupervisorPreflight {
    target: SupervisorTarget,
    state: VerifiedSupervisorState,
}

#[derive(Debug, Deserialize)]
struct RustToolchainFile {
    toolchain: RustToolchainSpec,
}

#[derive(Debug, Deserialize)]
struct RustToolchainSpec {
    channel: String,
}

#[derive(Debug, Deserialize)]
struct CargoInstallRegistry {
    #[serde(default)]
    installs: BTreeMap<String, CargoInstallRecord>,
}

#[derive(Debug, Deserialize)]
struct CargoInstallRecord {
    #[serde(default)]
    bins: Vec<String>,
}

/// 一个已安装或远端 checkout 的可比较构建标识。
#[derive(Debug, Clone, PartialEq, Eq)]
struct BuildRevision {
    commit: String,
    committed_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateAvailability {
    Available,
    CurrentCommit,
}

/// 从已解析的 Git 仓库更新当前 Cargo 安装的 ACN 二进制。
pub async fn run_update(options: UpdateOptions) -> anyhow::Result<()> {
    ensure_supported_platform()?;
    let current_exe =
        tokio::fs::canonicalize(std::env::current_exe().context("定位当前 acn 可执行文件失败")?)
            .await
            .context("解析当前 acn 可执行文件失败")?;
    let install_root = infer_cargo_install_root(&current_exe).await?;
    let current_revision = installed_build_revision(&current_exe).await?;

    println!("Checking remote branch '{}'…", options.branch);
    let branch_heads = remote_branch_heads(&options.url, &options.branch).await?;
    let branches = branch_heads.keys().cloned().collect::<Vec<_>>();
    let remote_commit = match branch_heads.get(&options.branch) {
        Some(commit) => commit,
        None => anyhow::bail!(unknown_branch_message(&options.branch, &branches)),
    };
    if matches!(
        update_availability(&current_revision, remote_commit),
        UpdateAvailability::CurrentCommit
    ) {
        println!(
            "ACN 已是最新版本，无需更新。\nNewest: {}",
            format_build_revision(&current_revision),
        );
        return Ok(());
    }

    let temp =
        tokio::task::spawn_blocking(|| tempfile::Builder::new().prefix("acn-update-").tempdir())
            .await
            .context("等待 ACN update 临时目录创建任务失败")?
            .context("创建 ACN update 临时目录失败")?;
    let checkout = temp.path().join("checkout");
    clone_branch(&options.url, &options.branch, &checkout).await?;

    let rustup = resolve_rustup_path().await?;
    let requested_config_path = options.config_path.clone();
    let (cfg, config_path) = tokio::task::spawn_blocking(move || {
        Config::load_or_init_for_update(requested_config_path.as_deref())
    })
    .await
    .context("等待 update config 加载任务失败")?
    .context("加载 update 所需 config 失败")?;
    let targets = collect_supervisor_targets(&cfg).await?;
    let toolchain = read_toolchain_channel(&checkout).await?;
    let commit = git_commit(&checkout).await?;

    println!("Preparing Rust toolchain {toolchain}…");
    run_status(
        Command::new(&rustup)
            .arg("toolchain")
            .arg("install")
            .arg(&toolchain)
            .arg("--profile")
            .arg("minimal"),
        "准备 ACN update Rust toolchain 失败",
    )
    .await?;

    // clone/toolchain 准备可能耗时，终止 supervisor 前必须重新读取一次权威状态。
    let preflight = preflight_supervisors(&targets, &options.retry_command).await?;
    println!("Shutting down previous ACN supervisor…");
    let _supervisor_guards = shutdown_supervisors(preflight, &options.retry_command).await?;

    println!(
        "Installing ACN from branch '{}' at commit {}…",
        options.branch, commit
    );
    run_status(
        Command::new(&rustup)
            .arg("run")
            .arg(&toolchain)
            .arg("cargo")
            .arg("install")
            .arg("--locked")
            .arg("--path")
            .arg(&checkout)
            .arg("--bins")
            .arg("--force")
            .arg("--root")
            .arg(&install_root),
        "cargo install ACN update 失败",
    )
    .await?;

    let installed_version = command_stdout(
        Command::new(&current_exe).arg("--version"),
        "验证更新后的 acn 版本失败",
    )
    .await?;
    println!("ACN updated successfully: {}", installed_version.trim());
    println!("Config: {}", config_path.display());
    println!("Branch: {}", options.branch);
    println!("Commit: {commit}");
    Ok(())
}

fn ensure_supported_platform() -> anyhow::Result<()> {
    if cfg!(unix) {
        Ok(())
    } else {
        anyhow::bail!("acn update currently supports macOS and Linux only")
    }
}

async fn infer_cargo_install_root(current_exe: &Path) -> anyhow::Result<PathBuf> {
    if current_exe.file_name().and_then(|name| name.to_str()) != Some("acn") {
        anyhow::bail!(
            "当前可执行文件不是 acn，无法确定 Cargo install root: {}",
            current_exe.display()
        );
    }
    let bin_dir = current_exe
        .parent()
        .context("当前 acn 可执行文件没有父目录")?;
    if path_contains_component(current_exe, "Cellar") {
        anyhow::bail!(
            "检测到当前 ACN 由 Homebrew 管理: {}\nacn update 不会修改 Homebrew Cellar；请改用:\n  brew upgrade acn",
            current_exe.display()
        );
    }
    if bin_dir.file_name().and_then(|name| name.to_str()) != Some("bin") {
        anyhow::bail!(
            "无法安全确定当前 ACN 的 Cargo install root: {}\nacn update 目前只支持位于 <cargo-root>/bin/acn 的 Cargo 安装。",
            current_exe.display()
        );
    }
    let install_root = bin_dir
        .parent()
        .map(Path::to_path_buf)
        .context("当前 acn 的 bin 目录没有安装根目录")?;
    let registry_path = install_root.join(".crates2.json");
    let raw = tokio::fs::read_to_string(&registry_path)
        .await
        .with_context(|| {
            format!(
                "无法确认当前 ACN 是 Cargo 安装：读取 {} 失败\nacn update 只支持由 cargo install 管理的 ACN；其他安装方式请使用对应的包管理器。",
                registry_path.display()
            )
        })?;
    let registry = serde_json::from_str::<CargoInstallRegistry>(&raw).with_context(|| {
        format!(
            "无法确认当前 ACN 是 Cargo 安装：解析 {} 失败",
            registry_path.display()
        )
    })?;
    let tracked = registry.installs.iter().any(|(package, record)| {
        cargo_install_package_matches(package) && record.bins.iter().any(|bin| bin == "acn")
    });
    if !tracked {
        anyhow::bail!(
            "无法确认当前 ACN 是 Cargo 安装：{} 未记录 agent-claim-network {} 的 acn binary\nacn update 只支持由 cargo install 管理的 ACN；其他安装方式请使用对应的包管理器。",
            registry_path.display(),
            PACKAGE_VERSION
        );
    }
    Ok(install_root)
}

fn path_contains_component(path: &Path, expected: &str) -> bool {
    path.components()
        .any(|component| component.as_os_str() == expected)
}

fn cargo_install_package_matches(package: &str) -> bool {
    ["agent-claim-network", "agent_claim_network"]
        .into_iter()
        .any(|name| {
            package
                .strip_prefix(name)
                .and_then(|rest| rest.strip_prefix(' '))
                .is_some_and(|rest| {
                    rest.strip_prefix(PACKAGE_VERSION)
                        .is_some_and(|suffix| suffix.starts_with(" ("))
                })
        })
}

async fn resolve_rustup_path() -> anyhow::Result<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path_value) = std::env::var_os("PATH") {
        candidates
            .extend(std::env::split_paths(&path_value).map(|directory| directory.join("rustup")));
    }
    if let Some(cargo_home) = std::env::var_os("CARGO_HOME") {
        candidates.push(PathBuf::from(cargo_home).join("bin").join("rustup"));
    }
    if let Some(user_home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(user_home)
                .join(".cargo")
                .join("bin")
                .join("rustup"),
        );
    }

    let mut seen = BTreeSet::new();
    for candidate in candidates {
        if seen.insert(candidate.clone()) && is_executable_file(&candidate).await {
            return Ok(candidate);
        }
    }
    anyhow::bail!(
        "ACN update requires rustup, but rustup was not found.\nChecked PATH, $CARGO_HOME/bin/rustup and ~/.cargo/bin/rustup.\nInstall rustup and retry."
    )
}

#[cfg(unix)]
async fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
async fn is_executable_file(_path: &Path) -> bool {
    false
}

async fn remote_branch_heads(
    repository_url: &str,
    requested_branch: &str,
) -> anyhow::Result<BTreeMap<String, String>> {
    let output = Command::new("git")
        .arg("ls-remote")
        .arg("--heads")
        .arg("--")
        .arg(repository_url)
        .output()
        .await
        .map_err(|error| {
            git_access_error(repository_url, requested_branch, None, &error.to_string())
        })?;
    if !output.status.success() {
        return Err(git_access_error(
            repository_url,
            requested_branch,
            Some(&output.stderr),
            &format!("git ls-remote exited with {}", output.status),
        ));
    }
    let stdout = String::from_utf8(output.stdout).context("git ls-remote 输出不是 UTF-8")?;
    Ok(parse_remote_branch_heads(&stdout))
}

fn parse_remote_branch_heads(stdout: &str) -> BTreeMap<String, String> {
    stdout
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .filter_map(|(commit, reference)| {
            reference
                .strip_prefix("refs/heads/")
                .map(|branch| (branch.to_owned(), commit.to_owned()))
        })
        .collect()
}

async fn clone_branch(repository_url: &str, branch: &str, checkout: &Path) -> anyhow::Result<()> {
    let output = Command::new("git")
        .arg("clone")
        .arg("--depth")
        .arg("1")
        .arg("--single-branch")
        .arg("--branch")
        .arg(branch)
        .arg("--")
        .arg(repository_url)
        .arg(checkout)
        .output()
        .await
        .map_err(|error| git_access_error(repository_url, branch, None, &error.to_string()))?;
    if output.status.success() {
        return Ok(());
    }
    Err(git_access_error(
        repository_url,
        branch,
        Some(&output.stderr),
        &format!("git clone exited with {}", output.status),
    ))
}

fn git_access_error(
    repository_url: &str,
    branch: &str,
    stderr: Option<&[u8]>,
    summary: &str,
) -> anyhow::Error {
    let stderr = stderr
        .map(String::from_utf8_lossy)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| summary.to_owned());
    anyhow::anyhow!(
        "无法访问 ACN 更新仓库。\nGit error:\n{stderr}\n\n请先在合适的临时目录手动确认以下命令可以成功执行：\n  {}",
        manual_clone_command(repository_url, branch)
    )
}

fn manual_clone_command(repository_url: &str, branch: &str) -> String {
    format!(
        "git clone --branch {} --single-branch -- {}",
        shell_quote(branch),
        shell_quote(repository_url)
    )
}

fn unknown_branch_message(branch: &str, branches: &[String]) -> String {
    let available = if branches.is_empty() {
        "  (no remote branches found)".to_string()
    } else {
        branches
            .iter()
            .map(|value| format!("  {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    format!("远端不存在 branch '{branch}'。\nAvailable branches:\n{available}")
}

async fn read_toolchain_channel(checkout: &Path) -> anyhow::Result<String> {
    let path = checkout.join("rust-toolchain.toml");
    let raw = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("读取更新仓库 toolchain 失败: {}", path.display()))?;
    let parsed: RustToolchainFile = toml::from_str(&raw)
        .with_context(|| format!("解析更新仓库 toolchain 失败: {}", path.display()))?;
    let channel = parsed.toolchain.channel.trim();
    if channel.is_empty() {
        anyhow::bail!("更新仓库 rust-toolchain.toml 的 channel 为空");
    }
    Ok(channel.to_owned())
}

async fn git_commit(checkout: &Path) -> anyhow::Result<String> {
    command_stdout(
        Command::new("git")
            .arg("-C")
            .arg(checkout)
            .arg("rev-parse")
            .arg("--short=7")
            .arg("HEAD"),
        "读取更新仓库 commit 失败",
    )
    .await
    .map(|value| value.trim().to_owned())
}

async fn installed_build_revision(current_exe: &Path) -> anyhow::Result<BuildRevision> {
    let version = command_stdout(
        Command::new(current_exe).arg("--version"),
        "读取当前 acn 版本失败",
    )
    .await?;
    parse_installed_build_revision(version.trim()).context("解析当前 acn 构建元数据失败")
}

fn parse_installed_build_revision(version: &str) -> anyhow::Result<BuildRevision> {
    let (_, metadata) = version
        .trim()
        .rsplit_once('(')
        .context("缺少 '(commit, timestamp)' 构建元数据")?;
    let metadata = metadata
        .strip_suffix(')')
        .context("构建元数据缺少结尾 ')' ")?;
    let (commit, timestamp) = metadata
        .split_once(',')
        .context("构建元数据缺少 commit 与 timestamp 分隔符")?;
    build_revision_from_parts(commit.trim(), timestamp.trim())
}

fn build_revision_from_parts(commit: &str, timestamp: &str) -> anyhow::Result<BuildRevision> {
    if commit.is_empty() || commit == "unknown" {
        anyhow::bail!("commit 不可用: {commit}");
    }
    if timestamp.is_empty() {
        anyhow::bail!("提交时间为空");
    }
    Ok(BuildRevision {
        commit: commit.to_owned(),
        committed_at: timestamp.to_owned(),
    })
}

fn update_availability(current: &BuildRevision, remote_commit: &str) -> UpdateAvailability {
    if same_commit(&current.commit, remote_commit) {
        UpdateAvailability::CurrentCommit
    } else {
        UpdateAvailability::Available
    }
}

fn same_commit(left: &str, right: &str) -> bool {
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn format_build_revision(revision: &BuildRevision) -> String {
    format!("{} ({})", revision.commit, revision.committed_at)
}

async fn collect_supervisor_targets(cfg: &Config) -> anyhow::Result<Vec<SupervisorTarget>> {
    let mut homes = BTreeMap::<PathBuf, BTreeSet<String>>::new();
    for (name, upstream) in &cfg.upstreams {
        validate_upstream_name(name)?;
        let runtime_root = cfg.storage.upstream_runtime_root(name);
        let raw_agent_id = upstream.agent_id.trim();
        if !raw_agent_id.is_empty() && raw_agent_id != AGENT_ID_PLACEHOLDER {
            let agent_id = AgentId::new(raw_agent_id.to_owned())
                .with_context(|| format!("upstreams.{name}.agent_id 不是合法 agent id"))?;
            homes
                .entry(paths::runtime_agent_home(&runtime_root, &agent_id))
                .or_default()
                .insert(name.clone());
        }

        let agents_root = paths::runtime_agents_root(&runtime_root);
        let mut entries = match tokio::fs::read_dir(&agents_root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "扫描 upstream '{name}' 的 agent 目录失败: {}",
                        agents_root.display()
                    )
                });
            }
        };
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let agent_home = entry.path();
            if tokio::fs::try_exists(paths::agent_home_supervisor_dir(&agent_home)).await? {
                homes.entry(agent_home).or_default().insert(name.clone());
            }
        }
    }

    Ok(homes
        .into_iter()
        .map(|(agent_home, labels)| SupervisorTarget {
            labels: labels.into_iter().collect(),
            agent_home,
        })
        .collect())
}

async fn preflight_supervisors(
    targets: &[SupervisorTarget],
    retry_command: &str,
) -> anyhow::Result<Vec<SupervisorPreflight>> {
    let mut result = Vec::with_capacity(targets.len());
    for target in targets {
        let state = supervisor::preflight_supervisor_shutdown(&target.agent_home)
            .await
            .map_err(|error| unsafe_supervisor_message(target, &error, retry_command))?;
        result.push(SupervisorPreflight {
            target: target.clone(),
            state,
        });
    }
    Ok(result)
}

async fn shutdown_supervisors(
    preflight: Vec<SupervisorPreflight>,
    retry_command: &str,
) -> anyhow::Result<Vec<SupervisorShutdownGuard>> {
    let mut guards = Vec::with_capacity(preflight.len());
    for checked in preflight {
        let guard =
            supervisor::shutdown_verified_supervisor(&checked.target.agent_home, checked.state)
                .await
                .map_err(|error| {
                    shutdown_failure_message(&checked.target, &error, retry_command)
                })?;
        guards.push(guard);
    }
    Ok(guards)
}

fn unsafe_supervisor_message(
    target: &SupervisorTarget,
    error: &anyhow::Error,
    retry_command: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "Cannot update ACN safely: a running supervisor could not be verified.\n\nUpstream: {}\nAgent home: {}\nReason: {error:#}\n\nNo supervisor was stopped and ACN was not updated.\nResolve the supervisor state, then retry:\n  {retry_command}",
        target.labels.join(", "),
        target.agent_home.display()
    )
}

fn shutdown_failure_message(
    target: &SupervisorTarget,
    error: &anyhow::Error,
    retry_command: &str,
) -> anyhow::Error {
    anyhow::anyhow!(
        "ACN update stopped before installation because a supervisor could not be shut down.\n\nUpstream: {}\nAgent home: {}\nReason: {error:#}\n\nAny already stopped supervisor will recover its persisted jobs on the next ACN start.\nRetry with:\n  {retry_command}",
        target.labels.join(", "),
        target.agent_home.display()
    )
}

async fn run_status(command: &mut Command, context: &'static str) -> anyhow::Result<()> {
    command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let status = command.status().await.context(context)?;
    if !status.success() {
        anyhow::bail!("{context}: process exited with {status}");
    }
    Ok(())
}

async fn command_stdout(command: &mut Command, context: &'static str) -> anyhow::Result<String> {
    let output = command.output().await.context(context)?;
    if !output.status.success() {
        anyhow::bail!(
            "{context}: process exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).context(context)
}

/// 把参数格式化为可安全复制执行的 shell token，用于动态 retry/clone 提示。
pub fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '@' | '+')
        })
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

/// 从已经解析成字符串的 argv 重建本轮 update 命令，供错误信息直接复用。
pub fn retry_command(args: &[String]) -> String {
    args.iter()
        .map(|value| shell_quote(value))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_REPOSITORY_URL: &str = "git@example.com:team/agent-claim-network.git";

    #[tokio::test]
    async fn cargo_install_root_requires_registry_record_for_current_package() {
        let dir = tempfile::tempdir().unwrap();
        let bin_dir = dir.path().join("bin");
        tokio::fs::create_dir_all(&bin_dir).await.unwrap();
        let executable = bin_dir.join("acn");
        let registry = serde_json::json!({
            "installs": {
                format!(
                    "agent-claim-network {} (git+https://example.com/acn)",
                    env!("CARGO_PKG_VERSION")
                ): {
                    "bins": ["acn", "acn-router", "acn-maintainer"]
                }
            }
        });
        tokio::fs::write(
            dir.path().join(".crates2.json"),
            serde_json::to_vec(&registry).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            infer_cargo_install_root(&executable).await.unwrap(),
            dir.path()
        );
        let error = infer_cargo_install_root(Path::new("/workspace/target/debug/acn"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("<cargo-root>/bin/acn"));
    }

    #[tokio::test]
    async fn cargo_install_root_accepts_legacy_underscore_package_record() {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("bin/acn");
        tokio::fs::create_dir_all(executable.parent().unwrap())
            .await
            .unwrap();
        let registry = serde_json::json!({
            "installs": {
                format!(
                    "agent_claim_network {} (path+file:///workspace)",
                    env!("CARGO_PKG_VERSION")
                ): {
                    "bins": ["acn"]
                }
            }
        });
        tokio::fs::write(
            dir.path().join(".crates2.json"),
            serde_json::to_vec(&registry).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            infer_cargo_install_root(&executable).await.unwrap(),
            dir.path()
        );
    }

    #[tokio::test]
    async fn cargo_install_root_rejects_untracked_bin_directory() {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("bin/acn");
        tokio::fs::create_dir_all(executable.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(dir.path().join(".crates2.json"), br#"{"installs":{}}"#)
            .await
            .unwrap();

        let error = infer_cargo_install_root(&executable)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("未记录 agent-claim-network"));
    }

    #[tokio::test]
    async fn cargo_install_root_routes_homebrew_users_to_brew_upgrade() {
        let error = infer_cargo_install_root(Path::new("/opt/homebrew/Cellar/acn/0.2.0/bin/acn"))
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("Homebrew"));
        assert!(error.contains("brew upgrade acn"));
    }

    #[test]
    fn remote_branch_parser_preserves_hashes_slashes_and_sorts() {
        let raw = "bbbbbbbb\trefs/heads/main\naaaaaaaa\trefs/heads/feature/file-diff-display\n";
        let branches = parse_remote_branch_heads(raw);
        assert_eq!(
            branches.keys().cloned().collect::<Vec<_>>(),
            ["feature/file-diff-display".to_string(), "main".to_string()]
        );
        assert_eq!(branches["main"], "bbbbbbbb");
    }

    #[test]
    fn retry_command_uses_actual_invocation_and_quotes_unsafe_values() {
        let command = retry_command(&[
            "acn".to_string(),
            "update".to_string(),
            "--url".to_string(),
            TEST_REPOSITORY_URL.to_string(),
            "--branch".to_string(),
            "feat/user's work".to_string(),
        ]);
        assert_eq!(
            command,
            "acn update --url git@example.com:team/agent-claim-network.git --branch 'feat/user'\"'\"'s work'"
        );
    }

    #[test]
    fn manual_clone_command_uses_requested_branch() {
        assert_eq!(
            manual_clone_command(TEST_REPOSITORY_URL, "feat/whatever_user"),
            "git clone --branch feat/whatever_user --single-branch -- \
git@example.com:team/agent-claim-network.git"
        );
    }

    #[test]
    fn git_access_error_keeps_stderr_and_manual_clone_hint() {
        let error = git_access_error(
            TEST_REPOSITORY_URL,
            "feature/sample",
            Some(b"Permission denied (publickey).\n"),
            "git failed",
        )
        .to_string();

        assert!(error.contains("Permission denied (publickey)."));
        assert!(error.contains(
            "git clone --branch feature/sample --single-branch -- \
git@example.com:team/agent-claim-network.git"
        ));
    }

    #[test]
    fn unknown_branch_lists_remote_heads() {
        let message = unknown_branch_message(
            "missing",
            &["feature/sample".to_string(), "main".to_string()],
        );
        assert!(message.contains("远端不存在 branch 'missing'"));
        assert!(message.contains("  feature/sample\n  main"));
    }

    fn revision(commit: &str, timestamp: &str) -> BuildRevision {
        build_revision_from_parts(commit, timestamp).unwrap()
    }

    #[test]
    fn installed_version_parser_reads_embedded_build_metadata() {
        let version = format!(
            "acn {} (123abcd, 2025-01-02 03:04:05)",
            env!("CARGO_PKG_VERSION")
        );
        let revision = parse_installed_build_revision(&version).unwrap();

        assert_eq!(revision.commit, "123abcd");
        assert_eq!(
            format_build_revision(&revision),
            "123abcd (2025-01-02 03:04:05)"
        );
    }

    #[test]
    fn update_check_treats_short_and_long_forms_of_same_commit_as_current() {
        let current = revision("123abcd", "2025-01-02 03:04:05");

        assert_eq!(
            update_availability(&current, "123abcdef00"),
            UpdateAvailability::CurrentCommit
        );
    }

    #[test]
    fn update_check_installs_for_any_different_remote_commit() {
        let current = revision("aaaaaaa", "2025-01-02 03:04:05");

        assert_eq!(
            update_availability(&current, "bbbbbbb"),
            UpdateAvailability::Available
        );
        assert_eq!(
            update_availability(&current, "ccccccc"),
            UpdateAvailability::Available
        );
    }
}
