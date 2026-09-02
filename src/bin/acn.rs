//! agent CLI 入口：进入交互式 TUI session。

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_claim_network::bootstrap;
use agent_claim_network::build_info;
use agent_claim_network::claim::{AgentId, SessionId};
use agent_claim_network::config::{
    resolve_workspace_root, Config, ResolvedUpstream, DEFAULT_SESSION_CLEANUP_RETENTION_DAYS,
};
use agent_claim_network::mcp::config::{
    lock_mcp_json_config, read_mcp_json_config, validate_server_name, write_mcp_json_config_atomic,
    McpJsonConfig, McpOAuthCredentialsStore, McpServerConfig, McpTransportKind,
};
use agent_claim_network::mcp::connection_manager::{
    McpConnectionManager, McpRuntimeState, McpServerStatus,
};
use agent_claim_network::mcp::oauth;
use agent_claim_network::session::{
    cleanup_old_sessions, SessionCleanupConfig, SessionCleanupEntry, SessionCleanupOutcome,
    SessionCleanupReport, SessionMetadata, SessionStatus,
};
use agent_claim_network::session_tui::{self, StartupResume};
use agent_claim_network::storage::{paths, read_yaml, FileLockGuard};
use agent_claim_network::supervisor::{self, SupervisorLaunchConfig, SupervisorRetryTarget};
use agent_claim_network::update::{
    self, UpdateOptions, DEFAULT_UPDATE_BRANCH, DEFAULT_UPDATE_REPOSITORY_URL,
};
use anyhow::Context;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use unicode_width::UnicodeWidthStr;

const DEFAULT_SUPERVISOR_JOBS_LIMIT: usize = 5;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let raw_args = std::env::args().collect::<Vec<_>>();
    if build_info::version_requested(&raw_args) {
        println!("{}", build_info::version_text("acn"));
        return Ok(());
    }
    if raw_args.get(1).is_some_and(|arg| arg == "supervisor") {
        return run_supervisor_cli(raw_args).await;
    }
    if raw_args.get(1).is_some_and(|arg| arg == "mcp") {
        return run_mcp_cli(raw_args).await;
    }
    if raw_args.get(1).is_some_and(|arg| arg == "session") {
        return run_session_cli(raw_args).await;
    }
    if raw_args.get(1).is_some_and(|arg| arg == "update") {
        return run_update_cli(raw_args).await;
    }

    let cli = parse_cli_from(raw_args)?;
    let (mut cfg, cfg_path) = Config::load_or_init_for_agent(cli.config.as_deref())
        .with_context(|| format!("加载 config: {:?}", cli.config))?;
    let workspace_root = resolve_workspace_root(cli.cd.as_deref())?;
    cfg.set_tool_workspace_root(workspace_root.clone());
    let upstream = cfg
        .resolve_upstream(cli.upstream.as_deref())
        .context("解析 upstream 失败")?;
    activate_acn_upstream_runtime(&mut cfg, &upstream, "激活 upstream 本地目录失败")?;
    let runtime_fingerprint = supervisor::runtime_fingerprint(&cfg, &upstream)?;
    let supervisor_launch = SupervisorLaunchConfig::new(
        cfg.agent_home(&upstream.agent_id),
        cfg_path,
        Some(upstream.name.clone()),
        cfg.agent.session.notify_on_finalize_completion,
        runtime_fingerprint,
    );
    let cleanup_housekeeping = session_tui::SessionCleanupHousekeepingConfig {
        agent_id: upstream.agent_id.clone(),
        agent_home: cfg.agent_home(&upstream.agent_id),
        retention_days: cfg.agent.session.cleanup_retention_days,
        sqlite_busy_timeout: std::time::Duration::from_millis(
            cfg.agent.tool.session_search_sqlite_busy_timeout_ms,
        ),
        timing: session_tui::SessionCleanupHousekeepingTiming::default(),
    };
    // 先恢复可能中断的 recap/finalize job，再判断目标 session 是否可 resume。否则
    // supervisor 崩溃留下的 queued/running job 会没有进程继续执行。
    if let Err(error) = supervisor::ensure_supervisor_running(&supervisor_launch).await {
        log::warn!(
            target: "acn",
            "recap/finalize supervisor 启动或接管失败，本次会话继续运行: {error:#}"
        );
    }
    if let Some(message) =
        direct_resume_preflight_failure(&cfg, &upstream.agent_id, &cli.resume).await
    {
        eprint!("{message}");
        return Ok(());
    }
    let mcp_manager = bootstrap::build_refreshed_mcp_manager(&cfg).await;
    let engine = match bootstrap::build_agent_cli_session_engine_with_mcp(
        &cfg,
        &upstream,
        Some(Arc::clone(&mcp_manager)),
    ) {
        Ok(engine) => engine,
        Err(error) => {
            mcp_manager.shutdown().await;
            return Err(error);
        }
    };
    let tui_result = session_tui::run_session_tui_with_resume_and_cleanup(
        engine,
        cfg.agent.session.id_mint_max_attempts(),
        cli.resume,
        cfg.agent.session.tui.clone(),
        Some(supervisor_launch),
        Some(cleanup_housekeeping),
    )
    .await;
    mcp_manager.shutdown().await;
    tui_result?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UpdateCli {
    url: String,
    branch: String,
    config: Option<PathBuf>,
    retry_command: String,
}

async fn run_update_cli(args: Vec<String>) -> anyhow::Result<()> {
    let cli = parse_update_cli_from(args)?;
    update::run_update(UpdateOptions {
        url: cli.url,
        branch: cli.branch,
        config_path: cli.config,
        retry_command: cli.retry_command,
    })
    .await
}

fn parse_update_cli_from<I, S>(args: I) -> anyhow::Result<UpdateCli>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    if args.get(1).map(String::as_str) != Some("update") {
        anyhow::bail!("{}", update_usage());
    }
    let retry_command = update::retry_command(&args);
    let mut url = None;
    let mut branch = None;
    let mut config = None;
    let mut index = 2;
    while index < args.len() {
        match args[index].as_str() {
            "--url" => {
                if url.is_some() {
                    anyhow::bail!("--url 不能重复指定");
                }
                index += 1;
                url = Some(
                    args.get(index)
                        .context("--url 后缺少 Git 仓库地址")?
                        .clone(),
                );
                index += 1;
            }
            "--branch" => {
                if branch.is_some() {
                    anyhow::bail!("--branch 不能重复指定");
                }
                index += 1;
                branch = Some(
                    args.get(index)
                        .context("--branch 后缺少远端 branch 名称")?
                        .clone(),
                );
                index += 1;
            }
            "--config" => {
                if config.is_some() {
                    anyhow::bail!("--config 不能重复指定");
                }
                index += 1;
                config = Some(PathBuf::from(
                    args.get(index).context("--config 后缺少路径")?,
                ));
                index += 1;
            }
            "-h" | "--help" => {
                eprintln!("{}", update_usage());
                std::process::exit(0);
            }
            other => anyhow::bail!("未知 update 参数: {other}\n{}", update_usage()),
        }
    }
    let url = url.unwrap_or_else(|| DEFAULT_UPDATE_REPOSITORY_URL.to_string());
    if url.trim().is_empty() {
        anyhow::bail!("--url 不能为空");
    }
    let branch = branch.unwrap_or_else(|| DEFAULT_UPDATE_BRANCH.to_string());
    if branch.trim().is_empty() {
        anyhow::bail!("--branch 不能为空");
    }
    Ok(UpdateCli {
        url,
        branch,
        config,
        retry_command,
    })
}

fn update_usage() -> &'static str {
    "用法:
  acn update [--url <git-url>] [--branch <branch>] [--config <path>]

默认从 https://github.com/FTShare-Lab/agent-claim-network.git 更新当前 Cargo 安装的三个 ACN 可执行文件。
可用 --url 临时指定其他可信的 ACN Git 仓库。branch 必须是远端现存分支；默认 main。
更新使用仓库 rust-toolchain.toml 指定的 Rust toolchain，并在安装前停止当前配置下的旧 supervisor。
Homebrew 安装不由此命令修改，请使用 brew upgrade acn；新版 ACN 首次运行时会接管旧 supervisor。

选项:
  --url <git-url>    指定其他可信的 ACN Git 仓库；默认使用上述仓库
  --branch <branch>  更新指定远端 branch；默认 main
  --config <path>    指定 config.toml；不传则按 ACN_CONFIG 和 ~/.acn/config.toml 查找
  -h, --help         显示帮助
"
}

async fn run_session_cli(args: Vec<String>) -> anyhow::Result<()> {
    let cli = parse_session_cli_from(args)?;
    let (mut cfg, _cfg_path) = Config::load_or_init_for_supervisor_control(cli.config.as_deref())
        .with_context(|| format!("加载 session config: {:?}", cli.config))?;
    let upstream = cfg
        .resolve_upstream(cli.upstream.as_deref())
        .context("解析 session upstream 失败")?;
    activate_acn_upstream_runtime(&mut cfg, &upstream, "激活 session upstream 本地目录失败")?;
    let agent_home = cfg.agent_home(&upstream.agent_id);
    match cli.command {
        SessionCommand::Cleanup { apply } => {
            let retention_days = manual_session_cleanup_retention_days(&cfg);
            let cutoff = Utc::now() - ChronoDuration::days(i64::from(retention_days));
            let _guard = FileLockGuard::lock_exclusive(
                paths::agent_home_session_cleanup_lock_path(&agent_home),
            )
            .await?;
            let report = cleanup_old_sessions(SessionCleanupConfig {
                agent_id: upstream.agent_id.clone(),
                agent_home: agent_home.clone(),
                cutoff,
                apply,
                sqlite_busy_timeout: std::time::Duration::from_millis(
                    cfg.agent.tool.session_search_sqlite_busy_timeout_ms,
                ),
                abort_check: None,
            })
            .await?;
            print!(
                "{}",
                session_cleanup_text(
                    &report,
                    upstream.agent_id.as_str(),
                    &agent_home,
                    retention_days,
                    cutoff,
                    apply,
                )
            );
        }
    }
    Ok(())
}

fn activate_acn_upstream_runtime(
    cfg: &mut Config,
    upstream: &ResolvedUpstream,
    context: &'static str,
) -> anyhow::Result<()> {
    let report = agent_claim_network::upstream_migration::migrate_legacy_runtime_if_needed(
        cfg.storage.base_acn_home(),
        &upstream.name,
        &upstream.agent_id,
    )
    .context("迁移旧 ACN upstream 本地目录失败")?;
    if report.moved > 0 {
        log::info!(
            target: "acn",
            "已自动迁移旧 ACN 本地状态到 upstream '{}' (moved={}, skipped_existing_target={})",
            upstream.name,
            report.moved,
            report.skipped_existing_target
        );
    }
    cfg.activate_upstream_runtime(upstream).context(context)
}

fn manual_session_cleanup_retention_days(cfg: &Config) -> u32 {
    if cfg.agent.session.cleanup_retention_days == 0 {
        DEFAULT_SESSION_CLEANUP_RETENTION_DAYS
    } else {
        cfg.agent.session.cleanup_retention_days
    }
}

#[derive(Debug)]
struct SessionCli {
    command: SessionCommand,
    config: Option<PathBuf>,
    upstream: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionCommand {
    Cleanup { apply: bool },
}

fn parse_session_cli_from<I, S>(args: I) -> anyhow::Result<SessionCli>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    if args.get(1).map(String::as_str) != Some("session") {
        anyhow::bail!("{}", session_usage());
    }
    let command_name = match args.get(2).map(String::as_str) {
        Some("-h" | "--help") => {
            eprintln!("{}", session_usage());
            std::process::exit(0);
        }
        Some(value) => value,
        None => anyhow::bail!("{}", session_usage()),
    };
    let mut config = None;
    let mut upstream = None;
    let command = match command_name {
        "cleanup" => {
            let mut apply = false;
            let mut index = 3;
            while index < args.len() {
                match args[index].as_str() {
                    "--apply" => {
                        apply = true;
                        index += 1;
                    }
                    "--config" => {
                        index += 1;
                        config = Some(PathBuf::from(
                            args.get(index).context("--config 后缺少路径")?,
                        ));
                        index += 1;
                    }
                    "--upstream" => {
                        index += 1;
                        upstream = Some(args.get(index).context("--upstream 后缺少名称")?.clone());
                        index += 1;
                    }
                    "-h" | "--help" => {
                        eprintln!("{}", session_usage());
                        std::process::exit(0);
                    }
                    other => anyhow::bail!("未知 session cleanup 参数: {other}"),
                }
            }
            SessionCommand::Cleanup { apply }
        }
        other => anyhow::bail!("未知 session 子命令: {other}\n{}", session_usage()),
    };
    Ok(SessionCli {
        command,
        config,
        upstream,
    })
}

fn session_usage() -> &'static str {
    "用法:
  acn session cleanup [--apply] [options]

清理当前 agent 的旧 Closed sessions。默认只预览，不删除；加 --apply 后才执行删除。
保留期来自 [agent.session].cleanup_retention_days；配置为 0 时手动命令按默认 30 天判断。

选项:
  --apply             执行删除；不加时仅 dry-run
  --config <path>     指定 config.toml；应与启动 TUI 时使用的配置一致
  --upstream <name>   选择 [upstreams.<name>]；应与要清理的 agent upstream 一致
  -h, --help          显示帮助
"
}

#[derive(Debug)]
struct Cli {
    config: Option<PathBuf>,
    upstream: Option<String>,
    resume: StartupResume,
    cd: Option<PathBuf>,
}

async fn direct_resume_preflight_failure(
    cfg: &Config,
    agent_id: &AgentId,
    resume: &StartupResume,
) -> Option<String> {
    let StartupResume::Session(session_id) = resume else {
        return None;
    };
    let session_yaml =
        paths::agent_home_session_dir(&cfg.agent_home(agent_id), session_id).join("session.yaml");
    let metadata: SessionMetadata = match read_yaml(&session_yaml).await {
        Ok(metadata) => metadata,
        Err(error) => return Some(format!("Resume failed: {error:#}\n")),
    };
    direct_resume_metadata_failure(agent_id, session_id, &metadata)
}

fn direct_resume_metadata_failure(
    expected_agent_id: &AgentId,
    session_id: &SessionId,
    metadata: &SessionMetadata,
) -> Option<String> {
    if metadata.agent_id != *expected_agent_id {
        return Some(format!(
            "Resume failed: Session {session_id} belongs to agent {}, not {}.\n",
            metadata.agent_id, expected_agent_id
        ));
    }
    if metadata.status == SessionStatus::Open
        && (metadata.closed_at.is_some() || metadata.finalized_at.is_some())
    {
        return Some(format!(
            "Resume failed: Session {session_id} has inconsistent Open metadata.\n"
        ));
    }
    None
}

async fn run_mcp_cli(args: Vec<String>) -> anyhow::Result<()> {
    let cli = parse_mcp_cli_from(args)?;
    let (mut cfg, _cfg_path) = Config::load_or_init_for_supervisor_control(cli.config.as_deref())
        .with_context(|| format!("加载 config: {:?}", cli.config))?;
    let upstream = cfg
        .resolve_upstream(cli.upstream.as_deref())
        .context("解析 mcp upstream 失败")?;
    activate_acn_upstream_runtime(&mut cfg, &upstream, "激活 mcp upstream 本地目录失败")?;
    let config_path = cfg.storage.mcp_config_path();
    let output = execute_mcp_command(&config_path, cli.command).await?;
    print!("{output}");
    Ok(())
}

#[derive(Debug)]
struct McpCli {
    command: McpCommand,
    config: Option<PathBuf>,
    upstream: Option<String>,
}

#[derive(Debug)]
enum McpCommand {
    List,
    Get {
        name: String,
        json: bool,
    },
    Add(McpAddCommand),
    AddJson {
        name: String,
        server: McpServerConfig,
    },
    Remove {
        name: String,
    },
    Enable {
        name: String,
    },
    Disable {
        name: String,
    },
    Login {
        name: String,
        no_browser: bool,
    },
    Logout {
        name: String,
    },
    Status {
        name: Option<String>,
    },
}

#[derive(Debug)]
struct McpAddCommand {
    name: String,
    transport: McpAddTransport,
    env: BTreeMap<String, String>,
    env_vars: Vec<String>,
}

#[derive(Debug)]
enum McpAddTransport {
    Stdio {
        command: String,
        args: Vec<String>,
    },
    StreamableHttp {
        url: String,
        bearer_token_env_var: Option<String>,
        oauth_client_id: Option<String>,
        oauth_callback_port: Option<u16>,
        oauth_credentials_store: Option<McpOAuthCredentialsStore>,
    },
}

async fn execute_mcp_command(path: &Path, command: McpCommand) -> anyhow::Result<String> {
    match command {
        McpCommand::List => {
            let cfg = read_mcp_json_config(path).await?;
            Ok(mcp_list_text(path, &cfg))
        }
        McpCommand::Get { name, json } => {
            let cfg = read_mcp_json_config(path).await?;
            let server = cfg
                .servers
                .get(&name)
                .with_context(|| format!("MCP server 不存在: {name}"))?;
            if json {
                let mut text =
                    serde_json::to_string_pretty(&redacted_mcp_server_config_json(server)?)?;
                text.push('\n');
                Ok(text)
            } else {
                Ok(mcp_server_text(path, &name, server))
            }
        }
        McpCommand::Add(add) => {
            let server = match add.transport {
                McpAddTransport::Stdio { command, args } => {
                    McpServerConfig::stdio(command, args, add.env, add.env_vars)
                }
                McpAddTransport::StreamableHttp {
                    url,
                    bearer_token_env_var,
                    oauth_client_id,
                    oauth_callback_port,
                    oauth_credentials_store,
                } => {
                    if !add.env.is_empty() || !add.env_vars.is_empty() {
                        anyhow::bail!(
                            "streamable_http server 不支持 -e/--env-var；请使用 --bearer-token-env-var"
                        );
                    }
                    let mut server = McpServerConfig::streamable_http(url, bearer_token_env_var);
                    server.oauth_client_id = oauth_client_id;
                    server.oauth_callback_port = oauth_callback_port;
                    server.oauth_credentials_store = oauth_credentials_store;
                    server
                }
            };
            add_mcp_server(path, add.name, server).await
        }
        McpCommand::AddJson { name, server } => add_mcp_server(path, name, server).await,
        McpCommand::Remove { name } => {
            let cfg = read_mcp_json_config(path).await?;
            let server = cfg
                .servers
                .get(&name)
                .with_context(|| format!("MCP server 不存在: {name}"))?
                .clone();
            let clear_oauth = server.transport_kind(&name)? == McpTransportKind::StreamableHttp;
            let mut credential_lease = if clear_oauth {
                Some(oauth::prepare_credentials_for_remove(path, &name, &server).await?)
            } else {
                None
            };
            let remove_result = async {
                let _config_guard = lock_mcp_json_config(path).await?;
                let mut current = read_mcp_json_config(path).await?;
                if current.servers.get(&name) != Some(&server) {
                    anyhow::bail!(
                        "MCP server '{name}' 的配置在 remove 等待期间已删除或变更；请重试"
                    );
                }
                current.servers.remove(&name);
                write_mcp_json_config_atomic(path, &current).await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(error) = remove_result {
                if let Some(credential_lease) = credential_lease.take() {
                    if let Err(cleanup_error) = credential_lease.cancel().await {
                        log::warn!(
                            "MCP server '{name}' remove 未落盘，回滚 OAuth cleanup record 失败: {cleanup_error}"
                        );
                    }
                }
                return Err(error);
            }
            let mut output = format!("removed MCP server '{name}'\n");
            if let Some(credential_lease) = credential_lease {
                if let Err(error) = credential_lease.finish().await {
                    log::warn!(
                        "MCP server '{name}' 配置已删除，但本地 OAuth 凭据清理失败: {error}"
                    );
                    output.push_str(&format!(
                        "warning: MCP server '{name}' 配置已删除，但本地 OAuth 凭据清理失败；可在凭据存储恢复后重试 `acn mcp logout {name}`：{error}\n"
                    ));
                }
            }
            Ok(output)
        }
        McpCommand::Enable { name } => {
            let _config_guard = lock_mcp_json_config(path).await?;
            let mut cfg = read_mcp_json_config(path).await?;
            let server = cfg
                .servers
                .get_mut(&name)
                .with_context(|| format!("MCP server 不存在: {name}"))?;
            server.enabled = Some(true);
            write_mcp_json_config_atomic(path, &cfg).await?;
            Ok(format!("enabled MCP server '{name}'\n"))
        }
        McpCommand::Disable { name } => {
            let _config_guard = lock_mcp_json_config(path).await?;
            let mut cfg = read_mcp_json_config(path).await?;
            let server = cfg
                .servers
                .get_mut(&name)
                .with_context(|| format!("MCP server 不存在: {name}"))?;
            server.enabled = Some(false);
            write_mcp_json_config_atomic(path, &cfg).await?;
            Ok(format!("disabled MCP server '{name}'\n"))
        }
        McpCommand::Login { name, no_browser } => {
            let cfg = read_mcp_json_config(path).await?;
            let server = cfg
                .servers
                .get(&name)
                .with_context(|| format!("MCP server 不存在: {name}"))?;
            if server.bearer_token_env_var.is_some() {
                anyhow::bail!(
                    "MCP server '{name}' 使用 bearer_token_env_var，不能执行 OAuth login；请先移除 bearer 配置"
                );
            }
            oauth::login(path, &name, server, no_browser).await?;
            Ok(format!("logged in to MCP server '{name}'\n"))
        }
        McpCommand::Logout { name } => {
            let cfg = read_mcp_json_config(path).await?;
            let retried_pending_cleanup = oauth::retry_pending_logout(path, &name).await?;
            if let Some(server) = cfg.servers.get(&name) {
                oauth::logout(path, &name, server).await?;
            } else if !retried_pending_cleanup {
                anyhow::bail!("MCP server 不存在: {name}");
            }
            Ok(format!("logged out of MCP server '{name}'\n"))
        }
        McpCommand::Status { name } => {
            let workspace_root = std::env::current_dir().context("读取当前工作目录失败")?;
            let manager = McpConnectionManager::new(path.to_path_buf(), workspace_root, None);
            let status_result = async {
                if let Some(name) = &name {
                    manager.reconnect_server(name).await?;
                } else {
                    manager.refresh_all().await?;
                }
                let snapshot = manager.snapshot().await;
                mcp_status_text(path, &snapshot, name.as_deref())
            }
            .await;
            manager.shutdown().await;
            status_result
        }
    }
}

async fn add_mcp_server(
    path: &Path,
    name: String,
    server: McpServerConfig,
) -> anyhow::Result<String> {
    validate_server_name(&name)?;
    let _config_guard = lock_mcp_json_config(path).await?;
    let mut cfg = read_mcp_json_config(path).await?;
    if cfg.servers.contains_key(&name) {
        anyhow::bail!("MCP server 已存在: {name}；请先 remove 后再 add");
    }
    if oauth::has_pending_cleanup(path, &name).await? {
        anyhow::bail!(
            "MCP server '{name}' 仍有待清理的本地 OAuth 凭据；请先执行 `acn mcp logout {name}`"
        );
    }
    cfg.servers.insert(name.clone(), server);
    write_mcp_json_config_atomic(path, &cfg).await?;
    Ok(format!(
        "added MCP server '{name}'\nconfig: {}\n",
        path.display()
    ))
}

fn parse_mcp_cli_from<I, S>(args: I) -> anyhow::Result<McpCli>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    if args.get(1).map(String::as_str) != Some("mcp") {
        anyhow::bail!("{}", mcp_usage());
    }
    let mut index = 2;
    let mut config = None;
    let mut upstream = None;
    consume_mcp_common_options(&args, &mut index, &mut config, &mut upstream)?;
    let command_name = match args.get(index).map(String::as_str) {
        Some("-h" | "--help") => {
            eprintln!("{}", mcp_usage());
            std::process::exit(0);
        }
        Some(value) => value.to_string(),
        None => anyhow::bail!("{}", mcp_usage()),
    };
    index += 1;
    let command = match command_name.as_str() {
        "list" => {
            parse_only_common_options(&args, &mut index, &mut config, &mut upstream, "list")?;
            McpCommand::List
        }
        "get" => parse_mcp_get(&args, &mut index, &mut config, &mut upstream)?,
        "add" => parse_mcp_add(&args, &mut index, &mut config, &mut upstream)?,
        "add-json" => parse_mcp_add_json(&args, &mut index, &mut config, &mut upstream)?,
        "remove" => parse_mcp_name_command(&args, &mut index, &mut config, &mut upstream, "remove")
            .map(|name| McpCommand::Remove { name })?,
        "enable" => parse_mcp_name_command(&args, &mut index, &mut config, &mut upstream, "enable")
            .map(|name| McpCommand::Enable { name })?,
        "disable" => {
            { parse_mcp_name_command(&args, &mut index, &mut config, &mut upstream, "disable") }
                .map(|name| McpCommand::Disable { name })?
        }
        "login" => parse_mcp_login(&args, &mut index, &mut config, &mut upstream)?,
        "logout" => parse_mcp_name_command(&args, &mut index, &mut config, &mut upstream, "logout")
            .map(|name| McpCommand::Logout { name })?,
        "status" => parse_mcp_status(&args, &mut index, &mut config, &mut upstream)?,
        other => anyhow::bail!("未知 mcp 子命令: {other}\n{}", mcp_usage()),
    };
    Ok(McpCli {
        command,
        config,
        upstream,
    })
}

fn parse_mcp_add_json(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
) -> anyhow::Result<McpCommand> {
    let name = take_mcp_value(args, index, "mcp add-json 后缺少 server name")?;
    validate_server_name(&name)?;
    let raw_json = take_mcp_value(args, index, "mcp add-json 后缺少 server JSON")?;
    let server = serde_json::from_str::<McpServerConfig>(&raw_json)
        .with_context(|| format!("解析 mcp add-json server JSON 失败: {name}"))?;
    McpJsonConfig {
        servers: BTreeMap::from([(name.clone(), server.clone())]),
    }
    .validate()
    .with_context(|| format!("校验 mcp add-json server 配置失败: {name}"))?;
    parse_only_common_options(args, index, config, upstream, "add-json")?;
    Ok(McpCommand::AddJson { name, server })
}

fn parse_mcp_get(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
) -> anyhow::Result<McpCommand> {
    let name = take_mcp_value(args, index, "mcp get 后缺少 server name")?;
    validate_server_name(&name)?;
    let mut json = false;
    while *index < args.len() {
        match args[*index].as_str() {
            "--json" => {
                json = true;
                *index += 1;
            }
            "--config" => {
                *index += 1;
                *config = Some(PathBuf::from(take_mcp_value(
                    args,
                    index,
                    "--config 后缺少路径",
                )?));
            }
            "--upstream" => {
                *index += 1;
                *upstream = Some(take_mcp_value(args, index, "--upstream 后缺少名称")?);
            }
            "-h" | "--help" => {
                eprintln!("{}", mcp_usage());
                std::process::exit(0);
            }
            other => anyhow::bail!("未知 mcp get 参数: {other}"),
        }
    }
    Ok(McpCommand::Get { name, json })
}

fn parse_mcp_add(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
) -> anyhow::Result<McpCommand> {
    let name = take_mcp_value(args, index, "mcp add 后缺少 server name")?;
    validate_server_name(&name)?;
    let mut env = BTreeMap::new();
    let mut env_vars = Vec::new();
    let mut url = None;
    let mut bearer_token_env_var = None;
    let mut oauth_client_id = None;
    let mut oauth_callback_port = None;
    let mut oauth_credentials_store = None;
    let mut stdio_command = None;
    while *index < args.len() {
        match args[*index].as_str() {
            "--" => {
                *index += 1;
                let command_parts = args[*index..].to_vec();
                if command_parts.is_empty() {
                    anyhow::bail!("mcp add 的 -- 后缺少 stdio command");
                }
                stdio_command = Some(command_parts);
                *index = args.len();
            }
            "--config" => {
                *index += 1;
                *config = Some(PathBuf::from(take_mcp_value(
                    args,
                    index,
                    "--config 后缺少路径",
                )?));
            }
            "--upstream" => {
                *index += 1;
                *upstream = Some(take_mcp_value(args, index, "--upstream 后缺少名称")?);
            }
            "-e" => {
                *index += 1;
                let raw = take_mcp_value(args, index, "-e 后缺少 KEY=VALUE")?;
                let (key, value) = parse_env_assignment(&raw)?;
                env.insert(key, value);
            }
            "--env-var" => {
                *index += 1;
                let key = take_mcp_value(args, index, "--env-var 后缺少 KEY")?;
                validate_env_key("--env-var", &key)?;
                env_vars.push(key);
            }
            "--url" => {
                *index += 1;
                url = Some(take_mcp_value(args, index, "--url 后缺少 URL")?);
            }
            "--bearer-token-env-var" => {
                *index += 1;
                let key = take_mcp_value(args, index, "--bearer-token-env-var 后缺少 ENV")?;
                validate_env_key("--bearer-token-env-var", &key)?;
                bearer_token_env_var = Some(key);
            }
            "--oauth-client-id" => {
                *index += 1;
                let client_id = take_mcp_value(args, index, "--oauth-client-id 后缺少 client ID")?;
                if client_id.trim().is_empty() {
                    anyhow::bail!("--oauth-client-id 不能为空");
                }
                oauth_client_id = Some(client_id);
            }
            "--oauth-callback-port" => {
                *index += 1;
                let raw = take_mcp_value(args, index, "--oauth-callback-port 后缺少端口")?;
                let port = raw
                    .parse::<u16>()
                    .ok()
                    .filter(|port| *port > 0)
                    .context("--oauth-callback-port 必须在 1..=65535")?;
                oauth_callback_port = Some(port);
            }
            "--oauth-credentials-store" => {
                *index += 1;
                let value = take_mcp_value(args, index, "--oauth-credentials-store 后缺少类型")?;
                oauth_credentials_store = Some(match value.as_str() {
                    "keyring" => McpOAuthCredentialsStore::Keyring,
                    "file" => McpOAuthCredentialsStore::File,
                    _ => anyhow::bail!("--oauth-credentials-store 仅支持 keyring 或 file"),
                });
            }
            "-h" | "--help" => {
                eprintln!("{}", mcp_usage());
                std::process::exit(0);
            }
            other => anyhow::bail!("未知 mcp add 参数: {other}"),
        }
    }
    let transport = if let Some(url) = url {
        if stdio_command.is_some() {
            anyhow::bail!("mcp add 不能同时使用 --url 和 -- <command...>");
        }
        if bearer_token_env_var.is_some()
            && (oauth_client_id.is_some()
                || oauth_callback_port.is_some()
                || oauth_credentials_store.is_some())
        {
            anyhow::bail!(
                "--bearer-token-env-var 不能与 OAuth 选项同时使用；请选择 bearer 或 OAuth"
            );
        }
        if url.trim().is_empty() {
            anyhow::bail!("--url 不能为空");
        }
        McpAddTransport::StreamableHttp {
            url,
            bearer_token_env_var,
            oauth_client_id,
            oauth_callback_port,
            oauth_credentials_store,
        }
    } else {
        if bearer_token_env_var.is_some()
            || oauth_client_id.is_some()
            || oauth_callback_port.is_some()
            || oauth_credentials_store.is_some()
        {
            anyhow::bail!(
                "stdio server 不支持 HTTP/OAuth 选项；请使用 -e 或 --env-var 传递 stdio 凭据"
            );
        }
        let command_parts =
            stdio_command.context("mcp add stdio server 需要在 -- 后提供 command")?;
        let mut parts = command_parts.into_iter();
        let command = parts
            .next()
            .context("mcp add stdio server command 不能为空")?;
        McpAddTransport::Stdio {
            command,
            args: parts.collect(),
        }
    };
    Ok(McpCommand::Add(McpAddCommand {
        name,
        transport,
        env,
        env_vars,
    }))
}

fn parse_mcp_login(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
) -> anyhow::Result<McpCommand> {
    let name = take_mcp_value(args, index, "mcp login 后缺少 server name")?;
    validate_server_name(&name)?;
    let mut no_browser = false;
    while *index < args.len() {
        match args[*index].as_str() {
            "--no-browser" => {
                no_browser = true;
                *index += 1;
            }
            "--config" => {
                *index += 1;
                *config = Some(PathBuf::from(take_mcp_value(
                    args,
                    index,
                    "--config 后缺少路径",
                )?));
            }
            "--upstream" => {
                *index += 1;
                *upstream = Some(take_mcp_value(args, index, "--upstream 后缺少名称")?);
            }
            "-h" | "--help" => {
                eprintln!("{}", mcp_usage());
                std::process::exit(0);
            }
            other => anyhow::bail!("未知 mcp login 参数: {other}"),
        }
    }
    Ok(McpCommand::Login { name, no_browser })
}

fn parse_mcp_name_command(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
    command: &str,
) -> anyhow::Result<String> {
    let name = take_mcp_value(args, index, &format!("mcp {command} 后缺少 server name"))?;
    validate_server_name(&name)?;
    parse_only_common_options(args, index, config, upstream, command)?;
    Ok(name)
}

fn parse_mcp_status(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
) -> anyhow::Result<McpCommand> {
    let mut name = None;
    while *index < args.len() {
        match args[*index].as_str() {
            "--config" => {
                *index += 1;
                *config = Some(PathBuf::from(take_mcp_value(
                    args,
                    index,
                    "--config 后缺少路径",
                )?));
            }
            "--upstream" => {
                *index += 1;
                *upstream = Some(take_mcp_value(args, index, "--upstream 后缺少名称")?);
            }
            "-h" | "--help" => {
                eprintln!("{}", mcp_usage());
                std::process::exit(0);
            }
            value if name.is_none() => {
                validate_server_name(value)?;
                name = Some(value.to_string());
                *index += 1;
            }
            other => anyhow::bail!("未知 mcp status 参数: {other}"),
        }
    }
    Ok(McpCommand::Status { name })
}

fn parse_only_common_options(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
    command: &str,
) -> anyhow::Result<()> {
    while *index < args.len() {
        match args[*index].as_str() {
            "--config" => {
                *index += 1;
                *config = Some(PathBuf::from(take_mcp_value(
                    args,
                    index,
                    "--config 后缺少路径",
                )?));
            }
            "--upstream" => {
                *index += 1;
                *upstream = Some(take_mcp_value(args, index, "--upstream 后缺少名称")?);
            }
            "-h" | "--help" => {
                eprintln!("{}", mcp_usage());
                std::process::exit(0);
            }
            other => anyhow::bail!("未知 mcp {command} 参数: {other}"),
        }
    }
    Ok(())
}

fn consume_mcp_common_options(
    args: &[String],
    index: &mut usize,
    config: &mut Option<PathBuf>,
    upstream: &mut Option<String>,
) -> anyhow::Result<()> {
    while *index < args.len() {
        match args[*index].as_str() {
            "--config" => {
                *index += 1;
                *config = Some(PathBuf::from(take_mcp_value(
                    args,
                    index,
                    "--config 后缺少路径",
                )?));
            }
            "--upstream" => {
                *index += 1;
                *upstream = Some(take_mcp_value(args, index, "--upstream 后缺少名称")?);
            }
            _ => break,
        }
    }
    Ok(())
}

fn take_mcp_value(args: &[String], index: &mut usize, missing: &str) -> anyhow::Result<String> {
    let value = args
        .get(*index)
        .with_context(|| missing.to_string())?
        .clone();
    *index += 1;
    Ok(value)
}

fn parse_env_assignment(raw: &str) -> anyhow::Result<(String, String)> {
    let (key, value) = raw
        .split_once('=')
        .with_context(|| format!("-e 需要 KEY=VALUE，实际: {raw}"))?;
    validate_env_key("-e", key)?;
    Ok((key.to_string(), value.to_string()))
}

fn validate_env_key(flag: &str, key: &str) -> anyhow::Result<()> {
    if key.trim().is_empty() || key.contains('=') {
        anyhow::bail!("{flag} 的环境变量名无效: {key}");
    }
    Ok(())
}

fn mcp_list_text(path: &Path, cfg: &McpJsonConfig) -> String {
    let mut text = format!("config: {}\n", path.display());
    if cfg.servers.is_empty() {
        text.push_str("MCP servers: 0\n");
        return text;
    }
    text.push_str(&format!("MCP servers: {}\n", cfg.servers.len()));
    for (name, server) in &cfg.servers {
        let transport = server
            .transport_kind(name)
            .map(format_mcp_transport)
            .unwrap_or_else(|_| "invalid".to_string());
        let enabled = if server.is_enabled() {
            "enabled"
        } else {
            "disabled"
        };
        text.push_str(&format!("- {name}  {transport}  {enabled}\n"));
    }
    text
}

fn mcp_server_text(path: &Path, name: &str, server: &McpServerConfig) -> String {
    let transport = server
        .transport_kind(name)
        .map(format_mcp_transport)
        .unwrap_or_else(|_| "invalid".to_string());
    let mut text = String::new();
    text.push_str(&format!("name: {name}\n"));
    text.push_str(&format!("config: {}\n", path.display()));
    text.push_str(&format!("transport: {transport}\n"));
    text.push_str(&format!(
        "enabled: {}\n",
        if server.is_enabled() { "true" } else { "false" }
    ));
    text.push_str(&format!(
        "startup_timeout_secs: {}\n",
        server.startup_timeout_secs()
    ));
    text.push_str(&format!(
        "tool_timeout_secs: {}\n",
        server.tool_timeout_secs()
    ));
    match server.transport_kind(name) {
        Ok(McpTransportKind::Stdio) => {
            text.push_str(&format!(
                "command: {}\n",
                server.command.as_deref().unwrap_or("-")
            ));
            text.push_str(&format!(
                "args: {}\n",
                format_arg_list(server.args.as_deref().unwrap_or(&[]))
            ));
            text.push_str(&format!(
                "cwd: {}\n",
                server
                    .cwd
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "-".to_string())
            ));
            text.push_str(&format!(
                "env keys: {}\n",
                format_string_list(&server.env_keys())
            ));
            text.push_str(&format!(
                "env_vars: {}\n",
                format_string_list(server.env_vars.as_deref().unwrap_or(&[]))
            ));
        }
        Ok(McpTransportKind::StreamableHttp) => {
            text.push_str(&format!(
                "url: {}\n",
                redact_cli_text(server.url.as_deref().unwrap_or("-"))
            ));
            text.push_str(&format!(
                "bearer_token_env_var: {}\n",
                server.bearer_token_env_var.as_deref().unwrap_or("-")
            ));
            text.push_str(&format!(
                "oauth_client_id: {}\n",
                server.oauth_client_id.as_deref().unwrap_or("-")
            ));
            text.push_str(&format!(
                "oauth_callback_port: {}\n",
                server
                    .oauth_callback_port
                    .map(|port| port.to_string())
                    .unwrap_or_else(|| "random".to_string())
            ));
            text.push_str(&format!(
                "oauth_credentials_store: {}\n",
                match server.oauth_credentials_store.unwrap_or_default() {
                    McpOAuthCredentialsStore::Keyring => "keyring",
                    McpOAuthCredentialsStore::File => "file",
                }
            ));
        }
        Err(err) => text.push_str(&format!("error: {err}\n")),
    }
    text.push_str(&format!(
        "enabled_tools: {}\n",
        format_string_list(server.enabled_tools.as_deref().unwrap_or(&[]))
    ));
    text.push_str(&format!(
        "disabled_tools: {}\n",
        format_string_list(server.disabled_tools.as_deref().unwrap_or(&[]))
    ));
    text
}

fn mcp_status_text(
    path: &Path,
    snapshot: &McpRuntimeState,
    name: Option<&str>,
) -> anyhow::Result<String> {
    let mut text = format!("config: {}\n", path.display());
    let servers = if let Some(name) = name {
        let server = snapshot
            .servers
            .get(name)
            .with_context(|| format!("MCP server 不存在: {name}"))?;
        vec![(name.to_string(), server)]
    } else {
        snapshot
            .servers
            .iter()
            .map(|(name, server)| (name.clone(), server))
            .collect::<Vec<_>>()
    };
    if servers.is_empty() {
        text.push_str("MCP servers: 0\n");
        return Ok(text);
    }
    text.push_str(&format!("MCP servers: {}\n", servers.len()));
    for (name, server) in servers {
        let transport = server
            .transport
            .map(format_mcp_transport)
            .unwrap_or_else(|| "invalid".to_string());
        text.push_str(&format!(
            "- {name}  {transport}  {}\n",
            server.status.as_str()
        ));
        text.push_str(&format!(
            "  tools: exposed={} discovered={}\n",
            server.exposed_tool_count(),
            server.discovered_tool_count()
        ));
        if let Some(connected_at) = server.last_connected_at {
            text.push_str(&format!(
                "  last_connected_at: {}\n",
                connected_at.to_rfc3339()
            ));
        }
        if let Some(error) = &server.last_error {
            text.push_str(&format!(
                "  error: {}\n",
                clean_table_field(&redact_cli_text(error))
            ));
        }
        if let Some(stderr) = &server.stderr_excerpt {
            text.push_str(&format!(
                "  stderr: {}\n",
                clean_table_field(&redact_cli_text(stderr))
            ));
        }
        if server.status == McpServerStatus::Ready {
            for tool in &server.tools {
                text.push_str(&format!(
                    "  - {} [{}]\n",
                    tool.raw_name,
                    tool.exposure.label()
                ));
            }
        }
    }
    Ok(text)
}

trait McpServerConfigCliExt {
    fn env_keys(&self) -> Vec<String>;
}

impl McpServerConfigCliExt for McpServerConfig {
    fn env_keys(&self) -> Vec<String> {
        self.env
            .as_ref()
            .map(|env| env.keys().cloned().collect())
            .unwrap_or_default()
    }
}

fn format_mcp_transport(kind: McpTransportKind) -> String {
    match kind {
        McpTransportKind::Stdio => "stdio".to_string(),
        McpTransportKind::StreamableHttp => "streamable_http".to_string(),
    }
}

fn format_string_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(",")
    }
}

fn format_arg_list(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        redacted_arg_values(values).join(",")
    }
}

fn redact_cli_text(value: &str) -> String {
    agent_claim_network::mcp::redact::redact_mcp_sensitive_text(value)
}

fn redacted_mcp_server_config_json(server: &McpServerConfig) -> anyhow::Result<serde_json::Value> {
    let mut value = serde_json::to_value(server)?;
    redact_mcp_server_json_value(&mut value);
    Ok(value)
}

fn redact_mcp_server_json_value(value: &mut serde_json::Value) {
    let serde_json::Value::Object(map) = value else {
        return;
    };
    if let Some(serde_json::Value::Array(args)) = map.get_mut("args") {
        let raw_args = args
            .iter()
            .filter_map(serde_json::Value::as_str)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        *args = redacted_arg_values(&raw_args)
            .into_iter()
            .map(serde_json::Value::String)
            .collect();
    }
    if let Some(serde_json::Value::Object(env)) = map.get_mut("env") {
        for value in env.values_mut() {
            *value = serde_json::Value::String("<redacted>".to_string());
        }
    }
    if let Some(serde_json::Value::String(url)) = map.get_mut("url") {
        *url = redact_cli_text(url);
    }
}

fn redacted_arg_values(values: &[String]) -> Vec<String> {
    let mut redact_next = false;
    values
        .iter()
        .map(|value| {
            if redact_next {
                redact_next = false;
                return "<redacted>".to_string();
            }
            if arg_expects_sensitive_value(value) {
                redact_next = true;
                value.clone()
            } else {
                redact_cli_text(value)
            }
        })
        .collect()
}

fn arg_expects_sensitive_value(value: &str) -> bool {
    if value.contains('=') || value.contains(':') {
        return false;
    }
    let normalized = value
        .trim_start_matches('-')
        .to_ascii_lowercase()
        .replace(['-', '.'], "_");
    normalized.contains("token")
        || normalized.contains("api_key")
        || normalized.contains("apikey")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized == "key"
        || normalized.ends_with("_key")
        || normalized.contains("bearer")
        || normalized.contains("auth")
}

fn mcp_usage() -> &'static str {
    "用法:
  acn mcp list [--config <path>] [--upstream <name>]
  acn mcp get <name> [--json] [--config <path>] [--upstream <name>]
  acn mcp add <name> [-e KEY=VALUE] [--env-var KEY] [--config <path>] [--upstream <name>] -- <command...>
  acn mcp add <name> --url <url> [--bearer-token-env-var ENV | [--oauth-client-id ID] [--oauth-callback-port PORT] [--oauth-credentials-store keyring|file]] [--config <path>] [--upstream <name>]
  acn mcp add-json <name> '<server-json>' [--config <path>] [--upstream <name>]
  acn mcp remove <name> [--config <path>] [--upstream <name>]
  acn mcp enable <name> [--config <path>] [--upstream <name>]
  acn mcp disable <name> [--config <path>] [--upstream <name>]
  acn mcp login <name> [--no-browser] [--config <path>] [--upstream <name>]
  acn mcp logout <name> [--config <path>] [--upstream <name>]
  acn mcp status [name] [--config <path>] [--upstream <name>]

管理选中 upstream 的 MCP server 配置文件 <acn_home>/<upstream>/.mcp.json。add/add-json 只保存配置；status 才会真实连接 server。login 仅适用于支持 OAuth discovery，且支持动态注册或已配置 public client ID 的 streamable_http server。

选项:
  --config <path>             指定 config.toml，用于定位 <acn_home>
  --upstream <name>           选择 [upstreams.<name>]；不传则使用配置里的默认 upstream
  -e KEY=VALUE                stdio server 的字面量环境变量，会写入 .mcp.json
  --env-var KEY               stdio server 运行时从当前进程继承的环境变量名
  --url <url>                 添加 streamable_http MCP endpoint
  --bearer-token-env-var ENV  streamable_http bearer token 所在环境变量名
  --oauth-client-id ID        预注册的 public OAuth client ID；不支持 client secret
  --oauth-callback-port PORT  固定 loopback callback 端口；默认使用随机端口
  --oauth-credentials-store S OAuth 凭据存储：keyring（默认）或 file
  --no-browser                不打开浏览器，提示粘贴完整 redirect URL
  <server-json>                单个 server JSON；type=http 会保存为 streamable_http
  --json                      get 时输出原始 JSON
"
}

fn session_cleanup_text(
    report: &SessionCleanupReport,
    agent_id: &str,
    agent_home: &Path,
    retention_days: u32,
    cutoff: DateTime<Utc>,
    apply: bool,
) -> String {
    let mut text = String::new();
    text.push_str("Session cleanup\n");
    if !apply {
        text.push_str("This is a dry run. Use --apply to delete.\n");
    }
    text.push_str(&format!("agent_id: {agent_id}\n"));
    text.push_str(&format!("agent_home: {}\n", agent_home.display()));
    text.push_str(&format!("retention_days: {retention_days}\n"));
    text.push_str(&format!("cutoff: {}\n\n", cutoff.to_rfc3339()));
    push_session_cleanup_summary(&mut text, report);
    text.push('\n');
    push_session_cleanup_entries(&mut text, &report.entries);
    text
}

fn push_session_cleanup_summary(text: &mut String, report: &SessionCleanupReport) {
    let rows = [
        ("scanned", report.scanned),
        ("eligible", report.eligible),
        ("deleted", report.deleted),
        ("skipped", report.skipped),
        ("sqlite_purged", report.sqlite_purged),
        ("errors", report.errors),
    ];
    let label_width = rows
        .iter()
        .map(|(label, _)| UnicodeWidthStr::width(*label))
        .max()
        .unwrap_or(0);
    text.push_str("summary:\n");
    for (label, value) in rows {
        let padding = label_width.saturating_sub(UnicodeWidthStr::width(label));
        text.push_str("  ");
        text.push_str(label);
        text.extend(std::iter::repeat_n(' ', padding));
        text.push_str("  ");
        text.push_str(&value.to_string());
        text.push('\n');
    }
}

fn push_session_cleanup_entries(text: &mut String, entries: &[SessionCleanupEntry]) {
    text.push_str("sessions:\n");
    let header = [
        "Outcome",
        "Session ID",
        "Last Activity At",
        "SQLite",
        "Reason",
    ];
    let mut sorted_entries = entries.iter().collect::<Vec<_>>();
    sorted_entries.sort_by(|left, right| session_cleanup_entry_order(left, right));
    let rows = sorted_entries
        .into_iter()
        .map(|entry| {
            [
                cleanup_outcome_label(entry.outcome).to_string(),
                entry
                    .session_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "-".to_string()),
                entry
                    .last_activity_at
                    .as_ref()
                    .map(DateTime::to_rfc3339)
                    .unwrap_or_else(|| "-".to_string()),
                if entry.sqlite_purged { "purged" } else { "-" }.to_string(),
                cleanup_reason_label(&entry.reason),
            ]
        })
        .collect::<Vec<_>>();
    let widths = session_cleanup_column_widths(&header, &rows);
    let separator = widths.map(|width| "-".repeat(width));
    push_session_cleanup_row(text, &header, &widths);
    push_session_cleanup_row(text, &separator, &widths);
    for row in rows {
        push_session_cleanup_row(text, &row, &widths);
    }
}

fn session_cleanup_entry_order(
    left: &SessionCleanupEntry,
    right: &SessionCleanupEntry,
) -> Ordering {
    cleanup_outcome_rank(left.outcome)
        .cmp(&cleanup_outcome_rank(right.outcome))
        .then_with(|| compare_optional_activity(left.last_activity_at, right.last_activity_at))
        .then_with(|| {
            left.session_id
                .as_ref()
                .map(SessionId::as_str)
                .unwrap_or("")
                .cmp(
                    right
                        .session_id
                        .as_ref()
                        .map(SessionId::as_str)
                        .unwrap_or(""),
                )
        })
}

fn cleanup_outcome_rank(outcome: SessionCleanupOutcome) -> u8 {
    match outcome {
        SessionCleanupOutcome::DeletedWithIndexError => 0,
        SessionCleanupOutcome::Deleted => 1,
        SessionCleanupOutcome::IndexPurged => 2,
        SessionCleanupOutcome::DryRun => 3,
        SessionCleanupOutcome::Error => 4,
        SessionCleanupOutcome::Aborted => 5,
        SessionCleanupOutcome::Skipped => 6,
    }
}

fn compare_optional_activity(
    left: Option<DateTime<Utc>>,
    right: Option<DateTime<Utc>>,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn cleanup_outcome_label(outcome: SessionCleanupOutcome) -> &'static str {
    match outcome {
        SessionCleanupOutcome::DryRun => "dry-run",
        SessionCleanupOutcome::Deleted => "deleted",
        SessionCleanupOutcome::DeletedWithIndexError => "deleted_index_error",
        SessionCleanupOutcome::Skipped => "skipped",
        SessionCleanupOutcome::Error => "error",
        SessionCleanupOutcome::IndexPurged => "index_purged",
        SessionCleanupOutcome::Aborted => "aborted",
    }
}

fn cleanup_reason_label(reason: &str) -> String {
    let cleaned = clean_table_field(reason).trim().to_string();
    let mut chars = cleaned.chars();
    let Some(first) = chars.next() else {
        return "Unknown.".to_string();
    };
    let mut label = first.to_uppercase().collect::<String>();
    label.push_str(chars.as_str());
    if !label.ends_with('.') {
        label.push('.');
    }
    label
}

fn session_cleanup_column_widths(header: &[&str; 5], rows: &[[String; 5]]) -> [usize; 5] {
    let mut widths = header.map(UnicodeWidthStr::width);
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(UnicodeWidthStr::width(cell.as_str()));
        }
    }
    widths
}

fn push_session_cleanup_row<T: AsRef<str>>(text: &mut String, row: &[T; 5], widths: &[usize; 5]) {
    text.push_str("  ");
    for (index, cell) in row.iter().enumerate() {
        if index > 0 {
            text.push_str("  ");
        }
        let cell = cell.as_ref();
        text.push_str(cell);
        if index + 1 < row.len() {
            let padding = widths[index].saturating_sub(UnicodeWidthStr::width(cell));
            text.extend(std::iter::repeat_n(' ', padding));
        }
    }
    text.push('\n');
}

async fn run_supervisor_cli(args: Vec<String>) -> anyhow::Result<()> {
    let cli = parse_supervisor_cli_from(args)?;
    let (mut cfg, cfg_path) = match cli.command {
        SupervisorCommand::Run | SupervisorCommand::Retry => {
            Config::load_or_init_for_agent(cli.config.as_deref())
        }
        SupervisorCommand::Status | SupervisorCommand::Jobs { .. } | SupervisorCommand::Stop => {
            Config::load_or_init_for_supervisor_control(cli.config.as_deref())
        }
    }
    .with_context(|| format!("加载 supervisor config: {:?}", cli.config))?;
    let upstream = cfg
        .resolve_upstream(cli.upstream.as_deref())
        .context("解析 supervisor upstream 失败")?;
    let agent_home = if matches!(
        cli.command,
        SupervisorCommand::Run | SupervisorCommand::Retry
    ) {
        activate_acn_upstream_runtime(
            &mut cfg,
            &upstream,
            "激活 supervisor upstream 本地目录失败",
        )?;
        cfg.agent_home(&upstream.agent_id)
    } else {
        paths::runtime_agent_home(&upstream.runtime_acn_home, &upstream.agent_id)
    };
    match cli.command {
        SupervisorCommand::Run => {
            // recap/finalize 不调用 agent 工具；给复用的 SessionEngine 一个稳定、中性的工作目录。
            cfg.set_tool_workspace_root(agent_home.clone());
            let runtime_fingerprint = supervisor::runtime_fingerprint(&cfg, &upstream)?;
            if let Some(expected) = cli.expected_runtime_fingerprint.as_deref() {
                if expected != runtime_fingerprint.digest {
                    anyhow::bail!(
                        "supervisor 配置在拉起期间发生变化: expected={}, actual={}",
                        short_fingerprint(expected),
                        short_fingerprint(&runtime_fingerprint.digest)
                    );
                }
            }
            let engine = bootstrap::build_agent_cli_session_engine(&cfg, &upstream)?
                .with_fork_memory_review(false);
            supervisor::run_supervisor(engine, agent_home, runtime_fingerprint).await
        }
        SupervisorCommand::Retry => {
            let target = cli
                .retry_target
                .context("acn supervisor retry 缺少 session_id 或 job_id")?;
            // 先完成与子进程相同的 engine 构造校验，避免无效新配置先接管并停掉旧实例。
            cfg.set_tool_workspace_root(agent_home.clone());
            let _engine_preflight = bootstrap::build_agent_cli_session_engine(&cfg, &upstream)?
                .with_fork_memory_review(false);
            let runtime_fingerprint = supervisor::runtime_fingerprint(&cfg, &upstream)?;
            let launch = SupervisorLaunchConfig::new(
                agent_home,
                cfg_path,
                Some(upstream.name.clone()),
                cfg.agent.session.notify_on_finalize_completion,
                runtime_fingerprint,
            );
            let report = supervisor::retry_finalize(&launch, target).await?;
            print!("{}", supervisor_retry_report_text(&report));
            Ok(())
        }
        SupervisorCommand::Status => {
            let status = supervisor::supervisor_status(&agent_home).await?;
            print!(
                "{}",
                supervisor_status_text(&status, upstream.agent_id.as_str(), &agent_home)
            );
            Ok(())
        }
        SupervisorCommand::Jobs { limit } => {
            let jobs = supervisor::supervisor_jobs(&agent_home).await?;
            print!("{}", supervisor_jobs_text(&jobs, limit));
            Ok(())
        }
        SupervisorCommand::Stop => {
            let report = supervisor::stop_supervisor(&agent_home).await?;
            print!("{}", supervisor_stop_report_text(&report));
            Ok(())
        }
    }
}

fn parse_cli_from<I, S>(args: I) -> anyhow::Result<Cli>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut config = None;
    let mut upstream = None;
    let mut resume = StartupResume::None;
    let mut cd = None;
    let mut args = args.into_iter().map(Into::into).skip(1).peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = Some(PathBuf::from(args.next().context("--config 后缺少路径")?)),
            "--upstream" => upstream = Some(args.next().context("--upstream 后缺少名称")?),
            "--resume" => {
                resume = match args.peek() {
                    Some(next) if !next.starts_with('-') => {
                        let raw = args.next().context("--resume 后缺少 session_id")?;
                        StartupResume::Session(
                            raw.parse::<SessionId>()
                                .with_context(|| format!("--resume session_id 无效: {raw}"))?,
                        )
                    }
                    _ => StartupResume::Picker,
                }
            }
            "--cd" | "-C" => cd = Some(PathBuf::from(args.next().context("--cd 后缺少目录参数")?)),
            "-h" | "--help" => {
                eprintln!("{}", acn_usage());
                std::process::exit(0);
            }
            other => anyhow::bail!("未知参数: {other}"),
        }
    }
    Ok(Cli {
        config,
        upstream,
        resume,
        cd,
    })
}

fn acn_usage() -> &'static str {
    "用法:
  acn [options]
  acn update [--url <git-url>] [--branch <branch>] [--config <path>]
  acn session cleanup [--apply] [options]
  acn supervisor <status|jobs|retry|stop> [options]
  acn mcp <list|get|add|remove|enable|disable|login|logout|status> [options]

启动 ACN 交互式 TUI session。普通用户通常只需要运行 `acn`；后台 recap/finalize supervisor 会自动启动，无需手动管理。

选项:
  --config <path>             指定 config.toml；不传则按 ACN_CONFIG 和默认配置查找
  --upstream <name>           选择 [upstreams.<name>]；不传则使用配置里的默认 upstream
  --resume [session_id]       不带 session_id 时打开恢复列表；带 session_id 时直接恢复指定会话
  --cd <dir>, -C <dir>        指定 agent 工具读写文件和执行命令的工作目录
  --version, -V               显示版本号、构建提交和提交时间
  -h, --help                  显示帮助

Session 维护命令:
  acn session cleanup         Dry-run 预览可清理的旧 Closed sessions 和孤儿 search index
  acn session cleanup --apply 删除可清理的旧 Closed sessions，并清理 search index

更新命令:
  acn update                                      更新 Cargo 安装；Homebrew 安装请用 brew upgrade acn
  acn update --branch <branch>                    从默认仓库的指定远端 branch 更新
  acn update --url <git-url>                      临时改用其他可信的 ACN Git 仓库
  acn update --config <path>                      使用指定配置定位并停止 supervisor

Supervisor 排障命令:
  acn supervisor status       查看后台 supervisor 是否在运行、PID、uptime 和队列概况
  acn supervisor jobs [-l n]  查看最近 recap/finalize job；默认 5 条，-l 0 显示全部
  acn supervisor retry <id>   按 session_id（推荐）或 job_id 重试失败的 finalize
  acn supervisor stop         优雅停止后台 supervisor

MCP 配置命令:
  acn mcp list [--upstream <name>]    查看选中 upstream 的 MCP server 配置
  acn mcp get <name>                  查看单个 MCP server 的详细配置
  acn mcp add <name> ...              新增 stdio server 或 streamable_http endpoint
  acn mcp add-json <name> <json>      从单个 server JSON 新增 MCP server
  acn mcp remove <name>               删除 MCP server 配置及其本地 OAuth 凭据
  acn mcp enable <name>               启用 MCP server
  acn mcp disable <name>              禁用但保留 MCP server 配置
  acn mcp login <name>                OAuth 登录；headless 环境可加 --no-browser
  acn mcp logout <name>               退出 MCP OAuth 登录
  acn mcp status [name]               连接检查 MCP server
"
}

#[derive(Debug)]
struct SupervisorCli {
    command: SupervisorCommand,
    config: Option<PathBuf>,
    upstream: Option<String>,
    retry_target: Option<SupervisorRetryTarget>,
    expected_runtime_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisorCommand {
    Run,
    Status,
    Jobs { limit: usize },
    Retry,
    Stop,
}

fn parse_supervisor_cli_from<I, S>(args: I) -> anyhow::Result<SupervisorCli>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    if args.get(1).map(String::as_str) != Some("supervisor") {
        anyhow::bail!("{}", supervisor_usage());
    }
    let command = match args.get(2).map(String::as_str) {
        Some("run") => SupervisorCommand::Run,
        Some("status") => SupervisorCommand::Status,
        Some("jobs") => SupervisorCommand::Jobs {
            limit: DEFAULT_SUPERVISOR_JOBS_LIMIT,
        },
        Some("retry") => SupervisorCommand::Retry,
        Some("stop") => SupervisorCommand::Stop,
        Some("-h" | "--help") => {
            eprintln!("{}", supervisor_usage());
            std::process::exit(0);
        }
        Some(other) => anyhow::bail!("未知 supervisor 子命令: {other}\n{}", supervisor_usage()),
        None => anyhow::bail!("{}", supervisor_usage()),
    };
    if command == SupervisorCommand::Run && args.get(3).map(String::as_str) == Some("--help") {
        eprintln!("{}", supervisor_usage());
        std::process::exit(0);
    }
    let mut config = None;
    let mut upstream = None;
    let mut retry_target = None;
    let mut expected_runtime_fingerprint = None;
    let mut jobs_limit_override = None;
    let mut args = args.into_iter().skip(3);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = Some(PathBuf::from(args.next().context("--config 后缺少路径")?)),
            "--upstream" => upstream = Some(args.next().context("--upstream 后缺少名称")?),
            "--runtime-fingerprint" if command == SupervisorCommand::Run => {
                let value = args.next().context("--runtime-fingerprint 后缺少摘要")?;
                validate_runtime_fingerprint_arg(&value)?;
                expected_runtime_fingerprint = Some(value);
            }
            "--runtime-fingerprint" => {
                anyhow::bail!("--runtime-fingerprint 仅支持 acn supervisor run")
            }
            "-l" | "--limit" if matches!(command, SupervisorCommand::Jobs { .. }) => {
                let raw = args.next().context("-l 后缺少数量")?;
                jobs_limit_override = Some(
                    raw.parse::<usize>()
                        .with_context(|| format!("-l 只接受非负整数，实际: {raw}"))?,
                );
            }
            "-l" | "--limit" => anyhow::bail!("-l 仅支持 acn supervisor jobs"),
            "-h" | "--help" => {
                eprintln!("{}", supervisor_usage());
                std::process::exit(0);
            }
            other if command == SupervisorCommand::Retry && !other.starts_with('-') => {
                if retry_target.is_some() {
                    anyhow::bail!("acn supervisor retry 只接受一个 session_id 或 job_id");
                }
                retry_target = Some(parse_supervisor_retry_target(other)?);
            }
            other => anyhow::bail!("未知 supervisor 参数: {other}"),
        }
    }
    let command = match (command, jobs_limit_override) {
        (SupervisorCommand::Jobs { .. }, Some(limit)) => SupervisorCommand::Jobs { limit },
        (command, _) => command,
    };
    if command == SupervisorCommand::Retry && retry_target.is_none() {
        anyhow::bail!("acn supervisor retry 缺少 session_id 或 job_id");
    }
    Ok(SupervisorCli {
        command,
        config,
        upstream,
        retry_target,
        expected_runtime_fingerprint,
    })
}

fn parse_supervisor_retry_target(value: &str) -> anyhow::Result<SupervisorRetryTarget> {
    if value.starts_with(SessionId::PREFIX) {
        let session_id = value
            .parse::<SessionId>()
            .with_context(|| format!("无效的 supervisor retry session_id: {value}"))?;
        return Ok(SupervisorRetryTarget::Session { session_id });
    }
    if value.strip_prefix("job_").is_some_and(|suffix| {
        !suffix.is_empty()
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    }) {
        return Ok(SupervisorRetryTarget::Job {
            job_id: value.to_string(),
        });
    }
    anyhow::bail!("supervisor retry id 必须是有效的 session_id 或 job_id，实际: {value}")
}

fn validate_runtime_fingerprint_arg(value: &str) -> anyhow::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Ok(());
    }
    anyhow::bail!("--runtime-fingerprint 必须是 64 位小写十六进制摘要")
}

fn short_fingerprint(value: &str) -> &str {
    value.get(..12).unwrap_or(value)
}

fn supervisor_usage() -> &'static str {
    "用法:
  acn supervisor status [options]
  acn supervisor jobs [options]
  acn supervisor retry <session_id|job_id> [options]
  acn supervisor stop [options]
  acn supervisor run [options]

管理 ACN recap/finalize supervisor。普通用户无需手动启动；`run` 是 ACN 自动拉起的后台内部命令，开发和排障时主要使用 status/jobs/retry/stop。

选项:
  --config <path>      指定 config.toml；应与启动 TUI 时使用的配置一致
  --upstream <name>    选择 [upstreams.<name>]；应与要排查的 agent upstream 一致
  -l <n>, --limit <n>  仅 jobs 使用，默认 5；0 表示显示全部
  -h, --help           显示帮助
"
}

fn supervisor_status_text(
    status: &supervisor::SupervisorStatusSnapshot,
    agent_id: &str,
    agent_home: &Path,
) -> String {
    let mut text = String::new();
    let status_label = match &status.runtime_state {
        supervisor::SupervisorRuntimeState::Running => "running",
        supervisor::SupervisorRuntimeState::Stopped => "stopped",
        supervisor::SupervisorRuntimeState::Stuck { .. } => "stuck",
    };
    text.push_str(&format!("supervisor: {status_label}"));
    text.push('\n');
    match (&status.runtime_state, status.pid) {
        (supervisor::SupervisorRuntimeState::Running, Some(pid)) => {
            text.push_str(&format!("pid: {pid}\n"));
        }
        (supervisor::SupervisorRuntimeState::Stopped, Some(pid)) => {
            text.push_str(&format!("pid: {pid} (stale)\n"));
        }
        (supervisor::SupervisorRuntimeState::Stuck { .. }, Some(pid)) => {
            text.push_str(&format!("pid: {pid} (ipc unresponsive)\n"));
        }
        (_, None) => text.push_str("pid: -\n"),
    }
    text.push_str(&format!(
        "uptime: {}\n",
        format_uptime(status.started_at.as_ref())
    ));
    match &status.build {
        Some(build) => text.push_str(&format!("build: {} ({})\n", build.version, build.commit)),
        None => text.push_str("build: -\n"),
    }
    match &status.runtime_fingerprint {
        Some(fingerprint) => text.push_str(&format!(
            "runtime: v{}:{}\n",
            fingerprint.schema,
            short_fingerprint(&fingerprint.digest)
        )),
        None => text.push_str("runtime: -\n"),
    }
    text.push_str(&format!("agent_id: {agent_id}\n"));
    text.push_str(&format!("agent_home: {}\n", agent_home.display()));
    text.push_str(&format!("socket: {}\n", status.socket_path.display()));
    text.push_str(&format!("pid_file: {}\n", status.pid_path.display()));
    if let supervisor::SupervisorRuntimeState::Stuck { ipc_error } = &status.runtime_state {
        text.push_str(&format!("ipc_error: {ipc_error}\n"));
        match status.pid {
            Some(pid) => text.push_str(&format!(
                "hint: supervisor process is alive but did not answer IPC; first try `acn supervisor stop`, then run `kill -9 {pid}` if it still does not respond\n"
            )),
            None => text.push_str(
                "hint: supervisor did not answer IPC, but no pid file was found\n",
            ),
        }
    }
    text.push_str(&format!(
        "jobs: total={} queued={} running={} succeeded={} failed={}",
        status.queue.total,
        status.queue.queued,
        status.queue.running,
        status.queue.succeeded,
        status.queue.failed
    ));
    text.push('\n');
    if let Some(job) = &status.current_job {
        let label = match &status.runtime_state {
            supervisor::SupervisorRuntimeState::Running => "current",
            supervisor::SupervisorRuntimeState::Stopped => "stale_current",
            supervisor::SupervisorRuntimeState::Stuck { .. } => "stuck_current",
        };
        text.push_str(&format!(
            "{}: {} kind={} agent_id={} session_id={} target={} attempts={} manual_retries={} started_at={}",
            label,
            job.id,
            job.kind,
            job.agent_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "-".to_string()),
            job.session_id,
            job.recap_end_index
                .map(|target| target.to_string())
                .unwrap_or_else(|| "-".to_string()),
            job.attempts,
            job.manual_retries,
            format_optional_time(job.started_at.as_ref())
        ));
        text.push('\n');
    } else {
        text.push_str("current: -\n");
    }
    text
}

fn supervisor_jobs_text(jobs: &[supervisor::SupervisorJobView], limit: usize) -> String {
    let header = [
        "job_id",
        "kind",
        "agent_id",
        "session_id",
        "target",
        "status",
        "created_at",
        "started_at",
        "finished_at",
        "attempts",
        "manual_retries",
        "last_error",
    ];
    let visible_jobs = recent_supervisor_jobs(jobs, limit);
    let rows = visible_jobs
        .into_iter()
        .map(|job| {
            [
                clean_table_field(&job.id),
                clean_table_field(&job.kind),
                clean_table_field(
                    &job.agent_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "-".to_string()),
                ),
                clean_table_field(job.session_id.as_str()),
                job.recap_end_index
                    .map(|target| target.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                clean_table_field(&job.status),
                format_time(&job.created_at),
                format_optional_time(job.started_at.as_ref()),
                format_optional_time(job.finished_at.as_ref()),
                job.attempts.to_string(),
                job.manual_retries.to_string(),
                clean_table_field(job.last_error.as_deref().unwrap_or("-")),
            ]
        })
        .collect::<Vec<_>>();
    let widths = supervisor_jobs_column_widths(&header, &rows);
    let separator = widths.map(|width| "-".repeat(width));

    let mut text = String::new();
    push_supervisor_jobs_row(&mut text, &header, &widths);
    push_supervisor_jobs_row(&mut text, &separator, &widths);
    for row in rows {
        push_supervisor_jobs_row(&mut text, &row, &widths);
    }
    text
}

fn recent_supervisor_jobs(
    jobs: &[supervisor::SupervisorJobView],
    limit: usize,
) -> Vec<&supervisor::SupervisorJobView> {
    let start = if limit == 0 || jobs.len() <= limit {
        0
    } else {
        jobs.len() - limit
    };
    jobs[start..].iter().rev().collect()
}

fn supervisor_jobs_column_widths(header: &[&str; 12], rows: &[[String; 12]]) -> [usize; 12] {
    let mut widths = header.map(UnicodeWidthStr::width);
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(UnicodeWidthStr::width(cell.as_str()));
        }
    }
    widths
}

fn push_supervisor_jobs_row<T: AsRef<str>>(text: &mut String, row: &[T; 12], widths: &[usize; 12]) {
    for (index, cell) in row.iter().enumerate() {
        if index > 0 {
            text.push_str("  ");
        }
        let cell = cell.as_ref();
        text.push_str(cell);
        if index + 1 < row.len() {
            let padding = widths[index].saturating_sub(UnicodeWidthStr::width(cell));
            text.extend(std::iter::repeat_n(' ', padding));
        }
    }
    text.push('\n');
}

fn supervisor_stop_report_text(report: &supervisor::SupervisorStopReport) -> String {
    let mut text = String::new();
    if !report.was_running {
        match report.pid {
            Some(pid) => text.push_str(&format!("supervisor: not running (stale pid: {pid})\n")),
            None => text.push_str("supervisor: not running\n"),
        }
        return text;
    }
    if report.stopped {
        text.push_str("supervisor: stopped\n");
    } else {
        text.push_str("supervisor: stop requested; supervisor still shutting down\n");
    }
    if let Some(pid) = report.pid {
        text.push_str(&format!("pid: {pid}\n"));
    }
    text
}

fn supervisor_retry_report_text(report: &supervisor::SupervisorRetryReport) -> String {
    format!(
        "finalize retry queued\nsession_id: {}\njob_id: {}\nattempts: {} -> 0\nmanual_retries: {}\n",
        report.session_id, report.job_id, report.previous_attempts, report.manual_retries
    )
}

fn format_uptime(started_at: Option<&DateTime<Utc>>) -> String {
    let Some(started_at) = started_at else {
        return "-".to_string();
    };
    let seconds = Utc::now()
        .signed_duration_since(*started_at)
        .num_seconds()
        .max(0);
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    if days > 0 {
        format!("{days}d {hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    }
}

fn format_time(value: &DateTime<Utc>) -> String {
    value.to_rfc3339()
}

fn format_optional_time(value: Option<&DateTime<Utc>>) -> String {
    value.map(format_time).unwrap_or_else(|| "-".to_string())
}

fn clean_table_field(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '\t' | '\n' | '\r' => ' ',
            _ => ch,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use agent_claim_network::build_info;
    use agent_claim_network::claim::{AgentId, SessionId};
    use agent_claim_network::config::Config;
    use agent_claim_network::session::{
        SessionCleanupEntry, SessionCleanupOutcome, SessionCleanupReport, SessionMetadata,
        SessionStatus,
    };
    use agent_claim_network::supervisor::{
        SupervisorJobView, SupervisorQueueSummary, SupervisorRuntimeState,
        SupervisorStatusSnapshot, SupervisorStopReport,
    };
    use chrono::{DateTime, Utc};
    use std::path::PathBuf;
    use std::str::FromStr;

    const TEST_UPDATE_URL: &str = "git@example.com:team/agent-claim-network.git";

    fn test_session_metadata(
        agent_id: AgentId,
        session_id: SessionId,
        status: SessionStatus,
    ) -> SessionMetadata {
        let now = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        SessionMetadata {
            id: session_id,
            agent_id,
            status,
            created_at: now,
            updated_at: now,
            closed_at: None,
            source: "tui".to_string(),
            model: "test-model".to_string(),
            system_prompt_path: "system_prompt.md".to_string(),
            message_count: 0,
            finalized_at: None,
            recapped_until: 0,
            provider_background_completion_until_seq: Some(0),
            recap_background_completion_until_seq: Some(0),
            compaction: None,
        }
    }

    #[test]
    fn agent_upstream_activation_preserves_shared_daemon_team_storage() {
        let dir = tempfile::tempdir().unwrap();
        let acn_home = dir.path().join("acn");
        let claim_path = acn_home
            .join("data")
            .join("team")
            .join("agents")
            .join("agent-a")
            .join("claims")
            .join("claim.yaml");
        std::fs::create_dir_all(claim_path.parent().unwrap()).unwrap();
        std::fs::write(&claim_path, "id: claim_1234abcd\n").unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = ""
router_endpoint = ""

[storage]
acn_home = "{}"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "test-model"
api_key_env = "PATH"
"#,
                acn_home.display()
            ),
        )
        .unwrap();

        let (mut cfg, _) = Config::load_or_init_for_agent(Some(&config_path)).unwrap();
        let upstream = cfg.resolve_upstream(None).unwrap();
        super::activate_acn_upstream_runtime(&mut cfg, &upstream, "激活测试 upstream 失败")
            .unwrap();

        assert_eq!(
            std::fs::read_to_string(&claim_path).unwrap(),
            "id: claim_1234abcd\n"
        );
        assert_eq!(cfg.storage.team_root, acn_home.join("data").join("team"));
        assert!(!acn_home.join("dev").join("data").join("team").exists());
    }

    #[tokio::test]
    async fn supervisor_stop_is_available_when_legacy_upstream_team_storage_is_nonempty() {
        let dir = tempfile::tempdir().unwrap();
        let acn_home = dir.path().join("acn");
        let legacy_claim = acn_home
            .join("dev")
            .join("data")
            .join("team")
            .join("claim.yaml");
        std::fs::create_dir_all(legacy_claim.parent().unwrap()).unwrap();
        std::fs::write(&legacy_claim, "id: claim_1234abcd\n").unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = ""
router_endpoint = ""

[storage]
acn_home = "{}"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "test-model"
api_key_env = "UNUSED_TEST_LLM_KEY"
"#,
                acn_home.display()
            ),
        )
        .unwrap();

        super::run_supervisor_cli(vec![
            "acn".to_string(),
            "supervisor".to_string(),
            "stop".to_string(),
            "--config".to_string(),
            config_path.display().to_string(),
        ])
        .await
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(&legacy_claim).unwrap(),
            "id: claim_1234abcd\n"
        );
        assert!(!acn_home.join("dev").join("data").join("agents").exists());
    }

    #[test]
    fn parse_cli_accepts_upstream_flag() {
        let cli = super::parse_cli_from([
            "acn",
            "--config",
            "config.toml",
            "--upstream",
            "agent_hub",
            "--cd",
            ".",
        ])
        .unwrap();

        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(cli.upstream.as_deref(), Some("agent_hub"));
        assert_eq!(cli.resume, super::StartupResume::None);
        assert_eq!(cli.cd.as_deref(), Some(std::path::Path::new(".")));
    }

    #[test]
    fn parse_cli_accepts_interactive_session_without_task() {
        let cli = super::parse_cli_from(["acn", "--config", "config.toml"]).unwrap();

        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(cli.upstream, None);
        assert_eq!(cli.resume, super::StartupResume::None);
    }

    #[test]
    fn parse_cli_leaves_config_empty_when_unspecified() {
        let cli = super::parse_cli_from(["acn"]).unwrap();

        assert_eq!(cli.config, None);
        assert_eq!(cli.upstream, None);
    }

    #[test]
    fn version_flags_are_recognized_only_as_the_sole_argument() {
        assert!(build_info::version_requested(&[
            "acn".to_string(),
            "--version".to_string()
        ]));
        assert!(build_info::version_requested(&[
            "acn".to_string(),
            "-V".to_string()
        ]));
        assert!(!build_info::version_requested(&["acn".to_string()]));
        assert!(!build_info::version_requested(&[
            "acn".to_string(),
            "--version".to_string(),
            "extra".to_string(),
        ]));
    }

    #[test]
    fn parse_update_cli_defaults_to_public_repository_and_main() {
        let cli = super::parse_update_cli_from(["acn", "update"]).unwrap();

        assert_eq!(cli.url, super::DEFAULT_UPDATE_REPOSITORY_URL);
        assert_eq!(cli.branch, "main");
        assert_eq!(cli.config, None);
        assert_eq!(cli.retry_command, "acn update");
    }

    #[test]
    fn parse_update_cli_accepts_explicit_url_and_defaults_to_main() {
        let cli =
            super::parse_update_cli_from(["acn", "update", "--url", TEST_UPDATE_URL]).unwrap();

        assert_eq!(cli.url, TEST_UPDATE_URL);
        assert_eq!(cli.branch, "main");
        assert_eq!(cli.config, None);
        assert_eq!(
            cli.retry_command,
            "acn update --url git@example.com:team/agent-claim-network.git"
        );
    }

    #[test]
    fn parse_update_cli_accepts_remote_branch_with_slashes() {
        let cli = super::parse_update_cli_from([
            "acn",
            "update",
            "--url",
            TEST_UPDATE_URL,
            "--branch",
            "feature/file-diff-display",
        ])
        .unwrap();

        assert_eq!(cli.url, TEST_UPDATE_URL);
        assert_eq!(cli.branch, "feature/file-diff-display");
        assert_eq!(cli.config, None);
        assert_eq!(
            cli.retry_command,
            "acn update --url git@example.com:team/agent-claim-network.git --branch feature/file-diff-display"
        );
    }

    #[test]
    fn parse_update_cli_accepts_config_and_preserves_it_in_retry_command() {
        let cli = super::parse_update_cli_from([
            "acn",
            "update",
            "--url",
            TEST_UPDATE_URL,
            "--config",
            "/tmp/acn config.toml",
            "--branch",
            "feature/sample",
        ])
        .unwrap();

        assert_eq!(cli.url, TEST_UPDATE_URL);
        assert_eq!(cli.branch, "feature/sample");
        assert_eq!(cli.config, Some(PathBuf::from("/tmp/acn config.toml")));
        assert_eq!(
            cli.retry_command,
            "acn update --url git@example.com:team/agent-claim-network.git --config '/tmp/acn config.toml' --branch feature/sample"
        );
    }

    #[test]
    fn parse_update_cli_rejects_invalid_explicit_url() {
        let empty = super::parse_update_cli_from(["acn", "update", "--url", "  "])
            .unwrap_err()
            .to_string();
        assert!(empty.contains("--url 不能为空"));

        let duplicate = super::parse_update_cli_from([
            "acn",
            "update",
            "--url",
            TEST_UPDATE_URL,
            "--url",
            "https://example.com/acn.git",
        ])
        .unwrap_err()
        .to_string();
        assert!(duplicate.contains("--url 不能重复指定"));

        let missing_value = super::parse_update_cli_from(["acn", "update", "--url"])
            .unwrap_err()
            .to_string();
        assert!(missing_value.contains("--url 后缺少 Git 仓库地址"));
    }

    #[test]
    fn parse_update_cli_rejects_duplicate_or_missing_optional_values() {
        let duplicate = super::parse_update_cli_from([
            "acn",
            "update",
            "--branch",
            "main",
            "--branch",
            "feature/sample",
        ])
        .unwrap_err()
        .to_string();
        assert!(duplicate.contains("--branch 不能重复指定"));

        let missing = super::parse_update_cli_from(["acn", "update", "--branch"])
            .unwrap_err()
            .to_string();
        assert!(missing.contains("--branch 后缺少远端 branch 名称"));

        let duplicate_config = super::parse_update_cli_from([
            "acn", "update", "--config", "one.toml", "--config", "two.toml",
        ])
        .unwrap_err()
        .to_string();
        assert!(duplicate_config.contains("--config 不能重复指定"));

        let missing_config = super::parse_update_cli_from(["acn", "update", "--config"])
            .unwrap_err()
            .to_string();
        assert!(missing_config.contains("--config 后缺少路径"));
    }

    #[test]
    fn version_text_includes_package_version_git_commit_and_timestamp() {
        assert_eq!(
            build_info::version_text("acn"),
            format!(
                "acn {} ({}, {})",
                env!("CARGO_PKG_VERSION"),
                env!("ACN_GIT_COMMIT"),
                env!("ACN_GIT_COMMIT_TIMESTAMP")
            )
        );
    }

    #[test]
    fn parse_session_cleanup_defaults_to_dry_run() {
        let cli = super::parse_session_cli_from(["acn", "session", "cleanup"]).unwrap();

        assert_eq!(cli.command, super::SessionCommand::Cleanup { apply: false });
        assert_eq!(cli.config, None);
        assert_eq!(cli.upstream, None);
    }

    #[test]
    fn parse_session_cleanup_accepts_apply_and_common_options() {
        let cli = super::parse_session_cli_from([
            "acn",
            "session",
            "cleanup",
            "--apply",
            "--config",
            "config.toml",
            "--upstream",
            "agent_b",
        ])
        .unwrap();

        assert_eq!(cli.command, super::SessionCommand::Cleanup { apply: true });
        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(cli.upstream.as_deref(), Some("agent_b"));
    }

    #[test]
    fn parse_cli_rejects_removed_fork_review_flag() {
        for value in ["true", "false"] {
            let err =
                super::parse_cli_from(["acn", "--config", "config.toml", "--fork-review", value])
                    .unwrap_err()
                    .to_string();

            assert!(err.contains("未知参数: --fork-review"));
        }
    }

    #[test]
    fn parse_cli_accepts_resume_flag() {
        let cli = super::parse_cli_from(["acn", "--config", "config.toml", "--resume"]).unwrap();

        assert_eq!(cli.resume, super::StartupResume::Picker);
    }

    #[test]
    fn parse_cli_accepts_resume_session_id() {
        let cli = super::parse_cli_from([
            "acn",
            "--config",
            "config.toml",
            "--resume",
            "session_1234abcd",
        ])
        .unwrap();

        assert_eq!(
            cli.resume,
            super::StartupResume::Session(SessionId::from_str("session_1234abcd").unwrap())
        );
    }

    #[test]
    fn direct_resume_metadata_failure_allows_consistent_open_closed_and_finalizing_sessions() {
        let agent = AgentId::new("agent-a").unwrap();
        let session_id = SessionId::from_str("session_1234abcd").unwrap();
        let metadata =
            test_session_metadata(agent.clone(), session_id.clone(), SessionStatus::Open);

        assert!(super::direct_resume_metadata_failure(&agent, &session_id, &metadata).is_none());

        let metadata =
            test_session_metadata(agent.clone(), session_id.clone(), SessionStatus::Closed);
        assert!(super::direct_resume_metadata_failure(&agent, &session_id, &metadata).is_none());

        let metadata =
            test_session_metadata(agent.clone(), session_id.clone(), SessionStatus::Finalizing);
        assert!(super::direct_resume_metadata_failure(&agent, &session_id, &metadata).is_none());
        let mut metadata =
            test_session_metadata(agent.clone(), session_id.clone(), SessionStatus::Closed);
        metadata.finalized_at = Some(metadata.updated_at);

        assert!(super::direct_resume_metadata_failure(&agent, &session_id, &metadata).is_none());

        let mut metadata =
            test_session_metadata(agent.clone(), session_id.clone(), SessionStatus::Open);
        metadata.finalized_at = Some(metadata.updated_at);
        let failure = super::direct_resume_metadata_failure(&agent, &session_id, &metadata)
            .expect("open finalized metadata should be rejected");
        assert!(failure.contains("inconsistent Open metadata"));
    }

    #[test]
    fn acn_usage_uses_installed_binary_and_mentions_supervisor() {
        let usage = super::acn_usage();

        assert!(usage.contains("acn [options]"));
        assert!(
            usage.contains("acn update [--url <git-url>] [--branch <branch>] [--config <path>]")
        );
        assert!(usage.contains("从默认仓库的指定远端 branch 更新"));
        assert!(super::update_usage().contains(super::DEFAULT_UPDATE_REPOSITORY_URL));
        assert!(super::update_usage().contains("--url <git-url>"));
        assert!(usage.contains("acn session cleanup"));
        assert!(usage.contains("Dry-run 预览可清理的旧 Closed sessions"));
        assert!(usage.contains("acn session cleanup --apply"));
        assert!(usage.contains("acn supervisor status"));
        assert!(usage.contains("acn supervisor jobs"));
        assert!(usage.contains("acn supervisor retry"));
        assert!(usage.contains("acn supervisor stop"));
        assert!(usage.contains("acn mcp list"));
        assert!(usage.contains("acn mcp list [--upstream <name>]"));
        assert!(usage.contains("acn mcp get <name>"));
        assert!(usage.contains("acn mcp add <name>"));
        assert!(usage.contains("acn mcp add-json <name>"));
        assert!(usage.contains("acn mcp remove <name>"));
        assert!(usage.contains("acn mcp enable <name>"));
        assert!(usage.contains("acn mcp disable <name>"));
        assert!(usage.contains("acn mcp login <name>"));
        assert!(usage.contains("acn mcp logout <name>"));
        assert!(usage.contains("acn mcp status [name]"));
        assert!(!usage.contains("接入 client 后可用"));
        assert!(!usage.contains("--upstream n"));
        assert!(!usage.contains("cargo run"));
    }

    #[test]
    fn session_cleanup_text_aligns_summary_and_entries() {
        let session_id = SessionId::from_str("session_1234abcd").unwrap();
        let deleted_session_id = SessionId::from_str("session_22222222").unwrap();
        let old_skipped_session_id = SessionId::from_str("session_33333333").unwrap();
        let new_skipped_session_id = SessionId::from_str("session_44444444").unwrap();
        let last_activity = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let deleted_activity = DateTime::parse_from_rfc3339("2026-01-20T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let old_skipped_activity = DateTime::parse_from_rfc3339("2026-01-10T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let new_skipped_activity = DateTime::parse_from_rfc3339("2026-01-11T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let report = SessionCleanupReport {
            scanned: 5,
            eligible: 2,
            deleted: 1,
            skipped: 3,
            sqlite_purged: 0,
            errors: 0,
            aborted: false,
            entries: vec![
                SessionCleanupEntry {
                    session_id: Some(new_skipped_session_id.clone()),
                    session_path: std::path::PathBuf::from("/tmp/session_44444444"),
                    outcome: SessionCleanupOutcome::Skipped,
                    reason: "Action exists within cutoff time.".to_string(),
                    last_activity_at: Some(new_skipped_activity),
                    sqlite_purged: false,
                },
                SessionCleanupEntry {
                    session_id: Some(session_id),
                    session_path: std::path::PathBuf::from("/tmp/session_1234abcd"),
                    outcome: SessionCleanupOutcome::DryRun,
                    reason: "last canonical message".to_string(),
                    last_activity_at: Some(last_activity),
                    sqlite_purged: false,
                },
                SessionCleanupEntry {
                    session_id: None,
                    session_path: std::path::PathBuf::from("/tmp/not-a-session"),
                    outcome: SessionCleanupOutcome::Skipped,
                    reason: "invalid session id directory name".to_string(),
                    last_activity_at: None,
                    sqlite_purged: false,
                },
                SessionCleanupEntry {
                    session_id: Some(old_skipped_session_id.clone()),
                    session_path: std::path::PathBuf::from("/tmp/session_33333333"),
                    outcome: SessionCleanupOutcome::Skipped,
                    reason: "Action exists within cutoff time.".to_string(),
                    last_activity_at: Some(old_skipped_activity),
                    sqlite_purged: false,
                },
                SessionCleanupEntry {
                    session_id: Some(deleted_session_id.clone()),
                    session_path: std::path::PathBuf::from("/tmp/session_22222222"),
                    outcome: SessionCleanupOutcome::Deleted,
                    reason: "last canonical message".to_string(),
                    last_activity_at: Some(deleted_activity),
                    sqlite_purged: false,
                },
            ],
        };
        let cutoff = DateTime::parse_from_rfc3339("2026-01-31T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let text = super::session_cleanup_text(
            &report,
            "agent-a",
            std::path::Path::new("/tmp/agent-a"),
            30,
            cutoff,
            false,
        );

        assert!(text.contains("This is a dry run. Use --apply to delete."));
        assert!(text.contains("  sqlite_purged  0\n"));
        assert!(text.contains("  Outcome  Session ID        Last Activity At      "));
        assert!(text.contains("Action exists within cutoff time."));
        assert!(text.contains("Last canonical message."));
        assert!(text.contains("Invalid session id directory name."));
        let deleted_index = text.find("  deleted  session_22222222").unwrap();
        let dry_run_index = text.find("  dry-run  session_1234abcd").unwrap();
        let old_skipped_index = text.find("  skipped  session_33333333").unwrap();
        let new_skipped_index = text.find("  skipped  session_44444444").unwrap();
        let no_time_skipped_index = text.find("  skipped  -").unwrap();
        assert!(deleted_index < dry_run_index);
        assert!(dry_run_index < old_skipped_index);
        assert!(old_skipped_index < new_skipped_index);
        assert!(new_skipped_index < no_time_skipped_index);
        assert!(text.contains("  dry-run  session_1234abcd  2026-01-01T00:00:00"));
        assert!(text.contains("  skipped  -                 -                     "));
    }

    #[test]
    fn parse_mcp_add_stdio_accepts_env_and_command() {
        let cli = super::parse_mcp_cli_from([
            "acn",
            "mcp",
            "--config",
            "config.toml",
            "add",
            "pal",
            "-e",
            "DEFAULT_MODEL=auto",
            "--env-var",
            "OPENAI_API_KEY",
            "--",
            "uvx",
            "--from",
            "git+https://example.com/pal.git",
            "pal-mcp-server",
        ])
        .unwrap();

        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(cli.upstream, None);
        match cli.command {
            super::McpCommand::Add(add) => {
                assert_eq!(add.name, "pal");
                assert_eq!(
                    add.env.get("DEFAULT_MODEL").map(String::as_str),
                    Some("auto")
                );
                assert_eq!(add.env_vars, vec!["OPENAI_API_KEY"]);
                match add.transport {
                    super::McpAddTransport::Stdio { command, args } => {
                        assert_eq!(command, "uvx");
                        assert_eq!(
                            args,
                            vec![
                                "--from".to_string(),
                                "git+https://example.com/pal.git".to_string(),
                                "pal-mcp-server".to_string()
                            ]
                        );
                    }
                    other => panic!("unexpected transport: {other:?}"),
                }
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_mcp_accepts_upstream_common_option() {
        let cli = super::parse_mcp_cli_from([
            "acn",
            "mcp",
            "--config",
            "config.toml",
            "--upstream",
            "agent_hub",
            "list",
        ])
        .unwrap();

        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(cli.upstream.as_deref(), Some("agent_hub"));
        assert!(matches!(cli.command, super::McpCommand::List));
    }

    #[tokio::test]
    async fn mcp_cli_writes_selected_upstream_runtime_config() {
        let dir = tempfile::tempdir().unwrap();
        let acn_home = dir.path().join("acn");
        std::fs::create_dir_all(&acn_home).unwrap();
        std::fs::write(
            acn_home.join(".mcp.json"),
            r#"{"mcpServers":{"legacy":{"url":"https://legacy.example/mcp"}}}"#,
        )
        .unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:8062"
router_endpoint = "http://127.0.0.1:8061"

[upstreams.agent_hub]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:9062"
router_endpoint = "http://127.0.0.1:9061"

[storage]
acn_home = "{}"

[agent.llm]
provider = "anthropic"
endpoint = "https://api.anthropic.com"
model = "test-model"
api_key_env = "PATH"
max_tokens = 4096
context_window = 200000
timeout_secs = 600
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000
"#,
                acn_home.display()
            ),
        )
        .unwrap();

        super::run_mcp_cli(vec![
            "acn".to_string(),
            "mcp".to_string(),
            "--config".to_string(),
            config_path.display().to_string(),
            "--upstream".to_string(),
            "agent_hub".to_string(),
            "add".to_string(),
            "pal".to_string(),
            "--".to_string(),
            "uvx".to_string(),
            "pal-mcp-server".to_string(),
        ])
        .await
        .unwrap();

        let target_path = acn_home.join("agent_hub").join(".mcp.json");
        let cfg = agent_claim_network::mcp::config::read_mcp_json_config(&target_path)
            .await
            .unwrap();
        assert!(cfg.servers.contains_key("pal"));
        assert!(cfg.servers.contains_key("legacy"));
        assert!(!acn_home.join(".mcp.json").exists());
        assert!(!acn_home.join("dev").join(".mcp.json").exists());
    }

    #[test]
    fn parse_mcp_add_http_rejects_bearer_with_oauth_options() {
        let error = super::parse_mcp_cli_from([
            "acn",
            "mcp",
            "add",
            "linear",
            "--url",
            "https://mcp.linear.app/mcp",
            "--bearer-token-env-var",
            "LINEAR_API_KEY",
            "--oauth-client-id",
            "linear-public-client",
            "--oauth-callback-port",
            "8765",
            "--oauth-credentials-store",
            "file",
        ])
        .unwrap_err()
        .to_string();

        assert!(error.contains("不能与 OAuth 选项同时使用"));
    }

    #[test]
    fn parse_mcp_login_and_logout_accept_common_options() {
        let login = super::parse_mcp_cli_from([
            "acn",
            "mcp",
            "login",
            "remote",
            "--config",
            "config.toml",
            "--upstream",
            "agent_hub",
            "--no-browser",
        ])
        .unwrap();
        let logout = super::parse_mcp_cli_from(["acn", "mcp", "logout", "remote"]).unwrap();

        assert_eq!(login.upstream.as_deref(), Some("agent_hub"));
        assert_eq!(
            login.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert!(matches!(
            login.command,
            super::McpCommand::Login {
                ref name,
                no_browser: true,
            } if name == "remote"
        ));
        assert!(matches!(
            logout.command,
            super::McpCommand::Logout { ref name } if name == "remote"
        ));
    }

    #[tokio::test]
    async fn mcp_login_rejects_stdio_server_before_starting_browser_flow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = agent_claim_network::mcp::config::McpJsonConfig::default();
        cfg.servers.insert(
            "local".to_string(),
            agent_claim_network::mcp::config::McpServerConfig::stdio(
                "server".to_string(),
                Vec::new(),
                std::collections::BTreeMap::new(),
                Vec::new(),
            ),
        );
        agent_claim_network::mcp::config::write_mcp_json_config_atomic(&path, &cfg)
            .await
            .unwrap();

        let error = super::execute_mcp_command(
            &path,
            super::McpCommand::Login {
                name: "local".to_string(),
                no_browser: false,
            },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("仅 streamable_http server 可登录"));
    }

    #[tokio::test]
    async fn mcp_login_rejects_bearer_server_before_starting_browser_flow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = agent_claim_network::mcp::config::McpJsonConfig::default();
        cfg.servers.insert(
            "remote".to_string(),
            agent_claim_network::mcp::config::McpServerConfig::streamable_http(
                "https://example.test/mcp".to_string(),
                Some("SERVICE_API_KEY".to_string()),
            ),
        );
        agent_claim_network::mcp::config::write_mcp_json_config_atomic(&path, &cfg)
            .await
            .unwrap();

        let error = super::execute_mcp_command(
            &path,
            super::McpCommand::Login {
                name: "remote".to_string(),
                no_browser: false,
            },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("使用 bearer_token_env_var"));
    }

    #[test]
    fn parse_mcp_add_json_accepts_all_existing_stdio_fields() {
        let cli = super::parse_mcp_cli_from([
            "acn",
            "mcp",
            "add-json",
            "pal",
            r#"{
                "type": "stdio",
                "enabled": false,
                "startup_timeout_secs": 30,
                "tool_timeout_secs": 90,
                "enabled_tools": ["chat"],
                "disabled_tools": ["delete"],
                "command": "uvx",
                "args": ["pal-mcp-server"],
                "env": {"DEFAULT_MODEL": "auto"},
                "env_vars": ["OPENAI_API_KEY"],
                "cwd": "/tmp"
            }"#,
            "--config",
            "config.toml",
            "--upstream",
            "agent_hub",
        ])
        .unwrap();

        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(cli.upstream.as_deref(), Some("agent_hub"));
        match cli.command {
            super::McpCommand::AddJson { name, server } => {
                assert_eq!(name, "pal");
                assert_eq!(
                    server.transport,
                    Some(agent_claim_network::mcp::config::McpTransportKind::Stdio)
                );
                assert_eq!(server.enabled, Some(false));
                assert_eq!(server.startup_timeout_secs, Some(30));
                assert_eq!(server.tool_timeout_secs, Some(90));
                assert_eq!(server.enabled_tools, Some(vec!["chat".to_string()]));
                assert_eq!(server.disabled_tools, Some(vec!["delete".to_string()]));
                assert_eq!(server.command.as_deref(), Some("uvx"));
                assert_eq!(server.args, Some(vec!["pal-mcp-server".to_string()]));
                assert_eq!(
                    server
                        .env
                        .as_ref()
                        .and_then(|env| env.get("DEFAULT_MODEL"))
                        .map(String::as_str),
                    Some("auto")
                );
                assert_eq!(server.env_vars, Some(vec!["OPENAI_API_KEY".to_string()]));
                assert_eq!(server.cwd.as_deref(), Some(std::path::Path::new("/tmp")));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn parse_mcp_add_json_rejects_missing_or_unsupported_config() {
        let cases = [
            (r#"{"mcpServers": {}}"#, "unknown field `mcpServers`"),
            (
                r#"{"type":"sse","url":"https://example.com/sse"}"#,
                "unknown variant `sse`",
            ),
            (r#"{"type":"stdio"}"#, "缺少 command"),
            (r#"{"type":"streamable_http"}"#, "缺少 url"),
            (
                r#"{"type":"streamable_http","url":"https://example.com/mcp","headers":{}}"#,
                "unknown field `headers`",
            ),
            (
                r#"{"type":"streamable_http","url":"https://example.com/mcp","oauth":{}}"#,
                "unknown field `oauth`",
            ),
            (
                r#"{"type":"stdio","command":"server","startup_timeout_secs":0}"#,
                "startup_timeout_secs 必须大于 0",
            ),
        ];

        for (json, expected) in cases {
            let error =
                super::parse_mcp_cli_from(["acn", "mcp", "add-json", "server", json]).unwrap_err();
            let message = format!("{error:#}");
            assert!(
                message.contains(expected),
                "unexpected error for {json}: {message}"
            );
        }

        let missing = super::parse_mcp_cli_from(["acn", "mcp", "add-json", "server"])
            .unwrap_err()
            .to_string();
        assert!(missing.contains("缺少 server JSON"));
    }

    #[tokio::test]
    async fn execute_mcp_add_json_normalizes_http_alias_and_rejects_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let args = [
            "acn",
            "mcp",
            "add-json",
            "linear",
            r#"{
                "type": "http",
                "url": "https://mcp.linear.app/mcp",
                "bearer_token_env_var": "LINEAR_API_KEY",
                "enabled": true,
                "tool_timeout_secs": 45
            }"#,
        ];

        let first = super::parse_mcp_cli_from(args).unwrap();
        let output = super::execute_mcp_command(&path, first.command)
            .await
            .unwrap();
        assert!(output.contains("added MCP server 'linear'"));

        let raw = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(raw.contains(r#""type": "streamable_http""#));
        assert!(!raw.contains(r#""type": "http""#));
        let cfg = agent_claim_network::mcp::config::read_mcp_json_config(&path)
            .await
            .unwrap();
        let linear = &cfg.servers["linear"];
        assert_eq!(linear.url.as_deref(), Some("https://mcp.linear.app/mcp"));
        assert_eq!(
            linear.bearer_token_env_var.as_deref(),
            Some("LINEAR_API_KEY")
        );
        assert_eq!(linear.enabled, Some(true));
        assert_eq!(linear.tool_timeout_secs, Some(45));

        let duplicate = super::parse_mcp_cli_from(args).unwrap();
        let error = super::execute_mcp_command(&path, duplicate.command)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("MCP server 已存在: linear；请先 remove 后再 add"));
    }

    #[test]
    fn parse_mcp_rejects_invalid_server_name() {
        let err = super::parse_mcp_cli_from(["acn", "mcp", "add", "bad.name", "--", "server"])
            .unwrap_err()
            .to_string();

        assert!(err.contains("MCP server name 无效"));
    }

    #[tokio::test]
    async fn execute_mcp_command_round_trips_add_get_disable_enable_remove() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        super::execute_mcp_command(
            &path,
            super::McpCommand::Add(super::McpAddCommand {
                name: "pal".to_string(),
                transport: super::McpAddTransport::Stdio {
                    command: "uvx".to_string(),
                    args: vec!["pal-mcp-server".to_string()],
                },
                env: std::collections::BTreeMap::from([(
                    "DEFAULT_MODEL".to_string(),
                    "auto".to_string(),
                )]),
                env_vars: vec!["OPENAI_API_KEY".to_string()],
            }),
        )
        .await
        .unwrap();

        let list = super::execute_mcp_command(&path, super::McpCommand::List)
            .await
            .unwrap();
        assert!(list.contains("pal"));
        assert!(list.contains("stdio"));

        let get = super::execute_mcp_command(
            &path,
            super::McpCommand::Get {
                name: "pal".to_string(),
                json: false,
            },
        )
        .await
        .unwrap();
        assert!(get.contains("env keys: DEFAULT_MODEL"));
        assert!(!get.contains("auto"));

        super::execute_mcp_command(
            &path,
            super::McpCommand::Disable {
                name: "pal".to_string(),
            },
        )
        .await
        .unwrap();
        let cfg = agent_claim_network::mcp::config::read_mcp_json_config(&path)
            .await
            .unwrap();
        assert_eq!(cfg.servers["pal"].enabled, Some(false));
        assert_eq!(cfg.servers["pal"].command.as_deref(), Some("uvx"));

        super::execute_mcp_command(
            &path,
            super::McpCommand::Enable {
                name: "pal".to_string(),
            },
        )
        .await
        .unwrap();
        let cfg = agent_claim_network::mcp::config::read_mcp_json_config(&path)
            .await
            .unwrap();
        assert_eq!(cfg.servers["pal"].enabled, Some(true));

        super::execute_mcp_command(
            &path,
            super::McpCommand::Remove {
                name: "pal".to_string(),
            },
        )
        .await
        .unwrap();
        let cfg = agent_claim_network::mcp::config::read_mcp_json_config(&path)
            .await
            .unwrap();
        assert!(cfg.servers.is_empty());
    }

    #[tokio::test]
    async fn mcp_remove_deletes_config_and_keeps_failed_oauth_cleanup_retryable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut server = agent_claim_network::mcp::config::McpServerConfig::streamable_http(
            "https://example.test/mcp".to_string(),
            None,
        );
        server.oauth_credentials_store =
            Some(agent_claim_network::mcp::config::McpOAuthCredentialsStore::File);
        let mut cfg = agent_claim_network::mcp::config::McpJsonConfig::default();
        cfg.servers.insert("remote".to_string(), server);
        agent_claim_network::mcp::config::write_mcp_json_config_atomic(&path, &cfg)
            .await
            .unwrap();
        tokio::fs::write(dir.path().join(".mcp-oauth"), "not a directory")
            .await
            .unwrap();

        let output = super::execute_mcp_command(
            &path,
            super::McpCommand::Remove {
                name: "remote".to_string(),
            },
        )
        .await
        .unwrap();

        assert!(output.contains("removed MCP server 'remote'"));
        assert!(output.contains("OAuth 凭据清理失败"));
        let cfg = agent_claim_network::mcp::config::read_mcp_json_config(&path)
            .await
            .unwrap();
        assert!(cfg.servers.is_empty());

        let blocked_add = super::execute_mcp_command(
            &path,
            super::McpCommand::AddJson {
                name: "remote".to_string(),
                server: agent_claim_network::mcp::config::McpServerConfig::stdio(
                    "server".to_string(),
                    Vec::new(),
                    std::collections::BTreeMap::new(),
                    Vec::new(),
                ),
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(blocked_add.contains("仍有待清理"));

        tokio::fs::remove_file(dir.path().join(".mcp-oauth"))
            .await
            .unwrap();
        let retry = super::execute_mcp_command(
            &path,
            super::McpCommand::Logout {
                name: "remote".to_string(),
            },
        )
        .await
        .unwrap();
        assert!(retry.contains("logged out of MCP server 'remote'"));
        let retry_again = super::execute_mcp_command(
            &path,
            super::McpCommand::Logout {
                name: "remote".to_string(),
            },
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(retry_again.contains("MCP server 不存在: remote"));
    }

    #[tokio::test]
    async fn mcp_remove_preserves_config_written_while_waiting_for_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut remote = agent_claim_network::mcp::config::McpServerConfig::streamable_http(
            "https://example.test/mcp".to_string(),
            None,
        );
        remote.oauth_credentials_store =
            Some(agent_claim_network::mcp::config::McpOAuthCredentialsStore::File);
        let mut cfg = agent_claim_network::mcp::config::McpJsonConfig::default();
        cfg.servers.insert("remote".to_string(), remote.clone());
        agent_claim_network::mcp::config::write_mcp_json_config_atomic(&path, &cfg)
            .await
            .unwrap();

        let credential_blocker = agent_claim_network::mcp::oauth::prepare_credentials_for_remove(
            &path, "remote", &remote,
        )
        .await
        .unwrap();
        let remove_path = path.clone();
        let remove = tokio::spawn(async move {
            super::execute_mcp_command(
                &remove_path,
                super::McpCommand::Remove {
                    name: "remote".to_string(),
                },
            )
            .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        super::execute_mcp_command(
            &path,
            super::McpCommand::AddJson {
                name: "other".to_string(),
                server: agent_claim_network::mcp::config::McpServerConfig::stdio(
                    "server".to_string(),
                    Vec::new(),
                    std::collections::BTreeMap::new(),
                    Vec::new(),
                ),
            },
        )
        .await
        .unwrap();
        credential_blocker.finish().await.unwrap();
        remove.await.unwrap().unwrap();

        let cfg = agent_claim_network::mcp::config::read_mcp_json_config(&path)
            .await
            .unwrap();
        assert!(!cfg.servers.contains_key("remote"));
        assert!(cfg.servers.contains_key("other"));
    }

    #[tokio::test]
    async fn execute_mcp_status_reports_disabled_without_starting_server() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        super::execute_mcp_command(
            &path,
            super::McpCommand::Add(super::McpAddCommand {
                name: "off".to_string(),
                transport: super::McpAddTransport::Stdio {
                    command: "definitely-not-a-real-mcp-command".to_string(),
                    args: Vec::new(),
                },
                env: std::collections::BTreeMap::new(),
                env_vars: Vec::new(),
            }),
        )
        .await
        .unwrap();
        super::execute_mcp_command(
            &path,
            super::McpCommand::Disable {
                name: "off".to_string(),
            },
        )
        .await
        .unwrap();

        let status = super::execute_mcp_command(
            &path,
            super::McpCommand::Status {
                name: Some("off".to_string()),
            },
        )
        .await
        .unwrap();

        assert!(status.contains("off"));
        assert!(status.contains("disabled"));
        assert!(!status.contains("failed"));
    }

    #[tokio::test]
    async fn execute_mcp_status_name_does_not_start_other_enabled_servers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let marker = dir.path().join("spawned_marker");
        let script_path = dir.path().join("marker_server.sh");
        tokio::fs::write(
            &script_path,
            format!("touch '{}'\nexit 1\n", marker.display()),
        )
        .await
        .unwrap();
        let mut cfg = agent_claim_network::mcp::config::McpJsonConfig::default();
        let mut target = agent_claim_network::mcp::config::McpServerConfig::streamable_http(
            "https://example.test/mcp".into(),
            None,
        );
        target.enabled = Some(false);
        cfg.servers.insert("pal".into(), target);
        cfg.servers.insert(
            "other".into(),
            agent_claim_network::mcp::config::McpServerConfig::stdio(
                "sh".into(),
                vec![script_path.display().to_string()],
                std::collections::BTreeMap::new(),
                Vec::new(),
            ),
        );
        agent_claim_network::mcp::config::write_mcp_json_config_atomic(&path, &cfg)
            .await
            .unwrap();

        let status = super::execute_mcp_command(
            &path,
            super::McpCommand::Status {
                name: Some("pal".to_string()),
            },
        )
        .await
        .unwrap();

        assert!(status.contains("pal"));
        assert!(status.contains("disabled"));
        assert!(!marker.exists());
    }

    #[test]
    fn mcp_cli_redacts_sensitive_output_fields() {
        assert_eq!(
            super::redact_cli_text("OPENAI_API_KEY=secret"),
            "OPENAI_API_KEY=<redacted>"
        );
        assert_eq!(
            super::redact_cli_text("https://user:pass@example.test/mcp?token=abc#frag"),
            "https://<redacted>@example.test/mcp?<redacted>"
        );
        assert_eq!(
            super::redact_cli_text("url=\"https://user:pass@example.test/mcp?token=abc\""),
            "url=\"https://<redacted>@example.test/mcp?<redacted>\""
        );
        assert_eq!(
            super::redact_cli_text("token: sk-test"),
            "token: <redacted>"
        );
        assert_eq!(
            super::redact_cli_text("X-API-Key: sk-test"),
            "X-API-Key: <redacted>"
        );
        assert_eq!(
            super::redact_cli_text("Authorization: Bearer abc"),
            "<redacted>"
        );
        assert_eq!(
            super::redact_cli_text("HTTPS://user:pass@example.test/mcp?token=abc"),
            "HTTPS://<redacted>@example.test/mcp?<redacted>"
        );
        assert_eq!(
            super::format_arg_list(&["--api-key".to_string(), "secret-value".to_string()]),
            "--api-key,<redacted>"
        );
        assert_eq!(super::redact_cli_text("--model=auto"), "--model=auto");
    }

    #[tokio::test]
    async fn mcp_get_json_redacts_sensitive_config_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = agent_claim_network::mcp::config::McpJsonConfig::default();
        cfg.servers.insert(
            "pal".to_string(),
            agent_claim_network::mcp::config::McpServerConfig::stdio(
                "uvx".to_string(),
                vec![
                    "--api-key".to_string(),
                    "secret-value".to_string(),
                    "--endpoint=HTTPS://user:pass@example.test/mcp?token=abc".to_string(),
                ],
                std::collections::BTreeMap::from([(
                    "OPENAI_API_KEY".to_string(),
                    "secret-value".to_string(),
                )]),
                vec!["OPENAI_BASE_URL".to_string()],
            ),
        );
        agent_claim_network::mcp::config::write_mcp_json_config_atomic(&path, &cfg)
            .await
            .unwrap();

        let text = super::execute_mcp_command(
            &path,
            super::McpCommand::Get {
                name: "pal".to_string(),
                json: true,
            },
        )
        .await
        .unwrap();

        assert!(text.contains("<redacted>"));
        assert!(text.contains("OPENAI_API_KEY"));
        assert!(text.contains("OPENAI_BASE_URL"));
        assert!(!text.contains("secret-value"));
        assert!(!text.contains("user:pass"));
        assert!(!text.contains("token=abc"));
    }

    #[test]
    fn supervisor_usage_describes_management_commands() {
        let usage = super::supervisor_usage();

        assert!(usage.contains("acn supervisor status"));
        assert!(usage.contains("acn supervisor jobs"));
        assert!(usage.contains("acn supervisor stop"));
        assert!(usage.contains("run` 是 ACN 自动拉起"));
    }

    #[test]
    fn parse_supervisor_cli_accepts_run_flags() {
        let cli = super::parse_supervisor_cli_from([
            "acn",
            "supervisor",
            "run",
            "--config",
            "config.toml",
            "--upstream",
            "agent_hub",
            "--runtime-fingerprint",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ])
        .unwrap();

        assert_eq!(cli.command, super::SupervisorCommand::Run);
        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(cli.upstream.as_deref(), Some("agent_hub"));
        assert_eq!(cli.retry_target, None);
        assert_eq!(
            cli.expected_runtime_fingerprint.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
    }

    #[test]
    fn parse_supervisor_cli_accepts_retry_session_or_job_with_config() {
        let session = super::parse_supervisor_cli_from([
            "acn",
            "supervisor",
            "retry",
            "--config",
            "config.toml",
            "session_1234abcd",
            "--upstream",
            "agent_hub",
        ])
        .unwrap();
        let job =
            super::parse_supervisor_cli_from(["acn", "supervisor", "retry", "job_123_abcdef01"])
                .unwrap();

        assert_eq!(session.command, super::SupervisorCommand::Retry);
        assert_eq!(
            session.retry_target,
            Some(
                agent_claim_network::supervisor::SupervisorRetryTarget::Session {
                    session_id: "session_1234abcd".parse().unwrap(),
                }
            )
        );
        assert_eq!(
            session.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
        assert_eq!(session.upstream.as_deref(), Some("agent_hub"));
        assert_eq!(
            job.retry_target,
            Some(
                agent_claim_network::supervisor::SupervisorRetryTarget::Job {
                    job_id: "job_123_abcdef01".to_string(),
                }
            )
        );
    }

    #[test]
    fn parse_supervisor_cli_rejects_missing_invalid_extra_retry_target_and_cd() {
        let missing = super::parse_supervisor_cli_from(["acn", "supervisor", "retry"]).unwrap_err();
        let invalid =
            super::parse_supervisor_cli_from(["acn", "supervisor", "retry", "session_bad"])
                .unwrap_err();
        let extra = super::parse_supervisor_cli_from([
            "acn",
            "supervisor",
            "retry",
            "session_1234abcd",
            "job_123",
        ])
        .unwrap_err();
        let cd = super::parse_supervisor_cli_from(["acn", "supervisor", "run", "--cd", "."])
            .unwrap_err();

        assert!(missing.to_string().contains("缺少 session_id 或 job_id"));
        assert!(invalid
            .to_string()
            .contains("无效的 supervisor retry session_id"));
        assert!(extra.to_string().contains("只接受一个"));
        assert!(cd.to_string().contains("未知 supervisor 参数: --cd"));
    }

    #[test]
    fn parse_supervisor_cli_rejects_invalid_or_management_runtime_fingerprint() {
        let invalid = super::parse_supervisor_cli_from([
            "acn",
            "supervisor",
            "run",
            "--runtime-fingerprint",
            "not-a-digest",
        ])
        .unwrap_err()
        .to_string();
        let status = super::parse_supervisor_cli_from([
            "acn",
            "supervisor",
            "status",
            "--runtime-fingerprint",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ])
        .unwrap_err()
        .to_string();

        assert!(invalid.contains("64 位小写十六进制"));
        assert!(status.contains("仅支持 acn supervisor run"));
    }

    #[test]
    fn parse_supervisor_cli_accepts_management_subcommands() {
        let status = super::parse_supervisor_cli_from(["acn", "supervisor", "status"]).unwrap();
        let jobs = super::parse_supervisor_cli_from(["acn", "supervisor", "jobs"]).unwrap();
        let stop = super::parse_supervisor_cli_from(["acn", "supervisor", "stop"]).unwrap();

        assert_eq!(status.command, super::SupervisorCommand::Status);
        assert_eq!(
            jobs.command,
            super::SupervisorCommand::Jobs {
                limit: super::DEFAULT_SUPERVISOR_JOBS_LIMIT
            }
        );
        assert_eq!(stop.command, super::SupervisorCommand::Stop);
    }

    #[test]
    fn parse_supervisor_cli_accepts_jobs_limit() {
        let jobs =
            super::parse_supervisor_cli_from(["acn", "supervisor", "jobs", "-l", "10"]).unwrap();
        let all = super::parse_supervisor_cli_from(["acn", "supervisor", "jobs", "--limit", "0"])
            .unwrap();

        assert_eq!(jobs.command, super::SupervisorCommand::Jobs { limit: 10 });
        assert_eq!(all.command, super::SupervisorCommand::Jobs { limit: 0 });
    }

    #[test]
    fn parse_supervisor_cli_rejects_jobs_limit_on_other_commands() {
        let err = super::parse_supervisor_cli_from(["acn", "supervisor", "status", "-l", "10"])
            .unwrap_err()
            .to_string();

        assert!(err.contains("-l 仅支持 acn supervisor jobs"));
    }

    #[test]
    fn parse_supervisor_cli_rejects_unknown_subcommand() {
        let err = super::parse_supervisor_cli_from(["acn", "supervisor", "restart"])
            .unwrap_err()
            .to_string();

        assert!(err.contains("未知 supervisor 子命令: restart"));
    }

    #[test]
    fn supervisor_status_text_labels_stale_current_when_stopped() {
        let now = test_time();
        let status = SupervisorStatusSnapshot {
            runtime_state: SupervisorRuntimeState::Stopped,
            pid: Some(42),
            started_at: None,
            build: None,
            runtime_fingerprint: None,
            queue: SupervisorQueueSummary {
                total: 1,
                queued: 0,
                running: 1,
                succeeded: 0,
                failed: 0,
            },
            current_job: Some(SupervisorJobView {
                id: "job_1".to_string(),
                agent_id: Some(AgentId::new("agent-a").unwrap()),
                kind: "finalize".to_string(),
                session_id: SessionId::from_str("session_1234abcd").unwrap(),
                recap_end_index: None,
                status: "running".to_string(),
                created_at: now,
                started_at: Some(now),
                finished_at: None,
                attempts: 2,
                manual_retries: 1,
                last_error: None,
            }),
            socket_path: "/tmp/acn.sock".into(),
            pid_path: "/tmp/acn.pid".into(),
        };

        let text =
            super::supervisor_status_text(&status, "agent-a", std::path::Path::new("/tmp/acn"));

        assert!(text.contains("supervisor: stopped"));
        assert!(text.contains("pid: 42 (stale)"));
        assert!(text.contains("stale_current: job_1"));
        assert!(!text.contains("\ncurrent: job_1"));
    }

    #[test]
    fn supervisor_status_text_reports_stuck_with_manual_recovery_hint() {
        let now = test_time();
        let status = SupervisorStatusSnapshot {
            runtime_state: SupervisorRuntimeState::Stuck {
                ipc_error: "supervisor IPC 超时".to_string(),
            },
            pid: Some(42),
            started_at: None,
            build: None,
            runtime_fingerprint: None,
            queue: SupervisorQueueSummary {
                total: 1,
                queued: 0,
                running: 1,
                succeeded: 0,
                failed: 0,
            },
            current_job: Some(SupervisorJobView {
                id: "job_1".to_string(),
                agent_id: Some(AgentId::new("agent-a").unwrap()),
                kind: "finalize".to_string(),
                session_id: SessionId::from_str("session_1234abcd").unwrap(),
                recap_end_index: None,
                status: "running".to_string(),
                created_at: now,
                started_at: Some(now),
                finished_at: None,
                attempts: 2,
                manual_retries: 1,
                last_error: None,
            }),
            socket_path: "/tmp/acn.sock".into(),
            pid_path: "/tmp/acn.pid".into(),
        };

        let text =
            super::supervisor_status_text(&status, "agent-a", std::path::Path::new("/tmp/acn"));

        assert!(text.contains("supervisor: stuck"));
        assert!(text.contains("pid: 42 (ipc unresponsive)"));
        assert!(text.contains("socket: /tmp/acn.sock"));
        assert!(text.contains("pid_file: /tmp/acn.pid"));
        assert!(text.contains("ipc_error: supervisor IPC 超时"));
        assert!(text.contains("first try `acn supervisor stop`"));
        assert!(text.contains("kill -9 42"));
        assert!(text.contains("stuck_current: job_1"));
    }

    #[test]
    fn supervisor_jobs_text_sanitizes_and_aligns_table_fields() {
        let now = test_time();
        let jobs = [SupervisorJobView {
            id: "job_1".to_string(),
            agent_id: Some(AgentId::new("agent-a").unwrap()),
            kind: "recap".to_string(),
            session_id: SessionId::from_str("session_1234abcd").unwrap(),
            recap_end_index: Some(42),
            status: "failed".to_string(),
            created_at: now,
            started_at: Some(now),
            finished_at: Some(now),
            attempts: 3,
            manual_retries: 2,
            last_error: Some("bad\tline\nagain".to_string()),
        }];

        let text = super::supervisor_jobs_text(&jobs, 0);

        assert!(text.contains("bad line again"));
        assert!(!text.contains('\t'));
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].find("agent_id"), lines[2].find("agent-a"));
        assert_eq!(
            lines[0].find("session_id"),
            lines[2].find("session_1234abcd")
        );
        assert_eq!(lines[0].find("last_error"), lines[2].find("bad line again"));
    }

    #[test]
    fn supervisor_jobs_text_limits_to_recent_jobs_by_default() {
        let now = test_time();
        let jobs = (0..7)
            .map(|idx| SupervisorJobView {
                id: format!("job_{idx}"),
                agent_id: Some(AgentId::new("agent-a").unwrap()),
                kind: "finalize".to_string(),
                session_id: SessionId::from_str("session_1234abcd").unwrap(),
                recap_end_index: None,
                status: "succeeded".to_string(),
                created_at: now + chrono::Duration::seconds(i64::from(idx)),
                started_at: None,
                finished_at: None,
                attempts: 1,
                manual_retries: 0,
                last_error: None,
            })
            .collect::<Vec<_>>();

        let text = super::supervisor_jobs_text(&jobs, super::DEFAULT_SUPERVISOR_JOBS_LIMIT);

        assert!(!text.contains("job_0"));
        assert!(!text.contains("job_1"));
        assert!(text.contains("job_2"));
        assert!(text.contains("job_6"));
        assert!(
            text.find("job_6").unwrap() < text.find("job_2").unwrap(),
            "recent jobs should be listed newest first:\n{text}"
        );
        assert_eq!(
            text.lines().count(),
            2 + super::DEFAULT_SUPERVISOR_JOBS_LIMIT
        );
    }

    #[test]
    fn supervisor_jobs_text_limit_zero_shows_all_jobs() {
        let now = test_time();
        let jobs = (0..7)
            .map(|idx| SupervisorJobView {
                id: format!("job_{idx}"),
                agent_id: Some(AgentId::new("agent-a").unwrap()),
                kind: "finalize".to_string(),
                session_id: SessionId::from_str("session_1234abcd").unwrap(),
                recap_end_index: None,
                status: "succeeded".to_string(),
                created_at: now + chrono::Duration::seconds(i64::from(idx)),
                started_at: None,
                finished_at: None,
                attempts: 1,
                manual_retries: 0,
                last_error: None,
            })
            .collect::<Vec<_>>();

        let text = super::supervisor_jobs_text(&jobs, 0);

        assert!(text.contains("job_0"));
        assert!(text.contains("job_6"));
        assert!(
            text.find("job_6").unwrap() < text.find("job_0").unwrap(),
            "limit=0 should still list newest jobs first:\n{text}"
        );
        assert_eq!(text.lines().count(), 2 + jobs.len());
    }

    #[test]
    fn supervisor_stop_report_text_uses_neutral_shutdown_message() {
        let report = SupervisorStopReport {
            was_running: true,
            stopped: false,
            pid: Some(42),
        };

        let text = super::supervisor_stop_report_text(&report);

        assert!(text.contains("supervisor still shutting down"));
        assert!(!text.contains("current job"));
    }

    #[test]
    fn supervisor_retry_output_is_identical_after_session_or_job_resolution() {
        let report = agent_claim_network::supervisor::SupervisorRetryReport {
            session_id: "session_1234abcd".parse().unwrap(),
            job_id: "job_123_abcdef01".to_string(),
            previous_attempts: 5,
            manual_retries: 2,
        };

        let from_session = super::supervisor_retry_report_text(&report);
        let from_job = super::supervisor_retry_report_text(&report);

        assert_eq!(from_session, from_job);
        assert!(from_session.contains("session_id: session_1234abcd"));
        assert!(from_session.contains("job_id: job_123_abcdef01"));
        assert!(from_session.contains("attempts: 5 -> 0"));
        assert!(from_session.contains("manual_retries: 2"));
    }

    #[test]
    fn parse_cli_rejects_old_agent_flag() {
        let err = super::parse_cli_from(["acn", "--agent", "agent-a"])
            .unwrap_err()
            .to_string();

        assert!(err.contains("未知参数: --agent"));
    }

    #[test]
    fn parse_cli_rejects_old_id_flag() {
        let err = super::parse_cli_from(["acn", "--id", "agent-a"])
            .unwrap_err()
            .to_string();

        assert!(err.contains("未知参数: --id"));
    }

    fn test_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-06-25T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }
}
