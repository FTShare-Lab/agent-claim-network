//! Router daemon 入口。

use std::path::PathBuf;

use anyhow::Context;
use tokio_util::sync::CancellationToken;

use agent_claim_network::auth::AuthVerifier;
use agent_claim_network::bootstrap;
use agent_claim_network::build_info;
use agent_claim_network::config::Config;
use agent_claim_network::router::server;
use agent_claim_network::storage::paths;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let raw_args = std::env::args().collect::<Vec<_>>();
    if build_info::version_requested(&raw_args) {
        println!("{}", build_info::version_text("acn-router"));
        return Ok(());
    }
    init_logging();
    let cli = parse_cli_from(raw_args.into_iter().skip(1))?;
    let (cfg, _cfg_path) = Config::load_or_init_for_router(cli.config.as_deref())
        .with_context(|| format!("加载 config: {:?}", cli.config))?;
    let router = bootstrap::build_router_service(&cfg);
    let cancel = CancellationToken::new();
    let refresh_worker =
        bootstrap::spawn_router_refresh_worker(&cfg, router.clone(), cancel.clone());
    let worker = bootstrap::maybe_spawn_router_vector_worker(&cfg, cancel.clone());
    let auth = AuthVerifier::from_key_store_path(
        &paths::team_store_auth_keys_path(&cfg.storage.team_root),
        cfg.router.auth.team.enabled,
    )
    .await?;
    let serve_result = server::serve(router, &cfg.router.daemon.listen, auth).await;
    cancel.cancel();
    refresh_worker.await??;
    if let Some(worker) = worker {
        worker.await??;
    }
    serve_result
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RouterCli {
    config: Option<PathBuf>,
}

fn parse_cli_from<I, S>(args: I) -> anyhow::Result<RouterCli>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut config = None;
    let mut args = args.into_iter().map(Into::into);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = Some(PathBuf::from(args.next().context("--config 后缺少路径")?)),
            "-h" | "--help" => {
                eprintln!(
                    "用法: acn-router [--config <path>]\n\n默认读取 <acn_home>/config.toml，不存在则自动生成；也可用 ACN_CONFIG 或 --config 指定。\n\n选项:\n  --config <path>  指定 config.toml\n  --version, -V    显示版本号、构建提交和提交时间\n  -h, --help       显示帮助"
                );
                std::process::exit(0);
            }
            other => anyhow::bail!("未知参数: {other}"),
        }
    }
    Ok(RouterCli { config })
}

fn init_logging() {
    let _ = ftlog::builder()
        .max_log_level(log::LevelFilter::Info)
        .root(std::io::stderr())
        .utc()
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_router_cli_accepts_config() {
        let cli = parse_cli_from(["--config", "config.toml"]).unwrap();

        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
    }

    #[test]
    fn router_version_uses_shared_build_metadata() {
        assert_eq!(
            build_info::version_text("acn-router"),
            format!(
                "acn-router {} ({}, {})",
                env!("CARGO_PKG_VERSION"),
                env!("ACN_GIT_COMMIT"),
                env!("ACN_GIT_COMMIT_TIMESTAMP")
            )
        );
    }
}
