//! Maintainer daemon 入口。

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;

use agent_claim_network::bootstrap;
use agent_claim_network::build_info;
use agent_claim_network::config::Config;
use agent_claim_network::maintainer::server;
use agent_claim_network::time::now_seconds;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let raw_args = std::env::args().collect::<Vec<_>>();
    if build_info::version_requested(&raw_args) {
        println!("{}", build_info::version_text("acn-maintainer"));
        return Ok(());
    }
    init_logging();
    let cli = parse_cli_from(raw_args.into_iter().skip(1))?;
    let (cfg, _cfg_path) = Config::load_or_init_for_maintainer_daemon(cli.config.as_deref())
        .with_context(|| format!("加载 config: {:?}", cli.config))?;
    let maintainer = bootstrap::build_maintainer_service(&cfg);
    let ticker_maintainer = maintainer.clone();
    let interval = Duration::from_secs(cfg.maintainer.sweep.tick_interval_secs);
    let sweep_scheduler = server::SweepScheduler::new(cfg.maintainer.sweep.tick_interval_secs);
    let ticker_scheduler = sweep_scheduler.clone();
    let ticker = tokio::spawn(async move {
        let triggered_at = now_seconds();
        if let Err(err) = ticker_maintainer
            .run_stale_sweep_with_trigger(triggered_at, "maintainer_startup")
            .await
        {
            log::warn!(target: "maintainer_bin", "启动 stale sweep 失败: {err:#}");
        } else {
            ticker_scheduler
                .mark_auto_sweep_finished(triggered_at, "maintainer_startup")
                .await;
        }
        loop {
            ticker_scheduler
                .mark_next_after(now_seconds(), interval)
                .await;
            tokio::time::sleep(interval).await;
            let triggered_at = now_seconds();
            if let Err(err) = ticker_maintainer
                .run_stale_sweep_with_trigger(triggered_at, "ticker")
                .await
            {
                log::warn!(target: "maintainer_bin", "stale sweep 失败: {err:#}");
            } else {
                ticker_scheduler
                    .mark_auto_sweep_finished(triggered_at, "ticker")
                    .await;
            }
        }
    });
    let server_result = server::serve(maintainer, &cfg, sweep_scheduler).await;
    ticker.abort();
    server_result
}

#[derive(Debug, Default, PartialEq, Eq)]
struct MaintainerCli {
    config: Option<PathBuf>,
}

fn parse_cli_from<I, S>(args: I) -> anyhow::Result<MaintainerCli>
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
                    "用法: acn-maintainer [--config <path>]\n\n默认读取 <acn_home>/config.toml，不存在则自动生成；也可用 ACN_CONFIG 或 --config 指定。\n\n选项:\n  --config <path>  指定 config.toml\n  --version, -V    显示版本号、构建提交和提交时间\n  -h, --help       显示帮助"
                );
                std::process::exit(0);
            }
            other => anyhow::bail!("未知参数: {other}"),
        }
    }
    Ok(MaintainerCli { config })
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
    fn parse_maintainer_cli_accepts_config() {
        let cli = parse_cli_from(["--config", "config.toml"]).unwrap();

        assert_eq!(
            cli.config.as_deref(),
            Some(std::path::Path::new("config.toml"))
        );
    }

    #[test]
    fn maintainer_version_uses_shared_build_metadata() {
        assert_eq!(
            build_info::version_text("acn-maintainer"),
            format!(
                "acn-maintainer {} ({}, {})",
                env!("CARGO_PKG_VERSION"),
                env!("ACN_GIT_COMMIT"),
                env!("ACN_GIT_COMMIT_TIMESTAMP")
            )
        );
    }
}
