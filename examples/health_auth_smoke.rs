//! 启动隔离 M/R，验收公开 health；--serve 保留进程与请求文件供手工 curl。
mod smoke_support;

use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use base64::Engine;
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};
use smoke_support::{interrupted, random_secret, Fixture};
use tokio::fs;

#[derive(Default)]
struct Options {
    directory: Option<PathBuf>,
    serve: bool,
    m_disabled: bool,
    r_disabled: bool,
}

fn options() -> Result<Options> {
    let mut result = Options::default();
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--directory") => result.directory = Some(args.next().context("--directory 后缺少路径")?.into()),
            Some("--serve") => result.serve = true,
            Some("--maintainer-auth-disabled") => result.m_disabled = true,
            Some("--router-auth-disabled") => result.r_disabled = true,
            _ => anyhow::bail!("用法：health_auth_smoke [--directory <空目录>] [--serve] [--maintainer-auth-disabled] [--router-auth-disabled]"),
        }
    }
    Ok(result)
}

async fn check(fixture: &Fixture, options: &Options, revoked: bool) -> Result<()> {
    let valid = fs::read(fixture.root.join("valid.json")).await?;
    let wrong_key = fs::read(fixture.root.join("wrong-key.json")).await?;
    let wrong_agent = fs::read(fixture.root.join("wrong-agent.json")).await?;
    for (label, url, enabled) in [
        ("M", &fixture.m_url, !options.m_disabled),
        ("R", &fixture.r_url, !options.r_disabled),
    ] {
        for (method, body, passed, name) in [
            (Method::GET, Vec::new(), None, "anonymous GET"),
            (Method::POST, b"{}".to_vec(), None, "empty envelope"),
            (
                Method::POST,
                valid.clone(),
                Some(!enabled || !revoked),
                "valid/revoked key",
            ),
            (
                Method::GET,
                valid.clone(),
                Some(!enabled || !revoked),
                "GET with envelope",
            ),
            (Method::POST, wrong_key.clone(), Some(!enabled), "wrong key"),
            (
                Method::POST,
                wrong_agent.clone(),
                Some(!enabled),
                "wrong agent",
            ),
        ] {
            let response = fixture
                .client
                .request(method, format!("{url}/health"))
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .await?;
            ensure!(
                response.status() == StatusCode::OK,
                "{label} {name}: HTTP {}",
                response.status()
            );
            let actual: Value = response.json().await?;
            let mut expected = json!({"status":"ok","team_auth_enabled":enabled});
            if let Some(passed) = passed {
                expected["auth_passed"] = passed.into();
            }
            // 只输出预期字段，失败响应也不回显潜在凭据。
            ensure!(
                actual == expected,
                "{label} {name}: health 响应与预期不一致"
            );
            println!("PASS {label} {name}: HTTP 200 {expected}");
        }
        let response = fixture
            .client
            .post(format!("{url}/health"))
            .header("Content-Type", "application/json")
            .body("{invalid")
            .send()
            .await?;
        ensure!(
            response.status() == StatusCode::BAD_REQUEST,
            "{label}: 非法 JSON 应返回 400"
        );
        println!("PASS {label} malformed JSON: HTTP 400");
    }
    Ok(())
}

async fn run(fixture: &mut Fixture, options: &Options) -> Result<()> {
    fixture.configure(180).await?;
    fixture
        .start("acn-maintainer", &fixture.m_url.clone())
        .await?;
    let created = fixture
        .client
        .post(format!("{}/api/team-auth/keys", fixture.m_url))
        .basic_auth("admin", Some(&fixture.password))
        .json(&json!({"agent_id":"health-agent"}))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let key = created["acn_key"]
        .as_str()
        .context("创建响应缺少 acn_key")?;
    let key_id = created["key"]["key_id"]
        .as_str()
        .context("创建响应缺少 key_id")?;
    let wrong = random_secret();
    for (file, agent, supplied) in [
        ("valid.json", "health-agent", key),
        ("wrong-key.json", "health-agent", &wrong),
        ("wrong-agent.json", "another-agent", key),
    ] {
        fs::write(
            fixture.root.join(file),
            serde_json::to_vec(&json!({"auth":{"agent_id":agent,"acn_key":supplied},"data":{}}))?,
        )
        .await?;
    }
    let authorization =
        base64::engine::general_purpose::STANDARD.encode(format!("admin:{}", fixture.password));
    fs::write(
        fixture.root.join("admin-header.txt"),
        format!("Authorization: Basic {authorization}\n"),
    )
    .await?;
    fs::write(
        fixture.root.join("endpoints.sh"),
        format!(
            "M_URL={}\nR_URL={}\nHEALTH_KEY_ID={key_id}\n",
            fixture.m_url, fixture.r_url
        ),
    )
    .await?;
    fixture.start("acn-router", &fixture.r_url.clone()).await?;
    check(fixture, options, false).await?;
    if options.serve {
        fs::write(fixture.root.join("ready"), "").await?;
        println!(
            "READY {}; Ctrl-C or SIGTERM stops both daemons.",
            fixture.root.display()
        );
        std::future::pending::<()>().await;
    }
    fixture
        .client
        .post(format!(
            "{}/api/team-auth/keys/{key_id}/revoke",
            fixture.m_url
        ))
        .basic_auth("admin", Some(&fixture.password))
        .send()
        .await?
        .error_for_status()?;
    check(fixture, options, true).await
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = options()?;
    let mut fixture = Fixture::new(
        options.directory.clone(),
        !options.m_disabled,
        !options.r_disabled,
    )
    .await?;
    let result = tokio::select! {
        result = run(&mut fixture, &options) => result,
        result = interrupted() => result,
    };
    let cleanup = fixture.cleanup().await;
    result.and(cleanup)
}
