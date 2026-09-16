//! 使用独立浏览器 profile 验收 Cookie 的持久化、跨标签同步、注销与过期。
mod smoke_support;

use std::{path::PathBuf, process::Stdio, time::Duration};

use anyhow::{ensure, Result};
use smoke_support::{interrupted, random_secret, Fixture};
use tokio::process::Command;

struct Browser {
    session: String,
    profile: PathBuf,
    password: String,
}

impl Browser {
    async fn call(&self, args: &[&str]) -> Result<()> {
        let child = Command::new("agent-browser")
            .arg("--session")
            .arg(&self.session)
            .arg("--profile")
            .arg(&self.profile)
            .arg("--args")
            .arg("--host-resolver-rules=MAP workbench.example 127.0.0.1\n--no-proxy-server")
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let output =
            tokio::time::timeout(Duration::from_secs(45), child.wait_with_output()).await??;
        let status = output.status;
        // 不打印命令参数；错误输出中的测试密码先脱敏。
        ensure!(
            status.success(),
            "浏览器 {} 操作失败：{status} {} {}",
            args.first().copied().unwrap_or("unknown"),
            String::from_utf8_lossy(&output.stdout).replace(&self.password, "<redacted>"),
            String::from_utf8_lossy(&output.stderr).replace(&self.password, "<redacted>")
        );
        Ok(())
    }

    async fn login(&self, password: &str) -> Result<()> {
        self.call(&["wait", "--text", "Admin credentials"]).await?;
        self.call(&["fill", "#admin-username", "admin"]).await?;
        self.call(&["fill", "#admin-password", password]).await?;
        self.call(&[
            "find",
            "role",
            "button",
            "click",
            "--name",
            "Open Workbench",
        ])
        .await?;
        self.overview().await
    }

    async fn overview(&self) -> Result<()> {
        self.call(&["wait", "--text", "Network Operations Overview"])
            .await
    }

    async fn check(&self, expression: &str) -> Result<()> {
        self.call(&[
            "eval",
            &format!(
                "if (!({expression})) throw new Error('Cookie session assertion failed'); true"
            ),
        ])
        .await
    }
}

async fn run(fixture: &mut Fixture, browser: &Browser, localhost: bool) -> Result<()> {
    let mut origin = reqwest::Url::parse(&fixture.m_url)?;
    origin.set_host(Some(if localhost {
        "localhost"
    } else {
        "workbench.example"
    }))?;
    let app_url = origin.join("/app/")?.to_string();
    fixture.configure(180).await?;
    fixture
        .start("acn-maintainer", &fixture.m_url.clone())
        .await?;
    browser.call(&["open", &app_url]).await?;
    if !localhost {
        browser
            .check("!window.isSecureContext && !navigator.locks && !crypto.randomUUID")
            .await?;
        println!("PASS plain HTTP origin without secure-context APIs");
    }
    browser.login(&fixture.password).await?;
    browser
        .check("!document.cookie.includes('acn_admin_session')")
        .await?;
    browser
        .check("!JSON.stringify(localStorage).match(/authorization|Basic /i)")
        .await?;
    browser
        .check(&format!(
            "!JSON.stringify(localStorage).includes({})",
            serde_json::to_string(&fixture.password)?
        ))
        .await?;
    println!("PASS login with HttpOnly Cookie and no stored password");

    browser.call(&["tab", "new", &app_url]).await?;
    browser.overview().await?;
    println!("PASS second tab shares login");
    browser.call(&["tab", "close"]).await?;
    browser.call(&["tab", "new", &app_url]).await?;
    browser.overview().await?;
    println!("PASS closed tab reopens authenticated");

    // 关闭真实浏览器进程，再打开同一磁盘 profile，不导入 Cookie。
    browser.call(&["close"]).await?;
    browser.call(&["open", &app_url]).await?;
    browser.overview().await?;
    println!("PASS browser process restart restores persistent Cookie");
    browser
        .call(&["eval", "localStorage.clear(); true"])
        .await?;
    browser.call(&["open", &app_url]).await?;
    browser.overview().await?;
    println!("PASS server restores login without local metadata");

    if !localhost {
        // 当前标签占住事务，确认另一标签的真实注销请求会等待；释放后应自动继续。
        browser
            .call(&[
                "eval",
                r#"(async () => { localStorage.removeItem('acn-smoke-release');
await new Promise((resolve, reject) => {
  const opening = indexedDB.open('acn-maintainer-admin-session', 1);
  opening.onupgradeneeded = () => opening.result.createObjectStore('lock');
  opening.onerror = reject;
  opening.onsuccess = () => {
    const db = opening.result;
    const tx = db.transaction('lock', 'readwrite');
    tx.oncomplete = () => db.close();
    const store = tx.objectStore('lock');
    const keep = () => {
      const read = store.get('session');
      read.onsuccess = () => {
        if (!localStorage.getItem('acn-smoke-release')) keep();
        resolve(true);
      };
    };
    keep();
  };
}); return true; })()"#,
            ])
            .await?;
    }
    browser.call(&["tab", "new", &app_url]).await?;
    browser.overview().await?;
    if !localhost {
        browser.call(&["eval", "window.smokeLogoutRequests = 0; const originalFetch = window.fetch; window.fetch = (...args) => { if (args[0] === '/api/admin-auth/logout') window.smokeLogoutRequests++; return originalFetch(...args); }; true"]).await?;
    }
    browser
        .call(&["find", "role", "button", "click", "--name", "Sign out"])
        .await?;
    if !localhost {
        browser.check("document.querySelector('button[title=\"Sign out\"]').disabled && window.smokeLogoutRequests === 0").await?;
        browser
            .call(&[
                "eval",
                "localStorage.setItem('acn-smoke-release', '1'); true",
            ])
            .await?;
    }
    browser
        .call(&["wait", "--text", "Admin credentials"])
        .await?;
    browser.call(&["tab", "close"]).await?;
    browser
        .call(&["wait", "--text", "Admin credentials"])
        .await?;
    browser.call(&["open", &app_url]).await?;
    browser
        .call(&["wait", "--text", "Admin credentials"])
        .await?;
    println!("PASS logout revokes Cookie and synchronizes other open tab");
    if !localhost {
        browser
            .call(&["eval", "localStorage.removeItem('acn-smoke-release'); true"])
            .await?;
        println!("PASS cross-tab IndexedDB lock serializes HTTP logout");
    }
    browser.login(&fixture.password).await?;
    println!("PASS relogin after logout");

    fixture.stop().await?;
    fixture.configure(8).await?;
    fixture
        .start("acn-maintainer", &fixture.m_url.clone())
        .await?;
    browser.call(&["open", &app_url]).await?;
    browser
        .call(&["wait", "--text", "Admin credentials"])
        .await?;
    println!("PASS server rejects credentials lost on service restart");
    browser.login(&fixture.password).await?;
    browser
        .call(&["wait", "--text", "Admin credentials"])
        .await?;
    println!("PASS expiry returns the idle Workbench to login");
    browser.login(&fixture.password).await?;
    println!("PASS relogin after expiry");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.is_empty() || args == ["--localhost"],
        "用法：auth_cookie_smoke [--localhost]"
    );
    let localhost = !args.is_empty();
    let mut fixture = Fixture::new(None, false, false).await?;
    let browser = Browser {
        session: format!("auth-smoke-{}", random_secret()),
        profile: fixture.root.join("profile"),
        password: fixture.password.clone(),
    };
    let result = tokio::select! {
        result = run(&mut fixture, &browser, localhost) => result,
        result = interrupted() => result,
    };
    let browser_cleanup = browser.call(&["close"]).await;
    let cleanup = fixture.cleanup().await;
    result.and(browser_cleanup).and(cleanup)
}
