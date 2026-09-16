//! 验收共用的临时配置、真实 daemon 生命周期与异步清理。

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::Engine;
use rand::RngCore;
use tokio::{fs, net::TcpListener, process::Child, process::Command};

pub fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub struct Fixture {
    pub root: PathBuf,
    pub client: reqwest::Client,
    pub password: String,
    pub m_url: String,
    pub r_url: String,
    m_enabled: bool,
    r_enabled: bool,
    temporary: Option<tempfile::TempDir>,
    children: Vec<Child>,
}

impl Fixture {
    pub async fn new(directory: Option<PathBuf>, m_enabled: bool, r_enabled: bool) -> Result<Self> {
        let (root, temporary) = match directory {
            Some(root) => {
                fs::create_dir_all(&root).await?;
                anyhow::ensure!(
                    !fs::try_exists(root.join("config.toml")).await?,
                    "验收目录已有 config.toml，请使用新的临时目录"
                );
                (fs::canonicalize(root).await?, None)
            }
            None => {
                let temp = tokio::task::spawn_blocking(|| {
                    tempfile::Builder::new()
                        .prefix("acn-server-smoke-")
                        .tempdir()
                })
                .await??;
                (temp.path().to_path_buf(), Some(temp))
            }
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).await?;
        }
        // 同时占住两个随机端口，避免选择到同一个端口。
        let m = TcpListener::bind("127.0.0.1:0").await?;
        let r = TcpListener::bind("127.0.0.1:0").await?;
        Ok(Self {
            m_url: format!("http://{}", m.local_addr()?),
            r_url: format!("http://{}", r.local_addr()?),
            root,
            temporary,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(10))
                .build()?,
            password: random_secret(),
            m_enabled,
            r_enabled,
            children: Vec::new(),
        })
    }

    pub async fn configure(&self, ttl: u64) -> Result<()> {
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"));
        let config = toml::toml! {
            [storage]
            acn_home = ""
            [maintainer.daemon]
            listen = ""
            [maintainer.ui]
            frontend_dist_dir = ""
            [maintainer.auth.admin]
            enabled = true
            password_env = "TEST_ADMIN_PASSWORD"
            session_ttl_secs = 180
            [maintainer.auth.team]
            enabled = false
            [maintainer.arbitration]
            enabled = false
            [router.daemon]
            listen = ""
            [router.auth.team]
            enabled = false
            [router.retrieval]
            enabled = false
            rerank_enabled = false
        };
        let mut config = toml::Value::Table(config);
        config["storage"]["acn_home"] =
            self.root.join("home").to_string_lossy().into_owned().into();
        config["maintainer"]["ui"]["frontend_dist_dir"] = repo
            .join("frontend/maintainer-workbench/dist")
            .to_string_lossy()
            .into_owned()
            .into();
        config["maintainer"]["daemon"]["listen"] = self.m_url.trim_start_matches("http://").into();
        config["router"]["daemon"]["listen"] = self.r_url.trim_start_matches("http://").into();
        config["maintainer"]["auth"]["admin"]["session_ttl_secs"] = i64::try_from(ttl)?.into();
        config["maintainer"]["auth"]["team"]["enabled"] = self.m_enabled.into();
        config["router"]["auth"]["team"]["enabled"] = self.r_enabled.into();
        fs::write(self.root.join("config.toml"), toml::to_string(&config)?).await?;
        Ok(())
    }

    pub async fn start(&mut self, binary: &str, url: &str) -> Result<()> {
        let executable = std::env::current_exe()?;
        let target = executable
            .parent()
            .and_then(Path::parent)
            .context("找不到 examples 对应的构建目录")?;
        let log_path = self.root.join(format!("{binary}.log"));
        let stdout = fs::File::create(&log_path).await?.into_std().await;
        let stderr = fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .await?
            .into_std()
            .await;
        let child = Command::new(target.join(binary))
            .arg("--config")
            .arg(self.root.join("config.toml"))
            .env("TEST_ADMIN_PASSWORD", &self.password)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true)
            .spawn()
            .context("启动失败，请先 cargo build --bins --examples")?;
        self.children.push(child);
        let child = self.children.last_mut().context("缺少刚启动的进程")?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            anyhow::ensure!(
                child.try_wait()?.is_none(),
                "{binary} 启动失败，日志：{}",
                log_path.display()
            );
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                bail!("{binary} 启动超时");
            }
            if let Ok(response) = self
                .client
                .get(format!("{url}/health"))
                .timeout(remaining.min(Duration::from_secs(1)))
                .send()
                .await
            {
                if response.status().is_success() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn stop(&mut self) -> Result<()> {
        let mut first_error = None;
        for child in self.children.iter_mut().rev() {
            if let Err(error) = child.kill().await {
                first_error.get_or_insert(error);
            }
        }
        self.children.clear();
        if let Some(error) = first_error {
            return Err(error.into());
        }
        Ok(())
    }

    pub async fn cleanup(mut self) -> Result<()> {
        let stopped = self.stop().await;
        let ready = self.root.join("ready");
        match fs::remove_file(ready).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if let Some(temp) = self.temporary.take() {
            tokio::task::spawn_blocking(move || temp.close()).await??;
        }
        stopped
    }
}

pub async fn interrupted() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
