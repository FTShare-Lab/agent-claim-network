//! Selected-upstream runtime MCP 配置文件读写。
//!
//! `.mcp.json` 独立于 `config.toml`，由 `Config::storage.mcp_config_path()`
//! 定位到当前 upstream runtime root。本模块负责 DTO、兼容性推断、基础校验
//! 和 JSON pretty 原子写。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::storage::{write_text_atomic, StorageError};

pub const DEFAULT_STARTUP_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_TOOL_TIMEOUT_SECS: u64 = 120;

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct McpJsonConfig {
    #[serde(default, rename = "mcpServers")]
    pub servers: BTreeMap<String, McpServerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub transport: Option<McpTransportKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_vars: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token_env_var: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum McpTransportKind {
    #[serde(rename = "stdio")]
    Stdio,
    #[serde(rename = "streamable_http", alias = "http")]
    StreamableHttp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpTransportConfig {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        env_vars: Vec<String>,
        cwd: Option<PathBuf>,
    },
    StreamableHttp {
        url: String,
        bearer_token_env_var: Option<String>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum McpConfigError {
    #[error("MCP 配置文件 I/O 失败 ({path:?}): {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("MCP 配置 JSON 序列化失败: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("MCP 配置 JSON 解析失败 ({path:?}): {source}")]
    Decode {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("MCP server name 无效 '{name}': 只能使用 ASCII 字母、数字、'-'、'_'，且不能为空")]
    InvalidServerName { name: String },
    #[error("MCP server '{name}' 缺少 transport 配置：需要 command 或 url")]
    MissingTransport { name: String },
    #[error("MCP server '{name}' 配置为 stdio，但缺少 command")]
    MissingCommand { name: String },
    #[error("MCP server '{name}' 配置为 streamable_http，但缺少 url")]
    MissingUrl { name: String },
    #[error("MCP server '{name}' 的 {field} 必须大于 0")]
    InvalidTimeout { name: String, field: &'static str },
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl McpJsonConfig {
    pub fn validate(&self) -> Result<(), McpConfigError> {
        for (name, server) in &self.servers {
            validate_server_name(name)?;
            server.transport_config(name)?;
            validate_timeout(name, "startup_timeout_secs", server.startup_timeout_secs)?;
            validate_timeout(name, "tool_timeout_secs", server.tool_timeout_secs)?;
        }
        Ok(())
    }
}

impl McpServerConfig {
    pub fn stdio(
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        env_vars: Vec<String>,
    ) -> Self {
        Self {
            transport: Some(McpTransportKind::Stdio),
            enabled: None,
            startup_timeout_secs: None,
            tool_timeout_secs: None,
            enabled_tools: None,
            disabled_tools: None,
            command: Some(command),
            args: non_empty_vec(args),
            env: non_empty_map(env),
            env_vars: non_empty_vec(env_vars),
            cwd: None,
            url: None,
            bearer_token_env_var: None,
        }
    }

    pub fn streamable_http(url: String, bearer_token_env_var: Option<String>) -> Self {
        Self {
            transport: Some(McpTransportKind::StreamableHttp),
            enabled: None,
            startup_timeout_secs: None,
            tool_timeout_secs: None,
            enabled_tools: None,
            disabled_tools: None,
            command: None,
            args: None,
            env: None,
            env_vars: None,
            cwd: None,
            url: Some(url),
            bearer_token_env_var,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    pub fn startup_timeout_secs(&self) -> u64 {
        self.startup_timeout_secs
            .unwrap_or(DEFAULT_STARTUP_TIMEOUT_SECS)
    }

    pub fn tool_timeout_secs(&self) -> u64 {
        self.tool_timeout_secs.unwrap_or(DEFAULT_TOOL_TIMEOUT_SECS)
    }

    pub fn transport_kind(&self, name: &str) -> Result<McpTransportKind, McpConfigError> {
        if let Some(transport) = self.transport {
            return Ok(transport);
        }
        match (
            self.command
                .as_deref()
                .is_some_and(|v| !v.trim().is_empty()),
            self.url.as_deref().is_some_and(|v| !v.trim().is_empty()),
        ) {
            (true, false) => Ok(McpTransportKind::Stdio),
            (false, true) => Ok(McpTransportKind::StreamableHttp),
            (true, true) => Err(McpConfigError::MissingTransport {
                name: format!("{name} (同时存在 command 和 url，请显式设置 type)"),
            }),
            (false, false) => Err(McpConfigError::MissingTransport {
                name: name.to_string(),
            }),
        }
    }

    pub fn transport_config(&self, name: &str) -> Result<McpTransportConfig, McpConfigError> {
        match self.transport_kind(name)? {
            McpTransportKind::Stdio => {
                let command = required_non_empty(&self.command, name, MissingField::Command)?;
                Ok(McpTransportConfig::Stdio {
                    command,
                    args: self.args.clone().unwrap_or_default(),
                    env: self.env.clone().unwrap_or_default(),
                    env_vars: self.env_vars.clone().unwrap_or_default(),
                    cwd: self.cwd.clone(),
                })
            }
            McpTransportKind::StreamableHttp => {
                let url = required_non_empty(&self.url, name, MissingField::Url)?;
                Ok(McpTransportConfig::StreamableHttp {
                    url,
                    bearer_token_env_var: self.bearer_token_env_var.clone(),
                })
            }
        }
    }
}

/// 读取 `.mcp.json`；文件不存在时返回空配置。
pub async fn read_mcp_json_config(path: &Path) -> Result<McpJsonConfig, McpConfigError> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(McpJsonConfig::default());
        }
        Err(source) => {
            return Err(McpConfigError::Io {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let cfg = serde_json::from_slice::<McpJsonConfig>(&bytes).map_err(|source| {
        McpConfigError::Decode {
            path: path.to_path_buf(),
            source,
        }
    })?;
    cfg.validate()?;
    Ok(cfg)
}

/// 原子写 `.mcp.json`，始终使用 pretty JSON。
pub async fn write_mcp_json_config_atomic(
    path: &Path,
    cfg: &McpJsonConfig,
) -> Result<(), McpConfigError> {
    cfg.validate()?;
    let mut json = serde_json::to_string_pretty(cfg)?;
    json.push('\n');
    write_text_atomic(path, json.as_bytes()).await?;
    Ok(())
}

pub fn validate_server_name(name: &str) -> Result<(), McpConfigError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(McpConfigError::InvalidServerName {
            name: name.to_string(),
        });
    }
    Ok(())
}

fn validate_timeout(
    name: &str,
    field: &'static str,
    value: Option<u64>,
) -> Result<(), McpConfigError> {
    if matches!(value, Some(0)) {
        return Err(McpConfigError::InvalidTimeout {
            name: name.to_string(),
            field,
        });
    }
    Ok(())
}

fn required_non_empty(
    value: &Option<String>,
    name: &str,
    field: MissingField,
) -> Result<String, McpConfigError> {
    let value = value.as_deref().unwrap_or_default().trim();
    if value.is_empty() {
        return match field {
            MissingField::Command => Err(McpConfigError::MissingCommand {
                name: name.to_string(),
            }),
            MissingField::Url => Err(McpConfigError::MissingUrl {
                name: name.to_string(),
            }),
        };
    }
    Ok(value.to_string())
}

enum MissingField {
    Command,
    Url,
}

fn non_empty_vec(value: Vec<String>) -> Option<Vec<String>> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn non_empty_map(value: BTreeMap<String, String>) -> Option<BTreeMap<String, String>> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn missing_file_returns_empty_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");

        let cfg = read_mcp_json_config(&path).await.unwrap();

        assert!(cfg.servers.is_empty());
    }

    #[tokio::test]
    async fn round_trip_pretty_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".mcp.json");
        let mut cfg = McpJsonConfig::default();
        cfg.servers.insert(
            "pal".to_string(),
            McpServerConfig::stdio(
                "uvx".to_string(),
                vec!["pal-mcp-server".to_string()],
                BTreeMap::from([("DEFAULT_MODEL".to_string(), "auto".to_string())]),
                vec!["OPENAI_API_KEY".to_string()],
            ),
        );

        write_mcp_json_config_atomic(&path, &cfg).await.unwrap();
        let raw = tokio::fs::read_to_string(&path).await.unwrap();
        let loaded = read_mcp_json_config(&path).await.unwrap();

        assert!(raw.contains("\"mcpServers\""));
        assert!(raw.ends_with('\n'));
        assert_eq!(loaded, cfg);
    }

    #[test]
    fn validates_server_name() {
        validate_server_name("github_1-dev").unwrap();
        assert!(validate_server_name("").is_err());
        assert!(validate_server_name("bad.name").is_err());
        assert!(validate_server_name("中文").is_err());
    }

    #[test]
    fn infers_transport_from_command_or_url() {
        let stdio = McpServerConfig {
            transport: None,
            enabled: None,
            startup_timeout_secs: None,
            tool_timeout_secs: None,
            enabled_tools: None,
            disabled_tools: None,
            command: Some("server".to_string()),
            args: None,
            env: None,
            env_vars: None,
            cwd: None,
            url: None,
            bearer_token_env_var: None,
        };
        let http = McpServerConfig {
            transport: None,
            enabled: None,
            startup_timeout_secs: None,
            tool_timeout_secs: None,
            enabled_tools: None,
            disabled_tools: None,
            command: None,
            args: None,
            env: None,
            env_vars: None,
            cwd: None,
            url: Some("https://example.com/mcp".to_string()),
            bearer_token_env_var: None,
        };

        assert_eq!(
            stdio.transport_kind("stdio_server").unwrap(),
            McpTransportKind::Stdio
        );
        assert_eq!(
            http.transport_kind("http_server").unwrap(),
            McpTransportKind::StreamableHttp
        );
    }

    #[test]
    fn enabled_defaults_to_true_and_can_be_overridden() {
        let mut server = McpServerConfig::streamable_http("https://example.com/mcp".into(), None);

        assert!(server.is_enabled());
        server.enabled = Some(false);
        assert!(!server.is_enabled());
        server.enabled = Some(true);
        assert!(server.is_enabled());
    }

    #[test]
    fn http_transport_alias_serializes_as_streamable_http() {
        let server = serde_json::from_str::<McpServerConfig>(
            r#"{"type":"http","url":"https://example.com/mcp"}"#,
        )
        .unwrap();

        assert_eq!(
            server.transport_kind("remote").unwrap(),
            McpTransportKind::StreamableHttp
        );
        let encoded = serde_json::to_string(&server).unwrap();
        assert!(encoded.contains(r#""type":"streamable_http""#));
        assert!(!encoded.contains(r#""type":"http""#));
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let error = serde_json::from_str::<McpJsonConfig>(r#"{"mcpServer": {}}"#).unwrap_err();

        assert!(error.to_string().contains("unknown field `mcpServer`"));
    }

    #[test]
    fn rejects_typos_in_server_permission_and_enabled_fields() {
        for (field, value) in [
            ("enabledTools", r#"["search_code"]"#),
            ("disabledTools", r#"["delete_repo"]"#),
            ("enable", "false"),
        ] {
            let json = format!(
                r#"{{
                    "mcpServers": {{
                        "github": {{
                            "type": "streamable_http",
                            "url": "https://example.com/mcp",
                            "{field}": {value}
                        }}
                    }}
                }}"#
            );
            let error = serde_json::from_str::<McpJsonConfig>(&json).unwrap_err();

            assert!(
                error
                    .to_string()
                    .contains(&format!("unknown field `{field}`")),
                "unexpected error for {field}: {error}"
            );
        }
    }
}
