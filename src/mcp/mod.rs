//! MCP 接入模块。
//!
//! 当前模块提供 selected-upstream runtime `<acn_home>/<upstream>/.mcp.json`
//! 的配置读写、校验和 CLI 管理基础。
//! 后续 client / manager / TUI 会在同一模块下继续扩展，避免把外部 MCP server
//! 的生命周期散落到 tool 或 session 代码里。

pub mod client;
pub mod config;
pub mod connection_manager;
pub mod name;
pub mod oauth;
mod oauth_http;
mod process_group;
pub mod redact;
pub mod tool;
