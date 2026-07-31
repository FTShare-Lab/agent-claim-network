//! Agent Claim Network — 顶层库入口。
//!
//! 模块按角色组织：
//! - `claim` 实体（claim/policy/trace/dispute/inbox）+ ID 工具
//! - `storage` 文件 I/O（YAML 原子写、路径拼接）
//! - `api` provider-neutral LLM 协议、工具回环、结构化 JSON 调用与具体 provider adapter
//! - `agent` agent 端 traits + AgentRunner / SessionEngine + 本地文件系统实现
//! - `router` Router struct + RouterClient trait + claim_index 维护
//! - `maintainer` Maintainer struct + client trait + dispute/policy 管理 + inbox 下发
//! - `skill` / `tool` Skill 加载与完整工具运行时
//! - `bootstrap` trait 对象的统一构造点
//! - `config` 配置加载
//! - `prompt` MiniJinja prompt 模板加载与渲染

pub mod agent;
pub mod api;
pub mod attachment;
pub mod auth;
pub mod bootstrap;
pub mod build_info;
pub mod claim;
pub mod config;
pub mod delegation;
pub mod maintainer;
pub mod mcp;
pub mod memory;
pub mod memory_safety;
mod path_util;
pub mod prompt;
pub mod router;
mod serde_util;
pub mod session;
pub mod session_search;
pub mod session_tui;
pub mod skill;
pub mod storage;
pub mod supervisor;
pub mod time;
pub mod tool;
// 保留 0.2 之前公开的模块路径；实现文件已统一收拢到 `src/tool/`。
#[doc(hidden)]
pub use tool::diff as tool_diff;
#[doc(hidden)]
pub use tool::memory as tool_memory;
#[doc(hidden)]
pub use tool::read_state as tool_read_state;
pub mod tracing;
pub mod update;
pub mod upstream_migration;
