//! SessionEngine prompt 渲染辅助。
//!
//! 本模块负责构造 session/memory review system prompt 的上下文，
//! 渲染本地 claim 快照、router scope 概览，并把 ACN.md 附加到 prompt 尾部。
//! 它不执行 turn、compaction 或 finalize。

use anyhow::Context;
use chrono::Utc;
use serde::Serialize;

use crate::agent::prepare::llm_visible_claims;
use crate::agent::{InboxProcessReport, TeamServiceConnectionStatus};
use crate::api::AvailableSkill;
use crate::claim::{Claim, ClaimId};
use crate::memory::{render_prompt_block, MemoryTarget};
use crate::router::ScopesOverviewSnapshot;

use super::{
    SessionEngine, PROMPT_AGENT_SYSTEM, PROMPT_EVALUATION_AGENT_SYSTEM,
    PROMPT_EVALUATION_MINIMAL_AGENT_SYSTEM, PROMPT_MEMORY_REVIEW_SYSTEM,
};

const SOLO_TEAM_SERVICES_OVERVIEW: &str = "【当前团队服务状态】用户未配置 maintainer_endpoint 和 router_endpoint，本 session 以单人模式运行；团队 maintainer、router 与 consult_router 均不可用，不会进行任何团队服务交互。请忽略本 prompt 下文关于团队服务和 consult_router 的通用操作说明。如需访问团队服务，请参考 docs/config_parameters.md，同时配置 maintainer_endpoint 和 router_endpoint。";

#[derive(Debug, Serialize)]
struct SessionSystemPromptContext<'a> {
    agent_id: &'a crate::claim::AgentId,
    memory_enabled: bool,
    memory_md: &'a str,
    user_md: &'a str,
    local_claims_snapshot: &'a str,
    router_scopes_overview: &'a str,
    available_skills: Vec<AvailableSkill>,
    subagent_max_concurrent: usize,
    file_edit_authority_enabled: bool,
    /// 当前 registry 实际暴露的工具名；minimal 评测 prompt 据此描述工具面，而不是靠模型读 schema。
    available_tools: Vec<String>,
}

#[derive(Debug, Serialize)]
struct PromptLocalClaimRow<'a> {
    id: &'a ClaimId,
    name: &'a str,
    scope: &'a str,
    statement: &'a str,
    status: crate::claim::ClaimStatus,
    confidence: crate::claim::Confidence,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct MemoryReviewSystemPromptContext<'a> {
    agent_id: &'a crate::claim::AgentId,
    memory_md: &'a str,
    user_md: &'a str,
}

pub(super) fn format_local_claims_snapshot(claims: &[Claim]) -> String {
    if claims.is_empty() {
        return "当前 agent 暂无 status == active 或 status == stale 的本地 claims。".into();
    }
    let mut sorted_claims = claims.iter().collect::<Vec<_>>();
    sorted_claims.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
    let lines = sorted_claims
        .into_iter()
        .map(|claim| {
            serde_json::to_string(&PromptLocalClaimRow {
                id: &claim.id,
                name: &claim.name,
                scope: &claim.scope,
                statement: &claim.statement,
                status: claim.status,
                confidence: claim.confidence,
                created_at: claim.created_at,
            })
            .unwrap_or_else(|_| "{\"error\":\"<unrenderable claim>\"}".into())
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    format!("```jsonl\n{lines}\n```")
}

pub(super) fn format_router_scopes_overview(snapshot: &ScopesOverviewSnapshot) -> String {
    if snapshot.scopes.is_empty() {
        return "团队 router 当前没有可用 scope。".into();
    }
    let scope_lines = snapshot
        .scopes
        .iter()
        .map(|item| {
            let scope = prompt_safe_scope(&item.scope);
            format!(
                "- scope={scope}：active={}，stale={}，open_disputes={}，resolved_disputes={}",
                item.active_claims, item.stale_claims, item.open_disputes, item.resolved_disputes,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "团队 router 当前在这些 scope 下有 claim：\n\n{scope_lines}\n\n当用户请求与这些 scope 相关、需要利用团队已有共享知识时（例如查找可复用判断、复用其他 agent 的 claim、检查潜在 claim 冲突等），使用 consult_router tool 查询具体 claim。"
    )
}

pub(super) fn prompt_safe_scope(scope: &str) -> String {
    serde_json::to_string(scope).unwrap_or_else(|_| "\"<unrenderable scope>\"".into())
}

pub(super) fn append_acn_md(mut system_prompt: String, acn_md: Option<String>) -> String {
    let Some(acn_md) = acn_md else {
        return system_prompt;
    };
    system_prompt.push_str("\n\n# ACN.md 用户指令\n\n");
    system_prompt.push_str(
        "**注：以下内容来自当前 ACN home 的 `ACN.md`，是用户为本 ACN 环境提供的持久偏好、项目约定或协作指令。请在不违反上文 system prompt、工具边界、数据边界和当前用户明确要求的前提下遵循。**\n\n",
    );
    system_prompt.push_str(
        "**如果 ACN.md 与当前用户请求冲突，优先当前用户请求；如果与上文核心系统规则冲突，优先上文系统规则；如果冲突会影响任务执行，应向用户说明并请求确认。**\n\n",
    );
    system_prompt.push_str(&acn_md);
    system_prompt
}

impl SessionEngine {
    pub(super) async fn render_session_system_prompt_for_inbox(
        &self,
        inbox_report: &InboxProcessReport,
    ) -> anyhow::Result<String> {
        match self.runtime_profile {
            super::SessionRuntimeProfile::Evaluation => {
                return self.render_evaluation_session_system_prompt().await;
            }
            super::SessionRuntimeProfile::EvaluationMinimal => {
                return self.render_minimal_evaluation_session_system_prompt().await;
            }
            super::SessionRuntimeProfile::Interactive => {}
        }
        let router_scopes_overview = match inbox_report.team_services.router {
            TeamServiceConnectionStatus::Unknown => SOLO_TEAM_SERVICES_OVERVIEW.into(),
            TeamServiceConnectionStatus::Connected => inbox_report
                .router_scopes_overview
                .as_ref()
                .map(format_router_scopes_overview)
                .unwrap_or_else(|| "Router scope overview 当前不可用。".into()),
            TeamServiceConnectionStatus::Failed => "Router scope overview 当前不可用。".into(),
        };
        self.render_session_system_prompt_with_router_overview(&router_scopes_overview)
            .await
    }

    async fn render_session_system_prompt_with_router_overview(
        &self,
        router_scopes_overview: &str,
    ) -> anyhow::Result<String> {
        let memory_enabled = self.turn_loop.tool_registry().memory_enabled();
        let (memory_text, user_text) = if memory_enabled {
            let memory_snapshot = self.agent.memory_store.read_snapshot().await?;
            (
                render_prompt_block(
                    MemoryTarget::Memory,
                    &memory_snapshot.memory_entries,
                    memory_snapshot.memory_usage.cap_chars,
                ),
                render_prompt_block(
                    MemoryTarget::User,
                    &memory_snapshot.user_entries,
                    memory_snapshot.user_usage.cap_chars,
                ),
            )
        } else {
            (String::new(), String::new())
        };
        let local_claims_snapshot = self.render_local_claims_snapshot().await;
        let context = SessionSystemPromptContext {
            agent_id: &self.agent.agent_id,
            memory_enabled,
            memory_md: &memory_text,
            user_md: &user_text,
            local_claims_snapshot: &local_claims_snapshot,
            router_scopes_overview,
            available_skills: self.agent.available_skills_for_prompt(),
            subagent_max_concurrent: self.subagent_max_concurrent,
            file_edit_authority_enabled: self
                .turn_loop
                .tool_registry()
                .file_edit_authority_enabled(),
            available_tools: Vec::new(),
        };
        let system_prompt = self
            .prompt_registry
            .render(PROMPT_AGENT_SYSTEM, context)
            .context("渲染 session system prompt 失败")?;
        Ok(append_acn_md(system_prompt, self.read_acn_md().await?))
    }

    async fn render_evaluation_session_system_prompt(&self) -> anyhow::Result<String> {
        let router = self
            .agent
            .router
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("evaluation session 缺少冻结 router"))?;
        let overview = router
            .scopes_overview()
            .await
            .context("读取冻结 router scope overview 失败")?;
        let local_claims_snapshot = self.render_local_claims_snapshot().await;
        let router_scopes_overview = format_router_scopes_overview(&overview);
        let context = SessionSystemPromptContext {
            agent_id: &self.agent.agent_id,
            memory_enabled: self.turn_loop.tool_registry().memory_enabled(),
            memory_md: "",
            user_md: "",
            local_claims_snapshot: &local_claims_snapshot,
            router_scopes_overview: &router_scopes_overview,
            available_skills: self.agent.available_skills_for_prompt(),
            subagent_max_concurrent: self.subagent_max_concurrent,
            file_edit_authority_enabled: self
                .turn_loop
                .tool_registry()
                .file_edit_authority_enabled(),
            available_tools: Vec::new(),
        };
        let system_prompt = self
            .prompt_registry
            .render(PROMPT_EVALUATION_AGENT_SYSTEM, context)
            .context("渲染 evaluation session system prompt 失败")?;
        Ok(append_acn_md(system_prompt, self.read_acn_md().await?))
    }

    async fn render_minimal_evaluation_session_system_prompt(&self) -> anyhow::Result<String> {
        let router = self
            .agent
            .router
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("minimal evaluation session 缺少冻结 router"))?;
        let overview = router
            .scopes_overview()
            .await
            .context("读取 minimal evaluation 冻结 router scope overview 失败")?;
        let router_scopes_overview = format_router_scopes_overview(&overview);
        let context = SessionSystemPromptContext {
            agent_id: &self.agent.agent_id,
            memory_enabled: false,
            memory_md: "",
            user_md: "",
            local_claims_snapshot: "",
            router_scopes_overview: &router_scopes_overview,
            available_skills: Vec::new(),
            subagent_max_concurrent: 0,
            file_edit_authority_enabled: false,
            available_tools: self
                .turn_loop
                .tool_registry()
                .definitions()
                .into_iter()
                .map(|definition| definition.name)
                .collect(),
        };
        self.prompt_registry
            .render(PROMPT_EVALUATION_MINIMAL_AGENT_SYSTEM, context)
            .context("渲染 minimal evaluation session system prompt 失败")
    }

    pub(super) async fn read_acn_md(&self) -> anyhow::Result<Option<String>> {
        let Some(path) = &self.acn_md_path else {
            return Ok(None);
        };
        match tokio::fs::read_to_string(path).await {
            Ok(content) => {
                let trimmed = content.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed.to_owned()))
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).with_context(|| format!("读取 ACN.md 失败: {}", path.display())),
        }
    }

    pub(super) async fn render_local_claims_snapshot(&self) -> String {
        match self.agent.claim_store.list_local_claims().await {
            Ok(claims) => format_local_claims_snapshot(&llm_visible_claims(claims)),
            Err(err) => {
                log::warn!(
                    target: "agent",
                    "渲染本地 self claims 快照失败，降级为空快照: {err}"
                );
                format_local_claims_snapshot(&[])
            }
        }
    }

    pub(super) async fn render_memory_review_system_prompt(&self) -> anyhow::Result<String> {
        anyhow::ensure!(
            self.turn_loop.tool_registry().memory_enabled(),
            "persistent memory is disabled"
        );
        let memory_snapshot = self.agent.memory_store.read_snapshot().await?;
        let memory_text = render_prompt_block(
            MemoryTarget::Memory,
            &memory_snapshot.memory_entries,
            memory_snapshot.memory_usage.cap_chars,
        );
        let user_text = render_prompt_block(
            MemoryTarget::User,
            &memory_snapshot.user_entries,
            memory_snapshot.user_usage.cap_chars,
        );
        let context = MemoryReviewSystemPromptContext {
            agent_id: &self.agent.agent_id,
            memory_md: &memory_text,
            user_md: &user_text,
        };
        self.prompt_registry
            .render(PROMPT_MEMORY_REVIEW_SYSTEM, context)
            .context("渲染 memory review system prompt 失败")
    }
}

#[cfg(test)]
mod tests {
    use super::SOLO_TEAM_SERVICES_OVERVIEW;

    #[test]
    fn local_team_services_overview_explains_mode_and_configuration_path() {
        assert!(SOLO_TEAM_SERVICES_OVERVIEW.contains("单人模式"));
        assert!(SOLO_TEAM_SERVICES_OVERVIEW.contains("consult_router 均不可用"));
        assert!(SOLO_TEAM_SERVICES_OVERVIEW.contains("docs/config_parameters.md"));
        assert!(SOLO_TEAM_SERVICES_OVERVIEW.contains("maintainer_endpoint"));
        assert!(SOLO_TEAM_SERVICES_OVERVIEW.contains("router_endpoint"));
    }
}
