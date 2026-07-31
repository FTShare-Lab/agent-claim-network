//! agent 运行上下文。
//!
//! `AgentContext` 承载单个 agent 在 runner、session engine 等生命周期入口之间共享的资源。
//! 它不包含 LLM client；模型协议依赖由 provider-neutral 的 turn/json 组件单独注入。

use std::sync::Arc;

use super::traits::{InboxReader, LocalClaimStore, MemoryStore};
use crate::api::AvailableSkill;
use crate::claim::AgentId;
use crate::maintainer::traits::MaintainerClient;
use crate::router::RouterClient;
use crate::skill::SkillSummary;

#[derive(Clone)]
pub struct AgentContext {
    pub agent_id: AgentId,
    pub claim_store: Arc<dyn LocalClaimStore>,
    pub inbox: Arc<dyn InboxReader>,
    pub memory_store: Arc<dyn MemoryStore>,
    pub router: Option<Arc<dyn RouterClient>>,
    pub maintainer_client: Option<Arc<dyn MaintainerClient>>,
    pub available_skills: Vec<SkillSummary>,
}

impl AgentContext {
    pub fn available_skills_for_prompt(&self) -> Vec<AvailableSkill> {
        self.available_skills
            .iter()
            .filter(|s| s.name != "consult_router")
            .map(|s| AvailableSkill {
                name: s.name.clone(),
                description: s.description.clone(),
                spec_path: s.spec_path.display().to_string(),
            })
            .collect()
    }
}
