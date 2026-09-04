//! `AgentRunner`：承载单个 agent 的本地资源、inbox 处理与 trace 写入能力。
//!
//! ## 设计纪律：团队中没有绝对真理
//! policy 不再作为"外部约束"凭空生效；它通过 inbox 抵达后，必须由 agent 主动**内化**为
//! 本地 claim（`source_claim_ids` 上挂 policy_id）才会进入判断体系。inbox 消息从
//! `.yaml` 变为 `.done.yaml` 的过程，本身就是这次"内化"行为的记录。
//! 因此 runner **不再**维护 `active_policies` 缓存，也没有 `replay_active_policies`。
//!
//! ## inbox 处理顺序
//! 1. 主动 pull maintainer outbox 并处理本地 pending inbox
//! 2. 按消息类型处理：
//!    - `PolicyUpdate`：批量交给 `llm.internalize_inbox`，由 LLM 决定新增或更新哪些本地
//!      claim、是否产出 dispute；runner 落地后再 ack 这些消息
//!    - `ClaimAttributeUpdate`：走独立 prompt，可新增 / 更新本地 claim 或产出 dispute；
//!      有产出时单独写 inbox trace，并在 `input_claims` 里记录来源 policy

use std::sync::Arc;

use tokio::sync::Mutex;

use super::context::AgentContext;
use super::inbox::InboxJsonGenerator;
use super::maintainer_upload::LocalFsMaintainerUploadQueue;
use super::traits::{InboxReader, LocalClaimStore, MemoryStore, ReportedDisputeClaimSetStore};
use crate::claim::{AgentId, ClaimId, DisputeId, TraceId};
use crate::maintainer::traits::MaintainerClient;
use crate::router::{RouterClient, ScopesOverviewSnapshot};
use crate::skill::SkillSummary;

pub struct AgentRunner {
    pub(super) context: Arc<AgentContext>,
    pub(super) agent_id: AgentId,
    pub(super) inbox_generator: Arc<dyn InboxJsonGenerator>,
    pub(super) claim_store: Arc<dyn LocalClaimStore>,
    pub(super) reported_dispute_claim_sets: Arc<dyn ReportedDisputeClaimSetStore>,
    pub(super) inbox: Arc<dyn InboxReader>,
    pub(super) maintainer_client: Option<Arc<dyn MaintainerClient>>,
    pub(super) maintainer_upload_queue: Arc<LocalFsMaintainerUploadQueue>,
    pub(super) llm_retry_count: u32,
    pub(super) inbox_process_lock: Mutex<()>,
    pub(super) dispute_report_lock: Mutex<()>,
}

impl AgentRunner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        agent_id: AgentId,
        inbox_generator: Arc<dyn InboxJsonGenerator>,
        claim_store: Arc<dyn LocalClaimStore>,
        reported_dispute_claim_sets: Arc<dyn ReportedDisputeClaimSetStore>,
        inbox: Arc<dyn InboxReader>,
        memory_store: Arc<dyn MemoryStore>,
        router: Arc<dyn RouterClient>,
        maintainer_client: Arc<dyn MaintainerClient>,
        maintainer_upload_queue: Arc<LocalFsMaintainerUploadQueue>,
        llm_retry_count: u32,
        available_skills: Vec<SkillSummary>,
    ) -> Self {
        Self::new_with_team_services(
            agent_id,
            inbox_generator,
            claim_store,
            reported_dispute_claim_sets,
            inbox,
            memory_store,
            Some(router),
            Some(maintainer_client),
            maintainer_upload_queue,
            llm_retry_count,
            available_skills,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_local(
        agent_id: AgentId,
        inbox_generator: Arc<dyn InboxJsonGenerator>,
        claim_store: Arc<dyn LocalClaimStore>,
        reported_dispute_claim_sets: Arc<dyn ReportedDisputeClaimSetStore>,
        inbox: Arc<dyn InboxReader>,
        memory_store: Arc<dyn MemoryStore>,
        maintainer_upload_queue: Arc<LocalFsMaintainerUploadQueue>,
        llm_retry_count: u32,
        available_skills: Vec<SkillSummary>,
    ) -> Self {
        Self::new_with_team_services(
            agent_id,
            inbox_generator,
            claim_store,
            reported_dispute_claim_sets,
            inbox,
            memory_store,
            None,
            None,
            maintainer_upload_queue,
            llm_retry_count,
            available_skills,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_team_services(
        agent_id: AgentId,
        inbox_generator: Arc<dyn InboxJsonGenerator>,
        claim_store: Arc<dyn LocalClaimStore>,
        reported_dispute_claim_sets: Arc<dyn ReportedDisputeClaimSetStore>,
        inbox: Arc<dyn InboxReader>,
        memory_store: Arc<dyn MemoryStore>,
        router: Option<Arc<dyn RouterClient>>,
        maintainer_client: Option<Arc<dyn MaintainerClient>>,
        maintainer_upload_queue: Arc<LocalFsMaintainerUploadQueue>,
        llm_retry_count: u32,
        available_skills: Vec<SkillSummary>,
    ) -> Self {
        let context = Arc::new(AgentContext {
            agent_id: agent_id.clone(),
            claim_store: claim_store.clone(),
            inbox: inbox.clone(),
            memory_store: memory_store.clone(),
            router,
            maintainer_client: maintainer_client.clone(),
            available_skills: available_skills.clone(),
        });
        Self {
            context,
            agent_id,
            inbox_generator,
            claim_store,
            reported_dispute_claim_sets,
            inbox,
            maintainer_client,
            maintainer_upload_queue,
            llm_retry_count,
            inbox_process_lock: Mutex::new(()),
            dispute_report_lock: Mutex::new(()),
        }
    }

    pub fn agent_id(&self) -> &AgentId {
        &self.agent_id
    }

    pub fn context(&self) -> Arc<AgentContext> {
        self.context.clone()
    }

    /// 当前 runner 是否启用了完整团队服务。
    pub(super) fn team_services_configured(&self) -> bool {
        self.maintainer_client.is_some() && self.context.router.is_some()
    }
}

/// 单个团队服务最近一次访问的连接结果。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TeamServiceConnectionStatus {
    /// 当前 upstream 未配置团队服务，或本会话尚未访问。
    #[default]
    Unknown,
    /// 最近一次访问成功。
    Connected,
    /// 最近一次访问失败。
    Failed,
}

/// 本会话最近一次 inbox 流程观测到的团队服务连接状态。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TeamServicesConnectionStatus {
    pub maintainer: TeamServiceConnectionStatus,
    pub router: TeamServiceConnectionStatus,
}

/// process_inbox 一次执行的统计
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InboxProcessReport {
    /// 本次 inbox 流程观测到的团队服务连接状态。
    pub team_services: TeamServicesConnectionStatus,
    /// Router 访问成功时返回的 scope 概览，供 session prompt 复用，避免重复请求。
    pub router_scopes_overview: Option<ScopesOverviewSnapshot>,
    /// 本次完成本地处理并写入 done ACK 的消息数。
    pub total: usize,
    /// 其中 PolicyUpdate 的条数
    pub policy_count: usize,
    /// inbox 内化阶段写出的 trace id
    pub trace_ids: Vec<TraceId>,
    /// 其中 PolicyUpdate deprecated 的确定性处理条数
    pub policy_deprecation_count: usize,
    /// inbox deprecate 阶段标记 deprecated 的本地 claim id
    pub deprecated_claim_ids: Vec<ClaimId>,
    /// 其中 ClaimAttributeUpdate 的条数
    pub claim_attribute_count: usize,
    /// inbox 内化阶段新增的 claim id
    pub new_claim_ids: Vec<ClaimId>,
    /// inbox 内化阶段更新的本地 claim id
    pub updated_claim_ids: Vec<ClaimId>,
    /// inbox 内化阶段新增的 dispute id
    pub new_dispute_ids: Vec<DisputeId>,
    /// 本轮降级处理但未阻断流程的 warning
    pub warnings: Vec<String>,
    /// 本轮 inbox 的可恢复失败；允许 session 继续创建或使用。
    pub failures: Vec<InboxProcessFailure>,
}

/// Inbox 失败对用户提示和副作用声明的分类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxProcessFailureKind {
    /// Provider、输出解析、schema 或业务校验在 prepared 结果产生前失败。
    Internalization,
    /// 本地持久化或应用失败，可能已经发生部分本地副作用。
    Local,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxProcessFailure {
    pub kind: InboxProcessFailureKind,
    pub error: String,
}
