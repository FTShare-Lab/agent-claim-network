//! 进程整合：从 `Config` 构造 TUI session、Router 与 Maintainer 的运行时依赖。
//!
//! 设计要点：
//! - **trait 对象集中点**：所有 `Arc<dyn ...>` 装配只发生在这里，业务模块不感知具体实现
//! - **取消优先**：`CancellationToken` 父级触发，Router / Maintainer 后台任务统一监听
//! - **provider-neutral**：agent 运行期只装配统一 provider adapter，Anthropic/OpenAI 不分叉
//! - **不在 build 阶段做 LLM 真实请求**：仅构造 client；实际请求等 session turn 到来再发出

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::agent::fs::{
    LocalFsClaimStore, LocalFsInboxReader, LocalFsMemoryStore, LocalFsReportedDisputeClaimSetStore,
};
use crate::agent::maintainer_upload::LocalFsMaintainerUploadQueue;
use crate::agent::{
    AgentRunner, InboxReader, LocalClaimStore, MemoryStore, PromptInboxJsonGenerator,
    ReportedDisputeClaimSetStore, SessionEngine, SessionEngineOptions,
};
use crate::api::{
    build_embedding_client, AgentTurnLoop, AnthropicProviderAdapter, MemoryReviewLoop,
    OpenAiCompatibleChatProviderAdapter, ProviderAdapter, StructuredJsonCaller,
    MEMORY_REVIEW_MAX_TOOL_LOOP_TURNS,
};
use crate::attachment::AttachmentLimits;
use crate::claim::AgentId;
use crate::config::{Config, LlmChatConfig, LlmProvider, ResolvedUpstream};
use crate::delegation::{DelegationRunnerConfig, DelegationWaitConfig, LlmDelegationExecutor};
use crate::maintainer::arbitration::{
    phase_timeout, ArbitrationContextBuilder, ArbitrationService, ArbitrationStore,
    LlmArbitrationEvaluator, ResolutionService, SystemArbitrationClock,
};
use crate::maintainer::http_client::HttpMaintainerClient;
use crate::maintainer::traits::MaintainerClient;
use crate::maintainer::Maintainer;
use crate::mcp::connection_manager::McpConnectionManager;
use crate::prompt::PromptRegistry;
use crate::router::http_client::HttpRouterClient;
use crate::router::{
    run_refresh_worker, run_vector_worker, Router, RouterClient, TimeoutRouterClient,
    VectorRetryPolicy,
};
use crate::session::SessionStore;
use crate::session_search::{SessionSearchConfig, SessionSearchService, SessionSearchSummarizer};
use crate::skill::SkillRegistry;
use crate::storage::paths;
use crate::tool::ToolRegistry;

const ROUTER_VECTOR_WORKER_TASK_NAME: &str = "router_vector_worker";
const ROUTER_REFRESH_WORKER_TASK_NAME: &str = "router_refresh_worker";
const REQUIRED_SESSION_PROMPTS: &[&str] = &[
    "agent_system",
    "inbox_policy_update_internalize",
    "inbox_claim_attribute_update_internalize",
    "memory_review",
    "session_recap",
    "session_compaction",
    "session_search_summary",
    "memory_review_system",
    "subagents_system",
    "subagents_compaction",
];
const PROMPT_SESSION_SEARCH_SUMMARY: &str = "session_search_summary";

/// 构造 agent CLI runner；endpoint 成对配置时接入团队 HTTP 服务，否则仅本地运行。
pub fn build_agent_cli_runner(
    cfg: &Config,
    upstream: &ResolvedUpstream,
) -> anyhow::Result<Arc<AgentRunner>> {
    let team_clients = if upstream.team_services_configured() {
        let router: Arc<dyn RouterClient> = Arc::new(TimeoutRouterClient::new(
            Arc::new(HttpRouterClient::new_with_auth(
                upstream.router_endpoint.clone(),
                &cfg.clients.http,
                upstream.agent_id.clone(),
                upstream.acn_key.clone(),
            )?),
            Duration::from_secs(cfg.clients.router.query_timeout_secs),
        ));
        let maintainer: Arc<dyn MaintainerClient> = Arc::new(HttpMaintainerClient::new_with_auth(
            upstream.maintainer_endpoint.clone(),
            &cfg.clients.http,
            upstream.agent_id.clone(),
            upstream.acn_key.clone(),
        )?);
        Some((router, maintainer))
    } else {
        None
    };
    let prompts = build_session_prompt_registry(cfg)?;
    let runner = build_agent_runner(cfg, upstream.agent_id.clone(), team_clients, prompts)?;
    Ok(Arc::new(runner))
}

/// 构造交互式 agent CLI session engine，供 headless REPL / TUI 复用。
pub fn build_agent_cli_session_engine(
    cfg: &Config,
    upstream: &ResolvedUpstream,
) -> anyhow::Result<SessionEngine> {
    build_agent_cli_session_engine_with_mcp(cfg, upstream, None)
}

/// 构造交互式 agent CLI session engine，并按需注入已初始化的 MCP manager。
pub fn build_agent_cli_session_engine_with_mcp(
    cfg: &Config,
    upstream: &ResolvedUpstream,
    mcp_manager: Option<Arc<McpConnectionManager>>,
) -> anyhow::Result<SessionEngine> {
    let runner = build_agent_cli_runner(cfg, upstream)?;
    let prompt_registry = build_session_prompt_registry(cfg)?;
    let provider = build_provider_adapter(cfg)?;
    let chat = &cfg.agent.llm;
    let json_caller = Arc::new(StructuredJsonCaller::new(
        provider.clone(),
        chat.max_tokens,
        chat.retry_count,
        Duration::from_millis(chat.retry_base_delay_ms),
        Duration::from_millis(chat.retry_max_delay_ms),
    ));
    let context = runner.context();
    let session_search_summarizer: Arc<dyn SessionSearchSummarizer> = Arc::new(
        ProviderSessionSearchSummarizer::new(prompt_registry.clone(), json_caller.clone()),
    );
    let session_search = Arc::new(SessionSearchService::new(
        context.agent_id.clone(),
        cfg.agent_home(&context.agent_id),
        chat.model.clone(),
        session_search_summarizer,
        SessionSearchConfig::from(&cfg.agent.tool),
    ));
    let attachment_limits = AttachmentLimits::from_configs(&cfg.agent.attachment, &cfg.agent.tool);
    let mut base_tool_registry = ToolRegistry::new(&cfg.agent.tool)?
        .with_file_write_lock_root(paths::base_acn_home_file_write_locks_dir(
            cfg.storage.base_acn_home(),
        ))
        .with_process_id_attempts(cfg.agent.session.id_mint_max_attempts())
        .with_process_owner_agent_id(context.agent_id.clone())
        .with_memory_store(context.memory_store.clone())
        .with_session_search(session_search)
        .with_attachment_limits(attachment_limits);
    if let Some(router) = context.router.clone() {
        base_tool_registry = base_tool_registry.with_router_client(router);
    }
    let engine_mcp_manager = mcp_manager.clone();
    if let Some(mcp_manager) = mcp_manager {
        base_tool_registry = base_tool_registry.with_mcp_manager(mcp_manager);
    }
    let agent_home = cfg.agent_home(&context.agent_id);
    let delegation_child_registry = base_tool_registry.clone().for_delegation(None);
    let delegation_compaction = cfg
        .agent
        .session
        .subagents
        .compaction
        .clone()
        .unwrap_or_else(|| cfg.agent.session.compaction.clone());
    let delegation_executor = Arc::new(
        LlmDelegationExecutor::new(
            provider.clone(),
            delegation_child_registry,
            prompt_registry.clone(),
            json_caller.clone(),
            chat.max_tokens,
            delegation_compaction,
            cfg.agent.llm.context_window,
        )
        .with_max_tool_loop_turns(cfg.agent.session.subagents.max_tool_loop_turns)
        .with_attachment_limits(attachment_limits)
        .with_tool_journal_preview_limits(
            cfg.agent.session.turn_journal.recovery_tool_input_max_chars,
            cfg.agent
                .session
                .turn_journal
                .recovery_tool_output_max_chars,
        ),
    );
    let delegation_runner_config = DelegationRunnerConfig {
        max_concurrent: cfg.agent.session.subagents.max_concurrent,
        wall_timeout: Duration::from_secs(cfg.agent.session.subagents.wall_timeout_secs),
        wait: DelegationWaitConfig {
            default_timeout: Duration::from_secs(
                cfg.agent.session.subagents.wait.default_timeout_secs,
            ),
            min_timeout: Duration::from_secs(cfg.agent.session.subagents.wait.min_timeout_secs),
            max_timeout: Duration::from_secs(cfg.agent.session.subagents.wait.max_timeout_secs),
        },
    };
    let tool_registry = Arc::new(base_tool_registry.clone().with_delegation_executor(
        agent_home,
        context.agent_id.clone(),
        delegation_executor,
        delegation_runner_config,
    ));
    let memory_review_tool_registry = Arc::new(base_tool_registry.for_memory_review());
    let turn_loop = Arc::new(
        AgentTurnLoop::new(provider.clone(), tool_registry.clone(), chat.max_tokens)
            .with_attachment_limits(attachment_limits)
            .with_tool_journal_preview_limits(
                cfg.agent.session.turn_journal.recovery_tool_input_max_chars,
                cfg.agent
                    .session
                    .turn_journal
                    .recovery_tool_output_max_chars,
            ),
    );
    let memory_review_loop = Arc::new(MemoryReviewLoop::new(
        provider.clone(),
        memory_review_tool_registry,
        MEMORY_REVIEW_MAX_TOOL_LOOP_TURNS,
        chat.max_tokens,
    ));
    let mut engine = SessionEngine::new(
        runner,
        turn_loop,
        memory_review_loop,
        json_caller,
        prompt_registry,
        SessionStore::new(cfg.storage.agents_root.clone()),
        SessionEngineOptions {
            compaction: cfg.agent.session.compaction.clone(),
            skills: cfg.agent.session.skills.clone(),
            context_window: cfg.agent.llm.context_window,
            user_shell: cfg.agent.session.user_shell.clone(),
            workspace_root: cfg.agent.tool.workspace_root.clone(),
            turn_journal: cfg.agent.session.turn_journal.clone(),
            subagent_max_concurrent: cfg.agent.session.subagents.max_concurrent,
        },
    )
    .with_acn_md_path(cfg.storage.acn_md_path())
    .with_session_metadata("tui", cfg.agent.llm.model.clone())
    .with_session_search_sqlite_busy_timeout(std::time::Duration::from_millis(
        cfg.agent.tool.session_search_sqlite_busy_timeout_ms,
    ))
    .with_fork_memory_review_interval_turns(cfg.agent.session.memory_review.interval_turns)
    .with_attachment_config(cfg.agent.attachment.clone());
    if let Some(mcp_manager) = engine_mcp_manager {
        engine = engine.with_mcp_manager(mcp_manager);
    }
    Ok(engine)
}

/// 启动时刷新全局 MCP manager；失败只降级为 warning，避免阻塞 TUI。
pub async fn build_refreshed_mcp_manager(cfg: &Config) -> Arc<McpConnectionManager> {
    let manager = Arc::new(McpConnectionManager::new(
        cfg.storage.mcp_config_path(),
        cfg.agent.tool.workspace_root.clone(),
        None,
    ));
    if let Err(err) = manager.refresh_all().await {
        log::warn!(
            target: "bootstrap",
            "MCP 初始化失败，当前 session 将不暴露 MCP 工具: {err}"
        );
        manager.set_startup_error(err.to_string());
    }
    manager
}

pub(crate) fn build_provider_adapter(cfg: &Config) -> anyhow::Result<Arc<dyn ProviderAdapter>> {
    build_provider_adapter_for(&cfg.agent.llm, "agent.llm")
}

fn build_provider_adapter_for(
    chat: &LlmChatConfig,
    config_path: &str,
) -> anyhow::Result<Arc<dyn ProviderAdapter>> {
    match chat.provider {
        LlmProvider::Anthropic => {
            let key = chat
                .api_key
                .clone()
                .with_context(|| missing_loaded_api_key_message(chat, config_path))?;
            Ok(Arc::new(
                AnthropicProviderAdapter::new(
                    key,
                    chat.endpoint.clone(),
                    chat.model.clone(),
                    chat.max_tokens,
                    Duration::from_secs(chat.timeout_secs),
                    chat.retry_count,
                    Duration::from_millis(chat.retry_base_delay_ms),
                    Duration::from_millis(chat.retry_max_delay_ms),
                )?
                .with_reasoning_effort(chat.reasoning_effort),
            ))
        }
        LlmProvider::OpenAiCompatibleChat => {
            let key = chat
                .api_key
                .clone()
                .with_context(|| missing_loaded_api_key_message(chat, config_path))?;
            Ok(Arc::new(
                OpenAiCompatibleChatProviderAdapter::new(
                    key,
                    chat.endpoint.clone(),
                    chat.model.clone(),
                    Duration::from_secs(chat.timeout_secs),
                    chat.retry_count,
                    Duration::from_millis(chat.retry_base_delay_ms),
                    Duration::from_millis(chat.retry_max_delay_ms),
                )?
                .with_reasoning_effort(chat.reasoning_effort),
            ))
        }
    }
}

fn missing_loaded_api_key_message(chat: &LlmChatConfig, config_path: &str) -> String {
    if chat.api_key_env.trim().is_empty() {
        format!("[{config_path}].api_key_env 为空，无法读取 LLM provider API key")
    } else {
        format!(
            "[{config_path}].api_key_env '{}' 对应的环境变量为空，无法读取 LLM provider API key",
            chat.api_key_env
        )
    }
}

fn build_session_prompt_registry(cfg: &Config) -> anyhow::Result<Arc<PromptRegistry>> {
    let reg = PromptRegistry::from_config(&cfg.prompt).with_context(|| {
        let source = cfg
            .prompt
            .external_root()
            .map_or_else(|| "内置模板".to_string(), |root| root.display().to_string());
        format!("加载 session prompt 模板失败: {source}")
    })?;
    let source = reg.root().display().to_string();
    reg.validate_renderable(REQUIRED_SESSION_PROMPTS)
        .with_context(|| format!("渲染 session prompt 失败: {source}"))?;
    Ok(Arc::new(reg))
}

/// 构造 router 本体，供 daemon binary 使用。
pub fn build_router_service(cfg: &Config) -> Arc<Router> {
    let embedding_client = match build_embedding_client(&cfg.router.embedding) {
        Ok(client) => Some(client),
        Err(err) => {
            log::warn!(
                target: "bootstrap",
                "构造 router query embedding client 失败，降级为仅 lexical recall: {err:#}"
            );
            None
        }
    };
    let reranker = if cfg.router.retrieval.rerank_enabled {
        match crate::router::build_reranker(&cfg.router.rerank) {
            Ok(reranker) => reranker,
            Err(err) => {
                log::warn!(
                    target: "bootstrap",
                    "构造 router reranker 失败，降级为 heuristic rerank: {err:#}"
                );
                crate::router::default_reranker()
            }
        }
    } else {
        crate::router::default_reranker()
    };
    Arc::new(Router::with_dependencies(
        cfg.storage.team_root.clone(),
        cfg.router.retrieval.clone(),
        embedding_client,
        reranker,
    ))
}

/// 构造 maintainer 本体，供 daemon binary 使用。
pub fn build_maintainer_service(cfg: &Config) -> Arc<Maintainer> {
    Arc::new(Maintainer::with_history_store(
        cfg.storage.team_root.clone(),
        chrono::Duration::days(i64::from(cfg.maintainer.sweep.stale_after_days)),
        chrono::Duration::days(i64::from(cfg.maintainer.sweep.deprecated_after_days)),
        cfg.maintainer.id_mint_max_attempts(),
        crate::maintainer::history::HistoryStore::new(
            cfg.storage.team_root.clone(),
            cfg.maintainer.history.clone(),
        ),
    ))
}

pub fn build_maintainer_arbitration_service(
    cfg: &Config,
    maintainer: Arc<Maintainer>,
    router: Arc<dyn RouterClient>,
) -> anyhow::Result<Option<Arc<ArbitrationService>>> {
    if !cfg.maintainer.arbitration.enabled {
        return Ok(None);
    }
    let llm = cfg
        .maintainer
        .llm
        .as_ref()
        .context("maintainer arbitration 已启用，但缺少 [maintainer.llm]")?;
    let provider = build_provider_adapter_for(llm, "maintainer.llm")?;
    let caller = Arc::new(StructuredJsonCaller::new(
        provider,
        llm.max_tokens,
        llm.retry_count,
        Duration::from_millis(llm.retry_base_delay_ms),
        Duration::from_millis(llm.retry_max_delay_ms),
    ));
    let prompts = Arc::new(PromptRegistry::from_config(&cfg.prompt)?);
    prompts.validate_renderable(&[
        "maintainer_arbitration_proposal",
        "maintainer_arbitration_verification",
    ])?;
    let evaluator = Arc::new(LlmArbitrationEvaluator::new(
        caller,
        prompts,
        llm,
        cfg.maintainer.arbitration.confidence_threshold,
    )?);
    let store = ArbitrationStore::new(cfg.storage.team_root.clone());
    let context_builder = ArbitrationContextBuilder::new(
        store.clone(),
        router,
        cfg.maintainer.arbitration.clone(),
        llm.clone(),
    );
    let resolution_service = ResolutionService::new(maintainer, store.clone());
    Ok(Some(Arc::new(ArbitrationService::new(
        store,
        context_builder,
        evaluator,
        resolution_service,
        cfg.maintainer.arbitration.clone(),
        llm.model.clone(),
        phase_timeout(llm)?,
        cfg.maintainer.id_mint_max_attempts(),
        Arc::new(SystemArbitrationClock),
    ))))
}

pub fn spawn_router_vector_worker(
    cfg: &Config,
    cancel: CancellationToken,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let embedding = build_embedding_client(&cfg.router.embedding)?;
    let team_root = cfg.storage.team_root.clone();
    let max_concurrency = cfg.router.embedding.max_concurrency;
    let poll_interval = Duration::from_secs(cfg.router.retrieval.vector.worker_poll_secs);
    let retry_policy = VectorRetryPolicy::new(
        Duration::from_millis(cfg.router.retrieval.vector.retry_base_delay_ms),
        Duration::from_millis(cfg.router.retrieval.vector.retry_max_delay_ms),
    )?;
    Ok(tokio::spawn(async move {
        log::info!(
            target: "bootstrap",
            "{ROUTER_VECTOR_WORKER_TASK_NAME} 启动 poll_secs={} max_concurrency={}",
            poll_interval.as_secs(),
            max_concurrency
        );
        let result = run_vector_worker(
            team_root,
            embedding,
            max_concurrency,
            poll_interval,
            retry_policy,
            cancel,
        )
        .await;
        log::info!(
            target: "bootstrap",
            "{ROUTER_VECTOR_WORKER_TASK_NAME} 退出: {result:?}"
        );
        result
    }))
}

pub fn maybe_spawn_router_vector_worker(
    cfg: &Config,
    cancel: CancellationToken,
) -> Option<JoinHandle<anyhow::Result<()>>> {
    if !cfg.router.retrieval.enabled {
        return None;
    }
    match spawn_router_vector_worker(cfg, cancel) {
        Ok(worker) => Some(worker),
        Err(err) => {
            log::warn!(
                target: "bootstrap",
                "构造 router vector worker 失败，降级为仅 lexical/vector-query 无后台派生: {err:#}"
            );
            None
        }
    }
}

pub fn spawn_router_refresh_worker(
    cfg: &Config,
    router: Arc<Router>,
    cancel: CancellationToken,
) -> JoinHandle<anyhow::Result<()>> {
    let interval = Duration::from_secs(cfg.router.refresh_interval_secs);
    tokio::spawn(async move {
        log::info!(
            target: "bootstrap",
            "{ROUTER_REFRESH_WORKER_TASK_NAME} 启动 interval_secs={}",
            interval.as_secs(),
        );
        let result = run_refresh_worker(router, interval, cancel).await;
        log::info!(
            target: "bootstrap",
            "{ROUTER_REFRESH_WORKER_TASK_NAME} 退出: {result:?}"
        );
        result
    })
}

pub(crate) struct ProviderSessionSearchSummarizer {
    prompts: Arc<PromptRegistry>,
    json_caller: Arc<StructuredJsonCaller>,
}

impl ProviderSessionSearchSummarizer {
    pub(crate) fn new(
        prompts: Arc<PromptRegistry>,
        json_caller: Arc<StructuredJsonCaller>,
    ) -> Self {
        Self {
            prompts,
            json_caller,
        }
    }
}

#[async_trait::async_trait]
impl SessionSearchSummarizer for ProviderSessionSearchSummarizer {
    async fn summarize_session_search(
        &self,
        request: crate::api::SessionSearchSummaryRequest,
    ) -> anyhow::Result<crate::api::SessionSearchSummaryOutcome> {
        let system_prompt = self
            .prompts
            .render(PROMPT_SESSION_SEARCH_SUMMARY, ())
            .context("渲染 session_search_summary prompt 失败")?;
        let user_text = serde_json::to_string_pretty(&request)?;
        let value = self
            .json_caller
            .generate_json(
                system_prompt,
                vec![crate::api::SessionTurnMessage::user_text(user_text)],
            )
            .await?;
        serde_json::from_value(value).map_err(Into::into)
    }
}

fn build_agent_runner(
    cfg: &Config,
    agent_id: AgentId,
    team_clients: Option<(Arc<dyn RouterClient>, Arc<dyn MaintainerClient>)>,
    prompts: Arc<PromptRegistry>,
) -> anyhow::Result<AgentRunner> {
    let agent_home = cfg.agent_home(&agent_id);

    let claim_store: Arc<dyn LocalClaimStore> =
        Arc::new(LocalFsClaimStore::new(agent_home.clone()));
    let reported_dispute_claim_sets: Arc<dyn ReportedDisputeClaimSetStore> =
        Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone()));
    let inbox: Arc<dyn InboxReader> = Arc::new(
        LocalFsInboxReader::new(agent_home.clone())
            .with_processing_stale_after_secs(cfg.agent.inbox.processing_stale_after_secs),
    );
    let maintainer_upload_queue = Arc::new(LocalFsMaintainerUploadQueue::new(agent_home.clone()));
    let memory_store: Arc<dyn MemoryStore> = Arc::new(LocalFsMemoryStore::new(
        agent_home.clone(),
        cfg.agent.memory.memory_char_limit,
        cfg.agent.memory.user_char_limit,
        cfg.agent.memory.memory_safety_scan,
    ));
    let provider = build_provider_adapter(cfg)?;
    let chat = &cfg.agent.llm;
    let json_caller = Arc::new(StructuredJsonCaller::new(
        provider,
        chat.max_tokens,
        chat.retry_count,
        Duration::from_millis(chat.retry_base_delay_ms),
        Duration::from_millis(chat.retry_max_delay_ms),
    ));
    let inbox_generator = Arc::new(PromptInboxJsonGenerator::new(prompts, json_caller));

    let available_skills = load_available_skills(cfg)?;

    Ok(match team_clients {
        Some((router, maintainer)) => AgentRunner::new(
            agent_id,
            inbox_generator,
            claim_store,
            reported_dispute_claim_sets,
            inbox,
            memory_store,
            router,
            maintainer,
            maintainer_upload_queue,
            cfg.agent.llm.retry_count,
            available_skills,
        ),
        None => AgentRunner::new_local(
            agent_id,
            inbox_generator,
            claim_store,
            reported_dispute_claim_sets,
            inbox,
            memory_store,
            maintainer_upload_queue,
            cfg.agent.llm.retry_count,
            available_skills,
        ),
    })
}

fn load_available_skills(cfg: &Config) -> anyhow::Result<Vec<crate::skill::SkillSummary>> {
    let registry = SkillRegistry::new(cfg.storage.skills_root());
    let mut summaries = registry.summaries_sync().map_err(anyhow::Error::from)?;
    summaries.retain(|skill| skill.name != "consult_router");
    Ok(summaries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AgentConfig, ClientsConfig, DaemonConfig, EmbeddingConfig, LangfuseConfig, LlmChatConfig,
        MaintainerConfig, MaintainerIdConfig, MaintainerSweepConfig, MaintainerUiConfig,
        PromptConfig, RouterAuthConfig, RouterConfig, RouterRetrievalConfig, StorageConfig,
        UpstreamConfig, DEFAULT_MAINTAINER_ENDPOINT, DEFAULT_MAINTAINER_LISTEN,
        DEFAULT_ROUTER_ENDPOINT, DEFAULT_ROUTER_LISTEN,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn cfg(team: PathBuf, hosts: PathBuf, prompts: PathBuf) -> Config {
        let acn_home = team.join("_acn_home");
        let workspace_root = PathBuf::from(".");
        let mut upstreams = BTreeMap::new();
        upstreams.insert(
            "dev".to_string(),
            UpstreamConfig {
                agent_id: "agent-a".to_string(),
                maintainer_endpoint: DEFAULT_MAINTAINER_ENDPOINT.to_string(),
                router_endpoint: DEFAULT_ROUTER_ENDPOINT.to_string(),
                acn_key_env: None,
            },
        );
        Config {
            upstream: "dev".to_string(),
            upstreams,
            storage: StorageConfig {
                acn_home,
                base_acn_home: team.join("_acn_home"),
                team_root: team,
                agents_root: hosts,
            },
            router: RouterConfig {
                refresh_interval_secs: 5,
                daemon: DaemonConfig {
                    listen: DEFAULT_ROUTER_LISTEN.into(),
                },
                auth: RouterAuthConfig::default(),
                retrieval: RouterRetrievalConfig::default(),
                embedding: EmbeddingConfig::default(),
                rerank: crate::config::RouterRerankConfig::default(),
            },
            maintainer: MaintainerConfig {
                sweep: MaintainerSweepConfig {
                    tick_interval_secs: 3600, // 不希望测试期间真触发
                    stale_after_days: 7,
                    deprecated_after_days: 30,
                },
                daemon: DaemonConfig {
                    listen: DEFAULT_MAINTAINER_LISTEN.into(),
                },
                history: crate::config::MaintainerHistoryConfig::default(),
                ui: MaintainerUiConfig {
                    frontend_dist_dir: PathBuf::from("./frontend/maintainer-workbench/dist"),
                },
                id: MaintainerIdConfig {
                    mint_max_retries: crate::config::DEFAULT_ID_MINT_MAX_RETRIES,
                },
                auth: Default::default(),
                arbitration: Default::default(),
                llm: None,
            },
            agent: AgentConfig {
                llm: LlmChatConfig {
                    provider: LlmProvider::Anthropic,
                    endpoint: "http://127.0.0.1:1".into(),
                    model: "test-model".into(),
                    reasoning_effort: crate::config::ReasoningEffort::None,
                    api_key_env: "ANTHROPIC_API_KEY".into(),
                    max_tokens: 1024,
                    context_window: crate::config::DEFAULT_LLM_CONTEXT_WINDOW,
                    timeout_secs: 600,
                    retry_count: 1,
                    retry_base_delay_ms: 200,
                    retry_max_delay_ms: 5000,
                    api_key: Some("test-key".into()),
                },
                tool: crate::config::ToolConfig {
                    workspace_root,
                    ..Default::default()
                },
                ..Default::default()
            },
            clients: ClientsConfig::default(),
            prompt: PromptConfig {
                root: Some(prompts),
            },
            langfuse: LangfuseConfig {
                enabled: false,
                endpoint: "http://localhost:3000/api/public/otel".into(),
                service_name: "agent_claim_network".into(),
                public_key: None,
                secret_key: None,
            },
        }
    }

    fn write_session_prompts(root: &std::path::Path) {
        for name in REQUIRED_SESSION_PROMPTS {
            std::fs::write(root.join(format!("{name}.j2")), format!("test {name}")).unwrap();
        }
    }

    #[test]
    fn anthropic_provider_without_key_errors() {
        let team = tempfile::tempdir().unwrap();
        let hosts = tempfile::tempdir().unwrap();
        let prompts = tempfile::tempdir().unwrap();
        write_session_prompts(prompts.path());
        let mut c = cfg(
            team.path().to_path_buf(),
            hosts.path().to_path_buf(),
            prompts.path().to_path_buf(),
        );
        c.agent.llm.provider = LlmProvider::Anthropic;
        c.agent.llm.api_key_env = "ANTHROPIC_API_KEY".into();
        c.agent.llm.api_key = None;
        let upstream = c.resolve_upstream(None).unwrap();
        let res = build_agent_cli_session_engine(&c, &upstream);
        let err = match res {
            Ok(_) => panic!("anthropic 缺 key 不应构造成功"),
            Err(e) => e,
        };
        assert!(err
            .to_string()
            .contains("[agent.llm].api_key_env 'ANTHROPIC_API_KEY'"));
    }

    #[test]
    fn anthropic_provider_missing_required_prompt_errors_at_build() {
        let team = tempfile::tempdir().unwrap();
        let hosts = tempfile::tempdir().unwrap();
        let prompts = tempfile::tempdir().unwrap();
        std::fs::write(prompts.path().join("agent_system.j2"), "test system").unwrap();
        let mut c = cfg(
            team.path().to_path_buf(),
            hosts.path().to_path_buf(),
            prompts.path().to_path_buf(),
        );
        c.agent.llm.provider = LlmProvider::Anthropic;
        c.agent.llm.api_key = Some("test-key".into());
        let upstream = c.resolve_upstream(None).unwrap();
        let res = build_agent_cli_session_engine(&c, &upstream);
        let err = match res {
            Ok(_) => panic!("anthropic 缺必需 prompt 不应构造成功"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("渲染 session prompt 失败"));
    }

    #[test]
    fn openai_compatible_chat_provider_builds_http_cli_session_engine() {
        let team = tempfile::tempdir().unwrap();
        let hosts = tempfile::tempdir().unwrap();
        let prompts = tempfile::tempdir().unwrap();
        write_session_prompts(prompts.path());
        let mut c = cfg(
            team.path().to_path_buf(),
            hosts.path().to_path_buf(),
            prompts.path().to_path_buf(),
        );
        c.agent.llm.provider = LlmProvider::OpenAiCompatibleChat;
        c.agent.llm.api_key_env = "EXAMPLE_LLM_API_KEY".into();
        c.agent.llm.api_key = Some("test-key".into());
        c.agent.llm.endpoint = "http://127.0.0.1:1".into();
        c.agent.llm.model = "test-chat-model".into();

        let upstream = c.resolve_upstream(None).unwrap();
        let engine = build_agent_cli_session_engine(&c, &upstream);

        assert!(engine.is_ok());
    }

    #[test]
    fn provider_rejects_invalid_endpoint_at_build() {
        let team = tempfile::tempdir().unwrap();
        let hosts = tempfile::tempdir().unwrap();
        let prompts = tempfile::tempdir().unwrap();
        write_session_prompts(prompts.path());
        let mut c = cfg(
            team.path().to_path_buf(),
            hosts.path().to_path_buf(),
            prompts.path().to_path_buf(),
        );

        for (provider, expected_error) in [
            (LlmProvider::Anthropic, "Anthropic Messages endpoint"),
            (
                LlmProvider::OpenAiCompatibleChat,
                "Chat Completions endpoint",
            ),
        ] {
            c.agent.llm.provider = provider;
            c.agent.llm.endpoint = "llm.example.com/v1".into();
            let error = build_provider_adapter(&c)
                .err()
                .expect("相对 endpoint 不应构造 provider");
            assert!(error.to_string().contains(expected_error));
            assert!(error.to_string().contains("不是有效的绝对 URL"));
        }
    }

    #[test]
    fn solo_mode_builds_runner_without_team_clients() {
        let team = tempfile::tempdir().unwrap();
        let hosts = tempfile::tempdir().unwrap();
        let prompts = tempfile::tempdir().unwrap();
        write_session_prompts(prompts.path());
        let mut c = cfg(
            team.path().to_path_buf(),
            hosts.path().to_path_buf(),
            prompts.path().to_path_buf(),
        );
        c.upstreams
            .get_mut("dev")
            .unwrap()
            .maintainer_endpoint
            .clear();
        c.upstreams.get_mut("dev").unwrap().router_endpoint.clear();
        let upstream = c.resolve_upstream(None).unwrap();

        let runner = build_agent_cli_runner(&c, &upstream).unwrap();
        let context = runner.context();

        assert!(context.maintainer_client.is_none());
        assert!(context.router.is_none());
    }

    #[test]
    fn load_available_skills_filters_consult_router_skill_from_acn_home() {
        let team = tempfile::tempdir().unwrap();
        let hosts = tempfile::tempdir().unwrap();
        let prompts = tempfile::tempdir().unwrap();
        let acn_home = tempfile::tempdir().unwrap();
        let skills_root = acn_home.path().join("skills");
        std::fs::create_dir_all(skills_root.join("consult_router")).unwrap();
        std::fs::create_dir_all(skills_root.join("handle_policy_update")).unwrap();
        std::fs::write(
            skills_root.join("consult_router").join("SKILL.md"),
            "# consult_router\n",
        )
        .unwrap();
        std::fs::write(
            skills_root.join("handle_policy_update").join("SKILL.md"),
            "# handle_policy_update\n",
        )
        .unwrap();

        let mut c = cfg(
            team.path().to_path_buf(),
            hosts.path().to_path_buf(),
            prompts.path().to_path_buf(),
        );
        c.storage.acn_home = acn_home.path().to_path_buf();
        c.storage.team_root = acn_home.path().join("data").join("team");
        c.storage.agents_root = acn_home.path().join("data").join("agents");

        let skills = load_available_skills(&c).unwrap();
        let names: Vec<_> = skills.into_iter().map(|skill| skill.name).collect();
        assert_eq!(names, vec!["handle_policy_update".to_string()]);
    }
}
