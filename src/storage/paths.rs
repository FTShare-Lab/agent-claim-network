//! 文件路径工具。
//!
//! 集中维护 `team_root` 与 `agents_root` 下的目录约定。
//! 业务代码不要自己 `team_root.join(...)`——一律走本模块函数，便于将来切换实现时统一审查。

use std::path::{Path, PathBuf};

use ring::digest::{digest, SHA256};

use crate::claim::{AgentId, ClaimId, SessionId};

/// `<base_acn_home>/runtime/file-write-locks/`：协作 ACN 进程共享的文件写锁目录。
pub fn base_acn_home_file_write_locks_dir(base_acn_home: &Path) -> PathBuf {
    base_acn_home.join("runtime").join("file-write-locks")
}

/// 按文件工具使用的稳定路径 key 派生跨进程写锁；锁名不暴露工作区路径。
pub fn file_write_lock_path(lock_root: &Path, stable_path_key: &Path) -> PathBuf {
    let hash = hex::encode(digest(
        &SHA256,
        stable_path_key.as_os_str().as_encoded_bytes(),
    ));
    lock_root.join(format!("{hash}.lock"))
}

// ---- team store 中心存储（router / maintainer 视角） ----

/// `team_root/agents/<agent_id>/claims/`：某 agent 同步上来的 claim 镜像目录
pub fn team_store_agent_claims_dir(team_root: &Path, agent: &AgentId) -> PathBuf {
    team_store_agent_dir(team_root, agent).join("claims")
}

/// `team_root/agents/<agent_id>/claims/.locks/<claim_id>.lock`：Claim 镜像写入与 Router
/// 检索发布共用的 per-claim 协调锁。
pub fn team_store_agent_claim_mirror_lock_path(
    team_root: &Path,
    agent: &AgentId,
    claim_id: &ClaimId,
) -> PathBuf {
    team_store_agent_claims_dir(team_root, agent)
        .join(".locks")
        .join(format!("{claim_id}.lock"))
}

pub fn team_store_router_dir(team_root: &Path) -> PathBuf {
    team_root.join("router")
}

pub fn team_store_router_retrieval_dir(team_root: &Path) -> PathBuf {
    team_store_router_dir(team_root).join("retrieval")
}

/// `team_root/router/derived_views.yaml`：claim index 与 scope overview 的同代快照。
pub fn team_store_router_derived_views_path(team_root: &Path) -> PathBuf {
    team_store_router_dir(team_root).join("derived_views.yaml")
}

/// `team_root/router/derived_views.lock`：跨 Router 实例刷新同代快照的独占锁。
pub fn team_store_router_derived_views_lock_path(team_root: &Path) -> PathBuf {
    team_store_router_dir(team_root).join("derived_views.lock")
}

/// router 运行时派生的检索文档目录。
pub fn team_store_router_retrieval_docs_dir(team_root: &Path) -> PathBuf {
    team_store_router_retrieval_dir(team_root).join("docs")
}

/// `team_root/router/retrieval/docs/<claim_id>.yaml`：单条 claim 的 lexical 检索文档。
pub fn team_store_router_retrieval_doc_path(team_root: &Path, claim_id: &ClaimId) -> PathBuf {
    team_store_router_retrieval_docs_dir(team_root).join(format!("{claim_id}.yaml"))
}

/// `team_root/router/retrieval/vector_queue/`：router 自管的 embedding 队列目录。
pub fn team_store_router_vector_queue_dir(team_root: &Path) -> PathBuf {
    team_store_router_retrieval_dir(team_root).join("vector_queue")
}

/// `team_root/router/retrieval/vector_queue/pending.jsonl`：待处理的 embedding 工作队列。
pub fn team_store_router_vector_queue_path(team_root: &Path) -> PathBuf {
    team_store_router_vector_queue_dir(team_root).join("pending.jsonl")
}

/// `team_root/router/retrieval/vector_queue/queue.lock`：向量队列跨进程锁。
pub fn team_store_router_vector_queue_lock_path(team_root: &Path) -> PathBuf {
    team_store_router_vector_queue_dir(team_root).join("queue.lock")
}

/// `team_root/router/retrieval/vector_state/`：每条 claim 的向量状态目录。
pub fn team_store_router_vector_state_dir(team_root: &Path) -> PathBuf {
    team_store_router_retrieval_dir(team_root).join("vector_state")
}

/// `team_root/router/retrieval/vector_state/<claim_id>.json`：单条 claim 的向量状态。
pub fn team_store_router_vector_state_path(team_root: &Path, claim_id: &ClaimId) -> PathBuf {
    team_store_router_vector_state_dir(team_root).join(format!("{claim_id}.json"))
}

/// 单 claim 向量状态跨进程锁路径。
pub fn team_store_router_vector_state_lock_path(team_root: &Path, claim_id: &ClaimId) -> PathBuf {
    team_store_router_vector_state_dir(team_root)
        .join(".locks")
        .join(format!("{claim_id}.lock"))
}

/// `team_root/router/retrieval/vector_intents/`：Vector target 发布失败后的恢复意图目录。
pub fn team_store_router_vector_intents_dir(team_root: &Path) -> PathBuf {
    team_store_router_retrieval_dir(team_root).join("vector_intents")
}

/// `team_root/router/retrieval/vector_intents/<claim_id>.json`：单条 claim 的待恢复 Vector target。
pub fn team_store_router_vector_intent_path(team_root: &Path, claim_id: &ClaimId) -> PathBuf {
    team_store_router_vector_intents_dir(team_root).join(format!("{claim_id}.json"))
}

pub fn team_store_disputes_dir(team_root: &Path) -> PathBuf {
    team_root.join("maintainer").join("disputes")
}

pub fn team_store_arbitrations_dir(team_root: &Path) -> PathBuf {
    team_root.join("maintainer").join("arbitrations")
}

/// `team_root/maintainer/arbitrations/semantic-inputs.lock`：Claim、治理 Policy、
/// Dispute 与 Resolution 写入同自动采用最终复核之间的跨进程协调锁。
pub fn team_store_arbitration_semantic_inputs_lock_path(team_root: &Path) -> PathBuf {
    team_store_arbitrations_dir(team_root).join("semantic-inputs.lock")
}

/// `team_root/maintainer/arbitrations/semantic-inputs-revision.yaml`：所有会改变
/// 仲裁语义输入的写入在持锁时递增，用于 Adopt 的乐观两阶段复核。
pub fn team_store_arbitration_semantic_inputs_revision_path(team_root: &Path) -> PathBuf {
    team_store_arbitrations_dir(team_root).join("semantic-inputs-revision.yaml")
}

pub fn team_store_arbitration_dispute_dir(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitrations_dir(team_root).join(dispute_id.as_str())
}

/// 每个 Dispute 唯一的当前 Analysis。再次 Analyze 会原子覆盖该文件。
pub fn team_store_arbitration_current_analysis_path(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitration_dispute_dir(team_root, dispute_id).join("analysis.yaml")
}

/// 旧版本自动分析路径，只用于读取兼容。
pub fn team_store_arbitration_legacy_automatic_analysis_path(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitration_dispute_dir(team_root, dispute_id).join("automatic_analysis.yaml")
}

/// 旧版本人工分析路径，只用于读取兼容。
pub fn team_store_arbitration_legacy_manual_analysis_path(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitration_dispute_dir(team_root, dispute_id).join("manual_analysis.yaml")
}

pub fn team_store_arbitration_resolution_path(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitration_dispute_dir(team_root, dispute_id).join("resolution.yaml")
}

/// 旧版本 resolution 记录使用的只读兼容目录；新写入不再使用。
pub fn team_store_arbitration_legacy_decisions_dir(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitration_dispute_dir(team_root, dispute_id).join("decisions")
}

pub fn team_store_arbitration_pending_deliveries_dir(team_root: &Path) -> PathBuf {
    team_store_arbitrations_dir(team_root).join("pending-deliveries")
}

pub fn team_store_arbitration_pending_delivery_path(
    team_root: &Path,
    resolution_id: &crate::claim::ArbitrationResolutionId,
) -> PathBuf {
    team_store_arbitration_pending_deliveries_dir(team_root).join(format!("{resolution_id}.yaml"))
}

pub fn team_store_arbitration_pending_observations_dir(team_root: &Path) -> PathBuf {
    team_store_arbitrations_dir(team_root).join("pending-observations")
}

pub fn team_store_arbitration_pending_observation_path(
    team_root: &Path,
    resolution_id: &crate::claim::ArbitrationResolutionId,
) -> PathBuf {
    team_store_arbitration_pending_observations_dir(team_root).join(format!("{resolution_id}.yaml"))
}

pub fn team_store_arbitration_event_inbox_index_path(
    team_root: &Path,
    inbox_id: &crate::claim::InboxId,
) -> PathBuf {
    team_store_arbitrations_dir(team_root)
        .join("event-index")
        .join("inboxes")
        .join(format!("{inbox_id}.yaml"))
}

pub fn team_store_arbitration_event_claim_index_dir(
    team_root: &Path,
    claim_id: &crate::claim::ClaimId,
) -> PathBuf {
    team_store_arbitrations_dir(team_root)
        .join("event-index")
        .join("claims")
        .join(claim_id.as_str())
}

pub fn team_store_arbitration_observations_dir(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitration_dispute_dir(team_root, dispute_id).join("observations")
}

pub fn team_store_arbitration_lock_path(
    team_root: &Path,
    dispute_id: &crate::claim::DisputeId,
) -> PathBuf {
    team_store_arbitration_dispute_dir(team_root, dispute_id).join("arbitration.lock")
}

pub fn team_store_policies_dir(team_root: &Path) -> PathBuf {
    team_root.join("maintainer").join("policies")
}

/// `team_root/maintainer/auth_keys.yaml`：团队 API key 台账。
pub fn team_store_auth_keys_path(team_root: &Path) -> PathBuf {
    team_root.join("maintainer").join("auth_keys.yaml")
}

/// `team_root/maintainer/service_keys/router_service_acn_key`：maintainer 调 router 的私有明文 key。
pub fn team_store_router_service_key_path(team_root: &Path) -> PathBuf {
    team_root
        .join("maintainer")
        .join("service_keys")
        .join("router_service_acn_key")
}

/// `team_root/maintainer/outbox/`：新模型下的待投递台账目录
pub fn team_store_outbox_dir(team_root: &Path) -> PathBuf {
    team_root.join("maintainer").join("outbox")
}

/// `team_root/maintainer/outbox.lock`：跨 Maintainer 进程协调 outbox 读改写与 action ID 生成。
pub fn team_store_outbox_lock_path(team_root: &Path) -> PathBuf {
    team_root.join("maintainer").join("outbox.lock")
}

pub fn team_store_maintainer_history_stream_dir(team_root: &Path, stream: &str) -> PathBuf {
    team_root.join("maintainer").join("history").join(stream)
}

pub fn team_store_maintainer_history_current_path(team_root: &Path, stream: &str) -> PathBuf {
    team_store_maintainer_history_stream_dir(team_root, stream).join("current.jsonl")
}

/// team store 上的 agents 父目录，router 用于扫描所有 agent 的 claims 镜像
pub fn team_store_agents_root(team_root: &Path) -> PathBuf {
    team_root.join("agents")
}

/// `team_root/agents/<agent_id>/`：maintainer 视角的 agent 注册目录
pub fn team_store_agent_dir(team_root: &Path, agent: &AgentId) -> PathBuf {
    team_store_agents_root(team_root).join(agent.as_str())
}

// ---- agent 本地存储（每个 agent 视角，仅看自己） ----

/// `<runtime_acn_home>/data/agents/<agent_id>/`：指定 upstream 的 agent 本地目录。
pub fn runtime_agent_home(runtime_acn_home: &Path, agent: &AgentId) -> PathBuf {
    runtime_agents_root(runtime_acn_home).join(agent.as_str())
}

/// `<runtime_acn_home>/data/agents/`：指定 upstream 的 agent 本地目录根。
pub fn runtime_agents_root(runtime_acn_home: &Path) -> PathBuf {
    runtime_acn_home.join("data").join("agents")
}

pub fn agent_home_claims_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("claims")
}

pub fn agent_home_traces_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("traces")
}

pub fn agent_home_disputes_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("disputes")
}

pub fn agent_home_maintainer_uploads_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("maintainer_uploads")
}

pub fn agent_home_pending_maintainer_uploads_path(agent_home: &Path) -> PathBuf {
    agent_home_maintainer_uploads_dir(agent_home).join("pending.yaml")
}

pub fn agent_home_pending_maintainer_uploads_lock_path(agent_home: &Path) -> PathBuf {
    agent_home_maintainer_uploads_dir(agent_home).join("pending.lock")
}

pub fn agent_home_runtime_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("runtime")
}

pub fn agent_home_supervisor_dir(agent_home: &Path) -> PathBuf {
    agent_home_runtime_dir(agent_home).join("supervisor")
}

pub fn agent_home_supervisor_jobs_dir(agent_home: &Path) -> PathBuf {
    agent_home_supervisor_dir(agent_home).join("jobs")
}

pub fn agent_home_supervisor_pid_path(agent_home: &Path) -> PathBuf {
    agent_home_supervisor_dir(agent_home).join("supervisor.pid")
}

pub fn agent_home_supervisor_launch_lock_path(agent_home: &Path) -> PathBuf {
    agent_home_supervisor_dir(agent_home).join("launch.lock")
}

pub fn agent_home_reported_dispute_claim_sets_path(agent_home: &Path) -> PathBuf {
    agent_home_disputes_dir(agent_home).join("reported_claim_sets.yaml")
}

pub fn agent_home_inbox_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("inbox")
}

pub fn agent_home_inbox_effects_dir(agent_home: &Path) -> PathBuf {
    agent_home_inbox_dir(agent_home).join("effects")
}

pub fn agent_home_inbox_effect_path(
    agent_home: &Path,
    inbox_id: &crate::claim::InboxId,
) -> PathBuf {
    agent_home_inbox_effects_dir(agent_home).join(format!("{inbox_id}.yaml"))
}

pub fn agent_home_memories_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("memories")
}

pub fn agent_home_sessions_dir(agent_home: &Path) -> PathBuf {
    agent_home.join("sessions")
}

pub fn agent_home_session_search_index_path(agent_home: &Path) -> PathBuf {
    agent_home.join("session_search_index.sqlite")
}

pub fn agent_home_session_cleanup_lock_path(agent_home: &Path) -> PathBuf {
    agent_home_runtime_dir(agent_home).join("session-cleanup.lock")
}

pub fn agent_home_session_cleanup_marker_path(agent_home: &Path) -> PathBuf {
    agent_home_runtime_dir(agent_home).join("session-cleanup.last-run")
}

pub fn agent_home_session_dir(agent_home: &Path, session: &SessionId) -> PathBuf {
    agent_home_sessions_dir(agent_home).join(session.as_str())
}

pub fn agent_home_memory_path(agent_home: &Path) -> PathBuf {
    agent_home_memories_dir(agent_home).join("MEMORY.md")
}

pub fn agent_home_user_memory_path(agent_home: &Path) -> PathBuf {
    agent_home_memories_dir(agent_home).join("USER.md")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn file_write_lock_path_uses_base_runtime_and_hides_workspace_path() {
        let base = PathBuf::from("/tmp/acn");
        let root = base_acn_home_file_write_locks_dir(&base);
        let lock = file_write_lock_path(&root, Path::new("/workspace/private/note.txt"));

        assert_eq!(root, PathBuf::from("/tmp/acn/runtime/file-write-locks"));
        assert_eq!(lock.parent(), Some(root.as_path()));
        let name = lock.file_name().unwrap().to_string_lossy();
        assert_eq!(name.len(), 64 + ".lock".len());
        assert!(name.ends_with(".lock"));
        assert!(!name.contains("note"));
    }

    #[test]
    fn team_store_paths_compose_correctly() {
        let root = PathBuf::from("/tmp/team");
        let agent = AgentId::new("agent-b").unwrap();
        assert_eq!(
            team_store_agent_claims_dir(&root, &agent),
            PathBuf::from("/tmp/team/agents/agent-b/claims")
        );
        let claim_id = ClaimId::from_str("claim_deadbeef").unwrap();
        assert_eq!(
            team_store_agent_claim_mirror_lock_path(&root, &agent, &claim_id),
            PathBuf::from("/tmp/team/agents/agent-b/claims/.locks/claim_deadbeef.lock")
        );
        assert_eq!(
            team_store_agent_dir(&root, &agent),
            PathBuf::from("/tmp/team/agents/agent-b")
        );
        assert_eq!(
            team_store_router_derived_views_path(&root),
            PathBuf::from("/tmp/team/router/derived_views.yaml")
        );
        assert_eq!(
            team_store_router_derived_views_lock_path(&root),
            PathBuf::from("/tmp/team/router/derived_views.lock")
        );
        assert_eq!(
            team_store_disputes_dir(&root),
            PathBuf::from("/tmp/team/maintainer/disputes")
        );
        assert_eq!(
            team_store_arbitration_semantic_inputs_lock_path(&root),
            PathBuf::from("/tmp/team/maintainer/arbitrations/semantic-inputs.lock")
        );
        assert_eq!(
            team_store_arbitration_semantic_inputs_revision_path(&root),
            PathBuf::from("/tmp/team/maintainer/arbitrations/semantic-inputs-revision.yaml")
        );
        assert_eq!(
            team_store_auth_keys_path(&root),
            PathBuf::from("/tmp/team/maintainer/auth_keys.yaml")
        );
        assert_eq!(
            team_store_outbox_dir(&root),
            PathBuf::from("/tmp/team/maintainer/outbox")
        );
        assert_eq!(
            team_store_outbox_lock_path(&root),
            PathBuf::from("/tmp/team/maintainer/outbox.lock")
        );
        assert_eq!(
            team_store_maintainer_history_stream_dir(&root, "policy_events"),
            PathBuf::from("/tmp/team/maintainer/history/policy_events")
        );
        assert_eq!(
            team_store_maintainer_history_current_path(&root, "policy_events"),
            PathBuf::from("/tmp/team/maintainer/history/policy_events/current.jsonl")
        );
        assert_eq!(
            team_store_maintainer_history_stream_dir(&root, "sweep_runs"),
            PathBuf::from("/tmp/team/maintainer/history/sweep_runs")
        );
        assert_eq!(
            team_store_maintainer_history_stream_dir(&root, "router_query_audit_logs"),
            PathBuf::from("/tmp/team/maintainer/history/router_query_audit_logs")
        );
    }

    #[test]
    fn agent_home_paths_compose_correctly() {
        let home = PathBuf::from("/tmp/agents/agent-a");
        assert_eq!(
            agent_home_claims_dir(&home),
            PathBuf::from("/tmp/agents/agent-a/claims")
        );
        assert_eq!(
            agent_home_inbox_dir(&home),
            PathBuf::from("/tmp/agents/agent-a/inbox")
        );
        assert_eq!(
            agent_home_reported_dispute_claim_sets_path(&home),
            PathBuf::from("/tmp/agents/agent-a/disputes/reported_claim_sets.yaml")
        );
        assert_eq!(
            agent_home_pending_maintainer_uploads_path(&home),
            PathBuf::from("/tmp/agents/agent-a/maintainer_uploads/pending.yaml")
        );
        assert_eq!(
            agent_home_pending_maintainer_uploads_lock_path(&home),
            PathBuf::from("/tmp/agents/agent-a/maintainer_uploads/pending.lock")
        );
        assert_eq!(
            agent_home_supervisor_jobs_dir(&home),
            PathBuf::from("/tmp/agents/agent-a/runtime/supervisor/jobs")
        );
        assert_eq!(
            agent_home_supervisor_pid_path(&home),
            PathBuf::from("/tmp/agents/agent-a/runtime/supervisor/supervisor.pid")
        );
        assert_eq!(
            agent_home_supervisor_launch_lock_path(&home),
            PathBuf::from("/tmp/agents/agent-a/runtime/supervisor/launch.lock")
        );
        assert_eq!(
            agent_home_memory_path(&home),
            PathBuf::from("/tmp/agents/agent-a/memories/MEMORY.md")
        );
        assert_eq!(
            agent_home_user_memory_path(&home),
            PathBuf::from("/tmp/agents/agent-a/memories/USER.md")
        );
        assert_eq!(
            agent_home_session_search_index_path(&home),
            PathBuf::from("/tmp/agents/agent-a/session_search_index.sqlite")
        );
        assert_eq!(
            agent_home_session_cleanup_lock_path(&home),
            PathBuf::from("/tmp/agents/agent-a/runtime/session-cleanup.lock")
        );
        assert_eq!(
            agent_home_session_cleanup_marker_path(&home),
            PathBuf::from("/tmp/agents/agent-a/runtime/session-cleanup.last-run")
        );
        let session = SessionId::from_str("session_1234abcd").unwrap();
        assert_eq!(
            agent_home_session_dir(&home, &session),
            PathBuf::from("/tmp/agents/agent-a/sessions/session_1234abcd")
        );
    }

    #[test]
    fn router_retrieval_paths_compose_correctly() {
        let root = PathBuf::from("/tmp/team");
        let claim_id = ClaimId::random();
        assert_eq!(
            team_store_router_retrieval_docs_dir(&root),
            PathBuf::from("/tmp/team/router/retrieval/docs")
        );
        assert_eq!(
            team_store_router_retrieval_doc_path(&root, &claim_id),
            PathBuf::from(format!("/tmp/team/router/retrieval/docs/{claim_id}.yaml"))
        );
        assert_eq!(
            team_store_router_vector_queue_path(&root),
            PathBuf::from("/tmp/team/router/retrieval/vector_queue/pending.jsonl")
        );
        assert_eq!(
            team_store_router_vector_queue_lock_path(&root),
            PathBuf::from("/tmp/team/router/retrieval/vector_queue/queue.lock")
        );
        assert_eq!(
            team_store_router_vector_state_path(&root, &claim_id),
            PathBuf::from(format!(
                "/tmp/team/router/retrieval/vector_state/{claim_id}.json"
            ))
        );
        assert_eq!(
            team_store_router_vector_state_lock_path(&root, &claim_id),
            PathBuf::from(format!(
                "/tmp/team/router/retrieval/vector_state/.locks/{claim_id}.lock"
            ))
        );
        assert_eq!(
            team_store_router_vector_intents_dir(&root),
            PathBuf::from("/tmp/team/router/retrieval/vector_intents")
        );
        assert_eq!(
            team_store_router_vector_intent_path(&root, &claim_id),
            PathBuf::from(format!(
                "/tmp/team/router/retrieval/vector_intents/{claim_id}.json"
            ))
        );
    }
}
