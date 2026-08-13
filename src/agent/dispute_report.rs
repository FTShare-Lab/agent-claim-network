//! agent 侧 dispute 上报幂等保护。
//!
//! LLM 仍可在 session recap / inbox 内化中提出 dispute；本模块只在真正上报
//! maintainer 前，用本 agent 本地台账过滤已成功上报过的 claim 组合。
//!
//! 当前幂等临界区覆盖单个 `AgentRunner` 实例。部署约束仍是同一 agent home 同时只由
//! 一个 agent 进程写入；如果未来允许多进程共享同一 agent home，需要把这里升级为文件锁
//! 或 pending/sent 两阶段台账。

use chrono::Utc;

use super::runner::AgentRunner;
use crate::claim::Dispute;

impl AgentRunner {
    /// 判断当前 agent 是否已经见过同一组 claim 的 dispute。
    pub(super) async fn dispute_claim_set_reported(
        &self,
        dispute: &Dispute,
    ) -> anyhow::Result<bool> {
        let _guard = self.dispute_report_lock.lock().await;
        self.reported_dispute_claim_sets
            .contains_claim_set(&dispute.claims)
            .await
    }

    /// 若当前 agent 未见过同一组 claim，则立即记录本地台账并返回 true。
    ///
    /// 调用方应先确保 dispute 已被 maintainer 接收或写入 durable pending 队列；
    /// 否则进程崩溃会留下"本地已报告、远端未排队"的不一致状态。
    pub(super) async fn record_dispute_if_new(&self, dispute: &Dispute) -> anyhow::Result<bool> {
        let _guard = self.dispute_report_lock.lock().await;
        if self
            .reported_dispute_claim_sets
            .contains_claim_set(&dispute.claims)
            .await?
        {
            log::info!(
                target: "agent",
                "agent {} 跳过重复 dispute 上报 id={} claims={:?}",
                self.agent_id,
                dispute.id,
                dispute.claims
            );
            return Ok(false);
        }

        self.reported_dispute_claim_sets
            .record_claim_set(&dispute.claims, &dispute.id, Utc::now())
            .await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::Value;

    use super::*;
    use crate::agent::fs::{
        LocalFsClaimStore, LocalFsInboxReader, LocalFsMemoryStore,
        LocalFsReportedDisputeClaimSetStore,
    };
    use crate::agent::inbox::InboxJsonGenerator;
    use crate::agent::maintainer_upload::LocalFsMaintainerUploadQueue;
    use crate::agent::traits::{
        InboxReader, LocalClaimStore, MemoryStore, ReportedDisputeClaimSetStore,
    };
    use crate::api::{InboxInternalizeKind, InternalizeRequest};
    use crate::claim::{AgentId, Claim, ClaimId, DisputeId, DisputeStatus, InboxId, InboxMessage};
    use crate::maintainer::traits::MaintainerClient;
    use crate::router::{AgentQuery, RouterClient, RouterQueryResult, ScopesOverviewSnapshot};
    use crate::skill::SkillSummary;

    struct UnusedInboxJsonGenerator;

    #[async_trait]
    impl InboxJsonGenerator for UnusedInboxJsonGenerator {
        async fn generate_json(
            &self,
            _kind: InboxInternalizeKind,
            _request: InternalizeRequest,
            _preferred_transport: Option<crate::api::ProviderTransport>,
        ) -> anyhow::Result<Value> {
            Ok(serde_json::json!({}))
        }
    }

    #[derive(Default)]
    struct RecordingMaintainerClient {
        disputes: Mutex<Vec<DisputeId>>,
    }

    #[async_trait]
    impl MaintainerClient for RecordingMaintainerClient {
        async fn pull_inbox(&self, _agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
            Ok(Vec::new())
        }

        async fn ack_inbox(
            &self,
            _agent_id: &AgentId,
            _inbox_ids: &[InboxId],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn upload_claim(&self, _claim: &Claim) -> anyhow::Result<()> {
            Ok(())
        }

        async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<()> {
            self.disputes.lock().unwrap().push(dispute.id.clone());
            Ok(())
        }
    }

    struct EmptyRouterClient;

    #[async_trait]
    impl RouterClient for EmptyRouterClient {
        async fn query(&self, _agent_query: &AgentQuery) -> anyhow::Result<RouterQueryResult> {
            Ok(RouterQueryResult {
                candidate_claims: Vec::new(),
                disputes: Vec::new(),
                retrieval_debug: None,
            })
        }

        async fn scopes_overview(&self) -> anyhow::Result<ScopesOverviewSnapshot> {
            Ok(ScopesOverviewSnapshot::default())
        }
    }

    fn sample_dispute(id: &str, claims: Vec<ClaimId>) -> Dispute {
        Dispute {
            id: id.parse().unwrap(),
            name: "duplicate_claim_set".into(),
            reporter_agent_id: AgentId::new("agent-a").unwrap(),
            claims,
            summary: "same claims should be reported once".into(),
            status: DisputeStatus::Open,
            created_at: "2026-06-22T10:00:00Z".parse().unwrap(),
            resolved_at: None,
        }
    }

    fn build_runner(
        dir: &tempfile::TempDir,
        maintainer: Arc<RecordingMaintainerClient>,
    ) -> AgentRunner {
        let agent_home = dir.path().to_path_buf();
        let claim_store: Arc<dyn LocalClaimStore> =
            Arc::new(LocalFsClaimStore::new(agent_home.clone()));
        let reported_store: Arc<dyn ReportedDisputeClaimSetStore> =
            Arc::new(LocalFsReportedDisputeClaimSetStore::new(agent_home.clone()));
        let inbox: Arc<dyn InboxReader> = Arc::new(LocalFsInboxReader::new(agent_home.clone()));
        let memory_store: Arc<dyn MemoryStore> =
            Arc::new(LocalFsMemoryStore::new(agent_home, 1024, 1024, true));
        AgentRunner::new(
            AgentId::new("agent-a").unwrap(),
            Arc::new(UnusedInboxJsonGenerator),
            claim_store,
            reported_store,
            inbox,
            memory_store,
            Arc::new(EmptyRouterClient),
            maintainer,
            Arc::new(LocalFsMaintainerUploadQueue::new(dir.path().to_path_buf())),
            0,
            Vec::<SkillSummary>::new(),
        )
    }

    #[tokio::test]
    async fn record_dispute_if_new_skips_reversed_duplicate_claim_set() {
        let dir = tempfile::tempdir().unwrap();
        let maintainer = Arc::new(RecordingMaintainerClient::default());
        let runner = build_runner(&dir, maintainer.clone());
        let a: ClaimId = "claim_11111111".parse().unwrap();
        let b: ClaimId = "claim_22222222".parse().unwrap();

        let first = sample_dispute("dispute_11111111", vec![b.clone(), a.clone()]);
        let second = sample_dispute("dispute_22222222", vec![a, b]);

        assert!(runner.record_dispute_if_new(&first).await.unwrap());
        assert!(!runner.record_dispute_if_new(&second).await.unwrap());

        let reported = maintainer.disputes.lock().unwrap().clone();
        assert!(reported.is_empty());
    }
}
