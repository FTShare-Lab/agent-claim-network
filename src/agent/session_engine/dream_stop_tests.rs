//! Dream 停机与恢复回归：模型等待、Prepared 收尾及对其他后台任务的影响。
use super::super::dream::{DreamControl, DreamStopped};
use super::dream_integration_tests::{checked_deprecation, draft_call, incremental_reviewer};
use super::*;
use std::path::Path;

fn stage(id: &ClaimId) -> ProviderStep {
    tool_use_step(
        "stage",
        "dream_stage_group",
        json!({"group_id":"a","group":{"kind":"quality","reason":"Session delivery only","operations":[{"id":id,"action":"deprecate","reason":"No reusable judgment"}]}}),
    )
}

fn finish() -> ProviderStep {
    tool_use_step(
        "finish",
        "dream_finish",
        json!({"group_ids":[],"review":{"quality":"Reviewed delivery record","evidence":"No unsupported factual changes","consolidation":"Keep other knowledge separate"}}),
    )
}

fn start(
    engine: &SessionEngine,
    workspace: &Path,
    control: DreamControl,
    manual: bool,
) -> tokio::task::JoinHandle<anyhow::Result<super::super::SessionFinalizeReport>> {
    let engine = engine.clone();
    let workspace = workspace.to_path_buf();
    tokio::spawn(async move {
        engine
            .run_dream_job_with_control(
                "job_stop",
                &workspace,
                &workspace.join("agents/agent-a/runtime/supervisor/jobs"),
                manual,
                true,
                control,
            )
            .await
    })
}

async fn stop_result(
    task: tokio::task::JoinHandle<anyhow::Result<super::super::SessionFinalizeReport>>,
) {
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.is::<DreamStopped>(), "{error:#}");
}

#[tokio::test]
async fn dream_stop_interrupts_initial_snapshot_lock_wait() {
    let dir = tempfile::tempdir().unwrap();
    let provider = Arc::new(RecordingProvider::new(vec![]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine
        .agent
        .claim_store
        .write_claim(&dream_fixture_claim("claim_11111111"))
        .await
        .unwrap();
    let home = dir.path().join("agents/agent-a");
    let guard = crate::storage::FileLockGuard::lock_exclusive(
        crate::storage::paths::agent_home_knowledge_apply_lock_path(&home),
    )
    .await
    .unwrap();
    let control = DreamControl::default();
    let task = start(&engine, dir.path(), control.clone(), true);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!task.is_finished());
    control.stop.cancel();
    stop_result(task).await;
    assert!(provider.requests().await.is_empty());
    drop(guard);
}

struct WaitingMaintainer {
    entered: Notify,
    uploads: AtomicUsize,
}

#[async_trait]
impl MaintainerClient for WaitingMaintainer {
    async fn pull_inbox(&self, _: &AgentId) -> anyhow::Result<Vec<InboxMessage>> {
        Ok(vec![])
    }
    async fn ack_inbox(&self, _: &AgentId, _: &[InboxId]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn upload_claim(&self, _: &Claim) -> anyhow::Result<()> {
        self.uploads.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn report_dispute(&self, _: &Dispute) -> anyhow::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn dream_ipc_recovery_defers_remote_upload_without_losing_pending_claims() {
    use crate::storage::{paths, read_yaml, write_yaml_atomic};
    use crate::supervisor::{
        self, SupervisorLaunchConfig, SupervisorRuntimeFingerprint, SupervisorRuntimeState,
    };

    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let apply = checked_deprecation(&reviewer, &claim).await;
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply).await["committed"],
        true
    );
    let (path, mut record) = execution.records().await.unwrap().remove(0);
    record.applied = false;
    record.groups[0].completed = false;
    record.self_versions.clear();
    record.success_at = None;
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    write_yaml_atomic(&path, &record).await.unwrap();
    let home = dir.path().join("agents/agent-a");
    write_yaml_atomic(&super::super::dream_execution::pending_path(&home), &path)
        .await
        .unwrap();

    let mut engine = build_dream_test_engine(&dir, Arc::new(RecordingProvider::new(vec![])));
    let maintainer = Arc::new(WaitingMaintainer {
        entered: Notify::new(),
        uploads: AtomicUsize::new(0),
    });
    let runner = Arc::get_mut(&mut engine.runner).unwrap();
    runner.maintainer_client = Some(maintainer.clone());
    Arc::make_mut(&mut runner.context).maintainer_client = Some(maintainer.clone());
    engine.agent = runner.context();
    let fingerprint = SupervisorRuntimeFingerprint {
        schema: 1,
        digest: "dream-stop-recovery".into(),
    };
    let config = SupervisorLaunchConfig::new(
        home.clone(),
        dir.path().join("config.toml"),
        None,
        false,
        fingerprint.clone(),
    );
    let task = tokio::spawn(supervisor::run_supervisor(
        engine.clone(),
        home.clone(),
        fingerprint,
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if supervisor::supervisor_status(&home)
                .await
                .unwrap()
                .runtime_state
                == SupervisorRuntimeState::Running
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let workspace = dir.path().to_path_buf();
    let check = tokio::time::timeout(
        Duration::from_secs(2),
        supervisor::check_dream(&config, workspace, true),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(check.job_id.is_none());
    assert_eq!(maintainer.uploads.load(Ordering::SeqCst), 0);
    // 本地恢复返回后可正常 Stop，不启动远端请求；待上传版本仍由队列持久保存。
    let stop = tokio::time::timeout(Duration::from_secs(2), supervisor::stop_supervisor(&home))
        .await
        .unwrap()
        .unwrap();
    assert!(stop.stopped);
    task.await.unwrap().unwrap();
    let pending: PendingMaintainerUploads =
        read_yaml(&paths::agent_home_pending_maintainer_uploads_path(&home))
            .await
            .unwrap();
    assert_eq!(pending.claims.len(), 1);
    assert_eq!(pending.claims[0].id, claim.id);
    assert_eq!(pending.claims[0].status, ClaimStatus::Deprecated);
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap()[0].status,
        ClaimStatus::Deprecated
    );
    let recovered: Value = read_yaml(&path).await.unwrap();
    assert_eq!(recovered["applied"], true);
    assert!(!super::super::dream_execution::pending_path(&home).exists());

    // 正常 Dream 提交仍会尝试交付，且等待远端时能被取消，不丢失待上传版本。
    let control = DreamControl::default();
    let upload_engine = engine.clone();
    let upload_control = control.clone();
    let upload = tokio::spawn(async move {
        upload_engine
            .apply_dream_checkpoint_with_control(&path, &upload_control)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), maintainer.entered.notified())
        .await
        .unwrap();
    control.stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), upload)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pending: PendingMaintainerUploads =
        read_yaml(&paths::agent_home_pending_maintainer_uploads_path(&home))
            .await
            .unwrap();
    assert_eq!(pending.claims[0].id, claim.id);
}

#[tokio::test]
async fn dream_recovery_failure_does_not_spend_session_job_attempts() {
    use crate::storage::{read_yaml, write_yaml_atomic};
    use crate::supervisor::{self, SupervisorRuntimeFingerprint};

    for kind in ["finalize", "recap"] {
        let dir = tempfile::tempdir().unwrap();
        let provider = Arc::new(RecordingProvider::new(vec![]));
        let engine = build_dream_test_engine(&dir, provider.clone());
        let report = engine.start_session(1, |_| {}).await.unwrap();
        let mut session = report.session;
        if kind == "finalize" {
            session.mark_finalizing(Utc::now()).await.unwrap();
        }
        drop(report.runtime_lease);
        let home = dir.path().join("agents/agent-a");
        let supervisor_dir = home.join("runtime/supervisor");
        let job_path = supervisor_dir.join("jobs/job_session.yaml");
        let pending_path = super::super::dream_execution::pending_path(&home);
        write_yaml_atomic(
            &pending_path,
            &supervisor_dir.join("missing-checkpoint.yaml"),
        )
        .await
        .unwrap();
        let mut job_kind = json!({"type":kind,"session_id":session.metadata.id});
        if kind == "recap" {
            job_kind["recap_end_index"] = json!(0);
        }
        write_yaml_atomic(
            &job_path,
            &json!({
                "id":"job_session", "agent_id":"agent-a", "kind":job_kind,
                "status":"queued", "attempts":4, "manual_retries":0,
                "created_at":Utc::now(), "updated_at":Utc::now(), "notify_on_completion":false
            }),
        )
        .await
        .unwrap();
        let task = tokio::spawn(supervisor::run_supervisor(
            engine,
            home.clone(),
            SupervisorRuntimeFingerprint {
                schema: 1,
                digest: "recovery-isolation".into(),
            },
        ));
        let deferred = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let job: Value = read_yaml(&job_path).await.unwrap();
                if job["last_error"]
                    .as_str()
                    .is_some_and(|e| e.contains("Dream"))
                {
                    break job;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(deferred["status"], "queued", "{kind}: {deferred}");
        assert_eq!(deferred["attempts"], 4);
        // 等待再次检查，验证连续恢复失败也不会耗尽该会话任务的预算。
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let deferred_again: Value = read_yaml(&job_path).await.unwrap();
        assert_eq!(deferred_again["attempts"], 4);
        assert_ne!(deferred_again["status"], "failed");
        assert!(provider.requests().await.is_empty());
        tokio::fs::remove_file(pending_path).await.unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let job: Value = read_yaml(&job_path).await.unwrap();
                if job["status"] == "succeeded" || job["status"] == "failed" {
                    break job;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(completed["status"], "succeeded", "{kind}: {completed}");
        assert_eq!(completed["attempts"], 5);
        assert!(completed["last_error"].is_null());
        assert!(supervisor::stop_supervisor(&home).await.unwrap().stopped);
        task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn dream_stop_interrupts_provider_and_resumes_staged_candidate() {
    let dir = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let claim = dream_fixture_claim("claim_11111111");
    let provider = Arc::new(RecordingProvider::new(vec![
        stage(&claim.id),
        ProviderStep::Wait {
            entered: entered.clone(),
        },
        tool_use_step("validate", "dream_validate", json!({})),
        ProviderStep::DreamGroupSelfReview,
        finish(),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    let control = DreamControl::default();
    let task = start(&engine, dir.path(), control.clone(), true);
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    control.stop.cancel();
    stop_result(task).await;
    assert_eq!(provider.requests().await.len(), 2);
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![claim.clone()]
    );
    let progress: Value = crate::storage::read_yaml(
        &dir.path()
            .join("agents/agent-a/runtime/supervisor/dream_exploration/job_stop.yaml"),
    )
    .await
    .unwrap();
    assert!(progress["draft"]["groups"]["a"].is_object());
    let report = start(&engine, dir.path(), DreamControl::default(), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.updated_claim_ids, vec![claim.id]);
    assert_eq!(provider.requests().await.len(), 5);
}

#[tokio::test]
async fn dream_stop_preserves_applied_group_and_resumes_below_automatic_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let claim = dream_fixture_claim("claim_00000001");
    let provider = Arc::new(RecordingProvider::new(vec![
        stage(&claim.id),
        tool_use_step("validate", "dream_validate", json!({})),
        ProviderStep::DreamGroupSelfReview,
        ProviderStep::Wait {
            entered: entered.clone(),
        },
        finish(),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    for index in 1..=5 {
        engine
            .agent
            .claim_store
            .write_claim(&dream_fixture_claim(&format!("claim_{index:08x}")))
            .await
            .unwrap();
    }
    let control = DreamControl::default();
    let task = start(&engine, dir.path(), control.clone(), false);
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let applied = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert!(applied
        .iter()
        .any(|c| c.id == claim.id && c.status == ClaimStatus::Deprecated));
    control.stop.cancel();
    stop_result(task).await;
    assert!(!engine.dream_eligible(false).await.unwrap());
    let state: Value = crate::storage::read_yaml(
        &dir.path()
            .join("agents/agent-a/runtime/supervisor/dream_state.yaml"),
    )
    .await
    .unwrap();
    assert!(state["last_success_at"].is_null());
    let report = start(&engine, dir.path(), DreamControl::default(), false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.updated_claim_ids, vec![claim.id]);
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        applied
    );
    assert_eq!(provider.requests().await.len(), 5);
}

#[tokio::test]
async fn dream_stop_before_prepared_rejects_write_without_losing_draft() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let apply = checked_deprecation(&reviewer, &claim).await;
    execution.control.stop.cancel();
    let result = draft_call(&reviewer, "dream_apply_group", apply).await;
    assert_eq!(result["accepted"], false);
    assert_eq!(result["committed"], false);
    assert!(execution.records().await.unwrap().is_empty());
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![claim]
    );
    assert!(!reviewer.is_empty().await);
}

struct GateApplyProvider {
    inner: RecordingProvider,
    calls: AtomicUsize,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl ProviderAdapter for GateApplyProvider {
    async fn send(
        &self,
        request: ProviderRequest,
        emit: &mut (dyn FnMut(ProviderEvent) + Send),
    ) -> anyhow::Result<ProviderResponse> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 2 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.send(request, emit).await
    }
}

#[tokio::test]
async fn dream_stop_during_prepared_waits_for_commit_then_preserves_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let provider = Arc::new(GateApplyProvider {
        inner: RecordingProvider::new(vec![
            stage(&claim.id),
            tool_use_step("validate", "dream_validate", json!({})),
            ProviderStep::DreamGroupSelfReview,
            finish(),
        ]),
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    let control = DreamControl::default();
    let task = start(&engine, dir.path(), control.clone(), true);
    tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
        .await
        .unwrap();
    let home = dir.path().join("agents/agent-a");
    let guard = crate::storage::FileLockGuard::lock_exclusive(
        &crate::storage::paths::agent_home_knowledge_apply_lock_path(&home),
    )
    .await
    .unwrap();
    provider.release.notify_one();
    let execution_path = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(mut entries) =
                tokio::fs::read_dir(home.join("dream/job_stop/executions")).await
            {
                if let Some(entry) = entries.next_entry().await.unwrap() {
                    if entry.path().extension().is_some_and(|ext| ext == "yaml") {
                        break entry.path();
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    control.stop.cancel();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !task.is_finished(),
        "Prepared tool must not be force-dropped after the foreground cancel grace"
    );
    drop(guard);
    stop_result(task).await;
    let applied = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(applied[0].status, ClaimStatus::Deprecated);
    let record: Value = crate::storage::read_yaml(&execution_path).await.unwrap();
    assert_eq!(record["applied"], true);
    assert!(!home.join("runtime/supervisor/dream_pending.yaml").exists());
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3);
    let report = start(&engine, dir.path(), DreamControl::default(), true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.updated_claim_ids, vec![claim.id]);
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        applied
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 4);
}
