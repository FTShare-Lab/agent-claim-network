//! 同上下文自复核的真实 turn-loop 回归：证据门槛、修订、审计和恢复。
use super::*;
use serde_json::Value;

#[tokio::test]
async fn dream_template_errors_are_isolated_from_foreground_sessions() {
    for broken in ["claim_dream", "dream_compaction"] {
        for malformed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let provider = Arc::new(RecordingProvider::new(vec![response_step("ready", vec![])]));
            let mut engine = build_dream_test_engine(&dir, provider.clone());
            let prompts = dir.path().join("prompts");
            tokio::fs::create_dir(&prompts).await.unwrap();
            for name in ["agent_system", "claim_dream", "dream_compaction"] {
                if name != broken || malformed {
                    let text = if name == broken {
                        "{% invalid_tag %}"
                    } else {
                        "test prompt"
                    };
                    tokio::fs::write(prompts.join(format!("{name}.j2")), text)
                        .await
                        .unwrap();
                }
            }
            engine.prompt_registry = Arc::new(PromptRegistry::new(prompts).unwrap());
            engine
                .agent
                .claim_store
                .write_claim(&dream_fixture_claim("claim_11111111"))
                .await
                .unwrap();
            let mut session = engine.start_session(1, |_| {}).await.unwrap().session;
            let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
            engine.dream_config.enable = false;
            engine
                .run_dream_job("job_disabled", dir.path(), &jobs, true, true)
                .await
                .unwrap();
            engine.dream_config.enable = true;
            let error = engine
                .run_dream_job("job_invalid_prompt", dir.path(), &jobs, true, true)
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains(broken), "{error:#}");
            assert!(provider.requests().await.is_empty());
            engine
                .run_turn(&mut session, "continue", |_| {})
                .await
                .unwrap();
            assert_eq!(provider.requests().await.len(), 1);
        }
    }
}

#[tokio::test]
async fn dream_finish_reminder_survives_retry_and_allows_zero_changes() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step("early", "dream_finish", finish_groups(&[])),
        ProviderStep::TerminalFailure {
            message: "retry after reminder",
        },
        tool_use_step("confirmed", "dream_finish", finish_groups(&[])),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    assert!(engine
        .run_dream_job("job_no_change", dir.path(), &jobs, true, true)
        .await
        .is_err());
    let report = engine
        .run_dream_job("job_no_change", dir.path(), &jobs, true, false)
        .await
        .unwrap();
    assert!(report.updated_claim_ids.is_empty());
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![claim]
    );
    let requests = provider.requests().await;
    assert_eq!(
        requests.len(),
        3,
        "the reminder must not repeat after retry"
    );
    let feedback = serde_json::to_string(&requests[1].messages).unwrap();
    assert!(feedback.contains("confirmation_required") && feedback.contains("high 也可清理"));
    let result: Value = serde_json::from_slice(
        &tokio::fs::read(
            dir.path()
                .join("agents/agent-a/dream/job_no_change/result.json"),
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(result["exploration_receipts"], 0);
    assert_eq!(result["applied"], true);
}

#[tokio::test]
async fn dream_finish_reminder_allows_quality_cleanup_without_file_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let mut claim = dream_fixture_claim("claim_11111111");
    claim.confidence = Confidence::High;
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step("early", "dream_finish", finish_groups(&[])),
        tool_use_step(
            "stage",
            "dream_stage_group",
            stage_change("a", claim.id.as_str(), "deprecate", json!({})),
        ),
        tool_use_step("validate", "dream_validate", json!({})),
        ProviderStep::DreamGroupSelfReview,
        tool_use_step("finish", "dream_finish", finish_groups(&[])),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    let report = engine
        .run_dream_job("job_quality_after_reminder", dir.path(), &jobs, true, true)
        .await
        .unwrap();
    assert_eq!(report.updated_claim_ids, vec![claim.id]);
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap()[0].status,
        ClaimStatus::Deprecated
    );
    assert_eq!(provider.requests().await.len(), 5);
}

#[tokio::test]
async fn dream_protocol_repairs_keep_code_run_available() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("evidence.txt"),
        "Persisted queue survives restart.",
    )
    .await
    .unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step("read", "file_read", json!({"path":"evidence.txt"})),
        response_step("not a JSON plan", vec![]),
        tool_use_step(
            "after_text_repair",
            "code_run",
            json!({"script":"cat evidence.txt"}),
        ),
        response_step(&dream_evidence_plan(&claim), vec![]),
        tool_use_step(
            "after_legacy_import",
            "code_run",
            json!({"script":"cat evidence.txt"}),
        ),
        tool_use_step("validate", "dream_validate", json!({})),
        ProviderStep::DreamSelfReview,
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    let result = engine
        .run_dream_job("job_repair_tools", dir.path(), &jobs, true, true)
        .await
        .unwrap();
    assert_eq!(result.updated_claim_ids, vec![claim.id]);
    let requests = provider.requests().await;
    for (index, id) in [(3, "after_text_repair"), (5, "after_legacy_import")] {
        let result = requests[index]
            .messages
            .iter()
            .flat_map(|m| &m.content)
            .find_map(|b| match b {
                SessionTurnContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    ..
                } if tool_use_id == id => Some(content),
                _ => None,
            })
            .expect("code_run result must reach the next request");
        assert!(
            result.contains("Persisted queue survives restart."),
            "{id}: {result}"
        );
        assert!(!result.contains("shutting down"), "{id}: {result}");
    }
}

#[tokio::test]
async fn dream_group_execution_survives_restart_and_finishes_without_replaying() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "stage",
            "dream_stage_group",
            stage_change("a", claim.id.as_str(), "deprecate", json!({})),
        ),
        tool_use_step("validate", "dream_validate", json!({"group_ids":["a"]})),
        ProviderStep::DreamGroupSelfReview,
        ProviderStep::TerminalFailure {
            message: "interrupted after group review",
        },
        tool_use_step("finish", "dream_finish", finish_groups(&[])),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    assert!(engine
        .run_dream_job("job_group_review", dir.path(), &jobs, true, true)
        .await
        .is_err());
    let applied = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(applied[0].status, ClaimStatus::Deprecated);
    let result = engine
        .run_dream_job("job_group_review", dir.path(), &jobs, true, false)
        .await
        .unwrap();
    assert_eq!(result.updated_claim_ids, vec![claim.id]);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 5);
    assert!(serde_json::to_string(&requests[4].messages)
        .unwrap()
        .contains("actual_changed_ids"));
}

#[tokio::test]
async fn dream_compaction_runs_at_tool_boundary_and_resumes_after_provider_failure() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("evidence.txt"),
        "Exact original evidence remains available.",
    )
    .await
    .unwrap();
    let mut large = tool_use_step(
        "read_before_compact",
        "file_read",
        json!({"path":"evidence.txt"}),
    );
    if let ProviderStep::Response { response, .. } = &mut large {
        response.assistant_message.content.insert(
            0,
            SessionTurnContentBlock::text("探索记录。".repeat(160_000)),
        );
    }
    let provider = Arc::new(RecordingProvider::new(vec![
        large,
        response_step(&json!({"checked":"A/C scanned","findings":"file_read read_before_compact contains exact original evidence","uncertainties":"No evaluator result","next_steps":"Use existing evidence; finish conservatively"}).to_string(), vec![]),
        tool_use_step("history_index", "dream_read_history", json!({})),
        ProviderStep::TerminalFailure { message:"retry after compacted checkpoint" },
        tool_use_step("finish", "dream_finish", finish_groups(&[])),
    ]));
    let mut engine = build_dream_test_engine(&dir, provider.clone());
    engine.context_window = 240_000;
    let claim = dream_fixture_claim("claim_11111111");
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    let error = engine
        .run_dream_job("job_compaction", dir.path(), &jobs, true, true)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("retry after compacted checkpoint"),
        "{error:#}"
    );
    let checkpoint: Value = crate::storage::read_yaml(
        &jobs
            .parent()
            .unwrap()
            .join("dream_exploration/job_compaction.yaml"),
    )
    .await
    .unwrap();
    assert!(checkpoint["summary"].is_object());
    assert!(checkpoint["receipts"].get("evidence_1").is_some());
    let report = engine
        .run_dream_job("job_compaction", dir.path(), &jobs, true, false)
        .await
        .unwrap();
    assert!(report.updated_claim_ids.is_empty());
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![claim]
    );
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 5);
    assert!(
        requests[1].tools.is_empty(),
        "Only compression calls a tool-free summarizer"
    );
    let compressed = serde_json::to_string(&requests[2].messages).unwrap();
    assert!(compressed.contains("dream_context_checkpoint") && compressed.contains("evidence_1"));
    assert!(!compressed.contains(&"探索记录。".repeat(1000)));
    let restored = serde_json::to_string(&requests[4].messages).unwrap();
    assert!(restored.contains("dream_context_checkpoint") && restored.contains("archives"));
}

#[tokio::test]
async fn dream_b_requires_new_evidence_then_accepts_a_supported_scope_change() {
    let dir = tempfile::tempdir().unwrap();
    tokio::fs::write(
        dir.path().join("result.txt"),
        "Restart test passes for persisted queue items; memory-only items disappear.",
    )
    .await
    .unwrap();
    let original = dream_fixture_claim("claim_11111111");
    let mut missing = stage_change(
        "b",
        original.id.as_str(),
        "update",
        json!({"scope":"example / persisted queue"}),
    );
    missing["group"]["kind"] = json!("evidence");
    let summary = "result.txt: restart test confirms persisted queue recovery only; memory-only items disappear.";
    let mut fixed = missing.clone();
    fixed["group"]["operations"][0]["changes"]["evidence_summary"] = json!(summary);
    fixed["group"]["evidence_ids"] = json!(["evidence_1"]);
    fixed["group"] = dream_add_basis(fixed["group"].clone());
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step("missing", "dream_stage_group", missing),
        tool_use_step("read", "file_read", json!({"path":"result.txt"})),
        tool_use_step("fixed", "dream_stage_group", fixed),
        tool_use_step("validate", "dream_validate", json!({})),
        ProviderStep::DreamSelfReview,
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine
        .agent
        .claim_store
        .write_claim(&original)
        .await
        .unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    let report = engine
        .run_dream_job("job_new_evidence", dir.path(), &jobs, true, true)
        .await
        .unwrap();
    assert_eq!(report.updated_claim_ids, vec![original.id.clone()]);
    let claims = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(claims[0].scope, "example / persisted queue");
    assert_eq!(claims[0].evidence_summary, summary);
    assert_eq!(claims[0].statement, original.statement);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 5);
    assert!(serde_json::to_string(&requests[1].messages)
        .unwrap()
        .contains("new evidence_summary"));
    let checkpoint: Value =
        crate::storage::read_yaml(&jobs.parent().unwrap().join("dream/job_new_evidence.yaml"))
            .await
            .unwrap();
    assert!(checkpoint["plan"].get("unresolved").is_none());
    assert!(checkpoint["plan"].get("resolved").is_none());
}

#[tokio::test]
async fn dream_b_without_direct_evidence_can_finish_with_original_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let original = dream_fixture_claim("claim_11111111");
    let mut stage = stage_change(
        "b",
        original.id.as_str(),
        "deprecate",
        json!({"evidence_summary":"No acceptance result was found."}),
    );
    stage["group"]["kind"] = json!("evidence");
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step("unsupported", "dream_stage_group", stage),
        tool_use_step(
            "withdraw",
            "dream_stage_group",
            json!({"group_id":"b","group":null}),
        ),
        tool_use_step("finish", "dream_finish", finish_groups(&[])),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine
        .agent
        .claim_store
        .write_claim(&original)
        .await
        .unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    let report = engine
        .run_dream_job("job_no_evidence", dir.path(), &jobs, true, true)
        .await
        .unwrap();
    assert!(report.updated_claim_ids.is_empty());
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![original]
    );
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(serde_json::to_string(&requests[1].messages)
        .unwrap()
        .contains("change_basis.evidence_ids"));
    assert!(!requests[0].system_prompt.contains("unresolved"));
}

#[tokio::test]
async fn dream_high_evidence_change_returns_feedback_and_can_finish_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let mut original = dream_fixture_claim("claim_11111111");
    original.confidence = Confidence::High;
    let mut stage = stage_change(
        "b",
        original.id.as_str(),
        "update",
        json!({"confidence":"low","evidence_summary":"No new verification result found."}),
    );
    stage["group"]["kind"] = json!("evidence");
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step("stage", "dream_stage_group", stage),
        tool_use_step(
            "withdraw",
            "dream_stage_group",
            json!({"group_id":"b","group":null}),
        ),
        tool_use_step("finish", "dream_finish", finish_groups(&[])),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine
        .agent
        .claim_store
        .write_claim(&original)
        .await
        .unwrap();
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    let report = engine
        .run_dream_job("job_confidence_gate", dir.path(), &jobs, true, true)
        .await
        .unwrap();
    assert!(report.updated_claim_ids.is_empty());
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![original]
    );
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|r| !r.json_output));
    assert!(serde_json::to_string(&requests[1].messages)
        .unwrap()
        .contains("original medium/low"));
}

fn stage_change(group: &str, id: &str, action: &str, changes: Value) -> Value {
    json!({"group_id":group,"group":dream_add_basis(json!({
        "kind":"quality","reason":"retain useful knowledge with bounded wording", "evidence_ids":[],"coverage":[],
        "operations":[{"id":id,"action":action,"changes":changes}]
    }))})
}
fn finish_groups(groups: &[&str]) -> Value {
    json!({"group_ids":groups,"review":{"quality":"reviewed","evidence":"unknown stays unknown","consolidation":"conditions retained"}})
}

#[tokio::test]
async fn dream_self_review_repairs_in_same_context_and_writes_auditable_results() {
    let dir = tempfile::tempdir().unwrap();
    let original = dream_fixture_claim("claim_11111111");
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "stage",
            "dream_stage_group",
            stage_change(
                "a",
                original.id.as_str(),
                "update",
                json!({"name":"persisted queue recovery"}),
            ),
        ),
        tool_use_step("early", "dream_finish", finish_groups(&["a"])),
        tool_use_step("validate", "dream_validate", json!({})),
        tool_use_step(
            "bad_quote",
            "dream_finish",
            json!({"group_ids":["a"],"review":{"quality":"reviewed","evidence":"unchanged","consolidation":"none"},"self_reviews":[{"group_id":"a","validation_id":"invented","preservation":[],"support":[],"scope_and_certainty":"unchanged"}]}),
        ),
        ProviderStep::DreamSelfReview,
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine
        .agent
        .claim_store
        .write_claim(&original)
        .await
        .unwrap();
    let home = dir.path().join("agents/agent-a");
    let jobs = home.join("runtime/supervisor/jobs");
    let result = engine
        .run_dream_job("job_review", dir.path(), &jobs, true, true)
        .await
        .unwrap();
    assert_eq!(result.updated_claim_ids, vec![original.id.clone()]);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 5, "No hidden independent model calls");
    assert!(requests
        .iter()
        .all(|r| r.system_prompt == requests[0].system_prompt && r.tools.len() == 12));
    let last = serde_json::to_string(&requests[4].messages).unwrap();
    assert!(last.contains("does not write Claims") && last.contains(&original.statement));
    let audit = home.join("dream/job_review");
    assert!(audit.join("input.json").exists());
    let mut entries = tokio::fs::read_dir(audit.join("revisions")).await.unwrap();
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.path().extension().is_some_and(|e| e == "json") {
            count += 1;
        }
    }
    assert_eq!(count, 6);
    let report = tokio::fs::read_to_string(audit.join("report.md"))
        .await
        .unwrap();
    assert!(report.contains("persisted queue recovery") && report.contains("实际结果"));
    let actual: Value =
        serde_json::from_slice(&tokio::fs::read(audit.join("result.json")).await.unwrap()).unwrap();
    assert_eq!(actual["applied"], true);
    assert!(audit.join("executions").exists());
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap()[0].statement,
        original.statement
    );
}

#[tokio::test]
async fn dream_audit_failure_prevents_claim_commit() {
    let dir = tempfile::tempdir().unwrap();
    let original = dream_fixture_claim("claim_11111111");
    let provider = Arc::new(RecordingProvider::new(vec![tool_use_step(
        "stage",
        "dream_stage_group",
        stage_change("a", original.id.as_str(), "deprecate", json!({})),
    )]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    engine
        .agent
        .claim_store
        .write_claim(&original)
        .await
        .unwrap();
    let home = dir.path().join("agents/agent-a");
    let audit = home.join("dream/job_audit_failure");
    tokio::fs::create_dir_all(&audit).await.unwrap();
    tokio::fs::write(audit.join("revisions"), "not a directory")
        .await
        .unwrap();
    let jobs = home.join("runtime/supervisor/jobs");
    assert!(engine
        .run_dream_job("job_audit_failure", dir.path(), &jobs, true, true)
        .await
        .is_err());
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![original]
    );
    assert!(!home
        .join("runtime/supervisor/dream/job_audit_failure.yaml")
        .exists());
    assert_eq!(provider.requests().await.len(), 1);
}

#[tokio::test]
async fn dream_validation_does_not_accept_a_file_changed_after_exploration() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("result.txt");
    tokio::fs::write(&file, "Only persisted items recover.")
        .await
        .unwrap();
    let owner = AgentId::new("agent-a").unwrap();
    let original = dream_fixture_claim("claim_11111111");
    let tools = ToolRegistry::new(&ToolConfig {
        workspace_root: dir.path().into(),
        ..Default::default()
    })
    .unwrap()
    .with_local_knowledge(
        Arc::new(LocalFsClaimStore::new(dir.path().join("agent"))),
        dir.path().join("agent"),
        owner.clone(),
    )
    .for_dream(dir.path().into());
    tools
        .dispatch("file_read", json!({"path":"result.txt"}))
        .await
        .unwrap();
    let (receipts, observed) = tools.dream_evidence().await;
    let reviewer = super::super::dream_draft::Reviewer::new(
        owner,
        vec![original.clone()],
        Default::default(),
        None,
    );
    use crate::tool::DreamPlanTools;
    let mut stage = stage_change(
        "a",
        original.id.as_str(),
        "update",
        json!({"name":"persisted queue"}),
    );
    stage["group"]["evidence_ids"] = json!(["evidence_1"]);
    assert_eq!(
        reviewer
            .call(
                "dream_stage_group",
                stage,
                receipts.clone(),
                observed.clone()
            )
            .await["accepted"],
        true
    );
    tokio::fs::write(&file, "A later file revision with different conditions.")
        .await
        .unwrap();
    let feedback = reviewer
        .call("dream_validate", json!({}), receipts, observed)
        .await;
    assert_eq!(feedback["accepted"], false);
    assert!(feedback["error"]
        .as_str()
        .unwrap()
        .contains("Evidence changed"));
}

pub(super) async fn incremental_reviewer(
    dir: &tempfile::TempDir,
    claims: &[Claim],
) -> (
    SessionEngine,
    Arc<super::super::dream_execution::Execution>,
    super::super::dream_draft::Reviewer,
) {
    let engine = build_dream_test_engine(dir, Arc::new(RecordingProvider::new(vec![])));
    for claim in claims {
        engine.agent.claim_store.write_claim(claim).await.unwrap();
    }
    let execution = Arc::new(
        super::super::dream_execution::Execution::new(
            engine.clone(),
            "job_incremental",
            &dir.path().join("agents/agent-a/runtime/supervisor/jobs"),
            claims.to_vec(),
            json!({}),
            Utc::now(),
        )
        .await
        .unwrap(),
    );
    let reviewer = super::super::dream_draft::Reviewer::new(
        engine.agent.agent_id.clone(),
        claims.to_vec(),
        Default::default(),
        None,
    )
    .with_execution(execution.clone());
    (engine, execution, reviewer)
}

pub(super) async fn draft_call(
    reviewer: &super::super::dream_draft::Reviewer,
    tool: &str,
    input: Value,
) -> Value {
    use crate::tool::DreamPlanTools;
    reviewer
        .call(tool, input, BTreeMap::new(), BTreeMap::new())
        .await
}

pub(super) async fn checked_deprecation(
    reviewer: &super::super::dream_draft::Reviewer,
    claim: &Claim,
) -> Value {
    assert_eq!(
        draft_call(
            reviewer,
            "dream_stage_group",
            stage_change("a", claim.id.as_str(), "deprecate", json!({}))
        )
        .await["accepted"],
        true
    );
    let validated = draft_call(reviewer, "dream_validate", json!({"group_ids":["a"]})).await;
    assert_eq!(validated["accepted"], true);
    json!({"group_id":"a","validation_id":validated["validated_groups"][0]["validation_id"],"review":{"claims":[{"claim_id":claim.id,"final_name":claim.name,"name_reason":"Retains the historical record name; no replacement claim is asserted.","information_preserved":"Only one session's delivery record; no reusable rule or condition is present.","removal_or_correction":"Remove the delivery record from active memory, retaining the original in audit.","factual_correction":false,"output_ids":[],"evidence_ids":[]}],"scope_and_certainty":"No factual conclusion or confidence change; episodic record only."}})
}

#[tokio::test]
async fn dream_quality_shorthand_preserves_content_and_records_explicit_basis() {
    let dir = tempfile::tempdir().unwrap();
    let mut claim = dream_fixture_claim("claim_11111111");
    claim.statement = "This session added a dashboard and ran its local tests.".into();
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let mut apply = checked_deprecation(&reviewer, &claim).await;
    let reason = "Only this session's delivery and test events; no reusable decision rule.";
    let stage = json!({"group_id":"a","group":{"kind":"quality","reason":"Remove an episodic record","operations":[{"id":claim.id,"action":"deprecate","reason":reason}]}});
    let staged = draft_call(&reviewer, "dream_stage_group", stage).await;
    assert_eq!(staged["accepted"], true, "{staged}");
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![claim.clone()]
    );
    let validated = draft_call(&reviewer, "dream_validate", json!({})).await;
    apply["validation_id"] = validated["validated_groups"][0]["validation_id"].clone();
    let result = draft_call(&reviewer, "dream_apply_group", apply).await;
    assert_eq!(result["accepted"], true, "{result}");
    let records = execution.records().await.unwrap();
    assert_eq!(records.len(), 1);
    let group = &records[0].1.plan.groups[0];
    assert_eq!(group.change_basis.len(), 1);
    assert_eq!(group.change_basis[0].justification, reason);
    let after = engine
        .agent
        .claim_store
        .list_local_claims()
        .await
        .unwrap()
        .remove(0);
    assert_eq!(after.status, ClaimStatus::Deprecated);
    assert_eq!(after.name, claim.name);
    assert_eq!(after.statement, claim.statement);
    assert_eq!(after.scope, claim.scope);
    assert_eq!(after.confidence, claim.confidence);
    assert_eq!(after.evidence_summary, claim.evidence_summary);
    assert_eq!(after.source_claim_ids, claim.source_claim_ids);
}

#[tokio::test]
async fn dream_quality_shorthand_cannot_bypass_other_change_requirements() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let base = json!({"group_id":"a","group":{"kind":"quality","reason":"cleanup","operations":[{"id":claim.id,"action":"deprecate","reason":"episodic only"}]}});
    let mut cases = Vec::new();
    for kind in ["evidence", "consolidation"] {
        let mut invalid = base.clone();
        invalid["group"]["kind"] = json!(kind);
        cases.push(invalid);
    }
    let mut changes = base.clone();
    changes["group"]["operations"][0]["changes"] =
        json!({"evidence_summary":"unsupported new evidence"});
    cases.push(changes);
    let mut blank = base.clone();
    blank["group"]["operations"][0]["reason"] = json!(" ");
    cases.push(blank);
    let mut missing = base.clone();
    missing["group"]["operations"][0]
        .as_object_mut()
        .unwrap()
        .remove("reason");
    cases.push(missing);
    let mut duplicate = base.clone();
    duplicate["group"]["change_basis"] = json!([{"claim_id":claim.id,"removed_or_changed":"another change","added":"","justification":"conflicting basis","evidence_ids":[]}]);
    cases.push(duplicate);
    for invalid in cases {
        let result = draft_call(&reviewer, "dream_stage_group", invalid.clone()).await;
        assert_eq!(result["accepted"], false, "{invalid}: {result}");
    }
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![claim]
    );
    assert!(execution.records().await.unwrap().is_empty());
    assert_eq!(
        draft_call(&reviewer, "dream_stage_group", base).await["accepted"],
        true
    );
}

#[tokio::test]
async fn dream_name_review_binds_final_name_and_legacy_execution_remains_replayable() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let mut apply = checked_deprecation(&reviewer, &claim).await;
    assert_eq!(
        draft_call(
            &reviewer,
            "dream_stage_group",
            stage_change(
                "a",
                claim.id.as_str(),
                "update",
                json!({"name":"persisted_queue_recovery"})
            )
        )
        .await["accepted"],
        true
    );
    let validated = draft_call(&reviewer, "dream_validate", json!({})).await;
    apply["validation_id"] = validated["validated_groups"][0]["validation_id"].clone();
    apply["review"]["claims"][0]["final_name"] = json!("persisted_queue_recovery");
    apply["review"]["claims"][0]["name_reason"] =
        json!("The renamed title describes the preserved recovery rule.");
    apply["review"]["claims"][0]["output_ids"] = json!([claim.id]);
    for field in ["final_name", "name_reason"] {
        let mut missing = apply.clone();
        missing["review"]["claims"][0]
            .as_object_mut()
            .unwrap()
            .remove(field);
        let result = draft_call(&reviewer, "dream_apply_group", missing).await;
        assert_eq!(result["accepted"], false, "{result}");
        assert!(result["error"].as_str().unwrap().contains("final_name"));
    }
    let mut mismatch = apply.clone();
    mismatch["review"]["claims"][0]["final_name"] = json!("a name absent from the plan");
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", mismatch).await["accepted"],
        false
    );
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![claim]
    );
    assert!(execution.records().await.unwrap().is_empty());
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply.clone()).await["accepted"],
        true
    );
    let records = execution.records().await.unwrap();
    let mut legacy = serde_json::to_value(&records[0].1).unwrap();
    let item = legacy["operation"]["review"]["claims"][0]
        .as_object_mut()
        .unwrap();
    item.remove("final_name");
    item.remove("name_reason");
    crate::storage::write_yaml_atomic(&records[0].0, &legacy)
        .await
        .unwrap();
    let replay = draft_call(&reviewer, "dream_apply_group", apply).await;
    assert_eq!(replay["accepted"], true, "{replay}");
    assert_eq!(execution.records().await.unwrap().len(), 1);
}

#[tokio::test]
async fn dream_incremental_execution_is_durable_before_finish_and_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let apply = checked_deprecation(&reviewer, &claim).await;
    let before_execution = reviewer.snapshot().await;
    let result = draft_call(&reviewer, "dream_apply_group", apply.clone()).await;
    assert_eq!(result["committed"], true, "{result}");
    assert!(reviewer.finished().await.is_none());
    let after = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(after[0].status, ClaimStatus::Deprecated);
    let records = execution.records().await.unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0].1;
    assert!(record.applied && record.operation.as_ref().unwrap().hard_validation_passed);
    assert_eq!(record.groups[0].before[0], claim);
    assert_eq!(record.groups[0].after[0], after[0]);
    assert!(records[0].0.with_extension("json").exists());
    assert!(records[0].0.with_extension("md").exists());
    let resumed = super::super::dream_draft::Reviewer::new(
        engine.agent.agent_id.clone(),
        vec![claim.clone()],
        before_execution,
        None,
    )
    .with_execution(execution.clone());
    resumed.reconcile_executions().await.unwrap();
    assert!(
        resumed.is_empty().await,
        "Applied operations must not remain in a recovered draft"
    );
    let replay = draft_call(&reviewer, "dream_apply_group", apply.clone()).await;
    assert_eq!(replay["replayed"], true);
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        after
    );
    assert_eq!(execution.records().await.unwrap().len(), 1);
    let mut wrong = apply;
    wrong["group_id"] = json!("other");
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", wrong).await["accepted"],
        false
    );
    let state: Value = crate::storage::read_yaml(
        &dir.path()
            .join("agents/agent-a/runtime/supervisor/dream_state.yaml"),
    )
    .await
    .unwrap();
    assert!(
        state["last_success_at"].is_null(),
        "One operation must not finish the entire run"
    );
}

#[tokio::test]
async fn dream_incremental_rejects_stale_validation_and_failed_audit_before_write() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let apply = checked_deprecation(&reviewer, &claim).await;
    let mut external = claim.clone();
    external.statement = "External revision must not be overwritten.".into();
    engine
        .agent
        .claim_store
        .write_claim(&external)
        .await
        .unwrap();
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply).await["accepted"],
        false
    );
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![external.clone()]
    );
    assert!(execution.records().await.unwrap().is_empty());
    let fresh = checked_deprecation(&reviewer, &external).await;
    let blocked = dir
        .path()
        .join("agents/agent-a/dream/job_incremental/executions");
    tokio::fs::write(&blocked, "not a directory").await.unwrap();
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", fresh).await["accepted"],
        false
    );
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        vec![external]
    );
}

#[tokio::test]
async fn dream_incremental_recovers_prepared_group_without_reexecuting_model() {
    let dir = tempfile::tempdir().unwrap();
    let claim = dream_fixture_claim("claim_11111111");
    let (engine, execution, reviewer) = incremental_reviewer(&dir, &[claim.clone()]).await;
    let apply = checked_deprecation(&reviewer, &claim).await;
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply).await["committed"],
        true
    );
    let (path, mut record) = execution.records().await.unwrap().remove(0);
    let after = record.groups[0].after.clone();
    record.applied = false;
    record.groups[0].completed = false;
    record.self_versions.clear();
    record.success_at = None;
    engine.agent.claim_store.write_claim(&claim).await.unwrap();
    crate::storage::write_yaml_atomic(&path, &record)
        .await
        .unwrap();
    // 已有 Prepared、但尚未写 pending 标记时，不能继续撤回或修改这组操作。
    assert!(execution.ensure_no_pending().await.is_err());
    let withdrawn = draft_call(
        &reviewer,
        "dream_stage_group",
        json!({"group_id":"a","group":null}),
    )
    .await;
    assert_eq!(withdrawn["accepted"], false);
    assert_eq!(withdrawn["recovery_required"], true);
    let pending = super::super::dream_execution::pending_path(&dir.path().join("agents/agent-a"));
    crate::storage::write_yaml_atomic(&pending, &path)
        .await
        .unwrap();
    engine.recover_pending_dream_execution().await.unwrap();
    assert_eq!(
        engine.agent.claim_store.list_local_claims().await.unwrap(),
        after
    );
    assert!(!pending.exists());
    assert!(execution.records().await.unwrap()[0].1.applied);
}

#[tokio::test]
async fn dream_incremental_missing_review_keeps_other_committed_groups() {
    let dir = tempfile::tempdir().unwrap();
    let first = dream_fixture_claim("claim_11111111");
    let second = dream_fixture_claim("claim_22222222");
    let (engine, _, reviewer) = incremental_reviewer(&dir, &[first.clone(), second.clone()]).await;
    let apply = checked_deprecation(&reviewer, &first).await;
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply.clone()).await["committed"],
        true
    );
    let mut bad = checked_deprecation(&reviewer, &second).await;
    // 旧执行的重复回执不能清除复用组名的新草稿。
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply).await["replayed"],
        true
    );
    bad["review"]["claims"] = json!([]);
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", bad).await["accepted"],
        false
    );
    assert_eq!(
        draft_call(&reviewer, "dream_finish", finish_groups(&[])).await["accepted"],
        false
    );
    let claims = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(
        claims.iter().find(|c| c.id == first.id).unwrap().status,
        ClaimStatus::Deprecated
    );
    assert_eq!(claims.iter().find(|c| c.id == second.id).unwrap(), &second);
}

fn candidate(group: &str, claim: &Claim, kind: &str) -> Value {
    json!({"group_id":group,"claim_ids":[claim.id],"kind":kind,
        "reason":"Check the identified memory issue before choosing the next candidate."})
}

#[tokio::test]
async fn dream_candidates_block_finish_after_other_work_and_survive_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let first = dream_fixture_claim("claim_11111111");
    let second = dream_fixture_claim("claim_22222222");
    let (engine, execution, reviewer) =
        incremental_reviewer(&dir, &[first.clone(), second.clone()]).await;
    let registered = draft_call(
        &reviewer,
        "dream_record_candidates",
        json!({"candidates":[
            candidate("a", &first, "quality"), candidate("b", &second, "evidence")
        ]}),
    )
    .await;
    assert_eq!(registered["accepted"], true, "{registered}");
    let apply = checked_deprecation(&reviewer, &first).await;
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply.clone()).await["committed"],
        true
    );
    let snapshot =
        serde_json::from_slice(&serde_json::to_vec(&reviewer.snapshot().await).unwrap()).unwrap();
    let restored = super::super::dream_draft::Reviewer::new(
        engine.agent.agent_id.clone(),
        vec![first, second.clone()],
        snapshot,
        None,
    )
    .with_execution(execution);
    restored.reconcile_executions().await.unwrap();
    let state = restored.resume_input(&BTreeMap::new()).await;
    assert_eq!(state["candidate_progress"]["current"]["group_id"], "b");
    assert_eq!(
        state["candidate_progress"]["handled"][0]["outcome"]["status"],
        "executed"
    );
    // 旧执行回执不能吞掉下一项，即使恢复之后重复执行。
    assert_eq!(
        draft_call(&restored, "dream_apply_group", apply).await["replayed"],
        true
    );
    let finish = draft_call(&restored, "dream_finish", finish_groups(&[])).await;
    assert_eq!(finish["accepted"], false);
    assert!(finish["error"]
        .as_str()
        .unwrap()
        .contains("Identified candidates remain"));
    assert_eq!(draft_call(&restored, "dream_keep_candidate", json!({"group_id":"b","reason_kind":"insufficient_evidence","reason":"The available trace is a completion self-report; there is no applicable independent result to change this judgment."})).await["accepted"], true);
    let completed = draft_call(&restored, "dream_finish", finish_groups(&[])).await;
    assert_eq!(completed["accepted"], true, "{completed}");
    assert_eq!(
        completed["candidate_progress"]["handled"][1]["outcome"]["status"],
        "kept"
    );
    let claims = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(claims.iter().find(|c| c.id == second.id).unwrap(), &second);
}

#[tokio::test]
async fn dream_candidate_withdrawal_and_failed_review_cannot_silently_abandon_work() {
    let dir = tempfile::tempdir().unwrap();
    let first = dream_fixture_claim("claim_11111111");
    let second = dream_fixture_claim("claim_22222222");
    let (engine, _, reviewer) = incremental_reviewer(&dir, &[first.clone(), second.clone()]).await;
    let mut apply = checked_deprecation(&reviewer, &first).await;
    apply["review"]["claims"] = json!([]);
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply).await["accepted"],
        false
    );
    assert_eq!(
        draft_call(
            &reviewer,
            "dream_stage_group",
            stage_change("b", second.id.as_str(), "deprecate", json!({}))
        )
        .await["accepted"],
        false
    );
    assert_eq!(
        draft_call(
            &reviewer,
            "dream_stage_group",
            json!({"group_id":"a","group":null})
        )
        .await["accepted"],
        true
    );
    let remaining = draft_call(&reviewer, "dream_read_draft", json!({})).await;
    assert!(remaining["groups"].as_array().unwrap().is_empty());
    assert_eq!(
        remaining["draft_state"]["candidate_progress"]["current"]["group_id"],
        "a"
    );
    assert_eq!(
        draft_call(&reviewer, "dream_finish", finish_groups(&[])).await["accepted"],
        false
    );
    assert_eq!(draft_call(&reviewer, "dream_keep_candidate", json!({"group_id":"a","reason_kind":"other_priority","reason":"Other work is more important."})).await["accepted"], false);
    let wrong_kind = draft_call(&reviewer, "dream_keep_candidate", json!({"group_id":"a","reason_kind":"no_consolidation_benefit","reason":"This pure record should be deprecated under A."})).await;
    assert_eq!(wrong_kind["accepted"], false);
    assert_eq!(
        wrong_kind["draft_state"]["candidate_progress"]["current"]["group_id"],
        "a"
    );
    assert_eq!(draft_call(&reviewer, "dream_keep_candidate", json!({"group_id":"a","reason_kind":"uncertain_preservation","reason":"The statement contains a recovery condition that the proposed cleanup does not preserve."})).await["accepted"], true);
    let stored = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(stored.len(), 2);
    for original in [&first, &second] {
        assert_eq!(stored.iter().find(|c| c.id == original.id), Some(original));
    }
}

#[tokio::test]
async fn dream_candidate_refinement_cannot_drop_claims_or_reuse_partial_validation() {
    let dir = tempfile::tempdir().unwrap();
    let first = dream_fixture_claim("claim_11111111");
    let second = dream_fixture_claim("claim_22222222");
    let (engine, _, reviewer) = incremental_reviewer(&dir, &[first.clone(), second.clone()]).await;
    let apply = checked_deprecation(&reviewer, &first).await;
    let mut expanded = candidate("a", &first, "quality");
    expanded["claim_ids"] = json!([first.id, second.id]);
    assert_eq!(
        draft_call(
            &reviewer,
            "dream_record_candidates",
            json!({"candidates":[expanded]})
        )
        .await["accepted"],
        true
    );
    assert_eq!(
        draft_call(&reviewer, "dream_apply_group", apply).await["accepted"],
        false
    );
    assert_eq!(
        draft_call(
            &reviewer,
            "dream_record_candidates",
            json!({"candidates":[candidate("a", &first, "quality")]})
        )
        .await["accepted"],
        false
    );
    assert_eq!(
        draft_call(&reviewer, "dream_validate", json!({})).await["accepted"],
        false
    );
    let state = reviewer.resume_input(&BTreeMap::new()).await;
    assert_eq!(
        state["candidate_progress"]["current"]["claim_ids"],
        json!([first.id, second.id])
    );
    let stored = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(stored.len(), 2);
    for original in [&first, &second] {
        assert_eq!(stored.iter().find(|c| c.id == original.id), Some(original));
    }
}

#[tokio::test]
async fn dream_candidate_registration_is_atomic_and_preserves_ownership_and_high_gate() {
    use crate::tool::DreamPlanTools;
    let dir = tempfile::tempdir().unwrap();
    let mut own = dream_fixture_claim("claim_11111111");
    own.confidence = Confidence::High;
    let mut foreign = dream_fixture_claim("claim_22222222");
    foreign.holder = AgentId::new("agent-other").unwrap();
    let (_, execution, reviewer) =
        incremental_reviewer(&dir, &[own.clone(), foreign.clone()]).await;
    let failed = draft_call(
        &reviewer,
        "dream_record_candidates",
        json!({"candidates":[
            candidate("a", &own, "quality"), candidate("b", &foreign, "quality")
        ]}),
    )
    .await;
    assert_eq!(failed["accepted"], false);
    assert!(failed["draft_state"]["candidate_progress"]["pending"]
        .as_array()
        .unwrap()
        .is_empty());
    let rejected_high = draft_call(
        &reviewer,
        "dream_record_candidates",
        json!({"candidates":[candidate("a", &own, "evidence")]}),
    )
    .await;
    assert_eq!(rejected_high["accepted"], false);
    assert!(rejected_high["error"]
        .as_str()
        .unwrap()
        .contains(own.id.as_str()));
    assert!(rejected_high["error"]
        .as_str()
        .unwrap()
        .contains("resubmit"));
    // 即使后续上下文读到了较低置信度，本轮原始 high 仍不能登记为 B。
    let mut lowered = own.clone();
    lowered.confidence = Confidence::Low;
    let eligibility = execution.eligibility_context(&[lowered.clone(), foreign.clone()]);
    assert_eq!(eligibility["loaded_b_eligible_ids"], json!([]));
    assert_eq!(
        reviewer
            .call(
                "dream_record_candidates",
                json!({"candidates":[candidate("a", &lowered, "evidence")]}),
                BTreeMap::new(),
                BTreeMap::from([(lowered.id.clone(), lowered)]),
            )
            .await["accepted"],
        false
    );
    assert_eq!(
        draft_call(
            &reviewer,
            "dream_record_candidates",
            json!({"candidates":[candidate("a", &own, "quality")]})
        )
        .await["accepted"],
        true
    );
}

#[tokio::test]
async fn dream_candidate_retry_can_keep_uncertain_b_then_execute_a_through_real_loop() {
    let dir = tempfile::tempdir().unwrap();
    let uncertain = dream_fixture_claim("claim_11111111");
    let mut record = dream_fixture_claim("claim_22222222");
    record.confidence = Confidence::High;
    let provider = Arc::new(RecordingProvider::new(vec![
        tool_use_step(
            "register",
            "dream_record_candidates",
            json!({"candidates":[candidate("b", &uncertain, "evidence"),candidate("a", &record, "quality")]}),
        ),
        ProviderStep::TerminalFailure {
            message: "retry after candidate registration",
        },
        tool_use_step("premature", "dream_finish", finish_groups(&[])),
        tool_use_step(
            "keep",
            "dream_keep_candidate",
            json!({"group_id":"b","reason_kind":"insufficient_evidence","reason":"Only the prior task self-report is available; it cannot establish a factual correction."}),
        ),
        tool_use_step(
            "stage",
            "dream_stage_group",
            stage_change("a", record.id.as_str(), "deprecate", json!({})),
        ),
        tool_use_step("validate", "dream_validate", json!({})),
        ProviderStep::DreamGroupSelfReview,
        tool_use_step("finish", "dream_finish", finish_groups(&[])),
    ]));
    let engine = build_dream_test_engine(&dir, provider.clone());
    for claim in [&uncertain, &record] {
        engine.agent.claim_store.write_claim(claim).await.unwrap();
    }
    let jobs = dir.path().join("agents/agent-a/runtime/supervisor/jobs");
    assert!(engine
        .run_dream_job("job_candidates", dir.path(), &jobs, true, true)
        .await
        .is_err());
    let report = engine
        .run_dream_job("job_candidates", dir.path(), &jobs, true, false)
        .await
        .unwrap();
    assert_eq!(report.updated_claim_ids, vec![record.id.clone()]);
    let requests = provider.requests().await;
    assert_eq!(requests.len(), 8);
    assert!(serde_json::to_string(&requests[3].messages)
        .unwrap()
        .contains("Identified candidates remain"));
    let claims = engine.agent.claim_store.list_local_claims().await.unwrap();
    assert_eq!(
        claims.iter().find(|c| c.id == uncertain.id).unwrap(),
        &uncertain
    );
    assert_eq!(
        claims.iter().find(|c| c.id == record.id).unwrap().status,
        ClaimStatus::Deprecated
    );
}
