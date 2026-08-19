use std::path::PathBuf;

use agent_claim_network::claim::{AgentId, Claim, ClaimId, ClaimStatus, Confidence};
use agent_claim_network::config::Config;
use agent_claim_network::evaluation::{
    run_attempt, EvaluationAttemptConfig, EvaluationEvent, EvaluationResult, EvaluationRunPaths,
    FrozenClaimBundle, FrozenClaimBundleRouter, EVALUATION_MODEL_KEY_ENV,
    EVALUATION_SCHEMA_VERSION,
};
use agent_claim_network::router::{AgentQuery, RouterClient};
use chrono::Utc;
use serde_json::json;

#[test]
fn python_generated_acn_config_loads_in_rust_evaluation_mode() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benchmarks/deepswe/tests/fixtures/generated-acn.toml");

    let config = Config::load_for_evaluation(&path).unwrap();
    let upstream = config.resolve_upstream(Some("eval")).unwrap();

    assert_eq!(upstream.agent_id.as_str(), "evaluation");
    assert_eq!(config.agent.llm.model, "fixture-model");
    // key 只能来自容器环境变量；配置文件里绝不出现明文。
    assert_eq!(config.agent.llm.api_key_env, EVALUATION_MODEL_KEY_ENV);
    assert!(config.agent.llm.api_key.is_none());
}

#[tokio::test]
async fn evaluation_rejects_model_key_env_other_than_isolated_env() {
    let root = tempfile::tempdir().unwrap();
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benchmarks/deepswe/tests/fixtures/generated-acn.toml");
    let wrong_config = root.path().join("wrong-key-env.toml");
    let body = std::fs::read_to_string(fixture)
        .unwrap()
        .replace(EVALUATION_MODEL_KEY_ENV, "ACN_EVAL_UPSTREAM_KEY");
    std::fs::write(&wrong_config, body).unwrap();
    let config = EvaluationAttemptConfig {
        schema_version: EVALUATION_SCHEMA_VERSION,
        attempt_id: "attempt-key-env".into(),
        task_prompt: "修复测试".into(),
        workspace_root: root.path().join("workspace"),
        runtime_root: root.path().join("runtime"),
        acn_config: wrong_config,
        output_dir: root.path().join("output"),
        upstream: "eval".into(),
        variant: "A".into(),
        attempt_deadline_secs: 1,
        model_egress_mode: "pier".into(),
        claim_bundle: None,
    };
    tokio::fs::create_dir_all(&config.workspace_root)
        .await
        .unwrap();

    let result = run_attempt(config).await.unwrap();

    assert_eq!(result.exit_type, "failed");
    assert!(result
        .error
        .as_deref()
        .unwrap()
        .contains(EVALUATION_MODEL_KEY_ENV));
}

#[tokio::test]
async fn invalid_claim_bundle_still_writes_failed_attempt_artifacts() {
    let root = tempfile::tempdir().unwrap();
    let bundle = root.path().join("invalid-claims.yaml");
    tokio::fs::write(&bundle, "claims: [not valid yaml")
        .await
        .unwrap();
    let config = EvaluationAttemptConfig {
        schema_version: EVALUATION_SCHEMA_VERSION,
        attempt_id: "attempt-invalid-bundle".into(),
        task_prompt: "修复测试".into(),
        workspace_root: root.path().join("workspace"),
        runtime_root: root.path().join("runtime"),
        acn_config: root.path().join("unused-acn.toml"),
        output_dir: root.path().join("output"),
        upstream: "eval".into(),
        variant: "B_claim".into(),
        attempt_deadline_secs: 1,
        model_egress_mode: "pier".into(),
        claim_bundle: Some(bundle.clone()),
    };
    tokio::fs::create_dir_all(&config.workspace_root)
        .await
        .unwrap();

    let result = run_attempt(config.clone()).await.unwrap();
    let events = tokio::fs::read_to_string(config.output_dir.join("events.jsonl"))
        .await
        .unwrap();
    let result_json = tokio::fs::read_to_string(config.output_dir.join("result.json"))
        .await
        .unwrap();

    assert_eq!(result.exit_type, "failed");
    assert_eq!(result.agent_steps, 0);
    assert_eq!(result.usage.model_requests, 0);
    assert!(result.router_evidence.is_empty());
    assert!(result.error.as_deref().unwrap().contains("stage=router"));
    assert!(result
        .error
        .as_deref()
        .unwrap()
        .contains(&config.attempt_id));
    assert!(result
        .error
        .as_deref()
        .unwrap()
        .contains(&config.workspace_root.display().to_string()));
    assert!(events.contains("attempt_started"));
    assert!(events.contains("attempt_failed"));
    assert!(events.contains("attempt_finished"));
    assert!(result_json.contains("\"exit_type\": \"failed\""));
    // bundle 已在 parse 前消费并删除，保持模型无法绕过 router 读取原文件的边界。
    assert!(!bundle.exists());
}

#[test]
fn evaluation_paths_are_absolute_and_versioned() {
    let config = EvaluationAttemptConfig {
        schema_version: EVALUATION_SCHEMA_VERSION,
        attempt_id: "attempt-001".into(),
        task_prompt: "修复测试".into(),
        workspace_root: PathBuf::from("/tmp/acn-eval/workspace"),
        runtime_root: PathBuf::from("/tmp/acn-eval/runtime"),
        acn_config: PathBuf::from("/tmp/acn-eval/acn.toml"),
        output_dir: PathBuf::from("/tmp/acn-eval/output"),
        upstream: "benchmark".into(),
        variant: "A".into(),
        attempt_deadline_secs: 5100,
        model_egress_mode: "pier".into(),
        claim_bundle: None,
    };

    let paths = EvaluationRunPaths::from_config(&config).unwrap();

    assert!(paths.event_ledger.is_absolute());
    assert!(paths.result.is_absolute());
    assert_eq!(paths.event_ledger, config.output_dir.join("events.jsonl"));
    assert_eq!(paths.result, config.output_dir.join("result.json"));

    // 没有自有截止时间就只能被 Pier 墙钟 SIGKILL，届时不会留下任何证据。
    let no_deadline = EvaluationAttemptConfig {
        attempt_deadline_secs: 0,
        ..config
    };
    let error = EvaluationRunPaths::from_config(&no_deadline)
        .unwrap_err()
        .to_string();
    assert!(error.contains("attempt_deadline_secs"));
}

#[test]
fn event_and_result_keep_required_fields_and_explicit_empty_arrays() {
    let event = EvaluationEvent::new(
        "attempt-001",
        3,
        "turn_completed",
        json!({"message_count": 2}),
        Utc::now(),
    );
    let value = serde_json::to_value(event).unwrap();
    assert_eq!(value["schema_version"], EVALUATION_SCHEMA_VERSION);
    assert_eq!(value["attempt_id"], "attempt-001");
    assert_eq!(value["seq"], 3);
    assert_eq!(value["event_type"], "turn_completed");
    assert!(value["timestamp_utc"].is_string());
    assert_eq!(value["payload"]["message_count"], 2);

    let result = EvaluationResult::empty(
        "attempt-001",
        "completed",
        PathBuf::from("/tmp/acn-eval/output/events.jsonl"),
    );
    let result_value = serde_json::to_value(result).unwrap();
    assert_eq!(result_value["schema_version"], EVALUATION_SCHEMA_VERSION);
    assert_eq!(result_value["agent_steps"], 0);
    assert_eq!(result_value["claim_new_ids"], json!([]));
    assert_eq!(result_value["claim_updated_ids"], json!([]));
    assert_eq!(result_value["claim_used_ids"], json!([]));
    assert_eq!(result_value["router_evidence"], json!([]));
    assert_eq!(result_value["router_evidence_incomplete"], false);
    assert_eq!(
        result_value["usage"],
        json!({
            "model_requests": 0,
            "complete_model_responses": 0,
            "incomplete_model_responses": 0,
            "audit_incomplete": false,
            "response_models": [],
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_tokens": 0,
            "reasoning_tokens": 0
        })
    );
    assert_eq!(
        result_value["event_ledger_path"],
        "/tmp/acn-eval/output/events.jsonl"
    );
}

#[tokio::test]
async fn frozen_bundle_router_returns_bundle_claims_only_through_router_client() {
    let matching = sample_claim("claim_11111111", "billing/payment");
    let unrelated = sample_claim("claim_22222222", "search/indexing");
    let router = FrozenClaimBundleRouter::new(
        FrozenClaimBundle {
            schema_version: EVALUATION_SCHEMA_VERSION,
            claims: vec![matching.clone(), unrelated],
        },
        "attempt-001".into(),
        Some("0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a".into()),
    )
    .unwrap();

    let result = RouterClient::query(&router, &AgentQuery::from_scope("billing/payment"))
        .await
        .unwrap();

    assert_eq!(result.candidate_claims.len(), 1);
    assert_eq!(result.candidate_claims[0].claim, matching);
    assert_eq!(result.disputes, Vec::new());
    assert!(
        RouterClient::query(&router, &AgentQuery::from_scope("missing"))
            .await
            .unwrap()
            .candidate_claims
            .is_empty()
    );
}

#[test]
fn frozen_bundle_router_rejects_stale_and_deprecated_claims() {
    let mut stale = sample_claim("claim_33333333", "billing/payment");
    stale.status = ClaimStatus::Stale;
    let error = FrozenClaimBundleRouter::new(
        FrozenClaimBundle {
            schema_version: EVALUATION_SCHEMA_VERSION,
            claims: vec![stale],
        },
        "attempt-001".into(),
        Some("0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a".into()),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("active"));
}

fn sample_claim(id: &str, scope: &str) -> Claim {
    Claim {
        id: id.parse::<ClaimId>().unwrap(),
        name: "frozen-claim".into(),
        statement: "该 claim 仅来自冻结 bundle".into(),
        scope: scope.into(),
        holder: AgentId::new("benchmark").unwrap(),
        confidence: Confidence::High,
        status: ClaimStatus::Active,
        created_at: "2026-07-26T00:00:00Z".parse().unwrap(),
        updated_at: None,
        source_claim_ids: Vec::new(),
        evidence_summary: "fixture".into(),
    }
}
