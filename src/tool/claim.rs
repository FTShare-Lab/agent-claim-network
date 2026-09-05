//! `claim` 工具：浏览与编辑当前 agent 自有的 Claim / Trace。

use std::str::FromStr;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};

use super::{ToolDefinition, ToolError, ToolExecution};
use crate::agent::claims::{ClaimUpdate, DEFAULT_CLAIM_LIST_LIMIT, DEFAULT_TRACE_TASK_PAGE_LIMIT};
use crate::agent::AgentRunner;
use crate::claim::{ClaimId, ClaimStatus, Confidence, TraceId};

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum ClaimToolArgs {
    List {
        query: Option<String>,
        include_deprecated: Option<bool>,
        offset: Option<usize>,
        limit: Option<usize>,
    },
    Read {
        id: String,
    },
    Update {
        id: String,
        expected_revision: String,
        name: Option<String>,
        statement: Option<String>,
        scope: Option<String>,
        evidence_summary: Option<String>,
        confidence: Option<Confidence>,
        status: Option<ClaimStatus>,
    },
    Traces {
        claim_id: Option<String>,
        offset: Option<usize>,
        limit: Option<usize>,
    },
    ReadTrace {
        id: String,
        task_offset: Option<usize>,
        task_limit: Option<usize>,
    },
}

pub(super) fn definition() -> ToolDefinition {
    ToolDefinition {
        name: "claim".into(),
        description: "List, search, read, or CAS-update this agent's own claims, and inspect the original task traces connected to them. Updates require the revision returned by read and preserve claim identity, holder, creation time, and sources.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "action": { "type": "string", "enum": ["list", "read", "update", "traces", "read_trace"] },
                "id": { "type": "string", "description": "claim id for read/update, or trace id for read_trace" },
                "expected_revision": { "type": "string", "description": "Required for update; use the exact revision returned by read" },
                "query": { "type": "string", "description": "list only: case-insensitive substring search across name, scope, and statement" },
                "include_deprecated": { "type": "boolean", "default": false, "description": "list only: include deprecated claims so they can be inspected or restored" },
                "claim_id": { "type": "string", "description": "traces only: return traces whose inputs or outputs reference this claim" },
                "offset": { "type": "integer", "minimum": 0, "description": "list/traces result offset" },
                "limit": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20, "description": "list/traces maximum results" },
                "task_offset": { "type": "integer", "minimum": 0, "description": "read_trace only: character offset into task text" },
                "task_limit": { "type": "integer", "minimum": 1, "maximum": 16000, "default": 4000, "description": "read_trace only: maximum task characters" },
                "name": { "type": "string", "description": "update only" },
                "statement": { "type": "string", "description": "update only" },
                "scope": { "type": "string", "description": "update only" },
                "evidence_summary": { "type": "string", "description": "update only" },
                "confidence": { "type": "string", "enum": ["high", "medium", "low"], "description": "update only" },
                "status": { "type": "string", "enum": ["active", "stale", "deprecated"], "description": "update only" }
            },
            "required": ["action"],
            "additionalProperties": false
        }),
    }
}

pub(super) async fn dispatch(
    runner: Option<&Arc<AgentRunner>>,
    input: Value,
) -> Result<ToolExecution, ToolError> {
    let runner = runner.ok_or_else(|| ToolError::UnknownTool("claim".into()))?;
    let args: ClaimToolArgs =
        serde_json::from_value(input).map_err(|error| ToolError::InvalidArgs(error.to_string()))?;
    let output = match args {
        ClaimToolArgs::List {
            query,
            include_deprecated,
            offset,
            limit,
        } => serde_json::to_value(
            runner
                .list_claims(
                    query.as_deref(),
                    include_deprecated.unwrap_or(false),
                    offset.unwrap_or(0),
                    limit.unwrap_or(DEFAULT_CLAIM_LIST_LIMIT),
                )
                .await
                .map_err(domain_error)?,
        ),
        ClaimToolArgs::Read { id } => {
            let id = parse_claim_id(&id)?;
            serde_json::to_value(runner.read_claim(&id).await.map_err(domain_error)?)
        }
        ClaimToolArgs::Update {
            id,
            expected_revision,
            name,
            statement,
            scope,
            evidence_summary,
            confidence,
            status,
        } => {
            let update = ClaimUpdate {
                id: parse_claim_id(&id)?,
                expected_revision,
                name,
                statement,
                scope,
                evidence_summary,
                confidence,
                status,
            };
            serde_json::to_value(runner.update_claim(update).await.map_err(domain_error)?)
        }
        ClaimToolArgs::Traces {
            claim_id,
            offset,
            limit,
        } => {
            let claim_id = claim_id.as_deref().map(parse_claim_id).transpose()?;
            serde_json::to_value(
                runner
                    .list_traces(
                        claim_id.as_ref(),
                        offset.unwrap_or(0),
                        limit.unwrap_or(DEFAULT_CLAIM_LIST_LIMIT),
                    )
                    .await
                    .map_err(domain_error)?,
            )
        }
        ClaimToolArgs::ReadTrace {
            id,
            task_offset,
            task_limit,
        } => {
            let id = TraceId::from_str(&id)
                .map_err(|error| ToolError::InvalidArgs(error.to_string()))?;
            serde_json::to_value(
                runner
                    .read_trace(
                        &id,
                        task_offset.unwrap_or(0),
                        task_limit.unwrap_or(DEFAULT_TRACE_TASK_PAGE_LIMIT),
                    )
                    .await
                    .map_err(domain_error)?,
            )
        }
    }
    .map_err(|error| ToolError::InvalidArgs(format!("claim 输出序列化失败: {error}")))?;
    Ok(ToolExecution::completed(output))
}

fn parse_claim_id(value: &str) -> Result<ClaimId, ToolError> {
    ClaimId::from_str(value).map_err(|error| ToolError::InvalidArgs(error.to_string()))
}

fn domain_error(error: anyhow::Error) -> ToolError {
    ToolError::Claim(format!("{error:#}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::fs::LocalFsClaimStore;
    use crate::agent::LocalClaimStore;
    use crate::claim::AgentId;
    use crate::config::ToolConfig;

    #[test]
    fn action_specific_args_reject_unrelated_fields() {
        let error = serde_json::from_value::<ClaimToolArgs>(json!({
            "action": "read",
            "id": "claim_12345678",
            "limit": 20
        }))
        .unwrap_err();
        assert!(error.to_string().contains("unknown field `limit`"));
    }

    #[tokio::test]
    async fn non_parent_profiles_reject_claim_regardless_of_builder_order() {
        let dir = tempfile::tempdir().unwrap();
        let config = ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..Default::default()
        };
        let runner = crate::agent::claims::tests::runner(&dir);
        let after_attach = super::super::ToolRegistry::new(&config)
            .unwrap()
            .with_claim_runner(runner.clone())
            .for_evaluation("TEST_SECRET".into());
        let before_attach = super::super::ToolRegistry::new(&config)
            .unwrap()
            .for_evaluation("TEST_SECRET".into())
            .with_claim_runner(runner.clone());
        let base = super::super::ToolRegistry::new(&config)
            .unwrap()
            .with_claim_runner(runner);
        let profiles = vec![
            after_attach,
            before_attach,
            base.clone().for_delegation(None),
            base.clone().for_memory_review(),
            base.clone().for_minimal_evaluation("TEST_SECRET".into()),
            base.clone().for_pi_like_evaluation("TEST_SECRET".into()),
            base.for_open_code_like_evaluation("TEST_SECRET".into()),
        ];
        for registry in profiles {
            assert!(!registry
                .definitions()
                .iter()
                .any(|definition| definition.name == "claim"));
            let error = registry
                .dispatch("claim", json!({"action": "list"}))
                .await
                .unwrap_err();
            assert!(matches!(error, ToolError::UnknownTool(_)));
        }
    }

    #[tokio::test]
    async fn parent_registry_dispatches_claim_list_read_update_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let config = ToolConfig {
            workspace_root: dir.path().to_path_buf(),
            ..Default::default()
        };
        let runner = crate::agent::claims::tests::runner(&dir);
        let claim = crate::claim::Claim {
            id: ClaimId::random(),
            name: "example claim".into(),
            statement: "example statement".into(),
            scope: "example scope".into(),
            holder: AgentId::new("agent-a").unwrap(),
            confidence: Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            updated_at: None,
            source_claim_ids: Vec::new(),
            evidence_summary: "example evidence".into(),
        };
        LocalFsClaimStore::new(dir.path().to_path_buf())
            .write_claim(&claim)
            .await
            .unwrap();
        let registry = super::super::ToolRegistry::new(&config)
            .unwrap()
            .with_claim_runner(runner);

        let listed = registry
            .dispatch("claim", json!({"action": "list"}))
            .await
            .unwrap();
        assert_eq!(listed.output["items"][0]["id"], claim.id.as_str());
        let read = registry
            .dispatch("claim", json!({"action": "read", "id": claim.id.as_str()}))
            .await
            .unwrap();
        let revision = read.output["revision"].as_str().unwrap();
        registry
            .dispatch(
                "claim",
                json!({
                    "action": "update",
                    "id": claim.id.as_str(),
                    "expected_revision": revision,
                    "name": "updated claim"
                }),
            )
            .await
            .unwrap();
        let reread = registry
            .dispatch("claim", json!({"action": "read", "id": claim.id.as_str()}))
            .await
            .unwrap();
        assert_eq!(reread.output["claim"]["name"], "updated claim");
    }
}
