//! 将已接受草稿转换为可读、可重新提交的反馈；不修改草稿或放宽校验。
use super::dream_plan::{OperationGroup, Update};
use crate::claim::{Claim, ClaimId, ClaimStatus};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// 一次指出所有动作字段错误；仅给出结构修复提示，不自动丢弃模型提出的内容变化。
pub(super) fn operation_errors(attempted: &Value) -> Vec<Value> {
    let mut errors = Vec::new();
    for op in attempted
        .pointer("/group/operations")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(changes) = op.get("changes").and_then(Value::as_object) else {
            continue;
        };
        let allowed: &[&str] = match op["action"].as_str() {
            Some("keep") => &[],
            Some("deprecate") => &["evidence_summary"],
            Some("update") => &[
                "name",
                "statement",
                "scope",
                "confidence",
                "status",
                "source_claim_ids",
                "evidence_summary",
            ],
            _ => continue,
        };
        let unexpected = changes
            .keys()
            .filter(|key| !allowed.contains(&key.as_str()))
            .collect::<Vec<_>>();
        if !unexpected.is_empty() {
            errors.push(json!({"claim_id":op["id"],"action":op["action"],"unexpected_fields":unexpected,
                "allowed_changes":allowed,"repair":"Remove these keys from changes; do not send null or repeat original values. For deprecate the host sets status and preserves original content. If content must survive elsewhere, put that content in a surviving update and describe it in coverage; do not silently drop intended knowledge."}));
        }
    }
    errors
}

fn operation(update: &Update, readable: &BTreeMap<ClaimId, Claim>) -> Value {
    let original = readable.get(&update.id).map(|claim| json!(claim));
    let fields = json!(update);
    let mut changes = BTreeMap::new();
    if let Some(fields) = fields.as_object() {
        for (key, value) in fields {
            if key != "id"
                && original.as_ref().and_then(|claim| claim.get(key)) != Some(value)
                && (update.status != ClaimStatus::Deprecated || key == "evidence_summary")
            {
                changes.insert(key.clone(), value.clone());
            }
        }
    }
    let action = if update.status == ClaimStatus::Deprecated {
        "deprecate"
    } else if changes.is_empty() {
        "keep"
    } else {
        "update"
    };
    json!({"id":update.id,"action":action,"changes":changes})
}

pub(super) fn group_input(
    id: &str,
    group: &OperationGroup,
    readable: &BTreeMap<ClaimId, Claim>,
) -> Value {
    json!({"group_id":id,"group":{
        "kind":group.kind,"reason":group.reason,"evidence_ids":group.evidence_ids,
        "coverage":group.coverage,"change_basis":group.change_basis,
        "operations":group.updates.iter().map(|u| operation(u, readable)).collect::<Vec<_>>()
    }})
}

pub(super) fn state(
    groups: &BTreeMap<String, OperationGroup>,
    rejected: &BTreeMap<String, String>,
    readable: &BTreeMap<ClaimId, Claim>,
    attempted: &Value,
) -> Value {
    let attempted_group = attempted.get("group_id").and_then(Value::as_str);
    let attempted_operations = attempted
        .pointer("/group/operations")
        .and_then(Value::as_array);
    let mut summaries = Vec::new();
    let mut conflicts = Vec::new();
    for (id, group) in groups {
        let mut operations = Vec::new();
        for update in &group.updates {
            let current = operation(update, readable);
            operations.push(json!({"id":update.id,"action":current["action"],
                "changed_fields":current["changes"].as_object().map(|fields| fields.keys().collect::<Vec<_>>()).unwrap_or_default()}));
            if attempted_group != Some(id.as_str()) {
                if let Some(attempted_operations) = attempted_operations {
                    for proposed in attempted_operations {
                        if proposed.get("id") == current.get("id") {
                            conflicts.push(json!({"claim_id":update.id,"staged_group_id":id,
                                "staged_operation":current,"attempted_action":proposed.get("action")}));
                        }
                    }
                }
            }
        }
        summaries.push(json!({"group_id":id,"kind":group.kind,"operations":operations}));
    }
    json!({"staged_groups":summaries,"pending_rejections":rejected,"conflicts":conflicts,
    "repair_instructions":[
        "This is the current accepted draft, not committed Claim content. read_claim reads original stored claims; use dream_read_draft for staged changes.",
        "For a cross-group conflict, read the named existing groups with dream_read_draft. Changing only the new group_id cannot fix it. In incremental Dream complete candidate_progress.current before another item.",
        "Replace the current group using its SAME group_id and retain all candidate claims, intended edits, evidence and coverage. If needed explicitly expand its candidate registration first; do not silently drop identified work.",
        "pending_rejections identifies groups needing repair or removal of an invalid proposal. group:null removes only the draft/error; a registered candidate remains pending until applied or kept with dream_keep_candidate and a concrete reason. Unknown group names change nothing.",
        "Use dream_validate, then dream_apply_group with a concise semantic review. Finish with empty group_ids only after all candidates are executed or explicitly kept. No modification quota; uncertainty permits keeping. Unchanged independent claims need no keep group; consolidation inputs do. Use actual execution receipts for results."
    ]})
}
