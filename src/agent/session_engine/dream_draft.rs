//! Dream 分组草案与即时反馈；只暂存计划，最终写入仍由 Prepared 协议负责。
use super::dream_audit::Audit;
use super::dream_candidates::{self, Candidates};
use super::dream_draft_feedback as feedback;
use super::dream_execution::{Execution, SemanticReview};
use super::dream_plan::{
    validate_plan, ChangeBasis, Coverage, Kind, OperationGroup, Plan, Review, Update,
};
use super::dream_review::{
    basis_error, coverage_gaps, evidence_feedback, review_input, validate_review, ReviewRecord,
    SelfReview, Validation,
};
use crate::claim::{AgentId, Claim, ClaimId, ClaimStatus};
use crate::tool::{DreamPlanTools, ToolDefinition};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct Draft {
    groups: BTreeMap<String, OperationGroup>,
    finished: Option<Plan>,
    #[serde(default)]
    rejected: BTreeMap<String, String>,
    #[serde(default)]
    validations: BTreeMap<String, Validation>,
    #[serde(default)]
    self_reviews: BTreeMap<String, ReviewRecord>,
    #[serde(default)]
    withdrawn: BTreeMap<String, Value>,
    #[serde(default, rename = "self_review_approvals")]
    approved: Vec<ReviewRecord>,
    #[serde(default)]
    proposal_attempted: bool,
    #[serde(default)]
    finish_reminder_sent: bool,
    #[serde(default)]
    candidates: Candidates,
}

impl Draft {
    fn finish_execution(&mut self, group_id: &str, validation_id: &str, accepted: bool) {
        self.validations.remove(group_id);
        self.self_reviews.remove(group_id);
        if accepted {
            self.groups.remove(group_id);
            self.rejected.remove(group_id);
            self.candidates.applied(group_id, validation_id);
        } else {
            // 业务拒绝不是完成：保留意图，让同一模型基于最新状态重新决定。
            self.rejected.insert(group_id.into(), "Claim/evidence changed at commit; original draft retained. Read execution feedback, revise this candidate and validate again, or explicitly keep it.".into());
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage {
    group_id: String,
    group: Option<Group>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Group {
    kind: Kind,
    reason: String,
    #[serde(default)]
    evidence_ids: Vec<String>,
    #[serde(default)]
    coverage: Vec<Coverage>,
    #[serde(default)]
    change_basis: Vec<ChangeBasis>,
    operations: Vec<Operation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Operation {
    id: ClaimId,
    action: Action,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    changes: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Keep,
    Update,
    Deprecate,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Finish {
    review: Review,
    // 兼容旧调用和批量结束流程；逐组执行模式不再向模型暴露此字段。
    #[serde(default)]
    group_ids: Vec<String>,
    #[serde(default)]
    self_reviews: Vec<SelfReview>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadDraft {
    #[serde(default)]
    group_ids: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Apply {
    group_id: String,
    validation_id: String,
    review: SemanticReview,
}

pub(super) struct Reviewer {
    owner: AgentId,
    readable: BTreeMap<ClaimId, Claim>,
    draft: Mutex<Draft>,
    audit: Option<Arc<Audit>>,
    fatal_error: Mutex<Option<anyhow::Error>>,
    history: Option<Arc<super::dream_history::History>>,
    execution: Option<Arc<Execution>>,
}

impl Reviewer {
    pub(super) fn new(
        owner: AgentId,
        claims: Vec<Claim>,
        draft: Draft,
        audit: Option<Arc<Audit>>,
    ) -> Self {
        Self {
            owner,
            readable: claims
                .into_iter()
                .map(|claim| (claim.id.clone(), claim))
                .collect(),
            draft: Mutex::new(draft),
            audit,
            fatal_error: Mutex::new(None),
            history: None,
            execution: None,
        }
    }

    pub(super) fn with_history(mut self, history: Arc<super::dream_history::History>) -> Self {
        self.history = Some(history);
        self
    }

    pub(super) fn with_execution(mut self, execution: Arc<Execution>) -> Self {
        let draft = self.draft.get_mut();
        for (id, group) in &draft.groups {
            draft.candidates.remember_group(id, group);
        }
        self.execution = Some(execution);
        self
    }

    pub(super) async fn reconcile_executions(&self) -> anyhow::Result<()> {
        if let Some(execution) = &self.execution {
            let records = execution.records().await?;
            let mut draft = self.draft.lock().await;
            for (_, record) in records {
                let Some(operation) = record.operation.filter(|_| record.applied) else {
                    continue;
                };
                // Claim 提交后、探索检查点保存前可能中断；只清除对应的旧草稿版本。
                if draft
                    .validations
                    .get(&operation.group_id)
                    .is_some_and(|v| v.validation_id == operation.validation_id)
                {
                    draft.finish_execution(
                        &operation.group_id,
                        &operation.validation_id,
                        !record.groups.iter().any(|group| group.skipped),
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) async fn snapshot(&self) -> Draft {
        self.draft.lock().await.clone()
    }

    pub(super) async fn finished(&self) -> Option<Plan> {
        self.draft.lock().await.finished.clone()
    }

    pub(super) async fn take_fatal_error(&self) -> Option<anyhow::Error> {
        self.fatal_error.lock().await.take()
    }

    pub(super) async fn approved(&self) -> Vec<ReviewRecord> {
        self.draft.lock().await.approved.clone()
    }

    pub(super) async fn resume_input(&self, observed: &BTreeMap<ClaimId, Claim>) -> Value {
        let mut readable = self.readable.clone();
        readable.extend(observed.clone());
        let draft = self.draft.lock().await;
        // 恢复时展示真实草案和已校验版本，不重新复制完整原文。
        json!({"execution":match &self.execution {Some(e)=>e.context().await.unwrap_or_else(|error|json!({"error":error.to_string()})),None=>Value::Null},"groups":draft.groups.iter().map(|(id, g)| feedback::group_input(id, g, &readable)).collect::<Vec<_>>(),
            "pending_rejections":draft.rejected,"validated_groups":draft.validations.iter().map(|(id,v)| (id,&v.validation_id)).collect::<BTreeMap<_,_>>(),
            "reviewed_groups":draft.self_reviews.keys().collect::<Vec<_>>(),"withdrawn":draft.withdrawn,
            "candidate_progress":draft.candidates.context()})
    }

    /// 旧 JSON 计划也进入同一个内容复核入口，不能绕过新门槛。
    pub(super) async fn import_legacy(&self, plan: Plan) -> Value {
        let mut draft = self.draft.lock().await;
        draft.groups = plan
            .groups
            .into_iter()
            .enumerate()
            .map(|(i, group)| (format!("legacy_{i}"), group))
            .collect();
        if self.execution.is_some() {
            let groups = draft.groups.clone();
            for (id, group) in &groups {
                draft.candidates.remember_group(id, group);
            }
        }
        json!({"review":plan.review,"group_ids":draft.groups.keys().collect::<Vec<_>>()})
    }

    pub(super) async fn is_empty(&self) -> bool {
        let draft = self.draft.lock().await;
        draft.groups.is_empty() && draft.rejected.is_empty() && !draft.candidates.pending()
    }

    fn expand(
        &self,
        mut group: Group,
        readable: &BTreeMap<ClaimId, Claim>,
    ) -> anyhow::Result<OperationGroup> {
        let mut updates = Vec::new();
        for operation in group.operations {
            let id = &operation.id;
            let original = readable.get(id).with_context(|| {
                format!("operation {id}: claim must exist and be completely read")
            })?;
            if let Some(reason) = &operation.reason {
                // 纯记录清理只改变状态；将逐条理由规范化为同一套可审计依据。
                anyhow::ensure!(
                    group.kind == Kind::Quality
                        && matches!(operation.action, Action::Deprecate)
                        && operation.changes.is_empty(),
                    "operation {id}: reason shorthand is only for quality deprecate without changes. Repair parameters: remove operation.reason and add one group.change_basis entry for this changed Claim with claim_id, removed_or_changed, added (empty if none), justification, evidence_ids. Keep group.reason and intended knowledge; do not change kind merely to accept shorthand."
                );
                anyhow::ensure!(
                    !reason.trim().is_empty(),
                    "operation {id}: explain why this record contains no reusable knowledge"
                );
                anyhow::ensure!(
                    !group.change_basis.iter().any(|basis| &basis.claim_id == id),
                    "operation {id}: use either reason shorthand or explicit change_basis, not both"
                );
                group.change_basis.push(ChangeBasis {
                    claim_id: id.clone(),
                    removed_or_changed: "Remove this episodic record from active memory; preserve its original fields in history.".into(),
                    added: String::new(),
                    justification: reason.clone(),
                    evidence_ids: Vec::new(),
                });
            }
            // 只允许 Update 的可变字段；holder、id、created_at 等无法通过合并进入。
            let mut value = json!({"id":id,"name":original.name,"statement":original.statement,
                "scope":original.scope,"confidence":original.confidence,"status":original.status,
                "source_claim_ids":original.source_claim_ids,"evidence_summary":original.evidence_summary});
            match operation.action {
                Action::Keep => anyhow::ensure!(
                    operation.changes.is_empty(),
                    "operation {id}: keep forbids changes"
                ),
                Action::Update => anyhow::ensure!(
                    !operation.changes.is_empty(),
                    "operation {id}: update requires changes"
                ),
                Action::Deprecate => {
                    anyhow::ensure!(operation.changes.keys().all(|key| key == "evidence_summary"), "operation {id}: deprecate only accepts evidence_summary; content stays in history");
                    value["status"] = json!("deprecated");
                }
            }
            for (key, field) in operation.changes {
                anyhow::ensure!(
                    key != "id" && value.get(&key).is_some(),
                    "operation {id}: unsupported changes.{key}"
                );
                value[&key] = field;
            }
            let update: Update = serde_json::from_value(value)
                .with_context(|| format!("operation {id}: invalid changes"))?;
            anyhow::ensure!(
                matches!(operation.action, Action::Deprecate)
                    || update.status != ClaimStatus::Deprecated,
                "operation {id}: use deprecate action for deprecation"
            );
            updates.push(update);
        }
        Ok(OperationGroup {
            kind: group.kind,
            reason: group.reason,
            evidence_ids: group.evidence_ids,
            coverage: group.coverage,
            change_basis: group.change_basis,
            updates,
        })
    }

    fn accept_reviews(
        draft: &mut Draft,
        reviews: Vec<SelfReview>,
        readable: &BTreeMap<ClaimId, Claim>,
        receipts: &BTreeMap<String, Value>,
    ) -> anyhow::Result<()> {
        for review in reviews {
            let validation = draft
                .validations
                .get(&review.group_id)
                .context("Call dream_validate before self-review")?;
            let group = draft
                .groups
                .get(&review.group_id)
                .context("Self-review group is not staged")?;
            anyhow::ensure!(
                validation.input == review_input(&review.group_id, group, readable, receipts),
                "Draft changed since validation; validate and review this group again"
            );
            validate_review(validation, &review)?;
            let record = ReviewRecord {
                validation: validation.clone(),
                review,
            };
            draft
                .self_reviews
                .insert(record.review.group_id.clone(), record);
        }
        Ok(())
    }

    async fn handle(
        &self,
        name: &str,
        input: Value,
        receipts: BTreeMap<String, Value>,
        observed: BTreeMap<ClaimId, Claim>,
    ) -> anyhow::Result<Value> {
        if name == "dream_review_sync" {
            return self
                .execution
                .as_ref()
                .context("Incremental executor unavailable")?
                .review_sync(input)
                .await;
        }
        if name == "dream_read_history" {
            return self
                .history
                .as_ref()
                .context("Dream history unavailable")?
                .read(input)
                .await;
        }
        let mut readable = self.readable.clone();
        readable.extend(observed);
        if let Some(execution) = &self.execution {
            execution.ensure_no_pending().await?;
            for (id, claim) in execution.current().await? {
                if readable.contains_key(&id) {
                    readable.insert(id, claim);
                }
            }
        }
        let mut draft = self.draft.lock().await;
        anyhow::ensure!(draft.finished.is_none(), "Dream plan is already finished");
        if name == "dream_stage_group" && input.get("group").is_some_and(|g| !g.is_null()) {
            draft.proposal_attempted = true;
        }
        match name {
            "dream_record_candidates" => {
                let execution = self
                    .execution
                    .as_ref()
                    .context("Incremental executor unavailable")?;
                let register: dream_candidates::Register =
                    serde_json::from_value(input).context("dream_record_candidates arguments")?;
                let ineligible: Vec<_> = register
                    .candidates
                    .iter()
                    .filter(|candidate| candidate.kind == Kind::Evidence)
                    .flat_map(|candidate| {
                        candidate
                            .claim_ids
                            .iter()
                            .filter(|id| execution.original_high_confidence(id))
                            .map(|id| {
                                format!("{}: {id} (original confidence=high)", candidate.group_id)
                            })
                    })
                    .collect();
                anyhow::ensure!(ineligible.is_empty(),
                    "B candidates rejected: {}. No candidates from this request were registered. Remove these IDs from B and resubmit the other candidates; high Claims may still participate in A/C without factual correction. Earlier A/C edits cannot bypass the original high gate.", ineligible.join(", "));
                draft
                    .candidates
                    .register(register.candidates, &readable, &self.owner)?;
                Ok(
                    json!({"accepted":true,"committed":false,"note":"Candidates recorded. Handle current candidate before the next; this does not authorize any Claim modification."}),
                )
            }
            "dream_keep_candidate" => {
                anyhow::ensure!(self.execution.is_some(), "Incremental executor unavailable");
                let keep: dream_candidates::Keep =
                    serde_json::from_value(input).context("dream_keep_candidate arguments")?;
                let id = keep.group_id.clone();
                draft.candidates.keep(keep)?;
                if let Some(group) = draft.groups.remove(&id) {
                    draft.withdrawn.insert(id.clone(), json!(group));
                }
                draft.validations.remove(&id);
                draft.self_reviews.remove(&id);
                draft.rejected.remove(&id);
                Ok(
                    json!({"accepted":true,"committed":false,"note":"Candidate kept with a reason; no Claim fields changed. Continue with the next candidate."}),
                )
            }
            "dream_apply_group" => {
                let apply: Apply =
                    serde_json::from_value(input).context("dream_apply_group arguments")?;
                let execution = self
                    .execution
                    .as_ref()
                    .context("Incremental executor unavailable")?;
                if let Some(result) = execution
                    .replay(&apply.validation_id, &apply.group_id)
                    .await?
                {
                    if draft
                        .validations
                        .get(&apply.group_id)
                        .is_some_and(|v| v.validation_id == apply.validation_id)
                    {
                        draft.finish_execution(
                            &apply.group_id,
                            &apply.validation_id,
                            result["accepted"] == true,
                        );
                    }
                    return Ok(result);
                }
                draft.candidates.require_current(&apply.group_id)?;
                let validation = draft
                    .validations
                    .get(&apply.group_id)
                    .context("Call dream_validate before execution")?
                    .clone();
                anyhow::ensure!(
                    validation.validation_id == apply.validation_id,
                    "Validation is for a different draft version"
                );
                let group = draft
                    .groups
                    .get(&apply.group_id)
                    .context("Group is not staged")?
                    .clone();
                draft.candidates.stage(&apply.group_id, &group)?;
                let result = execution
                    .apply(group, validation, apply.review, &readable, &receipts)
                    .await?;
                draft.finish_execution(
                    &apply.group_id,
                    &apply.validation_id,
                    result["accepted"] == true,
                );
                Ok(result)
            }
            "dream_read_draft" => {
                let query: ReadDraft =
                    serde_json::from_value(input).context("dream_read_draft arguments")?;
                for id in &query.group_ids {
                    anyhow::ensure!(draft.groups.contains_key(id), "No accepted draft group {id}; consult staged_groups and pending_rejections");
                }
                let groups = draft
                    .groups
                    .iter()
                    .filter(|(id, _)| query.group_ids.is_empty() || query.group_ids.contains(id))
                    .map(|(id, group)| feedback::group_input(id, group, &readable))
                    .collect::<Vec<_>>();
                Ok(
                    json!({"accepted":true,"committed":false,"groups":groups,"execution":match &self.execution {Some(e)=>e.context().await?,None=>Value::Null}}),
                )
            }
            "dream_stage_group" => {
                anyhow::ensure!(
                    input.get("group").is_some(),
                    "group is required; use explicit null to remove a group"
                );
                let errors = feedback::operation_errors(&input);
                anyhow::ensure!(errors.is_empty(), "Operation fields do not match their action; fix all operation_feedback entries and resubmit the same group_id");
                let stage: Stage =
                    serde_json::from_value(input).context("dream_stage_group arguments")?;
                anyhow::ensure!(
                    !stage.group_id.trim().is_empty(),
                    "group_id must not be empty"
                );
                let mut removed = json!({"staged_group":false,"rejected_error":false});
                if let Some(group) = stage.group {
                    let group = self
                        .expand(group, &readable)
                        .with_context(|| format!("group {}", stage.group_id))?;
                    let mut candidate = draft.groups.clone();
                    candidate.insert(stage.group_id.clone(), group.clone());
                    let plan = Plan {
                        review: Review {
                            quality: "draft".into(),
                            evidence: "draft".into(),
                            consolidation: "draft".into(),
                        },
                        groups: candidate.values().cloned().collect(),
                    };
                    validate_plan(
                        &plan,
                        &readable,
                        &self.owner,
                        &receipts,
                        crate::time::now_seconds(),
                    )
                    .with_context(|| {
                        format!(
                            "group {}: rejected; previously accepted draft unchanged",
                            stage.group_id
                        )
                    })?;
                    if let Some(error) = basis_error(
                        &group,
                        &review_input(&stage.group_id, &group, &readable, &receipts),
                    ) {
                        anyhow::bail!(
                            "group {}: {error}; previously accepted draft unchanged",
                            stage.group_id
                        );
                    }
                    if self.execution.is_some() {
                        draft.candidates.stage(&stage.group_id, &group)?;
                    }
                    let unchanged = draft
                        .groups
                        .get(&stage.group_id)
                        .is_some_and(|old| json!(old) == json!(group));
                    if !unchanged {
                        draft.validations.remove(&stage.group_id);
                        draft.self_reviews.remove(&stage.group_id);
                    }
                    draft.groups.insert(stage.group_id.clone(), group);
                    draft.withdrawn.remove(&stage.group_id);
                } else {
                    let prior = draft.groups.remove(&stage.group_id);
                    removed["staged_group"] = json!(prior.is_some());
                    if let Some(prior) = prior {
                        draft.withdrawn.insert(stage.group_id.clone(), json!(prior));
                    }
                    draft.validations.remove(&stage.group_id);
                    draft.self_reviews.remove(&stage.group_id);
                    removed["rejected_error"] = json!(draft.rejected.contains_key(&stage.group_id));
                }
                draft.rejected.remove(&stage.group_id);
                Ok(
                    json!({"accepted":true,"staged_group_ids":draft.groups.keys().collect::<Vec<_>>(),"committed":false,"removed":removed}),
                )
            }
            "dream_validate" => {
                let query: ReadDraft =
                    serde_json::from_value(input).context("dream_validate arguments")?;
                let plan = Plan {
                    review: Review {
                        quality: "validation".into(),
                        evidence: "validation".into(),
                        consolidation: "validation".into(),
                    },
                    groups: draft.groups.values().cloned().collect(),
                };
                validate_plan(
                    &plan,
                    &readable,
                    &self.owner,
                    &receipts,
                    crate::time::now_seconds(),
                )?;
                let ids: Vec<String> = if query.group_ids.is_empty() {
                    if self.execution.is_some() {
                        draft
                            .candidates
                            .current_id()
                            .filter(|id| draft.groups.contains_key(*id))
                            .map(str::to_owned)
                            .into_iter()
                            .collect()
                    } else {
                        draft.groups.keys().cloned().collect()
                    }
                } else {
                    query.group_ids
                };
                let mut validated = Vec::new();
                for id in ids {
                    if self.execution.is_some() {
                        draft.candidates.require_current(&id)?;
                    }
                    let group = draft
                        .groups
                        .get(&id)
                        .with_context(|| format!("No accepted draft group {id}"))?
                        .clone();
                    if self.execution.is_some() {
                        draft.candidates.stage(&id, &group)?;
                    }
                    let input = review_input(&id, &group, &readable, &receipts);
                    if let Some(error) = basis_error(&group, &input) {
                        anyhow::bail!("{id}: {error}");
                    }
                    for receipt in input["evidence"]
                        .as_object()
                        .into_iter()
                        .flat_map(|m| m.values())
                    {
                        anyhow::ensure!(
                            crate::tool::dream_evidence_is_current(receipt).await?,
                            "Evidence changed; reread it and update the group before validation"
                        );
                    }
                    let validation = Validation::new(input)?;
                    draft.validations.insert(id, validation.clone());
                    validated.push(validation);
                }
                Ok(
                    json!({"accepted":true,"committed":false,"validated_groups":validated,"next":"Review the actual before/after in this context. Review useful rules, conditions, alternatives, evidence boundaries and name/body consistency. Repair or withdraw uncertain groups. Execute each validated group with dream_apply_group and a concise per-Claim semantic review, then continue; structural validation is not semantic approval."}),
                )
            }
            "dream_review_group" => {
                let review: SelfReview =
                    serde_json::from_value(input).context("dream_review_group arguments")?;
                let group_id = review.group_id.clone();
                Self::accept_reviews(&mut draft, vec![review], &readable, &receipts)?;
                Ok(
                    json!({"accepted":true,"committed":false,"reviewed_group":group_id,"next":"Review remaining groups, then call dream_finish with all group_ids and the ABC report; cached reviews need not be repeated."}),
                )
            }
            "dream_finish" => {
                let finish: Finish =
                    serde_json::from_value(input).context("dream_finish arguments")?;
                if let Some(execution) = &self.execution {
                    anyhow::ensure!(draft.groups.is_empty() && draft.rejected.is_empty(), "Unexecuted groups remain. Execute them with dream_apply_group or explicitly withdraw them; dream_finish does not write Claims");
                    anyhow::ensure!(finish.group_ids.is_empty() && finish.self_reviews.is_empty(), "Executed groups are already recorded; finish with only an accurate ABC review summary");
                    anyhow::ensure!(!draft.candidates.pending(), "Identified candidates remain: {}. Complete the current candidate or use dream_keep_candidate with a specific safety/no-change reason; prioritizing other tasks does not complete this Dream", draft.candidates.context()["pending"]);
                    anyhow::ensure!(
                        [
                            &finish.review.quality,
                            &finish.review.evidence,
                            &finish.review.consolidation
                        ]
                        .iter()
                        .all(|s| !s.trim().is_empty()),
                        "Summarize actual A/B/C coverage"
                    );
                    // 只提醒一次，不用调用数量或修改配额代替语义判断；状态随草稿恢复。
                    if !readable.is_empty()
                        && !draft.finish_reminder_sent
                        && !draft.proposal_attempted
                        && draft.candidates.is_empty()
                        && receipts.is_empty()
                        && execution.records().await?.is_empty()
                    {
                        draft.finish_reminder_sent = true;
                        return Ok(json!({
                            "accepted":false,"finished":false,"confirmation_required":true,
                            "activity":{"exploration_receipts":0,"proposal_attempted":false,"executed_groups":0},
                            "note":"本轮尚无探索回执或修改提案。这是一次结束前提醒，不要求增加工具调用或修改数量。请检查：A 判断记忆层级，high 也可清理纯工作记录；B 才受 medium/low 门槛限制；C 可以保留分开的知识。尚未暂存/受理不是不能提出候选的理由。可以继续探索或暂存草稿；若仍决定全部保留，请在 ABC 总结中简述代表 Claim 的保留依据、B 未选候选的原因（未探索不等于查证后缺证据），再调用 dream_finish 即可结束，不会重复此提醒。"
                        }));
                    }
                    draft.finished = Some(Plan {
                        review: finish.review,
                        groups: vec![],
                    });
                    return Ok(
                        json!({"accepted":true,"finished":true,"execution":execution.context().await?,"candidate_progress":draft.candidates.context(),"note":"Only run summary finalized; actual execution results and candidate decisions above are authoritative. Executed changes were already durable."}),
                    );
                }
                anyhow::ensure!(draft.rejected.is_empty(), "Some groups still have unhandled validation errors: {:?}. Repair them or explicitly withdraw with group:null; do not silently abandon consolidation because of a format error", draft.rejected);
                let mut ids = finish.group_ids.clone();
                ids.sort();
                anyhow::ensure!(ids == draft.groups.keys().cloned().collect::<Vec<_>>(), "group_ids must list every staged group exactly once; remove unwanted groups explicitly with group:null");
                let plan = Plan {
                    review: finish.review,
                    groups: draft.groups.values().cloned().collect(),
                };
                validate_plan(
                    &plan,
                    &readable,
                    &self.owner,
                    &receipts,
                    crate::time::now_seconds(),
                )?;
                Self::accept_reviews(&mut draft, finish.self_reviews, &readable, &receipts)?;
                let approvals = draft.self_reviews.values().cloned().collect::<Vec<_>>();
                super::dream_review::validate_approvals(&plan, &readable, &receipts, &approvals)?;
                draft.approved = approvals;
                draft.finished = Some(plan);
                Ok(json!({"accepted":true,"ready_for_commit":true,"committed":false}))
            }
            _ => anyhow::bail!("unknown Dream plan tool"),
        }
    }
}

#[async_trait::async_trait]
impl DreamPlanTools for Reviewer {
    fn definitions(&self) -> Vec<ToolDefinition> {
        let string = json!({"type":"string"});
        let strings = json!({"type":"array","items":string});
        let changes = json!({"type":"object","properties":{
            "name":string,"statement":string,"scope":string,"evidence_summary":string,
            "confidence":{"type":"string","enum":["low","medium","high"]},
            "status":{"type":"string","enum":["active","stale"]},"source_claim_ids":strings
        },"minProperties":1,"additionalProperties":false});
        let operation = json!({"anyOf":[
            {"type":"object","properties":{"id":string,"action":{"type":"string","enum":["keep"]},
                "changes":{"type":"object","properties":{},"additionalProperties":false}},"required":["id","action"],"additionalProperties":false},
            {"type":"object","properties":{"id":string,"action":{"type":"string","enum":["update"]},"changes":changes},
                "required":["id","action","changes"],"additionalProperties":false},
            {"type":"object","properties":{"id":string,"action":{"type":"string","enum":["deprecate"]},
                "reason":{"type":"string","description":"For quality deprecate with no changes only: explain why this Claim contains no reusable knowledge. Host creates its change_basis; do not also supply explicit basis for this ID."},
                "changes":{"type":"object","properties":{"evidence_summary":string},"additionalProperties":false}},
                "required":["id","action"],"additionalProperties":false}
        ]});
        let coverage = json!({"type":"array","items":{"type":"object","properties":{
            "input_id":string,"output_ids":strings,"note":string},"required":["input_id","output_ids","note"],"additionalProperties":false}});
        let basis = json!({"type":"array","items":{"type":"object","properties":{
            "claim_id":string,"removed_or_changed":string,"added":string,"justification":string,"evidence_ids":strings
        },"required":["claim_id","removed_or_changed","added","justification","evidence_ids"],"additionalProperties":false}});
        let group = json!({"type":"object","properties":{
            "kind":{"type":"string","enum":["quality","evidence","consolidation"]},
            "reason":string,"evidence_ids":strings,"coverage":coverage,"change_basis":basis,
            "operations":{"type":"array","minItems":1,"items":operation}
        },"required":["kind","reason","operations"],"additionalProperties":false});
        let mut definitions = vec![
            super::dream_history::definition(),
            ToolDefinition {
                name: "dream_review_group".into(),
                description: "Submit one version-bound self-review from dream_validate. Accepted reviews are cached without finishing the job or changing Claims. Repair this group's missing_fragments in the same context; other groups' accepted reviews remain. After every changed group is reviewed, dream_finish needs only group_ids and the ABC report.".into(),
                input_schema: super::dream_review::review_schema()["items"].clone(),
            },
            ToolDefinition {
                name: "dream_read_draft".into(),
                description: "Read accepted pending groups, including exact operations, changed fields, evidence and coverage in dream_stage_group input format. Optional group_ids selects groups; omit or [] for all. This reads the staged plan, not Claim files. Use after conflicts or before replacing a group so existing edits are preserved.".into(),
                input_schema: json!({"type":"object","properties":{"group_ids":strings},"additionalProperties":false}),
            },
            ToolDefinition {
                name: "dream_stage_group".into(),
                description: "Stage or replace one group by stable group_id; group:null explicitly removes it. No Claim files are written. keep preserves all fields; update supplies only changed fields; deprecate changes accepts ONLY evidence_summary: omit status and all content fields, even unchanged values or null; the host sets deprecated. Pure A cleanup may use operations:[{id,action:\"deprecate\",reason:\"specific reason it has no reusable knowledge\"}] with kind=quality and group reason, omitting changes and empty arrays; host creates the per-Claim change_basis. Other changes require explicit change_basis. B evidence and C coverage remain mandatory when applicable. Read operation_feedback to remove every forbidden key before resubmitting. Every B evidence change, including deprecation, requires a new evidence_summary and claim-specific versioned file_read receipts in change_basis.evidence_ids. Evidence must support the exact change; absent evidence means keep, not a weaker rewrite. Consolidation must give an action AND coverage for every input, reusing existing IDs. Read accepted/error and draft_state feedback. For conflicts, dream_read_draft shows the existing group; replace that group or release its claims before moving them, preserving prior edits and unrelated operations. Renaming the new group does not fix a conflict. Each claim belongs to at most one group; combine its A/B/C edits there.".into(),
                input_schema: json!({"type":"object","properties":{"group_id":string,"group":{"anyOf":[group,{"type":"null"}]}},"required":["group_id","group"],"additionalProperties":false}),
            },
            ToolDefinition {
                name: "dream_validate".into(),
                description: "Run backend validation without another model call or Claim writes. Returns version-bound canonical original/target claims, real field differences and versioned evidence. Optional group_ids selects groups; omit/empty selects all. Then perform concrete bidirectional self-review, repair groups in place or withdraw uncertain changes, and submit each review separately with dream_review_group using the returned validation_id.".into(),
                input_schema: json!({"type":"object","properties":{"group_ids":strings},"additionalProperties":false}),
            },
            ToolDefinition {
                name: "dream_finish".into(),
                description: "Finish using all staged group_ids and ABC summary after dream_review_group accepted every changed group. Omit self_reviews to reuse cached reviews; inline self_reviews remains supported for recovery. Review every original name/statement/scope/evidence_summary against surviving outputs, and every output against original quotes or new file evidence. Backend checks quote coverage and references, not semantic truth. Repair errors in this same context, or withdraw uncertain groups. Unchanged accepted self-reviews are reused; changed groups must be validated/reviewed again. Summarize only the current accepted draft as pending host commit; distinguish rejected attempts from final operations. Do not assign resolution or arbitration states. No separate model review is called.".into(),
                input_schema: json!({"type":"object","properties":{"review":{"type":"object","properties":{"quality":string,"evidence":string,"consolidation":string},"required":["quality","evidence","consolidation"],"additionalProperties":false},"group_ids":strings,"self_reviews":super::dream_review::review_schema()},"required":["review","group_ids"],"additionalProperties":false}),
            },
        ];
        if self.execution.is_some() {
            definitions.retain(|d| d.name != "dream_review_group");
            definitions.extend(dream_candidates::definitions());
            definitions.push(ToolDefinition {
                name: "dream_review_sync".into(),
                description: "Reconsider only a blocked consolidation delivery, without changing Claim content/status. Call with source_id and current carrier_ids to read a version-bound validation. Then resubmit the validation_id and review using the same semantic schema as dream_apply_group, with one claims entry for the original source and all carrier_ids as output_ids. Explain complete preservation of its rules, conditions, exceptions and provenance; no factual correction or new evidence is allowed. Uncertainty leaves retirement pending. This cannot bypass A/B/C mutation rules or revive deprecated Claims.".into(),
                input_schema: json!({"type":"object","properties":{"source_id":{"type":"string"},"carrier_ids":{"type":"array","items":{"type":"string"}},"validation_id":{"type":"string"},"review":super::dream_execution::review_schema()},"required":["source_id","carrier_ids"],"additionalProperties":false}),
            });
            definitions.push(ToolDefinition {
                name:"dream_apply_group".into(),
                description:"Execute ONE validated group after semantic self-review. Persists before/after, evidence and review before any Claim write, then records actual results in the agent dream directory. validation_id must match the current draft; repeat calls return the existing receipt. Review each input's useful rules, conditions, exceptions and removal/correction basis. Do not paste all original fields. On rejection repair this group or withdraw it. Successful execution is immediate: use returned current_claims for subsequent work, even if the run later fails.".into(),
                input_schema:json!({"type":"object","properties":{"group_id":string,"validation_id":string,"review":super::dream_execution::review_schema()},"required":["group_id","validation_id","review"],"additionalProperties":false}),
            });
            for d in &mut definitions {
                if d.name == "dream_finish" {
                    d.description = "Finish with only an accurate ABC review summary after every identified candidate is executed or kept with a concrete reason using dream_keep_candidate. Outstanding candidates block finish even after another group succeeded. group:null does not handle its candidate. Other tasks or priorities do not justify abandoning known work. Returns actual execution/candidate results, not self-reported counts. A first finish with no activity returns one reminder. Zero changes remain valid; no tool/modification quota. Committed operations survive later errors.".into();
                    if let Some(properties) = d.input_schema["properties"].as_object_mut() {
                        properties.remove("group_ids");
                        properties.remove("self_reviews");
                    }
                    d.input_schema["required"] = json!(["review"]);
                } else if d.name == "dream_validate" {
                    d.description = "Check the current candidate's staged group without writing Claims. Returns version-bound real before/after, changes and evidence. Review the semantics including name/body/scope consistency, repair if needed, then execute this group with dream_apply_group using validation_id before continuing. Structural validity does not prove semantic correctness.".into();
                } else if d.name == "dream_stage_group" {
                    d.description = "Stage or replace the current candidate's complete plan using the SAME group_id, exact claim_ids and kind. If none is pending, a ready plan auto-registers. No Claim writes. group:null removes only the draft/error; its candidate remains until applied or explicitly kept. Complete this item before staging another. keep preserves all fields; update supplies only changed fields; deprecate changes accepts ONLY evidence_summary, never status or content fields. Pure A cleanup can omit empty arrays and supply an operation reason instead of change_basis: {id,action:\"deprecate\",reason:\"why it has no reusable knowledge\"}, with no changes. Other modifications need explicit change_basis; B evidence and C coverage remain required. Each B change needs a new evidence_summary and claim-specific versioned file_read receipts in change_basis.evidence_ids supporting the exact change. Consolidation needs an action AND coverage for every input, reusing existing IDs. Read accepted/error, operation_feedback and candidate_progress. Repair this group in place; explicitly refine its registration if targets change without silently dropping claims. Do not rename a group to bypass conflicts. Uncertainty means keep original knowledge, not an unsupported rewrite.".into();
                }
            }
        }
        definitions
    }

    async fn call(
        &self,
        name: &str,
        input: Value,
        receipts: BTreeMap<String, Value>,
        observed: BTreeMap<ClaimId, Claim>,
    ) -> Value {
        if name == "dream_read_history" {
            return match self.handle(name, input, receipts, observed).await {
                Ok(value) => value,
                Err(error) => json!({"error":format!("{error:#}"),"accepted":false}),
            };
        }
        let group_id = if name == "dream_stage_group" {
            input
                .get("group_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        } else {
            None
        };
        let mut readable = self.readable.clone();
        readable.extend(observed.clone());
        let audit_receipts = receipts.clone();
        let audit_observed = observed.clone();
        let mut result = match self.handle(name, input.clone(), receipts, observed).await {
            Ok(result) => result,
            Err(error) => {
                let mut draft = self.draft.lock().await;
                if let Some(id) = group_id.filter(|id| !id.trim().is_empty()) {
                    draft.rejected.insert(id, format!("{error:#}"));
                }
                let reviews = if name == "dream_review_group" {
                    vec![input.clone()]
                } else {
                    input["self_reviews"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                };
                let gaps = reviews
                    .into_iter()
                    .filter_map(|v| serde_json::from_value::<SelfReview>(v).ok())
                    .filter_map(|r| {
                        draft.validations.get(&r.group_id).map(|v| {
                            let mut feedback = evidence_feedback(&v.input, &r);
                            feedback["group_id"] = json!(r.group_id);
                            feedback["missing_fragments"] = json!(coverage_gaps(v, &r));
                            feedback
                        })
                    })
                    .collect::<Vec<_>>();
                json!({"accepted":false,"error":format!("{error:#}"),"boundary_feedback":feedback::boundary_error(&error),"operation_feedback":feedback::operation_errors(&input),"review_feedback":gaps,"staged_group_ids":draft.groups.keys().collect::<Vec<_>>(),"committed":false})
            }
        };
        if let Some(execution) = &self.execution {
            if let Err(error) = execution.ensure_no_pending().await {
                *self.fatal_error.lock().await = Some(error);
                result["committed"] = Value::Null;
                result["recovery_required"] = json!(true);
            }
        }
        let mut draft = self.draft.lock().await;
        result["draft_state"] = feedback::state(&draft.groups, &draft.rejected, &readable, &input);
        result["draft_state"]["validated_groups"] = json!(draft
            .validations
            .iter()
            .map(|(id, v)| (id, &v.validation_id))
            .collect::<BTreeMap<_, _>>());
        result["draft_state"]["reviewed_groups"] =
            json!(draft.self_reviews.keys().collect::<Vec<_>>());
        result["draft_state"]["withdrawn"] = json!(draft.withdrawn.keys().collect::<Vec<_>>());
        result["draft_state"]["candidate_progress"] = draft.candidates.context();
        if let Some(audit) = &self.audit {
            if let Err(error) = audit.event(json!({"tool":name,"input":input,"feedback":result,"draft":*draft,"receipts":audit_receipts,"observed":audit_observed})).await {
                draft.finished = None;
                *self.fatal_error.lock().await = Some(error);
                result = json!({"accepted":false,"retry_required":true,"error":"Dream audit update failed; execution records are authoritative and earlier changes may already be committed. Host will recover before continuing."});
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::Confidence;

    #[tokio::test]
    async fn dream_deprecation_feedback_lists_all_bad_fields_and_preserves_accepted_draft() {
        let a = claim("claim_11111111");
        let b = claim("claim_22222222");
        let reviewer = test_reviewer(
            a.holder.clone(),
            vec![a.clone(), b.clone()],
            Draft::default(),
        );
        let original = stage("records", a.id.as_str());
        assert_eq!(
            call(&reviewer, "dream_stage_group", original.clone()).await["accepted"],
            true
        );
        let mut bad = original;
        bad["group"]["operations"][0]["changes"]["status"] = json!("deprecated");
        bad["group"]["operations"][0]["changes"]["statement"] = json!(a.statement);
        bad["group"]["operations"]
            .as_array_mut()
            .unwrap()
            .push(json!({"id":b.id,"action":"deprecate","changes":{"scope":null}}));
        let rejected = call(&reviewer, "dream_stage_group", bad.clone()).await;
        assert_eq!(rejected["accepted"], false);
        assert_eq!(rejected["operation_feedback"].as_array().unwrap().len(), 2);
        assert_eq!(
            rejected["operation_feedback"][0]["unexpected_fields"],
            json!(["statement", "status"])
        );
        assert_eq!(
            rejected["operation_feedback"][1]["unexpected_fields"],
            json!(["scope"])
        );
        let saved = call(&reviewer, "dream_read_draft", json!({})).await;
        assert_eq!(
            saved["groups"][0]["group"]["operations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        for op in bad["group"]["operations"].as_array_mut().unwrap() {
            op["changes"]
                .as_object_mut()
                .unwrap()
                .retain(|key, _| key == "evidence_summary");
        }
        assert_eq!(
            call(&reviewer, "dream_stage_group", bad).await["accepted"],
            true
        );
        assert_eq!(
            call(&reviewer, "dream_validate", json!({})).await["accepted"],
            true
        );
    }

    #[tokio::test]
    async fn dream_reviews_are_cached_per_group_and_invalidated_only_when_changed() {
        let owner = AgentId::new("agent-example").unwrap();
        let a = claim("claim_11111111");
        let b = claim("claim_22222222");
        let reviewer = test_reviewer(owner.clone(), vec![a.clone(), b.clone()], Draft::default());
        for (id, original) in [("a", &a), ("b", &b)] {
            let staged = call(
                &reviewer,
                "dream_stage_group",
                stage(id, original.id.as_str()),
            )
            .await;
            assert_eq!(staged["accepted"], true, "{staged}");
        }
        let validated = reviewer
            .call(
                "dream_validate",
                json!({}),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await;
        for v in validated["validated_groups"].as_array().unwrap() {
            let v: Validation = serde_json::from_value(v.clone()).unwrap();
            let review = super::super::dream_review::fixture_review(&v);
            let result = reviewer
                .call(
                    "dream_review_group",
                    json!(review),
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .await;
            assert_eq!(result["accepted"], true, "{result}");
            assert!(reviewer.finished().await.is_none());
        }
        // 恢复只读草稿后可省略所有复核正文直接 finish。
        let restored = test_reviewer(owner, vec![a.clone(), b], reviewer.snapshot().await);
        let finish = json!({"review":{"quality":"episodic","evidence":"unchanged","consolidation":"none"},"group_ids":["a","b"]});
        let result = restored
            .call(
                "dream_finish",
                finish.clone(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await;
        assert_eq!(result["accepted"], true, "{result}");
        let mut changed = stage("a", a.id.as_str());
        changed["group"]["reason"] = json!("revised rationale requires another review");
        let result = call(&reviewer, "dream_stage_group", changed).await;
        assert_eq!(result["draft_state"]["reviewed_groups"], json!(["b"]));
        let result = reviewer
            .call("dream_finish", finish, BTreeMap::new(), BTreeMap::new())
            .await;
        assert_eq!(result["accepted"], false);
        assert!(reviewer.finished().await.is_none());
    }

    fn test_reviewer(owner: AgentId, claims: Vec<Claim>, draft: Draft) -> Reviewer {
        Reviewer::new(owner, claims, draft, None)
    }
    fn claim(id: &str) -> Claim {
        Claim {
            id: id.parse().unwrap(),
            name: "queue recovery".into(),
            statement: "Persisted queue items survive restart.".into(),
            scope: "example queue".into(),
            holder: AgentId::new("agent-example").unwrap(),
            confidence: Confidence::Medium,
            status: ClaimStatus::Active,
            created_at: crate::time::now_seconds(),
            updated_at: None,
            source_claim_ids: vec![],
            evidence_summary: "Existing restart test; memory-only queues not covered.".into(),
        }
    }
    fn stage(group_id: &str, id: &str) -> Value {
        json!({"group_id":group_id,"group":{"kind":"quality","reason":"episodic record", "evidence_ids":[],"coverage":[],
            "operations":[{"id":id,"action":"deprecate","changes":{"evidence_summary":"Contains delivery history without a reusable judgment."}}]}})
    }
    async fn call(reviewer: &Reviewer, name: &str, mut value: Value) -> Value {
        if name == "dream_stage_group" && value["group"].is_object() {
            let operations = value["group"]["operations"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            value["group"]["change_basis"] = json!(operations.iter().filter(|op| op["action"] != "keep").map(|op| json!({
                "claim_id":op["id"],"removed_or_changed":"test change","added":"", "justification":"test rationale", "evidence_ids":[]
            })).collect::<Vec<_>>());
        }
        if name == "dream_finish" {
            let validation = reviewer
                .call(
                    "dream_validate",
                    json!({}),
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
                .await;
            value["self_reviews"] = json!(validation["validated_groups"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| {
                    super::super::dream_review::fixture_review(
                        &serde_json::from_value(v.clone()).unwrap(),
                    )
                })
                .collect::<Vec<_>>());
        }
        reviewer
            .call(name, value, BTreeMap::new(), BTreeMap::new())
            .await
    }
    fn finish(ids: Vec<&str>) -> Value {
        json!({"review":{"quality":"reviewed", "evidence":"unknown", "consolidation":"preserved"},"group_ids":ids})
    }

    #[tokio::test]
    async fn dream_review_feedback_returns_all_bad_quotes_and_allows_in_place_repair() {
        let original = claim("claim_11111111");
        let reviewer = test_reviewer(original.holder.clone(), vec![original], Draft::default());
        assert_eq!(
            call(&reviewer, "dream_stage_group", stage("a", "claim_11111111")).await["accepted"],
            true
        );
        let result = call(&reviewer, "dream_validate", json!({})).await;
        let validation: Validation =
            serde_json::from_value(result["validated_groups"][0].clone()).unwrap();
        let review = super::super::dream_review::fixture_review(&validation);
        let mut invalid = serde_json::to_value(&review).unwrap();
        invalid["preservation"][0]["evidence"] = json!([
            {"evidence_id":"outside_group_a","quote":"first mismatched source"},
            {"evidence_id":"outside_group_b","quote":"second mismatched source"}
        ]);
        let rejected = call(&reviewer, "dream_review_group", invalid).await;
        assert_eq!(rejected["accepted"], false);
        assert_eq!(
            rejected["review_feedback"][0]["evidence_errors"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            rejected["review_feedback"][0]["evidence_errors"][1]["evidence_index"],
            1
        );
        assert_eq!(rejected["draft_state"]["reviewed_groups"], json!([]));
        let accepted = call(
            &reviewer,
            "dream_review_group",
            serde_json::to_value(review).unwrap(),
        )
        .await;
        assert_eq!(accepted["accepted"], true, "{accepted}");
        assert_eq!(accepted["draft_state"]["reviewed_groups"], json!(["a"]));
    }

    #[tokio::test]
    async fn finish_exposes_only_review_and_group_ids() {
        let original = claim("claim_11111111");
        let reviewer = test_reviewer(original.holder.clone(), vec![original], Draft::default());
        let definition = reviewer
            .definitions()
            .into_iter()
            .find(|t| t.name == "dream_finish")
            .unwrap();
        assert_eq!(
            definition.input_schema["required"],
            json!(["review", "group_ids"])
        );
        assert!(definition.input_schema["properties"]
            .get("unresolved")
            .is_none());
        let mut retired = finish(vec![]);
        retired["unresolved"] = json!([]);
        assert_eq!(
            call(&reviewer, "dream_finish", retired).await["accepted"],
            false
        );
        assert_eq!(
            call(&reviewer, "dream_finish", finish(vec![])).await["accepted"],
            true
        );
        let output = serde_json::to_value(reviewer.finished().await.unwrap()).unwrap();
        assert_eq!(output.as_object().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn stage_rejects_bad_basis_immediately_and_keeps_accepted_group() {
        let original = claim("claim_11111111");
        let reviewer = test_reviewer(original.holder.clone(), vec![original], Draft::default());
        assert_eq!(
            call(&reviewer, "dream_stage_group", stage("a", "claim_11111111")).await["accepted"],
            true
        );
        let saved = serde_json::to_value(reviewer.snapshot().await).unwrap();
        let mut replacement = stage("a", "claim_11111111");
        replacement["group"]["change_basis"] = json!([]);
        let missing = reviewer
            .call(
                "dream_stage_group",
                replacement.clone(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await;
        assert_eq!(missing["accepted"], false);
        assert!(missing["error"].as_str().unwrap().contains("change_basis"));
        replacement["group"]["change_basis"] = json!([{"claim_id":"claim_11111111","removed_or_changed":"Drop log","added":"","justification":"Only delivery history","evidence_ids":["evidence_missing"]}]);
        let unknown = reviewer
            .call(
                "dream_stage_group",
                replacement,
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await;
        assert_eq!(unknown["accepted"], false);
        assert!(unknown["error"]
            .as_str()
            .unwrap()
            .contains("evidence_missing"));
        assert_eq!(
            serde_json::to_value(reviewer.snapshot().await).unwrap()["groups"],
            saved["groups"]
        );
    }

    #[tokio::test]
    async fn conflict_feedback_recovers_existing_edits_and_guides_group_replacement() {
        let claims = vec![
            claim("claim_11111111"),
            claim("claim_22222222"),
            claim("claim_33333333"),
        ];
        let owner = claims[0].holder.clone();
        let reviewer = test_reviewer(owner.clone(), claims.clone(), Draft::default());
        let existing = json!({"group_id":"evidence","group":{"kind":"quality","reason":"retain knowledge","evidence_ids":[],"coverage":[],
            "operations":[{"id":"claim_11111111","action":"update","changes":{"statement":"Durable items survive restart; memory-only items do not."}},
                {"id":"claim_22222222","action":"keep"}]}});
        assert_eq!(
            call(&reviewer, "dream_stage_group", existing.clone()).await["accepted"],
            true
        );
        let merged = json!({"group_id":"topic","group":{"kind":"consolidation","reason":"combine conditions","evidence_ids":[],
            "coverage":[{"input_id":"claim_11111111","output_ids":["claim_11111111"],"note":"durable boundary"},
                {"input_id":"claim_33333333","output_ids":["claim_11111111"],"note":"ordering"}],
            "operations":[{"id":"claim_11111111","action":"update","changes":{"statement":"Durable items survive restart and preserve order; memory-only items do not."}},
                {"id":"claim_33333333","action":"deprecate"}]}});
        let error = call(&reviewer, "dream_stage_group", merged.clone()).await;
        assert_eq!(error["accepted"], false);
        assert_eq!(
            error["draft_state"]["conflicts"][0]["staged_group_id"],
            "evidence"
        );
        assert_eq!(
            error["draft_state"]["conflicts"][0]["staged_operation"],
            existing["group"]["operations"][0]
        );
        assert!(error["draft_state"]["pending_rejections"]
            .get("topic")
            .is_some());
        // 恢复后仍能读到真正接受的改写，不能被失败的整合提案覆盖。
        let restored = serde_json::from_value(json!(reviewer.snapshot().await)).unwrap();
        let reviewer = test_reviewer(owner, claims, restored);
        let read = call(
            &reviewer,
            "dream_read_draft",
            json!({"group_ids":["evidence"]}),
        )
        .await;
        let mut old = read["groups"][0].clone();
        assert_eq!(
            old["group"]["operations"][0],
            existing["group"]["operations"][0]
        );
        old["group"]["operations"].as_array_mut().unwrap().remove(0);
        assert_eq!(
            call(&reviewer, "dream_stage_group", old).await["accepted"],
            true
        );
        assert_eq!(
            call(&reviewer, "dream_stage_group", merged).await["accepted"],
            true
        );
        let none = call(
            &reviewer,
            "dream_stage_group",
            json!({"group_id":"nonexistent","group":null}),
        )
        .await;
        assert_eq!(
            none["removed"],
            json!({"staged_group":false,"rejected_error":false})
        );
        assert!(none["draft_state"]["pending_rejections"]
            .as_object()
            .unwrap()
            .is_empty());
        assert_eq!(
            call(&reviewer, "dream_finish", finish(vec!["evidence", "topic"])).await["accepted"],
            true
        );
        let plan = reviewer.finished().await.unwrap();
        assert_eq!(plan.groups[0].updates[0].id.as_str(), "claim_22222222");
        assert!(plan.groups[1].updates[0].statement.contains("memory-only"));
    }

    #[tokio::test]
    async fn rejected_group_preserves_draft_and_can_be_repaired_independently() {
        let first = claim("claim_11111111");
        let second = claim("claim_22222222");
        let reviewer = test_reviewer(first.holder.clone(), vec![first, second], Draft::default());
        assert_eq!(
            call(
                &reviewer,
                "dream_stage_group",
                stage("first", "claim_11111111")
            )
            .await["accepted"],
            true
        );
        let rejected = call(
            &reviewer,
            "dream_stage_group",
            stage("second", "claim_ffffffff"),
        )
        .await;
        assert_eq!(rejected["accepted"], false);
        assert!(rejected["error"]
            .as_str()
            .unwrap()
            .contains("claim_ffffffff"));
        assert_eq!(rejected["staged_group_ids"], json!(["first"]));
        let blocked = call(&reviewer, "dream_finish", finish(vec!["first"])).await;
        assert_eq!(blocked["accepted"], false);
        assert!(blocked["error"]
            .as_str()
            .unwrap()
            .contains("unhandled validation errors"));
        assert_eq!(
            call(
                &reviewer,
                "dream_stage_group",
                stage("second", "claim_11111111")
            )
            .await["accepted"],
            false
        );
        assert_eq!(
            call(
                &reviewer,
                "dream_stage_group",
                stage("second", "claim_22222222")
            )
            .await["accepted"],
            true
        );
        assert_eq!(
            call(&reviewer, "dream_finish", finish(vec!["first"])).await["accepted"],
            false
        );
        assert_eq!(
            call(&reviewer, "dream_finish", finish(vec!["second", "first"])).await["accepted"],
            true
        );
        assert_eq!(reviewer.finished().await.unwrap().groups.len(), 2);
    }

    #[tokio::test]
    async fn consolidation_expands_keep_patch_and_deprecate_without_losing_original_fields() {
        let first = claim("claim_11111111");
        let second = claim("claim_22222222");
        let third = claim("claim_33333333");
        let reviewer = test_reviewer(
            first.holder.clone(),
            vec![first.clone(), second.clone(), third.clone()],
            Draft::default(),
        );
        let input = json!({"group_id":"queue","group":{"kind":"consolidation","reason":"shared boundary","evidence_ids":[],
            "coverage":[
                {"input_id":first.id,"output_ids":[first.id],"note":"restart boundary"},
                {"input_id":second.id,"output_ids":[first.id],"note":"ordering retained"},
                {"input_id":third.id,"output_ids":[third.id],"note":"independent condition remains"}],
            "operations":[
                {"id":first.id,"action":"update","changes":{"statement":"Persisted queue survives restart and retries FIFO; memory-only items are excluded."}},
                {"id":second.id,"action":"deprecate"},
                {"id":third.id,"action":"keep"}]}});
        assert_eq!(
            call(&reviewer, "dream_stage_group", input).await["accepted"],
            true
        );
        assert_eq!(
            call(&reviewer, "dream_finish", finish(vec!["queue"])).await["accepted"],
            true
        );
        let plan = reviewer.finished().await.unwrap();
        let updates = &plan.groups[0].updates;
        assert_eq!(updates[0].scope, first.scope);
        assert_eq!(updates[0].confidence, first.confidence);
        assert_eq!(updates[1].status, ClaimStatus::Deprecated);
        assert_eq!(updates[1].statement, second.statement);
        assert_eq!(updates[2].statement, third.statement);
        assert_eq!(updates[2].evidence_summary, third.evidence_summary);
    }

    #[tokio::test]
    async fn draft_protocol_preserves_authority_and_evidence_guards() {
        let own = claim("claim_11111111");
        let mut foreign = claim("claim_22222222");
        foreign.holder = AgentId::new("agent-other").unwrap();
        let mut deprecated = claim("claim_33333333");
        deprecated.status = ClaimStatus::Deprecated;
        let reviewer = test_reviewer(
            own.holder.clone(),
            vec![own, foreign, deprecated],
            Draft::default(),
        );
        for id in ["claim_22222222", "claim_33333333"] {
            assert_eq!(
                call(&reviewer, "dream_stage_group", stage("bad", id)).await["accepted"],
                false
            );
        }
        for changes in [
            json!({"holder":"agent-other"}),
            json!({"id":"claim_ffffffff"}),
            json!({"confidence":"high"}),
            json!({"source_claim_ids":["claim_11111111"]}),
            json!({"status":"deprecated"}),
        ] {
            let mut input = stage("bad", "claim_11111111");
            input["group"]["operations"][0]["action"] = json!("update");
            input["group"]["operations"][0]["changes"] = changes;
            assert_eq!(
                call(&reviewer, "dream_stage_group", input).await["accepted"],
                false
            );
        }
        assert_eq!(
            call(
                &reviewer,
                "dream_stage_group",
                stage("good", "claim_11111111")
            )
            .await["accepted"],
            true
        );
        assert_eq!(
            call(&reviewer, "dream_stage_group", json!({"group_id":"good"})).await["accepted"],
            false
        );
        assert!(!reviewer.is_empty().await);
        assert_eq!(
            call(
                &reviewer,
                "dream_stage_group",
                json!({"group_id":"good","group":null})
            )
            .await["accepted"],
            true
        );
        assert!(!reviewer.is_empty().await);
        assert_eq!(
            call(
                &reviewer,
                "dream_stage_group",
                json!({"group_id":"bad","group":null})
            )
            .await["accepted"],
            true
        );
        assert!(reviewer.is_empty().await);
    }
    #[tokio::test]
    async fn dream_review_version_changes_invalidate_only_the_changed_group() {
        let first = claim("claim_11111111");
        let second = claim("claim_22222222");
        let reviewer = test_reviewer(
            first.holder.clone(),
            vec![first.clone(), second.clone()],
            Draft::default(),
        );
        for (group, id) in [("a", first.id.as_str()), ("b", second.id.as_str())] {
            assert_eq!(
                call(&reviewer, "dream_stage_group", stage(group, id)).await["accepted"],
                true
            );
        }
        let validation = reviewer
            .call(
                "dream_validate",
                json!({}),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await;
        let mut reviews = validation["validated_groups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                super::super::dream_review::fixture_review(
                    &serde_json::from_value(v.clone()).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let mut partial = finish(vec!["a", "b"]);
        partial["self_reviews"] = json!([reviews[0]]);
        assert_eq!(
            reviewer
                .call("dream_finish", partial, BTreeMap::new(), BTreeMap::new())
                .await["accepted"],
            false
        );
        let mut changed = stage("b", second.id.as_str());
        changed["group"]["reason"] = json!("More precise reason after rechecking the original");
        assert_eq!(
            call(&reviewer, "dream_stage_group", changed).await["accepted"],
            true
        );
        let mut stale = finish(vec!["a", "b"]);
        stale["self_reviews"] = json!([reviews[1]]);
        let rejected = reviewer
            .call(
                "dream_finish",
                stale.clone(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await;
        assert_eq!(rejected["accepted"], false);
        assert!(rejected["error"]
            .as_str()
            .unwrap()
            .contains("dream_validate"));
        let current = reviewer
            .call(
                "dream_validate",
                json!({"group_ids":["b"]}),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await;
        assert_eq!(
            reviewer
                .call("dream_finish", stale, BTreeMap::new(), BTreeMap::new())
                .await["accepted"],
            false
        );
        reviews[1] = super::super::dream_review::fixture_review(
            &serde_json::from_value(current["validated_groups"][0].clone()).unwrap(),
        );
        let mut finish = finish(vec!["a", "b"]);
        finish["self_reviews"] = json!([reviews[1]]);
        assert_eq!(
            reviewer
                .call("dream_finish", finish, BTreeMap::new(), BTreeMap::new())
                .await["accepted"],
            true
        );
        assert_eq!(
            reviewer.approved().await.len(),
            2,
            "Unchanged group self-review remains accepted"
        );
    }
    #[test]
    fn dream_old_exploration_approvals_are_ignored_instead_of_reused() {
        let draft: Draft = serde_json::from_value(json!({"groups":{},"finished":null,
            "approved":[{"input":{},"verdict":{"decision":"pass"}}],"safety_cache":{"old":{}},"safety_rejections":{}})).unwrap();
        assert!(draft.approved.is_empty() && draft.validations.is_empty());
    }
}
