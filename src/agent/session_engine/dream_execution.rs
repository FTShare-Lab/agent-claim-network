//! Dream 逐组执行与版本化回执；先记录再写入，重试复用同一执行结果。
use super::claim_context::content_hash;
use super::dream::{Checkpoint, DreamControl, DreamYield};
use super::dream_plan::{
    check_factual_correction_eligible, validate_plan, Kind, OperationGroup, Plan, Review,
};
use super::dream_review::{review_input, Validation};
use super::SessionEngine;
use crate::claim::{Claim, ClaimId, ClaimStatus, Confidence, TraceId};
use crate::storage::{read_yaml, write_yaml_atomic, StorageError};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClaimReview {
    pub claim_id: ClaimId,
    // 旧执行记录仍可恢复/重放；新提交在 review_check 中要求明确核对名称。
    #[serde(default)]
    pub final_name: String,
    #[serde(default)]
    pub name_reason: String,
    pub information_preserved: String,
    pub removal_or_correction: String,
    pub factual_correction: bool,
    pub output_ids: Vec<ClaimId>,
    pub evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SemanticReview {
    pub claims: Vec<ClaimReview>,
    pub scope_and_certainty: String,
}

impl SemanticReview {
    fn check_inputs(&self, ids: BTreeSet<ClaimId>) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.scope_and_certainty.trim().is_empty(),
            "Explain name/body consistency, scope, certainty and evidence boundaries"
        );
        anyhow::ensure!(
            self.claims.len() == ids.len()
                && self
                    .claims
                    .iter()
                    .map(|r| r.claim_id.clone())
                    .collect::<BTreeSet<_>>()
                    == ids,
            "Review must describe every input Claim exactly once"
        );
        for item in &self.claims {
            anyhow::ensure!(!item.information_preserved.trim().is_empty()
                && !item.removal_or_correction.trim().is_empty(),
                "Describe retained rules/conditions/alternatives and removal/correction basis for {}", item.claim_id);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ExecutionMeta {
    pub job_id: String,
    pub group_id: String,
    pub validation_id: String,
    pub review: SemanticReview,
    pub hard_validation_passed: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Origin {
    snapshot_at: DateTime<Utc>,
    claims: Vec<Claim>,
    coverage: Value,
}

pub(super) struct Execution {
    engine: SessionEngine,
    job_id: String,
    jobs: PathBuf,
    root: PathBuf,
    origin: Origin,
    pub(super) control: DreamControl,
}

pub(super) fn pending_path(home: &Path) -> PathBuf {
    home.join("runtime")
        .join("supervisor")
        .join("dream_pending.yaml")
}

impl Execution {
    pub(super) fn with_control(mut self, control: DreamControl) -> Self {
        self.control = control;
        self
    }
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    pub(super) fn original_high_confidence(&self, id: &ClaimId) -> bool {
        self.origin
            .claims
            .iter()
            .any(|claim| &claim.id == id && claim.confidence == Confidence::High)
    }

    pub(super) fn eligibility_context(&self, loaded: &[Claim]) -> Value {
        let original: BTreeMap<_, _> = self.origin.claims.iter().map(|c| (&c.id, c)).collect();
        let eligible: Vec<_> = loaded
            .iter()
            .filter(|claim| {
                claim.holder == self.engine.agent.agent_id
                    && claim.status != ClaimStatus::Deprecated
                    && claim.confidence != Confidence::High
                    && original
                        .get(&claim.id)
                        .is_some_and(|c| c.confidence != Confidence::High)
            })
            .map(|claim| &claim.id)
            .collect();
        json!({
            "ownership":"claims in the initial input are fully read, non-deprecated Claims owned by dream_context.agent_id; holder is their owner. This establishes modification permission, not factual correctness.",
            "loaded_b_eligible_ids":eligible,
            "b_rule":"Only these loaded Claims meet B's original medium/low gate; eligibility is not evidence or an obligation to change. Other loaded Claims may undergo A/C without factual correction. Omitted Claims require read_claim and the same gates."
        })
    }

    pub async fn new(
        engine: SessionEngine,
        job_id: &str,
        jobs: &Path,
        claims: Vec<Claim>,
        coverage: Value,
        snapshot_at: DateTime<Utc>,
    ) -> anyhow::Result<Self> {
        let root = engine
            .runner
            .maintainer_upload_queue
            .agent_home()
            .join("dream")
            .join(job_id);
        let origin_path = root.join("origin.yaml");
        let origin = match read_yaml(&origin_path).await {
            Ok(origin) => origin,
            Err(StorageError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                let origin = Origin {
                    snapshot_at,
                    claims,
                    coverage,
                };
                write_yaml_atomic(&origin_path, &origin).await?;
                origin
            }
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            control: DreamControl::default(),
            engine,
            job_id: job_id.into(),
            jobs: jobs.into(),
            root,
            origin,
        })
    }

    pub async fn records(&self) -> anyhow::Result<Vec<(PathBuf, Checkpoint)>> {
        let mut paths = Vec::new();
        match tokio::fs::read_dir(self.root.join("executions")).await {
            Ok(mut entries) => {
                while let Some(entry) = entries.next_entry().await? {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "yaml") {
                        paths.push(path);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let mut records = Vec::new();
        for path in paths {
            records.push((path.clone(), read_yaml::<Checkpoint>(&path).await?));
        }
        records.sort_by_key(|(_, c)| c.snapshot_at);
        Ok(records)
    }

    pub async fn ensure_no_pending(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !tokio::fs::try_exists(pending_path(
                self.engine.runner.maintainer_upload_queue.agent_home()
            ))
            .await?,
            "Dream execution needs durable checkpoint recovery; some writes may already exist"
        );
        // Prepared 已持久化就必须先恢复，不能在审计投影失败后撤回已承诺的操作。
        anyhow::ensure!(
            self.records()
                .await?
                .iter()
                .all(|(_, record)| record.applied),
            "Dream has a prepared execution awaiting recovery; do not change or withdraw its draft"
        );
        Ok(())
    }

    pub async fn recover(&self) -> anyhow::Result<()> {
        for (path, checkpoint) in self.records().await? {
            let pending: Option<PathBuf> = match read_yaml(&pending_path(
                self.engine.runner.maintainer_upload_queue.agent_home(),
            ))
            .await
            {
                Ok(path) => Some(path),
                Err(StorageError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    None
                }
                Err(error) => return Err(error.into()),
            };
            if !checkpoint.applied || pending.as_ref() == Some(&path) {
                self.engine
                    .apply_dream_checkpoint_with_control(&path, &self.control)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn current(&self) -> anyhow::Result<BTreeMap<ClaimId, Claim>> {
        Ok(self
            .engine
            .owned_dream_claims()
            .await?
            .into_iter()
            .map(|c| (c.id.clone(), c))
            .collect())
    }

    // 自己的已执行变化不使历史恢复指纹失效；外部变化仍保留其真实版本。
    pub async fn normalized_versions(
        &self,
        mut versions: BTreeMap<ClaimId, String>,
    ) -> anyhow::Result<BTreeMap<ClaimId, String>> {
        let mut written = BTreeMap::new();
        for (_, record) in self.records().await? {
            written.extend(record.self_versions);
        }
        for original in &self.origin.claims {
            if versions
                .get(&original.id)
                .is_some_and(|hash| written.get(&original.id) == Some(hash))
            {
                versions.insert(original.id.clone(), content_hash(original)?);
            }
        }
        Ok(versions)
    }

    pub async fn context(&self) -> anyhow::Result<Value> {
        let records = self.records().await?;
        let changed: BTreeSet<_> = records
            .iter()
            .flat_map(|(_, r)| r.self_versions.keys().cloned())
            .collect();
        let current = self.current().await?;
        Ok(
            json!({"executions":records.iter().map(|(_, r)| json!({"operation":r.operation,"completed":r.applied,"actual_changed_ids":r.self_versions.keys().collect::<Vec<_>>(),"skipped":r.groups.iter().any(|g|g.skipped)})).collect::<Vec<_>>(),
            "current_claims":changed.iter().filter_map(|id|current.get(id)).collect::<Vec<_>>(),
            "note":"Execution receipts are authoritative. These changes already exist, even if an old conversation or initial input shows earlier content. Do not replay them or treat Dream output as new independent evidence."}),
        )
    }

    pub fn review_check(
        &self,
        group: &OperationGroup,
        input: &Value,
        review: &SemanticReview,
    ) -> anyhow::Result<()> {
        review.check_inputs(group.updates.iter().map(|u| u.id.clone()).collect())?;
        for item in &review.claims {
            let before: Claim = serde_json::from_value(
                input["before"]
                    .as_array()
                    .and_then(|v| v.iter().find(|c| c["id"] == json!(item.claim_id)))
                    .context("Review input missing")?
                    .clone(),
            )?;
            let after = group
                .updates
                .iter()
                .find(|u| u.id == item.claim_id)
                .context("Review target missing")?;
            anyhow::ensure!(
                item.final_name == after.name && !item.name_reason.trim().is_empty(),
                "Review {} must set final_name to {:?} and explain in name_reason why it matches the final statement. If the name is misleading, repair changes.name, validate and review again; deprecation keeps its historical name.",
                item.claim_id, after.name
            );
            anyhow::ensure!(
                item.output_ids.iter().all(|id| group
                    .updates
                    .iter()
                    .any(|u| &u.id == id && u.status != ClaimStatus::Deprecated)),
                "Information destination must be a surviving Claim in this group"
            );
            if group.kind == Kind::Consolidation {
                let expected: BTreeSet<_> = group
                    .coverage
                    .iter()
                    .filter(|c| c.input_id == item.claim_id)
                    .flat_map(|c| c.output_ids.iter().cloned())
                    .collect();
                anyhow::ensure!(
                    item.output_ids.iter().cloned().collect::<BTreeSet<_>>() == expected,
                    "Review must match consolidation destinations for {}",
                    item.claim_id
                );
            } else if after.status != ClaimStatus::Deprecated {
                anyhow::ensure!(
                    item.output_ids.contains(&item.claim_id),
                    "Retained Claim needs its own information destination"
                );
            }
            for id in &item.evidence_ids {
                anyhow::ensure!(
                    input["evidence"].get(id).is_some(),
                    "Review evidence must be a versioned file_read receipt in this group: {id}"
                );
            }
            let changed = input["changes"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|c| c["id"] == json!(item.claim_id) && c["action"] != "keep");
            if item.factual_correction || (changed && group.kind == Kind::Evidence) {
                check_factual_correction_eligible(
                    &item.claim_id,
                    before.confidence == Confidence::High
                        || self.original_high_confidence(&item.claim_id),
                )?;
                let basis = group
                    .change_basis
                    .iter()
                    .find(|b| b.claim_id == item.claim_id)
                    .context("Factual correction requires claim-specific change_basis")?;
                anyhow::ensure!(!item.evidence_ids.is_empty() && item.evidence_ids.iter().all(|id|basis.evidence_ids.contains(id)) && after.evidence_summary != before.evidence_summary, "Factual correction requires new evidence_summary and this Claim's direct evidence");
            }
        }
        Ok(())
    }

    pub(super) async fn review_sync(&self, input: Value) -> anyhow::Result<Value> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            source_id: ClaimId,
            carrier_ids: Vec<ClaimId>,
            #[serde(default)]
            review: Option<SemanticReview>,
            #[serde(default)]
            validation_id: Option<String>,
        }
        let request: Request = serde_json::from_value(input)?;
        let home = self.engine.runner.maintainer_upload_queue.agent_home();
        let _guard = self
            .control
            .snapshot_guard(&crate::storage::paths::agent_home_knowledge_apply_lock_path(home))
            .await?;
        crate::agent::claim_alignment::ensure_knowledge_ready(home).await?;
        let current = self.current().await?;
        let groups = self.engine.runner.pending_consolidations().await?;
        let original = groups
            .iter()
            .find(|g| g.retired.id == request.source_id)
            .context("No pending consolidation for this source")?;
        anyhow::ensure!(
            original.retired.holder == self.engine.agent.agent_id
                && current.get(&request.source_id) == Some(&original.retired),
            "Source changed; cannot review old retirement"
        );
        let mut carriers = Vec::new();
        for id in &request.carrier_ids {
            let claim = current.get(id).context("Carrier not found")?;
            anyhow::ensure!(
                claim.holder == self.engine.agent.agent_id
                    && claim.status != ClaimStatus::Deprecated
                    && !carriers.contains(claim),
                "Carriers must be distinct surviving owned Claims"
            );
            carriers.push(claim.clone());
        }
        anyhow::ensure!(!carriers.is_empty(), "At least one carrier is required");
        let validation = Validation::new(json!({
            "before":original.before,"carriers":carriers,
            "previous_dependency":original,"current_source":original.retired
        }))?;
        let Some(review) = request.review else {
            return Ok(
                json!({"accepted":true,"committed":false,"validation":validation,
                "next":"Review the source's original rules, conditions, exceptions and provenance against these carriers. Resubmit source_id/carrier_ids, validation_id and the same semantic review format as dream_apply_group, with one claims entry for the source and all carrier IDs as output_ids. No correction or new evidence is allowed here. If uncertain leave delivery pending."}),
            );
        };
        anyhow::ensure!(
            request.validation_id.as_deref() == Some(&validation.validation_id),
            "Sync input changed; reread and review the current versions"
        );
        review.check_inputs(BTreeSet::from([request.source_id]))?;
        for item in &review.claims {
            anyhow::ensure!(item.final_name == original.retired.name
                && !item.name_reason.trim().is_empty()
                && !item.factual_correction && item.evidence_ids.is_empty()
                && item.output_ids.iter().cloned().collect::<BTreeSet<_>>()
                    == carriers.iter().map(|c| c.id.clone()).collect::<BTreeSet<_>>(),
                "Sync review only confirms complete preservation in the listed carriers; keep the source's historical name and do not correct or remove knowledge");
        }
        super::dream_plan::validate_consolidation_sources(
            &original.before,
            &carriers,
            &request.carrier_ids,
        )?;
        let _prepare = self.control.prepare_guard().await?;
        let path = self
            .root
            .join("sync_reviews")
            .join(format!("{}.yaml", validation.validation_id));
        let mut record = json!({"validation":validation,"review":review,"applied":false});
        write_yaml_atomic(&path, &record).await?;
        let revised = crate::agent::consolidation_delivery::ConsolidationDelivery {
            carriers,
            ..original.clone()
        };
        self.engine
            .runner
            .rebind_consolidation(original, revised)
            .await?;
        record["applied"] = json!(true);
        write_yaml_atomic(&path, &record).await?;
        Ok(
            json!({"accepted":true,"committed":true,"claim_changes":[],"next":"Sync dependencies updated and recorded; delivery still waits for exact carrier acknowledgements."}),
        )
    }

    pub async fn replay(
        &self,
        validation_id: &str,
        group_id: &str,
    ) -> anyhow::Result<Option<Value>> {
        for (path, record) in self.records().await? {
            if record
                .operation
                .as_ref()
                .is_some_and(|op| op.validation_id == validation_id)
            {
                anyhow::ensure!(
                    record
                        .operation
                        .as_ref()
                        .is_some_and(|op| op.group_id == group_id),
                    "Execution receipt belongs to another group"
                );
                if !record.applied {
                    self.engine
                        .apply_dream_checkpoint_with_control(&path, &self.control)
                        .await?;
                }
                let record: Checkpoint = read_yaml(&path).await?;
                return Ok(Some(self.receipt(&record, true).await?));
            }
        }
        Ok(None)
    }

    async fn receipt(&self, record: &Checkpoint, replayed: bool) -> anyhow::Result<Value> {
        let current = self.current().await?;
        let accepted = !record.groups.iter().any(|g| g.skipped);
        let before: Vec<_> = record
            .groups
            .iter()
            .flat_map(|g| g.before.iter().cloned())
            .collect();
        let actual: Vec<_> = current.values().cloned().collect();
        let changes = crate::agent::claim_alignment::changes(Some(&before), &[], &[], &actual);
        Ok(
            json!({"accepted":accepted,"committed":!record.self_versions.is_empty(),"replayed":replayed,"operation":record.operation,
            "actual_changed_ids":record.self_versions.keys().collect::<Vec<_>>(),"current_claims":record.groups.iter().flat_map(|g|g.before.iter()).filter_map(|c|current.get(&c.id)).collect::<Vec<_>>(),
            "analysis_claims":before,"changes":changes,"sync":"Existing durable sync staging used; local success is not remote acknowledgement.","next":if accepted {"This group completed. Continue another candidate or finish with a factual ABC summary."} else {"This candidate is still pending. Preserve the original intent and actual_changed_ids already committed. Compare analysis_claims with current_claims; repair only remaining work and validate/review again, or explicitly keep it. Do not move on or report full success."}}),
        )
    }

    pub async fn apply(
        &self,
        group: OperationGroup,
        validation: Validation,
        review: SemanticReview,
        readable: &BTreeMap<ClaimId, Claim>,
        receipts: &BTreeMap<String, Value>,
    ) -> anyhow::Result<Value> {
        let group_id = validation.input["group_id"]
            .as_str()
            .context("Missing group id")?
            .to_owned();
        self.review_check(&group, &validation.input, &review)?;
        if validation.input != review_input(&group_id, &group, readable, receipts) {
            let before: Vec<Claim> = serde_json::from_value(validation.input["before"].clone())?;
            let current: Vec<_> = readable.values().cloned().collect();
            return Ok(
                json!({"accepted":false,"committed":false,"actual_changed_ids":[],"analysis_claims":before,"current_claims":current,
                "changes":crate::agent::claim_alignment::changes(Some(&before), &[], &[], &current),
                "error":"Draft or input changed; original candidate remains pending. Compare the current state, repair or explicitly keep, and validate again."}),
            );
        }
        if crate::supervisor::dream_has_priority_work(&self.jobs).await? {
            return Err(DreamYield.into());
        }
        let plan = Plan {
            review: Review {
                quality: "Reviewed per operation".into(),
                evidence: "Claim-specific evidence checked".into(),
                consolidation: "Information destinations reviewed".into(),
            },
            groups: vec![group],
        };
        let groups = validate_plan(
            &plan,
            readable,
            &self.engine.agent.agent_id,
            receipts,
            crate::time::now_seconds(),
        )?;
        // validation_id 来自宿主内容哈希，不接受模型提供路径或未经匹配的任意编号。
        let path = self
            .root
            .join("executions")
            .join(format!("{}.yaml", validation.validation_id));
        let checkpoint = Checkpoint {
            snapshot_at: Utc::now(),
            input_versions: BTreeMap::new(),
            coverage: self.origin.coverage.clone(),
            plan,
            receipts: receipts
                .iter()
                .filter(|(id, _)| validation.input["evidence"].get(*id).is_some())
                .map(|(id, r)| (id.clone(), r.clone()))
                .collect(),
            exploration_receipts: None,
            groups,
            safety_reviews: vec![],
            self_reviews: vec![],
            trace_id: TraceId::random(),
            applied: false,
            success_at: None,
            self_versions: BTreeMap::new(),
            operation: Some(ExecutionMeta {
                job_id: self.job_id.clone(),
                group_id,
                validation_id: validation.validation_id,
                review,
                hard_validation_passed: true,
            }),
        };
        let guard = self.control.prepare_guard().await?;
        write_yaml_atomic(&path, &checkpoint).await?;
        drop(guard);
        self.engine
            .apply_dream_checkpoint_with_control(&path, &self.control)
            .await?;
        self.receipt(&read_yaml(&path).await?, false).await
    }

    pub async fn final_checkpoint(&self, review: Review) -> anyhow::Result<Checkpoint> {
        let mut checkpoint = Checkpoint {
            snapshot_at: self.origin.snapshot_at,
            input_versions: self
                .origin
                .claims
                .iter()
                .map(|c| Ok((c.id.clone(), content_hash(c)?)))
                .collect::<anyhow::Result<_>>()?,
            coverage: self.origin.coverage.clone(),
            plan: Plan {
                review,
                groups: vec![],
            },
            receipts: BTreeMap::new(),
            exploration_receipts: None,
            groups: vec![],
            safety_reviews: vec![],
            self_reviews: vec![],
            trace_id: TraceId::random(),
            applied: false,
            success_at: None,
            self_versions: BTreeMap::new(),
            operation: None,
        };
        for (_, record) in self.records().await? {
            anyhow::ensure!(
                record.applied,
                "An operation is still being recovered; cannot finish Dream"
            );
            checkpoint.plan.groups.extend(record.plan.groups);
            checkpoint.groups.extend(record.groups);
            checkpoint.self_versions.extend(record.self_versions);
        }
        Ok(checkpoint)
    }
}

pub(super) fn review_schema() -> Value {
    let text = json!({"type":"string"});
    let strings = json!({"type":"array","items":text});
    json!({"type":"object","properties":{"claims":{"type":"array","items":{"type":"object","properties":{"claim_id":text,"final_name":text,"name_reason":{"type":"string","description":"Explain why final_name accurately describes the final statement; check unchanged names too. Deprecated records retain their historical name."},"information_preserved":{"type":"string","description":"Identify the original useful conditions, exceptions, alternatives and evidence limits, and where output_ids preserve them; do not silently drop details during correction."},"removal_or_correction":text,"factual_correction":{"type":"boolean"},"output_ids":strings,"evidence_ids":strings},"required":["claim_id","final_name","name_reason","information_preserved","removal_or_correction","factual_correction","output_ids","evidence_ids"],"additionalProperties":false}},"scope_and_certainty":text},"required":["claims","scope_and_certainty"],"additionalProperties":false})
}
