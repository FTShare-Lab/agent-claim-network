//! 根据 outbox ACK 与 holder mirror 派生 Resolution 收敛观测。

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

use crate::claim::{AgentId, Claim, ClaimAssessment, SourceId};
use crate::maintainer::history::{fresh_record_id, HistoryStore, ResolutionObservationEventRecord};
use crate::maintainer::outbox_io;

use super::context::load_team_claims;
use super::store::ArbitrationStore;
use super::types::{
    ArbitrationResolutionRecord, ClaimObservation, HolderObservation, ObservationState,
    ResolutionObservation,
};

#[derive(Clone)]
pub struct ObservationService {
    store: ArbitrationStore,
    history: HistoryStore,
}

impl ObservationService {
    pub fn new(store: ArbitrationStore, history: HistoryStore) -> Self {
        Self { store, history }
    }

    pub async fn refresh(
        &self,
        record: &ArbitrationResolutionRecord,
        observed_at: DateTime<Utc>,
    ) -> anyhow::Result<ResolutionObservation> {
        // 同一 Dispute 的按需刷新与事件刷新必须串行。锁从输入扫描前开始持有，
        // 避免较早扫描到的 mirror/outbox 快照在较新的观测之后才落盘。
        let _dispute_guard = self.store.lock_dispute(&record.dispute_id).await?;
        let previous = self
            .store
            .read_observation(&record.dispute_id, &record.resolution_id)
            .await?;
        let current = self.store.read_dispute(&record.dispute_id).await?;
        if current
            .resolution
            .as_ref()
            .map(|resolution| &resolution.resolution_id)
            != Some(&record.resolution_id)
        {
            // 详情读取可能在取得锁前被 Reject & Replace 抢先。旧 Resolution 的
            // observation 必须冻结；没有历史 cache 时返回空投影但不落盘。
            return Ok(previous.unwrap_or_else(|| ResolutionObservation {
                resolution_id: record.resolution_id.clone(),
                dispute_id: record.dispute_id.clone(),
                observed_at,
                holders: Vec::new(),
            }));
        }
        let mirrors = load_team_claims(self.store.team_root()).await?;
        let outbox = outbox_io::list(self.store.team_root()).await?;
        let mut assessments = BTreeMap::new();
        for assessment in &record.resolution.claim_assessments {
            assessments.insert(assessment.claim_id.clone(), assessment);
        }
        let mut holders = Vec::new();
        if let Some(intent) = record.delivery_intent.as_ref() {
            let adoption_candidates = self
                .store
                .list_claim_adoption_candidates(
                    &record.dispute_id,
                    &record.resolution_id,
                    &intent.policy.id,
                )
                .await?;
            for target in &intent.targets {
                let previous_holder = previous.as_ref().and_then(|observation| {
                    observation
                        .holders
                        .iter()
                        .find(|holder| holder.agent_id == target.target_agent)
                });
                let delivered_at = outbox
                    .iter()
                    .find(|entry| entry.inbox_id == target.inbox_id)
                    .and_then(|entry| {
                        entry
                            .delivered_to
                            .iter()
                            .find(|mark| mark.agent_id == target.target_agent)
                            .map(|mark| mark.sent_at)
                    });
                holders.push(observe_holder(
                    &target.target_agent,
                    delivered_at,
                    observed_at,
                    HolderObservationContext {
                        policy_id: &intent.policy.id,
                        assessments: &assessments,
                        snapshots: &record.direct_claim_snapshots,
                        mirrors: &mirrors,
                        adoption_candidates: &adoption_candidates,
                    },
                    previous_holder,
                ));
            }
        }
        holders.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
        if let Some(previous) = previous.as_ref() {
            if previous.observed_at > observed_at {
                return Ok(previous.clone());
            }
            for holder in &mut holders {
                if let Some(existing) = previous.holders.iter().find(|existing| {
                    existing.agent_id == holder.agent_id
                        && existing.state == holder.state
                        && existing.reasons == holder.reasons
                        && existing.delivery_observed == holder.delivery_observed
                        && existing.delivered_at == holder.delivered_at
                        && existing.claims == holder.claims
                }) {
                    holder.last_observed_at = existing.last_observed_at;
                }
            }
            let mut previous_holders = previous.holders.clone();
            previous_holders.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
            if previous_holders == holders {
                return Ok(previous.clone());
            }
        }
        let observation = ResolutionObservation {
            resolution_id: record.resolution_id.clone(),
            dispute_id: record.dispute_id.clone(),
            observed_at,
            holders,
        };
        self.store
            .write_observation(&record.dispute_id, &observation)
            .await?;
        for holder in &observation.holders {
            let previous_state = previous
                .as_ref()
                .and_then(|observation| {
                    observation
                        .holders
                        .iter()
                        .find(|existing| existing.agent_id == holder.agent_id)
                })
                .map(|existing| existing.state);
            if previous_state == Some(holder.state) {
                continue;
            }
            let event = ResolutionObservationEventRecord {
                event_id: fresh_record_id("resolution_observation"),
                resolution_id: record.resolution_id.clone(),
                dispute_id: record.dispute_id.clone(),
                agent_id: holder.agent_id.clone(),
                occurred_at: observed_at,
                previous_state,
                current_state: holder.state,
                reasons: holder.reasons.clone(),
            };
            if let Err(error) = self
                .history
                .write_resolution_observation_event(&event)
                .await
            {
                log::warn!(
                    target: "maintainer_arbitration",
                    "写 resolution observation audit 失败: resolution={} agent={} error={error:#}",
                    record.resolution_id,
                    holder.agent_id
                );
            }
        }
        Ok(observation)
    }
}

#[derive(Clone, Copy)]
struct HolderObservationContext<'a> {
    policy_id: &'a crate::claim::PolicyId,
    assessments: &'a BTreeMap<crate::claim::ClaimId, &'a ClaimAssessment>,
    snapshots: &'a [Claim],
    mirrors: &'a [(AgentId, Claim)],
    adoption_candidates: &'a [Claim],
}

fn observe_holder(
    holder: &AgentId,
    delivered_at: Option<DateTime<Utc>>,
    observed_at: DateTime<Utc>,
    context: HolderObservationContext<'_>,
    previous: Option<&HolderObservation>,
) -> HolderObservation {
    let claims = claim_observations(
        holder,
        context.policy_id,
        context.assessments,
        context.snapshots,
        context.mirrors,
        context.adoption_candidates,
        previous,
    );
    let Some(delivered_at) = delivered_at else {
        return HolderObservation {
            agent_id: holder.clone(),
            state: ObservationState::NotDelivered,
            reasons: vec!["The inbox has not been acknowledged.".into()],
            delivery_observed: false,
            delivered_at: None,
            last_observed_at: Some(observed_at),
            claims,
        };
    };
    if claims.is_empty() {
        return HolderObservation {
            agent_id: holder.clone(),
            state: ObservationState::Unknown,
            reasons: vec!["No holder Claim is available for observation yet.".into()],
            delivery_observed: true,
            delivered_at: Some(delivered_at),
            last_observed_at: Some(observed_at),
            claims,
        };
    }
    let update_observed = claims.iter().any(|claim| claim.update_observed);
    let unavailable = claims.iter().any(|claim| claim.current_status.is_none());
    let mut reasons = claims
        .iter()
        .flat_map(|claim| claim.notes.iter().cloned())
        .collect::<Vec<_>>();
    reasons.sort();
    reasons.dedup();
    let state = if update_observed {
        ObservationState::UpdateObserved
    } else if unavailable {
        ObservationState::Unknown
    } else {
        ObservationState::NoUpdateObserved
    };
    HolderObservation {
        agent_id: holder.clone(),
        state,
        reasons,
        delivery_observed: true,
        delivered_at: Some(delivered_at),
        last_observed_at: Some(observed_at),
        claims,
    }
}

fn claim_observations(
    holder: &AgentId,
    policy_id: &crate::claim::PolicyId,
    assessments: &BTreeMap<crate::claim::ClaimId, &ClaimAssessment>,
    snapshots: &[Claim],
    mirrors: &[(AgentId, Claim)],
    adoption_candidates: &[Claim],
    previous: Option<&HolderObservation>,
) -> Vec<ClaimObservation> {
    let holder_snapshots = snapshots
        .iter()
        .filter(|claim| claim.holder == *holder)
        .collect::<Vec<_>>();
    let direct_ids = holder_snapshots
        .iter()
        .map(|claim| claim.id.clone())
        .collect::<BTreeSet<_>>();
    let previous_by_id = previous
        .map(|holder| {
            holder
                .claims
                .iter()
                .map(|claim| (claim.claim_id.clone(), claim))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut observations = Vec::with_capacity(holder_snapshots.len());
    for snapshot in holder_snapshots {
        let candidate = adoption_candidates
            .iter()
            .find(|claim| claim.holder == *holder && claim.id == snapshot.id);
        let assessment = assessments.get(&snapshot.id).copied();
        let matches: Vec<&Claim> = mirrors
            .iter()
            .filter(|(path_holder, claim)| {
                path_holder == holder && claim.holder == *holder && claim.id == snapshot.id
            })
            .map(|(_, claim)| claim)
            .collect();
        let mut observation = ClaimObservation {
            claim_id: snapshot.id.clone(),
            claim_name: snapshot.name.clone(),
            is_additional_claim: false,
            adoption_snapshot: previous_by_id
                .get(&snapshot.id)
                .and_then(|claim| claim.adoption_snapshot.clone())
                .or_else(|| candidate.cloned()),
            recommended_status: assessment.map(|assessment| assessment.recommended_status),
            current_status: None,
            recommended_scope: assessment
                .and_then(|assessment| assessment.recommended_scope.clone()),
            current_scope: None,
            recommended_statement: assessment
                .and_then(|assessment| assessment.recommended_statement.clone()),
            current_statement: None,
            policy_provenance_present: candidate.is_some(),
            update_observed: false,
            changed_fields: Vec::new(),
            notes: Vec::new(),
        };
        if matches.len() != 1 {
            observation.notes.push(format!(
                "The holder mirror for Claim {} is missing or duplicated.",
                snapshot.id
            ));
            observation.update_observed = observation.adoption_snapshot.is_some();
            if let Some(adoption) = observation.adoption_snapshot.as_ref() {
                observation.changed_fields = visible_changed_fields(snapshot, adoption);
            }
            observations.push(observation);
            continue;
        }
        let claim = matches[0];
        observation.current_status = Some(claim.status);
        observation.current_scope = Some(claim.scope.clone());
        observation.current_statement = Some(claim.statement.clone());
        observation.policy_provenance_present = claim
            .source_claim_ids
            .contains(&SourceId::Policy(policy_id.clone()))
            || candidate.is_some();
        if observation.adoption_snapshot.is_none() && observation.policy_provenance_present {
            observation.adoption_snapshot = Some(claim.clone());
        }
        if let Some(adoption) = observation.adoption_snapshot.as_ref() {
            observation.changed_fields = visible_changed_fields(snapshot, adoption);
            observation.update_observed = true;
        } else if !visible_changed_fields(snapshot, claim).is_empty() {
            observation.notes.push(
                "The current mirror differs, but no matching CAU Policy provenance was observed."
                    .into(),
            );
        }
        observations.push(observation);
    }

    let mut additional_claim_ids = previous_by_id
        .values()
        .filter(|claim| claim.is_additional_claim)
        .map(|claim| claim.claim_id.clone())
        .collect::<BTreeSet<_>>();
    additional_claim_ids.extend(
        adoption_candidates
            .iter()
            .filter(|claim| claim.holder == *holder && !direct_ids.contains(&claim.id))
            .map(|claim| claim.id.clone()),
    );
    for (path_holder, claim) in mirrors {
        if path_holder == holder
            && claim.holder == *holder
            && !direct_ids.contains(&claim.id)
            && claim
                .source_claim_ids
                .contains(&SourceId::Policy(policy_id.clone()))
        {
            additional_claim_ids.insert(claim.id.clone());
        }
    }
    for claim_id in additional_claim_ids {
        let matches = mirrors
            .iter()
            .filter(|(path_holder, claim)| {
                path_holder == holder && claim.holder == *holder && claim.id == claim_id
            })
            .map(|(_, claim)| claim)
            .collect::<Vec<_>>();
        let previous_claim = previous_by_id.get(&claim_id).copied();
        let candidate = adoption_candidates
            .iter()
            .find(|claim| claim.holder == *holder && claim.id == claim_id);
        let mut adoption_snapshot = previous_claim
            .and_then(|claim| claim.adoption_snapshot.clone())
            .or_else(|| candidate.cloned());
        let mut observation = ClaimObservation {
            claim_id: claim_id.clone(),
            claim_name: adoption_snapshot
                .as_ref()
                .map(|claim| claim.name.clone())
                .or_else(|| matches.first().map(|claim| claim.name.clone()))
                .unwrap_or_else(|| "Claim".into()),
            is_additional_claim: true,
            adoption_snapshot: None,
            recommended_status: None,
            current_status: None,
            recommended_scope: None,
            current_scope: None,
            recommended_statement: None,
            current_statement: None,
            policy_provenance_present: candidate.is_some(),
            update_observed: false,
            changed_fields: Vec::new(),
            notes: Vec::new(),
        };
        if matches.len() != 1 {
            observation.notes.push(format!(
                "The holder mirror for additional Claim {claim_id} is missing or duplicated."
            ));
        } else {
            let claim = matches[0];
            observation.current_status = Some(claim.status);
            observation.current_scope = Some(claim.scope.clone());
            observation.current_statement = Some(claim.statement.clone());
            observation.policy_provenance_present = claim
                .source_claim_ids
                .contains(&SourceId::Policy(policy_id.clone()))
                || candidate.is_some();
            if adoption_snapshot.is_none() && observation.policy_provenance_present {
                adoption_snapshot = Some(claim.clone());
            }
        }
        observation.update_observed = adoption_snapshot.is_some();
        observation.adoption_snapshot = adoption_snapshot;
        observations.push(observation);
    }
    observations.sort_by(|left, right| left.claim_id.cmp(&right.claim_id));
    observations
}

fn visible_changed_fields(before: &Claim, after: &Claim) -> Vec<String> {
    let mut fields = Vec::new();
    if after.status != before.status {
        fields.push("status".into());
    }
    if after.scope != before.scope {
        fields.push("scope".into());
    }
    if after.statement != before.statement {
        fields.push("statement".into());
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{
        ArbitrationResolutionContext, ArbitrationResolutionId, ClaimId, ClaimStatus, Confidence,
        DeliveredMark, Dispute, DisputeId, DisputeResolution, DisputeStatus, InboxId, InboxMessage,
        InboxMessageKind, MaintainerActionId, OutboxEntry, OutboxTarget, Policy, PolicyId,
        PolicyMessageType, PolicyStatus, ResolutionBasis, ResolutionType, ResolvedBy,
    };
    use crate::maintainer::arbitration::types::{DeliveryTargetIntent, MaintainerDisputeRecord};
    use crate::maintainer::arbitration::{ArbitrationResolutionRecord, DeliveryIntent};
    use crate::maintainer::history::HistoryStore;
    use crate::storage::{paths, write_yaml_atomic};

    fn assessment(claim_id: ClaimId, status: ClaimStatus) -> ClaimAssessment {
        ClaimAssessment {
            claim_id,
            recommended_status: status,
            assessment: "assessment".into(),
            recommended_scope: None,
            recommended_statement: None,
            reason: "reason".into(),
        }
    }

    fn assessment_lookup(assessments: &[ClaimAssessment]) -> BTreeMap<ClaimId, &ClaimAssessment> {
        assessments
            .iter()
            .map(|assessment| (assessment.claim_id.clone(), assessment))
            .collect()
    }

    fn observation_context<'a>(
        policy_id: &'a PolicyId,
        assessments: &'a BTreeMap<ClaimId, &'a ClaimAssessment>,
        snapshots: &'a [Claim],
        mirrors: &'a [(AgentId, Claim)],
    ) -> HolderObservationContext<'a> {
        HolderObservationContext {
            policy_id,
            assessments,
            snapshots,
            mirrors,
            adoption_candidates: &[],
        }
    }

    fn observation_context_with_candidates<'a>(
        policy_id: &'a PolicyId,
        assessments: &'a BTreeMap<ClaimId, &'a ClaimAssessment>,
        snapshots: &'a [Claim],
        mirrors: &'a [(AgentId, Claim)],
        adoption_candidates: &'a [Claim],
    ) -> HolderObservationContext<'a> {
        HolderObservationContext {
            policy_id,
            assessments,
            snapshots,
            mirrors,
            adoption_candidates,
        }
    }

    fn mirror(holder: &AgentId, claim_id: ClaimId, policy_id: &PolicyId) -> Claim {
        Claim {
            id: claim_id,
            name: "knowledge".into(),
            statement: "statement".into(),
            scope: "scope".into(),
            holder: holder.clone(),
            confidence: Confidence::High,
            status: ClaimStatus::Stale,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            updated_at: Some("2026-08-03T00:00:00Z".parse().unwrap()),
            source_claim_ids: vec![SourceId::Policy(policy_id.clone())],
            evidence_summary: "evidence".into(),
        }
    }

    #[test]
    fn holder_without_direct_snapshots_observes_additional_policy_claim() {
        let holder = AgentId::new("agent-a").unwrap();
        let policy_id = PolicyId::random();
        let present_id = ClaimId::random();
        let missing_id = ClaimId::random();
        let present = mirror(&holder, present_id.clone(), &policy_id);
        let assessments = [
            assessment(present_id, ClaimStatus::Active),
            assessment(missing_id, ClaimStatus::Deprecated),
        ];

        let observed = observe_holder(
            &holder,
            Some("2026-08-02T00:00:00Z".parse().unwrap()),
            "2026-08-03T00:00:00Z".parse().unwrap(),
            observation_context(
                &policy_id,
                &assessment_lookup(&assessments),
                &[],
                &[(holder.clone(), present)],
            ),
            None,
        );

        assert_eq!(observed.state, ObservationState::UpdateObserved);
        assert_eq!(observed.claims.len(), 1);
        assert!(observed.claims[0].is_additional_claim);
    }

    #[test]
    fn missing_receipt_is_not_delivered() {
        let holder = AgentId::new("agent-a").unwrap();
        let observed = observe_holder(
            &holder,
            None,
            "2026-08-03T00:00:00Z".parse().unwrap(),
            observation_context(&PolicyId::random(), &BTreeMap::new(), &[], &[]),
            None,
        );
        assert_eq!(observed.state, ObservationState::NotDelivered);
    }

    #[test]
    fn delivered_holder_without_assessments_still_compares_direct_snapshot() {
        let holder = AgentId::new("agent-a").unwrap();
        let policy_id = PolicyId::random();
        let claim_id = ClaimId::random();
        let mut snapshot = mirror(&holder, claim_id, &policy_id);
        snapshot.source_claim_ids.clear();
        let delivered_at = "2026-08-02T00:00:00Z".parse().unwrap();
        let observed_at = "2026-08-03T00:00:00Z".parse().unwrap();

        let unchanged = observe_holder(
            &holder,
            Some(delivered_at),
            observed_at,
            observation_context(
                &policy_id,
                &BTreeMap::new(),
                std::slice::from_ref(&snapshot),
                &[(holder.clone(), snapshot.clone())],
            ),
            None,
        );
        assert_eq!(unchanged.state, ObservationState::NoUpdateObserved);
        assert_eq!(unchanged.claims.len(), 1);
        assert!(!unchanged.claims[0].update_observed);

        let mut changed_mirror = snapshot.clone();
        changed_mirror.status = ClaimStatus::Deprecated;
        changed_mirror
            .source_claim_ids
            .push(SourceId::Policy(policy_id.clone()));
        let changed = observe_holder(
            &holder,
            Some(delivered_at),
            observed_at,
            observation_context(
                &policy_id,
                &BTreeMap::new(),
                &[snapshot],
                &[(holder.clone(), changed_mirror)],
            ),
            None,
        );
        assert_eq!(changed.state, ObservationState::UpdateObserved);
        assert_eq!(changed.claims.len(), 1);
        assert_eq!(changed.claims[0].changed_fields, ["status"]);
    }

    #[test]
    fn visible_change_without_policy_provenance_is_not_attributed() {
        let holder = AgentId::new("agent-a").unwrap();
        let policy_id = PolicyId::random();
        let claim_id = ClaimId::random();
        let assessment = assessment(claim_id.clone(), ClaimStatus::Stale);
        let delivered_at = "2026-08-02T00:00:00Z".parse().unwrap();
        let observed_at = "2026-08-03T00:00:00Z".parse().unwrap();

        let mut snapshot = mirror(&holder, claim_id.clone(), &policy_id);
        snapshot.source_claim_ids.clear();
        let unchanged = observe_holder(
            &holder,
            Some(delivered_at),
            observed_at,
            observation_context(
                &policy_id,
                &assessment_lookup(std::slice::from_ref(&assessment)),
                std::slice::from_ref(&snapshot),
                &[(holder.clone(), snapshot.clone())],
            ),
            None,
        );
        assert_eq!(unchanged.state, ObservationState::NoUpdateObserved);
        assert!(!unchanged.claims[0].update_observed);
        assert!(unchanged.claims[0].changed_fields.is_empty());
        assert!(!unchanged.claims[0].policy_provenance_present);

        let mut changed_mirror = snapshot.clone();
        changed_mirror.status = ClaimStatus::Deprecated;
        changed_mirror.statement = "new current knowledge".into();
        let changed = observe_holder(
            &holder,
            Some(delivered_at),
            observed_at,
            observation_context(
                &policy_id,
                &assessment_lookup(std::slice::from_ref(&assessment)),
                &[snapshot],
                &[(holder.clone(), changed_mirror)],
            ),
            None,
        );
        assert_eq!(changed.state, ObservationState::NoUpdateObserved);
        assert!(!changed.claims[0].update_observed);
        assert!(changed.claims[0].changed_fields.is_empty());
        assert!(!changed.claims[0].policy_provenance_present);
        assert_eq!(changed.claims[0].notes.len(), 1);
    }

    #[test]
    fn policy_provenance_attributes_additional_claim_and_freezes_first_adoption() {
        let holder = AgentId::new("agent-a").unwrap();
        let policy_id = PolicyId::random();
        let direct_id = ClaimId::random();
        let mut direct = mirror(&holder, direct_id, &policy_id);
        direct.source_claim_ids.clear();
        direct.status = ClaimStatus::Active;
        let mut new_claim = mirror(&holder, ClaimId::random(), &policy_id);
        new_claim.name = "new-knowledge".into();
        new_claim.statement = "first attributed result".into();
        let delivered_at = "2026-08-02T00:00:00Z".parse().unwrap();
        let first = observe_holder(
            &holder,
            Some(delivered_at),
            "2026-08-03T00:00:00Z".parse().unwrap(),
            observation_context(
                &policy_id,
                &BTreeMap::new(),
                std::slice::from_ref(&direct),
                &[
                    (holder.clone(), direct.clone()),
                    (holder.clone(), new_claim.clone()),
                ],
            ),
            None,
        );
        assert_eq!(first.state, ObservationState::UpdateObserved);
        let first_new = first
            .claims
            .iter()
            .find(|claim| claim.is_additional_claim)
            .unwrap();
        assert!(first_new.update_observed);
        assert_eq!(
            first_new
                .adoption_snapshot
                .as_ref()
                .map(|claim| claim.statement.as_str()),
            Some("first attributed result")
        );

        new_claim.statement = "later unrelated edit".into();
        let refreshed = observe_holder(
            &holder,
            Some(delivered_at),
            "2026-08-04T00:00:00Z".parse().unwrap(),
            observation_context(
                &policy_id,
                &BTreeMap::new(),
                &[direct.clone()],
                &[(holder.clone(), direct), (holder.clone(), new_claim)],
            ),
            Some(&first),
        );
        let refreshed_new = refreshed
            .claims
            .iter()
            .find(|claim| claim.is_additional_claim)
            .unwrap();
        assert_eq!(
            refreshed_new
                .adoption_snapshot
                .as_ref()
                .map(|claim| claim.statement.as_str()),
            Some("first attributed result")
        );
        assert_eq!(
            refreshed_new.current_statement.as_deref(),
            Some("later unrelated edit")
        );
    }

    #[test]
    fn persisted_candidate_survives_mirror_overwrite_before_first_refresh() {
        let holder = AgentId::new("agent-a").unwrap();
        let policy_id = PolicyId::random();
        let mut direct = mirror(&holder, ClaimId::random(), &policy_id);
        direct.status = ClaimStatus::Active;
        direct.source_claim_ids.clear();

        let mut direct_candidate = direct.clone();
        direct_candidate.status = ClaimStatus::Deprecated;
        direct_candidate.statement = "first attributed direct result".into();
        direct_candidate
            .source_claim_ids
            .push(SourceId::Policy(policy_id.clone()));
        let mut additional_candidate = mirror(&holder, ClaimId::random(), &policy_id);
        additional_candidate.statement = "first attributed additional result".into();

        let mut overwritten_direct = direct_candidate.clone();
        overwritten_direct.statement = "later direct edit".into();
        overwritten_direct.source_claim_ids.clear();
        let mut overwritten_additional = additional_candidate.clone();
        overwritten_additional.statement = "later additional edit".into();
        overwritten_additional.source_claim_ids.clear();
        let candidates = [direct_candidate.clone(), additional_candidate.clone()];
        let mirrors = [
            (holder.clone(), overwritten_direct),
            (holder.clone(), overwritten_additional),
        ];

        let observed = observe_holder(
            &holder,
            Some("2026-08-02T00:00:00Z".parse().unwrap()),
            "2026-08-03T00:00:00Z".parse().unwrap(),
            observation_context_with_candidates(
                &policy_id,
                &BTreeMap::new(),
                std::slice::from_ref(&direct),
                &mirrors,
                &candidates,
            ),
            None,
        );

        assert_eq!(observed.state, ObservationState::UpdateObserved);
        let direct_observation = observed
            .claims
            .iter()
            .find(|claim| claim.claim_id == direct.id)
            .unwrap();
        assert_eq!(
            direct_observation
                .adoption_snapshot
                .as_ref()
                .map(|claim| claim.statement.as_str()),
            Some("first attributed direct result")
        );
        assert_eq!(
            direct_observation.current_statement.as_deref(),
            Some("later direct edit")
        );
        let additional_observation = observed
            .claims
            .iter()
            .find(|claim| claim.claim_id == additional_candidate.id)
            .unwrap();
        assert!(additional_observation.is_additional_claim);
        assert_eq!(
            additional_observation
                .adoption_snapshot
                .as_ref()
                .map(|claim| claim.statement.as_str()),
            Some("first attributed additional result")
        );
        assert_eq!(
            additional_observation.current_statement.as_deref(),
            Some("later additional edit")
        );
    }

    #[test]
    fn legacy_holder_observation_yaml_gets_safe_defaults() {
        let yaml = r#"
resolution_id: arbitration_1234abcd
dispute_id: dispute_1234abcd
observed_at: 2026-08-03T00:00:00Z
holders:
  - agent_id: agent-a
    state: delivered_unobserved
    reasons:
      - legacy cache
"#;
        let observation: ResolutionObservation = serde_yaml_ng::from_str(yaml).unwrap();
        let holder = &observation.holders[0];
        assert_eq!(holder.state, ObservationState::NoUpdateObserved);
        assert!(!holder.delivery_observed);
        assert!(holder.claims.is_empty());
        assert!(holder.delivered_at.is_none());

        let legacy_claim_yaml = r#"
claim_id: claim_1234abcd
claim_name: legacy
update_observed: true
"#;
        let claim: ClaimObservation = serde_yaml_ng::from_str(legacy_claim_yaml).unwrap();
        assert!(!claim.is_additional_claim);
        assert!(claim.adoption_snapshot.is_none());
    }

    #[tokio::test]
    async fn unchanged_refresh_preserves_observation_time_and_audits_only_state_changes() {
        let root = tempfile::tempdir().unwrap();
        let holder = AgentId::new("agent-a").unwrap();
        let policy_id = PolicyId::random();
        let claim_id = ClaimId::random();
        let mut current = mirror(&holder, claim_id.clone(), &policy_id);
        current.status = ClaimStatus::Active;
        current.source_claim_ids.clear();
        let mirror_path = paths::team_store_agent_claims_dir(root.path(), &holder)
            .join(format!("{}.yaml", current.id));
        write_yaml_atomic(&mirror_path, &current).await.unwrap();
        let dispute = Dispute {
            id: DisputeId::random(),
            name: "observation".into(),
            reporter_agent_id: holder.clone(),
            claims: vec![claim_id.clone()],
            summary: "summary".into(),
            status: DisputeStatus::Resolved,
            created_at: "2026-08-01T00:00:00Z".parse().unwrap(),
            resolved_at: Some("2026-08-02T00:00:00Z".parse().unwrap()),
        };
        let resolution_id = ArbitrationResolutionId::random();
        let resolution = DisputeResolution {
            resolution_id: resolution_id.clone(),
            resolved_by: ResolvedBy::Automatic,
            resolved_at: "2026-08-02T00:00:00Z".parse().unwrap(),
            resolution_type: Some(ResolutionType::ConflictResolved),
            resolution_basis: Some(ResolutionBasis::Evidence),
            conclusion: "conclusion".into(),
            claim_assessments: vec![assessment(claim_id, ClaimStatus::Active)],
            rejection_reason: None,
        };
        let policy = Policy {
            id: policy_id.clone(),
            message_type: PolicyMessageType::ClaimAttributeUpdate,
            name: "dispute_arbitration".into(),
            statement: "statement".into(),
            scope: "scope".into(),
            status: PolicyStatus::Active,
            created_at: resolution.resolved_at,
            updated_at: None,
            target_agents: Some(vec![holder.clone()]),
        };
        let inbox_id = InboxId::random();
        let message = InboxMessage {
            id: inbox_id.clone(),
            kind: InboxMessageKind::ClaimAttributeUpdate {
                policy: policy.clone(),
                arbitration_resolution: Some(Box::new(ArbitrationResolutionContext {
                    dispute_id: dispute.id.clone(),
                    resolution: resolution.clone(),
                    context_snapshot_hash: None,
                    dispute_snapshot: dispute.clone(),
                    direct_claim_snapshots: vec![current.clone()],
                    snapshot_source_resolution_id: None,
                })),
            },
            handled_at: None,
        };
        let action_id = MaintainerActionId::random();
        let intent = DeliveryIntent {
            policy,
            maintainer_action_id: action_id.clone(),
            targets: vec![DeliveryTargetIntent {
                inbox_id: inbox_id.clone(),
                target_agent: holder.clone(),
                inbox_message: message.clone(),
            }],
        };
        outbox_io::write(
            root.path(),
            &OutboxEntry {
                inbox_id,
                maintainer_action_id: action_id,
                target: OutboxTarget::Targeted {
                    target_agent: holder,
                },
                created_at: resolution.resolved_at,
                offered_to: Vec::new(),
                delivered_to: vec![DeliveredMark {
                    agent_id: current.holder.clone(),
                    sent_at: "2026-08-02T12:00:00Z".parse().unwrap(),
                }],
                inbox_message: message,
            },
        )
        .await
        .unwrap();
        let record = ArbitrationResolutionRecord {
            schema_version: 1,
            resolution_id,
            dispute_id: dispute.id.clone(),
            created_at: resolution.resolved_at,
            resolution,
            dispute_snapshot: dispute,
            direct_claim_snapshots: vec![current.clone()],
            semantic_fingerprint: None,
            context_snapshot_hash: None,
            analysis_source_id: None,
            legacy_source_attempt_id: None,
            delivery_intent: Some(intent),
            snapshot_source_resolution_id: None,
        };
        let history = HistoryStore::with_defaults(root.path().to_path_buf());
        let store = ArbitrationStore::new(root.path().to_path_buf());
        store
            .write_dispute(&MaintainerDisputeRecord {
                dispute: record.dispute_snapshot.clone(),
                resolution: Some(record.resolution.clone()),
            })
            .await
            .unwrap();
        let service = ObservationService::new(store.clone(), history.clone());

        let first = service
            .refresh(&record, "2026-08-03T00:00:00Z".parse().unwrap())
            .await
            .unwrap();
        let unchanged = service
            .refresh(&record, "2026-08-03T01:00:00Z".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(unchanged, first);
        assert_eq!(
            store
                .read_observation(&record.dispute_id, &record.resolution_id)
                .await
                .unwrap(),
            Some(first.clone())
        );
        assert_eq!(first.holders[0].state, ObservationState::NoUpdateObserved);
        assert_eq!(
            history
                .list_resolution_observation_events()
                .await
                .unwrap()
                .len(),
            1
        );

        current.status = ClaimStatus::Deprecated;
        current
            .source_claim_ids
            .push(SourceId::Policy(policy_id.clone()));
        current.updated_at = Some("2026-08-03T02:00:00Z".parse().unwrap());
        write_yaml_atomic(&mirror_path, &current).await.unwrap();
        let changed = service
            .refresh(&record, "2026-08-03T03:00:00Z".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(changed.holders[0].state, ObservationState::UpdateObserved);
        assert_eq!(
            history
                .list_resolution_observation_events()
                .await
                .unwrap()
                .len(),
            2
        );

        // 较早启动、较晚完成的刷新不能覆盖已经落盘的较新观测。
        current.status = ClaimStatus::Active;
        current.updated_at = Some("2026-08-03T02:30:00Z".parse().unwrap());
        write_yaml_atomic(&mirror_path, &current).await.unwrap();
        let ignored_older = service
            .refresh(&record, "2026-08-03T02:45:00Z".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            ignored_older.observed_at,
            "2026-08-03T03:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            ignored_older.holders[0].state,
            ObservationState::UpdateObserved
        );
        assert_eq!(
            history
                .list_resolution_observation_events()
                .await
                .unwrap()
                .len(),
            2
        );

        current.status = ClaimStatus::Stale;
        current.statement = "later unrelated edit".into();
        current.updated_at = Some("2026-08-03T03:30:00Z".parse().unwrap());
        write_yaml_atomic(&mirror_path, &current).await.unwrap();
        let later = service
            .refresh(&record, "2026-08-03T03:45:00Z".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            later.holders[0].claims[0]
                .adoption_snapshot
                .as_ref()
                .map(|claim| claim.status),
            Some(ClaimStatus::Deprecated)
        );
        assert_eq!(
            later.holders[0].claims[0].current_status,
            Some(ClaimStatus::Stale)
        );

        // 已被替换的 Resolution cache 必须冻结；详情读取即使持有旧 record，
        // 也不能在替换后继续写旧 observation。
        let mut current_dispute = store.read_dispute(&record.dispute_id).await.unwrap();
        let mut replacement = record.resolution.clone();
        replacement.resolution_id = ArbitrationResolutionId::random();
        replacement.resolved_at = "2026-08-03T04:00:00Z".parse().unwrap();
        current_dispute.resolution = Some(replacement);
        store.write_dispute(&current_dispute).await.unwrap();
        let frozen = service
            .refresh(&record, "2026-08-03T05:00:00Z".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            frozen.observed_at,
            "2026-08-03T03:45:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(frozen.holders[0].state, ObservationState::UpdateObserved);
        assert_eq!(
            history
                .list_resolution_observation_events()
                .await
                .unwrap()
                .len(),
            2
        );
    }
}
