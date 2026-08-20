import { describe, expect, it } from 'vitest'

import type {
  AnalysisListResponse,
  ArbitrationAnalysisDetail,
  DisputeDetail,
} from '../features/disputes/types'
import {
  demoAgents,
  demoAudits,
  demoClaims,
  demoDisputes,
  demoOverview,
  demoPolicies,
  demoPolicyRecords,
  demoSweeps,
  demoTeamAuthKeys,
  requestStaticDemoData,
} from './demoData'

const ID_PATTERNS = {
  claim: /^claim_[0-9a-f]{8}$/,
  dispute: /^dispute_[0-9a-f]{8}$/,
  arbitration: /^arbitration_[0-9a-f]{8}$/,
  analysis: /^analysis_[0-9a-f]{16}$/,
  policy: /^policy_[0-9a-f]{8}$/,
  inbox: /^inbox_[0-9a-f]{8}$/,
  action: /^intent_[0-9a-f]{8}$/,
  key: /^key_[0-9a-f]{8}$/,
  history: /^(policy_event|agent_activity|http_audit|dispute_resolution|sweep_run)_\d+_[0-9a-f]{8}$/,
} as const

function expectNewestFirst(values: string[]) {
  expect(values).toEqual([...values].sort((left, right) => right.localeCompare(left)))
}

function expectOldestFirst(values: string[]) {
  expect(values).toEqual([...values].sort((left, right) => left.localeCompare(right)))
}

describe('public demo data contract', () => {
  it('uses production ID formats and keeps typed relationships valid', () => {
    const claimIds = new Set(demoClaims.map((item) => item.claim.id))
    const disputeIds = new Set(demoDisputes.map((item) => item.id))
    const policyIds = new Set(demoPolicies.map((item) => item.id))

    expect(Array.from(claimIds).every((id) => ID_PATTERNS.claim.test(id))).toBe(true)
    expect(Array.from(disputeIds).every((id) => ID_PATTERNS.dispute.test(id))).toBe(true)
    expect(Array.from(policyIds).every((id) => ID_PATTERNS.policy.test(id))).toBe(true)
    expect(demoTeamAuthKeys.every((item) => ID_PATTERNS.key.test(item.key_id))).toBe(true)
    expect(demoAgents.every((item) => /^[a-z0-9_-]+$/.test(item.agent_id))).toBe(true)

    for (const dispute of demoDisputes) {
      expect(dispute.claims.every((claimId) => claimIds.has(claimId))).toBe(true)
      if (dispute.resolution) {
        expect(dispute.resolution.resolution_id).toMatch(ID_PATTERNS.arbitration)
      }
    }
    for (const view of demoClaims) {
      expect(
        [...view.open_dispute_ids, ...view.resolved_dispute_ids].every((id) => disputeIds.has(id)),
      ).toBe(true)
      expect(
        view.claim.source_claim_ids.every(
          (id) => ID_PATTERNS.claim.test(id) || ID_PATTERNS.policy.test(id),
        ),
      ).toBe(true)
    }
    for (const entry of demoPolicyRecords.outbox) {
      expect(entry.inbox_id).toMatch(ID_PATTERNS.inbox)
      expect(entry.maintainer_action_id).toMatch(ID_PATTERNS.action)
      expect(entry.inbox_message.id).toBe(entry.inbox_id)
      expect(policyIds.has(entry.inbox_message.policy.id)).toBe(true)
    }
    for (const row of demoPolicyRecords.send_log) {
      expect(row.inbox_id).toMatch(ID_PATTERNS.inbox)
      expect(row.maintainer_action_id).toMatch(ID_PATTERNS.action)
      expect(policyIds.has(row.policy_id)).toBe(true)
    }
    for (const event of demoPolicyRecords.events) {
      expect(policyIds.has(event.policy_id)).toBe(true)
    }
    for (const action of demoOverview.snapshot.actions) {
      expect(action.maintainer_action_id).toMatch(ID_PATTERNS.action)
      expect(action.inbox_ids.every((id) => ID_PATTERNS.inbox.test(id))).toBe(true)
      expect(policyIds.has(action.policy_id)).toBe(true)
    }
    for (const sweep of demoSweeps) {
      for (const [, claimId] of [
        ...sweep.report.stale_claims,
        ...sweep.report.deprecated_claims,
      ]) {
        expect(claimIds.has(claimId)).toBe(true)
      }
      for (const notification of sweep.report.notifications) {
        expect(policyIds.has(notification.policy_id)).toBe(true)
        expect(
          [...notification.stale_claims, ...notification.deprecated_claims].every((id) =>
            claimIds.has(id),
          ),
        ).toBe(true)
      }
    }
    for (const resolution of demoOverview.recent_dispute_resolutions) {
      expect(disputeIds.has(resolution.dispute_id)).toBe(true)
    }
  })

  it('matches Rust omission and tagged-union serialization semantics', () => {
    const activePolicies = demoPolicies.filter((policy) => policy.status === 'active')
    const broadcastPolicies = demoPolicies.filter((policy) => !policy.target_agents)

    expect(activePolicies.every((policy) => !Object.hasOwn(policy, 'updated_at'))).toBe(true)
    expect(broadcastPolicies.every((policy) => !Object.hasOwn(policy, 'target_agents'))).toBe(true)

    for (const entry of demoPolicyRecords.outbox) {
      if (entry.target_kind === 'broadcast') {
        expect(Object.hasOwn(entry, 'target_agent')).toBe(false)
      } else {
        expect(entry.target_agent).toMatch(/^[a-z0-9_-]+$/)
      }
      expect(Object.hasOwn(entry.inbox_message, 'handled_at')).toBe(false)
    }

    expect(Object.hasOwn(demoOverview, 'latest_sweep')).toBe(true)
    expect(Object.hasOwn(demoOverview, 'sweep_schedule')).toBe(true)
    expect(Object.hasOwn(demoOverview.sweep_schedule, 'last_auto_sweep_at')).toBe(true)
    expect(Object.hasOwn(demoOverview.sweep_schedule, 'next_sweep_at')).toBe(true)
    expect(Object.hasOwn(demoOverview.sweep_schedule, 'last_auto_trigger')).toBe(true)
    expect(demoAudits.every((audit) => Object.hasOwn(audit, 'source_ip'))).toBe(true)
    expect(demoAudits.every((audit) => Object.hasOwn(audit, 'resource_id'))).toBe(true)
  })

  it('uses only enum values and numeric shapes emitted by current services', async () => {
    const policyEventKinds = new Set([
      'policy_update_published',
      'claim_attribute_update_published',
      'policy_deprecated',
    ])
    const activityKinds = new Set(['inbox_pulled', 'claim_uploaded', 'dispute_reported'])
    const sweepTriggers = new Set(['manual', 'maintainer_startup', 'ticker'])

    expect(
      demoPolicyRecords.events.every((event) => policyEventKinds.has(event.event_kind)),
    ).toBe(true)
    expect(
      demoOverview.recent_agent_activities.every((event) =>
        activityKinds.has(event.activity_kind),
      ),
    ).toBe(true)
    expect(demoSweeps.every((sweep) => sweepTriggers.has(sweep.trigger))).toBe(true)

    const result = await requestStaticDemoData<{
      candidate_claims: Array<{ id: string }>
      disputes: Array<{ claim_ids: string[] }>
      retrieval_debug: {
        mode: string
        candidates: Array<{
          claim_id: string
          hit_sources: string
          lexical_score: number
          vector_score: number
          vector_status: string
        }>
      }
    }>('/api/router-query', {
      method: 'POST',
      body: JSON.stringify({
        scope: 'coordination/router',
        semantic_query: 'router candidates and local judgment',
      }),
    })

    const candidateIds = new Set(result.candidate_claims.map((item) => item.id))
    expect(result.candidate_claims.every((item) => ID_PATTERNS.claim.test(item.id))).toBe(true)
    expect(['hybrid', 'vector_only', 'lexical_only']).toContain(result.retrieval_debug.mode)
    for (const candidate of result.retrieval_debug.candidates) {
      expect(candidateIds.has(candidate.claim_id)).toBe(true)
      expect(['both', 'lexical', 'vector', 'none']).toContain(candidate.hit_sources)
      expect(Number.isInteger(candidate.lexical_score)).toBe(true)
      expect(Number.isInteger(candidate.vector_score)).toBe(true)
      expect(candidate.lexical_score).toBeGreaterThanOrEqual(0)
      expect(candidate.vector_score).toBeGreaterThanOrEqual(0)
      expect(candidate.vector_score).toBeLessThanOrEqual(1000)
      expect(['pending', 'ready', 'failed', 'not_requested']).toContain(candidate.vector_status)
    }
    for (const dispute of result.disputes) {
      expect(dispute.claim_ids.filter((id) => candidateIds.has(id)).length).toBeGreaterThanOrEqual(2)
    }
  })

  it('preserves endpoint ordering guarantees and history record shapes', () => {
    expectNewestFirst(demoClaims.map((view) => view.claim.created_at))
    expectNewestFirst(demoDisputes.map((dispute) => dispute.created_at))
    expectNewestFirst(demoPolicyRecords.policies.map((policy) => policy.created_at))
    expectNewestFirst(demoPolicyRecords.outbox.map((entry) => entry.created_at))
    expectOldestFirst(demoPolicyRecords.send_log.map((entry) => entry.sent_at))
    expectNewestFirst(demoPolicyRecords.events.map((entry) => entry.occurred_at))
    expectNewestFirst(demoOverview.snapshot.send_log.map((entry) => entry.sent_at))
    expectNewestFirst(demoSweeps.map((entry) => entry.triggered_at))
    expectNewestFirst(demoAudits.map((entry) => entry.occurred_at))
    expectNewestFirst(
      demoOverview.recent_agent_activities.map((entry) => entry.occurred_at),
    )
    expectNewestFirst(demoTeamAuthKeys.map((entry) => entry.generated_time))
    expect(demoAgents.map((entry) => entry.agent_id)).toEqual(
      [...demoAgents.map((entry) => entry.agent_id)].sort(),
    )

    const historyIds = [
      ...demoPolicyRecords.events.map((event) => event.event_id),
      ...demoOverview.recent_agent_activities.map((event) => event.event_id),
      ...demoAudits.map((audit) => audit.audit_id),
      ...demoOverview.recent_dispute_resolutions.map((event) => event.event_id),
      ...demoSweeps.map((sweep) => sweep.run_id),
    ]
    expect(historyIds.every((id) => ID_PATTERNS.history.test(id))).toBe(true)
  })

  it('uses realistic audited HTTP shapes without exposing credentials', () => {
    const publicPayload = JSON.stringify({
      agents: demoAgents,
      audits: demoAudits,
      claims: demoClaims,
      disputes: demoDisputes,
      policies: demoPolicies,
      teamAuthKeys: demoTeamAuthKeys,
    })

    expect(publicPayload).not.toMatch(/authorization|bearer|password|private[_-]?key|secret/i)
    expect(
      demoAgents.every((agent) =>
        /^(192\.0\.2\.|198\.51\.100\.|203\.0\.113\.)/.test(agent.last_source_ip ?? ''),
      ),
    ).toBe(true)
    for (const audit of demoAudits) {
      expect(audit.summary).toBe(`${audit.method} ${audit.path} -> ${audit.status_code}`)
      if (audit.path === '/claims/upload' || audit.path === '/inbox/pull') {
        const request = JSON.parse(audit.request_body) as {
          auth: { acn_key: string }
        }
        expect(request.auth.acn_key).toBe('<redacted>')
      }
    }
    expect(demoAudits.some((audit) => audit.path === '/claims/upload')).toBe(true)
  })

  it('serves consistent dispute detail, analysis, and resolution routes', async () => {
    const resolved = demoDisputes.find((dispute) => dispute.status === 'resolved')
    const open = demoDisputes.find((dispute) => dispute.status === 'open')
    expect(resolved?.resolution).toBeDefined()
    expect(open).toBeDefined()

    const detail = await requestStaticDemoData<DisputeDetail>(
      `/api/disputes/${resolved!.id}`,
    )
    const analyses = await requestStaticDemoData<AnalysisListResponse>(
      `/api/disputes/${resolved!.id}/analyses`,
    )
    const analysisId = analyses.current_analysis!.analysis_id
    expect(analysisId).toMatch(ID_PATTERNS.analysis)
    const analysis = await requestStaticDemoData<ArbitrationAnalysisDetail>(
      `/api/disputes/${resolved!.id}/analyses/${analysisId}`,
    )

    expect(detail.resolution).toEqual(resolved!.resolution)
    expect(detail.current_analysis).toEqual(analyses.current_analysis)
    expect(detail.holder_adoption?.summary.updated_claims).toBe(1)
    expect(analysis.analysis_id).toBe(analysisId)
    expect(analysis.resolution_id).toBe(resolved!.resolution!.resolution_id)

    await expect(
      requestStaticDemoData(`/api/disputes/${open!.id}/analyses`),
    ).resolves.toEqual({})
  })

  it('keeps policy statements textual and rejects public-demo writes', async () => {
    expect(demoPolicies.every((item) => !/^\s*[{[]/.test(item.statement))).toBe(true)

    await expect(
      requestStaticDemoData('/policies/policy-update', {
        method: 'POST',
        body: '{}',
      }),
    ).rejects.toThrow(/read-only/)
  })
})
