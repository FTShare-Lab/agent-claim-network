// 知识树测试使用的合成来源链，包含共享来源和两种治理消息。
import type { ClaimView } from '../features/claims/types'
import type { Policy } from '../features/overview/types'

export function makeClaim(id: string, sources: string[] = [], name = id): ClaimView {
  return {
    claim: {
      id, name, source_claim_ids: sources,
      statement: `${name} statement`, evidence_summary: `${name} evidence`,
      scope: 'example/runtime', holder: 'example-agent', confidence: 'high', status: 'active',
      created_at: '2026-09-01T08:00:00Z',
    },
    open_dispute_ids: [], resolved_dispute_ids: [],
  }
}

export const knowledgePolicies: Policy[] = [
  {
    id: 'policy_00000001', name: 'Reliable execution', message_type: 'policy_update',
    statement: 'Preserve evidence for later verification.', scope: 'example/governance',
    status: 'active', created_at: '2026-08-01T08:00:00Z',
  },
  {
    id: 'policy_00000002', name: 'Review old evidence', message_type: 'claim_attribute_update',
    statement: 'Recheck evidence before adopting this claim.', scope: 'example/governance',
    status: 'deprecated', created_at: '2026-08-02T08:00:00Z',
  },
]

export const knowledgeClaims = [
  makeClaim('claim_00000001', ['claim_00000002', 'claim_00000003', 'policy_00000002'], 'Release readiness'),
  makeClaim('claim_00000002', ['claim_00000004'], 'Verified recovery'),
  makeClaim('claim_00000003', ['claim_00000004'], 'Verified replay'),
  makeClaim('claim_00000004', ['policy_00000001'], 'Shared evidence'),
]
