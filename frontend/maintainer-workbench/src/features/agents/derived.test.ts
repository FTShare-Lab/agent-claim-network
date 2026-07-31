import { describe, expect, it } from 'vitest'

import type { Policy } from '../overview/types'
import type { OutboxEntry } from '../policies/types'
import { deriveAgentDeliverySummary } from './derived'

const activeSnapshot: Policy = {
  id: 'policy-a',
  message_type: 'policy_update',
  name: 'policy_a',
  statement: 'statement',
  scope: 'service / prod',
  status: 'active',
  created_at: '2026-05-14T10:00:00Z',
}

const oldActiveOutbox: OutboxEntry = {
  inbox_id: 'inbox-active',
  maintainer_action_id: 'action-active',
  target_kind: 'broadcast',
  created_at: '2026-05-14T10:00:00Z',
  offered_to: [{
    agent_id: 'agent-a',
    first_offered_at: '2026-05-14T10:01:00Z',
    last_offered_at: '2026-05-14T10:01:00Z',
    attempts: 1,
  }],
  delivered_to: [],
  inbox_message: {
    id: 'inbox-active',
    message_type: 'policy_update',
    policy: activeSnapshot,
  },
}

describe('deriveAgentDeliverySummary', () => {
  it('does not count an old active broadcast after current policy deprecation', () => {
    const deprecatedPolicy: Policy = {
      ...activeSnapshot,
      status: 'deprecated',
      updated_at: '2026-05-14T11:00:00Z',
    }

    const summary = deriveAgentDeliverySummary({
      agentId: 'agent-a',
      outbox: [oldActiveOutbox],
      policies: [deprecatedPolicy],
      sendLog: [],
      audits: [],
    })

    expect(summary.openCount).toBe(0)
  })

  it('accepts legacy outbox rows without offered_to', () => {
    const legacyOutbox: OutboxEntry = { ...oldActiveOutbox, offered_to: undefined }

    const summary = deriveAgentDeliverySummary({
      agentId: 'agent-a',
      outbox: [legacyOutbox],
      policies: [activeSnapshot],
      sendLog: [],
      audits: [],
    })

    expect(summary.openCount).toBe(1)
  })
})
