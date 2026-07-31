import { describe, expect, it } from 'vitest'

import { isOutboxEntryOpen } from './derived'
import type { OutboxEntry } from './types'

function entry(
  id: string,
  status: 'active' | 'deprecated',
  offeredAgents: string[],
  receivedAgents: string[],
  createdAt: string,
): OutboxEntry {
  return {
    inbox_id: id,
    maintainer_action_id: `action-${id}`,
    target_kind: 'broadcast',
    created_at: createdAt,
    offered_to: offeredAgents.map((agentId) => ({
      agent_id: agentId,
      first_offered_at: createdAt,
      last_offered_at: createdAt,
      attempts: 1,
    })),
    delivered_to: receivedAgents.map((agentId) => ({
      agent_id: agentId,
      sent_at: createdAt,
    })),
    inbox_message: {
      id,
      message_type: 'policy_update',
      policy: {
        id: 'policy-a',
        message_type: 'policy_update',
        name: 'policy_a',
        statement: 'statement',
        scope: 'service / prod',
        status,
        created_at: '2026-05-14T09:00:00Z',
        updated_at: status === 'deprecated' ? createdAt : undefined,
      },
    },
  }
}

describe('isOutboxEntryOpen', () => {
  it('keeps deprecation open for agents that were offered the active policy', () => {
    // 发布与退役可落在同一秒；recipient 资格不能依赖时间严格小于。
    const active = entry('inbox-active', 'active', ['agent-a'], [], '2026-05-14T11:00:00Z')
    const deprecated = entry('inbox-deprecated', 'deprecated', [], [], '2026-05-14T11:00:00Z')

    expect(isOutboxEntryOpen(deprecated, [active, deprecated], [deprecated.inbox_message.policy])).toBe(true)

    const acknowledged = entry(
      'inbox-deprecated',
      'deprecated',
      ['agent-a'],
      ['agent-a'],
      '2026-05-14T11:00:00Z',
    )
    expect(isOutboxEntryOpen(acknowledged, [active, acknowledged], [acknowledged.inbox_message.policy])).toBe(false)
  })

  it('closes deprecated broadcast when no agent was ever exposed to active policy', () => {
    const active = entry('inbox-active', 'active', [], [], '2026-05-14T10:00:00Z')
    const deprecated = entry('inbox-deprecated', 'deprecated', [], [], '2026-05-14T11:00:00Z')

    expect(isOutboxEntryOpen(deprecated, [active, deprecated], [deprecated.inbox_message.policy])).toBe(false)
  })

  it('closes an old active snapshot after the current policy is deprecated', () => {
    const active = entry('inbox-active', 'active', ['agent-a'], [], '2026-05-14T10:00:00Z')
    const deprecated = entry('inbox-deprecated', 'deprecated', [], [], '2026-05-14T11:00:00Z')

    expect(isOutboxEntryOpen(active, [active, deprecated], [deprecated.inbox_message.policy])).toBe(false)
  })

  it('treats missing offered_to from a legacy server as empty', () => {
    const legacyActive = entry('inbox-active', 'active', [], [], '2026-05-14T10:00:00Z')
    delete legacyActive.offered_to
    const deprecated = entry('inbox-deprecated', 'deprecated', [], [], '2026-05-14T11:00:00Z')

    expect(isOutboxEntryOpen(deprecated, [legacyActive, deprecated], [deprecated.inbox_message.policy])).toBe(false)
  })
})
