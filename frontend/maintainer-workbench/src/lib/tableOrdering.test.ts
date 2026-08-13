import { describe, expect, it } from 'vitest'

import {
  orderActionsByCreatedAt,
  orderAgentsByRecentActivity,
  orderClaimsByStatusAndRecentChange,
  orderDisputesByStatusAndRecentChange,
  orderOutboxByCreatedAt,
  orderPoliciesByTypeAndRecentChange,
  orderSendLogBySentAt,
} from './tableOrdering'

describe('workbench table ordering', () => {
  it('orders claims by status, effective update, creation time, then claim ID', () => {
    const rows = [
      {
        claim: {
          id: 'claim_66666666',
          status: 'deprecated' as const,
          created_at: '2026-05-18T10:00:00Z',
          updated_at: '2026-05-19T10:00:00Z',
        },
      },
      {
        claim: {
          id: 'claim_55555555',
          status: 'stale' as const,
          created_at: '2026-05-17T10:00:00Z',
        },
      },
      {
        claim: {
          id: 'claim_44444444',
          status: 'active' as const,
          created_at: '2026-05-15T10:00:00Z',
        },
      },
      {
        claim: {
          id: 'claim_33333333',
          status: 'active' as const,
          created_at: '2026-05-14T10:00:00Z',
          updated_at: '2026-05-16T10:00:00Z',
        },
      },
      {
        claim: {
          id: 'claim_22222222',
          status: 'active' as const,
          created_at: '2026-05-15T10:00:00Z',
        },
      },
      {
        claim: {
          id: 'claim_11111111',
          status: 'active' as const,
          created_at: '2026-05-15T10:00:00Z',
        },
      },
    ]

    expect(orderClaimsByStatusAndRecentChange(rows).map((row) => row.claim.id)).toEqual([
      'claim_33333333',
      'claim_11111111',
      'claim_22222222',
      'claim_44444444',
      'claim_55555555',
      'claim_66666666',
    ])
    expect(rows.map((row) => row.claim.id)).toEqual([
      'claim_66666666',
      'claim_55555555',
      'claim_44444444',
      'claim_33333333',
      'claim_22222222',
      'claim_11111111',
    ])
  })

  it('orders active agents first by latest activity and leaves inactive agents last', () => {
    const rows = [
      { agent_id: 'agent-c', last_activity: null },
      {
        agent_id: 'agent-b',
        last_activity: { occurred_at: '2026-05-15T11:00:00Z' },
      },
      {
        agent_id: 'agent-a',
        last_activity: { occurred_at: '2026-05-15T11:00:00Z' },
      },
      { agent_id: 'agent-d', last_activity: null },
    ]

    expect(orderAgentsByRecentActivity(rows).map((row) => row.agent_id)).toEqual([
      'agent-a',
      'agent-b',
      'agent-c',
      'agent-d',
    ])
  })

  it('orders policy history by message type and most recent change', () => {
    const rows = [
      {
        id: 'policy_44444444',
        message_type: 'claim_attribute_update' as const,
        created_at: '2026-05-16T10:00:00Z',
      },
      {
        id: 'policy_33333333',
        message_type: 'policy_update' as const,
        created_at: '2026-05-14T10:00:00Z',
        updated_at: '2026-05-17T10:00:00Z',
      },
      {
        id: 'policy_22222222',
        message_type: 'policy_update' as const,
        created_at: '2026-05-15T10:00:00Z',
      },
      {
        id: 'policy_11111111',
        message_type: 'claim_attribute_update' as const,
        created_at: '2026-05-15T10:00:00Z',
      },
    ]

    expect(orderPoliciesByTypeAndRecentChange(rows).map((row) => row.id)).toEqual([
      'policy_33333333',
      'policy_22222222',
      'policy_44444444',
      'policy_11111111',
    ])
  })

  it('orders open disputes before resolved disputes and each group by recent change', () => {
    const rows = [
      {
        id: 'dispute_44444444',
        status: 'resolved' as const,
        created_at: '2026-05-15T10:00:00Z',
        resolved_at: '2026-05-18T10:00:00Z',
      },
      {
        id: 'dispute_33333333',
        status: 'open' as const,
        created_at: '2026-05-16T10:00:00Z',
      },
      {
        id: 'dispute_22222222',
        status: 'resolved' as const,
        created_at: '2026-05-14T10:00:00Z',
        resolved_at: '2026-05-17T10:00:00Z',
      },
      {
        id: 'dispute_11111111',
        status: 'open' as const,
        created_at: '2026-05-17T10:00:00Z',
      },
    ]

    expect(orderDisputesByStatusAndRecentChange(rows).map((row) => row.id)).toEqual([
      'dispute_11111111',
      'dispute_33333333',
      'dispute_44444444',
      'dispute_22222222',
    ])
  })

  it('orders each maintainer activity table by its own event time', () => {
    const actions = [
      { maintainer_action_id: 'intent_22222222', created_at: '2026-05-15T10:00:00Z' },
      { maintainer_action_id: 'intent_11111111', created_at: '2026-05-15T10:00:00Z' },
      { maintainer_action_id: 'intent_33333333', created_at: '2026-05-14T10:00:00Z' },
    ]
    const sendLog = [
      {
        sent_at: '2026-05-14T10:00:00Z',
        agent_id: 'agent-c',
        inbox_id: 'inbox_33333333',
        maintainer_action_id: 'intent_33333333',
      },
      {
        sent_at: '2026-05-15T10:00:00Z',
        agent_id: 'agent-b',
        inbox_id: 'inbox_22222222',
        maintainer_action_id: 'intent_22222222',
      },
      {
        sent_at: '2026-05-15T10:00:00Z',
        agent_id: 'agent-a',
        inbox_id: 'inbox_11111111',
        maintainer_action_id: 'intent_11111111',
      },
    ]
    const outbox = [
      { inbox_id: 'inbox_22222222', created_at: '2026-05-15T10:00:00Z' },
      { inbox_id: 'inbox_11111111', created_at: '2026-05-15T10:00:00Z' },
      { inbox_id: 'inbox_33333333', created_at: '2026-05-14T10:00:00Z' },
    ]

    expect(orderActionsByCreatedAt(actions).map((row) => row.maintainer_action_id)).toEqual([
      'intent_11111111',
      'intent_22222222',
      'intent_33333333',
    ])
    expect(orderSendLogBySentAt(sendLog).map((row) => row.agent_id)).toEqual([
      'agent-a',
      'agent-b',
      'agent-c',
    ])
    expect(orderOutboxByCreatedAt(outbox).map((row) => row.inbox_id)).toEqual([
      'inbox_11111111',
      'inbox_22222222',
      'inbox_33333333',
    ])
  })
})
