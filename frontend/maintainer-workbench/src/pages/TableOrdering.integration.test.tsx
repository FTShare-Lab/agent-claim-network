import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { renderWorkbenchRoute } from '../app/test-utils'
import { saveAdminSession } from '../features/auth/session'
import { formatDateTime } from '../lib/format'

function hexId(prefix: string, value: number) {
  return `${prefix}_${value.toString(16).padStart(8, '0')}`
}

function rowsUnderHeading(name: string) {
  const heading = screen.getByRole('heading', { name })
  const container =
    heading.tagName === 'H2'
      ? heading.closest('section')
      : heading.parentElement?.parentElement
  if (!container) throw new Error(`无法定位 ${name} 表格容器`)
  return within(within(container).getByRole('table')).getAllByRole('row').slice(1)
}

function policyRecord(value: number, createdAt: string) {
  return {
    id: hexId('policy', value),
    message_type: 'policy_update',
    name: `policy-${value}`,
    statement: `statement-${value}`,
    scope: 'runtime / test',
    status: 'active',
    created_at: createdAt,
  }
}

describe('workbench page table ordering', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    saveAdminSession('admin', 'Basic test')
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('sorts claims before pagination and renders created and optional updated times separately', async () => {
    const claims = Array.from({ length: 10 }, (_, index) => {
      const value = index + 1
      return {
        claim: {
          id: hexId('claim', value),
          name: `claim-${value}`,
          statement: `statement-${value}`,
          scope: 'runtime / test',
          holder: 'agent-a',
          confidence: 'high',
          status: 'active',
          created_at: `2026-07-${String(value + 9).padStart(2, '0')}T10:00:00Z`,
          source_claim_ids: [],
          evidence_summary: `evidence-${value}`,
        },
        open_dispute_ids: [],
        resolved_dispute_ids: [],
      }
    })
    const recentlyUpdated = {
      claim: {
        id: 'claim_f0000000',
        name: 'recently-updated-claim',
        statement: 'updated statement',
        scope: 'runtime / test',
        holder: 'agent-a',
        confidence: 'high',
        status: 'active',
        created_at: '2026-07-01T10:00:00Z',
        updated_at: '2026-07-29T10:00:00Z',
        source_claim_ids: [],
        evidence_summary: 'updated evidence',
      },
      open_dispute_ids: [],
      resolved_dispute_ids: [],
    }
    const newestStale = {
      claim: {
        ...recentlyUpdated.claim,
        id: 'claim_e0000000',
        name: 'newest-stale-claim',
        status: 'stale',
        updated_at: '2026-07-30T10:00:00Z',
      },
      open_dispute_ids: [],
      resolved_dispute_ids: [],
    }
    const allClaims = [...claims, recentlyUpdated, newestStale]

    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/claims')) {
          return new Response(JSON.stringify(allClaims))
        }
        const requestedClaim = allClaims.find((row) => path.endsWith(`/api/claims/${row.claim.id}`))
        if (requestedClaim) {
          return new Response(JSON.stringify(requestedClaim))
        }
        if (path.endsWith('/api/disputes')) {
          return new Response(JSON.stringify([]))
        }
        return new Response(JSON.stringify([]))
      }),
    )

    render(renderWorkbenchRoute('/claims'))

    expect(await screen.findByRole('heading', { name: 'All Claims' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Created At' })).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'Updated At' })).toBeInTheDocument()

    const rows = rowsUnderHeading('All Claims')
    expect(rows).toHaveLength(10)
    expect(within(rows[0]).getByText('recently-updated-claim')).toBeInTheDocument()
    expect(within(rows[0]).getByText(formatDateTime(recentlyUpdated.claim.created_at))).toBeInTheDocument()
    expect(within(rows[0]).getByText(formatDateTime(recentlyUpdated.claim.updated_at))).toBeInTheDocument()
    expect(within(rows[1]).getByText('N/A')).toBeInTheDocument()
    expect(rows.some((row) => within(row).queryByText('newest-stale-claim'))).toBe(false)

    fireEvent.click(within(rows[0]).getByText('recently-updated-claim'))
    const updatedDrawer = await screen.findByRole('dialog', { name: 'recently-updated-claim' })
    expect(within(updatedDrawer).getByText('Updated At')).toBeInTheDocument()
    expect(
      within(updatedDrawer).getByText(formatDateTime(recentlyUpdated.claim.updated_at)),
    ).toBeInTheDocument()

    fireEvent.click(
      within(updatedDrawer).getByRole('button', { name: 'Close detail drawer' }),
    )
    await waitFor(() => {
      expect(screen.queryByRole('dialog', { name: 'recently-updated-claim' })).not.toBeInTheDocument()
    })

    fireEvent.click(within(rows[1]).getByText('claim-10'))
    const unchangedDrawer = await screen.findByRole('dialog', { name: 'claim-10' })
    expect(within(unchangedDrawer).queryByText('Updated At')).not.toBeInTheDocument()
  })

  it('sorts registered agents by their latest activity before pagination', async () => {
    const agents = Array.from({ length: 10 }, (_, index) => {
      const value = index + 1
      return {
        agent_id: `agent-${value}`,
        mirror_claims: 1,
        active_claims: 1,
        stale_claims: 0,
        deprecated_claims: 0,
        last_source_ip: null,
        last_activity: {
          event_id: `agent_activity_${value}`,
          agent_id: `agent-${value}`,
          activity_kind: 'claim_uploaded',
          occurred_at: `2026-07-${String(value + 9).padStart(2, '0')}T10:00:00Z`,
          summary: `activity-${value}`,
        },
        recent_activities: [],
      }
    })
    const mostRecent = {
      ...agents[0],
      agent_id: 'agent-most-recent',
      last_activity: {
        ...agents[0].last_activity,
        agent_id: 'agent-most-recent',
        occurred_at: '2026-07-29T10:00:00Z',
      },
    }

    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/agents')) {
          return new Response(JSON.stringify([...agents, mostRecent]))
        }
        if (path.endsWith('/api/policies')) {
          return new Response(JSON.stringify({ policies: [], outbox: [], send_log: [], events: [] }))
        }
        if (path.endsWith('/api/audits')) {
          return new Response(JSON.stringify([]))
        }
        return new Response(JSON.stringify([]))
      }),
    )

    render(renderWorkbenchRoute('/agents'))

    expect(await screen.findByRole('heading', { name: 'Registered Agents' })).toBeInTheDocument()
    const rows = rowsUnderHeading('Registered Agents')
    expect(rows).toHaveLength(10)
    expect(within(rows[0]).getByText('agent-most-recent')).toBeInTheDocument()
  })

  it('filters agents by their latest activity time window', async () => {
    const now = Date.now()
    const occurredAt = (days: number, hours = 0) =>
      new Date(now - (days * 24 + hours) * 60 * 60 * 1000).toISOString()
    const agent = (agentId: string, lastActivityAt: string | null) => ({
      agent_id: agentId,
      mirror_claims: 0,
      active_claims: 0,
      stale_claims: 0,
      deprecated_claims: 0,
      last_source_ip: null,
      last_activity: lastActivityAt
        ? {
            event_id: `agent_activity_${agentId}`,
            agent_id: agentId,
            activity_kind: 'inbox_pulled',
            occurred_at: lastActivityAt,
            summary: 'inbox_pulled offered_messages=0',
          }
        : null,
      recent_activities: [],
    })
    const agents = [
      agent('agent-one-hour', occurredAt(0, 1)),
      agent('agent-three-days', occurredAt(3)),
      agent('agent-fourteen-days', occurredAt(14)),
      agent('agent-forty-five-days', occurredAt(45)),
      agent('agent-no-record', null),
    ]

    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/agents')) {
          return new Response(JSON.stringify(agents))
        }
        if (path.endsWith('/api/policies')) {
          return new Response(JSON.stringify({ policies: [], outbox: [], send_log: [], events: [] }))
        }
        if (path.endsWith('/api/audits')) {
          return new Response(JSON.stringify([]))
        }
        return new Response(JSON.stringify([]))
      }),
    )

    render(renderWorkbenchRoute('/agents'))

    expect(await screen.findByRole('heading', { name: 'Registered Agents' })).toBeInTheDocument()
    const activity = screen.getByRole('combobox', { name: 'Activity' })
    expect(rowsUnderHeading('Registered Agents')).toHaveLength(5)
    expect(screen.getAllByText('Inbox checked')).toHaveLength(4)
    expect(screen.getByText('Registered')).toBeInTheDocument()
    expect(screen.getByText('No activity recorded')).toBeInTheDocument()

    fireEvent.change(activity, { target: { value: '24h' } })
    expect(rowsUnderHeading('Registered Agents')).toHaveLength(1)
    expect(screen.getByText('agent-one-hour')).toBeInTheDocument()

    fireEvent.change(activity, { target: { value: '7d' } })
    expect(rowsUnderHeading('Registered Agents')).toHaveLength(2)
    expect(screen.getByText('agent-three-days')).toBeInTheDocument()

    fireEvent.change(activity, { target: { value: '30d' } })
    expect(rowsUnderHeading('Registered Agents')).toHaveLength(3)
    expect(screen.getByText('agent-fourteen-days')).toBeInTheDocument()

    fireEvent.change(activity, { target: { value: 'all' } })
    expect(rowsUnderHeading('Registered Agents')).toHaveLength(5)
  })

  it('sorts policy history and all maintainer activity tables before truncation', async () => {
    const policies = Array.from({ length: 10 }, (_, index) => {
      const value = index + 1
      return policyRecord(value, `2026-07-${String(value + 9).padStart(2, '0')}T10:00:00Z`)
    })
    const newestDeprecatedPolicy = {
      ...policyRecord(240, '2026-07-29T10:00:00Z'),
      status: 'deprecated',
      updated_at: '2026-07-29T11:00:00Z',
    }
    const latestActivePolicy = policies[policies.length - 1]
    const allPolicies = [...policies, newestDeprecatedPolicy]
    const actions = policies.map((policy, index) => ({
      created_at: policy.created_at,
      maintainer_action_id: hexId('intent', index + 1),
      message_type: 'policy_update',
      policy_id: policy.id,
      policy_name: policy.name,
      policy_scope: policy.scope,
      policy_status: policy.status,
      target_kind: 'targeted',
      inbox_ids: [hexId('inbox', index + 1)],
      target_agents: ['agent-a'],
      delivered_agents: [],
      outbox_entries: 1,
      send_events: 0,
    }))
    const latestAction = {
      ...actions[0],
      created_at: '2026-07-29T10:01:00Z',
      maintainer_action_id: 'intent_f0000000',
      policy_id: newestDeprecatedPolicy.id,
      policy_name: newestDeprecatedPolicy.name,
      inbox_ids: ['inbox_f0000000'],
    }
    const sendLog = policies.map((policy, index) => ({
      sent_at: policy.created_at,
      agent_id: `agent-${index + 1}`,
      inbox_id: hexId('inbox', index + 1),
      maintainer_action_id: hexId('intent', index + 1),
      policy_id: policy.id,
      message_type: 'policy_update',
    }))
    const latestSend = {
      sent_at: '2026-07-29T10:02:00Z',
      agent_id: 'agent-most-recent',
      inbox_id: 'inbox_f0000000',
      maintainer_action_id: 'intent_f0000000',
      policy_id: newestDeprecatedPolicy.id,
      message_type: 'policy_update',
    }
    const outbox = policies.map((policy, index) => ({
      inbox_id: hexId('inbox', index + 1),
      maintainer_action_id: hexId('intent', index + 1),
      target_kind: 'targeted',
      target_agent: 'agent-a',
      created_at: policy.created_at,
      offered_to: [],
      delivered_to: [],
      inbox_message: {
        id: hexId('inbox', index + 1),
        message_type: 'policy_update',
        policy,
      },
    }))
    const latestOutbox = {
      inbox_id: 'inbox_f0000000',
      maintainer_action_id: 'intent_f0000000',
      target_kind: 'targeted',
      target_agent: 'agent-a',
      created_at: '2026-07-29T10:01:00Z',
      offered_to: [],
      delivered_to: [],
      inbox_message: {
        id: 'inbox_f0000000',
        message_type: 'policy_update',
        policy: newestDeprecatedPolicy,
      },
    }
    const overview = {
      snapshot: {
        generated_at: '2026-07-29T10:03:00Z',
        counts: {
          agents: 1,
          claims: 0,
          active_claims: 0,
          stale_claims: 0,
          deprecated_claims: 0,
          active_policies: policies.length,
          deprecated_policies: 1,
          open_disputes: 0,
          resolved_disputes: 0,
          outbox_entries: outbox.length + 1,
          send_events: sendLog.length + 1,
        },
        agents: [],
        policies: allPolicies,
        disputes: [],
        actions: [...actions, latestAction],
        send_log: [...sendLog, latestSend],
      },
      latest_sweep: null,
      sweep_schedule: {
        tick_interval_secs: 86_400,
        last_auto_sweep_at: null,
        next_sweep_at: null,
        last_auto_trigger: null,
      },
      recent_policy_events: [],
      recent_agent_activities: [],
      recent_http_audits: [],
      recent_dispute_resolutions: [],
    }

    vi.spyOn(Date, 'now').mockReturnValue(new Date('2026-07-29T12:00:00Z').getTime())
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/policies')) {
          return new Response(
            JSON.stringify({
              policies: allPolicies,
              outbox: [...outbox, latestOutbox],
              send_log: [...sendLog, latestSend],
              events: [],
            }),
          )
        }
        if (path.endsWith('/api/overview')) {
          return new Response(JSON.stringify(overview))
        }
        if (path.endsWith('/api/agents')) {
          return new Response(JSON.stringify([]))
        }
        return new Response(JSON.stringify([]))
      }),
    )

    render(renderWorkbenchRoute('/policies'))

    expect(await screen.findByRole('heading', { name: 'Policy History' })).toBeInTheDocument()

    const policyRows = rowsUnderHeading('Policy History')
    expect(policyRows).toHaveLength(10)
    expect(within(policyRows[0]).getByText(latestActivePolicy.name)).toBeInTheDocument()
    expect(
      policyRows.some((row) => within(row).queryByText(newestDeprecatedPolicy.name)),
    ).toBe(false)

    const actionRows = rowsUnderHeading('Maintainer Actions')
    expect(actionRows).toHaveLength(8)
    expect(within(actionRows[0]).getByText(latestAction.maintainer_action_id)).toBeInTheDocument()

    const sendRows = rowsUnderHeading('Send Log')
    expect(sendRows).toHaveLength(8)
    expect(within(sendRows[0]).getByText(latestSend.agent_id)).toBeInTheDocument()

    const outboxRows = rowsUnderHeading('Outbox')
    expect(outboxRows).toHaveLength(8)
    expect(within(outboxRows[0]).getByText(latestOutbox.inbox_id)).toBeInTheDocument()
  })
})
