import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { renderWorkbenchRoute } from '../app/test-utils'
import { saveAdminSession } from '../features/auth/session'
import { formatDateTime } from '../lib/format'

const targetedPolicy = {
  id: 'policy_active_1',
  message_type: 'policy_update',
  name: 'payment_guardrail',
  statement: 'keep retries below threshold',
  scope: 'payments / prod',
  status: 'active',
  created_at: '2026-05-15T10:00:00Z',
  target_agents: ['agent-a', 'agent-b'],
}

const broadcastPolicy = {
  id: 'policy_deprecated_1',
  message_type: 'policy_update',
  name: 'legacy_guardrail',
  statement: 'old rule',
  scope: 'payments / prod',
  status: 'deprecated',
  created_at: '2026-05-14T10:00:00Z',
  updated_at: '2026-05-15T11:00:00Z',
}

const policiesResponse = {
  policies: [targetedPolicy, broadcastPolicy],
  outbox: [
    {
      inbox_id: 'inbox_targeted_a',
      maintainer_action_id: 'action_targeted',
      target_kind: 'targeted',
      target_agent: 'agent-a',
      created_at: '2026-05-15T10:01:00Z',
      offered_to: [{ agent_id: 'agent-a', first_offered_at: '2026-05-15T10:01:30Z', last_offered_at: '2026-05-15T10:01:30Z', attempts: 1 }],
      delivered_to: [{ agent_id: 'agent-a', sent_at: '2026-05-15T10:02:00Z' }],
      inbox_message: {
        id: 'inbox_targeted_a',
        message_type: 'policy_update',
        policy: targetedPolicy,
      },
    },
    {
      inbox_id: 'inbox_targeted_b',
      maintainer_action_id: 'action_targeted',
      target_kind: 'targeted',
      target_agent: 'agent-b',
      created_at: '2026-05-15T10:01:00Z',
      offered_to: [],
      delivered_to: [],
      inbox_message: {
        id: 'inbox_targeted_b',
        message_type: 'policy_update',
        policy: targetedPolicy,
      },
    },
    {
      inbox_id: 'inbox_broadcast_old_active',
      maintainer_action_id: 'action_broadcast_old_active',
      target_kind: 'broadcast',
      created_at: '2026-05-14T10:01:00Z',
      offered_to: [
        { agent_id: 'agent-c', first_offered_at: '2026-05-14T10:01:30Z', last_offered_at: '2026-05-14T10:01:30Z', attempts: 1 },
      ],
      delivered_to: [
        { agent_id: 'agent-c', sent_at: '2026-05-14T10:02:00Z' },
      ],
      inbox_message: {
        id: 'inbox_broadcast_old_active',
        message_type: 'policy_update',
        policy: { ...broadcastPolicy, status: 'active', updated_at: undefined },
      },
    },
    {
      inbox_id: 'inbox_broadcast',
      maintainer_action_id: 'action_broadcast',
      target_kind: 'broadcast',
      created_at: '2026-05-15T11:01:00Z',
      offered_to: [
        { agent_id: 'agent-a', first_offered_at: '2026-05-15T11:01:30Z', last_offered_at: '2026-05-15T11:01:30Z', attempts: 1 },
        { agent_id: 'agent-b', first_offered_at: '2026-05-15T11:02:30Z', last_offered_at: '2026-05-15T11:02:30Z', attempts: 1 },
      ],
      delivered_to: [
        { agent_id: 'agent-a', sent_at: '2026-05-15T11:02:00Z' },
        { agent_id: 'agent-b', sent_at: '2026-05-15T11:03:00Z' },
      ],
      inbox_message: {
        id: 'inbox_broadcast',
        message_type: 'policy_update',
        policy: broadcastPolicy,
      },
    },
  ],
  send_log: [],
  events: [],
}

const overviewResponse = {
  snapshot: {
    generated_at: '2026-05-15T10:00:00Z',
    counts: {
      agents: 2,
      claims: 2,
      active_claims: 2,
      stale_claims: 0,
      deprecated_claims: 0,
      active_policies: 1,
      deprecated_policies: 1,
      open_disputes: 0,
      resolved_disputes: 0,
      outbox_entries: 0,
      send_events: 0,
    },
    agents: [
      {
        agent_id: 'agent-a',
        mirror_claims: 1,
        active_claims: 1,
        stale_claims: 0,
        deprecated_claims: 0,
      },
      {
        agent_id: 'agent-b',
        mirror_claims: 1,
        active_claims: 1,
        stale_claims: 0,
        deprecated_claims: 0,
      },
    ],
    policies: [],
    disputes: [],
    actions: [],
    send_log: [],
  },
  latest_sweep: null,
  recent_policy_events: [],
  recent_agent_activities: [],
  recent_http_audits: [],
  recent_dispute_resolutions: [],
}

describe('PoliciesPage', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    saveAdminSession('admin', 'Basic test')
    vi.spyOn(Date, 'now').mockReturnValue(new Date('2026-05-20T00:00:00Z').getTime())
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/policies')) {
          return new Response(JSON.stringify(policiesResponse))
        }
        if (path.endsWith('/api/overview')) {
          return new Response(JSON.stringify(overviewResponse))
        }
        if (path.endsWith('/api/agents')) {
          return new Response(
            JSON.stringify([
              { agent_id: 'agent-a', mirror_claims: 1, active_claims: 1, stale_claims: 0, deprecated_claims: 0, recent_activities: [] },
              { agent_id: 'agent-b', mirror_claims: 1, active_claims: 1, stale_claims: 0, deprecated_claims: 0, recent_activities: [] },
            ]),
          )
        }
        if (path.endsWith('/policies/policy-update') || path.endsWith('/policies/claim-update-suggestion') || path.endsWith('/policies/policy-deprecation')) {
          return new Response(JSON.stringify({ ok: true, body: init?.body ? JSON.parse(String(init.body)) : null }))
        }
        return new Response(JSON.stringify([]))
      }),
    )
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('shows browse-first history view by default', async () => {
    render(renderWorkbenchRoute('/policies'))

    expect(await screen.findByRole('heading', { name: 'Policy History' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'New Action' })).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Publish Policy Update' })).not.toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Publish Claim Attribute Update' })).not.toBeInTheDocument()
    expect(await screen.findByText('1 / 2 receipt ACKs')).toBeInTheDocument()
    expect(screen.getByText('2 receipt ACKs for broadcast')).toBeInTheDocument()
  })

  it('shows updated time only for policies that have changed', async () => {
    render(renderWorkbenchRoute('/policies'))

    const historyHeading = await screen.findByRole('heading', { name: 'Policy History' })
    const historySection = historyHeading.closest('section')
    expect(historySection).not.toBeNull()
    const historyTable = within(historySection!).getByRole('table')

    const deprecatedRow = within(historyTable).getByText('legacy_guardrail').closest('tr')
    expect(deprecatedRow).not.toBeNull()
    fireEvent.click(deprecatedRow!)

    const deprecatedDrawer = await screen.findByRole('dialog', { name: 'legacy_guardrail' })
    expect(within(deprecatedDrawer).getByText('Updated At')).toBeInTheDocument()
    expect(
      within(deprecatedDrawer).getByText(formatDateTime(broadcastPolicy.updated_at)),
    ).toBeInTheDocument()

    fireEvent.click(
      within(deprecatedDrawer).getByRole('button', { name: 'Close detail drawer' }),
    )
    await waitFor(() => {
      expect(screen.queryByRole('dialog', { name: 'legacy_guardrail' })).not.toBeInTheDocument()
    })

    const activeRow = within(historyTable).getByText('payment_guardrail').closest('tr')
    expect(activeRow).not.toBeNull()
    fireEvent.click(activeRow!)

    const activeDrawer = await screen.findByRole('dialog', { name: 'payment_guardrail' })
    expect(within(activeDrawer).queryByText('Updated At')).not.toBeInTheDocument()
  })

  it('opens the action drawer and navigates to three action workspaces', async () => {
    render(renderWorkbenchRoute('/policies'))

    fireEvent.click(await screen.findByRole('button', { name: 'New Action' }))
    expect(await screen.findByText('Choose an action')).toBeInTheDocument()
    const chooserDrawer = await screen.findByLabelText('Policy action workspace')

    fireEvent.click(within(chooserDrawer).getByRole('button', { name: 'New PU' }))
    const newPuDrawer = await screen.findByLabelText('Policy action workspace')
    await waitFor(() => {
      expect(within(newPuDrawer).getByRole('button', { name: 'Close detail drawer' })).toHaveFocus()
    })
    expect(within(newPuDrawer).getByLabelText('Name')).toBeInTheDocument()
    expect(within(newPuDrawer).getByLabelText('Statement')).toBeInTheDocument()
    expect(within(newPuDrawer).getByText('Broadcast to all agents')).toBeInTheDocument()

    fireEvent.click(within(newPuDrawer).getAllByRole('button', { name: 'Back' })[0])
    fireEvent.click(within(await screen.findByLabelText('Policy action workspace')).getByRole('button', { name: 'Deprecate PU' }))
    const deprecateDrawer = await screen.findByLabelText('Policy action workspace')
    expect(within(deprecateDrawer).getByPlaceholderText('Search active policies')).toBeInTheDocument()
    expect(within(deprecateDrawer).getByText('payment_guardrail')).toBeInTheDocument()
    expect(within(deprecateDrawer).queryByText('legacy_guardrail')).not.toBeInTheDocument()

    fireEvent.click(within(deprecateDrawer).getAllByRole('button', { name: 'Back' })[0])
    fireEvent.click(within(await screen.findByLabelText('Policy action workspace')).getByRole('button', { name: 'CAU' }))
    const cauDrawer = await screen.findByLabelText('Policy action workspace')
    expect(within(cauDrawer).getByLabelText('Statement')).toBeInTheDocument()
    expect(within(cauDrawer).getByText('Broadcast to all agents')).toBeInTheDocument()
  })
})
