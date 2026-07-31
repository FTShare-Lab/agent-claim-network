import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { saveAdminSession } from '../features/auth/session'
import { renderWorkbenchRoute } from './test-utils'

const agent = {
  agent_id: 'agent-a',
  mirror_claims: 3,
  active_claims: 2,
  stale_claims: 1,
  deprecated_claims: 0,
  last_source_ip: '127.0.0.1',
  last_activity: {
    event_id: 'activity-a',
    agent_id: 'agent-a',
    activity_kind: 'claim_uploaded',
    occurred_at: '2026-05-15T10:00:00Z',
    summary: 'claim_uploaded claim_e1834b6f',
  },
  recent_activities: [
    {
      event_id: 'activity-a',
      agent_id: 'agent-a',
      activity_kind: 'claim_uploaded',
      occurred_at: '2026-05-15T10:00:00Z',
      summary: 'claim_uploaded claim_e1834b6f',
    },
  ],
}

const claim = {
  claim: {
    id: 'claim-a',
    name: 'claim-a-name',
    statement: 'claim a statement',
    scope: 'payments',
    holder: 'agent-a',
    confidence: 'high',
    status: 'active',
    created_at: '2026-05-15T09:00:00Z',
    source_claim_ids: [],
    evidence_summary: 'claim a evidence',
  },
  open_dispute_ids: ['dispute-open'],
  resolved_dispute_ids: [],
}

const dispute = {
  id: 'dispute-open',
  name: 'Scope mismatch',
  reporter_agent_id: 'agent-a',
  claims: ['claim-a'],
  summary: 'scope conflict',
  status: 'open',
  created_at: '2026-05-15T10:00:00Z',
}

const audit = {
  audit_id: 'audit-a',
  occurred_at: '2026-05-15T10:02:00Z',
  method: 'POST',
  path: '/api/team-auth/keys',
  status_code: 200,
  duration_ms: 12,
  source_ip: '127.0.0.1',
  request_body: '{"agent_id":"agent-a"}',
  response_body: '{"ok":true}',
  resource_id: 'key-a',
  summary: 'created key',
}

const policy = {
  id: 'policy-a',
  message_type: 'policy_update',
  name: 'payment_guardrail',
  statement: '{"version":1}',
  scope: 'payments / prod',
  status: 'active',
  created_at: '2026-05-15T10:00:00Z',
  target_agents: ['agent-a'],
}

const action = {
  created_at: '2026-05-15T10:01:00Z',
  maintainer_action_id: 'action-a',
  message_type: 'policy_update',
  policy_id: 'policy-a',
  policy_name: 'payment_guardrail',
  policy_scope: 'payments / prod',
  policy_status: 'active',
  target_kind: 'targeted',
  inbox_ids: ['inbox-a'],
  target_agents: ['agent-a'],
  delivered_agents: [],
  outbox_entries: 1,
  send_events: 0,
}

const outbox = {
  inbox_id: 'inbox-a',
  maintainer_action_id: 'action-a',
  target_kind: 'targeted',
  target_agent: 'agent-a',
  created_at: '2026-05-15T10:01:00Z',
  offered_to: [],
  delivered_to: [],
  inbox_message: {
    id: 'message-a',
    message_type: 'policy_update',
    policy,
  },
}

const policiesResponse = {
  policies: [policy],
  outbox: [outbox],
  send_log: [],
  events: [],
}

const sweep = {
  run_id: 'sweep-a',
  triggered_at: '2026-05-15T10:03:00Z',
  trigger: 'manual',
  report: {
    stale_claims: [['agent-a', 'claim-a']],
    deprecated_claims: [],
    notifications: [
      {
        agent_id: 'agent-a',
        stale_claims: ['claim-a'],
        deprecated_claims: [],
        policy_id: 'policy-a',
        pushed: 1,
      },
    ],
    notification_errors: [],
  },
}

const overviewResponse = {
  snapshot: {
    generated_at: '2026-05-15T10:05:00Z',
    counts: {
      agents: 1,
      claims: 1,
      active_claims: 1,
      stale_claims: 0,
      deprecated_claims: 0,
      active_policies: 1,
      deprecated_policies: 0,
      open_disputes: 1,
      resolved_disputes: 0,
      outbox_entries: 1,
      send_events: 0,
    },
    agents: [agent],
    policies: [policy],
    disputes: [dispute],
    actions: [action],
    send_log: [],
  },
  latest_sweep: sweep,
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

describe('route state shortcuts', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    saveAdminSession('admin', 'Basic test')
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/agents')) {
          return new Response(JSON.stringify([agent]))
        }
        if (path.endsWith('/api/audits')) {
          return new Response(JSON.stringify([audit]))
        }
        if (path.endsWith('/api/policies')) {
          return new Response(JSON.stringify(policiesResponse))
        }
        if (path.endsWith('/api/overview')) {
          return new Response(JSON.stringify(overviewResponse))
        }
        if (path.endsWith('/api/disputes')) {
          return new Response(JSON.stringify([dispute]))
        }
        if (path.endsWith('/api/claims')) {
          return new Response(JSON.stringify([claim]))
        }
        if (path.endsWith('/api/sweeps')) {
          return new Response(JSON.stringify([sweep]))
        }
        throw new Error(`Unhandled fetch: ${path}`)
      }),
    )
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('opens the agent drawer from route state', async () => {
    render(renderWorkbenchRoute({ pathname: '/agents', state: { agentId: 'agent-a' } }))

    expect(await screen.findByText('Recent Activities')).toBeInTheDocument()
    expect(screen.getByText('Mirror Claims')).toBeInTheDocument()
    expect(screen.getByText('claim_e1834b6f')).toBeInTheDocument()
    expect(screen.queryByText('claim_uploaded claim_e1834b6f')).not.toBeInTheDocument()
  })

  it('opens the dispute drawer from route state', async () => {
    render(renderWorkbenchRoute({ pathname: '/disputes', state: { disputeId: 'dispute-open' } }))

    expect(await screen.findByLabelText('Resolve Note')).toBeInTheDocument()
    expect(within(screen.getByLabelText('Affected agents')).getByText('agent-a')).toBeInTheDocument()
  })

  it('opens the HTTP audit drawer from route state', async () => {
    render(renderWorkbenchRoute({ pathname: '/http-audits', state: { auditId: 'audit-a' } }))

    expect(await screen.findByText('Request Body')).toBeInTheDocument()
    expect(screen.getByText('Response Body')).toBeInTheDocument()
  })

  it.each([
    [{ policyId: 'policy-a' }, 'Statement'],
    [{ actionId: 'action-a' }, 'Delivery Summary'],
    [{ outboxId: 'inbox-a' }, 'Inbox Message Snapshot'],
  ])('opens the policies drawer from route state %#', async (state, expectedText) => {
    render(renderWorkbenchRoute({ pathname: '/policies', state }))

    expect(await screen.findByText(expectedText)).toBeInTheDocument()
  })

  it('opens the sweep drawer from route state', async () => {
    render(renderWorkbenchRoute({ pathname: '/sweep', state: { runId: 'sweep-a' } }))

    expect(await screen.findByText('Run Summary')).toBeInTheDocument()
    expect(screen.getByText('Sweep Notifications')).toBeInTheDocument()
  })
})
