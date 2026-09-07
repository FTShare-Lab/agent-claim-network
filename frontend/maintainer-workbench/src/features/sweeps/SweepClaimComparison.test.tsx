import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { SweepClaimComparison } from './SweepClaimComparison'
import type { SweepRunRecord } from './types'

const before = '2026-08-01T00:00:00Z'
const after = '2026-09-01T00:00:00Z'
const run: SweepRunRecord = {
  run_id: 'sweep_example', triggered_at: after, trigger: 'manual',
  report: {
    stale_claims: [['agent-example', 'claim_suppressed']], deprecated_claims: [], notification_errors: [],
    notifications: [{ agent_id: 'agent-example', stale_claims: ['claim_notified'], deprecated_claims: [], policy_id: 'policy_example', pushed: 0 }],
  },
}

function entry(agent = 'agent-example', historical = true, ack = false) {
  return {
    target_kind: 'targeted', target_agent: agent,
    inbox_message: { policy: { id: 'policy_example' } },
    delivered_to: ack ? [{ agent_id: agent, sent_at: after }] : [],
    ...(historical ? { sweep_items: [{ claim_id: 'claim_notified', effective_updated_at: before, suggested_status: 'stale' }] } : {}),
  }
}

function claim(holder = 'agent-example', status = 'active', updated_at = before) {
  return { claim: { id: 'claim_notified', holder, status, name: 'Example Claim', created_at: before, updated_at } }
}

function mount(selected = run) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter><SweepClaimComparison run={selected} /></MemoryRouter></QueryClientProvider>)
}

afterEach(() => { cleanup(); vi.unstubAllGlobals() })

describe('Sweep notified Claim comparison', () => {
  it('compares only notified Claims, joins by holder, and refreshes both mirror and ACK', async () => {
    let refreshed = false
    vi.stubGlobal('fetch', vi.fn(async (path: string) => new Response(JSON.stringify(
      path === '/outbox' ? [entry('other-agent', true, true), entry('agent-example', true, refreshed)]
        : [claim('other-agent', 'deprecated', after), claim('agent-example', refreshed ? 'stale' : 'active', refreshed ? after : before)],
    ))))
    mount()
    expect(await screen.findByText('Status differs from suggestion')).toBeInTheDocument()
    expect(screen.getByText('Same update time')).toBeInTheDocument()
    expect(screen.getByText('Awaiting ACK')).toBeInTheDocument()
    expect(screen.queryByText('claim_suppressed')).not.toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'claim_notified' })).toHaveAttribute('href', '/claims?claim_id=claim_notified')
    refreshed = true
    fireEvent.click(screen.getByRole('button', { name: 'Refresh current data' }))
    expect(await screen.findByText('Status matches suggestion')).toBeInTheDocument()
    expect(screen.getByText('Updated since notification')).toBeInTheDocument()
    expect(screen.getByText('ACK received')).toBeInTheDocument()
  })

  it('keeps the legacy suggestion with no invented historical timestamp', async () => {
    vi.stubGlobal('fetch', vi.fn(async (path: string) => new Response(JSON.stringify(path === '/outbox' ? [entry('agent-example', false)] : [claim()]))))
    mount()
    expect(await screen.findByText('Not recorded')).toBeInTheDocument()
    expect(screen.getByText('Update comparison unavailable')).toBeInTheDocument()
    expect(screen.getByText('Status differs from suggestion')).toBeInTheDocument()
  })

  it('does not substitute another holder or another policy when records are missing', async () => {
    const wrongPolicy = { ...entry(), inbox_message: { policy: { id: 'policy_other' } } }
    vi.stubGlobal('fetch', vi.fn(async (path: string) => new Response(JSON.stringify(path === '/outbox' ? [entry('other-agent'), wrongPolicy] : [claim('other-agent')]))))
    mount()
    expect(await screen.findByText('Delivery unavailable')).toBeInTheDocument()
    expect(screen.getByText('Claim unavailable')).toBeInTheDocument()
    expect(screen.getByText('Not recorded')).toBeInTheDocument()
  })

  it('does not fetch comparisons for a sweep with only suppressed candidates', () => {
    const fetch = vi.fn()
    vi.stubGlobal('fetch', fetch)
    mount({ ...run, report: { ...run.report, notifications: [] } })
    expect(screen.getByText(/This sweep generated no notifications/)).toBeInTheDocument()
    expect(fetch).not.toHaveBeenCalled()
  })

  it('shows a retryable error instead of misreporting missing records', async () => {
    let failed = true
    vi.stubGlobal('fetch', vi.fn(async (path: string) => failed ? new Response('Unavailable', { status: 503 })
      : new Response(JSON.stringify(path === '/outbox' ? [entry()] : [claim('agent-example', 'active', '2026-07-01T00:00:00Z')]))))
    mount()
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to load')
    expect(screen.queryByText('Claim unavailable')).not.toBeInTheDocument()
    failed = false
    fireEvent.click(screen.getByRole('button', { name: 'Refresh current data' }))
    expect(await screen.findByText('Current update time is earlier')).toBeInTheDocument()
    const article = screen.getByRole('article')
    expect(within(article).getByRole('link')).toBeInTheDocument()
  })
})
