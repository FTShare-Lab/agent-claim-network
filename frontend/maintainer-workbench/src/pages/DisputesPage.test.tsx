import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { saveAdminSession } from '../features/auth/session'
import { renderWorkbenchRoute } from '../app/test-utils'

const openDispute = {
  id: 'dispute_open',
  name: 'Scope mismatch',
  reporter_agent_id: 'agent-a',
  claims: ['claim_a', 'claim_b'],
  summary: 'original dispute summary',
  status: 'open',
  created_at: '2026-05-15T10:00:00Z',
}

const resolvedDispute = {
  id: 'dispute_resolved',
  name: 'Already resolved',
  reporter_agent_id: 'agent-b',
  claims: ['claim_a'],
  summary: 'already resolved summary',
  status: 'resolved',
  created_at: '2026-05-14T10:00:00Z',
  resolved_at: '2026-05-15T11:00:00Z',
}

const claimsResponse = [
  {
    claim: {
      id: 'claim_a',
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
    open_dispute_ids: ['dispute_open'],
    resolved_dispute_ids: ['dispute_resolved'],
  },
  {
    claim: {
      id: 'claim_b',
      name: 'claim-b-name',
      statement: 'claim b statement',
      scope: 'payments',
      holder: 'agent-b',
      confidence: 'medium',
      status: 'active',
      created_at: '2026-05-15T09:30:00Z',
      source_claim_ids: ['claim_a'],
      evidence_summary: 'claim b evidence',
    },
    open_dispute_ids: ['dispute_open'],
    resolved_dispute_ids: [],
  },
]

describe('DisputesPage', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    saveAdminSession('admin', 'Basic test')
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/disputes')) {
          return new Response(JSON.stringify([openDispute, resolvedDispute]))
        }
        if (path.endsWith('/api/claims')) {
          return new Response(JSON.stringify(claimsResponse))
        }
        if (path.endsWith('/api/overview')) {
          return new Response(JSON.stringify({}))
        }
        if (path.endsWith('/disputes/dispute_open/resolve')) {
          if (init?.method !== 'POST') {
            throw new Error(`Unexpected resolve method: ${init?.method}`)
          }
          return new Response(null, { status: 204 })
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

  it('submits resolve note and notify flag for open disputes', async () => {
    render(renderWorkbenchRoute('/disputes'))

    fireEvent.click(await screen.findByText('Scope mismatch'))
    const note = await screen.findByLabelText('Resolve Note')
    const affectedAgents = screen.getByLabelText('Affected agents')
    expect(within(affectedAgents).getByText('agent-a')).toBeInTheDocument()
    expect(within(affectedAgents).getByText('agent-b')).toBeInTheDocument()
    fireEvent.change(note, { target: { value: '  Use agent-a scope as canonical.  ' } })
    fireEvent.click(screen.getByRole('checkbox', { name: /Notify Affected Agents/ }))
    fireEvent.click(screen.getByRole('button', { name: 'Resolve Dispute' }))

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        '/disputes/dispute_open/resolve',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({
            resolve_note: 'Use agent-a scope as canonical.',
            notify_affected_agents: true,
          }),
        }),
      )
    })
  })

  it('renders related claim chips and full claim detail block', async () => {
    render(renderWorkbenchRoute('/disputes'))

    fireEvent.click(await screen.findByText('Scope mismatch'))

    expect(await screen.findAllByText('claim-a-name')).not.toHaveLength(0)
    expect(screen.getAllByText('claim-b-name')).not.toHaveLength(0)
    const details = screen.getByLabelText('Related claim details')
    expect(details).toHaveTextContent('"holder": "agent-a"')
    expect(details).toHaveTextContent('"statement": "claim b statement"')
  })

  it('submits notify=false by default', async () => {
    render(renderWorkbenchRoute('/disputes'))

    fireEvent.click(await screen.findByText('Scope mismatch'))
    fireEvent.change(await screen.findByLabelText('Resolve Note'), { target: { value: 'No notification needed.' } })
    fireEvent.click(screen.getByRole('button', { name: 'Resolve Dispute' }))

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        '/disputes/dispute_open/resolve',
        expect.objectContaining({
          method: 'POST',
          body: JSON.stringify({
            resolve_note: 'No notification needed.',
            notify_affected_agents: false,
          }),
        }),
      )
    })
  })

  it('shows an inline error and does not submit an empty resolve note', async () => {
    render(renderWorkbenchRoute('/disputes'))

    fireEvent.click(await screen.findByText('Scope mismatch'))
    fireEvent.click(screen.getByRole('button', { name: 'Resolve Dispute' }))

    expect(await screen.findByRole('alert', { name: '' })).toHaveTextContent('Resolve Note 不能为空')
    expect(fetch).not.toHaveBeenCalledWith('/disputes/dispute_open/resolve', expect.anything())
  })

  it('hides resolve inputs and disables resolve button for resolved disputes', async () => {
    render(renderWorkbenchRoute('/disputes'))

    fireEvent.click(await screen.findByText('Already resolved'))

    expect(screen.queryByLabelText('Resolve Note')).not.toBeInTheDocument()
    expect(screen.queryByRole('checkbox', { name: /Notify Affected Agents/ })).not.toBeInTheDocument()
    expect(await screen.findByRole('button', { name: 'Resolve Dispute' })).toBeDisabled()
  })
})
