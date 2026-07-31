import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { saveAdminSession } from '../features/auth/session'
import { renderWorkbenchRoute } from './test-utils'

describe('workbench routes', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/claims')) {
          return new Response(
            JSON.stringify([
              {
                claim: {
                  id: 'claim_1',
                  name: 'test_agent_outcome',
                  statement: 'Execution result',
                  scope: 'order-system / payment-service / prod',
                  holder: 'agent-a',
                  confidence: 'medium',
                  status: 'active',
                  created_at: '2026-05-14T17:49:22Z',
                  source_claim_ids: [],
                  evidence_summary: 'summary',
                },
                open_dispute_ids: [],
                resolved_dispute_ids: [],
              },
            ]),
          )
        }
        if (path.endsWith('/api/overview')) {
          return new Response(
            JSON.stringify({
              snapshot: {
                generated_at: '2026-05-15T10:00:00Z',
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
                ],
                policies: [],
                disputes: [
                  {
                    id: 'dispute_1',
                    name: 'test_context_dispute',
                    reporter_agent_id: 'agent-a',
                    claims: ['claim_1'],
                    summary: 'summary',
                    status: 'open',
                    created_at: '2026-05-15T09:00:00Z',
                  },
                ],
                actions: [],
                send_log: [],
              },
              latest_sweep: null,
              recent_policy_events: [],
              recent_agent_activities: [],
              recent_http_audits: [],
              recent_dispute_resolutions: [],
            }),
          )
        }
        if (path.endsWith('/api/disputes')) {
          return new Response(JSON.stringify([]))
        }

        return new Response(JSON.stringify([]))
      }),
    )
  })

  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
  })

  it('renders the claims page inside the app shell', async () => {
    saveAdminSession('admin', 'Basic test')
    render(renderWorkbenchRoute('/claims'))

    expect((await screen.findAllByRole('heading', { name: 'Claims' })).length).toBeGreaterThan(0)
    expect(screen.getByText('Browse mirrored claims published across your agent network.')).toBeInTheDocument()
    expect(screen.getByText('Maintainer Workbench')).toBeInTheDocument()
  })

  it('renders the overview route by default', async () => {
    saveAdminSession('admin', 'Basic test')
    render(renderWorkbenchRoute('/'))

    expect(await screen.findByRole('heading', { name: 'Network Operations Overview' })).toBeInTheDocument()
  })

  it('redirects protected routes to login without credentials', async () => {
    render(renderWorkbenchRoute('/claims'))

    expect(await screen.findByRole('heading', { name: 'Admin credentials' })).toBeInTheDocument()
  })

  it('renders protected routes directly when admin auth is disabled', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: false }))
        }
        if (path.endsWith('/api/claims')) {
          return new Response(JSON.stringify([]))
        }
        return new Response(JSON.stringify([]))
      }),
    )

    render(renderWorkbenchRoute('/claims'))

    expect((await screen.findAllByRole('heading', { name: 'Claims' })).length).toBeGreaterThan(0)
    expect(screen.queryByRole('heading', { name: 'Admin credentials' })).not.toBeInTheDocument()
  })

  it('returns to login when stored credentials are rejected by the API', async () => {
    saveAdminSession('admin', 'Basic stale')
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/claims')) {
          return new Response('admin auth required', { status: 401 })
        }
        return new Response(JSON.stringify([]))
      }),
    )

    render(renderWorkbenchRoute('/claims'))

    expect(await screen.findByRole('heading', { name: 'Admin credentials' })).toBeInTheDocument()
  })

  it('shows an inline error when admin credentials are wrong', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/admin-auth/check')) {
          return new Response('invalid', { status: 401 })
        }
        return new Response(JSON.stringify([]))
      }),
    )

    render(renderWorkbenchRoute('/login'))

    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'wrong' } })
    fireEvent.click(screen.getByRole('button', { name: /Open Workbench/i }))

    expect(await screen.findByText('用户名或密码不正确')).toBeInTheDocument()
  })

  it('toggles password visibility from the reveal button', async () => {
    render(renderWorkbenchRoute('/login'))

    const passwordInput = screen.getByLabelText('Password')
    const revealButton = screen.getByRole('button', { name: 'Show password' })

    fireEvent.change(passwordInput, { target: { value: 'secret' } })
    expect(passwordInput).toHaveAttribute('type', 'password')

    fireEvent.click(revealButton)
    expect(passwordInput).toHaveAttribute('type', 'text')

    fireEvent.click(screen.getByRole('button', { name: 'Hide password' }))
    expect(passwordInput).toHaveAttribute('type', 'password')
  })
})
