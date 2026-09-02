import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { renderWorkbenchRoute } from '../app/test-utils'
import { saveAdminSession } from '../features/auth/session'
import type { SweepRunRecord } from '../features/sweeps/types'

const NOW = new Date('2026-08-28T12:00:00Z').getTime()

function sweepRun(index: number, hoursAgo: number, trigger: SweepRunRecord['trigger']): SweepRunRecord {
  return {
    run_id: `sweep_run_${String(index).padStart(2, '0')}`,
    triggered_at: new Date(NOW - hoursAgo * 60 * 60 * 1000).toISOString(),
    trigger,
    report: {
      stale_claims: [],
      deprecated_claims: [],
      notifications: [],
      notification_errors: [],
    },
  }
}

function sweepRows() {
  const heading = screen.getByRole('heading', { name: 'Sweep History' })
  const section = heading.closest('section')
  if (!section) throw new Error('Unable to locate Sweep History section')
  return within(within(section).getByRole('table')).getAllByRole('row').slice(1)
}

describe('SweepPage history filters and pagination', () => {
  const sweeps = [
    sweepRun(1, 1, 'manual'),
    sweepRun(2, 6, 'ticker'),
    sweepRun(3, 20, 'maintainer_startup'),
    sweepRun(4, 48, 'manual'),
    sweepRun(5, 72, 'ticker'),
    sweepRun(6, 120, 'manual'),
    sweepRun(7, 168, 'maintainer_startup'),
    sweepRun(8, 240, 'ticker'),
    sweepRun(9, 336, 'manual'),
    sweepRun(10, 480, 'ticker'),
    sweepRun(11, 600, 'maintainer_startup'),
    sweepRun(12, 720, 'manual'),
    sweepRun(13, 960, 'ticker'),
  ]

  beforeEach(() => {
    window.sessionStorage.clear()
    saveAdminSession('admin', 'Basic test')
    vi.spyOn(Date, 'now').mockReturnValue(NOW)
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = typeof input === 'string' ? input : input.toString()
        if (path.endsWith('/api/admin-auth/status')) {
          return new Response(JSON.stringify({ enabled: true }))
        }
        if (path.endsWith('/api/sweeps')) {
          return new Response(JSON.stringify(sweeps))
        }
        if (path.endsWith('/api/overview')) {
          return new Response(JSON.stringify({
            sweep_schedule: {
              tick_interval_secs: 86_400,
              last_auto_sweep_at: sweeps[1].triggered_at,
              next_sweep_at: null,
              last_auto_trigger: 'ticker',
            },
          }))
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

  it('paginates the complete sweep history', async () => {
    render(renderWorkbenchRoute('/sweep'))

    expect(await screen.findByRole('heading', { name: 'Sweep History' })).toBeInTheDocument()
    expect(sweepRows()).toHaveLength(10)
    expect(screen.getByText('1–10 / 13')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Next page' }))

    expect(sweepRows()).toHaveLength(3)
    expect(screen.getByText('11–13 / 13')).toBeInTheDocument()
  })

  it('filters by time range and trigger before pagination', async () => {
    render(renderWorkbenchRoute('/sweep'))

    expect(await screen.findByRole('heading', { name: 'Sweep History' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }))

    fireEvent.change(screen.getByRole('combobox', { name: 'Time Range' }), {
      target: { value: '24h' },
    })
    expect(sweepRows()).toHaveLength(3)
    expect(screen.getByText('1–3 / 3')).toBeInTheDocument()

    fireEvent.change(screen.getByRole('combobox', { name: 'Triggered By' }), {
      target: { value: 'manual' },
    })
    expect(sweepRows()).toHaveLength(1)
    expect(within(sweepRows()[0]).getByText('Manual')).toBeInTheDocument()
    expect(screen.getByText('1–1 / 1')).toBeInTheDocument()

    fireEvent.change(screen.getByRole('combobox', { name: 'Time Range' }), {
      target: { value: 'all' },
    })
    expect(sweepRows()).toHaveLength(5)
    expect(screen.getByText('1–5 / 5')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Reset' }))
    expect(sweepRows()).toHaveLength(10)
    expect(screen.getByText('1–10 / 13')).toBeInTheDocument()
  })
})
