// 覆盖根节点选择、筛选、方向切换及共享详情面板的完整路由行为。
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { renderWorkbenchRoute } from '../app/test-utils'
import { knowledgeClaims, knowledgePolicies, makeClaim } from '../test/knowledgeFixtures'

describe('Claims Flow page', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      if (path.endsWith('/api/admin-auth/status')) return new Response(JSON.stringify({ enabled: false }))
      if (path.endsWith('/api/claims')) return new Response(JSON.stringify(knowledgeClaims))
      if (path.endsWith('/api/policies')) return new Response(JSON.stringify({ policies: knowledgePolicies, outbox: [], send_log: [], events: [] }))
      if (path.endsWith('/api/disputes')) return new Response('[]')
      const claim = knowledgeClaims.find((entry) => path.endsWith(`/api/claims/${entry.claim.id}`))
      if (claim) return new Response(JSON.stringify(claim))
      throw new Error(`Unexpected endpoint: ${path}`)
    }))
  })

  afterEach(() => { cleanup(); vi.unstubAllGlobals() })

  it('requires an explicit root and keeps complete ancestry when search filters hide its sources', async () => {
    render(renderWorkbenchRoute('/knowledge-tree'))
    expect(await screen.findByText('Choose a Claim, Policy Update, or Claim Attribute Update to explore its flow.')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Search'), { target: { value: 'Release readiness evidence' } })
    const choices = within(screen.getByRole('region', { name: 'Choose root knowledge' }))
    expect(choices.queryByRole('button', { name: /Shared evidence/ })).not.toBeInTheDocument()
    fireEvent.click(choices.getByRole('button', { name: /Release readiness/ }))
    fireEvent.click(screen.getByRole('button', { name: 'Expand all' }))
    const canvas = within(screen.getByRole('region', { name: 'Claims flow canvas' }))
    expect(canvas.getAllByRole('button', { name: 'Claim: Shared evidence' })).toHaveLength(2)
    expect(canvas.getAllByRole('button', { name: 'Policy Update: Reliable execution' })).toHaveLength(2)
    expect(canvas.getByRole('button', { name: 'Claim Attribute Update: Review old evidence' })).toBeInTheDocument()
  })

  it('opens claim and policy details in place, follows sources, and supports back and re-rooting', async () => {
    render(renderWorkbenchRoute('/knowledge-tree?root_id=claim_00000001'))
    fireEvent.click(await screen.findByRole('button', { name: 'Claim: Release readiness (root)' }))
    let drawer = within(screen.getByRole('dialog'))
    expect(drawer.getByText('Release readiness statement')).toBeInTheDocument()
    expect(drawer.getByText('Release readiness evidence')).toBeInTheDocument()
    fireEvent.click(drawer.getByRole('button', { name: 'policy_00000002' }))
    drawer = within(screen.getByRole('dialog'))
    expect(drawer.getByText('Recheck evidence before adopting this claim.')).toBeInTheDocument()
    expect(drawer.getByRole('link', { name: 'View policy delivery details' })).toHaveAttribute('href', '/policies?policy_id=policy_00000002')
    fireEvent.click(drawer.getByRole('button', { name: 'Back to previous node' }))
    expect(within(screen.getByRole('dialog')).getByText('Release readiness statement')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Close detail drawer' }))
    fireEvent.click(screen.getByRole('button', { name: 'Expand all' }))
    fireEvent.click(screen.getAllByRole('button', { name: 'Policy Update: Reliable execution' })[0])
    fireEvent.click(screen.getByRole('button', { name: 'Explore from this node' }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(await screen.findByRole('button', { name: 'Policy Update: Reliable execution (root)' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Impact' }))
    fireEvent.click(screen.getByRole('button', { name: 'Expand all' }))
    expect(await screen.findAllByRole('button', { name: 'Claim: Release readiness' })).toHaveLength(2)
  })

  it('opens a deep linked policy impact tree and keeps arrow semantics when direction changes', async () => {
    render(renderWorkbenchRoute('/knowledge-tree?root_id=policy_00000001&direction=successors'))
    const button = await screen.findByRole('button', { name: 'Policy Update: Reliable execution (root)' })
    expect(button).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Impact' })).toHaveAttribute('aria-pressed', 'true')
    const canvas = screen.getByRole('region', { name: 'Claims flow canvas' })
    expect(canvas.querySelector('path[data-source="policy_00000001"][data-target="claim_00000004"]')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Sources' }))
    await waitFor(() => expect(within(screen.getByRole('region', { name: 'Claims flow canvas' })).getAllByRole('button')).toHaveLength(1))
  })

  it('starts each detail at the top when following a source or returning to the previous node', async () => {
    render(renderWorkbenchRoute('/knowledge-tree?root_id=claim_00000001'))
    fireEvent.click(await screen.findByRole('button', { name: 'Claim: Release readiness (root)' }))
    const detailBody = () => screen.getByRole('dialog').querySelector<HTMLDivElement>('.overflow-y-auto')!
    detailBody().scrollTop = 240
    fireEvent.click(within(screen.getByRole('dialog')).getByRole('button', { name: 'claim_00000002' }))
    expect(screen.getByRole('dialog', { name: 'Verified recovery' })).toBeInTheDocument()
    expect(detailBody().scrollTop).toBe(0)
    detailBody().scrollTop = 180
    fireEvent.click(screen.getByRole('button', { name: 'Back to previous node' }))
    expect(screen.getByRole('dialog', { name: 'Release readiness' })).toBeInTheDocument()
    expect(detailBody().scrollTop).toBe(0)
  })

  it('supports knowledge type, agent, status, scope, and disputed filters', async () => {
    render(renderWorkbenchRoute('/knowledge-tree'))
    const choices = within(await screen.findByRole('region', { name: 'Choose root knowledge' }))
    fireEvent.change(screen.getByLabelText('Knowledge type'), { target: { value: 'claim_attribute_update' } })
    expect(choices.getAllByRole('button')).toHaveLength(1)
    expect(choices.getByRole('button', { name: /Review old evidence/ })).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Status'), { target: { value: 'active' } })
    expect(choices.getByText('No knowledge matched the current filters.')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Knowledge type'), { target: { value: 'all' } })
    fireEvent.change(screen.getByLabelText('Agent'), { target: { value: 'example-agent' } })
    expect(choices.getAllByRole('button')).toHaveLength(4)
    fireEvent.change(screen.getByLabelText('Scope'), { target: { value: 'governance' } })
    expect(choices.queryAllByRole('button')).toHaveLength(0)
    fireEvent.change(screen.getByLabelText('Scope'), { target: { value: '' } })
    fireEvent.click(screen.getByLabelText('Only disputed claims'))
    expect(choices.queryAllByRole('button')).toHaveLength(0)
  })

  it('does not show a partial tree as complete when policy loading fails', async () => {
    const successfulFetch = vi.mocked(fetch)
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => String(input).endsWith('/api/policies')
      ? new Response('Unavailable', { status: 503 }) : successfulFetch(input)))
    render(renderWorkbenchRoute('/knowledge-tree?root_id=claim_00000001'))
    expect(await screen.findByRole('alert')).toHaveTextContent('Team knowledge could not be loaded.')
    expect(screen.queryByRole('region', { name: 'Claims flow canvas' })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
  })

  it('links from the existing claim inspector into its tree', async () => {
    render(renderWorkbenchRoute('/claims?claim_id=claim_00000001'))
    fireEvent.click(await screen.findByRole('button', { name: 'View claims flow' }))
    expect(await screen.findByRole('button', { name: 'Claim: Release readiness (root)' })).toBeInTheDocument()
  })

  it('renders a duplicate source only once in the shared claim inspector', async () => {
    const successfulFetch = vi.mocked(fetch)
    const duplicate = makeClaim('claim_00000001', ['claim_00000002', 'claim_00000002'])
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => String(input).endsWith('/api/claims/claim_00000001')
      ? new Response(JSON.stringify(duplicate)) : successfulFetch(input)))
    render(renderWorkbenchRoute('/claims?claim_id=claim_00000001'))
    const drawer = within(await screen.findByRole('dialog'))
    expect(drawer.getAllByRole('button', { name: 'claim_00000002' })).toHaveLength(1)
  })

  it('keeps cycles and unavailable sources visible without losing the reference ID', async () => {
    const successfulFetch = vi.mocked(fetch)
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => String(input).endsWith('/api/claims')
      ? new Response(JSON.stringify([makeClaim('claim_00000001', ['claim_00000001', 'policy_00000003'], 'Cyclic claim')])) : successfulFetch(input)))
    render(renderWorkbenchRoute('/knowledge-tree?root_id=claim_00000001'))
    expect(await screen.findByRole('button', { name: 'Claim: Cyclic claim (cycle ends here)' })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Unavailable source: Unavailable source' }))
    const drawer = within(screen.getByRole('dialog'))
    expect(drawer.getByText('policy_00000003')).toBeInTheDocument()
    expect(drawer.getByText(/details are unavailable in the current team snapshot/)).toBeInTheDocument()
  })

  it('reports an absent deep linked root without silently selecting a different claim', async () => {
    render(renderWorkbenchRoute('/knowledge-tree?root_id=claim_absent'))
    expect(await screen.findByText('The selected knowledge is unavailable. Choose another root from the list.')).toBeInTheDocument()
    expect(screen.queryByRole('region', { name: 'Claims flow canvas' })).not.toBeInTheDocument()
  })

  it('selects roots beyond the first page and resets pagination when filtering', async () => {
    const successfulFetch = vi.mocked(fetch)
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => String(input).endsWith('/api/claims')
      ? new Response(JSON.stringify(Array.from({ length: 12 }, (_, i) => makeClaim(`claim_${i}`, [], `Candidate ${i}`)))) : successfulFetch(input)))
    render(renderWorkbenchRoute('/knowledge-tree'))
    fireEvent.click(await screen.findByRole('button', { name: 'Next roots' }))
    const choices = within(screen.getByRole('region', { name: 'Choose root knowledge' }))
    const candidate = choices.getAllByRole('button').find((button) => button.textContent?.includes('Candidate'))!
    fireEvent.click(candidate)
    expect(await screen.findByRole('region', { name: 'Claims flow canvas' })).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Search'), { target: { value: 'Candidate 0' } })
    expect(choices.getByRole('button', { name: /Candidate 0/ })).toBeInTheDocument()
    expect(choices.queryByRole('button', { name: 'Next roots' })).not.toBeInTheDocument()
  })
})
