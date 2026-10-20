// 异步排布期间可切换页面；布局只缓存坐标，始终读取最新 Claim 内容。
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { knowledgePolicies, makeClaim } from '../../test/knowledgeFixtures'
import { ClaimOverview } from './ClaimOverview'
import { loadOverviewLayout } from './overviewLayoutBrowser'
import { setLanguage } from '../../i18n'
import type { OverviewLayout } from './overviewLayout'

vi.mock('./overviewLayoutBrowser', () => ({ loadOverviewLayout: vi.fn() }))
afterEach(() => { cleanup(); vi.resetAllMocks() })
const arranged: OverviewLayout = {
  nodes: [{ id: 'a', x: 36, y: 36 }, { id: 'b', x: 36, y: 166 }],
  edges: [{ source: 'a', target: 'b', points: [{ x: 146, y: 96 }, { x: 146, y: 166 }] }],
  width: 292, height: 262,
}
function setup(claims = [makeClaim('a', [], 'Source'), makeClaim('b', ['a'], 'Derived')]) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const select = vi.fn()
  const content = (values = claims) => <QueryClientProvider client={client}><ClaimOverview claims={values} onSelect={select} /></QueryClientProvider>
  return { ...render(content()), content, select }
}
describe('overview layout lifecycle', () => {
  it('shows progress then current content and details, without recalculating for content-only updates', async () => {
    let finish: (result: OverviewLayout) => void = () => {}
    vi.mocked(loadOverviewLayout).mockImplementation(() => new Promise((resolve) => { finish = resolve }))
    const app = setup()
    expect(screen.getByRole('status')).toHaveTextContent('Arranging connections…')
    await waitFor(() => expect(loadOverviewLayout).toHaveBeenCalledOnce())
    finish(arranged)
    fireEvent.click(await screen.findByRole('button', { name: /Source$/ }))
    expect(app.select).toHaveBeenCalledWith(expect.objectContaining({ id: 'a' }))
    app.rerender(app.content([makeClaim('a', [], 'Updated source'), makeClaim('b', ['a'], 'Derived')]))
    expect(screen.getByRole('button', { name: /Updated source$/ })).toBeInTheDocument()
    expect(loadOverviewLayout).toHaveBeenCalledOnce()
  })
  it('shows a retryable error instead of silently hiding edges', async () => {
    vi.mocked(loadOverviewLayout).mockRejectedValueOnce(new Error('Failed')).mockResolvedValue(arranged)
    setup()
    expect(await screen.findByRole('alert')).toHaveTextContent('Unable to arrange the graph.')
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('button', { name: /Source$/ })).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
  })
  it('renders PU and CAU with their own colors and details, and localizes combined-limit notices', async () => {
    setLanguage('zh')
    const policies = Array.from({ length: 21 }, (_, i) => ({ ...knowledgePolicies[i % 2], id: `policy_${i}`, name: `Policy ${i}` }))
    const claims = [makeClaim('a', policies.map((p) => p.id))]
    vi.mocked(loadOverviewLayout).mockImplementation(async (topology) => ({
      nodes: topology.ids.map((id, i) => ({ id, x: i * 260, y: 0 })),
      edges: topology.edges.map((edge) => ({ ...edge, points: [{ x: 0, y: 0 }, { x: 0, y: 100 }] })),
      width: 5600, height: 300,
    }))
    const select = vi.fn()
    render(<QueryClientProvider client={new QueryClient()}><ClaimOverview claims={claims} policies={policies} onSelect={select} /></QueryClientProvider>)
    const pu = await screen.findByRole('button', { name: /Policy 0$/ })
    expect(pu).toHaveClass('bg-orange-50')
    const cau = screen.getByRole('button', { name: /Policy 1$/ })
    expect(cau).toHaveClass('bg-violet-50')
    fireEvent.click(cau)
    expect(select).toHaveBeenCalledWith(expect.objectContaining({ id: 'policy_1', kind: 'claim_attribute_update', policy: policies[1] }))
    expect(screen.getByText(/显示 1 \/ 1 条 Claim · 20 \/ 21 条关联 PU\/CAU/)).toBeInTheDocument()
    expect(screen.getByText(/关联 PU\/CAU 合计最多显示 20 条/)).toBeInTheDocument()
    fireEvent.click(pu)
    expect(select).toHaveBeenLastCalledWith(expect.objectContaining({ id: 'policy_0', kind: 'policy_update' }))
  })
  it('aborts the outstanding layout when leaving the overview', async () => {
    let active: AbortSignal | undefined
    vi.mocked(loadOverviewLayout).mockImplementation((_, __, signal) => {
      active = signal
      return new Promise(() => {})
    })
    const app = setup()
    await waitFor(() => expect(active).toBeDefined())
    app.unmount()
    expect(active?.aborted).toBe(true)
  })
})
