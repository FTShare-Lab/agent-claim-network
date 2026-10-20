import { StatusBadge } from '../components/badges/StatusBadge'
import { TeamAuthPage } from '../pages/TeamAuthPage'
// 验证展示切换不会改变业务值、表单输入或请求参数。
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { LANGUAGE_KEY, setLanguage, translate } from './index'
import { LanguageSwitch } from './LanguageSwitch'
import { AppProviders } from '../app/providers'
import { PolicyComposeDrawer } from '../features/policies/PolicyComposeDrawer'
import { ClaimDetails } from '../features/claims/ClaimDetails'
import { ClaimFilters } from '../features/claims/ClaimFilters'
import { emptyClaimFilters } from '../features/claims/filter'
import { makeClaim } from '../test/knowledgeFixtures'

afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals() })

function Filters() {
  const [filters, setFilters] = useState(emptyClaimFilters)
  return <><LanguageSwitch /><ClaimFilters filters={filters} agents={['example-agent']} onChange={(patch) => setFilters({ ...filters, ...patch })} /></>
}

describe('display language', () => {
  it('defaults to Chinese without a saved preference and synchronizes other tabs', () => {
    localStorage.removeItem(LANGUAGE_KEY)
    window.dispatchEvent(new StorageEvent('storage', { key: LANGUAGE_KEY }))
    render(<LanguageSwitch />)
    expect(screen.getByRole('combobox')).toHaveValue('zh')
    expect(document.documentElement.lang).toBe('zh-CN')
    localStorage.setItem(LANGUAGE_KEY, 'en')
    act(() => window.dispatchEvent(new StorageEvent('storage', { key: LANGUAGE_KEY, newValue: 'en' })))
    expect(screen.getByRole('combobox')).toHaveValue('en')
    expect(document.documentElement.lang).toBe('en')
  })

  it('switches immediately, saves the choice and keeps filters and enum values', () => {
    setLanguage('zh')
    render(<Filters />)
    fireEvent.change(screen.getByLabelText('搜索'), { target: { value: 'Overview 原始正文' } })
    fireEvent.change(screen.getByLabelText('状态'), { target: { value: 'stale' } })
    fireEvent.change(screen.getByLabelText('界面语言'), { target: { value: 'en' } })
    expect(screen.getByLabelText('Search')).toHaveValue('Overview 原始正文')
    expect(screen.getByLabelText('Status')).toHaveValue('stale')
    expect(localStorage.getItem(LANGUAGE_KEY)).toBe('en')
    fireEvent.change(screen.getByLabelText('Interface language'), { target: { value: 'zh' } })
    expect(screen.getByLabelText('搜索')).toHaveValue('Overview 原始正文')
    expect(screen.getByLabelText('状态')).toHaveValue('stale')
  })

  it('never translates raw Claim values even when they match a UI key', () => {
    const view = makeClaim('claim_00000001', [], 'Overview')
    view.claim.statement = 'Status'; view.claim.evidence_summary = 'Evidence'; view.claim.scope = 'Settings'
    const before = JSON.stringify(view)
    setLanguage('zh')
    render(<><LanguageSwitch /><ClaimDetails view={view} onOpenSource={() => {}} onOpenDispute={() => {}} /></>)
    expect(screen.getByText('正文')).toBeInTheDocument()
    expect(screen.getByText('Status')).toBeInTheDocument()
    expect(screen.getByText('Evidence')).toBeInTheDocument()
    expect(screen.getByText('Settings')).toBeInTheDocument()
    expect(screen.getByText('活跃')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('界面语言'), { target: { value: 'en' } })
    expect(screen.getByText('Statement')).toBeInTheDocument()
    expect(screen.getByText('active')).toBeInTheDocument()
    expect(JSON.stringify(view)).toBe(before)
  })

  it('switches when storage is unavailable and treats interpolation values as data', () => {
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('Unavailable') })
    render(<LanguageSwitch />)
    fireEvent.change(screen.getByLabelText('Interface language'), { target: { value: 'zh' } })
    expect(screen.getByLabelText('界面语言')).toHaveValue('zh')
    expect(translate('zh', 'Revoke key {key} for {agent}?', { key: 'Status', agent: 'Overview' })).toBe('撤销 Overview 的密钥 Status？')
  })
  it('preserves a policy draft across language changes and submits original values', async () => {
    const request = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => new Response(init?.method === 'GET' && String(input).endsWith('/api/agents') ? '[]' : '{}', { status: 200 }))
    vi.stubGlobal('fetch', request)
    setLanguage('zh')
    render(<AppProviders><PolicyComposeDrawer open policies={[]} onClose={() => {}} /></AppProviders>)
    fireEvent.click(screen.getByRole('button', { name: /^新建 Policy/ }))
    fireEvent.change(screen.getByLabelText('名称'), { target: { value: 'Overview' } })
    fireEvent.change(screen.getByLabelText('适用范围'), { target: { value: 'Settings' } })
    fireEvent.change(screen.getByLabelText('正文'), { target: { value: 'Status 原始内容' } })
    act(() => setLanguage('en'))
    expect(screen.getByLabelText('Name')).toHaveValue('Overview')
    expect(screen.getByLabelText('Scope')).toHaveValue('Settings')
    expect(screen.getByLabelText('Statement')).toHaveValue('Status 原始内容')
    await act(async () => fireEvent.click(screen.getByRole('button', { name: 'Publish New PU' })))
    expect(request).toHaveBeenCalled()
    const init = request.mock.calls.find(([, init]) => init?.method === 'POST')![1]!
    expect(JSON.parse(String(init.body))).toMatchObject({ name: 'Overview', scope: 'Settings', statement: 'Status 原始内容' })
    vi.unstubAllGlobals()
  })

  it('localizes only opted-in status badges, retaining their tone and original values', () => {
    const states = ['active', 'stale', 'deprecated', 'open', 'resolved', 'live', 'lagging', 'idle']
    render(<><LanguageSwitch />{states.map((state) => <StatusBadge key={state} localizeStatus>{state}</StatusBadge>)}<StatusBadge>active</StatusBadge></>)
    const before = screen.getAllByText('active')[0].className
    fireEvent.change(screen.getByLabelText('Interface language'), { target: { value: 'zh' } })
    for (const label of ['活跃', '陈旧', '废弃', '待处理', '已处理', '最近活跃', '最近未同步', '长期未同步']) {
      expect(screen.getByText(label)).toBeInTheDocument()
    }
    expect(screen.getByText('活跃').className).toBe(before)
    expect(screen.getByText('active')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('界面语言'), { target: { value: 'en' } })
    for (const state of states) expect(screen.getAllByText(state).length).toBeGreaterThan(0)
  })

  it('translates the admin-auth prerequisite immediately without refetching or changing the error', async () => {
    const error = 'Maintainer admin auth must be enabled to manage team auth keys.'
    const request = vi.fn(async (input: RequestInfo | URL) => String(input).endsWith('/api/team-auth/keys')
      ? new Response(error, { status: 403 })
      : new Response(JSON.stringify({ maintainer_team_auth_enabled: false, router_team_auth_enabled: false })))
    vi.stubGlobal('fetch', request)
    render(<AppProviders><LanguageSwitch /><TeamAuthPage /></AppProviders>)
    expect(await screen.findByText(error)).toBeInTheDocument()
    const requests = request.mock.calls.length
    fireEvent.change(screen.getByLabelText('Interface language'), { target: { value: 'zh' } })
    expect(screen.getByText('管理团队密钥前，请先启用 Maintainer 管理员鉴权。')).toBeInTheDocument()
    expect(request).toHaveBeenCalledTimes(requests)
    fireEvent.change(screen.getByLabelText('界面语言'), { target: { value: 'en' } })
    expect(screen.getByText(error)).toBeInTheDocument()
  })

})
