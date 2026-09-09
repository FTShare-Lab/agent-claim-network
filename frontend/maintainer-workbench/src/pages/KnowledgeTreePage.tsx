// 从团队知识中选取根节点，按来源或影响方向浏览知识树。
import { useMemo, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router'

import { ClaimFilters } from '../features/claims/ClaimFilters'
import { emptyClaimFilters, matchesClaimFilters, type ClaimFilterValues } from '../features/claims/filter'
import { useClaimsQuery } from '../features/claims/hooks'
import { KnowledgeDetails } from '../features/knowledge-tree/KnowledgeDetails'
import { KnowledgeTree } from '../features/knowledge-tree/KnowledgeTree'
import { knowledgeStyles } from '../features/knowledge-tree/styles'
import { indexKnowledge, type Direction, type Knowledge } from '../features/knowledge-tree/tree'
import { usePoliciesQuery } from '../features/policies/hooks'
import { PageContainer } from '../layouts/PageContainer'
import { orderClaimsByStatusAndRecentChange, orderPoliciesByTypeAndRecentChange } from '../lib/tableOrdering'
import { cn } from '../lib/utils'

const PAGE_SIZE = 8

export function KnowledgeTreePage() {
  const claims = useClaimsQuery()
  const policies = usePoliciesQuery()
  const navigate = useNavigate()
  const [params, setParams] = useSearchParams()
  const rootId = params.get('root_id') ?? ''
  const direction: Direction = params.get('direction') === 'successors' ? 'successors' : 'predecessors'
  const [filters, setFilters] = useState(emptyClaimFilters)
  const [kind, setKind] = useState('all')
  const [page, setPage] = useState(1)
  const [history, setHistory] = useState<{ rootId: string; ids: string[] }>({ rootId: '', ids: [] })
  const ids = history.rootId === rootId ? history.ids : []
  const selectedId = ids[ids.length - 1]
  const index = useMemo(() => indexKnowledge(
    orderClaimsByStatusAndRecentChange(claims.data ?? []),
    orderPoliciesByTypeAndRecentChange(policies.data?.policies ?? []),
  ), [claims.data, policies.data])
  const agents = useMemo(() => [...new Set((claims.data ?? []).map((view) => view.claim.holder))].sort(), [claims.data])
  const matches = useMemo(() => [...index.entries.values()].filter((entry) => {
    if (entry.kind === 'missing' || (kind !== 'all' && entry.kind !== kind)) return false
    if (entry.kind === 'claim') return matchesClaimFilters(entry.view, filters)
    if (filters.agent !== 'all' || filters.onlyDisputed) return false
    const policy = entry.policy
    return (filters.status === 'all' || policy.status === filters.status)
      && policy.scope.toLowerCase().includes(filters.scope.toLowerCase())
      && `${policy.id} ${policy.name} ${policy.statement} ${policy.scope}`.toLowerCase().includes(filters.keyword.toLowerCase())
  }), [filters, index, kind])
  const currentPage = Math.min(page, Math.max(1, Math.ceil(matches.length / PAGE_SIZE)))
  const root = index.entries.get(rootId)
  const selected = index.entries.get(selectedId)

  function selectRoot(id: string) {
    const next = new URLSearchParams(params)
    next.set('root_id', id)
    setParams(next)
    setHistory({ rootId: id, ids: [] })
  }

  function changeFilters(patch: Partial<ClaimFilterValues>) {
    setFilters({ ...filters, ...patch })
    setPage(1)
  }

  function openKnowledge(entry: Knowledge) {
    setHistory({ rootId, ids: selectedId === entry.id ? ids : [...ids, entry.id] })
  }

  const error = claims.error || policies.error
  return (
    <PageContainer title="Knowledge Tree" subtitle="Explore where team knowledge comes from and how it shapes later claims."
      actions={
        <div role="group" aria-label="Traversal direction" className="inline-flex rounded-lg border border-slate-200 bg-white p-1 text-sm">
          {(['predecessors', 'successors'] as const).map((value) => (
            <button key={value} type="button" aria-pressed={direction === value}
              className={cn('rounded-md px-3 py-2 font-medium', direction === value ? 'bg-[var(--accent-weak)] text-[var(--accent-strong)]' : 'text-slate-500 hover:bg-slate-50')}
              onClick={() => { const next = new URLSearchParams(params); next.set('direction', value); setParams(next) }}
            >{value === 'predecessors' ? 'Sources' : 'Impact'}</button>
          ))}
        </div>
      }
    >
      <ClaimFilters filters={filters} agents={agents} onChange={changeFilters} />
      <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-slate-600">
        <label className="flex items-center gap-2">Knowledge type
          <select className="rounded-md border border-slate-200 bg-white px-2.5 py-1.5" value={kind} onChange={(event) => { setKind(event.target.value); setPage(1) }}>
            <option value="all">All types</option>
            <option value="claim">Claim</option>
            <option value="policy_update">Policy</option>
            <option value="claim_attribute_update">Attribute suggestion</option>
          </select>
        </label>
        <label className="inline-flex items-center gap-2"><input type="checkbox" checked={filters.onlyDisputed} onChange={(event) => changeFilters({ onlyDisputed: event.target.checked })} />Only disputed claims</label>
      </div>
      {error ? (
        <div role="alert" className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">
          <p>Team knowledge could not be loaded. {String(error)}</p>
          <button type="button" className="mt-3 font-semibold underline" onClick={() => { void claims.refetch(); void policies.refetch() }}>Retry</button>
        </div>
      ) : claims.isLoading || policies.isLoading ? <p role="status" className="p-6 text-sm text-slate-500">Loading team knowledge…</p> : (
        <div className="grid items-start gap-4 lg:grid-cols-[240px_minmax(0,1fr)]">
          <section aria-label="Choose root knowledge" className="overflow-hidden rounded-xl border border-slate-200 bg-white">
            <div className="border-b border-slate-100 px-4 py-3">
              <h2 className="text-sm font-semibold text-slate-900">Choose a root</h2>
              <p className="mt-1 text-xs text-slate-500">{matches.length} matches · Filters apply to this list.</p>
            </div>
            <div className="max-h-80 overflow-y-auto lg:max-h-[560px]">
              {matches.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE).map((entry) => (
                <button key={entry.id} type="button" aria-pressed={rootId === entry.id} onClick={() => selectRoot(entry.id)}
                  className={cn('block w-full border-b border-slate-100 px-4 py-3 text-left last:border-b-0 hover:bg-slate-50', rootId === entry.id && 'bg-[var(--accent-weak)]')}
                >
                  <span className="mb-1 flex items-center gap-1.5 text-[11px] text-slate-500"><span className={cn('h-2 w-2 rounded-full', knowledgeStyles[entry.kind].dot)} />{knowledgeStyles[entry.kind].label}</span>
                  <span className="block text-sm font-medium text-slate-900 [overflow-wrap:anywhere]">{entry.name}</span>
                  <span className="mt-1 block truncate font-mono text-[10px] text-slate-500" title={entry.id}>{entry.id}</span>
                </button>
              ))}
              {matches.length === 0 ? <p className="p-4 text-sm text-slate-500">No knowledge matched the current filters.</p> : null}
            </div>
            {matches.length > PAGE_SIZE ? <div className="flex items-center justify-between border-t border-slate-100 p-3 text-xs">
              <button type="button" aria-label="Previous roots" disabled={currentPage === 1} className="rounded px-2 py-1 disabled:opacity-40" onClick={() => setPage(currentPage - 1)}>Previous</button>
              <span>{currentPage} / {Math.ceil(matches.length / PAGE_SIZE)}</span>
              <button type="button" aria-label="Next roots" disabled={currentPage * PAGE_SIZE >= matches.length} className="rounded px-2 py-1 disabled:opacity-40" onClick={() => setPage(currentPage + 1)}>Next</button>
            </div> : null}
          </section>
          <div className="min-w-0 space-y-3">
            {root ? (
              <>
                <div className="px-1">
                  <h2 className="text-sm font-semibold text-slate-900 [overflow-wrap:anywhere]">{root.name}</h2>
                  <p className="mt-1 text-xs leading-5 text-slate-500">{direction === 'predecessors' ? 'Sources branch upward from the selected root.' : 'Derived claims branch downward from the selected root.'} Click a node to inspect its details.</p>
                </div>
                <KnowledgeTree key={`${rootId}:${direction}`} index={index} rootId={rootId} direction={direction} selectedId={selectedId} onSelect={openKnowledge} />
              </>
            ) : <div role="status" className="flex min-h-96 items-center justify-center rounded-xl border border-dashed border-slate-300 bg-white p-8 text-center text-sm text-slate-500">
              {rootId ? 'The selected knowledge is unavailable. Choose another root from the list.' : 'Choose a claim, policy, or attribute suggestion to explore its knowledge tree.'}
            </div>}
          </div>
        </div>
      )}
      {!error && selected ? <KnowledgeDetails knowledge={selected} onClose={() => setHistory({ rootId, ids: [] })}
        onRoot={selectRoot}
        onSource={(id) => { const entry = index.entries.get(id); if (entry) openKnowledge(entry) }}
        onDispute={(id) => navigate('/disputes', { state: { disputeId: id } })}
        onBack={ids.length > 1 ? () => setHistory({ rootId, ids: ids.slice(0, -1) }) : undefined}
      /> : null}
    </PageContainer>
  )
}
