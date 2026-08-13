import { useMemo, useState } from 'react'
import { useSearchParams } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { DetailTextBlock } from '../components/drawer/DetailTextBlock'
import { FilterBar } from '../components/filters/FilterBar'
import { PaginationBar } from '../components/pagination/PaginationBar'
import { useDisputesQuery } from '../features/disputes/hooks'
import type { Dispute } from '../features/disputes/types'
import { PageContainer } from '../layouts/PageContainer'
import { useClaimQuery, useClaimsQuery } from '../features/claims/hooks'
import type { ClaimView } from '../features/claims/types'
import { formatDateTime, truncateMiddle } from '../lib/format'
import { orderClaimsByStatusAndRecentChange } from '../lib/tableOrdering'

function paginate<T>(items: T[], page: number, pageSize: number) {
  const start = (page - 1) * pageSize
  return items.slice(start, start + pageSize)
}

function severityFromDispute(dispute: Dispute) {
  const summary = `${dispute.name} ${dispute.summary}`.toLowerCase()
  if (summary.includes('conflict') || summary.includes('mismatch')) return 'high'
  if (summary.includes('scope') || summary.includes('integrity')) return 'medium'
  return 'low'
}

function severityTone(dispute: Dispute) {
  const severity = severityFromDispute(dispute)
  if (severity === 'high') return 'danger'
  if (severity === 'medium') return 'warning'
  return 'success'
}

type DrawerResource = {
  type: 'claim' | 'dispute'
  id: string
}

type DrawerState = {
  current: DrawerResource
  previous?: DrawerResource
} | null

const chipLinkClass = "rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 font-mono text-[11px] font-medium text-blue-700 transition hover:bg-blue-100"

export function ClaimsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const claimIdFromUrl = searchParams.get('claim_id')
  const { data = [], isLoading, error } = useClaimsQuery()
  const { data: disputes = [] } = useDisputesQuery()
  const [keyword, setKeyword] = useState('')
  const [agent, setAgent] = useState('all')
  const [status, setStatus] = useState('all')
  const [scope, setScope] = useState('')
  const [onlyDisputed, setOnlyDisputed] = useState(false)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [drawerState, setDrawerState] = useState<DrawerState>(null)
  const urlDrawerState: DrawerState = claimIdFromUrl
    ? { current: { type: 'claim', id: claimIdFromUrl } }
    : null
  const effectiveDrawerState = drawerState ?? urlDrawerState
  const selectedClaimId =
    effectiveDrawerState?.current.type === 'claim' ? effectiveDrawerState.current.id : null
  const selectedClaim = useClaimQuery(selectedClaimId)
  const selectedDispute =
    effectiveDrawerState?.current.type === 'dispute'
      ? disputes.find((item) => item.id === effectiveDrawerState.current.id) ?? null
      : null

  const agents = useMemo(() => Array.from(new Set(data.map((row) => row.claim.holder))).sort(), [data])

  const filtered = useMemo(() => {
    return orderClaimsByStatusAndRecentChange(
      data.filter((row) => {
        const haystack = `${row.claim.id} ${row.claim.name} ${row.claim.statement} ${row.claim.evidence_summary} ${row.claim.scope}`.toLowerCase()
        if (keyword && !haystack.includes(keyword.toLowerCase())) return false
        if (agent !== 'all' && row.claim.holder !== agent) return false
        if (status !== 'all' && row.claim.status !== status) return false
        if (scope && !row.claim.scope.toLowerCase().includes(scope.toLowerCase())) return false
        if (onlyDisputed && row.open_dispute_ids.length === 0 && row.resolved_dispute_ids.length === 0) return false
        return true
      }),
    )
  }, [agent, data, keyword, onlyDisputed, scope, status])

  const pagedRows = paginate(filtered, page, pageSize)

  function openClaim(claimId: string) {
    setDrawerState({ current: { type: 'claim', id: claimId } })
  }

  function openDispute(disputeId: string) {
    setDrawerState((current) => {
      const currentState = current ?? effectiveDrawerState
      if (!currentState) {
        return { current: { type: 'dispute', id: disputeId } }
      }

      if (currentState.current.type === 'dispute' && currentState.current.id === disputeId) {
        return currentState
      }

      return {
        current: { type: 'dispute', id: disputeId },
        previous: currentState.current,
      }
    })
  }

  function goBack() {
    setDrawerState((current) => {
      if (!current?.previous) {
        return current
      }

      return {
        current: current.previous,
      }
    })
  }

  function closeDrawer() {
    setDrawerState(null)
    if (!claimIdFromUrl) return
    const next = new URLSearchParams(searchParams)
    next.delete('claim_id')
    setSearchParams(next, { replace: true })
  }

  if (isLoading) return <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm text-slate-500">Loading claims…</div>
  if (error) return <div className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">{String(error)}</div>

  return (
    <PageContainer title="Claims" subtitle="Browse mirrored claims published across your agent network.">
      <FilterBar>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Search</span>
          <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" placeholder="id, statement, evidence, scope" value={keyword} onChange={(event) => { setKeyword(event.target.value); setPage(1) }} />
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Agent</span>
          <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={agent} onChange={(event) => { setAgent(event.target.value); setPage(1) }}>
            <option value="all">All Agents</option>
            {agents.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Status</span>
          <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={status} onChange={(event) => { setStatus(event.target.value); setPage(1) }}>
            <option value="all">All Status</option>
            <option value="active">Active</option>
            <option value="stale">Stale</option>
            <option value="deprecated">Deprecated</option>
          </select>
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Scope</span>
          <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" placeholder="scope fragment" value={scope} onChange={(event) => { setScope(event.target.value); setPage(1) }} />
        </label>
      </FilterBar>

      <section className="space-y-3">
        <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-2">
            <h2 className="text-sm font-semibold tracking-tight text-slate-900">All Claims</h2>
            <StatusBadge tone="info">{filtered.length}</StatusBadge>
          </div>
          <label className="inline-flex items-center gap-2 text-xs text-slate-600">
            <input type="checkbox" checked={onlyDisputed} onChange={(event) => { setOnlyDisputed(event.target.checked); setPage(1) }} />
            Only disputed claims
          </label>
        </div>
        <DataTable
          columns={[
            {
              key: 'claim',
              header: 'Claim',
              render: (row: ClaimView) => (
                <div>
                  <div className="font-medium text-slate-900">{row.claim.name}</div>
                  <div className="mt-0.5 font-mono text-[11px] text-slate-500">{row.claim.id}</div>
                </div>
              ),
            },
            { key: 'holder', header: 'Holder', render: (row: ClaimView) => <span className="font-mono text-xs">{row.claim.holder}</span> },
            { key: 'status', header: 'Status', render: (row: ClaimView) => <StatusBadge>{row.claim.status}</StatusBadge> },
            {
              key: 'scope',
              header: 'Scope',
              mobileHidden: true,
              render: (row: ClaimView) => <div className="max-w-xs text-xs leading-5 text-slate-600">{row.claim.scope}</div>,
            },
            {
              key: 'disputes',
              header: 'Related Disputes',
              mobileHidden: true,
              render: (row: ClaimView) =>
                row.open_dispute_ids.length || row.resolved_dispute_ids.length ? (
                  <div className="flex flex-wrap gap-1">
                    {[...row.open_dispute_ids, ...row.resolved_dispute_ids].map((id) => (
                      <button
                        key={id}
                        type="button"
                        onClick={(event) => {
                          event.stopPropagation()
                          openDispute(id)
                        }}
                        className={chipLinkClass}
                      >
                        {truncateMiddle(id)}
                      </button>
                    ))}
                  </div>
                ) : (
                  <span className="text-xs text-slate-500">No disputes</span>
                ),
            },
            { key: 'created', header: 'Created At', render: (row: ClaimView) => <span className="font-mono text-xs">{formatDateTime(row.claim.created_at)}</span> },
            { key: 'updated', header: 'Updated At', render: (row: ClaimView) => <span className="font-mono text-xs">{formatDateTime(row.claim.updated_at)}</span> },
            { key: 'actions', header: 'Actions', mobileHidden: true, render: () => <span className="text-xs font-medium text-blue-700">View</span> },
          ]}
          rows={pagedRows}
          getRowId={(row) => row.claim.id}
          onRowClick={(row) => openClaim(row.claim.id)}
          emptyState="No claims matched the current filters."
        />
        <PaginationBar page={page} pageSize={pageSize} total={filtered.length} onPageChange={setPage} onPageSizeChange={(size) => { setPageSize(size); setPage(1) }} />
      </section>

      <DetailDrawer
        modal={false}
        size="default"
        open={Boolean(effectiveDrawerState?.current && (selectedClaim.data || selectedDispute))}
        onClose={closeDrawer}
        onBack={effectiveDrawerState?.previous ? goBack : undefined}
        backLabel={
          effectiveDrawerState?.previous
            ? effectiveDrawerState.previous.type === 'claim'
              ? 'Back to claim'
              : 'Back to dispute'
            : undefined
        }
        label={selectedDispute ? 'Dispute' : 'Claim'}
        title={selectedDispute?.name ?? selectedClaim.data?.claim.name ?? 'Claim'}
        subtitle={selectedDispute?.id ?? selectedClaim.data?.claim.id}
      >
        {selectedClaim.data ? (
          <>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Overview</div>
              <dl className="mt-2 space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Holder Agent</dt><dd className="font-mono text-xs text-slate-900">{selectedClaim.data.claim.holder}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge>{selectedClaim.data.claim.status}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Confidence</dt><dd className="font-medium capitalize text-slate-900">{selectedClaim.data.claim.confidence}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedClaim.data.claim.created_at)}</dd></div>
                {selectedClaim.data.claim.updated_at ? (
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedClaim.data.claim.updated_at)}</dd></div>
                ) : null}
              </dl>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Related Disputes</div>
              {selectedClaim.data.open_dispute_ids.length || selectedClaim.data.resolved_dispute_ids.length ? (
                <div className="mt-2 flex flex-wrap gap-1">
                  {[...selectedClaim.data.open_dispute_ids, ...selectedClaim.data.resolved_dispute_ids].map((disputeId) => (
                    <button
                      key={disputeId}
                      type="button"
                      onClick={() => openDispute(disputeId)}
                      className={chipLinkClass}
                    >
                      {truncateMiddle(disputeId)}
                    </button>
                  ))}
                </div>
              ) : (
                <div className="mt-2 text-xs text-slate-500">No disputes</div>
              )}
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Scope</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedClaim.data.claim.scope}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Statement</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedClaim.data.claim.statement}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Evidence Summary</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedClaim.data.claim.evidence_summary}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Source IDs</div>
              <DetailTextBlock className="mt-2">{JSON.stringify(selectedClaim.data.claim.source_claim_ids, null, 2)}</DetailTextBlock>
            </div>
          </>
        ) : selectedDispute ? (
          <>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Overview</div>
              <dl className="mt-2 space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge tone={selectedDispute.status === 'open' ? 'danger' : 'success'}>{selectedDispute.status}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Severity</dt><dd><StatusBadge tone={severityTone(selectedDispute)}>{severityFromDispute(selectedDispute)}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.created_at)}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.resolved_at ?? selectedDispute.created_at)}</dd></div>
              </dl>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Direct Claim(s)</div>
              <div className="mt-2 flex flex-wrap gap-1">
                {selectedDispute.claims.map((claimId) => (
                  <button
                    key={claimId}
                    type="button"
                    onClick={() => openClaim(claimId)}
                    className={chipLinkClass}
                  >
                    {truncateMiddle(claimId)}
                  </button>
                ))}
              </div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Summary</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedDispute.summary}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Source IDs</div>
              <DetailTextBlock className="mt-2">{JSON.stringify(selectedDispute.claims, null, 2)}</DetailTextBlock>
            </div>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
