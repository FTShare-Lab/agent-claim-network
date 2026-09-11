import { useMemo, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router'

import { ClaimDetails } from '../features/claims/ClaimDetails'
import { matchesClaimFilters } from '../features/claims/filter'
import { ClaimFilters } from '../features/claims/ClaimFilters'
import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { DrawerSection } from '../components/drawer/DrawerSection'
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
  const navigate = useNavigate()
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
        return matchesClaimFilters(row, { keyword, agent, status, scope, onlyDisputed })
      }),
    )
  }, [agent, data, keyword, onlyDisputed, scope, status])

  const pagedRows = paginate(filtered, page, pageSize)

  function openClaim(claimId: string) {
    setDrawerState({ current: { type: 'claim', id: claimId } })
  }

  function openSource(sourceId: string) {
    if (sourceId.startsWith('policy_')) {
      navigate(`/policies?policy_id=${encodeURIComponent(sourceId)}`)
      return
    }

    setDrawerState((current) => {
      const currentState = current ?? effectiveDrawerState
      if (currentState?.current.type === 'claim' && currentState.current.id === sourceId) {
        return currentState
      }
      return {
        current: { type: 'claim', id: sourceId },
        previous: currentState?.current,
      }
    })
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
      <ClaimFilters
        filters={{ keyword, agent, status, scope, onlyDisputed }}
        agents={agents}
        onChange={(patch) => {
          if (patch.keyword !== undefined) setKeyword(patch.keyword)
          if (patch.agent !== undefined) setAgent(patch.agent)
          if (patch.status !== undefined) setStatus(patch.status)
          if (patch.scope !== undefined) setScope(patch.scope)
          setPage(1)
        }}
      />

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
        footer={selectedClaim.data ? (
          <button type="button" className="text-sm font-semibold text-blue-700" onClick={() => navigate(`/knowledge-tree?root_id=${encodeURIComponent(selectedClaim.data.claim.id)}`)}>View claims flow</button>
        ) : undefined}
        modal={false}
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
          <ClaimDetails view={selectedClaim.data} onOpenSource={openSource} onOpenDispute={openDispute} />
        ) : selectedDispute ? (
          <>
            <DrawerSection title="Overview">
              <dl className="space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge tone={selectedDispute.status === 'open' ? 'danger' : 'success'}>{selectedDispute.status}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Severity</dt><dd><StatusBadge tone={severityTone(selectedDispute)}>{severityFromDispute(selectedDispute)}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.created_at)}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.resolved_at ?? selectedDispute.created_at)}</dd></div>
              </dl>
            </DrawerSection>
            <DrawerSection title="Direct Claim(s)" tone="claim">
              <div className="flex flex-wrap gap-1">
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
            </DrawerSection>
            <DrawerSection title="Summary">
              <div className="whitespace-pre-wrap text-sm leading-6 text-slate-800">{selectedDispute.summary}</div>
            </DrawerSection>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
