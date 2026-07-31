import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { DetailTextBlock } from '../components/drawer/DetailTextBlock'
import { FilterBar } from '../components/filters/FilterBar'
import { PaginationBar } from '../components/pagination/PaginationBar'
import { PageContainer } from '../layouts/PageContainer'
import type { ClaimView } from '../features/claims/types'
import { useClaimsQuery } from '../features/claims/hooks'
import { useDisputesQuery, useResolveDisputeMutation } from '../features/disputes/hooks'
import type { Dispute } from '../features/disputes/types'
import { formatDateTime, truncateMiddle } from '../lib/format'
import { isStaticDemo } from '../lib/runtime'

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

function drawerResourceFromRouteState(state: unknown): DrawerResource | null {
  if (!state || typeof state !== 'object') return null
  const { disputeId, claimId } = state as { disputeId?: unknown; claimId?: unknown }
  if (typeof disputeId === 'string' && disputeId) {
    return { type: 'dispute', id: disputeId }
  }
  if (typeof claimId === 'string' && claimId) {
    return { type: 'claim', id: claimId }
  }
  return null
}

function drawerResourceKey(resource: DrawerResource | null) {
  return resource ? `${resource.type}:${resource.id}` : null
}

function relatedClaimPayload(dispute: Dispute | null, claims: ClaimView[], claimsLoading: boolean) {
  if (!dispute) return []
  return dispute.claims.map((claimId) => {
    const view = claims.find((item) => item.claim.id === claimId)
    if (!view && claimsLoading) return { id: claimId, loading: true }
    if (!view) return { id: claimId, missing: true }
    return view.claim
  })
}

function affectedAgentsForDispute(dispute: Dispute | null, claims: ClaimView[]) {
  if (!dispute) return []
  const claimIds = new Set(dispute.claims)
  const agents = claims
    .filter((item) => claimIds.has(item.claim.id))
    .map((item) => item.claim.holder)
  return Array.from(new Set(agents)).sort((left, right) => left.localeCompare(right))
}

const chipLinkClass = "rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 font-mono text-[11px] font-medium text-blue-700 transition hover:bg-blue-100"

export function DisputesPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const routeDrawerResource = drawerResourceFromRouteState(location.state)
  const routeDrawerResourceKey = drawerResourceKey(routeDrawerResource)
  const { data: disputes = [], isLoading, error } = useDisputesQuery()
  const { data: claims = [], isLoading: claimsLoading } = useClaimsQuery()
  const resolveMutation = useResolveDisputeMutation()
  const claimNameMap = useMemo(() => new Map(claims.map((claim) => [claim.claim.id, claim.claim.name])), [claims])
  const [keyword, setKeyword] = useState('')
  const [status, setStatus] = useState('all')
  const [severity, setSeverity] = useState('all')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [drawerState, setDrawerState] = useState<DrawerState>(() =>
    routeDrawerResource ? { current: routeDrawerResource } : null,
  )
  const [resolveNote, setResolveNote] = useState('')
  const [notifyAffectedAgents, setNotifyAffectedAgents] = useState(true)
  const [resolveError, setResolveError] = useState<string | null>(null)

  useEffect(() => {
    if (!routeDrawerResourceKey) return
    navigate(location.pathname + location.search, { replace: true, state: null })
  }, [location.pathname, location.search, navigate, routeDrawerResourceKey])

  const filtered = useMemo(
    () =>
      disputes.filter((dispute) => {
        const text = `${dispute.id} ${dispute.name} ${dispute.summary}`.toLowerCase()
        if (keyword && !text.includes(keyword.toLowerCase())) return false
        if (status !== 'all' && dispute.status !== status) return false
        if (severity !== 'all' && severityFromDispute(dispute) !== severity) return false
        return true
      }),
    [disputes, keyword, severity, status],
  )

  const selectedDispute =
    drawerState?.current.type === 'dispute'
      ? filtered.find((item) => item.id === drawerState.current.id) ?? disputes.find((item) => item.id === drawerState.current.id) ?? null
      : null
  const selectedClaim = drawerState?.current.type === 'claim' ? claims.find((item) => item.claim.id === drawerState.current.id) ?? null : null
  const relatedClaimsJson = useMemo(
    () => JSON.stringify(relatedClaimPayload(selectedDispute, claims, claimsLoading), null, 2),
    [claims, claimsLoading, selectedDispute],
  )
  const affectedAgentIds = useMemo(
    () => affectedAgentsForDispute(selectedDispute, claims),
    [claims, selectedDispute],
  )
  const pagedRows = paginate(filtered, page, pageSize)

  function resetResolveForm() {
    setResolveNote('')
    setNotifyAffectedAgents(false)
    setResolveError(null)
  }

  function openDispute(disputeId: string) {
    const trackedDisputeId =
      drawerState?.current.type === 'dispute'
        ? drawerState.current.id
        : drawerState?.previous?.type === 'dispute'
          ? drawerState.previous.id
          : null
    if (trackedDisputeId !== disputeId) {
      resetResolveForm()
    }
    setDrawerState({ current: { type: 'dispute', id: disputeId } })
  }

  function closeDrawer() {
    setDrawerState(null)
    resetResolveForm()
  }

  function openClaim(claimId: string) {
    setDrawerState((current) => {
      if (!current) {
        return { current: { type: 'claim', id: claimId } }
      }

      if (current.current.type === 'claim' && current.current.id === claimId) {
        return current
      }

      return {
        current: { type: 'claim', id: claimId },
        previous: current.current,
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

  function resolveSelectedDispute() {
    if (!selectedDispute || selectedDispute.status !== 'open') return
    const trimmed = resolveNote.trim()
    setResolveError(null)
    if (!trimmed) {
      setResolveError('Resolve Note 不能为空')
      return
    }
    resolveMutation.mutate({
      id: selectedDispute.id,
      request: {
        resolve_note: trimmed,
        notify_affected_agents: notifyAffectedAgents,
      },
    }, {
      onError: (err) => {
        setResolveError(err instanceof Error ? err.message : String(err))
      },
    })
  }

  if (isLoading) return <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm text-slate-500">Loading disputes…</div>
  if (error) return <div className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">{String(error)}</div>

  return (
    <PageContainer title="Disputes" subtitle="Review, filter, and resolve coordination conflicts across your agent network.">
      <FilterBar>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Search</span>
          <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" placeholder="dispute id, name, summary" value={keyword} onChange={(event) => { setKeyword(event.target.value); setPage(1) }} />
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Status</span>
          <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={status} onChange={(event) => { setStatus(event.target.value); setPage(1) }}>
            <option value="all">All</option>
            <option value="open">Open</option>
            <option value="resolved">Resolved</option>
          </select>
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Severity</span>
          <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={severity} onChange={(event) => { setSeverity(event.target.value); setPage(1) }}>
            <option value="all">All</option>
            <option value="high">High</option>
            <option value="medium">Medium</option>
            <option value="low">Low</option>
          </select>
        </label>
        <div className="flex gap-2 self-end">
          <button type="button" className="rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50" onClick={() => { setKeyword(''); setStatus('all'); setSeverity('all'); setPage(1) }}>
            Reset
          </button>
          <button type="button" className="rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50" onClick={() => { setStatus('open'); setPage(1) }}>
            Open only
          </button>
        </div>
      </FilterBar>

      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold tracking-tight text-slate-900">All Disputes</h2>
          <StatusBadge tone="info">{filtered.length}</StatusBadge>
        </div>
        <DataTable
          columns={[
            {
              key: 'dispute',
              header: 'Dispute',
              render: (row: Dispute) => (
                <div>
                  <div className="font-medium text-slate-900">{row.name}</div>
                  <div className="mt-0.5 font-mono text-[11px] text-slate-500">{row.id}</div>
                </div>
              ),
            },
            { key: 'status', header: 'Status', render: (row: Dispute) => <StatusBadge tone={row.status === 'open' ? 'danger' : 'success'}>{row.status}</StatusBadge> },
            { key: 'severity', header: 'Severity', render: (row: Dispute) => <StatusBadge tone={severityTone(row)}>{severityFromDispute(row)}</StatusBadge> },
            {
              key: 'claims',
              header: 'Claim(s)',
              render: (row: Dispute) => (
                <div className="flex flex-wrap gap-1">
                  {row.claims.map((claimId) => (
                    <button
                      key={claimId}
                      type="button"
                      onClick={(event) => {
                        event.stopPropagation()
                        openClaim(claimId)
                      }}
                      className={chipLinkClass}
                    >
                      {truncateMiddle(claimNameMap.get(claimId) ?? claimId)}
                    </button>
                  ))}
                </div>
              ),
            },
            {
              key: 'agent',
              header: 'Agent',
              render: (row: Dispute) => <span className="font-mono text-xs">{row.reporter_agent_id}</span>,
            },
            { key: 'created', header: 'Created At', render: (row: Dispute) => <span className="font-mono text-xs">{formatDateTime(row.created_at)}</span> },
            { key: 'updated', header: 'Updated At', render: (row: Dispute) => <span className="font-mono text-xs">{formatDateTime(row.resolved_at ?? row.created_at)}</span> },
            { key: 'actions', header: 'Actions', render: (row: Dispute) => <span className="text-xs font-medium text-blue-700">{row.status === 'open' ? 'View · Resolve' : 'View'}</span> },
          ]}
          rows={pagedRows}
          getRowId={(row) => row.id}
          onRowClick={(row) => openDispute(row.id)}
          emptyState="No disputes matched the current filters."
        />
        <PaginationBar page={page} pageSize={pageSize} total={filtered.length} onPageChange={setPage} onPageSizeChange={(size) => { setPageSize(size); setPage(1) }} />
      </section>

      <DetailDrawer
        open={Boolean(drawerState?.current && (selectedDispute || selectedClaim))}
        onClose={closeDrawer}
        onBack={drawerState?.previous ? goBack : undefined}
        backLabel={
          drawerState?.previous
            ? drawerState.previous.type === 'dispute'
              ? 'Back to dispute'
              : 'Back to claim'
            : undefined
        }
        label={selectedClaim ? 'Claim' : 'Dispute'}
        title={selectedClaim?.claim.name ?? selectedDispute?.name ?? 'Detail'}
        subtitle={selectedClaim?.claim.id ?? selectedDispute?.id}
        footer={
          selectedDispute ? (
            <div className="grid grid-cols-2 gap-2">
              <button type="button" className="rounded-md border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700" onClick={closeDrawer}>
                Close
              </button>
              <button
                type="button"
                className={
                  selectedDispute.status === 'open'
                    ? 'rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-blue-800 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200'
                    : 'rounded-md bg-slate-200 px-3 py-1.5 text-sm font-medium text-slate-600'
                }
                disabled={isStaticDemo || selectedDispute.status !== 'open' || resolveMutation.isPending}
                title={isStaticDemo ? 'Static preview is read-only' : undefined}
                onClick={resolveSelectedDispute}
              >
                {selectedDispute.status === 'open' && resolveMutation.isPending ? 'Resolving…' : 'Resolve Dispute'}
              </button>
            </div>
          ) : (
            <button type="button" className="w-full rounded-md border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700" onClick={closeDrawer}>
              Close
            </button>
          )
        }
      >
        {selectedDispute ? (
          <>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Overview</div>
              <dl className="mt-2 space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge tone={selectedDispute.status === 'open' ? 'danger' : 'success'}>{selectedDispute.status}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Severity</dt><dd><StatusBadge tone={severityTone(selectedDispute)}>{severityFromDispute(selectedDispute)}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Reporter Agent</dt><dd className="font-mono text-xs text-slate-900">{selectedDispute.reporter_agent_id}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.created_at)}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.resolved_at ?? selectedDispute.created_at)}</dd></div>
              </dl>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Related Claim(s)</div>
              <div className="mt-2 flex flex-wrap gap-1">
                {selectedDispute.claims.map((claimId) => (
                  <button
                    key={claimId}
                    type="button"
                    onClick={() => openClaim(claimId)}
                    className={chipLinkClass}
                  >
                    {claimNameMap.get(claimId) ?? claimId}
                  </button>
                ))}
              </div>
              <pre
                aria-label="Related claim details"
                tabIndex={0}
                className="mt-2 max-h-72 overflow-y-auto whitespace-pre-wrap break-words rounded-md border border-slate-200 bg-slate-50 px-2.5 py-2 font-mono text-[11px] leading-5 text-slate-700 outline-none focus:border-slate-400"
              >
                <code>{relatedClaimsJson}</code>
              </pre>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Summary</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedDispute.summary}</div>
            </div>
            {selectedDispute.status === 'open' ? (
              <div className="rounded-lg border border-slate-200 p-3">
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-500" htmlFor="resolve-note">
                  Resolve Note
                </label>
                <textarea
                  id="resolve-note"
                  className="mt-2 min-h-24 w-full resize-y rounded-md border border-slate-200 bg-white px-2.5 py-2 text-sm leading-5 text-slate-700 outline-none transition focus:border-slate-400"
                  placeholder="Write the maintainer resolution before closing this dispute."
                  value={resolveNote}
                  onChange={(event) => {
                    setResolveNote(event.target.value)
                    if (resolveError) setResolveError(null)
                  }}
                  disabled={isStaticDemo || resolveMutation.isPending}
                />
                <label className="mt-3 flex items-start gap-2 text-xs text-slate-700">
                  <input
                    type="checkbox"
                    className="mt-0.5 h-3.5 w-3.5 rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                    checked={notifyAffectedAgents}
                    onChange={(event) => setNotifyAffectedAgents(event.target.checked)}
                    disabled={isStaticDemo || resolveMutation.isPending}
                  />
                  <span>
                    <span className="font-medium text-slate-900">Notify Affected Agents</span>
                    <span className="block leading-5 text-slate-500">Send a ClaimAttributeUpdate to the holder agents of the related claims:</span>
                    <span aria-label="Affected agents" className="mt-1.5 flex flex-wrap gap-1">
                      {affectedAgentIds.length ? (
                        affectedAgentIds.map((agentId) => (
                          <span key={agentId} className="rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 font-mono text-[11px] font-medium text-slate-700">
                            {agentId}
                          </span>
                        ))
                      ) : (
                        <span className="text-[11px] text-slate-500">{claimsLoading ? 'Loading affected agents…' : 'No related claim holders found'}</span>
                      )}
                    </span>
                  </span>
                </label>
                {resolveError ? <p className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-2.5 py-1.5 text-xs text-rose-700" role="alert">{resolveError}</p> : null}
              </div>
            ) : null}
          </>
        ) : selectedClaim ? (
          <>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Overview</div>
              <dl className="mt-2 space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Holder Agent</dt><dd className="font-mono text-xs text-slate-900">{selectedClaim.claim.holder}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge>{selectedClaim.claim.status}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Confidence</dt><dd className="font-medium capitalize text-slate-900">{selectedClaim.claim.confidence}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedClaim.claim.created_at)}</dd></div>
              </dl>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Related Disputes</div>
              {selectedClaim.open_dispute_ids.length || selectedClaim.resolved_dispute_ids.length ? (
                <div className="mt-2 flex flex-wrap gap-1">
                  {[...selectedClaim.open_dispute_ids, ...selectedClaim.resolved_dispute_ids].map((disputeId) => (
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
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedClaim.claim.scope}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Statement</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedClaim.claim.statement}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Evidence Summary</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{selectedClaim.claim.evidence_summary}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Source IDs</div>
              <DetailTextBlock className="mt-2">{JSON.stringify(selectedClaim.claim.source_claim_ids, null, 2)}</DetailTextBlock>
            </div>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
