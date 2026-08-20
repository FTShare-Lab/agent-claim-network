import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { DrawerSection } from '../components/drawer/DrawerSection'
import { FilterBar } from '../components/filters/FilterBar'
import { PaginationBar } from '../components/pagination/PaginationBar'
import { PageContainer } from '../layouts/PageContainer'
import type { Claim, ClaimView } from '../features/claims/types'
import { useClaimsQuery } from '../features/claims/hooks'
import { AnalysisCard } from '../features/disputes/AnalysisCard'
import { HolderAdoptionPanel } from '../features/disputes/HolderAdoptionPanel'
import {
  useAdoptAnalysisMutation,
  useAnalysesQuery,
  useAnalysisDetailQuery,
  useCreateAnalysisMutation,
  useDisputeDetailQuery,
  useDisputesQuery,
  useRejectResolutionMutation,
  useResolveDisputeMutation,
} from '../features/disputes/hooks'
import type {
  ArbitrationAnalysisSummary,
  Dispute,
  DisputeResolution,
  ResolutionBasis,
  ResolutionType,
} from '../features/disputes/types'
import { formatDateTime, truncateMiddle } from '../lib/format'
import { ApiError } from '../lib/apiClient'
import { isStaticDemo } from '../lib/runtime'
import { orderDisputesByStatusAndRecentChange } from '../lib/tableOrdering'

function paginate<T>(items: T[], page: number, pageSize: number) {
  const start = (page - 1) * pageSize
  return items.slice(start, start + pageSize)
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

function affectedAgentsForDispute(dispute: Dispute | null, claims: ClaimView[]) {
  if (!dispute) return []
  const claimIds = new Set(dispute.claims)
  const agents = claims
    .filter((item) => claimIds.has(item.claim.id))
    .map((item) => item.claim.holder)
  return Array.from(new Set(agents)).sort((left, right) => left.localeCompare(right))
}

const chipLinkClass = "rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 font-mono text-[11px] font-medium text-blue-700 transition hover:bg-blue-100"

function formatResolutionValue(value: string | undefined) {
  return value ? value.replaceAll('_', ' ') : 'Not specified'
}

function analyzeResultMessage(analysis: ArbitrationAnalysisSummary) {
  const label = `${analysis.analysis_id} · ${formatResolutionValue(analysis.state)}`
  if (analysis.state === 'failed') {
    const failure = analysis.error
      ? `${analysis.error.code}: ${analysis.error.message}`
      : 'Open the analysis to inspect the failure.'
    return { isError: true, text: `Analysis failed · ${label}. ${failure}` }
  }
  if (analysis.state === 'approved') {
    return {
      isError: false,
      text: analysis.adoptable
        ? `Analysis completed and can be adopted · ${label}.`
        : `Analysis completed · ${label}.`,
    }
  }
  if (analysis.state === 'unresolved') {
    return { isError: false, text: `Analysis completed as unresolved · ${label}.` }
  }
  return { isError: false, text: `Analysis started · ${label}.` }
}

function arbitrationMutationError(error: unknown, conflictMessage: string) {
  const detail = error instanceof Error ? error.message : String(error)
  if (error instanceof ApiError && error.status === 409) {
    return `${conflictMessage} Current dispute data has been refreshed. ${detail}`
  }
  return detail
}

function DirectClaimCard({ claim, onOpen }: { claim: Claim; onOpen: () => void }) {
  return (
    <article aria-label={claim.name} className="rounded-lg border border-blue-100 bg-white p-3 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <button
            type="button"
            className="break-words text-left text-sm font-semibold text-blue-700 hover:text-blue-900 hover:underline"
            onClick={onOpen}
          >
            {claim.name}
          </button>
          <div className="mt-0.5 break-all font-mono text-[11px] text-slate-500">{claim.id}</div>
        </div>
        <StatusBadge>{claim.status}</StatusBadge>
      </div>

      <dl className="mt-3 grid gap-2 text-xs sm:grid-cols-3">
        <div className="rounded bg-slate-50 px-2.5 py-2">
          <dt className="font-medium text-slate-500">Holder</dt>
          <dd className="mt-1 break-all font-mono text-[11px] text-slate-800">{claim.holder}</dd>
        </div>
        <div className="rounded bg-slate-50 px-2.5 py-2">
          <dt className="font-medium text-slate-500">Confidence</dt>
          <dd className="mt-1 capitalize text-slate-800">{claim.confidence}</dd>
        </div>
        <div className="rounded bg-slate-50 px-2.5 py-2 sm:col-span-1">
          <dt className="font-medium text-slate-500">Scope</dt>
          <dd className="mt-1 break-words text-slate-800">{claim.scope}</dd>
        </div>
      </dl>

      <div className="mt-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Statement</div>
        <p className="mt-1 whitespace-pre-wrap text-xs leading-5 text-slate-800">{claim.statement}</p>
      </div>
      <div className="mt-3 border-t border-slate-100 pt-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Evidence Summary</div>
        <p className="mt-1 whitespace-pre-wrap text-xs leading-5 text-slate-700">{claim.evidence_summary}</p>
      </div>
      <div className="mt-3 border-t border-slate-100 pt-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Source IDs</div>
        {claim.source_claim_ids.length ? (
          <div className="mt-1.5 flex flex-wrap gap-1">
            {claim.source_claim_ids.map((sourceId) => (
              <span key={sourceId} className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
                {sourceId}
              </span>
            ))}
          </div>
        ) : (
          <div className="mt-1 text-xs text-slate-500">No sources</div>
        )}
      </div>
    </article>
  )
}

function ResolutionDetail({ resolution, ariaLabel = 'Current resolution' }: { resolution: DisputeResolution; ariaLabel?: string }) {
  return (
    <section aria-label={ariaLabel} className="mt-2 space-y-3">
      <div className="flex flex-wrap gap-1.5">
        <StatusBadge tone={resolution.resolved_by === 'automatic' ? 'info' : 'success'}>
          {resolution.resolved_by}
        </StatusBadge>
        {resolution.resolution_type ? (
          <StatusBadge>{formatResolutionValue(resolution.resolution_type)}</StatusBadge>
        ) : null}
        {resolution.resolution_basis ? (
          <StatusBadge>{formatResolutionValue(resolution.resolution_basis)}</StatusBadge>
        ) : null}
      </div>

      <div className="rounded-md border border-blue-100 bg-blue-50/70 p-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-blue-700">
          Conclusion
        </div>
        <p className="mt-1.5 whitespace-pre-wrap text-sm leading-6 text-slate-800">
          {resolution.conclusion}
        </p>
      </div>

      <dl className="grid gap-2 text-xs sm:grid-cols-2">
        <div className="rounded-md bg-slate-50 p-2.5">
          <dt className="font-medium text-slate-500">Resolution ID</dt>
          <dd className="mt-1 break-all font-mono text-[11px] text-slate-800">
            {resolution.resolution_id}
          </dd>
        </div>
        <div className="rounded-md bg-slate-50 p-2.5">
          <dt className="font-medium text-slate-500">Resolved At</dt>
          <dd className="mt-1 font-mono text-[11px] text-slate-800">
            {formatDateTime(resolution.resolved_at)}
          </dd>
        </div>
        {resolution.rejection_reason ? (
          <div className="rounded-md border border-amber-100 bg-amber-50 p-2.5 sm:col-span-2">
            <dt className="font-medium text-amber-700">Replacement Reason</dt>
            <dd className="mt-1 whitespace-pre-wrap leading-5 text-amber-950">
              {resolution.rejection_reason}
            </dd>
          </div>
        ) : null}
      </dl>

      {resolution.claim_assessments?.length ? (
        <div className="rounded-lg border border-amber-200 bg-amber-50/60 p-3">
          <div className="text-[11px] font-bold uppercase tracking-wide text-amber-900">
            Resolution recommendations
          </div>
          <p className="mt-1 text-xs leading-5 text-amber-900">
            These are governance recommendations from the Resolution, not the Claims&apos; current status.
          </p>
          <div className="mt-2 space-y-2">
            {resolution.claim_assessments.map((assessment) => (
              <article
                key={assessment.claim_id}
                className="rounded-md border border-amber-100 bg-white p-2.5"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="break-all font-mono text-[11px] text-slate-700">
                    {assessment.claim_id}
                  </span>
                  <StatusBadge tone="warning">Recommended · {assessment.recommended_status}</StatusBadge>
                </div>
                <p className="mt-2 text-xs font-medium leading-5 text-slate-800">
                  {assessment.assessment}
                </p>
                <p className="mt-1 text-xs leading-5 text-slate-600">{assessment.reason}</p>
                {assessment.recommended_scope ? (
                  <div className="mt-2 rounded bg-slate-50 px-2 py-1.5 text-xs text-slate-700">
                    <span className="font-medium text-slate-500">Scope: </span>
                    {assessment.recommended_scope}
                  </div>
                ) : null}
                {assessment.recommended_statement ? (
                  <div className="mt-1.5 rounded bg-slate-50 px-2 py-1.5 text-xs leading-5 text-slate-700">
                    <span className="font-medium text-slate-500">Statement: </span>
                    {assessment.recommended_statement}
                  </div>
                ) : null}
              </article>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  )
}

export function DisputesPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const routeDrawerResource = drawerResourceFromRouteState(location.state)
  const routeDrawerResourceKey = drawerResourceKey(routeDrawerResource)
  const { data: disputes = [], isLoading, error } = useDisputesQuery()
  const { data: claims = [], isLoading: claimsLoading } = useClaimsQuery()
  const resolveMutation = useResolveDisputeMutation()
  const analyzeMutation = useCreateAnalysisMutation()
  const adoptMutation = useAdoptAnalysisMutation()
  const rejectMutation = useRejectResolutionMutation()
  const claimNameMap = useMemo(() => new Map(claims.map((claim) => [claim.claim.id, claim.claim.name])), [claims])
  const [keyword, setKeyword] = useState('')
  const [status, setStatus] = useState('all')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [drawerState, setDrawerState] = useState<DrawerState>(() =>
    routeDrawerResource ? { current: routeDrawerResource } : null,
  )
  const [resolveNote, setResolveNote] = useState('')
  const [notifyAffectedAgents, setNotifyAffectedAgents] = useState(true)
  const [resolveError, setResolveError] = useState<string | null>(null)
  const [rejectReason, setRejectReason] = useState('')
  const [replacementConclusion, setReplacementConclusion] = useState('')
  const [replacementResolutionType, setReplacementResolutionType] = useState<Exclude<ResolutionType, 'unresolved'> | ''>('')
  const [replacementResolutionBasis, setReplacementResolutionBasis] = useState<ResolutionBasis | ''>('')
  const [arbitrationError, setArbitrationError] = useState<string | null>(null)
  const [analyzeNotice, setAnalyzeNotice] = useState<{
    disputeId: string
    result: ArbitrationAnalysisSummary
  } | null>(null)
  const [adoptionNotice, setAdoptionNotice] = useState<string | null>(null)
  const [adoptingAnalysisId, setAdoptingAnalysisId] = useState<string>()

  useEffect(() => {
    if (!routeDrawerResourceKey) return
    navigate(location.pathname + location.search, { replace: true, state: null })
  }, [location.pathname, location.search, navigate, routeDrawerResourceKey])

  const filtered = useMemo(
    () =>
      orderDisputesByStatusAndRecentChange(
        disputes.filter((dispute) => {
          const text = `${dispute.id} ${dispute.name} ${dispute.summary}`.toLowerCase()
          if (keyword && !text.includes(keyword.toLowerCase())) return false
          if (status !== 'all' && dispute.status !== status) return false
          return true
        }),
      ),
    [disputes, keyword, status],
  )

  const selectedDisputeFromList =
    drawerState?.current.type === 'dispute'
      ? filtered.find((item) => item.id === drawerState.current.id) ?? disputes.find((item) => item.id === drawerState.current.id) ?? null
      : null
  const selectedDisputeId = selectedDisputeFromList?.id
  const disputeDetail = useDisputeDetailQuery(selectedDisputeId)
  const analyses = useAnalysesQuery(selectedDisputeId)
  const selectedDispute = disputeDetail.data ?? selectedDisputeFromList
  const currentAnalysisSummary = analyses.data
    ? analyses.data.current_analysis ?? undefined
    : disputeDetail.data?.current_analysis ?? undefined
  const currentAnalysisDetail = useAnalysisDetailQuery(
    selectedDisputeId,
    currentAnalysisSummary?.analysis_id,
  )
  const currentAnalysis = currentAnalysisDetail.data ?? currentAnalysisSummary
  const selectedClaim = drawerState?.current.type === 'claim' ? claims.find((item) => item.claim.id === drawerState.current.id) ?? null : null
  const selectedAnalyzeNotice = (() => {
    if (!analyzeNotice || analyzeNotice.disputeId !== selectedDispute?.id) return null
    const current = currentAnalysisSummary?.analysis_id === analyzeNotice.result.analysis_id
      ? currentAnalysisSummary
      : analyzeNotice.result
    return analyzeResultMessage(current)
  })()
  const affectedAgentIds = useMemo(
    () => affectedAgentsForDispute(selectedDispute, claims),
    [claims, selectedDispute],
  )
  const pagedRows = paginate(filtered, page, pageSize)

  function resetResolveForm() {
    setResolveNote('')
    setNotifyAffectedAgents(true)
    setResolveError(null)
    setRejectReason('')
    setReplacementConclusion('')
    setReplacementResolutionType('')
    setReplacementResolutionBasis('')
    setArbitrationError(null)
    setAnalyzeNotice(null)
    setAdoptionNotice(null)
    setAdoptingAnalysisId(undefined)
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

  function analyzeSelectedDispute() {
    if (!selectedDispute || selectedDispute.status !== 'open') return
    const disputeId = selectedDispute.id
    setArbitrationError(null)
    setAnalyzeNotice(null)
    setAdoptionNotice(null)
    analyzeMutation.mutate(disputeId, {
      onSuccess: (result) => setAnalyzeNotice({ disputeId, result }),
      onError: (err) => {
        setAnalyzeNotice(null)
        setArbitrationError(arbitrationMutationError(
          err,
          'Analysis could not be started because the dispute state changed.',
        ))
      },
    })
  }

  function adoptSelectedAnalysis(analysisId: string) {
    if (!selectedDispute || selectedDispute.status !== 'open') return
    setArbitrationError(null)
    setAdoptionNotice(null)
    setAdoptingAnalysisId(analysisId)
    adoptMutation.mutate({ id: selectedDispute.id, analysisId }, {
      onSuccess: (resolution) => {
        setAdoptingAnalysisId(undefined)
        setAnalyzeNotice(null)
        setAdoptionNotice(`Analysis adopted as resolution ${resolution.resolution_id}.`)
      },
      onError: (err) => {
        setAdoptingAnalysisId(undefined)
        setArbitrationError(arbitrationMutationError(
          err,
          'This analysis is no longer adoptable; run Analyze again if a fresh analysis is needed.',
        ))
      },
    })
  }

  function rejectSelectedResolution() {
    if (!selectedDispute?.resolution || selectedDispute.resolution.resolved_by !== 'automatic') return
    if (!rejectReason.trim() || !replacementConclusion.trim()) {
      setArbitrationError('Rejection reason and replacement conclusion are required.')
      return
    }
    setArbitrationError(null)
    rejectMutation.mutate({
      id: selectedDispute.id,
      request: {
        expected_resolution_id: selectedDispute.resolution.resolution_id,
        rejection_reason: rejectReason.trim(),
        conclusion: replacementConclusion.trim(),
        resolution_type: replacementResolutionType || undefined,
        resolution_basis: replacementResolutionBasis || undefined,
      },
    }, {
      onError: (err) => setArbitrationError(arbitrationMutationError(
        err,
        'The automatic resolution changed before it could be replaced; review the latest resolution.',
      )),
    })
  }

  const currentAnalysisContent = (
    <>
      {analyses.isLoading && !currentAnalysis ? <div className="text-xs text-slate-500">Loading current analysis…</div> : null}
      {analyses.error ? <div className="text-xs text-rose-700">{String(analyses.error)}</div> : null}
      {currentAnalysisDetail.error ? <div className="text-xs text-rose-700">{String(currentAnalysisDetail.error)}</div> : null}
      {currentAnalysis ? (
        <AnalysisCard
          analysis={currentAnalysis}
          label="Current analysis"
          onAdopt={selectedDispute?.status === 'open' ? adoptSelectedAnalysis : undefined}
          adopting={adoptMutation.isPending && adoptingAnalysisId === currentAnalysis.analysis_id}
          resolutionClosed={selectedDispute?.status === 'resolved'}
        />
      ) : (
        <div className="text-xs text-slate-500">No analysis is recorded for this dispute.</div>
      )}
    </>
  )

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
        <div className="flex gap-2 self-end">
          <button type="button" className="rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50" onClick={() => { setKeyword(''); setStatus('all'); setPage(1) }}>
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
            {
              key: 'claims',
              header: 'Direct Claim(s)',
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
            <div className="space-y-2">
              <div className="flex flex-wrap justify-end gap-2">
                <button type="button" className="rounded-md border border-slate-200 px-2.5 py-1.5 text-xs font-semibold text-slate-700 hover:bg-slate-50" onClick={closeDrawer}>
                  Close
                </button>
                {selectedDispute.status === 'open' ? (
                  <>
                    <button
                      type="button"
                      className="rounded-md border border-violet-300 bg-violet-50 px-2.5 py-1.5 text-xs font-semibold text-violet-800 hover:bg-violet-100 disabled:cursor-not-allowed disabled:border-slate-200 disabled:bg-white disabled:text-slate-400"
                      disabled={isStaticDemo || analyzeMutation.isPending}
                      onClick={analyzeSelectedDispute}
                    >
                      {analyzeMutation.isPending ? 'Starting…' : 'Analyze'}
                    </button>
                    <button
                      type="button"
                      className="rounded-md bg-blue-700 px-2.5 py-1.5 text-xs font-semibold text-white transition hover:bg-blue-800 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200"
                      disabled={isStaticDemo || resolveMutation.isPending}
                      title={isStaticDemo ? 'Static preview is read-only' : undefined}
                      onClick={resolveSelectedDispute}
                    >
                      {resolveMutation.isPending ? 'Resolving…' : 'Resolve Dispute'}
                    </button>
                  </>
                ) : null}
              </div>
              {selectedAnalyzeNotice ? (
                <p
                  className={
                    selectedAnalyzeNotice.isError
                      ? 'rounded-md border border-rose-200 bg-rose-50 px-2.5 py-1.5 text-xs leading-5 text-rose-700'
                      : 'rounded-md border border-blue-200 bg-blue-50 px-2.5 py-1.5 text-xs leading-5 text-blue-800'
                  }
                  role={selectedAnalyzeNotice.isError ? 'alert' : 'status'}
                >
                  {selectedAnalyzeNotice.text}
                </p>
              ) : null}
              {adoptionNotice ? (
                <p className="rounded-md border border-emerald-200 bg-emerald-50 px-2.5 py-1.5 text-xs leading-5 text-emerald-800" role="status">
                  {adoptionNotice}
                </p>
              ) : null}
            </div>
          ) : undefined
        }
      >
        {selectedDispute ? (
          <>
            <DrawerSection title="Overview">
              <dl className="space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge tone={selectedDispute.status === 'open' ? 'danger' : 'success'}>{selectedDispute.status}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Reporter Agent</dt><dd className="font-mono text-xs text-slate-900">{selectedDispute.reporter_agent_id}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.created_at)}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedDispute.resolved_at ?? selectedDispute.created_at)}</dd></div>
              </dl>
            </DrawerSection>
            <DrawerSection title="Direct Claim(s)" tone="claim">
              <div aria-label="Direct claim details" className="space-y-2">
                {selectedDispute.claims.map((claimId) => {
                  const directClaim = claims.find((item) => item.claim.id === claimId)?.claim
                  if (directClaim) {
                    return (
                      <DirectClaimCard
                        key={claimId}
                        claim={directClaim}
                        onOpen={() => openClaim(claimId)}
                      />
                    )
                  }
                  return (
                    <article key={claimId} aria-label={claimId} className="rounded-md border border-slate-200 bg-slate-50 p-3">
                      <div className="break-all font-mono text-[11px] text-slate-700">{claimId}</div>
                      <div className="mt-1.5 text-xs text-slate-500">
                        {claimsLoading ? 'Loading claim details…' : 'Claim mirror unavailable'}
                      </div>
                    </article>
                  )
                })}
              </div>
            </DrawerSection>
            <DrawerSection title="Summary">
              <div className="whitespace-pre-wrap text-sm leading-6 text-slate-800">{selectedDispute.summary}</div>
            </DrawerSection>
            {selectedDispute.resolution ? (
              <DrawerSection title="Current Resolution" tone="resolution">
                <ResolutionDetail resolution={selectedDispute.resolution} />
              </DrawerSection>
            ) : null}
            {selectedDispute.status === 'open' || currentAnalysis ? (
              <DrawerSection title="Current Analysis" tone="analysis">
                {selectedDispute.status === 'resolved' ? (
                  <details className="rounded-lg border border-violet-200 bg-white p-3">
                    <summary className="cursor-pointer text-xs font-semibold text-violet-800">
                      View analysis process
                    </summary>
                    <div className="mt-3">{currentAnalysisContent}</div>
                  </details>
                ) : currentAnalysisContent}
              </DrawerSection>
            ) : null}
            <HolderAdoptionPanel adoption={disputeDetail.data?.holder_adoption} />
            {selectedDispute.resolution?.resolved_by === 'automatic' ? (
              <div className="rounded-lg border border-amber-200 bg-amber-50 p-3">
                <div className="text-xs font-semibold uppercase tracking-wide text-amber-800">Reject &amp; Replace Automatic Resolution</div>
                <textarea className="mt-2 min-h-16 w-full rounded-md border border-amber-200 bg-white px-2.5 py-2 text-sm" placeholder="Why is the automatic resolution rejected?" value={rejectReason} onChange={(event) => setRejectReason(event.target.value)} />
                <textarea className="mt-2 min-h-20 w-full rounded-md border border-amber-200 bg-white px-2.5 py-2 text-sm" placeholder="Replacement conclusion" value={replacementConclusion} onChange={(event) => setReplacementConclusion(event.target.value)} />
                <div className="mt-2 grid gap-2 sm:grid-cols-2">
                  <label className="text-xs text-amber-900" htmlFor="replacement-resolution-type">
                    <span className="font-medium">Replacement Resolution Type</span>
                    <select id="replacement-resolution-type" className="mt-1 w-full rounded-md border border-amber-200 bg-white px-2.5 py-2 text-sm" value={replacementResolutionType} onChange={(event) => setReplacementResolutionType(event.target.value as Exclude<ResolutionType, 'unresolved'> | '')}>
                      <option value="">Unspecified</option>
                      <option value="coexist">Coexist</option>
                      <option value="lifecycle_update">Lifecycle update</option>
                      <option value="conflict_resolved">Conflict resolved</option>
                    </select>
                  </label>
                  <label className="text-xs text-amber-900" htmlFor="replacement-resolution-basis">
                    <span className="font-medium">Replacement Resolution Basis</span>
                    <select id="replacement-resolution-basis" className="mt-1 w-full rounded-md border border-amber-200 bg-white px-2.5 py-2 text-sm" value={replacementResolutionBasis} onChange={(event) => setReplacementResolutionBasis(event.target.value as ResolutionBasis | '')}>
                      <option value="">Unspecified</option>
                      <option value="direct_analysis">Direct analysis</option>
                      <option value="prior_resolution">Prior resolution</option>
                      <option value="policy">Policy</option>
                      <option value="evidence">Evidence</option>
                      <option value="insufficient_evidence">Insufficient evidence</option>
                    </select>
                  </label>
                </div>
                <button type="button" className="mt-2 rounded-md bg-amber-700 px-3 py-1.5 text-sm font-semibold text-white disabled:bg-slate-300" disabled={isStaticDemo || rejectMutation.isPending} onClick={rejectSelectedResolution}>
                  {rejectMutation.isPending ? 'Replacing…' : 'Reject & Replace'}
                </button>
              </div>
            ) : null}
            {arbitrationError ? <p className="rounded-md border border-rose-200 bg-rose-50 px-2.5 py-1.5 text-xs text-rose-700" role="alert">{arbitrationError}</p> : null}
            {selectedDispute.status === 'open' ? (
              <DrawerSection title="Resolve Note">
                <label className="sr-only" htmlFor="resolve-note">
                  Resolve Note
                </label>
                <textarea
                  id="resolve-note"
                  className="min-h-24 w-full resize-y rounded-md border border-slate-200 bg-white px-2.5 py-2 text-sm leading-5 text-slate-800 outline-none transition focus:border-blue-400 focus:ring-2 focus:ring-blue-100"
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
                    <span className="block leading-5 text-slate-500">Send a ClaimAttributeUpdate to the holder agents of the direct Claims:</span>
                    <span aria-label="Affected agents" className="mt-1.5 flex flex-wrap gap-1">
                      {affectedAgentIds.length ? (
                        affectedAgentIds.map((agentId) => (
                          <span key={agentId} className="rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 font-mono text-[11px] font-medium text-slate-700">
                            {agentId}
                          </span>
                        ))
                      ) : (
                        <span className="text-[11px] text-slate-500">{claimsLoading ? 'Loading affected agents…' : 'No direct Claim holders found'}</span>
                      )}
                    </span>
                  </span>
                </label>
                {resolveError ? <p className="mt-2 rounded-md border border-rose-200 bg-rose-50 px-2.5 py-1.5 text-xs text-rose-700" role="alert">{resolveError}</p> : null}
              </DrawerSection>
            ) : null}
          </>
        ) : selectedClaim ? (
          <>
            <DrawerSection title="Overview">
              <dl className="space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Holder Agent</dt><dd className="font-mono text-xs text-slate-900">{selectedClaim.claim.holder}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge>{selectedClaim.claim.status}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Confidence</dt><dd className="font-medium capitalize text-slate-900">{selectedClaim.claim.confidence}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedClaim.claim.created_at)}</dd></div>
              </dl>
            </DrawerSection>
            <DrawerSection title="Related Disputes">
              {selectedClaim.open_dispute_ids.length || selectedClaim.resolved_dispute_ids.length ? (
                <div className="flex flex-wrap gap-1">
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
                <div className="text-xs text-slate-500">No disputes</div>
              )}
            </DrawerSection>
            <DrawerSection title="Scope" tone="claim">
              <div className="text-sm leading-6 text-slate-900">{selectedClaim.claim.scope}</div>
            </DrawerSection>
            <DrawerSection title="Statement" tone="claim">
              <div className="whitespace-pre-wrap text-sm leading-6 text-slate-900">{selectedClaim.claim.statement}</div>
            </DrawerSection>
            <DrawerSection title="Evidence Summary" tone="claim">
              <div className="whitespace-pre-wrap text-sm leading-6 text-slate-800">{selectedClaim.claim.evidence_summary}</div>
            </DrawerSection>
            <DrawerSection title="Source IDs" tone="claim">
              {selectedClaim.claim.source_claim_ids.length ? (
                <div className="flex flex-wrap gap-1">
                  {selectedClaim.claim.source_claim_ids.map((sourceId) => (
                    <span key={sourceId} className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">
                      {sourceId}
                    </span>
                  ))}
                </div>
              ) : (
                <div className="text-xs text-slate-500">No sources</div>
              )}
            </DrawerSection>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
