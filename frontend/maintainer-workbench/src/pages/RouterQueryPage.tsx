import { useMemo, useState } from 'react'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { PageContainer } from '../layouts/PageContainer'
import { useRouterQueryMutation } from '../features/router-query/hooks'
import type { CandidateClaim, DisputeRef } from '../features/router-query/types'
import { formatDateTime } from '../lib/format'
import { isStaticDemo } from '../lib/runtime'

export function RouterQueryPage() {
  const queryMutation = useRouterQueryMutation()
  const [scope, setScope] = useState(() => isStaticDemo ? 'coordination/router' : '')
  const [semanticQuery, setSemanticQuery] = useState(() =>
    isStaticDemo ? 'Router 候选应该如何影响当前任务中的本地判断？' : '',
  )
  const [selected, setSelected] = useState<{ kind: 'claim' | 'dispute'; id: string } | null>(null)

  const candidateClaim = useMemo(
    () => queryMutation.data?.candidate_claims.find((item) => item.id === selected?.id) ?? null,
    [queryMutation.data?.candidate_claims, selected?.id],
  )
  const retrievalDebugCandidate = useMemo(
    () => queryMutation.data?.retrieval_debug?.candidates.find((item) => item.claim_id === candidateClaim?.id) ?? null,
    [candidateClaim?.id, queryMutation.data?.retrieval_debug?.candidates],
  )
  const dispute = useMemo(
    () => queryMutation.data?.disputes.find((item) => item.id === selected?.id) ?? null,
    [queryMutation.data?.disputes, selected?.id],
  )

  return (
    <PageContainer title="Router Query" subtitle="Search claims and disputes across your coordination network using semantic retrieval.">
      <section className="grid gap-3 xl:grid-cols-[1.4fr_1fr]">
        <div className="space-y-3">
          <section className="rounded-lg border border-slate-200 bg-white p-3">
            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Router Input</div>
            <h2 className="mt-1 text-sm font-semibold tracking-tight text-slate-900">Run Query</h2>
            <p className="mt-1 text-xs text-slate-600">Enter a scope string and optional semantic query to search related claims and disputes.</p>
            <div className="mt-3 space-y-2">
              <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-slate-400" placeholder="order-system / batch-order-submit" value={scope} onChange={(event) => setScope(event.target.value)} />
              <textarea className="min-h-24 w-full resize-y rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-slate-400" placeholder="Optional semantic query" value={semanticQuery} onChange={(event) => setSemanticQuery(event.target.value)} />
            </div>
            <button
              type="button"
              className="mt-3 rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-blue-800 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200"
              disabled={!scope || queryMutation.isPending}
              onClick={() => queryMutation.mutate({ scope, semantic_query: semanticQuery || undefined })}
            >
              {queryMutation.isPending ? 'Running semantic retrieval…' : 'Run Query'}
            </button>
          </section>

          <div className="grid gap-3 xl:grid-cols-2">
            <section className="space-y-2">
              <div className="flex items-center gap-2">
                <h2 className="text-sm font-semibold tracking-tight text-slate-900">Candidate Claims</h2>
                <StatusBadge tone="info">{queryMutation.data?.candidate_claims.length ?? 0}</StatusBadge>
              </div>
              <DataTable
                columns={[
                  {
                    key: 'claim',
                    header: 'Claim',
                    render: (row: CandidateClaim) => (
                      <div>
                        <div className="font-medium text-slate-900">{row.name}</div>
                        <div className="mt-0.5 font-mono text-[11px] text-slate-500">{row.id}</div>
                      </div>
                    ),
                  },
                  { key: 'agent', header: 'Agent', render: (row: CandidateClaim) => <span className="font-mono text-xs">{row.holder}</span> },
                  { key: 'updated', header: 'Updated At', render: (row: CandidateClaim) => <span className="font-mono text-xs">{formatDateTime(row.created_at)}</span> },
                ]}
                rows={queryMutation.data?.candidate_claims ?? []}
                getRowId={(row) => row.id}
                onRowClick={(row) => setSelected({ kind: 'claim', id: row.id })}
                emptyState="Run a router query to inspect candidate claims."
              />
            </section>

            <section className="space-y-2">
              <div className="flex items-center gap-2">
                <h2 className="text-sm font-semibold tracking-tight text-slate-900">Related Disputes</h2>
                <StatusBadge tone="info">{queryMutation.data?.disputes.length ?? 0}</StatusBadge>
              </div>
              <DataTable
                columns={[
                  { key: 'dispute', header: 'Dispute', render: (row: DisputeRef) => row.name },
                  { key: 'status', header: 'Status', render: (row: DisputeRef) => <StatusBadge>{row.status}</StatusBadge> },
                  { key: 'claims', header: 'Claims', render: (row: DisputeRef) => <span className="font-mono text-xs">{row.claim_ids.length}</span> },
                ]}
                rows={queryMutation.data?.disputes ?? []}
                getRowId={(row) => row.id}
                onRowClick={(row) => setSelected({ kind: 'dispute', id: row.id })}
                emptyState="Dispute results appear here when the router includes related disputes."
              />
            </section>
          </div>
        </div>

        <section className="rounded-lg border border-slate-200 bg-white p-3">
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Query Summary</div>
          <h2 className="mt-1 text-sm font-semibold tracking-tight text-slate-900">Execution</h2>
          {queryMutation.data?.retrieval_debug ? (
            <div className="mt-3 space-y-1.5 text-xs text-slate-600">
              <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Mode</span> <span className="font-mono text-slate-900">{queryMutation.data.retrieval_debug.mode}</span></div>
              <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Lexical Hits</span> <span className="font-mono text-slate-900">{queryMutation.data.retrieval_debug.lexical_hits}</span></div>
              <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Vector Hits</span> <span className="font-mono text-slate-900">{queryMutation.data.retrieval_debug.vector_hits}</span></div>
              <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Rerank Fallback</span> <span className="font-mono text-slate-900">{queryMutation.data.retrieval_debug.rerank_fallback ? 'yes' : 'no'}</span></div>
              <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Candidates</span> <span className="font-mono text-slate-900">{queryMutation.data.retrieval_debug.candidates.length}</span></div>
              {queryMutation.data.retrieval_debug.failed_paths.length ? (
                <div className="rounded border border-amber-200 bg-amber-50 px-2.5 py-1.5"><span className="text-amber-700">Failed Paths</span> <span className="font-mono text-amber-900">{queryMutation.data.retrieval_debug.failed_paths.join(', ')}</span></div>
              ) : null}
              {queryMutation.data.retrieval_debug.error_summaries.length ? (
                <div className="rounded border border-rose-200 bg-rose-50 px-2.5 py-1.5"><span className="text-rose-700">Errors</span> <span className="font-mono text-rose-900">{queryMutation.data.retrieval_debug.error_summaries.join('; ')}</span></div>
              ) : null}
            </div>
          ) : (
            <p className="mt-3 text-xs leading-5 text-slate-600">Run a query to inspect retrieval metadata, candidate claims, and related disputes.</p>
          )}
        </section>
      </section>

      <DetailDrawer
        modal={false}
        size="default"
        open={Boolean(selected && (candidateClaim || dispute))}
        onClose={() => setSelected(null)}
        label={selected?.kind === 'claim' ? 'Claim' : 'Dispute'}
        title={candidateClaim?.name ?? dispute?.name ?? 'Query Detail'}
        subtitle={candidateClaim?.id ?? dispute?.id}
      >
        {candidateClaim ? (
          <>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Claim Summary</div>
              <div className="mt-2 space-y-1.5 text-xs text-slate-600">
                <div><span className="text-slate-500">Holder</span> <span className="font-mono text-slate-900">{candidateClaim.holder}</span></div>
                <div><span className="text-slate-500">Scope</span> <span className="text-slate-900">{candidateClaim.scope}</span></div>
                <div><span className="text-slate-500">Status</span> <span className="text-slate-900">{candidateClaim.status}</span></div>
              </div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Statement</div>
              <div className="mt-2 text-xs leading-5 text-slate-600">{candidateClaim.statement}</div>
            </div>
            {retrievalDebugCandidate ? (
              <div className="rounded-lg border border-slate-200 p-3">
                <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Retrieval Debug</div>
                <div className="mt-2 grid grid-cols-1 gap-1.5 text-xs text-slate-600">
                  <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Hit Sources</span> <span className="font-mono text-slate-900">{retrievalDebugCandidate.hit_sources}</span></div>
                  <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Vector Status</span> <span className="font-mono text-slate-900">{retrievalDebugCandidate.vector_status}</span></div>
                  <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Lexical</span> <span className="font-mono text-slate-900">{retrievalDebugCandidate.lexical_score}</span></div>
                  <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Vector</span> <span className="font-mono text-slate-900">{retrievalDebugCandidate.vector_score}</span></div>
                  <div className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5"><span className="text-slate-500">Rank</span> <span className="font-mono text-slate-900">{retrievalDebugCandidate.rank_before_rerank} → {retrievalDebugCandidate.rank_after_rerank}</span></div>
                </div>
              </div>
            ) : null}
          </>
        ) : null}
        {dispute ? (
          <div className="rounded-lg border border-slate-200 p-3">
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Dispute Summary</div>
            <div className="mt-2 text-xs leading-5 text-slate-600">{dispute.summary}</div>
          </div>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
