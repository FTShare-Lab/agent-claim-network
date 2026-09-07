// 仅对本轮实际生成通知的 Claim 展示建议与当前 mirror；不保存历史快照。
import { useQuery } from '@tanstack/react-query'
import { RefreshCw } from 'lucide-react'
import { Link } from 'react-router'

import { StatusBadge } from '../../components/badges/StatusBadge'
import { formatDateTime } from '../../lib/format'
import { getSweepComparison } from './api'
import type { SweepRunRecord } from './types'

function updateComparison(baseline: string | undefined, current: string | undefined) {
  if (!baseline || !current) return 'Update comparison unavailable'
  const before = Date.parse(baseline)
  const after = Date.parse(current)
  if (!Number.isFinite(before) || !Number.isFinite(after)) return 'Update comparison unavailable'
  if (after > before) return 'Updated since notification'
  if (after < before) return 'Current update time is earlier'
  return 'Same update time'
}

export function SweepClaimComparison({ run }: { run: SweepRunRecord }) {
  const notifications = run.report.notifications
  const query = useQuery({
    queryKey: ['sweeps', 'comparison', run.run_id],
    queryFn: getSweepComparison,
    enabled: notifications.length > 0,
  })

  if (!notifications.length) {
    return <p className="text-xs text-slate-500">This sweep generated no notifications. There are no notified Claims to compare.</p>
  }

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <p className="max-w-xl text-xs leading-5 text-slate-500">
          Only Claims in this sweep’s generated notifications are shown. Current values come from the latest fetched mirror. Matching values do not imply acceptance; ACK confirms delivery only.
        </p>
        <button
          type="button"
          disabled={query.isFetching}
          onClick={() => void query.refetch()}
          aria-busy={query.isFetching}
          className="inline-flex shrink-0 items-center gap-2 rounded-md bg-blue-700 px-3 py-2 text-sm font-semibold text-white transition hover:bg-blue-800 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-700 disabled:cursor-wait disabled:bg-blue-700/70"
        >
          <RefreshCw aria-hidden="true" className={`h-4 w-4 ${query.isFetching ? 'motion-safe:animate-spin' : ''}`} />
          {query.isFetching ? 'Refreshing…' : 'Refresh current data'}
        </button>
      </div>
      {query.isError ? (
        <p role="alert" className="text-xs text-rose-700">Unable to load the comparison. Refresh to retry.</p>
      ) : query.isPending ? (
        <p role="status" className="text-xs text-slate-500">Loading current Claims and delivery confirmations…</p>
      ) : (
        <>
          <p className="text-[11px] text-slate-500">Fetched at {formatDateTime(new Date(query.dataUpdatedAt).toISOString())}</p>
          {notifications.map((notification) => {
            const entry = query.data.outbox.find((item) =>
              item.target_kind === 'targeted' && item.target_agent === notification.agent_id
              && item.inbox_message.policy.id === notification.policy_id,
            )
            const ack = entry?.delivered_to.find((mark) => mark.agent_id === notification.agent_id)
            const suggestions = [
              ...notification.stale_claims.map((id) => ({ id, status: 'stale' as const })),
              ...notification.deprecated_claims.map((id) => ({ id, status: 'deprecated' as const })),
            ]
            return (
              <section key={`${notification.agent_id}-${notification.policy_id}`} className="overflow-hidden rounded-lg border border-slate-200">
                <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 bg-slate-50 px-3 py-2">
                  <span className="break-all font-mono text-xs font-medium text-slate-900">{notification.agent_id}</span>
                  <div className="flex flex-wrap items-center gap-2">
                    <StatusBadge tone={ack ? 'success' : 'neutral'}>{ack ? 'ACK received' : entry ? 'Awaiting ACK' : 'Delivery unavailable'}</StatusBadge>
                    {ack ? <span className="text-[11px] text-slate-500">{formatDateTime(ack.sent_at)}</span> : null}
                  </div>
                </div>
                <div className="divide-y divide-slate-100">
                  {suggestions.map(({ id, status }) => {
                    const item = entry?.sweep_items?.find((item) => item.claim_id === id && item.suggested_status === status)
                    const current = query.data.claims.find(({ claim }) => claim.id === id && claim.holder === notification.agent_id)?.claim
                    const currentTime = current ? current.updated_at ?? current.created_at : undefined
                    return (
                      <article key={`${id}-${status}`} className="space-y-2 p-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <Link to={`/claims?claim_id=${encodeURIComponent(id)}`} className="max-w-full break-all rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 font-mono text-[11px] font-medium text-blue-700 underline-offset-2 transition hover:text-blue-900 hover:underline">
                            {id}
                          </Link>
                          {current ? <StatusBadge>{current.status}</StatusBadge> : null}
                        </div>
                        <div className="grid gap-2 sm:grid-cols-2">
                          <div className="rounded-lg bg-slate-50 p-2.5">
                            <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500">Sweep suggestion</div>
                            <StatusBadge>{status}</StatusBadge>
                            <p className="mt-2 text-[11px] text-slate-500">Claim update at notification</p>
                            <p className="mt-0.5 text-xs text-slate-700">{item ? formatDateTime(item.effective_updated_at) : 'Not recorded'}</p>
                          </div>
                          <div className="rounded-lg bg-slate-50 p-2.5">
                            <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500">Current mirror</div>
                            {current ? <StatusBadge>{current.status}</StatusBadge> : <span className="text-xs text-slate-500">Claim unavailable</span>}
                            <p className="mt-2 text-[11px] text-slate-500">Latest Claim update</p>
                            <p className="mt-0.5 text-xs text-slate-700">{currentTime ? formatDateTime(currentTime) : 'Unavailable'}</p>
                          </div>
                        </div>
                        <div className="flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-slate-600">
                          <span>{current ? current.status === status ? 'Status matches suggestion' : 'Status differs from suggestion' : 'Status comparison unavailable'}</span>
                          <span>{updateComparison(item?.effective_updated_at, currentTime)}</span>
                        </div>
                      </article>
                    )
                  })}
                </div>
              </section>
            )
          })}
        </>
      )}
    </div>
  )
}
