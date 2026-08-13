import { useState } from 'react'

import { StatusBadge } from '../../components/badges/StatusBadge'
import { formatDateTime } from '../../lib/format'
import type {
  ClaimAdoptionComparison,
  HolderAdoption,
  HolderAdoptionView,
} from './types'
import { ExpandableText } from './ExpandableText'

const observationLabels: Record<HolderAdoption['observation_state'], string> = {
  not_delivered: '尚未送达',
  delivered_unobserved: '已送达，尚未观察到关联更新',
  observed_converged: '已观察到采纳',
  observed_diverged: '已更新但与建议不一致',
  unknown: '当前无法判断',
}

function observationTone(state: HolderAdoption['observation_state']) {
  if (state === 'observed_converged') return 'success' as const
  if (state === 'observed_diverged') return 'danger' as const
  if (state === 'delivered_unobserved') return 'warning' as const
  return 'neutral' as const
}

function ClaimComparison({ claim }: { claim: ClaimAdoptionComparison }) {
  return (
    <article aria-label={`Claim adoption ${claim.claim_id}`} className="rounded-md border border-slate-200 bg-white p-2.5">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="break-words text-xs font-semibold text-slate-900">{claim.claim_name ?? 'Claim'}</div>
          <div className="mt-0.5 break-all font-mono text-[11px] text-slate-500">{claim.claim_id}</div>
        </div>
        <StatusBadge tone={claim.matches ? 'success' : 'danger'}>{claim.matches ? 'Matched' : 'Mismatch'}</StatusBadge>
      </div>

      <dl className="mt-2 grid gap-2 text-[11px] sm:grid-cols-2">
        <div className="rounded bg-slate-50 p-2">
          <dt className="font-medium text-slate-500">Recommended status</dt>
          <dd className="mt-1"><StatusBadge>{claim.recommended_status}</StatusBadge></dd>
        </div>
        <div className="rounded bg-slate-50 p-2">
          <dt className="font-medium text-slate-500">Current status</dt>
          <dd className="mt-1">{claim.current_status ? <StatusBadge>{claim.current_status}</StatusBadge> : 'Mirror unavailable'}</dd>
        </div>
        <div className="rounded bg-slate-50 p-2">
          <dt className="font-medium text-slate-500">Recommended scope</dt>
          <dd className="mt-1 break-words text-slate-800">{claim.recommended_scope ?? 'Keep current scope'}</dd>
        </div>
        <div className="rounded bg-slate-50 p-2">
          <dt className="font-medium text-slate-500">Current scope</dt>
          <dd className="mt-1 break-words text-slate-800">{claim.current_scope ?? 'Mirror unavailable'}</dd>
        </div>
      </dl>

      <div className="mt-2 grid gap-2 text-xs sm:grid-cols-2">
        <div className="rounded border border-slate-100 p-2">
          <div className="text-[11px] font-medium text-slate-500">Recommended statement</div>
          <ExpandableText className="mt-1 block leading-5 text-slate-700" limit={130} emptyLabel="Keep current statement">
            {claim.recommended_statement}
          </ExpandableText>
        </div>
        <div className="rounded border border-slate-100 p-2">
          <div className="text-[11px] font-medium text-slate-500">Current statement</div>
          <ExpandableText className="mt-1 block leading-5 text-slate-700" limit={130} emptyLabel="Mirror unavailable">
            {claim.current_statement}
          </ExpandableText>
        </div>
      </div>

      <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px] text-slate-600">
        <span>Policy provenance</span>
        <StatusBadge tone={claim.policy_provenance_present ? 'success' : 'warning'}>
          {claim.policy_provenance_present ? 'Present' : 'Missing'}
        </StatusBadge>
      </div>
      {claim.mismatch_reasons.length ? (
        <div className="mt-2 rounded bg-amber-50 p-2 text-xs leading-5 text-amber-900">
          <div className="text-[11px] font-semibold uppercase tracking-wide text-amber-700">Mismatch reason</div>
          {claim.mismatch_reasons.map((reason) => (
            <ExpandableText key={reason} className="mt-1 block" limit={180}>{reason}</ExpandableText>
          ))}
        </div>
      ) : null}
    </article>
  )
}

function HolderCard({ holder, observedAt }: { holder: HolderAdoption; observedAt?: string }) {
  return (
    <article aria-label={`Holder adoption ${holder.agent_id}`} className="rounded-md border border-slate-200 bg-slate-50 p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-xs font-semibold text-slate-900">{holder.agent_id}</div>
          <div className="mt-1 text-[11px] text-slate-500">
            {holder.assessment_count} assessment{holder.assessment_count === 1 ? '' : 's'} · {holder.matched_count} matched
          </div>
        </div>
        <div className="flex flex-wrap justify-end gap-1.5">
          <StatusBadge tone={holder.delivery_state === 'delivered' ? 'success' : 'neutral'}>
            {holder.delivery_state === 'delivered' ? 'Delivered' : 'Not delivered'}
          </StatusBadge>
          <StatusBadge tone={observationTone(holder.observation_state)}>
            {observationLabels[holder.observation_state]}
          </StatusBadge>
        </div>
      </div>

      <dl className="mt-3 grid gap-2 text-[11px] sm:grid-cols-2">
        <div><dt className="text-slate-500">Last delivery</dt><dd className="mt-0.5 font-mono text-slate-800">{formatDateTime(holder.last_delivered_at)}</dd></div>
        <div><dt className="text-slate-500">Last observation</dt><dd className="mt-0.5 font-mono text-slate-800">{formatDateTime(holder.last_observed_at ?? observedAt)}</dd></div>
      </dl>

      {holder.reasons.length ? (
        <div className="mt-3 rounded bg-white p-2 text-xs leading-5 text-slate-700">
          {holder.reasons.map((reason) => (
            <ExpandableText key={reason} className="block" limit={200}>{reason}</ExpandableText>
          ))}
        </div>
      ) : (
        <div className="mt-3 text-xs text-slate-500">No additional observation reason.</div>
      )}

      <details className="mt-3 rounded-md border border-slate-200 bg-white p-2.5">
        <summary className="cursor-pointer text-xs font-medium text-blue-700">
          Claim comparison ({holder.claims.length})
        </summary>
        <div className="mt-2 space-y-2">
          {holder.claims.length
            ? holder.claims.map((claim) => <ClaimComparison key={claim.claim_id} claim={claim} />)
            : <div className="text-xs text-slate-500">No Claim comparison is available.</div>}
        </div>
      </details>

      {holder.technical && Object.values(holder.technical).some(Boolean) ? (
        <details className="mt-2 rounded-md border border-slate-200 bg-white p-2.5 text-[11px]">
          <summary className="cursor-pointer font-medium text-slate-600">技术详情</summary>
          <dl className="mt-2 space-y-1.5">
            {holder.technical.policy_id ? <div><dt className="inline text-slate-500">Policy ID: </dt><dd className="inline break-all font-mono">{holder.technical.policy_id}</dd></div> : null}
            {holder.technical.inbox_id ? <div><dt className="inline text-slate-500">Inbox ID: </dt><dd className="inline break-all font-mono">{holder.technical.inbox_id}</dd></div> : null}
            {holder.technical.snapshot_source ? <div><dt className="inline text-slate-500">Snapshot source: </dt><dd className="inline break-all">{holder.technical.snapshot_source}</dd></div> : null}
          </dl>
        </details>
      ) : null}
    </article>
  )
}

export function HolderAdoptionPanel({ adoption }: { adoption?: HolderAdoptionView | null }) {
  const [expanded, setExpanded] = useState(false)
  const summary = adoption?.summary ?? {
    notified_holders: 0,
    delivered: 0,
    converged: 0,
    diverged: 0,
    unobserved: 0,
    unknown: 0,
  }
  const unseen = summary.unobserved + summary.unknown

  return (
    <section aria-label="Delivery and holder adoption" className="rounded-lg border border-slate-200 p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Delivery &amp; Holder Adoption</div>
        <button
          type="button"
          className="rounded-md border border-slate-200 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
          aria-expanded={expanded}
          onClick={() => setExpanded((current) => !current)}
        >
          {expanded ? 'Hide holder adoption' : 'Show holder adoption'}
        </button>
      </div>

      <dl className="mt-3 grid grid-cols-2 gap-2 text-xs sm:grid-cols-3">
        <div className="rounded bg-slate-50 p-2"><dt className="text-slate-500">Notified</dt><dd className="mt-0.5 text-base font-semibold">{summary.notified_holders}</dd></div>
        <div className="rounded bg-slate-50 p-2"><dt className="text-slate-500">Delivered</dt><dd className="mt-0.5 text-base font-semibold">{summary.delivered}</dd></div>
        <div className="rounded bg-emerald-50 p-2"><dt className="text-emerald-700">Converged</dt><dd className="mt-0.5 text-base font-semibold text-emerald-800">{summary.converged}</dd></div>
        <div className="rounded bg-rose-50 p-2"><dt className="text-rose-700">Diverged</dt><dd className="mt-0.5 text-base font-semibold text-rose-800">{summary.diverged}</dd></div>
        <div className="rounded bg-amber-50 p-2"><dt className="text-amber-700">Unobserved / unknown</dt><dd className="mt-0.5 text-base font-semibold text-amber-800">{unseen}</dd></div>
        <div className="rounded bg-slate-50 p-2"><dt className="text-slate-500">Last observed</dt><dd className="mt-1 font-mono text-[11px]">{formatDateTime(adoption?.observed_at)}</dd></div>
      </dl>

      {expanded ? (
        <div className="mt-3 space-y-2">
          {adoption?.holders.length
            ? adoption.holders.map((holder) => (
              <HolderCard key={holder.agent_id} holder={holder} observedAt={adoption.observed_at} />
            ))
            : <div className="rounded bg-slate-50 p-3 text-xs text-slate-500">No holder delivery or adoption data is available.</div>}
        </div>
      ) : (
        <div className="mt-2 text-xs text-slate-500">
          {summary.notified_holders
            ? `${summary.notified_holders} holder${summary.notified_holders === 1 ? '' : 's'} tracked; expand to inspect delivery and Claim matching.`
            : 'No holder notification is associated with the current resolution.'}
        </div>
      )}
    </section>
  )
}
