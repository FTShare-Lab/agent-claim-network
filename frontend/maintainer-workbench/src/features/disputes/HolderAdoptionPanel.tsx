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
  not_delivered: 'Awaiting delivery',
  delivered_unobserved: 'Delivered, awaiting Agent update',
  observed_converged: 'Agent update observed',
  observed_diverged: 'Agent update observed',
  unknown: 'Claim mirror unavailable',
}

const summaryCellClass = 'rounded border border-amber-900/15 bg-white/80 p-2'
const summaryLabelClass = 'text-amber-800'
const summaryValueClass = 'mt-0.5 text-base font-semibold text-amber-950'

function observationTone(state: HolderAdoption['observation_state']) {
  if (state === 'observed_converged' || state === 'observed_diverged') return 'success' as const
  if (state === 'delivered_unobserved') return 'warning' as const
  return 'neutral' as const
}

function changedFields(claim: ClaimAdoptionComparison) {
  const fields: string[] = []
  if (claim.snapshot_status && claim.current_status && claim.snapshot_status !== claim.current_status) fields.push('status')
  if (claim.snapshot_scope && claim.current_scope && claim.snapshot_scope !== claim.current_scope) fields.push('scope')
  if (claim.snapshot_statement && claim.current_statement && claim.snapshot_statement !== claim.current_statement) fields.push('statement')
  return fields
}

function ClaimComparison({ claim }: { claim: ClaimAdoptionComparison }) {
  const changes = changedFields(claim)
  const hasSnapshot = Boolean(claim.snapshot_status || claim.snapshot_scope || claim.snapshot_statement)
  return (
    <article aria-label={`Claim adoption ${claim.claim_id}`} className="rounded-lg border border-amber-200 bg-white p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="break-words text-xs font-semibold text-slate-900">{claim.claim_name ?? 'Claim'}</div>
          <div className="mt-0.5 break-all font-mono text-[11px] text-slate-500">{claim.claim_id}</div>
        </div>
        {!hasSnapshot ? (
          <StatusBadge tone="warning">Snapshot unavailable</StatusBadge>
        ) : claim.current_status ? (
          <StatusBadge tone={changes.length ? 'info' : 'neutral'}>
            {changes.length ? `Changed · ${changes.join(', ')}` : 'No visible field change'}
          </StatusBadge>
        ) : (
          <StatusBadge tone="warning">Mirror unavailable</StatusBadge>
        )}
      </div>

      <div className="mt-3 grid gap-2.5 text-xs sm:grid-cols-2">
        <div className="rounded-lg border border-slate-200 bg-slate-50/80 p-2.5">
          <div className="text-[11px] font-bold uppercase tracking-wide text-slate-600">Before · Resolution snapshot</div>
          <div className="mt-2">{claim.snapshot_status ? <StatusBadge>{claim.snapshot_status}</StatusBadge> : <span className="text-slate-500">Snapshot unavailable</span>}</div>
          <div className="mt-2 text-[11px] font-medium text-slate-500">Scope</div>
          <div className="mt-0.5 break-words leading-5 text-slate-800">{claim.snapshot_scope ?? 'Snapshot unavailable'}</div>
          <div className="mt-2 text-[11px] font-medium text-slate-500">Statement</div>
          <ExpandableText className="mt-0.5 block leading-5 text-slate-800" limit={130} emptyLabel="Snapshot unavailable">
            {claim.snapshot_statement}
          </ExpandableText>
        </div>
        <div className="rounded-lg border border-amber-200 bg-amber-50/70 p-2.5">
          <div className="text-[11px] font-bold uppercase tracking-wide text-amber-800">After · Current Agent Claim</div>
          <div className="mt-2">{claim.current_status ? <StatusBadge>{claim.current_status}</StatusBadge> : <span className="text-slate-500">Mirror unavailable</span>}</div>
          <div className="mt-2 text-[11px] font-medium text-amber-700">Scope</div>
          <div className="mt-0.5 break-words leading-5 text-slate-900">{claim.current_scope ?? 'Mirror unavailable'}</div>
          <div className="mt-2 text-[11px] font-medium text-amber-700">Statement</div>
          <ExpandableText className="mt-0.5 block leading-5 text-slate-900" limit={130} emptyLabel="Mirror unavailable">
            {claim.current_statement}
          </ExpandableText>
        </div>
      </div>
    </article>
  )
}

function HolderCard({ holder, observedAt }: { holder: HolderAdoption; observedAt?: string }) {
  return (
    <article aria-label={`Holder adoption ${holder.agent_id}`} className="rounded-md border border-amber-200 bg-amber-50/40 p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-xs font-semibold text-slate-900">{holder.agent_id}</div>
          <div className="mt-1 text-[11px] text-slate-500">
            {holder.claims.length} Claim snapshot{holder.claims.length === 1 ? '' : 's'} available
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

      {holder.reasons.length && !['observed_converged', 'observed_diverged'].includes(holder.observation_state) ? (
        <div className="mt-3 rounded bg-white p-2 text-xs leading-5 text-slate-700">
          {holder.reasons.map((reason) => (
            <ExpandableText key={reason} className="block" limit={200}>{reason}</ExpandableText>
          ))}
        </div>
      ) : null}

      <details className="mt-3 rounded-md border border-slate-200 bg-white p-2.5">
        <summary className="cursor-pointer text-xs font-medium text-amber-800">
          Before / after ({holder.claims.length})
        </summary>
        <div className="mt-2 space-y-2">
          {holder.claims.length
            ? holder.claims.map((claim) => <ClaimComparison key={claim.claim_id} claim={claim} />)
            : <div className="text-xs text-slate-500">No Claim snapshots are available.</div>}
        </div>
      </details>

      {holder.technical && Object.values(holder.technical).some(Boolean) ? (
        <details className="mt-2 rounded-md border border-slate-200 bg-white p-2.5 text-[11px]">
          <summary className="cursor-pointer font-medium text-slate-600">Technical details</summary>
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
  const observedUpdates = summary.converged + summary.diverged

  return (
    <section aria-label="Delivery and holder adoption" className="rounded-xl border border-amber-700 bg-amber-50/40 p-3.5 shadow-sm">
      <div className="flex items-center justify-between gap-3 border-b border-amber-900/20 pb-2.5">
        <div className="flex items-center gap-2 text-xs font-bold uppercase tracking-[0.08em] text-amber-950">
          <span aria-hidden="true" className="h-4 w-1 rounded-full bg-amber-600" />
          Delivery &amp; Holder Adoption
        </div>
        <button
          type="button"
          className="rounded-md border border-amber-700/40 bg-white px-2 py-1 text-xs font-semibold text-amber-950 hover:bg-amber-100/60"
          aria-expanded={expanded}
          onClick={() => setExpanded((current) => !current)}
        >
          {expanded ? 'Hide holder adoption' : 'Show holder adoption'}
        </button>
      </div>

      <dl className="mt-3 grid grid-cols-2 gap-2 text-xs sm:grid-cols-3">
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Notified</dt><dd className={summaryValueClass}>{summary.notified_holders}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Delivered</dt><dd className={summaryValueClass}>{summary.delivered}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Observed updates</dt><dd className={summaryValueClass}>{observedUpdates}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Awaiting update</dt><dd className={summaryValueClass}>{summary.unobserved}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Mirror unavailable</dt><dd className={summaryValueClass}>{summary.unknown}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Last observed</dt><dd className="mt-1 font-mono text-[11px] text-amber-950">{formatDateTime(adoption?.observed_at)}</dd></div>
      </dl>

      {expanded ? (
        <div className="mt-3 space-y-2">
          {adoption?.holders.length
            ? adoption.holders.map((holder) => (
              <HolderCard key={holder.agent_id} holder={holder} observedAt={adoption.observed_at} />
            ))
            : <div className="rounded border border-amber-200 bg-white p-3 text-xs text-slate-500">No holder delivery or adoption data is available.</div>}
        </div>
      ) : (
        <div className="mt-2 text-xs text-slate-600">
          {summary.notified_holders
            ? `${summary.notified_holders} holder${summary.notified_holders === 1 ? '' : 's'} tracked; expand to inspect how each Agent changed its Claims.`
            : 'No holder notification is associated with the current resolution.'}
        </div>
      )}
    </section>
  )
}
