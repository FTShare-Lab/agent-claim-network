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
  not_delivered: 'Not delivered',
  no_update_observed: 'No update observed',
  update_observed: 'Update observed',
  unknown: 'Mirror unavailable',
}

const summaryCellClass = 'rounded border border-amber-900/15 bg-white/80 p-2'
const summaryLabelClass = 'text-amber-800'
const summaryValueClass = 'mt-0.5 text-base font-semibold text-amber-950'

function observationTone(state: HolderAdoption['observation_state']) {
  if (state === 'update_observed') return 'success' as const
  return 'neutral' as const
}

function ClaimFields({
  label,
  status,
  scope,
  statement,
  tone = 'slate',
}: {
  label: string
  status?: ClaimAdoptionComparison['current_status']
  scope?: string | null
  statement?: string | null
  tone?: 'slate' | 'amber' | 'emerald'
}) {
  const styles = tone === 'amber'
    ? 'border-amber-200 bg-amber-50/70 text-amber-800'
    : tone === 'emerald'
      ? 'border-emerald-200 bg-emerald-50/60 text-emerald-800'
      : 'border-slate-200 bg-slate-50/80 text-slate-600'
  return (
    <div className={`rounded-lg border p-2.5 ${styles}`}>
      <div className="text-[11px] font-bold uppercase tracking-wide">{label}</div>
      <div className="mt-2">{status ? <StatusBadge>{status}</StatusBadge> : <span className="text-slate-500">Not recorded</span>}</div>
      <div className="mt-2 text-[11px] font-medium">Scope</div>
      <div className="mt-0.5 break-words leading-5 text-slate-900">{scope ?? 'Not recorded'}</div>
      <div className="mt-2 text-[11px] font-medium">Statement</div>
      <ExpandableText className="mt-0.5 block leading-5 text-slate-900" limit={130} emptyLabel="Not recorded">
        {statement}
      </ExpandableText>
    </div>
  )
}

function ClaimComparison({ claim }: { claim: ClaimAdoptionComparison }) {
  const changes = claim.changed_fields
  const currentDiffersFromAdoption = Boolean(claim.adopted_status) && (
    claim.current_status !== claim.adopted_status
    || claim.current_scope !== claim.adopted_scope
    || claim.current_statement !== claim.adopted_statement
  )

  if (claim.is_additional_claim) {
    return (
      <article aria-label={`Additional Claim adoption ${claim.claim_id}`} className="rounded-lg border border-emerald-200 bg-white p-3">
        <div className="flex flex-wrap items-start justify-between gap-2">
          <div className="min-w-0">
            <div className="break-words text-xs font-semibold text-slate-900">{claim.claim_name || 'Claim'}</div>
            <div className="mt-0.5 break-all font-mono text-[11px] text-slate-500">{claim.claim_id}</div>
          </div>
          <StatusBadge tone="success">Additional Claim observed</StatusBadge>
        </div>
        <div className={`mt-3 grid gap-2.5 text-xs ${currentDiffersFromAdoption ? 'sm:grid-cols-2' : ''}`}>
          <ClaimFields
            label="Additional Claim from this CAU"
            status={claim.adopted_status}
            scope={claim.adopted_scope}
            statement={claim.adopted_statement}
            tone="emerald"
          />
          {currentDiffersFromAdoption ? (
            <ClaimFields
              label="Current Mirror"
              status={claim.current_status}
              scope={claim.current_scope}
              statement={claim.current_statement}
              tone="amber"
            />
          ) : null}
        </div>
      </article>
    )
  }

  return (
    <article aria-label={`Claim adoption ${claim.claim_id}`} className="rounded-lg border border-amber-200 bg-white p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="break-words text-xs font-semibold text-slate-900">{claim.claim_name ?? 'Claim'}</div>
          <div className="mt-0.5 break-all font-mono text-[11px] text-slate-500">{claim.claim_id}</div>
        </div>
        {claim.current_status ? (
          <StatusBadge tone={claim.update_observed ? 'info' : 'neutral'}>
            {claim.update_observed
              ? `Attributed update${changes.length ? ` · ${changes.join(', ')}` : ''}`
              : 'No attributed update'}
          </StatusBadge>
        ) : (
          <StatusBadge tone="warning">Mirror unavailable</StatusBadge>
        )}
      </div>

      <div className={`mt-3 grid gap-2.5 text-xs ${claim.adopted_status ? 'sm:grid-cols-3' : 'sm:grid-cols-2'}`}>
        <ClaimFields label="At Resolution" status={claim.snapshot_status} scope={claim.snapshot_scope} statement={claim.snapshot_statement} />
        {claim.adopted_status ? (
          <ClaimFields label="Agent Adoption" status={claim.adopted_status} scope={claim.adopted_scope} statement={claim.adopted_statement} tone="emerald" />
        ) : null}
        <ClaimFields label="Current Mirror" status={claim.current_status} scope={claim.current_scope} statement={claim.current_statement} tone="amber" />
      </div>
    </article>
  )
}

function HolderCard({ holder, observedAt }: { holder: HolderAdoption; observedAt?: string }) {
  const directClaims = holder.claims.filter((claim) => !claim.is_additional_claim)
  const additionalClaims = holder.claims.filter((claim) => claim.is_additional_claim)
  return (
    <article aria-label={`Holder adoption ${holder.agent_id}`} className="rounded-md border border-amber-200 bg-amber-50/40 p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-xs font-semibold text-slate-900">{holder.agent_id}</div>
          <div className="mt-1 text-[11px] text-slate-500">
            {holder.claim_count} Claim{holder.claim_count === 1 ? '' : 's'} observed · {holder.updated_claim_count} updated · {holder.additional_claim_count ?? 0} additional · {holder.unchanged_claim_count} unchanged
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
      ) : null}

      <details className="mt-3 rounded-md border border-slate-200 bg-white p-2.5">
        <summary className="cursor-pointer text-xs font-medium text-amber-800">
          Direct Claim adoption ({directClaims.length})
        </summary>
        <div className="mt-2 space-y-2">
          {directClaims.length
            ? directClaims.map((claim) => <ClaimComparison key={claim.claim_id} claim={claim} />)
            : <div className="text-xs text-slate-500">No direct Claims are associated with this holder.</div>}
        </div>
      </details>

      {additionalClaims.length ? (
        <details open className="mt-2 rounded-md border border-emerald-200 bg-white p-2.5">
          <summary className="cursor-pointer text-xs font-medium text-emerald-800">
            Additional Claims from this CAU ({additionalClaims.length})
          </summary>
          <div className="mt-2 space-y-2">
            {additionalClaims.map((claim) => <ClaimComparison key={claim.claim_id} claim={claim} />)}
          </div>
        </details>
      ) : null}

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
    delivered_holders: 0,
    updated_claims: 0,
    additional_claims: 0,
    unchanged_claims: 0,
    unavailable_claims: 0,
  }

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
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Notified holders</dt><dd className={summaryValueClass}>{summary.notified_holders}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Delivered holders</dt><dd className={summaryValueClass}>{summary.delivered_holders}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Updated Claims</dt><dd className={summaryValueClass}>{summary.updated_claims}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Additional Claims</dt><dd className={summaryValueClass}>{summary.additional_claims ?? 0}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Unchanged Claims</dt><dd className={summaryValueClass}>{summary.unchanged_claims}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Unavailable Claims</dt><dd className={summaryValueClass}>{summary.unavailable_claims}</dd></div>
        <div className={summaryCellClass}><dt className={summaryLabelClass}>Last observation change</dt><dd className="mt-1 font-mono text-[11px] text-amber-950">{formatDateTime(adoption?.observed_at)}</dd></div>
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
