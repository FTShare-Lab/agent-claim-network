// Claim 详情由列表和知识树共用，保持来源与争议跳转一致。
import { StatusBadge } from '../../components/badges/StatusBadge'
import { DrawerSection } from '../../components/drawer/DrawerSection'
import { formatDateTime, truncateMiddle } from '../../lib/format'
import type { ClaimView } from './types'

const chipLinkClass = 'rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 font-mono text-[11px] font-medium text-blue-700 transition hover:bg-blue-100'

export function ClaimDetails({ view, onOpenSource, onOpenDispute }: {
  view: ClaimView
  onOpenSource: (id: string) => void
  onOpenDispute: (id: string) => void
}) {
  return (
    <>
      <DrawerSection title="Overview">
        <dl className="space-y-2 text-sm">
          <div className="flex justify-between gap-3"><dt className="text-slate-500">Holder Agent</dt><dd className="font-mono text-xs text-slate-900">{view.claim.holder}</dd></div>
          <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge>{view.claim.status}</StatusBadge></dd></div>
          <div className="flex justify-between gap-3"><dt className="text-slate-500">Confidence</dt><dd className="font-medium capitalize text-slate-900">{view.claim.confidence}</dd></div>
          <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(view.claim.created_at)}</dd></div>
          {view.claim.updated_at ? (
            <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(view.claim.updated_at)}</dd></div>
          ) : null}
        </dl>
      </DrawerSection>
      <DrawerSection title="Related Disputes">
        {view.open_dispute_ids.length || view.resolved_dispute_ids.length ? (
          <div className="flex flex-wrap gap-1">
            {[...view.open_dispute_ids, ...view.resolved_dispute_ids].map((disputeId) => (
              <button
                key={disputeId}
                type="button"
                onClick={() => onOpenDispute(disputeId)}
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
        <div className="text-sm leading-6 text-slate-900">{view.claim.scope}</div>
      </DrawerSection>
      <DrawerSection title="Statement" tone="claim">
        <div className="whitespace-pre-wrap text-sm leading-6 text-slate-900">{view.claim.statement}</div>
      </DrawerSection>
      <DrawerSection title="Evidence Summary" tone="claim">
        <div className="whitespace-pre-wrap text-sm leading-6 text-slate-800">{view.claim.evidence_summary}</div>
      </DrawerSection>
      <DrawerSection title="Source IDs" tone="claim">
        {view.claim.source_claim_ids.length ? (
          <div className="flex flex-wrap gap-1">
            {[...new Set(view.claim.source_claim_ids)].map((sourceId) => (
              <button
                key={sourceId}
                type="button"
                className={chipLinkClass}
                onClick={() => onOpenSource(sourceId)}
              >
                {sourceId}
              </button>
            ))}
          </div>
        ) : (
          <div className="text-xs text-slate-500">No sources</div>
        )}
      </DrawerSection>
    </>
  )
}
