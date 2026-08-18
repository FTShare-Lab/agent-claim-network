import { StatusBadge } from '../../components/badges/StatusBadge'
import { formatDateTime } from '../../lib/format'
import type {
  ArbitrationAnalysisDetail,
  ArbitrationAnalysisSummary,
} from './types'
import { ExpandableText } from './ExpandableText'

function formatValue(value: string | undefined) {
  return value ? value.replaceAll('_', ' ') : 'Not specified'
}

function confidence(value: number | undefined) {
  return value === undefined ? 'N/A' : `${Math.round(value * 100)}%`
}

function analysisStatus(analysis: ArbitrationAnalysisSummary | ArbitrationAnalysisDetail) {
  if (analysis.state === 'waiting_reanalysis') {
    return analysis.context_change_count === 1
      ? '等待 5 分钟后重新分析'
      : '等待 15 分钟后重新分析'
  }
  if (analysis.adoption_blocker === '分析输入连续变化，已停止自动处理，等待人工') {
    return analysis.adoption_blocker
  }
  if (analysis.state === 'approved' && analysis.adoption_blocker) {
    return 'Approved，但采用被阻止'
  }
  return undefined
}

type AnalysisCardProps = {
  analysis: ArbitrationAnalysisSummary | ArbitrationAnalysisDetail
  label: string
  onAdopt?: (analysisId: string) => void
  adopting?: boolean
  compact?: boolean
  resolutionClosed?: boolean
}

export function AnalysisCard({ analysis, label, onAdopt, adopting = false, compact = false, resolutionClosed = false }: AnalysisCardProps) {
  const detail = 'frozen_context' in analysis ? analysis : undefined
  const proposal = analysis.proposal
  const unresolved = analysis.state === 'unresolved'
    || proposal?.resolution_type === 'unresolved'
    || detail?.verification?.verdict === 'unresolved'
  // Resolution 已经消费了 Analysis；即使客户端仍持有采用前的缓存，也不再展示
  // “等待重分析”或“采用被阻止”等仅对 open Dispute 有意义的操作状态。
  const status = resolutionClosed ? undefined : analysisStatus(analysis)
  return (
    <article aria-label={`${label} ${analysis.analysis_id}`} className="rounded-lg border border-violet-200 bg-white p-3 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">{label}</div>
          <div className="mt-1 break-all font-mono text-[11px] text-slate-700">{analysis.analysis_id}</div>
        </div>
        <div className="flex flex-wrap gap-1.5">
          <StatusBadge>{formatValue(analysis.state)}</StatusBadge>
        </div>
      </div>

      <dl className="mt-3 grid gap-2 text-xs sm:grid-cols-2">
        <div className="rounded bg-slate-50 px-2.5 py-2">
          <dt className="font-medium text-slate-500">Created</dt>
          <dd className="mt-1 font-mono text-[11px] text-slate-800">{formatDateTime(analysis.created_at)}</dd>
        </div>
        <div className="rounded bg-slate-50 px-2.5 py-2">
          <dt className="font-medium text-slate-500">Updated</dt>
          <dd className="mt-1 font-mono text-[11px] text-slate-800">{formatDateTime(analysis.updated_at)}</dd>
        </div>
      </dl>

      {status ? (
        <div role="status" className="mt-3 rounded-md border border-amber-200 bg-amber-50 p-2.5 text-xs leading-5 text-amber-900">
          <div className="font-semibold">{status}</div>
          <div className="mt-1">当前第 {analysis.analysis_round ?? 1} 轮；已检测到 {analysis.context_change_count ?? 0} 次分析输入变化。</div>
          {analysis.next_retry_at ? <div>下次重试：{formatDateTime(analysis.next_retry_at)}</div> : null}
          {analysis.context_change_reason ? <ExpandableText className="mt-1 block" limit={220}>{analysis.context_change_reason}</ExpandableText> : null}
        </div>
      ) : null}

      {analysis.error ? (
        <div className="mt-3 rounded-md border border-rose-200 bg-rose-50 p-2.5 text-xs leading-5 text-rose-800">
          <div className="font-semibold">{analysis.error.code}</div>
          <ExpandableText limit={220}>{analysis.error.message}</ExpandableText>
        </div>
      ) : null}

      {proposal ? (
        <section aria-label={`${label} proposal`} className="mt-3 rounded-md border border-blue-100 bg-blue-50/60 p-3">
          <div className="flex flex-wrap gap-1.5">
            <StatusBadge>{formatValue(proposal.resolution_type)}</StatusBadge>
            <StatusBadge>{formatValue(proposal.resolution_basis)}</StatusBadge>
            <StatusBadge tone="info">confidence {confidence(proposal.confidence)}</StatusBadge>
          </div>
          <div className="mt-2 text-[11px] font-semibold uppercase tracking-wide text-blue-700">Conclusion</div>
          <ExpandableText className="mt-1 block text-xs leading-5 text-slate-800" limit={compact ? 150 : 260}>
            {proposal.conclusion}
          </ExpandableText>
          {!compact && proposal.missing_evidence?.length ? (
            <div className="mt-3">
              <div className="text-[11px] font-semibold uppercase tracking-wide text-amber-700">Missing evidence</div>
              <ul className="mt-1 list-disc space-y-1 pl-4 text-xs leading-5 text-amber-900">
                {proposal.missing_evidence.map((item) => <li key={item}>{item}</li>)}
              </ul>
            </div>
          ) : null}
          {!compact && proposal.human_review_reason ? (
            <div className="mt-3 rounded bg-amber-50 p-2 text-xs leading-5 text-amber-900">
              <div className="text-[11px] font-semibold uppercase tracking-wide text-amber-700">Human review note</div>
              <ExpandableText className="mt-1 block" limit={220}>{proposal.human_review_reason}</ExpandableText>
            </div>
          ) : null}
          {!compact && unresolved ? (
            <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 p-2.5 text-xs leading-5 text-amber-900">
              当前 Analysis 未形成可采用的裁决结论，不建议修改 Claim；Dispute 保持 open，等待人类管理者处理。
            </div>
          ) : null}
          {!compact && !unresolved ? (
            <details className="mt-3 rounded-md border border-blue-100 bg-white p-2.5" open>
              <summary className="cursor-pointer text-xs font-medium text-blue-800">
                Direct Claim assessments ({proposal.claim_assessments.length})
              </summary>
              <div className="mt-2 space-y-2">
                {proposal.claim_assessments.map((assessment) => (
                  <article key={assessment.claim_id} className="rounded border border-slate-200 p-2.5">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <span className="break-all font-mono text-[11px] text-slate-700">{assessment.claim_id}</span>
                      <StatusBadge>{assessment.recommended_status}</StatusBadge>
                    </div>
                    <ExpandableText className="mt-2 block text-xs font-medium leading-5 text-slate-800" limit={180}>
                      {assessment.assessment}
                    </ExpandableText>
                    <ExpandableText className="mt-1 block text-xs leading-5 text-slate-600" limit={180}>
                      {assessment.reason}
                    </ExpandableText>
                    {assessment.recommended_scope ? (
                      <div className="mt-2 rounded bg-slate-50 px-2 py-1.5 text-xs text-slate-700">
                        <span className="font-medium text-slate-500">Recommended scope: </span>
                        {assessment.recommended_scope}
                      </div>
                    ) : null}
                    {assessment.recommended_statement ? (
                      <div className="mt-2 rounded bg-slate-50 px-2 py-1.5 text-xs leading-5 text-slate-700">
                        <span className="font-medium text-slate-500">Recommended statement: </span>
                        <ExpandableText limit={180}>{assessment.recommended_statement}</ExpandableText>
                      </div>
                    ) : null}
                  </article>
                ))}
              </div>
            </details>
          ) : null}
          {!compact && proposal.evidence_refs?.length ? (
            <details className="mt-2 rounded-md border border-blue-100 bg-white p-2.5">
              <summary className="cursor-pointer text-xs font-medium text-blue-800">Evidence references ({proposal.evidence_refs.length})</summary>
              <div className="mt-2 flex flex-wrap gap-1">
                {proposal.evidence_refs.map((reference) => (
                  <span key={reference} className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700">{reference}</span>
                ))}
              </div>
            </details>
          ) : null}
        </section>
      ) : (
        <div className="mt-3 text-xs text-slate-500">No proposal has been produced yet.</div>
      )}

      {!compact && detail?.verification ? (
        <section aria-label={`${label} verification`} className="mt-3 rounded-md border border-slate-200 bg-slate-50 p-3">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Verification</span>
            <StatusBadge>{detail.verification.verdict}</StatusBadge>
            <StatusBadge tone="info">confidence {confidence(detail.verification.confidence)}</StatusBadge>
          </div>
          <dl className="mt-2 grid grid-cols-3 gap-1.5 text-[11px]">
            <div className="rounded bg-white px-2 py-1.5"><dt className="text-slate-500">Type</dt><dd>{detail.verification.resolution_type_agreed ? 'Agreed' : 'Disagreed'}</dd></div>
            <div className="rounded bg-white px-2 py-1.5"><dt className="text-slate-500">Basis</dt><dd>{detail.verification.resolution_basis_agreed ? 'Agreed' : 'Disagreed'}</dd></div>
            <div className="rounded bg-white px-2 py-1.5"><dt className="text-slate-500">Conclusion</dt><dd>{detail.verification.conclusion_agreed ? 'Agreed' : 'Disagreed'}</dd></div>
          </dl>
          <ExpandableText className="mt-2 block text-xs leading-5 text-slate-700" limit={220}>
            {detail.verification.reasoning}
          </ExpandableText>
          {detail.verification.verdict === 'unresolved' ? (
            <div className="mt-2 text-[11px] text-slate-500">Verification 未给出 Claim 修改建议，等待人类管理者处理。</div>
          ) : (
            <div className="mt-2 text-[11px] text-slate-500">
              {detail.verification.claim_assessments.filter((assessment) => assessment.agreed).length}
              /{detail.verification.claim_assessments.length} Claim assessments agreed
            </div>
          )}
          {detail.verification.missing_evidence?.length ? (
            <ul className="mt-2 list-disc space-y-1 pl-4 text-xs leading-5 text-amber-900">
              {detail.verification.missing_evidence.map((item) => <li key={item}>{item}</li>)}
            </ul>
          ) : null}
        </section>
      ) : null}

      {!compact && detail?.frozen_context ? (
        <details className="mt-3 rounded-md border border-slate-200 bg-slate-50 p-2.5 text-xs">
          <summary className="cursor-pointer font-medium text-slate-700">Analysis context summary</summary>
          <dl className="mt-2 grid grid-cols-2 gap-2 text-[11px] sm:grid-cols-3">
            <div><dt className="text-slate-500">Direct Claims</dt><dd className="font-semibold">{detail.frozen_context.direct_claims.length}</dd></div>
            <div><dt className="text-slate-500">Source Claims</dt><dd className="font-semibold">{detail.frozen_context.source_claims.length}</dd></div>
            <div><dt className="text-slate-500">Policies</dt><dd className="font-semibold">{detail.frozen_context.policies.length}</dd></div>
            <div><dt className="text-slate-500">Related Claims in context</dt><dd className="font-semibold">{detail.frozen_context.router_candidate_claims.length}</dd></div>
            <div><dt className="text-slate-500">Prior resolutions</dt><dd className="font-semibold">{detail.frozen_context.prior_resolutions.length}</dd></div>
            <div><dt className="text-slate-500">Frozen at</dt><dd className="font-mono">{formatDateTime(detail.frozen_context.generated_at)}</dd></div>
          </dl>
          {detail.warnings.length ? (
            <ul className="mt-2 list-disc space-y-1 pl-4 text-amber-800">
              {detail.warnings.map((warning) => <li key={`${warning.code}:${warning.detail}`}>{warning.code}: {warning.detail}</li>)}
            </ul>
          ) : null}
          <div className="mt-2 text-slate-500">Validation: {detail.validation_result}</div>
        </details>
      ) : null}

      {!compact && detail?.rounds?.length ? (
        <details className="mt-3 rounded-md border border-slate-200 bg-slate-50 p-2.5 text-xs">
          <summary className="cursor-pointer font-medium text-slate-700">Analysis rounds ({detail.rounds.length})</summary>
          <div className="mt-2 space-y-2">
            {detail.rounds.map((round) => (
              <div key={`${round.round}:${round.semantic_fingerprint}`} className="rounded bg-white p-2">
                <div className="font-semibold">Round {round.round} · {formatValue(round.proposal.resolution_type)} · {round.verification.verdict}</div>
                <div className="mt-1 text-slate-500">{formatDateTime(round.started_at)} → {round.completed_at ? formatDateTime(round.completed_at) : 'In progress'}</div>
                {round.context_change_reason ? <ExpandableText className="mt-1 block text-amber-800" limit={180}>{round.context_change_reason}</ExpandableText> : null}
                <div className="mt-1 break-all font-mono text-[10px] text-slate-400">{round.semantic_fingerprint}</div>
              </div>
            ))}
          </div>
        </details>
      ) : null}

      {!resolutionClosed && analysis.adoption_blocker && !analysis.adoptable ? (
        <div className="mt-3 text-xs leading-5 text-slate-500">Cannot adopt: {analysis.adoption_blocker}</div>
      ) : null}
      {!resolutionClosed && analysis.adoptable && analysis.state === 'approved' && onAdopt ? (
        <button
          type="button"
          className="mt-3 rounded-md bg-violet-700 px-2.5 py-1.5 text-xs font-semibold text-white hover:bg-violet-800 disabled:bg-slate-300"
          disabled={adopting}
          onClick={() => onAdopt(analysis.analysis_id)}
        >
          {adopting ? 'Adopting…' : '采用此分析'}
        </button>
      ) : null}
    </article>
  )
}
