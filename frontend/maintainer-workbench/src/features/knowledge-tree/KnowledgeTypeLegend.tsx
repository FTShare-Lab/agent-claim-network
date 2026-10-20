// 两种画布共享类型配色；持有者对比仅适用于选定根 Claim 的视图。
import { useT } from '../../i18n'
import { cn } from '../../lib/utils'
import { knowledgeStyles } from './styles'

export function KnowledgeTypeLegend({ showOtherHolder = false }: { showOtherHolder?: boolean }) {
  const t = useT()
  return (
    <div role="group" aria-label={t("Knowledge type legend")} className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-slate-600">
      {Object.entries(knowledgeStyles).map(([kind, style]) => (
        <span key={kind} className="inline-flex items-center gap-1.5"><span className={cn('h-3 w-3 shrink-0 rounded border', style.className)} />{t(style.label)}</span>
      ))}
      {showOtherHolder && <span className="inline-flex items-center gap-1.5">
        <span data-testid="other-holder-claim-marker" className="h-3.5 w-5 shrink-0" aria-hidden="true">
          <svg viewBox="0 0 20 14" className="h-full w-full"><rect x="0.5" y="0.5" width="19" height="13" rx="3.5" fill="none" stroke="currentColor" strokeWidth="1" strokeDasharray="6 4" className="text-emerald-400" /></svg>
        </span>{t("Claim held by others")}</span>}
    </div>
  )
}
