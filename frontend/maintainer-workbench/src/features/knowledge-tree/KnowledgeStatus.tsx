// 状态独立于知识类型；争议只取未解决记录，不覆盖节点底色。
import { CircleCheck, CircleSlash, Clock3, MessageCircleWarning } from 'lucide-react'

import { cn } from '../../lib/utils'
import type { Knowledge } from './tree'

const statusStyles = {
  active: { label: 'Active', Icon: CircleCheck, color: 'text-green-700', background: 'bg-green-100' },
  stale: { label: 'Stale', Icon: Clock3, color: 'text-amber-700', background: 'bg-amber-100' },
  deprecated: { label: 'Deprecated', Icon: CircleSlash, color: 'text-slate-600', background: 'bg-slate-100' },
  disputed: { label: 'Disputed', Icon: MessageCircleWarning, color: 'text-rose-700', background: 'bg-rose-100' },
}

export function KnowledgeStatusIcons({ knowledge }: { knowledge: Knowledge }) {
  if (knowledge.kind === 'missing') return null
  const status = knowledge.kind === 'claim' ? knowledge.view.claim.status : knowledge.policy.status
  const statuses: (keyof typeof statusStyles)[] = [status]
  if (knowledge.kind === 'claim' && knowledge.view.open_dispute_ids.length > 0) statuses.push('disputed')
  return (
    <span className="absolute right-1.5 top-1 flex items-center gap-0.5">
      {statuses.map((value) => {
        const { label, Icon, color } = statusStyles[value]
        return <span key={value} role="img" aria-label={label} title={label} className={cn('inline-flex', color)}><Icon size={14} aria-hidden="true" /></span>
      })}
    </span>
  )
}

export function KnowledgeStatusLegend() {
  return (
    <div role="group" aria-label="Status legend" className="flex flex-wrap items-center gap-2">
      {Object.entries(statusStyles).map(([value, { label, Icon, color, background }]) => (
        <span key={value} className={cn('inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-xs font-medium', color, background)}>
          <Icon size={16} aria-hidden="true" />{label}
        </span>
      ))}
    </div>
  )
}
