import { ArrowRight } from 'lucide-react'
import { Link } from 'react-router'

import { cn } from '../../lib/utils'

type MetricCardProps = {
  label: string
  value: string | number
  detail: string
  href?: string
  accent?: 'blue' | 'green' | 'purple' | 'red'
}

const accentText = {
  blue: 'text-blue-700',
  green: 'text-emerald-700',
  purple: 'text-violet-700',
  red: 'text-rose-700',
} as const

export function MetricCard({ label, value, detail, href, accent = 'blue' }: MetricCardProps) {
  const content = (
    <div
      className={cn(
        'flex h-full flex-col rounded-lg border border-slate-200 bg-white p-3 transition',
        href ? 'hover:border-slate-300 hover:bg-slate-50' : '',
      )}
    >
      <div className="text-[11px] font-medium uppercase tracking-wide text-slate-500">{label}</div>
      <div className={cn('mt-1.5 font-mono text-2xl font-semibold tracking-tight', accentText[accent])}>{value}</div>
      <div className="mt-1 text-xs text-slate-500">{detail}</div>
      {href ? (
        <div className="mt-2 inline-flex items-center gap-1 text-[11px] font-medium text-blue-700">
          open <ArrowRight className="h-3 w-3" />
        </div>
      ) : null}
    </div>
  )

  return href ? <Link to={href}>{content}</Link> : content
}
