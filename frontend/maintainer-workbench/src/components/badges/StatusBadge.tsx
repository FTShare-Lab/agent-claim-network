import type { PropsWithChildren } from 'react'

import { statusTone } from '../../lib/format'
import { cn } from '../../lib/utils'

const toneClasses = {
  success: 'bg-[var(--success-weak)] text-[var(--success)]',
  warning: 'bg-[var(--warning-weak)] text-[var(--warning)]',
  danger: 'bg-[var(--danger-weak)] text-[var(--danger)]',
  neutral: 'bg-slate-100 text-slate-600',
  info: 'bg-[var(--info-weak)] text-[var(--info)]',
} as const

type StatusBadgeProps = PropsWithChildren<{
  tone?: keyof typeof toneClasses
  className?: string
}>

export function StatusBadge({ children, tone, className }: StatusBadgeProps) {
  const resolvedTone = tone ?? statusTone(String(children))
  return (
    <span
      className={cn(
        'inline-flex items-center whitespace-nowrap rounded-full px-2.5 py-1 text-[11px] font-semibold leading-4 capitalize',
        toneClasses[resolvedTone],
        className,
      )}
    >
      {children}
    </span>
  )
}
