import type { PropsWithChildren } from 'react'

import { cn } from '../../lib/utils'

const toneClasses = {
  neutral: {
    shell: 'border-slate-200 bg-white',
    divider: 'border-slate-100',
    marker: 'bg-slate-500',
    heading: 'text-slate-900',
  },
  claim: {
    shell: 'border-blue-200 bg-blue-50/30',
    divider: 'border-blue-100',
    marker: 'bg-blue-600',
    heading: 'text-blue-950',
  },
  analysis: {
    shell: 'border-violet-200 bg-violet-50/30',
    divider: 'border-violet-100',
    marker: 'bg-violet-600',
    heading: 'text-violet-950',
  },
  resolution: {
    shell: 'border-emerald-200 bg-emerald-50/30',
    divider: 'border-emerald-100',
    marker: 'bg-emerald-600',
    heading: 'text-emerald-950',
  },
} as const

type DrawerSectionProps = PropsWithChildren<{
  title: string
  tone?: keyof typeof toneClasses
  className?: string
  contentClassName?: string
}>

export function DrawerSection({
  title,
  tone = 'neutral',
  className,
  contentClassName,
  children,
}: DrawerSectionProps) {
  const classes = toneClasses[tone]
  return (
    <section className={cn('rounded-xl border p-3.5 shadow-sm', classes.shell, className)}>
      <div className={cn('flex items-center gap-2 border-b pb-2.5', classes.divider)}>
        <span aria-hidden="true" className={cn('h-4 w-1 rounded-full', classes.marker)} />
        <h4 className={cn('text-xs font-bold uppercase tracking-[0.08em]', classes.heading)}>{title}</h4>
      </div>
      <div className={cn('mt-3', contentClassName)}>{children}</div>
    </section>
  )
}
