import type { PropsWithChildren, ReactNode } from 'react'

export function PageContainer({
  title,
  subtitle,
  kicker,
  actions,
  children,
}: PropsWithChildren<{ title: string; subtitle: string; kicker?: string; actions?: ReactNode }>) {
  return (
    <div className="mx-auto max-w-[1440px] space-y-5 lg:space-y-6">
      <header className="flex flex-col gap-4 pb-1 lg:flex-row lg:items-center lg:justify-between">
        <div className="space-y-1.5">
          {kicker ? (
            <div className="text-xs font-semibold tracking-wide text-[var(--accent)]">{kicker}</div>
          ) : null}
          <h1 className="text-2xl font-[650] leading-tight tracking-[-0.025em] text-slate-950">{title}</h1>
          <p className="max-w-[72ch] text-sm leading-6 text-slate-500">{subtitle}</p>
        </div>
        {actions ? <div className="shrink-0">{actions}</div> : null}
      </header>
      {children}
    </div>
  )
}
