import type { PropsWithChildren } from 'react'
import { SlidersHorizontal } from 'lucide-react'
import { useId, useState } from 'react'

export function FilterBar({ children }: PropsWithChildren) {
  const [mobileOpen, setMobileOpen] = useState(false)
  const controlsId = useId()

  return (
    <section aria-label="Filters" className="rounded-[var(--radius-md)] bg-white p-2.5 ring-1 ring-slate-200/80 md:p-4">
      <button
        type="button"
        className="flex min-h-11 w-full items-center justify-between rounded-lg px-2 text-sm font-semibold text-slate-700 md:hidden"
        aria-expanded={mobileOpen}
        aria-controls={controlsId}
        onClick={() => setMobileOpen((open) => !open)}
      >
        <span className="inline-flex items-center gap-2"><SlidersHorizontal aria-hidden="true" className="h-4 w-4 text-[var(--accent)]" />Search & filters</span>
        <span className="text-xs font-medium text-slate-500">{mobileOpen ? 'Done' : 'Open'}</span>
      </button>
      <div id={controlsId} className={`${mobileOpen ? 'grid' : 'hidden'} items-end gap-3 px-2 pb-2 pt-3 md:grid md:grid-cols-2 md:px-0 md:pb-0 md:pt-0 xl:grid-cols-4`}>
        {children}
      </div>
    </section>
  )
}
