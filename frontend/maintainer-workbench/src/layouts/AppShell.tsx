import { Outlet } from 'react-router'

import { isStaticDemo } from '../lib/runtime'
import { Sidebar } from './Sidebar'
import { Topbar } from './Topbar'

export function AppShell() {
  return (
    <div className="min-h-screen bg-[var(--bg-page)] text-[var(--text-primary)]">
      <a
        href="#workbench-main"
        className="fixed left-3 top-3 z-50 -translate-y-20 rounded-lg bg-slate-950 px-3 py-2 text-sm font-semibold text-white shadow-[var(--shadow-raised)] transition-transform duration-150 focus:translate-y-0"
      >
        Skip to main content
      </a>
      <Topbar />
      <div className="flex">
        <Sidebar />
        <main id="workbench-main" className="min-w-0 flex-1 px-4 py-6 sm:px-6 lg:px-9 lg:py-8 2xl:px-12" tabIndex={-1}>
          {isStaticDemo ? (
            <aside className="mb-4 flex flex-col gap-1 rounded-[var(--radius-md)] border border-violet-200 bg-violet-50 px-4 py-3 text-violet-950 sm:flex-row sm:items-center sm:justify-between">
              <div className="text-xs font-semibold">Public preview · synthetic data</div>
              <div className="text-[11px] leading-5 text-violet-800">
                Read-only. This page does not contact Maintainer, Router, or any team service.
              </div>
            </aside>
          ) : null}
          <Outlet />
        </main>
      </div>
    </div>
  )
}
