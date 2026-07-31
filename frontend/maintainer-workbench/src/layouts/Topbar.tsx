import { useState } from 'react'
import { LogOut, Menu, RefreshCw } from 'lucide-react'
import { useQueryClient } from '@tanstack/react-query'
import { useLocation, useNavigate } from 'react-router'

import { formatDateTime } from '../lib/format'
import { isStaticDemo } from '../lib/runtime'
import { useWorkbenchUiStore } from '../app/store'
import { clearAdminSession } from '../features/auth/session'

export function Topbar() {
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const location = useLocation()
  const lastUpdatedAt = useWorkbenchUiStore((state) => state.lastUpdatedAt)
  const markUpdated = useWorkbenchUiStore((state) => state.markUpdated)
  const openMobileNav = useWorkbenchUiStore((state) => state.openMobileNav)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const daemonPort =
    window.location.port || (window.location.protocol === 'https:' ? '443' : '80')

  function goHome() {
    if (location.pathname !== '/') navigate('/')
  }

  async function handleRefresh() {
    if (isRefreshing) return
    setIsRefreshing(true)
    try {
      await queryClient.invalidateQueries()
      markUpdated()
    } finally {
      setIsRefreshing(false)
    }
  }

  function handleSignOut() {
    clearAdminSession()
    queryClient.clear()
    navigate('/login', { replace: true })
  }

  return (
    <header className="sticky top-0 z-40 border-b border-slate-200/70 bg-white/85 shadow-[0_1px_8px_rgb(24_32_51_/_0.03)] backdrop-blur-xl">
      <div className="flex h-14 items-center justify-between gap-3 px-3 sm:px-5 lg:px-6">
        <div className="flex min-w-0 items-center gap-2.5">
          <button
            type="button"
            onClick={openMobileNav}
            aria-label="Open navigation"
            className="acn-interactive inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-slate-100 text-slate-700 hover:bg-slate-200/70 xl:hidden"
          >
            <Menu aria-hidden="true" className="h-4 w-4" />
          </button>
          <button
            type="button"
            onClick={goHome}
            aria-label="Go to overview"
            title="Go to overview"
            className="acn-interactive relative flex h-9 shrink-0 items-center rounded-[10px] border border-slate-300 bg-[linear-gradient(115deg,#5a35ff,#1f5bff_55%,#0c8c5e)] bg-clip-text px-2.5 text-transparent text-[17px] font-black tracking-[0.02em] hover:opacity-80"
          >
            ACN
          </button>
          <button type="button" onClick={goHome} title="Go to overview" className="acn-interactive min-w-0 cursor-pointer rounded-md border-0 bg-transparent p-1 -m-1 text-left hover:bg-slate-100/70">
            <div className="truncate text-sm font-[700] leading-5 text-slate-900">Maintainer Workbench</div>
            <div className="hidden text-[10px] font-medium leading-3 tracking-wide text-slate-500 sm:block">Agent Claim Network</div>
          </button>
        </div>
        <div className="flex shrink-0 items-center gap-1.5 text-xs text-slate-500 sm:gap-2">
          <span
            className={
              isStaticDemo
                ? 'hidden items-center gap-1.5 rounded-full bg-violet-50 px-2.5 py-1.5 text-violet-800 md:inline-flex'
                : 'hidden items-center gap-1.5 rounded-full bg-emerald-50 px-2.5 py-1.5 text-emerald-800 md:inline-flex'
            }
            title={isStaticDemo ? 'Synthetic data; no team service connection' : `Connected to daemon on port ${daemonPort}`}
          >
            <span
              aria-hidden="true"
              className={
                isStaticDemo
                  ? 'h-1.5 w-1.5 rounded-full bg-violet-500 shadow-[0_0_0_3px_rgb(139_92_246_/_0.12)]'
                  : 'h-1.5 w-1.5 rounded-full bg-emerald-500 shadow-[0_0_0_3px_rgb(16_185_129_/_0.12)]'
              }
            />
            <span className="text-[11px] font-semibold">{isStaticDemo ? 'Static preview' : 'Connected'}</span>
          </span>
          <button
            type="button"
            onClick={handleRefresh}
            disabled={isRefreshing}
            aria-busy={isRefreshing}
            aria-label={isRefreshing ? 'Refreshing workbench data' : `Refresh workbench data. Last updated ${formatDateTime(lastUpdatedAt)}`}
            title={`Last updated ${formatDateTime(lastUpdatedAt)}`}
            className="acn-interactive inline-flex h-10 items-center justify-center gap-1.5 rounded-xl bg-slate-100 px-2.5 text-xs font-semibold text-slate-700 hover:bg-slate-200/70 disabled:cursor-wait disabled:text-slate-400 sm:px-3"
          >
            <RefreshCw aria-hidden="true" className={`h-3.5 w-3.5 ${isRefreshing ? 'animate-spin' : ''}`} />
            <span className="hidden sm:inline">{isRefreshing ? 'Refreshing' : 'Refresh'}</span>
          </button>
          {!isStaticDemo ? (
            <button
              type="button"
              onClick={handleSignOut}
              title="Sign out"
              className="acn-interactive inline-flex h-10 items-center justify-center gap-1.5 rounded-xl px-2.5 text-xs font-semibold text-slate-500 hover:bg-rose-50 hover:text-rose-700 sm:px-3"
            >
              <LogOut aria-hidden="true" className="h-3.5 w-3.5" />
              <span className="hidden sm:inline">Sign out</span>
            </button>
          ) : null}
          <span className="sr-only" aria-live="polite">
            {isRefreshing ? 'Refreshing workbench data' : 'Workbench data ready'}
          </span>
        </div>
      </div>
    </header>
  )
}
