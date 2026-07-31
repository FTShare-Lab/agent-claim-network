import { ChevronLeft, X } from 'lucide-react'
import { useEffect, useId, useRef } from 'react'
import { createPortal } from 'react-dom'
import { NavLink } from 'react-router'

import { useWorkbenchUiStore } from '../app/store'
import { navSections } from '../lib/constants'
import { cn } from '../lib/utils'

const focusableSelector = [
  'a[href]',
  'button:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

function NavigationItems({
  collapsed = false,
  mobile = false,
  onNavigate,
}: {
  collapsed?: boolean
  mobile?: boolean
  onNavigate?: () => void
}) {
  return (
    <nav aria-label={mobile ? 'Mobile workbench navigation' : 'Workbench navigation'} className="py-4">
      {navSections.map((section) => (
        <section key={section.title} className="mb-5 last:mb-0">
          <div
            className={cn(
              'px-4 text-[10px] font-bold uppercase tracking-[0.14em] text-slate-500',
              collapsed && 'sr-only',
            )}
          >
            {section.title}
          </div>
          <div className="mt-1.5 space-y-1 px-2">
            {section.items.map((item) => (
              <NavLink
                key={item.href}
                to={item.href}
                end={item.href === '/'}
                title={collapsed ? item.label : undefined}
                onClick={onNavigate}
                className={({ isActive }) =>
                  cn(
                    'group flex min-h-10 items-center rounded-[10px] text-sm transition-[color,background-color] duration-150',
                    collapsed ? 'justify-center px-2' : 'gap-2.5 px-2.5 py-1.5',
                    isActive
                      ? 'bg-[var(--accent-weak)] font-semibold text-[var(--accent-strong)]'
                      : 'text-slate-500 hover:bg-white hover:text-slate-900',
                  )
                }
              >
                {({ isActive }) => (
                  <>
                    <span className={cn('relative flex h-8 w-8 shrink-0 items-center justify-center rounded-lg transition-colors duration-150', isActive ? 'text-[var(--accent)]' : 'text-slate-500 group-hover:text-slate-700')}>
                      <item.icon aria-hidden="true" className="h-4 w-4" />
                    </span>
                    <span className={cn('min-w-0 flex-1', collapsed && 'hidden')}>
                      <span className="block truncate leading-5">{item.label}</span>
                      {mobile ? (
                        <span className="block truncate text-[11px] font-normal leading-4 text-slate-500">
                          {item.description}
                        </span>
                      ) : null}
                    </span>
                  </>
                )}
              </NavLink>
            ))}
          </div>
        </section>
      ))}
    </nav>
  )
}

function trapFocus(event: KeyboardEvent, container: HTMLElement) {
  if (event.key !== 'Tab') return
  const focusable = Array.from(container.querySelectorAll<HTMLElement>(focusableSelector))
  if (!focusable.length) {
    event.preventDefault()
    container.focus()
    return
  }
  const first = focusable[0]
  const last = focusable[focusable.length - 1]
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault()
    last.focus()
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault()
    first.focus()
  }
}

export function Sidebar() {
  const sidebarCollapsed = useWorkbenchUiStore((state) => state.sidebarCollapsed)
  const toggleSidebar = useWorkbenchUiStore((state) => state.toggleSidebar)
  const mobileNavOpen = useWorkbenchUiStore((state) => state.mobileNavOpen)
  const closeMobileNav = useWorkbenchUiStore((state) => state.closeMobileNav)
  const mobileSheetRef = useRef<HTMLElement>(null)
  const mobileCloseRef = useRef<HTMLButtonElement>(null)
  const mobileTitleId = useId()

  useEffect(() => {
    if (!mobileNavOpen) return undefined

    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const previousOverflow = document.body.style.overflow
    const appRoot = document.getElementById('root')
    const rootWasInert = appRoot?.hasAttribute('inert') ?? false
    document.body.style.overflow = 'hidden'
    appRoot?.setAttribute('inert', '')
    const frame = window.requestAnimationFrame(() => mobileCloseRef.current?.focus())

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        closeMobileNav()
        return
      }
      if (mobileSheetRef.current) trapFocus(event, mobileSheetRef.current)
    }
    const media = typeof window.matchMedia === 'function' ? window.matchMedia('(min-width: 1280px)') : null
    const handleViewportChange = () => {
      if (media?.matches) closeMobileNav()
    }

    document.addEventListener('keydown', handleKeyDown)
    media?.addEventListener?.('change', handleViewportChange)
    return () => {
      window.cancelAnimationFrame(frame)
      document.removeEventListener('keydown', handleKeyDown)
      media?.removeEventListener?.('change', handleViewportChange)
      document.body.style.overflow = previousOverflow
      if (!rootWasInert) appRoot?.removeAttribute('inert')
      if (previouslyFocused?.isConnected) previouslyFocused.focus()
    }
  }, [closeMobileNav, mobileNavOpen])

  return (
    <>
      <aside
        className={cn(
          'sticky top-14 hidden h-[calc(100vh-3.5rem)] shrink-0 border-r border-slate-200/70 bg-[#f2f1f5] xl:flex xl:flex-col',
          sidebarCollapsed ? 'w-[68px]' : 'w-[232px]',
        )}
      >
        <div className="flex-1 overflow-y-auto overscroll-contain">
          <NavigationItems collapsed={sidebarCollapsed} />
        </div>
        <div className="border-t border-slate-200/70 p-2.5">
          <button
            type="button"
            onClick={toggleSidebar}
            aria-label={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            className={cn(
              'flex min-h-11 w-full items-center rounded-xl px-2 text-xs font-semibold text-slate-500 transition-colors duration-150 hover:bg-white hover:text-slate-900',
              sidebarCollapsed ? 'justify-center' : 'gap-2',
            )}
          >
            <ChevronLeft
              aria-hidden="true"
              className={cn('h-4 w-4 transition-transform duration-150', sidebarCollapsed && 'rotate-180')}
            />
            <span className={cn(sidebarCollapsed && 'hidden')}>Collapse sidebar</span>
          </button>
        </div>
      </aside>

      {mobileNavOpen
        ? createPortal(
            <div className="fixed inset-0 z-50 xl:hidden">
              <button
                type="button"
                tabIndex={-1}
                aria-label="Close navigation background"
                onClick={closeMobileNav}
                className="acn-overlay-enter absolute inset-0 h-full w-full cursor-default bg-slate-950/35 backdrop-blur-[2px]"
              />
              <aside
                ref={mobileSheetRef}
                role="dialog"
                aria-modal="true"
                aria-labelledby={mobileTitleId}
                tabIndex={-1}
                className="acn-sheet-enter absolute inset-y-0 left-0 z-10 flex w-[min(88vw,360px)] flex-col bg-[#f7f6f9] shadow-[var(--shadow-overlay)]"
              >
                <div className="flex min-h-16 items-center justify-between border-b border-slate-200/70 px-4">
                  <div>
                    <div id={mobileTitleId} className="text-sm font-[680] text-slate-900">Maintainer Workbench</div>
                    <div className="font-mono text-[11px] text-slate-500">Agent Claim Network</div>
                  </div>
                  <button
                    ref={mobileCloseRef}
                    type="button"
                    onClick={closeMobileNav}
                    aria-label="Close navigation"
                    className="acn-interactive inline-flex h-10 w-10 items-center justify-center rounded-xl bg-white text-slate-500 shadow-sm hover:text-slate-900"
                  >
                    <X aria-hidden="true" className="h-4 w-4" />
                  </button>
                </div>
                <div className="flex-1 overflow-y-auto overscroll-contain px-1.5">
                  <NavigationItems mobile onNavigate={closeMobileNav} />
                </div>
                <p className="m-3 rounded-xl bg-white px-3.5 py-3 text-xs leading-5 text-slate-500 shadow-sm">
                  Maintainer publishes, suggests, and reviews. Agents retain ownership of local judgment.
                </p>
              </aside>
            </div>,
            document.body,
          )
        : null}
    </>
  )
}
