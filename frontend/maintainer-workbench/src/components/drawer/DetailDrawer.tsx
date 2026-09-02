import { ChevronLeft, X } from 'lucide-react'
import { type PropsWithChildren, type ReactNode, useEffect, useId, useRef } from 'react'
import { createPortal } from 'react-dom'

import { cn } from '../../lib/utils'

type DetailDrawerProps = PropsWithChildren<{
  open: boolean
  title: string
  subtitle?: string
  label: string
  ariaLabel?: string
  onClose: () => void
  footer?: ReactNode
  backLabel?: string
  onBack?: () => void
  modal?: boolean
}>

const focusableSelector = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',')

function trapFocus(event: KeyboardEvent, container: HTMLElement) {
  if (event.key !== 'Tab') return
  const focusable = Array.from(container.querySelectorAll<HTMLElement>(focusableSelector)).filter(
    (element) => element.getAttribute('aria-hidden') !== 'true',
  )
  if (!focusable.length) {
    event.preventDefault()
    container.focus()
    return
  }

  const first = focusable[0]
  const last = focusable[focusable.length - 1]
  if (!container.contains(document.activeElement)) {
    event.preventDefault()
    const edge = event.shiftKey ? last : first
    edge.focus()
    return
  }
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault()
    last.focus()
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault()
    first.focus()
  }
}

export function DetailDrawer({
  open,
  title,
  subtitle,
  label,
  ariaLabel,
  onClose,
  footer,
  backLabel,
  onBack,
  modal = true,
  children,
}: DetailDrawerProps) {
  const dialogRef = useRef<HTMLElement>(null)
  const closeButtonRef = useRef<HTMLButtonElement>(null)
  const onCloseRef = useRef(onClose)
  const titleId = useId()
  const subtitleId = useId()

  useEffect(() => {
    onCloseRef.current = onClose
  }, [onClose])

  useEffect(() => {
    if (!open) return undefined

    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null
    const previousOverflow = document.body.style.overflow
    const appRoot = document.getElementById('root')
    const rootWasInert = appRoot?.hasAttribute('inert') ?? false
    if (modal) {
      document.body.style.overflow = 'hidden'
      appRoot?.setAttribute('inert', '')
    }

    const frame = window.requestAnimationFrame(() => closeButtonRef.current?.focus())
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        event.stopPropagation()
        onCloseRef.current()
        return
      }
      if (modal && dialogRef.current) trapFocus(event, dialogRef.current)
    }
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      window.cancelAnimationFrame(frame)
      document.removeEventListener('keydown', handleKeyDown)
      if (modal) {
        document.body.style.overflow = previousOverflow
        if (!rootWasInert) appRoot?.removeAttribute('inert')
      }
      if (previouslyFocused?.isConnected) previouslyFocused.focus()
    }
  }, [modal, open])

  useEffect(() => {
    if (!open) return undefined
    const frame = window.requestAnimationFrame(() => {
      const dialog = dialogRef.current
      if (dialog && !dialog.contains(document.activeElement)) closeButtonRef.current?.focus()
    })
    return () => window.cancelAnimationFrame(frame)
  }, [label, open, subtitle, title])

  if (!open) return null

  return createPortal(
    <div className={cn('fixed inset-0 z-50', !modal && 'pointer-events-none')}>
      {modal ? (
        <button
          type="button"
          tabIndex={-1}
          aria-label="Close dialog background"
          onClick={() => onCloseRef.current()}
          className="acn-overlay-enter absolute inset-0 h-full w-full cursor-default bg-slate-950/35 backdrop-blur-[2px]"
        />
      ) : null}
      <aside
        ref={dialogRef}
        role="dialog"
        aria-modal={modal ? true : undefined}
        aria-label={ariaLabel}
        aria-labelledby={ariaLabel ? undefined : titleId}
        aria-describedby={subtitle ? subtitleId : undefined}
        tabIndex={-1}
        className={cn(
          'acn-detail-drawer acn-sheet-enter pointer-events-auto absolute inset-y-0 right-0 z-10 w-full max-w-[760px] overflow-hidden rounded-l-2xl bg-white shadow-[var(--shadow-overlay)]',
          !modal && 'acn-context-inspector ring-1 ring-slate-200/70',
        )}
      >
        <div className="flex h-full flex-col">
          <div className="border-b border-slate-100 bg-white/95 px-4 py-4 backdrop-blur-xl sm:px-6">
            {onBack ? (
              <button
                type="button"
                className="mb-2 inline-flex min-h-10 items-center gap-1 rounded-lg px-1.5 text-xs font-semibold text-slate-500 transition-colors duration-150 hover:bg-slate-100 hover:text-slate-900"
                onClick={onBack}
              >
                <ChevronLeft aria-hidden="true" className="h-4 w-4" />
                <span>{backLabel ?? 'Back'}</span>
              </button>
            ) : null}
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="text-[11px] font-semibold uppercase tracking-[0.1em] text-[var(--accent)]">{label}</div>
                <h3 id={titleId} className="mt-1 [overflow-wrap:anywhere] text-lg font-[700] tracking-[-0.015em] text-slate-900">
                  {title}
                </h3>
                {subtitle ? (
                  <p id={subtitleId} className="mt-0.5 [overflow-wrap:anywhere] font-mono text-xs text-slate-600">
                    {subtitle}
                  </p>
                ) : null}
              </div>
              <button
                ref={closeButtonRef}
                type="button"
                className="acn-interactive inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-slate-100 text-slate-500 hover:bg-slate-200/70 hover:text-slate-900"
                onClick={() => onCloseRef.current()}
                aria-label="Close detail drawer"
              >
                <X aria-hidden="true" className="h-4 w-4" />
              </button>
            </div>
          </div>
          <div className="flex-1 overscroll-contain overflow-y-auto bg-slate-50/45 px-4 py-5 sm:px-6">
            <div className="space-y-4">{children}</div>
          </div>
          {footer ? <div className="border-t border-slate-100 bg-white px-4 py-3 sm:px-6">{footer}</div> : null}
        </div>
      </aside>
    </div>,
    document.body,
  )
}
