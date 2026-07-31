import { ChevronLeft, ChevronRight } from 'lucide-react'
import { useId } from 'react'

import { cn } from '../../lib/utils'

type PaginationBarProps = {
  page: number
  pageSize: number
  total: number
  onPageChange: (page: number) => void
  onPageSizeChange?: (size: number) => void
  pageSizeOptions?: number[]
}

export function PaginationBar({
  page,
  pageSize,
  total,
  onPageChange,
  onPageSizeChange,
  pageSizeOptions = [10, 20, 50],
}: PaginationBarProps) {
  const pageSizeId = useId()
  const totalPages = Math.max(1, Math.ceil(total / pageSize))
  const from = total === 0 ? 0 : (page - 1) * pageSize + 1
  const to = Math.min(total, page * pageSize)
  const pages = Array.from({ length: Math.min(totalPages, 5) }, (_, index) => {
    if (totalPages <= 5) return index + 1
    const start = Math.max(1, Math.min(page - 2, totalPages - 4))
    return start + index
  })

  return (
    <nav aria-label="Pagination" className="flex flex-col gap-3 text-xs text-slate-600 md:flex-row md:items-center md:justify-between">
      <div className="font-mono" aria-live="polite">
        {from}–{to} / {total}
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          aria-label="Previous page"
          className="acn-interactive inline-flex h-10 min-w-10 items-center justify-center rounded-lg bg-white text-slate-600 shadow-sm ring-1 ring-slate-200 hover:bg-slate-50 disabled:cursor-not-allowed disabled:text-slate-300 disabled:shadow-none disabled:ring-slate-100 disabled:hover:translate-y-0 disabled:hover:bg-white"
          onClick={() => onPageChange(page - 1)}
          disabled={page <= 1}
        >
          <ChevronLeft aria-hidden="true" className="h-4 w-4" />
        </button>
        {pages.map((item) => (
          <button
            key={item}
            type="button"
            aria-current={item === page ? 'page' : undefined}
            aria-label={item === page ? `Page ${item}, current page` : `Go to page ${item}`}
            onClick={() => onPageChange(item)}
            className={cn(
              'acn-interactive h-10 min-w-10 rounded-lg px-2 font-mono text-xs font-semibold',
              item === page
                ? 'bg-[var(--accent)] text-white shadow-[0_5px_14px_rgb(86_95_232_/_0.22)]'
                : 'bg-white text-slate-600 shadow-sm ring-1 ring-slate-200 hover:bg-slate-50',
            )}
          >
            {item}
          </button>
        ))}
        <button
          type="button"
          aria-label="Next page"
          className="acn-interactive inline-flex h-10 min-w-10 items-center justify-center rounded-lg bg-white text-slate-600 shadow-sm ring-1 ring-slate-200 hover:bg-slate-50 disabled:cursor-not-allowed disabled:text-slate-300 disabled:shadow-none disabled:ring-slate-100 disabled:hover:translate-y-0 disabled:hover:bg-white"
          onClick={() => onPageChange(page + 1)}
          disabled={page >= totalPages}
        >
          <ChevronRight aria-hidden="true" className="h-4 w-4" />
        </button>
        {onPageSizeChange ? (
          <label htmlFor={pageSizeId} className="ml-1 inline-flex items-center gap-2 font-medium text-slate-600">
            <span>Rows per page</span>
            <select
              id={pageSizeId}
              className="h-10 rounded-lg border-0 bg-white px-2.5 text-xs text-slate-700 shadow-sm ring-1 ring-slate-200"
              value={pageSize}
              onChange={(event) => onPageSizeChange(Number(event.target.value))}
            >
              {pageSizeOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        ) : null}
      </div>
    </nav>
  )
}
