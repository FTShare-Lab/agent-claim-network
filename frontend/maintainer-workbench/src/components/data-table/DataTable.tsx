import type { KeyboardEvent, MouseEvent } from 'react'

import { cn } from '../../lib/utils'

export type DataTableColumn<Row> = {
  key: string
  header: string
  className?: string
  mobileHidden?: boolean
  render: (row: Row) => React.ReactNode
}

type DataTableProps<Row> = {
  columns: DataTableColumn<Row>[]
  rows: Row[]
  getRowId: (row: Row) => string
  onRowClick?: (row: Row) => void
  emptyState?: string
}

export function DataTable<Row>({
  columns,
  rows,
  getRowId,
  onRowClick,
  emptyState = 'No rows found.',
}: DataTableProps<Row>) {
  function isNestedInteractiveTarget(target: EventTarget | null, currentTarget: EventTarget) {
    if (!(target instanceof HTMLElement) || target === currentTarget) return false
    return Boolean(
      target.closest('a, button, input, select, textarea, [role="button"], [role="link"]'),
    )
  }

  function activateRow(
    event: KeyboardEvent<HTMLTableRowElement> | MouseEvent<HTMLTableRowElement>,
    row: Row,
  ) {
    if (!onRowClick || isNestedInteractiveTarget(event.target, event.currentTarget)) return
    if ('key' in event && event.key !== 'Enter' && event.key !== ' ') return
    if ('key' in event) event.preventDefault()
    onRowClick(row)
  }

  return (
    <div className="acn-data-table overflow-hidden rounded-[var(--radius-md)] bg-white ring-1 ring-slate-200/80">
      <div className="overflow-x-auto overscroll-x-contain">
        <table className="min-w-full border-collapse text-left text-[13px] text-slate-700">
          <thead className="border-b border-slate-200/80 bg-slate-50/80 text-xs font-semibold text-slate-500">
            <tr>
              {columns.map((column) => (
                <th key={column.key} scope="col" className={cn('px-3 py-2.5 font-semibold', column.mobileHidden && 'hidden md:table-cell', column.className)}>
                  {column.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td className="acn-empty-row px-4 py-12 text-center text-sm text-slate-500" colSpan={columns.length}>
                  {emptyState}
                </td>
              </tr>
            ) : (
              rows.map((row) => (
                <tr
                  key={getRowId(row)}
                  className={cn(
                    'border-b border-slate-100/90 last:border-b-0',
                    onRowClick
                      ? 'cursor-pointer transition-[background-color,box-shadow] duration-150 hover:bg-blue-50/45 hover:shadow-[inset_3px_0_0_var(--accent)] focus-visible:bg-blue-50 focus-visible:outline focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-[var(--accent)]'
                      : '',
                  )}
                  tabIndex={onRowClick ? 0 : undefined}
                  onClick={onRowClick ? (event) => activateRow(event, row) : undefined}
                  onKeyDown={onRowClick ? (event) => activateRow(event, row) : undefined}
                >
                  {columns.map((column) => (
                    <td key={column.key} data-label={column.header} className={cn('px-3 py-3 align-top', column.mobileHidden && 'hidden md:table-cell', column.className)}>
                      {column.render(row)}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
