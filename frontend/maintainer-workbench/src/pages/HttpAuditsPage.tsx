import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { FilterBar } from '../components/filters/FilterBar'
import { PaginationBar } from '../components/pagination/PaginationBar'
import { PageContainer } from '../layouts/PageContainer'
import { useAuditsQuery } from '../features/audits/hooks'
import type { HttpAuditRecord } from '../features/audits/types'
import { formatDateTime } from '../lib/format'

function formatJsonBody(raw: string) {
  if (!raw.trim()) return 'N/A'
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

function paginate<T>(items: T[], page: number, pageSize: number) {
  const start = (page - 1) * pageSize
  return items.slice(start, start + pageSize)
}

function auditIdFromRouteState(state: unknown) {
  if (!state || typeof state !== 'object') return null
  const auditId = (state as { auditId?: unknown }).auditId
  return typeof auditId === 'string' && auditId ? auditId : null
}

export function HttpAuditsPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const routeAuditId = auditIdFromRouteState(location.state)
  const { data = [], isLoading, error } = useAuditsQuery()
  const [selectedAuditId, setSelectedAuditId] = useState<string | null>(() => routeAuditId)
  const [pathFilter, setPathFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('all')
  const [methodFilter, setMethodFilter] = useState('all')
  const [sourceFilter, setSourceFilter] = useState('')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)

  useEffect(() => {
    if (!routeAuditId) return
    navigate(location.pathname + location.search, { replace: true, state: null })
  }, [location.pathname, location.search, navigate, routeAuditId])

  const filtered = useMemo(
    () =>
      data.filter((audit) => {
        if (pathFilter && !audit.path.includes(pathFilter)) return false
        if (statusFilter !== 'all' && String(audit.status_code).charAt(0) !== statusFilter) return false
        if (methodFilter !== 'all' && audit.method !== methodFilter) return false
        if (sourceFilter && !(audit.source_ip ?? '').includes(sourceFilter)) return false
        return true
      }),
    [data, methodFilter, pathFilter, sourceFilter, statusFilter],
  )
  const selected = filtered.find((audit) => audit.audit_id === selectedAuditId) ?? data.find((audit) => audit.audit_id === selectedAuditId) ?? null
  const pagedRows = paginate(filtered, page, pageSize)
  const healthyCount = filtered.filter((audit) => audit.status_code < 400).length
  const errorCount = filtered.filter((audit) => audit.status_code >= 400).length
  const latestSource = filtered[0]?.source_ip ?? 'Unavailable'

  if (isLoading) return <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm text-slate-500">Loading audits…</div>
  if (error) return <div className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">{String(error)}</div>

  return (
    <PageContainer title="HTTP Audits" subtitle="Inspect maintainer daemon request traces, response metadata, and related operational resources.">
      <FilterBar>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Path</span>
          <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" placeholder="/maintenance/sweep or /disputes/*" value={pathFilter} onChange={(event) => { setPathFilter(event.target.value); setPage(1) }} />
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Status</span>
          <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={statusFilter} onChange={(event) => { setStatusFilter(event.target.value); setPage(1) }}>
            <option value="all">All Status</option>
            <option value="2">2xx</option>
            <option value="4">4xx</option>
            <option value="5">5xx</option>
          </select>
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Source IP</span>
          <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" placeholder="127.0.0.1" value={sourceFilter} onChange={(event) => { setSourceFilter(event.target.value); setPage(1) }} />
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Method</span>
          <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={methodFilter} onChange={(event) => { setMethodFilter(event.target.value); setPage(1) }}>
            <option value="all">All Methods</option>
            <option value="GET">GET</option>
            <option value="POST">POST</option>
          </select>
        </label>
      </FilterBar>

      <div className="grid gap-2.5 xl:grid-cols-4">
        {[
          ['Audit Rows', filtered.length, 'Filtered results'],
          ['Healthy', healthyCount, '2xx–3xx responses'],
          ['Errors', errorCount, '4xx–5xx responses'],
          ['Latest Source', latestSource, filtered[0] ? `${filtered[0].method} ${filtered[0].path}` : 'No matching rows'],
        ].map(([label, value, detail]) => (
          <div key={label as string} className="min-w-0 rounded-md border border-slate-200 bg-white px-3 py-2.5">
            <div className="text-[11px] text-slate-500">{label}</div>
            <div className="mt-1 [overflow-wrap:anywhere] font-mono text-lg font-semibold tracking-tight text-slate-900">{value as string | number}</div>
            <div className="mt-0.5 [overflow-wrap:anywhere] text-[11px] text-slate-500">{detail as string}</div>
          </div>
        ))}
      </div>

      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold tracking-tight text-slate-900">HTTP Audit Log</h2>
          <StatusBadge tone="info">{filtered.length}</StatusBadge>
        </div>
        <DataTable
          columns={[
            { key: 'time', header: 'Time', render: (row: HttpAuditRecord) => <span className="font-mono text-xs">{formatDateTime(row.occurred_at)}</span> },
            { key: 'request', header: 'Request', render: (row: HttpAuditRecord) => <span className="[overflow-wrap:anywhere] font-mono text-xs font-medium text-slate-900">{row.method} {row.path}</span> },
            { key: 'status', header: 'Status', render: (row: HttpAuditRecord) => <StatusBadge tone={row.status_code < 400 ? 'success' : 'danger'}>{row.status_code}</StatusBadge> },
            { key: 'source', header: 'Source', render: (row: HttpAuditRecord) => <span className="font-mono text-[11px] text-slate-500">{row.source_ip ?? 'Unavailable'}</span> },
            { key: 'resource', header: 'Resource', render: (row: HttpAuditRecord) => <span className="[overflow-wrap:anywhere] font-mono text-[11px] text-slate-500">{row.resource_id ?? '—'}</span> },
            { key: 'duration', header: 'Duration', render: (row: HttpAuditRecord) => <span className="font-mono text-xs">{row.duration_ms} ms</span> },
          ]}
          rows={pagedRows}
          getRowId={(row) => row.audit_id}
          onRowClick={(row) => setSelectedAuditId(row.audit_id)}
          emptyState="No audit rows matched the current filters."
        />
        <PaginationBar page={page} pageSize={pageSize} total={filtered.length} onPageChange={setPage} onPageSizeChange={(size) => { setPageSize(size); setPage(1) }} />
      </section>

      <DetailDrawer
        modal={false}
        open={Boolean(selected)}
        onClose={() => setSelectedAuditId(null)}
        label="HTTP Audit"
        title={selected ? `${selected.method} ${selected.path}` : 'Audit'}
        subtitle={selected?.audit_id}
      >
        {selected ? (
          <>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Overview</div>
              <div className="mt-2 space-y-1.5 text-xs text-slate-600">
                <div className="flex justify-between"><span className="text-slate-500">Status</span> <span className="font-mono text-slate-900">{selected.status_code}</span></div>
                <div className="flex justify-between"><span className="text-slate-500">Duration</span> <span className="font-mono text-slate-900">{selected.duration_ms} ms</span></div>
                <div className="flex justify-between"><span className="text-slate-500">Source IP</span> <span className="font-mono text-slate-900">{selected.source_ip ?? 'Unavailable'}</span></div>
                <div className="flex justify-between"><span className="text-slate-500">Occurred At</span> <span className="font-mono text-slate-900">{formatDateTime(selected.occurred_at)}</span></div>
              </div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Summary</div>
              <div className="mt-2 [overflow-wrap:anywhere] text-xs leading-5 text-slate-600">{selected.summary}</div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Request Body</div>
              <pre className="mt-2 max-w-full overflow-x-auto rounded-md border border-slate-700 bg-slate-900 px-2.5 py-2 font-mono text-[11px] leading-5 text-slate-100">
                {formatJsonBody(selected.request_body)}
              </pre>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Response Body</div>
              <pre className="mt-2 max-w-full overflow-x-auto rounded-md border border-slate-700 bg-slate-900 px-2.5 py-2 font-mono text-[11px] leading-5 text-slate-100">
                {formatJsonBody(selected.response_body)}
              </pre>
            </div>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
