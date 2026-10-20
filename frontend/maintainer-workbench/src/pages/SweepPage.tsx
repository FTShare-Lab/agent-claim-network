import { useT, useLanguage } from '../i18n'
import { type ReactNode, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { FilterBar } from '../components/filters/FilterBar'
import { PaginationBar } from '../components/pagination/PaginationBar'
import { PageContainer } from '../layouts/PageContainer'
import { useOverviewQuery } from '../features/overview/hooks'
import { useSweepsQuery, useTriggerSweepMutation } from '../features/sweeps/hooks'
import type { SweepRunRecord } from '../features/sweeps/types'
import { SweepClaimComparison } from '../features/sweeps/SweepClaimComparison'
import { formatDateTime, formatRelativeMinutes } from '../lib/format'
import { isStaticDemo } from '../lib/runtime'

type SweepTimeRange = 'all' | '24h' | '7d' | '30d'
type SweepTriggerFilter = 'all' | SweepRunRecord['trigger']

const SWEEP_TIME_RANGE_DAYS: Record<Exclude<SweepTimeRange, 'all'>, number> = {
  '24h': 1,
  '7d': 7,
  '30d': 30,
}

function paginate<T>(items: T[], page: number, pageSize: number) {
  const start = (page - 1) * pageSize
  return items.slice(start, start + pageSize)
}

function isWithinTimeRange(run: SweepRunRecord, range: Exclude<SweepTimeRange, 'all'>, now: number) {
  const triggeredAt = Date.parse(run.triggered_at)
  if (!Number.isFinite(triggeredAt)) return false
  return triggeredAt >= now - SWEEP_TIME_RANGE_DAYS[range] * 24 * 60 * 60 * 1000
}

function triggerLabel(trigger: SweepRunRecord['trigger']) {
  if (trigger === 'manual') return 'Manual'
  if (trigger === 'maintainer_startup') return 'Maintainer startup'
  return 'Scheduled ticker'
}

function notificationsOf(run: SweepRunRecord | null) {
  return run?.report.notifications ?? []
}

function notificationErrorsOf(run: SweepRunRecord | null) {
  return run?.report.notification_errors ?? []
}

function findingCount(run: SweepRunRecord | null) {
  if (!run) return 0
  return run.report.stale_claims.length + run.report.deprecated_claims.length
}

function ClaimLink({ id }: { id: string }) {
  return (
    <Link
      to={`/claims?claim_id=${encodeURIComponent(id)}`}
      className="inline-flex max-w-full items-center rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 font-mono text-[11px] font-medium leading-4 text-blue-700 underline-offset-2 transition hover:border-blue-300 hover:bg-blue-100 hover:text-blue-900"
    >
      {id}
    </Link>
  )
}

function PolicyLink({ id }: { id: string }) {
  return (
    <Link
      to={`/policies?policy_id=${encodeURIComponent(id)}`}
      className="inline-flex max-w-full items-center rounded border border-blue-200 bg-blue-50 px-1.5 py-0.5 font-mono text-[11px] font-medium leading-4 text-blue-700 underline-offset-2 transition hover:border-blue-300 hover:bg-blue-100 hover:text-blue-900"
    >
      {id}
    </Link>
  )
}

function ClaimList({ ids }: { ids: string[] }) {
  if (!ids.length) return <span className="font-mono text-[11px] text-slate-500">[]</span>
  return (
    <div className="flex flex-wrap gap-1">
      {ids.map((id) => (
        <ClaimLink key={id} id={id} />
      ))}
    </div>
  )
}

function DrawerSection({
  title,
  children,
}: {
  title: string
  children: ReactNode
}) {
  return (
    <section className="rounded-lg border border-slate-200 bg-white p-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{title}</div>
      <div className="mt-2">{children}</div>
    </section>
  )
}

function CandidateSection({ title, children }: { title: string; children: ReactNode }) {
  const t = useT()
  const [expanded, setExpanded] = useState(false)
  const contentId = useId()
  return (
    <section className="rounded-lg border border-slate-200 bg-white p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{title}</div>
        <button type="button" aria-expanded={expanded} aria-controls={contentId}
          aria-label={`${expanded ? t("Collapse") : t("Expand")} ${title}`}
          onClick={() => setExpanded(!expanded)}
          className="rounded-md border border-slate-200 px-3 py-1.5 text-xs font-semibold text-blue-700 transition hover:bg-blue-50">
          {expanded ? t("Collapse") : t("Expand")}
        </button>
      </div>
      <div id={contentId} hidden={!expanded} className="mt-2">{expanded ? children : null}</div>
    </section>
  )
}

// 通知行可因 Claim 标签换行而增高，按实际前五行高度限制滚动区域。
function FiveRowTable({ children }: { children: ReactNode }) {
  const container = useRef<HTMLDivElement>(null)
  const [maxHeight, setMaxHeight] = useState<number>()
  useLayoutEffect(() => {
    const table = container.current?.querySelector('table')
    if (!table) return
    const measure = () => {
      const rows = table.querySelectorAll('tbody tr')
      const scrollbarHeight = container.current ? container.current.offsetHeight - container.current.clientHeight : 0
      setMaxHeight(rows.length > 5 ? rows[4].getBoundingClientRect().bottom - table.getBoundingClientRect().top + scrollbarHeight : undefined)
    }
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(table)
    return () => observer.disconnect()
  }, [children])
  return <div ref={container} style={{ maxHeight }} className="overflow-auto overscroll-contain">{children}</div>
}

function CandidateTable({
  rows,
  empty,
}: {
  rows: Array<[string, string]>
  empty: string
}) {
  const t = useT()
  if (!rows.length) return <div className="text-xs text-slate-500">{empty}</div>
  return (
    <FiveRowTable>
      <table className="w-full min-w-[440px] text-left text-xs leading-4">
        <thead className="border-b border-slate-200 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
          <tr>
            <th className="align-middle py-2 pr-4">{t("Agent")}</th>
            <th className="align-middle py-2 pr-4">{t("Claim")}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.map(([agentId, claimId]) => (
            <tr key={`${agentId}-${claimId}`}>
              <td className="align-middle py-2 pr-4 font-mono text-slate-900">{agentId}</td>
              <td className="align-middle py-2 pr-4"><ClaimLink id={claimId} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </FiveRowTable>
  )
}

function NotificationTable({ run }: { run: SweepRunRecord }) {
  const t = useT()
  const notifications = notificationsOf(run)
  if (!notifications.length) return <div className="text-xs text-slate-500">{t("No ClaimAttributeUpdate notifications were sent.")}</div>
  return (
    <FiveRowTable>
      <table className="w-full min-w-[760px] table-fixed text-left text-xs leading-4">
        <colgroup><col className="w-[18%]" /><col className="w-[25%]" /><col className="w-[25%]" /><col className="w-[24%]" /><col className="w-[8%]" /></colgroup>
        <thead className="border-b border-slate-200 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
          <tr>
            <th className="align-middle py-2 pr-4">{t("Agent")}</th>
            <th className="align-middle py-2 pr-4">{t("Stale Claims")}</th>
            <th className="align-middle py-2 pr-4">{t("Deprecated Claims")}</th>
            <th className="align-middle py-2 pr-4">{t("Policy")}</th>
            <th className="align-middle py-2 pr-4">{t("Pushed")}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {notifications.map((row) => (
            <tr key={`${row.agent_id}-${row.policy_id}`}>
              <td className="align-middle break-words py-2 pr-4 font-mono text-slate-900">{row.agent_id}</td>
              <td className="align-middle py-2 pr-4"><ClaimList ids={row.stale_claims} /></td>
              <td className="align-middle py-2 pr-4"><ClaimList ids={row.deprecated_claims} /></td>
              <td className="align-middle py-2 pr-4"><PolicyLink id={row.policy_id} /></td>
              <td className="align-middle py-2 pr-4 font-mono text-slate-700">{row.pushed}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </FiveRowTable>
  )
}

function NotificationErrorTable({ run }: { run: SweepRunRecord }) {
  const t = useT()
  const errors = notificationErrorsOf(run)
  if (!errors.length) return <div className="text-xs text-slate-500">{t("No notification errors were recorded for this run.")}</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[760px] text-left text-xs">
        <thead className="border-b border-slate-200 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
          <tr>
            <th className="align-middle py-2 pr-4">{t("Agent")}</th>
            <th className="align-middle py-2 pr-4">{t("Stale Claims")}</th>
            <th className="align-middle py-2 pr-4">{t("Deprecated Claims")}</th>
            <th className="align-middle py-2 pr-4">{t("Error")}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {errors.map((row) => (
            <tr key={`${row.agent_id}-${row.error}`}>
              <td className="align-middle py-2 pr-4 font-mono text-slate-900">{row.agent_id}</td>
              <td className="align-middle py-2 pr-4"><ClaimList ids={row.stale_claims} /></td>
              <td className="align-middle py-2 pr-4"><ClaimList ids={row.deprecated_claims} /></td>
              <td className="align-middle py-2 pr-4 text-[11px] leading-5 text-rose-700">{row.error}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function runIdFromRouteState(state: unknown) {
  if (!state || typeof state !== 'object') return null
  const runId = (state as { runId?: unknown }).runId
  return typeof runId === 'string' && runId ? runId : null
}

export function SweepPage() {
  const t = useT()
  const language = useLanguage()
  const location = useLocation()
  const navigate = useNavigate()
  const routeRunId = runIdFromRouteState(location.state)
  const { data = [], isLoading, error } = useSweepsQuery()
  const { data: overviewData } = useOverviewQuery()
  const triggerSweep = useTriggerSweepMutation()
  const [selectedRunId, setSelectedRunId] = useState<string | null>(() => routeRunId)
  const [timeRange, setTimeRange] = useState<SweepTimeRange>('all')
  const [triggerFilter, setTriggerFilter] = useState<SweepTriggerFilter>('all')
  const [filterNow, setFilterNow] = useState(Date.now)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)

  useEffect(() => {
    if (!routeRunId) return
    navigate(location.pathname + location.search, { replace: true, state: null })
  }, [location.pathname, location.search, navigate, routeRunId])

  useEffect(() => {
    const interval = window.setInterval(() => setFilterNow(Date.now()), 60_000)
    return () => window.clearInterval(interval)
  }, [])

  const latest = data[0] ?? null
  const selectedRun = useMemo(
    () => data.find((run) => run.run_id === selectedRunId) ?? null,
    [data, selectedRunId],
  )
  const filteredRuns = useMemo(
    () => data.filter((run) => {
      if (timeRange !== 'all' && !isWithinTimeRange(run, timeRange, filterNow)) return false
      if (triggerFilter !== 'all' && run.trigger !== triggerFilter) return false
      return true
    }),
    [data, filterNow, timeRange, triggerFilter],
  )
  const pagedRuns = paginate(filteredRuns, page, pageSize)
  const latestFindingCount = useMemo(() => findingCount(latest), [latest])
  const nextSweepAt = overviewData?.sweep_schedule?.next_sweep_at ?? null

  if (isLoading) return <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm text-slate-500">{t("Loading sweep history...")}</div>
  if (error) return <div className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">{String(error)}</div>

  return (
    <PageContainer
      title={t("Sweep")}
      subtitle={t("Review claim aging findings, maintainer notifications, and retryable notification failures.")}
      actions={
        <button
          type="button"
          className="rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-blue-800 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200"
          disabled={isStaticDemo || triggerSweep.isPending}
          title={isStaticDemo ? t("Static preview is read-only") : undefined}
          onClick={() => triggerSweep.mutate()}
        >
          {isStaticDemo ? t("Trigger Sweep (read-only)") : triggerSweep.isPending ? t("Running Sweep...") : t("Trigger Sweep Now")}
        </button>
      }
    >
      <section className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2.5 text-xs text-slate-600">{t("Sweep sends ClaimAttributeUpdate suggestions, but claim status changes remain owned by each agent.")}</section>

      <div className="grid gap-2.5 xl:grid-cols-6">
        {[
          [t("Last Sweep"), latest ? formatDateTime(latest.triggered_at) : t("N/A"), latest ? formatRelativeMinutes(latest.triggered_at, language) : t("No runs")],
          [t("Next Sweep (ETA)"), nextSweepAt ? formatDateTime(nextSweepAt) : t("N/A"), nextSweepAt ? formatRelativeMinutes(nextSweepAt, language) : t("not scheduled")],
          [t("Total Findings"), latestFindingCount, t("latest run")],
          [t("Stale Claims"), latest?.report.stale_claims.length ?? 0, t("current run")],
          [t("Deprecated Claims"), latest?.report.deprecated_claims.length ?? 0, t("current run")],
          [t("Notified Agents"), notificationsOf(latest).length, t('{count} errors', { count: notificationErrorsOf(latest).length })],
        ].map(([label, value, detail]) => (
          <div key={label as string} className="rounded-md border border-slate-200 bg-white px-3 py-2.5">
            <div className="text-[11px] text-slate-500">{label}</div>
            <div className="mt-1 font-mono text-lg font-semibold tracking-tight text-slate-900">{value as string | number}</div>
            <div className="mt-0.5 text-[11px] text-slate-500">{detail as string}</div>
          </div>
        ))}
      </div>

      <FilterBar>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">{t("Time Range")}</span>
          <select
            className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none"
            value={timeRange}
            onChange={(event) => {
              setFilterNow(Date.now())
              setTimeRange(event.target.value as SweepTimeRange)
              setPage(1)
            }}
          >
            <option value="all">{t("All Time")}</option>
            <option value="24h">{t("Last 24 hours")}</option>
            <option value="7d">{t("Last 7 days")}</option>
            <option value="30d">{t("Last 30 days")}</option>
          </select>
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">{t("Triggered By")}</span>
          <select
            className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none"
            value={triggerFilter}
            onChange={(event) => {
              setTriggerFilter(event.target.value as SweepTriggerFilter)
              setPage(1)
            }}
          >
            <option value="all">{t("All Triggers")}</option>
            <option value="manual">{t("Manual")}</option>
            <option value="maintainer_startup">{t("Maintainer startup")}</option>
            <option value="ticker">{t("Scheduled ticker")}</option>
          </select>
        </label>
        <button
          type="button"
          className="self-end rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50"
          onClick={() => {
            setFilterNow(Date.now())
            setTimeRange('all')
            setTriggerFilter('all')
            setPage(1)
          }}
        >{t("Reset")}</button>
      </FilterBar>

      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold tracking-tight text-slate-900">{t("Sweep History")}</h2>
          <StatusBadge tone="info">{filteredRuns.length}</StatusBadge>
        </div>
        <DataTable
          columns={[
            { key: 'triggeredAt', header: t("Triggered At"), render: (row: SweepRunRecord) => <span className="font-mono text-xs">{formatDateTime(row.triggered_at)}</span> },
            { key: 'trigger', header: t("Triggered By"), render: (row: SweepRunRecord) => <span className="text-xs font-medium text-slate-700">{t(triggerLabel(row.trigger))}</span> },
            {
              key: 'findings',
              header: t("Findings"),
              render: (row: SweepRunRecord) => <span className="font-mono text-xs">{findingCount(row)}</span>,
            },
            { key: 'stale', header: t("Stale Claims"), render: (row: SweepRunRecord) => <span className="font-mono text-xs">{row.report.stale_claims.length}</span> },
            { key: 'deprecated', header: t("Deprecated Claims"), render: (row: SweepRunRecord) => <span className="font-mono text-xs">{row.report.deprecated_claims.length}</span> },
            { key: 'notifications', header: t("Notifications"), render: (row: SweepRunRecord) => <span className="font-mono text-xs">{notificationsOf(row).length}</span> },
            {
              key: 'status',
              header: t("Status"),
              render: (row: SweepRunRecord) =>
                notificationErrorsOf(row).length ? (
                  <StatusBadge tone="warning">{t("retryable errors")}</StatusBadge>
                ) : (
                  <StatusBadge tone="success">{t("recorded")}</StatusBadge>
                ),
            },
          ]}
          rows={pagedRuns}
          getRowId={(row) => row.run_id}
          onRowClick={(row) => setSelectedRunId(row.run_id)}
          emptyState={data.length ? t("No sweep runs matched the current filters.") : t("No sweep history has been recorded yet.")}
        />
        <PaginationBar
          page={page}
          pageSize={pageSize}
          total={filteredRuns.length}
          onPageChange={setPage}
          onPageSizeChange={(size) => {
            setPageSize(size)
            setPage(1)
          }}
        />
      </section>

      <DetailDrawer
        modal={false}
        open={Boolean(selectedRun)}
        onClose={() => setSelectedRunId(null)}
        label={t("Claim Sweep")}
        title={selectedRun ? formatDateTime(selectedRun.triggered_at) : t("Sweep")}
        subtitle={selectedRun?.run_id}
      >
        {selectedRun ? (
          <>
            <DrawerSection title={t("Run Summary")}>
              <dl className="grid gap-2 text-sm md:grid-cols-2">
                <div className="min-w-0 rounded-lg bg-slate-50 px-2.5 py-2"><dt className="text-xs text-slate-500">{t("Run ID")}</dt><dd className="mt-1 break-all font-mono text-[11px] leading-4 text-slate-700">{selectedRun.run_id}</dd></div>
                <div className="min-w-0 rounded-lg bg-slate-50 px-2.5 py-2"><dt className="text-xs text-slate-500">{t("Trigger")}</dt><dd className="mt-1 break-words text-xs font-medium leading-4 text-slate-900">{t(triggerLabel(selectedRun.trigger))}</dd></div>
                <div className="min-w-0 rounded-lg bg-slate-50 px-2.5 py-2"><dt className="text-xs text-slate-500">{t("Triggered At")}</dt><dd className="mt-1 break-words font-mono text-xs text-slate-900">{formatDateTime(selectedRun.triggered_at)}</dd></div>
                <div className="min-w-0 rounded-lg bg-slate-50 px-2.5 py-2"><dt className="text-xs text-slate-500">{t("Findings")}</dt><dd className="mt-1 font-mono text-xs text-slate-900">{findingCount(selectedRun)}</dd></div>
                <div className="min-w-0 rounded-lg bg-slate-50 px-2.5 py-2"><dt className="text-xs text-slate-500">{t("Notifications")}</dt><dd className="mt-1 font-mono text-xs text-slate-900">{notificationsOf(selectedRun).length}</dd></div>
                <div className="min-w-0 rounded-lg bg-slate-50 px-2.5 py-2"><dt className="text-xs text-slate-500">{t("Errors")}</dt><dd className="mt-1 font-mono text-xs text-slate-900">{notificationErrorsOf(selectedRun).length}</dd></div>
              </dl>
            </DrawerSection>

            <div className="grid gap-2.5" key={selectedRun.run_id}>
              <CandidateSection title={t("Stale Candidates")}>
                <CandidateTable rows={selectedRun.report.stale_claims} empty="No stale candidates in this run." />
              </CandidateSection>
              <CandidateSection title={t("Deprecated Candidates")}>
                <CandidateTable rows={selectedRun.report.deprecated_claims} empty="No deprecated candidates in this run." />
              </CandidateSection>
            </div>

            <DrawerSection title={t("Sweep Notifications")}>
              <NotificationTable run={selectedRun} />
            </DrawerSection>

            <DrawerSection title={t("Suggestions & Current Claims")}>
              <SweepClaimComparison key={selectedRun.run_id} run={selectedRun} />
            </DrawerSection>

            <DrawerSection title={t("Notification Errors")}>
              <NotificationErrorTable run={selectedRun} />
            </DrawerSection>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
