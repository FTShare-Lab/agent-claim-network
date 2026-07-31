import { type ReactNode, useEffect, useMemo, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { PageContainer } from '../layouts/PageContainer'
import { useOverviewQuery } from '../features/overview/hooks'
import { useSweepsQuery, useTriggerSweepMutation } from '../features/sweeps/hooks'
import type { SweepRunRecord } from '../features/sweeps/types'
import { formatDateTime, formatRelativeMinutes } from '../lib/format'
import { isStaticDemo } from '../lib/runtime'

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
      className="font-mono text-[11px] font-medium text-blue-700 underline-offset-2 transition hover:text-blue-900 hover:underline"
    >
      {id}
    </Link>
  )
}

function PolicyLink({ id }: { id: string }) {
  return (
    <Link
      to={`/policies?policy_id=${encodeURIComponent(id)}`}
      className="font-mono text-[11px] font-medium text-blue-700 underline-offset-2 transition hover:text-blue-900 hover:underline"
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
        <span key={id} className="rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5">
          <ClaimLink id={id} />
        </span>
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

function CandidateTable({
  rows,
  empty,
}: {
  rows: Array<[string, string]>
  empty: string
}) {
  if (!rows.length) return <div className="text-xs text-slate-500">{empty}</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[440px] text-left text-xs">
        <thead className="border-b border-slate-200 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-1.5 pr-4">Agent</th>
            <th className="py-1.5 pr-4">Claim</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.map(([agentId, claimId]) => (
            <tr key={`${agentId}-${claimId}`}>
              <td className="py-1.5 pr-4 font-mono text-slate-900">{agentId}</td>
              <td className="py-1.5 pr-4"><ClaimLink id={claimId} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function NotificationTable({ run }: { run: SweepRunRecord }) {
  const notifications = notificationsOf(run)
  if (!notifications.length) return <div className="text-xs text-slate-500">No ClaimAttributeUpdate notifications were sent.</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[760px] text-left text-xs">
        <thead className="border-b border-slate-200 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-1.5 pr-4">Agent</th>
            <th className="py-1.5 pr-4">Stale Claims</th>
            <th className="py-1.5 pr-4">Deprecated Claims</th>
            <th className="py-1.5 pr-4">Policy</th>
            <th className="py-1.5 pr-4">Pushed</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {notifications.map((row) => (
            <tr key={`${row.agent_id}-${row.policy_id}`}>
              <td className="py-1.5 pr-4 font-mono text-slate-900">{row.agent_id}</td>
              <td className="py-1.5 pr-4"><ClaimList ids={row.stale_claims} /></td>
              <td className="py-1.5 pr-4"><ClaimList ids={row.deprecated_claims} /></td>
              <td className="py-1.5 pr-4"><PolicyLink id={row.policy_id} /></td>
              <td className="py-1.5 pr-4 font-mono text-slate-700">{row.pushed}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function NotificationErrorTable({ run }: { run: SweepRunRecord }) {
  const errors = notificationErrorsOf(run)
  if (!errors.length) return <div className="text-xs text-slate-500">No notification errors were recorded for this run.</div>
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[760px] text-left text-xs">
        <thead className="border-b border-slate-200 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-1.5 pr-4">Agent</th>
            <th className="py-1.5 pr-4">Stale Claims</th>
            <th className="py-1.5 pr-4">Deprecated Claims</th>
            <th className="py-1.5 pr-4">Error</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {errors.map((row) => (
            <tr key={`${row.agent_id}-${row.error}`}>
              <td className="py-1.5 pr-4 font-mono text-slate-900">{row.agent_id}</td>
              <td className="py-1.5 pr-4"><ClaimList ids={row.stale_claims} /></td>
              <td className="py-1.5 pr-4"><ClaimList ids={row.deprecated_claims} /></td>
              <td className="py-1.5 pr-4 text-[11px] leading-5 text-rose-700">{row.error}</td>
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
  const location = useLocation()
  const navigate = useNavigate()
  const routeRunId = runIdFromRouteState(location.state)
  const { data = [], isLoading, error } = useSweepsQuery()
  const { data: overviewData } = useOverviewQuery()
  const triggerSweep = useTriggerSweepMutation()
  const [selectedRunId, setSelectedRunId] = useState<string | null>(() => routeRunId)

  useEffect(() => {
    if (!routeRunId) return
    navigate(location.pathname + location.search, { replace: true, state: null })
  }, [location.pathname, location.search, navigate, routeRunId])

  const latest = data[0] ?? null
  const selectedRun = useMemo(
    () => data.find((run) => run.run_id === selectedRunId) ?? null,
    [data, selectedRunId],
  )
  const latestFindingCount = useMemo(() => findingCount(latest), [latest])
  const nextSweepAt = overviewData?.sweep_schedule?.next_sweep_at ?? null

  if (isLoading) return <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm text-slate-500">Loading sweep history...</div>
  if (error) return <div className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">{String(error)}</div>

  return (
    <PageContainer
      title="Sweep"
      subtitle="Review claim aging findings, maintainer notifications, and retryable notification failures."
      actions={
        <button
          type="button"
          className="rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-blue-800 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200"
          disabled={isStaticDemo || triggerSweep.isPending}
          title={isStaticDemo ? 'Static preview is read-only' : undefined}
          onClick={() => triggerSweep.mutate()}
        >
          {isStaticDemo ? 'Trigger Sweep (read-only)' : triggerSweep.isPending ? 'Running Sweep...' : 'Trigger Sweep Now'}
        </button>
      }
    >
      <section className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2.5 text-xs text-slate-600">
        Sweep sends ClaimAttributeUpdate suggestions, but claim status changes remain owned by each agent.
      </section>

      <div className="grid gap-2.5 xl:grid-cols-6">
        {[
          ['Last Sweep', latest ? formatDateTime(latest.triggered_at) : 'N/A', latest ? formatRelativeMinutes(latest.triggered_at) : 'No runs'],
          ['Next Sweep (ETA)', nextSweepAt ? formatDateTime(nextSweepAt) : 'N/A', nextSweepAt ? formatRelativeMinutes(nextSweepAt) : 'not scheduled'],
          ['Total Findings', latestFindingCount, 'latest run'],
          ['Stale Claims', latest?.report.stale_claims.length ?? 0, 'current run'],
          ['Deprecated Claims', latest?.report.deprecated_claims.length ?? 0, 'current run'],
          ['Notified Agents', notificationsOf(latest).length, `${notificationErrorsOf(latest).length} errors`],
        ].map(([label, value, detail]) => (
          <div key={label as string} className="rounded-md border border-slate-200 bg-white px-3 py-2.5">
            <div className="text-[11px] text-slate-500">{label}</div>
            <div className="mt-1 font-mono text-lg font-semibold tracking-tight text-slate-900">{value as string | number}</div>
            <div className="mt-0.5 text-[11px] text-slate-500">{detail as string}</div>
          </div>
        ))}
      </div>

      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold tracking-tight text-slate-900">Sweep History</h2>
          <StatusBadge tone="info">{data.length}</StatusBadge>
        </div>
        <DataTable
          columns={[
            { key: 'triggeredAt', header: 'Triggered At', render: (row: SweepRunRecord) => <span className="font-mono text-xs">{formatDateTime(row.triggered_at)}</span> },
            { key: 'trigger', header: 'Triggered By', render: (row: SweepRunRecord) => <span className="font-mono text-xs">{row.trigger}</span> },
            {
              key: 'findings',
              header: 'Findings',
              render: (row: SweepRunRecord) => <span className="font-mono text-xs">{findingCount(row)}</span>,
            },
            { key: 'stale', header: 'Stale Claims', render: (row: SweepRunRecord) => <span className="font-mono text-xs">{row.report.stale_claims.length}</span> },
            { key: 'deprecated', header: 'Deprecated Claims', render: (row: SweepRunRecord) => <span className="font-mono text-xs">{row.report.deprecated_claims.length}</span> },
            { key: 'notifications', header: 'Notifications', render: (row: SweepRunRecord) => <span className="font-mono text-xs">{notificationsOf(row).length}</span> },
            {
              key: 'status',
              header: 'Status',
              render: (row: SweepRunRecord) =>
                notificationErrorsOf(row).length ? (
                  <StatusBadge tone="warning">retryable errors</StatusBadge>
                ) : (
                  <StatusBadge tone="success">recorded</StatusBadge>
                ),
            },
          ]}
          rows={data}
          getRowId={(row) => row.run_id}
          onRowClick={(row) => setSelectedRunId(row.run_id)}
          emptyState="No sweep history has been recorded yet."
        />
      </section>

      <DetailDrawer
        modal={false}
        open={Boolean(selectedRun)}
        size="default"
        onClose={() => setSelectedRunId(null)}
        label="Claim Sweep"
        title={selectedRun ? formatDateTime(selectedRun.triggered_at) : 'Sweep'}
        subtitle={selectedRun?.run_id}
      >
        {selectedRun ? (
          <>
            <DrawerSection title="Run Summary">
              <dl className="grid gap-2 text-sm md:grid-cols-2">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Run ID</dt><dd className="font-mono text-[11px] text-slate-600">{selectedRun.run_id}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Trigger</dt><dd className="font-mono text-xs text-slate-900">{selectedRun.trigger}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Triggered At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedRun.triggered_at)}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Findings</dt><dd className="font-mono text-xs text-slate-900">{findingCount(selectedRun)}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Notifications</dt><dd className="font-mono text-xs text-slate-900">{notificationsOf(selectedRun).length}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Errors</dt><dd className="font-mono text-xs text-slate-900">{notificationErrorsOf(selectedRun).length}</dd></div>
              </dl>
            </DrawerSection>

            <div className="grid gap-2.5 xl:grid-cols-2">
              <DrawerSection title="Stale Candidates">
                <CandidateTable rows={selectedRun.report.stale_claims} empty="No stale candidates in this run." />
              </DrawerSection>
              <DrawerSection title="Deprecated Candidates">
                <CandidateTable rows={selectedRun.report.deprecated_claims} empty="No deprecated candidates in this run." />
              </DrawerSection>
            </div>

            <DrawerSection title="Sweep Notifications">
              <NotificationTable run={selectedRun} />
            </DrawerSection>

            <DrawerSection title="Notification Errors">
              <NotificationErrorTable run={selectedRun} />
            </DrawerSection>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
