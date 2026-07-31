import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { useAuditsQuery } from '../features/audits/hooks'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { FilterBar } from '../components/filters/FilterBar'
import { PaginationBar } from '../components/pagination/PaginationBar'
import { deriveAgentDeliverySummary } from '../features/agents/derived'
import { PageContainer } from '../layouts/PageContainer'
import { useAgentsQuery } from '../features/agents/hooks'
import { usePoliciesQuery } from '../features/policies/hooks'
import type { AgentView } from '../features/agents/types'
import { formatDateTime, toPercent } from '../lib/format'
import { orderAgentsByRecentActivity } from '../lib/tableOrdering'

type ActivityWindow = 'all' | '24h' | '7d' | '30d'

const ACTIVITY_WINDOW_DAYS: Record<Exclude<ActivityWindow, 'all'>, number> = {
  '24h': 1,
  '7d': 7,
  '30d': 30,
}

function paginate<T>(items: T[], page: number, pageSize: number) {
  const start = (page - 1) * pageSize
  return items.slice(start, start + pageSize)
}

function agentActivityLabel(activity: AgentView['last_activity']) {
  if (!activity) return 'Registered'
  if (activity.activity_kind === 'inbox_pulled') return 'Inbox checked'
  if (activity.activity_kind === 'claim_uploaded') return 'Claim uploaded'
  return 'Dispute reported'
}

function agentActivityDetail(activity: NonNullable<AgentView['last_activity']>) {
  const repeatedKindPrefix = `${activity.activity_kind} `
  if (!activity.summary.startsWith(repeatedKindPrefix)) return activity.summary
  return activity.summary.slice(repeatedKindPrefix.length).trim()
}

function isActiveWithin(agent: AgentView, window: Exclude<ActivityWindow, 'all'>, now: number) {
  if (!agent.last_activity) return false
  const occurredAt = Date.parse(agent.last_activity.occurred_at)
  if (!Number.isFinite(occurredAt)) return false
  return occurredAt >= now - ACTIVITY_WINDOW_DAYS[window] * 24 * 60 * 60 * 1000
}

function agentHealth(agent: AgentView) {
  return toPercent(agent.active_claims, Math.max(agent.mirror_claims, 1))
}

function agentStatus(agent: AgentView): 'live' | 'lagging' | 'idle' {
  if (!agent.last_activity) return 'idle'
  const health = agentHealth(agent)
  if (health >= 85) return 'live'
  if (health >= 60) return 'lagging'
  return 'idle'
}

function agentStatusTone(status: ReturnType<typeof agentStatus>) {
  if (status === 'live') return 'success'
  if (status === 'lagging') return 'warning'
  return 'neutral'
}

function agentIdFromRouteState(state: unknown) {
  if (!state || typeof state !== 'object') return null
  const agentId = (state as { agentId?: unknown }).agentId
  return typeof agentId === 'string' && agentId ? agentId : null
}

export function AgentsPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const routeAgentId = agentIdFromRouteState(location.state)
  const { data = [], isLoading, error } = useAgentsQuery()
  const { data: audits = [] } = useAuditsQuery()
  const { data: policyRecords } = usePoliciesQuery()
  const [selectedAgentId, setSelectedAgentId] = useState<string | null>(() => routeAgentId)
  const [keyword, setKeyword] = useState('')
  const [status, setStatus] = useState('all')
  const [activityWindow, setActivityWindow] = useState<ActivityWindow>('all')
  const [activityNow, setActivityNow] = useState(Date.now)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)

  useEffect(() => {
    if (!routeAgentId) return
    navigate(location.pathname + location.search, { replace: true, state: null })
  }, [location.pathname, location.search, navigate, routeAgentId])

  useEffect(() => {
    const interval = window.setInterval(() => setActivityNow(Date.now()), 60_000)
    return () => window.clearInterval(interval)
  }, [])

  const filtered = useMemo(
    () =>
      orderAgentsByRecentActivity(
        data.filter((agent) => {
          if (keyword && !agent.agent_id.toLowerCase().includes(keyword.toLowerCase())) return false
          const derivedStatus = agentStatus(agent)
          if (status !== 'all' && derivedStatus !== status) return false
          if (
            activityWindow !== 'all' &&
            !isActiveWithin(agent, activityWindow, activityNow)
          ) {
            return false
          }
          return true
        }),
      ),
    [activityNow, activityWindow, data, keyword, status],
  )

  const selected =
    filtered.find((agent) => agent.agent_id === selectedAgentId) ??
    data.find((agent) => agent.agent_id === selectedAgentId) ??
    null
  const pagedRows = paginate(filtered, page, pageSize)
  const deliverySummaryByAgent = useMemo(
    () =>
      new Map(
        data.map((agent) => [
          agent.agent_id,
          deriveAgentDeliverySummary({
            agentId: agent.agent_id,
            outbox: policyRecords?.outbox ?? [],
            policies: policyRecords?.policies ?? [],
            sendLog: policyRecords?.send_log ?? [],
            audits,
          }),
        ]),
      ),
    [audits, data, policyRecords?.outbox, policyRecords?.policies, policyRecords?.send_log],
  )

  if (isLoading) return <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm text-slate-500">Loading agents…</div>
  if (error) return <div className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">{String(error)}</div>

  return (
    <PageContainer title="Agents" subtitle="Inspect registered agents, coordination state, activity, and claim propagation health.">
      <FilterBar>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Agent Search</span>
          <input
            className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none"
            placeholder="agent id"
            value={keyword}
            onChange={(event) => {
              setKeyword(event.target.value)
              setPage(1)
            }}
          />
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Status</span>
          <select
            className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none"
            value={status}
            onChange={(event) => {
              setStatus(event.target.value)
              setPage(1)
            }}
          >
            <option value="all">All Status</option>
            <option value="live">Live</option>
            <option value="lagging">Lagging</option>
            <option value="idle">Idle</option>
          </select>
        </label>
        <label className="space-y-1 text-xs text-slate-600">
          <span className="font-medium text-slate-700">Activity</span>
          <select
            className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none"
            value={activityWindow}
            onChange={(event) => {
              setActivityNow(Date.now())
              setActivityWindow(event.target.value as ActivityWindow)
              setPage(1)
            }}
          >
            <option value="all">All</option>
            <option value="24h">Active in 24 hours</option>
            <option value="7d">Active in 7 days</option>
            <option value="30d">Active in 30 days</option>
          </select>
        </label>
        <button
          type="button"
          className="self-end rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50"
          onClick={() => {
            setKeyword('')
            setStatus('all')
            setActivityWindow('all')
            setPage(1)
          }}
        >
          Reset
        </button>
      </FilterBar>

      <section className="space-y-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold tracking-tight text-slate-900">Registered Agents</h2>
          <StatusBadge tone="info">{filtered.length}</StatusBadge>
        </div>
        <DataTable
          columns={[
            {
              key: 'agent',
              header: 'Agent',
              render: (row: AgentView) => <div className="font-mono text-xs font-medium text-slate-900">{row.agent_id}</div>,
            },
            {
              key: 'status',
              header: 'Status',
              render: (row: AgentView) => (
                <StatusBadge tone={agentStatusTone(agentStatus(row))}>{agentStatus(row)}</StatusBadge>
              ),
            },
            {
              key: 'claims',
              header: 'Claims',
              render: (row: AgentView) => <span className="font-mono text-xs">{row.mirror_claims} total · {row.active_claims} active</span>,
            },
            {
              key: 'inbox',
              header: 'Inbox',
              render: (row: AgentView) => {
                const summary = deliverySummaryByAgent.get(row.agent_id)
                if (!summary || (summary.deliveredCount === 0 && summary.openCount === 0)) {
                  return <span className="text-xs text-slate-500">No delivery</span>
                }
                return <span className="font-mono text-xs">{summary.deliveredCount} received · {summary.openCount} pending</span>
              },
            },
            { key: 'lastIp', header: 'Last IP', render: (row: AgentView) => <span className="font-mono text-[11px] text-slate-500">{row.last_source_ip ?? 'Unavailable'}</span> },
            {
              key: 'lastActivity',
              header: 'Last Activity',
              render: (row: AgentView) => (
                <div>
                  <div className="text-xs font-medium text-slate-800">
                    {agentActivityLabel(row.last_activity)}
                  </div>
                  <div className="mt-0.5 font-mono text-[11px] text-slate-500">
                    {row.last_activity
                      ? formatDateTime(row.last_activity.occurred_at)
                      : 'No activity recorded'}
                  </div>
                </div>
              ),
            },
            {
              key: 'health',
              header: 'Health',
              render: (row: AgentView) => (
                <div className="min-w-24">
                  <div className="h-1 overflow-hidden rounded-full bg-slate-100">
                    <div className="h-full rounded-full bg-blue-600" style={{ width: `${agentHealth(row)}%` }} />
                  </div>
                  <div className="mt-1 font-mono text-[11px] text-slate-500">{agentHealth(row)}%</div>
                </div>
              ),
            },
            { key: 'actions', header: 'Actions', render: () => <span className="text-xs font-medium text-blue-700">View</span> },
          ]}
          rows={pagedRows}
          getRowId={(row) => row.agent_id}
          onRowClick={(row) => setSelectedAgentId(row.agent_id)}
          emptyState="No agents matched the current filters."
        />
        <PaginationBar
          page={page}
          pageSize={pageSize}
          total={filtered.length}
          onPageChange={setPage}
          onPageSizeChange={(size) => {
            setPageSize(size)
            setPage(1)
          }}
        />
      </section>

      <DetailDrawer
        modal={false}
        size="default"
        open={Boolean(selected)}
        onClose={() => setSelectedAgentId(null)}
        label="Agent"
        title={selected?.agent_id ?? 'Agent'}
        subtitle={selected?.last_source_ip ?? 'No source IP observed'}
      >
        {selected ? (
          <>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Overview</div>
              <dl className="mt-2 space-y-2 text-sm">
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge tone={agentStatusTone(agentStatus(selected))}>{agentStatus(selected)}</StatusBadge></dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Mirror Claims</dt><dd className="font-mono text-xs text-slate-900">{selected.mirror_claims}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Active Claims</dt><dd className="font-mono text-xs text-slate-900">{selected.active_claims}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Stale Claims</dt><dd className="font-mono text-xs text-slate-900">{selected.stale_claims}</dd></div>
                <div className="flex justify-between gap-3"><dt className="text-slate-500">Health</dt><dd className="font-mono text-xs text-slate-900">{agentHealth(selected)}%</dd></div>
              </dl>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Recent Activities</div>
              <div className="mt-2 space-y-2">
                {selected.recent_activities.length ? (
                  selected.recent_activities.map((activity) => {
                    const detail = agentActivityDetail(activity)
                    return (
                      <div key={activity.event_id} className="rounded border border-slate-100 bg-slate-50 px-2.5 py-2">
                        <div className="text-xs font-medium text-slate-900">{agentActivityLabel(activity)}</div>
                        <div className="mt-0.5 font-mono text-[11px] text-slate-500">{formatDateTime(activity.occurred_at)}</div>
                        {detail ? <div className="mt-1 text-xs text-slate-600">{detail}</div> : null}
                      </div>
                    )
                  })
                ) : (
                  <div className="text-xs text-slate-500">Registered, but no activity has been recorded yet.</div>
                )}
              </div>
            </div>
            <div className="rounded-lg border border-slate-200 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Delivery State</div>
              {(() => {
                const summary = deliverySummaryByAgent.get(selected.agent_id)
                return (
                  <dl className="mt-2 space-y-2 text-sm">
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">Durably Received</dt><dd className="font-mono text-xs text-slate-900">{summary?.deliveredCount ?? 0}</dd></div>
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">Open Outbox Entries</dt><dd className="font-mono text-xs text-slate-900">{summary?.openCount ?? 0}</dd></div>
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">Recent Pulls</dt><dd className="font-mono text-xs text-slate-900">{summary?.recentPullCount ?? 0}</dd></div>
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">Last Pull</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(summary?.lastPullAt)}</dd></div>
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">Last Receipt ACK</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(summary?.lastDelivery)}</dd></div>
                  </dl>
                )
              })()}
            </div>
          </>
        ) : null}
      </DetailDrawer>
    </PageContainer>
  )
}
