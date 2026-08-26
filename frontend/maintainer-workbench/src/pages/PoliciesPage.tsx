import { type ReactNode, useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate, useSearchParams } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { DetailDrawer } from '../components/drawer/DetailDrawer'
import { DetailTextBlock } from '../components/drawer/DetailTextBlock'
import { PaginationBar } from '../components/pagination/PaginationBar'
import { PolicyComposeDrawer } from '../features/policies/PolicyComposeDrawer'
import { useOverviewQuery } from '../features/overview/hooks'
import type { MaintainerActionRow, Policy, SendLogRow } from '../features/overview/types'
import {
  useDeprecatePolicyMutation,
  usePoliciesQuery,
} from '../features/policies/hooks'
import {
  deliveredCountFromOutbox,
  deliverySummaryText,
  deriveDeliveryStats,
  findPolicyEvent,
  isOutboxEntryOpen,
  lastOfferedAtFromOutbox,
  lastSentAtFromOutbox,
  messageTypeFromOutbox,
  openStateFromOutbox,
  offeredCountFromOutbox,
  policyFromOutbox,
  policyIdFromOutbox,
  relatedPolicies,
  targetKindLabel,
  targetLabelFromAction,
  targetLabelFromOutbox,
} from '../features/policies/derived'
import type { OutboxEntry } from '../features/policies/types'
import { PageContainer } from '../layouts/PageContainer'
import { formatDateTime } from '../lib/format'
import { isStaticDemo } from '../lib/runtime'
import {
  orderActionsByCreatedAt,
  orderOutboxByCreatedAt,
  orderPoliciesByTypeAndRecentChange,
  orderSendLogBySentAt,
} from '../lib/tableOrdering'

function paginate<T>(items: T[], page: number, pageSize: number) {
  const start = (page - 1) * pageSize
  return items.slice(start, start + pageSize)
}

function dateMatchesRange(iso: string, range: string) {
  if (range === 'all') return true
  const days = Number(range)
  if (Number.isNaN(days)) return true
  const then = new Date(iso).getTime()
  const now = Date.now()
  return now - then <= days * 24 * 60 * 60 * 1000
}

function DrawerSection({
  title,
  children,
}: {
  title: string
  children: ReactNode
}) {
  return (
    <div className="rounded-lg border border-slate-200 p-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{title}</div>
      <div className="mt-2">{children}</div>
    </div>
  )
}

type DrawerResource =
  | { type: 'policy'; id: string }
  | { type: 'action'; id: string }
  | { type: 'outbox'; id: string }

function drawerResourceFromRouteState(state: unknown): DrawerResource | null {
  if (!state || typeof state !== 'object') return null
  const { policyId, actionId, outboxId } = state as {
    policyId?: unknown
    actionId?: unknown
    outboxId?: unknown
  }
  if (typeof policyId === 'string' && policyId) {
    return { type: 'policy', id: policyId }
  }
  if (typeof actionId === 'string' && actionId) {
    return { type: 'action', id: actionId }
  }
  if (typeof outboxId === 'string' && outboxId) {
    return { type: 'outbox', id: outboxId }
  }
  return null
}

function drawerResourceKey(resource: DrawerResource | null) {
  return resource ? `${resource.type}:${resource.id}` : null
}

export function PoliciesPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const location = useLocation()
  const navigate = useNavigate()
  const policyIdFromUrl = searchParams.get('policy_id')
  const routeDrawerResource = drawerResourceFromRouteState(location.state)
  const routeDrawerResourceKey = drawerResourceKey(routeDrawerResource)
  const { data, isLoading, error } = usePoliciesQuery()
  const { data: overviewData } = useOverviewQuery()
  const deprecatePolicy = useDeprecatePolicyMutation()
  const [drawerResource, setDrawerResource] = useState<DrawerResource | null>(() => routeDrawerResource)
  const urlDrawerResource: DrawerResource | null = policyIdFromUrl
    ? { type: 'policy', id: policyIdFromUrl }
    : null
  const effectiveDrawerResource = drawerResource ?? urlDrawerResource
  const [composeOpen, setComposeOpen] = useState(false)
  const [policySearch, setPolicySearch] = useState('')
  const [typeFilter, setTypeFilter] = useState('all')
  const [statusFilter, setStatusFilter] = useState('all')
  const [dateRange, setDateRange] = useState('30')
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)

  useEffect(() => {
    if (!routeDrawerResourceKey) return
    navigate(location.pathname + location.search, { replace: true, state: null })
  }, [location.pathname, location.search, navigate, routeDrawerResourceKey])

  const maintainerActions = useMemo(
    () => overviewData?.snapshot.actions ?? [],
    [overviewData?.snapshot.actions],
  )
  const selectedPolicy = useMemo(() => {
    if (effectiveDrawerResource?.type !== 'policy') return null
    return data?.policies.find((policy) => policy.id === effectiveDrawerResource.id) ?? null
  }, [data?.policies, effectiveDrawerResource])
  const selectedAction = useMemo(() => {
    if (effectiveDrawerResource?.type !== 'action') return null
    return maintainerActions.find((action) => action.maintainer_action_id === effectiveDrawerResource.id) ?? null
  }, [effectiveDrawerResource, maintainerActions])
  const selectedOutbox = useMemo(() => {
    if (effectiveDrawerResource?.type !== 'outbox') return null
    return data?.outbox.find((entry) => entry.inbox_id === effectiveDrawerResource.id) ?? null
  }, [data?.outbox, effectiveDrawerResource])

  const filteredPolicies = useMemo(() => {
    if (!data) return []
    return orderPoliciesByTypeAndRecentChange(
      data.policies.filter((policy) => {
        const haystack = `${policy.id} ${policy.name} ${policy.scope} ${policy.statement}`.toLowerCase()
        if (policySearch && !haystack.includes(policySearch.toLowerCase())) return false
        if (typeFilter !== 'all' && policy.message_type !== typeFilter) return false
        if (statusFilter !== 'all' && policy.status !== statusFilter) return false
        if (!dateMatchesRange(policy.created_at, dateRange)) return false
        return true
      }),
    )
  }, [data, dateRange, policySearch, statusFilter, typeFilter])

  const pagedPolicies = useMemo(
    () => paginate(filteredPolicies, page, pageSize),
    [filteredPolicies, page, pageSize],
  )

  if (isLoading) {
    return <div className="rounded-lg border border-slate-200 bg-white p-6 text-sm text-slate-500">Loading policies…</div>
  }
  if (error || !data) {
    return <div className="rounded-lg border border-rose-200 bg-rose-50 p-6 text-sm text-rose-700">{String(error)}</div>
  }

  const selectedStats = selectedPolicy ? deriveDeliveryStats(selectedPolicy, data) : null
  const selectedEvent = selectedPolicy ? findPolicyEvent(data, selectedPolicy.id) : null
  const selectedRelated = selectedPolicy ? relatedPolicies(selectedPolicy, data) : []
  const recentActions = orderActionsByCreatedAt(maintainerActions).slice(0, 8)
  const recentSendLog = orderSendLogBySentAt(data.send_log).slice(0, 8)
  const recentOutbox = orderOutboxByCreatedAt(data.outbox).slice(0, 8)

  function closePolicyDrawer() {
    setDrawerResource(null)
    if (!policyIdFromUrl) return
    const next = new URLSearchParams(searchParams)
    next.delete('policy_id')
    setSearchParams(next, { replace: true })
  }

  return (
    <>
      <PageContainer
        title="Policies"
        subtitle="Browse policy history, inspect delivery state, and open focused workspaces only when you need to publish or retire a policy."
        actions={
          <button
            type="button"
            onClick={() => {
              setDrawerResource(null)
              setComposeOpen(true)
            }}
            className="rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-blue-800"
          >
            New Action
          </button>
        }
      >
        <section className="space-y-3">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <div className="flex items-center gap-2">
              <h2 className="text-sm font-semibold tracking-tight text-slate-900">Policy History</h2>
              <StatusBadge tone="info">{filteredPolicies.length}</StatusBadge>
            </div>
            <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
              <input
                className="rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm"
                placeholder="Search policies..."
                value={policySearch}
                onChange={(event) => {
                  setPolicySearch(event.target.value)
                  setPage(1)
                }}
              />
              <select
                className="rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm"
                value={typeFilter}
                onChange={(event) => {
                  setTypeFilter(event.target.value)
                  setPage(1)
                }}
              >
                <option value="all">All Types</option>
                <option value="policy_update">Policy Update</option>
                <option value="claim_attribute_update">Claim Attribute Update</option>
              </select>
              <select
                className="rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm"
                value={statusFilter}
                onChange={(event) => {
                  setStatusFilter(event.target.value)
                  setPage(1)
                }}
              >
                <option value="all">All Status</option>
                <option value="active">Active</option>
                <option value="deprecated">Deprecated</option>
              </select>
              <select
                className="rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm"
                value={dateRange}
                onChange={(event) => {
                  setDateRange(event.target.value)
                  setPage(1)
                }}
              >
                <option value="30">Last 30 days</option>
                <option value="7">Last 7 days</option>
                <option value="1">Last 24 hours</option>
                <option value="all">All time</option>
              </select>
            </div>
          </div>
          <DataTable
            columns={[
              {
                key: 'policy',
                header: 'Policy',
                render: (row: Policy) => (
                  <div>
                    <div className="font-medium text-slate-900">{row.name}</div>
                    <div className="mt-0.5 font-mono text-[11px] text-slate-500">{row.id}</div>
                  </div>
                ),
              },
              {
                key: 'messageType',
                header: 'Type',
                render: (row: Policy) => <StatusBadge>{row.message_type.replaceAll('_', ' ')}</StatusBadge>,
              },
              { key: 'status', header: 'Status', render: (row: Policy) => <StatusBadge>{row.status}</StatusBadge> },
              { key: 'scope', header: 'Scope', render: (row: Policy) => <div className="max-w-xs text-xs leading-5">{row.scope}</div> },
              {
                key: 'deliveries',
                header: 'Deliveries',
                render: (row: Policy) => {
                  const stats = deriveDeliveryStats(row, data)
                  return <span className="font-mono text-xs">{deliverySummaryText(stats)}</span>
                },
              },
              {
                key: 'propagation',
                header: 'Propagation',
                render: (row: Policy) => {
                  const stats = deriveDeliveryStats(row, data)
                  return stats.ratio !== null ? (
                    <div className="min-w-24">
                      <div className="h-1 overflow-hidden rounded-full bg-slate-100">
                        <div className="h-full rounded-full bg-emerald-500" style={{ width: `${stats.ratio}%` }} />
                      </div>
                      <div className="mt-1 font-mono text-[11px] text-slate-500">{stats.ratio}%</div>
                    </div>
                  ) : (
                    <span className="font-mono text-[11px] text-slate-500">—</span>
                  )
                },
              },
              { key: 'created', header: 'Published At', render: (row: Policy) => <span className="font-mono text-xs">{formatDateTime(row.created_at)}</span> },
              {
                key: 'actions',
                header: 'Actions',
                render: (row: Policy) => <span className="text-xs font-medium text-blue-700">{row.status === 'active' ? 'View · Deprecate' : 'View'}</span>,
              },
            ]}
            rows={pagedPolicies}
            getRowId={(row) => row.id}
            onRowClick={(row) => {
              setComposeOpen(false)
              setDrawerResource({ type: 'policy', id: row.id })
            }}
            emptyState="No policy records are available."
          />
          <PaginationBar
            page={page}
            pageSize={pageSize}
            total={filteredPolicies.length}
            onPageChange={setPage}
            onPageSizeChange={(size) => {
              setPageSize(size)
              setPage(1)
            }}
          />
        </section>

        <section className="space-y-3">
          <div className="flex items-center gap-2">
            <h2 className="text-sm font-semibold tracking-tight text-slate-900">Maintainer Activity</h2>
            <StatusBadge tone="info">{maintainerActions.length}</StatusBadge>
          </div>

          <div className="rounded-lg border border-slate-200 bg-white p-3">
            <div className="mb-2 flex items-center justify-between">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Maintainer Actions</h3>
              <StatusBadge tone="info">{maintainerActions.length}</StatusBadge>
            </div>
            <DataTable
              columns={[
                {
                  key: 'action',
                  header: 'Action ID',
                  render: (row: MaintainerActionRow) => <span className="font-mono text-[11px] text-slate-500">{row.maintainer_action_id}</span>,
                },
                {
                  key: 'policy',
                  header: 'Policy',
                  render: (row: MaintainerActionRow) => (
                    <div>
                      <div className="font-medium text-slate-900">{row.policy_name}</div>
                      <div className="mt-0.5 font-mono text-[11px] text-slate-500">{row.policy_id}</div>
                    </div>
                  ),
                },
                { key: 'type', header: 'Type', render: (row: MaintainerActionRow) => <StatusBadge>{row.message_type.replaceAll('_', ' ')}</StatusBadge> },
                { key: 'target', header: 'Target', render: (row: MaintainerActionRow) => <StatusBadge tone={row.target_kind === 'broadcast' ? 'info' : 'warning'}>{targetLabelFromAction(row)}</StatusBadge> },
                { key: 'outboxEntries', header: 'Outbox', render: (row: MaintainerActionRow) => <span className="font-mono text-xs">{row.outbox_entries}</span> },
                { key: 'deliveredAgents', header: 'Receipt ACKs', render: (row: MaintainerActionRow) => <span className="font-mono text-xs">{row.delivered_agents.length}</span> },
                { key: 'sendEvents', header: 'Send Events', render: (row: MaintainerActionRow) => <span className="font-mono text-xs">{row.send_events}</span> },
                { key: 'createdAt', header: 'Created At', render: (row: MaintainerActionRow) => <span className="font-mono text-xs">{formatDateTime(row.created_at)}</span> },
              ]}
              rows={recentActions}
              getRowId={(row) => row.maintainer_action_id}
              onRowClick={(row) => {
                setComposeOpen(false)
                setDrawerResource({ type: 'action', id: row.maintainer_action_id })
              }}
              emptyState="No maintainer actions are available yet."
            />
          </div>

          <div className="grid gap-3 xl:grid-cols-2">
            <div className="rounded-lg border border-slate-200 bg-white p-3">
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Send Log</h3>
                <StatusBadge tone="info">{data.send_log.length}</StatusBadge>
              </div>
              <DataTable
                columns={[
                  { key: 'sentAt', header: 'Sent At', render: (row: SendLogRow) => <span className="font-mono text-[11px]">{formatDateTime(row.sent_at)}</span> },
                  { key: 'agent', header: 'Agent', render: (row: SendLogRow) => <span className="font-mono text-xs">{row.agent_id}</span> },
                  { key: 'policy', header: 'Policy', render: (row: SendLogRow) => <span className="font-mono text-[11px] text-slate-500">{row.policy_id}</span> },
                  { key: 'type', header: 'Message Type', render: (row: SendLogRow) => <StatusBadge>{row.message_type.replaceAll('_', ' ')}</StatusBadge> },
                  { key: 'inbox', header: 'Inbox ID', render: (row: SendLogRow) => <span className="font-mono text-[11px] text-slate-500">{row.inbox_id}</span> },
                  { key: 'action', header: 'Action ID', render: (row: SendLogRow) => <span className="font-mono text-[11px] text-slate-500">{row.maintainer_action_id}</span> },
                ]}
                rows={recentSendLog}
                getRowId={(row) => `${row.inbox_id}-${row.agent_id}-${row.sent_at}`}
                emptyState="No send log rows yet."
              />
            </div>

            <div className="rounded-lg border border-slate-200 bg-white p-3">
              <div className="mb-2 flex items-center justify-between">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Outbox</h3>
                <StatusBadge tone="info">{data.outbox.length}</StatusBadge>
              </div>
              <DataTable
                columns={[
                  { key: 'inbox', header: 'Inbox ID', render: (row: OutboxEntry) => <span className="font-mono text-[11px] text-slate-500">{row.inbox_id}</span> },
                  {
                    key: 'policy',
                    header: 'Policy',
                    render: (row: OutboxEntry) => (
                      <div>
                        <div className="font-medium text-slate-900">{policyFromOutbox(row).name}</div>
                        <div className="mt-0.5 font-mono text-[11px] text-slate-500">{policyIdFromOutbox(row)}</div>
                      </div>
                    ),
                  },
                  { key: 'target', header: 'Target Kind', render: (row: OutboxEntry) => <StatusBadge tone={row.target_kind === 'broadcast' ? 'info' : 'warning'}>{targetKindLabel(row.target_kind)}</StatusBadge> },
                  { key: 'offered', header: 'Offered', render: (row: OutboxEntry) => <span className="font-mono text-xs">{offeredCountFromOutbox(row)}</span> },
                  { key: 'delivered', header: 'Receipt ACKs', render: (row: OutboxEntry) => <span className="font-mono text-xs">{deliveredCountFromOutbox(row)}</span> },
                  { key: 'state', header: 'Open State', render: (row: OutboxEntry) => <StatusBadge tone={isOutboxEntryOpen(row, data.outbox, data.policies) ? 'warning' : 'success'}>{openStateFromOutbox(row, data.outbox, data.policies)}</StatusBadge> },
                  { key: 'created', header: 'Created At', render: (row: OutboxEntry) => <span className="font-mono text-xs">{formatDateTime(row.created_at)}</span> },
                ]}
                rows={recentOutbox}
                getRowId={(row) => row.inbox_id}
                onRowClick={(row) => {
                  setComposeOpen(false)
                  setDrawerResource({ type: 'outbox', id: row.inbox_id })
                }}
                emptyState="No outbox entries yet."
              />
            </div>
          </div>
        </section>

        <DetailDrawer
          open={Boolean(selectedPolicy || selectedAction || selectedOutbox)}
          onClose={closePolicyDrawer}
          label={selectedPolicy ? 'Policy' : selectedAction ? 'Action' : 'Outbox'}
          title={
            selectedPolicy?.name ??
            selectedAction?.policy_name ??
            (selectedOutbox ? policyFromOutbox(selectedOutbox).name : 'Detail')
          }
          subtitle={selectedPolicy?.id ?? selectedAction?.maintainer_action_id ?? selectedOutbox?.inbox_id}
          footer={
            selectedPolicy ? (
              <div className="grid grid-cols-2 gap-2">
                <button
                  type="button"
                  className="rounded-md border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700"
                  onClick={closePolicyDrawer}
                >
                  Close
                </button>
                <button
                  type="button"
                  className="rounded-md bg-rose-600 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-rose-700 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200"
                  disabled={isStaticDemo || selectedPolicy.status !== 'active' || deprecatePolicy.isPending}
                  title={isStaticDemo ? 'Static preview is read-only' : undefined}
                  onClick={() => deprecatePolicy.mutate(selectedPolicy.id)}
                >
                  {selectedPolicy.status === 'active' ? 'Deprecate Policy' : 'Deprecated'}
                </button>
              </div>
            ) : (
              <button
                type="button"
                className="w-full rounded-md border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700"
                onClick={closePolicyDrawer}
              >
                Close
              </button>
            )
          }
        >
          {selectedPolicy ? (
            <>
              <DrawerSection title="Overview">
                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Type</dt><dd><StatusBadge>{selectedPolicy.message_type.replaceAll('_', ' ')}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge>{selectedPolicy.status}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Scope</dt><dd className="text-right text-slate-900">{selectedPolicy.scope}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Published At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedPolicy.created_at)}</dd></div>
                  {selectedPolicy.updated_at ? (
                    <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedPolicy.updated_at)}</dd></div>
                  ) : null}
                </dl>
              </DrawerSection>

              <DrawerSection title="Statement">
                <DetailTextBlock>{selectedPolicy.statement.trim() || 'Unavailable'}</DetailTextBlock>
              </DrawerSection>

              <DrawerSection title="Propagation Summary">
                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Deliveries</dt><dd className="font-mono text-xs text-slate-900">{selectedStats ? deliverySummaryText(selectedStats) : '—'}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Failed Deliveries</dt><dd className="font-mono text-xs text-slate-900">{selectedStats?.failed ?? '—'}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Last Delivery</dt><dd className="font-mono text-xs text-slate-900">{selectedStats?.lastDelivery ? formatDateTime(selectedStats.lastDelivery) : '—'}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Propagation Lag</dt><dd className="font-mono text-xs text-slate-900">{selectedStats?.ratio !== null ? `${100 - (selectedStats?.ratio ?? 0)}% remaining` : '—'}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Outbox Entries</dt><dd className="font-mono text-xs text-slate-900">{selectedStats?.outboxCount ?? 0}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Send Events</dt><dd className="font-mono text-xs text-slate-900">{selectedStats?.sendCount ?? 0}</dd></div>
                </dl>
              </DrawerSection>

              <DrawerSection title="Affected">
                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Target Agents</dt><dd className="text-right font-mono text-xs text-slate-900">{selectedEvent?.target_agents.length ? selectedEvent.target_agents.join(', ') : selectedPolicy.target_agents?.length ? selectedPolicy.target_agents.join(', ') : 'broadcast'}</dd></div>
                </dl>
              </DrawerSection>

              <DrawerSection title="Related Policies">
                {selectedRelated.length ? (
                  <div className="flex flex-wrap gap-1.5">
                    {selectedRelated.map((policy) => (
                      <button
                        key={policy.id}
                        type="button"
                        className="rounded border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs font-medium text-slate-700 hover:bg-slate-100"
                        onClick={() => setDrawerResource({ type: 'policy', id: policy.id })}
                      >
                        {policy.name}
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="text-xs text-slate-500">No related policies in current snapshot.</div>
                )}
              </DrawerSection>
            </>
          ) : selectedAction ? (
            <>
              <DrawerSection title="Overview">
                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Action ID</dt><dd className="font-mono text-[11px] text-slate-500">{selectedAction.maintainer_action_id}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Type</dt><dd><StatusBadge>{selectedAction.message_type.replaceAll('_', ' ')}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Target</dt><dd><StatusBadge tone={selectedAction.target_kind === 'broadcast' ? 'info' : 'warning'}>{targetLabelFromAction(selectedAction)}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Policy Status</dt><dd><StatusBadge>{selectedAction.policy_status}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedAction.created_at)}</dd></div>
                </dl>
              </DrawerSection>

              <DrawerSection title="Delivery Summary">
                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Outbox Entries</dt><dd className="font-mono text-xs text-slate-900">{selectedAction.outbox_entries}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Receipt-ACK Agents</dt><dd className="font-mono text-xs text-slate-900">{selectedAction.delivered_agents.length}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Send Events</dt><dd className="font-mono text-xs text-slate-900">{selectedAction.send_events}</dd></div>
                </dl>
              </DrawerSection>

              <DrawerSection title="Inbox IDs">
                <div className="flex flex-wrap gap-1.5">
                  {selectedAction.inbox_ids.map((inboxId) => (
                    <button
                      key={inboxId}
                      type="button"
                      className="rounded border border-slate-200 bg-slate-50 px-2 py-0.5 font-mono text-[11px] text-slate-700 hover:bg-slate-100"
                      onClick={() => setDrawerResource({ type: 'outbox', id: inboxId })}
                    >
                      {inboxId}
                    </button>
                  ))}
                </div>
              </DrawerSection>

              <DrawerSection title="Agents">
                <div className="space-y-2 text-sm text-slate-600">
                  <div>
                    <div className="mb-1 text-xs font-medium text-slate-500">Target Agents</div>
                    <div className="font-mono text-xs text-slate-900">{selectedAction.target_agents.length ? selectedAction.target_agents.join(', ') : 'broadcast'}</div>
                  </div>
                  <div>
                    <div className="mb-1 text-xs font-medium text-slate-500">Receipt-ACK Agents</div>
                    <div className="font-mono text-xs text-slate-900">{selectedAction.delivered_agents.length ? selectedAction.delivered_agents.join(', ') : 'No deliveries yet'}</div>
                  </div>
                </div>
              </DrawerSection>
            </>
          ) : selectedOutbox ? (
            <>
              <DrawerSection title="Overview">
                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Inbox ID</dt><dd className="font-mono text-[11px] text-slate-500">{selectedOutbox.inbox_id}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Action ID</dt><dd className="font-mono text-[11px] text-slate-500">{selectedOutbox.maintainer_action_id}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Type</dt><dd><StatusBadge>{messageTypeFromOutbox(selectedOutbox).replaceAll('_', ' ')}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Target</dt><dd><StatusBadge tone={selectedOutbox.target_kind === 'broadcast' ? 'info' : 'warning'}>{targetLabelFromOutbox(selectedOutbox)}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Open State</dt><dd><StatusBadge tone={isOutboxEntryOpen(selectedOutbox, data.outbox, data.policies) ? 'warning' : 'success'}>{openStateFromOutbox(selectedOutbox, data.outbox, data.policies)}</StatusBadge></dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Created At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(selectedOutbox.created_at)}</dd></div>
                </dl>
              </DrawerSection>

              <DrawerSection title="Delivery Facts">
                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Offered Agents</dt><dd className="font-mono text-xs text-slate-900">{offeredCountFromOutbox(selectedOutbox)}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Last Offered At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(lastOfferedAtFromOutbox(selectedOutbox))}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Receipt ACK Count</dt><dd className="font-mono text-xs text-slate-900">{deliveredCountFromOutbox(selectedOutbox)}</dd></div>
                  <div className="flex justify-between gap-3"><dt className="text-slate-500">Last Receipt At</dt><dd className="font-mono text-xs text-slate-900">{formatDateTime(lastSentAtFromOutbox(selectedOutbox))}</dd></div>
                </dl>
                <div className="mt-2 space-y-1.5">
                  {selectedOutbox.delivered_to.length ? (
                    selectedOutbox.delivered_to.map((mark) => (
                      <div key={`${mark.agent_id}-${mark.sent_at}`} className="rounded border border-slate-100 bg-slate-50 px-2.5 py-1.5 text-sm text-slate-600">
                        <div className="font-mono text-xs text-slate-900">{mark.agent_id}</div>
                        <div className="mt-0.5 font-mono text-[11px] text-slate-500">{formatDateTime(mark.sent_at)}</div>
                      </div>
                    ))
                  ) : (
                    <div className="text-xs text-slate-500">No receipt ACKs yet.</div>
                  )}
                </div>
              </DrawerSection>

              <DrawerSection title="Inbox Message Snapshot">
                <DetailTextBlock>{JSON.stringify(selectedOutbox.inbox_message, null, 2)}</DetailTextBlock>
              </DrawerSection>
            </>
          ) : null}
        </DetailDrawer>
      </PageContainer>

      {composeOpen ? (
        <PolicyComposeDrawer
          open={composeOpen}
          policies={data.policies}
          onClose={() => setComposeOpen(false)}
        />
      ) : null}
    </>
  )
}
