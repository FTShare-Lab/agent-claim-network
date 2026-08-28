import * as Dialog from '@radix-ui/react-dialog'
import { CheckCircle2, X } from 'lucide-react'
import { useState } from 'react'
import { Link, useNavigate } from 'react-router'

import { StatusBadge } from '../components/badges/StatusBadge'
import { PageContainer } from '../layouts/PageContainer'
import { formatDateTime, formatRelativeMinutes, toPercent } from '../lib/format'
import { useOverviewQuery } from '../features/overview/hooks'
import type { AgentStatusSummary, MaintainerStatusCounts } from '../features/overview/types'

function Panel({ title, action, children }: { title: string; action?: React.ReactNode; children: React.ReactNode }) {
  return (
    <section className="min-w-0 rounded-[var(--radius-md)] bg-white p-5 ring-1 ring-slate-200/80">
      <div className="mb-4 flex items-center justify-between gap-3">
        <h2 className="text-sm font-semibold tracking-tight text-slate-900">{title}</h2>
        {action}
      </div>
      {children}
    </section>
  )
}

function healthLabel(integrity: number | null) {
  if (integrity === null) return 'Unknown'
  if (integrity >= 90) return 'Healthy'
  if (integrity >= 70) return 'Watch'
  return 'Degraded'
}
function healthTone(integrity: number | null): 'success' | 'warning' | 'danger' | 'neutral' {
  if (integrity === null) return 'neutral'
  if (integrity >= 90) return 'success'
  if (integrity >= 70) return 'warning'
  return 'danger'
}

const clickableRow =
  'cursor-pointer rounded-lg px-3 py-2.5 transition-colors hover:bg-[var(--bg-muted)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--accent)]'

function DefRow({
  title,
  formula,
  desc,
  note,
}: {
  title: string
  formula: string
  desc: string
  note?: string
}) {
  return (
    <div className="space-y-1.5">
      <div className="text-sm font-semibold text-slate-900">{title}</div>
      <pre className="overflow-x-auto whitespace-pre-wrap break-words rounded-md border border-slate-200 bg-slate-50 px-2.5 py-2 font-mono text-[11px] leading-5 text-slate-700">
        {formula}
      </pre>
      <div className="text-xs leading-5 text-slate-600">{desc}</div>
      {note ? <div className="text-[11px] leading-5 text-slate-500">{note}</div> : null}
    </div>
  )
}

function NetworkHealthDialog({
  integrity,
  counts,
  agents,
}: {
  integrity: number | null
  counts: MaintainerStatusCounts
  agents: AgentStatusSummary[]
}) {
  const atRisk = agents.filter(
    (a) => a.mirror_claims > 0 && a.active_claims / a.mirror_claims < 0.5,
  ).length
  return (
    <Dialog.Root>
      <Dialog.Trigger asChild>
        <button type="button" className="text-xs font-medium text-blue-700 hover:underline">
          View Definition
        </button>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-slate-900/40 backdrop-blur-sm" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-[60] flex max-h-[85vh] w-[min(640px,calc(100vw-2rem))] -translate-x-1/2 -translate-y-1/2 flex-col rounded-lg border border-slate-200 bg-white shadow-lg">
          <div className="flex items-start justify-between gap-3 border-b border-slate-200 px-5 py-3">
            <div>
              <Dialog.Title className="text-sm font-semibold text-slate-900">Network Health — 计算定义</Dialog.Title>
              <Dialog.Description className="mt-0.5 text-xs text-slate-500">
                各指标的算法与含义，公式中带入当前实时值便于核对。
              </Dialog.Description>
            </div>
            <Dialog.Close asChild>
              <button
                type="button"
                aria-label="Close"
                className="shrink-0 rounded-md border border-slate-200 p-1.5 text-slate-500 transition hover:bg-slate-50"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </Dialog.Close>
          </div>
          <div className="flex-1 space-y-4 overflow-y-auto px-5 py-4">
            <DefRow
              title="Network Integrity（顶部进度条）"
              formula={`分子 = active_claims + active_policies + resolved_disputes
分母 = claims + active_policies + open_disputes + resolved_disputes
Integrity% = 分子 / 分母 × 100，clamp 到 [0,100]

当前: (${counts.active_claims} + ${counts.active_policies} + ${counts.resolved_disputes})
     / (${counts.claims} + ${counts.active_policies} + ${counts.open_disputes} + ${counts.resolved_disputes})
     = ${integrity === null ? 'insufficient signal' : `${integrity}%`}`}
              desc={'把 claim / policy / dispute 三类资源里"处于健康或已收敛状态"的数量相加作分子，除以三类资源总数作分母，得到一个综合健康比例。'}
              note="分子 = 活跃 claim + 生效中 policy + 已关闭 dispute。分母 = 所有 claim(含 stale/deprecated) + 生效中 policy + 所有 dispute(open+resolved)。注意：分母里 policy 只算 active（deprecated_policies 不计入），这是既有口径。健康档位：≥90% Healthy，70–89% Watch，<70% Degraded。"
            />
            <DefRow
              title="Semantic Consistency"
              formula={`active_claims / claims × 100

当前: ${counts.active_claims} / ${counts.claims} = ${toPercent(counts.active_claims, counts.claims)}%`}
              desc="活跃 claim 占总 claim 的比例。越高说明 claim 整体还没退化成 stale/deprecated，语义一致性越好。100% 表示所有 claim 都还活跃。"
            />
            <DefRow
              title="Policy Propagation"
              formula={`active_policies / max(active_policies + deprecated_policies, 1) × 100

当前: ${counts.active_policies} / ${Math.max(counts.active_policies + counts.deprecated_policies, 1)} = ${toPercent(counts.active_policies, Math.max(counts.active_policies + counts.deprecated_policies, 1))}%`}
              desc="生效中 policy 占所有 policy(生效+已废弃) 的比例。越高说明已发布的 policy 大多还在生效，没有被大量废弃。分母用 max(...,1) 防止除零。"
            />
            <DefRow
              title="Stale Claims"
              formula={`counts.stale_claims（原始计数，非百分比）

当前: ${counts.stale_claims}`}
              desc={'当前被判定为 stale 的 claim 数量。越低越好，0 最好。这是绝对计数，不受总数影响，直接反映"有多少 claim 该刷新了"。'}
            />
            <DefRow
              title="At-Risk Agents"
              formula={`对每个 agent:
  ratio = active_claims / mirror_claims       // 活跃 claim 占该 agent 持有 claim 总数的比例
  若 mirror_claims > 0 且 ratio < 0.5，则该 agent 计为 at-risk
显示 = at-risk 数量 / 总 agent 数量

当前: ${atRisk} / ${agents.length}`}
              desc={'对每个 agent 算它"活跃 claim 占自己持有 claim 总数(mirror_claims)的比例"；比例不到 50% 的 agent 视为"有风险"——它持有的 claim 大部分已退化成 stale/deprecated，需要关注或触发 sweep。'}
              note="mirror_claims = 该 agent 持有的 claim 总数(active + stale + deprecated)，即 /api/overview 里每个 agent 的 mirror_claims 字段。持有 0 条 claim 的新 agent 不计入 at-risk（分母为 0 时跳过）。越低越好，0 / N 最佳。"
            />
          </div>
          <div className="border-t border-slate-200 px-5 py-3">
            <Dialog.Close asChild>
              <button
                type="button"
                className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm font-semibold text-slate-700 transition hover:bg-slate-50"
              >
                Close
              </button>
            </Dialog.Close>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

export function OverviewPage() {
  const navigate = useNavigate()
  const [activityTab, setActivityTab] = useState<'disputes' | 'policies' | 'actions'>('disputes')
  const { data, isLoading, error } = useOverviewQuery()

  if (isLoading) {
    return (
      <div aria-busy="true" aria-label="Loading overview" className="space-y-3">
        <div className="h-24 animate-pulse rounded-[10px] bg-slate-100" />
        <div className="grid gap-3 lg:grid-cols-3">
          <div className="h-52 animate-pulse rounded-[10px] bg-slate-100 lg:col-span-2" />
          <div className="h-52 animate-pulse rounded-[10px] bg-slate-100" />
        </div>
      </div>
    )
  }
  if (error || !data) {
    return (
      <div role="alert" className="rounded-[10px] border border-rose-200 bg-rose-50 p-6 text-sm text-rose-800">
        <div className="font-semibold">Overview data could not be loaded.</div>
        <div className="mt-1 break-words">Refresh the page or check the Maintainer daemon. {String(error)}</div>
      </div>
    )
  }

  const { counts, disputes, agents } = data.snapshot
  const integrityDenominator =
    counts.claims + counts.active_policies + counts.open_disputes + counts.resolved_disputes
  const integrity =
    integrityDenominator > 0
      ? toPercent(
          counts.active_claims + counts.active_policies + counts.resolved_disputes,
          integrityDenominator,
        )
      : null
  const atRiskAgents = agents.filter(
    (a) => a.mirror_claims > 0 && a.active_claims / a.mirror_claims < 0.5,
  ).length
  const recentAuditErrors = data.recent_http_audits.filter((audit) => audit.status_code >= 400).length
  const attentionItems = [
    {
      count: counts.open_disputes,
      label: 'Open disputes',
      detail: 'Claims need a Maintainer review and resolution note.',
      href: '/disputes',
      tone: 'danger' as const,
    },
    {
      count: counts.stale_claims,
      label: 'Stale claims',
      detail: 'Review sweep evidence before sending status suggestions.',
      href: '/sweep',
      tone: 'warning' as const,
    },
    {
      count: atRiskAgents,
      label: 'At-risk agents',
      detail: 'Fewer than half of mirrored claims remain active.',
      href: '/agents',
      tone: 'warning' as const,
    },
    {
      count: recentAuditErrors,
      label: 'Recent HTTP errors',
      detail: 'Inspect request and response bodies before retrying.',
      href: '/http-audits',
      tone: 'danger' as const,
    },
  ].filter((item) => item.count > 0)
  const quickActions = [
    ['Open Disputes', 'Review & Resolve', '/disputes'],
    ['Browse Claims', 'Search Corpus', '/claims'],
    ['Publish Policy', 'Create & Propagate', '/policies'],
    ['Run Sweep Now', 'Diagnostics Sweep', '/sweep'],
    ['Router Query', 'Semantic Search', '/router-query'],
  ]

  return (
    <PageContainer
      title="Network Operations Overview"
      subtitle="Review what needs a Maintainer resolution, then inspect network health and evidence."
    >
      <div className="grid items-start gap-4 xl:grid-cols-[minmax(0,1.55fr)_minmax(320px,0.85fr)]">
        <Panel
          title="Needs Attention"
          action={
            <StatusBadge tone={attentionItems.length === 0 ? 'success' : 'danger'}>
              {attentionItems.length === 0 ? 'clear' : `${attentionItems.length} ${attentionItems.length === 1 ? 'queue' : 'queues'}`}
            </StatusBadge>
          }
        >
          {attentionItems.length === 0 ? (
            <div className="flex flex-col gap-3 rounded-xl bg-[var(--success-weak)] px-4 py-3.5 sm:flex-row sm:items-center">
              <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-white/80 text-[var(--success)]">
                <CheckCircle2 aria-hidden="true" className="h-5 w-5" />
              </span>
              <div className="min-w-0 flex-1">
                <div className="text-sm font-semibold text-slate-900">No network resolutions are waiting.</div>
                <p className="mt-0.5 text-xs leading-5 text-slate-600">
                  Disputes, stale suggestions, agent risk, and recent HTTP errors are clear in this snapshot.
                </p>
              </div>
              <Link to="/sweep" className="shrink-0 text-xs font-semibold text-[var(--success)] hover:underline">
                Run diagnostics
              </Link>
            </div>
          ) : (
            <div className="divide-y divide-slate-100/90">
              {attentionItems.map((item) => (
                <Link
                  key={item.label}
                  to={item.href}
                  className="group flex items-start gap-3 rounded-lg px-2 py-3 transition-colors hover:bg-[var(--bg-muted)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--accent)]"
                >
                  <span className="mt-0.5 min-w-8 font-mono text-lg font-semibold text-slate-900">{item.count}</span>
                  <span className="min-w-0 flex-1">
                    <span className="flex items-center gap-2 text-sm font-semibold text-slate-900">
                      {item.label}
                      <StatusBadge tone={item.tone}>review</StatusBadge>
                    </span>
                    <span className="mt-0.5 block text-xs leading-5 text-slate-600">{item.detail}</span>
                  </span>
                  <span aria-hidden="true" className="mt-1 text-sm text-slate-400 transition-transform group-hover:translate-x-0.5">→</span>
                </Link>
              ))}
            </div>
          )}
        </Panel>

        <Panel
          title="Network Health"
          action={<NetworkHealthDialog integrity={integrity} counts={counts} agents={agents} />}
        >
          <div className="space-y-3">
            <div>
              <div className="mb-2 flex items-end justify-between text-xs">
                <span className="text-slate-500">Network integrity</span>
                <span className="flex items-center gap-2">
                  <span className="font-mono text-lg font-semibold text-slate-900">
                    {integrity === null ? '—' : `${integrity}%`}
                  </span>
                  <StatusBadge tone={healthTone(integrity)}>{healthLabel(integrity)}</StatusBadge>
                </span>
              </div>
              <div className="h-1.5 overflow-hidden rounded-full bg-slate-100">
                <div
                  className={integrity === null ? 'h-full rounded-full bg-slate-300' : 'h-full rounded-full bg-[var(--success)]'}
                  style={{ width: integrity === null ? '18%' : `${integrity}%` }}
                />
              </div>
              {integrity === null ? (
                <p className="mt-2 text-[11px] leading-4 text-slate-500">Insufficient network data for a health judgment.</p>
              ) : null}
            </div>
            <div className="divide-y divide-slate-100 border-t border-slate-100">
              {[
                ['Semantic Consistency', counts.claims > 0 ? `${toPercent(counts.active_claims, counts.claims)}%` : '—', counts.claims > 0 ? 'success' : 'neutral'],
                ['Policy Propagation', counts.active_policies + counts.deprecated_policies > 0 ? `${toPercent(counts.active_policies, counts.active_policies + counts.deprecated_policies)}%` : '—', counts.active_policies + counts.deprecated_policies > 0 ? 'success' : 'neutral'],
                ['Stale Claims', `${counts.stale_claims}`, 'warning'],
                ['At-Risk Agents', agents.length > 0 ? `${atRiskAgents} / ${agents.length}` : '—', agents.length > 0 ? (atRiskAgents === 0 ? 'success' : 'danger') : 'neutral'],
              ].map(([label, value, tone]) => (
                <div key={label} className="flex items-center justify-between py-2">
                  <span className="text-xs text-slate-600">{label}</span>
                  <StatusBadge tone={tone as 'success' | 'warning' | 'danger' | 'neutral'}>{value}</StatusBadge>
                </div>
              ))}
            </div>
          </div>
        </Panel>
      </div>

      <nav aria-label="Common Maintainer tasks" className="overflow-x-auto rounded-[var(--radius-md)] bg-white p-2 ring-1 ring-slate-200/80">
        <div className="flex min-w-max sm:min-w-0">
          {quickActions.map(([title, description, href]) => (
            <Link
              key={href}
              to={href}
              className="group min-w-48 flex-1 rounded-[10px] px-3 py-2.5 transition-colors hover:bg-[var(--accent-weak)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-[var(--accent)]"
            >
              <div className="flex items-center justify-between gap-3 text-sm font-semibold text-slate-800 group-hover:text-[var(--accent-strong)]">
                {title}
                <span aria-hidden="true" className="text-slate-400 transition-colors group-hover:text-[var(--accent)]">↗</span>
              </div>
              <div className="mt-0.5 text-[11px] text-slate-500">{description}</div>
            </Link>
          ))}
        </div>
      </nav>

      <section aria-labelledby="inventory-title" className="overflow-hidden rounded-[var(--radius-md)] bg-white ring-1 ring-slate-200/80">
        <div className="border-b border-slate-100 px-5 py-3.5">
          <h2 id="inventory-title" className="text-sm font-semibold text-slate-900">Knowledge Inventory</h2>
        </div>
        <div className="grid grid-cols-2 divide-x divide-y divide-slate-100 md:grid-cols-4 md:divide-y-0">
          {[
            ['Claims', counts.claims, `${counts.active_claims} active · ${counts.stale_claims} stale`, '/claims'],
            ['Policies', counts.active_policies, `${counts.deprecated_policies} deprecated`, '/policies'],
            ['Agents', counts.agents, `${counts.send_events} delivery events`, '/agents'],
            ['Resolved disputes', counts.resolved_disputes, `${counts.open_disputes} still open`, '/disputes'],
          ].map(([label, value, detail, href]) => (
            <Link key={label} to={String(href)} className="group min-w-0 px-5 py-4 transition-colors hover:bg-[var(--bg-muted)] focus-visible:outline focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-[var(--accent)]">
              <div className="font-mono text-2xl font-semibold tracking-tight text-slate-900">{value}</div>
              <div className="mt-1 text-xs font-semibold text-slate-800 group-hover:text-[var(--accent-strong)]">{label}</div>
              <div className="mt-0.5 truncate text-[11px] text-slate-500" title={String(detail)}>{detail}</div>
            </Link>
          ))}
        </div>
      </section>

      <div className="grid items-start gap-4 xl:grid-cols-[minmax(0,1.45fr)_minmax(340px,0.8fr)]">
        <section className="min-w-0 rounded-[var(--radius-md)] bg-white p-5 ring-1 ring-slate-200/80">
          <div className="flex flex-col gap-3 border-b border-slate-100 pb-3 sm:flex-row sm:items-center sm:justify-between">
            <h2 className="text-sm font-semibold text-slate-900">Network Activity</h2>
            <div role="tablist" aria-label="Network activity type" className="inline-flex max-w-full w-fit overflow-x-auto rounded-lg bg-[var(--bg-muted)] p-1">
              {[
                ['disputes', 'Disputes', disputes.length],
                ['policies', 'Policies', data.recent_policy_events.length],
                ['actions', 'Actions', data.snapshot.actions.length],
              ].map(([value, label, count]) => (
                <button
                  key={value}
                  type="button"
                  role="tab"
                  aria-selected={activityTab === value}
                  aria-controls={`activity-${value}`}
                  onClick={() => setActivityTab(value as typeof activityTab)}
                  className={activityTab === value
                    ? 'shrink-0 rounded-md bg-white px-3 py-1.5 text-xs font-semibold text-slate-900 shadow-sm'
                    : 'shrink-0 rounded-md px-3 py-1.5 text-xs font-medium text-slate-500 transition-colors hover:text-slate-900'}
                >
                  {label} <span className="ml-1 font-mono text-[10px] text-slate-500">{count}</span>
                </button>
              ))}
            </div>
          </div>

          <div id={`activity-${activityTab}`} role="tabpanel" className="acn-content-swap mt-3 min-h-40" key={activityTab}>
            {activityTab === 'disputes' ? (
              <div className="divide-y divide-slate-100">
                {disputes.slice(0, 6).map((dispute) => (
                  <button key={dispute.id} type="button" onClick={() => navigate('/disputes', { state: { disputeId: dispute.id } })} className={`${clickableRow} flex w-full items-center justify-between text-left`}>
                    <div className="min-w-0">
                      <div className="truncate text-sm font-medium text-slate-900">{dispute.name}</div>
                      <div className="truncate font-mono text-[11px] text-slate-500">{dispute.id}</div>
                    </div>
                    <div className="flex shrink-0 items-center gap-2">
                      <span className="font-mono text-[11px] text-slate-500">{formatRelativeMinutes(dispute.created_at)}</span>
                      <StatusBadge>{dispute.status}</StatusBadge>
                    </div>
                  </button>
                ))}
                {disputes.length === 0 ? <p className="px-2 py-8 text-center text-xs leading-5 text-slate-500">No open disputes. Conflicting claims will appear here for review.</p> : null}
              </div>
            ) : null}

            {activityTab === 'policies' ? (
              <div className="divide-y divide-slate-100">
                {data.recent_policy_events.slice(0, 6).map((event) => (
                  <button key={event.event_id} type="button" onClick={() => navigate('/policies', { state: { policyId: event.policy_id } })} className={`${clickableRow} block w-full text-left`}>
                    <div className="flex items-center justify-between gap-2">
                      <div className="truncate text-sm font-medium text-slate-900">{event.policy_name}</div>
                      <div className="font-mono text-[11px] text-slate-500">{formatDateTime(event.occurred_at)}</div>
                    </div>
                    <div className="mt-1.5 flex items-center gap-1.5"><StatusBadge>{event.message_type.replaceAll('_', ' ')}</StatusBadge><StatusBadge>{event.policy_status}</StatusBadge></div>
                  </button>
                ))}
                {data.recent_policy_events.length === 0 ? <p className="px-2 py-8 text-center text-xs leading-5 text-slate-500">No policy activity has been recorded.</p> : null}
              </div>
            ) : null}

            {activityTab === 'actions' ? (
              <div className="divide-y divide-slate-100">
                {data.snapshot.actions.slice(0, 6).map((action) => (
                  <button key={action.maintainer_action_id} type="button" onClick={() => navigate('/policies', { state: { actionId: action.maintainer_action_id } })} className={`${clickableRow} block w-full text-left`}>
                    <div className="flex items-center justify-between gap-2">
                      <div className="truncate text-sm font-medium text-slate-900">{action.policy_name}</div>
                      <div className="font-mono text-[11px] text-slate-500">{formatDateTime(action.created_at)}</div>
                    </div>
                    <div className="mt-1.5 flex flex-wrap items-center gap-1.5"><StatusBadge>{action.message_type.replaceAll('_', ' ')}</StatusBadge><StatusBadge tone={action.target_kind === 'broadcast' ? 'info' : 'warning'}>{action.target_kind}</StatusBadge><StatusBadge tone="neutral">{action.delivered_agents.length} delivered</StatusBadge></div>
                  </button>
                ))}
                {data.snapshot.actions.length === 0 ? <p className="px-2 py-8 text-center text-xs leading-5 text-slate-500">No Maintainer actions have been published.</p> : null}
              </div>
            ) : null}
          </div>
          <div className="mt-2 flex justify-end">
            <Link to={activityTab === 'disputes' ? '/disputes' : '/policies'} className="text-xs font-semibold text-[var(--accent)] hover:underline">View all activity</Link>
          </div>
        </section>

        <Panel title="Operational Pulse" action={<Link to="/sweep" className="text-xs font-semibold text-[var(--accent)] hover:underline">Open diagnostics</Link>}>
          <section aria-labelledby="latest-sweep-title">
            <h3 id="latest-sweep-title" className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-500">Latest sweep</h3>
            {data.latest_sweep ? (
              <button type="button" onClick={() => navigate('/sweep', { state: { runId: data.latest_sweep!.run_id } })} className={`${clickableRow} mt-1 block w-full text-left`}>
                <div className="flex items-center justify-between"><span className="font-mono text-sm font-semibold text-slate-900">{formatDateTime(data.latest_sweep.triggered_at)}</span><StatusBadge tone="success">success</StatusBadge></div>
                <div className="mt-2 flex flex-wrap gap-x-5 gap-y-1 text-[11px] text-slate-500"><span><strong className="font-mono text-slate-800">{data.latest_sweep.report.stale_claims.length}</strong> stale</span><span><strong className="font-mono text-slate-800">{data.latest_sweep.report.deprecated_claims.length}</strong> deprecated</span><span className="truncate">trigger <strong className="font-mono text-slate-800">{data.latest_sweep.trigger}</strong></span></div>
              </button>
            ) : (
              <p className="py-3 text-xs leading-5 text-slate-500">No sweep history is recorded yet.</p>
            )}
          </section>

          <section aria-labelledby="agent-pulse-title" className="mt-4 border-t border-slate-100 pt-4">
            <div className="flex items-center justify-between"><h3 id="agent-pulse-title" className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-500">Agents</h3><Link to="/agents" className="text-[11px] font-semibold text-[var(--accent)]">View all</Link></div>
            <div className="mt-1 divide-y divide-slate-100">
              {agents.slice(0, 4).map((agent) => (
                <button key={agent.agent_id} type="button" onClick={() => navigate('/agents', { state: { agentId: agent.agent_id } })} className={`${clickableRow} block w-full text-left`}>
                  <div className="flex items-center justify-between"><span className="font-mono text-xs text-slate-800">{agent.agent_id}</span><span className="text-[11px] text-slate-500">{agent.active_claims} active</span></div>
                  <div className="mt-1.5 h-1 overflow-hidden rounded-full bg-slate-100"><div className="h-full rounded-full bg-[var(--accent)]" style={{ width: `${toPercent(agent.active_claims, Math.max(agent.mirror_claims, 1))}%` }} /></div>
                </button>
              ))}
              {agents.length === 0 ? <p className="py-3 text-xs leading-5 text-slate-500">No Agents are registered yet.</p> : null}
            </div>
          </section>

          <section aria-labelledby="http-pulse-title" className="mt-4 border-t border-slate-100 pt-4">
            <div className="flex items-center justify-between"><h3 id="http-pulse-title" className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate-500">HTTP audits · 24h</h3><Link to="/http-audits" className="text-[11px] font-semibold text-[var(--accent)]">View all</Link></div>
            <div className="mt-2 flex items-center gap-4 text-xs text-slate-500"><span><strong className="font-mono text-slate-900">{data.recent_http_audits.length}</strong> rows</span><span><strong className="font-mono text-[var(--success)]">{data.recent_http_audits.filter((audit) => audit.status_code < 400).length}</strong> healthy</span><span><strong className="font-mono text-[var(--danger)]">{recentAuditErrors}</strong> errors</span></div>
            {data.recent_http_audits.length === 0 ? <p className="mt-2 text-xs leading-5 text-slate-500">No audited write requests have been recorded.</p> : null}
          </section>
        </Panel>
      </div>
    </PageContainer>
  )
}
