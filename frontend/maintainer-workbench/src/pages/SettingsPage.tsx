import { useEffect, useState } from 'react'
import { ChevronDown } from 'lucide-react'

import { PageContainer } from '../layouts/PageContainer'
import { getAdminAuthStatus, readAdminSession } from '../features/auth/session'
import { useWorkbenchUiStore } from '../app/store'
import { formatDateTime } from '../lib/format'
import { isStaticDemo } from '../lib/runtime'

type EndpointEntry = {
  method: 'GET' | 'POST'
  path: string
  summary: string
  body?: string
  response: string
  caller: string
}

type EndpointGroup = {
  group: string
  endpoints: EndpointEntry[]
}

const ENDPOINT_GROUPS: EndpointGroup[] = [
  {
    group: 'Admin & Auth',
    endpoints: [
      {
        method: 'POST',
        path: '/api/admin-auth/check',
        summary: '校验管理员账号密码',
        body: '无 JSON body。凭据通过 HTTP Basic Auth 头 Authorization: Basic <base64(user:pass)> 传入。',
        response: '200 OK（凭据正确，返回空体）\n401 Unauthorized（账号或密码错误）',
        caller: 'workbench 登录页',
      },
      {
        method: 'GET',
        path: '/api/admin-auth/status',
        summary: '查询 admin 鉴权是否启用',
        response: '{\n  "enabled": boolean   // true=需要登录, false=免登直接进\n}',
        caller: 'workbench 路由守卫',
      },
    ],
  },
  {
    group: 'Overview & Status',
    endpoints: [
      {
        method: 'GET',
        path: '/health',
        summary: '存活探针，返回 200',
        response: '200 OK（无 body）',
        caller: '编排 / 探活',
      },
      {
        method: 'GET',
        path: '/status',
        summary: '返回 maintainer 状态快照',
        response: `MaintainerStatusSnapshot {
  generated_at: ISO8601,
  counts: {
    agents, claims, active_claims, stale_claims, deprecated_claims,
    active_policies, deprecated_policies, open_disputes, resolved_disputes,
    outbox_entries, send_events   // 均为整数计数
  },
  agents: [ AgentStatusSummary { agent_id, mirror_claims, active_claims,
           stale_claims, deprecated_claims } ],
  policies: [ Policy ],
  disputes: [ Dispute ],
  actions: [ MaintainerActionRow ],
  send_log: [ SendLogRow ]
}`,
        caller: 'agent / workbench',
      },
      {
        method: 'GET',
        path: '/api/overview',
        summary: '运维聚合视图：snapshot + 最新 sweep + 最近事件',
        response: `OverviewResponse {
  snapshot: MaintainerStatusSnapshot,        // 同 /status
  latest_sweep: SweepRunRecord | null,       // 最近一次扫描，见 Sweep 组
  sweep_schedule: {
    tick_interval_secs: 整数,
    last_auto_sweep_at: ISO8601 | null,
    next_sweep_at: ISO8601 | null,
    last_auto_trigger: "maintainer_startup"|"ticker"|null
  },
  recent_policy_events: [ PolicyEventRecord ],   // 见 Policies 组
  recent_agent_activities: [ AgentActivityRecord ],
  recent_http_audits: [ HttpAuditRecord ],       // 见 HTTP Audits 组
  recent_dispute_resolutions: [ {
    event_id: string, dispute_id: string,
    occurred_at: ISO8601, summary: string | null
  } ]
}`,
        caller: 'workbench Overview 页',
      },
    ],
  },
  {
    group: 'Agents',
    endpoints: [
      {
        method: 'GET',
        path: '/api/agents',
        summary: '列出所有已注册 agent 及其 claim 计数、活动、来源 IP',
        response: `AgentView[] 每项 {
  agent_id: string,
  mirror_claims: 整数,    // 该 agent 镜像的 claim 总数
  active_claims: 整数,     // 其中 active 数量
  stale_claims: 整数,
  deprecated_claims: 整数,
  last_source_ip: string | null,
  last_activity: {
    event_id: string, agent_id: string,
    activity_kind: "inbox_pulled"|"claim_uploaded"|"dispute_reported", occurred_at: ISO8601,
    summary: string
  } | null,
  recent_activities: [ 同 last_activity 结构的数组 ]
}`,
        caller: 'workbench Agents 页',
      },
    ],
  },
  {
    group: 'Claims',
    endpoints: [
      {
        method: 'GET',
        path: '/api/claims',
        summary: '列出所有 claim 视图（claim 本体 + 关联 dispute id）',
        response: `ClaimView[] 每项 {
  claim: {
    id: string, name: string, statement: string,
    scope: string, holder: string,        // holder = agent_id
    confidence: "high"|"medium"|"low",
    status: "active"|"stale"|"deprecated",
    created_at: ISO8601, updated_at?: ISO8601,
    source_claim_ids: [ string ],
    evidence_summary: string
  },
  open_dispute_ids: [ string ],
  resolved_dispute_ids: [ string ]
}`,
        caller: 'workbench Claims 页',
      },
      {
        method: 'GET',
        path: '/api/claims/{id}',
        summary: '查询单个 claim 详情',
        response: 'ClaimView（结构同 /api/claims 的单项）',
        caller: 'workbench Claims Drawer',
      },
      {
        method: 'POST',
        path: '/claims/upload',
        summary: 'agent 上传 / 覆盖一条 claim',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: Claim {
    id, name, statement, scope, holder,
    confidence, status, created_at, updated_at?,
    source_claim_ids: [ string ],
    evidence_summary: string
  }
}`,
        response: '200 OK（无 body）',
        caller: 'agent runtime',
      },
    ],
  },
  {
    group: 'Disputes',
    endpoints: [
      {
        method: 'GET',
        path: '/api/disputes',
        summary: '列出所有 dispute',
        response: `MaintainerDisputeRecord[] 每项 {
  id: string, name: string,
  reporter_agent_id: string,
  claims: [ string ],          // direct claim_id 列表
  summary: string,
  status: "open"|"resolved",
  created_at: ISO8601,
  resolved_at?: ISO8601,
  resolution?: DisputeResolution
}`,
        caller: 'workbench Disputes 页',
      },
      {
        method: 'GET',
        path: '/api/disputes/{id}',
        summary: '查询单个 dispute',
        response: `DisputeDetail {
  ...MaintainerDisputeRecord,
  current_analysis?: ArbitrationAnalysisSummary,
  holder_adoption?: HolderAdoptionView
}`,
        caller: 'workbench Disputes Drawer',
      },
      {
        method: 'GET',
        path: '/api/disputes/{id}/analyses',
        summary: '查询当前 Analysis',
        response: '{ current_analysis? }',
        caller: 'workbench Disputes Drawer',
      },
      {
        method: 'POST',
        path: '/api/disputes/{id}/analyses',
        summary: '显式运行 Analysis，并覆盖当前结果',
        response: '202 Accepted + ArbitrationAnalysisSummary',
        caller: 'workbench Disputes Analyze',
      },
      {
        method: 'GET',
        path: '/api/disputes/{id}/analyses/{analysis_id}',
        summary: '查询冻结上下文、proposal 与 verification',
        response: 'ArbitrationAnalysisDetail',
        caller: 'workbench Analysis 卡片',
      },
      {
        method: 'POST',
        path: '/api/disputes/{id}/analyses/{analysis_id}/adopt',
        summary: '显式采用 approved Analysis，不重新调用模型',
        response: '201 Created + ArbitrationResolutionRecord；输入已变化或已关闭时 409',
        caller: 'workbench Analysis Adopt',
      },
      {
        method: 'POST',
        path: '/disputes/report',
        summary: 'agent 上报 dispute；shadow/auto 建立 Current Analysis，manual 只保存 dispute',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: Dispute {
    id, name, reporter_agent_id, claims: [claim_id],
    summary, status, created_at, resolved_at?
  }
}`,
        response: '200 OK（无 body）',
        caller: 'agent runtime',
      },
      {
        method: 'POST',
        path: '/disputes/{id}/resolve',
        summary: '人类直接关闭一条 dispute，可选通知 direct Claim holder',
        body: `{
  resolve_note: string,
  notify_affected_agents: boolean,
  resolution_type?: ResolutionType,
  resolution_basis?: ResolutionBasis,
  claim_assessments?: ClaimAssessment[]
}`,
        response: '204 No Content（成功无 body）',
        caller: 'workbench Disputes Resolve',
      },
      {
        method: 'POST',
        path: '/api/disputes/{id}/resolution/reject',
        summary: '驳回并替换当前 automatic Resolution',
        body: `{
  expected_resolution_id: string,
  rejection_reason: string,
  conclusion: string,
  resolution_type?: ResolutionType,
  resolution_basis?: ResolutionBasis,
  claim_assessments?: ClaimAssessment[]
}`,
        response: '201 Created + ArbitrationResolutionRecord；Resolution 已变化时 409',
        caller: 'workbench Reject & Replace',
      },
    ],
  },
  {
    group: 'Policies',
    endpoints: [
      {
        method: 'GET',
        path: '/api/policies',
        summary: '列出所有 policy 及其投递 / 事件流水',
        response: `PolicyRecordsResponse {
  policies: [ Policy ],
  outbox: [ OutboxEntry ],
  send_log: [ SendLogRow ],
  events: [ PolicyEventRecord ]
}

Policy {
  id: string,
  message_type: "policy_update"|"claim_attribute_update",
  name: string, statement: string, scope: string,
  status: "active"|"deprecated",
  created_at: ISO8601, updated_at?: ISO8601,
  target_agents?: [ string ]            // 省略 = 广播
}

OutboxEntry {
  inbox_id: string, maintainer_action_id: string,
  target_kind: "broadcast",              // 广播时无 target_agent
  // 或 target_kind: "targeted", target_agent: string
  created_at: ISO8601,
  offered_to: [ {
    agent_id: string, first_offered_at: ISO8601,
    last_offered_at: ISO8601, attempts: integer
  } ],
  delivered_to: [ { agent_id: string, sent_at: ISO8601 } ],
  inbox_message: { id, message_type, policy, handled_at?: ISO8601 }
}

SendLogRow {
  sent_at: ISO8601, agent_id: string, inbox_id: string,
  maintainer_action_id: string, policy_id: string,
  message_type: "policy_update"|"claim_attribute_update"
}`,
        caller: 'workbench Policies 页',
      },
      {
        method: 'POST',
        path: '/policies/policy-update',
        summary: '发布一条 Policy Update (PU)，广播或定向投递给 agent',
        body: `{
  name: string,            // 必填
  statement: string,      // 必填，Policy 文本
  scope: string,           // 必填
  target_agents: [ string ] | null   // null/省略 = 广播
}`,
        response: 'Policy（新建的那条，结构见 /api/policies）',
        caller: 'workbench New PU',
      },
      {
        method: 'POST',
        path: '/policies/claim-update-suggestion',
        summary: '发布一条 Claim Attribute Update (CAU) 建议',
        body: `{
  statement: string,                   // 必填
  target_agents: [ string ] | null     // null/省略 = 广播
}`,
        response: 'Policy（新建的那条，message_type=claim_attribute_update）',
        caller: 'workbench New CAU',
      },
      {
        method: 'POST',
        path: '/policies/policy-deprecation',
        summary: '废弃一条 active policy',
        body: `{ policy_id: string }`,
        response: `DeprecatePolicyResponse {
  pushed: 整数    // 向 agent 推送的投递条数
}`,
        caller: 'workbench Deprecate',
      },
    ],
  },
  {
    group: 'Sweep & Delivery',
    endpoints: [
      {
        method: 'GET',
        path: '/api/sweeps',
        summary: '列出 sweep 运行历史',
        response: `SweepRunRecord[] 每项 {
  run_id: string,
  triggered_at: ISO8601,
  trigger: "manual"|"maintainer_startup"|"ticker",
  report: ClaimSweepReport     // 见下
}

ClaimSweepReport {
  stale_claims:      [ [ agent_id, claim_id ] ],
  deprecated_claims: [ [ agent_id, claim_id ] ],
  notifications: [ {
    agent_id, stale_claims: [claim_id],
    deprecated_claims: [claim_id], policy_id, pushed: 整数
  } ],
  notification_errors: [ {
    agent_id, stale_claims, deprecated_claims, error: string
  } ]
}`,
        caller: 'workbench Sweep 页',
      },
      {
        method: 'POST',
        path: '/maintenance/sweep',
        summary: '立即触发一次 stale/deprecated claim 扫描，向 agent 推送 CAU',
        response: 'ClaimSweepReport（结构同 /api/sweeps 的 report 字段）',
        caller: 'workbench Trigger Sweep',
      },
      {
        method: 'GET',
        path: '/outbox',
        summary: '查询 outbox 待投递条目',
        response: `OutboxEntry[]（结构见 /api/policies 的 OutboxEntry）
支持 query 参数：?limit=整数&open=boolean`,
        caller: '内部 / 调试',
      },
      {
        method: 'GET',
        path: '/send_log',
        summary: '查询已发送投递日志',
        response: 'SendLogRow[]（结构见 /api/policies 的 SendLogRow）',
        caller: '内部 / 调试',
      },
      {
        method: 'GET',
        path: '/actions',
        summary: '查询 maintainer 动作记录',
        response: `MaintainerActionRow[] 每项 {
  created_at: ISO8601,
  maintainer_action_id: string,
  message_type: "policy_update"|"claim_attribute_update",
  policy_id: string, policy_name: string, policy_scope: string,
  policy_status: "active"|"deprecated",
  target_kind: "broadcast"|"targeted"|"mixed"|"unknown",
  inbox_ids: [ string ],
  target_agents: [ string ],
  delivered_agents: [ string ],
  outbox_entries: 整数,
  send_events: 整数
}`,
        caller: '内部 / 调试',
      },
      {
        method: 'POST',
        path: '/inbox/pull',
        summary: 'agent 拉取自己 inbox 里的待处理消息；ACK 前相同 inbox_id 可重投',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: { agent_id: string }
}`,
        response: `InboxMessage[] 每项 {
  id: string,
  message_type: "policy_update"|"claim_attribute_update",
  policy: Policy,           // 内嵌的完整 policy
  handled_at?: ISO8601
}`,
        caller: 'agent runtime',
      },
      {
        method: 'POST',
        path: '/inbox/ack',
        summary: 'agent 在消息原子落盘后确认持久收件；不代表已内化或应用',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: { agent_id: string, inbox_ids: [ string ] }
}`,
        response: '200 OK（无 body；重复 ACK 幂等）',
        caller: 'agent runtime',
      },
    ],
  },
  {
    group: 'HTTP Audits',
    endpoints: [
      {
        method: 'GET',
        path: '/api/audits',
        summary: '列出最近的 HTTP 审计记录',
        response: `HttpAuditRecord[] 每项 {
  audit_id: string, occurred_at: ISO8601,
  method: string, path: string,
  status_code: 整数,        // 如 200 / 401 / 500
  duration_ms: 整数,
  source_ip: string | null,
  request_body: string,    // 审计脱敏后的请求体
  response_body: string,   // 审计脱敏后的响应体
  resource_id: string | null,
  summary: string
}`,
        caller: 'workbench HTTP Audits 页',
      },
      {
        method: 'GET',
        path: '/api/audits/{id}',
        summary: '查询单条 HTTP 审计详情',
        response: 'HttpAuditRecord（结构同 /api/audits 的单项）',
        caller: 'workbench HTTP Audits Drawer',
      },
    ],
  },
  {
    group: 'Team Auth',
    endpoints: [
      {
        method: 'GET',
        path: '/api/team-auth/status',
        summary: '返回 maintainer/router team auth 开关状态',
        response: `{
  maintainer_team_auth_enabled: boolean,
  router_team_auth_enabled: boolean
}`,
        caller: 'workbench Team Auth 页',
      },
      {
        method: 'GET',
        path: '/api/team-auth/keys',
        summary: '列出团队 API key 台账行，不返回 hash 或明文 key',
        response: `TeamAuthKey[] 每项 {
  key_id: string,
  agent_id: string,
  generated_time: ISO8601,
  status: "active"|"revoked"
}`,
        caller: 'workbench Team Auth 页',
      },
      {
        method: 'POST',
        path: '/api/team-auth/keys',
        summary: '为普通 agent 创建新 key；router-service 是系统保留身份，明文 acn_key 只在本次响应返回',
        body: `{ agent_id: string }`,
        response: `{
  key: { key_id, agent_id, generated_time, status },
  acn_key: string
}`,
        caller: 'workbench Team Auth 页',
      },
      {
        method: 'POST',
        path: '/api/team-auth/keys/{key_id}/revoke',
        summary: '把 key 标记为 revoked',
        response: '{ key_id, agent_id, generated_time, status }',
        caller: 'workbench Team Auth 页',
      },
    ],
  },
  {
    group: 'Router Query',
    endpoints: [
      {
        method: 'POST',
        path: '/api/router-query',
        summary: '语义检索：按 scope + 语义查询候选 claim 与关联 dispute',
        body: `{
  scope: string,                 // 必填，如 "order-system / batch-order-submit"
  semantic_query: string | null  // 可选语义查询串
}`,
        response: `RouterQueryResult {
  candidate_claims: [ {
    ...Claim,                      // 展平的完整 claim 字段
    open_dispute_ids: [ string ],
    resolved_dispute_ids: [ string ]
  } ],
  disputes: [ {
    id, name, claim_ids: [string],
    summary, status: "open"|"resolved"
  } ],
  retrieval_debug: {
    mode: "lexical_only"|"vector_only"|"hybrid",
    failed_paths: [ string ],
    error_summaries: [ string ],
    lexical_hits: 整数,
    vector_hits: 整数,
    rerank_fallback: boolean,
    candidates: [ {
      claim_id, hit_sources: "both"|"lexical"|"vector"|"none",
      lexical_score: 非负整数, vector_score: 0..1000 的整数,
      rank_before_rerank, rank_after_rerank,
      vector_status: "pending"|"ready"|"failed"|"not_requested"
    } ]
  } | null
}`,
        caller: 'workbench Router Query 页',
      },
      {
        method: 'POST',
        path: '/claims/query',
        summary: 'router daemon 查询团队共享 claim 池',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: {
    scope: string,
    semantic_query: string | null
  }
}`,
        response: 'RouterQueryResult（结构同 /api/router-query）',
        caller: 'agent runtime；Workbench Router Query 由 maintainer 使用 router-service 调用',
      },
      {
        method: 'POST',
        path: '/claims/scopes/overview',
        summary: 'router daemon 返回团队 scope overview',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: {}
}`,
        response: `ScopesOverviewSnapshot {
  scopes: [ {
    scope, active_claims, stale_claims, open_disputes, resolved_disputes,
    latest_claim_created_at: ISO8601
  } ]
}`,
        caller: 'agent runtime；maintainer scope overview 使用 router-service 调用',
      },
    ],
  },
]

function methodTone(method: 'GET' | 'POST') {
  return method === 'POST' ? 'border-amber-200 bg-amber-50 text-amber-700' : 'border-emerald-200 bg-emerald-50 text-emerald-700'
}

function EndpointToggle({ entry }: { entry: EndpointEntry }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="rounded-lg border border-slate-200 bg-white">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-3 px-3 py-2 text-left transition hover:bg-slate-50"
      >
        <span className={`inline-flex w-14 shrink-0 justify-center rounded border px-1.5 py-0.5 font-mono text-[11px] font-semibold ${methodTone(entry.method)}`}>
          {entry.method}
        </span>
        <span className="min-w-0 flex-1 break-all font-mono text-xs text-slate-900">{entry.path}</span>
        <span className="hidden truncate text-xs text-slate-500 md:inline">{entry.summary}</span>
        <ChevronDown className={`h-3.5 w-3.5 shrink-0 text-slate-400 transition ${open ? 'rotate-180' : ''}`} />
      </button>
      {open ? (
        <div className="space-y-2.5 border-t border-slate-100 px-3 py-2.5 text-xs">
          <div><div className="text-slate-500">Summary</div><div className="text-slate-700">{entry.summary}</div></div>
          <div><div className="text-slate-500">Caller</div><div className="text-slate-700">{entry.caller}</div></div>
          {entry.body ? (
            <div>
              <div className="text-slate-500">Body</div>
              <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-words rounded-md border border-slate-200 bg-slate-50 px-2.5 py-2 font-mono text-[11px] leading-5 text-slate-700">{entry.body}</pre>
            </div>
          ) : null}
          <div>
            <div className="text-slate-500">Response</div>
            <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-words rounded-md border border-slate-200 bg-slate-50 px-2.5 py-2 font-mono text-[11px] leading-5 text-slate-700">{entry.response}</pre>
          </div>
        </div>
      ) : null}
    </div>
  )
}

export function SettingsPage() {
  const lastUpdatedAt = useWorkbenchUiStore((state) => state.lastUpdatedAt)
  const [authEnabled, setAuthEnabled] = useState<boolean | null>(null)
  const session = readAdminSession()

  const daemonHost = isStaticDemo ? 'demo' : window.location.hostname || 'localhost'
  const daemonPort = isStaticDemo
    ? 'demo'
    : window.location.port || (window.location.protocol === 'https:' ? '443' : '80')

  useEffect(() => {
    let active = true
    getAdminAuthStatus()
      .then((status) => {
        if (active) setAuthEnabled(status.enabled)
      })
      .catch(() => {
        if (active) setAuthEnabled(null)
      })
    return () => {
      active = false
    }
  }, [])

  const runtimeCards: Array<[string, string]> = [
    ['Daemon Host', daemonHost],
    ['Daemon Port', daemonPort],
    ['Admin Auth', authEnabled === null ? 'unknown' : authEnabled ? 'enabled' : 'disabled'],
    ['Signed-in User', session?.username ?? '—'],
    ['Last UI Refresh', formatDateTime(lastUpdatedAt)],
  ]
  const endpointGroups = ENDPOINT_GROUPS

  return (
    <PageContainer title="Settings" subtitle="Read-only runtime information and the maintainer daemon endpoint catalog.">
      <section>
        <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Runtime</h2>
        <div className="grid gap-2 md:grid-cols-3 xl:grid-cols-5">
          {runtimeCards.map(([label, value]) => (
            <div key={label} className="rounded-lg border border-slate-200 bg-white px-3 py-2.5">
              <div className="text-[11px] text-slate-500">{label}</div>
              <div className="mt-1 truncate font-mono text-sm font-semibold text-slate-900" title={value}>{value}</div>
            </div>
          ))}
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Known Endpoints</h2>
        {endpointGroups.map((g) => (
          <div key={g.group} className="space-y-2">
            <div className="px-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500">{g.group}</div>
            <div className="space-y-1.5">
              {g.endpoints.map((e) => (
                <EndpointToggle key={`${e.method} ${e.path}`} entry={e} />
              ))}
            </div>
          </div>
        ))}
      </section>
    </PageContainer>
  )
}
