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
        summary: 'Validate administrator credentials',
        body: 'No JSON body. Send credentials in the HTTP Basic Auth header: Authorization: Basic <base64(user:pass)>.',
        response: '200 OK (valid credentials, empty body)\n401 Unauthorized (incorrect username or password)',
        caller: 'Workbench sign-in page',
      },
      {
        method: 'GET',
        path: '/api/admin-auth/status',
        summary: 'Check whether administrator authentication is enabled',
        response: '{\n  "enabled": boolean   // true = sign-in required, false = sign-in not required\n}',
        caller: 'Workbench route guard',
      },
    ],
  },
  {
    group: 'Overview & Status',
    endpoints: [
      {
        method: 'GET',
        path: '/health',
        summary: 'Liveness probe returning HTTP 200',
        response: '200 OK (empty body)',
        caller: 'Orchestration / health checks',
      },
      {
        method: 'GET',
        path: '/status',
        summary: 'Return the Maintainer status snapshot',
        response: `MaintainerStatusSnapshot {
  generated_at: ISO8601,
  counts: {
    agents, claims, active_claims, stale_claims, deprecated_claims,
    active_policies, deprecated_policies, open_disputes, resolved_disputes,
    outbox_entries, send_events   // all values are integer counts
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
        summary: 'Operations overview: snapshot, latest sweep, and recent events',
        response: `OverviewResponse {
  snapshot: MaintainerStatusSnapshot,        // same as /status
  latest_sweep: SweepRunRecord | null,       // latest sweep; see the Sweep group
  sweep_schedule: {
    tick_interval_secs: integer,
    last_auto_sweep_at: ISO8601 | null,
    next_sweep_at: ISO8601 | null,
    last_auto_trigger: "maintainer_startup"|"ticker"|null
  },
  recent_policy_events: [ PolicyEventRecord ],   // see the Policies group
  recent_agent_activities: [ AgentActivityRecord ],
  recent_http_audits: [ HttpAuditRecord ],       // see the HTTP Audits group
  recent_dispute_resolutions: [ {
    event_id: string, dispute_id: string,
    occurred_at: ISO8601, summary: string | null
  } ]
}`,
        caller: 'Workbench Overview page',
      },
    ],
  },
  {
    group: 'Agents',
    endpoints: [
      {
        method: 'GET',
        path: '/api/agents',
        summary: 'List registered agents with Claim counts, activity, and source IP',
        response: `AgentView[] entries: {
  agent_id: string,
  mirror_claims: integer,    // total mirrored Claims for this agent
  active_claims: integer,     // active Claim count
  stale_claims: integer,
  deprecated_claims: integer,
  last_source_ip: string | null,
  last_activity: {
    event_id: string, agent_id: string,
    activity_kind: "inbox_pulled"|"claim_uploaded"|"dispute_reported", occurred_at: ISO8601,
    summary: string
  } | null,
  recent_activities: [ objects with the same structure as last_activity ]
}`,
        caller: 'Workbench Agents page',
      },
    ],
  },
  {
    group: 'Claims',
    endpoints: [
      {
        method: 'GET',
        path: '/api/claims',
        summary: 'List Claim views with the Claim and related Dispute IDs',
        response: `ClaimView[] entries: {
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
        caller: 'Workbench Claims page',
      },
      {
        method: 'GET',
        path: '/api/claims/{id}',
        summary: 'Get details of one Claim',
        response: 'ClaimView (same structure as one /api/claims entry)',
        caller: 'workbench Claims Drawer',
      },
      {
        method: 'POST',
        path: '/claims/upload',
        summary: 'Upload or replace a Claim on behalf of an agent',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: Claim {
    id, name, statement, scope, holder,
    confidence, status, created_at, updated_at?,
    source_claim_ids: [ string ],
    evidence_summary: string
  }
}`,
        response: '200 OK (empty body)',
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
        summary: 'List all Disputes',
        response: `MaintainerDisputeRecord[] entries: {
  id: string, name: string,
  reporter_agent_id: string,
  claims: [ string ],          // direct Claim IDs
  summary: string,
  status: "open"|"resolved",
  created_at: ISO8601,
  resolved_at?: ISO8601,
  resolution?: DisputeResolution
}`,
        caller: 'Workbench Disputes page',
      },
      {
        method: 'GET',
        path: '/api/disputes/{id}',
        summary: 'Get one Dispute',
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
        summary: 'Get the current Analysis',
        response: '{ current_analysis? }',
        caller: 'workbench Disputes Drawer',
      },
      {
        method: 'POST',
        path: '/api/disputes/{id}/analyses',
        summary: 'Run Analysis explicitly and replace the current result',
        response: '202 Accepted + ArbitrationAnalysisSummary',
        caller: 'workbench Disputes Analyze',
      },
      {
        method: 'GET',
        path: '/api/disputes/{id}/analyses/{analysis_id}',
        summary: 'Get the frozen context, proposal, and verification',
        response: 'ArbitrationAnalysisDetail',
        caller: 'Workbench Analysis card',
      },
      {
        method: 'POST',
        path: '/api/disputes/{id}/analyses/{analysis_id}/adopt',
        summary: 'Adopt an approved Analysis without another model call',
        response: '201 Created + ArbitrationResolutionRecord; 409 if inputs changed or the Dispute is closed',
        caller: 'workbench Analysis Adopt',
      },
      {
        method: 'POST',
        path: '/disputes/report',
        summary: 'Report a Dispute; shadow/auto modes create the Current Analysis, while manual mode only saves the Dispute',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: Dispute {
    id, name, reporter_agent_id, claims: [claim_id],
    summary, status, created_at, resolved_at?
  }
}`,
        response: '200 OK (empty body)',
        caller: 'agent runtime',
      },
      {
        method: 'POST',
        path: '/disputes/{id}/resolve',
        summary: 'Close a Dispute manually, optionally notifying direct Claim holders',
        body: `{
  resolve_note: string,
  notify_affected_agents: boolean,
  resolution_type?: ResolutionType,
  resolution_basis?: ResolutionBasis,
  claim_assessments?: ClaimAssessment[]
}`,
        response: '204 No Content (success, empty body)',
        caller: 'workbench Disputes Resolve',
      },
      {
        method: 'POST',
        path: '/api/disputes/{id}/resolution/reject',
        summary: 'Reject and replace the current automatic Resolution',
        body: `{
  expected_resolution_id: string,
  rejection_reason: string,
  conclusion: string,
  resolution_type?: ResolutionType,
  resolution_basis?: ResolutionBasis,
  claim_assessments?: ClaimAssessment[]
}`,
        response: '201 Created + ArbitrationResolutionRecord; 409 if the Resolution changed',
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
        summary: 'List Policies and their delivery and event history',
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
  target_agents?: [ string ]            // omitted = broadcast
}

OutboxEntry {
  inbox_id: string, maintainer_action_id: string,
  target_kind: "broadcast",              // target_agent is absent for broadcasts
  // or target_kind: "targeted", target_agent: string
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
        caller: 'Workbench Policies page',
      },
      {
        method: 'POST',
        path: '/policies/policy-update',
        summary: 'Publish a Policy Update (PU) to all agents or selected agents',
        body: `{
  name: string,            // required
  statement: string,      // required Policy text
  scope: string,           // required
  target_agents: [ string ] | null   // null/omitted = broadcast
}`,
        response: 'New Policy (see /api/policies for its structure)',
        caller: 'workbench New PU',
      },
      {
        method: 'POST',
        path: '/policies/claim-update-suggestion',
        summary: 'Publish a Claim Attribute Update (CAU) suggestion',
        body: `{
  statement: string,                   // required
  target_agents: [ string ] | null     // null/omitted = broadcast
}`,
        response: 'New Policy with message_type=claim_attribute_update',
        caller: 'workbench New CAU',
      },
      {
        method: 'POST',
        path: '/policies/policy-deprecation',
        summary: 'Deprecate an active Policy',
        body: `{ policy_id: string }`,
        response: `DeprecatePolicyResponse {
  pushed: integer    // number of deliveries queued for agents
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
        summary: 'List sweep history',
        response: `SweepRunRecord[] entries: {
  run_id: string,
  triggered_at: ISO8601,
  trigger: "manual"|"maintainer_startup"|"ticker",
  report: ClaimSweepReport     // see below
}

ClaimSweepReport {
  stale_claims:      [ [ agent_id, claim_id ] ],
  deprecated_claims: [ [ agent_id, claim_id ] ],
  notifications: [ {
    agent_id, stale_claims: [claim_id],
    deprecated_claims: [claim_id], policy_id, pushed: integer
  } ],
  notification_errors: [ {
    agent_id, stale_claims, deprecated_claims, error: string
  } ]
}`,
        caller: 'Workbench Sweep page',
      },
      {
        method: 'POST',
        path: '/maintenance/sweep',
        summary: 'Run a stale/deprecated Claim sweep immediately and send CAU suggestions to agents',
        response: 'ClaimSweepReport (same as the report field in /api/sweeps)',
        caller: 'workbench Trigger Sweep',
      },
      {
        method: 'GET',
        path: '/outbox',
        summary: 'List pending outbox deliveries',
        response: `OutboxEntry[] (see OutboxEntry in /api/policies)
Supported query parameters: ?limit=integer&open=boolean`,
        caller: 'Internal / debugging',
      },
      {
        method: 'GET',
        path: '/send_log',
        summary: 'List sent delivery logs',
        response: 'SendLogRow[] (see SendLogRow in /api/policies)',
        caller: 'Internal / debugging',
      },
      {
        method: 'GET',
        path: '/actions',
        summary: 'List Maintainer action records',
        response: `MaintainerActionRow[] entries: {
  created_at: ISO8601,
  maintainer_action_id: string,
  message_type: "policy_update"|"claim_attribute_update",
  policy_id: string, policy_name: string, policy_scope: string,
  policy_status: "active"|"deprecated",
  target_kind: "broadcast"|"targeted"|"mixed"|"unknown",
  inbox_ids: [ string ],
  target_agents: [ string ],
  delivered_agents: [ string ],
  outbox_entries: integer,
  send_events: integer
}`,
        caller: 'Internal / debugging',
      },
      {
        method: 'POST',
        path: '/inbox/pull',
        summary: 'Pull pending messages for the agent; the same inbox_id may be redelivered before ACK',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: { agent_id: string }
}`,
        response: `InboxMessage[] entries: {
  id: string,
  message_type: "policy_update"|"claim_attribute_update",
  policy: Policy,           // complete embedded Policy
  handled_at?: ISO8601
}`,
        caller: 'agent runtime',
      },
      {
        method: 'POST',
        path: '/inbox/ack',
        summary: 'Acknowledge durable receipt after atomic persistence; this does not confirm internalization or application',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: { agent_id: string, inbox_ids: [ string ] }
}`,
        response: '200 OK (empty body; repeated ACKs are idempotent)',
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
        summary: 'List recent HTTP audit records',
        response: `HttpAuditRecord[] entries: {
  audit_id: string, occurred_at: ISO8601,
  method: string, path: string,
  status_code: integer,        // for example, 200 / 401 / 500
  duration_ms: integer,
  source_ip: string | null,
  request_body: string,    // redacted request body
  response_body: string,   // redacted response body
  resource_id: string | null,
  summary: string
}`,
        caller: 'Workbench HTTP Audits page',
      },
      {
        method: 'GET',
        path: '/api/audits/{id}',
        summary: 'Get one HTTP audit record',
        response: 'HttpAuditRecord (same as one /api/audits entry)',
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
        summary: 'Return the Maintainer and Router team authentication settings',
        response: `{
  maintainer_team_auth_enabled: boolean,
  router_team_auth_enabled: boolean
}`,
        caller: 'Workbench Team Auth page',
      },
      {
        method: 'GET',
        path: '/api/team-auth/keys',
        summary: 'List team API key records without hashes or plaintext keys',
        response: `TeamAuthKey[] entries: {
  key_id: string,
  agent_id: string,
  generated_time: ISO8601,
  status: "active"|"revoked"
}`,
        caller: 'Workbench Team Auth page',
      },
      {
        method: 'POST',
        path: '/api/team-auth/keys',
        summary: 'Create a key for a regular agent; router-service is a reserved identity and plaintext acn_key is returned only in this response',
        body: `{ agent_id: string }`,
        response: `{
  key: { key_id, agent_id, generated_time, status },
  acn_key: string
}`,
        caller: 'Workbench Team Auth page',
      },
      {
        method: 'POST',
        path: '/api/team-auth/keys/{key_id}/revoke',
        summary: 'Mark a key as revoked',
        response: '{ key_id, agent_id, generated_time, status }',
        caller: 'Workbench Team Auth page',
      },
    ],
  },
  {
    group: 'Router Query',
    endpoints: [
      {
        method: 'POST',
        path: '/api/router-query',
        summary: 'Retrieve candidate Claims and related Disputes by scope and semantic query',
        body: `{
  scope: string,                 // required, for example "order-system / batch-order-submit"
  semantic_query: string | null  // optional semantic query
}`,
        response: `RouterQueryResult {
  candidate_claims: [ {
    ...Claim,                      // complete flattened Claim fields
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
    lexical_hits: integer,
    vector_hits: integer,
    rerank_fallback: boolean,
    candidates: [ {
      claim_id, hit_sources: "both"|"lexical"|"vector"|"none",
      lexical_score: nonnegative integer, vector_score: integer from 0 to 1000,
      rank_before_rerank, rank_after_rerank,
      vector_status: "pending"|"ready"|"failed"|"not_requested"
    } ]
  } | null
}`,
        caller: 'Workbench Router Query page',
      },
      {
        method: 'POST',
        path: '/claims/query',
        summary: 'Query the shared team Claim pool through the Router daemon',
        body: `{
  auth: { agent_id: string, acn_key: string },
  data: {
    scope: string,
    semantic_query: string | null
  }
}`,
        response: 'RouterQueryResult (same structure as /api/router-query)',
        caller: 'Agent runtime; Workbench Router Query calls through Maintainer using router-service',
      },
      {
        method: 'POST',
        path: '/claims/scopes/overview',
        summary: 'Return the team scope overview from the Router daemon',
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
        caller: 'Agent runtime; Maintainer requests the scope overview using router-service',
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
