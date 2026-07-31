import type { Dispute } from '../disputes/types'

export type MaintainerStatusCounts = {
  agents: number
  claims: number
  active_claims: number
  stale_claims: number
  deprecated_claims: number
  active_policies: number
  deprecated_policies: number
  open_disputes: number
  resolved_disputes: number
  outbox_entries: number
  send_events: number
}

export type AgentStatusSummary = {
  agent_id: string
  mirror_claims: number
  active_claims: number
  stale_claims: number
  deprecated_claims: number
}

export type Policy = {
  id: string
  message_type: 'policy_update' | 'claim_attribute_update'
  name: string
  statement: string
  scope: string
  status: 'active' | 'deprecated'
  created_at: string
  updated_at?: string
  target_agents?: string[]
}

export type MaintainerActionRow = {
  created_at: string
  maintainer_action_id: string
  message_type: 'policy_update' | 'claim_attribute_update'
  policy_id: string
  policy_name: string
  policy_scope: string
  policy_status: 'active' | 'deprecated'
  target_kind: 'broadcast' | 'targeted' | 'mixed' | 'unknown'
  inbox_ids: string[]
  target_agents: string[]
  delivered_agents: string[]
  outbox_entries: number
  send_events: number
}

export type SendLogRow = {
  sent_at: string
  agent_id: string
  inbox_id: string
  maintainer_action_id: string
  policy_id: string
  message_type: 'policy_update' | 'claim_attribute_update'
}

export type MaintainerStatusSnapshot = {
  generated_at: string
  counts: MaintainerStatusCounts
  agents: AgentStatusSummary[]
  policies: Policy[]
  disputes: Dispute[]
  actions: MaintainerActionRow[]
  send_log: SendLogRow[]
}

export type SweepRunRecord = {
  run_id: string
  triggered_at: string
  trigger: 'manual' | 'maintainer_startup' | 'ticker'
  report: {
    stale_claims: Array<[string, string]>
    deprecated_claims: Array<[string, string]>
    notifications: Array<{
      agent_id: string
      stale_claims: string[]
      deprecated_claims: string[]
      policy_id: string
      pushed: number
    }>
    notification_errors: Array<{
      agent_id: string
      stale_claims: string[]
      deprecated_claims: string[]
      error: string
    }>
  }
}

export type SweepScheduleStatus = {
  tick_interval_secs: number
  last_auto_sweep_at: string | null
  next_sweep_at: string | null
  last_auto_trigger: 'maintainer_startup' | 'ticker' | null
}

export type PolicyEventRecord = {
  event_id: string
  policy_id: string
  event_kind:
    | 'policy_update_published'
    | 'claim_attribute_update_published'
    | 'policy_deprecated'
  occurred_at: string
  policy_name: string
  policy_scope: string
  policy_status: 'active' | 'deprecated'
  message_type: 'policy_update' | 'claim_attribute_update'
  target_agents: string[]
  statement: string
}

export type AgentActivityRecord = {
  event_id: string
  agent_id: string
  activity_kind: 'inbox_pulled' | 'claim_uploaded' | 'dispute_reported'
  occurred_at: string
  summary: string
}

export type HttpAuditRecord = {
  audit_id: string
  occurred_at: string
  method: string
  path: string
  status_code: number
  duration_ms: number
  source_ip: string | null
  request_body: string
  response_body: string
  resource_id: string | null
  summary: string
}

export type DisputeResolutionEventRecord = {
  event_id: string
  dispute_id: string
  occurred_at: string
  summary: string | null
}

export type OverviewResponse = {
  snapshot: MaintainerStatusSnapshot
  latest_sweep: SweepRunRecord | null
  sweep_schedule: SweepScheduleStatus
  recent_policy_events: PolicyEventRecord[]
  recent_agent_activities: AgentActivityRecord[]
  recent_http_audits: HttpAuditRecord[]
  recent_dispute_resolutions: DisputeResolutionEventRecord[]
}
