export type AgentActivityRecord = {
  event_id: string
  agent_id: string
  activity_kind: 'inbox_pulled' | 'claim_uploaded' | 'dispute_reported'
  occurred_at: string
  summary: string
}

export type AgentView = {
  agent_id: string
  mirror_claims: number
  active_claims: number
  stale_claims: number
  deprecated_claims: number
  last_source_ip: string | null
  last_activity: AgentActivityRecord | null
  recent_activities: AgentActivityRecord[]
}
