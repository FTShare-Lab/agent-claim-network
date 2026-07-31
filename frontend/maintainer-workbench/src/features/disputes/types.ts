export type DisputeStatus = 'open' | 'resolved'

export type Dispute = {
  id: string
  name: string
  reporter_agent_id: string
  claims: string[]
  summary: string
  status: DisputeStatus
  created_at: string
  resolved_at?: string
}

export type ResolveDisputeRequest = {
  resolve_note: string
  notify_affected_agents: boolean
}
