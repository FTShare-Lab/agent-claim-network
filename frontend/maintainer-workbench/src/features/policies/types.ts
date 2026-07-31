import type { Policy, PolicyEventRecord, SendLogRow } from '../overview/types'

export type CreatePolicyRequest = {
  name: string
  statement: string
  scope: string
  target_agents?: string[]
}

export type ClaimAttributeSuggestionRequest = {
  statement: string
  target_agents?: string[]
}

export type DeliveredMark = {
  agent_id: string
  sent_at: string
}

export type OfferedMark = {
  agent_id: string
  first_offered_at: string
  last_offered_at: string
  attempts: number
}

export type OutboxMessageSnapshot = {
  id: string
  message_type: 'policy_update' | 'claim_attribute_update'
  policy: Policy
  handled_at?: string
}

type OutboxEntryBase = {
  inbox_id: string
  maintainer_action_id: string
  created_at: string
  /** 旧 Maintainer 响应可能缺失；派生逻辑统一按空数组处理。 */
  offered_to?: OfferedMark[]
  delivered_to: DeliveredMark[]
  inbox_message: OutboxMessageSnapshot
}

export type OutboxEntry = OutboxEntryBase &
  (
    | {
        target_kind: 'broadcast'
        target_agent?: never
      }
    | {
        target_kind: 'targeted'
        target_agent: string
      }
  )

export type PolicyRecordsResponse = {
  policies: Policy[]
  outbox: OutboxEntry[]
  send_log: SendLogRow[]
  events: PolicyEventRecord[]
}
