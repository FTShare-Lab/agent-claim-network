import type { HttpAuditRecord } from '../audits/types'
import type { Policy, SendLogRow } from '../overview/types'
import type { OutboxEntry } from '../policies/types'
import { policyIdFromOutbox } from '../policies/derived'

type AgentDeliverySummaryArgs = {
  agentId: string
  outbox: OutboxEntry[]
  policies: Policy[]
  sendLog: SendLogRow[]
  audits: HttpAuditRecord[]
}

export function deriveAgentDeliverySummary({
  agentId,
  outbox,
  policies,
  sendLog,
  audits,
}: AgentDeliverySummaryArgs) {
  const deliveredRows = sendLog.filter((row) => row.agent_id === agentId)
  const deliveredCount = deliveredRows.length
  const lastDelivery = deliveredRows[0]?.sent_at ?? null

  const activePolicyIds = new Set(
    policies.filter((policy) => policy.status === 'active').map((policy) => policy.id),
  )
  const exposedBroadcastPolicies = new Set<string>()
  for (const entry of outbox) {
    if (
      entry.target_kind === 'broadcast' &&
      entry.inbox_message.policy.status === 'active' &&
      ((entry.offered_to ?? []).some((mark) => mark.agent_id === agentId) ||
        entry.delivered_to.some((mark) => mark.agent_id === agentId))
    ) {
      exposedBroadcastPolicies.add(policyIdFromOutbox(entry))
    }
  }
  const openEntries = outbox.filter((entry) => {
    const delivered = entry.delivered_to.some((mark) => mark.agent_id === agentId)
    if (delivered) return false

    if (entry.target_kind === 'targeted') {
      return entry.target_agent === agentId
    }

    const policy = entry.inbox_message.policy
    if (policy.status === 'active') {
      return activePolicyIds.has(policy.id)
    }

    return exposedBroadcastPolicies.has(policyIdFromOutbox(entry))
  })

  const pullAudits = audits.filter(
    (audit) =>
      audit.path === '/inbox/pull' &&
      audit.status_code < 400 &&
      audit.request_body.includes(agentId),
  )

  return {
    deliveredCount,
    openCount: openEntries.length,
    lastDelivery,
    recentPullCount: pullAudits.length,
    lastPullAt: pullAudits[0]?.occurred_at ?? null,
  }
}
