import type { MaintainerActionRow, Policy } from '../overview/types'
import type { OutboxEntry, PolicyRecordsResponse } from './types'

export function policyFromOutbox(entry: OutboxEntry) {
  return entry.inbox_message.policy
}

export function policyIdFromOutbox(entry: OutboxEntry) {
  return policyFromOutbox(entry).id
}

export function messageTypeFromOutbox(entry: OutboxEntry) {
  return entry.inbox_message.message_type
}

export function targetKindLabel(targetKind: 'broadcast' | 'targeted') {
  return targetKind === 'broadcast' ? 'broadcast' : 'targeted'
}

export function targetLabelFromAction(action: MaintainerActionRow) {
  if (action.target_kind === 'broadcast') return 'broadcast'
  return action.target_agents.length ? action.target_agents.join(', ') : 'targeted'
}

export function targetLabelFromOutbox(entry: OutboxEntry) {
  if (entry.target_kind === 'broadcast') return 'broadcast'
  return entry.target_agent ?? 'targeted'
}

function offeredMarks(entry: OutboxEntry) {
  return entry.offered_to ?? []
}

function exposedAgentIds(entry: OutboxEntry) {
  return new Set([
    ...offeredMarks(entry).map((mark) => mark.agent_id),
    ...entry.delivered_to.map((mark) => mark.agent_id),
  ])
}

function eligibleDeprecatedRecipients(entry: OutboxEntry, outbox: OutboxEntry[]) {
  const policyId = policyIdFromOutbox(entry)
  const recipients = new Set<string>()
  for (const candidate of outbox) {
    if (
      candidate.target_kind !== 'broadcast' ||
      policyIdFromOutbox(candidate) !== policyId ||
      policyFromOutbox(candidate).status !== 'active'
    ) {
      continue
    }
    for (const agentId of exposedAgentIds(candidate)) recipients.add(agentId)
  }
  return recipients
}

export function isOutboxEntryOpen(entry: OutboxEntry, outbox: OutboxEntry[], policies: Policy[]) {
  if (entry.target_kind === 'targeted') {
    return entry.delivered_to.length === 0
  }

  const policy = policyFromOutbox(entry)
  if (policy.status === 'active') {
    return policies.some((current) => current.id === policy.id && current.status === 'active')
  }

  const expectedRecipients = eligibleDeprecatedRecipients(entry, outbox)
  if (expectedRecipients.size === 0) {
    return false
  }

  const delivered = new Set(entry.delivered_to.map((mark) => mark.agent_id))
  return Array.from(expectedRecipients).some((agentId) => !delivered.has(agentId))
}

export function openStateFromOutbox(entry: OutboxEntry, outbox: OutboxEntry[], policies: Policy[]) {
  return isOutboxEntryOpen(entry, outbox, policies) ? 'open' : 'closed'
}

export function deliveredCountFromOutbox(entry: OutboxEntry) {
  return entry.delivered_to.length
}

export function offeredCountFromOutbox(entry: OutboxEntry) {
  return offeredMarks(entry).length
}

export function lastSentAtFromOutbox(entry: OutboxEntry) {
  return entry.delivered_to
    .map((mark) => mark.sent_at)
    .sort((left, right) => new Date(right).getTime() - new Date(left).getTime())[0] ?? null
}

export function lastOfferedAtFromOutbox(entry: OutboxEntry) {
  return offeredMarks(entry)
    .map((mark) => mark.last_offered_at)
    .sort((left, right) => new Date(right).getTime() - new Date(left).getTime())[0] ?? null
}

export function findPolicyEvent(data: PolicyRecordsResponse, policyId: string) {
  return data.events.find((event) => event.policy_id === policyId) ?? null
}

export function deliverySummaryText(stats: ReturnType<typeof deriveDeliveryStats>) {
  return stats.expected === null
    ? `${stats.delivered} receipt ACKs for broadcast`
    : `${stats.delivered} / ${stats.expected} receipt ACKs`
}

function latestOutboxCohort(entries: OutboxEntry[]) {
  if (entries.length === 0) return []
  const latest = entries.reduce((current, entry) =>
    new Date(entry.created_at).getTime() > new Date(current.created_at).getTime() ? entry : current,
  )
  return entries.filter((entry) => entry.maintainer_action_id === latest.maintainer_action_id)
}

function deliveryOutboxForPolicy(policy: Policy, data: PolicyRecordsResponse) {
  const relatedOutbox = data.outbox.filter((entry) => policyIdFromOutbox(entry) === policy.id)
  const matchingStatus = relatedOutbox.filter((entry) => policyFromOutbox(entry).status === policy.status)
  return latestOutboxCohort(matchingStatus.length ? matchingStatus : relatedOutbox)
}

export function deriveDeliveryStats(policy: Policy, data: PolicyRecordsResponse) {
  const relatedOutbox = deliveryOutboxForPolicy(policy, data)
  const deliveredAgents = new Set(
    relatedOutbox.flatMap((entry) => entry.delivered_to.map((mark) => mark.agent_id)),
  )
  const targeted = policy.target_agents?.length ?? relatedOutbox.filter((entry) => entry.target_kind === 'targeted').length
  const expected = targeted > 0 ? targeted : null
  const outboxCount = relatedOutbox.length
  const sendCount = relatedOutbox.reduce((count, entry) => count + entry.delivered_to.length, 0)
  const lastDelivery = relatedOutbox
    .flatMap((entry) => entry.delivered_to.map((mark) => mark.sent_at))
    .sort((left, right) => new Date(right).getTime() - new Date(left).getTime())[0] ?? null
  const delivered = deliveredAgents.size
  const ratio = expected && expected > 0 ? Math.min(100, Math.round((delivered / expected) * 100)) : null
  const failed = expected && expected > delivered ? expected - delivered : null

  return {
    delivered,
    expected,
    ratio,
    failed,
    outboxCount,
    sendCount,
    lastDelivery,
  }
}

export function relatedPolicies(policy: Policy, data: PolicyRecordsResponse) {
  return data.policies
    .filter(
      (candidate) =>
        candidate.id !== policy.id &&
        (candidate.scope === policy.scope || candidate.message_type === policy.message_type),
    )
    .slice(0, 3)
}
