type ClaimOrderRow = {
  claim: {
    id: string
    status: 'active' | 'stale' | 'deprecated'
    created_at: string
    updated_at?: string
  }
}

type AgentOrderRow = {
  agent_id: string
  last_activity: {
    occurred_at: string
  } | null
}

type PolicyOrderRow = {
  id: string
  status: 'active' | 'deprecated'
  created_at: string
}

type ActionOrderRow = {
  maintainer_action_id: string
  created_at: string
}

type SendLogOrderRow = {
  sent_at: string
  agent_id: string
  inbox_id: string
  maintainer_action_id: string
}

type OutboxOrderRow = {
  inbox_id: string
  created_at: string
}

function compareTimeDescending(left: string, right: string) {
  return Date.parse(right) - Date.parse(left)
}

function compareTextAscending(left: string, right: string) {
  return left.localeCompare(right)
}

const CLAIM_STATUS_ORDER: Record<ClaimOrderRow['claim']['status'], number> = {
  active: 0,
  stale: 1,
  deprecated: 2,
}

const POLICY_STATUS_ORDER: Record<PolicyOrderRow['status'], number> = {
  active: 0,
  deprecated: 1,
}

export function orderClaimsByStatusAndRecentChange<T extends ClaimOrderRow>(rows: readonly T[]) {
  return [...rows].sort((left, right) => {
    const effectiveLeft = left.claim.updated_at ?? left.claim.created_at
    const effectiveRight = right.claim.updated_at ?? right.claim.created_at
    return (
      CLAIM_STATUS_ORDER[left.claim.status] - CLAIM_STATUS_ORDER[right.claim.status] ||
      compareTimeDescending(effectiveLeft, effectiveRight) ||
      compareTimeDescending(left.claim.created_at, right.claim.created_at) ||
      compareTextAscending(left.claim.id, right.claim.id)
    )
  })
}

export function orderAgentsByRecentActivity<T extends AgentOrderRow>(rows: readonly T[]) {
  return [...rows].sort((left, right) => {
    if (left.last_activity && right.last_activity) {
      const byActivity = compareTimeDescending(
        left.last_activity.occurred_at,
        right.last_activity.occurred_at,
      )
      if (byActivity !== 0) return byActivity
    } else if (left.last_activity) {
      return -1
    } else if (right.last_activity) {
      return 1
    }
    return compareTextAscending(left.agent_id, right.agent_id)
  })
}

export function orderPoliciesByStatusAndPublishedAt<T extends PolicyOrderRow>(rows: readonly T[]) {
  return [...rows].sort(
    (left, right) =>
      POLICY_STATUS_ORDER[left.status] - POLICY_STATUS_ORDER[right.status] ||
      compareTimeDescending(left.created_at, right.created_at) ||
      compareTextAscending(left.id, right.id),
  )
}

export function orderActionsByCreatedAt<T extends ActionOrderRow>(rows: readonly T[]) {
  return [...rows].sort(
    (left, right) =>
      compareTimeDescending(left.created_at, right.created_at) ||
      compareTextAscending(left.maintainer_action_id, right.maintainer_action_id),
  )
}

export function orderSendLogBySentAt<T extends SendLogOrderRow>(rows: readonly T[]) {
  return [...rows].sort(
    (left, right) =>
      compareTimeDescending(left.sent_at, right.sent_at) ||
      compareTextAscending(left.agent_id, right.agent_id) ||
      compareTextAscending(left.inbox_id, right.inbox_id) ||
      compareTextAscending(left.maintainer_action_id, right.maintainer_action_id),
  )
}

export function orderOutboxByCreatedAt<T extends OutboxOrderRow>(rows: readonly T[]) {
  return [...rows].sort(
    (left, right) =>
      compareTimeDescending(left.created_at, right.created_at) ||
      compareTextAscending(left.inbox_id, right.inbox_id),
  )
}
