export type SweepNotification = {
  agent_id: string
  stale_claims: string[]
  deprecated_claims: string[]
  policy_id: string
  pushed: number
}

export type SweepNotificationError = {
  agent_id: string
  stale_claims: string[]
  deprecated_claims: string[]
  error: string
}

export type ClaimSweepReport = {
  stale_claims: Array<[string, string]>
  deprecated_claims: Array<[string, string]>
  notifications: SweepNotification[]
  notification_errors: SweepNotificationError[]
}

export type SweepRunRecord = {
  run_id: string
  triggered_at: string
  trigger: 'manual' | 'maintainer_startup' | 'ticker'
  report: ClaimSweepReport
}
