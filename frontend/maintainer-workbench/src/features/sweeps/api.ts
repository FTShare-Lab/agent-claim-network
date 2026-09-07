import { apiClient } from '../../lib/apiClient'
import { listClaims } from '../claims/api'
import type { OutboxEntry } from '../policies/types'
import type { ClaimSweepReport, SweepRunRecord } from './types'

export function listSweeps() {
  return apiClient.get<SweepRunRecord[]>('/api/sweeps')
}

export function triggerSweep() {
  return apiClient.post<ClaimSweepReport>('/maintenance/sweep')
}

export async function getSweepComparison() {
  const [claims, outbox] = await Promise.all([
    listClaims(),
    apiClient.get<OutboxEntry[]>('/outbox'),
  ])
  return { claims, outbox }
}
