import { apiClient } from '../../lib/apiClient'
import type { ClaimSweepReport, SweepRunRecord } from './types'

export function listSweeps() {
  return apiClient.get<SweepRunRecord[]>('/api/sweeps')
}

export function triggerSweep() {
  return apiClient.post<ClaimSweepReport>('/maintenance/sweep')
}
