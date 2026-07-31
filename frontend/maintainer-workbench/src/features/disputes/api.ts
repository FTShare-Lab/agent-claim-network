import { apiClient } from '../../lib/apiClient'
import type { Dispute, ResolveDisputeRequest } from './types'

export function listDisputes() {
  return apiClient.get<Dispute[]>('/api/disputes')
}

export function resolveDispute(id: string, request: ResolveDisputeRequest) {
  return apiClient.post<void>(`/disputes/${id}/resolve`, request)
}
