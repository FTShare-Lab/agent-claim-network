import { apiClient } from '../../lib/apiClient'
import type { ClaimView } from './types'

export function listClaims() {
  return apiClient.get<ClaimView[]>('/api/claims')
}

export function getClaim(id: string) {
  return apiClient.get<ClaimView>(`/api/claims/${id}`)
}
