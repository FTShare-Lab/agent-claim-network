import { apiClient } from '../../lib/apiClient'
import type {
  ClaimAttributeSuggestionRequest,
  CreatePolicyRequest,
  PolicyRecordsResponse,
} from './types'

export function listPolicies() {
  return apiClient.get<PolicyRecordsResponse>('/api/policies')
}

export function createPolicy(body: CreatePolicyRequest) {
  return apiClient.post('/policies/policy-update', body)
}

export function suggestClaimAttributeUpdate(body: ClaimAttributeSuggestionRequest) {
  return apiClient.post('/policies/claim-update-suggestion', body)
}

export function deprecatePolicy(policy_id: string) {
  return apiClient.post('/policies/policy-deprecation', { policy_id })
}
