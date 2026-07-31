import { apiClient } from '../../lib/apiClient'
import type { CreateTeamAuthKeyRequest, CreateTeamAuthKeyResponse, TeamAuthKey, TeamAuthStatus } from './types'

export function getTeamAuthStatus() {
  return apiClient.get<TeamAuthStatus>('/api/team-auth/status')
}

export function listTeamAuthKeys() {
  return apiClient.get<TeamAuthKey[]>('/api/team-auth/keys')
}

export function createTeamAuthKey(body: CreateTeamAuthKeyRequest) {
  return apiClient.post<CreateTeamAuthKeyResponse>('/api/team-auth/keys', body)
}

export function revokeTeamAuthKey(keyId: string) {
  return apiClient.post<TeamAuthKey>(`/api/team-auth/keys/${encodeURIComponent(keyId)}/revoke`)
}
