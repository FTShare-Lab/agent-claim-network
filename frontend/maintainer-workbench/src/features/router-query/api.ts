import { apiClient } from '../../lib/apiClient'
import type { AgentQuery, RouterQueryResult } from './types'

export function runRouterQuery(body: AgentQuery) {
  return apiClient.post<RouterQueryResult>('/api/router-query', body)
}
