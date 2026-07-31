import { apiClient } from '../../lib/apiClient'
import type { AgentView } from './types'

export function listAgents() {
  return apiClient.get<AgentView[]>('/api/agents')
}
