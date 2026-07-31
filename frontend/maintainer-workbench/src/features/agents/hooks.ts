import { useQuery } from '@tanstack/react-query'

import { listAgents } from './api'

export function useAgentsQuery() {
  return useQuery({ queryKey: ['agents'], queryFn: listAgents })
}
