import { useQuery } from '@tanstack/react-query'

import { listAudits } from './api'

export function useAuditsQuery() {
  return useQuery({ queryKey: ['audits'], queryFn: listAudits })
}
