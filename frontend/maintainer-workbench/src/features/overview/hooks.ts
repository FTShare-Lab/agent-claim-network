import { useQuery } from '@tanstack/react-query'

import { getOverview } from './api'

export function useOverviewQuery() {
  return useQuery({ queryKey: ['overview'], queryFn: getOverview })
}
