import { apiClient } from '../../lib/apiClient'
import type { OverviewResponse } from './types'

export function getOverview() {
  return apiClient.get<OverviewResponse>('/api/overview')
}
