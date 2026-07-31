import { apiClient } from '../../lib/apiClient'
import type { HttpAuditRecord } from './types'

export function listAudits() {
  return apiClient.get<HttpAuditRecord[]>('/api/audits')
}
