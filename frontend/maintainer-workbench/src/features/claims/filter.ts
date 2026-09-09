// 保持列表与知识树的 Claim 搜索语义一致。
import type { ClaimView } from './types'

export type ClaimFilterValues = {
  keyword: string
  agent: string
  status: string
  scope: string
  onlyDisputed: boolean
}

export const emptyClaimFilters: ClaimFilterValues = {
  keyword: '', agent: 'all', status: 'all', scope: '', onlyDisputed: false,
}

export function matchesClaimFilters(row: ClaimView, filters: ClaimFilterValues) {
  const { claim } = row
  const haystack = `${claim.id} ${claim.name} ${claim.statement} ${claim.evidence_summary} ${claim.scope}`.toLowerCase()
  return (!filters.keyword || haystack.includes(filters.keyword.toLowerCase()))
    && (filters.agent === 'all' || claim.holder === filters.agent)
    && (filters.status === 'all' || claim.status === filters.status)
    && (!filters.scope || claim.scope.toLowerCase().includes(filters.scope.toLowerCase()))
    && (!filters.onlyDisputed || row.open_dispute_ids.length + row.resolved_dispute_ids.length > 0)
}
