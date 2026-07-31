import { useQuery } from '@tanstack/react-query'

import { getClaim, listClaims } from './api'

export function useClaimsQuery() {
  return useQuery({ queryKey: ['claims'], queryFn: listClaims })
}

export function useClaimQuery(id: string | null) {
  return useQuery({
    queryKey: ['claims', id],
    queryFn: () => getClaim(id!),
    enabled: Boolean(id),
  })
}
