import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { listDisputes, resolveDispute } from './api'
import type { ResolveDisputeRequest } from './types'

export function useDisputesQuery() {
  return useQuery({ queryKey: ['disputes'], queryFn: listDisputes })
}

export function useResolveDisputeMutation() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ id, request }: { id: string; request: ResolveDisputeRequest }) => resolveDispute(id, request),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['disputes'] }),
        queryClient.invalidateQueries({ queryKey: ['claims'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
      ])
    },
  })
}
