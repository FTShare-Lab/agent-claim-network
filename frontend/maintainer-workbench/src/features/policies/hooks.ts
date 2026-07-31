import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { createPolicy, deprecatePolicy, listPolicies, suggestClaimAttributeUpdate } from './api'

export function usePoliciesQuery() {
  return useQuery({ queryKey: ['policies'], queryFn: listPolicies })
}

export function useCreatePolicyMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: createPolicy,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['policies'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
        queryClient.invalidateQueries({ queryKey: ['agents'] }),
      ])
    },
  })
}

export function useClaimAttributeSuggestionMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: suggestClaimAttributeUpdate,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['policies'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
        queryClient.invalidateQueries({ queryKey: ['agents'] }),
      ])
    },
  })
}

export function useDeprecatePolicyMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: deprecatePolicy,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['policies'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
        queryClient.invalidateQueries({ queryKey: ['agents'] }),
      ])
    },
  })
}
