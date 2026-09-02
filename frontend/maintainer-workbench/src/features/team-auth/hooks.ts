import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { ApiError } from '../../lib/apiClient'
import { createTeamAuthKey, getTeamAuthStatus, listTeamAuthKeys, revokeTeamAuthKey } from './api'

export function shouldRetryTeamAuthKeys(failureCount: number, error: unknown) {
  if (error instanceof ApiError && error.status >= 400 && error.status < 500) return false
  return failureCount < 3
}

export function useTeamAuthStatusQuery() {
  return useQuery({ queryKey: ['team-auth-status'], queryFn: getTeamAuthStatus })
}

export function useTeamAuthKeysQuery() {
  return useQuery({
    queryKey: ['team-auth-keys'],
    queryFn: listTeamAuthKeys,
    retry: shouldRetryTeamAuthKeys,
  })
}

export function useCreateTeamAuthKeyMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: createTeamAuthKey,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['team-auth-keys'] }),
        queryClient.invalidateQueries({ queryKey: ['team-auth-status'] }),
      ])
    },
  })
}

export function useRevokeTeamAuthKeyMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: revokeTeamAuthKey,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['team-auth-keys'] }),
        queryClient.invalidateQueries({ queryKey: ['team-auth-status'] }),
      ])
    },
  })
}
