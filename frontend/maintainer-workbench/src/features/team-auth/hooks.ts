import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { createTeamAuthKey, getTeamAuthStatus, listTeamAuthKeys, revokeTeamAuthKey } from './api'

export function useTeamAuthStatusQuery() {
  return useQuery({ queryKey: ['team-auth-status'], queryFn: getTeamAuthStatus })
}

export function useTeamAuthKeysQuery() {
  return useQuery({ queryKey: ['team-auth-keys'], queryFn: listTeamAuthKeys })
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
