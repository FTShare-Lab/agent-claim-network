import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { listSweeps, triggerSweep } from './api'

export function useSweepsQuery() {
  return useQuery({ queryKey: ['sweeps'], queryFn: listSweeps })
}

export function useTriggerSweepMutation() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: triggerSweep,
    onSuccess: () =>
      Promise.all([
        queryClient.invalidateQueries({ queryKey: ['sweeps'] }),
        queryClient.invalidateQueries({ queryKey: ['overview'] }),
        queryClient.invalidateQueries({ queryKey: ['claims'] }),
        queryClient.invalidateQueries({ queryKey: ['policies'] }),
        queryClient.invalidateQueries({ queryKey: ['agents'] }),
      ]),
  })
}
