import { useMutation } from '@tanstack/react-query'

import { runRouterQuery } from './api'

export function useRouterQueryMutation() {
  return useMutation({ mutationFn: runRouterQuery })
}
