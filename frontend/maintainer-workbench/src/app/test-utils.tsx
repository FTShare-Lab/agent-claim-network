import { QueryClient } from '@tanstack/react-query'
import { createMemoryRouter } from 'react-router'
import { RouterProvider } from 'react-router/dom'

import { routes } from './router'
import { AppProviders } from './providers'

type WorkbenchInitialEntry =
  | string
  | {
      pathname: string
      search?: string
      hash?: string
      state?: unknown
    }

export function renderWorkbenchRoute(initialEntry: WorkbenchInitialEntry) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        staleTime: 0,
      },
    },
  })
  const router = createMemoryRouter(routes, {
    initialEntries: [initialEntry],
  })

  return (
    <AppProviders queryClient={queryClient}>
      <RouterProvider router={router} />
    </AppProviders>
  )
}
