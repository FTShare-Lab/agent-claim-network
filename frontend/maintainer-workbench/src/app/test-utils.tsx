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
  const router = createMemoryRouter(routes, {
    initialEntries: [initialEntry],
  })

  return (
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>
  )
}
