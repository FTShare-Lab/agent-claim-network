import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { type PropsWithChildren, useState } from 'react'

type AppProvidersProps = PropsWithChildren<{
  queryClient?: QueryClient
}>

export function AppProviders({ children, queryClient }: AppProvidersProps) {
  const [defaultQueryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 15_000,
            refetchOnWindowFocus: false,
          },
        },
      }),
  )

  return <QueryClientProvider client={queryClient ?? defaultQueryClient}>{children}</QueryClientProvider>
}
