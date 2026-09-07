import type { RouteObject } from 'react-router'

import { AppShell } from '../layouts/AppShell'
import { LoginPage, RequireAdminAuth } from '../pages/LoginPage'
import { OverviewPage } from '../pages/OverviewPage'

export const routes: RouteObject[] = [
  {
    path: '/login',
    element: <LoginPage />,
  },
  {
    path: '/',
    hydrateFallbackElement: <p role="status" className="p-6 text-sm text-slate-500">Loading workbench…</p>,
    element: (
      <RequireAdminAuth>
        <AppShell />
      </RequireAdminAuth>
    ),
    children: [
      { index: true, element: <OverviewPage /> },
      { path: 'disputes', lazy: async () => ({ Component: (await import('../pages/DisputesPage')).DisputesPage }) },
      { path: 'claims', lazy: async () => ({ Component: (await import('../pages/ClaimsPage')).ClaimsPage }) },
      { path: 'policies', lazy: async () => ({ Component: (await import('../pages/PoliciesPage')).PoliciesPage }) },
      { path: 'agents', lazy: async () => ({ Component: (await import('../pages/AgentsPage')).AgentsPage }) },
      { path: 'sweep', lazy: async () => ({ Component: (await import('../pages/SweepPage')).SweepPage }) },
      { path: 'router-query', lazy: async () => ({ Component: (await import('../pages/RouterQueryPage')).RouterQueryPage }) },
      { path: 'team-auth', lazy: async () => ({ Component: (await import('../pages/TeamAuthPage')).TeamAuthPage }) },
      { path: 'http-audits', lazy: async () => ({ Component: (await import('../pages/HttpAuditsPage')).HttpAuditsPage }) },
      { path: 'settings', lazy: async () => ({ Component: (await import('../pages/SettingsPage')).SettingsPage }) },
    ],
  },
]
