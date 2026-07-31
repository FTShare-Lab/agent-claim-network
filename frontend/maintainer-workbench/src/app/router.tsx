import type { RouteObject } from 'react-router'

import { AppShell } from '../layouts/AppShell'
import { AgentsPage } from '../pages/AgentsPage'
import { ClaimsPage } from '../pages/ClaimsPage'
import { DisputesPage } from '../pages/DisputesPage'
import { HttpAuditsPage } from '../pages/HttpAuditsPage'
import { LoginPage, RequireAdminAuth } from '../pages/LoginPage'
import { OverviewPage } from '../pages/OverviewPage'
import { PoliciesPage } from '../pages/PoliciesPage'
import { RouterQueryPage } from '../pages/RouterQueryPage'
import { SettingsPage } from '../pages/SettingsPage'
import { SweepPage } from '../pages/SweepPage'
import { TeamAuthPage } from '../pages/TeamAuthPage'

export const routes: RouteObject[] = [
  {
    path: '/login',
    element: <LoginPage />,
  },
  {
    path: '/',
    element: (
      <RequireAdminAuth>
        <AppShell />
      </RequireAdminAuth>
    ),
    children: [
      { index: true, element: <OverviewPage /> },
      { path: 'disputes', element: <DisputesPage /> },
      { path: 'claims', element: <ClaimsPage /> },
      { path: 'policies', element: <PoliciesPage /> },
      { path: 'agents', element: <AgentsPage /> },
      { path: 'sweep', element: <SweepPage /> },
      { path: 'router-query', element: <RouterQueryPage /> },
      { path: 'team-auth', element: <TeamAuthPage /> },
      { path: 'http-audits', element: <HttpAuditsPage /> },
      { path: 'settings', element: <SettingsPage /> },
    ],
  },
]
