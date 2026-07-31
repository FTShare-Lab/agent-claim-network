import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { createBrowserRouter, createHashRouter } from 'react-router'
import { RouterProvider } from 'react-router/dom'

import './index.css'
import { AppProviders } from './app/providers'
import { routes } from './app/router'
import { isStaticDemo } from './lib/runtime'

const router = isStaticDemo
  ? createHashRouter(routes)
  : createBrowserRouter(routes, { basename: '/app' })

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <AppProviders>
      <RouterProvider router={router} />
    </AppProviders>
  </StrictMode>,
)
