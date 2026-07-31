import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const isPages = mode === 'pages'
  const pagesBase = normalizePagesBase(process.env.ACN_PAGES_BASE)

  return {
    base: isPages ? `${pagesBase}app/` : '/app/',
    build: isPages
      ? {
          outDir: 'dist-pages/app',
        }
      : undefined,
    plugins: [react(), tailwindcss()],
  }
})

function normalizePagesBase(value: string | undefined) {
  const raw = value?.trim() || '/agent-claim-network/'
  const normalized = raw.replace(/^\/+|\/+$/g, '')
  return normalized ? `/${normalized}/` : '/'
}
