import { type FormEvent, type ReactNode, useEffect, useMemo, useState } from 'react'
import { ArrowRight, Eye, ShieldCheck } from 'lucide-react'
import { Navigate, useLocation, useNavigate } from 'react-router'

import { getAdminAuthStatus, readAdminSession, subscribeAdminSession, verifyAdminCredentials } from '../features/auth/session'
import { isStaticDemo } from '../lib/runtime'

type LoginLocationState = {
  from?: {
    pathname?: string
    search?: string
  }
}

export function RequireAdminAuth({ children }: { children: ReactNode }) {
  if (isStaticDemo) return children
  return <AdminAuthGuard>{children}</AdminAuthGuard>
}

function AdminAuthGuard({ children }: { children: ReactNode }) {
  const location = useLocation()
  const [session, setSession] = useState(() => readAdminSession())
  const [authEnabled, setAuthEnabled] = useState<boolean | null>(null)

  useEffect(() => subscribeAdminSession(() => setSession(readAdminSession())), [])
  useEffect(() => {
    if (session) {
      return
    }
    let active = true
    getAdminAuthStatus()
      .then((status) => {
        if (active) setAuthEnabled(status.enabled)
      })
      .catch(() => {
        if (active) setAuthEnabled(true)
      })
    return () => {
      active = false
    }
  }, [session])

  if (session) {
    return children
  }

  if (authEnabled === null) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-[var(--bg-page)]" role="status" aria-live="polite">
        <div className="flex items-center gap-2 font-mono text-xs text-slate-600">
          <span aria-hidden="true" className="h-2 w-2 animate-pulse rounded-full bg-blue-700" />
          Checking workbench access…
        </div>
      </div>
    )
  }

  if (authEnabled) {
    return <Navigate to="/login" state={{ from: location }} replace />
  }

  return children
}

export function LoginPage() {
  if (isStaticDemo) return <Navigate to="/" replace />
  return <AdminLoginPage />
}

function AdminLoginPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const session = readAdminSession()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [isPasswordVisible, setIsPasswordVisible] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const redirectTo = useMemo(() => {
    const state = location.state as LoginLocationState | null
    const pathname = state?.from?.pathname && state.from.pathname !== '/login' ? state.from.pathname : '/'
    return `${pathname}${state?.from?.search ?? ''}`
  }, [location.state])

  useEffect(() => {
    if (session) {
      navigate(redirectTo, { replace: true })
      return
    }
    let active = true
    getAdminAuthStatus()
      .then((status) => {
        if (active && !status.enabled) {
          navigate(redirectTo, { replace: true })
        }
      })
      .catch(() => {})
    return () => {
      active = false
    }
  }, [navigate, redirectTo, session])

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)
    setIsSubmitting(true)

    try {
      const ok = await verifyAdminCredentials(username.trim(), password)
      if (!ok) {
        setError('用户名或密码不正确')
        return
      }
      navigate(redirectTo, { replace: true })
    } catch (err) {
      setError(err instanceof Error ? err.message : '登录失败，请稍后重试')
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <main className="relative flex min-h-screen items-center justify-center overflow-hidden bg-[var(--bg-page)] px-4 py-10 sm:px-6">
      <div aria-hidden="true" className="absolute -left-24 top-[-15%] h-[420px] w-[420px] rounded-full bg-[var(--accent-weak)] blur-3xl" />
      <div aria-hidden="true" className="absolute -right-36 bottom-[-20%] h-[460px] w-[460px] rounded-full bg-[#eaf6f0] blur-3xl" />
      <div className="relative w-full max-w-[440px]">
        <header className="mb-6 flex items-center gap-3 px-1">
          <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-[var(--accent)] font-mono text-[11px] font-bold tracking-tight text-white">ACN</div>
          <div>
            <div className="text-sm font-[700] text-slate-900">Maintainer Workbench</div>
            <div className="text-[11px] text-slate-500">Agent Claim Network</div>
          </div>
        </header>

        <section className="rounded-2xl bg-white p-6 shadow-[var(--shadow-raised)] ring-1 ring-slate-200/70 sm:p-8">
          <form onSubmit={handleSubmit} className="w-full" aria-describedby="login-guidance">
            <div className="mb-7">
              <div className="mb-4 flex h-10 w-10 items-center justify-center rounded-xl bg-[var(--accent-weak)] text-[var(--accent)]">
                <ShieldCheck aria-hidden="true" className="h-[18px] w-[18px]" />
              </div>
              <h1 className="text-2xl font-[650] tracking-[-0.025em] text-slate-950">Admin credentials</h1>
              <p id="login-guidance" className="mt-2 text-sm leading-6 text-slate-500">
                Sign in to review provenance, resolve disputes, and publish guidance. Credentials are verified by the local Maintainer daemon.
              </p>
            </div>

            <div className="space-y-4">
              <div>
                <label htmlFor="admin-username" className="text-xs font-semibold text-slate-700">Username</label>
                <input
                  id="admin-username"
                  autoComplete="username"
                  className="mt-1.5 min-h-11 w-full rounded-lg border border-slate-300 bg-white px-3 text-sm text-slate-900 transition-[border-color,box-shadow] duration-150 placeholder:text-slate-400 hover:border-slate-400 focus:border-[var(--accent)]"
                  value={username}
                  onChange={(event) => setUsername(event.target.value)}
                  required
                />
              </div>

              <div>
                <label htmlFor="admin-password" className="text-xs font-semibold text-slate-700">Password</label>
                <div className="relative mt-1.5">
                  <input
                    id="admin-password"
                    autoComplete="current-password"
                    className="min-h-11 w-full rounded-lg border border-slate-300 bg-white px-3 pr-12 text-sm text-slate-900 transition-[border-color,box-shadow] duration-150 placeholder:text-slate-400 hover:border-slate-400 focus:border-[var(--accent)]"
                    type={isPasswordVisible ? 'text' : 'password'}
                    value={password}
                    onChange={(event) => setPassword(event.target.value)}
                    required
                  />
                  <button
                    type="button"
                    aria-label={isPasswordVisible ? 'Hide password' : 'Show password'}
                    aria-pressed={isPasswordVisible}
                    title={isPasswordVisible ? 'Hide password' : 'Show password'}
                    onClick={() => setIsPasswordVisible((visible) => !visible)}
                    className="absolute right-1.5 top-1/2 flex h-9 w-9 -translate-y-1/2 items-center justify-center rounded-lg text-slate-500 transition-colors duration-150 hover:bg-slate-100 hover:text-slate-800"
                  >
                    <Eye aria-hidden="true" className="h-4 w-4" />
                  </button>
                </div>
              </div>
            </div>

            {error ? (
              <div role="alert" aria-live="assertive" className="mt-4 rounded-md border border-rose-200 bg-rose-50 px-3 py-2.5 text-xs font-semibold text-rose-800">
                {error}
              </div>
            ) : null}

            <button
              type="submit"
              disabled={isSubmitting}
              aria-busy={isSubmitting}
              className="mt-6 inline-flex min-h-11 w-full items-center justify-center gap-2 rounded-lg bg-[var(--accent)] px-3 text-sm font-semibold text-white transition-colors duration-150 hover:bg-[var(--accent-strong)] disabled:cursor-wait disabled:bg-slate-300"
            >
              {isSubmitting ? 'Checking…' : 'Open Workbench'}
              <ArrowRight aria-hidden="true" className="h-4 w-4" />
            </button>
            <p className="mt-4 text-center text-[11px] text-slate-500" aria-live="polite">
              {isSubmitting ? 'Verifying credentials with the Maintainer daemon…' : 'Local admin session · Agent judgment remains private'}
            </p>
          </form>
        </section>
        <p className="mt-5 px-4 text-center text-xs leading-5 text-slate-500">
          Maintainer governs shared policy and disputes; it cannot overwrite an Agent&apos;s private Memory or local Claim.
        </p>
      </div>
    </main>
  )
}
