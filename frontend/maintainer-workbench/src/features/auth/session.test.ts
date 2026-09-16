import { describe, expect, it } from 'vitest'

import { buildBasicAuthHeader } from './session'

function decodeBasicAuthHeader(header: string) {
  const encoded = header.replace(/^Basic /, '')
  const binary = window.atob(encoded)
  const bytes = Uint8Array.from(Array.from(binary), (char) => char.charCodeAt(0))
  return new TextDecoder().decode(bytes)
}

describe('admin auth session', () => {
  it('encodes non-ascii Basic Auth credentials as UTF-8', () => {
    const header = buildBasicAuthHeader('管理员', '密钥')

    expect(decodeBasicAuthHeader(header)).toBe('管理员:密钥')
  })
})

import { afterEach, beforeEach, vi } from 'vitest'
import { clearAdminSession, getAdminAuthStatus, logoutAdmin, readAdminSession, subscribeAdminSession, verifyAdminCredentials } from './session'
import { apiClient } from '../../lib/apiClient'
import { seedAdminSession } from '../../test/adminSession'

const sessionKey = 'acn-maintainer-admin-session-v2'
const metadata = (id: string) => ({ id, username: 'admin', expires_at: 4102444800 })

describe('persistent Cookie session coordination', () => {
  beforeEach(() => { localStorage.clear(); sessionStorage.clear() })
  afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks() })

  it('persists metadata when HTTP does not expose crypto.randomUUID', async () => {
    vi.stubGlobal('crypto', { getRandomValues: crypto.getRandomValues.bind(crypto) })
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify(metadata('http')))))
    expect(await verifyAdminCredentials('admin', 'example-password')).toBe(true)
    expect(readAdminSession()?.id).toBe('http')
  })

  it('sends Basic only during login and persists no password or authorization', async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(new Response(JSON.stringify(metadata('new'))))
      .mockResolvedValueOnce(new Response('{}'))
    vi.stubGlobal('fetch', fetchMock)
    expect(await verifyAdminCredentials('admin', 'example-password')).toBe(true)
    await apiClient.get('/api/overview')
    expect(fetchMock.mock.calls[0][0]).toBe('/api/admin-auth/login')
    expect(fetchMock.mock.calls[1][1].headers).not.toHaveProperty('Authorization')
    expect(fetchMock.mock.calls[1][1].headers['X-ACN-Admin-Session']).toBe('new')
    expect(localStorage.getItem(sessionKey)).not.toMatch(/password|authorization|Basic/i)
    sessionStorage.clear()
    expect(readAdminSession()?.id).toBe('new')
  })

  it('restores from the server Cookie even without local metadata and removes legacy credentials', async () => {
    sessionStorage.setItem('acn-maintainer-admin-auth', JSON.stringify({ authorization: 'Basic obsolete' }))
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ enabled: true, session: metadata('restored') }))))
    await getAdminAuthStatus()
    expect(readAdminSession()?.id).toBe('restored')
    expect(sessionStorage.length).toBe(0)
  })

  it('notifies local and other-tab subscribers and cleans up listeners', async () => {
    seedAdminSession()
    const listener = vi.fn()
    const unsubscribe = subscribeAdminSession(listener)
    clearAdminSession('test-session')
    expect(listener).toHaveBeenCalledTimes(1)
    seedAdminSession('other-tab')
    window.dispatchEvent(new StorageEvent('storage', { key: sessionKey, storageArea: localStorage }))
    expect(listener).toHaveBeenCalledTimes(2)
    expect(readAdminSession()?.id).toBe('other-tab')
    unsubscribe()
    clearAdminSession('other-tab')
    expect(listener).toHaveBeenCalledTimes(2)
  })

  it('ignores a delayed 401 after a newer login and clears a currently rejected session', async () => {
    seedAdminSession('old')
    let resolve!: (response: Response) => void
    vi.stubGlobal('fetch', vi.fn().mockImplementationOnce(() => new Promise<Response>((done) => { resolve = done }))
      .mockResolvedValueOnce(new Response(JSON.stringify(metadata('new'))))
      .mockResolvedValueOnce(new Response('expired', { status: 401 })))
    const oldRequest = apiClient.get('/api/overview')
    const rejection = expect(oldRequest).rejects.toMatchObject({ status: 401 })
    await verifyAdminCredentials('admin', 'example-password')
    resolve(new Response('expired', { status: 401 }))
    await rejection
    expect(readAdminSession()?.id).toBe('new')
    await expect(apiClient.get('/api/overview')).rejects.toMatchObject({ status: 401 })
    expect(readAdminSession()).toBeNull()
  })

  it('revokes at the server before clearing UI state and keeps a failed logout retryable', async () => {
    seedAdminSession()
    const fetchMock = vi.fn().mockResolvedValueOnce(new Response('offline', { status: 503 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)
    await expect(logoutAdmin()).rejects.toThrow('Sign-out failed')
    expect(readAdminSession()).not.toBeNull()
    await logoutAdmin()
    expect(fetchMock.mock.calls[1][1].headers['X-ACN-Admin-Session']).toBe('test-session')
    expect(readAdminSession()).toBeNull()
  })

  it('serializes concurrent logout and login until the old Cookie deletion finishes', async () => {
    seedAdminSession('old')
    let resolve!: (response: Response) => void
    const fetchMock = vi.fn().mockImplementationOnce(() => new Promise<Response>((done) => { resolve = done }))
      .mockResolvedValueOnce(new Response(JSON.stringify(metadata('new'))))
    vi.stubGlobal('fetch', fetchMock)
    const logout = logoutAdmin()
    const login = verifyAdminCredentials('admin', 'example-password')
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    resolve(new Response(null, { status: 204 }))
    await Promise.all([logout, login])
    expect(readAdminSession()?.id).toBe('new')
  })

  it('clears expired metadata on restoration and allows a fresh login', async () => {
    seedAdminSession('expired')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({ enabled: true })))
      .mockResolvedValueOnce(new Response(JSON.stringify(metadata('fresh')))))
    await getAdminAuthStatus()
    expect(readAdminSession()).toBeNull()
    await verifyAdminCredentials('admin', 'example-password')
    expect(readAdminSession()?.id).toBe('fresh')
  })
})


it('keeps auth-disabled HTTP workbenches accessible without Web Locks', async () => {
  const locks = navigator.locks
  Object.defineProperty(navigator, 'locks', { configurable: true, value: undefined })
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ enabled: false }))))
  try {
    expect(await getAdminAuthStatus()).toEqual({ enabled: false })
  } finally {
    Object.defineProperty(navigator, 'locks', { configurable: true, value: locks })
    vi.unstubAllGlobals()
  }
})
