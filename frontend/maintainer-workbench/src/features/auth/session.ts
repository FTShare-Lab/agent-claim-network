// Cookie 由服务端管理；本地只存非凭据的界面元数据和跨标签变更标记。
import { isStaticDemo } from '../../lib/runtime'
import { withSessionLock } from './sessionLock'

const STORAGE_KEY = 'acn-maintainer-admin-session-v2'
const LEGACY_KEY = 'acn-maintainer-admin-auth'
const SESSION_EVENT = 'acn-maintainer-admin-session-change'

export type AdminSession = {
  id: string
  username: string
  expires_at: number
}

type AuthStatus = { enabled: boolean; session?: AdminSession }

export function buildBasicAuthHeader(username: string, password: string) {
  const bytes = new TextEncoder().encode(`${username}:${password}`)
  let binary = ''
  bytes.forEach((byte) => { binary += String.fromCharCode(byte) })
  return `Basic ${window.btoa(binary)}`
}

export function readAdminSession(): AdminSession | null {
  // 清除旧版本保存的密码编码，不迁移为长期存储。
  window.sessionStorage.removeItem(LEGACY_KEY)
  window.localStorage.removeItem(LEGACY_KEY)
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    const session = raw ? JSON.parse(raw).session as Partial<AdminSession> | null : null
    if (!session || typeof session.id !== 'string' || typeof session.username !== 'string'
      || typeof session.expires_at !== 'number') return null
    return session as AdminSession
  } catch {
    return null
  }
}

function saveAdminSession(session: AdminSession | null) {
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ session, revision: Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) => byte.toString(16).padStart(2, '0')).join('') }))
  window.dispatchEvent(new Event(SESSION_EVENT))
}

// 失败响应只能使发起请求时的会话失效，不能覆盖后续登录。
export function clearAdminSession(expectedId: string | undefined) {
  if (!expectedId || readAdminSession()?.id !== expectedId) return
  saveAdminSession(null)
}

export function subscribeAdminSession(listener: () => void) {
  const onStorage = (event: StorageEvent) => {
    if (event.storageArea === window.localStorage && (event.key === STORAGE_KEY || event.key === null)) listener()
  }
  window.addEventListener(SESSION_EVENT, listener)
  window.addEventListener('storage', onStorage)
  return () => {
    window.removeEventListener(SESSION_EVENT, listener)
    window.removeEventListener('storage', onStorage)
  }
}

export async function verifyAdminCredentials(username: string, password: string) {
  if (isStaticDemo) return false
  return withSessionLock(async () => {
    const response = await fetch('/api/admin-auth/login', {
      method: 'POST', credentials: 'same-origin', cache: 'no-store',
      headers: { Authorization: buildBasicAuthHeader(username, password), 'X-ACN-Workbench': '1' },
    })
    if (!response.ok) return false
    if (response.status !== 204) saveAdminSession(await response.json() as AdminSession)
    return true
  })
}

export async function logoutAdmin() {
  return withSessionLock(async () => {
    const session = readAdminSession()
    const response = await fetch('/api/admin-auth/logout', {
      method: 'POST', credentials: 'same-origin', cache: 'no-store',
      headers: { 'X-ACN-Workbench': '1', ...(session ? { 'X-ACN-Admin-Session': session.id } : {}) },
    })
    if (!response.ok) throw new Error('Sign-out failed. Please retry.')
    saveAdminSession(null)
  })
}

export async function getAdminAuthStatus(): Promise<AuthStatus> {
  if (isStaticDemo) return { enabled: false }
  const readStatus = async () => {
    const before = window.localStorage.getItem(STORAGE_KEY)
    const response = await fetch('/api/admin-auth/status', {
      credentials: 'same-origin', cache: 'no-store', headers: { 'X-ACN-Workbench': '1' },
    })
    if (!response.ok) throw new Error(`${response.status} ${response.statusText}`)
    const status = await response.json() as AuthStatus
    if (before === window.localStorage.getItem(STORAGE_KEY)
      && readAdminSession()?.id !== status.session?.id) saveAdminSession(status.session ?? null)
    return status
  }
  // 未启用管理员鉴权的普通 HTTP 管理台仍可读取状态；Cookie 变更必须持有锁。
  return navigator.locks ? withSessionLock(readStatus) : readStatus()
}
