import { isStaticDemo } from '../../lib/runtime'

const STORAGE_KEY = 'acn-maintainer-admin-auth'
const SESSION_EVENT = 'acn-maintainer-admin-session-change'

export type AdminSession = {
  username: string
  authorization: string
}

export function buildBasicAuthHeader(username: string, password: string) {
  const bytes = new TextEncoder().encode(`${username}:${password}`)
  let binary = ''
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte)
  })
  return `Basic ${window.btoa(binary)}`
}

export function readAdminSession(): AdminSession | null {
  try {
    const raw = window.sessionStorage.getItem(STORAGE_KEY)
    if (!raw) return null
    const session = JSON.parse(raw) as Partial<AdminSession>
    if (!session.username || !session.authorization) return null
    return {
      username: session.username,
      authorization: session.authorization,
    }
  } catch {
    return null
  }
}

export function saveAdminSession(username: string, authorization: string) {
  window.sessionStorage.setItem(
    STORAGE_KEY,
    JSON.stringify({
      username,
      authorization,
    } satisfies AdminSession),
  )
  dispatchSessionChange()
}

export function clearAdminSession() {
  window.sessionStorage.removeItem(STORAGE_KEY)
  dispatchSessionChange()
}

export function subscribeAdminSession(listener: () => void) {
  window.addEventListener(SESSION_EVENT, listener)
  return () => window.removeEventListener(SESSION_EVENT, listener)
}

function dispatchSessionChange() {
  window.dispatchEvent(new Event(SESSION_EVENT))
}

export async function verifyAdminCredentials(username: string, password: string) {
  if (isStaticDemo) return false

  const authorization = buildBasicAuthHeader(username, password)
  const response = await fetch('/api/admin-auth/check', {
    method: 'POST',
    headers: {
      Authorization: authorization,
      'X-ACN-Workbench': '1',
    },
  })

  if (!response.ok) {
    return false
  }

  saveAdminSession(username, authorization)
  return true
}

export async function getAdminAuthStatus() {
  if (isStaticDemo) return { enabled: false }

  const response = await fetch('/api/admin-auth/status', {
    headers: {
      'X-ACN-Workbench': '1',
    },
  })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`)
  }
  return response.json() as Promise<{ enabled: boolean }>
}
