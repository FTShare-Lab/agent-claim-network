import { clearAdminSession, readAdminSession } from '../features/auth/session'
import { requestStaticDemoData } from './demoData'
import { isStaticDemo } from './runtime'

export type QueryValue = string | number | boolean | null | undefined

export class ApiError extends Error {
  readonly status: number
  readonly statusText: string

  constructor(
    message: string,
    status: number,
    statusText: string,
  ) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.statusText = statusText
  }
}

function buildQuery(params?: Record<string, QueryValue>) {
  const searchParams = new URLSearchParams()
  Object.entries(params ?? {}).forEach(([key, value]) => {
    if (value === null || value === undefined || value === '') return
    searchParams.set(key, String(value))
  })
  const queryString = searchParams.toString()
  return queryString ? `?${queryString}` : ''
}

async function request<T>(
  path: string,
  init?: RequestInit,
  query?: Record<string, QueryValue>,
): Promise<T> {
  if (isStaticDemo) {
    return requestStaticDemoData<T>(path, init)
  }

  const session = readAdminSession()
  const response = await fetch(`${path}${buildQuery(query)}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      'X-ACN-Workbench': '1',
      ...(session ? { Authorization: session.authorization } : {}),
      ...(init?.headers ?? {}),
    },
  })

  if (!response.ok) {
    if (response.status === 401) {
      clearAdminSession()
    }
    const text = await response.text()
    throw new ApiError(
      text || `${response.status} ${response.statusText}`,
      response.status,
      response.statusText,
    )
  }

  if (response.status === 204) {
    return undefined as T
  }

  return response.json() as Promise<T>
}

export const apiClient = {
  get<T>(path: string, query?: Record<string, QueryValue>) {
    return request<T>(path, { method: 'GET' }, query)
  },
  post<T>(path: string, body?: unknown) {
    return request<T>(path, {
      method: 'POST',
      body: body === undefined ? undefined : JSON.stringify(body),
    })
  },
}
