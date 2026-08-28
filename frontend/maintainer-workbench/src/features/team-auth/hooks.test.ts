import { describe, expect, it } from 'vitest'

import { ApiError } from '../../lib/apiClient'
import { shouldRetryTeamAuthKeys } from './hooks'

describe('shouldRetryTeamAuthKeys', () => {
  it('does not retry a non-recoverable client response', () => {
    const forbidden = new ApiError('Admin auth is disabled.', 403, 'Forbidden')

    expect(shouldRetryTeamAuthKeys(0, forbidden)).toBe(false)
  })

  it('retains bounded retries for transient failures', () => {
    expect(shouldRetryTeamAuthKeys(0, new Error('network unavailable'))).toBe(true)
    expect(shouldRetryTeamAuthKeys(2, new ApiError('Unavailable', 503, 'Unavailable'))).toBe(true)
    expect(shouldRetryTeamAuthKeys(3, new Error('network unavailable'))).toBe(false)
  })
})
