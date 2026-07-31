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
