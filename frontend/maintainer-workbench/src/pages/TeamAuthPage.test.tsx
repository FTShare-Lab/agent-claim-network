import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { teamAuthErrorMessage } from '../features/team-auth/errors'
import { copyTextToClipboard } from '../lib/clipboard'

describe('errorMessage', () => {
  it('formats backend text as a complete sentence', () => {
    expect(teamAuthErrorMessage(new Error('maintainer admin auth must be enabled'))).toBe(
      'Maintainer admin auth must be enabled.',
    )
    expect(teamAuthErrorMessage(new Error('Already complete.'))).toBe('Already complete.')
  })
})

describe('copyTextToClipboard', () => {
  let clipboardDescriptor: PropertyDescriptor | undefined
  let execCommandDescriptor: PropertyDescriptor | undefined

  beforeEach(() => {
    clipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, 'clipboard')
    execCommandDescriptor = Object.getOwnPropertyDescriptor(document, 'execCommand')
  })

  afterEach(() => {
    restoreProperty(navigator, 'clipboard', clipboardDescriptor)
    restoreProperty(document, 'execCommand', execCommandDescriptor)
    vi.restoreAllMocks()
  })

  it('uses the Clipboard API when it is available', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    const execCommand = vi.fn(() => true)
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText },
    })
    Object.defineProperty(document, 'execCommand', {
      configurable: true,
      value: execCommand,
    })

    await expect(copyTextToClipboard('acn_secret')).resolves.toBe(true)

    expect(writeText).toHaveBeenCalledWith('acn_secret')
    expect(execCommand).not.toHaveBeenCalled()
  })

  it('falls back to selected textarea copy when Clipboard API is unavailable', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: undefined,
    })
    const execCommand = vi.fn(() => {
      expect(document.querySelector('textarea')).toHaveValue('acn_secret')
      return true
    })
    Object.defineProperty(document, 'execCommand', {
      configurable: true,
      value: execCommand,
    })

    await expect(copyTextToClipboard('acn_secret')).resolves.toBe(true)

    expect(execCommand).toHaveBeenCalledWith('copy')
    expect(document.querySelector('textarea')).not.toBeInTheDocument()
  })

  it('falls back when the Clipboard API rejects the copy request', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('blocked'))
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText },
    })
    const execCommand = vi.fn(() => true)
    Object.defineProperty(document, 'execCommand', {
      configurable: true,
      value: execCommand,
    })

    await expect(copyTextToClipboard('acn_secret')).resolves.toBe(true)

    expect(writeText).toHaveBeenCalledWith('acn_secret')
    expect(execCommand).toHaveBeenCalledWith('copy')
    expect(document.querySelector('textarea')).not.toBeInTheDocument()
  })
})

function restoreProperty(
  target: object,
  key: string,
  descriptor: PropertyDescriptor | undefined,
) {
  if (descriptor) {
    Object.defineProperty(target, key, descriptor)
    return
  }

  Reflect.deleteProperty(target, key)
}
