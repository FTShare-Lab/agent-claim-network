import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { useState } from 'react'
import { afterEach, describe, expect, it } from 'vitest'

import { DetailDrawer } from './DetailDrawer'

afterEach(cleanup)

function DrawerHarness({ modal }: { modal: boolean }) {
  const [open, setOpen] = useState(false)

  return (
    <div id="root">
      <button type="button" onClick={() => setOpen(true)}>Open claim</button>
      <DetailDrawer
        open={open}
        modal={modal}
        size="default"
        label="Claim"
        title="payment_timeout"
        onClose={() => setOpen(false)}
      >
        <p>Claim detail</p>
      </DetailDrawer>
    </div>
  )
}

describe('DetailDrawer', () => {
  it('keeps the source context interactive for a non-modal inspector', async () => {
    render(<DrawerHarness modal={false} />)
    const trigger = screen.getByRole('button', { name: 'Open claim' })

    trigger.focus()
    fireEvent.click(trigger)
    const dialog = screen.getByRole('dialog')
    expect(dialog).not.toHaveAttribute('aria-modal')
    expect(document.getElementById('root')).not.toHaveAttribute('inert')
    expect(document.body.style.overflow).not.toBe('hidden')

    await waitFor(() => expect(screen.getByRole('button', { name: 'Close detail drawer' })).toHaveFocus())
    fireEvent.keyDown(document, { key: 'Escape' })

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    expect(trigger).toHaveFocus()
  })

  it('locks context for a modal focus workflow and restores it on close', async () => {
    render(<DrawerHarness modal />)
    const trigger = screen.getByRole('button', { name: 'Open claim' })

    trigger.focus()
    fireEvent.click(trigger)
    expect(screen.getByRole('dialog')).toHaveAttribute('aria-modal', 'true')
    expect(document.getElementById('root')).toHaveAttribute('inert')
    expect(document.body.style.overflow).toBe('hidden')

    fireEvent.keyDown(document, { key: 'Escape' })

    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    expect(document.getElementById('root')).not.toHaveAttribute('inert')
    expect(document.body.style.overflow).not.toBe('hidden')
    expect(trigger).toHaveFocus()
  })
})
