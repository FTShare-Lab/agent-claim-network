import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { DataTable } from './DataTable'

describe('DataTable', () => {
  it('renders rows and notifies row clicks', () => {
    const onRowClick = vi.fn()

    render(
      <DataTable
        columns={[
          { key: 'name', header: 'Name', render: (row: { name: string }) => row.name },
          { key: 'status', header: 'Status', render: (row: { status: string }) => row.status },
        ]}
        rows={[
          { id: 'claim-1', name: 'test_agent_outcome', status: 'active' },
          { id: 'claim-2', name: 'policy_delivery_state', status: 'stale' },
        ]}
        getRowId={(row) => row.id}
        onRowClick={onRowClick}
      />,
    )

    fireEvent.click(screen.getByRole('row', { name: /test_agent_outcome active/i }))
    expect(onRowClick).toHaveBeenCalledWith({
      id: 'claim-1',
      name: 'test_agent_outcome',
      status: 'active',
    })
  })

  it('renders the empty state when there are no rows', () => {
    render(
      <DataTable
        columns={[{ key: 'name', header: 'Name', render: (row: { name: string }) => row.name }]}
        rows={[]}
        getRowId={(row) => row.name}
        emptyState="No claims available."
      />,
    )

    expect(screen.getByText('No claims available.')).toBeInTheDocument()
  })

  it('activates clickable rows with Enter and Space', () => {
    const onRowClick = vi.fn()
    const row = { id: 'claim-1', name: 'payment_batch_timeout' }

    render(
      <DataTable
        columns={[{ key: 'name', header: 'Name', render: (item: typeof row) => item.name }]}
        rows={[row]}
        getRowId={(item) => item.id}
        onRowClick={onRowClick}
      />,
    )

    const interactiveRow = screen.getByRole('row', { name: /payment_batch_timeout/i })
    interactiveRow.focus()
    fireEvent.keyDown(interactiveRow, { key: 'Enter' })
    fireEvent.keyDown(interactiveRow, { key: ' ' })

    expect(onRowClick).toHaveBeenCalledTimes(2)
    expect(onRowClick).toHaveBeenNthCalledWith(1, row)
    expect(onRowClick).toHaveBeenNthCalledWith(2, row)
  })
})
