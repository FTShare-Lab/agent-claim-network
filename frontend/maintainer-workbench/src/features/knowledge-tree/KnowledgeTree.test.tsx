// 验证大树的界面展开，以及 Fit 对真实缩放范围的处理。
import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { createKnowledgePreview, knowledgePreviewRoots } from '../../test/knowledgePreview'
import { knowledgeClaims, knowledgePolicies, makeClaim } from '../../test/knowledgeFixtures'
import { KnowledgeTree } from './KnowledgeTree'
import { indexKnowledge, NODE_HEIGHT } from './tree'

const sample = createKnowledgePreview(Date.parse('2026-09-09T08:00:00Z'))
const index = indexKnowledge(sample.claims, sample.policies)
afterEach(cleanup)

describe('knowledge tree branch controls', () => {
  it('starts a wide tree with four branches and expands incrementally or fully', () => {
    render(<KnowledgeTree index={index} rootId={knowledgePreviewRoots.wide} direction="successors" onSelect={vi.fn()} />)
    const canvas = within(screen.getByRole('region', { name: 'Claims flow canvas' }))
    const titleButtons = () => canvas.getAllByRole('button', { pressed: false })
    expect(titleButtons()).toHaveLength(5)
    expect(screen.getByText('5 displayed nodes · 5 unique items')).toBeInTheDocument()
    fireEvent.click(canvas.getByRole('button', { name: 'Show more branches for 宽树案例：512 条独立采纳路径' }))
    expect(titleButtons()).toHaveLength(9)
    fireEvent.click(screen.getByRole('button', { name: 'Expand all' }))
    expect(titleButtons()).toHaveLength(500)
    expect(screen.getByRole('button', { name: 'Expand all' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Show more nodes' }))
    expect(titleButtons()).toHaveLength(513)
    expect(screen.getByText('513 displayed nodes · 513 unique items')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Show more nodes' })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Collapse all' }))
    expect(titleButtons()).toHaveLength(1)
    expect(screen.getByText('1 displayed node · 1 unique item')).toBeInTheDocument()
    fireEvent.click(canvas.getByRole('button', { name: 'Expand branches for 宽树案例：512 条独立采纳路径' }))
    expect(titleButtons()).toHaveLength(5)
  })

  it('expands repeated lattice paths across multiple batches and resets when re-rooted', () => {
    const view = render(<KnowledgeTree key="lattice" index={index} rootId={knowledgePreviewRoots.lattice} direction="predecessors" onSelect={vi.fn()} />)
    const summary = within(screen.getByRole('group', { name: 'Flow summary' }))
    expect(screen.getByText('3 displayed nodes · 3 unique items')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Expand all' }))
    for (const count of [1000, 1500, 1535]) {
      fireEvent.click(summary.getByRole('button', { name: 'Show more nodes' }))
      expect(summary.getByText(new RegExp(`^${count} displayed nodes`))).toBeInTheDocument()
    }
    expect(summary.getByText('1535 displayed nodes · 20 unique items')).toBeInTheDocument()
    expect(summary.queryByRole('button', { name: 'Show more nodes' })).not.toBeInTheDocument()
    view.rerender(<KnowledgeTree key="wide" index={index} rootId={knowledgePreviewRoots.wide} direction="successors" onSelect={vi.fn()} />)
    expect(screen.getByText('5 displayed nodes · 5 unique items')).toBeInTheDocument()
  })

  it('expands repeated knowledge independently while title clicks keep opening details', () => {
    const onSelect = vi.fn()
    render(<KnowledgeTree index={indexKnowledge(knowledgeClaims, knowledgePolicies)} rootId="claim_00000001" direction="predecessors" onSelect={onSelect} />)
    expect(screen.getByText('4 displayed nodes · 4 unique items')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Claim: Verified recovery' }))
    expect(onSelect).toHaveBeenCalledWith(expect.objectContaining({ id: 'claim_00000002' }))
    expect(screen.queryByRole('button', { name: 'Claim: Shared evidence' })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Expand branches for Verified recovery' }))
    fireEvent.click(screen.getByRole('button', { name: 'Expand branches for Verified replay' }))
    expect(screen.getByText('6 displayed nodes · 5 unique items')).toBeInTheDocument()
    fireEvent.click(screen.getAllByRole('button', { name: 'Expand branches for Shared evidence' })[1])
    expect(screen.getAllByRole('button', { name: 'Policy Update: Reliable execution' })).toHaveLength(1)
    expect(screen.getByText('7 displayed nodes · 6 unique items')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Collapse branches for Verified recovery' }))
    expect(screen.getAllByRole('button', { name: 'Policy Update: Reliable execution' })).toHaveLength(1)
    expect(screen.getByText('6 displayed nodes · 6 unique items')).toBeInTheDocument()
    expect(onSelect).toHaveBeenCalledTimes(1)
  })

  it('does not offer expansion for isolated leaves, missing leaves, or cycle stops', () => {
    render(<KnowledgeTree index={indexKnowledge([makeClaim('a', ['a', 'missing'])], [])} rootId="a" direction="predecessors" onSelect={vi.fn()} />)
    expect(screen.getByText('3 displayed nodes · 2 unique items')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Expand all' })).toBeDisabled()
    expect(screen.queryByRole('button', { name: /^Expand branches/ })).not.toBeInTheDocument()
    expect(screen.getAllByRole('button', { name: /^Collapse branches/ })).toHaveLength(1)
  })

  it('fits below 20 percent and restores the overview when Fit is clicked again', () => {
    render(<KnowledgeTree index={index} rootId={knowledgePreviewRoots.release} direction="predecessors" onSelect={vi.fn()} />)
    const canvas = screen.getByRole('region', { name: 'Claims flow canvas' })
    Object.defineProperties(canvas, { clientWidth: { value: 720 }, clientHeight: { value: 480 } })
    fireEvent.click(screen.getByRole('button', { name: 'Expand all' }))
    fireEvent.click(screen.getByRole('button', { name: 'Fit' }))
    expect(screen.getByLabelText('Zoom level')).toHaveTextContent('4.8%')
    canvas.scrollLeft = 200
    canvas.scrollTop = 100
    fireEvent.click(screen.getByRole('button', { name: 'Fit' }))
    expect(canvas.scrollLeft).toBe(0)
    expect(canvas.scrollTop).toBe(0)
  })

  it.each(['predecessors', 'successors'] as const)('keeps the next layer branch controls inside the %s viewport', (direction) => {
    const claims = Array.from({ length: 5 }, (_, i) => makeClaim(`node_${i}`, i < 4 ? [`node_${i + 1}`] : []))
    const rootId = direction === 'predecessors' ? 'node_0' : 'node_4'
    const childId = direction === 'predecessors' ? 'node_1' : 'node_3'
    render(<KnowledgeTree index={indexKnowledge(claims, [])} rootId={rootId} direction={direction} onSelect={vi.fn()} />)
    const canvas = screen.getByRole('region', { name: 'Claims flow canvas' })
    Object.defineProperties(canvas, { clientWidth: { value: 720 }, clientHeight: { value: 360 } })
    canvas.scrollTop = 0
    fireEvent.click(screen.getByRole('button', { name: `Expand branches for ${childId}` }))
    const node = canvas.querySelector<HTMLDivElement>(`[data-tree-node="${rootId}/${childId}/node_2"]`)!
    const nodeTop = Number.parseFloat(node.style.top) - canvas.scrollTop
    // Sources 的按钮位于标题上方 30px；Impact 的按钮位于标题下方并带阴影。
    if (direction === 'predecessors') expect(nodeTop - 30).toBeGreaterThanOrEqual(0)
    else expect(nodeTop + NODE_HEIGHT + 36).toBeLessThanOrEqual(canvas.clientHeight)
  })

  it('keeps the current canvas center when zooming a distant branch', () => {
    render(<KnowledgeTree index={index} rootId={knowledgePreviewRoots.release} direction="predecessors" onSelect={vi.fn()} />)
    const canvas = screen.getByRole('region', { name: 'Claims flow canvas' })
    Object.defineProperties(canvas, { clientWidth: { value: 720 }, clientHeight: { value: 480 } })
    fireEvent.click(screen.getByRole('button', { name: 'Expand all' }))
    canvas.scrollLeft = 2000
    canvas.scrollTop = 200
    fireEvent.click(screen.getByRole('button', { name: 'Zoom in' }))
    expect(canvas.scrollLeft).toBeCloseTo((2000 + 360) * 1.2 - 360)
    expect(canvas.scrollTop).toBeCloseTo((200 + 240) * 1.2 - 240)
    fireEvent.click(screen.getByRole('button', { name: 'Zoom out' }))
    expect(canvas.scrollLeft).toBeCloseTo(2000)
    expect(canvas.scrollTop).toBeCloseTo(200)
    fireEvent.click(screen.getByRole('button', { name: 'Center root' }))
    expect(canvas.scrollLeft).not.toBeCloseTo(2000)
  })
})
