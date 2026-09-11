// 验证状态与争议并列展示，以及鼠标平移不会抢占节点点击。
import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { knowledgePolicies, makeClaim } from '../../test/knowledgeFixtures'
import { KnowledgeTree } from './KnowledgeTree'
import { indexKnowledge } from './tree'

afterEach(() => { cleanup(); vi.unstubAllGlobals() })

describe('claims flow status indicators', () => {
  it.each(['active', 'stale', 'deprecated'] as const)('shows only the %s status alongside an unresolved dispute', (status) => {
    const view = makeClaim('root')
    view.claim.status = status
    view.open_dispute_ids = ['dispute_open']
    render(<KnowledgeTree index={indexKnowledge([view], [])} rootId="root" direction="predecessors" onSelect={vi.fn()} />)
    const card = screen.getByRole('button', { name: 'Claim: root (root)' })
    const label = status[0].toUpperCase() + status.slice(1)
    expect(within(card).getAllByRole('img')).toHaveLength(2)
    for (const name of [label, 'Disputed']) {
      const icon = within(card).getByRole('img', { name })
      expect(icon).toHaveAttribute('title', name)
      expect(icon.querySelector('svg')).toHaveAttribute('width', '14')
      expect(icon.querySelector('svg')).toHaveAttribute('height', '14')
    }
    expect(card).toHaveClass('bg-emerald-50')
    expect(card).toHaveTextContent(/^root$/)
    const legend = within(screen.getByRole('group', { name: 'Status legend' }))
    for (const name of ['Active', 'Stale', 'Deprecated', 'Disputed']) expect(legend.getByText(name)).toBeInTheDocument()
  })

  it('shows policy statuses, excludes resolved disputes, and leaves missing source status unknown', () => {
    const root = makeClaim('root', [knowledgePolicies[0].id, knowledgePolicies[1].id, 'missing'])
    root.resolved_dispute_ids = ['dispute_resolved']
    render(<KnowledgeTree index={indexKnowledge([root], knowledgePolicies)} rootId="root" direction="predecessors" onSelect={vi.fn()} />)
    const rootCard = within(screen.getByRole('button', { name: 'Claim: root (root)' }))
    expect(rootCard.getAllByRole('img')).toHaveLength(1)
    expect(rootCard.queryByRole('img', { name: 'Disputed' })).not.toBeInTheDocument()
    const policy = screen.getByRole('button', { name: 'Policy Update: Reliable execution' })
    expect(within(policy).getByRole('img', { name: 'Active' })).toBeInTheDocument()
    const suggestion = screen.getByRole('button', { name: 'Claim Attribute Update: Review old evidence' })
    expect(within(suggestion).getByRole('img', { name: 'Deprecated' })).toBeInTheDocument()
    expect(within(screen.getByRole('button', { name: 'Unavailable source: Unavailable source' })).queryAllByRole('img')).toHaveLength(0)
    const types = within(screen.getByRole('group', { name: 'Knowledge type legend' }))
    expect(types.getByText('Policy Update').firstElementChild).toHaveClass('bg-orange-50', 'border-orange-300')
    expect(policy).toHaveClass('bg-orange-50', 'border-orange-300')
    expect(types.getByText('Claim Attribute Update').firstElementChild).toHaveClass('bg-violet-50', 'border-violet-300')
    expect(suggestion).toHaveClass('bg-violet-50', 'border-violet-300')
  })

  it('uses a dashed border only for claims held by another agent', () => {
    const root = makeClaim('root', ['own', 'other', ...knowledgePolicies.map((policy) => policy.id)], 'Root claim')
    const own = makeClaim('own', [], 'Own claim')
    const other = makeClaim('other', [], 'Another agent claim')
    root.claim.holder = 'selected-agent'
    own.claim.holder = 'selected-agent'
    other.claim.holder = 'another-agent'
    render(<KnowledgeTree index={indexKnowledge([root, own, other], knowledgePolicies)} rootId="root" direction="predecessors" onSelect={vi.fn()} />)

    expect(screen.getByRole('button', { name: 'Claim: Root claim (root)' })).not.toHaveClass('border-dashed')
    expect(screen.getByRole('button', { name: 'Claim: Own claim' })).not.toHaveClass('border-dashed')
    const otherCard = screen.getByRole('button', { name: 'Claim: Another agent claim' })
    expect(otherCard).toHaveClass('border-transparent')
    const otherBorder = within(otherCard).getByTestId('other-holder-claim-border').querySelector('rect')
    expect(otherBorder).toHaveAttribute('stroke-dasharray', '12 10')
    expect(otherBorder).toHaveAttribute('stroke-width', '1')
    expect(screen.getByRole('button', { name: 'Policy Update: Reliable execution' })).not.toHaveClass('border-dashed')
    expect(screen.getByRole('button', { name: 'Claim Attribute Update: Review old evidence' })).not.toHaveClass('border-dashed')

    const legend = within(screen.getByRole('group', { name: 'Knowledge type legend' }))
    expect(legend.getByText('Claim held by others')).toBeInTheDocument()
    const legendMarker = legend.getByTestId('other-holder-claim-marker').querySelector('rect')
    expect(legendMarker).toHaveAttribute('stroke-dasharray', '6 4')
    expect(legendMarker).toHaveAttribute('stroke-width', '1')
  })
})

describe('claims flow mouse panning', () => {
  beforeEach(() => {
    // jsdom 没有 PointerEvent 与指针捕获；只补浏览器事件边界，实际拖拽另做浏览器验收。
    class MousePointerEvent extends MouseEvent {
      pointerId: number
      pointerType: string
      constructor(type: string, init: PointerEventInit = {}) {
        super(type, init)
        this.pointerId = init.pointerId ?? 1
        this.pointerType = init.pointerType ?? 'mouse'
      }
    }
    vi.stubGlobal('PointerEvent', MousePointerEvent)
  })

  function setup() {
    const onSelect = vi.fn()
    render(<KnowledgeTree index={indexKnowledge([makeClaim('root', ['child']), makeClaim('child')], [])} rootId="root" direction="predecessors" onSelect={onSelect} />)
    const canvas = screen.getByRole('region', { name: 'Claims flow canvas' })
    const capture = vi.fn()
    const release = vi.fn()
    Object.assign(canvas, { setPointerCapture: capture, hasPointerCapture: () => true, releasePointerCapture: release })
    canvas.scrollLeft = 200
    canvas.scrollTop = 100
    return { canvas, capture, release, onSelect }
  }

  it('pans both axes at the current zoom and stops on release without opening a node', () => {
    const { canvas, capture, release, onSelect } = setup()
    fireEvent.click(screen.getByRole('button', { name: 'Zoom out' }))
    canvas.scrollLeft = 200
    canvas.scrollTop = 100
    fireEvent.pointerDown(canvas, { clientX: 200, clientY: 200, button: 0 })
    expect(capture).toHaveBeenCalledWith(1)
    expect(canvas).toHaveFocus()
    expect(canvas).toHaveClass('cursor-grabbing')
    fireEvent.pointerMove(canvas, { clientX: 80, clientY: 150 })
    expect(canvas.scrollLeft).toBe(320)
    expect(canvas.scrollTop).toBe(150)
    fireEvent.pointerUp(canvas)
    expect(release).toHaveBeenCalledWith(1)
    expect(canvas).toHaveClass('cursor-grab')
    fireEvent.pointerMove(canvas, { clientX: 0, clientY: 0 })
    expect(canvas.scrollLeft).toBe(320)
    expect(canvas.scrollTop).toBe(150)
    expect(onSelect).not.toHaveBeenCalled()
  })

  it('keeps panning room after Fit and zooms only blank canvas space on double click', () => {
    const { canvas, onSelect } = setup()
    Object.defineProperties(canvas, { clientWidth: { value: 720 }, clientHeight: { value: 480 } })
    fireEvent.click(screen.getByRole('button', { name: 'Fit' }))
    expect(canvas.scrollLeft).toBe(240)
    expect(canvas.scrollTop).toBe(240)
    fireEvent.pointerDown(canvas, { clientX: 200, clientY: 200, button: 0 })
    fireEvent.pointerMove(canvas, { clientX: 80, clientY: 150 })
    expect(canvas.scrollLeft).toBe(360)
    expect(canvas.scrollTop).toBe(290)
    fireEvent.pointerUp(canvas)

    fireEvent.doubleClick(canvas, { clientX: 200, clientY: 200 })
    expect(screen.getByLabelText('Zoom level')).toHaveTextContent('160%')
    fireEvent.doubleClick(screen.getByRole('button', { name: 'Claim: root (root)' }))
    expect(screen.getByLabelText('Zoom level')).toHaveTextContent('160%')
    expect(onSelect).not.toHaveBeenCalled()
  })

  it.each(['pointerCancel', 'lostPointerCapture'] as const)('stops panning on %s', (event) => {
    const { canvas } = setup()
    fireEvent.pointerDown(canvas, { clientX: 200, clientY: 200, button: 0 })
    fireEvent[event](canvas)
    fireEvent.pointerMove(canvas, { clientX: 50, clientY: 50 })
    expect(canvas.scrollLeft).toBe(200)
    expect(canvas.scrollTop).toBe(100)
    expect(canvas).toHaveClass('cursor-grab')
  })

  it('preserves node and branch clicks, right clicks, and native touch scrolling', () => {
    const { canvas, capture, onSelect } = setup()
    const card = screen.getByRole('button', { name: 'Claim: root (root)' })
    fireEvent.pointerDown(within(card).getByRole('img', { name: 'Active' }), { button: 0 })
    fireEvent.click(card)
    expect(onSelect).toHaveBeenCalledWith(expect.objectContaining({ id: 'root' }))
    const collapse = screen.getByRole('button', { name: 'Collapse branches for root' })
    fireEvent.pointerDown(collapse, { button: 0 })
    fireEvent.click(collapse)
    expect(screen.queryByRole('button', { name: 'Claim: child' })).not.toBeInTheDocument()
    fireEvent.pointerDown(canvas, { button: 2 })
    fireEvent.pointerDown(canvas, { pointerType: 'touch', button: 0 })
    expect(capture).not.toHaveBeenCalled()
  })
})
