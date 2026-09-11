// 可滚动和缩放的树形画布，箭头始终从知识来源指向派生知识。
import { useId, useLayoutEffect, useMemo, useRef, useState, type MouseEvent } from 'react'

import { knowledgeStyles } from './styles'
import { KnowledgeStatusIcons, KnowledgeStatusLegend } from './KnowledgeStatus'
import { useCanvasPan } from './useCanvasPan'
import { cn } from '../../lib/utils'
import { BRANCH_BATCH_SIZE, buildKnowledgeTree, NODE_HEIGHT, NODE_WIDTH, VERTICAL_PADDING, type Direction, type Knowledge, type KnowledgeIndex, type TreeExpansion, type TreeNode } from './tree'

const PAN_MARGIN = 240

type ViewportSize = { width: number; height: number }

function getCanvasLayout(tree: ReturnType<typeof buildKnowledgeTree>, zoom: number, viewportSize: ViewportSize) {
  const treeWidth = tree.width * zoom
  const treeHeight = tree.height * zoom
  const width = Math.max(treeWidth + PAN_MARGIN * 2, viewportSize.width + PAN_MARGIN * 2)
  const height = Math.max(treeHeight + PAN_MARGIN * 2, viewportSize.height + PAN_MARGIN * 2)
  return { width, height, treeLeft: (width - treeWidth) / 2, treeTop: (height - treeHeight) / 2 }
}

function scrollToRoot(element: HTMLDivElement | null, tree: ReturnType<typeof buildKnowledgeTree>, zoom: number) {
  const root = tree.nodes[0]
  if (!element || !root) return
  const layout = getCanvasLayout(tree, zoom, { width: element.clientWidth, height: element.clientHeight })
  element.scrollLeft = Math.min(layout.width - element.clientWidth, Math.max(0, layout.treeLeft + (root.x + NODE_WIDTH / 2) * zoom - element.clientWidth / 2))
  element.scrollTop = Math.min(layout.height - element.clientHeight, Math.max(0, layout.treeTop + (root.y + NODE_HEIGHT / 2) * zoom - element.clientHeight / 2))
}

function isHeldByAnotherAgent(knowledge: Knowledge, root: Knowledge | undefined) {
  return root?.kind === 'claim'
    && knowledge.kind === 'claim'
    && knowledge.view.claim.holder !== root.view.claim.holder
}

export function KnowledgeTree({ index, rootId, direction, selectedId, onSelect }: {
  index: KnowledgeIndex
  rootId: string
  direction: Direction
  selectedId?: string
  onSelect: (knowledge: Knowledge) => void
}) {
  const [budget, setBudget] = useState(500)
  const [expansion, setExpansion] = useState<TreeExpansion>(() => ({ expandAll: false, childLimits: new Map() }))
  const [zoom, setZoom] = useState(1)
  const [viewportSize, setViewportSize] = useState<ViewportSize>({ width: 0, height: 0 })
  const viewport = useRef<HTMLDivElement>(null)
  const { isPanning, ...panHandlers } = useCanvasPan()
  const branchAnchor = useRef<{ path: string; x: number; y: number; reveal: boolean } | null>(null)
  const zoomAnchor = useRef<{ x: number; y: number } | null>(null)
  const markerId = useId()
  const tree = useMemo(() => buildKnowledgeTree(index, rootId, direction, budget, expansion), [index, rootId, direction, budget, expansion])
  const rootKnowledge = index.entries.get(rootId)
  const fullyExpanded = tree.nodes.every((node) => node.children.length === node.childCount)
  const canvasLayout = useMemo(() => getCanvasLayout(tree, zoom, viewportSize), [tree, zoom, viewportSize])

  useLayoutEffect(() => {
    const element = viewport.current
    if (!element) return
    const updateViewportSize = () => {
      const next = { width: element.clientWidth, height: element.clientHeight }
      setViewportSize((current) => current.width === next.width && current.height === next.height ? current : next)
    }
    updateViewportSize()
    if (typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(updateViewportSize)
    observer.observe(element)
    return () => observer.disconnect()
  }, [])

  useLayoutEffect(() => {
    const element = viewport.current
    const anchor = branchAnchor.current
    branchAnchor.current = null
    const center = zoomAnchor.current
    zoomAnchor.current = null
    if (element && center) {
      const layout = getCanvasLayout(tree, zoom, { width: element.clientWidth, height: element.clientHeight })
      element.scrollLeft = layout.treeLeft + center.x * zoom - element.clientWidth / 2
      element.scrollTop = layout.treeTop + center.y * zoom - element.clientHeight / 2
      return
    }
    const node = anchor && tree.nodes.find((entry) => entry.path === anchor.path)
    if (element && anchor && node) {
      // 分支增删会改变树的宽高；保留点击节点的位置，便于连续向远端展开。
      const layout = getCanvasLayout(tree, zoom, { width: element.clientWidth, height: element.clientHeight })
      let anchorY = anchor.y
      if (anchor.reveal && node.children.length) {
        const delta = (tree.nodes[node.children[0]].y - node.y) * zoom
        const margin = (NODE_HEIGHT / 2 + VERTICAL_PADDING) * zoom
        // 靠近画布边缘时，让新一层节点及其展开按钮一起进入可视区域。
        anchorY = delta < 0 ? Math.max(anchorY, margin - delta) : Math.min(anchorY, element.clientHeight - margin - delta)
      }
      element.scrollLeft = layout.treeLeft + (node.x + NODE_WIDTH / 2) * zoom - anchor.x
      element.scrollTop = layout.treeTop + (node.y + NODE_HEIGHT / 2) * zoom - anchorY
      return
    }
    scrollToRoot(viewport.current, tree, zoom)
  }, [tree, zoom, viewportSize])

  function changeZoom(nextZoom: number) {
    if (nextZoom === zoom) return
    const element = viewport.current
    if (element) {
      // 缩放围绕当前视口中心，远端分支不会被重新定位到根节点。
      const layout = getCanvasLayout(tree, zoom, { width: element.clientWidth, height: element.clientHeight })
      zoomAnchor.current = {
        x: (element.scrollLeft + element.clientWidth / 2 - layout.treeLeft) / zoom,
        y: (element.scrollTop + element.clientHeight / 2 - layout.treeTop) / zoom,
      }
    }
    setZoom(nextZoom)
  }

  function changeBranch(node: TreeNode, limit: number) {
    const element = viewport.current
    if (element) {
      const layout = getCanvasLayout(tree, zoom, { width: element.clientWidth, height: element.clientHeight })
      branchAnchor.current = {
        path: node.path,
        x: layout.treeLeft + (node.x + NODE_WIDTH / 2) * zoom - element.scrollLeft,
        y: layout.treeTop + (node.y + NODE_HEIGHT / 2) * zoom - element.scrollTop,
        reveal: limit > node.children.length,
      }
    }
    setExpansion((current) => ({ ...current, childLimits: new Map(current.childLimits).set(node.path, limit) }))
    if (limit > 0 && tree.nodes.length + limit - node.children.length > budget) setBudget((current) => current + 500)
  }

  function zoomAtPoint(event: MouseEvent<HTMLDivElement>) {
    if (event.target instanceof Element && event.target.closest('button, a, input, select, textarea')) return
    const element = event.currentTarget
    if (zoom >= 1.6) return
    const bounds = element.getBoundingClientRect()
    const layout = getCanvasLayout(tree, zoom, { width: element.clientWidth, height: element.clientHeight })
    zoomAnchor.current = {
      x: (element.scrollLeft + event.clientX - bounds.left - layout.treeLeft) / zoom,
      y: (element.scrollTop + event.clientY - bounds.top - layout.treeTop) / zoom,
    }
    setZoom(Math.min(1.6, Math.max(zoom + 0.2, zoom * 2)))
  }

  return (
    <section aria-label="Claims flow" className="overflow-hidden rounded-xl border border-slate-200 bg-white">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 px-4 py-3">
        <div className="space-y-2">
          <div role="group" aria-label="Knowledge type legend" className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-slate-600">
            {Object.entries(knowledgeStyles).map(([kind, style]) => (
              <span key={kind} className="inline-flex items-center gap-1.5"><span className={cn('h-3 w-3 shrink-0 rounded border', style.className)} />{style.label}</span>
            ))}
            <span className="inline-flex items-center gap-1.5">
              <span data-testid="other-holder-claim-marker" className="h-3.5 w-5 shrink-0" aria-hidden="true">
                <svg viewBox="0 0 20 14" className="h-full w-full"><rect x="0.5" y="0.5" width="19" height="13" rx="3.5" fill="none" stroke="currentColor" strokeWidth="1" strokeDasharray="6 4" className="text-emerald-400" /></svg>
              </span>
              Claim held by others
            </span>
          </div>
          <KnowledgeStatusLegend />
        </div>
        <div className="flex items-center gap-1 text-xs text-slate-600">
          <button type="button" aria-label="Zoom out" disabled={zoom <= 0.2} className="rounded px-2 py-1.5 hover:bg-slate-100 disabled:opacity-40" onClick={() => changeZoom(Math.max(0.2, zoom - 0.2))}>−</button>
          <span aria-label="Zoom level" className="w-10 text-center font-mono">{Number((zoom * 100).toFixed(1))}%</span>
          <button type="button" aria-label="Zoom in" disabled={zoom >= 1.6} className="rounded px-2 py-1.5 hover:bg-slate-100 disabled:opacity-40" onClick={() => changeZoom(Math.min(1.6, zoom + 0.2))}>+</button>
          <button type="button" className="rounded px-2 py-1.5 hover:bg-slate-100" onClick={() => scrollToRoot(viewport.current, tree, zoom)}>Center root</button>
          <button type="button" className="rounded px-2 py-1.5 hover:bg-slate-100" onClick={() => {
            const element = viewport.current
            if (!element || !tree.width || !tree.height) return
            // 整体预览允许低于手动缩放下限，并保留四周平移空间。
            const nextZoom = Math.min(1, element.clientWidth / tree.width, element.clientHeight / tree.height)
            const center = { x: tree.width / 2, y: tree.height / 2 }
            if (nextZoom === zoom) {
              const layout = getCanvasLayout(tree, zoom, { width: element.clientWidth, height: element.clientHeight })
              element.scrollLeft = layout.treeLeft + center.x * zoom - element.clientWidth / 2
              element.scrollTop = layout.treeTop + center.y * zoom - element.clientHeight / 2
              return
            }
            zoomAnchor.current = center
            setZoom(nextZoom)
          }}>Fit</button>
          <span aria-hidden="true" className="mx-1 h-4 w-px bg-slate-200" />
          <button type="button" disabled={fullyExpanded || (expansion.expandAll && expansion.childLimits.size === 0)} className="rounded px-2 py-1.5 font-medium text-blue-700 hover:bg-slate-100 disabled:text-slate-400" onClick={() => setExpansion({ expandAll: true, childLimits: new Map() })}>Expand all</button>
          <button type="button" disabled={tree.nodes.length <= 1} className="rounded px-2 py-1.5 font-medium text-blue-700 hover:bg-slate-100 disabled:text-slate-400" onClick={() => { setExpansion({ expandAll: false, childLimits: new Map([[encodeURIComponent(rootId), 0]]) }); setBudget(500) }}>Collapse all</button>
        </div>
      </div>
      <div ref={viewport} tabIndex={0} role="region" aria-label="Claims flow canvas" {...panHandlers} onDoubleClick={zoomAtPoint} className={cn('h-[560px] max-h-[65vh] min-h-72 overflow-auto overscroll-contain bg-[radial-gradient(#d9dce5_1px,transparent_1px)] bg-[size:20px_20px]', isPanning ? 'cursor-grabbing select-none' : 'cursor-grab')}>
        <div className="relative" style={{ width: canvasLayout.width, height: canvasLayout.height }}>
          <div className="absolute overflow-hidden" style={{ left: canvasLayout.treeLeft, top: canvasLayout.treeTop, width: tree.width * zoom, height: tree.height * zoom }}>
            <div className="relative origin-top-left" style={{ width: tree.width, height: tree.height, transform: `scale(${zoom})` }}>
              <svg aria-hidden="true" width={tree.width} height={tree.height} className="pointer-events-none absolute inset-0">
                <defs><marker id={markerId} viewBox="0 0 10 10" refX="10" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b" /></marker></defs>
                {tree.nodes.filter((node) => node.parent !== null).map((node) => {
                  const parent = tree.nodes[node.parent!]
                  const source = direction === 'predecessors' ? node : parent
                  const target = direction === 'predecessors' ? parent : node
                  const x1 = source.x + NODE_WIDTH / 2
                  const y1 = source.y + NODE_HEIGHT + 4
                  const x2 = target.x + NODE_WIDTH / 2
                  // 末端保留直线与根节点外圈间距，箭头在两种模式下都指向派生知识。
                  const y2 = target.y - 6
                  const approachY = y2 - 12
                  const midY = (y1 + approachY) / 2
                  return <path key={node.path} data-source={source.knowledge.id} data-target={target.knowledge.id} d={`M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${approachY} L ${x2} ${y2}`} fill="none" stroke="#94a3b8" strokeWidth="1.5" markerEnd={`url(#${markerId})`} />
                })}
              </svg>
              {tree.nodes.map((node) => {
                const style = knowledgeStyles[node.knowledge.kind]
                const heldByAnotherAgent = isHeldByAnotherAgent(node.knowledge, rootKnowledge)
                return (
                  <div key={node.path} data-tree-node={node.path} className="absolute" style={{ left: node.x, top: node.y, width: NODE_WIDTH }}>
                    <button
                      type="button"
                      aria-label={`${style.label}: ${node.knowledge.name}${node.key === 0 ? ' (root)' : ''}${node.cycle ? ' (cycle ends here)' : ''}`}
                      aria-pressed={selectedId === node.knowledge.id}
                      title={`${node.knowledge.name} · ${style.label} · ${node.knowledge.id}${node.cycle ? ' · Cycle ends here' : ''}`}
                      onClick={() => onSelect(node.knowledge)}
                      className={cn('relative flex w-full cursor-pointer items-center justify-center rounded-lg border px-3 text-center text-sm font-semibold shadow-sm transition-shadow hover:shadow-md focus-visible:outline-offset-4', node.knowledge.kind !== 'missing' && 'pt-4 pb-0.5', style.className, heldByAnotherAgent && 'border-transparent', node.key === 0 && 'ring-2 ring-slate-400 ring-offset-2', selectedId === node.knowledge.id && 'outline-2 outline-offset-2 outline-[var(--accent)]')}
                      style={{ height: NODE_HEIGHT }}
                    >
                      {heldByAnotherAgent ? <svg data-testid="other-holder-claim-border" aria-hidden="true" viewBox={`0 0 ${NODE_WIDTH} ${NODE_HEIGHT}`} preserveAspectRatio="none" className="pointer-events-none absolute inset-0 h-full w-full text-emerald-400"><rect x="0.5" y="0.5" width={NODE_WIDTH - 1} height={NODE_HEIGHT - 1} rx="7.5" fill="none" stroke="currentColor" strokeWidth="1" strokeDasharray="12 10" /></svg> : null}
                      <KnowledgeStatusIcons knowledge={node.knowledge} />
                      <span className="line-clamp-2 [overflow-wrap:anywhere]">{node.knowledge.name}</span>
                    </button>
                    {node.childCount > 0 ? (
                      <div className="absolute left-1/2 ml-4 flex cursor-pointer items-center gap-1" style={{ top: direction === 'predecessors' ? -30 : NODE_HEIGHT + 6 }}>
                        <button type="button" aria-expanded={node.expanded}
                          aria-label={`${node.expanded ? 'Collapse' : 'Expand'} branches for ${node.knowledge.name}`}
                          title={node.expanded ? 'Collapse this branch' : `Show ${Math.min(BRANCH_BATCH_SIZE, node.childCount)} of ${node.childCount} branches`}
                          className="min-w-7 whitespace-nowrap rounded-full border border-slate-300 bg-white px-2 py-1 text-xs font-semibold text-slate-700 shadow-sm hover:border-blue-400 hover:text-blue-700"
                          onClick={() => changeBranch(node, node.expanded ? 0 : BRANCH_BATCH_SIZE)}
                        >{node.expanded ? '−' : `+ ${node.childCount}`}</button>
                        {node.expanded && node.children.length < node.childCount ? (
                          <button type="button" aria-label={`Show more branches for ${node.knowledge.name}`}
                            title={`Show next ${Math.min(BRANCH_BATCH_SIZE, node.childCount - node.children.length)} of ${node.childCount - node.children.length} hidden branches`}
                            className="whitespace-nowrap rounded-full border border-blue-200 bg-white px-2 py-1 text-xs font-semibold text-blue-700 shadow-sm hover:bg-blue-50"
                            onClick={() => changeBranch(node, node.children.length + BRANCH_BATCH_SIZE)}
                          >+ {node.childCount - node.children.length}</button>
                        ) : null}
                      </div>
                    ) : null}
                    {node.cycle ? <span className="mt-1 block text-center text-[11px] text-slate-600">↻ Cycle ends here</span> : null}
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      </div>
      <div role="group" aria-label="Flow summary" className="flex flex-wrap items-center justify-between gap-2 border-t border-slate-200 px-4 py-3 text-xs text-slate-500">
        <div>
          <p className="font-medium text-slate-700" aria-live="polite">{tree.nodes.length} displayed {tree.nodes.length === 1 ? 'node' : 'nodes'} · {tree.uniqueCount} unique {tree.uniqueCount === 1 ? 'item' : 'items'}</p>
          <p className="mt-1">Counts cover visible nodes. Arrows point from source to derived knowledge.</p>
        </div>
        {tree.omitted > 0 ? <button type="button" className="font-semibold text-blue-700" onClick={() => setBudget(budget + 500)}>Show more nodes</button> : null}
      </div>
    </section>
  )
}
