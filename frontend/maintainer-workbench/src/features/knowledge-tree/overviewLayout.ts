// 只计算展示坐标与走线；比较多个分层方案，业务来源关系始终保留。
import type { ELK, ElkNode, ElkPoint } from 'elkjs/lib/elk-api'
import { OVERVIEW_NODE_HEIGHT as H, OVERVIEW_NODE_WIDTH as W } from './overview'

export interface OverviewTopology {
  ids: string[]
  edges: { source: string; target: string }[]
}
export interface OverviewLayout {
  nodes: { id: string; x: number; y: number }[]
  edges: { source: string; target: string; points: ElkPoint[] }[]
  width: number
  height: number
}

function extractLayout(result: ElkNode, topology: OverviewTopology): OverviewLayout {
  const nodes = (result.children ?? []).map((node) => ({ id: node.id, x: node.x ?? 0, y: node.y ?? 0 }))
  const routes = new Map((result.edges ?? []).map((edge) => [edge.id, edge.sections]))
  const edges = topology.edges.map((edge, i) => {
    const section = routes.get(`edge-${i}`)?.[0]
    if (!section) throw new Error('Missing overview edge route')
    return { ...edge, points: [section.startPoint, ...(section.bendPoints ?? []), section.endPoint] }
  })
  return { nodes, edges, width: Math.max(1, result.width ?? 0), height: Math.max(1, result.height ?? 0) }
}

// 正交线段的严格交叉与共线重叠；同一来源/终点的共享线段不算视觉冲突。
export function layoutQuality(layout: OverviewLayout, aspectRatio: number) {
  let crossings = 0, overlaps = 0, length = 0, bends = 0
  const segments = layout.edges.flatMap((edge, index) => {
    bends += Math.max(0, edge.points.length - 2)
    return edge.points.slice(1).map((b, i) => {
      const a = edge.points[i]
      length += Math.abs(b.x - a.x) + Math.abs(b.y - a.y)
      return { a, b, index, vertical: Math.abs(a.x - b.x) < 0.01 }
    })
  })
  for (let i = 0; i < segments.length; i++) for (let j = i + 1; j < segments.length; j++) {
    const a = segments[i], b = segments[j]
    if (a.index === b.index) continue
    const ea = layout.edges[a.index], eb = layout.edges[b.index]
    if (a.vertical !== b.vertical) {
      const v = a.vertical ? a : b, h = a.vertical ? b : a
      if (v.a.x > Math.min(h.a.x, h.b.x) + 0.01 && v.a.x < Math.max(h.a.x, h.b.x) - 0.01
        && h.a.y > Math.min(v.a.y, v.b.y) + 0.01 && h.a.y < Math.max(v.a.y, v.b.y) - 0.01) crossings++
    } else if (ea.source !== eb.source && ea.target !== eb.target) {
      const axis = a.vertical ? 'y' : 'x', fixed = a.vertical ? 'x' : 'y'
      if (Math.abs(a.a[fixed] - b.a[fixed]) < 0.01) {
        overlaps += Math.max(0, Math.min(Math.max(a.a[axis], a.b[axis]), Math.max(b.a[axis], b.b[axis]))
          - Math.max(Math.min(a.a[axis], a.b[axis]), Math.min(b.a[axis], b.b[axis])))
      }
    }
  }
  // 优先降低交叉；同等关系复杂度时，兼顾当前视口内的字号、线长和转折。
  const extent = Math.max(layout.width / aspectRatio, layout.height)
  return { crossings, overlaps, score: crossings * 1000 + overlaps * 10 + extent + length * 0.015 + bends * 2 }
}

export async function layoutOverview(topology: OverviewTopology, aspectRatio: number, engine: Pick<ELK, 'layout'>, signal?: AbortSignal): Promise<OverviewLayout> {
  if (!topology.ids.length) return { nodes: [], edges: [], width: 500, height: 300 }
  let best: OverviewLayout | undefined
  let bestScore = Infinity
  // 固定种子保证刷新稳定；方向与节点放置策略按实际连接情况竞争。
  for (const direction of ['DOWN', 'RIGHT']) for (const placement of ['BRANDES_KOEPF', 'NETWORK_SIMPLEX']) for (const seed of [1, 7]) for (const wrapping of ['OFF', 'SINGLE_EDGE']) {
    signal?.throwIfAborted()
    const result = await engine.layout({
      id: 'overview',
      layoutOptions: {
        'elk.algorithm': 'layered',
        'elk.direction': direction,
        'elk.edgeRouting': 'ORTHOGONAL',
        'elk.randomSeed': String(seed),
        'elk.aspectRatio': String(aspectRatio),
        'elk.padding': '[top=36,left=36,bottom=36,right=36]',
        'elk.spacing.nodeNode': '44',
        'elk.spacing.componentComponent': '90',
        'elk.spacing.edgeNode': '20',
        'elk.spacing.edgeEdge': '14',
        'elk.layered.spacing.nodeNodeBetweenLayers': '70',
        'elk.layered.spacing.edgeNodeBetweenLayers': '20',
        'elk.layered.spacing.edgeEdgeBetweenLayers': '14',
        'elk.layered.thoroughness': '20',
        'elk.layered.nodePlacement.strategy': placement,
        'elk.layered.mergeEdges': 'false',
        'elk.layered.wrapping.strategy': wrapping,
      },
      children: topology.ids.map((id) => ({ id, width: W, height: H })),
      edges: topology.edges.map((edge, i) => ({ id: `edge-${i}`, sources: [edge.source], targets: [edge.target] })),
    })
    signal?.throwIfAborted()
    const candidate = extractLayout(result, topology)
    const { score } = layoutQuality(candidate, aspectRatio)
    if (score < bestScore) { best = candidate; bestScore = score }
  }
  if (!best) throw new Error('Overview layout unavailable')
  return best
}

export function edgePath(points: ElkPoint[]) {
  return points.map((point, i) => `${i ? 'L' : 'M'} ${point.x} ${point.y}`).join(' ')
}
