// 真实布局引擎回归：保留关系、矩形不重叠、走线不穿过其他节点。
import ELK from 'elkjs/lib/elk.bundled.js'
import { describe, expect, it } from 'vitest'
import { knowledgePolicies, makeClaim } from '../../test/knowledgeFixtures'
import { createKnowledgePreview } from '../../test/knowledgePreview'
import { buildClaimOverview, OVERVIEW_NODE_HEIGHT as H, OVERVIEW_NODE_WIDTH as W } from './overview'
import { layoutOverview, layoutQuality, type OverviewLayout } from './overviewLayout'

function assertClear(layout: OverviewLayout) {
  for (const node of layout.nodes) {
    expect(node.x).toBeGreaterThanOrEqual(0)
    expect(node.y).toBeGreaterThanOrEqual(0)
    expect(node.x + W).toBeLessThanOrEqual(layout.width)
    expect(node.y + H).toBeLessThanOrEqual(layout.height)
    for (const other of layout.nodes) {
      if (node.id === other.id) continue
      expect(node.x + W <= other.x || other.x + W <= node.x || node.y + H <= other.y || other.y + H <= node.y).toBe(true)
    }
    for (const edge of layout.edges) {
      if (edge.source === node.id || edge.target === node.id) continue
      for (let i = 1; i < edge.points.length; i++) {
        const a = edge.points[i - 1], b = edge.points[i]
        const through = a.x === b.x
          ? a.x > node.x && a.x < node.x + W && Math.max(a.y, b.y) > node.y && Math.min(a.y, b.y) < node.y + H
          : a.y > node.y && a.y < node.y + H && Math.max(a.x, b.x) > node.x && Math.min(a.x, b.x) < node.x + W
        expect(through, `${edge.source} → ${edge.target} through ${node.id}`).toBe(false)
      }
    }
  }
  for (const edge of layout.edges) {
    expect(edge.points.length).toBeGreaterThanOrEqual(2)
    for (let i = 1; i < edge.points.length; i++) {
      const a = edge.points[i - 1], b = edge.points[i]
      expect(Math.abs(a.x - b.x) < 0.01 || Math.abs(a.y - b.y) < 0.01).toBe(true)
    }
  }
}

const fixtures = createKnowledgePreview(Date.UTC(2026, 0, 1))
describe('adaptive overview layout', () => {
  it.each([
    ['release graph', fixtures.claims.filter((v) => v.claim.scope === 'demo/knowledge/release')],
    ['30 selected from 630', fixtures.claims],
    ['cycles and disconnected groups', [makeClaim('a', ['b']), makeClaim('b', ['a', 'b']), makeClaim('c', ['b']), makeClaim('d'), makeClaim('e', ['d']), makeClaim('alone')]],
    ['wide fan', [makeClaim('center'), ...Array.from({ length: 29 }, (_, i) => makeClaim(`leaf${i}`, ['center']))]],
  ])('routes %s without collisions or dropped relationships', async (_, claims) => {
    const before = JSON.stringify(claims)
    const graph = buildClaimOverview(claims)
    const topology = { ids: graph.nodes.map((v) => v.id).sort(), edges: graph.edges }
    const engine = new ELK()
    const result = await layoutOverview(topology, 2, engine)
    expect(result.nodes.map((v) => v.id).sort()).toEqual(topology.ids)
    expect(result.edges.map(({ source, target }) => ({ source, target }))).toEqual(graph.edges)
    assertClear(result)
    if (claims.length > 30 && graph.edges.length === 29) {
      // 深链可折行，避免总览 Fit 后缩成一条难以阅读的细线。
      expect(Math.max(result.width / 2, result.height)).toBeLessThan(1500)
    }
    if (claims.length === 20) expect(layoutQuality(result, 2).crossings).toBeLessThanOrEqual(10)
    expect(JSON.stringify(claims)).toBe(before)
  }, 20000)


  it('routes 30 Claims plus 20 PU/CAU without collisions or lost links', async () => {
    const policies = Array.from({ length: 20 }, (_, i) => ({ ...knowledgePolicies[i % 2], id: `policy_${i}` }))
    const claims = Array.from({ length: 30 }, (_, i) => makeClaim(`claim_${i}`, [
      ...(i ? [`claim_${i - 1}`] : []), policies[i % 20].id,
    ]))
    const graph = buildClaimOverview(claims, policies)
    expect(graph.nodes).toHaveLength(50)
    const topology = { ids: graph.nodes.map((n) => n.id).sort(), edges: graph.edges }
    const result = await layoutOverview(topology, 2, new ELK())
    expect(result.nodes).toHaveLength(50)
    expect(result.edges.map(({ source, target }) => ({ source, target }))).toEqual(graph.edges)
    assertClear(result)
  }, 20000)

  it('handles empty graphs and cancellation', async () => {
    const engine = new ELK()
    expect((await layoutOverview({ ids: [], edges: [] }, 2, engine)).nodes).toEqual([])
    const controller = new AbortController()
    controller.abort()
    await expect(layoutOverview({ ids: ['a'], edges: [] }, 2, engine, controller.signal)).rejects.toBeDefined()
  })
})
