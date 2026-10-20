import { describe, expect, it } from 'vitest'
import { knowledgePolicies, makeClaim } from '../../test/knowledgeFixtures'
import { buildClaimOverview } from './overview'

const chain = (prefix: string, n: number) => Array.from({ length: n }, (_, i) => makeClaim(`${prefix}${String(i).padStart(3, '0')}`, i ? [`${prefix}${String(i - 1).padStart(3, '0')}`] : []))
describe('global Claim overview', () => {
  it('preserves unique nodes, all directed links, cycles and self references without inventing sources', () => {
    const claims = [makeClaim('a', ['b', 'b', 'missing', 'policy_1']), makeClaim('b', ['a', 'b']), makeClaim('c', ['b']), makeClaim('isolated')]
    const before = JSON.stringify(claims)
    const graph = buildClaimOverview(claims)
    expect(graph.nodes.map((node) => node.id).sort()).toEqual(['a', 'b', 'c', 'isolated'])
    expect(graph.edges).toEqual([{ source: 'b', target: 'a' }, { source: 'a', target: 'b' }, { source: 'b', target: 'b' }, { source: 'b', target: 'c' }])
    expect(graph.components).toBe(2)
    expect(graph.displayedComponents).toBe(2)
    expect(JSON.stringify(claims)).toBe(before)
  })
  it('selects a connected subset of 30 from the largest component, independent of input order', () => {
    const data = [...chain('small', 20), ...chain('large', 40), makeClaim('isolated')]
    const graph = buildClaimOverview(data)
    expect(graph.nodes).toHaveLength(30)
    expect(graph.total).toBe(61)
    expect(graph.displayedComponents).toBe(1)
    expect(graph.edges).toHaveLength(29)
    expect(graph.nodes.every((node) => node.id.startsWith('large'))).toBe(true)
    expect(buildClaimOverview([...data].reverse()).nodes.map((node) => node.id)).toEqual(graph.nodes.map((node) => node.id))
  })
  it('minimizes selected components and prefers well-connected neighbors', () => {
    const star = [makeClaim('center'), ...Array.from({ length: 35 }, (_, i) => makeClaim(`leaf${i}`, ['center']))]
    const graph = buildClaimOverview(star)
    expect(graph.nodes[0].id).toBe('center')
    expect(graph.edges).toHaveLength(29)
    const disconnected = buildClaimOverview([...chain('a', 18), ...chain('b', 10), ...chain('c', 8), ...chain('d', 2)])
    expect(disconnected.nodes).toHaveLength(30)
    expect(disconnected.displayedComponents).toBe(3)
  })
  it('handles empty and isolated Claims deterministically', () => {
    expect(buildClaimOverview([]).nodes).toEqual([])
    const graph = buildClaimOverview(Array.from({ length: 40 }, (_, i) => makeClaim(`id${i}`)))
    expect(graph.nodes).toHaveLength(30)
    expect(graph.edges).toEqual([])
    expect(buildClaimOverview(Array.from({ length: 40 }, (_, i) => makeClaim(`id${i}`))).nodes).toEqual(graph.nodes)
  })
})

describe('related PU/CAU overview selection', () => {
  it('includes only explicit sources of selected Claims, deduplicates sources and retains every selected edge', () => {
    const policies = knowledgePolicies
    const claims = [makeClaim('a', [policies[0].id, policies[0].id, policies[1].id, 'missing']), makeClaim('b', ['a', policies[0].id])]
    const before = JSON.stringify({ claims, policies })
    const graph = buildClaimOverview(claims, policies)
    expect(graph.claimCount).toBe(2)
    expect(graph.policyCount).toBe(2)
    expect(graph.policyTotal).toBe(2)
    expect(graph.nodes.map((v) => v.knowledge.kind)).toEqual(['claim', 'claim', 'policy_update', 'claim_attribute_update'])
    expect(graph.edges).toEqual([
      { source: 'a', target: 'b' },
      { source: policies[0].id, target: 'a' }, { source: policies[0].id, target: 'b' },
      { source: policies[1].id, target: 'a' },
    ])
    expect(JSON.stringify({ claims, policies })).toBe(before)
    expect(buildClaimOverview([], policies).nodes).toEqual([])
  })
  it('caps the combined graph at 50 and ranks by selected Claim count, then recency and ID', () => {
    const policies = Array.from({ length: 25 }, (_, i) => ({
      ...knowledgePolicies[i % 2], id: `policy_${String(i).padStart(2, '0')}`,
      created_at: `2026-09-${String(i + 1).padStart(2, '0')}T00:00:00Z`,
    }))
    // The oldest is most connected. updated_at must override created_at for the next priority.
    policies[1].updated_at = '2026-10-01T00:00:00Z'
    policies[2].updated_at = policies[1].updated_at
    const claims = chain('a', 35)
    for (const claim of claims) claim.claim.source_claim_ids.push(policies[0].id)
    claims[1].claim.source_claim_ids.push(...policies.map((p) => p.id))
    const unrelated = { ...knowledgePolicies[0], id: 'unrelated' }
    const graph = buildClaimOverview(claims, [...policies, unrelated])
    expect(graph.nodes).toHaveLength(50)
    expect(graph.claimCount).toBe(30)
    expect(graph.policyCount).toBe(20)
    expect(graph.policyTotal).toBe(25)
    const policyIds = graph.nodes.slice(30).map((n) => n.id)
    expect(policyIds.slice(0, 4)).toEqual(['policy_00', 'policy_01', 'policy_02', 'policy_24'])
    expect(policyIds).not.toContain('policy_03')
    expect(graph.edges.filter((e) => e.source === 'policy_00')).toHaveLength(30)
    expect(buildClaimOverview([...claims].reverse(), [...policies].reverse()).nodes.map((n) => n.id)).toEqual(graph.nodes.map((n) => n.id))
    const selected = new Set(graph.nodes.map((n) => n.id))
    expect(graph.edges.every((e) => selected.has(e.source) && selected.has(e.target))).toBe(true)
    const outside = makeClaim('z', ['unrelated'])
    expect(buildClaimOverview([...claims, outside], [...policies, unrelated]).policyTotal).toBe(25)
  })
})
