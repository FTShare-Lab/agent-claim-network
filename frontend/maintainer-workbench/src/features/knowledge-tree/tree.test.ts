// 验证来源图的方向、重复路径、循环终止和大树预算。
import { describe, expect, it } from 'vitest'

import { knowledgeClaims, knowledgePolicies, makeClaim } from '../../test/knowledgeFixtures'
import { buildKnowledgeTree, indexKnowledge, NODE_HEIGHT, NODE_WIDTH } from './tree'

const index = indexKnowledge(knowledgeClaims, knowledgePolicies)

describe('knowledge tree', () => {
  it('expands every predecessor path without merging shared sources', () => {
    const tree = buildKnowledgeTree(index, 'claim_00000001', 'predecessors')
    expect(tree.nodes).toHaveLength(8)
    expect(tree.nodes.filter((node) => node.knowledge.name === 'Shared evidence')).toHaveLength(2)
    expect(tree.nodes.filter((node) => node.knowledge.kind === 'policy_update')).toHaveLength(2)
    expect(tree.nodes.filter((node) => node.knowledge.kind === 'claim_attribute_update')).toHaveLength(1)
    expect(tree.nodes.every((node) => !node.cycle)).toBe(true)
    expect(tree.omitted).toBe(0)
    expect(tree.uniqueCount).toBe(6)
    for (const node of tree.nodes) {
      if (node.parent !== null) expect(node.y + NODE_HEIGHT).toBeLessThan(tree.nodes[node.parent].y)
    }
  })

  it('starts shallow and retains independent expansion state when sibling paths are removed', () => {
    const limits = new Map<string, number>()
    const expansion = { expandAll: false, childLimits: limits }
    const rootId = 'claim_00000001'
    let tree = buildKnowledgeTree(index, rootId, 'predecessors', 500, expansion)
    expect(tree.nodes).toHaveLength(4)
    expect(tree.uniqueCount).toBe(4)
    expect(tree.omitted).toBe(0)
    limits.set(`${rootId}/claim_00000002`, 4)
    limits.set(`${rootId}/claim_00000003`, 4)
    limits.set(`${rootId}/claim_00000003/claim_00000004`, 4)
    tree = buildKnowledgeTree(index, rootId, 'predecessors', 500, expansion)
    expect(tree.nodes).toHaveLength(7)
    expect(tree.uniqueCount).toBe(6)
    expect(tree.nodes.filter((node) => node.knowledge.id === 'claim_00000004').map((node) => node.expanded)).toEqual([false, true])
    limits.set(`${rootId}/claim_00000002`, 0)
    tree = buildKnowledgeTree(index, rootId, 'predecessors', 500, expansion)
    expect(tree.nodes).toHaveLength(6)
    expect(tree.nodes.find((node) => node.knowledge.id === 'policy_00000001')?.path).toBe(`${rootId}/claim_00000003/claim_00000004/policy_00000001`)
  })

  it('counts unique IDs rather than titles, including missing references and cycle stops', () => {
    const graph = indexKnowledge([makeClaim('a', ['a', 'b', 'missing'], 'Same title'), makeClaim('b', [], 'Same title')], [])
    const tree = buildKnowledgeTree(graph, 'a', 'predecessors')
    expect(tree.nodes).toHaveLength(4)
    expect(tree.uniqueCount).toBe(3)
    expect(tree.nodes.filter((node) => node.cycle).every((node) => node.childCount === 0)).toBe(true)
    expect(buildKnowledgeTree(graph, 'absent', 'predecessors').uniqueCount).toBe(0)
  })

  it('finds transitive policy impact and repeats a shared descendant along both paths', () => {
    const tree = buildKnowledgeTree(index, 'policy_00000001', 'successors')
    expect(tree.nodes.map((node) => node.knowledge.name)).toEqual([
      'Reliable execution', 'Shared evidence', 'Verified recovery', 'Verified replay', 'Release readiness', 'Release readiness',
    ])
    for (const node of tree.nodes) {
      if (node.parent !== null) expect(node.y).toBeGreaterThan(tree.nodes[node.parent].y + NODE_HEIGHT)
    }
    for (const depth of new Set(tree.nodes.map((node) => node.depth))) {
      const row = tree.nodes.filter((node) => node.depth === depth).sort((a, b) => a.x - b.x)
      for (let i = 1; i < row.length; i++) expect(row[i].x).toBeGreaterThan(row[i - 1].x + NODE_WIDTH)
    }
  })

  it.each(['predecessors', 'successors'] as const)('terminates cycles within each %s path, including self references', (direction) => {
    const cyclic = indexKnowledge([makeClaim('a', ['b', 'a']), makeClaim('b', ['a'])], [])
    const tree = buildKnowledgeTree(cyclic, 'a', direction)
    expect(tree.nodes).toHaveLength(4)
    expect(tree.nodes.filter((node) => node.cycle)).toHaveLength(2)
    expect(tree.nodes.filter((node) => node.cycle).every((node) => node.children.length === 0)).toBe(true)
    expect(tree.omitted).toBe(0)
  })

  it('keeps missing source references inspectable and deduplicates identical edges', () => {
    const missing = indexKnowledge([makeClaim('a', ['policy_missing', 'policy_missing'])], [])
    const tree = buildKnowledgeTree(missing, 'a', 'predecessors')
    expect(tree.nodes).toHaveLength(2)
    expect(tree.nodes[1].knowledge).toEqual({ kind: 'missing', id: 'policy_missing', name: 'Unavailable source' })
    expect(buildKnowledgeTree(missing, 'policy_missing', 'successors').nodes[1].knowledge.id).toBe('a')
  })

  it('shows isolated roots and handles a root absent from the snapshot', () => {
    expect(buildKnowledgeTree(index, 'policy_00000001', 'predecessors').nodes).toHaveLength(1)
    expect(buildKnowledgeTree(index, 'absent', 'predecessors').nodes).toEqual([])
  })

  it('bounds large expansions and resumes without dropping whole branches', () => {
    const tree = buildKnowledgeTree(index, 'claim_00000001', 'predecessors', 4)
    expect(tree.nodes.map((node) => node.knowledge.id)).toEqual(['claim_00000001', 'claim_00000002', 'claim_00000003', 'policy_00000002'])
    expect(tree.omitted).toBe(2)
    expect(buildKnowledgeTree(index, 'claim_00000001', 'predecessors', 8).omitted).toBe(0)
  })

  it('lays out long chains without recursive traversal or layout calls', () => {
    const chain = Array.from({ length: 1500 }, (_, i) => makeClaim(`claim_${i}`, i ? [`claim_${i - 1}`] : []))
    const tree = buildKnowledgeTree(indexKnowledge(chain, []), 'claim_1499', 'predecessors', 1500)
    expect(tree.nodes).toHaveLength(1500)
    expect(tree.omitted).toBe(0)
    expect(new Set(tree.nodes.map((node) => node.x)).size).toBe(1)
  })
})
