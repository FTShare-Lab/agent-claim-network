// 检查预览案例的规模、来源完整性及各类复杂拓扑的展开结果。
import { describe, expect, it } from 'vitest'

import { buildKnowledgeTree, indexKnowledge } from '../features/knowledge-tree/tree'
import { createKnowledgePreview, knowledgePreviewRoots, missingPreviewSources } from './knowledgePreview'

const sample = createKnowledgePreview(Date.parse('2026-09-09T08:00:00Z'))
const index = indexKnowledge(sample.claims, sample.policies)

describe('complex knowledge preview data', () => {
  it('adds unique records with valid IDs and only the two intentional missing references', () => {
    expect(sample.claims).toHaveLength(627)
    expect(sample.policies).toHaveLength(5)
    const ids = [...sample.claims.map((view) => view.claim.id), ...sample.policies.map((policy) => policy.id)]
    expect(new Set(ids).size).toBe(ids.length)
    expect(ids.every((id) => /^(claim|policy)_[0-9a-f]{8}$/.test(id))).toBe(true)
    const missing = new Set(sample.claims.flatMap((view) => view.claim.source_claim_ids).filter((id) => !ids.includes(id)))
    expect(missing).toEqual(new Set(missingPreviewSources))
  })

  it('expands the release decision across governance types and shared evidence', () => {
    const tree = buildKnowledgeTree(index, knowledgePreviewRoots.release, 'predecessors')
    expect(tree.nodes).toHaveLength(145)
    expect(new Set(tree.nodes.map((node) => node.knowledge.id)).size).toBe(23)
    expect(Math.max(...tree.nodes.map((node) => node.depth))).toBe(9)
    expect(new Set(tree.nodes.map((node) => node.knowledge.kind))).toEqual(new Set(['claim', 'policy_update', 'claim_attribute_update']))
    expect(tree.nodes.some((node) => node.cycle)).toBe(false)
    expect(tree.omitted).toBe(0)
  })

  it('terminates both cycle types and keeps missing sources and same-title claims separate', () => {
    const tree = buildKnowledgeTree(index, knowledgePreviewRoots.boundaries, 'predecessors')
    expect(tree.nodes).toHaveLength(20)
    expect(tree.nodes.filter((node) => node.cycle)).toHaveLength(2)
    expect(tree.nodes.filter((node) => node.knowledge.kind === 'missing').map((node) => node.knowledge.id)).toEqual(missingPreviewSources)
    expect(new Set(tree.nodes.filter((node) => node.knowledge.name === '边界案例：相同标题').map((node) => node.knowledge.id)).size).toBe(2)
    expect(tree.omitted).toBe(0)
  })

  it.each(['predecessors', 'successors'] as const)('keeps an isolated root in %s mode', (direction) => {
    expect(buildKnowledgeTree(index, knowledgePreviewRoots.isolated, direction).nodes).toHaveLength(1)
  })

  it('traverses all 64 levels of the deep chain to its policy', () => {
    const tree = buildKnowledgeTree(index, knowledgePreviewRoots.deep, 'predecessors')
    expect(tree.nodes).toHaveLength(65)
    expect(tree.nodes[64].knowledge.id).toBe(knowledgePreviewRoots.policy)
    expect(tree.nodes[64].depth).toBe(64)
    expect(tree.omitted).toBe(0)
  })

  it('expands the wide policy beyond 500 without duplicating or losing adoption paths', () => {
    const initial = buildKnowledgeTree(index, knowledgePreviewRoots.wide, 'successors')
    expect(initial.nodes).toHaveLength(500)
    expect(initial.omitted).toBe(13)
    const expanded = buildKnowledgeTree(index, knowledgePreviewRoots.wide, 'successors', 1000)
    expect(expanded.nodes).toHaveLength(513)
    expect(new Set(expanded.nodes.map((node) => node.knowledge.id)).size).toBe(513)
    expect(expanded.omitted).toBe(0)
  })

  it('preserves all 1535 explanation nodes in the nine-layer lattice', () => {
    for (const budget of [500, 1000, 1500]) {
      const partial = buildKnowledgeTree(index, knowledgePreviewRoots.lattice, 'predecessors', budget)
      expect(partial.nodes).toHaveLength(budget)
      expect(partial.omitted).toBeGreaterThan(0)
    }
    const tree = buildKnowledgeTree(index, knowledgePreviewRoots.lattice, 'predecessors', 2000)
    expect(tree.nodes).toHaveLength(1535)
    expect(new Set(tree.nodes.map((node) => node.knowledge.id)).size).toBe(20)
    expect(tree.nodes.filter((node) => node.knowledge.kind === 'policy_update')).toHaveLength(512)
    expect(tree.omitted).toBe(0)
  })
})
