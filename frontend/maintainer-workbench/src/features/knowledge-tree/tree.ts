// 将来源图按路径展开为树：跨路径保留重复节点，路径内的回环终止展开。
import type { ClaimView } from '../claims/types'
import type { Policy } from '../overview/types'

export type Knowledge =
  | { kind: 'claim'; id: string; name: string; view: ClaimView }
  | { kind: 'policy_update' | 'claim_attribute_update'; id: string; name: string; policy: Policy }
  | { kind: 'missing'; id: string; name: string }

export type Direction = 'predecessors' | 'successors'

export type KnowledgeIndex = {
  entries: Map<string, Knowledge>
  predecessors: Map<string, string[]>
  successors: Map<string, string[]>
}

export function indexKnowledge(claims: ClaimView[], policies: Policy[]): KnowledgeIndex {
  const entries = new Map<string, Knowledge>()
  const predecessors = new Map<string, string[]>()
  const successors = new Map<string, string[]>()
  for (const view of claims) {
    const { claim } = view
    entries.set(claim.id, { kind: 'claim', id: claim.id, name: claim.name, view })
    const sources = [...new Set(claim.source_claim_ids)]
    predecessors.set(claim.id, sources)
    for (const source of sources) {
      const children = successors.get(source) ?? []
      children.push(claim.id)
      successors.set(source, children)
    }
  }
  for (const policy of policies) {
    entries.set(policy.id, { kind: policy.message_type, id: policy.id, name: policy.name, policy })
  }
  for (const sources of predecessors.values()) {
    for (const id of sources) {
      if (!entries.has(id)) entries.set(id, { kind: 'missing', id, name: 'Unavailable source' })
    }
  }
  return { entries, predecessors, successors }
}

export type TreeNode = {
  key: number
  path: string
  knowledge: Knowledge
  parent: number | null
  children: number[]
  depth: number
  cycle: boolean
  childCount: number
  expanded: boolean
  omitted: number
  x: number
  y: number
}

export const NODE_WIDTH = 220
// 为右上角状态图标单独留出一行，避免覆盖两行标题。
export const NODE_HEIGHT = 80
export const BRANCH_BATCH_SIZE = 4

export type TreeExpansion = {
  expandAll: boolean
  childLimits: ReadonlyMap<string, number>
}
const COLUMN_GAP = 32
const ROW_GAP = 64
const PADDING = 32
// 节点外的分支按钮也占空间，需为按钮边框、阴影和焦点轮廓预留上下留白。
export const VERTICAL_PADDING = 48

export function buildKnowledgeTree(index: KnowledgeIndex, rootId: string, direction: Direction, budget = 500, expansion?: TreeExpansion) {
  const root = index.entries.get(rootId)
  const nodes: TreeNode[] = []
  if (!root || budget < 1) return { nodes, width: 0, height: 0, omitted: 0, uniqueCount: 0 }
  const adjacency = index[direction]
  nodes.push({ key: 0, path: encodeURIComponent(rootId), knowledge: root, parent: null, children: [], depth: 0, cycle: false, childCount: 0, expanded: false, omitted: 0, x: 0, y: 0 })
  let maxDepth = 0
  // 广度优先预算让大型图先呈现各条分支；后续提高预算可继续展开。
  for (let cursor = 0; cursor < nodes.length; cursor++) {
    const node = nodes[cursor]
    if (node.cycle) continue
    const nextIds = adjacency.get(node.knowledge.id) ?? []
    const defaultLimit = !expansion || expansion.expandAll ? Infinity : node.depth === 0 ? BRANCH_BATCH_SIZE : 0
    const limit = expansion?.childLimits.get(node.path) ?? defaultLimit
    node.childCount = nextIds.length
    node.expanded = limit > 0 && nextIds.length > 0
    for (const id of nextIds.slice(0, limit)) {
      if (nodes.length >= budget) {
        node.omitted++
        continue
      }
      const knowledge = index.entries.get(id)
      if (!knowledge) continue
      let cycle = false
      let ancestor: TreeNode | undefined = node
      while (ancestor) {
        if (ancestor.knowledge.id === id) { cycle = true; break }
        ancestor = ancestor.parent === null ? undefined : nodes[ancestor.parent]
      }
      const key = nodes.length
      node.children.push(key)
      maxDepth = Math.max(maxDepth, node.depth + 1)
      // 路径使用知识 ID，避免其他分支增删后展开状态跟随数组序号错位。
      nodes.push({ key, path: `${node.path}/${encodeURIComponent(id)}`, knowledge, parent: cursor, children: [], depth: node.depth + 1, cycle, childCount: 0, expanded: false, omitted: 0, x: 0, y: 0 })
    }
  }

  // 迭代后序布局避免深链消耗调用栈；父节点位于子树中央。
  let leaf = 0
  const stack: { key: number; visited: boolean }[] = [{ key: 0, visited: false }]
  while (stack.length) {
    const step = stack.pop()
    if (!step) break
    const node = nodes[step.key]
    if (!step.visited && node.children.length) {
      stack.push({ key: step.key, visited: true })
      for (const key of [...node.children].reverse()) stack.push({ key, visited: false })
      continue
    }
    node.x = node.children.length
      ? (nodes[node.children[0]].x + nodes[node.children[node.children.length - 1]].x) / 2
      : PADDING + leaf++ * (NODE_WIDTH + COLUMN_GAP)
    node.y = VERTICAL_PADDING + (direction === 'predecessors' ? maxDepth - node.depth : node.depth) * (NODE_HEIGHT + ROW_GAP)
  }
  return {
    nodes,
    width: PADDING * 2 + leaf * (NODE_WIDTH + COLUMN_GAP) - COLUMN_GAP,
    height: VERTICAL_PADDING * 2 + NODE_HEIGHT + maxDepth * (NODE_HEIGHT + ROW_GAP),
    omitted: nodes.reduce((sum, node) => sum + node.omitted, 0),
    uniqueCount: new Set(nodes.map((node) => node.knowledge.id)).size,
  }
}
