// 先按连通关系选 Claim，再补充其直接引用的 PU/CAU；仅生成展示数据。
import type { ClaimView } from '../claims/types'
import type { Policy } from '../overview/types'
import type { Knowledge } from './tree'

export const OVERVIEW_LIMIT = 30
export const OVERVIEW_POLICY_LIMIT = 20
export const OVERVIEW_NODE_WIDTH = 220
export const OVERVIEW_NODE_HEIGHT = 60

export function buildClaimOverview(views: ClaimView[], policies: Policy[] = [], limit = OVERVIEW_LIMIT) {
  const claims = new Map(views.map((view) => [view.claim.id, view]))
  const adjacency = new Map([...claims.keys()].map((id) => [id, new Set<string>()]))
  const edges: { source: string; target: string }[] = []
  for (const [id, view] of claims) {
    for (const source of new Set(view.claim.source_claim_ids)) {
      if (!claims.has(source)) continue
      edges.push({ source, target: id })
      if (source !== id) {
        adjacency.get(id)!.add(source)
        adjacency.get(source)!.add(id)
      }
    }
  }
  const compare = (a: string, b: string) => adjacency.get(b)!.size - adjacency.get(a)!.size || a.localeCompare(b)
  const unseen = new Set([...claims.keys()].sort(compare))
  const components: string[][] = []
  while (unseen.size) {
    const first = unseen.values().next().value!
    const component = [first]
    unseen.delete(first)
    for (let i = 0; i < component.length; i++) {
      for (const next of adjacency.get(component[i])!) {
        if (unseen.delete(next)) component.push(next)
      }
    }
    components.push(component.sort(compare))
  }
  components.sort((a, b) => b.length - a.length || compare(a[0], b[0]))
  const selected = new Set<string>()
  let displayedComponents = 0
  for (const component of components) {
    if (selected.size >= Math.max(0, limit)) break
    displayedComponents++
    const frontier = new Set([component[0]])
    while (frontier.size && selected.size < limit) {
      // 仅从当前连通区域的边界扩展，避免高连接节点排序切出零散孤点。
      const next = [...frontier].sort(compare)[0]
      frontier.delete(next)
      selected.add(next)
      for (const neighbor of adjacency.get(next)!) if (!selected.has(neighbor)) frontier.add(neighbor)
    }
  }
  const selectedEdges = edges.filter((edge) => selected.has(edge.source) && selected.has(edge.target))
  const policyById = new Map(policies.map((policy) => [policy.id, policy]))
  const policyTargets = new Map<string, Set<string>>()
  for (const id of selected) {
    for (const source of claims.get(id)!.claim.source_claim_ids) {
      if (!policyById.has(source) || claims.has(source)) continue
      const targets = policyTargets.get(source) ?? new Set<string>()
      targets.add(id)
      policyTargets.set(source, targets)
    }
  }
  const updatedAt = (id: string) => {
    const policy = policyById.get(id)!
    return Date.parse(policy.updated_at ?? policy.created_at) || 0
  }
  const selectedPolicies = [...policyTargets.keys()].sort((a, b) =>
    policyTargets.get(b)!.size - policyTargets.get(a)!.size
    || updatedAt(b) - updatedAt(a) || a.localeCompare(b),
  ).slice(0, OVERVIEW_POLICY_LIMIT)
  const nodes: { id: string; knowledge: Knowledge }[] = [...selected].map((id) => {
    const view = claims.get(id)!
    return { id, knowledge: { kind: 'claim', id, name: view.claim.name, view } }
  })
  for (const id of selectedPolicies) {
    const policy = policyById.get(id)!
    nodes.push({ id, knowledge: { kind: policy.message_type, id, name: policy.name, policy } })
    for (const target of policyTargets.get(id)!) selectedEdges.push({ source: id, target })
  }
  return {
    nodes, edges: selectedEdges, total: claims.size, claimCount: selected.size,
    policyCount: selectedPolicies.length, policyTotal: policyTargets.size,
    components: components.length, displayedComponents,
  }
}

export type ClaimOverviewGraph = ReturnType<typeof buildClaimOverview>
