// 全局 Claim 关系图复用节点详情与状态图标，不生成任何持久化派生数据。
import { useT } from '../../i18n'
import { useQuery } from '@tanstack/react-query'
import { edgePath } from './overviewLayout'
import { useId, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { ClaimView } from '../claims/types'
import type { Policy } from '../overview/types'
import { knowledgeStyles } from './styles'
import type { Knowledge } from './tree'
import { buildClaimOverview, OVERVIEW_NODE_HEIGHT as H, OVERVIEW_NODE_WIDTH as W } from './overview'
import { KnowledgeStatusIcons, KnowledgeStatusLegend } from './KnowledgeStatus'
import { KnowledgeTypeLegend } from './KnowledgeTypeLegend'
import { useCanvasPan } from './useCanvasPan'
import { cn } from '../../lib/utils'

export function ClaimOverview({ claims, policies = [], onSelect }: { claims: ClaimView[]; policies?: Policy[]; onSelect: (knowledge: Knowledge) => void }) {
  const t = useT()
  const selection = useMemo(() => buildClaimOverview(claims, policies), [claims, policies])
  const viewport = useRef<HTMLDivElement>(null)
  const marker = useId()
  const [zoom, setZoom] = useState(1)
  const [size, setSize] = useState({ width: 900, height: 560 })
  const [highlight, setHighlight] = useState<string | null>(null)
  const topology = useMemo(() => ({
    ids: selection.nodes.map((node) => node.id).sort(),
    edges: [...selection.edges].sort((a, b) => a.source.localeCompare(b.source) || a.target.localeCompare(b.target)),
  }), [selection])
  // 视口比例按半档取整，避免拖动窗口时为每个像素重新布局。
  const aspectRatio = Math.max(0.5, Math.round(size.width / size.height * 2) / 2)
  const layout = useQuery({
    queryKey: ['claim-overview-layout', topology, aspectRatio],
    queryFn: async ({ signal }) => {
      const { loadOverviewLayout } = await import('./overviewLayoutBrowser')
      return loadOverviewLayout(topology, aspectRatio, signal)
    },
    staleTime: Infinity,
    gcTime: 300000,
    retry: false,
  })
  const graph = layout.data ?? { nodes: [], edges: [], width: 500, height: 300 }
  const views = new Map(selection.nodes.map((node) => [node.id, node.knowledge]))
  const related = new Set(highlight ? [highlight, ...graph.edges.filter((edge) => edge.source === highlight || edge.target === highlight).flatMap((edge) => [edge.source, edge.target])] : [])
  const { isPanning, ...pan } = useCanvasPan()
  useLayoutEffect(() => {
    const element = viewport.current
    if (!element) return
    const update = () => setSize({ width: element.clientWidth || 900, height: element.clientHeight || 560 })
    update()
    if (typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(update); observer.observe(element)
    return () => observer.disconnect()
  }, [])
  const fit = Math.min(1, size.width / graph.width, size.height / graph.height)
  const scale = fit * zoom
  const width = Math.max(graph.width * scale, size.width) + 480
  const height = Math.max(graph.height * scale, size.height) + 480
  const left = (width - graph.width * scale) / 2, top = (height - graph.height * scale) / 2
  useLayoutEffect(() => {
    if (viewport.current) { viewport.current.scrollLeft = (width - size.width) / 2; viewport.current.scrollTop = (height - size.height) / 2 }
  }, [width, height, size])
  return <section aria-label={t("All claims overview")} className="overflow-hidden rounded-xl border border-slate-200 bg-white">
    <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 px-4 py-3">
      <div className="space-y-2">
        <KnowledgeTypeLegend />
        <KnowledgeStatusLegend />
      </div>
      <div className="flex items-center gap-2 text-xs">
        <button type="button" aria-label={t("Zoom out")} className="rounded px-2 py-2 hover:bg-slate-100" disabled={zoom <= 0.5} onClick={() => setZoom(Math.max(0.5, zoom / 1.4))}>−</button>
        <span>{Math.round(scale * 100)}%</span>
        <button type="button" aria-label={t("Zoom in")} className="rounded px-2 py-2 hover:bg-slate-100" disabled={zoom >= 5} onClick={() => setZoom(Math.min(5, zoom * 1.4))}>+</button>
        <button type="button" className="rounded px-2 py-2 hover:bg-slate-100" onClick={() => { setZoom(1); if (viewport.current) { viewport.current.scrollLeft = 240; viewport.current.scrollTop = 240 } }}>{t("Fit")}</button>
      </div>
    </div>
      {layout.isPending ? <p role="status" className="p-6 text-sm text-slate-500">{t('Arranging connections…')}</p> : layout.isError ? <div role="alert" className="p-6 text-sm text-slate-600">{t('Unable to arrange the graph.')} <button type="button" className="underline" onClick={() => void layout.refetch()}>{t('Retry')}</button></div> : null}
    <div ref={viewport} {...pan} tabIndex={0} role="region" aria-label={t("Overview canvas")} className={cn('h-[560px] max-h-[65vh] min-h-72 overflow-auto bg-[radial-gradient(#d9dce5_1px,transparent_1px)] bg-[size:20px_20px]', isPanning ? 'cursor-grabbing' : 'cursor-grab')}>

      <div className="relative" style={{ width, height }}><div className="absolute origin-top-left" style={{ left, top, width: graph.width, height: graph.height, transform: `scale(${scale})` }}>
        <svg aria-hidden="true" width={graph.width} height={graph.height} className="pointer-events-none absolute inset-0">
          <defs><marker id={marker} viewBox="0 0 10 10" refX="10" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0 L10 5 L0 10 z" fill="#64748b" /></marker></defs>
          {graph.edges.map(({ source, target, points }) => {
            const active = !highlight || source === highlight || target === highlight
            return <g key={`${source}:${target}`} opacity={active ? 1 : 0.12}>
              <path d={edgePath(points)} fill="none" stroke="white" strokeWidth="5" strokeLinejoin="round" />
              <path data-source={source} data-target={target} d={edgePath(points)} fill="none" stroke={highlight && active ? '#475569' : '#94a3b8'} strokeWidth="1.5" strokeLinejoin="round" markerEnd={`url(#${marker})`} />
            </g>
          })}
        </svg>
        {graph.nodes.map(({ id, x, y }) => {
          const knowledge = views.get(id)!
          const style = knowledgeStyles[knowledge.kind]
          return <button key={id} type="button" data-overview-node={id} title={`${knowledge.name} · ${id}`} onClick={() => onSelect(knowledge)} onMouseEnter={() => setHighlight(id)} onMouseLeave={() => setHighlight(null)} onFocus={() => setHighlight(id)} onBlur={() => setHighlight(null)} className={cn('absolute flex items-center justify-center rounded-lg border px-3 pb-0.5 pt-4 text-center text-sm font-semibold shadow-sm hover:ring-2 hover:ring-slate-300', style.className)} style={{ left: x, top: y, width: W, height: H, opacity: highlight && !related.has(id) ? 0.3 : 1 }}>
            <KnowledgeStatusIcons knowledge={knowledge} /><span className="line-clamp-2 [overflow-wrap:anywhere]">{knowledge.name}</span>
          </button>
        })}
      </div></div>
    </div>
    <div className="space-y-1 border-t border-slate-200 px-4 py-3 text-xs text-slate-600" aria-live="polite">
      <p>{t('Showing {shown} of {total} Claims · {policies} of {policyTotal} related PU/CAU · {edges} connections.', { shown: selection.claimCount, total: selection.total, policies: selection.policyCount, policyTotal: selection.policyTotal, edges: selection.edges.length })}</p>
      {selection.total > selection.claimCount ? <p>{t("Limited to 30 Claims. Larger connected groups take priority; highly connected neighbors are selected first. Other Claims remain available in the list.")}</p> : null}
      {selection.policyTotal > selection.policyCount ? <p>{t("Limited to 20 related PU/CAU. Those linked to more displayed Claims take priority, then the most recently updated. Other sources remain available in the individual Claim view.")}</p> : null}
      <p>{t("Each node appears once. Arrows point from source to derived Claim. Unavailable sources are omitted from this overview.")}</p>
      {selection.total === 0 ? <p>{t("No Claims available.")}</p> : null}
    </div>
  </section>
}
