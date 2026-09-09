// 列表与知识树共享 Claim 搜索和筛选控件。
import { FilterBar } from '../../components/filters/FilterBar'
import type { ClaimFilterValues } from './filter'

export function ClaimFilters({ filters, agents, onChange }: {
  filters: ClaimFilterValues
  agents: string[]
  onChange: (patch: Partial<ClaimFilterValues>) => void
}) {
  const { keyword, agent, status, scope } = filters
  return (
    <FilterBar>
      <label className="space-y-1 text-xs text-slate-600">
        <span className="font-medium text-slate-700">Search</span>
        <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" placeholder="id, title, statement, evidence, scope" value={keyword} onChange={(event) => { onChange({ keyword: event.target.value }) }} />
      </label>
      <label className="space-y-1 text-xs text-slate-600">
        <span className="font-medium text-slate-700">Agent</span>
        <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={agent} onChange={(event) => { onChange({ agent: event.target.value }) }}>
          <option value="all">All Agents</option>
          {agents.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
      </label>
      <label className="space-y-1 text-xs text-slate-600">
        <span className="font-medium text-slate-700">Status</span>
        <select className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" value={status} onChange={(event) => { onChange({ status: event.target.value }) }}>
          <option value="all">All Status</option>
          <option value="active">Active</option>
          <option value="stale">Stale</option>
          <option value="deprecated">Deprecated</option>
        </select>
      </label>
      <label className="space-y-1 text-xs text-slate-600">
        <span className="font-medium text-slate-700">Scope</span>
        <input className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none" placeholder="scope fragment" value={scope} onChange={(event) => { onChange({ scope: event.target.value }) }} />
      </label>
    </FilterBar>
  )
}
