import { type ReactNode, useMemo, useState } from 'react'

import { DetailDrawer } from '../../components/drawer/DetailDrawer'
import { useAgentsQuery } from '../agents/hooks'
import type { AgentView } from '../agents/types'
import type { Policy } from '../overview/types'
import { isStaticDemo } from '../../lib/runtime'
import {
  useClaimAttributeSuggestionMutation,
  useCreatePolicyMutation,
  useDeprecatePolicyMutation,
} from './hooks'

type ComposeMode = 'chooser' | 'new-pu' | 'deprecate-pu' | 'cau'

type PolicyComposeDrawerProps = {
  open: boolean
  policies: Policy[]
  onClose: () => void
}

function ActionCard({
  title,
  description,
  onClick,
}: {
  title: string
  description: string
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={title}
      className="w-full rounded-md border border-slate-200 bg-white px-3 py-3 text-left transition hover:border-slate-300 hover:bg-slate-50"
    >
      <div className="text-sm font-semibold text-slate-900">{title}</div>
      <div className="mt-1 text-xs leading-5 text-slate-600">{description}</div>
    </button>
  )
}

function SectionLabel({ children }: { children: ReactNode }) {
  return <div className="mb-1 text-xs font-medium text-slate-700">{children}</div>
}

function ErrorBanner({ message }: { message: string | null }) {
  if (!message) return null
  return <div className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700">{message}</div>
}

function AgentTargetPicker({
  agents,
  selectedAgents,
  onChange,
}: {
  agents: AgentView[]
  selectedAgents: string[]
  onChange: (next: string[]) => void
}) {
  const selectedSet = new Set(selectedAgents)

  return (
    <div>
      <SectionLabel>Target Agents</SectionLabel>
      <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
        <div className="rounded border border-dashed border-slate-300 bg-white px-2.5 py-1.5 text-xs font-medium text-slate-700">
          Broadcast to all agents
        </div>
        <div className="mt-2 text-[11px] leading-5 text-slate-500">
          Leave every agent unchecked to broadcast. Selecting one or more agents switches this action to targeted delivery.
        </div>
        <div className="mt-2 space-y-1.5">
          {agents.length ? (
            agents.map((agent) => {
              const checked = selectedSet.has(agent.agent_id)
              return (
                <label
                  key={agent.agent_id}
                  className="flex items-start gap-2 rounded border border-slate-200 bg-white px-2.5 py-2 text-xs text-slate-700"
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => {
                      if (checked) {
                        onChange(selectedAgents.filter((agentId) => agentId !== agent.agent_id))
                        return
                      }
                      onChange([...selectedAgents, agent.agent_id])
                    }}
                  />
                  <span>
                    <span className="block font-mono font-medium text-slate-900">{agent.agent_id}</span>
                    <span className="mt-0.5 block text-[11px] text-slate-500">
                      {agent.active_claims} active claims
                    </span>
                  </span>
                </label>
              )
            })
          ) : (
            <div className="rounded border border-dashed border-slate-300 bg-white px-3 py-2 text-xs text-slate-500">
              No registered agents found. This action will broadcast if you submit it.
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

const inputClass = "w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-slate-400"
const primaryBtn = "rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-blue-800 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200"
const dangerBtn = "rounded-md bg-rose-600 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-rose-700 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500 disabled:hover:bg-slate-200"
const secondaryBtn = "rounded-md border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50"

export function PolicyComposeDrawer({
  open,
  policies,
  onClose,
}: PolicyComposeDrawerProps) {
  const { data: agents = [] } = useAgentsQuery()
  const createPolicy = useCreatePolicyMutation()
  const createClaimAttribute = useClaimAttributeSuggestionMutation()
  const deprecatePolicy = useDeprecatePolicyMutation()

  const [mode, setMode] = useState<ComposeMode>('chooser')
  const [error, setError] = useState<string | null>(null)
  const [newPuForm, setNewPuForm] = useState({
    name: '',
    scope: '',
    statement: '',
    targetAgents: [] as string[],
  })
  const [cauForm, setCauForm] = useState({
    statement: '',
    targetAgents: [] as string[],
  })
  const [policySearch, setPolicySearch] = useState('')
  const [selectedPolicyId, setSelectedPolicyId] = useState('')

  const activePolicies = useMemo(
    () => policies.filter((policy) => policy.status === 'active'),
    [policies],
  )
  const filteredActivePolicies = useMemo(() => {
    const needle = policySearch.trim().toLowerCase()
    if (!needle) return activePolicies
    return activePolicies.filter((policy) =>
      `${policy.id} ${policy.name} ${policy.scope}`.toLowerCase().includes(needle),
    )
  }, [activePolicies, policySearch])

  function handleMutationError(value: unknown) {
    setError(value instanceof Error ? value.message : 'Request failed')
  }

  function goBack() {
    setError(null)
    setMode('chooser')
  }

  async function submitNewPolicy() {
    setError(null)
    const legalAgents = new Set(agents.map((agent) => agent.agent_id))
    const hasIllegalAgent = newPuForm.targetAgents.some((agentId) => !legalAgents.has(agentId))
    if (!newPuForm.name.trim() || !newPuForm.scope.trim() || !newPuForm.statement.trim()) {
      setError('Name、Scope、Statement 不能为空。')
      return
    }
    if (hasIllegalAgent) {
      setError('存在非法 target agent。')
      return
    }
    createPolicy.mutate(
      {
        name: newPuForm.name.trim(),
        scope: newPuForm.scope.trim(),
        statement: newPuForm.statement.trim(),
        target_agents: [...newPuForm.targetAgents],
      },
      {
        onSuccess: () => onClose(),
        onError: handleMutationError,
      },
    )
  }

  async function submitClaimAttributeUpdate() {
    setError(null)
    const legalAgents = new Set(agents.map((agent) => agent.agent_id))
    const hasIllegalAgent = cauForm.targetAgents.some((agentId) => !legalAgents.has(agentId))
    if (!cauForm.statement.trim()) {
      setError('Statement 不能为空。')
      return
    }
    if (hasIllegalAgent) {
      setError('存在非法 target agent。')
      return
    }
    createClaimAttribute.mutate(
      {
        statement: cauForm.statement.trim(),
        target_agents: [...cauForm.targetAgents],
      },
      {
        onSuccess: () => onClose(),
        onError: handleMutationError,
      },
    )
  }

  async function submitDeprecatePolicy() {
    setError(null)
    if (!selectedPolicyId) {
      setError('请选择一个 active policy。')
      return
    }
    deprecatePolicy.mutate(selectedPolicyId, {
      onSuccess: () => onClose(),
      onError: handleMutationError,
    })
  }

  const title =
    mode === 'chooser'
      ? 'Choose an action'
      : mode === 'new-pu'
        ? 'New PU'
        : mode === 'deprecate-pu'
          ? 'Deprecate PU'
          : 'CAU'
  const subtitle =
    mode === 'chooser'
      ? 'Open a focused workspace for publishing or policy lifecycle actions.'
      : mode === 'new-pu'
        ? 'Publish a new policy update to a targeted audience or the whole network.'
        : mode === 'deprecate-pu'
          ? 'Deprecate an existing active policy by selecting it from the current history.'
          : 'Publish a claim attribute update with scoped delivery.'

  return (
    <DetailDrawer
      open={open}
      onClose={onClose}
      label="Action"
      ariaLabel="Policy action workspace"
      title={title}
      subtitle={subtitle}
      onBack={mode === 'chooser' ? undefined : goBack}
      backLabel="Back"
      footer={
        mode === 'chooser' ? (
          <button type="button" className={secondaryBtn + ' w-full'} onClick={onClose}>
            Close
          </button>
        ) : mode === 'new-pu' ? (
          <div className="grid grid-cols-2 gap-2">
            <button type="button" className={secondaryBtn} onClick={goBack}>Back</button>
            <button type="button" className={primaryBtn} disabled={isStaticDemo || createPolicy.isPending} onClick={submitNewPolicy}>
              {createPolicy.isPending ? 'Publishing…' : 'Publish New PU'}
            </button>
          </div>
        ) : mode === 'deprecate-pu' ? (
          <div className="grid grid-cols-2 gap-2">
            <button type="button" className={secondaryBtn} onClick={goBack}>Back</button>
            <button type="button" className={dangerBtn} disabled={isStaticDemo || deprecatePolicy.isPending} onClick={submitDeprecatePolicy}>
              {deprecatePolicy.isPending ? 'Deprecating…' : 'Deprecate Policy'}
            </button>
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-2">
            <button type="button" className={secondaryBtn} onClick={goBack}>Back</button>
            <button type="button" className={primaryBtn} disabled={isStaticDemo || createClaimAttribute.isPending} onClick={submitClaimAttributeUpdate}>
              {createClaimAttribute.isPending ? 'Publishing…' : 'Publish CAU'}
            </button>
          </div>
        )
      }
    >
      <ErrorBanner message={error} />
      {isStaticDemo ? (
        <div className="rounded-md border border-violet-200 bg-violet-50 px-3 py-2 text-xs leading-5 text-violet-800">
          Public preview is read-only. You can inspect the workflow, but publishing and deprecation are disabled.
        </div>
      ) : null}

      {mode === 'chooser' ? (
        <div className="space-y-2">
          <ActionCard
            title="New PU"
            description="Create a fresh policy update with name, statement, scope, and targeted delivery."
            onClick={() => setMode('new-pu')}
          />
          <ActionCard
            title="Deprecate PU"
            description="Search active policies and deprecate the one you want to retire."
            onClick={() => setMode('deprecate-pu')}
          />
          <ActionCard
            title="CAU"
            description="Publish a claim attribute update with statement text and optional target agents."
            onClick={() => setMode('cau')}
          />
        </div>
      ) : null}

      {mode === 'new-pu' ? (
        <div className="space-y-3">
          <label className="block">
            <SectionLabel>Name</SectionLabel>
            <input
              aria-label="Name"
              className={inputClass}
              value={newPuForm.name}
              onChange={(event) => setNewPuForm((state) => ({ ...state, name: event.target.value }))}
            />
          </label>
          <label className="block">
            <SectionLabel>Scope</SectionLabel>
            <input
              aria-label="Scope"
              className={inputClass}
              value={newPuForm.scope}
              onChange={(event) => setNewPuForm((state) => ({ ...state, scope: event.target.value }))}
            />
          </label>
          <label className="block">
            <SectionLabel>Statement</SectionLabel>
            <textarea
              aria-label="Statement"
              className="min-h-24 w-full resize-y rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-slate-400"
              value={newPuForm.statement}
              onChange={(event) =>
                setNewPuForm((state) => ({ ...state, statement: event.target.value }))
              }
            />
          </label>
          <AgentTargetPicker
            agents={agents}
            selectedAgents={newPuForm.targetAgents}
            onChange={(next) => setNewPuForm((state) => ({ ...state, targetAgents: next }))}
          />
        </div>
      ) : null}

      {mode === 'deprecate-pu' ? (
        <div className="space-y-3">
          <label className="block">
            <SectionLabel>Policy Search</SectionLabel>
            <input
              aria-label="Policy Search"
              placeholder="Search active policies"
              className={inputClass}
              value={policySearch}
              onChange={(event) => setPolicySearch(event.target.value)}
            />
          </label>

          <div className="space-y-2">
            {filteredActivePolicies.length ? (
              filteredActivePolicies.map((policy) => {
                const selected = selectedPolicyId === policy.id
                return (
                  <button
                    key={policy.id}
                    type="button"
                    onClick={() => setSelectedPolicyId(policy.id)}
                    className={`w-full rounded-md border px-3 py-2.5 text-left transition ${
                      selected
                        ? 'border-slate-900 bg-slate-50'
                        : 'border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50'
                    }`}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="text-sm font-semibold text-slate-900">{policy.name}</div>
                      <div className="rounded border border-slate-200 bg-white px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-slate-500">
                        active
                      </div>
                    </div>
                    <div className="mt-1 font-mono text-[11px] text-slate-500">{policy.id}</div>
                    <div className="mt-1 text-xs text-slate-600">{policy.scope}</div>
                  </button>
                )
              })
            ) : (
              <div className="rounded-md border border-dashed border-slate-300 bg-slate-50 px-3 py-3 text-xs text-slate-500">
                No active policies match the current filter.
              </div>
            )}
          </div>
        </div>
      ) : null}

      {mode === 'cau' ? (
        <div className="space-y-3">
          <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700">
            This action publishes a <code className="font-mono text-slate-900">claim_attribute_update</code> policy message.
          </div>
          <label className="block">
            <SectionLabel>Statement</SectionLabel>
            <textarea
              aria-label="Statement"
              className="min-h-24 w-full resize-y rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-slate-400"
              value={cauForm.statement}
              onChange={(event) => setCauForm((state) => ({ ...state, statement: event.target.value }))}
            />
          </label>
          <AgentTargetPicker
            agents={agents}
            selectedAgents={cauForm.targetAgents}
            onChange={(next) => setCauForm((state) => ({ ...state, targetAgents: next }))}
          />
        </div>
      ) : null}
    </DetailDrawer>
  )
}
