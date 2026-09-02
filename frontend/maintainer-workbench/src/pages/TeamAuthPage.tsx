import { useMemo, useState } from 'react'
import { AlertCircle, Ban, Copy, KeyRound, Plus, ShieldCheck } from 'lucide-react'

import { StatusBadge } from '../components/badges/StatusBadge'
import { DataTable } from '../components/data-table/DataTable'
import { teamAuthErrorMessage } from '../features/team-auth/errors'
import { useCreateTeamAuthKeyMutation, useRevokeTeamAuthKeyMutation, useTeamAuthKeysQuery, useTeamAuthStatusQuery } from '../features/team-auth/hooks'
import type { CreateTeamAuthKeyResponse, TeamAuthKey } from '../features/team-auth/types'
import { PageContainer } from '../layouts/PageContainer'
import { copyTextToClipboard } from '../lib/clipboard'
import { formatDateTime } from '../lib/format'
import { ApiError } from '../lib/apiClient'
import { isStaticDemo } from '../lib/runtime'

export function TeamAuthPage() {
  const statusQuery = useTeamAuthStatusQuery()
  const keysQuery = useTeamAuthKeysQuery()
  const createMutation = useCreateTeamAuthKeyMutation()
  const revokeMutation = useRevokeTeamAuthKeyMutation()
  const [agentId, setAgentId] = useState('')
  const [createdKey, setCreatedKey] = useState<CreateTeamAuthKeyResponse | null>(null)
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'failed'>('idle')
  const statusError = teamAuthErrorMessage(statusQuery.error)
  const keysError = teamAuthErrorMessage(keysQuery.error)
  const managementUnavailable = keysQuery.error instanceof ApiError && keysQuery.error.status === 403
  const activeCount = useMemo(
    () => keysQuery.data?.filter((item) => item.status === 'active').length ?? 0,
    [keysQuery.data],
  )
  const sortedKeys = useMemo(
    () => [...(keysQuery.data ?? [])].sort(compareTeamAuthKeys),
    [keysQuery.data],
  )

  const createKey = () => {
    const trimmed = agentId.trim()
    if (!trimmed) return
    setCreatedKey(null)
    setCopyState('idle')
    createMutation.mutate(
      { agent_id: trimmed },
      {
        onSuccess: (result) => {
          setCreatedKey(result)
          setAgentId('')
        },
      },
    )
  }

  const copyCreatedKey = async () => {
    if (!createdKey) return
    setCopyState('idle')
    setCopyState(await copyTextToClipboard(createdKey.acn_key) ? 'copied' : 'failed')
  }

  const revokeKey = (key: TeamAuthKey) => {
    const confirmed = window.confirm(`Revoke key ${key.key_id} for ${key.agent_id}?`)
    if (!confirmed) return
    revokeMutation.mutate(key.key_id)
  }

  return (
    <PageContainer
      title="Team Auth"
      subtitle="Create and revoke per-agent ACN keys for this team. Plain keys are only returned once after creation."
      actions={<StatusBadge tone="info">{activeCount} active</StatusBadge>}
    >
      <section className="rounded-lg border border-slate-300 bg-slate-950 px-3 py-3 text-white">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div className="flex items-center gap-2">
            <ShieldCheck className="h-4 w-4 text-sky-300" />
            <div>
              <div className="text-sm font-semibold">Team request authentication</div>
              <div className="text-xs text-slate-300">Startup config controls whether agent-facing requests are checked against team keys.</div>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <AuthStatusPill
              label="Maintainer"
              enabled={statusQuery.data?.maintainer_team_auth_enabled}
            />
            <AuthStatusPill
              label="Router"
              enabled={statusQuery.data?.router_team_auth_enabled}
            />
          </div>
        </div>
        {statusError ? (
          <div className="mt-3 flex items-start gap-2 rounded-md border border-rose-300 bg-rose-50 px-2.5 py-2 text-xs text-rose-800" role="alert">
            <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            <span>{statusError}</span>
          </div>
        ) : null}
      </section>

      <section className="grid gap-3 xl:grid-cols-[360px_1fr]">
        <div className="space-y-3">
          <section className="rounded-lg border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-2">
              <KeyRound className="h-4 w-4 text-blue-700" />
              <h2 className="text-sm font-semibold tracking-tight text-slate-900">Create Key</h2>
            </div>
            <div className="mt-3 space-y-2">
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2.5 py-1.5 text-sm outline-none focus:border-slate-400"
                placeholder="agent_id"
                value={agentId}
                disabled={isStaticDemo || managementUnavailable}
                onChange={(event) => setAgentId(event.target.value)}
              />
              <button
                type="button"
                className="inline-flex items-center gap-1.5 rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-blue-800 disabled:cursor-not-allowed disabled:bg-slate-200 disabled:text-slate-500"
                disabled={isStaticDemo || managementUnavailable || !agentId.trim() || createMutation.isPending}
                title={isStaticDemo
                  ? 'Static preview is read-only'
                  : managementUnavailable
                    ? 'Enable Maintainer admin auth to manage team auth keys.'
                    : undefined}
                onClick={createKey}
              >
                <Plus className="h-4 w-4" />
                {createMutation.isPending ? 'Creating' : 'Create'}
              </button>
              {createMutation.error ? (
                <p className="text-xs text-rose-700">{createMutation.error.message}</p>
              ) : null}
            </div>
          </section>

          {createdKey ? (
            <section className="rounded-lg border border-amber-200 bg-amber-50 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-amber-700">
                Plain Key
              </div>
              <div className="mt-2 max-w-full overflow-hidden break-all rounded border border-amber-200 bg-white px-2.5 py-2 font-mono text-xs leading-relaxed text-slate-900">
                {createdKey.acn_key}
              </div>
              <div className="mt-2 flex items-center gap-2">
                <button
                  type="button"
                  className="inline-flex items-center gap-1.5 rounded-md border border-amber-300 bg-white px-2.5 py-1.5 text-xs font-medium text-amber-800 hover:bg-amber-100"
                  onClick={copyCreatedKey}
                >
                  <Copy className="h-3.5 w-3.5" />
                  {copyState === 'copied' ? 'Copied' : copyState === 'failed' ? 'Copy failed' : 'Copy'}
                </button>
                <button
                  type="button"
                  className="rounded-md px-2.5 py-1.5 text-xs font-medium text-amber-800 hover:bg-amber-100"
                  onClick={() => {
                    setCreatedKey(null)
                    setCopyState('idle')
                  }}
                >
                  Dismiss
                </button>
              </div>
              {copyState === 'failed' ? (
                <div className="mt-2 text-xs text-rose-700" role="status">
                  Clipboard copy failed. Select and copy manually.
                </div>
              ) : null}
            </section>
          ) : null}
        </div>

        <div className="space-y-2">
          {revokeMutation.error ? (
            <p className="text-xs text-rose-700">{revokeMutation.error.message}</p>
          ) : null}
          {keysError ? (
            <div className="flex items-start gap-2 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-xs text-rose-700" role="alert">
              <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              <span>{keysError}</span>
            </div>
          ) : null}
          <DataTable
            columns={[
            {
              key: 'key',
              header: 'Key',
              render: (row: TeamAuthKey) => (
                <div>
                  <div className="font-mono text-xs text-slate-900">{row.key_id}</div>
                  <div className="mt-0.5 font-mono text-[11px] text-slate-500">{row.agent_id}</div>
                </div>
              ),
            },
            {
              key: 'generated',
              header: 'Generated',
              render: (row: TeamAuthKey) => (
                <span className="font-mono text-xs">{formatDateTime(row.generated_time)}</span>
              ),
            },
            {
              key: 'status',
              header: 'Status',
              render: (row: TeamAuthKey) => (
                <StatusBadge tone={row.status === 'revoked' ? 'neutral' : undefined}>
                  {row.status}
                </StatusBadge>
              ),
            },
            {
              key: 'actions',
              header: 'Actions',
              className: 'w-28 text-right',
              render: (row: TeamAuthKey) =>
                row.status === 'active' ? (
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 rounded-md border border-slate-200 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:text-slate-400"
                    disabled={isStaticDemo || revokeMutation.isPending}
                    title={isStaticDemo ? 'Static preview is read-only' : undefined}
                    onClick={(event) => {
                      event.stopPropagation()
                      revokeKey(row)
                    }}
                  >
                    <Ban className="h-3.5 w-3.5" />
                    Revoke
                  </button>
                ) : (
                  <span className="text-xs text-slate-400">-</span>
                ),
            },
          ]}
            rows={sortedKeys}
            getRowId={(row) => row.key_id}
            emptyState={keysError ? 'Team auth key management is unavailable.' : keysQuery.isLoading ? 'Loading keys...' : 'No team auth keys have been created.'}
          />
        </div>
      </section>
    </PageContainer>
  )
}

function compareTeamAuthKeys(left: TeamAuthKey, right: TeamAuthKey) {
  const statusRank = (key: TeamAuthKey) => (key.status === 'active' ? 0 : 1)
  const byStatus = statusRank(left) - statusRank(right)
  if (byStatus !== 0) return byStatus

  const byGenerated = Date.parse(right.generated_time) - Date.parse(left.generated_time)
  if (byGenerated !== 0) return byGenerated

  return left.key_id.localeCompare(right.key_id)
}

function AuthStatusPill({ label, enabled }: { label: string; enabled?: boolean }) {
  const text = enabled === undefined ? 'Unknown' : enabled ? 'Enabled' : 'Disabled'
  return (
    <span className="inline-flex items-center gap-1.5 rounded-md border border-white/10 bg-white/10 px-2.5 py-1 text-xs font-semibold text-white">
      <span className="text-slate-300">{label}</span>
      <span className={enabled === false ? 'text-amber-200' : enabled ? 'text-emerald-200' : 'text-slate-300'}>
        {text}
      </span>
    </span>
  )
}
