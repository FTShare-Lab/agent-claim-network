import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { renderWorkbenchRoute } from '../app/test-utils'
import { saveAdminSession } from '../features/auth/session'
import type { ClaimView } from '../features/claims/types'
import type {
  ArbitrationAnalysisDetail,
  ArbitrationAnalysisSummary,
  DisputeDetail,
} from '../features/disputes/types'

const createdAt = '2026-05-15T10:00:00Z'

const claimsResponse: ClaimView[] = [
  {
    claim: {
      id: 'claim_a',
      name: 'claim-a-name',
      statement: 'claim a statement',
      scope: 'payments',
      holder: 'agent-a',
      confidence: 'high',
      status: 'active',
      created_at: '2026-05-15T09:00:00Z',
      source_claim_ids: [],
      evidence_summary: 'claim a evidence',
    },
    open_dispute_ids: ['dispute_open'],
    resolved_dispute_ids: ['dispute_resolved'],
  },
  {
    claim: {
      id: 'claim_b',
      name: 'claim-b-name',
      statement: 'claim b statement',
      scope: 'payments',
      holder: 'agent-b',
      confidence: 'medium',
      status: 'active',
      created_at: '2026-05-15T09:30:00Z',
      source_claim_ids: ['claim_a'],
      evidence_summary: 'claim b evidence',
    },
    open_dispute_ids: ['dispute_open'],
    resolved_dispute_ids: [],
  },
]

const proposal = {
  resolution_type: 'conflict_resolved' as const,
  resolution_basis: 'evidence' as const,
  conclusion: 'Keep the evidence-backed Claim and retire the contradicted default.',
  claim_assessments: [
    {
      claim_id: 'claim_a',
      recommended_status: 'active' as const,
      assessment: 'Supported by current evidence.',
      reason: 'The production trace directly supports it.',
    },
    {
      claim_id: 'claim_b',
      recommended_status: 'deprecated' as const,
      assessment: 'Contradicted under the same scope.',
      reason: 'The reproduction disproves the old default.',
    },
  ],
  confidence: 0.94,
  evidence_refs: ['claim_a', 'claim_b'],
  missing_evidence: [],
  reasoning: 'The scopes and environments are directly comparable.',
}

const unresolvedAnalysis: ArbitrationAnalysisSummary = {
  analysis_id: 'analysis_unresolved',
  state: 'unresolved',
  created_at: createdAt,
  updated_at: '2026-05-15T10:05:00Z',
  semantic_fingerprint: 'sha256-v1:auto',
  proposal,
  adoptable: false,
  adoption_blocker: 'The verification result is unresolved.',
}

const approvedAnalysis: ArbitrationAnalysisSummary = {
  analysis_id: 'analysis_approved',
  state: 'approved',
  created_at: '2026-05-15T11:00:00Z',
  updated_at: '2026-05-15T11:04:00Z',
  semantic_fingerprint: 'sha256-v1:manual',
  proposal,
  adoptable: true,
}

function analysisDetail(analysis: ArbitrationAnalysisSummary): ArbitrationAnalysisDetail {
  return {
    ...analysis,
    frozen_context: {
      generated_at: analysis.created_at,
      dispute: openDispute,
      direct_claims: claimsResponse.map((item) => item.claim),
      source_claims: [],
      policies: [{ id: 'policy_team' }],
      router_candidate_claims: [{ id: 'claim_related_a' }, { id: 'claim_related_b' }],
      router_disputes: [],
      prior_resolutions: [],
      warnings: [],
    },
    verification: analysis.state === 'approved' ? {
      verdict: 'approve',
      resolution_type_agreed: true,
      resolution_basis_agreed: true,
      conclusion_agreed: true,
      claim_assessments: proposal.claim_assessments.map((assessment) => ({
        claim_id: assessment.claim_id,
        agreed: true,
        reason: 'The assessment follows the frozen evidence.',
      })),
      confidence: 0.95,
      missing_evidence: [],
      reasoning: 'Independent verification agrees with every core field.',
    } : analysis.state === 'unresolved' ? {
      verdict: 'unresolved',
      resolution_type_agreed: false,
      resolution_basis_agreed: false,
      conclusion_agreed: false,
      claim_assessments: [],
      confidence: 0.58,
      missing_evidence: ['A decisive reproduction is missing.'],
      reasoning: 'Independent verification cannot approve the proposed Claim changes.',
    } : undefined,
    warnings: [],
    validation_result: 'valid',
  }
}

const openDispute = {
  id: 'dispute_open',
  name: 'Scope mismatch',
  reporter_agent_id: 'agent-a',
  claims: ['claim_a', 'claim_b'],
  summary: 'original dispute summary',
  status: 'open' as const,
  created_at: createdAt,
}

const resolution = {
  resolution_id: 'resolution_1234abcd',
  resolved_by: 'automatic' as const,
  resolved_at: '2026-05-15T11:00:00Z',
  resolution_type: 'conflict_resolved' as const,
  resolution_basis: 'evidence' as const,
  conclusion: 'automatic conclusion',
  claim_assessments: proposal.claim_assessments,
}

const resolvedDispute = {
  id: 'dispute_resolved',
  name: 'Already resolved',
  reporter_agent_id: 'agent-b',
  claims: ['claim_a'],
  summary: 'already resolved summary',
  status: 'resolved' as const,
  created_at: '2026-05-14T10:00:00Z',
  resolved_at: '2026-05-15T11:00:00Z',
  resolution,
}

const longSnapshotStatement = '裁决时快照中的知识陈述包含明确的版本、部署拓扑、复现条件和操作边界。'.repeat(10)
const longCurrentStatement = '当前镜像中的知识陈述保留了旧版本默认行为，并包含需要人工复核的运行条件。'.repeat(10)

const resolvedDetail: DisputeDetail = {
  ...resolvedDispute,
  current_analysis: {
    ...approvedAnalysis,
    analysis_id: 'analysis_resolved',
    // 模拟 Adopt 后仍残留在客户端缓存里的旧 approved 摘要。已 resolved 时
    // 页面必须以 Resolution 为准，不能继续展示旧采用阻挡信息。
    state: 'approved',
    resolution_id: resolution.resolution_id,
    adoptable: false,
    adoption_blocker: 'Dispute 已经被其他 Decision 解决',
  },
  holder_adoption: {
    observed_at: '2026-05-15T12:30:00Z',
    summary: {
      notified_holders: 5,
      delivered_holders: 4,
      updated_claims: 1,
      unchanged_claims: 1,
      unavailable_claims: 1,
    },
    holders: [
      {
        agent_id: 'agent-unchanged',
        delivery_state: 'delivered',
        observation_state: 'no_update_observed',
        claim_count: 1,
        updated_claim_count: 0,
        unchanged_claim_count: 1,
        unavailable_claim_count: 0,
        reasons: [],
        last_delivered_at: '2026-05-15T11:10:00Z',
        last_observed_at: '2026-05-15T12:30:00Z',
        claims: [{
          claim_id: 'claim_b',
          claim_name: 'claim-b-name',
          snapshot_status: 'active',
          snapshot_scope: 'payments',
          snapshot_statement: 'claim b statement',
          recommended_status: 'deprecated',
          current_status: 'active',
          current_scope: 'payments',
          current_statement: 'claim b statement',
          policy_provenance_present: false,
          update_observed: false,
          changed_fields: [],
          notes: [],
        }],
        technical: { policy_id: 'policy_resolution', inbox_id: 'inbox_unchanged' },
      },
      {
        agent_id: 'agent-updated',
        delivery_state: 'delivered',
        observation_state: 'update_observed',
        claim_count: 1,
        updated_claim_count: 1,
        unchanged_claim_count: 0,
        unavailable_claim_count: 0,
        reasons: [],
        last_delivered_at: '2026-05-15T11:12:00Z',
        last_observed_at: '2026-05-15T12:30:00Z',
        claims: [{
          claim_id: 'claim_a',
          claim_name: 'claim-a-name',
          snapshot_status: 'active',
          snapshot_scope: 'payments / previous',
          snapshot_statement: longSnapshotStatement,
          recommended_status: 'deprecated',
          current_status: 'active',
          recommended_scope: 'payments / current',
          current_scope: 'payments / legacy',
          recommended_statement: longSnapshotStatement,
          current_statement: longCurrentStatement,
          policy_provenance_present: false,
          update_observed: true,
          changed_fields: ['scope', 'statement'],
          notes: [],
        }],
        technical: {
          policy_id: 'policy_resolution',
          inbox_id: 'inbox_resolution',
          snapshot_source: 'current mirror',
        },
      },
      {
        agent_id: 'agent-no-update',
        delivery_state: 'delivered',
        observation_state: 'no_update_observed',
        claim_count: 0,
        updated_claim_count: 0,
        unchanged_claim_count: 0,
        unavailable_claim_count: 0,
        reasons: [],
        claims: [],
        technical: { policy_id: 'policy_resolution', inbox_id: 'inbox_no_update' },
      },
      {
        agent_id: 'agent-unknown',
        delivery_state: 'delivered',
        observation_state: 'unknown',
        claim_count: 1,
        updated_claim_count: 0,
        unchanged_claim_count: 0,
        unavailable_claim_count: 1,
        reasons: ['The holder mirror is missing or duplicated.'],
        claims: [],
        technical: { policy_id: 'policy_resolution', inbox_id: 'inbox_unknown' },
      },
      {
        agent_id: 'agent-not-delivered',
        delivery_state: 'not_delivered',
        observation_state: 'not_delivered',
        claim_count: 0,
        updated_claim_count: 0,
        unchanged_claim_count: 0,
        unavailable_claim_count: 0,
        reasons: ['The inbox has not been acknowledged.'],
        claims: [],
        technical: { policy_id: 'policy_resolution', inbox_id: 'inbox_not_delivered' },
      },
    ],
  },
}

function buildFetch(overrides?: {
  createAnalysis?: Response
  adopt?: Response
  resolvedDetail?: DisputeDetail
  currentAnalysis?: ArbitrationAnalysisSummary | null
}) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = typeof input === 'string' ? input : input.toString()
    const method = init?.method ?? 'GET'
    if (path === '/api/disputes') return new Response(JSON.stringify([resolvedDispute, openDispute]))
    if (path === '/api/claims') return new Response(JSON.stringify(claimsResponse))
    if (path === '/api/overview') return new Response(JSON.stringify({}))
    if (path === '/api/disputes/dispute_open' && method === 'GET') {
      return new Response(JSON.stringify({
        ...openDispute,
        current_analysis: overrides && 'currentAnalysis' in overrides
          ? overrides.currentAnalysis
          : approvedAnalysis,
      }))
    }
    if (path === '/api/disputes/dispute_resolved' && method === 'GET') {
      return new Response(JSON.stringify(overrides?.resolvedDetail ?? resolvedDetail))
    }
    if (path === '/api/disputes/dispute_open/analyses' && method === 'POST') {
      return overrides?.createAnalysis ?? new Response(JSON.stringify({
        ...approvedAnalysis,
        analysis_id: 'analysis_started',
        state: 'pending',
        proposal: undefined,
        adoptable: false,
        adoption_blocker: 'Analysis is still running.',
      }), { status: 202 })
    }
    if (path === '/api/disputes/dispute_open/analyses' && method === 'GET') {
      return new Response(JSON.stringify({
        current_analysis: overrides && 'currentAnalysis' in overrides
          ? overrides.currentAnalysis
          : approvedAnalysis,
      }))
    }
    if (path === '/api/disputes/dispute_resolved/analyses' && method === 'GET') {
      return new Response(JSON.stringify({
        current_analysis: overrides?.resolvedDetail?.current_analysis ?? resolvedDetail.current_analysis,
      }))
    }
    const analysisMatch = path.match(/^\/api\/disputes\/dispute_(open|resolved)\/analyses\/([^/]+)$/)
    if (analysisMatch && method === 'GET') {
      const analysisId = analysisMatch[2]
      const analysis = [
        unresolvedAnalysis,
        approvedAnalysis,
        overrides?.resolvedDetail?.current_analysis ?? resolvedDetail.current_analysis!,
      ].find((item) => item.analysis_id === analysisId)
      if (!analysis) return new Response('analysis not found', { status: 404 })
      return new Response(JSON.stringify(analysisDetail(analysis)))
    }
    if (path === '/api/disputes/dispute_open/analyses/analysis_approved/adopt' && method === 'POST') {
      return overrides?.adopt ?? new Response(JSON.stringify({
        resolution_id: 'resolution_adopted',
        dispute_id: 'dispute_open',
        created_at: '2026-05-15T12:00:00Z',
        resolution: { ...resolution, resolution_id: 'resolution_adopted', resolved_by: 'human' },
        dispute_snapshot: openDispute,
        direct_claim_snapshots: claimsResponse.map((item) => item.claim),
        analysis_source_id: approvedAnalysis.analysis_id,
      }), { status: 201 })
    }
    if (path === '/disputes/dispute_open/resolve' && method === 'POST') {
      return new Response(null, { status: 204 })
    }
    if (path === '/api/disputes/dispute_resolved/resolution/reject' && method === 'POST') {
      return new Response(JSON.stringify({
        resolution_id: 'resolution_replacement',
        dispute_id: resolvedDispute.id,
        created_at: '2026-05-15T12:00:00Z',
        resolution: { ...resolution, resolution_id: 'resolution_replacement', resolved_by: 'human' },
        dispute_snapshot: resolvedDispute,
        direct_claim_snapshots: [],
      }), { status: 201 })
    }
    throw new Error(`Unhandled fetch: ${method} ${path}`)
  })
}

describe('DisputesPage', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    saveAdminSession('admin', 'Basic test')
    vi.stubGlobal('fetch', buildFetch())
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('orders open disputes first and keeps attempt concepts out of the list', async () => {
    render(renderWorkbenchRoute('/disputes'))
    const table = within((await screen.findByRole('heading', { name: 'All Disputes' })).closest('section')!).getByRole('table')
    const rows = within(table).getAllByRole('row').slice(1)

    expect(within(rows[0]).getByText('Scope mismatch')).toBeInTheDocument()
    expect(within(rows[1]).getByText('Already resolved')).toBeInTheDocument()
    expect(table).not.toHaveTextContent('Latest Attempt')
    expect(table).not.toHaveTextContent('Attempt')
  })

  it('keeps direct Claims readable and contains no serialized JSON', async () => {
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))

    const details = screen.getByLabelText('Direct claim details')
    expect(within(details).getByRole('article', { name: 'claim-a-name' })).toHaveTextContent('agent-a')
    expect(details).toHaveTextContent('claim b statement')
    expect(details).toHaveTextContent('claim b evidence')
    expect(details).not.toHaveTextContent('"holder":')
    expect(details).not.toHaveTextContent('"statement":')
  })

  it('shows exactly one current unresolved analysis without source categories', async () => {
    vi.stubGlobal('fetch', buildFetch({ currentAnalysis: unresolvedAnalysis }))
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    const drawer = await screen.findByRole('dialog', { name: 'Scope mismatch' })

    const current = await within(drawer).findByRole('article', { name: 'Current analysis analysis_unresolved' })
    expect(current).toHaveTextContent('unresolved')
    expect(current).toHaveTextContent('不建议修改 Claim')
    expect(within(current).queryByText(/Direct Claim assessments/)).not.toBeInTheDocument()
    expect(within(current).queryByRole('button', { name: '采用此分析' })).not.toBeInTheDocument()
    fireEvent.click(await within(current).findByText('Analysis context summary'))
    expect(current).toHaveTextContent('Related Claims in context2')
    expect(current).not.toHaveTextContent('Router candidates')
    expect(drawer).not.toHaveTextContent('Automatic Analysis')
    expect(drawer).not.toHaveTextContent('Manual Analysis')
    expect(within(drawer).queryByText('Attempt Timeline')).not.toBeInTheDocument()
  })

  it('keeps Analyze and Human Resolve available when no current analysis exists', async () => {
    vi.stubGlobal('fetch', buildFetch({ currentAnalysis: null }))
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    const drawer = await screen.findByRole('dialog', { name: 'Scope mismatch' })

    expect(await within(drawer).findByText('No analysis is recorded for this dispute.')).toBeInTheDocument()
    expect(within(drawer).getByRole('button', { name: 'Analyze' })).toBeEnabled()
    expect(within(drawer).getByRole('button', { name: 'Resolve Dispute' })).toBeEnabled()
  })

  it('creates a replacement current analysis and reports that it started', async () => {
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    fireEvent.click(await screen.findByRole('button', { name: 'Analyze' }))

    await waitFor(() => expect(fetch).toHaveBeenCalledWith(
      '/api/disputes/dispute_open/analyses',
      expect.objectContaining({ method: 'POST', body: '{}' }),
    ))
    expect(await screen.findByRole('status')).toHaveTextContent('Analysis started')
    expect(screen.getByRole('status')).toHaveTextContent('analysis_started')
  })

  it('updates Analyze feedback when polling reaches a terminal state', async () => {
    const fallbackFetch = buildFetch()
    const startedAnalysis: ArbitrationAnalysisSummary = {
      ...approvedAnalysis,
      analysis_id: 'analysis_started',
      state: 'pending',
      created_at: '2026-05-15T12:00:00Z',
      updated_at: '2026-05-15T12:00:00Z',
      proposal: undefined,
      adoptable: false,
      adoption_blocker: 'Analysis is still running.',
    }
    const failedAnalysis: ArbitrationAnalysisSummary = {
      ...startedAnalysis,
      state: 'failed',
      updated_at: '2026-05-15T12:00:05Z',
      error: { code: 'provider_timeout', message: 'The evaluator timed out.' },
      adoption_blocker: 'Failed analyses cannot be adopted.',
    }
    let analysisStarted = false
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = typeof input === 'string' ? input : input.toString()
      const method = init?.method ?? 'GET'
      if (path === '/api/disputes/dispute_open/analyses' && method === 'POST') {
        analysisStarted = true
        return new Response(JSON.stringify(startedAnalysis), { status: 202 })
      }
      if (analysisStarted && path === '/api/disputes/dispute_open/analyses' && method === 'GET') {
        return new Response(JSON.stringify({
          current_analysis: failedAnalysis,
        }))
      }
      if (
        analysisStarted
        && path === '/api/disputes/dispute_open/analyses/analysis_started'
        && method === 'GET'
      ) {
        return new Response(JSON.stringify(analysisDetail(failedAnalysis)))
      }
      return fallbackFetch(input, init)
    }))

    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    fireEvent.click(await screen.findByRole('button', { name: 'Analyze' }))

    const feedback = await screen.findByRole('alert')
    expect(feedback).toHaveTextContent('Analysis failed')
    expect(feedback).toHaveTextContent('provider_timeout')
    expect(feedback).toHaveTextContent('analysis_started')
  })

  it('shows an Analyze conflict inline', async () => {
    vi.stubGlobal('fetch', buildFetch({ createAnalysis: new Response('analysis scheduler is unavailable', { status: 409 }) }))
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    fireEvent.click(await screen.findByRole('button', { name: 'Analyze' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('analysis scheduler is unavailable')
  })

  it('adopts an approved analysis without starting a new model call', async () => {
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    fireEvent.click(await screen.findByRole('button', { name: '采用此分析' }))

    await waitFor(() => expect(fetch).toHaveBeenCalledWith(
      '/api/disputes/dispute_open/analyses/analysis_approved/adopt',
      expect.objectContaining({ method: 'POST', body: '{}' }),
    ))
    expect(await screen.findByRole('status')).toHaveTextContent('Analysis adopted as resolution resolution_adopted')
    expect(fetch).not.toHaveBeenCalledWith(
      '/api/disputes/dispute_open/analyses',
      expect.objectContaining({ method: 'POST' }),
    )
  })

  it('shows an analysis-input-changed adoption conflict and refreshes current data', async () => {
    vi.stubGlobal('fetch', buildFetch({ adopt: new Response('分析输入已变化，请重新 Analyze', { status: 409 }) }))
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    const countGets = (path: string) => vi.mocked(fetch).mock.calls.filter(
      ([input, init]) => input === path && (init?.method ?? 'GET') === 'GET',
    ).length
    const detailGets = countGets('/api/disputes/dispute_open')
    const analysisGets = countGets('/api/disputes/dispute_open/analyses')
    fireEvent.click(await screen.findByRole('button', { name: '采用此分析' }))

    const conflict = await screen.findByRole('alert')
    expect(conflict).toHaveTextContent('分析输入已变化')
    expect(conflict).toHaveTextContent('run Analyze again')
    expect(conflict).toHaveTextContent('Current dispute data has been refreshed')
    await waitFor(() => {
      expect(countGets('/api/disputes/dispute_open')).toBeGreaterThan(detailGets)
      expect(countGets('/api/disputes/dispute_open/analyses')).toBeGreaterThan(analysisGets)
    })
  })

  it('prioritizes the current resolution and collapses resolved analysis details', async () => {
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Already resolved'))
    const drawer = await screen.findByRole('dialog', { name: 'Already resolved' })
    const currentResolution = await within(drawer).findByRole('region', { name: 'Current resolution' })
    expect(currentResolution).toHaveTextContent('automatic conclusion')
    expect(currentResolution).toHaveTextContent('Resolution recommendations')
    expect(currentResolution).toHaveTextContent('not the Claims\' current status')
    expect(currentResolution).toHaveTextContent('Recommended · active')
    expect(within(drawer).queryByRole('button', { name: /decision chain/i })).not.toBeInTheDocument()
    expect(within(drawer).queryByText(/Cannot adopt:/)).not.toBeInTheDocument()
    expect(within(drawer).queryByText('Approved，但采用被阻止')).not.toBeInTheDocument()
    expect(within(drawer).queryByText('Dispute 已经被其他 Decision 解决')).not.toBeInTheDocument()
    expect(within(drawer).queryByRole('button', { name: 'Analyze' })).not.toBeInTheDocument()
    expect(within(drawer).queryByRole('button', { name: 'Resolve Dispute' })).not.toBeInTheDocument()

    const analysisToggle = within(drawer).getByText(/View analysis process/)
    const collapsedAnalysis = analysisToggle.closest('details')
    expect(collapsedAnalysis).not.toHaveAttribute('open')

    fireEvent.click(analysisToggle)
    expect(collapsedAnalysis).toHaveAttribute('open')
    expect(within(drawer).getByRole('article', { name: 'Current analysis analysis_resolved' })).toBeInTheDocument()
    expect(within(drawer).getByText('Historical analysis')).toBeInTheDocument()
  })

  it('never presents a resolved analysis as an active reanalysis wait', async () => {
    const waitingResolvedDetail: DisputeDetail = {
      ...resolvedDetail,
      current_analysis: {
        ...resolvedDetail.current_analysis!,
        state: 'waiting_reanalysis',
        analysis_round: 2,
        context_change_count: 1,
        next_retry_at: '2026-05-15T11:09:00Z',
        context_change_reason: 'The analysis input changed.',
      },
    }
    vi.stubGlobal('fetch', buildFetch({ resolvedDetail: waitingResolvedDetail }))
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Already resolved'))
    const drawer = await screen.findByRole('dialog', { name: 'Already resolved' })
    const analysisToggle = await within(drawer).findByText('View analysis process')

    expect(analysisToggle).not.toHaveTextContent('waiting reanalysis')
    fireEvent.click(analysisToggle)
    const analysis = await within(drawer).findByRole('article', { name: 'Current analysis analysis_resolved' })
    expect(analysis).toHaveTextContent('Historical analysis')
    expect(analysis).not.toHaveTextContent('等待 5 分钟后重新分析')
    expect(analysis).not.toHaveTextContent('下次重试')
  })

  it('summarizes observed updates without scoring recommendation compliance', async () => {
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Already resolved'))
    const panel = await screen.findByRole('region', { name: 'Delivery and holder adoption' })

    await waitFor(() => expect(panel).toHaveTextContent('Notified holders5'))
    expect(panel).toHaveClass('border-amber-700', 'bg-amber-50/40')
    expect(panel).toHaveTextContent('Delivered holders4')
    expect(panel).toHaveTextContent('Updated Claims1')
    expect(panel).toHaveTextContent('Unchanged Claims1')
    expect(panel).toHaveTextContent('Unavailable Claims1')
    expect(panel).not.toHaveTextContent('Converged')
    expect(panel).not.toHaveTextContent('Diverged')
    expect(within(panel).queryByText('agent-updated')).not.toBeInTheDocument()

    fireEvent.click(within(panel).getByRole('button', { name: 'Show holder adoption' }))
    expect(within(panel).getAllByText('Not delivered')).not.toHaveLength(0)
    expect(within(panel).getAllByText('No update observed')).not.toHaveLength(0)
    expect(within(panel).getByText('Update observed')).toBeInTheDocument()
    expect(within(panel).getByText('Claim mirror unavailable')).toBeInTheDocument()
  })

  it('renders Claim snapshots before and after Agent internalization', async () => {
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Already resolved'))
    const panel = await screen.findByRole('region', { name: 'Delivery and holder adoption' })
    await waitFor(() => expect(panel).toHaveTextContent('Notified holders5'))
    fireEvent.click(within(panel).getByRole('button', { name: 'Show holder adoption' }))
    const holder = within(panel).getByRole('article', { name: 'Holder adoption agent-updated' })

    fireEvent.click(within(holder).getByText('Resolution snapshot / current mirror (1)'))
    const comparison = within(holder).getByRole('article', { name: 'Claim adoption claim_a' })
    expect(comparison).toHaveTextContent('At Resolution')
    expect(comparison).toHaveTextContent('Current Mirror')
    expect(comparison).toHaveTextContent('payments / previous')
    expect(comparison).toHaveTextContent('payments / legacy')
    expect(comparison).not.toHaveTextContent('Recommended status')
    expect(comparison).not.toHaveTextContent('Matched')
    expect(comparison).not.toHaveTextContent('Mismatch')
    expect(comparison).not.toHaveTextContent(longSnapshotStatement)
    fireEvent.click(within(comparison).getAllByRole('button', { name: '展开全文' })[0])
    expect(comparison).toHaveTextContent(longSnapshotStatement)
    fireEvent.click(within(comparison).getByRole('button', { name: '收起全文' }))
    expect(comparison).not.toHaveTextContent(longSnapshotStatement)

    fireEvent.click(within(holder).getByText('Technical details'))
    expect(holder).toHaveTextContent('Policy ID:')
    expect(holder).toHaveTextContent('policy_resolution')
    expect(holder).not.toHaveTextContent('"policy_id"')
  })

  it('renders an empty holder-adoption state without raw JSON', async () => {
    vi.stubGlobal('fetch', buildFetch({ resolvedDetail: { ...resolvedDetail, holder_adoption: null } }))
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Already resolved'))
    const panel = await screen.findByRole('region', { name: 'Delivery and holder adoption' })
    expect(panel).toHaveTextContent('Notified holders0')
    fireEvent.click(within(panel).getByRole('button', { name: 'Show holder adoption' }))
    expect(panel).toHaveTextContent('No holder delivery or adoption data is available.')
    expect(panel).not.toHaveTextContent('{')
  })

  it('preserves Human Resolve and Reject & Replace mutations', async () => {
    render(renderWorkbenchRoute('/disputes'))
    fireEvent.click(await screen.findByText('Scope mismatch'))
    fireEvent.change(await screen.findByLabelText('Resolve Note'), { target: { value: '  Human conclusion.  ' } })
    expect(screen.getByRole('checkbox', { name: /Notify Affected Agents/ })).toBeChecked()
    fireEvent.click(screen.getByRole('button', { name: 'Resolve Dispute' }))
    await waitFor(() => expect(fetch).toHaveBeenCalledWith(
      '/disputes/dispute_open/resolve',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ resolve_note: 'Human conclusion.', notify_affected_agents: true }),
      }),
    ))

    fireEvent.click(screen.getByRole('button', { name: 'Close' }))
    fireEvent.click(await screen.findByText('Already resolved'))
    fireEvent.change(await screen.findByPlaceholderText('Why is the automatic resolution rejected?'), { target: { value: 'Incomplete evidence.' } })
    fireEvent.change(screen.getByPlaceholderText('Replacement conclusion'), { target: { value: 'Reviewed conclusion.' } })
    expect(screen.getByLabelText('Replacement Resolution Basis')).not.toHaveTextContent('Insufficient evidence')
    fireEvent.click(screen.getByRole('button', { name: 'Reject & Replace' }))
    await waitFor(() => expect(fetch).toHaveBeenCalledWith(
      '/api/disputes/dispute_resolved/resolution/reject',
      expect.objectContaining({ method: 'POST' }),
    ))
  })
})
