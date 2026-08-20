import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, cleanup, renderHook } from '@testing-library/react'
import type { PropsWithChildren } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { ApiError } from '../../lib/apiClient'
import * as disputeApi from './api'
import {
  useAdoptAnalysisMutation,
  useAnalysesQuery,
  useAnalysisDetailQuery,
  useDisputesQuery,
  useResolveDisputeMutation,
} from './hooks'
import type {
  AnalysisState,
  ArbitrationResolutionRecord,
  ArbitrationAnalysisDetail,
  ArbitrationAnalysisSummary,
  Dispute,
} from './types'

vi.mock('./api', () => ({
  adoptAnalysis: vi.fn(),
  createAnalysis: vi.fn(),
  getAnalysis: vi.fn(),
  getDispute: vi.fn(),
  listAnalyses: vi.fn(),
  listDisputes: vi.fn(),
  rejectResolution: vi.fn(),
  resolveDispute: vi.fn(),
}))

const disputeId = 'dispute_1234abcd'
const analysisId = 'analysis_1234abcd1234abcd'
const createdAt = '2026-08-10T08:00:00Z'

function analysis(state: AnalysisState, automaticProgressPending = false): ArbitrationAnalysisSummary {
  return {
    analysis_id: analysisId,
    state,
    created_at: createdAt,
    updated_at: createdAt,
    semantic_fingerprint: 'sha256-v1:test',
    adoptable: state === 'approved',
    automatic_progress_pending: automaticProgressPending,
  }
}

function analysisDetail(state: AnalysisState): ArbitrationAnalysisDetail {
  return {
    ...analysis(state),
    frozen_context: {
      generated_at: createdAt,
      dispute: dispute('open'),
      direct_claims: [],
      source_claims: [],
      policies: [],
      router_candidate_claims: [],
      router_disputes: [],
      prior_resolutions: [],
      warnings: [],
    },
    warnings: [],
    validation_result: 'valid',
  }
}

function dispute(status: Dispute['status']): Dispute {
  return {
    id: disputeId,
    name: 'background arbitration',
    reporter_agent_id: 'agent-a',
    claims: [],
    summary: 'background arbitration',
    status,
    created_at: createdAt,
    resolved_at: status === 'resolved' ? '2026-08-10T08:05:00Z' : undefined,
  }
}

function createQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: 0 } },
  })
}

function wrapper(queryClient: QueryClient) {
  return function QueryWrapper({ children }: PropsWithChildren) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  }
}

afterEach(() => {
  cleanup()
  vi.useRealTimers()
  vi.clearAllMocks()
})

describe('dispute analysis queries', () => {
  it('does not poll the dispute list while the Workbench remains mounted', async () => {
    vi.useFakeTimers()
    vi.mocked(disputeApi.listDisputes).mockResolvedValue([dispute('open')])
    const queryClient = createQueryClient()
    const { result } = renderHook(() => useDisputesQuery(), { wrapper: wrapper(queryClient) })

    await vi.waitFor(() => expect(result.current.data?.[0]?.status).toBe('open'))
    await act(async () => vi.advanceTimersByTimeAsync(10_000))
    expect(disputeApi.listDisputes).toHaveBeenCalledTimes(1)
  })

  it('polls the current analysis only while active and refreshes dependent views after progress', async () => {
    vi.useFakeTimers()
    vi.mocked(disputeApi.listAnalyses)
      .mockResolvedValueOnce({ current_analysis: analysis('proposing') })
      .mockResolvedValue({ current_analysis: analysis('approved') })
    const queryClient = createQueryClient()
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries')
    const { result } = renderHook(() => useAnalysesQuery(disputeId), { wrapper: wrapper(queryClient) })

    await vi.waitFor(() => expect(result.current.data?.current_analysis?.state).toBe('proposing'))
    await act(async () => vi.advanceTimersByTimeAsync(1_000))
    await vi.waitFor(() => expect(result.current.data?.current_analysis?.state).toBe('approved'))
    expect(disputeApi.listAnalyses).toHaveBeenCalledTimes(2)
    await act(async () => vi.advanceTimersByTimeAsync(10_000))
    expect(disputeApi.listAnalyses).toHaveBeenCalledTimes(2)

    for (const queryKey of [
      ['disputes'],
      ['disputes', disputeId],
      ['disputes', disputeId, 'analyses', analysisId],
      ['claims'],
      ['overview'],
      ['policies'],
      ['outbox'],
    ]) {
      expect(invalidate).toHaveBeenCalledWith(expect.objectContaining({ queryKey }))
    }
  })

  it('continues polling an auto-approved analysis through automatic adoption', async () => {
    vi.useFakeTimers()
    vi.mocked(disputeApi.listAnalyses)
      .mockResolvedValueOnce({ current_analysis: analysis('proposing') })
      .mockResolvedValueOnce({ current_analysis: analysis('approved', true) })
      .mockResolvedValue({ current_analysis: analysis('adopted') })
    const queryClient = createQueryClient()
    const { result } = renderHook(() => useAnalysesQuery(disputeId), { wrapper: wrapper(queryClient) })

    await vi.waitFor(() => expect(result.current.data?.current_analysis?.state).toBe('proposing'))
    await act(async () => vi.advanceTimersByTimeAsync(1_000))
    await vi.waitFor(() => expect(result.current.data?.current_analysis?.state).toBe('approved'))
    await act(async () => vi.advanceTimersByTimeAsync(1_000))
    await vi.waitFor(() => expect(result.current.data?.current_analysis?.state).toBe('adopted'))
    await act(async () => vi.advanceTimersByTimeAsync(10_000))

    expect(disputeApi.listAnalyses).toHaveBeenCalledTimes(3)
  })

  it('does not fetch analyses without a selected dispute', async () => {
    const queryClient = createQueryClient()
    renderHook(() => useAnalysesQuery(undefined), { wrapper: wrapper(queryClient) })
    await act(async () => Promise.resolve())
    expect(disputeApi.listAnalyses).not.toHaveBeenCalled()
  })

  it('polls an analysis detail only while it is in progress', async () => {
    vi.useFakeTimers()
    vi.mocked(disputeApi.getAnalysis)
      .mockResolvedValueOnce(analysisDetail('verifying'))
      .mockResolvedValue(analysisDetail('approved'))
    const queryClient = createQueryClient()
    const { result } = renderHook(
      () => useAnalysisDetailQuery(disputeId, analysisId),
      { wrapper: wrapper(queryClient) },
    )

    await vi.waitFor(() => expect(result.current.data?.state).toBe('verifying'))
    await act(async () => vi.advanceTimersByTimeAsync(1_000))
    await vi.waitFor(() => expect(result.current.data?.state).toBe('approved'))
    expect(disputeApi.getAnalysis).toHaveBeenCalledTimes(2)
  })

  it('continues polling auto-approved analysis detail until adoption completes', async () => {
    vi.useFakeTimers()
    vi.mocked(disputeApi.getAnalysis)
      .mockResolvedValueOnce(analysisDetail('verifying'))
      .mockResolvedValueOnce({
        ...analysisDetail('approved'),
        automatic_progress_pending: true,
      })
      .mockResolvedValue(analysisDetail('adopted'))
    const queryClient = createQueryClient()
    const { result } = renderHook(
      () => useAnalysisDetailQuery(disputeId, analysisId),
      { wrapper: wrapper(queryClient) },
    )

    await vi.waitFor(() => expect(result.current.data?.state).toBe('verifying'))
    await act(async () => vi.advanceTimersByTimeAsync(1_000))
    await vi.waitFor(() => expect(result.current.data?.state).toBe('approved'))
    await act(async () => vi.advanceTimersByTimeAsync(1_000))
    await vi.waitFor(() => expect(result.current.data?.state).toBe('adopted'))

    expect(disputeApi.getAnalysis).toHaveBeenCalledTimes(3)
  })

  it('invalidates analysis, resolution, delivery, observation, policy, and overview data after adopt', async () => {
    const adoptedRecord: ArbitrationResolutionRecord = {
      resolution_id: 'resolution_1234abcd',
      dispute_id: disputeId,
      created_at: createdAt,
      resolution: {
        resolution_id: 'resolution_1234abcd',
        resolved_by: 'human',
        resolved_at: createdAt,
        conclusion: 'adopted',
      },
      dispute_snapshot: dispute('open'),
      direct_claim_snapshots: [],
      analysis_source_id: analysisId,
    }
    vi.mocked(disputeApi.adoptAnalysis).mockResolvedValue(adoptedRecord)
    const queryClient = createQueryClient()
    queryClient.setQueryData(['disputes'], [dispute('open')])
    queryClient.setQueryData(['disputes', disputeId], dispute('open'))
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries')
    const { result } = renderHook(() => useAdoptAnalysisMutation(), { wrapper: wrapper(queryClient) })

    await act(async () => {
      await result.current.mutateAsync({ id: disputeId, analysisId })
    })

    expect(disputeApi.adoptAnalysis).toHaveBeenCalledWith(disputeId, analysisId)
    expect(queryClient.getQueryData<Dispute[]>(['disputes'])?.[0]).toMatchObject({
      status: 'resolved',
      resolution: adoptedRecord.resolution,
    })
    expect(queryClient.getQueryData<Dispute>(['disputes', disputeId])).toMatchObject({
      status: 'resolved',
      resolution: adoptedRecord.resolution,
    })
    for (const queryKey of [
      ['disputes', disputeId, 'analyses'],
      ['claims'],
      ['overview'],
      ['policies'],
      ['outbox'],
    ]) {
      expect(invalidate).toHaveBeenCalledWith(expect.objectContaining({ queryKey }))
    }
  })

  it('immediately refreshes fencing data after an adoption conflict', async () => {
    vi.mocked(disputeApi.adoptAnalysis).mockRejectedValue(
      new ApiError('分析输入已变化', 409, 'Conflict'),
    )
    const queryClient = createQueryClient()
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries')
    const { result } = renderHook(() => useAdoptAnalysisMutation(), { wrapper: wrapper(queryClient) })

    await act(async () => {
      await result.current.mutateAsync({ id: disputeId, analysisId }).catch(() => undefined)
    })

    for (const queryKey of [
      ['disputes'],
      ['disputes', disputeId],
      ['disputes', disputeId, 'analyses'],
    ]) {
      expect(invalidate).toHaveBeenCalledWith(expect.objectContaining({ queryKey }))
    }
  })

  it('refetches authoritative dispute data when a resolve response is lost', async () => {
    vi.mocked(disputeApi.resolveDispute).mockRejectedValue(new TypeError('connection reset'))
    const queryClient = createQueryClient()
    queryClient.setQueryData(['disputes', disputeId], dispute('open'))
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries')
    const { result } = renderHook(() => useResolveDisputeMutation(), {
      wrapper: wrapper(queryClient),
    })

    await act(async () => {
      await result.current.mutateAsync({
        id: disputeId,
        request: { resolve_note: 'resolved on server', notify_affected_agents: false },
      }).catch(() => undefined)
    })

    for (const queryKey of [
      ['disputes'],
      ['disputes', disputeId],
      ['disputes', disputeId, 'analyses'],
    ]) {
      expect(invalidate).toHaveBeenCalledWith(expect.objectContaining({ queryKey }))
    }
  })
})
