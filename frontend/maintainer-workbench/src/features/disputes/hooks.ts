import { useEffect, useRef } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { ApiError } from '../../lib/apiClient'
import {
  adoptAnalysis,
  createManualAnalysis,
  getAnalysis,
  getDispute,
  listAnalyses,
  listDisputes,
  rejectResolution,
  resolveDispute,
} from './api'
import type {
  AnalysisState,
  ArbitrationResolutionRecord,
  ArbitrationAnalysisSummary,
  Dispute,
  DisputeDetail,
  RejectResolutionRequest,
  ResolveDisputeRequest,
} from './types'

const IN_PROGRESS_ANALYSIS_STATES = new Set<AnalysisState>([
  'pending',
  'waiting_context',
  'waiting_reanalysis',
  'proposing',
  'verifying',
  'adopting',
])
const ACTIVE_POLL_INTERVAL_MS = 1_000
const IDLE_POLL_INTERVAL_MS = 5_000

function isAnalysisInProgress(state?: AnalysisState) {
  return state ? IN_PROGRESS_ANALYSIS_STATES.has(state) : false
}

function applyResolutionToCache(
  queryClient: ReturnType<typeof useQueryClient>,
  id: string,
  record: ArbitrationResolutionRecord,
) {
  const closeDispute = <T extends Dispute>(dispute: T): T => ({
    ...dispute,
    status: 'resolved',
    resolved_at: record.resolution.resolved_at,
    resolution: record.resolution,
  })
  queryClient.setQueryData<Dispute[]>(['disputes'], (current) => (
    current?.map((dispute) => dispute.id === id ? closeDispute(dispute) : dispute)
  ))
  queryClient.setQueryData<DisputeDetail>(['disputes', id], (current) => (
    current ? closeDispute(current) : current
  ))
}

function allAnalyses(data?: {
  automatic_analysis?: ArbitrationAnalysisSummary | null
  manual_analysis?: ArbitrationAnalysisSummary | null
}) {
  if (!data) return []
  return [data.automatic_analysis, data.manual_analysis].filter(
    (analysis): analysis is ArbitrationAnalysisSummary => Boolean(analysis),
  )
}

export function useDisputesQuery() {
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: ['disputes'],
    queryFn: listDisputes,
    refetchInterval: IDLE_POLL_INTERVAL_MS,
  })

  useEffect(() => {
    for (const dispute of query.data ?? []) {
      const detailKey = ['disputes', dispute.id] as const
      const current = queryClient.getQueryData<DisputeDetail>(detailKey)
      if (!current || dispute.status !== 'resolved') continue

      const resolutionChanged = (
        current.status !== 'resolved'
        || current.resolved_at !== dispute.resolved_at
        || current.resolution?.resolution_id !== dispute.resolution?.resolution_id
      )
      if (!resolutionChanged) continue

      queryClient.setQueryData<DisputeDetail>(detailKey, {
        ...current,
        status: 'resolved',
        resolved_at: dispute.resolved_at,
        resolution: dispute.resolution,
      })
      void Promise.all([
        queryClient.invalidateQueries({ queryKey: detailKey, exact: true }),
        queryClient.invalidateQueries({
          queryKey: ['disputes', dispute.id, 'analyses'],
        }),
      ])
    }
  }, [query.data, queryClient])

  return query
}

export function useDisputeDetailQuery(id?: string) {
  return useQuery({
    queryKey: ['disputes', id],
    queryFn: () => getDispute(id!),
    enabled: Boolean(id),
    refetchInterval: false,
  })
}

export function useAnalysesQuery(id?: string) {
  const queryClient = useQueryClient()
  const previousStates = useRef<{ disputeId?: string; states: Map<string, AnalysisState> }>({
    states: new Map(),
  })
  const query = useQuery({
    queryKey: ['disputes', id, 'analyses'],
    queryFn: () => listAnalyses(id!),
    enabled: Boolean(id),
    refetchInterval: (currentQuery) => (
      allAnalyses(currentQuery.state.data).some((analysis) => isAnalysisInProgress(analysis.state))
        ? ACTIVE_POLL_INTERVAL_MS
        : IDLE_POLL_INTERVAL_MS
    ),
  })

  useEffect(() => {
    if (!id || !query.data) return
    const analyses = allAnalyses(query.data)
    const previous = previousStates.current
    const progressed = previous.disputeId === id && analyses.some((analysis) => {
      const oldState = previous.states.get(analysis.analysis_id)
      return oldState !== undefined && oldState !== analysis.state
    })
    previousStates.current = {
      disputeId: id,
      states: new Map(analyses.map((analysis) => [analysis.analysis_id, analysis.state])),
    }
    if (progressed) {
      void invalidateDisputeQueries(queryClient, id, false)
    }
  }, [id, query.data, queryClient])

  return query
}

export function useAnalysisDetailQuery(id?: string, analysisId?: string) {
  return useQuery({
    queryKey: ['disputes', id, 'analyses', analysisId],
    queryFn: () => getAnalysis(id!, analysisId!),
    enabled: Boolean(id && analysisId),
    refetchInterval: (query) => (
      isAnalysisInProgress(query.state.data?.state) ? ACTIVE_POLL_INTERVAL_MS : false
    ),
  })
}

async function invalidateDisputeQueries(
  queryClient: ReturnType<typeof useQueryClient>,
  id: string,
  includeAnalyses = true,
) {
  const invalidations = [
    queryClient.invalidateQueries({ queryKey: ['disputes'], exact: true }),
    queryClient.invalidateQueries({ queryKey: ['disputes', id], exact: true }),
    queryClient.invalidateQueries({ queryKey: ['claims'] }),
    queryClient.invalidateQueries({ queryKey: ['overview'] }),
    queryClient.invalidateQueries({ queryKey: ['policies'] }),
    queryClient.invalidateQueries({ queryKey: ['outbox'] }),
  ]
  if (includeAnalyses) {
    invalidations.push(queryClient.invalidateQueries({ queryKey: ['disputes', id, 'analyses'] }))
  }
  await Promise.all(invalidations)
}

async function refreshResolutionFenceAfterConflict(
  queryClient: ReturnType<typeof useQueryClient>,
  id: string,
  error: Error,
) {
  if (!(error instanceof ApiError) || error.status !== 409) return
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['disputes'], exact: true }),
    queryClient.invalidateQueries({ queryKey: ['disputes', id], exact: true }),
    queryClient.invalidateQueries({ queryKey: ['disputes', id, 'analyses'] }),
  ])
}

export function useResolveDisputeMutation() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ id, request }: { id: string; request: ResolveDisputeRequest }) => resolveDispute(id, request),
    onSettled: async (_data, _error, variables) => invalidateDisputeQueries(queryClient, variables.id),
  })
}

export function useCreateManualAnalysisMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: createManualAnalysis,
    onSettled: async (_data, _error, id) => invalidateDisputeQueries(queryClient, id),
  })
}

export function useAdoptAnalysisMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, analysisId }: { id: string; analysisId: string }) => (
      adoptAnalysis(id, analysisId)
    ),
    onSuccess: async (record, variables) => {
      applyResolutionToCache(queryClient, variables.id, record)
    },
    onError: async (error, variables) => refreshResolutionFenceAfterConflict(
      queryClient,
      variables.id,
      error,
    ),
    onSettled: async (_data, _error, variables) => invalidateDisputeQueries(queryClient, variables.id),
  })
}

export function useRejectResolutionMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, request }: { id: string; request: RejectResolutionRequest }) => rejectResolution(id, request),
    onSuccess: async (record, variables) => {
      applyResolutionToCache(queryClient, variables.id, record)
    },
    onError: async (error, variables) => refreshResolutionFenceAfterConflict(
      queryClient,
      variables.id,
      error,
    ),
    onSettled: async (_data, _error, variables) => invalidateDisputeQueries(queryClient, variables.id),
  })
}
