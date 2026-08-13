import { apiClient } from '../../lib/apiClient'
import type {
  AnalysisListResponse,
  ArbitrationAnalysisDetail,
  ArbitrationAnalysisSummary,
  ArbitrationResolutionRecord,
  Dispute,
  DisputeDetail,
  RejectResolutionRequest,
  ResolveDisputeRequest,
} from './types'

export function listDisputes() {
  return apiClient.get<Dispute[]>('/api/disputes')
}

export function resolveDispute(id: string, request: ResolveDisputeRequest) {
  return apiClient.post<void>(`/disputes/${id}/resolve`, request)
}

export function getDispute(id: string) {
  return apiClient.get<DisputeDetail>(`/api/disputes/${id}`)
}

export function listAnalyses(id: string) {
  return apiClient.get<AnalysisListResponse>(`/api/disputes/${id}/analyses`)
}

export function getAnalysis(id: string, analysisId: string) {
  return apiClient.get<ArbitrationAnalysisDetail>(`/api/disputes/${id}/analyses/${analysisId}`)
}

export function createManualAnalysis(id: string) {
  return apiClient.post<ArbitrationAnalysisSummary>(`/api/disputes/${id}/analyses`, {})
}

export function adoptAnalysis(id: string, analysisId: string) {
  return apiClient.post<ArbitrationResolutionRecord>(
    `/api/disputes/${id}/analyses/${analysisId}/adopt`,
    {},
  )
}

export function rejectResolution(id: string, request: RejectResolutionRequest) {
  return apiClient.post<ArbitrationResolutionRecord>(`/api/disputes/${id}/resolution/reject`, request)
}
