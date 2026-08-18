import type { Claim } from '../claims/types'

export type DisputeStatus = 'open' | 'resolved'
export type ResolutionType = 'coexist' | 'lifecycle_update' | 'conflict_resolved' | 'unresolved'
export type ResolutionBasis = 'direct_analysis' | 'prior_resolution' | 'policy' | 'evidence' | 'insufficient_evidence'

export type ClaimAssessment = {
  claim_id: string
  recommended_status: 'active' | 'stale' | 'deprecated'
  assessment: string
  recommended_scope?: string
  recommended_statement?: string
  reason: string
}

export type DisputeResolution = {
  resolution_id: string
  resolved_by: 'automatic' | 'human'
  resolved_at: string
  resolution_type?: ResolutionType
  resolution_basis?: ResolutionBasis
  conclusion: string
  claim_assessments?: ClaimAssessment[]
  rejection_reason?: string
}

export type AnalysisState =
  | 'pending'
  | 'waiting_context'
  | 'waiting_reanalysis'
  | 'proposing'
  | 'verifying'
  | 'approved'
  | 'unresolved'
  | 'failed'
  | 'adopting'
  | 'adopted'

export type AnalysisError = { code: string; message: string }

export type AnalysisProposal = {
  resolution_type: ResolutionType
  resolution_basis: ResolutionBasis
  conclusion: string
  claim_assessments: ClaimAssessment[]
  confidence: number
  evidence_refs?: string[]
  missing_evidence?: string[]
  human_review_reason?: string
  reasoning: string
}

export type AnalysisVerification = {
  verdict: 'approve' | 'unresolved'
  resolution_type_agreed: boolean
  resolution_basis_agreed: boolean
  conclusion_agreed: boolean
  claim_assessments: Array<{ claim_id: string; agreed: boolean; reason: string }>
  confidence: number
  missing_evidence?: string[]
  reasoning: string
}

export type ArbitrationAnalysisSummary = {
  analysis_id: string
  state: AnalysisState
  phase?: 'proposal' | 'verification' | null
  created_at: string
  updated_at: string
  semantic_fingerprint?: string | null
  proposal?: AnalysisProposal | null
  resolution_id?: string | null
  error?: AnalysisError | null
  adoptable: boolean
  adoption_blocker?: string | null
  analysis_round?: number
  context_change_count?: number
  next_retry_at?: string | null
  context_change_reason?: string | null
}

export type AnalysisRound = {
  round: number
  started_at: string
  completed_at?: string | null
  semantic_projection_version: number
  semantic_fingerprint: string
  context_snapshot_hash: string
  proposal: AnalysisProposal
  verification: AnalysisVerification
  context_change_reason?: string | null
}

export type FrozenAnalysisContext = {
  generated_at: string
  dispute: Dispute
  direct_claims: Claim[]
  source_claims: Claim[]
  policies: unknown[]
  router_candidate_claims: unknown[]
  router_disputes: unknown[]
  prior_resolutions: unknown[]
  warnings: Array<{ code: string; detail: string }>
}

export type ArbitrationAnalysisDetail = ArbitrationAnalysisSummary & {
  frozen_context?: FrozenAnalysisContext | null
  verification?: AnalysisVerification | null
  warnings: Array<{ code: string; detail: string }>
  validation_result: string
  rounds?: AnalysisRound[]
}

export type AnalysisListResponse = {
  current_analysis?: ArbitrationAnalysisSummary | null
}

export type ClaimAdoptionComparison = {
  claim_id: string
  claim_name: string
  snapshot_status?: 'active' | 'stale' | 'deprecated' | null
  snapshot_scope?: string | null
  snapshot_statement?: string | null
  recommended_status: 'active' | 'stale' | 'deprecated'
  current_status?: 'active' | 'stale' | 'deprecated' | null
  recommended_scope?: string | null
  current_scope?: string | null
  recommended_statement?: string | null
  current_statement?: string | null
  policy_provenance_present: boolean
  matches: boolean
  mismatch_reasons: string[]
}

export type HolderAdoption = {
  agent_id: string
  delivery_state: 'not_delivered' | 'delivered'
  observation_state: 'not_delivered' | 'delivered_unobserved' | 'observed_converged' | 'observed_diverged' | 'unknown'
  assessment_count: number
  matched_count: number
  reasons: string[]
  last_delivered_at?: string | null
  last_observed_at?: string | null
  claims: ClaimAdoptionComparison[]
  technical: {
    policy_id: string
    inbox_id: string
    snapshot_source?: string | null
  }
}

export type HolderAdoptionSummary = {
  notified_holders: number
  delivered: number
  converged: number
  diverged: number
  unobserved: number
  unknown: number
}

export type HolderAdoptionView = {
  observed_at: string
  summary: HolderAdoptionSummary
  holders: HolderAdoption[]
}

export type Dispute = {
  id: string
  name: string
  reporter_agent_id: string
  claims: string[]
  summary: string
  status: DisputeStatus
  created_at: string
  resolved_at?: string
  resolution?: DisputeResolution
}

export type DisputeDetail = Dispute & {
  current_analysis?: ArbitrationAnalysisSummary | null
  holder_adoption?: HolderAdoptionView | null
}

export type ArbitrationResolutionRecord = {
  resolution_id: string
  dispute_id: string
  created_at: string
  resolution: DisputeResolution
  dispute_snapshot: Dispute
  direct_claim_snapshots: Claim[]
  semantic_fingerprint?: string
  context_snapshot_hash?: string
  analysis_source_id?: string
  snapshot_source_resolution_id?: string
}

export type ResolveDisputeRequest = {
  resolve_note: string
  notify_affected_agents: boolean
  resolution_type?: Exclude<ResolutionType, 'unresolved'>
  resolution_basis?: ResolutionBasis
  claim_assessments?: ClaimAssessment[]
}

export type RejectResolutionRequest = {
  expected_resolution_id: string
  rejection_reason: string
  conclusion: string
  resolution_type?: Exclude<ResolutionType, 'unresolved'>
  resolution_basis?: ResolutionBasis
  claim_assessments?: ClaimAssessment[]
}
