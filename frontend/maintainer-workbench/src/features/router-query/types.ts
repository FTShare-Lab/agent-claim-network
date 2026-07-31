import type { Claim } from '../claims/types'

export type AgentQuery = {
  scope: string
  semantic_query?: string | null
}

export type DisputeRef = {
  id: string
  name: string
  claim_ids: string[]
  summary: string
  status: 'open' | 'resolved'
}

export type CandidateClaim = Claim & {
  open_dispute_ids: string[]
  resolved_dispute_ids: string[]
}

export type RetrievalDebugCandidate = {
  claim_id: string
  hit_sources: 'both' | 'lexical' | 'vector' | 'none'
  lexical_score: number
  vector_score: number
  rank_before_rerank: number
  rank_after_rerank: number
  vector_status: 'pending' | 'ready' | 'failed' | 'not_requested'
}

export type RetrievalDebug = {
  mode: 'hybrid' | 'vector_only' | 'lexical_only'
  failed_paths: string[]
  error_summaries: string[]
  lexical_hits: number
  vector_hits: number
  rerank_fallback: boolean
  candidates: RetrievalDebugCandidate[]
}

export type RouterQueryResult = {
  candidate_claims: CandidateClaim[]
  disputes: DisputeRef[]
  retrieval_debug: RetrievalDebug | null
}
