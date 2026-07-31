export type Confidence = 'high' | 'medium' | 'low'
export type ClaimStatus = 'active' | 'stale' | 'deprecated'

export type Claim = {
  id: string
  name: string
  statement: string
  scope: string
  holder: string
  confidence: Confidence
  status: ClaimStatus
  created_at: string
  updated_at?: string
  source_claim_ids: string[]
  evidence_summary: string
}

export type ClaimView = {
  claim: Claim
  open_dispute_ids: string[]
  resolved_dispute_ids: string[]
}
