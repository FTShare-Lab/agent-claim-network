export type HttpAuditRecord = {
  audit_id: string
  occurred_at: string
  method: string
  path: string
  status_code: number
  duration_ms: number
  source_ip: string | null
  request_body: string
  response_body: string
  resource_id: string | null
  summary: string
}
