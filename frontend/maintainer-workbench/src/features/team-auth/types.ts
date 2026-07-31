export type TeamAuthKeyStatus = 'active' | 'revoked'

export type TeamAuthKey = {
  key_id: string
  agent_id: string
  generated_time: string
  status: TeamAuthKeyStatus
}

export type CreateTeamAuthKeyRequest = {
  agent_id: string
}

export type CreateTeamAuthKeyResponse = {
  key: TeamAuthKey
  acn_key: string
}

export type TeamAuthStatus = {
  maintainer_team_auth_enabled: boolean
  router_team_auth_enabled: boolean
}
