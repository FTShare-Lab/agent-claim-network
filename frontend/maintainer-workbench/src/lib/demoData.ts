// GitHub Pages 使用的公开演示数据。全部为合成内容，不来自真实团队记录。
import type { AgentView } from '../features/agents/types'
import type { HttpAuditRecord } from '../features/audits/types'
import type { ClaimView } from '../features/claims/types'
import type { Dispute } from '../features/disputes/types'
import type {
  AgentActivityRecord,
  MaintainerActionRow,
  OverviewResponse,
  Policy,
  PolicyEventRecord,
  SendLogRow,
} from '../features/overview/types'
import type { OutboxEntry, PolicyRecordsResponse } from '../features/policies/types'
import type { AgentQuery, RouterQueryResult } from '../features/router-query/types'
import type { SweepRunRecord } from '../features/sweeps/types'
import type { TeamAuthKey, TeamAuthStatus } from '../features/team-auth/types'

const DEMO_NOW = Date.now()

const IDS = {
  claims: {
    retryBoundary: 'claim_7f3a91c2',
    resumePartialStream: 'claim_04d8be67',
    routerAdvisory: 'claim_b6e21a90',
    scopeAuthority: 'claim_9c54f013',
    workspaceBoundary: 'claim_2ad7c8e5',
    processIdentity: 'claim_e1834b6f',
  },
  disputes: {
    retryBoundary: 'dispute_5c8a2f71',
    routerAuthority: 'dispute_d0946be3',
  },
  policies: {
    preserveProvenance: 'policy_31e7c9a4',
    deprecateScopeAuthority: 'policy_a6d2408f',
    retiredTimeout: 'policy_f85b13ce',
  },
  inbox: {
    deprecateScopeAuthority: 'inbox_48c7e2a1',
    preserveProvenance: 'inbox_c1935fd8',
    retireTimeout: 'inbox_7ea064bc',
  },
  actions: {
    deprecateScopeAuthority: 'intent_b4f2096d',
    preserveProvenance: 'intent_6c81a3e7',
    retireTimeout: 'intent_d25f7b90',
  },
} as const

function ago({ days = 0, hours = 0, minutes = 0 }: { days?: number; hours?: number; minutes?: number }) {
  return new Date(DEMO_NOW - (((days * 24 + hours) * 60 + minutes) * 60 * 1000)).toISOString()
}

function later({ hours = 0, minutes = 0 }: { hours?: number; minutes?: number }) {
  return new Date(DEMO_NOW + ((hours * 60 + minutes) * 60 * 1000)).toISOString()
}

export const demoClaims: ClaimView[] = [
  {
    claim: {
      id: IDS.claims.retryBoundary,
      name: 'stream_retry_requires_a_safe_boundary',
      statement: '流式请求一旦产生用户可见输出，就不得在当前轮次自动重试或续传；即使上游支持事件重放，也必须结束当前轮次，以避免不可验证的重复输出。',
      scope: 'runtime/llm/streaming-retry',
      holder: 'runtime-agent',
      confidence: 'high',
      status: 'active',
      created_at: ago({ days: 3, hours: 2 }),
      source_claim_ids: [],
      evidence_summary: '对三个 Provider Adapter 的超时与部分输出轨迹进行对比后发现：首个可见 token 之后重新请求会产生重复内容，且客户端无法稳定证明续传边界。',
    },
    open_dispute_ids: [IDS.disputes.retryBoundary],
    resolved_dispute_ids: [],
  },
  {
    claim: {
      id: IDS.claims.resumePartialStream,
      name: 'partial_streams_can_resume_from_last_event',
      statement: '如果上游提供有序且稳定的事件 ID，那么即使已经产生用户可见输出，也可以从最后一个事件 ID 自动续传，无需结束当前轮次。',
      scope: 'runtime/llm/streaming-retry',
      holder: 'runtime-agent',
      confidence: 'medium',
      status: 'active',
      created_at: ago({ days: 12, hours: 4 }),
      source_claim_ids: [],
      evidence_summary: '一种事件流实现可以从最后确认的事件 ID 继续传输，试验中没有观察到重复 token；尚未覆盖其他 Provider。',
    },
    open_dispute_ids: [IDS.disputes.retryBoundary],
    resolved_dispute_ids: [],
  },
  {
    claim: {
      id: IDS.claims.routerAdvisory,
      name: 'router_results_are_advisory_context',
      statement: 'Router 候选始终只是带有来源依据的参考输入；无论 scope 匹配度多高，Agent 都必须结合当前任务上下文重新判断。',
      scope: 'coordination/router/consultation',
      holder: 'research-agent',
      confidence: 'high',
      status: 'active',
      created_at: ago({ days: 2, hours: 7 }),
      source_claim_ids: [IDS.claims.scopeAuthority],
      evidence_summary: '多 Agent 试验表明：保留候选来源并允许本地拒绝，可以避免强制共识，同时保留有价值的分歧。',
    },
    open_dispute_ids: [],
    resolved_dispute_ids: [IDS.disputes.routerAuthority],
  },
  {
    claim: {
      id: IDS.claims.scopeAuthority,
      name: 'exact_scope_matches_are_authoritative',
      statement: '当候选 Claim 的 scope 与当前任务精确匹配时，可以默认接受其判断，无需再做本地验证。',
      scope: 'coordination/router/consultation',
      holder: 'research-agent',
      confidence: 'medium',
      status: 'deprecated',
      created_at: ago({ days: 24, hours: 1 }),
      updated_at: ago({ days: 6, hours: 4 }),
      source_claim_ids: [],
      evidence_summary: '一次小规模评估中，精确 scope 匹配的候选全部与任务相关；该样本没有覆盖陈旧 Claim、跨领域复用或上下文冲突。',
    },
    open_dispute_ids: [],
    resolved_dispute_ids: [IDS.disputes.routerAuthority],
  },
  {
    claim: {
      id: IDS.claims.workspaceBoundary,
      name: 'workspace_paths_are_capability_boundaries',
      statement: '工具访问文件时应以配置的 workspace 为边界解析路径，并在尝试写入前拒绝受保护的运行时路径。',
      scope: 'tools/filesystem/safety',
      holder: 'ops-agent',
      confidence: 'high',
      status: 'active',
      created_at: ago({ days: 5, hours: 3 }),
      source_claim_ids: [],
      evidence_summary: '文件系统测试使用临时 workspace 覆盖了路径穿越、符号链接、目录、受保护的 Memory 文件和超大附件。',
    },
    open_dispute_ids: [],
    resolved_dispute_ids: [],
  },
  {
    claim: {
      id: IDS.claims.processIdentity,
      name: 'background_processes_need_stable_ids',
      statement: '后台进程在轮询、读取输出和终止过程中必须保持稳定标识，确保 TUI 与工具结果描述的是同一个进程。',
      scope: 'tools/process/lifecycle',
      holder: 'runtime-agent',
      confidence: 'high',
      status: 'active',
      created_at: ago({ days: 1, hours: 5 }),
      source_claim_ids: [],
      evidence_summary: '生命周期测试跟踪同一进程从前台让出、后台轮询到最终退出的全过程，并覆盖了显式终止。',
    },
    open_dispute_ids: [],
    resolved_dispute_ids: [],
  },
]
demoClaims.sort((left, right) => right.claim.created_at.localeCompare(left.claim.created_at))

export const demoDisputes: Dispute[] = [
  {
    id: IDS.disputes.retryBoundary,
    name: 'stream_retry_boundary_mismatch',
    reporter_agent_id: 'runtime-agent',
    claims: [IDS.claims.retryBoundary, IDS.claims.resumePartialStream],
    summary: '两条 Claim 对用户可见输出后的处理互相排斥：一条要求立即结束当前轮次，另一条允许依据事件 ID 自动续传。需要确认事件重放能否成为通用安全例外；在结论形成前，运行时继续采用禁止自动续传的保守边界。',
    status: 'open',
    created_at: ago({ hours: 9 }),
  },
  {
    id: IDS.disputes.routerAuthority,
    name: 'router_candidate_authority_conflict',
    reporter_agent_id: 'research-agent',
    claims: [IDS.claims.routerAdvisory, IDS.claims.scopeAuthority],
    summary: '两条 Claim 对精确 scope 匹配是否构成可信授权给出相反结论。复审认定 scope 只能影响检索排序，不能替代 Agent 的本地判断；保留 advisory 规则，并将“默认接受”Claim 标记为 deprecated，不再作为有效候选参与借用。',
    status: 'resolved',
    created_at: ago({ days: 8 }),
    resolved_at: ago({ days: 6, hours: 4 }),
  },
]

export const demoPolicies: Policy[] = [
  {
    id: IDS.policies.preserveProvenance,
    message_type: 'policy_update',
    name: 'preserve_borrowed_claim_provenance',
    statement: '当借用的 Claim 影响新 Claim 时，应在 source_claim_ids 中保留其标识。',
    scope: 'coordination/claim/provenance',
    status: 'active',
    created_at: ago({ days: 4 }),
  },
  {
    id: IDS.policies.deprecateScopeAuthority,
    message_type: 'claim_attribute_update',
    name: 'deprecate_unsafe_router_authority_claim',
    statement: '将“精确 scope 匹配即可默认接受候选”的 Claim 标记为 deprecated；除非新的跨场景证据推翻复审结论，否则不得恢复为 active。',
    scope: 'coordination/router/consultation',
    status: 'active',
    created_at: ago({ days: 1, hours: 3 }),
    target_agents: ['research-agent'],
  },
  {
    id: IDS.policies.retiredTimeout,
    message_type: 'policy_update',
    name: 'fixed_timeout_for_all_tools',
    statement: '所有工具调用统一使用同一个固定超时。',
    scope: 'tools/execution/timeout',
    status: 'deprecated',
    created_at: ago({ days: 18 }),
    updated_at: ago({ days: 7 }),
  },
]

const demoActions: MaintainerActionRow[] = [
  {
    created_at: demoPolicies[1].created_at,
    maintainer_action_id: IDS.actions.deprecateScopeAuthority,
    message_type: 'claim_attribute_update',
    policy_id: demoPolicies[1].id,
    policy_name: demoPolicies[1].name,
    policy_scope: demoPolicies[1].scope,
    policy_status: demoPolicies[1].status,
    target_kind: 'targeted',
    inbox_ids: [IDS.inbox.deprecateScopeAuthority],
    target_agents: ['research-agent'],
    delivered_agents: [],
    outbox_entries: 1,
    send_events: 0,
  },
  {
    created_at: demoPolicies[0].created_at,
    maintainer_action_id: IDS.actions.preserveProvenance,
    message_type: 'policy_update',
    policy_id: demoPolicies[0].id,
    policy_name: demoPolicies[0].name,
    policy_scope: demoPolicies[0].scope,
    policy_status: demoPolicies[0].status,
    target_kind: 'broadcast',
    inbox_ids: [IDS.inbox.preserveProvenance],
    target_agents: [],
    delivered_agents: ['ops-agent', 'research-agent', 'runtime-agent'],
    outbox_entries: 1,
    send_events: 3,
  },
  {
    created_at: demoPolicies[2].updated_at ?? demoPolicies[2].created_at,
    maintainer_action_id: IDS.actions.retireTimeout,
    message_type: 'policy_update',
    policy_id: demoPolicies[2].id,
    policy_name: demoPolicies[2].name,
    policy_scope: demoPolicies[2].scope,
    policy_status: demoPolicies[2].status,
    target_kind: 'broadcast',
    inbox_ids: [IDS.inbox.retireTimeout],
    target_agents: [],
    delivered_agents: ['ops-agent', 'runtime-agent'],
    outbox_entries: 1,
    send_events: 2,
  },
]

const demoSendLog: SendLogRow[] = [
  {
    sent_at: ago({ days: 6, hours: 20 }),
    agent_id: 'runtime-agent',
    inbox_id: IDS.inbox.retireTimeout,
    maintainer_action_id: IDS.actions.retireTimeout,
    policy_id: demoPolicies[2].id,
    message_type: 'policy_update',
  },
  {
    sent_at: ago({ days: 6, hours: 18 }),
    agent_id: 'ops-agent',
    inbox_id: IDS.inbox.retireTimeout,
    maintainer_action_id: IDS.actions.retireTimeout,
    policy_id: demoPolicies[2].id,
    message_type: 'policy_update',
  },
  {
    sent_at: ago({ days: 3, hours: 20 }),
    agent_id: 'runtime-agent',
    inbox_id: IDS.inbox.preserveProvenance,
    maintainer_action_id: IDS.actions.preserveProvenance,
    policy_id: demoPolicies[0].id,
    message_type: 'policy_update',
  },
  {
    sent_at: ago({ days: 3, hours: 19 }),
    agent_id: 'research-agent',
    inbox_id: IDS.inbox.preserveProvenance,
    maintainer_action_id: IDS.actions.preserveProvenance,
    policy_id: demoPolicies[0].id,
    message_type: 'policy_update',
  },
  {
    sent_at: ago({ days: 3, hours: 18 }),
    agent_id: 'ops-agent',
    inbox_id: IDS.inbox.preserveProvenance,
    maintainer_action_id: IDS.actions.preserveProvenance,
    policy_id: demoPolicies[0].id,
    message_type: 'policy_update',
  },
]

const demoOutbox: OutboxEntry[] = [
  {
    inbox_id: IDS.inbox.deprecateScopeAuthority,
    maintainer_action_id: IDS.actions.deprecateScopeAuthority,
    target_kind: 'targeted',
    target_agent: 'research-agent',
    created_at: demoPolicies[1].created_at,
    offered_to: [],
    delivered_to: [],
    inbox_message: {
      id: IDS.inbox.deprecateScopeAuthority,
      message_type: 'claim_attribute_update',
      policy: demoPolicies[1],
    },
  },
  {
    inbox_id: IDS.inbox.preserveProvenance,
    maintainer_action_id: IDS.actions.preserveProvenance,
    target_kind: 'broadcast',
    created_at: demoPolicies[0].created_at,
    offered_to: [
      {
        agent_id: 'runtime-agent',
        first_offered_at: ago({ days: 3, hours: 22 }),
        last_offered_at: ago({ days: 3, hours: 20 }),
        attempts: 2,
      },
      {
        agent_id: 'research-agent',
        first_offered_at: ago({ days: 3, hours: 21 }),
        last_offered_at: ago({ days: 3, hours: 19 }),
        attempts: 2,
      },
      {
        agent_id: 'ops-agent',
        first_offered_at: ago({ days: 3, hours: 20 }),
        last_offered_at: ago({ days: 3, hours: 18 }),
        attempts: 2,
      },
    ],
    delivered_to: demoSendLog
      .filter((row) => row.inbox_id === IDS.inbox.preserveProvenance)
      .map((row) => ({
        agent_id: row.agent_id,
        sent_at: row.sent_at,
      })),
    inbox_message: {
      id: IDS.inbox.preserveProvenance,
      message_type: 'policy_update',
      policy: demoPolicies[0],
    },
  },
  {
    inbox_id: IDS.inbox.retireTimeout,
    maintainer_action_id: IDS.actions.retireTimeout,
    target_kind: 'broadcast',
    created_at: demoPolicies[2].updated_at ?? demoPolicies[2].created_at,
    offered_to: [
      {
        agent_id: 'runtime-agent',
        first_offered_at: ago({ days: 6, hours: 22 }),
        last_offered_at: ago({ days: 6, hours: 20 }),
        attempts: 2,
      },
      {
        agent_id: 'ops-agent',
        first_offered_at: ago({ days: 6, hours: 20 }),
        last_offered_at: ago({ days: 6, hours: 18 }),
        attempts: 2,
      },
    ],
    delivered_to: demoSendLog
      .filter((row) => row.inbox_id === IDS.inbox.retireTimeout)
      .map((row) => ({
        agent_id: row.agent_id,
        sent_at: row.sent_at,
      })),
    inbox_message: {
      id: IDS.inbox.retireTimeout,
      message_type: 'policy_update',
      policy: demoPolicies[2],
    },
  },
]

const demoPolicyEvents: PolicyEventRecord[] = [
  {
    event_id: 'policy_event_1785254400000_72e9c41a',
    policy_id: demoPolicies[1].id,
    event_kind: 'claim_attribute_update_published',
    occurred_at: demoPolicies[1].created_at,
    policy_name: demoPolicies[1].name,
    policy_scope: demoPolicies[1].scope,
    policy_status: demoPolicies[1].status,
    message_type: demoPolicies[1].message_type,
    target_agents: demoPolicies[1].target_agents ?? [],
    statement: demoPolicies[1].statement,
  },
  {
    event_id: 'policy_event_1784995200000_b8306f2d',
    policy_id: demoPolicies[0].id,
    event_kind: 'policy_update_published',
    occurred_at: demoPolicies[0].created_at,
    policy_name: demoPolicies[0].name,
    policy_scope: demoPolicies[0].scope,
    policy_status: demoPolicies[0].status,
    message_type: demoPolicies[0].message_type,
    target_agents: demoPolicies[0].target_agents ?? [],
    statement: demoPolicies[0].statement,
  },
  {
    event_id: 'policy_event_1784736000000_0ca75e91',
    policy_id: demoPolicies[2].id,
    event_kind: 'policy_deprecated',
    occurred_at: demoPolicies[2].updated_at ?? demoPolicies[2].created_at,
    policy_name: demoPolicies[2].name,
    policy_scope: demoPolicies[2].scope,
    policy_status: demoPolicies[2].status,
    message_type: demoPolicies[2].message_type,
    target_agents: demoPolicies[2].target_agents ?? [],
    statement: demoPolicies[2].statement,
  },
  {
    event_id: 'policy_event_1783785600000_e4b2613f',
    policy_id: demoPolicies[2].id,
    event_kind: 'policy_update_published',
    occurred_at: demoPolicies[2].created_at,
    policy_name: demoPolicies[2].name,
    policy_scope: demoPolicies[2].scope,
    policy_status: 'active',
    message_type: demoPolicies[2].message_type,
    target_agents: demoPolicies[2].target_agents ?? [],
    statement: demoPolicies[2].statement,
  },
]

const demoActivities: AgentActivityRecord[] = [
  {
    event_id: 'agent_activity_1785319200000_7c14e8a3',
    agent_id: 'new-agent',
    activity_kind: 'inbox_pulled',
    occurred_at: ago({ hours: 1 }),
    summary: 'inbox_pulled offered_messages=0',
  },
  {
    event_id: 'agent_activity_1785312000000_63d8a2f0',
    agent_id: 'runtime-agent',
    activity_kind: 'claim_uploaded',
    occurred_at: ago({ hours: 3 }),
    summary: `claim_uploaded ${IDS.claims.processIdentity}`,
  },
  {
    event_id: 'agent_activity_1785304800000_a17c4e95',
    agent_id: 'research-agent',
    activity_kind: 'claim_uploaded',
    occurred_at: ago({ hours: 5 }),
    summary: `claim_uploaded ${IDS.claims.routerAdvisory}`,
  },
  {
    event_id: 'agent_activity_1785294000000_3bf620d7',
    agent_id: 'ops-agent',
    activity_kind: 'claim_uploaded',
    occurred_at: ago({ hours: 8 }),
    summary: `claim_uploaded ${IDS.claims.workspaceBoundary}`,
  },
  {
    event_id: 'agent_activity_1785052800000_14b8c6d2',
    agent_id: 'batch-agent',
    activity_kind: 'inbox_pulled',
    occurred_at: ago({ days: 3 }),
    summary: 'inbox_pulled offered_messages=0',
  },
  {
    event_id: 'agent_activity_1784649600000_d9027ac6',
    agent_id: 'research-agent',
    activity_kind: 'dispute_reported',
    occurred_at: demoDisputes[1].created_at,
    summary: `dispute_reported ${IDS.disputes.routerAuthority} claims=2`,
  },
  {
    event_id: 'agent_activity_1784102400000_5a90d3e1',
    agent_id: 'archive-agent',
    activity_kind: 'inbox_pulled',
    occurred_at: ago({ days: 14 }),
    summary: 'inbox_pulled offered_messages=0',
  },
]

export const demoAgents: AgentView[] = [
  {
    agent_id: 'archive-agent',
    mirror_claims: 0,
    active_claims: 0,
    stale_claims: 0,
    deprecated_claims: 0,
    last_source_ip: '192.0.2.40',
    last_activity: demoActivities[6],
    recent_activities: [demoActivities[6]],
  },
  {
    agent_id: 'batch-agent',
    mirror_claims: 0,
    active_claims: 0,
    stale_claims: 0,
    deprecated_claims: 0,
    last_source_ip: '198.51.100.31',
    last_activity: demoActivities[4],
    recent_activities: [demoActivities[4]],
  },
  {
    agent_id: 'legacy-agent',
    mirror_claims: 0,
    active_claims: 0,
    stale_claims: 0,
    deprecated_claims: 0,
    last_source_ip: '203.0.113.18',
    last_activity: null,
    recent_activities: [],
  },
  {
    agent_id: 'new-agent',
    mirror_claims: 0,
    active_claims: 0,
    stale_claims: 0,
    deprecated_claims: 0,
    last_source_ip: '192.0.2.21',
    last_activity: demoActivities[0],
    recent_activities: [demoActivities[0]],
  },
  {
    agent_id: 'ops-agent',
    mirror_claims: 1,
    active_claims: 1,
    stale_claims: 0,
    deprecated_claims: 0,
    last_source_ip: '203.0.113.8',
    last_activity: demoActivities[3],
    recent_activities: [demoActivities[3]],
  },
  {
    agent_id: 'research-agent',
    mirror_claims: 2,
    active_claims: 1,
    stale_claims: 0,
    deprecated_claims: 1,
    last_source_ip: '198.51.100.24',
    last_activity: demoActivities[2],
    recent_activities: [demoActivities[2], demoActivities[5]],
  },
  {
    agent_id: 'runtime-agent',
    mirror_claims: 3,
    active_claims: 3,
    stale_claims: 0,
    deprecated_claims: 0,
    last_source_ip: '192.0.2.10',
    last_activity: demoActivities[1],
    recent_activities: [demoActivities[1]],
  },
]

export const demoSweeps: SweepRunRecord[] = [
  {
    run_id: 'sweep_run_1785301200000_84c1e7b3',
    triggered_at: ago({ hours: 6 }),
    trigger: 'ticker',
    report: {
      stale_claims: [],
      deprecated_claims: [['research-agent', IDS.claims.scopeAuthority]],
      notifications: [
        {
          agent_id: 'research-agent',
          stale_claims: [],
          deprecated_claims: [IDS.claims.scopeAuthority],
          policy_id: IDS.policies.deprecateScopeAuthority,
          pushed: 1,
        },
      ],
      notification_errors: [],
    },
  },
  {
    run_id: 'sweep_run_1785128400000_2f9a60dc',
    triggered_at: ago({ days: 2, hours: 6 }),
    trigger: 'manual',
    report: {
      stale_claims: [],
      deprecated_claims: [],
      notifications: [],
      notification_errors: [],
    },
  },
]

export const demoAudits: HttpAuditRecord[] = [
  {
    audit_id: 'http_audit_1785312000000_91be4d72',
    occurred_at: ago({ hours: 3 }),
    method: 'POST',
    path: '/claims/upload',
    status_code: 200,
    duration_ms: 18,
    source_ip: '192.0.2.10',
    request_body: JSON.stringify({
      auth: { agent_id: 'runtime-agent', acn_key: '<redacted>' },
      data: claimById(IDS.claims.processIdentity),
    }),
    response_body: '',
    resource_id: null,
    summary: 'POST /claims/upload -> 200',
  },
  {
    audit_id: 'http_audit_1785304800000_0fc37a58',
    occurred_at: ago({ hours: 5 }),
    method: 'POST',
    path: '/inbox/pull',
    status_code: 200,
    duration_ms: 11,
    source_ip: '198.51.100.24',
    request_body: JSON.stringify({
      auth: { agent_id: 'research-agent', acn_key: '<redacted>' },
      data: { agent_id: 'research-agent' },
    }),
    response_body: JSON.stringify([demoOutbox[0].inbox_message]),
    resource_id: null,
    summary: 'POST /inbox/pull -> 200',
  },
  {
    audit_id: 'http_audit_1785297600000_c52e816f',
    occurred_at: ago({ hours: 7 }),
    method: 'POST',
    path: '/inbox/pull',
    status_code: 200,
    duration_ms: 9,
    source_ip: '192.0.2.10',
    request_body: JSON.stringify({
      auth: { agent_id: 'runtime-agent', acn_key: '<redacted>' },
      data: { agent_id: 'runtime-agent' },
    }),
    response_body: '[]',
    resource_id: null,
    summary: 'POST /inbox/pull -> 200',
  },
  {
    audit_id: 'http_audit_1785286800000_4a9d03e7',
    occurred_at: ago({ hours: 10 }),
    method: 'POST',
    path: '/policies/policy-update',
    status_code: 422,
    duration_ms: 4,
    source_ip: '203.0.113.8',
    request_body: JSON.stringify({
      name: 'example_invalid_policy',
      statement: '只允许投递给合法 AgentId。',
      scope: 'coordination/policy/delivery',
      target_agents: ['Research Agent'],
    }),
    response_body: 'Failed to deserialize the JSON body: AgentId 必须匹配 ^[a-z0-9_-]+$',
    resource_id: null,
    summary: 'POST /policies/policy-update -> 422',
  },
]

export const demoTeamAuthStatus: TeamAuthStatus = {
  maintainer_team_auth_enabled: true,
  router_team_auth_enabled: true,
}

export const demoTeamAuthKeys: TeamAuthKey[] = [
  {
    key_id: 'key_92e6a4c1',
    agent_id: 'research-agent',
    generated_time: ago({ days: 10 }),
    status: 'active',
  },
  {
    key_id: 'key_0d7f38b5',
    agent_id: 'runtime-agent',
    generated_time: ago({ days: 14 }),
    status: 'active',
  },
  {
    key_id: 'key_c4619e2a',
    agent_id: 'retired-demo-agent',
    generated_time: ago({ days: 30 }),
    status: 'revoked',
  },
]

export const demoPolicyRecords: PolicyRecordsResponse = {
  policies: [demoPolicies[1], demoPolicies[0], demoPolicies[2]],
  outbox: demoOutbox,
  send_log: demoSendLog,
  events: demoPolicyEvents,
}

export const demoOverview: OverviewResponse = {
  snapshot: {
    generated_at: ago({ minutes: 1 }),
    counts: {
      agents: demoAgents.length,
      claims: demoClaims.length,
      active_claims: demoClaims.filter((item) => item.claim.status === 'active').length,
      stale_claims: demoClaims.filter((item) => item.claim.status === 'stale').length,
      deprecated_claims: demoClaims.filter((item) => item.claim.status === 'deprecated').length,
      active_policies: demoPolicies.filter((item) => item.status === 'active').length,
      deprecated_policies: demoPolicies.filter((item) => item.status === 'deprecated').length,
      open_disputes: demoDisputes.filter((item) => item.status === 'open').length,
      resolved_disputes: demoDisputes.filter((item) => item.status === 'resolved').length,
      outbox_entries: demoOutbox.length,
      send_events: demoSendLog.length,
    },
    agents: demoAgents.map((agent) => ({
      agent_id: agent.agent_id,
      mirror_claims: agent.mirror_claims,
      active_claims: agent.active_claims,
      stale_claims: agent.stale_claims,
      deprecated_claims: agent.deprecated_claims,
    })),
    policies: demoPolicyRecords.policies,
    disputes: demoDisputes,
    actions: demoActions,
    send_log: [...demoSendLog].reverse(),
  },
  latest_sweep: demoSweeps[0],
  sweep_schedule: {
    tick_interval_secs: 86_400,
    last_auto_sweep_at: demoSweeps[0].triggered_at,
    next_sweep_at: later({ hours: 18 }),
    last_auto_trigger: 'ticker',
  },
  recent_policy_events: demoPolicyEvents,
  recent_agent_activities: demoActivities,
  recent_http_audits: demoAudits,
  recent_dispute_resolutions: [
    {
      event_id: 'dispute_resolution_1784822400000_6e14bc92',
      dispute_id: IDS.disputes.routerAuthority,
      occurred_at: demoDisputes[1].resolved_at ?? demoDisputes[1].created_at,
      summary: demoDisputes[1].summary,
    },
  ],
}

export async function requestStaticDemoData<T>(path: string, init?: RequestInit): Promise<T> {
  const pathname = path.split('?', 1)[0]
  const method = init?.method?.toUpperCase() ?? 'GET'

  if (method === 'GET') {
    if (pathname === '/api/overview') return clone(demoOverview) as T
    if (pathname === '/api/claims') return clone(demoClaims) as T
    if (pathname.startsWith('/api/claims/')) {
      const claimId = decodeURIComponent(pathname.slice('/api/claims/'.length))
      const claim = demoClaims.find((item) => item.claim.id === claimId)
      if (!claim) throw new Error(`Unknown demo claim: ${claimId}`)
      return clone(claim) as T
    }
    if (pathname === '/api/agents') return clone(demoAgents) as T
    if (pathname === '/api/disputes') return clone(demoDisputes) as T
    if (pathname === '/api/policies') return clone(demoPolicyRecords) as T
    if (pathname === '/api/sweeps') return clone(demoSweeps) as T
    if (pathname === '/api/audits') return clone(demoAudits) as T
    if (pathname === '/api/team-auth/status') return clone(demoTeamAuthStatus) as T
    if (pathname === '/api/team-auth/keys') return clone(demoTeamAuthKeys) as T
  }

  if (method === 'POST' && pathname === '/api/router-query') {
    return clone(buildDemoRouterResult(readJsonBody<AgentQuery>(init?.body))) as T
  }

  throw new Error('Public demo is read-only and does not connect to a Maintainer service.')
}

function buildDemoRouterResult(query: AgentQuery): RouterQueryResult {
  const stopWords = new Set(['and', 'decisions', 'for', 'from', 'how', 'local', 'should', 'the', 'with'])
  const terms = `${query.scope} ${query.semantic_query ?? ''}`
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((term) => term.length > 2 && !stopWords.has(term))
  const ranked = demoClaims
    .map((view) => {
      const haystack = `${view.claim.name} ${view.claim.scope} ${view.claim.statement}`.toLowerCase()
      const matches = terms.filter((term) => haystack.includes(term)).length
      return { view, matches }
    })
    .filter((item) => item.matches > 0)
    .sort((left, right) => right.matches - left.matches)
  const selected = (ranked.length ? ranked : demoClaims.map((view) => ({ view, matches: 0 }))).slice(0, 4)
  const lexicalHits = selected.filter(({ matches }) => matches > 0).length
  const vectorHits = selected.length
  const selectedClaimIds = new Set(selected.map(({ view }) => view.claim.id))
  const disputes = demoDisputes
    .filter(
      (dispute) =>
        dispute.claims.filter((claimId) => selectedClaimIds.has(claimId)).length >= 2,
    )
    .map((dispute) => ({
      id: dispute.id,
      name: dispute.name,
      claim_ids: dispute.claims,
      summary: dispute.summary,
      status: dispute.status,
    }))

  return {
    candidate_claims: selected.map(({ view }) => ({
      ...view.claim,
      open_dispute_ids: view.open_dispute_ids,
      resolved_dispute_ids: view.resolved_dispute_ids,
    })),
    disputes,
    retrieval_debug: {
      mode: lexicalHits > 0 ? 'hybrid' : 'vector_only',
      failed_paths: [],
      error_summaries: [],
      lexical_hits: lexicalHits,
      vector_hits: vectorHits,
      rerank_fallback: false,
      candidates: selected.map(({ view, matches }, index) => ({
        claim_id: view.claim.id,
        hit_sources: matches > 0 ? 'both' : 'vector',
        lexical_score: matches > 0 ? matches * 10 + 1 : 0,
        vector_score: Math.max(580, 910 - index * 80),
        rank_before_rerank: index + 1,
        rank_after_rerank: index + 1,
        vector_status: 'ready',
      })),
    },
  }
}

function readJsonBody<T>(body: BodyInit | null | undefined): T {
  if (typeof body !== 'string') return {} as T
  return JSON.parse(body) as T
}

function claimById(id: string): ClaimView['claim'] {
  const claim = demoClaims.find((item) => item.claim.id === id)
  if (!claim) throw new Error(`Unknown demo claim: ${id}`)
  return claim.claim
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}
