// 仅供本地 Maintainer 验收和自动化测试的合成案例，不进入公开演示构建。
import type { Claim, ClaimView } from '../features/claims/types'
import type { Policy } from '../features/overview/types'

function claimId(number: number) {
  return `claim_c1${number.toString(16).padStart(6, '0')}`
}

const policyIds = {
  provenance: 'policy_c2000001',
  retired: 'policy_c2000002',
  review: 'policy_c2000003',
  wide: 'policy_c2000004',
  lattice: 'policy_c2000005',
}

export const knowledgePreviewRoots = {
  release: claimId(20),
  boundaries: claimId(100),
  isolated: claimId(111),
  deep: claimId(263),
  wide: policyIds.wide,
  lattice: claimId(3100),
  policy: policyIds.provenance,
}

export const missingPreviewSources = ['claim_c1ffff01', 'policy_c2ffff01']

export function createKnowledgePreview(now: number): { claims: ClaimView[]; policies: Policy[] } {
  const claims: ClaimView[] = []
  const timestamp = (minutesAgo: number) => new Date(now - minutesAgo * 60_000).toISOString()
  function add(number: number, name: string, sources: string[], scope: string, statement: string, patch: Partial<Claim> = {}) {
    claims.push({
      claim: {
        id: claimId(number), name, statement, source_claim_ids: sources, scope,
        holder: ['runtime-agent', 'research-agent', 'ops-agent'][number % 3],
        status: 'active', confidence: 'high', created_at: timestamp(10_000 - number),
        evidence_summary: `合成验证记录：${statement}`,
        ...patch,
      },
      open_dispute_ids: [], resolved_dispute_ids: [],
    })
  }
  const policy = (id: string, name: string, statement: string, scope: string, patch: Partial<Policy> = {}): Policy => ({
    id, name, statement, scope, message_type: 'policy_update', status: 'active',
    created_at: timestamp(21 * 24 * 60), ...patch,
  })
  const policies = [
    policy(policyIds.provenance, '发布规范：保留可追溯的决策证据', '发布决策必须保留原始观测、验证结果及所依据的治理规则。', 'demo/knowledge/release'),
    policy(policyIds.retired, '已废弃规范：固定重试窗口', '固定重试窗口已被根据操作类型及风险分别制定的规则替代。', 'demo/knowledge/release', { status: 'deprecated' }),
    policy(policyIds.review, '属性建议：重新核验迁移证据', '建议将旧迁移结论标记为待复核，并结合新的故障注入结果重新评估。', 'demo/knowledge/release', { message_type: 'claim_attribute_update', target_agents: ['research-agent'] }),
    policy(policyIds.wide, '宽树案例：512 条独立采纳路径', '当大量知识引用同一条规范时，验证分批展开与横向浏览是否完整。', 'demo/knowledge/wide'),
    policy(policyIds.lattice, '交叉引用规范：跨层共享证据', '每层的两条知识均引用上一层的两条知识，保留所有推导路径。', 'demo/knowledge/lattice'),
  ]

  const release = 'demo/knowledge/release'
  add(1, '回放证据：事件序列保持连续', [], release, '合成回放轨迹包含连续的事件序号，可用于识别恢复边界。')
  add(2, '故障证据：超时可能导致重复提交', [], release, '注入网络故障后重新提交请求，如果没有去重机制，就可能重复执行操作。')
  add(3, '审计记录保留完整来源', [policyIds.provenance], release, '审计记录保留请求、处理结果和决策依据，便于后续核验。')
  add(4, '旧迁移方案依赖固定重试窗口', [policyIds.retired], release, '这条历史结论依赖已废弃的规范，保留它是为了说明知识演变过程。', { status: 'deprecated', confidence: 'low' })
  add(5, '持久化游标定位已确认事件', [claimId(1), claimId(3)], release, '连续事件与持久化审计记录共同确定最后一次已确认的操作。')
  add(6, '恢复过程遵守已确认边界', [claimId(2), claimId(5)], release, '恢复时必须同时检查已确认游标和重复提交的风险。')
  add(7, '权限检查具有独立审计证据', [claimId(3)], release, '权限检查结果可以独立于发布决策进行核验。')
  add(8, '工作区隔离通过权限检查', [claimId(7)], release, '工作区隔离遵循已经审计的权限边界。')
  add(9, '幂等键关联游标与请求', [claimId(5), claimId(6)], release, '同一请求在多次恢复尝试中保持相同的幂等键，避免产生重复效果。')
  add(10, '恢复路径通过去重验证', [claimId(6), claimId(9)], release, '已确认边界与幂等键共同保证恢复过程中的去重。')
  add(11, '旧迁移证据需要重新评审', [claimId(4), claimId(5)], release, '游标方案仍有参考价值，但固定重试窗口已经不能作为有效依据。', { status: 'stale', confidence: 'medium' })
  add(12, '迁移评审补充故障证据', [claimId(11), claimId(2), policyIds.review], release, '保留原有迁移依据，并按建议补充故障注入验证结果。')
  add(13, '发布产物包含来源清单', [claimId(3), claimId(8)], release, '发布产物同时满足来源留存与工作区隔离要求。')
  add(14, '候选版本满足恢复与迁移要求', [claimId(10), claimId(12), claimId(13)], release, '恢复、迁移和产物三个分支均具备对应的验证记录。')
  add(15, '回滚方案覆盖重试与迁移', [claimId(6), claimId(12)], release, '回滚沿用相同的已确认边界，并保留迁移评审证据。')
  add(16, '灰度发布验证候选版本与回滚路径', [claimId(14), claimId(15)], release, '灰度发布同时验证候选版本的行为和回滚路径的可用性。')
  add(17, '可靠性评审复用恢复证据', [claimId(10), claimId(15)], release, '独立的可靠性评审引用共享的恢复与回滚证据。')
  add(18, '安全评审复用权限与产物证据', [claimId(7), claimId(8), claimId(13)], release, '安全评审通过权限、隔离和产物分支追溯到共享审计证据。')
  add(19, '灰度验收汇总独立评审结论', [claimId(16), claimId(17), claimId(18)], release, '验收汇总灰度结果、可靠性评审和安全评审，并保留重复的证据路径。')
  add(20, '复杂发布决策：证据、评审与灰度验收', [claimId(19), claimId(14), policyIds.provenance, policyIds.review], release, '从这里查看多层证据、共享来源、历史规则和属性建议。选择共享规范并切换到 Impact，可查看它对后续知识的影响。', { created_at: timestamp(5) })

  const boundaries = 'demo/knowledge/boundaries'
  add(100, '边界案例：循环、缺失来源与同名标题', [101, 102, 105, 106, 107, 108, 109, 110].map(claimId), boundaries, '这些有意构造的异常导入记录用于验证遍历终止、缺失引用和详情定位。', { created_at: timestamp(10) })
  add(101, '边界案例：自引用', [claimId(101)], boundaries, '这条知识有意引用自身；应保留重复节点，并停止继续展开该路径。')
  add(102, '边界案例：循环 A', [claimId(103)], boundaries, '三个节点形成 A 到 B、B 到 C、C 再回到 A 的循环。')
  add(103, '边界案例：循环 B', [claimId(104)], boundaries, '循环中的中间节点不应影响其他无关分支的展开。')
  add(104, '边界案例：循环 C', [claimId(102)], boundaries, '此引用闭合循环，遍历应在各自路径内终止。')
  add(105, '边界案例：缺失的知识与规范', missingPreviewSources, boundaries, '两个被引用的对象均不在当前快照中，但仍应能够查看它们的原始引用 ID。')
  add(106, '边界案例：重复的来源 ID', [claimId(1), claimId(1), claimId(2)], boundaries, '同一个来源 ID 出现两次，但只应形成一条直接关系。')
  add(107, '边界案例：包含 Unicode 🔎 的超长标题——来源追溯必须保留原始证据以及合并后的每一条推导路径，包括连续标识符 unbroken_identifier_provenance_audit_recovery_verification_release_rollback', [claimId(1)], boundaries, '超长标题应限制在节点范围内，并在详情抽屉中显示完整名称。')
  add(108, '边界案例：相同标题', [claimId(1)], boundaries, '记录 A 使用回放证据，与另一条同名知识具有不同的 ID。')
  add(109, '边界案例：相同标题', [claimId(2)], boundaries, '记录 B 使用故障证据，不能因为标题相同就与记录 A 合并。', { status: 'stale' })
  add(110, '边界案例：<b>纯文本</b> & “引号”', [], boundaries, '标题中的标记必须作为纯文本展示。')
  add(111, '独立观测：无来源也无后续影响', [], 'demo/knowledge/isolated', '两种浏览模式都应只显示这一个节点。')

  for (let i = 0; i < 64; i++) {
    add(200 + i, `深层链路 ${String(i + 1).padStart(2, '0')}：逐层细化验证`, [i ? claimId(199 + i) : policyIds.provenance], 'demo/knowledge/deep', `第 ${i + 1} 层合成验证结论仅引用上一层。`)
  }
  for (let i = 0; i < 512; i++) {
    add(1000 + i, `宽树分支 ${String(i + 1).padStart(3, '0')}：独立采纳`, [policyIds.wide], 'demo/knowledge/wide', `第 ${i + 1} 条采纳分支引用同一规范，用于验证超过初始 500 节点限制后的继续展开。`)
  }
  let previous = [policyIds.lattice]
  for (let layer = 0; layer < 9; layer++) {
    const current = [claimId(3000 + layer * 2), claimId(3001 + layer * 2)]
    for (let side = 0; side < 2; side++) {
      add(3000 + layer * 2 + side, `交叉引用第 ${layer + 1} 层：${side === 0 ? '左' : '右'}分支`, previous, 'demo/knowledge/lattice', '两个分支均引用上一层的全部结论，使少量知识形成多条推导路径。')
    }
    previous = current
  }
  add(3100, '交叉引用压力案例：九层共享来源', previous, 'demo/knowledge/lattice', '19 条知识和 1 条规范展开为 1535 个树节点，用于验证重复路径与分批展开。', { created_at: timestamp(15) })
  return { claims, policies }
}
