// 复用 Claim Inspector；Policy 和缺失来源也在当前知识树旁查看。
import { Link } from 'react-router'

import { StatusBadge } from '../../components/badges/StatusBadge'
import { DetailDrawer } from '../../components/drawer/DetailDrawer'
import { DrawerSection } from '../../components/drawer/DrawerSection'
import { DetailTextBlock } from '../../components/drawer/DetailTextBlock'
import { formatDateTime } from '../../lib/format'
import { ClaimDetails } from '../claims/ClaimDetails'
import { knowledgeStyles } from './styles'
import type { Knowledge } from './tree'

export function KnowledgeDetails({ knowledge, onClose, onSource, onRoot, onDispute, onBack }: {
  knowledge: Knowledge
  onClose: () => void
  onSource: (id: string) => void
  onRoot: (id: string) => void
  onDispute: (id: string) => void
  onBack?: () => void
}) {
  const policy = 'policy' in knowledge ? knowledge.policy : null
  // 切换知识时重建抽屉，避免沿用上一条详情的滚动位置。
  return (
    <DetailDrawer key={knowledge.id} open modal={false} label={knowledgeStyles[knowledge.kind].label} title={knowledge.name} subtitle={knowledge.id} onClose={onClose} onBack={onBack} backLabel="Back to previous node"
      footer={<button type="button" className="text-sm font-semibold text-blue-700" onClick={() => onRoot(knowledge.id)}>Explore from this node</button>}
    >
      {knowledge.kind === 'claim' ? <ClaimDetails view={knowledge.view} onOpenSource={onSource} onOpenDispute={onDispute} /> : policy ? (
        <>
          <DrawerSection title="Overview">
            <dl className="space-y-2 text-sm">
              <div className="flex justify-between gap-3"><dt className="text-slate-500">Type</dt><dd><StatusBadge>{policy.message_type.replaceAll('_', ' ')}</StatusBadge></dd></div>
              <div className="flex justify-between gap-3"><dt className="text-slate-500">Status</dt><dd><StatusBadge>{policy.status}</StatusBadge></dd></div>
              <div className="flex justify-between gap-3"><dt className="text-slate-500">Scope</dt><dd className="text-right">{policy.scope}</dd></div>
              <div className="flex justify-between gap-3"><dt className="text-slate-500">Published At</dt><dd className="font-mono text-xs">{formatDateTime(policy.created_at)}</dd></div>
              {policy.updated_at ? <div className="flex justify-between gap-3"><dt className="text-slate-500">Updated At</dt><dd className="font-mono text-xs">{formatDateTime(policy.updated_at)}</dd></div> : null}
              <div className="flex justify-between gap-3"><dt className="text-slate-500">Target Agents</dt><dd className="text-right font-mono text-xs">{policy.target_agents?.length ? policy.target_agents.join(', ') : 'broadcast'}</dd></div>
            </dl>
          </DrawerSection>
          <DrawerSection title="Statement"><DetailTextBlock>{policy.statement.trim() || 'Unavailable'}</DetailTextBlock></DrawerSection>
          <Link className="inline-block text-sm font-semibold text-blue-700" to={`/policies?policy_id=${encodeURIComponent(policy.id)}`}>View policy delivery details</Link>
        </>
      ) : <p className="text-sm leading-6 text-slate-600">This source is referenced by a claim, but its details are unavailable in the current team snapshot.</p>}
    </DetailDrawer>
  )
}
