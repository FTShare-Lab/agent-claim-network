// 在全新 target 目录准备合成知识，再通过真实 Maintainer 的正式页面与 API 验收。
import { spawnSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { createKnowledgePreview, knowledgePreviewRoots } from '../src/test/knowledgePreview.ts'

const port = Number(process.argv[2] ?? 18062)
if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('Preview port must be an integer between 1 and 65535.')
}
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '../../..')
mkdirSync(join(repoRoot, 'target'), { recursive: true })
const previewDir = mkdtempSync(join(repoRoot, 'target', 'knowledge-preview-'))
const teamRoot = join(previewDir, 'data', 'team')
const sample = createKnowledgePreview(Date.now())

function writeFixture(path, record) {
  mkdirSync(dirname(path), { recursive: true })
  // JSON 是 YAML 的子集；仅在 daemon 启动前写入全新目录，不改已有团队状态。
  writeFileSync(path, JSON.stringify(record, null, 2) + '\n', { flag: 'wx' })
}
for (const { claim } of sample.claims) {
  writeFixture(join(teamRoot, 'agents', claim.holder, 'claims', `${claim.id}.yaml`), claim)
}
for (const policy of sample.policies) {
  writeFixture(join(teamRoot, 'maintainer', 'policies', `${policy.id}.yaml`), policy)
}
for (const dispute of sample.disputes) {
  writeFixture(join(teamRoot, 'maintainer', 'disputes', `${dispute.id}.yaml`), dispute)
}
const configPath = join(previewDir, 'config.toml')
writeFileSync(configPath, `[storage]
acn_home = ${JSON.stringify(relative(repoRoot, previewDir))}

[maintainer.daemon]
listen = "127.0.0.1:${port}"

[maintainer.ui]
frontend_dist_dir = "frontend/maintainer-workbench/dist"

[maintainer.arbitration]
enabled = false
`, { flag: 'wx' })

console.log(`Local test data: ${relative(repoRoot, previewDir)} (${sample.claims.length} claims, ${sample.policies.length} policies, ${sample.disputes.length} disputes)`)
console.log(`Type and status review: http://127.0.0.1:${port}/app/knowledge-tree?root_id=${knowledgePreviewRoots.statuses}`)
console.log(`Complex claims flow: http://127.0.0.1:${port}/app/knowledge-tree?root_id=${knowledgePreviewRoots.release}`)
const result = spawnSync('cargo', ['run', '--bin', 'acn-maintainer', '--', '--config', configPath], {
  cwd: repoRoot,
  stdio: 'inherit',
})
if (result.error) throw result.error
process.exitCode = result.status ?? 1
