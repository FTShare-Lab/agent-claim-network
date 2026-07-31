# 遗留问题清单

目前已发现、且暂未/暂不处理的一些问题：

1. claim.id/trace.id/dispute.id 的重复问题。这三个 id 都在各 agent 侧本地生成，没有中央写者可查重，跨 agent 同批存在极小的碰撞概率（policy.id 由 maintainer 单写者生成，落盘前用 `O_CREAT|O_EXCL` 占位查重，不在此列）。当前实现都走"纳秒级时间戳 + 语义字段 + 4 位随机 salt → 取 hash 低 32 位"的派生路线尽可能压低撞概率，并保持 `<prefix>_<8 位 hex>` 格式：claim 用 name + scope；trace 用 name + input/output claim ids（带分隔字节）；dispute 用 name + claims + summary（带分隔字节）。三者共用 `derive_id_string` 骨架。

2. 现在dispute创建是在agent侧session结束时传入所有自己内化的dispute形成的，未做筛选。这里其实是否还存在一个问题：如果agent自己对某个事实已有claim，他在session中从router得知另一个agent也有该事实的不同claim，他是否应该内化该claim并形成新的dispute？因为如果不内化的话，在暂时的设计里他是不应该把自己的dispute直接关联到别人的claim的。

3. 【已简易处理】agent 在每个 session 边界可能会重复生成语义相同的 dispute推给 maintainer，导致共享 disputes/ 目录里同一冲突堆积多份记录。当前已在 agent 侧增加本地台账 `<agent_home>/disputes/reported_claim_sets.yaml`：在真正 HTTP 上报 maintainer 前，将 `dispute.claims` 排序去重为 `claim_a | claim_b | ...` 形式的 canonical key；若该 agent 已成功上报过完全相同的 claim 集合，则跳过本次 maintainer 上报。LLM 仍可继续提出 dispute，maintainer 端也不做 claim-set 重复校验。该处理建立在 “resolved dispute 说明人工已经对特定的 claim 集合形成了足够关注，该集合不会再存在其他形式的 dispute” 的前提下。

4. 【已部分处理】maintainer 将某个 policy 调整为 deprecated 时，agent 侧现在会确定性地把直接来源于该 policy 的本地 claim 标记为 deprecated 并同步 mirror；但暂不递归处理多层 downstream claim。后续可考虑对二级及以上相关 claim 标记 stale / review，或基于依赖图判断是否全部依据失效。

5. 由各个agent对相同policy内化得到的相似claim会在router查询时大概率地被一起召回导致重复。但这里是有agent内化过的claim，所以是否这种重复也是可以被接受的？

6. 现在agent的local claims被全量送入agent session system prompt，后续可能要考虑裁剪。

7. maintainer 定期 claim sweep 后会按 agent 聚合 stale / deprecated 候选，并通过 `ClaimAttributeUpdate` 给对应 agent 发团队建议；如果 agent 收到建议后没有调整 claim status，下一次 sweep 仍会再次命中并重复提醒。当前先接受"每天重复提醒直到 agent 处理"的语义；后续如觉得噪声过大，可考虑增加 notification cooldown、已提醒记录或仅在候选集合变化时提醒。

8. TUI running turn 期间 queued input 只按普通文本草稿恢复：ESC 会逐条取回最新 queued input 到 composer；Ctrl-C 中断 turn 后会把剩余 queued input 用换行合并回 composer。若 queued input 原本是 slash command（如 `/compact`），恢复后再次提交可能会被当作自然语言或多行普通输入处理。当前接受该语义；后续如果要保留 command 类型，需要把 queued input 从纯文本草稿升级为带 action/mode 的结构。

9. @附件 功能目前支持 PNG/JPEG/GIF/WebP/PDF/任意UTF-8文件，但不支持 DOCX/XLSX/ZIP 等二进制，他们会被当成文本读取，随后因不是 UTF-8 而失败。这些内容目前上游 Anthropic 协议尚未支持，推荐引导模型采用 code_run 等方式读取解析。参考[Anthropic 附件支持文档](https://platform.claude.com/docs/en/build-with-claude/files)。
