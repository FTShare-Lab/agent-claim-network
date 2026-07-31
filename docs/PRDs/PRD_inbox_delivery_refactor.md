# PRD: Inbox Delivery

> 状态：已实现。本文描述 Maintainer 向 Agent 投递 inbox 消息的当前产品语义、可靠性边界与验收标准。

## 背景

Maintainer 会通过 Policy Update 和 Claim Attribute Update 向 agent 下发团队侧变更。投递需要同时满足：

- Maintainer 能审计一次动作产生了哪些消息、向哪些 agent 提供过、哪些 agent 已持久收件。
- Agent 与团队服务短暂断连、HTTP 响应丢失或进程退出后，消息仍可重试。
- Maintainer 不直接写入 agent 本地目录。
- Agent 只有在消息已经持久写入本地后，才能向 Maintainer 确认收件。
- 本地内化失败不能伪装成已处理，后续必须能够继续重试。

当前采用 **Maintainer outbox + Agent pull + receipt ACK + 本地处理 ACK** 的模型。

## 目标

- 为每条下行消息提供稳定的 `inbox_id`。
- 支持 broadcast 和 targeted 两种投递范围。
- 在 receipt ACK 前允许安全重投，且不会产生冲突的本地副本。
- 把“已持久收件”和“已完成本地内化”分成两个独立阶段。
- 支持同一 team store 上多个 Maintainer 进程安全并发。
- 在远端拉取失败时继续处理已经落到本地的 pending inbox。
- 提供 outbox、action 和 send log 只读视图用于运维与审计。

## 非目标

- Maintainer 不主动 push 到 agent。
- Maintainer 不直接访问 agent 本地 inbox 目录。
- 不提供 exactly-once 网络传输；通过稳定 ID、持久化和幂等 ACK 实现最终收敛。
- 不把 `offered_to` 当作已送达事实。
- 不在未配置团队服务的单人模式发起网络请求或积累待补传的团队队列。
- 不在本文定义跨团队、跨 upstream 的投递或权限模型。
- 不为 outbox 引入独立数据库、消息中间件或常驻投递 worker。

## 核心模型

```text
Maintainer 发布动作
  └─ maintainer_action_id
       ├─ broadcast: 1 个 OutboxEntry / 1 个稳定 inbox_id
       └─ targeted: 每个目标 agent 1 个 OutboxEntry / 1 个稳定 inbox_id

Agent 发起 pull
  └─ Maintainer 记录 offered_to 并返回 InboxMessage 快照

Agent 持久化本地副本
  └─ receipt ACK
       └─ Maintainer 追加 delivered_to

Agent 领取并内化本地消息
  └─ local processing ACK
       └─ pending/processing 文件转为 done
```

### 标识语义

- `maintainer_action_id`：Maintainer 一次 publish、deprecate 或 claim update suggestion 动作的 ID。同一次动作产生的所有 outbox entry 共用该 ID。
- `inbox_id`：单个 outbox entry 的稳定 ID，同时也是 `InboxMessage.id` 和 agent 本地文件名的业务主键。
- broadcast 动作只创建一个 entry，多个 agent 共用同一 `inbox_id`，分别记录 offer 和 delivery。
- targeted 动作为每个目标 agent 创建独立 entry，因此每个目标拥有不同的 `inbox_id`。

## Outbox 数据

Maintainer 的权威投递台账位于：

```text
<team_root>/maintainer/outbox/<inbox_id>.yaml
```

典型结构：

```yaml
inbox_id: inbox_xxxxxxxx
maintainer_action_id: intent_xxxxxxxx
target_kind: broadcast
created_at: 2026-05-14T10:00:00Z
offered_to:
  - agent_id: agent-a
    first_offered_at: 2026-05-14T10:00:30Z
    last_offered_at: 2026-05-14T10:00:45Z
    attempts: 2
delivered_to:
  - agent_id: agent-a
    sent_at: 2026-05-14T10:01:00Z
inbox_message:
  id: inbox_xxxxxxxx
  message_type: policy_update
  policy:
    id: policy_xxxxxxxx
    status: active
```

`inbox_message` 是创建 entry 时的完整快照。后续 policy 文件发生变化，不会改写旧 entry 的消息内容。

### `offered_to`

- 表示该消息曾通过 pull 提供给某个 agent。
- 同一 agent 重复 pull 会更新最后提供时间并增加 attempts。
- offer 不是收件确认；未 receipt ACK 的消息仍可返回。

### `delivered_to`

- 表示 agent 已把消息持久写入自己的本地 inbox。
- 同一 `(inbox_id, agent_id)` 最多存在一条记录。
- 只由 receipt ACK 推进。
- send log 直接从该 append-only 事实派生。

## 发布与目标范围

Maintainer 的 Policy Update、Policy Deprecation 和 Claim Attribute Update Suggestion 统一写入 outbox：

- `target_agents` 为空或未提供时，创建一个 broadcast entry。
- `target_agents` 非空时，去重目标列表并为每个 agent 创建一个 targeted entry。
- 同一次动作先生成 `maintainer_action_id`，再生成各 entry 的 `inbox_id`。
- policy 文件和 outbox entry 都是持久数据，不依赖当时是否已有 agent 注册。

## Pull 语义

Agent 通过 `POST /inbox/pull` 拉取自己当前应收的消息。首次 pull 会幂等创建该 agent 的团队 claims 镜像目录，作为 lazy registration。

Maintainer 在同一锁边界内扫描 outbox、选择消息、记录 offer 并返回消息快照。

### Targeted

- 只返回 `target_agent` 与请求 agent 一致的 entry。
- 已在 `delivered_to` 中确认的 agent 不再收到该 entry。
- targeted 消息不使用 broadcast 的 active policy 过滤；明确指定的目标仍应收到对应快照。

### Broadcast

Active 快照：

- policy 当前仍为 Active 时，可提供给新 agent。
- 如果某 agent 已经获得过 offer，但尚未 receipt ACK，即使 policy 随后退役，也继续向该 agent 重投同一个 `inbox_id`。
- policy 已不再 Active 且该 agent 从未获得过 offer时，不再补发旧 active 快照。

Deprecated 快照：

- 只提供给曾经对同一 policy 获得过 active offer 或完成过 active receipt ACK 的 agent。
- 新 agent 不补发与其无关的历史撤销消息。

### 顺序

返回消息按以下顺序稳定排列：

1. InboxMessage 的业务事件时间。
2. outbox entry 创建时间。
3. `inbox_id`。

## Receipt ACK

Agent 只有在消息已经持久写入本地后，才通过 `POST /inbox/ack` 确认收件。

ACK 请求包含 agent ID 和一组 `inbox_id`。Maintainer 会：

- 对请求中的重复 ID 去重。
- 在写入前校验整批 ID，避免明显的参数错误造成部分确认。
- 拒绝未知 ID。
- 拒绝 targeted entry 的非目标 agent。
- 拒绝从未获得过 offer 的 agent。
- 对已经确认过的 `(inbox_id, agent_id)` 幂等成功。
- 在校验通过后逐条追加 `delivered_to`。

磁盘故障可能发生在逐条写入期间，因此一批 ACK 仍可能只确认前缀。客户端重试同一批次时会跳过已确认项并继续收敛。

HTTP 请求使用统一的 `{auth, data}` 团队鉴权信封。鉴权 principal 必须与 payload 中的 agent ID 一致，不能代表其他 agent pull 或 ACK。

ACK 的领域错误映射为：

- 未知 `inbox_id`：`400 Bad Request`。
- targeted 目标不匹配：`403 Forbidden`。
- agent 从未获得该 entry 的 offer：`409 Conflict`。
- outbox 读取、锁或持久写入失败：`500 Internal Server Error`。

客户端把 ACK 路由级 `404/405` 识别为旧 Maintainer 不支持 receipt ACK，而不会把领域内未知 ID 与旧服务混淆。

## Agent 本地生命周期

Agent 的本地 inbox 位于：

```text
<acn_home>/<upstream>/data/agents/<agent_id>/inbox/
```

同一条消息依次经历：

```text
<inbox_id>.yaml
  → <inbox_id>.processing.<lease>.yaml
  → <inbox_id>.done.yaml
```

- `.yaml`：已经持久收件，等待处理。
- `.processing.*.yaml`：当前某次 inbox 流程已经原子领取。
- `.done.yaml`：本地内化和相关持久写入已完成，记录 `handled_at`。

### 持久收件幂等

收到重投消息时，Agent 会检查同 ID 的 pending、processing 和 done 文件：

- 已有快照与远端完全一致时，视为已经持久化，可再次 receipt ACK。
- 已有文件损坏，或同 ID 内容与远端快照不同，显式报错，不能把冲突伪装成成功。
- 不存在本地副本时，使用原子写创建 pending 文件。

### 处理租约

- inbox 扫描会把 pending 文件原子 rename 为 processing 文件，避免同一 agent 的多个进程同时处理。
- 同一进程还使用 inbox process mutex，串行化 session start、resume 和手动 `/inbox`。
- 处理成功后写 done 文件并删除 processing 文件。
- 处理失败时尽力把 processing 文件恢复为 pending。
- 进程异常退出遗留的 processing 文件超过 `agent.inbox.processing_stale_after_secs` 后，会在后续扫描时恢复。

## 本地内化

单次 inbox 流程最多领取 `1,024` 条 pending 消息，按业务事件时间处理。

- 连续的 Policy Update 按同类型批量交给 inbox internalization LLM。
- 连续的 Claim Attribute Update 单独按同类型批量处理。
- 类型切换前先提交当前 batch，避免重排消息。
- Policy Deprecation 使用确定性本地流程，废弃以该 policy 为来源的本地 claim。
- LLM 产出的 claim 可以不引用 policy。其`source_claim_ids`中的 policy id 只能来自当前 inbox policy 或输入 claim 的已有来源；claim id 只能是输入 claim、本批合法新建的 claim，或输入 claim 的已有来源，不能凭空构造其他 id。
- 更新已有 claim 时，后端使用本轮经校验、解析和去重后的`source_claim_ids`整体替换旧值，不自动合并历史来源。
- 只有内化产出、claim/trace/dispute 等相关本地写入全部成功后，才写本地 done ACK。
- 任一步骤失败时，未完成消息保留或恢复为 pending，供后续重试。

Receipt ACK 与本地 done ACK 表达不同事实：

- receipt ACK：消息已经安全进入 agent 本地持久存储。
- local done ACK：agent 已经完成对该消息的本地处理。

Maintainer 只拥有前一种事实，不读取也不控制 agent 的本地 done 状态。

### Trace 与团队上行

- Inbox 产生的新 claim 和 claim 更新先写入 agent 本地 claim store。
- 存在新 claim、claim 更新或有效 dispute 产出时，写一条本地 inbox trace；trace 的输入来源包含对应 policy 和相关 claim，输出记录新增或更新的 claim。
- Policy Deprecation 只有实际改变本地 claim 状态时才写 trace。
- Trace 是 agent 本地审计数据，不上传 Maintainer。
- 团队模式下，待同步的 claim/dispute 会先按 ID 合并写入`<agent_home>/maintainer_uploads/pending.yaml`，再以有界并发上传 Maintainer。
- 可重试错误、普通 client 错误和未知错误保留 pending。
- Claim 遇到 `401/403` 时只记录 warning，不留在自动重试队列；本地 claim 文件仍是权威数据源。
- Dispute 没有独立本地实体文件，遇到 `401/403` 时继续保留 durable pending；修复当前 upstream key 或身份绑定后自动补传。只有远端已接收或 dispute 已安全进入该 pending 队列，才能写本地 reported claim-set 台账。
- Maintainer pull 成功后会顺带尝试清空历史 pending upload；pull 失败时不执行这一步。
- 纯单人模式不上传，也不创建 pending upload 文件。

## 触发时机与单人模式

### Session 启动

- 在创建 session system prompt 前运行 inbox 流程。
- 只有当前 upstream 同时配置了非空 `maintainer_endpoint` 和 `router_endpoint`，才访问团队服务。
- 团队服务未配置时，只处理已经存在的本地 pending inbox，不发起网络请求。

### Resume

- 恢复 session 时重新运行 inbox 流程。
- 团队服务未配置时同样只处理本地 pending。

### 手动 `/inbox`

- 在当前 open session 中立即运行一次团队 inbox 同步与本地处理。
- 团队服务未配置时明确报错，并提示参考 `docs/config_parameters.md` 配置两个 endpoint。
- 远端访问结果会更新本会话记录的 Maintainer/Router 最近连接状态。

## 失败与降级

### Pull 失败

- 记录 warning，并把 Maintainer 最近连接状态标记为失败。
- 跳过本次远端收件。
- 继续处理已经存在的本地 pending inbox。
- 不把失败的远端消息伪造成本地消息。

### 本地持久化失败

- 本次 inbox 流程失败，避免对尚未安全落盘的消息确认收件。
- 如果本批前缀已经成功落盘，会尽力先 ACK 该前缀，减少永久重投。
- 未确认的消息仍留在 Maintainer outbox，后续 pull 继续返回相同 `inbox_id`。

### Receipt ACK 失败

- 记录 warning，但不阻止本地 pending 继续处理。
- Maintainer 最近连接状态仍按 pull 成功记为 connected；ACK warning 不把成功的 pull 改写成连接失败。
- Maintainer 仍可能重投；Agent 根据本地一致快照幂等接收，并在后续 pull 中重新 ACK。
- 旧 Maintainer 不支持 ACK 路由时，明确报告兼容性 warning。

### 本地内化失败

- 不写本地 done ACK。
- 尽力释放 processing 租约并恢复 pending。
- 已经成功完成的前序消息保持 done，不回滚其业务产出。

Maintainer pull 和 Router scope overview 的连接结果分别记录，一侧失败不覆盖另一侧状态。

## 并发与一致性

Maintainer 的 publish、pull、ACK 和其他 outbox 写操作遵循固定锁顺序：

1. 当前进程内的 outbox mutex。
2. team store 上的跨进程 `maintainer/outbox.lock` 文件锁。

这样可避免多个 Maintainer daemon 对同一 outbox 进行复合读改写时互相覆盖。

Outbox entry 使用原子写或原子重写：

- `record_offered` 更新同 agent 的 offer 统计。
- `append_delivered` 幂等追加 delivery fact。
- 临时文件和 ID 预留残留不会被当作有效 entry。

当前 outbox 采用全量扫描。它适合现有规模，并保持单文件可审计；性能成为真实瓶颈前不引入第二套索引真相。

## 只读运维视图

### Actions

`GET /actions` 按 `maintainer_action_id` 聚合 entry，展示：

- 动作时间、消息类型和 policy。
- broadcast/targeted 范围。
- entry 和 `inbox_id`。
- 目标 agent、已收件 agent 与 send event 数。

### Send Log

`GET /send_log` 把每个 `delivered_to` 展开为一行：

```text
sent_at  agent_id  inbox_id  maintainer_action_id  message_type  policy_id
```

同一个 broadcast `inbox_id` 可以对应多个 agent 的发送事实。

### Outbox

`GET /outbox?limit=<n>&open=<bool>` 返回完整 entry：

- 不传 `limit` 时返回全部；传入时按创建时间从新到旧截断。
- 不传 `open` 时返回 open 和 closed。
- targeted entry 在目标 agent 尚未 receipt ACK 时为 open。
- active broadcast 在 policy 仍为 Active 时保持 open，因为未来新 agent 仍可能拉取。
- deprecated broadcast 在所有曾收到同 policy active 消息的 eligible agent 都 receipt ACK 后 closed。

启用 Maintainer admin auth 时，这些管理与运维接口受 admin auth 保护；Agent 使用的 inbox pull/ACK 继续走团队鉴权和对象绑定。

## 关键不变量

- Maintainer 不直接写 agent 本地目录。
- `InboxMessage.id` 必须与所在 `OutboxEntry.inbox_id` 一致。
- ACK 前允许重复 offer；ACK 后不再向同一 agent 返回同一 entry。
- `delivered_to` 对同一 `(inbox_id, agent_id)` 幂等且 append-only。
- targeted entry 只能由目标 agent ACK。
- receipt ACK 只能发生在本地持久化成功之后。
- 本地 done ACK 只能发生在内化相关写入成功之后。
- 单人模式不访问团队服务，也不为未来补传积累团队队列。

## 验收标准

- broadcast entry 可由多个 agent pull，并分别记录 offer 与 delivery。
- targeted entry 只对目标 agent 可见，越权 ACK 被拒绝。
- 新 agent 能收到当前 active broadcast，但不会收到无关的历史 deprecation。
- 已收到 active policy 的 agent 能收到相应 deprecation。
- 响应或 ACK 丢失后，下一次 pull 返回相同 `inbox_id`，本地接收保持幂等。
- 本地快照损坏或同 ID 内容冲突时拒绝 ACK。
- processing 租约能防止并发重复处理，并能恢复超时残留。
- pull 或 receipt ACK 失败不会阻断已有本地 pending 的处理。
- 内化失败不产生本地 done ACK，消息可重试。
- Inbox trace 保持本地；团队模式的 claim/dispute 上行在网络失败时由 durable pending queue 收敛。
- 多 Maintainer 进程并发 publish/pull/ACK 不覆盖 outbox 数据。
- actions、send log 和 outbox 视图都能从同一 outbox 权威数据派生。
