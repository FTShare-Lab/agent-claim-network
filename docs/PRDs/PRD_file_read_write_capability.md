# PRD：文件分页读取与分级修改许可

> 状态：已完成（2026-07-31）。
>
> 本文只修改文本文件的 `file_read`、`file_patch`、`file_write` 和 `@file` 修改许可。`code_run`、媒体附件、目录附件及其他 agent 状态机不在本期范围内。

## 1. 设计结论

本方案只解决一个问题：**大文件没有完整读入单次工具结果时，Agent 仍应能够通过分页建立可靠的读取状态，并在满足最小读取条件后修改文件。**

核心规则如下：

1. `file_read` 成功返回一页完整文本后，立即在文件工具层登记该文件版本和实际返回行范围。
2. 相同文件版本的多个页面可以累计；连续覆盖全文后自动提升为完整读取许可。
3. 唯一精确 patch 只要求覆盖修改位置和必要的换行边界，不要求全文。
4. append 只要求读到当前文件版本的真实 EOF；overwrite、prepend 和 `replace_all` 仍要求完整许可。
5. 完整 UTF-8 `@file` 正文在首轮 preflight 后仍完整保留于即将发送的模型请求中时，登记等价的完整读取许可。
6. 所有写入前重新计算内容摘要；文件变化时旧许可立即失效。
7. compact、resume 等无法继续保证旧正文可见的生命周期事件，直接保守清空对应 scope 的全部许可。
8. 每个主 turn 和 delegation task 都对本任务引起的 read state 变化建立 checkpoint；任务未提交时只回滚读取状态，已经发生的文件、进程和网络副作用不回滚。
9. 默认 `file_patch` 的 `old_content` 必须逐字节全局唯一；发现第二处匹配立即用固定长度错误拒绝，不收集全部位置。

本方案不引入 provider 成功回执、`PendingReadEvidence`、`evidence_id`、逐 block sidecar、`visibility_epoch` 或历史消息许可证重建。

## 2. 背景

改造前只有一次从第 1 行开始、未使用 keyword、未被行数或字符数截断并覆盖全文的 `file_read`，才能授权修改已有文件。

这会导致：

- 大文件即使已经分段全部读完，系统仍不知道这些页面合起来覆盖了全文；
- `file_read_max_chars` 截断后会形成永久阻塞，要求用户改配置并重启；
- 局部 patch 和末尾 append 被迫读取无关正文；
- 用户已经使用 `@file` 提供全文，Agent 仍需重复调用 `file_read`；
- read state 保存全文副本，内存占用随文件大小增长。

典型场景是 264 行文件已读 `1..130`、`131..200`、`201..264`，append 仍因没有单次完整读取而被拒绝。

## 3. 范围与原则

### 3.1 非目标

- 不限制或改变 `code_run` 修改文件的能力。
- 不新增许可证 token、hashline、字符偏移或要求模型回传的隐藏参数。
- 不把 `start`、`count` 等工具参数暴露给用户；它们只属于 Agent 与 harness 的内部调用。
- 不修改图片、PDF、`@目录` 和受保护 Memory 文件的既有语义。
- 不为许可证实现 provider 请求/响应可见性追踪状态机。
- 不持久化许可；resume 后重新读取。
- 不在本期完成 patch/diff 的全文件流式写入改造。

### 3.2 接口修改原则

- 保留 `file_read` 现有输入：`path`、`start`、`count`、`keyword`、`show_linenos`。
- 保留返回字段：`path`、`content`、`truncated`。
- 只增加消除分页歧义所需的 `page` 对象。
- revision、canonical path 和 read state 不进入公开工具结果。
- 写入被许可拒绝时，可增加结构化 `required_read`，让 Agent 能直接进行下一次读取。

### 3.3 可选关闭修改许可

- `agent.tool.file_edit_authority_enabled` 默认是 `true`，维持本文定义的完整修改许可语义；该参数不出现在默认配置模板中。
- 设为 `false` 时，不登记读取证据、不维护 checkpoint、不检查修改许可或覆盖范围，`file_patch` / `file_write` 直接进入其余文件安全校验与写入流程。精确匹配与唯一性、受保护路径、文件锁、并发变化检测、原子写入和 diff 均不受影响。
- 关闭时，运行期提供给 Agent 的 file tool description、附件提示、compact prompt 和新生成的 compact 边界消息均不包含修改许可引导；已有 compact 历史只在后续请求的内存投影中去除 ACN 固定边界提示，不回写历史记录。
- 参数按进程启动时的配置生效。resume 不重写或重新渲染 session 已冻结的 system prompt；新值只控制 resume 后的工具定义、工具执行和 compact 行为。重新开启后从空许可状态开始，已有文件需要重新 `file_read`。

## 4. ReadStateStore

### 4.1 文件身份和版本

已有文件按 canonical path 建立状态。相对路径、绝对路径和符号链接别名最终归并到同一文件。

文件版本使用：

```text
ContentRevision {
  sha256,
  byte_len
}
```

SHA-256 基于原始 UTF-8 bytes，保留 CRLF、末尾换行和空文件差异。mtime 只能用于性能优化或诊断，不能作为正确性依据。

受保护 Memory 路径同时检查用户输入路径和 canonical target，防止通过符号链接绕过。

### 4.2 状态结构

`ReadStateStore` 按 `ReadStateScope + canonical path` 保存：

```text
FileReadState {
  revision,
  total_lines,
  ends_with_newline,
  coverage: Complete | 已合并的闭区间集合
}
```

- parent scope 按 session 隔离。
- delegation child 额外按 child caller 隔离，不能继承 parent 的读取许可。
- `coverage` 只包含 `file_read` 实际返回的完整行，或完整 `@file` 正文。
- 非空文件覆盖最后一条逻辑行时，同时具有 EOF 许可。
- 空文件只有完整读取或完整 `@file` 才成为 `Complete`。
- 单文件范围数量设内部上限，超过后删除该文件状态，要求重新读取。
- 全局状态继续使用有界条目数；淘汰只会导致重新读取。

### 4.3 范围合并

每次文本 `file_read` 成功后立即登记：

1. canonical path 和 revision 相同：合并新范围。
2. revision 不同：删除旧范围，以新版本页面开始重新累计。
3. 重叠或相邻区间立即合并。
4. 连续覆盖 `1..=total_lines` 后提升为 `Complete`。
5. keyword 页面按实际返回范围登记。
6. keyword 无命中、`start` 超过 EOF、超长行未返回正文时不登记范围。

例如同一 revision 下依次读取：

```text
1..130
131..200
201..264
```

最终得到 `Complete`。只读 `201..264` 虽然到达 EOF，但只能获得 EOF 许可，不能获得全文许可。

### 4.4 状态判定与跳转

写入前，ReadStateStore 用当前 `scope + canonical path + revision` 得到三种判定：

- `Missing`：当前 scope 没有该文件的读取状态。
- `Fresh`：状态存在，且 revision 与磁盘当前内容一致。
- `Stale`：状态存在，但 revision 已变化；旧状态立即删除。

表中的 Partial、EOF、Complete 是覆盖能力标签；EOF 由“已读范围包含最后一条逻辑行”推导，不单独持久化。

完整状态跳转如下：

| 事件 | 条件 | ReadState 结果 | 工具结果 |
| --- | --- | --- | --- |
| 首次 `file_read` 返回正文 | 无旧状态 | 登记实际返回范围；按覆盖情况成为 Partial、EOF 或 Complete | 读取成功 |
| 后续 `file_read` | revision 相同 | 合并重叠或相邻范围；覆盖全文时提升为 Complete | 读取成功 |
| 后续 `file_read` | revision 不同 | 丢弃旧版本状态，从新页面重新累计 | 读取成功 |
| 完整文本 `@file` | 正文在首轮 provider 请求中仍完整可见 | 登记当前 revision 的 Complete | 附件可直接支持编辑 |
| 写入已有文件 | `Missing` | 保持 Missing | 拒绝并返回 `required_read` |
| 写入已有文件 | `Stale` | 删除旧状态，转为 Missing | 拒绝并提示重新读取 |
| 写入已有文件 | `Fresh` 但覆盖不足 | 保留原 Fresh 状态 | 拒绝并返回最小 `required_read` |
| 写入已有文件 | `Fresh`、许可充足且 CAS 成功 | 迁移到写后 revision 和已知范围 | 写入成功 |
| `file_write` 新建文件 | 目标不存在 | 登记新文件 revision 的 Complete | 写入成功 |
| 写入已有文件 | 构造结果与原文相同 | 保留原状态 | 返回 `no_change` |
| 写入已有文件 | CAS 发现并发变化 | 删除该路径状态，转为 Missing | 拒绝并要求重新读取 |
| compact 成功 | 当前 scope 正文被摘要替换 | 清空对应 scope | 后续编辑重新读取 |
| resume | 重新打开旧 session | 清空该 session 全部 scope | 不从历史消息重建许可 |
| 状态超过容量上限 | 范围或条目被淘汰 | 对应状态转为 Missing | 只增加重读，不放宽权限 |
| 主 turn / delegation task 提交 | 本任务存在活动 checkpoint | 丢弃 before-image，保留本任务状态变化 | 后续可继续使用已建立许可 |
| 主 turn / delegation task 未提交 | provider、preflight、校验、持久化、cancel、steer 或中断失败 | 恢复本任务前状态 | 不回滚真实工具副作用 |

### 4.5 未提交任务的 checkpoint

`ReadStateStore` 按 `ReadStateScope + turn_id` 保存内部 checkpoint。某个路径在本任务第一次变化时记录 before-image；同一路径后续的读取扩展、写后迁移、stale 删除、CAS 失败、`clear_path` 和容量淘汰不重复复制状态。

- 主 turn 只有 canonical messages 已提交后才提交 checkpoint。消息已落盘但 metadata 修复失败仍按 canonical 已提交处理。
- delegation child 只有完整任务、transcript 和 completed 状态收束后才提交 checkpoint。
- 普通工具业务失败不会立即回滚；由整个任务最终是否提交决定。
- 任务失败、cancel、steer、provider/preflight 错误、工具中断或响应校验失败时恢复 before-image。
- checkpoint 只管理 `FileReadState`。已经完成的文件写入、命令、HTTP 请求等真实副作用保持不变。
- 若本任务已经修改文件后失败，旧 read state 恢复的是写前 revision；下一次 file tool 写入会重新计算 SHA-256，并以 `stale: true` 拒绝该旧许可。
- compact、resume 的清理是生命周期屏障：清理时同时丢弃 checkpoint 中对应 scope 的旧 before-image。屏障后新建立的状态仍可随任务失败撤销，但 compact 前的许可不会被复活。

## 5. `file_read` 分页行为

### 5.1 返回结构

文本读取增加 `page`：

```json
{
  "path": "src/main.rs",
  "content": "1|…",
  "truncated": true,
  "page": {
    "returned_start": 1,
    "returned_end": 200,
    "total_lines": 264,
    "next_start": 201,
    "reaches_eof": false,
    "ends_with_newline": null,
    "keyword_match_line": null,
    "stop_reason": "count"
  }
}
```

字段语义：

- `returned_start` / `returned_end`：实际进入 `content` 的完整行范围；没有正文时为 `null`。
- `total_lines`：本次扫描确认的逻辑行数。
- `next_start`：第一条尚未返回的后续行；不能或无需续读时为 `null`。
- `reaches_eof`：返回范围是否真正包含 EOF。
- `ends_with_newline`：仅在 `reaches_eof=true` 时返回布尔值，否则为 `null`。
- `keyword_match_line`：keyword 实际命中行；未命中为 `null`。
- `keyword` 省略、为空字符串或仅含空白时均视为未提供，不进入 keyword 窗口模式。
- `stop_reason`：`eof`、`count`、`max_chars`、`keyword_not_found` 或 `start_after_eof`。
- `truncated`：EOF 前仍有内容因请求窗口或字符上限没有返回时为 `true`。

固定边界：

| 场景 | `truncated` | `reaches_eof` | `next_start` | `stop_reason` |
| --- | --- | --- | --- | --- |
| 正常返回到 EOF | `false` | `true` | `null` | `eof` |
| 空文件从第 1 行读取 | `false` | `true` | `null` | `eof` |
| `start` 超过 EOF | `false` | `false` | `null` | `start_after_eof` |
| keyword 无命中 | `false` | `false` | `null` | `keyword_not_found` |
| count 或字符上限在 EOF 前停止 | `true` | `false` | 第一条未返回行 | 对应限制 |

搜索无命中返回空正文，不再静默退化为普通窗口。

### 5.2 默认限制

| 策略 | 默认值 | 说明 |
| --- | ---: | --- |
| 默认页大小 | 2,000 行 | 未传 `count` 时使用 |
| 单次字符上限 | 100,000 字符 | `[agent.tool].file_read_max_chars` 可配置且必须大于 0 |

- 2,000 行只是在未传 `count` 时使用的默认值；显式 `count` 只要求正整数，可以大于 2,000。
- 单次实际返回只受 `file_read_max_chars` 字符硬上限约束，不设置同一 assistant 响应内多个 `file_read` 共享的 token 预算。
- 只返回完整逻辑行，不返回半行。
- 普通分页的请求窗口，或 keyword 定位时必须检查的行及最终保留的前后文中，若有单行无法完整返回，则整次 `file_read` 返回显式业务失败（包含 `path`、`status=error`、`line` 和引导使用 `code_run` 的 `msg`），不生成任何读取证据或修改许可；窗口外的超长行不影响当前分页。
- `file_read_max_chars` 只限制本页，不再形成要求改配置并重启的永久阻塞状态。

### 5.3 流式扫描和行语义

parent 与 delegation child 复用同一异步流式实现：

1. 顺序扫描完整 UTF-8 文件，同时计算 SHA-256、字节数、逻辑行数和末尾换行状态。
2. 内存只保留请求窗口、当前行、有界 keyword 前置窗口和摘要状态。
3. 任意 `start` 都可以读取，不受旧的文件前缀读取上限影响。
4. 输出保留原始 `\n` / `\r\n` 和末尾换行，便于构造精确 patch 与 append。

逻辑行定义：空文件 0 行；非空文件至少 1 行；换行符属于前一逻辑行；末尾换行不额外生成空白行。

每一页都扫描全文以可靠计算 revision 和 `total_lines`，时间复杂度为 O(文件大小)，额外内存为 O(本页大小)。这是本期正确性优先的取舍。

### 5.4 Token 原则

- `Complete` 不是所有编辑任务的目标。
- 修改位置已知时优先读取能唯一定位 patch 的最小连续范围。
- append 优先读取最后一页。
- `truncated=true` 不自动触发继续读完整文件；确需续页时使用 `page.next_start`。
- prompt 避免在同一上下文重复读取相同范围。

因此常见 patch 的 token 成本由目标窗口决定，不会因许可证机制明显上升。

## 6. 分级修改许可

| 操作 | 所需许可 |
| --- | --- |
| 新建文件 | 无需预读 |
| `file_patch`，默认唯一替换 | `Complete`，或唯一匹配及换行边界涉及的行均已覆盖 |
| `file_patch(replace_all=true)` | `Complete` |
| `file_write(mode="append")` | `Complete`，或当前 revision 的 EOF 许可 |
| `file_write(mode="overwrite")` | `Complete` |
| `file_write(mode="prepend")` | `Complete` |

### 6.1 统一写入判定

`file_patch` 和已有文件的 `file_write` 共用以下安全流程：

1. 解析路径、固定 canonical target，并检查受保护路径。
2. 获取 canonical path 写锁，读取磁盘当前全文并计算 revision。
3. 查询当前 scope 的 ReadState；`Missing` 或 `Stale` 直接拒绝。
4. revision 相同时，按操作类型检查局部范围、EOF 或 Complete。
5. 在内存中构造写后内容，并计算可迁移的已知范围。
6. 以当前原始 bytes 为 expected value 执行原子 compare-and-swap。
7. CAS 失败时删除旧状态并拒绝；成功时把状态更新到写后 revision。

这同时防止“拿旧版本许可修改新内容”和“许可检查后、落盘前文件又被其他进程修改”。

### 6.2 局部 patch

默认 patch 还需执行以下操作级校验：

1. 在全文按大小写敏感、逐字节精确语义查找 `old_content`；默认必须恰好匹配一次。
2. 默认扫描只保留第一处位置；发现第二处非重叠匹配立即停止并返回固定长度歧义错误，提示扩大 `old_content` 并加入目标附近上下文。
3. `old_content` 直接涉及的旧行必须已覆盖。
4. 把已覆盖行临时映射为已知 byte spans，在内存中模拟替换。
5. 替换后所有受影响结果行必须完全由已知旧 bytes 和 `new_content` 组成。
6. 删除换行导致已读行与未读相邻行拼接时拒绝，并在 `required_read` 中建议补读相邻行。

`replace_all=true` 会修改所有非重叠精确匹配，因此仍要求 Complete。实现只保存首个位置并以迭代计数统计其余匹配，不构造位置列表；不做模糊匹配、缩进或换行归一化。

局部许可只放宽读取范围，不放宽全局唯一匹配、UTF-8、路径保护、revision 或 compare-and-swap 校验。

### 6.3 EOF append

非空文件只有实际返回范围包含最后一条逻辑行时才获得 EOF 许可。append 保持精确字节语义，不自动补换行、不去重。

模型根据 `ends_with_newline` 和原始尾部内容决定传入：

```text
成功审核\n
```

或：

```text
\n成功审核\n
```

超 EOF 空读、最后一行未完整返回或 revision 已变化都不能 append。空文件需要完整空文件许可。

### 6.4 写入后的状态

模型知道自己提交的精确修改，因此成功写入后更新状态：

- 新建、overwrite，或基于 `Complete` 的任何写入：新版本为 `Complete`。
- 局部 patch：把旧已知 byte spans 映射到新内容，并加入 `new_content` spans；只把完全已知的新逻辑行投影回覆盖区间。
- EOF append：保留旧已知 spans，加入追加 bytes，再按新行边界重新投影。
- 无法可靠迁移时删除该文件状态，要求下次写入前重新读取。
- `no_change` 保留原状态。
- revision 不同或原子比较失败时删除该文件状态。

byte provenance 只在写入事务内临时存在，`ReadStateStore` 仍只保存行区间。

## 7. `@file` 完整许可

### 7.1 授权条件

`@file` 同时满足以下条件时，在首轮 preflight 完成、provider 请求即将发送的接入点登记 `Complete`：

- 通过既有附件入口读取普通 UTF-8 文本；
- 通过附件开关、文件大小、单轮数量和受保护路径校验；
- 单个文本文件的 Unicode 字符数不超过 `[agent.tool].file_read_max_chars`；多个文本附件分别判断，不设置合计字符预算；
- 正文是完整原始内容，不是预览、摘录或目录列表；
- 生成的用户消息确实包含该完整正文。

附件先解析 canonical target 并检查受保护路径，再从该 canonical 路径读取正文，避免符号链接切换使正文来源与许可目标不一致。登记前用构造时保存的精确正文 block 与最终 provider request 比对；block 已被 compact 摘要或外置为资产引用时不授权。

revision 只基于正文 bytes，不包含 `Attached file:`、`Path:` 等包装文本。空文件同样可以获得完整许可。

### 7.2 不授权

- `@目录`；
- 图片、PDF、剪贴板图片和其他媒体；
- 无效 UTF-8、超限、读取失败、不存在、非普通文件或受保护路径；
- 用户手写的 `Attached file:` 文本或普通自然语言路径；
- 被截断、摘要化或只剩引用的附件。

超过 `file_read_max_chars` 的文本附件不作为错误终止本轮：请求中只保留路径、实际字符数和使用 `file_read` 的提示，TUI 显示非致命 warning，不登记任何读取许可。该字符限制不适用于 PDF、磁盘图片或剪贴板图片；这些媒体继续只受既有 5 MiB、数量和格式校验约束。

### 7.3 生命周期

- 自动或手动 compact、摘要替换发生时，保守清空当前 scope 的全部文件许可，不逐 block 迁移。
- compact 后注入固定的许可边界提示：摘要或保留的 raw tail 中“曾读取文件”只表示历史事实，不代表当前修改许可。摘要生成规则不得写成“后续修改无需重读”；需要继续修改已有文件时，Agent 按 file tool 返回的 `required_read` 重新建立许可。
- 当前 turn 的附件正文先完成校验和消息构造；首轮 preflight 结束、provider 请求即将发送时再登记一次。这样 compact 可以清除旧许可，又不会误删仍在当前请求中的 `@file` 许可。
- resume 清空该 session 的运行期许可，不从 journal 或历史消息恢复。
- parent 的附件许可不传给 delegation child；授权接入点不修改 provider adapter、transcript 格式或通用 message DTO。

## 8. 错误和工具引导

许可失败时，在可计算的情况下返回：

```json
{
  "required_read": {
    "kind": "range",
    "start": 120,
    "count": 8
  }
}
```

`kind` 只有三种：

- `range`：局部 patch 需要补读指定范围。
- `eof`：append 需要读取最后一页。
- `complete`：overwrite、prepend 或 `replace_all` 需要从第 1 行开始分页读完。

其他要求：

- revision 变化提示重新读取所需区域。
- 确认 revision 已变化或原子写入发现版本冲突时返回 `stale: true`；普通未读或范围不足错误省略该字段。
- `file_read_max_chars` 截断提示依据 `next_start` 继续，不提示用户改配置或重启。
- tool description 明确每种写操作的许可条件。
- prompt 明确优先最小读取加 `file_patch`，不要仅因 `truncated=true` 自动追读全文。
- 不返回 revision、许可证 token、配置路径或 `requires_user_config_change`。

## 9. 并发和安全边界

- 同一 canonical path 的文本 `file_read` 和修改调用由路径锁按 tool-call 顺序串行执行。
- 不同 canonical path 的文本 `file_read` 可以按现有调度并发执行。
- 同路径多个写入保持 tool-call 顺序，每次调用只依据执行时的真实文件状态独立校验和执行；前序业务失败不阻止后续调用。
- 不同路径的安全工具仍可按现有调度并行。
- `code_run` 修改不主动更新 read state；下一次 file tool 写入会因 revision 不同拒绝旧许可。
- 不设置单轮 `file_read` 总字符或 token 预算；单次结果仍由 `file_read_max_chars` 限制。

直接登记 `file_read` 页面的取舍是：同一 assistant 响应如果先执行 read、再执行 write，后者可以使用刚登记的许可。安全性由“返回完整行范围、精确 patch/EOF 分级、内容摘要和原子写入”保证，不再额外证明模型是否在生成该 write 前看过 tool result。

## 10. 验证清单

### 10.1 分页

- 264 行文件分三页、乱序或重叠读取后合并；存在缺口时不能 `Complete`。
- 单次字符限制后可从 `next_start` 继续，不改配置、不重启。
- 未传 `count` 时默认读取 2,000 行；显式 `count` 可以大于默认值，单次返回仍受字符硬上限约束。
- count、字符上限、超长单行、keyword miss、超 EOF、空文件和正常 EOF 元数据正确。
- CRLF、Unicode、无末尾换行和无效 UTF-8 行为正确。
- 大文件可读取旧前缀限制以后的页面，read state 不保存全文。

### 10.2 修改许可

- 已覆盖区域内唯一 patch 成功；跨未读边界、零匹配和重复匹配拒绝。
- 删除换行连接未读相邻行时拒绝，补读后成功。
- 非 Complete 的 `replace_all`、overwrite 和 prepend 拒绝。
- 有效 EOF 页允许 append；超 EOF 空读、缺失最后一行和 stale revision 拒绝。
- patch 插入/删除行和 EOF append 后范围迁移正确。
- 两处及大量重复匹配都以固定长度错误拒绝；扩大 `old_content` 后只修改目标位置。
- `replace_all=true` 正确替换全部非重叠匹配，且不保存匹配位置列表。
- `old_content == new_content` 仍完成正常校验与同内容原子写入，返回成功、保留修改许可且不生成 diff。
- 外部并发修改在摘要或 compare-and-swap 阶段被拒绝。
- 许可失败返回正确 `required_read`。

### 10.3 `@file` 和生命周期

- 完整 UTF-8 `@file` 无需 `file_read` 即可执行所有写操作。
- 附件读取后文件变化，写前摘要拒绝旧许可。
- 相对路径、绝对路径和符号链接归并。
- `@目录`、媒体、无效 UTF-8、失败和受保护路径不授权；文本超出 `file_read_max_chars` 时仅路径降级并显示 warning，同样不授权。
- 多个文本附件按单文件分别应用 `file_read_max_chars`，不设置单轮合计字符预算；PDF 和图片不应用文本字符限制。
- compact 和 resume 清理旧许可；delegation compact 只清理对应 child scope。
- provider 失败、cancel 和 steer 会撤销本任务新建或扩展的 read state，并保留任务前已有状态。
- 任务已成功写文件后再失败：磁盘内容保留，恢复的旧 revision 在下次 file tool 写入时判为 stale。
- compact 后任务失败只撤销 compact 后新状态，不复活 compact 前许可。
- 主会话历史、当前 turn 和 delegation child 的 compacted context 都明确提示旧读取不再授权，并引导按 `required_read` 重建许可。
- parent 许可不传给 delegation child。

### 10.4 调度和 TUI

- 不同 canonical path 的多个 file read 可并发执行，同一路径的调用由路径锁串行执行。
- 同路径多个写入按 tool-call 顺序独立执行；前序失败后，后序仍基于届时的真实文件状态正常推进。
- 真实 ACN TUI 可以完成分页读取、局部 patch、EOF append、完整改写、`@file` 修改和 stale 拒绝。

## 11. 已知取舍

- 每页扫描全文计算 revision，分页很多时会重复 O(文件大小) I/O；后续可增加经过校验的摘要和行偏移缓存。
- compact 直接清空 scope 比逐消息追踪更保守，可能多一次读取，但实现边界清晰。
- 异常超长单行缺少字符偏移协议，继续使用 `code_run` 处理。
- patch/diff 写入仍会加载全文；其流式化不属于本期。
- file read 立即登记不证明 provider 实际消费了结果，这是为避免引入通用可见性状态机而接受的明确取舍。

## 12. 实施验证

- 版本一致性、格式化、Clippy（warnings denied）和 `cargo check` 通过。
- 1874 个 library 测试通过；一个可在最新 main 独立复现的 MCP discovery 既有失败被单独排除。55 个 binary 测试、6 个 integration 测试和 doc tests 通过。
- 使用隔离 fake provider 配置运行仓库 tmux 冒烟流程通过，stderr 为空。
- 超限 `@` 文本专项 tmux 流程通过：TUI 显示非致命 warning 后继续完成任务，请求中只包含路径和实际字符数，不泄露正文；120 列终端下未发现边框、换行或状态栏错位。
- 独立只读代码审查发现的问题均已修复并补充回归测试，包括 hard abort 回滚、父会话 compact 与 child scope 隔离、canonical 提交边界、delegation terminal 提交边界、summary 本地失败时禁止 recap 请求、Memory tool result 脱敏、transport/parse/shape 共用 retry budget、每次 retry 逐次重算预算，以及 compaction guard 复用统一的 `4 chars/token` 本地粗估。
