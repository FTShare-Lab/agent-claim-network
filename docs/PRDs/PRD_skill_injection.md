# Skill 显式调用与正文注入

> 状态：已实现。本文保留 Skill 识别、正文注入、持久化和安全边界。

## 背景

TUI 已支持用户 skill 的 `/` 菜单与句中唯一前缀补全，但补全后的`/skill-name` 当前只作为普通文本发送。模型只能自行联想到系统 prompt 中的skill 摘要并调用 `file_read`，不能保证用户显式选择的 skill 一定生效。

本需求将显式 `/skill` 变成确定性的运行时语义：解析受信任的 skill 引用，读取对应 `SKILL.md` 完整正文，并作为当前用户 turn 的结构化上下文发送给模型。

## 目标

- 行首和句中显式提及的已注册 user skill 都稳定注入完整 `SKILL.md` 正文。
- 注入不是文件附件、不是假造 tool result，也不是简单拼接到展示给用户的文本。
- 用户原始输入、图片、文档和粘贴内容继续按既有方式发送并展示。
- skill 正文作为不可变快照持久化到 canonical transcript；后续未 compact 的历史按原样复用该快照，不重复读取磁盘。
- 当前 active turn 发生 compaction 时，skill 正文必须随用户消息 anchor 完整保留。

## 术语

- **原生命令**：`/compact`、`/copy`、`/exit`、`/help`、`/inbox`、`/mcp`、`/ps`、`/resume`、`/skills` 等 TUI 操作。
- **显式 skill**：用户输入中以 `/` 加合法 skill 名称表示的已注册 user skill。
- **可见输入**：TUI composer 展示的文本，包含 `[Pasted Content …]` 与`[Image #…]` 占位符。
- **展开输入**：真正发送给模型的文本；粘贴占位符替换为粘贴正文，图片仍作为附件content block 单独发送。

## 调用语义

### Skill 识别

- 只识别已注册 user skill；名称由 ASCII 字母、数字、`-`、`_` 组成。
- `/skill` 可位于输入开头或空白之后；名称后可接输入结尾、空白或标点。
- 一个输入可提及多个 skill，按首次出现顺序注入；同一路径只注入一次。
- 原生命令永远不作为 skill 识别；同名时原生命令优先。
- 未知的句中 `/name` 保持普通文本；未知的行首 slash 输入保持现有“未知命令”错误。
- 用户手工完整输入与通过 Tab/菜单补全得到的 `/skill` 语义完全相同。

### 行首参数

行首形式支持参数：

```text
/review src/auth.rs --strict
```

`SKILL.md` 中可以使用以下模板占位符：

- `$ARGUMENTS`：完整参数字符串。
- `$ARGUMENTS[0]`、`$ARGUMENTS[1]`：按 shell 风格解析后的第 n 个参数。
- `$0`、`$1`：对应参数的简写。
- `${ACN_SKILL_DIR}`：当前 skill 所在目录的绝对路径。

如果正文没有参数占位符且行首调用提供了非空参数，追加`ARGUMENTS: <原始参数>`。句中引用没有独立参数；用户完整输入本身就是任务描述。

不支持 shell 插值、skill 自行授予工具权限、hook、fork、模型覆盖等能力。

### 用户消息形状

对于：

```text
先看当前修改，再用 /code-review 检查并发安全，最后只列 P1/P2
```

内部以一条 `role=user` 消息发送：

```text
SkillInstructions(code-review, 完整快照)
Text(用户展开后的原始输入)
Image / Document（如有）
```

provider 适配层把 `SkillInstructions` 渲染为带 name、spec_path、base_dir、arguments 和正文的 `<acn_skill scope="current_user_turn">…</acn_skill>` 文本块。skill块在用户原文前；原文完整保留，包括 `/code-review`。

TUI、session search、用户 transcript 展示只显示 Text 与已有附件占位，不展示完整skill 正文。

## 粘贴与附件边界

skill 识别必须发生在可见输入上，并把结构化引用随 `QueuedInput` 一起传递。不得在展开后的粘贴正文中扫描 `/skill`。

因此：

- 用户在 `[Pasted Content …]` 外输入 `/review` 会调用 skill。
- 粘贴的代码、日志或文档内部出现 `/review` 不会调用 skill。
- `[Image #…]` 与 `/review` 可在同一输入中共存；图片仍通过既有附件 block 发送。
- 原生 slash command 分类也以可见输入为准，粘贴正文不能意外执行 `/exit`、`/help`等本地命令。

## 持久化、历史与压缩

- turn 开始时异步读取正文，生成内容和 hash 固定的 snapshot。
- canonical transcript 保存完整 `SkillInstructions` block；后续未 compact 历史直接使用该 snapshot，不重新读文件。
- skill 只约束提及它的当前用户 turn。历史里保留的 skill 正文用于保持模型可见历史、审计和稳定前缀，不自动成为下一用户请求的工作协议。
- active turn compaction 必须保留包含 `SkillInstructions` 的第一条用户消息 anchor。
- 正式 compaction 可以把已经完成的历史 turn（及其中的 skill 正文）纳入摘要；不将所有旧 skill 永久固定在摘要后。compaction 本身重写前缀，无法保留旧缓存命中。
- turn journal 记录已解析的完整 skill snapshot，确保未完成 turn 的恢复不受磁盘文件后续变化或删除影响。

## 错误、安全与限制

- 只接受启动时 registry 中的 canonical `SKILL.md` 路径，拒绝路径逃逸和未注册引用。
- 显式 skill 缺失、不可读、超限或解析失败时，在调用模型前明确失败；不退化成普通文本发送。多个显式 skill 中任意一个失败，整轮不发送。
- 默认单个正文最大 256 KiB，单轮最多 8 个 skill；两项均通过配置提供。
- runtime 注入的已注册 `<skill>` 是可遵循的工作流指令，但永远低于 system、当前用户目标和现有工具安全边界。
- 未显式提及但任务明显匹配 skill 时，保留既有模型自主 `file_read` 路径；显式调用已注入正文时，prompt 要求模型不要重复读取同一 `SKILL.md`。

## 非目标

- 不新增独立的 `Skill` tool；skill 只通过 system prompt 注入和既有工具执行。
- 不实现远程/MCP skill、hook、allowed-tools、subagent fork 或模型覆盖。
- 不改变普通路径、`@path` 附件、图片粘贴和原生命令的业务语义。
- 不在本需求中为 provider 增加显式 prompt-cache breakpoint；保持消息序列稳定，允许支持自动前缀缓存的上游复用。当前 provider 如需要显式 cache_control，应另行设计。

## 验收标准

- 行首 `/skill`、`/skill args`、句中 `/skill`、多 skill 都生成完整正文注入。
- `$ARGUMENTS`、索引参数、`${ACN_SKILL_DIR}` 替换符合定义；句中调用不误解析参数。
- 原生命令和未知 slash 输入保持既有优先级与错误语义。
- 粘贴正文中的 `/skill`、`/exit` 不触发 skill 或原生命令；可见文本中的对应输入正常生效。
- Skill、粘贴占位符、图片/文档附件可以同轮发送，TUI 显示不泄露正文。
- provider 请求、canonical transcript、恢复 journal 都含同一份正文 snapshot 和 hash。
- active compaction 后当前 turn 仍看到完整 skill；未 compact 的后续 turn 保留完整快照。
- 缺失、不可读、超限 skill 在 provider 调用前失败。
