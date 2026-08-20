# 附件与 `@路径` 输入功能 PRD

本文档完整说明当前 ACN 的附件输入体系：用户如何通过 `@路径` 引用本地文件或目录、通过
`Ctrl+V` 粘贴图片，文件如何经公共读取层和 `file_read` 进入模型消息，以及带附件会话如何在
持久化和 `/resume` 时保持正确且紧凑的展示。

本文档以已经实现的行为为准，是附件输入的唯一 PRD。

---

## 1. 背景与问题

在终端中讨论代码、图片、PDF 或项目目录时，用户常常需要将本地信息明确提供给模型。仅在自然语言中写一个路径，例如 `/tmp/design.pdf`，模型并不能确定用户是否授权读取该文件，也会额外消耗一次工具调用。

`@路径` 提供显式、可见且可校验的引用入口：

- `@文件` 表示把该文件作为当前 user turn 的附件；
- `@目录` 表示把该目录的一级条目列表作为当前 user turn 的文本上下文；
- 输入过程中提供路径补全，避免手工输入长路径；
- 用户消息气泡始终保留用户输入的原文，不把解析出的代码、base64 媒体或目录列表铺在 TUI 中；
- session resume 时不会因 journal 与 `messages.jsonl` 的附件表示不同而把同一轮显示两次。

普通文本中的路径不具有附件语义。例如“请读取 `/tmp/a.pdf`”仍只是普通文本，模型可自行决定是否调用 `file_read`。

---

## 2. 目标、范围与非目标

### 2.1 目标

- 在 TUI composer 中解析明确的 `@路径` 标记。
- 支持相对路径、绝对路径、`~` 家目录和包含空白字符的路径。
- 支持对目录和文件的异步补全。
- 将文本、图片和 PDF 文件作为当前 turn 的模型上下文发送。
- 将目录引用转换为稳定、有限、非递归的一级目录列表。
- 为用户提供附件预览入口。
- 支持 macOS 用 `Ctrl+V` 粘贴剪贴板图片。
- 复用公共附件读取/校验逻辑，避免 TUI 与 `file_read` 的媒体处理规则漂移。
- 将文本、图片、PDF 统一映射到 Anthropic Messages API 和 OpenAI Chat Completions 的请求格式。
- 保证 canonical transcript、turn journal 和 resume 时间线相互对齐；`canonical_user_message`
  对齐事件只写哈希，不重复写入文本附件正文或媒体数据。目录列表属于发送时展开的
  `user_text`，当前会同时进入 canonical transcript 与 journal 的 `UserInputAccepted`。

### 2.2 非目标

- 不把自然语言里的任意路径自动变成附件。
- 不递归展开目录，不读取目录内文件正文，也不把目录本身当作二进制附件上传。
- 不在补全阶段递归扫描、读取候选文件内容，或按“模型支持的附件类型”过滤候选。
- 不支持任意二进制文件；非图片、非 PDF 的文件只按 UTF-8 文本尝试读取。
- 不做 PDF 的本地 OCR、文本抽取、分页或渲染。
- 不支持 Windows/Linux 的系统预览和剪贴板图片读取；`@路径` 本身仍按 Rust 标准路径语义工作。
- 不把附件伪装为模型发起的 `tool_use` / `tool_result`。
- 不提供 `/attach` slash command、OpenAI Responses API、PDF 按页/延迟读取或图片以外的剪贴板文件对象。

---

## 3. 用户可见行为

### 3.1 基本用法

```text
请审查 @src/session_tui/attachment.rs
总结 @docs/design.pdf
比较 @"docs/Release Plan.md" 和 @src/main.rs
列出 @src/session_tui/ 的职责
```

一条消息可以包含多个引用。文件与目录可混用：文件形成附件，目录形成文本上下文。

`@` 只有位于输入开头，或其前一个字符是空白字符时，才被识别为引用起点。因此以下内容不会误触发：

```text
user@example.com
路径a@b
```

### 3.2 路径词法规则

| 写法 | 结果 |
| --- | --- |
| `@src/lib.rs` | 裸相对路径；到第一个未转义空白字符结束。 |
| `@/tmp/a.pdf` | 绝对路径。 |
| `@~/Desktop/a.png` | 以当前用户家目录展开。 |
| `@docs/a\ b.md` | 裸路径中的反斜杠转义空白字符。 |
| `@"docs/a b.md"` | 双引号路径。 |
| `@'docs/a b.md'` | 单引号路径。 |

补全插入带空格的路径时使用裸路径的反斜杠转义形式，例如 `@docs/a\ b.md`。

下列情况会阻止发送，并把原始草稿恢复到 composer：

- `@` 后没有路径，或紧跟空白字符；
- 引号未闭合；
- 路径不存在、不可读取或不是常规文件/目录；
- 文件大小、数量、类型或内容校验不通过；
- 命中受保护的 memory 文件。

解析错误绝不静默忽略。用户可修正草稿后重试。

### 3.3 Composer 展示与键盘操作

识别出的 `@路径` 在 composer 中以灰色加粗显示；光标位于该标记上时额外加下划线，提示可以预览。只有标记本身被着色，前后的自然语言维持普通样式。

当光标位于一个尚可继续输入的 `@路径` 末尾时，TUI 显示候选菜单：

- `Tab`：接受当前候选；
- `↑` / `↓`：循环选择候选；
- `Esc`：关闭当前路径上下文的菜单；
- `Enter`：菜单存在时先接受候选，而不是立即提交消息；菜单关闭后才提交；
- 选中目录时会补入尾随路径分隔符，并继续读取/展示该目录的下一级候选；
- 选中文件后关闭当前菜单；输入空白字符也会结束目录引用 token。

候选菜单与 slash command 菜单共用统一的交互组件：一次最多显示 5 行，选中项滚动保持可见。

### 3.4 路径补全规则

补全使用 ACN 的有效 workspace：`acn --cd` 指定时使用该目录，否则使用启动 TUI 时的当前工作目录。相对路径相对该 workspace；绝对路径不改写；`~` 展开为当前用户的家目录。

补全只异步读取当前父目录的直接子项：

- 不递归进入子目录，不读取文件正文；
- 最多读取 1000 个目录项；该上限与目录引用上下文共用语义；
- 目录项读取完成后，过滤并排序，最终最多显示 50 个候选；
- 目录优先于文件；同类中优先精确大小写前缀匹配，再按大小写不敏感前缀和路径名排序；
- 隐藏文件也可补全；
- 文件类型不参与候选过滤，所以 `.zip`、视频等也可能出现，真正发送时才会按附件规则校验；
- 常规文件、目录以及最终指向这两类的符号链接可成为候选；其他文件系统对象跳过；
- `memories/MEMORY.md` 与 `memories/USER.md` 被隐藏，不能通过补全选择。

`1000` 和 `50` 是 `src/config.rs` 中的内部常量：

```rust
DEFAULT_TUI_AT_PATH_DIRECTORY_CONTEXT_MAX_ENTRIES = 1_000
DEFAULT_TUI_AT_PATH_MAX_CANDIDATES = 50
```

它们不属于 TOML 用户配置。这样补全扫描和最终目录上下文不会出现两个语义近似却可配置为不同值的上限。

如果目录读取失败，菜单显示错误，并针对该目录停止自动重试；用户改变路径上下文后可再次触发读取。异步读取结果带 generation，过期结果不会覆盖用户已经继续输入后的菜单状态。

### 3.5 终端输出安全

Unix 文件名可包含 `ESC`、Tab、CR 等 C0/C1 控制字符。补全不能因此拒绝候选或改写
`raw_path`：原始路径仍用于候选选择、写回 composer、`@路径` 解析和实际文件读取，否则合法文件
将无法引用。

但任何动态 TUI 文本在写入物理终端前，必须由
`src/session_tui/terminal.rs::terminal_safe_content` 移除全部 C0/C1 控制字符。该边界统一覆盖
候选菜单、composer、排队预览、用户历史、模型输出和工具输出，而不只覆盖 `@路径` 菜单。行宽计算
也必须使用相同的净化结果，确保 live region 的清理、折行与 composer 光标位置不发生错位。

例如真实文件名 `evil<ESC>[2J.txt` 在候选菜单和 composer 中显示为 `evil[2J.txt`；`ESC[2J`
不会到达终端，因此不能触发清屏。被移除的控制字符在当前 Unicode 宽度模型中为零宽，展示层删除它们
不改变原始文本的光标坐标。业务数据、附件内容、`messages.jsonl` 和 turn journal 均保存原始路径，
不因展示安全策略而改变。

### 3.6 预览

`Ctrl+O` 预览输入框中的附件：

- 光标命中某个 `@路径` 或 `[Image #N]` 占位符时，仅预览该项；
- 否则预览输入框内的全部附件，顺序与输入中出现的顺序一致；
- macOS 使用 `open` 交给系统默认应用（图片和 PDF 通常为 Preview）；
- `@目录` 不能预览，非文件路径也会报错；
- 剪贴板图片会临时落盘供系统打开，TUI 退出时清理该临时文件；
- 非 macOS 明确提示“附件预览仅支持 macOS”。

`Ctrl+V` 是同一附件体系的剪贴板图片入口：macOS 下读取图片后在 composer 插入 `[Image #N]`，删除占位符即撤销该图片附件。`Command+V` 和普通 paste 事件一律按文本插入，不会意外附带剪贴板图片；`Ctrl+V` 时剪贴板不是图片则给出轻量提示。

---

## 4. 文件、目录与安全约束

### 4.1 文件分类和传输形式

提交阶段按扩展名先做路由，读取阶段再做内容校验：

| 路由 | 扩展名 | canonical user content |
| --- | --- | --- |
| 图片 | `png`、`jpg`、`jpeg`、`gif`、`webp` | `Image { media_type, data }` 多模态块。 |
| PDF | `pdf` | `Document { media_type: application/pdf, data, filename }` 多模态块。 |
| 文本 | 其他全部路径 | 不超过字符上限时为 `Attached file: <文件名>`、绝对 `Path` 和完整 UTF-8 正文；超过上限时只提供路径、实际字符数和 `file_read` 指引。 |

图片不只信任扩展名：必须能识别为 PNG/JPEG/GIF/WebP 并成功解码。最长边超过 2048 像素时，按 2048、1536、1024、768、512 的阶梯等比缩小并重新编码，直到满足大小上限；JPEG 保留 JPEG，其他重编码为 PNG。PDF 必须具有 `%PDF-` 文件头。文本必须是有效 UTF-8。

图片和 PDF 的 bytes 使用 base64 进入 provider 所需的多模态内容块；TUI 不展示这些 base64 数据。

### 4.2 文件限制

`[agent.attachment]` 是文件附件和剪贴板图片的用户配置：

```toml
[agent.attachment]
enabled = true
clipboard_image_enabled = true
max_file_bytes = 5242880
max_files_per_turn = 5
```

- 默认单文件上限为 5 MiB，默认每轮最多 5 个文件/图片附件；
- 提交前的 metadata 检查与最终 turn loop 读取时均会检查上限，后者是最终防线；
- `@文件` 与剪贴板图片合并计入 `max_files_per_turn`；目录不计入文件附件数量；
- `enabled = false` 时，composer 的 `@` 高亮与补全关闭，`@文件` 与 `@目录` 都按普通文本发送；不会读取附件，也不会扫描并注入本地目录列表；
- `clipboard_image_enabled = false` 只禁用 `Ctrl+V` 图片入口。

图片 resize/downsample 的最大边长和媒体上下文预算都是内部实现约束，不提供 TOML 配置项；用户可配置的附件参数仅为上述四项。

文本附件还复用 `[agent.tool].file_read_max_chars` 作为单个文件的 Unicode 字符上限，默认 `100000`。多个文本文件分别判断，不设置单轮合计字符预算。正文不超过上限时完整内联，并可形成等价的完整读取许可；超过上限时不截断、不注入预览，只向模型提供路径、实际字符数和 `file_read` 提示，同时在 TUI 显示非致命 warning，不发放读取许可。文本仍同时受 5 MiB 文件大小限制；该字符上限不适用于 PDF、磁盘图片或剪贴板图片。

`memories/MEMORY.md` 和 `memories/USER.md`（先按词法消解 `.` / `..` 后判断）是受保护路径。`@路径`、附件公共读取层和 `file_read` 始终拒绝读取；Memory 子系统启用时只能通过专用 memory 工具访问，关闭时则不提供任何 agent 访问通道。

### 4.3 目录引用

目录不是文件附件。提交 `@src/` 时，TUI 在后台生成如下附加文本：

```text
[Referenced directory: src/]
Resolved path: /absolute/workspace/src
First-level entries (ls -A, showing 3):
api
main.rs
session_tui
```

规则如下：

- 使用解析后的实际目录读取一级条目，不递归；
- 保留隐藏条目（语义近似 `ls -A`），不额外读取元数据以区分文件/目录；
- 最多读取 1001 个名称：第 1001 项只用于确认截断，不保留或排序；因此大目录不会被全量读入内存；
- 名称按稳定的 Unicode 可见文本字典序排序；非 UTF-8 文件名仅作为显示用替代文本；
- 最多追加 1000 个名称。未截断时显示 `showing N`；截断时显示 `showing first 1000; more entries omitted`，明确总数未知且仍有后续条目；
- 一条消息有多个目录时，每个目录生成一个区块，区块之间用空行分隔；
- 目录列表追加到用户原始文本末尾，使用两个换行分隔，因而可被模型作为当前请求的补充上下文。

例如用户输入 `请检查 @src/`，模型看到的是“请检查 `@src/`”加上上述目录列表；但 TUI 用户气泡只显示 `请检查 @src/`。

### 4.4 公共读取与 `file_read`

`@常规文件` 与模型主动调用的 `file_read` 共享同一套读取、校验与规格化逻辑：

```text
read_attachment(path)
  -> 校验路径、权限和文件大小
  -> 读取 bytes 并判断文件类型
  -> 文本：校验 UTF-8，保留既有行号、分页、keyword window 和最大字符数语义
  -> 图片：校验实际 media type，必要时 resize / downsample，再编码为 base64
  -> PDF：全量读取，校验 %PDF- 文件头，再编码为 base64
```

入口关系如下：

```text
@常规文件 path       -> read_attachment(path)
@目录 path           -> 生成有限的一级目录列表文本上下文
file_read(path)      -> read_attachment(path)
普通自然语言路径      -> 不处理；模型可自行决定是否调用 file_read
剪贴板图片            -> 直接走图片规格化，不经过路径读取
```

`file_read` 的工具语义扩展为读取文本、图片或 PDF：文本仍返回 JSON 文本结果；图片/PDF 返回简短
工具文本元信息，并在同一工具回环的内部 meta user message 中携带对应的 image/document content
block。媒体 base64 不进入工具结果可见文本，以便 transcript、debug 和失败排查仍可读。

---

## 5. 提交与模型消息链路

### 5.1 发送前异步解析

普通 `Send` 类型输入中出现 `@路径` 后，TUI 不立即启动 turn：

1. 取走 `InputDraft`，保留其可见文本、展开后的粘贴文本和剪贴板图片附件；
2. 在 `spawn_blocking` 中调用 `resolve_at_paths`，执行路径词法结果校验、metadata 检查和目录列表读取；
3. 将结果通过带提交 sequence 的 `AppEvent::AtPathResolved` 回灌；
4. 成功时，把目录上下文追加到实际模型输入，并把解析出的文件附件追加到草稿已有的剪贴板附件后；
5. 失败时不提交 turn，显示 `attach failed: ...`，并恢复原草稿；
6. 所有普通提交（包括不含 `@路径` 的输入）按 sequence 顺序 flush，避免异步 A 尚未完成时已提交的 B 先写入历史或先进入队列；若用户取消/恢复时需要还原输入，异步结果只恢复草稿而不误提交；
7. 每个成功 flush 的普通输入用其所属 `QueuedInput.draft` 记录输入历史。`BottomPane` 不保存全局“最后一次取走的草稿”：当 A 正在解析而用户已提交 B 时，B 的草稿不会覆盖 A 的历史记录，且历史保持 A、B 的提交顺序；解析失败或被恢复到 composer 的草稿不记为已提交历史。

slash command 不走这条解析链路。运行中使用 `Ctrl+Enter` 试图 steer 时，只要该输入含 `@路径` 或内联图片，就强制改为排队下一轮；这是因为附件预检、文件读取和当前 turn 的 interrupt-and-steer 不能安全地混合。

### 5.2 内部数据对象

TUI 向 `SessionTurnRequest` 传递两部分信息：

```text
QueuedInput
├── text              # 模型可见文本：原输入 + 可选目录上下文
├── draft             # 可见原文、粘贴映射、[Image #N] 占位符
└── attachments       # 剪贴板图片 + @文件解析出的 SessionAttachment
```

`draft` 是提交级别的数据所有权边界，而不是对 composer 当前状态的临时引用。异步结果回灌、
队列预览、取消恢复和输入历史都从同一个 `QueuedInput.draft` 读取原始草稿；因此不能以一个
跨提交的单槽缓存推断“本次提交的草稿”。

`SessionAttachment` 有四种形式：

- `LocalImage { path }`
- `InlineImage { media_type, data }`
- `TextFile { path }`
- `DocumentFile { path, media_type }`

真正读取文件、图片规格化和 PDF 校验在 `AgentTurnLoop` 构造 user message 时完成，而不是长期保存在 composer。这避免把大内容和 base64 放进 TUI 草稿状态，也让 turn loop 对所有调用方执行最终校验。

最终 user content block 的顺序固定为：

1. 当前回合生效的 skill instructions；
2. 用户文本（包含目录上下文时也在这一块）；
3. 每个附件的内容块，顺序与剪贴板图片、`@文件` 的收集顺序一致。

这是用户主动提供的上下文，不生成伪造的工具调用历史。

超出 `file_read_max_chars` 的文本附件仍占用一个附件数量名额，但 user content 中只有有界路径说明。该降级不终止 turn；模型如需正文，必须按提示使用 `file_read` 分页读取。

### 5.3 会话 content block、provider 映射与 base64

会话协议在既有 skill、工具调用和工具结果内容之外，显式区分 `Text`、
`Image { media_type, data }` 与 `Document { media_type, data, filename }`。文本化展示媒体时只使用
`[attached image: ...]`、`[attached document: ...]` 一类的短占位，绝不输出 base64。

provider 映射如下：

| 内部内容 | Anthropic Messages API | OpenAI Chat Completions |
| --- | --- | --- |
| `Text` | `{"type":"text","text":"..."}` | `{"type":"text","text":"..."}` |
| `Image` | `image` + base64 `source` | `image_url` + `data:<media_type>;base64,...` URL |
| `Document` | `document` + base64 `source` | `file` + `filename` 与 `data:application/pdf;base64,...` `file_data` |

其中的精确 payload 形状为：

```text
Anthropic Image    -> {"type":"image", "source":{"type":"base64", "media_type":"...", "data":"..."}}
Anthropic Document -> {"type":"document", "source":{"type":"base64", "media_type":"application/pdf", "data":"..."}}
OpenAI Image       -> {"type":"image_url", "image_url":{"url":"data:<media_type>;base64,<data>"}}
OpenAI Document    -> {"type":"file", "file":{"filename":"...", "file_data":"data:application/pdf;base64,<data>"}}
```

MVP 统一使用 base64：本地附件可在单次请求中完成，不需要维护远端上传文件的生命周期，并可让两个
provider 复用同一内部表示。单文件上限为 5 MiB，base64 膨胀后的请求仍受控。它避免记录 uploaded
file id、清理远端文件及处理跨 provider 的生命周期差异。后续只有在文件很大、需要跨轮重复引用，或
需要降低 payload 时，才另行设计上传和 `file_id` 缓存。

上游模型不支持图片/PDF、provider 拒绝对应 content block，或上游施加更严格的媒体大小、数量和
格式限制时，错误直接展示给用户，并尽量保留 provider 的核心错误信息；不得静默降级为普通文本。

### 5.4 失败与可恢复性

发送前的错误不会吞掉用户输入。发送后文件可能被替换、删除或权限改变，因此 turn loop 再次读取时仍可能失败；这时 turn 按普通失败语义落入 journal，而不会写入不完整的 canonical transcript。文本正文只超过字符上限不是错误：turn 继续，并把路径降级说明作为 canonical user content；正文不会进入 provider 请求或 canonical transcript。

目录内容只在提交时取一次快照；之后目录变化不会改写已提交 turn 的上下文。

---

## 6. TUI 展示、transcript 与 resume

### 6.1 三种文本必须分离

功能有三个不同用途的表示，不能互相替代：

| 表示 | 内容 | 用途 |
| --- | --- | --- |
| 可见原文 | 用户在 composer 中输入的原始 `@路径` 文本及图片占位符 | composer、排队预览、用户气泡。 |
| canonical user message | 原文（或其 recovery wrapper）+ 目录上下文 + 文件正文/媒体块 | provider 历史、`messages.jsonl`、compact、search、finalize、memory review。 |
| journal 对齐标识 | canonical user content 的 `sha256-v1` 哈希 | 判断 journal turn 是否已进入 canonical transcript，避免 resume 重复展示。 |

可见原文绝不能被目录列表、文本文件正文或媒体 base64 替换。`QueuedInput::command_text`、pending turn 的用户 echo 和输入队列预览均使用 `InputDraft::visible_text`。

### 6.2 Canonical transcript

turn 成功提交后，完整 user content 写入 `messages.jsonl`。其中：

- 文本文件正文会作为第二个及之后的文本块保存；
- 图片/PDF 媒体块按 session message 协议保存；
- 目录列表写在首个用户文本块末尾；
- 它们是模型真实看到的上下文，因此必须保留在 canonical transcript。

渲染用户历史时，只取第一段 user 文本，并移除自动追加的 `\n\n[Referenced directory: ...]` 区块；若文本包含 recovery wrapper，则先提取 `<current_user_request>`。这使普通历史和 resume 用户气泡只显示用户原始的带 `@` 消息。

### 6.3 `turn_events.jsonl` 的 compact 对齐事件

每个准备进入 canonical commit 的 user turn，会先在 `turn_events.jsonl` 写入 durable `canonical_user_message` 事件。新格式只记录：

```json
{
  "kind": "canonical_user_message",
  "content_hash": "sha256-v1:<64 位 SHA-256 hex>"
}
```

哈希算法是：对该 user message 的完整 `SessionContentBlock` 数组做稳定 JSON 序列化，再计算 SHA-256，并加上 `sha256-v1:` 版本前缀。因此文本附件、图片/PDF 媒体块、skill block 和目录上下文都参与哈希。

该事件的目的仅是连接 journal 与 `messages.jsonl`：

- `canonical_user_message` 事件不重复保存文本附件、目录列表或媒体数据，只保存完整
  canonical user content 的哈希；
- resume 可确认某个 journal turn 对应哪条完整 canonical user message；
- `messages.jsonl` 仍然是唯一的 committed transcript 权威来源；
- 哈希不参与 search、compact、finalize 或 memory review。

`UserInputAccepted` 沿用当前 turn 接收到的 `user_text`。文本文件正文、图片/PDF 媒体块以
独立附件 block 构造，因此不会写入该事件；目录列表则在 TUI 发送前直接追加到
`user_text`，所以会同时保存在 `UserInputAccepted` 和 canonical user message 中。
resume 展示仍会移除该目录区块，只显示用户原始的 `@目录` 文本。

### 6.4 Resume 去重与历史兼容

resume 会同时使用 `messages.jsonl` 与 `turn_events.jsonl`：

1. 从 `messages.jsonl` 建立已提交的对话历史；
2. replay journal，以恢复失败/中断 turn、assistant partial、工具状态等时间线事实；
3. journal 无读取 warning，且其中 committed turn 的末尾完整覆盖最近的 canonical
   历史时，直接使用 journal 时间线；覆盖检查要求 user 身份和 assistant 文本一致；
4. 不能完整覆盖时，对最近的 canonical 与 journal turn 做降级合并：journal 有
   `content_hash` 时以完整 content hash 作为 user 身份，否则比较规范化后的可见用户文本；
   两边都有 assistant 文本时要求完全相等，任一边缺少时允许继续判断；
5. 只有在 canonical 与 journal 两个方向都恰好存在一个兼容候选时才建立关联，再通过
   LCS 保持 turn 的相对顺序；TUI 合并不比较两个 JSONL 中的时间戳，无法唯一关联的 journal
   turn 保持独立并显示降级恢复提示；
6. 降级合并不按 turn 终态过滤候选。failed、cancelled、interrupted 的 journal turn
   如果满足上述唯一关联条件，也可以与 canonical 合并；合并结果仍保留 journal 的原始
   状态、工具调用和时间线，不会将其状态改写为 committed。

旧会话兼容策略：

- 更早的 journal 没有 `canonical_user_message` 时，使用规范化后的原始用户请求文本、
  assistant 内容、一对一唯一性和相对顺序进行回退比对；
- 曾经写入完整 `content` 的旧版 `canonical_user_message` 事件，在读取 projection 时即时计算同一 `sha256-v1` 哈希，并只保留首个文本供旧气泡展示；
- 不迁移、不重写旧 `turn_events.jsonl`，也不会在 resume 时把历史附件正文重新写入 journal。

这解决了“有附件时 `messages.jsonl` 有完整内容、journal 没有或有另一份表示，resume 退化为两条用户消息”的问题，同时避免长灰色用户气泡。

### 6.5 Compact、recap、search 与媒体上下文预算

未 compact 的最近历史保留真实附件 block，并随正常历史发送给主模型。compact 的 summary 输入、
recap、finalize、memory review 和 session search 会把媒体降级为可读占位，原始图片/PDF base64
不会写入摘要、索引或 resume 展示。

若完成 compact 后完整 tail 仍超过 hard-tail budget，provider 投影会把 Skill、文本附件和媒体
block 外置为 session 内的不可变、内容寻址文件，并在原位置留下
`externalized_compaction_asset` 引用。canonical `messages.jsonl` 和 TUI 气泡不改写；模型需要
原始内容时可按引用路径重新调用 `file_read`。只有引用化后仍超限时，才使用当前 summary 字符上限
的一半重新压缩一次；仍超限才提示用户拆分纯文本请求或开启新会话。详细预算与恢复顺序见
[Provider Request 前统一压缩](PRD_compact_in_turn.md)。

媒体的 base64 长度不等于模型上下文成本。MVP 对每个 image block 和 PDF document block 固定估算
`2000` tokens，用于内部上下文压力判断、compact 触发和 UI 预算提示，而不代表 provider 的实际计费。
每轮最多 5 个文件/图片附件，因此媒体预算最多约为 `5 * 2000 = 10000` estimated tokens。compact 后
只保留普通文本占位，并按普通文本计数。`MEDIA_BLOCK_ESTIMATED_TOKENS = 2000` 是内部实现常量，不
暴露为 TOML 配置。

---

## 7. 模块职责

| 模块 | 职责 |
| --- | --- |
| `src/session_tui/at_path.rs` | 词法扫描、错误识别、活动 token 计算、跨折行高亮分段。 |
| `src/session_tui/at_path_completion.rs` | workspace/父目录推导、异步一级目录扫描、候选过滤排序和插入编码。 |
| `src/session_tui/completion_menu.rs` | `@路径` 与 slash command 共用的选择、滚动窗口和单行菜单渲染。 |
| `src/session_tui/bottom_pane/mod.rs` | composer 编辑、菜单可见性、键盘接收、`@路径`/图片占位符高亮与预览目标选择，以及按提交草稿记录输入历史。 |
| `src/session_tui/attachment.rs` | `@路径` 发送前解析、目录上下文、macOS 剪贴板读取、预览文件准备。 |
| `src/session_tui/chat_widget.rs` | 输入事件编排、异步解析任务、附件 steer 改为排队的交互语义。 |
| `src/session_tui/app.rs` | `AppEvent` 回灌、提交顺序控制、目录上下文拼接、系统预览启动。 |
| `src/session_tui/input_queue.rs` | 保留模型文本、可见草稿和附件列表的不同职责。 |
| `src/session_tui/terminal.rs` | 物理终端写入、live region 清理；统一移除动态文本中的 C0/C1 控制字符。 |
| `src/attachment.rs` | 图片/PDF/文本的公共读取、校验和规格化；也供 `file_read` 使用。 |
| `src/api/turn_loop.rs` | 最终附件校验，构造 provider 所需的 user content blocks。 |
| `src/session/turn_journal.rs` | 生成/读取 `canonical_user_message` 哈希并投影旧 journal。 |
| `src/agent/session_engine.rs` | canonical commit 前写哈希事件，并在恢复时据此判断 journal 是否已 canonical 化。 |
| `src/session/mod.rs` | 从 canonical 或 journal projection 生成不含附件展开内容的用户展示文本。 |

所有阻塞文件系统工作遵循运行时边界：补全目录扫描使用 `tokio::fs`；发送前的 metadata/目录列表与 macOS 剪贴板读取使用 `spawn_blocking`；图片 CPU 解码/重采样也在 `spawn_blocking` 中执行。

---

## 8. 测试与验收标准

### 8.1 单元与集成覆盖

实现至少覆盖以下行为：

#### TUI 与交互

- `@` 触发边界、引号、反斜杠、多个 token、空路径和未闭合引号；
- 相对、绝对、`~` 路径的补全父目录计算；
- 候选目录优先、大小写前缀匹配、隐藏文件、受保护 memory 文件、50 条候选上限；
- 目录补全后继续进入下一级，文件补全后关闭菜单；
- 输入解析失败时草稿恢复，异步结果按提交 sequence 顺序处理；
- 异步 `@文件` A 与普通输入 B 混合提交时，B 必须等待 A 的 sequence flush 后才写入历史；取消后连续按 `↑` 先恢复 B、再恢复 A，不出现重复、覆盖或附件/粘贴映射串位；
- 含 `ESC`、Tab、CR、BEL 或 C1 的文件名保留原始 `raw_path`，但候选菜单、composer 与实际终端输出均不包含控制字符，行宽与净化后的输出一致；

#### 附件与 provider

- 文本/PDF/图片的 `@路径` 解析、大小/数量限制、相对 workspace、目录上下文排序，以及最多扫描 1001 项、最多输出 1000 项的截断；
- 文件和剪贴板图片合并计数，删除 `[Image #N]` 即撤销附件；
- 图片实际 media type 与 PDF `%PDF-` 文件头校验；文本文件的 UTF-8 校验，以及 `file_read` 读取文本时既有行号、分页和 keyword window 语义；
- 文本文件在 `<`、`=`、`>` `file_read_max_chars` 时分别完整内联、完整内联和路径降级；多个文本文件分别计限，超限 warning 不终止 turn，也不形成读取许可；
- PDF、磁盘图片和剪贴板图片不应用文本字符上限，继续只受既有大小、数量、格式和图片规格化约束；
- `file_read` 读取图片/PDF 时保留短工具结果，并在内部 meta user message 中追加正确的媒体 block；
- Anthropic image/document 与 OpenAI Chat image/file 的 JSON 映射正确；不支持媒体的 model/provider 错误可透传或包装为明确错误；

#### 持久化与恢复

- 限额内文本附件完整进入 canonical message，但用户显示不含 `Attached file:` 正文；超字符上限的文本只保存路径降级说明，不保存正文；
- 目录上下文进入模型输入，但用户显示不含 `[Referenced directory: ...]` 内容；
- 目录上下文作为展开后的 `user_text` 同时写入 `UserInputAccepted` 与 canonical message；
- `canonical_user_message` 新事件只含 hash，序列化结果不含附件正文；
- 旧版 journal 的完整 content 可投影出同一 hash；
- resume 对带文本附件、媒体附件和目录上下文的 committed turn 只显示一次。
- compact summary、recap、search 与 resume 展示不含媒体 base64；未 compact 的 image/PDF 按每项 2000 estimated tokens 计入上下文预算。
- compact 后完整 tail 超限时，Skill 与附件可外置为 session 内不可变引用；引用不写回 canonical transcript，resume 气泡保持用户原文。

### 8.2 手工 TUI 验收

建议在仓库根目录启动交互 TUI，并确认：

1. 输入 `请检查 @src/s`，候选菜单出现；`Tab` 选择 `src/` 后继续列出下一级；
2. 输入 `@docs/a\ b.md` 或引用一个含空格的路径，发送后模型能收到文件内容；
3. 输入 `请看 @src/`，模型可获得一级列表，用户消息气泡仍只显示 `请看 @src/`；
4. 引用一个较长源文件后退出并 `/resume`，历史中没有第二条附带完整源代码的灰色用户消息；
5. 在 macOS 上用 `Ctrl+O` 预览光标所在附件和全部附件；
6. 在 macOS 上用 `Ctrl+V` 粘贴图片，确认出现 `[Image #N]` 占位；`Command+V` 和非图片 `Ctrl+V` 不会意外加入媒体附件；
7. 路径不存在、未闭合引号、超限文件和受保护 memory 文件均给出明确错误，且原输入可继续修改；
8. 运行中的 turn 输入带附件并按 `Ctrl+Enter` 时，提示其已排队而不是把附件塞进中途 steer。

---

## 9. 后续演进边界

以下能力可以后续单独设计，当前不应通过改变本功能的语义“顺手实现”：

- 为补全加入模糊匹配、Git ignore 规则、最近使用排序或跨目录递归搜索；
- 更丰富的目录快照（类型、大小、树形结构）或可配置的目录深度；
- Windows/Linux 的剪贴板图片和系统文件预览；
- PDF OCR/抽取和更多二进制格式；
- 为 journal 新增独立搜索索引；
- journal compaction 或哈希算法版本升级。若升级哈希算法，必须保留版本前缀并维持对旧版本事件的读取能力。
