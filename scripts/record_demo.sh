#!/usr/bin/env bash
# 录制 README 首屏的 TUI 演示 GIF：团队模式下借用其他 Agent 的 Claim。
#
# 关键版本（这是本次验证通过的组合，不表示所有工具的最低版本）：
#   Rust/Cargo 1.90.0
#   VHS 0.11.0
#   ttyd 1.7.7_11（命令可能显示 1.7.7-unknown）+ xterm.js 6.0.0
#   zsh 5.9、Expect 5.45.4、jq 1.7.1、ripgrep 14.1.1、FFmpeg 8.1.2
# 要得到不含同步中间帧的 GIF，xterm.js 必须使用支持 DEC 2026 的 6.x；其余工具
# 的小版本通常不影响录制。vhs（brew install vhs）会带上 ttyd 与 ffmpeg，
# expect、jq 需另行安装。
#
# 演示目录固定在 /tmp/acn-demo（不用 $HOME），避免 GIF 里出现本机用户名。
# 起本地 Router 与 Maintainer，并预置另一条 Agent 的 Claim。不读写真实 ~/.acn，
# 也不连接任何真实团队服务。
#
# 用法：
#   source export_env.sh
#   bash scripts/record_demo.sh
#   bash scripts/record_demo.sh /绝对路径/acn-demo.gif
# 默认读取仓库 config.toml 中的 [agent.llm]，需要提前导出 api_key_env 指向的 key。
# 可用 ACN_DEMO_LLM_CONFIG 指定另一份 LLM 配置；可用 ACN_DEMO_TTYD_BIN_DIR 指定
# 包含 xterm.js 6 wrapper 的目录。运行 `bash scripts/record_demo.sh --help` 查看摘要。
#
# 关于闪烁：ACN 会把一整帧 ANSI 合并后再提交，并使用 DEC 2026 同步刷新。录制时
# 优先让 vhs 使用 target/ttyd-xterm6/bin 下支持 DEC 2026 的前端；也可通过
# ACN_DEMO_TTYD_BIN_DIR 指向另一份 wrapper。没有 wrapper 时仍可使用系统 ttyd，
# 但旧版 xterm.js 可能拍到同步帧的中间态（见脚本末尾的手录说明）。
#
# 录制流程：
#   1. 构建当前源码的 acn、acn-router、acn-maintainer。
#   2. 重建 /tmp/acn-demo，在 main 分支准备演示数据、独立 Agent home 与团队 home。
#   3. 启动本地 Router/Maintainer，等待预置 Claim 完成索引。
#   4. VHS 负责 GIF 抓屏，Expect 提供 138x41 PTY 并原样转发 ACN 的 ANSI；不套 tmux。
#   5. 后台每 2 秒检查 messages.jsonl；最终 assistant 回复不再含 tool_use 时发送 /exit。
#   6. 轮询 supervisor jobs，finalize 开始或完成后结束录制并核验工具、文件、颜色和 stderr。

set -euo pipefail

usage() {
  cat <<'USAGE'
用法：
  source export_env.sh
  bash scripts/record_demo.sh [输出 GIF 路径]

默认输出：
  docs/assets/acn-demo.gif

可选环境变量：
  ACN_DEMO_LLM_CONFIG     LLM 配置来源，默认使用仓库 config.toml
  ACN_DEMO_TTYD_BIN_DIR   xterm.js 6 ttyd wrapper 所在目录，
                          默认使用 target/ttyd-xterm6/bin

已验证录制组合：
  Rust/Cargo 1.90.0；VHS 0.11.0；ttyd 1.7.7_11；
  xterm.js 6.0.0；zsh 5.9；Expect 5.45.4；jq 1.7.1；
  ripgrep 14.1.1；FFmpeg 8.1.2

脚本会删除并重建 /tmp/acn-demo，但不会读写真实 ~/.acn。
USAGE
}

case "${1:-}" in
  -h | --help)
    usage
    exit 0
    ;;
esac
if (($# > 1)); then
  usage >&2
  exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT="${1:-$REPO_ROOT/docs/assets/acn-demo.gif}"
# 不用 $HOME/…，否则 Type 命令和工具 cwd 会把用户名写进画面。
DEMO_ROOT="/tmp/acn-demo"
ACN_HOME="$DEMO_ROOT/.acn-data"
UPSTREAM="team"
# 团队服务使用独立 home，让演示中的 Agent 私有数据与团队数据边界一目了然。
TEAM_HOME="$DEMO_ROOT/.team-data"
TEAM_CONFIG="$DEMO_ROOT/team-config.toml"
# daemon 的团队数据根目录是 <acn_home>/data/team，不带 upstream 段。
TEAM_CLAIMS="$TEAM_HOME/data/team/agents/agent-lin/claims"
TAPE="$REPO_ROOT/target/demo.tape"
DEMO_MODEL="deepseek-v4-flash"
DEMO_AGENT_ID="agent-demo"
DEMO_WATCHER_LOG="$DEMO_ROOT/watcher.log"
DEMO_ANSI_CAPTURE="$DEMO_ROOT/tui-output.ansi"
DEMO_AGENT_STDERR="$DEMO_ROOT/agent.stderr"
DEMO_FINAL_MESSAGES="$DEMO_ROOT/final-messages.jsonl"
DEMO_HARNESS_PID="$DEMO_ROOT/harness.pid"
DEMO_FINALIZE_READY="$DEMO_ROOT/finalize.ready"
DEMO_BIN_DIR="$DEMO_ROOT/.demo-bin"
DEMO_TTYD_BIN_DIR="${ACN_DEMO_TTYD_BIN_DIR:-$REPO_ROOT/target/ttyd-xterm6/bin}"
DEMO_TURN_TIMEOUT_SECS=300
DEMO_FINALIZE_TIMEOUT_SECS=120
LLM_CONFIG_SOURCE="${ACN_DEMO_LLM_CONFIG:-$REPO_ROOT/config.toml}"
ROUTER_PORT=18061
MAINTAINER_PORT=18062

# crossterm 在 NO_COLOR 下会丢掉全部前景色，而 ACN 仍会画出自己的浅色 surface，
# 录出来就是米白底配终端默认字色。录制必须在允许颜色的环境里进行。
unset NO_COLOR
export TERM=xterm-256color

for command in vhs ttyd zsh expect jq rg nc lsof git cargo; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required to record the demo" >&2
    exit 127
  fi
done
DEMO_VHS_PATH="$DEMO_BIN_DIR:$PATH"
if [[ -x "$DEMO_TTYD_BIN_DIR/ttyd" ]]; then
  DEMO_VHS_PATH="$DEMO_BIN_DIR:$DEMO_TTYD_BIN_DIR:$PATH"
else
  echo "warning: xterm.js 6 ttyd wrapper not found: $DEMO_TTYD_BIN_DIR/ttyd" >&2
  echo "the recording may contain intermediate frames from synchronized updates" >&2
fi

ACN_BIN="$REPO_ROOT/target/debug/acn"
ROUTER_BIN="$REPO_ROOT/target/debug/acn-router"
MAINTAINER_BIN="$REPO_ROOT/target/debug/acn-maintainer"
echo "building acn, acn-router and acn-maintainer..."
(cd "$REPO_ROOT" && cargo build --bin acn --bin acn-router --bin acn-maintainer)

for port in "$ROUTER_PORT" "$MAINTAINER_PORT"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $port is already in use; refusing to disturb existing process" >&2
    exit 1
  fi
done

if [[ ! -f "$LLM_CONFIG_SOURCE" ]]; then
  echo "LLM config source does not exist: $LLM_CONFIG_SOURCE" >&2
  exit 1
fi

rm -rf -- "$DEMO_ROOT"
mkdir -p "$ACN_HOME" "$TEAM_HOME" "$TEAM_CLAIMS" "$DEMO_BIN_DIR"
git init -q --initial-branch=main "$DEMO_ROOT"

# 沿用仓库开发配置中的 provider、endpoint 与 key 环境变量；可用
# ACN_DEMO_LLM_CONFIG 覆盖来源。演示模型固定为响应更快的版本。
awk -v demo_model="$DEMO_MODEL" '
  /^\[/ { section = $0 }
  section ~ /^\[agent\.llm\]$/ {
    if ($0 ~ /^model[[:space:]]*=/) {
      print "model = \"" demo_model "\""
      model_written = 1
    } else {
      print
    }
  }
  END {
    if (!model_written) {
      exit 2
    }
  }
' "$LLM_CONFIG_SOURCE" > "$DEMO_ROOT/llm.toml"

if [[ ! -s "$DEMO_ROOT/llm.toml" ]]; then
  echo "failed to extract [agent.llm] with model from $LLM_CONFIG_SOURCE" >&2
  exit 1
fi
LLM_API_KEY_ENV="$(
  awk -F= '
    /^api_key_env[[:space:]]*=/ {
      value = $2
      gsub(/^[[:space:]"]+|[[:space:]"]+$/, "", value)
      print value
      exit
    }
  ' "$DEMO_ROOT/llm.toml"
)"
if [[ ! "$LLM_API_KEY_ENV" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "invalid or missing [agent.llm].api_key_env in $LLM_CONFIG_SOURCE" >&2
  exit 1
fi
if [[ -z "${!LLM_API_KEY_ENV:-}" ]]; then
  echo "required LLM key environment variable is not set: $LLM_API_KEY_ENV" >&2
  exit 1
fi

{
  cat <<EOF
upstream = "$UPSTREAM"

[upstreams.$UPSTREAM]
agent_id = "$DEMO_AGENT_ID"
acn_key_env = ""
maintainer_endpoint = "http://127.0.0.1:$MAINTAINER_PORT"
router_endpoint = "http://127.0.0.1:$ROUTER_PORT"

[storage]
acn_home = "$ACN_HOME"

[agent.session.tui]
live_response_preview_max_lines = -1

EOF
  cat "$DEMO_ROOT/llm.toml"
} > "$ACN_HOME/config.toml"

{
  cat <<EOF
upstream = "$UPSTREAM"

[upstreams.$UPSTREAM]
agent_id = "team-service"
acn_key_env = ""
maintainer_endpoint = "http://127.0.0.1:$MAINTAINER_PORT"
router_endpoint = "http://127.0.0.1:$ROUTER_PORT"

[storage]
acn_home = "$TEAM_HOME"

[router]
refresh_interval_secs = 5

[router.daemon]
listen = "127.0.0.1:$ROUTER_PORT"

[router.auth.team]
enabled = false

# 演示只走 lexical 召回：没有配 [router.embedding] 时向量检索会自动跳过。
# vector_top_m 仍需为正数才能通过配置校验。
[router.retrieval]
enabled = true
lexical_top_n = 16
vector_top_m = 16
top_k = 8
rerank_enabled = false

[maintainer.daemon]
listen = "127.0.0.1:$MAINTAINER_PORT"

[maintainer.auth.admin]
enabled = false

[maintainer.auth.team]
enabled = false

EOF
  cat "$DEMO_ROOT/llm.toml"
} > "$TEAM_CONFIG"
rm -f "$DEMO_ROOT/llm.toml"

# 预置一条属于另一个 Agent 的团队 Claim，让 consult_router 能召回真实结果。
cat > "$TEAM_CLAIMS/claim_9f3c21a7.yaml" <<'CLAIM'
id: claim_9f3c21a7
name: 西部收入每日 09:30 后完成回填
statement: 西部大区的 revenue 由第三方对账系统回填，每天 09:30 之后才完整。在此之前导出的销售明细里，西部当日 revenue 会是空值或非数字占位符，不能直接计入日报总收入，应标记为暂定并在回填后重新核算。
scope: sales-reporting/data-quality
holder: agent-lin
confidence: high
status: active
created_at: 2026-07-14T02:11:40Z
source_claim_ids: []
evidence_summary: 连续三周比对 08:00 与 10:00 两次导出的西部明细，09:30 前缺失率 100%，之后恢复正常；与数据平台确认为第三方对账回填时序所致。
CLAIM

cat > "$TEAM_CLAIMS/claim_4d81b0e2.yaml" <<'CLAIM'
id: claim_4d81b0e2
name: 缺失字段的日报合计必须标注暂定
statement: 日报中任何包含未回填字段的合计值都必须显式标注为暂定，并写明缺失的大区与字段，便于第二天复核时定位差异。
scope: sales-reporting/conventions
holder: agent-lin
confidence: medium
status: active
created_at: 2026-07-16T06:35:12Z
source_claim_ids:
- claim_9f3c21a7
evidence_summary: 6 月两次日报因未标注暂定合计导致下游复核返工，此后团队约定统一标注口径。
CLAIM

cat > "$DEMO_ROOT/sales_2026-07.csv" <<'CSV'
date,region,orders,revenue
2026-07-20,east,412,183400
2026-07-21,east,398,176900
2026-07-22,east,455,201300
2026-07-20,west,221,PENDING
2026-07-21,west,236,104800
2026-07-22,west,244,109200
CSV

cat > "$DEMO_ROOT/daily_report.md" <<'MD'
# 2026 年 7 月销售日报

## 结论

（待补）

## 数据

明细见 sales_2026-07.csv。
MD

cat > "$DEMO_ROOT/.gitignore" <<'GITIGNORE'
.acn-data/
.team-data/
.demo-bin/
*.log
*.stderr
*.pid
team-config.toml
watcher.log
watcher.pid
final-messages.jsonl
finalize.ready
tui-output.ansi
GITIGNORE

git -C "$DEMO_ROOT" add .gitignore daily_report.md sales_2026-07.csv
git \
  -C "$DEMO_ROOT" \
  -c user.name="ACN Demo" \
  -c user.email="demo@localhost" \
  -c commit.gpgSign=false \
  commit -qm "demo baseline"

cleanup() {
  if [[ -f "$DEMO_ROOT/watcher.pid" ]]; then
    kill "$(cat "$DEMO_ROOT/watcher.pid")" 2>/dev/null || true
  fi
  if [[ -f "$DEMO_HARNESS_PID" ]]; then
    kill "$(cat "$DEMO_HARNESS_PID")" 2>/dev/null || true
  fi
  for pid_file in "$DEMO_ROOT/router.pid" "$DEMO_ROOT/maintainer.pid"; do
    if [[ -f "$pid_file" ]]; then
      kill "$(cat "$pid_file")" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

"$ROUTER_BIN" --config "$TEAM_CONFIG" > "$DEMO_ROOT/router.log" 2>&1 &
echo $! > "$DEMO_ROOT/router.pid"
"$MAINTAINER_BIN" --config "$TEAM_CONFIG" > "$DEMO_ROOT/maintainer.log" 2>&1 &
echo $! > "$DEMO_ROOT/maintainer.pid"

for port in "$ROUTER_PORT" "$MAINTAINER_PORT"; do
  for _ in $(seq 1 60); do
    if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  if ! nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
    echo "service did not open port $port; see $DEMO_ROOT/*.log" >&2
    exit 1
  fi
done

# 必须等到 Router 真的把预置 Claim 索引进派生视图，否则录出来的 consult_router 会是空结果。
DERIVED_VIEWS="$TEAM_HOME/data/team/router/derived_views.yaml"
for _ in $(seq 1 60); do
  if [[ -f "$DERIVED_VIEWS" ]] && rg -q '^  - scope: sales-reporting/' "$DERIVED_VIEWS"; then
    break
  fi
  sleep 1
done
if ! rg -q '^  - scope: sales-reporting/' "$DERIVED_VIEWS" 2>/dev/null; then
  echo "router did not index the seeded claims; see $DEMO_ROOT/router.log" >&2
  exit 1
fi
echo "router indexed scopes: $(rg -o '^  - scope: (.*)' -r '$1' "$DERIVED_VIEWS" | paste -sd' ' -)"

mkdir -p "$(dirname "$TAPE")" "$(dirname "$OUTPUT")"

# 录制时不能在 ACN 和 xterm.js 之间再套一层 tmux：大帧超过 PTY 缓冲后，
# tmux 会分批解析并转发，外层终端仍可能看到清屏后的半帧。expect 只负责创建
# 固定尺寸 PTY、透传字节和接收后台信号，不解析 ANSI，因此 DEC 2026 能直达 xterm.js。
cat > "$DEMO_BIN_DIR/run-real-acn" <<EOF
#!/usr/bin/env bash
exec "$ACN_BIN" "\$@" 2>"$DEMO_AGENT_STDERR"
EOF
chmod +x "$DEMO_BIN_DIR/run-real-acn"

cat > "$DEMO_BIN_DIR/acn" <<EOF
#!/usr/bin/expect -f
set timeout -1
set pid_file [open "$DEMO_HARNESS_PID" w]
puts \$pid_file [pid]
close \$pid_file
log_file -noappend "$DEMO_ANSI_CAPTURE"
spawn -noecho "$DEMO_BIN_DIR/run-real-acn" {*}\$argv
stty rows 41 columns 138
trap {
    catch {send -i \$spawn_id -- "/exit\\r"}
} SIGUSR1
interact
catch {wait}
set deadline [expr {[clock seconds] + $DEMO_FINALIZE_TIMEOUT_SECS}]
while {![file exists "$DEMO_FINALIZE_READY"]} {
    if {[clock seconds] >= \$deadline} {
        puts stderr "timed out waiting for finalize observer"
        exit 1
    }
    after 200
}
file delete -force "$DEMO_HARNESS_PID"
EOF
chmod +x "$DEMO_BIN_DIR/acn"

watch_demo_session() {
  local sessions_root="$ACN_HOME/$UPSTREAM/data/agents/$DEMO_AGENT_ID/sessions"
  local messages_path=""
  local session_id=""
  local deadline=$((SECONDS + DEMO_TURN_TIMEOUT_SECS))

  while ((SECONDS < deadline)); do
    messages_path="$(
      find "$sessions_root" -mindepth 2 -maxdepth 2 -type f -name messages.jsonl -print -quit \
        2>/dev/null || true
    )"
    if [[ -n "$messages_path" ]] \
      && jq -s -e '
        length >= 2
        and .[0].index == 0
        and .[0].role == "user"
        and .[-1].role == "assistant"
        and ([.[-1].content[]? | select(.type == "tool_use")] | length == 0)
      ' "$messages_path" >/dev/null 2>&1; then
      session_id="$(basename "$(dirname "$messages_path")")"
      echo "first turn committed: $session_id"
      cp "$messages_path" "$DEMO_FINAL_MESSAGES"
      # messages.jsonl 的提交略早于终端完成最后一帧绘制；最终回复完整显示后再停留
      # 几秒，让观看者有时间阅读结果，然后通知 expect 向真实 TUI PTY 注入 /exit。
      sleep 10
      if [[ ! -s "$DEMO_HARNESS_PID" ]] \
        || ! kill -USR1 "$(cat "$DEMO_HARNESS_PID")" 2>/dev/null; then
        echo "failed to signal the expect harness" >&2
        return 1
      fi
      break
    fi
    sleep 2
  done

  if [[ -z "$session_id" ]]; then
    echo "timed out waiting for the first committed turn" >&2
    return 1
  fi

  deadline=$((SECONDS + DEMO_FINALIZE_TIMEOUT_SECS))
  while ((SECONDS < deadline)); do
    local jobs_output
    local job_status
    jobs_output="$("$ACN_BIN" supervisor jobs --config "$ACN_HOME/config.toml" --limit 0 2>&1 || true)"
    job_status="$(
      awk -v session_id="$session_id" '
        NR > 2 && $3 == session_id {
          print $4
          exit
        }
      ' <<<"$jobs_output"
    )"
    case "$job_status" in
      running)
        echo "finalize running: $session_id"
        : > "$DEMO_FINALIZE_READY"
        return 0
        ;;
      succeeded | failed)
        # 极快的 finalize 可能在两次轮询之间直接进入终态；此时同样结束录制。
        echo "finalize already $job_status before the next poll: $session_id"
        : > "$DEMO_FINALIZE_READY"
        return 0
        ;;
    esac
    sleep 2
  done

  echo "timed out waiting for finalize to start: $session_id" >&2
  return 1
}

watch_demo_session >"$DEMO_WATCHER_LOG" 2>&1 &
echo $! > "$DEMO_ROOT/watcher.pid"

cat > "$TAPE" <<TAPE
Output "$OUTPUT"

# macOS 自带 bash 仍是 3.2，默认提示符可能显示成 bash-3.2\$。VHS 的 zsh 模式
# 使用 --no-rcs，不读取个人 .zshrc，并提供固定的简洁提示符，保证录制环境一致。
Set Shell zsh
Set FontSize 16
Set Width 1320
Set Height 760
Set Padding 14
Set Framerate 12
Set PlaybackSpeed 2
Set TypingSpeed 45ms
Set WaitTimeout 7m
# 录制里不需要闪烁光标：ACN 重绘 live region 时光标会被移动，闪烁相位会被逐帧拍下来。
Set CursorBlink false

# 必须是一套完整的「浅色终端」调色板，而不是只把 background/foreground 调浅。
# TUI 的部分元素用的是 ANSI 调色板色而非 RGB，例如用户气泡是
# fg(Color::Black) + bg(Color::Gray)（见 src/session_tui/cell.rs），
# 所以 white/black 必须保持各自应有的明暗关系，否则灰条会变成深底深字。
Set Theme { "background": "#faf8f2", "foreground": "#232321", "cursor": "#ce7a58", "selection": "#e8e2d6", "black": "#232321", "brightBlack": "#6f6a64", "red": "#97322d", "brightRed": "#c8503f", "green": "#1e6031", "brightGreen": "#2f8a4a", "yellow": "#8a6a1e", "brightYellow": "#b08a2a", "blue": "#3571b1", "brightBlue": "#5a92cc", "magenta": "#7a4a8c", "brightMagenta": "#9a6aac", "cyan": "#2a7a8a", "brightCyan": "#4a9aaa", "white": "#c8c4bc", "brightWhite": "#f2f0ea" }

# 进入演示目录、启动命令都用相对路径，画面里不要出现绝对路径。
Hide
Type "cd /tmp/acn-demo && clear"
Enter
Sleep 1s
Show

Type "acn --config .acn-data/config.toml"
Sleep 300ms
Enter
Sleep 2s

Type "根据 sales_2026-07.csv 更新 daily_report.md。西部收入有一项异常，先看看团队里有没有现成结论，再按团队口径处理；报告里只写业务结论，完成后用两三句话说明结果。"
Sleep 1s
Enter
# 后台观察者在 messages.jsonl 出现首轮完整回复后发送 /exit，并在对应 finalize
# 进入 running 后放行 expect wrapper；回到 VHS shell 的提示符即结束录制。
# VHS 的 zsh 模式使用简洁的大于号提示符；美元符仍保留在匹配中，兼容自定义 shell。
Wait+Line /[>\$] ?$/
TAPE

echo "recording -> $OUTPUT"
PATH="$DEMO_VHS_PATH" COLORTERM=truecolor vhs "$TAPE"
watcher_status=0
wait "$(cat "$DEMO_ROOT/watcher.pid")" || watcher_status=$?
if ((watcher_status != 0)); then
  echo "demo watcher failed; see $DEMO_WATCHER_LOG" >&2
  exit "$watcher_status"
fi

if [[ -s "$DEMO_AGENT_STDERR" ]]; then
  echo "recorded ACN wrote to stderr: $DEMO_AGENT_STDERR" >&2
  exit 1
fi
for tool_name in file_read consult_router; do
  if ! rg -q "\"name\":\"$tool_name\"" "$DEMO_FINAL_MESSAGES"; then
    echo "recorded session did not call $tool_name: $DEMO_FINAL_MESSAGES" >&2
    exit 1
  fi
done
if ! rg -q '"name":"file_(patch|write)"' "$DEMO_FINAL_MESSAGES"; then
  echo "recorded session did not modify the report with a file tool" >&2
  exit 1
fi
if rg -q '（待补）' "$DEMO_ROOT/daily_report.md"; then
  echo "recorded session did not update daily_report.md" >&2
  exit 1
fi
if rg -q 'claim_[[:alnum:]_]+|agent-lin|sales-reporting/' "$DEMO_ROOT/daily_report.md"; then
  echo "recorded report leaked internal Claim metadata: $DEMO_ROOT/daily_report.md" >&2
  exit 1
fi

# 终态画面必须保留 ACN 主题中的 24 位语义色。这里同时检查前景和背景，
# 防止录制环境变化后悄悄把 diff 再次降成灰阶。
for required_ansi in \
  $'\033[38;2;30;96;49m' \
  $'\033[48;2;225;242;228m' \
  $'\033[38;2;151;50;45m' \
  $'\033[48;2;249;228;225m' \
  $'\033[48;2;250;248;242m'; do
  if ! LC_ALL=C rg -Fq "$required_ansi" "$DEMO_ANSI_CAPTURE"; then
    echo "recorded TUI did not preserve an expected RGB style; see $DEMO_ANSI_CAPTURE" >&2
    exit 1
  fi
done

echo "done: $(du -h "$OUTPUT" | cut -f1) $OUTPUT"
echo
echo "提示：若 GIF 仍有抓屏伪影，可对比系统 Terminal 中的实际 TUI，并手动："
echo "  1) 保留本次已启动的服务，或重新跑到 daemon ready 后 Ctrl-C 掉 vhs 段"
echo "  2) 在系统 Terminal 中：cd /tmp/acn-demo && acn --config .acn-data/config.toml"
echo "  3) 用 Kap / CleanShot / asciinema+agg 录该窗口"
