#!/usr/bin/env bash
# 验证 turn idle 后 heartbeat 持续刷新 `/ps` / focus，并把后台终态改写回历史 ToolCell。
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

TUI_SESSION="${TUI_SESSION:-acn_idle_heartbeat}"
TUI_WIDTH="${TUI_WIDTH:-126}"
TUI_HEIGHT="${TUI_HEIGHT:-36}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenarios/idle-heartbeat-refresh}"
TUI_BUILD_COMMAND="${TUI_BUILD_COMMAND:-cargo build --quiet --bin acn --example fake_anthropic_sse_server}"

source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

tui_build_if_needed
TUI_SKIP_BUILD=1
mkdir -p "$TUI_OUT_DIR"
TUI_OUT_DIR_ABS="$(cd "$TUI_OUT_DIR" && pwd)"
FAKE_READY_FILE="$TUI_OUT_DIR_ABS/fake-anthropic.port"
FAKE_CONFIG="$TUI_OUT_DIR_ABS/config.toml"
FAKE_ACN_HOME="$(mktemp -d "$TUI_OUT_DIR_ABS/acn-home.XXXXXX")"
FAKE_SERVER_PID=""

cleanup() {
  tui_cleanup
  if [[ -x "$REPO_ROOT/target/debug/acn" && -f "$FAKE_CONFIG" ]]; then
    "$REPO_ROOT/target/debug/acn" supervisor stop --config "$FAKE_CONFIG" >/dev/null 2>&1 || true
  fi
  if [[ "$FAKE_SERVER_PID" =~ ^[0-9]+$ ]]; then
    kill -TERM "$FAKE_SERVER_PID" >/dev/null 2>&1 || true
    wait "$FAKE_SERVER_PID" >/dev/null 2>&1 || true
  fi
  if [[ -d "$FAKE_ACN_HOME" ]]; then
    case "$FAKE_ACN_HOME" in
      "$TUI_OUT_DIR_ABS"/acn-home.*) rm -rf -- "$FAKE_ACN_HOME" ;;
    esac
  fi
}
trap cleanup EXIT

rm -f "$FAKE_READY_FILE"
"$REPO_ROOT/target/debug/examples/fake_anthropic_sse_server" \
  --ready-file "$FAKE_READY_FILE" \
  --response-mode background-process &
FAKE_SERVER_PID="$!"
for _ in $(seq 1 50); do
  [[ -s "$FAKE_READY_FILE" ]] && break
  sleep 0.1
done
[[ -s "$FAKE_READY_FILE" ]] || {
  echo "fake Anthropic server did not become ready" >&2
  exit 1
}
FAKE_PORT="$(cat "$FAKE_READY_FILE")"

cat > "$FAKE_CONFIG" <<EOF
upstream = "dev"

[upstreams.dev]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:$FAKE_PORT"
router_endpoint = "http://127.0.0.1:$FAKE_PORT"

[storage]
acn_home = "$FAKE_ACN_HOME"

[agent.llm]
provider = "anthropic"
endpoint = "http://127.0.0.1:$FAKE_PORT"
model = "fake-background-model"
api_key_env = "ACN_FAKE_LLM_API_KEY"
max_tokens = 4096
context_window = 200000
timeout_secs = 30
retry_count = 0
retry_base_delay_ms = 1
retry_max_delay_ms = 1

[agent.session]
notify_on_finalize_completion = false

[agent.session.memory_review]
interval_turns = 100
EOF

TUI_COMMAND="ACN_FAKE_LLM_API_KEY=test-key target/debug/acn --config '$FAKE_CONFIG'"
tui_start
# tui_start 会覆盖 trap；恢复包含 fake server、supervisor 与临时 home 的完整清理。
trap cleanup EXIT

wait_capture() {
  local name="$1" pattern="$2" description="$3"
  for _ in $(seq 1 80); do
    sleep 0.25
    tui_capture "$name"
    if rg -q "$pattern" "$TUI_OUT_DIR_ABS/$name.txt"; then
      return 0
    fi
  done
  echo "timeout waiting for $description" >&2
  return 1
}

extract_seconds_after() {
  local capture="$1" marker="$2"
  awk -v marker="$marker" '
    {
      for (i = 1; i < NF; i++) {
        if ($i == marker && $(i + 1) ~ /^[0-9]+s$/) {
          value = $(i + 1)
          sub(/s$/, "", value)
          print value
          exit
        }
      }
    }
  ' "$TUI_OUT_DIR_ABS/$capture.txt"
}

extract_process_elapsed_seconds() {
  local capture="$1"
  awk '
    /main.*running/ {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^[0-9]+s$/) {
          value = $i
          sub(/s$/, "", value)
          print value
          exit
        }
      }
    }
  ' "$TUI_OUT_DIR_ABS/$capture.txt"
}

wait_capture "initial" "focus [0-9]+[smh].*open" "TUI open state"
tui_send_keys "start the background process"
sleep 0.1
tui_send_keys Enter
wait_capture "turn_idle" "Background process started" "fake tool turn completion"
tui_assert_contains "turn_idle" "Processes:.*running" "background process footer is absent after turn completion"

tui_send_keys "/ps"
sleep 0.1
tui_send_keys Enter
wait_capture "ps_first" "PROCESS ID.*OWNER.*STATUS.*ELAPSED" "/ps process table"
FIRST_ELAPSED="$(extract_process_elapsed_seconds ps_first)"
sleep 3
tui_capture "ps_second"
SECOND_ELAPSED="$(extract_process_elapsed_seconds ps_second)"
if [[ ! "$FIRST_ELAPSED" =~ ^[0-9]+$ || ! "$SECOND_ELAPSED" =~ ^[0-9]+$ ]] \
  || (( SECOND_ELAPSED <= FIRST_ELAPSED )); then
  echo "/ps elapsed did not advance while turn was idle: first=$FIRST_ELAPSED second=$SECOND_ELAPSED" >&2
  exit 1
fi

tui_send_keys Escape
sleep 0.5
tui_capture "focus_first"
FIRST_FOCUS="$(extract_seconds_after focus_first focus)"
sleep 2
tui_capture "focus_second"
SECOND_FOCUS="$(extract_seconds_after focus_second focus)"
if [[ ! "$FIRST_FOCUS" =~ ^[0-9]+$ || ! "$SECOND_FOCUS" =~ ^[0-9]+$ ]] \
  || (( SECOND_FOCUS <= FIRST_FOCUS )); then
  echo "focus did not advance while turn was idle: first=$FIRST_FOCUS second=$SECOND_FOCUS" >&2
  exit 1
fi

tui_send_keys "/mcp"
sleep 0.1
tui_send_keys Enter
wait_capture "mcp_first" "MCP.*Servers|No MCP servers configured" "/mcp panel"
sleep 2
tui_capture "mcp_second"
tui_assert_contains "mcp_second" "MCP.*Servers|No MCP servers configured" "/mcp panel disappeared during idle heartbeat"

tui_send_keys Escape
tui_send_keys "/ps"
sleep 0.1
tui_send_keys Enter
wait_capture "ps_before_terminate" "main.*running" "background process before user termination"
tui_send_keys t
wait_capture "terminate_confirm" "\[y\] Yes.*\[n/Esc\] No" "/ps terminate confirmation"
tui_send_keys y
# 立即回到聊天页；后续必须由 completion heartbeat 自己重写已经落入原生 scrollback 的
# code_run cell，不能借关闭 panel 的 hard-clear 偶然得到正确结果。
tui_send_keys Escape
wait_capture "background_terminal" "Process terminated: signal 9" "rewritten background code_run terminal result"
tui_assert_not_contains \
  "background_terminal" \
  "Process running in background" \
  "completed background code_run still showed its stale running result"
tui_assert_not_contains \
  "background_terminal" \
  "Background process ID=" \
  "background completion appended a duplicate transcript notification"
if ! rg -n '"kind":"background_process_completed".*"tool_use_id":"toolu_fake_background_process"' \
  "$FAKE_ACN_HOME" > "$TUI_OUT_DIR_ABS/background-completion-journal.txt"; then
  echo "background completion was not persisted to the turn journal" >&2
  exit 1
fi

tui_assert_stderr_empty

echo "idle heartbeat scenario passed: elapsed ${FIRST_ELAPSED}s -> ${SECOND_ELAPSED}s, focus ${FIRST_FOCUS}s -> ${SECOND_FOCUS}s, history completion rewritten and journaled"
