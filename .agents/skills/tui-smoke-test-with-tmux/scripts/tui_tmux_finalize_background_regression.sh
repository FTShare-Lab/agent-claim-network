#!/usr/bin/env bash
# 验证 `/exit` 在 heartbeat 前收束 live main process 时，最终 scrollback 与 journal 都已更新。
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

TUI_SESSION="${TUI_SESSION:-acn_finalize_background}"
TUI_WIDTH="${TUI_WIDTH:-126}"
TUI_HEIGHT="${TUI_HEIGHT:-36}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenarios/finalize-background}"
TUI_BUILD_COMMAND="${TUI_BUILD_COMMAND:-cargo build --quiet --bin acn --example fake_anthropic_sse_server}"

source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"
tui_build_if_needed
ACN_BINARY="$(tui_resolve_binary TUI_ACN_BINARY acn bin)"
FAKE_SERVER_BINARY="$(tui_resolve_binary TUI_FAKE_SERVER_BINARY fake_anthropic_sse_server example)"
TUI_SKIP_BUILD=1
mkdir -p "$TUI_OUT_DIR"
TUI_OUT_DIR_ABS="$(cd "$TUI_OUT_DIR" && pwd)"
FAKE_READY_FILE="$TUI_OUT_DIR_ABS/fake-anthropic.port"
FAKE_CONFIG="$TUI_OUT_DIR_ABS/config.toml"
FAKE_ACN_HOME="$(mktemp -d "$TUI_OUT_DIR_ABS/acn-home.XXXXXX")"
FAKE_SERVER_PID=""

cleanup() {
  tui_cleanup
  if [[ -f "$FAKE_CONFIG" ]]; then
    tui_terminate_owned_supervisors "$FAKE_CONFIG" "$ACN_BINARY" || true
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
"$FAKE_SERVER_BINARY" \
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

TUI_COMMAND="ACN_FAKE_LLM_API_KEY=test-key '$ACN_BINARY' --config '$FAKE_CONFIG'"
tui_start

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

wait_capture "initial" "focus [0-9]+[smh].*open" "TUI open state"
tui_send_keys "start the background process"
sleep 0.1
tui_send_keys Enter
wait_capture "background_running" "Process running in background" "background tool result"

# 保留退出后的 pane，捕获 TUI 最后一帧和原生 scrollback。
tmux set-option -w -t "$TUI_SESSION" remain-on-exit on
tui_send_keys "/exit"
tui_send_keys Enter
for _ in $(seq 1 80); do
  sleep 0.25
  if [[ "$(tmux display-message -p -t "$TUI_SESSION" '#{pane_dead}')" == "1" ]]; then
    break
  fi
done
if [[ "$(tmux display-message -p -t "$TUI_SESSION" '#{pane_dead}')" != "1" ]]; then
  echo "TUI did not exit after /exit" >&2
  exit 1
fi
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$TUI_OUT_DIR_ABS/finalize_exit.txt"
tui_assert_contains \
  "finalize_exit" \
  "Process terminated: signal 9" \
  "/exit did not rewrite the background cell to signal 9"
tui_assert_not_contains \
  "finalize_exit" \
  "Process running in background" \
  "/exit left a stale running background cell in final scrollback"
tui_assert_not_contains \
  "finalize_exit" \
  "Background process ID=" \
  "/exit appended a standalone background completion notification"
if ! rg -n '"kind":"background_process_completed".*"tool_use_id":"toolu_fake_background_process".*"signal":9' \
  "$FAKE_ACN_HOME" > "$TUI_OUT_DIR_ABS/background-completion-journal.txt"; then
  echo "/exit completion was not persisted to the turn journal" >&2
  exit 1
fi
tui_assert_stderr_empty

echo "finalize-background TUI regression passed: /exit rewrote and journaled the live process completion before exit"
