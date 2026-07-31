#!/usr/bin/env bash
# 验证 write_stdin 同批去重、硬终止 outcome、自然非零退出与不同进程并行控制。
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

TUI_SESSION="${TUI_SESSION:-acn_process_control}"
TUI_WIDTH="${TUI_WIDTH:-180}"
TUI_HEIGHT="${TUI_HEIGHT:-72}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenarios/process-control}"
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

wait_capture() {
  local name="$1" pattern="$2" description="$3"
  for _ in $(seq 1 160); do
    sleep 0.25
    tui_capture "$name"
    if rg -q "$pattern" "$TUI_OUT_DIR_ABS/$name.txt"; then
      return 0
    fi
  done
  echo "timeout waiting for $description" >&2
  return 1
}

assert_min_count() {
  local capture="$1" pattern="$2" minimum="$3" description="$4"
  local count
  count="$(rg -c "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt" || true)"
  if (( count < minimum )); then
    echo "$description: expected at least $minimum, got $count" >&2
    return 1
  fi
}

rm -f "$FAKE_READY_FILE"
"$REPO_ROOT/target/debug/examples/fake_anthropic_sse_server" \
  --ready-file "$FAKE_READY_FILE" \
  --response-mode process-control &
FAKE_SERVER_PID="$!"
for _ in $(seq 1 50); do
  [[ -s "$FAKE_READY_FILE" ]] && break
  sleep 0.1
done
[[ -s "$FAKE_READY_FILE" ]] || {
  echo "fake Anthropic server did not become ready" >&2
  exit 1
}
FAKE_PORT="$(<"$FAKE_READY_FILE")"

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
model = "fake-process-control-model"
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
# tui_start 会安装自己的 trap；恢复包含 fake server、supervisor 与临时 home 的清理。
trap cleanup EXIT

wait_capture "initial" "type / for commands.*Enter sends" "TUI open state"
tui_send_keys "run the deterministic process-control regression"
sleep 0.1
tui_send_keys Enter

wait_capture "completed" "PROCESS_CONTROL_RESULT" "process-control tool loop completion"
tui_assert_contains \
  "completed" \
  "PROCESS_CONTROL_RESULT duplicate_poll_ok=true duplicate_terminate_rejected=true survivor_running=true" \
  "provider did not observe the expected duplicate-call semantics"
tui_assert_contains \
  "completed" \
  "terminate_ok=true natural_failed=true pair_polls_ok=true" \
  "provider did not observe the expected terminate/exit/pair-poll semantics"
tui_assert_contains \
  "completed" \
  "pair_terminates_ok=true no_live_processes=true" \
  "provider did not observe pair cleanup and an empty final process list"
tui_assert_contains \
  "completed" \
  "already called for this process" \
  "same-process duplicate write_stdin was not rejected visibly"
tui_assert_contains \
  "completed" \
  "Process exit code: 7" \
  "natural nonzero process exit was not rendered with its exit code"
tui_assert_not_contains \
  "completed" \
  "Process exit: unavailable" \
  "hard termination regressed to an unavailable process exit"
assert_min_count \
  "completed" \
  "Process terminated: signal 9" \
  3 \
  "hard termination signal rendering is incomplete"
assert_min_count \
  "completed" \
  "Called code_run" \
  4 \
  "not all code_run fixtures were visible"
assert_min_count \
  "completed" \
  "Called write_stdin" \
  7 \
  "not all write_stdin calls were visible"

for _ in $(seq 1 20); do
  sleep 0.25
  tui_capture "settled"
  if ! rg -q "Processes: [1-9][0-9]* running" "$TUI_OUT_DIR_ABS/settled.txt"; then
    break
  fi
done
tui_assert_not_contains \
  "settled" \
  "Processes: [1-9][0-9]* running" \
  "TUI process footer did not converge after all managed processes exited"
tui_assert_stderr_empty

echo "process-control TUI regression passed: $TUI_OUT_DIR_ABS"
