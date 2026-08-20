#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

TUI_SESSION="${TUI_SESSION:-acn_tui_live_region}"
TUI_WIDTH="${TUI_WIDTH:-96}"
TUI_HEIGHT="${TUI_HEIGHT:-28}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenarios/live-region}"
TUI_STARTUP_WAIT="${TUI_STARTUP_WAIT:-8}"
TUI_RESIZE_WAIT="${TUI_RESIZE_WAIT:-0.8}"
TUI_BUILD_COMMAND="${TUI_BUILD_COMMAND:-cargo build --quiet --bin acn --example fake_anthropic_sse_server}"

source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

WORKING_PATTERN="Working .*Streaming response|working .*streaming response|thinking"
PROMPT_TEXT="请等待本地流式响应"
FAKE_SERVER_PID=""
FAKE_ACN_HOME=""

cleanup_live_region() {
  tui_cleanup
  if [[ -f "$FAKE_CONFIG" ]]; then
    tui_terminate_owned_supervisors "$FAKE_CONFIG" "$ACN_BINARY" || true
  fi
  if [[ -n "$FAKE_SERVER_PID" ]]; then
    kill "$FAKE_SERVER_PID" >/dev/null 2>&1 || true
    wait "$FAKE_SERVER_PID" 2>/dev/null || true
  fi
  if [[ -n "$FAKE_ACN_HOME" && -d "$FAKE_ACN_HOME" ]]; then
    case "$FAKE_ACN_HOME" in
      "$TUI_OUT_DIR_ABS"/acn-home.*) rm -rf -- "$FAKE_ACN_HOME" ;;
    esac
  fi
}

assert_prompt_before_live_box() {
  local capture="$1"
  awk '
    /› 请等待本地流式响应/ { prompt = NR }
    /Working .*Streaming response|working .*streaming response|thinking/ && box == 0 { box = NR }
    END {
      if (prompt == 0 || box == 0 || prompt >= box) {
        exit 1
      }
    }
  ' "$TUI_OUT_DIR_ABS/$capture.txt" || {
    echo "$capture did not keep the active prompt before the live working box" >&2
    return 1
  }
}

assert_occurrences_at_most() {
  local capture="$1"
  local pattern="$2"
  local max_count="$3"
  local actual_count
  actual_count="$(rg -c "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt" || true)"
  if (( actual_count > max_count )); then
    echo "$capture has $actual_count occurrences of '$pattern', expected at most $max_count" >&2
    return 1
  fi
}

assert_box_inner_rows() {
  local capture="$1"
  local expected="$2"
  local actual
  actual="$(awk '
    /^┌ Working .*Streaming response/ { top = NR; next }
    top > 0 && /^└/ { print NR - top - 1; exit }
  ' "$TUI_OUT_DIR_ABS/$capture.txt")"
  if [[ "$actual" != "$expected" ]]; then
    echo "$capture has ${actual:-no} inner box rows, expected $expected" >&2
    return 1
  fi
}

tui_build_if_needed
ACN_BINARY="$(tui_resolve_binary TUI_ACN_BINARY acn bin)"
FAKE_SERVER_BINARY="$(tui_resolve_binary TUI_FAKE_SERVER_BINARY fake_anthropic_sse_server example)"
TUI_SKIP_BUILD=1

mkdir -p "$TUI_OUT_DIR"
TUI_OUT_DIR_ABS="$(cd "$TUI_OUT_DIR" && pwd)"
FAKE_READY_FILE="$TUI_OUT_DIR_ABS/fake-anthropic.port"
FAKE_CONFIG="$TUI_OUT_DIR_ABS/config.toml"
FAKE_ACN_HOME="$(mktemp -d "$TUI_OUT_DIR_ABS/acn-home.XXXXXX")"
SUPERVISOR_JOBS_CAPTURE="$TUI_OUT_DIR_ABS/supervisor-jobs.txt"
rm -f "$FAKE_READY_FILE"
"$FAKE_SERVER_BINARY" \
  --ready-file "$FAKE_READY_FILE" &
FAKE_SERVER_PID="$!"
# 确保后续配置或 TUI 启动失败时也能回收 fake server。
trap cleanup_live_region EXIT
for _ in $(seq 1 50); do
  [[ -s "$FAKE_READY_FILE" ]] && break
  sleep 0.1
done
if [[ ! -s "$FAKE_READY_FILE" ]]; then
  echo "fake Anthropic server did not become ready" >&2
  cleanup_live_region
  exit 1
fi
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
model = "fake-streaming-model"
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

[agent.session.tui]
live_response_preview_max_lines = 15
EOF
TUI_COMMAND="ACN_FAKE_LLM_API_KEY=test-key '$ACN_BINARY' --config '$FAKE_CONFIG'"

tui_start

INITIAL_SEEN="0"
for _ in $(seq 1 "$TUI_STARTUP_WAIT"); do
  sleep 1
  tui_capture "initial"
  if rg -q "Agent Claim Network|Whisper your wish here|initializing|open" "$TUI_OUT_DIR_ABS/initial.txt"; then
    INITIAL_SEEN="1"
    break
  fi
done
if [[ "$INITIAL_SEEN" != "1" ]]; then
  tui_assert_contains "initial" "Agent Claim Network|Whisper your wish here|initializing|open" "TUI did not start with the expected shell"
fi

tui_send_keys "$PROMPT_TEXT"
sleep 0.1
tui_send_keys Enter
sleep 0.3
tui_capture "during_turn"
tui_assert_contains "during_turn" "$PROMPT_TEXT" "active user prompt is not visible during the running turn"
tui_assert_contains "during_turn" "$WORKING_PATTERN" "live working box is not visible during the running turn"
tui_assert_contains "during_turn" "\\[fake stream\\]" "streaming preview is not visible during the running turn"
assert_occurrences_at_most "during_turn" "$WORKING_PATTERN" 1
assert_prompt_before_live_box "during_turn"
assert_box_inner_rows "during_turn" 15

tmux resize-window -t "$TUI_SESSION" -x 80 -y 22
sleep "$TUI_RESIZE_WAIT"
tui_capture "after_resize"
tui_assert_contains "after_resize" "$WORKING_PATTERN" "live working box disappeared after resize"
assert_occurrences_at_most "after_resize" "$WORKING_PATTERN" 1
assert_box_inner_rows "after_resize" 15

tmux resize-window -t "$TUI_SESSION" -x 110 -y 28
sleep "$TUI_RESIZE_WAIT"
tui_capture "after_expand"
tui_assert_contains "after_expand" "$PROMPT_TEXT" "active user prompt disappeared after expand resize"
tui_assert_contains "after_expand" "$WORKING_PATTERN" "live working box disappeared after expand resize"
assert_occurrences_at_most "after_expand" "$WORKING_PATTERN" 1
assert_prompt_before_live_box "after_expand"
assert_box_inner_rows "after_expand" 15

tmux resize-window -t "$TUI_SESSION" -x 72 -y 12
sleep "$TUI_RESIZE_WAIT"
tui_capture "after_shrink_height"
tui_assert_contains "after_shrink_height" "$WORKING_PATTERN" "live working box disappeared after shrinking height"
assert_occurrences_at_most "after_shrink_height" "$WORKING_PATTERN" 1
assert_box_inner_rows "after_shrink_height" 7

sleep 4
tui_capture "after_commit"
tui_assert_not_contains "after_commit" "$WORKING_PATTERN" "live working box remained after commit"
tui_assert_contains "after_commit" "open" "session did not return to open after commit"

tmux resize-window -t "$TUI_SESSION" -x 110 -y 28
sleep "$TUI_RESIZE_WAIT"
tui_capture "after_commit_expanded"
tui_assert_contains "after_commit_expanded" "\\[fake stream\\] line 119" "committed assistant response is not visible after expanding viewport"
tui_assert_not_contains "after_commit_expanded" "$WORKING_PATTERN" "live working box remained after commit after expanding viewport"

tui_send_keys "/exit" Enter
FINALIZE_SUCCEEDED="0"
for _ in $(seq 1 50); do
  sleep 0.2
  if "$ACN_BINARY" supervisor jobs --config "$FAKE_CONFIG" -l 0 \
    > "$SUPERVISOR_JOBS_CAPTURE" 2>&1 \
    && [[ "$(rg -c '^job_[0-9]' "$SUPERVISOR_JOBS_CAPTURE" || true)" == "1" ]] \
    && rg -q '^job_[^[:space:]]+[[:space:]]+[^[:space:]]+[[:space:]]+session_[0-9a-f]{8}[[:space:]]+succeeded[[:space:]]+' "$SUPERVISOR_JOBS_CAPTURE" \
    && ! rg -q '^job_[^[:space:]]+[[:space:]]+[^[:space:]]+[[:space:]]+session_[0-9a-f]{8}[[:space:]]+(queued|running|failed)[[:space:]]+' "$SUPERVISOR_JOBS_CAPTURE"
  then
    FINALIZE_SUCCEEDED="1"
    break
  fi
done
if [[ "$FINALIZE_SUCCEEDED" != "1" ]]; then
  echo "fake Anthropic server did not complete the finalize job" >&2
  sed -n '1,120p' "$SUPERVISOR_JOBS_CAPTURE" >&2 || true
  exit 1
fi
tui_finish
