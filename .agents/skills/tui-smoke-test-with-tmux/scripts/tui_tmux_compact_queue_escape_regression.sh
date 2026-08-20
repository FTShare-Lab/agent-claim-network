#!/usr/bin/env bash
# 验证 compact 期间 Esc 先逐条取回 queued input，队列清空后才转交中断语义。
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

TUI_SESSION="${TUI_SESSION:-acn_compact_queue_escape}"
TUI_WIDTH="${TUI_WIDTH:-160}"
TUI_HEIGHT="${TUI_HEIGHT:-72}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenarios/compact-queue-escape}"
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

wait_capture() {
  local name="$1" pattern="$2" description="$3"
  for _ in $(seq 1 160); do
    sleep 0.1
    tui_capture "$name"
    if rg -q "$pattern" "$TUI_OUT_DIR_ABS/$name.txt"; then
      return 0
    fi
  done
  echo "timeout waiting for $description" >&2
  return 1
}

send_prompt() {
  local prompt_path="$TUI_OUT_DIR_ABS/prompt.txt"
  printf '%s' "$1" > "$prompt_path"
  tmux load-buffer -b "${TUI_SESSION}_prompt" "$prompt_path"
  tmux paste-buffer -t "$TUI_SESSION" -b "${TUI_SESSION}_prompt"
  sleep 0.2
  tui_send_keys C-m
}

rm -f "$FAKE_READY_FILE"
"$FAKE_SERVER_BINARY" \
  --ready-file "$FAKE_READY_FILE" \
  --response-mode slow-structured &
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
model = "fake-compact-queue-model"
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

wait_capture "initial" "type / for commands.*Enter sends" "TUI open state"
tui_assert_contains \
  "initial" \
  "session_[0-9a-f]{8} type / for commands.*Enter sends" \
  "open footer did not retain the session id"

# manual compact 至少保留最近三轮；先制造足够的 committed history，确保进入真实压缩。
for turn in $(seq 1 5); do
  prompt="compact queue history $turn"
  send_prompt "$prompt"
  wait_capture \
    "history_${turn}_running" \
    "Working · Streaming response" \
    "history turn $turn running state"
  tui_assert_contains \
    "history_${turn}_running" \
    "session_[0-9a-f]{8} Enter queues" \
    "running footer did not retain the session id"
  wait_capture "history_$turn" "┌ Idle" "history turn $turn completion"
  tui_assert_contains "history_$turn" "$prompt" "history turn $turn user input was absent"
  tui_assert_contains \
    "history_$turn" \
    "fake compact queue turn completed" \
    "history turn $turn assistant response was absent"
done

send_prompt "/compact"
wait_capture "compacting" "Compacting · Session history" "manual compact running state"
tui_assert_contains \
  "compacting" \
  "session_[0-9a-f]{8} input will be queued" \
  "compacting footer did not retain the session id"

send_prompt "first queued"
# 两次 tmux paste 紧邻时，终端 reader 可能把它们合并成一条多行 paste；先观察到第一条
# 已成为独立 queued input，再提交第二条，才能稳定验证逐条取回顺序。
wait_capture \
  "first_input_queued" \
  "queued: first queued$" \
  "first compact-time input entering the queue"
tui_assert_contains \
  "first_input_queued" \
  "Compacting · Session history" \
  "compaction finished before the first queued input became visible"

send_prompt "second queued"
wait_capture \
  "both_inputs_queued" \
  "queued\(2\): first queued \| second queued" \
  "both inputs entering the compact-time queue"
tui_assert_contains \
  "both_inputs_queued" \
  "Compacting · Session history" \
  "compaction finished before the queued-input Escape check"

tui_send_keys Escape
wait_capture \
  "after_first_escape" \
  "^› second queued" \
  "first Esc did not restore the latest queued input"
tui_assert_not_contains \
  "after_first_escape" \
  "Session task is running" \
  "first Esc reached session-task interruption before restoring queued input"

tui_send_keys Escape
wait_capture \
  "after_second_escape" \
  "^› first queued" \
  "second Esc did not restore the remaining queued input"
tui_assert_not_contains \
  "after_second_escape" \
  "Session task is running" \
  "second Esc reached session-task interruption before emptying the queue"

tui_send_keys Escape
wait_capture \
  "after_third_escape" \
  "Session task is running" \
  "interrupt routing after the queue became empty"
tui_assert_stderr_empty

echo "compact queue Escape TUI regression passed: $TUI_OUT_DIR_ABS"
