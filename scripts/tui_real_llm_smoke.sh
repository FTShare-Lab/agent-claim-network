#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/tui_real_llm_smoke.sh <label> <config-path>

Run the real-LLM ACN team-mode TUI smoke test.

Arguments:
  label        Artifact label: letters, numbers, underscores, and hyphens only
  config-path  Source ACN config; the script modifies only a copied fixture

The script optionally sources ./export_env.sh. Otherwise, export the environment
variable named by agent.llm.api_key_env before running it.
USAGE
}

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  usage
  exit 0
fi
if [[ $# -ne 2 ]]; then
  usage >&2
  exit 2
fi

LABEL="$1"
CONFIG_SRC="$2"
if [[ ! "$LABEL" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$ ]]; then
  echo "label must match [A-Za-z0-9][A-Za-z0-9_-]{0,47}" >&2
  exit 2
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is required for the real LLM TUI smoke test" >&2
  exit 127
fi
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

if [[ ! -f "$CONFIG_SRC" ]]; then
  echo "config file not found: $CONFIG_SRC" >&2
  exit 1
fi
CONFIG_SRC="$(cd "$(dirname "$CONFIG_SRC")" && pwd)/$(basename "$CONFIG_SRC")"

for command in awk cargo tmux rg lsof nc perl seq; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required for the real LLM TUI smoke test" >&2
    exit 127
  fi
done

if [[ -f export_env.sh ]]; then
  # shellcheck disable=SC1091
  source export_env.sh
fi

pick_free_port() {
  local candidate
  for _ in $(seq 1 200); do
    candidate=$((20000 + RANDOM % 20000))
    if ! lsof -nP -iTCP:"$candidate" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "$candidate"
      return 0
    fi
  done
  echo "failed to find a free TCP port" >&2
  return 1
}

RUNS_ROOT="$REPO_ROOT/target/tui-real-smoke"
BASE="$RUNS_ROOT/$LABEL"
case "$CONFIG_SRC" in
  "$RUNS_ROOT"/*)
    echo "config file must be outside the generated artifact root: $RUNS_ROOT" >&2
    exit 2
    ;;
esac
CONFIG="$BASE/config.toml"
ACN_HOME="$BASE/data"
WORKSPACE_ROOT="$BASE/workspace"
ROUTER_SESSION="acn_real_${LABEL}_router"
MAINTAINER_SESSION="acn_real_${LABEL}_maintainer"
AGENT_SESSION="acn_real_${LABEL}_agent"
RESUME_SESSION="acn_real_${LABEL}_resume"
TMUX_SOCKET="acn-real-smoke-${LABEL}-$$"
ROUTER_PORT="${ACN_TUI_SMOKE_ROUTER_PORT:-$(pick_free_port)}"
MAINTAINER_PORT="${ACN_TUI_SMOKE_MAINTAINER_PORT:-$(pick_free_port)}"
while [[ "$MAINTAINER_PORT" == "$ROUTER_PORT" ]]; do
  MAINTAINER_PORT="$(pick_free_port)"
done

tmux_exec() {
  tmux -L "$TMUX_SOCKET" "$@"
}

cleanup() {
  tmux_exec kill-server >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
mkdir -p "$RUNS_ROOT"
rm -rf -- "$BASE"
mkdir -p "$BASE" "$WORKSPACE_ROOT"
cp "$CONFIG_SRC" "$CONFIG"
cat > "$WORKSPACE_ROOT/smoke_note.md" <<'NOTE'
# Smoke Note

This file exists so the real TUI smoke test can ask the LLM to call file_read.

- provider-neutral session turn
- tool use
- consult_router
- manual compaction
- resume
NOTE

for pattern in \
  '^acn_home = ".*"$' \
  '^\[router\.daemon\]$' \
  '^\[maintainer\.daemon\]$' \
  '^maintainer_endpoint = ".*"$' \
  '^router_endpoint = ".*"$'; do
  if ! rg -q "$pattern" "$CONFIG"; then
    echo "config is missing a field required by the smoke test: $pattern" >&2
    exit 1
  fi
done

ACN_HOME="$ACN_HOME" perl -0pi -e 's|acn_home = ".*"|acn_home = "$ENV{ACN_HOME}"|' "$CONFIG"
perl -0pi -e "s|\\[router\\.daemon\\]\\nlisten = \".*\"|[router.daemon]\nlisten = \"127.0.0.1:$ROUTER_PORT\"|" "$CONFIG"
perl -0pi -e "s|\\[maintainer\\.daemon\\]\\nlisten = \".*\"|[maintainer.daemon]\nlisten = \"127.0.0.1:$MAINTAINER_PORT\"|" "$CONFIG"
perl -0pi -e "s|maintainer_endpoint = \".*\"|maintainer_endpoint = \"http://127.0.0.1:$MAINTAINER_PORT\"|" "$CONFIG"
perl -0pi -e "s|router_endpoint = \".*\"|router_endpoint = \"http://127.0.0.1:$ROUTER_PORT\"|" "$CONFIG"

if ! rg -Fq "acn_home = \"$ACN_HOME\"" "$CONFIG" \
  || ! rg -q -U "\\[router\\.daemon\\]\\nlisten = \"127\\.0\\.0\\.1:$ROUTER_PORT\"" "$CONFIG" \
  || ! rg -q -U "\\[maintainer\\.daemon\\]\\nlisten = \"127\\.0\\.0\\.1:$MAINTAINER_PORT\"" "$CONFIG" \
  || ! rg -Fq "maintainer_endpoint = \"http://127.0.0.1:$MAINTAINER_PORT\"" "$CONFIG" \
  || ! rg -Fq "router_endpoint = \"http://127.0.0.1:$ROUTER_PORT\"" "$CONFIG"; then
  echo "failed to rewrite the copied smoke-test config" >&2
  exit 1
fi

CONFIG_ARG="$(printf '%q' "$CONFIG")"
WORKSPACE_ARG="$(printf '%q' "$WORKSPACE_ROOT")"

write_runner() {
  local path="$1"
  local command="$2"
  local stderr_path="$3"
  local stdout_path="$4"
  local mode="${5:-redirect_stdout}"
  cat > "$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$REPO_ROOT"
EOF
  if [[ "$mode" == "terminal_stdout" ]]; then
    cat >> "$path" <<EOF
{
$command
} 2> "$stderr_path"
echo "__TUI_EXITED__"
sleep "\${ACN_TUI_SMOKE_EXIT_HOLD_SECS:-5}"
EOF
  else
    cat >> "$path" <<EOF
{
$command
} > "$stdout_path" 2> "$stderr_path"
EOF
  fi
  chmod +x "$path"
}

wait_for_port() {
  local port="$1"
  local name="$2"
  local session="$3"
  local stderr_path="$4"
  for _ in $(seq 1 120); do
    if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
      return 0
    fi
    if ! tmux_exec has-session -t "$session" >/dev/null 2>&1; then
      echo "$name exited before opening port $port; inspect $stderr_path" >&2
      return 1
    fi
    sleep 0.5
  done
  echo "$name did not open port $port" >&2
  return 1
}

capture_agent() {
  local session="$1"
  local name="$2"
  tmux_exec capture-pane -t "$session" -p -S - > "$BASE/$name.txt" || true
}

capture_is_open() {
  local path="$1"
  rg -Fq "type / for commands · Enter sends" "$path" \
    && rg -q '·[[:space:]]+open[[:space:]]*$' "$path"
}

wait_for_agent_open() {
  local session="$1"
  local name="$2"
  local max="${3:-360}"
  for _ in $(seq 1 "$max"); do
    capture_agent "$session" "$name"
    if capture_is_open "$BASE/$name.txt"; then
      return 0
    fi
    if [[ -s "$BASE/agent.stderr" || -s "$BASE/resume.stderr" ]]; then
      echo "agent stderr became non-empty while waiting for open" >&2
      return 1
    fi
    sleep 1
  done
  echo "TUI did not return to open state for $name" >&2
  return 1
}

wait_for_resumed_agent_open() {
  local session="$1"
  local name="$2"
  local max="${3:-360}"
  for _ in $(seq 1 "$max"); do
    capture_agent "$session" "$name"
    if rg -q "Session session_[0-9a-f]{8} resumed\\." "$BASE/$name.txt" \
      && capture_is_open "$BASE/$name.txt"; then
      return 0
    fi
    if [[ -s "$BASE/resume.stderr" ]]; then
      echo "resume stderr became non-empty while waiting for resumed session" >&2
      return 1
    fi
    if ! tmux_exec has-session -t "$session" >/dev/null 2>&1; then
      echo "resume TUI exited before the selected session became open" >&2
      return 1
    fi
    sleep 1
  done
  echo "resumed TUI did not show the resumed/open state for $name" >&2
  return 1
}

wait_for_resume_entry() {
  local session="$1"
  local max="${2:-180}"
  for _ in $(seq 1 "$max"); do
    capture_agent "$session" "resume_picker"
    if rg -q "Session Resume" "$BASE/resume_picker.txt"; then
      return 0
    fi
    if [[ -s "$BASE/resume.stderr" ]]; then
      echo "resume stderr became non-empty while waiting for resume entry" >&2
      return 1
    fi
    if ! tmux_exec has-session -t "$session" >/dev/null 2>&1; then
      echo "resume TUI exited before showing a picker or open session" >&2
      return 1
    fi
    sleep 1
  done
  echo "resume TUI did not show the session picker" >&2
  return 1
}

SESSION_MESSAGES=""
SESSION_METADATA=""

wait_for_session_files() {
  local max="${1:-180}"
  for _ in $(seq 1 "$max"); do
    local messages
    messages="$(
      find "$ACN_HOME" \
        -type f \
        -path '*/data/agents/*/sessions/session_*/messages.jsonl' \
        -print \
        | sort \
        | tail -1
    )"
    if [[ -n "$messages" && -f "$(dirname "$messages")/session.yaml" ]]; then
      SESSION_MESSAGES="$messages"
      SESSION_METADATA="$(dirname "$messages")/session.yaml"
      return 0
    fi
    sleep 1
  done
  echo "session files were not created below $ACN_HOME" >&2
  return 1
}

session_message_count() {
  if [[ -z "$SESSION_MESSAGES" || ! -f "$SESSION_MESSAGES" ]]; then
    echo 0
    return
  fi
  wc -l < "$SESSION_MESSAGES" | tr -d '[:space:]'
}

wait_for_session_status() {
  local expected="$1"
  local max="${2:-180}"
  for _ in $(seq 1 "$max"); do
    if rg -q "^status: $expected$" "$SESSION_METADATA"; then
      return 0
    fi
    sleep 0.5
  done
  local actual
  actual="$(awk '/^status:/ { print $2; exit }' "$SESSION_METADATA")"
  echo "session did not reach status $expected; actual=${actual:-unknown}" >&2
  return 1
}

wait_for_message_count() {
  local session="$1"
  local name="$2"
  local min_messages="$3"
  local max="${4:-420}"
  for _ in $(seq 1 "$max"); do
    capture_agent "$session" "$name"
    local count
    count="$(session_message_count)"
    if [[ "$count" -ge "$min_messages" ]] \
      && capture_is_open "$BASE/$name.txt"; then
      return 0
    fi
    if [[ -s "$BASE/agent.stderr" || -s "$BASE/resume.stderr" ]]; then
      echo "agent stderr became non-empty while waiting for messages >= $min_messages" >&2
      return 1
    fi
    sleep 1
  done
  echo "TUI did not reach messages >= $min_messages for $name" >&2
  return 1
}

compact_echo_count() {
  local path="$1"
  awk 'index($0, "› /compact") { count += 1 } END { print count + 0 }' "$path"
}

run_compact() {
  local session="$1"
  local name="$2"
  local max="${3:-420}"
  local before_name="${name}_before"
  capture_agent "$session" "$before_name"
  local before_count
  before_count="$(compact_echo_count "$BASE/$before_name.txt")"

  tmux_exec send-keys -t "$session" "/compact" Enter
  for _ in $(seq 1 "$max"); do
    capture_agent "$session" "$name"
    local current_count
    current_count="$(compact_echo_count "$BASE/$name.txt")"
    if [[ "$current_count" -gt "$before_count" ]] \
      && capture_is_open "$BASE/$name.txt"; then
      return 0
    fi
    if [[ -s "$BASE/agent.stderr" || -s "$BASE/resume.stderr" ]]; then
      echo "agent stderr became non-empty while waiting for $name" >&2
      return 1
    fi
    sleep 1
  done
  echo "TUI did not complete /compact for $name" >&2
  return 1
}

wait_for_capture_contains() {
  local session="$1"
  local name="$2"
  local pattern="$3"
  local max="${4:-420}"
  for _ in $(seq 1 "$max"); do
    capture_agent "$session" "$name"
    if rg -q "$pattern" "$BASE/$name.txt"; then
      return 0
    fi
    if [[ -s "$BASE/agent.stderr" || -s "$BASE/resume.stderr" ]]; then
      echo "agent stderr became non-empty while waiting for $name" >&2
      return 1
    fi
    sleep 1
  done
  echo "TUI did not show expected pattern for $name: $pattern" >&2
  return 1
}

wait_for_session_gone() {
  local session="$1"
  local max="${2:-360}"
  for _ in $(seq 1 "$max"); do
    if ! tmux_exec has-session -t "$session" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "tmux session $session did not exit" >&2
  return 1
}

send_turn() {
  local session="$1"
  local name="$2"
  local text="$3"
  local current_messages
  current_messages="$(session_message_count)"
  local min_messages=$((current_messages + 2))
  tmux_exec send-keys -t "$session" -l "$text"
  sleep 0.2
  tmux_exec send-keys -t "$session" Enter
  wait_for_message_count "$session" "$name" "$min_messages" 420
}

if lsof -nP -iTCP:"$ROUTER_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $ROUTER_PORT is already in use; refusing to disturb existing process" >&2
  exit 1
fi
if lsof -nP -iTCP:"$MAINTAINER_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port $MAINTAINER_PORT is already in use; refusing to disturb existing process" >&2
  exit 1
fi

write_runner "$BASE/run_router.sh" \
  "cargo run --quiet --bin acn-router -- --config $CONFIG_ARG" \
  "$BASE/router.stderr" "$BASE/router.stdout"
write_runner "$BASE/run_maintainer.sh" \
  "cargo run --quiet --bin acn-maintainer -- --config $CONFIG_ARG" \
  "$BASE/maintainer.stderr" "$BASE/maintainer.stdout"
write_runner "$BASE/run_agent.sh" \
  "cargo run --quiet --bin acn -- --config $CONFIG_ARG --cd $WORKSPACE_ARG" \
  "$BASE/agent.stderr" "$BASE/agent.stdout" terminal_stdout
write_runner "$BASE/run_resume.sh" \
  "cargo run --quiet --bin acn -- --config $CONFIG_ARG --resume --cd $WORKSPACE_ARG" \
  "$BASE/resume.stderr" "$BASE/resume.stdout" terminal_stdout

tmux_exec new-session -d -s "$ROUTER_SESSION" -x 120 -y 36 "$BASE/run_router.sh"
tmux_exec new-session -d -s "$MAINTAINER_SESSION" -x 120 -y 36 "$BASE/run_maintainer.sh"
wait_for_port "$ROUTER_PORT" router "$ROUTER_SESSION" "$BASE/router.stderr"
wait_for_port "$MAINTAINER_PORT" maintainer "$MAINTAINER_SESSION" "$BASE/maintainer.stderr"

tmux_exec new-session -d -s "$AGENT_SESSION" -x 140 -y 44 "$BASE/run_agent.sh"
wait_for_agent_open "$AGENT_SESSION" "initial" 180
wait_for_session_files 180

send_turn "$AGENT_SESSION" "after_turn_1" \
  "Real smoke turn 1. You must call file_read on smoke_note.md with count 20, then answer in one short sentence."
send_turn "$AGENT_SESSION" "after_turn_2" \
  "Real smoke turn 2. You must call working_note with action add and note 'provider neutral smoke note', then answer in one short sentence."
send_turn "$AGENT_SESSION" "after_turn_3" \
  "Real smoke turn 3. You must call consult_router with scope 'smoke/test' and semantic_query 'provider neutral router smoke', then answer even if it returns no claims."
send_turn "$AGENT_SESSION" "after_turn_4" \
  "Real smoke turn 4. You must call file_read on smoke_note.md with keyword consult_router and count 8, and also call consult_router with scope 'agent_claim_network'. Then answer briefly."

run_compact "$AGENT_SESSION" "after_compact_1" 420

send_turn "$AGENT_SESSION" "after_turn_5" \
  "Real smoke turn 5 after compact. You must call file_read on smoke_note.md with start 1 and count 5, then answer briefly."

tmux_exec send-keys -t "$AGENT_SESSION" "/exit" Enter
wait_for_capture_contains "$AGENT_SESSION" "after_exit_1" "Background finalize enqueued: job_|__TUI_EXITED__" 420
if ! rg -q "Background finalize enqueued: job_[0-9]+_[0-9a-f]+" "$BASE/after_exit_1.txt" \
  || ! rg -q "Resume this session with: --resume session_[0-9a-f]{8}" "$BASE/after_exit_1.txt"; then
  echo "first exit did not show finalize enqueue and resume instructions" >&2
  exit 1
fi
wait_for_session_gone "$AGENT_SESSION" 420
wait_for_session_status closed 180

tmux_exec new-session -d -s "$RESUME_SESSION" -x 140 -y 44 "$BASE/run_resume.sh"
wait_for_resume_entry "$RESUME_SESSION" 180
tmux_exec send-keys -t "$RESUME_SESSION" Enter
wait_for_resumed_agent_open "$RESUME_SESSION" "after_resume_open" 180

send_turn "$RESUME_SESSION" "after_turn_6" \
  "Real smoke turn 6 after resume. You must call consult_router with scope 'smoke/resume' and semantic_query 'resumed session router call', then answer briefly."

run_compact "$RESUME_SESSION" "after_compact_2" 420

send_turn "$RESUME_SESSION" "after_turn_7" \
  "Real smoke turn 7 final. You must call working_note with action list, then answer briefly that the resumed session is still working."

tmux_exec send-keys -t "$RESUME_SESSION" "/exit" Enter
wait_for_capture_contains "$RESUME_SESSION" "after_exit_2" "Background finalize enqueued: job_|__TUI_EXITED__" 420
if ! rg -q "Background finalize enqueued: job_[0-9]+_[0-9a-f]+" "$BASE/after_exit_2.txt" \
  || ! rg -q "Resume this session with: --resume session_[0-9a-f]{8}" "$BASE/after_exit_2.txt"; then
  echo "second exit did not show finalize enqueue and resume instructions" >&2
  exit 1
fi
wait_for_session_gone "$RESUME_SESSION" 420
wait_for_session_status closed 180

if [[ -s "$BASE/agent.stderr" ]]; then
  echo "agent stderr is not empty" >&2
  exit 1
fi
if [[ -s "$BASE/resume.stderr" ]]; then
  echo "resume stderr is not empty" >&2
  exit 1
fi
if rg -q "ERROR|panic|panicked" "$BASE/router.stderr" "$BASE/maintainer.stderr"; then
  echo "router/maintainer logs contain errors" >&2
  exit 1
fi

if [[ -z "$SESSION_MESSAGES" || -z "$SESSION_METADATA" ]]; then
  echo "session files were not created" >&2
  exit 1
fi
cp "$SESSION_MESSAGES" "$BASE/final_messages.jsonl"
cp "$SESSION_METADATA" "$BASE/final_metadata.yaml"

for pattern in file_read consult_router working_note; do
  if ! rg -q "$pattern" "$BASE/final_messages.jsonl"; then
    echo "final transcript does not contain expected tool marker: $pattern" >&2
    exit 1
  fi
done
if ! rg -q "status: closed" "$BASE/final_metadata.yaml"; then
  echo "final metadata is not closed" >&2
  exit 1
fi
if ! rg -q "compaction:" "$BASE/final_metadata.yaml"; then
  echo "final metadata does not contain compaction state" >&2
  exit 1
fi

echo "REAL_TUI_SMOKE_PASSED label=$LABEL base=$BASE"
