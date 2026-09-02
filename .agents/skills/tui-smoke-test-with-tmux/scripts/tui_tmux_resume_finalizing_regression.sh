#!/usr/bin/env bash
# 确定性验证 Finalizing Resume 的等待、queued input 归属、失败清除与前台锁拒绝。
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

TUI_SESSION="${TUI_SESSION:-acn_resume_finalizing}"
TUI_WIDTH="${TUI_WIDTH:-132}"
TUI_HEIGHT="${TUI_HEIGHT:-40}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenarios/resume-finalizing}"
TUI_BUILD_COMMAND="${TUI_BUILD_COMMAND:-cargo build --quiet --bin acn --example fake_anthropic_sse_server}"

source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"
tui_build_if_needed
ACN_BINARY="$(tui_resolve_binary TUI_ACN_BINARY acn bin)"
FAKE_SERVER_BINARY="$(tui_resolve_binary TUI_FAKE_SERVER_BINARY fake_anthropic_sse_server example)"
mkdir -p "$TUI_OUT_DIR"
BASE="$(cd "$TUI_OUT_DIR" && pwd)"
FAKE_ACN_HOME="$(mktemp -d "$BASE/acn-home.XXXXXX")"
CONFIG="$BASE/config.toml"
READY_FILE="$BASE/fake-anthropic.port"
FAKE_SERVER_PID=""
LOCK_PIDS=()
LAST_LOCK_PID=""

cleanup() {
  tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
  for pid in "${LOCK_PIDS[@]:-}"; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  if [[ -f "$CONFIG" ]]; then
    tui_terminate_owned_supervisors "$CONFIG" "$ACN_BINARY" || true
  fi
  if [[ "$FAKE_SERVER_PID" =~ ^[0-9]+$ ]]; then
    kill -TERM "$FAKE_SERVER_PID" >/dev/null 2>&1 || true
    wait "$FAKE_SERVER_PID" >/dev/null 2>&1 || true
  fi
  if [[ "${TUI_KEEP_RUNTIME:-0}" != "1" && -d "$FAKE_ACN_HOME" ]]; then
    case "$FAKE_ACN_HOME" in
      "$BASE"/acn-home.*) rm -rf -- "$FAKE_ACN_HOME" ;;
    esac
  fi
}
trap cleanup EXIT

rm -f "$READY_FILE"
"$FAKE_SERVER_BINARY" --ready-file "$READY_FILE" --response-mode streaming-text &
FAKE_SERVER_PID="$!"
for _ in $(seq 1 80); do
  [[ -s "$READY_FILE" ]] && break
  sleep 0.1
done
[[ -s "$READY_FILE" ]] || {
  echo "fake Anthropic server did not become ready" >&2
  exit 1
}
FAKE_PORT="$(cat "$READY_FILE")"

cat > "$CONFIG" <<EOF
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
model = "fake-resume-finalizing-model"
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

start_tui() {
  local label="$1" extra_args="${2:-}"
  local runner="$BASE/run-$label.sh" stderr_log="$BASE/$label.stderr.log"
  tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
  cat > "$runner" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$REPO_ROOT"
if [[ -f export_env.sh ]]; then
  source export_env.sh
fi
ACN_FAKE_LLM_API_KEY=test-key "$ACN_BINARY" --config "$CONFIG" $extra_args 2> "$stderr_log"
EOF
  chmod +x "$runner"
  tmux new-session -d -s "$TUI_SESSION" -x "$TUI_WIDTH" -y "$TUI_HEIGHT" "$runner"
  tmux set-option -t "$TUI_SESSION" remain-on-exit on
}

capture_until() {
  local name="$1" pattern="$2" description="$3" attempts="${4:-120}"
  for _ in $(seq 1 "$attempts"); do
    sleep 0.2
    tmux capture-pane -t "$TUI_SESSION" -S - -p > "$BASE/$name.txt"
    if rg -q "$pattern" "$BASE/$name.txt"; then
      return 0
    fi
  done
  echo "timeout waiting for $description" >&2
  return 1
}

send_text() {
  local text="$1" buffer="${TUI_SESSION}_input"
  printf '%s' "$text" | tmux load-buffer -b "$buffer" -
  tmux paste-buffer -t "$TUI_SESSION" -b "$buffer"
  sleep 0.1
  tmux send-keys -t "$TUI_SESSION" C-m
}

assert_stderr_empty() {
  local label="$1"
  local path="$BASE/$label.stderr.log"
  if [[ -s "$path" ]]; then
    echo "$path is not empty" >&2
    return 1
  fi
}

wait_for_pane_dead() {
  local description="$1"
  for _ in $(seq 1 120); do
    if [[ "$(tmux display-message -p -t "$TUI_SESSION" '#{pane_dead}')" == "1" ]]; then
      return 0
    fi
    sleep 0.2
  done
  echo "timeout waiting for $description" >&2
  return 1
}

assert_picker_error_layout() {
  local capture="$1" target_id="$2" error_text="${3:-This session is still finalizing; wait for finalization to complete before resuming.}"
  python3 - "$capture" "$target_id" "$error_text" <<'PY'
import re
import sys
from pathlib import Path

lines = Path(sys.argv[1]).read_text().splitlines()
target = sys.argv[2]
error_text = sys.argv[3]
index = next(i for i, line in enumerate(lines) if line.startswith(f"› {target}"))
expected = f"      Error: {error_text}"
assert lines[index + 1].rstrip() == expected, lines[index:index + 3]
next_session = next(
    (i for i in range(index + 2, len(lines)) if re.match(r"^[ ›] session_[0-9a-f]{8}", lines[i])),
    None,
)
if next_session is not None:
    assert next_session == index + 2, lines[index:next_session + 1]
PY
}

assert_picker_selection_moved_off_target() {
  local capture="$1" target_id="$2"
  python3 - "$capture" "$target_id" <<'PY'
import re
import sys
from pathlib import Path

lines = Path(sys.argv[1]).read_text().splitlines()
selected = [line for line in lines if re.match(r"^› session_[0-9a-f]{8}", line)]
assert len(selected) == 1, selected
assert not selected[0].startswith(f"› {sys.argv[2]}"), selected[0]
PY
}

assert_direct_resume_warning_layout() {
  local capture="$1"
  python3 - "$capture" <<'PY'
import sys
from pathlib import Path

lines = Path(sys.argv[1]).read_text().splitlines()
inbox = next(i for i, line in enumerate(lines) if "Inbox completed: processed=" in line)
warning = next(
    i for i, line in enumerate(lines)
    if "Warning: MCP server nd4-broken-startup failed:" in line
)
queued = next(i for i, line in enumerate(lines) if line.startswith("› QUEUED-DIRECT-731"))
assert inbox < warning < queued, (inbox, warning, queued)
assert sum(not line.strip() for line in lines[inbox + 1:warning]) == 1, lines[inbox:warning + 1]
assert sum(not line.strip() for line in lines[warning + 1:queued]) == 1, lines[warning:queued + 1]
PY
}

session_id_from_capture() {
  local capture="$1"
  rg -o 'session_[0-9a-f]{8}' "$capture" | tail -1
}

wait_for_message_count() {
  local session_dir="$1" minimum="$2" description="$3"
  for _ in $(seq 1 150); do
    if [[ -f "$session_dir/messages.jsonl" ]] &&
      (( $(wc -l < "$session_dir/messages.jsonl") >= minimum )); then
      return 0
    fi
    sleep 0.2
  done
  echo "timeout waiting for $description" >&2
  return 1
}

set_target_updated_at() {
  local session_dir="$1" timestamp="${2:-2099-01-01T00:00:00Z}"
  python3 - "$session_dir/session.yaml" "$timestamp" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text()
text = re.sub(r"(?m)^updated_at:.*$", "updated_at: " + sys.argv[2], text)
path.write_text(text)
PY
}

mark_finalizing() {
  local session_dir="$1"
  python3 - "$session_dir/session.yaml" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = path.read_text()
text = re.sub(r"(?m)^status: open$", "status: finalizing", text)
text = re.sub(r"(?m)^updated_at:.*$", "updated_at: 2026-09-01T00:00:00Z", text)
path.write_text(text)
PY
}

write_checkpoint() {
  local session_dir="$1" valid="$2"
  python3 - "$session_dir" "$valid" <<'PY'
import re
import sys
from pathlib import Path

session_dir = Path(sys.argv[1])
valid = sys.argv[2] == "valid"
metadata = (session_dir / "session.yaml").read_text()
count = int(re.search(r"(?m)^message_count: (\d+)$", metadata).group(1))
if valid:
    value = 0xCBF29CE484222325
    prime = 0x100000001B3
    for line in (session_dir / "messages.jsonl").read_bytes().splitlines():
        for byte in line + b"\n":
            value ^= byte
            value = (value * prime) & 0xFFFFFFFFFFFFFFFF
    segment_hash = f"{value:016x}"
else:
    segment_hash = "invalid-segment-hash"
(session_dir / "finalize_checkpoint.yaml").write_text(f"""recap_start_index: 0
recap_end_index: {count}
recap_segment_hash: {segment_hash}
prepared_claims: []
prepared_disputes: []
used_claim_ids: []
trace_text: deterministic resume checkpoint
trace_created_at: 2026-09-01T00:00:00Z
trace_id: null
status: prepared
""")
PY
}

write_failed_finalize_job() {
  local agent_home="$1" session_id="$2" job_id="$3"
  local jobs_dir="$agent_home/runtime/supervisor/jobs"
  mkdir -p "$jobs_dir"
  cat > "$jobs_dir/$job_id.yaml" <<EOF
id: $job_id
agent_id: agent-a
kind:
  type: finalize
  session_id: $session_id
status: failed
attempts: 5
created_at: 2026-09-01T00:00:00Z
updated_at: 2026-09-01T00:00:00Z
finished_at: 2026-09-01T00:00:00Z
last_error: deterministic fixture failure
notify_on_completion: false
EOF
}

hold_finalize_lock() {
  local session_dir="$1" seconds="$2" ready="$BASE/lock-ready-$RANDOM"
  python3 - "$session_dir/finalize.lock" "$ready" "$seconds" <<'PY' &
import fcntl
import sys
import time
from pathlib import Path

lock_path, ready_path, seconds = sys.argv[1], Path(sys.argv[2]), float(sys.argv[3])
with open(lock_path, "a+") as lock_file:
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    ready_path.write_text("ready")
    time.sleep(seconds)
PY
  local pid="$!"
  LOCK_PIDS+=("$pid")
  LAST_LOCK_PID="$pid"
  for _ in $(seq 1 50); do
    [[ -s "$ready" ]] && break
    sleep 0.1
  done
  [[ -s "$ready" ]] || {
    echo "failed to acquire deterministic finalize lock" >&2
    return 1
  }
}

hold_foreground_finalize_locks() {
  local session_dir="$1" seconds="$2" ready="$BASE/foreground-lock-ready-$RANDOM"
  python3 - "$session_dir/runtime.lock" "$session_dir/finalize.lock" "$ready" "$seconds" <<'PY' &
import fcntl
import sys
import time
from pathlib import Path

runtime_path, finalize_path = sys.argv[1], sys.argv[2]
ready_path, seconds = Path(sys.argv[3]), float(sys.argv[4])
with open(runtime_path, "a+") as runtime_file, open(finalize_path, "a+") as finalize_file:
    fcntl.flock(runtime_file.fileno(), fcntl.LOCK_EX)
    fcntl.flock(finalize_file.fileno(), fcntl.LOCK_EX)
    ready_path.write_text("ready")
    time.sleep(seconds)
PY
  local pid="$!"
  LOCK_PIDS+=("$pid")
  LAST_LOCK_PID="$pid"
  for _ in $(seq 1 50); do
    [[ -s "$ready" ]] && break
    sleep 0.1
  done
  [[ -s "$ready" ]] || {
    echo "failed to acquire deterministic foreground finalize locks" >&2
    return 1
  }
}

# 创建一个带真实 canonical turn 的 seed；随后崩溃式退出，保留 Open session。
start_tui seed
capture_until seed_open 'focus [0-9]+[smh].*open' 'seed TUI open'
SEED_ID="$(session_id_from_capture "$BASE/seed_open.txt")"
[[ -n "$SEED_ID" ]] || {
  echo "could not resolve seed session id" >&2
  exit 1
}
SEED_DIR="$(find "$FAKE_ACN_HOME" -type d -path "*/sessions/$SEED_ID" -print -quit)"
[[ -n "$SEED_DIR" ]] || {
  echo "could not locate seed session directory" >&2
  exit 1
}
send_text 'SEED-CONTEXT-731'
wait_for_message_count "$SEED_DIR" 2 'seed turn canonical commit'
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$BASE/seed_turn.txt"
tmux kill-session -t "$TUI_SESSION"
assert_stderr_empty seed

SESSIONS_DIR="$(dirname "$SEED_DIR")"
AGENT_HOME="$(dirname "$SESSIONS_DIR")"
SUCCESS_ID="session_a1a1a1a1"
FAILURE_ID="session_f1f1f1f1"
FOREGROUND_ID="session_e1e1e1e1"

python3 - "$SEED_DIR" "$SESSIONS_DIR" "$SEED_ID" "$SUCCESS_ID" "$FAILURE_ID" "$FOREGROUND_ID" <<'PY'
import shutil
import sys
from pathlib import Path

source, parent, old, *targets = sys.argv[1:]
source = Path(source)
parent = Path(parent)
for target in targets:
    destination = parent / target
    shutil.copytree(source, destination)
    for path in destination.rglob("*"):
        if path.is_file():
            data = path.read_bytes().replace(old.encode(), target.encode())
            path.write_bytes(data)
shutil.rmtree(source)
PY

SUCCESS_DIR="$SESSIONS_DIR/$SUCCESS_ID"
FAILURE_DIR="$SESSIONS_DIR/$FAILURE_ID"
FOREGROUND_DIR="$SESSIONS_DIR/$FOREGROUND_ID"
for dir in "$SUCCESS_DIR" "$FAILURE_DIR" "$FOREGROUND_DIR"; do
  mark_finalizing "$dir"
  rm -f "$dir/finalize_checkpoint.yaml"
done
write_checkpoint "$SUCCESS_DIR" valid
write_checkpoint "$FAILURE_DIR" invalid
write_failed_finalize_job "$AGENT_HOME" "$SUCCESS_ID" job_a1a1a1a1
write_failed_finalize_job "$AGENT_HOME" "$FAILURE_ID" job_f1f1f1f1

# Success：Prepared recovery 在锁后完成，等待期输入最终只进入目标 A。
hold_finalize_lock "$SUCCESS_DIR" 20
SUCCESS_LOCK_PID="$LAST_LOCK_PID"
set_target_updated_at "$SUCCESS_DIR"
start_tui main
capture_until main_open 'focus [0-9]+[smh].*open' 'current session open'
CURRENT_B_ID="$(session_id_from_capture "$BASE/main_open.txt")"
CURRENT_B_DIR="$(find "$FAKE_ACN_HOME" -type d -path "*/sessions/$CURRENT_B_ID" -print -quit)"
send_text 'CURRENT-B-CONTEXT-731'
wait_for_message_count "$CURRENT_B_DIR" 2 'current session turn canonical commit'
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$BASE/main_turn.txt"
send_text '/resume'
capture_until success_picker "$SUCCESS_ID.*Finalizing" 'success target in resume picker'
tmux send-keys -t "$TUI_SESSION" Enter
capture_until success_wait 'Resuming · Waiting for target finalization' 'Finalizing resume wait'
rg -qF "Target resume $SUCCESS_ID finalizing..." "$BASE/success_wait.txt"
send_text 'QUEUED-SUCCESS-731'
capture_until success_queued 'queued=1' 'queued input visible during Finalizing resume wait'
wait "$SUCCESS_LOCK_PID" >/dev/null 2>&1 || true
wait_for_message_count "$SUCCESS_DIR" 4 'queued target turn canonical commit'
capture_until success_open 'focus [0-9]+[smh].*open' 'resumed target open' 180

rg -q 'QUEUED-SUCCESS-731' "$SUCCESS_DIR/messages.jsonl"
if rg -q 'QUEUED-SUCCESS-731' "$SESSIONS_DIR/$CURRENT_B_ID/messages.jsonl"; then
  echo "waiting input leaked into current session B" >&2
  exit 1
fi
rg -q '^status: open$' "$SUCCESS_DIR/session.yaml"
rg -q '^status: succeeded$' "$AGENT_HOME/runtime/supervisor/jobs/job_a1a1a1a1.yaml"

# Failure：无效 checkpoint 在锁释放后耗尽 recovery；等待期输入被清除且当前 A 保持 Open。
hold_finalize_lock "$FAILURE_DIR" 8
FAILURE_LOCK_PID="$LAST_LOCK_PID"
set_target_updated_at "$FAILURE_DIR"
send_text '/resume'
capture_until failure_picker "$FAILURE_ID.*Finalizing" 'failure target in resume picker'
tmux send-keys -t "$TUI_SESSION" Enter
capture_until failure_wait 'Resuming · Waiting for target finalization' 'failed recovery wait'
rg -qF "Target resume $FAILURE_ID finalizing..." "$BASE/failure_wait.txt"
send_text 'QUEUED-DISCARD-731'
capture_until failure_result 'This session is still finalizing; wait for finalization to complete before resuming.' 'fixed resume failure' 180
assert_picker_error_layout "$BASE/failure_result.txt" "$FAILURE_ID"
if rg -q 'Queued input entered while resuming was discarded\.' "$BASE/failure_result.txt"; then
  echo "resume failure displayed the removed queued-input notice" >&2
  exit 1
fi
wait "$FAILURE_LOCK_PID" >/dev/null 2>&1 || true

if rg -q 'QUEUED-DISCARD-731' "$SUCCESS_DIR/messages.jsonl" "$FAILURE_DIR/messages.jsonl"; then
  echo "discarded waiting input entered canonical messages" >&2
  exit 1
fi
rg -q '^status: open$' "$SUCCESS_DIR/session.yaml"
rg -q '^status: finalizing$' "$FAILURE_DIR/session.yaml"
tmux send-keys -t "$TUI_SESSION" Escape

# Foreground：真实前台 Finalize 同时持有 runtime/finalize 锁时直接拒绝，不取消持锁执行者。
hold_foreground_finalize_locks "$FOREGROUND_DIR" 12
FOREGROUND_LOCK_PID="$LAST_LOCK_PID"
set_target_updated_at "$FOREGROUND_DIR" 2099-01-01T00:00:01Z
send_text '/resume'
capture_until foreground_picker "$FOREGROUND_ID.*Finalizing" 'foreground target in resume picker'
tmux send-keys -t "$TUI_SESSION" Enter
FOREGROUND_ERROR="$FOREGROUND_ID is still finalizing foreground; Try again after its completion."
capture_until foreground_rejected "$FOREGROUND_ID is still finalizing foreground; Try again after its completion\." 'foreground Finalize rejection'
assert_picker_error_layout "$BASE/foreground_rejected.txt" "$FOREGROUND_ID" "$FOREGROUND_ERROR"
tmux send-keys -t "$TUI_SESSION" Down
sleep 0.3
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$BASE/foreground_navigated.txt"
assert_picker_selection_moved_off_target "$BASE/foreground_navigated.txt" "$FOREGROUND_ID"
tmux send-keys -t "$TUI_SESSION" Escape
send_text '/resume'
capture_until foreground_reopened "$FOREGROUND_ID.*Finalizing" 'reopened foreground picker'
if rg -qF "$FOREGROUND_ERROR" "$BASE/foreground_reopened.txt"; then
  echo "picker retained the previous inline error after reopen" >&2
  exit 1
fi
kill -0 "$FOREGROUND_LOCK_PID"
rg -q '^status: finalizing$' "$FOREGROUND_DIR/session.yaml"

assert_stderr_empty main
tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true

# Direct Resume 的本次 MCP 启动 warning 在立即拒绝时保持隐藏，成功时在 Inbox 后、queued input 前显示。
MCP_CONFIG="$FAKE_ACN_HOME/dev/.mcp.json"
mkdir -p "$(dirname "$MCP_CONFIG")"
cat > "$MCP_CONFIG" <<'JSON'
{
  "mcpServers": {
    "nd4-broken-startup": {
      "type": "stdio",
      "command": "/usr/bin/false"
    }
  }
}
JSON

start_tui direct "--resume $FOREGROUND_ID"
wait_for_pane_dead 'direct foreground Finalize rejection'
DIRECT_STATUS="$(tmux display-message -p -t "$TUI_SESSION" '#{pane_dead_status}')"
if [[ "$DIRECT_STATUS" == "0" ]]; then
  echo "direct foreground Finalize rejection exited successfully" >&2
  exit 1
fi
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$BASE/direct_rejected.txt"
rg -qxF "Error: $FOREGROUND_ERROR" "$BASE/direct.stderr.log"
if rg -v -q '^[[:space:]]*$|^Pane is dead \(status [0-9]+, .+\)$' "$BASE/direct_rejected.txt"; then
  echo "direct foreground rejection left output in terminal scrollback" >&2
  exit 1
fi
kill -0 "$FOREGROUND_LOCK_PID"
tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
tui_terminate_owned_supervisors "$CONFIG" "$ACN_BINARY"

wait "$FOREGROUND_LOCK_PID" >/dev/null 2>&1 || true
write_checkpoint "$FOREGROUND_DIR" valid
write_failed_finalize_job "$AGENT_HOME" "$FOREGROUND_ID" job_e1e1e1e1
hold_finalize_lock "$FOREGROUND_DIR" 8
DIRECT_WAIT_LOCK_PID="$LAST_LOCK_PID"
start_tui direct_after "--resume $FOREGROUND_ID"
capture_until direct_after_wait 'Resuming · Waiting for target finalization' 'direct prepared recovery wait'
send_text 'QUEUED-DIRECT-731'
capture_until direct_after_queued 'queued=1' 'direct queued input during recovery wait'
wait "$DIRECT_WAIT_LOCK_PID" >/dev/null 2>&1 || true
wait_for_message_count "$FOREGROUND_DIR" 4 'direct queued target turn canonical commit'
capture_until direct_after_open "$FOREGROUND_ID type /" 'direct resume with startup warning open' 180
assert_direct_resume_warning_layout "$BASE/direct_after_open.txt"
rg -q 'QUEUED-DIRECT-731' "$FOREGROUND_DIR/messages.jsonl"
rg -q '^status: open$' "$FOREGROUND_DIR/session.yaml"
rg -q '^status: succeeded$' "$AGENT_HOME/runtime/supervisor/jobs/job_e1e1e1e1.yaml"
assert_stderr_empty direct_after
tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
tui_terminate_owned_supervisors "$CONFIG" "$ACN_BINARY"

echo "resume-finalizing TUI regression passed: wait/success ownership, picker inline errors, direct exit/startup warning, and failure discard"
