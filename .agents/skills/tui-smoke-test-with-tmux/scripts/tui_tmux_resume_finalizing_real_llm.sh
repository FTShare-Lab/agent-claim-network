#!/usr/bin/env bash
# 真实 LLM 验证 Finalizing Resume 的转换、checkpoint 等待、输入归属和队列顺序。
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
# shellcheck disable=SC1091
source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

if [[ -f export_env.sh ]]; then
  # shellcheck disable=SC1091
  source export_env.sh
fi

CONFIG_SOURCE="${RESUME_FINALIZING_REAL_LLM_CONFIG:-$REPO_ROOT/config.toml}"
[[ -f "$CONFIG_SOURCE" ]] || {
  echo "real LLM config not found: $CONFIG_SOURCE" >&2
  exit 1
}
API_KEY_ENV="$(python3 - "$CONFIG_SOURCE" <<'PY'
import sys
import tomllib

with open(sys.argv[1], "rb") as stream:
    print(tomllib.load(stream)["agent"]["llm"]["api_key_env"])
PY
)"
[[ -n "${!API_KEY_ENV:-}" ]] || {
  echo "required env var $API_KEY_ENV is empty; source export_env.sh first" >&2
  exit 1
}

tui_build_if_needed
ACN_BINARY="$(tui_resolve_binary TUI_ACN_BINARY acn bin)"

BASE_OUT="${RESUME_FINALIZING_REAL_LLM_OUT_DIR:-target/tui-scenarios/resume-finalizing-real-llm}"
RUN_ROOT="$REPO_ROOT/$BASE_OUT/$(date +%Y%m%d-%H%M%S)-$$"
CONFIG="$RUN_ROOT/config.toml"
ACN_HOME="$RUN_ROOT/acn_home"
TUI_SESSION="acn_resume_finalizing_real_llm_$$"
TUI_WIDTH="${TUI_WIDTH:-132}"
TUI_HEIGHT="${TUI_HEIGHT:-40}"
WAIT_SECS="${RESUME_FINALIZING_REAL_LLM_WAIT_SECS:-240}"
LOCK_PIDS=()
LAST_LOCK_PID=""
mkdir -p "$RUN_ROOT"

python3 - "$CONFIG_SOURCE" "$CONFIG" "$ACN_HOME" <<'PY'
import json
import re
import sys
from pathlib import Path

source, target, home = map(Path, sys.argv[1:])
text = source.read_text()
text, count = re.subn(
    r"(?m)^acn_home\s*=\s*.*$",
    "acn_home = " + json.dumps(str(home.resolve())),
    text,
    count=1,
)
if count != 1:
    raise SystemExit("expected exactly one storage.acn_home entry")
target.write_text(text)
PY
IFS=$'\t' read -r UPSTREAM AGENT_ID < <(tui_config_agent_identity "$CONFIG")
AGENT_HOME="$ACN_HOME/$UPSTREAM/data/agents/$AGENT_ID"
SESSIONS_DIR="$AGENT_HOME/sessions"
JOBS_DIR="$AGENT_HOME/runtime/supervisor/jobs"

cleanup() {
  local cleanup_status=0
  tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
  for pid in "${LOCK_PIDS[@]:-}"; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  if ! tui_terminate_owned_supervisors "$CONFIG" "$ACN_BINARY"; then
    cleanup_status=1
  fi
  if [[ "${RESUME_FINALIZING_REAL_LLM_KEEP_RUNTIME:-0}" != "1" && -d "$ACN_HOME" ]]; then
    case "$ACN_HOME" in
      "$RUN_ROOT"/acn_home) find "$ACN_HOME" -depth -delete ;;
      *) cleanup_status=1 ;;
    esac
  fi
  return "$cleanup_status"
}
trap cleanup EXIT

start_tui() {
  local label="$1" extra_args="${2:-}"
  local runner="$RUN_ROOT/run-$label.sh" stderr_log="$RUN_ROOT/$label.stderr.log"
  tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
  cat > "$runner" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$REPO_ROOT"
if [[ -f export_env.sh ]]; then
  source export_env.sh
fi
"$ACN_BINARY" --config "$CONFIG" $extra_args 2> "$stderr_log"
EOF
  chmod +x "$runner"
  tmux new-session -d -s "$TUI_SESSION" -x "$TUI_WIDTH" -y "$TUI_HEIGHT" "$runner"
  tmux set-option -t "$TUI_SESSION" remain-on-exit on
}

capture_until() {
  local name="$1" pattern="$2" description="$3"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    sleep 0.5
    tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/$name.txt"
    if rg -q "$pattern" "$RUN_ROOT/$name.txt"; then
      return 0
    fi
  done
  echo "timeout waiting for $description" >&2
  return 1
}

capture_until_count_greater() {
  local name="$1" pattern="$2" previous="$3" description="$4"
  local deadline=$((SECONDS + WAIT_SECS)) count
  while (( SECONDS < deadline )); do
    sleep 0.5
    tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/$name.txt"
    count="$(rg -c "$pattern" "$RUN_ROOT/$name.txt" || true)"
    if (( count > previous )); then
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
  sleep 0.2
  tmux send-keys -t "$TUI_SESSION" C-m
}

session_id_from_capture() {
  rg -o 'session_[0-9a-f]{8}' "$1" | tail -1
}

wait_for_message_count() {
  local session_dir="$1" minimum="$2" description="$3"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    if [[ -f "$session_dir/messages.jsonl" ]] &&
      (( $(wc -l < "$session_dir/messages.jsonl") >= minimum )); then
      return 0
    fi
    sleep 0.5
  done
  echo "timeout waiting for $description" >&2
  return 1
}

wait_for_job_status() {
  local job_id="$1" status="$2" description="$3"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    if [[ -f "$JOBS_DIR/$job_id.yaml" ]] && rg -q "^status: $status$" "$JOBS_DIR/$job_id.yaml"; then
      return 0
    fi
    sleep 0.5
  done
  echo "timeout waiting for $description" >&2
  return 1
}

set_target_updated_at() {
  local session_dir="$1" timestamp="$2"
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
text = re.sub(r"(?m)^closed_at:.*$", "closed_at: null", text)
path.write_text(text)
PY
}

inject_invalid_tool_pair() {
  local session_dir="$1"
  python3 - "$session_dir" <<'PY'
import json
import re
import sys
from pathlib import Path

session_dir = Path(sys.argv[1])
metadata_path = session_dir / "session.yaml"
metadata = metadata_path.read_text()
count = int(re.search(r"(?m)^message_count: (\d+)$", metadata).group(1))
model = re.search(r"(?m)^model: (.+)$", metadata).group(1).strip().strip("'").strip('"')
created_at = "2026-09-01T00:00:00Z"
messages = [
    {
        "index": count,
        "role": "user",
        "content": [{
            "type": "text",
            "text": "CONTROLLED-INVALID-TURN-731",
        }],
        "created_at": created_at,
        "model": model,
    },
    {
        "index": count + 1,
        "role": "assistant",
        "content": [{
            "type": "invalid_tool_use",
            "id": "call_real_smoke_invalid",
            "name": "file_read",
            "error": "function_call.arguments was not a JSON object",
        }],
        "created_at": created_at,
        "model": model,
    },
    {
        "index": count + 2,
        "role": "user",
        "content": [{
            "type": "tool_result",
            "tool_use_id": "call_real_smoke_invalid",
            "content": '{"ok":false,"outcome":{"kind":"dispatch_failure"}}',
        }],
        "created_at": created_at,
        "model": model,
    },
    {
        "index": count + 3,
        "role": "assistant",
        "content": [{
            "type": "text",
            "text": "CONTROLLED-INVALID-CONTINUED-731",
        }],
        "created_at": created_at,
        "model": model,
    },
]
with (session_dir / "messages.jsonl").open("a") as stream:
    for message in messages:
        stream.write(json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n")
metadata = re.sub(r"(?m)^message_count: \d+$", f"message_count: {count + 4}", metadata)
metadata_path.write_text(metadata)
(session_dir / "provider_history.json").unlink(missing_ok=True)
PY
}

write_checkpoint() {
  local session_dir="$1" validity="$2"
  python3 - "$session_dir" "$validity" <<'PY'
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
trace_text: real LLM resume checkpoint fixture
trace_created_at: 2026-09-01T00:00:00Z
trace_id: null
status: prepared
""")
PY
}

write_finalize_job() {
  local session_id="$1" job_id="$2" status="$3" attempts="$4" created_at="$5" notify="$6"
  mkdir -p "$JOBS_DIR"
  cat > "$JOBS_DIR/$job_id.yaml" <<EOF
id: $job_id
agent_id: $AGENT_ID
kind:
  type: finalize
  session_id: $session_id
status: $status
attempts: $attempts
created_at: $created_at
updated_at: $created_at
notify_on_completion: $notify
EOF
  if [[ "$status" == "failed" ]]; then
    cat >> "$JOBS_DIR/$job_id.yaml" <<EOF
finished_at: $created_at
last_error: real LLM controlled fixture
EOF
  fi
}

write_recap_job() {
  local session_id="$1" job_id="$2" end_index="$3" created_at="$4"
  mkdir -p "$JOBS_DIR"
  cat > "$JOBS_DIR/$job_id.yaml" <<EOF
id: $job_id
agent_id: $AGENT_ID
kind:
  type: recap
  session_id: $session_id
  recap_end_index: $end_index
status: queued
attempts: 0
created_at: $created_at
updated_at: $created_at
notify_on_completion: false
EOF
}

hold_finalize_lock() {
  local session_dir="$1" ready="$RUN_ROOT/lock-ready-$RANDOM"
  python3 - "$session_dir/finalize.lock" "$ready" <<'PY' &
import fcntl
import signal
import sys
import time
from pathlib import Path

lock_path, ready_path = sys.argv[1], Path(sys.argv[2])
with open(lock_path, "a+") as lock_file:
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    ready_path.write_text("ready")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while True:
        time.sleep(1)
PY
  local pid="$!"
  LOCK_PIDS+=("$pid")
  LAST_LOCK_PID="$pid"
  for _ in $(seq 1 50); do
    [[ -s "$ready" ]] && break
    sleep 0.1
  done
  [[ -s "$ready" ]]
}

hold_foreground_finalize_locks() {
  local session_dir="$1" ready="$RUN_ROOT/foreground-lock-ready-$RANDOM"
  python3 - "$session_dir/runtime.lock" "$session_dir/finalize.lock" "$ready" <<'PY' &
import fcntl
import signal
import sys
import time
from pathlib import Path

runtime_path, finalize_path, ready_path = sys.argv[1], sys.argv[2], Path(sys.argv[3])
with open(runtime_path, "a+") as runtime_file, open(finalize_path, "a+") as finalize_file:
    fcntl.flock(runtime_file.fileno(), fcntl.LOCK_EX)
    fcntl.flock(finalize_file.fileno(), fcntl.LOCK_EX)
    ready_path.write_text("ready")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    while True:
        time.sleep(1)
PY
  local pid="$!"
  LOCK_PIDS+=("$pid")
  LAST_LOCK_PID="$pid"
  for _ in $(seq 1 50); do
    [[ -s "$ready" ]] && break
    sleep 0.1
  done
  [[ -s "$ready" ]]
}

release_lock() {
  local pid="$1"
  kill -TERM "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
}

assert_stderr_empty() {
  local path="$1"
  [[ ! -s "$path" ]] || {
    echo "$path is not empty" >&2
    return 1
  }
}

wait_for_pane_dead() {
  local description="$1"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    if [[ "$(tmux display-message -p -t "$TUI_SESSION" '#{pane_dead}')" == "1" ]]; then
      return 0
    fi
    sleep 0.5
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

assert_direct_resume_warning_before_prompt() {
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
prompt = next(i for i, line in enumerate(lines) if line.startswith("› Whisper your wish here..."))
assert inbox < warning < prompt, (inbox, warning, prompt)
assert sum(not line.strip() for line in lines[inbox + 1:warning]) == 1, lines[inbox:warning + 1]
assert sum(not line.strip() for line in lines[warning + 1:prompt]) == 1, lines[warning:prompt + 1]
PY
}

assert_last_assistant_contains() {
  local session_dir="$1"
  shift
  python3 - "$session_dir/messages.jsonl" "$@" <<'PY'
import json
import sys

messages = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
assistant = next(message for message in reversed(messages) if message["role"] == "assistant")
text = "\n".join(
    block.get("text", "") for block in assistant["content"] if block.get("type") == "text"
)
for marker in sys.argv[2:]:
    assert marker in text, (marker, text)
PY
}

# 真实模型建立 canonical 历史；后续 fixture 只复制该 session，不伪造模型回答。
start_tui seed
capture_until seed_open 'focus [0-9]+[smh].*open' 'seed TUI open'
SEED_ID="$(session_id_from_capture "$RUN_ROOT/seed_open.txt")"
SEED_DIR="$SESSIONS_DIR/$SEED_ID"
SEED_BASE="$(rg -o '^message_count: [0-9]+' "$SEED_DIR/session.yaml" | awk '{print $2}')"
send_text '当前计算题的数据编号是 REAL-A-CODE-731。请原样引用该编号，并计算末尾数字 731 加 2 的结果。'
wait_for_message_count "$SEED_DIR" "$((SEED_BASE + 2))" 'real seed turn commit'
assert_last_assistant_contains "$SEED_DIR" REAL-A-CODE-731 733
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/seed_answer.txt"
tmux kill-session -t "$TUI_SESSION"
assert_stderr_empty "$RUN_ROOT/seed.stderr.log"

CONVERT_ID="session_a1a1a1a1"
PREPARED_ID="session_b2b2b2b2"
FAILURE_ID="session_c3c3c3c3"
FOREGROUND_ID="session_d4d4d4d4"
FIFO_ID="session_e5e5e5e5"
BLOCKER_ID="session_f6f6f6f6"
python3 - "$SEED_DIR" "$SESSIONS_DIR" "$SEED_ID" \
  "$CONVERT_ID" "$PREPARED_ID" "$FAILURE_ID" "$FOREGROUND_ID" "$FIFO_ID" \
  "$BLOCKER_ID" <<'PY'
import shutil
import sys
from pathlib import Path

source, parent, old, *targets = sys.argv[1:]
source, parent = Path(source), Path(parent)
for target in targets:
    destination = parent / target
    shutil.copytree(source, destination)
    for path in destination.rglob("*"):
        if path.is_file():
            path.write_bytes(path.read_bytes().replace(old.encode(), target.encode()))
shutil.rmtree(source)
PY

CONVERT_DIR="$SESSIONS_DIR/$CONVERT_ID"
PREPARED_DIR="$SESSIONS_DIR/$PREPARED_ID"
FAILURE_DIR="$SESSIONS_DIR/$FAILURE_ID"
FOREGROUND_DIR="$SESSIONS_DIR/$FOREGROUND_ID"
FIFO_DIR="$SESSIONS_DIR/$FIFO_ID"
BLOCKER_DIR="$SESSIONS_DIR/$BLOCKER_ID"
for dir in "$CONVERT_DIR" "$PREPARED_DIR" "$FAILURE_DIR" "$FOREGROUND_DIR" "$BLOCKER_DIR"; do
  mark_finalizing "$dir"
  rm -f "$dir/finalize_checkpoint.yaml"
done
inject_invalid_tool_pair "$CONVERT_DIR"
CONVERT_CANONICAL_SHA="$(shasum -a 256 "$CONVERT_DIR/messages.jsonl" | awk '{print $1}')"
CONVERT_SYSTEM_SHA="$(shasum -a 256 "$CONVERT_DIR/system_prompt.md" | awk '{print $1}')"
write_checkpoint "$PREPARED_DIR" valid
write_checkpoint "$FAILURE_DIR" invalid
write_checkpoint "$BLOCKER_DIR" valid
write_finalize_job "$CONVERT_ID" job_a1a1a1a1 failed 5 2026-09-01T01:00:00Z true
write_finalize_job "$PREPARED_ID" job_b2b2b2b2 failed 5 2026-09-01T02:00:00Z true
write_finalize_job "$FAILURE_ID" job_c3c3c3c3 failed 5 2026-09-01T03:00:00Z false
write_finalize_job "$BLOCKER_ID" job_f6f6f6f6 queued 0 2026-09-01T04:00:00Z false
FIFO_END="$(rg -o '^message_count: [0-9]+' "$FIFO_DIR/session.yaml" | awk '{print $2}')"
write_recap_job "$FIFO_ID" job_e5e5e5e5 "$FIFO_END" 2026-09-01T00:00:00Z

# 先让另一个 Finalize 占住 worker；A 转换后，B 的后到 Finalize 和两个 Recap 都留在队列中。
hold_finalize_lock "$BLOCKER_DIR"
BLOCKER_LOCK_PID="$LAST_LOCK_PID"
set_target_updated_at "$CONVERT_DIR" 2099-01-01T00:00:01Z
start_tui main
capture_until main_open 'focus [0-9]+[smh].*open' 'current session B open'
CURRENT_B_ID="$(session_id_from_capture "$RUN_ROOT/main_open.txt")"
CURRENT_B_DIR="$SESSIONS_DIR/$CURRENT_B_ID"
CURRENT_B_BASE="$(rg -o '^message_count: [0-9]+' "$CURRENT_B_DIR/session.yaml" | awk '{print $2}')"
send_text '当前计算题的数据编号是 REAL-B-CODE-204。请原样引用该编号，并计算 204 加 3 的结果。'
wait_for_message_count "$CURRENT_B_DIR" "$((CURRENT_B_BASE + 2))" 'current B real turn commit'
assert_last_assistant_contains "$CURRENT_B_DIR" REAL-B-CODE-204 207
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/main_answer.txt"
send_text '/resume'
capture_until convert_picker "$CONVERT_ID.*Finalizing" 'conversion target in picker'
tmux send-keys -t "$TUI_SESSION" Enter
capture_until convert_open "$CONVERT_ID type /" 'converted target open'
rg -q 'focus [0-9]+[smh].*open' "$RUN_ROOT/convert_open.txt"
rg -q '^  type: recap$' "$JOBS_DIR/job_a1a1a1a1.yaml"
rg -q '^status: open$' "$CONVERT_DIR/session.yaml"
rg -q 'CONTROLLED-INVALID-TURN-731' "$RUN_ROOT/convert_open.txt"
rg -q 'CONTROLLED-INVALID-CONTINUED-731' "$RUN_ROOT/convert_open.txt"
test "$(shasum -a 256 "$CONVERT_DIR/messages.jsonl" | awk '{print $1}')" = "$CONVERT_CANONICAL_SHA"
test "$(shasum -a 256 "$CONVERT_DIR/system_prompt.md" | awk '{print $1}')" = "$CONVERT_SYSTEM_SHA"
CONVERT_TARGET="$(rg -o '^message_count: [0-9]+' "$CONVERT_DIR/session.yaml" | awk '{print $2}')"
send_text '请从本会话之前的用户消息中找出以 REAL-A-CODE 开头的数据编号，原样引用它，并计算末尾数字加 11 的结果。'
wait_for_message_count "$CONVERT_DIR" "$((CONVERT_TARGET + 2))" 'real turn after invalid-tool history'
assert_last_assistant_contains "$CONVERT_DIR" REAL-A-CODE-731 742
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/convert_answer.txt"
if rg -q '末尾数字加 11' "$CURRENT_B_DIR/messages.jsonl"; then
  echo "resumed target input leaked into current session B" >&2
  exit 1
fi

# 放开 blocker 后，B 的 Finalize 虽在两个 Recap 之后到达，仍必须先完成；随后按 Recap FIFO。
release_lock "$BLOCKER_LOCK_PID"
wait_for_job_status job_f6f6f6f6 succeeded 'blocking Finalize completion'
wait_for_job_status "$(python3 - "$JOBS_DIR" "$CURRENT_B_ID" <<'PY'
import sys
from pathlib import Path

for path in Path(sys.argv[1]).glob("*.yaml"):
    text = path.read_text()
    if "type: finalize" in text and f"session_id: {sys.argv[2]}" in text:
        print(path.stem)
        break
PY
)" succeeded 'current B finalize priority'
wait_for_job_status job_e5e5e5e5 succeeded 'older FIFO recap completion'
wait_for_job_status job_a1a1a1a1 succeeded 'converted recap completion'
python3 - "$JOBS_DIR" "$CURRENT_B_ID" > "$RUN_ROOT/queue-order.txt" <<'PY'
import re
import sys
from datetime import datetime
from pathlib import Path

jobs = {}
for path in Path(sys.argv[1]).glob("*.yaml"):
    text = path.read_text()
    def field(name):
        match = re.search(rf"(?m)^{name}: (.+)$", text)
        return match.group(1) if match else None
    jobs[path.stem] = {
        "text": text,
        "created_at": field("created_at"),
        "finished_at": field("finished_at"),
        "attempts": int(field("attempts")),
        "notify": field("notify_on_completion"),
    }
converted = jobs["job_a1a1a1a1"]
older = jobs["job_e5e5e5e5"]
assert "type: recap" in converted["text"]
assert converted["created_at"].startswith("2026-09-01T01:00:00")
assert converted["attempts"] <= 5
assert converted["notify"] == "false"
parse = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00"))
assert parse(older["finished_at"]) <= parse(converted["finished_at"])
finalize = next(
    job for job in jobs.values()
    if "type: finalize" in job["text"] and f"session_id: {sys.argv[2]}" in job["text"]
)
assert parse(finalize["finished_at"]) <= parse(older["finished_at"])
print("later_finalize_finished_at=" + finalize["finished_at"])
print("older_recap_finished_at=" + older["finished_at"])
print("converted_recap_finished_at=" + converted["finished_at"])
PY
rg -q "^recapped_until: $CONVERT_TARGET$" "$CONVERT_DIR/session.yaml"

# Prepared checkpoint 必须等待原 Finalize Closed 后 reopen；等待期输入最终只属于目标。
hold_finalize_lock "$PREPARED_DIR"
PREPARED_LOCK_PID="$LAST_LOCK_PID"
PREPARED_BASE="$(rg -o '^message_count: [0-9]+' "$PREPARED_DIR/session.yaml" | awk '{print $2}')"
set_target_updated_at "$PREPARED_DIR" 2099-01-01T00:00:02Z
send_text '/resume'
capture_until prepared_picker "$PREPARED_ID.*Finalizing" 'prepared target in picker'
tmux send-keys -t "$TUI_SESSION" Enter
capture_until prepared_wait 'Resuming · Waiting for target finalization' 'prepared checkpoint wait'
rg -qF "Target resume $PREPARED_ID finalizing..." "$RUN_ROOT/prepared_wait.txt"
send_text '请从本会话之前的用户消息中找出以 REAL-A-CODE 开头的数据编号，原样引用它，并计算末尾数字加 19 的结果。'
capture_until prepared_queued 'queued=1' 'queued input during target wait'
release_lock "$PREPARED_LOCK_PID"
wait_for_message_count "$PREPARED_DIR" "$((PREPARED_BASE + 2))" 'queued real turn on resumed target'
assert_last_assistant_contains "$PREPARED_DIR" REAL-A-CODE-731 750
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/prepared_answer.txt"
if rg -q '末尾数字加 19' "$CONVERT_DIR/messages.jsonl"; then
  echo "queued input leaked into previous current session" >&2
  exit 1
fi
rg -q '末尾数字加 19' "$PREPARED_DIR/messages.jsonl"
rg -q '^status: open$' "$PREPARED_DIR/session.yaml"
test ! -e "$PREPARED_DIR/finalize_checkpoint.yaml"

# 无效 checkpoint 五次耗尽：B 保持 Open，本等待窗口输入不落入任何 canonical messages。
hold_finalize_lock "$FAILURE_DIR"
FAILURE_LOCK_PID="$LAST_LOCK_PID"
set_target_updated_at "$FAILURE_DIR" 2099-01-01T00:00:03Z
send_text '/resume'
capture_until failure_picker "$FAILURE_ID.*Finalizing" 'failed target in picker'
tmux send-keys -t "$TUI_SESSION" Enter
capture_until failure_wait 'Resuming · Waiting for target finalization' 'failed recovery wait'
rg -qF "Target resume $FAILURE_ID finalizing..." "$RUN_ROOT/failure_wait.txt"
send_text 'REAL-DISCARD-731'
release_lock "$FAILURE_LOCK_PID"
ERROR_PATTERN='This session is still finalizing; wait for finalization to complete before resuming\.'
capture_until failure_result "$ERROR_PATTERN" 'fixed recovery failure'
assert_picker_error_layout "$RUN_ROOT/failure_result.txt" "$FAILURE_ID"
if rg -q 'Queued input entered while resuming was discarded\.' "$RUN_ROOT/failure_result.txt"; then
  echo "resume failure displayed the removed queued-input notice" >&2
  exit 1
fi
if rg -q 'REAL-DISCARD-731' "$PREPARED_DIR/messages.jsonl" "$FAILURE_DIR/messages.jsonl"; then
  echo "discarded waiting input entered canonical messages" >&2
  exit 1
fi
rg -q '^status: open$' "$PREPARED_DIR/session.yaml"
rg -q '^status: finalizing$' "$FAILURE_DIR/session.yaml"
rg -q '^attempts: 5$' "$JOBS_DIR/job_c3c3c3c3.yaml"
tmux send-keys -t "$TUI_SESSION" Escape

# 真实前台 Finalize 同时持有 runtime/finalize 锁且不被抢占；同一固定错误覆盖 picker 与 direct。
hold_foreground_finalize_locks "$FOREGROUND_DIR"
FOREGROUND_LOCK_PID="$LAST_LOCK_PID"
set_target_updated_at "$FOREGROUND_DIR" 2099-01-01T00:00:04Z
send_text '/resume'
capture_until foreground_picker "$FOREGROUND_ID.*Finalizing" 'foreground target in picker'
tmux send-keys -t "$TUI_SESSION" Enter
FOREGROUND_ERROR="$FOREGROUND_ID is still finalizing foreground; Try again after its completion."
capture_until foreground_rejected "$FOREGROUND_ID is still finalizing foreground; Try again after its completion\." 'foreground Finalize rejection'
assert_picker_error_layout "$RUN_ROOT/foreground_rejected.txt" "$FOREGROUND_ID" "$FOREGROUND_ERROR"
tmux send-keys -t "$TUI_SESSION" Down
sleep 0.6
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/foreground_navigated.txt"
assert_picker_selection_moved_off_target "$RUN_ROOT/foreground_navigated.txt" "$FOREGROUND_ID"
tmux send-keys -t "$TUI_SESSION" Escape
send_text '/resume'
capture_until foreground_reopened "$FOREGROUND_ID.*Finalizing" 'reopened foreground picker'
if rg -qF "$FOREGROUND_ERROR" "$RUN_ROOT/foreground_reopened.txt"; then
  echo "picker retained the previous inline error after reopen" >&2
  exit 1
fi
kill -0 "$FOREGROUND_LOCK_PID"
rg -q '^status: finalizing$' "$FOREGROUND_DIR/session.yaml"
assert_stderr_empty "$RUN_ROOT/main.stderr.log"
tmux kill-session -t "$TUI_SESSION"

MCP_CONFIG="$ACN_HOME/$UPSTREAM/.mcp.json"
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
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$RUN_ROOT/direct_rejected.txt"
rg -qxF "Error: $FOREGROUND_ERROR" "$RUN_ROOT/direct.stderr.log"
if rg -v -q '^[[:space:]]*$|^Pane is dead \(status [0-9]+, .+\)$' "$RUN_ROOT/direct_rejected.txt"; then
  echo "direct foreground rejection left output in terminal scrollback" >&2
  exit 1
fi
kill -0 "$FOREGROUND_LOCK_PID"
tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
release_lock "$FOREGROUND_LOCK_PID"

# 前台执行者退出后形成 orphaned；下一次 direct resume 自动接管，不要求 supervisor retry。
start_tui direct_after "--resume $FOREGROUND_ID"
capture_until direct_after_open "$FOREGROUND_ID type /" 'direct resume after foreground executor exit'
rg -q 'focus [0-9]+[smh].*open' "$RUN_ROOT/direct_after_open.txt"
assert_direct_resume_warning_before_prompt "$RUN_ROOT/direct_after_open.txt"
rg -q '^status: open$' "$FOREGROUND_DIR/session.yaml"
assert_stderr_empty "$RUN_ROOT/direct_after.stderr.log"
tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
tui_terminate_owned_supervisors "$CONFIG" "$ACN_BINARY"

EVIDENCE_DIR="$RUN_ROOT/evidence"
mkdir -p "$EVIDENCE_DIR/jobs" "$EVIDENCE_DIR/sessions"
cp "$JOBS_DIR"/*.yaml "$EVIDENCE_DIR/jobs/"
if [[ -f "$AGENT_HOME/runtime/supervisor/supervisor.log" ]]; then
  cp "$AGENT_HOME/runtime/supervisor/supervisor.log" "$EVIDENCE_DIR/"
fi
for session_id in "$CONVERT_ID" "$PREPARED_ID" "$FAILURE_ID" "$FOREGROUND_ID" "$FIFO_ID" "$BLOCKER_ID" "$CURRENT_B_ID"; do
  mkdir -p "$EVIDENCE_DIR/sessions/$session_id"
  cp "$SESSIONS_DIR/$session_id/session.yaml" "$EVIDENCE_DIR/sessions/$session_id/"
  cp "$SESSIONS_DIR/$session_id/messages.jsonl" "$EVIDENCE_DIR/sessions/$session_id/"
done

test ! -s "$RUN_ROOT/seed.stderr.log"
test ! -s "$RUN_ROOT/main.stderr.log"
rg -qxF "Error: $FOREGROUND_ERROR" "$RUN_ROOT/direct.stderr.log"
test ! -s "$RUN_ROOT/direct_after.stderr.log"
if tmux has-session -t "$TUI_SESSION" 2>/dev/null; then
  echo "real LLM tmux session was not cleaned up" >&2
  exit 1
fi

echo "resume-finalizing real LLM TUI smoke passed; captures: $RUN_ROOT"
