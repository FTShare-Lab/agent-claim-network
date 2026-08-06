#!/usr/bin/env bash
# 真实 LLM 的后台 shell / PTY 控制面回归。它不使用 fake provider 或预录 tool response。
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

if [[ -f export_env.sh ]]; then
  # shellcheck disable=SC1091
  source export_env.sh
fi
# 本场景需要核验 `/ps` 的状态 ANSI 色；开发环境常设置 NO_COLOR，必须在启动真实 TUI
# 前显式撤销它，不能让标题等无关 style 掩盖状态色回归。
unset NO_COLOR
[[ -n "${ACN_LLM_API_KEY:-}" ]] || {
  echo "ACN_LLM_API_KEY is required; source export_env.sh first" >&2
  exit 1
}

BASE_OUT="${BACKGROUND_SHELL_REAL_LLM_OUT_DIR:-target/tui-scenarios/background-shell-real-llm}"
if [[ "$BASE_OUT" == /* ]]; then
  RUN_ROOT="$BASE_OUT/$(date +%Y%m%d-%H%M%S)-$$"
else
  # 真实模型可以显式传入任意 `cwd`；fixture 路径必须不依赖当前 shell/workspace。
  RUN_ROOT="$REPO_ROOT/$BASE_OUT/$(date +%Y%m%d-%H%M%S)-$$"
fi
CONFIG_PATH="$RUN_ROOT/config.toml"
CONFIG_SOURCE="${BACKGROUND_SHELL_REAL_LLM_CONFIG:-$REPO_ROOT/config.toml}"
if [[ ! -f "$CONFIG_SOURCE" ]]; then
  CONFIG_SOURCE="$REPO_ROOT/config.toml"
fi
ACN_HOME="$RUN_ROOT/acn_home"
PROCESS_PID_PATH="$RUN_ROOT/managed-root.pid"
PROCESS_PGID_PATH="$RUN_ROOT/managed-root.pgid"
PROCESS_PGIDS_PATH="$RUN_ROOT/managed.pgids"
PROCESS_STARTED_PATH="$RUN_ROOT/managed-started.log"
VIEWPORT_STARTED_PATH="$RUN_ROOT/viewport-started.log"
VIEWPORT_PGID_PREFIX="$RUN_ROOT/managed-viewport"
SUBAGENT_STARTED_PATH="$RUN_ROOT/subagent-started.log"
SUBAGENT_PGID_PREFIX="$RUN_ROOT/managed-subagent"
MCP_CONFIG_PATH="$ACN_HOME/dev/.mcp.json"
MCP_FIXTURE_PATH="$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/shared_mcp_real_llm_fixture.sh"
MCP_FIXTURE_LOG="$RUN_ROOT/mcp-fixture.jsonl"
MCP_FIXTURE_INIT_COUNT="$RUN_ROOT/mcp-initialize-count.txt"
MCP_FIXTURE_CANCELLED_DIR="$RUN_ROOT/mcp-cancelled-requests"
MCP_FIXTURE_TIMEOUT_ONCE_STATE="$RUN_ROOT/mcp-timeout-once-used"
SUBAGENT_TERMINATE_CAPTURE=""
TUI_SESSION="acn_background_shell_real_llm_$$"
TUI_OUT_DIR="$RUN_ROOT"
TUI_COMMAND="cargo run --quiet --bin acn -- --config $CONFIG_PATH"
TUI_WIDTH=132
TUI_HEIGHT=40
WAIT_SECS="${BACKGROUND_SHELL_REAL_LLM_WAIT_SECS:-180}"

mkdir -p "$RUN_ROOT" "$(dirname "$MCP_CONFIG_PATH")"
python3 - "$CONFIG_SOURCE" "$CONFIG_PATH" "$ACN_HOME" "${BACKGROUND_SHELL_REAL_LLM_MODEL:-}" <<'PY'
import json
import re
import sys
from pathlib import Path

source, target, home = map(Path, sys.argv[1:4])
model_override = sys.argv[4]
text = source.read_text()
text, home_count = re.subn(
    r"(?m)^acn_home\s*=\s*.*$", "acn_home = " + json.dumps(str(home.resolve())), text, count=1
)
text, provider_count = re.subn(
    r'(?ms)(^\[agent\.llm\]\n.*?^provider\s*=\s*)"[^"]*"',
    r'\1"openai_chat"',
    text,
    count=1,
)
if home_count != 1 or provider_count != 1:
    raise SystemExit("failed to derive real-LLM config")
if model_override:
    text, model_count = re.subn(
        r'(?ms)(^\[agent\.llm\]\n.*?^model\s*=\s*)"[^"]*"',
        r'\1' + json.dumps(model_override),
        text,
        count=1,
    )
    if model_count != 1:
        raise SystemExit("failed to apply BACKGROUND_SHELL_REAL_LLM_MODEL")
target.write_text(text)
PY
python3 - "$MCP_CONFIG_PATH" "$MCP_FIXTURE_PATH" "$MCP_FIXTURE_LOG" "$MCP_FIXTURE_INIT_COUNT" "$MCP_FIXTURE_TIMEOUT_ONCE_STATE" "$MCP_FIXTURE_CANCELLED_DIR" <<'PY'
import json
import sys
from pathlib import Path

target, fixture, log, count, timeout_once, cancelled = map(Path, sys.argv[1:])
target.write_text(json.dumps({"mcpServers": {"shared": {
    "type": "stdio",
    "command": "bash",
    "args": [str(fixture)],
    "env": {
        "MCP_FIXTURE_LOG": str(log),
        "MCP_FIXTURE_INIT_COUNT": str(count),
        "MCP_FIXTURE_TIMEOUT_ONCE_STATE": str(timeout_once),
        "MCP_FIXTURE_CANCELLED_DIR": str(cancelled),
    },
    "startup_timeout_secs": 30,
    "tool_timeout_secs": 8,
}}}, indent=2) + "\n")
PY

fixture_group_is_still_owned_by_this_run() {
  local pgid="$1" leader_pgid leader_command
  [[ "$pgid" =~ ^[0-9]+$ ]] || return 1
  leader_pgid="$(ps -p "$pgid" -o pgid= 2>/dev/null | tr -d '[:space:]')"
  leader_command="$(ps -p "$pgid" -o command= 2>/dev/null || true)"
  [[ "$leader_pgid" == "$pgid" && "$leader_command" == *"$RUN_ROOT"* ]]
}

terminate_fixture_group_if_still_owned() {
  local pgid="$1"
  if fixture_group_is_still_owned_by_this_run "$pgid"; then
    kill -KILL -- "-$pgid" 2>/dev/null || true
  fi
}

fixture_pid_is_still_owned_by_this_run() {
  local pid="$1" command
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"$RUN_ROOT"* ]]
}

terminate_fixture_pid_if_still_owned() {
  local pid="$1"
  if fixture_pid_is_still_owned_by_this_run "$pid"; then
    kill -KILL "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  tui_cleanup
  # `/exit` 可以把最终收束交给测试专用 supervisor；它和 TUI 不在同一个 tmux
  # session，必须按这次生成的 config 再做一次精确清理，不能遗留到开发环境。
  while IFS= read -r supervisor_pid_path; do
    supervisor_pid="$(tr -d '[:space:]' < "$supervisor_pid_path")"
    [[ "$supervisor_pid" =~ ^[0-9]+$ ]] || continue
    supervisor_command="$(ps -p "$supervisor_pid" -o command= 2>/dev/null || true)"
    if [[ "$supervisor_command" == *"acn supervisor run --config $CONFIG_PATH"* ]]; then
      kill -TERM "$supervisor_pid" 2>/dev/null || true
    fi
  done < <(find "$ACN_HOME" -path '*/runtime/supervisor/supervisor.pid' -type f -print)
  if [[ -f "$PROCESS_PGIDS_PATH" ]]; then
    while IFS= read -r pgid; do
      pgid="${pgid//[[:space:]]/}"
      if [[ "$pgid" =~ ^[0-9]+$ ]]; then
        # fixture 的 numeric PGID/PID 可在测试收束后被内核复用；只在当前组长仍是
        # 本次 RUN_ROOT 命令时才信号，校验失败宁可交给 supervisor/OS 回收。
        terminate_fixture_group_if_still_owned "$pgid"
      fi
    done < "$PROCESS_PGIDS_PATH"
  elif [[ -f "$PROCESS_PGID_PATH" ]]; then
    pgid="$(tr -d '[:space:]' < "$PROCESS_PGID_PATH")"
    if [[ "$pgid" =~ ^[0-9]+$ ]]; then
      terminate_fixture_group_if_still_owned "$pgid"
    fi
  fi
  if [[ -f "$PROCESS_PID_PATH" ]]; then
    pid="$(tr -d '[:space:]' < "$PROCESS_PID_PATH")"
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
      terminate_fixture_pid_if_still_owned "$pid"
    fi
  fi
  if [[ -f "$MCP_FIXTURE_LOG" ]]; then
    while IFS= read -r fixture_pid; do
      [[ "$fixture_pid" =~ ^[0-9]+$ ]] || continue
      fixture_command="$(ps -p "$fixture_pid" -o command= 2>/dev/null || true)"
      if [[ "$fixture_command" == *"$MCP_FIXTURE_PATH"* ]]; then
        kill -TERM "$fixture_pid" 2>/dev/null || true
      fi
    done < <(perl -MJSON::PP -e '
      while (<>) {
        my $event = eval { decode_json($_) };
        print "$event->{pid}\n" if $event && defined $event->{pid};
      }
    ' "$MCP_FIXTURE_LOG" | sort -u)
  fi
}

assert_process_group_live_path() {
  local pgid_path="$1" description="$2" pgid
  [[ -f "$pgid_path" ]] || {
    echo "$description never recorded its PGID: $pgid_path" >&2
    exit 1
  }
  pgid="$(tr -d '[:space:]' < "$pgid_path")"
  [[ "$pgid" =~ ^[0-9]+$ ]] || {
    echo "$description PGID is invalid: $pgid" >&2
    exit 1
  }
  ps -eo pid=,pgid= | awk -v expected="$pgid" '$2 == expected { found = 1 } END { exit !found }' || {
    echo "$description is not live: pgid=$pgid" >&2
    exit 1
  }
}

assert_managed_process_group_live() {
  assert_process_group_live_path "$PROCESS_PGID_PATH" "registered root fixture process group"
}

assert_all_managed_process_groups_exited() {
  [[ -f "$PROCESS_PGIDS_PATH" ]] || return 0
  local pgid
  while IFS= read -r pgid; do
    pgid="${pgid//[[:space:]]/}"
    [[ "$pgid" =~ ^[0-9]+$ ]] || continue
    if ps -eo pid=,pgid= | awk -v expected="$pgid" '$2 == expected { found = 1 } END { exit !found }'; then
      echo "session close left a managed process-group member alive: pgid=$pgid" >&2
      exit 1
    fi
  done < <(sort -u "$PROCESS_PGIDS_PATH")
}

wait_all_managed_process_groups_exited() {
  local deadline=$((SECONDS + 15))
  while (( SECONDS < deadline )); do
    if ! ps -eo pid=,pgid= | awk '
      NR == FNR { expected[$1] = 1; next }
      expected[$2] { found = 1 }
      END { exit found }
    ' <(sort -u "$PROCESS_PGIDS_PATH") -; then
      return 0
    fi
    sleep 0.1
  done
  assert_all_managed_process_groups_exited
}

wait_managed_process_group_exited() {
  local pgid_path="$1" deadline=$((SECONDS + 15)) pgid
  [[ -f "$pgid_path" ]] || {
    echo "selected fixture never recorded PGID: $pgid_path" >&2
    return 1
  }
  pgid="$(tr -d '[:space:]' < "$pgid_path")"
  [[ "$pgid" =~ ^[0-9]+$ ]] || {
    echo "selected fixture PGID is invalid: $pgid_path" >&2
    return 1
  }
  while (( SECONDS < deadline )); do
    if ! ps -eo pid=,pgid= | awk -v expected="$pgid" '$2 == expected { found = 1 } END { exit !found }'; then
      return 0
    fi
    sleep 0.1
  done
  echo "/ps terminate did not kill its selected process group: pgid=$pgid" >&2
  return 1
}

wait_any_process_group_exited() {
  local deadline=$((SECONDS + 15)) pgid_path
  while (( SECONDS < deadline )); do
    for pgid_path in "$@"; do
      [[ -f "$pgid_path" ]] || continue
      local pgid
      pgid="$(tr -d '[:space:]' < "$pgid_path")"
      [[ "$pgid" =~ ^[0-9]+$ ]] || continue
      if ! ps -eo pid=,pgid= | awk -v expected="$pgid" '$2 == expected { found = 1 } END { exit !found }'; then
        printf '%s\n' "$pgid_path"
        return 0
      fi
    done
    sleep 0.1
  done
  echo "/ps terminate did not kill any expected fixture process group" >&2
  return 1
}

selected_terminate_pgid_path() {
  local capture_path="$1" command_block label
  # 确认页的滚屏 capture 同时包含上方的聊天历史；只能从 Command: 之后取 marker，
  # 否则此前 root fixture 的 ACN_ROOT 会误把当前选中的 viewport 行映射到 root PGID。
  command_block="$(sed -n '/^Command:$/,$p' "$capture_path")"
  if rg -Fq 'ACN_ROOT' <<<"$command_block"; then
    printf '%s\n' "$PROCESS_PGID_PATH"
    return 0
  fi
  for label in ALPHA BRAVO CHARLIE DELTA; do
    # 真实模型偶尔会把注释保留为字面量 `ACN_VIEWPORT_LABEL`，但实际 fixture
    # 命令仍会替换 echo 与 PGID 文件名；两种形式都能无歧义映射到被选进程组。
    if rg -Fq "ACN_VIEWPORT_$label" <<<"$command_block" ||
      rg -Fq "managed-viewport-$label.pgid" <<<"$command_block"; then
      printf '%s-%s.pgid\n' "$VIEWPORT_PGID_PREFIX" "$label"
      return 0
    fi
  done
  for label in ALPHA BRAVO; do
    if rg -Fq "ACN_CHILD_$label" <<<"$command_block" ||
      rg -Fq "managed-subagent-$label.pgid" <<<"$command_block"; then
      printf '%s-%s.pgid\n' "$SUBAGENT_PGID_PREFIX" "$label"
      return 0
    fi
  done
  echo "unable to map TerminateConfirm command to its fixture PGID" >&2
  return 1
}

wait_capture() {
  local capture="$1" pattern="$2" description="$3"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    tui_capture "$capture"
    if rg -q -- "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt"; then
      return 0
    fi
    sleep 1
  done
  echo "timeout waiting for $description ($pattern)" >&2
  return 1
}

wait_for_file() {
  local path="$1" description="$2"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    [[ -s "$path" ]] && return 0
    sleep 0.05
  done
  echo "timeout waiting for $description ($path)" >&2
  return 1
}

wait_for_line_count() {
  local path="$1" expected="$2" description="$3"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    if [[ -f "$path" ]] && (( $(wc -l < "$path") >= expected )); then
      return 0
    fi
    sleep 0.05
  done
  echo "timeout waiting for $description ($path: $expected lines)" >&2
  return 1
}

wait_for_pty_process_id() {
  local deadline=$((SECONDS + WAIT_SECS)) event_path process_id
  while (( SECONDS < deadline )); do
    event_path="$(find "$ACN_HOME/dev/data/agents/agent-a/sessions" -name turn_events.jsonl -type f -print | sort | tail -n 1)"
    if [[ -n "$event_path" ]]; then
      process_id="$(perl -MJSON::PP -e '
        while (<>) {
          my $event = eval { decode_json($_) } or next;
          next unless ($event->{kind} // q{}) eq q{tool_call_completed};
          next unless (($event->{outcome} // {})->{kind} // q{}) eq q{process_running};
          my $output = $event->{output_preview} // q{};
          if ($output =~ /"process_id":"([0-9a-f]{8})"/) {
            my $process_id = $1;
            if ($output =~ /ACN_PTY_READY/) {
              print $process_id;
            }
          }
        }
      ' "$event_path" | tail -n 1)"
      if [[ "$process_id" =~ ^[0-9a-f]{8}$ ]]; then
        PTY_PROCESS_ID="$process_id"
        return 0
      fi
    fi
    sleep 0.05
  done
  echo "timeout waiting for real PTY code_run process_id" >&2
  return 1
}

wait_for_short_code_run_result() {
  local deadline=$((SECONDS + WAIT_SECS)) event_path
  while (( SECONDS < deadline )); do
    event_path="$(find "$ACN_HOME/dev/data/agents/agent-a/sessions" -name turn_events.jsonl -type f -print | sort | tail -n 1)"
    if [[ -n "$event_path" ]] && perl -MJSON::PP -e '
      my (%names, %inputs);
      while (<>) {
        my $event = eval { decode_json($_) } or next;
        if (($event->{kind} // q{}) eq q{tool_call_started}) {
          $names{$event->{tool_use_id} // q{}} = $event->{name} // q{};
          $inputs{$event->{tool_use_id} // q{}} = $event->{input_preview} // q{};
          next;
        }
        next unless ($event->{kind} // q{}) eq q{tool_call_completed};
        next unless ($names{$event->{tool_use_id} // q{}} // q{}) eq q{code_run};
        my $input = $inputs{$event->{tool_use_id} // q{}} // q{};
        my $output = $event->{output_preview} // q{};
        if ($input =~ /ACN_SHORT_DONE/
          && (($event->{outcome} // {})->{kind} // q{}) eq q{process_exit}
          && (($event->{outcome} // {})->{exit_code} // -1) == 0
          && $output =~ /ACN_SHORT_DONE/) {
          exit 0;
        }
      }
      exit 1;
    ' "$event_path"; then
      return 0
    fi
    sleep 0.05
  done
  echo "timeout waiting for real short code_run process_exit result" >&2
  return 1
}

wait_for_pty_first_write_result() {
  local deadline=$((SECONDS + WAIT_SECS)) event_path
  while (( SECONDS < deadline )); do
    event_path="$(find "$ACN_HOME/dev/data/agents/agent-a/sessions" -name turn_events.jsonl -type f -print | sort | tail -n 1)"
    if [[ -n "$event_path" ]] && perl -MJSON::PP -e '
      my (%names, %inputs, $saw_list, $saw_first) = ((), (), 0, 0);
      while (<>) {
        my $event = eval { decode_json($_) } or next;
        if (($event->{kind} // q{}) eq q{tool_call_started}) {
          $names{$event->{tool_use_id} // q{}} = $event->{name} // q{};
          $inputs{$event->{tool_use_id} // q{}} = $event->{input_preview} // q{};
          next;
        }
        next unless ($event->{kind} // q{}) eq q{tool_call_completed};
        my $name = $names{$event->{tool_use_id} // q{}} // q{};
        $saw_list = 1 if $name eq q{process_list};
        next unless $name eq q{write_stdin};
        my $input = $inputs{$event->{tool_use_id} // q{}} // q{};
        my $output = $event->{output_preview} // q{};
        $saw_first = 1
          if $input =~ /"process_id":"\Q$ENV{PTY_PROCESS_ID}\E"/
          && $input =~ /"chars":"first"/
          && (($event->{outcome} // {})->{kind} // q{}) eq q{process_running}
          && $output =~ /ACN_PTY_FIRST=first/;
      }
      exit !($saw_list && $saw_first);
    ' "$event_path"; then
      return 0
    fi
    sleep 0.05
  done
  echo "timeout waiting for owner-scoped process_list and first real PTY write_stdin result" >&2
  return 1
}

wait_for_pty_terminal_result() {
  local deadline=$((SECONDS + WAIT_SECS)) event_path
  while (( SECONDS < deadline )); do
    event_path="$(find "$ACN_HOME/dev/data/agents/agent-a/sessions" -name turn_events.jsonl -type f -print | sort | tail -n 1)"
    if [[ -n "$event_path" ]] && perl -MJSON::PP -e '
      my (%names, %inputs);
      while (<>) {
        my $event = eval { decode_json($_) } or next;
        if (($event->{kind} // q{}) eq q{tool_call_started}) {
          $names{$event->{tool_use_id} // q{}} = $event->{name} // q{};
          $inputs{$event->{tool_use_id} // q{}} = $event->{input_preview} // q{};
          next;
        }
        next unless ($event->{kind} // q{}) eq q{tool_call_completed};
        next unless ($names{$event->{tool_use_id} // q{}} // q{}) eq q{write_stdin};
        my $input = $inputs{$event->{tool_use_id} // q{}} // q{};
        my $output = $event->{output_preview} // q{};
        if ($input =~ /"process_id":"\Q$ENV{PTY_PROCESS_ID}\E"/
          && $input =~ /"chars":"second"/
          && (($event->{outcome} // {})->{kind} // q{}) eq q{process_exit}
          && (($event->{outcome} // {})->{exit_code} // -1) == 0
          && $output =~ /ACN_PTY_SECOND=second/) {
          exit 0;
        }
      }
      exit 1;
    ' "$event_path"; then
      return 0
    fi
    sleep 0.05
  done
  echo "timeout waiting for second real PTY write_stdin terminal result" >&2
  return 1
}

wait_for_mcp_event_count() {
  local pattern="$1" expected="$2" description="$3"
  local deadline=$((SECONDS + WAIT_SECS)) count
  while (( SECONDS < deadline )); do
    count=0
    if [[ -f "$MCP_FIXTURE_LOG" ]]; then
      count="$(rg -c -- "$pattern" "$MCP_FIXTURE_LOG" || true)"
      count="${count:-0}"
    fi
    if (( count >= expected )); then
      return 0
    fi
    sleep 0.05
  done
  echo "timeout waiting for $description ($pattern: $expected events)" >&2
  return 1
}

fixture_initialize_pid() {
  local initialize_index="$1"
  perl -MJSON::PP -e '
    my ($wanted, $seen) = (shift, 0);
    while (<>) {
      my $event = decode_json($_);
      next unless $event->{event} eq "initialize";
      $seen++;
      if ($seen == $wanted) {
        print $event->{pid};
        exit 0;
      }
    }
    exit 1;
  ' "$initialize_index" "$MCP_FIXTURE_LOG"
}

assert_fixture_pid_exited() {
  local fixture_pid="$1"
  if kill -0 "$fixture_pid" 2>/dev/null; then
    echo "old MCP fixture PID $fixture_pid is still alive after reconnect" >&2
    return 1
  fi
}

select_subagent_process_for_terminate() {
  local attempt capture command_block
  for attempt in {1..10}; do
    tui_send_keys t
    sleep 0.15
    capture="subagent_terminate_candidate_$attempt"
    tui_capture "$capture"
    if rg -Fq 'Owner: subagent_' "$TUI_OUT_DIR_ABS/$capture.txt"; then
      SUBAGENT_TERMINATE_CAPTURE="$capture"
      return 0
    fi
    tui_send_keys Escape
    sleep 0.15
    # `/ps` 默认按 started_at 倒序；先前选中的 root 通常在末行，因此向上遍历才能到达
    # 新建的 subagent terminal。
    tui_send_keys Up
    sleep 0.15
  done
  echo "could not select a subagent-owned process in /ps" >&2
  return 1
}

wait_capture_min_matches() {
  local capture="$1" pattern="$2" expected="$3" description="$4"
  local deadline=$((SECONDS + WAIT_SECS))
  while (( SECONDS < deadline )); do
    tui_capture "$capture"
    local count
    count="$({ rg -o -- "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt" || true; } | wc -l | tr -d '[:space:]')"
    if (( count >= expected )); then
      return 0
    fi
    sleep 0.1
  done
  echo "timeout waiting for $description ($pattern appeared fewer than $expected times)" >&2
  return 1
}

assert_ansi_status_color() {
  local capture="$1" status="$2" ansi_index="$3"
  python3 - "$TUI_OUT_DIR_ABS/$capture.ansi.txt" "$status" "$ansi_index" <<'PY'
import sys
from pathlib import Path

path, status, ansi_index = sys.argv[1:]
data = Path(path).read_bytes()
needle = status.encode()
color = f"\x1b[38;5;{ansi_index}m".encode()
for offset in range(len(data)):
    if data.startswith(needle, offset) and color in data[max(0, offset - 64):offset]:
        break
else:
    raise SystemExit(
        f"missing ANSI color {ansi_index} immediately before process status {status!r} in {path}"
    )
PY
}

wait_ansi_status_color() {
  local capture="$1" status="$2" ansi_index="$3"
  local deadline=$((SECONDS + 5))
  while (( SECONDS < deadline )); do
    tui_capture_ansi "$capture"
    if python3 - "$TUI_OUT_DIR_ABS/$capture.ansi.txt" "$status" "$ansi_index" <<'PY'
import sys
from pathlib import Path

path, status, ansi_index = sys.argv[1:]
data = Path(path).read_bytes()
needle = status.encode()
color = f"\x1b[38;5;{ansi_index}m".encode()
raise SystemExit(not any(
    data.startswith(needle, offset) and color in data[max(0, offset - 64):offset]
    for offset in range(len(data))
))
PY
    then
      return 0
    fi
    sleep 0.05
  done
  echo "timeout waiting for ANSI color $ansi_index on process status $status" >&2
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

tui_start
trap cleanup EXIT
wait_capture "initial" "ACN|initializing|open|agent-a" "TUI startup"

send_prompt "Use process tools only. Call code_run with type bash, tty false and yield_time_ms 10000. Its script must start with the exact first line '# ACN_ROOT', then execute: echo \$\$ > '$PROCESS_PID_PATH'; pgid=\$(ps -o pgid= -p \$\$ | tr -d '[:space:]'); printf '%s\\n' \"\$pgid\" > '$PROCESS_PGID_PATH'; printf '%s\\n' \"\$pgid\" >> '$PROCESS_PGIDS_PATH'; echo started > '$PROCESS_STARTED_PATH'; sleep 300; printf natural-finish. Do not use '&', kill, write_stdin, or answer while code_run is still in its initial observation window."
wait_for_file "$PROCESS_STARTED_PATH" "registered code_run to enter its initial yield"
# marker 只能说明 child 已 spawn；先在 active turn 打开 `/ps` 确认 entry 已登记，再退出
# 面板并 Esc。这样 D20 的断言不会把 reserve 前的取消误当作后台进程继续运行。
tui_send_keys "/ps" Enter
wait_capture "ps_before_cancel" "PROCESS ID.*OWNER.*STATUS" "registered code_run /ps panel before Esc"
tui_assert_contains "ps_before_cancel" "main.*running" "registered code_run row was absent before Esc"
tui_send_keys Escape
# 此时 initial yield 尚未结束。Esc 必须放弃当前 tool-call 等待而不是杀掉已登记的 entry。
tui_send_keys Escape
tui_capture "cancel_requested"
tui_assert_contains "cancel_requested" "Turn cancel pending: settling active tool calls" "Esc did not show the explicit-cancel pending notice"
wait_capture "cancelled_background" "Interrupted · process [0-9a-f]{8} continues in background" "explicit cancel continuation notice"
assert_managed_process_group_live

tui_send_keys "/ps" Enter
sleep 1
tui_capture "ps_live"
tui_assert_contains "ps_live" "Processes" "/ps did not open"
tui_assert_contains "ps_live" "running|terminating" "/ps did not show the managed process"
tui_capture_ansi "ps_live"
assert_ansi_status_color "ps_live" "running" 2

# unified exec 的真实 PTY 路径不能只靠单元测试。为避免真实模型在单个超长 prompt 中
# 卡在规划阶段，以下仍在同一 TUI/session 内，但拆成短回合依次核验短命令、PTY、
# owner-scoped process_list、两次 stdin 写入与自然终态。渲染检查看真实 ToolCell，
# 精确的 process_id 与 stdout 关联则看该 turn 的真实工具事件日志，不依赖模型文本。
tui_send_keys Escape
wait_capture "ps_closed_before_unified_pty" "┌ Idle" "closing /ps before the PTY turn"
send_prompt "Use code_run exactly once: type bash, tty false, yield_time_ms 250, script printf 'ACN_SHORT_DONE\\n'. Use no other tool, then stop."
wait_for_short_code_run_result
wait_capture "unified_short_terminal" "Process exit code: 0" "short unified code_run terminal result"
wait_capture "unified_short_idle" "┌ Idle" "short unified code_run idle"

send_prompt "Use code_run exactly once: type bash, tty true, yield_time_ms 250, script printf 'ACN_PTY_READY\\n'; IFS= read -r -n 5 first; printf 'ACN_PTY_FIRST=%s\\n' \"\$first\"; IFS= read -r -n 6 second; printf 'ACN_PTY_SECOND=%s\\n' \"\$second\". Use no other tool, then stop."
wait_for_pty_process_id
export PTY_PROCESS_ID
wait_capture "unified_pty_started_idle" "┌ Idle" "PTY code_run idle"

send_prompt "The managed PTY process_id is $PTY_PROCESS_ID. Call process_list exactly once, then call write_stdin exactly once with that same process_id, chars exactly first and no newline. Use no other tool, then stop."
wait_for_pty_first_write_result
wait_capture "unified_pty_first_write" "┌ Idle" "first PTY stdin turn idle"
tui_assert_contains "unified_pty_first_write" "process_list" "real model did not call owner-scoped process_list"
tui_assert_contains "unified_pty_first_write" "write_stdin" "real model did not call first write_stdin"

send_prompt "Call write_stdin exactly once with process_id $PTY_PROCESS_ID, chars exactly second and no newline. Wait for its natural terminal result. Use no other tool, then stop."
wait_for_pty_terminal_result
wait_capture "unified_pty_terminal" "┌ Idle" "PTY terminal turn idle"
tui_assert_contains "unified_pty_terminal" "write_stdin" "real model did not call second write_stdin"

# 真实模型必须启动多个独立 terminal，不能把多个 sleep 合并进一个 shell；这样 `/ps`
# 才会有超过 viewport 的 live rows。它们会在 session close 时由 ProcessSupervisor 清理。
send_prompt "Use process tools only. Start four separate code_run calls (not one combined script), all type bash, tty false, yield_time_ms 250. For labels ALPHA, BRAVO, CHARLIE, DELTA, each script must start with an exact first line '# ACN_VIEWPORT_LABEL', then contain these commands in order: echo LABEL >> '$VIEWPORT_STARTED_PATH'; pgid=\$(ps -o pgid= -p \$\$ | tr -d '[:space:]'); printf '%s\\n' \"\$pgid\" > '$VIEWPORT_PGID_PREFIX-LABEL.pgid'; printf '%s\\n' \"\$pgid\" >> '$PROCESS_PGIDS_PATH'; sleep 300. Replace every LABEL with that call's label. Do not use '&', write_stdin, kill, subagents, or any other tools. After all four have returned process_id, stop."
wait_for_line_count "$VIEWPORT_STARTED_PATH" 4 "four independently managed viewport processes"
for viewport_label in ALPHA BRAVO CHARLIE DELTA; do
  wait_for_file "$VIEWPORT_PGID_PREFIX-$viewport_label.pgid" "$viewport_label fixture PGID"
done

tui_send_keys "/ps" Enter
wait_capture_min_matches "ps_viewport_wide" "running" 5 "five live managed process rows before resize"
tmux resize-window -t "$TUI_SESSION" -x 48 -y 8
sleep 0.5
tui_capture "ps_narrow_initial"
tui_assert_contains "ps_narrow_initial" "PROCESS ID" "narrow /ps lost its required identity column"
tui_assert_contains "ps_narrow_initial" "owner=.*cmd=" "narrow /ps did not retain owner and command fields"
# 选中项在时间竞争下可能是首行（新快照已先应用）也可能是末行（旧 process_id
# 选中被保留）。先试 Down；如果刚好在末行再回退到 Up，避免把边界 no-op 误判为
# viewport 不会跟随。
tui_send_keys Down
sleep 0.3
tui_capture "ps_narrow_after_down"
if cmp -s "$TUI_OUT_DIR_ABS/ps_narrow_initial.txt" "$TUI_OUT_DIR_ABS/ps_narrow_after_down.txt"; then
  tui_send_keys Up
  sleep 0.3
fi
tui_capture "ps_narrow_scrolled"
if cmp -s "$TUI_OUT_DIR_ABS/ps_narrow_initial.txt" "$TUI_OUT_DIR_ABS/ps_narrow_scrolled.txt"; then
  echo "narrow /ps selection did not advance through an over-viewport list" >&2
  exit 1
fi

tmux resize-window -t "$TUI_SESSION" -x "$TUI_WIDTH" -y "$TUI_HEIGHT"
sleep 0.5
tui_capture "ps_wide_after_resize"
tui_assert_contains "ps_wide_after_resize" "Processes" "wide /ps did not recover after resize"

# 保证选中按 started_at 倒序排列的最新 viewport fixture，而不是早先保留选中的 root row。
for _ in {1..8}; do
  tui_send_keys Up
done
tui_send_keys t
sleep 1
tui_capture "terminate_confirm"
tui_assert_contains "terminate_confirm" "Command:" "confirmation omitted command renderer"
tui_assert_contains "terminate_confirm" "\[y\] Yes.*\[n/Esc\] No" "confirmation footer missing"
tui_capture_ansi "terminate_confirm"
assert_ansi_status_color "terminate_confirm" "running" 2
selected_viewport_pgid_path="$(selected_terminate_pgid_path "$TUI_OUT_DIR_ABS/terminate_confirm.txt")"
case "$selected_viewport_pgid_path" in
  "$VIEWPORT_PGID_PREFIX"-*.pgid) ;;
  *)
    echo "viewport terminate confirmation did not select a viewport fixture" >&2
    exit 1
    ;;
esac
tui_send_keys y
wait_ansi_status_color "terminating" "terminating" 3
# `wait_ansi_status_color` 已经捕获并确认包含黄色 terminating 的那一帧。不能再单独
# capture 一次：硬终止可在两次 capture 之间完成，届时行按 PRD 已应从 live 列表消失。
perl -pe 's/\e\[[0-?]*[ -\/]*[@-~]//g' "$TUI_OUT_DIR_ABS/terminating.ansi.txt" \
  > "$TUI_OUT_DIR_ABS/terminating.txt"
tui_assert_contains "terminating" "terminating" "terminate did not expose the optimistic terminating row"
wait_managed_process_group_exited "$selected_viewport_pgid_path"
for viewport_label in ALPHA BRAVO CHARLIE DELTA; do
  viewport_pgid_path="$VIEWPORT_PGID_PREFIX-$viewport_label.pgid"
  [[ "$viewport_pgid_path" == "$selected_viewport_pgid_path" ]] && continue
  assert_process_group_live_path "$viewport_pgid_path" "non-selected $viewport_label viewport process group"
done
tui_capture "after_terminate"
tui_assert_contains "after_terminate" "Processes" "terminate did not return to /ps"
tui_send_keys Escape

# `/mcp` 必须是 active-turn 面板：old generation 的 request 由 Reconnect 收束为
# dispatch failure，但当前 turn、replacement generation 和已有后台 terminal 都继续。
send_prompt "Call MCP tool mcp__shared__slow_read exactly once with an empty object. Do not call code_run, create subagents, write_stdin, process_list, or any other tool. Wait for the tool result before answering."
wait_for_mcp_event_count '"event":"start","tool":"slow_read"' 1 "active slow MCP call"
wait_capture "mcp_slow_read_live" "Calling mcp shared/slow_read" "live slow MCP ToolCell"
old_mcp_fixture_pid="$(fixture_initialize_pid 1)"
[[ "$old_mcp_fixture_pid" =~ ^[0-9]+$ ]] || {
  echo "first MCP fixture initialize did not expose a valid PID" >&2
  exit 1
}
tui_send_keys "/mcp" Enter
wait_capture "mcp_panel_active_turn" "MCP · servers" "active-turn /mcp panel"
tui_assert_contains "mcp_panel_active_turn" "shared" "active-turn /mcp omitted shared server"
tui_send_keys r
wait_for_mcp_event_count '"event":"initialize"' 2 "MCP replacement generation initialize"
wait_capture "mcp_panel_after_active_reconnect" "MCP server shared updated" "active-turn reconnect completion"
tui_assert_contains "mcp_panel_after_active_reconnect" "ready" "replacement MCP generation was not ready"
assert_fixture_pid_exited "$old_mcp_fixture_pid"
assert_managed_process_group_live
tui_send_keys Escape
wait_capture "mcp_reconnect_dispatch_failure" "Error: dispatch failed" "old MCP tool dispatch failure"
wait_capture "mcp_reconnect_turn_idle" "┌ Idle" "turn continuation after MCP reconnect"

send_prompt "Call MCP tool mcp__shared__ping exactly once with an empty object. Do not call code_run, create subagents, write_stdin, process_list, or any other tool. After it returns, answer only MCP_REPLACEMENT_PING_DONE."
wait_for_mcp_event_count '"event":"end","tool":"ping"' 1 "replacement MCP ping"
new_mcp_fixture_pid="$(fixture_initialize_pid 2)"
[[ "$new_mcp_fixture_pid" =~ ^[0-9]+$ && "$new_mcp_fixture_pid" != "$old_mcp_fixture_pid" ]] || {
  echo "MCP reconnect did not create a distinct replacement fixture process" >&2
  exit 1
}
perl -MJSON::PP -e '
  my ($pid, $seen) = (shift, 0);
  while (<>) {
    my $event = decode_json($_);
    $seen++ if $event->{event} eq "end" && $event->{tool} eq "ping" && $event->{pid} == $pid;
  }
  die "replacement MCP generation did not handle follow-up ping\n" unless $seen;
' "$new_mcp_fixture_pid" "$MCP_FIXTURE_LOG"
wait_capture "mcp_replacement_turn_idle" "┌ Idle" "replacement MCP turn idle"

# 两个真实 session subagent 分别登记自己的 terminal；主模型不应获得它们的 owner-scoped
# process_list 视图，但用户 `/ps` 必须聚合展示并允许只 terminate 其中一个。
send_prompt "Create exactly two session subagents now, named process-child-alpha and process-child-bravo. In this parent turn call only create_subagent twice; do not call code_run, process_list, write_stdin, or MCP tools yourself. Give child alpha this exact objective: call code_run once with type bash, tty false and yield_time_ms 250; start its script with the exact first line '# ACN_CHILD_ALPHA', then execute: echo ALPHA >> '$SUBAGENT_STARTED_PATH'; pgid=\$(ps -o pgid= -p \$\$ | tr -d '[:space:]'); printf '%s\\n' \"\$pgid\" > '$SUBAGENT_PGID_PREFIX-ALPHA.pgid'; printf '%s\\n' \"\$pgid\" >> '$PROCESS_PGIDS_PATH'; sleep 300. Once code_run returns process_id, call write_stdin exactly once using that process_id, empty chars, and yield_time_ms 300000; do not reply until that poll ends. Do not use '&', kill, process_list, MCP, or any other tool. Give child bravo the same objective with every ALPHA replaced by BRAVO and the first line '# ACN_CHILD_BRAVO'. Both children must start immediately and remain running after code_run returns process_id."
wait_for_line_count "$SUBAGENT_STARTED_PATH" 2 "two subagent-owned process fixtures"
for child_label in ALPHA BRAVO; do
  wait_for_file "$SUBAGENT_PGID_PREFIX-$child_label.pgid" "$child_label subagent fixture PGID"
done

tui_send_keys "/ps" Enter
wait_capture "ps_subagent_aggregate" "PROCESS ID.*OWNER.*STATUS" "aggregated /ps panel"
select_subagent_process_for_terminate
tui_assert_contains "$SUBAGENT_TERMINATE_CAPTURE" "Owner: subagent_" "selected /ps row did not belong to a subagent"
selected_subagent_pgid_path="$(selected_terminate_pgid_path "$TUI_OUT_DIR_ABS/$SUBAGENT_TERMINATE_CAPTURE.txt")"
case "$selected_subagent_pgid_path" in
  "$SUBAGENT_PGID_PREFIX"-*.pgid) ;;
  *)
    echo "subagent terminate confirmation did not select a subagent fixture" >&2
    exit 1
    ;;
esac
tui_send_keys y
wait_ansi_status_color "subagent_terminating" "terminating" 3
wait_managed_process_group_exited "$selected_subagent_pgid_path"
case "$selected_subagent_pgid_path" in
  *-ALPHA.pgid) other_subagent_pgid_path="$SUBAGENT_PGID_PREFIX-BRAVO.pgid" ;;
  *-BRAVO.pgid) other_subagent_pgid_path="$SUBAGENT_PGID_PREFIX-ALPHA.pgid" ;;
esac
assert_process_group_live_path "$other_subagent_pgid_path" "non-selected subagent process group"
assert_managed_process_group_live
tui_capture "ps_after_subagent_terminate"
tui_assert_contains "ps_after_subagent_terminate" "Processes" "subagent terminate did not return to /ps"
tui_send_keys Escape

[[ -f "$PROCESS_STARTED_PATH" ]] || {
  echo "fixture process was never started" >&2
  exit 1
}

tui_send_keys "/exit" Enter
sleep 1
wait_all_managed_process_groups_exited
tui_finish
tui_assert_stderr_empty
printf 'Background-shell real LLM smoke passed. Artifacts: %s\n' "$RUN_ROOT"
