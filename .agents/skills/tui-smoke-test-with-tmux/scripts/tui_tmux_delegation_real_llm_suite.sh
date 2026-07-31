#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

BASE_OUT="${BASE_OUT:-target/tui-scenarios/delegation-real-llm}"
REAL_LLM_HAPPY_REPEATS="${REAL_LLM_HAPPY_REPEATS:-3}"
REAL_LLM_BOUNDARY_REPEATS="${REAL_LLM_BOUNDARY_REPEATS:-2}"
REAL_LLM_LOCK_REPEATS="${REAL_LLM_LOCK_REPEATS:-1}"
REAL_LLM_DIFF_REPEATS="${REAL_LLM_DIFF_REPEATS:-1}"
REAL_LLM_WAIT_SECS="${REAL_LLM_WAIT_SECS:-420}"
REAL_LLM_STARTUP_WAIT_SECS="${REAL_LLM_STARTUP_WAIT_SECS:-8}"
REAL_LLM_WIDTH="${REAL_LLM_WIDTH:-132}"
REAL_LLM_HEIGHT="${REAL_LLM_HEIGHT:-40}"

mkdir -p "$BASE_OUT"
SUMMARY_PATH="$BASE_OUT/summary.md"
printf '# Delegation Real LLM TUI Smoke Suite\n\n' > "$SUMMARY_PATH"

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "required env var $name is empty; source export_env.sh first" >&2
    return 1
  fi
}

make_real_llm_config() {
  local run_root="$1"
  local config_path="$run_root/config.real-llm.toml"
  local acn_home="$run_root/acn_home"
  mkdir -p "$run_root"
  python3 - "$config_path" "$acn_home" <<'PY'
import json
import re
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
acn_home = str(Path(sys.argv[2]).resolve())
text = Path("config.toml").read_text()
text, acn_home_count = re.subn(
    r'(?m)^acn_home = .*$',
    "acn_home = " + json.dumps(acn_home),
    text,
    count=1,
)
text, provider_count = re.subn(
    r'(?m)^provider = "anthropic"$',
    'provider = "openai_compatible_chat"',
    text,
    count=1,
)
if acn_home_count != 1:
    raise SystemExit("expected exactly one acn_home entry in config.toml")
if provider_count != 1:
    raise SystemExit('expected exactly one agent provider = "anthropic" entry in config.toml')
config_path.write_text(text)
PY
  printf '%s\n' "$config_path"
}

send_prompt_text() {
  local name="$1"
  local text="$2"
  local prompt_path="$TUI_OUT_DIR_ABS/$name.prompt.txt"
  printf '%s' "$text" > "$prompt_path"
  tmux load-buffer -b "${TUI_SESSION}_prompt" "$prompt_path"
  tmux paste-buffer -t "$TUI_SESSION" -b "${TUI_SESSION}_prompt"
  sleep 0.3
  tmux send-keys -t "$TUI_SESSION" C-m
}

wait_until() {
  local timeout="$1"
  local message="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timeout waiting for: $message" >&2
  return 1
}

agent_home_for_run() {
  local run_root="$1"
  printf '%s\n' "$run_root/acn_home/dev/data/agents/agent-a"
}

latest_session_dir() {
  local run_root="$1"
  local agent_home
  agent_home="$(agent_home_for_run "$run_root")"
  local -a dirs=()
  if [[ -d "$agent_home/sessions" ]]; then
    while IFS= read -r dir; do
      dirs+=("$dir")
    done < <(find "$agent_home/sessions" -mindepth 1 -maxdepth 1 -type d -name 'session_*' -print | sort -r)
  fi
  if (( ${#dirs[@]} == 0 )); then
    return 1
  fi
  printf '%s\n' "${dirs[0]}"
}

delegation_dirs_for_session() {
  local session_dir="$1"
  if [[ ! -d "$session_dir/delegations" ]]; then
    return 0
  fi
  find "$session_dir/delegations" -mindepth 1 -maxdepth 1 -type d -name 'subagent_*' -print | sort
}

delegation_count_at_least() {
  local session_dir="$1"
  local expected="$2"
  local count
  count="$(delegation_dirs_for_session "$session_dir" | wc -l | tr -d ' ')"
  [[ "$count" =~ ^[0-9]+$ ]] && (( count >= expected ))
}

delegations_terminal_at_least() {
  local session_dir="$1"
  local expected="$2"
  local count=0
  local metadata
  while IFS= read -r metadata; do
    if rg -q '^status: (completed|failed|abandoned)$' "$metadata"; then
      count=$((count + 1))
    fi
  done < <(find "$session_dir/delegations" -mindepth 2 -maxdepth 2 -name delegation.yaml -print 2>/dev/null)
  (( count >= expected ))
}

delegations_completed_at_least() {
  local session_dir="$1"
  local expected="$2"
  local count=0
  local metadata
  while IFS= read -r metadata; do
    if rg -q '^status: completed$' "$metadata"; then
      count=$((count + 1))
    fi
  done < <(find "$session_dir/delegations" -mindepth 2 -maxdepth 2 -name delegation.yaml -print 2>/dev/null)
  (( count >= expected ))
}

delegation_status_count() {
  local session_dir="$1"
  local status="$2"
  local count=0
  local metadata
  while IFS= read -r metadata; do
    if rg -q "^status: $status$" "$metadata"; then
      count=$((count + 1))
    fi
  done < <(find "$session_dir/delegations" -mindepth 2 -maxdepth 2 -name delegation.yaml -print 2>/dev/null)
  printf '%s\n' "$count"
}

delegation_status_at_least() {
  local session_dir="$1"
  local status="$2"
  local expected="$3"
  local count
  count="$(delegation_status_count "$session_dir" "$status")"
  [[ "$count" =~ ^[0-9]+$ ]] && (( count >= expected ))
}

delegation_status_at_most() {
  local session_dir="$1"
  local status="$2"
  local expected="$3"
  local count
  count="$(delegation_status_count "$session_dir" "$status")"
  [[ "$count" =~ ^[0-9]+$ ]] && (( count <= expected ))
}

sample_running_status_at_most() {
  local session_dir="$1"
  local max_running="$2"
  local duration="$3"
  local terminal_expected="$4"
  local deadline=$((SECONDS + duration))
  local observed_running=0
  local count
  while (( SECONDS < deadline )); do
    count="$(delegation_status_count "$session_dir" "running")"
    if [[ "$count" =~ ^[0-9]+$ ]]; then
      if (( count > max_running )); then
        echo "running delegation count exceeded limit: $count > $max_running" >&2
        return 1
      fi
      if (( count > 0 )); then
        observed_running=1
      fi
    fi
    if (( observed_running == 1 )) && delegations_terminal_at_least "$session_dir" "$terminal_expected"; then
      return 0
    fi
    sleep 1
  done
  if (( observed_running == 0 )); then
    echo "running lock state was not observed before delegations advanced; final marker checks still apply" >&2
  fi
  return 0
}

file_contains() {
  local path="$1"
  local pattern="$2"
  [[ -f "$path" ]] && rg -q "$pattern" "$path"
}

file_contains_exactly_once() {
  local path="$1"
  local pattern="$2"
  [[ -f "$path" ]] || return 1
  local count
  count="$(rg -F -o "$pattern" "$path" | wc -l | tr -d ' ')"
  [[ "$count" == "1" ]]
}

events_contain() {
  local session_dir="$1"
  local pattern="$2"
  rg -q "$pattern" "$session_dir/delegations"/*/events.jsonl
}

metadata_contain() {
  local session_dir="$1"
  local pattern="$2"
  rg -q "$pattern" "$session_dir/delegations"/*/delegation.yaml
}

progress_contain() {
  local session_dir="$1"
  local pattern="$2"
  rg -q "$pattern" "$session_dir/delegations"/*/progress.json
}

assert_progress_written() {
  local session_dir="$1"
  local progress_file
  while IFS= read -r progress_file; do
    if [[ -s "$progress_file" ]]; then
      return 0
    fi
  done < <(find "$session_dir/delegations" -mindepth 2 -maxdepth 2 -type f -name progress.json -print)
  echo "delegation progress.json was not written" >&2
  return 1
}

result_contain() {
  local session_dir="$1"
  local pattern="$2"
  rg -q "$pattern" "$session_dir/delegations"/*/result.md
}

events_contain_all() {
  local session_dir="$1"
  shift
  local pattern
  for pattern in "$@"; do
    if ! events_contain "$session_dir" "$pattern"; then
      echo "events missing pattern: $pattern" >&2
      return 1
    fi
  done
}

progress_contain_all() {
  local session_dir="$1"
  shift
  local pattern
  for pattern in "$@"; do
    if ! progress_contain "$session_dir" "$pattern"; then
      echo "progress missing pattern: $pattern" >&2
      return 1
    fi
  done
}

result_contain_all() {
  local session_dir="$1"
  shift
  local pattern
  for pattern in "$@"; do
    if ! result_contain "$session_dir" "$pattern"; then
      echo "result missing pattern: $pattern" >&2
      return 1
    fi
  done
}

assert_tool_completed_line_has_literals() {
  local session_dir="$1"
  local tool_name="$2"
  shift 2
  local lines
  lines="$(rg -F "\"type\":\"tool_completed\",\"tool_name\":\"$tool_name\"" "$session_dir/delegations"/*/events.jsonl || true)"
  local literal
  for literal in "$@"; do
    lines="$(printf '%s\n' "$lines" | rg -F "$literal" || true)"
    if [[ -z "$lines" ]]; then
      echo "completed tool event for $tool_name missing literal: $literal" >&2
      return 1
    fi
  done
}

assert_forbidden_child_tools_absent() {
  local session_dir="$1"
  local pattern='"tool_name":"(ask_user|working_note|memory|consult_router|session_search|create_subagent|list_subagents|read_subagent|steer_subagent|wait_subagent|finalize_session)"'
  if rg -q "$pattern" "$session_dir/delegations"/*/events.jsonl; then
    echo "delegation child used a forbidden tool" >&2
    rg "$pattern" "$session_dir/delegations"/*/events.jsonl >&2 || true
    return 1
  fi
}

assert_no_secret_env_leaks() {
  local -a existing_targets=()
  local target
  for target in "$@"; do
    if [[ -e "$target" ]]; then
      existing_targets+=("$target")
    fi
  done
  if (( ${#existing_targets[@]} == 0 )); then
    return 0
  fi
  local name value
  for name in ACN_LLM_API_KEY ANTHROPIC_API_KEY OPENAI_API_KEY GEMINI_API_KEY GOOGLE_API_KEY; do
    value="${!name:-}"
    if [[ -n "$value" ]] && rg -F -q "$value" "${existing_targets[@]}"; then
      echo "secret env var $name leaked into delegation smoke artifacts" >&2
      return 1
    fi
  done
}

capture_has_patterns() {
  local capture="$1"
  shift
  tui_capture "$capture"
  local pattern
  for pattern in "$@"; do
    if ! rg -q "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt"; then
      return 1
    fi
  done
}

lock_background_status_or_active_turn_visible() {
  tui_capture "background_after_create"
  local capture="$TUI_OUT_DIR_ABS/background_after_create.txt"
  if rg -q "subagents? .*background" "$capture" && rg -q "/subagents" "$capture"; then
    return 0
  fi
  rg -q "Working ·|Called create_subagent|Calling create_subagent" "$capture"
}

assert_no_panel_cancel() {
  local capture="$1"
  tui_assert_not_contains "$capture" "\\bcancel\\b|取消" "delegation panel exposed a cancel affordance"
}

open_subagents_panel() {
  tui_send_keys "/subagents"
  sleep 0.1
  tui_send_keys Enter
}

append_summary() {
  printf '%s\n' "$*" >> "$SUMMARY_PATH"
}

start_tui_case() {
  local case_name="$1"
  local iter="$2"
  RUN_ROOT="$BASE_OUT/$case_name/iter-$iter"
  rm -rf "$RUN_ROOT"
  mkdir -p "$RUN_ROOT/work"
  CONFIG_PATH="$(make_real_llm_config "$RUN_ROOT")"
  local prestart="prepare_${case_name}_before_tui"
  if declare -F "$prestart" >/dev/null 2>&1; then
    "$prestart" "$iter"
  fi
  TUI_SESSION="acn_subagent_${case_name}_${iter}_$$"
  TUI_OUT_DIR="$RUN_ROOT/captures"
  TUI_COMMAND="cargo run --quiet --bin acn -- --config '$CONFIG_PATH'"
  TUI_WIDTH="$REAL_LLM_WIDTH"
  TUI_HEIGHT="$REAL_LLM_HEIGHT"
  TUI_SKIP_BUILD="${TUI_SKIP_BUILD:-0}"
  tui_start
  sleep "$REAL_LLM_STARTUP_WAIT_SECS"
  tui_capture "initial"
  tui_assert_contains "initial" "ACN|open|initializing|Whisper your wish" "TUI did not start for $case_name/$iter"
}

finish_tui_case() {
  tui_capture "final"
  tui_assert_stderr_empty
  tui_cleanup
}

case_happy() {
  local iter="$1"
  start_tui_case "happy" "$iter"
  local marker="ACN_SUBAGENT_HAPPY_${iter}_$$"
  local rel_path="target/tui-scenarios/delegation-real-llm/happy/iter-$iter/work/happy.txt"
  local abs_path="$REPO_ROOT/$rel_path"
  mkdir -p "$(dirname "$abs_path")"
  local prompt="Create exactly one session subagent now with title smoke-happy-$iter and role tui verifier. Do not write files in the parent turn. Subagent objective: call update_subagent_progress with current_step writing; use file_write overwrite path '$rel_path' with content '$marker\nworkspace_root_visible\nno_api_key_value_visible\n'; call update_subagent_progress current_step done; final answer must include exactly these sections: Changed files: newline - $rel_path newline Artifacts: newline - $rel_path - happy-path smoke output. Parent-only instruction: in this parent turn, the only tool you may call is create_subagent; do not call code_run, file tools, web tools, read_subagent, or any other tool before or after create_subagent; this parent-only instruction must not be passed as a subagent constraint."
  send_prompt_text "happy" "$prompt"

  local session_dir
  wait_until "$REAL_LLM_WAIT_SECS" "session directory for happy/$iter" latest_session_dir "$RUN_ROOT"
  session_dir="$(latest_session_dir "$RUN_ROOT")"
  wait_until "$REAL_LLM_WAIT_SECS" "one delegation for happy/$iter" delegation_count_at_least "$session_dir" 1
  sleep 2
  open_subagents_panel
  sleep 1
  tui_capture "panel_after_create"
  tui_capture_ansi "panel_after_create"
  tui_assert_contains "panel_after_create" "Session Subagents" "/subagents panel did not show happy delegation header"
  tui_assert_contains "panel_after_create" "Update_time" "/subagents panel did not show update_time column"
  tui_assert_contains "panel_after_create" "read-only" "/subagents panel did not show read-only marker"
  tui_assert_contains "panel_after_create" "↑/↓ to navigate.*Esc to back" "/subagents panel did not show navigation hint"
  tui_assert_contains "panel_after_create" "smoke-happy" "/subagents panel did not show happy delegation title"
  assert_no_panel_cancel "panel_after_create"

  wait_until "$REAL_LLM_WAIT_SECS" "happy output file marker" file_contains "$abs_path" "$marker"
  wait_until "$REAL_LLM_WAIT_SECS" "happy delegation completed" delegations_completed_at_least "$session_dir" 1
  sleep 2
  tui_capture "panel_completed"
  tui_assert_contains "panel_completed" "completed" "happy completed panel missing completed status"
  tui_assert_contains "panel_completed" "changed:" "happy completed panel missing changed label"
  tui_assert_contains "panel_completed" "$rel_path" "happy completed panel missing changed path"
  tui_assert_not_contains "panel_completed" "^  result:" "happy completed panel unexpectedly exposed the internal result ref"
  tui_send_keys Escape
  sleep 1
  tui_capture "panel_closed"
  tui_assert_not_contains "panel_closed" "Session Subagents" "Esc did not close happy panel"
  metadata_contain "$session_dir" "owner_agent_id: agent-a"
  metadata_contain "$session_dir" "parent_session_id: session_"
  result_contain "$session_dir" "$rel_path"
  events_contain "$session_dir" "file_write"
  events_contain "$session_dir" "update_subagent_progress"
  assert_forbidden_child_tools_absent "$session_dir"
  assert_no_secret_env_leaks "$session_dir/delegations" "$abs_path"
  finish_tui_case
  append_summary "- happy/$iter passed: $session_dir"
}

prepare_boundary_before_tui() {
  local iter="$1"
  local upstream_root="$RUN_ROOT/acn_home/dev"
  local mcp_config="$upstream_root/.mcp.json"
  local marker_file="$RUN_ROOT/mcp_boundary_marker.txt"
  local server_script="$RUN_ROOT/mcp_boundary_server.py"
  mkdir -p "$upstream_root"
  printf '%s\n' "$BOUNDARY_MCP_TOOL_MARKER" > "$marker_file"
  python3 - "$server_script" <<'PY'
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(r'''import json
import sys
from pathlib import Path

marker_path = Path(sys.argv[1])

def send(message):
    print(json.dumps(message, separators=(",", ":")), flush=True)

for line in sys.stdin:
    try:
        request = json.loads(line)
    except json.JSONDecodeError:
        continue
    method = request.get("method")
    request_id = request.get("id")
    if method == "initialize":
        result = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "boundary-mcp", "version": "1.0.0"},
        }
    elif method == "tools/list":
        result = {
            "tools": [
                {
                    "name": "boundary_probe",
                    "description": "Return the hidden boundary smoke marker.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                }
            ]
        }
    elif method == "tools/call":
        params = request.get("params") or {}
        if params.get("name") == "boundary_probe":
            result = {
                "content": [
                    {"type": "text", "text": marker_path.read_text().strip()}
                ],
                "isError": False,
            }
        else:
            result = {
                "content": [{"type": "text", "text": "unknown tool"}],
                "isError": True,
            }
    else:
        if request_id is None:
            continue
        result = {}
    if request_id is not None:
        send({"jsonrpc": "2.0", "id": request_id, "result": result})
''')
PY
  chmod +x "$server_script"
  python3 - "$mcp_config" "$server_script" "$marker_file" "$BOUNDARY_MCP_FILE_MARKER" <<'PY'
import json
import sys
from pathlib import Path

config_path = Path(sys.argv[1])
server_script = str(Path(sys.argv[2]).resolve())
marker_file = str(Path(sys.argv[3]).resolve())
config_marker = sys.argv[4]
config_path.write_text(json.dumps({
    "mcp_boundary_marker": config_marker,
    "mcpServers": {
        "boundary": {
            "type": "stdio",
            "command": "python3",
            "args": [server_script, marker_file],
            "startup_timeout_secs": 30,
            "tool_timeout_secs": 30,
        }
    }
}, separators=(",", ":")) + "\n")
PY
}

case_boundary() {
  local iter="$1"
  BOUNDARY_OUTSIDE_MARKER="BOUNDARY_OUTSIDE_OK_${iter}_$$"
  BOUNDARY_MCP_FILE_MARKER="BOUNDARY_MCP_FILE_OK_${iter}_$$"
  BOUNDARY_WEB_MARKER="BOUNDARY_WEB_REQUEST_OK_${iter}_$$"
  BOUNDARY_CODE_MARKER="BOUNDARY_CODE_RUN_OK_${iter}_$$"
  BOUNDARY_MCP_TOOL_MARKER="BOUNDARY_MCP_TOOL_OK_${iter}_$$"
  start_tui_case "boundary" "$iter"
  local work_rel="target/tui-scenarios/delegation-real-llm/boundary/iter-$iter/work"
  local outside_dir
  outside_dir="$(mktemp -d "${TMPDIR:-/tmp}/acn-deleg-boundary-outside.XXXXXX")"
  local outside_marker="$BOUNDARY_OUTSIDE_MARKER"
  local mcp_file_marker="$BOUNDARY_MCP_FILE_MARKER"
  local web_marker="$BOUNDARY_WEB_MARKER"
  local code_marker="$BOUNDARY_CODE_MARKER"
  local mcp_tool_marker="$BOUNDARY_MCP_TOOL_MARKER"
  local web_pid=""
  cleanup_boundary_resources() {
    if [[ -n "${web_pid:-}" ]]; then
      kill "$web_pid" >/dev/null 2>&1 || true
      wait "$web_pid" 2>/dev/null || true
    fi
    if [[ -n "${outside_dir:-}" && -d "$outside_dir" ]]; then
      rm -rf "$outside_dir"
    fi
  }
  trap cleanup_boundary_resources RETURN
  mkdir -p "$REPO_ROOT/$work_rel"
  local outside_abs="$outside_dir/outside.txt"
  printf '%s\n' "$outside_marker" > "$outside_abs"
  local runtime_mcp_config="$RUN_ROOT/acn_home/dev/.mcp.json"
  local code_probe="$RUN_ROOT/code_probe.sh"
  {
    printf '#!/usr/bin/env bash\n'
    printf "printf '%%s\\\\n' %q\n" "$code_marker"
    printf "printf 'ACN_SUBAGENT_ID=%%s\\\\n' \"\$ACN_SUBAGENT_ID\"\n"
    printf "printf 'ACN_PARENT_SESSION_ID=%%s\\\\n' \"\$ACN_PARENT_SESSION_ID\"\n"
    printf 'case "$ACN_SUBAGENT_ID" in subagent_*) ;; *) printf "bad ACN_SUBAGENT_ID=%%s\\\\n" "$ACN_SUBAGENT_ID" >&2; exit 41 ;; esac\n'
    printf 'case "$ACN_PARENT_SESSION_ID" in session_*) ;; *) printf "bad ACN_PARENT_SESSION_ID=%%s\\\\n" "$ACN_PARENT_SESSION_ID" >&2; exit 42 ;; esac\n'
  } > "$code_probe"
  chmod +x "$code_probe"
  local web_root="$RUN_ROOT/web"
  local web_port_file="$RUN_ROOT/web_port.txt"
  mkdir -p "$web_root"
  printf '%s\n' "$web_marker" > "$web_root/probe.txt"
  python3 - "$web_root" "$web_port_file" <<'PY' &
import http.server
import socketserver
import sys
from functools import partial
from pathlib import Path

root = Path(sys.argv[1])
port_file = Path(sys.argv[2])
class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return

handler = partial(QuietHandler, directory=str(root))
with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
    port_file.write_text(str(httpd.server_address[1]))
    httpd.serve_forever()
PY
  web_pid=$!
  wait_until 10 "local boundary web server port" test -s "$web_port_file"
  local web_port
  web_port="$(cat "$web_port_file")"
  local prompt="Create exactly one session subagent now with title smoke-boundary-$iter and role capability verifier. Subagent objective: call update_subagent_progress current_step boundary-start; use file_read with absolute path '$outside_abs' and include the exact token found in that file; use file_read with absolute path '$runtime_mcp_config' and include the exact value of mcp_boundary_marker from the selected upstream runtime MCP config; use web_request GET url 'http://127.0.0.1:$web_port/probe.txt' and include the exact response body token; use code_run to run exactly bash '$code_probe' and include the exact stdout token plus the exact ACN_SUBAGENT_ID=... and ACN_PARENT_SESSION_ID=... lines printed by that script; call MCP tool mcp__boundary__boundary_probe with empty arguments and include the exact returned text token; call update_subagent_progress current_step boundary-done with a concise summary containing outside_read_ok, mcp_file_read_ok, localhost_web_request_ok, code_run_ok, mcp_tool_ok; final answer must contain boundary_capability_complete, the exact five observed tokens, and the exact ACN_SUBAGENT_ID=subagent... and ACN_PARENT_SESSION_ID=session... lines observed from code_run. Do not create files. Parent-only instruction: in this parent turn, the only tool you may call is create_subagent; do not call code_run, file tools, web tools, read_subagent, or any other tool before or after create_subagent; this parent-only instruction must not be passed as a subagent constraint."
  send_prompt_text "boundary" "$prompt"

  local session_dir
  wait_until "$REAL_LLM_WAIT_SECS" "session directory for boundary/$iter" latest_session_dir "$RUN_ROOT"
  session_dir="$(latest_session_dir "$RUN_ROOT")"
  wait_until "$REAL_LLM_WAIT_SECS" "one delegation for boundary/$iter" delegation_count_at_least "$session_dir" 1
  sleep 2
  open_subagents_panel
  sleep 1
  tui_capture "panel_after_create"
  tui_assert_contains "panel_after_create" "Session Subagents" "/subagents panel did not show boundary delegation header"
  tui_assert_contains "panel_after_create" "Update_time" "/subagents panel did not show boundary update_time column"
  tui_assert_contains "panel_after_create" "read-only" "/subagents panel did not show read-only marker"
  tui_assert_contains "panel_after_create" "↑/↓ to navigate.*Esc to back" "/subagents panel did not show navigation hint"
  tui_assert_contains "panel_after_create" "smoke-boundary" "/subagents panel did not show boundary delegation title"
  assert_no_panel_cancel "panel_after_create"
  wait_until "$REAL_LLM_WAIT_SECS" "boundary delegation completed" delegations_completed_at_least "$session_dir" 1
  sleep 2
  tui_capture "panel_terminal"
  tui_assert_contains "panel_terminal" "Session Subagents" "boundary terminal panel missing header"
  tui_assert_contains "panel_terminal" "read-only" "boundary terminal panel missing read-only marker"
  tui_assert_contains "panel_terminal" "completed" "boundary panel did not show completed status"
  tui_assert_not_contains "panel_terminal" "\\bfailed\\b" "boundary capability smoke unexpectedly showed failed status"
  tui_assert_not_contains "panel_terminal" "Called code_run" "boundary parent turn unexpectedly called code_run"
  tui_assert_contains "panel_terminal" "boundary" "boundary panel did not show readable boundary text"
  local delegation_id
  delegation_id="$(find "$session_dir/delegations" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort | head -n 1)"
  local session_id
  session_id="$(basename "$session_dir")"
  if [[ -z "$delegation_id" || -z "$session_id" ]]; then
    echo "failed to derive delegation/session ids for boundary smoke" >&2
    return 1
  fi
  events_contain_all "$session_dir" \
    "file_read" \
    "web_request" \
    "code_run" \
    "mcp__boundary__boundary_probe" \
    "$outside_abs" \
    "\\.mcp\\.json" \
    "127\\.0\\.0\\.1:$web_port" \
    '"type":"tool_completed","tool_name":"file_read"' \
    '"type":"tool_completed","tool_name":"web_request"' \
    '"type":"tool_completed","tool_name":"code_run"' \
    '"type":"tool_completed","tool_name":"mcp__boundary__boundary_probe"'
  assert_tool_completed_line_has_literals "$session_dir" \
    "code_run" \
    "tool code_run ok" \
    "ACN_SUBAGENT_ID=$delegation_id" \
    "ACN_PARENT_SESSION_ID=$session_id"
  assert_tool_completed_line_has_literals "$session_dir" \
    "mcp__boundary__boundary_probe" \
    "tool mcp__boundary__boundary_probe ok" \
    "$mcp_tool_marker"
  assert_progress_written "$session_dir"
  result_contain_all "$session_dir" \
    "boundary_capability_complete" \
    "$outside_marker" \
    "$mcp_file_marker" \
    "$web_marker" \
    "$code_marker" \
    "$mcp_tool_marker" \
    "ACN_SUBAGENT_ID=$delegation_id" \
    "ACN_PARENT_SESSION_ID=$session_id"
  assert_forbidden_child_tools_absent "$session_dir"
  assert_no_secret_env_leaks "$session_dir/delegations"
  finish_tui_case
  cleanup_boundary_resources
  trap - RETURN
  append_summary "- boundary/$iter passed: $session_dir"
}

case_lock() {
  local iter="$1"
  start_tui_case "lock" "$iter"
  local rel_path="target/tui-scenarios/delegation-real-llm/lock/iter-$iter/work/shared.txt"
  local abs_path="$REPO_ROOT/$rel_path"
  mkdir -p "$(dirname "$abs_path")"
  printf 'lock-test-start\n' > "$abs_path"
  local prompt="Create exactly seven session subagents now, with titles smoke-lock-a-$iter, smoke-lock-b-$iter, smoke-lock-c-$iter, smoke-lock-d-$iter, smoke-lock-e-$iter, smoke-lock-f-$iter, smoke-lock-g-$iter and role lock verifier. Do not write files in the parent turn. Subagent objective for each child: use file_write mode append on the same path '$rel_path' and append exactly one unique token: LOCK_A_$iter, LOCK_B_$iter, LOCK_C_$iter, LOCK_D_$iter, LOCK_E_$iter, LOCK_F_$iter, or LOCK_G_$iter. Each subagent must call update_subagent_progress before and after the append. Each final answer must include Changed files: newline - $rel_path. Parent-only instruction: in this parent turn, the only tools you may call are the seven create_subagent calls; do not call code_run, file tools, web tools, read_subagent, or any other tool before, between, or after those create_subagent calls; this parent-only instruction must not be passed as a subagent constraint."
  send_prompt_text "lock" "$prompt"

  local session_dir
  wait_until "$REAL_LLM_WAIT_SECS" "session directory for lock/$iter" latest_session_dir "$RUN_ROOT"
  session_dir="$(latest_session_dir "$RUN_ROOT")"
  wait_until "$REAL_LLM_WAIT_SECS" "seven delegations for lock/$iter" delegation_count_at_least "$session_dir" 7
  delegation_status_at_most "$session_dir" "running" 6
  sleep 1
  wait_until 30 "lock background status line or active parent turn" lock_background_status_or_active_turn_visible
  open_subagents_panel
  sleep 1
  tui_capture "panel_many"
  tui_assert_contains "panel_many" "Session Subagents" "lock panel missing header"
  tui_assert_contains "panel_many" "read-only" "lock panel missing read-only marker"
  tui_assert_contains "panel_many" "↑/↓ to navigate.*Esc to back" "lock panel missing navigation hint"
  tui_assert_contains "panel_many" "smoke-lock" "lock panel missing delegation titles"
  tui_assert_contains "panel_many" "Status[[:space:]]+Update_time[[:space:]]+Title[[:space:]]+Role[[:space:]]+Latest" "lock panel list header missing"
  assert_no_panel_cancel "panel_many"
  sample_running_status_at_most "$session_dir" 6 30 7
  wait_until "$REAL_LLM_WAIT_SECS" "lock delegations terminal" delegations_terminal_at_least "$session_dir" 7
  for marker in LOCK_A_$iter LOCK_B_$iter LOCK_C_$iter LOCK_D_$iter LOCK_E_$iter LOCK_F_$iter LOCK_G_$iter; do
    wait_until "$REAL_LLM_WAIT_SECS" "shared file marker $marker" file_contains "$abs_path" "$marker"
    if ! file_contains_exactly_once "$abs_path" "$marker"; then
      echo "shared file marker $marker did not appear exactly once" >&2
      cat "$abs_path" >&2
      return 1
    fi
  done
  sleep 2
  tui_capture "panel_completed"
  tui_assert_contains "panel_completed" "completed" "lock completed panel missing completed status"
  tui_assert_contains "panel_completed" "changed:" "lock completed panel missing changed label"
  tui_assert_contains "panel_completed" "$rel_path" "lock completed panel missing changed path"
  events_contain "$session_dir" "file_write"
  assert_forbidden_child_tools_absent "$session_dir"
  finish_tui_case
  append_summary "- lock/$iter passed: $session_dir"
}

case_diff() {
  local iter="$1"
  start_tui_case "diff" "$iter"
  local rel_path="target/tui-scenarios/delegation-real-llm/diff/iter-$iter/work/fixture.txt"
  local before_rel="target/tui-scenarios/delegation-real-llm/diff/iter-$iter/work/fixture.before.txt"
  local abs_path="$REPO_ROOT/$rel_path"
  local before_abs="$REPO_ROOT/$before_rel"
  local marker="new_line_from_delegation_$iter"
  mkdir -p "$(dirname "$abs_path")"
  printf 'alpha\nold_line\nomega\n' > "$abs_path"
  cp "$abs_path" "$before_abs"
  local prompt="Create exactly one session subagent now with title smoke-diff-$iter and role patch verifier. Do not write files in the parent turn. Subagent objective: use file_patch on path '$rel_path' replacing old_content 'old_line' with new_content '$marker'; call update_subagent_progress before and after patch; final answer must include Changed files: newline - $rel_path. Parent-only instruction: in this parent turn, the only tool you may call is create_subagent; do not call code_run, file tools, web tools, read_subagent, or any other tool before or after create_subagent; this parent-only instruction must not be passed as a subagent constraint."
  send_prompt_text "diff_create" "$prompt"

  local session_dir
  wait_until "$REAL_LLM_WAIT_SECS" "session directory for diff/$iter" latest_session_dir "$RUN_ROOT"
  session_dir="$(latest_session_dir "$RUN_ROOT")"
  wait_until "$REAL_LLM_WAIT_SECS" "one delegation for diff/$iter" delegation_count_at_least "$session_dir" 1
  wait_until "$REAL_LLM_WAIT_SECS" "diff fixture patched" file_contains "$abs_path" "$marker"
  wait_until "$REAL_LLM_WAIT_SECS" "diff delegation completed" delegations_completed_at_least "$session_dir" 1
  open_subagents_panel
  sleep 1
  tui_capture "panel_completed"
  tui_assert_contains "panel_completed" "completed" "diff panel missing completed status"
  tui_assert_contains "panel_completed" "changed:" "diff panel missing changed label"
  tui_assert_contains "panel_completed" "$rel_path" "diff panel missing changed file"
  tui_send_keys Escape
  sleep 1
  local diff_prompt="Use code_run now to execute exactly: diff -u '$before_rel' '$rel_path' || true . Then show the diff in your answer and mention that it has one removed line and one added line."
  send_prompt_text "diff_show" "$diff_prompt"
  wait_until "$REAL_LLM_WAIT_SECS" "diff answer visible" capture_has_patterns "diff_answer" "Called code_run|Calling code_run" "\\-old_line" "\\+new_line_from_delegation_$iter" "$rel_path"
  events_contain "$session_dir" "file_patch"
  result_contain "$session_dir" "$rel_path"
  assert_forbidden_child_tools_absent "$session_dir"
  finish_tui_case
  append_summary "- diff/$iter passed: $session_dir"
}

main() {
  if [[ -f export_env.sh ]]; then
    # shellcheck disable=SC1091
    source export_env.sh
  fi
  require_env ACN_LLM_API_KEY
  append_summary "- real LLM provider: openai_compatible_chat"
  append_summary "- repeats: happy=$REAL_LLM_HAPPY_REPEATS boundary=$REAL_LLM_BOUNDARY_REPEATS lock=$REAL_LLM_LOCK_REPEATS diff=$REAL_LLM_DIFF_REPEATS"
  TUI_BUILD_COMMAND="${TUI_BUILD_COMMAND:-cargo build --quiet --bin acn}"

  local i
  for ((i = 1; i <= REAL_LLM_HAPPY_REPEATS; i++)); do
    case_happy "$i"
  done
  for ((i = 1; i <= REAL_LLM_BOUNDARY_REPEATS; i++)); do
    case_boundary "$i"
  done
  for ((i = 1; i <= REAL_LLM_LOCK_REPEATS; i++)); do
    case_lock "$i"
  done
  for ((i = 1; i <= REAL_LLM_DIFF_REPEATS; i++)); do
    case_diff "$i"
  done

  append_summary ""
  append_summary "All delegation real LLM TUI smoke batches passed."
  printf 'Delegation real LLM TUI smoke suite passed. Summary: %s\n' "$SUMMARY_PATH"
}

main "$@"
