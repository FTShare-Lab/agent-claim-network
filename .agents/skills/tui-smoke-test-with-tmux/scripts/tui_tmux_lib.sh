#!/usr/bin/env bash

# Shared helpers for ACN TUI tmux scenarios. Source this file from a scenario
# script after setting any TUI_* variables that differ from the defaults.

TUI_SESSION="${TUI_SESSION:-acn_tui_scenario}"
TUI_WIDTH="${TUI_WIDTH:-120}"
TUI_HEIGHT="${TUI_HEIGHT:-36}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenario}"
TUI_COMMAND="${TUI_COMMAND:-}"
TUI_BUILD_COMMAND="${TUI_BUILD_COMMAND:-cargo build --quiet --bin acn}"
TUI_SKIP_BUILD="${TUI_SKIP_BUILD:-0}"
TUI_SUPERVISOR_CONFIG="${TUI_SUPERVISOR_CONFIG:-}"
TUI_SUPERVISOR_BINARY="${TUI_SUPERVISOR_BINARY:-${TUI_ACN_BINARY:-acn}}"
TUI_SUPERVISOR_BASELINE_PIDS="${TUI_SUPERVISOR_BASELINE_PIDS:-}"
TUI_BUILD_ARTIFACTS_JSON="[]"

tui_capture_cargo_artifacts() {
  if [[ "$TUI_BUILD_COMMAND" == *"--message-format"* ]]; then
    echo "TUI_BUILD_COMMAND must not set --message-format; the runner adds JSON artifact output" >&2
    return 2
  fi
  eval "$TUI_BUILD_COMMAND --message-format=json-render-diagnostics" | python3 -c '
import json
import sys

artifacts = []
for line in sys.stdin:
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        sys.stderr.write(line)
        continue
    if message.get("reason") == "compiler-message":
        rendered = (message.get("message") or {}).get("rendered")
        if isinstance(rendered, str):
            sys.stderr.write(rendered)
    if message.get("reason") != "compiler-artifact":
        continue
    target = message.get("target") or {}
    executable = message.get("executable")
    if not isinstance(executable, str) or not executable:
        continue
    artifacts.append({
        "name": target.get("name"),
        "kind": target.get("kind") or [],
        "executable": executable,
    })
print(json.dumps(artifacts, separators=(",", ":")))
'
}

tui_built_artifact_path() {
  local target_name="$1"
  local target_kind="$2"
  local artifact
  artifact="$(python3 - "$target_name" "$target_kind" "$TUI_BUILD_ARTIFACTS_JSON" <<'PY'
import json
import sys

target_name, target_kind, raw = sys.argv[1:]
artifacts = json.loads(raw)
matches = {
    item["executable"]
    for item in artifacts
    if item.get("name") == target_name
    and target_kind in item.get("kind", [])
    and isinstance(item.get("executable"), str)
}
if len(matches) != 1:
    raise SystemExit(
        f"expected one Cargo executable artifact for {target_kind} {target_name}, "
        f"found {len(matches)}; set the corresponding TUI_*_BINARY override when skipping "
        "or wrapping the build"
    )
print(matches.pop())
PY
)" || return
  if [[ ! -x "$artifact" ]]; then
    echo "Cargo artifact is not executable: $artifact" >&2
    return 1
  fi
  printf '%s\n' "$artifact"
}

tui_resolve_binary() {
  local override_name="$1"
  local target_name="$2"
  local target_kind="$3"
  local override="${!override_name:-}"
  if [[ -n "$override" ]]; then
    if [[ ! -x "$override" ]]; then
      echo "$override_name is not executable: $override" >&2
      return 1
    fi
    printf '%s\n' "$override"
    return 0
  fi
  tui_built_artifact_path "$target_name" "$target_kind"
}

tui_require_python_tomllib() {
  if ! command -v python3 >/dev/null 2>&1 || ! python3 -c 'import tomllib' >/dev/null 2>&1; then
    echo "Python 3.11+ with tomllib is required for TUI scenario config inspection" >&2
    return 127
  fi
}

tui_config_agent_identity() {
  local config_path="$1"
  tui_require_python_tomllib || return
  python3 - "$config_path" <<'PY'
import sys
from pathlib import Path

import tomllib

config_path = Path(sys.argv[1])
config = tomllib.loads(config_path.read_text())
upstream = config.get("upstream")
if not isinstance(upstream, str) or not upstream.strip():
    raise SystemExit(f"{config_path}: top-level upstream must be a non-empty string")
upstream_config = (config.get("upstreams") or {}).get(upstream)
if not isinstance(upstream_config, dict):
    raise SystemExit(f"{config_path}: missing [upstreams.{upstream}]")
agent_id = upstream_config.get("agent_id")
if not isinstance(agent_id, str) or not agent_id.strip():
    raise SystemExit(f"{config_path}: [upstreams.{upstream}].agent_id must be non-empty")
if any(char in upstream or char in agent_id for char in "\t\r\n"):
    raise SystemExit(f"{config_path}: upstream and agent_id must not contain tabs or newlines")
print(f"{upstream}\t{agent_id}")
PY
}

tui_config_acn_home() {
  local config_path="$1"
  tui_require_python_tomllib || return
  python3 - "$config_path" <<'PY'
import sys
from pathlib import Path

import tomllib

config_path = Path(sys.argv[1])
config = tomllib.loads(config_path.read_text())
acn_home = (config.get("storage") or {}).get("acn_home")
if not isinstance(acn_home, str) or not acn_home.strip():
    raise SystemExit(f"{config_path}: [storage].acn_home must be a non-empty string")
print(Path(acn_home).expanduser().resolve())
PY
}

tui_require_tmux() {
  if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is required for TUI scenario tests" >&2
    return 127
  fi
}

tui_prepare_output() {
  mkdir -p "$TUI_OUT_DIR"
  TUI_OUT_DIR_ABS="$(cd "$TUI_OUT_DIR" && pwd)"
  TUI_STDERR_LOG="$TUI_OUT_DIR_ABS/stderr.log"
  TUI_RUNNER="$TUI_OUT_DIR_ABS/run_tui.sh"
  rm -f "$TUI_OUT_DIR_ABS"/*.txt "$TUI_STDERR_LOG" "$TUI_RUNNER"
}

tui_build_if_needed() {
  if [[ "$TUI_SKIP_BUILD" != "1" ]]; then
    TUI_BUILD_ARTIFACTS_JSON="$(tui_capture_cargo_artifacts)"
  fi
}

tui_write_runner() {
  local repo_root
  repo_root="$(pwd)"
  cat > "$TUI_RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$repo_root"
if [[ -f export_env.sh ]]; then
  # shellcheck disable=SC1091
  source export_env.sh
fi
{
$TUI_COMMAND
} 2> "$TUI_STDERR_LOG"
EOF
  chmod +x "$TUI_RUNNER"
}

tui_cleanup() {
  tmux kill-session -t "$TUI_SESSION" >/dev/null 2>&1 || true
}

tui_is_owned_supervisor_command() {
  local command="$1"
  local config_path="$2"
  local executable="$3"
  local expected="$executable supervisor run --config $config_path"
  # 候选已由预期 executable 或隔离 home 内的 supervisor.pid 确认；这里再匹配
  # executable 后紧邻的完整参数，防止 stale PID 被解释器或其他进程复用后误杀。
  [[ "$command" == "$expected" || "$command" == "$expected "* ]]
}

tui_owned_supervisor_pids() {
  local config_path="$1"
  local expected_executable="${2:-${TUI_ACN_BINARY:-acn}}"
  local process_snapshot command_snapshot acn_home pid_paths
  local pid_path pid executable command_pid command candidate_pids=""
  if ! process_snapshot="$(ps -Ao pid=,comm=)" || \
    ! command_snapshot="$(ps -Ao pid=,command=)"; then
    echo "failed to inspect processes while cleaning TUI supervisors" >&2
    return 1
  fi
  if ! acn_home="$(tui_config_acn_home "$config_path")"; then
    return 1
  fi
  pid_paths=""
  if [[ -d "$acn_home" ]] && \
    ! pid_paths="$(find "$acn_home" -path '*/runtime/supervisor/supervisor.pid' -type f -print)"; then
    echo "failed to inspect supervisor PID files under $acn_home" >&2
    return 1
  fi
  while read -r pid executable; do
    [[ "$pid" =~ ^[0-9]+$ && "${executable##*/}" == "${expected_executable##*/}" ]] || continue
    candidate_pids+="$pid"$'\n'
  done <<< "$process_snapshot"
  while IFS= read -r pid_path; do
    [[ -n "$pid_path" ]] || continue
    pid="$(tr -d '[:space:]' < "$pid_path")"
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    candidate_pids+="$pid"$'\n'
  done <<< "$pid_paths"
  while IFS= read -r pid; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    executable=""
    while read -r command_pid executable; do
      if [[ "$command_pid" == "$pid" ]]; then
        break
      fi
      executable=""
    done <<< "$process_snapshot"
    [[ -n "$executable" ]] || continue
    if [[ -L "/proc/$pid/exe" ]]; then
      executable="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
      [[ -n "$executable" ]] || continue
    fi
    command=""
    while read -r command_pid command; do
      if [[ "$command_pid" == "$pid" ]]; then
        break
      fi
      command=""
    done <<< "$command_snapshot"
    # 候选可能在两份完整快照之间退出；没有同 PID 命令行时不能复用旧数据。
    [[ -n "$command" ]] || continue
    if tui_is_owned_supervisor_command "$command" "$config_path" "$executable"; then
      printf '%s\n' "$pid"
    fi
  done < <(printf '%s' "$candidate_pids" | sort -un)
}

tui_pid_is_listed() {
  local target_pid="$1"
  local pid_list="$2"
  local listed_pid
  while IFS= read -r listed_pid; do
    if [[ "$listed_pid" == "$target_pid" ]]; then
      return 0
    fi
  done <<< "$pid_list"
  return 1
}

tui_terminate_owned_supervisors() {
  local config_path="$1"
  local expected_executable="${2:-${TUI_ACN_BINARY:-acn}}"
  local protected_pids="${3:-}"
  local attempt pid owned_pids found quiet_rounds=0
  # TUI 退出与 supervisor 启动可以交错；要求连续 1 秒没有匹配进程才认为收束。
  for ((attempt = 0; attempt < 30; attempt++)); do
    if ! owned_pids="$(tui_owned_supervisor_pids "$config_path" "$expected_executable")"; then
      return 1
    fi
    found=0
    while read -r pid; do
      [[ "$pid" =~ ^[0-9]+$ ]] || continue
      tui_pid_is_listed "$pid" "$protected_pids" && continue
      found=1
      kill -TERM "$pid" 2>/dev/null || true
    done <<< "$owned_pids"
    if (( found == 0 )); then
      quiet_rounds=$((quiet_rounds + 1))
      if (( quiet_rounds >= 10 )); then
        return 0
      fi
    else
      quiet_rounds=0
    fi
    sleep 0.1
  done
  # 只对经过完整 config 命令行二次确认的本次测试 supervisor 做硬收束。
  if ! owned_pids="$(tui_owned_supervisor_pids "$config_path" "$expected_executable")"; then
    return 1
  fi
  while read -r pid; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    tui_pid_is_listed "$pid" "$protected_pids" && continue
    kill -KILL "$pid" 2>/dev/null || true
  done <<< "$owned_pids"
}

tui_cleanup_runtime() {
  local cleanup_status=0
  tui_cleanup
  if [[ -n "$TUI_SUPERVISOR_CONFIG" && -f "$TUI_SUPERVISOR_CONFIG" ]]; then
    if ! tui_terminate_owned_supervisors \
      "$TUI_SUPERVISOR_CONFIG" \
      "$TUI_SUPERVISOR_BINARY" \
      "$TUI_SUPERVISOR_BASELINE_PIDS"; then
      cleanup_status=1
    fi
  fi
  return "$cleanup_status"
}

tui_start() {
  local default_binary existing_exit_trap
  tui_require_tmux
  tui_prepare_output
  tui_build_if_needed
  if [[ -z "$TUI_COMMAND" ]]; then
    default_binary="$(tui_resolve_binary TUI_ACN_BINARY acn bin)"
    TUI_COMMAND="'$default_binary' --config config.toml"
  fi
  if [[ -n "$TUI_SUPERVISOR_CONFIG" && -f "$TUI_SUPERVISOR_CONFIG" ]]; then
    TUI_SUPERVISOR_BASELINE_PIDS="$(
      tui_owned_supervisor_pids "$TUI_SUPERVISOR_CONFIG" "$TUI_SUPERVISOR_BINARY"
    )"
  else
    TUI_SUPERVISOR_BASELINE_PIDS=""
  fi
  tui_write_runner
  # 调用方可能已安装同时回收 fake server、临时目录和 supervisor 的完整 trap；
  # 只有没有 EXIT trap 时才提供 tmux fallback，不能在启动失败窗口覆盖调用方清理。
  existing_exit_trap="$(trap -p EXIT)"
  if [[ -z "$existing_exit_trap" ]]; then
    trap tui_cleanup_runtime EXIT
  fi
  tui_cleanup
  tmux new-session -d -s "$TUI_SESSION" -x "$TUI_WIDTH" -y "$TUI_HEIGHT" "$TUI_RUNNER"
}

tui_capture() {
  local name="$1"
  tmux capture-pane -t "$TUI_SESSION" -p > "$TUI_OUT_DIR_ABS/$name.txt"
}

tui_capture_ansi() {
  local name="$1"
  tmux capture-pane -t "$TUI_SESSION" -e -p > "$TUI_OUT_DIR_ABS/$name.ansi.txt"
}

tui_send_keys() {
  tmux send-keys -t "$TUI_SESSION" "$@"
}

tui_assert_contains() {
  local capture="$1"
  local pattern="$2"
  local message="$3"
  if ! rg -q "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt"; then
    echo "$message" >&2
    echo "missing pattern: $pattern" >&2
    echo "capture: $TUI_OUT_DIR_ABS/$capture.txt" >&2
    return 1
  fi
}

tui_assert_not_contains() {
  local capture="$1"
  local pattern="$2"
  local message="$3"
  if rg -q "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt"; then
    echo "$message" >&2
    echo "unexpected pattern: $pattern" >&2
    echo "capture: $TUI_OUT_DIR_ABS/$capture.txt" >&2
    return 1
  fi
}

tui_assert_stderr_empty() {
  if [[ -s "$TUI_STDERR_LOG" ]]; then
    echo "stderr.log is not empty: $TUI_STDERR_LOG" >&2
    return 1
  fi
}

tui_finish() {
  if tmux has-session -t "$TUI_SESSION" >/dev/null 2>&1; then
    tui_capture "final"
  fi
  tui_assert_stderr_empty
  echo "TUI scenario passed. Captures saved in $TUI_OUT_DIR_ABS"
}
