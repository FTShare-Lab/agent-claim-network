#!/usr/bin/env bash

# Shared helpers for ACN TUI tmux scenarios. Source this file from a scenario
# script after setting any TUI_* variables that differ from the defaults.

TUI_SESSION="${TUI_SESSION:-acn_tui_scenario}"
TUI_WIDTH="${TUI_WIDTH:-120}"
TUI_HEIGHT="${TUI_HEIGHT:-36}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenario}"
TUI_COMMAND="${TUI_COMMAND:-cargo run --quiet --bin acn -- --config config.toml}"
TUI_BUILD_COMMAND="${TUI_BUILD_COMMAND:-cargo build --quiet --bin acn}"
TUI_SKIP_BUILD="${TUI_SKIP_BUILD:-0}"

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
    eval "$TUI_BUILD_COMMAND"
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

tui_start() {
  tui_require_tmux
  tui_prepare_output
  tui_build_if_needed
  tui_write_runner
  trap tui_cleanup EXIT
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
