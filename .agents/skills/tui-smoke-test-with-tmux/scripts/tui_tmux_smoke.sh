#!/usr/bin/env bash
set -euo pipefail

SESSION="acn_tui_smoke"
WIDTH="120"
HEIGHT="36"
STARTUP_WAIT="30"
AFTER_WAIT="1"
EXIT_WAIT="1"
OUT_DIR="target/tui-smoke"
COMMAND="cargo run --quiet --bin acn -- --config config.toml"
BUILD_COMMAND="cargo build --quiet --bin acn"
SKIP_BUILD="0"
HELP_HEADER_PATTERN="ACN commands"
HELP_SKILLS_PATTERN="/skills[[:space:]]+list available skills"

usage() {
  cat <<'USAGE'
Usage: tui_tmux_smoke.sh [options]

Options:
  --session NAME       tmux session name (default: acn_tui_smoke)
  --width COLS         tmux width (default: 120)
  --height ROWS        tmux height (default: 36)
  --startup-wait SEC   maximum seconds to wait for startup (default: 30)
  --after-wait SEC     seconds after /help before capture (default: 1)
  --exit-wait SEC      seconds after /exit before cleanup (default: 1)
  --out-dir DIR        output directory (default: target/tui-smoke)
  --command COMMAND    command to run inside tmux
  --build-command CMD  build command before tmux run
  --skip-build         do not build before starting tmux
  -h, --help           show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --session)
      SESSION="$2"
      shift 2
      ;;
    --width)
      WIDTH="$2"
      shift 2
      ;;
    --height)
      HEIGHT="$2"
      shift 2
      ;;
    --startup-wait)
      STARTUP_WAIT="$2"
      shift 2
      ;;
    --after-wait)
      AFTER_WAIT="$2"
      shift 2
      ;;
    --exit-wait)
      EXIT_WAIT="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --command)
      COMMAND="$2"
      shift 2
      ;;
    --build-command)
      BUILD_COMMAND="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for TUI smoke tests" >&2
  exit 127
fi

REPO_ROOT="$(pwd)"
mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"
STDERR_LOG="$OUT_DIR_ABS/stderr.log"
RUNNER="$OUT_DIR_ABS/run_tui.sh"

rm -f "$OUT_DIR_ABS/initial.txt" \
  "$OUT_DIR_ABS/after_help.txt" \
  "$OUT_DIR_ABS/final.txt" \
  "$STDERR_LOG" \
  "$RUNNER"

if [[ "$SKIP_BUILD" != "1" ]]; then
  eval "$BUILD_COMMAND"
fi

cat > "$RUNNER" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$REPO_ROOT"
if [[ -f export_env.sh ]]; then
  # shellcheck disable=SC1091
  source export_env.sh
fi
{
$COMMAND
} 2> "$STDERR_LOG"
EOF
chmod +x "$RUNNER"

cleanup() {
  tmux kill-session -t "$SESSION" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
tmux new-session -d -s "$SESSION" -x "$WIDTH" -y "$HEIGHT" "$RUNNER"

INITIAL_SEEN="0"
for _ in $(seq 1 "$STARTUP_WAIT"); do
  sleep 1
  tmux capture-pane -t "$SESSION" -p > "$OUT_DIR_ABS/initial.txt"
  if rg -q "Agent Claim Network|Whisper your wish here|initializing|open" "$OUT_DIR_ABS/initial.txt"; then
    INITIAL_SEEN="1"
    break
  fi
done

tmux send-keys -t "$SESSION" "/help" Enter
HELP_SEEN="0"
for _ in $(seq 1 10); do
  sleep "$AFTER_WAIT"
  tmux capture-pane -t "$SESSION" -p > "$OUT_DIR_ABS/after_help.txt"
  if rg -q "$HELP_HEADER_PATTERN" "$OUT_DIR_ABS/after_help.txt" \
    && rg -q "$HELP_SKILLS_PATTERN" "$OUT_DIR_ABS/after_help.txt"; then
    HELP_SEEN="1"
    break
  fi
done

tmux send-keys -t "$SESSION" "/exit" Enter
sleep "$EXIT_WAIT"
if tmux has-session -t "$SESSION" >/dev/null 2>&1; then
  tmux capture-pane -t "$SESSION" -p > "$OUT_DIR_ABS/final.txt"
fi

if [[ "$INITIAL_SEEN" != "1" ]]; then
  echo "initial capture does not show expected ACN TUI markers" >&2
  exit 1
fi

if [[ "$HELP_SEEN" != "1" ]]; then
  echo "help capture does not show expected command hint" >&2
  exit 1
fi

if [[ -s "$STDERR_LOG" ]]; then
  echo "stderr.log is not empty: $STDERR_LOG" >&2
  exit 1
fi

echo "TUI smoke test passed. Captures saved in $OUT_DIR_ABS"
