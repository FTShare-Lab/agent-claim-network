#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

TUI_SESSION="${TUI_SESSION:-acn_tui_welcome_resize}"
TUI_WIDTH="${TUI_WIDTH:-131}"
TUI_HEIGHT="${TUI_HEIGHT:-32}"
TUI_OUT_DIR="${TUI_OUT_DIR:-target/tui-scenarios/welcome-resize}"

source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

assert_single_welcome_card() {
  local capture="$1"
  local count
  count="$(rg -c 'Agent Claim Network' "$TUI_OUT_DIR_ABS/$capture.txt" || true)"
  if [[ "$count" != "1" ]]; then
    echo "$capture should contain exactly one welcome card, found $count" >&2
    return 1
  fi
}

assert_single_blank_before_entry() {
  local capture="$1"
  local entry_pattern="$2"
  local border_line
  local entry_line
  border_line="$(rg -n '^(╰.*╯|─+)$' "$TUI_OUT_DIR_ABS/$capture.txt" | head -n 1 | cut -d: -f1 || true)"
  entry_line="$(rg -n "$entry_pattern" "$TUI_OUT_DIR_ABS/$capture.txt" | head -n 1 | cut -d: -f1 || true)"
  if [[ -z "$border_line" || -z "$entry_line" || $((entry_line - border_line - 1)) -ne 1 ]]; then
    echo "$capture should contain exactly one blank line between welcome card and $entry_pattern" >&2
    return 1
  fi
}

tui_start

sleep 3
tui_capture "initial_131"
tui_assert_contains "initial_131" "Agent Claim Network" "initial capture missing welcome card"
tui_assert_contains "initial_131" "Runtime Metadata.*ACN 工作流" "initial capture missing paired section headings"
tui_assert_contains "initial_131" "Maintainer (✅|❓|❌)  Router (✅|❓|❌)" "initial capture missing combined team status"
tui_assert_contains "initial_131" "Roles       Agent · Router · Maintainer" "Roles value is not aligned"
tui_assert_contains "initial_131" "Memory      偏好与经验沉淀 → 私有记忆" "Memory value is not aligned"
tui_assert_contains "initial_131" "Claim       可协作的判断对象 → 团队可见" "Claim value is not aligned"
tui_assert_contains "initial_131" "Router      团队信息检索器" "Router value is not aligned"
tui_assert_contains "initial_131" "Maintainer  团队管理与台账" "Maintainer value is not aligned"
assert_single_welcome_card "initial_131"
assert_single_blank_before_entry "initial_131" "^› Whisper your wish here"

tmux resize-window -t "$TUI_SESSION" -x 82 -y 32
sleep 0.2
tmux resize-window -t "$TUI_SESSION" -x 60 -y 32
sleep 1
tui_capture "shrunk_60"
tui_assert_contains "shrunk_60" "ready|claim network|open" "shrunk capture missing live region"
assert_single_welcome_card "shrunk_60"

tmux resize-window -t "$TUI_SESSION" -x 44 -y 32
sleep 1
tui_capture "shrunk_44"
tui_assert_contains "shrunk_44" "Agent Claim Network" "compact capture missing compact title"
tui_assert_contains "shrunk_44" "Runtime Metadata" "compact capture missing runtime heading"
tui_assert_contains "shrunk_44" "Maintainer (✅|❓|❌)  Router (✅|❓|❌)" "compact capture missing combined team status"
tui_assert_contains "shrunk_44" "ready|claim network|open" "narrow capture missing live region"
assert_single_welcome_card "shrunk_44"
assert_single_blank_before_entry "shrunk_44" "^› Whisper your wish here"

tmux resize-window -t "$TUI_SESSION" -x 96 -y 32
sleep 0.2
tmux resize-window -t "$TUI_SESSION" -x 131 -y 32
sleep 1
tui_capture "expanded_131"
tui_assert_contains "expanded_131" "Agent Claim Network" "expanded capture missing welcome card"
tui_assert_contains "expanded_131" "ready|claim network|open" "expanded capture missing live region"
assert_single_welcome_card "expanded_131"

tui_send_keys "/help" Enter
sleep 1
tmux capture-pane -t "$TUI_SESSION" -S - -p > "$TUI_OUT_DIR_ABS/after_first_history.txt"
tui_assert_contains "after_first_history" "^› /help$" "first history entry did not render"
assert_single_blank_before_entry "after_first_history" "^› /help$"

tui_send_keys "/exit" Enter
sleep 1
tui_finish
