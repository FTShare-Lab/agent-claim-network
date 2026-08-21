#!/usr/bin/env bash
# 真实 LLM + 协议真实 stdio MCP 的共享连接回归。
# 用法：source export_env.sh && SHARED_MCP_SCENARIO=reads bash "$0"
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

SCENARIO="${SHARED_MCP_SCENARIO:-reads}"
case "$SCENARIO" in
  reads|children|timeout|reconnect) ;;
  *)
    echo "SHARED_MCP_SCENARIO must be reads, children, timeout, or reconnect" >&2
    exit 2
    ;;
esac
[[ -n "${ACN_LLM_API_KEY:-}" ]] || {
  echo "ACN_LLM_API_KEY is required; source export_env.sh first" >&2
  exit 1
}
tui_build_if_needed
ACN_BINARY="$(tui_resolve_binary TUI_ACN_BINARY acn bin)"
TUI_SKIP_BUILD=1

BASE_OUT="${SHARED_MCP_OUT_DIR:-target/tui-scenarios/shared-mcp-real-llm}"
RUN_ROOT="$BASE_OUT/$SCENARIO/$(date +%Y%m%d-%H%M%S)-$$"
CONFIG_SOURCE="$REPO_ROOT/config.toml"
IFS=$'\t' read -r SELECTED_UPSTREAM _ < <(tui_config_agent_identity "$CONFIG_SOURCE")
ACN_HOME="$RUN_ROOT/acn_home"
CONFIG_PATH="$RUN_ROOT/config.toml"
MCP_CONFIG_PATH="$ACN_HOME/$SELECTED_UPSTREAM/.mcp.json"
FIXTURE_PATH="$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/shared_mcp_real_llm_fixture.sh"
LOG_PATH="$RUN_ROOT/fixture.jsonl"
INIT_COUNT_PATH="$RUN_ROOT/initialize-count.txt"
TUI_SESSION="acn_shared_mcp_${SCENARIO}_$$"
TUI_OUT_DIR="$RUN_ROOT"
TUI_COMMAND="'$ACN_BINARY' --config '$CONFIG_PATH'"
TUI_WIDTH=132
TUI_HEIGHT=40
WAIT_SECS="${SHARED_MCP_WAIT_SECS:-300}"
STARTUP_WAIT_SECS="${SHARED_MCP_STARTUP_WAIT_SECS:-30}"

mkdir -p "$RUN_ROOT" "$(dirname "$MCP_CONFIG_PATH")"
python3 - "$CONFIG_SOURCE" "$CONFIG_PATH" "$ACN_HOME" <<'PY'
import json
import re
import sys
from pathlib import Path

source, target, acn_home = map(Path, sys.argv[1:])
text = source.read_text()
text, count = re.subn(
    r"(?m)^acn_home\s*=\s*.*$",
    "acn_home = " + json.dumps(str(acn_home.resolve())),
    text,
    count=1,
)
if count != 1:
    raise SystemExit("expected exactly one storage.acn_home in config.toml")
text, count = re.subn(
    r'(?ms)(^\[agent\.llm\]\n.*?^provider\s*=\s*)"[^"]*"',
    r'\1"openai_chat"',
    text,
    count=1,
)
if count != 1:
    raise SystemExit("expected exactly one agent.llm.provider in config.toml")
target.write_text(text)
PY
python3 - "$MCP_CONFIG_PATH" "$FIXTURE_PATH" "$LOG_PATH" "$INIT_COUNT_PATH" <<'PY'
import json
import sys
from pathlib import Path

target, fixture, log, count = map(Path, sys.argv[1:])
target.write_text(json.dumps({"mcpServers": {"shared": {
    "type": "stdio",
    "command": "bash",
    "args": [str(fixture)],
    "env": {
        "MCP_FIXTURE_LOG": str(log),
        "MCP_FIXTURE_INIT_COUNT": str(count),
        "MCP_FIXTURE_TIMEOUT_ONCE_STATE": str(count.parent / "timeout-once-used"),
        "MCP_FIXTURE_CANCELLED_DIR": str(count.parent / "cancelled-requests"),
    },
    "startup_timeout_secs": 30,
    "tool_timeout_secs": 4,
}}}, indent=2) + "\n")
PY

cleanup_owned_supervisor() {
  tui_cleanup
  tui_terminate_owned_supervisors "$CONFIG_PATH" "$ACN_BINARY"
}

send_prompt() {
  local prompt_path="$TUI_OUT_DIR_ABS/prompt.txt"
  printf '%s' "$1" > "$prompt_path"
  tmux load-buffer -b "${TUI_SESSION}_prompt" "$prompt_path"
  tmux paste-buffer -t "$TUI_SESSION" -b "${TUI_SESSION}_prompt"
  sleep 0.3
  tmux send-keys -t "$TUI_SESSION" C-m
}

wait_for_count() {
  wait_for_count_with_timeout "$1" "$2" "$WAIT_SECS"
}

wait_for_count_with_timeout() {
  local pattern="$1"
  local expected="$2"
  local timeout_secs="$3"
  local deadline=$((SECONDS + timeout_secs))
  local count
  while (( SECONDS < deadline )); do
    count=0
    if [[ -f "$LOG_PATH" ]]; then
      count="$(rg -c "$pattern" "$LOG_PATH" || true)"
      count="${count:-0}"
    fi
    if (( count >= expected )); then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_tui_contains() {
  local capture="$1"
  local pattern="$2"
  local timeout_secs="$3"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    tui_capture "$capture"
    if rg -q --fixed-strings "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

assert_overlap() {
  local tool="$1"
  perl -MJSON::PP -e '
    my ($tool, %start, %end) = shift;
    while (<>) {
      my $event = decode_json($_);
      next unless $event->{tool} eq $tool;
      $start{$event->{id}} = $event->{ts} if $event->{event} eq "start";
      $end{$event->{id}} = $event->{ts} if $event->{event} eq "end";
    }
    my @ids = grep { exists $start{$_} && exists $end{$_} } keys %start;
    for my $i (0 .. $#ids - 1) {
      for my $j ($i + 1 .. $#ids) {
        my ($a, $b) = @ids[$i, $j];
        exit 0 if $start{$a} < $end{$b} && $start{$b} < $end{$a};
      }
    }
    die "$tool execution intervals did not overlap\n";
  ' "$tool" "$LOG_PATH"
}

assert_one_initialize() {
  [[ "$(cat "$INIT_COUNT_PATH")" == "1" ]] || {
    echo "expected exactly one initialize, got $(cat "$INIT_COUNT_PATH")" >&2
    exit 1
  }
  [[ "$(rg -c '"event":"initialize"' "$LOG_PATH")" == "1" ]] || {
    echo "fixture observed more than one initialize" >&2
    exit 1
  }
}

assert_capture_occurrences_at_least() {
  local capture="$1"
  local pattern="$2"
  local minimum="$3"
  local count
  count="$(rg -o "$pattern" "$TUI_OUT_DIR_ABS/$capture.txt" | wc -l | tr -d ' ')"
  (( count >= minimum )) || {
    echo "expected at least $minimum occurrences of '$pattern' in $capture, got $count" >&2
    exit 1
  }
}

assert_distinct_progress_tokens() {
  local tool="$1"
  local minimum="$2"
  perl -MJSON::PP -e '
    my ($tool, $minimum, %tokens) = @ARGV;
    while (<STDIN>) {
      my $event = decode_json($_);
      next unless $event->{event} eq "start" && $event->{tool} eq $tool;
      die "missing progress token for $tool request $event->{id}\n"
        unless defined $event->{progress_token} && length $event->{progress_token};
      $tokens{$event->{progress_token}} = 1;
    }
    die "expected at least $minimum distinct progress tokens for $tool, got " . keys(%tokens) . "\n"
      unless keys(%tokens) >= $minimum;
  ' "$tool" "$minimum" < "$LOG_PATH"
}

assert_no_active_shared_mcp_tool_cell() {
  local capture="$1"
  tui_assert_not_contains "$capture" "Calling mcp shared/" "shared MCP ToolCell was still calling"
}

trap cleanup_owned_supervisor EXIT
tui_start
if ! wait_for_tui_contains "initial" "ACN" "$STARTUP_WAIT_SECS"; then
  tui_capture "initial" || true
  echo "TUI did not start within ${STARTUP_WAIT_SECS}s" >&2
  exit 1
fi

case "$SCENARIO" in
  reads)
    send_prompt "Use exactly two calls to MCP tool mcp__shared__slow_read in your next assistant tool-use response, one for slot A and one for slot B. They are read-only and should be issued together. Do not call any other tool, do not create subagents, and after receiving both results answer with only: SHARED_MCP_READS_DONE."
    wait_for_count '"event":"start","tool":"slow_read"' 2 || {
      tui_capture "scenario_not_formed"; exit 2;
    }
    sleep 0.6
    tui_capture "while_slow_reads"
    tui_assert_contains "while_slow_reads" "Calling mcp shared/slow_read" "TUI did not render a live shared MCP ToolCell"
    assert_capture_occurrences_at_least "while_slow_reads" "Calling mcp shared/slow_read" 2
    tui_assert_contains "while_slow_reads" "progress 1/2 shared fixture running" "TUI did not render MCP progress"
    assert_capture_occurrences_at_least "while_slow_reads" "progress 1/2 shared fixture running" 2
    assert_distinct_progress_tokens "slow_read" 2
    wait_for_count '"event":"end","tool":"slow_read"' 2 || {
      tui_capture "slow_reads_not_completed"; exit 2;
    }
    sleep 0.5
    tui_capture "after_slow_reads"
    assert_overlap "slow_read"
    assert_one_initialize
    assert_capture_occurrences_at_least "after_slow_reads" "Called mcp shared/slow_read" 2
    assert_no_active_shared_mcp_tool_cell "after_slow_reads"
    ;;
  children)
    send_prompt "Create exactly two session subagents now, named shared-mcp-child-a and shared-mcp-child-b. In this parent turn call only create_subagent twice and do not call MCP tools yourself. Give each child this objective: call mcp__shared__slow_write exactly once with an empty object, do not use any other tool, then reply only SHARED_MCP_CHILD_DONE. The two children should start immediately and run independently."
    wait_for_count '"event":"start","tool":"slow_write"' 2 || {
      tui_capture "scenario_not_formed"; exit 2;
    }
    sleep 0.6
    tui_capture "while_slow_writes"
    tui_assert_contains "while_slow_writes" "Subagents: 2 running" "TUI did not show both active subagents"
    assert_distinct_progress_tokens "slow_write" 2
    wait_for_count '"event":"end","tool":"slow_write"' 2 || {
      tui_capture "slow_writes_not_completed"; exit 2;
    }
    # 子代理在 MCP response 后还需完成一次真实 LLM 收束；不能仅以 fixture 的 tool end
    # 就断言 UI 已更新为 completed。
    wait_for_tui_contains "waiting_for_child_completion" "Subagents: 2 completed" "$WAIT_SECS" || {
      tui_capture "after_slow_writes"; exit 1;
    }
    tui_capture "after_slow_writes"
    assert_overlap "slow_write"
    assert_one_initialize
    tui_assert_contains "after_slow_writes" "Subagents: 2 completed" "TUI did not show child completion"
    ;;
  timeout)
    send_prompt "Create exactly two session subagents now, named timeout-child and peer-child. In this parent turn call only create_subagent twice and do not call MCP tools yourself. Give timeout-child this objective: call mcp__shared__timeout_once exactly once with an empty object, do not call any other tool, then report the error. Give peer-child this objective: call mcp__shared__slow_read exactly once with an empty object, do not call any other tool, then reply only PEER_DONE. Start both children immediately and independently."
    wait_for_count '"event":"start","tool":"timeout_once"' 1 &&
      wait_for_count '"event":"start","tool":"slow_read"' 1 || {
      tui_capture "timeout_scenario_not_formed"; exit 2;
    }
    sleep 0.6
    tui_capture "while_timeout_and_peer"
    # peer 可能在 timeout_once 真正进入等待前已完成；这仍然证明共享 client
    # 没有被慢请求阻塞。同时接受“两者均在运行”和“peer 已完成”两种合法时序。
    tui_assert_contains "while_timeout_and_peer" \
      "Subagents: (2 running|1 completed · 1 running)" \
      "TUI did not show both timeout peers in running/completed state"
    assert_distinct_progress_tokens "timeout_once" 1
    assert_distinct_progress_tokens "slow_read" 1
    wait_for_count '"event":"end","tool":"slow_read"' 1 &&
      wait_for_count '"event":"cancelled"' 1 || {
      tui_capture "timeout_scenario_not_formed"; exit 2;
    }
    tui_capture "after_timeout_and_peer"
    send_prompt "Now call MCP tool mcp__shared__ping exactly once with an empty object. Do not create subagents or call any other tool. After it returns, answer only SHARED_MCP_TIMEOUT_PING_DONE."
    wait_for_count '"event":"end","tool":"ping"' 1 || {
      tui_capture "ping_not_formed"; exit 2;
    }
    sleep 0.5
    tui_capture "after_follow_up_ping"
    assert_one_initialize
    perl -MJSON::PP -e '
      my (%pid, %seen, $timeout_id, $peer_id, $cancelled_id, $active_cancelled_id);
      while (<>) {
        my $event = decode_json($_);
        if ($event->{tool} =~ /^(?:timeout_once|slow_read|ping)$/) {
          $seen{$event->{tool}} = 1;
          $pid{$event->{pid}} = 1;
          $timeout_id = $event->{id} if $event->{event} eq "start" && $event->{tool} eq "timeout_once" && !defined $timeout_id;
          $peer_id = $event->{id} if $event->{event} eq "start" && $event->{tool} eq "slow_read";
        }
        $cancelled_id = $event->{id} if $event->{event} eq "cancelled";
        $active_cancelled_id = $event->{id}
          if $event->{event} eq "timeout_cancelled" && $event->{tool} eq "timeout_once";
      }
      die "missing required fixture events\n" unless $seen{timeout_once} && $seen{slow_read} && $seen{ping};
      die "shared client PID changed across timeout scenario\n" unless keys(%pid) == 1;
      die "missing timeout/peer/cancellation request ids\n" unless defined $timeout_id && defined $peer_id && defined $cancelled_id;
      die "cancellation did not target the timed-out request\n" unless $cancelled_id eq $timeout_id;
      die "cancellation incorrectly targeted the peer request\n" if $cancelled_id eq $peer_id;
      die "timed-out handler did not observe cancellation while active\n"
        unless defined $active_cancelled_id && $active_cancelled_id eq $timeout_id;
    ' "$LOG_PATH"
    tui_assert_contains "after_follow_up_ping" "Called mcp shared/ping" "TUI did not expose follow-up ping"
    assert_no_active_shared_mcp_tool_cell "after_follow_up_ping"
    send_prompt "Now call MCP tool mcp__shared__timeout_once exactly once with an empty object. Do not create subagents or call any other tool. This is its second invocation, so wait for its successful result and then answer only SHARED_MCP_TIMEOUT_RECOVERY_DONE."
    wait_for_count '"event":"end","tool":"timeout_once"' 1 || {
      tui_capture "timeout_recovery_not_formed"; exit 2;
    }
    sleep 0.5
    tui_capture "after_timeout_recovery"
    assert_one_initialize
    tui_assert_contains "after_timeout_recovery" "Called mcp shared/timeout_once" "TUI did not expose timeout recovery"
    assert_no_active_shared_mcp_tool_cell "after_timeout_recovery"
    ;;
  reconnect)
    send_prompt "Call MCP tool mcp__shared__ping exactly once with an empty object. Do not call any other tool or create subagents. After it returns, answer only SHARED_MCP_RECONNECT_FIRST_PING_DONE."
    wait_for_count '"event":"end","tool":"ping"' 1 || {
      tui_capture "first_ping_not_formed"; exit 2;
    }
    # 首次 ping 的 MCP request 返回后，真实 LLM 还可能在生成最终文本；必须等 turn 回到
    # Idle，避免把 /mcp 排进当前 turn 的输入队列而不是打开 MCP 面板。
    wait_for_tui_contains "after_first_ping_turn" "┌ Idle" "$WAIT_SECS" || {
      tui_capture "first_ping_turn_not_settled"; exit 1;
    }
    old_pid="$(perl -MJSON::PP -e 'while (<>) { my $event = decode_json($_); if ($event->{event} eq "initialize") { print $event->{pid}; exit 0; } } exit 1' "$LOG_PATH")"
    assert_one_initialize
    tui_send_keys "/mcp" Enter
    sleep 1
    tui_capture "mcp_panel_before_reconnect"
    tui_assert_contains "mcp_panel_before_reconnect" "MCP · servers" "MCP panel did not open"
    tui_assert_contains "mcp_panel_before_reconnect" "shared" "MCP panel did not list shared server"
    tui_assert_contains "mcp_panel_before_reconnect" "ready" "MCP panel did not show shared server ready"
    tui_send_keys "r"
    if ! wait_for_count_with_timeout '"event":"initialize"' 2 5; then
      # 某些 tmux/终端时序下首个单键可能在 panel 刚渲染时丢失；确认仍在 panel 后只重发一次。
      tui_capture "reconnect_first_key_not_formed"
      tui_assert_contains "reconnect_first_key_not_formed" "MCP · servers" "MCP panel closed before reconnect retry"
      tui_send_keys "r"
      wait_for_count '"event":"initialize"' 2 || {
        tui_capture "reconnect_not_completed"; exit 1;
      }
    fi
    sleep 1
    tui_capture "mcp_panel_after_reconnect"
    tui_assert_contains "mcp_panel_after_reconnect" "MCP server shared updated" "MCP panel did not report reconnect completion"
    tui_assert_contains "mcp_panel_after_reconnect" "shared" "MCP panel lost shared server after reconnect"
    tui_assert_contains "mcp_panel_after_reconnect" "ready" "MCP panel did not return to ready state"
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "old fixture PID $old_pid is still alive after reconnect" >&2
      exit 1
    fi
    new_pid="$(perl -MJSON::PP -e 'my @pids; while (<>) { my $event = decode_json($_); push @pids, $event->{pid} if $event->{event} eq "initialize"; } exit 1 unless @pids == 2 && $pids[0] ne $pids[1]; print $pids[1]' "$LOG_PATH")"
    tui_send_keys Escape
    sleep 1
    send_prompt "Call MCP tool mcp__shared__ping exactly once with an empty object. Do not call any other tool or create subagents. After it returns, answer only SHARED_MCP_RECONNECT_SECOND_PING_DONE."
    wait_for_count '"event":"end","tool":"ping"' 2 || {
      tui_capture "second_ping_not_formed"; exit 2;
    }
    sleep 0.5
    tui_capture "after_reconnect_ping"
    perl -MJSON::PP -e '
      my ($pid, $count) = (shift, 0);
      while (<>) { my $event = decode_json($_); $count++ if $event->{event} eq "end" && $event->{tool} eq "ping" && $event->{pid} eq $pid; }
      die "follow-up ping did not run on replacement PID\n" unless $count >= 1;
    ' "$new_pid" "$LOG_PATH"
    assert_capture_occurrences_at_least "after_reconnect_ping" "Called mcp shared/ping" 2
    assert_no_active_shared_mcp_tool_cell "after_reconnect_ping"
    ;;
esac

tui_capture "before_exit"
tui_send_keys "/exit" Enter
sleep 2
tui_assert_stderr_empty
tui_cleanup
echo "shared MCP real-LLM scenario '$SCENARIO' passed: $TUI_OUT_DIR_ABS"
