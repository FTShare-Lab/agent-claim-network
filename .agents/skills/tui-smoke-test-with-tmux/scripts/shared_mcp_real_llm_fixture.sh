#!/usr/bin/env bash
# 供 real-LLM TUI 场景共用的协议真实 stdio MCP fixture。
set -euo pipefail

response_id() {
  printf '%s' "$1" | sed -n 's/.*"id":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}

cancellation_request_id() {
  printf '%s' "$1" | sed -n 's/.*"requestId":[[:space:]]*\([0-9][0-9]*\).*/\1/p'
}

progress_token_json() {
  # 原样保留 number/string 的 JSON 类型；MCP progress notification 必须回显同一个 token。
  printf '%s' "$1" | python3 -c '
import json
import sys

request = json.load(sys.stdin)
token = ((request.get("params") or {}).get("_meta") or {}).get("progressToken")
if isinstance(token, bool) or not isinstance(token, (int, float, str)):
    raise SystemExit(0)
print(json.dumps(token, separators=(",", ":")))
'
}

progress_token_value() {
  printf '%s' "$1" | python3 -c '
import json
import sys

try:
    token = json.load(sys.stdin)
except json.JSONDecodeError:
    raise SystemExit(0)
print(token)
'
}

json_string() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

timestamp() {
  perl -MTime::HiRes=time -e 'printf "%.6f", time'
}

append_event() {
  local event="$1"
  local tool="$2"
  local request_id="$3"
  local progress_token="${4:-}"
  printf '{"event":"%s","tool":"%s","id":"%s","progress_token":"%s","ts":%s,"pid":%s}\n' \
    "$event" "$tool" "$request_id" "$(json_string "$progress_token")" "$(timestamp)" "$$" >> "$MCP_FIXTURE_LOG"
}

record_shutdown() {
  append_event "shutdown" "shutdown" ""
}

trap record_shutdown EXIT

init_count=0
if [[ -f "$MCP_FIXTURE_INIT_COUNT" ]]; then
  init_count="$(cat "$MCP_FIXTURE_INIT_COUNT")"
fi
init_count=$((init_count + 1))
printf '%s\n' "$init_count" > "$MCP_FIXTURE_INIT_COUNT"

timeout_once_is_first_call() {
  local lock_path="${MCP_FIXTURE_TIMEOUT_ONCE_STATE}.lock"
  while ! mkdir "$lock_path" 2>/dev/null; do
    sleep 0.01
  done
  if [[ ! -e "$MCP_FIXTURE_TIMEOUT_ONCE_STATE" ]]; then
    : > "$MCP_FIXTURE_TIMEOUT_ONCE_STATE"
    rmdir "$lock_path"
    return 0
  fi
  rmdir "$lock_path"
  return 1
}

wait_for_request_cancellation() {
  local request_id="$1"
  local deadline=$((SECONDS + 8))
  while (( SECONDS < deadline )); do
    if [[ -f "$MCP_FIXTURE_CANCELLED_DIR/$request_id" ]]; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

mkdir -p "$MCP_FIXTURE_CANCELLED_DIR"

respond_tool() {
  local request_id="$1"
  local tool="$2"
  local progress_token_json="$3"
  local progress_token="$4"
  append_event "start" "$tool" "$request_id" "$progress_token"
  if [[ -n "$progress_token_json" ]]; then
    # 先让 client/TUI 建立 ToolCell，再发送 progress，避免真实终端 smoke 在 cell 尚未渲染时
    # 错过 notification，导致只凭 fixture 日志而非可见 UI 判断。
    sleep 0.2
    append_event "progress" "$tool" "$request_id" "$progress_token"
    printf '{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":%s,"progress":1,"total":2,"message":"shared fixture running"}}\n' \
      "$progress_token_json"
  fi
  case "$tool" in
    slow_read|slow_write)
      # 给 tmux capture 留出稳定窗口，以验证 live ToolCell 与 progress，而非只看结束日志。
      sleep 2
      ;;
    timeout_once)
      if timeout_once_is_first_call; then
        # 首次调用必须保持活动状态，直到 client 的 request-scoped cancellation 到达；仅仅省略
        # response 会造成“已经结束的 handler 被取消”的假证据。
        append_event "timeout_waiting" "$tool" "$request_id" "$progress_token"
        if wait_for_request_cancellation "$request_id"; then
          append_event "timeout_cancelled" "$tool" "$request_id" "$progress_token"
        else
          append_event "timeout_deadline_elapsed" "$tool" "$request_id" "$progress_token"
        fi
        return
      fi
      ;;
  esac
  append_event "end" "$tool" "$request_id" "$progress_token"
  local text="tool=$tool pid=$$ initialize_count=$init_count"
  printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"%s"}],"isError":false}}\n' \
    "$request_id" "$(json_string "$text")"
}

while IFS= read -r line; do
  request_id="$(response_id "$line")"
  case "$line" in
    *'"method":"server/discover"'*)
      # Auto lifecycle 会先探测新协议；显式 Method not found 才会触发 legacy initialize。
      append_event "discover" "server/discover" "$request_id"
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$request_id"
      ;;
    *'"method":"initialize"'*)
      append_event "initialize" "initialize" "$request_id"
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"shared-real-llm-fixture","version":"1.0.0"}}}\n' "$request_id"
      ;;
    *'"method":"tools/list"'*)
      append_event "list" "tools/list" "$request_id"
      printf '%s\n' "{\"jsonrpc\":\"2.0\",\"id\":$request_id,\"result\":{\"tools\":[{\"name\":\"slow_read\",\"description\":\"Read-only operation; sleeps 2 seconds.\",\"inputSchema\":{\"type\":\"object\"},\"annotations\":{\"readOnlyHint\":true}},{\"name\":\"slow_write\",\"description\":\"Non-read-only operation; sleeps 2 seconds.\",\"inputSchema\":{\"type\":\"object\"},\"annotations\":{\"readOnlyHint\":false}},{\"name\":\"timeout_once\",\"description\":\"Does not return before the request deadline.\",\"inputSchema\":{\"type\":\"object\"}},{\"name\":\"ping\",\"description\":\"Fast fixture health check.\",\"inputSchema\":{\"type\":\"object\"},\"annotations\":{\"readOnlyHint\":true}}]}}"
      ;;
    *'"method":"tools/call"'*)
      tool="$(printf '%s' "$line" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')"
      request_progress_token_json="$(progress_token_json "$line")"
      request_progress_token="$(progress_token_value "$request_progress_token_json")"
      respond_tool "$request_id" "$tool" "$request_progress_token_json" "$request_progress_token" &
      ;;
    *'"method":"notifications/cancelled"'*)
      cancelled_request_id="$(cancellation_request_id "$line")"
      : > "$MCP_FIXTURE_CANCELLED_DIR/$cancelled_request_id"
      append_event "cancelled" "cancelled" "$cancelled_request_id"
      ;;
  esac
done
