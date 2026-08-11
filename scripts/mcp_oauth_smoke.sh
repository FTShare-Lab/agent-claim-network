#!/usr/bin/env bash
set -euo pipefail

# 在完全隔离的临时 upstream 中端到端验收 MCP 新旧协议、OAuth 与 TUI 面板。

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

for dependency in cargo curl expect jq rg tmux; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "missing smoke-test dependency: $dependency" >&2
    exit 127
  fi
done

mkdir -p "$REPO_ROOT/target"
SMOKE_DIR="$(mktemp -d "$REPO_ROOT/target/mcp-oauth-smoke.XXXXXX")"
CONFIG_PATH="$SMOKE_DIR/config.toml"
ACN_BIN="$REPO_ROOT/target/debug/acn"
REQUEST_LOG="$SMOKE_DIR/requests.jsonl"
FIXTURE_BIN="$REPO_ROOT/target/debug/examples/mcp_oauth_fake_server"
PIDS=()

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  for pid in "${PIDS[@]}"; do
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [[ "$status" -eq 0 ]]; then
    echo "MCP OAuth smoke passed. Artifacts: $SMOKE_DIR"
  else
    echo "MCP OAuth smoke failed. Artifacts preserved: $SMOKE_DIR" >&2
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

export NO_PROXY="127.0.0.1,localhost"
export no_proxy="127.0.0.1,localhost"

cat > "$CONFIG_PATH" <<EOF
upstream = "review_a"

[upstreams.review_a]
agent_id = "agent-a"
maintainer_endpoint = "http://127.0.0.1:19062"
router_endpoint = "http://127.0.0.1:19061"

[upstreams.review_b]
agent_id = "agent-b"
maintainer_endpoint = "http://127.0.0.1:19162"
router_endpoint = "http://127.0.0.1:19161"

[upstreams.review_tui]
agent_id = "agent-tui"
maintainer_endpoint = "http://127.0.0.1:19262"
router_endpoint = "http://127.0.0.1:19261"

[storage]
acn_home = "$SMOKE_DIR/acn-home"

[agent.llm]
provider = "anthropic"
endpoint = "https://llm.example.test"
model = "fixture-model"
api_key_env = "PATH"
max_tokens = 4096
context_window = 200000
timeout_secs = 600
retry_count = 1
retry_base_delay_ms = 200
retry_max_delay_ms = 5000
EOF

start_fixture() {
  local label="$1"
  local port="$2"
  shift 2
  "$FIXTURE_BIN" \
    --port "$port" \
    --log-file "$SMOKE_DIR/$label.jsonl" \
    "$@" \
    > "$SMOKE_DIR/$label.stdout" \
    2> "$SMOKE_DIR/$label.stderr" &
  PIDS+=("$!")

  local ready=0
  for _ in $(seq 1 50); do
    if curl --noproxy '*' -fsS \
      "http://127.0.0.1:$port/.well-known/oauth-protected-resource" \
      >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.1
  done
  if [[ "$ready" != "1" ]]; then
    echo "fixture did not become ready: $label on $port" >&2
    return 1
  fi
}

mcp_a() {
  "$ACN_BIN" mcp "$@" --config "$CONFIG_PATH" --upstream review_a
}

assert_status() {
  local upstream="$1"
  local server_name="$2"
  local expected="$3"
  local output="$SMOKE_DIR/status-${upstream}-${server_name}-${expected}.txt"
  "$ACN_BIN" mcp status "$server_name" \
    --config "$CONFIG_PATH" \
    --upstream "$upstream" \
    > "$output"
  if ! rg -q -- "^- ${server_name}[[:space:]]+streamable_http[[:space:]]+${expected}$" "$output"; then
    echo "unexpected MCP status for $upstream/$server_name; expected $expected" >&2
    sed -n '1,80p' "$output" >&2
    return 1
  fi
}

credential_file_count() {
  local directory="$SMOKE_DIR/acn-home/review_a/.mcp-oauth"
  if [[ ! -d "$directory" ]]; then
    echo 0
    return
  fi
  find "$directory" -type f | wc -l | tr -d '[:space:]'
}

assert_private_credentials() {
  local directory="$SMOKE_DIR/acn-home/review_a/.mcp-oauth"
  local mode
  local file
  while IFS= read -r file; do
    if [[ "$(uname -s)" == "Darwin" ]]; then
      mode="$(stat -f '%Lp' "$file")"
    else
      mode="$(stat -c '%a' "$file")"
    fi
    if [[ "$mode" != "600" ]]; then
      echo "OAuth credential file has mode $mode instead of 600: $file" >&2
      return 1
    fi
  done < <(find "$directory" -type f -print)
}

drive_login() {
  local server_name="$1"
  local upstream="$2"
  local authorization_port="$3"
  local mode="$4"
  local log_path="$SMOKE_DIR/login-${upstream}-${server_name}-${mode}.txt"

  SMOKE_ACN_BIN="$ACN_BIN" \
  SMOKE_CONFIG_PATH="$CONFIG_PATH" \
  SMOKE_LOGIN_SERVER="$server_name" \
  SMOKE_LOGIN_UPSTREAM="$upstream" \
  SMOKE_AUTHORIZATION_PORT="$authorization_port" \
  SMOKE_LOGIN_MODE="$mode" \
  SMOKE_LOGIN_LOG="$log_path" \
  SMOKE_CHILD_PATH="$SMOKE_DIR/no-browser-bin" \
  expect <<'EXPECT'
set timeout 30
log_file -noappend $env(SMOKE_LOGIN_LOG)
set command [list \
  $env(SMOKE_ACN_BIN) mcp login $env(SMOKE_LOGIN_SERVER) \
  --config $env(SMOKE_CONFIG_PATH) --upstream $env(SMOKE_LOGIN_UPSTREAM)]
if {$env(SMOKE_LOGIN_MODE) eq "headless"} {
  lappend command --no-browser
}
spawn env PATH=$env(SMOKE_CHILD_PATH) \
  NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost {*}$command
set pattern [format {(http://127[.]0[.]0[.]1:%s/authorize[?][^\r\n]+)} \
  $env(SMOKE_AUTHORIZATION_PORT)]
expect {
  -re $pattern {
    set authorization_url $expect_out(1,string)
  }
  eof {
    catch wait result
    exit [lindex $result 3]
  }
  timeout {
    puts stderr "timed out waiting for authorization URL"
    exit 124
  }
}
if {$env(SMOKE_LOGIN_MODE) eq "headless"} {
  set headers [exec curl --noproxy * -sS -D - -o /dev/null -- $authorization_url]
  if {![regexp -nocase -line {^Location: ([^\r]+)} $headers ignored redirect_url]} {
    puts stderr "authorization endpoint did not return a redirect"
    exit 1
  }
  send -- "$redirect_url\r"
} else {
  set status [exec curl --noproxy * -sS -L -o /dev/null -w %{http_code} -- $authorization_url]
  if {$status ne "200"} {
    puts stderr "desktop callback returned HTTP $status"
    exit 1
  }
}
expect eof
catch wait result
exit [lindex $result 3]
EXPECT
}

run_focused_tui_smoke() (
  export TUI_SESSION="acn_tui_mcp_oauth_smoke_$$"
  export TUI_WIDTH="140"
  export TUI_HEIGHT="40"
  export TUI_OUT_DIR="$SMOKE_DIR/tui-mcp-panel"
  export TUI_COMMAND="env NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost $ACN_BIN --config $CONFIG_PATH --upstream review_tui"
  export TUI_SKIP_BUILD="1"
  # shellcheck source=/dev/null
  source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

  tui_start
  local initial_seen=0
  for _ in $(seq 1 30); do
    sleep 1
    if ! tmux has-session -t "$TUI_SESSION" >/dev/null 2>&1; then
      echo "TUI exited before the MCP panel test" >&2
      return 1
    fi
    tui_capture initial
    if rg -q "Agent Claim Network|Whisper your wish here|initializing|open" \
      "$TUI_OUT_DIR_ABS/initial.txt"; then
      initial_seen=1
      break
    fi
  done
  if [[ "$initial_seen" != "1" ]]; then
    echo "TUI did not reach its initial screen" >&2
    return 1
  fi

  tui_send_keys "/mcp" Enter
  local panel_seen=0
  for _ in $(seq 1 20); do
    sleep 0.5
    tui_capture mcp_servers
    if rg -q "MCP.*servers" "$TUI_OUT_DIR_ABS/mcp_servers.txt" \
      && rg -q '^> 1[[:space:]]+anonymous[[:space:]]+ready' \
        "$TUI_OUT_DIR_ABS/mcp_servers.txt"; then
      panel_seen=1
      break
    fi
  done
  if [[ "$panel_seen" != "1" ]]; then
    echo "MCP panel did not show anonymous as ready" >&2
    return 1
  fi
  tui_assert_contains mcp_servers "legacy" "MCP panel did not show the legacy server"

  tui_send_keys Enter
  sleep 0.5
  tui_capture server_detail
  tui_assert_contains server_detail "anonymous" "MCP server detail did not open"
  tui_assert_contains server_detail 'status[[:space:]]+ready' \
    "MCP server detail did not preserve the ready state"
  tui_assert_contains server_detail 'tools[[:space:]]+1 exposed / 1 discovered' \
    "MCP server detail did not show the discovered ping tool"

  tui_send_keys v
  local tools_seen=0
  for _ in $(seq 1 20); do
    sleep 0.25
    tui_capture tools
    if rg -q "MCP.*tools" "$TUI_OUT_DIR_ABS/tools.txt" \
      && rg -q "ping" "$TUI_OUT_DIR_ABS/tools.txt"; then
      tools_seen=1
      break
    fi
  done
  if [[ "$tools_seen" != "1" ]]; then
    echo "MCP panel did not expose ping in the tool list" >&2
    return 1
  fi

  tui_send_keys Escape
  tui_send_keys d
  local disabled_seen=0
  for _ in $(seq 1 20); do
    sleep 0.25
    tui_capture disabled
    if rg -q 'status[[:space:]]+disabled|anonymous[[:space:]]+disabled' \
      "$TUI_OUT_DIR_ABS/disabled.txt"; then
      disabled_seen=1
      break
    fi
  done
  if [[ "$disabled_seen" != "1" ]]; then
    echo "MCP panel did not disable anonymous" >&2
    return 1
  fi

  tui_send_keys d
  local enabled_seen=0
  for _ in $(seq 1 40); do
    sleep 0.25
    tui_capture reenabled
    if rg -q 'status[[:space:]]+ready|anonymous[[:space:]]+ready' \
      "$TUI_OUT_DIR_ABS/reenabled.txt"; then
      enabled_seen=1
      break
    fi
  done
  if [[ "$enabled_seen" != "1" ]]; then
    echo "MCP panel did not re-enable anonymous to ready" >&2
    return 1
  fi

  tui_send_keys r
  local reconnect_seen=0
  for _ in $(seq 1 40); do
    sleep 0.25
    tui_capture reconnected
    if rg -q 'status[[:space:]]+ready|anonymous[[:space:]]+ready' \
      "$TUI_OUT_DIR_ABS/reconnected.txt"; then
      reconnect_seen=1
      break
    fi
  done
  if [[ "$reconnect_seen" != "1" ]]; then
    echo "MCP panel reconnect did not return anonymous to ready" >&2
    return 1
  fi

  tui_send_keys Escape
  tui_send_keys Escape
  tui_send_keys "/exit" Enter
  sleep 1
  tui_finish
)

echo "[mcp-smoke] build"
cargo build --quiet --bin acn --example mcp_oauth_fake_server

# 让 desktop login 的 launcher 明确失败，避免真的拉起外部浏览器；测试驱动仍会访问
# authorization URL 并跟随重定向，从而完整覆盖 loopback listener 与 callback。
mkdir -p "$SMOKE_DIR/no-browser-bin/open" "$SMOKE_DIR/no-browser-bin/xdg-open"

echo "[mcp-smoke] start fixtures"
start_fixture requests 8765

echo "[mcp-smoke] configure servers"
mcp_a add anonymous --url http://127.0.0.1:8765/anonymous-mcp
mcp_a add legacy --url http://127.0.0.1:8765/legacy-mcp
mcp_a add oauth-a --url http://127.0.0.1:8765/mcp \
  --oauth-callback-port 8766 --oauth-credentials-store file
mcp_a add oauth-b --url http://127.0.0.1:8765/mcp \
  --oauth-callback-port 8767 --oauth-credentials-store file
mcp_a add prereg --url http://127.0.0.1:8765/mcp \
  --oauth-client-id fixture-public-client \
  --oauth-callback-port 8768 --oauth-credentials-store file
mcp_a add bearer --url http://127.0.0.1:8765/mcp \
  --bearer-token-env-var FIXTURE_BEARER
"$ACN_BIN" mcp add oauth-a --url http://127.0.0.1:8765/mcp \
  --oauth-callback-port 8769 --oauth-credentials-store file \
  --config "$CONFIG_PATH" --upstream review_b

echo "[mcp-smoke] protocol negotiation and bearer precedence"
assert_status review_a anonymous ready
assert_status review_a legacy ready
[[ ! -e "$SMOKE_DIR/acn-home/review_a/.mcp-oauth" ]]
bearer_log_start="$(wc -l < "$REQUEST_LOG" | tr -d '[:space:]')"
FIXTURE_BEARER=fixture-static-token \
  "$ACN_BIN" mcp status bearer --config "$CONFIG_PATH" --upstream review_a \
  > "$SMOKE_DIR/status-review_a-bearer-ready.txt"
rg -q -- '^- bearer[[:space:]]+streamable_http[[:space:]]+ready$' \
  "$SMOKE_DIR/status-review_a-bearer-ready.txt"
tail -n "+$((bearer_log_start + 1))" "$REQUEST_LOG" > "$SMOKE_DIR/bearer-requests.jsonl"
jq -s -e '
  any(.[]; .endpoint == "oauth" and .authorized == true)
  and all(.[]; .path != "/.well-known/oauth-protected-resource"
               and .path != "/.well-known/oauth-authorization-server")
' "$SMOKE_DIR/bearer-requests.jsonl" >/dev/null
jq -s -e '
  any(.[]; .endpoint == "anonymous"
           and .rpc_method == "server/discover"
           and .protocol_version == "2026-07-28")
  and any(.[]; .endpoint == "legacy"
               and .rpc_method == "server/discover"
               and .protocol_version == "2026-07-28")
  and any(.[]; .endpoint == "legacy"
               and .rpc_method == "initialize"
               and .protocol_version == "2025-11-25")
  and any(.[]; .endpoint == "legacy"
               and .rpc_method == "tools/list"
               and .protocol_version == "2025-11-25")
' "$REQUEST_LOG" >/dev/null

echo "[mcp-smoke] desktop DCR login, PKCE, issuer and refresh"
drive_login oauth-a review_a 8765 desktop
jq -s -e '
  any(.[]; .path == "/register")
  and any(.[]; .path == "/authorize"
               and .accepted == true
               and .scope == "fixture:read"
               and .resource == "http://127.0.0.1:8765/mcp")
  and any(.[]; .path == "/token"
               and .grant_type == "authorization_code"
               and .accepted == true)
' "$REQUEST_LOG" >/dev/null
assert_private_credentials
if rg -q '"saved_at"' "$SMOKE_DIR/acn-home/review_a/.mcp-oauth"; then
  echo "legacy saved_at field was persisted" >&2
  exit 1
fi
sleep 6
assert_status review_a oauth-a ready
jq -s -e '
  any(.[]; .path == "/token"
           and .grant_type == "refresh_token"
           and .accepted == true)
' "$REQUEST_LOG" >/dev/null

echo "[mcp-smoke] preregistered public client skips DCR"
register_before="$(jq -s '[.[] | select(.path == "/register")] | length' "$REQUEST_LOG")"
drive_login prereg review_a 8765 desktop
register_after="$(jq -s '[.[] | select(.path == "/register")] | length' "$REQUEST_LOG")"
[[ "$register_before" == "$register_after" ]]
jq -s -e '
  any(.[]; .path == "/authorize"
           and .client_id == "fixture-public-client"
           and .accepted == true)
' "$REQUEST_LOG" >/dev/null

echo "[mcp-smoke] credential isolation"
assert_status review_a oauth-b failed
assert_status review_b oauth-a failed
mcp_a logout oauth-b
assert_status review_a oauth-a ready
[[ "$(credential_file_count)" == "4" ]]

echo "[mcp-smoke] remove and logout lifecycle"
mcp_a remove prereg
[[ "$(credential_file_count)" == "2" ]]
if mcp_a get prereg > "$SMOKE_DIR/get-removed-prereg.txt" 2>&1; then
  echo "removed server is still readable" >&2
  exit 1
fi
mcp_a logout oauth-a
[[ "$(credential_file_count)" == "0" ]]
mcp_a get oauth-a > "$SMOKE_DIR/get-logged-out-oauth-a.txt"

echo "[mcp-smoke] headless login"
drive_login oauth-b review_a 8765 headless
assert_status review_a oauth-b ready

echo "[mcp-smoke] fail-closed negative cases"
start_fixture pkce 8775 --omit-pkce
start_fixture insecure 8776 --insecure-metadata
start_fixture issuer 8777 --mismatched-callback-issuer
mcp_a add pkce-bad --url http://127.0.0.1:8775/mcp \
  --oauth-callback-port 8785 --oauth-credentials-store file
mcp_a add insecure-bad --url http://127.0.0.1:8776/mcp \
  --oauth-callback-port 8786 --oauth-credentials-store file
mcp_a add issuer-bad --url http://127.0.0.1:8777/mcp \
  --oauth-callback-port 8787 --oauth-credentials-store file
if mcp_a login pkce-bad --no-browser > "$SMOKE_DIR/pkce-failure.txt" 2>&1; then
  echo "login unexpectedly accepted metadata without PKCE S256" >&2
  exit 1
fi
rg -q '不支持 PKCE S256' "$SMOKE_DIR/pkce-failure.txt"
if mcp_a login insecure-bad --no-browser > "$SMOKE_DIR/insecure-failure.txt" 2>&1; then
  echo "login unexpectedly accepted insecure OAuth metadata" >&2
  exit 1
fi
rg -q 'OAuth metadata 无效或不完整' "$SMOKE_DIR/insecure-failure.txt"
if drive_login issuer-bad review_a 8777 headless; then
  echo "login unexpectedly accepted a mismatched callback issuer" >&2
  exit 1
fi
rg -q '回调 issuer 不匹配' "$SMOKE_DIR/login-review_a-issuer-bad-headless.txt"
mcp_a logout oauth-b
[[ "$(credential_file_count)" == "0" ]]

echo "[mcp-smoke] canonical and focused TUI"
"$ACN_BIN" mcp add anonymous --url http://127.0.0.1:8765/anonymous-mcp \
  --config "$CONFIG_PATH" --upstream review_tui
"$ACN_BIN" mcp add legacy --url http://127.0.0.1:8765/legacy-mcp \
  --config "$CONFIG_PATH" --upstream review_tui
"$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_smoke.sh" \
  --session "acn_tui_mcp_canonical_$$" \
  --command "env NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost $ACN_BIN --config $CONFIG_PATH --upstream review_tui" \
  --skip-build \
  --out-dir "$SMOKE_DIR/tui-canonical"
run_focused_tui_smoke

echo "[mcp-smoke] all assertions passed"
