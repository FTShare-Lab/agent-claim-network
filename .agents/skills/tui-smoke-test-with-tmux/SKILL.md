---
name: tui-smoke-test-with-tmux
description: Verify and debug this project's ratatui/crossterm TUI by running the ACN agent CLI inside tmux, capturing terminal screens as text, sending scripted keys, checking stderr, and writing focused tmux regressions for requested TUI flows. Use after changes to src/session_tui, src/bin/acn.rs interactive mode, terminal rendering, key handling, layout, composer behavior, queued input behavior, cancellation, scrolling, or when asked to smoke-test a concrete TUI flow.
---

# TUI Smoke Test With tmux

Use this skill to validate the real terminal behavior of the Agent Claim Network TUI. It supports both the default smoke test and task-specific tmux scripts for user-described interaction flows.

The project TUI entrypoint is:

```bash
cargo run --quiet --bin acn -- --config config.toml
```

The TUI uses `ratatui` + `crossterm`, so verify it in a real tty. Prefer tmux capture files over screenshots because they can be inspected with `rg`, `sed`, and diffs.

## Workflow

1. Review the TUI change and decide the expected screen states or key flow.
2. Ensure `tmux` is installed. If an environment or dependency is missing, fix that environment issue first.
3. Source `export_env.sh` before verification when it exists or the run needs API/model variables.
4. For broad startup verification, run the bundled smoke script from the repo root:

```bash
.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_smoke.sh
```

5. For a specific user story, write a focused script using `scripts/tui_tmux_lib.sh`; use an existing regression script as the structural example.
6. Inspect the generated `*.txt` captures and `stderr.log`; use exact text assertions where stable.

## Focused Verification

When the user asks whether a concrete TUI behavior is fixed, do not rely only on the default `/help` smoke test. Convert the behavior into a tmux flow:

1. Name the flow after the behavior, for example `queue-cancel-resend`.
2. Define checkpoints: `initial`, `queued`, `after_cancel`, `after_resend`, etc.
3. At each checkpoint, capture the pane to a task-specific directory under `target/`.
4. Assert stable visible markers with `rg`; avoid brittle assertions on session ids, timing-dependent model output, or full-screen whitespace.
5. Always assert `stderr.log` is empty and the tmux session is cleaned up.

For one-off verification, create the script in a task-specific directory under `target/`. For a reusable regression, add it under this skill's `scripts/` directory. Source `scripts/tui_tmux_lib.sh`, which provides:

- `tui_start`: build if needed, create the runner, and start the fixed-size tmux session.
- `tui_capture <name>`: save the current pane as `<name>.txt`.
- `tui_send_keys ...`: send keys such as `"/help" Enter`, `Escape`, `PageUp`.
- `tui_assert_contains <capture> <regex> <message>`: fail if a capture lacks an expected marker.
- `tui_assert_not_contains <capture> <regex> <message>`: fail if a capture shows a forbidden marker.
- `tui_assert_stderr_empty`: fail on stderr output.

Example custom flow:

```bash
#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"
source "$REPO_ROOT/.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_lib.sh"

TUI_CASE="queue-cancel-resend"
TUI_OUT_DIR="target/tui-flows/$TUI_CASE"
tui_start

sleep 3
tui_capture "initial"
tui_assert_contains "initial" "ACN|open|initializing" "TUI did not start"

# Replace these keys with the flow required by the user story.
tui_send_keys "第一条" Enter
sleep 1
tui_send_keys "第二条" Enter
sleep 1
tui_send_keys Escape
sleep 1
tui_capture "after_cancel"

tui_assert_contains "after_cancel" "第二条|turn cancelled" "cancel did not restore expected state"
tui_assert_stderr_empty
```

If a flow depends on long LLM turns, prefer a controlled test hook or a focused unit test for the exact state transition, then use tmux to verify the terminal shell of the behavior. Do not fake successful external behavior in production code.

## Default Checks

The bundled script builds `--bin acn` first, then starts a fixed-size tmux session and captures:

- `initial.txt`: startup screen.
- `after_help.txt`: screen after typing `/help` and pressing Enter.
- `final.txt`: final visible screen before cleanup, if the session still exists.
- `stderr.log`: stderr from the TUI process.

Treat the smoke test as passing only when:

- the captured screen shows the ACN status line or initialization state;
- `/help` shows the `ACN commands` header and the `/skills` command;
- `stderr.log` is empty;
- the tmux session is cleaned up.

Expected stable strings in the current TUI include `Agent Claim Network`, `Whisper your wish here...`, `type / for commands · Enter sends`, `ACN commands`, `/skills`, `initializing`, and `open`. Agent ids, session ids, elapsed time, model output, and terminal whitespace are dynamic and must not be treated as stable assertions.

## Useful Commands

Run the default smoke test:

```bash
.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_smoke.sh
```

Run focused resize regressions:

```bash
.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_welcome_resize_regression.sh
.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_live_region_regression.sh
```

Use a different agent or config:

```bash
.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_smoke.sh \
  --command "cargo run --quiet --bin acn -- --config config.toml --upstream agent_hub"
```

Skip the pre-build only when the binary is already known to be current:

```bash
.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_smoke.sh --skip-build
```

Keep ANSI escape codes for color/style review:

```bash
tmux capture-pane -t acn_tui_smoke -e -p > target/tui-smoke/screen.ansi.txt
```

Send keys manually to an active session:

```bash
tmux send-keys -t acn_tui_smoke "/help" Enter
tmux send-keys -t acn_tui_smoke PageUp PageDown
tmux send-keys -t acn_tui_smoke Escape
tmux send-keys -t acn_tui_smoke "/exit" Enter
```

Always clean up stale sessions before or after a failed run:

```bash
tmux kill-session -t acn_tui_smoke 2>/dev/null || true
```

## When Debugging Failures

- Empty or garbled capture: confirm the command is running in tmux, the fixed size is large enough, and the TUI entered alternate screen.
- Missing prompt/footer: inspect `src/session_tui/chat_widget.rs`, `src/session_tui/bottom_pane/mod.rs`, and the current rendering tests.
- Keys not taking effect: inspect `handle_key_event`, `classify_input`, and whether the state accepts text.
- Non-empty stderr: fix the underlying panic, terminal error, config issue, or dependency problem instead of bypassing the smoke test.
- Startup hangs: capture the initial screen and stderr, then decide whether the app is waiting on external services, LLM env vars, or session initialization.
