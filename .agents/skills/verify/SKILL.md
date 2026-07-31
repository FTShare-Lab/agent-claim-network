---
name: verify
description: Run ACN's full Rust verification — formatting, Clippy, tests, and type checking — plus the canonical tmux smoke test when interactive TUI behavior is affected.
---

# Verify ACN

Run from the repository root. Load the local environment file when present, but never print its contents:

```bash
if [[ -f export_env.sh ]]; then
  source export_env.sh
fi
scripts/check_version_consistency.sh
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo check
```

Report each command's result. Stop and diagnose failures instead of skipping the remaining verification silently.

When the change affects `src/session_tui/`, interactive behavior in `src/bin/acn.rs`, terminal rendering, input handling, or TUI-facing help/status text, also follow `../tui-smoke-test-with-tmux/SKILL.md` and run:

```bash
.agents/skills/tui-smoke-test-with-tmux/scripts/tui_tmux_smoke.sh
```

Treat the bundled script's current assertions and captured `stderr.log` as authoritative; do not duplicate old prompt or footer strings in this skill.
