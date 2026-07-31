---
name: code-review
description: Multi-pass code review. Review changes locally, then run `codex exec --json` as an independent read-only reviewer and merge the findings. Use before merging or after significant refactoring.
---

# Multi-Pass Code Review

## 1. Define the review range

Inspect `git status`, the relevant diff, and the surrounding implementation. Preserve unrelated working-tree changes. Review the code that actually runs; do not substitute an old PRD for the current behavior.

## 2. Perform the local review

Follow the repository's `AGENTS.md` and focus on actionable defects:

- business-logic and state-transition gaps;
- persistence, recovery, compatibility, and protocol-boundary problems;
- async blocking, cancellation, locking, process-lifecycle, and resource leaks;
- tool authorization, path handling, secrets, and network exposure;
- user-visible TUI errors, misleading status, and broken interaction flows;
- missing tests for realistic failure paths.

Do not report formatting already covered by rustfmt, speculative low-probability crashes without a credible trigger, or style preferences that do not affect correctness or maintainability.

Unless the user explicitly requests a broader review, use this default severity boundary:

- Ignore extreme edge cases and crashes, layout shifts, or state mismatches assessed as extremely unlikely. Do not inflate them into actionable findings.
- Keep the actionable and automatic-fix set to P0 and P1 findings with a realistic trigger and material impact.
- Prioritize real business-logic, security/data-integrity, and user-visible TUI defects. Do not automatically fix P2/P3 findings; mention a lower-severity item only when it materially affects a decision, and leave it deferred unless the user expands the scope.

## 3. Obtain an external Codex review

Run one direct, read-only `codex exec --json` review pass. The external prompt must forbid invoking this skill, running another `codex` process, calling delegation tools, or modifying files.

Requirements:

1. Require `codex` to be available on `PATH`; do not source a personal shell startup file.
2. Create a run directory with `mktemp -d` outside the repository.
3. Save JSONL, stderr, and the final response so partial output survives interruption.
4. Use the Codex CLI's configured default model. Only pass `-m` when `REVIEW_MODEL` is explicitly set by the caller.
5. Default the external timeout to 30 minutes (`1800` seconds), overridable through `REVIEW_TIMEOUT_SECONDS`.
6. If the review times out, inspect partial artifacts, split the diff into smaller review units, and retry. Do not declare the external pass complete without a usable result.

Example:

```bash
REVIEW_DIR="$(mktemp -d "${TMPDIR:-/tmp}/acn-code-review.XXXXXX")"
REPO_ROOT="$(git rev-parse --show-toplevel)"
REVIEW_TIMEOUT_SECONDS="${REVIEW_TIMEOUT_SECONDS:-1800}"

cat > "$REVIEW_DIR/prompt.txt" <<'PROMPT'
Review the current repository changes directly. Focus on realistic business-logic defects,
security boundaries, persistence and recovery, async/process lifecycle, user-visible TUI problems,
and missing high-value tests. By default, ignore extreme or extremely unlikely crashes, layout
shifts, and state mismatches. Keep actionable findings to realistically triggered P0/P1 defects;
do not recommend fixes for P2/P3 items unless the caller explicitly requests broader coverage.
Read AGENTS.md and the surrounding implementation. Do not invoke a code-review skill, run codex or
codex exec, use delegation tools, spawn subagents, or modify files. Return actionable findings with
severity and file/line references; say explicitly when there are no findings.
PROMPT

PROMPT_TEXT="$(cat "$REVIEW_DIR/prompt.txt")"
CODEX_ARGS=(exec --json)
if [[ -n "${REVIEW_MODEL:-}" ]]; then
  CODEX_ARGS+=(-m "$REVIEW_MODEL")
fi
CODEX_ARGS+=(
  -C "$REPO_ROOT"
  --sandbox read-only
  -o "$REVIEW_DIR/final.md"
  "$PROMPT_TEXT"
)

if command -v gtimeout >/dev/null 2>&1; then
  gtimeout "$REVIEW_TIMEOUT_SECONDS" codex "${CODEX_ARGS[@]}" \
    >"$REVIEW_DIR/events.jsonl" 2>"$REVIEW_DIR/stderr.log"
elif command -v timeout >/dev/null 2>&1; then
  timeout "$REVIEW_TIMEOUT_SECONDS" codex "${CODEX_ARGS[@]}" \
    >"$REVIEW_DIR/events.jsonl" 2>"$REVIEW_DIR/stderr.log"
else
  codex "${CODEX_ARGS[@]}" \
    >"$REVIEW_DIR/events.jsonl" 2>"$REVIEW_DIR/stderr.log"
fi
```

Read `final.md` first, then use `events.jsonl` and `stderr.log` for missing or partial context.

## 4. Merge and report

Deduplicate local and external findings. Lead with findings ordered by severity, identify their source as local, external, or both, and include precise file references and realistic trigger conditions. If there are no findings, state that explicitly and list any verification gaps.
