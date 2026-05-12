---
description: "Fast CI check: lint + pytest only. Use when: 'quick check', 'fast CI', 'lint and test', 'ci fast', 'pre-commit check'."
argument-hint: "Optional: 'fix' to auto-fix lint/format issues"
agent: "agent"
---

Run only lint and tests — skip build, import smoke, and platform checks.

## 1. Lint

Run `ruff check .` and `ruff format --check .` from the workspace root.
If either fails and the user passed "fix", auto-fix with `ruff check --fix .`
and `ruff format .`. Otherwise report and stop.

## 2. Test

Run `pytest --tb=short -q` and report the summary line.

## Rules

- Use the workspace `.venv` Python interpreter.
- On failure: show relevant output, diagnose, and suggest a fix.
- On success: confirm with `CI-fast passed: lint ✓, test ✓ (N passed, M skipped)`.

{{input}}
