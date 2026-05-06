---
description: "Simulate the full GitHub Actions CI pipeline locally (lint + test matrix). Use when: 'run CI', 'simulate CI', 'pre-push check', 'will CI pass', 'check before push', 'lint and test'."
argument-hint: "Optional: specific job to run (lint, test) or Python version"
agent: "agent"
---

Read the CI workflow at [ci.yml](../.github/workflows/ci.yml) and simulate each
job locally in the terminal. Run them in the same order GitHub Actions would:

## 1. Lint

Run `ruff check .` and `ruff format --check .` from the workspace root.
If either fails, report the issues and stop — do not proceed to tests.

## 2. Test

Run `pytest` with `--tb=short -q` to keep output concise.
Report the final summary line (passed / failed / skipped / errors).

## Rules

- Use the workspace `.venv` Python interpreter (`.venv/Scripts/python.exe` on
  Windows, `.venv/bin/python` on Unix).
- If the user specifies a job name (e.g. "lint" or "test"), run only that job.
- If the user specifies a Python version, note it but use the local venv
  (mention any version mismatch as a caveat).
- On failure: show the relevant output, diagnose the issue, and suggest a fix.
- On success: confirm with a single summary line like
  `CI simulation passed: lint ✓, test ✓ (N passed, M skipped)`.
