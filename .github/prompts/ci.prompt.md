---
description: "Simulate the full GitHub Actions CI pipeline locally (lint + test matrix). Use when: 'run CI', 'simulate CI', 'pre-push check', 'will CI pass', 'check before push', 'lint and test'."
argument-hint: "Optional: specific job to run (lint, test, build, imports) or Python version"
agent: "agent"
---

Read the CI workflow at [ci.yml](../.github/workflows/ci.yml) and simulate each
job locally in the terminal. Run them in the same order GitHub Actions would:

## 1. Lint

Run `ruff check .` and `ruff format --check .` from the workspace root.
If either fails, report the issues and stop — do not proceed to later steps.

## 2. Build / Install check

Run `pip install -e ".[research,dashboard]" --dry-run` to verify that
`pyproject.toml` is valid, all declared dependencies resolve, and the package
builds without errors. This catches:
- Typos or missing entries in `[project.dependencies]` or `[project.optional-dependencies]`
- Invalid `pyproject.toml` syntax that would fail `pip install -e .` in CI
- Missing or mis-named package data declarations

If `--dry-run` is not supported by the local pip version, fall back to
`python -m build --no-isolation` (requires `build` package) or at minimum
`pip install -e . 2>&1` and check the exit code.

## 3. Import smoke test

After ensuring the package is installed, verify that all public modules
import cleanly — the same way CI will discover import errors at test
collection time:

```
python -c "
import cellarbrain.cli
import cellarbrain.settings
import cellarbrain.mcp_server
import cellarbrain.query
import cellarbrain.transform
import cellarbrain.writer
import cellarbrain.markdown
import cellarbrain.dossier_ops
import cellarbrain.computed
import cellarbrain.pairing
import cellarbrain.search
import cellarbrain.price
import cellarbrain.info
import cellarbrain.doctor
import cellarbrain.service
import cellarbrain.dashboard
"
```

If any import fails, report the traceback and stop.

## 4. Test

Run `pytest` with `--tb=short -q` to keep output concise.
Report the final summary line (passed / failed / skipped / errors).

## 5. Platform-divergence checks

These catch issues that would only surface on GitHub's Ubuntu runner:

- **Path separators**: search for hardcoded `\\` in assertions or string
  comparisons in tests. Flag any that aren't wrapped in `pathlib.Path()` or
  `os.sep` — they will break on Linux.
- **Case sensitivity**: flag any file path references in tests that rely on
  Windows case-insensitivity (e.g. `"Output"` vs `"output"`).
- **Missing test markers**: if any test depends on local files (raw/ CSV,
  output/ Parquet, .env), verify it has `@pytest.mark.skipif` or
  `@pytest.mark.integration` so it won't fail in CI's clean checkout.
- **Encoding assumptions**: flag any `open()` calls in tests that omit
  `encoding=` — Ubuntu's default may differ from Windows.

Report findings as warnings (they won't block, but note them).

## Rules

- Use the workspace `.venv` Python interpreter (`.venv/Scripts/python.exe` on
  Windows, `.venv/bin/python` on Unix).
- If the user specifies a job name (e.g. "lint", "test", "build", "imports"),
  run only that job.
- If the user specifies a Python version, note it but use the local venv
  (mention any version mismatch as a caveat).
- CI tests against Python 3.11, 3.12, 3.13. Note the local version and flag
  if using syntax/features not available in 3.11 (e.g. `type` statement,
  `ExceptionGroup` without backport).
- CI installs extras `[research,dashboard]`. If those extras aren't installed
  locally, note which tests may be skipped that would run in CI.
- On failure: show the relevant output, diagnose the issue, and suggest a fix.
  Auto-fix lint/format issues with `ruff check --fix .` and `ruff format .`
  if the user confirms.
- On success: confirm with a single summary line like
  `CI simulation passed: lint ✓, build ✓, imports ✓, test ✓ (N passed, M skipped)`.
