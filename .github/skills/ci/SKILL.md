---
name: ci
description: "Run the full CI pipeline locally: lint, format, typecheck, tests, smoke-test"
user-invocable: true
disable-model-invocation: true
---
<!-- GENERATED FROM .github/_shared/prompts/ci.prompt.md -->
<!-- DO NOT EDIT — run `python .github/tools/sync-shared-meta.py` at the parent root. -->

# Local CI

Run the full CI pipeline locally for `cellarbrain`. All steps are
**blocking** — every step must pass with zero errors and zero warnings unless
the user explicitly overrides.

## 1. Lint & format

```
ruff check .
ruff format --check .
```

Fix any issues before proceeding. Auto-fix with `ruff check --fix .` and
`ruff format .` if the user confirms.

## 2. Type check

```
mypy src/cellarbrain
```

Mypy is currently `continue-on-error` in GitHub CI, but locally we still
report and review every finding. Fix what you can; note the rest in the
report.

## 3. Test collection check

```
pytest --collect-only -q
```

Verify there are no collection errors (missing fixtures, bad imports) and no
`PytestCollectionWarning`.

## 4. Unit tests with coverage

```
pytest --cov=cellarbrain --cov-report=term-missing
```

All tests must pass. Report the coverage summary line.

## 5. Skills sync check

Verify mirrored skills are in sync:

```
python .github/tools/sync-skills.py
git diff --exit-code src/cellarbrain/skills/
```

If the diff is non-empty, the skill mirrors are stale — re-commit and retry.

## 6. Smoke test

Verify the package imports and the CLI entry point works:

```
python -c "import cellarbrain; print(cellarbrain.__version__)"
cellarbrain --help
```

If the MCP server module is available, also verify it loads:

```
python -c "from cellarbrain.mcp_server import mcp; print(f'{len(mcp._tool_manager._tools)} tools registered')"
```

## Fix & retest

If any step produces errors or warnings, fix the issues and re-run the
failing step(s) until they pass cleanly.

## Report

Summarise pass/fail per step. Confirm all checks passed (or list what was
deferred and why).
