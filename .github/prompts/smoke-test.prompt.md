---
description: "Run extended smoke tests for recent changes — validates new features and checks for regressions. Use when: 'smoke test', 'test changes', 'regression test', 'validate feature'."
argument-hint: "Describe what changed or leave blank to auto-detect"
agent: cellarbrain-extended-smoketest
---
Run the extended smoke-test pipeline against the current state. Focus on:

1. **New feature validation** — exercise any newly added or modified
   functionality described below (or auto-detected from recent changes).
2. **Regression check** — ensure existing behaviour is not broken.
3. **Report** — produce a timestamped report in `smoke-reports/`.

If specific changes are described below, design targeted test cases for them
in addition to the standard pipeline.

## Changes to validate

{{input}}
