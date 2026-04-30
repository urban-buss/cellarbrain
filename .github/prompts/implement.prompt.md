---
description: "Plan and implement all changes from an analysis document. Use when: 'implement analysis', 'implement plan', 'execute recommendations', 'fix issues from analysis'."
argument-hint: "Link the analysis doc to implement"
agent: plan
---
Read the linked analysis document completely. Then:

## 1. Plan

Build a detailed implementation plan covering **every** recommendation /
proposed fix in the analysis. For each item include:

- Files to create or modify (source, tests, docs, dashboard)
- Specific changes (schema additions, new functions, config keys)
- Test updates — add or extend tests in the matching `tests/test_<module>.py`
- Doc updates — update relevant pages in `docs/` if behaviour changes
- Dashboard updates — adjust `dashboard/` templates or routes if UI is affected
- Dependencies / ordering between items

Present the plan as a numbered checklist before writing any code.

## 2. Implement

Execute the plan item by item. After all changes:

- Run `pytest` and fix any failures
- Verify no regressions in related tests

## Analysis document

{{input}}
