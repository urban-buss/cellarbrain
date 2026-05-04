---
applyTo: ".memories/**"
---
# Memory Management Instructions

## Filename Convention

```
YYYY-MM-DD_<category>_<short-slug>.md
```

**Categories:** `mistake`, `efficiency`, `convention`, `user-preference`, `tool-behavior`, `pattern`

**Examples:**
- `2026-05-04_mistake_wrong-view-for-bottles.md`
- `2026-05-04_efficiency_batch-dossier-updates.md`
- `2026-05-04_convention_always-check-freshness.md`

## Content Template

```markdown
---
severity: medium
---
# <Short title>

**Context:** What was happening when this was learned.

**Lesson:** The actionable takeaway.

**Evidence:** Link to conversation or file where this came up.
```

## Severity Guide

| Level | When to use |
|-------|-------------|
| `high` | Explicit user correction, factual error, security issue, or repeated failure |
| `medium` | Efficiency wins, useful patterns, or unexpected behavior |
| `low` | Incidental observations, situational notes |

## Conflict Handling

When a memory contradicts an existing rule in `.github/`:
- Mark severity as `high`
- Add a `**Conflict**:` line explaining the contradiction
- Example: `**Conflict**: copilot-instructions.md says X, but user corrected to Y`

These are surfaced first during dream cycles for resolution.

## Reading Rules

1. Check `.memories/INDEX.md` first (if it exists) for a grouped overview
2. Scan filenames — the category and slug convey enough to decide relevance
3. Only read full content when the filename suggests relevance to the current task

## Cadence

- At **10+ active memories**: mention `/dream` to the user (once per conversation)
- At **25+ active memories**: strongly recommend running `/dream`

## Hard Rules

- **Never** `git add` any `.memories/` file
- **Never** reference memory content in user-facing outputs
- **Never** modify existing memory files (append-only system; corrections get new files)
- **Never** write secrets, tokens, passwords, or API keys to memories
