---
description: "Consolidate agent memories into learned rules — the dream cycle"
mode: "agent"
---
# Dream Cycle

You are running the **dream cycle** — consolidating `.memories/` into durable rules.

Follow these steps exactly. **Do not apply changes without user approval.**

## 1. Gather

- Read all files in `.memories/` (skip `_archive/` and `INDEX.md`)
- Parse the `severity` frontmatter from each file
- List what you found: count by category and severity

## 2. Conflicts First

- Surface any memories containing a `**Conflict**:` line
- For each conflict, present options:
  1. Update the existing rule to match the lesson
  2. Discard the lesson (archive the memory)
  3. Keep both (note the exception)
- Wait for user resolution before continuing

## 3. Cluster

- Group related memories by topic
- Weight groups by severity (high > medium > low)

## 4. Evaluate

Apply severity-aware thresholds to decide which clusters to act on:

- Any `severity: high` → act immediately
- 2+ `severity: medium` on the same topic → act
- 3+ `severity: low` on the same topic → act
- Below threshold → defer (keep memories, skip for now)

## 5. Propose

Present a **dream plan** to the user:

- Which `.github/` files will be modified or created
- What rules/guidance will be added or refined
- Which memories will be archived
- Which memories are deferred

**Wait for explicit user approval before proceeding.**

## 6. Apply

- Edit `.github/` files minimally — add or refine rules, don't restructure
- Match existing style and conventions
- Don't remove or rewrite content that isn't related to the lessons

## 7. Archive

- Move processed memories to `.memories/_archive/`
- Verify `_archive/` is covered by `.gitignore` (it is, under `.memories/`)

## 8. Update INDEX

- Regenerate `.memories/INDEX.md` with remaining (deferred) memories grouped by theme
- If no memories remain, write a placeholder noting the last dream date

## 9. Report

Summarize:
- What rules were added or changed
- What was deferred and why
- Remind the user: all changes are revertible via `git checkout -- .github/`
