---
name: dream
description: "Consolidate agent memories into learned rules — the dream cycle"
user-invocable: true
disable-model-invocation: true
---
<!-- GENERATED FROM .github/_shared/prompts/dream.prompt.md -->
<!-- DO NOT EDIT — run `python .github/tools/sync-shared-meta.py` at the parent root. -->

# Dream Cycle

You are running the **dream cycle** — consolidating lessons from `.memories/`
into permanent workspace rules under `.github/`.

Follow these steps in order. **Do not apply changes without user approval.**

## 1. Gather

- Read every file in `.memories/` (skip `_archive/` and `INDEX.md`).
- Parse the `severity` frontmatter from each file.
- Report what was found: count by category and severity.

If `.memories/` is empty or contains only `.gitkeep`, report
"No memories to process" and stop.

## 2. Conflicts first

Surface any memory containing a `**Conflict:**` line. For each, present three
options:

1. **Update rule** — modify the `.github/` file to match the lesson.
2. **Discard lesson** — archive the memory without changes.
3. **Keep both** — add a nuanced note to the rule acknowledging the exception.

Wait for user resolution before proceeding past conflicts.

## 3. Cluster

Group remaining memories by theme. Weight clusters by severity
(`high` > `medium` > `low`).

## 4. Evaluate

Apply severity-aware thresholds to decide which clusters to act on:

- Any `severity: high` → act immediately.
- 2+ `severity: medium` on the same theme → act.
- 3+ `severity: low` on the same theme → act.
- Below threshold → defer (keep in `.memories/` for the next cycle).

## 5. Propose

Present a **dream plan** to the user:

- Which `.github/` files will be modified or created.
- What rules/guidelines will be added or refined.
- Which memories will be archived.
- Which memories will be deferred (and why).

**Wait for explicit user approval before making any changes.**

## 6. Apply

Edit `.github/` files minimally:

- Add or refine rules — don't restructure existing content.
- Match the voice and formatting of the existing file.
- Prefer appending to sections over rewriting them.

## 7. Archive

Move processed memories to `.memories/_archive/`:

- Create `_archive/` if it does not exist.
- Move each processed memory file there.
- Verify `_archive/` is still covered by `.gitignore`.

## 8. Update INDEX

Regenerate `.memories/INDEX.md` with the remaining (deferred) memories
grouped by theme:

```markdown
# Memory Index

Last updated: YYYY-MM-DD

## <Theme>
- `filename.md` — one-line summary (severity)
```

If no memories remain, write a placeholder noting the date of the last dream.

## 9. Report

Summarise:

- What rules were added or changed in `.github/`.
- What memories were archived.
- What memories were deferred (and why).
- Remind the user: all `.github/` changes are revertible via
  `git checkout -- .github/`.

## Hard rules

- Never `git add` anything inside `.memories/`.
- Never reference memory content verbatim in user-facing output.
- Never write secrets into rules or memories.
