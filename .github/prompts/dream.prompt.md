---
mode: agent
description: "Consolidate agent memories into improved instructions and prompts. Run periodically to turn lessons learned into permanent workspace rules."
---

# Dream Cycle

You are running a **dream cycle** — consolidating lessons from `.memories/` into permanent workspace rules in `.github/`.

## Process

### 1. Gather

Read all files in `.memories/` (skip `_archive/` and `INDEX.md`). Parse the `severity` frontmatter from each file.

If `.memories/` is empty or contains only `.gitkeep`, report "No memories to process" and stop.

### 2. Conflicts First

Surface any memories containing a `**Conflict**:` line. For each conflict, present the user with options:
- **Update rule** — modify the `.github/` file to match the lesson
- **Discard lesson** — archive the memory without changes
- **Keep both** — add a nuanced note to the rule acknowledging the exception

Wait for user input before proceeding past conflicts.

### 3. Cluster

Group remaining memories by theme (e.g., "SQL queries", "dossier formatting", "pairing workflow"). Weight by severity.

### 4. Evaluate

Apply severity-aware thresholds to decide what to act on:
- Any `severity: high` → act immediately
- 2+ `severity: medium` on the same topic → act
- 3+ `severity: low` on the same topic → act
- Below threshold → defer (keep in `.memories/` for next cycle)

### 5. Propose

Present a **dream plan** to the user:
- Which `.github/` files will be modified
- What rules/guidelines will be added or refined
- Which memories will be archived
- Which memories will be deferred

**Wait for user approval before making any changes.**

### 6. Apply

Edit `.github/` files minimally:
- Add or refine rules — don't restructure existing content
- Match the voice and formatting of the existing file
- Prefer appending to sections over rewriting them

### 7. Archive

Move processed memories to `.memories/_archive/`:
- Create `_archive/` if it doesn't exist
- Move each processed memory file there
- Verify `_archive/` is covered by `.gitignore` (it's under `.memories/`)

### 8. Update INDEX

Regenerate `.memories/INDEX.md` with remaining (deferred) memories grouped by theme:

```markdown
# Memory Index

Last updated: YYYY-MM-DD

## <Theme>
- `filename.md` — one-line summary (severity)

## <Theme>
- `filename.md` — one-line summary (severity)
```

### 9. Report

Summarize:
- What rules were added/modified in `.github/`
- What memories were archived
- What memories were deferred (and why)
- Remind user: all `.github/` changes are revertible via `git checkout`
