---
description: "Rules for reading, writing, and maintaining agent memory files in .memories/"
applyTo: ".memories/**"
---

<!-- GENERATED FROM .github/_shared/instructions/memory-management.instructions.md -->
<!-- DO NOT EDIT — run `python .github/tools/sync-shared-meta.py` at the parent root. -->

# Memory Management

How agents read and write `.memories/` in the cellarbrain workspace.

## Filename convention

```
YYYY-MM-DD_<category>_<short-slug>.md
```

### Categories

- `mistake` — factual errors, wrong assumptions, failed approaches
- `efficiency` — faster ways to accomplish tasks
- `convention` — project style, naming, or structure rules discovered
- `user-preference` — explicit user guidance on how they like things done
- `tool-behavior` — quirks or gotchas with tools, APIs, or libraries
- `pattern` — reusable solutions or recurring code shapes

## Content template

```markdown
---
severity: medium
---
# <Short title>

**Context:** What was happening when this was learned.

**Lesson:** The actionable takeaway.

**Evidence:** Path, commit, or conversation reference.
```

`Context` / `Lesson` / `Evidence` are recommended structure for non-trivial
lessons; a short free-form body is fine for incidental notes.

## Severity

| Level | When |
|---|---|
| `high` | Explicit user correction, factual error, security issue, repeated failure. |
| `medium` | Efficiency win, useful pattern, unexpected tool behaviour. |
| `low` | Incidental observation, situational note. |

## Conflict handling

When a memory contradicts an existing rule in `.github/`:

- Set `severity: high`.
- Add a `**Conflict:**` line explaining what it contradicts and why.

Conflicts are surfaced first during `/dream` cycles for resolution.

## Reading rules

1. Check `.memories/INDEX.md` first (if it exists) for a grouped overview.
2. Scan filenames — category + slug usually convey enough to decide relevance.
3. Read individual files only when the topic is directly relevant to the
   current task.

## Cadence

- 10+ active memories → mention `/dream` to the user (once per conversation).
- 25+ active memories → strongly recommend running `/dream`.

## Hard rules

- **Never** `git add` any `.memories/` file (it is gitignored).
- **Never** reference memory contents verbatim in user-facing output.
- **Never** modify an existing memory file — write a new one instead
  (append-only system; corrections get new files).
- **Never** write secrets, credentials, tokens, or API keys into memories.

## Bootstrap

On first use in a fresh clone: create `.memories/` + `.gitkeep` if missing,
and verify `.gitignore` covers `.memories/`.
