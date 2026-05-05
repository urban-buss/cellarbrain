---
description: "Rules for reading, writing, and maintaining agent memory files in .memories/"
applyTo: ".memories/**"
---
# Memory Management

## Filename Convention

```
YYYY-MM-DD_<category>_<short-slug>.md
```

### Categories

- `mistake` — factual errors, wrong assumptions, failed approaches
- `efficiency` — faster ways to accomplish tasks
- `convention` — project style, naming, structure rules discovered
- `user-preference` — explicit user guidance on how they like things done
- `tool-behavior` — quirks or gotchas with tools, APIs, or libraries
- `pattern` — reusable solutions or recurring code shapes

## Content Template

```markdown
---
severity: medium
---
# <Short title>

<What happened, what was learned, and what to do differently.>
```

## Severity Guide

- **high** — explicit user correction, factual error, security issue, or repeated failure
- **medium** — efficiency wins, useful patterns, or unexpected behavior
- **low** — incidental observations, situational notes

## Conflict Handling

When a memory contradicts an existing rule in `.github/instructions/` or `.github/agents/`:

- Set `severity: high`
- Add a `**Conflict**:` line explaining what it contradicts and why

## Reading Rules

1. Check `INDEX.md` first for a grouped overview
2. Scan filenames for category/slug relevance
3. Read individual files only when the topic is directly relevant to the current task

## Dreaming Cadence

- At 10+ active memories: suggest `/dream` to the user (once per conversation)
- At 25+ active memories: strongly nudge — performance may degrade without consolidation

## Hard Rules

- **Never** `git add` any `.memories/` file
- **Never** reference memory contents in user-visible outputs
- **Never** modify an existing memory file (write a new one instead)
- **Never** write secrets, credentials, or tokens into memories
