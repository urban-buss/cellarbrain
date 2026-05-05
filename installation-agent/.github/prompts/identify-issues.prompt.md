---
description: "Analyze chat history to identify issues, bugs, and problems encountered during cellarbrain testing, then save them as structured issue files in .github/issues/"
agent: "agent"
---
Review the full conversation history and identify all issues encountered. For each issue, create a separate markdown file in `.github/issues/`.

## Process

1. Read through the entire chat history carefully
2. Identify distinct issues: errors, unexpected behavior, workarounds needed, version mismatches, configuration problems
3. Create the `.github/issues/` folder if it doesn't exist
4. Write one file per issue using the naming convention: `NNN-short-slug.md` (e.g. `001-stale-editable-install.md`)

## Issue File Format

Each issue file should contain:

```markdown
# <Title>

**Status:** open | resolved | workaround
**Severity:** critical | high | medium | low
**Component:** etl | mcp | dashboard | ingest | install | cli
**Version:** <cellarbrain version affected>

## Description

<Clear description of what went wrong or was unexpected>

## Steps to Reproduce

1. <step>
2. <step>

## Expected Behavior

<what should happen>

## Actual Behavior

<what actually happened, include error messages>

## Workaround

<if any workaround was found, describe it>

## Resolution

<if resolved, explain how>
```

## Guidelines

- Be specific: include exact error messages, commands, and versions
- Separate distinct problems into individual files even if they appeared together
- Note whether an issue was resolved during the session or remains open
- Include the workaround if one was discovered
