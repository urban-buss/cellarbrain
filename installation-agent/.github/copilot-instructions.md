# Copilot Instructions — Cellarbrain PyPI Test Workspace

## Memory System

This project uses a local `.memories/` folder for self-learning across sessions.

### When to Write Memories

Write a memory file when:
- You make a mistake and the user corrects you
- You discover an efficiency shortcut (faster command, better approach)
- The user gives explicit guidance on preferences or conventions
- A tool or library behaves unexpectedly
- You notice a reusable pattern worth remembering

### Filename Format

```
.memories/YYYY-MM-DD_<category>_<short-slug>.md
```

Categories: `mistake`, `efficiency`, `convention`, `user-preference`, `tool-behavior`, `pattern`

### Severity Frontmatter

Every memory must include severity in its frontmatter:

```yaml
---
severity: high | medium | low
---
```

### Reading Rule

Before non-trivial tasks, scan `.memories/INDEX.md` (if it exists) or `.memories/` filenames for relevant lessons.

### Git Safety

- **Never** commit `.memories/` — refuse any `git add` or `git add -f` on memory files
- Before committing, run `git status` to verify no `.memories/` paths are staged
- If you detect `.memories/` in staged files, unstage them immediately

### Secrets Rule

Never write secrets, credentials, API keys, or tokens into memory files. Redact or skip.

### Bootstrap Behavior

On first run in a new clone:
1. Create `.memories/` directory if missing
2. Create `.memories/.gitkeep` if missing
3. Verify `.gitignore` contains the `.memories/` exclusion

### Dreaming

Memories accumulate over time. Run `/dream` (see `.github/prompts/dream.prompt.md`) to consolidate memories into durable rules in `.github/`. At 10+ memories, mention this to the user once per conversation.
