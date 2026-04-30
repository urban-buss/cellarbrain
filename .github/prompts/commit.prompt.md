---
description: "Commit staged or all changes with a conventional-commit message. Use when: 'commit', 'save changes', 'commit all'."
argument-hint: "Optional: scope or hint for the commit message"
agent: agent
---
Review the current `git diff --staged` (or `git diff` if nothing is staged).
Write a concise conventional-commit message (`feat:`, `fix:`, `refactor:`,
`docs:`, `test:`, `chore:`). If changes span multiple scopes, use a
multi-line body. Stage all unstaged changes only if nothing is currently
staged. Then commit.

{{input}}
