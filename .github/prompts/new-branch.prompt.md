---
description: "Create a new local dev branch from latest main, named with the next version. Use when: 'new branch', 'start new version', 'fresh branch', 'next version branch'."
agent: "agent"
---

Create a new local development branch from the latest `main`, named after the next patch version.

## Steps

1. **Read current version** from `pyproject.toml` (`[project] version`).

2. **Compute next version**: increment the patch component by 1.
   Example: `0.2.13` → `0.2.14`.

3. **Derive branch name**: `local_v<version with dots replaced by dashes>`.
   Example: version `0.2.14` → branch `local_v0-2-14`.

4. **Update local main**:
   ```bash
   git fetch origin
   git checkout main
   git pull origin main
   ```

5. **Create and switch to the new branch**:
   ```bash
   git checkout -b local_v0-2-14
   ```
   (use the actual computed branch name)

6. **Confirm** by printing the current branch and the version it targets.
