---
description: "Prepare a release branch with a squash commit. Use when: 'release', 'cut a release', 'publish version', 'bump version', 'tag release'."
argument-hint: "Optional: version bump type (patch, minor, major) or explicit version"
agent: "agent"
---

Prepare a cellarbrain release: bump version, create a feature branch from main,
and squash-merge all local branch changes into a single meaningful commit.

## 1. Verify local branch

Check that the current branch starts with `local_`:

```bash
git branch --show-current
```

If the branch does not match `local_*`, stop and ask the user to check out a
local branch first.

## 2. Pre-flight checks

Run these checks before proceeding — stop on failure. If CI checks (lint,
tests, imports) were already executed **and passed** earlier in the same
session, skip them and note "pre-flight already passed this session".

1. **Clean working tree**: `git status --porcelain` must be empty (untracked
   files in ignored directories are OK). If there are uncommitted changes,
   stop and ask the user to commit or stash first.
2. **Lint passes**: `ruff check . && ruff format --check .`
3. **Import smoke**: verify key modules import cleanly (same as ci.prompt.md §3).
4. **Tests pass**: `pytest --tb=short -q`

If any check fails, diagnose, suggest a fix, and stop — do not continue.

## 3. Bump version

Read the current version from `pyproject.toml` (`[project] version`).
Compare it to the version on `main`:

```bash
git show main:pyproject.toml | grep -m1 'version'
```

- If the user specified a bump type (patch/minor/major) or explicit version,
  use that.
- If the local version is **the same** as on `main`, bump to the next **patch**
  version.
- If the local version is already ahead of `main`, keep it as-is (no bump
  needed).

When bumping, update:
- `version` in `pyproject.toml`.
- Rename the `[Unreleased]` section in `CHANGELOG.md` to
  `[X.Y.Z] — YYYY-MM-DD` (today's date) and add a fresh empty
  `[Unreleased]` section above it.

Commit the version bump on the local branch:

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "chore: bump version to X.Y.Z"
```

## 4. Review commits

Collect all commits on the local branch that are not yet on `main`:

```bash
git log main..HEAD --pretty=format:"%s%n%b" --reverse
```

Review these to understand the scope of changes. This informs both the branch
name (step 5) and the squash commit message (step 6).

## 5. Create feature branch

Choose a **meaningful branch name** based on the consolidated changes from
step 4. The name should summarise the theme of the work (e.g.
`sommelier-path-anchoring`, `food-tags-rederive`, `mcp-price-tracking`).

Do **not** simply strip the `local_` prefix — think about what name best
describes the combined changes.

```bash
git checkout main
git pull --ff-only origin main
git checkout -b <feature-branch>
```

## 6. Squash commit

Squash-merge all changes from the local branch into the new feature branch:

```bash
git merge --squash local_<local-branch-name>
git commit -m "<squash commit message>"
```

### Commit message format

- **Subject line**: `release vX.Y.Z — <1-line summary of the theme>`
- **Body**: a bullet list of the key changes, grouped by category (features,
  fixes, chores). Derive from the individual commit messages and the CHANGELOG.

**Rules for the commit message:**
- Do NOT include issue IDs, analysis doc IDs, or internal references.
- Do NOT just repeat "chore: release vX.Y.Z" — summarise actual changes.
- Keep it concise but informative.

Example:

```
release v1.5.0 — sommelier path anchoring, daemon restart, dual-format output

Features:
- Anchor sommelier model paths to data_dir for portable installs
- Auto-restart ingest daemon on package upgrade or max uptime
- Add plain-text output format alongside Markdown for MCP tools

Fixes:
- Fix DuckDB binder error on stale Parquet schema

Chores:
- Extend CI prompt with build and import smoke checks
- Fix lint/format issues across query, pairing, and test modules
```

## 7. Final state

After completion, the workspace should be on the **new feature branch** with
the squash commit as HEAD. The local branch remains intact (not deleted).

Do **not** push — the user will handle push and PR creation separately.

## Rules

- Never skip the local branch check (step 1).
- Use the workspace `.venv` Python for all commands.
- If any step fails, diagnose, suggest a fix, and stop — do not continue the
  release with a broken state.
- Do not push, create tags, or delete branches.
- Do not include internal issue IDs or analysis doc references in commit
  messages or branch names.

{{input}}
