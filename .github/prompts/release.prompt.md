---
description: "Prepare and publish a new cellarbrain release. Use when: 'release', 'cut a release', 'publish version', 'bump version', 'tag release'."
argument-hint: "Optional: version bump type (patch, minor, major) or explicit version"
agent: "agent"
---

Guide a cellarbrain release from version bump through PyPI publication.

## 1. Determine version

Read the current version from `pyproject.toml` (`[project] version`).
If the user specified a bump type (patch/minor/major) or explicit version, use
that. Otherwise infer from the `[Unreleased]` section in `CHANGELOG.md`:
- Only fixes → patch
- New features or settings → minor
- Breaking changes or schema migrations → major

## 2. Pre-flight checks

Run these checks before proceeding — stop on failure:

1. **Clean working tree**: `git status --porcelain` must be empty.
2. **On a local branch**: `git branch --show-current` must start with `local_`
   (e.g. `local_imessage-daemon-sommelier`). Store the branch name — the part
   after `local_` becomes the feature branch name.
3. **Tests pass**: `pytest --tb=short -q`.
4. **Lint passes**: `ruff check . && ruff format --check .`.
5. **Import smoke**: verify key modules import cleanly (same as ci.prompt.md §3).
6. **No TODO/FIXME in staged changes**: `git diff main --name-only` should not
   contain files with unresolved markers in the diff.

## 3. Update version

- Bump `version` in `pyproject.toml`.
- Rename the `[Unreleased]` section in `CHANGELOG.md` to
  `[X.Y.Z] — YYYY-MM-DD` (today's date) and add a fresh empty
  `[Unreleased]` section above it.

## 4. Create feature branch and squash commit

Derive the feature branch name by stripping the `local_` prefix from the
current branch (e.g. `local_imessage-daemon-sommelier` → `imessage-daemon-sommelier`).

### Build the squash commit message

Before squashing, collect all individual commit messages from the local branch:

```bash
git log main..HEAD --pretty=format:"%s%n%b" --reverse
```

Compose a **meaningful** squash commit message:

- **Subject line**: `release vX.Y.Z — <1-line summary of the theme>` (e.g.
  `release v1.5.0 — sommelier path anchoring, daemon restart, dual-format output`).
- **Body**: a bullet list summarising the key changes across all squashed
  commits, grouped by category (features, fixes, chores). Derive these from
  the individual commit messages and the `[X.Y.Z]` CHANGELOG section — do NOT
  just repeat "chore: release vX.Y.Z".

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

### Git commands

```bash
# Commit version bump on the local branch first
git add pyproject.toml CHANGELOG.md
git commit -m "chore: bump version to vX.Y.Z"

# Create feature branch from main
git checkout main
git pull --ff-only origin main
git checkout -b <feature-branch>

# Squash-merge all local branch changes
git merge --squash local_<feature-branch>
git commit -m "<squash commit message composed above>"
```

Do **not** push — the user will handle push and PR creation separately.

## Rules

- Never skip pre-flight checks.
- Use the workspace `.venv` Python for all commands.
- If any step fails, diagnose, suggest a fix, and stop — do not continue the
  release with a broken state.
- Do not push or create tags — the prompt only prepares a local feature branch.

{{input}}
