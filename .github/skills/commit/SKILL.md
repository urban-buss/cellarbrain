---
name: commit
description: "Stage changes and create a well-formed conventional commit"
user-invocable: true
disable-model-invocation: true
---
<!-- GENERATED FROM .github/_shared/prompts/commit.prompt.md -->
<!-- DO NOT EDIT — run `python .github/tools/sync-shared-meta.py` at the parent root. -->

# Commit

Create a well-formed commit for the current `cellarbrain` changes.
Follow these steps.

## 1. Review changes

```
git status
git diff --stat
```

Examine the staged and unstaged changes to understand what was modified.

## 2. Verify quality

Before committing, ensure:

- `ruff check .` passes (no lint errors).
- `ruff format --check .` passes (formatting clean).
- `pytest` passes (no broken tests).
- No `.memories/` files are staged — if found, unstage them immediately.

## 3. Stage files

Stage the relevant files logically. Group related changes into a single
commit. If changes span unrelated concerns, suggest splitting into multiple
commits.

**Never stage:**

- `.memories/` files
- `cellarbrain.local.toml`
- `output/` directory contents
- Anything listed in `.gitignore` (refuse `-f` overrides for these)

## 4. Commit message

Use conventional commit format:

```
<type>(<scope>): <short summary>

<optional body explaining what and why>
```

### Types

- `feat` — new feature or capability
- `fix` — bug fix
- `refactor` — code restructuring without behaviour change
- `test` — adding or updating tests
- `docs` — documentation changes
- `chore` — build, CI, tooling changes
- `perf` — performance improvement

### Scope

Use the most specific module/area touched (for example: `parser`, `mcp`,
`cli`, `query`, `dossier`, `writer`). For changes that cross many areas, omit
the scope.

## 5. Execute

Run the commit directly — do not ask for approval:

```
git commit -m "<message>"
```

**Do not push** unless the user explicitly asks. Pushing the brain's branch
should be a separate, deliberate step.
