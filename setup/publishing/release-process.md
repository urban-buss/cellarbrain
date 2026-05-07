# Release Process

How to version, tag, and publish a new Cellarbrain release.

## Versioning

Cellarbrain follows [Semantic Versioning](https://semver.org/):

| Bump | When | Example |
|------|------|---------|
| **MAJOR** | Breaking changes to CLI, MCP tools, or config format | `1.0.0` → `2.0.0` |
| **MINOR** | New features, new MCP tools, new CLI commands | `0.2.0` → `0.3.0` |
| **PATCH** | Bug fixes, parser corrections, documentation | `0.2.0` → `0.2.1` |

Version is defined in `pyproject.toml`:

```toml
[project]
version = "0.2.0"
```

## Release Checklist

### 1. Update Version

Edit `pyproject.toml` → `version = "0.3.0"`

### 2. Update Changelog

Move items from `[Unreleased]` to a new version heading in `CHANGELOG.md`:

```markdown
## [0.3.0] — 2026-05-01

### Added
- New MCP tool: `suggest_wines` for semantic food-wine pairing

### Fixed
- ...
```

### 3. Commit

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "release: v0.3.0"
```

### 4. Tag and Push

```bash
git tag v0.3.0
git push origin main
git push origin v0.3.0
```

Pushing the `v*` tag triggers the automated PyPI publish workflow.

### 5. Verify on PyPI

After ~2 minutes: `https://pypi.org/project/cellarbrain/0.3.0/`

### 6. Update Homebrew (if applicable)

See [Homebrew](homebrew.md).

## Pre-Release Testing

```bash
# 1. Full test suite
pytest

# 2. Build and verify
python -m build
twine check dist/*

# 3. Test install in clean venv
python3 -m venv /tmp/release-test
source /tmp/release-test/bin/activate
pip install dist/cellarbrain-*.whl
cellarbrain --help
deactivate
rm -rf /tmp/release-test
```

## Rollback

### PyPI

PyPI doesn't allow re-uploading the same version. For critical bugs:

1. Publish a patch release (e.g., `0.3.1`)
2. Yank the broken version on PyPI (Settings → "Yank this release")

### Homebrew

```bash
cd homebrew-cellarbrain
git revert HEAD
git push origin main
# Users: brew update && brew reinstall cellarbrain
```

## Next Steps

- [PyPI](pypi.md) — Automated and manual publishing to PyPI
- [Homebrew](homebrew.md) — Homebrew tap and formula
