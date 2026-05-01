# Publishing

How to version, release, and publish Cellarbrain to PyPI and Homebrew.

---

## 1. Versioning

Cellarbrain follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (`1.0.0` → `2.0.0`) — Breaking changes to CLI, MCP tools, or config format
- **MINOR** (`0.1.0` → `0.2.0`) — New features, new MCP tools, new CLI commands
- **PATCH** (`0.2.0` → `0.2.1`) — Bug fixes, parser corrections, documentation

Current version is defined in `pyproject.toml`:

```toml
[project]
version = "0.2.0"
```

---

## 2. Release Checklist

Follow these steps for every release:

### Step 1 — Update Version

Edit `pyproject.toml`:

```bash
# Edit the version field
# version = "0.2.0"  →  version = "0.3.0"
```

### Step 2 — Update Changelog

Edit `CHANGELOG.md` — move items from `[Unreleased]` to the new version heading:

```markdown
## [0.3.0] — 2026-05-01

### Added
- New MCP tool: `suggest_wines` for semantic food-wine pairing
- ...

### Fixed
- ...
```

### Step 3 — Commit the Release

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "release: v0.3.0"
```

### Step 4 — Tag and Push

```bash
git tag v0.3.0
git push origin main
git push origin v0.3.0
```

Pushing the `v*` tag triggers the automated PyPI publish workflow.

### Step 5 — Verify on PyPI

After ~2 minutes, verify the package appears at:
`https://pypi.org/project/cellarbrain/0.3.0/`

### Step 6 — Update Homebrew Formula (if applicable)

See [Section 4](#4-publishing-to-homebrew) below.

---

## 3. Publishing to PyPI

### 3.1 Automated (Recommended)

The repository includes a GitHub Actions workflow at `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  push:
    tags: ["v*"]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.x"
      - run: pip install build
      - run: python -m build
      - uses: actions/upload-artifact@v4
        with:
          name: dist
          path: dist/

  publish:
    needs: build
    runs-on: ubuntu-latest
    environment: pypi
    permissions:
      id-token: write
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/
      - uses: pypa/gh-action-pypi-publish@release/v1
```

**How it works:**
1. You push a tag matching `v*` (e.g., `v0.3.0`)
2. GitHub Actions builds the sdist + wheel
3. Uses [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC) to upload — no API tokens needed

**One-time setup for Trusted Publishing:**
1. Go to [pypi.org](https://pypi.org) → your project → "Publishing" tab
2. Add a "GitHub Actions" trusted publisher:
   - Repository owner: `urban-buss`
   - Repository name: `cellarbrain`
   - Workflow name: `publish.yml`
   - Environment name: `pypi`

### 3.2 Manual Publishing

For emergency releases or testing:

```bash
# 1. Build
pip install build twine
python -m build

# 2. Check the package
twine check dist/*

# 3. Upload to Test PyPI first (optional)
twine upload --repository testpypi dist/*

# 4. Upload to PyPI
twine upload dist/*
```

You'll be prompted for your PyPI API token (create one at pypi.org → Account settings → API tokens).

### 3.3 Verify Installation from PyPI

```bash
# Test in a clean environment
python3 -m venv /tmp/pypi-test
source /tmp/pypi-test/bin/activate
pip install cellarbrain
cellarbrain --help
deactivate
rm -rf /tmp/pypi-test
```

---

## 4. Publishing to Homebrew

> **Note:** A Homebrew formula for cellarbrain does not yet exist. This section documents how to create and maintain one.

### 4.1 Create a Homebrew Tap

A "tap" is a GitHub repository that contains Homebrew formulae.

```bash
# Create the tap repository on GitHub: urban-buss/homebrew-cellarbrain
# Then clone it locally:
git clone https://github.com/urban-buss/homebrew-cellarbrain.git
cd homebrew-cellarbrain
mkdir Formula
```

### 4.2 Write the Formula

Create `Formula/cellarbrain.rb`:

```ruby
class Cellarbrain < Formula
  include Language::Python::Virtualenv

  desc "AI sommelier for your wine cellar — ETL, DuckDB queries, MCP server"
  homepage "https://github.com/urban-buss/cellarbrain"
  url "https://files.pythonhosted.org/packages/source/c/cellarbrain/cellarbrain-0.3.0.tar.gz"
  sha256 "REPLACE_WITH_ACTUAL_SHA256"
  license "MIT"

  depends_on "python@3.13"

  # Core dependencies
  resource "pyarrow" do
    url "https://files.pythonhosted.org/packages/..."
    sha256 "..."
  end

  resource "duckdb" do
    url "https://files.pythonhosted.org/packages/..."
    sha256 "..."
  end

  resource "pandas" do
    url "https://files.pythonhosted.org/packages/..."
    sha256 "..."
  end

  resource "tabulate" do
    url "https://files.pythonhosted.org/packages/..."
    sha256 "..."
  end

  # mcp[cli] and its dependencies
  resource "mcp" do
    url "https://files.pythonhosted.org/packages/..."
    sha256 "..."
  end

  def install
    virtualenv_install_with_resources
  end

  test do
    assert_match "cellarbrain", shell_output("#{bin}/cellarbrain --help")
  end
end
```

### 4.3 Generate Resource Stanzas

Use `homebrew-pypi-poet` to automatically generate the `resource` blocks:

```bash
pip install homebrew-pypi-poet
poet cellarbrain
```

This outputs all the `resource` stanzas needed for cellarbrain's dependencies.

### 4.4 Get the SHA256

```bash
# Download the sdist from PyPI and compute the hash
curl -sL https://files.pythonhosted.org/packages/source/c/cellarbrain/cellarbrain-0.3.0.tar.gz | shasum -a 256
```

### 4.5 Test the Formula Locally

```bash
# From the homebrew-cellarbrain repo:
brew install --build-from-source ./Formula/cellarbrain.rb

# Verify
cellarbrain --help
cellarbrain stats  # (if output/ exists)

# Uninstall test build
brew uninstall cellarbrain
```

### 4.6 Publish the Tap

```bash
cd homebrew-cellarbrain
git add Formula/cellarbrain.rb
git commit -m "cellarbrain 0.3.0"
git push origin main
```

### 4.7 Install from the Tap

Users can now install via:

```bash
brew tap urban-buss/cellarbrain
brew install cellarbrain
```

### 4.8 Updating the Formula for New Releases

When a new version is published to PyPI:

```bash
cd homebrew-cellarbrain

# 1. Update the URL and version in Formula/cellarbrain.rb
# 2. Update the sha256 hash
# 3. Re-run poet to update resource stanzas if dependencies changed

# Get new hash:
curl -sL https://files.pythonhosted.org/packages/source/c/cellarbrain/cellarbrain-0.3.0.tar.gz | shasum -a 256

# Commit and push
git add Formula/cellarbrain.rb
git commit -m "cellarbrain 0.3.1"
git push origin main
```

Users update with:

```bash
brew update
brew upgrade cellarbrain
```

### 4.9 Homebrew with Optional Extras

The base formula installs core dependencies only. For optional extras (sommelier, dashboard, ingest), users should use pip inside the Homebrew-managed virtualenv:

```bash
# Find the Homebrew venv site-packages
CELLARBRAIN_VENV=$(brew --prefix cellarbrain)/libexec

# Install extras into the Homebrew venv
$CELLARBRAIN_VENV/bin/pip install "cellarbrain[sommelier]"
$CELLARBRAIN_VENV/bin/pip install "cellarbrain[dashboard]"
$CELLARBRAIN_VENV/bin/pip install "cellarbrain[ingest]"
```

Alternatively, create a separate formula for the full installation:

```ruby
class CellarbrainFull < Formula
  # ... same as above but with all extras included as resources
end
```

---

## 5. Pre-Release Testing

Before any release, run:

```bash
# 1. Full test suite
pytest

# 2. Build and verify package
python -m build
twine check dist/*

# 3. Test install in clean venv
python3 -m venv /tmp/release-test
source /tmp/release-test/bin/activate
pip install dist/cellarbrain-*.whl
cellarbrain --help
cellarbrain stats  # verify with real data if available
deactivate
rm -rf /tmp/release-test

# 4. Smoke test (optional but recommended)
# @cellarbrain-smoketest smoke test ETL
```

---

## 6. Rollback

### PyPI

PyPI does not allow re-uploading the same version. If a release has critical bugs:

1. Publish a patch release (e.g., `0.3.1`)
2. Yank the broken version on PyPI (Settings → "Yank this release")

### Homebrew

```bash
cd homebrew-cellarbrain
git revert HEAD  # revert the formula update
git push origin main
```

Users:
```bash
brew update
brew reinstall cellarbrain
```

---

## Next Steps

- [Installation & Running](04-installation-and-running.md) — Install and run on a fresh Mac
- [Debugging & Monitoring](05-debugging-and-monitoring.md) — Diagnose issues in production
