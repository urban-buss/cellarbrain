# PyPI Publishing

Automated and manual publishing to PyPI, including Trusted Publishing setup.

## Automated Publishing (Recommended)

The repository includes `.github/workflows/publish.yml`:

1. Push a tag matching `v*` (e.g., `v0.3.0`)
2. GitHub Actions builds sdist + wheel
3. Uses [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC) to upload — no API tokens needed

### One-Time Setup

#### GitHub Environment

1. Repository → **Settings → Environments** → **New environment**
2. Name it exactly: **`pypi`** (must match the workflow file)
3. Optional: add protection rules (require manual approval before publishing)

#### PyPI Trusted Publisher

**If the package doesn't exist on PyPI yet** (Pending Publisher):

1. [pypi.org](https://pypi.org/manage/account/) → **Publishing**
2. Under "Add a new pending publisher", fill in:

   | Field | Value |
   |-------|-------|
   | PyPI project name | `cellarbrain` |
   | Owner | your GitHub username or org |
   | Repository name | `cellarbrain` |
   | Workflow name | `publish.yml` |
   | Environment name | `pypi` |

3. Click **Add**

**If the package already exists on PyPI:**

1. `https://pypi.org/manage/project/cellarbrain/settings/publishing/`
2. Fill in the same fields under "Add a new publisher"

### Verification Checklist

| Check | Where |
|-------|-------|
| Environment `pypi` exists | GitHub → Settings → Environments |
| Workflow has `environment: pypi` | `.github/workflows/publish.yml` |
| Workflow has `permissions: id-token: write` | `.github/workflows/publish.yml` |
| Publisher configured on PyPI | PyPI → Project → Settings → Publishing |
| Owner/repo/workflow/environment match exactly | Both sides |

### Dry Run

```bash
git tag v0.0.1-rc1
git push origin v0.0.1-rc1
# Watch Actions tab for build + publish jobs
```

## Manual Publishing

For emergency releases:

```bash
pip install build twine
python -m build
twine check dist/*

# Optional: Test PyPI first
twine upload --repository testpypi dist/*

# Production
twine upload dist/*
```

Prompted for PyPI API token (create at pypi.org → Account settings → API tokens).

## Verify Installation

```bash
python3 -m venv /tmp/pypi-test
source /tmp/pypi-test/bin/activate
pip install cellarbrain
cellarbrain --help
deactivate
rm -rf /tmp/pypi-test
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Environment 'pypi' not found` | Environment doesn't exist | Create it in GitHub Settings |
| `id-token permission not available` | Missing `permissions` block | Add `id-token: write` to workflow |
| `Token request failed` | Publisher not configured on PyPI | Add trusted publisher |
| `403 … not allowed to upload` | Owner/repo/workflow mismatch | Check fields match exactly |
| `The name 'cellarbrain' is already taken` | Someone else owns the name | Use pending publisher before first upload |
| Package is empty after publish | Build not finding source | Check `[tool.setuptools.packages.find]` in pyproject.toml |

## Next Steps

- [Release Process](release-process.md) — Version and tag workflow
- [Homebrew](homebrew.md) — Homebrew tap and formula
