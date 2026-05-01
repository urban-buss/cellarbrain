# PyPI Trusted Publishing Setup

Detailed steps to configure the GitHub environment and PyPI trusted publisher
required by the automated publish workflow (`.github/workflows/publish.yml`).

Both pieces must be in place before your first `v*` tag push will successfully
publish to PyPI.

---

## Prerequisites

- You are an **Owner** (or have Admin access) on the GitHub repository.
- You have a [PyPI account](https://pypi.org/account/register/) with access to
  the `cellarbrain` project (or will create it as a new project).

---

## Part 1 — GitHub Environment

The publish workflow declares `environment: pypi`. GitHub Actions will refuse to
run the job unless a matching environment exists.

### Step 1 — Create the Environment

1. Open the repository on GitHub.
2. Go to **Settings → Environments** (left sidebar, under *Code and automation*).
3. Click **New environment**.
4. Name it exactly: **`pypi`** (lowercase, must match the workflow file).
5. Click **Configure environment**.

### Step 2 — (Optional) Add Protection Rules

For extra safety you can require manual approval before publishing:

| Setting | Recommended Value |
|---------|-------------------|
| Required reviewers | 1 (yourself) — you'll click "Approve" before each publish |
| Wait timer | 0 minutes |
| Deployment branches | **Selected branches** → add `main` (or leave "All branches" if tags are your gate) |

> **Note:** Protection rules are optional. Without them the workflow runs
> automatically on every `v*` tag push — which is fine for a personal project.

### Step 3 — Verify

- The environment now appears under **Settings → Environments** with a green
  checkmark.
- No secrets need to be added — trusted publishing uses OIDC tokens, not API
  keys.

---

## Part 2 — PyPI Trusted Publisher

PyPI's "Trusted Publishers" feature lets GitHub Actions authenticate via OIDC
without storing any API token. You register the GitHub workflow as an authorised
publisher for the package.

### Option A — First-time Publish (Package Does Not Exist on PyPI Yet)

Use a **Pending Publisher** so PyPI will accept the very first upload.

1. Log in to [pypi.org](https://pypi.org/manage/account/).
2. Go to **Your account → Publishing** (or direct link:
   `https://pypi.org/manage/account/publishing/`).
3. Under **"Add a new pending publisher"**, fill in:

   | Field | Value |
   |-------|-------|
   | PyPI project name | `cellarbrain` |
   | Owner | your GitHub username or org (e.g. `urbanbusslinger`) |
   | Repository name | `cellarbrain` |
   | Workflow name | `publish.yml` |
   | Environment name | `pypi` |

4. Click **Add**.

Once the first successful publish completes, PyPI automatically converts the
pending publisher into a full trusted publisher.

### Option B — Package Already Exists on PyPI

If `cellarbrain` is already published and you're adding trusted publishing
after-the-fact:

1. Go to the project page: `https://pypi.org/manage/project/cellarbrain/settings/publishing/`.
2. Under **"Add a new publisher"**, fill in:

   | Field | Value |
   |-------|-------|
   | Owner | your GitHub username or org |
   | Repository name | `cellarbrain` |
   | Workflow name | `publish.yml` |
   | Environment name | `pypi` |

3. Click **Add**.

---

## Verification Checklist

Run through this list before your first real release:

### 1. GitHub Environment Exists

```
Repository → Settings → Environments → "pypi" is listed
```

### 2. Workflow References Match

Open `.github/workflows/publish.yml` and confirm:

```yaml
publish:
  needs: build
  runs-on: ubuntu-latest
  environment: pypi          # ← must match the environment name above
  permissions:
    id-token: write          # ← required for OIDC token exchange
```

### 3. PyPI Publisher Configured

On PyPI (Account → Publishing, or Project → Settings → Publishing):

- **Owner** matches your GitHub username/org exactly (case-sensitive).
- **Repository** is `cellarbrain`.
- **Workflow** is `publish.yml` (just the filename, no path).
- **Environment** is `pypi`.

### 4. Dry-Run Test

Push a tag to trigger the workflow and verify end-to-end:

```bash
# Use TestPyPI first (optional but recommended)
# Change the workflow to point at test.pypi.org, or push a pre-release tag:
git tag v0.0.1-rc1
git push origin v0.0.1-rc1
```

Watch the Actions tab:
- **build** job should produce `dist/` artifacts.
- **publish** job should show "Waiting for deployment approval" (if you added
  reviewers) or proceed directly to upload.

If the publish job fails with `"Token request failed"` or
`"The trusted publisher … is not configured"`:
- Double-check that the Owner/Repository/Workflow/Environment fields on PyPI
  match exactly.
- Confirm the GitHub environment is named `pypi` (not `PyPI` or `production`).

### 5. Confirm on PyPI

After a successful publish, verify:

```
https://pypi.org/project/cellarbrain/
```

The new version should appear within a few minutes.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `Error: Environment 'pypi' not found` | Environment doesn't exist | Create it (Part 1, Step 1) |
| `Error: id-token permission not available` | Workflow missing `permissions` block | Ensure `id-token: write` is set |
| `Token request failed` on publish | Trusted publisher not configured on PyPI | Add it (Part 2) |
| `403 … is not allowed to upload to project` | Owner/repo/workflow mismatch | Check field values exactly match |
| `The name 'cellarbrain' is already taken` | Someone else owns the name | Use pending publisher before first upload, or rename |
| Publish succeeds but package is empty | `python -m build` not finding source | Verify `[tool.setuptools.packages.find]` in pyproject.toml |

---

## References

- [PyPI Trusted Publishers documentation](https://docs.pypi.org/trusted-publishers/)
- [GitHub: Using environments for deployment](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment)
- [pypa/gh-action-pypi-publish](https://github.com/pypa/gh-action-pypi-publish)
