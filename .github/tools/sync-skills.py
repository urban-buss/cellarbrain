#!/usr/bin/env python3
# GENERATED FROM .github/_shared/tools/sync-skills.py
# DO NOT EDIT — run `python .github/tools/sync-shared-meta.py` at the parent root.

"""Sync .openclaw/ skill files into src/<package>/skills/ (one-way).

The destination package name is read from `pyproject.toml`
(`[project] name`) so the same script works in any brain.

Run before ``python -m build`` to ensure the PyPI package includes the
latest skill definitions. Idempotent and cross-platform.

Usage:
    python .github/tools/sync-skills.py
"""

from __future__ import annotations

import pathlib
import shutil
import sys
import tomllib

# Script lives at .github/tools/ — three levels up to repo root.
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SRC_DIR = REPO_ROOT / ".openclaw"
SKIP = {"_archive", "__pycache__"}


def _package_name() -> str:
    pyproject = REPO_ROOT / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    name = data.get("project", {}).get("name")
    if not name:
        raise SystemExit(f"ERROR: no [project] name in {pyproject}")
    return name


def sync() -> None:
    if not SRC_DIR.is_dir():
        print(f"ERROR: source directory not found: {SRC_DIR}", file=sys.stderr)
        sys.exit(1)

    package = _package_name()
    dst_dir = REPO_ROOT / "src" / package / "skills"
    dst_dir.mkdir(parents=True, exist_ok=True)

    # Copy top-level README.md
    readme = SRC_DIR / "README.md"
    if readme.is_file():
        shutil.copy2(readme, dst_dir / "README.md")

    # Copy skill folders (only SKILL.md files)
    synced: list[str] = []
    for child in sorted(SRC_DIR.iterdir()):
        if not child.is_dir():
            continue
        if child.name in SKIP or child.name.startswith("."):
            continue
        skill_file = child / "SKILL.md"
        if not skill_file.is_file():
            continue
        dst_skill_dir = dst_dir / child.name
        dst_skill_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(skill_file, dst_skill_dir / "SKILL.md")
        synced.append(child.name)

    print(f"Synced {len(synced)} skills from .openclaw/ -> src/{package}/skills/")
    for name in synced:
        print(f"  - {name}")


if __name__ == "__main__":
    sync()
