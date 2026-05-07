#!/usr/bin/env python3
"""Sync .openclaw/ skill files into src/cellarbrain/skills/ (one-way).

Run before ``python -m build`` to ensure the PyPI package includes the
latest skill definitions. This script is idempotent and cross-platform.

Usage:
    python .github/tools/sync-skills.py
"""

from __future__ import annotations

import pathlib
import shutil
import sys

# Script lives at .github/tools/ — three levels up to repo root.
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SRC_DIR = REPO_ROOT / ".openclaw"
DST_DIR = REPO_ROOT / "src" / "cellarbrain" / "skills"

SKIP = {"_archive", "__pycache__"}


def sync() -> None:
    if not SRC_DIR.is_dir():
        print(f"ERROR: source directory not found: {SRC_DIR}", file=sys.stderr)
        sys.exit(1)

    DST_DIR.mkdir(parents=True, exist_ok=True)

    # Copy top-level README.md
    readme = SRC_DIR / "README.md"
    if readme.is_file():
        shutil.copy2(readme, DST_DIR / "README.md")

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
        dst_skill_dir = DST_DIR / child.name
        dst_skill_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(skill_file, dst_skill_dir / "SKILL.md")
        synced.append(child.name)

    print(f"Synced {len(synced)} skills from .openclaw/ → src/cellarbrain/skills/")
    for name in synced:
        print(f"  {name}")


if __name__ == "__main__":
    sync()
