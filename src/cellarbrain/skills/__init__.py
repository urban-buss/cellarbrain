"""Bundled OpenClaw skill files for cellarbrain.

Skills are shipped as package data so that ``pip install cellarbrain``
includes them. Use :func:`install_skills` (or the ``cellarbrain
install-skills`` CLI command) to copy them into a target Open Claw
skills directory.
"""

from __future__ import annotations

import pathlib
import shutil

SKILLS_DIR = pathlib.Path(__file__).resolve().parent

SKILL_NAMES: list[str] = [
    "tonight",
    "food-pairing",
    "wine-info",
    "cellar-stats",
    "research",
    "drinking-window",
    "price-scan",
    "manage",
]


def install_skills(target_dir: pathlib.Path, *, force: bool = False) -> list[str]:
    """Copy bundled skills to the target Open Claw directory.

    Args:
        target_dir: Destination directory (e.g. ``~/.openclaw/skills/cellarbrain/``).
        force: Overwrite existing files when True.

    Returns:
        List of skill names that were copied. Excludes skills already present
        when *force* is False.
    """
    target_dir = pathlib.Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    # Copy README
    readme_src = SKILLS_DIR / "README.md"
    readme_dst = target_dir / "README.md"
    if readme_src.is_file() and (force or not readme_dst.exists()):
        shutil.copy2(readme_src, readme_dst)

    installed: list[str] = []
    for skill_name in SKILL_NAMES:
        src = SKILLS_DIR / skill_name / "SKILL.md"
        if not src.is_file():
            continue
        dst_dir = target_dir / skill_name
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "SKILL.md"
        if force or not dst.exists():
            shutil.copy2(src, dst)
            installed.append(skill_name)

    return installed
