"""Bundled OpenClaw skill files for cellarbrain.

Skills are shipped as package data so that ``pip install cellarbrain``
includes them. Use :func:`install_skills` (or the ``cellarbrain
install-skills`` CLI command) to copy them into a target Open Claw
skills directory. Use :func:`uninstall_skills` to remove them.
"""

from __future__ import annotations

import pathlib
import shutil

SKILLS_DIR = pathlib.Path(__file__).resolve().parent

SKILL_NAMES: list[str] = [
    "cellar-gaps",
    "cellar-stats",
    "consumption-velocity",
    "digest",
    "dinner-party",
    "drinking-window",
    "food-pairing",
    "hilfe",
    "manage",
    "monthly-report",
    "price-scan",
    "promotion-analysis",
    "research",
    "research-completeness",
    "tonight",
    "wine-info",
    "wine-of-the-day",
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


def uninstall_skills(target_dir: pathlib.Path) -> list[str]:
    """Remove installed cellarbrain skills from the target directory.

    Args:
        target_dir: Directory where skills were installed
            (e.g. ``~/.openclaw/skills/cellarbrain/``).

    Returns:
        List of skill names that were removed.
    """
    target_dir = pathlib.Path(target_dir)
    if not target_dir.is_dir():
        return []
    removed: list[str] = []
    for skill_name in SKILL_NAMES:
        skill_dir = target_dir / skill_name
        if skill_dir.is_dir():
            shutil.rmtree(skill_dir)
            removed.append(skill_name)
    readme = target_dir / "README.md"
    if readme.is_file():
        readme.unlink()
    # Remove the cellarbrain dir itself if empty
    if target_dir.is_dir() and not any(target_dir.iterdir()):
        target_dir.rmdir()
    return removed
