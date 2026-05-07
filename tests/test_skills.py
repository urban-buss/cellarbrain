"""Tests for the bundled skills sub-package and install command."""

from __future__ import annotations

import pathlib

import pytest

from cellarbrain.skills import SKILL_NAMES, SKILLS_DIR, install_skills


class TestSkillsPackageData:
    """Verify bundled skill files are present."""

    def test_readme_exists(self) -> None:
        assert (SKILLS_DIR / "README.md").is_file()

    @pytest.mark.parametrize("skill_name", SKILL_NAMES)
    def test_skill_file_exists(self, skill_name: str) -> None:
        assert (SKILLS_DIR / skill_name / "SKILL.md").is_file()

    def test_skill_names_non_empty(self) -> None:
        assert len(SKILL_NAMES) >= 8


class TestInstallSkills:
    """Verify install_skills() copies files correctly."""

    def test_install_copies_all_skills(self, tmp_path: pathlib.Path) -> None:
        installed = install_skills(tmp_path)
        assert set(installed) == set(SKILL_NAMES)
        for name in SKILL_NAMES:
            assert (tmp_path / name / "SKILL.md").is_file()
        assert (tmp_path / "README.md").is_file()

    def test_install_idempotent_no_overwrite(self, tmp_path: pathlib.Path) -> None:
        install_skills(tmp_path)
        # Second call should skip all (already present)
        installed = install_skills(tmp_path)
        assert installed == []

    def test_install_force_overwrites(self, tmp_path: pathlib.Path) -> None:
        install_skills(tmp_path)
        # Modify a file
        marker = tmp_path / SKILL_NAMES[0] / "SKILL.md"
        marker.write_text("modified")
        # Force should overwrite
        installed = install_skills(tmp_path, force=True)
        assert SKILL_NAMES[0] in installed
        assert marker.read_text() != "modified"

    def test_install_creates_nested_directories(self, tmp_path: pathlib.Path) -> None:
        deep = tmp_path / "a" / "b" / "c"
        installed = install_skills(deep)
        assert deep.is_dir()
        assert len(installed) == len(SKILL_NAMES)


class TestSyncWithOpenclaw:
    """Ensure src/cellarbrain/skills/ stays in sync with .openclaw/."""

    def test_skills_match_openclaw(self) -> None:
        # SKILLS_DIR is src/cellarbrain/skills/ → 3 parents up = repo root
        repo_root = SKILLS_DIR.parent.parent.parent
        openclaw_dir = repo_root / ".openclaw"
        if not openclaw_dir.is_dir():
            pytest.skip(".openclaw/ directory not found (running from installed package)")

        for skill_name in SKILL_NAMES:
            src = openclaw_dir / skill_name / "SKILL.md"
            dst = SKILLS_DIR / skill_name / "SKILL.md"
            assert src.is_file(), f".openclaw/{skill_name}/SKILL.md missing"
            assert dst.is_file(), f"skills/{skill_name}/SKILL.md missing"
            assert src.read_text() == dst.read_text(), (
                f"skills/{skill_name}/SKILL.md out of sync with .openclaw/ — run: python scripts/sync-skills.py"
            )
