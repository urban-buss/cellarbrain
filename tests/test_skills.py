"""Tests for the bundled skills sub-package and install command."""

from __future__ import annotations

import ast
import pathlib
import re

import pytest
import yaml

from cellarbrain.skills import SKILL_NAMES, SKILLS_DIR, install_skills, uninstall_skills


class TestSkillsPackageData:
    """Verify bundled skill files are present."""

    def test_readme_exists(self) -> None:
        assert (SKILLS_DIR / "README.md").is_file()

    @pytest.mark.parametrize("skill_name", SKILL_NAMES)
    def test_skill_file_exists(self, skill_name: str) -> None:
        assert (SKILLS_DIR / skill_name / "SKILL.md").is_file()

    def test_skill_names_non_empty(self) -> None:
        assert len(SKILL_NAMES) == 17

    def test_inventory_matches_packaged_directories(self) -> None:
        directories = {p.parent.name for p in SKILLS_DIR.glob("*/SKILL.md")}
        assert directories == set(SKILL_NAMES)

    @pytest.mark.parametrize("skill_name", SKILL_NAMES)
    def test_skill_frontmatter_and_encoding(self, skill_name: str) -> None:
        raw = (SKILLS_DIR / skill_name / "SKILL.md").read_bytes()
        assert not raw.startswith(b"\xef\xbb\xbf"), f"{skill_name} has a UTF-8 BOM"
        text = raw.decode("utf-8")
        assert text.startswith("---\n")
        _, frontmatter, _ = text.split("---", 2)
        metadata = yaml.safe_load(frontmatter)
        assert metadata["name"] == skill_name
        assert metadata["description"]

    def test_skill_tool_references_match_mcp_surface(self) -> None:
        server_path = SKILLS_DIR.parent / "mcp_server.py"
        tree = ast.parse(server_path.read_text(encoding="utf-8"))
        tools: dict[str, set[str]] = {}
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if not any(
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "_tool"
                for decorator in node.decorator_list
            ):
                continue
            tools[node.name] = {arg.arg for arg in node.args.args + node.args.kwonlyargs}

        inline_call = re.compile(r"`([a-z][a-z0-9_]*)\(([^`\n]*)\)`")
        named_argument = re.compile(r"\b([a-z][a-z0-9_]*)\s*=")
        for skill_name in SKILL_NAMES:
            text = (SKILLS_DIR / skill_name / "SKILL.md").read_text(encoding="utf-8")
            for tool_name, arguments in inline_call.findall(text):
                assert tool_name in tools, f"{skill_name} references unknown MCP tool {tool_name}"
                supplied = set(named_argument.findall(arguments))
                assert supplied <= tools[tool_name], (
                    f"{skill_name} passes unsupported arguments to {tool_name}: "
                    f"{sorted(supplied - tools[tool_name])}"
                )

    def test_skills_do_not_contain_known_stale_api_examples(self) -> None:
        combined = "\n".join(
            (SKILLS_DIR / name / "SKILL.md").read_text(encoding="utf-8") for name in SKILL_NAMES
        )
        stale_fragments = (
            'Always pass `format="plain"` to every tool call',
            "pair_wine(wine_id",
            "purchase_price_chf",
            "date_added",
            "latest_vintage",
            "WHERE is_active",
            "ingest_status()",
            "ingest_start()",
            "ingest_stop(",
        )
        for fragment in stale_fragments:
            assert fragment not in combined
        assert re.search(r"(?m)^`sql$", combined) is None


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


class TestUninstallSkills:
    """Verify uninstall_skills() removes files correctly."""

    def test_uninstall_removes_all_skills(self, tmp_path: pathlib.Path) -> None:
        install_skills(tmp_path)
        removed = uninstall_skills(tmp_path)
        assert set(removed) == set(SKILL_NAMES)
        for name in SKILL_NAMES:
            assert not (tmp_path / name).exists()
        assert not (tmp_path / "README.md").exists()

    def test_uninstall_removes_empty_parent(self, tmp_path: pathlib.Path) -> None:
        install_skills(tmp_path)
        uninstall_skills(tmp_path)
        assert not tmp_path.exists()

    def test_uninstall_preserves_extra_files(self, tmp_path: pathlib.Path) -> None:
        install_skills(tmp_path)
        (tmp_path / "custom.txt").write_text("keep")
        uninstall_skills(tmp_path)
        # Parent still exists because of the extra file
        assert tmp_path.is_dir()
        assert (tmp_path / "custom.txt").is_file()

    def test_uninstall_empty_dir(self, tmp_path: pathlib.Path) -> None:
        removed = uninstall_skills(tmp_path)
        assert removed == []

    def test_uninstall_nonexistent_dir(self, tmp_path: pathlib.Path) -> None:
        removed = uninstall_skills(tmp_path / "nonexistent")
        assert removed == []


class TestSyncWithOpenclaw:
    """Ensure src/cellarbrain/skills/ stays in sync with .openclaw/."""

    def test_skills_match_openclaw(self) -> None:
        # SKILLS_DIR is src/cellarbrain/skills/ → 3 parents up = repo root
        repo_root = SKILLS_DIR.parent.parent.parent
        openclaw_dir = repo_root / ".openclaw"
        if not openclaw_dir.is_dir():
            pytest.skip(".openclaw/ directory not found (running from installed package)")

        source_names = {
            path.parent.name
            for path in openclaw_dir.glob("*/SKILL.md")
            if path.parent.name != "_archive"
        }
        assert source_names == set(SKILL_NAMES)

        for skill_name in SKILL_NAMES:
            src = openclaw_dir / skill_name / "SKILL.md"
            dst = SKILLS_DIR / skill_name / "SKILL.md"
            assert src.is_file(), f".openclaw/{skill_name}/SKILL.md missing"
            assert dst.is_file(), f"skills/{skill_name}/SKILL.md missing"
            assert src.read_text() == dst.read_text(), (
                f"skills/{skill_name}/SKILL.md out of sync with .openclaw/ — run: python .github/tools/sync-skills.py"
            )
