"""Integration tests verifying a fresh install works without errors.

These tests run cellarbrain CLI commands in a subprocess to validate
that the package imports cleanly and basic commands work without
requiring any data files.

Skipped if cellarbrain is not installed (e.g. in CI without editable install).
"""

from __future__ import annotations

import subprocess
import sys

import pytest

_PYTHON = sys.executable


def _run_cellarbrain(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [_PYTHON, "-m", "cellarbrain", *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.skipif(
    subprocess.run(
        [sys.executable, "-c", "import cellarbrain"],
        capture_output=True,
    ).returncode
    != 0,
    reason="cellarbrain not installed",
)
class TestFreshInstall:
    def test_version_flag(self):
        result = _run_cellarbrain("--version")
        assert result.returncode == 0
        assert "cellarbrain" in result.stdout

    def test_help_flag(self):
        result = _run_cellarbrain("--help")
        assert result.returncode == 0
        assert "etl" in result.stdout
        assert "mcp" in result.stdout

    def test_etl_help(self):
        result = _run_cellarbrain("etl", "--help")
        assert result.returncode == 0
        assert "--no-migrate" in result.stdout

    def test_import_no_errors(self):
        """All core modules import without errors."""
        result = subprocess.run(
            [
                _PYTHON,
                "-c",
                "import cellarbrain.cli; "
                "import cellarbrain.mcp_server; "
                "import cellarbrain.query; "
                "import cellarbrain.search; "
                "import cellarbrain.settings; "
                "import cellarbrain.sommelier.seed",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"

    def test_sommelier_seed_accessible(self):
        """Bundled sommelier seed files are accessible from installed package."""
        result = subprocess.run(
            [
                _PYTHON,
                "-c",
                "from cellarbrain.sommelier.seed import bundled_food_catalogue; "
                "p = bundled_food_catalogue(); "
                "assert p.exists(), f'Not found: {p}'",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"Seed check failed: {result.stderr}"
