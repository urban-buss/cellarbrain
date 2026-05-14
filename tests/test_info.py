"""Tests for the info module."""

from __future__ import annotations

import json
import pathlib
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from cellarbrain.info import (
    ExtraStatus,
    InfoReport,
    _build_mcp_config_snippet,
    _check_extras,
    _config_resolution_label,
    _count_parquet_files,
    _detect_install_type,
    _get_data_summary,
    _get_skills_status,
    _get_version,
    collect_info,
    format_json,
    format_mcp_config,
    format_text,
)
from cellarbrain.settings import (
    BackupConfig,
    DashboardConfig,
    PathsConfig,
    Settings,
    SommelierConfig,
)
from cellarbrain.skills import SKILL_NAMES
from cellarbrain.writer import SCHEMAS


def _minimal_settings(tmp_path) -> Settings:
    data_dir = tmp_path / "output"
    data_dir.mkdir()
    return Settings(
        paths=PathsConfig(data_dir=str(data_dir)),
        sommelier=SommelierConfig(model_dir=str(tmp_path / "models")),
        backup=BackupConfig(backup_dir=str(tmp_path / "bkp")),
        dashboard=DashboardConfig(port=8017),
        config_source=str(tmp_path / "cellarbrain.toml"),
    )


# ---------------------------------------------------------------------------
# TestGetVersion
# ---------------------------------------------------------------------------


class TestGetVersion:
    def test_known_package(self):
        v = _get_version("pyarrow")
        assert v != "?"
        assert "." in v

    def test_unknown_package(self):
        assert _get_version("nonexistent-package-xyz") == "?"


# ---------------------------------------------------------------------------
# TestDetectInstallType
# ---------------------------------------------------------------------------


class TestDetectInstallType:
    def test_returns_tuple(self):
        install_type, location = _detect_install_type()
        assert install_type in ("editable", "regular", "unknown")
        assert isinstance(location, str)

    def test_missing_package(self):
        with patch("cellarbrain.info.importlib.metadata.distribution", side_effect=Exception):
            # Falls through to unknown
            pass


# ---------------------------------------------------------------------------
# TestCheckExtras
# ---------------------------------------------------------------------------


class TestCheckExtras:
    def test_returns_all_groups(self):
        result = _check_extras()
        assert "ml" in result
        for status in result.values():
            assert isinstance(status, ExtraStatus)

    def test_installed_extra_has_packages(self):
        result = _check_extras()
        # Check structure — if ml is installed it should have packages
        for status in result.values():
            if status.installed:
                assert len(status.packages) > 0


# ---------------------------------------------------------------------------
# TestCountParquetFiles
# ---------------------------------------------------------------------------


class TestCountParquetFiles:
    def test_empty_dir(self, tmp_path):
        assert _count_parquet_files(tmp_path) == 0

    def test_nonexistent_dir(self, tmp_path):
        assert _count_parquet_files(tmp_path / "nope") == 0

    def test_counts_parquet_only(self, tmp_path):
        (tmp_path / "a.parquet").write_text("")
        (tmp_path / "b.parquet").write_text("")
        (tmp_path / "c.txt").write_text("")
        assert _count_parquet_files(tmp_path) == 2


# ---------------------------------------------------------------------------
# TestGetDataSummary
# ---------------------------------------------------------------------------


class TestGetDataSummary:
    def test_no_data(self, tmp_path):
        assert _get_data_summary(tmp_path) is None

    def test_with_data(self, tmp_path):
        # Write minimal wine.parquet
        schema = SCHEMAS["wine"]
        arrays = [pa.array([], type=f.type) for f in schema]
        table = pa.table(arrays, schema=schema)
        pq.write_table(table, tmp_path / "wine.parquet")

        result = _get_data_summary(tmp_path)
        assert result is not None
        assert result.wines == 0
        assert result.bottles_stored == 0


# ---------------------------------------------------------------------------
# TestGetSkillsStatus
# ---------------------------------------------------------------------------


class TestGetSkillsStatus:
    def test_no_dir(self, tmp_path):
        assert _get_skills_status(tmp_path / "nope") == 0

    def test_with_skills(self, tmp_path):
        for name in SKILL_NAMES[:3]:
            skill_dir = tmp_path / name
            skill_dir.mkdir()
            (skill_dir / "SKILL.md").write_text("test")
        assert _get_skills_status(tmp_path) == 3


# ---------------------------------------------------------------------------
# TestBuildMcpConfigSnippet
# ---------------------------------------------------------------------------


class TestBuildMcpConfigSnippet:
    def test_with_config_source(self, tmp_path):
        settings = _minimal_settings(tmp_path)
        snippet = _build_mcp_config_snippet(settings, "/usr/bin/cellarbrain")
        assert "mcpServers" in snippet
        server = snippet["mcpServers"]["cellarbrain"]
        assert server["command"] == "/usr/bin/cellarbrain"
        assert "-d" in server["args"]
        assert "-c" in server["args"]
        assert "mcp" in server["args"]
        # -d value must be an absolute path
        d_idx = server["args"].index("-d")
        assert pathlib.Path(server["args"][d_idx + 1]).is_absolute()

    def test_without_config_source(self, tmp_path):
        settings = Settings(
            paths=PathsConfig(data_dir=str(tmp_path)),
            config_source=None,
        )
        snippet = _build_mcp_config_snippet(settings, None)
        server = snippet["mcpServers"]["cellarbrain"]
        assert server["command"] == "cellarbrain"
        assert "-d" in server["args"]
        assert "mcp" in server["args"]
        # Should include absolute data_dir then mcp
        d_idx = server["args"].index("-d")
        assert server["args"][d_idx + 1] == str(tmp_path.resolve())
        assert server["args"][-1] == "mcp"


# ---------------------------------------------------------------------------
# TestConfigResolutionLabel
# ---------------------------------------------------------------------------


class TestConfigResolutionLabel:
    def test_no_config(self, tmp_path):
        settings = Settings(paths=PathsConfig(data_dir=str(tmp_path)), config_source=None)
        assert "defaults" in _config_resolution_label(settings)

    def test_toml_fallback(self, tmp_path):
        settings = Settings(
            paths=PathsConfig(data_dir=str(tmp_path)),
            config_source=str(tmp_path / "cellarbrain.toml"),
        )
        label = _config_resolution_label(settings)
        assert "cellarbrain.toml" in label

    def test_local_override(self, tmp_path):
        settings = Settings(
            paths=PathsConfig(data_dir=str(tmp_path)),
            config_source=str(tmp_path / "cellarbrain.local.toml"),
        )
        label = _config_resolution_label(settings)
        assert "local" in label


# ---------------------------------------------------------------------------
# TestCollectInfo
# ---------------------------------------------------------------------------


class TestCollectInfo:
    def test_returns_info_report(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        settings = _minimal_settings(tmp_path)
        report = collect_info(settings)
        assert isinstance(report, InfoReport)
        assert report.version != "?"
        assert report.python_version
        assert report.platform_system
        assert report.mcp_tools == 28
        assert report.mcp_resources == 10
        assert report.skills_bundled == len(SKILL_NAMES)

    def test_env_vars_captured(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CELLARBRAIN_CONFIG", "/test/config.toml")
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", "/test/data")
        settings = _minimal_settings(tmp_path)
        report = collect_info(settings)
        assert report.env_config == "/test/config.toml"
        assert report.env_data_dir == "/test/data"


# ---------------------------------------------------------------------------
# TestFormatText
# ---------------------------------------------------------------------------


class TestFormatText:
    def test_contains_sections(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        settings = _minimal_settings(tmp_path)
        report = collect_info(settings)
        text = format_text(report)
        assert "Cellarbrain Info" in text
        assert "Config" in text
        assert "Paths" in text
        assert "Modules" in text
        assert "MCP Server" in text
        assert "Skills" in text
        assert "Version:" in text

    def test_mcp_snippet_in_output(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        settings = _minimal_settings(tmp_path)
        report = collect_info(settings)
        text = format_text(report)
        assert "mcpServers" in text


# ---------------------------------------------------------------------------
# TestFormatJson
# ---------------------------------------------------------------------------


class TestFormatJson:
    def test_valid_json(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        settings = _minimal_settings(tmp_path)
        report = collect_info(settings)
        text = format_json(report)
        data = json.loads(text)
        assert data["version"] == report.version
        assert "python_version" in data
        assert "core_packages" in data
        assert "extras" in data
        assert "mcp_config_snippet" in data

    def test_contains_all_keys(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        settings = _minimal_settings(tmp_path)
        report = collect_info(settings)
        data = json.loads(format_json(report))
        expected_keys = {
            "version",
            "python_version",
            "python_impl",
            "python_executable",
            "platform_system",
            "platform_machine",
            "install_type",
            "install_location",
            "entry_point",
            "config_file",
            "config_resolution",
            "env_config",
            "env_data_dir",
            "data_dir",
            "raw_dir",
            "wines_dir",
            "backup_dir",
            "log_db",
            "sommelier_model_dir",
            "parquet_file_count",
            "core_packages",
            "extras",
            "mcp_tools",
            "mcp_resources",
            "mcp_default_port",
            "dashboard_port",
            "mcp_config_snippet",
            "skills_bundled",
            "skills_installed_count",
            "skills_target",
            "data_summary",
        }
        assert expected_keys.issubset(data.keys())


# ---------------------------------------------------------------------------
# TestFormatMcpConfig
# ---------------------------------------------------------------------------


class TestFormatMcpConfig:
    def test_valid_json(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)
        settings = _minimal_settings(tmp_path)
        report = collect_info(settings)
        text = format_mcp_config(report)
        data = json.loads(text)
        assert "mcpServers" in data
        assert "cellarbrain" in data["mcpServers"]
        server = data["mcpServers"]["cellarbrain"]
        assert "command" in server
        assert "args" in server
        assert "mcp" in server["args"]
