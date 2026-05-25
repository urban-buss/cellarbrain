"""Tests for the configurable format parameter across MCP tools."""

from __future__ import annotations

from decimal import Decimal

import pytest

from dataset_factory import (
    make_bottle,
    make_cellar,
    make_etl_run,
    make_grape,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)


@pytest.fixture()
def data_dir(tmp_path):
    """Minimal dataset for format tests."""
    entities = {
        "winery": [make_winery()],
        "appellation": [],
        "grape": [make_grape()],
        "wine": [make_wine(drinking_status="optimal", drink_from=2024, drink_until=2030)],
        "wine_grape": [make_wine_grape()],
        "bottle": [make_bottle(purchase_price=Decimal("25.00"))],
        "cellar": [make_cellar()],
        "provider": [],
        "tasting": [],
        "pro_rating": [],
        "etl_run": [make_etl_run()],
        "change_log": [],
    }
    write_dataset(tmp_path, entities)
    return tmp_path


@pytest.fixture()
def server(data_dir, monkeypatch):
    """MCP server with test data dir and markdown default."""
    import cellarbrain.observability as obs
    from cellarbrain import mcp_server

    monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(data_dir))

    if obs._collector is not None:
        obs._collector.close()
        obs._collector = None

    mcp_server._mcp_settings = None
    mcp_server._hybrid_engine = None
    mcp_server.invalidate_connections()

    settings = mcp_server._load_mcp_settings()
    object.__setattr__(settings.output, "default_format", "markdown")

    yield mcp_server

    if obs._collector is not None:
        obs._collector.close()
        obs._collector = None


class TestFormatMarkdown:
    """When format='markdown', output contains markdown table markers."""

    def test_cellar_stats_markdown(self, server):
        result = server.cellar_stats(format="markdown")
        assert "##" in result or "|" in result

    def test_find_wine_markdown(self, server):
        result = server.find_wine("Test", format="markdown")
        assert "|" in result

    def test_cellar_info_markdown(self, server):
        result = server.cellar_info(format="markdown")
        assert "|" in result
        assert "Cellar Info" in result


class TestFormatPlain:
    """When format='plain', output has no markdown table formatting."""

    def test_cellar_stats_plain(self, server):
        result = server.cellar_stats(format="plain")
        assert "|---" not in result
        assert "CELLAR SUMMARY" in result

    def test_find_wine_plain(self, server):
        result = server.find_wine("Test", format="plain")
        # Plain format uses numbered list or key-value pairs
        assert "|---" not in result

    def test_cellar_info_plain(self, server):
        result = server.cellar_info(format="plain")
        assert "|---" not in result
        # Should have converted headings to uppercase
        assert "CELLAR INFO" in result

    def test_cache_stats_plain(self, server):
        result = server.cache_stats(format="plain")
        assert "|---" not in result
        assert "QUERY CACHE STATS" in result

    def test_currency_rates_plain(self, server):
        result = server.currency_rates(format="plain")
        assert "|---" not in result
        assert "currency rates" in result


class TestFormatDefault:
    """When format=None, uses the settings default (markdown in tests)."""

    def test_default_uses_settings(self, server):
        # Test fixture sets default_format="markdown"
        result = server.cellar_stats()
        assert "##" in result or "|" in result

    def test_plain_default_override(self, server):
        # Override settings to plain
        settings = server._load_mcp_settings()
        object.__setattr__(settings.output, "default_format", "plain")
        result = server.cellar_stats()
        assert "|---" not in result
        assert "CELLAR SUMMARY" in result
