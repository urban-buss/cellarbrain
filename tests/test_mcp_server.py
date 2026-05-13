"""Tests for cellarbrain.mcp_server — FastMCP server tools, resources, and prompts."""

from __future__ import annotations

import asyncio
from datetime import UTC
from decimal import Decimal
from unittest.mock import patch

import pytest

from cellarbrain import markdown, writer

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
from dataset_factory import (
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_provider,
    make_tracked_wine,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)


def _make_dataset(tmp_path):
    """Write a minimal Parquet + dossier dataset for MCP tests."""
    from cellarbrain import companion_markdown
    from cellarbrain.settings import Settings

    winery_name = "Château MCP"
    wine_name = "Test Cuvée"

    wines = [
        make_wine(
            winery_name=winery_name,
            name=wine_name,
            is_favorite=True,
            tracked_wine_id=90_001,
            drink_from=2024,
            drink_until=2030,
            optimal_from=2025,
            optimal_until=2028,
            drinking_status="optimal",
            age_years=5,
        )
    ]

    entities = {
        "winery": [make_winery(name=winery_name)],
        "appellation": [make_appellation()],
        "grape": [make_grape()],
        "wine": wines,
        "wine_grape": [make_wine_grape()],
        "bottle": [
            make_bottle(
                original_purchase_price=Decimal("30.00"),
                purchase_price=Decimal("30.00"),
            )
        ],
        "cellar": [make_cellar()],
        "provider": [make_provider(name="Vendor")],
        "tasting": [],
        "pro_rating": [],
        "etl_run": [make_etl_run(total_inserts=3)],
        "change_log": [make_change_log()],
    }

    write_dataset(tmp_path, entities)

    # Generate dossiers
    markdown.generate_dossiers(entities, tmp_path, current_year=2025)

    # Generate tracked wines and companion dossiers
    settings = Settings()
    slug = companion_markdown.companion_dossier_slug(
        90_001,
        winery_name,
        wine_name,
    )
    tracked_wines = [
        make_tracked_wine(
            winery_name=wine_name,
            wine_name=wine_name,
            dossier_path=f"tracked/{slug}",
        )
    ]
    entities["tracked_wine"] = tracked_wines
    writer.write_parquet("tracked_wine", tracked_wines, tmp_path)
    companion_markdown.generate_companion_dossiers(entities, tmp_path, settings)

    return tmp_path


@pytest.fixture()
def data_dir(tmp_path):
    return _make_dataset(tmp_path)


@pytest.fixture(autouse=True)
def _set_data_dir_env(data_dir, monkeypatch):
    """Point CELLARBRAIN_DATA_DIR at the test dataset for all tests."""
    monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(data_dir))


# We import mcp_server AFTER the env is set so _data_dir() picks it up.
# Use importlib to avoid module-level import issues.
@pytest.fixture()
def server():
    import cellarbrain.observability as obs
    from cellarbrain import mcp_server

    # Reset the global collector to prevent stale MagicMock-based collectors
    # leaking from daemon tests (which use MagicMock settings).
    if obs._collector is not None:
        obs._collector.close()
        obs._collector = None

    mcp_server._mcp_settings = None  # force re-read from env
    mcp_server._hybrid_engine = None  # reset cached hybrid engine
    mcp_server.invalidate_connections()
    yield mcp_server

    # Cleanup: close and reset collector to avoid cross-test pollution
    if obs._collector is not None:
        obs._collector.close()
        obs._collector = None


# ---------------------------------------------------------------------------
# TestQueryCellarTool
# ---------------------------------------------------------------------------


class TestQueryCellarTool:
    def test_select_returns_markdown(self, server):
        result = server.query_cellar("SELECT wine_name FROM wines")
        assert "Test Cuvée" in result

    def test_invalid_sql_returns_error(self, server):
        result = server.query_cellar("DROP TABLE wines")
        assert result.startswith("Error:")

    def test_empty_sql_returns_error(self, server):
        result = server.query_cellar("")
        assert result.startswith("Error:")

    def test_docstring_contains_view_guidance(self):
        from cellarbrain.mcp_server import query_cellar

        doc = query_cellar.__doc__
        assert "View selection guide" in doc
        assert "Default to" in doc
        assert "wines_stored" in doc
        assert "bottles_stored" in doc
        assert "wines_full" in doc
        assert "bottles_full" in doc


# ---------------------------------------------------------------------------
# TestCellarStatsTool
# ---------------------------------------------------------------------------


class TestCellarStatsTool:
    def test_default_stats(self, server):
        result = server.cellar_stats()
        assert "Cellar Summary" in result

    def test_grouped_stats(self, server):
        result = server.cellar_stats(group_by="country")
        assert "France" in result

    def test_invalid_group_by(self, server):
        result = server.cellar_stats(group_by="invalid")
        assert result.startswith("Error:")

    def test_sort_by_passthrough(self, server):
        result = server.cellar_stats(group_by="country", sort_by="value")
        assert "France" in result

    def test_sort_by_invalid(self, server):
        result = server.cellar_stats(group_by="country", sort_by="bad")
        assert result.startswith("Error:")

    def test_limit_passthrough(self, server):
        result = server.cellar_stats(group_by="category", limit=1)
        assert "by Category" in result

    def test_limit_invalid(self, server):
        result = server.cellar_stats(group_by="country", limit=-1)
        assert result.startswith("Error:")


# ---------------------------------------------------------------------------
# TestCellarInfoTool
# ---------------------------------------------------------------------------


class TestCellarInfoTool:
    def test_returns_currency(self, server):
        result = server.cellar_info()
        assert "Cellar Info" in result
        assert "Default currency" in result
        assert "CHF" in result

    def test_returns_data_dir(self, server):
        result = server.cellar_info()
        assert "Data directory" in result

    def test_returns_version(self, server):
        result = server.cellar_info()
        assert "Version" in result

    def test_returns_config_file(self, server):
        result = server.cellar_info()
        assert "Config file" in result

    def test_returns_query_limits(self, server):
        result = server.cellar_info()
        assert "Row limit" in result
        assert "Search limit" in result

    def test_returns_etl_freshness(self, server):
        result = server.cellar_info()
        assert "Data Freshness" in result
        assert "Last ETL run" in result
        assert "full" in result

    def test_returns_last_changeset(self, server):
        result = server.cellar_info()
        assert "Last changeset" in result
        assert "+3 inserts" in result

    def test_returns_inventory(self, server):
        result = server.cellar_info()
        assert "Inventory" in result
        assert "Wines" in result
        assert "Bottles in cellar" in result

    def test_returns_dossier_count(self, server):
        result = server.cellar_info()
        assert "Dossiers" in result

    def test_returns_tracked_wines_count(self, server):
        result = server.cellar_info()
        assert "Tracked wines" in result

    def test_verbose_returns_python_version(self, server):
        result = server.cellar_info(verbose=True)
        assert "Python" in result

    def test_verbose_returns_total_etl_runs(self, server):
        result = server.cellar_info(verbose=True)
        assert "Total ETL runs" in result

    def test_verbose_returns_currency_rates(self, server):
        result = server.cellar_info(verbose=True)
        assert "Currency Rates" in result
        assert "EUR" in result

    def test_verbose_returns_companion_dossiers(self, server):
        result = server.cellar_info(verbose=True)
        assert "Companion dossiers" in result

    def test_graceful_without_data(self, tmp_path, monkeypatch):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(empty_dir))
        from cellarbrain import mcp_server

        mcp_server._mcp_settings = None
        mcp_server.invalidate_connections()
        result = mcp_server.cellar_info()
        assert "Cellar Info" in result
        assert "Version" in result
        assert "run `cellarbrain etl`" in result


# ---------------------------------------------------------------------------
# TestFindWineTool
# ---------------------------------------------------------------------------


class TestFindWineTool:
    def test_find_by_name(self, server):
        result = server.find_wine("Cuvée")
        assert "Test Cuvée" in result

    def test_no_match(self, server):
        result = server.find_wine("NonexistentXYZ")
        assert "No wines found" in result

    def test_multi_token_search(self, server):
        result = server.find_wine("Bordeaux 2020")
        assert "Cuvée" in result

    def test_accent_insensitive(self, server):
        result = server.find_wine("Chateau")
        assert "Château" in result

    def test_limit_zero_returns_error(self, server):
        result = server.find_wine("wine", limit=0)
        assert "Error" in result
        assert "limit" in result.lower()

    def test_limit_negative_returns_error(self, server):
        result = server.find_wine("wine", limit=-1)
        assert "Error" in result
        assert "limit" in result.lower()

    def test_intent_ready_to_drink(self, server):
        """Wine 1 has drinking_status='optimal' — should match 'ready to drink'."""
        result = server.find_wine("ready to drink")
        assert "Test Cuvée" in result

    def test_intent_too_young_no_match(self, server):
        """No wine in the fixture is too_young."""
        result = server.find_wine("too young")
        assert "No wines found" in result

    def test_intent_top_rated_no_match(self, server):
        """Fixture has no pro_ratings → nothing with best_pro_score."""
        result = server.find_wine("top rated")
        assert "No wines found" in result

    def test_concept_tracked(self, server):
        """Wine 1 has tracked_wine_id — should match 'tracked' system concept."""
        result = server.find_wine("tracked")
        assert "Test Cuvée" in result

    def test_concept_favorite(self, server):
        """Wine 1 has is_favorite=True — should match 'favorite' system concept."""
        result = server.find_wine("favorite")
        assert "Test Cuvée" in result


# ---------------------------------------------------------------------------
# TestSearchSynonymsTool
# ---------------------------------------------------------------------------


class TestSearchSynonymsTool:
    def test_list_returns_synonyms(self, server):
        result = server.search_synonyms(action="list")
        assert "search synonyms" in result
        assert "built-in" in result
        assert "rotwein" in result

    def test_add_custom_synonym(self, server, data_dir):
        result = server.search_synonyms(action="add", key="testterm", value="Barolo")
        assert "Added" in result
        # Verify listed as custom
        listing = server.search_synonyms(action="list")
        assert "testterm" in listing
        assert "custom" in listing

    def test_remove_custom_synonym(self, server, data_dir):
        server.search_synonyms(action="add", key="removeme", value="test")
        result = server.search_synonyms(action="remove", key="removeme")
        assert "Removed" in result
        listing = server.search_synonyms(action="list")
        assert "removeme" not in listing

    def test_remove_nonexistent_returns_error(self, server):
        result = server.search_synonyms(action="remove", key="nonexistent")
        assert "Error" in result

    def test_add_empty_key_returns_error(self, server):
        result = server.search_synonyms(action="add", key="", value="test")
        assert "Error" in result

    def test_add_stopword(self, server, data_dir):
        result = server.search_synonyms(action="add", key="noise", value="")
        assert "stopword" in result.lower()

    def test_invalid_action_returns_error(self, server):
        result = server.search_synonyms(action="invalid")
        assert "Error" in result

    def test_find_wine_uses_synonyms(self, server, data_dir):
        # "frankreich" is a built-in synonym for "France"
        result = server.find_wine("frankreich")
        assert "Château" in result or "Test Cuvée" in result


# ---------------------------------------------------------------------------
# TestCurrencyRatesTool
# ---------------------------------------------------------------------------


class TestCurrencyRatesTool:
    def test_list_returns_rates(self, server):
        result = server.currency_rates(action="list")
        assert "currency rates" in result
        assert "EUR" in result
        assert "toml" in result

    def test_set_new_rate(self, server, data_dir):
        result = server.currency_rates(action="set", currency="RON", rate=0.19)
        assert "Set exchange rate" in result
        assert "RON" in result
        # Verify listed as custom
        listing = server.currency_rates(action="list")
        assert "RON" in listing
        assert "custom" in listing

    def test_set_overwrites_toml_rate(self, server, data_dir):
        result = server.currency_rates(action="set", currency="EUR", rate=0.97)
        assert "Set exchange rate" in result
        listing = server.currency_rates(action="list")
        assert "0.97" in listing

    def test_remove_custom_rate(self, server, data_dir):
        server.currency_rates(action="set", currency="HRK", rate=0.13)
        result = server.currency_rates(action="remove", currency="HRK")
        assert "Removed" in result
        listing = server.currency_rates(action="list")
        assert "HRK" not in listing

    def test_remove_nonexistent_returns_error(self, server):
        result = server.currency_rates(action="remove", currency="XYZ")
        assert "Error" in result
        assert "not a custom rate" in result

    def test_set_missing_currency_returns_error(self, server):
        result = server.currency_rates(action="set", currency="", rate=0.5)
        assert "Error" in result

    def test_set_missing_rate_returns_error(self, server):
        result = server.currency_rates(action="set", currency="RON")
        assert "Error" in result

    def test_set_negative_rate_returns_error(self, server):
        result = server.currency_rates(action="set", currency="RON", rate=-0.5)
        assert "Error" in result
        assert "positive" in result

    def test_set_zero_rate_returns_error(self, server):
        result = server.currency_rates(action="set", currency="RON", rate=0.0)
        assert "Error" in result

    def test_set_default_currency_returns_error(self, server):
        result = server.currency_rates(action="set", currency="CHF", rate=1.0)
        assert "Error" in result
        assert "default currency" in result

    def test_set_invalid_currency_code_returns_error(self, server):
        result = server.currency_rates(action="set", currency="Euro", rate=0.93)
        assert "Error" in result
        assert "ISO 4217" in result

    def test_set_auto_uppercases(self, server, data_dir):
        result = server.currency_rates(action="set", currency="ron", rate=0.19)
        assert "RON" in result
        listing = server.currency_rates(action="list")
        assert "RON" in listing

    def test_remove_missing_currency_returns_error(self, server):
        result = server.currency_rates(action="remove", currency="")
        assert "Error" in result

    def test_invalid_action_returns_error(self, server):
        result = server.currency_rates(action="invalid")
        assert "Error" in result

    def test_sidecar_persists_across_reloads(self, server, data_dir):
        server.currency_rates(action="set", currency="RON", rate=0.19)
        # Force settings re-read
        server._mcp_settings = None
        listing = server.currency_rates(action="list")
        assert "RON" in listing
        assert "custom" in listing


# ---------------------------------------------------------------------------
# TestReadDossierTool
# ---------------------------------------------------------------------------


class TestReadDossierTool:
    def test_reads_existing_wine(self, server):
        result = server.read_dossier(1)
        assert "Test Cuvée" in result
        assert "---" in result

    def test_nonexistent_wine(self, server):
        result = server.read_dossier(999)
        assert result.startswith("Error:")

    def test_sections_filter_single_etl(self, server):
        result = server.read_dossier(1, sections=["identity"])
        assert "## Identity" in result
        assert "## Origin" not in result
        assert "wine_id:" in result  # frontmatter always present

    def test_sections_filter_agent_section(self, server):
        result = server.read_dossier(1, sections=["producer_profile"])
        assert "## Producer Profile" in result
        assert "## Identity" not in result
        assert "Test Cuvée" in result  # H1 always present

    def test_sections_empty_list(self, server):
        result = server.read_dossier(1, sections=[])
        assert "## Identity" not in result
        assert "wine_id:" in result  # frontmatter still present

    def test_sections_none_returns_full(self, server):
        full = server.read_dossier(1)
        filtered = server.read_dossier(1, sections=None)
        assert filtered == full


# ---------------------------------------------------------------------------
# TestUpdateDossierTool
# ---------------------------------------------------------------------------


class TestUpdateDossierTool:
    def test_update_agent_section(self, server):
        result = server.update_dossier(
            wine_id=1,
            section="producer_profile",
            content="Famous winery in Bordeaux.",
        )
        assert "Updated" in result

        # Verify content persisted
        dossier = server.read_dossier(1)
        assert "Famous winery in Bordeaux" in dossier

    def test_invalid_section(self, server):
        result = server.update_dossier(
            wine_id=1,
            section="identity",
            content="hacked",
        )
        assert result.startswith("Error:")

    def test_nonexistent_wine(self, server):
        result = server.update_dossier(
            wine_id=999,
            section="producer_profile",
            content="content",
        )
        assert result.startswith("Error:")

    def test_empty_content_returns_error(self, server):
        result = server.update_dossier(
            wine_id=1,
            section="wine_description",
            content="",
        )
        assert result.startswith("Error:")
        assert "empty" in result.lower()

    def test_whitespace_content_returns_error(self, server):
        result = server.update_dossier(
            wine_id=1,
            section="wine_description",
            content="   \n  ",
        )
        assert result.startswith("Error:")
        assert "empty" in result.lower()


# ---------------------------------------------------------------------------
# TestPendingResearchTool
# ---------------------------------------------------------------------------


class TestPendingResearchTool:
    def test_returns_pending_wines(self, server):
        result = server.pending_research()
        assert "wine_id" in result or "No wines" in result

    def test_limit_parameter(self, server):
        result = server.pending_research(limit=1)
        lines = [l for l in result.strip().split("\n") if l.strip()]
        if "No wines" not in result:
            assert len(lines) <= 3  # header + separator + 1 row

    def test_no_companion_data_in_result(self, server):
        result = server.pending_research()
        assert "Companion Dossiers" not in result

    def test_section_filter(self, server):
        result = server.pending_research(section="producer_profile")
        # Should return pending wines or "No wines" — either is valid
        assert "wine_id" in result or "No wines" in result

    def test_include_stale_parameter(self, server):
        result = server.pending_research(include_stale=True, stale_months=6)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestStaleResearchTool
# ---------------------------------------------------------------------------


class TestStaleResearchTool:
    def test_returns_result(self, server):
        result = server.stale_research()
        assert "No stale research" in result or "wine_id" in result

    def test_months_parameter(self, server):
        result = server.stale_research(months=1)
        assert isinstance(result, str)

    def test_section_filter(self, server):
        result = server.stale_research(section="producer_profile")
        assert isinstance(result, str)

    def test_limit_parameter(self, server):
        result = server.stale_research(limit=5)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestResearchCompletenessTool
# ---------------------------------------------------------------------------


class TestResearchCompletenessTool:
    def test_returns_result(self, server, data_dir):
        # Generate completeness parquet so the view exists
        from cellarbrain.dossier_ops import write_completeness_parquet

        write_completeness_parquet(data_dir)
        server.invalidate_connections()
        result = server.research_completeness()
        assert "wine_id" in result or "not available" in result

    def test_single_wine(self, server, data_dir):
        from cellarbrain.dossier_ops import write_completeness_parquet

        write_completeness_parquet(data_dir)
        server.invalidate_connections()
        result = server.research_completeness(wine_id=1)
        assert isinstance(result, str)

    def test_limit_parameter(self, server, data_dir):
        from cellarbrain.dossier_ops import write_completeness_parquet

        write_completeness_parquet(data_dir)
        server.invalidate_connections()
        result = server.research_completeness(limit=1)
        assert isinstance(result, str)

    def test_score_range_filter(self, server, data_dir):
        from cellarbrain.dossier_ops import write_completeness_parquet

        write_completeness_parquet(data_dir)
        server.invalidate_connections()
        result = server.research_completeness(min_score=0, max_score=50)
        assert isinstance(result, str)

    def test_no_parquet_returns_error(self, server):
        """Without completeness parquet, returns error message."""
        server.invalidate_connections()
        result = server.research_completeness()
        assert "not available" in result or "Error" in result


# ---------------------------------------------------------------------------
# TestUpdateDossierSources
# ---------------------------------------------------------------------------


class TestUpdateDossierSources:
    def test_sources_and_confidence_params(self, server):
        result = server.update_dossier(
            wine_id=1,
            section="producer_profile",
            content="Test content from sources.",
            sources=["wine-searcher.com"],
            confidence="high",
        )
        assert "Updated" in result or "Error" in result


# ---------------------------------------------------------------------------
# TestPrompts
# ---------------------------------------------------------------------------


class TestPrompts:
    def test_cellar_qa_prompt(self, server):
        result = server.cellar_qa()
        assert "wine cellar assistant" in result.lower()

    def test_food_pairing_prompt(self, server):
        result = server.food_pairing("grilled lamb")
        assert "grilled lamb" in result

    def test_wine_research_prompt(self, server):
        result = server.wine_research(1)
        assert "1" in result

    def test_batch_research_prompt(self, server):
        result = server.batch_research(5)
        assert "5" in result


# ---------------------------------------------------------------------------
# TestReloadDataErrors
# ---------------------------------------------------------------------------


class TestReloadDataErrors:
    def test_missing_bottles_csv_returns_error(self, tmp_path, monkeypatch):
        """reload_data reports error when bottles CSV is missing."""
        # Use a nested directory to isolate from the autouse data_dir fixture.
        base = tmp_path / "isolate"
        base.mkdir()
        out = base / "output"
        out.mkdir()
        raw = base / "raw"
        raw.mkdir()
        # Create only the wines CSV, not the bottles CSV.
        (raw / "export-wines.csv").write_text("placeholder", encoding="utf-8")

        monkeypatch.setenv("CELLARBRAIN_DATA_DIR", str(out))

        from cellarbrain import mcp_server

        # Reset the cached settings so _data_dir() picks up the new env var.
        monkeypatch.setattr(mcp_server, "_mcp_settings", None)

        result = mcp_server.reload_data()
        assert result.startswith("Error:")
        assert "export-bottles-stored.csv" in result


# ---------------------------------------------------------------------------
# TestConnectionCaching
# ---------------------------------------------------------------------------


class TestConnectionCaching:
    def test_get_connection_returns_cached(self, server, monkeypatch):
        """Repeated _get_connection() calls return the same object."""
        from cellarbrain import mcp_server

        # Ensure cache is clear
        mcp_server.invalidate_connections()
        c1 = mcp_server._get_connection()
        c2 = mcp_server._get_connection()
        assert c1 is c2

    def test_get_agent_connection_returns_cached(self, server, monkeypatch):
        """Repeated _get_agent_connection() calls return the same object."""
        from cellarbrain import mcp_server

        mcp_server.invalidate_connections()
        c1 = mcp_server._get_agent_connection()
        c2 = mcp_server._get_agent_connection()
        assert c1 is c2

    def test_invalidate_clears_cache(self, server, monkeypatch):
        """invalidate_connections() causes fresh connections on next call."""
        from cellarbrain import mcp_server

        mcp_server.invalidate_connections()
        c1 = mcp_server._get_connection()
        a1 = mcp_server._get_agent_connection()
        mcp_server.invalidate_connections()
        c2 = mcp_server._get_connection()
        a2 = mcp_server._get_agent_connection()
        assert c1 is not c2
        assert a1 is not a2


# ---------------------------------------------------------------------------
# TestCompanionDossierTools
# ---------------------------------------------------------------------------


class TestCompanionDossierTools:
    def test_read_companion_dossier(self, server):
        result = server.read_companion_dossier(90_001)
        assert "Test Cuvée" in result
        assert "tracked_wine_id:" in result

    def test_read_companion_nonexistent(self, server):
        result = server.read_companion_dossier(999)
        assert result.startswith("Error:")

    def test_read_companion_filtered(self, server):
        result = server.read_companion_dossier(90_001, sections=["producer_deep_dive"])
        assert "## Producer Deep Dive" in result
        assert "## Buying Guide" not in result

    def test_update_companion_dossier(self, server):
        result = server.update_companion_dossier(
            tracked_wine_id=90_001,
            section="producer_deep_dive",
            content="Famous Bordeaux domaine.",
        )
        assert "Updated" in result

        dossier = server.read_companion_dossier(90_001)
        assert "Famous Bordeaux domaine" in dossier

    def test_update_companion_invalid_section(self, server):
        result = server.update_companion_dossier(
            tracked_wine_id=90_001,
            section="identity",
            content="hacked",
        )
        assert result.startswith("Error:")

    def test_list_companion_dossiers_pending(self, server):
        result = server.list_companion_dossiers(pending_only=True)
        assert "tracked_wine_id" in result or "No" in result


# ---------------------------------------------------------------------------
# TestPendingCompanionResearchTool
# ---------------------------------------------------------------------------


class TestPendingCompanionResearchTool:
    def test_returns_pending_companions(self, server):
        result = server.pending_companion_research()
        assert "tracked_wine_id" in result or "No" in result

    def test_limit_parameter(self, server):
        result = server.pending_companion_research(limit=1)
        lines = [l for l in result.strip().split("\n") if l.strip()]
        if "No" not in result:
            assert len(lines) <= 3  # header + separator + 1 row


# ---------------------------------------------------------------------------
# TestPriceTools
# ---------------------------------------------------------------------------


class TestPriceTools:
    def test_log_price_tool(self, server):
        result = server.log_price(
            tracked_wine_id=90_001,
            bottle_size_ml=750,
            retailer_name="Test Retailer",
            price=45.0,
            currency="CHF",
            in_stock=True,
            vintage=2020,
        )
        assert "Recorded" in result or "Updated" in result

    def test_tracked_wine_prices_tool(self, server):
        # Log a price first
        server.log_price(
            tracked_wine_id=90_001,
            bottle_size_ml=750,
            retailer_name="Test Retailer",
            price=45.0,
            currency="CHF",
            in_stock=True,
            vintage=2020,
        )
        result = server.tracked_wine_prices(tracked_wine_id=90_001)
        assert "Test Retailer" in result

    def test_price_history_tool(self, server):
        server.log_price(
            tracked_wine_id=90_001,
            bottle_size_ml=750,
            retailer_name="Test Retailer",
            price=45.0,
            currency="CHF",
            in_stock=True,
            vintage=2020,
        )
        result = server.price_history(tracked_wine_id=90_001)
        assert "Test Retailer" in result or "No price history" in result

    def test_log_price_invalid_tracked_wine(self, server):
        result = server.log_price(
            tracked_wine_id=999,
            bottle_size_ml=750,
            retailer_name="Shop",
            price=10.0,
            currency="CHF",
            in_stock=True,
        )
        assert result.startswith("Error:")

    def test_tracked_wine_prices_invalid(self, server):
        result = server.tracked_wine_prices(tracked_wine_id=999)
        assert result.startswith("Error:")

    def test_wishlist_alerts_tool(self, server):
        server.log_price(
            tracked_wine_id=90_001,
            bottle_size_ml=750,
            retailer_name="Alert Shop",
            price=50.0,
            currency="CHF",
            in_stock=True,
            vintage=2020,
        )
        result = server.wishlist_alerts()
        assert "New Listing" in result or "No wishlist alerts" in result

    def test_wishlist_alerts_empty(self, server):
        result = server.wishlist_alerts()
        assert "No price observations" in result


class TestToolLogging:
    def test_tool_logs_invocation(self, server, caplog):
        with caplog.at_level("INFO", logger="cellarbrain.mcp_server"):
            server.find_wine(query="Merlot")
        assert "tool=find_wine" in caplog.text

    def test_tool_logs_elapsed(self, server, caplog):
        with caplog.at_level("INFO", logger="cellarbrain.mcp_server"):
            server.cellar_stats()
        assert "elapsed_ms" in caplog.text

    def test_tool_logs_error(self, server, caplog):
        with caplog.at_level("WARNING", logger="cellarbrain.mcp_server"):
            server.read_dossier(wine_id=99999)
        assert "tool=read_dossier" in caplog.text
        assert "error=" in caplog.text


class TestBuildEventSoftErrors:
    """Verify _build_event detects soft errors (tools returning 'Error: ...')."""

    @pytest.fixture(autouse=True)
    def _collector(self, tmp_path):

        from cellarbrain.observability import EventCollector
        from cellarbrain.settings import LoggingConfig

        config = LoggingConfig(log_db=str(tmp_path / "test.duckdb"))
        self._col = EventCollector(config, str(tmp_path))
        with patch("cellarbrain.observability.get_collector", return_value=self._col):
            yield
        self._col.close()

    def _last_event(self):
        self._col.flush()
        return self._col._db.execute(
            "SELECT status, error_type, error_message FROM tool_events ORDER BY started_at DESC LIMIT 1"
        ).fetchone()

    def test_soft_error_sets_status_error(self):
        import time

        from cellarbrain.mcp_server import _build_event

        _build_event(
            "test_tool",
            "tool",
            {},
            {},
            time.perf_counter(),
            result="Error: some descriptive error message",
            exc=None,
        )
        status, error_type, error_message = self._last_event()
        assert status == "error"
        assert error_type == "HandledError"
        assert error_message == "some descriptive error message"

    def test_hard_error_uses_exception(self):
        import time

        from cellarbrain.mcp_server import _build_event
        from cellarbrain.query import QueryError

        _build_event(
            "test_tool",
            "tool",
            {},
            {},
            time.perf_counter(),
            result=None,
            exc=QueryError("Column 'foo' not found"),
        )
        status, error_type, error_message = self._last_event()
        assert status == "error"
        assert error_type == "QueryError"
        assert error_message == "Column 'foo' not found"

    def test_ok_result_stays_ok(self):
        import time

        from cellarbrain.mcp_server import _build_event

        _build_event(
            "test_tool",
            "tool",
            {},
            {},
            time.perf_counter(),
            result="| col |\n|---|\n| val |",
            exc=None,
        )
        status, error_type, error_message = self._last_event()
        assert status == "ok"
        assert error_type is None
        assert error_message is None


class TestMetaParameter:
    def test_query_cellar_accepts_meta(self, server):
        result = server.query_cellar(
            sql="SELECT COUNT(*) AS n FROM wines",
            meta={"agent_name": "test-agent"},
        )
        assert "n" in result

    def test_find_wine_accepts_meta(self, server):
        result = server.find_wine(query="Merlot", meta={"agent_name": "qa"})
        assert isinstance(result, str)

    def test_meta_none_works(self, server):
        result = server.cellar_stats(meta=None)
        assert isinstance(result, str)

    def test_meta_absent_backward_compat(self, server):
        result = server.cellar_stats()
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestToolRegistration
# ---------------------------------------------------------------------------


class TestToolRegistration:
    """Verify _tool() decorator produces correct tool metadata."""

    EXPECTED_TOOLS = {
        "query_cellar",
        "cellar_stats",
        "cellar_churn",
        "cellar_info",
        "find_wine",
        "wine_suggestions",
        "read_dossier",
        "update_dossier",
        "reload_data",
        "pending_research",
        "suggest_wines",
        "suggest_foods",
        "pair_wine",
        "add_pairing",
        "server_stats",
        "cache_stats",
        "log_price",
        "tracked_wine_prices",
        "price_history",
        "currency_rates",
    }

    def test_all_expected_tools_registered(self, server):
        tools = asyncio.run(server.mcp.list_tools())
        tool_names = {t.name for t in tools}
        missing = self.EXPECTED_TOOLS - tool_names
        assert not missing, f"Missing tools: {missing}"

    def test_meta_optional_in_input_schema(self, server):
        """meta parameter must be optional (default None) so clients can ignore it."""
        tools = asyncio.run(server.mcp.list_tools())
        for tool in tools:
            if not tool.inputSchema or "properties" not in tool.inputSchema:
                continue
            if "meta" in tool.inputSchema["properties"]:
                required = tool.inputSchema.get("required", [])
                assert "meta" not in required, (
                    f"Tool {tool.name} has 'meta' as required — it must be optional for backward compat"
                )

    def test_tools_have_descriptions(self, server):
        tools = asyncio.run(server.mcp.list_tools())
        for tool in tools:
            assert tool.description, f"Tool {tool.name} has no description"
            assert len(tool.description) > 10, f"Tool {tool.name} description too short"

    def test_no_duplicate_tool_names(self, server):
        tools = asyncio.run(server.mcp.list_tools())
        names = [t.name for t in tools]
        assert len(names) == len(set(names)), "Duplicate tool registrations found"


# ---------------------------------------------------------------------------
# TestSchemaResource
# ---------------------------------------------------------------------------


class TestSchemaResource:
    def test_schema_returns_non_empty(self, server):
        result = server.view_schemas()
        assert len(result) > 0

    def test_schema_contains_wines_view(self, server):
        result = server.view_schemas()
        assert "## wines" in result

    def test_schema_contains_wines_full_view(self, server):
        result = server.view_schemas()
        assert "## wines_full" in result

    def test_schema_contains_critical_columns(self, server):
        result = server.view_schemas()
        assert "subregion" in result
        assert "primary_grape" in result
        assert "price" in result

    def test_schema_excludes_internal_views(self, server):
        result = server.view_schemas()
        assert "## _wines_wishlist" not in result

    def test_schema_contains_view_descriptions(self, server):
        result = server.view_schemas()
        assert "One row per wine (slim: 20 columns)" in result
        assert "wines_stored" in result


# ---------------------------------------------------------------------------
# TestSommelierErrors
# ---------------------------------------------------------------------------


class TestSommelierErrors:
    @pytest.fixture(autouse=True)
    def _force_missing_model(self, server):
        """Reset engine cache and point model_dir to a nonexistent path."""
        server._sommelier_engine = None
        server._food_catalogue_meta = None
        server._hybrid_engine = None
        original = server._load_mcp_settings

        def _patched():
            s = original()
            object.__setattr__(
                s,
                "sommelier",
                type(s.sommelier)(
                    model_dir="nonexistent/model",
                ),
            )
            return s

        server._load_mcp_settings = _patched
        server._mcp_settings = None
        yield
        server._load_mcp_settings = original
        server._mcp_settings = None
        server._sommelier_engine = None
        server._food_catalogue_meta = None
        server._hybrid_engine = None

    def test_suggest_wines_returns_not_trained(self, server):
        result = asyncio.run(server.suggest_wines(food_query="grilled lamb"))
        assert result.startswith("Error:")

    def test_suggest_foods_returns_not_trained(self, server):
        result = asyncio.run(server.suggest_foods(wine_id=1))
        assert result.startswith("Error:")


# ---------------------------------------------------------------------------
# TestPairingCandidates
# ---------------------------------------------------------------------------


class TestPairingCandidates:
    """Tests for the pairing_candidates MCP tool."""

    def test_returns_markdown_table(self, server):
        result = asyncio.run(
            server.pairing_candidates(dish_description="grilled steak", protein="red_meat", category="red")
        )
        # Should return either a table or no-results message
        assert "| Rank |" in result or "No pairing candidates" in result

    def test_no_results_message(self, server):
        result = asyncio.run(
            server.pairing_candidates(
                dish_description="xyznonexistent",
                category="sparkling",
                grapes="ZZZ_NoGrape",
            )
        )
        assert "No pairing candidates" in result

    def test_grape_param_comma_split(self, server):
        result = asyncio.run(server.pairing_candidates(dish_description="pasta", grapes="Nebbiolo, Sangiovese"))
        # Should not error — grape splitting works
        assert "Error:" not in result

    def test_auto_classify_dish_only(self, server):
        result = asyncio.run(server.pairing_candidates(dish_description="grilled lamb chops"))
        # Should auto-classify and return results (not error or empty)
        assert "Error:" not in result
        # Should have found some wines (auto-classified as red_meat)
        assert "| Rank |" in result or "No pairing candidates" in result


# ---------------------------------------------------------------------------
# TestPairWine
# ---------------------------------------------------------------------------


class TestPairWine:
    """Tests for the pair_wine simplified MCP tool."""

    def test_returns_recommendations(self, server):
        result = asyncio.run(server.pair_wine(dish="grilled steak"))
        assert "Error:" not in result
        # Should return either recommendations or no-wines message
        assert "Top Pairing Recommendations" in result or "No wines found" in result

    def test_no_results_message(self, server):
        result = asyncio.run(server.pair_wine(dish="xyznonexistent"))
        # Unclassifiable dish — may return no wines
        assert "Error:" not in result

    def test_with_occasion(self, server):
        result = asyncio.run(server.pair_wine(dish="raclette", occasion="casual dinner"))
        assert "Error:" not in result

    def test_limit_parameter(self, server):
        result = asyncio.run(server.pair_wine(dish="grilled steak", limit=2))
        assert "Error:" not in result
        if "Top Pairing Recommendations" in result:
            # Count wine_id occurrences — should be at most 2
            assert result.count("wine_id:") <= 2

    def test_mode_trailer_rag_when_model_missing(self, server):
        """The pair_wine output includes a Mode trailer (hybrid or rag)."""
        result = asyncio.run(server.pair_wine(dish="grilled steak"))
        if "Top Pairing Recommendations" in result:
            assert "_Mode: " in result
            assert ("rag" in result.split("_Mode: ", 1)[1].split("_")[0]) or (
                "hybrid" in result.split("_Mode: ", 1)[1].split("_")[0]
            )

    def test_pairing_candidates_mode_trailer(self, server):
        result = asyncio.run(
            server.pairing_candidates(
                dish_description="grilled steak",
                protein="red_meat",
                category="red",
            )
        )
        if "| Rank |" in result:
            assert "_Mode:" in result


# ---------------------------------------------------------------------------
# TestAddPairing
# ---------------------------------------------------------------------------


class TestAddPairing:
    """Tests for the add_pairing MCP tool."""

    @pytest.fixture(autouse=True)
    def _patch_dataset(self, server, tmp_path):
        """Point pairing_dataset to a temp file for each test."""
        self._dataset_path = tmp_path / "pairings.parquet"
        server._mcp_settings = None
        original = server._load_mcp_settings

        def _patched():
            s = original()
            object.__setattr__(
                s,
                "sommelier",
                type(s.sommelier)(
                    pairing_dataset=str(self._dataset_path),
                ),
            )
            return s

        server._load_mcp_settings = _patched
        server._mcp_settings = None
        yield
        server._load_mcp_settings = original
        server._mcp_settings = None

    def test_add_pairing_creates_new_file(self, server):
        """First add_pairing creates the dataset file."""
        result = server.add_pairing(
            food_text="grilled lamb | ingredients: lamb, rosemary | French | heavy | lamb | grill | rich",
            wine_text="Château Test | Merlot | Bordeaux, France | red",
            pairing_score=0.85,
            pairing_reason="Rich tannins complement lamb fat.",
        )
        assert "Pairing added" in result
        assert "1 pairs" in result
        assert self._dataset_path.exists()

    def test_add_pairing_appends_to_dataset(self, server):
        """Second call appends — count increases."""
        server.add_pairing(
            food_text="food one",
            wine_text="wine one",
            pairing_score=0.7,
        )
        result = server.add_pairing(
            food_text="food two",
            wine_text="wine two",
            pairing_score=0.5,
        )
        assert "2 pairs" in result

    def test_add_pairing_validates_score_too_high(self, server):
        result = server.add_pairing(
            food_text="valid food",
            wine_text="valid wine",
            pairing_score=1.5,
        )
        assert result.startswith("Error:")
        assert "between 0.0 and 1.0" in result

    def test_add_pairing_validates_score_negative(self, server):
        result = server.add_pairing(
            food_text="valid food",
            wine_text="valid wine",
            pairing_score=-0.1,
        )
        assert result.startswith("Error:")

    def test_add_pairing_validates_empty_food_text(self, server):
        result = server.add_pairing(
            food_text="",
            wine_text="valid wine",
            pairing_score=0.5,
        )
        assert result.startswith("Error:")
        assert "food_text" in result

    def test_add_pairing_validates_empty_wine_text(self, server):
        result = server.add_pairing(
            food_text="valid food",
            wine_text="   ",
            pairing_score=0.5,
        )
        assert result.startswith("Error:")
        assert "wine_text" in result

    def test_add_pairing_returns_total_count(self, server):
        """Response includes the total pair count."""
        for i in range(3):
            result = server.add_pairing(
                food_text=f"food {i}",
                wine_text=f"wine {i}",
                pairing_score=0.5 + i * 0.1,
            )
        assert "3 pairs" in result


# ---------------------------------------------------------------------------
# TestGetFormatSiblingsTool
# ---------------------------------------------------------------------------


class TestGetFormatSiblingsTool:
    def test_no_siblings(self, server):
        result = server.get_format_siblings(wine_id=1)
        assert "no format siblings" in result.lower()

    def test_nonexistent_wine(self, server):
        result = server.get_format_siblings(wine_id=999)
        assert "no format siblings" in result.lower()


# ---------------------------------------------------------------------------
# TestBatchUpdateDossierTool
# ---------------------------------------------------------------------------


class TestBatchUpdateDossierTool:
    def test_batch_updates_multiple_wines(self, server):
        result = server.batch_update_dossier(
            wine_ids=[1],
            section="producer_profile",
            content="Shared producer profile content.",
        )
        assert "1/1" in result
        assert "✓" in result

    def test_batch_partial_failure(self, server):
        result = server.batch_update_dossier(
            wine_ids=[1, 999],
            section="producer_profile",
            content="Content here.",
        )
        assert "1/2" in result
        assert "✗" in result


# ---------------------------------------------------------------------------
# TestTrainSommelierTool
# ---------------------------------------------------------------------------


class TestTrainSommelierTool:
    """Tests for the train_sommelier MCP tool."""

    def test_missing_dataset_returns_error(self, server, tmp_path):
        """When pairing dataset doesn't exist and seed is unavailable, returns an error."""
        import unittest.mock

        server._mcp_settings = None
        original = server._load_mcp_settings
        missing = str(tmp_path / "no" / "such" / "pairings.parquet")

        def _patched():
            s = original()
            object.__setattr__(
                s,
                "sommelier",
                type(s.sommelier)(
                    pairing_dataset=missing,
                ),
            )
            return s

        server._load_mcp_settings = _patched
        server._mcp_settings = None
        try:
            with unittest.mock.patch(
                "cellarbrain.sommelier.seed.ensure_pairing_dataset",
            ):
                result = server.train_sommelier()
            assert "Error" in result
            assert "not found" in result
        finally:
            server._load_mcp_settings = original
            server._mcp_settings = None

    def test_incremental_without_model_returns_error(self, server):
        """Incremental training without existing model returns an error."""
        server._mcp_settings = None
        original = server._load_mcp_settings

        def _patched():
            s = original()
            object.__setattr__(
                s,
                "sommelier",
                type(s.sommelier)(
                    model_dir="nonexistent/model",
                    pairing_dataset="nonexistent/pairings.parquet",
                ),
            )
            return s

        server._load_mcp_settings = _patched
        server._mcp_settings = None
        try:
            result = server.train_sommelier(incremental=True)
            assert "Error:" in result
        finally:
            server._load_mcp_settings = original
            server._mcp_settings = None


# ---------------------------------------------------------------------------
# TestGetDrinkTonightTool (Phase D - dashboard sidecar bridge)
# ---------------------------------------------------------------------------


class TestGetDrinkTonightTool:
    def test_empty_returns_message(self, server):
        result = server.get_drink_tonight()
        assert "empty" in result.lower()

    def test_populated_lists_wines(self, server, data_dir):
        from cellarbrain.dashboard.sidecars import add_drink_tonight

        add_drink_tonight(str(data_dir), wine_id=1, note="for friday")
        result = server.get_drink_tonight()
        assert "Drink Tonight" in result
        assert "Test Cuv" in result
        assert "for friday" in result

    def test_resource_mirrors_tool(self, server, data_dir):
        from cellarbrain.dashboard.sidecars import add_drink_tonight

        add_drink_tonight(str(data_dir), wine_id=1)
        from cellarbrain.mcp_server import drink_tonight_list

        out = drink_tonight_list()
        assert "Drink Tonight" in out

    def test_missing_wine_id_renders_placeholder(self, server, data_dir):
        from cellarbrain.dashboard.sidecars import add_drink_tonight

        add_drink_tonight(str(data_dir), wine_id=99999)
        result = server.get_drink_tonight()
        assert "99999" in result
        assert "missing" in result


# ---------------------------------------------------------------------------
# TestSuggestWinesTool
# ---------------------------------------------------------------------------


class TestSuggestWinesTool:
    def test_returns_suggestions(self, server):
        result = server.wine_suggestions("Cuv")
        # May return suggestions or no-suggestions message
        assert isinstance(result, str)

    def test_short_query(self, server):
        result = server.wine_suggestions("Cu")
        assert isinstance(result, str)

    def test_no_match_query(self, server):
        result = server.wine_suggestions("Zyxwvutsrqponm")
        assert "Error" not in result


# ---------------------------------------------------------------------------
# TestSearchStatsTool
# ---------------------------------------------------------------------------


class TestSearchStatsTool:
    def test_returns_stats_or_error(self, server):
        """search_stats requires observability — returns error if not init'd."""
        result = server.search_stats()
        # Without observability init, returns error
        assert isinstance(result, str)

    def test_with_observability(self, server, tmp_path, monkeypatch):
        """search_stats returns stats when observability is active."""
        from cellarbrain import observability
        from cellarbrain.observability import SearchEvent, init_observability
        from cellarbrain.settings import LoggingConfig

        # Reset global collector to allow re-init
        monkeypatch.setattr(observability, "_collector", None)

        config = LoggingConfig(log_db=str(tmp_path / "search_test.duckdb"))
        init_observability(config, str(tmp_path))

        from datetime import UTC, datetime

        from cellarbrain.observability import get_collector

        collector = get_collector()
        event = SearchEvent(
            event_id="test-001",
            session_id=collector.session_id,
            turn_id=collector.turn_id,
            query="test query",
            normalized_query="test query",
            result_count=5,
            intent_matched=False,
            used_soft_and=False,
            used_fuzzy=False,
            used_phonetic=False,
            used_suggestions=False,
            started_at=datetime.now(UTC),
            duration_ms=10.0,
            client_id=None,
        )
        collector.record_search(event)
        collector.flush()

        result = server.search_stats(window_days=30)
        assert "Search Statistics" in result
        assert "Total searches" in result
        collector.close()


# ---------------------------------------------------------------------------
# TestPlanTripTool
# ---------------------------------------------------------------------------


class TestPlanTripTool:
    def test_returns_travel_brief(self, server):
        result = server.plan_trip(destination="France")
        assert isinstance(result, str)
        assert "Wine Travel Brief" in result or "No wines found" in result

    def test_no_match_returns_message(self, server):
        result = server.plan_trip(destination="Narnia")
        assert "No wines found" in result

    def test_empty_destination_returns_error(self, server):
        result = server.plan_trip(destination="")
        assert "Error" in result

    def test_accepts_meta(self, server):
        result = server.plan_trip(destination="France", meta={"agent_name": "test"})
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestCellarAnomaliesTool
# ---------------------------------------------------------------------------


class TestCellarAnomaliesTool:
    def test_returns_no_anomalies_on_empty(self, server):
        result = server.cellar_anomalies()
        # Either "No anomalies" or an error about log store — both acceptable
        assert isinstance(result, str)

    def test_severity_filter(self, server):
        result = server.cellar_anomalies(severity="critical")
        assert isinstance(result, str)

    def test_kinds_filter(self, server):
        result = server.cellar_anomalies(kinds="call_volume_spike,latency_spike")
        assert isinstance(result, str)

    def test_accepts_meta(self, server):
        result = server.cellar_anomalies(meta={"agent_name": "test"})
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# TestToolResponseStructuredOutput
# ---------------------------------------------------------------------------


class TestToolResponseStructuredOutput:
    """Verify that migrated tools return ToolResponse with structured data."""

    def test_query_cellar_returns_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.query_cellar("SELECT wine_name FROM wines")
        assert isinstance(result, ToolResponse)
        assert result.has_structured_content
        assert "columns" in result.data
        assert "rows" in result.data
        assert "row_count" in result.data
        assert result.data["row_count"] >= 1

    def test_query_cellar_error_is_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.query_cellar("DROP TABLE wines")
        assert isinstance(result, ToolResponse)
        assert result.startswith("Error:")
        assert "error" in result.data

    def test_cellar_stats_returns_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.cellar_stats()
        assert isinstance(result, ToolResponse)
        assert result.has_structured_content
        assert "group_by" in result.data
        assert result.data["group_by"] is None  # default no grouping

    def test_cellar_stats_grouped_has_group_by(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.cellar_stats(group_by="country")
        assert isinstance(result, ToolResponse)
        assert result.data["group_by"] == "country"

    def test_find_wine_returns_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.find_wine("Cuvée")
        assert isinstance(result, ToolResponse)
        assert result.has_structured_content
        assert "query" in result.data
        assert result.data["query"] == "Cuvée"
        assert result.data["result_count"] >= 1

    def test_find_wine_no_match_has_zero_count(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.find_wine("NonexistentXYZ")
        assert isinstance(result, ToolResponse)
        assert result.data["result_count"] == 0

    def test_cellar_info_returns_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.cellar_info()
        assert isinstance(result, ToolResponse)
        assert result.has_structured_content
        assert "version" in result.data
        assert "data_dir" in result.data
        assert "currency" in result.data

    def test_reload_data_error_returns_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        # Without CSV files in raw/, reload_data returns an error ToolResponse
        result = server.reload_data()
        assert isinstance(result, ToolResponse)
        assert result.startswith("Error:")
        assert result.has_structured_content
        assert "error" in result.data

    def test_non_migrated_tool_returns_plain_str(self, server):
        """Tools not yet migrated still return plain str (not ToolResponse)."""
        # wine_detail is not migrated — returns plain str
        result = server.wine_detail(wine_id=1)
        # It should still be a valid string
        assert isinstance(result, str)
        assert len(result) > 0


# ---------------------------------------------------------------------------
# TestCellarDigestTool
# ---------------------------------------------------------------------------


class TestCellarDigestTool:
    def test_daily_digest_returns_string(self, server):
        result = server.cellar_digest(period="daily")
        assert isinstance(result, str)
        assert "Cellar Digest" in result

    def test_weekly_digest_returns_string(self, server):
        result = server.cellar_digest(period="weekly")
        assert isinstance(result, str)
        assert "Weekly Cellar Digest" in result

    def test_invalid_period_returns_error(self, server):
        result = server.cellar_digest(period="monthly")
        assert "Error" in result

    def test_digest_contains_inventory(self, server):
        result = server.cellar_digest(period="daily")
        # Should contain some digest content (header + at least one section)
        assert "Cellar Digest" in result


# ---------------------------------------------------------------------------
# TestWineOfTheDayTool
# ---------------------------------------------------------------------------


class TestWineOfTheDayTool:
    def test_returns_pick(self, server):
        result = server.wine_of_the_day()
        assert isinstance(result, str)
        assert "Wine of the Day" in result

    def test_deterministic(self, server):
        result1 = server.wine_of_the_day()
        result2 = server.wine_of_the_day()
        assert result1 == result2

    def test_contains_wine_from_dataset(self, server):
        result = server.wine_of_the_day()
        # The test dataset has "Test Cuvée" from "Château MCP"
        assert "Test" in result or "Wine of the Day" in result


# ---------------------------------------------------------------------------
# TestConsumptionVelocityTool
# ---------------------------------------------------------------------------


class TestConsumptionVelocityTool:
    def test_returns_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.consumption_velocity()
        assert isinstance(result, ToolResponse)
        assert result.has_structured_content

    def test_structured_data_keys(self, server):
        result = server.consumption_velocity()
        expected_keys = {
            "months",
            "avg_acquired_per_month",
            "avg_consumed_per_month",
            "net_growth_per_month",
            "current_bottles",
            "projected_12m",
            "lookback_months",
        }
        assert expected_keys <= set(result.data.keys())

    def test_default_months_is_6(self, server):
        result = server.consumption_velocity()
        assert result.data["lookback_months"] == 6

    def test_custom_months(self, server):
        result = server.consumption_velocity(months=3)
        assert result.data["lookback_months"] == 3

    def test_invalid_months_returns_error(self, server):
        result = server.consumption_velocity(months=0)
        assert result.startswith("Error:")

    def test_excessive_months_returns_error(self, server):
        result = server.consumption_velocity(months=100)
        assert result.startswith("Error:")

    def test_markdown_contains_header(self, server):
        result = server.consumption_velocity()
        assert "Consumption Velocity" in result


# ---------------------------------------------------------------------------
# TestCellarGapsTool
# ---------------------------------------------------------------------------


class TestCellarGapsTool:
    """Tests for the cellar_gaps MCP tool."""

    def test_returns_tool_response(self, server):
        from cellarbrain.mcp_responses import ToolResponse

        result = server.cellar_gaps()
        assert isinstance(result, ToolResponse)
        assert result.has_structured_content

    def test_structured_data_keys(self, server):
        result = server.cellar_gaps()
        expected_keys = {"dimension", "months", "region_gaps", "grape_gaps", "price_tier_gaps", "vintage_gaps"}
        assert expected_keys <= set(result.data.keys())

    def test_default_returns_all_dimensions(self, server):
        result = server.cellar_gaps()
        assert "Region Gaps" in result
        assert "Grape Gaps" in result
        assert "Price Tier Gaps" in result
        assert "Vintage Gaps" in result

    def test_single_dimension(self, server):
        result = server.cellar_gaps(dimension="region")
        assert "Region Gaps" in result
        assert "Grape Gaps" not in result

    def test_invalid_dimension_returns_error(self, server):
        result = server.cellar_gaps(dimension="invalid")
        assert result.startswith("Error:")

    def test_invalid_months_returns_error(self, server):
        result = server.cellar_gaps(months=0)
        assert result.startswith("Error:")

    def test_excessive_months_returns_error(self, server):
        result = server.cellar_gaps(months=100)
        assert result.startswith("Error:")

    def test_custom_months(self, server):
        result = server.cellar_gaps(months=3)
        assert result.data["months"] == 3

    def test_metadata_includes_params(self, server):
        result = server.cellar_gaps(dimension="grape", months=6)
        assert result.metadata["dimension"] == "grape"
        assert result.metadata["months"] == 6


# ---------------------------------------------------------------------------
# TestPromotionMatchesTool — QW-5 historical match queries
# ---------------------------------------------------------------------------


class TestPromotionMatchesTool:
    """Tests for the promotion_matches MCP tool."""

    def _seed_matches(self, data_dir):
        """Write test promotion_match data to Parquet."""
        from datetime import datetime, timedelta
        from decimal import Decimal

        from cellarbrain import writer

        now = datetime.now(UTC)
        rows = [
            {
                "match_id": 1,
                "scan_time": now - timedelta(days=5),
                "retailer_id": "kapweine",
                "wine_name": "Chenin Blanc",
                "producer": "Carinus",
                "vintage": 2022,
                "sale_price": Decimal("17.90"),
                "currency": "CHF",
                "original_price": Decimal("29.00"),
                "discount_pct": 38.0,
                "match_type": "fuzzy",
                "match_category": "rebuy",
                "confidence": 0.85,
                "wine_id": 1,
                "matched_wine_name": "Chenin Blanc Cellar",
                "bottles_owned": 3,
                "reference_price": Decimal("29.00"),
                "discount_vs_reference": 38.3,
                "similar_to_wine_id": None,
                "similarity_score": None,
                "value_score": 0.88,
                "gap_dimension": None,
                "gap_detail": None,
            },
            {
                "match_id": 2,
                "scan_time": now - timedelta(days=3),
                "retailer_id": "testshop",
                "wine_name": "Barolo",
                "producer": "Test",
                "vintage": 2019,
                "sale_price": Decimal("45.00"),
                "currency": "CHF",
                "original_price": None,
                "discount_pct": None,
                "match_type": "gap_fill",
                "match_category": "gap_fill",
                "confidence": 0.5,
                "wine_id": None,
                "matched_wine_name": None,
                "bottles_owned": 0,
                "reference_price": None,
                "discount_vs_reference": None,
                "similar_to_wine_id": None,
                "similarity_score": None,
                "value_score": 0.6,
                "gap_dimension": "region",
                "gap_detail": "Only 0 bottles of Piemonte",
            },
        ]
        writer.write_partitioned_parquet("promotion_match", rows, data_dir, partition_field="scan_time")

    def test_returns_table(self, server, data_dir):
        self._seed_matches(data_dir)
        result = server.promotion_matches(months=12)
        assert "Promotion Matches" in result
        assert "Chenin Blanc" in result

    def test_filter_by_category(self, server, data_dir):
        self._seed_matches(data_dir)
        result = server.promotion_matches(months=12, category="gap_fill")
        assert "Barolo" in result
        assert "Chenin Blanc" not in result

    def test_filter_by_min_score(self, server, data_dir):
        self._seed_matches(data_dir)
        result = server.promotion_matches(months=12, min_score=0.7)
        assert "Chenin Blanc" in result
        assert "Barolo" not in result

    def test_empty_results(self, server, data_dir):
        # No seeded data
        result = server.promotion_matches(months=1)
        assert "No promotion matches" in result


# ---------------------------------------------------------------------------
# TestPromotionHistoryTool — QW-5 month-by-month trends
# ---------------------------------------------------------------------------


class TestPromotionHistoryTool:
    """Tests for the promotion_history MCP tool."""

    def _seed_matches(self, data_dir):
        """Write promotion_match data spanning multiple months."""
        from datetime import datetime, timedelta
        from decimal import Decimal

        from cellarbrain import writer

        now = datetime.now(UTC)
        rows = [
            {
                "match_id": 1,
                "scan_time": now - timedelta(days=35),
                "retailer_id": "kapweine",
                "wine_name": "Wine A",
                "producer": "Producer A",
                "vintage": 2020,
                "sale_price": Decimal("20.00"),
                "currency": "CHF",
                "original_price": None,
                "discount_pct": None,
                "match_type": "fuzzy",
                "match_category": "rebuy",
                "confidence": 0.9,
                "wine_id": 1,
                "matched_wine_name": "Wine A Cellar",
                "bottles_owned": 2,
                "reference_price": Decimal("30.00"),
                "discount_vs_reference": 33.3,
                "similar_to_wine_id": None,
                "similarity_score": None,
                "value_score": 0.85,
                "gap_dimension": None,
                "gap_detail": None,
            },
            {
                "match_id": 2,
                "scan_time": now - timedelta(days=3),
                "retailer_id": "testshop",
                "wine_name": "Wine B",
                "producer": "Producer B",
                "vintage": 2021,
                "sale_price": Decimal("35.00"),
                "currency": "CHF",
                "original_price": None,
                "discount_pct": None,
                "match_type": "similar",
                "match_category": "similar",
                "confidence": 0.6,
                "wine_id": None,
                "matched_wine_name": "Wine B Similar",
                "bottles_owned": 0,
                "reference_price": None,
                "discount_vs_reference": None,
                "similar_to_wine_id": 5,
                "similarity_score": 0.55,
                "value_score": 0.5,
                "gap_dimension": None,
                "gap_detail": None,
            },
        ]
        writer.write_partitioned_parquet("promotion_match", rows, data_dir, partition_field="scan_time")

    def test_returns_monthly_summary(self, server, data_dir):
        self._seed_matches(data_dir)
        result = server.promotion_history(months=12)
        assert "Promotion History" in result
        # The months will be based on current date - just check structure
        assert "Month" in result

    def test_category_counts(self, server, data_dir):
        self._seed_matches(data_dir)
        result = server.promotion_history(months=12)
        # Both months should appear with category info
        assert "Month" in result
        assert "Re-buy" in result

    def test_empty_history(self, server, data_dir):
        result = server.promotion_history(months=1)
        assert "No promotion matches" in result


# ---------------------------------------------------------------------------
# TestStructuredOutputValidation — regression for issue #007
# ---------------------------------------------------------------------------


class TestStructuredOutputValidation:
    """Ensure tools don't fail MCP output Pydantic validation (issue #007).

    MCP library >=1.27 auto-generates output models from return annotations.
    Our tools return CallToolResult with custom structuredContent which doesn't
    match the auto-generated ``{"result": ...}`` schema.  Setting
    ``structured_output=False`` disables this validation.
    """

    def test_tools_have_no_output_schema(self):
        """All registered tools must have output_schema=None (structured_output=False)."""
        from cellarbrain.mcp_server import mcp

        tools = mcp._tool_manager._tools
        assert len(tools) > 0, "No tools registered"
        for name, tool in tools.items():
            assert tool.fn_metadata.output_schema is None, (
                f"Tool {name!r} has an output_schema — will cause Pydantic "
                f"validation errors on the MCP wire. Set structured_output=False."
            )

    def test_cellar_stats_wire_conversion(self, server):
        """cellar_stats returns valid CallToolResult through wire wrapper."""
        from mcp.types import CallToolResult

        from cellarbrain.mcp_responses import to_call_tool_result

        result = server.cellar_stats()
        wire = to_call_tool_result(result)
        assert isinstance(wire, CallToolResult)
        assert wire.structuredContent is not None
        assert "data" in wire.structuredContent

    def test_find_wine_wire_conversion(self, server):
        """find_wine returns valid CallToolResult through wire wrapper."""
        from mcp.types import CallToolResult

        from cellarbrain.mcp_responses import to_call_tool_result

        result = server.find_wine(query="Test")
        wire = to_call_tool_result(result)
        assert isinstance(wire, CallToolResult)
        assert wire.structuredContent is not None
        assert "data" in wire.structuredContent

    def test_reload_data_error_wire_conversion(self, server):
        """reload_data error path returns valid CallToolResult through wire wrapper."""
        from mcp.types import CallToolResult

        from cellarbrain.mcp_responses import to_call_tool_result

        # Will error because CSV files don't exist
        result = server.reload_data()
        wire = to_call_tool_result(result)
        assert isinstance(wire, CallToolResult)
        assert wire.structuredContent is not None
        assert "data" in wire.structuredContent
