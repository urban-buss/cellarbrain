"""Tests for cellarbrain.mcp_server — FastMCP server tools, resources, and prompts."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from cellarbrain import markdown, writer
from cellarbrain.markdown import dossier_filename


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime(2025, 1, 1)


def _make_dataset(tmp_path):
    """Write a minimal Parquet + dossier dataset for MCP tests."""
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Château MCP", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1, "country": "France", "region": "Bordeaux",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1, "wine_slug": "chateau-mcp-test-cuvee-2020",
            "winery_id": 1, "name": "Test Cuvée",
            "vintage": 2020, "is_non_vintage": False, "appellation_id": 1,
            "category": "Red wine",
            "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 14.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2024, "drink_until": 2030,
            "optimal_from": 2025, "optimal_until": 2028,
            "original_list_price": None, "original_list_currency": None,
            "list_price": None, "list_currency": None,
            "comment": None, "winemaking_notes": None,
            "is_favorite": True, "is_wishlist": False,
            "tracked_wine_id": 90_001,
            "full_name": "Château MCP Test Cuvée 2020",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": f"cellar/{dossier_filename(1, 'Château MCP', 'Test Cuvée', 2020, False)}",
            "drinking_status": "optimal",
            "age_years": 5,
            "price_tier": "unknown",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        {
            "bottle_id": 1, "wine_id": 1, "status": "stored",
            "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
            "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("30.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("30.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": None, "output_type": None, "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    cellars = [
        {"cellar_id": 1, "name": "Cave", "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    providers = [
        {"provider_id": 1, "name": "Vendor", "etl_run_id": rid, "updated_at": now},
    ]
    etl_runs = [
        {
            "run_id": 1, "started_at": now, "finished_at": now,
            "run_type": "full", "wines_source_hash": "abc",
            "bottles_source_hash": "def", "bottles_gone_source_hash": None,
            "total_inserts": 3, "total_updates": 0, "total_deletes": 0,
            "wines_inserted": 1, "wines_updated": 0,
            "wines_deleted": 0, "wines_renamed": 0,
        },
    ]
    change_logs = [
        {
            "change_id": 1, "run_id": 1, "entity_type": "wine",
            "entity_id": 1, "change_type": "insert", "changed_fields": None,
        },
    ]

    entities = {
        "winery": wineries,
        "appellation": appellations,
        "grape": grapes,
        "wine": wines,
        "wine_grape": wine_grapes,
        "bottle": bottles,
        "cellar": cellars,
        "provider": providers,
        "tasting": [],
        "pro_rating": [],
    }

    for name, rows in entities.items():
        writer.write_parquet(name, rows, tmp_path)
    writer.write_parquet("etl_run", etl_runs, tmp_path)
    writer.write_parquet("change_log", change_logs, tmp_path)

    # Generate dossiers
    markdown.generate_dossiers(entities, tmp_path, current_year=2025)

    # Generate tracked wines and companion dossiers
    from cellarbrain import companion_markdown
    from cellarbrain.settings import Settings

    settings = Settings()
    slug = companion_markdown.companion_dossier_slug(
        90_001, "Château MCP", "Test Cuvée",
    )
    tracked_wines = [
        {
            "tracked_wine_id": 90_001, "winery_id": 1, "wine_name": "Test Cuvée",
            "category": "Red wine", "appellation_id": 1,
            "dossier_path": f"tracked/{slug}",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
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
    from cellarbrain import mcp_server
    mcp_server._mcp_settings = None  # force re-read from env
    return mcp_server


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


# ---------------------------------------------------------------------------
# TestSommelierErrors
# ---------------------------------------------------------------------------


class TestSommelierErrors:
    @pytest.fixture(autouse=True)
    def _force_missing_model(self, server):
        """Reset engine cache and point model_dir to a nonexistent path."""
        server._sommelier_engine = None
        server._food_catalogue_meta = None
        original = server._load_mcp_settings

        def _patched():
            s = original()
            object.__setattr__(s, "sommelier", type(s.sommelier)(
                model_dir="nonexistent/model",
            ))
            return s

        server._load_mcp_settings = _patched
        server._mcp_settings = None
        yield
        server._load_mcp_settings = original
        server._mcp_settings = None
        server._sommelier_engine = None
        server._food_catalogue_meta = None

    def test_suggest_wines_returns_not_trained(self, server):
        result = asyncio.run(server.suggest_wines(food_query="grilled lamb"))
        assert result.startswith("Error:")

    def test_suggest_foods_returns_not_trained(self, server):
        result = asyncio.run(server.suggest_foods(wine_id=1))
        assert result.startswith("Error:")


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
            object.__setattr__(s, "sommelier", type(s.sommelier)(
                pairing_dataset=str(self._dataset_path),
            ))
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
