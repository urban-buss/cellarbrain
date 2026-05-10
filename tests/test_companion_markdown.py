"""Tests for companion_markdown module."""

from __future__ import annotations

from datetime import datetime

from cellarbrain.companion_markdown import (
    _extract_agent_sections,
    companion_dossier_slug,
    generate_companion_dossiers,
    render_companion_dossier,
    render_price_tracker_section,
)
from cellarbrain.settings import Settings
from cellarbrain.slugify import companion_slug

_NOW = datetime(2026, 1, 15, 10, 0, 0)


# ---------------------------------------------------------------------------
# Slug helpers
# ---------------------------------------------------------------------------


class TestMakeSlug:
    def test_simple(self):
        assert companion_slug("Château Margaux", "Grand Vin") == "chateau-margaux-grand-vin"

    def test_accents_stripped(self):
        assert companion_slug("Château Phélan Ségur", None) == "chateau-phelan-segur"

    def test_none_winery(self):
        assert companion_slug(None, "Test Wine") == "test-wine"

    def test_both_none(self):
        assert companion_slug(None, None) == ""

    def test_max_length(self):
        slug = companion_slug("Very Long Winery Name", "Very Long Wine Name", slug_max_length=20)
        assert len(slug) <= 20


class TestCompanionDossierSlug:
    def test_format(self):
        result = companion_dossier_slug(90_001, "Château Margaux", "Grand Vin")
        assert result == "90001-chateau-margaux-grand-vin.md"

    def test_id_padding(self):
        result = companion_dossier_slug(42, "Test", "Wine")
        assert result.startswith("00042-")

    def test_large_id(self):
        result = companion_dossier_slug(9999, "Test", "Wine")
        assert result.startswith("09999-")


# ---------------------------------------------------------------------------
# Agent section extraction
# ---------------------------------------------------------------------------


class TestExtractAgentSections:
    def test_empty_content(self):
        assert _extract_agent_sections("") == {}

    def test_populated_section(self):
        content = (
            "## Producer Deep Dive\n"
            "<!-- source: agent:research -->\n"
            "Some research content.\n"
            "<!-- source: agent:research — end -->\n"
        )
        result = _extract_agent_sections(content)
        assert "Producer Deep Dive" in result
        assert "Some research content." in result["Producer Deep Dive"]

    def test_multiple_sections(self):
        content = (
            "## Producer Deep Dive\n"
            "<!-- source: agent:research -->\n"
            "Deep dive content.\n"
            "<!-- source: agent:research — end -->\n"
            "\n"
            "## Vintage Tracker\n"
            "<!-- source: agent:research -->\n"
            "Vintage content.\n"
            "<!-- source: agent:research — end -->\n"
        )
        result = _extract_agent_sections(content)
        assert len(result) == 2
        assert "Producer Deep Dive" in result
        assert "Vintage Tracker" in result

    def test_pending_section_not_extracted(self):
        content = "## Producer Deep Dive\nSome text without agent fences.\n"
        result = _extract_agent_sections(content)
        assert "Producer Deep Dive" not in result


# ---------------------------------------------------------------------------
# Render companion dossier
# ---------------------------------------------------------------------------


def _tracked_wine(
    tracked_wine_id: int = 90_001,
    winery_id: int = 1,
    wine_name: str = "Grand Vin",
    category: str = "red",
    appellation_id: int | None = 1,
    is_deleted: bool = False,
    updated_at: datetime = _NOW,
) -> dict:
    return {
        "tracked_wine_id": tracked_wine_id,
        "winery_id": winery_id,
        "wine_name": wine_name,
        "category": category,
        "appellation_id": appellation_id,
        "dossier_path": f"tracked/{tracked_wine_id:05d}-test.md",
        "is_deleted": is_deleted,
        "etl_run_id": 1,
        "updated_at": updated_at,
    }


def _related_wine(wine_id: int = 1, vintage: int = 2020) -> dict:
    return {
        "wine_id": wine_id,
        "vintage": vintage,
        "is_deleted": False,
        "tracked_wine_id": 90_001,
    }


class TestRenderCompanionDossier:
    def test_basic_render(self):
        tw = _tracked_wine()
        wines = [_related_wine(1, 2020), _related_wine(2, 2018)]
        appellation = {"country": "France", "region": "Bordeaux", "classification": "1er Grand Cru Classé"}
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Château Margaux", appellation, settings)

        assert "---\n" in result
        assert "tracked_wine_id: 90001\n" in result
        assert 'winery: "Château Margaux"\n' in result
        assert 'wine_name: "Grand Vin"\n' in result
        assert 'category: "red"\n' in result
        assert "is_active: true\n" in result
        assert "# Château Margaux Grand Vin — Companion Dossier\n" in result

    def test_frontmatter_has_related_ids(self):
        tw = _tracked_wine()
        wines = [_related_wine(10, 2020), _related_wine(20, 2018)]
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Test", None, settings)
        assert "  - 10\n" in result
        assert "  - 20\n" in result

    def test_frontmatter_has_vintages(self):
        tw = _tracked_wine()
        wines = [_related_wine(1, 2020), _related_wine(2, 2018)]
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Test", None, settings)
        assert "  - 2018\n" in result
        assert "  - 2020\n" in result

    def test_inactive_tracked_wine(self):
        tw = _tracked_wine(is_deleted=True)
        settings = Settings()

        result = render_companion_dossier(tw, [], "Test", None, settings)
        assert "is_active: false\n" in result

    def test_agent_sections_present(self):
        tw = _tracked_wine()
        settings = Settings()

        result = render_companion_dossier(tw, [], "Test", None, settings)
        assert "## Producer Deep Dive\n" in result
        assert "## Vintage Tracker\n" in result
        assert "## Buying Guide\n" in result
        assert "## Price Tracker\n" in result

    def test_pending_sections_have_fences(self):
        tw = _tracked_wine()
        settings = Settings()

        result = render_companion_dossier(tw, [], "Test", None, settings)
        assert "<!-- source: agent:research -->" in result
        assert "<!-- source: agent:research — end -->" in result
        assert "*Not yet researched. Pending agent action.*" in result


# ---------------------------------------------------------------------------
# Price tracker section
# ---------------------------------------------------------------------------


class TestRenderPriceTrackerSection:
    def test_no_data(self, tmp_path):
        from cellarbrain import writer

        now = datetime(2026, 1, 1)
        # Need a tracked_wine + core data for the connection
        writer.write_parquet(
            "tracked_wine",
            [
                {
                    "tracked_wine_id": 90_001,
                    "winery_id": 1,
                    "wine_name": "Test",
                    "category": "red",
                    "appellation_id": None,
                    "dossier_path": "tracked/90001-test.md",
                    "is_deleted": False,
                    "etl_run_id": 1,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        result = render_price_tracker_section(90_001, tmp_path)
        assert "No price observations" in result

    def test_with_data(self, tmp_path):
        from decimal import Decimal

        from cellarbrain import writer
        from cellarbrain.query import log_price

        now = datetime(2026, 1, 1)
        rid = 1

        # Minimal dataset for connection
        writer.write_parquet(
            "winery",
            [
                {"winery_id": 1, "name": "W", "etl_run_id": rid, "updated_at": now},
            ],
            tmp_path,
        )
        writer.write_parquet(
            "appellation",
            [
                {
                    "appellation_id": 1,
                    "country": "France",
                    "region": "Bordeaux",
                    "subregion": None,
                    "classification": None,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "grape",
            [
                {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
            ],
            tmp_path,
        )
        writer.write_parquet(
            "wine",
            [
                {
                    "wine_id": 1,
                    "wine_slug": "w-test-2020",
                    "winery_id": 1,
                    "name": "Test",
                    "vintage": 2020,
                    "is_non_vintage": False,
                    "appellation_id": 1,
                    "category": "red",
                    "_raw_classification": None,
                    "subcategory": None,
                    "specialty": None,
                    "sweetness": None,
                    "effervescence": None,
                    "volume_ml": 750,
                    "_raw_volume": None,
                    "container": None,
                    "hue": None,
                    "cork": None,
                    "alcohol_pct": None,
                    "acidity_g_l": None,
                    "sugar_g_l": None,
                    "ageing_type": None,
                    "ageing_months": None,
                    "farming_type": None,
                    "serving_temp_c": None,
                    "opening_type": None,
                    "opening_minutes": None,
                    "drink_from": None,
                    "drink_until": None,
                    "optimal_from": None,
                    "optimal_until": None,
                    "original_list_price": None,
                    "original_list_currency": None,
                    "list_price": None,
                    "list_currency": None,
                    "comment": None,
                    "winemaking_notes": None,
                    "is_favorite": False,
                    "is_wishlist": False,
                    "tracked_wine_id": 90_001,
                    "full_name": "W Test 2020",
                    "grape_type": "varietal",
                    "primary_grape": "Merlot",
                    "grape_summary": "Merlot",
                    "_raw_grapes": None,
                    "dossier_path": "cellar/0001-w-test-2020.md",
                    "drinking_status": "unknown",
                    "age_years": 6,
                    "price_tier": "unknown",
                    "bottle_format": "Standard",
                    "price_per_750ml": None,
                    "format_group_id": None,
                    "food_tags": None,
                    "is_deleted": False,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "wine_grape",
            [
                {
                    "wine_id": 1,
                    "grape_id": 1,
                    "percentage": 100.0,
                    "sort_order": 1,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "bottle",
            [
                {
                    "bottle_id": 1,
                    "wine_id": 1,
                    "status": "stored",
                    "cellar_id": 1,
                    "shelf": "A1",
                    "bottle_number": 1,
                    "provider_id": 1,
                    "purchase_date": now.date(),
                    "acquisition_type": "market_price",
                    "original_purchase_price": Decimal("20"),
                    "original_purchase_currency": "CHF",
                    "purchase_price": Decimal("20"),
                    "purchase_currency": "CHF",
                    "purchase_comment": None,
                    "output_date": None,
                    "output_type": None,
                    "output_comment": None,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "cellar",
            [
                {
                    "cellar_id": 1,
                    "name": "Main",
                    "location_type": "onsite",
                    "sort_order": 1,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )
        writer.write_parquet(
            "provider",
            [
                {"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now},
            ],
            tmp_path,
        )
        writer.write_parquet("tasting", [], tmp_path)
        writer.write_parquet("pro_rating", [], tmp_path)
        writer.write_parquet(
            "tracked_wine",
            [
                {
                    "tracked_wine_id": 90_001,
                    "winery_id": 1,
                    "wine_name": "Test",
                    "category": "red",
                    "appellation_id": 1,
                    "dossier_path": "tracked/90001-test.md",
                    "is_deleted": False,
                    "etl_run_id": rid,
                    "updated_at": now,
                },
            ],
            tmp_path,
        )

        log_price(
            tmp_path,
            {
                "tracked_wine_id": 90_001,
                "vintage": 2020,
                "bottle_size_ml": 750,
                "retailer_name": "Shop A",
                "price": Decimal("45.00"),
                "currency": "CHF",
                "in_stock": True,
                "observed_at": datetime(2026, 4, 7, 12, 0),
                "observation_source": "agent",
            },
        )

        result = render_price_tracker_section(90_001, tmp_path)
        assert "Shop A" in result

    def test_preserves_existing_agent_content(self):
        tw = _tracked_wine()
        existing = (
            "---\ntracked_wine_id: 90001\n---\n\n"
            "# Test — Companion Dossier\n\n"
            "## Producer Deep Dive\n"
            "<!-- source: agent:research -->\n"
            "Preserved research content.\n"
            "<!-- source: agent:research — end -->\n"
        )
        settings = Settings()

        result = render_companion_dossier(
            tw,
            [],
            "Test",
            None,
            settings,
            existing_content=existing,
        )
        assert "Preserved research content." in result
        assert "Producer Deep Dive" in result

    def test_all_pending_when_no_existing(self):
        tw = _tracked_wine()
        settings = Settings()

        result = render_companion_dossier(tw, [], "Test", None, settings)
        assert "agent_sections_pending:" in result
        assert "  - producer_deep_dive\n" in result
        assert "agent_sections_populated: []\n" in result

    def test_placeholder_sections_remain_pending_on_rerender(self):
        """Regression: placeholder scaffolding must not flip sections to populated."""
        tw = _tracked_wine()
        settings = Settings()

        first_render = render_companion_dossier(tw, [], "Test", None, settings)
        second_render = render_companion_dossier(
            tw,
            [],
            "Test",
            None,
            settings,
            existing_content=first_render,
        )
        assert "agent_sections_populated: []\n" in second_render
        assert "  - producer_deep_dive\n" in second_render
        assert "  - vintage_tracker\n" in second_render
        assert "  - buying_guide\n" in second_render
        assert "  - price_tracker\n" in second_render

    def test_populated_preserved_after_agent_write(self):
        """Sections marked populated in frontmatter stay populated on re-render."""
        tw = _tracked_wine()
        existing = (
            "---\n"
            "tracked_wine_id: 90001\n"
            "agent_sections_populated:\n"
            "  - producer_deep_dive\n"
            "agent_sections_pending:\n"
            "  - vintage_tracker\n"
            "  - buying_guide\n"
            "  - price_tracker\n"
            "---\n\n"
            "# Test — Companion Dossier\n\n"
            "## Producer Deep Dive\n"
            "<!-- source: agent:research -->\n"
            "Real research content written by an agent.\n"
            "<!-- source: agent:research — end -->\n"
        )
        settings = Settings()

        result = render_companion_dossier(
            tw,
            [],
            "Test",
            None,
            settings,
            existing_content=existing,
        )
        assert "agent_sections_populated:\n  - producer_deep_dive\n" in result
        assert "  - vintage_tracker\n" in result
        assert "  - buying_guide\n" in result
        assert "  - price_tracker\n" in result
        assert "Real research content written by an agent." in result

    def test_vintages_table(self):
        tw = _tracked_wine()
        wines = [_related_wine(1, 2020), _related_wine(2, 2018)]
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Test", None, settings)
        assert "## Vintages in Cellar" in result
        assert "| 2018 | 2 |" in result
        assert "| 2020 | 1 |" in result


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------


class TestGenerateCompanionDossiers:
    def test_generates_files(self, tmp_path):
        entities = {
            "tracked_wine": [_tracked_wine(90_001, 1, "Grand Vin")],
            "wine": [
                {**_related_wine(1, 2020), "winery_id": 1, "name": "Grand Vin"},
            ],
            "winery": [{"winery_id": 1, "name": "Château Test"}],
            "appellation": [
                {"appellation_id": 1, "country": "France", "region": "Bordeaux", "classification": None},
            ],
        }
        settings = Settings()
        out = tmp_path / "wines"
        out.mkdir()

        paths = generate_companion_dossiers(entities, tmp_path, settings)
        assert len(paths) == 1
        assert paths[0].exists()
        content = paths[0].read_text(encoding="utf-8")
        assert "tracked_wine_id: 90001" in content

    def test_skips_deleted_tracked_wines(self, tmp_path):
        entities = {
            "tracked_wine": [_tracked_wine(90_001, is_deleted=True)],
            "wine": [],
            "winery": [{"winery_id": 1, "name": "Test"}],
            "appellation": [],
        }
        settings = Settings()
        (tmp_path / "wines").mkdir()

        paths = generate_companion_dossiers(entities, tmp_path, settings)
        assert len(paths) == 0

    def test_empty_tracked_wines(self, tmp_path):
        entities = {"tracked_wine": [], "wine": [], "winery": [], "appellation": []}
        settings = Settings()

        paths = generate_companion_dossiers(entities, tmp_path, settings)
        assert paths == []

    def test_preserves_existing_content(self, tmp_path):
        # Create existing dossier with agent content
        out = tmp_path / "wines" / "tracked"
        out.mkdir(parents=True)
        existing = out / "90001-chateau-test-grand-vin.md"
        existing.write_text(
            "---\ntracked_wine_id: 90001\n---\n\n"
            "# Château Test Grand Vin — Companion Dossier\n\n"
            "## Producer Deep Dive\n"
            "<!-- source: agent:research -->\n"
            "Existing research.\n"
            "<!-- source: agent:research — end -->\n",
            encoding="utf-8",
        )

        entities = {
            "tracked_wine": [_tracked_wine(90_001, 1, "Grand Vin")],
            "wine": [{**_related_wine(1, 2020), "winery_id": 1, "name": "Grand Vin"}],
            "winery": [{"winery_id": 1, "name": "Château Test"}],
            "appellation": [{"appellation_id": 1, "country": "France", "region": "Bordeaux", "classification": None}],
        }
        settings = Settings()

        paths = generate_companion_dossiers(entities, tmp_path, settings)
        assert len(paths) == 1
        content = paths[0].read_text(encoding="utf-8")
        assert "Existing research." in content
