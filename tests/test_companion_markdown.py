"""Tests for companion_markdown module."""

from __future__ import annotations

from datetime import datetime

from cellarbrain.companion_markdown import (
    VintageStats,
    _aggregate_vintage_stats,
    _extract_agent_sections,
    _select_best_value,
    _suggest_drink_order,
    _vintage_trend,
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


def _related_wine(
    wine_id: int = 1,
    vintage: int = 2020,
    drinking_status: str = "unknown",
    list_price: float | None = None,
    list_currency: str | None = None,
    price_per_750ml: float | None = None,
    tracked_wine_id: int = 90_001,
) -> dict:
    return {
        "wine_id": wine_id,
        "vintage": vintage,
        "is_deleted": False,
        "tracked_wine_id": tracked_wine_id,
        "drinking_status": drinking_status,
        "list_price": list_price,
        "list_currency": list_currency,
        "price_per_750ml": price_per_750ml,
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
        assert "## Vintage Comparison" in result
        assert "| 2018 |" in result
        assert "| 2020 |" in result


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

    def test_renders_vintage_comparison_with_entity_data(self, tmp_path):
        """generate_companion_dossiers uses tasting/pro_rating/bottle entities."""
        entities = {
            "tracked_wine": [_tracked_wine(90_001, 1, "Grand Vin")],
            "wine": [
                {**_related_wine(1, 2020, "optimal", 45.0, "CHF", 45.0), "winery_id": 1, "name": "GV"},
                {**_related_wine(2, 2021, "drinkable", 42.0, "CHF", 42.0), "winery_id": 1, "name": "GV"},
            ],
            "winery": [{"winery_id": 1, "name": "Château Test"}],
            "appellation": [],
            "pro_rating": [
                {"wine_id": 1, "source": "Parker", "score": 94.0, "max_score": 100.0},
            ],
            "tasting": [
                {"wine_id": 2, "score": 17.0, "max_score": 20.0},
            ],
            "bottle": [
                {"wine_id": 1, "status": "stored", "is_in_transit": False},
                {"wine_id": 1, "status": "stored", "is_in_transit": False},
                {"wine_id": 2, "status": "consumed", "is_in_transit": False},
            ],
        }
        settings = Settings()
        (tmp_path / "wines").mkdir()

        paths = generate_companion_dossiers(entities, tmp_path, settings)
        content = paths[0].read_text(encoding="utf-8")
        assert "## Vintage Comparison" in content
        assert "94/100" in content
        assert "17/20" in content
        assert "| 2020 | optimal | 94/100 | CHF 45 | 2 |" in content


# ---------------------------------------------------------------------------
# Vintage comparison helpers
# ---------------------------------------------------------------------------


class TestAggregateVintageStats:
    def test_basic(self):
        wines = [_related_wine(1, 2020, "optimal", 45.0, "CHF", 45.0)]
        tastings = {1: [{"score": 17.0, "max_score": 20.0}]}
        ratings = {1: [{"score": 94.0, "max_score": 100.0}]}
        bottles = {1: 3}

        result = _aggregate_vintage_stats(wines, tastings, ratings, bottles)
        assert len(result) == 1
        s = result[0]
        assert s.vintage == 2020
        assert s.best_score == 94.0
        assert s.best_score_source == "pro"
        assert s.bottles_stored == 3
        assert s.drinking_status == "optimal"

    def test_prefers_pro_over_personal(self):
        wines = [_related_wine(1, 2020)]
        tastings = {1: [{"score": 18.0, "max_score": 20.0}]}
        ratings = {1: [{"score": 92.0, "max_score": 100.0}]}

        result = _aggregate_vintage_stats(wines, tastings, ratings, {})
        assert result[0].best_score == 92.0
        assert result[0].best_score_source == "pro"

    def test_falls_back_to_personal(self):
        wines = [_related_wine(1, 2020)]
        tastings = {1: [{"score": 16.5, "max_score": 20.0}]}

        result = _aggregate_vintage_stats(wines, tastings, {}, {})
        assert result[0].best_score == 16.5
        assert result[0].best_score_source == "personal"

    def test_no_scores(self):
        wines = [_related_wine(1, 2020)]
        result = _aggregate_vintage_stats(wines, {}, {}, {})
        assert result[0].best_score is None
        assert result[0].best_score_source is None

    def test_multiple_vintages_sorted(self):
        wines = [_related_wine(2, 2021), _related_wine(1, 2019)]
        result = _aggregate_vintage_stats(wines, {}, {}, {})
        assert result[0].vintage == 2019
        assert result[1].vintage == 2021

    def test_max_pro_score_selected(self):
        wines = [_related_wine(1, 2020)]
        ratings = {
            1: [
                {"score": 89.0, "max_score": 100.0},
                {"score": 93.0, "max_score": 100.0},
                {"score": 91.0, "max_score": 100.0},
            ]
        }

        result = _aggregate_vintage_stats(wines, {}, ratings, {})
        assert result[0].best_score == 93.0


class TestSelectBestValue:
    def test_basic(self):
        stats = [
            VintageStats(1, 2020, "optimal", 94.0, 100.0, "pro", 45.0, "CHF", 45.0, 3),
            VintageStats(2, 2021, "drinkable", 91.0, 100.0, "pro", 42.0, "CHF", 42.0, 2),
        ]
        result = _select_best_value(stats)
        # 2021: 91/100 / 42 = 0.02167; 2020: 94/100 / 45 = 0.02089
        assert result is not None
        assert result.vintage == 2021

    def test_none_when_no_price(self):
        stats = [
            VintageStats(1, 2020, "optimal", 94.0, 100.0, "pro", None, None, None, 3),
        ]
        assert _select_best_value(stats) is None

    def test_none_when_no_score(self):
        stats = [
            VintageStats(1, 2020, "optimal", None, None, None, 45.0, "CHF", 45.0, 3),
        ]
        assert _select_best_value(stats) is None

    def test_skips_zero_price(self):
        stats = [
            VintageStats(1, 2020, "optimal", 94.0, 100.0, "pro", 0.0, "CHF", 0.0, 1),
        ]
        assert _select_best_value(stats) is None


class TestSuggestDrinkOrder:
    def test_basic_ordering(self):
        stats = [
            VintageStats(1, 2018, "optimal", None, None, None, None, None, None, 1),
            VintageStats(2, 2020, "drinkable", None, None, None, None, None, None, 1),
            VintageStats(3, 2022, "too_young", None, None, None, None, None, None, 1),
        ]
        order, past = _suggest_drink_order(stats)
        assert order == [2018, 2020, 2022]
        assert past == []

    def test_past_window_separated(self):
        stats = [
            VintageStats(1, 2015, "past_window", None, None, None, None, None, None, 1),
            VintageStats(2, 2020, "optimal", None, None, None, None, None, None, 1),
        ]
        order, past = _suggest_drink_order(stats)
        assert order == [2020]
        assert past == [2015]

    def test_urgent_before_drinkable(self):
        stats = [
            VintageStats(1, 2020, "drinkable", None, None, None, None, None, None, 1),
            VintageStats(2, 2018, "past_optimal", None, None, None, None, None, None, 1),
        ]
        order, past = _suggest_drink_order(stats)
        assert order == [2018, 2020]

    def test_too_young_reversed(self):
        stats = [
            VintageStats(1, 2022, "too_young", None, None, None, None, None, None, 1),
            VintageStats(2, 2023, "too_young", None, None, None, None, None, None, 1),
        ]
        order, _ = _suggest_drink_order(stats)
        # Newest first among too_young (they will mature later)
        assert order == [2023, 2022]

    def test_empty_stats(self):
        order, past = _suggest_drink_order([])
        assert order == []
        assert past == []


class TestVintageTrend:
    def test_improving(self):
        stats = [
            VintageStats(1, 2018, None, 88.0, 100.0, "pro", None, None, None, 0),
            VintageStats(2, 2019, None, 91.0, 100.0, "pro", None, None, None, 0),
            VintageStats(3, 2020, None, 95.0, 100.0, "pro", None, None, None, 0),
        ]
        assert _vintage_trend(stats) == "improving"

    def test_declining(self):
        stats = [
            VintageStats(1, 2018, None, 95.0, 100.0, "pro", None, None, None, 0),
            VintageStats(2, 2019, None, 91.0, 100.0, "pro", None, None, None, 0),
            VintageStats(3, 2020, None, 87.0, 100.0, "pro", None, None, None, 0),
        ]
        assert _vintage_trend(stats) == "declining"

    def test_stable(self):
        stats = [
            VintageStats(1, 2018, None, 92.0, 100.0, "pro", None, None, None, 0),
            VintageStats(2, 2019, None, 92.0, 100.0, "pro", None, None, None, 0),
            VintageStats(3, 2020, None, 92.0, 100.0, "pro", None, None, None, 0),
        ]
        assert _vintage_trend(stats) == "stable"

    def test_none_with_fewer_than_3(self):
        stats = [
            VintageStats(1, 2018, None, 92.0, 100.0, "pro", None, None, None, 0),
            VintageStats(2, 2019, None, 95.0, 100.0, "pro", None, None, None, 0),
        ]
        assert _vintage_trend(stats) is None

    def test_none_with_no_scores(self):
        stats = [
            VintageStats(1, 2018, None, None, None, None, None, None, None, 0),
            VintageStats(2, 2019, None, None, None, None, None, None, None, 0),
            VintageStats(3, 2020, None, None, None, None, None, None, None, 0),
        ]
        assert _vintage_trend(stats) is None


class TestVintageComparisonMatrix:
    """Integration tests for the full Vintage Comparison section in render_companion_dossier."""

    def test_happy_path_multi_vintage(self):
        tw = _tracked_wine()
        wines = [
            _related_wine(1, 2019, "optimal", 45.0, "CHF", 45.0),
            _related_wine(2, 2020, "drinkable", 42.0, "CHF", 42.0),
            _related_wine(3, 2021, "too_young", 40.0, "CHF", 40.0),
        ]
        settings = Settings()
        ratings = {
            1: [{"score": 94.0, "max_score": 100.0}],
            2: [{"score": 91.0, "max_score": 100.0}],
            3: [{"score": 88.0, "max_score": 100.0}],
        }
        tastings = {2: [{"score": 17.5, "max_score": 20.0}]}
        bottles = {1: 3, 2: 2}

        result = render_companion_dossier(
            tw,
            wines,
            "Test",
            None,
            settings,
            tastings_by_wine=tastings,
            pro_ratings_by_wine=ratings,
            bottles_by_wine=bottles,
        )
        assert "## Vintage Comparison" in result
        assert "| 2019 | optimal | 94/100 | CHF 45 | 3 |" in result
        assert "91/100" in result
        assert "**Best value:**" in result
        assert "**Suggested drink order:**" in result
        assert "**Vintage trend:**" in result

    def test_single_vintage_no_trend(self):
        tw = _tracked_wine()
        wines = [_related_wine(1, 2020, "optimal", 50.0, "CHF", 50.0)]
        settings = Settings()
        ratings = {1: [{"score": 92.0, "max_score": 100.0}]}

        result = render_companion_dossier(
            tw,
            wines,
            "Test",
            None,
            settings,
            pro_ratings_by_wine=ratings,
        )
        assert "## Vintage Comparison" in result
        assert "| 2020 | optimal | 92/100 | CHF 50 |" in result
        # Only one vintage — no trend
        assert "**Vintage trend:**" not in result
        # Single vintage with score+price still gets best value
        assert "**Best value:**" in result

    def test_no_scores(self):
        tw = _tracked_wine()
        wines = [
            _related_wine(1, 2019, "drinkable", 45.0, "CHF", 45.0),
            _related_wine(2, 2020, "drinkable", 42.0, "CHF", 42.0),
        ]
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Test", None, settings)
        assert "## Vintage Comparison" in result
        # Score column shows dashes
        assert "| 2019 | drinkable | — | CHF 45 |" in result
        # No best value without scores
        assert "**Best value:**" not in result
        assert "**Vintage trend:**" not in result

    def test_no_prices(self):
        tw = _tracked_wine()
        wines = [_related_wine(1, 2020, "optimal"), _related_wine(2, 2021, "drinkable")]
        settings = Settings()
        ratings = {1: [{"score": 93.0, "max_score": 100.0}], 2: [{"score": 90.0, "max_score": 100.0}]}

        result = render_companion_dossier(
            tw,
            wines,
            "Test",
            None,
            settings,
            pro_ratings_by_wine=ratings,
        )
        # Price column shows dashes
        assert "| 2020 | optimal | 93/100 | — |" in result
        # No best value without prices
        assert "**Best value:**" not in result
        # Drink order still works
        assert "**Suggested drink order:**" in result

    def test_past_window_warning(self):
        tw = _tracked_wine()
        wines = [
            _related_wine(1, 2015, "past_window"),
            _related_wine(2, 2020, "optimal"),
        ]
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Test", None, settings)
        assert "> ⚠️ Past window: 2015" in result
        # Past window not in main drink order
        assert "**Suggested drink order:** 2020" in result

    def test_nv_vintage(self):
        tw = _tracked_wine()
        wines = [_related_wine(1, None, "drinkable")]
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Test", None, settings)
        # NV vintage gets None in the dict, but vintages list is empty so no section
        # Actually, let's use vintage=0 convention for NV...
        # The current code uses `vintage or 0` for sorting, but the table
        # only renders if `vintages` (the list of non-None vintage values) is non-empty.
        # With a None vintage, `vintages` filter would be empty.
        # This is expected — NV wines alone don't produce the comparison.
        assert "## Vintage Comparison" not in result

    def test_nv_with_real_vintages(self):
        """NV wines render alongside real vintages when real vintages exist."""
        tw = _tracked_wine()
        wines = [
            _related_wine(1, 2020, "optimal"),
            {**_related_wine(2, None, "drinkable"), "vintage": None},
        ]
        settings = Settings()

        result = render_companion_dossier(tw, wines, "Test", None, settings)
        assert "## Vintage Comparison" in result
        assert "| NV |" in result
        assert "| 2020 |" in result

    def test_empty_related_wines(self):
        tw = _tracked_wine()
        settings = Settings()

        result = render_companion_dossier(tw, [], "Test", None, settings)
        assert "## Vintage Comparison" not in result
