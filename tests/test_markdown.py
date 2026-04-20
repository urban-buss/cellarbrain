"""Unit tests for cellarbrain.markdown — dossier generation."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from cellarbrain.markdown import (
    _drinking_status,
    _extract_agent_sections,
    _extract_frontmatter_agent_fields,
    _find_existing_dossier,
    _make_slug,
    affected_wine_ids,
    dossier_filename,
    generate_dossiers,
    mark_deleted_dossiers,
    render_wine_dossier,
)

_NOW = datetime(2025, 6, 1, 12, 0, 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wine(
    wine_id: int = 1,
    winery_id: int = 1,
    name: str | None = None,
    vintage: int | None = 2020,
    *,
    is_non_vintage: bool = False,
    appellation_id: int | None = None,
    category: str = "red",
    volume_ml: int = 750,
    alcohol_pct: float | None = None,
    drink_from: int | None = None,
    drink_until: int | None = None,
    optimal_from: int | None = None,
    optimal_until: int | None = None,
    comment: str | None = None,
    list_price: Decimal | None = None,
    list_currency: str | None = None,
    original_list_price: Decimal | None = None,
    original_list_currency: str | None = None,
    is_favorite: bool = False,
    is_wishlist: bool = False,
    tracked_wine_id: int | None = None,
    full_name: str | None = None,
    grape_type: str = "unknown",
    primary_grape: str | None = None,
    grape_summary: str | None = None,
    drinking_status: str = "unknown",
    age_years: int | None = None,
    price_tier: str = "unknown",
) -> dict:
    return {
        "wine_id": wine_id, "winery_id": winery_id, "name": name,
        "vintage": vintage, "is_non_vintage": is_non_vintage,
        "appellation_id": appellation_id, "category": category,
        "subcategory": None, "specialty": None, "sweetness": None,
        "effervescence": None, "volume_ml": volume_ml, "container": None,
        "hue": None, "cork": None,
        "alcohol_pct": alcohol_pct, "acidity_g_l": None, "sugar_g_l": None,
        "ageing_type": None, "ageing_months": None, "farming_type": None,
        "serving_temp_c": None, "opening_type": None, "opening_minutes": None,
        "drink_from": drink_from, "drink_until": drink_until,
        "optimal_from": optimal_from, "optimal_until": optimal_until,
        "list_price": list_price, "list_currency": list_currency,
        "original_list_price": original_list_price, "original_list_currency": original_list_currency,
        "comment": comment, "winemaking_notes": None,
        "is_favorite": is_favorite, "is_wishlist": is_wishlist,
        "tracked_wine_id": tracked_wine_id,
        "full_name": full_name, "grape_type": grape_type,
        "primary_grape": primary_grape, "grape_summary": grape_summary,
        "drinking_status": drinking_status, "age_years": age_years,
        "price_tier": price_tier,
        "etl_run_id": 1, "updated_at": _NOW,
    }


def _render(wine: dict | None = None, **kwargs) -> str:
    """Shortcut to render a dossier with sensible defaults."""
    w = wine or _wine(**{k: v for k, v in kwargs.items() if k in _wine.__code__.co_varnames})
    return render_wine_dossier(
        wine=w,
        winery_name=kwargs.get("winery_name", "TestWinery"),
        appellation=kwargs.get("appellation"),
        grapes=kwargs.get("grapes", []),
        bottles=kwargs.get("bottles", []),
        cellar_names=kwargs.get("cellar_names", {}),
        provider_names=kwargs.get("provider_names", {}),
        tastings=kwargs.get("tastings", []),
        pro_ratings=kwargs.get("pro_ratings", []),
        current_year=kwargs.get("current_year", 2026),
        existing_content=kwargs.get("existing_content"),
    )


def _minimal_entities(
    wines: list[dict] | None = None,
    wineries: list[dict] | None = None,
) -> dict[str, list[dict]]:
    """Build a minimal entities dict for generate_dossiers."""
    return {
        "winery": wineries or [{"winery_id": 1, "name": "W", "etl_run_id": 1, "updated_at": _NOW}],
        "appellation": [],
        "grape": [],
        "wine": wines or [_wine()],
        "wine_grape": [],
        "bottle": [],
        "cellar": [],
        "provider": [],
        "tasting": [],
        "pro_rating": [],
    }


# ---------------------------------------------------------------------------
# TestMakeSlug
# ---------------------------------------------------------------------------

class TestMakeSlug:
    def test_basic_slug(self):
        assert _make_slug("Marques De Murrieta", None, 2016, False) == "marques-de-murrieta-2016"

    def test_accented_chars(self):
        assert _make_slug("Château Phélan Ségur", None, 2020, False) == "chateau-phelan-segur-2020"

    def test_unicode_apostrophe(self):
        assert _make_slug("Château d\u2019Aiguilhe", None, 2019, False) == "chateau-daiguilhe-2019"

    def test_with_wine_name(self):
        assert _make_slug("Spier", "21 Gables", 2020, False) == "spier-21-gables-2020"

    def test_non_vintage(self):
        assert _make_slug("Dom", "Brut", None, True) == "dom-brut-nv"

    def test_no_name(self):
        assert _make_slug("Spier", None, 2020, False) == "spier-2020"

    def test_no_winery(self):
        assert _make_slug(None, "Cuvée", 2020, False) == "cuvee-2020"

    def test_no_winery_no_name(self):
        assert _make_slug(None, None, 2020, False) == "2020"

    def test_truncation_at_60(self):
        long_name = "A" * 80
        slug = _make_slug(long_name, None, 2020, False)
        assert len(slug) <= 60
        assert not slug.endswith("-")

    def test_consecutive_special_chars(self):
        assert _make_slug("A & B", "C / D", 2020, False) == "a-b-c-d-2020"


# ---------------------------------------------------------------------------
# TestDossierFilename
# ---------------------------------------------------------------------------

class TestDossierFilename:
    def test_basic(self):
        assert dossier_filename(25, "Marques De Murrieta", None, 2016, False) == "0025-marques-de-murrieta-2016.md"

    def test_high_id(self):
        fn = dossier_filename(9999, "X", None, 2020, False)
        assert fn.startswith("9999-")
        assert fn.endswith(".md")

    def test_five_digit_id(self):
        fn = dossier_filename(10000, "X", None, 2020, False)
        assert fn.startswith("10000-")


# ---------------------------------------------------------------------------
# TestFindExistingDossier
# ---------------------------------------------------------------------------

class TestFindExistingDossier:
    def test_finds_by_prefix(self, tmp_path):
        d = tmp_path / "wines"
        d.mkdir()
        (d / "0001-old-slug-2020.md").write_text("content", encoding="utf-8")
        assert _find_existing_dossier(1, d) == d / "0001-old-slug-2020.md"

    def test_no_match_returns_none(self, tmp_path):
        d = tmp_path / "wines"
        d.mkdir()
        assert _find_existing_dossier(1, d) is None

    def test_searches_multiple_dirs(self, tmp_path):
        d1 = tmp_path / "cellar"
        d2 = tmp_path / "archive"
        d1.mkdir()
        d2.mkdir()
        (d2 / "0005-some-wine-2019.md").write_text("content", encoding="utf-8")
        assert _find_existing_dossier(5, d1, d2) == d2 / "0005-some-wine-2019.md"

    def test_returns_first_match(self, tmp_path):
        d1 = tmp_path / "cellar"
        d2 = tmp_path / "archive"
        d1.mkdir()
        d2.mkdir()
        (d1 / "0001-cellar-file.md").write_text("c", encoding="utf-8")
        (d2 / "0001-archive-file.md").write_text("a", encoding="utf-8")
        result = _find_existing_dossier(1, d1, d2)
        assert result == d1 / "0001-cellar-file.md"

    def test_nonexistent_dir_skipped(self, tmp_path):
        missing = tmp_path / "nope"
        assert _find_existing_dossier(1, missing) is None


# ---------------------------------------------------------------------------
# TestDrinkingStatus
# ---------------------------------------------------------------------------

class TestDrinkingStatus:
    def test_too_young(self):
        assert _drinking_status(2022, 2036, 2024, 2030, 2020) == "Too young"

    def test_drinkable_not_optimal(self):
        assert _drinking_status(2022, 2036, 2024, 2030, 2023) == "Drinkable, not yet optimal"

    def test_in_optimal_window(self):
        assert _drinking_status(2022, 2036, 2024, 2030, 2026) == "In optimal window"

    def test_past_optimal_still_drinkable(self):
        assert _drinking_status(2022, 2036, 2024, 2030, 2032) == "Past optimal, still drinkable"

    def test_past_drinking_window(self):
        assert _drinking_status(2022, 2036, 2024, 2030, 2040) == "Past drinking window"

    def test_no_data(self):
        assert _drinking_status(None, None, None, None, 2026) == "No drinking window data"

    def test_partial_data_only_drink_from(self):
        result = _drinking_status(2022, None, None, None, 2023)
        assert result == "Drinkable, not yet optimal"


# ---------------------------------------------------------------------------
# TestExtractAgentSections
# ---------------------------------------------------------------------------

class TestExtractAgentSections:
    def test_extracts_populated_section(self):
        content = (
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\n"
            "Some research content here.\n\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        result = _extract_agent_sections(content)
        assert "Producer Profile" in result
        assert "Some research content here." in result["Producer Profile"]

    def test_extracts_multiple_sections(self):
        content = (
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\nProfile.\n\n"
            "<!-- source: agent:research \u2014 end -->\n\n"
            "## Similar Wines\n"
            "<!-- source: agent:recommendation -->\n\nSimilar.\n\n"
            "<!-- source: agent:recommendation \u2014 end -->\n"
        )
        result = _extract_agent_sections(content)
        assert len(result) == 2
        assert "Profile." in result["Producer Profile"]
        assert "Similar." in result["Similar Wines"]

    def test_no_agent_sections(self):
        content = "## Identity\n\n| Field | Value |\n"
        assert _extract_agent_sections(content) == {}

    def test_preserves_content_verbatim(self):
        inner = "\n**Bold** and *italic* with `code`\n- list item\n\n"
        content = (
            "## Wine Description\n"
            f"<!-- source: agent:research -->{inner}"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        result = _extract_agent_sections(content)
        assert inner in result["Wine Description"]
    def test_does_not_cross_h2_boundaries(self):
        """Regression: agent fence must associate with its own H2 section."""
        content = (
            "## Identity\n\n| Field | Value |\n| --- | --- |\n\n"
            "## Origin\n\nSome origin text.\n\n"
            "## Ratings & Reviews\n\n"
            "### From Cellar Export\n"
            "<!-- source: etl — do not edit below this line -->\n\n"
            "*No ratings.*\n\n"
            "<!-- source: etl — end -->\n\n"
            "### From Research\n"
            "<!-- source: agent:research -->\n\n"
            "Critic scores here.\n\n"
            "<!-- source: agent:research — end -->\n"
        )
        result = _extract_agent_sections(content)
        assert "Identity" not in result
        assert "Origin" not in result
        assert "Ratings & Reviews" in result
        assert "Critic scores here." in result["Ratings & Reviews"]

    def test_mixed_sections_keyed_correctly(self):
        """All three mixed sections associate with the correct H2 heading."""
        content = (
            "## Identity\n\n| Field | Value |\n\n"
            "## Ratings & Reviews\n\n"
            "### From Research\n"
            "<!-- source: agent:research -->\n\nRR content.\n\n"
            "<!-- source: agent:research — end -->\n\n"
            "## Tasting Notes\n\n"
            "### Community Tasting Notes\n"
            "<!-- source: agent:research -->\n\nTN content.\n\n"
            "<!-- source: agent:research — end -->\n\n"
            "## Food Pairings\n\n"
            "### Recommended Pairings\n"
            "<!-- source: agent:research -->\n\nFP content.\n\n"
            "<!-- source: agent:research — end -->\n"
        )
        result = _extract_agent_sections(content)
        assert "Ratings & Reviews" in result
        assert "Tasting Notes" in result
        assert "Food Pairings" in result
        assert "RR content." in result["Ratings & Reviews"]
        assert "TN content." in result["Tasting Notes"]
        assert "FP content." in result["Food Pairings"]

# ---------------------------------------------------------------------------
# TestExtractFrontmatterAgentFields
# ---------------------------------------------------------------------------

class TestExtractFrontmatterAgentFields:
    def test_extracts_populated_list(self):
        content = (
            "---\nwine_id: 1\n"
            "agent_sections_populated:\n  - ratings_reviews\n  - food_pairings\n"
            "agent_sections_pending:\n  - producer_profile\n"
            "---\n"
        )
        result = _extract_frontmatter_agent_fields(content)
        assert result["agent_sections_populated"] == ["ratings_reviews", "food_pairings"]
        assert result["agent_sections_pending"] == ["producer_profile"]

    def test_empty_frontmatter(self):
        content = "---\nwine_id: 1\n---\n"
        result = _extract_frontmatter_agent_fields(content)
        assert result["agent_sections_populated"] == []
        assert result["agent_sections_pending"] == []

    def test_no_frontmatter(self):
        content = "# Title\nSome text\n"
        result = _extract_frontmatter_agent_fields(content)
        assert result["agent_sections_populated"] == []


# ---------------------------------------------------------------------------
# TestRenderWineDossier
# ---------------------------------------------------------------------------

class TestRenderWineDossier:
    def test_full_wine_all_data(self):
        w = _wine(
            wine_id=25, appellation_id=1, alcohol_pct=14.0,
            drink_from=2022, drink_until=2036, optimal_from=2024, optimal_until=2030,
            comment="Great wine", list_price=Decimal("39.00"), list_currency="CHF",
            original_list_price=Decimal("39.00"), original_list_currency="CHF",
        )
        app = {"appellation_id": 1, "country": "Spain", "region": "La Rioja", "subregion": "Rioja", "classification": "DOCa"}
        grapes = [{"grape_name": "Tempranillo", "percentage": 82.0, "sort_order": 1}]
        bottles = [{
            "bottle_id": 1, "wine_id": 25, "status": "stored",
            "cellar_id": 1, "shelf": "A1",
            "provider_id": 1, "purchase_date": date(2024, 12, 7),
            "original_purchase_price": Decimal("39.00"), "original_purchase_currency": "CHF",
            "purchase_price": Decimal("39.00"), "purchase_currency": "CHF",
            "acquisition_type": "market_price",
            "output_date": None, "output_type": None, "output_comment": None,
        }]
        ratings = [{"rating_id": 1, "wine_id": 25, "source": "JS", "score": 92.0, "max_score": 100, "review_text": None}]
        tastings = [{"tasting_id": 1, "wine_id": 25, "tasting_date": date(2024, 2, 21), "note": "Great!", "score": 96.0, "max_score": 100}]

        md = render_wine_dossier(
            wine=w, winery_name="Marques De Murrieta", appellation=app,
            grapes=grapes, bottles=bottles,
            cellar_names={1: "Main Cellar"}, provider_names={1: "Mövenpick"},
            tastings=tastings, pro_ratings=ratings, current_year=2026,
        )
        assert "wine_id: 25" in md
        assert "# Marques De Murrieta 2020" in md
        assert "## Identity" in md
        assert "## Origin" in md
        assert "Spain" in md
        assert "## Grapes" in md
        assert "Tempranillo" in md
        assert "82%" in md
        assert "## Characteristics" in md
        assert "14%" in md
        assert "## Drinking Window" in md
        assert "In optimal window" in md
        assert "## Cellar Inventory" in md
        assert "Mövenpick" in md
        assert "## Purchase History" in md
        assert "## Consumption History" in md
        assert "No bottles consumed yet" in md
        assert "## Owner Notes" in md
        assert "Great wine" in md
        assert "## Ratings & Reviews" in md
        assert "JS" in md
        assert "92/100" in md
        assert "## Tasting Notes" in md
        assert "Great!" in md
        assert "## Producer Profile" in md
        assert "## Agent Log" in md
        assert "Pending agent action" in md

    def test_minimal_wine_no_optional_data(self):
        w = _wine()
        md = _render(wine=w, winery_name="W")
        assert "## Identity" in md
        assert "\u2014" in md  # dashes for missing data
        assert "## Origin" not in md  # no appellation
        assert "## Grapes" not in md  # no grapes
        assert "## Owner Notes" not in md  # no comment
        assert "No bottles in cellar" in md

    def test_no_bottles_shows_empty_inventory(self):
        md = _render(bottles=[])
        assert "No bottles in cellar" in md
        assert "No purchase history" in md

    def test_non_vintage_wine(self):
        w = _wine(vintage=None, is_non_vintage=True)
        md = _render(wine=w)
        assert "vintage: null" in md
        assert "NV" in md
        assert "Skipped for non-vintage" in md

    def test_agent_sections_preserved_on_rerender(self):
        agent_content = (
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\n"
            "Detailed producer info here.\n\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        md = _render(existing_content=agent_content)
        assert "Detailed producer info here." in md
        # ETL sections still fresh
        assert "## Identity" in md

    def test_mixed_section_h3_heading_preserved_on_rerender(self):
        """Regression: H3 subheadings must survive regeneration for mixed sections."""
        existing = (
            "## Tasting Notes\n\n"
            "### Personal Tastings\n"
            "<!-- source: etl — do not edit below this line -->\n\n"
            "*No personal tastings recorded.*\n\n"
            "<!-- source: etl — end -->\n\n"
            "### Community Tasting Notes\n"
            "<!-- source: agent:research -->\n\n"
            "Preserved TN content.\n\n"
            "<!-- source: agent:research — end -->\n\n"
            "## Food Pairings\n\n"
            "### Recommended Pairings\n"
            "<!-- source: agent:research -->\n\n"
            "Preserved FP content.\n\n"
            "<!-- source: agent:research — end -->\n\n"
            "## Ratings & Reviews\n\n"
            "### From Cellar Export\n"
            "<!-- source: etl — do not edit below this line -->\n\n"
            "*No ratings.*\n\n"
            "<!-- source: etl — end -->\n\n"
            "### From Research\n"
            "<!-- source: agent:research -->\n\n"
            "Preserved RR content.\n\n"
            "<!-- source: agent:research — end -->\n"
        )
        md = _render(existing_content=existing)
        # All three H3 headings present before their agent fences
        assert "### Community Tasting Notes\n<!-- source: agent:research -->" in md
        assert "### Recommended Pairings\n<!-- source: agent:research -->" in md
        assert "### From Research\n<!-- source: agent:research -->" in md
        # All agent content preserved
        assert "Preserved TN content." in md
        assert "Preserved FP content." in md
        assert "Preserved RR content." in md

    def test_frontmatter_merges_agent_fields(self):
        existing = (
            "---\nwine_id: 1\n"
            "agent_sections_populated:\n  - producer_profile\n"
            "agent_sections_pending:\n  - vintage_report\n"
            "---\n## Producer Profile\n"
            "<!-- source: agent:research -->\n\nInfo.\n\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        md = _render(existing_content=existing)
        assert "agent_sections_populated:" in md
        assert "producer_profile" in md

    def test_multiple_bottles_aggregated_in_purchase_history(self):
        bottles = [
            {"bottle_id": 1, "wine_id": 1, "status": "stored",
             "cellar_id": 1, "shelf": "A1",
             "provider_id": 1, "purchase_date": date(2024, 1, 1),
             "original_purchase_price": Decimal("20.00"), "original_purchase_currency": "CHF",
             "purchase_price": Decimal("20.00"), "purchase_currency": "CHF",
             "acquisition_type": "market_price",
             "output_date": None, "output_type": None, "output_comment": None},
            {"bottle_id": 2, "wine_id": 1, "status": "stored",
             "cellar_id": 1, "shelf": "A2",
             "provider_id": 1, "purchase_date": date(2024, 1, 1),
             "original_purchase_price": Decimal("20.00"), "original_purchase_currency": "CHF",
             "purchase_price": Decimal("20.00"), "purchase_currency": "CHF",
             "acquisition_type": "market_price",
             "output_date": None, "output_type": None, "output_comment": None},
            {"bottle_id": 3, "wine_id": 1, "status": "stored",
             "cellar_id": 1, "shelf": "A3",
             "provider_id": 2, "purchase_date": date(2024, 6, 1),
             "original_purchase_price": Decimal("25.00"), "original_purchase_currency": "EUR",
             "purchase_price": Decimal("23.25"), "purchase_currency": "CHF",
             "acquisition_type": "market_price",
             "output_date": None, "output_type": None, "output_comment": None},
        ]
        md = _render(
            bottles=bottles,
            cellar_names={1: "C1"},
            provider_names={1: "P1", 2: "P2"},
        )
        # Should have 2 purchase rows (grouped)
        assert "Total bottles:** 3" in md
        assert "Total invested:" in md

    def test_food_pairings_etl_section_from_comment(self):
        w = _wine(comment="Pairs with cheese and bread")
        md = _render(wine=w)
        assert "## Food Pairings" in md
        assert "Pairs with cheese and bread" in md
        assert "From Owner Notes" in md

    def test_consumption_history_with_gone_bottles(self):
        bottles = [
            {"bottle_id": 1, "wine_id": 1, "status": "stored",
             "cellar_id": 1, "shelf": "A1",
             "provider_id": 1, "purchase_date": date(2024, 1, 1),
             "original_purchase_price": Decimal("20.00"), "original_purchase_currency": "CHF",
             "purchase_price": Decimal("20.00"), "purchase_currency": "CHF",
             "acquisition_type": "market_price",
             "output_date": None, "output_type": None, "output_comment": None},
            {"bottle_id": 2, "wine_id": 1, "status": "drunk",
             "cellar_id": None, "shelf": None,
             "provider_id": 1, "purchase_date": date(2024, 1, 1),
             "original_purchase_price": Decimal("20.00"), "original_purchase_currency": "CHF",
             "purchase_price": Decimal("20.00"), "purchase_currency": "CHF",
             "acquisition_type": "market_price",
             "output_date": date(2025, 3, 15), "output_type": "drunk",
             "output_comment": "Birthday dinner"},
        ]
        md = _render(
            bottles=bottles,
            cellar_names={1: "C1"},
            provider_names={1: "P1"},
        )
        assert "bottles_in_cellar: 1" in md
        assert "bottles_consumed: 1" in md
        assert "bottles_total: 2" in md
        assert "## Consumption History" in md
        assert "Total consumed:** 1" in md
        assert "Birthday dinner" in md
        assert "drunk" in md
        assert "## Cellar Inventory" in md
        assert "Total bottles:** 1" in md


# ---------------------------------------------------------------------------
# TestGenerateDossiers
# ---------------------------------------------------------------------------

class TestGenerateDossiers:
    def test_generates_all_files(self, tmp_path):
        wines = [_wine(wine_id=1), _wine(wine_id=2, name="Cuvée")]
        entities = _minimal_entities(wines=wines)
        paths = generate_dossiers(entities, tmp_path, 2026)
        assert len(paths) == 2
        assert (tmp_path / "wines" / "archive").is_dir()
        for p in paths:
            assert p.exists()
            assert p.suffix == ".md"
            # No stored bottles → archive
            assert "archive" in str(p)

    def test_stored_bottles_go_to_cellar(self, tmp_path):
        wines = [_wine(wine_id=1)]
        bottles = [
            {"bottle_id": 1, "wine_id": 1, "status": "stored",
             "cellar_id": 1, "shelf": "A1",
             "provider_id": 1, "purchase_date": date(2024, 1, 1),
             "original_purchase_price": Decimal("20.00"), "original_purchase_currency": "CHF",
             "purchase_price": Decimal("20.00"), "purchase_currency": "CHF",
             "acquisition_type": "market_price",
             "output_date": None, "output_type": None, "output_comment": None},
        ]
        entities = _minimal_entities(wines=wines)
        entities["bottle"] = bottles
        paths = generate_dossiers(entities, tmp_path, 2026)
        assert len(paths) == 1
        assert "cellar" in str(paths[0])

    def test_filtered_by_wine_ids(self, tmp_path):
        wines = [_wine(wine_id=1), _wine(wine_id=2, name="Other")]
        entities = _minimal_entities(wines=wines)
        paths = generate_dossiers(entities, tmp_path, 2026, wine_ids={2})
        assert len(paths) == 1
        assert "0002-" in paths[0].name

    def test_preserves_agent_sections_on_regeneration(self, tmp_path):
        entities = _minimal_entities()
        archive_dir = tmp_path / "wines" / "archive"
        archive_dir.mkdir(parents=True)

        # Write a file with agent content
        fname = dossier_filename(1, "W", None, 2020, False)
        agent_md = (
            "---\nwine_id: 1\n---\n# W 2020\n\n"
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\n"
            "Preserved research.\n\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        (archive_dir / fname).write_text(agent_md, encoding="utf-8")

        paths = generate_dossiers(entities, tmp_path, 2026)
        content = paths[0].read_text(encoding="utf-8")
        assert "Preserved research." in content
        assert "## Identity" in content

    def test_creates_subdirectories(self, tmp_path):
        entities = _minimal_entities()
        generate_dossiers(entities, tmp_path, 2026)
        assert (tmp_path / "wines" / "cellar").is_dir()
        assert (tmp_path / "wines" / "archive").is_dir()

    def test_transition_cellar_to_archive(self, tmp_path):
        """When stored bottles are consumed the file moves to archive."""
        fname = dossier_filename(1, "W", None, 2020, False)
        cellar_dir = tmp_path / "wines" / "cellar"
        cellar_dir.mkdir(parents=True)
        (tmp_path / "wines" / "archive").mkdir(parents=True)
        # Pre-existing file in cellar (from previous run with stored bottles)
        (cellar_dir / fname).write_text(
            "---\nwine_id: 1\n---\n# W 2020\n\n"
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\nResearch.\n\n"
            "<!-- source: agent:research \u2014 end -->\n",
            encoding="utf-8",
        )
        # Now no stored bottles (all consumed)
        entities = _minimal_entities()
        entities["bottle"] = []
        paths = generate_dossiers(entities, tmp_path, 2026)
        assert len(paths) == 1
        assert "archive" in str(paths[0])
        assert not (cellar_dir / fname).exists()  # old file removed
        content = paths[0].read_text(encoding="utf-8")
        assert "Research." in content  # agent content preserved

    def test_transition_archive_to_cellar(self, tmp_path):
        """When bottles are added the file moves to cellar."""
        fname = dossier_filename(1, "W", None, 2020, False)
        archive_dir = tmp_path / "wines" / "archive"
        archive_dir.mkdir(parents=True)
        (tmp_path / "wines" / "cellar").mkdir(parents=True)
        (archive_dir / fname).write_text(
            "---\nwine_id: 1\n---\n# W 2020\n\n"
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\nResearch.\n\n"
            "<!-- source: agent:research \u2014 end -->\n",
            encoding="utf-8",
        )
        # Now this wine has a stored bottle
        entities = _minimal_entities()
        entities["bottle"] = [
            {"bottle_id": 1, "wine_id": 1, "status": "stored",
             "cellar_id": 1, "shelf": "A1",
             "provider_id": 1, "purchase_date": date(2024, 1, 1),
             "original_purchase_price": Decimal("20.00"), "original_purchase_currency": "CHF",
             "purchase_price": Decimal("20.00"), "purchase_currency": "CHF",
             "acquisition_type": "market_price",
             "output_date": None, "output_type": None, "output_comment": None},
        ]
        paths = generate_dossiers(entities, tmp_path, 2026)
        assert len(paths) == 1
        assert "cellar" in str(paths[0])
        assert not (archive_dir / fname).exists()
        content = paths[0].read_text(encoding="utf-8")
        assert "Research." in content

    def test_slug_change_preserves_agent_sections(self, tmp_path):
        """Wine rename: old slug file exists, new slug different, agent content migrated."""
        archive_dir = tmp_path / "wines" / "archive"
        archive_dir.mkdir(parents=True)
        (tmp_path / "wines" / "cellar").mkdir(parents=True)

        # Old dossier with agent content under the OLD slug
        old_fname = dossier_filename(1, "W", "Old Name", 2020, False)
        agent_md = (
            "---\nwine_id: 1\n---\n# W Old Name 2020\n\n"
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\n"
            "Expensive research.\n\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        (archive_dir / old_fname).write_text(agent_md, encoding="utf-8")

        # Wine now has a different name → different slug
        wines = [_wine(wine_id=1, name="New Name")]
        entities = _minimal_entities(wines=wines)
        paths = generate_dossiers(entities, tmp_path, 2026)

        assert len(paths) == 1
        new_fname = dossier_filename(1, "W", "New Name", 2020, False)
        assert paths[0].name == new_fname
        content = paths[0].read_text(encoding="utf-8")
        assert "Expensive research." in content
        assert "## Identity" in content

    def test_slug_change_cleans_old_file(self, tmp_path):
        """Old file with stale slug is removed after migration."""
        archive_dir = tmp_path / "wines" / "archive"
        archive_dir.mkdir(parents=True)
        (tmp_path / "wines" / "cellar").mkdir(parents=True)

        old_fname = dossier_filename(1, "W", "Old", 2020, False)
        (archive_dir / old_fname).write_text(
            "---\nwine_id: 1\n---\n# W Old 2020\n", encoding="utf-8",
        )

        wines = [_wine(wine_id=1, name="New")]
        entities = _minimal_entities(wines=wines)
        generate_dossiers(entities, tmp_path, 2026)

        assert not (archive_dir / old_fname).exists()

    def test_slug_change_with_subfolder_move(self, tmp_path):
        """Slug change + cellar→archive move in the same run."""
        cellar_dir = tmp_path / "wines" / "cellar"
        cellar_dir.mkdir(parents=True)
        (tmp_path / "wines" / "archive").mkdir(parents=True)

        old_fname = dossier_filename(1, "W", "Old Name", 2020, False)
        agent_md = (
            "---\nwine_id: 1\n---\n# W Old Name 2020\n\n"
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\n"
            "Keep this.\n\n"
            "<!-- source: agent:research \u2014 end -->\n"
        )
        (cellar_dir / old_fname).write_text(agent_md, encoding="utf-8")

        # No stored bottles → should move to archive, and slug changed
        wines = [_wine(wine_id=1, name="New Name")]
        entities = _minimal_entities(wines=wines)
        entities["bottle"] = []
        paths = generate_dossiers(entities, tmp_path, 2026)

        assert len(paths) == 1
        assert "archive" in str(paths[0])
        new_fname = dossier_filename(1, "W", "New Name", 2020, False)
        assert paths[0].name == new_fname
        content = paths[0].read_text(encoding="utf-8")
        assert "Keep this." in content
        assert not (cellar_dir / old_fname).exists()


# ---------------------------------------------------------------------------
# TestMarkDeletedDossiers
# ---------------------------------------------------------------------------

class TestMarkDeletedDossiers:
    def test_marks_file_as_deleted(self, tmp_path):
        archive_dir = tmp_path / "wines" / "archive"
        archive_dir.mkdir(parents=True)
        fpath = archive_dir / "0001-test-2020.md"
        fpath.write_text("---\nwine_id: 1\n---\n# Test 2020\n\nContent.\n", encoding="utf-8")

        modified = mark_deleted_dossiers(tmp_path, {1}, 5, "2025-07-01T00:00:00")
        assert len(modified) == 1
        content = fpath.read_text(encoding="utf-8")
        assert "deleted: true" in content
        assert "\u26a0\ufe0f" in content
        assert "ETL run 5" in content

    def test_marks_cellar_file(self, tmp_path):
        cellar_dir = tmp_path / "wines" / "cellar"
        cellar_dir.mkdir(parents=True)
        fpath = cellar_dir / "0001-test-2020.md"
        fpath.write_text("---\nwine_id: 1\n---\n# Test 2020\n\nContent.\n", encoding="utf-8")

        modified = mark_deleted_dossiers(tmp_path, {1}, 5, "2025-07-01")
        assert len(modified) == 1
        assert "deleted: true" in fpath.read_text(encoding="utf-8")

    def test_file_not_found_is_noop(self, tmp_path):
        (tmp_path / "wines" / "cellar").mkdir(parents=True)
        (tmp_path / "wines" / "archive").mkdir(parents=True)
        modified = mark_deleted_dossiers(tmp_path, {999}, 5, "2025-07-01T00:00:00")
        assert modified == []

    def test_preserves_agent_sections(self, tmp_path):
        archive_dir = tmp_path / "wines" / "archive"
        archive_dir.mkdir(parents=True)
        fpath = archive_dir / "0001-test-2020.md"
        fpath.write_text(
            "---\nwine_id: 1\n---\n# Test 2020\n\n"
            "## Producer Profile\n"
            "<!-- source: agent:research -->\n\nResearch.\n\n"
            "<!-- source: agent:research \u2014 end -->\n",
            encoding="utf-8",
        )
        mark_deleted_dossiers(tmp_path, {1}, 5, "2025-07-01")
        content = fpath.read_text(encoding="utf-8")
        assert "Research." in content
        assert "deleted: true" in content


# ---------------------------------------------------------------------------
# TestAffectedWineIds
# ---------------------------------------------------------------------------

class TestAffectedWineIds:
    def _entities(self) -> dict[str, list[dict]]:
        return {
            "wine": [
                _wine(wine_id=1, winery_id=1, appellation_id=10),
                _wine(wine_id=2, winery_id=1, appellation_id=20),
                _wine(wine_id=3, winery_id=2),
            ],
            "winery": [
                {"winery_id": 1, "name": "W1", "etl_run_id": 1, "updated_at": _NOW},
                {"winery_id": 2, "name": "W2", "etl_run_id": 1, "updated_at": _NOW},
            ],
            "appellation": [],
            "grape": [],
            "wine_grape": [
                {"wine_id": 1, "grape_id": 5, "percentage": 100.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW},
            ],
            "bottle": [
                {"bottle_id": 100, "wine_id": 2, "status": "stored",
                 "cellar_id": 1, "shelf": "A", "provider_id": 1,
                 "purchase_date": date(2024, 1, 1),
                 "original_purchase_price": Decimal("10"), "original_purchase_currency": "CHF",
                 "purchase_price": Decimal("10"), "purchase_currency": "CHF",
                 "acquisition_type": "market_price",
                 "output_date": None, "output_type": None, "output_comment": None,
                 "etl_run_id": 1, "updated_at": _NOW},
            ],
            "cellar": [],
            "provider": [],
            "tasting": [
                {"tasting_id": 200, "wine_id": 3, "tasting_date": date(2024, 1, 1), "note": "x", "score": 90.0, "max_score": 100, "etl_run_id": 1, "updated_at": _NOW},
            ],
            "pro_rating": [
                {"rating_id": 300, "wine_id": 1, "source": "JS", "score": 92.0, "max_score": 100, "review_text": None, "etl_run_id": 1, "updated_at": _NOW},
            ],
        }

    def test_direct_wine_change(self):
        log = [{"entity_type": "wine", "entity_id": 1, "change_type": "insert"}]
        result = affected_wine_ids(log, self._entities())
        assert 1 in result

    def test_bottle_change_affects_wine(self):
        log = [{"entity_type": "bottle", "entity_id": 100, "change_type": "insert"}]
        result = affected_wine_ids(log, self._entities())
        assert 2 in result

    def test_winery_change_affects_all_wines(self):
        log = [{"entity_type": "winery", "entity_id": 1, "change_type": "update"}]
        result = affected_wine_ids(log, self._entities())
        assert 1 in result
        assert 2 in result
        assert 3 not in result

    def test_grape_change_affects_wines_via_wine_grape(self):
        log = [{"entity_type": "grape", "entity_id": 5, "change_type": "update"}]
        result = affected_wine_ids(log, self._entities())
        assert 1 in result

    def test_wine_delete_not_in_affected(self):
        log = [{"entity_type": "wine", "entity_id": 1, "change_type": "delete"}]
        result = affected_wine_ids(log, self._entities())
        assert 1 not in result
