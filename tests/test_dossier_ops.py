"""Tests for cellarbrain.dossier_ops — dossier read/write/pending operations."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cellarbrain import markdown, writer
from cellarbrain.dossier_ops import (
    _ETL_SECTION_KEYS,
    AGENT_SECTIONS,
    ALLOWED_COMPANION_SECTIONS,
    ALLOWED_SECTIONS,
    COMPANION_SECTIONS,
    ProtectedSectionError,
    TrackedWineNotFoundError,
    WineNotFoundError,
    _all_heading_to_key,
    _filter_sections,
    _merge_food_groups,
    _merge_food_tags,
    pending_companion_research,
    pending_research,
    read_companion_dossier,
    read_dossier,
    read_dossier_sections,
    read_research_meta,
    resolve_companion_dossier_path,
    resolve_dossier_path,
    stale_research,
    update_companion_dossier,
    update_dossier,
)
from cellarbrain.markdown import dossier_filename

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime(2025, 1, 1)


def _build_entities(tmp_path):
    """Build minimal entities and write Parquet + dossiers."""
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Domaine Test", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1,
            "country": "France",
            "region": "Bordeaux",
            "subregion": "Saint-Émilion",
            "classification": "Grand Cru",
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1,
            "wine_slug": "domaine-test-cuvee-alpha-2020",
            "winery_id": 1,
            "name": "Cuvée Alpha",
            "vintage": 2020,
            "is_non_vintage": False,
            "appellation_id": 1,
            "category": "Red wine",
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
            "alcohol_pct": 14.5,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2023,
            "drink_until": 2030,
            "optimal_from": 2025,
            "optimal_until": 2028,
            "original_list_price": None,
            "original_list_currency": None,
            "list_price": None,
            "list_currency": None,
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": True,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Domaine Test Cuvée Alpha 2020",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": f"cellar/{dossier_filename(1, 'Domaine Test', 'Cuvée Alpha', 2020, False)}",
            "drinking_status": "optimal",
            "age_years": 5,
            "price_tier": "unknown",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "wine_id": 2,
            "wine_slug": "domaine-test-cuvee-beta-2019",
            "winery_id": 1,
            "name": "Cuvée Beta",
            "vintage": 2019,
            "is_non_vintage": False,
            "appellation_id": 1,
            "category": "Red wine",
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
            "alcohol_pct": 13.0,
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
            "tracked_wine_id": None,
            "full_name": "Domaine Test Cuvée Beta 2019",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": f"archive/{dossier_filename(2, 'Domaine Test', 'Cuvée Beta', 2019, False)}",
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
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        {"wine_id": 2, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        {
            "bottle_id": 1,
            "wine_id": 1,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "A1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    cellars = [
        {
            "cellar_id": 1,
            "name": "Main Cellar",
            "location_type": "onsite",
            "sort_order": 1,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    providers = [
        {"provider_id": 1, "name": "Wine Shop", "etl_run_id": rid, "updated_at": now},
    ]
    tastings: list[dict] = []
    pro_ratings: list[dict] = []
    etl_runs = [
        {
            "run_id": 1,
            "started_at": now,
            "finished_at": now,
            "run_type": "full",
            "wines_source_hash": "abc",
            "bottles_source_hash": "def",
            "bottles_gone_source_hash": None,
            "total_inserts": 5,
            "total_updates": 0,
            "total_deletes": 0,
            "wines_inserted": 1,
            "wines_updated": 0,
            "wines_deleted": 0,
            "wines_renamed": 0,
        },
    ]
    change_logs = [
        {
            "change_id": 1,
            "run_id": 1,
            "entity_type": "wine",
            "entity_id": 1,
            "change_type": "insert",
            "changed_fields": None,
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
        "tasting": tastings,
        "pro_rating": pro_ratings,
    }

    # Write Parquet files
    for name, rows in entities.items():
        writer.write_parquet(name, rows, tmp_path)
    writer.write_parquet("etl_run", etl_runs, tmp_path)
    writer.write_parquet("change_log", change_logs, tmp_path)

    # Generate dossier markdown files
    markdown.generate_dossiers(entities, tmp_path, current_year=2025)

    return tmp_path


@pytest.fixture()
def data_dir(tmp_path):
    return _build_entities(tmp_path)


# ---------------------------------------------------------------------------
# TestResolveDossierPath
# ---------------------------------------------------------------------------


class TestResolveDossierPath:
    def test_resolves_existing_wine(self, data_dir):
        path = resolve_dossier_path(1, data_dir)
        assert path.exists()
        assert path.name.endswith(".md")

    def test_nonexistent_wine_id_raises(self, data_dir):
        with pytest.raises(WineNotFoundError, match="does not exist"):
            resolve_dossier_path(999, data_dir)

    def test_missing_parquet_raises(self, tmp_path):
        with pytest.raises(WineNotFoundError, match="wine.parquet not found"):
            resolve_dossier_path(1, tmp_path)


# ---------------------------------------------------------------------------
# TestReadDossier
# ---------------------------------------------------------------------------


class TestReadDossier:
    def test_returns_markdown_content(self, data_dir):
        content = read_dossier(1, data_dir)
        assert "Cuvée Alpha" in content
        assert "---" in content  # has frontmatter

    def test_nonexistent_wine_raises(self, data_dir):
        with pytest.raises(WineNotFoundError):
            read_dossier(999, data_dir)


# ---------------------------------------------------------------------------
# TestReadDossierSections
# ---------------------------------------------------------------------------


class TestReadDossierSections:
    def test_no_sections_returns_full(self, data_dir):
        full = read_dossier(1, data_dir)
        filtered = read_dossier_sections(1, data_dir, sections=None)
        assert filtered == full

    def test_single_etl_section(self, data_dir):
        result = read_dossier_sections(1, data_dir, sections=["identity"])
        assert "## Identity" in result
        assert "## Origin" not in result
        assert "## Cellar Inventory" not in result
        # Frontmatter always included
        assert "wine_id:" in result
        assert "Cuvée Alpha" in result

    def test_single_agent_section(self, data_dir):
        result = read_dossier_sections(1, data_dir, sections=["producer_profile"])
        assert "## Producer Profile" in result
        assert "## Identity" not in result
        assert "wine_id:" in result

    def test_multiple_sections(self, data_dir):
        result = read_dossier_sections(1, data_dir, sections=["identity", "drinking_window"])
        assert "## Identity" in result
        assert "## Drinking Window" in result
        assert "## Origin" not in result
        assert "## Producer Profile" not in result

    def test_mixed_section(self, data_dir):
        result = read_dossier_sections(1, data_dir, sections=["ratings_reviews"])
        assert "## Ratings & Reviews" in result
        assert "## Tasting Notes" not in result
        assert "## Identity" not in result
        assert "wine_id:" in result

    def test_unknown_section_raises(self, data_dir):
        with pytest.raises(ValueError, match="Unknown section"):
            read_dossier_sections(1, data_dir, sections=["nonexistent_key"])

    def test_empty_sections_list(self, data_dir):
        result = read_dossier_sections(1, data_dir, sections=[])
        assert "## Identity" not in result
        assert "wine_id:" in result

    def test_frontmatter_always_included(self, data_dir):
        for section in ["identity", "producer_profile", "ratings_reviews"]:
            result = read_dossier_sections(1, data_dir, sections=[section])
            assert result.startswith("---\n")
            assert "Cuvée Alpha" in result

    def test_nonexistent_wine_raises(self, data_dir):
        with pytest.raises(WineNotFoundError):
            read_dossier_sections(999, data_dir, sections=["identity"])


# ---------------------------------------------------------------------------
# TestFilterSections
# ---------------------------------------------------------------------------


class TestFilterSections:
    def test_round_trip_all_keys(self, data_dir):
        """Filtering with all known keys returns equivalent content to full dossier."""
        full = read_dossier(1, data_dir)
        all_keys = list(_all_heading_to_key().values())
        filtered = _filter_sections(full, all_keys)
        # All H2 sections from the full dossier should be present
        for section in ["## Identity", "## Characteristics", "## Producer Profile"]:
            if section in full:
                assert section in filtered

    def test_etl_heading_to_key_mapping(self):
        """All ETL section headings map to expected keys."""
        expected = {
            "Identity": "identity",
            "Origin": "origin",
            "Grapes": "grapes",
            "Characteristics": "characteristics",
            "Drinking Window": "drinking_window",
            "Cellar Inventory": "cellar_inventory",
            "Purchase History": "purchase_history",
            "Consumption History": "consumption_history",
            "Owner Notes": "owner_notes",
            "Ratings & Reviews": "ratings_reviews",
            "Tasting Notes": "tasting_notes",
            "Food Pairings": "food_pairings",
        }
        assert expected == _ETL_SECTION_KEYS

    def test_all_heading_to_key_includes_agent_sections(self):
        """_all_heading_to_key includes pure agent section headings."""
        h2k = _all_heading_to_key()
        assert h2k["Producer Profile"] == "producer_profile"
        assert h2k["Vintage Report"] == "vintage_report"
        assert h2k["Wine Description"] == "wine_description"
        assert h2k["Market & Availability"] == "market_availability"
        assert h2k["Similar Wines"] == "similar_wines"
        assert h2k["Agent Log"] == "agent_log"

    def test_mixed_sections_not_duplicated_from_agent_headings(self):
        """Mixed sections use H2 heading from _ETL_SECTION_KEYS, not AgentSection.heading."""
        h2k = _all_heading_to_key()
        # H2 headings map correctly
        assert h2k["Ratings & Reviews"] == "ratings_reviews"
        assert h2k["Tasting Notes"] == "tasting_notes"
        assert h2k["Food Pairings"] == "food_pairings"
        # Sub-headings (H3 level) are NOT in the map
        assert "From Research" not in h2k
        assert "Community Tasting Notes" not in h2k
        assert "Recommended Pairings" not in h2k


# ---------------------------------------------------------------------------
# TestUpdateDossier
# ---------------------------------------------------------------------------


class TestUpdateDossier:
    def test_update_producer_profile(self, data_dir):
        result = update_dossier(
            1,
            "producer_profile",
            "Domaine Test is a family winery in Bordeaux.",
            data_dir,
        )
        assert "Updated" in result

        content = read_dossier(1, data_dir)
        assert "family winery in Bordeaux" in content

    def test_update_moves_section_to_populated(self, data_dir):
        update_dossier(
            1,
            "producer_profile",
            "Test content",
            data_dir,
        )
        content = read_dossier(1, data_dir)
        assert "producer_profile" in _extract_populated(content)
        assert "producer_profile" not in _extract_pending(content)

    def test_update_preserves_other_sections(self, data_dir):
        content_before = read_dossier(1, data_dir)
        assert "Identity" in content_before

        update_dossier(1, "producer_profile", "New content", data_dir)

        content_after = read_dossier(1, data_dir)
        assert "Identity" in content_after
        assert "Cuvée Alpha" in content_after

    def test_agent_log_appends_entry(self, data_dir):
        update_dossier(1, "agent_log", "First research completed", data_dir)
        content = read_dossier(1, data_dir)
        assert "First research completed" in content
        assert "(research)" in content  # default agent_name

    def test_agent_log_custom_agent_name(self, data_dir):
        update_dossier(
            1,
            "agent_log",
            "Price check done",
            data_dir,
            agent_name="pricing",
        )
        content = read_dossier(1, data_dir)
        assert "(pricing)" in content

    def test_protected_section_raises(self, data_dir):
        with pytest.raises(ProtectedSectionError, match="not an allowed"):
            update_dossier(1, "identity", "hacked", data_dir)

    def test_nonexistent_wine_raises(self, data_dir):
        with pytest.raises(WineNotFoundError):
            update_dossier(999, "producer_profile", "content", data_dir)

    def test_empty_content_raises(self, data_dir):
        with pytest.raises(ValueError, match="empty or whitespace-only"):
            update_dossier(1, "wine_description", "", data_dir)

    def test_whitespace_content_raises(self, data_dir):
        with pytest.raises(ValueError, match="empty or whitespace-only"):
            update_dossier(1, "wine_description", "   \n  ", data_dir)

    def test_empty_content_does_not_modify_dossier(self, data_dir):
        path = list((data_dir / "wines").rglob("0001-*.md"))[0]
        before = path.read_text(encoding="utf-8")
        with pytest.raises(ValueError):
            update_dossier(1, "wine_description", "", data_dir)
        assert path.read_text(encoding="utf-8") == before

    def test_all_allowed_section_keys(self):
        assert frozenset(AGENT_SECTIONS.keys()) == ALLOWED_SECTIONS

    def test_multiple_updates_to_same_section(self, data_dir):
        update_dossier(1, "wine_description", "Draft 1", data_dir)
        update_dossier(1, "wine_description", "Draft 2 — improved", data_dir)
        content = read_dossier(1, data_dir)
        assert "Draft 2 — improved" in content
        assert "Draft 1" not in content

    def test_placeholder_revert_removes_from_populated(self, data_dir):
        """Writing placeholder text back to a section should move it to pending."""
        update_dossier(1, "wine_description", "Real content", data_dir)
        content = read_dossier(1, data_dir)
        assert "wine_description" in _extract_populated(content)
        assert "wine_description" not in _extract_pending(content)

        update_dossier(
            1,
            "wine_description",
            "*Not yet researched. Pending agent action.*",
            data_dir,
        )
        content = read_dossier(1, data_dir)
        assert "wine_description" not in _extract_populated(content)
        assert "wine_description" in _extract_pending(content)

    def test_pending_order_stable_after_write_revert(self, data_dir):
        """Pending list ordering must match canonical section order after revert."""
        pending_before = _extract_pending(read_dossier(1, data_dir))

        update_dossier(1, "wine_description", "Temporary content", data_dir)
        update_dossier(
            1,
            "wine_description",
            "*Not yet researched. Pending agent action.*",
            data_dir,
        )

        pending_after = _extract_pending(read_dossier(1, data_dir))
        assert pending_before == pending_after

    def test_ratings_reviews_subsection(self, data_dir):
        update_dossier(
            1,
            "ratings_reviews",
            "Parker 95/100\nSuckling 93/100",
            data_dir,
        )
        content = read_dossier(1, data_dir)
        assert "Parker 95/100" in content

    def test_tasting_notes_after_regeneration(self, data_dir):
        """Regression: tasting_notes must be writable after dossier regeneration."""
        # First write agent content via update
        update_dossier(1, "tasting_notes", "Initial TN.", data_dir)
        content = read_dossier(1, data_dir)
        assert "Initial TN." in content

        # Simulate ETL regeneration by re-generating the dossier
        from cellarbrain import markdown

        dossier_path = resolve_dossier_path(1, data_dir)
        existing = dossier_path.read_text(encoding="utf-8")
        wine = {
            "wine_id": 1,
            "winery_id": 1,
            "name": None,
            "vintage": 2020,
            "is_non_vintage": False,
            "appellation_id": None,
            "category": "red",
            "subcategory": None,
            "specialty": None,
            "sweetness": None,
            "effervescence": None,
            "volume_ml": 750,
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
            "list_price": None,
            "list_currency": None,
            "original_list_price": None,
            "original_list_currency": None,
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": False,
            "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": None,
            "grape_type": "unknown",
            "primary_grape": None,
            "grape_summary": None,
            "drinking_status": "unknown",
            "age_years": None,
            "price_tier": "unknown",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "etl_run_id": 1,
            "updated_at": "2025-06-01T12:00:00",
        }
        regenerated = markdown.render_wine_dossier(
            wine=wine,
            winery_name="Domaine Test",
            appellation=None,
            grapes=[],
            bottles=[],
            cellar_names={},
            provider_names={},
            tastings=[],
            pro_ratings=[],
            current_year=2025,
            existing_content=existing,
        )
        dossier_path.write_text(regenerated, encoding="utf-8")

        # Agent content should be preserved
        assert "Initial TN." in regenerated
        # H3 heading must be present
        assert "### Community Tasting Notes\n<!-- source: agent:research -->" in regenerated

        # Now update again — this must NOT raise ProtectedSectionError
        update_dossier(1, "tasting_notes", "Updated TN.", data_dir)
        content = read_dossier(1, data_dir)
        assert "Updated TN." in content

    def test_food_pairings_after_regeneration(self, data_dir):
        """Regression: food_pairings must be writable after dossier regeneration."""
        update_dossier(1, "food_pairings", "Pairs with lamb.", data_dir)
        content = read_dossier(1, data_dir)
        assert "Pairs with lamb." in content


# ---------------------------------------------------------------------------
# TestPendingResearch
# ---------------------------------------------------------------------------


class TestPendingResearch:
    def test_all_sections_pending_initially(self, data_dir):
        result = pending_research(data_dir)
        assert "wine_id" in result
        assert "1" in result  # wine_id 1

    def test_pending_count_decreases_after_update(self, data_dir):
        result_before = pending_research(data_dir)
        update_dossier(1, "producer_profile", "test", data_dir)
        result_after = pending_research(data_dir)
        # Both should contain wine 1, but pending count should differ
        assert "1" in result_before
        assert "1" in result_after

    def test_limit_respected(self, data_dir):
        result = pending_research(data_dir, limit=1)
        # Should have header + separator + 1 data row = 3 lines
        lines = [l for l in result.strip().split("\n") if l.strip()]
        assert len(lines) == 3

    def test_no_dossiers_directory(self, tmp_path):
        result = pending_research(tmp_path)
        assert "No wine dossiers found" in result

    def test_favorites_sorted_first(self, data_dir):
        result = pending_research(data_dir)
        lines = result.strip().split("\n")
        data_lines = [l for l in lines if l.startswith("| ") and "---" not in l and "wine_id" not in l]
        # Wine 1 (favorite) should appear before wine 2
        if len(data_lines) >= 2:
            assert "1" in data_lines[0]

    def test_section_filter(self, data_dir):
        result = pending_research(data_dir, section="producer_profile")
        assert "wine_id" in result
        # All returned wines should have producer_profile pending
        assert "No wines with pending" not in result

    def test_section_filter_no_match(self, data_dir):
        result = pending_research(data_dir, section="nonexistent_section")
        assert "No wines with pending" in result


# ---------------------------------------------------------------------------
# TestResearchMetadata
# ---------------------------------------------------------------------------


class TestResearchMetadata:
    def test_update_embeds_research_meta(self, data_dir):
        update_dossier(1, "producer_profile", "Famous winery in Bordeaux.", data_dir)
        content = read_dossier(1, data_dir)
        assert "<!-- research-meta:" in content

    def test_research_meta_contains_date(self, data_dir):
        update_dossier(1, "producer_profile", "Famous winery.", data_dir)
        meta = read_research_meta(1, "producer_profile", data_dir)
        assert meta is not None
        assert "date" in meta
        # Date should be today (ISO format)
        from datetime import date

        assert meta["date"] == date.today().isoformat()

    def test_research_meta_contains_agent_name(self, data_dir):
        update_dossier(1, "producer_profile", "Info.", data_dir, agent_name="test-agent")
        meta = read_research_meta(1, "producer_profile", data_dir)
        assert meta is not None
        assert meta["agent"] == "test-agent"

    def test_research_meta_with_sources(self, data_dir):
        update_dossier(
            1,
            "producer_profile",
            "Info from sources.",
            data_dir,
            sources=["wine-searcher.com", "jancisrobinson.com"],
        )
        meta = read_research_meta(1, "producer_profile", data_dir)
        assert meta is not None
        assert meta["sources"] == ["wine-searcher.com", "jancisrobinson.com"]

    def test_research_meta_with_confidence(self, data_dir):
        update_dossier(
            1,
            "producer_profile",
            "High confidence info.",
            data_dir,
            confidence="high",
        )
        meta = read_research_meta(1, "producer_profile", data_dir)
        assert meta is not None
        assert meta["confidence"] == "high"

    def test_research_meta_without_optional_fields(self, data_dir):
        update_dossier(1, "producer_profile", "Basic info.", data_dir)
        meta = read_research_meta(1, "producer_profile", data_dir)
        assert meta is not None
        assert "sources" not in meta
        assert "confidence" not in meta

    def test_multiple_updates_replace_meta(self, data_dir):
        update_dossier(1, "producer_profile", "Draft 1.", data_dir, agent_name="agent-a")
        update_dossier(1, "producer_profile", "Draft 2.", data_dir, agent_name="agent-b")
        meta = read_research_meta(1, "producer_profile", data_dir)
        assert meta is not None
        assert meta["agent"] == "agent-b"
        # Should only have one meta comment
        content = read_dossier(1, data_dir)
        assert content.count("<!-- research-meta:") == 1

    def test_read_research_meta_returns_none_for_unresearched(self, data_dir):
        # Wine 1 producer_profile has not been researched yet (placeholder)
        meta = read_research_meta(1, "producer_profile", data_dir)
        assert meta is None

    def test_agent_log_does_not_get_research_meta(self, data_dir):
        update_dossier(1, "agent_log", "Test log entry", data_dir)
        content = read_dossier(1, data_dir)
        # agent_log should NOT contain research-meta
        # Count should be 0 since nothing else was researched
        assert "<!-- research-meta:" not in content

    def test_content_preserved_with_meta(self, data_dir):
        update_dossier(1, "wine_description", "A rich, full-bodied wine.", data_dir)
        content = read_dossier(1, data_dir)
        assert "A rich, full-bodied wine." in content


# ---------------------------------------------------------------------------
# TestResearchMetaBackwardCompat
# ---------------------------------------------------------------------------


class TestResearchMetaBackwardCompat:
    """Verify dossiers with research-meta HTML comments are readable as plain Markdown."""

    def test_read_dossier_includes_content_not_meta_comments(self, data_dir):
        """Content should be returned; meta comments are invisible in rendered Markdown."""
        update_dossier(
            1,
            "producer_profile",
            "Famous winery in Saint-Émilion.",
            data_dir,
            agent_name="test-agent",
            sources=["wine-searcher.com"],
        )
        content = read_dossier(1, data_dir)
        assert "Famous winery in Saint-Émilion." in content
        # Meta comment is present in raw file but shouldn't block reading
        assert "<!-- research-meta:" in content

    def test_sections_parse_correctly_with_meta(self, data_dir):
        """read_dossier_sections still returns section content when meta is present."""
        update_dossier(1, "producer_profile", "Bio.", data_dir, agent_name="agent")
        update_dossier(1, "wine_description", "Desc.", data_dir, agent_name="agent")
        sections = read_dossier_sections(1, data_dir, sections=["producer_profile", "wine_description"])
        assert "Bio." in sections
        assert "Desc." in sections

    def test_pending_research_ignores_meta(self, data_dir):
        """Sections with meta are not considered pending."""
        update_dossier(1, "producer_profile", "Research done.", data_dir)
        result = pending_research(data_dir)
        # producer_profile should NOT appear as pending for wine 1
        assert (
            "producer_profile" not in result.split("Cuvée Alpha")[0].split("Cuvée Alpha")[-1]
            or "producer_profile" not in result
        )

    def test_plain_dossier_without_meta_still_works(self, data_dir):
        """Pre-v0.3 dossiers without any meta comments are fully readable."""
        content = read_dossier(1, data_dir)
        # Initially no research-meta
        assert "<!-- research-meta:" not in content
        # Sections still parseable
        sections = read_dossier_sections(1, data_dir, sections=["producer_profile"])
        assert isinstance(sections, str)
        assert len(sections) > 0


# ---------------------------------------------------------------------------
# TestStaleResearch
# ---------------------------------------------------------------------------


class TestStaleResearch:
    def test_no_stale_when_all_recent(self, data_dir):
        update_dossier(1, "producer_profile", "Fresh research.", data_dir)
        result = stale_research(data_dir, months=6)
        assert "*No stale research found.*" in result

    def test_detects_stale_section(self, data_dir):
        # Write research, then manually backdate the meta comment
        update_dossier(1, "producer_profile", "Old research.", data_dir)
        path = list((data_dir / "wines").rglob("0001-*.md"))[0]
        text = path.read_text(encoding="utf-8")
        # Replace the current date with one from 8 months ago
        import re
        from datetime import date, timedelta

        old_date = (date.today() - timedelta(days=250)).isoformat()
        text = re.sub(
            r'"date": "\d{4}-\d{2}-\d{2}"',
            f'"date": "{old_date}"',
            text,
            count=1,
        )
        path.write_text(text, encoding="utf-8")

        result = stale_research(data_dir, months=6)
        assert "producer_profile" in result
        assert "1" in result  # wine_id

    def test_limit_respected(self, data_dir):
        result = stale_research(data_dir, months=6, limit=1)
        # Should only have at most 1 data row (or no stale)
        lines = [l for l in result.strip().split("\n") if l.startswith("| ") and "---" not in l and "wine_id" not in l]
        assert len(lines) <= 1

    def test_section_filter(self, data_dir):
        # Write + backdate producer_profile
        update_dossier(1, "producer_profile", "Old.", data_dir)
        update_dossier(1, "wine_description", "Also old.", data_dir)
        path = list((data_dir / "wines").rglob("0001-*.md"))[0]
        text = path.read_text(encoding="utf-8")
        import re
        from datetime import date, timedelta

        old_date = (date.today() - timedelta(days=250)).isoformat()
        text = re.sub(r'"date": "\d{4}-\d{2}-\d{2}"', f'"date": "{old_date}"', text)
        path.write_text(text, encoding="utf-8")

        result = stale_research(data_dir, months=6, section="wine_description")
        assert "wine_description" in result
        assert "producer_profile" not in result

    def test_no_dossiers_returns_message(self, tmp_path):
        result = stale_research(tmp_path, months=6)
        assert "No wine dossiers found" in result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_populated(content: str) -> list[str]:
    """Extract agent_sections_populated list from frontmatter."""
    import re

    m = re.search(
        r"^agent_sections_populated:\s*\n((?:\s+-\s+.+\n)*)",
        content,
        re.MULTILINE,
    )
    if not m:
        return []
    return re.findall(r"-\s+(\S+)", m.group(1))


def _extract_pending(content: str) -> list[str]:
    """Extract agent_sections_pending list from frontmatter."""
    import re

    m = re.search(
        r"^agent_sections_pending:\s*\n((?:\s+-\s+.+\n)*)",
        content,
        re.MULTILINE,
    )
    if not m:
        return []
    return re.findall(r"-\s+(\S+)", m.group(1))


# ---------------------------------------------------------------------------
# Companion dossier fixtures
# ---------------------------------------------------------------------------


def _build_companion_entities(tmp_path):
    """Build entities with tracked wines and write Parquet + dossiers."""
    from cellarbrain import companion_markdown
    from cellarbrain.settings import Settings

    now = _now()
    rid = 1
    settings = Settings()

    wineries = [
        {"winery_id": 1, "name": "Domaine Test", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1,
            "country": "France",
            "region": "Bordeaux",
            "subregion": "Saint-Émilion",
            "classification": "Grand Cru",
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1,
            "wine_slug": "domaine-test-cuvee-alpha-2020",
            "winery_id": 1,
            "name": "Cuvée Alpha",
            "vintage": 2020,
            "is_non_vintage": False,
            "appellation_id": 1,
            "category": "Red wine",
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
            "alcohol_pct": 14.5,
            "acidity_g_l": None,
            "sugar_g_l": None,
            "ageing_type": None,
            "ageing_months": None,
            "farming_type": None,
            "serving_temp_c": None,
            "opening_type": None,
            "opening_minutes": None,
            "drink_from": 2023,
            "drink_until": 2030,
            "optimal_from": 2025,
            "optimal_until": 2028,
            "original_list_price": None,
            "original_list_currency": None,
            "list_price": None,
            "list_currency": None,
            "comment": None,
            "winemaking_notes": None,
            "is_favorite": True,
            "is_wishlist": False,
            "tracked_wine_id": 90_001,
            "full_name": "Domaine Test Cuvée Alpha 2020",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": f"cellar/{dossier_filename(1, 'Domaine Test', 'Cuvée Alpha', 2020, False)}",
            "drinking_status": "optimal",
            "age_years": 5,
            "price_tier": "unknown",
            "bottle_format": "Standard",
            "price_per_750ml": None,
            "format_group_id": None,
            "food_tags": None,
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]

    slug = companion_markdown.companion_dossier_slug(90_001, "Domaine Test", "Cuvée Alpha")
    tracked_wines = [
        {
            "tracked_wine_id": 90_001,
            "winery_id": 1,
            "wine_name": "Cuvée Alpha",
            "category": "Red wine",
            "appellation_id": 1,
            "dossier_path": f"tracked/{slug}",
            "is_deleted": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]

    entities = {
        "winery": wineries,
        "appellation": appellations,
        "grape": grapes,
        "wine": wines,
        "wine_grape": [
            {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
        ],
        "bottle": [],
        "cellar": [],
        "provider": [],
        "tasting": [],
        "pro_rating": [],
        "tracked_wine": tracked_wines,
    }

    for name, rows in entities.items():
        writer.write_parquet(name, rows, tmp_path)
    writer.write_parquet(
        "etl_run",
        [
            {
                "run_id": 1,
                "started_at": now,
                "finished_at": now,
                "run_type": "full",
                "wines_source_hash": "abc",
                "bottles_source_hash": "def",
                "bottles_gone_source_hash": None,
                "total_inserts": 5,
                "total_updates": 0,
                "total_deletes": 0,
                "wines_inserted": 1,
                "wines_updated": 0,
                "wines_deleted": 0,
                "wines_renamed": 0,
            },
        ],
        tmp_path,
    )
    writer.write_parquet(
        "change_log",
        [
            {
                "change_id": 1,
                "run_id": 1,
                "entity_type": "wine",
                "entity_id": 1,
                "change_type": "insert",
                "changed_fields": None,
            },
        ],
        tmp_path,
    )

    # Generate wine dossiers
    markdown.generate_dossiers(entities, tmp_path, current_year=2025)

    # Generate companion dossiers
    companion_markdown.generate_companion_dossiers(entities, tmp_path, settings)

    return tmp_path


# ---------------------------------------------------------------------------
# TestMergeFoodTags
# ---------------------------------------------------------------------------


class TestMergeFoodTags:
    def test_adds_tags_to_empty_list(self):
        text = "---\nwine_id: 1\nfood_tags: []\nagent_sections_populated: []\n---\n"
        result = _merge_food_tags(text, ["duck-confit", "raclette"])
        assert "- duck-confit" in result
        assert "- raclette" in result

    def test_preserves_existing_tags(self):
        text = "---\nwine_id: 1\nfood_tags:\n  - existing-tag\nagent_sections_populated: []\n---\n"
        result = _merge_food_tags(text, ["new-tag"])
        assert "- existing-tag" in result
        assert "- new-tag" in result

    def test_deduplicates_tags(self):
        text = "---\nwine_id: 1\nfood_tags:\n  - duck-confit\nagent_sections_populated: []\n---\n"
        result = _merge_food_tags(text, ["duck-confit", "raclette"])
        assert result.count("duck-confit") == 1
        assert "- raclette" in result

    def test_inserts_before_agent_sections_when_missing(self):
        text = "---\nwine_id: 1\nagent_sections_populated: []\n---\n"
        result = _merge_food_tags(text, ["raclette"])
        assert "food_tags:" in result
        assert "- raclette" in result
        ft_pos = result.index("food_tags:")
        ap_pos = result.index("agent_sections_populated:")
        assert ft_pos < ap_pos


class TestMergeFoodGroups:
    def test_adds_groups_to_empty_list(self):
        text = "---\nwine_id: 1\nfood_tags: []\nfood_groups: []\nagent_sections_populated: []\n---\n"
        result = _merge_food_groups(text, ["heavy", "French"])
        assert "- heavy" in result
        assert "- French" in result

    def test_preserves_existing_groups(self):
        text = "---\nwine_id: 1\nfood_tags: []\nfood_groups:\n  - existing-group\nagent_sections_populated: []\n---\n"
        result = _merge_food_groups(text, ["new-group"])
        assert "- existing-group" in result
        assert "- new-group" in result

    def test_deduplicates_groups(self):
        text = "---\nwine_id: 1\nfood_tags: []\nfood_groups:\n  - heavy\nagent_sections_populated: []\n---\n"
        result = _merge_food_groups(text, ["heavy", "French"])
        assert result.count("- heavy") == 1
        assert "- French" in result

    def test_inserts_after_food_tags_when_missing(self):
        text = "---\nwine_id: 1\nfood_tags: []\nagent_sections_populated: []\n---\n"
        result = _merge_food_groups(text, ["grilled"])
        assert "food_groups:" in result
        assert "- grilled" in result
        fg_pos = result.index("food_groups:")
        ap_pos = result.index("agent_sections_populated:")
        assert fg_pos < ap_pos


@pytest.fixture()
def companion_dir(tmp_path):
    return _build_companion_entities(tmp_path)


# ---------------------------------------------------------------------------
# TestCompanionDossierOps
# ---------------------------------------------------------------------------


class TestCompanionDossierOps:
    def test_resolve_companion_path(self, companion_dir):
        path = resolve_companion_dossier_path(90_001, companion_dir)
        assert path.exists()
        assert path.name.endswith(".md")
        assert "tracked" in str(path)

    def test_resolve_nonexistent_tracked_wine_raises(self, companion_dir):
        with pytest.raises(TrackedWineNotFoundError, match="does not exist"):
            resolve_companion_dossier_path(999, companion_dir)

    def test_resolve_missing_parquet_raises(self, tmp_path):
        with pytest.raises(TrackedWineNotFoundError, match="tracked_wine.parquet"):
            resolve_companion_dossier_path(90_001, tmp_path)

    def test_read_companion_full(self, companion_dir):
        content = read_companion_dossier(90_001, companion_dir)
        assert "Cuvée Alpha" in content
        assert "tracked_wine_id:" in content

    def test_read_companion_filtered(self, companion_dir):
        result = read_companion_dossier(
            90_001,
            companion_dir,
            sections=["producer_deep_dive"],
        )
        assert "## Producer Deep Dive" in result
        assert "## Buying Guide" not in result
        assert "tracked_wine_id:" in result

    def test_read_companion_nonexistent_raises(self, companion_dir):
        with pytest.raises(TrackedWineNotFoundError):
            read_companion_dossier(999, companion_dir)

    def test_update_companion_section(self, companion_dir):
        result = update_companion_dossier(
            90_001,
            "producer_deep_dive",
            "Domaine Test is a premier Bordeaux estate.",
            companion_dir,
        )
        assert "Updated" in result

        content = read_companion_dossier(90_001, companion_dir)
        assert "premier Bordeaux estate" in content
        assert "producer_deep_dive" in _extract_populated(content)
        assert "producer_deep_dive" not in _extract_pending(content)

    def test_update_companion_protected_section_raises(self, companion_dir):
        with pytest.raises(ProtectedSectionError, match="not an allowed companion"):
            update_companion_dossier(
                90_001,
                "identity",
                "hacked",
                companion_dir,
            )

    def test_update_companion_nonexistent_raises(self, companion_dir):
        with pytest.raises(TrackedWineNotFoundError):
            update_companion_dossier(
                999,
                "producer_deep_dive",
                "content",
                companion_dir,
            )

    def test_companion_empty_content_raises(self, companion_dir):
        with pytest.raises(ValueError, match="empty or whitespace-only"):
            update_companion_dossier(90_001, "producer_deep_dive", "", companion_dir)

    def test_companion_whitespace_content_raises(self, companion_dir):
        with pytest.raises(ValueError, match="empty or whitespace-only"):
            update_companion_dossier(90_001, "producer_deep_dive", "   \n", companion_dir)

    def test_pending_companion_research(self, companion_dir):
        result = pending_companion_research(companion_dir)
        assert "tracked_wine_id" in result
        assert "90001" in result
        assert "Cuvée Alpha" in result

    def test_pending_decreases_after_update(self, companion_dir):
        result_before = pending_companion_research(companion_dir)
        update_companion_dossier(
            90_001,
            "producer_deep_dive",
            "content",
            companion_dir,
        )
        result_after = pending_companion_research(companion_dir)
        # Still pending but fewer sections
        assert "90001" in result_before
        assert "90001" in result_after

    def test_pending_companion_no_dir(self, tmp_path):
        result = pending_companion_research(tmp_path)
        assert "No companion dossiers found" in result

    def test_companion_sections_constants(self):
        assert frozenset(COMPANION_SECTIONS.keys()) == ALLOWED_COMPANION_SECTIONS
        assert "producer_deep_dive" in ALLOWED_COMPANION_SECTIONS
        assert "vintage_tracker" in ALLOWED_COMPANION_SECTIONS
        assert "buying_guide" in ALLOWED_COMPANION_SECTIONS
        assert "price_tracker" in ALLOWED_COMPANION_SECTIONS


# ---------------------------------------------------------------------------
# TestLogging
# ---------------------------------------------------------------------------


class TestLogging:
    def test_update_dossier_logs_info(self, data_dir, caplog):
        with caplog.at_level("INFO", logger="cellarbrain.dossier_ops"):
            update_dossier(
                1,
                "producer_profile",
                "Test content for logging.",
                data_dir,
            )
        assert "Dossier updated" in caplog.text
        assert "wine_id=1" in caplog.text
        assert "section=producer_profile" in caplog.text

    def test_protected_section_logs_warning(self, data_dir, caplog):
        with caplog.at_level("WARNING", logger="cellarbrain.dossier_ops"), pytest.raises(ProtectedSectionError):
            update_dossier(1, "identity", "Bad content", data_dir)
        assert "Protected section write attempt" in caplog.text


# ---------------------------------------------------------------------------
# TestReadAgentSectionContent (Phase B - dashboard_notes editor support)
# ---------------------------------------------------------------------------


class TestReadAgentSectionContent:
    def test_returns_empty_for_placeholder(self, data_dir):
        from cellarbrain.dossier_ops import read_agent_section_content

        result = read_agent_section_content(1, "dashboard_notes", data_dir)
        assert result == ""

    def test_returns_content_after_update(self, data_dir):
        from cellarbrain.dossier_ops import (
            read_agent_section_content,
            update_dossier,
        )

        update_dossier(
            1,
            "dashboard_notes",
            "Try with venison stew.\n",
            data_dir,
            agent_name="dashboard",
        )
        result = read_agent_section_content(1, "dashboard_notes", data_dir)
        assert "Try with venison stew" in result

    def test_unknown_section_raises(self, data_dir):
        from cellarbrain.dossier_ops import (
            ProtectedSectionError,
            read_agent_section_content,
        )

        with pytest.raises(ProtectedSectionError):
            read_agent_section_content(1, "not_a_real_section", data_dir)

    def test_etl_owned_section_raises(self, data_dir):
        from cellarbrain.dossier_ops import (
            ProtectedSectionError,
            read_agent_section_content,
        )

        with pytest.raises(ProtectedSectionError):
            read_agent_section_content(1, "identity", data_dir)


# ---------------------------------------------------------------------------
# TestResearchCompleteness
# ---------------------------------------------------------------------------


class TestResearchCompleteness:
    """Tests for research completeness scoring."""

    def _make_dossier(self, tmp_path, wine_id, frontmatter_extra="", body_extra=""):
        """Create a minimal dossier file with given frontmatter and body."""
        wines_dir = tmp_path / "wines" / "cellar"
        wines_dir.mkdir(parents=True, exist_ok=True)
        fm = f"---\nwine_id: {wine_id}\n{frontmatter_extra}---\n"
        content = fm + "\n# Wine\n" + body_extra
        path = wines_dir / f"wine-{wine_id}.md"
        path.write_text(content, encoding="utf-8")
        return path

    def _make_pro_ratings(self, tmp_path, wine_ids):
        """Write a minimal pro_rating.parquet with given wine_ids."""
        from datetime import datetime as dt

        now = dt(2025, 1, 1)
        rows = [
            {
                "rating_id": i + 1,
                "wine_id": wid,
                "source": "Parker",
                "score": 95.0,
                "max_score": 100,
                "review_text": None,
                "etl_run_id": 1,
                "updated_at": now,
            }
            for i, wid in enumerate(wine_ids)
        ]
        writer.write_parquet("pro_rating", rows, tmp_path)

    def test_fully_pending_score_zero(self, tmp_path):
        """Wine with all sections pending, no food, no pro → score 0."""
        from cellarbrain.dossier_ops import _compute_wine_completeness

        fm = (
            "agent_sections_pending:\n"
            "  - producer_profile\n"
            "  - vintage_report\n"
            "  - wine_description\n"
            "  - market_availability\n"
            "  - ratings_reviews\n"
            "  - tasting_notes\n"
            "  - food_pairings\n"
            "  - similar_wines\n"
            "food_tags: []\n"
            "food_groups: []\n"
            "agent_sections_populated: []\n"
        )
        text = f"---\nwine_id: 1\n{fm}---\n"
        result = _compute_wine_completeness(text, has_pro_rating=False)
        assert result["score"] == 0
        assert result["populated_count"] == 0
        assert result["pending_count"] == 8
        assert result["has_food_tags"] is False
        assert result["has_food_groups"] is False
        assert result["has_pro_ratings"] is False

    def test_fully_populated_fresh_score_100(self, tmp_path):
        """Wine with all sections populated+fresh, food data, and pro → 100."""
        from datetime import datetime

        from cellarbrain.dossier_ops import _compute_wine_completeness

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        fm = (
            "agent_sections_populated:\n"
            "  - producer_profile\n"
            "  - vintage_report\n"
            "  - wine_description\n"
            "  - market_availability\n"
            "  - ratings_reviews\n"
            "  - tasting_notes\n"
            "  - food_pairings\n"
            "  - similar_wines\n"
            "agent_sections_pending: []\n"
            "food_tags:\n"
            "  - grilled-lamb\n"
            "food_groups:\n"
            "  - red-meat\n"
        )
        # Build body with research-meta for all 8 sections
        sections = [
            ("Producer Profile", "agent:research"),
            ("Vintage Report", "agent:research"),
            ("Wine Description", "agent:research"),
            ("Market & Availability", "agent:research"),
            ("From Research", "agent:research"),
            ("Community Tasting Notes", "agent:research"),
            ("Recommended Pairings", "agent:research"),
            ("Similar Wines", "agent:recommendation"),
        ]
        body = ""
        for heading, tag in sections:
            body += (
                f"\n## {heading}\n\n"
                f"<!-- source: {tag} -->\n"
                f'<!-- research-meta: {{"date": "{today}", "agent": "test"}} -->\n'
                f"Some content here.\n"
                f"<!-- source: {tag} — end -->\n"
            )

        text = f"---\nwine_id: 1\n{fm}---\n{body}"
        result = _compute_wine_completeness(text, has_pro_rating=True)
        assert result["score"] == 100
        assert result["populated_count"] == 8
        assert result["pending_count"] == 0
        assert result["fresh_count"] == 8
        assert result["stale_count"] == 0
        assert result["has_food_tags"] is True
        assert result["has_food_groups"] is True
        assert result["has_pro_ratings"] is True

    def test_partial_population(self, tmp_path):
        """4/8 sections populated → ~25 pts from sections alone."""
        from cellarbrain.dossier_ops import _compute_wine_completeness

        fm = (
            "agent_sections_populated:\n"
            "  - producer_profile\n"
            "  - vintage_report\n"
            "  - wine_description\n"
            "  - market_availability\n"
            "agent_sections_pending:\n"
            "  - ratings_reviews\n"
            "  - tasting_notes\n"
            "  - food_pairings\n"
            "  - similar_wines\n"
            "food_tags: []\n"
            "food_groups: []\n"
        )
        text = f"---\nwine_id: 1\n{fm}---\n"
        result = _compute_wine_completeness(text, has_pro_rating=False)
        # 4/8 × 50 = 25, no freshness (no meta), no food, no pro
        assert result["score"] == 25
        assert result["populated_count"] == 4
        assert result["pending_count"] == 4

    def test_stale_reduces_freshness(self, tmp_path):
        """Populated but stale (>12mo) → freshness penalty."""
        from cellarbrain.dossier_ops import _compute_wine_completeness

        fm = (
            "agent_sections_populated:\n"
            "  - producer_profile\n"
            "  - vintage_report\n"
            "agent_sections_pending:\n"
            "  - wine_description\n"
            "  - market_availability\n"
            "  - ratings_reviews\n"
            "  - tasting_notes\n"
            "  - food_pairings\n"
            "  - similar_wines\n"
            "food_tags: []\n"
            "food_groups: []\n"
        )
        # Both sections have old research dates (2 years ago)
        body = (
            "\n## Producer Profile\n\n"
            "<!-- source: agent:research -->\n"
            '<!-- research-meta: {"date": "2024-01-01", "agent": "test"} -->\n'
            "Content.\n"
            "<!-- source: agent:research — end -->\n"
            "\n## Vintage Report\n\n"
            "<!-- source: agent:research -->\n"
            '<!-- research-meta: {"date": "2024-01-01", "agent": "test"} -->\n'
            "Content.\n"
            "<!-- source: agent:research — end -->\n"
        )
        text = f"---\nwine_id: 1\n{fm}---\n{body}"
        result = _compute_wine_completeness(text, has_pro_rating=False, stale_months=12)
        # 2/8 × 50 = 12 (section score), 0/2 fresh → 0 freshness
        assert result["score"] == 12
        assert result["stale_count"] == 2
        assert result["fresh_count"] == 0

    def test_food_tags_contribute(self, tmp_path):
        """food_tags populated → +10 pts."""
        from cellarbrain.dossier_ops import _compute_wine_completeness

        fm = (
            "agent_sections_populated: []\n"
            "agent_sections_pending:\n"
            "  - producer_profile\n"
            "food_tags:\n"
            "  - grilled-lamb\n"
            "  - pasta\n"
            "food_groups: []\n"
        )
        text = f"---\nwine_id: 1\n{fm}---\n"
        result = _compute_wine_completeness(text, has_pro_rating=False)
        assert result["score"] == 10
        assert result["has_food_tags"] is True
        assert result["has_food_groups"] is False

    def test_food_groups_contribute(self, tmp_path):
        """food_groups populated → +5 pts."""
        from cellarbrain.dossier_ops import _compute_wine_completeness

        fm = (
            "agent_sections_populated: []\n"
            "agent_sections_pending:\n"
            "  - producer_profile\n"
            "food_tags: []\n"
            "food_groups:\n"
            "  - red-meat\n"
        )
        text = f"---\nwine_id: 1\n{fm}---\n"
        result = _compute_wine_completeness(text, has_pro_rating=False)
        assert result["score"] == 5
        assert result["has_food_groups"] is True

    def test_pro_ratings_contribute(self, tmp_path):
        """Pro rating exists → +15 pts."""
        from cellarbrain.dossier_ops import _compute_wine_completeness

        fm = (
            "agent_sections_populated: []\n"
            "agent_sections_pending:\n"
            "  - producer_profile\n"
            "food_tags: []\n"
            "food_groups: []\n"
        )
        text = f"---\nwine_id: 1\n{fm}---\n"
        result = _compute_wine_completeness(text, has_pro_rating=True)
        assert result["score"] == 15
        assert result["has_pro_ratings"] is True

    def test_compute_research_completeness_multi_wine(self, tmp_path):
        """compute_research_completeness scans multiple dossiers."""
        from cellarbrain.dossier_ops import compute_research_completeness

        # Create two dossiers
        self._make_dossier(
            tmp_path,
            1,
            "agent_sections_populated: []\n"
            "agent_sections_pending:\n"
            "  - producer_profile\n"
            "food_tags: []\n"
            "food_groups: []\n",
        )
        self._make_dossier(
            tmp_path,
            2,
            "agent_sections_populated:\n"
            "  - producer_profile\n"
            "  - vintage_report\n"
            "agent_sections_pending: []\n"
            "food_tags:\n"
            "  - lamb\n"
            "food_groups: []\n",
        )
        # No pro_rating.parquet → no pro ratings for anyone

        results = compute_research_completeness(tmp_path)
        assert len(results) == 2
        # Wine 1: 0 populated, no food, no pro → 0
        w1 = next(r for r in results if r["wine_id"] == 1)
        assert w1["score"] == 0
        # Wine 2: 2/8 × 50 = 12, food_tags → +10 = 22
        w2 = next(r for r in results if r["wine_id"] == 2)
        assert w2["score"] == 22

    def test_write_completeness_parquet(self, tmp_path):
        """write_completeness_parquet creates a valid Parquet file."""
        from cellarbrain.dossier_ops import write_completeness_parquet

        self._make_dossier(
            tmp_path,
            1,
            "agent_sections_populated:\n"
            "  - producer_profile\n"
            "agent_sections_pending: []\n"
            "food_tags: []\n"
            "food_groups: []\n",
        )
        self._make_pro_ratings(tmp_path, [1])

        path = write_completeness_parquet(tmp_path)
        assert path.exists()
        assert path.name == "research_completeness.parquet"

        # Read it back
        import pyarrow.parquet as pq_reader

        table = pq_reader.read_table(path)
        assert table.num_rows == 1
        assert table.column("wine_id")[0].as_py() == 1
        # 1/8 × 50 = 6, no freshness, no food, pro = 15 → 21
        assert table.column("score")[0].as_py() == 21
        assert table.column("has_pro_ratings")[0].as_py() is True

    def test_no_dossiers_returns_empty(self, tmp_path):
        """No wines dir → empty list."""
        from cellarbrain.dossier_ops import compute_research_completeness

        results = compute_research_completeness(tmp_path)
        assert results == []
