"""Tests for the wine travel planner module."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from cellarbrain import markdown
from cellarbrain.dossier_ops import update_dossier
from cellarbrain.query import get_agent_connection
from cellarbrain.travel import (
    ProducerVisit,
    RegionGap,
    TravelBrief,
    _extract_producer_visits,
    _match_destination,
    _region_gaps,
    _region_inventory,
    build_travel_brief,
    format_travel_brief,
)
from dataset_factory import (
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_pro_rating,
    make_provider,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)

# ---------------------------------------------------------------------------
# Fixture: multi-region dataset
# ---------------------------------------------------------------------------


def _build_travel_dataset(tmp_path):
    """Create a dataset with wines from multiple regions for travel tests."""
    appellations = [
        make_appellation(appellation_id=1, country="Italy", region="Piedmont", subregion="Barolo"),
        make_appellation(appellation_id=2, country="Italy", region="Piedmont", subregion="Barbaresco"),
        make_appellation(appellation_id=3, country="France", region="Burgundy", subregion="Côte de Nuits"),
        make_appellation(appellation_id=4, country="Italy", region="Tuscany", subregion="Chianti"),
    ]
    wineries = [
        make_winery(winery_id=1, name="Giacomo Conterno"),
        make_winery(winery_id=2, name="Bruno Giacosa"),
        make_winery(winery_id=3, name="Domaine Leroy"),
        make_winery(winery_id=4, name="Antinori"),
    ]
    grapes = [
        make_grape(grape_id=1, name="Nebbiolo"),
        make_grape(grape_id=2, name="Pinot Noir"),
        make_grape(grape_id=3, name="Sangiovese"),
    ]
    wines = [
        make_wine(
            wine_id=1,
            winery_id=1,
            winery_name="Giacomo Conterno",
            name="Barolo Monfortino",
            vintage=2016,
            appellation_id=1,
            primary_grape="Nebbiolo",
            grape_type="varietal",
            drinking_status="optimal",
            list_price=Decimal("250.00"),
            price_tier="fine",
        ),
        make_wine(
            wine_id=2,
            winery_id=2,
            winery_name="Bruno Giacosa",
            name="Barbaresco Rabajà",
            vintage=2017,
            appellation_id=2,
            primary_grape="Nebbiolo",
            grape_type="varietal",
            drinking_status="drinkable",
            list_price=Decimal("120.00"),
            price_tier="premium",
        ),
        make_wine(
            wine_id=3,
            winery_id=3,
            winery_name="Domaine Leroy",
            name="Vosne-Romanée",
            vintage=2018,
            appellation_id=3,
            primary_grape="Pinot Noir",
            grape_type="varietal",
            drinking_status="optimal",
            list_price=Decimal("300.00"),
            price_tier="fine",
        ),
        # Consumed wine from Tuscany (for gap analysis)
        make_wine(
            wine_id=4,
            winery_id=4,
            winery_name="Antinori",
            name="Tignanello",
            vintage=2015,
            appellation_id=4,
            primary_grape="Sangiovese",
            grape_type="blend",
            drinking_status="past_optimal",
        ),
    ]
    wine_grapes = [
        make_wine_grape(wine_id=1, grape_id=1),
        make_wine_grape(wine_id=2, grape_id=1),
        make_wine_grape(wine_id=3, grape_id=2),
        make_wine_grape(wine_id=4, grape_id=3),
    ]
    bottles = [
        # 3 stored bottles for wine 1
        make_bottle(bottle_id=1, wine_id=1, purchase_price=Decimal("250.00")),
        make_bottle(bottle_id=2, wine_id=1, purchase_price=Decimal("250.00")),
        make_bottle(bottle_id=3, wine_id=1, purchase_price=Decimal("250.00")),
        # 2 stored bottles for wine 2
        make_bottle(bottle_id=4, wine_id=2, purchase_price=Decimal("120.00")),
        make_bottle(bottle_id=5, wine_id=2, purchase_price=Decimal("120.00")),
        # 1 stored bottle for wine 3 (Burgundy)
        make_bottle(bottle_id=6, wine_id=3, purchase_price=Decimal("300.00")),
        # 1 consumed bottle for wine 4 (Tuscany — no stored bottles)
        make_bottle(bottle_id=7, wine_id=4, status="consumed", output_date=date(2024, 6, 1), output_type="consumed"),
    ]

    entities = {
        "winery": wineries,
        "appellation": appellations,
        "grape": grapes,
        "wine": wines,
        "wine_grape": wine_grapes,
        "bottle": bottles,
        "cellar": [make_cellar()],
        "provider": [make_provider()],
        "tasting": [],
        "pro_rating": [make_pro_rating(rating_id=1, wine_id=1, score=96.0)],
        "etl_run": [make_etl_run()],
        "change_log": [make_change_log()],
    }

    write_dataset(tmp_path, entities)
    markdown.generate_dossiers(entities, tmp_path, current_year=2026)
    return tmp_path


@pytest.fixture()
def data_dir(tmp_path):
    return _build_travel_dataset(tmp_path)


@pytest.fixture()
def con(data_dir):
    return get_agent_connection(data_dir)


# ---------------------------------------------------------------------------
# TestMatchDestination
# ---------------------------------------------------------------------------


class TestMatchDestination:
    def test_match_region(self, con):
        assert _match_destination(con, "Piedmont") == "region"

    def test_match_country(self, con):
        assert _match_destination(con, "Italy") == "country"

    def test_match_subregion(self, con):
        assert _match_destination(con, "Barolo") == "subregion"

    def test_no_match(self, con):
        assert _match_destination(con, "Narnia") is None

    def test_accent_insensitive(self, con):
        # "Côte de Nuits" should match without accent
        assert _match_destination(con, "Cote de Nuits") == "subregion"


# ---------------------------------------------------------------------------
# TestRegionInventory
# ---------------------------------------------------------------------------


class TestRegionInventory:
    def test_returns_matching_wines(self, con):
        wines = _region_inventory(con, "Piedmont")
        assert len(wines) == 2
        names = {w["wine_name"] for w in wines}
        assert "Giacomo Conterno Barolo Monfortino 2016" in names
        assert "Bruno Giacosa Barbaresco Rabajà 2017" in names

    def test_country_match_returns_stored_only(self, con):
        wines = _region_inventory(con, "Italy")
        # wine 4 (Tuscany) is consumed, so only Piedmont wines stored
        assert len(wines) == 2
        for w in wines:
            assert w["bottles_stored"] > 0

    def test_limit(self, con):
        wines = _region_inventory(con, "Italy", limit=1)
        assert len(wines) == 1

    def test_no_match_returns_empty(self, con):
        wines = _region_inventory(con, "Narnia")
        assert wines == []

    def test_includes_expected_columns(self, con):
        wines = _region_inventory(con, "Piedmont")
        assert len(wines) > 0
        w = wines[0]
        assert "wine_id" in w
        assert "winery_name" in w
        assert "region" in w
        assert "drinking_status" in w


# ---------------------------------------------------------------------------
# TestExtractProducerVisits
# ---------------------------------------------------------------------------


class TestExtractProducerVisits:
    def test_returns_empty_when_no_profiles(self, data_dir, con):
        wines = _region_inventory(con, "Piedmont")
        visits = _extract_producer_visits(wines, data_dir)
        # No producer_profile has been written yet, so empty
        assert visits == []

    def test_returns_profile_when_populated(self, data_dir, con):
        # Write a producer_profile to wine 1's dossier
        update_dossier(
            1,
            "producer_profile",
            "Historic Barolo producer founded in 1908. Traditional winemaking.",
            data_dir,
        )
        wines = _region_inventory(con, "Piedmont")
        visits = _extract_producer_visits(wines, data_dir)
        assert len(visits) == 1
        assert visits[0].winery_name == "Giacomo Conterno"
        assert "1908" in visits[0].profile_excerpt

    def test_max_producers_cap(self, data_dir, con):
        update_dossier(1, "producer_profile", "Producer one.", data_dir)
        update_dossier(2, "producer_profile", "Producer two.", data_dir)
        wines = _region_inventory(con, "Piedmont")
        visits = _extract_producer_visits(wines, data_dir, max_producers=1)
        assert len(visits) == 1

    def test_sorted_by_bottle_count(self, data_dir, con):
        update_dossier(1, "producer_profile", "Conterno profile.", data_dir)
        update_dossier(2, "producer_profile", "Giacosa profile.", data_dir)
        wines = _region_inventory(con, "Piedmont")
        visits = _extract_producer_visits(wines, data_dir, max_producers=5)
        # Wine 1 has 3 bottles, wine 2 has 2 → Conterno first
        assert visits[0].winery_name == "Giacomo Conterno"
        assert visits[1].winery_name == "Bruno Giacosa"


# ---------------------------------------------------------------------------
# TestRegionGaps
# ---------------------------------------------------------------------------


class TestRegionGaps:
    def test_country_level_finds_subregion_gaps(self, con):
        # Italy has Barolo, Barbaresco stored + Chianti consumed
        gaps = _region_gaps(con, "Italy", "country")
        subregion_gaps = [g for g in gaps if g.dimension == "subregion"]
        gap_values = {g.value for g in subregion_gaps}
        assert "Chianti" in gap_values

    def test_country_level_finds_grape_gaps(self, con):
        gaps = _region_gaps(con, "Italy", "country")
        grape_gaps = [g for g in gaps if g.dimension == "grape"]
        gap_values = {g.value for g in grape_gaps}
        assert "Sangiovese" in gap_values

    def test_region_level_finds_no_grape_gaps(self, con):
        # Piedmont only has Nebbiolo — no consumed grapes missing
        gaps = _region_gaps(con, "Piedmont", "region")
        grape_gaps = [g for g in gaps if g.dimension == "grape"]
        assert grape_gaps == []

    def test_subregion_level_returns_empty(self, con):
        # At subregion level, gap analysis isn't meaningful
        gaps = _region_gaps(con, "Barolo", "subregion")
        assert gaps == []


# ---------------------------------------------------------------------------
# TestBuildTravelBrief
# ---------------------------------------------------------------------------


class TestBuildTravelBrief:
    def test_returns_brief_for_region(self, con, data_dir):
        brief = build_travel_brief(con, "Piedmont", data_dir, include_producers=False)
        assert brief is not None
        assert brief.destination == "Piedmont"
        assert brief.match_level == "region"
        assert len(brief.wines) == 2
        assert brief.total_bottles == 5
        assert brief.winery_count == 2

    def test_returns_brief_for_country(self, con, data_dir):
        brief = build_travel_brief(con, "Italy", data_dir, include_producers=False)
        assert brief is not None
        assert brief.match_level == "country"
        assert len(brief.wines) == 2  # only stored wines

    def test_returns_none_for_no_match(self, con, data_dir):
        assert build_travel_brief(con, "Narnia", data_dir) is None

    def test_includes_gaps(self, con, data_dir):
        brief = build_travel_brief(con, "Italy", data_dir, include_producers=False)
        assert brief is not None
        assert len(brief.gaps) > 0

    def test_respects_include_gaps_false(self, con, data_dir):
        brief = build_travel_brief(
            con,
            "Italy",
            data_dir,
            include_producers=False,
            include_gaps=False,
        )
        assert brief is not None
        assert brief.gaps == []

    def test_respects_limit(self, con, data_dir):
        brief = build_travel_brief(
            con,
            "Piedmont",
            data_dir,
            include_producers=False,
            limit=1,
        )
        assert brief is not None
        assert len(brief.wines) == 1


# ---------------------------------------------------------------------------
# TestFormatTravelBrief
# ---------------------------------------------------------------------------


class TestFormatTravelBrief:
    def _make_brief(self) -> TravelBrief:
        wines = [
            {
                "wine_id": 1,
                "wine_name": "Barolo Monfortino",
                "vintage": 2016,
                "winery_name": "Conterno",
                "subregion": "Barolo",
                "best_pro_score": 96.0,
                "bottles_stored": 3,
                "bottles_consumed": 1,
                "drinking_status": "optimal",
                "cellar_value": 750.0,
            },
        ]
        return TravelBrief(
            destination="Piedmont",
            match_level="region",
            wines=wines,
            total_bottles=3,
            total_value=750.0,
            winery_count=1,
            subregion_count=1,
            producers=[
                ProducerVisit(
                    winery_name="Conterno",
                    subregion="Barolo",
                    wine_count=1,
                    profile_excerpt="Famous Barolo producer.",
                ),
            ],
            gaps=[
                RegionGap(dimension="subregion", value="Barbaresco"),
                RegionGap(dimension="grape", value="Dolcetto"),
            ],
        )

    def test_contains_title(self):
        text = format_travel_brief(self._make_brief())
        assert "## Wine Travel Brief: Piedmont" in text

    def test_contains_inventory_table(self):
        text = format_travel_brief(self._make_brief())
        assert "| Wine | Vintage | Winery |" in text
        assert "Barolo Monfortino" in text

    def test_contains_summary(self):
        text = format_travel_brief(self._make_brief())
        assert "1 winery" in text
        assert "CHF 750" in text

    def test_contains_highlights(self):
        text = format_travel_brief(self._make_brief())
        assert "Highest-rated" in text
        assert "96 pts" in text

    def test_contains_producer_visits(self):
        text = format_travel_brief(self._make_brief())
        assert "Producer Visit Suggestions" in text
        assert "**Conterno**" in text
        assert "Famous Barolo producer." in text

    def test_contains_gaps(self):
        text = format_travel_brief(self._make_brief())
        assert "Regional Gaps" in text
        assert "Barbaresco" in text
        assert "Dolcetto" in text

    def test_no_producers_section_when_empty(self):
        brief = self._make_brief()
        brief_no_prod = TravelBrief(
            destination=brief.destination,
            match_level=brief.match_level,
            wines=brief.wines,
            total_bottles=brief.total_bottles,
            total_value=brief.total_value,
            winery_count=brief.winery_count,
            subregion_count=brief.subregion_count,
            producers=[],
            gaps=brief.gaps,
        )
        text = format_travel_brief(brief_no_prod)
        assert "Producer Visit Suggestions" not in text

    def test_no_gaps_section_when_empty(self):
        brief = self._make_brief()
        brief_no_gaps = TravelBrief(
            destination=brief.destination,
            match_level=brief.match_level,
            wines=brief.wines,
            total_bottles=brief.total_bottles,
            total_value=brief.total_value,
            winery_count=brief.winery_count,
            subregion_count=brief.subregion_count,
            producers=brief.producers,
            gaps=[],
        )
        text = format_travel_brief(brief_no_gaps)
        assert "Regional Gaps" not in text


# ---------------------------------------------------------------------------
# End-to-end: build + format
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_full_pipeline(self, con, data_dir):
        brief = build_travel_brief(con, "Piedmont", data_dir, include_producers=False)
        assert brief is not None
        text = format_travel_brief(brief)
        assert "Wine Travel Brief: Piedmont" in text
        assert "Barolo Monfortino" in text or "Giacomo Conterno" in text
