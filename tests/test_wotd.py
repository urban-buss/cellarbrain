"""Tests for the Wine of the Day module."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from cellarbrain.wotd import (
    WineOfTheDay,
    _date_seed,
    format_wine_of_the_day,
    pick_wine_of_the_day,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def con():
    """In-memory DuckDB with wines_full view matching recommend.py's query."""
    db = duckdb.connect(":memory:")
    db.execute("""
        CREATE TABLE wines_full (
            wine_id INTEGER,
            wine_name VARCHAR,
            vintage INTEGER,
            winery_name VARCHAR,
            category VARCHAR,
            country VARCHAR,
            region VARCHAR,
            primary_grape VARCHAR,
            price DOUBLE,
            price_tier VARCHAR,
            drinking_status VARCHAR,
            bottles_stored INTEGER,
            volume_ml INTEGER,
            bottle_format VARCHAR,
            is_favorite BOOLEAN,
            best_pro_score DOUBLE,
            drink_from INTEGER,
            drink_until INTEGER,
            optimal_from INTEGER,
            optimal_until INTEGER,
            last_tasting_date DATE,
            tasting_count INTEGER
        )
    """)
    db.execute("""
        INSERT INTO wines_full VALUES
        (1, 'Barolo Cannubi', 2018, 'Marchesi', 'Red wine', 'Italy', 'Piedmont',
         'Nebbiolo', 65.0, 'premium', 'optimal', 3, 750, 'Standard', false,
         93.0, 2023, 2035, 2025, 2030, NULL, 0),
        (2, 'Hermitage Rouge', 2016, 'Jaboulet', 'Red wine', 'France', 'Rhône',
         'Syrah', 85.0, 'premium', 'past_optimal', 2, 750, 'Standard', true,
         95.0, 2020, 2028, 2022, 2026, NULL, 0),
        (3, 'Riesling GG', 2020, 'Dönnhoff', 'White wine', 'Germany', 'Nahe',
         'Riesling', 45.0, 'everyday', 'optimal', 5, 750, 'Standard', false,
         91.0, 2022, 2035, 2024, 2032, NULL, 0),
        (4, 'Brunello DOCG', 2017, 'Biondi-Santi', 'Red wine', 'Italy', 'Tuscany',
         'Sangiovese', 120.0, 'fine', 'optimal', 1, 750, 'Standard', true,
         96.0, 2022, 2040, 2025, 2035, NULL, 0),
        (5, 'Chablis Premier Cru', 2021, 'Dauvissat', 'White wine', 'France', 'Burgundy',
         'Chardonnay', 55.0, 'premium', 'drinkable', 4, 750, 'Standard', false,
         90.0, 2023, 2030, 2025, 2028, NULL, 0)
    """)
    yield db
    db.close()


@pytest.fixture()
def empty_con():
    """In-memory DuckDB with wines_full but no rows."""
    db = duckdb.connect(":memory:")
    db.execute("""
        CREATE TABLE wines_full (
            wine_id INTEGER,
            wine_name VARCHAR,
            vintage INTEGER,
            winery_name VARCHAR,
            category VARCHAR,
            country VARCHAR,
            region VARCHAR,
            primary_grape VARCHAR,
            price DOUBLE,
            price_tier VARCHAR,
            drinking_status VARCHAR,
            bottles_stored INTEGER,
            volume_ml INTEGER,
            bottle_format VARCHAR,
            is_favorite BOOLEAN,
            best_pro_score DOUBLE,
            drink_from INTEGER,
            drink_until INTEGER,
            optimal_from INTEGER,
            optimal_until INTEGER,
            last_tasting_date DATE,
            tasting_count INTEGER
        )
    """)
    yield db
    db.close()


# ---------------------------------------------------------------------------
# _date_seed tests
# ---------------------------------------------------------------------------


class TestDateSeed:
    """Unit tests for _date_seed."""

    def test_basic_conversion(self):
        assert _date_seed(date(2026, 5, 11)) == 20260511

    def test_leading_zeros(self):
        assert _date_seed(date(2026, 1, 1)) == 20260101


# ---------------------------------------------------------------------------
# pick_wine_of_the_day tests
# ---------------------------------------------------------------------------


class TestPickWineOfTheDay:
    """Unit tests for pick_wine_of_the_day."""

    def test_returns_wine_of_the_day_dataclass(self, con):
        result = pick_wine_of_the_day(con, today=date(2026, 5, 11))
        assert isinstance(result, WineOfTheDay)

    def test_deterministic_for_same_date(self, con):
        pick1 = pick_wine_of_the_day(con, today=date(2026, 5, 11))
        pick2 = pick_wine_of_the_day(con, today=date(2026, 5, 11))
        assert pick1 is not None
        assert pick2 is not None
        assert pick1.wine_id == pick2.wine_id

    def test_different_dates_can_differ(self, con):
        """Over a range of dates, the pick should vary (not always the same)."""
        picks = set()
        for day in range(1, 31):
            pick = pick_wine_of_the_day(con, today=date(2026, 5, day))
            if pick:
                picks.add(pick.wine_id)
        # With 5 wines and 30 days, we should see more than 1 distinct pick
        assert len(picks) > 1

    def test_empty_cellar_returns_none(self, empty_con):
        result = pick_wine_of_the_day(empty_con, today=date(2026, 5, 11))
        assert result is None

    def test_reason_is_non_empty(self, con):
        result = pick_wine_of_the_day(con, today=date(2026, 5, 11))
        assert result is not None
        assert len(result.reason) > 0

    def test_score_is_positive(self, con):
        result = pick_wine_of_the_day(con, today=date(2026, 5, 11))
        assert result is not None
        assert result.score > 0

    def test_wine_id_is_from_cellar(self, con):
        result = pick_wine_of_the_day(con, today=date(2026, 5, 11))
        assert result is not None
        assert result.wine_id in {1, 2, 3, 4, 5}


# ---------------------------------------------------------------------------
# format_wine_of_the_day tests
# ---------------------------------------------------------------------------


class TestFormatWineOfTheDay:
    """Unit tests for format_wine_of_the_day."""

    def test_formats_none(self):
        result = format_wine_of_the_day(None)
        assert "No wines available" in result

    def test_formats_pick(self):
        pick = WineOfTheDay(
            wine_id=1,
            wine_name="Barolo Cannubi",
            vintage=2018,
            winery_name="Marchesi",
            category="Red wine",
            region="Piedmont",
            primary_grape="Nebbiolo",
            price=65.0,
            drinking_status="optimal",
            bottles_stored=3,
            score=25.5,
            reason="At peak drinking right now. A chance to enjoy Nebbiolo.",
        )
        result = format_wine_of_the_day(pick)
        assert "Wine of the Day" in result
        assert "Barolo Cannubi" in result
        assert "Marchesi" in result
        assert "2018" in result
        assert "Nebbiolo" in result
        assert "Piedmont" in result
        assert "At peak" in result

    def test_formats_nv_wine(self):
        pick = WineOfTheDay(
            wine_id=2,
            wine_name="NV Champagne",
            vintage=None,
            winery_name="Krug",
            category="Sparkling wine",
            region="Champagne",
            primary_grape="Chardonnay",
            price=None,
            drinking_status=None,
            bottles_stored=1,
            score=10.0,
            reason="A great pick from your cellar.",
        )
        result = format_wine_of_the_day(pick)
        assert "NV" in result
        assert "1 bottle" in result
