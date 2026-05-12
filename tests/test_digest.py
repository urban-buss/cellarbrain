"""Tests for the cellar digest module."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from cellarbrain.digest import (
    DigestResult,
    EtlChange,
    InventorySummary,
    WineSummary,
    build_digest,
    format_digest,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def con():
    """In-memory DuckDB with wines and etl_run views matching digest SQL."""
    db = duckdb.connect(":memory:")
    db.execute("""
        CREATE TABLE wines (
            wine_id INTEGER,
            wine_name VARCHAR,
            winery_name VARCHAR,
            vintage INTEGER,
            bottles_stored INTEGER,
            drinking_status VARCHAR,
            optimal_from INTEGER,
            optimal_until INTEGER,
            cellar_value DOUBLE
        )
    """)
    db.execute("""
        CREATE TABLE etl_run (
            etl_timestamp TIMESTAMP,
            etl_mode VARCHAR,
            total_inserts INTEGER,
            total_updates INTEGER,
            total_deletes INTEGER
        )
    """)
    # Insert test wines
    db.execute("""
        INSERT INTO wines VALUES
        (1, 'Old Merlot', 'Domaine A', 2015, 2, 'past_optimal', 2018, 2022, 60.0),
        (2, 'Past Peak', 'Domaine B', 2014, 1, 'past_optimal', 2017, 2021, 45.0),
        (3, 'Just Right', 'Domaine C', 2019, 3, 'optimal', 2025, 2030, 90.0),
        (4, 'Still Young', 'Domaine D', 2022, 6, 'too_young', 2028, 2035, 150.0),
        (5, 'Daily Sipper', 'Domaine E', 2021, 4, 'drinkable', NULL, NULL, 40.0),
        (6, 'Consumed', 'Domaine F', 2018, 0, 'optimal', 2023, 2028, 0.0)
    """)
    db.execute("""
        INSERT INTO etl_run VALUES
        ('2025-06-01 08:00:00', 'full', 5, 2, 1)
    """)
    yield db
    db.close()


# ---------------------------------------------------------------------------
# build_digest tests
# ---------------------------------------------------------------------------


class TestBuildDigest:
    """Unit tests for build_digest."""

    def test_returns_digest_result(self, con):
        result = build_digest(con, None, today=date(2025, 6, 2), period="daily")
        assert isinstance(result, DigestResult)
        assert result.date == date(2025, 6, 2)
        assert result.period == "daily"

    def test_drink_soon_finds_past_optimal(self, con):
        result = build_digest(con, None, today=date(2025, 6, 2), period="daily")
        assert len(result.drink_soon) == 2
        # Should be ordered by optimal_until ASC
        assert result.drink_soon[0].wine_id == 2  # optimal_until=2021
        assert result.drink_soon[1].wine_id == 1  # optimal_until=2022

    def test_drink_soon_excludes_zero_bottles(self, con):
        result = build_digest(con, None, today=date(2025, 6, 2), period="daily")
        ids = [w.wine_id for w in result.drink_soon]
        assert 6 not in ids  # bottles_stored=0

    def test_newly_optimal_finds_current_year(self, con):
        result = build_digest(con, None, today=date(2025, 6, 2), period="daily")
        assert len(result.newly_optimal) == 1
        assert result.newly_optimal[0].wine_id == 3  # optimal_from=2025

    def test_newly_optimal_empty_when_no_match(self, con):
        result = build_digest(con, None, today=date(2030, 1, 1), period="daily")
        # Only wine_id=4 has optimal_from=2028, nothing for 2030
        assert len(result.newly_optimal) == 0

    def test_inventory_summary(self, con):
        result = build_digest(con, None, today=date(2025, 6, 2), period="daily")
        inv = result.inventory
        assert inv is not None
        # Only wines with bottles_stored > 0 count
        assert inv.total_wines == 5
        assert inv.total_bottles == 16  # 2+1+3+6+4
        assert inv.past_optimal_count == 2
        assert inv.optimal_count == 1
        assert inv.too_young_count == 1

    def test_recent_changes(self, con):
        result = build_digest(con, None, today=date(2025, 6, 2), period="daily")
        ch = result.recent_changes
        assert ch is not None
        assert ch.last_run_mode == "full"
        assert ch.inserts == 5
        assert ch.updates == 2
        assert ch.deletes == 1

    def test_period_weekly(self, con):
        result = build_digest(con, None, today=date(2025, 6, 8), period="weekly")
        assert result.period == "weekly"

    def test_top_pick_graceful_when_recommend_fails(self, con):
        # recommend() requires wines_full view which doesn't exist here
        result = build_digest(con, None, today=date(2025, 6, 2), period="daily")
        # Should not raise — gracefully None
        assert result.top_pick is None


class TestBuildDigestEmptyDb:
    """Tests with empty database."""

    @pytest.fixture()
    def empty_con(self):
        db = duckdb.connect(":memory:")
        db.execute("""
            CREATE TABLE wines (
                wine_id INTEGER, wine_name VARCHAR, winery_name VARCHAR,
                vintage INTEGER, bottles_stored INTEGER, drinking_status VARCHAR,
                optimal_from INTEGER, optimal_until INTEGER, cellar_value DOUBLE
            )
        """)
        db.execute("""
            CREATE TABLE etl_run (
                etl_timestamp TIMESTAMP, etl_mode VARCHAR,
                total_inserts INTEGER, total_updates INTEGER, total_deletes INTEGER
            )
        """)
        yield db
        db.close()

    def test_empty_returns_empty_lists(self, empty_con):
        result = build_digest(empty_con, None, today=date(2025, 1, 1), period="daily")
        assert result.drink_soon == []
        assert result.newly_optimal == []
        assert result.top_pick is None

    def test_empty_inventory(self, empty_con):
        result = build_digest(empty_con, None, today=date(2025, 1, 1), period="daily")
        inv = result.inventory
        assert inv is not None
        assert inv.total_wines == 0
        assert inv.total_bottles == 0


# ---------------------------------------------------------------------------
# format_digest tests
# ---------------------------------------------------------------------------


class TestFormatDigest:
    """Tests for format_digest output."""

    def test_header_contains_date_and_period(self):
        result = DigestResult(date=date(2025, 6, 15), period="daily")
        output = format_digest(result)
        assert "Daily Cellar Digest" in output
        assert "15 June 2025" in output

    def test_weekly_header(self):
        result = DigestResult(date=date(2025, 6, 15), period="weekly")
        output = format_digest(result)
        assert "Weekly Cellar Digest" in output

    def test_drink_soon_section(self):
        result = DigestResult(
            date=date(2025, 6, 15),
            period="daily",
            drink_soon=[
                WineSummary(
                    wine_id=1,
                    wine_name="Old Merlot",
                    winery_name="Domaine A",
                    vintage=2015,
                    bottles_stored=2,
                    drinking_status="past_optimal",
                    optimal_until=2022,
                ),
            ],
        )
        output = format_digest(result)
        assert "Drink Soon" in output
        assert "Domaine A Old Merlot" in output
        assert "2015" in output
        assert "optimal until 2022" in output

    def test_newly_optimal_section(self):
        result = DigestResult(
            date=date(2025, 6, 15),
            period="daily",
            newly_optimal=[
                WineSummary(
                    wine_id=3,
                    wine_name="Pinot Noir",
                    winery_name="Domaine C",
                    vintage=2019,
                    bottles_stored=3,
                    drinking_status="optimal",
                ),
            ],
        )
        output = format_digest(result)
        assert "Newly Optimal" in output
        assert "Domaine C Pinot Noir" in output

    def test_top_pick_section(self):
        result = DigestResult(
            date=date(2025, 6, 15),
            period="daily",
            top_pick=WineSummary(
                wine_id=2,
                wine_name="Chardonnay",
                winery_name="Domaine B",
                vintage=2020,
                bottles_stored=4,
                drinking_status="optimal",
            ),
            top_pick_reason="Optimal window, high urgency",
        )
        output = format_digest(result)
        assert "Today's Pick" in output
        assert "Domaine B Chardonnay" in output
        assert "Optimal window, high urgency" in output

    def test_inventory_section(self):
        result = DigestResult(
            date=date(2025, 6, 15),
            period="daily",
            inventory=InventorySummary(
                total_wines=50,
                total_bottles=120,
                total_value=5000.0,
                past_optimal_count=3,
                optimal_count=20,
                too_young_count=10,
            ),
        )
        output = format_digest(result)
        assert "50 wines" in output
        assert "120 bottles" in output
        assert "CHF 5,000" in output
        assert "20 optimal" in output

    def test_etl_changes_section(self):
        result = DigestResult(
            date=date(2025, 6, 15),
            period="daily",
            recent_changes=EtlChange(
                last_run_timestamp="2025-06-14 08:00:00",
                last_run_mode="incremental",
                inserts=3,
                updates=1,
                deletes=0,
            ),
        )
        output = format_digest(result)
        assert "Last ETL Run" in output
        assert "+3 new" in output
        assert "~1 updated" in output

    def test_empty_state_message(self):
        result = DigestResult(date=date(2025, 6, 15), period="daily")
        output = format_digest(result)
        assert "No actionable items today" in output

    def test_promotions_section(self):
        result = DigestResult(
            date=date(2025, 6, 15),
            period="daily",
            promotion_matches=["Promotion data available"],
        )
        output = format_digest(result)
        assert "Promotions" in output
        assert "Promotion data available" in output
