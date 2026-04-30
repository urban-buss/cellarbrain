"""Tests for cellarbrain.flat — view SQL definitions and query.get_agent_connection."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb
import pytest

from cellarbrain.query import get_agent_connection

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Shared helper: minimal Parquet dataset
# ---------------------------------------------------------------------------
from dataset_factory import (
    make_appellation,
    make_bottle,
    make_cellar,
    make_change_log,
    make_etl_run,
    make_grape,
    make_pro_rating,
    make_provider,
    make_tasting,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)


def _make_dataset(tmp_path):
    """Write the 12 relational Parquet files needed by the views."""
    wines = [
        make_wine(
            wine_id=1,
            winery_id=1,
            winery_name="Château Test",
            name="Cuvée Alpha",
            vintage=2020,
            appellation_id=1,
            alcohol_pct=14.5,
            is_favorite=True,
            drink_from=2023,
            drink_until=2030,
            optimal_from=2025,
            optimal_until=2028,
            drinking_status="optimal",
            age_years=5,
        ),
        make_wine(
            wine_id=2,
            winery_id=2,
            winery_name="Bodega Ejemplo",
            name="Reserva Especial",
            vintage=2018,
            appellation_id=2,
            alcohol_pct=13.5,
            primary_grape="Tempranillo",
            grape_summary="Tempranillo",
            drink_from=2022,
            drink_until=2028,
            drinking_status="drinkable",
            age_years=7,
        ),
        make_wine(
            wine_id=3,
            winery_id=1,
            winery_name="Château Test",
            name="Deleted Cuvée",
            vintage=2015,
            appellation_id=None,
            alcohol_pct=None,
            grape_type="unknown",
            primary_grape=None,
            grape_summary=None,
            dossier_path="archive/0003-chateau-test-deleted-cuvee-2015.md",
            is_deleted=True,
        ),
    ]
    return write_dataset(
        tmp_path,
        {
            "winery": [
                make_winery(1, name="Château Test"),
                make_winery(2, name="Bodega Ejemplo"),
            ],
            "appellation": [
                make_appellation(1, subregion="Saint-Émilion", classification="Grand Cru"),
                make_appellation(2, country="Spain", region="Rioja", classification="Reserva"),
            ],
            "grape": [
                make_grape(1),
                make_grape(2, name="Tempranillo"),
            ],
            "wine": wines,
            "wine_grape": [
                make_wine_grape(1, 1),
                make_wine_grape(2, 2),
            ],
            "bottle": [
                make_bottle(1, 1),
                make_bottle(2, 1, shelf="A2", bottle_number=2),
                make_bottle(
                    3,
                    2,
                    status="consumed",
                    cellar_id=None,
                    shelf=None,
                    bottle_number=None,
                    provider_id=2,
                    purchase_date=date(2022, 1, 15),
                    original_purchase_price=Decimal("18.50"),
                    original_purchase_currency="EUR",
                    purchase_price=Decimal("17.21"),
                    output_date=date(2024, 12, 25),
                    output_type="consumed",
                    output_comment="Christmas dinner",
                ),
            ],
            "cellar": [make_cellar(name="Main Cellar")],
            "provider": [
                make_provider(1, name="Wine Shop A"),
                make_provider(2, name="Bodega Direct"),
            ],
            "tasting": [make_tasting()],
            "pro_rating": [make_pro_rating()],
            "etl_run": [
                make_etl_run(
                    total_inserts=10,
                    wines_inserted=2,
                    wines_source_hash="abc123",
                    bottles_source_hash="def456",
                )
            ],
            "change_log": [make_change_log()],
        },
    )


@pytest.fixture()
def data_dir(tmp_path):
    return _make_dataset(tmp_path)


@pytest.fixture()
def agent_con(data_dir):
    return get_agent_connection(data_dir)


# ---------------------------------------------------------------------------
# TestAgentConnectionViews
# ---------------------------------------------------------------------------


class TestAgentConnectionViews:
    def test_has_expected_views(self, agent_con):
        rows = agent_con.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall()
        names = sorted(r[0] for r in rows)
        assert names == [
            "_wines_wishlist",
            "bottles",
            "bottles_consumed",
            "bottles_full",
            "bottles_on_order",
            "bottles_stored",
            "format_groups",
            "wines",
            "wines_drinking_now",
            "wines_full",
            "wines_on_order",
            "wines_stored",
            "wines_wishlist",
        ]

    def test_wines_row_count(self, agent_con):
        # 3 wines in Parquet, 1 is deleted → only 2 visible in the view
        count = agent_con.execute("SELECT count(*) FROM wines").fetchone()[0]
        assert count == 2

    def test_bottles_row_count(self, agent_con):
        count = agent_con.execute("SELECT count(*) FROM bottles").fetchone()[0]
        assert count == 3

    def test_relational_table_not_accessible(self, agent_con):
        with pytest.raises(duckdb.CatalogException):
            agent_con.execute("SELECT count(*) FROM wine")


# ---------------------------------------------------------------------------
# TestWinesViewColumns
# ---------------------------------------------------------------------------


class TestWinesViewColumns:
    def test_expected_columns_present(self, agent_con):
        cols = {row[0] for row in agent_con.execute("DESCRIBE SELECT * FROM wines").fetchall()}
        expected = {
            "wine_id",
            "wine_name",
            "vintage",
            "winery_name",
            "category",
            "country",
            "region",
            "subregion",
            "primary_grape",
            "blend_type",
            "drinking_status",
            "price_tier",
            "price",
            "price_per_750ml",
            "volume_ml",
            "bottle_format",
            "format_group_id",
            "style_tags",
            "bottles_stored",
            "bottles_on_order",
            "bottles_consumed",
            "is_favorite",
            "is_wishlist",
            "tracked_wine_id",
        }
        assert cols == expected

    def test_no_etl_columns(self, agent_con):
        cols = {row[0] for row in agent_con.execute("DESCRIBE SELECT * FROM wines").fetchall()}
        assert "etl_run_id" not in cols
        assert "updated_at" not in cols


# ---------------------------------------------------------------------------
# TestBottlesViewColumns
# ---------------------------------------------------------------------------


class TestBottlesViewColumns:
    def test_expected_columns_present(self, agent_con):
        cols = {row[0] for row in agent_con.execute("DESCRIBE SELECT * FROM bottles").fetchall()}
        expected = {
            "bottle_id",
            "wine_id",
            "wine_name",
            "vintage",
            "winery_name",
            "category",
            "country",
            "region",
            "primary_grape",
            "drinking_status",
            "price_tier",
            "price",
            "price_per_750ml",
            "volume_ml",
            "bottle_format",
            "status",
            "cellar_name",
            "shelf",
            "output_date",
            "output_type",
        }
        assert cols == expected

    def test_no_fk_ids(self, agent_con):
        cols = {row[0] for row in agent_con.execute("DESCRIBE SELECT * FROM bottles").fetchall()}
        assert "winery_id" not in cols
        assert "appellation_id" not in cols
        assert "cellar_id" not in cols
        assert "provider_id" not in cols


# ---------------------------------------------------------------------------
# TestWinesViewAggregates
# ---------------------------------------------------------------------------


class TestWinesViewAggregates:
    def test_bottles_stored_wine1(self, agent_con):
        row = agent_con.execute("SELECT bottles_stored FROM wines WHERE wine_id = 1").fetchone()
        assert row[0] == 2

    def test_bottles_consumed_wine1(self, agent_con):
        row = agent_con.execute("SELECT bottles_consumed FROM wines WHERE wine_id = 1").fetchone()
        assert row[0] == 0

    def test_bottles_stored_wine2(self, agent_con):
        row = agent_con.execute("SELECT bottles_stored FROM wines WHERE wine_id = 2").fetchone()
        assert row[0] == 0

    def test_bottles_consumed_wine2(self, agent_con):
        row = agent_con.execute("SELECT bottles_consumed FROM wines WHERE wine_id = 2").fetchone()
        assert row[0] == 1

    def test_cellar_value_wine1(self, agent_con):
        row = agent_con.execute("SELECT cellar_value FROM wines_full WHERE wine_id = 1").fetchone()
        assert row[0] == pytest.approx(50.0)

    def test_tasting_count_wine1(self, agent_con):
        row = agent_con.execute("SELECT tasting_count FROM wines_full WHERE wine_id = 1").fetchone()
        assert row[0] == 1

    def test_last_tasting_score_wine1(self, agent_con):
        row = agent_con.execute("SELECT last_tasting_score FROM wines_full WHERE wine_id = 1").fetchone()
        assert row[0] == pytest.approx(92.0)

    def test_pro_rating_count_wine1(self, agent_con):
        row = agent_con.execute("SELECT pro_rating_count FROM wines_full WHERE wine_id = 1").fetchone()
        assert row[0] == 1

    def test_best_pro_score_wine1(self, agent_con):
        row = agent_con.execute("SELECT best_pro_score FROM wines_full WHERE wine_id = 1").fetchone()
        assert row[0] == pytest.approx(95.0)

    def test_no_tastings_wine2(self, agent_con):
        row = agent_con.execute("SELECT tasting_count, last_tasting_score FROM wines_full WHERE wine_id = 2").fetchone()
        assert row[0] == 0
        assert row[1] is None

    def test_no_ratings_wine2(self, agent_con):
        row = agent_con.execute("SELECT pro_rating_count, best_pro_score FROM wines_full WHERE wine_id = 2").fetchone()
        assert row[0] == 0
        assert row[1] is None


# ---------------------------------------------------------------------------
# TestBottlesViewDenormalization
# ---------------------------------------------------------------------------


class TestBottlesViewDenormalization:
    def test_winery_name_populated(self, agent_con):
        row = agent_con.execute("SELECT DISTINCT winery_name FROM bottles WHERE wine_id = 1").fetchone()
        assert row[0] == "Château Test"

    def test_country_populated(self, agent_con):
        row = agent_con.execute("SELECT DISTINCT country FROM bottles WHERE wine_id = 1").fetchone()
        assert row[0] == "France"

    def test_cellar_name_stored_bottle(self, agent_con):
        row = agent_con.execute("SELECT cellar_name FROM bottles WHERE bottle_id = 1").fetchone()
        assert row[0] == "Main Cellar"

    def test_cellar_name_consumed_bottle_is_null(self, agent_con):
        row = agent_con.execute("SELECT cellar_name FROM bottles WHERE bottle_id = 3").fetchone()
        assert row[0] is None

    def test_provider_name_populated_on_full_view(self, agent_con):
        row = agent_con.execute("SELECT provider_name FROM bottles_full WHERE bottle_id = 1").fetchone()
        assert row[0] == "Wine Shop A"


# ---------------------------------------------------------------------------
# TestSoftDeleteFiltering
# ---------------------------------------------------------------------------


class TestSoftDeleteFiltering:
    """Verify that is_deleted=True wines are excluded from all views."""

    def test_deleted_wine_absent_from_wines_view(self, agent_con):
        ids = {r[0] for r in agent_con.execute("SELECT wine_id FROM wines").fetchall()}
        assert 3 not in ids  # wine_id=3 is the deleted wine

    def test_deleted_wine_absent_from_wines_stored(self, agent_con):
        ids = {r[0] for r in agent_con.execute("SELECT wine_id FROM wines_stored").fetchall()}
        assert 3 not in ids

    def test_deleted_wine_absent_from_wines_drinking_now(self, agent_con):
        ids = {r[0] for r in agent_con.execute("SELECT wine_id FROM wines_drinking_now").fetchall()}
        assert 3 not in ids
