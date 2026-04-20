"""Tests for cellarbrain.flat — view SQL definitions and query.get_agent_connection."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from cellarbrain import writer
from cellarbrain.markdown import dossier_filename
from cellarbrain.query import get_agent_connection


# ---------------------------------------------------------------------------
# Shared helper: minimal Parquet dataset (mirrors test_query._make_dataset)
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime(2025, 1, 1)


def _make_dataset(tmp_path):
    """Write the 12 relational Parquet files needed by the views."""
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Château Test", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Bodega Ejemplo", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1, "country": "France", "region": "Bordeaux",
            "subregion": "Saint-Émilion", "classification": "Grand Cru",
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 2, "country": "Spain", "region": "Rioja",
            "subregion": None, "classification": "Reserva",
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Tempranillo", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1, "wine_slug": "chateau-test-cuvee-alpha-2020",
            "winery_id": 1, "name": "Cuvée Alpha",
            "vintage": 2020, "is_non_vintage": False, "appellation_id": 1,
            "category": "Red wine",
            "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 14.5,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2023, "drink_until": 2030,
            "optimal_from": 2025, "optimal_until": 2028,
            "original_list_price": None, "original_list_currency": None,
            "list_price": None, "list_currency": None,
            "comment": None, "winemaking_notes": None,
            "is_favorite": True, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Château Test Cuvée Alpha 2020",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": f"cellar/{dossier_filename(1, 'Château Test', 'Cuvée Alpha', 2020, False)}",
            "drinking_status": "optimal",
            "age_years": 5,
            "price_tier": "unknown",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 2, "wine_slug": "bodega-ejemplo-reserva-especial-2018",
            "winery_id": 2, "name": "Reserva Especial",
            "vintage": 2018, "is_non_vintage": False, "appellation_id": 2,
            "category": "Red wine",
            "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 13.5,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2022, "drink_until": 2028,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": None, "original_list_currency": None,
            "list_price": None, "list_currency": None,
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Bodega Ejemplo Reserva Especial 2018",
            "grape_type": "varietal",
            "primary_grape": "Tempranillo",
            "grape_summary": "Tempranillo",
            "_raw_grapes": None,
            "dossier_path": f"cellar/{dossier_filename(2, 'Bodega Ejemplo', 'Reserva Especial', 2018, False)}",
            "drinking_status": "drinkable",
            "age_years": 7,
            "price_tier": "unknown",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 3, "wine_slug": "chateau-test-deleted-cuvee-2015",
            "winery_id": 1, "name": "Deleted Cuvée",
            "vintage": 2015, "is_non_vintage": False, "appellation_id": None,
            "category": "Red wine",
            "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": None,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": None, "drink_until": None,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": None, "original_list_currency": None,
            "list_price": None, "list_currency": None,
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Château Test Deleted Cuvée 2015",
            "grape_type": "unknown",
            "primary_grape": None,
            "grape_summary": None,
            "_raw_grapes": None,
            "dossier_path": "archive/0003-chateau-test-deleted-cuvee-2015.md",
            "drinking_status": "unknown",
            "age_years": None,
            "price_tier": "unknown",
            "is_deleted": True,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 2, "grape_id": 2, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        {
            "bottle_id": 1, "wine_id": 1, "status": "stored",
            "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
            "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": None, "output_type": None, "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "bottle_id": 2, "wine_id": 1, "status": "stored",
            "cellar_id": 1, "shelf": "A2", "bottle_number": 2,
            "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": None, "output_type": None, "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "bottle_id": 3, "wine_id": 2, "status": "consumed",
            "cellar_id": None, "shelf": None, "bottle_number": None,
            "provider_id": 2, "purchase_date": datetime(2022, 1, 15).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("18.50"),
            "original_purchase_currency": "EUR",
            "purchase_price": Decimal("17.21"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": datetime(2024, 12, 25).date(),
            "output_type": "consumed", "output_comment": "Christmas dinner",
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    cellars = [
        {"cellar_id": 1, "name": "Main Cellar", "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    providers = [
        {"provider_id": 1, "name": "Wine Shop A", "etl_run_id": rid, "updated_at": now},
        {"provider_id": 2, "name": "Bodega Direct", "etl_run_id": rid, "updated_at": now},
    ]
    tastings = [
        {
            "tasting_id": 1, "wine_id": 1, "tasting_date": datetime(2024, 3, 15).date(),
            "note": "Great depth", "score": 92.0, "max_score": 100,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    pro_ratings = [
        {
            "rating_id": 1, "wine_id": 1, "source": "Parker",
            "score": 95.0, "max_score": 100, "review_text": "Outstanding",
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    etl_runs = [
        {
            "run_id": 1, "started_at": now, "finished_at": now,
            "run_type": "full", "wines_source_hash": "abc123",
            "bottles_source_hash": "def456", "bottles_gone_source_hash": None,
            "total_inserts": 10, "total_updates": 0, "total_deletes": 0,
            "wines_inserted": 2, "wines_updated": 0,
            "wines_deleted": 0, "wines_renamed": 0,
        },
    ]
    change_logs = [
        {
            "change_id": 1, "run_id": 1, "entity_type": "wine",
            "entity_id": 1, "change_type": "insert", "changed_fields": None,
        },
    ]

    for name, rows in [
        ("winery", wineries),
        ("appellation", appellations),
        ("grape", grapes),
        ("wine", wines),
        ("wine_grape", wine_grapes),
        ("bottle", bottles),
        ("cellar", cellars),
        ("provider", providers),
        ("tasting", tastings),
        ("pro_rating", pro_ratings),
        ("etl_run", etl_runs),
        ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


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
        rows = agent_con.execute(
            "SELECT table_name FROM information_schema.tables ORDER BY table_name"
        ).fetchall()
        names = sorted(r[0] for r in rows)
        assert names == [
            "_wines_wishlist",
            "bottles", "bottles_consumed", "bottles_full",
            "bottles_on_order", "bottles_stored",
            "wines", "wines_drinking_now", "wines_full",
            "wines_on_order", "wines_stored",
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
        cols = {
            row[0]
            for row in agent_con.execute("DESCRIBE SELECT * FROM wines").fetchall()
        }
        expected = {
            "wine_id", "wine_name", "vintage", "winery_name",
            "category", "country", "region", "subregion",
            "primary_grape", "blend_type",
            "drinking_status", "price_tier", "price",
            "style_tags",
            "bottles_stored", "bottles_on_order", "bottles_consumed",
            "is_favorite", "is_wishlist", "tracked_wine_id",
        }
        assert cols == expected

    def test_no_etl_columns(self, agent_con):
        cols = {
            row[0]
            for row in agent_con.execute("DESCRIBE SELECT * FROM wines").fetchall()
        }
        assert "etl_run_id" not in cols
        assert "updated_at" not in cols


# ---------------------------------------------------------------------------
# TestBottlesViewColumns
# ---------------------------------------------------------------------------


class TestBottlesViewColumns:
    def test_expected_columns_present(self, agent_con):
        cols = {
            row[0]
            for row in agent_con.execute("DESCRIBE SELECT * FROM bottles").fetchall()
        }
        expected = {
            "bottle_id", "wine_id", "wine_name", "vintage", "winery_name",
            "category", "country", "region", "primary_grape",
            "drinking_status", "price_tier", "price",
            "status", "cellar_name", "shelf",
            "output_date", "output_type",
        }
        assert cols == expected

    def test_no_fk_ids(self, agent_con):
        cols = {
            row[0]
            for row in agent_con.execute("DESCRIBE SELECT * FROM bottles").fetchall()
        }
        assert "winery_id" not in cols
        assert "appellation_id" not in cols
        assert "cellar_id" not in cols
        assert "provider_id" not in cols


# ---------------------------------------------------------------------------
# TestWinesViewAggregates
# ---------------------------------------------------------------------------


class TestWinesViewAggregates:
    def test_bottles_stored_wine1(self, agent_con):
        row = agent_con.execute(
            "SELECT bottles_stored FROM wines WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == 2

    def test_bottles_consumed_wine1(self, agent_con):
        row = agent_con.execute(
            "SELECT bottles_consumed FROM wines WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == 0

    def test_bottles_stored_wine2(self, agent_con):
        row = agent_con.execute(
            "SELECT bottles_stored FROM wines WHERE wine_id = 2"
        ).fetchone()
        assert row[0] == 0

    def test_bottles_consumed_wine2(self, agent_con):
        row = agent_con.execute(
            "SELECT bottles_consumed FROM wines WHERE wine_id = 2"
        ).fetchone()
        assert row[0] == 1

    def test_cellar_value_wine1(self, agent_con):
        row = agent_con.execute(
            "SELECT cellar_value FROM wines_full WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == pytest.approx(50.0)

    def test_tasting_count_wine1(self, agent_con):
        row = agent_con.execute(
            "SELECT tasting_count FROM wines_full WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == 1

    def test_last_tasting_score_wine1(self, agent_con):
        row = agent_con.execute(
            "SELECT last_tasting_score FROM wines_full WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == pytest.approx(92.0)

    def test_pro_rating_count_wine1(self, agent_con):
        row = agent_con.execute(
            "SELECT pro_rating_count FROM wines_full WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == 1

    def test_best_pro_score_wine1(self, agent_con):
        row = agent_con.execute(
            "SELECT best_pro_score FROM wines_full WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == pytest.approx(95.0)

    def test_no_tastings_wine2(self, agent_con):
        row = agent_con.execute(
            "SELECT tasting_count, last_tasting_score FROM wines_full WHERE wine_id = 2"
        ).fetchone()
        assert row[0] == 0
        assert row[1] is None

    def test_no_ratings_wine2(self, agent_con):
        row = agent_con.execute(
            "SELECT pro_rating_count, best_pro_score FROM wines_full WHERE wine_id = 2"
        ).fetchone()
        assert row[0] == 0
        assert row[1] is None


# ---------------------------------------------------------------------------
# TestBottlesViewDenormalization
# ---------------------------------------------------------------------------


class TestBottlesViewDenormalization:
    def test_winery_name_populated(self, agent_con):
        row = agent_con.execute(
            "SELECT DISTINCT winery_name FROM bottles WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == "Château Test"

    def test_country_populated(self, agent_con):
        row = agent_con.execute(
            "SELECT DISTINCT country FROM bottles WHERE wine_id = 1"
        ).fetchone()
        assert row[0] == "France"

    def test_cellar_name_stored_bottle(self, agent_con):
        row = agent_con.execute(
            "SELECT cellar_name FROM bottles WHERE bottle_id = 1"
        ).fetchone()
        assert row[0] == "Main Cellar"

    def test_cellar_name_consumed_bottle_is_null(self, agent_con):
        row = agent_con.execute(
            "SELECT cellar_name FROM bottles WHERE bottle_id = 3"
        ).fetchone()
        assert row[0] is None

    def test_provider_name_populated_on_full_view(self, agent_con):
        row = agent_con.execute(
            "SELECT provider_name FROM bottles_full WHERE bottle_id = 1"
        ).fetchone()
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
        ids = {
            r[0] for r in
            agent_con.execute("SELECT wine_id FROM wines_stored").fetchall()
        }
        assert 3 not in ids

    def test_deleted_wine_absent_from_wines_drinking_now(self, agent_con):
        ids = {
            r[0] for r in
            agent_con.execute("SELECT wine_id FROM wines_drinking_now").fetchall()
        }
        assert 3 not in ids
