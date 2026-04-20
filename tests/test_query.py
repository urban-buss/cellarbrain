"""Tests for cellarbrain.query — DuckDB query layer over Parquet files."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import duckdb
import pytest

from cellarbrain import writer
from cellarbrain.markdown import dossier_filename
from cellarbrain.query import (
    DataStaleError,
    IntentResult,
    QueryError,
    _CONCEPT_EXPANSIONS,
    _SEARCH_COLS,
    _SYSTEM_CONCEPTS,
    _extract_intents,
    _fmt_chf,
    _fmt_litres,
    _normalise_query_tokens,
    cellar_churn,
    cellar_stats,
    execute_query,
    find_wine,
    get_agent_connection,
    get_connection,
    validate_sql,
)


# ---------------------------------------------------------------------------
# Shared fixture: minimal Parquet dataset
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime(2025, 1, 1)


def _make_dataset(tmp_path):
    """Write a minimal but complete set of Parquet entity files."""
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Château Test", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Bodega Ejemplo", "etl_run_id": rid, "updated_at": now},
        # Winery with curly RIGHT SINGLE QUOTATION MARK (U+2019) — tests quote normalisation
        {"winery_id": 3, "name": "Ch\u00e2teau d\u2019Aiguilhe", "etl_run_id": rid, "updated_at": now},
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
            "wine_id": 3, "wine_slug": "chateau-daiguilhe-2019",
            "winery_id": 3, "name": None,
            "vintage": 2019, "is_non_vintage": False, "appellation_id": 1,
            "category": "Red wine",
            "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 14.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2024, "drink_until": 2032,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": None, "original_list_currency": None,
            "list_price": None, "list_currency": None,
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            # winery_name contains curly quote (U+2019) — tests quote normalisation
            "full_name": "Ch\u00e2teau d\u2019Aiguilhe 2019",
            "grape_type": "varietal",
            "primary_grape": "Merlot",
            "grape_summary": "Merlot",
            "_raw_grapes": None,
            "dossier_path": f"cellar/{dossier_filename(3, 'Ch\u00e2teau d\u2019Aiguilhe', None, 2019, False)}",
            "drinking_status": "optimal",
            "age_years": 6,
            "price_tier": "unknown",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 2, "grape_id": 2, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 3, "grape_id": 1, "percentage": 100.0, "sort_order": 1,
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
        {
            "bottle_id": 4, "wine_id": 2, "status": "stored",
            "cellar_id": 1, "shelf": "B1", "bottle_number": 1,
            "provider_id": 2, "purchase_date": datetime(2025, 3, 10).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("20.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("20.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": None, "output_type": None, "output_comment": None,
            "is_onsite": False,
            "is_in_transit": True,
            "etl_run_id": rid, "updated_at": now,
        },
        # Bottles for churn testing (span 2024–2025)
        {
            "bottle_id": 5, "wine_id": 1, "status": "drunk",
            "cellar_id": None, "shelf": None, "bottle_number": None,
            "provider_id": 1, "purchase_date": datetime(2024, 6, 15).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": datetime(2024, 9, 20).date(),
            "output_type": "drunk", "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "bottle_id": 6, "wine_id": 2, "status": "stored",
            "cellar_id": 1, "shelf": "C1", "bottle_number": 1,
            "provider_id": 2, "purchase_date": datetime(2024, 11, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("18.50"),
            "original_purchase_currency": "EUR",
            "purchase_price": Decimal("17.21"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": None, "output_type": None, "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "bottle_id": 7, "wine_id": 1, "status": "offered",
            "cellar_id": None, "shelf": None, "bottle_number": None,
            "provider_id": 1, "purchase_date": datetime(2025, 2, 10).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("25.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("25.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": datetime(2025, 3, 15).date(),
            "output_type": "offered", "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "bottle_id": 8, "wine_id": 3, "status": "stored",
            "cellar_id": 1, "shelf": "C1", "bottle_number": 1,
            "provider_id": 1, "purchase_date": datetime(2023, 9, 1).date(),
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


# ---------------------------------------------------------------------------
# TestGetConnection
# ---------------------------------------------------------------------------


class TestGetConnection:
    def test_creates_views(self, data_dir):
        con = get_connection(data_dir)
        result = con.execute("SELECT count(*) FROM wines").fetchone()
        assert result[0] == 3

    def test_missing_parquet_raises_data_stale_error(self, tmp_path):
        with pytest.raises(DataStaleError, match="Missing Parquet files"):
            get_connection(tmp_path)

    def test_internal_connection_has_etl_views(self, data_dir):
        con = get_connection(data_dir)
        result = con.execute("SELECT count(*) FROM etl_run").fetchone()
        assert result[0] >= 1


# ---------------------------------------------------------------------------
# TestAgentConnection
# ---------------------------------------------------------------------------


class TestAgentConnection:
    def test_has_expected_views(self, data_dir):
        con = get_agent_connection(data_dir)
        rows = con.execute(
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

    def test_relational_tables_not_accessible(self, data_dir):
        con = get_agent_connection(data_dir)
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT count(*) FROM wine")

    def test_etl_run_not_accessible(self, data_dir):
        con = get_agent_connection(data_dir)
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT count(*) FROM etl_run")


# ---------------------------------------------------------------------------
# TestViews
# ---------------------------------------------------------------------------


class TestViews:
    def test_wines_queryable(self, data_dir):
        con = get_connection(data_dir)
        count = con.execute("SELECT count(*) FROM wines").fetchone()[0]
        assert count == 3

    def test_bottles_queryable(self, data_dir):
        con = get_connection(data_dir)
        count = con.execute("SELECT count(*) FROM bottles").fetchone()[0]
        assert count == 8

    def test_wines_stored_excludes_zero_bottles(self, data_dir):
        con = get_connection(data_dir)
        rows = con.execute("SELECT wine_id FROM wines_stored ORDER BY wine_id").fetchall()
        wine_ids = [r[0] for r in rows]
        # Wine 1 has 2 stored bottles; wine 2 has 1 stored (bottle 6); wine 3 has 1 stored (bottle 8)
        assert wine_ids == [1, 2, 3]

    def test_bottles_stored_excludes_consumed(self, data_dir):
        con = get_connection(data_dir)
        count = con.execute("SELECT count(*) FROM bottles_stored").fetchone()[0]
        assert count == 4  # bottles 1, 2 (wine 1) + bottle 6 (wine 2) + bottle 8 (wine 3)

    def test_bottles_consumed_excludes_stored(self, data_dir):
        con = get_connection(data_dir)
        count = con.execute("SELECT count(*) FROM bottles_consumed").fetchone()[0]
        assert count == 3  # bottles 3 (consumed), 5 (drunk), 7 (offered)

    def test_bottles_stored_has_slim_columns(self, data_dir):
        con = get_agent_connection(data_dir)
        cols = [
            r[0] for r in con.execute("DESCRIBE SELECT * FROM bottles_stored").fetchall()
        ]
        assert len(cols) == 17
        assert cols == [
            "bottle_id", "wine_id", "wine_name", "vintage", "winery_name",
            "category", "country", "region", "primary_grape",
            "drinking_status", "price_tier", "price",
            "status", "cellar_name", "shelf",
            "output_date", "output_type",
        ]

    def test_bottles_consumed_has_slim_columns(self, data_dir):
        con = get_agent_connection(data_dir)
        cols = [
            r[0] for r in con.execute("DESCRIBE SELECT * FROM bottles_consumed").fetchall()
        ]
        assert len(cols) == 17

    def test_bottles_on_order_has_slim_columns(self, data_dir):
        con = get_agent_connection(data_dir)
        cols = [
            r[0] for r in con.execute("DESCRIBE SELECT * FROM bottles_on_order").fetchall()
        ]
        assert len(cols) == 17

    def test_internal_full_bottle_views_on_connection(self, data_dir):
        """get_connection() adds internal full-column bottle convenience views."""
        con = get_connection(data_dir)
        # Internal views have all 37 columns from bottles_full
        stored_cols = [
            r[0] for r in con.execute("DESCRIBE SELECT * FROM _bottles_stored_full").fetchall()
        ]
        assert len(stored_cols) == 37
        assert "volume_ml" in stored_cols
        assert "provider_name" in stored_cols
        assert "is_in_transit" in stored_cols

    def test_internal_full_views_not_on_agent_connection(self, data_dir):
        """Agent connection must NOT expose internal full-column views."""
        con = get_agent_connection(data_dir)
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT count(*) FROM _bottles_stored_full")

    def test_wines_drinking_now_filter(self, data_dir):
        con = get_connection(data_dir)
        rows = con.execute("SELECT wine_id FROM wines_drinking_now").fetchall()
        wine_ids = sorted(r[0] for r in rows)
        # Wine 1: optimal + 2 stored bottles → included
        # Wine 2: drinkable + 1 stored bottle (bottle 6) → included
        # Wine 3: optimal + 1 stored bottle (bottle 8) → included
        assert wine_ids == [1, 2, 3]


# ---------------------------------------------------------------------------
# TestValidateSql
# ---------------------------------------------------------------------------


class TestValidateSql:
    def test_select_accepted(self):
        validate_sql("SELECT * FROM wine")

    def test_with_cte_accepted(self):
        validate_sql("WITH x AS (SELECT 1) SELECT * FROM x")

    def test_whitespace_trimmed(self):
        validate_sql("  SELECT 1  ")

    @pytest.mark.parametrize("stmt", [
        "INSERT INTO wine VALUES (1)",
        "UPDATE wine SET name='x'",
        "DELETE FROM wine",
        "DROP TABLE wine",
        "CREATE TABLE evil (id INT)",
        "ALTER TABLE wine ADD col INT",
        "TRUNCATE TABLE wine",
    ])
    def test_ddl_dml_rejected(self, stmt):
        with pytest.raises(QueryError, match="Only SELECT queries"):
            validate_sql(stmt)

    def test_empty_sql_rejected(self):
        with pytest.raises(QueryError, match="Empty SQL"):
            validate_sql("")

    def test_whitespace_only_rejected(self):
        with pytest.raises(QueryError, match="Empty SQL"):
            validate_sql("   ")

    def test_nonsense_rejected(self):
        with pytest.raises(QueryError, match="SQL must start with SELECT"):
            validate_sql("EXPLAIN SELECT 1")


# ---------------------------------------------------------------------------
# TestExecuteQuery
# ---------------------------------------------------------------------------


class TestExecuteQuery:
    def test_returns_markdown_table(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(con, "SELECT wine_id, wine_name FROM wines ORDER BY wine_id")
        assert "Cuvée Alpha" in result
        assert "Reserva Especial" in result
        assert "|" in result  # Markdown table pipes

    def test_no_results(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(con, "SELECT * FROM wines WHERE wine_id = 999")
        assert result == "*No results.*"

    def test_truncation_with_row_limit(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(con, "SELECT * FROM wines", row_limit=1)
        assert "3 rows total, showing first 1" in result

    def test_invalid_sql_raises_query_error(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError):
            execute_query(con, "SELECT * FROM nonexistent_table")

    def test_dml_rejected(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError):
            execute_query(con, "DROP TABLE wines")

    def test_unaccent_macro_available(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(
            con, "SELECT unaccent('Château') AS stripped"
        )
        assert "Chateau" in result

    def test_unaccent_in_where_clause(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(
            con,
            "SELECT wine_name FROM wines WHERE unaccent(winery_name) ILIKE '%chateau%'",
        )
        assert "Cuvée Alpha" in result

    def test_null_renders_as_empty(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(
            con, "SELECT NULL AS col_a, CAST(NULL AS DOUBLE) AS col_b"
        )
        assert "nan" not in result.lower()
        assert "<NA>" not in result

    def test_normalize_quotes_macro_available(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(
            con, "SELECT normalize_quotes('d\u2019Aiguilhe') AS normalized"
        )
        assert "d'Aiguilhe" in result

    def test_normalize_quotes_in_where_clause(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(
            con,
            "SELECT winery_name FROM wines WHERE normalize_quotes(winery_name) ILIKE '%d''Aiguilhe%'",
        )
        assert "Aiguilhe" in result

    def test_column_not_found_suggests_subregion(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError, match="subregion"):
            execute_query(con, "SELECT appellation FROM wines")

    def test_column_not_found_suggests_primary_grape(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError, match="primary_grape"):
            execute_query(con, "SELECT grape_variety FROM wines")

    def test_column_not_found_suggests_price(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError, match="price"):
            execute_query(con, "SELECT price_chf FROM wines")

    def test_column_not_found_unrelated_no_crash(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError):
            execute_query(con, "SELECT zzz_nonexistent FROM wines")


# ---------------------------------------------------------------------------
# TestFmtChf / TestFmtLitres
# ---------------------------------------------------------------------------


class TestFmtChf:
    def test_thousands_separator(self):
        assert _fmt_chf(14200.0) == "14'200.00"

    def test_no_separator_small(self):
        assert _fmt_chf(830.5) == "830.50"

    def test_zero(self):
        assert _fmt_chf(0.0) == "0.00"


class TestFmtLitres:
    def test_no_separator(self):
        assert _fmt_litres(256.5) == "256.50"

    def test_thousands_separator(self):
        assert _fmt_litres(1250.0) == "1'250.00"


# ---------------------------------------------------------------------------
# TestCellarStats
# ---------------------------------------------------------------------------


class TestCellarStats:
    def test_summary_table_structure(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con)
        assert "## Cellar Summary" in result
        assert "| wines" in result
        assert "| bottles" in result
        assert "| value (" in result
        assert "| volume (L)" in result
        assert "in cellar" in result
        assert "on order" in result
        assert "total" in result

    def test_in_cellar_section(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con)
        assert "### In Cellar" in result
        assert "#### By Category" in result
        assert "#### Drinking Window Status" in result

    def test_summary_table_values(self, data_dir):
        """Wine 1 has 2 stored bottles at 25 CHF each (750 ml).
        Wine 2 has 1 stored bottle at 17.21 CHF + 1 on-order at 20 CHF.
        Wine 3 has 1 stored bottle at 30 CHF (750 ml).
        """
        con = get_connection(data_dir)
        result = cellar_stats(con)
        # bottles_cellar=4, value_cellar=97.21, volume_cellar=3.0
        assert "97.21" in result
        assert "3.00" in result
        # value_total = cellar + on_order = 97.21 + 20.00 = 117.21
        assert "117.21" in result

    def test_on_order_in_summary(self, data_dir):
        """Wine 2 has 1 on-order bottle at 20 CHF."""
        con = get_connection(data_dir)
        result = cellar_stats(con)
        assert "20.00" in result

    def test_value_total_without_on_order(self, tmp_path):
        """When no wines have on-order bottles, value_total must equal value_cellar."""
        now = _now()
        rid = 1
        wineries = [{"winery_id": 1, "name": "Solo Winery", "etl_run_id": rid, "updated_at": now}]
        appellations = [{
            "appellation_id": 1, "country": "France", "region": "Bordeaux",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        }]
        grapes = [{"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now}]
        wines = [{
            "wine_id": 1, "wine_slug": "solo-winery-solo-2020",
            "winery_id": 1, "name": "Solo",
            "vintage": 2020, "is_non_vintage": False, "appellation_id": 1,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 13.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2023, "drink_until": 2030,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": None, "original_list_currency": None,
            "list_price": None, "list_currency": None,
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Solo Winery Solo 2020",
            "grape_type": "varietal", "primary_grape": "Merlot",
            "grape_summary": "Merlot", "_raw_grapes": None,
            "dossier_path": "cellar/0001-solo-winery-solo-2020.md",
            "drinking_status": "drinkable", "age_years": 5,
            "price_tier": "unknown", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        }]
        wine_grapes = [{"wine_id": 1, "grape_id": 1, "percentage": 100.0,
                        "sort_order": 1, "etl_run_id": rid, "updated_at": now}]
        bottles = [{
            "bottle_id": 1, "wine_id": 1, "status": "stored",
            "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
            "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("30.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("30.00"),
            "purchase_currency": "CHF", "purchase_comment": None,
            "output_date": None, "output_type": None, "output_comment": None,
            "is_onsite": True, "is_in_transit": False,
            "etl_run_id": rid, "updated_at": now,
        }]
        cellars = [{"cellar_id": 1, "name": "Cellar", "sort_order": 1,
                    "etl_run_id": rid, "updated_at": now}]
        providers = [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]
        etl_runs = [{
            "run_id": 1, "started_at": now, "finished_at": now,
            "run_type": "full", "wines_source_hash": "abc",
            "bottles_source_hash": "def", "bottles_gone_source_hash": None,
            "total_inserts": 1, "total_updates": 0, "total_deletes": 0,
            "wines_inserted": 1, "wines_updated": 0,
            "wines_deleted": 0, "wines_renamed": 0,
        }]
        change_logs = [{"change_id": 1, "run_id": 1, "entity_type": "wine",
                        "entity_id": 1, "change_type": "insert", "changed_fields": None}]
        for name, rows in [
            ("winery", wineries), ("appellation", appellations),
            ("grape", grapes), ("wine", wines), ("wine_grape", wine_grapes),
            ("bottle", bottles), ("cellar", cellars), ("provider", providers),
            ("tasting", []), ("pro_rating", []),
            ("etl_run", etl_runs), ("change_log", change_logs),
        ]:
            writer.write_parquet(name, rows, tmp_path)

        con = get_connection(tmp_path)
        result = cellar_stats(con)
        # value_cellar = 30.00, on_order = 0, so total must also be 30.00
        lines = result.split("\n")
        value_line = [l for l in lines if l.startswith("| value")][0]
        cells = [c.strip() for c in value_line.split("|") if c.strip()]
        # cells: ["value (CHF)", in_cellar, on_order, total]
        assert cells[1] == "30.00"   # in cellar
        assert cells[2] == "0.00"    # on order
        assert cells[3] == "30.00"   # total (was 0.00 before fix)

    def test_wines_total_accounts_for_overlap(self, data_dir):
        """Wine 2 has both consumed + on-order bottles.
        wines_total counts unique wines with stored OR on-order.
        Wine 1: stored. Wine 2: stored + on-order. Wine 3: stored. Total = 3.
        """
        con = get_connection(data_dir)
        result = cellar_stats(con)
        # The total column for wines should be 3
        lines = result.split("\n")
        wines_line = [l for l in lines if l.startswith("| wines")][0]
        cells = [c.strip() for c in wines_line.split("|") if c.strip()]
        assert cells[-1] == "3"  # total wines

    def test_by_category_has_value_and_volume(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con)
        # Find the By Category table — should have value and volume columns
        cat_idx = result.index("#### By Category")
        cat_section = result[cat_idx:cat_idx + 500]
        assert "value (" in cat_section
        assert "volume (L)" in cat_section

    def test_drinking_window_has_all_columns(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con)
        dw_idx = result.index("#### Drinking Window Status")
        dw_section = result[dw_idx:dw_idx + 500]
        assert "value (" in dw_section
        assert "volume (L)" in dw_section

    def test_data_freshness_no_changes_line(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con)
        assert "### Data Freshness" in result
        assert "run #1" in result
        assert "Changes:" not in result

    def test_removed_sections(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con)
        assert "Cellar Value" not in result
        assert "Tracked Wines" not in result
        assert "consumed/gone" not in result
        assert "Breakdown by original" not in result

    @pytest.mark.parametrize("dimension", [
        "country", "region", "category", "vintage", "winery",
        "grape", "cellar", "provider", "status", "on_order",
    ])
    def test_grouped_stats_valid_dimensions(self, data_dir, dimension):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by=dimension)
        assert f"by {dimension.title()}" in result

    @pytest.mark.parametrize("dimension", [
        "country", "region", "category", "vintage", "winery",
        "grape", "cellar", "provider", "status", "on_order",
    ])
    def test_grouped_stats_uniform_columns(self, data_dir, dimension):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by=dimension)
        assert "wines" in result
        assert "bottles" in result
        assert "bottles_%" in result
        assert "value (" in result
        assert "volume (L)" in result

    def test_grouped_stats_percentage_sums(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="category")
        lines = result.split("\n")
        # Find header row to locate bottles_% column index
        header = [l for l in lines if "bottles_%" in l][0]
        cols = [c.strip() for c in header.split("|") if c.strip()]
        pct_idx = cols.index("bottles_%")
        # Sum percentage values from data rows (skip header + separator)
        total = 0.0
        for line in lines:
            if "|" not in line or "---" in line or "bottles_%" in line:
                continue
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if len(cells) > pct_idx:
                try:
                    total += float(cells[pct_idx])
                except ValueError:
                    pass
        assert abs(total - 100.0) < 0.2

    def test_invalid_group_by_raises(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(ValueError, match="Invalid group_by"):
            cellar_stats(con, group_by="color")

    def test_case_insensitive_group_by(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="Category")
        assert "by Category" in result

    def test_whitespace_stripped_group_by(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by=" country ")
        assert "by Country" in result

    def test_sort_by_value(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="country", sort_by="value")
        lines = [l for l in result.split("\n") if "|" in l and "---" not in l and "country" not in l]
        # First data row should have highest value
        assert len(lines) >= 1

    def test_sort_by_wines(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="country", sort_by="wines")
        assert "France" in result

    def test_sort_by_invalid_raises(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(ValueError, match="Invalid sort_by"):
            cellar_stats(con, group_by="country", sort_by="invalid")

    def test_vintage_default_sort(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="vintage")
        lines = [l for l in result.split("\n")
                 if "|" in l and "---" not in l and "vintage" not in l]
        # Parse vintage values from first column of data rows
        vintages = []
        for line in lines:
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if cells:
                try:
                    vintages.append(int(cells[0]))
                except ValueError:
                    pass
        # Should be sorted descending
        assert vintages == sorted(vintages, reverse=True)

    def test_vintage_sort_by_value(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="vintage", sort_by="value")
        # Should not raise and should contain vintage heading
        assert "by Vintage" in result

    def test_limit_with_rollup(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="country", limit=1)
        lines = [l for l in result.split("\n") if "|" in l and "---" not in l and "country" not in l]
        # 1 data row + 1 (other) row = 2
        assert len(lines) == 2
        assert "(other)" in result
        assert "Showing top 1 of" in result

    def test_limit_zero_unlimited(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="country", limit=0)
        assert "(other)" not in result
        assert "Showing top" not in result

    def test_limit_exceeds_rows(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="category", limit=100)
        assert "(other)" not in result
        assert "Showing top" not in result

    def test_limit_negative_raises(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(ValueError, match="limit must be >= 0"):
            cellar_stats(con, group_by="country", limit=-1)

    def test_grouped_in_cellar_heading(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="country")
        assert "## In Cellar Statistics — by Country" in result

    def test_grouped_status_heading(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="status")
        assert "## Cellar Statistics — by Status" in result
        assert "In Cellar" not in result

    def test_grouped_on_order_heading(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="on_order")
        assert "## On Order Statistics" in result

    def test_country_stats_contain_france(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by="country")
        assert "France" in result

    @pytest.mark.parametrize("dimension,expected_col", [
        ("country", "| country"),
        ("category", "| category"),
        ("grape", "| grape"),
        ("cellar", "| cellar"),
        ("status", "| status"),
    ])
    def test_grouped_column_name_matches_dimension(self, data_dir, dimension, expected_col):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by=dimension)
        assert expected_col in result
        assert "group_val" not in result

    def test_null_group_val_labelled(self, tmp_path):
        """Wines with NULL country appear as '(not set)' in grouped output."""
        now = _now()
        rid = 1
        for name, rows in [
            ("winery", [{"winery_id": 1, "name": "W", "etl_run_id": rid, "updated_at": now}]),
            ("appellation", []),
            ("grape", [{"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now}]),
            ("wine", [{
                "wine_id": 1, "wine_slug": "w-x-2020", "winery_id": 1, "name": "X",
                "vintage": 2020, "is_non_vintage": False, "appellation_id": None,
                "category": "Red wine", "_raw_classification": None,
                "subcategory": None, "specialty": None,
                "sweetness": None, "effervescence": None, "volume_ml": 750,
                "_raw_volume": None,
                "container": None, "hue": None, "cork": None, "alcohol_pct": 13.0,
                "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
                "ageing_months": None, "farming_type": None, "serving_temp_c": None,
                "opening_type": None, "opening_minutes": None,
                "drink_from": 2023, "drink_until": 2030,
                "optimal_from": None, "optimal_until": None,
                "original_list_price": None, "original_list_currency": None,
                "list_price": None, "list_currency": None,
                "comment": None, "winemaking_notes": None,
                "is_favorite": False, "is_wishlist": False,
                "tracked_wine_id": None,
                "full_name": "W X 2020",
                "grape_type": "varietal", "primary_grape": "Merlot",
                "grape_summary": "Merlot", "_raw_grapes": None,
                "dossier_path": "cellar/0001-w-x-2020.md",
                "drinking_status": "drinkable", "age_years": 5,
                "price_tier": "unknown", "is_deleted": False,
                "etl_run_id": rid, "updated_at": now,
            }]),
            ("wine_grape", [{"wine_id": 1, "grape_id": 1, "percentage": 100.0,
                             "sort_order": 1, "etl_run_id": rid, "updated_at": now}]),
            ("bottle", [{
                "bottle_id": 1, "wine_id": 1, "status": "stored",
                "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
                "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
                "acquisition_type": "purchase",
                "original_purchase_price": Decimal("20.00"),
                "original_purchase_currency": "CHF",
                "purchase_price": Decimal("20.00"),
                "purchase_currency": "CHF", "purchase_comment": None,
                "output_date": None, "output_type": None, "output_comment": None,
                "is_onsite": True, "is_in_transit": False,
                "etl_run_id": rid, "updated_at": now,
            }]),
            ("cellar", [{"cellar_id": 1, "name": "Cellar", "sort_order": 1,
                         "etl_run_id": rid, "updated_at": now}]),
            ("provider", [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]),
            ("tasting", []), ("pro_rating", []),
            ("etl_run", [{
                "run_id": 1, "started_at": now, "finished_at": now,
                "run_type": "full", "wines_source_hash": "a",
                "bottles_source_hash": "b", "bottles_gone_source_hash": None,
                "total_inserts": 1, "total_updates": 0, "total_deletes": 0,
                "wines_inserted": 1, "wines_updated": 0,
                "wines_deleted": 0, "wines_renamed": 0,
            }]),
            ("change_log", [{"change_id": 1, "run_id": 1, "entity_type": "wine",
                             "entity_id": 1, "change_type": "insert", "changed_fields": None}]),
        ]:
            writer.write_parquet(name, rows, tmp_path)

        con = get_connection(tmp_path)
        result = cellar_stats(con, group_by="country")
        assert "(not set)" in result

    def test_grape_includes_null_as_not_set(self, tmp_path):
        """Wines with NULL primary_grape show as '(not set)' in grape breakdown."""
        now = _now()
        rid = 1
        for name, rows in [
            ("winery", [{"winery_id": 1, "name": "W", "etl_run_id": rid, "updated_at": now}]),
            ("appellation", [{
                "appellation_id": 1, "country": "France", "region": "Bordeaux",
                "subregion": None, "classification": None,
                "etl_run_id": rid, "updated_at": now,
            }]),
            ("grape", []),
            ("wine", [{
                "wine_id": 1, "wine_slug": "w-x-2020", "winery_id": 1, "name": "X",
                "vintage": 2020, "is_non_vintage": False, "appellation_id": 1,
                "category": "Red wine", "_raw_classification": None,
                "subcategory": None, "specialty": None,
                "sweetness": None, "effervescence": None, "volume_ml": 750,
                "_raw_volume": None,
                "container": None, "hue": None, "cork": None, "alcohol_pct": 13.0,
                "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
                "ageing_months": None, "farming_type": None, "serving_temp_c": None,
                "opening_type": None, "opening_minutes": None,
                "drink_from": 2023, "drink_until": 2030,
                "optimal_from": None, "optimal_until": None,
                "original_list_price": None, "original_list_currency": None,
                "list_price": None, "list_currency": None,
                "comment": None, "winemaking_notes": None,
                "is_favorite": False, "is_wishlist": False,
                "tracked_wine_id": None,
                "full_name": "W X 2020",
                "grape_type": "unknown", "primary_grape": None,
                "grape_summary": None, "_raw_grapes": None,
                "dossier_path": "cellar/0001-w-x-2020.md",
                "drinking_status": "drinkable", "age_years": 5,
                "price_tier": "unknown", "is_deleted": False,
                "etl_run_id": rid, "updated_at": now,
            }]),
            ("wine_grape", []),
            ("bottle", [{
                "bottle_id": 1, "wine_id": 1, "status": "stored",
                "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
                "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
                "acquisition_type": "purchase",
                "original_purchase_price": Decimal("20.00"),
                "original_purchase_currency": "CHF",
                "purchase_price": Decimal("20.00"),
                "purchase_currency": "CHF", "purchase_comment": None,
                "output_date": None, "output_type": None, "output_comment": None,
                "is_onsite": True, "is_in_transit": False,
                "etl_run_id": rid, "updated_at": now,
            }]),
            ("cellar", [{"cellar_id": 1, "name": "Cellar", "sort_order": 1,
                         "etl_run_id": rid, "updated_at": now}]),
            ("provider", [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]),
            ("tasting", []), ("pro_rating", []),
            ("etl_run", [{
                "run_id": 1, "started_at": now, "finished_at": now,
                "run_type": "full", "wines_source_hash": "a",
                "bottles_source_hash": "b", "bottles_gone_source_hash": None,
                "total_inserts": 1, "total_updates": 0, "total_deletes": 0,
                "wines_inserted": 1, "wines_updated": 0,
                "wines_deleted": 0, "wines_renamed": 0,
            }]),
            ("change_log", [{"change_id": 1, "run_id": 1, "entity_type": "wine",
                             "entity_id": 1, "change_type": "insert", "changed_fields": None}]),
        ]:
            writer.write_parquet(name, rows, tmp_path)

        con = get_connection(tmp_path)
        result = cellar_stats(con, group_by="grape")
        assert "(not set)" in result


# ---------------------------------------------------------------------------
# TestCellarChurn
# ---------------------------------------------------------------------------


class TestCellarChurn:
    """Test cellar_churn roll-forward analysis.

    Fixture bottles timeline:
      id=1: purchased 2023-06-01, stored (in cellar)
      id=2: purchased 2023-06-01, stored (in cellar)
      id=3: purchased 2022-01-15, consumed 2024-12-25
      id=4: purchased 2025-03-10, stored (in transit)
      id=5: purchased 2024-06-15, consumed 2024-09-20
      id=6: purchased 2024-11-01, stored (in cellar)
      id=7: purchased 2025-02-10, consumed 2025-03-15
    """

    def test_single_period_structure(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, year=2024, month=6)
        assert "## Cellar Churn" in result
        assert "June 2024" in result
        assert "Beginning" in result
        assert "+ Purchased" in result
        assert "\u2212 Consumed" in result
        assert "**Ending**" in result
        assert "### Ending Inventory" in result

    def test_rollforward_identity(self, data_dir):
        """beginning + purchased - consumed = ending (bottles)."""
        con = get_connection(data_dir)
        # Sep 2024: bottle 5 is consumed (output_date=2024-09-20)
        result = cellar_churn(con, year=2024, month=9)
        # Parse bottles from the table rows
        lines = [l for l in result.split("\n") if "|" in l and "---" not in l]
        # Find data rows (skip header)
        data_rows = [l for l in lines if "Beginning" in l or "Purchased" in l
                     or "Consumed" in l or "Ending" in l]
        assert len(data_rows) == 4
        # Extract bottles column (index 2, after label and wines)
        def _bottles(row):
            cells = [c.strip().replace("**", "") for c in row.split("|") if c.strip()]
            return int(cells[2])
        beg = _bottles(data_rows[0])
        pur = _bottles(data_rows[1])
        con_b = _bottles(data_rows[2])
        end = _bottles(data_rows[3])
        assert beg + pur - con_b == end

    def test_ending_inventory_split(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, year=2025, month=4)
        assert "In Cellar" in result
        assert "In Transit" in result

    def test_month_by_month_structure(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, period="month", year=2024)
        assert "## Cellar Churn" in result
        assert "Month-by-Month" in result
        assert "beg. bottles" in result
        assert "net" in result

    def test_month_by_month_has_value_table(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, period="month", year=2024)
        assert "### Value" in result

    def test_year_by_year(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, period="year")
        assert "Year-by-Year" in result
        # Should include years from first purchase (2022) onward
        assert "2022" in result
        assert "2023" in result
        assert "2024" in result

    def test_default_current_month(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con)
        # Should contain current month name
        import datetime as _dt
        today = _dt.date.today()
        from cellarbrain.query import _MONTH_NAMES
        assert _MONTH_NAMES[today.month] in result

    def test_single_year(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, year=2024)
        assert "2024" in result
        assert "Beginning" in result

    def test_net_column_signed(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, period="year")
        # Net column should have + or \u2212 prefix
        assert "+" in result

    def test_swiss_formatting(self, data_dir):
        con = get_connection(data_dir)
        # Use a period where values won't have apostrophes (small amounts),
        # but at least verify the value column header is present
        result = cellar_churn(con, year=2024, month=6)
        assert "value (" in result

    def test_invalid_period_raises(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(ValueError, match="Invalid period"):
            cellar_churn(con, period="week")

    def test_period_alias_monthly(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, period="monthly", year=2024)
        assert "Month-by-Month" in result

    def test_period_alias_yearly(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, period="yearly")
        assert "Year-by-Year" in result

    def test_invalid_month_raises(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(ValueError, match="Invalid month"):
            cellar_churn(con, month=13)

    def test_bold_ending_row(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_churn(con, year=2024, month=6)
        assert "**Ending**" in result


# ---------------------------------------------------------------------------
# TestFindWine
# ---------------------------------------------------------------------------


class TestFindWine:
    def test_find_by_wine_name(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Alpha")
        assert "Cuvée Alpha" in result

    def test_find_by_winery_name(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Château")
        assert "Château Test" in result

    def test_find_by_country(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Spain")
        assert "Reserva Especial" in result

    def test_find_by_region(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Bordeaux")
        assert "Cuvée Alpha" in result

    def test_find_by_grape(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Tempranillo")
        assert "Reserva Especial" in result

    def test_find_by_vintage(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "2020")
        assert "Cuvée Alpha" in result

    def test_case_insensitive(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "alpha")
        assert "Cuvée Alpha" in result

    def test_no_match(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Zyxnonexist Qxznowhere Jxzmissing")
        assert "No wines found" in result

    def test_limit_respected(self, data_dir):
        con = get_connection(data_dir)
        # Search for "France" which matches via country — should limit to 1 row.
        result = find_wine(con, "France", limit=1)
        assert "|" in result
        # Only 1 data row (plus header and separator)
        data_rows = [l for l in result.strip().split("\n")
                     if l.strip().startswith("|") and "---" not in l][1:]  # skip header
        assert len(data_rows) == 1

    def test_injection_safe(self, data_dir):
        con = get_connection(data_dir)
        # SQL injection attempt via query parameter — should be safe
        result = find_wine(con, "'; DROP TABLE wine; --")
        assert "No wines found" in result

    def test_multi_token_region_and_vintage(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Bordeaux 2020")
        assert "Cuvée Alpha" in result

    def test_multi_token_country_and_grape(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Spain Tempranillo")
        assert "Reserva Especial" in result

    def test_multi_token_from_name(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Cuvée Alpha")
        assert "Cuvée Alpha" in result

    def test_accent_insensitive(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Chateau")
        assert "Château Test" in result

    def test_category_search(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Red")
        assert "Cuvée Alpha" in result
        assert "Reserva Especial" in result

    def test_full_name_search(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Château Test Cuvée Alpha 2020")
        assert "Cuvée Alpha" in result

    def test_empty_query(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "")
        assert "Empty search query" in result

    def test_fuzzy_off_by_default(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Barollo")
        assert "No wines found" in result

    def test_fuzzy_fallback(self, data_dir):
        con = get_connection(data_dir)
        # "Chateau Tst" close to "Château Test" — JW should trigger fuzzy match
        result = find_wine(con, "Chateau Tst", fuzzy=True)
        assert "Fuzzy matches" in result or "Château Test" in result or "No wines found" in result

    def test_limit_zero_returns_empty(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "France", limit=0)
        assert "No wines found" in result

    def test_limit_negative_raises(self, data_dir):
        con = get_connection(data_dir)
        with pytest.raises(QueryError):
            find_wine(con, "France", limit=-1)

    def test_straight_apostrophe_finds_curly_quote_winery(self, data_dir):
        """Straight apostrophe (U+0027) should match curly quote (U+2019) in data."""
        con = get_connection(data_dir)
        result = find_wine(con, "d'Aiguilhe")
        assert "Aiguilhe" in result

    def test_curly_apostrophe_finds_curly_quote_winery(self, data_dir):
        """Searching with the same curly quote as in the data should also work."""
        con = get_connection(data_dir)
        result = find_wine(con, "d\u2019Aiguilhe")
        assert "Aiguilhe" in result

    def test_straight_and_curly_quote_return_same_results(self, data_dir):
        """Both quote variants should return identical results."""
        con = get_connection(data_dir)
        straight = find_wine(con, "d'Aiguilhe")
        curly = find_wine(con, "d\u2019Aiguilhe")
        assert straight == curly


class TestNormaliseQueryTokens:
    def test_synonym_expansion(self):
        result = _normalise_query_tokens(["rotwein"], {"rotwein": "red"})
        assert result == ["red"]

    def test_multi_word_expansion(self):
        result = _normalise_query_tokens(
            ["spätburgunder"], {"spätburgunder": "Pinot Noir"},
        )
        assert result == ["Pinot", "Noir"]

    def test_stopword_removal(self):
        result = _normalise_query_tokens(["weingut", "thörle"], {"weingut": ""})
        assert result == ["thörle"]

    def test_unknown_token_passthrough(self):
        result = _normalise_query_tokens(["barolo"], {"rotwein": "red"})
        assert result == ["barolo"]

    def test_mixed_expansion_and_passthrough(self):
        synonyms = {"rotwein": "red", "schweiz": "Switzerland"}
        result = _normalise_query_tokens(["rotwein", "schweiz"], synonyms)
        assert result == ["red", "Switzerland"]

    def test_stopword_with_remaining_tokens(self):
        synonyms = {"weingut": "", "jahrgang": ""}
        result = _normalise_query_tokens(["weingut", "thörle"], synonyms)
        assert result == ["thörle"]

    def test_all_stopwords_preserves_original(self):
        synonyms = {"wein": "", "zum": ""}
        result = _normalise_query_tokens(["wein", "zum"], synonyms)
        assert result == ["wein", "zum"]

    def test_case_insensitive_lookup(self):
        result = _normalise_query_tokens(["ROTWEIN"], {"rotwein": "red"})
        assert result == ["red"]

    def test_empty_synonyms_dict(self):
        result = _normalise_query_tokens(["bordeaux"], {})
        assert result == ["bordeaux"]

    def test_single_token_stopword_preserves(self):
        result = _normalise_query_tokens(["wein"], {"wein": ""})
        assert result == ["wein"]


class TestFindWineWithSynonyms:
    def test_german_category_synonym(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "rotwein", synonyms={"rotwein": "red"})
        assert "Cuvée Alpha" in result

    def test_german_country_synonym(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Frankreich", synonyms={"frankreich": "France"})
        assert "Cuvée Alpha" in result

    def test_grape_synonym_multi_word(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(
            con, "Tempranillo", synonyms={"tempranillo": "Tempranillo"},
        )
        assert "Reserva Especial" in result

    def test_stopword_removal(self, data_dir):
        con = get_connection(data_dir)
        # "Château" is a stopword → drops it, searches "Test" only
        result = find_wine(con, "Château Test", synonyms={"château": ""})
        assert "Château Test" in result

    def test_synonym_plus_normal_token(self, data_dir):
        con = get_connection(data_dir)
        # "frankreich" → "France" + "Merlot" → both must match
        synonyms = {"frankreich": "France"}
        result = find_wine(con, "frankreich Merlot", synonyms=synonyms)
        assert "Cuvée Alpha" in result

    def test_none_synonyms_unchanged_behaviour(self, data_dir):
        con = get_connection(data_dir)
        result = find_wine(con, "Alpha", synonyms=None)
        assert "Cuvée Alpha" in result


# ---------------------------------------------------------------------------
# Intent detection — unit tests
# ---------------------------------------------------------------------------


class TestExtractIntents:
    def test_ready_to_drink(self):
        result = _extract_intents(["ready", "to", "drink"], 1)
        assert "drinking_status" in result.where_clauses[0]
        assert "optimal" in result.where_params
        assert "drinkable" in result.where_params
        assert result.consumed_indices == {0, 1, 2}

    def test_too_young(self):
        result = _extract_intents(["too", "young"], 1)
        assert "drinking_status" in result.where_clauses[0]
        assert "too_young" in result.where_params
        assert result.consumed_indices == {0, 1}

    def test_past_optimal(self):
        result = _extract_intents(["past", "optimal"], 1)
        assert "past_optimal" in result.where_params

    def test_drinkable(self):
        result = _extract_intents(["drinkable"], 1)
        assert "drinking_status" in result.where_clauses[0]

    def test_price_under(self):
        result = _extract_intents(["under", "30"], 1)
        assert "price" in result.where_clauses[0]
        assert 30.0 in result.where_params
        assert result.consumed_indices == {0, 1}

    def test_price_below(self):
        result = _extract_intents(["below", "25"], 1)
        assert "price" in result.where_clauses[0]
        assert 25.0 in result.where_params

    def test_price_cheaper_than(self):
        result = _extract_intents(["cheaper", "than", "50"], 1)
        assert "price" in result.where_clauses[0]
        assert 50.0 in result.where_params
        assert result.consumed_indices == {0, 1, 2}

    def test_budget(self):
        result = _extract_intents(["budget"], 1)
        assert "price_tier" in result.where_clauses[0]
        assert "budget" in result.where_params

    def test_top_rated(self):
        result = _extract_intents(["top", "rated"], 1)
        assert "best_pro_score" in result.where_clauses[0]
        assert result.order_by is not None
        assert "best_pro_score DESC" in result.order_by

    def test_best_rated(self):
        result = _extract_intents(["best", "rated"], 1)
        assert "best_pro_score" in result.where_clauses[0]

    def test_low_stock(self):
        result = _extract_intents(["low", "stock"], 1)
        assert "bottles_stored" in result.where_clauses[0]
        assert result.order_by is not None
        assert "bottles_stored ASC" in result.order_by

    def test_last_bottle(self):
        result = _extract_intents(["last", "bottle"], 1)
        assert "bottles_stored" in result.where_clauses[0]

    def test_running_low(self):
        result = _extract_intents(["running", "low"], 1)
        assert "bottles_stored" in result.where_clauses[0]

    def test_no_intent_passthrough(self):
        result = _extract_intents(["Barolo", "2020"], 1)
        assert result.where_clauses == []
        assert result.where_params == []
        assert result.order_by is None
        assert result.consumed_indices == set()

    def test_mixed_intent_and_text(self):
        result = _extract_intents(["Barolo", "ready", "to", "drink"], 1)
        assert len(result.where_clauses) == 1
        assert result.consumed_indices == {1, 2, 3}
        # Index 0 (Barolo) not consumed.
        assert 0 not in result.consumed_indices

    def test_numeric_tail_rejects_vintage(self):
        """'under 2020' should NOT match price intent (2020 looks like a vintage)."""
        result = _extract_intents(["under", "2020"], 1)
        assert result.where_clauses == []

    def test_param_idx_offset(self):
        """Parameters should start at the given param_idx."""
        result = _extract_intents(["under", "30"], 5)
        assert "$5" in result.where_clauses[0]

    def test_multiple_intents(self):
        """Multiple non-overlapping intents accumulate."""
        result = _extract_intents(["ready", "to", "drink", "under", "30"], 1)
        assert len(result.where_clauses) == 2
        assert result.consumed_indices == {0, 1, 2, 3, 4}


# ---------------------------------------------------------------------------
# Concept expansion — unit tests
# ---------------------------------------------------------------------------


class TestSearchCols:
    """Verify _SEARCH_COLS contains all expected columns."""

    def test_contains_core_text_columns(self):
        for col in ("wine_name", "winery_name", "country", "region",
                     "subregion", "classification", "category", "primary_grape"):
            assert col in _SEARCH_COLS

    def test_contains_style_columns(self):
        for col in ("subcategory", "sweetness", "effervescence", "specialty"):
            assert col in _SEARCH_COLS

    def test_total_count(self):
        assert len(_SEARCH_COLS) == 12


class TestConceptExpansion:
    def test_concept_expansions_dict_has_sparkling(self):
        assert "sparkling" in _CONCEPT_EXPANSIONS
        assert "Champagne" in _CONCEPT_EXPANSIONS["sparkling"]
        assert "Prosecco" in _CONCEPT_EXPANSIONS["sparkling"]

    def test_concept_expansions_dict_has_dessert(self):
        assert "dessert" in _CONCEPT_EXPANSIONS
        assert "Sauternes" in _CONCEPT_EXPANSIONS["dessert"]

    def test_concept_expansions_dict_has_fortified(self):
        assert "fortified" in _CONCEPT_EXPANSIONS
        assert "Port" in _CONCEPT_EXPANSIONS["fortified"]

    def test_concept_expansions_dict_has_sweet(self):
        assert "sweet" in _CONCEPT_EXPANSIONS
        assert "Tokaji" in _CONCEPT_EXPANSIONS["sweet"]

    def test_concept_expansions_dict_has_natural(self):
        assert "natural" in _CONCEPT_EXPANSIONS

    def test_system_concepts_has_tracked(self):
        assert "tracked" in _SYSTEM_CONCEPTS
        sql, params = _SYSTEM_CONCEPTS["tracked"]
        assert "tracked_wine_id IS NOT NULL" in sql
        assert params == []

    def test_system_concepts_has_favorite_variants(self):
        for key in ("favorite", "favourite", "favorites", "favourites"):
            assert key in _SYSTEM_CONCEPTS
            sql, _ = _SYSTEM_CONCEPTS[key]
            assert "is_favorite" in sql

    def test_system_concepts_has_wishlist(self):
        assert "wishlist" in _SYSTEM_CONCEPTS
        sql, _ = _SYSTEM_CONCEPTS["wishlist"]
        assert "is_wishlist" in sql

    def test_non_concept_not_in_dicts(self):
        assert "Barolo" not in _CONCEPT_EXPANSIONS
        assert "Barolo" not in _SYSTEM_CONCEPTS

    @pytest.mark.parametrize("key, expected", [
        ("shiraz", "Syrah"),
        ("syrah", "Shiraz"),
        ("garnacha", "Grenache"),
        ("grenache", "Garnacha"),
        ("monastrell", "Mourvèdre"),
        ("mourvèdre", "Monastrell"),
        ("primitivo", "Zinfandel"),
        ("zinfandel", "Primitivo"),
        ("tempranillo", "Tinta del Pais"),
        ("carignan", "Cariñena"),
        ("cariñena", "Carignan"),
        ("grigio", "Gris"),
        ("gris", "Grigio"),
    ])
    def test_grape_concept_bidirectional(self, key, expected):
        assert key in _CONCEPT_EXPANSIONS
        assert expected in _CONCEPT_EXPANSIONS[key]


# ---------------------------------------------------------------------------
# Intent detection — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_intent_dataset(tmp_path):
    """Write a dataset with diverse attributes for intent testing.

    Wines:
      id=1: optimal, list_price=25, price_tier=everyday, pro_rating=95, 2 stored bottles
      id=2: drinkable, list_price=15, price_tier=budget, no pro_rating, 1 stored bottle
      id=3: too_young, list_price=80, price_tier=premium, pro_rating=92, 3 stored bottles
      id=4: past_optimal, list_price=12, price_tier=budget, no pro_rating, 1 stored bottle (low stock)
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Domaine Alpha", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Bodega Beta", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Château Gamma", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Weingut Delta", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1, "country": "France", "region": "Burgundy",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 2, "country": "Spain", "region": "Rioja",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 3, "country": "France", "region": "Bordeaux",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 4, "country": "Germany", "region": "Rheinhessen",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Pinot Noir", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Tempranillo", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Riesling", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1, "wine_slug": "domaine-alpha-reserve-2020",
            "winery_id": 1, "name": "Réserve",
            "vintage": 2020, "is_non_vintage": False, "appellation_id": 1,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 13.5,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2023, "drink_until": 2030,
            "optimal_from": 2025, "optimal_until": 2028,
            "original_list_price": Decimal("25.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("25.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Domaine Alpha Réserve 2020",
            "grape_type": "varietal", "primary_grape": "Pinot Noir",
            "grape_summary": "Pinot Noir", "_raw_grapes": None,
            "dossier_path": "cellar/0001-domaine-alpha-reserve-2020.md",
            "drinking_status": "optimal", "age_years": 5,
            "price_tier": "everyday", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 2, "wine_slug": "bodega-beta-crianza-2019",
            "winery_id": 2, "name": "Crianza",
            "vintage": 2019, "is_non_vintage": False, "appellation_id": 2,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 13.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2022, "drink_until": 2027,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": Decimal("15.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("15.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Bodega Beta Crianza 2019",
            "grape_type": "varietal", "primary_grape": "Tempranillo",
            "grape_summary": "Tempranillo", "_raw_grapes": None,
            "dossier_path": "cellar/0002-bodega-beta-crianza-2019.md",
            "drinking_status": "drinkable", "age_years": 6,
            "price_tier": "budget", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 3, "wine_slug": "chateau-gamma-grand-vin-2021",
            "winery_id": 3, "name": "Grand Vin",
            "vintage": 2021, "is_non_vintage": False, "appellation_id": 3,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 14.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2027, "drink_until": 2040,
            "optimal_from": 2030, "optimal_until": 2038,
            "original_list_price": Decimal("80.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("80.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Château Gamma Grand Vin 2021",
            "grape_type": "varietal", "primary_grape": "Merlot",
            "grape_summary": "Merlot", "_raw_grapes": None,
            "dossier_path": "cellar/0003-chateau-gamma-grand-vin-2021.md",
            "drinking_status": "too_young", "age_years": 4,
            "price_tier": "premium", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 4, "wine_slug": "weingut-delta-spatburgunder-2016",
            "winery_id": 4, "name": "Spätburgunder",
            "vintage": 2016, "is_non_vintage": False, "appellation_id": 4,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 12.5,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2019, "drink_until": 2024,
            "optimal_from": 2020, "optimal_until": 2023,
            "original_list_price": Decimal("12.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("12.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Weingut Delta Spätburgunder 2016",
            "grape_type": "varietal", "primary_grape": "Riesling",
            "grape_summary": "Riesling", "_raw_grapes": None,
            "dossier_path": "cellar/0004-weingut-delta-spatburgunder-2016.md",
            "drinking_status": "past_optimal", "age_years": 9,
            "price_tier": "budget", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 2, "grape_id": 2, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 3, "grape_id": 3, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 4, "grape_id": 4, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        # Wine 1: 2 stored bottles
        {"bottle_id": 1, "wine_id": 1, "status": "stored",
         "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("25.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("25.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        {"bottle_id": 2, "wine_id": 1, "status": "stored",
         "cellar_id": 1, "shelf": "A2", "bottle_number": 2,
         "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("25.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("25.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        # Wine 2: 1 stored bottle
        {"bottle_id": 3, "wine_id": 2, "status": "stored",
         "cellar_id": 1, "shelf": "B1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2022, 1, 15).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("15.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("15.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        # Wine 3: 3 stored bottles
        {"bottle_id": 4, "wine_id": 3, "status": "stored",
         "cellar_id": 1, "shelf": "C1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2023, 9, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("80.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("80.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        {"bottle_id": 5, "wine_id": 3, "status": "stored",
         "cellar_id": 1, "shelf": "C2", "bottle_number": 2,
         "provider_id": 1, "purchase_date": datetime(2023, 9, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("80.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("80.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        {"bottle_id": 6, "wine_id": 3, "status": "stored",
         "cellar_id": 1, "shelf": "C3", "bottle_number": 3,
         "provider_id": 1, "purchase_date": datetime(2023, 9, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("80.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("80.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        # Wine 4: 1 stored bottle (low stock)
        {"bottle_id": 7, "wine_id": 4, "status": "stored",
         "cellar_id": 1, "shelf": "D1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2020, 3, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("12.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("12.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
    ]
    cellars = [
        {"cellar_id": 1, "name": "Main Cellar", "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    providers = [
        {"provider_id": 1, "name": "Wine Shop", "etl_run_id": rid, "updated_at": now},
    ]
    tastings = []
    pro_ratings = [
        {"rating_id": 1, "wine_id": 1, "source": "Parker",
         "score": 95.0, "max_score": 100, "review_text": "Outstanding",
         "etl_run_id": rid, "updated_at": now},
        {"rating_id": 2, "wine_id": 3, "source": "Suckling",
         "score": 92.0, "max_score": 100, "review_text": "Excellent potential",
         "etl_run_id": rid, "updated_at": now},
    ]
    etl_runs = [
        {
            "run_id": 1, "started_at": now, "finished_at": now,
            "run_type": "full", "wines_source_hash": "abc",
            "bottles_source_hash": "def", "bottles_gone_source_hash": None,
            "total_inserts": 4, "total_updates": 0, "total_deletes": 0,
            "wines_inserted": 4, "wines_updated": 0,
            "wines_deleted": 0, "wines_renamed": 0,
        },
    ]
    change_logs = [
        {"change_id": 1, "run_id": 1, "entity_type": "wine",
         "entity_id": 1, "change_type": "insert", "changed_fields": None},
    ]

    for name, rows in [
        ("winery", wineries), ("appellation", appellations),
        ("grape", grapes), ("wine", wines), ("wine_grape", wine_grapes),
        ("bottle", bottles), ("cellar", cellars), ("provider", providers),
        ("tasting", tastings), ("pro_rating", pro_ratings),
        ("etl_run", etl_runs), ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def intent_dir(tmp_path):
    return _make_intent_dataset(tmp_path)


class TestFindWineWithIntents:
    """Integration tests: find_wine with intent-based queries."""

    def test_ready_to_drink_returns_optimal_and_drinkable(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "ready to drink")
        # Wine 1 (optimal) and wine 2 (drinkable) should match.
        assert "Réserve" in result        # wine 1
        assert "Crianza" in result          # wine 2
        assert "Grand Vin" not in result    # wine 3 (too_young)

    def test_too_young_filter(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "too young")
        assert "Grand Vin" in result
        assert "Réserve" not in result

    def test_past_optimal_filter(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "past optimal")
        assert "Spätburgunder" in result
        assert "Réserve" not in result

    def test_price_under(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "under 20")
        # Wine 2 (15 CHF) and wine 4 (12 CHF) should match.
        assert "Crianza" in result
        assert "Spätburgunder" in result
        assert "Grand Vin" not in result  # 80 CHF

    def test_budget(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "budget")
        # Wine 2 and wine 4 have price_tier=budget.
        assert "Crianza" in result
        assert "Spätburgunder" in result
        assert "Grand Vin" not in result

    def test_top_rated_ordering(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "top rated")
        # Wines 1 (95) and 3 (92) have pro_ratings; result ordered by score DESC.
        assert "Réserve" in result
        assert "Grand Vin" in result
        lines = [l for l in result.strip().split("\n")
                 if l.strip().startswith("|") and "---" not in l][1:]
        # First data row should be wine 1 (score 95).
        assert "95" in lines[0] or "Réserve" in lines[0]

    def test_low_stock(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "low stock")
        # Wine 2 (1 bottle) and wine 4 (1 bottle) are low stock.
        assert "Crianza" in result
        assert "Spätburgunder" in result
        # Wine 1 (2 bottles) is on the boundary — BETWEEN 1 AND 2 includes it.
        assert "Réserve" in result
        # Wine 3 (3 bottles) should not appear.
        assert "Grand Vin" not in result

    def test_intent_plus_text(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "Bordeaux too young")
        # Only wine 3 is both Bordeaux and too_young.
        assert "Grand Vin" in result
        assert "Réserve" not in result

    def test_intent_only_no_text_tokens(self, intent_dir):
        """Query consisting entirely of intent tokens should still work."""
        con = get_connection(intent_dir)
        result = find_wine(con, "ready to drink")
        assert "No wines found" not in result

    def test_synonym_to_intent_chain(self, intent_dir):
        """German synonym 'trinkreif' → 'ready to drink' → drinking_status intent."""
        con = get_connection(intent_dir)
        synonyms = {"trinkreif": "ready to drink"}
        result = find_wine(con, "trinkreif", synonyms=synonyms)
        assert "Réserve" in result
        assert "Crianza" in result
        assert "Grand Vin" not in result


# ---------------------------------------------------------------------------
# Concept expansion — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_concept_dataset(tmp_path):
    """Write a dataset with concept-relevant attributes for concept testing.

    Wines:
      id=1: category=Red wine, France/Burgundy, not tracked, not favorite
      id=2: category=Sparkling wine, Italy, wine_name="Prosecco Brut"
      id=3: category=Dessert wine, France/Bordeaux, wine_name="Sauternes 1er Cru"
      id=4: category=Red wine, tracked, is_favorite=True
      id=5: category=Red wine, is_wishlist=True
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Domaine Alpha", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Casa Vinicola Beta", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Château Gamma", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Bodega Delta", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 5, "name": "Cantina Epsilon", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1, "country": "France", "region": "Burgundy",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 2, "country": "Italy", "region": "Veneto",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 3, "country": "France", "region": "Bordeaux",
            "subregion": "Sauternes", "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 4, "country": "Spain", "region": "Rioja",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "appellation_id": 5, "country": "Italy", "region": "Toscana",
            "subregion": None, "classification": None,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    grapes = [
        {"grape_id": 1, "name": "Pinot Noir", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Glera", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Sémillon", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Tempranillo", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 5, "name": "Sangiovese", "etl_run_id": rid, "updated_at": now},
    ]
    wines = [
        {
            "wine_id": 1, "wine_slug": "domaine-alpha-reserve-2020",
            "winery_id": 1, "name": "Réserve",
            "vintage": 2020, "is_non_vintage": False, "appellation_id": 1,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 13.5,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2023, "drink_until": 2030,
            "optimal_from": 2025, "optimal_until": 2028,
            "original_list_price": Decimal("25.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("25.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Domaine Alpha Réserve 2020",
            "grape_type": "varietal", "primary_grape": "Pinot Noir",
            "grape_summary": "Pinot Noir", "_raw_grapes": None,
            "dossier_path": "cellar/0001-domaine-alpha-reserve-2020.md",
            "drinking_status": "optimal", "age_years": 5,
            "price_tier": "everyday", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 2, "wine_slug": "casa-vinicola-beta-prosecco-brut-2022",
            "winery_id": 2, "name": "Prosecco Brut",
            "vintage": 2022, "is_non_vintage": False, "appellation_id": 2,
            "category": "Sparkling wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 11.5,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2022, "drink_until": 2025,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": Decimal("18.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("18.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Casa Vinicola Beta Prosecco Brut 2022",
            "grape_type": "varietal", "primary_grape": "Glera",
            "grape_summary": "Glera", "_raw_grapes": None,
            "dossier_path": "cellar/0002-casa-vinicola-beta-prosecco-brut-2022.md",
            "drinking_status": "drinkable", "age_years": 3,
            "price_tier": "budget", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 3, "wine_slug": "chateau-gamma-sauternes-2018",
            "winery_id": 3, "name": "Sauternes 1er Cru",
            "vintage": 2018, "is_non_vintage": False, "appellation_id": 3,
            "category": "Dessert wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 375,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 14.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2022, "drink_until": 2040,
            "optimal_from": 2025, "optimal_until": 2035,
            "original_list_price": Decimal("45.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("45.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": "Château Gamma Sauternes 1er Cru 2018",
            "grape_type": "varietal", "primary_grape": "Sémillon",
            "grape_summary": "Sémillon", "_raw_grapes": None,
            "dossier_path": "cellar/0003-chateau-gamma-sauternes-2018.md",
            "drinking_status": "optimal", "age_years": 7,
            "price_tier": "everyday", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 4, "wine_slug": "bodega-delta-crianza-2019",
            "winery_id": 4, "name": "Crianza",
            "vintage": 2019, "is_non_vintage": False, "appellation_id": 4,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 13.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2022, "drink_until": 2028,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": Decimal("20.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("20.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": True, "is_wishlist": False,
            "tracked_wine_id": 90001,
            "full_name": "Bodega Delta Crianza 2019",
            "grape_type": "varietal", "primary_grape": "Tempranillo",
            "grape_summary": "Tempranillo", "_raw_grapes": None,
            "dossier_path": "cellar/0004-bodega-delta-crianza-2019.md",
            "drinking_status": "drinkable", "age_years": 6,
            "price_tier": "budget", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
        {
            "wine_id": 5, "wine_slug": "cantina-epsilon-chianti-2021",
            "winery_id": 5, "name": "Chianti",
            "vintage": 2021, "is_non_vintage": False, "appellation_id": 5,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 13.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2023, "drink_until": 2028,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": Decimal("15.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("15.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": True,
            "tracked_wine_id": None,
            "full_name": "Cantina Epsilon Chianti 2021",
            "grape_type": "varietal", "primary_grape": "Sangiovese",
            "grape_summary": "Sangiovese", "_raw_grapes": None,
            "dossier_path": "cellar/0005-cantina-epsilon-chianti-2021.md",
            "drinking_status": "drinkable", "age_years": 4,
            "price_tier": "budget", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 2, "grape_id": 2, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 3, "grape_id": 3, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 4, "grape_id": 4, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
        {"wine_id": 5, "grape_id": 5, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    bottles = [
        {"bottle_id": 1, "wine_id": 1, "status": "stored",
         "cellar_id": 1, "shelf": "A1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2023, 6, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("25.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("25.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        {"bottle_id": 2, "wine_id": 2, "status": "stored",
         "cellar_id": 1, "shelf": "B1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2023, 1, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("18.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("18.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        {"bottle_id": 3, "wine_id": 3, "status": "stored",
         "cellar_id": 1, "shelf": "C1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2022, 11, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("45.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("45.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        {"bottle_id": 4, "wine_id": 4, "status": "stored",
         "cellar_id": 1, "shelf": "D1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2022, 5, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("20.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("20.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
        {"bottle_id": 5, "wine_id": 5, "status": "stored",
         "cellar_id": 1, "shelf": "E1", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2023, 3, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("15.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("15.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now},
    ]
    cellars = [
        {"cellar_id": 1, "name": "Main Cellar", "sort_order": 1,
         "etl_run_id": rid, "updated_at": now},
    ]
    providers = [
        {"provider_id": 1, "name": "Wine Shop", "etl_run_id": rid, "updated_at": now},
    ]
    tastings = []
    pro_ratings = []
    etl_runs = [
        {
            "run_id": 1, "started_at": now, "finished_at": now,
            "run_type": "full", "wines_source_hash": "abc",
            "bottles_source_hash": "def", "bottles_gone_source_hash": None,
            "total_inserts": 5, "total_updates": 0, "total_deletes": 0,
            "wines_inserted": 5, "wines_updated": 0,
            "wines_deleted": 0, "wines_renamed": 0,
        },
    ]
    change_logs = [
        {"change_id": 1, "run_id": 1, "entity_type": "wine",
         "entity_id": 1, "change_type": "insert", "changed_fields": None},
    ]

    for name, rows in [
        ("winery", wineries), ("appellation", appellations),
        ("grape", grapes), ("wine", wines), ("wine_grape", wine_grapes),
        ("bottle", bottles), ("cellar", cellars), ("provider", providers),
        ("tasting", tastings), ("pro_rating", pro_ratings),
        ("etl_run", etl_runs), ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def concept_dir(tmp_path):
    return _make_concept_dataset(tmp_path)


class TestFindWineWithConcepts:
    """Integration tests: find_wine with concept expansion queries."""

    def test_sparkling_finds_sparkling_wine(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "sparkling")
        # Wine 2 (Prosecco Brut, category=Sparkling wine) should match
        # via both category ILIKE and concept expansion "Prosecco".
        assert "Prosecco" in result
        assert "Réserve" not in result

    def test_dessert_finds_dessert_wine(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "dessert")
        # Wine 3 (Sauternes, category=Dessert wine) should match
        # via both category ILIKE and concept expansion "Sauternes".
        assert "Sauternes" in result
        assert "Prosecco" not in result

    def test_concept_plus_region(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "sparkling Italy")
        # Wine 2 is sparkling + Italy → should match.
        assert "Prosecco" in result
        # Wine 3 is dessert + France → should not match.
        assert "Sauternes" not in result

    def test_tracked_system_concept(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "tracked")
        # Only wine 4 has tracked_wine_id.
        assert "Crianza" in result
        assert "Réserve" not in result
        assert "Prosecco" not in result

    def test_favorite_system_concept(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "favorite")
        # Only wine 4 has is_favorite=True.
        assert "Crianza" in result
        assert "Chianti" not in result

    def test_favourite_british_spelling(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "favourite")
        assert "Crianza" in result

    def test_wishlist_system_concept(self, concept_dir):
        con = get_connection(concept_dir)
        result = find_wine(con, "wishlist")
        # Only wine 5 has is_wishlist=True.
        assert "Chianti" in result
        assert "Crianza" not in result

    def test_synonym_to_concept_chain(self, concept_dir):
        """German 'schaumwein' → 'sparkling' (synonym) → concept expansion."""
        con = get_connection(concept_dir)
        synonyms = {"schaumwein": "sparkling"}
        result = find_wine(con, "schaumwein", synonyms=synonyms)
        assert "Prosecco" in result
        assert "Réserve" not in result

    def test_non_concept_unchanged(self, concept_dir):
        """Normal text tokens bypass concept expansion."""
        con = get_connection(concept_dir)
        result = find_wine(con, "Pinot Noir")
        assert "Réserve" in result
        assert "Prosecco" not in result

    def test_system_concept_plus_text(self, concept_dir):
        """System concept combined with text narrows results."""
        con = get_connection(concept_dir)
        result = find_wine(con, "tracked Spain")
        # Wine 4 is tracked + Spain.
        assert "Crianza" in result
        assert "Réserve" not in result


# ---------------------------------------------------------------------------
# Grape concept expansions — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_grape_synonym_dataset(tmp_path):
    """Write a dataset with grape synonym pairs for concept expansion testing.

    Wines:
      id=1: primary_grape=Shiraz, Australia/Barossa Valley
      id=2: primary_grape=Syrah, France/Rhône
      id=3: primary_grape=Garnacha, Spain/Navarra
      id=4: primary_grape=Grenache, France/Châteauneuf-du-Pape
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Henschke", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Guigal", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Bodegas Ochoa", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Château Beaucastel", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {"appellation_id": 1, "country": "Australia", "region": "Barossa Valley",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 2, "country": "France", "region": "Vallée du Rhône",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 3, "country": "Spain", "region": "Navarra",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 4, "country": "France", "region": "Châteauneuf-du-Pape",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
    ]
    grapes = [
        {"grape_id": 1, "name": "Shiraz", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Syrah", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Garnacha", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Grenache", "etl_run_id": rid, "updated_at": now},
    ]

    def _wine(wid, winery_id, app_id, name, grape, full_name):
        return {
            "wine_id": wid, "wine_slug": full_name.lower().replace(" ", "-"),
            "winery_id": winery_id, "name": name,
            "vintage": 2020, "is_non_vintage": False, "appellation_id": app_id,
            "category": "Red wine", "_raw_classification": None,
            "subcategory": None, "specialty": None,
            "sweetness": None, "effervescence": None, "volume_ml": 750,
            "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 14.0,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2023, "drink_until": 2030,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": Decimal("30.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("30.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": full_name,
            "grape_type": "varietal", "primary_grape": grape,
            "grape_summary": grape, "_raw_grapes": None,
            "dossier_path": f"cellar/{wid:04d}-test.md",
            "drinking_status": "drinkable", "age_years": 5,
            "price_tier": "everyday", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        }

    wines = [
        _wine(1, 1, 1, "Hill of Grace", "Shiraz", "Henschke Hill of Grace 2020"),
        _wine(2, 2, 2, "Côte-Rôtie", "Syrah", "Guigal Côte-Rôtie 2020"),
        _wine(3, 3, 3, "Rosado", "Garnacha", "Bodegas Ochoa Rosado 2020"),
        _wine(4, 4, 4, "Hommage", "Grenache", "Château Beaucastel Hommage 2020"),
    ]
    wine_grapes = [
        {"wine_id": i, "grape_id": i, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now}
        for i in range(1, 5)
    ]
    bottles = [
        {"bottle_id": i, "wine_id": i, "status": "stored",
         "cellar_id": 1, "shelf": f"A{i}", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2023, 1, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("30.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("30.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now}
        for i in range(1, 5)
    ]
    cellars = [{"cellar_id": 1, "name": "Main", "sort_order": 1,
                "etl_run_id": rid, "updated_at": now}]
    providers = [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]
    etl_runs = [{
        "run_id": 1, "started_at": now, "finished_at": now,
        "run_type": "full", "wines_source_hash": "abc",
        "bottles_source_hash": "def", "bottles_gone_source_hash": None,
        "total_inserts": 4, "total_updates": 0, "total_deletes": 0,
        "wines_inserted": 4, "wines_updated": 0,
        "wines_deleted": 0, "wines_renamed": 0,
    }]
    change_logs = [{"change_id": 1, "run_id": 1, "entity_type": "wine",
                    "entity_id": 1, "change_type": "insert", "changed_fields": None}]

    for name, rows in [
        ("winery", wineries), ("appellation", appellations),
        ("grape", grapes), ("wine", wines), ("wine_grape", wine_grapes),
        ("bottle", bottles), ("cellar", cellars), ("provider", providers),
        ("tasting", []), ("pro_rating", []),
        ("etl_run", etl_runs), ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def grape_synonym_dir(tmp_path):
    return _make_grape_synonym_dataset(tmp_path)


class TestGrapeSynonymConcepts:
    """Integration tests: grape concept expansions find cross-named varieties."""

    def test_shiraz_also_finds_syrah(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Shiraz")
        assert "Hill of Grace" in result  # primary_grape = Shiraz
        assert "Côte-Rôtie" in result     # primary_grape = Syrah

    def test_syrah_also_finds_shiraz(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Syrah")
        assert "Côte-Rôtie" in result     # primary_grape = Syrah
        assert "Hill of Grace" in result  # primary_grape = Shiraz

    def test_garnacha_also_finds_grenache(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Garnacha")
        assert "Rosado" in result   # primary_grape = Garnacha
        assert "Hommage" in result  # primary_grape = Grenache

    def test_grenache_also_finds_garnacha(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Grenache")
        assert "Hommage" in result  # primary_grape = Grenache
        assert "Rosado" in result   # primary_grape = Garnacha

    def test_grape_concept_combined_with_country(self, grape_synonym_dir):
        con = get_connection(grape_synonym_dir)
        result = find_wine(con, "Syrah France")
        assert "Côte-Rôtie" in result     # Syrah + France
        assert "Hill of Grace" not in result  # Shiraz but Australia


# ---------------------------------------------------------------------------
# Style column search — integration tests with find_wine
# ---------------------------------------------------------------------------


def _make_style_dataset(tmp_path):
    """Write a dataset with style attributes for style-column search testing.

    Wines:
      id=1: sweetness=dry, Riesling, Germany
      id=2: sweetness=sweet, specialty=late-harvest_grapes, Sauternes, France
      id=3: specialty=orange_wine, Italy/Friuli
      id=4: subcategory=champagne, effervescence=sparkling, France/Champagne
      id=5: specialty=ice_wine, Germany (for eiswein synonym test)
      id=6: no style attributes, Nebbiolo, Italy (negative control)
    """
    now = _now()
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Weingut Keller", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Château Suduiraut", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 3, "name": "Radikon", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 4, "name": "Krug", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 5, "name": "Weingut Dönnhoff", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 6, "name": "Giacomo Conterno", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {"appellation_id": 1, "country": "Germany", "region": "Rheinhessen",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 2, "country": "France", "region": "Bordeaux",
         "subregion": "Sauternes", "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 3, "country": "Italy", "region": "Friuli",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 4, "country": "France", "region": "Champagne",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 5, "country": "Germany", "region": "Nahe",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
        {"appellation_id": 6, "country": "Italy", "region": "Piemonte",
         "subregion": None, "classification": None,
         "etl_run_id": rid, "updated_at": now},
    ]
    grapes = [
        {"grape_id": 1, "name": "Riesling", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 2, "name": "Sémillon", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 3, "name": "Ribolla Gialla", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 4, "name": "Chardonnay", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 5, "name": "Riesling", "etl_run_id": rid, "updated_at": now},
        {"grape_id": 6, "name": "Nebbiolo", "etl_run_id": rid, "updated_at": now},
    ]

    def _wine(wid, winery_id, app_id, name, grape, full_name, *,
              sweetness=None, effervescence=None, specialty=None,
              subcategory=None, category="Red wine"):
        return {
            "wine_id": wid, "wine_slug": full_name.lower().replace(" ", "-"),
            "winery_id": winery_id, "name": name,
            "vintage": 2020, "is_non_vintage": False, "appellation_id": app_id,
            "category": category, "_raw_classification": None,
            "subcategory": subcategory, "specialty": specialty,
            "sweetness": sweetness, "effervescence": effervescence,
            "volume_ml": 750, "_raw_volume": None,
            "container": None, "hue": None, "cork": None, "alcohol_pct": 12.5,
            "acidity_g_l": None, "sugar_g_l": None, "ageing_type": None,
            "ageing_months": None, "farming_type": None, "serving_temp_c": None,
            "opening_type": None, "opening_minutes": None,
            "drink_from": 2022, "drink_until": 2030,
            "optimal_from": None, "optimal_until": None,
            "original_list_price": Decimal("30.00"),
            "original_list_currency": "CHF",
            "list_price": Decimal("30.00"), "list_currency": "CHF",
            "comment": None, "winemaking_notes": None,
            "is_favorite": False, "is_wishlist": False,
            "tracked_wine_id": None,
            "full_name": full_name,
            "grape_type": "varietal", "primary_grape": grape,
            "grape_summary": grape, "_raw_grapes": None,
            "dossier_path": f"cellar/{wid:04d}-test.md",
            "drinking_status": "drinkable", "age_years": 5,
            "price_tier": "everyday", "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        }

    wines = [
        _wine(1, 1, 1, "Trocken GG", "Riesling",
              "Weingut Keller Trocken GG 2020",
              sweetness="dry", category="White wine"),
        _wine(2, 2, 2, "Crème de Tête", "Sémillon",
              "Château Suduiraut Crème de Tête 2020",
              sweetness="sweet", specialty="late-harvest_grapes",
              category="Dessert wine"),
        _wine(3, 3, 3, "Oslavje", "Ribolla Gialla",
              "Radikon Oslavje 2020",
              specialty="orange_wine", category="White wine"),
        _wine(4, 4, 4, "Grande Cuvée", "Chardonnay",
              "Krug Grande Cuvée 2020",
              subcategory="champagne", effervescence="sparkling",
              category="Sparkling wine"),
        _wine(5, 5, 5, "Eiswein", "Riesling",
              "Weingut Dönnhoff Eiswein 2020",
              specialty="ice_wine", sweetness="sweet",
              category="Dessert wine"),
        _wine(6, 6, 6, "Monfortino", "Nebbiolo",
              "Giacomo Conterno Monfortino 2020",
              category="Red wine"),
    ]
    wine_grapes = [
        {"wine_id": i, "grape_id": i, "percentage": 100.0, "sort_order": 1,
         "etl_run_id": rid, "updated_at": now}
        for i in range(1, 7)
    ]
    bottles = [
        {"bottle_id": i, "wine_id": i, "status": "stored",
         "cellar_id": 1, "shelf": f"A{i}", "bottle_number": 1,
         "provider_id": 1, "purchase_date": datetime(2023, 1, 1).date(),
         "acquisition_type": "purchase",
         "original_purchase_price": Decimal("30.00"),
         "original_purchase_currency": "CHF",
         "purchase_price": Decimal("30.00"), "purchase_currency": "CHF",
         "purchase_comment": None,
         "output_date": None, "output_type": None, "output_comment": None,
         "is_onsite": True, "is_in_transit": False,
         "etl_run_id": rid, "updated_at": now}
        for i in range(1, 7)
    ]
    cellars = [{"cellar_id": 1, "name": "Main", "sort_order": 1,
                "etl_run_id": rid, "updated_at": now}]
    providers = [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]
    etl_runs = [{
        "run_id": 1, "started_at": now, "finished_at": now,
        "run_type": "full", "wines_source_hash": "abc",
        "bottles_source_hash": "def", "bottles_gone_source_hash": None,
        "total_inserts": 6, "total_updates": 0, "total_deletes": 0,
        "wines_inserted": 6, "wines_updated": 0,
        "wines_deleted": 0, "wines_renamed": 0,
    }]
    change_logs = [{"change_id": 1, "run_id": 1, "entity_type": "wine",
                    "entity_id": 1, "change_type": "insert", "changed_fields": None}]

    for name, rows in [
        ("winery", wineries), ("appellation", appellations),
        ("grape", grapes), ("wine", wines), ("wine_grape", wine_grapes),
        ("bottle", bottles), ("cellar", cellars), ("provider", providers),
        ("tasting", []), ("pro_rating", []),
        ("etl_run", etl_runs), ("change_log", change_logs),
    ]:
        writer.write_parquet(name, rows, tmp_path)

    return tmp_path


@pytest.fixture()
def style_dir(tmp_path):
    return _make_style_dataset(tmp_path)


class TestFindWineWithStyles:
    """Integration tests: find_wine matches style columns (sweetness, etc.)."""

    def test_sweetness_dry_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "dry")
        assert "Trocken GG" in result
        assert "Monfortino" not in result  # no sweetness attribute

    def test_sweetness_sweet_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "sweet")
        # "sweet" is also a concept expansion → matches dessert wine names
        assert "Crème de Tête" in result

    def test_specialty_orange_wine_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "orange_wine")
        assert "Oslavje" in result
        assert "Monfortino" not in result

    def test_subcategory_champagne_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "champagne")
        assert "Grande Cuvée" in result

    def test_effervescence_sparkling_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "sparkling")
        # Both concept expansion (Champagne in wine name) and effervescence column
        assert "Grande Cuvée" in result

    def test_specialty_ice_wine_matches(self, style_dir):
        con = get_connection(style_dir)
        result = find_wine(con, "ice_wine")
        assert "Eiswein" in result  # wine name contains Eiswein, specialty=ice_wine

    def test_eiswein_synonym_chain(self, style_dir):
        """eiswein synonym → ice_wine → matches specialty column."""
        con = get_connection(style_dir)
        synonyms = {"eiswein": "ice_wine"}
        result = find_wine(con, "eiswein", synonyms=synonyms)
        assert "Dönnhoff" in result

    def test_trocken_synonym_chain(self, style_dir):
        """trocken → dry via synonym → matches sweetness column."""
        con = get_connection(style_dir)
        synonyms = {"trocken": "dry"}
        result = find_wine(con, "trocken Riesling", synonyms=synonyms)
        assert "Trocken GG" in result

    def test_no_false_positives_from_style_columns(self, style_dir):
        """Unrelated queries should not gain spurious results from style cols."""
        con = get_connection(style_dir)
        result = find_wine(con, "Nebbiolo Italy")
        assert "Monfortino" in result
        # Only Monfortino matches; others shouldn't leak in
        assert "Trocken GG" not in result
        assert "Oslavje" not in result


# ---------------------------------------------------------------------------
# Soft AND fallback — integration tests with find_wine
# ---------------------------------------------------------------------------


class TestSoftAndFallback:
    """Integration tests: soft-AND fallback when strict AND returns 0."""

    def test_soft_and_recovers_with_one_bad_token(self, intent_dir):
        """One nonsense token among valid ones → partial match recovers."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France xyznothing Burgundy")
        # Wine 1 (France + Burgundy) should be recovered.
        assert "Réserve" in result

    def test_soft_and_shows_partial_match_header(self, intent_dir):
        con = get_connection(intent_dir)
        result = find_wine(con, "France xyznothing Burgundy")
        assert "Partial match" in result

    def test_soft_and_ranks_by_match_count(self, intent_dir):
        """Wines matching more tokens appear first."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France Burgundy Pinot xyznothing")
        assert "Partial match" in result
        # Wine 1 matches France + Burgundy + Pinot (3 tokens).
        # Wine 3 matches France only (1 token).
        assert "Réserve" in result
        lines = [
            l for l in result.strip().split("\n")
            if l.strip().startswith("|") and "---" not in l
        ][1:]  # skip header row
        # Wine 1 should appear before wine 3.
        reserve_idx = next(i for i, l in enumerate(lines) if "Réserve" in l)
        grand_vin_idx = next(i for i, l in enumerate(lines) if "Grand Vin" in l)
        assert reserve_idx < grand_vin_idx

    def test_soft_and_skipped_for_single_token(self, intent_dir):
        """Single ILIKE token → nothing to relax, falls through to no-result."""
        con = get_connection(intent_dir)
        result = find_wine(con, "xyznothing")
        assert "No wines found" in result

    def test_soft_and_skipped_for_single_ilike_with_intent(self, intent_dir):
        """Intent consumes most tokens, only 1 ILIKE left → no soft AND."""
        con = get_connection(intent_dir)
        result = find_wine(con, "ready to drink xyznothing")
        # "ready to drink" consumed by intent → only "xyznothing" as ILIKE.
        # Single ILIKE condition → soft AND skipped.
        assert "No wines found" in result

    def test_all_tokens_fail_no_result(self, intent_dir):
        """All ILIKE tokens nonsense → soft AND fires but still 0 results."""
        con = get_connection(intent_dir)
        result = find_wine(con, "xyzaaa xyzbbbb")
        assert "No wines found" in result

    def test_strict_and_still_preferred(self, intent_dir):
        """When strict AND works, no partial-match header appears."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France Burgundy")
        assert "Partial match" not in result
        assert "Réserve" in result

    def test_soft_and_before_fuzzy(self, intent_dir):
        """Soft AND fires before fuzzy; result is ILIKE-based, not fuzzy."""
        con = get_connection(intent_dir)
        result = find_wine(con, "France xyznothing Burgundy", fuzzy=True)
        # Soft AND should recover results — not fuzzy.
        assert "Partial match" in result
        assert "match_score" not in result


# ---------------------------------------------------------------------------
# Price tracking tests
# ---------------------------------------------------------------------------

from cellarbrain.query import log_price, get_tracked_wine_prices, get_price_history, wishlist_alerts
from cellarbrain.dossier_ops import TrackedWineNotFoundError


def _make_dataset_with_tracked(tmp_path):
    """Write a dataset that includes tracked_wine for price tests."""
    now = _now()
    rid = 1
    # Use the base dataset and add tracked_wine
    base_dir = _make_dataset(tmp_path)
    tracked = [
        {
            "tracked_wine_id": 90_001, "winery_id": 1, "wine_name": "Cuvée Alpha",
            "category": "Red wine", "appellation_id": 1,
            "dossier_path": "tracked/90001-chateau-test-cuvee-alpha.md",
            "is_deleted": False,
            "etl_run_id": rid, "updated_at": now,
        },
    ]
    writer.write_parquet("tracked_wine", tracked, base_dir)
    return base_dir


@pytest.fixture()
def price_dir(tmp_path):
    return _make_dataset_with_tracked(tmp_path)


class TestLogPrice:
    def test_basic(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Wine Shop A",
            "price": Decimal("45.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        result = log_price(price_dir, obs)
        assert "Recorded" in result
        assert "Wine Shop A" in result

    def test_deduplication(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Wine Shop A",
            "price": Decimal("45.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 10, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)

        # Same key, same day, different time → should replace
        obs2 = {**obs, "price": Decimal("42.00"), "observed_at": datetime(2026, 4, 7, 15, 0)}
        result = log_price(price_dir, obs2)
        assert "Updated" in result

        # Only 1 row should remain
        from cellarbrain.writer import read_partitioned_parquet_rows
        rows = read_partitioned_parquet_rows("price_observation", price_dir)
        assert len(rows) == 1
        assert rows[0]["price"] == Decimal("42.00")

    def test_auto_convert_chf(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001,
            "vintage": 2020,
            "bottle_size_ml": 750,
            "retailer_name": "Euro Shop",
            "price": Decimal("20.00"),
            "currency": "EUR",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        result = log_price(price_dir, obs)
        assert "Recorded" in result

        from cellarbrain.writer import read_partitioned_parquet_rows
        rows = read_partitioned_parquet_rows("price_observation", price_dir)
        assert len(rows) == 1
        # EUR rate is 0.93, so 20.00 * 0.93 = 18.60
        assert rows[0]["price_chf"] == Decimal("18.60")

    def test_invalid_tracked_wine(self, price_dir):
        obs = {
            "tracked_wine_id": 999,
            "bottle_size_ml": 750,
            "retailer_name": "Shop",
            "price": Decimal("10.00"),
            "currency": "CHF",
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        with pytest.raises(TrackedWineNotFoundError):
            log_price(price_dir, obs)

    def test_missing_required_field(self, price_dir):
        obs = {"tracked_wine_id": 90_001}
        with pytest.raises(ValueError, match="Missing required fields"):
            log_price(price_dir, obs)


class TestGetTrackedWinePrices:
    def test_returns_table(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop A", "price": Decimal("45.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = get_tracked_wine_prices(price_dir, 90_001)
        assert "Shop A" in result
        assert "|" in result

    def test_no_data(self, price_dir):
        result = get_tracked_wine_prices(price_dir, 90_001)
        assert "No price observations" in result

    def test_invalid_tracked_wine(self, price_dir):
        with pytest.raises(TrackedWineNotFoundError):
            get_tracked_wine_prices(price_dir, 999)


# ---------------------------------------------------------------------------
# TestLogging
# ---------------------------------------------------------------------------


class TestLogging:
    def test_execute_query_logs_row_count(self, data_dir, caplog):
        con = get_connection(data_dir)
        with caplog.at_level("DEBUG", logger="cellarbrain.query"):
            execute_query(con, "SELECT * FROM wines")
        assert "execute_query rows=" in caplog.text

    def test_validate_sql_logs_accepted(self, caplog):
        with caplog.at_level("DEBUG", logger="cellarbrain.query"):
            validate_sql("SELECT 1")
        assert "SQL validated" in caplog.text

    def test_agent_connection_logs_open(self, data_dir, caplog):
        with caplog.at_level("DEBUG", logger="cellarbrain.query"):
            get_agent_connection(data_dir)
        assert "DuckDB agent connection opened" in caplog.text


class TestGetPriceHistory:
    def test_returns_table(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop A", "price": Decimal("45.00"),
            "currency": "CHF", "price_chf": Decimal("45.00"),
            "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = get_price_history(price_dir, 90_001)
        assert "Shop A" in result or "No price history" in result

    def test_invalid_tracked_wine(self, price_dir):
        with pytest.raises(TrackedWineNotFoundError):
            get_price_history(price_dir, 999)


class TestPriceViewsRegistered:
    def test_price_views_present_when_data_exists(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop", "price": Decimal("50.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)

        con = get_agent_connection(price_dir)
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables ORDER BY table_name"
        ).fetchall()
        names = [r[0] for r in rows]
        assert "price_observations" in names
        assert "latest_prices" in names
        assert "price_history" in names

    def test_price_views_absent_without_data(self, price_dir):
        con = get_agent_connection(price_dir)
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables ORDER BY table_name"
        ).fetchall()
        names = [r[0] for r in rows]
        assert "price_observations" not in names


class TestWishlistAlerts:
    def test_new_listing_alert(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "New Shop", "price": Decimal("50.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = wishlist_alerts(price_dir)
        assert "New Listing" in result
        assert "New Shop" in result
        assert "High Priority" in result

    def test_price_drop_alert(self, price_dir):
        old_obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop A", "price": Decimal("100.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 3, 1, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, old_obs)
        new_obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop A", "price": Decimal("80.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 4, 5, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, new_obs)
        result = wishlist_alerts(price_dir)
        assert "Price Drop" in result
        assert "20%" in result

    def test_no_price_drop_below_threshold(self, price_dir):
        old_obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop A", "price": Decimal("100.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 3, 1, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, old_obs)
        new_obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop A", "price": Decimal("95.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 4, 5, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, new_obs)
        result = wishlist_alerts(price_dir)
        assert "Price Drop" not in result

    def test_back_in_stock_alert(self, price_dir):
        out_obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop B", "price": Decimal("60.00"),
            "currency": "CHF", "in_stock": False,
            "observed_at": datetime(2026, 3, 1, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, out_obs)
        in_obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Shop B", "price": Decimal("60.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 4, 5, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, in_obs)
        result = wishlist_alerts(price_dir)
        assert "Back in Stock" in result
        assert "Shop B" in result

    def test_en_primeur_alert(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001, "vintage": 2023, "bottle_size_ml": 750,
            "retailer_name": "Futures Shop", "price": Decimal("35.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2026, 4, 7, 12, 0),
            "observation_source": "agent",
            "notes": "En primeur release",
        }
        log_price(price_dir, obs)
        result = wishlist_alerts(price_dir)
        assert "En Primeur" in result
        assert "Futures Shop" in result

    def test_no_alerts_empty(self, price_dir):
        result = wishlist_alerts(price_dir)
        assert "No price observations" in result

    def test_alert_window_filtering(self, price_dir):
        obs = {
            "tracked_wine_id": 90_001, "vintage": 2020, "bottle_size_ml": 750,
            "retailer_name": "Old Shop", "price": Decimal("50.00"),
            "currency": "CHF", "in_stock": True,
            "observed_at": datetime(2025, 1, 1, 12, 0),
            "observation_source": "agent",
        }
        log_price(price_dir, obs)
        result = wishlist_alerts(price_dir, days=30)
        assert "Old Shop" not in result
