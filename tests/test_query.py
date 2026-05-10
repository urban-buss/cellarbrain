"""Tests for cellarbrain.query — DuckDB query layer over Parquet files."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import duckdb
import pytest

from cellarbrain import writer
from cellarbrain.query import (
    DataStaleError,
    QueryError,
    _fmt_chf,
    _fmt_litres,
    cellar_churn,
    cellar_stats,
    execute_query,
    get_agent_connection,
    get_connection,
    validate_sql,
)

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Shared fixture: minimal Parquet dataset
# ---------------------------------------------------------------------------
from dataset_factory import (
    _now,
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
    """Write a minimal but complete set of Parquet entity files."""
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
            winery_id=3,
            winery_name="Château d\u2019Aiguilhe",
            name=None,
            vintage=2019,
            appellation_id=1,
            alcohol_pct=14.0,
            drink_from=2024,
            drink_until=2032,
            drinking_status="optimal",
            age_years=6,
        ),
    ]
    bottles = [
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
        make_bottle(
            4,
            2,
            cellar_id=2,
            shelf="B1",
            provider_id=2,
            purchase_date=date(2025, 3, 10),
            original_purchase_price=Decimal("20.00"),
            purchase_price=Decimal("20.00"),
        ),
        # Bottles for churn testing (span 2024-2025)
        make_bottle(
            5,
            1,
            status="drunk",
            cellar_id=None,
            shelf=None,
            bottle_number=None,
            purchase_date=date(2024, 6, 15),
            output_date=date(2024, 9, 20),
            output_type="drunk",
        ),
        make_bottle(
            6,
            2,
            shelf="C1",
            provider_id=2,
            purchase_date=date(2024, 11, 1),
            original_purchase_price=Decimal("18.50"),
            original_purchase_currency="EUR",
            purchase_price=Decimal("17.21"),
        ),
        make_bottle(
            7,
            1,
            status="offered",
            cellar_id=None,
            shelf=None,
            bottle_number=None,
            purchase_date=date(2025, 2, 10),
            output_date=date(2025, 3, 15),
            output_type="offered",
        ),
        make_bottle(
            8,
            3,
            shelf="C1",
            purchase_date=date(2023, 9, 1),
            original_purchase_price=Decimal("30.00"),
            purchase_price=Decimal("30.00"),
        ),
    ]
    return write_dataset(
        tmp_path,
        {
            "winery": [
                make_winery(1, name="Château Test"),
                make_winery(2, name="Bodega Ejemplo"),
                make_winery(3, name="Château d\u2019Aiguilhe"),
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
                make_wine_grape(3, 1),
            ],
            "bottle": bottles,
            "cellar": [make_cellar(name="Main Cellar"), make_cellar(2, name="Transit", location_type="in_transit")],
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
        rows = con.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name").fetchall()
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
        cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM bottles_stored").fetchall()]
        assert len(cols) == 20
        assert cols == [
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
        ]

    def test_bottles_consumed_has_slim_columns(self, data_dir):
        con = get_agent_connection(data_dir)
        cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM bottles_consumed").fetchall()]
        assert len(cols) == 20

    def test_bottles_on_order_has_slim_columns(self, data_dir):
        con = get_agent_connection(data_dir)
        cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM bottles_on_order").fetchall()]
        assert len(cols) == 20

    def test_internal_full_bottle_views_on_connection(self, data_dir):
        """get_connection() adds internal full-column bottle convenience views."""
        con = get_connection(data_dir)
        # Internal views have all 40 columns from bottles_full
        stored_cols = [r[0] for r in con.execute("DESCRIBE SELECT * FROM _bottles_stored_full").fetchall()]
        assert len(stored_cols) == 41
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

    @pytest.mark.parametrize(
        "stmt",
        [
            "INSERT INTO wine VALUES (1)",
            "UPDATE wine SET name='x'",
            "DELETE FROM wine",
            "DROP TABLE wine",
            "CREATE TABLE evil (id INT)",
            "ALTER TABLE wine ADD col INT",
            "TRUNCATE TABLE wine",
        ],
    )
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
        result = execute_query(con, "SELECT unaccent('Château') AS stripped")
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
        result = execute_query(con, "SELECT NULL AS col_a, CAST(NULL AS DOUBLE) AS col_b")
        assert "nan" not in result.lower()
        assert "<NA>" not in result

    def test_normalize_quotes_macro_available(self, data_dir):
        con = get_connection(data_dir)
        result = execute_query(con, "SELECT normalize_quotes('d\u2019Aiguilhe') AS normalized")
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
        appellations = [
            {
                "appellation_id": 1,
                "country": "France",
                "region": "Bordeaux",
                "subregion": None,
                "classification": None,
                "etl_run_id": rid,
                "updated_at": now,
            }
        ]
        grapes = [{"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now}]
        wines = [
            {
                "wine_id": 1,
                "wine_slug": "solo-winery-solo-2020",
                "winery_id": 1,
                "name": "Solo",
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
                "alcohol_pct": 13.0,
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
                "full_name": "Solo Winery Solo 2020",
                "grape_type": "varietal",
                "primary_grape": "Merlot",
                "grape_summary": "Merlot",
                "_raw_grapes": None,
                "dossier_path": "cellar/0001-solo-winery-solo-2020.md",
                "drinking_status": "drinkable",
                "age_years": 5,
                "price_tier": "unknown",
                "bottle_format": "Standard",
                "price_per_750ml": None,
                "format_group_id": None,
                "food_tags": None,
                "is_deleted": False,
                "etl_run_id": rid,
                "updated_at": now,
            }
        ]
        wine_grapes = [
            {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now}
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
                "original_purchase_price": Decimal("30.00"),
                "original_purchase_currency": "CHF",
                "purchase_price": Decimal("30.00"),
                "purchase_currency": "CHF",
                "purchase_comment": None,
                "output_date": None,
                "output_type": None,
                "output_comment": None,
                "etl_run_id": rid,
                "updated_at": now,
            }
        ]
        cellars = [
            {
                "cellar_id": 1,
                "name": "Cellar",
                "location_type": "onsite",
                "sort_order": 1,
                "etl_run_id": rid,
                "updated_at": now,
            }
        ]
        providers = [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]
        etl_runs = [
            {
                "run_id": 1,
                "started_at": now,
                "finished_at": now,
                "run_type": "full",
                "wines_source_hash": "abc",
                "bottles_source_hash": "def",
                "bottles_gone_source_hash": None,
                "total_inserts": 1,
                "total_updates": 0,
                "total_deletes": 0,
                "wines_inserted": 1,
                "wines_updated": 0,
                "wines_deleted": 0,
                "wines_renamed": 0,
            }
        ]
        change_logs = [
            {
                "change_id": 1,
                "run_id": 1,
                "entity_type": "wine",
                "entity_id": 1,
                "change_type": "insert",
                "changed_fields": None,
            }
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
            ("tasting", []),
            ("pro_rating", []),
            ("etl_run", etl_runs),
            ("change_log", change_logs),
        ]:
            writer.write_parquet(name, rows, tmp_path)

        con = get_connection(tmp_path)
        result = cellar_stats(con)
        # value_cellar = 30.00, on_order = 0, so total must also be 30.00
        lines = result.split("\n")
        value_line = [l for l in lines if l.startswith("| value")][0]
        cells = [c.strip() for c in value_line.split("|") if c.strip()]
        # cells: ["value (CHF)", in_cellar, on_order, total]
        assert cells[1] == "30.00"  # in cellar
        assert cells[2] == "0.00"  # on order
        assert cells[3] == "30.00"  # total (was 0.00 before fix)

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
        cat_section = result[cat_idx : cat_idx + 500]
        assert "value (" in cat_section
        assert "volume (L)" in cat_section

    def test_drinking_window_has_all_columns(self, data_dir):
        con = get_connection(data_dir)
        result = cellar_stats(con)
        dw_idx = result.index("#### Drinking Window Status")
        dw_section = result[dw_idx : dw_idx + 500]
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

    @pytest.mark.parametrize(
        "dimension",
        [
            "country",
            "region",
            "category",
            "vintage",
            "winery",
            "grape",
            "cellar",
            "provider",
            "status",
            "on_order",
        ],
    )
    def test_grouped_stats_valid_dimensions(self, data_dir, dimension):
        con = get_connection(data_dir)
        result = cellar_stats(con, group_by=dimension)
        assert f"by {dimension.title()}" in result

    @pytest.mark.parametrize(
        "dimension",
        [
            "country",
            "region",
            "category",
            "vintage",
            "winery",
            "grape",
            "cellar",
            "provider",
            "status",
            "on_order",
        ],
    )
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
        lines = [l for l in result.split("\n") if "|" in l and "---" not in l and "vintage" not in l]
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

    @pytest.mark.parametrize(
        "dimension,expected_col",
        [
            ("country", "| country"),
            ("category", "| category"),
            ("grape", "| grape"),
            ("cellar", "| cellar"),
            ("status", "| status"),
        ],
    )
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
            (
                "wine",
                [
                    {
                        "wine_id": 1,
                        "wine_slug": "w-x-2020",
                        "winery_id": 1,
                        "name": "X",
                        "vintage": 2020,
                        "is_non_vintage": False,
                        "appellation_id": None,
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
                        "drink_from": 2023,
                        "drink_until": 2030,
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
                        "full_name": "W X 2020",
                        "grape_type": "varietal",
                        "primary_grape": "Merlot",
                        "grape_summary": "Merlot",
                        "_raw_grapes": None,
                        "dossier_path": "cellar/0001-w-x-2020.md",
                        "drinking_status": "drinkable",
                        "age_years": 5,
                        "price_tier": "unknown",
                        "bottle_format": "Standard",
                        "price_per_750ml": None,
                        "format_group_id": None,
                        "food_tags": None,
                        "is_deleted": False,
                        "etl_run_id": rid,
                        "updated_at": now,
                    }
                ],
            ),
            (
                "wine_grape",
                [
                    {
                        "wine_id": 1,
                        "grape_id": 1,
                        "percentage": 100.0,
                        "sort_order": 1,
                        "etl_run_id": rid,
                        "updated_at": now,
                    }
                ],
            ),
            (
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
                        "purchase_date": datetime(2023, 6, 1).date(),
                        "acquisition_type": "purchase",
                        "original_purchase_price": Decimal("20.00"),
                        "original_purchase_currency": "CHF",
                        "purchase_price": Decimal("20.00"),
                        "purchase_currency": "CHF",
                        "purchase_comment": None,
                        "output_date": None,
                        "output_type": None,
                        "output_comment": None,
                        "etl_run_id": rid,
                        "updated_at": now,
                    }
                ],
            ),
            (
                "cellar",
                [
                    {
                        "cellar_id": 1,
                        "name": "Cellar",
                        "location_type": "onsite",
                        "sort_order": 1,
                        "etl_run_id": rid,
                        "updated_at": now,
                    }
                ],
            ),
            ("provider", [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]),
            ("tasting", []),
            ("pro_rating", []),
            (
                "etl_run",
                [
                    {
                        "run_id": 1,
                        "started_at": now,
                        "finished_at": now,
                        "run_type": "full",
                        "wines_source_hash": "a",
                        "bottles_source_hash": "b",
                        "bottles_gone_source_hash": None,
                        "total_inserts": 1,
                        "total_updates": 0,
                        "total_deletes": 0,
                        "wines_inserted": 1,
                        "wines_updated": 0,
                        "wines_deleted": 0,
                        "wines_renamed": 0,
                    }
                ],
            ),
            (
                "change_log",
                [
                    {
                        "change_id": 1,
                        "run_id": 1,
                        "entity_type": "wine",
                        "entity_id": 1,
                        "change_type": "insert",
                        "changed_fields": None,
                    }
                ],
            ),
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
            (
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
                    }
                ],
            ),
            ("grape", []),
            (
                "wine",
                [
                    {
                        "wine_id": 1,
                        "wine_slug": "w-x-2020",
                        "winery_id": 1,
                        "name": "X",
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
                        "alcohol_pct": 13.0,
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
                        "full_name": "W X 2020",
                        "grape_type": "unknown",
                        "primary_grape": None,
                        "grape_summary": None,
                        "_raw_grapes": None,
                        "dossier_path": "cellar/0001-w-x-2020.md",
                        "drinking_status": "drinkable",
                        "age_years": 5,
                        "price_tier": "unknown",
                        "bottle_format": "Standard",
                        "price_per_750ml": None,
                        "format_group_id": None,
                        "food_tags": None,
                        "is_deleted": False,
                        "etl_run_id": rid,
                        "updated_at": now,
                    }
                ],
            ),
            ("wine_grape", []),
            (
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
                        "purchase_date": datetime(2023, 6, 1).date(),
                        "acquisition_type": "purchase",
                        "original_purchase_price": Decimal("20.00"),
                        "original_purchase_currency": "CHF",
                        "purchase_price": Decimal("20.00"),
                        "purchase_currency": "CHF",
                        "purchase_comment": None,
                        "output_date": None,
                        "output_type": None,
                        "output_comment": None,
                        "etl_run_id": rid,
                        "updated_at": now,
                    }
                ],
            ),
            (
                "cellar",
                [
                    {
                        "cellar_id": 1,
                        "name": "Cellar",
                        "location_type": "onsite",
                        "sort_order": 1,
                        "etl_run_id": rid,
                        "updated_at": now,
                    }
                ],
            ),
            ("provider", [{"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now}]),
            ("tasting", []),
            ("pro_rating", []),
            (
                "etl_run",
                [
                    {
                        "run_id": 1,
                        "started_at": now,
                        "finished_at": now,
                        "run_type": "full",
                        "wines_source_hash": "a",
                        "bottles_source_hash": "b",
                        "bottles_gone_source_hash": None,
                        "total_inserts": 1,
                        "total_updates": 0,
                        "total_deletes": 0,
                        "wines_inserted": 1,
                        "wines_updated": 0,
                        "wines_deleted": 0,
                        "wines_renamed": 0,
                    }
                ],
            ),
            (
                "change_log",
                [
                    {
                        "change_id": 1,
                        "run_id": 1,
                        "entity_type": "wine",
                        "entity_id": 1,
                        "change_type": "insert",
                        "changed_fields": None,
                    }
                ],
            ),
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
        data_rows = [l for l in lines if "Beginning" in l or "Purchased" in l or "Consumed" in l or "Ending" in l]
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
