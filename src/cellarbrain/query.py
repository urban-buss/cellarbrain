"""DuckDB query layer over Parquet entity files.

Provides read-only SQL access, pre-computed statistics, and text search
across the wine cellar data stored in Parquet files.

Two-layer view architecture:
- **Full views** (``wines_full``, ``bottles_full``): all columns with
  backward-compatible aliases.  Used by ``cellar_stats``,
  ``cellar_churn``, and ``find_wine`` internally.
- **Slim views** (``wines``, ``bottles``): curated ~18/~16 columns with
  clean naming.  Default surface for agent-submitted SQL.

Two connection modes:
- ``get_agent_connection`` — agent-visible views only (wines, bottles,
  wines_full, bottles_full, convenience views, tracked/wishlist/price).
  Bottle convenience views return slim columns (17 cols).
  Relational tables are not registered.
- ``get_connection`` — extends the agent connection with ``etl_run``,
  ``change_log``, and internal full-column bottle convenience views
  (``_bottles_stored_full``, ``_bottles_on_order_full``) used by
  ``cellar_stats`` and ``cellar_churn``.
"""

from __future__ import annotations

import difflib
import logging
import pathlib
import re
from dataclasses import dataclass, field

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

from .flat import (
    BOTTLES_FULL_VIEW_SQL,
    BOTTLES_VIEW_SQL,
    LATEST_PRICES_VIEW_SQL,
    PRICE_HISTORY_VIEW_SQL,
    PRICE_OBSERVATIONS_VIEW_SQL,
    TRACKED_WINES_VIEW_SQL,
    WINES_FULL_VIEW_SQL,
    WINES_VIEW_SQL,
    WINES_WISHLIST_VIEW_SQL,
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class QueryError(Exception):
    """SQL validation or execution error."""


class DataStaleError(Exception):
    """Parquet files missing or corrupted."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENTITY_TABLES = [
    "wine", "tracked_wine", "bottle", "winery", "appellation", "grape",
    "wine_grape", "tasting", "pro_rating", "cellar", "provider",
    "etl_run", "change_log",
]

# Slim bottle columns — matches BOTTLES_VIEW_SQL in flat.py.
_SLIM_BOTTLE_COLS = (
    "bottle_id, wine_id, wine_name, vintage, winery_name, category, "
    "country, region, primary_grape, drinking_status, price_tier, price, "
    "status, cellar_name, shelf, output_date, output_type"
)

# Agent-facing convenience views — slim columns only.
_CONVENIENCE_VIEWS = {
    "wines_stored": "SELECT * FROM wines WHERE bottles_stored > 0",
    "bottles_stored": (
        f"SELECT {_SLIM_BOTTLE_COLS} FROM bottles_full "
        "WHERE status = 'stored' AND NOT is_in_transit"
    ),
    "bottles_consumed": (
        f"SELECT {_SLIM_BOTTLE_COLS} FROM bottles_full "
        "WHERE status != 'stored'"
    ),
    "bottles_on_order": (
        f"SELECT {_SLIM_BOTTLE_COLS} FROM bottles_full "
        "WHERE status = 'stored' AND is_in_transit"
    ),
    "wines_on_order": "SELECT * FROM wines WHERE bottles_on_order > 0",
    "wines_drinking_now": (
        "SELECT * FROM wines "
        "WHERE drinking_status IN ('optimal', 'drinkable') "
        "AND bottles_stored > 0"
    ),
    "wines_wishlist": "SELECT * FROM _wines_wishlist",
}

# Internal full-column bottle convenience views — used by cellar_stats
# grouped dimensions (cellar, provider, on_order) that need volume_ml,
# price, and provider_name columns not in the slim surface.
_INTERNAL_CONVENIENCE_VIEWS = {
    "_bottles_stored_full": (
        "SELECT * FROM bottles_full "
        "WHERE status = 'stored' AND NOT is_in_transit"
    ),
    "_bottles_on_order_full": (
        "SELECT * FROM bottles_full "
        "WHERE status = 'stored' AND is_in_transit"
    ),
}

_VALID_GROUP_BY = {
    "country", "region", "category", "vintage", "winery",
    "grape", "cellar", "provider", "status", "on_order",
}

_GROUP_LABEL: dict[str, str] = {
    "category": "category",
    "country": "country",
    "region": "region",
    "vintage": "vintage",
    "winery": "winery",
    "grape": "grape",
    "cellar": "cellar",
    "provider": "provider",
    "status": "status",
    "on_order": "cellar",
}

_VALID_SORT_BY = {"bottles", "value", "wines", "volume"}

_VALID_PERIODS = {"month", "year"}
_PERIOD_ALIASES: dict[str, str] = {"monthly": "month", "yearly": "year"}

_MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

_DISALLOWED_PATTERN = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b",
    re.IGNORECASE,
)

_ALLOWED_PATTERN = re.compile(
    r"^\s*(SELECT|WITH)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_chf(amount: float) -> str:
    """Format a CHF value with Swiss apostrophe thousands separator."""
    return f"{amount:,.2f}".replace(",", "'")


def _fmt_litres(litres: float) -> str:
    """Format volume in litres with Swiss apostrophe thousands separator."""
    return f"{litres:,.2f}".replace(",", "'")


def _to_md(df: "pd.DataFrame") -> str:
    """Render a DataFrame as a Markdown table with NULLs as empty cells."""
    return df.astype(object).where(df.notna(), "").to_markdown(index=False)


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _parquet_path(data_dir: pathlib.Path, table: str) -> str:
    """Return a DuckDB-friendly forward-slash path for a Parquet file."""
    return str(data_dir / f"{table}.parquet").replace("\\", "/")


# Tables whose Parquet files are required for the wines/bottles views.
_VIEW_REQUIRED_TABLES = [
    "wine", "bottle", "winery", "appellation",
    "cellar", "provider", "tasting", "pro_rating",
]

# Tables used by the tracked-wines view (optional — may not exist yet).
_TRACKED_VIEW_TABLES = ["tracked_wine", "wine", "bottle", "winery", "appellation"]


def _wines_full_view_sql(data_dir: pathlib.Path) -> str:
    """Return the ``wines_full`` view SQL with inline ``read_parquet()`` paths."""
    sql = WINES_FULL_VIEW_SQL
    for table in ("wine", "winery", "appellation", "bottle", "tasting", "pro_rating"):
        path = _parquet_path(data_dir, table)
        sql = sql.replace(f"FROM {table}\n", f"FROM read_parquet('{path}')\n")
        sql = sql.replace(f"FROM {table} ", f"FROM read_parquet('{path}') ")
        sql = sql.replace(f"JOIN {table} ", f"JOIN read_parquet('{path}') ")
    return sql


def _tracked_wines_view_sql(data_dir: pathlib.Path) -> str:
    """Return the ``tracked_wines`` view SQL with inline ``read_parquet()`` paths."""
    sql = TRACKED_WINES_VIEW_SQL
    for table in _TRACKED_VIEW_TABLES:
        path = _parquet_path(data_dir, table)
        sql = sql.replace(f"FROM {table}\n", f"FROM read_parquet('{path}')\n")
        sql = sql.replace(f"FROM {table} ", f"FROM read_parquet('{path}') ")
        sql = sql.replace(f"JOIN {table} ", f"JOIN read_parquet('{path}') ")
    return sql


def _wines_wishlist_view_sql(data_dir: pathlib.Path) -> str:
    """Return the ``wines_wishlist`` view SQL with inline ``read_parquet()`` paths."""
    sql = WINES_WISHLIST_VIEW_SQL
    for table in ("wine", "winery", "appellation", "bottle"):
        path = _parquet_path(data_dir, table)
        sql = sql.replace(f"FROM {table}\n", f"FROM read_parquet('{path}')\n")
        sql = sql.replace(f"FROM {table} ", f"FROM read_parquet('{path}') ")
        sql = sql.replace(f"JOIN {table} ", f"JOIN read_parquet('{path}') ")
    return sql


def _bottles_full_view_sql(data_dir: pathlib.Path) -> str:
    """Return the ``bottles_full`` view SQL with inline ``read_parquet()`` paths."""
    sql = BOTTLES_FULL_VIEW_SQL
    for table in ("bottle", "wine", "winery", "appellation", "cellar", "provider"):
        path = _parquet_path(data_dir, table)
        sql = sql.replace(f"FROM {table} ", f"FROM read_parquet('{path}') ")
        sql = sql.replace(f"JOIN {table} ", f"JOIN read_parquet('{path}') ")
    return sql


# Tables used by the price observation views (optional — may not exist yet).
_PRICE_VIEW_TABLES = ["tracked_wine", "winery"]


def _parquet_glob_path(data_dir: pathlib.Path, entity_name: str) -> str:
    """Return a DuckDB-friendly glob path for year-partitioned Parquet files."""
    return str(data_dir / f"{entity_name}_*.parquet").replace("\\", "/")


def _has_partitioned_files(data_dir: pathlib.Path, entity_name: str) -> bool:
    """Check whether any year-partitioned Parquet files exist."""
    return bool(list(data_dir.glob(f"{entity_name}_*.parquet")))


def _substitute_price_tables(sql: str, data_dir: pathlib.Path) -> str:
    """Substitute table names with read_parquet() paths in price view SQL."""
    # price_observation uses glob for year-partitioned files
    glob_path = _parquet_glob_path(data_dir, "price_observation")
    sql = sql.replace(
        "FROM price_observation po", f"FROM read_parquet('{glob_path}') po"
    )
    sql = sql.replace(
        "FROM price_observation ", f"FROM read_parquet('{glob_path}') "
    )
    # Regular tables
    for table in _PRICE_VIEW_TABLES:
        path = _parquet_path(data_dir, table)
        sql = sql.replace(f"FROM {table}\n", f"FROM read_parquet('{path}')\n")
        sql = sql.replace(f"FROM {table} ", f"FROM read_parquet('{path}') ")
        sql = sql.replace(f"JOIN {table} ", f"JOIN read_parquet('{path}') ")
    return sql


def get_agent_connection(
    data_dir: str | pathlib.Path,
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with agent views — for agent-submitted SQL.

    Two-layer view structure:

    - **Full views** (``wines_full``, ``bottles_full``): all columns,
      backward-compatible aliases.  Used by ``cellar_stats``,
      ``cellar_churn``, and agents needing technical detail.
    - **Slim views** (``wines``, ``bottles``): curated ~18/~16 columns,
      clean naming.  Used by default agent queries and convenience views.

    Convenience views (``wines_stored``, ``bottles_stored``, etc.) all
    return slim columns.  Bottle convenience views select the 17 slim
    bottle columns from ``bottles_full`` (needed for ``is_in_transit``
    filtering) rather than ``SELECT *``.

    Relational tables are **not** registered — agents cannot query them.
    """
    d = pathlib.Path(data_dir)
    missing = [
        t for t in _VIEW_REQUIRED_TABLES if not (d / f"{t}.parquet").exists()
    ]
    if missing:
        raise DataStaleError(
            f"Missing Parquet files: {', '.join(missing)}. "
            "Run 'cellarbrain etl' first to generate the data."
        )

    logger.debug("DuckDB agent connection opened — data_dir=%s", d)
    con = duckdb.connect(":memory:")

    # Full views first (other views reference these)
    con.execute(f"CREATE VIEW wines_full AS {_wines_full_view_sql(d)}")
    con.execute(f"CREATE VIEW bottles_full AS {_bottles_full_view_sql(d)}")

    # Slim views on top
    con.execute(f"CREATE VIEW wines AS {WINES_VIEW_SQL}")
    con.execute(f"CREATE VIEW bottles AS {BOTTLES_VIEW_SQL}")

    con.execute(f"CREATE VIEW _wines_wishlist AS {_wines_wishlist_view_sql(d)}")

    # Tracked-wines view (optional — tracked_wine.parquet may not exist yet)
    if (d / "tracked_wine.parquet").exists():
        con.execute(
            f"CREATE VIEW tracked_wines AS {_tracked_wines_view_sql(d)}"
        )

    # Price views (optional — price_observation_*.parquet may not exist yet)
    if _has_partitioned_files(d, "price_observation"):
        con.execute(
            f"CREATE VIEW price_observations AS "
            f"{_substitute_price_tables(PRICE_OBSERVATIONS_VIEW_SQL, d)}"
        )
        con.execute(
            f"CREATE VIEW latest_prices AS "
            f"{_substitute_price_tables(LATEST_PRICES_VIEW_SQL, d)}"
        )
        con.execute(
            f"CREATE VIEW price_history AS "
            f"{_substitute_price_tables(PRICE_HISTORY_VIEW_SQL, d)}"
        )

    for name, sql in _CONVENIENCE_VIEWS.items():
        con.execute(f"CREATE VIEW {name} AS {sql}")

    # PostgreSQL-compatible alias so raw SQL can use unaccent() as well as
    # the DuckDB-native strip_accents().
    con.execute("CREATE MACRO unaccent(s) AS strip_accents(s)")

    # Normalise typographic (curly) quotes to ASCII equivalents so that
    # user searches with straight apostrophes match data containing
    # RIGHT SINGLE QUOTATION MARK (U+2019) and friends.
    con.execute(
        "CREATE MACRO normalize_quotes(s) AS "
        "REPLACE(REPLACE(REPLACE(REPLACE("
        "s, chr(8216), chr(39)), chr(8217), chr(39)), chr(8220), chr(34)), chr(8221), chr(34))"
    )

    return con


def get_connection(data_dir: str | pathlib.Path) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with full internal access.

    Extends the agent connection with ``etl_run``, ``change_log``, and
    internal full-column bottle convenience views used by ``cellar_stats``
    and ``cellar_churn``.
    """
    d = pathlib.Path(data_dir)
    con = get_agent_connection(d)
    for table in ("etl_run", "change_log"):
        path = _parquet_path(d, table)
        if (d / f"{table}.parquet").exists():
            con.execute(
                f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')"
            )
    for name, sql in _INTERNAL_CONVENIENCE_VIEWS.items():
        con.execute(f"CREATE VIEW {name} AS {sql}")
    return con


# ---------------------------------------------------------------------------
# SQL validation
# ---------------------------------------------------------------------------

def validate_sql(sql: str) -> None:
    """Check that *sql* is a read-only SELECT statement.

    Raises QueryError if the SQL is empty, contains DDL/DML, does not
    start with SELECT or WITH, or contains multiple statements.
    """
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        raise QueryError("Empty SQL statement.")
    if ";" in stripped:
        raise QueryError("Multiple SQL statements are not allowed.")
    if _DISALLOWED_PATTERN.match(stripped):
        raise QueryError(
            f"Only SELECT queries are allowed. "
            f"Got: {stripped.split()[0].upper()}"
        )
    if not _ALLOWED_PATTERN.match(stripped):
        raise QueryError(
            "SQL must start with SELECT or WITH. "
            f"Got: {stripped[:40]!r}"
        )
    logger.debug("SQL validated: %s", stripped[:200])


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def _suggest_columns(
    con: duckdb.DuckDBPyConnection,
    bad_column: str,
) -> list[str]:
    """Return up to 3 similar column names from agent-visible views."""
    rows = con.execute(
        "SELECT DISTINCT column_name FROM information_schema.columns "
        "WHERE table_schema = 'main'"
    ).fetchall()
    all_columns = sorted({r[0] for r in rows})
    return difflib.get_close_matches(bad_column, all_columns, n=3, cutoff=0.4)


def execute_query(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    row_limit: int = 200,
) -> str:
    """Execute a read-only SQL query and return a Markdown table.

    Raises QueryError on validation failure or DuckDB execution error.
    """
    validate_sql(sql)
    try:
        df = con.execute(sql).fetchdf()
    except duckdb.Error as exc:
        msg = str(exc)
        match = re.search(
            r'column\s+["\']?(\w+)["\']?.*not found', msg, re.IGNORECASE,
        )
        if match:
            suggestions = _suggest_columns(con, match.group(1))
            if suggestions:
                msg += f"\n\nDid you mean: {', '.join(suggestions)}?"
        raise QueryError(msg) from exc

    total = len(df)
    logger.debug("execute_query rows=%d sql=%s", total, sql.strip()[:200])
    if total == 0:
        return "*No results.*"

    if total > row_limit:
        result = _to_md(df.head(row_limit))
        result += f"\n\n*({total} rows total, showing first {row_limit})*"
        return result

    return _to_md(df)


# ---------------------------------------------------------------------------
# Cellar statistics
# ---------------------------------------------------------------------------

def cellar_stats(
    con: duckdb.DuckDBPyConnection,
    group_by: str | None = None,
    currency: str = "CHF",
    limit: int = 20,
    sort_by: str | None = None,
) -> str:
    """Return formatted cellar statistics as Markdown.

    Raises ValueError for an invalid group_by dimension, sort_by column,
    or negative limit.
    """
    if group_by is not None:
        group_by = group_by.strip().lower()
        if group_by not in _VALID_GROUP_BY:
            raise ValueError(
                f"Invalid group_by: {group_by!r}. "
                f"Must be one of: {', '.join(sorted(_VALID_GROUP_BY))}"
            )
    if sort_by is not None:
        sort_by = sort_by.strip().lower()
        if sort_by not in _VALID_SORT_BY:
            raise ValueError(
                f"Invalid sort_by: {sort_by!r}. "
                f"Must be one of: {', '.join(sorted(_VALID_SORT_BY))}"
            )
    if limit < 0:
        raise ValueError(f"limit must be >= 0, got {limit}")

    if group_by is not None:
        return _grouped_stats(
            con, group_by, currency=currency, limit=limit, sort_by=sort_by,
        )
    return _overall_stats(con, currency=currency)


def _overall_stats(con: duckdb.DuckDBPyConnection, *, currency: str = "CHF") -> str:
    """Build the default cellar summary."""
    lines: list[str] = ["## Cellar Summary\n"]

    # Single query for the 4×3 summary table
    row = con.execute("""
        SELECT
            count(*) FILTER (WHERE bottles_stored > 0)                                    AS wines_cellar,
            count(*) FILTER (WHERE bottles_on_order > 0)                                  AS wines_on_order,
            count(*) FILTER (WHERE bottles_stored > 0 OR bottles_on_order > 0)            AS wines_total,
            COALESCE(sum(bottles_stored), 0)                                              AS bottles_cellar,
            COALESCE(sum(bottles_on_order), 0)                                            AS bottles_on_order,
            COALESCE(sum(bottles_stored), 0) + COALESCE(sum(bottles_on_order), 0)         AS bottles_total,
            COALESCE(sum(cellar_value), 0)                                                AS value_cellar,
            COALESCE(sum(on_order_value), 0)                                              AS value_on_order,
            COALESCE(sum(cellar_value), 0) + COALESCE(sum(on_order_value), 0)             AS value_total,
            COALESCE(sum(volume_ml * bottles_stored), 0) / 1000.0                         AS volume_cellar,
            COALESCE(sum(volume_ml * bottles_on_order), 0) / 1000.0                       AS volume_on_order,
            COALESCE(sum(volume_ml * (bottles_stored + bottles_on_order)), 0) / 1000.0    AS volume_total
        FROM wines_full
    """).fetchone()
    (
        wines_cellar, wines_on_order, wines_total,
        bottles_cellar, bottles_on_order, bottles_total,
        value_cellar, value_on_order, value_total,
        volume_cellar, volume_on_order, volume_total,
    ) = row

    # Currency label
    curr = currency

    # Render summary table
    lines.append(f"|             | in cellar | on order | total |")
    lines.append(f"|:------------|----------:|---------:|------:|")
    lines.append(f"| wines       | {wines_cellar} | {wines_on_order} | {wines_total} |")
    lines.append(f"| bottles     | {bottles_cellar} | {bottles_on_order} | {bottles_total} |")
    lines.append(
        f"| value ({curr}) | {_fmt_chf(value_cellar)} "
        f"| {_fmt_chf(value_on_order)} | {_fmt_chf(value_total)} |"
    )
    lines.append(
        f"| volume (L)  | {_fmt_litres(volume_cellar)} "
        f"| {_fmt_litres(volume_on_order)} | {_fmt_litres(volume_total)} |"
    )
    lines.append("")

    # --- In Cellar breakdowns ---
    lines.append("### In Cellar\n")

    # By category
    cat_df = con.execute("""
        SELECT category, count(*) AS wines,
               CAST(sum(bottles_stored) AS BIGINT) AS bottles,
               CAST(COALESCE(sum(cellar_value), 0) AS DOUBLE) AS value,
               COALESCE(sum(volume_ml * bottles_stored), 0) / 1000.0 AS volume
        FROM wines_full
        WHERE bottles_stored > 0
        GROUP BY category ORDER BY bottles DESC
    """).fetchdf()
    if not cat_df.empty:
        cat_df[f"value ({curr})"] = cat_df["value"].apply(_fmt_chf)
        cat_df["volume (L)"] = cat_df["volume"].apply(_fmt_litres)
        cat_df = cat_df.drop(columns=["value", "volume"])
        lines.append("#### By Category")
        lines.append(_to_md(cat_df))
        lines.append("")

    # Drinking window status
    dw_df = con.execute("""
        SELECT drinking_status AS status, count(*) AS wines,
               CAST(sum(bottles_stored) AS BIGINT) AS bottles,
               CAST(COALESCE(sum(cellar_value), 0) AS DOUBLE) AS value,
               COALESCE(sum(volume_ml * bottles_stored), 0) / 1000.0 AS volume
        FROM wines_full
        WHERE bottles_stored > 0
        GROUP BY drinking_status ORDER BY wines DESC
    """).fetchdf()
    if not dw_df.empty:
        dw_df[f"value ({curr})"] = dw_df["value"].apply(_fmt_chf)
        dw_df["volume (L)"] = dw_df["volume"].apply(_fmt_litres)
        dw_df = dw_df.drop(columns=["value", "volume"])
        lines.append("#### Drinking Window Status")
        lines.append(_to_md(dw_df))
        lines.append("")

    # Data freshness
    try:
        etl_row = con.execute("""
            SELECT run_id, started_at, run_type
            FROM etl_run ORDER BY run_id DESC LIMIT 1
        """).fetchone()
        if etl_row:
            run_id, started, run_type = etl_row
            lines.append("### Data Freshness")
            lines.append(f"- **Last ETL run:** {started} (run #{run_id}, {run_type})")
    except duckdb.Error:
        pass

    return "\n".join(lines)


def _grouped_stats(
    con: duckdb.DuckDBPyConnection,
    group_by: str,
    *,
    currency: str = "CHF",
    limit: int = 20,
    sort_by: str | None = None,
) -> str:
    """Return stats grouped by a single dimension."""
    # Wine-based dimensions query wines_full (in-cellar only)
    _WINES_TEMPLATE = """
        SELECT {col} AS group_val, count(*) AS wines,
               CAST(sum(bottles_stored) AS BIGINT) AS bottles,
               CAST(COALESCE(sum(cellar_value), 0) AS DOUBLE) AS value,
               COALESCE(sum(volume_ml * bottles_stored), 0) / 1000.0 AS volume
        FROM wines_full
        WHERE bottles_stored > 0
        {where}
        GROUP BY {col} ORDER BY {order}
    """
    # Bottle-based dimensions query at the bottle level
    _BOTTLES_TEMPLATE = """
        SELECT {col} AS group_val,
               COUNT(DISTINCT wine_id) AS wines,
               count(*) AS bottles,
               CAST(COALESCE(sum(price), 0) AS DOUBLE) AS value,
               COALESCE(sum(volume_ml), 0) / 1000.0 AS volume
        FROM {view}
        GROUP BY {col} ORDER BY bottles DESC
    """

    sql_map: dict[str, str] = {
        "country": _WINES_TEMPLATE.format(
            col="country", where="", order="bottles DESC",
        ),
        "region": _WINES_TEMPLATE.format(
            col="region", where="", order="bottles DESC",
        ),
        "category": _WINES_TEMPLATE.format(
            col="category", where="", order="bottles DESC",
        ),
        "vintage": _WINES_TEMPLATE.format(
            col="vintage", where="", order="vintage DESC",
        ),
        "winery": _WINES_TEMPLATE.format(
            col="winery_name", where="", order="bottles DESC",
        ),
        "grape": _WINES_TEMPLATE.format(
            col="primary_grape",
            where="",
            order="wines DESC",
        ),
        "cellar": _BOTTLES_TEMPLATE.format(
            col="cellar_name", view="_bottles_stored_full",
        ),
        "provider": _BOTTLES_TEMPLATE.format(
            col="provider_name", view="_bottles_stored_full",
        ),
        "status": _BOTTLES_TEMPLATE.format(
            col="status", view="bottles_full",
        ),
        "on_order": _BOTTLES_TEMPLATE.format(
            col="cellar_name", view="_bottles_on_order_full",
        ),
    }

    df = con.execute(sql_map[group_by]).fetchdf()
    if df.empty:
        return f"*No data for group_by={group_by}.*"

    # Label NULL group values so they don't render as blank rows
    df["group_val"] = df["group_val"].fillna("(not set)").replace("", "(not set)")

    # Sort
    if sort_by is not None:
        df = df.sort_values(sort_by, ascending=False, ignore_index=True)
    elif group_by == "vintage":
        df = df.sort_values("group_val", ascending=False, na_position="last", ignore_index=True)
    else:
        df = df.sort_values("bottles", ascending=False, ignore_index=True)

    # Limit + rollup
    total_rows = len(df)
    if limit and total_rows > limit:
        top = df.head(limit)
        rest = df.iloc[limit:]
        other_row = pd.DataFrame([{
            "group_val": "(other)",
            "wines": rest["wines"].sum(),
            "bottles": rest["bottles"].sum(),
            "value": rest["value"].sum(),
            "volume": rest["volume"].sum(),
        }])
        df = pd.concat([top, other_row], ignore_index=True)
        footnote = (
            f"\n\n*Showing top {limit} of {total_rows}; "
            f"remaining {total_rows - limit} groups rolled into '(other)'.*"
        )
    else:
        footnote = ""

    # Percentage column
    total_bottles = df["bottles"].sum()
    if total_bottles > 0:
        df.insert(
            df.columns.get_loc("bottles") + 1,
            "bottles_%",
            (df["bottles"] / total_bottles * 100).round(1),
        )

    # Currency label
    curr = currency

    # Format value/volume columns
    df[f"value ({curr})"] = df["value"].apply(_fmt_chf)
    df["volume (L)"] = df["volume"].apply(_fmt_litres)
    df = df.drop(columns=["value", "volume"])

    # Dynamic heading
    if group_by == "status":
        header = f"## Cellar Statistics — by {group_by.title()}\n"
    elif group_by == "on_order":
        header = f"## On Order Statistics — by {group_by.title()}\n"
    else:
        header = f"## In Cellar Statistics — by {group_by.title()}\n"

    # Rename group_val to the dimension name for display
    df = df.rename(columns={"group_val": _GROUP_LABEL[group_by]})

    return header + "\n" + _to_md(df) + footnote


# ---------------------------------------------------------------------------
# Cellar churn (roll-forward analysis)
# ---------------------------------------------------------------------------

def cellar_churn(
    con: duckdb.DuckDBPyConnection,
    period: str | None = None,
    year: int | None = None,
    month: int | None = None,
    currency: str = "CHF",
) -> str:
    """Return cellar churn (roll-forward) analysis as Markdown.

    Raises ValueError for invalid parameters.
    """
    import datetime as _dt

    today = _dt.date.today()

    if period is not None:
        period = _PERIOD_ALIASES.get(period, period)
    if period is not None and period not in _VALID_PERIODS:
        raise ValueError(
            f"Invalid period: {period!r}. Must be one of: month, year."
        )
    if month is not None and not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month}. Must be 1–12.")

    if period == "year":
        # Year-by-year since first purchase
        first_year = con.execute(
            "SELECT min(extract('year' FROM purchase_date))::INT FROM bottles_full"
        ).fetchone()[0]
        if first_year is None:
            return "*No bottles in the cellar.*"
        range_start = _dt.date(first_year, 1, 1)
        range_end = _dt.date(today.year, 1, 1)
        return _multi_period_churn(
            con, range_start, range_end,
            interval="1 YEAR", date_fmt="%Y", title="Year-by-Year",
            currency=currency,
        )

    if period == "month":
        y = year if year is not None else today.year
        range_start = _dt.date(y, 1, 1)
        # End at the 1st of the current month (inclusive of current month)
        if y == today.year:
            range_end = _dt.date(y, today.month, 1)
        else:
            range_end = _dt.date(y, 12, 1)
        return _multi_period_churn(
            con, range_start, range_end,
            interval="1 MONTH", date_fmt="%Y-%m",
            title=f"{y} Month-by-Month",
            currency=currency,
        )

    # Single-period mode
    if year is not None and month is not None:
        start = _dt.date(year, month, 1)
        if month == 12:
            end = _dt.date(year + 1, 1, 1)
        else:
            end = _dt.date(year, month + 1, 1)
        title = f"{_MONTH_NAMES[month]} {year}"
    elif year is not None:
        start = _dt.date(year, 1, 1)
        end = _dt.date(year + 1, 1, 1)
        title = str(year)
    else:
        start = _dt.date(today.year, today.month, 1)
        if today.month == 12:
            end = _dt.date(today.year + 1, 1, 1)
        else:
            end = _dt.date(today.year, today.month + 1, 1)
        title = f"{_MONTH_NAMES[today.month]} {today.year}"

    return _single_period_churn(con, start, end, title, currency=currency)


def _single_period_churn(
    con: duckdb.DuckDBPyConnection,
    start: object,
    end: object,
    title: str,
    *,
    currency: str = "CHF",
) -> str:
    """Render a single-period churn roll-forward."""
    row = con.execute(f"""
        SELECT
            count(DISTINCT wine_id) FILTER (
                WHERE purchase_date < '{start}'
                AND (output_date IS NULL OR output_date >= '{start}')
            ) AS beg_wines,
            count(DISTINCT wine_id) FILTER (
                WHERE purchase_date >= '{start}' AND purchase_date < '{end}'
            ) AS pur_wines,
            count(DISTINCT wine_id) FILTER (
                WHERE output_date >= '{start}' AND output_date < '{end}'
            ) AS con_wines,
            count(DISTINCT wine_id) FILTER (
                WHERE purchase_date < '{end}'
                AND (output_date IS NULL OR output_date >= '{end}')
            ) AS end_wines,
            count(*) FILTER (
                WHERE purchase_date < '{start}'
                AND (output_date IS NULL OR output_date >= '{start}')
            ) AS beg_bottles,
            count(*) FILTER (
                WHERE purchase_date >= '{start}' AND purchase_date < '{end}'
            ) AS pur_bottles,
            count(*) FILTER (
                WHERE output_date >= '{start}' AND output_date < '{end}'
            ) AS con_bottles,
            count(*) FILTER (
                WHERE purchase_date < '{end}'
                AND (output_date IS NULL OR output_date >= '{end}')
            ) AS end_bottles,
            COALESCE(sum(price) FILTER (
                WHERE purchase_date < '{start}'
                AND (output_date IS NULL OR output_date >= '{start}')
            ), 0) AS beg_val,
            COALESCE(sum(price) FILTER (
                WHERE purchase_date >= '{start}' AND purchase_date < '{end}'
            ), 0) AS pur_val,
            COALESCE(sum(price) FILTER (
                WHERE output_date >= '{start}' AND output_date < '{end}'
            ), 0) AS con_val,
            COALESCE(sum(price) FILTER (
                WHERE purchase_date < '{end}'
                AND (output_date IS NULL OR output_date >= '{end}')
            ), 0) AS end_val,
            COALESCE(sum(volume_ml) FILTER (
                WHERE purchase_date < '{start}'
                AND (output_date IS NULL OR output_date >= '{start}')
            ), 0) / 1000.0 AS beg_vol,
            COALESCE(sum(volume_ml) FILTER (
                WHERE purchase_date >= '{start}' AND purchase_date < '{end}'
            ), 0) / 1000.0 AS pur_vol,
            COALESCE(sum(volume_ml) FILTER (
                WHERE output_date >= '{start}' AND output_date < '{end}'
            ), 0) / 1000.0 AS con_vol,
            COALESCE(sum(volume_ml) FILTER (
                WHERE purchase_date < '{end}'
                AND (output_date IS NULL OR output_date >= '{end}')
            ), 0) / 1000.0 AS end_vol
        FROM bottles_full
    """).fetchone()

    (
        beg_w, pur_w, con_w, end_w,
        beg_b, pur_b, con_b, end_b,
        beg_v, pur_v, con_v, end_v,
        beg_l, pur_l, con_l, end_l,
    ) = row

    # Currency label
    curr = currency

    lines: list[str] = [f"## Cellar Churn \u2014 {title}\n"]
    lines.append(f"|               |   wines |   bottles | value ({curr})   | volume (L)   |")
    lines.append(f"|:--------------|--------:|----------:|:--------------|:-------------|")
    lines.append(
        f"| Beginning     | {beg_w:7d} | {beg_b:9d} "
        f"| {_fmt_chf(beg_v):13s} | {_fmt_litres(beg_l):12s} |"
    )
    lines.append(
        f"| + Purchased   | {pur_w:7d} | {pur_b:9d} "
        f"| {_fmt_chf(pur_v):13s} | {_fmt_litres(pur_l):12s} |"
    )
    lines.append(
        f"| \u2212 Consumed    | {con_w:7d} | {con_b:9d} "
        f"| {_fmt_chf(con_v):13s} | {_fmt_litres(con_l):12s} |"
    )
    lines.append(
        f"| **Ending**    | **{end_w}** | **{end_b}** "
        f"| **{_fmt_chf(end_v)}** | **{_fmt_litres(end_l)}** |"
    )
    lines.append("")

    # Ending inventory split
    inv_df = con.execute(f"""
        SELECT
            CASE WHEN is_in_transit THEN 'In Transit' ELSE 'In Cellar' END AS segment,
            count(DISTINCT wine_id) AS wines,
            count(*) AS bottles,
            CAST(COALESCE(sum(price), 0) AS DOUBLE) AS value,
            COALESCE(sum(volume_ml), 0) / 1000.0 AS volume
        FROM bottles_full
        WHERE status = 'stored'
          AND purchase_date < '{end}'
          AND (output_date IS NULL OR output_date >= '{end}')
        GROUP BY is_in_transit
        ORDER BY is_in_transit
    """).fetchdf()

    if not inv_df.empty:
        inv_df[f"value ({curr})"] = inv_df["value"].apply(_fmt_chf)
        inv_df["volume (L)"] = inv_df["volume"].apply(_fmt_litres)
        inv_df = inv_df.drop(columns=["value", "volume"])
        lines.append("### Ending Inventory\n")
        lines.append(_to_md(inv_df))

    return "\n".join(lines)


def _multi_period_churn(
    con: duckdb.DuckDBPyConnection,
    range_start: object,
    range_end: object,
    interval: str,
    date_fmt: str,
    title: str,
    *,
    currency: str = "CHF",
) -> str:
    """Render a multi-period churn table (bottles + value)."""
    sql = f"""
        WITH series AS (
            SELECT unnest(generate_series(
                '{range_start}'::DATE, '{range_end}'::DATE, INTERVAL {interval}
            )) AS period_start
        ),
        periods AS (
            SELECT period_start::DATE AS period_start,
                   (period_start + INTERVAL {interval})::DATE AS period_end,
                   strftime(period_start, '{date_fmt}') AS label
            FROM series
        )
        SELECT
            p.label,
            count(*) FILTER (
                WHERE b.purchase_date < p.period_start
                AND (b.output_date IS NULL OR b.output_date >= p.period_start)
            ) AS beg_bottles,
            count(*) FILTER (
                WHERE b.purchase_date >= p.period_start AND b.purchase_date < p.period_end
            ) AS pur_bottles,
            count(*) FILTER (
                WHERE b.output_date >= p.period_start AND b.output_date < p.period_end
            ) AS con_bottles,
            count(*) FILTER (
                WHERE b.purchase_date < p.period_end
                AND (b.output_date IS NULL OR b.output_date >= p.period_end)
            ) AS end_bottles,
            COALESCE(sum(b.price) FILTER (
                WHERE b.purchase_date < p.period_start
                AND (b.output_date IS NULL OR b.output_date >= p.period_start)
            ), 0) AS beg_val,
            COALESCE(sum(b.price) FILTER (
                WHERE b.purchase_date >= p.period_start AND b.purchase_date < p.period_end
            ), 0) AS pur_val,
            COALESCE(sum(b.price) FILTER (
                WHERE b.output_date >= p.period_start AND b.output_date < p.period_end
            ), 0) AS con_val,
            COALESCE(sum(b.price) FILTER (
                WHERE b.purchase_date < p.period_end
                AND (b.output_date IS NULL OR b.output_date >= p.period_end)
            ), 0) AS end_val
        FROM periods p
        CROSS JOIN bottles_full b
        GROUP BY p.label, p.period_start
        ORDER BY p.period_start
    """
    df = con.execute(sql).fetchdf()
    if df.empty:
        return f"*No churn data for {title}.*"

    df["net_bottles"] = df["pur_bottles"] - df["con_bottles"]
    df["net_val"] = df["pur_val"] - df["con_val"]

    # Currency label
    curr = currency

    # Determine period column name
    col_name = "year" if date_fmt == "%Y" else "month"

    # --- Bottles table ---
    def _signed_int(n: int) -> str:
        return f"+{n}" if n >= 0 else str(n)

    bt = df[["label", "beg_bottles", "pur_bottles", "con_bottles", "end_bottles", "net_bottles"]].copy()
    bt.columns = [col_name, "beg. bottles", "+ purchased", "\u2212 consumed", "end. bottles", "net"]
    bt["net"] = bt["net"].apply(_signed_int)

    lines: list[str] = [f"## Cellar Churn \u2014 {title}\n"]
    lines.append(_to_md(bt))
    lines.append("")

    # --- Value table ---
    def _signed_chf(v: float) -> str:
        s = _fmt_chf(abs(v))
        return f"+{s}" if v >= 0 else f"\u2212{s}"

    vt = df[["label"]].copy()
    vt[col_name] = vt["label"]
    vt[f"beg. value"] = df["beg_val"].apply(_fmt_chf)
    vt[f"+ purchased"] = df["pur_val"].apply(_fmt_chf)
    vt[f"\u2212 consumed"] = df["con_val"].apply(_fmt_chf)
    vt[f"end. value"] = df["end_val"].apply(_fmt_chf)
    vt["net"] = df["net_val"].apply(_signed_chf)
    vt = vt.drop(columns=["label"])

    lines.append(f"### Value ({curr})\n")
    lines.append(_to_md(vt))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Text search — intent detection
# ---------------------------------------------------------------------------

@dataclass
class IntentResult:
    """Accumulated WHERE clauses and ORDER BY from intent detection."""

    where_clauses: list[str] = field(default_factory=list)
    where_params: list = field(default_factory=list)
    order_by: str | None = None
    consumed_indices: set[int] = field(default_factory=set)


# Each pattern is (token_tuple, handler).  Handlers receive the matched
# tokens and current param_idx, and return (where_sql, params, order_by).
# Patterns are checked longest-first so multi-word patterns take priority.

def _intent_drinking_ready(
    matched: list[str], idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"drinking_status IN (${idx}, ${idx + 1})",
        ["optimal", "drinkable"],
        None,
    )


def _intent_drinking_exact(
    status: str,
) -> callable:
    def _handler(
        matched: list[str], idx: int,
    ) -> tuple[str, list, str | None]:
        return f"drinking_status = ${idx}", [status], None
    return _handler


def _intent_drinking_drinkable(
    matched: list[str], idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"drinking_status IN (${idx}, ${idx + 1})",
        ["optimal", "drinkable"],
        None,
    )


def _intent_price_under(
    matched: list[str], idx: int,
) -> tuple[str, list, str | None]:
    # Last matched token is the numeric value.
    price = float(matched[-1])
    return f"price <= ${idx} AND price IS NOT NULL", [price], None


def _intent_price_budget(
    matched: list[str], idx: int,
) -> tuple[str, list, str | None]:
    return f"price_tier = ${idx}", ["budget"], None


def _intent_price_premium(
    matched: list[str], idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"price_tier IN (${idx}, ${idx + 1})",
        ["premium", "fine"],
        "price DESC",
    )


def _intent_top_rated(
    matched: list[str], idx: int,
) -> tuple[str, list, str | None]:
    return (
        "best_pro_score IS NOT NULL",
        [],
        "best_pro_score DESC, bottles_stored DESC, vintage DESC",
    )


def _intent_low_stock(
    matched: list[str], idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"bottles_stored BETWEEN ${idx} AND ${idx + 1}",
        [1, 2],
        "bottles_stored ASC, vintage DESC",
    )


def _matches_numeric(token: str) -> bool:
    """Return True if token looks like a positive number (not a vintage)."""
    try:
        val = float(token)
        return val > 0 and val < 1000  # exclude vintage-like numbers
    except ValueError:
        return False


# Patterns ordered longest-first for greedy matching.  Each entry:
#   (tuple_of_lowercase_tokens, handler, numeric_tail)
# numeric_tail=True means the last token slot matches any number.
_INTENT_PATTERNS: list[tuple[tuple[str, ...], callable, bool]] = [
    # --- drinking status (multi-word first) ---
    (("ready", "to", "drink"), _intent_drinking_ready, False),
    (("ready", "drink"), _intent_drinking_ready, False),
    (("drink", "soon"), _intent_drinking_exact("past_optimal"), False),
    (("too", "young"), _intent_drinking_exact("too_young"), False),
    (("past", "optimal"), _intent_drinking_exact("past_optimal"), False),
    # --- price (multi-word first) ---
    (("cheaper", "than"), _intent_price_under, True),
    (("under",), _intent_price_under, True),
    (("below",), _intent_price_under, True),
    # --- single-word intents ---
    (("optimal",), _intent_drinking_exact("optimal"), False),
    (("drinkable",), _intent_drinking_drinkable, False),
    (("budget",), _intent_price_budget, False),
    (("premium",), _intent_price_premium, False),
    (("highest", "rated"), _intent_top_rated, False),
    (("top", "rated"), _intent_top_rated, False),
    (("best", "rated"), _intent_top_rated, False),
    (("running", "low"), _intent_low_stock, False),
    (("last", "bottle"), _intent_low_stock, False),
    (("low", "stock"), _intent_low_stock, False),
]


def _extract_intents(tokens: list[str], param_idx: int) -> IntentResult:
    """Scan tokens for intent patterns and return accumulated SQL fragments.

    Modifies nothing; returns an ``IntentResult`` with WHERE clauses,
    parameters, optional ORDER BY override, and indices of consumed tokens.
    ``param_idx`` is the next free DuckDB parameter index ($N).
    """
    result = IntentResult()
    lower_tokens = [t.lower() for t in tokens]
    n = len(lower_tokens)
    consumed: set[int] = set()

    for pattern, handler, numeric_tail in _INTENT_PATTERNS:
        plen = len(pattern) + (1 if numeric_tail else 0)
        for i in range(n - plen + 1):
            # Skip if any position already consumed.
            if any(j in consumed for j in range(i, i + plen)):
                continue
            # Check fixed tokens.
            fixed_match = all(
                lower_tokens[i + k] == pattern[k]
                for k in range(len(pattern))
            )
            if not fixed_match:
                continue
            # Check numeric tail if needed.
            if numeric_tail:
                tail_idx = i + len(pattern)
                if not _matches_numeric(lower_tokens[tail_idx]):
                    continue
                matched = [tokens[j] for j in range(i, i + plen)]
            else:
                matched = [tokens[j] for j in range(i, i + plen)]

            where_sql, params, order_by = handler(matched, param_idx)
            if where_sql:
                result.where_clauses.append(where_sql)
                result.where_params.extend(params)
                param_idx += len(params)
            if order_by and result.order_by is None:
                result.order_by = order_by
            for j in range(i, i + plen):
                consumed.add(j)
            break  # Re-scan from the start for the next pattern.

    result.consumed_indices = consumed
    return result


# ---------------------------------------------------------------------------
# Text search — concept expansion, token normalisation, and ILIKE engine
# ---------------------------------------------------------------------------

# Concept expansions: abstract wine-style keywords → concrete search terms.
# When a token matches a key here, the ILIKE engine generates an OR across
# the original token AND all expansion terms (so the category column still
# matches "sparkling" while wine_name might match "Prosecco").
_CONCEPT_EXPANSIONS: dict[str, list[str]] = {
    # -- Wine style concepts -----------------------------------------------
    "sparkling": [
        "Prosecco", "Champagne", "Crémant", "Cava", "Spumante",
        "Sekt", "Franciacorta",
    ],
    "dessert": [
        "Sauternes", "Tokaji", "Moscato", "Eiswein", "Passito",
        "Vin Santo", "Recioto", "Beerenauslese",
        "Trockenbeerenauslese", "late harvest",
    ],
    "fortified": ["Port", "Sherry", "Madeira", "Marsala", "Vermouth"],
    "sweet": [
        "Sauternes", "Tokaji", "Moscato", "Eiswein", "Passito",
        "Vin Santo", "Beerenauslese", "Trockenbeerenauslese",
        "late harvest", "Recioto",
    ],
    "natural": ["natural wine", "vin nature", "sans soufre"],
    # -- Grape synonym clusters (bidirectional cross-references) -----------
    "shiraz": ["Syrah"],
    "syrah": ["Shiraz"],
    "garnacha": ["Grenache"],
    "grenache": ["Garnacha"],
    "monastrell": ["Mourvèdre"],
    "mourvèdre": ["Monastrell"],
    "primitivo": ["Zinfandel"],
    "zinfandel": ["Primitivo"],
    "tempranillo": ["Tinta del Pais"],
    "carignan": ["Cariñena"],
    "cariñena": ["Carignan"],
    # -- Sub-variety name cross-references ---------------------------------
    "grigio": ["Gris"],
    "gris": ["Grigio"],
}

# System concepts: keywords that translate into WHERE clauses rather than
# ILIKE text search.  Values are ``(sql_fragment, params_list)``.
_SYSTEM_CONCEPTS: dict[str, tuple[str, list]] = {
    "tracked": ("tracked_wine_id IS NOT NULL", []),
    "favorite": ("is_favorite = true", []),
    "favourite": ("is_favorite = true", []),
    "favourites": ("is_favorite = true", []),
    "favorites": ("is_favorite = true", []),
    "wishlist": ("is_wishlist = true", []),
}

_SEARCH_COLS = [
    "wine_name", "winery_name",
    "country", "region", "subregion",
    "classification", "category",
    "primary_grape",
    "subcategory", "sweetness", "effervescence", "specialty",
]


def _normalise_query_tokens(
    tokens: list[str],
    synonyms: dict[str, str],
) -> list[str]:
    """Expand/drop query tokens using the synonym dict.

    For each token the lowercase form is looked up in *synonyms*:
    - Found with non-empty value → replace (multi-word values are split).
    - Found with empty value → drop (stopword).
    - Not found → keep the original token.

    If all tokens are stopwords the originals are returned unchanged so
    that the caller never receives an empty list.
    """
    result: list[str] = []
    for token in tokens:
        replacement = synonyms.get(token.lower())
        if replacement is None:
            result.append(token)
        elif replacement:
            result.extend(replacement.split())
        # else: empty string → stopword, drop token
    return result if result else tokens


def find_wine(
    con: duckdb.DuckDBPyConnection,
    query: str,
    limit: int = 10,
    fuzzy: bool = False,
    synonyms: dict[str, str] | None = None,
) -> str:
    """Search wines by ILIKE matching across multiple columns.

    Tokenises multi-word queries: each token must match at least one
    searchable column (OR across columns), all tokens must match (AND
    across tokens).  Uses ``strip_accents()`` for accent-insensitive
    matching, ``normalize_quotes()`` for typographic-quote-insensitive
    matching, and DuckDB parameter binding to prevent SQL injection.

    After synonym normalisation, an intent detection layer recognises
    attribute-based patterns (drinking status, price, rating, stock)
    and injects WHERE / ORDER BY clauses.  Consumed tokens are removed
    from the ILIKE engine so they don't cause false positives.

    When strict AND returns zero results and there are two or more
    ILIKE text conditions, a soft-AND fallback query fires: it requires
    at least one ILIKE condition to match and ranks results by the
    number of matching conditions.  Intent and system-concept filters
    remain mandatory.  This recovers results for near-miss queries
    where one token matches nothing.
    """
    tokens = query.split()
    if not tokens:
        return "*Empty search query.*"
    if synonyms:
        tokens = _normalise_query_tokens(tokens, synonyms)

    intent_conds: list[str] = []
    ilike_conds: list[str] = []
    params: list = []
    param_idx = 1

    # --- Intent detection (attribute-based queries) ---
    intent = _extract_intents(tokens, param_idx)
    if intent.where_clauses:
        intent_conds.extend(intent.where_clauses)
        params.extend(intent.where_params)
        param_idx += len(intent.where_params)

    # Filter out tokens consumed by intent patterns.
    remaining_tokens = [
        t for i, t in enumerate(tokens) if i not in intent.consumed_indices
    ]

    # --- Concept expansion + ILIKE engine for remaining text tokens ---
    for token in remaining_tokens:
        lower = token.lower()

        # System concepts → WHERE clause (like mini-intents).
        sys = _SYSTEM_CONCEPTS.get(lower)
        if sys is not None:
            sql_frag, sys_params = sys
            intent_conds.append(sql_frag)
            params.extend(sys_params)
            param_idx += len(sys_params)
            continue

        # Concept expansion → OR across the original token + all expansions.
        expansions = _CONCEPT_EXPANSIONS.get(lower)
        search_terms = [token] if expansions is None else [token, *expansions]

        term_groups: list[str] = []
        for term in search_terms:
            col_checks = [
                f"normalize_quotes(strip_accents({col})) ILIKE "
                f"normalize_quotes(strip_accents(${param_idx}))"
                for col in _SEARCH_COLS
            ]
            col_checks.append(f"CAST(vintage AS VARCHAR) = ${param_idx + 1}")
            term_groups.append(f"({' OR '.join(col_checks)})")
            params.append(f"%{term}%")
            params.append(term)
            param_idx += 2

        ilike_conds.append(f"({' OR '.join(term_groups)})")

    # Build WHERE — intent-only queries (all tokens consumed) still work.
    conditions = intent_conds + ilike_conds
    if not conditions:
        return "*Empty search query.*"
    where_clause = " AND ".join(conditions)
    limit_param = f"${param_idx}"
    params.append(limit)

    order_by = intent.order_by or "bottles_stored DESC, vintage DESC"

    sql = f"""
        SELECT wine_id, winery_name, wine_name, vintage, category,
               country, region, primary_grape, bottles_stored, drinking_status,
               tracked_wine_id
        FROM wines_full
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT {limit_param}
    """
    try:
        df = con.execute(sql, params).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc

    if df.empty:
        # --- Soft AND fallback: relax ILIKE conditions when ≥2 exist ---
        if len(ilike_conds) >= 2:
            df = _find_wine_soft_and(
                con, intent_conds, ilike_conds, params, param_idx, order_by,
            )
            if not df.empty:
                header = f"*Partial match for '{query}' (not all terms matched):*\n\n"
                return header + _to_md(df)
        if fuzzy and remaining_tokens:
            expanded_query = " ".join(remaining_tokens)
            return _find_wine_fuzzy(con, expanded_query, limit)
        return f"*No wines found matching '{query}'.*"
    return _to_md(df)


def _find_wine_soft_and(
    con: duckdb.DuckDBPyConnection,
    intent_conds: list[str],
    ilike_conds: list[str],
    params: list,
    limit_param_idx: int,
    order_by: str,
) -> pd.DataFrame:
    """Scored fallback: require at least one ILIKE match, rank by count.

    Intent/system-concept conditions remain mandatory. Only ILIKE text
    conditions are relaxed (OR instead of AND). Results are ranked by
    how many ILIKE conditions matched (descending), then the original
    sort order.
    """
    any_match = f"({' OR '.join(ilike_conds)})"
    score_expr = " + ".join(
        f"CASE WHEN {cond} THEN 1 ELSE 0 END" for cond in ilike_conds
    )

    where_parts = [any_match, *intent_conds]
    where_clause = " AND ".join(where_parts)
    limit_param = f"${limit_param_idx}"

    sql = f"""
        SELECT wine_id, winery_name, wine_name, vintage, category,
               country, region, primary_grape, bottles_stored, drinking_status,
               tracked_wine_id
        FROM wines_full
        WHERE {where_clause}
        ORDER BY ({score_expr}) DESC, {order_by}
        LIMIT {limit_param}
    """
    try:
        return con.execute(sql, params).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc


def _find_wine_fuzzy(
    con: duckdb.DuckDBPyConnection,
    query: str,
    limit: int,
    threshold: float = 0.85,
) -> str:
    """Fuzzy fallback — find close matches using Jaro-Winkler similarity."""
    sql = """
        WITH scored AS (
            SELECT wine_id, winery_name, wine_name, vintage, category,
                   country, region, primary_grape, bottles_stored, drinking_status,
                   tracked_wine_id,
                   GREATEST(
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(wine_name))), normalize_quotes(strip_accents(LOWER($1)))),
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(winery_name))), normalize_quotes(strip_accents(LOWER($1)))),
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(COALESCE(region, '')))), normalize_quotes(strip_accents(LOWER($1)))),
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(COALESCE(primary_grape, '')))), normalize_quotes(strip_accents(LOWER($1))))
                   ) AS similarity
            FROM wines_full
        )
        SELECT wine_id, winery_name, wine_name, vintage, category,
               country, region, primary_grape, bottles_stored, drinking_status,
               tracked_wine_id,
               ROUND(similarity, 2) AS match_score
        FROM scored
        WHERE similarity >= $2
        ORDER BY similarity DESC, bottles_stored DESC
        LIMIT $3
    """
    try:
        df = con.execute(sql, [query, threshold, limit]).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc

    if df.empty:
        return f"*No wines found matching '{query}' (including fuzzy).*"
    header = f"*Fuzzy matches for '{query}':*\n\n"
    return header + _to_md(df)


# ---------------------------------------------------------------------------
# Price tracking
# ---------------------------------------------------------------------------

def log_price(
    data_dir: str | pathlib.Path,
    observation: dict,
    settings: None = None,
) -> str:
    """Record a price observation for a tracked wine.

    Validates required fields, auto-converts to CHF, deduplicates by
    ``(tracked_wine_id, vintage, bottle_size_ml, retailer_name, date)``,
    and writes to year-partitioned Parquet.

    Returns a confirmation string.
    """
    from .computed import convert_to_default_currency
    from .dossier_ops import TrackedWineNotFoundError
    from .settings import Settings, load_settings
    from . import writer

    d = pathlib.Path(data_dir)

    # Validate required fields
    required = [
        "tracked_wine_id", "bottle_size_ml", "retailer_name",
        "price", "currency", "in_stock", "observed_at", "observation_source",
    ]
    missing = [f for f in required if observation.get(f) is None]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    # Validate tracked_wine exists
    tw_rows = writer.read_parquet_rows("tracked_wine", d)
    tw_ids = {r["tracked_wine_id"] for r in tw_rows}
    twid = observation["tracked_wine_id"]
    if twid not in tw_ids:
        raise TrackedWineNotFoundError(
            f"Tracked wine {twid} not found in tracked_wine.parquet"
        )

    # Auto-convert price_chf
    from decimal import Decimal
    price = observation["price"]
    if not isinstance(price, Decimal):
        price = Decimal(str(price))
    currency = observation["currency"]

    price_chf = observation.get("price_chf")
    if price_chf is None:
        s = load_settings() if settings is None else settings
        price_chf = convert_to_default_currency(
            price, currency, s.currency.default, s.currency.rates,
        )

    # Read existing observations to get max ID and handle dedup
    existing = writer.read_partitioned_parquet_rows("price_observation", d)
    max_id = max((r["observation_id"] for r in existing), default=0)

    # Dedup key: (tracked_wine_id, vintage, bottle_size_ml, retailer_name, date)
    obs_at = observation["observed_at"]
    obs_date = obs_at.date() if hasattr(obs_at, "date") else obs_at
    vintage = observation.get("vintage")
    bottle_size_ml = observation["bottle_size_ml"]
    retailer_name = observation["retailer_name"]

    dedup_key = (twid, vintage, bottle_size_ml, retailer_name, obs_date)

    def _row_key(r: dict) -> tuple:
        rat = r["observed_at"]
        rd = rat.date() if hasattr(rat, "date") else rat
        return (r["tracked_wine_id"], r.get("vintage"), r["bottle_size_ml"],
                r["retailer_name"], rd)

    # Filter out any existing row with the same dedup key
    filtered = [r for r in existing if _row_key(r) != dedup_key]
    replaced = len(existing) - len(filtered)

    # Build the new row
    new_row = {
        "observation_id": max_id + 1 if not replaced else next(
            r["observation_id"] for r in existing if _row_key(r) == dedup_key
        ),
        "tracked_wine_id": twid,
        "vintage": vintage,
        "bottle_size_ml": bottle_size_ml,
        "retailer_name": retailer_name,
        "retailer_url": observation.get("retailer_url"),
        "price": price,
        "currency": currency,
        "price_chf": price_chf,
        "in_stock": observation["in_stock"],
        "observed_at": obs_at,
        "observation_source": observation["observation_source"],
        "notes": observation.get("notes"),
    }

    all_rows = filtered + [new_row]
    writer.write_partitioned_parquet("price_observation", all_rows, d)

    action = "Updated" if replaced else "Recorded"
    return (
        f"{action} price observation #{new_row['observation_id']}: "
        f"{currency} {price} for tracked wine #{twid} at {retailer_name}"
    )


def get_tracked_wine_prices(
    data_dir: str | pathlib.Path,
    tracked_wine_id: int,
    vintage: int | None = None,
) -> str:
    """Return latest prices for a tracked wine as a Markdown table."""
    from .dossier_ops import TrackedWineNotFoundError
    from . import writer

    d = pathlib.Path(data_dir)

    # Validate tracked_wine exists
    tw_rows = writer.read_parquet_rows("tracked_wine", d)
    tw_ids = {r["tracked_wine_id"] for r in tw_rows}
    if tracked_wine_id not in tw_ids:
        raise TrackedWineNotFoundError(
            f"Tracked wine {tracked_wine_id} not found"
        )

    if not _has_partitioned_files(d, "price_observation"):
        return "*No price observations recorded yet.*"

    con = get_agent_connection(d)
    sql = """
        SELECT retailer_name, vintage, bottle_size_ml,
               price, currency, price_chf, observed_at
        FROM latest_prices
        WHERE tracked_wine_id = $1
    """
    params = [tracked_wine_id]
    if vintage is not None:
        sql += " AND vintage = $2"
        params.append(vintage)
    sql += " ORDER BY price_chf ASC NULLS LAST"

    try:
        df = con.execute(sql, params).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc

    if df.empty:
        return f"*No current prices for tracked wine #{tracked_wine_id}.*"
    return _to_md(df)


def get_price_history(
    data_dir: str | pathlib.Path,
    tracked_wine_id: int,
    vintage: int | None = None,
    months: int = 12,
) -> str:
    """Return monthly price history for a tracked wine as a Markdown table."""
    from .dossier_ops import TrackedWineNotFoundError
    from . import writer

    d = pathlib.Path(data_dir)

    # Validate tracked_wine exists
    tw_rows = writer.read_parquet_rows("tracked_wine", d)
    tw_ids = {r["tracked_wine_id"] for r in tw_rows}
    if tracked_wine_id not in tw_ids:
        raise TrackedWineNotFoundError(
            f"Tracked wine {tracked_wine_id} not found"
        )

    if not _has_partitioned_files(d, "price_observation"):
        return "*No price observations recorded yet.*"

    con = get_agent_connection(d)
    months_int = int(months)
    sql = f"""
        SELECT retailer_name, month, min_price_chf, max_price_chf,
               avg_price_chf, observations
        FROM price_history
        WHERE tracked_wine_id = $1
          AND month >= CURRENT_DATE - INTERVAL '{months_int} months'
    """
    params: list = [tracked_wine_id]
    if vintage is not None:
        sql += " AND vintage = $2"
        params.append(vintage)
    sql += " ORDER BY month DESC, retailer_name"

    try:
        df = con.execute(sql, params).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc

    if df.empty:
        return f"*No price history for tracked wine #{tracked_wine_id}.*"
    return _to_md(df)


def wishlist_alerts(
    data_dir: str | pathlib.Path,
    settings: "Settings | None" = None,
    days: int | None = None,
) -> str:
    """Detect and return wishlist alerts grouped by priority.

    Checks for 6 alert types across 3 priority levels:
    - **High:** New Listing, Price Drop, Back in Stock
    - **Medium:** Best Price, En Primeur, Last Bottles
    """
    from .settings import Settings, load_settings

    d = pathlib.Path(data_dir)
    if not _has_partitioned_files(d, "price_observation"):
        return "*No price observations — nothing to alert on.*"

    if settings is None:
        settings = load_settings()
    window = int(days if days is not None else settings.wishlist.alert_window_days)
    drop_pct = float(settings.wishlist.price_drop_alert_pct)

    con = get_agent_connection(d)

    alerts: dict[str, list[str]] = {
        "high": [],
        "medium": [],
    }

    # 1. New Listing — first observation within window
    try:
        df = con.execute(f"""
            WITH first_seen AS (
                SELECT tracked_wine_id, vintage, retailer_name,
                       MIN(observed_at) AS first_observed
                FROM price_observations
                GROUP BY tracked_wine_id, vintage, retailer_name
                HAVING MIN(observed_at) >= CURRENT_TIMESTAMP - INTERVAL '{window} days'
            )
            SELECT fs.tracked_wine_id, fs.vintage, fs.retailer_name,
                   po.wine_name, po.winery_name, po.price, po.currency
            FROM first_seen fs
            JOIN price_observations po ON fs.tracked_wine_id = po.tracked_wine_id
                AND fs.vintage IS NOT DISTINCT FROM po.vintage
                AND fs.retailer_name = po.retailer_name
                AND po.observed_at = fs.first_observed
        """).fetchdf()
        for _, r in df.iterrows():
            vint = f" {int(r['vintage'])}" if r["vintage"] else ""
            alerts["high"].append(
                f"- **New Listing:** {r['winery_name']} {r['wine_name']}{vint}"
                f" at {r['retailer_name']} — {r['currency']} {r['price']}"
            )
    except duckdb.Error:
        pass

    # 2. Price Drop — latest vs previous, >= drop_pct
    try:
        df = con.execute(f"""
            WITH ranked AS (
                SELECT tracked_wine_id, vintage, bottle_size_ml, retailer_name,
                       wine_name, winery_name,
                       price_chf, observed_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY tracked_wine_id, vintage, bottle_size_ml, retailer_name
                           ORDER BY observed_at DESC
                       ) AS rn
                FROM price_observations
            )
            SELECT c.tracked_wine_id, c.vintage, c.retailer_name,
                   c.wine_name, c.winery_name,
                   p.price_chf AS old_price, c.price_chf AS new_price
            FROM ranked c
            JOIN ranked p ON c.tracked_wine_id = p.tracked_wine_id
                         AND c.vintage IS NOT DISTINCT FROM p.vintage
                         AND c.bottle_size_ml = p.bottle_size_ml
                         AND c.retailer_name = p.retailer_name
                         AND p.rn = 2
            WHERE c.rn = 1
              AND c.price_chf IS NOT NULL AND p.price_chf IS NOT NULL
              AND p.price_chf > 0
              AND c.observed_at >= CURRENT_TIMESTAMP - INTERVAL '{window} days'
              AND ((p.price_chf - c.price_chf) / p.price_chf) * 100 >= {drop_pct}
        """).fetchdf()
        for _, r in df.iterrows():
            vint = f" {int(r['vintage'])}" if r["vintage"] else ""
            pct = ((r["old_price"] - r["new_price"]) / r["old_price"]) * 100
            alerts["high"].append(
                f"- **Price Drop ({pct:.0f}%):** {r['winery_name']} {r['wine_name']}{vint}"
                f" at {r['retailer_name']} — CHF {r['new_price']}"
                f" (was CHF {r['old_price']})"
            )
    except duckdb.Error:
        pass

    # 3. Back in Stock — was out, now in
    try:
        df = con.execute(f"""
            WITH ranked AS (
                SELECT tracked_wine_id, vintage, bottle_size_ml, retailer_name,
                       wine_name, winery_name,
                       in_stock, price_chf, currency, observed_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY tracked_wine_id, vintage, bottle_size_ml, retailer_name
                           ORDER BY observed_at DESC
                       ) AS rn
                FROM price_observations
            )
            SELECT c.tracked_wine_id, c.vintage, c.retailer_name,
                   c.wine_name, c.winery_name,
                   c.price_chf, c.currency
            FROM ranked c
            JOIN ranked p ON c.tracked_wine_id = p.tracked_wine_id
                         AND c.vintage IS NOT DISTINCT FROM p.vintage
                         AND c.bottle_size_ml = p.bottle_size_ml
                         AND c.retailer_name = p.retailer_name
                         AND p.rn = 2
            WHERE c.rn = 1
              AND c.in_stock = true AND p.in_stock = false
              AND c.observed_at >= CURRENT_TIMESTAMP - INTERVAL '{window} days'
        """).fetchdf()
        for _, r in df.iterrows():
            vint = f" {int(r['vintage'])}" if r["vintage"] else ""
            price_str = f" — {r['currency']} {r['price_chf']}" if r["price_chf"] else ""
            alerts["high"].append(
                f"- **Back in Stock:** {r['winery_name']} {r['wine_name']}{vint}"
                f" at {r['retailer_name']}{price_str}"
            )
    except duckdb.Error:
        pass

    # 4. Best Price — cheapest across retailers (only when >1 retailer)
    try:
        df = con.execute(f"""
            WITH latest AS (
                SELECT tracked_wine_id, vintage, bottle_size_ml, retailer_name,
                       wine_name, winery_name,
                       price_chf, currency, in_stock,
                       ROW_NUMBER() OVER (
                           PARTITION BY tracked_wine_id, vintage, bottle_size_ml, retailer_name
                           ORDER BY observed_at DESC
                       ) AS rn
                FROM price_observations
                WHERE observed_at >= CURRENT_TIMESTAMP - INTERVAL '{window} days'
            ),
            in_stock_latest AS (
                SELECT * FROM latest WHERE rn = 1 AND in_stock = true AND price_chf IS NOT NULL
            ),
            best AS (
                SELECT tracked_wine_id, vintage, bottle_size_ml,
                       MIN(price_chf) AS min_price,
                       COUNT(DISTINCT retailer_name) AS retailer_count
                FROM in_stock_latest
                GROUP BY tracked_wine_id, vintage, bottle_size_ml
                HAVING COUNT(DISTINCT retailer_name) > 1
            )
            SELECT isl.tracked_wine_id, isl.vintage, isl.retailer_name,
                   isl.wine_name, isl.winery_name,
                   isl.price_chf, isl.currency
            FROM in_stock_latest isl
            JOIN best b ON isl.tracked_wine_id = b.tracked_wine_id
                       AND isl.vintage IS NOT DISTINCT FROM b.vintage
                       AND isl.bottle_size_ml = b.bottle_size_ml
                       AND isl.price_chf = b.min_price
        """).fetchdf()
        for _, r in df.iterrows():
            vint = f" {int(r['vintage'])}" if r["vintage"] else ""
            alerts["medium"].append(
                f"- **Best Price:** {r['winery_name']} {r['wine_name']}{vint}"
                f" at {r['retailer_name']} — {r['currency']} {r['price_chf']}"
            )
    except duckdb.Error:
        pass

    # 5. En Primeur — notes-based detection
    try:
        df = con.execute(f"""
            SELECT DISTINCT tracked_wine_id, vintage, retailer_name,
                   wine_name, winery_name, price, currency
            FROM price_observations
            WHERE notes ILIKE '%en primeur%'
              AND observed_at >= CURRENT_TIMESTAMP - INTERVAL '{window} days'
        """).fetchdf()
        for _, r in df.iterrows():
            vint = f" {int(r['vintage'])}" if r["vintage"] else ""
            alerts["medium"].append(
                f"- **En Primeur:** {r['winery_name']} {r['wine_name']}{vint}"
                f" at {r['retailer_name']} — {r['currency']} {r['price']}"
            )
    except duckdb.Error:
        pass

    # 6. Last Bottles — notes-based detection
    try:
        df = con.execute(f"""
            SELECT DISTINCT tracked_wine_id, vintage, retailer_name,
                   wine_name, winery_name, price, currency
            FROM price_observations
            WHERE (notes ILIKE '%last%bottles%' OR notes ILIKE '%limited%')
              AND observed_at >= CURRENT_TIMESTAMP - INTERVAL '{window} days'
        """).fetchdf()
        for _, r in df.iterrows():
            vint = f" {int(r['vintage'])}" if r["vintage"] else ""
            alerts["medium"].append(
                f"- **Last Bottles:** {r['winery_name']} {r['wine_name']}{vint}"
                f" at {r['retailer_name']} — {r['currency']} {r['price']}"
            )
    except duckdb.Error:
        pass

    # Format output
    if not alerts["high"] and not alerts["medium"]:
        return "*No wishlist alerts.*"

    lines = ["## Wishlist Alerts", ""]
    if alerts["high"]:
        lines.append("### \U0001f534 High Priority")
        lines.append("")
        lines.extend(alerts["high"])
        lines.append("")
    if alerts["medium"]:
        lines.append("### \U0001f7e1 Medium Priority")
        lines.append("")
        lines.extend(alerts["medium"])
        lines.append("")

    return "\n".join(lines)
