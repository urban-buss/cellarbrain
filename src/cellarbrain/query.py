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

import duckdb
import pandas as pd

from ._query_base import DataStaleError, QueryError, _to_md
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENTITY_TABLES = [
    "wine",
    "tracked_wine",
    "bottle",
    "winery",
    "appellation",
    "grape",
    "wine_grape",
    "tasting",
    "pro_rating",
    "cellar",
    "provider",
    "etl_run",
    "change_log",
]

# Slim bottle columns — matches BOTTLES_VIEW_SQL in flat.py.
_SLIM_BOTTLE_COLS = (
    "bottle_id, wine_id, wine_name, vintage, winery_name, category, "
    "country, region, primary_grape, drinking_status, price_tier, price, "
    "price_per_750ml, volume_ml, bottle_format, "
    "status, cellar_name, shelf, output_date, output_type"
)

# Agent-facing convenience views — slim columns only.
_CONVENIENCE_VIEWS = {
    "wines_stored": "SELECT * FROM wines WHERE bottles_stored > 0",
    "bottles_stored": (f"SELECT {_SLIM_BOTTLE_COLS} FROM bottles_full WHERE status = 'stored' AND NOT is_in_transit"),
    "bottles_consumed": (f"SELECT {_SLIM_BOTTLE_COLS} FROM bottles_full WHERE status != 'stored'"),
    "bottles_on_order": (f"SELECT {_SLIM_BOTTLE_COLS} FROM bottles_full WHERE status = 'stored' AND is_in_transit"),
    "wines_on_order": "SELECT * FROM wines WHERE bottles_on_order > 0",
    "wines_drinking_now": (
        "SELECT * FROM wines WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0"
    ),
    "wines_wishlist": "SELECT * FROM _wines_wishlist",
    "format_groups": ("SELECT * FROM wines_full WHERE format_group_id IS NOT NULL ORDER BY format_group_id, volume_ml"),
}

# Internal full-column bottle convenience views — used by cellar_stats
# grouped dimensions (cellar, provider, on_order) that need volume_ml,
# price, and provider_name columns not in the slim surface.
_INTERNAL_CONVENIENCE_VIEWS = {
    "_bottles_stored_full": ("SELECT * FROM bottles_full WHERE status = 'stored' AND NOT is_in_transit"),
    "_bottles_on_order_full": ("SELECT * FROM bottles_full WHERE status = 'stored' AND is_in_transit"),
}

_VALID_GROUP_BY = {
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
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
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


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


def _parquet_path(data_dir: pathlib.Path, table: str) -> str:
    """Return a DuckDB-friendly forward-slash path for a Parquet file."""
    return str(data_dir / f"{table}.parquet").replace("\\", "/")


# Tables whose Parquet files are required for the wines/bottles views.
_VIEW_REQUIRED_TABLES = [
    "wine",
    "bottle",
    "winery",
    "appellation",
    "cellar",
    "provider",
    "tasting",
    "pro_rating",
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
    sql = sql.replace("FROM price_observation po", f"FROM read_parquet('{glob_path}') po")
    sql = sql.replace("FROM price_observation ", f"FROM read_parquet('{glob_path}') ")
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
    missing = [t for t in _VIEW_REQUIRED_TABLES if not (d / f"{t}.parquet").exists()]
    if missing:
        raise DataStaleError(
            f"Missing Parquet files: {', '.join(missing)}. Run 'cellarbrain etl' first to generate the data."
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
        con.execute(f"CREATE VIEW tracked_wines AS {_tracked_wines_view_sql(d)}")

    # Price views (optional — price_observation_*.parquet may not exist yet)
    if _has_partitioned_files(d, "price_observation"):
        con.execute(f"CREATE VIEW price_observations AS {_substitute_price_tables(PRICE_OBSERVATIONS_VIEW_SQL, d)}")
        con.execute(f"CREATE VIEW latest_prices AS {_substitute_price_tables(LATEST_PRICES_VIEW_SQL, d)}")
        con.execute(f"CREATE VIEW price_history AS {_substitute_price_tables(PRICE_HISTORY_VIEW_SQL, d)}")

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
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
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
        raise QueryError(f"Only SELECT queries are allowed. Got: {stripped.split()[0].upper()}")
    if not _ALLOWED_PATTERN.match(stripped):
        raise QueryError(f"SQL must start with SELECT or WITH. Got: {stripped[:40]!r}")
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
        "SELECT DISTINCT column_name FROM information_schema.columns WHERE table_schema = 'main'"
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
            r'column\s+["\']?(\w+)["\']?.*not found',
            msg,
            re.IGNORECASE,
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
            raise ValueError(f"Invalid group_by: {group_by!r}. Must be one of: {', '.join(sorted(_VALID_GROUP_BY))}")
    if sort_by is not None:
        sort_by = sort_by.strip().lower()
        if sort_by not in _VALID_SORT_BY:
            raise ValueError(f"Invalid sort_by: {sort_by!r}. Must be one of: {', '.join(sorted(_VALID_SORT_BY))}")
    if limit < 0:
        raise ValueError(f"limit must be >= 0, got {limit}")

    if group_by is not None:
        return _grouped_stats(
            con,
            group_by,
            currency=currency,
            limit=limit,
            sort_by=sort_by,
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
        wines_cellar,
        wines_on_order,
        wines_total,
        bottles_cellar,
        bottles_on_order,
        bottles_total,
        value_cellar,
        value_on_order,
        value_total,
        volume_cellar,
        volume_on_order,
        volume_total,
    ) = row

    # Currency label
    curr = currency

    # Render summary table
    lines.append("|             | in cellar | on order | total |")
    lines.append("|:------------|----------:|---------:|------:|")
    lines.append(f"| wines       | {wines_cellar} | {wines_on_order} | {wines_total} |")
    lines.append(f"| bottles     | {bottles_cellar} | {bottles_on_order} | {bottles_total} |")
    lines.append(
        f"| value ({curr}) | {_fmt_chf(value_cellar)} | {_fmt_chf(value_on_order)} | {_fmt_chf(value_total)} |"
    )
    lines.append(
        f"| volume (L)  | {_fmt_litres(volume_cellar)} | {_fmt_litres(volume_on_order)} | {_fmt_litres(volume_total)} |"
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
            col="country",
            where="",
            order="bottles DESC",
        ),
        "region": _WINES_TEMPLATE.format(
            col="region",
            where="",
            order="bottles DESC",
        ),
        "category": _WINES_TEMPLATE.format(
            col="category",
            where="",
            order="bottles DESC",
        ),
        "vintage": _WINES_TEMPLATE.format(
            col="vintage",
            where="",
            order="vintage DESC",
        ),
        "winery": _WINES_TEMPLATE.format(
            col="winery_name",
            where="",
            order="bottles DESC",
        ),
        "grape": _WINES_TEMPLATE.format(
            col="primary_grape",
            where="",
            order="wines DESC",
        ),
        "cellar": _BOTTLES_TEMPLATE.format(
            col="cellar_name",
            view="_bottles_stored_full",
        ),
        "provider": _BOTTLES_TEMPLATE.format(
            col="provider_name",
            view="_bottles_stored_full",
        ),
        "status": _BOTTLES_TEMPLATE.format(
            col="status",
            view="bottles_full",
        ),
        "on_order": _BOTTLES_TEMPLATE.format(
            col="cellar_name",
            view="_bottles_on_order_full",
        ),
    }

    df = con.execute(sql_map[group_by]).fetchdf()
    if df.empty:
        return f"*No data for group_by={group_by}.*"

    # Label NULL group values so they don't render as blank rows.
    # Cast to str first — numeric columns (e.g. Int16 vintage) can't fillna with str.
    # Then fillna catches remaining NaN; replace catches "<NA>"/"None"/"" edge cases.
    df["group_val"] = (
        df["group_val"]
        .astype(str)
        .fillna("(not set)")
        .replace({"<NA>": "(not set)", "None": "(not set)", "": "(not set)"})
    )

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
        other_row = pd.DataFrame(
            [
                {
                    "group_val": "(other)",
                    "wines": rest["wines"].sum(),
                    "bottles": rest["bottles"].sum(),
                    "value": rest["value"].sum(),
                    "volume": rest["volume"].sum(),
                }
            ]
        )
        df = pd.concat([top, other_row], ignore_index=True)
        footnote = (
            f"\n\n*Showing top {limit} of {total_rows}; remaining {total_rows - limit} groups rolled into '(other)'.*"
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
        raise ValueError(f"Invalid period: {period!r}. Must be one of: month, year.")
    if month is not None and not 1 <= month <= 12:
        raise ValueError(f"Invalid month: {month}. Must be 1–12.")

    if period == "year":
        # Year-by-year since first purchase
        first_year = con.execute("SELECT min(extract('year' FROM purchase_date))::INT FROM bottles_full").fetchone()[0]
        if first_year is None:
            return "*No bottles in the cellar.*"
        range_start = _dt.date(first_year, 1, 1)
        range_end = _dt.date(today.year, 1, 1)
        return _multi_period_churn(
            con,
            range_start,
            range_end,
            interval="1 YEAR",
            date_fmt="%Y",
            title="Year-by-Year",
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
            con,
            range_start,
            range_end,
            interval="1 MONTH",
            date_fmt="%Y-%m",
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
        beg_w,
        pur_w,
        con_w,
        end_w,
        beg_b,
        pur_b,
        con_b,
        end_b,
        beg_v,
        pur_v,
        con_v,
        end_v,
        beg_l,
        pur_l,
        con_l,
        end_l,
    ) = row

    # Currency label
    curr = currency

    lines: list[str] = [f"## Cellar Churn \u2014 {title}\n"]
    lines.append(f"|               |   wines |   bottles | value ({curr})   | volume (L)   |")
    lines.append("|:--------------|--------:|----------:|:--------------|:-------------|")
    lines.append(f"| Beginning     | {beg_w:7d} | {beg_b:9d} | {_fmt_chf(beg_v):13s} | {_fmt_litres(beg_l):12s} |")
    lines.append(f"| + Purchased   | {pur_w:7d} | {pur_b:9d} | {_fmt_chf(pur_v):13s} | {_fmt_litres(pur_l):12s} |")
    lines.append(f"| \u2212 Consumed    | {con_w:7d} | {con_b:9d} | {_fmt_chf(con_v):13s} | {_fmt_litres(con_l):12s} |")
    lines.append(f"| **Ending**    | **{end_w}** | **{end_b}** | **{_fmt_chf(end_v)}** | **{_fmt_litres(end_l)}** |")
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
    vt["beg. value"] = df["beg_val"].apply(_fmt_chf)
    vt["+ purchased"] = df["pur_val"].apply(_fmt_chf)
    vt["\u2212 consumed"] = df["con_val"].apply(_fmt_chf)
    vt["end. value"] = df["end_val"].apply(_fmt_chf)
    vt["net"] = df["net_val"].apply(_signed_chf)
    vt = vt.drop(columns=["label"])

    lines.append(f"### Value ({curr})\n")
    lines.append(_to_md(vt))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Re-exports -- backward compatibility for callers using query.find_wine
# etc.  The actual implementations now live in search and price.
# ---------------------------------------------------------------------------

_SEARCH_REEXPORTS = {
    "IntentResult",
    "_CONCEPT_EXPANSIONS",
    "_SEARCH_COLS",
    "_SYSTEM_CONCEPTS",
    "_extract_intents",
    "_normalise_query_tokens",
    "find_wine",
    "format_siblings",
}
_PRICE_REEXPORTS = {
    "get_price_history",
    "get_tracked_wine_prices",
    "log_price",
    "wishlist_alerts",
}


def __getattr__(name: str):
    if name in _SEARCH_REEXPORTS:
        from . import search

        return getattr(search, name)
    if name in _PRICE_REEXPORTS:
        from . import price

        return getattr(price, name)
    raise AttributeError(f"module 'cellarbrain.query' has no attribute {name!r}")
