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

from ._query_base import DataStaleError, QueryError, _format_df
from .flat import (
    BOTTLES_FULL_VIEW_SQL,
    BOTTLES_VIEW_SQL,
    LATEST_PRICES_VIEW_SQL,
    PRICE_HISTORY_VIEW_SQL,
    PRICE_OBSERVATIONS_VIEW_SQL,
    TRACKED_WINES_VIEW_SQL,
    WINES_COMPLETENESS_VIEW_SQL,
    WINES_FULL_VIEW_SQL,
    WINES_VIEW_SQL,
    WINES_WISHLIST_VIEW_SQL,
)
from .writer import SCHEMAS, schema_version_is_current

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


def _check_schema_compatibility(data_dir: pathlib.Path) -> None:
    """Raise :class:`DataStaleError` when on-disk Parquet schemas are stale.

    Fast path: if the schema-version sidecar matches the current fingerprint,
    skip the per-Parquet metadata reads.

    Slow path: read the schema of each required Parquet file and verify that
    every column declared in :data:`writer.SCHEMAS` is present.  Missing
    columns indicate the user upgraded cellarbrain without re-running ETL —
    DuckDB would otherwise emit a misleading ``BinderException`` referencing
    a "missing table".
    """
    if schema_version_is_current(data_dir):
        return

    import pyarrow.parquet as pq

    for table in _VIEW_REQUIRED_TABLES:
        path = data_dir / f"{table}.parquet"
        if not path.exists():
            continue  # handled by the missing-files check
        actual = {field.name for field in pq.read_schema(path)}
        expected = {field.name for field in SCHEMAS[table]}
        missing = expected - actual
        if missing:
            cols = ", ".join(sorted(missing))
            raise DataStaleError(
                f"{table}.parquet is missing columns: {cols}. "
                f"The schema changed in a newer cellarbrain release — "
                f"re-run 'cellarbrain etl' to rebuild the Parquet files."
            )


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
_TRACKED_VIEW_TABLES = ["tracked_wine", "wine", "bottle", "cellar", "winery", "appellation"]


def _wines_full_view_sql(data_dir: pathlib.Path) -> str:
    """Return the ``wines_full`` view SQL with inline ``read_parquet()`` paths."""
    sql = WINES_FULL_VIEW_SQL
    for table in ("wine", "winery", "appellation", "bottle", "cellar", "tasting", "pro_rating"):
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
    _check_schema_compatibility(d)

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

    # Research completeness view (optional — generated after dossiers exist)
    rc_path = d / "research_completeness.parquet"
    if rc_path.exists():
        rc_pq = str(rc_path).replace("\\", "/")
        con.execute(f"CREATE VIEW research_completeness AS SELECT * FROM read_parquet('{rc_pq}')")
        con.execute(f"CREATE VIEW wines_completeness AS {WINES_COMPLETENESS_VIEW_SQL}")

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

    # Register phonetic UDFs if the [search] extra is installed (non-fatal).
    from .phonetic import register_udfs as _register_phonetic_udfs

    _register_phonetic_udfs(con)

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
    fmt: str = "markdown",
) -> str:
    """Execute a read-only SQL query and return formatted results.

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
        result = _format_df(df.head(row_limit), fmt)
        result += (
            f"\n\n*({total} rows total, showing first {row_limit})*"
            if fmt == "markdown"
            else f"\n\n({total} rows total, showing first {row_limit})"
        )
        return result

    return _format_df(df, fmt)


def execute_query_structured(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    row_limit: int = 200,
) -> tuple[str, dict]:
    """Execute a read-only SQL query and return (markdown_text, structured_data).

    The structured data dict contains:
    - columns: list of column names
    - rows: list of row dicts (respecting row_limit)
    - row_count: total rows before truncation
    - truncated: whether output was truncated

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
    logger.debug("execute_query_structured rows=%d sql=%s", total, sql.strip()[:200])
    truncated = total > row_limit

    if total == 0:
        data = {"columns": list(df.columns), "rows": [], "row_count": 0, "truncated": False}
        return "*No results.*", data

    display_df = df.head(row_limit) if truncated else df
    text = _format_df(display_df, "markdown")
    if truncated:
        text += f"\n\n*({total} rows total, showing first {row_limit})*"

    # Convert to serialisable rows (NaN → None)
    rows = display_df.where(display_df.notna(), None).to_dict(orient="records")
    data = {
        "columns": list(df.columns),
        "rows": rows,
        "row_count": total,
        "truncated": truncated,
    }
    return text, data


# ---------------------------------------------------------------------------
# Cellar statistics
# ---------------------------------------------------------------------------


def cellar_stats(
    con: duckdb.DuckDBPyConnection,
    group_by: str | None = None,
    currency: str = "CHF",
    limit: int = 20,
    sort_by: str | None = None,
    fmt: str = "markdown",
) -> str:
    """Return formatted cellar statistics.

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
            fmt=fmt,
        )
    return _overall_stats(con, currency=currency, fmt=fmt)


def _overall_stats(con: duckdb.DuckDBPyConnection, *, currency: str = "CHF", fmt: str = "markdown") -> str:
    """Build the default cellar summary."""
    lines: list[str] = ["## Cellar Summary\n"] if fmt == "markdown" else ["📊 CELLAR SUMMARY\n"]

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
    if fmt == "plain":
        lines.append(f"🍷 Wines: {wines_cellar} stored, {wines_on_order} on order ({wines_total} total)")
        lines.append(f"🍾 Bottles: {bottles_cellar} stored, {bottles_on_order} on order ({bottles_total} total)")
        lines.append(
            f"💰 Value: {curr} {_fmt_chf(value_cellar)} stored + {curr} {_fmt_chf(value_on_order)} on order = {curr} {_fmt_chf(value_total)}"
        )
        lines.append(
            f"📏 Volume: {_fmt_litres(volume_cellar)}L stored + {_fmt_litres(volume_on_order)}L on order = {_fmt_litres(volume_total)}L"
        )
    else:
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
    lines.append("### In Cellar\n" if fmt == "markdown" else "IN CELLAR\n")

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
        lines.append("#### By Category" if fmt == "markdown" else "By Category:")
        lines.append(_format_df(cat_df, fmt, style="compact"))
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
        lines.append("#### Drinking Window Status" if fmt == "markdown" else "Drinking Window:")
        lines.append(_format_df(dw_df, fmt, style="compact"))
        lines.append("")

    # Data freshness
    try:
        etl_row = con.execute("""
            SELECT run_id, started_at, run_type
            FROM etl_run ORDER BY run_id DESC LIMIT 1
        """).fetchone()
        if etl_row:
            run_id, started, run_type = etl_row
            if fmt == "plain":
                lines.append(f"Last refresh: {started} ({run_type})")
            else:
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
    fmt: str = "markdown",
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
    if fmt == "plain":
        if group_by == "status":
            header = f"📊 STATS BY {group_by.upper()}\n"
        elif group_by == "on_order":
            header = f"📊 ON ORDER — BY {group_by.upper()}\n"
        else:
            header = f"📊 IN CELLAR — BY {group_by.upper()}\n"
    else:
        if group_by == "status":
            header = f"## Cellar Statistics — by {group_by.title()}\n"
        elif group_by == "on_order":
            header = f"## On Order Statistics — by {group_by.title()}\n"
        else:
            header = f"## In Cellar Statistics — by {group_by.title()}\n"

    # Rename group_val to the dimension name for display
    df = df.rename(columns={"group_val": _GROUP_LABEL[group_by]})

    return header + "\n" + _format_df(df, fmt, style="compact") + footnote


# ---------------------------------------------------------------------------
# Cellar churn (roll-forward analysis)
# ---------------------------------------------------------------------------


def cellar_churn(
    con: duckdb.DuckDBPyConnection,
    period: str | None = None,
    year: int | None = None,
    month: int | None = None,
    currency: str = "CHF",
    fmt: str = "markdown",
) -> str:
    """Return cellar churn (roll-forward) analysis.

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
            fmt=fmt,
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
            fmt=fmt,
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

    return _single_period_churn(con, start, end, title, currency=currency, fmt=fmt)


def _single_period_churn(
    con: duckdb.DuckDBPyConnection,
    start: object,
    end: object,
    title: str,
    *,
    currency: str = "CHF",
    fmt: str = "markdown",
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

    if fmt == "plain":
        lines: list[str] = [f"📊 CELLAR CHURN — {title}\n"]
        lines.append(f"Beginning: {beg_w} wines, {beg_b} bottles, {curr} {_fmt_chf(beg_v)}, {_fmt_litres(beg_l)}L")
        lines.append(f"+ Purchased: {pur_w} wines, {pur_b} bottles, {curr} {_fmt_chf(pur_v)}, {_fmt_litres(pur_l)}L")
        lines.append(f"− Consumed: {con_w} wines, {con_b} bottles, {curr} {_fmt_chf(con_v)}, {_fmt_litres(con_l)}L")
        lines.append(f"= Ending: {end_w} wines, {end_b} bottles, {curr} {_fmt_chf(end_v)}, {_fmt_litres(end_l)}L")
    else:
        lines: list[str] = [f"## Cellar Churn — {title}\n"]
        lines.append(f"|               |   wines |   bottles | value ({curr})   | volume (L)   |")
        lines.append("|:--------------|--------:|----------:|:--------------|:-------------|")
        lines.append(f"| Beginning     | {beg_w:7d} | {beg_b:9d} | {_fmt_chf(beg_v):13s} | {_fmt_litres(beg_l):12s} |")
        lines.append(f"| + Purchased   | {pur_w:7d} | {pur_b:9d} | {_fmt_chf(pur_v):13s} | {_fmt_litres(pur_l):12s} |")
        lines.append(f"| − Consumed    | {con_w:7d} | {con_b:9d} | {_fmt_chf(con_v):13s} | {_fmt_litres(con_l):12s} |")
        lines.append(
            f"| **Ending**    | **{end_w}** | **{end_b}** | **{_fmt_chf(end_v)}** | **{_fmt_litres(end_l)}** |"
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
        lines.append("### Ending Inventory\n" if fmt == "markdown" else "Ending Inventory:\n")
        lines.append(_format_df(inv_df, fmt, style="compact"))

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
    fmt: str = "markdown",
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
    bt.columns = [col_name, "beg. bottles", "+ purchased", "− consumed", "end. bottles", "net"]
    bt["net"] = bt["net"].apply(_signed_int)

    if fmt == "plain":
        lines: list[str] = [f"📊 CELLAR CHURN — {title}\n"]
    else:
        lines: list[str] = [f"## Cellar Churn — {title}\n"]
    lines.append(_format_df(bt, fmt, style="compact"))
    lines.append("")

    # --- Value table ---
    def _signed_chf(v: float) -> str:
        s = _fmt_chf(abs(v))
        return f"+{s}" if v >= 0 else f"−{s}"

    vt = df[["label"]].copy()
    vt[col_name] = vt["label"]
    vt["beg. value"] = df["beg_val"].apply(_fmt_chf)
    vt["+ purchased"] = df["pur_val"].apply(_fmt_chf)
    vt["− consumed"] = df["con_val"].apply(_fmt_chf)
    vt["end. value"] = df["end_val"].apply(_fmt_chf)
    vt["net"] = df["net_val"].apply(_signed_chf)
    vt = vt.drop(columns=["label"])

    lines.append(f"### Value ({curr})\n" if fmt == "markdown" else f"Value ({curr}):\n")
    lines.append(_format_df(vt, fmt, style="compact"))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Consumption velocity (rate analysis)
# ---------------------------------------------------------------------------


def consumption_velocity(
    con: duckdb.DuckDBPyConnection,
    months: int = 6,
    currency: str = "CHF",
) -> tuple[str, dict]:
    """Compute consumption and acquisition rates over recent months.

    Returns a tuple of (markdown_text, structured_data) with month-by-month
    counts, averages, net growth rate, and a 12-month cellar size projection.

    Raises ValueError for invalid parameters.
    """
    import datetime as _dt

    if months < 1:
        raise ValueError(f"months must be >= 1, got {months}")

    today = _dt.date.today()
    # Start of the current month
    current_month_start = _dt.date(today.year, today.month, 1)
    # Go back N months from the start of the current month
    # (we exclude the current incomplete month)
    if today.month - months >= 1:
        range_start = _dt.date(today.year, today.month - months, 1)
    else:
        years_back = (months - today.month) // 12 + 1
        m = today.month - months + 12 * years_back
        range_start = _dt.date(today.year - years_back, m, 1)

    sql = f"""
        WITH series AS (
            SELECT unnest(generate_series(
                '{range_start}'::DATE, '{current_month_start}'::DATE, INTERVAL '1 MONTH'
            )) AS period_start
        ),
        periods AS (
            SELECT period_start::DATE AS period_start,
                   (period_start + INTERVAL '1 MONTH')::DATE AS period_end,
                   strftime(period_start, '%Y-%m') AS label
            FROM series
            WHERE period_start < '{current_month_start}'::DATE
        )
        SELECT
            p.label,
            count(*) FILTER (
                WHERE b.purchase_date >= p.period_start AND b.purchase_date < p.period_end
            ) AS acquired,
            count(*) FILTER (
                WHERE b.output_date >= p.period_start AND b.output_date < p.period_end
            ) AS consumed
        FROM periods p
        CROSS JOIN bottles_full b
        GROUP BY p.label, p.period_start
        ORDER BY p.period_start
    """
    rows = con.execute(sql).fetchall()

    month_data = [{"month": r[0], "acquired": int(r[1]), "consumed": int(r[2])} for r in rows]

    num_months = len(month_data) if month_data else 1
    total_acquired = sum(m["acquired"] for m in month_data)
    total_consumed = sum(m["consumed"] for m in month_data)
    avg_acquired = round(total_acquired / num_months, 1)
    avg_consumed = round(total_consumed / num_months, 1)
    net_growth = round(avg_acquired - avg_consumed, 1)

    # Current cellar size (stored, not in transit)
    current_bottles = con.execute(
        "SELECT count(*) FROM bottles_full WHERE status = 'stored' AND NOT is_in_transit"
    ).fetchone()[0]

    projected_12m = int(current_bottles + round(net_growth * 12))

    # Build structured data
    data = {
        "months": month_data,
        "avg_acquired_per_month": avg_acquired,
        "avg_consumed_per_month": avg_consumed,
        "net_growth_per_month": net_growth,
        "current_bottles": int(current_bottles),
        "projected_12m": projected_12m,
        "lookback_months": num_months,
    }

    # Build markdown
    net_sign = "+" if net_growth >= 0 else ""
    lines: list[str] = [f"## Consumption Velocity — Last {num_months} Months\n"]
    lines.append("| month | acquired | consumed | net |")
    lines.append("|:------|--------:|---------:|----:|")
    for m in month_data:
        n = m["acquired"] - m["consumed"]
        ns = f"+{n}" if n >= 0 else str(n)
        lines.append(f"| {m['month']} | {m['acquired']} | {m['consumed']} | {ns} |")
    lines.append("")
    lines.append(
        f"**Averages (per month):** acquired {avg_acquired}, consumed {avg_consumed}, net {net_sign}{net_growth}"
    )
    lines.append(f"**Current cellar:** {current_bottles} bottles")
    lines.append(f"**Projected in 12 months:** ~{projected_12m} bottles")

    return "\n".join(lines), data


# ---------------------------------------------------------------------------
# cellar_gaps — gap analysis
# ---------------------------------------------------------------------------

_VALID_GAP_DIMENSIONS = {"region", "grape", "price_tier", "vintage"}


def cellar_gaps(
    con: duckdb.DuckDBPyConnection,
    dimension: str | None = None,
    months: int = 12,
) -> tuple[str, dict]:
    """Identify underrepresented categories in the cellar.

    Analyses gaps across four dimensions: regions consumed but depleted,
    grape varieties with no drinkable bottles, price tiers lacking
    ready-to-drink options, and aging-worthy vintages with no coverage.

    Args:
        dimension: Analyse a single dimension — one of "region", "grape",
                   "price_tier", "vintage". If None, returns all four.
        months: Lookback months for consumption frequency inference
                (default 12, max 60).

    Returns a tuple of (markdown_text, structured_data).
    Raises ValueError for invalid parameters.
    """
    if dimension is not None:
        dimension = dimension.strip().lower()
        if dimension not in _VALID_GAP_DIMENSIONS:
            raise ValueError(
                f"Invalid dimension: {dimension!r}. Must be one of: {', '.join(sorted(_VALID_GAP_DIMENSIONS))}"
            )
    if months < 1 or months > 60:
        raise ValueError(f"months must be between 1 and 60, got {months}")

    sections: list[str] = []
    data: dict = {"dimension": dimension, "months": months}

    if dimension is None or dimension == "region":
        region_md, region_data = _region_gaps(con, months)
        sections.append(region_md)
        data["region_gaps"] = region_data

    if dimension is None or dimension == "grape":
        grape_md, grape_data = _grape_gaps(con)
        sections.append(grape_md)
        data["grape_gaps"] = grape_data

    if dimension is None or dimension == "price_tier":
        tier_md, tier_data = _price_tier_gaps(con)
        sections.append(tier_md)
        data["price_tier_gaps"] = tier_data

    if dimension is None or dimension == "vintage":
        vintage_md, vintage_data = _vintage_gaps(con)
        sections.append(vintage_md)
        data["vintage_gaps"] = vintage_data

    text = "\n\n".join(sections) if sections else "No gap data available."
    return text, data


def _region_gaps(
    con: duckdb.DuckDBPyConnection,
    months: int,
) -> tuple[str, list[dict]]:
    """Regions consumed frequently but with 0-1 bottles currently stored."""
    import datetime as _dt

    today = _dt.date.today()
    if today.month - months >= 1:
        range_start = _dt.date(today.year, today.month - months, 1)
    else:
        years_back = (months - today.month) // 12 + 1
        m = today.month - months + 12 * years_back
        range_start = _dt.date(today.year - years_back, m, 1)

    sql = f"""
        WITH consumed_regions AS (
            SELECT w.region, count(*) AS consumed_count
            FROM bottles_full b
            JOIN wines_full w ON b.wine_id = w.wine_id
            WHERE b.output_date >= '{range_start}'
              AND w.region IS NOT NULL AND w.region != ''
            GROUP BY w.region
            HAVING count(*) >= 2
        ),
        stored_regions AS (
            SELECT region, CAST(sum(bottles_stored) AS BIGINT) AS bottles_stored
            FROM wines_full
            WHERE region IS NOT NULL AND region != ''
              AND bottles_stored > 0
            GROUP BY region
        )
        SELECT cr.region,
               cr.consumed_count,
               COALESCE(sr.bottles_stored, 0) AS bottles_stored
        FROM consumed_regions cr
        LEFT JOIN stored_regions sr ON cr.region = sr.region
        WHERE COALESCE(sr.bottles_stored, 0) <= 1
        ORDER BY cr.consumed_count DESC
    """
    rows = con.execute(sql).fetchall()
    gaps = [{"region": r[0], "consumed": int(r[1]), "stored": int(r[2])} for r in rows]

    lines = [f"### Region Gaps (consumed in last {months} months but ≤1 bottle stored)\n"]
    if gaps:
        lines.append("| region | consumed | stored |")
        lines.append("|:-------|--------:|-------:|")
        for g in gaps:
            lines.append(f"| {g['region']} | {g['consumed']} | {g['stored']} |")
    else:
        lines.append("No region gaps detected — all frequently consumed regions are well stocked.")

    return "\n".join(lines), gaps


def _grape_gaps(con: duckdb.DuckDBPyConnection) -> tuple[str, list[dict]]:
    """Grape varieties in cellar with no bottles in optimal/drinkable window for next 12 months."""
    import datetime as _dt

    current_year = _dt.date.today().year

    sql = f"""
        WITH cellar_grapes AS (
            SELECT DISTINCT primary_grape
            FROM wines_full
            WHERE bottles_stored > 0
              AND primary_grape IS NOT NULL AND primary_grape != ''
        ),
        drinkable_grapes AS (
            SELECT DISTINCT primary_grape
            FROM wines_full
            WHERE bottles_stored > 0
              AND primary_grape IS NOT NULL AND primary_grape != ''
              AND (
                  drinking_status IN ('optimal', 'drinkable')
                  OR (drink_from IS NOT NULL AND drink_from <= {current_year + 1})
              )
        )
        SELECT cg.primary_grape,
               CAST(sum(w.bottles_stored) AS BIGINT) AS total_stored
        FROM cellar_grapes cg
        JOIN wines_full w ON cg.primary_grape = w.primary_grape AND w.bottles_stored > 0
        WHERE cg.primary_grape NOT IN (SELECT primary_grape FROM drinkable_grapes)
        GROUP BY cg.primary_grape
        ORDER BY total_stored DESC
    """
    rows = con.execute(sql).fetchall()
    gaps = [{"grape": r[0], "bottles_stored": int(r[1])} for r in rows]

    lines = ["### Grape Gaps (varieties with no drinkable bottles in next 12 months)\n"]
    if gaps:
        lines.append("| grape | bottles (all too young) |")
        lines.append("|:------|----------------------:|")
        for g in gaps:
            lines.append(f"| {g['grape']} | {g['bottles_stored']} |")
    else:
        lines.append("No grape gaps — all varieties have at least one drinkable bottle.")

    return "\n".join(lines), gaps


def _price_tier_gaps(con: duckdb.DuckDBPyConnection) -> tuple[str, list[dict]]:
    """Price tiers with stored bottles but no ready-to-drink options."""
    sql = """
        WITH tier_stored AS (
            SELECT price_tier, CAST(sum(bottles_stored) AS BIGINT) AS total_stored
            FROM wines_full
            WHERE bottles_stored > 0
              AND price_tier IS NOT NULL AND price_tier != '' AND price_tier != 'unknown'
            GROUP BY price_tier
        ),
        tier_ready AS (
            SELECT price_tier, CAST(sum(bottles_stored) AS BIGINT) AS ready_count
            FROM wines_full
            WHERE bottles_stored > 0
              AND drinking_status IN ('optimal', 'drinkable')
              AND price_tier IS NOT NULL AND price_tier != '' AND price_tier != 'unknown'
            GROUP BY price_tier
        )
        SELECT ts.price_tier,
               ts.total_stored,
               COALESCE(tr.ready_count, 0) AS ready_count
        FROM tier_stored ts
        LEFT JOIN tier_ready tr ON ts.price_tier = tr.price_tier
        WHERE COALESCE(tr.ready_count, 0) = 0
        ORDER BY ts.total_stored DESC
    """
    rows = con.execute(sql).fetchall()
    gaps = [{"price_tier": r[0], "total_stored": int(r[1]), "ready": int(r[2])} for r in rows]

    lines = ["### Price Tier Gaps (tiers with bottles but none ready to drink)\n"]
    if gaps:
        lines.append("| price tier | bottles stored | ready to drink |")
        lines.append("|:-----------|-------------:|---------------:|")
        for g in gaps:
            lines.append(f"| {g['price_tier']} | {g['total_stored']} | {g['ready']} |")
    else:
        lines.append("No price tier gaps — all tiers have at least one ready-to-drink bottle.")

    return "\n".join(lines), gaps


def _vintage_gaps(con: duckdb.DuckDBPyConnection) -> tuple[str, list[dict]]:
    """Aging-worthy categories with empty vintage decades."""
    sql = """
        WITH aging_wines AS (
            SELECT vintage,
                   CAST((vintage / 10) * 10 AS BIGINT) AS decade,
                   CAST(sum(bottles_stored) AS BIGINT) AS bottles
            FROM wines_full
            WHERE bottles_stored > 0
              AND category IN ('Red wine', 'Fortified wine')
              AND vintage IS NOT NULL AND vintage > 1900
            GROUP BY vintage, CAST((vintage / 10) * 10 AS BIGINT)
        ),
        decade_range AS (
            SELECT unnest(generate_series(
                (SELECT min(decade) FROM aging_wines)::BIGINT,
                (SELECT max(decade) FROM aging_wines)::BIGINT,
                10::BIGINT
            )) AS decade
        ),
        decade_summary AS (
            SELECT dr.decade,
                   COALESCE(sum(aw.bottles), 0) AS bottles
            FROM decade_range dr
            LEFT JOIN aging_wines aw ON dr.decade = aw.decade
            GROUP BY dr.decade
        )
        SELECT decade, CAST(bottles AS BIGINT) AS bottles
        FROM decade_summary
        WHERE bottles = 0
        ORDER BY decade
    """
    rows = con.execute(sql).fetchall()
    gaps = [{"decade": int(r[0]), "bottles": int(r[1])} for r in rows]

    lines = ["### Vintage Gaps (aging-worthy decades with zero bottles)\n"]
    if gaps:
        lines.append("| decade | bottles |")
        lines.append("|:-------|--------:|")
        for g in gaps:
            lines.append(f"| {g['decade']}s | {g['bottles']} |")
    else:
        lines.append("No vintage decade gaps — all decades between your oldest and newest have coverage.")

    return "\n".join(lines), gaps


# ---------------------------------------------------------------------------
# Re-exports -- backward compatibility for callers using query.find_wine
# etc.  The actual implementations now live in search and price.
# ---------------------------------------------------------------------------

_SEARCH_REEXPORTS = {
    "IntentResult",
    "SearchTelemetry",
    "_CONCEPT_EXPANSIONS",
    "_SEARCH_COLS",
    "_SYSTEM_CONCEPTS",
    "_extract_intents",
    "_normalise_query_tokens",
    "find_wine",
    "find_wine_with_telemetry",
    "format_siblings",
    "suggest_wines",
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
