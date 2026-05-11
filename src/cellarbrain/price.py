"""Price tracking operations for tracked wines.

Provides price observation logging, price lookup, history retrieval,
and wishlist alert detection. Extracted from ``query.py`` for cohesion.

Public API:
- ``log_price`` — record/update a price observation
- ``get_tracked_wine_prices`` — latest prices for a tracked wine
- ``get_price_history`` — monthly price history
- ``wishlist_alerts`` — detect and format price/stock alerts
"""

from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

import duckdb

from ._query_base import QueryError, _format_df

if TYPE_CHECKING:
    from .settings import Settings

logger = logging.getLogger(__name__)


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
    from . import writer
    from .computed import convert_to_default_currency
    from .dossier_ops import TrackedWineNotFoundError
    from .settings import load_settings

    d = pathlib.Path(data_dir)

    # Validate required fields
    required = [
        "tracked_wine_id",
        "bottle_size_ml",
        "retailer_name",
        "price",
        "currency",
        "in_stock",
        "observed_at",
        "observation_source",
    ]
    missing = [f for f in required if observation.get(f) is None]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    # Validate tracked_wine exists
    tw_rows = writer.read_parquet_rows("tracked_wine", d)
    tw_ids = {r["tracked_wine_id"] for r in tw_rows}
    twid = observation["tracked_wine_id"]
    if twid not in tw_ids:
        raise TrackedWineNotFoundError(f"Tracked wine {twid} not found in tracked_wine.parquet")

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
            price,
            currency,
            s.currency.default,
            s.currency.rates,
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
        return (r["tracked_wine_id"], r.get("vintage"), r["bottle_size_ml"], r["retailer_name"], rd)

    # Filter out any existing row with the same dedup key
    filtered = [r for r in existing if _row_key(r) != dedup_key]
    replaced = len(existing) - len(filtered)

    # Build the new row
    new_row = {
        "observation_id": max_id + 1
        if not replaced
        else next(r["observation_id"] for r in existing if _row_key(r) == dedup_key),
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
    fmt: str = "markdown",
) -> str:
    """Return latest prices for a tracked wine."""
    from . import writer
    from .dossier_ops import TrackedWineNotFoundError

    d = pathlib.Path(data_dir)

    # Validate tracked_wine exists
    tw_rows = writer.read_parquet_rows("tracked_wine", d)
    tw_ids = {r["tracked_wine_id"] for r in tw_rows}
    if tracked_wine_id not in tw_ids:
        raise TrackedWineNotFoundError(f"Tracked wine {tracked_wine_id} not found")

    from .query import _has_partitioned_files, get_agent_connection

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
    return _format_df(df, fmt, style="list")


def get_price_history(
    data_dir: str | pathlib.Path,
    tracked_wine_id: int,
    vintage: int | None = None,
    months: int = 12,
    fmt: str = "markdown",
) -> str:
    """Return monthly price history for a tracked wine."""
    from . import writer
    from .dossier_ops import TrackedWineNotFoundError

    d = pathlib.Path(data_dir)

    # Validate tracked_wine exists
    tw_rows = writer.read_parquet_rows("tracked_wine", d)
    tw_ids = {r["tracked_wine_id"] for r in tw_rows}
    if tracked_wine_id not in tw_ids:
        raise TrackedWineNotFoundError(f"Tracked wine {tracked_wine_id} not found")

    from .query import _has_partitioned_files, get_agent_connection

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
    return _format_df(df, fmt, style="list")


def wishlist_alerts(
    data_dir: str | pathlib.Path,
    settings: Settings | None = None,
    days: int | None = None,
    fmt: str = "markdown",
) -> str:
    """Detect and return wishlist alerts grouped by priority.

    Checks for 6 alert types across 3 priority levels:
    - **High:** New Listing, Price Drop, Back in Stock
    - **Medium:** Best Price, En Primeur, Last Bottles
    """
    from .settings import load_settings

    d = pathlib.Path(data_dir)
    from .query import _has_partitioned_files, get_agent_connection

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
                f"- **Back in Stock:** {r['winery_name']} {r['wine_name']}{vint} at {r['retailer_name']}{price_str}"
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

    if fmt == "plain":
        lines = ["🔔 WISHLIST ALERTS", ""]
        if alerts["high"]:
            lines.append("🔴 HIGH PRIORITY")
            for a in alerts["high"]:
                lines.append(a.replace("- **", "").replace(":**", ":").replace("**", ""))
            lines.append("")
        if alerts["medium"]:
            lines.append("🟡 MEDIUM PRIORITY")
            for a in alerts["medium"]:
                lines.append(a.replace("- **", "").replace(":**", ":").replace("**", ""))
            lines.append("")
    else:
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
