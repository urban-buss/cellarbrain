"""Cellar data queries — wines, bottles, statistics, dossier metadata."""

from __future__ import annotations

import duckdb

# Sort column allow-lists (prevent SQL injection)
_WINE_SORT_COLUMNS = {
    "wine_id",
    "wine_name",
    "vintage",
    "winery_name",
    "region",
    "category",
    "bottles_stored",
    "price",
    "drinking_status",
}
_BOTTLE_SORT_COLUMNS = {
    "bottle_id",
    "wine_name",
    "vintage",
    "cellar_name",
    "shelf",
    "price",
    "status",
}


def get_wines(
    con: duckdb.DuckDBPyConnection,
    *,
    q: str | None = None,
    category: str | None = None,
    region: str | None = None,
    status: str | None = None,
    vintage_min: int | None = None,
    vintage_max: int | None = None,
    hide_empty: bool = True,
    sort: str = "wine_name",
    desc: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Paginated, filterable wine list from the ``wines`` view.

    Returns (rows, total_count).
    """
    wheres: list[str] = []
    params: list = []

    if hide_empty:
        wheres.append("(bottles_stored + COALESCE(bottles_on_order, 0)) > 0")
    if q:
        wheres.append("(wine_name ILIKE ? OR winery_name ILIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])
    if category:
        wheres.append("category = ?")
        params.append(category)
    if region:
        wheres.append("(region = ? OR country = ?)")
        params.extend([region, region])
    if status:
        wheres.append("drinking_status = ?")
        params.append(status)
    if vintage_min is not None:
        wheres.append("vintage >= ?")
        params.append(vintage_min)
    if vintage_max is not None:
        wheres.append("vintage <= ?")
        params.append(vintage_max)

    where = " AND ".join(wheres) if wheres else "TRUE"
    col = sort if sort in _WINE_SORT_COLUMNS else "wine_name"
    direction = "DESC" if desc else "ASC"

    total = con.execute(
        f"SELECT COUNT(*) FROM wines WHERE {where}",
        params,
    ).fetchone()[0]

    rows = con.execute(
        f"""
        SELECT wine_id, wine_name, vintage, winery_name, country, region,
               category, bottles_stored, price, drinking_status
        FROM wines
        WHERE {where}
        ORDER BY {col} {direction}
        LIMIT ? OFFSET ?
    """,
        params + [limit, offset],
    ).fetchall()

    return [
        {
            "wine_id": r[0],
            "wine_name": r[1],
            "vintage": r[2],
            "winery_name": r[3],
            "country": r[4],
            "region": r[5],
            "category": r[6],
            "bottles_stored": r[7],
            "price": r[8],
            "drinking_status": r[9],
        }
        for r in rows
    ], total


def get_wine_detail(
    con: duckdb.DuckDBPyConnection,
    wine_id: int,
) -> dict | None:
    """Full wine record from ``wines_full`` view."""
    row = con.execute(
        "SELECT * FROM wines_full WHERE wine_id = ?",
        [wine_id],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in con.description]
    return dict(zip(cols, row))


def get_wine_bottles(
    con: duckdb.DuckDBPyConnection,
    wine_id: int,
) -> list[dict]:
    """Bottles for a specific wine from ``bottles_full`` view."""
    rows = con.execute(
        """
        SELECT bottle_id, cellar_name, shelf, price,
               CASE WHEN is_in_transit THEN 'on order' ELSE status END
                   AS status
        FROM bottles_full WHERE wine_id = ?
        ORDER BY bottle_id
    """,
        [wine_id],
    ).fetchall()
    return [{"bottle_id": r[0], "cellar_name": r[1], "shelf": r[2], "price": r[3], "status": r[4]} for r in rows]


def get_format_siblings(
    con: duckdb.DuckDBPyConnection,
    wine_id: int,
    format_group_id: int | None,
) -> list[dict]:
    """Other wines in the same format group (excluding the current wine)."""
    if format_group_id is None:
        return []
    rows = con.execute(
        """
        SELECT wine_id, bottle_format, volume_ml
        FROM wines_full
        WHERE format_group_id = ? AND wine_id != ?
        ORDER BY volume_ml
    """,
        [format_group_id, wine_id],
    ).fetchall()
    return [{"wine_id": r[0], "bottle_format": r[1], "volume_ml": r[2]} for r in rows]


def get_bottles(
    con: duckdb.DuckDBPyConnection,
    *,
    view: str = "stored",
    cellar: str | None = None,
    category: str | None = None,
    sort: str = "wine_name",
    desc: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Paginated bottle list.

    ``view`` selects the underlying DuckDB view:
    - "stored" → ``bottles_stored``
    - "on_order" → ``bottles_on_order``
    - "consumed" → ``bottles_consumed``
    - "all" → ``bottles``
    """
    _VIEW_FILTERS = {
        "stored": "status = 'stored' AND NOT is_in_transit",
        "on_order": "status = 'stored' AND is_in_transit",
        "consumed": "status != 'stored'",
        "all": "TRUE",
    }
    view_filter = _VIEW_FILTERS.get(view, _VIEW_FILTERS["stored"])
    col = sort if sort in _BOTTLE_SORT_COLUMNS else "wine_name"
    direction = "DESC" if desc else "ASC"

    wheres: list[str] = [view_filter]
    params: list = []
    if cellar:
        wheres.append("cellar_name = ?")
        params.append(cellar)
    if category:
        wheres.append("category = ?")
        params.append(category)
    where = " AND ".join(wheres)

    total = con.execute(
        f"SELECT COUNT(*) FROM bottles_full WHERE {where}",
        params,
    ).fetchone()[0]

    rows = con.execute(
        f"""
        SELECT bottle_id, wine_name, vintage, cellar_name, shelf,
               price,
               CASE WHEN is_in_transit THEN 'on order' ELSE status END
                   AS status,
               wine_id
        FROM bottles_full
        WHERE {where}
        ORDER BY {col} {direction}
        LIMIT ? OFFSET ?
    """,
        params + [limit, offset],
    ).fetchall()

    return [
        {
            "bottle_id": r[0],
            "wine_name": r[1],
            "vintage": r[2],
            "cellar_name": r[3],
            "shelf": r[4],
            "price": r[5],
            "status": r[6],
            "wine_id": r[7],
        }
        for r in rows
    ], total


def get_drinking_now(
    con: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Wines in their drinking window, ordered by urgency."""
    rows = con.execute("""
        SELECT wine_id, wine_name, vintage, winery_name, category,
               bottles_stored, price, drinking_status,
               drink_from, drink_until, optimal_from, optimal_until
        FROM wines_full
        WHERE drinking_status IN ('optimal', 'drinkable', 'past_optimal')
          AND bottles_stored > 0
        ORDER BY
            CASE drinking_status
                WHEN 'past_optimal' THEN 1
                WHEN 'optimal' THEN 2
                WHEN 'drinkable' THEN 3
            END,
            optimal_until ASC NULLS LAST
    """).fetchall()
    return [
        {
            "wine_id": r[0],
            "wine_name": r[1],
            "vintage": r[2],
            "winery_name": r[3],
            "category": r[4],
            "bottles_stored": r[5],
            "price": r[6],
            "drinking_status": r[7],
            "drink_from": r[8],
            "drink_until": r[9],
            "optimal_from": r[10],
            "optimal_until": r[11],
        }
        for r in rows
    ]


def get_filter_options(
    con: duckdb.DuckDBPyConnection,
) -> dict[str, list[str]]:
    """Distinct values for filter dropdowns on the wine catalogue."""
    result = {}
    for col in ("category", "country", "region", "drinking_status"):
        rows = con.execute(f"SELECT DISTINCT {col} FROM wines WHERE {col} IS NOT NULL ORDER BY {col}").fetchall()
        result[col] = [r[0] for r in rows]
    return result


def get_cellar_names(
    con: duckdb.DuckDBPyConnection,
) -> list[str]:
    """Distinct cellar names for bottle filter dropdown."""
    rows = con.execute(
        "SELECT DISTINCT cellar_name FROM bottles WHERE cellar_name IS NOT NULL ORDER BY cellar_name"
    ).fetchall()
    return [r[0] for r in rows]


def get_quick_stats(
    con: duckdb.DuckDBPyConnection,
) -> dict:
    """Summary stats for the wine catalogue footer."""
    row = con.execute("""
        SELECT COUNT(DISTINCT wine_id) AS wines,
               SUM(bottles_stored)     AS bottles,
               ROUND(SUM(bottles_stored * COALESCE(price, 0)), 0) AS value,
               SUM(CASE WHEN drinking_status IN ('optimal', 'drinkable')
                        THEN 1 ELSE 0 END) AS ready
        FROM wines
    """).fetchone()
    return {
        "wines": row[0],
        "bottles": row[1],
        "value": row[2],
        "ready": row[3],
    }


def get_wine_of_the_day(
    con: duckdb.DuckDBPyConnection,
) -> dict | None:
    """Today's wine pick for the dashboard widget."""
    try:
        from cellarbrain.wotd import pick_wine_of_the_day

        pick = pick_wine_of_the_day(con)
        if pick is None:
            return None
        return {
            "wine_id": pick.wine_id,
            "wine_name": pick.wine_name,
            "vintage": pick.vintage,
            "winery_name": pick.winery_name,
            "category": pick.category,
            "region": pick.region,
            "primary_grape": pick.primary_grape,
            "drinking_status": pick.drinking_status,
            "bottles_stored": pick.bottles_stored,
            "reason": pick.reason,
        }
    except Exception:
        return None


# ---- Statistics -----------------------------------------------------------


def get_cellar_stats_overview(
    con: duckdb.DuckDBPyConnection,
) -> dict:
    """Overall cellar statistics."""
    row = con.execute("""
        SELECT
            COUNT(DISTINCT wine_id)                                    AS wines,
            SUM(bottles_stored)                                        AS bottles,
            ROUND(SUM(bottles_stored * COALESCE(price, 0)), 0)        AS total_value,
            ROUND(AVG(CASE WHEN price > 0 THEN price END), 0)         AS avg_price,
            SUM(CASE WHEN drinking_status IN ('optimal', 'drinkable')
                     THEN 1 ELSE 0 END)                               AS ready,
            COUNT(DISTINCT country)                                    AS countries,
            COUNT(DISTINCT region)                                     AS regions,
            COALESCE(SUM(bottles_on_order), 0)                         AS on_order,
            ROUND(SUM(COALESCE(bottles_on_order, 0) * COALESCE(price, 0)), 0) AS on_order_value
        FROM wines
    """).fetchone()
    return {
        "wines": row[0],
        "bottles": row[1],
        "total_value": row[2],
        "avg_price": row[3],
        "ready": row[4],
        "countries": row[5],
        "regions": row[6],
        "on_order": row[7],
        "on_order_value": row[8],
    }


_STATS_GROUP_COLUMNS = {
    "country",
    "region",
    "category",
    "vintage",
    "winery_name",
    "drinking_status",
}
_STATS_SORT_COLUMNS = {"wines", "bottles", "value"}


def get_cellar_stats_grouped(
    con: duckdb.DuckDBPyConnection,
    group_by: str = "country",
    sort: str = "bottles",
    desc: bool = True,
) -> list[dict]:
    """Grouped statistics for Chart.js and table display."""
    col = group_by if group_by in _STATS_GROUP_COLUMNS else "country"
    sort_col = sort if sort in _STATS_SORT_COLUMNS else "bottles"
    direction = "DESC" if desc else "ASC"

    rows = con.execute(f"""
        SELECT {col}                                           AS label,
               COUNT(DISTINCT wine_id)                         AS wines,
               SUM(bottles_stored)                             AS bottles,
               ROUND(SUM(bottles_stored * COALESCE(price, 0)), 0) AS value,
               ROUND(AVG(CASE WHEN price > 0 THEN price END), 0) AS avg_price
        FROM wines
        WHERE {col} IS NOT NULL
        GROUP BY {col}
        ORDER BY {sort_col} {direction}
    """).fetchall()
    return [{"label": r[0], "wines": r[1], "bottles": r[2], "value": r[3], "avg_price": r[4]} for r in rows]


# ---- Tracked wines --------------------------------------------------------


def get_tracked_wines(
    con: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """All tracked wines from the ``tracked_wines`` view.

    Returns an empty list if the view does not exist (no tracked wines
    configured).
    """
    try:
        rows = con.execute("""
            SELECT tracked_wine_id, wine_name, winery_name, category,
                   country, region, subregion, classification,
                   wine_count, bottles_stored, bottles_on_order
            FROM tracked_wines
            ORDER BY wine_name
        """).fetchall()
    except duckdb.CatalogException:
        return []
    return [
        {
            "tracked_wine_id": r[0],
            "wine_name": r[1],
            "winery_name": r[2],
            "category": r[3],
            "country": r[4],
            "region": r[5],
            "subregion": r[6],
            "classification": r[7],
            "wine_count": r[8],
            "bottles_stored": r[9],
            "bottles_on_order": r[10],
        }
        for r in rows
    ]


def get_tracked_wine_detail(
    con: duckdb.DuckDBPyConnection,
    tracked_wine_id: int,
) -> dict | None:
    """Single tracked wine record."""
    try:
        row = con.execute(
            "SELECT * FROM tracked_wines WHERE tracked_wine_id = ?",
            [tracked_wine_id],
        ).fetchone()
    except duckdb.CatalogException:
        return None
    if row is None:
        return None
    cols = [d[0] for d in con.description]
    return dict(zip(cols, row))


def get_price_chart_data(
    con: duckdb.DuckDBPyConnection,
    tracked_wine_id: int,
) -> list[dict]:
    """Monthly price aggregates for a tracked wine (Chart.js line chart)."""
    try:
        rows = con.execute(
            """
            SELECT month, retailer_name, min_price_chf, max_price_chf,
                   avg_price_chf, observations
            FROM price_history
            WHERE tracked_wine_id = ?
            ORDER BY month
        """,
            [tracked_wine_id],
        ).fetchall()
    except duckdb.CatalogException:
        return []
    return [
        {
            "month": str(r[0]),
            "retailer": r[1],
            "min_price": float(r[2]) if r[2] is not None else None,
            "max_price": float(r[3]) if r[3] is not None else None,
            "avg_price": float(r[4]) if r[4] is not None else None,
            "observations": r[5],
        }
        for r in rows
    ]


def get_price_observations(
    con: duckdb.DuckDBPyConnection,
    tracked_wine_id: int,
) -> list[dict]:
    """Raw price observations for a tracked wine."""
    try:
        rows = con.execute(
            """
            SELECT observation_id, wine_name, winery_name, vintage,
                   retailer_name, price, currency, price_chf, in_stock,
                   observed_at, observation_source
            FROM price_observations
            WHERE tracked_wine_id = ?
            ORDER BY observed_at DESC
        """,
            [tracked_wine_id],
        ).fetchall()
    except duckdb.CatalogException:
        return []
    return [
        {
            "observation_id": r[0],
            "wine_name": r[1],
            "winery_name": r[2],
            "vintage": r[3],
            "retailer": r[4],
            "price": float(r[5]) if r[5] is not None else None,
            "currency": r[6],
            "price_chf": float(r[7]) if r[7] is not None else None,
            "in_stock": r[8],
            "observed_at": str(r[9]) if r[9] else None,
            "observation_source": r[10],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Phase C/D/E/F: Interactive dashboard helpers
# ---------------------------------------------------------------------------


def get_pending_consumed_details(
    con: duckdb.DuckDBPyConnection,
    bottle_ids: list[int],
) -> list[dict]:
    """Look up bottle + wine display info for the consumed-pending sidecar."""
    if not bottle_ids:
        return []
    placeholders = ",".join("?" for _ in bottle_ids)
    rows = con.execute(
        f"""
        SELECT bottle_id, wine_id, wine_name, vintage, winery_name,
               cellar_name, shelf, status, is_in_transit
        FROM bottles_full
        WHERE bottle_id IN ({placeholders})
        ORDER BY wine_name, vintage
        """,
        bottle_ids,
    ).fetchall()
    return [
        {
            "bottle_id": r[0],
            "wine_id": r[1],
            "wine_name": r[2],
            "vintage": r[3],
            "winery_name": r[4],
            "cellar_name": r[5],
            "shelf": r[6],
            "status": r[7],
            "is_in_transit": r[8],
        }
        for r in rows
    ]


def get_wines_by_ids(
    con: duckdb.DuckDBPyConnection,
    wine_ids: list[int],
) -> list[dict]:
    """Display info for a list of wine IDs (used by the drink-tonight page)."""
    if not wine_ids:
        return []
    placeholders = ",".join("?" for _ in wine_ids)
    rows = con.execute(
        f"""
        SELECT wine_id, wine_name, vintage, winery_name, category,
               bottles_stored, drinking_status, drink_from, drink_until
        FROM wines_full
        WHERE wine_id IN ({placeholders})
        ORDER BY wine_name, vintage
        """,
        wine_ids,
    ).fetchall()
    return [
        {
            "wine_id": r[0],
            "wine_name": r[1],
            "vintage": r[2],
            "winery_name": r[3],
            "category": r[4],
            "bottles_stored": r[5],
            "drinking_status": r[6],
            "drink_from": r[7],
            "drink_until": r[8],
        }
        for r in rows
    ]


def get_drinking_window_dataset(
    con: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Rows for the floating-bar drinking-window timeline.

    Only includes wines with at least one stored bottle and a defined
    drink_from/drink_until window.
    """
    rows = con.execute(
        """
        SELECT wine_id, wine_name, vintage, winery_name, category,
               bottles_stored, drinking_status,
               drink_from, drink_until, optimal_from, optimal_until
        FROM wines_full
        WHERE bottles_stored > 0
          AND drink_from IS NOT NULL
          AND drink_until IS NOT NULL
        ORDER BY drink_from
        """
    ).fetchall()
    return [
        {
            "wine_id": r[0],
            "wine_name": r[1],
            "vintage": r[2],
            "winery_name": r[3],
            "category": r[4],
            "bottles_stored": r[5],
            "drinking_status": r[6],
            "drink_from": r[7],
            "drink_until": r[8],
            "optimal_from": r[9],
            "optimal_until": r[10],
        }
        for r in rows
    ]


def get_heatmap_layout(
    con: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Per-cellar / per-shelf grouping with drinking-status counts.

    Returns a list shaped as::

        [
            {
                "cellar_name": "Cave",
                "shelves": [
                    {
                        "shelf": "A1",
                        "bottles": [
                            {"wine_id": 1, "wine_name": "...",
                             "drinking_status": "optimal", "vintage": 2018},
                            ...
                        ],
                    },
                    ...
                ],
            },
            ...
        ]

    Bottles without a shelf are grouped under a virtual ``"(no shelf)"`` row.
    """
    rows = con.execute(
        """
        SELECT
            COALESCE(cellar_name, '(no cellar)') AS cellar_name,
            COALESCE(shelf, '(no shelf)') AS shelf,
            wine_id, wine_name, vintage, drinking_status
        FROM bottles_full
        WHERE status = 'stored' AND NOT is_in_transit
        ORDER BY cellar_name, shelf, wine_name, vintage
        """
    ).fetchall()

    layout: dict[str, dict[str, list[dict]]] = {}
    for r in rows:
        cellar = r[0]
        shelf = r[1]
        layout.setdefault(cellar, {}).setdefault(shelf, []).append(
            {
                "wine_id": r[2],
                "wine_name": r[3],
                "vintage": r[4],
                "drinking_status": r[5],
            }
        )
    return [
        {
            "cellar_name": cellar,
            "shelves": [{"shelf": shelf, "bottles": bottles} for shelf, bottles in shelves.items()],
        }
        for cellar, shelves in layout.items()
    ]


def get_consumption_velocity(
    con: duckdb.DuckDBPyConnection,
    months: int = 6,
) -> dict | None:
    """Consumption velocity data for the dashboard widget.

    Returns a dict with month-by-month acquired/consumed counts and summary
    metrics, or None if no data is available.
    """
    import datetime as _dt

    today = _dt.date.today()
    current_month_start = _dt.date(today.year, today.month, 1)

    if today.month - months >= 1:
        range_start = _dt.date(today.year, today.month - months, 1)
    else:
        years_back = (months - today.month) // 12 + 1
        m = today.month - months + 12 * years_back
        range_start = _dt.date(today.year - years_back, m, 1)

    try:
        rows = con.execute(f"""
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
        """).fetchall()
    except Exception:
        return None

    if not rows:
        return None

    month_labels = [r[0] for r in rows]
    acquired = [int(r[1]) for r in rows]
    consumed = [int(r[2]) for r in rows]

    num_months = len(rows)
    total_acquired = sum(acquired)
    total_consumed = sum(consumed)
    avg_acquired = round(total_acquired / num_months, 1)
    avg_consumed = round(total_consumed / num_months, 1)
    net_growth = round(avg_acquired - avg_consumed, 1)

    try:
        current_bottles = con.execute(
            "SELECT count(*) FROM bottles_full WHERE status = 'stored' AND NOT is_in_transit"
        ).fetchone()[0]
    except Exception:
        current_bottles = 0

    projected_12m = int(current_bottles + round(net_growth * 12))

    return {
        "labels": month_labels,
        "acquired": acquired,
        "consumed": consumed,
        "avg_acquired": avg_acquired,
        "avg_consumed": avg_consumed,
        "net_growth": net_growth,
        "current_bottles": int(current_bottles),
        "projected_12m": projected_12m,
    }
