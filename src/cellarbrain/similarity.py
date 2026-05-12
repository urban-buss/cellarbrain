"""Wine similarity engine — weighted multi-signal scoring.

Computes structural similarity between wines using six signals:
winery, appellation/region, grape composition, category, price tier,
and food group overlap.  All computation happens in DuckDB via a
single CTE-based query — no materialisation needed for ~500 wines.
"""

from __future__ import annotations

import logging
import pathlib

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Scoring weights (hardcoded; add SimilarityConfig later if tuning needed)
# ---------------------------------------------------------------------------

W_WINERY = 0.30
W_REGION = 0.25
W_GRAPE = 0.20
W_CATEGORY = 0.10
W_PRICE = 0.10
W_FOOD_GROUPS = 0.05
MIN_SCORE = 0.15
MAX_SAME_WINERY = 2  # Diversity cap: max results from same winery


def _build_similarity_sql(wine_grape_path: str, *, include_gone: bool) -> str:
    """Build the parameterised similarity SQL query.

    Parameters $1 = wine_id, $2 = limit.
    """
    stored_filter = "" if include_gone else "AND w.bottles_stored > 0"

    return f"""\
WITH target AS (
    SELECT
        wine_id, winery_name, category,
        primary_grape, price_tier, food_groups, format_group_id,
        country, region, subregion
    FROM wines_full
    WHERE wine_id = $1
),
target_grapes AS (
    SELECT grape_id
    FROM read_parquet('{wine_grape_path}')
    WHERE wine_id = $1
),
candidate_grape_overlap AS (
    SELECT
        wg.wine_id,
        COUNT(*) AS shared_grapes
    FROM read_parquet('{wine_grape_path}') wg
    INNER JOIN target_grapes tg ON wg.grape_id = tg.grape_id
    WHERE wg.wine_id != $1
    GROUP BY wg.wine_id
),
target_grape_count AS (
    SELECT COUNT(*) AS cnt FROM target_grapes
),
candidate_grape_counts AS (
    SELECT wine_id, COUNT(*) AS cnt
    FROM read_parquet('{wine_grape_path}')
    WHERE wine_id != $1
    GROUP BY wine_id
),
scored AS (
    SELECT
        w.wine_id,
        w.wine_name,
        w.vintage,
        w.winery_name,
        w.country,
        w.region,
        w.category,
        w.primary_grape,
        w.price_tier,
        w.drinking_status,
        w.bottles_stored,
        -- Signal 1: Same winery (by name)
        CASE WHEN w.winery_name = t.winery_name AND t.winery_name IS NOT NULL
             THEN {W_WINERY} ELSE 0 END AS s_winery,
        -- Signal 2: Region match (graduated: subregion > region > country)
        CASE
            WHEN w.subregion = t.subregion AND t.subregion IS NOT NULL
                 AND w.region = t.region
                THEN {W_REGION}
            WHEN w.region = t.region AND t.region IS NOT NULL AND w.region IS NOT NULL
                THEN {W_REGION * 0.6}
            WHEN w.country = t.country AND t.country IS NOT NULL AND w.country IS NOT NULL
                THEN {W_REGION * 0.4}
            ELSE 0
        END AS s_region,
        -- Signal 3: Grape affinity (primary_grape + junction overlap)
        CASE
            WHEN w.primary_grape = t.primary_grape AND t.primary_grape IS NOT NULL
                THEN 0.15
            ELSE 0
        END
        + COALESCE(
            cgo.shared_grapes * 0.05 / GREATEST(tgc.cnt, cgc.cnt, 1),
            0
        ) AS s_grape,
        -- Signal 4: Same category
        CASE WHEN w.category = t.category THEN {W_CATEGORY} ELSE 0 END AS s_category,
        -- Signal 5: Price tier adjacency
        CASE
            WHEN w.price_tier = t.price_tier AND t.price_tier != 'unknown'
                THEN {W_PRICE}
            WHEN ABS(
                CASE w.price_tier
                    WHEN 'budget' THEN 0 WHEN 'everyday' THEN 1
                    WHEN 'premium' THEN 2 WHEN 'fine' THEN 3 ELSE -99
                END
                - CASE t.price_tier
                    WHEN 'budget' THEN 0 WHEN 'everyday' THEN 1
                    WHEN 'premium' THEN 2 WHEN 'fine' THEN 3 ELSE -99
                END
            ) = 1 THEN {W_PRICE * 0.5}
            ELSE 0
        END AS s_price,
        -- Signal 6: Food group overlap (Jaccard-like)
        CASE
            WHEN w.food_groups IS NOT NULL AND t.food_groups IS NOT NULL
                 AND len(w.food_groups) > 0 AND len(t.food_groups) > 0
            THEN len(list_intersect(w.food_groups, t.food_groups)) * {W_FOOD_GROUPS}
                 / GREATEST(len(w.food_groups), len(t.food_groups))
            ELSE 0
        END AS s_food
    FROM wines_full w
    CROSS JOIN target t
    LEFT JOIN candidate_grape_overlap cgo ON w.wine_id = cgo.wine_id
    CROSS JOIN target_grape_count tgc
    LEFT JOIN candidate_grape_counts cgc ON w.wine_id = cgc.wine_id
    WHERE w.wine_id != $1
      AND (t.format_group_id IS NULL OR w.format_group_id IS NULL
           OR w.format_group_id != t.format_group_id)
      {stored_filter}
),
ranked AS (
    SELECT
        *,
        ROUND(s_winery + s_region + s_grape + s_category + s_price + s_food, 3) AS similarity_score,
        ROW_NUMBER() OVER (
            PARTITION BY winery_name
            ORDER BY (s_winery + s_region + s_grape + s_category + s_price + s_food) DESC
        ) AS winery_rank
    FROM scored
    WHERE (s_winery + s_region + s_grape + s_category + s_price + s_food) >= {MIN_SCORE}
)
SELECT
    wine_id,
    wine_name,
    vintage,
    winery_name,
    country,
    region,
    primary_grape,
    price_tier,
    drinking_status,
    bottles_stored,
    similarity_score,
    CONCAT_WS(', ',
        CASE WHEN s_winery > 0 THEN 'same winery' END,
        CASE WHEN s_region >= {W_REGION} THEN 'same subregion'
             WHEN s_region >= {W_REGION * 0.6} THEN 'same region'
             WHEN s_region > 0 THEN 'same country' END,
        CASE WHEN s_grape >= 0.15 THEN 'same grape'
             WHEN s_grape > 0 THEN 'shared grapes' END,
        CASE WHEN s_category > 0 THEN 'same category' END,
        CASE WHEN s_price >= {W_PRICE} THEN 'same price tier'
             WHEN s_price > 0 THEN 'adjacent price' END,
        CASE WHEN s_food > 0 THEN 'food group overlap' END
    ) AS match_signals
FROM ranked
WHERE winery_rank <= {MAX_SAME_WINERY}
ORDER BY similarity_score DESC, wine_name
LIMIT $2
"""


def similar_wines(
    con: duckdb.DuckDBPyConnection,
    wine_id: int,
    data_dir: str | pathlib.Path,
    *,
    limit: int = 5,
    include_gone: bool = False,
) -> str:
    """Return top-N similar wines to *wine_id*, ranked by weighted score.

    Uses a 6-signal scoring model (winery, region, grape, category,
    price tier, food groups) computed entirely in DuckDB.

    Args:
        con: DuckDB connection with wines_full view registered.
        wine_id: Target wine to find similar wines for.
        data_dir: Path to the Parquet data directory (for wine_grape.parquet).
        limit: Maximum results to return.
        include_gone: If True, include wines with zero bottles stored.

    Returns:
        Formatted Markdown table with similarity scores and match signals,
        or an error/empty message.
    """
    d = pathlib.Path(data_dir)
    wine_grape_path = str(d / "wine_grape.parquet").replace("\\", "/")

    # Verify target wine exists
    row = con.execute(
        "SELECT wine_name, vintage FROM wines_full WHERE wine_id = $1",
        [wine_id],
    ).fetchone()
    if row is None:
        return f"Error: Wine {wine_id} not found."

    target_name = row[0]
    target_vintage = row[1]

    sql = _build_similarity_sql(wine_grape_path, include_gone=include_gone)
    logger.debug("similar_wines wine_id=%d limit=%d include_gone=%s", wine_id, limit, include_gone)

    results = con.execute(sql, [wine_id, limit]).fetchall()

    if not results:
        return f"No similar wines found for wine #{wine_id} (minimum score threshold: {MIN_SCORE})."

    # Format header
    vintage_str = f" {target_vintage}" if target_vintage else ""
    header = f"**Similar to:** {target_name}{vintage_str} (wine #{wine_id})\n\n"

    # Build Markdown table
    lines = [
        "| # | Wine | Vintage | Score | Signals |",
        "|---|------|---------|-------|---------|",
    ]
    for i, r in enumerate(results, 1):
        # r: wine_id, wine_name, vintage, winery_name, country, region,
        #    primary_grape, price_tier, drinking_status, bottles_stored,
        #    similarity_score, match_signals
        r_wine_name = r[1] or "—"
        r_vintage = r[2] if r[2] else "NV"
        r_score = f"{r[10]:.2f}"
        r_signals = r[11] or ""
        lines.append(f"| {i} | {r_wine_name} | {r_vintage} | {r_score} | {r_signals} |")

    return header + "\n".join(lines)
