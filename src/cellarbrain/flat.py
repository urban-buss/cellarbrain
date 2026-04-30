"""View SQL definitions for ``wines`` and ``bottles`` DuckDB views.

These SQL templates define the canonical column lists and JOIN structure
for the denormalised views exposed to agents.  ``query.py`` substitutes
table-name placeholders with ``read_parquet(...)`` paths when building
DuckDB connections.

Two layers:

- ``*_FULL_*`` views contain **all** columns — used internally by
  ``cellar_stats``, ``cellar_churn``, and available to agents via
  ``wines_full`` / ``bottles_full`` for deep analysis.
- Slim ``WINES_VIEW_SQL`` / ``BOTTLES_VIEW_SQL`` select a curated subset
  of ~19 / ~17 columns from the full views — the default for agents.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Full views — all columns (internal + advanced agent queries)
# ---------------------------------------------------------------------------

WINES_FULL_VIEW_SQL = """\
SELECT
    w.wine_id,
    wy.name AS winery_name,
    w.full_name AS wine_name,
    w.vintage,
    a.country,
    a.region,
    a.subregion,
    a.classification,
    w.category,
    w.subcategory,
    w.specialty,
    w.sweetness,
    w.effervescence,
    w.grape_type AS blend_type,
    w.primary_grape,
    w.grape_summary AS grapes,
    w.volume_ml,
    w.container,
    w.hue,
    w.cork,
    w.alcohol_pct,
    w.acidity_g_l,
    w.sugar_g_l,
    w.ageing_type,
    w.ageing_months,
    w.farming_type,
    w.serving_temp_c,
    w.opening_type,
    w.opening_minutes,
    w.drink_from,
    w.drink_until,
    w.optimal_from,
    w.optimal_until,
    w.drinking_status,
    w.age_years,
    w.list_price AS price,
    w.price_tier,
    w.bottle_format,
    w.price_per_750ml,
    w.format_group_id,
    w.food_tags,
    w.food_groups,
    w.comment,
    w.winemaking_notes,
    w.is_favorite,
    w.is_wishlist,
    w.tracked_wine_id,
    -- Bottle aggregates
    COALESCE(bs.bottles_stored, 0)   AS bottles_stored,
    COALESCE(bs.bottles_consumed, 0) AS bottles_consumed,
    COALESCE(bs.bottles_on_order, 0) AS bottles_on_order,
    bs.cellar_value,
    bs.on_order_value,
    -- Tasting aggregates
    COALESCE(ts.tasting_count, 0) AS tasting_count,
    ts.last_tasting_date,
    ts.last_tasting_score,
    ts.avg_tasting_score,
    -- Pro rating aggregates
    COALESCE(pr.pro_rating_count, 0) AS pro_rating_count,
    pr.best_pro_score,
    pr.avg_pro_score
FROM wine w
LEFT JOIN winery wy ON w.winery_id = wy.winery_id
LEFT JOIN appellation a ON w.appellation_id = a.appellation_id
LEFT JOIN (
    SELECT
        wine_id,
        count(*) FILTER (WHERE status = 'stored' AND NOT is_in_transit)  AS bottles_stored,
        count(*) FILTER (WHERE status != 'stored') AS bottles_consumed,
        count(*) FILTER (WHERE status = 'stored' AND is_in_transit) AS bottles_on_order,
        CAST(sum(purchase_price) FILTER (WHERE status = 'stored' AND NOT is_in_transit) AS DOUBLE) AS cellar_value,
        CAST(sum(purchase_price) FILTER (WHERE status = 'stored' AND is_in_transit) AS DOUBLE) AS on_order_value
    FROM bottle
    GROUP BY wine_id
) bs ON w.wine_id = bs.wine_id
LEFT JOIN (
    SELECT
        wine_id,
        count(*)       AS tasting_count,
        max(tasting_date) AS last_tasting_date,
        arg_max(score, tasting_date) AS last_tasting_score,
        CAST(avg(score) AS FLOAT)    AS avg_tasting_score
    FROM tasting
    GROUP BY wine_id
) ts ON w.wine_id = ts.wine_id
LEFT JOIN (
    SELECT
        wine_id,
        count(*)                  AS pro_rating_count,
        CAST(max(score) AS FLOAT) AS best_pro_score,
        CAST(avg(score) AS FLOAT) AS avg_pro_score
    FROM pro_rating
    GROUP BY wine_id
) pr ON w.wine_id = pr.wine_id
WHERE NOT w.is_deleted
ORDER BY w.wine_id
"""

# Slim wines view — 23 agent-friendly columns from wines_full.
WINES_VIEW_SQL = """\
SELECT wine_id, wine_name, vintage, winery_name,
       category, country, region, subregion,
       primary_grape, blend_type,
       drinking_status, price_tier, price, price_per_750ml,
       volume_ml, bottle_format, format_group_id,
       CONCAT_WS(', ',
           NULLIF(subcategory, ''),
           NULLIF(sweetness, ''),
           NULLIF(effervescence, ''),
           NULLIF(specialty, '')
       ) AS style_tags,
       bottles_stored, bottles_on_order, bottles_consumed,
       is_favorite, is_wishlist,
       tracked_wine_id
FROM wines_full
"""

BOTTLES_FULL_VIEW_SQL = """\
SELECT
    b.bottle_id,
    b.wine_id,
    w.full_name AS wine_name,
    w.vintage,
    wy.name     AS winery_name,
    a.country,
    a.region,
    a.subregion,
    a.classification,
    w.category,
    w.subcategory,
    w.grape_type AS blend_type,
    w.primary_grape,
    w.grape_summary AS grapes,
    w.alcohol_pct,
    w.drink_from,
    w.drink_until,
    w.optimal_from,
    w.optimal_until,
    w.drinking_status,
    w.age_years,
    w.price_tier,
    w.bottle_format,
    w.price_per_750ml AS list_price_per_750ml,
    w.volume_ml,
    w.is_favorite,
    b.status,
    c.name      AS cellar_name,
    b.shelf,
    p.name      AS provider_name,
    b.purchase_date,
    b.acquisition_type,
    b.purchase_price AS price,
    CASE WHEN w.volume_ml > 0
         THEN ROUND(b.purchase_price * 750.0 / w.volume_ml, 2)
         ELSE NULL
    END AS price_per_750ml,
    b.purchase_comment,
    b.output_date,
    b.output_type,
    b.output_comment,
    b.is_onsite,
    b.is_in_transit
FROM bottle b
JOIN wine w ON b.wine_id = w.wine_id
LEFT JOIN winery wy ON w.winery_id = wy.winery_id
LEFT JOIN appellation a ON w.appellation_id = a.appellation_id
LEFT JOIN cellar c ON b.cellar_id = c.cellar_id
LEFT JOIN provider p ON b.provider_id = p.provider_id
WHERE NOT w.is_deleted
ORDER BY b.bottle_id
"""

# Slim bottles view — 20 agent-friendly columns from bottles_full.
BOTTLES_VIEW_SQL = """\
SELECT bottle_id, wine_id, wine_name, vintage, winery_name, category,
       country, region, primary_grape, drinking_status, price_tier, price,
       price_per_750ml, volume_ml, bottle_format,
       status, cellar_name, shelf,
       output_date, output_type
FROM bottles_full
"""

TRACKED_WINES_VIEW_SQL = """\
SELECT
    tw.tracked_wine_id,
    wy.name AS winery_name,
    tw.wine_name,
    tw.category,
    a.country,
    a.region,
    a.subregion,
    a.classification,
    -- Wine aggregates
    COALESCE(ws.wine_count, 0) AS wine_count,
    ws.vintages,
    COALESCE(ws.bottles_stored, 0) AS bottles_stored,
    COALESCE(ws.bottles_on_order, 0) AS bottles_on_order
FROM tracked_wine tw
LEFT JOIN winery wy ON tw.winery_id = wy.winery_id
LEFT JOIN appellation a ON tw.appellation_id = a.appellation_id
LEFT JOIN (
    SELECT
        tracked_wine_id,
        count(*) AS wine_count,
        list_sort(list(vintage) FILTER (WHERE vintage IS NOT NULL)) AS vintages,
        COALESCE(sum(bs.bottles_stored), 0) AS bottles_stored,
        COALESCE(sum(bs.bottles_on_order), 0) AS bottles_on_order
    FROM wine w2
    LEFT JOIN (
        SELECT wine_id,
               count(*) FILTER (WHERE NOT is_in_transit) AS bottles_stored,
               count(*) FILTER (WHERE is_in_transit) AS bottles_on_order
        FROM bottle
        WHERE status = 'stored'
        GROUP BY wine_id
    ) bs ON w2.wine_id = bs.wine_id
    WHERE NOT w2.is_deleted AND w2.tracked_wine_id IS NOT NULL
    GROUP BY tracked_wine_id
) ws ON tw.tracked_wine_id = ws.tracked_wine_id
WHERE NOT tw.is_deleted
ORDER BY tw.tracked_wine_id
"""

WINES_WISHLIST_VIEW_SQL = """\
SELECT * FROM wines
WHERE is_wishlist OR is_favorite
ORDER BY winery_name, wine_name, vintage
"""

# ---------------------------------------------------------------------------
# Price observation views
# ---------------------------------------------------------------------------

PRICE_OBSERVATIONS_VIEW_SQL = """\
SELECT
    po.observation_id,
    po.tracked_wine_id,
    tw.wine_name,
    wy.name AS winery_name,
    po.vintage,
    po.bottle_size_ml,
    po.retailer_name,
    po.retailer_url,
    po.price,
    po.currency,
    po.price_chf,
    ROUND(po.price_chf * 750.0 / po.bottle_size_ml, 2) AS price_per_750ml,
    po.in_stock,
    po.observed_at,
    po.observation_source,
    po.notes
FROM price_observation po
JOIN tracked_wine tw ON po.tracked_wine_id = tw.tracked_wine_id
LEFT JOIN winery wy ON tw.winery_id = wy.winery_id
ORDER BY po.observed_at DESC
"""

LATEST_PRICES_VIEW_SQL = """\
SELECT po.*,
       ROUND(po.price_chf * 750.0 / po.bottle_size_ml, 2) AS price_per_750ml
FROM price_observation po
INNER JOIN (
    SELECT tracked_wine_id, vintage, bottle_size_ml, retailer_name,
           MAX(observed_at) AS max_at
    FROM price_observation WHERE in_stock
    GROUP BY tracked_wine_id, vintage, bottle_size_ml, retailer_name
) latest ON po.tracked_wine_id = latest.tracked_wine_id
        AND po.vintage IS NOT DISTINCT FROM latest.vintage
        AND po.bottle_size_ml = latest.bottle_size_ml
        AND po.retailer_name = latest.retailer_name
        AND po.observed_at = latest.max_at
WHERE po.in_stock
"""

PRICE_HISTORY_VIEW_SQL = """\
SELECT
    po.tracked_wine_id,
    po.vintage,
    po.bottle_size_ml,
    po.retailer_name,
    DATE_TRUNC('month', po.observed_at) AS month,
    CAST(MIN(po.price_chf) AS DOUBLE) AS min_price_chf,
    CAST(MAX(po.price_chf) AS DOUBLE) AS max_price_chf,
    CAST(AVG(po.price_chf) AS DOUBLE) AS avg_price_chf,
    count(*) AS observations
FROM price_observation po
WHERE po.price_chf IS NOT NULL
GROUP BY po.tracked_wine_id, po.vintage, po.bottle_size_ml,
         po.retailer_name, DATE_TRUNC('month', po.observed_at)
ORDER BY month DESC
"""
