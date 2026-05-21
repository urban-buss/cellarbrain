---
name: cellar-gaps
description: "Identify underrepresented cellar categories: depleted regions, undrinkable grapes, price tier gaps, vintage holes."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Cellar Gaps

Identify underrepresented categories in the cellar — regions, grapes, price tiers, and vintages that need attention or restocking.

## Owner Context

Switzerland, CHF. Focus on actionable buying intelligence and drinking-window awareness.

## When to Use

- "What gaps do I have in my cellar?"
- "Which regions am I running low on?"
- "Do I have anything ready to drink in budget range?"
- "Any grape varieties I can't drink right now?"
- "Cellar coverage check"
- "What should I buy next?"
- "Purchase intelligence"
- "Am I missing any vintages?"
- "Cellar balance analysis"

## Quick Path

`cellar_gaps()` — all four dimensions at once. Returns region, grape, price tier, and vintage gaps in a single response.

## Detailed Analysis

### 1. Region Gaps

`cellar_gaps(dimension="region", months=12)`

Identifies regions where the owner has been consuming bottles but has 0-1 remaining. Indicates buying opportunities to replenish favourites.

Interpretation:
- **High consumed + 0 stored** → urgent replenishment needed
- **Moderate consumed + 1 stored** → plan ahead, last bottle warning

### 2. Grape Gaps

`cellar_gaps(dimension="grape")`

Grape varieties present in the cellar but with no bottles in optimal/drinkable window for the next 12 months. All bottles are too young.

Interpretation:
- Large count of too-young bottles → patience needed, or buy ready-to-drink alternatives
- Small count → low priority, variety will become available naturally

### 3. Price Tier Gaps

`cellar_gaps(dimension="price_tier")`

Price tiers with stored bottles but zero ready-to-drink options tonight. Example: "You have 12 premium reds but none are ready — all too young."

Interpretation:
- **Budget/everyday gap** → can't open a casual bottle tonight without opening something expensive
- **Premium/fine gap** → special occasion wines all need more aging

### 4. Vintage Gaps

`cellar_gaps(dimension="vintage")`

For aging-worthy categories (Red wine, Fortified wine), identifies entire decades with zero bottles. Helps identify collection diversity gaps.

### 5. Custom Lookback

`cellar_gaps(months=6)` — shorter consumption window (recent habits only)
`cellar_gaps(months=24)` — longer window (broader consumption pattern)

## Month-by-Month Analysis Template

For temporal gap tracking — understanding how gaps evolve month-by-month — use `query_cellar` with these SQL templates:

### Monthly Ready-to-Drink Coverage by Price Tier

```sql
SELECT
    price_tier,
    count(*) FILTER (WHERE drinking_status IN ('optimal', 'drinkable')) AS ready_now,
    count(*) FILTER (WHERE drinking_status = 'too_young'
        AND drink_from IS NOT NULL AND drink_from <= EXTRACT(YEAR FROM CURRENT_DATE) + 1) AS ready_within_12m,
    count(*) FILTER (WHERE drinking_status = 'too_young'
        AND (drink_from IS NULL OR drink_from > EXTRACT(YEAR FROM CURRENT_DATE) + 1)) AS aging
FROM wines_full
WHERE bottles_stored > 0
  AND price_tier IS NOT NULL AND price_tier != 'unknown'
GROUP BY price_tier
ORDER BY price_tier
```

### Monthly Consumption by Region (Trend)

```sql
SELECT
    strftime(b.output_date, '%Y-%m') AS month,
    w.region,
    count(*) AS consumed
FROM bottles_full b
JOIN wines_full w ON b.wine_id = w.wine_id
WHERE b.output_date >= CURRENT_DATE - INTERVAL 12 MONTH
  AND w.region IS NOT NULL
GROUP BY month, w.region
ORDER BY month, consumed DESC
```

### Projected Depletion by Region

```sql
WITH stored AS (
    SELECT region, CAST(sum(bottles_stored) AS BIGINT) AS bottles
    FROM wines_full
    WHERE bottles_stored > 0 AND region IS NOT NULL
    GROUP BY region
),
velocity AS (
    SELECT w.region, count(*) AS consumed_12m
    FROM bottles_full b
    JOIN wines_full w ON b.wine_id = w.wine_id
    WHERE b.output_date >= CURRENT_DATE - INTERVAL 12 MONTH
      AND w.region IS NOT NULL
    GROUP BY w.region
)
SELECT s.region,
       s.bottles AS current_stock,
       COALESCE(v.consumed_12m, 0) AS consumed_12m,
       CASE WHEN COALESCE(v.consumed_12m, 0) > 0
            THEN ROUND(s.bottles * 12.0 / v.consumed_12m, 1)
            ELSE NULL
       END AS months_until_depleted
FROM stored s
LEFT JOIN velocity v ON s.region = v.region
ORDER BY months_until_depleted ASC NULLS LAST
```

### Grape Readiness Timeline

```sql
SELECT
    primary_grape,
    count(*) FILTER (WHERE drinking_status IN ('optimal', 'drinkable')) AS ready_now,
    count(*) FILTER (WHERE drink_from IS NOT NULL AND drink_from = EXTRACT(YEAR FROM CURRENT_DATE) + 1) AS ready_next_year,
    count(*) FILTER (WHERE drink_from IS NOT NULL AND drink_from > EXTRACT(YEAR FROM CURRENT_DATE) + 1) AS aging_longer,
    count(*) AS total
FROM wines_full
WHERE bottles_stored > 0
  AND primary_grape IS NOT NULL AND primary_grape != ''
GROUP BY primary_grape
ORDER BY ready_now ASC, total DESC
```

## Presentation

Summarise gaps as actionable recommendations:
- "You've been drinking Burgundy regularly but only have 1 bottle left — consider restocking."
- "All 8 of your Nebbiolo bottles are too young to drink until 2028."
- "You have no budget-friendly wines ready for a casual weeknight."
- "Your cellar skips the 1990s decade entirely for aging reds."

Offer follow-up: `find_wine` for alternatives, `recommend_tonight` for what IS ready, or `consumption_velocity` for broader trends.

## Tools

| Tool | Purpose |
|------|---------|
| `cellar_gaps` | Primary gap identification (4 dimensions) |
| `query_cellar` | Custom SQL for temporal gap drill-downs |
| `consumption_velocity` | Broader acquisition vs consumption context |
| `cellar_stats` | Overall cellar composition for comparison |
| `recommend_tonight` | What IS ready to drink right now |
| `find_wine` | Search for alternatives to fill gaps |
