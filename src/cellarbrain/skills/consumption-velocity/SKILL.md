---
name: consumption-velocity
description: "Analyse cellar growth rate: month-by-month acquisition vs consumption, net growth, and 12-month projection."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Consumption Velocity

Analyse how fast the cellar is growing or shrinking — bottles acquired vs consumed per month.

## Owner Context

Switzerland, CHF. Uses `bottles_full` view (purchase_date for acquisitions, output_date for consumption).

## When to Use

- "How fast is my cellar growing?"
- "Am I drinking faster than buying?"
- "Cellar health check"
- "Monthly consumption report"
- "How many bottles will I have next year?"
- "Acquisition vs consumption rate"

## Quick Path

`consumption_velocity(months=6)` — last 6 months, returns per-month acquired/consumed, averages, net growth, 12-month projection.

## Detailed Analysis

### 1. Overall Velocity

`consumption_velocity(months=6)` — default lookback.

Interpret the result:
- **Net positive**: cellar is growing (acquiring more than consuming)
- **Net negative**: cellar is shrinking (consuming more than acquiring)
- **Near zero**: cellar is stable

### 2. Extended Lookback

`consumption_velocity(months=12)` — full year for seasonal patterns.
`consumption_velocity(months=24)` — two years for long-term trends.

### 3. Category-Specific Velocity (SQL drill-down)

```sql
SELECT
    w.category,
    count(*) FILTER (WHERE b.purchase_date >= CURRENT_DATE - INTERVAL 6 MONTH) AS acquired_6m,
    count(*) FILTER (WHERE b.output_date >= CURRENT_DATE - INTERVAL 6 MONTH) AS consumed_6m,
    count(*) FILTER (WHERE b.purchase_date >= CURRENT_DATE - INTERVAL 6 MONTH)
      - count(*) FILTER (WHERE b.output_date >= CURRENT_DATE - INTERVAL 6 MONTH) AS net_6m
FROM bottles_full b
JOIN wines_full w ON b.wine_id = w.wine_id
GROUP BY w.category
ORDER BY net_6m DESC
```

### 4. Regional Velocity

```sql
SELECT
    w.country, w.region,
    count(*) FILTER (WHERE b.purchase_date >= CURRENT_DATE - INTERVAL 6 MONTH) AS acquired_6m,
    count(*) FILTER (WHERE b.output_date >= CURRENT_DATE - INTERVAL 6 MONTH) AS consumed_6m
FROM bottles_full b
JOIN wines_full w ON b.wine_id = w.wine_id
GROUP BY w.country, w.region
HAVING acquired_6m + consumed_6m > 0
ORDER BY consumed_6m DESC
LIMIT 10
```

### 5. Price Tier Velocity

```sql
SELECT
    w.price_tier,
    count(*) FILTER (WHERE b.purchase_date >= CURRENT_DATE - INTERVAL 6 MONTH) AS acquired_6m,
    count(*) FILTER (WHERE b.output_date >= CURRENT_DATE - INTERVAL 6 MONTH) AS consumed_6m,
    count(*) FILTER (WHERE b.status = 'stored' AND NOT b.is_in_transit) AS current_stock
FROM bottles_full b
JOIN wines_full w ON b.wine_id = w.wine_id
GROUP BY w.price_tier
ORDER BY w.price_tier
```

### 6. Monthly Churn Detail

For detailed month-by-month inventory roll-forward (beginning → purchased → consumed → ending):

`cellar_churn(period="month")` — current year month-by-month
`cellar_churn(period="month", year=2025)` — specific year
`cellar_churn(period="year")` — year-by-year all-time

### 7. Seasonal Patterns

```sql
SELECT
    EXTRACT(MONTH FROM b.output_date) AS month_num,
    MONTHNAME(b.output_date) AS month_name,
    count(*) AS bottles_consumed,
    ROUND(count(*) * 1.0 / COUNT(DISTINCT EXTRACT(YEAR FROM b.output_date)), 1) AS avg_per_year
FROM bottles_full b
WHERE b.output_date IS NOT NULL
  AND b.output_date >= CURRENT_DATE - INTERVAL 3 YEAR
GROUP BY month_num, month_name
ORDER BY month_num
```

## Presentation

Start with the summary: "Your cellar is [growing/shrinking/stable] at [net] bottles/month."

Then show:
- Average acquired vs consumed rates
- Current size and 12-month projection
- Offer drill-down by category or region if user wants detail

Flag imbalances: if net growth > 5/month, note accumulation; if consuming from one category without replenishing, note depletion risk.

## Tools

| Tool | Purpose |
|------|---------|
| `consumption_velocity` | Primary velocity analysis (rates + projection) |
| `cellar_churn` | Detailed roll-forward by period |
| `query_cellar` | Custom SQL for category/region/price drill-downs |
| `cellar_stats` | Current inventory breakdown |
