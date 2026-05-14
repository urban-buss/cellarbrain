---
name: cellar-stats
description: "Cellar overview: bottle counts, value, regions, aging potential, anomalies."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Cellar Statistics

Overview and analysis of cellar composition.

## Owner Context

Switzerland, CHF. Currency conversion automatic (EUR and USD inputs normalised to CHF).

## Monthly Summary Template

Use these steps in order when asked for a cellar overview / monthly summary:

### 1. Core Stats

`cellar_stats()` -- bottles, value, avg price, category/region splits.

### 2. Bottle Counts by Category

`sql
SELECT category, COUNT(*) as bottles, SUM(purchase_price_chf) as total_chf
FROM wines_full WHERE bottles_stored > 0
GROUP BY category ORDER BY bottles DESC
`

### 3. Regional Distribution

`sql
SELECT country, region, COUNT(*) as bottles
FROM wines_full WHERE bottles_stored > 0
GROUP BY country, region ORDER BY bottles DESC LIMIT 15
`

### 4. Aging Potential

`sql
SELECT drinking_status, COUNT(*) as bottles
FROM wines_full WHERE bottles_stored > 0
GROUP BY drinking_status
`

### 5. Recent Additions (last 30 days)

`sql
SELECT wine_id, winery_name, wine_name, vintage, purchase_price_chf, date_added
FROM wines_full WHERE date_added >= CURRENT_DATE - INTERVAL 30 DAY
  AND bottles_stored > 0
ORDER BY date_added DESC LIMIT 10
`

### 6. Anomaly Check

`cellar_anomalies()` -- flags wines with missing data, unusual values, stale dossiers.

### 7. Health Check (optional, on request)

`cache_stats()` -- search/query cache hit rates.
`search_stats()` -- index coverage and query patterns.

## Ad-Hoc Queries

For specific questions ("how many Burgundy?", "total value of Pinot?"), build a SQL query from the `wines_full` view.

## Tools

| Tool | Purpose |
|------|---------|
| `cellar_info` | Version, freshness, inventory counts |
| `cellar_stats` | Summary or grouped breakdown (10 dimensions) |
| `cellar_churn` | Monthly/yearly roll-forward (begin + purchased − consumed = end) |
| `query_cellar` | Custom SQL for specific questions |
| `cellar_anomalies` | Data quality flags |
| `cache_stats` | Cache hit/miss rates |
| `search_stats` | Search index coverage |

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.

