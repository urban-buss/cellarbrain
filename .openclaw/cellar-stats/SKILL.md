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

```sql
SELECT category, COUNT(*) AS bottles, ROUND(SUM(price), 2) AS total_chf
FROM bottles_stored
GROUP BY category ORDER BY bottles DESC
```

### 3. Regional Distribution

```sql
SELECT country, region, COUNT(*) as bottles
FROM bottles_stored
GROUP BY country, region ORDER BY bottles DESC LIMIT 15
```

### 4. Aging Potential

```sql
SELECT drinking_status, COUNT(*) as bottles
FROM bottles_stored
GROUP BY drinking_status
```

### 5. Recent Additions (last 30 days)

```sql
SELECT wine_id, winery_name, wine_name, vintage, price, purchase_date,
       cellar_name, is_in_transit
FROM bottles_full
WHERE purchase_date >= CURRENT_DATE - INTERVAL 30 DAY
  AND status = 'stored'
ORDER BY purchase_date DESC LIMIT 10
```

### 6. Anomaly Check

`cellar_anomalies()` -- flags operational anomalies in MCP usage, latency, errors, and ETL output.

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

Pass `format="plain"` only when the tool schema exposes a `format` parameter. Never add unsupported arguments. Present the final answer as plain text for iMessage.
