---
name: drinking-window
description: "Find wines approaching or past their optimal drinking window. Urgency-sorted."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Drinking Window

Find wines that need attention -- past optimal or approaching peak.

## Owner Context

Switzerland. Show storage location so user can find the bottle.

## Quick Path

`recommend_tonight(occasion="solo", limit=15)` -- urgency-weighted picks. Fastest overview of what to drink next.

## Detailed Path

### 1. Past Optimal -- Drink Now

```sql
SELECT wine_id, winery_name, wine_name, vintage, optimal_until,
       COUNT(*) AS bottles_stored,
       string_agg(DISTINCT cellar_name, ', ') AS locations
FROM bottles_full
WHERE status = 'stored' AND NOT is_in_transit
  AND drinking_status = 'past_optimal'
GROUP BY wine_id, winery_name, wine_name, vintage, optimal_until
ORDER BY optimal_until ASC
LIMIT 15
```

### 2. Approaching Peak -- Plan Ahead

```sql
SELECT wine_id, winery_name, wine_name, vintage, optimal_from, optimal_until,
       COUNT(*) AS bottles_stored,
       string_agg(DISTINCT cellar_name, ', ') AS locations
FROM bottles_full
WHERE status = 'stored' AND NOT is_in_transit
  AND drinking_status = 'drinkable'
  AND optimal_from <= EXTRACT(YEAR FROM CURRENT_DATE) + 1
GROUP BY wine_id, winery_name, wine_name, vintage, optimal_from, optimal_until
ORDER BY optimal_from ASC
LIMIT 15
```

### 3. Present

| Priority | Wine | Vintage | Window | Bottles | Location |
|----------|------|---------|--------|---------|----------|
| Past | ... | ... | ended YYYY | ... | ... |
| Soon | ... | ... | starts YYYY | ... | ... |

Offer `read_dossier(wine_id)` for details or `similar_wines(wine_id)` for alternatives.

## Tools

| Tool | Purpose |
|------|---------|
| `query_cellar` | SQL queries for drinking status |
| `read_dossier` | Details on specific wines |
| `recommend_tonight` | Quick urgency-sorted picks |
| `similar_wines` | Alternatives to drink instead |

## Output Format

Pass `format="plain"` only when the tool schema exposes a `format` parameter. Never add unsupported arguments. Present the final answer as plain text for iMessage.
