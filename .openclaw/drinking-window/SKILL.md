---
name: drinking-window
description: "Find wines approaching or past their optimal drinking window. Urgency-sorted."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Drinking Window

Find wines that need attention — past optimal (drink ASAP) or approaching their peak.

## Owner Context

- Switzerland. Show storage location so the user knows where to find the bottle.

## Workflow

### 1. Past Optimal — Drink Now

```sql
SELECT wine_id, winery_name, wine_name, vintage,
       optimal_until, bottles_stored, cellar_name
FROM wines_full
WHERE drinking_status = 'past_optimal' AND bottles_stored > 0
ORDER BY optimal_until ASC
LIMIT 15
```

### 2. Approaching Peak — Plan Ahead

```sql
SELECT wine_id, winery_name, wine_name, vintage,
       optimal_from, optimal_until, bottles_stored, cellar_name
FROM wines_full
WHERE drinking_status = 'drinkable'
  AND optimal_from <= EXTRACT(YEAR FROM CURRENT_DATE) + 1
  AND bottles_stored > 0
ORDER BY optimal_from ASC
LIMIT 15
```

### 3. Present

Show as an urgency table:

| Priority | Wine | Vintage | Window | Bottles | Location |
|----------|------|---------|--------|---------|----------|
| 🔴 Past | ... | ... | ended YYYY | ... | ... |
| 🟡 Soon | ... | ... | starts YYYY | ... | ... |

Offer to show dossier details for any wine: `read_dossier(wine_id)`.

## Tools

| Tool | Purpose |
|------|---------|
| `query_cellar` | SQL queries for drinking status |
| `read_dossier` | Details on specific wines |

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.
