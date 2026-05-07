---
name: cellar-stats
description: "Cellar overview: bottle counts, values by location, churn, spending, last ETL run, monthly summary."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Cellar Statistics

Provide cellar overviews, per-location breakdowns, monthly summaries, and spending analysis.

## Owner Context

- Switzerland, CHF. Multiple storage locations (onsite + offsite).
- Monthly summary needed on last day of month for discussions with friends.

## Workflow: General Question

1. `cellar_info()` — last ETL run, version, inventory totals
2. `cellar_stats()` — overview (bottles, wines, value)
3. Add detail as needed:
   - By location: `cellar_stats(group_by="cellar")`
   - By country: `cellar_stats(group_by="country")`
   - By category: `cellar_stats(group_by="category")`
   - By vintage: `cellar_stats(group_by="vintage")`
   - By winery: `cellar_stats(group_by="winery", sort_by="value")`

## Workflow: Monthly Summary Template

Use when the user asks for a monthly report or cellar overview for friends:

1. `cellar_info()` — capture last ETL timestamp
2. `cellar_stats(group_by="cellar")` — bottles + value per location
3. `query_cellar("SELECT count(*) AS bottles, sum(price) AS value FROM bottles_stored WHERE cellar_name != 'On Order'")` — total stored value
4. `query_cellar("SELECT count(*) AS bottles, sum(price) AS value FROM bottles_stored WHERE cellar_name = 'On Order' OR wine_id IN (SELECT wine_id FROM wines_on_order)")` — on-order value
5. `cellar_churn(period="month")` — this month's purchases vs consumption

Present as:
```
📊 Cellar Summary (Month YYYY)
- Last data refresh: <timestamp>
- Stored: X bottles, CHF Y (onsite: A bottles / offsite: B bottles)
- On order: Z bottles, CHF W
- This month: +N purchased, -M consumed
- Locations: <table>
```

## Workflow: Spending / Purchase Questions

```sql
-- Spending by provider
SELECT provider_name, count(*) AS bottles, sum(price) AS total
FROM bottles_full WHERE provider_name IS NOT NULL
GROUP BY provider_name ORDER BY total DESC

-- Recent purchases
SELECT wine_id, winery_name, wine_name, vintage, purchase_date, price
FROM bottles_full WHERE purchase_date >= '<date>'
ORDER BY purchase_date DESC
```

## Tools

| Tool | Purpose |
|------|---------|
| `cellar_info` | Version, freshness, inventory counts |
| `cellar_stats` | Summary or grouped breakdown (10 dimensions) |
| `cellar_churn` | Monthly/yearly roll-forward (begin + purchased − consumed = end) |
| `query_cellar` | Custom SQL for specific questions |
