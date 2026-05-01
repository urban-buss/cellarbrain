---
name: price-tracking
description: "Research wines and track prices via cellarbrain MCP. Use when: 'price check', 'scan retailers', 'market price', 'price history', 'price trends', 'Swiss availability', 'price comparison', 'what does it cost', 'price drop', 'price alert'."
---

# Price Tracking

Workflows for checking prices, logging observations, and monitoring price alerts using cellarbrain MCP tools.

## When to Use

- User asks about current prices for a tracked wine
- User wants to scan retailers for price updates
- User asks about price trends or history
- User wants to see wishlist alerts (price drops, new listings, etc.)
- User asks about Swiss wine retailer availability

## Workflow: Single Wine Price Check

**Trigger:** "what does wine #N cost", "price for [wine]", "check price"

1. Identify the wine via `find_wine(query)` or `query_cellar(sql)`
2. Check if it's tracked: look for `tracked_wine_id` in the result
3. If tracked, call `tracked_wine_prices(tracked_wine_id)` for current prices
4. For history, call `price_history(tracked_wine_id, months=12)`
5. Present prices sorted by value, noting stock status

## Workflow: Batch Price Check

**Trigger:** "scan all prices", "check retailer prices", "price update"

1. Discover tracked wines: `query_cellar("SELECT * FROM tracked_wines WHERE is_active")`
2. For each wine, search retailer websites for current prices
3. Log each observation via `log_price(tracked_wine_id, vintage, ...)`
4. After scanning, call `wishlist_alerts()` for the alert summary
5. Report: wines scanned, observations logged, alerts found

## Workflow: Price Alerts

**Trigger:** "any price drops", "wishlist alerts", "what's new"

1. Call `wishlist_alerts()` — optionally with `days` parameter
2. Present alerts grouped by priority:
   - **High:** New Listing, Price Drop, Back in Stock
   - **Medium:** Best Price, En Primeur, Last Bottles
3. For each high-priority alert, suggest next steps

## Useful SQL Patterns

```sql
-- All tracked wines with current prices
SELECT tw.tracked_wine_id, tw.winery_name, tw.wine_name,
       tw.vintage_count, tw.total_bottles_stored
FROM tracked_wines tw
WHERE tw.is_active
ORDER BY tw.winery_name, tw.wine_name

-- Wines with no price observations yet
SELECT tw.tracked_wine_id, tw.winery_name, tw.wine_name
FROM tracked_wines tw
LEFT JOIN price_observations po ON tw.tracked_wine_id = po.tracked_wine_id
WHERE tw.is_active AND po.observation_id IS NULL

-- Latest prices across all tracked wines
SELECT * FROM latest_prices ORDER BY tracked_wine_id, price_chf ASC

-- Price history for a specific wine
SELECT * FROM price_history
WHERE tracked_wine_id = 42
ORDER BY month DESC, retailer_name
```

## Retailer Search Patterns

When searching for a wine on retailer sites:

| Retailer | Search pattern |
|---|---|
| gerstl.ch | `"{winery}" "{vintage}" site:gerstl.ch` |
| martel.ch | `"{winery}" "{wine}" site:martel.ch` |
| flaschenpost.ch | `"{winery}" "{vintage}" site:flaschenpost.ch` |
| moevenpick-wein.com | `"{winery}" site:moevenpick-wein.com` |
| wine.ch | `"{winery}" "{wine}" site:wine.ch` |

## Data Quality Reminders

- Always verify wine identity (winery + name + vintage) before logging
- Log the displayed currency — CHF conversion is automatic
- Default bottle size is 750ml; verify larger/smaller formats
- `in_stock=true` only if purchasable; "sold out" / "notify me" = false
- Use `notes` for signals: "en primeur", "last bottles", "limited", "case price"
