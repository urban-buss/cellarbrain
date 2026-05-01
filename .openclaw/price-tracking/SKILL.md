---
name: price-tracking
description: "Swiss retailer price scanning, observation logging, and alert monitoring via cellarbrain MCP."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Price Tracking

Automated Swiss wine retailer scanning. Check prices, log observations, and monitor alerts for tracked wines.

## Cardinal Rules

1. **Identity verification.** Confirm winery + wine name + vintage before logging any price.
2. **Facts only.** Every price must come from a retailer page visited in this session.
3. **One observation per visit.** Log exactly what you see: price, currency, stock status.
4. **MCP only.** Use `log_price` for all observations.

## Owner Context

- Switzerland, CHF primary
- Pages may be in German or French
- Default bottle size: 750ml unless stated otherwise

## MCP Tools

| Tool | Use |
|---|---|
| `query_cellar(sql)` | Discover tracked wines: `SELECT * FROM tracked_wines WHERE is_active` |
| `tracked_wine_prices(tracked_wine_id)` | Check existing prices |
| `log_price(...)` | Record observation: tracked_wine_id, vintage, bottle_size_ml, retailer_name, price, currency, in_stock |
| `wishlist_alerts(days)` | Alert summary after scanning |
| `price_history(tracked_wine_id, vintage, months)` | Price trends |
| `find_wine(query)` | Search cellar by text |

## Retailer Registry

| Key | Domain |
|---|---|
| gerstl | gerstl.ch |
| martel | martel.ch |
| flaschenpost | flaschenpost.ch |
| moevenpick | moevenpick-wein.com |
| weinauktion | weinauktion.ch |
| wine_ch | wine.ch |
| juan_sanchez | juan-sanchez.ch |
| globalwine | globalwine.ch |
| divo | divo.ch |
| schuler | schuler.ch |

## Workflow: Full Scan

### Phase 1 — Discover

`query_cellar("SELECT tracked_wine_id, winery_name, wine_name, latest_vintage FROM tracked_wines WHERE is_active")`

### Phase 2 — Scan

For each tracked wine + vintage:

1. `tracked_wine_prices(tracked_wine_id)` — check existing
2. Search each retailer: `"{winery}" "{wine}" "{vintage}" site:{domain}`
3. Verify identity (winery + name + vintage)
4. `log_price(tracked_wine_id, vintage, 750, retailer_name, price, currency, in_stock, retailer_url, notes)`
5. Out-of-stock: log `in_stock=false` with last known price

### Phase 3 — Report

1. `wishlist_alerts()` for alert summary
2. Report: wines scanned, observations logged, alerts found

## Workflow: Single Wine

1. Identify via `find_wine` or `query_cellar`
2. `tracked_wine_prices(tracked_wine_id)` for current data
3. Scan all retailers
4. Log and report

## Workflow: Alerts

1. `wishlist_alerts()` with optional `days` parameter
2. Group by priority: High (price drop, new listing, back in stock), Medium (best price, en primeur)
3. Suggest next steps

## Data Quality

- Log displayed currency — auto-converts to CHF
- Verify bottle size (375, 750, 1500, 3000ml)
- `in_stock=true` only if purchasable ("sold out"/"notify me" = false)
- Notes: "en primeur", "last N bottles", "case price", "sale -X%"

## Retailer Search Patterns

| Retailer | Pattern |
|---|---|
| gerstl.ch | `"{winery}" "{vintage}" site:gerstl.ch` |
| martel.ch | `"{winery}" "{wine}" site:martel.ch` |
| flaschenpost.ch | `"{winery}" "{vintage}" site:flaschenpost.ch` |
| moevenpick-wein.com | `"{winery}" site:moevenpick-wein.com` |
| wine.ch | `"{winery}" "{wine}" site:wine.ch` |
