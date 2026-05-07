---
name: price-scan
description: "Check current prices at Swiss retailers, log observations, and review alerts for tracked wines."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Price Scanning

Check Swiss retailer prices for tracked wines, log observations, and monitor alerts.

## Owner Context

- Switzerland, CHF primary. Pages may be in German or French.
- Default bottle size: 750ml unless stated otherwise.

## Retailers

| Name | Domain | Search |
|------|--------|--------|
| Gerstl | gerstl.ch | `?q=` |
| Flaschenpost | flaschenpost.ch | `?search=` |
| Globalwine | globalwine.ch | `?search=` |
| Mövenpick | moevenpick-wein.com | `?q=` |
| Schuler | schuler.ch | `?search=` |
| DIVO | divo.ch | `?q=` |
| Millesima | de.millesima.ch | Direct URL only |
| Martel | martel.ch | `site:martel.ch` |
| Paul Ullrich | paul-ullrich.ch | `?search=` |
| Schubi | schubi-weine.ch | `?searchtext=` |

## Identity Verification (3-Field Match)

Before logging ANY price, verify:
1. **Winery** — name matches (allow minor variants: Château/Chateau)
2. **Wine name** — core name matches (ignore appellation suffixes)
3. **Vintage** — exact 4-digit year

If any field does not match → skip. Never log a "close enough" wine.

## Workflow: Full Scan

1. `query_cellar("SELECT tracked_wine_id, winery_name, wine_name, latest_vintage FROM tracked_wines WHERE is_active")` — discover targets
2. For each tracked wine:
   - `tracked_wine_prices(tracked_wine_id)` — check existing data
   - Search retailers for current price
   - Verify identity (3-field match)
   - `log_price(tracked_wine_id, vintage, 750, retailer_name, price, currency, in_stock, retailer_url)`
3. `wishlist_alerts()` — summarise alerts (price drops, new listings, back in stock)

## Workflow: Single Wine

1. `find_wine(query)` or `query_cellar(...)` to identify the tracked wine
2. `tracked_wine_prices(tracked_wine_id)` — current data
3. Search retailers, verify, log
4. `price_history(tracked_wine_id, vintage, months=6)` — show trend

## Workflow: Alerts Only

`wishlist_alerts(days=7)` — show recent alerts grouped by priority.

## Tools

| Tool | Purpose |
|------|---------|
| `query_cellar` | Discover tracked wines |
| `tracked_wine_prices` | Current prices per retailer |
| `log_price` | Record a price observation |
| `price_history` | Monthly price trends |
| `wishlist_alerts` | Price drops, new listings, back in stock |
| `find_wine` | Resolve wine identity |
