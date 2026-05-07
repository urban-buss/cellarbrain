---
shop_id: globalwine
domain: globalwine.ch
display_name: Globalwine
currency: CHF
rate_limit_ms: 2000
catalogue_size: 1000
cms: Shopware 6 (nextagtheme)
search:
  url: "https://www.globalwine.ch/search?search={query}"
  encoding: url_plus
  robots_ok: true
  result_text: 'Zu "{query}" wurden {N} Produkte gefunden'
  no_result_text: 'Zu "{query}" wurde kein Produkt gefunden'
product:
  url: "https://www.globalwine.ch/{wine-name-vintage-producer-size-slug}"
browse:
  by_country: "https://www.globalwine.ch/land/{country}"
  by_region: "https://www.globalwine.ch/region/{region}"
  by_grape: "https://www.globalwine.ch/traubensorte/{grape}"
  by_producer: "https://www.globalwine.ch/winzer/{country}/{region}/{producer}/"
  fine_wine: "https://www.globalwine.ch/wein?filter=finewine,ja"
  on_sale: "https://www.globalwine.ch/wein?filter=aktion_privat,ja"
robots_blocked: ["/account", "/checkout", "/widgets"]
llms_txt: "https://www.globalwine.ch/llms.txt"
---
# Globalwine — Extraction Guide

## Search
URL: `https://www.globalwine.ch/search?search={query}` — replace spaces with `+`.
Results are SSR. Each product card shows: wine name (linked), price (`CHF X.XX`), and "IN DEN WARENKORB" button.
Result count: `Zu "{query}" wurden {N} Produkte gefunden` or `wurde kein Produkt gefunden`.

Product URL slug encodes: `{wine-name}-{vintage}-{producer}-{size}` (e.g. `chateau-giscours-2021-chateau-giscours-75cl`).

**Note**: This shop publishes `llms.txt` explicitly inviting AI usage. Most AI-friendly shop assessed.

## Price
- Search card: `CHF 59.90` next to wine name
- Product page: regular price + sale price (original struck-through)
- `*` after wine name means "Nettoartikel" (not discount-eligible)
- Volume discounts available for 6+ units on some items

## Ratings
Product page shows multiple critic ratings with badges:
- Robert Parker, James Suckling, Falstaff, Decanter, Vinous, Gambero Rosso, Guía Peñín
- Format: critic icon + numeric score (e.g. "96" next to Parker icon)

## Stock
- "noch {N} verfügbar" → exact count: `stock_count: N`
- "IN DEN WARENKORB" button → `in_stock: true`
- No availability text → likely out of stock

## Product Data
- Bio badge: "Bio" marker on card
- Vegan badge: "Vegan" marker on card
- Aktion badge: sale indicator
- Wine type: Rotwein/Weisswein/Roséwein/Schaumwein/Süsswein
- Country + region in metadata

## Failure Patterns
- 0 results: `Zu "{query}" wurde kein Produkt gefunden` + `Keine Produkte gefunden.`
- Tignanello/Sassicaia: not in catalogue (confirmed April 2026)
