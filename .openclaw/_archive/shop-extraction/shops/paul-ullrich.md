---
shop_id: paul-ullrich
domain: ullrich.ch
display_name: Paul Ullrich
currency: CHF
rate_limit_ms: 2000
catalogue_size: 500
cms: Shopware 6
search:
  url: "https://ullrich.ch/de/search?search={query}"
  encoding: url_plus
  robots_ok: true
  result_text: 'Zu «{query}» wurden N Produkte gefunden'
  no_result_text: "Es wurden leider keine Suchergebnisse gefunden."
product:
  url: "https://ullrich.ch/de/{producer}-{wine}-{vintage}-{appellation}-{abv}-{size}/{product-id}/"
browse:
  all_wine: "https://ullrich.ch/de/wein-schaumwein/"
  red: "https://ullrich.ch/de/rotwein-wein-schaumwein/"
  by_producer: "https://ullrich.ch/de/produzenten-p/{producer-slug}/"
robots_blocked: ["/wishlist", "/compare", "/checkout", "/account", "/note", "/widgets"]
---
# Paul Ullrich — Extraction Guide

## Search
URL: `https://ullrich.ch/de/search?search={query}` — replace spaces with `+`.
SSR results with product cards showing: wine name, price, stock count, rating badge.

## Price
- Format: `CHF {price}` (per bottle, inkl. MwSt.)
- Sale: `-{X}%` badge + original price struck-through + new price
- Volume discounts: 6 bottles: 5%, 12: 7%, 18: 10%. Max 20% cumulation
- "HIT" badge for featured deals
- Use single-bottle price (no discount applied)

## Vintage Chaining (Unique Feature)
- **"ZUM NACHFOLGER"** link on old vintages → navigates to the next vintage
- Example: Crianza 2020 → "ZUM NACHFOLGER" → 2021 → 2022
- Each vintage is a **separate product URL** with its own ID

## Stock
- "**{N} verfügbar**" → exact count (e.g. "6 verfügbar", "93 verfügbar")
- "**>100 verfügbar**" → `stock_count: 100` (lower bound)
- "**nicht verfügbar**" → `in_stock: false`
- "**In den Filialen verfügbar**" → `in_stock: true`, notes: `"in-store only"`

## Ratings
SSR on listing cards: `{score} Punkte {critic}`:
- "96 Punkte Robert Parker"
- "95 Punkte Decanter"
- "92 Punkte Wine Spectator"
- "91 Punkte James Suckling"
- "90 Punkte Vinum"
- Medals: "Gold Medaille", "Silber Medaille"

## Product URL Slug
Encodes full wine identity — very machine-parseable:
`/de/{producer}-{wine-name}-{vintage}-{appellation}-{abv}-{size}/{product-id}/`
Example: `/de/malhadinha-marias-da-malhadinha-vinhas-velhas-tinto-2021-alentejano-vr-150-75cl/105729/`

## Product Page Tabs (may need JS)
- MEHR ERFAHREN — expanded description
- ZUSATZINFORMATION — technical details
- SCHMECKT NACH — tasting notes
- PASST ZU — food pairing
- BEWERTUNGEN — customer reviews

## Failure Patterns
- 0 results: "Es wurden leider keine Suchergebnisse gefunden."
- No fine wine — Tignanello/Sassicaia only as Grappa. Focus is value/mid-range
- Strong Portugal/Spain/Austria focus
