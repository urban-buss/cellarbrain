---
shop_id: von-salis
domain: vonsalis-wein.ch
display_name: Von Salis
currency: CHF
rate_limit_ms: 2000
catalogue_size: 200
cms: nopCommerce (.NET)
search:
  url: null
  robots_ok: false
  note: "Search blocked by robots.txt (/search?). Use category browsing."
product:
  url: "https://www.vonsalis-wein.ch/{product-slug}"
browse:
  all: "https://www.vonsalis-wein.ch/weine"
  sparkling: "https://www.vonsalis-wein.ch/schaumweine"
  spirits: "https://www.vonsalis-wein.ch/spirituosen"
  topseller: "https://www.vonsalis-wein.ch/topseller"
  on_sale: "https://www.vonsalis-wein.ch/aktionen"
  last_bottles: "https://www.vonsalis-wein.ch/letzte-flaschen"
  graubuenden: "https://www.vonsalis-wein.ch/graubuenden"
  pinot_noir: "https://www.vonsalis-wein.ch/pinot-noir"
robots_blocked: ["/search?", "/cart", "/checkout", "/customer/", "/wishlist"]
---
# Von Salis — Extraction Guide

## Search Strategy
Search is blocked by robots.txt. Use category/region browsing instead:
- All wines: `/weine`
- By region: `/graubuenden`, `/malans`
- By grape: `/pinot-noir`
- Special: `/topseller`, `/aktionen`, `/letzte-flaschen`
- Or use web search: `"{wine name}" site:vonsalis-wein.ch`

## Price
- Format: `CHF {price}` (inkl. MwSt.)
- Sale: original price struck-through + sale price
- Note: minimum 6-bottle quantity on most wines

## Stock
- No exact stock counts visible
- Products listed with purchase button → `in_stock: true`
- "Letzte Flaschen" section → limited stock

## Ratings
- Falstaff, Grand Prix du Vin Suisse, Mondial medals
- Shown on product detail pages in structured data table

## Product Data (detail page — all SSR)
- Degustationsnotiz — full tasting note
- Geniessen zu — food pairing
- Trinkreife — drinking window
- Herstellung — winemaking method
- Grape varieties, ABV, region, producer info
- PDF wine description downloadable

## Failure Patterns
- Small catalogue (~200 wines) — many searches will return no results
- German-only
- Graubünden/Swiss wine specialist — no Bordeaux, Tuscany, etc.
- Note: `vonsalis.ch` is a different company (PR agency) — correct domain is `vonsalis-wein.ch`
