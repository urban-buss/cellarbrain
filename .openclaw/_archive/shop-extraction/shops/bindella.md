---
shop_id: bindella
domain: bindella.ch
display_name: Bindella
currency: CHF
rate_limit_ms: 2000
catalogue_size: 300
cms: Contentful + Perfion PIM
search:
  url: null
  robots_ok: true
  note: "No search URL found. Catalogue at /weinshop/sortiment with JS-only filtering. Use category pages or direct product URLs."
product:
  url: "https://www.bindella.ch/{wine-slug}.html"
browse:
  all: "https://www.bindella.ch/weinshop/sortiment"
  red: "https://www.bindella.ch/weinshop/sortiment/rotweine"
  white: "https://www.bindella.ch/weinshop/sortiment/weissweine"
  rose: "https://www.bindella.ch/weinshop/sortiment/roseweine"
  sparkling: "https://www.bindella.ch/weinshop/sortiment/schaumweine"
  by_producer: "https://www.bindella.ch/wein/produzenten/{producer}"
  by_region: "https://www.bindella.ch/wein/weinregionen/{region}"
  by_grape: "https://www.bindella.ch/wein/traubensorten/{grape}"
robots_blocked: []
note: "No robots.txt (404) — fully open"
---
# Bindella — Extraction Guide

## Search Strategy
No functional search URL. Use:
- Category pages: `/weinshop/sortiment/rotweine`, etc.
- Producer pages: `/wein/produzenten/{producer}`
- Region pages: `/wein/weinregionen/{region}`
- Or web search: `"{wine name}" site:bindella.ch`

## Price
- Format: `CHF {price}` (inkl. MwSt.)
- Per-bottle pricing

## Product Data (product page — SSR, "Fakten" table)
- Wine name, producer, vintage
- Region/appellation
- Grape varieties with percentages
- Bottle size (Inhalt)
- ABV (Alkoholgehalt)
- Ratings (e.g. "Parker 100 Punkte")
- Stock: "Verfügbar" + max purchase quantity (e.g. max 3, max 6, max 18)
- Degustationsnotizen (tasting notes)
- Food pairing
- Article number
- Storage info, packaging type
- **PDF datasheet** downloadable per wine

## Ratings
- Format: `{critic} {score} Punkte` (e.g. "Parker 100 Punkte")
- From Perfion PIM system

## Stock
- "Verfügbar" with max quantity → `in_stock: true`
- Purchase limits on premium wines: Masseto max 3, Solaia max 6, Tignanello max 18

## Failure Patterns
- **100% Italian wines only** — no French, Spanish, etc.
- Filtering on /sortiment is JS-dependent — category pages work better for SSR
- Sassicaia available (Tenuta San Guido); Tignanello available (Antinori)
- German-only
