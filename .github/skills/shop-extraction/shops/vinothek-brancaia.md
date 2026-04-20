---
shop_id: vinothek-brancaia
domain: vinothek-brancaia.ch
display_name: Vinothek Brancaia
currency: CHF
rate_limit_ms: 5000
catalogue_size: 400
cms: Custom e-commerce
search:
  url: "https://vinothek-brancaia.ch/de/search/advancedsearch?SearchTerm={query}"
  robots_ok: false
  note: "Search works SSR but blocked by robots.txt (/de/search). Use category browsing."
product:
  url: "https://vinothek-brancaia.ch/de/product/{product-id}/{wine-slug}?variantid={variant-id}"
browse:
  red: "https://vinothek-brancaia.ch/de/rotwein"
  white: "https://vinothek-brancaia.ch/de/weisswein"
  sparkling: "https://vinothek-brancaia.ch/de/schaumwein"
  by_country: "https://vinothek-brancaia.ch/de/{country}"
  rarities: "https://vinothek-brancaia.ch/de/exklusive-raritaeten"
  classics: "https://vinothek-brancaia.ch/de/grosse-klassiker"
  everyday: "https://vinothek-brancaia.ch/de/ideale-alltagsweine"
  by_producer: "https://vinothek-brancaia.ch/de/manufacturer/{id}/{producer-slug}"
robots_blocked: ["/de/search"]
---
# Vinothek Brancaia — Extraction Guide

## Search Strategy
Search blocked by robots.txt. Use category browsing:
- By type: `/de/rotwein`, `/de/weisswein`
- By country: `/de/italien`, `/de/frankreich`, `/de/oesterreich`
- Curated: `/de/exklusive-raritaeten`, `/de/grosse-klassiker`, `/de/ideale-alltagsweine`
- By producer: `/de/manufacturer/{id}/{slug}`

## Price
- Format: `CHF {price}` (inkl. MwSt.)
- "ab:" pricing for wines with multiple size/vintage variants
- Article number: `{product-code}.{size-code}.{vintage}` (e.g. "82602.075.18")

## Vintage & Size
- "Verfügbare Jahrgänge" selector — each vintage is a variant (`?variantid={id}`), not a separate product
- "Verfügbare Grössen" selector + "+ weitere Formate"
- Sizes: 37.5cl, 75cl, 150cl

## Ratings
**Excellent structured data** on product pages:
- "Rating James Suckling 97"
- "Rating Wine Spectator 96–99"
- On listing cards: "92 PUNKTE", "94 PUNKTE"

## Drinking Window (Unique Feature)
- "Trinkreife: {year-year}" (e.g. "2025-2040") — extract as `drinking_window`
- One of the few shops showing this data

## Product Data (detail page — all SSR)
Structured table with all fields:
- Land, Region, Jahrgang, Traubensorte, Grösse, Alkoholgehalt
- Passt zu (food pairing), Trinktemperatur, Sortiment
- Trinkreife (drinking window), Rating with source + score
- Full prose tasting notes

## Stock
- No exact stock counts visible
- Purchasable → `in_stock: true`

## Failure Patterns
- No Antinori wines (Brancaia is a competitor)
- Tignanello: not in catalogue
- Italian specialist + surprise Bordeaux depth (Lynch-Bages, Léoville Poyferré)
- Note: `crawl-delay: 5` in robots.txt — respect 5-second intervals
