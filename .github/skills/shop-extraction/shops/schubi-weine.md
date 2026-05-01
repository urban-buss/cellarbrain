---
shop_id: schubi-weine
domain: schubiweine.ch
display_name: Schubi Weine
currency: CHF
rate_limit_ms: 2000
catalogue_size: 3500
cms: Java-based (custom)
search:
  url: "https://www.schubiweine.ch/search.html?resultType=1&searchtext={query}"
  encoding: url_plus
  robots_ok: true
  result_text: "Suchergebnis"
  no_result_text: "kein Ergebnis"
product:
  url: "https://www.schubiweine.ch/{product-slug}.html"
browse:
  red: "https://www.schubiweine.ch/rotwein.html"
  white: "https://www.schubiweine.ch/weisswein.html"
  rose: "https://www.schubiweine.ch/rosewein.html"
  sparkling: "https://www.schubiweine.ch/champagner-prosecco.html"
  by_country: "https://www.schubiweine.ch/rotwein/{country}.html"
  producer: "https://www.schubiweine.ch/{producer-slug}-p{id}.html"
robots_blocked: ["/admin/"]
---
# Schubi Weine — Extraction Guide

## Search
URL: `https://www.schubiweine.ch/search.html?resultType=1&searchtext={query}`
Replace spaces with `+`. Fully SSR results.

**Important**: The `&` between `resultType=1` and `searchtext=` must be a literal `&`, not URL-encoded `%26`. If your tool encodes it, the search will fail. Domain is `schubiweine.ch` (no hyphen).

Search results include rich filter sidebar: Rubrik, Land, Region, Produzent, Flaschengrösse, Traubensorte, Jahrgang, Preis, Prämierungen.

## Price
- Format: `CHF {price}` on product card
- Sale: `CHF {sale_price} statt CHF {original_price}` with discount badge (`36% Rabatt`)
- Event discounts: `inkl. 10% Lucerne Wine-Festival Rabatt` — temporary promotions
- VAT included: "inkl. 8.1% MwSt."
- Use per-bottle price, ignore case/bulk pricing
- When event discount active, both discounted and original prices shown

## Stock
- "Artikel sofort lieferbar" → `in_stock: true`
- No exact stock count. Set `stock_count: null`

## Ratings
- "Prämierungen" filter available on search
- "TIPP" badge on recommended wines
- Discount percentage badges (e.g. "36% Rabatt", "22% Rabatt")
- Detailed critic scores not on listing pages — check product detail pages

## Product Data (listing cards)
- Wine name + vintage in title
- Producer name (linked)
- Bottle size (75cl, 37.5cl, 150cl)
- Price + sale price
- "sofort lieferbar" stock status

## Product Data (detail pages)
- Tasting notes: may require navigating to individual product page
- Food pairing: not on listing pages
- Producer pages at `/{producer-name}-p{id}.html`

## Failure Patterns
- 0 results: empty search result, page shows no product cards
- Spirits in results: skip items without vintage year (whisky, grappa, etc. — large spirits catalogue)
- Country URLs nested under wine type: `/rotwein/schweiz.html`, `/rotwein/frankreich.html`
