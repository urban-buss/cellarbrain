---
shop_id: divo
domain: divo.ch
display_name: DIVO
currency: CHF
rate_limit_ms: 2000
catalogue_size: 1256
cms: Neos CMS
search:
  url: "https://www.divo.ch/de/sortiment.html?q={query}"
  encoding: url_plus
  robots_ok: true
  result_text: "{N} ausgewählte Artikel"
  no_result_text: "Leider gibt es keinen Artikel"
product:
  url: "https://www.divo.ch/de/sortiment/artikel-{article_number}.html"
  article_format: "{id} {size} {year}"
browse:
  all: "https://www.divo.ch/de/sortiment.html"
  filters: "?filters[color][]={Rot|Weiss|Rosé}&filters[country][]={country}&filters[region][]={region}"
  promotions: "?promotions=true"
  by_price: "?ranges[price][to]={max}"
  by_grape: "?filters[grapes][]={grape}"
  bio: "?filters[winegrowing][]=Bio"
robots_blocked: ["/neos/"]
---
# DIVO — Extraction Guide

## Search
URL: `https://www.divo.ch/de/sortiment.html?q={query}` — replace spaces with `+`.
Fully SSR results. Page shows `{N} ausgewählte Artikel` or `Leider gibt es keinen Artikel`.

## Price
- **Per-bottle**: `{price} / flasche` (e.g. "29.70 / flasche")
- **Per-case**: `{price} / karton` (e.g. "178.20 / karton") for 6x75cl
- **Katalogpreis**: full non-member price (e.g. "Katalogpreis: 33.10")
- Use the per-bottle price. If member and catalogue prices differ, use catalogue price unless instructed otherwise
- "Die Preise sind in CHF und inkl. MwSt." — VAT included

## Article Number
Format: `{id} {size} {year}` — URL-encoded spaces as `%20`.
Example: article `93692 75 2020` → URL `/artikel-93692%2075%202020.html`
- `93692` = product ID
- `75` = bottle size (cl)
- `2020` = vintage year

## Stock
- Product listed with "In den Warenkorb" → `in_stock: true`
- No exact stock count shown. Set `stock_count: null`

## Ratings
- Critic ratings on product detail pages: "WinesCritic: 97/100", "James Suckling: 96/100"
- Not visible on listing cards — must visit product page

## Tasting Notes (product page)
Structured sections:
- **Anblick** (appearance)
- **Geruch** (nose)
- **Geschmack** (palate)
- **Schlussbewertung** (conclusion)
- Serving temperature + food pairing: "Servieren: bei 15° C. Rindfleisch, Hirschpfeffer."

## Product Data (listing cards)
- Wine name + vintage
- Appellation (e.g. "Amarone della Valpolicella Classico DOCG")
- Producer name
- Article number
- Case size (e.g. "Karton 6x75 cl")
- Per-bottle and per-case prices

## Failure Patterns
- 0 results: "Leider gibt es keinen Artikel" message
- No New World wines — European focus only
- Tignanello/Sassicaia: not in catalogue
