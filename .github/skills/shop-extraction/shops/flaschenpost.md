---
shop_id: flaschenpost
domain: flaschenpost.ch
display_name: Flaschenpost
currency: CHF
rate_limit_ms: 2000
catalogue_size: 21200
cms: Next.js (SSR)
search:
  url: "https://www.flaschenpost.ch/wein?search={query}"
  encoding: url_plus
  robots_ok: true
  result_text: "{N} von {total} Produkten"
  no_result_text: "Keine Produkte gefunden"
  pagination: "?search={query}&page={page}"
product:
  url: "https://www.flaschenpost.ch/{wine-slug}_{producer-slug}?_size={ml}&_vintage={year}"
browse:
  by_country: "https://www.flaschenpost.ch/wein?country={Country}"
  by_producer: "https://www.flaschenpost.ch/wein?producer={Producer+Name}"
  by_type: "https://www.flaschenpost.ch/wein?winetype={Rotwein|Weisswein|Roséwein}"
robots_blocked: []
---
# Flaschenpost — Extraction Guide

## Search
URL: `https://www.flaschenpost.ch/wein?search={query}` — replace spaces with `+`.
Results are SSR HTML. Each product card shows: wine name, vintage, producer, country, region, wine type, bottle size, price (CHF), and the top critic rating with score.

## Price
- Look for `CHF` followed by a number: `CHF 139.95`
- Ignore per-unit price (`CHF 18.66/10cl`)
- Sale: two prices shown — lower is sale price, higher is struck-through original
- "3% Mengenrabatt ab 24 Flaschen" = volume discount (ignore, use single-bottle price)

## Vintage & Size
- URL params: `_vintage={year}`, `_size={ml}` (750, 1500, 375, 187)
- Vintage selector on product page — each vintage is the same URL with different `_vintage=`
- "4 Varianten verfügbar" means multiple size/vintage combos exist

## Ratings
On product page, look for `{critic}: {score}/100`:
- "James Suckling: 96/100" → `{"critic": "James Suckling", "score": 96, "max": 100}`
- "A. Galloni: 95/100" → `{"critic": "Vinous", "score": 95, "max": 100}`
- Also: Robert Parker, Falstaff, Decanter, Luca Maroni, Vinum
- On search listing cards, only the top rating badge is shown (e.g. "96 James Suckling")

## Stock
- "Bis 17 Uhr bestellt, morgen geliefert" → `in_stock: true`
- "In den Warenkorb" button present → `in_stock: true`
- No exact stock count visible. Set `stock_count: null`

## Tasting Notes
- Section "Degustationsnotiz" on product page — full prose text
- Section "Kulinarische Empfehlung" — food pairing text
- Section "Vinifikation" — winemaking details
- These sections may be behind JS accordion tabs — try to extract from initial HTML first

## Product Data
- Grapes: listed with percentages (e.g. "80 % Sangiovese, 15 % Cabernet Sauvignon, 5 % Cabernet Franc")
- ABV: number only (e.g. "14")
- Serving temp: "16°-18°C"
- Art.Nr.: article number (e.g. "1205766")

## Failure Patterns
- 0 results: empty product grid, text shows "0 von {total} Produkten"
- Spirits in results: skip items with wine type "Spirituosen" or no vintage
- Fuzzy matching: misspellings often still work (e.g. "Chateau Margeaux" → finds Margaux)
