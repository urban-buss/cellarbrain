---
shop_id: example
domain: example.ch
display_name: "Example Wine Shop"
currency: CHF
rate_limit_ms: 2000
search:
  url: "https://www.example.ch/search?q={query}"
  encoding: url_plus
  robots_ok: true
  result_text: "N Produkte gefunden"
  no_result_text: "Keine Produkte gefunden"
product:
  url: "https://www.example.ch/product/{slug}"
browse:
  by_country: "https://www.example.ch/weine/{country}"
  by_producer: "https://www.example.ch/produzent/{producer}"
  by_type: "https://www.example.ch/rotwein"
robots_blocked:
  - "/search"
---
# {Shop Name} — Extraction Guide

## Price Extraction

- **Where**: look for "CHF" followed by a number (e.g. "CHF 139.95")
- **Sale price**: if two prices shown, the lower one is the sale price; note "sale -X%" in notes
- **Per-unit price**: ignore "/10cl" or "/dl" — extract the per-bottle price only

## Ratings

Look for critic names with numeric scores:
- "James Suckling: 96" → `{"critic": "James Suckling", "score": 96, "max": 100}`
- List all critics found on the page

## Stock Status

- "Verfügbar" / "In den Warenkorb" / "sofort lieferbar" → `in_stock: true`
- "Nicht verfügbar" / "Ausverkauft" / "Benachrichtigen" → `in_stock: false`
- Exact count: "N verfügbar" / "noch N Stück" → `stock_count: N`

## Tasting Notes

- Look for sections labelled: "Degustationsnotiz", "Beschreibung", "Über den Wein"
- Extract the full prose text

## Food Pairing

- Look for: "Passt zu", "Kulinarische Empfehlung", "Servieren zu"
- Extract as comma-separated list or prose

## Failure Patterns

- **0 results**: page shows "{no_result_text}" → return `status: "not_found"`
- **Spirits in results**: skip items without a 4-digit vintage year
- **Page error**: 404, 500, redirect → return `status: "extraction_failed"`
