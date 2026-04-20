---
shop_id: gerstl
domain: gerstl.ch
display_name: Gerstl Weinselektionen
currency: CHF
rate_limit_ms: 2000
catalogue_size: 1000
cms: Custom (cdn3.snpy.ch)
search:
  url: "https://www.gerstl.ch/c?q={query}"
  encoding: url_plus
  robots_ok: true
  result_text: "Resultate für '{query}' (N)"
  no_result_text: "0 Produkte"
  note: "robots.txt blocks ?p= (pagination) but allows search"
product:
  url: "https://www.gerstl.ch/{vintage}-{producer-slug}-{country}-{article-code}-{year}/p"
browse:
  all: "https://www.gerstl.ch/c"
  promotions: "https://www.gerstl.ch/aktionen/c"
  subscriptions: "https://www.gerstl.ch/subskriptionen/c"
  new_arrivals: "https://www.gerstl.ch/neu-eingetroffen/c"
  last_bottles: "https://www.gerstl.ch/restposten-ausverkauf/c"
  aged_wines: "https://www.gerstl.ch/gereifte-weine/c"
  tasting_boxes: "https://www.gerstl.ch/degustations-boxen/c"
  six_for_five: "https://www.gerstl.ch/de/6-fur-5-flaschen-angebot"
  sitemap: "https://www.gerstl.ch/sitemap.xml"
robots_blocked: ["*?p=*", "*&p=*"]
---
# Gerstl Weinselektionen — Extraction Guide

## Search
URL: `https://www.gerstl.ch/c?q={query}` — replace spaces with `+`.
SSR results. Shows `Resultate für '{query}' (N)` or `0 Produkte`.

**Pagination blocked** — robots.txt disallows `?p=`. Use page 1 results only.

## Price
- Format: `CHF {price}` per bottle (75cl, inkl. MwSt.)
- Sale: `CHF 16.80 statt 24.00` — original price after "statt"
- "6 für 5 Angebote" = buy 6 pay for 5 (~17% discount). Use single-bottle price
- "Smart Buys" label for value picks
- "HIT" badge for featured promotions

## Packaging
Listed on cards: `75cl (CT-6)` or `75cl (OWC-6)`
- **CT-6** = carton of 6
- **OWC-6** = original wooden case of 6
- Extract bottle size from this field

## Product URL Pattern
Encodes vintage, producer, country, article code:
`/{vintage}-{producer-slug}-{country}-{article-code}-{year}/p`

Country codes: `fra` (France), `ita` (Italy), `aut` (Austria), `che` (Switzerland)
Optional suffix: `-f6` (likely OWC/case format)

Examples:
- `2018-chateau-clos-manou-medoc-aoc-fra-266424-2018-f6/p`
- `2024-azienda-agricola-alessio-dorigo-ita-266751-2024/p`
- Non-vintage: `castello-bonomi-franciacorta-docg-ita-249289/p`

## Ratings
- In listing prose: `20/20 Punkte` (score out of 20 for some critics)
- Max Gerstl's personal recommendations in editorial text
- Detailed ratings likely on product detail pages

## Stock
- "Online verfügbar" filter toggle in sidebar
- Product count per result shown
- No exact stock count observed

## Browsing Filters (sidebar)
- Land (Country), Region, Subregion
- Weintyp (Wine type), Appellation
- Produzent (Producer), Jahrgang (Vintage)
- Traubensorte (Grape variety)
- Toggles: Nur Aktionen, Keine Subskriptionen, Keine Gerstl Fine Wines, Online verfügbar

## Special Sections
- **Subskriptionen** — Bordeaux en primeur (currently 2023 Arrivage) + Monteverro
- **Gereifte Weine** — aged wines ready to drink
- **Restposten & Ausverkauf** — last bottles / clearance
- **Magazin** — Max Gerstl's vintage reports, editorial content

## Failure Patterns
- Smaller-estate focus — Tignanello not in catalogue (0 results)
- Pagination blocked — cannot crawl beyond page 1
- Custom platform (not Shopware/Magento) — non-standard HTML structure
