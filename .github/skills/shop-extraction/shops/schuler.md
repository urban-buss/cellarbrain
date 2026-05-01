---
shop_id: schuler
domain: schuler.ch
display_name: Schuler 1694
currency: CHF
rate_limit_ms: 2000
catalogue_size: 700
cms: Shopware 6
search:
  url: "https://www.schuler.ch/search?search={query}"
  encoding: url_plus
  robots_ok: false
  no_result_text: "Hoppla! Zu '{query}' wurde kein Produkt gefunden."
  note: "robots.txt blocks /*? but search works SSR. Small catalogue limits usefulness."
product:
  url: "https://www.schuler.ch/p/{wine-slug}-{article-number}"
browse:
  all: "https://www.schuler.ch/weine/"
  red: "https://www.schuler.ch/rotwein/"
  white: "https://www.schuler.ch/weisswein/"
  rose: "https://www.schuler.ch/rosewein/"
  sparkling: "https://www.schuler.ch/schaumweine/"
  orange: "https://www.schuler.ch/orangewein/"
  organic: "https://www.schuler.ch/bioweine/"
  on_sale: "https://www.schuler.ch/weine-in-aktion/"
  last_bottles: "https://www.schuler.ch/restpostenweine/"
  by_country: "https://www.schuler.ch/alle-weinlaender/"
  by_grape: "https://www.schuler.ch/alle-rebsorten/"
  subscription: "https://www.schuler.ch/abo-weinpakete/"
  sitemap_de: "https://www.schuler.ch/sitemap.xml"
  sitemap_fr: "https://www.schuler.ch/fr/sitemap.xml"
robots_blocked: ["/*?"]
---
# Schuler 1694 — Extraction Guide

## Search
URL: `https://www.schuler.ch/search?search={query}`
SSR Shopware 6. Shows `Hoppla! Zu '{query}' wurde kein Produkt gefunden.` for no results.

**Note**: robots.txt blocks `/*?` — technically disallowed. Small catalogue (~700 wines) limits relevance.

## Price
- **Volume pricing tiers**: `Ab 1: CHF 25.00` | `Ab 6: CHF 19.90`
- Use the **single-bottle** price ("Ab 1") unless instructed otherwise
- The 6-bottle price is typically 20–40% lower
- Sale: percentage badge + strikethrough original price
- Currency: CHF, inkl. MwSt.

## Article Number
Format: `{7digits}-{YY}-CH-{size_code}`
- Example: `1059608-20-CH-K06` → product 1059608, year 20(20), CH market, K06 = carton of 6
- Embedded in product URL slug

## Product URL
`/p/{wine-slug}-{article-number}`
Examples:
- `/p/noa-noah-of-areni-2020-1059608-20-ch-k06`
- `/p/tenuta-san-guido-sassicaia-bolgheri-sassicaia-doc-2020-045891602-20-ch-h06`

## Ratings
Structured on product page under "Prämierungen":
- `Falstaff 92 (2022)` — critic + score + (year of rating)
- `Decanter 92 (2023)`
- `La Sélection Silver (2023)` — medal, not numeric
- `AWC Vienna Silver (2023)` — medal
- `MUNDUS Vini Gold (2025)` — medal

Extract numeric scores for scored critics; note medals as text in `notes` field.

## Tasting Notes (Exceptional Quality)
Structured in separate German sections:
- **Im Auge** — visual appearance ("kräftiges Kirschrot, leuchtend")
- **In der Nase** — nose aromas (comma-separated notes)
- **Am Gaumen** — palate (comma-separated descriptors)

## Food Pairing
`Passt zu:` field with detailed suggestions (gegrilltem Fleisch, Ratatouille, etc.)

## Drinking Window
`Geniessen bis:` field with year (e.g. "2028")

## Technical Data
- **Alkoholgehalt** — ABV %
- **Serviertemperatur** — serving temp °C
- **Ausbau in Barrique** — months in oak
- **Restzucker** — residual sugar g/L
- **Herkunft** — country + region
- **Traubensorte** — grape variety

## Stock
- Products with "In den Warenkorb" → `in_stock: true`
- No exact stock counts visible

## Customer Reviews
Extensive: `{score}/5 ({count} reviews)` — detailed user reviews on product pages.

## Failure Patterns
- Very small curated catalogue (~700 wines) — most standard wines not carried
- Tignanello: not found. Sassicaia: found (CHF 290)
- Many wines are Schuler-exclusives (own-label, partner estates)
- Only useful for price comparison if the specific wine is in their range
