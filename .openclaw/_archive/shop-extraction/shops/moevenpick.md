---
shop_id: moevenpick
domain: moevenpick-wein.com
display_name: "Mövenpick Wein"
currency: CHF
rate_limit_ms: 2000
catalogue_size: 3000
cms: Magento 2
search:
  url: "https://www.moevenpick-wein.com/de/catalogsearch/result/?q={query}"
  encoding: url_plus
  robots_ok: true
  note: "Magento catalogsearch — SSR results. robots.txt blocks /*?* but allows .html pages and ?p=/?page= params"
product:
  url: "https://www.moevenpick-wein.com/de/{vintage}-{wine-slug}-{appellation}-{producer}.html"
browse:
  red: "https://www.moevenpick-wein.com/de/rotweine.html"
  by_country: "https://www.moevenpick-wein.com/de/rotweine/{country}.html"
  by_region: "https://www.moevenpick-wein.com/de/rotweine/{country}/{region}.html"
  by_grape: "https://www.moevenpick-wein.com/de/rebsorten/{grape}.html"
  by_producer: "https://www.moevenpick-wein.com/de/winzer/{country}/{producer}/"
  bio: "https://www.moevenpick-wein.com/de/bio.html"
robots_blocked: ["/*?*"]
---
# Mövenpick Wein — Extraction Guide

## Search
URL: `https://www.moevenpick-wein.com/de/catalogsearch/result/?q={query}`
Magento SSR search. Results show product cards with wine details.

## Price
- Format: `CHF {price}` per bottle
- Case pricing may also show
- "Auf Anfrage" for rare wines = not directly purchasable (email link instead)
- VAT included

## Ratings
Product detail page shows up to 8 critics in structured table:
- James Suckling, Robert Parker, Antonio Galloni, Falstaff
- Decanter, Gambero Rosso, Vinum, "Score" (generic)
- Format: critic name + numeric score

## Product Data (detail table — SSR)
- Wine name, vintage, producer
- Region/subregion (e.g. "Diverse Toskana")
- Rebsorte with percentages (grape varieties)
- Alkoholgehalt (ABV %)
- Ausbau (aging method)
- Energy values (kJ/kcal)
- Article number
- Customer reviews (with vintage info)

## Stock
- "Online Exklusiv" badge
- "Verfügbar in Filialen" → available in physical stores
- "Auf Anfrage" → not directly purchasable
- No exact stock counts

## Product URL Pattern
Verbose but predictable: `/de/{vintage}-{wine-name}-{appellation}-{producer}.html`
Each vintage has a separate URL.

## Failure Patterns
- Search may include non-wine results — filter by product type
- "Auf Anfrage" items cannot be added to cart
- robots.txt blocks `/*?*` but `.html` product pages are accessible
