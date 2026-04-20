---
shop_id: coop-mondovino
domain: coop.ch
display_name: Coop Mondovino
currency: CHF
rate_limit_ms: 5000
catalogue_size: 2000
cms: SAP Hybris
search:
  url: null
  robots_ok: false
  note: "Search blocked by robots.txt — /search* and /*?text=* disallowed. Crawl-delay: 5. Use category browsing or sitemap."
product:
  url: "https://www.coop.ch/de/weine/alle-weine/{type}/{country}/{wine-slug}/p/{product-id}"
  alt_url: "https://www.coop.ch/de/weine/spezialitaeten/raritaeten/{wine-slug}/p/{product-id}"
browse:
  all: "https://www.coop.ch/de/weine/alle-weine/c/m_2508"
  red: "https://www.coop.ch/de/weine/alle-weine/rotweine/c/m_0223"
  white: "https://www.coop.ch/de/weine/alle-weine/weissweine/c/m_0224"
  rose: "https://www.coop.ch/de/weine/alle-weine/roseweine/c/m_0225"
  sparkling: "https://www.coop.ch/de/weine/alle-weine/schaumweine/c/m_0226"
  champagne: "https://www.coop.ch/de/weine/alle-weine/champagner/c/m_0227"
  swiss: "https://www.coop.ch/de/weine/herkunft/schweizer-weine/c/m_0228"
  italian: "https://www.coop.ch/de/weine/herkunft/italienische-weine/c/m_0231"
  french: "https://www.coop.ch/de/weine/herkunft/franzoesische-weine/c/m_0229"
  rarities: "https://www.coop.ch/de/weine/spezialitaeten/raritaeten/c/m_2511"
  on_sale: "https://www.coop.ch/de/weine/aktionen/c/SPECIAL_OFFERS_WINE"
  subscriptions: "https://www.coop.ch/de/weine/spezialitaeten/subskriptionen/c/m_7614"
  by_grape: "https://www.coop.ch/de/weine/rebsorten/rote-rebsorten/{grape}/c/m_{id}"
  sitemap: "https://www.coop.ch/sitemap.xml"
facet_filters:
  country: "?q=:relevance:wineCountryFacet:WINE_ORIGIN_COUNTRY_{code}"
  type: "?q=:relevance:wineTypeFacet:WINE_TYPE_{code}"
  on_sale: "?q=:relevance:specialOfferFacet:true"
  organic: "?q=:relevance:bioFacet:true"
robots_blocked: ["/search*", "/*?text=*", "/*?sort=*", "/*?*:posFilter:*"]
---
# Coop Mondovino — Extraction Guide

## Search Strategy
Search is **blocked** by robots.txt (`/search*`, `/*?text=*`). Crawl-delay: 5 seconds.

Use category browsing instead:
- Red wines: `/de/weine/alle-weine/rotweine/c/m_0223`
- By country: `/de/weine/herkunft/italienische-weine/c/m_0231`
- Rarities: `/de/weine/spezialitaeten/raritaeten/c/m_2511`
- By grape: `/de/weine/rebsorten/rote-rebsorten/merlot/c/m_5571`
- Or use web search: `"{wine name}" site:coop.ch`

**Note**: `mondovino.ch` redirects to `coop.ch/de/weine/`.

## Price
- Format: `CHF {price}` per bottle
- Also shows: `Preis pro 10 Zentiliter: {price}/10cl`
- Sale: `50% CHF 29.70 statt CHF 59.70` — percentage + new price + "statt" + original
- Volume: `20% Mengenrabatt ab 12 Flaschen`, `Online 10% ab 6`
- "Raritäten sind von allen Aktionen ausgeschlossen" — rarities excluded from promotions
- Use the single-bottle non-discounted price

## Ratings (Excellent)
Product pages show **multiple critics** in structured format:
- James Suckling: 97
- Antonio Galloni: 96
- Wine Enthusiast: 96
- Luca Maroni: 95
- Falstaff: 95
- Robert Parker: 95
- Jeb Dunnuck: 91
- Jancis Robinson: 17 (out of 20)

All shown on a single product page (e.g. Tignanello).

## Tasting Notes
Expert tasting notes with **named expert** (e.g. Jan Schwarzenbach):
- Full prose description: "Dunkles Rubinrot, hohe Intensität..."
- Drinking window: "Genussreife 2026-2034"
- Serving temperature: "Trinktemperatur 16-18°C"

## Food Pairing
Structured list: "Fremdländische Küche, Salate, Vegetarisch, Rind, Wild/Lamm, Pasta/Reisgerichte, Schwein, Käsegerichte, Vorspeisen, Kalb, Genussweine"

## Product Data (SSR)
- Wine name (H1): "Toscana IGT Tignanello Antinori"
- Producer: "Produzent Marchesi Antinori"
- Vintage: "Jahrgang 2020"
- Grape varieties: "Sangiovese, Cabernet Sauvignon, Cabernet Franc"
- ABV: "14.0%"
- Region: "Italien, Toskana"
- Bottle size: "75cl" (selector for half-bottles as separate products)
- Customer reviews: "Bewertungen (35)" with average score

## Stock
- "In den Warenkorb" button → `in_stock: true`
- No exact stock count
- Half-bottles (37.5cl) are separate product URLs with different IDs

## Vintage & Size
- **Separate product URLs per vintage** — different product IDs (e.g. `/p/1008024016` vs `/p/1008024018`)
- **Half-bottles as separate products** — 37.5cl at CHF 68 vs 75cl at CHF 135

## Multi-Language
4 languages with parallel URLs:
- `/de/weine/...` (German)
- `/fr/vins/...` (French)
- `/it/vini/...` (Italian)
- `/en/wines/...` (English)

## Failure Patterns
- Search blocked — must navigate via categories or sitemap
- Crawl-delay: 5 seconds — respect this strictly
- Non-wine items in results (Grappa Tignanello appeared alongside wine)
- Opaque numeric product IDs — no pattern to predict
- Many bot user agents blocked entirely in robots.txt
