---
shop_id: baur-au-lac-vins
domain: bauraulacvins.ch
display_name: Baur au Lac Vins
currency: CHF
rate_limit_ms: 2000
catalogue_size: 2000
cms: Custom e-commerce
search:
  url: null
  robots_ok: true
  note: "?quicksearch= parameter exists but is JS-rendered — does NOT filter SSR results. Use category browsing or sitemap."
product:
  url: "https://www.bauraulacvins.ch/{lang}/p/{category}/{country}/{region}/{wine-slug}-{vintage}-{id}.html"
  note: "Deep hierarchical path: /p/{type}/{country}/{region?}/{sub-region?}/{slug}-{vintage}-{id}.html"
browse:
  all: "https://www.bauraulacvins.ch/en/r/all-products-1000000.html"
  red: "https://www.bauraulacvins.ch/en/r/red-wines-160.html"
  white: "https://www.bauraulacvins.ch/en/r/white-wines-1.html"
  rose: "https://www.bauraulacvins.ch/en/r/rose-wines-161.html"
  sparkling: "https://www.bauraulacvins.ch/en/r/sparkling-wines-162.html"
  sweet: "https://www.bauraulacvins.ch/en/r/sweet-wines-100606.html"
  fortified: "https://www.bauraulacvins.ch/en/r/fortified-wines-100607.html"
  spirits: "https://www.bauraulacvins.ch/en/r/spirits-164.html"
  fine_rare: "https://www.bauraulacvins.ch/en/fine-rare-_content---1--850.html"
  organic: "?labelFilter=2"
  sitemap: "https://www.bauraulacvins.ch/myinterfaces/cms/googlesitemap-overview.xml"
food_pairing_urls:
  aperitif: "https://www.bauraulacvins.ch/en/aperitif-_content---1--945--10048756.html"
  fish: "https://www.bauraulacvins.ch/en/fish-_content---1--946--10011719.html"
  seafood: "https://www.bauraulacvins.ch/en/seafood-_content---1--947--10143718.html"
robots_blocked: ["/admin/", "BLEXBot", "SEOkicks", "MJ12bot"]
---
# Baur au Lac Vins — Extraction Guide

## Search Strategy
Search (`?quicksearch=`) is **JS-rendered only** — SSR returns unfiltered product listing.

Use category browsing instead:
- Red wines: `/en/r/red-wines-160.html`
- White: `/en/r/white-wines-1.html`
- Sparkling: `/en/r/sparkling-wines-162.html`
- Fine & Rare: `/en/fine-rare-_content---1--850.html`
- Or web search: `"{wine name}" site:bauraulacvins.ch`

## Multi-Language
3 languages: replace `/en/` with `/de/` or `/fr/`:
- `/en/r/red-wines-160.html` → `/de/r/rotweine-160.html`

## Price
- Format: `CHF {price}` per bottle
- Sale: `CHF 77.90 CHF 99.80` — new price first, then original (no "statt")
- Seasonal promotions: "15% price advantage"
- Free shipping from CHF 200

## Product URL Pattern
Hierarchical and verbose:
`/{lang}/p/{category}/{country}/{region?}/{sub-region?}/{wine-slug}-{vintage}-{id}.html`

Examples:
- `/en/p/red-wines/spain/mallorca/ribas-negre-2021-37069721.html`
- `/en/p/white-wines/france/bordeaux/pessac-leognan/la-clarte-de-haut-brion-2011-11107711.html`
- `/en/p/sparkling-wines/france/champagne/philipponnat-reserve-perpetuelle-brut-ex-royale-reserve-60041700.html`

**Product ID** is the trailing numeric code: `37069721`, `11107711`.

## Product Data (SSR listing cards)
- Wine name: link text ("Ribas negre 2021")
- Producer: card description ("Ribas")
- Vintage: in name + URL
- Price: `CHF 25.50`
- Appellation: "VdT", "AC Pessac-Léognan"
- Bottle size in ml: "750 ml", "1500 ml"
- Stock: "In Stock" badge
- Default order quantity: 6 for wines, 1 for spirits

## Vintage & Size Differentiation
- **Separate product URLs per vintage** with different IDs
- **Separate product URLs per size**: Sió negre 750ml (`37060722`, CHF 36) vs 1500ml (`37060822`, CHF 81.50)

## Ratings
- Not observed on listing cards
- May exist on detail pages — inspect individually

## Tasting Notes
- Not observed on listing cards
- Likely on detail pages

## Stock
- "In Stock" badge on listing cards → `in_stock: true`
- Missing badge → `in_stock: false`

## Fine & Rare (Unique Feature)
Dedicated section for investment-grade wines — both **buying and selling** collectible wines.
URL: `/en/fine-rare-_content---1--850.html`

## Failure Patterns
- Search does not work for SSR extraction
- Product URL slugs contain content IDs (e.g. `_content---1--850.html`)
- Custom platform — non-standard patterns
- Ratings and tasting notes may require individual product page visits
- Some bot user agents blocked (BLEXBot, SEOkicks, MJ12bot)
