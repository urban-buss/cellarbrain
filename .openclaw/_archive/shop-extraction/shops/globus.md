---
shop_id: globus
domain: globus.ch
display_name: Globus
currency: CHF
rate_limit_ms: 5000
catalogue_size: 800
cms: Custom (Akeneo PIM)
search:
  url: null
  robots_ok: false
  note: "Search blocked by robots.txt AND returns 403 Forbidden. Use category browsing only."
product:
  url: "https://www.globus.ch/{slug}-rotwein-bp{article-number}"
  note: "URL slugs inconsistent — some use producer names, others internal codes like 'verglb-marken-deli'"
browse:
  red: "https://www.globus.ch/wein-delicatessa/wein/rotwein"
  white: "https://www.globus.ch/wein-delicatessa/wein/weisswein"
  sparkling: "https://www.globus.ch/wein-delicatessa/wein/champagner-schaumweine"
  top10: "https://www.globus.ch/wein-delicatessa/wein/top-10-weine"
  by_country: "https://www.globus.ch/wein-delicatessa/wein/rotwein?produktionsland={code}"
  by_region: "https://www.globus.ch/wein-delicatessa/wein/rotwein?weinregion={region}"
  by_grape: "https://www.globus.ch/wein-delicatessa/wein/rotwein?_={grape}"
  by_producer: "https://www.globus.ch/wein-delicatessa/wein/rotwein?wein_produzent=pro_{slug}"
  sitemap: "https://www.globus.ch/sitemap.xml"
filter_codes:
  countries: "it, fr, es, pt, us, ar, za"
  grapes: "cabernetsauvignon, merlot, pinotnoir, chardonnay"
robots_blocked: ["/suche", "/fr/recherche", "/en/search", "*?page", "*?marken"]
---
# Globus — Extraction Guide

## Search Strategy
Search is **blocked** (robots.txt disallows `/suche`, 403 Forbidden).

Use category browsing:
- Red wines: `/wein-delicatessa/wein/rotwein`
- By country: `?produktionsland=it` (Italy), `fr`, `es`, `pt`, `us`, `ar`, `za`
- By region: `?weinregion=bordeaux`, `toskana`, etc.
- By grape: `?_=cabernetsauvignon`, `merlot`, `pinotnoir`
- By producer: `?wein_produzent=pro_veuve_clicquot`, etc.
- Or web search: `"{wine name}" site:globus.ch`

## Price
- Format: `CHF {price}` per bottle
- Also shows per 100ml: `CHF {price} / 100 ml`
- Sale: `CHF 25.00 → CHF 20.00 -20%` — new price + percentage discount
- No volume/case discounts observed

## Ratings
- **No critic ratings** on product pages (Parker, Suckling, etc. absent)
- Not useful for rating extraction — use other shops

## Tasting Notes (Good Quality)
Full prose in product description:
- "duftet intensiv nach dunklen Früchten, nach Zedernholz..."
- Aromas listed separately: "Brombeere, Cassis, Schokolade, Lakritze/Süssholz, rotfruchtig"
- Serving temperature: "Ideale Temperatur liegt zwischen 16-18°C"

## Food Pairing
`Passt zu:` in description: "italienischen Speisen aller Art, rotem Fleisch"

## Product Data (SSR)
- Wine name (H1): "ANTINORITignanello Jahrgang 2022" (note: no space between producer and wine name)
- Producer: icon line "Antinori"
- Vintage: "Variante: Jahrgang 2022" — **variant selector** (not separate URLs)
- Region/country: "Aus der Toskana, Italien"
- Grape varieties: "Cabernet Franc, Cabernet Sauvignon, Sangiovese"
- Bottle size: "Grösse: 75cl"
- ABV: "13.5% Alkoholgehalt"
- Article number: "Artikelnummer: 1339350700"
- Producer info: "Über den Produzenten" section
- Drinking window: "Erste Trinkreife" note

## Vintage Handling
Vintages are **variants on one product page** (dropdown selector), not separate URLs.
This makes vintage-specific price tracking harder — check the variant picker.

## Stock
- "versendet in 1-2 Tagen" → `in_stock: true`
- In-store availability check available via link
- "+1 Variante" on listing cards = size or vintage variants exist

## Anti-Bot
- **Aggressive**: 100+ bot user-agents blocked in robots.txt
- fraud0 tracking pixel present
- Age verification required for alcohol at checkout

## Multi-Language
3 languages: DE, FR, EN with parallel sitemaps.

## Failure Patterns
- Search completely unavailable
- No critic ratings — only tasting notes and food pairing
- Vintage as variant (not separate URL) — cannot deep-link to specific vintage
- Product URL slugs inconsistent (mix of real producer names and internal codes)
- Modest catalogue (~800 wines)
