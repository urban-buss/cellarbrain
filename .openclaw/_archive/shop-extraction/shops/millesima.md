---
shop_id: millesima
domain: de.millesima.ch
display_name: "Millésima"
currency: CHF
rate_limit_ms: 2000
catalogue_size: 12000
cms: Custom (Bordeaux-based)
search:
  url: null
  robots_ok: false
  note: "Search blocked by robots.txt (*q=* disallowed). Navigate via producer or appellation pages."
product:
  url: "https://de.millesima.ch/{wine-slug}-{vintage}.html"
browse:
  by_producer: "https://de.millesima.ch/produzent-{producer-slug}.html"
  by_appellation: "https://de.millesima.ch/{appellation-slug}.html"
  all_producers: "https://de.millesima.ch/all-estates.html"
robots_blocked: ["*q=*", "*producer=*", "*price=*", "*bottlesize=*", "*vintage=*", "*filter=*", "*p=*"]
---
# Millésima — Extraction Guide

## Search Strategy
Search is **blocked** by robots.txt. Instead, navigate via:
- Producer page: `https://de.millesima.ch/produzent-{producer-slug}.html`
- Appellation page: `https://de.millesima.ch/{appellation-slug}.html`
- Producer slug: lowercase, hyphens for spaces (e.g. `chateau-lynch-bages`)

## Price
- **Case pricing is default**: `CHF 877.00 inkl. MwSt.` = price per case (typically 6 bottles)
- **Per-unit price**: `( CHF 146.17 / Einheit )` — use THIS as the per-bottle price
- **Bottle size**: default 75cl unless URL or text specifies otherwise (magnums, half-bottles exist)
- Sale: original price struck-through, new price shown. "Sale" badge visible
- Volume discount: "-5% ab 12 Flaschen" noted on some listings
- "zzgl. MwSt." = ex-VAT (subskription wines). Use "inkl. MwSt." price when available

## Ratings
Shown as abbreviated badges on listing cards:
- `RP 96` → Robert Parker 96/100
- `JR 18+` → Jancis Robinson 18+/20
- `JS 97` → James Suckling 97/100
- `WS 96` → Wine Spectator 96/100
- `DE 98` → Decanter 98/100
- `RG 19` → Revue du Vin de France (Guide) 19/20
- `BD 93` → Bettane+Desseauve 93/100
- `VG 91` → Vinous (Antonio Galloni) 91/100
- Range scores: `RP 93-95` → use lower bound as score, note range in notes

**Note**: JR scores are out of 20, not 100. RG scores are out of 20.

## Stock
- "am Lager" → `in_stock: true`
- "In den Einkaufskorb" button → `in_stock: true`
- "Subskriptionsweine – lieferbar Frühjahr {year}" → `in_stock: false`, notes: `"en primeur, delivery {year}"`
- "Informieren Sie mich" → `in_stock: false`

## Product Data (listing cards)
- Wine name + vintage in title link
- Appellation (e.g. "Bordeaux - Pauillac")
- Producer name
- Short tasting description (1-2 sentences)
- Up to 3 critic rating badges
- "Nachhaltiger Weinbau" (HVE sustainability) badge

## Failure Patterns
- Producer not in catalogue → page 404 or empty listing
- Wine exists as "Informieren Sie mich" → not purchasable, future vintage
- Second wines (Echo de Lynch-Bages) listed alongside Grand Vin — verify wine name matches
