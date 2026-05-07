---
shop_id: qoqa
domain: qoqa.ch
display_name: QoQa
currency: CHF
rate_limit_ms: 2000
catalogue_size: 0
cms: Custom (flash-sale platform)
search:
  url: null
  note: "No search — flash-sale model with daily rotating wine deals. No persistent catalogue."
product:
  url: null
  note: "Deal URLs are ephemeral — valid only during the deal window (hours to days)."
browse:
  live_deals_fr: "https://www.qoqa.ch/fr/live"
  wine_vertical_de: "https://www.qoqa.ch/de/wine"
  rss_feed: "Available — check qoqa.ch for current RSS URL"
robots_blocked: []
---
# QoQa — Extraction Guide

## IMPORTANT: Not a Traditional Wine Shop
QoQa is a **flash-sale platform**. Wine deals rotate daily with countdown timers and limited quantities. There is:
- **No persistent catalogue** to search
- **No search URL** for specific wines
- **No stable product URLs** — deals expire in hours/days
- Only 3–8 active wine deals at any time

## When to Use QoQa
Only for **deal monitoring** — alerting when a cellar-matching wine appears on sale.
NOT suitable for systematic price comparison or market research.

## Access Pattern
```
https://www.qoqa.ch/fr/live   — Active deals (French)
https://www.qoqa.ch/de/wine   — Wine vertical (German)
```

An **RSS feed** may be available for automated deal monitoring.

## Price
- Per-case/lot pricing with per-bottle breakdown
- All deals are already discounted from retail — discount clearly shown
- Format: `CHF {price}` per lot/case
- Look for per-bottle breakdown in deal description

## Stock
- **Excellent transparency**: stock remaining shown as percentage (`X% verbleibend`)
- Countdown timer shows deal expiry
- When stock hits 0% or timer expires, deal is gone

## Data Available per Deal (SSR)
| Field | Available |
|-------|-----------|
| Wine name | Yes |
| Price (CHF) | Yes |
| Original price / discount % | Yes |
| Stock remaining (%) | Yes |
| Countdown timer | Yes |
| Deal description / tasting notes | Yes (in marketing copy) |
| Product image | Yes |
| Ratings | Sometimes, in deal copy |
| Food pairing | Sometimes, in deal copy |

## Integration Strategy
Best approach: **poll RSS feed or deal page periodically**, fuzzy-match wine names against cellar, alert on matches.

## Failure Patterns
- Wines appear and disappear unpredictably — no historical access
- Primarily case/lot buying, not single bottles
- Bilingual (FR/DE) — same deal may appear in both
- Deal descriptions are marketing copy, not structured data
- Cannot be used for persistent price tracking
