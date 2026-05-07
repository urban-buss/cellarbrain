---
shop_id: smith-and-smith
domain: smithandsmith.ch
display_name: "Smith & Smith"
currency: CHF
rate_limit_ms: 2000
catalogue_size: 2610
cms: Custom (allink)
search:
  url: null
  robots_ok: true
  note: "No text search URL. Use category browsing + lazy-load pagination."
product:
  url: "https://www.smithandsmith.ch/de/shop/{category}/{product-slug}"
browse:
  all: "https://www.smithandsmith.ch/de/shop"
  red: "https://www.smithandsmith.ch/de/shop/rotwein"
  white: "https://www.smithandsmith.ch/de/shop/weisswein"
  rose: "https://www.smithandsmith.ch/de/shop/rose"
  sparkling: "https://www.smithandsmith.ch/de/shop/schaumwein"
  bio: "https://www.smithandsmith.ch/de/shop?bio=true"
robots_blocked: []
---
# Smith & Smith — Extraction Guide

## Search Strategy
No text search URL available. To find a wine:
1. Browse the appropriate category URL (rotwein, weisswein, etc.)
2. Or use a web search engine: `"{wine name}" site:smithandsmith.ch`
3. The page loads 24 items initially, then "24 weitere laden" for more

## Price
- Format: `CHF {price}` on product card
- Sale: two prices, lower one is current
- Exact stock count shown: `{N} Stück verfügbar`

## Stock
- "{N} Stück verfügbar" → `in_stock: true`, `stock_count: N`
- No stock text → assume available if "In den Warenkorb" button present

## Product Data
All SSR on product cards:
- Wine name + vintage
- Producer name
- Region + country
- Price (CHF)
- Bottle size (75cl default)
- Badges: Bio, Vegan, Vinatur, awards
- Stock count
- Product image (.webp)

## Ratings
- Award badges visible on cards (medals, distinctions)
- No inline critic scores on listing pages

## Failure Patterns
- Wine not in category listing → not in catalogue
- Category page shows empty grid → no products matching filters
- All content in German only
