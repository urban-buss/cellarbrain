# Source Routing — Per-Region Research Paths

## Route by Region

| Region | Research path (in order) |
|---|---|
| **Bordeaux** | Wine-Searcher → Millesima → Wine Cellar Insider → Wine-Searcher vintage page → Wikipedia |
| **Swiss-market** (Ticino, etc.) | Wine-Searcher → Mövenpick → Schuler.ch → Producer website |
| **South Africa** | Wine-Searcher → winemag.co.za → Producer website |
| **Italy** | Wine-Searcher → Millesima → Falstaff / Gambero Rosso → Producer website |
| **Rhône / Burgundy / Champagne** | Wine-Searcher → Millesima → Mövenpick → Producer website |
| **All other regions** | Wine-Searcher → Producer website → Mövenpick → Gerstl |

## Source Details

### Wine-Searcher (first for every wine)

- Search: `https://www.wine-searcher.com/find/{winery}+{wine-name}+{vintage}`
- Vintage pages: `https://www.wine-searcher.com/vintage-{year}-{region}`
- Extracts: average price, grape variety, Swiss retailers, community rating, critic excerpts
- 100% hit rate. Best for Swiss retail discovery and vintage narratives.
- **Limitation:** No full tasting notes. Vintage pages only for major regions.

### Millésima (richest for Bordeaux & French wines)

- Product: `https://www.millesima.co.uk/{wine-slug}-{vintage}.html`
- Producer: `https://www.millesima.co.uk/producer-{producer-slug}.html`
- CHF pricing: `https://de.millesima.ch/{wine-slug}-{vintage}.html`
- Slug: lowercase, hyphens (e.g. `chateau-lynch-moussas-2018`)
- **Search blocked** — direct URLs only.
- Extracts: 10–20+ critic scores with quotes, blend, harvest, barrel, producer history.

### Mövenpick Wein

- Product: `https://www.moevenpick-wein.com/de/{vintage}-{wine-slug}.html`
- Search: `https://www.moevenpick-wein.com/de/catalogsearch/result/?q={winery}+{vintage}`
- Extracts: up to 8 critic scores, German tasting notes, food pairings, drinking window, producer bio.

### Wine Cellar Insider (Bordeaux only)

- URL: `https://www.thewinecellarinsider.com/bordeaux-wine-producer-profiles/bordeaux/{bank}/{appellation}/{property}/`
- Use `left-bank` or `right-bank`. Try both spelling variants (e.g. `st-estephe` / `saint-estephe`).
- Budget: 2 URL attempts max. ~40% hit rate.

### Regional Specialists

| Region | Source | Strengths |
|---|---|---|
| South Africa | winemag.co.za | Full professional reviews, blind tastings |
| Switzerland | Schuler.ch | Niche wines, awards, CHF pricing |
| Italy | Falstaff | Scores, producer profiles |
| Any | Wikipedia | Classification history (Bordeaux only) |
| Any | Gerstl.ch (`/c?q={winery}`) | German editorial, producer profiles |

### Producer Website (supplementary)

- Try `/about`, `/history`, `/weingut`, `/the-estate`, `/our-wines`
- Budget: 1–2 attempts. ~30% success rate (JS/cookie walls block many).

### Producer Fallback Chain (when producer website fails)

If the producer website is blocked, empty, or unavailable, try in order:

1. **Wine-Searcher producer snippet** — The product page (already fetched) often contains founding year, owner, region. Check existing content first.
2. **Millesima producer page** — `https://www.millesima.co.uk/producer-{producer-slug}.html` — dedicated producer profiles.
3. **Swiss retailer bios** — Gerstl (`/c?q={winery}`) and Mövenpick product pages embed producer descriptions.
4. **Google webcache** — `https://webcache.googleusercontent.com/search?q=cache:{producer-url}` — 1 attempt only.

Budget: 2–3 additional fetches total. If all fail → skip `producer_profile`.

### Vivino (last resort only)

- Only when all above sources yield insufficient data
- Extract: aggregate score + rating count, taste profile themes if >100 reviews
- **Do not** cite individual reviews, "average price", or generic pairings

## Source Tiers

| Tier | Sources | Trust |
|---|---|---|
| **1a** | Winery / estate homepage | High |
| **1b** | Millésima, Wine-Searcher (incl. Producer tab), Mövenpick, Gerstl | High |
| **2** | Wine Cellar Insider, winemag.co.za, Schuler.ch, Wikipedia | Medium-High |
| **3** | Vivino, CellarTracker | Low — last resort |
| **Blocked** | James Suckling, Wine Spectator, Decanter, Wine Enthusiast, Tim Atkin | Inaccessible |
| **Avoid** | Personal blogs, unknown sites, AI content, social media | Do not cite |

## Vintage Data Gaps

Check this map **before** fetching a Wine-Searcher vintage page. For regions marked **No**, skip the vintage-page fetch and extract vintage commentary from retailer product pages instead.

| Region | Has vintage page | Availability | Action |
|---|---|---|---|
| Bordeaux | **Yes** | ~95% | Wine-Searcher + Millesima |
| Burgundy, Rhône, Champagne | **Yes** | ~80% | Wine-Searcher vintage pages |
| Italy (Piedmont, Tuscany) | **Yes** (less detailed) | ~60% | Wine-Searcher vintage pages |
| South Africa | Partial | ~50% | winemag.co.za, WOSA |
| Argentina (Mendoza) | **No** | ~10% | Rarely published — skip vintage fetch |
| Armenia, Ticino, Languedoc, Alentejo | **No** | ~0% | Accept the gap — skip vintage fetch |
| Any, very recent vintage (year −1) | **No** | ~10% | Too recent — skip vintage fetch |
