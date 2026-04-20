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

### Vivino (last resort only)

- Only when all above sources yield insufficient data
- Extract: aggregate score + rating count, taste profile themes if >100 reviews
- **Do not** cite individual reviews, "average price", or generic pairings

## Source Tiers

| Tier | Sources | Trust |
|---|---|---|
| **1a** | Winery / estate homepage | High |
| **1b** | Millésima, Wine-Searcher, Mövenpick, Gerstl | High |
| **2** | Wine Cellar Insider, winemag.co.za, Schuler.ch, Wikipedia | Medium-High |
| **3** | Vivino, CellarTracker | Low — last resort |
| **Blocked** | James Suckling, Wine Spectator, Decanter, Wine Enthusiast, Tim Atkin | Inaccessible |
| **Avoid** | Personal blogs, unknown sites, AI content, social media | Do not cite |

## Vintage Data Gaps

| Region | Availability | Action |
|---|---|---|
| Bordeaux | ~95% | Wine-Searcher + Millesima |
| Burgundy, Rhône, Champagne | ~80% | Wine-Searcher vintage pages |
| Italy (Piedmont, Tuscany) | ~60% | Wine-Searcher (less detailed) |
| South Africa | ~50% | winemag.co.za, WOSA |
| Argentina (Mendoza) | ~10% | Rarely published |
| Armenia, Ticino, Languedoc | ~0% | Accept the gap |
| Any, very recent vintage | ~10% | Too recent for reports |
