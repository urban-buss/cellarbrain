---
name: shop-extraction
description: "Extract wine prices, ratings, and product data from 17 Swiss retailer websites with per-shop extraction guides."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Shop Extraction

Structured data extraction from Swiss wine retailers. Each shop has a dedicated extraction guide with URL patterns, field locations, and rate limits.

## When to Use

- Extract price for a specific wine from a specific retailer
- Scan a retailer for availability and product data
- Collect ratings, tasting notes, food pairings from a shop page
- Validate retailer URLs

## Shared Rules

1. **Load the shop guide first.** Read `./shops/{shop_id}.md` before visiting any retailer.
2. **Identity first.** Verify winery + wine name + vintage before extracting. See [identity rules](./references/identity-rules.md).
3. **Respect robots.txt.** If search is blocked, use category/producer browsing per the shop file.
4. **Rate limit.** Wait at least `rate_limit_ms` from the shop's frontmatter between requests.
5. **Output format.** Return JSON per the [output schema](./references/output-schema.md).
6. **Facts only.** Extract only what the page shows. Never infer or estimate.
7. **Report failures.** Not found → `{"status": "not_found"}`. Extraction error → `{"status": "extraction_failed", "reason": "..."}`.

## Workflow: Price Extraction

1. Read `./shops/{shop_id}.md`
2. Construct search/browse URL from YAML frontmatter
3. Fetch the page
4. Verify wine identity
5. Extract price, stock, bottle size per the shop guide
6. Return JSON per [price schema](./references/output-schema.md#price-observation)

## Workflow: Wine Research Extraction

1. Navigate to product page
2. Extract all metadata: ratings, tasting notes, food pairing, grapes, ABV, drinking window
3. Return JSON per [research schema](./references/output-schema.md#wine-research)

## Workflow: Multi-Shop Scan

1. For each target shop, run single-wine workflow
2. Return summary: shop, price, stock, URL

## Shop Index

| Shop | File | Search | Difficulty |
|------|------|--------|-----------|
| Flaschenpost | [flaschenpost](./shops/flaschenpost.md) | SSR `?search=` | Easy |
| Globalwine | [globalwine](./shops/globalwine.md) | SSR `?search=` | Easy |
| Smith & Smith | [smith-and-smith](./shops/smith-and-smith.md) | Browse/filter | Easy |
| Schubi Weine | [schubi-weine](./shops/schubi-weine.md) | SSR `?searchtext=` | Easy |
| Millésima | [millesima](./shops/millesima.md) | Blocked — producer pages | Easy |
| DIVO | [divo](./shops/divo.md) | SSR `?q=` | Easy |
| Paul Ullrich | [paul-ullrich](./shops/paul-ullrich.md) | SSR `?search=` | Easy |
| Von Salis | [von-salis](./shops/von-salis.md) | Blocked — categories | Easy |
| Vinothek Brancaia | [vinothek-brancaia](./shops/vinothek-brancaia.md) | Blocked — categories | Easy |
| Mövenpick Wein | [moevenpick](./shops/moevenpick.md) | SSR Magento `?q=` | Easy |
| Bindella | [bindella](./shops/bindella.md) | None — browse | Easy |
| Gerstl | [gerstl](./shops/gerstl.md) | SSR `?q=` | Easy |
| Schuler 1694 | [schuler](./shops/schuler.md) | SSR `?search=` | Easy |
| Coop Mondovino | [coop-mondovino](./shops/coop-mondovino.md) | Blocked — categories | Medium |
| Globus | [globus](./shops/globus.md) | Blocked (403) — categories | Medium |
| Baur au Lac Vins | [baur-au-lac-vins](./shops/baur-au-lac-vins.md) | JS-only — category cards | Medium |
| QoQa | [qoqa](./shops/qoqa.md) | Flash deals — RSS | Medium |
