---
name: shop-extraction
description: "Extract wine prices, ratings, and product data from Swiss retailer websites. Use when: 'extract price', 'shop scan', 'retailer lookup', 'scan shop', 'check retailer', 'shop price', 'extract from flaschenpost/globalwine/schubi/etc'. Supports 17 priority Swiss wine shops with per-shop extraction guides."
---

# Shop Extraction

On-demand extraction of structured wine data (prices, ratings, stock, tasting notes) from Swiss wine retailer websites. Designed for both large reasoning models and small local models with limited context.

## When to Use

- Extract current price for a specific wine from a specific retailer
- Scan a retailer for a wine's availability and product data
- Collect structured wine metadata (ratings, tasting notes, food pairings) from a shop page
- Validate a retailer URL still works and returns expected data

## Shared Rules

1. **Identity first.** Before extracting any data, verify you found the correct wine. See [identity rules](./references/identity-rules.md).
2. **Respect robots.txt.** If a shop blocks search in robots.txt, use category/producer browsing instead. Each shop file documents what is allowed.
3. **Rate limit.** Wait at least the `rate_limit_ms` specified in the shop's YAML frontmatter between requests to the same domain. Default: 2000ms.
4. **Output format.** Return data as JSON matching the [output schema](./references/output-schema.md). Do not invent fields.
5. **Facts only.** Extract only what the page shows. Do not infer, estimate, or hallucinate data.
6. **Report failures.** If a wine is not found, return `{"status": "not_found"}`. If the page structure doesn't match expectations, return `{"status": "extraction_failed", "reason": "..."}`.

## Workflow: Price Extraction

1. Read the shop skill file for the target retailer from `./shops/{shop_id}.md`
2. Construct the search or browse URL using the YAML frontmatter patterns
3. Fetch the page
4. Verify the wine identity (winery + wine name + vintage)
5. Extract price, stock, bottle size per the shop's extraction guide
6. Return JSON per the [price schema](./references/output-schema.md#price-observation)

## Workflow: Wine Research Extraction

1. Read the shop skill file for the target retailer
2. Navigate to the product page (via search or direct URL)
3. Extract all available metadata: ratings, tasting notes, food pairings, producer info
4. Return JSON per the [research schema](./references/output-schema.md#wine-research)

## Shop Index

| Shop | File | Search | Difficulty |
|------|------|--------|-----------|
| Flaschenpost | [flaschenpost.md](./shops/flaschenpost.md) | SSR `?search=` | Easy |
| Globalwine | [globalwine.md](./shops/globalwine.md) | SSR `?search=` | Easy |
| Smith & Smith | [smith-and-smith.md](./shops/smith-and-smith.md) | Browse/filter | Easy |
| Schubi Weine | [schubi-weine.md](./shops/schubi-weine.md) | SSR `?searchtext=` | Easy |
| Millésima | [millesima.md](./shops/millesima.md) | Blocked — use producer pages | Easy |
| DIVO | [divo.md](./shops/divo.md) | SSR `?q=` | Easy |
| Paul Ullrich | [paul-ullrich.md](./shops/paul-ullrich.md) | SSR `?search=` | Easy |
| Von Salis | [von-salis.md](./shops/von-salis.md) | Blocked — use categories | Easy |
| Vinothek Brancaia | [vinothek-brancaia.md](./shops/vinothek-brancaia.md) | Blocked — use categories | Easy |
| Mövenpick Wein | [moevenpick.md](./shops/moevenpick.md) | SSR Magento `?q=` | Easy |
| Bindella | [bindella.md](./shops/bindella.md) | None — browse categories | Easy |
| Gerstl | [gerstl.md](./shops/gerstl.md) | SSR `?q=` | Easy |
| Schuler 1694 | [schuler.md](./shops/schuler.md) | SSR `?search=` | Easy |
| Coop Mondovino | [coop-mondovino.md](./shops/coop-mondovino.md) | Blocked — use categories | Medium |
| Globus | [globus.md](./shops/globus.md) | Blocked (403) — use categories | Medium |
| Baur au Lac Vins | [baur-au-lac-vins.md](./shops/baur-au-lac-vins.md) | JS-only — use category cards | Medium |
| QoQa | [qoqa.md](./shops/qoqa.md) | Flash deals — RSS monitor | Medium |
