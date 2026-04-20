---
name: market-research
description: "Market pricing and availability research for wine dossiers via cellarbrain MCP. Only writes verified facts."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Market Research

Defensive, fact-only pricing and availability research for the `market_availability` dossier section. Every price must come from a web page visited in this session.

**Scope:** Only `market_availability`. Does NOT write other sections — use the `wine-research` skill for those.

## Cardinal Rules

1. **Facts only.** Every price must trace to a web source visited in this session.
2. **Identify before you write.** Confirm winery + vintage + appellation. If uncertain, STOP.
3. **Skip over guess.** No data? Leave pending.
4. **MCP only.** Read via `read_dossier`, write via `update_dossier`.
5. **Never fabricate prices.** If you didn't find it, don't estimate.

## Owner Context

- Switzerland, CHF primary (EUR/USD as reference)
- Swiss retailers: Gerstl, Martel, Flaschenpost, Mövenpick, wine.ch

## Confidence Gate

| Level | Criteria | Action |
|---|---|---|
| **High** | ≥2 sources agree on pricing | Write |
| **Medium** | 1 reliable source (tier 1–3) | Write, note single-source |
| **Low** | Only retailer data or conflicts | **SKIP** |
| **None** | No data | **SKIP** |

## Workflow

### Phase 1 — Load & Understand

1. `read_dossier(wine_id, sections=[])` — minimal load
2. Check `market_availability` is in `agent_sections_pending`. If not, stop.
3. Extract identity: `full_name`, `winery`, `vintage`, `list_price`, `list_currency`
4. Build search: `"{winery}" "{vintage}" {region}`

### Phase 2 — Web Research

Run all four searches:

| # | Query | Purpose |
|---|---|---|
| 1 | `"{winery}" "{vintage}" price buy wine` | Retail pricing |
| 2 | `"{winery}" "{vintage}" Wine-Searcher` | Aggregated prices |
| 3 | `"{winery}" "{vintage}" site:gerstl.ch OR site:martel.ch OR site:flaschenpost.ch` | Swiss availability |
| 4 | `"{winery}" "{vintage}" auction OR secondary market` | Older/rare wines only |

### Phase 3 — Identity Verification

Verify: winery, vintage (exact), region, category. Any mismatch → STOP.

### Phase 4 — Write Section

`update_dossier(wine_id, "market_availability", content)` — structure:

1. **Current retail price range** — Wine-Searcher average and/or specific retailers
2. **Swiss availability** — Gerstl, Martel, Flaschenpost, Mövenpick, wine.ch with CHF prices
3. **International benchmark** — EUR/USD prices for comparison
4. **Purchase price comparison** — current vs. owner's `list_price` (appreciation/decline)
5. **Secondary market** — only for ≥10-year or classified wines
6. **Availability outlook** — in production / sold out / secondary only

End with `Sources: [list URLs visited]`

### Phase 5 — Verify & Report

1. `read_dossier(wine_id)` to confirm write
2. Report: written/skipped, sources consulted, key findings (price, vs. purchase, Swiss availability)

## Batch Research

1. `pending_research(limit=N)` — filter to wines with `market_availability` pending
2. One wine at a time
3. Summary table: wine, status, price vs. purchase, notes

## What You Must Never Do

- Fabricate prices
- Write Low/None confidence data
- Overwrite populated sections
- Write sections other than `market_availability`
- Edit dossier files directly
