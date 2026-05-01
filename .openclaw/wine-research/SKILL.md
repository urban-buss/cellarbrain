---
name: wine-research
description: "Fact-only wine research populating dossier sections via cellarbrain MCP. Covers producer, vintage, description, ratings, tasting notes, food pairings, similar wines."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Wine Research

Defensive, fact-only research for populating agent-owned dossier sections. Every claim must trace to a web source visited in this session — never from training data alone.

**Scope:** `producer_profile`, `vintage_report`, `wine_description`, `ratings_reviews`, `tasting_notes`, `food_pairings`, `similar_wines`. Does NOT write `market_availability` — use the `market-research` skill.

## Cardinal Rules

1. **Facts only.** Every claim must trace to a web page visited in this session.
2. **Identify before you write.** Confirm winery + vintage + appellation before writing. If uncertain, STOP.
3. **Skip over guess.** No data? Leave pending. Nothing > something wrong.
4. **MCP only.** Read via `read_dossier`, write via `update_dossier`. Never edit files directly.
5. **One section at a time.** Separate `update_dossier` call per section.
6. **Never touch `market_availability`.** That belongs to the `market-research` skill.

## Owner Context

- Switzerland, CHF (EUR/USD as reference)
- Swiss retailers: Gerstl, Martel, Flaschenpost, Mövenpick
- Notes may be in German

## Confidence Gate

| Level | Criteria | Action |
|---|---|---|
| **High** | ≥2 independent sources agree | Write |
| **Medium** | 1 reliable tier 1–2 source | Write, note single-source |
| **Low** | Only community data or conflicts | **SKIP** |
| **None** | No data found | **SKIP** |

## Workflow: Single Wine

### Phase 1 — Load & Understand

1. `read_dossier(wine_id, sections=[])` — minimal load (frontmatter + H1 only)
2. Extract: `full_name`, `winery`, `vintage`, `category`, `list_price`
3. Check `agent_sections_pending` — only research pending sections
4. **Pre-resolve ambiguous identity.** If the wine name is generic or shared across producers (e.g. "Ilumina", "Reserva", "Grand Vin"), use `query_cellar` to look up winery, region, and grapes from the wines table. Build search identity from resolved metadata — not the wine name alone.
5. Build search identity: `"{winery}" "{vintage}" {region} {classification}`

### Phase 2 — Web Research

Use region-routed, vendor-first strategy. See [source routing](./references/source-routing.md) for per-region research paths and source tiers.

**Quick order:** Wine-Searcher → Millésima → Mövenpick → Regional specialist → Producer website → **Producer fallback chain** (Wine-Searcher Producer tab → Millesima producer page → Swiss retailer bios → Google webcache) → Vivino (last resort)

### Phase 3 — Identity Verification

Before writing, verify ALL match:
- Winery name (allow Château/Chateau)
- Vintage (exact year)
- Region/appellation
- Category (red/white/etc.)

If ANY fails → STOP, report discrepancy, write nothing.

### Phase 4 — Write Sections

For each pending section with High/Medium confidence, call `update_dossier(wine_id, section, content)`.

| Section | Length | Key content |
|---|---|---|
| `producer_profile` | 150–300 words | History, philosophy, vineyard, terroir, classification |
| `vintage_report` | 100–200 words | Growing season, weather, harvest (skip for NV wines). Consult the vintage-page region map before fetching — skip for unmapped regions. |
| `wine_description` | 150–250 words | Style, nose, palate (only from reviews — never invent) |
| `ratings_reviews` | Table | Verified scores only; "reportedly X" for secondhand |
| `tasting_notes` | 100–200 words | Professional consensus; note review count |
| `food_pairings` | 5–8 bullets | From critics/winery/pairing databases |
| `similar_wines` | 3–5 wines | Same region/style; check cellar via `find_wine` |

Every section ends with `Sources: [list URLs visited]`.

### Phase 5 — Verify

1. `read_dossier(wine_id, sections=[])` — **headers only** (cheapest verification). Confirm `agent_sections_populated` includes written sections and `agent_sections_pending` no longer lists them. Do NOT re-read full content.
2. Report: sections written, sections skipped (with reason), source count

## Batch Research

1. `pending_research(limit=N)` for the priority queue
2. Group by vintage-region to reuse vintage data
3. One wine at a time; finish A before starting B
4. **Track source success/failure in session memory.** After each wine, note which URLs succeeded (✓) or failed (✗). Before fetching a producer domain for a subsequent wine, check session memory — skip domains that already failed. Reuse vintage data already fetched for the same region-year.
5. Summary table at end

## What You Must Never Do

- Write from training data alone
- Fabricate scores or tasting notes
- Write Low/None confidence sections
- Overwrite populated sections
- Write `market_availability`
- Edit dossier files directly
