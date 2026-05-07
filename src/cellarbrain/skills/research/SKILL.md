---
name: research
description: "Research a wine online and populate its dossier with verified facts. Covers producer, vintage, ratings, market pricing, and tracked-wine companions."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Wine Research

Fact-only web research to populate dossier sections. Every claim must trace to a web source visited in this session.

## Cardinal Rules

1. **Facts only.** Every claim must come from a page you visited. Never from training data alone.
2. **Identify first.** Confirm winery + vintage + region before writing. If uncertain → STOP.
3. **Skip over guess.** No data? Leave section pending. Nothing > something wrong.
4. **One section at a time.** Separate `update_dossier` / `update_companion_dossier` call per section.
5. **Never overwrite** populated sections unless explicitly asked.

## Owner Context

- Switzerland, CHF. Swiss retailers: Gerstl, Martel, Flaschenpost, Mövenpick.

## Confidence Gate

| Level | Criteria | Action |
|---|---|---|
| High | ≥2 sources agree | Write |
| Medium | 1 reliable source | Write, note single-source |
| Low/None | Conflicts or no data | **SKIP** |

## Branch A: Per-Vintage Dossier

**Sections:** `producer_profile`, `vintage_report`, `wine_description`, `market_availability`, `ratings_reviews`, `tasting_notes`, `food_pairings`, `similar_wines`

### Workflow

1. `read_dossier(wine_id, sections=[])` — get identity + pending list
2. Build search: `"{winery}" "{vintage}" {region}`
3. Web research: Wine-Searcher → Millesima → Mövenpick → producer site → Vivino (last resort)
4. Verify identity (winery + vintage + region + category all match)
5. For each pending section with High/Medium confidence: `update_dossier(wine_id, section, content)`
6. Every section ends with `Sources: [URLs]`
7. `read_dossier(wine_id, sections=[])` — confirm sections populated

**Batch:** `pending_research(limit=N)` → process one wine at a time.

## Branch B: Companion Dossier (Tracked Wines)

**Sections:** `producer_deep_dive`, `vintage_tracker`, `buying_guide`, `price_tracker`

### Workflow

1. `read_companion_dossier(tracked_wine_id, sections=[])` — get identity + pending
2. Build search: `"{winery}" "{wine}" {region}`
3. Web research (same source order as Branch A)
4. Verify identity (winery + wine name + category)
5. For each pending section: `update_companion_dossier(tracked_wine_id, section, content)`
6. Every section ends with `Sources: [URLs]`

**Batch:** `pending_companion_research(limit=N)` → process one wine at a time.

## Tools

| Tool | Purpose |
|------|---------|
| `read_dossier` | Load per-vintage dossier |
| `update_dossier` | Write agent sections (per-vintage) |
| `read_companion_dossier` | Load companion dossier (tracked wine) |
| `update_companion_dossier` | Write companion sections |
| `pending_research` | Priority queue of wines needing research |
| `pending_companion_research` | Priority queue of tracked wines needing research |
| `find_wine` | Resolve wine identity from text |
