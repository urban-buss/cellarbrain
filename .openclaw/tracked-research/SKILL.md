---
name: tracked-research
description: "Companion dossier research for tracked wines. Producer deep dives, vintage tracking, and buying guides via cellarbrain MCP."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Tracked Wine Research

Defensive, fact-only research for companion dossiers — cross-vintage research documents for tracked wines (favourites and wishlist items).

**Scope:** `producer_deep_dive`, `vintage_tracker`, `buying_guide`. Does NOT write `price_tracker` — use the `market-research` skill. Does NOT write per-vintage sections — use the `wine-research` skill.

## Cardinal Rules

1. **Facts only.** Every claim must trace to a web source visited in this session.
2. **Identify before you write.** Confirm winery + wine name. If uncertain, STOP.
3. **Skip over guess.** No data? Leave pending.
4. **MCP only.** Read via `read_companion_dossier`, write via `update_companion_dossier`.
5. **One section at a time.**
6. **Never touch `price_tracker`.** That belongs to the `market-research` skill.

## Owner Context

- Switzerland, CHF primary
- Swiss retailers: Gerstl, Martel, Flaschenpost, Mövenpick
- Notes may be in German

## Confidence Gate

| Level | Criteria | Action |
|---|---|---|
| **High** | ≥2 sources agree | Write |
| **Medium** | 1 reliable source | Write, note single-source |
| **Low** | Only retailer data or conflicts | **SKIP** |
| **None** | No data | **SKIP** |

## Workflow

### Phase 1 — Load & Understand

1. `read_companion_dossier(tracked_wine_id, sections=[])` — minimal load
2. Extract: `winery_name`, `wine_name`, `category`, `country`, `region`, `vintages_tracked`
3. Check `agent_sections_pending` — only research pending sections

### Phase 2 — Web Research

| # | Query | Purpose |
|---|---|---|
| 1 | `"{winery}" profile history {region}` | Full producer background |
| 2 | `"{winery}" "{wine}" vintage ratings` | Multi-vintage scores |
| 3 | `"{winery}" "{wine}" buy switzerland OR retailer` | Swiss availability |
| 4 | `"{winery}" "{wine}" best vintage recommendations` | Buying guidance |

### Phase 3 — Identity Verification

Verify: winery name, wine name (cross-vintage label), category. Any mismatch → STOP.

### Phase 4 — Write Sections

| Section | Length | Content |
|---|---|---|
| `producer_deep_dive` | 300–500 words | Full history, team, vineyard holdings, all wine ranges, terroir, certification |
| `vintage_tracker` | Table | Row per vintage from `vintages_tracked`: rating range, harvest notes, drinking window |
| `buying_guide` | 200–400 words | Recommended vintages, typical pricing, Swiss retailers, auction potential. NO real-time pricing. |

Every section ends with `Sources: [list URLs visited]`.

### Phase 5 — Verify & Report

1. `read_companion_dossier(tracked_wine_id)` to confirm writes
2. Report: sections written/skipped, source count

## Batch Research

1. `pending_companion_research(limit=N)` for priority queue
2. One wine at a time
3. Summary table: wine, deep_dive, vintage, buying, notes

## What You Must Never Do

- Write from training data alone
- Fabricate scores
- Write Low/None confidence sections
- Overwrite populated sections
- Write `price_tracker`
- Write per-vintage dossier sections
- Edit dossier files directly
