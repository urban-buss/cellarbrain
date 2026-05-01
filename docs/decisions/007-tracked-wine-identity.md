# ADR-007: Tracked Wine Cross-Vintage Identity

## Status
Accepted

## Context
The same wine from different vintages (e.g. Château Margaux 2015, 2018, 2020) shares a producer profile, buying guide, and price tracking history. Per-vintage dossiers can't capture this cross-vintage knowledge effectively.

## Decision
Introduce `tracked_wine` as a cross-vintage identity entity. Wines are grouped by `(winery_id, wine_name)` when any vintage has `is_wishlist` or `is_favorite` set.

- IDs start at `TRACKED_WINE_ID_OFFSET = 90_001` to avoid collision with wine IDs
- Each tracked wine gets a **companion dossier** (`output/wines/tracked/{id}-{slug}.md`) with agent-owned research sections
- Tracked wines support soft-delete (tombstones preserved when all constituent wines disappear)
- `tracked_wine_id` is set as a FK on each constituent wine entity

Companion dossier sections: `producer_deep_dive`, `vintage_tracker`, `buying_guide`, `price_tracker`.

## Consequences
- Agents can write cross-vintage research once, shared across all vintages
- Price observations are linked to tracked_wine (not individual wines), enabling vintage-spanning price tracking
- A wine becoming a wishlist/favorite triggers tracked_wine creation on the next ETL run
- Removing wishlist/favorite status doesn't delete the tracked wine (soft-delete semantics)
