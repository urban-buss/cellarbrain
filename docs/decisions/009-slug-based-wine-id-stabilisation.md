# ADR-009: Slug-Based Wine ID Stabilisation

## Status
Accepted

## Context
[ADR-001](001-natural-key-id-stabilisation.md) introduced natural-key ID stabilisation for all entities. For wine, the natural key is `(winery_id, name, vintage, is_non_vintage)`. This works in sync mode but has structural weaknesses:

- `winery_id` is a synthetic ID assigned during transform — if winery sort order changes, `winery_id` shifts, breaking every wine's natural key for that winery.
- The two-phase approach (assign sequential IDs → remap via natural key) adds complexity.
- Full-load mode skips the remap phase entirely, making wine IDs unstable.

These weaknesses mean that a full ETL re-run can reshuffle wine IDs, breaking dossier filenames, external references (ratings files), and tracked-wine linkages.

## Decision
Replace natural-key ID stabilisation for wine with **slug-based pre-matching** that runs *before* the transform step.

### Slug definition
A deterministic, accent-folded ASCII string derived from raw CSV fields: `wine_slug(Winery, Name, Year)`. Same algorithm as dossier filename slugs (`markdown._make_slug()`). Stored in the new `wine_slug` column on `wine.parquet`.

### Classification algorithm
`classify_wines()` in `incremental.py` matches each CSV row against the slug index from existing `wine.parquet`:

| Condition | Classification | Action |
|---|---|---|
| Slug matches active entry | **EXISTING** | Reuse `wine_id` |
| Slug matches only soft-deleted entries | **REVIVED** | Reuse old `wine_id`, undelete |
| NEW + DELETED pair shares vintage + one of (winery, name) | **RENAMED** | Reuse old `wine_id`, update slug |
| Slug not in index | **NEW** | Assign `max_existing_id + 1` |
| Existing wine not consumed by any CSV row | **DELETED** | Soft-delete tombstone |

### Fingerprint disambiguation
When multiple wines share the same slug (different purchase lots of the same wine), a 5-field fingerprint cascade disambiguates: volume → classification → grapes → category → price. Resolves 96% of duplicate groups deterministically; the remaining identical-twin group falls back to positional matching.

### Schema changes
Four new columns added to `wine.parquet`:

| Column | Type | Purpose |
|---|---|---|
| `wine_slug` | string (not null) | Identity slug for matching |
| `_raw_volume` | string (nullable) | Raw CSV Volume for fingerprint |
| `_raw_classification` | string (nullable) | Raw CSV Classification for fingerprint |
| `_raw_grapes` | string (nullable) | Full CSV Grapes string for fingerprint |

### Pipeline integration
- `classify_wines()` runs before `build_wines()` in both full-load and sync modes.
- `build_wines()` accepts optional `id_assignments` parameter with pre-assigned wine IDs.
- `sync()` and `annotate_full_load()` skip the wine entity — wine change detection is handled by `annotate_classified_wines()`.
- FK remappings from non-wine entity stabilisation are applied to wine rows downstream.

## Consequences
- Wine IDs are stable across re-exports regardless of `--sync` / `--full` mode.
- Dossier filenames (`{wine_id:04d}-{slug}.md`) are stable.
- External references (ratings files, tracked wines) survive re-runs.
- Rename detection preserves wine_id when winery name or wine name changes (6 renames across 3 test exports, zero false positives).
- ADR-001's 4-level NK stabilisation remains in effect for all non-wine entities.
- The `--sync` flag continues to exist but only affects non-wine entity stabilisation.
- Future work: apply slug-based stabilisation to tracked wines; expose `wine_slug` in DuckDB views.
