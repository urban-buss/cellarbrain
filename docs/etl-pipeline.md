# ETL Pipeline

End-to-end reference for the extract-transform-load pipeline: CSV ingestion through Parquet output, dossier generation, and validation.

## 1. CSV Ingestion

Three Vinocell exports are consumed:

| File | Content | Required |
|------|---------|----------|
| `export-wines.csv` | Wine metadata (one row per wine) | Yes |
| `export-bottles-stored.csv` | Bottles currently in cellar | Yes |
| `export-bottles-gone.csv` | Consumed/removed bottles | Yes |

Format: **UTF-16 LE**, tab-delimited. The `vinocell_reader` module handles encoding and the duplicate "Pro Ratings" header (column 52 renamed to "Pro Ratings Detail"). Empty cells are mapped to `None`. After validation, all CSV headers are remapped to canonical column names via `VINOCELL_COLUMN_MAP` (e.g. `"Winery"` → `"winery"`, `"Input date"` → `"purchase_date_raw"`), so `transform` and downstream modules use source-agnostic keys.

## 2. Field Parsing

`parsers` converts raw CSV strings to generic typed values; `vinocell_parsers` handles Vinocell-specific formats (category labels, rating lines, acquisition types, wine-name placeholders):

| Parser | Example Input | Output |
|--------|--------------|--------|
| `parse_grapes` | `"Merlot (80%), Cab Franc (20%)"` | `[("Merlot", 80.0), ("Cabernet Franc", 20.0)]` |
| `parse_alcohol` | `"14.5 %"` | `14.5` |
| `parse_volume` | `"3.0 L - Jéroboam"` | `3000` |
| `parse_eu_date` | `"16.08.2024"` | `date(2024, 8, 16)` |
| `parse_vintage` | `"Non vintage"` | `(None, True)` |
| `parse_category` | `"Red wine"` | `"red"` |
| `parse_tasting_line` | `"21 February 2024 - Notes - 16.5/20"` | `{date, note, score, max_score}` |
| `parse_pro_rating_wine` | `"Parker - 95.0/100 - Excellent"` | `{source, score, max_score, review_text}` |

Convention: `None` input → `None` output for optional fields; `ValueError` for required.

## 3. Entity Build Order

`transform` builds entities in dependency order — lookups first so FK values are available for core entities:

**Lookups:**
1. `winery` — deduplicate winery names from wines CSV
2. `appellation` — deduplicate (country, region, subregion, classification) combos
3. `grape` — parse all Grapes fields, deduplicate variety names
4. `cellar` — deduplicate cellar names from bottles CSV, extract sort_order
5. `provider` — deduplicate provider names from stored + gone bottles

**Core:**
6. `wine` — build from wines CSV, resolve winery_id and appellation_id via lookups. When existing `wine.parquet` is available, `classify_wines()` pre-matches each CSV row by **slug** (accent-folded `Winery + Name + Year`) against existing wines, assigning stable `wine_id` values before transform. Duplicate slugs are disambiguated via a **fingerprint cascade** (volume → classification → grapes → category → price). New wines get IDs from `max_existing_id + 1`. See [ADR-009](decisions/009-slug-based-wine-id-stabilisation.md).
7. `wine_grape` — junction table from parsed grape blends
8. `bottle` — stored bottles + gone bottles (separate builders, merged with offset IDs)
9. `tasting` — parse from wines CSV field W51 (multi-line)
10. `pro_rating` — parse from wines W53 + bottles B49, deduplicated by (wine_id, source, score)

**Derived:**
11. `tracked_wine` — group by (winery_id, wine_name) where any wine is wishlist/favorite

## 4. Wine Enrichment (3-Pass)

`computed.enrich_wines()` adds computed fields to wine entities **in-place**:

### Pass 1 — Grape Properties

For each wine, builds a sorted grape list from `wine_grape` rows:

- `grape_type`: `"varietal"` (1 grape), `"blend"` (2+), `"unknown"` (0)
- `primary_grape`: dominant grape (single varietal, or >50% in blend, or first-mentioned if no percentages)
- `grape_summary`: display string — `"Nebbiolo"`, `"Merlot / Cabernet Franc"`, `"Syrah blend"`, `"CS / Merlot / …"`

### Pass 2 — Full Name

Detects **grape-ambiguous** names — same (winery, name) pair with different primary grapes — then computes `full_name` via decision tree:

1. Winery always included (or "Unknown Wine")
2. If cuvée name exists → include it. If grape-ambiguous → append grape_summary.
3. If no cuvée name → disambiguate with subregion, classification_short, or primary grape.
4. Append vintage (or "NV").
5. Truncate at `max_full_name_length` (default 80) on word boundary with `…`.

### Pass 3 — Status & Pricing

Requires `current_year` and `settings`:

- `drinking_status`: 6-state enum (see [computed-properties.md](computed-properties.md))
- `age_years`: `current_year - vintage` (None for NV)
- `list_price`: `original_list_price` converted to default currency (CHF)
- `list_currency`: always the default currency when price exists
- `price_tier`: `"budget"` / `"everyday"` / `"premium"` / `"fine"` / `"unknown"` based on configurable thresholds

## 5. Currency Normalisation

Happens at ETL time, not query time. Both wine and bottle entities carry dual columns:

| Original Column | Converted Column |
|----------------|------------------|
| `original_list_price` / `original_list_currency` | `list_price` / `list_currency` |
| `original_purchase_price` / `original_purchase_currency` | `purchase_price` / `purchase_currency` |

Conversion uses `convert_to_default_currency()`:
- Same currency → pass through
- Different → multiply by fixed rate from `settings.currency.rates`
- No rate configured → `ValueError`
- Quantised to 2 decimal places

Default currency: CHF. Rates configured in `cellarbrain.toml` under `[currency.rates]`.

## 6. Bottle-Level Enrichment

After currency conversion, each bottle gets:

- `purchase_price` / `purchase_currency` — converted from original
- `is_onsite` — `True` unless the bottle's cellar is in `settings.offsite_cellars` or `settings.in_transit_cellars`
- `is_in_transit` — `True` if the bottle's cellar is in `settings.in_transit_cellars`

In-transit bottles represent orders not yet in the physical cellar (`status = 'stored'` but `is_in_transit = true`). They are excluded from cellar inventory counts, cellar value, volume calculations, and drinking recommendations.

## 7. Tracked Wine Construction

`transform.build_tracked_wines()` creates cross-vintage identities:

- Groups wines by `(winery_id, wine_name)` where any wine has `is_wishlist` or `is_favorite` set
- Assigns IDs starting at **`TRACKED_WINE_ID_OFFSET = 90_001`**
- `assign_tracked_wine_ids()` then sets `tracked_wine_id` on each wine entity
- `assign_tracked_dossier_paths()` sets companion dossier paths under `tracked/`

## 8. Incremental Sync

`incremental.sync()` stabilises IDs and detects changes when re-processing CSVs.

### Wine ID Stabilisation — Slug-Based Pre-Matching

Wine uses a dedicated **slug-based pre-matching** system (`classify_wines()`) that runs *before* the transform step, in both full-load and sync modes. Wine is **skipped** by `sync()` and `annotate_full_load()` — its stabilisation and change detection are handled separately via `annotate_classified_wines()`.

The algorithm:

1. **Slug generation** — For each CSV row, compute `wine_slug(Winery, Name, Year)`: NFKD accent-fold → ASCII → lowercase → hyphens → truncate to 60 chars. Same algorithm as dossier filename slugs.
2. **Slug index** — Load existing `wine.parquet`, build `slug → [(wine_id, is_deleted), ...]` mapping.
3. **Classification** — Match each CSV slug against the index:
   - Slug found, active entry → **EXISTING** (reuse `wine_id`)
   - Slug found, only soft-deleted entries → **REVIVED** (reuse old `wine_id`)
   - Slug not found → tentatively **NEW** (assign `max_existing_id + 1`)
4. **Fingerprint disambiguation** — When multiple wines share the same slug (different purchase lots), a fingerprint cascade selects the correct match: exact 5-field match first, then single-field cascade (volume → classification → grapes → category → price), then positional as last resort.
5. **Rename detection** — Post-pass on unmatched NEW + DELETED pairs: if a pair shares the same vintage and one of (winery, name) matches while the other changed, it is reclassified as **RENAMED** — the old `wine_id` is reused and the slug is updated.
6. **Deletions** — Existing wines not consumed by any CSV row are **DELETED** (soft-delete). Already-deleted tombstones are preserved.

### Non-Wine ID Stabilisation — 4 Levels

For all entities except wine, `_stabilize_entity()` matches new rows to existing by natural key:

1. **Exact natural key match** — same NK → reuse existing ID (positional within duplicate NK groups)
2. **Positional within group** — handles duplicate NKs by matching by position
3. **Partial-NK fallback** (wine only) — when full NK doesn't match, try `(winery_id, vintage, is_non_vintage)`. If exactly 1 disappeared + 1 appeared → treat as rename.
4. **Fuzzy name matching** (wine only, configurable) — for N-to-N partial-key matches, use `SequenceMatcher.ratio()` with threshold `identity.rename_threshold` (default 0.85)

### Winery Rename Detection

`_detect_winery_renames()` uses a structural heuristic:
- Exactly 1 old winery disappeared, 1 new winery appeared
- Both have the same number of wines (> 0)
- Treat as rename: remap the new winery ID to the old stable ID

### Change Detection

`_diff_rows()` classifies each row:

| Type | Condition | Metadata |
|------|-----------|----------|
| **insert** | New NK not in old data | `etl_run_id` = current run, `updated_at` = now |
| **update** | Same NK, different field values | `etl_run_id` = current run, `updated_at` = now, `changed_fields` logged |
| **delete** | Old NK not consumed by new data | Tombstone for soft-delete entities; hard-delete for others |
| **rename** | Same entity_id as both insert + delete | Merged into single rename entry with changed_fields |

Float comparison uses `struct.pack("f", ...)` round-trip to match Parquet float32 storage precision.

### Soft-Delete Tombstones

For `wine` and `tracked_wine`:
- Deleted rows are retained with `is_deleted = true`
- `etl_run_id` and `updated_at` updated to current run
- Tombstones excluded from DuckDB views and FK validation
- Revival: if NK reappears in future export, `is_deleted` set back to `false`
- Tombstones carried forward across both sync and full-load runs

## 9. Full Load

`incremental.annotate_full_load()`:
- Sets `etl_run_id` and `updated_at` on every row
- Generates insert change-log entries for all rows
- Carries forward existing tombstones (soft-deleted rows from previous runs)
- **Skips the wine entity** — wine stabilisation and change detection are handled by `classify_wines()` + `annotate_classified_wines()`, which run in both full-load and sync modes

## 10. Dossier Path Assignment

After ID stabilisation:

- `assign_dossier_paths()` sets `dossier_path` on each wine:
  - Wines with ≥1 stored bottle → `cellar/{wine_id:04d}-{slug}.md`
  - All others → `archive/{wine_id:04d}-{slug}.md`
- `assign_tracked_dossier_paths()` sets paths on tracked wines: `tracked/{tracked_wine_id:05d}-{slug}.md`
- Slug: accent-folded, lowercase, hyphen-separated, max 60 chars

## 11. ETL Tracking

Two append-only tables track pipeline history:

**`etl_run`**: one row per run with timestamps, run_type, CSV hashes, and change counts. Includes both total (all entities, field-level) counts (`total_inserts`, `total_updates`, `total_deletes`) and wine-level counts (`wines_inserted`, `wines_updated`, `wines_deleted`, `wines_renamed`) that track how many wine entities were affected.

**`change_log`**: one row per entity change with `change_type`, `entity_id`, and `changed_fields` (JSON array of field names).

Both are written via `writer.append_parquet()` after entity Parquet files.

## 12. Dossier Generation

After Parquet writes:

- **Sync mode**: only wine IDs affected by changes are regenerated (traced via FK relationships in `markdown.affected_wine_ids()`). Deleted wines get a banner and `deleted: true` in frontmatter.
- **Full mode**: all wines regenerated.
- **Companion dossiers**: generated for all active tracked wines. Agent-owned sections preserved.

## 13. Validation

`validate.validate()` runs DuckDB checks on the output Parquet files:

- **FK integrity** — 12 checks (every FK column resolves to parent)
- **PK uniqueness** — 10 checks (every PK and natural key is unique)
- **Domain constraints** — category values, acquisition_type values, score ≤ max_score, drinking window ordering, etc.
- **Consistency** — stored bottles have no output_date; gone bottles have output_date; no stored bottles for deleted wines
- **Row count sanity** — wine and bottle tables have > 0 rows
- **Price observation integrity** — FK to tracked_wine, price ≥ 0, bottle_size_ml > 0
