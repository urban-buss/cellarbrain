# Architecture

## Pipeline Overview

```
                          settings (cellarbrain.toml)
                                 ↓
raw/*.csv → vinocell_reader → parsers / vinocell_parsers → classify_wines (slug pre-match against wine.parquet)
                                                ↓
                               transform → computed (enrich_wines, 3-pass)
                                                ↓
                            incremental (stabilise IDs, diff, annotate)
                                                ↓
                         writer (Parquet) → markdown (per-wine dossiers)
                                    ↓              → companion_markdown (tracked-wine dossiers)
                           query (DuckDB) ← mcp_server (FastMCP stdio)
                                    ↓
                              validate (DuckDB integrity checks)
```

## Module Responsibilities

| Module | Role |
|--------|------|
| `settings` | Frozen dataclasses + TOML loader. `load_settings()` merges built-in defaults → `cellarbrain.toml` → env vars → CLI flags. Includes currency, identity, wishlist, dossier config. |
| `vinocell_reader` | Read UTF-16 LE tab-delimited CSV exports. Handles duplicate "Pro Ratings" header. Empty→None conversion. Remaps Vinocell CSV headers to canonical column names via `VINOCELL_COLUMN_MAP`. |
| `slugify` | URL-safe slug generation for wine identifiers. `make_slug()` (winery + name + vintage) and `companion_slug()` (winery + wine name, no vintage). Used by `transform`, `markdown`, and `companion_markdown`. |
| `parsers` | Generic field-level parsing: grapes, dates, volumes, decimals, slugs, booleans. None in → None out for optional; ValueError for required. |
| `vinocell_parsers` | Vinocell-specific field parsers: category map, vintage, rating line formats, acquisition/output type enums, wine-name cleaning, cellar sort order, opening time. |
| `transform` | Build normalised entities from raw CSV rows. Lookup entities (winery, appellation, grape, cellar, provider), core entities (wine, wine_grape, bottle, tasting, pro_rating), derived (tracked_wine). Provides `wine_slug()` and `wine_fingerprint()` for slug-based wine ID stabilisation. `build_wines()` accepts optional pre-assigned IDs from `classify_wines()`. |
| `computed` | Pure functions for derived properties. 3-pass `enrich_wines()`: grape props → full_name → drinking_status/price_tier. Currency conversion. |
| `writer` | Write Parquet with explicit PyArrow schemas (`SCHEMAS` dict, 14 entities). Read-back, append, and year-partitioned write helpers. |
| `incremental` | ID stabilisation via natural keys (4-level matching) for non-wine entities. Slug-based pre-matching for wine (`classify_wines()`, `annotate_classified_wines()`). Change detection (insert/update/delete/rename). Soft-delete tombstones. ETL run tracking. |
| `markdown` | Per-wine Markdown dossiers. ETL-owned sections regenerated; agent-owned sections preserved via HTML comment fences. Batch generation, cellar↔archive moves, deleted marking. |
| `companion_markdown` | Companion dossiers for tracked wines (cross-vintage). Fully agent-owned after ETL scaffolding. |
| `dossier_ops` | Agent CRUD for dossier sections. Path traversal protection. Frontmatter pending/populated tracking. Section filtering. |
| `flat` | SQL templates for denormalised DuckDB views: `wines`, `bottles`, `tracked_wines`, `wines_wishlist`, 3 price views. |
| `query` | DuckDB layer over Parquet. Dual-connection model. SQL validation (read-only). Search, statistics, price tracking, wishlist alerts. |
| `validate` | DuckDB-based checks: FK integrity, PK uniqueness, domain constraints, row-count sanity. |
| `mcp_server` | 15 thin data tools + 8 resources + prompts via FastMCP. No LLM reasoning in server code. |
| `cli` | Subcommand router: `etl`, `validate`, `query`, `stats`, `dossier`, `mcp`, `recalc`, `wishlist`, `logs`. Legacy compat for flat args. |
| `log` | Configure stdlib logging: stderr handler + optional `RotatingFileHandler`. `JsonFormatter` for structured JSON log lines. Suppress noisy third-party loggers. |
| `observability` | Structured event capture for MCP tool/resource/prompt invocations. `ToolEvent` dataclass, `EventCollector` with session/turn tracking, buffered DuckDB log store, auto-pruning. |
| `dashboard` | Starlette web app for browsing observability data, cellar contents, SQL playground, and MCP tool workbench. Sub-modules: `app` (routes), `queries` (obs query functions), `cellar_queries` (cellar view queries), `workbench` (tool introspection + execution), `dossier_render` (Markdown→HTML). Templates use HTMX + Pico CSS + Chart.js. |
| `email_poll` | IMAP polling daemon for automated Vinocell CSV ingestion. Sub-modules: `grouping` (batch detection, pure functions), `placement` (snapshot + flush), `imap` (IMAP client wrapper), `credentials` (keyring + env var resolution), `etl_runner` (subprocess ETL invocation). Optional dependency: `imapclient`, `keyring`. |

## Data Flow: A Single ETL Run

1. **Read CSVs** — `vinocell_reader` ingests 3 UTF-16 LE exports (wines, bottles-stored, bottles-gone). Duplicate "Pro Ratings" header renamed. All CSV headers are remapped to canonical column names (e.g. `"Winery"` → `"winery"`, `"Input date"` → `"purchase_date_raw"`) so downstream modules never reference source-specific headers.

2. **Parse fields** — `parsers` converts raw strings: alcohol `"14.5 %"` → `14.5`, grapes `"Merlot (80%), Cab Franc (20%)"` → list of tuples, dates `"16.08.2024"` → `date` objects. `vinocell_parsers` handles Vinocell-specific formats: category labels, rating lines, acquisition types, wine-name placeholders.

3. **Build entities** — `transform` creates lookup entities first (winery, appellation, grape, cellar, provider), then core (wine, wine_grape, bottle, tasting, pro_rating), then tracked_wine. **Wine IDs are pre-assigned** via `classify_wines()` slug-based matching against existing `wine.parquet` (runs before transform, in both full-load and sync modes). Duplicate slugs are disambiguated by a fingerprint cascade. Renamed wines (winery or name changed in Vinocell) are detected by pairing unmatched NEW + DELETED pairs.

4. **Enrich** — `computed.enrich_wines()` runs 3 passes over wine entities in-place:
   - Pass 1: compute `grape_type`, `primary_grape`, `grape_summary` per wine.
   - Pass 2: detect grape-ambiguous names, compute `full_name`.
   - Pass 3: `drinking_status`, `age_years`, currency conversion, `price_tier`.
   - Bottle-level: `purchase_price` currency conversion, `is_onsite`, `is_in_transit`.

5. **Stabilise IDs** (sync mode) — `incremental.sync()` matches each new entity row (except wine) to existing Parquet data by natural key, preserving stable IDs across re-exports. Detects inserts, updates, deletes, and renames. Wine stabilisation is handled separately by `classify_wines()` + `annotate_classified_wines()` which run in both modes. Produces `change_log` rows.

6. **Write Parquet** — `writer.write_all()` writes each entity with explicit PyArrow schemas. `etl_run` and `change_log` are appended.

7. **Generate dossiers** — `markdown.generate_dossiers()` creates/regenerates per-wine `.md` files. Agent-owned sections are preserved. In sync mode, only affected wines are regenerated. `companion_markdown` generates tracked-wine dossiers.

8. **Validate** — `validate.validate()` runs FK integrity, PK uniqueness, and constraint checks via DuckDB.

## Dual-Connection Model

The query layer provides two DuckDB connection types:

| Connection | Function | Exposed |
|------------|----------|---------|
| **Agent** | `get_agent_connection(data_dir)` | 6+ views: `wines`, `bottles`, `tracked_wines`, `wines_wishlist`, price views, plus convenience views (`wines_stored`, `bottles_stored`, `bottles_consumed`, `wines_drinking_now`). No raw entity tables. |
| **Internal** | `get_connection(data_dir)` | Everything above + `etl_run` and `change_log` views. Used by `cellar_stats` and MCP resources. |

Agent-submitted SQL is validated by `validate_sql()` which rejects DDL/DML, multi-statement, and non-SELECT/WITH queries. `find_wine()` uses parameterised queries to prevent SQL injection.

## MCP Transport

The MCP server uses **stdio transport** (FastMCP). Configuration:

- `CELLARBRAIN_DATA_DIR` env var → Parquet data directory (default: `output`).
- `CELLARBRAIN_CONFIG` env var → path to `cellarbrain.toml`.
- CLI: `cellarbrain mcp` sets env vars from settings, then starts the server.

All 15 tools are thin data accessors — no LLM reasoning in server code. Agents provide the reasoning layer.

## Food Pairing — RAG Retrieval

The `pairing` module (`src/cellarbrain/pairing.py`) provides SQL-based retrieval-augmented generation for food→wine pairing. It requires **no ML model** — strategies use existing columns (`category`, `primary_grape`, `food_tags`, `food_groups`, `country`, `region`) to score and rank cellar candidates.

Five strategies run in parallel: category matching, grape affinity, food_tag keyword search, food_group membership, and regional affinity. Results are merged by `wine_id` and ranked by signal count (number of strategies that matched). See `docs/food-pairing.md` for full details.

The `pairing_candidates` MCP tool wraps `retrieve_candidates()` and is the **primary retrieval tool** for the food-pairing skill. The dashboard exposes the same functionality at `/pairing`.

## Sommelier Module

The sommelier module (`src/cellarbrain/sommelier/`) provides AI-powered food-wine pairing via embedding-based semantic similarity.

### Training Pipeline

1. `cellarbrain train-model` loads the pairing dataset (9,000 food-wine pairs with scores).
2. Fine-tunes `all-MiniLM-L6-v2` (22M params, 384-dim) with `CosineSimilarityLoss`.
3. Stratified train/eval split by score quintile. `EmbeddingSimilarityEvaluator` at each epoch.
4. Saves model to `models/sommelier/model/` (~80 MB). Model is **not committed to Git** — train locally.

### Index Build

1. `cellarbrain rebuild-indexes` loads the trained model.
2. Encodes all food catalogue entries → FAISS `IndexFlatIP` (L2-normalised = cosine similarity).
3. Encodes all wines with `bottles_stored > 0` → separate wine FAISS index in `<data_dir>/sommelier/`.
4. The wine index is **auto-rebuilt** after each ETL run when `sommelier.enabled = true`.

### Query Flow

- **food → wine** (`suggest_wines`): Encode food description → search wine index → return ranked `ScoredWine` list. MCP tool enriches with DuckDB metadata (vintage, category, region, grape, bottles, price).
- **wine → food** (`suggest_foods`): Build wine text from DuckDB metadata → encode → search food index → return ranked `ScoredFood` list. MCP tool enriches with food catalogue metadata (cuisine, weight class, protein, flavour profile).

### Column Mapping

Sommelier SQL queries use the `wines_full` view (not the slim `wines` view) to access `grapes` (aliased from `grape_summary`). The `build_wine_text()` function uses parameter names `full_name` and `grape_summary` — call sites map from view aliases `wine_name` and `grapes`.

### Lazy Loading

All heavy dependencies (`sentence-transformers`, `faiss-cpu`, `torch`) are lazy-imported. The model and indexes are loaded on first query, not at server startup. The `[sommelier]` extra is optional — the base install works without ML deps.

### Sub-modules

| Module | Role |
|--------|------|
| `model` | Lazy model loader. `load_model()` / `ModelNotTrainedError`. |
| `training` | Fine-tuning pipeline. `train_model()` → metrics dict. |
| `index` | FAISS build/load/search. `build_index()`, `load_index()`, `search_index()`. |
| `engine` | Orchestrator. `SommelierEngine` with `suggest_wines()` / `suggest_foods()`. |
| `text_builder` | Text serialisation. `build_food_text()` / `build_wine_text()`. |
| `food_catalogue` | Food catalogue schema (Parquet schema + dataclass). |

