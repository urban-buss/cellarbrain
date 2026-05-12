# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.14] — 2026-05-12

### Fixed
- **Agent view selection**: `wines` and `bottles` views now clearly document that they include
  ALL non-deleted wines (stored, consumed, on order). Agent-facing guidance in `query_cellar`
  docstring and view descriptions updated to recommend `wines_stored`/`bottles_stored` as default
  for cellar questions.
- **Column hint accuracy**: `wines.price` hint corrected from "purchase price" to "list/catalogue
  price". Added `bottles.price` and `bottles_full.price` hints for actual purchase prices.
- **`find_wine` status markers**: search results now append `[on order]` or `[consumed]` to
  wine names for non-stored wines, so agents and users can immediately see storage status.
- **Flaky currency freshness test**: relaxed timing-sensitive assertion that could fail across
  day boundaries.

### Added
- New local-branch creation prompt (`new-branch.prompt.md`).

## [0.2.13] — 2026-05-11

### Added
- **Query Cache & Connection Pooling** (`QueryCache` class): Thread-safe LRU cache for expensive
  read-only MCP tool queries (`cellar_stats`, `find_wine`). Configurable via `[cache]` TOML
  section (`enabled`, `max_size`). ETL-triggered invalidation on Parquet file changes via mtime
  fingerprinting. New `cache_stats` MCP tool exposes hit/miss/eviction metrics.
- Observability schema extended with `cache_hit` (BOOLEAN) column for tracking cache effectiveness.
- **Typed MCP Tool Responses** (`ToolResponse` class): MCP tools can now return structured data
  alongside human-readable text. `ToolResponse` is a `str` subclass carrying optional `.data` and
  `.metadata` dicts. The wire wrapper converts these to `CallToolResult` with `structuredContent`
  for MCP clients that support it. Six tools migrated: `query_cellar`, `cellar_stats`, `find_wine`,
  `cellar_info`, `recommend_tonight`, `reload_data`. Remaining tools continue to work unchanged.
- Observability schema extended with `data_size` (INTEGER) and `metadata_keys` (VARCHAR) columns,
  with idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` upgrade for existing log stores.
- `query.execute_query_structured()` returns both markdown text and machine-readable row data.
- **Web Explorer dashboard** (`cellarbrain dashboard`): local Starlette web app with observability
  overview, tool usage, error log, session drill-down, latency charts, live event tail (SSE),
  cellar browser, bottle inventory, drinking window, tracked wines, SQL playground, cellar statistics,
  and interactive MCP tool workbench. Uses HTMX + Pico CSS + Chart.js.
- Soft-delete for wines: `is_deleted` column (bool, default `false`) added to `wine.parquet`.
  Wines that disappear from the CSV export are retained as tombstones (`is_deleted=true`) instead
  of being hard-deleted. Tombstones are excluded from all DuckDB views and FK validation, and
  are revived if the wine reappears in a future export.
- **Schema-version sidecar** (`.schema_version.json`): every `cellarbrain etl` run now writes a
  small JSON sidecar with the cellarbrain version and a fingerprint of the Parquet schemas.
  The query layer uses it as a fast-path check to detect stale data after an upgrade.

### Fixed
- Misleading DuckDB `BinderException` ("Referenced table 'c' not found") that appeared when a
  user upgraded cellarbrain without re-running ETL. The query layer now performs a pre-flight
  schema compatibility check and raises a clear `DataStaleError` naming the affected table and
  missing columns. The CLI also prints a `cellarbrain doctor` hint after any `DataStaleError`.

### Upgrade Notes
- **Re-run `cellarbrain etl` after upgrading from v0.2.9.** The cellar-classification refactor
  introduced in v0.2.10 added a `location_type` column to `cellar.parquet` (and removed
  `is_onsite` / `is_in_transit` from `bottle.parquet`). Existing Parquet files written by v0.2.9
  lack these columns and will trigger a `DataStaleError` until ETL is re-run.

### Fixed
- **DuckDB log-store lock contention** — the MCP server and ingest daemon now
  write to separate log-store files (`cellarbrain-mcp-logs.duckdb` and
  `cellarbrain-ingest-logs.duckdb`) by default, eliminating the DuckDB
  exclusive-lock conflict that silently disabled observability when both
  processes ran simultaneously. Read commands (`cellarbrain logs`, dashboard)
  auto-discover and merge all log files. Lock errors now log an actionable
  one-line warning with the conflicting PID instead of a full traceback.

## [0.1.0] — 2026-04-01

### Added
- ETL pipeline: CSV → Parquet with 12 normalised entity tables
- Incremental sync with change detection and stable IDs
- Per-wine Markdown dossier generation with agent research sections
- DuckDB-based SQL query layer (`cellarbrain query`)
- Cellar statistics with grouping (`cellarbrain stats --by country`)
- Dossier search and pending-research listing
- Parquet integrity validation (`cellarbrain validate`)
- MCP server with stdio and SSE transports (`cellarbrain mcp`)
