# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.13] — 2026-05-11

### Added
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
