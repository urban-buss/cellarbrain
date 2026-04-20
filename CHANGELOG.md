# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Soft-delete for wines: `is_deleted` column (bool, default `false`) added to `wine.parquet`.
  Wines that disappear from the CSV export are retained as tombstones (`is_deleted=true`) instead
  of being hard-deleted. Tombstones are excluded from all DuckDB views and FK validation, and
  are revived if the wine reappears in a future export.

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
