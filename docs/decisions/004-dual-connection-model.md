# ADR-004: DuckDB Dual Connection Model

## Status
Accepted

## Context
The query layer serves two audiences: internal ETL (which needs `etl_run` and `change_log` tables for metadata) and external agents (which should only see pre-joined, filtered views to prevent accidental data access or SQL errors on raw tables).

## Decision
Two connection factories:

- `get_connection(data_dir)` — registers all views including `etl_run` and `change_log`. Used by CLI commands and internal functions.
- `get_agent_connection(data_dir)` — registers only the 6 agent-facing views (`wines`, `bottles`, `wines_stored`, `bottles_stored`, `bottles_consumed`, `wines_drinking_now`, plus optional price views). Used by MCP tools.

Both connections are read-only (backed by `read_parquet()` views). SQL validation (`validate_sql()`) additionally blocks DDL/DML keywords.

## Consequences
- Agents cannot query raw entity tables or ETL metadata
- All data is accessible through denormalised views (no JOINs needed by agents)
- Adding a new agent view requires updating both `flat.py` and `query.get_agent_connection()`
