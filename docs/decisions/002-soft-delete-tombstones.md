# ADR-002: Soft-Delete Tombstones

## Status
Accepted

## Context
When a wine disappears from the CSV export, hard-deleting it would discard agent-written dossier research, FK references from bottles/tastings, and the stable ID. But keeping stale data in active views would pollute query results.

## Decision
`wine` and `tracked_wine` entities use soft deletes. When a row's natural key disappears from the export, it is retained with `is_deleted = true`, `etl_run_id` and `updated_at` set to the current run.

Soft-deleted rows are:
- Excluded from all DuckDB views via `WHERE NOT is_deleted`
- Excluded from FK validation
- Preserved in Parquet (both sync and full-load runs carry forward tombstones)
- Revivable: if the NK reappears in a future export, `is_deleted` is set back to `false`

Other entities (bottle, tasting, pro_rating, etc.) use hard deletes because they have no agent-owned content.

## Consequences
- Dossier files for deleted wines get a banner and `deleted: true` in frontmatter but are not removed
- Agent research is preserved indefinitely
- Parquet files grow monotonically; no mechanism for purging tombstones yet
