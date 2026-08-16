---
name: schema-change
description: Plan and implement safe Cellarbrain schema evolution across parsing, transforms, Parquet, DuckDB, queries, dossiers, MCP, and dashboard consumers. Use whenever a persisted or externally visible field, type, unit, identifier, null rule, or migration changes.
---

# Schema Change

1. Inventory the old schema and every producer and consumer.
2. Define the new field contract: name, type, unit, null/default behavior,
   normalization, provenance, uniqueness, ordering, and version semantics.
3. Choose additive evolution when possible. For replacement or removal, define
   backfill, compatibility reads, migration idempotence, and rollback behavior.
4. Update layers in dependency order: parser, transform/computed, writer,
   migration, validation, query/dossier, MCP, dashboard.
5. Test old data, new data, mixed versions, null and malformed values, repeated
   migration, and all externally visible response shapes using synthetic data.
6. Run `$etl-synthetic-smoke`, `$mcp-contract-check` when relevant, focused
   consumer tests, lint, and then the full suite.
