# ADR-001: Natural-Key ID Stabilisation

## Status
Accepted

## Context
Wine cellar CSV exports don't provide stable IDs. Each export is a fresh dump where row order can change. We need stable IDs across ETL runs so that dossier paths, FK references, and agent-written content survive re-exports.

## Decision
Each entity defines a **natural key** (unique business fields). On sync, new rows are matched to existing by natural key and assigned the same stable ID. When no match is found, a new ID is minted from the next available sequence value.

Four stabilisation levels handle increasingly ambiguous cases:
1. Exact NK match → reuse ID
2. Positional match within duplicate NK groups
3. Partial-NK fallback for wines (winery_id, vintage, is_non_vintage)
4. Fuzzy name matching via `SequenceMatcher` (threshold configurable)

## Consequences
- IDs are stable across re-exports, enabling dossier persistence
- Winery renames are detected via structural heuristic (1-disappear, 1-appear, same wine count)
- Float comparison uses `struct.pack("f", ...)` round-trip to match Parquet float32 precision
- Fuzzy matching can be disabled for deterministic builds
