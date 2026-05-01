# ADR-005: Year-Partitioned Price Observations

## Status
Accepted

## Context
Price observations are append-only data that grows continuously as agents scan retailers. Storing all observations in a single Parquet file means rewriting the entire file on each new observation.

## Decision
Price observations are stored in year-partitioned Parquet files: `price_observation_2025.parquet`, `price_observation_2026.parquet`, etc.

- **Write**: `write_partitioned_parquet()` groups rows by year from `observed_at`, writes one file per year
- **Append**: `append_partitioned_parquet()` reads the existing year file, merges new rows, rewrites only that year's file
- **Read**: DuckDB uses glob pattern `price_observation_*.parquet` to read all partitions as a single virtual table
- **Deduplication**: by `(tracked_wine_id, vintage, bottle_size_ml, retailer_name, date)` — handled at write time

## Consequences
- Appending a single observation only rewrites the current year's file (~100s of rows), not the entire history
- DuckDB transparently reads all partitions via glob
- No partition pruning by year in queries yet (not needed at current scale)
- Year boundary handling is implicit in `observed_at.year`
