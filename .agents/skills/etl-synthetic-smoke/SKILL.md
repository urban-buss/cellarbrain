---
name: etl-synthetic-smoke
description: Verify Cellarbrain ingestion and ETL behavior end to end using only synthetic development fixtures. Use after parser, transform, computed-field, validation, writer, Parquet, DuckDB, dossier, or query changes.
---

# ETL Synthetic Smoke

Run `python3 scripts/run_smoke.py` from this skill directory. The script uses
the repository's existing uv-created `.venv` and focused synthetic ETL tests;
it never reads production data.

Before accepting a schema change, also confirm field names, types, nulls,
units, normalized identifiers, dates, currencies, row counts, deterministic
ordering, and downstream query compatibility. Add a minimal synthetic fixture
when the changed behavior is not covered. Escalate to the full suite after the
focused smoke passes.
