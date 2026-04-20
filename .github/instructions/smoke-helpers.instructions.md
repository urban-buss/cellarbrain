---
description: "Use when editing or extending the ETL smoke test helpers in tests/smoke_helpers/. Covers CheckResult patterns, DuckDB usage, async MCP testing, and report generation conventions."
applyTo: "tests/smoke_helpers/**/*.py"
---

# Smoke Helpers Conventions

## Return Types

- All verification functions return `CheckResult(name, passed, details, data)`.
- `data` is an optional dict for structured results (e.g. entity counts). Default to empty dict.
- ETL runs return `RunResult` with structured metrics — no stdout parsing by callers.

## DuckDB Usage (verify.py)

- Use `_pq(output_dir, name)` for DuckDB-friendly Parquet paths.
- All queries run in-process via `duckdb.sql()` — no persistent database.
- No hardcoded row counts or wine names — query the Parquet files dynamically.
- Register Parquet as views for FK checks: `CREATE VIEW x AS SELECT * FROM read_parquet(...)`.

## MCP Testing (mcp_checks.py)

- All MCP tests are async — the public `run_mcp_checks()` wraps with `asyncio.run()`.
- Use `StdioServerParameters` to spawn the server and `mcp.client.ClientSession` to connect.
- Tool timeout: 30s. Resource timeout: 15s.
- Dynamic tools (requiring a tracked_wine_id) are skipped with PASS when no tracked wines exist.
- `_non_error(text)` is the default pass function — checks result doesn't start with "Error:".

## Report Generation (report.py)

- `generate_report()` produces the complete Markdown string — callers should not compose reports manually.
- `write_report()` handles timestamped filenames and directory creation.
- Reports are always written, even on total success.

## Testing

- Smoke helpers are tested indirectly via the `py -m tests.smoke_helpers` CLI.
- Unit tests for individual check functions belong in `tests/test_smoke_helpers.py` if needed.
