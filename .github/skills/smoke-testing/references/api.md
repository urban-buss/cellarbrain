# Smoke Helpers API Reference

Detailed function signatures and dataclass fields for `tests/smoke_helpers/`.
Only needed when using the library module-by-module rather than the CLI.

## Dataclasses (`__init__.py`)

### SmokeConfig

| Field | Type | Description |
|-------|------|-------------|
| `raw_dir` | `Path` | Directory containing YYMMDD raw CSV folders |
| `output_dir` | `Path` | ETL output directory |
| `folders` | `list[str]` | Ordered folder names; first = full load |
| `python_version` | `str` | e.g. "3.14.0" |
| `cellarbrain_version` | `str` | e.g. "0.1.0" |

### RunResult

| Field | Type | Description |
|-------|------|-------------|
| `folder` | `str` | YYMMDD folder name |
| `sync_mode` | `bool` | `False` = full load, `True` = incremental sync |
| `exit_ok` | `bool` | Whether the run passed validation |
| `csv_counts` | `dict[str, int]` | Keys: `wines`, `bottles`, `bottles_gone` |
| `slug_matching` | `dict[str, int]` | Keys: `existing`, `new`, `deleted`, `revived`, `renamed` |
| `entity_counts` | `dict[str, int]` | Keys: `winery`, `wine`, `bottle`, etc. |
| `change_summary` | `dict[str, int]` | Keys: `inserts`, `updates`, `deletes`, `renames` |
| `validation_passed` | `int` | Number of validation rules that passed |
| `validation_failed` | `int` | Number of validation rules that failed |
| `dossier_count` | `int` | Wine dossiers generated |
| `companion_count` | `int` | Companion dossiers generated |
| `warnings` | `list[str]` | Warning messages |
| `errors` | `list[str]` | Error messages |

### CheckResult

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Human-readable check name |
| `passed` | `bool` | Whether the check passed |
| `details` | `str` | Summary string |
| `data` | `dict` | Optional structured data (e.g. entity counts) |

### PytestResult (`runner.py`)

| Field | Type | Description |
|-------|------|-------------|
| `passed` | `int` | Tests passed |
| `failed` | `int` | Tests failed |
| `errors` | `int` | Collection errors |
| `warnings` | `int` | Warnings |
| `total` | `int` | Total tests run |
| `exit_code` | `int` | pytest exit code |
| `output` | `str` | Raw combined stdout+stderr |
| `ok` | `bool` | Property: `exit_code == 0` |

### RebuildResult (`runner.py`)

| Field | Type | Description |
|-------|------|-------------|
| `ok` | `bool` | Whether `pip install -e .` succeeded |
| `exe_path` | `Path \| None` | Path to `cellarbrain.exe` entry point |
| `output` | `str` | Raw pip output |

## Functions

### discover.py

| Function | Signature | Returns |
|----------|-----------|---------|
| `discover_raw_folders` | `(raw_dir: Path) -> list[str]` | Sorted YYMMDD folder names |
| `validate_folder` | `(raw_dir: Path, folder: str) -> bool` | True if all 3 CSVs present |
| `csv_paths` | `(raw_dir: Path, folder: str) -> tuple[str, str, str]` | (wines, bottles, gone) paths |
| `get_environment` | `() -> dict[str, str]` | `python_version`, `cellarbrain_version` |

### runner.py

| Function | Signature | Returns |
|----------|-----------|---------|
| `run_etl` | `(raw_dir, folder, output_dir, *, sync=False, settings=None) -> RunResult` | Calls `cli.run()` in-process; parses captured stdout |
| `clean_output` | `(output_dir: Path) -> int` | Removes `*.parquet`; returns count |
| `run_pytest` | `() -> PytestResult` | Runs `pytest --tb=short -q` with 300s timeout |
| `rebuild_server` | `() -> RebuildResult` | Runs `pip install -e .`; locates exe |

### verify.py

| Function | Signature | Returns |
|----------|-----------|---------|
| `check_parquet_files` | `(output_dir: Path) -> CheckResult` | 13 files exist, size > 0 |
| `check_etl_runs` | `(output_dir: Path, expected_count: int) -> CheckResult` | Run history, types, timestamps |
| `check_entity_counts` | `(output_dir: Path) -> CheckResult` | Row counts + sanity checks |
| `check_wine_schema` | `(output_dir: Path) -> CheckResult` | Required columns present, banned absent |
| `check_dossiers` | `(output_dir: Path) -> CheckResult` | Count + spot-check frontmatter |
| `run_validation` | `(output_dir: Path) -> CheckResult` | Calls `validate.validate()` directly |
| `check_cross_run` | `(output_dir: Path) -> list[CheckResult]` | Wine count trend, change log growth, run count |
| `check_fk_integrity` | `(output_dir: Path) -> list[CheckResult]` | 13 FK constraint checks |
| `check_dossier_integrity` | `(output_dir: Path) -> list[CheckResult]` | 6 bidirectional linkage checks |

### mcp_checks.py

| Function | Signature | Returns |
|----------|-----------|---------|
| `run_mcp_checks` | `(exe_path: Path, output_dir: Path) -> list[CheckResult]` | 16 tool + 5 resource checks |

Spawns MCP server via `StdioServerParameters`, connects with `mcp.client.ClientSession`. Tools have 30s timeout; resources 15s. Dynamic tools (companion dossiers, price tracking) are skipped when no tracked wines exist.

### report.py

| Function | Signature | Returns |
|----------|-----------|---------|
| `generate_report` | `(config, runs, output_checks, cross_checks, *, trigger, findings, pytest_result, integrity_checks, mcp_checks) -> str` | Complete Markdown report |
| `write_report` | `(content: str, report_dir: Path) -> Path` | Writes to `YYYY-MM-DD-HHMMSS.md`; returns path |

## CLI Arguments (`__main__.py`)

| Argument | Default | Description |
|----------|---------|-------------|
| `--raw-dir` | `raw` | Directory containing YYMMDD raw CSV folders |
| `--output-dir` | `output` | ETL output directory |
| `--report-dir` | `smoke-reports` | Directory for report output |
| `--folders` | auto-discover | Comma-separated list of YYMMDD folders |
| `--trigger` | `"py -m tests.smoke_helpers"` | Description of what triggered this run |
| `--settings-file` | none | Path to cellarbrain TOML settings file |

## Module-by-Module Usage Example

```python
from pathlib import Path
from tests.smoke_helpers import SmokeConfig, RunResult, CheckResult
from tests.smoke_helpers.discover import discover_raw_folders, get_environment
from tests.smoke_helpers.runner import run_etl, run_pytest, rebuild_server, clean_output
from tests.smoke_helpers.verify import (
    check_parquet_files, check_etl_runs, check_entity_counts,
    check_wine_schema, check_dossiers, run_validation,
    check_cross_run, check_fk_integrity, check_dossier_integrity,
)
from tests.smoke_helpers.mcp_checks import run_mcp_checks
from tests.smoke_helpers.report import generate_report, write_report

folders = discover_raw_folders(Path("raw"))
clean_output(Path("output"))
result = run_etl(Path("raw"), folders[0], Path("output"), sync=False)
checks = [check_parquet_files(Path("output")), ...]
```
