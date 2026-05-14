---
description: "Validate an alpha release installed from PyPI in a clean venv with data in a separate folder. Use when: 'validate alpha', 'test pypi install', 'validate release'."
argument-hint: "Version to validate, e.g. 0.3.0a1"
---
Validate a PyPI-published alpha release of cellarbrain in an isolated
environment. The source checkout provides test files; the installed package
comes from PyPI (NOT an editable install).

## Version

Install: `cellarbrain=={{input}}`

## Setup Instructions

Create a **separate venv** (not the dev `.venv`) and install the alpha:

```powershell
# Windows — from the repo root
python -m venv .venv-alpha
.venv-alpha\Scripts\Activate.ps1
pip install "cellarbrain=={{input}}"
pip install pytest pyarrow duckdb   # test dependencies
```

## Data Directory

Use a **dedicated data folder** outside the source tree for ETL output.
The raw CSVs live in `raw/` (source tree) but output goes elsewhere:

```powershell
$DATA_DIR = "$env:TEMP\cellarbrain-alpha-test"
New-Item -ItemType Directory -Force -Path $DATA_DIR
```

## Validation Phases

### Phase 1 — Package Integrity

```powershell
cellarbrain --help                                              # CLI works
python -c "from cellarbrain import cli, query, mcp_server; print('imports OK')"
pip show cellarbrain                                            # version matches
```

### Phase 2 — Unit Tests (against installed wheel)

Run from the source checkout but import from the installed package:

```powershell
pytest tests/ -k "not integration" --import-mode=importlib -x -q
```

Report pass/fail count.

### Phase 3 — ETL Integration (raw/ → temp output)

```powershell
# Pick the latest raw folder for a quick single-run test
$LATEST = (Get-ChildItem raw -Directory | Sort-Object Name | Select-Object -Last 1).Name
cellarbrain etl "raw/$LATEST/export-wines.csv" "raw/$LATEST/export-bottles-stored.csv" "raw/$LATEST/export-bottles-gone.csv" -o "$DATA_DIR"
```

Verify:
- All 13 Parquet files exist in `$DATA_DIR`
- Dossier directory (`$DATA_DIR/wines/`) has `.md` files
- `cellarbrain validate` passes (if available)

### Phase 4 — Smoke Pipeline (full multi-run)

```powershell
python -m tests.smoke_helpers --raw-dir raw --output-dir "$DATA_DIR" --report-dir "$DATA_DIR/reports"
```

### Phase 5 — MCP Server Spot-Check

Start the MCP server against the alpha output and call key tools:
- `query_cellar` — returns rows
- `find_wine` — finds at least one wine
- `read_dossier` — returns markdown content
- `cellar_stats` — returns statistics

### Phase 6 — Cleanup

```powershell
deactivate
Remove-Item -Recurse -Force .venv-alpha
Remove-Item -Recurse -Force "$env:TEMP\cellarbrain-alpha-test"
```

## Reporting

Summarize results per phase. Flag any differences from running against
the editable dev install. If failures occur, diagnose whether they are:
- **Packaging bugs** (missing files, wrong dependencies)
- **Code bugs** (logic errors caught by tests)
- **Environment issues** (Python version, missing extras)
