# Fresh Install Validation

Agent prompt for validating a fresh PyPI install end-to-end.

Copy-paste this prompt into a fresh agent session:

---

```
You are validating a fresh install of the "cellarbrain" Python package from PyPI.
Follow these steps exactly and report the result of each step.

### Step 1 — Create a clean environment

```bash
mkdir cellarbrain-test && cd cellarbrain-test
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate
```

### Step 2 — Install from PyPI

```bash
pip install "cellarbrain[dashboard,dev]"
cellarbrain --version
```

Report the installed version.

### Step 3 — Prepare raw CSV files

Create `raw/` and place three Vinocell CSV exports:

```
raw/
├── export-wines.csv
├── export-bottles-stored.csv
└── export-bottles-gone.csv
```

### Step 4 — Run ETL

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

Report: success?, wine count, files created in `output/`.

### Step 5 — Run pytest

```bash
pytest
```

Report pass/fail counts.

### Step 6 — Run smoke tests

```bash
mkdir raw/260501
cp raw/export-*.csv raw/260501/
python -m tests.smoke_helpers --raw-dir raw --output-dir output --folders 260501
```

Report results.

### Step 7 — Verify MCP server

```bash
cellarbrain mcp   # Ctrl+C to stop
```

### Step 8 — Verify dashboard

```bash
cellarbrain dashboard --port 8765   # Ctrl+C to stop
```

Confirm reachable at http://localhost:8765.

### Summary

Report overall status: all steps passed / which failed.
```
