# Building & Testing

How to run the test suite, build distribution packages, and validate ETL output on macOS.

---

## 1. Running Tests

### 1.1 Prerequisites

```bash
cd ~/repos/cellarbrain
source .venv/bin/activate
pip install -e ".[research,sommelier,dashboard,ingest]"
pip install pytest
```

### 1.2 Run All Unit Tests

```bash
pytest tests/ -v --ignore=tests/test_integration.py
```

Unit tests run entirely in-memory using temporary directories — no CSV files or network access needed.

### 1.3 Run Integration Tests

Integration tests require real Vinocell CSV exports in `raw/`:

```bash
pytest tests/test_integration.py -v
```

These tests run the full ETL pipeline and verify output integrity. They are skipped automatically if CSV files are missing.

### 1.4 Run All Tests

```bash
pytest
```

Runs everything in `tests/` as configured in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
```

### 1.5 Useful pytest Options

| Command | Purpose |
|---------|---------|
| `pytest -x` | Stop on first failure |
| `pytest --tb=short` | Shorter tracebacks |
| `pytest -k "test_parsers"` | Run tests matching name pattern |
| `pytest -k "TestBuildWineries"` | Run a specific test class |
| `pytest tests/test_query.py` | Run a single test file |
| `pytest tests/test_query.py::TestExecuteQuery::test_basic` | Run a single test |
| `pytest -v --tb=long` | Verbose with full tracebacks |
| `pytest --co` | List tests without running (collect only) |
| `pytest -x --pdb` | Drop into debugger on first failure |

### 1.6 VS Code Test Runner

With pytest enabled in `.vscode/settings.json`, the VS Code Test Explorer automatically discovers all tests:

1. Open the **Testing** panel (beaker icon in sidebar, or `Cmd+Shift+P` → "Testing: Focus on Test Explorer View")
2. Click the play button next to any test, class, or file to run it
3. Click the debug button to run with breakpoints
4. Green ✓ = pass, red ✗ = fail, click to see output

### 1.7 Test Structure

```
tests/
├── conftest.py                # Shared fixtures (sample_wine_row, sample_bottle_row, etc.)
├── test_parsers.py            # Generic parser functions
├── test_vinocell_parsers.py   # Vinocell-specific parsers
├── test_transform.py          # Entity builder functions
├── test_writer.py             # Parquet writer + schema enforcement
├── test_query.py              # DuckDB query layer (views, search, validation)
├── test_computed.py           # Calculated fields (drinking_status, price_tier, etc.)
├── test_incremental.py        # Incremental sync / change detection
├── test_dossier_ops.py        # Dossier read/write/resolve
├── test_markdown.py           # Dossier Markdown generation
├── test_mcp_server.py         # MCP tool functions
├── test_settings.py           # TOML config loading + merge
├── test_search.py             # find_wine intent parsing + synonyms
├── test_price.py              # Price tracking
├── test_sommelier.py          # ML pairing engine
├── test_email_poll.py         # Email ingestion
├── test_integration.py        # Full pipeline (requires raw/*.csv)
└── smoke_helpers/             # Smoke test infrastructure
    ├── __init__.py
    ├── checks.py              # CheckResult patterns
    ├── etl_runner.py          # Multi-run ETL execution
    ├── mcp_checks.py          # Async MCP tool validation
    └── report.py              # Markdown report generation
```

### 1.8 Writing Tests

Follow these conventions (from project coding standards):

```python
"""Tests for the transform module."""
from __future__ import annotations

import pytest

from cellarbrain.transform import build_wineries


class TestBuildWineries:
    """Tests for build_wineries."""

    def test_basic(self, sample_wine_row):
        result = build_wineries([sample_wine_row])
        assert len(result) == 1
        assert result[0]["name"] == "Château Margaux"

    def test_empty_input(self):
        result = build_wineries([])
        assert result == []

    def test_deduplication(self, sample_wine_row):
        result = build_wineries([sample_wine_row, sample_wine_row])
        assert len(result) == 1


class TestBuildWineriesErrors:
    """Error cases for build_wineries."""

    def test_missing_required_field(self):
        with pytest.raises(ValueError):
            build_wineries([{"name": None}])
```

Key conventions:
- One test file per source module (`test_<module>.py`)
- Group related tests in classes (`class TestXxx:`)
- Use `tmp_path` fixture for all file I/O
- Use `pytest.raises()` for expected errors
- Use `@pytest.mark.parametrize` for data-driven tests
- Shared fixtures live in `conftest.py`

---

## 2. Running ETL Locally

### 2.1 Full Load (First Time)

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

Output:
```
Parsed 484 wines, 813 bottles (stored), 247 bottles (gone)
Written 12 Parquet tables to output/
Generated 484 dossiers in output/wines/
```

### 2.2 Incremental Sync (Subsequent Runs)

After re-exporting from Vinocell:

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
```

Sync mode:
- Preserves stable entity IDs across runs
- Detects inserts, updates, deletes, and renames
- Only regenerates dossiers for changed wines
- Reports a change summary

### 2.3 Validate Output

```bash
cellarbrain validate
```

Checks:
- Primary key uniqueness across all 12 tables
- Foreign key referential integrity
- Domain constraints (enums, ranges)
- Dossier file existence for active wines

### 2.4 Verbose ETL (for debugging)

```bash
cellarbrain -vv etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

The `-vv` flag enables DEBUG logging — shows every parse decision, ID assignment, and file write.

---

## 3. Building Distribution Packages

### 3.1 Install Build Tool

```bash
pip install build
```

### 3.2 Build sdist + wheel

```bash
python -m build
```

This creates:
```
dist/
├── cellarbrain-0.2.0.tar.gz     # Source distribution
└── cellarbrain-0.2.0-py3-none-any.whl  # Wheel (binary)
```

### 3.3 Verify the Build

```bash
# Check wheel contents
unzip -l dist/cellarbrain-0.2.0-py3-none-any.whl | head -30

# Test install in a fresh venv
python3 -m venv /tmp/test-install
source /tmp/test-install/bin/activate
pip install dist/cellarbrain-0.2.0-py3-none-any.whl
cellarbrain --help
deactivate
rm -rf /tmp/test-install
```

### 3.4 Build Metadata

The build is configured in `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]
```

The entry point:
```toml
[project.scripts]
cellarbrain = "cellarbrain.cli:main"
```

This means `cellarbrain` CLI command calls `src/cellarbrain/cli.py:main()`.

---

## 4. Smoke Testing

Smoke tests run the full ETL pipeline multiple times and validate output integrity, cross-run consistency, and MCP server integration.

### 4.1 Run via Smoke Test Agent

The recommended way to run smoke tests is via the `cellarbrain-smoketest` agent:

```
@cellarbrain-smoketest smoke test ETL
```

This executes a 6-phase pipeline:
1. **pytest** — Run the full test suite
2. **Full ETL** — Fresh load from CSVs
3. **Incremental sync** — Re-run with `--sync`
4. **Output verification** — Parquet schema + row counts
5. **Data integrity** — FK constraints, dossier linkage
6. **MCP integration** — Start server, call tools, verify responses

Results are written to `smoke-reports/YYYY-MM-DD-HHMMSS.md`.

### 4.2 Run Manually

```bash
# Phase 1: Unit tests
pytest tests/ -v --ignore=tests/test_integration.py

# Phase 2: Full ETL
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# Phase 3: Incremental sync
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync

# Phase 4: Validate
cellarbrain validate

# Phase 5: Quick stats check
cellarbrain stats
cellarbrain stats --by country
```

### 4.3 Extended Smoke Tests

For change-aware testing after code modifications:

```
@cellarbrain-extended-smoketest test my changes
```

This agent analyses your code changes, plans targeted tests, and produces a combined report.

---

## 5. Recalculating Fields

After changing configuration (price tiers, currency rates, cellar rules):

```bash
cellarbrain recalc
```

This recomputes:
- `drinking_status` from drinking window dates
- `age_years` from vintage
- `price_tier` from list price + tier config
- `list_price` from original price + currency conversion
- `is_onsite` / `is_in_transit` from cellar classification rules

Then regenerates affected dossiers.

---

## 6. Code Quality (Recommended)

While not strictly required by the project, these tools are recommended for development:

### 6.1 Type Checking with Pylance

VS Code with Pylance provides inline type checking. The project uses:
- `from __future__ import annotations` in every module
- Modern union syntax: `str | None` (not `Optional[str]`)
- Built-in generics: `list[str]`, `dict[str, int]`, `tuple[str, ...]`

### 6.2 Linting with ruff (optional)

```bash
pip install ruff
ruff check src/ tests/
ruff format --check src/ tests/
```

---

## Next Steps

- [Publishing](03-publishing.md) — Publish to PyPI and Homebrew
- [Installation & Running](04-installation-and-running.md) — Run all modules in detail
