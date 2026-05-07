# Testing

How to run the test suite, write tests, and perform smoke testing.

## Prerequisites

```bash
source .venv/bin/activate
pip install -e ".[research,sommelier,dashboard,ingest]"
pip install pytest
```

## Running Tests

### All Unit Tests

```bash
pytest tests/ -v --ignore=tests/test_integration.py
```

Unit tests run in-memory using temporary directories — no CSV files or network access needed.

### Integration Tests

Require real Vinocell CSV exports in `raw/` (skipped automatically if missing):

```bash
pytest tests/test_integration.py -v
```

### All Tests

```bash
pytest
```

### Useful Options

| Command | Purpose |
|---------|---------|
| `pytest -x` | Stop on first failure |
| `pytest --tb=short` | Shorter tracebacks |
| `pytest -k "test_parsers"` | Run tests matching name pattern |
| `pytest -k "TestBuildWineries"` | Run a specific test class |
| `pytest tests/test_query.py` | Run a single test file |
| `pytest tests/test_query.py::TestExecuteQuery::test_basic` | Run a single test |
| `pytest --co` | List tests without running (collect only) |
| `pytest -x --pdb` | Drop into debugger on first failure |

### VS Code Test Runner

With pytest enabled in `.vscode/settings.json`, the Test Explorer discovers all tests automatically:

1. Open the **Testing** panel (beaker icon in sidebar)
2. Click the play button next to any test, class, or file
3. Click the debug button to run with breakpoints

## Test Structure

```
tests/
├── conftest.py                # Shared fixtures (sample_wine_row, sample_bottle_row, etc.)
├── test_parsers.py            # Generic parser functions
├── test_vinocell_parsers.py   # Vinocell-specific parsers
├── test_transform.py          # Entity builder functions
├── test_writer.py             # Parquet writer + schema enforcement
├── test_query.py              # DuckDB query layer
├── test_computed.py           # Calculated fields
├── test_incremental.py        # Incremental sync / change detection
├── test_dossier_ops.py        # Dossier read/write/resolve
├── test_markdown.py           # Dossier Markdown generation
├── test_mcp_server.py         # MCP tool functions
├── test_settings.py           # TOML config loading
├── test_search.py             # find_wine intent parsing
├── test_price.py              # Price tracking
├── test_sommelier.py          # ML pairing engine
├── test_email_poll.py         # Email ingestion
├── test_integration.py        # Full pipeline (requires raw/*.csv)
└── smoke_helpers/             # Smoke test infrastructure
```

## Writing Tests

Follow these conventions:

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
```

Key conventions:
- One test file per source module (`test_<module>.py`)
- Group related tests in classes (`class TestXxx:`)
- Use `tmp_path` fixture for all file I/O
- Use `pytest.raises()` for expected errors
- Use `@pytest.mark.parametrize` for data-driven tests
- Shared fixtures live in `conftest.py`

## Smoke Testing

Smoke tests validate the full ETL pipeline end-to-end.

### Via Agent (recommended)

```
@cellarbrain-smoketest smoke test ETL
```

Executes a 6-phase pipeline: pytest → full ETL → incremental sync → output verification → data integrity → MCP integration. Results in `smoke-reports/`.

### Manual

```bash
pytest tests/ -v --ignore=tests/test_integration.py
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
cellarbrain validate
cellarbrain stats && cellarbrain stats --by country
```

### Extended (change-aware)

```
@cellarbrain-extended-smoketest test my changes
```

## Code Quality

```bash
# Type checking — Pylance in VS Code (inline)
# Linting (optional)
pip install ruff
ruff check src/ tests/
ruff format --check src/ tests/
```

## Next Steps

- [Building](building.md) — Build distribution packages
- [VS Code Debugging](../operations/vscode-debugging.md) — Debug configurations
