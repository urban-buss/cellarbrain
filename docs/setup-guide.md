# Setup Guide

Step-by-step instructions for installing, configuring, and running Cellarbrain on a new machine.

## Prerequisites

- **Python 3.11+** — verify with `python --version`
- **Git** — to clone the repository
- **Vinocell CSV exports** — exported from the Vinocell iOS/macOS app via File → Export → CSV

## 1. Clone & Install

```bash
git clone https://github.com/urban-buss/cellarbrain
cd cellarbrain
```

Create and activate a virtual environment:

```bash
python -m venv .venv

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Windows (cmd)
.venv\Scripts\activate.bat

# macOS / Linux
source .venv/bin/activate
```

Install in editable mode:

```bash
# Core dependencies only
pip install -e .

# With research extras (adds httpx for web-based wine research)
pip install -e ".[research]"
```

Dependencies installed:

| Package | Purpose |
|---------|---------|
| `pyarrow` | Parquet read/write with explicit schemas |
| `duckdb` | In-process SQL query engine over Parquet |
| `tabulate` | Markdown table formatting for CLI and MCP output |
| `mcp[cli]` | FastMCP server framework (Model Context Protocol) |
| `httpx` | HTTP client for research agents (optional `research` extra) |

## 2. Prepare CSV Exports

Export your cellar from the Vinocell app and place the three CSV files in the `raw/` directory:

```
raw/
├── export-wines.csv
├── export-bottles-stored.csv
└── export-bottles-gone.csv
```

The files are UTF-16 LE encoded, tab-delimited — this is the default Vinocell export format. No conversion needed.

## 3. Configuration

Cellarbrain uses TOML configuration with layered precedence (highest wins):

1. CLI arguments (`--data-dir`, `--config`)
2. Environment variables (`CELLARBRAIN_DATA_DIR`, `CELLARBRAIN_CONFIG`)
3. `cellarbrain.local.toml` (gitignored — machine-specific overrides)
4. `cellarbrain.toml` (checked in — project defaults)
5. Built-in defaults

### Minimal setup

The checked-in `cellarbrain.toml` provides sensible defaults. No configuration is required for a basic setup — just run the ETL.

### Machine-specific overrides

Create `cellarbrain.local.toml` for settings that differ per machine (this file is gitignored):

```toml
# cellarbrain.local.toml — machine-specific overrides

[paths]
data_dir = "output"    # change if your output lives elsewhere
raw_dir = "raw"        # change if CSVs are in a different location
```

### Common configuration options

#### Offsite and in-transit cellars

```toml
# Bottles in these cellars are flagged as offsite
offsite_cellars = ["Off-site Storage", "Wine Locker"]

# Bottles in these cellars are tracked but excluded from inventory counts
in_transit_cellars = ["99 Orders & Subscriptions"]
```

#### Price tiers

Customise the price tier boundaries (in your default currency):

```toml
[[price_tiers]]
label = "budget"
max = 15

[[price_tiers]]
label = "everyday"
max = 27

[[price_tiers]]
label = "premium"
max = 40

[[price_tiers]]
label = "fine"
# no max — catch-all for everything above 40
```

#### Currency

```toml
[currency]
default = "CHF"

[currency.rates]
EUR = 0.93
USD = 0.88
GBP = 1.11
```

See [settings-reference.md](settings-reference.md) for the full list of configuration options.

## 4. Run the ETL Pipeline

### First run (full load)

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

This creates:
- **12 Parquet tables** in `output/` (wine, bottle, winery, appellation, etc.)
- **Per-wine Markdown dossiers** in `output/wines/cellar/` and `output/wines/archive/`

### Subsequent runs (incremental sync)

After re-exporting from cellarbrain with updated data:

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
```

Sync mode preserves stable IDs, detects changes (inserts, updates, deletes, renames), and only regenerates affected dossiers.

### Validate output

```bash
cellarbrain validate
```

Checks FK integrity, PK uniqueness, and domain constraints across all Parquet tables.

## 5. Query Your Cellar

```bash
# Overview statistics
cellarbrain stats
cellarbrain stats --by country

# SQL queries (DuckDB syntax, read-only)
cellarbrain query "SELECT count(*) AS total FROM bottles_stored"
cellarbrain query "SELECT full_name, vintage, bottles_stored FROM wines_stored ORDER BY vintage" --format csv

# Wine dossiers
cellarbrain dossier 42                # view dossier for wine #42
cellarbrain dossier --search Barolo   # search by name, grape, region
cellarbrain dossier --pending         # wines needing agent research
```

See [cli-reference.md](cli-reference.md) for all subcommands and options.

## 6. MCP Server Setup

The MCP server lets AI agents (Claude, Copilot, OpenClaw) query and manage your cellar via the Model Context Protocol.

### Start the server

```bash
# stdio transport (default — used by Claude Desktop, VS Code)
cellarbrain mcp

# SSE transport (for HTTP-based clients)
cellarbrain mcp --transport sse --port 8080
```

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["mcp"],
      "env": {}
    }
  }
}
```

If installed in a virtualenv, use the full path to the executable:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "/path/to/cellarbrain/.venv/bin/cellarbrain",
      "args": ["mcp"],
      "env": {}
    }
  }
}
```

To use a custom data directory or config file:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["-d", "/path/to/output", "-c", "/path/to/cellarbrain.toml", "mcp"],
      "env": {}
    }
  }
}
```

### VS Code (Copilot)

Create `.vscode/mcp.json` in your workspace:

```json
{
  "servers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["mcp"],
      "env": {}
    }
  }
}
```

Windows with a virtualenv:

```json
{
  "servers": {
    "cellarbrain": {
      "command": ".venv\\Scripts\\cellarbrain.exe",
      "args": ["-d", "output", "mcp"],
      "env": {}
    }
  }
}
```

### Environment variables

The MCP server also reads:

| Variable | Purpose |
|----------|---------|
| `CELLARBRAIN_DATA_DIR` | Path to the Parquet data directory (default: `output`) |
| `CELLARBRAIN_CONFIG` | Path to `cellarbrain.toml` config file |

## 7. Development Setup

For contributors and local development:

```bash
# Install with test and research dependencies
pip install -e ".[research]"
pip install pytest

# Run unit tests (no CSV files needed)
pytest tests/ -v --ignore=tests/test_integration.py

# Run all tests including integration (requires raw/*.csv files)
pytest tests/ -v

# Run the app as a module
python -m cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

## Directory Structure After Setup

```
cellarbrain/
├── raw/                          # CSV exports from Vinocell app
│   ├── export-wines.csv
│   ├── export-bottles-stored.csv
│   └── export-bottles-gone.csv
├── output/                       # Generated Parquet + dossiers
│   ├── wine.parquet
│   ├── bottle.parquet
│   ├── winery.parquet
│   ├── appellation.parquet
│   ├── grape.parquet
│   ├── wine_grape.parquet
│   ├── tasting.parquet
│   ├── pro_rating.parquet
│   ├── cellar.parquet
│   ├── provider.parquet
│   ├── etl_run.parquet
│   ├── change_log.parquet
│   └── wines/
│       ├── cellar/               # Dossiers for wines currently stored
│       │   ├── 0001-wine-slug.md
│       │   └── ...
│       ├── archive/              # Dossiers for consumed/removed wines
│       │   └── ...
│       └── tracked/              # Companion dossiers for tracked wines
│           └── ...
├── cellarbrain.toml                 # Project configuration (checked in)
├── cellarbrain.local.toml           # Machine-specific overrides (gitignored)
└── src/cellarbrain/                 # Source code
```

## Troubleshooting

### `ModuleNotFoundError: No module named 'cellarbrain'`

You need to install the package. Run `pip install -e .` from the repo root with your virtualenv active.

### `FileNotFoundError` when running ETL

Check that the CSV file paths are correct and the files exist in `raw/`. The files must be the original Vinocell exports (UTF-16 LE, tab-delimited).

### `UnicodeDecodeError` during CSV reading

The CSV encoding defaults to UTF-16. If your Vinocell version exports in a different encoding, override it in config:

```toml
[csv]
encoding = "utf-8"
```

### Parquet validation errors after ETL

Run `cellarbrain validate` to see specific integrity failures. Common causes:
- Re-running a full load after previously using `--sync` (use `--sync` consistently after the first run)
- CSV exports that are out of sync (re-export all three files at the same time)

### MCP server not connecting

- Verify the path to `cellarbrain` (or `cellarbrain.exe`) in your MCP config is correct
- Ensure the virtualenv is activated or use the full path to the executable
- Check that `output/` contains Parquet files (run ETL first)
- Test with `cellarbrain stats` to confirm the data directory is accessible
