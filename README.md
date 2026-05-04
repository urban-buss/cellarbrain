# Cellarbrain

AI sommelier for your wine cellar. Transforms
[Vinocell](https://www.vinocell.com/) CSV exports into normalised Parquet
tables, per-wine Markdown dossiers, and an in-process DuckDB query layer
that AI agents can use via the [Model Context Protocol](https://modelcontextprotocol.io/).

## Quick start

```bash
# Clone and install (Python 3.11+)
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain
python -m venv .venv

# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -e .
```

## Usage

### 1. Run the ETL pipeline

Export your cellar from cellarbrain (File → Export → CSV) and place the files
in `raw/`:

```
raw/
├── export-wines.csv
├── export-bottles-stored.csv
└── export-bottles-gone.csv      # optional
```

Then run:

```bash
# Full load (first time)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# Incremental sync (subsequent runs — detects changes, preserves IDs)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
```

Output goes to `output/` — 12 Parquet entity files and per-wine Markdown
dossiers under `output/wines/`.

### 2. Query your cellar

```bash
# SQL query (DuckDB syntax)
cellarbrain query "SELECT w.name, wy.name AS winery, w.vintage FROM wine w JOIN winery wy ON w.winery_id = wy.winery_id LIMIT 10"

# Output as CSV or JSON
cellarbrain query "SELECT * FROM wine LIMIT 5" --format csv
cellarbrain query "SELECT * FROM bottle WHERE status = 'stored'" --format json

# SQL from a file
cellarbrain query -f my_query.sql
```

### 3. Cellar statistics

```bash
cellarbrain stats                     # Overall summary
cellarbrain stats --by country        # Grouped by country
cellarbrain stats --by grape          # Grouped by grape variety
# Also: region, category, vintage, winery, cellar, provider, status
```

### 4. Wine dossiers

```bash
cellarbrain dossier 42                # Read dossier for wine #42
cellarbrain dossier --search Barolo   # Search wines by name, grape, region…
cellarbrain dossier --pending         # Wines with pending agent research
```

### 5. Validate output

```bash
cellarbrain validate                  # Check Parquet integrity
```

### 6. Start the MCP server

```bash
cellarbrain mcp                       # stdio transport (default)
cellarbrain mcp --transport sse       # SSE transport for HTTP clients
```

All subcommands accept `-d <path>` to point at a different data directory
(default: `output`).

> **Legacy mode:** The old `cellarbrain <wines.csv> <bottles.csv>` syntax still
> works but emits a deprecation warning.

---

## Web Explorer

A local web dashboard for browsing your cellar, observability data, and running queries interactively.

```bash
cellarbrain dashboard          # opens at http://localhost:8017
```

Pages: overview, tool usage, errors, sessions, latency charts, live tail (SSE), cellar browser, bottles, drinking window, tracked wines, SQL playground, statistics, and workbench. Requires a prior ETL run and MCP log store.

---

## MCP server

The MCP server exposes 7 read/write tools for AI agents (Claude, OpenClaw,
Copilot, etc.). Tools are **thin data primitives** — all reasoning stays in
the agent.

### Tools

| Tool | Description |
|---|---|
| `query_cellar` | Run read-only SQL against the cellar (DuckDB over Parquet) |
| `cellar_stats` | Summary statistics, optionally grouped by 9 dimensions |
| `find_wine` | Text search across name, winery, region, grape, vintage |
| `read_dossier` | Read a wine's full Markdown dossier |
| `update_dossier` | Write to agent-owned dossier sections (ETL sections protected) |
| `reload_data` | Re-run the ETL pipeline in-process |
| `pending_research` | List wines with empty agent sections, sorted by priority |

### Resources

| URI | Description |
|---|---|
| `wine://list` | All wines with basic metadata |
| `wine://cellar` | Wines currently in the cellar |
| `wine://favorites` | Favorite wines |
| `wine://{wine_id}` | Full dossier for a specific wine |
| `cellar://stats` | Current cellar statistics |
| `cellar://drinking-now` | Wines in their optimal drinking window |
| `etl://last-run` | Last ETL run metadata |
| `etl://changes` | Change log from the last ETL run |

### Prompts

| Prompt | Description |
|---|---|
| `cellar_qa` | System prompt for cellar Q&A (embeds live stats) |
| `food_pairing` | Food pairing workflow for a given dish |
| `wine_research` | Deep research workflow for a single wine |
| `batch_research` | Batch research across pending wines |

### Configure with Claude Desktop

Add to `claude_desktop_config.json`:

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

If cellarbrain is installed in a virtualenv, use the full path:

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

To point at a different data directory:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["-d", "/path/to/output", "mcp"],
      "env": {}
    }
  }
}
```

### Configure with VS Code (Copilot)

Add to `.vscode/mcp.json` in your workspace:

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

Or with a virtualenv on Windows:

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

### Configure with OpenClaw

See [`.docs/design/openclaw-skill.md`](.docs/design/openclaw-skill.md) for
the full skill integration design.

---

## Data model

The ETL produces 12 normalised Parquet tables:

| Table | Description |
|---|---|
| `wine` | Central wine catalog with all attributes |
| `bottle` | Individual bottles (stored + consumed) |
| `winery` | Producer lookup |
| `appellation` | Country / region / subregion / classification |
| `grape` | Grape variety lookup |
| `wine_grape` | Wine–grape junction with blend percentages |
| `tasting` | Personal tasting notes and scores |
| `pro_rating` | Professional critic scores |
| `cellar` | Physical storage locations |
| `provider` | Retailers / sources |
| `etl_run` | Pipeline run history |
| `change_log` | Row-level insert / update / delete audit trail |

Plus per-wine Markdown dossiers in `output/wines/` with:
- ETL-owned sections (identity, origin, inventory, tastings, etc.)
- Agent-owned sections (producer profile, vintage report, food pairings, etc.)
- YAML frontmatter tracking which agent sections are populated vs pending

See [`.docs/data-model/source/`](.docs/data-model/source/) and
[`.docs/data-model/target/`](.docs/data-model/target/) for detailed field documentation.

---

## Project structure

```
src/cellarbrain/
├── cli.py            # CLI entry point with subcommands
├── reader.py         # CSV readers
├── parsers.py        # Field-level parsers
├── transform.py      # Normalisation and entity building
├── writer.py         # Parquet writer with Arrow schemas
├── validate.py       # Post-ETL validation
├── incremental.py    # Change detection and sync
├── markdown.py       # Dossier generation with agent section preservation
├── query.py          # DuckDB query layer (stats, search, SQL)
├── dossier_ops.py    # Dossier read/write/pending operations
└── mcp_server.py     # FastMCP server (7 tools, 8 resources, 4 prompts)
```

## Development

> **Note:** Unit tests and smoke tests require a source checkout of the
> repository. They are not included in the PyPI package.

```bash
# Clone and set up for development
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate      # macOS / Linux
pip install -e ".[dev,research]"

# Run tests
pytest tests/ -v

# Run only unit tests (fast, no CSV files needed)
pytest tests/ -v --ignore=tests/test_integration.py

# Run integration tests (requires raw/*.csv files)
pytest tests/test_integration.py -v
```

## License

Private — not for redistribution.
