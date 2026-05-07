# Quick Start

Get from zero to a working cellar in 5 minutes.

## Prerequisites

- Python 3.11+ installed ([details](prerequisites.md))
- Three Vinocell CSV exports in a `raw/` directory

## Steps

```bash
# 1. Clone and enter
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain

# 2. Create venv and install
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[research,sommelier,dashboard,ingest]"

# 3. Run ETL (place your Vinocell CSVs in raw/ first)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# 4. Verify
cellarbrain validate && cellarbrain stats

# 5. Start MCP server (for AI agent use)
cellarbrain mcp
```

## Verify

After step 4 you should see:

```
✓ All 12 tables valid
✓ Primary keys unique
✓ Foreign keys valid
✓ Domain constraints satisfied
```

## Next Steps

- [Installation options](installation.md) — PyPI, Homebrew, or source
- [ETL module](../modules/etl.md) — Full/sync/verbose modes, recalc, validate
- [MCP server](../modules/mcp-server.md) — Connect AI agents
- [Configuration](../configuration/overview.md) — TOML settings, env vars
