# CLI Commands

Command-line interface for querying cellar data, viewing dossiers, and managing the wishlist.

## Cellar Statistics

```bash
# Overview
cellarbrain stats

# Group by dimension
cellarbrain stats --by country
cellarbrain stats --by region
cellarbrain stats --by category
cellarbrain stats --by vintage
cellarbrain stats --by winery
cellarbrain stats --by grape
cellarbrain stats --by cellar
cellarbrain stats --by provider
cellarbrain stats --by status
```

## SQL Queries

```bash
# Count all wines
cellarbrain query "SELECT count(*) AS total FROM wines"

# List wines by region
cellarbrain query "SELECT full_name, vintage, region FROM wines WHERE region = 'Bordeaux'"

# Export as CSV
cellarbrain query "SELECT * FROM wines_stored" --format csv > wines.csv

# Export as JSON
cellarbrain query "SELECT * FROM wines_stored LIMIT 10" --format json

# Read SQL from a file
cellarbrain query -f queries/my-query.sql
```

### Available Views

| View | Description |
|------|-------------|
| `wines` | All wines with computed fields (full_name, drinking_status, price_tier) |
| `wines_stored` | Only wines with bottles currently in cellar |
| `wines_full` | Extended with winery, appellation, grapes joined |
| `bottles_stored` | Individual bottles with cellar/shelf location |
| `bottles_full` | Bottles with wine details joined |
| `tracked_wines` | Wishlist / tracked wines for price monitoring |

## Wine Dossiers

```bash
# View a specific wine dossier
cellarbrain dossier 42

# View specific sections only
cellarbrain dossier 42 --sections identity origin producer_profile

# Search for wines
cellarbrain dossier --search "Barolo"
cellarbrain dossier --search "Pinot Noir 2019"

# List wines pending research
cellarbrain dossier --pending
cellarbrain dossier --pending --limit 50
```

## Wishlist & Price Tracking

```bash
# Current alerts (price drops, new listings, back in stock)
cellarbrain wishlist alerts
cellarbrain wishlist alerts --days 7

# Tracked wine statistics
cellarbrain wishlist stats
```

Price scanning is agent-driven — use the `cellarbrain-price-tracker` agent or manually log via MCP.

## Observability Logs

```bash
cellarbrain logs                         # tail recent events
cellarbrain logs --tail 50               # more results
cellarbrain logs --errors --since 24     # errors in last 24 hours
cellarbrain logs --usage                 # tool usage summary
cellarbrain logs --latency               # latency percentiles
cellarbrain logs --sessions              # session summary
cellarbrain logs --prune                 # delete events older than retention_days
```

## Installation Diagnostics

```bash
# Full diagnostic report (version, paths, modules, MCP config)
cellarbrain info

# JSON output for scripting
cellarbrain info --json

# Ready-to-paste MCP client config for Claude Desktop / OpenClaw
cellarbrain info --mcp-config

# Just resolved paths
cellarbrain info --paths

# Just installed modules and optional extras
cellarbrain info --modules
```

## Global Options

| Flag | Effect |
|------|--------|
| `-v` | INFO logging |
| `-vv` | DEBUG logging |
| `-q` | ERROR-only logging |
| `-c CONFIG` | Custom TOML config file |
| `-d DATA_DIR` | Custom data directory |
| `--log-file PATH` | Write logs to file |

## Full Reference

See [docs/cli-reference.md](../../docs/cli-reference.md) for the complete CLI command reference.

## Next Steps

- [ETL](etl.md) — Run the pipeline
- [MCP Server](mcp-server.md) — Connect AI agents
