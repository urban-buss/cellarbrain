# MCP Server

The MCP (Model Context Protocol) server exposes the cellar data layer to AI agents.

## Prerequisites

- ETL must have been run at least once (Parquet data in `output/`)
- For sommelier tools: trained model ([Sommelier](sommelier.md))

## Transports

### stdio (default)

```bash
cellarbrain mcp
```

Default transport for Claude Desktop and VS Code Copilot. Communicates over stdin/stdout via JSON-RPC. No visible output — use Ctrl+C to stop.

### SSE (HTTP)

```bash
cellarbrain mcp --transport sse --port 8080
```

Starts an HTTP server at `http://localhost:8080`. Useful for HTTP-based MCP clients, remote access, or always-on services.

### Options

```bash
cellarbrain -d /path/to/output mcp              # custom data directory
cellarbrain -c /path/to/cellarbrain.toml mcp     # custom config
```

| Environment Variable | Purpose |
|---------------------|---------|
| `CELLARBRAIN_DATA_DIR` | Path to Parquet data (overrides config) |
| `CELLARBRAIN_CONFIG` | Path to TOML config file |

## Client Configuration

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain",
      "args": ["-d", "/Users/<you>/cellarbrain-data/output", "mcp"],
      "env": {}
    }
  }
}
```

### VS Code (Copilot)

The workspace includes `.vscode/mcp.json`:

```json
{
  "servers": {
    "cellarbrain": {
      "command": "${command:python.interpreterPath}",
      "args": ["-m", "cellarbrain", "-d", "${workspaceFolder}/output", "mcp"],
      "env": {}
    }
  }
}
```

### OpenClaw (or other MCP clients)

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain",
      "args": ["-d", "/Users/<you>/repos/cellarbrain/output", "mcp"],
      "env": {}
    }
  }
}
```

With environment variables:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["mcp"],
      "env": {
        "CELLARBRAIN_DATA_DIR": "/Users/<you>/repos/cellarbrain/output",
        "CELLARBRAIN_CONFIG": "/Users/<you>/repos/cellarbrain/cellarbrain.toml"
      }
    }
  }
}
```

SSE transport:

```json
{
  "mcpServers": {
    "cellarbrain": { "url": "http://localhost:8080/sse" }
  }
}
```

## Available Tools

### Query & Search

| Tool | Parameters | Description |
|------|-----------|-------------|
| `query_cellar` | `sql: str` | Read-only DuckDB SQL against pre-joined views |
| `find_wine` | `query: str, limit?: int` | Text search with intent parsing + synonym expansion |
| `cellar_info` | `verbose?: bool` | Version, config, ETL freshness, inventory summary |
| `cellar_stats` | `group_by?: str, limit?: int, sort_by?: str` | Summary statistics, optionally grouped |
| `cellar_churn` | `days?: int` | Recent additions and removals |
| `search_synonyms` | `action: str, key?: str, value?: str` | Manage custom search synonyms |
| `server_stats` | — | Internal MCP server performance metrics |

### Dossier Management

| Tool | Parameters | Description |
|------|-----------|-------------|
| `read_dossier` | `wine_id: int, sections?: list[str]` | Read wine dossier (filtered sections) |
| `update_dossier` | `wine_id: int, section: str, content: str` | Write an agent-owned section |
| `batch_update_dossier` | `wine_ids: list[int], section: str, content: str` | Write same section to multiple wines |
| `pending_research` | `limit?: int` | List wines with empty agent sections |
| `read_companion_dossier` | `tracked_wine_id: int, sections?: list[str]` | Read companion dossier |
| `update_companion_dossier` | `tracked_wine_id: int, section: str, content: str` | Write companion section |
| `list_companion_dossiers` | `pending_only?: bool` | List tracked wines |
| `pending_companion_research` | `limit?: int` | Tracked wines needing research |
| `get_format_siblings` | `wine_id: int` | Get format variants (Magnum, etc.) |

### Price Tracking

| Tool | Parameters | Description |
|------|-----------|-------------|
| `log_price` | `tracked_wine_id, vintage, bottle_size_ml, retailer_name, price, currency, in_stock, ...` | Record a price observation |
| `tracked_wine_prices` | `tracked_wine_id: int` | Latest prices across retailers |
| `price_history` | `tracked_wine_id: int, vintage?: int, months?: int` | Monthly min/max/avg CHF |
| `wishlist_alerts` | `days?: int` | Price drops, new listings, back in stock |

### Sommelier (requires trained model)

| Tool | Parameters | Description |
|------|-----------|-------------|
| `suggest_wines` | `food_description: str, limit?: int` | Semantic food → wine pairing |
| `suggest_foods` | `wine_id: int, limit?: int` | Semantic wine → food pairing |
| `add_pairing` | `wine_id: int, food_description: str, score?: float` | Record a pairing observation |
| `train_sommelier` | — | Trigger model retraining |

### Data Refresh

| Tool | Parameters | Description |
|------|-----------|-------------|
| `reload_data` | — | Re-run ETL from CSV exports |

## SQL Views (for `query_cellar`)

| View | Description |
|------|-------------|
| `wines` | All wines with computed fields (full_name, drinking_status, price_tier) |
| `wines_stored` | Only wines with bottles in cellar |
| `wines_full` | Extended with grapes, appellation joined |
| `bottles_stored` | Individual bottles with cellar/shelf location |
| `bottles_full` | Bottles with wine details joined |
| `tracked_wines` | Wishlist wines for price monitoring |

## Debugging

### Verify Data Exists

```bash
ls output/*.parquet   # expect 12 files
cellarbrain stats
cellarbrain query "SELECT count(*) FROM wines"
```

### Test Server Starts

```bash
cellarbrain -vv --log-file /tmp/mcp-debug.log mcp
# Ctrl+C to stop, then check /tmp/mcp-debug.log
```

### Diagnostics via Tool

Call `cellar_info(verbose=True)` — returns version, data dir, last ETL, table counts, config summary, sommelier status.

### Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "No such file or directory" | Wrong path to cellarbrain binary | Use full path: `.venv/bin/cellarbrain` |
| "ModuleNotFoundError" | Package not in the target venv | Check which venv the MCP config uses |
| "DataStaleError" | No Parquet files | Run `cellarbrain etl` first |
| "Connection refused" (SSE) | Server not running / wrong port | Verify `--port` matches client config |
| Tools return empty results | Data directory mismatch | Check `-d` flag or `CELLARBRAIN_DATA_DIR` |

## Testing the Integration

```
"How many wines are in my cellar?"      → cellar_stats()
"Find any Barolo wines"                 → find_wine(query="Barolo")
"Read the dossier for wine 42"          → read_dossier(wine_id=42)
"What wine goes with grilled lamb?"     → suggest_wines(food_description="grilled lamb")
```

## Next Steps

- [Agent Skills](agent-skills.md) — Available skills and design principles
- [Logging](../operations/logging.md) — MCP server logging configuration
