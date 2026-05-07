# Web Dashboard

Local web UI for browsing cellar data, observability metrics, and running interactive MCP tool queries.

## Prerequisites

```bash
pip install "cellarbrain[dashboard]"
```

ETL must have been run (for cellar data). The MCP log store is created automatically on first MCP server run.

## Configuration

In `cellarbrain.toml`:

```toml
[dashboard]
port = 8017
workbench_read_only = true       # Block write tools by default
workbench_allow = ["log_price"]  # Explicitly allow specific write tools
```

## Running

```bash
# Default: http://localhost:8017
cellarbrain dashboard

# Custom port
cellarbrain dashboard --port 9000

# Auto-open browser
cellarbrain dashboard --open
```

## Pages

| Page | URL | Description |
|------|-----|-------------|
| Observability Overview | `/` | KPI cards, hourly volume chart, top tools, recent errors |
| Tool Usage | `/tools` | Per-tool call counts, latency percentiles, error rates |
| Error Log | `/errors` | Filterable error list with details |
| Sessions | `/sessions` | Session drill-down with turn grouping |
| Latency | `/latency` | P50/P95/P99 latency breakdown per tool |
| Live Tail | `/tail` | Real-time SSE event stream |
| Cellar Browser | `/cellar` | Paginated, filterable wine list with detail views |
| Bottle Inventory | `/bottles` | Individual bottle locations and status |
| Tracked Wines | `/tracked` | Wishlist wines with price observations |
| Statistics | `/stats` | Visual cellar statistics (country, region, category, etc.) |
| SQL Playground | `/sql` | Interactive DuckDB SQL editor |
| Tool Workbench | `/workbench` | Call MCP tools with a web form UI |

## Monitoring Features

### Observability Overview (`/`)

- **KPI Cards**: Total calls, error rate %, average latency, active sessions
- **Hourly Volume Chart**: Bar chart showing call distribution over time
- **Top Tools**: Most-called tools with call count and avg latency
- **Recent Errors**: Last 5 errors with tool name, type, and message

### Period Selector

All observability pages support time filtering: 1h / 24h / 7d / 30d.

### Live Tail (`/tail`)

Real-time event stream via SSE — shows events as they happen with auto-scroll.

### Tool Workbench (`/workbench`)

Interactive tool execution for debugging:
1. Select a tool from the list
2. Fill in parameters (auto-generated form from tool schema)
3. Click "Execute"
4. View raw response

> **Note:** Write tools are blocked by default. Enable specific ones via `workbench_allow`.

### SQL Playground (`/sql`)

Ad-hoc DuckDB queries against cellar data. Pre-loaded with agent views. Read-only (INSERT/UPDATE/DELETE blocked).

## Running as a Service

For always-on operation, deploy as a launchd user agent. See [launchd template](../reference/launchd-template.md).

## Next Steps

- [Observability](../operations/observability.md) — EventCollector and CLI log queries
- [Configuration](../configuration/overview.md) — Dashboard config settings
