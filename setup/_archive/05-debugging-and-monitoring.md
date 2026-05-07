# Debugging & Monitoring

How to debug issues, monitor health, and observe the cellarbrain system on macOS.

---

## 1. Logging Configuration

### 1.1 TOML Configuration

In `cellarbrain.toml` or `cellarbrain.local.toml`:

```toml
[logging]
level = "WARNING"                                          # Root log level
log_file = "output/logs/cellarbrain.log"                   # Rotating log file (null = disabled)
max_bytes = 5242880                                        # 5 MB before rotation
backup_count = 3                                           # Keep 3 rotated files
format = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"  # Text format
date_format = "%Y-%m-%d %H:%M:%S"                         # Timestamp format
turn_gap_seconds = 2.0                                     # MCP turn boundary detection
slow_threshold_ms = 2000.0                                 # Warn on slow tool calls
log_db = null                                              # DuckDB log store path (auto-derived)
retention_days = 90                                        # Prune events older than this
```

### 1.2 CLI Flag Overrides

| Flag | Effect | Use Case |
|------|--------|----------|
| `-v` | Set level to INFO | See operational messages |
| `-vv` | Set level to DEBUG | Full diagnostic output |
| `-q` | Set level to ERROR | Suppress warnings |
| `--log-file PATH` | Write logs to file | Persist logs for later analysis |

Examples:

```bash
# Verbose ETL
cellarbrain -vv etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# Quiet mode (errors only)
cellarbrain -q validate

# Log to file
cellarbrain --log-file /tmp/debug.log mcp
```

### 1.3 MCP Server Logging

When running `cellarbrain mcp`:
- **stderr** is locked to WARNING (to protect JSON-RPC transport on stdout)
- Use `log_file` for debug output
- JSON format is recommended for structured parsing

```toml
[logging]
level = "DEBUG"
log_file = "output/logs/mcp-debug.log"
format = "json"
```

---

## 2. JSON Structured Logging

### 2.1 Enable JSON Format

```toml
[logging]
format = "json"
log_file = "output/logs/cellarbrain.log"
```

### 2.2 JSON Log Format

Each line is a single JSON object:

```json
{
  "ts": "2026-04-30T14:23:01",
  "level": "INFO",
  "logger": "cellarbrain.mcp_server",
  "message": "Tool invoked: find_wine",
  "session_id": "a1b2c3d4e5f6...",
  "turn_id": "f6e5d4c3b2a1...",
  "event_type": "tool",
  "tool_name": "find_wine",
  "duration_ms": 42.5
}
```

### 2.3 Parsing JSON Logs

```bash
# View recent errors
cat output/logs/cellarbrain.log | python3 -c "
import sys, json
for line in sys.stdin:
    evt = json.loads(line)
    if evt['level'] == 'ERROR':
        print(f\"{evt['ts']} {evt['logger']}: {evt['message']}\")
"

# Count events per tool
cat output/logs/cellarbrain.log | python3 -c "
import sys, json
from collections import Counter
tools = Counter()
for line in sys.stdin:
    evt = json.loads(line)
    if 'tool_name' in evt:
        tools[evt['tool_name']] += 1
for tool, count in tools.most_common():
    print(f'{count:>5}  {tool}')
"

# Filter by session
cat output/logs/cellarbrain.log | grep '"session_id":"abc123"'
```

---

## 3. Observability System

### 3.1 Architecture

The observability system captures every MCP tool, resource, and prompt invocation:

```
MCP Tool Call → EventCollector → Buffer (deque, 50 events) → DuckDB Log Store
                                                              ↓
                                                    output/logs/cellarbrain-logs.duckdb
```

### 3.2 ToolEvent Record

Each invocation is captured as a `ToolEvent`:

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | UUID | Unique event identifier |
| `session_id` | UUID | MCP server process session |
| `turn_id` | UUID | Logical conversation turn (rotates on idle gap) |
| `event_type` | str | `"tool"`, `"resource"`, or `"prompt"` |
| `name` | str | Tool/resource/prompt name |
| `started_at` | datetime | Invocation start |
| `ended_at` | datetime | Invocation end |
| `duration_ms` | float | Execution time in milliseconds |
| `status` | str | `"ok"` or `"error"` |
| `request_id` | str | MCP request ID |
| `parameters` | JSON str | Tool parameters (serialised) |
| `error_type` | str | Exception class name (on error) |
| `error_message` | str | Error message (on error) |
| `result_size` | int | Response size in characters |
| `agent_name` | str | Agent that made the call (from `meta`) |
| `trace_id` | str | Distributed trace ID (from `meta`) |
| `client_id` | str | Client identifier (from `meta`) |

### 3.3 Session and Turn IDs

- **Session ID**: Random UUID generated when the MCP server process starts. Groups all events within one server lifetime.
- **Turn ID**: Rotates automatically when the gap between events exceeds `turn_gap_seconds` (default: 2s). Groups related tool calls into logical conversation turns without client signalling.

### 3.4 Slow Call Warnings

When a tool invocation exceeds `slow_threshold_ms` (default: 2000ms), a WARNING is logged:

```
2026-04-30 14:23:05 WARNING  cellarbrain.observability — Slow tool call: query_cellar took 3421ms
```

### 3.5 DuckDB Log Store Location

Default: `<data_dir>/logs/cellarbrain-logs.duckdb`

Override in config:
```toml
[logging]
log_db = "/path/to/custom/cellarbrain-logs.duckdb"
```

The store is created automatically on first MCP server run. Events are buffered (50 events) and batch-flushed.

---

## 4. CLI Log Queries

The `cellarbrain logs` command provides read-only access to the DuckDB log store.

### 4.1 Tail Recent Events

```bash
cellarbrain logs
```

Output:
```
Timestamp              Type       Name                      Status    ms     Agent
----------------------------------------------------------------------------------------
2026-04-30 14:23:01    tool       find_wine                 ok          42
2026-04-30 14:23:02    tool       read_dossier              ok          18
2026-04-30 14:23:03    tool       query_cellar              ok         156
```

### 4.2 Errors

```bash
cellarbrain logs --errors --since 24
```

Output:
```
Timestamp              Tool                      Error                Message                                  ms
-------------------------------------------------------------------------------------------------------------------
2026-04-30 12:15:03    query_cellar              QueryError           Invalid column: 'nonexistent'             5
2026-04-30 11:02:44    update_dossier            ProtectedSection...  Cannot overwrite ETL section 'identity'   2
```

### 4.3 Tool Usage

```bash
cellarbrain logs --usage --since 24
```

Output:
```
Tool                            Calls    Avg ms     Max ms    Errors
------------------------------------------------------------------
find_wine                         142      38.2      421.0         0
query_cellar                       98     145.3     3421.0         3
read_dossier                       87      15.8       89.0         1
cellar_stats                       34      22.1       45.0         0
```

### 4.4 Latency Percentiles

```bash
cellarbrain logs --latency --since 24
```

Output:
```
Tool                            Avg       P50       P95       P99       Max     Calls
------------------------------------------------------------------------------------
query_cellar                  145.3     102.0     890.0    2100.0    3421.0        98
find_wine                      38.2      28.0      95.0     210.0     421.0       142
suggest_wines                 234.5     220.0     350.0     480.0     520.0        12
```

### 4.5 Sessions

```bash
cellarbrain logs --sessions --since 24
```

Output:
```
Session                            First Event             Events     Turns    Errors
------------------------------------------------------------------------------------
a1b2c3d4e5f6789012345678...        2026-04-30 09:15:22        245        18         3
b2c3d4e5f6789012345678ab...        2026-04-30 14:02:11         34         5         0
```

### 4.6 Prune Old Events

```bash
cellarbrain logs --prune
# Pruned 1247 events older than 90 days.
```

### 4.7 Custom Time Window

```bash
# Last 1 hour
cellarbrain logs --since 1

# Last 7 days
cellarbrain logs --since 168

# More results
cellarbrain logs --tail 100
```

---

## 5. Web Dashboard Monitoring

### 5.1 Start the Dashboard

```bash
cellarbrain dashboard
# → http://localhost:8017
```

### 5.2 Observability Overview (`/`)

- **KPI Cards**: Total calls, error rate %, average latency, active sessions
- **Hourly Volume Chart**: Bar chart showing call distribution over time
- **Top Tools**: Most-called tools with call count and avg latency
- **Recent Errors**: Last 5 errors with tool name, type, and message

### 5.3 Period Selector

All observability pages support time filtering:
- 1h / 24h / 7d / 30d (via tab selector in the page header)

### 5.4 Live Tail (`/tail`)

Real-time event stream via Server-Sent Events (SSE):
- Shows events as they happen
- Auto-scrolls to latest
- Includes tool name, status, duration, agent

### 5.5 Tool Workbench (`/workbench`)

Interactive tool execution for debugging:
1. Select a tool from the list
2. Fill in parameters (auto-generated form from tool schema)
3. Click "Execute"
4. View raw response

> **Note:** Write tools are blocked by default. Enable specific ones in config:
> ```toml
> [dashboard]
> workbench_allow = ["log_price"]
> ```

### 5.6 SQL Playground (`/sql`)

Run ad-hoc DuckDB queries against the cellar data:
- Pre-loaded with agent views (wines, bottles_stored, etc.)
- Results displayed as HTML tables
- Read-only (INSERT/UPDATE/DELETE blocked)

---

## 6. Debugging MCP Connections

### 6.1 Verify Data Exists

```bash
# Check that Parquet files exist
ls output/*.parquet

# Expected: wine.parquet, bottle.parquet, winery.parquet, etc. (12 files)
```

### 6.2 Test CLI Access

```bash
# This uses the same query layer as the MCP server
cellarbrain stats
cellarbrain query "SELECT count(*) FROM wines"
```

### 6.3 Check MCP Server Starts

```bash
# Start in verbose mode (writes to log file since stderr is locked)
cellarbrain -vv --log-file /tmp/mcp-debug.log mcp &
MCP_PID=$!

# Check it's running
ps aux | grep cellarbrain

# Stop it
kill $MCP_PID

# Check the log
cat /tmp/mcp-debug.log
```

### 6.4 Diagnose Connection Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "No such file or directory" | Wrong path to cellarbrain binary | Use full path: `.venv/bin/cellarbrain` |
| "ModuleNotFoundError" | Package not installed in the venv being used | Check which venv the MCP config points to |
| "DataStaleError" | No Parquet files found | Run `cellarbrain etl` first |
| "Connection refused" (SSE) | Server not running or wrong port | Check `--port` matches client config |
| Tools return empty results | Data directory mismatch | Verify `-d` flag or `CELLARBRAIN_DATA_DIR` |

### 6.5 MCP Diagnostics via Tool

If the MCP server is connected but behaving unexpectedly, call `cellar_info(verbose=True)`:

```
# Response includes:
# - Version
# - Data directory path
# - Last ETL timestamp
# - Table row counts
# - Config summary
# - Sommelier model status
```

---

## 7. Debugging ETL Issues

### 7.1 Verbose ETL

```bash
cellarbrain -vv etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

DEBUG output shows:
- CSV parsing decisions (field mapping, type coercion)
- Entity deduplication (how IDs are assigned)
- Change detection in sync mode (what changed)
- Parquet write operations

### 7.2 Common ETL Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `UnicodeDecodeError` | CSV encoding mismatch | Check `[csv] encoding` — default is `utf-16` |
| `FileNotFoundError` | Wrong file paths | Verify CSV files exist at specified paths |
| `ValueError: duplicate primary key` | Corrupt incremental state | Delete `output/` and do a fresh full load |
| `Schema mismatch` | Code change added columns | Run a fresh full load (without `--sync`) |

### 7.3 Validate After ETL

```bash
cellarbrain validate
```

Reports:
- PK uniqueness violations
- FK referential integrity failures
- Domain constraint violations
- Missing dossier files

### 7.4 Inspect Parquet Directly

```bash
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('output/wine.parquet')
print(f'Schema: {table.schema}')
print(f'Rows: {table.num_rows}')
print(table.to_pandas().head())
"
```

---

## 8. Debugging Sommelier Issues

### 8.1 Check Model Status

```bash
python3 -c "
from cellarbrain.sommelier.model import load_model
model = load_model('models/sommelier/model')
print(f'Model loaded: {type(model).__name__}')
print(f'Max seq length: {model.max_seq_length}')
"
```

### 8.2 Check Index Status

```bash
python3 -c "
import faiss, json
idx = faiss.read_index('models/sommelier/food.index')
print(f'Food index: {idx.ntotal} vectors, dimension {idx.d}')
with open('models/sommelier/food_ids.json') as f:
    ids = json.load(f)
print(f'Food IDs: {len(ids)} entries')
"
```

### 8.3 Common Sommelier Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: sentence_transformers` | Extra not installed | `pip install "cellarbrain[sommelier]"` |
| `FileNotFoundError: model/` | Model not trained | Run `cellarbrain train-model` |
| `FileNotFoundError: food.index` | Indexes not built | Run `cellarbrain rebuild-indexes` |
| Empty results from `suggest_wines` | Wine index stale | Run `cellarbrain rebuild-indexes --wine-only` |

---

## 9. VS Code Debugging

### 9.1 Debug CLI Commands

Create `.vscode/launch.json`:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "ETL (Full Load)",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["-vv", "etl", "raw/export-wines.csv", "raw/export-bottles-stored.csv", "raw/export-bottles-gone.csv", "-o", "output"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "ETL (Sync)",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["-vv", "etl", "raw/export-wines.csv", "raw/export-bottles-stored.csv", "raw/export-bottles-gone.csv", "-o", "output", "--sync"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "Validate",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["validate"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "Query",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["query", "SELECT count(*) FROM wines"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "MCP Server (stdio)",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["-vv", "--log-file", "output/logs/mcp-debug.log", "mcp"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "Dashboard",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["dashboard", "--port", "8017"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "Train Model",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["train-model", "--epochs", "2"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "Ingest (Single Cycle)",
      "type": "debugpy",
      "request": "launch",
      "module": "cellarbrain",
      "args": ["ingest", "--once", "--dry-run"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    },
    {
      "name": "pytest (Current File)",
      "type": "debugpy",
      "request": "launch",
      "module": "pytest",
      "args": ["${file}", "-v", "--tb=short"],
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal"
    }
  ]
}
```

### 9.2 Using Debug Configurations

1. Open the **Run and Debug** panel (`Cmd+Shift+D`)
2. Select a configuration from the dropdown
3. Set breakpoints in source files (click the gutter)
4. Press `F5` to start debugging
5. Use the debug toolbar: Continue, Step Over, Step Into, Step Out

### 9.3 Debug Tests

1. Open a test file (e.g., `tests/test_query.py`)
2. Click the "Debug Test" icon above any test function
3. Set breakpoints in both test code and source code
4. The debugger stops at breakpoints in both

### 9.4 Useful Debug Expressions

In the VS Code debug console:

```python
# Inspect settings
settings.__dict__

# Check Parquet table contents
import pyarrow.parquet as pq
pq.read_table('output/wine.parquet').num_rows

# Check DuckDB connection
con.execute("SELECT count(*) FROM wines").fetchone()

# Inspect a dossier path
from cellarbrain.dossier_ops import resolve_dossier_path
resolve_dossier_path(42, 'output')
```

---

## 10. Health Monitoring Checklist

Daily health check for a production Mac Mini setup:

```bash
# 1. Check services are running
launchctl list | grep com.cellarbrain

# 2. Check for recent errors
cellarbrain logs --errors --since 24

# 3. Verify data freshness (last ETL timestamp)
cellarbrain query "SELECT max(etl_timestamp) FROM wines" 2>/dev/null || echo "Query failed"

# 4. Check log store size
ls -lh output/logs/cellarbrain-logs.duckdb

# 5. Check disk usage
du -sh output/

# 6. Verify dashboard is responsive
curl -s -o /dev/null -w "%{http_code}" http://localhost:8017/

# 7. Check ingest daemon logs for errors
tail -5 output/logs/ingest-stderr.log
```

### Automated Health Check Script

Save as `~/bin/cellarbrain-health`:

```bash
#!/bin/bash
set -euo pipefail

echo "=== Cellarbrain Health Check ==="
echo "Date: $(date)"
echo ""

# Services
echo "--- Services ---"
launchctl list 2>/dev/null | grep com.cellarbrain || echo "No launchd services found"
echo ""

# Recent errors
echo "--- Errors (last 24h) ---"
cellarbrain logs --errors --since 24 2>/dev/null || echo "Log store not available"
echo ""

# Data freshness
echo "--- Data Freshness ---"
cellarbrain query "SELECT max(etl_timestamp) as last_etl FROM wines" 2>/dev/null || echo "Cannot query"
echo ""

# Disk
echo "--- Disk Usage ---"
du -sh output/ 2>/dev/null || echo "No output directory"
echo ""

echo "=== Done ==="
```

```bash
chmod +x ~/bin/cellarbrain-health
cellarbrain-health
```

---

## Next Steps

- [OpenClaw Integration](06-openclaw-integration.md) — Connect AI agents
- [Installation & Running](04-installation-and-running.md) — Run modules step-by-step
