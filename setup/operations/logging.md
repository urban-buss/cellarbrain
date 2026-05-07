# Logging

Configure text and JSON logging for all Cellarbrain components.

## TOML Configuration

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

## CLI Flag Overrides

| Flag | Effect | Use Case |
|------|--------|----------|
| `-v` | Set level to INFO | See operational messages |
| `-vv` | Set level to DEBUG | Full diagnostic output |
| `-q` | Set level to ERROR | Suppress warnings |
| `--log-file PATH` | Write logs to file | Persist logs for later analysis |

Examples:

```bash
cellarbrain -vv etl raw/*.csv -o output        # Verbose ETL
cellarbrain -q validate                          # Quiet mode (errors only)
cellarbrain --log-file /tmp/debug.log mcp        # Log to file
```

## MCP Server Logging

When running `cellarbrain mcp`:
- **stderr** is locked to WARNING (protects JSON-RPC transport on stdout)
- Use `log_file` for debug output
- JSON format recommended for structured parsing

```toml
[logging]
level = "DEBUG"
log_file = "output/logs/mcp-debug.log"
format = "json"
```

## JSON Structured Logging

### Enable

```toml
[logging]
format = "json"
log_file = "output/logs/cellarbrain.log"
```

### Format

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

### Parsing JSON Logs

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
grep '"session_id":"abc123"' output/logs/cellarbrain.log
```

## Next Steps

- [Observability](observability.md) — EventCollector and DuckDB log store
- [Configuration](../configuration/overview.md) — Full `[logging]` settings
