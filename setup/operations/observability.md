# Observability

The EventCollector captures every MCP tool, resource, and prompt invocation in a DuckDB log store.

## Architecture

```
MCP Tool Call → EventCollector → Buffer (deque, 50 events) → DuckDB Log Store
                                                              ↓
                                                    output/logs/cellarbrain-logs.duckdb
```

## ToolEvent Record

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | UUID | Unique event identifier |
| `session_id` | UUID | MCP server process session |
| `turn_id` | UUID | Logical conversation turn |
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

## Session and Turn IDs

- **Session ID**: Random UUID on MCP server startup. Groups all events within one server lifetime.
- **Turn ID**: Rotates when the gap between events exceeds `turn_gap_seconds` (default: 2s). Groups related tool calls into conversation turns without client signalling.

## Slow Call Warnings

When a tool exceeds `slow_threshold_ms` (default: 2000ms):

```
WARNING cellarbrain.observability — Slow tool call: query_cellar took 3421ms
```

## DuckDB Log Store

Default location: `<data_dir>/logs/cellarbrain-logs.duckdb`

Override:
```toml
[logging]
log_db = "/path/to/custom/cellarbrain-logs.duckdb"
```

Created automatically on first MCP server run. Events are buffered (50) and batch-flushed.

## CLI Log Queries

### Tail Recent Events

```bash
cellarbrain logs
cellarbrain logs --tail 100
```

### Errors

```bash
cellarbrain logs --errors --since 24
```

### Tool Usage

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
```

### Latency Percentiles

```bash
cellarbrain logs --latency --since 24
```

Output:
```
Tool                            Avg       P50       P95       P99       Max     Calls
------------------------------------------------------------------------------------
query_cellar                  145.3     102.0     890.0    2100.0    3421.0        98
find_wine                      38.2      28.0      95.0     210.0     421.0       142
```

### Sessions

```bash
cellarbrain logs --sessions --since 24
```

### Prune Old Events

```bash
cellarbrain logs --prune
# Pruned 1247 events older than 90 days.
```

### Custom Time Window

```bash
cellarbrain logs --since 1       # last 1 hour
cellarbrain logs --since 168     # last 7 days
```

## Next Steps

- [Health Monitoring](health-monitoring.md) — Daily checks for production
- [Dashboard](../modules/dashboard.md) — Web-based observability UI
