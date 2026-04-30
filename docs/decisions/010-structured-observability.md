# ADR 010 — Structured Observability

**Status:** accepted  
**Date:** 2026-04-27

## Context

The MCP server had basic stdlib logging via a `_log_tool` decorator but lacked:
- Persistent storage of tool invocations for post-hoc analysis
- Session and turn correlation IDs for grouping related calls
- Latency percentile tracking and slow-call alerts
- Agent identification across tool calls
- A CLI for querying historical usage data

## Decision

### DuckDB Log Store

Tool, resource, and prompt invocations are captured as `ToolEvent` dataclass
records and buffered in a `collections.deque`. Events are batch-flushed to a
DuckDB database at `<data_dir>/logs/cellarbrain-logs.duckdb` (configurable via
`logging.log_db`). DuckDB was chosen because it is already a dependency —
zero new packages required.

### Session and Turn IDs

Each MCP server process gets a random `session_id` (UUID4 hex). Within a
session, a `turn_id` rotates when the idle gap between events exceeds
`turn_gap_seconds` (default 2s). This groups related tool calls into
logical conversation turns without requiring explicit client signalling.

### `meta` Parameter

All tool functions accept an optional `meta: dict | None = None` parameter.
Callers can pass `{"agent_name": "...", "trace_id": "...", "turn_id": "..."}`
to enrich events. This is a plain dict (not a typed dataclass) to keep the
JSON schema simple and avoid breaking MCP tool schema generation.

### Buffered Writes

Events are buffered in a deque and flushed to DuckDB when either:
- The buffer reaches 50 events, or
- The collector is closed (atexit hook)

This avoids per-event write overhead while ensuring data is persisted.

### JSON Log Formatter

A new `JsonFormatter` class emits log records as single-line JSON when
`logging.format = "json"`. Embeds `session_id`, `turn_id`, and other
observability fields into structured log lines.

## Consequences

- Every tool/resource/prompt invocation is captured with timing, status, and
  correlation IDs
- `cellarbrain logs` CLI provides --errors, --usage, --latency, --sessions views
- `server_stats` MCP tool exposes usage data to agents
- Auto-pruning via `retention_days` prevents unbounded log growth
- The `_meta` parameter is backward-compatible — existing callers that omit it
  are unaffected
