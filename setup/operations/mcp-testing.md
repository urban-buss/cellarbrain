# Testing the MCP Server

How to verify the MCP server starts correctly, test individual tools, and run automated tests.

## Prerequisites

- Parquet data in `output/` (run ETL first)
- `pip install -e .` (editable install)

## 1. Verify the Server Starts

### stdio (quick check)

```bash
cellarbrain -vv --log-file /tmp/mcp-debug.log mcp
# Ctrl+C to stop — check /tmp/mcp-debug.log for startup messages
```

### SSE (interactive testing)

```bash
cellarbrain mcp --transport sse --port 8080
```

Opens an HTTP endpoint at `http://localhost:8080`. Useful for manual tool calls via `curl` or any HTTP client.

### With a specific dataset

```bash
cellarbrain -d /path/to/output mcp
cellarbrain -c cellarbrain.toml mcp
```

## 2. Smoke-Test via CLI Commands

These CLI commands exercise the same data layer the MCP server uses — if they work, the server will too.

```bash
# Verify Parquet data exists and is queryable
cellarbrain stats
cellarbrain stats --by country
cellarbrain query "SELECT count(*) FROM wines"

# Verify dossier access
cellarbrain dossier 1
cellarbrain dossier 1 --sections identity

# Verify MCP config generation
cellarbrain info --mcp-config

# Verify full diagnostics
cellarbrain info --paths
cellarbrain info --modules
```

## 3. Test Individual Tools via SSE

Start the server with SSE transport, then call tools over HTTP:

```bash
# Terminal 1: start server
cellarbrain mcp --transport sse --port 8080

# Terminal 2: call a tool
curl -X POST http://localhost:8080/call \
  -H "Content-Type: application/json" \
  -d '{"tool": "cellar_stats", "arguments": {}}'

curl -X POST http://localhost:8080/call \
  -H "Content-Type: application/json" \
  -d '{"tool": "find_wine", "arguments": {"query": "Barolo"}}'

curl -X POST http://localhost:8080/call \
  -H "Content-Type: application/json" \
  -d '{"tool": "read_dossier", "arguments": {"wine_id": 1}}'
```

## 4. Test with MCP Inspector

The [MCP Inspector](https://github.com/modelcontextprotocol/inspector) provides a web UI for interactive tool testing:

```bash
npx @modelcontextprotocol/inspector cellarbrain -d output mcp
```

This opens a browser UI where you can browse available tools, call them with parameters, and inspect responses.

## 5. Automated Tests (pytest)

### MCP tool unit tests

```bash
pytest tests/test_mcp_server.py -v
```

These tests create a temporary Parquet dataset, import the MCP tool functions directly, and verify responses — no running server needed.

### Run a specific test

```bash
pytest tests/test_mcp_server.py::TestQueryCellar -v
pytest tests/test_mcp_server.py -k "find_wine" -v
```

### All tests (includes MCP)

```bash
pytest tests/ -v --ignore=tests/test_integration.py
```

## 6. Check the Observability Log

After running the MCP server, query the built-in log store:

```bash
# Tail recent events
cellarbrain logs

# Show recent errors
cellarbrain logs --errors

# Usage summary (call counts + avg latency per tool)
cellarbrain logs --usage
```

The log store lives at `output/logs/cellarbrain-logs.duckdb` and is created automatically on first MCP server run.

## 7. VS Code Debugging

Use the **MCP Server (stdio)** debug configuration in VS Code:

1. Open **Run and Debug** (`Ctrl+Shift+D`)
2. Select **MCP Server (stdio)**
3. Set breakpoints in `src/cellarbrain/mcp_server.py`
4. Press `F5` — the server starts with `-vv` logging

See [VS Code Debugging](vscode-debugging.md) for the full `launch.json`.

## Quick Validation Checklist

| Step | Command | Expected |
|------|---------|----------|
| Data exists | `cellarbrain stats` | Cellar summary with counts |
| Query works | `cellarbrain query "SELECT count(*) FROM wines"` | Row count |
| Dossiers readable | `cellarbrain dossier 1` | Markdown output |
| Config correct | `cellarbrain info --mcp-config` | Valid JSON with `mcpServers` |
| Server starts | `cellarbrain -vv mcp` then Ctrl+C | No errors in output |
| Unit tests pass | `pytest tests/test_mcp_server.py -v` | All green |
| Logs captured | `cellarbrain logs` | Event rows (after server use) |
