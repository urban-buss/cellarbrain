# VS Code Debugging

Debug configurations for stepping through Cellarbrain CLI commands, tests, and the MCP server.

## launch.json

Add to `.vscode/launch.json`:

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

## Using Debug Configurations

1. Open the **Run and Debug** panel (`Cmd+Shift+D` / `Ctrl+Shift+D`)
2. Select a configuration from the dropdown
3. Set breakpoints in source files (click the gutter)
4. Press `F5` to start debugging
5. Use the debug toolbar: Continue, Step Over, Step Into, Step Out

## Debugging Tests

1. Open a test file (e.g., `tests/test_query.py`)
2. Click the "Debug Test" icon above any test function
3. Set breakpoints in both test code and source code
4. The debugger stops at breakpoints in both

## Useful Debug Expressions

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

## Next Steps

- [Testing](../development/testing.md) — Run tests from CLI
- [Logging](logging.md) — Configure log levels for debugging
