---
name: manage
description: "Maintain CellarBrain: inspect or restart the ingest service, refresh data, upgrade the package, run migrations, manage currency rates, and check system health."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# System Management

Maintain the CellarBrain installation. Ask for confirmation before upgrades, full rebuilds, migrations, service changes, or other state-changing commands.

## Ingest Service

Use the supported CLI service commands; `ingest_status`, `ingest_start`, and `ingest_stop` are not MCP tools.

- Check status: `cellarbrain service status`
- Restart or repair the LaunchAgent: `cellarbrain service install --force`
- View recent logs: `cellarbrain service logs`
- View error logs: `cellarbrain service logs --stderr`
- Stop and remove the service only when explicitly requested: `cellarbrain service uninstall`

After starting or restarting, run `cellarbrain service status` and report the loaded/running state and PID.

## Upgrade CellarBrain

1. Upgrade in the same Python environment used by the `cellarbrain` entry point.
2. Run `cellarbrain info` and confirm the version and resolved paths.
3. Run the health check below.
4. Restart the ingest service with `cellarbrain service install --force` if it did not recycle automatically.

Do not guess the installation environment. Use `cellarbrain info` to identify it first.

## Refresh Data

- Incremental ETL: `reload_data(mode="sync")`
- Full rebuild, only when explicitly requested: `reload_data(mode="full")`
- Verify freshness afterward: `cellar_info()`

## Schema Migrations

- Check: `cellarbrain migrate --status`
- Preview: `cellarbrain migrate --dry-run`
- Apply, only with confirmation: `cellarbrain migrate`

Migrations normally run during ETL. Use a manual migration only when an upgrade requires it before the next ETL run.

## Currency Rates

- List: `currency_rates(action="list")`
- Set: `currency_rates(action="set", currency="EUR", rate=0.93)`
- Remove: `currency_rates(action="remove", currency="EUR")`

Run ETL after changing a rate so normalized prices are recalculated.

## Health Check

1. `cellar_info(verbose=true, format="plain")` — version, paths, ETL freshness, inventory counts
2. `cellar_anomalies(severity="critical", format="plain")` — operational anomalies
3. `server_stats(period="24h", format="plain")` — MCP errors and latency
4. `cache_stats(format="plain")` — cache usage
5. `search_stats(window_days=7, format="plain")` — failed and rescued searches

Report stale ETL, failed service state, MCP errors, critical anomalies, or path/version mismatches. Do not describe `cellar_anomalies` as a wine-data completeness check.

## Tools

| Tool | Purpose |
|------|---------|
| `cellar_info` | Version, freshness, paths, diagnostics |
| `reload_data` | Run incremental or full ETL |
| `currency_rates` | Manage exchange rates |
| `cellar_anomalies` | Detect operational anomalies |
| `server_stats` | MCP usage, latency, and errors |
| `cache_stats` | Query-cache health |
| `search_stats` | Search behavior and failures |

## Output Format

Pass `format="plain"` only when the tool schema exposes a `format` parameter. Never add unsupported arguments. Present the final answer as plain text for iMessage.
