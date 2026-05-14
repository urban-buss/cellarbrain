---
name: manage
description: "Upgrade cellarbrain, refresh data, migrate schemas, check anomalies, monitor system health."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# System Management

Maintain the cellarbrain installation: upgrades, data refresh, migrations, anomaly detection, and health checks.

## Workflow: Daemon Control

### Check Status
`ingest_status()` — returns running/stopped, PID, last poll time, recent errors.

### Start Daemon
`ingest_start()` — spawns daemon process if not running. Errors if already running.

### Restart Daemon (kill + restart)
`ingest_stop(restart=True)` — gracefully stops the running daemon, then starts fresh.

### Stop Daemon
`ingest_stop()` — sends termination signal for clean shutdown.

### Troubleshooting
If `ingest_status()` shows STOPPED but user expects it running:
1. Check `cellar_info(verbose=True)` for environment issues
2. Start daemon: `ingest_start()`
3. Wait ~60s, then check `ingest_status()` again
4. If still failing, report the error details to the user

## Workflow: Daemon Control

### Check Status
`ingest_status()` — returns running/stopped, PID, last poll time, recent errors.

### Start Daemon
`ingest_start()` — spawns daemon process if not running. Errors if already running.

### Restart Daemon (kill + restart)
`ingest_stop(restart=True)` — gracefully stops the running daemon, then starts fresh.

### Stop Daemon
`ingest_stop()` — sends termination signal for clean shutdown.

### Troubleshooting
If `ingest_status()` shows STOPPED but user expects it running:
1. Check `cellar_info(verbose=True)` for environment issues
2. Start daemon: `ingest_start()`
3. Wait ~60s, then check `ingest_status()` again
4. If still failing, report the error details to the user

## Workflow: Upgrade Cellarbrain

1. **Upgrade package:**

   ```bash
   pip install --upgrade cellarbrain
   ```
2. **Verify version:**
   `cellar_info(verbose=True)` — confirm new version number
3. **Run quick health check** (see below)

> **Note:** The ingest daemon auto-detects version changes and restarts itself
> within ~90 seconds. No manual stop/start is needed. If immediate restart is
> required, use `ingest_stop(restart=True)`.


## Workflow: Refresh Data

- Incremental: `reload_data()`
- Full rebuild: `reload_data(mode="full")`
- After reload: `cellar_info()` to verify ETL timestamp

## Workflow: Schema Migrations

- Check status: `cellarbrain migrate --status` (CLI) -- shows current version + pending
- Apply: `cellarbrain migrate` -- forward-only, creates backup first
- Preview: `cellarbrain migrate --dry-run`

Migrations run automatically during ETL. Manual run needed only after package upgrade without ETL.

## Workflow: Currency Rates

- List: `currency_rates(action="list")`
- Set: `currency_rates(action="set", currency="EUR", rate=0.93)`
- Remove: `currency_rates(action="remove", currency="EUR")`

## Workflow: Health Check

1. `cellar_info(verbose=True)` -- version, ETL freshness, inventory counts
2. `cellar_anomalies(severity="critical")` -- flag critical operational anomalies
3. `cache_stats()` -- cache hit ratio, evictions (healthy: ratio > 0.5)
4. `search_stats(window_days=7)` -- zero-result queries (vocabulary gaps)

Check for: stale ETL (>24h), pending migrations, high error rates, cache thrashing.

## Workflow: Sidecar Maintenance

`cellarbrain dashboard-prune` -- removes stale consumed-pending and drink-tonight entries.

## Tools

| Tool | Purpose |
|------|---------|

| `ingest_status` | Check daemon running/stopped, PID, last poll |
| `ingest_start` | Start daemon if not running |
| `ingest_stop` | Stop daemon (optionally restart) |
| `cellar_info` | Version, freshness, diagnostics |
| `reload_data` | Re-run ETL pipeline |
| `currency_rates` | Manage exchange rates |

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.

