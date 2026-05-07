---
name: manage
description: "Upgrade cellarbrain, refresh data, update currency rates, restart services, check system health."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# System Management

Maintain the cellarbrain installation: upgrades, data refresh, currency rates, and health checks.

## Workflow: Upgrade Cellarbrain

1. **Stop ingest daemon** (if running):
   - macOS: `launchctl unload ~/Library/LaunchAgents/com.cellarbrain.ingest.plist`
   - Linux: `systemctl --user stop cellarbrain-ingest`
2. **Upgrade package:**
   ```bash
   pip install --upgrade cellarbrain
   ```
3. **Verify version:**
   `cellar_info(verbose=True)` — confirm new version number
4. **Restart ingest daemon:**
   - macOS: `launchctl load ~/Library/LaunchAgents/com.cellarbrain.ingest.plist`
   - Linux: `systemctl --user start cellarbrain-ingest`
5. **Run quick health check** (see below)

## Workflow: Refresh Data

- **Incremental sync** (normal): `reload_data()` or `reload_data(mode="sync")`
- **Full rebuild**: `reload_data(mode="full")` — reprocesses all CSVs from scratch

After reload, verify with `cellar_info()` (check ETL timestamp and changeset).

## Workflow: Currency Rates

- **List rates:** `currency_rates(action="list")`
- **Set rate:** `currency_rates(action="set", currency="EUR", rate=0.93)` — 1 EUR = 0.93 CHF
- **Remove custom rate:** `currency_rates(action="remove", currency="EUR")`

Rates affect price normalisation in the next ETL run.

## Workflow: Health Check

`cellar_info(verbose=True)` — returns:
- Version and Python environment
- Last ETL run (timestamp, type, changeset summary)
- Inventory counts (wines, bottles, dossiers, tracked wines)
- Currency rates
- Cellar locations

Check for: stale ETL (>24h old), missing data directory, version mismatch.

## Tools

| Tool | Purpose |
|------|---------|
| `cellar_info` | Version, freshness, diagnostics |
| `reload_data` | Re-run ETL pipeline |
| `currency_rates` | Manage exchange rates |
