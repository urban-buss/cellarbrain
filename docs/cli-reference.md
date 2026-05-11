# CLI Reference

Cellarbrain uses a subcommand-based CLI. If the first positional argument ends in `.csv`, a legacy flat interface is used for backward compatibility.

---

## Global Options

| Flag | Default | Description |
|---|---|---|
| `-c`, `--config` | `cellarbrain.toml` or `CELLARBRAIN_CONFIG` env var | Path to configuration file |
| `-d`, `--data-dir` | `output` (or `paths.data_dir` from config) | Path to Parquet + dossier output directory |

---

## `cellarbrain etl`

Run the ETL pipeline to ingest Vinocell CSV exports into Parquet files and Markdown dossiers.

```bash
# Full load (rebuilds all IDs)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# Incremental sync (preserves IDs, detects changes)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
```

| Arg / Flag | Required | Description |
|---|---|---|
| `wines_csv` | Yes | Path to `export-wines.csv` |
| `bottles_csv` | Yes | Path to `export-bottles-stored.csv` |
| `bottles_gone_csv` | Yes | Path to `export-bottles-gone.csv` |
| `-o`, `--output` | No | Output directory (default from config) |
| `--sync` | No | Incremental sync mode (default: full load). Wine IDs are stable in both modes via slug-based pre-matching; `--sync` affects non-wine entity stabilisation and change detection. |

---

## `cellarbrain validate`

Validate Parquet output for schema conformance and referential integrity.

```bash
cellarbrain validate
```

---

## `cellarbrain query`

Run read-only SQL against the DuckDB query layer. Available views: `wines`, `bottles`, `wines_stored`, `bottles_stored`, `bottles_consumed`, `wines_drinking_now`. See [query-layer.md](query-layer.md).

```bash
# Inline SQL
cellarbrain query "SELECT count(*) FROM bottles_stored"

# From file
cellarbrain query -f queries/cellar-summary.sql

# Output formats
cellarbrain query "SELECT ..." --format table    # Default: Markdown table
cellarbrain query "SELECT ..." --format csv
cellarbrain query "SELECT ..." --format json
```

| Arg / Flag | Description |
|---|---|
| `sql` | SQL query (positional, optional if `-f` is used) |
| `-f`, `--file` | Read SQL from file |
| `--format` | Output format: `table` (default), `csv`, `json` |

---

## `cellarbrain stats`

Pre-computed cellar statistics.

```bash
# Overall summary
cellarbrain stats

# Grouped by dimension
cellarbrain stats --by country
cellarbrain stats --by region
cellarbrain stats --by category
cellarbrain stats --by vintage
cellarbrain stats --by winery
cellarbrain stats --by grape
cellarbrain stats --by cellar
cellarbrain stats --by provider
cellarbrain stats --by status
cellarbrain stats --by on_order
```

| Flag | Description |
|---|---|
| `--by` | Group by dimension. One of: `country`, `region`, `category`, `vintage`, `winery`, `grape`, `cellar`, `provider`, `status`, `on_order` |

---

## `cellarbrain dossier`

Wine dossier management — view, search, and check research status.

```bash
# View a specific dossier
cellarbrain dossier 25

# View filtered sections
cellarbrain dossier 25 --sections identity drinking_window

# Search by name/winery/region
cellarbrain dossier --search "barolo"

# List wines with pending agent sections
cellarbrain dossier --pending
cellarbrain dossier --pending --limit 10
```

| Arg / Flag | Description |
|---|---|
| `wine_id` | Wine ID (positional, optional) |
| `--search` | Text search across wine attributes |
| `--pending` | List wines with unfilled agent sections |
| `--limit` | Max results for `--pending` and `--search` (default: 20) |
| `--sections` | Section keys to include (space-separated). See [dossier-format.md](dossier-format.md) for keys. |

---

## `cellarbrain recalc`

Recompute calculated fields (`drinking_status`, `age_years`, `price_tier`, `full_name`, etc.) from existing Parquet data without re-running the full ETL. Useful after changing the current year or price tier configuration.

```bash
cellarbrain recalc
cellarbrain recalc -o output
```

| Flag | Description |
|---|---|
| `-o`, `--output` | Output directory (default from config) |

---

## `cellarbrain wishlist`

Wishlist and price tracking management.

```bash
# Show price alerts (drops, new listings, back in stock)
cellarbrain wishlist alerts
cellarbrain wishlist alerts --days 30

# Tracked wine statistics
cellarbrain wishlist stats

# Price scanning info (agent-driven)
cellarbrain wishlist scan
```

| Subcommand | Description |
|---|---|
| `alerts` | Show prioritised wishlist alerts. `--days` controls the alert window. |
| `stats` | Tracked wine statistics summary |
| `scan` | Price scanning information |

---

## `cellarbrain mcp`

Start the MCP server for agent access. See [mcp-tools.md](mcp-tools.md) for tool details and [agent-architecture.md](agent-architecture.md) for the agent integration design.

```bash
# stdio transport (default — for Claude Desktop, VS Code Copilot)
cellarbrain mcp

# SSE transport (for remote access)
cellarbrain mcp --transport sse --port 8080
```

| Flag | Description |
|---|---|
| `--transport` | Transport protocol: `stdio` (default) or `sse` |
| `--port` | Port for SSE transport (default: 8080) |

---

## `cellarbrain logs`

Query the MCP observability log store (DuckDB). The log store is created automatically when the MCP server starts.

```bash
# Tail recent events (default)
cellarbrain logs

# Show recent errors
cellarbrain logs --errors

# Usage summary (call counts + avg latency per tool)
cellarbrain logs --usage

# Latency percentiles (p50, p95, p99)
cellarbrain logs --latency

# Session overview
cellarbrain logs --sessions

# Look back 48 hours, show 50 events
cellarbrain logs --since 48 --tail 50

# Prune old events per retention_days setting
cellarbrain logs --prune
```

| Flag | Default | Description |
|---|---|---|
| `--errors` | — | Show recent error events |
| `--usage` | — | Show per-tool call count and latency summary |
| `--latency` | — | Show latency percentiles (p50/p95/p99) per tool |
| `--sessions` | — | Show session summary (events, turns, errors) |
| `--tail` | `20` | Number of recent events to display |
| `--since` | `24` | Hours to look back |
| `--prune` | — | Delete events older than `retention_days` setting |

---

## `cellarbrain dashboard`

Start the Web Explorer dashboard — a local Starlette web app for browsing observability data, cellar contents, and running interactive queries.

```bash
# Start on default port (8017)
cellarbrain dashboard

# Custom port
cellarbrain dashboard --port 9000
```

| Flag | Default | Description |
|---|---|---|
| `--port` | `8017` (or `dashboard.port` from config) | HTTP port to listen on |

### Prerequisites

- A DuckDB log store must exist (created automatically on first MCP server run via `cellarbrain mcp`)
- For cellar pages: Parquet output must exist (`cellarbrain etl` must have been run)

### Pages

| Path | Description |
|------|-------------|
| `/` | Observability overview — KPIs, hourly volume chart, top tools |
| `/tools` | Per-tool usage table with call count, avg/p95 latency, error rate |
| `/errors` | Filterable error log with detail drill-down |
| `/sessions` | Session list → turn list → turn event detail |
| `/latency` | Latency percentiles, histogram, and time-series chart |
| `/live` | Live event tail via Server-Sent Events (SSE) |
| `/cellar` | Paginated wine browser with search, category, region, status filters |
| `/bottles` | Bottle inventory with stored/consumed/on-order tabs |
| `/drinking` | Wines currently in their optimal drinking window |
| `/tracked` | Tracked wines and price history |
| `/sql` | SQL Playground — run read-only queries against cellar views |
| `/stats` | Cellar statistics with group-by selector and chart |
| `/workbench` | Interactive MCP tool runner (read-only tools by default) |
| `/dossier/{wine_id}` | Rendered wine dossier (Markdown → HTML) |

---

## Legacy Interface

For backward compatibility, if the first argument ends in `.csv`, the legacy flat interface is used:

```bash
cellarbrain raw/export-wines.csv raw/export-bottles-stored.csv -o output [--sync]
```

This is equivalent to `cellarbrain etl ...` and will be removed in a future version.

---

## `cellarbrain install-skills`

Install bundled OpenClaw skill files to a target directory.

```bash
# Default: installs to ~/.openclaw/skills/cellarbrain/
cellarbrain install-skills

# Custom target directory
cellarbrain install-skills -t /path/to/skills/dir

# Force overwrite existing files (useful for upgrades)
cellarbrain install-skills --force
```

| Arg / Flag | Required | Default | Description |
|---|---|---|---|
| `-t`, `--target` | No | `~/.openclaw/skills/cellarbrain/` | Target directory for skill files |
| `--force` | No | `false` | Overwrite existing skill files |

The command copies 8 skill files (SKILL.md) and a README.md from the bundled package data. Without `--force`, files that already exist are skipped.

---

## `cellarbrain train-model`

Fine-tune the sommelier embedding model on the pairing dataset.

```bash
cellarbrain train-model [--epochs N] [--batch-size N] [--output DIR]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--epochs` | `10` (from TOML) | Number of training epochs |
| `--batch-size` | `32` (from TOML) | Training batch size |
| `--output` | `models/sommelier/model` | Output directory for model artefacts |

Trains `all-MiniLM-L6-v2` with `CosineSimilarityLoss` on the pairing dataset. Takes ~3-5 minutes on CPU. Prints eval metrics on completion. Model artefacts are **not committed to Git** — run this after cloning.

---

## `cellarbrain retrain-model`

Incrementally retrain the sommelier model with new data.

```bash
cellarbrain retrain-model
```

**Status:** Stub — returns "not implemented" pending Phase 6.

---

## `cellarbrain rebuild-indexes`

Rebuild FAISS indexes from the trained model embeddings.

```bash
cellarbrain rebuild-indexes [--food-only] [--wine-only]
```

| Flag | Description |
|------|-------------|
| `--food-only` | Only rebuild the food index |
| `--wine-only` | Only rebuild the wine index |

Loads the trained model, encodes all food catalogue entries and/or wines with `bottles_stored > 0`, and saves FAISS `IndexFlatIP` indexes. The wine index is also auto-rebuilt after each ETL run when `sommelier.enabled = true`.

---

## `cellarbrain ingest`

Start the IMAP email ingestion daemon that monitors a mailbox for Vinocell CSV export emails, groups them into complete batches, writes snapshot folders, flushes the `raw/` working set, and triggers the ETL pipeline.

```bash
# Start polling daemon (foreground, INFO logging visible by default)
cellarbrain ingest

# Start with suppressed output (only warnings/errors shown)
cellarbrain -q ingest

# Single poll cycle, then exit (for cron/testing)
cellarbrain ingest --once

# Detect batches but don't write files or run ETL
cellarbrain ingest --dry-run

# Combined: single dry-run cycle
cellarbrain ingest --once --dry-run

# Interactive credential setup (stores in system keyring)
cellarbrain ingest --setup

# One-shot cleanup of orphan/duplicate messages
cellarbrain ingest --reap-orphans

# Dry-run orphan cleanup (shows what would be reaped, no IMAP changes)
cellarbrain ingest --reap-orphans --dry-run
```

| Flag | Description |
|------|-------------|
| `--once` | Run a single poll cycle and exit |
| `--dry-run` | Detect batches but don't write files or invoke ETL |
| `--foreground`, `-f` | *(deprecated)* No-op — INFO logging is now the default for daemon mode |
| `--setup` | Interactive credential storage (prompts for IMAP user/password, stores in keyring) |
| `--reap-orphans` | One-shot cleanup of orphan/duplicate messages that cannot form a batch |

> **Note:** The daemon now defaults to INFO-level logging so poll activity is
> always visible. Use `cellarbrain -q ingest` to suppress output (only warnings
> and errors shown). A periodic heartbeat is printed to stdout every
> `heartbeat_interval` polls (default: 10 cycles, ~10 minutes) regardless of
> log level.

### Prerequisites

- Install ingest dependencies: `pip install cellarbrain[ingest]`
- Configure `[ingest]` section in `cellarbrain.toml` (see [Settings Reference](settings-reference.md#ingestconfig))
- Store IMAP credentials via `cellarbrain ingest --setup` or set `CELLARBRAIN_IMAP_USER` and `CELLARBRAIN_IMAP_PASSWORD` environment variables

### How It Works

1. Connects to the IMAP server and searches for unread messages matching the subject filter
2. **Dedup step** (if `dedup_strategy = "latest"`): removes duplicate messages per filename, keeping only the newest. Older duplicates are marked as read immediately
3. Groups messages into batches by timestamp (all 3 files within `batch_window` seconds)
4. **Reaper step** (if `reaper_enabled`): messages that couldn't form a batch and are older than `stale_threshold` (default: 2× `batch_window`) are marked as read or moved to `dead_letter_folder`
5. For complete batches: extracts attachments, writes a snapshot folder (`raw/YYMMDD-HHMM/`), flushes top-level `raw/*.csv`
6. Invokes `cellarbrain etl` as a subprocess
7. Marks processed emails as read (or moves them to a configured folder)
8. In daemon mode, sleeps for `poll_interval` seconds and repeats. On transient errors, applies exponential backoff (up to 10 minutes)

### macOS Deployment

For always-on operation, deploy as a `launchd` user agent:

```bash
cellarbrain service install
```

This auto-generates a correct plist from your current environment (entry point, config path, data directory) and loads it via `launchctl`. See [`cellarbrain service`](#cellarbrain-service) below for full details.

<details>
<summary>Manual alternative (advanced)</summary>

A template plist is provided in `setup/reference/launchd-template.md`. Copy it to `~/Library/LaunchAgents/`, edit the three placeholder paths, then `launchctl load` it manually.

</details>

See also the [detailed design](../analysis/email-ingestion/02-detailed-design.md#9-macos-deployment-launchd) for background.

---

## `cellarbrain service`

Manage macOS `launchd` services for cellarbrain daemons (ingest, dashboard). Automatically generates plist files from the running environment — no manual path editing required.

> **macOS only.** Exits with an error on other platforms.

### `cellarbrain service install`

```bash
cellarbrain service install              # generate plist, load service
cellarbrain service install --dry-run    # print generated plist to stdout
cellarbrain service install --force      # overwrite existing plist
cellarbrain service install --no-start   # write plist without loading
```

| Flag | Description |
|------|-------------|
| `--dry-run` | Print the generated plist XML to stdout without writing or loading |
| `--force` | Overwrite an existing plist file (unloads first) |
| `--no-start` | Write the plist but skip `launchctl load` |

The generated plist uses:
- **Entry point**: resolved from `shutil.which("cellarbrain")`
- **Config**: from the currently resolved `cellarbrain.toml`
- **Working directory**: parent of the resolved data directory
- **Logs**: `~/Library/Logs/cellarbrain/ingest-{stdout,stderr}.log`

### `cellarbrain service uninstall`

```bash
cellarbrain service uninstall
```

Unloads the service via `launchctl unload` and removes the plist file from `~/Library/LaunchAgents/`.

### `cellarbrain service status`

```bash
cellarbrain service status
```

Shows whether the service plist is installed, loaded, running (with PID), and warns if the entry point has changed since install (e.g. after a `pip install --upgrade`).

### `cellarbrain service logs`

```bash
cellarbrain service logs              # last 50 lines of stdout
cellarbrain service logs --follow     # tail -f stdout
cellarbrain service logs --stderr     # show stderr log instead
```

| Flag | Description |
|------|-------------|
| `--follow`, `-f` | Follow log output (like `tail -f`) |
| `--stderr` | Show the stderr log instead of stdout |

---

## `cellarbrain info`

Show installation and configuration diagnostics — version, Python environment, resolved paths, installed optional dependencies, MCP server connection details, and a ready-to-paste Claude Desktop / OpenClaw config snippet.

```bash
# Full diagnostic report
cellarbrain info

# JSON output (for scripting)
cellarbrain info --json

# Print only the MCP client config snippet (copy-paste into claude_desktop_config.json)
cellarbrain info --mcp-config

# Print only resolved paths
cellarbrain info --paths

# Print only installed modules
cellarbrain info --modules
```

| Flag | Description |
|------|-------------|
| `--json` | Output as JSON for scripting or MCP integration |
| `--mcp-config` | Print only the Claude Desktop / OpenClaw JSON config snippet |
| `--paths` | Print only the resolved paths section |
| `--modules` | Print only the installed modules section |

### Output Sections

| Section | Content |
|---------|---------|
| **Header** | Package version, Python version & path, platform, install type (editable or regular), entry point |
| **Config** | Resolved config file path, resolution source, `CELLARBRAIN_CONFIG` and `CELLARBRAIN_DATA_DIR` env var status |
| **Paths** | Data directory, raw directory, dossier directory, backup directory, log database, sommelier model directory, Parquet file count |
| **Modules** | Core dependencies with versions, optional extras with install status |
| **MCP Server** | Command, transport options, tool/resource counts, dashboard port, Claude Desktop config snippet |
| **Skills** | Bundled skill count, install target, installed status |
| **Data Summary** | Last ETL run, wine count, bottle counts (only shown when data exists) |

### Complementary to `doctor`

`cellarbrain info` answers *"where is everything and how do I connect?"* while `cellarbrain doctor` answers *"is my data healthy?"*. Use `info` for setup verification and `doctor` for ongoing health checks.
