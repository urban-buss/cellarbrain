# Installation & Running

Complete guide to installing Cellarbrain on macOS and running every module and mode. Written for Mac Mini M4 (24 GB RAM).

---

## 1. Installation Options

### Option A: Install from PyPI (recommended for users)

```bash
# Ensure Python 3.11+ is installed
python3 --version

# Install core only (ETL + CLI + MCP server)
pip install cellarbrain
```

### Option B: Install from Homebrew (when available)

```bash
brew tap urban-buss/cellarbrain
brew install cellarbrain
```

The Homebrew formula includes **all** optional modules (research, sommelier, dashboard, ingest). No additional install steps are needed — skip to [Initial Setup](#2-initial-setup).

See [Publishing](03-publishing.md#4-publishing-to-homebrew) for tap creation details.

### Option C: Install from Source (developers)

```bash
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[research,sommelier,dashboard,ingest]"
```

See [Local Development](01-local-development.md) for full dev setup.

### Install All Modules (PyPI)

To install everything at once (recommended if you want the full feature set):

```bash
pip install "cellarbrain[research,sommelier,dashboard,ingest]"
```

This gives you:

| Extra | What it adds | Setup guide |
|-------|-------------|-------------|
| `research` | httpx for web research agents | No additional setup needed |
| `sommelier` | ML food-wine pairing (sentence-transformers, FAISS) | [Section 7 — Sommelier ML Module](#7-sommelier-ml-module) |
| `dashboard` | Web UI (Starlette, Uvicorn, Jinja2) | [Section 6 — Web Dashboard](#6-web-dashboard) |
| `ingest` | Email polling (IMAPClient, keyring) | [Section 8 — Email Ingestion](#8-email-ingestion) |

Or install individual extras as needed:

```bash
pip install "cellarbrain[research]"          # web research only
pip install "cellarbrain[sommelier]"         # ML pairing only
pip install "cellarbrain[dashboard]"         # web dashboard only
pip install "cellarbrain[ingest]"            # email ingestion only
```

> **Homebrew users:** All modules are included in the formula — no extras to install separately.

### Verify Installation

```bash
cellarbrain --help
```

---

## 2. Initial Setup

### 2.1 Prepare Vinocell CSV Exports

Export from the Vinocell app (macOS/iOS): **File → Export → CSV**.

Create a working directory and place the exports:

```bash
mkdir -p ~/cellarbrain-data/raw
# Copy your exported CSV files:
cp ~/Downloads/export-wines.csv ~/cellarbrain-data/raw/
cp ~/Downloads/export-bottles-stored.csv ~/cellarbrain-data/raw/
cp ~/Downloads/export-bottles-gone.csv ~/cellarbrain-data/raw/
```

Expected file structure:
```
~/cellarbrain-data/
└── raw/
    ├── export-wines.csv           # Wine definitions (names, regions, grapes)
    ├── export-bottles-stored.csv  # Bottles currently in cellar
    └── export-bottles-gone.csv    # Consumed/removed bottles (optional)
```

> **Note:** These files are UTF-16 LE encoded, tab-delimited. This is the default Vinocell export format — no conversion needed.

### 2.2 Configuration

Create a configuration file (optional — defaults work for most setups):

```bash
mkdir -p ~/cellarbrain-data
cat > ~/cellarbrain-data/cellarbrain.toml << 'EOF'
[paths]
data_dir = "output"
raw_dir = "raw"

[currency]
default = "CHF"

[currency.rates]
EUR = 0.93
USD = 0.88
GBP = 1.11

[[cellar_rules]]
pattern = "03*"
classification = "offsite"

[[cellar_rules]]
pattern = "99*"
classification = "in_transit"

[logging]
level = "INFO"
log_file = "output/logs/cellarbrain.log"
format = "json"
EOF
```

Configuration precedence (highest wins):
1. CLI arguments (`--data-dir`, `--config`)
2. Environment variables (`CELLARBRAIN_DATA_DIR`, `CELLARBRAIN_CONFIG`)
3. `cellarbrain.local.toml` (if exists, machine-specific)
4. `cellarbrain.toml` (project defaults)
5. Built-in defaults

### 2.3 Run Initial ETL

```bash
cd ~/cellarbrain-data
cellarbrain -c cellarbrain.toml etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

Expected output:
```
Parsed 484 wines, 813 bottles (stored), 247 bottles (gone)
Written 12 Parquet tables to output/
Generated 484 dossiers in output/wines/
```

### 2.4 Validate

```bash
cellarbrain validate
```

Expected output:
```
✓ All 12 tables valid
✓ Primary keys unique
✓ Foreign keys valid
✓ Domain constraints satisfied
```

---

## 3. Running the ETL Pipeline

### 3.1 Full Load

The first ETL run creates all Parquet tables and dossiers from scratch:

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

**What it creates:**

| Output | Location | Description |
|--------|----------|-------------|
| Parquet tables (12) | `output/*.parquet` | wine, bottle, winery, appellation, grape, wine_grape, cellar, provider, tasting, pro_rating, tracked_wine, price_observation |
| Wine dossiers | `output/wines/cellar/*.md` | Per-wine Markdown with ETL + agent sections |
| Archive dossiers | `output/wines/archive/*.md` | Dossiers for consumed wines |
| Companion dossiers | `output/wines/tracked/*.md` | Dossiers for tracked/wishlist wines |

### 3.2 Incremental Sync

After re-exporting from Vinocell with updated data:

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
```

Sync mode:
- **Preserves stable IDs** — wine_id, bottle_id, etc. remain consistent
- **Detects changes** — inserts, updates, deletes, renames
- **Preserves agent content** — only regenerates ETL-owned dossier sections
- **Reports changes** — summary of what changed

> **Important:** After the first full load, always use `--sync` for subsequent runs.

### 3.3 Verbose Mode (debugging)

```bash
cellarbrain -vv etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
```

### 3.4 Custom Output Directory

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o /path/to/custom/output
```

---

## 4. CLI Queries

### 4.1 Cellar Statistics

```bash
# Overview
cellarbrain stats

# Group by dimension
cellarbrain stats --by country
cellarbrain stats --by region
cellarbrain stats --by category
cellarbrain stats --by vintage
cellarbrain stats --by winery
cellarbrain stats --by grape
cellarbrain stats --by cellar
cellarbrain stats --by provider
cellarbrain stats --by status
```

### 4.2 SQL Queries

```bash
# Count all wines
cellarbrain query "SELECT count(*) AS total FROM wines"

# List wines by region
cellarbrain query "SELECT full_name, vintage, region FROM wines WHERE region = 'Bordeaux'"

# Export as CSV
cellarbrain query "SELECT * FROM wines_stored" --format csv > wines.csv

# Export as JSON
cellarbrain query "SELECT * FROM wines_stored LIMIT 10" --format json

# Read SQL from a file
cellarbrain query -f queries/my-query.sql
```

Available views for queries:
- `wines` — All wines with computed fields (full_name, drinking_status, price_tier)
- `wines_stored` — Only wines with bottles currently in cellar
- `wines_full` — Extended wine view with winery, appellation, grapes joined
- `bottles_stored` — Individual bottles with cellar/shelf location
- `bottles_full` — Bottles with wine details joined
- `tracked_wines` — Wishlist / tracked wines for price monitoring

### 4.3 Wine Dossiers

```bash
# View a specific wine dossier
cellarbrain dossier 42

# View specific sections only
cellarbrain dossier 42 --sections identity origin producer_profile

# Search for wines
cellarbrain dossier --search "Barolo"
cellarbrain dossier --search "Pinot Noir 2019"

# List wines pending research
cellarbrain dossier --pending
cellarbrain dossier --pending --limit 50
```

---

## 5. MCP Server

The MCP (Model Context Protocol) server allows AI agents to query and manage your cellar.

### 5.1 Start with stdio Transport (default)

```bash
cellarbrain mcp
```

This is the default transport used by Claude Desktop and VS Code Copilot. The server communicates over stdin/stdout using JSON-RPC.

> **Note:** When running in stdio mode, the server produces no visible output — it's waiting for JSON-RPC messages on stdin. Use Ctrl+C to stop.

### 5.2 Start with SSE Transport (HTTP)

```bash
cellarbrain mcp --transport sse --port 8080
```

This starts an HTTP server at `http://localhost:8080` with Server-Sent Events transport. Useful for:
- HTTP-based MCP clients
- Remote access
- Always-on service (with launchd)

### 5.3 With Custom Data Directory

```bash
cellarbrain -d /path/to/output mcp
```

### 5.4 With Custom Config

```bash
cellarbrain -c /path/to/cellarbrain.toml mcp
```

### 5.5 Environment Variables

| Variable | Purpose |
|----------|---------|
| `CELLARBRAIN_DATA_DIR` | Path to Parquet data (overrides config) |
| `CELLARBRAIN_CONFIG` | Path to TOML config file |

### 5.6 Configure with Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain",
      "args": ["-d", "/Users/<you>/cellarbrain-data/output", "mcp"],
      "env": {}
    }
  }
}
```

If installed globally via pip:
```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["mcp"],
      "env": {
        "CELLARBRAIN_DATA_DIR": "/Users/<you>/cellarbrain-data/output"
      }
    }
  }
}
```

### 5.7 Configure with VS Code (Copilot)

Create `.vscode/mcp.json` in your workspace:

```json
{
  "servers": {
    "cellarbrain": {
      "command": "${command:python.interpreterPath}",
      "args": ["-m", "cellarbrain", "-d", "${workspaceFolder}/output", "mcp"],
      "env": {}
    }
  }
}
```

---

## 6. Web Dashboard

The web dashboard provides a local UI for browsing cellar data, observability metrics, and running interactive MCP tool queries.

### 6.1 Prerequisites

```bash
# Install dashboard dependencies (PyPI)
pip install "cellarbrain[dashboard]"

# Homebrew users: already included — skip the pip command above
brew install cellarbrain
```

Additionally, ETL must have been run (for cellar data):

```bash
cellarbrain validate

# MCP server must have been run at least once (for log store)
# The log store is created automatically on first MCP server run
```

### 6.2 Start the Dashboard

```bash
# Default: http://localhost:8017
cellarbrain dashboard

# Custom port
cellarbrain dashboard --port 9000

# Auto-open browser
cellarbrain dashboard --open
```

### 6.3 Dashboard Pages

| Page | URL | Description |
|------|-----|-------------|
| Observability Overview | `/` | KPI cards, hourly volume chart, top tools, recent errors |
| Tool Usage | `/tools` | Per-tool call counts, latency percentiles, error rates |
| Error Log | `/errors` | Filterable error list with details |
| Sessions | `/sessions` | Session drill-down with turn grouping |
| Latency | `/latency` | P50/P95/P99 latency breakdown per tool |
| Live Tail | `/tail` | Real-time SSE event stream |
| Cellar Browser | `/cellar` | Paginated, filterable wine list with detail views |
| Bottle Inventory | `/bottles` | Individual bottle locations and status |
| Tracked Wines | `/tracked` | Wishlist wines with price observations |
| Statistics | `/stats` | Visual cellar statistics (country, region, category, etc.) |
| SQL Playground | `/sql` | Interactive DuckDB SQL editor |
| Tool Workbench | `/workbench` | Call MCP tools with a web form UI |

### 6.4 Configuration

In `cellarbrain.toml`:

```toml
[dashboard]
port = 8017
workbench_read_only = true       # Block write tools by default
workbench_allow = ["log_price"]  # Explicitly allow specific write tools
```

---

## 7. Sommelier ML Module

The sommelier module provides semantic food-wine pairing using a fine-tuned sentence-transformer model and FAISS vector indexes.

### 7.1 Install Dependencies

```bash
# PyPI
pip install "cellarbrain[sommelier]"

# Homebrew users: already included — skip the pip command above
brew install cellarbrain
```

This installs:
- `sentence-transformers` — Embedding model framework
- `faiss-cpu` — Vector similarity search
- `datasets` — Training data loading
- `accelerate` — Training acceleration

### 7.2 Enable Sommelier in Config

In `cellarbrain.toml`:

```toml
[sommelier]
enabled = true
model_dir = "models/sommelier/model"
food_catalogue = "models/sommelier/food_catalogue.parquet"
pairing_dataset = "models/sommelier/pairing_dataset.parquet"
base_model = "models/sommelier/base-model"
```

### 7.3 Train the Model

```bash
# Default training (10 epochs, batch size 32)
cellarbrain train-model

# Custom parameters
cellarbrain train-model --epochs 15 --batch-size 64

# Output to a different directory
cellarbrain train-model --output /path/to/models
```

Training on Mac Mini M4 (24 GB):
- Duration: ~3–5 minutes (CPU-only, Apple Silicon optimised)
- Memory usage: ~4–6 GB peak
- Output: model weights saved to `models/sommelier/model/`

### 7.4 Build FAISS Indexes

```bash
# Build both food and wine indexes
cellarbrain rebuild-indexes

# Food index only
cellarbrain rebuild-indexes --food-only

# Wine index only (useful after ETL)
cellarbrain rebuild-indexes --wine-only
```

> **Note:** The wine index is automatically rebuilt after each ETL run when `sommelier.enabled = true`.

### 7.5 Retrain (Incremental)

After adding new pairing data:

```bash
cellarbrain retrain-model
cellarbrain rebuild-indexes
```

### 7.6 Verify Sommelier Works

Use the CLI or MCP tools:

```bash
# Via SQL (checks wine index)
cellarbrain query "SELECT count(*) FROM wines_stored"

# Via MCP (start server and test with Claude/Copilot):
# "What wine goes with grilled lamb?"
# → triggers suggest_wines tool
```

Or test via the web dashboard workbench at `/workbench`:
1. Select `suggest_wines` tool
2. Enter: `food_description = "grilled lamb with rosemary"`
3. Click Execute

---

## 8. Email Ingestion

Automated IMAP email polling that detects Vinocell CSV export emails, extracts attachments, and triggers the ETL pipeline.

### 8.1 Install Dependencies

```bash
# PyPI
pip install "cellarbrain[ingest]"

# Homebrew users: already included — skip the pip command above
brew install cellarbrain
```

### 8.2 Configure IMAP Settings

In `cellarbrain.toml`:

```toml
[ingest]
imap_host = "imap.mail.me.com"    # iCloud IMAP server
imap_port = 993
use_ssl = true
mailbox = "INBOX"
subject_filter = "[VinoCell] CSV file"
poll_interval = 60                 # seconds between polls
batch_window = 300                 # seconds to wait for all 3 files
processed_action = "flag"          # "flag" (mark read) or "move"
processed_folder = "VinoCell/Processed"  # target for "move" action
```

### 8.3 Set Up Credentials

**Option A: Interactive setup (stores in macOS Keychain)**

```bash
cellarbrain ingest --setup
```

This prompts for:
- IMAP username (your email address)
- IMAP password (app-specific password)

Credentials are stored securely in the macOS Keychain via the `keyring` library.

**Option B: Environment variables**

```bash
export CELLARBRAIN_IMAP_USER="user@icloud.com"
export CELLARBRAIN_IMAP_PASSWORD="xxxx-xxxx-xxxx-xxxx"
```

### 8.4 iCloud App-Specific Password

For iCloud Mail (imap.mail.me.com):

1. Go to [appleid.apple.com](https://appleid.apple.com)
2. Sign in → Security → App-Specific Passwords
3. Click "+" to generate a new password
4. Label it "Cellarbrain Ingest"
5. Copy the generated password (format: `xxxx-xxxx-xxxx-xxxx`)
6. Use this password in `cellarbrain ingest --setup`

> **Important:** Your regular Apple ID password will NOT work with IMAP. You must use an app-specific password.

### 8.5 Run Single Poll Cycle

```bash
# Process any pending emails, then exit
cellarbrain ingest --once

# Dry run — detect batches but don't write files
cellarbrain ingest --once --dry-run
```

### 8.6 Run as Daemon (Foreground)

```bash
cellarbrain ingest
```

The daemon:
1. Connects to IMAP server
2. Searches for unread messages matching the subject filter
3. Groups messages into complete batches (all 3 CSV files within `batch_window`)
4. Extracts attachments → writes snapshot folder (`raw/YYMMDD/`)
5. Flushes top-level `raw/*.csv` files
6. Runs `cellarbrain etl` as subprocess
7. Marks emails as processed
8. Sleeps for `poll_interval` seconds
9. Repeats (with exponential backoff on transient errors)

Stop with `Ctrl+C`.

### 8.7 Run as launchd Service (Background, Always-On)

Create the plist file:

```bash
cat > ~/Library/LaunchAgents/com.cellarbrain.ingest.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cellarbrain.ingest</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain</string>
        <string>--config</string>
        <string>/Users/<you>/repos/cellarbrain/cellarbrain.toml</string>
        <string>ingest</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/<you>/repos/cellarbrain/output/logs/ingest-stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/<you>/repos/cellarbrain/output/logs/ingest-stderr.log</string>
    <key>WorkingDirectory</key>
    <string>/Users/<you>/repos/cellarbrain</string>
</dict>
</plist>
EOF
```

> **Replace** `<you>` with your macOS username.

Load the service:

```bash
# Start the daemon
launchctl load ~/Library/LaunchAgents/com.cellarbrain.ingest.plist

# Check if running
launchctl list | grep cellarbrain

# View logs
tail -f output/logs/ingest-stdout.log
tail -f output/logs/ingest-stderr.log

# Stop the daemon
launchctl unload ~/Library/LaunchAgents/com.cellarbrain.ingest.plist
```

The daemon will:
- Start automatically on login (`RunAtLoad`)
- Restart automatically on crash (`KeepAlive`)
- Log stdout/stderr to rotating log files

---

## 9. Recalculating Fields

After changing configuration (price tiers, currency rates, cellar rules):

```bash
cellarbrain recalc
```

### What It Recomputes

| Field | Source |
|-------|--------|
| `drinking_status` | `drink_from`, `drink_until`, `optimal_from`, `optimal_until` + current year |
| `age_years` | `vintage` + current year |
| `list_price` | `original_list_price` + currency conversion rates |
| `price_tier` | `list_price` + `[[price_tiers]]` config |
| `is_onsite` | Cellar name + `[[cellar_rules]]` |
| `is_in_transit` | Cellar name + `[[cellar_rules]]` |

### When to Use

- After changing `[[price_tiers]]` boundaries
- After updating `[currency.rates]`
- After modifying `[[cellar_rules]]`
- At the start of a new year (updates `age_years` and `drinking_status`)

### Custom Output

```bash
cellarbrain recalc -o /path/to/output
```

---

## 10. Wishlist & Price Tracking

### 10.1 View Alerts

```bash
# Current alerts (price drops, new listings, back in stock)
cellarbrain wishlist alerts

# Custom alert window
cellarbrain wishlist alerts --days 7
```

### 10.2 Tracked Wine Statistics

```bash
cellarbrain wishlist stats
```

### 10.3 Price Scanning

Price scanning is agent-driven — use the `cellarbrain-price-tracker` agent:

```
@cellarbrain-price-tracker scan prices
```

Or manually log prices via MCP:
- `log_price(tracked_wine_id, vintage, bottle_size_ml, retailer_name, price, currency, in_stock)`

---

## 11. Observability Logs

### 11.1 Tail Recent Events

```bash
cellarbrain logs
cellarbrain logs --tail 50
```

### 11.2 View Errors

```bash
cellarbrain logs --errors
cellarbrain logs --errors --since 48  # last 48 hours
```

### 11.3 Tool Usage Summary

```bash
cellarbrain logs --usage
```

### 11.4 Latency Statistics

```bash
cellarbrain logs --latency
```

### 11.5 Session Summary

```bash
cellarbrain logs --sessions
```

### 11.6 Prune Old Events

```bash
cellarbrain logs --prune
```

Deletes events older than `retention_days` (default: 90 days).

---

## 12. Running as Background Services (launchd)

### 12.1 MCP Server (SSE Mode)

For always-on MCP access via HTTP:

```bash
cat > ~/Library/LaunchAgents/com.cellarbrain.mcp.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cellarbrain.mcp</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain</string>
        <string>--config</string>
        <string>/Users/<you>/repos/cellarbrain/cellarbrain.toml</string>
        <string>mcp</string>
        <string>--transport</string>
        <string>sse</string>
        <string>--port</string>
        <string>8080</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/<you>/repos/cellarbrain/output/logs/mcp-stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/<you>/repos/cellarbrain/output/logs/mcp-stderr.log</string>
    <key>WorkingDirectory</key>
    <string>/Users/<you>/repos/cellarbrain</string>
</dict>
</plist>
EOF
```

### 12.2 Web Dashboard

```bash
cat > ~/Library/LaunchAgents/com.cellarbrain.dashboard.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cellarbrain.dashboard</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain</string>
        <string>--config</string>
        <string>/Users/<you>/repos/cellarbrain/cellarbrain.toml</string>
        <string>dashboard</string>
        <string>--port</string>
        <string>8017</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/<you>/repos/cellarbrain/output/logs/dashboard-stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/<you>/repos/cellarbrain/output/logs/dashboard-stderr.log</string>
    <key>WorkingDirectory</key>
    <string>/Users/<you>/repos/cellarbrain</string>
</dict>
</plist>
EOF
```

### 12.3 Managing launchd Services

```bash
# Load (start) a service
launchctl load ~/Library/LaunchAgents/com.cellarbrain.ingest.plist

# Unload (stop) a service
launchctl unload ~/Library/LaunchAgents/com.cellarbrain.ingest.plist

# Check running status
launchctl list | grep cellarbrain

# View all cellarbrain services
launchctl list | grep com.cellarbrain

# Force restart
launchctl unload ~/Library/LaunchAgents/com.cellarbrain.ingest.plist
launchctl load ~/Library/LaunchAgents/com.cellarbrain.ingest.plist
```

### 12.4 Log Rotation for launchd Services

The cellarbrain log system handles rotation automatically via `RotatingFileHandler`:
- Max file size: 5 MB (configurable via `logging.max_bytes`)
- Backup count: 3 (configurable via `logging.backup_count`)

For launchd stdout/stderr logs, add periodic cleanup:

```bash
# Add to crontab: truncate logs weekly
crontab -e
# Add:
0 3 * * 0 truncate -s 0 /Users/<you>/repos/cellarbrain/output/logs/ingest-stdout.log
0 3 * * 0 truncate -s 0 /Users/<you>/repos/cellarbrain/output/logs/ingest-stderr.log
```

---

## 13. Complete Workflow Example

A typical day-to-day workflow on the Mac Mini:

```bash
# 1. Export from Vinocell (done on iOS/macOS — emails arrive via iCloud)
#    → ingest daemon automatically processes them

# 2. Check that ETL ran successfully
cellarbrain logs --tail 5
cellarbrain validate

# 3. Quick cellar check
cellarbrain stats
cellarbrain stats --by status

# 4. Check what's ready to drink
cellarbrain query "SELECT full_name, vintage, drinking_status FROM wines_stored WHERE drinking_status = 'In optimal window' ORDER BY vintage"

# 5. View alerts (price drops, new listings)
cellarbrain wishlist alerts

# 6. Open dashboard for visual overview
open http://localhost:8017

# 7. Use AI agent for food pairing (via Claude Desktop or VS Code Copilot)
# "What wine goes with tonight's grilled lamb?"
```

---

## Next Steps

- [Debugging & Monitoring](05-debugging-and-monitoring.md) — Troubleshoot issues, monitor health
- [OpenClaw Integration](06-openclaw-integration.md) — Connect AI agents
