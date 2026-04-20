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

## Legacy Interface

For backward compatibility, if the first argument ends in `.csv`, the legacy flat interface is used:

```bash
cellarbrain raw/export-wines.csv raw/export-bottles-stored.csv -o output [--sync]
```

This is equivalent to `cellarbrain etl ...` and will be removed in a future version.

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
