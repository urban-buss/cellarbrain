# ETL Pipeline

Transform Vinocell CSV exports into normalised Parquet tables and per-wine Markdown dossiers.

## Prerequisites

- Cellarbrain installed ([Installation](../getting-started/installation.md))
- Three Vinocell CSV exports in a `raw/` directory

## Preparing CSV Data

Export from the Vinocell app (macOS/iOS): **File → Export → CSV**.

```
raw/
├── export-wines.csv           # Wine definitions (names, regions, grapes)
├── export-bottles-stored.csv  # Bottles currently in cellar
└── export-bottles-gone.csv    # Consumed/removed bottles (optional)
```

> **Note:** These files are UTF-16 LE encoded, tab-delimited — the standard Vinocell export format. No conversion needed.

## Running ETL

### Full Load (first time)

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

### Incremental Sync (subsequent runs)

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output --sync
```

Sync mode preserves stable IDs, detects changes (inserts/updates/deletes/renames), preserves agent-written dossier content, and reports a change summary.

> **Important:** After the first full load, always use `--sync` for subsequent runs.

### Verbose Mode

```bash
cellarbrain -vv etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

Shows every parse decision, ID assignment, and file write.

## Validating Output

```bash
cellarbrain validate
```

Checks: PK uniqueness, FK referential integrity, domain constraints, dossier file existence.

## Recalculating Fields

After changing configuration (price tiers, currency rates, cellar rules):

```bash
cellarbrain recalc
```

| Field | Source |
|-------|--------|
| `drinking_status` | `drink_from`, `drink_until`, `optimal_from`, `optimal_until` + current year |
| `age_years` | `vintage` + current year |
| `list_price` | `original_list_price` + currency conversion rates |
| `price_tier` | `list_price` + `[[price_tiers]]` config |
| `is_onsite` | Cellar name + `[[cellar_rules]]` |
| `is_in_transit` | Cellar name + `[[cellar_rules]]` |

**When to use:** After changing `[[price_tiers]]`, `[currency.rates]`, `[[cellar_rules]]`, or at the start of a new year.

## Debugging

### Verbose ETL

```bash
cellarbrain -vv etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

DEBUG output shows CSV parsing decisions, entity deduplication, change detection, and Parquet write operations.

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `UnicodeDecodeError` | CSV encoding mismatch | Check `[csv] encoding` — default is `utf-16` |
| `FileNotFoundError` | Wrong file paths | Verify CSV files exist at specified paths |
| `ValueError: duplicate primary key` | Corrupt incremental state | Delete `output/` and do a fresh full load |
| `Schema mismatch` | Code change added columns | Run a fresh full load (without `--sync`) |

### Inspect Parquet Directly

```bash
python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('output/wine.parquet')
print(f'Schema: {table.schema}')
print(f'Rows: {table.num_rows}')
print(table.to_pandas().head())
"
```

## Next Steps

- [CLI](cli.md) — Query data via the command line
- [Configuration](../configuration/overview.md) — TOML settings for ETL behaviour
