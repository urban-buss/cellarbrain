# Configuration Overview

Cellarbrain configuration uses TOML files with a layered precedence system.

## Precedence (highest wins)

1. **CLI arguments** — `--data-dir`, `--config`
2. **Environment variables** — `CELLARBRAIN_DATA_DIR`, `CELLARBRAIN_CONFIG`
3. **`cellarbrain.local.toml`** — machine-specific overrides (gitignored)
4. **`cellarbrain.toml`** — project defaults (checked in)
5. **Built-in defaults** — in `settings.py` dataclasses

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `CELLARBRAIN_DATA_DIR` | Path to Parquet data directory |
| `CELLARBRAIN_CONFIG` | Path to TOML config file |
| `CELLARBRAIN_IMAP_USER` | IMAP username (email ingest) |
| `CELLARBRAIN_IMAP_PASSWORD` | IMAP password (email ingest) |

## Local Overrides

Create `cellarbrain.local.toml` at the project root (gitignored):

```toml
# cellarbrain.local.toml — machine-specific overrides

[paths]
data_dir = "output"
raw_dir = "raw"

[logging]
level = "INFO"
log_file = "output/logs/cellarbrain.log"
format = "json"
```

## Configuration Sections

| Section | Purpose | Details |
|---------|---------|---------|
| `[paths]` | Data and output directories | `data_dir`, `raw_dir` |
| `[csv]` | CSV encoding/delimiter | `encoding` (default: `utf-16`), `delimiter` |
| `[currency]` | Default currency + exchange rates | `default`, `[currency.rates]` |
| `[[cellar_rules]]` | Cellar classification (onsite/offsite/in-transit) | `pattern`, `classification` |
| `[[price_tiers]]` | Price tier boundaries | `name`, `min`, `max` |
| `[sommelier]` | ML model paths and training params | `enabled`, `model_dir`, `base_model` |
| `[logging]` | Log level, file, format, rotation | `level`, `log_file`, `format`, `max_bytes` |
| `[dashboard]` | Web dashboard port and permissions | `port`, `workbench_read_only`, `workbench_allow` |
| `[ingest]` | IMAP polling configuration | `imap_host`, `poll_interval`, `batch_window` |

## Example Configuration

```toml
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

[sommelier]
enabled = true
model_dir = "models/sommelier/model"

[dashboard]
port = 8017
workbench_read_only = true
workbench_allow = ["log_price"]

[ingest]
imap_host = "imap.mail.me.com"
imap_port = 993
use_ssl = true
poll_interval = 60
batch_window = 300
```

## Full Reference

See [docs/settings-reference.md](../../docs/settings-reference.md) for all configuration fields with types, defaults, and descriptions.

## Next Steps

- [ETL](../modules/etl.md) — Run the pipeline with custom config
- [Logging](../operations/logging.md) — Configure logging in detail
