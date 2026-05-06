# Settings Reference

All configuration is managed through frozen dataclasses in `settings.py`. Values can come from built-in defaults, a TOML config file, or environment variables.

## Precedence (highest → lowest)

1. CLI arguments (`--data-dir`, `--config`)
2. Environment variables (`CELLARBRAIN_DATA_DIR`, `CELLARBRAIN_CONFIG`)
3. Local config file (`cellarbrain.local.toml` — gitignored)
4. Config file (`cellarbrain.toml`)
5. Built-in defaults

## Merge Strategy

| Config Type | TOML Behaviour |
|------------|----------------|
| Scalars | TOML value replaces default |
| Tables (`classification_short`, `currency.rates`, `search.synonyms`) | Merged — TOML entries add/override; defaults preserved for absent keys |
| Arrays (`price_tiers`, `agent_sections`, `cellar_rules`) | Replaced entirely when present in TOML |

## Dataclasses

### `PathsConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `data_dir` | `str` | `"output"` | Root directory for Parquet and dossier output |
| `raw_dir` | `str` | `"raw"` | Directory containing CSV exports |
| `wines_subdir` | `str` | `"wines"` | Subdirectory under data_dir for dossiers |
| `cellar_subdir` | `str` | `"cellar"` | Subdirectory for wines with stored bottles |
| `archive_subdir` | `str` | `"archive"` | Subdirectory for wines without stored bottles |
| `wines_filename` | `str` | `"export-wines.csv"` | Expected filename for the wines CSV export |
| `bottles_filename` | `str` | `"export-bottles-stored.csv"` | Expected filename for the stored-bottles CSV export |
| `bottles_gone_filename` | `str` | `"export-bottles-gone.csv"` | Expected filename for the consumed-bottles CSV export |

### `CsvConfig`

| Field | Type | Default |
|-------|------|---------|
| `encoding` | `str` | `"utf-16"` |
| `delimiter` | `str` | `"\t"` |

### `QueryConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `row_limit` | `int` | `200` | Max rows returned by `execute_query` |
| `search_limit` | `int` | `10` | Max results from `find_wine` |
| `pending_limit` | `int` | `20` | Max results from `pending_research` |

### `DisplayConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `null_char` | `str` | `"—"` | Placeholder for null values in display |
| `separator` | `str` | `"·"` | Field separator in formatted output |
| `date_format` | `str` | `"%d.%m.%Y"` | Date rendering (European format) |
| `tasting_date_format` | `str` | `"%d %B %Y"` | Human-readable tasting date |
| `timestamp_format` | `str` | `"%Y-%m-%d %H:%M UTC"` | Timestamp rendering |

### `DrinkingWindowConfig`

Display labels for each drinking status enum:

| Field | Default Label |
|-------|--------------|
| `too_young` | `"Too young"` |
| `drinkable` | `"Drinkable, not yet optimal"` |
| `optimal` | `"In optimal window"` |
| `past_optimal` | `"Past optimal, still drinkable"` |
| `past_window` | `"Past drinking window"` |
| `unknown` | `"No drinking window data"` |

### `DossierConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `filename_format` | `str` | `"{wine_id:04d}-{slug}.md"` | Dossier filename template |
| `slug_max_length` | `int` | `60` | Max characters in filename slug |
| `max_full_name_length` | `int` | `80` | Truncation limit for `full_name` |
| `output_encoding` | `str` | `"utf-8"` | File encoding for dossier output |

### `EtlConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `default_mode` | `str` | `"full"` | Default ETL mode |
| `etl_fence_start` | `str` | `"<!-- source: etl — do not edit below this line -->"` | Start marker for ETL-owned sections |
| `etl_fence_end` | `str` | `"<!-- source: etl — end -->"` | End marker for ETL-owned sections |

### `IdentityConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `enable_fuzzy_match` | `bool` | `True` | Enable fuzzy name matching for wine renames |
| `rename_threshold` | `float` | `0.85` | SequenceMatcher ratio threshold for fuzzy match |

### `CurrencyConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `default` | `str` | `"CHF"` | Target currency for normalisation |
| `rates` | `dict[str, float]` | `{"EUR": 0.93, "USD": 0.88, "GBP": 1.11, "AUD": 0.56, "CAD": 0.62}` | Fixed exchange rates to default currency |

Rates are merged with TOML so you can add currencies without overriding the built-in ones.

Agents can also manage rates at runtime via the `currency_rates` MCP tool. Agent-managed rates are stored in `{data_dir}/currency-rates.json` and take highest priority (override both built-in defaults and TOML values). This sidecar file is loaded automatically by `load_settings()` during ETL and MCP operations.

### `WishlistConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `sections` | `tuple[str, ...]` | `("producer_deep_dive", "vintage_tracker", "buying_guide", "price_tracker")` | Companion dossier section keys |
| `scan_cadence_days` | `int` | `7` | Days between price scans |
| `alert_window_days` | `int` | `30` | Time window for alert generation |
| `price_drop_alert_pct` | `float` | `10.0` | Minimum % drop to trigger alert |
| `wishlist_subdir` | `str` | `"tracked"` | Subdirectory for companion dossiers |
| `retailers` | `dict[str, str]` | 10 Swiss retailers | Retailer slug → domain mapping |
| `bottle_sizes` | `dict[str, int]` | 6 sizes (375–6000 ml) | Size name → ml mapping |

Default retailers: gerstl.ch, martel.ch, flaschenpost.ch, moevenpick-wein.com, weinauktion.ch, wine.ch, juan-sanchez.ch, globalwine.ch, divo.ch, schuler.ch.

### `SearchConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `synonyms` | `dict[str, str]` | ~82 built-in entries | Maps query tokens to stored-data equivalents |

The synonyms dict enables multi-language search by mapping German (and other) terms to the values stored in the database. Entries fall into five categories:

| Category | Example | Effect |
|----------|---------|--------|
| Countries DE→EN | `schweiz → Switzerland` | Translates German country names |
| Country adjectives | `französisch → France` | Maps adjective forms to stored names |
| Categories DE→EN | `rotwein → red` | Translates German wine categories |
| Regions DE→stored | `burgund → Burgundy` | Maps German region names |
| Grapes DE→intl | `spätburgunder → Pinot Noir` | Maps German grape names |
| Stopwords | `weingut → ""` | Empty value drops the token from query |

Synonyms are merged from three layers (highest priority wins):
1. MCP-writable custom file (`data_dir/search-synonyms.json`) — managed via `search_synonyms` tool
2. TOML `[search.synonyms]` — overrides/extends defaults
3. Built-in defaults (~82 entries)

### `SommelierConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|--------|
| `enabled` | `bool` | `False` | Enable sommelier features |
| `model_dir` | `str` | `"models/sommelier/model"` | Directory for trained model artefacts |
| `food_catalogue` | `str` | `"models/sommelier/food_catalogue.parquet"` | Path to food catalogue Parquet file |
| `pairing_dataset` | `str` | `"models/sommelier/pairing_dataset.parquet"` | Path to wine-food pairing dataset |
| `food_index` | `str` | `"models/sommelier/food.index"` | Path to FAISS food index |
| `food_ids` | `str` | `"models/sommelier/food_ids.json"` | Path to food ID mapping (JSON list) |
| `wine_index_dir` | `str` | `"sommelier"` | Sub-directory under data_dir for wine FAISS index |
| `default_limit` | `int` | `10` | Default number of results returned |
| `min_score` | `float` | `0.0` | Minimum similarity score threshold |
| `base_model` | `str` | `"models/sommelier/base-model"` | Path to base sentence-transformer model (local or HuggingFace name) |
| `training_epochs` | `int` | `10` | Number of fine-tuning epochs |
| `training_batch_size` | `int` | `32` | Training batch size |
| `warmup_ratio` | `float` | `0.1` | Fraction of steps used for learning-rate warm-up |
| `eval_split` | `float` | `0.1` | Fraction of data held out for evaluation |

### `LoggingConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|--------|
| `level` | `str` | `"WARNING"` | Root log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) |
| `log_file` | `str \| None` | `None` | Path to log file; `None` disables file logging |
| `max_bytes` | `int` | `5242880` | Max file size in bytes before rotation (5 MB) |
| `backup_count` | `int` | `3` | Number of rotated backup files to keep |
| `format` | `str` | `"%(asctime)s %(levelname)-8s %(name)s — %(message)s"` | Log line format string. Set to `"json"` for structured JSON log lines |
| `date_format` | `str` | `"%Y-%m-%d %H:%M:%S"` | Timestamp format |
| `turn_gap_seconds` | `float` | `2.0` | Idle gap (seconds) before minting a new turn ID in MCP observability |
| `slow_threshold_ms` | `float` | `2000.0` | Warn when a tool invocation exceeds this duration |
| `log_db` | `str \| None` | `None` | Explicit path to DuckDB log store. Default: `<data_dir>/logs/cellarbrain-logs.duckdb` |
| `retention_days` | `int` | `90` | Auto-prune events older than this when `cellarbrain logs --prune` is run |

CLI flags override TOML: `-v` sets INFO, `-vv` sets DEBUG, `-q` sets ERROR, `--log-file` overrides `log_file`.
When running `cellarbrain mcp`, stderr is locked to WARNING to protect the JSON-RPC transport; use `log_file` for debug output.

### `DashboardConfig`

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `port` | `int` | `8017` | HTTP port for the web dashboard |
| `workbench_read_only` | `bool` | `True` | Block write tools in the workbench by default |
| `workbench_allow` | `list[str]` | `[]` | Write tools explicitly allowed even when `workbench_read_only` is `True` |

TOML example:

```toml
[dashboard]
port = 9000
workbench_read_only = true
workbench_allow = ["log_price"]
```
### `IngestConfig`

Configuration for the IMAP email ingestion daemon (`cellarbrain ingest`). Credentials are stored externally (system keyring or environment variables) — never in TOML.

| Field | Type | Default | Purpose |
|-------|------|---------|--------|
| `imap_host` | `str` | `"imap.mail.me.com"` | IMAP server hostname |
| `imap_port` | `int` | `993` | IMAP server port |
| `use_ssl` | `bool` | `True` | Use SSL/TLS for IMAP connection |
| `mailbox` | `str` | `"INBOX"` | IMAP folder to monitor |
| `subject_filter` | `str` | `"[VinoCell] CSV file"` | Only process emails matching this subject |
| `sender_filter` | `str` | `""` | IMAP-level FROM pre-filter (performance — reduces fetched messages) |
| `sender_whitelist` | `tuple[str, ...]` | `()` | Application-level post-fetch sender validation (security — see below) |
| `poll_interval` | `int` | `60` | Seconds between poll cycles |
| `batch_window` | `int` | `300` | Seconds within which all 3 files must arrive to form a batch |
| `expected_files` | `tuple[str, ...]` | `("export-wines.csv", "export-bottles-stored.csv", "export-bottles-gone.csv")` | Attachment filenames that form a complete batch |
| `processed_action` | `str` | `"flag"` | What to do with processed emails: `"flag"` (mark as read) or `"move"` |
| `processed_folder` | `str` | `"VinoCell/Processed"` | Target IMAP folder when `processed_action = "move"` |
| `etl_timeout` | `int` | `300` | Seconds before the ETL subprocess is killed |
| `max_backoff_interval` | `int` | `600` | Maximum seconds between retries on transient errors |
| `max_attachment_bytes` | `int` | `10485760` | Reject attachments larger than this (bytes). 0 = unlimited |

#### Sender filtering (layered model)

`sender_filter` and `sender_whitelist` serve complementary roles:

| Mechanism | Layer | Purpose |
|-----------|-------|---------|
| `sender_filter` | IMAP server | Reduces UIDs returned by SEARCH (performance) |
| `sender_whitelist` | Application | Defence-in-depth — validates parsed `From:` header after fetch (security) |

When `sender_whitelist` is non-empty, only emails whose `From:` address (case-insensitive exact match) appears in the list are processed. Unmatched messages are logged and discarded before grouping. An empty list (default) accepts all messages that pass the IMAP-level filters.

TOML example:

```toml
[ingest]
imap_host = "imap.mail.me.com"
poll_interval = 30
batch_window = 600
processed_action = "move"
processed_folder = "Archive/VinoCell"
sender_whitelist = ["noreply@vinocell.com"]
etl_timeout = 600
max_backoff_interval = 300
max_attachment_bytes = 10485760
```

Credential storage:

```bash
# Interactive setup (stores in system keyring)
cellarbrain ingest --setup

# Or use environment variables
export CELLARBRAIN_IMAP_USER="user@icloud.com"
export CELLARBRAIN_IMAP_PASSWORD="app-specific-password"
```
### `AgentSection`

Defines a single agent-owned section:

| Field | Type | Purpose |
|-------|------|---------|
| `key` | `str` | Machine-readable identifier |
| `heading` | `str` | Markdown H2/H3 heading text |
| `tag` | `str` | HTML comment fence tag (e.g. `"agent:research"`) |
| `mixed` | `bool` | `True` for sections with both ETL and agent content |

### `PriceTier`

| Field | Type | Purpose |
|-------|------|---------|
| `label` | `str` | Tier name (e.g. `"budget"`, `"fine"`) |
| `max` | `float | None` | Upper bound in default currency; `None` for catch-all |

## Top-Level `Settings`

Composes all sub-configs plus convenience helpers:

| Field | Type |
|-------|------|
| `paths` | `PathsConfig` |
| `csv` | `CsvConfig` |
| `price_tiers` | `tuple[PriceTier, ...]` |
| `query` | `QueryConfig` |
| `display` | `DisplayConfig` |
| `drinking_window` | `DrinkingWindowConfig` |
| `dossier` | `DossierConfig` |
| `agent_sections` | `tuple[AgentSection, ...]` |
| `classification_short` | `dict[str, str]` |
| `offsite_cellars` | `tuple[str, ...]` | Legacy — use `cellar_rules` instead |
| `in_transit_cellars` | `tuple[str, ...]` | Legacy — use `cellar_rules` instead |
| `cellar_rules` | `tuple[CellarRule, ...]` |
| `currency` | `CurrencyConfig` |
| `etl` | `EtlConfig` |
| `identity` | `IdentityConfig` |
| `wishlist` | `WishlistConfig` |
| `search` | `SearchConfig` |
| `sommelier` | `SommelierConfig` |
| `logging` | `LoggingConfig` |
| `companion_sections` | `tuple[AgentSection, ...]` |

### Helper Methods

| Method | Returns |
|--------|---------|
| `agent_section_keys()` | `frozenset[str]` of all per-vintage agent section keys |
| `pure_agent_sections()` | Agent sections with `mixed=False` |
| `mixed_agent_sections()` | Agent sections with `mixed=True` |
| `agent_section_by_key(key)` | Single `AgentSection` lookup (raises `KeyError`) |
| `heading_to_key()` | `dict[str, str]` mapping heading → key |
| `companion_section_keys()` | `frozenset[str]` of companion section keys |
| `companion_section_by_key(key)` | Single companion `AgentSection` lookup |

## TOML Example

```toml
[paths]
data_dir = "output"

[query]
row_limit = 200
search_limit = 10

[currency]
default = "CHF"

[currency.rates]
EUR = 0.93
USD = 0.88
ZAR = 0.05

[identity]
enable_fuzzy_match = true
rename_threshold = 0.85

# [search.synonyms]
# custom_term = "stored_value"    # add custom DE→EN mappings
# stopword = ""                   # empty string drops the token

# Legacy flat lists (still supported, but prefer cellar_rules):
# offsite_cellars = ["Remote storage"]
# in_transit_cellars = ["99 Orders & Subscriptions"]

# Cellar classification rules (first match wins, default is "onsite"):
[[cellar_rules]]
pattern = "03*"
classification = "offsite"

[[cellar_rules]]
pattern = "99*"
classification = "in_transit"

[[price_tiers]]
label = "budget"
max = 15

[[price_tiers]]
label = "everyday"
max = 27

[[price_tiers]]
label = "premium"
max = 40

[[price_tiers]]
label = "fine"

[logging]
level = "WARNING"
# log_file = "logs/cellarbrain.log"
# max_bytes = 5242880
# backup_count = 3

# [sommelier]
# enabled = false
# model_dir = "models/sommelier/model"
# base_model = "models/sommelier/base-model"
# training_epochs = 10
# training_batch_size = 32
# warmup_ratio = 0.1
# eval_split = 0.1
# default_limit = 10
# min_score = 0.0
```
