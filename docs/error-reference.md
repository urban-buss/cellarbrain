# Error Reference

Every user-facing error message, its cause, and how to fix it.

Cellarbrain surfaces errors through two interfaces:

- **CLI** — prints `Error: <message>` to stderr and exits with code 1.
- **MCP tools** — returns `Error: <message>` as the tool response string. Agents should check for the `Error:` prefix before processing results.

Parser errors during ETL include row context:

```
Wine row 42 ('Château Margaux' / 'Grand Vin' / '2019'): Cannot parse volume: 'big'
```

Data warnings are printed to stderr but do **not** stop the ETL pipeline.

---

## CLI Errors

### File & Encoding Errors

Raised by the CSV reader when input files are missing, corrupt, or in the wrong format.

| Error Pattern | Cause | Fix |
|---|---|---|
| `CSV file not found: <path>` | The specified CSV file does not exist | Verify the path; re-export from Vinocell |
| `Cannot read <file>: encoding is not utf-16` | The CSV file is not in Vinocell's UTF-16 LE format | Re-export from Vinocell; do not re-save in Excel |
| `CSV file is empty: <path>` | The export has headers but no data rows | Re-export from Vinocell; ensure the cellar is not empty |
| `Row has N columns but expected M` | A CSV row has a different column count than the header | The export may be corrupted; re-export |

### Parser Errors (during ETL)

All parser errors are wrapped with row context:

```
Wine row N ('winery' / 'name' / 'year'): <detail>
Bottle row N ('winery' / 'name' / 'year'): <detail>
Bottles-gone row N ('winery' / 'name' / 'year'): <detail>
```

| Detail Pattern | Cause | Fix |
|---|---|---|
| `Invalid category: <value>` | Unrecognised Category value | Check in Vinocell; supported values in `parsers.CATEGORY_MAP` |
| `Cannot parse volume: <value>` | Unexpected Volume format | Use: 375mL, 500mL, 750mL, Magnum, or X.X L |
| `Volume is required but got empty value` | Blank Volume field | Fill in the volume in Vinocell |
| `Cannot parse alcohol: <value>` | Non-numeric Alcohol content | Check the alcohol % in Vinocell |
| `Cannot parse acidity: <value>` | Non-numeric Acidity content | Check the acidity g/L in Vinocell |
| `Cannot parse sugar: <value>` | Non-numeric Sugar content | Check the sugar g/L in Vinocell |
| `Cannot parse ageing months: <value>` | Non-numeric Ageing Duration | Enter a numeric month count |
| `Cannot parse opening time: <value>` | Unexpected Opening Time format | Expected: `Xh00min` (e.g. `1h30min`) |
| `Cannot parse date: <value>` | Wrong date format | Use DD.MM.YYYY (e.g. `16.08.2024`) |
| `Cannot parse tasting date: <value>` | Wrong tasting date format | Use `21 February 2024` |
| `Invalid purchase type: <value>` | Unrecognised Input type in bottles CSV | Supported: Market price, Discount price, Present, Free |
| `Unknown output type: <value>` | Unrecognised output type in bottles-gone CSV | Check the value in Vinocell |
| `Cannot parse decimal: <value>` | Non-numeric price or rating | Check the value in Vinocell |
| `Cannot parse integer: <value>` | Non-numeric integer field | Check the value in Vinocell |
| `No exchange rate configured for <curr> → <default>` | Currency not in `cellarbrain.toml` | Add the rate under `[currency.rates]` |

### Data Warnings (during ETL)

Warnings are printed to stderr but do not stop the pipeline. They indicate data that was **skipped**.

| Warning Pattern | Cause | Action |
|---|---|---|
| `Duplicate wine natural key (...) at row N` | Two wines share winery + name + year | Check for duplicates; the later entry takes precedence |
| `Bottle row N: no matching wine for (...)` | Bottle references a missing wine | Ensure wines and bottles exports are from the same date |
| `Bottles-gone row N: no matching wine for (...)` | Consumed bottle references a missing wine | Same as above |
| `Tasting row N: no matching wine for (...)` | Tasting references a missing wine | Same as above |
| `Pro-rating row N: no matching wine for (...)` | Rating references a missing wine | Same as above |

### Configuration Errors

Raised by the settings loader when `cellarbrain.toml` has problems.

| Error Pattern | Cause | Fix |
|---|---|---|
| `Config file not found: <path>` | The `-c` flag points to a missing file | Check the path |
| `CELLARBRAIN_CONFIG points to missing file: <path>` | Environment variable points to a missing file | Fix or unset `CELLARBRAIN_CONFIG` |
| `Invalid TOML in <path>: <detail>` | Syntax error in TOML | Check for missing quotes, unclosed brackets, etc. |
| `Unknown key(s) in [<section>] config: <keys>` | Unrecognised keys in a TOML section | Fix the typo; the error lists valid key names |
| `Each price_tiers entry must have a 'label'` | A `[[price_tiers]]` entry is missing its label | Add `label = "..."` |
| `Each agent_sections entry must have 'key', 'heading', and 'tag'` | Incomplete `[[agent_sections]]` entry | Add all three required fields |

### Query Errors

Raised by the DuckDB query layer.

| Error Pattern | Cause | Fix |
|---|---|---|
| `Empty SQL statement.` | No SQL provided | Provide a `SELECT` query |
| `Multiple SQL statements are not allowed.` | Semicolon-separated statements | Use one statement at a time |
| `Only SELECT queries are allowed. Got: <keyword>` | DDL/DML attempted | Cellarbrain is read-only; use `SELECT` only |
| `SQL must start with SELECT or WITH. Got: <preview>` | SQL starts with SHOW, DESCRIBE, etc. | Rewrite as a `SELECT` query |
| `Missing Parquet files: <list>` | Data directory is empty or incomplete | Run `cellarbrain etl` first |
| `Invalid group_by: <value>` | Unknown dimension for `cellarbrain stats` | Use one of the listed valid dimensions |

### Schema / Writer Errors

Raised when a transformed value is incompatible with the Parquet schema. These normally indicate a bug.

| Error Pattern | Cause | Fix |
|---|---|---|
| `Schema error writing '<entity>' row N, field '<col>'` | Transform produced an incompatible value | Report with full error message |

---

## Dossier Errors

Shared between the CLI `dossier` subcommand and MCP `read_dossier` / `update_dossier` tools.

| Error Pattern | Cause | Fix |
|---|---|---|
| `wine.parquet not found in <dir>` | No data loaded yet | Run `cellarbrain etl` or `reload_data` first |
| `Wine ID N does not exist.` | Wine ID not in data | Use `find_wine` or `query_cellar` to find valid IDs |
| `Dossier file not found: <path>` | Markdown dossier missing on disk | Run `cellarbrain etl` or `reload_data` |
| `Invalid dossier path for wine N: path traversal detected.` | Security check blocked an invalid path | Report as a bug |
| `Section '<key>' is not an allowed agent section` | Attempted write to ETL-owned or unknown section | Use one of the allowed section keys listed in the error |
| `Could not find agent fence for '<heading>'` | Dossier missing expected HTML comment fences | Run `reload_data` or `cellarbrain etl` to regenerate |
| `Could not find Agent Log section` | Dossier missing the Agent Log section | Run `reload_data` or `cellarbrain etl` to regenerate |

---

## MCP Tool Errors

All MCP tool errors are returned as `Error: <message>` strings. The messages and causes match the CLI sections above.

| Tool | Caught Exceptions | Notes |
|---|---|---|
| `query_cellar` | `QueryError`, `DataStaleError` | Same as CLI query |
| `cellar_stats` | `ValueError`, `DataStaleError` | Invalid `group_by` or missing data |
| `find_wine` | `QueryError`, `DataStaleError` | Same as CLI query |
| `read_dossier` | `WineNotFoundError` | See Dossier Errors above |
| `update_dossier` | `WineNotFoundError`, `ProtectedSectionError` | See Dossier Errors above |
| `reload_data` | `ValueError`, `FileNotFoundError`, `UnicodeDecodeError` | Pre-flight checks + ETL errors |
| `pending_research` | `QueryError`, `DataStaleError` | Same as CLI query |
| `log_price` | `TrackedWineNotFoundError`, `ValueError` | Invalid tracked wine or price |
| `tracked_wine_prices` | `TrackedWineNotFoundError`, `DataStaleError` | Missing tracked wine or data |

### reload_data Pre-Flight Checks

Before running the ETL pipeline, `reload_data` checks for required CSV files:

| Error Pattern | Cause | Fix |
|---|---|---|
| `CSV file not found: <wines_csv_path>` | Wines CSV not in `raw/` directory | Export from cellarbrain to `raw/` |
| `CSV file not found: <bottles_csv_path>` | Bottles CSV not in `raw/` directory | Same as above |

---

## Exception Hierarchy

All exceptions inherit from `Exception`.

```
Exception
├── ValueError             — parser, config, and schema errors
├── FileNotFoundError      — missing CSV or config files
├── UnicodeDecodeError     — wrong file encoding
├── QueryError             — SQL validation or DuckDB execution failures
├── DataStaleError         — missing Parquet files
├── WineNotFoundError      — wine ID not found or dossier missing
├── TrackedWineNotFoundError — tracked wine ID not found
└── ProtectedSectionError  — write to ETL-owned or unknown dossier section
```

`QueryError` and `DataStaleError` are defined in `cellarbrain.query`. `WineNotFoundError`, `TrackedWineNotFoundError`, and `ProtectedSectionError` are defined in `cellarbrain.dossier_ops`.
