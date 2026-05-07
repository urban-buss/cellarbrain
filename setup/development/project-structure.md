# Project Structure

Annotated source tree for the Cellarbrain codebase.

```
cellarbrain/
├── src/cellarbrain/           # Source code (installed as editable package)
│   ├── __init__.py
│   ├── cli.py                 # CLI entry point — all subcommands
│   ├── mcp_server.py          # MCP server (FastMCP, stdio/SSE)
│   ├── settings.py            # Configuration dataclasses + TOML loader
│   ├── parsers.py             # Generic field parsers (dates, numbers, etc.)
│   ├── vinocell_parsers.py    # Vinocell-specific field parsers
│   ├── vinocell_reader.py     # CSV reader for Vinocell exports
│   ├── transform.py           # CSV → entity builders (normalise + split)
│   ├── writer.py              # Parquet writer with explicit schemas
│   ├── incremental.py         # Incremental sync (change detection, ID stability)
│   ├── query.py               # DuckDB query layer (views, search, validation)
│   ├── _query_base.py         # Base query helpers
│   ├── search.py              # find_wine intent parsing + synonym expansion
│   ├── computed.py            # Calculated fields (drinking_status, price_tier)
│   ├── markdown.py            # Dossier Markdown generation (ETL-owned sections)
│   ├── companion_markdown.py  # Companion dossier generation (tracked wines)
│   ├── dossier_ops.py         # Agent section read/write operations
│   ├── validate.py            # Parquet integrity validation
│   ├── flat.py                # Flat-file export helpers
│   ├── price.py               # Price tracking operations
│   ├── slugify.py             # Wine name → filename slug
│   ├── log.py                 # Logging setup (text + JSON formatters)
│   ├── observability.py       # MCP tool invocation tracking (DuckDB log store)
│   ├── dashboard/             # Web explorer (Starlette + HTMX + Pico CSS)
│   │   ├── app.py             # App assembly, routes, lifespan
│   │   ├── queries.py         # Observability queries
│   │   ├── cellar_queries.py  # Cellar data queries
│   │   ├── workbench.py       # Interactive MCP tool workbench
│   │   ├── templates/         # Jinja2 HTML templates
│   │   └── static/            # CSS, JS assets
│   ├── sommelier/             # ML food-wine pairing module
│   │   ├── engine.py          # Pairing engine (encode → search → rank)
│   │   ├── model.py           # Model loading/management
│   │   ├── index.py           # FAISS index build/load
│   │   ├── training.py        # Fine-tuning pipeline
│   │   ├── text_builder.py    # Text representation for encoding
│   │   ├── catalogue.py       # Food catalogue management
│   │   └── schemas.py         # Parquet schemas for training data
│   └── email_poll/            # IMAP email ingestion daemon
│       ├── __init__.py        # IngestDaemon, poll_once exports
│       ├── imap.py            # IMAP client wrapper
│       ├── grouping.py        # Batch detection (pure functions)
│       ├── placement.py       # Snapshot writing + raw/ flush
│       ├── credentials.py     # Keyring + env var credential resolution
│       └── etl_runner.py      # Subprocess ETL invocation
├── tests/                     # Test suite (pytest)
│   ├── conftest.py            # Shared fixtures
│   ├── test_*.py              # One file per source module
│   └── smoke_helpers/         # Smoke test infrastructure
├── docs/                      # Reference documentation
├── models/sommelier/          # ML model artefacts (trained weights, indexes)
├── raw/                       # Vinocell CSV exports (input data)
├── output/                    # Generated output (Parquet, dossiers, logs)
├── .github/                   # Workflows, agent definitions, instructions
├── .vscode/                   # VS Code workspace configuration
├── cellarbrain.toml           # Default configuration (checked in)
├── cellarbrain.local.toml     # Machine-specific overrides (gitignored)
├── pyproject.toml             # Package metadata, build config, dependencies
└── cellarbrain.code-workspace # VS Code workspace file
```

## Key Entry Points

| File | Purpose |
|------|---------|
| `cli.py` | CLI entry point — all subcommands dispatch from here |
| `mcp_server.py` | MCP server — `@mcp.tool()` decorated functions |
| `settings.py` | Configuration dataclasses + TOML loader |
| `transform.py` | Entity builder functions (CSV rows → normalised entities) |
| `writer.py` | Parquet writer — `SCHEMAS` dict defines all table schemas |

## Next Steps

- [Testing](testing.md) — Run and write tests
- [Building](building.md) — Build distribution packages
