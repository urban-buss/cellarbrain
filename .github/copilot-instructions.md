---
applyTo: "**"
---
# Cellarbrain — Copilot Workspace Instructions

## Project Overview

Cellarbrain is a CLI toolkit and MCP server that transforms Vinocell wine-cellar CSV exports into normalised Parquet tables, per-wine Markdown dossiers, and an in-process DuckDB query layer. Python 3.11+, MIT license.

## Versioning

The `local_future_of_cellarbrain` branch targets **0.3.0** (pre-release: `0.3.0a1`). The `main` branch is still on the 0.2.x line. When resolving merge conflicts involving the version field in `pyproject.toml`, always keep the **0.3.0** series version from this branch.

## Knowledge Base

Detailed documentation lives in `docs/`. Consult these pages for architecture, data model, and subsystem behaviour:

| Topic | File |
|-------|------|
| Architecture & modules | `docs/architecture.md` |
| Entity model (14 tables, schemas, FK refs) | `docs/entity-model.md` |
| ETL pipeline (ingestion → Parquet → dossiers) | `docs/etl-pipeline.md` |
| Dossier system (ownership, fences, lifecycle) | `docs/dossier-system.md` |
| Query layer (views, security, statistics) | `docs/query-layer.md` |
| Computed properties (full_name, drinking_status, etc.) | `docs/computed-properties.md` |
| MCP tools (19 tools, 10 resources) | `docs/mcp-tools.md` |
| Settings reference (all dataclasses & TOML) | `docs/settings-reference.md` |
| Architecture decisions | `docs/decisions/001-*` through `008-*` |
| Sommelier module (AI food-wine pairing) | `analysis/sommelier-model/00-project-overview.md` |
| Full index | `docs/index.md` |

## Coding Conventions

### Python Style

- `from __future__ import annotations` at the top of every module.
- Union syntax: `str | None`, never `Optional[str]`. Built-in generics: `list`, `dict`, `tuple`.
- Private helpers: `_leading_underscore`.
- Module-level docstring explaining purpose, then stdlib → third-party → local imports.
- Parsers include `Examples:` in docstrings showing input → output.

### Type Aliases

```python
Lookup = dict[str, int]              # display_value → id
CompositeLookup = dict[tuple, int]   # composite_key → id
```

### Error Patterns

- Parsers: return `None` for optional missing fields, raise `ValueError` for required.
- Query layer: `QueryError` for SQL validation/execution, `DataStaleError` for missing Parquet.
- Dossier ops: `WineNotFoundError`, `ProtectedSectionError`.
- No defensive error handling for scenarios that cannot occur.

### Writer Schemas

All Parquet schemas are defined in `writer.SCHEMAS` as `pa.Schema` objects. When adding a new entity or column, update the schema dict first — the writer enforces schema conformance.

## Testing Conventions (pytest)

- Tests live in `tests/`, one file per source module.
- Group related tests in classes: `class TestBuildWineries:`.
- Shared fixtures in `conftest.py` (e.g., `sample_wine_row`, `sample_bottle_row`).
- Test data builders: helper functions like `_make_dataset`, `_minimal_wine` create temp Parquet datasets.
- Integration tests (`test_integration.py`) use `@pytest.mark.skipif` — they require real CSV files in `raw/`.
- Use `tmp_path` for all file I/O in tests; never write to the real `output/` directory.
- Use `pytest.raises(ValueError)` for expected failures.
- Use `@pytest.mark.parametrize` for data-driven tests; direct assertions for simple cases.

## Security Invariants

- `dossier_ops.resolve_dossier_path` uses `is_relative_to()` to prevent path traversal.
- `query.validate_sql` rejects INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE.
- Agent sections are gated by `ALLOWED_SECTIONS` — ETL-owned content cannot be overwritten.

## Always Write Tests

Every code change must include corresponding test updates. Never submit production code without tests. When adding or modifying functionality:

- Add unit tests in the matching `tests/test_<module>.py` file.
- Cover the happy path, edge cases (None, empty string), and error cases.
- If a change touches multiple modules (e.g. parser + transform + writer), add tests for each.
- Run `pytest` before considering any task complete.

## Common Tasks

### Adding a new parsed field
1. Add parser in `parsers.py` (generic) or `vinocell_parsers.py` (Vinocell-specific) with docstring examples.
2. Map it in `transform.py` builder function.
3. Add column to `writer.SCHEMAS`.
4. Add a schema migration in `src/cellarbrain/migrations/` if existing Parquet files need updating.
5. Add column to relevant dossier section in `markdown.py`.
6. Add tests for parser + transform.
7. Run `pytest` and `cellarbrain validate`.

### Adding a schema migration
1. Bump `CURRENT_VERSION` in `migrate.py`.
2. Create `src/cellarbrain/migrations/m00N_description.py` with a `MigrationStep` using primitives from `migrate.py`.
3. Import and register in `migrations/__init__.py`.
4. Add tests in `tests/test_migrate.py`.
5. Run `pytest`.

### Adding a sommelier feature
1. Implement in `src/cellarbrain/sommelier/` — keep engine, model, index, and text_builder separate.
2. Settings go in `SommelierConfig` dataclass (`settings.py`).
3. CLI commands go as subparsers in `cli.py` (e.g. `train-model`, `retrain-model`, `rebuild-indexes`).
4. MCP tools go in `mcp_server.py` — call `engine.check_availability()` before processing.
5. Add tests in `tests/test_sommelier.py`.
6. Optional deps: `sentence-transformers` and `faiss-cpu` are in `[ml]` extra.

### Adding a new MCP tool
1. Define in `mcp_server.py` with `@mcp.tool()`.
2. Keep the tool thin — data access only, return formatted strings.
3. Handle exceptions from `query`/`dossier_ops`, return `f"Error: {exc}"`.
4. Add tests in `test_mcp_server.py` using a temp Parquet dataset.
5. Update the tool table in `.github/agents/cellarbrain.agent.md`.

### Adding an email ingestion feature
1. Implement in `src/cellarbrain/email_poll/` — keep grouping, placement, imap, credentials, and etl_runner as separate sub-modules.
2. Settings go in `IngestConfig` dataclass (`settings.py`).
3. CLI subcommand is `ingest` in `cli.py`.
4. Add tests in `tests/test_email_poll.py`.

### Running the project
```bash
pip install -e .            # editable install (includes all non-ML deps)
pip install -e ".[ml]"      # install ML dependencies (sentence-transformers, faiss-cpu)
pip install -e ".[dev]"     # install dev tools (pytest, ruff)
pytest                      # unit tests (integration tests need raw/ CSVs)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
cellarbrain -c cellarbrain.toml etl ...  # use custom config
cellarbrain mcp                # start MCP server (reads CELLARBRAIN_CONFIG env var)
cellarbrain recalc             # recompute calculated fields from existing Parquet
cellarbrain ingest             # start IMAP polling daemon
cellarbrain ingest --once      # single poll cycle, then exit
cellarbrain train-model        # fine-tune the sommelier pairing model (~3-5 min CPU, needs [ml])
cellarbrain rebuild-indexes    # build FAISS food + wine indexes (needs [ml])
```

## Memory System

The workspace includes a self-learning memory system that captures lessons across conversations.

### When to Write Memories

Write a memory file in `.memories/` when:
- You make a mistake and the user corrects you
- You discover an efficiency trick or shortcut
- The user provides explicit guidance or preferences
- You observe unexpected tool/API behavior
- You identify a recurring pattern worth codifying

### Format

Filename: `YYYY-MM-DD_<category>_<short-slug>.md` (see `.github/instructions/memory-management.instructions.md` for details).

Frontmatter must include `severity: low|medium|high`.

### Reading Rule

Before non-trivial tasks, scan `.memories/INDEX.md` (if it exists) or `.memories/` filenames for relevant lessons. Only read full content when a filename is clearly relevant.

### Git Safety

- **Never** commit `.memories/` — refuse any `git add` or `git add -f` targeting memory files
- Before commits, verify with `git status` that no `.memories/` paths are staged
- If `.memories/` is missing from `.gitignore`, add it before proceeding

### Secrets

Never write secrets, tokens, passwords, or API keys to memory files. Redact or skip entirely.

### Bootstrap

On first use in a fresh clone: create `.memories/` + `.gitkeep` if missing, and verify `.gitignore` coverage.

### Dreaming

When 10+ memories accumulate, mention the `/dream` prompt to the user. The dream cycle consolidates lessons into permanent `.github/` rules — see `.github/prompts/dream.prompt.md`.
