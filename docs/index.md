# Cellarbrain Knowledge Base

Reference documentation for the Cellarbrain wine-cellar toolkit. Each page is standalone — read only the pages relevant to your task.

## Topic Index

| Topic | File | When to read |
|-------|------|-------------|
| Setup guide | [setup-guide.md](setup-guide.md) | Installing, configuring, and running Cellarbrain for the first time |
| System architecture | [architecture.md](architecture.md) | Understanding the pipeline and module responsibilities |
| Entity model & schemas | [entity-model.md](entity-model.md) | Adding columns, checking types, understanding relationships |
| Field mapping (CSV → target) | [field-mapping.md](field-mapping.md) | Adding fields, fixing parsers, understanding source→target transforms |
| ETL pipeline | [etl-pipeline.md](etl-pipeline.md) | Debugging ETL, understanding incremental sync, ID stabilisation |
| Wine dossiers (ownership) | [dossier-system.md](dossier-system.md) | Working with agent/ETL-owned sections, dossier lifecycle |
| Dossier file format | [dossier-format.md](dossier-format.md) | File naming, section layout, frontmatter schema, token budgets |
| DuckDB query layer | [query-layer.md](query-layer.md) | Writing SQL, understanding views, price tracking queries |
| MCP tool catalogue | [mcp-tools.md](mcp-tools.md) | Using or adding MCP tools, checking parameters & return shapes |
| Agent architecture | [agent-architecture.md](agent-architecture.md) | MCP server design, agent skills, connection model |
| CLI reference | [cli-reference.md](cli-reference.md) | Subcommand usage, flags, and examples |
| Error reference | [error-reference.md](error-reference.md) | Every error message, its cause, and how to fix it |
| Computed properties | [computed-properties.md](computed-properties.md) | Understanding derived fields: full_name, drinking_status, etc. |
| Settings reference | [settings-reference.md](settings-reference.md) | Configuring cellarbrain.toml, checking defaults & precedence |
| Design decisions | [decisions/](decisions/) | Understanding *why* things are built a certain way |

## Design Decisions (ADRs)

| ADR | Title |
|-----|-------|
| [001](decisions/001-parquet-over-sqlite.md) | Parquet over SQLite |
| [002](decisions/002-soft-delete-tombstones.md) | Soft-delete tombstones |
| [003](decisions/003-natural-key-id-stabilisation.md) | Natural key ID stabilisation |
| [004](decisions/004-dossier-ownership-fences.md) | Dossier ownership fences |
| [005](decisions/005-tracked-wine-identity.md) | Tracked wine identity |
| [006](decisions/006-year-partitioned-price-observations.md) | Year-partitioned price observations |
| [007](decisions/007-currency-normalisation-at-etl.md) | Currency normalisation at ETL |
| [008](decisions/008-winery-rename-heuristic.md) | Winery rename heuristic |
| [009](decisions/009-slug-based-wine-id-stabilisation.md) | Slug-based wine ID stabilisation |

## Conventions

Coding conventions, testing patterns, and common tasks remain in [`.github/copilot-instructions.md`](../.github/copilot-instructions.md). This knowledge base covers architecture and design only.

## Detailed Implementation Reference

For deeper implementation specs not covered here — source CSV field-by-field analysis (23 files), per-entity target schema details, and original design proposals — see [`.docs/`](../.docs/). That directory is a complementary reference for deep debugging and feature planning.
