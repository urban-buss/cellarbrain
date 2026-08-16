# Cellarbrain Development Instructions

## Scope and architecture

This repository is the primary development target in the `brains` monorepo.
It is a Python 3.12 application with these major areas:

- ingestion and ETL from Vinocell exports into Parquet and DuckDB;
- query, dossier, Markdown, and validation layers;
- an MCP server and structured MCP response contracts;
- a Starlette/HTMX dashboard;
- promotion ingestion and matching;
- optional sommelier/ML features.

Read the parent `/Users/ruedi/dev/brains/AGENTS.md` first. Its production
boundaries are mandatory.

## Isolated development runtime

Work only from this checkout and use its local environment:

```bash
cd /Users/ruedi/dev/brains/cellarbrain
source ./dev-shell.zsh
uv run cellarbrain --help
```

Use `.venv`, `cellarbrain.local.toml`, `.env.dev`, `raw/dev`, `output_dev`,
and `data/dev`. Never use `/Users/ruedi/cellarbrain-data`, the production
configuration exported by the login shell, or production ports.

The dashboard development port is `8081`. PostgreSQL development, when a
feature actually needs it, is `127.0.0.1:5433`. Codex must not operate Docker;
give the user an exact Compose command when container work is required.

## Engineering invariants

- Preserve CLI and MCP response compatibility unless a change explicitly
  includes a migration plan.
- Keep transformations deterministic and testable with synthetic fixtures.
- Never copy production cellar data into tests or committed fixtures.
- Treat schemas, normalized identifiers, units, dates, currency, and null
  semantics as contracts. Review downstream query and dashboard consumers
  before changing them.
- Keep DuckDB and Parquet writes inside development paths and prefer atomic
  replacement where supported.
- Keep optional ML dependencies and behavior isolated from the core runtime.
- Do not silently swallow malformed source data; preserve useful diagnostics.

## Verification map

Run the narrowest relevant tests first:

- ETL/schema: parser, transform, writer, validation, and query tests.
- MCP: MCP server, response, dossier, and contract tests.
- Dashboard: route/view tests plus a local smoke check on `127.0.0.1:8081`.
- Promotions: parser, normalization, matching, and MCP/presentation tests.
- Sommelier: focused unit tests; do not download models without approval.

Before handoff, normally run:

```bash
uv run ruff check src tests
uv run pytest -q
```

Compare failures with the documented baseline in the parent instructions.
Do not repair unrelated baseline failures as part of a focused change.

## Change discipline

Inspect callers and tests before editing. For schema or MCP changes, document
the compatibility impact. For UI changes, verify both successful and empty or
missing-data states. Commit this submodule first; update the parent submodule
pointer only when the user asks for that integration step.
