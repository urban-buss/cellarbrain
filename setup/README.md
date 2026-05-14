# Setup Wiki

Cellarbrain setup, development, deployment, and operations documentation.

## Quick Start

```bash
git clone https://github.com/urban-buss/cellarbrain.git && cd cellarbrain
python3 -m venv .venv && source .venv/bin/activate
pip install -e "."
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
cellarbrain validate && cellarbrain stats
```

See [Quick Start](getting-started/quick-start.md) for the full walkthrough.

---

## Getting Started

| Page | Description |
|------|-------------|
| [Quick Start](getting-started/quick-start.md) | Zero to working cellar in 5 commands |
| [Prerequisites](getting-started/prerequisites.md) | Platform requirements and tool installation |
| [Installation](getting-started/installation.md) | Install from PyPI, Homebrew, or source |

## Development

| Page | Description |
|------|-------------|
| [Local Setup](development/local-setup.md) | Clone, venv, VS Code workspace |
| [Project Structure](development/project-structure.md) | Annotated source tree |
| [Testing](development/testing.md) | Run tests, write tests, smoke testing |
| [Building](development/building.md) | Build sdist and wheel packages |

## Configuration

| Page | Description |
|------|-------------|
| [Overview](configuration/overview.md) | TOML config, precedence, env vars, all sections |

## Modules

| Page | Description |
|------|-------------|
| [ETL Pipeline](modules/etl.md) | CSV → Parquet → dossiers, full/sync/recalc |
| [MCP Server](modules/mcp-server.md) | Transports, client configs, tools reference |
| [CLI](modules/cli.md) | Stats, query, dossier, wishlist commands |
| [Dashboard](modules/dashboard.md) | Web UI for cellar browsing and observability |
| [Sommelier](modules/sommelier.md) | ML food-wine pairing model and indexes |
| [Email Ingest](modules/email-ingest.md) | IMAP polling, credentials, launchd daemon |
| [Agent Skills](modules/agent-skills.md) | Skill architecture and available skills |
| [iOS Prompt-Book](modules/ios-prompt-book.md) | Apple Notes, Text Replacements, Shortcuts for iMessage |

> **Service management:** Use `cellarbrain service install` to register daemons (ingest, dashboard) as macOS launchd services. See [CLI reference](../docs/cli-reference.md#cellarbrain-service).

## Operations

| Page | Description |
|------|-------------|
| [Logging](operations/logging.md) | Text and JSON logging configuration |
| [Observability](operations/observability.md) | EventCollector, DuckDB log store, CLI queries |
| [Health Monitoring](operations/health-monitoring.md) | Daily checks for production |
| [VS Code Debugging](operations/vscode-debugging.md) | Debug configurations and tips |
| [MCP Testing](operations/mcp-testing.md) | Verify and test the MCP server |

## Publishing

| Page | Description |
|------|-------------|
| [Release Process](publishing/release-process.md) | Version, tag, publish workflow |
| [PyPI](publishing/pypi.md) | Automated and manual PyPI publishing |
| [Homebrew](publishing/homebrew.md) | Homebrew tap and formula |

## Reference

| Page | Description |
|------|-------------|
| [Fresh Install Validation](reference/fresh-install-validation.md) | Agent prompt for post-install QA |
| [launchd Template](reference/launchd-template.md) | macOS launchd plist for ingest daemon |

---

## Related Docs

- [docs/cli-reference.md](../docs/cli-reference.md) — Full CLI command reference
- [docs/settings-reference.md](../docs/settings-reference.md) — All configuration fields
- [docs/architecture.md](../docs/architecture.md) — System architecture
- [docs/entity-model.md](../docs/entity-model.md) — 14-table data model
- [docs/mcp-tools.md](../docs/mcp-tools.md) — MCP tools and resources

---

## Legacy Docs

The original monolithic guides are archived in [`_archive/`](_archive/). They are superseded by the pages above.
