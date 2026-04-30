# Setup, Deployment & Installation

Comprehensive step-by-step guides for developing, deploying, and operating Cellarbrain on **macOS** (Mac Mini M4, 24 GB RAM).

## Target Platform

| Spec | Value |
|------|-------|
| Hardware | Mac Mini M4, 24 GB unified memory |
| OS | macOS 15 (Sequoia) or later |
| Python | 3.11+ (recommended: 3.13 via Homebrew) |
| Shell | zsh (default macOS shell) |

## Prerequisites

Before starting any guide below, ensure you have:

- **Homebrew** — `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
- **Python 3.13** — `brew install python@3.13`
- **Git** — `brew install git`
- **VS Code** — `brew install --cask visual-studio-code`

## Guides

| # | Guide | Description |
|---|-------|-------------|
| 1 | [Local Development](01-local-development.md) | Clone, virtual environment, editable install, VS Code configuration, project structure |
| 2 | [Building & Testing](02-building-and-testing.md) | Run tests (unit/integration/smoke), build distribution packages, validate ETL output |
| 3 | [Publishing](03-publishing.md) | Version bumping, PyPI publishing (automated + manual), Homebrew tap creation |
| 4 | [Installation & Running](04-installation-and-running.md) | Install from pip/Homebrew/source, run every module step-by-step, launchd services |
| 5 | [Debugging & Monitoring](05-debugging-and-monitoring.md) | Logging configuration, observability system, CLI log queries, web dashboard, VS Code debugging |
| 6 | [OpenClaw Integration](06-openclaw-integration.md) | MCP server config for OpenClaw, available skills, sommelier model setup, testing |

## Quick Start (5 minutes)

For the impatient — get from zero to a working cellar in 5 commands:

```bash
# 1. Clone and enter
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain

# 2. Create venv and install
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[research,sommelier,dashboard,ingest]"

# 3. Run ETL (place your Vinocell CSVs in raw/ first)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# 4. Verify
cellarbrain validate && cellarbrain stats

# 5. Start MCP server (for AI agent use)
cellarbrain mcp
```

## Related Documentation

These guides expand on the existing reference docs:

- [docs/setup-guide.md](../docs/setup-guide.md) — Condensed setup reference
- [docs/cli-reference.md](../docs/cli-reference.md) — Full CLI command reference
- [docs/settings-reference.md](../docs/settings-reference.md) — All configuration fields
- [docs/architecture.md](../docs/architecture.md) — System architecture overview
- [docs/agent-architecture.md](../docs/agent-architecture.md) — MCP server and agent design
