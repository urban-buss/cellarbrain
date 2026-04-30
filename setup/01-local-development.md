# Local Development Setup

Step-by-step guide to set up Cellarbrain for local development on macOS (Mac Mini M4) using VS Code.

---

## 1. Prerequisites

### 1.1 Install Homebrew

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

After installation, follow the on-screen instructions to add Homebrew to your PATH:

```bash
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
eval "$(/opt/homebrew/bin/brew shellenv)"
```

Verify:

```bash
brew --version
# Homebrew 4.x.x
```

### 1.2 Install Python 3.13

```bash
brew install python@3.13
```

Verify:

```bash
python3 --version
# Python 3.13.x
```

> **Note:** Cellarbrain requires Python 3.11+. Python 3.13 is recommended for Apple Silicon performance on M4.

### 1.3 Install Git

```bash
brew install git
```

### 1.4 Install VS Code

```bash
brew install --cask visual-studio-code
```

### 1.5 Install VS Code Extensions

Open VS Code and install these extensions (or use the CLI):

```bash
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension GitHub.copilot
code --install-extension GitHub.copilot-chat
```

| Extension | Purpose |
|-----------|---------|
| Python | Python language support, debugging, test runner |
| Pylance | Type checking, IntelliSense, go-to-definition |
| GitHub Copilot | AI code completion |
| GitHub Copilot Chat | AI chat with MCP tool access |

---

## 2. Clone the Repository

```bash
cd ~/repos  # or your preferred projects directory
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain
```

---

## 3. Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Your shell prompt should now show `(.venv)`.

> **Tip:** Add this to your `~/.zshrc` for automatic activation when entering the project:
> ```bash
> # Auto-activate venv when entering cellarbrain directory
> cd() { builtin cd "$@" && [ -f .venv/bin/activate ] && source .venv/bin/activate; }
> ```

Verify the Python path points to the venv:

```bash
which python3
# /Users/<you>/repos/cellarbrain/.venv/bin/python3
```

---

## 4. Install Dependencies

### 4.1 Core Only (minimal)

```bash
pip install -e .
```

Installs: `pyarrow`, `duckdb`, `pandas`, `tabulate`, `mcp[cli]`

### 4.2 With Research (web-based wine research)

```bash
pip install -e ".[research]"
```

Adds: `httpx`

### 4.3 With Sommelier ML (food-wine pairing model)

```bash
pip install -e ".[sommelier]"
```

Adds: `sentence-transformers`, `faiss-cpu`, `datasets`, `accelerate`

> **Note:** On M4 with 24 GB RAM, the sommelier model trains comfortably in ~3–5 minutes using CPU. No GPU configuration needed.

### 4.4 With Dashboard (web explorer)

```bash
pip install -e ".[dashboard]"
```

Adds: `starlette`, `uvicorn[standard]`, `jinja2`, `markdown`, `pyyaml`

### 4.5 With Email Ingestion

```bash
pip install -e ".[ingest]"
```

Adds: `imapclient`, `keyring`

### 4.6 Everything (recommended for development)

```bash
pip install -e ".[research,sommelier,dashboard,ingest]"
```

### 4.7 Test Dependencies

```bash
pip install pytest
```

### 4.8 Verify Installation

```bash
cellarbrain --help
```

Expected output:

```
usage: cellarbrain [-h] [-c CONFIG] [-d DATA_DIR] [-v] [-q] [--log-file LOG_FILE]
                   {etl,validate,query,stats,dossier,mcp,recalc,wishlist,train-model,
                    retrain-model,rebuild-indexes,logs,dashboard,ingest} ...

Cellarbrain wine cellar toolkit — ETL, query, and agent interface.
```

---

## 5. VS Code Workspace Configuration

### 5.1 Open the Workspace

```bash
code cellarbrain.code-workspace
```

Or from VS Code: **File → Open Workspace from File…** → select `cellarbrain.code-workspace`.

### 5.2 Select Python Interpreter

1. Press `Cmd+Shift+P` → "Python: Select Interpreter"
2. Choose `.venv/bin/python` (should appear at the top)

### 5.3 Workspace Settings (`.vscode/settings.json`)

The repository includes pre-configured settings:

```json
{
    "python.testing.pytestArgs": ["tests"],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```

This enables the VS Code test explorer with pytest automatically.

### 5.4 MCP Server for Copilot (`.vscode/mcp.json`)

The workspace includes MCP server configuration for GitHub Copilot Chat:

```json
{
  "servers": {
    "cellarbrain": {
      "command": "${command:python.interpreterPath}",
      "args": ["-m", "cellarbrain", "-d", "${workspaceFolder}/output", "mcp"],
      "env": {}
    }
  }
}
```

This allows Copilot Chat to use cellarbrain MCP tools directly in VS Code. Prerequisites:
- ETL must have been run at least once (so `output/` contains Parquet data)
- The Python interpreter must be set to the project's `.venv`

---

## 6. Project Structure

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
│   ├── test_parsers.py        # Parser tests
│   ├── test_transform.py      # Transform builder tests
│   ├── test_writer.py         # Writer + schema tests
│   ├── test_query.py          # Query layer tests
│   ├── test_dossier_ops.py    # Dossier operations tests
│   ├── test_mcp_server.py     # MCP tool tests
│   ├── test_integration.py    # Full pipeline integration tests
│   └── smoke_helpers/         # Smoke test infrastructure
├── docs/                      # Reference documentation
├── models/sommelier/          # ML model artefacts (trained weights, indexes)
├── raw/                       # Vinocell CSV exports (input data)
├── output/                    # Generated output (Parquet, dossiers, logs)
├── .openclaw/                 # OpenClaw skill definitions
├── .github/                   # GitHub workflows, agent definitions, instructions
├── .vscode/                   # VS Code workspace configuration
├── cellarbrain.toml           # Default configuration (checked in)
├── cellarbrain.local.toml     # Machine-specific overrides (gitignored)
├── pyproject.toml             # Package metadata, build config, dependencies
└── cellarbrain.code-workspace # VS Code workspace file
```

---

## 7. Configuration

### 7.1 Configuration Precedence (highest wins)

1. **CLI arguments** — `--data-dir`, `--config`
2. **Environment variables** — `CELLARBRAIN_DATA_DIR`, `CELLARBRAIN_CONFIG`
3. **`cellarbrain.local.toml`** — machine-specific overrides (gitignored)
4. **`cellarbrain.toml`** — project defaults (checked in)
5. **Built-in defaults** — in `settings.py` dataclasses

### 7.2 Create Local Overrides

Create `cellarbrain.local.toml` at the project root (this file is gitignored):

```toml
# cellarbrain.local.toml — machine-specific overrides (Mac Mini)

[paths]
data_dir = "output"
raw_dir = "raw"

[logging]
level = "INFO"
log_file = "output/logs/cellarbrain.log"
format = "json"
```

### 7.3 Key Configuration Sections

| Section | Purpose | Reference |
|---------|---------|-----------|
| `[paths]` | Data and output directories | [settings-reference.md](../docs/settings-reference.md) |
| `[csv]` | CSV encoding/delimiter | [settings-reference.md](../docs/settings-reference.md) |
| `[currency]` | Default currency + exchange rates | [settings-reference.md](../docs/settings-reference.md) |
| `[[cellar_rules]]` | Cellar classification (onsite/offsite/in-transit) | [settings-reference.md](../docs/settings-reference.md) |
| `[[price_tiers]]` | Price tier boundaries | [settings-reference.md](../docs/settings-reference.md) |
| `[sommelier]` | ML model paths and training params | [settings-reference.md](../docs/settings-reference.md) |
| `[logging]` | Log level, file, format, rotation | [settings-reference.md](../docs/settings-reference.md) |
| `[dashboard]` | Web dashboard port and permissions | [settings-reference.md](../docs/settings-reference.md) |
| `[ingest]` | IMAP polling configuration | [settings-reference.md](../docs/settings-reference.md) |

---

## 8. Prepare Test Data

### 8.1 Vinocell CSV Exports

Export from the Vinocell app (macOS): **File → Export → CSV**.

Place the three files in the `raw/` directory:

```
raw/
├── export-wines.csv
├── export-bottles-stored.csv
└── export-bottles-gone.csv
```

These files are UTF-16 LE encoded, tab-delimited — the standard Vinocell export format.

### 8.2 Run Initial ETL

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

### 8.3 Validate Output

```bash
cellarbrain validate
```

---

## 9. Verify Setup

Run the complete verification sequence:

```bash
# 1. Check CLI is available
cellarbrain --help

# 2. Run unit tests
pytest tests/ -v --ignore=tests/test_integration.py

# 3. Run ETL (if CSV files available)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# 4. Validate output
cellarbrain validate

# 5. Check stats
cellarbrain stats

# 6. Start MCP server (Ctrl+C to stop)
cellarbrain mcp
```

If all steps complete without errors, your development environment is ready.

---

## 10. Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `python3: command not found` | Homebrew Python not in PATH | Run `eval "$(/opt/homebrew/bin/brew shellenv)"` |
| `ModuleNotFoundError: cellarbrain` | Package not installed | Run `pip install -e .` with venv active |
| `No module named 'sentence_transformers'` | Sommelier extra not installed | Run `pip install -e ".[sommelier]"` |
| `FileNotFoundError` on ETL | Wrong CSV paths | Verify files exist in `raw/` with correct names |
| `UnicodeDecodeError` | Wrong CSV encoding | Check `[csv] encoding` in `cellarbrain.toml` |
| VS Code can't find tests | Wrong interpreter | `Cmd+Shift+P` → "Python: Select Interpreter" → `.venv/bin/python` |
| MCP tools not available in Copilot | No Parquet data | Run ETL first, then reload VS Code |

---

## Next Steps

- [Building & Testing](02-building-and-testing.md) — Run tests, build packages
- [Installation & Running](04-installation-and-running.md) — Run all modules in detail
- [Debugging & Monitoring](05-debugging-and-monitoring.md) — Debug and observe the system
