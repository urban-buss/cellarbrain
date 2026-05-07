# Installation

Three ways to install Cellarbrain: PyPI (users), Homebrew (macOS users), or source (developers).

## Option A: Install from PyPI (recommended)

```bash
python3 --version  # ensure 3.11+
pip install cellarbrain
```

> **Note:** The PyPI package does not include the test suite. For development, use Option C.

## Option B: Install from Homebrew (when available)

```bash
brew tap urban-buss/cellarbrain
brew install cellarbrain
```

The Homebrew formula includes all optional modules — no extras to install.

## Option C: Install from Source (developers)

```bash
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain
python3 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows
pip install -e ".[research,sommelier,dashboard,ingest]"
```

See [Local Setup](../development/local-setup.md) for the full development guide.

## Optional Extras

Install individual extras or all at once:

```bash
pip install "cellarbrain[research,sommelier,dashboard,ingest]"
```

| Extra | What it adds | Dependencies |
|-------|-------------|--------------|
| `research` | Web-based wine research | `httpx` |
| `sommelier` | ML food-wine pairing | `sentence-transformers`, `faiss-cpu`, `datasets`, `accelerate` |
| `dashboard` | Web UI explorer | `starlette`, `uvicorn[standard]`, `jinja2`, `markdown`, `pyyaml` |
| `ingest` | IMAP email polling | `imapclient`, `keyring` |

Install individual extras:

```bash
pip install "cellarbrain[research]"
pip install "cellarbrain[sommelier]"
pip install "cellarbrain[dashboard]"
pip install "cellarbrain[ingest]"
```

## Verify

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

## Next Steps

- [Quick Start](quick-start.md) — Run ETL and verify
- [Configuration](../configuration/overview.md) — TOML settings, env vars
- [ETL](../modules/etl.md) — Prepare data and run the pipeline
