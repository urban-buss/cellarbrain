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
pip install -e "."
```

See [Local Setup](../development/local-setup.md) for the full development guide.

## Optional Extras

The only optional extra is `ml` for the AI food-wine pairing model:

```bash
pip install "cellarbrain[ml]"
```

| Extra | What it adds | Dependencies |
|-------|-------------|--------------|
| `ml` | AI food-wine pairing | `sentence-transformers`, `faiss-cpu`, `datasets`, `accelerate` |

All other features (web dashboard, email ingestion, web research, phonetic search) are included in the base install.

> **Note (v0.3+):** Legacy extra names (`sommelier`, `research`, `dashboard`,
> `ingest`, `promotions`, `search`) are retained as empty aliases for backward
> compatibility. They install nothing additional — all functionality is in the
> base package. See [Upgrading](../../docs/upgrading.md) for migration details.

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
