# Local Development Setup

Set up Cellarbrain for local development with VS Code.

## Clone the Repository

```bash
cd ~/repos  # or your preferred projects directory
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain
```

## Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows
```

Verify the Python path points to the venv:

```bash
which python3
# /Users/<you>/repos/cellarbrain/.venv/bin/python3
```

> **Tip (macOS):** Add to `~/.zshrc` for automatic activation:
> ```bash
> cd() { builtin cd "$@" && [ -f .venv/bin/activate ] && source .venv/bin/activate; }
> ```

## Install Dependencies

```bash
# Everything (recommended for development)
pip install -e ".[research,sommelier,dashboard,ingest]"

# Test runner
pip install pytest
```

See [Installation](../getting-started/installation.md) for individual extras.

## VS Code Workspace

```bash
code cellarbrain.code-workspace
```

Or: **File → Open Workspace from File…** → select `cellarbrain.code-workspace`.

### Select Python Interpreter

1. Press `Cmd+Shift+P` (macOS) / `Ctrl+Shift+P` (Windows) → "Python: Select Interpreter"
2. Choose `.venv/bin/python` (should appear at the top)

### Workspace Settings

The repository includes pre-configured settings in `.vscode/settings.json`:

```json
{
    "python.testing.pytestArgs": ["tests"],
    "python.testing.unittestEnabled": false,
    "python.testing.pytestEnabled": true
}
```

This enables the VS Code test explorer with pytest automatically.

## Verify Setup

```bash
# 1. Check CLI is available
cellarbrain --help

# 2. Run unit tests
pytest tests/ -v --ignore=tests/test_integration.py

# 3. Run ETL (if CSV files available in raw/)
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output

# 4. Validate output
cellarbrain validate

# 5. Check stats
cellarbrain stats
```

## Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `python3: command not found` | Python not in PATH | macOS: `eval "$(/opt/homebrew/bin/brew shellenv)"` |
| `ModuleNotFoundError: cellarbrain` | Package not installed | Run `pip install -e .` with venv active |
| `No module named 'sentence_transformers'` | Sommelier extra missing | `pip install -e ".[sommelier]"` |
| `FileNotFoundError` on ETL | Wrong CSV paths | Verify files exist in `raw/` |
| VS Code can't find tests | Wrong interpreter | Select `.venv/bin/python` as interpreter |
| MCP tools not available in Copilot | No Parquet data | Run ETL first, then reload VS Code |

## Next Steps

- [Project Structure](project-structure.md) — Source tree overview
- [Testing](testing.md) — Run and write tests
- [Configuration](../configuration/overview.md) — TOML settings
