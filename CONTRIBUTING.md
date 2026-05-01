# Contributing to Cellarbrain

Thanks for your interest! Here's how to get started.

## Setup

```bash
git clone https://github.com/urban-buss/cellarbrain
cd cellarbrain
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[research]" pytest
```

## Running tests

```bash
pytest
```

### Smoke tests

The ETL smoke pipeline exercises the full load → sync → verify → MCP cycle.
Always invoke it with the **venv Python** (not the system `py` launcher):

```bash
python -m tests.smoke_helpers          # with venv activated
# or explicitly:
.venv/Scripts/python -m tests.smoke_helpers   # Windows
.venv/bin/python -m tests.smoke_helpers       # macOS/Linux
```

## Submitting changes

1. Fork the repo and create a feature branch from `main`.
2. Make your changes and add/update tests as needed.
3. Run `pytest` and ensure all tests pass.
4. Open a pull request with a clear description of the change.

## Code style

- Python 3.11+, type hints encouraged.
- Keep functions focused; follow existing patterns in `src/cellarbrain/`.
- No additional linter configuration is required at this time.

## Reporting issues

Open an issue on GitHub with steps to reproduce. Include your Python version
and OS.
