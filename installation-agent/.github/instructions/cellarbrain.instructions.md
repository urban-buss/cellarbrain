---
description: "Use when installing, testing, or troubleshooting the cellarbrain PyPI package. Covers ETL setup, MCP server testing, dashboard, and email ingest module."
applyTo: "**"
---
# Cellarbrain PyPI Test Workspace

This workspace is used to test the `cellarbrain` package installed from PyPI (not editable/local dev).

## Installation

- Python: use `py -3` (Windows Python launcher, Python 3.14)
- Install command: `py -3 -m pip install "cellarbrain[dashboard,dev,ingest]"`
- Always verify version after install: `py -3 -m cellarbrain --version`
- If a stale editable install conflicts, use `--force-reinstall --no-deps` first

## ETL

- Raw CSV exports live in `raw-files/`:
  - `export-wines.csv`
  - `export-bottles-stored.csv`
  - `export-bottles-gone.csv`
- Run ETL: `py -3 -m cellarbrain -d output etl "raw-files/export-wines.csv" "raw-files/export-bottles-stored.csv" "raw-files/export-bottles-gone.csv"`
- Output Parquet files go to `output/`

## MCP Server

- Start: `py -3 -m cellarbrain -d output mcp --transport sse --port 8765`
- Test with Python MCP client (sse_client → ClientSession → list_tools)
- Expect 28 tools in v0.2.1

## Email Ingest

- Extra: `cellarbrain[ingest]` (adds imapclient, keyring)
- Dry-run test: `py -3 -m cellarbrain -d output ingest --once --dry-run`
- Requires IMAP creds via keyring or env vars `CELLARBRAIN_IMAP_USER` / `CELLARBRAIN_IMAP_PASSWORD`

## Dashboard

- Start: `py -3 -m cellarbrain -d output dashboard`
- Requires the `[dashboard]` extra (starlette, uvicorn, jinja2)
