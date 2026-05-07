---
description: "Install or update cellarbrain from PyPI with all extras, run ETL on raw files, and verify the MCP server"
agent: "agent"
---
Install the latest `cellarbrain` package from PyPI with all extras. Follow these steps:

## 1. Install / Update

```
py -3 -m pip install "cellarbrain[dashboard,dev,ingest]" --upgrade
```

If a stale editable (local dev) install is detected (e.g. version from `c:\repos\vinocell\src`), force-reinstall first:

```
py -3 -m pip install "cellarbrain[dashboard,dev,ingest]" --force-reinstall --no-deps
py -3 -m pip install "cellarbrain[dashboard,dev,ingest]"
```

Verify the installed version:

```
py -3 -m cellarbrain --version
```

## 2. Run ETL

Use the raw CSV files in `raw-files/`:

```
py -3 -m cellarbrain -d output etl "raw-files/export-wines.csv" "raw-files/export-bottles-stored.csv" "raw-files/export-bottles-gone.csv"
```

Confirm Parquet files appear in `output/` and all FK validations pass.

## 3. Test MCP Server

Start the MCP server on SSE transport:

```
py -3 -m cellarbrain -d output mcp --transport sse --port 8765
```

Then connect with a Python MCP client and list tools:

```python
import asyncio
from mcp.client.sse import sse_client
from mcp import ClientSession

async def main():
    async with sse_client('http://127.0.0.1:8000/sse') as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            tools = await s.list_tools()
            for t in tools.tools:
                print(f'  {t.name}')
            print(f'\nTotal tools: {len(tools.tools)}')

asyncio.run(main())
```

Report the total tool count and confirm the server is responding.

## 4. Test Email Ingest (dry-run)

```
py -3 -m cellarbrain -d output ingest --once --dry-run
```

Expect a credential error (no IMAP configured) — this confirms the module loads correctly.

## 5. Install OpenClaw Skills (optional)

```
py -3 -m cellarbrain install-skills
```

This copies the bundled skill files to `~/.openclaw/skills/cellarbrain/`. Verify the skills directory was populated with 8 subdirectories.
