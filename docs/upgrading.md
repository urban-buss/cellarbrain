# Upgrading to v0.3.0

This guide covers migrating from cellarbrain v0.2.x to v0.3.0.

## Breaking Changes

### 1. Dependency Consolidation

All features are now included in the base install. The separate extras
(`research`, `dashboard`, `ingest`, `promotions`, `search`) have been
absorbed into core dependencies.

**Before (v0.2.x):**
```bash
pip install "cellarbrain[dashboard,ingest,research]"
```

**After (v0.3.0):**
```bash
pip install cellarbrain
```

The only remaining optional extra is `[ml]` for the AI food-wine pairing
model (adds `sentence-transformers`, `faiss-cpu`, `datasets`, `accelerate`).

Legacy extra names (`sommelier`, `research`, `dashboard`, `ingest`,
`promotions`, `search`) are retained as empty aliases — existing
`pip install` commands will not break, they simply install nothing extra.

**Disk impact:** The base install is now ~40 MB larger than the v0.2.x
core-only install due to the absorbed dashboard, ingest, and research
dependencies.

### 2. MCP Tool Decorator Change

Tools are now registered via an internal `_tool()` wrapper instead of
`@mcp.tool()` directly. This adds structured logging, cache integration,
and `ToolResponse` support. The wire protocol is unchanged — MCP clients
see the same tool names and parameters.

### 3. Dossier Research Metadata

Agent-written dossier sections may now include HTML comments with
research metadata (source URLs, confidence scores, timestamps):

```markdown
<!-- research-meta: {"source": "...", "confidence": 0.9} -->
```

Older tools that parse dossiers as plain Markdown will see these as
invisible comments. No action needed unless you strip comments.

### 4. Auto-Migration on ETL

Running `cellarbrain etl` now automatically applies pending schema
migrations before processing. Use `--no-migrate` to skip this if you
prefer to run migrations manually:

```bash
cellarbrain etl --no-migrate raw/*.csv -o output
```

### 5. Observability Schema Expansion

The DuckDB log store gains three columns: `data_size` (INTEGER),
`metadata_keys` (VARCHAR), and `cache_hit` (BOOLEAN). These are added
automatically via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` on first
write after upgrade. No manual action needed.

### 6. Search Fallback Chain

`find_wine` now implements a three-tier fallback:
1. Exact/fuzzy text match
2. Phonetic matching (Double Metaphone)
3. Suggestion generation

This is transparent to callers but changes result ordering when exact
matches are not found.

## Upgrade Steps

1. **Update the package:**
   ```bash
   pip install --upgrade cellarbrain
   ```

2. **Re-run ETL** to apply schema migrations:
   ```bash
   cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
   ```

3. **Verify:**
   ```bash
   cellarbrain --version   # should show 0.3.x
   cellarbrain validate    # checks Parquet integrity
   ```

4. **Clean up extras** (optional): Remove now-empty extras from your
   `requirements.txt` or `pyproject.toml`:
   ```diff
   - cellarbrain[dashboard,ingest,research]
   + cellarbrain
   ```

## Configuration Changes

### New TOML Sections

| Section | Purpose | Default |
|---------|---------|---------|
| `[cache]` | Query cache settings | `enabled = true`, `max_size = 128`, `ttl_seconds = 300` |
| `[recommend]` | Recommendation engine tuning | See settings reference |
| `[dinner]` | Dinner-tonight feature | See settings reference |
| `[gifting]` | Gift recommendation params | See settings reference |
| `[anomaly]` | Anomaly detection thresholds | See settings reference |
| `[retailer]` | Price tracking retailer list | See settings reference |
| `[promotions]` | Promotion scanning config | See settings reference |

### Removed Settings

None. All v0.2.x settings remain valid.

## Rollback

If you need to revert to v0.2.x:

```bash
pip install cellarbrain==0.2.15
cellarbrain etl ...   # re-run to regenerate Parquet with v0.2 schema
```

Dossier files are forward-compatible — v0.2.x will ignore research-meta
comments. Parquet files written by v0.3 may have extra columns that v0.2
does not recognise; re-running ETL on v0.2 regenerates them cleanly.
