# OpenClaw Integration

How to configure and use Cellarbrain with OpenClaw (or any MCP-compatible AI agent host).

---

## 1. What is OpenClaw?

OpenClaw is an AI agent platform that connects to tools via the Model Context Protocol (MCP). Cellarbrain exposes its wine cellar data layer as an MCP server, allowing OpenClaw agents to:

- Query the cellar database (SQL, search, statistics)
- Read and update wine dossiers
- Track prices and monitor wishlist alerts
- Perform semantic food-wine pairing (with trained sommelier model)

The integration is **tool-only** — all reasoning, recommendations, and research synthesis happen in the agent (LLM side), not in the MCP server.

---

## 2. Prerequisites

Before connecting OpenClaw to Cellarbrain:

### 2.1 Install Cellarbrain

```bash
# Option A: From PyPI
pip install "cellarbrain[research,sommelier]"

# Option B: From source
git clone https://github.com/urban-buss/cellarbrain.git
cd cellarbrain
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[research,sommelier]"
```

### 2.2 Run ETL At Least Once

```bash
cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
```

### 2.3 Verify the MCP Server Starts

```bash
# Should produce no output (waiting for JSON-RPC on stdin)
# Press Ctrl+C to stop
cellarbrain mcp
```

### 2.4 (Optional) Train Sommelier Model

For semantic food-wine pairing:

```bash
pip install "cellarbrain[sommelier]"
cellarbrain train-model
cellarbrain rebuild-indexes
```

---

## 3. MCP Server Configuration

### 3.1 OpenClaw Configuration

Add to your `openclaw.json` (or equivalent MCP client config):

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["-d", "/Users/<you>/repos/cellarbrain/output", "mcp"],
      "env": {}
    }
  }
}
```

### 3.2 With Virtualenv Path

If cellarbrain is installed in a virtualenv (not globally):

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain",
      "args": ["-d", "/Users/<you>/repos/cellarbrain/output", "mcp"],
      "env": {}
    }
  }
}
```

### 3.3 With Custom Config File

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "/Users/<you>/repos/cellarbrain/.venv/bin/cellarbrain",
      "args": [
        "--config", "/Users/<you>/repos/cellarbrain/cellarbrain.toml",
        "-d", "/Users/<you>/repos/cellarbrain/output",
        "mcp"
      ],
      "env": {}
    }
  }
}
```

### 3.4 With Environment Variables

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["mcp"],
      "env": {
        "CELLARBRAIN_DATA_DIR": "/Users/<you>/repos/cellarbrain/output",
        "CELLARBRAIN_CONFIG": "/Users/<you>/repos/cellarbrain/cellarbrain.toml"
      }
    }
  }
}
```

### 3.5 SSE Transport (HTTP)

For HTTP-based MCP clients:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

Start the server:
```bash
cellarbrain mcp --transport sse --port 8080
```

---

## 4. Available MCP Tools

Once connected, OpenClaw agents have access to these tools:

### 4.1 Query & Search

| Tool | Parameters | Description |
|------|-----------|-------------|
| `query_cellar` | `sql: str` | Run read-only DuckDB SQL against pre-joined views |
| `find_wine` | `query: str, limit?: int` | Text search with intent parsing + synonym expansion |
| `cellar_info` | `verbose?: bool` | Version, config, ETL freshness, inventory summary |
| `cellar_stats` | `group_by?: str, limit?: int, sort_by?: str` | Summary statistics, optionally grouped |
| `cellar_churn` | `days?: int` | Recent additions and removals |
| `search_synonyms` | `action: str, key?: str, value?: str` | Manage custom search synonyms |
| `server_stats` | — | Internal MCP server performance metrics |

### 4.2 Dossier Management

| Tool | Parameters | Description |
|------|-----------|-------------|
| `read_dossier` | `wine_id: int, sections?: list[str]` | Read wine dossier (filtered sections) |
| `update_dossier` | `wine_id: int, section: str, content: str` | Write an agent-owned section |
| `batch_update_dossier` | `wine_ids: list[int], section: str, content: str` | Write same section to multiple wines |
| `pending_research` | `limit?: int` | List wines with empty agent sections |
| `read_companion_dossier` | `tracked_wine_id: int, sections?: list[str]` | Read companion dossier |
| `update_companion_dossier` | `tracked_wine_id: int, section: str, content: str` | Write companion section |
| `list_companion_dossiers` | `pending_only?: bool` | List tracked wines |
| `pending_companion_research` | `limit?: int` | Tracked wines needing research |
| `get_format_siblings` | `wine_id: int` | Get format variants (Magnum, etc.) |

### 4.3 Price Tracking

| Tool | Parameters | Description |
|------|-----------|-------------|
| `log_price` | `tracked_wine_id, vintage, bottle_size_ml, retailer_name, price, currency, in_stock, ...` | Record a price observation |
| `tracked_wine_prices` | `tracked_wine_id: int` | Latest prices across retailers |
| `price_history` | `tracked_wine_id: int, vintage?: int, months?: int` | Monthly min/max/avg CHF |
| `wishlist_alerts` | `days?: int` | Price drops, new listings, back in stock |

### 4.4 Sommelier (requires trained model)

| Tool | Parameters | Description |
|------|-----------|-------------|
| `suggest_wines` | `food_description: str, limit?: int` | Semantic food → wine pairing |
| `suggest_foods` | `wine_id: int, limit?: int` | Semantic wine → food pairing |
| `add_pairing` | `wine_id: int, food_description: str, score?: float` | Record a pairing observation |
| `train_sommelier` | — | Trigger model retraining |

### 4.5 Data Refresh

| Tool | Parameters | Description |
|------|-----------|-------------|
| `reload_data` | — | Re-run ETL from CSV exports |

---

## 5. Available SQL Views (for `query_cellar`)

| View | Description | Key Columns |
|------|-------------|-------------|
| `wines` | All wines with computed fields | wine_id, full_name, vintage, winery_name, region, country, category, bottles_stored, list_price, drinking_status, price_tier |
| `wines_stored` | Only wines with bottles in cellar | Same as `wines`, filtered to `bottles_stored > 0` |
| `wines_full` | Extended with grapes, appellation | All of `wines` + grape_names, appellation, classification |
| `bottles_stored` | Individual bottles | bottle_id, wine_id, wine_name, cellar_name, shelf, purchase_price, is_onsite |
| `bottles_full` | Bottles with wine details joined | bottle fields + wine fields |
| `tracked_wines` | Wishlist wines for price monitoring | tracked_wine_id, wine_name, winery_name, category, country, target_price |

---

## 6. Available Skills

The `.openclaw/` directory contains skill definitions that agents can load for structured workflows:

### 6.1 Core Sommelier (`cellarbrain`)

**File:** `.openclaw/cellarbrain/SKILL.md`

The primary skill — combines wine expertise with MCP data tools:
- Cellar Q&A (statistics, recommendations, drinking window)
- Food-wine pairing (semantic + rule-based)
- Wine search and discovery
- Dossier section reading

### 6.2 Cellar Q&A (`cellar-qa`)

**File:** `.openclaw/cellar-qa/`

Structured workflows for:
- "What's in my cellar?" questions
- Occasion-based recommendations ("date night wine", "BBQ wine")
- Drinking urgency queries ("what should I open soon?")
- Purchase decision support

### 6.3 Wine Research (`wine-research`)

**File:** `.openclaw/wine-research/`

Fact-only research agent workflow:
- Reads pending research queue
- Searches web for producer info, vintage reports, critic reviews
- Writes verified findings to dossier sections
- Never guesses — skips sections with insufficient data

Populates: `producer_profile`, `vintage_report`, `wine_description`, `ratings_reviews`, `tasting_notes`, `food_pairings`, `similar_wines`

### 6.4 Market Research (`market-research`)

**File:** `.openclaw/market-research/`

Pricing and availability:
- Swiss retailer stock checks
- Price comparison across retailers
- Secondary market data
- Populates the `market_availability` dossier section

### 6.5 Tracked Wine Research (`tracked-research`)

**File:** `.openclaw/tracked-research/`

Companion dossier research for wishlist/tracked wines:
- Producer deep dives
- Vintage tracking
- Buying guides

### 6.6 Food Pairing (`food-pairing`)

**File:** `.openclaw/food-pairing/`

Structured pairing framework:
- Semantic model lookup (if available)
- Rule-based pairing logic (weight matching, flavour bridging)
- Cellar-aware recommendations (only suggests wines you own)

### 6.7 Price Tracking (`price-tracking`)

**File:** `.openclaw/price-tracking/`

Swiss retailer scanning:
- Systematic price checks across configured retailers
- Stock status monitoring
- Price drop alerts
- Historical trend analysis

### 6.8 Shop Extraction (`shop-extraction`)

**File:** `.openclaw/shop-extraction/`

Per-shop data extraction with guides for 17 Swiss retailers:
- Gerstl, Martel, Flaschenpost, Mövenpick, etc.
- Price, rating, stock, description extraction
- AI accessibility assessments per retailer

---

## 7. Testing the Integration

### 7.1 Verify Tool Availability

After configuring the MCP server, verify the agent can list tools:

Ask the agent: "What cellarbrain tools are available?"

Expected: The agent lists all tools from Section 4 above.

### 7.2 Basic Smoke Tests

Test each tool category with simple calls:

```
# 1. Statistics
"How many wines are in my cellar?"
→ Agent calls cellar_stats() → reports total wines and bottles

# 2. Search
"Find any Barolo wines"
→ Agent calls find_wine(query="Barolo") → lists matching wines

# 3. SQL Query
"What countries are my wines from?"
→ Agent calls query_cellar(sql="SELECT country, count(*) FROM wines GROUP BY country ORDER BY count(*) DESC")

# 4. Dossier
"Read the dossier for wine 42"
→ Agent calls read_dossier(wine_id=42) → displays dossier content

# 5. Food pairing (requires sommelier model)
"What wine goes with grilled lamb?"
→ Agent calls suggest_wines(food_description="grilled lamb") → returns ranked list
```

### 7.3 Write Test (Dossier Update)

```
"Add a tasting note to wine 42: Deep ruby colour, aromas of dark cherry and cedar."
→ Agent calls update_dossier(wine_id=42, section="tasting_notes", content="...")
→ Verify with: cellarbrain dossier 42 --sections tasting_notes
```

### 7.4 Verify Diagnostics

```
"Check the cellar status and data freshness"
→ Agent calls cellar_info(verbose=True)
→ Reports version, data dir, last ETL, table counts, sommelier status
```

---

## 8. Agent Design Principles

When building skills or custom agents that use Cellarbrain:

### 8.1 MCP = Data, Agent = Reasoning

The MCP server provides **deterministic data operations**. All reasoning belongs in the agent:

| Task | Where |
|------|-------|
| Execute SQL query | MCP (`query_cellar`) |
| Decide which wine to recommend | Agent (LLM reasoning) |
| Read a dossier | MCP (`read_dossier`) |
| Synthesize research into prose | Agent (LLM writing) |
| Search wines by text | MCP (`find_wine`) |
| Pair food with wine | Agent (calls `suggest_wines` + applies pairing rules) |

### 8.2 Agent Section Ownership

Dossier sections have strict ownership:

| Owner | Sections |
|-------|----------|
| **ETL** (read-only) | `identity`, `origin`, `classification`, `purchase`, `bottles`, `metrics`, `history` |
| **Agent** (writeable) | `producer_profile`, `vintage_report`, `wine_description`, `ratings_reviews`, `tasting_notes`, `food_pairings`, `similar_wines`, `market_availability` |

Attempting to write ETL-owned sections raises `ProtectedSectionError`.

### 8.3 Efficiency Tips

- Use `read_dossier(wine_id, sections=[])` for minimal metadata (frontmatter only)
- Use `find_wine` instead of raw SQL for text search (handles synonyms, intents)
- Use `cellar_stats(group_by=...)` instead of `query_cellar` for standard aggregations
- Batch updates with `batch_update_dossier` when writing the same section to multiple wines

---

## 9. Customisation

### 9.1 Adapting for a Different Cellar

The skill files in `.openclaw/` contain owner-specific context:

```markdown
## Owner Context
- Based in **Switzerland** — prices in **CHF**
- Uses **Vinocell** to track their cellar
- Notes may be in **German**
```

To adapt for a different owner:
1. Fork the skill files
2. Update owner context (location, currency, language)
3. Update retailer registry (for price tracking)
4. Re-run ETL with the new cellar's CSV exports

### 9.2 Adding Custom Skills

Create a new skill in `.openclaw/<skill-name>/SKILL.md`:

```markdown
---
name: my-custom-skill
description: "Description of what this skill does"
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# My Custom Skill

## MCP Tools Used
| Tool | Purpose |
|---|---|
| `query_cellar` | ... |
| `find_wine` | ... |

## Workflow
1. ...
2. ...
```

### 9.3 Disabling Sommelier Features

If the sommelier model is not trained, `suggest_wines` and `suggest_foods` will return errors gracefully. Skills should fall back to SQL-based search:

```sql
-- Fallback: find wines by grape + region instead of semantic search
SELECT full_name, vintage, category, region
FROM wines_stored
WHERE category = 'Red' AND region = 'Bordeaux'
ORDER BY vintage DESC
LIMIT 10
```

---

## 10. Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| Agent can't connect to MCP server | Wrong path in config | Use full path to `.venv/bin/cellarbrain` |
| "DataStaleError" | No Parquet data | Run `cellarbrain etl` first |
| `suggest_wines` returns error | Model not trained | Run `cellarbrain train-model && cellarbrain rebuild-indexes` |
| `update_dossier` fails | Trying to write ETL section | Only write agent-owned sections (see 8.2) |
| Empty search results | Wrong query syntax | Use simple terms, not complex phrases |
| Slow `query_cellar` | Complex SQL or missing indexes | Simplify query, use views |
| Agent writes wrong wine | Didn't verify identity | Always read dossier first to confirm wine_id |

---

## Related Documentation

- [MCP Tools Reference](../docs/mcp-tools.md) — Full tool parameter schemas and return shapes
- [Agent Architecture](../docs/agent-architecture.md) — Design decisions and efficiency model
- [Dossier System](../docs/dossier-system.md) — Section ownership and lifecycle
- [Query Layer](../docs/query-layer.md) — Available views and SQL conventions
