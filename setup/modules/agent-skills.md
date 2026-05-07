# Agent Skills

Architecture and available skills for AI agents connecting via MCP.

## Architecture: MCP = Data, Agent = Reasoning

The MCP server provides **deterministic data operations**. All reasoning belongs in the agent:

| Task | Where |
|------|-------|
| Execute SQL query | MCP (`query_cellar`) |
| Decide which wine to recommend | Agent (LLM reasoning) |
| Read a dossier | MCP (`read_dossier`) |
| Synthesize research into prose | Agent (LLM writing) |
| Search wines by text | MCP (`find_wine`) |
| Pair food with wine | Agent (calls `suggest_wines` + applies pairing rules) |

## Dossier Section Ownership

Sections have strict ownership — attempting to write ETL-owned sections raises `ProtectedSectionError`.

| Owner | Sections |
|-------|----------|
| **ETL** (read-only) | `identity`, `origin`, `classification`, `purchase`, `bottles`, `metrics`, `history` |
| **Agent** (writeable) | `producer_profile`, `vintage_report`, `wine_description`, `ratings_reviews`, `tasting_notes`, `food_pairings`, `similar_wines`, `market_availability` |

## Available Skills

### Core Sommelier (`cellarbrain`)

Primary skill combining wine expertise with MCP data tools: cellar Q&A, food-wine pairing, wine search, dossier reading.

### Cellar Q&A (`cellar-qa`)

Structured workflows for inventory questions, occasion-based recommendations, drinking urgency, purchase support.

### Wine Research (`wine-research`)

Fact-only research: reads pending queue → searches web → writes verified findings to dossier sections (producer_profile, vintage_report, wine_description, ratings_reviews, tasting_notes, food_pairings, similar_wines).

### Market Research (`market-research`)

Swiss retailer stock checks, price comparisons, secondary market data. Populates `market_availability`.

### Tracked Wine Research (`tracked-research`)

Companion dossier research for wishlist wines: producer deep dives, vintage tracking, buying guides.

### Food Pairing (`food-pairing`)

Semantic model lookup + rule-based pairing logic + cellar-aware recommendations.

### Price Tracking (`price-tracking`)

Swiss retailer scanning, stock monitoring, price drop alerts, trend analysis.

### Shop Extraction (`shop-extraction`)

Per-shop extraction guides for 17 Swiss retailers (Gerstl, Martel, Flaschenpost, etc.).

## Efficiency Tips

- Use `read_dossier(wine_id, sections=[])` for minimal metadata (frontmatter only)
- Use `find_wine` instead of raw SQL for text search (handles synonyms, intents)
- Use `cellar_stats(group_by=...)` instead of `query_cellar` for standard aggregations
- Batch updates with `batch_update_dossier` when writing the same section to multiple wines

## Creating Custom Skills

Create a skill in `.openclaw/<skill-name>/SKILL.md`:

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
```

## Adapting for a Different Cellar

The skill files contain owner-specific context (location, currency, language). To adapt:

1. Fork the skill files
2. Update owner context
3. Update retailer registry (for price tracking)
4. Re-run ETL with new cellar CSV exports

## Disabling Sommelier Features

Without a trained model, `suggest_wines` and `suggest_foods` return errors gracefully. Skills fall back to SQL-based search:

```sql
SELECT full_name, vintage, category, region
FROM wines_stored
WHERE category = 'Red' AND region = 'Bordeaux'
ORDER BY vintage DESC LIMIT 10
```

## Next Steps

- [MCP Server](mcp-server.md) — Available tools and SQL views
- [Sommelier](sommelier.md) — Train the pairing model
