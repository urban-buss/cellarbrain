---
name: wine-info
description: "Look up a wine and show its dossier: tasting notes, ratings, drinking window, food pairings, similar wines."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Wine Information

Look up a wine and present its dossier details.

## Owner Context

Switzerland, CHF. German notes -- translate.

## Workflow

### 1. Find the Wine

`find_wine(query="<user's description>", limit=5)` -- searches name, winery, region, grape, vintage. Has 5-stage fallback (fuzzy, phonetic, suggestions) -- no manual retry needed.

If zero results: `wine_suggestions(query)` for autocomplete hints.

### 2. Read the Dossier

`read_dossier(wine_id, sections=["identity", "origin", "characteristics", "drinking_window", "ratings_reviews", "tasting_notes", "food_pairings", "cellar_inventory"])`

### 3. Present

- **Identity:** Full name, winery, vintage, category, grapes
- **Character:** Tasting profile, style, structure
- **Ratings:** Professional scores (cite critic + publication)
- **Drinking window:** Status + dates
- **Inventory:** Bottles stored, location, purchase price
- **Food pairings:** Suggested dishes

Translate German notes.

### 4. Similar Wines (offer)

`similar_wines(wine_id, limit=5)` -- structurally similar wines in cellar (winery, region, grape, category, price tier).

### 5. Research Freshness (optional)

If dossier info looks sparse, note research may be stale. The `stale_research` tool can verify.

## Tools

| Tool | Purpose |
|------|---------|
| `find_wine` | Text search across cellar |
| `read_dossier` | Full wine dossier content |
| `wine_suggestions` | Autocomplete when search fails |
| `similar_wines` | Structurally similar wines |
| `stale_research` | Check research freshness |

## Output Format

Pass `format="plain"` only when the tool schema exposes a `format` parameter. Never add unsupported arguments. Present the final answer as plain text for iMessage.
