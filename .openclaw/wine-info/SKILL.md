---
name: wine-info
description: "Look up a wine and show its dossier: tasting notes, ratings, drinking window, food pairings."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Wine Information

Look up a wine in the cellar and present its key details from the dossier.

## Owner Context

- Switzerland, CHF. Notes may be in German — translate when presenting.

## Workflow

### 1. Find the Wine

`find_wine(query="<user's description>", limit=5)` — search by name, winery, region, grape, vintage.

If multiple results, ask the user to clarify or pick the best match.

### 2. Read the Dossier

`read_dossier(wine_id, sections=["identity", "origin", "characteristics", "drinking_window", "ratings_reviews", "tasting_notes", "food_pairings", "cellar_inventory"])`.

### 3. Present

Summarise clearly:
- **Identity:** Full name, winery, vintage, category, grapes
- **Character:** Tasting profile, style, structure
- **Ratings:** Professional scores (cite critic + publication)
- **Drinking window:** Status (optimal / drinkable / too young / past), dates
- **Inventory:** Bottles stored, location, purchase price
- **Food pairings:** Suggested dishes

Translate any German owner notes or tasting comments.

## Tools

| Tool | Purpose |
|------|---------|
| `find_wine` | Text search across cellar |
| `read_dossier` | Full wine dossier content |
