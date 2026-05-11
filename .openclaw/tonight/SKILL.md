---
name: tonight
description: "Recommend a wine to open tonight. Considers occasion, mood, food, and what's ready to drink."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# What to Drink Tonight

Recommend a wine from the cellar for tonight, considering occasion, mood, food, budget, and drinking readiness.

## Owner Context

- Switzerland, CHF. Notes may be in German — translate when presenting.
- Prefer wines stored **onsite** (home cellar) over offsite.

## Workflow

### 1. Understand the Request

Extract: occasion (casual / date night / guests), food (if any), mood (bold / light / celebratory), budget feel (everyday / special).

### 2. If Food Is Mentioned

Call `pair_wine(dish, occasion)` — returns ready-to-drink recommendations with reasons. Present the top 3 directly. Done.

### 3. Without Food — Search by Intent

Build a `find_wine` query using intent keywords:

| Occasion | Query keywords |
|----------|---------------|
| Casual weeknight | `"ready to drink" budget` + preferred category |
| Date night | `"ready to drink" top rated` + category |
| Guests / celebration | `"ready to drink" favorite` |
| Adventurous | `"ready to drink"` + unusual grape or region |

Call `find_wine(query, limit=8)`.

### 4. Deep-Dive Top Candidates

For the best 3–5 matches: `read_dossier(wine_id, sections=["tasting_notes", "wine_description", "food_pairings"])`.

### 5. Present

For each recommendation show:
- Wine name, vintage, wine_id
- One-sentence character description
- Drinking status (optimal / drinkable / drink soon)
- Storage location
- Purchase price

Flag wines past optimal as "drink soon — don't wait."

## Tools

| Tool | Purpose |
|------|---------|
| `find_wine` | Text search with intent detection |
| `read_dossier` | Wine details and tasting notes |
| `pair_wine` | Single-shot food+wine pairing |

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.
