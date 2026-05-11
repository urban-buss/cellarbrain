---
name: food-pairing
description: "Pair a wine from the cellar with a dish or menu. Food-to-wine and wine-to-food."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Food–Wine Pairing

Find the best wine in the cellar for a dish, or suggest dishes for a wine.

## Owner Context

- Switzerland, CHF. Notes may be in German — translate when presenting.
- Only recommend wines with `bottles_stored > 0` that are ready to drink.

## Workflow: Dish → Wine

### Quick Path (recommended for most requests)

`pair_wine(dish="<user's dish description>", occasion="<optional context>")` → returns pre-ranked recommendations with pairing reasons. Present the top 3–5.

### Structured Path (when you want more control)

1. `pairing_candidates(dish_description="<dish>", limit=10)` — server classifies the dish and returns candidates
2. `read_dossier(wine_id, sections=["tasting_notes", "food_pairings"])` for top 3–5
3. Present with pairing rationale

### With Sommelier Model (optional enhancement)

`suggest_wines(food_query="<dish>", limit=10)` — embedding-based retrieval. Scores ≥0.65 = strong match. Use alongside `pair_wine` for diversity.

## Workflow: Wine → Food

1. `suggest_foods(wine_id, limit=10)` — returns dish suggestions ranked by similarity
2. `read_dossier(wine_id, sections=["tasting_notes", "wine_description"])` — understand the wine
3. Present top 3–5 dishes grouped by cuisine/weight

## Swiss Speciality Overrides

| Dish | Classic Pairing | Note |
|------|----------------|------|
| Raclette | Chasselas (Fendant) | Dry, mineral, high acid cuts cheese fat |
| Fondue | Chasselas, Gutedel | Must not overpower the cheese |
| Rösti with meat | Cornalin, Humagne Rouge, Gamaret | Medium body, local affinity |
| Zürcher Geschnetzeltes | Pinot Noir, Chasselas | Cream sauce needs acid |
| Bündnerfleisch | Pinot Noir, Cornalin | Delicate air-dried meat |

If the user's dish matches a Swiss speciality, prioritise these pairings over generic results.

## Presentation

For each recommendation:
- Wine name, vintage, wine_id
- One-sentence pairing rationale
- Drinking status + location
- Purchase price

## Tools

| Tool | Purpose |
|------|---------|
| `pair_wine` | Single-shot dish→wine (server-side classification) |
| `pairing_candidates` | Structured multi-strategy retrieval |
| `suggest_wines` | Embedding-based food→wine (needs trained model) |
| `suggest_foods` | Embedding-based wine→food (needs trained model) |
| `read_dossier` | Wine details for final selection |

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.
