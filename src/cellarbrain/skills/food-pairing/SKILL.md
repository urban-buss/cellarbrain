---
name: food-pairing
description: "Pair a wine from the cellar with a dish or menu. Food-to-wine and wine-to-food."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Food-Wine Pairing

Find the best wine for a dish, or suggest dishes for a wine.

## Owner Context

Switzerland, CHF. German notes -- translate. Only wines with bottles stored and ready to drink.

## Workflow: Dish -> Wine

### Quick Path A (most requests)

`pair_wine(dish="<user's dish>", occasion="<optional>")` -- server classifies, retrieves, ranks. Auto-uses embedding re-ranking when sommelier model available. Present top 3-5.

### Quick Path B (tonight's dinner)

`recommend_tonight(cuisine="<dish>", occasion="dinner_party", limit=5)` -- adds urgency and freshness scoring on top of pairing. Best when "what should I open tonight for X?"

### Structured Path (more control)

1. `pairing_candidates(dish_description="<dish>", limit=10)`
2. `read_dossier(wine_id, sections=["tasting_notes", "food_pairings"])` for top 3-5
3. Present with rationale

### With Sommelier Model

`suggest_wines(food_query="<dish>", limit=10)` -- embedding retrieval. Scores >=0.65 = strong. Use alongside `pair_wine` for diversity.

## Workflow: Wine -> Food

1. `suggest_foods(wine_id, limit=10)` -- dishes ranked by similarity
2. `read_dossier(wine_id, sections=["tasting_notes", "wine_description"])`
3. Present top 3-5 grouped by cuisine/weight

After results: offer `similar_wines(wine_id, limit=3)` for alternatives with same pairing affinity.

## Swiss Speciality Overrides

| Dish | Classic Pairing |
|------|----------------|
| Raclette | Chasselas (Fendant) |
| Fondue | Chasselas, Gutedel |
| Rosti with meat | Cornalin, Humagne Rouge, Gamaret |
| Zurcher Geschnetzeltes | Pinot Noir, Chasselas |
| Bundnerfleisch | Pinot Noir, Cornalin |

Prioritise Swiss pairings when dish matches.

## Presentation

Per pick: Wine vintage (id) -- pairing rationale -- status + location -- CHF price.

## Tools

| Tool | Purpose |
|------|---------|
| `pair_wine` | Single-shot dish→wine (server-side classification) |
| `pairing_candidates` | Structured multi-strategy retrieval |
| `suggest_wines` | Embedding-based food→wine (needs trained model) |
| `suggest_foods` | Embedding-based wine→food (needs trained model) |
| `read_dossier` | Wine details for final selection |
| `recommend_tonight` | Occasion-aware scored picks with food |
| `similar_wines` | Alternatives with same pairing affinity |

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.

