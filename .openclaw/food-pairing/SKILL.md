---
name: food-pairing
description: "Wine and food pairing sommelier skill with structured pairing logic. Use when: 'what wine goes with', 'food pairing', 'pair wine with', 'dinner tonight', 'what to open with'."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Wine–Food Pairing

Structured food-pairing workflow that translates a dish into wine attributes, then queries the cellar for matches. Replaces guesswork with a systematic framework.

## When to Use

- User names a dish and wants a wine recommendation
- User describes a meal or occasion with food
- User asks "what goes with X" or "pair wine with Y"
- User mentions specific cuisine (Thai, Italian, Swiss, etc.)

## Sommelier-Assisted Workflow

When the cellarbrain MCP server has a trained sommelier model, use embedding-based retrieval as Step 0 before applying the pairing framework.

### Modified Workflow (with Sommelier)

| Step | Action | Tool |
|------|--------|------|
| 0 | **Retrieve candidates** — get semantically similar wines | `suggest_wines(food_query)` |
| 1 | Classify the dish (weight, protein, flavours, cuisine) | Your knowledge |
| 2 | Apply pairing rules to rerank the retrieved candidates | This skill |
| 3 | Read dossiers for top 3–5 candidates | `read_dossier(wine_id, sections=[...])` |
| 4 | Present recommendations with pairing rationale | Your reasoning |

### How to Interpret Retrieval Scores

The `suggest_wines` tool returns a `Score` column (0.0–1.0) based on embedding cosine similarity:

- **0.65+** — strong semantic match
- **0.50–0.65** — moderate match, worth considering if pairing rules support it
- **Below 0.50** — weak match, likely irrelevant

### Reranking Strategy

After retrieval, rerank candidates by applying the pairing rules in order:

1. **Weight mismatch → demote.** Full-bodied Barolo for a light salad = poor weight match.
2. **Tannin conflict → eliminate.** High-tannin red with delicate fish = metallic bitterness.
3. **Acid confirmation → promote.** High-acid wine for a rich dish = excellent.
4. **Regional bonus → promote.** Chasselas with raclette, Sangiovese with ragu.
5. **Drinking window → gate.** Never recommend past-window or too-young wines.

### Fallback (No Sommelier Model)

If `suggest_wines` returns an error, fall back to the SQL-based approach in Step 3 below (query by grape, category, region using the pre-built SQL patterns).

## Pairing Decision Framework

Apply these principles **in order** to build a wine profile for the dish.

### 1. Weight Matching (most important)

| Dish Weight | Examples | Wine Body Target |
|-------------|----------|-----------------|
| **Light** | Salad, raw fish, steamed vegetables | Light (Chasselas, Arneis, Sauvignon Blanc, Pinot Gris) |
| **Medium** | Roast chicken, grilled fish, risotto | Medium (Chardonnay, Chenin Blanc, Pinot Noir, Barbera) |
| **Heavy** | Braised meat, stew, game, aged cheese | Full (Cabernet Sauvignon, Merlot, Nebbiolo, Syrah, Malbec) |

### 2. Tannin–Protein Interaction

| Scenario | Effect | Rule |
|----------|--------|------|
| High tannin + red meat protein | Tannin softens, meat tastes better | ✅ Seek |
| High tannin + fish/seafood | Metallic, bitter taste | ❌ Avoid |
| High tannin + fat (cheese, charcuterie) | Fat coats palate, tannin cuts through | ✅ Works |
| High tannin + spicy food | Amplifies heat and bitterness | ❌ Avoid |

### 3. Acid Balance

| Dish Characteristic | Wine Acid Need | Good Matches |
|---------------------|---------------|-------------|
| Fatty/rich (duck confit, cream sauce) | High acid to cut through | Sangiovese, Barbera, Sauvignon Blanc, Riesling |
| Tomato-based | High acid to match | Sangiovese, Barbera, Nebbiolo |
| Citrus/vinaigrette | High acid to complement | Sauvignon Blanc, Chasselas, Arneis |
| Mild/neutral | Moderate acid | Merlot, Chardonnay, Chenin Blanc |

### 4. Sweetness Rule

**Wine must be at least as sweet as the dish.**

| Dish Sweetness | Wine Style |
|----------------|-----------|
| Savoury | Dry (most wines) |
| Mildly sweet (glazed, teriyaki, BBQ sauce) | Off-dry or fruity (Gewürztraminer, Riesling) |
| Dessert | Sweet wine (Sauternes, Moscato d'Asti) |
| Spicy-hot | Off-dry or low-alcohol fruity |

### 5. Regional Affinity

| Cuisine | First Look |
|---------|-----------|
| French | Bordeaux, Rhône, Languedoc, Bourgogne |
| Italian | Piemonte, Toscana, Veneto, Puglia |
| Swiss (fondue, raclette) | Valais, Vaud |
| Argentine (asado) | Mendoza |
| South African (braai) | Western Cape |

### 6. Flavour Bridges

| Dish Flavour | Flavour Bridge | Wine Match |
|-------------|----------------|-----------|
| Herbal | Green/herbal aromatics | Sauvignon Blanc, Cabernet Franc, Syrah |
| Smoky (grilled, BBQ) | Oak, smoke | Oaked Chardonnay, Syrah, Tempranillo |
| Earthy (mushrooms, truffles) | Earth, forest floor | Pinot Noir, Nebbiolo, aged Bordeaux |
| Fruity (fruit sauces) | Fruit-forward wines | Merlot, Malbec, Grenache |
| Spice (pepper, cumin) | Spicy wine notes | Syrah, Grenache, Gewürztraminer |
| Umami (soy, aged cheese) | High acid, moderate tannin | Barbera, Sangiovese, Champagne |
| Butter/cream | Round, oaked whites | Oaked Chardonnay, Sémillon |

## Workflow

### Step 1 — Classify the Dish

Build a profile: **Weight** (light/medium/heavy), **Protein** (red meat/poultry/fish/shellfish/vegetarian/cheese), **Key flavours**, **Cuisine**, **Sweetness level**.

### Step 2 — Derive Wine Attributes

Determine: category, body, target grapes (4–8), preferred regions, what to avoid.

### Step 3 — Query the Cellar

Consult [query strategies](./references/query-strategies.md) for pre-built SQL patterns by dish type.

Key rules:
- Always filter to `wines_drinking_now` or add `drinking_status IN ('optimal', 'drinkable')`
- Sort: `optimal` before `drinkable`, then by `best_pro_score DESC NULLS LAST`
- If the dish is ambiguous (could go red or white), run two queries

### Step 4 — Deep-Dive Top Candidates

For the 3–5 best matches: `read_dossier(wine_id, sections=["tasting_notes", "food_pairings", "wine_description"])`

### Step 5 — Present Recommendations

Offer **3–5 wines** with: wine name + vintage + wine_id, one-sentence pairing rationale, drinking window status, storage location, purchase price. Best match first. Include one "safe pick" and one "adventurous pick."

## Swiss Speciality Pairings

| Dish | Classic Pairing | Notes |
|------|----------------|-------|
| **Raclette** | Chasselas (Fendant) | Dry, mineral, high acid to cut cheese fat |
| **Fondue** | Chasselas, Gutedel | Must not overpower the cheese |
| **Rösti with meat** | Cornalin, Humagne Rouge, Gamaret | Medium body, local affinity |
| **Zürcher Geschnetzeltes** | Pinot Noir, Chasselas | Cream sauce needs acid |
| **Bündnerfleisch** | Pinot Noir, Cornalin | Air-dried meat = delicate |

## Dessert Pairing Rules

1. **Wine must be sweeter than the dish**
2. **Sauternes + foie gras** — the classic French pairing
3. **Sauternes + Roquefort/blue cheese** — sweet + salty contrast
4. **Sauternes + fruit tarts** — apricot, peach, apple
5. **Chocolate** — avoid most dessert wines; too difficult to pair
6. **Moscato d'Asti + light fruit desserts** — low alcohol, sparkling, delicate

## Common Mistakes to Avoid

- Tannic reds with fish or shellfish → metallic bitterness
- Dry wine with sweet desserts → wine tastes sour
- Ignoring drinking window → perfectly paired but past-window wine disappoints
- Defaulting to most expensive bottle → match formality to the meal
- Overlooking rosé → excellent bridge wine for mixed menus and BBQ

## Reference Files

- [Pairing Matrix](./references/pairing-matrix.md) — detailed dish-to-wine lookup by category
- [Query Strategies](./references/query-strategies.md) — pre-built SQL patterns for cellar searches by dish type
