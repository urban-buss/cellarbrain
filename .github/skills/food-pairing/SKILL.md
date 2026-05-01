---
name: food-pairing
description: "Wine and food pairing sommelier skill with structured pairing logic. Use when: 'what wine goes with', 'food pairing', 'pair wine with', 'dinner tonight', 'what to open with', 'what pairs with lamb', 'wine for fish', 'wine for pasta', 'raclette wine', 'dessert wine pairing', 'BBQ wine', 'cheese pairing', 'spicy food wine'."
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

The `suggest_wines` tool returns a `Score` column (0.0–1.0) based on embedding cosine similarity. These scores are **relative, not absolute**:

- **0.65+** — strong semantic match. The model sees a meaningful food-wine connection.
- **0.50–0.65** — moderate match. Worth considering if pairing rules support it.
- **Below 0.50** — weak match. Likely irrelevant unless the model is undertrained.

**Important:** Scores reflect the training data's pairing patterns, not universal truth. A score of 0.70 means "the model thinks this is a good pairing based on 5,000+ training examples." Always validate with the pairing framework below.

### Reranking Strategy

After retrieval, rerank candidates by applying the pairing rules in order:

1. **Weight mismatch → demote.** A full-bodied Barolo for a light salad scores poorly on weight matching even if the embedding score is high.
2. **Tannin conflict → eliminate.** High-tannin red with delicate fish = metallic bitterness. Remove regardless of score.
3. **Acid confirmation → promote.** High-acid wine for a rich dish = excellent pairing. Promote even if score is moderate.
4. **Regional bonus → promote.** Chasselas with raclette, Sangiovese with ragu — centuries of co-evolution should be respected.
5. **Drinking window → gate.** Never recommend a wine that's past its window or too young unless the user explicitly asks.

### Chaining Example: "What wine with duck confit?"

```
1. suggest_wines("duck confit, slow cooked, crispy skin, rich fatty duck", limit=10)
   → Returns 10 wines with scores

2. Classify: weight=heavy, protein=poultry (fatty), flavours=rich/fatty,
   cuisine=French, sweetness=savoury

3. Apply pairing rules:
   - Weight: need medium-full body (demote light wines)
   - Tannin: moderate OK (duck fat can handle some tannin, but not extreme)
   - Acid: HIGH needed to cut through duck fat → promote Barbera, Sangiovese, Pinot Noir
   - Regional: French dish → promote Burgundy, Rhône
   - Flavour bridges: earthy/cherry notes complement duck

4. Read dossiers for top 3–5 after reranking:
   read_dossier(wine_id, sections=["tasting_notes", "food_pairings", "wine_description"])

5. Present with rationale:
   "The 2019 Musar's Pinot Noir pairs beautifully — its bright acidity cuts through
   the duck fat while cherry fruit complements the richness."
```

### Fallback (No Sommelier Model)

If `suggest_wines` returns an error, fall back to the SQL-based approach in Step 3 below (query by grape, category, region using the pre-built SQL patterns). The pairing framework works identically — you just start at Step 1 instead of Step 0.

## Pairing Decision Framework

Apply these principles **in order** to build a wine profile for the dish.

### 1. Weight Matching (most important)

The wine's body must match the dish's richness. A mismatch ruins both.

| Dish Weight | Examples | Wine Body Target |
|-------------|----------|-----------------|
| **Light** | Salad, raw fish, steamed vegetables, consommé | Light (Chasselas, Arneis, Sauvignon Blanc, Pinot Gris) |
| **Medium** | Roast chicken, grilled fish, risotto, pasta in light sauce | Medium (Chardonnay, Chenin Blanc, Pinot Noir, Barbera, Sangiovese) |
| **Heavy** | Braised meat, stew, game, aged cheese, rich sauces | Full (Cabernet Sauvignon, Merlot, Nebbiolo, Syrah, Malbec, Mourvèdre) |

### 2. Tannin–Protein Interaction

| Scenario | Effect | Rule |
|----------|--------|------|
| High tannin + red meat protein | Tannin softens, meat tastes better | ✅ Seek this |
| High tannin + fish/seafood | Metallic, bitter taste | ❌ Avoid |
| High tannin + fat (cheese, charcuterie) | Fat coats palate, tannin cuts through | ✅ Works well |
| High tannin + spicy food | Amplifies heat and bitterness | ❌ Avoid |

**High-tannin grapes in cellar:** Cabernet Sauvignon, Nebbiolo, Petit Verdot, Lagrein, Mourvèdre, Tannat

### 3. Acid Balance

High-acid wines cut through richness and complement acidic dishes.

| Dish Characteristic | Wine Acid Need | Good Cellar Matches |
|---------------------|---------------|-------------------|
| Fatty/rich (duck confit, cream sauce) | High acid to cut through | Sangiovese, Barbera, Sauvignon Blanc, Chenin Blanc, Riesling |
| Tomato-based (marinara, ragu) | High acid to match | Sangiovese, Barbera, Nebbiolo |
| Citrus/vinaigrette | High acid to complement | Sauvignon Blanc, Chasselas, Arneis, Verdejo |
| Mild/neutral | Moderate acid | Merlot, Chardonnay, Chenin Blanc |

### 4. Sweetness Rule

**Wine must be at least as sweet as the dish.** Dry wine + sweet food = wine tastes thin and sour.

| Dish Sweetness | Wine Style | Cellar Options |
|----------------|-----------|---------------|
| Savoury | Dry (most wines) | Default |
| Mildly sweet (glazed, teriyaki, BBQ sauce) | Off-dry or fruity | Gewürztraminer, Riesling, Moscato |
| Dessert | Sweet wine — must be sweeter than the dish | Sauternes (Suduiraut, Coutet, D'Yquem, Climens), Moscato d'Asti |
| Spicy-hot | Off-dry or low-alcohol fruity | Gewürztraminer, Riesling, Moscato, rosé |

### 5. Regional Affinity

When a dish comes from a wine region, start with local wines — centuries of co-evolution.

| Cuisine | First Look | Cellar Regions |
|---------|-----------|---------------|
| French (Provençal, Bordelais) | France | Bordeaux, Vallée du Rhône, Languedoc-Roussillon, Bourgogne |
| Italian (pasta, risotto, pizza) | Italy | Piemonte, Toscana, Veneto, Trentino Alto-Adige, Puglia |
| Spanish (tapas, paella, jamón) | Spain | La Rioja, Castilla y León |
| Swiss (fondue, raclette, Rösti) | Switzerland | Valais, Vaud |
| Argentine (asado, empanadas) | Argentina | Mendoza |
| South African (braai, bobotie) | South Africa | Western Cape |

### 6. Flavour Bridges

Match aromatic/flavour compounds between dish and wine.

| Dish Flavour | Flavour Bridge | Wine Match |
|-------------|----------------|-----------|
| Herbal (rosemary, thyme, sage) | Green/herbal aromatics | Sauvignon Blanc, Cabernet Franc, Syrah |
| Smoky (grilled, BBQ) | Oak, smoke | Oaked Chardonnay, Syrah, Tempranillo |
| Earthy (mushrooms, truffles) | Earth, forest floor | Pinot Noir, Nebbiolo, aged Bordeaux |
| Fruity (fruit sauces, berry compote) | Fruit-forward wines | Merlot, Malbec, Grenache, Primitivo |
| Spice (pepper, cumin, cinnamon) | Spicy wine notes | Syrah, Grenache, Gewürztraminer |
| Umami (soy, aged cheese, mushroom) | High acid, moderate tannin | Barbera, Sangiovese, Champagne |
| Butter/cream | Round, oaked whites | Oaked Chardonnay, Sémillon |

## Workflow

### Step 1 — Classify the Dish

Build a profile:
- **Weight:** light / medium / heavy
- **Dominant protein:** red meat / poultry / fish / shellfish / vegetarian / cheese
- **Key flavours:** herbal, smoky, acidic, sweet, spicy, earthy, creamy
- **Cuisine origin:** French, Italian, Swiss, Asian, American, etc.
- **Sweetness level:** savoury / mildly sweet / sweet / spicy-hot

### Step 2 — Derive Wine Attributes

Using the framework above, determine:
- **Category:** red / white / rosé / sparkling / sweet
- **Body:** light / medium / full
- **Target grapes:** 4–8 varieties that match the dish profile
- **Regions to prefer:** if regional affinity applies
- **What to avoid:** high tannin with fish, dry with dessert, etc.

### Step 3 — Query the Cellar

Consult [query strategies](./references/query-strategies.md) for pre-built SQL patterns by dish type.

Key rules:
- Always filter to `wines_drinking_now` or add `drinking_status IN ('optimal', 'drinkable')`
- Sort: `optimal` before `drinkable`, then by `best_pro_score DESC NULLS LAST`
- Limit to 10–15 candidates
- If the dish is ambiguous (could go red or white), run two queries

### Step 4 — Deep-Dive Top Candidates

For the 3–5 best matches from Step 3:

```
read_dossier(wine_id, sections=["tasting_notes", "food_pairings", "wine_description"])
```

Check:
- Existing `food_pairings` section — may already confirm or contradict your reasoning
- Owner comments (may be in German — translate relevant parts)
- Tasting descriptors that reinforce the flavour bridge
- Alcohol level — higher alcohol can overwhelm delicate dishes

### Step 5 — Present Recommendations

Offer **3–5 wines** with:

1. **Wine name, vintage, wine_id** — always include wine_id for reference
2. **One-sentence pairing rationale** — name the specific principle (e.g., "the Barbera's high acidity cuts through the richness of the ragu")
3. **Drinking window status** — flag as "drink soon" if past optimal
4. **Storage location** — cellar + shelf from dossier for easy retrieval
5. **Price context** — purchase price from inventory section

**Order by fit:** best match first, then alternatives. Include at least one "safe pick" and one "adventurous pick."

If no good matches exist, say so honestly and suggest what to buy.

## Swiss Speciality Pairings

The cellar owner is based in Switzerland. These traditional pairings are well-established:

| Dish | Classic Pairing | Cellar Grapes | Notes |
|------|----------------|---------------|-------|
| **Raclette** | Chasselas (Fendant) | Chasselas, Humagne Blanche | Dry, mineral, high acid to cut cheese fat |
| **Fondue** | Chasselas, Gutedel | Chasselas, Gutedel | Same logic — must not overpower the cheese |
| **Rösti with meat** | Swiss reds | Cornalin, Humagne Rouge, Diolinoir, Gamaret | Medium body, local affinity |
| **Zürcher Geschnetzeltes** | Swiss white or light red | Pinot Noir, Chasselas | Cream sauce needs acid; avoid heavy tannin |
| **Bündnerfleisch** | Light red or rosé | Pinot Noir, Cornalin, rosé | Air-dried meat = delicate; don't overwhelm |
| **Älplermagronen** | Swiss white | Chasselas, Silvaner | Comfort food, keep it simple |

## Dessert Pairing Rules

The cellar has notable Sauternes (D'Yquem, Suduiraut, Coutet, Climens, Lafaurie-Peyraguey). Apply these rules:

1. **Wine must be sweeter than the dish** — otherwise the wine tastes flat
2. **Sauternes + foie gras** — the classic French pairing (also works as appetiser)
3. **Sauternes + Roquefort/blue cheese** — sweet + salty = extraordinary contrast
4. **Sauternes + fruit tarts** — apricot, peach, apple work perfectly
5. **Chocolate → avoid most dessert wines** — chocolate overwhelms; consider fortified (not in cellar) or skip wine
6. **Moscato d'Asti + light fruit desserts** — low alcohol, sparkling, delicate

## Common Mistakes to Avoid

- **Never pair tannic reds with fish or shellfish** — metallic bitterness
- **Never pair dry wine with sweet desserts** — wine tastes sour and thin
- **Don't default to "red with meat, white with fish"** — too simplistic; a rich fish stew can take a light Pinot Noir
- **Don't ignore drinking window** — a perfectly paired wine that's past its window will disappoint
- **Don't recommend the most expensive bottle** unless the occasion warrants it — match the wine to the meal's formality
- **Don't overlook rosé** — excellent bridge wine for mixed menus, charcuterie, BBQ

## Reference Files

- [Pairing Matrix](./references/pairing-matrix.md) — detailed dish-to-wine lookup by category
- [Query Strategies](./references/query-strategies.md) — pre-built SQL patterns for cellar searches by dish type
