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

## Primary Workflow (RAG-First)

The pairing system uses **structured SQL retrieval** as the primary mechanism.
No ML model is required — `pairing_candidates` always works.

| Step | Action | Tool |
|------|--------|------|
| 0 | **Classify the dish** — weight, protein, cuisine, flavours, sweetness | Your knowledge |
| 1 | **Retrieve candidates** — structured multi-strategy retrieval | `pairing_candidates(dish_description, category, weight, protein, cuisine, grapes)` |
| 1.5 | *(Optional)* **Embedding boost** — if sommelier available, also query | `suggest_wines(food_query)` |
| 2 | **Apply pairing rules** — rerank using 6-principle framework | This skill |
| 3 | **Read dossiers** — tasting notes, existing pairings, wine description | `read_dossier(wine_id, sections=[...])` |
| 4 | **Present recommendations** — 3–5 wines with rationale | Your reasoning |

### How to Call pairing_candidates

After classifying the dish, map your classification to tool parameters:

| Classification | Parameter | Example |
|---------------|-----------|---------|
| Weight: heavy | `weight="heavy"` | Braised beef → heavy |
| Protein: red_meat | `protein="red_meat"` | Steak → red_meat |
| Cuisine: French | `cuisine="French"` | Duck confit → French |
| Category: red | `category="red"` | (from protein inference) |
| Target grapes | `grapes="Syrah,Merlot,Nebbiolo"` | (from pairing rules) |
| Dish text | `dish_description="grilled lamb chops with rosemary and roasted garlic"` | Full description |

The tool combines these into multi-strategy SQL retrieval and returns
a ranked candidate table. You then apply the pairing framework to rerank.

**Protein values:** `red_meat`, `poultry`, `fish`, `seafood`, `pork`, `game`, `vegetarian`, `cheese`

**Weight values:** `light`, `medium`, `heavy`

**Category values:** `red`, `white`, `rose`, `sparkling`, `sweet`

### Optional Embedding Boost (Step 1.5)

If `suggest_wines` is available (sommelier model trained), call it additionally
for semantic matches the SQL retrieval might miss. Merge results with
candidates from Step 1, then apply the pairing framework to the combined set.

### Reranking Strategy (Step 2)

After retrieval, rerank candidates by applying the pairing rules in order:

1. **Weight mismatch → demote.** A full-bodied Barolo for a light salad scores poorly on weight matching even if it has many match signals.
2. **Tannin conflict → eliminate.** High-tannin red with delicate fish = metallic bitterness. Remove regardless of signals.
3. **Acid confirmation → promote.** High-acid wine for a rich dish = excellent pairing. Promote even if signal count is moderate.
4. **Regional bonus → promote.** Chasselas with raclette, Sangiovese with ragu — centuries of co-evolution should be respected.
5. **Drinking window → gate.** Never recommend a wine that's past its window or too young unless the user explicitly asks.

### Chaining Example: "What wine with duck confit?"

```
0. Classify: weight=heavy, protein=poultry (fatty), flavours=rich/fatty,
   cuisine=French, sweetness=savoury
   → category=red, grapes=Pinot Noir,Barbera,Sangiovese,Syrah,Grenache

1. pairing_candidates(
     dish_description="duck confit, slow cooked, crispy skin, rich fatty duck",
     protein="poultry", weight="heavy", cuisine="French",
     category="red", grapes="Pinot Noir,Barbera,Sangiovese,Syrah,Grenache"
   )
   → Returns 10–15 wines with match signals

1.5. (Optional) suggest_wines("duck confit, slow cooked, crispy skin, rich fatty duck", limit=10)
   → Merge with Step 1 results

2. Apply pairing rules:
   - Weight: need medium-full body (demote light wines)
   - Tannin: moderate OK (duck fat can handle some tannin, but not extreme)
   - Acid: HIGH needed to cut through duck fat → promote Barbera, Sangiovese, Pinot Noir
   - Regional: French dish → promote Burgundy, Rhône
   - Flavour bridges: earthy/cherry notes complement duck

3. Read dossiers for top 3–5 after reranking:
   read_dossier(wine_id, sections=["tasting_notes", "food_pairings", "wine_description"])

4. Present with rationale:
   "The 2019 Musar's Pinot Noir pairs beautifully — its bright acidity cuts through
   the duck fat while cherry fruit complements the richness."
```

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

### Step 3 — Retrieve Candidates

Call `pairing_candidates` with your classification from Steps 1–2:

```
pairing_candidates(
    dish_description="<full dish description>",
    category="red",        # from Step 2
    weight="heavy",        # from Step 1
    protein="red_meat",    # from Step 1
    cuisine="French",      # from Step 1
    grapes="Syrah,Merlot"  # from Step 2 (optional — overrides inference)
)
```

The tool returns a ranked Markdown table with match signals showing why each wine was selected. Wines matching more strategies rank higher.

Also consult [query strategies](./references/query-strategies.md) for additional dish-specific SQL patterns if needed.

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
