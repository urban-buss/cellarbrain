# Query Strategies — SQL Patterns by Dish Type

Pre-built SQL templates for searching the cellar by dish category. All use the denormalised `wines_full` view filtered to ready-to-drink wines only (`drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0`). Adapt the grape lists and limits as needed.

## Base Pattern

Every food-pairing query should follow this structure:

```sql
SELECT wine_id, wine_name, winery_name, vintage, category,
       country, region, primary_grape, grape_summary,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND <category and grape filters>
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Red Meat (Steak, Braised, Roast)

```sql
-- Full-bodied reds for grilled/roasted red meat
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'red'
  AND primary_grape IN (
    'Cabernet Sauvignon', 'Merlot', 'Syrah', 'Malbec',
    'Mourvèdre', 'Petit Verdot', 'Lagrein', 'Cabernet Franc'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 12
```

### Braised / Slow-Cooked Variant
Use softer grapes — replace Cabernet Sauvignon with focus on blends:
```sql
  AND primary_grape IN (
    'Merlot', 'Cabernet Franc', 'Grenache', 'Mourvèdre',
    'Tempranillo', 'Syrah'
  )
```

---

## Game (Venison, Wild Boar, Duck)

```sql
-- Earthy, structured reds for game
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'red'
  AND primary_grape IN (
    'Nebbiolo', 'Syrah', 'Mourvèdre', 'Pinot Noir',
    'Cabernet Sauvignon', 'Cornalin'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Poultry (Chicken, Turkey)

```sql
-- Medium whites and light reds for poultry
SELECT wine_id, wine_name, vintage, category, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND (
  (category = 'white' AND primary_grape IN (
    'Chardonnay', 'Chenin Blanc', 'Sauvignon Blanc', 'Sémillon'
  ))
  OR
  (category = 'red' AND primary_grape IN (
    'Pinot Noir', 'Barbera', 'Merlot'
  ))
)
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Light Seafood (White Fish, Shellfish, Sushi)

```sql
-- Crisp, light whites for delicate seafood
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'white'
  AND primary_grape IN (
    'Sauvignon Blanc', 'Chasselas', 'Arneis', 'Nascetta',
    'Pinot Gris', 'Riesling', 'Malvazija Istarska',
    'Verdejo', 'Silvaner'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Oily Fish (Salmon, Tuna)

```sql
-- Medium whites or light reds that handle oily fish
SELECT wine_id, wine_name, vintage, category, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND (
  (category = 'white' AND primary_grape IN (
    'Chardonnay', 'Chenin Blanc', 'Sémillon', 'Sauvignon Blanc'
  ))
  OR
  (category = 'red' AND primary_grape IN ('Pinot Noir', 'Barbera'))
)
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Italian (Tomato-Based Pasta, Pizza)

```sql
-- Italian reds with acid for tomato dishes
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'red'
  AND country = 'Italy'
  AND primary_grape IN (
    'Sangiovese', 'Barbera', 'Nebbiolo', 'Primitivo',
    'Negroamaro', 'Lagrein'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Cream-Based / Rich Sauces

```sql
-- Whites with acid or oaked body for cream sauces
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'white'
  AND primary_grape IN (
    'Chardonnay', 'Chenin Blanc', 'Arneis', 'Sémillon',
    'Nascetta'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Spicy / Asian Cuisine

```sql
-- Aromatic, off-dry whites that handle heat
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'white'
  AND primary_grape IN (
    'Gewürztraminer', 'Riesling', 'Chenin Blanc',
    'Sauvignon Blanc', 'Muscat', 'Moscato Giallo'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Swiss Specialties (Fondue, Raclette)

```sql
-- Swiss whites and local grapes for traditional dishes
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND country = 'Switzerland'
  AND (
    primary_grape IN (
      'Chasselas', 'Gutedel', 'Humagne', 'Silvaner',
      'Johannisberg', 'Amigne', 'Lafnetscha'
    )
    OR category = 'white'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

### Swiss Reds (for Rösti, Bündnerfleisch, charcuterie)
```sql
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND country = 'Switzerland'
  AND category = 'red'
  AND primary_grape IN (
    'Cornalin', 'Humagne rouge', 'Pinot Noir', 'Gamaret',
    'Diolinoir', 'Carminoir'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Dessert Pairings

```sql
-- Sweet wines for dessert / foie gras / blue cheese
SELECT wine_id, wine_name, winery_name, vintage,
       primary_grape, region, drinking_status,
       bottles_stored, best_pro_score, price_tier
FROM wines_stored
WHERE (
  region ILIKE '%sauternes%'
  OR region ILIKE '%barsac%'
  OR wine_name ILIKE '%moscato%'
  OR wine_name ILIKE '%d''asti%'
)
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## BBQ / Grilling

```sql
-- Bold, fruit-forward reds for BBQ
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'red'
  AND primary_grape IN (
    'Malbec', 'Grenache', 'Primitivo', 'Petite Syrah',
    'Syrah', 'Shiraz', 'Tempranillo', 'Cinsault'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Cheese Course

```sql
-- Hard/aged cheese: structured reds
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'red'
  AND primary_grape IN (
    'Nebbiolo', 'Cabernet Sauvignon', 'Tempranillo',
    'Sangiovese', 'Merlot'
  )
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 10
```

---

## Rosé — The Versatile Bridge

When the dish is mixed, ambiguous, or the menu has varied courses, rosé is a safe bet:

```sql
SELECT wine_id, wine_name, vintage, primary_grape, region,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
  AND category = 'rose'
ORDER BY
  CASE drinking_status WHEN 'optimal' THEN 1 WHEN 'drinkable' THEN 2 ELSE 3 END,
  best_pro_score DESC NULLS LAST
LIMIT 5
```

---

## Champagne / Sparkling — The Universal Aperitif

Champagne works with shellfish, sushi, fried food, charcuterie, and as aperitif:

```sql
SELECT wine_id, wine_name, winery_name, vintage,
       drinking_status, bottles_stored, best_pro_score, price_tier
FROM wines_stored
WHERE region = 'Champagne'
  AND drinking_status IN ('optimal', 'drinkable')
ORDER BY best_pro_score DESC NULLS LAST
LIMIT 5
```

---

## Urgent / Drink-Soon Overlay

Combine any pairing query with urgency — prefer wines past their optimal window that still match:

```sql
-- Add to any WHERE clause to prioritise urgent bottles:
AND (drinking_status = 'past_optimal'
     OR (drinking_status = 'optimal' AND optimal_until <= 2027))
```

This helps the owner use wines before they decline while still getting a good pairing.
