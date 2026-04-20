---
name: cellar-qa
description: "Answer questions about the wine cellar, recommend wines, food pairings, and drinking suggestions via cellarbrain MCP."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Cellar Q&A and Recommendations

Workflows for answering cellar questions, food pairing, and wine recommendations using cellarbrain MCP tools.

## When to Use

- Cellar questions (counts, values, statistics)
- Wine recommendations for an occasion or meal
- Food pairing requests
- Drinking window and "what to open" questions
- Purchase history or spending queries

## Workflow: Cellar Questions

**Trigger:** "how many bottles", "cellar value", "wines by country", statistics

1. `cellar_stats()` for overview, or `cellar_stats(group_by="...")` for breakdowns
2. For specific questions, use `query_cellar(sql)`:
   - `wines_stored` or `bottles_stored` for "in cellar" questions
   - `purchase_date` for "since when" questions
   - `output_date` and `output_type` for consumption history
3. Present clearly with totals and context
4. Mention data freshness (last ETL run date)

### Useful SQL Patterns

```sql
-- Wines bought since a date
SELECT wine_id, winery_name, wine_name, vintage,
       purchase_date, purchase_price, purchase_currency
FROM bottles_full
WHERE purchase_date >= '2026-01-01'
ORDER BY purchase_date DESC

-- Wines in optimal window right now
SELECT wine_id, winery_name, wine_name, vintage, category,
       optimal_from, optimal_until, bottles_stored
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable') AND bottles_stored > 0
ORDER BY optimal_until ASC

-- Most urgent to drink
SELECT wine_id, winery_name, wine_name, vintage,
       optimal_until, bottles_stored
FROM wines_full
WHERE drinking_status = 'past_optimal' AND bottles_stored > 0
ORDER BY optimal_until ASC

-- Wines by grape variety
SELECT primary_grape, count(*) AS wines, sum(bottles_stored) AS bottles
FROM wines_stored
WHERE primary_grape IS NOT NULL
GROUP BY primary_grape
ORDER BY bottles DESC
```

## Workflow: Food Pairing

**Trigger:** "what goes with [dish]", "pair wine with", "dinner tonight"

> **Detailed guidance lives in the `food-pairing` skill.** It provides a structured decision framework, pairing matrix, and SQL query strategies.

Quick summary:
1. `suggest_wines(food_query, limit=10)` if sommelier available
2. Classify dish (weight, protein, flavours, cuisine, sweetness)
3. Apply pairing rules from the `food-pairing` skill to rerank
4. Deep-dive top candidates: `read_dossier(wine_id, sections=["tasting_notes", "food_pairings", "wine_description"])`
5. Present 3–5 recommendations with pairing rationale, drinking status, location

If `suggest_wines` returns an error (model not trained), skip step 1 and use SQL queries from the `food-pairing` skill's query strategies.

### Reverse Pairing ("What to cook with wine #N?")

1. `suggest_foods(wine_id, limit=10)`
2. `read_dossier(wine_id, sections=["tasting_notes", "wine_description"])`
3. Filter and explain top 3–5 dishes

## Workflow: Occasion Recommendation

**Trigger:** "date night wine", "what should I open", "recommend something elegant"

1. Understand context — occasion, mood, food, preferences
2. Query with SQL:
   - `optimal_from <= YEAR AND optimal_until >= YEAR`
   - Filter by category/style if implied
   - Use `wines_stored` or `wines_drinking_now`
   - Consider `is_favorite` for special occasions
3. Check urgency — also query wines past optimal window
4. Read dossiers for top candidates
5. Recommend 3–5 wines with reasoning, drinking status, location

## Workflow: Purchase / Value Questions

**Trigger:** "how much did I spend", "most expensive wine"

1. `query_cellar()` with aggregates on `bottles_full`:
   - `purchase_price`, `purchase_currency` for costs
   - Group by provider, date range, category
2. `cellar_stats()` for total cellar value
3. Present totals, averages, breakdowns

## Response Format

- Lead with the answer, then reasoning
- Include `wine_id` for reference
- Show storage location when recommending
- Flag "drink soon" if past optimal
- Translate German owner comments
- Cite data freshness
