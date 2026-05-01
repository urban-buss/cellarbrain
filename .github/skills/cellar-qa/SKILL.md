---
name: cellar-qa
description: "Answer questions about the wine cellar, recommend wines, food pairings, and drinking suggestions via cellarbrain MCP. Use when: 'what wine goes with', 'food pairing', 'recommend a wine', 'what to drink', 'how many bottles', 'cellar statistics', 'purchase history', 'drinking window', 'date night wine', 'what should I open', 'is this wine ready'."
---

# Cellar Q&A and Recommendations

Workflows for answering cellar questions, food pairing, and wine recommendations using cellarbrain MCP tools.

## When to Use

- User asks about their cellar (counts, values, statistics)
- User wants a wine recommendation for an occasion or meal
- User asks about food pairings
- User asks about drinking windows or what to open
- User asks about purchase history or spending

## Workflow: Cellar Questions

**Trigger:** "how many bottles", "cellar value", "wines by country", statistics questions

1. Call `cellar_stats()` for overview, or `cellar_stats(group_by="...")` for breakdowns
2. For specific questions, use `query_cellar(sql)` with targeted SQL:
   - Use `wines_stored` or `bottles_stored` for "in cellar" questions
   - Use `wines`, `bottles`, or convenience views — no JOINs needed
   - Use `purchase_date` for "since when" questions
   - Use `output_date` and `output_type` for consumption history
3. Present results clearly with totals and context
4. Always mention data freshness (last ETL run date from stats output)

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

-- Most urgent to drink (past optimal, still in cellar)
SELECT wine_id, winery_name, wine_name, vintage,
       optimal_until, bottles_stored
FROM wines_full
WHERE drinking_status = 'past_optimal' AND bottles_stored > 0
ORDER BY optimal_until ASC

-- Wines by grape variety (stored only)
SELECT primary_grape, count(*) AS wines, sum(bottles_stored) AS bottles
FROM wines_stored
WHERE primary_grape IS NOT NULL
GROUP BY primary_grape
ORDER BY bottles DESC
```

## Workflow: Food Pairing

**Trigger:** "what goes with [dish]", "pair wine with", "dinner tonight", "spaghetti carbonara"

> **Detailed guidance lives in the `food-pairing` skill.** It provides a structured decision framework (weight matching, tannin–protein interaction, acid balance, sweetness, regional affinity, flavour bridges), a comprehensive pairing matrix by dish category, Swiss specialty pairings, and pre-built SQL query strategies.

Quick summary:
1. **Retrieve candidates** (if sommelier available): `suggest_wines(food_query, limit=10)` — semantic retrieval finds wines whose embedding profile matches the dish
2. Classify the dish (weight, protein, flavours, cuisine, sweetness)
3. Apply pairing rules from the food-pairing skill to rerank the candidates
4. Deep-dive top candidates: `read_dossier(wine_id, sections=["tasting_notes", "food_pairings", "wine_description"])`
5. Present 3–5 recommendations with pairing rationale, drinking status, and storage location

If `suggest_wines` returns an error (model not trained), skip step 1 and query the cellar with targeted SQL instead (see `food-pairing` query strategies).

### Reverse Pairing ("What to cook with wine #N?")

1. `suggest_foods(wine_id, limit=10)` — returns dishes ranked by embedding similarity with cuisine, weight, protein, and flavour profile
2. `read_dossier(wine_id, sections=["tasting_notes", "wine_description"])` — understand the wine
3. Filter and explain the top 3–5 dishes with brief pairing rationale

## Workflow: What to Drink / Occasion Recommendation

**Trigger:** "date night wine", "what should I open", "recommend something elegant", "casual BBQ wine"

1. **Understand context** — occasion, mood, food (if mentioned), preferences
2. **Query candidates** using SQL:
   - Start with drinking window: `optimal_from <= YEAR AND optimal_until >= YEAR`
   - Filter by category/style if the request implies one (e.g., "elegant cab" → category = 'red', grape = 'Cabernet Sauvignon')
   - Use `wines_stored` or `wines_drinking_now` for cellar queries
   - Consider `is_favorite` for special occasions
3. **Check urgency** — also query wines past their optimal window that should be drunk soon
4. **Read dossiers** for top candidates to get full context
5. **Recommend 3–5 wines** with reasoning:
   - Why it fits the occasion
   - Drinking window status
   - Tasting profile if available
   - Storage location for easy retrieval

## Workflow: Purchase / Value Questions

**Trigger:** "how much did I spend", "what's my most expensive wine", "good deal"

1. Use `query_cellar()` with aggregate queries on the `bottles_full` view:
   - `purchase_price`, `purchase_currency` for costs
   - `purchase_date` for time-based questions
   - Group by provider, date range, category as needed
2. For value assessment, use `cellar_stats()` which shows cellar value totals
3. Present clear totals, averages, and time-based breakdowns

## Response Format Guidelines

- **Be direct** — lead with the answer, then add reasoning
- **Use wine_id** — always include wine_id so the user can reference it later
- **Show location** — when recommending a bottle, tell where it's stored
- **Flag urgency** — label wines as "drink soon" if past optimal window
- **Translate German** — if owner comments are in German, provide English translation
- **Cite data freshness** — mention when the data was last updated
