---
name: wine-of-the-day
description: "Daily wine pick — deterministic rotation weighted by urgency, freshness, and diversity. Same wine all day, changes at midnight."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Wine of the Day

A zero-effort daily suggestion. Answers "what should I open tonight?" before the user asks.

## Owner Context

Switzerland, CHF. German notes — translate. Prefer onsite wines.

## When to Use

- "What should I drink today?"
- "Wine of the day"
- "Daily pick"
- "Surprise me"
- "Quick suggestion"

## 1. Get Today's Pick

`wine_of_the_day()`

Returns one wine — same pick all day, changes at midnight. Weighted by:
- Drinking window urgency (2× weight for past-optimal / nearing end)
- Diversity (rotates through regions/grapes over days)
- Quality (critic scores, favourites)

## 2. Deep-Dive (optional)

If user wants more detail:
`read_dossier(wine_id, sections=["tasting_notes", "wine_description"])`

## 3. Pairing (optional)

If user mentions food or dinner:
`pair_wine(wine_id, dish="<what they're eating>")`

## 4. Alternatives

If user doesn't like the pick:
`recommend_tonight(occasion="casual", limit=5)`

Or for a completely different style:
`recommend_tonight(occasion="casual", cuisine="<preferred>", limit=5)`

## 5. Add to Drink-Tonight

If user accepts the pick, suggest adding to the drink-tonight shortlist via the dashboard.

## Presentation

Show: **Wine Name** Vintage (wine_id) — one-line reason — status badge — stock count.
Keep it brief — the value is instant, low-friction suggestion.

## Tools

| Tool | Purpose |
|------|---------|
| `wine_of_the_day` | Primary daily pick |
| `read_dossier` | Deep-dive on the pick |
| `pair_wine` | Food pairing for the pick |
| `recommend_tonight` | Alternatives if user rejects pick |
| `similar_wines` | Wines similar to today's pick |
