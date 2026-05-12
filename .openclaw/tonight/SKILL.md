---
name: tonight
description: "Recommend a wine to open tonight. Uses the scoring engine for occasion, food, budget, and urgency."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# What to Drink Tonight

Recommend a wine using the smart recommendation engine.

## Owner Context

Switzerland, CHF. German notes — translate. Prefer onsite wines.

## 1. Map the Request

| User says | `occasion` | `budget` |
|-----------|------------|----------|
| weeknight, casual, just me | `casual` or `solo` | `under_30` |
| dinner with friends, guests | `dinner_party` | `under_50` |
| celebrate, birthday | `celebration` | `special` |
| date night, anniversary | `romantic` | `special` |
| tasting, compare wines | `tasting` | `any` |

## 2. Recommend

`recommend_tonight(occasion=..., cuisine="<dish>" if food mentioned, budget=..., limit=5)`

Scores: urgency + occasion fit + pairing + freshness + diversity + quality. Wines on drink-tonight list are auto-excluded.

## 3. Drink-Tonight List

- Show list: `get_drink_tonight()`
- Add/remove: instruct user to use dashboard `/drink-tonight`

## 4. Deep-Dive (optional)

`read_dossier(wine_id, sections=["tasting_notes", "wine_description"])` for the user's pick.

Offer `similar_wines(wine_id, limit=3)` if they want alternatives.

## 5. Fallback

If `recommend_tonight` returns nothing: `find_wine("ready to drink", limit=8)` then `read_dossier`. Search auto-falls-back through fuzzy/phonetic.

## Presentation

Per pick: **Wine** vintage (id) — one-line character — status — location — CHF price. Flag past-optimal as "drink soon".

## Tools

| Tool | Purpose |
|------|---------|
| `find_wine` | Text search with intent detection |
| `read_dossier` | Wine details and tasting notes |
| `pair_wine` | Single-shot food+wine pairing |
| `recommend_tonight` | Primary scored recommendation |
| `get_drink_tonight` | Read drink-tonight shortlist |
| `similar_wines` | Alternatives to a pick |

## Output Format

Always pass `format="plain"` to every tool call. The user receives responses via iMessage where Markdown tables and formatting are not supported. Plain format uses numbered lists, bullet points, and simple text separators instead.

