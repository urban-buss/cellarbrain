---
name: digest
description: "Generate a proactive cellar intelligence brief — urgency warnings, newly optimal wines, top pick, inventory changes."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Cellar Digest

Generate a periodic cellar intelligence brief with actionable insights.

## Owner Context

Switzerland, CHF. German notes — translate. Prefer onsite wines.

## When to Use

- Morning check-in: "What's happening in my cellar?"
- Weekly review: "Cellar summary for this week"
- Before shopping: "Any bottles I need to drink soon?"
- Routine scheduled trigger (cron / Task Scheduler)

## 1. Generate Digest

`cellar_digest(period="daily")` — 1-day lookback (default)
`cellar_digest(period="weekly")` — 7-day lookback

The digest includes:
- **Urgency alerts**: wines past their optimal drinking window
- **Newly optimal**: wines that just entered their window
- **Top pick**: today's smart recommendation
- **Inventory summary**: total bottles, wines, value
- **Recent changes**: ETL additions/removals since last run

## 2. Deep-Dive on Urgent Wines

For any wine flagged as urgent:
`read_dossier(wine_id, sections=["tasting_notes", "drinking_window"])`

Offer to add to drink-tonight list or recommend a pairing.

## 3. Weekly Mode Extras

Weekly digests additionally show:
- Bottles consumed in the period
- New acquisitions
- Drinking window transitions

## 4. Follow-Up Actions

Based on digest content, offer:
- `recommend_tonight(occasion="casual")` for urgent wines
- `pair_wine(wine_id, dish="...")` if user mentions dinner
- `find_wine("ready to drink")` for broader exploration

## Presentation

Start with a one-line summary: "X urgent, Y newly ready, Z bottles total."
Then detail each section with wine names, vintages, and IDs.
Flag past-optimal wines prominently.

## Tools

| Tool | Purpose |
|------|---------|
| `cellar_digest` | Primary digest generation |
| `read_dossier` | Deep-dive on flagged wines |
| `recommend_tonight` | Follow-up recommendation |
| `pair_wine` | Food pairing for urgent wines |
| `find_wine` | Broader search fallback |
