---
name: dinner-party
description: "Plan a complete wine flight for a multi-course dinner party. Selects one wine per course with light-to-heavy progression, diversity, and budget awareness."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Dinner Party Concierge

Plan a harmonious wine flight for a multi-course dinner.

## Owner Context

Switzerland, CHF. German notes — translate. Prefer onsite wines with sufficient stock.

## 1. Parse the Menu

Ask the user for their courses (in serving order). Examples:
- "oysters | risotto ai funghi | lamb rack | cheese plate"
- "We're having fish to start, then duck, then dessert"

Map to a pipe-separated `courses` string for the tool.

## 2. Plan the Flight

```
plan_dinner(
  courses="oysters | risotto ai funghi | lamb rack | cheese plate",
  guests=6,
  budget="under_200",
  dinner_time="19:30"
)
```

Parameters:
| Param | Default | Notes |
|-------|---------|-------|
| `courses` | required | Pipe-separated course descriptions |
| `guests` | 4 | Number of diners |
| `budget` | "any" | Total: any, under_50, under_100, under_150, under_200, under_300 |
| `style` | "classic" | Pairing philosophy (classic only in v1) |
| `dinner_time` | None | HH:MM 24h for preparation timeline |

## 3. Present the Plan

The tool returns a formatted flight plan with:
- **Flight Progression** — table with course → wine → reason → bottles
- **Preparation Timeline** — when to chill/decant each wine
- **Tasting Card** — per-course serving details

Present the full plan. Highlight any warnings (low stock, no match).

## 4. Deep-Dive (optional)

For any wine the user wants to know more about:
`read_dossier(wine_id, sections=["tasting_notes", "wine_description", "food_pairings"])`

Offer substitutions: `pair_wine(dish="<course>", limit=5)` if user wants alternatives for a specific course.

## 5. Fallback

If `plan_dinner` returns too few matches:
- Ask user to relax budget constraint
- Suggest simpler menu (fewer courses)
- Offer `find_wine("ready to drink <category>")` for manual selection

## Presentation

Show the full formatted plan as returned. Add a brief intro: "Here's your flight plan for tonight's dinner — wines arranged from lightest to heaviest."

Flag any courses without a match clearly. Offer to help find alternatives.

## Tools

| Tool | Purpose |
|------|---------|
| `plan_dinner` | Primary flight planning |
| `read_dossier` | Wine details for deep-dive |
| `pair_wine` | Alternative suggestions per course |
| `find_wine` | Fallback search |
