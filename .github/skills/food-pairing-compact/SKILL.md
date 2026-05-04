---
name: food-pairing-compact
description: "Compact wine–food pairing skill for small/local LLMs. Use when: 'what wine goes with', 'food pairing', 'pair wine with', 'dinner tonight', 'what to open with'. Lightweight alternative to the full food-pairing skill."
---

# Wine Pairing — Compact Guide

Simplified food-pairing workflow. Call one tool, present the results.

## Quick Start

Call `pair_wine` with the user's food description:

```
pair_wine(dish="grilled lamb with rosemary")
```

The tool handles everything: dish classification, cellar search, and ranking.
Present the returned recommendations to the user.

## When to Use `pair_wine`

- User names a dish and wants wine suggestions
- User asks "what goes with X" or "wine for Y"
- Any food-to-wine pairing question

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `dish` | Yes | What the user is eating (free text) |
| `occasion` | No | Context: "casual", "date night", "formal" |
| `limit` | No | Number of picks (default 5) |

## Presenting Results

The tool returns pre-formatted recommendations with reasons. You can:
1. Present them directly to the user
2. Add a brief personal comment if helpful
3. Mention wine_id so the user can find the bottle

## Advanced Use

For more control (expert users), use `pairing_candidates` instead — it
accepts explicit protein, weight, category, cuisine, and grapes parameters.
