---
name: research-completeness
description: "Analyse dossier research completeness: overall cellar score, per-wine breakdown, month-by-month tracking, and improvement priorities."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Research Completeness

Measure and track how thoroughly the cellar has been researched.

## Owner Context

Switzerland, CHF. Scores range 0–100%. Components:
- **Section population** (50 pts): 8 research sections per wine
- **Research freshness** (20 pts): research-meta date within 12 months
- **Food data** (15 pts): food_tags (10) + food_groups (5)
- **Pro ratings** (15 pts): at least one professional rating

## When to Use

- "How researched is my cellar?"
- "Research completeness report"
- "Which wines need research?"
- "Monthly research progress"
- "Completeness score"
- "Research campaign priorities"
- "Least researched wines"
- "Improvement tracking"

## Quick Path

`research_completeness(limit=20, sort="score_asc")` — least-complete wines first.

## Detailed Workflows

### 1. Overall Cellar Score

```sql
SELECT
    COUNT(*) AS total_wines,
    ROUND(AVG(completeness_score), 1) AS avg_score,
    COUNT(*) FILTER (WHERE completeness_score = 100) AS fully_complete,
    COUNT(*) FILTER (WHERE completeness_score = 0) AS unresearched,
    COUNT(*) FILTER (WHERE completeness_score BETWEEN 1 AND 49) AS partial,
    COUNT(*) FILTER (WHERE completeness_score >= 50) AS well_researched
FROM wines_completeness
```

Present as: "Your cellar is **X%** researched (N wines fully complete, M unresearched)."

### 2. Score Distribution by Category

```sql
SELECT
    category,
    COUNT(*) AS wines,
    ROUND(AVG(completeness_score), 1) AS avg_score,
    MIN(completeness_score) AS min_score,
    MAX(completeness_score) AS max_score
FROM wines_completeness
GROUP BY category
ORDER BY avg_score ASC
```

### 3. Score Distribution by Region

```sql
SELECT
    country, region,
    COUNT(*) AS wines,
    ROUND(AVG(completeness_score), 1) AS avg_score
FROM wines_completeness
GROUP BY country, region
ORDER BY avg_score ASC
LIMIT 15
```

### 4. Improvement Priorities (Stored Wines First)

```sql
SELECT wine_id, wine_name, vintage, winery_name,
       completeness_score, populated_count, pending_count,
       has_food_tags, has_food_groups, has_pro_ratings
FROM wines_completeness
WHERE bottles_stored > 0
ORDER BY completeness_score ASC, is_favorite DESC
LIMIT 20
```

### 5. Freshness Audit

```sql
SELECT wine_id, wine_name, vintage,
       completeness_score, fresh_count, stale_count,
       populated_count
FROM wines_completeness
WHERE stale_count > 0
ORDER BY stale_count DESC
LIMIT 20
```

### 6. Food Tags Gap

```sql
SELECT wine_id, wine_name, vintage, completeness_score
FROM wines_completeness
WHERE NOT has_food_tags AND bottles_stored > 0
ORDER BY completeness_score DESC
LIMIT 20
```

These wines are partially researched but missing food pairing tags — good candidates for `rederive-food-tags` or manual food_pairings research.

### 7. Month-by-Month Progress Tracking

The `computed_at` timestamp in `research_completeness` records when scores were last computed. To track progress over time:

**Current snapshot:**
```sql
SELECT
    DATE_TRUNC('month', computed_at) AS month,
    ROUND(AVG(completeness_score), 1) AS avg_score,
    COUNT(*) FILTER (WHERE completeness_score >= 50) AS well_researched,
    COUNT(*) AS total
FROM wines_completeness
GROUP BY DATE_TRUNC('month', computed_at)
```

**Compare with previous periods** — Run `research_completeness()` periodically and note:
- Average score trend (aim for monthly increase)
- Number of wines moving from 0→partial or partial→complete
- Stale section count (should decrease after research campaigns)

**Tracking workflow:**
1. Run `research_completeness(limit=100, sort="score_asc")` at start of month
2. Note the average score and count of wines at 0%
3. Run research campaigns (use `pending_research()` for priority queue)
4. Re-run at end of month to measure improvement
5. Target: +5% average score per month with active research

### 8. Research Campaign Planning

After reviewing scores, suggest a campaign:

1. **Quick wins:** Wines at 40-60% missing only food_tags or pro ratings
2. **High-value gaps:** Favorite wines or expensive bottles at <50%
3. **Stale refresh:** Wines with high populated_count but many stale sections

`research_completeness(min_score=40, max_score=60, limit=20)` — partial wines needing a push.
`research_completeness(max_score=20, limit=10)` — least researched, start from scratch.

## Tools

| Tool | Purpose |
|------|---------|
| `research_completeness` | Per-wine scores with filtering |
| `query_cellar` | SQL against `wines_completeness` view |
| `pending_research` | Priority queue for research |
| `stale_research` | Sections needing refresh |

## Presentation

- Lead with overall percentage: "Your cellar is **67%** researched."
- Show distribution: "42 fully complete, 18 unresearched, 95 partial."
- Highlight quick wins: "12 wines need only food tags to reach 75%+."
- Suggest next action: "Run a research campaign on your 10 least-complete stored wines."
