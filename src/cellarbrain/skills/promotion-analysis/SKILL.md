---
name: promotion-analysis
description: "Analyse newsletter promotions matched to your cellar: re-buy deals, similar wines, gap fills. Month-by-month trends and retailer comparison."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Promotion Analysis

Analyse newsletter promotion matches scored by cellar relevance — re-buy opportunities, similar wines, and gap fills.

## Owner Context

Switzerland, CHF. Newsletters from: KapWeine, Coop Mondovino, Mövenpick, Schuler, DIVO, and others.

## When to Use

- "What deals match my cellar?"
- "Monthly promotion analysis"
- "Best re-buy opportunities"
- "Which retailers have the best deals for me?"
- "Promotion trends over time"
- "What gaps could I fill from recent promotions?"
- "Month-by-month promotion report"

## Quick Path: Latest Matches

`promotion_matches(months=1)` — all matches from the last month, sorted by value score.

## Workflow: Fresh Scan + Analysis

### 1. Run a New Scan

`scan_promotions()` — scans all configured retailers. Returns categorised matches:
- **Re-buy**: wines you own, now cheaper than your purchase price
- **Similar**: wines structurally similar to your collection (region + grape + category)
- **Gap Fill**: wines in underrepresented cellar dimensions

### 2. Review by Category

`promotion_matches(months=1, category="rebuy")` — best re-buy deals
`promotion_matches(months=1, category="similar")` — wines like yours
`promotion_matches(months=1, category="gap_fill")` — fill collection gaps

### 3. Filter by Value Score

`promotion_matches(months=1, min_score=0.5)` — only high-relevance matches

## Workflow: Month-by-Month Trends

### 1. Get History

`promotion_history(months=6)` — month-over-month summary with counts, categories, avg scores, and top retailers.

### 2. Drill Down a Specific Month

`promotion_matches(months=1, category="rebuy")` — narrow to a time window and category.

### 3. Custom SQL Analysis

```sql
-- Matches per month and category
SELECT
    strftime(scan_time, '%Y-%m') AS month,
    match_category,
    COUNT(*) AS matches,
    ROUND(AVG(value_score), 2) AS avg_score,
    ROUND(AVG(discount_vs_reference), 1) AS avg_discount
FROM read_parquet('promotion_match_*.parquet')
GROUP BY month, match_category
ORDER BY month DESC, matches DESC
```

```sql
-- Best re-buy opportunities (biggest savings vs purchase price)
SELECT
    wine_name, retailer_id, sale_price, reference_price,
    discount_vs_reference, scan_time
FROM read_parquet('promotion_match_*.parquet')
WHERE match_category = 'rebuy' AND discount_vs_reference > 10
ORDER BY discount_vs_reference DESC
LIMIT 10
```

```sql
-- Retailer comparison: who offers the most relevant deals?
SELECT
    retailer_id,
    COUNT(*) AS total_matches,
    ROUND(AVG(value_score), 2) AS avg_score,
    COUNT(*) FILTER (WHERE match_category = 'rebuy') AS rebuys,
    COUNT(*) FILTER (WHERE match_category = 'similar') AS similar,
    COUNT(*) FILTER (WHERE match_category = 'gap_fill') AS gaps
FROM read_parquet('promotion_match_*.parquet')
WHERE scan_time >= CURRENT_DATE - INTERVAL 3 MONTH
GROUP BY retailer_id
ORDER BY avg_score DESC
```

```sql
-- Gap fill recommendations: what's missing and who has it?
SELECT
    gap_dimension, gap_detail, wine_name, producer,
    retailer_id, sale_price, scan_time
FROM read_parquet('promotion_match_*.parquet')
WHERE match_category = 'gap_fill'
ORDER BY scan_time DESC
LIMIT 15
```

## Workflow: Retailer Comparison

1. `promotion_history(months=6)` — see which retailer dominates each month
2. `promotion_matches(months=3, retailer="kapweine")` — drill into one retailer
3. Compare average scores across retailers using the SQL above

## Interpretation Guide

**Value Score (0.0–1.0):**
- 0.8+ = Strong re-buy (significant savings on a wine you own)
- 0.5–0.8 = Good match (re-buy or high-similarity)
- 0.3–0.5 = Moderate (partial similarity or small gap fill)
- < 0.3 = Weak match (filtered out by default)

**Match Categories:**
- `rebuy`: You own this wine and the promo price is better than what you paid
- `similar`: You don't own this exact wine, but it matches your cellar profile (region, grape, style)
- `gap_fill`: This wine fills a dimension where you have very few bottles (< 2)

## Follow-Up Actions

After identifying interesting promotions:
- `find_wine(query="...")` — check if you already have it
- `read_dossier(wine_id, sections=["ratings_reviews"])` — verify quality
- `similar_wines(wine_id)` — see what's similar in your cellar
- Hand off to **price-scan** skill for price verification at the retailer

## Tools

| Tool | Purpose |
|------|---------|
| `scan_promotions` | Run a fresh newsletter scan with scoring |
| `promotion_matches` | Query historical scored matches |
| `promotion_history` | Month-by-month trend summary |
| `query_cellar` | Custom SQL for advanced analysis |
| `find_wine` | Verify wine identity |
| `similar_wines` | Cross-reference with cellar |
