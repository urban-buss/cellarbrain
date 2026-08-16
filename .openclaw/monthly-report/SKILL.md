---
name: monthly-report
description: "Generate structured monthly cellar report with value, bottles, volume, and fluctuation analysis."
metadata: {"openclaw": {"requires": {"bins": ["cellarbrain"]}}}
---

# Monthly Report

Generate a structured cellar portfolio report for month-end or period-end reporting.

## Owner Context

Switzerland, CHF. Reports at-cost value (purchase price), bottle counts, and volume in liters.

## When to Use

- "Monthly report"
- "Cellar report for May"
- "End of month numbers"
- "Portfolio snapshot"
- "Compare to last month / last year"
- "Year-to-date cellar"
- "How did my cellar change this month?"

## Quick Path

`cellar_report()` — current date vs prior month.

## Parameters

| Param | Values | Default |
|-------|--------|---------|
| cutoff | ISO date, "today", "current_month" | "current_month" |
| compare_to | "prior_month", "prior_year", "year_start" | "prior_month" |

## Date Resolution Examples

- "May report" → `cellar_report(cutoff="2026-05-31", compare_to="prior_month")`
- "Year to date" → `cellar_report(compare_to="year_start")`
- "Compare to last year" → `cellar_report(compare_to="prior_year")`
- "End of April" → `cellar_report(cutoff="2026-04-30")`

## Output Format

Three sections:
1. **Current snapshot** — stored / on order / all (value CHF, bottles, liters)
2. **Prior period snapshot** — same breakdown
3. **Fluctuation** — in / out / net per segment (stored, on order, all)

## Limitations

On-order classification uses **current** transit status. Bottles that arrived
during the reporting period appear in the "stored" segment. For most monthly
cadences this is exact.

## Tools

| Tool | Purpose |
|------|---------|
| `cellar_report` | Primary report generation |
| `cellar_stats` | Quick current-state overview |
| `cellar_churn` | Detailed roll-forward by period |
