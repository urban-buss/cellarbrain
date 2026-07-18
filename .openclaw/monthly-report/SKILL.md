---
name: monthly-report
description: "Generate a CellarBrain month-end, period-end, prior-year, or year-to-date portfolio report with value, bottle, volume, and fluctuation analysis."
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

`cellar_report(format="plain")` — current date vs the final day of the prior month.

## Parameters

| Param | Values | Default |
|-------|--------|---------|
| cutoff | ISO date, "today", "current_month" | current date |
| compare_to | "prior_month", "prior_year", "year_start" | "prior_month" |

## Date Resolution Examples

- "May report" → calculate the final calendar day of the requested May and pass it as an ISO date
- "Year to date" → `cellar_report(compare_to="year_start")`
- "Compare to last year" → `cellar_report(compare_to="prior_year")`
- "End of April" → calculate April's final day for the intended year; ask only when the year cannot be inferred safely

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

## Output Format

Pass `format="plain"` to `cellar_report`, `cellar_stats`, and `cellar_churn` for iMessage. Present the resolved cutoff date and comparison date explicitly. Use the structured snapshots and fluctuation fields as the source of truth.
