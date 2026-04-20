# Cellarbrain Agents

Custom agents for the cellarbrain wine cellar toolkit. Each is a specialist with focused tools and responsibilities.

## Setup

| Agent | Purpose | Tools |
|-------|---------|-------|
| **cellarbrain-devsetup** | Bootstrap local dev environment: Python, venv, deps, VS Code config, test verification (Windows + macOS) | `execute`, `read`, `search` |

## Cellar & Wine Agents

| Agent | Purpose | Tools |
|-------|---------|-------|
| **cellarbrain** | Sommelier: cellar Q&A, semantic food-wine pairing (embedding retrieval + pairing rules), wine recommendations, drinking window advice | `cellarbrain/*`, `web`, `todo` |
| **cellarbrain-research** | Research wines via web, populate dossier sections (producer, vintage, reviews, tasting notes) | `cellarbrain/*`, `web`, `todo` |
| **cellarbrain-tracked** | Research tracked wines, populate companion dossier sections (producer deep dives, buying guides) | `web/*`, `cellarbrain/*`, `todo` |
| **cellarbrain-market** | Market & availability research, pricing, retailer stock, secondary-market data | `cellarbrain/*`, `web`, `todo` |

## Price & Shop Agents

| Agent | Purpose | Tools |
|-------|---------|-------|
| **cellarbrain-price-tracker** | Automated Swiss retailer price scanning, stock status, alerts | `cellarbrain/*`, `web`, `todo` |
| **cellarbrain-shopscanner** | Extract prices, ratings, stock from retailer websites using per-shop guides | `cellarbrain/*`, `web`, `todo` |
| **cellarbrain-shopresearch** | Assess Swiss wine retailers for AI accessibility, catalogue depth, scraping feasibility | `agent/*`, `edit/*`, `web/*`, `browser/*`, `todo` |

## Testing & QA Agents

| Agent | Purpose | Tools |
|-------|---------|-------|
| **cellarbrain-smoketest** | ETL pipeline smoke tester: pytest → ETL → output verification → integrity → cross-run → MCP integration | `execute`, `read`, `search`, `todo` |
| **cellarbrain-extended-smoketest** | Change-aware extended smoke tester: analyses code changes, plans feature-specific + regression tests, runs standard pipeline + targeted extended tests | `cellarbrain/*`, `execute`, `read`, `search`, `todo` |
| **cellarbrain-qa** | Test MCP tools, CLI commands, and agent workflows end-to-end against live data | `cellarbrain/*`, `execute`, `read`, `search`, `todo` |
| **cellarbrain-search-tester** | Test MCP search & recommendation quality with realistic user scenarios | `read/*`, `search/*`, `cellarbrain/*`, `todo` |
| **cellarbrain-explorer** | Creative exploratory MCP testing: invents novel test cases, probes boundaries, discovers defects | `cellarbrain/*`, `todo` |

## Skills

Skills provide on-demand workflows that agents load as needed. Available via `/` slash commands.

| Skill | Purpose |
|-------|---------|
| **cellar-qa** | Cellar questions, food pairing, wine recommendations via MCP |
| **wine-research** | Research standards, confidence gates, dossier section guidelines |
| **price-tracking** | Price checking, scanning, alerts workflows |
| **shop-extraction** | Per-shop extraction guides for 17 Swiss retailers |
| **smoke-testing** | ETL smoke test pipeline, `tests/smoke_helpers/` API reference |

## Instructions

| File | Scope | Purpose |
|------|-------|---------|
| `copilot-instructions.md` | `**` | Project overview, coding conventions, testing, security |
| `smoke-helpers.instructions.md` | `tests/smoke_helpers/**/*.py` | CheckResult patterns, DuckDB usage, async MCP testing |
