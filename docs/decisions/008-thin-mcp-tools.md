# ADR-008: Thin MCP Tools — No Reasoning in Server

## Status
Accepted

## Context
The MCP server exposes wine cellar data to AI agents. Embedding LLM reasoning (recommendations, analysis, summaries) in the server would couple it to specific agent patterns and make testing harder.

## Decision
All 15 MCP tools are **thin data accessors** — they fetch, write, or transform data and return formatted Markdown strings. No tool performs reasoning, summarisation, or recommendation logic.

The boundary is clear:
- **Server**: data I/O, SQL validation, parameter binding, error handling, formatted output
- **Agent**: reasoning, recommendations, tasting note interpretation, food pairing suggestions

Tool functions catch domain exceptions (`QueryError`, `WineNotFoundError`, etc.) and return `f"Error: {exc}"` strings rather than raising.

## Consequences
- Tools are easily testable with temp Parquet datasets
- Any agent (sommelier, researcher, market tracker) can use the same tools
- Formatting logic (Markdown tables via pandas) stays in the server for consistent output
- Agents must compose multiple tool calls for complex workflows (e.g. "research pending wines" requires `pending_research` → `find_wine` → web search → `update_dossier`)
