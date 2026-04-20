# Agent Architecture

How AI agents access cellarbrain data. Two complementary interfaces share the same Python codebase, Parquet + DuckDB + Markdown stack, and require zero new infrastructure.

| Interface | Purpose | Consumer |
|---|---|---|
| **MCP Server** (`cellarbrain mcp`) | Structured tool/resource access for agents | Claude Desktop, VS Code Copilot, any MCP host |
| **CLI** (`cellarbrain`) | Script-triggering, cron jobs, manual operations | Shell, cron, CI |

---

## Design Principle

> **MCP tools = deterministic data operations.** If a task requires LLM reasoning, world knowledge, or subjective judgment (food pairing, wine recommendations, research synthesis, price comparison), it belongs in the **agent**, not in the MCP server. The server provides data building blocks; the agent provides intelligence.

See [mcp-tools.md](mcp-tools.md) for the complete tool catalogue.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                        Host Machine                            │
│                                                                │
│  ┌────────────┐     ┌──────────────────────────────────────┐  │
│  │  Vinocell  │     │         MCP Host (any client)        │  │
│  │  (app)     │     │  Claude Desktop / VS Code Copilot    │  │
│  └─────┬──────┘     └──────────────┬───────────────────────┘  │
│        │ CSV export                │                           │
│        ▼                           ▼                           │
│  raw/*.csv                  Agent Skills (LLM reasoning)      │
│        │                    ┌──────┴──────┐                   │
│        │                    ▼              ▼                   │
│        │             SKILL.md        MCP Server Config         │
│        │             (workflows)     (stdio transport)         │
│        ▼                    │                                  │
│  ┌──────────────┐   ┌──────┴──────────────────────────────┐  │
│  │  cellarbrain    │   │      cellarbrain-mcp (FastMCP)          │  │
│  │  CLI         │   │                                      │  │
│  │              │   │  TOOLS         RESOURCES   PROMPTS   │  │
│  │  • etl       │   │  • query()     • wine://   • qa      │  │
│  │  • validate  │   │  • stats()     • cellar:// • pair    │  │
│  │  • dossier   │   │  • find()      • etl://    • research│  │
│  │  • query     │   │  • read()                            │  │
│  │  • stats     │   │  • update()                          │  │
│  │  • recalc    │   │  • reload()                          │  │
│  │  • wishlist  │   │  • pending()                         │  │
│  │  • mcp       │   │  • log_price()                       │  │
│  └──────┬───────┘   └──────┬───────────────────────────────┘  │
│         │                  │                                   │
│         ▼                  ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                 Shared Data Layer                        │  │
│  │                                                         │  │
│  │  output/                                                │  │
│  │  ├── wine.parquet          DuckDB (in-process)          │  │
│  │  ├── bottle.parquet        ◄── SQL on Parquet           │  │
│  │  ├── winery.parquet                                     │  │
│  │  ├── ...                   6 agent views + 3 convenience│  │
│  │  └── wines/                                             │  │
│  │      ├── cellar/*.md       Agent-readable dossiers      │  │
│  │      ├── archive/*.md                                   │  │
│  │      └── tracked/*.md      Companion dossiers           │  │
│  └─────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## Agent Skills (LLM Reasoning)

These workflows require LLM reasoning, world knowledge, or web access. The agent orchestrates them using its own capabilities + MCP data tools. They are **not** MCP tools.

| Skill | What the agent does | MCP tools it calls |
|---|---|---|
| **Food pairing** | Retrieves candidates via sommelier → applies pairing framework → reads dossiers → ranks and presents. Falls back to SQL when model unavailable. | `suggest_wines`, `suggest_foods`, `query_cellar`, `read_dossier` |
| **"What to drink next"** | Queries by drinking urgency → considers occasion/mood → recommends | `query_cellar`, `cellar_stats`, `read_dossier` |
| **Wine research** | Gets pending sections → reads dossier for identity → web searches for producer, vintage, ratings → synthesizes into Markdown → writes back | `pending_research`, `read_dossier`, `update_dossier` |
| **Price monitoring** | Reads identity → searches retailer sites → compares with purchase price → updates dossier | `read_dossier`, `query_cellar`, `log_price`, `update_dossier` |
| **Purchase advisor** | Reads cellar composition → identifies gaps → uses wine knowledge to suggest additions | `cellar_stats`, `query_cellar` |
| **Batch research** | Gets research queue → loops through top-N → executes wine research per wine | `pending_research` + wine research skill |

### Why Skills Live in the Agent, Not the MCP Server

| Concern | MCP tool (Python) | Agent skill (LLM) |
|---|---|---|
| "What goes with Chicken Massala?" | Would need a hardcoded rules engine | LLM has deep food+wine knowledge natively |
| "Research this wine" | Would need to embed an LLM call inside the server | Agent already has web search + synthesis |
| "Check prices" | Would need web scraping code per retailer | Agent has general web browsing |
| "What should I drink next?" | Heuristic scoring ignores taste history | LLM reasons about occasion + preference + urgency |

---

## Efficiency

| Concern | Decision | Rationale |
|---|---|---|
| **Memory** | DuckDB in-process | Reads Parquet directly, ~50MB for full dataset. No server process. |
| **Latency** | stdio transport | Near-zero overhead vs HTTP. |
| **Concurrency** | Single-process | One MCP server per agent session (sufficient for personal use). |
| **Storage** | Parquet + Markdown | Same files serve querying and browsing. No duplication. |
| **Complexity** | One Python package | `cellarbrain` does ETL, querying, MCP serving, and CLI. One `pip install`. |
| **Token cost** | Tools > Resources > Prompts | `cellar_stats` returns <200 tokens for a full summary. |

---

## Connection Model

The MCP server uses two DuckDB connection types with different access levels. See [ADR-004](decisions/004-dual-connection-model.md) for rationale.

| Connection | Used By | Exposes | Purpose |
|---|---|---|---|
| **Agent** | `query_cellar` | Slim + full views, convenience views | Safe for agent-written SQL — no raw tables |
| **Internal** | All other tools | Agent views + `etl_run`, `change_log` | Used by `cellar_stats`, `find_wine`, etc. |

Agent views: `wines` (slim), `bottles` (slim), `wines_full`, `bottles_full`, plus convenience views (`wines_stored`, `bottles_stored`, `bottles_consumed`, `bottles_on_order`, `wines_on_order`, `wines_drinking_now`, `wines_wishlist`). See [query-layer.md](query-layer.md) for full view definitions.
