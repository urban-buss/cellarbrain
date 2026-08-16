---
name: change-impact-testing
description: Map a Cellarbrain code change to affected layers and the smallest sufficient test plan. Use before implementation, during review, or when deciding which focused tests and full checks are required for ETL, query, MCP, dashboard, promotions, or sommelier work.
---

# Change Impact Testing

1. Identify edited symbols, public entry points, callers, persisted schemas,
   configuration, and user-visible or MCP-visible behavior.
2. Trace upstream producers and downstream consumers with `rg`; include tests
   that assert the old contract.
3. Classify risk: local implementation, cross-module behavior, persisted data,
   public MCP/CLI, or cross-submodule contract.
4. Build a verification ladder: direct unit test, adjacent integration tests,
   subsystem tests, lint, then full suite. Add dashboard smoke checks only for
   affected UI/server behavior.
5. Include negative, empty, malformed, boundary, and compatibility cases where
   relevant. Use synthetic fixtures only.
6. Record exact commands and explain any skipped layer.
