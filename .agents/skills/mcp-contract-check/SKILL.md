---
name: mcp-contract-check
description: Review and test Cellarbrain MCP tools and structured response contracts. Use after changes to tool registration, parameters, validation, response schemas, dossier operations, errors, protocol dependencies, or Recipebrain interoperability.
---

# MCP Contract Check

Run `python3 scripts/check_contract.py`. It checks the installed MCP major
version, imports the Cellarbrain MCP module, and runs focused MCP tests.

Then inspect tool names/descriptions, input defaults and limits, structured
output types, empty and error responses, stable identifiers, and legacy
consumer behavior. Treat every externally visible shape as a public contract.
Prefer additive changes; document and test migrations for breaking changes.
