# Promotions Instructions

Promotion inputs are untrusted external data. Keep parsers deterministic,
retain provenance, normalize currency and dates explicitly, and avoid fuzzy
matches that cannot be explained. Tests must use synthetic or sanitized
fixtures, never personal emails, credentials, or production captures.

Changes should cover parser failure, duplicate offers, missing fields,
boundary dates, and ambiguous wine matching. Review MCP and dashboard
consumers when the normalized promotion schema changes.
