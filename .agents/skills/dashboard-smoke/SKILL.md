---
name: dashboard-smoke
description: Verify the Cellarbrain Starlette/HTMX dashboard with isolated development configuration. Use after route, template, static asset, query/view-model, navigation, accessibility, or server startup changes.
---

# Dashboard Smoke

Run `python3 scripts/check_dashboard.py`. It performs non-network route and
template discovery plus focused dashboard tests; it does not start Docker or a
production service.

Review successful, empty-data, missing-data, invalid-input, and server-error
states. Check semantic headings and labels, keyboard navigation, useful link
and button text, HTMX fallbacks, and stable routes. If a live smoke is needed,
use the development shell and bind only to `127.0.0.1:8081`, then stop the
process you started.
