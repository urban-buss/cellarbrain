# Dashboard Instructions

This is a server-rendered Starlette/HTMX dashboard. Preserve progressive
enhancement, accessible HTML, stable routes, and useful empty/error states.
Do not introduce a Node build pipeline unless the feature genuinely requires
one and the user approves the architectural change.

Use development data only and bind local smoke servers to `127.0.0.1:8081`.
Never start a server on a production port. Check route tests, template
rendering, static assets, and behavior when Parquet output is absent.
