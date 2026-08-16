Use `$schema-change`, `$etl-synthetic-smoke`, and `cb_data_reviewer`.

Data change: <field/schema behavior>

Trace every producer and consumer, specify compatibility and migration
semantics, implement with synthetic fixtures, and verify old, new, mixed,
null, and malformed cases. Include MCP/dashboard checks when they consume the
changed shape.
