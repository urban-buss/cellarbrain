# ADR-006: Frozen Dataclass Settings

## Status
Accepted

## Context
Configuration is used across many modules. Mutable config objects risk accidental modification, especially when passed between ETL stages. We also need deterministic behaviour for testing.

## Decision
All configuration is expressed as `@dataclass(frozen=True)` instances. The top-level `Settings` composes 13 sub-config dataclasses.

Loading follows a merge chain:
1. Built-in defaults (defined in the dataclass defaults and `_default_*()` functions)
2. TOML config file (scalars replace, tables merge, arrays replace)
3. Environment variables (`VINOCELL_DATA_DIR`, `VINOCELL_CONFIG`)
4. CLI arguments

Unknown TOML keys are rejected via `_validate_keys()` to catch typos.

## Consequences
- Settings are immutable once loaded — no accidental mid-pipeline modification
- Tests can construct Settings with precise overrides using `dataclasses.replace()`
- Adding a new config field requires updating the dataclass, the TOML loader, and docs
- The merge strategy differs by field type, which makes adding array-type configs slightly more complex
