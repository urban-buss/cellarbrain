# ADR-003: Dossier Ownership Split

## Status
Accepted

## Context
Per-wine dossiers need both EDL-generated factual data (identity, origin, inventory) and agent-written research (producer profile, vintage report). ETL must regenerate its sections on every run without destroying agent content.

## Decision
Dossier sections are classified into three ownership types:
- **ETL-owned**: regenerated completely on every run
- **Mixed**: have both an ETL sub-section and an agent sub-section under separate headings
- **Agent-owned**: fully written by agents, preserved across runs

Agent content is delimited by HTML comment fences: `<!-- source: agent:research -->` ... `<!-- source: agent:research — end -->`. During regeneration, `_extract_agent_sections()` parses existing fences, and `render_wine_dossier()` re-injects them into the new layout.

Allowed section keys are defined in `Settings.agent_sections` and enforced by `dossier_ops.ALLOWED_SECTIONS`.

## Consequences
- Agents can only write to declared sections; ETL content is protected from accidental overwrite
- Adding a new agent section requires updating `_default_agent_sections()` in `settings.py` and the dossier template in `markdown.py`
- Mixed sections require careful H2/H3 nesting to separate ETL and agent sub-sections
