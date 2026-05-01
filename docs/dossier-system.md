# Dossier System

Two types of Markdown dossier files provide structured, per-wine documentation with a split ownership model between ETL and AI agents.

## Two Dossier Types

### Per-Vintage Dossiers

One file per wine. Located in `output/wines/{cellar|archive}/`:
- **Cellar**: `output/wines/cellar/NNNN-slug.md` — wines with ≥1 stored bottle
- **Archive**: `output/wines/archive/NNNN-slug.md` — wines with no stored bottles

Filename: `{wine_id:04d}-{slug}.md` where slug is accent-folded, lowercase, hyphen-separated (max 60 chars).

Contains YAML frontmatter, ETL-owned sections, mixed sections, and pure agent sections.

### Companion Dossiers

One file per tracked wine (cross-vintage identity). Located in `output/wines/tracked/`.

Filename: `{tracked_wine_id:05d}-{slug}.md`.

Contains YAML frontmatter, a vintages overview table, and agent-owned research sections. No ETL-owned content beyond the scaffold.

## Ownership Model

### ETL-Owned Sections

Regenerated on every ETL run. Agent content in these sections will be overwritten.

| Section | Content |
|---------|---------|
| Identity | Wine ID, winery, name, grape info, vintage, category, volume, etc. |
| Origin | Country, region, subregion, classification |
| Grapes | Grape varieties with percentages |
| Characteristics | Alcohol, acidity, sugar, ageing, farming, serving temp, opening |
| Drinking Window | From/until years, optimal window, current status |
| Cellar Inventory | Stored bottles with cellar, shelf, price, provider; on-order/in-transit bottles listed separately |
| Purchase History | Aggregated purchases by date/provider/price |
| Consumption History | Gone bottles with output date/type/comment |
| Owner Notes | User comments from cellarbrain |

### Mixed Sections

Have both an ETL sub-section (data from cellarbrain) and an agent sub-section (research). The ETL part is regenerated; the agent part is preserved via fences.

| H2 Section | ETL Sub-section | Agent Sub-section (H3) | Agent Key |
|------------|----------------|----------------------|-----------|
| Ratings & Reviews | From Cellar Export (pro_rating table) | From Research | `ratings_reviews` |
| Tasting Notes | Personal Tastings (tasting table) | Community Tasting Notes | `tasting_notes` |
| Food Pairings | From Owner Notes (comment field) | Recommended Pairings | `food_pairings` |

### Pure Agent Sections

Fully owned by agents. Preserved across ETL runs.

| Section Heading | Key | Agent Tag |
|----------------|-----|-----------|
| Producer Profile | `producer_profile` | `agent:research` |
| Vintage Report | `vintage_report` | `agent:research` |
| Wine Description | `wine_description` | `agent:research` |
| Market & Availability | `market_availability` | `agent:research` |
| Similar Wines | `similar_wines` | `agent:recommendation` |
| Agent Log | `agent_log` | `agent` |

## Agent Section Fence Syntax

Agent-owned content is delimited by HTML comment fences:

```markdown
## Producer Profile
<!-- source: agent:research -->

Content written by the research agent goes here.
It is **preserved** across ETL runs.

<!-- source: agent:research — end -->
```

For mixed sections, the agent sub-section uses an H3 heading:

```markdown
## Ratings & Reviews

### From Cellar Export
<!-- source: etl — do not edit below this line -->
| Source | Score | Review |
|--------|-------|--------|
| Parker | 95/100 | Excellent |
<!-- source: etl — end -->

### From Research
<!-- source: agent:research -->

Agent-written research content here.

<!-- source: agent:research — end -->
```

The ETL fences `<!-- source: etl — ... -->` mark data that is regenerated. The agent fences mark content that is preserved.

## Frontmatter Metadata

YAML frontmatter tracks which agent sections have been populated:

```yaml
---
wine_id: 42
full_name: "Château Margaux 2015"
...
agent_sections_populated:
  - producer_profile
  - vintage_report
agent_sections_pending:
  - wine_description
  - market_availability
  - similar_wines
  - ratings_reviews
  - tasting_notes
  - food_pairings
---
```

When `update_dossier()` writes a section, it moves the key from `agent_sections_pending` to `agent_sections_populated`.

## Companion Dossier Sections

| Section Heading | Key | Agent Tag |
|----------------|-----|-----------|
| Producer Deep Dive | `producer_deep_dive` | `agent:research` |
| Vintage Tracker | `vintage_tracker` | `agent:research` |
| Buying Guide | `buying_guide` | `agent:research` |
| Price Tracker | `price_tracker` | `agent:price` |

Companion dossiers also have `agent_sections_populated` / `agent_sections_pending` in frontmatter, plus `related_wine_ids` and `vintages_tracked` arrays.

## Dossier Lifecycle

1. **Creation** — ETL generates the dossier with all ETL-owned sections filled and agent sections scaffolded with placeholder text.

2. **Agent population** — Agents use `update_dossier` (MCP tool) or `dossier_ops.update_dossier()` to write research content into agent-owned sections.

3. **Preservation** — On the next ETL run, `_extract_agent_sections()` parses existing agent blocks and `render_wine_dossier()` re-inserts them into the regenerated file.

4. **Move handling** — When a wine gains/loses physically stored bottles, the dossier moves between `cellar/` and `archive/`. On-order/in-transit bottles do not count as stored for routing purposes — a wine with only on-order bottles is routed to `archive/`. Agent content is preserved during the move.

5. **Slug change** — When winery or wine name changes, a new slug is generated. The old file is found by `_find_existing_dossier()` (ID-prefix glob), content read, old file deleted, new file written.

6. **Deletion marking** — Soft-deleted wines get `deleted: true` in frontmatter and a warning banner below the H1. Files are not removed.

## Section Filtering API

`dossier_ops.read_dossier_sections(wine_id, data_dir, sections=["identity", "producer_profile"])` returns only the requested H2 sections plus frontmatter.

Valid section keys:

| Scope | Keys |
|-------|------|
| ETL | `identity`, `origin`, `grapes`, `characteristics`, `drinking_window`, `cellar_inventory`, `purchase_history`, `consumption_history`, `owner_notes` |
| Mixed | `ratings_reviews`, `tasting_notes`, `food_pairings` |
| Agent | `producer_profile`, `vintage_report`, `wine_description`, `market_availability`, `similar_wines`, `agent_log` |

Unknown keys are silently ignored. When `sections` is `None`, the full dossier is returned.

## Path Traversal Protection

`dossier_ops.resolve_dossier_path()` reads `dossier_path` from `wine.parquet` and validates with `is_relative_to()` to prevent path traversal attacks.
