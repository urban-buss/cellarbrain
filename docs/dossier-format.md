# Dossier File Format

Physical file structure, naming conventions, section layout, and frontmatter schema for wine Markdown dossiers. For ownership semantics and agent write-back rules, see [dossier-system.md](dossier-system.md).

---

## File Naming & Location

```
output/wines/
├── cellar/          ← wines with ≥1 stored bottle
│   ├── 0001-spier-21-gables-2020.md
│   ├── 0025-marques-de-murrieta-2016.md
│   └── ...
├── archive/         ← wines with 0 stored bottles
│   ├── 0004-chateau-montrose-1991.md
│   └── ...
└── tracked/         ← companion dossiers (cross-vintage)
    ├── 90001-chateau-margaux.md
    └── ...
```

**Per-vintage pattern:** `{subfolder}/{wine_id:04d}-{slug}.md`

- `subfolder` = `cellar` if wine has ≥1 bottle with `status == "stored"`, else `archive`
- `wine_id` zero-padded to 4 digits
- `slug` = lowercased, ASCII-folded `{winery}-{name}-{vintage}`, max 60 chars, hyphens for spaces/special chars
- Non-vintage wines use `nv` instead of year
- Files move between `cellar/` and `archive/` when bottle status changes; agent sections are preserved across moves

**Companion pattern:** `{tracked_wine_id:05d}-{slug}.md`

---

## YAML Frontmatter

Machine-readable metadata block. Agents can parse this without reading the full file.

```yaml
---
wine_id: 25
winery: "Marques De Murrieta"
name: ""
vintage: 2016
category: red
country: "Spain"
region: "La Rioja"
is_favorite: false
is_wishlist: false
bottles_in_cellar: 2
bottles_on_order: 0
bottles_consumed: 1
bottles_total: 3
list_price: 39.00
list_currency: "CHF"
original_list_price: 39.00
original_list_currency: "CHF"
drinking_status: "optimal"
age_years: 10
price_tier: "premium"
format_group_id: 25
format_siblings:
  - wine_id: 42
    bottle_format: "Magnum"
    volume_ml: 1500
etl_run_id: 1
updated_at: "2025-06-01T12:00:00"
agent_sections_populated:
  - ratings_reviews
  - food_pairings
agent_sections_pending:
  - producer_profile
  - vintage_report
  - wine_description
  - market_availability
  - similar_wines
---
```

`bottles_consumed` counts bottles with status ≠ `stored`. `bottles_on_order` counts bottles with `status = 'stored'` and `is_in_transit = true` (ordered but not yet physically present). `bottles_total` = `bottles_in_cellar` + `bottles_consumed`. `drinking_status`, `age_years`, and `price_tier` are computed fields refreshed by ETL and `cellarbrain recalc`. `format_group_id` is the `wine_id` of the Standard variant when the wine belongs to a format group (multiple bottle sizes); `format_siblings` lists the other members.

---

## Section Layout

The file is divided into clearly delimited sections. Each is tagged with its data source so agents know what they can and cannot overwrite.

| # | Section | Source | Section Key |
|---|---|---|---|
| 1 | YAML Frontmatter | ETL | *(always included)* |
| 2 | Identity | ETL | `identity` |
| 3 | Related Formats | ETL | `related_formats` |
| 4 | Origin | ETL | `origin` |
| 5 | Grapes | ETL | `grapes` |
| 6 | Characteristics | ETL | `characteristics` |
| 7 | Drinking Window | ETL | `drinking_window` |
| 8 | Cellar Inventory | ETL | `cellar_inventory` |
| 9 | Purchase History | ETL | `purchase_history` |
| 10 | Consumption History | ETL | `consumption_history` |
| 11 | Owner Notes | ETL | `owner_notes` |
| 12 | Ratings & Reviews | Mixed | `ratings_reviews` |
| 13 | Tasting Notes | Mixed | `tasting_notes` |
| 14 | Food Pairings | Mixed | `food_pairings` |
| 15 | Producer Profile | Agent | `producer_profile` |
| 16 | Vintage Report | Agent | `vintage_report` |
| 17 | Wine Description | Agent | `wine_description` |
| 18 | Market & Availability | Agent | `market_availability` |
| 19 | Similar Wines | Agent | `similar_wines` |
| 20 | Agent Log | Agent | `agent_log` |

---

## Selective Section Retrieval

The `read_dossier` MCP tool and `cellarbrain dossier` CLI command support an optional `sections` parameter. YAML frontmatter + H1 title + subtitle are **always included** regardless of filter.

```python
# MCP — research agent checking pending sections
read_dossier(wine_id=2, sections=["wine_description", "similar_wines"])
```

```bash
# CLI — inspect identity and drinking window only
cellarbrain dossier 2 --sections identity drinking_window
```

When `sections` is omitted, the full dossier is returned.

### Token Cost Estimate

| Scenario | Full dossier | Filtered (2 sections) | Saving |
|---|---|---|---|
| Unpopulated wine | ~170 lines / ~800 tokens | ~30 lines / ~150 tokens | ~80% |
| Fully researched wine | ~400+ lines / ~2000 tokens | ~60 lines / ~300 tokens | ~85% |

---

## Section Examples

### Identity (ETL)

```markdown
## Identity

| Field | Value |
|---|---|
| **Wine ID** | 25 |
| **Winery** | Marques De Murrieta |
| **Name** | *(no cuvée name)* |
| **Vintage** | 2016 |
| **Category** | Red |
| **Volume** | 750 mL |
```

### Related Formats (ETL)

Only rendered when the wine belongs to a format group (same winery/name/vintage in multiple bottle sizes). The current wine's row is bolded. Sorted by volume.

```markdown
## Related Formats

| Format | Volume | Wine ID | Drinking Window |
|---|---|---|---|
| **Standard** | **750 mL** | **25** | **2024 – 2030** |
| Magnum | 1500 mL | 42 | 2024 – 2030 |
```

### Drinking Window (ETL)

```markdown
## Drinking Window

| Window | From | Until |
|---|---|---|
| **Drinkable** | 2022 | 2036 |
| **Optimal** | 2024 | 2030 |

**Status:** 🟢 In optimal window (current year: 2026)
```

Status emojis: 🔴 Too young · 🟡 Drinkable, not yet optimal · 🟢 Optimal · 🟠 Past optimal · ⚫ Past window · ⚪ No data

### Mixed Section (ETL + Agent)

```markdown
## Ratings & Reviews

### From Cellar Export
<!-- source: etl — do not edit below this line -->
| Source | Score | Review |
|---|---|---|
| James Suckling | 92/100 | — |
<!-- source: etl — end -->

### From Research
<!-- source: agent:research — last updated: 2025-07-15 -->
| Source | Score | Year | Review |
|---|---|---|---|
| Wine Spectator | 93/100 | 2020 | "Deep ruby color with aromas of..." |
| Robert Parker | 94/100 | 2020 | — |
<!-- source: agent:research — end -->
```

### Agent Section

```markdown
## Producer Profile
<!-- source: agent:research — last updated: 2025-07-15 -->

**Marques De Murrieta** is one of the oldest wineries in Rioja, founded in 1852.

- **Founded:** 1852
- **Location:** Logroño, La Rioja, Spain
- **Vineyard area:** 300 ha
- **Winemaker:** María Vargas (since 1994)

<!-- source: agent:research — end -->
```

### Agent Log

```markdown
## Agent Log
<!-- source: agent -->

| Date | Agent | Action |
|---|---|---|
| 2025-07-15 | research | Populated producer_profile, vintage_report, ratings_reviews |
| 2025-08-01 | research | Updated market_availability with Swiss retailer prices |

<!-- source: agent — end -->
```

---

## Agent Section Guidelines

| Section | Guidance |
|---|---|
| `producer_profile` | 150–300 words. Include structured facts (founded, location, vineyard area, winemaker, website, style). |
| `vintage_report` | Focus on the specific region, not generic country-wide. Include quality rating (star scale). Skip for NV wines. |
| `wine_description` | Official winery description + sensory profile (appearance, nose, palate, body, tannins). |
| `ratings_reviews` | Append new sources, update if newer vintage review found. Never remove ETL rows. Include review year. |
| `tasting_notes` | Community notes from Vivino, CellarTracker, etc. Optional. |
| `food_pairings` | Tiered format (Ideal / Excellent / Good / Avoid). Include cuisine affinities. Complement, don't repeat owner notes. |
| `market_availability` | Current prices, retailer links, price trends. Factual only. |
| `similar_wines` | 5–10 comparable wines from other producers. Include why they're similar. |
