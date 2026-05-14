# Food Pairing — RAG Retrieval Engine

## Overview

The food-pairing subsystem provides **SQL-based retrieval-augmented generation** (RAG) for finding cellar wines that match a described dish. It requires no ML model — strategies use existing columns (`category`, `primary_grape`, `food_tags`, `food_groups`, `country`, `region`) to score and rank candidates.

## Architecture

```
Dish description + classification
        │
        ▼
┌─────────────────────────────┐
│  pairing.retrieve_candidates │  (src/cellarbrain/pairing.py)
│                              │
│  ┌─────────────────────────┐│
│  │ Strategy 1: category    ││  → Filter by wine type (red/white/rosé/sparkling/sweet)
│  │ Strategy 2: grapes      ││  → Match primary_grape against protein→grape mapping
│  │ Strategy 3: food_tags   ││  → Keyword search in food_tags array column
│  │ Strategy 4: food_groups ││  → Match protein/weight/cuisine in food_groups array
│  │ Strategy 5: region      ││  → Match cuisine→region mapping (regional affinity)
│  └─────────────────────────┘│
│                              │
│  _merge_and_rank()           │  → Union results, count matching signals, sort
└─────────────────────────────┘
        │
        ▼
  List[PairingCandidate]  (top N wines with match_signals)
```

## Input Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `dish_description` | `str` | Free-text description of the dish (used for food_tag keyword matching) |
| `category` | `str \| None` | Wine category: `red`, `white`, `rose`, `sparkling`, `sweet` |
| `weight` | `str \| None` | Dish weight: `light`, `medium`, `heavy` |
| `protein` | `str \| None` | Primary protein: `red_meat`, `poultry`, `fish`, `seafood`, `pork`, `game`, `vegetarian`, `cheese` |
| `cuisine` | `str \| None` | Cuisine style: `French`, `Italian`, `Swiss`, `Spanish`, `Argentine` |
| `grapes` | `list[str] \| None` | Preferred grape varieties |
| `limit` | `int` | Maximum candidates to return (default 10) |

## Output

`PairingCandidate` frozen dataclass with fields:

- `wine_id`, `wine_name`, `vintage`, `category`, `country`, `region`
- `primary_grape`, `bottles_stored`, `price`, `drinking_status`, `best_pro_score`
- `food_tags`, `food_groups`
- `match_signals`: list of strategy names that matched (e.g. `["category", "food_groups", "region"]`)
- `signal_count`: number of matching strategies (used for ranking)

## Strategy Details

### 1. Category Strategy

Maps `protein` to expected wine categories using `PROTEIN_CATEGORIES`:
- `red_meat` / `game` → red
- `fish` / `seafood` / `vegetarian` → white
- `poultry` / `pork` / `cheese` → red, white (both acceptable)

If `category` is explicitly provided, it takes precedence.

### 2. Grapes Strategy

Uses `CATEGORY_GRAPES` and `PROTEIN_CATEGORIES` to derive grape lists per protein type. If explicit `grapes` list is provided, uses that directly. Matches against `primary_grape` column.

### 3. Food Tags Strategy

Tokenises `dish_description` into keywords, filters stopwords, and searches the `food_tags` array column using DuckDB `list_contains`. Matches tag substrings for fuzzy matching.

### 4. Food Groups Strategy

Matches `protein`, `weight`, and `cuisine` against the `food_groups` array column. These are set by the ETL pipeline's food-group derivation logic.

### 5. Region Strategy

Maps `cuisine` to wine regions using `CUISINE_REGIONS`:
- `French` → France
- `Italian` → Italy
- `Swiss` → Switzerland
- `Spanish` → Spain
- `Argentine` → Argentina

Matches against `country` column.

## MCP Tool

The `pairing_candidates` MCP tool wraps `retrieve_candidates()` and returns a Markdown table. It accepts the same parameters and returns formatted output suitable for LLM consumption.

## Dashboard Page

`/pairing` provides an interactive HTMX form with:
- Dish description input (required)
- Category, weight, protein, cuisine dropdowns
- Grapes text input (comma-separated)
- Results rendered as a table with signal badges

## Relationship to Sommelier Model

The RAG engine (`pairing_candidates`) is **always available** and is the primary retrieval tool. The optional sommelier model (`suggest_wines`/`suggest_foods`) provides embedding-based similarity search as a supplementary signal. The agent skill uses `pairing_candidates` first, then optionally enriches with `suggest_wines` if the model is trained.

## Hybrid Retrieval (RAG + Embedding Re-rank)

When the sommelier model is available, the `pairing_candidates` and `pair_wine` MCP tools transparently switch to a hybrid pipeline implemented in `cellarbrain/hybrid_pairing.py`:

1. `pairing.retrieve_candidates(...)` returns up to `sommelier.rerank_pool_size` (default 30) RAG candidates.
2. The dish description is encoded with the sommelier model. Candidate wines are batch-encoded with the same `build_wine_text()` function used at index-build time.
3. Each candidate is scored as `blend * embed_similarity + (1 − blend) * normalized_signal_count` where `blend = sommelier.rerank_blend` (default 0.5).
4. Re-ranked candidates are returned with an extra `embed:0.NN` entry appended to `match_signals`.
5. Tool output ends with a one-line trailer indicating the mode (`hybrid` or `rag`).

If the sommelier model is missing, disabled, or the embedding step raises, the tool returns the pure-RAG ranking (mode `rag`) so callers never fail. Configuration lives under `[sommelier]` in TOML — see `docs/settings-reference.md`.

## Dinner Party Flight Planning

The `plan_dinner` MCP tool builds on the hybrid pairing engine to plan complete multi-course wine flights. It retrieves candidates for each course independently, then selects wines using a greedy algorithm that enforces:

- **Progression**: wines ordered light → heavy using a computed wine-weight model (1–10 scale)
- **Deduplication**: no wine or winery repeated across courses
- **Budget awareness**: total spend respects the chosen budget tier
- **Preparation timeline**: chilling and decanting guidance with timing

See `cellarbrain/dinner.py` for the implementation and `docs/settings-reference.md` → `DinnerConfig` for configuration.
