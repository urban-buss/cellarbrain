# Computed Properties

Pure functions in `computed.py` — no I/O, no side effects. Invoked during ETL via `enrich_wines()`.

## Grape Properties

### `compute_grape_type(grapes) → str`

Stored as `grape_type` in Parquet; aliased as `blend_type` in DuckDB views.

| Input | Output |
|-------|--------|
| `[]` | `"unknown"` |
| 1 grape | `"varietal"` |
| 2+ grapes | `"blend"` |

### `compute_primary_grape(grapes) → str | None`

Evaluated in order:

1. No grapes → `None`
2. Single varietal → that grape name
3. Blend without percentages → first-mentioned grape
4. Blend with one grape > 50% → that grape
5. Blend with no grape > 50% → `None`

### `compute_grape_summary(grapes) → str | None`

| Grapes | Example Output |
|--------|---------------|
| 0 | `None` |
| 1 | `"Nebbiolo"` |
| 2 | `"Merlot / Cabernet Franc"` |
| 3+ with primary | `"Syrah blend"` |
| 3+ without primary | `"Cabernet Sauvignon / Merlot / …"` |

## Grape-Ambiguity Detection

`build_grape_ambiguous_names(wines, wine_grapes, grape_names) → set[tuple[str, str]]`

Returns `(winery_name, wine_name)` pairs where the same combo exists with different primary grapes across vintages. Used in Pass 2 to decide whether grape info must be appended to `full_name`.

## Full Name

`compute_full_name(winery, name, subregion, classification, grape_type, primary_grape, grape_summary, vintage, is_nv, *, name_needs_grape, classification_short, max_full_name_length) → str`

Decision tree:

```
Has winery?
├─ No → "Unknown Wine"
└─ Yes → include winery
         Has cuvée name?
         ├─ Yes → include name
         │        Grape-ambiguous? → append grape_summary
         └─ No (need disambiguator)
              Has subregion?
              ├─ Yes → include subregion (+ short_class if available)
              └─ No
                   Has short classification?
                   ├─ Yes → include it
                   └─ No
                        Is varietal with primary grape?
                        ├─ Yes → include primary_grape
                        └─ No → winery-only fallback
Append vintage (or "NV")
Truncate at max_full_name_length on word boundary with "…"
```

### `shorten_classification(classification, classification_short) → str | None`

Looks up the full classification string in the `classification_short` dict. Returns the mapped short form, or `None` if not mapped (meaning: omit from display name).

Covers ~50 classifications across France, Italy, Spain, Germany, Austria, Switzerland, and New World.

## Drinking Status

`compute_drinking_status(drink_from, drink_until, optimal_from, optimal_until, current_year) → str`

| Status | Condition |
|--------|-----------|
| `"too_young"` | `current_year < drink_from` |
| `"drinkable"` | `drink_from ≤ current_year` but `current_year < optimal_from` |
| `"optimal"` | `optimal_from ≤ current_year ≤ optimal_until` |
| `"past_optimal"` | `current_year > optimal_until` but `current_year ≤ drink_until` |
| `"past_window"` | `current_year > drink_until` |
| `"unknown"` | All four window fields are `None` |

## Age

`compute_age_years(vintage, current_year) → int | None`

Returns `current_year - vintage`, or `None` for NV wines.

## Price Tier

`compute_price_tier(price, tiers) → str`

Default tiers (configurable via `cellarbrain.toml`):

| Label | Max (CHF) |
|-------|-----------|
| `"budget"` | 15 |
| `"everyday"` | 27 |
| `"premium"` | 40 |
| `"fine"` | ∞ (catch-all) |
| `"unknown"` | no price data |

Returns `"unknown"` when price is `None`.

## On-site Detection

`compute_is_onsite(cellar_name, offsite_cellars, in_transit_cellars) → bool`

Returns `False` if `cellar_name` is in the `offsite_cellars` or `in_transit_cellars` tuple. `None` cellar → `True`.

## In-Transit Detection

`compute_is_in_transit(cellar_name, in_transit_cellars) → bool`

Returns `True` if `cellar_name` is in the `in_transit_cellars` tuple. Used to identify bottles on order or in transit — typically assigned to a virtual cellar (e.g. “99 Orders & Subscriptions”). `None` cellar → `False`.

## Currency Conversion

`convert_to_default_currency(price, source_currency, default_currency, rates) → Decimal | None`

- Same currency → pass through
- Different → `price × rate`, quantised to 2 decimal places
- Missing rate → `ValueError`
- `None` price or currency → `None`

## Pipeline Integration: `enrich_wines()`

Modifies wine dicts **in-place** across three passes:

| Pass | Fields Set | Dependencies |
|------|-----------|-------------|
| 1 — Grape properties | `grape_type`, `primary_grape`, `grape_summary` | `wine_grapes`, `grape_names` |
| 2 — Full name | `full_name` | Pass 1 results, `winery_names`, `appellation_map`, `classification_short` |
| 3 — Status & pricing | `drinking_status`, `age_years`, `list_price`, `list_currency`, `price_tier` | `current_year`, `settings` (currency config, price tiers) |

Pass 3 only runs when `current_year` is provided.
