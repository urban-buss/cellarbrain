# Field Mapping: Raw CSV → Target Data Model

Complete column-by-column mapping from cellarbrain CSV exports to the target relational model. See [entity-model.md](entity-model.md) for target schemas and [etl-pipeline.md](etl-pipeline.md) for processing order.

The `reader` module remaps all CSV headers to canonical column names before passing rows to `transform`. The "Raw Field" column below shows the original CSV header; the "Canonical Key" is what `transform` receives (defined in `reader.VINOCELL_COLUMN_MAP`).

---

## Shared Fields (cols 1–47) → `wine` + lookup entities

| # | Raw Field | Fill % | Target Entity | Target Column | Transform |
|---|---|---|---|---|---|
| 01 | Winery | 99% | `winery` → `wine.winery_id` | FK lookup | Normalize to `winery` table, link by name |
| 02 | Name | 71% | `wine` | `name` | Direct, NULL if empty |
| 03 | Producer | 0% | — | **DROPPED** | Always empty |
| 04 | Year | 99% | `wine` | `vintage` + `is_non_vintage` | `int()` or NULL + `true` for "Non vintage" |
| 05 | Price | 98% | `wine` | `original_list_price` → `list_price` | `decimal(value)` → convert to default currency |
| 06 | Currency | 100% | `wine` | `original_list_currency` → `list_currency` | Direct string → set to default currency |
| 07 | Grapes | 87% | `grape` + `wine_grape` | FK + `percentage` | Parse blend string → grape lookup + junction rows |
| 08 | Country | 99% | `appellation` | `country` | Direct string |
| 09 | Region | 97% | `appellation` | `region` | Direct, NULL if empty |
| 10 | Subregion | 86% | `appellation` | `subregion` | Direct, NULL if empty |
| 11 | Classification | 44% | `appellation` | `classification` | Direct, NULL if empty |
| 11b | Classification | 44% | `wine` | `_raw_classification` | Direct string — raw CSV value preserved for fingerprint disambiguation |
| 12 | Category | 100% | `wine` | `category` | Map: `"Red wine"→"red"` etc. |
| 13 | Subcategory | 2% | `wine` | `subcategory` | Lowercase + underscore |
| 14 | Specialty | 2% | `wine` | `specialty` | Map to slug enum |
| 15 | Sweetness | 5% | `wine` | `sweetness` | Lowercase + underscore |
| 16 | Effervescence | 2% | `wine` | `effervescence` | Lowercase |
| 17 | Volume | 100% | `wine` | `volume_ml` | Parse: `"750mL"→750`, `"Magnum"→1500` |
| 17b | Volume | 100% | `wine` | `_raw_volume` | Direct string — raw CSV value preserved for fingerprint disambiguation |
| 18 | Container | 35% | `wine` | `container` | Lowercase |
| 19 | Hue | 35% | `wine` | `hue` | Lowercase + underscore |
| 20 | Capsule | 0% | — | **DROPPED** | Always empty |
| 21 | Cork | 4% | `wine` | `cork` | Map: `"Natural cork"→"natural_cork"` |
| 22 | Alcohol | 66% | `wine` | `alcohol_pct` | Strip `" %"` → `float` |
| 23 | Acidity | <1% | `wine` | `acidity_g_l` | Strip `" g/l"` → `float` |
| 24 | pH | 0% | — | **DROPPED** | Always empty |
| 25 | Sugar | <1% | `wine` | `sugar_g_l` | Strip `" g/l"` → `float` |
| 26 | SO2 | 0% | — | **DROPPED** | Always empty |
| 27 | Ageing type | 14% | `wine` | `ageing_type` | Map: `"Wood barrel"→"wood_barrel"` |
| 28 | Ageing months | 9% | `wine` | `ageing_months` | Strip `" Months"` → `int` |
| 29 | Winemaking infos | <1% | `wine` | `winemaking_notes` | Direct string |
| 30 | Viticulture | 0% | — | **DROPPED** | Always empty |
| 31 | Farming type | 2% | `wine` | `farming_type` | Map: `"Biodynamic farming"→"biodynamic"` etc. |
| 32 | Soil | 0% | — | **DROPPED** | Always empty |
| 33 | Temperature | <1% | `wine` | `serving_temp_c` | `int(value)` |
| 34 | Opening type | 1% | `wine` | `opening_type` | Lowercase |
| 35 | Opening time | <1% | `wine` | `opening_minutes` | Parse `"1h00min"` → `60` |
| 36 | Drink from | 97% | `wine` | `drink_from` | `int(value)` |
| 37 | Drink until | 98% | `wine` | `drink_until` | `int(value)` |
| 38 | Optimal from | 97% | `wine` | `optimal_from` | `int(value)` |
| 39 | Optimal until | 97% | `wine` | `optimal_until` | `int(value)` |
| 40 | EAN/UPC | 0% | — | **DROPPED** | Always empty |
| 41 | REF | 0% | — | **DROPPED** | Always empty |
| 42 | Favorite | 100% | `wine` | `is_favorite` | `value == "Yes"` |
| 43 | Wishlist | 100% | `wine` | `is_wishlist` | `value == "Yes"` |
| 44 | Web | 0% | — | **DROPPED** | Always empty |
| 45 | Comment | 15% | `wine` | `comment` | Direct string |
| 46 | Pairings | 0% | — | **DROPPED** | Always empty |
| 47 | Groups | 0% | — | **DROPPED** | Always empty |
| — | *(derived)* | 100% | `wine` | `dossier_path` | `{subfolder}/{wine_id:04d}-{slug}.md` |
| — | *(derived)* | 100% | `wine` | `wine_slug` | Accent-folded slug from `Winery + Name + Year` — used for ID stabilisation |
| — | *(derived)* | 87% | `wine` | `_raw_grapes` | Full raw CSV `Grapes` string — preserved for fingerprint disambiguation |

---

## Wine-Only Fields (cols 48–53) → `tasting` + `pro_rating`

| # | Raw Field | Fill % | Target Entity | Target Column | Transform |
|---|---|---|---|---|---|
| W48 | Bottles | 100% | — | **DROPPED** | Computed: `COUNT(bottle)` per wine |
| W49 | Location info | 73% | — | **DROPPED** | Computed: aggregate from `bottle.cellar + shelf` |
| W50 | Tastings | 100% | — | **DROPPED** | Computed: `COUNT(tasting)` per wine |
| W51 | Tasting | <1% | `tasting` | Multiple columns | Parse structured text → `tasting_date`, `note`, `score`, `max_score` |
| W52 | Pro Ratings (count) | 100% | — | **DROPPED** | Computed: `COUNT(pro_rating)` per wine |
| W53 | Pro Ratings (text) | 2% | `pro_rating` | Multiple columns | Parse structured text → `source`, `score`, `max_score`, `review_text` |

---

## Bottle-Stored Fields (cols 48–60) → `bottle` + lookups

| # | Raw Field | Fill % | Target Entity | Target Column | Transform |
|---|---|---|---|---|---|
| B48 | Last tasting | <1% | — | **DROPPED** | Derivable from `tasting` table |
| B49 | Last rating | 6% | `pro_rating` | Multiple columns | Parse `"Source: score/max"` → deduplicate with W53 |
| B50 | Bottle number | <1% | `bottle` | `bottle_number` | `int()`, NULL if empty |
| B51 | Reference | 0% | — | **DROPPED** | Always empty |
| B52 | Cellar | 99% | `cellar` → `bottle.cellar_id` | FK lookup | Normalize to `cellar` table |
| B53 | Shelf | 90% | `bottle` | `shelf` | Direct string |
| B54 | Location | 100% | — | **DROPPED** | Computed: `cellar.name || ', ' || bottle.shelf` |
| B55 | Provider | 63% | `provider` → `bottle.provider_id` | FK lookup | Normalize to `provider` table |
| B56 | Input date | 100% | `bottle` | `purchase_date` | Parse `DD.MM.YYYY` → ISO date |
| B57 | Input type | 100% | `bottle` | `acquisition_type` | Map: `"Market price"→"market_price"` etc. |
| B58 | Input price | 98% | `bottle` | `original_purchase_price` → `purchase_price` | Decimal → convert to default currency |
| B59 | Input currency | 100% | `bottle` | `original_purchase_currency` → `purchase_currency` | Direct → set to default |
| B60 | Input comment | 39% | `bottle` | `purchase_comment` | Direct string |
| — | *(derived)* | — | `bottle` | `status` | Always `"stored"` |

---

## Bottle-Gone Fields (cols 48–63) → `bottle` + lookups

| # | Raw Field | Fill % | Target Entity | Target Column | Transform |
|---|---|---|---|---|---|
| G48 | Last tasting | 3% | — | **DROPPED** | Derivable from `tasting` table |
| G49 | Last rating | 3% | `pro_rating` | Multiple columns | Parse `"Source: score/max"` → deduplicate |
| G50 | Bottle number | <1% | `bottle` | `bottle_number` | `int()`, NULL if empty |
| G51 | Reference | 0% | — | **DROPPED** | Always empty |
| G52 | Provider | 48% | `provider` → `bottle.provider_id` | FK lookup | Normalize to `provider` table |
| G53 | Input date | 100% | `bottle` | `purchase_date` | Parse `DD.MM.YYYY` → ISO date |
| G54 | Input type | 100% | `bottle` | `acquisition_type` | Map: `"Market price"→"market_price"` etc. |
| G55 | Input price | 97% | `bottle` | `original_purchase_price` → `purchase_price` | Decimal → convert to default currency |
| G56 | Input currency | 100% | `bottle` | `original_purchase_currency` → `purchase_currency` | Direct → set to default |
| G57 | Input comment | 28% | `bottle` | `purchase_comment` | Direct string |
| G58 | Recipient | 0% | — | **DROPPED** | Always empty |
| G59 | Output date | 100% | `bottle` | `output_date` | Parse `DD.MM.YYYY` → ISO date |
| G60 | Output type | 100% | `bottle` | `status` + `output_type` | Map: `"Drunk"→"drunk"`, `"Offered"→"offered"`, `"Removed"→"removed"` |
| G61 | Output price | 0% | — | **DROPPED** | Always empty |
| G62 | Output currency | 0% | — | **DROPPED** | Always empty |
| G63 | Output comment | 14% | `bottle` | `output_comment` | Direct string |

---

## ETL Metadata Columns

Every entity table receives two additional columns not derived from the raw CSV:

| Target Column | Type | Description |
|---|---|---|
| `etl_run_id` | `int` | FK → `etl_run.run_id` — identifies which pipeline run last touched this row |
| `updated_at` | `timestamp` | UTC timestamp of last insert or update |

---

## Summary

| Category | Raw Fields | Kept | Dropped | To Lookup | To Junction |
|---|---|---|---|---|---|
| Shared (1–47) | 47 | 30 → `wine` | 12 (empty) | 3 → `winery`, `appellation` | 1 → `wine_grape` |
| Shared (derived) | — | 5 → `wine` | — | — | — |
| Wine-only (W48–53) | 6 | 0 | 4 (computed) | — | 2 → `tasting`, `pro_rating` |
| Bottle-stored (B48–60) | 13 | 6 → `bottle` | 4 (empty/computed) | 2 → `cellar`, `provider` | 1 → `pro_rating` |
| Bottle-gone (G48–63) | 16 | 9 → `bottle` | 5 (empty/computed) | 1 → `provider` | 1 → `pro_rating` |
| **Total** | **82** | **50 columns** | **25 dropped** | **6 lookups** | **5 parsed** |
