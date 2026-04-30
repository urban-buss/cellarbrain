# Query Layer

DuckDB in-process query engine over Parquet files. Provides denormalised views, SQL validation, text search, statistics, and price tracking.

## View Definitions

All views are defined as SQL templates in `flat.py`. Table names are substituted with `read_parquet()` paths at connection time.

Two-layer architecture:

- **Full views** (`wines_full`, `bottles_full`): all columns with backward-compatible aliases. Used by `cellar_stats`, `cellar_churn`, `find_wine`, and agents needing technical detail.
- **Slim views** (`wines`, `bottles`): curated ~23/~20 columns with clean naming. Default surface for agent-submitted SQL.

### `wines_full` View

One row per wine (excludes soft-deleted). Joins wine with winery, appellation, and aggregates from bottle, tasting, and pro_rating.

Key columns:

| Column | Source |
|--------|--------|
| `wine_id`, `wine_name`, `vintage`, `category` | wine (`wine_name` is aliased from `full_name`) |
| `winery_name` | winery.name |
| `country`, `region`, `subregion`, `classification` | appellation |
| `blend_type`, `primary_grape`, `grapes` | wine (computed) |
| `drinking_status`, `age_years`, `price_tier` | wine (computed) |
| `bottle_format`, `price_per_750ml` | wine (computed — volume-normalised pricing) |
| `style_tags` | slim only (computed — `CONCAT_WS` of subcategory, sweetness, effervescence, specialty) |
| `is_favorite`, `is_wishlist`, `tracked_wine_id` | wine |
| `bottles_stored`, `bottles_consumed` | COUNT from bottle (stored excludes in-transit) |
| `bottles_on_order` | COUNT from bottle WHERE stored AND in-transit |
| `cellar_value` | SUM(price) for stored (excludes in-transit) |
| `on_order_value` | SUM(price) for on-order bottles |
| `tasting_count`, `last_tasting_date`, `last_tasting_score`, `avg_tasting_score` | tasting aggregates |
| `pro_rating_count`, `best_pro_score`, `avg_pro_score` | pro_rating aggregates |

### `wines` View (Slim)

Curated 23-column subset of `wines_full` for agent queries:

`wine_id`, `wine_name`, `vintage`, `winery_name`, `category`, `country`, `region`, `subregion`, `primary_grape`, `blend_type`, `drinking_status`, `price_tier`, `price`, `price_per_750ml`, `volume_ml`, `bottle_format`, `bottles_stored`, `bottles_on_order`, `bottles_consumed`, `is_favorite`, `is_wishlist`, `tracked_wine_id`, `style_tags`.

### `bottles_full` View

One row per bottle (excludes deleted wines). Joins bottle with wine, winery, appellation, cellar, provider.

Key columns: `bottle_id`, `wine_id`, `wine_name`, `vintage`, `winery_name`, `country`, `region`, `category`, `primary_grape`, `drinking_status`, `status`, `cellar_name`, `shelf`, `provider_name`, `purchase_date`, `price`, `output_date`, `output_type`, `is_onsite`, `is_in_transit`.

### `bottles` View (Slim)

Curated 20-column subset of `bottles_full` for agent queries:

`bottle_id`, `wine_id`, `wine_name`, `vintage`, `winery_name`, `category`, `country`, `region`, `primary_grape`, `drinking_status`, `price_tier`, `price`, `price_per_750ml`, `volume_ml`, `bottle_format`, `status`, `cellar_name`, `shelf`, `output_date`, `output_type`.

### `tracked_wines` View

One row per tracked wine (excludes deleted). Joins tracked_wine with winery, appellation, and wine aggregates.

Key columns: `tracked_wine_id`, `winery_name`, `wine_name`, `category`, `country`, `region`, `wine_count`, `vintages` (list), `bottles_stored`, `bottles_on_order`.

### `wines_wishlist` View (`_wines_wishlist`)

Wines with `is_wishlist` or `is_favorite` set. Same columns as `wines` slim view (`SELECT * FROM wines WHERE is_wishlist OR is_favorite`).

### Price Views (optional — require `price_observation_*.parquet`)

| View | Description |
|------|-------------|
| `price_observations` | All observations joined with tracked_wine and winery. Ordered by `observed_at DESC`. |
| `latest_prices` | Most recent in-stock observation per (tracked_wine, vintage, size, retailer). |
| `price_history` | Monthly aggregates: min/max/avg price_chf per (tracked_wine, vintage, size, retailer). |

## Convenience Views

Built on top of the slim or full views:

| View | Base | Cols | Definition |
|------|------|------|-----------|
| `wines_stored` | `wines` | 20 | `WHERE bottles_stored > 0` |
| `bottles_stored` | `bottles` (slim) | 17 | `WHERE status = 'stored' AND NOT is_in_transit` |
| `bottles_consumed` | `bottles` (slim) | 17 | `WHERE status != 'stored'` |
| `bottles_on_order` | `bottles` (slim) | 17 | `WHERE status = 'stored' AND is_in_transit` |
| `wines_on_order` | `wines` | 20 | `WHERE bottles_on_order > 0` |
| `wines_drinking_now` | `wines` | 20 | `WHERE drinking_status IN ('optimal','drinkable') AND bottles_stored > 0` |
| `wines_wishlist` | `_wines_wishlist` | 20 | Wishlist/favorite wines |

All convenience views expose slim columns — 23 for wine views, 20 for bottle views — keeping agent responses compact.

### Internal Full-Column Views

`cellar_stats` needs columns like `volume_ml`, `provider_name`, and `is_in_transit` that are not in the slim bottle views. Two internal views provide full `bottles_full` columns (37 cols) with the same filters:

| Internal View | Equivalent To | Used By |
|---------------|---------------|---------|
| `_bottles_stored_full` | `bottles_full WHERE status = 'stored' AND NOT is_in_transit` | `cellar_stats` |
| `_bottles_on_order_full` | `bottles_full WHERE status = 'stored' AND is_in_transit` | `cellar_stats` |

These views are registered on the internal connection only (via `get_connection()`) and are **not** accessible to agents via `get_agent_connection()`.

### Agent View Selection Guide

The `query_cellar` tool docstring steers agents toward the right view:

- **Default to `wines` / `bottles`** (slim) for most queries.
- **Use `wines_full` / `bottles_full`** only when you need:
  - Wine details: `alcohol_pct`, `grapes`, `volume_ml`, `classification`, ageing, `serving_temp_c`.
  - Bottle details: `provider_name`, `purchase_date`, `purchase_comment`, `volume_ml`, `is_in_transit`.
  - Aggregates: `cellar_value`, `on_order_value`, tasting/rating scores.

### Schema Introspection

The `schema://views` MCP resource returns the complete column reference (name, type, hint) for every agent-visible view, auto-generated from the live DuckDB connection. Agents should read this once at session start instead of querying `information_schema` directly.

## Security Model

### SQL Validation

`validate_sql()` enforces read-only access:

- **Rejects**: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE` (regex at start)
- **Requires**: must start with `SELECT` or `WITH`
- **Single statement**: no `;` within the SQL body
- Raises `QueryError` on violation

### Parameterised Queries

`find_wine()` uses DuckDB parameter binding to prevent SQL injection. Multi-word queries are tokenised (AND across tokens, OR across 12 text columns: wine name, winery, country, region, subregion, classification, category, primary grape, subcategory, sweetness, effervescence, specialty) with `strip_accents()` for accent-insensitive matching:

```sql
WHERE (strip_accents(wine_name) ILIKE strip_accents($1) OR strip_accents(winery_name) ILIKE strip_accents($1) ...)
  AND (strip_accents(wine_name) ILIKE strip_accents($3) OR ...)
ORDER BY bottles_stored DESC LIMIT $N
```

### Accent-Insensitive Search in Raw SQL

Both `strip_accents()` (DuckDB native) and `unaccent()` (PostgreSQL-compatible alias) are available in all connections. Use either in `query_cellar` for accent-insensitive matching:

```sql
SELECT * FROM wines WHERE strip_accents(wine_name) ILIKE '%chateau%'
SELECT * FROM wines WHERE unaccent(wine_name) ILIKE '%chateau%'   -- equivalent
```

Note: plain `ILIKE` without `strip_accents`/`unaccent` is accent-sensitive — `'%chateau%'` will **not** match `Château`.

### Agent Connection Restrictions

`get_agent_connection()` exposes **views only** — raw entity tables (`wine`, `bottle`, etc.) are not registered. Agents see the two-layer view structure:

- Full views: `wines_full`, `bottles_full` (all columns)
- Slim views: `wines`, `bottles` (curated columns)
- Convenience views: `wines_stored`, `bottles_stored`, etc.
- Tracked/wishlist/price views when data exists

`get_connection()` (internal) adds `etl_run` and `change_log` views for ETL metadata.

## Text Search

`find_wine(con, query, limit, fuzzy, synonyms)` tokenises the query on whitespace, applies synonym normalisation, runs intent detection for attribute-based queries, and searches `wines_full` across: `wine_name`, `winery_name`, `country`, `region`, `subregion`, `classification`, `category`, `primary_grape`, and exact vintage match. Each text token must match at least one column (AND semantics). Uses `strip_accents()` for accent-insensitive matching and `normalize_quotes()` for typographic-quote-insensitive matching. When `fuzzy=True` and no ILIKE results are found, falls back to Jaro-Winkler similarity (threshold 0.85). Returns columns: `wine_id`, `winery_name`, `wine_name`, `vintage`, `category`, `country`, `region`, `primary_grape`, `bottles_stored`, `drinking_status`, `tracked_wine_id`. Default sort: `bottles_stored DESC, vintage DESC` (overridden by intent ORDER BY when applicable).

### Query Token Normalisation

`_normalise_query_tokens(tokens, synonyms)` performs O(n) dict lookups to transform query tokens before they reach the SQL layer. Each token is lowercased for matching: if found in the synonyms dict, its mapping determines the action:

| Mapping | Action | Example |
|---------|--------|---------|
| Non-empty string | **Expansion** — token replaced by mapping value (split on whitespace for multi-word) | `spätburgunder → ["Pinot", "Noir"]` |
| Empty string `""` | **Stopword removal** — token dropped | `weingut → (removed)` |
| Not in dict | **Passthrough** — token kept as-is | `2019 → 2019` |

Safety guard: if all tokens would be removed as stopwords, the original tokens are returned unchanged to avoid an empty query.

Synonym sources (merged, highest priority wins):
1. Custom JSON file (`data_dir/search-synonyms.json`) — managed via MCP `search_synonyms` tool
2. TOML `[search.synonyms]` — merged with defaults at load time
3. Built-in defaults (~87 entries covering DE→EN countries, grapes, categories, regions, intent triggers, and stopwords)

### Query Intent Detection

After synonym normalisation, `_extract_intents(tokens, param_idx)` scans the token list for known attribute-based patterns and generates parameterised SQL WHERE/ORDER BY clauses. Matched tokens are consumed and excluded from the ILIKE text search engine.

Processing pipeline: `query → synonyms → intent extraction → remaining tokens → ILIKE engine`

| Pattern | Example Query | Filter | Sort Override |
|---------|---------------|--------|---------------|
| `ready to drink` | "ready to drink" | `drinking_status IN ('optimal', 'drinkable')` | — |
| `too young` | "too young" | `drinking_status = 'too_young'` | — |
| `past optimal` | "past optimal" | `drinking_status = 'past_optimal'` | — |
| `drinkable` | "drinkable" | `drinking_status IN ('optimal', 'drinkable')` | — |
| `optimal` | "optimal" | `drinking_status = 'optimal'` | — |
| `under N` / `below N` | "under 30" | `price <= N AND price IS NOT NULL` | — |
| `cheaper than N` | "cheaper than 50" | `price <= N AND price IS NOT NULL` | — |
| `budget` | "budget" | `price_tier = 'budget'` | — |
| `top rated` / `best rated` / `highest rated` | "top rated" | `best_pro_score IS NOT NULL` | `best_pro_score DESC` |
| `low stock` / `last bottle` / `running low` | "low stock" | `bottles_stored BETWEEN 1 AND 2` | `bottles_stored ASC` |

Intents combine with free-text tokens: `"Barolo ready to drink"` filters drinking status AND searches "Barolo" via ILIKE. Multiple intents accumulate (e.g., `"ready to drink under 30"`).

German intent triggers are handled via the synonym layer: `trinkreif` → `"ready to drink"`, `günstig` → `"budget"`, which then match the intent patterns above.

Numeric values in price patterns exclude vintage-like numbers (≥ 1000) to prevent `"under 2020"` from being interpreted as a price filter.

### Concept Expansion

After intent extraction, remaining tokens are checked against two concept dictionaries before entering the ILIKE engine.

**System concepts** map a keyword directly to a WHERE clause (like mini-intents):

| Keyword | Filter |
|---------|--------|
| `tracked` | `tracked_wine_id IS NOT NULL` |
| `favorite` / `favourite` / `favorites` / `favourites` | `is_favorite = true` |
| `wishlist` | `is_wishlist = true` |

**Wine-style concepts** expand a single token into an OR across the original term plus concrete wine names/types:

| Concept | Expansion Terms |
|---------|----------------|
| `sparkling` | Prosecco, Champagne, Crémant, Cava, Spumante, Sekt, Franciacorta |
| `dessert` | Sauternes, Tokaji, Moscato, Eiswein, Passito, Vin Santo, Recioto, Beerenauslese, Trockenbeerenauslese, late harvest |
| `fortified` | Port, Sherry, Madeira, Marsala, Vermouth |
| `sweet` | Sauternes, Tokaji, Moscato, Eiswein, Passito, Vin Santo, Beerenauslese, Trockenbeerenauslese, late harvest, Recioto |
| `natural` | natural wine, vin nature, sans soufre |

For a concept token like `"sparkling"`, the ILIKE engine generates: `(category ILIKE '%sparkling%' OR … ) OR (wine_name ILIKE '%Prosecco%' OR …) OR (wine_name ILIKE '%Champagne%' OR …)` etc. This catches wines by category AND by name.

German wine-style terms are handled via the synonym layer first: `schaumwein` → `"sparkling"`, `süsswein`/`dessertwein` → `"dessert"`, `likörwein` → `"fortified"`, `sekt`/`perlwein` → `"sparkling"`, `süss`/`süß` → `"sweet"`.

### Soft AND Fallback

When strict AND across all ILIKE conditions returns zero results and there are at least two ILIKE text conditions, a scored fallback query fires before the fuzzy layer. The fallback relaxes the ILIKE conditions from AND to OR (at least one must match) while keeping intent and system-concept filters mandatory.

Results are ranked by number of matching ILIKE conditions (descending), then the original sort order. This surfaces wines that match the most query terms first. A `"Partial match"` header signals to the agent that not all terms matched.

The fallback adds zero overhead on the happy path — it only fires when the strict query returns empty. It uses a single additional SQL query (scored by `CASE WHEN … THEN 1 ELSE 0 END` per condition) rather than combinatorial N-1 token subsets.

Constraints:
- Minimum 2 ILIKE tokens required (single-token queries have nothing to relax)
- Intent conditions (drinking status, price, ratings, stock) always enforced
- System concept conditions (tracked, favorite, wishlist) always enforced
- Fires **before** the Jaro-Winkler fuzzy fallback (exact ILIKE partial matches are more precise)

Processing pipeline: `query → synonyms → intent extraction → concept expansion → ILIKE engine → soft AND fallback → fuzzy fallback`

## Statistics Engine

`cellar_stats(con, group_by, limit=20, sort_by=None)` returns formatted Markdown:

**Overall** (no group_by): total wines, stored bottles, consumed, volume, cellar value, category breakdown, drinking window status, data freshness, tracked wines.

**Grouped** (one of 10 dimensions):

| Dimension | Grouping Column |
|-----------|-----------------|
| `country` | `country` |
| `region` | `region` |
| `category` | `category` |
| `vintage` | `vintage` |
| `winery` | `winery_name` |
| `grape` | `primary_grape` |
| `cellar` | `cellar_name` (from bottles_stored) |
| `provider` | `provider_name` (from bottles_stored) |
| `status` | `status` (from bottles) |
| `on_order` | `cellar_name` (from bottles_on_order) |

Grouped output columns: `<dimension> | wines | bottles | bottles_% | value (CHF) | volume (L)`. NULL group values render as `(not set)`. When results exceed `limit` (default 20, 0=unlimited), excess groups are rolled into an `(other)` summary row with a footnote.

## Price Tracking

### `log_price()`

Records a price observation for a tracked wine:
1. Validates required fields and tracked_wine existence
2. Auto-converts price to CHF using `convert_to_default_currency()`
3. Deduplicates by `(tracked_wine_id, vintage, bottle_size_ml, retailer_name, date)`
4. Writes to year-partitioned Parquet (`price_observation_YYYY.parquet`)

### `get_tracked_wine_prices()`

Returns latest in-stock prices from the `latest_prices` view, sorted by `price_chf ASC`.

### `get_price_history()`

Returns monthly price statistics from the `price_history` view, filtered to the last N months.

### `wishlist_alerts()`

Scans recent price observations and returns prioritised alerts:

| Priority | Alert Type | Trigger |
|----------|-----------|---------|
| **High** | New Listing | First observation within alert window |
| **High** | Price Drop | Latest vs previous price ≥ `price_drop_alert_pct` (default 10%) |
| **High** | Back in Stock | Was out of stock, now in stock |
| **Medium** | Best Price | Cheapest across ≥2 retailers |
| **Medium** | En Primeur | Specific vintage available |
| **Medium** | Last Bottles | Low stock indicator |

Alert window: `wishlist.alert_window_days` (default 30 days).

## Year-Partitioned Parquet

Price observations use year-partitioned storage: `price_observation_2025.parquet`, `price_observation_2026.parquet`, etc.

- **Write**: `writer.write_partitioned_parquet()` groups rows by year from `observed_at`, writes one file per year.
- **Append**: `writer.append_partitioned_parquet()` reads existing year file, merges, rewrites.
- **Read**: DuckDB uses glob pattern `price_observation_*.parquet` to read all partitions.
- **Rationale**: append-friendly for continuous price logging; avoids rewriting entire history on each observation.

## Output Formatting

`execute_query()` returns results as Markdown tables (via pandas `.to_markdown()`). Results are capped at `query.row_limit` (default 200); a count note is appended if truncated. Empty results return `"*No results.*"`.
