# MCP Tools Reference

19 tools, 10 resources, and prompts exposed via FastMCP with stdio transport.

## Configuration

- Settings loaded at startup via `CELLARBRAIN_CONFIG` env var or built-in defaults
- Data directory from `CELLARBRAIN_DATA_DIR` env var (default: `output`)
- Tool defaults (row_limit, search_limit, pending_limit) configurable in `cellarbrain.toml` under `[query]`

## Tools

### Query & Search

| Tool | Args | Returns |
|------|------|---------|
| `query_cellar` | `sql: str` | Markdown table from read-only SQL. Agent connection (views only). |
| `find_wine` | `query: str`, `limit?: int`, `fuzzy?: bool` | Markdown table of matches across name, winery, region, grape, category, vintage, sweetness, effervescence, specialty, subcategory. Applies synonym expansion (DE→EN), intent detection (drinking status, price, ratings, stock), and concept expansion (sparkling, dessert, fortified, sweet, tracked, favorite, wishlist) before search. Falls back to soft-AND (partial match) when strict AND returns 0 results and ≥2 text tokens exist. |
| `cellar_info` | `verbose?: bool` | Version, currency, data directory, ETL freshness, inventory counts, and config metadata. Set `verbose=True` for extended diagnostics. |
| `cellar_stats` | `group_by?: str`, `limit?: int`, `sort_by?: str` | Overall summary or grouped breakdown by one of 10 dimensions. `limit`: max groups (default 20, 0=unlimited); excess rolled into `(other)`. `sort_by`: "bottles" (default), "value", "wines", "volume". |
| `cellar_churn` | `period?: str`, `year?: int`, `month?: int` | Roll-forward churn analysis (beginning/purchased/consumed/ending). |
| `search_synonyms` | `action: str`, `key?: str`, `value?: str` | Manage custom search synonyms. Actions: `list`, `add`, `remove`. |

`query_cellar` validates SQL is read-only (SELECT/WITH only, no DDL/DML) and uses the agent connection which exposes only views, not raw entity tables.

**View selection guide:** Default to `wines` / `bottles` (slim) for most queries. Use `wines_full` / `bottles_full` only when you need wine details (`alcohol_pct`, `grapes`, `volume_ml`, `classification`), bottle details (`provider_name`, `purchase_date`, `volume_ml`), or aggregates (`cellar_value`, `on_order_value`, tasting/rating scores). Convenience views (`wines_stored`, `bottles_stored`, etc.) also return slim columns.

`find_wine` tokenises multi-word queries (AND across words, OR across columns) and uses `strip_accents()` for accent-insensitive matching. Searches 12 text columns: wine name, winery, country, region, subregion, classification, category, primary grape, subcategory, sweetness, effervescence, and specialty. Before searching, tokens are normalised via a synonym dictionary (~155 built-in entries) that maps German terms to stored values (e.g. `rotwein` → `red`, `schweiz` → `Switzerland`, `trocken` → `dry`) and drops stopwords (e.g. `weingut`, `wein`). After synonym normalisation, an intent detection layer recognises attribute-based patterns — drinking status (`ready to drink`, `too young`, `past optimal`), price (`under 30`, `budget`), ratings (`top rated`), and stock levels (`low stock`, `last bottle`) — and injects WHERE/ORDER BY clauses. Consumed intent tokens are excluded from text search. German intent triggers (e.g. `trinkreif` → `"ready to drink"`) are handled via the synonym layer. A concept expansion layer then handles wine-style keywords (`sparkling`, `dessert`, `fortified`, `sweet`, `natural`) by OR-expanding them to concrete wine names (e.g. `sparkling` also matches Prosecco, Champagne, Crémant, etc.) and system concepts (`tracked`, `favorite`/`favourite`, `wishlist`) which inject WHERE filters. German wine-style terms (`Schaumwein`, `Süsswein`, `Dessertwein`, `Likörwein`, `Sekt`) are mapped to concept keywords via the synonym layer. Custom synonyms can be added via `search_synonyms` or TOML `[search.synonyms]`. When strict AND returns zero results and at least two ILIKE text conditions exist, a soft-AND fallback fires: it requires at least one ILIKE condition to match and ranks results by match count (descending). Intent and system-concept filters remain mandatory. Results are prefixed with a "Partial match" header. If soft AND also returns nothing, the fuzzy fallback fires (when `fuzzy=True`). Uses parameterised queries to prevent SQL injection. The `limit` parameter must be ≥ 1; invalid values return an error message.

`search_synonyms` manages the custom synonym layer (stored in `data_dir/search-synonyms.json`). Actions: `list` shows all synonyms (built-in + custom) with source; `add` saves a key→value mapping (empty value = stopword); `remove` deletes a custom synonym (built-in entries cannot be removed). Changes take effect on the next `find_wine` call.

`cellar_info` returns three sections: **Cellar Info** (version, currency, data directory, config file, query limits), **Data Freshness** (last ETL run timestamp/type, changeset summary), and **Inventory** (wines, bottles, tracked wines, dossiers, price observations). With `verbose=True`, adds Python/MCP SDK versions, total ETL runs, companion dossier count, currency conversion rates, and cellar location names. Degrades gracefully when no ETL data exists.

`cellar_stats` group_by options: `country`, `region`, `category`, `vintage`, `winery`, `grape`, `cellar`, `provider`, `status`, `on_order`.

`cellar_churn` period options: `"month"` (month-by-month), `"year"` (year-by-year). If omitted, returns a single-period summary. Defaults to current month when no args given. Examples: `cellar_churn()`, `cellar_churn(year=2025, month=3)`, `cellar_churn(period="month", year=2025)`, `cellar_churn(period="year")`.

#### Example SQL Queries

```sql
-- Total bottles in cellar
SELECT count(*) AS total_bottles FROM bottles_stored

-- Total value
SELECT count(*) AS bottles,
       sum(price) AS total_value
FROM bottles_stored

-- Wines ready to drink now
SELECT wine_id, winery_name, wine_name, vintage,
       drink_from, drink_until, optimal_from, optimal_until
FROM wines_drinking_now

-- Wines by grape variety
SELECT primary_grape, count(*) AS wines, sum(bottles_stored) AS bottles
FROM wines_stored
WHERE primary_grape IS NOT NULL
GROUP BY primary_grape ORDER BY wines DESC

-- Best food pairing candidates (red, in optimal window, high rated)
SELECT wine_id, winery_name, vintage, category,
       region, alcohol_pct, best_pro_score
FROM wines_full
WHERE drinking_status IN ('optimal', 'drinkable')
  AND bottles_stored > 0 AND category = 'red'
ORDER BY best_pro_score DESC NULLS LAST

-- Purchase history by provider
SELECT provider_name, count(*) AS bottles,
       sum(price) AS total_spent
FROM bottles_full
WHERE provider_name IS NOT NULL
GROUP BY provider_name ORDER BY total_spent DESC

-- Wines past their optimal window (drink soon!)
SELECT wine_id, wine_name, vintage, optimal_until, bottles_stored
FROM wines_stored
WHERE drinking_status = 'past_optimal'
ORDER BY optimal_until ASC

-- Recent consumption (last 12 months)
SELECT wine_name, vintage, output_date, output_type
FROM bottles_consumed
WHERE output_date >= CURRENT_DATE - INTERVAL '12 months'
ORDER BY output_date DESC

-- Wines on order (not yet in cellar)
SELECT wine_id, winery_name, wine_name, bottles_on_order, on_order_value
FROM wines
WHERE bottles_on_order > 0
ORDER BY on_order_value DESC

-- Combined inventory: stored + on order
SELECT wine_id, wine_name, bottles_stored, bottles_on_order,
       cellar_value, on_order_value
FROM wines
WHERE bottles_stored > 0 OR bottles_on_order > 0
ORDER BY wine_id
```

### Dossier Management

| Tool | Args | Returns |
|------|------|---------|
| `read_dossier` | `wine_id: int`, `sections?: list[str]` | Full dossier or filtered sections. |
| `update_dossier` | `wine_id: int`, `section: str`, `content: str`, `agent_name?: str` | Confirmation. Agent sections only. |
| `pending_research` | `limit?: int`, `section?: str` | Per-vintage wines with unfilled agent sections. |

`read_dossier` section keys: ETL (`identity`, `origin`, `grapes`, `characteristics`, `drinking_window`, `cellar_inventory`, `purchase_history`, `consumption_history`, `owner_notes`), Mixed (`ratings_reviews`, `tasting_notes`, `food_pairings`), Agent (`producer_profile`, `vintage_report`, `wine_description`, `market_availability`, `similar_wines`, `agent_log`).

`update_dossier` allowed sections: `producer_profile`, `vintage_report`, `wine_description`, `market_availability`, `similar_wines`, `ratings_reviews`, `tasting_notes`, `food_pairings`. ETL sections are protected.

### Companion Dossier Management

| Tool | Args | Returns |
|------|------|---------|
| `read_companion_dossier` | `tracked_wine_id: int`, `sections?: list[str]` | Full companion dossier or filtered sections. |
| `update_companion_dossier` | `tracked_wine_id: int`, `section: str`, `content: str` | Confirmation. Agent sections only. |
| `list_companion_dossiers` | `pending_only?: bool` | All tracked wines, or only those with pending sections. |
| `pending_companion_research` | `limit?: int` | Tracked wines with unfilled companion sections. |

Companion section keys: `producer_deep_dive`, `vintage_tracker`, `buying_guide`, `price_tracker`.

### Data Refresh

| Tool | Args | Returns |
|------|------|---------|
| `reload_data` | `mode?: str` | Re-runs ETL pipeline. `"sync"` (default, incremental) or `"full"`. |

Expects CSV exports in `raw/` directory alongside the data directory. Agent-owned dossier sections are preserved across reloads.

### Sommelier (Food-Wine Pairing)

| Tool | Args | Returns |
|------|------|--------|
| `suggest_wines` | `food_query: str`, `limit?: int` | Markdown table of wines ranked by embedding similarity to the food query. Includes vintage, category, region, grape, bottles, and price. |
| `suggest_foods` | `wine_id: int`, `limit?: int` | Markdown table of dishes ranked by embedding similarity to the wine. Includes cuisine, weight class, protein, and flavour profile from the food catalogue. |

Both tools use a fine-tuned `all-MiniLM-L6-v2` sentence-transformer model with FAISS indexes. They require:
1. `cellarbrain train-model` — fine-tune the pairing model (~3-5 min CPU).
2. `cellarbrain rebuild-indexes` — build the food and wine FAISS indexes.

The wine index is also auto-rebuilt after each ETL run when `sommelier.enabled = true` and the model exists. The `limit` parameter defaults to 10.

**Agent workflow:** For food → wine pairing, the agent chains `suggest_wines` → `read_dossier` (top 3–5) → food-pairing skill (rerank) → LLM explanation. For wine → food, the agent calls `suggest_foods` → reads the wine dossier → presents dishes with context. When the model is unavailable, the agent falls back to SQL-based queries using the food-pairing skill's query strategies.

### Price Tracking

| Tool | Args | Returns |
|------|------|---------|
| `log_price` | `tracked_wine_id: int`, `bottle_size_ml: int`, `retailer_name: str`, `price: float`, `currency: str`, `in_stock: bool`, `vintage?: int`, `retailer_url?: str`, `notes?: str`, `observation_source?: str` | Confirmation. Auto-converts to CHF. Deduplicates by (tracked_wine_id, vintage, size, retailer, date). |
| `tracked_wine_prices` | `tracked_wine_id: int`, `vintage?: int` | Latest in-stock prices per retailer, sorted by price ascending. |
| `price_history` | `tracked_wine_id: int`, `vintage?: int`, `months?: int` | Monthly price statistics (min/max/avg) over N months. |
| `wishlist_alerts` | `days?: int` | Prioritised alerts: price drops, new listings, back in stock, best prices. |

## Resources

| URI | Description |
|-----|-------------|
| `wine://list` | All wines with basic metadata |
| `wine://cellar` | Wines with stored bottles |
| `wine://on-order` | Wines with on-order / in-transit bottles |
| `wine://favorites` | Favorite wines |
| `wine://{wine_id}` | Full dossier for a specific wine |
| `cellar://stats` | Cellar statistics snapshot |
| `cellar://drinking-now` | Wines in optimal drinking window |
| `etl://last-run` | Most recent ETL run metadata |
| `etl://changes` | Change log from last ETL run |
| `schema://views` | Column reference for all queryable views — read before writing SQL |

## Prompts

| Prompt | Purpose |
|--------|---------|
| `cellar_qa` | System prompt for cellar Q&A — includes live stats snapshot |

## Error Handling

All tools catch domain exceptions and return `f"Error: {exc}"` strings:

| Exception | Raised When |
|-----------|-------------|
| `QueryError` | SQL validation failure or execution error |
| `DataStaleError` | Parquet files not found (ETL not run) |
| `WineNotFoundError` | wine_id not in wine.parquet |
| `TrackedWineNotFoundError` | tracked_wine_id not found |
| `ProtectedSectionError` | Attempt to update an ETL-owned section |
| `ValueError` | Invalid group_by, missing CSV files, etc. |

See [error-reference.md](error-reference.md) for the full error catalogue with causes and fixes. For the agent integration architecture, see [agent-architecture.md](agent-architecture.md).
