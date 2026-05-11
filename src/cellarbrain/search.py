"""Text search engine for the wine cellar.

Provides intent-aware, concept-expanding text search across wine entities
via DuckDB ILIKE matching. Extracted from ``query.py`` for cohesion.

Public API:
- ``find_wine`` — tokenised multi-column ILIKE search with intent detection
- ``format_siblings`` — Markdown table of format-group siblings
- ``IntentResult`` — dataclass returned by intent extraction

Internal helpers re-exported for test access:
- ``_extract_intents``, ``_normalise_query_tokens``
- ``_CONCEPT_EXPANSIONS``, ``_SEARCH_COLS``, ``_SYSTEM_CONCEPTS``
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import duckdb
import pandas as pd

from ._query_base import QueryError, _format_df, _to_md

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Text search — intent detection
# ---------------------------------------------------------------------------


@dataclass
class IntentResult:
    """Accumulated WHERE clauses and ORDER BY from intent detection."""

    where_clauses: list[str] = field(default_factory=list)
    where_params: list = field(default_factory=list)
    order_by: str | None = None
    consumed_indices: set[int] = field(default_factory=set)


# Each pattern is (token_tuple, handler).  Handlers receive the matched
# tokens and current param_idx, and return (where_sql, params, order_by).
# Patterns are checked longest-first so multi-word patterns take priority.


def _intent_drinking_ready(
    matched: list[str],
    idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"drinking_status IN (${idx}, ${idx + 1})",
        ["optimal", "drinkable"],
        None,
    )


def _intent_drinking_exact(
    status: str,
) -> callable:
    def _handler(
        matched: list[str],
        idx: int,
    ) -> tuple[str, list, str | None]:
        return f"drinking_status = ${idx}", [status], None

    return _handler


def _intent_drinking_drinkable(
    matched: list[str],
    idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"drinking_status IN (${idx}, ${idx + 1})",
        ["optimal", "drinkable"],
        None,
    )


def _intent_price_under(
    matched: list[str],
    idx: int,
) -> tuple[str, list, str | None]:
    # Last matched token is the numeric value.
    price = float(matched[-1])
    return f"price <= ${idx} AND price IS NOT NULL", [price], None


def _intent_price_budget(
    matched: list[str],
    idx: int,
) -> tuple[str, list, str | None]:
    return f"price_tier = ${idx}", ["budget"], None


def _intent_price_premium(
    matched: list[str],
    idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"price_tier IN (${idx}, ${idx + 1})",
        ["premium", "fine"],
        "price DESC",
    )


def _intent_top_rated(
    matched: list[str],
    idx: int,
) -> tuple[str, list, str | None]:
    return (
        "best_pro_score IS NOT NULL",
        [],
        "best_pro_score DESC, bottles_stored DESC, vintage DESC",
    )


def _intent_low_stock(
    matched: list[str],
    idx: int,
) -> tuple[str, list, str | None]:
    return (
        f"bottles_stored BETWEEN ${idx} AND ${idx + 1}",
        [1, 2],
        "bottles_stored ASC, vintage DESC",
    )


def _matches_numeric(token: str) -> bool:
    """Return True if token looks like a positive number (not a vintage)."""
    try:
        val = float(token)
        return val > 0 and val < 1000  # exclude vintage-like numbers
    except ValueError:
        return False


# Patterns ordered longest-first for greedy matching.  Each entry:
#   (tuple_of_lowercase_tokens, handler, numeric_tail)
# numeric_tail=True means the last token slot matches any number.
_INTENT_PATTERNS: list[tuple[tuple[str, ...], callable, bool]] = [
    # --- drinking status (multi-word first) ---
    (("ready", "to", "drink"), _intent_drinking_ready, False),
    (("ready", "drink"), _intent_drinking_ready, False),
    (("drink", "soon"), _intent_drinking_exact("past_optimal"), False),
    (("too", "young"), _intent_drinking_exact("too_young"), False),
    (("past", "optimal"), _intent_drinking_exact("past_optimal"), False),
    # --- price (multi-word first) ---
    (("cheaper", "than"), _intent_price_under, True),
    (("under",), _intent_price_under, True),
    (("below",), _intent_price_under, True),
    # --- single-word intents ---
    (("optimal",), _intent_drinking_exact("optimal"), False),
    (("drinkable",), _intent_drinking_drinkable, False),
    (("budget",), _intent_price_budget, False),
    (("premium",), _intent_price_premium, False),
    (("highest", "rated"), _intent_top_rated, False),
    (("top", "rated"), _intent_top_rated, False),
    (("best", "rated"), _intent_top_rated, False),
    (("running", "low"), _intent_low_stock, False),
    (("last", "bottle"), _intent_low_stock, False),
    (("low", "stock"), _intent_low_stock, False),
]


def _extract_intents(tokens: list[str], param_idx: int) -> IntentResult:
    """Scan tokens for intent patterns and return accumulated SQL fragments.

    Modifies nothing; returns an ``IntentResult`` with WHERE clauses,
    parameters, optional ORDER BY override, and indices of consumed tokens.
    ``param_idx`` is the next free DuckDB parameter index ($N).
    """
    result = IntentResult()
    lower_tokens = [t.lower() for t in tokens]
    n = len(lower_tokens)
    consumed: set[int] = set()

    for pattern, handler, numeric_tail in _INTENT_PATTERNS:
        plen = len(pattern) + (1 if numeric_tail else 0)
        for i in range(n - plen + 1):
            # Skip if any position already consumed.
            if any(j in consumed for j in range(i, i + plen)):
                continue
            # Check fixed tokens.
            fixed_match = all(lower_tokens[i + k] == pattern[k] for k in range(len(pattern)))
            if not fixed_match:
                continue
            # Check numeric tail if needed.
            if numeric_tail:
                tail_idx = i + len(pattern)
                if not _matches_numeric(lower_tokens[tail_idx]):
                    continue
                matched = [tokens[j] for j in range(i, i + plen)]
            else:
                matched = [tokens[j] for j in range(i, i + plen)]

            where_sql, params, order_by = handler(matched, param_idx)
            if where_sql:
                result.where_clauses.append(where_sql)
                result.where_params.extend(params)
                param_idx += len(params)
            if order_by and result.order_by is None:
                result.order_by = order_by
            for j in range(i, i + plen):
                consumed.add(j)
            break  # Re-scan from the start for the next pattern.

    result.consumed_indices = consumed
    return result


# ---------------------------------------------------------------------------
# Text search — concept expansion, token normalisation, and ILIKE engine
# ---------------------------------------------------------------------------

# Concept expansions: abstract wine-style keywords → concrete search terms.
# When a token matches a key here, the ILIKE engine generates an OR across
# the original token AND all expansion terms (so the category column still
# matches "sparkling" while wine_name might match "Prosecco").
_CONCEPT_EXPANSIONS: dict[str, list[str]] = {
    # -- Wine style concepts -----------------------------------------------
    "sparkling": [
        "Prosecco",
        "Champagne",
        "Crémant",
        "Cava",
        "Spumante",
        "Sekt",
        "Franciacorta",
    ],
    "dessert": [
        "Sauternes",
        "Tokaji",
        "Moscato",
        "Eiswein",
        "Passito",
        "Vin Santo",
        "Recioto",
        "Beerenauslese",
        "Trockenbeerenauslese",
        "late harvest",
    ],
    "fortified": ["Port", "Sherry", "Madeira", "Marsala", "Vermouth"],
    "sweet": [
        "Sauternes",
        "Tokaji",
        "Moscato",
        "Eiswein",
        "Passito",
        "Vin Santo",
        "Beerenauslese",
        "Trockenbeerenauslese",
        "late harvest",
        "Recioto",
    ],
    "natural": ["natural wine", "vin nature", "sans soufre"],
    # -- Grape synonym clusters (bidirectional cross-references) -----------
    "shiraz": ["Syrah"],
    "syrah": ["Shiraz"],
    "garnacha": ["Grenache"],
    "grenache": ["Garnacha"],
    "monastrell": ["Mourvèdre"],
    "mourvèdre": ["Monastrell"],
    "primitivo": ["Zinfandel"],
    "zinfandel": ["Primitivo"],
    "tempranillo": ["Tinta del Pais"],
    "carignan": ["Cariñena"],
    "cariñena": ["Carignan"],
    # -- Sub-variety name cross-references ---------------------------------
    "grigio": ["Gris"],
    "gris": ["Grigio"],
}

# System concepts: keywords that translate into WHERE clauses rather than
# ILIKE text search.  Values are ``(sql_fragment, params_list)``.
_SYSTEM_CONCEPTS: dict[str, tuple[str, list]] = {
    "tracked": ("tracked_wine_id IS NOT NULL", []),
    "favorite": ("is_favorite = true", []),
    "favourite": ("is_favorite = true", []),
    "favourites": ("is_favorite = true", []),
    "favorites": ("is_favorite = true", []),
    "wishlist": ("is_wishlist = true", []),
}

_SEARCH_COLS = [
    "wine_name",
    "winery_name",
    "country",
    "region",
    "subregion",
    "classification",
    "category",
    "primary_grape",
    "subcategory",
    "sweetness",
    "effervescence",
    "specialty",
]


def _normalise_query_tokens(
    tokens: list[str],
    synonyms: dict[str, str],
) -> list[str]:
    """Expand/drop query tokens using the synonym dict.

    For each token the lowercase form is looked up in *synonyms*:
    - Found with non-empty value → replace (multi-word values are split).
    - Found with empty value → drop (stopword).
    - Not found → keep the original token.

    If all tokens are stopwords the originals are returned unchanged so
    that the caller never receives an empty list.
    """
    result: list[str] = []
    for token in tokens:
        replacement = synonyms.get(token.lower())
        if replacement is None:
            result.append(token)
        elif replacement:
            result.extend(replacement.split())
        # else: empty string → stopword, drop token
    return result if result else tokens


def format_siblings(
    con: duckdb.DuckDBPyConnection,
    wine_id: int,
) -> str:
    """Return a Markdown table of format siblings for *wine_id*.

    Returns an empty string if the wine has no format_group_id or no siblings.
    """
    row = con.execute(
        "SELECT format_group_id FROM wines_full WHERE wine_id = ?",
        [wine_id],
    ).fetchone()
    if row is None or row[0] is None:
        return ""
    fgid = row[0]
    rows = con.execute(
        "SELECT wine_id, bottle_format, volume_ml, "
        "drinking_status, bottles_stored "
        "FROM wines_full WHERE format_group_id = ? "
        "ORDER BY volume_ml",
        [fgid],
    ).fetchall()
    if len(rows) < 2:
        return ""
    lines = [
        "| Wine ID | Format | Volume | Status | Stored |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        marker = " ★" if r[0] == wine_id else ""
        lines.append(f"| {r[0]}{marker} | {r[1]} | {r[2]} mL | {r[3]} | {r[4]} |")
    return "\n".join(lines)


def find_wine(
    con: duckdb.DuckDBPyConnection,
    query: str,
    limit: int = 10,
    fuzzy: bool = False,
    synonyms: dict[str, str] | None = None,
    fmt: str = "markdown",
) -> str:
    """Search wines by ILIKE matching across multiple columns.

    Tokenises multi-word queries: each token must match at least one
    searchable column (OR across columns), all tokens must match (AND
    across tokens).  Uses ``strip_accents()`` for accent-insensitive
    matching, ``normalize_quotes()`` for typographic-quote-insensitive
    matching, and DuckDB parameter binding to prevent SQL injection.

    After synonym normalisation, an intent detection layer recognises
    attribute-based patterns (drinking status, price, rating, stock)
    and injects WHERE / ORDER BY clauses.  Consumed tokens are removed
    from the ILIKE engine so they don't cause false positives.

    When strict AND returns zero results and there are two or more
    ILIKE text conditions, a soft-AND fallback query fires: it requires
    at least one ILIKE condition to match and ranks results by the
    number of matching conditions.  Intent and system-concept filters
    remain mandatory.  This recovers results for near-miss queries
    where one token matches nothing.
    """
    tokens = query.split()
    if not tokens:
        return "*Empty search query.*"
    if synonyms:
        tokens = _normalise_query_tokens(tokens, synonyms)

    intent_conds: list[str] = []
    ilike_conds: list[str] = []
    params: list = []
    param_idx = 1

    # --- Intent detection (attribute-based queries) ---
    intent = _extract_intents(tokens, param_idx)
    if intent.where_clauses:
        intent_conds.extend(intent.where_clauses)
        params.extend(intent.where_params)
        param_idx += len(intent.where_params)

    # Filter out tokens consumed by intent patterns.
    remaining_tokens = [t for i, t in enumerate(tokens) if i not in intent.consumed_indices]

    # --- Concept expansion + ILIKE engine for remaining text tokens ---
    for token in remaining_tokens:
        lower = token.lower()

        # System concepts → WHERE clause (like mini-intents).
        sys = _SYSTEM_CONCEPTS.get(lower)
        if sys is not None:
            sql_frag, sys_params = sys
            intent_conds.append(sql_frag)
            params.extend(sys_params)
            param_idx += len(sys_params)
            continue

        # Concept expansion → OR across the original token + all expansions.
        expansions = _CONCEPT_EXPANSIONS.get(lower)
        search_terms = [token] if expansions is None else [token, *expansions]

        term_groups: list[str] = []
        for term in search_terms:
            col_checks = [
                f"normalize_quotes(strip_accents({col})) ILIKE normalize_quotes(strip_accents(${param_idx}))"
                for col in _SEARCH_COLS
            ]
            col_checks.append(f"CAST(vintage AS VARCHAR) = ${param_idx + 1}")
            term_groups.append(f"({' OR '.join(col_checks)})")
            params.append(f"%{term}%")
            params.append(term)
            param_idx += 2

        ilike_conds.append(f"({' OR '.join(term_groups)})")

    # Build WHERE — intent-only queries (all tokens consumed) still work.
    conditions = intent_conds + ilike_conds
    if not conditions:
        return "*Empty search query.*"
    where_clause = " AND ".join(conditions)
    limit_param = f"${param_idx}"
    params.append(limit)

    order_by = intent.order_by or "bottles_stored DESC, vintage DESC"

    sql = f"""
        SELECT wine_id, winery_name, wine_name, vintage, category,
               country, region, primary_grape, bottles_stored, drinking_status,
               price,
               CASE WHEN format_group_id IS NOT NULL
                    THEN bottle_format || ' ★'
                    ELSE bottle_format END AS size,
               price_per_750ml,
               tracked_wine_id
        FROM wines_full
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT {limit_param}
    """
    try:
        df = con.execute(sql, params).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc

    if df.empty:
        # --- Soft AND fallback: relax ILIKE conditions when ≥2 exist ---
        if len(ilike_conds) >= 2:
            df = _find_wine_soft_and(
                con,
                intent_conds,
                ilike_conds,
                params,
                param_idx,
                order_by,
            )
            if not df.empty:
                header = f"*Partial match for '{query}' (not all terms matched):*\n\n"
                return header + _format_df(df, fmt, style="list")
        if fuzzy and remaining_tokens:
            expanded_query = " ".join(remaining_tokens)
            return _find_wine_fuzzy(con, expanded_query, limit)
        return f"*No wines found matching '{query}'.*"
    return _format_df(df, fmt, style="list")


def _find_wine_soft_and(
    con: duckdb.DuckDBPyConnection,
    intent_conds: list[str],
    ilike_conds: list[str],
    params: list,
    limit_param_idx: int,
    order_by: str,
) -> pd.DataFrame:
    """Scored fallback: require at least one ILIKE match, rank by count.

    Intent/system-concept conditions remain mandatory. Only ILIKE text
    conditions are relaxed (OR instead of AND). Results are ranked by
    how many ILIKE conditions matched (descending), then the original
    sort order.
    """
    any_match = f"({' OR '.join(ilike_conds)})"
    score_expr = " + ".join(f"CASE WHEN {cond} THEN 1 ELSE 0 END" for cond in ilike_conds)

    where_parts = [any_match, *intent_conds]
    where_clause = " AND ".join(where_parts)
    limit_param = f"${limit_param_idx}"

    sql = f"""
        SELECT wine_id, winery_name, wine_name, vintage, category,
               country, region, primary_grape, bottles_stored, drinking_status,
               price,
               CASE WHEN format_group_id IS NOT NULL
                    THEN bottle_format || ' ★'
                    ELSE bottle_format END AS size,
               price_per_750ml,
               tracked_wine_id
        FROM wines_full
        WHERE {where_clause}
        ORDER BY ({score_expr}) DESC, {order_by}
        LIMIT {limit_param}
    """
    try:
        return con.execute(sql, params).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc


def _find_wine_fuzzy(
    con: duckdb.DuckDBPyConnection,
    query: str,
    limit: int,
    threshold: float = 0.85,
) -> str:
    """Fuzzy fallback — find close matches using Jaro-Winkler similarity."""
    sql = """
        WITH scored AS (
            SELECT wine_id, winery_name, wine_name, vintage, category,
                   country, region, primary_grape, bottles_stored, drinking_status,
                   price,
                   CASE WHEN format_group_id IS NOT NULL
                        THEN bottle_format || ' ★'
                        ELSE bottle_format END AS size,
                   price_per_750ml,
                   tracked_wine_id,
                   GREATEST(
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(wine_name))), normalize_quotes(strip_accents(LOWER($1)))),
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(winery_name))), normalize_quotes(strip_accents(LOWER($1)))),
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(COALESCE(region, '')))), normalize_quotes(strip_accents(LOWER($1)))),
                       jaro_winkler_similarity(normalize_quotes(strip_accents(LOWER(COALESCE(primary_grape, '')))), normalize_quotes(strip_accents(LOWER($1))))
                   ) AS similarity
            FROM wines_full
        )
        SELECT wine_id, winery_name, wine_name, vintage, category,
               country, region, primary_grape, bottles_stored, drinking_status,
               price, size, price_per_750ml,
               tracked_wine_id,
               ROUND(similarity, 2) AS match_score
        FROM scored
        WHERE similarity >= $2
        ORDER BY similarity DESC, bottles_stored DESC
        LIMIT $3
    """
    try:
        df = con.execute(sql, [query, threshold, limit]).fetchdf()
    except duckdb.Error as exc:
        raise QueryError(str(exc)) from exc

    if df.empty:
        return f"*No wines found matching '{query}' (including fuzzy).*"
    header = f"*Fuzzy matches for '{query}':*\n\n"
    return header + _to_md(df)
