"""Smart drinking recommendation engine — scores wines for tonight's occasion.

Combines urgency (drinking window), occasion fit (tier/format/category),
pairing (RAG integration), freshness (avoid recently consumed), and
diversity (variety within batch) into a single recommendation score.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OCCASIONS: dict[str, dict] = {
    "casual": {"tiers": frozenset({"budget", "everyday"}), "category": None, "format": "standard"},
    "weeknight": {"tiers": frozenset({"budget", "everyday"}), "category": None, "format": "standard"},
    "dinner_party": {"tiers": frozenset({"everyday", "premium"}), "category": None, "format": "standard"},
    "celebration": {"tiers": frozenset({"premium", "fine"}), "category": frozenset({"Sparkling wine"}), "format": None},
    "romantic": {
        "tiers": frozenset({"premium", "fine"}),
        "category": frozenset({"Red wine", "Sparkling wine"}),
        "format": "standard",
    },
    "solo": {"tiers": frozenset({"budget", "everyday"}), "category": None, "format": "standard"},
    "tasting": {"tiers": None, "category": None, "format": "standard"},
}

BUDGETS: dict[str, frozenset[str] | None] = {
    "any": None,
    "under_15": frozenset({"budget"}),
    "under_30": frozenset({"budget", "everyday"}),
    "under_50": frozenset({"budget", "everyday", "premium"}),
    "special": frozenset({"premium", "fine"}),
}

# Adjacent tiers for partial scoring (not direct match but close)
_ADJACENT_TIERS: dict[str, frozenset[str]] = {
    "budget": frozenset({"everyday"}),
    "everyday": frozenset({"budget", "premium"}),
    "premium": frozenset({"everyday", "fine"}),
    "fine": frozenset({"premium"}),
    "unknown": frozenset(),
}

# Base urgency score by drinking_status (used when no window boundaries known)
_STATUS_BASE_URGENCY: dict[str | None, float] = {
    "past_window": 10.0,
    "past_optimal": 8.0,
    "optimal": 5.0,
    "drinkable": 3.0,
    "too_young": 0.0,
    None: 2.0,
}

# Columns fetched from wines_full
_CANDIDATE_COLUMNS = (
    "wine_id",
    "wine_name",
    "vintage",
    "winery_name",
    "category",
    "country",
    "region",
    "primary_grape",
    "price",
    "price_tier",
    "drinking_status",
    "bottles_stored",
    "volume_ml",
    "bottle_format",
    "is_favorite",
    "best_pro_score",
    "drink_from",
    "drink_until",
    "optimal_from",
    "optimal_until",
    "last_tasting_date",
    "tasting_count",
)

_CANDIDATE_SQL = f"""\
SELECT {", ".join(_CANDIDATE_COLUMNS)}
FROM wines_full
WHERE bottles_stored > 0
  AND (drinking_status IS NULL OR drinking_status != 'too_young')
"""


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecommendParams:
    """Input parameters for recommendation scoring."""

    occasion: str | None = None
    cuisine: str | None = None
    guests: int | None = None
    budget: str | None = None
    limit: int = 5
    exclude_wine_ids: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class RecommendWeights:
    """Scoring weights — mirrors RecommendConfig for isolation from settings."""

    urgency: float = 3.0
    occasion: float = 2.0
    pairing: float = 2.0
    freshness: float = 1.0
    diversity: float = 1.0
    quality: float = 1.0
    freshness_days_hard: int = 7
    freshness_days_mid: int = 14
    freshness_days_soft: int = 30
    last_bottle_penalty: float = 1.0
    last_bottle_exceptions: tuple[str, ...] = ("celebration", "romantic")

    @classmethod
    def from_config(cls, cfg: object) -> RecommendWeights:
        """Build from a RecommendConfig (or any object with matching attrs)."""
        return cls(
            urgency=getattr(cfg, "urgency_weight", 3.0),
            occasion=getattr(cfg, "occasion_weight", 2.0),
            pairing=getattr(cfg, "pairing_weight", 2.0),
            freshness=getattr(cfg, "freshness_weight", 1.0),
            diversity=getattr(cfg, "diversity_weight", 1.0),
            quality=getattr(cfg, "quality_weight", 1.0),
            freshness_days_hard=getattr(cfg, "freshness_days_hard", 7),
            freshness_days_mid=getattr(cfg, "freshness_days_mid", 14),
            freshness_days_soft=getattr(cfg, "freshness_days_soft", 30),
            last_bottle_penalty=getattr(cfg, "last_bottle_penalty", 1.0),
            last_bottle_exceptions=getattr(cfg, "last_bottle_exceptions", ("celebration", "romantic")),
        )


@dataclass(frozen=True)
class Recommendation:
    """A scored wine recommendation with reasoning."""

    wine_id: int
    wine_name: str
    vintage: int | None
    winery_name: str
    category: str
    region: str | None
    primary_grape: str | None
    price: float | None
    price_tier: str
    drinking_status: str | None
    bottles_stored: int
    volume_ml: int
    is_favorite: bool
    total_score: float
    urgency_score: float
    occasion_score: float
    pairing_score: float
    freshness_penalty: float
    diversity_bonus: float
    quality_bonus: float
    reason: str


# ---------------------------------------------------------------------------
# Scoring functions (pure — no side effects)
# ---------------------------------------------------------------------------


def score_urgency(
    drinking_status: str | None,
    drink_from: int | None,
    drink_until: int | None,
    optimal_from: int | None,
    optimal_until: int | None,
    current_year: int,
) -> float:
    """Compute urgency score (0–10) based on drinking window proximity.

    Examples:
        >>> score_urgency("past_window", 2015, 2023, 2018, 2022, 2026)
        10.0
        >>> score_urgency("optimal", 2015, 2030, 2020, 2025, 2020)
        5.0
    """
    if drinking_status == "too_young":
        return 0.0
    if drinking_status == "past_window":
        return 10.0
    if drinking_status == "past_optimal":
        return 8.0

    # Within optimal window — interpolate based on proximity to end
    if drinking_status == "optimal" and optimal_from and optimal_until:
        window_length = optimal_until - optimal_from
        if window_length > 0:
            remaining = optimal_until - current_year
            remaining_pct = max(0.0, min(1.0, remaining / window_length))
            # 5 (start of optimal) → 8 (end of optimal)
            return 5.0 + (1.0 - remaining_pct) * 3.0
        return 5.0

    # Drinkable — approaching optimal
    if drinking_status == "drinkable":
        return 3.0

    # Unknown / no status — base score
    return _STATUS_BASE_URGENCY.get(drinking_status, 2.0)


def score_occasion(
    price_tier: str,
    category: str,
    volume_ml: int,
    is_favorite: bool,
    params: RecommendParams,
) -> float:
    """Compute occasion score (0–~6) based on occasion/budget/guests fit.

    Examples:
        >>> score_occasion("budget", "Red wine", 750, False, RecommendParams(occasion="casual"))
        3.0
        >>> score_occasion("fine", "Red wine", 750, False, RecommendParams(occasion="casual"))
        0.0
    """
    if not params.occasion and not params.budget:
        return 0.0

    score = 0.0
    profile = OCCASIONS.get(params.occasion or "", {})
    preferred_tiers = profile.get("tiers") if profile else None

    # Budget override: if budget specified, it takes precedence for tier matching
    budget_tiers = BUDGETS.get(params.budget or "any")
    effective_tiers = budget_tiers if budget_tiers is not None else preferred_tiers

    # Price tier match
    if effective_tiers is not None:
        if price_tier in effective_tiers:
            score += 3.0
        else:
            adjacent = _ADJACENT_TIERS.get(price_tier, frozenset())
            if adjacent & effective_tiers:
                score += 1.0

    # Category bias
    category_bias = profile.get("category") if profile else None
    if category_bias and category in category_bias:
        score += 2.0

    # Format bias (guests-driven)
    if params.guests and params.guests > 4 and volume_ml >= 1500:
        score += 2.0
    elif params.guests and params.guests <= 2 and volume_ml <= 500:
        score += 1.0

    # Favorite bonus for special occasions
    if params.occasion in ("celebration", "romantic") and is_favorite:
        score += 1.0

    return score


def score_pairing(
    wine_id: int,
    pairing_signals: dict[int, int],
    max_signals: int,
) -> float:
    """Compute pairing score (0–5) normalised against max signals.

    Examples:
        >>> score_pairing(1, {1: 4, 2: 2}, 4)
        5.0
        >>> score_pairing(3, {1: 4, 2: 2}, 4)
        0.0
    """
    if not pairing_signals or max_signals <= 0:
        return 0.0
    signals = pairing_signals.get(wine_id, 0)
    return (signals / max_signals) * 5.0


def compute_freshness_penalty(
    last_tasting_date: date | None,
    today: date,
    weights: RecommendWeights,
) -> float:
    """Compute freshness penalty (negative, 0 to −5).

    Examples:
        >>> from datetime import date
        >>> compute_freshness_penalty(date(2026, 5, 1), date(2026, 5, 6), RecommendWeights())
        -3.0
        >>> compute_freshness_penalty(None, date(2026, 5, 6), RecommendWeights())
        0.0
    """
    if last_tasting_date is None:
        return 0.0
    days = (today - last_tasting_date).days
    if days < 0:
        return 0.0
    if days < weights.freshness_days_hard:
        return -5.0
    if days < weights.freshness_days_mid:
        return -3.0
    if days < weights.freshness_days_soft:
        return -1.0
    return 0.0


def score_quality(
    best_pro_score: float | None,
    is_favorite: bool,
    bottles_stored: int,
    occasion: str | None,
    weights: RecommendWeights,
) -> float:
    """Compute quality bonus (typically −1 to +4).

    Examples:
        >>> score_quality(95.0, True, 5, "casual", RecommendWeights())
        2.75
        >>> score_quality(None, False, 1, "casual", RecommendWeights())
        -1.0
    """
    bonus = 0.0
    if best_pro_score and best_pro_score > 88:
        bonus += min((best_pro_score - 88) / 4.0, 3.0)
    if is_favorite:
        bonus += 1.0
    # Last-bottle caution
    if bottles_stored == 1 and occasion not in weights.last_bottle_exceptions:
        bonus -= weights.last_bottle_penalty
    return bonus


# ---------------------------------------------------------------------------
# Diversity re-ranking
# ---------------------------------------------------------------------------


def _apply_diversity_rerank(
    scored: list[dict],
    limit: int,
) -> list[dict]:
    """Greedy diversity re-ranking: prefer unseen wineries/grapes.

    Takes pre-sorted candidates (by raw score DESC) and returns top `limit`
    with diversity bonus applied.
    """
    if not scored:
        return []

    seen_wineries: set[str] = set()
    seen_grapes: set[str | None] = set()
    result: list[dict] = []

    # Work on a copy so we can adjust scores
    remaining = list(scored)

    while remaining and len(result) < limit:
        # Recalculate diversity bonus for all remaining candidates
        best_idx = 0
        best_total = -999.0
        for i, cand in enumerate(remaining):
            div_bonus = 0.0
            winery = cand.get("winery_name", "")
            grape = cand.get("primary_grape")
            if winery and winery not in seen_wineries:
                div_bonus += 2.0
            if grape and grape not in seen_grapes:
                div_bonus += 1.0
            total = cand["raw_score"] + div_bonus
            if total > best_total:
                best_total = total
                best_idx = i

        chosen = remaining.pop(best_idx)
        chosen["diversity_bonus"] = best_total - chosen["raw_score"]
        chosen["total_score"] = best_total
        result.append(chosen)

        # Track seen
        winery = chosen.get("winery_name", "")
        if winery:
            seen_wineries.add(winery)
        grape = chosen.get("primary_grape")
        if grape:
            seen_grapes.add(grape)

    return result


# ---------------------------------------------------------------------------
# Reason composition
# ---------------------------------------------------------------------------


def _compose_reason(
    drinking_status: str | None,
    urgency: float,
    occasion_score: float,
    pairing_score: float,
    best_pro_score: float | None,
    is_favorite: bool,
    params: RecommendParams,
) -> str:
    """Build a concise human-readable reason string."""
    parts: list[str] = []

    # Urgency reasons
    if drinking_status == "past_window":
        parts.append("Past drinking window \u2014 drink now!")
    elif drinking_status == "past_optimal":
        parts.append("Past optimal \u2014 consider opening soon.")
    elif urgency >= 7.0:
        parts.append("Approaching end of optimal window.")
    elif drinking_status == "optimal":
        parts.append("At peak.")

    # Occasion match
    if occasion_score >= 3.0 and params.occasion:
        occasion_label = params.occasion.replace("_", " ")
        parts.append(f"Great fit for {occasion_label}.")

    # Pairing
    if pairing_score >= 3.0 and params.cuisine:
        parts.append(f"Pairs well with {params.cuisine}.")

    # Quality
    if best_pro_score and best_pro_score >= 93:
        parts.append(f"High critic score ({best_pro_score:.0f}).")
    elif is_favorite:
        parts.append("A personal favorite.")

    return " ".join(parts) if parts else "Good candidate."


# ---------------------------------------------------------------------------
# Pairing integration helper
# ---------------------------------------------------------------------------


def _build_pairing_signals(
    con: duckdb.DuckDBPyConnection,
    params: RecommendParams,
) -> tuple[dict[int, int], int]:
    """Retrieve pairing signals from the RAG engine when cuisine is provided.

    Returns (wine_id → signal_count, max_signal_count). Returns ({}, 0) when
    pairing is not applicable or fails.
    """
    if not params.cuisine:
        return {}, 0

    try:
        from .pairing import retrieve_candidates

        candidates = retrieve_candidates(
            con,
            dish_description=params.cuisine,
            cuisine=params.cuisine,
            limit=50,
        )
        if not candidates:
            return {}, 0
        signals = {c.wine_id: c.signal_count for c in candidates}
        max_sig = max(signals.values(), default=1)
        return signals, max_sig
    except Exception:
        logger.debug("Pairing integration failed", exc_info=True)
        return {}, 0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def recommend(
    con: duckdb.DuckDBPyConnection,
    params: RecommendParams,
    *,
    today: date | None = None,
    weights: RecommendWeights | None = None,
    data_dir: str | None = None,
) -> list[Recommendation]:
    """Score and rank cellar wines for tonight's drinking occasion.

    Args:
        con: DuckDB connection with wines_full view available.
        params: User request parameters.
        today: Override for current date (useful in tests).
        weights: Scoring weights (defaults to RecommendWeights()).
        data_dir: Data directory for reading drink-tonight sidecar.

    Returns:
        List of Recommendation objects sorted by total_score descending.
    """
    if weights is None:
        weights = RecommendWeights()
    if today is None:
        today = date.today()
    current_year = today.year

    # Expand exclusion set with drink-tonight sidecar items
    exclude_ids = set(params.exclude_wine_ids)
    if data_dir:
        try:
            from .dashboard.sidecars import read_drink_tonight

            staged = read_drink_tonight(data_dir)
            for item in staged:
                try:
                    exclude_ids.add(int(item["wine_id"]))
                except (KeyError, ValueError, TypeError):
                    pass
        except Exception:
            pass

    # Build and execute query
    sql = _CANDIDATE_SQL
    query_params: list = []

    if exclude_ids:
        placeholders = ", ".join("?" for _ in exclude_ids)
        sql += f"  AND wine_id NOT IN ({placeholders})\n"
        query_params.extend(sorted(exclude_ids))

    # Budget tier filter (hard exclude non-matching tiers)
    budget_tiers = BUDGETS.get(params.budget or "any")
    if budget_tiers is not None:
        tier_placeholders = ", ".join("?" for _ in budget_tiers)
        sql += f"  AND (price_tier IN ({tier_placeholders}) OR price_tier = 'unknown')\n"
        query_params.extend(sorted(budget_tiers))

    try:
        rows = con.execute(sql, query_params).fetchall()
    except Exception:
        logger.warning("Recommend query failed", exc_info=True)
        return []

    if not rows:
        return []

    # Build column index
    col_idx = {name: i for i, name in enumerate(_CANDIDATE_COLUMNS)}

    # Get pairing signals (only if cuisine is provided)
    pairing_signals, max_signals = _build_pairing_signals(con, params)

    # Score each wine
    scored: list[dict] = []
    for row in rows:
        wine_id = row[col_idx["wine_id"]]
        drinking_status = row[col_idx["drinking_status"]]
        drink_from = row[col_idx["drink_from"]]
        drink_until = row[col_idx["drink_until"]]
        optimal_from = row[col_idx["optimal_from"]]
        optimal_until = row[col_idx["optimal_until"]]
        price_tier = row[col_idx["price_tier"]] or "unknown"
        category = row[col_idx["category"]] or ""
        volume_ml = row[col_idx["volume_ml"]] or 750
        is_favorite = bool(row[col_idx["is_favorite"]])
        best_pro_score = row[col_idx["best_pro_score"]]
        bottles_stored = row[col_idx["bottles_stored"]] or 0
        last_tasting_date_raw = row[col_idx["last_tasting_date"]]

        # Parse last_tasting_date
        last_tasting_date: date | None = None
        if last_tasting_date_raw:
            if isinstance(last_tasting_date_raw, date):
                last_tasting_date = last_tasting_date_raw
            elif isinstance(last_tasting_date_raw, str):
                try:
                    last_tasting_date = date.fromisoformat(last_tasting_date_raw)
                except ValueError:
                    pass

        # Compute factor scores
        urg = score_urgency(
            drinking_status,
            drink_from,
            drink_until,
            optimal_from,
            optimal_until,
            current_year,
        )
        occ = score_occasion(price_tier, category, volume_ml, is_favorite, params)
        pair = score_pairing(wine_id, pairing_signals, max_signals)
        fresh = compute_freshness_penalty(last_tasting_date, today, weights)
        qual = score_quality(best_pro_score, is_favorite, bottles_stored, params.occasion, weights)

        raw_score = (
            urg * weights.urgency
            + occ * weights.occasion
            + pair * weights.pairing
            + fresh * weights.freshness
            + qual * weights.quality
        )

        scored.append(
            {
                "wine_id": wine_id,
                "wine_name": row[col_idx["wine_name"]] or "",
                "vintage": row[col_idx["vintage"]],
                "winery_name": row[col_idx["winery_name"]] or "",
                "category": category,
                "region": row[col_idx["region"]],
                "primary_grape": row[col_idx["primary_grape"]],
                "price": row[col_idx["price"]],
                "price_tier": price_tier,
                "drinking_status": drinking_status,
                "bottles_stored": bottles_stored,
                "volume_ml": volume_ml,
                "is_favorite": is_favorite,
                "best_pro_score": best_pro_score,
                "raw_score": raw_score,
                "urgency_score": urg,
                "occasion_score": occ,
                "pairing_score": pair,
                "freshness_penalty": fresh,
                "quality_bonus": qual,
                "diversity_bonus": 0.0,
                "total_score": raw_score,
            }
        )

    # Sort by raw_score DESC before diversity pass
    scored.sort(key=lambda x: x["raw_score"], reverse=True)

    # Apply diversity re-ranking (considers top candidates for diversity)
    # Feed more candidates than limit to give diversity room to work
    pool_size = min(len(scored), params.limit * 4)
    reranked = _apply_diversity_rerank(scored[:pool_size], params.limit)

    # Build Recommendation objects
    results: list[Recommendation] = []
    for cand in reranked:
        reason = _compose_reason(
            cand["drinking_status"],
            cand["urgency_score"],
            cand["occasion_score"],
            cand["pairing_score"],
            cand["best_pro_score"],
            cand["is_favorite"],
            params,
        )
        results.append(
            Recommendation(
                wine_id=cand["wine_id"],
                wine_name=cand["wine_name"],
                vintage=cand["vintage"],
                winery_name=cand["winery_name"],
                category=cand["category"],
                region=cand["region"],
                primary_grape=cand["primary_grape"],
                price=cand["price"],
                price_tier=cand["price_tier"],
                drinking_status=cand["drinking_status"],
                bottles_stored=cand["bottles_stored"],
                volume_ml=cand["volume_ml"],
                is_favorite=cand["is_favorite"],
                total_score=cand["total_score"],
                urgency_score=cand["urgency_score"],
                occasion_score=cand["occasion_score"],
                pairing_score=cand["pairing_score"],
                freshness_penalty=cand["freshness_penalty"],
                diversity_bonus=cand["diversity_bonus"],
                quality_bonus=cand["quality_bonus"],
                reason=reason,
            )
        )

    return results


# ---------------------------------------------------------------------------
# Markdown formatter
# ---------------------------------------------------------------------------


def format_recommendations(
    results: list[Recommendation],
    params: RecommendParams,
) -> str:
    """Format scored recommendations as a Markdown table with context.

    Examples:
        >>> format_recommendations([], RecommendParams())
        'No recommendations available for the current criteria.'
    """
    if not results:
        return "No recommendations available for the current criteria."

    # Context line
    context_parts: list[str] = []
    if params.occasion:
        context_parts.append(params.occasion.replace("_", " "))
    if params.cuisine:
        context_parts.append(params.cuisine)
    if params.guests:
        context_parts.append(f"{params.guests} guest{'s' if params.guests != 1 else ''}")
    if params.budget and params.budget != "any":
        context_parts.append(params.budget.replace("_", " "))
    context = ", ".join(context_parts) if context_parts else "urgency-based"

    lines = [
        f"# Tonight's Recommendations ({len(results)})",
        "",
        f"**Context:** {context}",
        "",
        "| # | Wine | Vintage | Score | Why |",
        "|--:|:-----|:-------:|------:|:----|",
    ]

    for i, rec in enumerate(results, 1):
        vintage_str = str(rec.vintage) if rec.vintage else "\u2014"
        lines.append(f"| {i} | {rec.wine_name} | {vintage_str} | {rec.total_score:.0f} | {rec.reason} |")

    return "\n".join(lines)
