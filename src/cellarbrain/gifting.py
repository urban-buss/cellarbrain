"""Wine gift advisor — recipient-aware gift selection and scoring.

Scores wines by gift-worthiness (prestige, storytelling, drinkability,
recognition, recipient fit, presentation) and returns ranked suggestions
with gift notes suitable for card writing or verbal introduction.
"""

from __future__ import annotations

import logging
import pathlib
import re
from dataclasses import dataclass

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

W_PRESTIGE = 0.25
W_STORYTELLING = 0.20
W_DRINKABILITY = 0.20
W_RECOGNITION = 0.15
W_RECIPIENT_FIT = 0.15
W_PRESENTATION = 0.05

GIFT_BUDGETS: dict[str, tuple[float, float]] = {
    "modest": (0, 30),
    "nice": (30, 60),
    "generous": (60, 120),
    "lavish": (120, 250),
    "extraordinary": (250, 10_000),
    "any": (0, 10_000),
}

GIFT_OCCASIONS: dict[str, dict[str, float]] = {
    "birthday": {"prestige": 1.2, "storytelling": 1.2, "recognition": 1.0},
    "milestone": {"prestige": 1.3, "storytelling": 1.2, "recognition": 1.1},
    "thank_you": {"drinkability": 1.3, "prestige": 0.9},
    "host_gift": {"drinkability": 1.3, "presentation": 1.2},
    "corporate": {"recognition": 1.4, "prestige": 1.2, "storytelling": 0.8},
    "romantic": {"presentation": 1.3, "prestige": 1.1},
    "holiday": {"prestige": 1.1, "storytelling": 1.1},
}

_DRINKABILITY_SCORES: dict[str | None, float] = {
    "optimal": 10.0,
    "drinkable": 7.0,
    "past_optimal": 4.0,
    "past_window": 1.0,
    "too_young": 2.0,
    None: 5.0,
}

_PRESTIGE_CLASSIFICATIONS: dict[str, float] = {
    "Grand Cru": 3.0,
    "Premier Cru": 2.0,
    "1er Cru": 2.0,
    "DOCG": 2.0,
    "Gran Reserva": 1.5,
    "Reserva": 1.0,
    "DOC": 1.0,
    "Cru Bourgeois": 1.0,
}

_DEFAULT_FAMOUS_REGIONS: frozenset[str] = frozenset(
    {
        "Burgundy",
        "Bordeaux",
        "Champagne",
        "Barolo",
        "Barbaresco",
        "Napa Valley",
        "Tuscany",
        "Rioja",
        "Mosel",
    }
)

# Keyword → structured filter mapping for recipient profile parsing
_CATEGORY_KEYWORDS: dict[str, str] = {
    "red": "Red wine",
    "reds": "Red wine",
    "white": "White wine",
    "whites": "White wine",
    "sparkling": "Sparkling wine",
    "champagne": "Sparkling wine",
    "bubbly": "Sparkling wine",
    "rosé": "Rosé",
    "rose": "Rosé",
    "sweet": "Sweet wine",
    "dessert": "Sweet wine",
}

_WEIGHT_KEYWORDS: dict[str, str] = {
    "bold": "bold",
    "powerful": "bold",
    "full-bodied": "bold",
    "big": "bold",
    "robust": "bold",
    "elegant": "light",
    "light": "light",
    "delicate": "light",
    "subtle": "light",
    "refined": "light",
}

_EXPERIENCE_KEYWORDS: dict[str, str] = {
    "novice": "novice",
    "beginner": "novice",
    "new": "novice",
    "collector": "collector",
    "expert": "collector",
    "connoisseur": "collector",
    "enthusiast": "enthusiast",
}

_REGION_KEYWORDS: dict[str, str] = {
    "italian": "Italy",
    "italy": "Italy",
    "french": "France",
    "france": "France",
    "spanish": "Spain",
    "spain": "Spain",
    "swiss": "Switzerland",
    "switzerland": "Switzerland",
    "german": "Germany",
    "germany": "Germany",
    "austrian": "Austria",
    "austria": "Austria",
    "portuguese": "Portugal",
    "portugal": "Portugal",
    "burgundy": "Burgundy",
    "bordeaux": "Bordeaux",
    "champagne region": "Champagne",
    "piedmont": "Piedmont",
    "tuscany": "Tuscany",
    "rioja": "Rioja",
    "barolo": "Barolo",
    "barbaresco": "Barbaresco",
    "rhône": "Rhône",
    "rhone": "Rhône",
    "napa": "Napa Valley",
    "mosel": "Mosel",
    "alsace": "Alsace",
    "loire": "Loire",
    "valais": "Valais",
}

_GRAPE_KEYWORDS: dict[str, str] = {
    "nebbiolo": "Nebbiolo",
    "pinot noir": "Pinot Noir",
    "pinot": "Pinot Noir",
    "cabernet": "Cabernet Sauvignon",
    "cabernet sauvignon": "Cabernet Sauvignon",
    "merlot": "Merlot",
    "syrah": "Syrah",
    "shiraz": "Syrah",
    "sangiovese": "Sangiovese",
    "tempranillo": "Tempranillo",
    "chardonnay": "Chardonnay",
    "riesling": "Riesling",
    "sauvignon blanc": "Sauvignon Blanc",
    "gamay": "Gamay",
    "malbec": "Malbec",
    "grenache": "Grenache",
    "barbera": "Barbera",
}

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecipientProfile:
    """Parsed recipient taste preferences."""

    categories: frozenset[str]
    regions: frozenset[str]
    grapes: frozenset[str]
    weight_pref: str | None
    experience: str | None
    raw_keywords: list[str]


@dataclass(frozen=True)
class GiftScore:
    """Gift-worthiness score breakdown."""

    prestige: float
    storytelling: float
    drinkability: float
    recognition: float
    recipient_fit: float
    presentation: float
    total: float


@dataclass(frozen=True)
class GiftSuggestion:
    """A single gift wine recommendation."""

    wine_id: int
    wine_name: str
    vintage: int | None
    winery_name: str
    category: str
    region: str | None
    classification: str | None
    primary_grape: str | None
    drinking_status: str | None
    bottles_available: int
    retail_value: float | None
    value_source: str
    gift_score: GiftScore
    gift_note: str


@dataclass(frozen=True)
class GiftPlan:
    """Complete gift advisor result."""

    profile_summary: str
    budget_tier: str
    occasion: str | None
    suggestions: list[GiftSuggestion]
    warnings: list[str]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_recipient_profile(text: str) -> RecipientProfile:
    """Parse free-text recipient description into structured filters.

    Examples:
        >>> p = parse_recipient_profile("loves bold Italian reds")
        >>> "Red wine" in p.categories
        True
        >>> "Italy" in p.regions
        True
        >>> p.weight_pref
        'bold'
    """
    lower = text.lower().strip()
    categories: set[str] = set()
    regions: set[str] = set()
    grapes: set[str] = set()
    weight_pref: str | None = None
    experience: str | None = None
    raw_keywords: list[str] = []

    # Multi-word matches first (longer phrases take priority)
    remaining = lower
    for phrase, grape in sorted(_GRAPE_KEYWORDS.items(), key=lambda x: -len(x[0])):
        if phrase in remaining:
            grapes.add(grape)
            remaining = remaining.replace(phrase, " ")

    for phrase, region in sorted(_REGION_KEYWORDS.items(), key=lambda x: -len(x[0])):
        if phrase in remaining:
            regions.add(region)
            remaining = remaining.replace(phrase, " ")

    # Single-word token matching
    tokens = remaining.split()
    for token in tokens:
        token_clean = token.strip(".,!?;:'\"")
        if not token_clean:
            continue

        if token_clean in _CATEGORY_KEYWORDS:
            categories.add(_CATEGORY_KEYWORDS[token_clean])
        elif token_clean in _WEIGHT_KEYWORDS:
            weight_pref = _WEIGHT_KEYWORDS[token_clean]
        elif token_clean in _EXPERIENCE_KEYWORDS:
            experience = _EXPERIENCE_KEYWORDS[token_clean]
        elif token_clean in _REGION_KEYWORDS:
            regions.add(_REGION_KEYWORDS[token_clean])
        elif token_clean in _GRAPE_KEYWORDS:
            grapes.add(_GRAPE_KEYWORDS[token_clean])
        else:
            # Skip common filler words
            if token_clean not in {
                "loves",
                "likes",
                "enjoys",
                "prefers",
                "who",
                "a",
                "an",
                "the",
                "and",
                "or",
                "with",
                "from",
                "for",
                "wine",
                "wines",
                "very",
                "really",
                "quite",
                "mostly",
                "usually",
                "food",
            }:
                raw_keywords.append(token_clean)

    return RecipientProfile(
        categories=frozenset(categories),
        regions=frozenset(regions),
        grapes=frozenset(grapes),
        weight_pref=weight_pref,
        experience=experience,
        raw_keywords=raw_keywords,
    )


def parse_budget(budget_str: str) -> tuple[float, float]:
    """Parse budget string into (min, max) CHF range.

    Accepts named tiers ("generous") or explicit ranges ("80-150").

    Examples:
        >>> parse_budget("generous")
        (60, 120)
        >>> parse_budget("80-150")
        (80.0, 150.0)
        >>> parse_budget("any")
        (0, 10000)

    Raises:
        ValueError: If budget string is neither a known tier nor valid range.
    """
    budget_lower = budget_str.strip().lower()

    if budget_lower in GIFT_BUDGETS:
        return GIFT_BUDGETS[budget_lower]

    # Try explicit range: "80-150" or "80 - 150"
    match = re.match(r"(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)", budget_lower)
    if match:
        return float(match.group(1)), float(match.group(2))

    raise ValueError(
        f"Unknown budget '{budget_str}'. Use a tier ({', '.join(GIFT_BUDGETS.keys())}) or a range like '80-150'."
    )


# ---------------------------------------------------------------------------
# Scoring functions (pure — no side effects)
# ---------------------------------------------------------------------------


def _score_prestige(
    best_pro_score: float | None,
    classification: str | None,
    famous_regions: frozenset[str],
    region: str | None = None,
) -> float:
    """Compute prestige score (0–10) from critic scores and classification.

    Examples:
        >>> _score_prestige(96.0, "Grand Cru", _DEFAULT_FAMOUS_REGIONS)
        10.0
        >>> _score_prestige(None, None, _DEFAULT_FAMOUS_REGIONS)
        0.0
    """
    score = 0.0
    if best_pro_score and best_pro_score > 88:
        score += min((best_pro_score - 88) / 12.0 * 7.0, 7.0)
    if classification:
        for label, bonus in _PRESTIGE_CLASSIFICATIONS.items():
            if label.lower() in classification.lower():
                score += bonus
                break
    if region and region in famous_regions:
        score += 1.5
    return min(score, 10.0)


def _score_storytelling(
    has_producer_profile: bool,
    producer_profile_length: int,
    has_wine_description: bool,
    has_vintage_report: bool,
    age_years: int | None,
    classification: str | None,
) -> float:
    """Compute storytelling score (0–10) from dossier richness.

    Examples:
        >>> _score_storytelling(True, 400, True, True, 15, "DOCG")
        10.0
        >>> _score_storytelling(False, 0, False, False, None, None)
        0.0
    """
    score = 0.0
    if has_producer_profile:
        score += min(producer_profile_length / 200.0, 3.0)
    if has_wine_description:
        score += 2.0
    if has_vintage_report:
        score += 2.0
    if age_years and age_years >= 10:
        score += min(age_years / 10.0, 2.0)
    if classification:
        score += 1.0
    return min(score, 10.0)


def _score_drinkability(drinking_status: str | None) -> float:
    """Compute drinkability score (0–10) from drinking window status.

    Examples:
        >>> _score_drinkability("optimal")
        10.0
        >>> _score_drinkability("too_young")
        2.0
        >>> _score_drinkability(None)
        5.0
    """
    return _DRINKABILITY_SCORES.get(drinking_status, 5.0)


def _score_recognition(
    pro_rating_count: int,
    best_pro_score: float | None,
    region: str | None,
    classification: str | None,
    famous_regions: frozenset[str],
) -> float:
    """Compute recognition score (0–10) from fame indicators.

    Examples:
        >>> _score_recognition(5, 95.0, "Burgundy", "Grand Cru", _DEFAULT_FAMOUS_REGIONS)
        10.0
        >>> _score_recognition(0, None, None, None, _DEFAULT_FAMOUS_REGIONS)
        0.0
    """
    score = 0.0
    if pro_rating_count >= 5:
        score += 3.0
    elif pro_rating_count >= 2:
        score += 1.5
    if best_pro_score and best_pro_score >= 93:
        score += 2.0
    if (region and region in famous_regions) or (classification and "Grand Cru" in classification):
        score += 3.0
    if classification:
        score += 2.0
    return min(score, 10.0)


def _score_recipient_fit(
    category: str,
    region: str | None,
    country: str | None,
    primary_grape: str | None,
    profile: RecipientProfile,
) -> float:
    """Compute recipient fit score (0–10) against parsed profile.

    Examples:
        >>> p = RecipientProfile(frozenset({"Red wine"}), frozenset({"Italy"}), frozenset(), None, None, [])
        >>> _score_recipient_fit("Red wine", "Piedmont", "Italy", "Nebbiolo", p)
        6.0
    """
    if not profile.categories and not profile.regions and not profile.grapes:
        return 5.0

    score = 0.0
    if profile.categories and category in profile.categories:
        score += 4.0
    if profile.regions:
        if region and region in profile.regions:
            score += 3.0
        elif country and country in profile.regions:
            score += 2.0
    if profile.grapes and primary_grape and primary_grape in profile.grapes:
        score += 3.0
    return min(score, 10.0)


def _score_presentation(volume_ml: int, budget_tier: str) -> float:
    """Compute presentation score (0–10) from bottle format.

    Examples:
        >>> _score_presentation(1500, "lavish")
        9.0
        >>> _score_presentation(750, "generous")
        6.0
        >>> _score_presentation(375, "modest")
        4.0
    """
    if volume_ml >= 1500 and budget_tier in ("lavish", "extraordinary"):
        return 9.0
    if volume_ml == 375:
        return 4.0
    if volume_ml >= 1500:
        return 7.0
    return 6.0  # standard 750ml


def _compute_gift_score(
    prestige: float,
    storytelling: float,
    drinkability: float,
    recognition: float,
    recipient_fit: float,
    presentation: float,
    occasion: str | None = None,
) -> GiftScore:
    """Compute weighted gift-worthiness score with occasion adjustments."""
    # Apply occasion weight adjustments
    adjustments = GIFT_OCCASIONS.get(occasion or "", {})
    adj_prestige = prestige * adjustments.get("prestige", 1.0)
    adj_storytelling = storytelling * adjustments.get("storytelling", 1.0)
    adj_drinkability = drinkability * adjustments.get("drinkability", 1.0)
    adj_recognition = recognition * adjustments.get("recognition", 1.0)
    adj_recipient_fit = recipient_fit * adjustments.get("recipient_fit", 1.0)
    adj_presentation = presentation * adjustments.get("presentation", 1.0)

    total = (
        adj_prestige * W_PRESTIGE
        + adj_storytelling * W_STORYTELLING
        + adj_drinkability * W_DRINKABILITY
        + adj_recognition * W_RECOGNITION
        + adj_recipient_fit * W_RECIPIENT_FIT
        + adj_presentation * W_PRESENTATION
    )

    return GiftScore(
        prestige=round(min(adj_prestige, 10.0), 1),
        storytelling=round(min(adj_storytelling, 10.0), 1),
        drinkability=round(min(adj_drinkability, 10.0), 1),
        recognition=round(min(adj_recognition, 10.0), 1),
        recipient_fit=round(min(adj_recipient_fit, 10.0), 1),
        presentation=round(min(adj_presentation, 10.0), 1),
        total=round(min(total, 10.0), 1),
    )


# ---------------------------------------------------------------------------
# Retail value estimation
# ---------------------------------------------------------------------------


def _estimate_retail_value(
    wine_id: int,
    purchase_price: float | None,
    price_observations: dict[int, float] | None,
    markup_factor: float = 1.0,
) -> tuple[float | None, str]:
    """Estimate the retail/gift value of a wine.

    Returns (estimated_value, source) where source is "market", "purchase",
    or "unknown".

    Examples:
        >>> _estimate_retail_value(1, 50.0, {1: 80.0})
        (80.0, 'market')
        >>> _estimate_retail_value(2, 50.0, None)
        (50.0, 'purchase')
        >>> _estimate_retail_value(3, None, None)
        (None, 'unknown')
    """
    if price_observations and wine_id in price_observations:
        return price_observations[wine_id], "market"
    if purchase_price:
        return purchase_price * markup_factor, "purchase"
    return None, "unknown"


# ---------------------------------------------------------------------------
# Gift note generation
# ---------------------------------------------------------------------------


def _generate_gift_note(
    wine_name: str,
    winery_name: str,
    region: str | None,
    vintage: int | None,
    classification: str | None,
    wine_description_excerpt: str | None,
) -> str:
    """Generate a 1-2 sentence gift note for a bottle.

    Uses the dossier's wine_description if available, otherwise constructs
    from structured data.

    Examples:
        >>> _generate_gift_note("Barolo", "Conterno", "Piedmont", 2016, "DOCG", None)
        'A DOCG Barolo from Conterno in Piedmont (2016 vintage).'
    """
    if wine_description_excerpt:
        sentences = wine_description_excerpt.split(". ")
        excerpt = ". ".join(sentences[:2]).strip()
        if not excerpt.endswith("."):
            excerpt += "."
        return excerpt

    parts: list[str] = []
    classification_prefix = f"{classification} " if classification else ""
    parts.append(f"A {classification_prefix}{wine_name}")
    if winery_name:
        parts.append(f"from {winery_name}")
    if region:
        parts.append(f"in {region}")
    if vintage:
        parts.append(f"({vintage} vintage)")
    return " ".join(parts) + "."


# ---------------------------------------------------------------------------
# Candidate SQL
# ---------------------------------------------------------------------------

_CANDIDATE_COLUMNS = (
    "wine_id",
    "winery_name",
    "wine_name",
    "vintage",
    "category",
    "country",
    "region",
    "subregion",
    "classification",
    "primary_grape",
    "price",
    "price_tier",
    "price_per_750ml",
    "drinking_status",
    "bottles_stored",
    "volume_ml",
    "bottle_format",
    "best_pro_score",
    "is_favorite",
    "age_years",
    "pro_rating_count",
)

_CANDIDATE_SQL = f"""\
SELECT {", ".join(_CANDIDATE_COLUMNS)}
FROM wines_full
WHERE bottles_stored >= $1
  AND (drinking_status IS NULL OR drinking_status != 'too_young')
"""


# ---------------------------------------------------------------------------
# Core orchestrator
# ---------------------------------------------------------------------------


def suggest_gifts(
    con: duckdb.DuckDBPyConnection,
    profile_text: str,
    budget: str = "any",
    occasion: str | None = None,
    data_dir: str | pathlib.Path | None = None,
    limit: int = 3,
    min_bottles: int = 2,
    markup_factor: float = 1.0,
    famous_regions: frozenset[str] | None = None,
) -> GiftPlan:
    """Score and rank cellar wines for gifting.

    Args:
        con: DuckDB connection with wines_full view available.
        profile_text: Free-text recipient taste description.
        budget: Named tier or "MIN-MAX" range string.
        occasion: Gift occasion for weight adjustments.
        data_dir: Data directory for dossier reads.
        limit: Number of suggestions to return.
        min_bottles: Minimum bottles_stored threshold.
        markup_factor: Multiplier for purchase price when no market data.
        famous_regions: Override set of famous region names.

    Returns:
        GiftPlan with ranked suggestions and warnings.
    """
    if famous_regions is None:
        famous_regions = _DEFAULT_FAMOUS_REGIONS

    # Parse inputs
    profile = parse_recipient_profile(profile_text)
    budget_min, budget_max = parse_budget(budget)
    budget_tier = budget.strip().lower() if budget.strip().lower() in GIFT_BUDGETS else "any"

    warnings: list[str] = []

    # Build SQL with filters
    sql = _CANDIDATE_SQL
    params: list = [min_bottles]

    # Category filter
    if profile.categories:
        cat_placeholders = ", ".join("?" for _ in profile.categories)
        sql += f"  AND category IN ({cat_placeholders})\n"
        params.extend(sorted(profile.categories))

    # Budget filter on price
    if budget_min > 0:
        sql += "  AND (price >= ? OR price IS NULL)\n"
        params.append(budget_min)
    if budget_max < 10_000:
        sql += "  AND (price <= ? OR price IS NULL)\n"
        params.append(budget_max)

    sql += "ORDER BY best_pro_score DESC NULLS LAST, price DESC NULLS LAST\n"
    sql += "LIMIT 50\n"

    try:
        rows = con.execute(sql, params).fetchall()
    except Exception:
        logger.warning("Gift advisor query failed", exc_info=True)
        return GiftPlan(
            profile_summary=profile_text,
            budget_tier=budget_tier,
            occasion=occasion,
            suggestions=[],
            warnings=["Query failed — ensure Parquet data exists."],
        )

    if not rows and profile.categories:
        sql_relaxed = _CANDIDATE_SQL
        params_relaxed: list = [min_bottles]
        if budget_min > 0:
            sql_relaxed += "  AND (price >= ? OR price IS NULL)\n"
            params_relaxed.append(budget_min)
        if budget_max < 10_000:
            sql_relaxed += "  AND (price <= ? OR price IS NULL)\n"
            params_relaxed.append(budget_max)
        sql_relaxed += "ORDER BY best_pro_score DESC NULLS LAST, price DESC NULLS LAST\nLIMIT 50\n"
        try:
            rows = con.execute(sql_relaxed, params_relaxed).fetchall()
            if rows:
                warnings.append("No exact category match — showing best available wines.")
        except Exception:
            pass

    if not rows:
        return GiftPlan(
            profile_summary=profile_text,
            budget_tier=budget_tier,
            occasion=occasion,
            suggestions=[],
            warnings=["No wines match the criteria. Try a wider budget or fewer constraints."],
        )

    # Build column index
    col_idx = {name: i for i, name in enumerate(_CANDIDATE_COLUMNS)}

    # Load price observations for retail value estimation
    price_obs = _load_price_observations(con)

    # Get dossier metadata for storytelling scoring
    dossier_meta = {}
    if data_dir:
        dossier_meta = _load_dossier_metadata(data_dir, [row[col_idx["wine_id"]] for row in rows[:20]])

    # Score all candidates
    scored: list[tuple[float, dict]] = []
    for row in rows:
        wine_id = row[col_idx["wine_id"]]
        region = row[col_idx["region"]]
        classification = row[col_idx["classification"]]
        best_pro_score = row[col_idx["best_pro_score"]]
        pro_rating_count = row[col_idx["pro_rating_count"]] or 0

        # Dossier metadata for this wine
        meta = dossier_meta.get(wine_id, {})

        prestige = _score_prestige(best_pro_score, classification, famous_regions, region)
        storytelling = _score_storytelling(
            meta.get("has_producer_profile", False),
            meta.get("producer_profile_length", 0),
            meta.get("has_wine_description", False),
            meta.get("has_vintage_report", False),
            row[col_idx["age_years"]],
            classification,
        )
        drinkability = _score_drinkability(row[col_idx["drinking_status"]])
        recognition = _score_recognition(pro_rating_count, best_pro_score, region, classification, famous_regions)
        recipient_fit = _score_recipient_fit(
            row[col_idx["category"]],
            region,
            row[col_idx["country"]],
            row[col_idx["primary_grape"]],
            profile,
        )
        presentation = _score_presentation(
            row[col_idx["volume_ml"]] or 750,
            budget_tier,
        )

        gift_score = _compute_gift_score(
            prestige,
            storytelling,
            drinkability,
            recognition,
            recipient_fit,
            presentation,
            occasion=occasion,
        )

        retail_value, value_source = _estimate_retail_value(wine_id, row[col_idx["price"]], price_obs, markup_factor)

        candidate = {
            "wine_id": wine_id,
            "wine_name": row[col_idx["wine_name"]],
            "vintage": row[col_idx["vintage"]],
            "winery_name": row[col_idx["winery_name"]],
            "category": row[col_idx["category"]],
            "region": region,
            "classification": classification,
            "primary_grape": row[col_idx["primary_grape"]],
            "drinking_status": row[col_idx["drinking_status"]],
            "bottles_available": row[col_idx["bottles_stored"]],
            "retail_value": retail_value,
            "value_source": value_source,
            "gift_score": gift_score,
            "wine_description_excerpt": meta.get("wine_description_excerpt"),
        }
        scored.append((gift_score.total, candidate))

    # Sort by score descending
    scored.sort(key=lambda x: x[0], reverse=True)

    # Diversity re-rank: no duplicate wineries
    selected: list[dict] = []
    seen_wineries: set[str] = set()
    for _, cand in scored:
        if len(selected) >= limit:
            break
        winery = cand["winery_name"] or ""
        if winery and winery in seen_wineries:
            continue
        selected.append(cand)
        if winery:
            seen_wineries.add(winery)

    # If diversity was too strict, fill remaining slots
    if len(selected) < limit:
        for _, cand in scored:
            if len(selected) >= limit:
                break
            if cand not in selected:
                selected.append(cand)

    # Generate gift notes for selected wines
    suggestions: list[GiftSuggestion] = []
    for cand in selected:
        gift_note = _generate_gift_note(
            cand["wine_name"] or "",
            cand["winery_name"] or "",
            cand["region"],
            cand["vintage"],
            cand["classification"],
            cand.get("wine_description_excerpt"),
        )
        suggestions.append(
            GiftSuggestion(
                wine_id=cand["wine_id"],
                wine_name=cand["wine_name"] or "",
                vintage=cand["vintage"],
                winery_name=cand["winery_name"] or "",
                category=cand["category"] or "",
                region=cand["region"],
                classification=cand["classification"],
                primary_grape=cand["primary_grape"],
                drinking_status=cand["drinking_status"],
                bottles_available=cand["bottles_available"],
                retail_value=cand["retail_value"],
                value_source=cand["value_source"],
                gift_score=cand["gift_score"],
                gift_note=gift_note,
            )
        )

    # Add warnings for value_source == "purchase" or "unknown"
    purchase_only = [s for s in suggestions if s.value_source == "purchase"]
    if purchase_only:
        warnings.append(
            f"No market price data for {len(purchase_only)} wine(s) — gift value estimated from purchase price."
        )

    return GiftPlan(
        profile_summary=profile_text,
        budget_tier=budget_tier,
        occasion=occasion,
        suggestions=suggestions,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_price_observations(con: duckdb.DuckDBPyConnection) -> dict[int, float]:
    """Load latest price observations keyed by wine_id.

    Returns wine_id → latest price_chf (or price) from price_observation
    data if available.  Returns empty dict if the view doesn't exist.
    """
    try:
        # Check if tracked wines + price observations are available
        rows = con.execute("""\
            SELECT w.wine_id, po.price_chf
            FROM wines_full w
            INNER JOIN tracked_wine tw ON w.tracked_wine_id = tw.tracked_wine_id
            INNER JOIN latest_prices po ON tw.tracked_wine_id = po.tracked_wine_id
            WHERE w.bottles_stored > 0 AND po.price_chf IS NOT NULL
        """).fetchall()
        return {row[0]: float(row[1]) for row in rows}
    except Exception:
        # Views may not exist (no tracked wines or price observations)
        return {}


def _load_dossier_metadata(
    data_dir: str | pathlib.Path,
    wine_ids: list[int],
) -> dict[int, dict]:
    """Load dossier section presence metadata for scoring.

    Returns wine_id → {has_producer_profile, producer_profile_length,
    has_wine_description, has_vintage_report, wine_description_excerpt}.
    """
    from .dossier_ops import WineNotFoundError, read_dossier_sections

    d = pathlib.Path(data_dir)
    result: dict[int, dict] = {}

    for wine_id in wine_ids[:20]:  # Cap to avoid excessive I/O
        meta: dict = {
            "has_producer_profile": False,
            "producer_profile_length": 0,
            "has_wine_description": False,
            "has_vintage_report": False,
            "wine_description_excerpt": None,
        }
        try:
            text = read_dossier_sections(
                wine_id,
                d,
                sections=["producer_profile", "wine_description", "vintage_report"],
            )
            # Check section presence by looking for the H2/H3 headings in returned text
            if "### Producer Profile" in text or "## Producer Profile" in text:
                meta["has_producer_profile"] = True
                # Estimate length of producer profile content
                idx = text.find("### Producer Profile")
                if idx == -1:
                    idx = text.find("## Producer Profile")
                if idx >= 0:
                    # Content between this heading and next heading
                    after = text[idx:]
                    lines = after.split("\n")[1:]  # skip heading line
                    content_lines: list[str] = []
                    for line in lines:
                        if line.startswith("## ") or line.startswith("### "):
                            break
                        content_lines.append(line)
                    profile_text = "\n".join(content_lines).strip()
                    meta["producer_profile_length"] = len(profile_text)

            if "### Wine Description" in text or "## Wine Description" in text:
                meta["has_wine_description"] = True
                # Extract first paragraph for gift note
                idx = text.find("### Wine Description")
                if idx == -1:
                    idx = text.find("## Wine Description")
                if idx >= 0:
                    after = text[idx:]
                    lines = after.split("\n")[1:]
                    content_lines = []
                    for line in lines:
                        if line.startswith("## ") or line.startswith("### "):
                            break
                        content_lines.append(line)
                    desc_text = "\n".join(content_lines).strip()
                    # Remove markdown formatting for excerpt
                    desc_clean = re.sub(r"\*\*|__|\*|_", "", desc_text)
                    # Skip placeholder/pending markers
                    if desc_clean and "pending" not in desc_clean.lower()[:20]:
                        meta["wine_description_excerpt"] = desc_clean[:500]

            if "### Vintage Report" in text or "## Vintage Report" in text:
                meta["has_vintage_report"] = True

        except (WineNotFoundError, ValueError, OSError):
            pass

        result[wine_id] = meta

    return result


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def format_gift_suggestions(plan: GiftPlan) -> str:
    """Format a GiftPlan as Markdown output.

    Examples:
        >>> plan = GiftPlan("test", "any", None, [], ["No wines found."])
        >>> "No gift suggestions" in format_gift_suggestions(plan)
        True
    """
    if not plan.suggestions:
        lines = [
            "## Gift Advisor",
            "",
            "*No gift suggestions available for the current criteria.*",
        ]
        if plan.warnings:
            lines.append("")
            for w in plan.warnings:
                lines.append(f"- {w}")
        return "\n".join(lines)

    # Header
    lines = [
        "## 🎁 Gift Advisor",
        "",
    ]

    # Context line
    context_parts: list[str] = []
    context_parts.append(f"**Recipient:** {plan.profile_summary}")
    if plan.budget_tier != "any":
        budget_range = GIFT_BUDGETS.get(plan.budget_tier, (0, 0))
        context_parts.append(f"**Budget:** CHF {budget_range[0]}-{budget_range[1]}")
    if plan.occasion:
        context_parts.append(f"**Occasion:** {plan.occasion.replace('_', ' ').title()}")
    lines.append(" | ".join(context_parts))
    lines.append("")
    lines.append("---")
    lines.append("")

    # Top pick
    top = plan.suggestions[0]
    lines.append("### Top Pick")
    lines.append("")
    lines.append(f"**{top.wine_name} {top.vintage or ''}** — {top.winery_name}".strip())

    detail_parts: list[str] = []
    if top.region:
        detail_parts.append(top.region)
    if top.classification:
        detail_parts.append(top.classification)
    if top.primary_grape:
        detail_parts.append(top.primary_grape)
    if top.gift_score.prestige >= 7.0 and top.retail_value:
        detail_parts.append(f"{top.gift_score.prestige:.0f} pts prestige")
    lines.append(" · ".join(detail_parts) if detail_parts else "")

    # Value and availability
    value_str = ""
    if top.retail_value:
        value_str = f"Gift value: ~CHF {top.retail_value:.0f} ({top.value_source})"
    avail_str = f"{top.bottles_available} bottle{'s' if top.bottles_available != 1 else ''} available"
    status_str = top.drinking_status.replace("_", " ").title() if top.drinking_status else "Unknown"
    lines.append(f"{value_str} · {avail_str} · {status_str}")
    lines.append("")

    # Gift note
    lines.append(f'> "{top.gift_note}"')
    lines.append("")

    # Score breakdown
    gs = top.gift_score
    lines.append(f"**Gift score:** {gs.total}/10")
    lines.append("| Prestige | Story | Drinkability | Recognition | Fit | Presentation |")
    lines.append("|:--------:|:-----:|:------------:|:-----------:|:---:|:------------:|")
    lines.append(
        f"| {gs.prestige} | {gs.storytelling} | {gs.drinkability} "
        f"| {gs.recognition} | {gs.recipient_fit} | {gs.presentation} |"
    )

    # Also recommended
    if len(plan.suggestions) > 1:
        lines.append("")
        lines.append("---")
        lines.append("")
        lines.append("### Also Recommended")
        lines.append("")

        for i, s in enumerate(plan.suggestions[1:], 2):
            lines.append(f"**{i}. {s.wine_name} {s.vintage or ''}** — {s.winery_name}".strip())
            parts: list[str] = []
            if s.region:
                parts.append(s.region)
            if s.classification:
                parts.append(s.classification)
            if s.primary_grape:
                parts.append(s.primary_grape)
            lines.append(" · ".join(parts) if parts else "")
            val = f"~CHF {s.retail_value:.0f} ({s.value_source})" if s.retail_value else "Price unknown"
            lines.append(
                f"{val} · {s.bottles_available} bottles · {(s.drinking_status or 'unknown').replace('_', ' ').title()}"
            )
            lines.append("")
            lines.append(f'> "{s.gift_note}"')
            lines.append("")
            lines.append(f"**Gift score:** {s.gift_score.total}/10")
            lines.append("")

    # Warnings
    if plan.warnings:
        lines.append("---")
        lines.append("")
        lines.append("### Notes")
        lines.append("")
        for w in plan.warnings:
            lines.append(f"- ⚠️ {w}")

    return "\n".join(lines)
