"""Cellar matching for extracted promotions.

Compares wine names from promotions against the ``wines_full`` view
in DuckDB using ``difflib.SequenceMatcher`` for fuzzy string matching.

Enhanced matching (QW-5) adds three match categories:
- **rebuy**: promotion for a wine already owned, priced below purchase price
- **similar**: promotion for a wine structurally similar to cellar favorites
- **gap_fill**: promotion for a wine in an underrepresented cellar dimension
"""

from __future__ import annotations

import difflib
import logging
from decimal import Decimal

from .models import ExtractedPromotion, PromotionMatch

logger = logging.getLogger(__name__)

_FUZZY_THRESHOLD = 0.75
_SIMILAR_THRESHOLD = 0.30
_GAP_BOTTLE_THRESHOLD = 2  # dimensions with fewer bottles are "gaps"


def match_promotions(
    promotions: list[ExtractedPromotion],
    data_dir: str,
) -> list[PromotionMatch]:
    """Match extracted promotions against the cellar.

    Queries ``wines_full`` for all owned wines, then fuzzy-matches each
    promotion's wine name against the cellar list. Returns a
    ``PromotionMatch`` for every promotion that matches above the
    similarity threshold.
    """
    if not promotions:
        return []

    cellar_wines = _load_cellar_wines(data_dir)
    if not cellar_wines:
        return []

    matches: list[PromotionMatch] = []
    for promo in promotions:
        best = _best_match(promo, cellar_wines)
        if best is not None:
            matches.append(best)

    logger.info(
        "Matched %d of %d promotions against cellar (%d wines)",
        len(matches),
        len(promotions),
        len(cellar_wines),
    )
    return matches


def _load_cellar_wines(data_dir: str) -> list[dict]:
    """Load wine names and metadata from the cellar via DuckDB."""
    try:
        import pathlib

        from ..query import get_agent_connection

        con = get_agent_connection(pathlib.Path(data_dir))
        rows = con.execute(
            "SELECT wine_id, wine_name, vintage, winery_name, "
            "bottles_stored, price "
            "FROM wines_full "
            "WHERE bottles_stored > 0"
        ).fetchall()
        con.close()

        return [
            {
                "wine_id": r[0],
                "wine_name": r[1],
                "vintage": r[2],
                "winery_name": r[3],
                "bottles_stored": r[4],
                "price": r[5],
            }
            for r in rows
        ]
    except Exception as exc:
        logger.warning("Cannot load cellar wines for matching: %s", exc)
        return []


def _normalise(name: str) -> str:
    """Lowercase and strip common noise for matching."""
    return name.lower().strip()


def _best_match(
    promo: ExtractedPromotion,
    cellar_wines: list[dict],
) -> PromotionMatch | None:
    """Find the best cellar match for a single promotion."""
    promo_name = _normalise(promo.wine_name)
    best_score = 0.0
    best_wine: dict | None = None

    for wine in cellar_wines:
        cellar_name = _normalise(wine["wine_name"])
        score = difflib.SequenceMatcher(None, promo_name, cellar_name).ratio()
        if score > best_score:
            best_score = score
            best_wine = wine

    if best_wine is None or best_score < _FUZZY_THRESHOLD:
        return None

    match_type = "exact" if best_score >= 0.95 else "fuzzy"

    # Compute discount vs reference (cellar purchase price)
    discount_vs_ref: float | None = None
    ref_price: Decimal | None = None
    if best_wine["price"] is not None:
        ref_price = Decimal(str(best_wine["price"]))
        if ref_price > 0:
            discount_vs_ref = round(float((1 - promo.sale_price / ref_price) * 100), 1)

    return PromotionMatch(
        promotion=promo,
        match_type=match_type,
        confidence=round(best_score, 3),
        wine_id=best_wine["wine_id"],
        wine_name=best_wine["wine_name"],
        bottles_owned=best_wine["bottles_stored"],
        reference_price=ref_price,
        reference_source="cellar_purchase",
        discount_vs_reference=discount_vs_ref,
    )


# ---------------------------------------------------------------------------
# Enhanced matching — QW-5: cellar relevance scoring
# ---------------------------------------------------------------------------


def score_promotions(
    promotions: list[ExtractedPromotion],
    data_dir: str,
) -> list[PromotionMatch]:
    """Score promotions against cellar with enhanced match categories.

    For each promotion, tries (in order):
    1. Exact/fuzzy cellar match → classify as rebuy if cheaper than owned
    2. Structural similarity to cellar wines → "similar"
    3. Fills an identified cellar gap → "gap_fill"

    Returns all matches sorted by value_score descending.
    """
    if not promotions:
        return []

    cellar_wines = _load_enriched_cellar_wines(data_dir)
    if not cellar_wines:
        return []

    composition = _build_cellar_composition(cellar_wines)
    gaps = _identify_gaps(composition)

    scored: list[PromotionMatch] = []
    for promo in promotions:
        match = _score_single_promotion(promo, cellar_wines, gaps)
        if match is not None:
            scored.append(match)

    scored.sort(key=lambda m: m.value_score, reverse=True)
    logger.info(
        "Scored %d of %d promotions (rebuy=%d, similar=%d, gap_fill=%d)",
        len(scored),
        len(promotions),
        sum(1 for m in scored if m.match_category == "rebuy"),
        sum(1 for m in scored if m.match_category == "similar"),
        sum(1 for m in scored if m.match_category == "gap_fill"),
    )
    return scored


def _load_enriched_cellar_wines(data_dir: str) -> list[dict]:
    """Load cellar wines with additional columns for similarity scoring."""
    try:
        import pathlib

        from ..query import get_agent_connection

        con = get_agent_connection(pathlib.Path(data_dir))
        rows = con.execute(
            "SELECT wine_id, wine_name, vintage, winery_name, "
            "bottles_stored, price, category, country, region, "
            "primary_grape, price_tier, is_favorite, best_pro_score "
            "FROM wines_full "
            "WHERE bottles_stored > 0"
        ).fetchall()
        con.close()

        return [
            {
                "wine_id": r[0],
                "wine_name": r[1],
                "vintage": r[2],
                "winery_name": r[3],
                "bottles_stored": r[4],
                "price": r[5],
                "category": r[6],
                "country": r[7],
                "region": r[8],
                "primary_grape": r[9],
                "price_tier": r[10],
                "is_favorite": r[11],
                "best_pro_score": r[12],
            }
            for r in rows
        ]
    except Exception as exc:
        logger.warning("Cannot load enriched cellar wines: %s", exc)
        return []


def _build_cellar_composition(cellar_wines: list[dict]) -> dict:
    """Build a profile of cellar bottle distribution by dimension."""
    by_region: dict[str, int] = {}
    by_grape: dict[str, int] = {}
    by_price_tier: dict[str, int] = {}
    by_category: dict[str, int] = {}

    for wine in cellar_wines:
        bottles = wine.get("bottles_stored", 0) or 0
        region = wine.get("region") or ""
        grape = wine.get("primary_grape") or ""
        tier = wine.get("price_tier") or ""
        cat = wine.get("category") or ""

        if region:
            by_region[region] = by_region.get(region, 0) + bottles
        if grape:
            by_grape[grape] = by_grape.get(grape, 0) + bottles
        if tier:
            by_price_tier[tier] = by_price_tier.get(tier, 0) + bottles
        if cat:
            by_category[cat] = by_category.get(cat, 0) + bottles

    return {
        "by_region": by_region,
        "by_grape": by_grape,
        "by_price_tier": by_price_tier,
        "by_category": by_category,
        "total_bottles": sum(w.get("bottles_stored", 0) or 0 for w in cellar_wines),
    }


def _identify_gaps(composition: dict) -> list[dict]:
    """Find underrepresented dimensions in the cellar.

    A gap is any dimension value with fewer than _GAP_BOTTLE_THRESHOLD
    bottles. Only reports gaps in dimensions that have *some* variety
    (avoids flagging when cellar is too small).
    """
    gaps: list[dict] = []

    for dimension, dist in [
        ("region", composition["by_region"]),
        ("grape", composition["by_grape"]),
        ("price_tier", composition["by_price_tier"]),
    ]:
        if len(dist) < 2:
            continue
        for value, count in dist.items():
            if count < _GAP_BOTTLE_THRESHOLD:
                gaps.append(
                    {
                        "dimension": dimension,
                        "value": value,
                        "bottles": count,
                    }
                )

    return gaps


def _score_single_promotion(
    promo: ExtractedPromotion,
    cellar_wines: list[dict],
    gaps: list[dict],
) -> PromotionMatch | None:
    """Score a single promotion: try exact → similar → gap_fill."""
    # 1. Try exact/fuzzy match (cellar wine by name)
    cellar_match = _best_match_enriched(promo, cellar_wines)
    if cellar_match is not None:
        return _classify_cellar_match(cellar_match)

    # 2. Try structural similarity
    similar_match = _find_similar_match(promo, cellar_wines)
    if similar_match is not None:
        return similar_match

    # 3. Try gap fill
    gap_match = _find_gap_match(promo, gaps)
    if gap_match is not None:
        return gap_match

    return None


def _best_match_enriched(
    promo: ExtractedPromotion,
    cellar_wines: list[dict],
) -> PromotionMatch | None:
    """Find best cellar match using enriched wine data."""
    promo_name = _normalise(promo.wine_name)
    best_score = 0.0
    best_wine: dict | None = None

    for wine in cellar_wines:
        cellar_name = _normalise(wine["wine_name"])
        score = difflib.SequenceMatcher(None, promo_name, cellar_name).ratio()
        if score > best_score:
            best_score = score
            best_wine = wine

    if best_wine is None or best_score < _FUZZY_THRESHOLD:
        return None

    match_type = "exact" if best_score >= 0.95 else "fuzzy"

    discount_vs_ref: float | None = None
    ref_price: Decimal | None = None
    if best_wine["price"] is not None:
        ref_price = Decimal(str(best_wine["price"]))
        if ref_price > 0:
            discount_vs_ref = round(float((1 - promo.sale_price / ref_price) * 100), 1)

    return PromotionMatch(
        promotion=promo,
        match_type=match_type,
        confidence=round(best_score, 3),
        wine_id=best_wine["wine_id"],
        wine_name=best_wine["wine_name"],
        bottles_owned=best_wine["bottles_stored"],
        reference_price=ref_price,
        reference_source="cellar_purchase",
        discount_vs_reference=discount_vs_ref,
    )


def _classify_cellar_match(match: PromotionMatch) -> PromotionMatch:
    """Classify a cellar match as rebuy (cheaper) or plain match."""
    if match.discount_vs_reference is not None and match.discount_vs_reference > 0:
        # Promo is cheaper than what we paid → rebuy opportunity
        match.match_category = "rebuy"
        # Higher discount = higher value
        discount_bonus = min(match.discount_vs_reference / 100, 0.2)
        match.value_score = round(0.8 + discount_bonus, 3)
    else:
        match.match_category = "rebuy"
        match.value_score = 0.4
    return match


def _find_similar_match(
    promo: ExtractedPromotion,
    cellar_wines: list[dict],
) -> PromotionMatch | None:
    """Find the best structural similarity between a promotion and cellar wines.

    Uses a light heuristic with four signals:
    - Region match: 0.4 weight
    - Grape match: 0.3 weight
    - Category match: 0.2 weight
    - Price proximity: 0.1 weight
    """
    best_score = 0.0
    best_wine: dict | None = None

    promo_region = _normalise(promo.appellation)
    promo_grape = _extract_grape_hint(promo.wine_name)
    promo_category = _infer_category(promo.color, promo.category)

    for wine in cellar_wines:
        score = 0.0

        # Region signal (0.4)
        wine_region = _normalise(wine.get("region") or "")
        if promo_region and wine_region and promo_region in wine_region:
            score += 0.4
        elif promo_region and wine_region:
            wine_country = _normalise(wine.get("country") or "")
            if promo_region in wine_country:
                score += 0.2

        # Grape signal (0.3)
        wine_grape = _normalise(wine.get("primary_grape") or "")
        if promo_grape and wine_grape and promo_grape in wine_grape:
            score += 0.3

        # Category signal (0.2)
        wine_cat = _normalise(wine.get("category") or "")
        if promo_category and wine_cat and promo_category in wine_cat:
            score += 0.2

        # Price proximity signal (0.1)
        wine_price = wine.get("price")
        if wine_price and promo.sale_price:
            try:
                ratio = float(promo.sale_price) / float(wine_price)
                if 0.5 <= ratio <= 2.0:
                    score += 0.1 * (1 - abs(1 - ratio))
            except (ZeroDivisionError, TypeError):
                pass

        if score > best_score:
            best_score = score
            best_wine = wine

    if best_wine is None or best_score < _SIMILAR_THRESHOLD:
        return None

    return PromotionMatch(
        promotion=promo,
        match_type="similar",
        match_category="similar",
        confidence=round(best_score, 3),
        similar_to_wine_id=best_wine["wine_id"],
        similarity_score=round(best_score, 3),
        wine_name=best_wine["wine_name"],
        value_score=round(0.5 * best_score, 3),
    )


def _find_gap_match(
    promo: ExtractedPromotion,
    gaps: list[dict],
) -> PromotionMatch | None:
    """Check if a promotion fills an identified cellar gap."""
    if not gaps:
        return None

    promo_region = _normalise(promo.appellation)
    promo_grape = _extract_grape_hint(promo.wine_name)
    promo_category = _infer_category(promo.color, promo.category)

    for gap in gaps:
        dim = gap["dimension"]
        value = _normalise(gap["value"])

        if dim == "region" and promo_region and value in promo_region:
            return _make_gap_match(promo, gap)
        if dim == "grape" and promo_grape and value in promo_grape:
            return _make_gap_match(promo, gap)
        if dim == "price_tier" and promo_category and value in promo_category:
            return _make_gap_match(promo, gap)

    return None


def _make_gap_match(promo: ExtractedPromotion, gap: dict) -> PromotionMatch:
    """Create a gap-fill PromotionMatch."""
    return PromotionMatch(
        promotion=promo,
        match_type="gap_fill",
        match_category="gap_fill",
        confidence=0.5,
        value_score=0.6,
        gap_dimension=gap["dimension"],
        gap_detail=f"Only {gap['bottles']} bottle(s) of {gap['value']} in cellar",
        cellar_bottles_in_category=gap["bottles"],
    )


# ---------------------------------------------------------------------------
# Helpers for enhanced matching
# ---------------------------------------------------------------------------


def _extract_grape_hint(wine_name: str) -> str:
    """Extract grape variety hint from a wine name if present.

    Examples:
        >>> _extract_grape_hint("Chenin Blanc Rooidraai")
        'chenin blanc'
        >>> _extract_grape_hint("Grand Vin 2018")
        ''
    """
    known_grapes = [
        "cabernet sauvignon",
        "merlot",
        "pinot noir",
        "syrah",
        "shiraz",
        "chardonnay",
        "sauvignon blanc",
        "chenin blanc",
        "riesling",
        "tempranillo",
        "sangiovese",
        "nebbiolo",
        "grenache",
        "mourvèdre",
        "malbec",
        "zinfandel",
        "gamay",
        "barbera",
        "primitivo",
        "viognier",
        "gewürztraminer",
        "pinot grigio",
        "pinot gris",
        "grüner veltliner",
        "albariño",
        "verdejo",
        "garnacha",
    ]
    name_lower = wine_name.lower()
    for grape in known_grapes:
        if grape in name_lower:
            return grape
    return ""


def _infer_category(color: str, category: str) -> str:
    """Infer wine category from color or explicit category field.

    Examples:
        >>> _infer_category("red", "")
        'red'
        >>> _infer_category("", "Red wine")
        'red'
    """
    if category:
        return category.lower().replace(" wine", "").strip()
    if color:
        return color.lower().strip()
    return ""
