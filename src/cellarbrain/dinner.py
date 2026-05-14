"""Dinner Party Concierge — multi-course flight planning.

Plans a complete wine flight for a dinner party by:
1. Classifying each course via rule-based dish analysis
2. Retrieving pairing candidates per course (RAG + optional embedding)
3. Selecting one wine per course with progression, diversity, and budget constraints
4. Computing serving guidance (temperatures, decanting, timeline)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import duckdb

from .hybrid_pairing import HybridPairingEngine
from .pairing import PairingCandidate, classify_dish

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Wine weight base by category (1–10 scale, lower = lighter/earlier in flight)
_CATEGORY_BASE_WEIGHT: dict[str, float] = {
    "Sparkling wine": 2.0,
    "White wine": 4.0,
    "Rosé": 4.5,
    "Red wine": 7.0,
    "Sweet wine": 9.0,
}

# Tannic / heavier grapes get a positive adjustment
_GRAPE_WEIGHT_ADJUSTMENT: dict[str, float] = {
    # Heavy reds
    "Cabernet Sauvignon": 1.0,
    "Nebbiolo": 1.0,
    "Mourvèdre": 0.8,
    "Petit Verdot": 0.8,
    "Tannat": 1.0,
    "Aglianico": 0.8,
    "Sagrantino": 1.0,
    "Lagrein": 0.5,
    "Syrah": 0.5,
    "Malbec": 0.5,
    "Corvina": 0.3,
    # Medium reds (no adjustment)
    "Merlot": 0.0,
    "Tempranillo": 0.0,
    "Sangiovese": 0.0,
    "Grenache": -0.2,
    "Barbera": -0.3,
    # Light reds
    "Pinot Noir": -0.5,
    "Gamay": -0.8,
    "Schiava": -1.0,
    # Whites — mostly no adjustment; oak/body indicated by grape
    "Chardonnay": 0.3,
    "Viognier": 0.3,
    "Chenin Blanc": 0.0,
    "Sauvignon Blanc": -0.3,
    "Riesling": -0.3,
    "Chasselas": -0.5,
    "Arneis": -0.5,
    "Pinot Gris": 0.0,
}

# Default serving temperatures by category (°C)
_DEFAULT_SERVING_TEMP: dict[str, int] = {
    "Sparkling wine": 7,
    "White wine": 10,
    "Rosé": 10,
    "Red wine": 17,
    "Sweet wine": 10,
}

# Default decanting minutes by category
_DEFAULT_DECANT_MINUTES: dict[str, int] = {
    "Red wine": 30,
}

# Budget tier max CHF values
BUDGET_TIERS: dict[str, float | None] = {
    "any": None,
    "under_50": 50.0,
    "under_100": 100.0,
    "under_150": 150.0,
    "under_200": 200.0,
    "under_300": 300.0,
}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CourseWinePick:
    """A single course's wine selection."""

    course_number: int
    course_description: str
    wine_id: int
    wine_name: str
    vintage: int | None
    winery_name: str
    category: str
    country: str | None
    region: str | None
    primary_grape: str | None
    price: float | None
    bottles_needed: int
    bottles_available: int
    serving_temp_c: int
    decant_minutes: int
    pairing_reason: str
    wine_weight: float


@dataclass(frozen=True)
class TimelineEntry:
    """A preparation action with relative timing."""

    minutes_before_dinner: int
    action: str
    wine_name: str


@dataclass(frozen=True)
class FlightPlan:
    """Complete dinner party wine plan."""

    courses: list[CourseWinePick]
    total_bottles: int
    total_cost: float | None
    guests: int
    style: str
    timeline: list[TimelineEntry]
    warnings: list[str]


# ---------------------------------------------------------------------------
# Wine weight model
# ---------------------------------------------------------------------------


def compute_wine_weight(
    category: str,
    primary_grape: str | None = None,
    sweetness: str | None = None,
    effervescence: str | None = None,
) -> float:
    """Compute a wine's weight on a 1–10 scale for progression ordering.

    Higher values = heavier/richer/sweeter wines that should come later
    in a flight.

    Examples:
        >>> compute_wine_weight("Sparkling wine", "Chardonnay")
        2.3
        >>> compute_wine_weight("Red wine", "Cabernet Sauvignon")
        8.0
        >>> compute_wine_weight("Sweet wine")
        9.0
    """
    base = _CATEGORY_BASE_WEIGHT.get(category, 5.0)

    # Grape adjustment
    if primary_grape:
        base += _GRAPE_WEIGHT_ADJUSTMENT.get(primary_grape, 0.0)

    # Sweetness pushes later
    if sweetness and sweetness.lower() in ("sweet", "off-dry", "moelleux", "doux"):
        base += 1.0

    # Sparkling / effervescence pulls earlier
    if effervescence and effervescence.lower() in ("sparkling", "pétillant"):
        base -= 1.5

    return max(1.0, min(10.0, base))


# ---------------------------------------------------------------------------
# Quantity calculation
# ---------------------------------------------------------------------------


def bottles_needed(guests: int, volume_ml: int = 750, glasses_per_bottle: int = 5) -> int:
    """Calculate bottles needed for one course (one glass per guest).

    Examples:
        >>> bottles_needed(4)
        1
        >>> bottles_needed(6)
        2
        >>> bottles_needed(10)
        2
        >>> bottles_needed(4, volume_ml=1500)
        1
    """
    if volume_ml >= 1500:
        glasses_per_bottle = glasses_per_bottle * 2
    return max(1, math.ceil(guests / glasses_per_bottle))


# ---------------------------------------------------------------------------
# Serving guidance
# ---------------------------------------------------------------------------


def _serving_temp(category: str, stored_temp: int | None) -> int:
    """Determine serving temperature (°C)."""
    if stored_temp and stored_temp > 0:
        return stored_temp
    return _DEFAULT_SERVING_TEMP.get(category, 14)


def _decant_minutes(category: str, stored_minutes: int | None) -> int:
    """Determine decanting time (minutes)."""
    if stored_minutes and stored_minutes > 0:
        return stored_minutes
    return _DEFAULT_DECANT_MINUTES.get(category, 0)


def _chill_minutes(target_temp: int, cellar_temp: int = 14) -> int:
    """Estimate fridge time to reach target temperature from cellar temp.

    Rough approximation: ~15 minutes per °C to drop in a standard fridge.

    Examples:
        >>> _chill_minutes(7, 14)
        105
        >>> _chill_minutes(17, 14)
        0
    """
    if target_temp >= cellar_temp:
        return 0
    delta = cellar_temp - target_temp
    return delta * 15


def build_timeline(
    picks: list[CourseWinePick],
    dinner_time_minutes: int | None = None,
) -> list[TimelineEntry]:
    """Build preparation timeline with relative minutes-before-dinner.

    Args:
        picks: Selected wines per course.
        dinner_time_minutes: Minutes from midnight (e.g. 19:30 = 1170).
            If None, timeline entries use relative offsets only.
    """
    entries: list[TimelineEntry] = []

    for pick in picks:
        # Chilling
        chill = _chill_minutes(pick.serving_temp_c)
        if chill > 0:
            entries.append(
                TimelineEntry(
                    minutes_before_dinner=chill + 15,  # buffer
                    action=f"Put in fridge (target {pick.serving_temp_c}°C)",
                    wine_name=pick.wine_name,
                )
            )

        # Decanting
        if pick.decant_minutes > 0:
            entries.append(
                TimelineEntry(
                    minutes_before_dinner=pick.decant_minutes + 10,  # buffer
                    action=f"Decant ({pick.decant_minutes} min before serving)",
                    wine_name=pick.wine_name,
                )
            )

    # Sort by time (longest before dinner first)
    entries.sort(key=lambda e: e.minutes_before_dinner, reverse=True)
    return entries


# ---------------------------------------------------------------------------
# Flight selection
# ---------------------------------------------------------------------------


def _best_reason(candidate: PairingCandidate) -> str:
    """Extract the best pairing reason from candidate signals."""
    for sig in candidate.match_signals:
        if sig.startswith("food_tag:"):
            tag = sig.split(":", 1)[1]
            return f"Specifically paired with {tag.replace('-', ' ')}"
    for sig in candidate.match_signals:
        if sig.startswith("food_group:"):
            group = sig.split(":", 1)[1]
            return f"Proven match for {group.replace('_', ' ')} dishes"
    for sig in candidate.match_signals:
        if sig.startswith("grape:"):
            grape = sig.split(":", 1)[1]
            return f"{grape} is a classic pairing grape for this dish"
    if "region" in candidate.match_signals:
        return "Regional affinity — local wine for local food"
    if "category" in candidate.match_signals:
        return "Correct wine style for this dish"
    return "Matches the dish profile"


def plan_flight(
    con: duckdb.DuckDBPyConnection,
    hybrid_engine: HybridPairingEngine,
    courses: list[str],
    *,
    guests: int = 4,
    budget: str = "any",
    style: str = "classic",
    dinner_time_minutes: int | None = None,
    pool_size: int = 20,
    glasses_per_bottle: int = 5,
) -> FlightPlan:
    """Plan a complete wine flight for a multi-course dinner.

    Args:
        con: DuckDB connection with wines_full view.
        hybrid_engine: Pairing retrieval engine.
        courses: List of course descriptions in serving order.
        guests: Number of guests.
        budget: Budget tier (any, under_50, under_100, etc.).
        style: Pairing style (classic only in v1).
        dinner_time_minutes: Minutes from midnight for timeline.
        pool_size: Candidates to retrieve per course.
        glasses_per_bottle: Glasses per standard 750ml bottle.

    Returns:
        FlightPlan with selected wines, timeline, and warnings.
    """
    if not courses:
        return FlightPlan(
            courses=[],
            total_bottles=0,
            total_cost=None,
            guests=guests,
            style=style,
            timeline=[],
            warnings=["No courses provided."],
        )

    budget_max = BUDGET_TIERS.get(budget)
    warnings: list[str] = []

    # 1. Classify each course
    classifications = [classify_dish(course) for course in courses]

    # 2. Retrieve candidate pools per course
    pools: list[list[PairingCandidate]] = []
    for cls in classifications:
        try:
            outcome = hybrid_engine.retrieve(
                con,
                dish_description=None,
                category=cls.category,
                weight=cls.weight,
                protein=cls.protein,
                cuisine=cls.cuisine,
                limit=pool_size,
            )
            pools.append(outcome.candidates)
        except Exception:
            logger.debug("Retrieval failed for course", exc_info=True)
            pools.append([])

    # 3. Fetch extended metadata for all candidate wine_ids
    all_wine_ids: set[int] = set()
    for pool in pools:
        for c in pool:
            all_wine_ids.add(c.wine_id)

    metadata = _fetch_extended_metadata(con, all_wine_ids)

    # 4. Compute wine weights for all candidates
    wine_weights: dict[int, float] = {}
    for wid, meta in metadata.items():
        wine_weights[wid] = compute_wine_weight(
            meta["category"],
            meta["primary_grape"],
            meta.get("sweetness"),
            meta.get("effervescence"),
        )

    # 5. Determine expected course progression weights
    # Courses are given in serving order; assign expected weight range
    # proportionally across the light→heavy spectrum

    # 6. Greedy selection — pick best wine per course, avoiding duplicates
    used_wine_ids: set[int] = set()
    used_wineries: set[str] = set()
    picks: list[CourseWinePick] = []
    running_cost = 0.0

    for i, (course_desc, pool, _cls) in enumerate(zip(courses, pools, classifications)):
        best_pick: CourseWinePick | None = None
        best_score = -999.0

        for candidate in pool:
            wid = candidate.wine_id
            if wid in used_wine_ids:
                continue

            meta = metadata.get(wid)
            if meta is None:
                continue

            winery = meta.get("winery_name", "")

            # Prefer avoiding duplicate wineries (soft constraint)
            winery_penalty = -5.0 if winery and winery in used_wineries else 0.0

            # Budget check per-bottle
            price = candidate.price
            btls = bottles_needed(guests, meta.get("volume_ml", 750), glasses_per_bottle)
            if budget_max is not None and price is not None:
                course_cost = price * btls
                if running_cost + course_cost > budget_max:
                    # Skip wines that bust the budget
                    continue

            # Score: signal_count + quality bonus + winery diversity
            score = float(candidate.signal_count) * 3.0
            if candidate.best_pro_score:
                score += min((candidate.best_pro_score - 85) / 5.0, 3.0)
            score += winery_penalty

            # Prefer wines with sufficient stock
            if candidate.bottles_stored < btls:
                score -= 3.0

            if score > best_score:
                best_score = score
                serving_temp = _serving_temp(candidate.category, meta.get("serving_temp_c"))
                decant = _decant_minutes(candidate.category, meta.get("opening_minutes"))

                best_pick = CourseWinePick(
                    course_number=i + 1,
                    course_description=course_desc,
                    wine_id=wid,
                    wine_name=candidate.wine_name,
                    vintage=candidate.vintage,
                    winery_name=winery,
                    category=candidate.category,
                    country=candidate.country,
                    region=candidate.region,
                    primary_grape=candidate.primary_grape,
                    price=price,
                    bottles_needed=btls,
                    bottles_available=candidate.bottles_stored,
                    serving_temp_c=serving_temp,
                    decant_minutes=decant,
                    pairing_reason=_best_reason(candidate),
                    wine_weight=wine_weights.get(wid, 5.0),
                )

        if best_pick is not None:
            picks.append(best_pick)
            used_wine_ids.add(best_pick.wine_id)
            if best_pick.winery_name:
                used_wineries.add(best_pick.winery_name)
            if best_pick.price is not None:
                running_cost += best_pick.price * best_pick.bottles_needed

            # Warnings
            if best_pick.bottles_available < best_pick.bottles_needed:
                warnings.append(
                    f"Course {best_pick.course_number}: only {best_pick.bottles_available} "
                    f"bottle(s) of {best_pick.wine_name} available, need {best_pick.bottles_needed}."
                )
        else:
            warnings.append(f'Course {i + 1}: no suitable wine found for "{course_desc}".')

    # 7. Reorder picks by wine weight for progression
    picks.sort(key=lambda p: p.wine_weight)
    # Reassign course numbers to match progression order
    reordered: list[CourseWinePick] = []
    for idx, pick in enumerate(picks):
        reordered.append(
            CourseWinePick(
                course_number=idx + 1,
                course_description=pick.course_description,
                wine_id=pick.wine_id,
                wine_name=pick.wine_name,
                vintage=pick.vintage,
                winery_name=pick.winery_name,
                category=pick.category,
                country=pick.country,
                region=pick.region,
                primary_grape=pick.primary_grape,
                price=pick.price,
                bottles_needed=pick.bottles_needed,
                bottles_available=pick.bottles_available,
                serving_temp_c=pick.serving_temp_c,
                decant_minutes=pick.decant_minutes,
                pairing_reason=pick.pairing_reason,
                wine_weight=pick.wine_weight,
            )
        )

    # 8. Build timeline
    timeline = build_timeline(reordered, dinner_time_minutes)

    # 9. Compute totals
    total_bottles = sum(p.bottles_needed for p in reordered)
    total_cost = running_cost if running_cost > 0 else None

    return FlightPlan(
        courses=reordered,
        total_bottles=total_bottles,
        total_cost=total_cost,
        guests=guests,
        style=style,
        timeline=timeline,
        warnings=warnings,
    )


def _fetch_extended_metadata(
    con: duckdb.DuckDBPyConnection,
    wine_ids: set[int],
) -> dict[int, dict]:
    """Fetch extended wine metadata needed for flight planning."""
    if not wine_ids:
        return {}

    placeholders = ", ".join("?" for _ in wine_ids)
    sql = f"""
        SELECT wine_id, winery_name, category, primary_grape,
               sweetness, effervescence, serving_temp_c, opening_minutes,
               volume_ml, price, bottles_stored
        FROM wines_full
        WHERE wine_id IN ({placeholders})
    """
    try:
        rows = con.execute(sql, sorted(wine_ids)).fetchall()
    except Exception:
        logger.warning("Failed to fetch extended metadata", exc_info=True)
        return {}

    result: dict[int, dict] = {}
    for row in rows:
        result[row[0]] = {
            "winery_name": row[1] or "",
            "category": row[2] or "",
            "primary_grape": row[3],
            "sweetness": row[4],
            "effervescence": row[5],
            "serving_temp_c": row[6],
            "opening_minutes": row[7],
            "volume_ml": row[8] or 750,
            "price": row[9],
            "bottles_stored": row[10] or 0,
        }
    return result


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------


def format_flight_plan(plan: FlightPlan) -> str:
    """Format a FlightPlan as a complete Markdown document."""
    if not plan.courses:
        if plan.warnings:
            return "No flight plan could be generated.\n\n" + "\n".join(f"- {w}" for w in plan.warnings)
        return "No flight plan could be generated."

    lines: list[str] = []
    lines.append("# 🍷 Dinner Party Flight Plan")
    lines.append("")

    # Header
    parts = [f"**Menu:** {len(plan.courses)} courses"]
    parts.append(f"**Guests:** {plan.guests}")
    parts.append(f"**Style:** {plan.style.capitalize()}")
    if plan.total_cost is not None:
        parts.append(f"**Est. cost:** ~CHF {plan.total_cost:.0f}")
    lines.append(" | ".join(parts))
    lines.append("")
    lines.append("---")
    lines.append("")

    # Flight progression table
    lines.append("## Flight Progression")
    lines.append("")
    lines.append("| # | Course | Wine | Vintage | Why | Bottles |")
    lines.append("|--:|--------|------|---------|-----|--------:|")

    for pick in plan.courses:
        vintage_str = str(pick.vintage) if pick.vintage else "NV"
        lines.append(
            f"| {pick.course_number} | {pick.course_description} "
            f"| {pick.wine_name} | {vintage_str} "
            f"| {pick.pairing_reason} | {pick.bottles_needed} |"
        )

    lines.append("")
    lines.append(f"**Total:** {plan.total_bottles} bottle(s)")
    if plan.total_cost is not None:
        lines.append(f" | ~CHF {plan.total_cost:.0f}")
    lines.append("")

    # Timeline
    if plan.timeline:
        lines.append("---")
        lines.append("")
        lines.append("## Preparation Timeline")
        lines.append("")
        lines.append("| Minutes before dinner | Action | Wine |")
        lines.append("|----------------------:|--------|------|")
        for entry in plan.timeline:
            lines.append(f"| {entry.minutes_before_dinner} min | {entry.action} | {entry.wine_name} |")
        lines.append("")

    # Tasting card
    lines.append("---")
    lines.append("")
    lines.append("## Tasting Card")
    lines.append("")
    for pick in plan.courses:
        location = ", ".join(filter(None, [pick.region, pick.country]))
        lines.append(f"### Course {pick.course_number}: {pick.course_description}")
        lines.append(f"**{pick.wine_name} {pick.vintage or 'NV'}** — {location}")
        details = [pick.primary_grape or "blend"]
        details.append(f"Serve at {pick.serving_temp_c}°C")
        if pick.decant_minutes > 0:
            details.append(f"Decant {pick.decant_minutes} min")
        lines.append(" | ".join(details))
        lines.append("")

    # Warnings
    if plan.warnings:
        lines.append("---")
        lines.append("")
        lines.append("## ⚠️ Notes")
        lines.append("")
        for w in plan.warnings:
            lines.append(f"- {w}")
        lines.append("")

    return "\n".join(lines)
