"""Wine of the Day — deterministic daily rotation pick.

Selects one wine from the cellar each day using a date-seeded random
selection weighted by drinking-window urgency. The same wine is returned
all day; the pick changes at midnight.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from datetime import date

import duckdb

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WineOfTheDay:
    """Today's deterministic wine pick with reasoning."""

    wine_id: int
    wine_name: str
    vintage: int | None
    winery_name: str
    category: str
    region: str | None
    primary_grape: str | None
    price: float | None
    drinking_status: str | None
    bottles_stored: int
    score: float
    reason: str


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def _date_seed(today: date) -> int:
    """Convert a date to a deterministic integer seed.

    Examples:
        >>> from datetime import date
        >>> _date_seed(date(2026, 5, 11))
        20260511
    """
    return int(today.strftime("%Y%m%d"))


def pick_wine_of_the_day(
    con: duckdb.DuckDBPyConnection,
    *,
    today: date | None = None,
    data_dir: str | None = None,
) -> WineOfTheDay | None:
    """Select today's wine pick using urgency-weighted random selection.

    Uses the recommendation engine to score candidates, then picks one
    from the top-10 using a date-seeded RNG weighted by urgency score.
    This ensures the same wine is returned all day while still rotating
    through different wines on different days.

    Args:
        con: DuckDB connection with wines_full view available.
        today: Override for current date (useful in tests).
        data_dir: Data directory for recommendation engine access.

    Returns:
        WineOfTheDay or None if the cellar is empty.
    """
    from .recommend import RecommendParams, recommend

    if today is None:
        today = date.today()

    params = RecommendParams(limit=10)
    candidates = recommend(con, params, today=today, data_dir=data_dir)

    if not candidates:
        return None

    # Build urgency-based weights (minimum 1.0 to ensure all have a chance)
    weights = [max(1.0, rec.urgency_score * 2.0) for rec in candidates]

    # Date-seeded deterministic selection
    rng = random.Random(_date_seed(today))
    chosen = rng.choices(candidates, weights=weights, k=1)[0]

    reason = _compose_reason(chosen, today)

    return WineOfTheDay(
        wine_id=chosen.wine_id,
        wine_name=chosen.wine_name,
        vintage=chosen.vintage,
        winery_name=chosen.winery_name,
        category=chosen.category,
        region=chosen.region,
        primary_grape=chosen.primary_grape,
        price=chosen.price,
        drinking_status=chosen.drinking_status,
        bottles_stored=chosen.bottles_stored,
        score=chosen.total_score,
        reason=reason,
    )


def _compose_reason(rec, today: date) -> str:
    """Build a concise reason string for why this wine was picked today.

    Focuses on urgency signals and grape/region freshness narrative.
    """

    parts: list[str] = []

    # Urgency-based reasons
    if rec.drinking_status == "past_optimal":
        parts.append("Past optimal — drink soon before it fades further.")
    elif rec.drinking_status == "optimal" and rec.urgency_score >= 7.0:
        parts.append("Nearing the end of its optimal window.")
    elif rec.drinking_status == "optimal":
        parts.append("At peak drinking right now.")
    elif rec.drinking_status == "drinkable":
        parts.append("Ready and waiting to be enjoyed.")

    # Grape/region diversity narrative
    if rec.primary_grape:
        parts.append(f"A chance to enjoy {rec.primary_grape}.")
    elif rec.region:
        parts.append(f"Explore your {rec.region} collection.")

    # Quality signal
    if rec.is_favorite:
        parts.append("One of your favourites.")

    return " ".join(parts) if parts else "A great pick from your cellar."


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def format_wine_of_the_day(pick: WineOfTheDay | None) -> str:
    """Format the daily pick as a Markdown card.

    Examples:
        >>> format_wine_of_the_day(None)
        'No wines available for today\\'s pick.'
    """
    if pick is None:
        return "No wines available for today's pick."

    vintage_str = str(pick.vintage) if pick.vintage else "NV"
    price_str = f"CHF {pick.price:.2f}" if pick.price else "—"
    status_str = (pick.drinking_status or "unknown").replace("_", " ").title()

    lines = [
        "# 🍷 Wine of the Day",
        "",
        f"**{pick.winery_name} {pick.wine_name}** {vintage_str}",
        "",
        "| Detail | Value |",
        "|:-------|:------|",
        f"| Category | {pick.category or '—'} |",
        f"| Region | {pick.region or '—'} |",
        f"| Grape | {pick.primary_grape or '—'} |",
        f"| Price | {price_str} |",
        f"| Status | {status_str} |",
        f"| Stock | {pick.bottles_stored} bottle{'s' if pick.bottles_stored != 1 else ''} |",
        "",
        f"_{pick.reason}_",
    ]

    return "\n".join(lines)
