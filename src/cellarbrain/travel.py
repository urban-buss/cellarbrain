"""Wine travel planner — region discovery and travel brief generation.

Combines cellar inventory, dossier producer profiles, and gap analysis
into a structured "travel wine brief" for a destination wine region.
"""

from __future__ import annotations

import logging
import pathlib
from dataclasses import dataclass

import duckdb

from .dossier_ops import WineNotFoundError, read_agent_section_content

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProducerVisit:
    """Producer info extracted from a dossier's producer_profile section."""

    winery_name: str
    subregion: str | None
    wine_count: int
    profile_excerpt: str


@dataclass(frozen=True)
class RegionGap:
    """Identified gap in region coverage."""

    dimension: str  # "subregion" or "grape"
    value: str


@dataclass(frozen=True)
class TravelBrief:
    """Complete travel brief for a wine region."""

    destination: str
    match_level: str  # "country", "region", or "subregion"
    wines: list[dict]
    total_bottles: int
    total_value: float
    winery_count: int
    subregion_count: int
    producers: list[ProducerVisit]
    gaps: list[RegionGap]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Columns to fetch from wines_full for the inventory table.
_INVENTORY_COLUMNS = (
    "wine_id",
    "winery_name",
    "wine_name",
    "vintage",
    "country",
    "region",
    "subregion",
    "classification",
    "category",
    "primary_grape",
    "bottles_stored",
    "bottles_consumed",
    "drinking_status",
    "best_pro_score",
    "cellar_value",
)

_DESTINATION_FILTER = """
    (strip_accents(region) ILIKE strip_accents($1)
     OR strip_accents(country) ILIKE strip_accents($1)
     OR strip_accents(subregion) ILIKE strip_accents($1))
"""


def _match_destination(
    con: duckdb.DuckDBPyConnection,
    destination: str,
) -> str | None:
    """Determine the geographic level that *destination* matches.

    Returns ``"region"``, ``"country"``, or ``"subregion"`` — whichever
    matches first in priority order.  Returns ``None`` when no wines match.
    """
    row = con.execute(
        f"""
        SELECT
            CASE
                WHEN bool_or(strip_accents(region) ILIKE strip_accents($1)) THEN 'region'
                WHEN bool_or(strip_accents(country) ILIKE strip_accents($1)) THEN 'country'
                WHEN bool_or(strip_accents(subregion) ILIKE strip_accents($1)) THEN 'subregion'
            END AS match_level
        FROM wines_full
        WHERE bottles_stored > 0
          AND {_DESTINATION_FILTER}
        """,
        [destination],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0]


def _region_inventory(
    con: duckdb.DuckDBPyConnection,
    destination: str,
    limit: int = 20,
) -> list[dict]:
    """Fetch stored wines matching *destination* as a list of dicts."""
    cols = ", ".join(_INVENTORY_COLUMNS)
    sql = f"""
        SELECT {cols}
        FROM wines_full
        WHERE bottles_stored > 0
          AND {_DESTINATION_FILTER}
        ORDER BY best_pro_score DESC NULLS LAST, bottles_stored DESC
        LIMIT $2
    """
    rows = con.execute(sql, [destination, limit]).fetchall()
    desc = con.description
    col_names = [d[0] for d in desc] if desc else list(_INVENTORY_COLUMNS)
    return [dict(zip(col_names, row)) for row in rows]


def _extract_producer_visits(
    wines: list[dict],
    data_dir: str | pathlib.Path,
    max_producers: int = 5,
) -> list[ProducerVisit]:
    """Read producer_profile excerpts for top wineries.

    Groups wines by winery, sorted by total bottles descending, and reads
    the producer_profile agent section from the first wine's dossier per
    winery.  Skips wineries whose dossier has no profile content.
    """
    # Group wine_ids and subregions by winery name
    winery_info: dict[str, dict] = {}
    for w in wines:
        name = w.get("winery_name") or ""
        if not name:
            continue
        info = winery_info.setdefault(name, {"wine_ids": [], "subregion": None, "bottles": 0})
        info["wine_ids"].append(w["wine_id"])
        if info["subregion"] is None and w.get("subregion"):
            info["subregion"] = w["subregion"]
        info["bottles"] += w.get("bottles_stored", 0) or 0

    # Sort by bottle count descending
    sorted_wineries = sorted(winery_info.items(), key=lambda x: x[1]["bottles"], reverse=True)

    visits: list[ProducerVisit] = []
    for winery_name, info in sorted_wineries[:max_producers]:
        for wine_id in info["wine_ids"]:
            try:
                profile = read_agent_section_content(wine_id, "producer_profile", data_dir)
            except (WineNotFoundError, Exception):
                profile = ""
            if profile:
                visits.append(
                    ProducerVisit(
                        winery_name=winery_name,
                        subregion=info["subregion"],
                        wine_count=len(info["wine_ids"]),
                        profile_excerpt=profile[:300],
                    )
                )
                break  # Got profile for this winery, move to next

    return visits


def _region_gaps(
    con: duckdb.DuckDBPyConnection,
    destination: str,
    match_level: str,
) -> list[RegionGap]:
    """Identify gaps by comparing all-time holdings against currently stored wines.

    Uses the full wine history (including consumed bottles) as a proxy for
    what the user considers relevant in this region.
    """
    gaps: list[RegionGap] = []

    if match_level == "subregion":
        # At subregion level, gap analysis isn't meaningful
        return gaps

    # Build the geographic filter for the matched level
    level_col = "region" if match_level == "region" else "country"
    filter_clause = f"strip_accents({level_col}) ILIKE strip_accents($1)"

    # Subregion gaps: all-time subregions vs. currently stored
    all_subregions = con.execute(
        f"""
        SELECT DISTINCT subregion
        FROM wines_full
        WHERE {filter_clause}
          AND subregion IS NOT NULL
        """,
        [destination],
    ).fetchall()

    stored_subregions = con.execute(
        f"""
        SELECT DISTINCT subregion
        FROM wines_full
        WHERE {filter_clause}
          AND subregion IS NOT NULL
          AND bottles_stored > 0
        """,
        [destination],
    ).fetchall()

    all_set = {r[0] for r in all_subregions}
    stored_set = {r[0] for r in stored_subregions}
    for sub in sorted(all_set - stored_set):
        gaps.append(RegionGap(dimension="subregion", value=sub))

    # Grape gaps: all-time grapes vs. currently stored
    all_grapes = con.execute(
        f"""
        SELECT DISTINCT primary_grape
        FROM wines_full
        WHERE {filter_clause}
          AND primary_grape IS NOT NULL
        """,
        [destination],
    ).fetchall()

    stored_grapes = con.execute(
        f"""
        SELECT DISTINCT primary_grape
        FROM wines_full
        WHERE {filter_clause}
          AND primary_grape IS NOT NULL
          AND bottles_stored > 0
        """,
        [destination],
    ).fetchall()

    all_grape_set = {r[0] for r in all_grapes}
    stored_grape_set = {r[0] for r in stored_grapes}
    for grape in sorted(all_grape_set - stored_grape_set):
        gaps.append(RegionGap(dimension="grape", value=grape))

    return gaps


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_travel_brief(
    con: duckdb.DuckDBPyConnection,
    destination: str,
    data_dir: str | pathlib.Path,
    *,
    include_producers: bool = True,
    include_gaps: bool = True,
    limit: int = 20,
    max_producers: int = 5,
) -> TravelBrief | None:
    """Build a complete travel brief for *destination*.

    Returns ``None`` when no stored wines match the destination.
    """
    match_level = _match_destination(con, destination)
    if match_level is None:
        return None

    wines = _region_inventory(con, destination, limit=limit)
    if not wines:
        return None

    total_bottles = sum(w.get("bottles_stored", 0) or 0 for w in wines)
    total_value = sum(w.get("cellar_value", 0) or 0 for w in wines)
    winery_names = {w.get("winery_name") for w in wines if w.get("winery_name")}
    subregions = {w.get("subregion") for w in wines if w.get("subregion")}

    producers: list[ProducerVisit] = []
    if include_producers:
        producers = _extract_producer_visits(wines, data_dir, max_producers=max_producers)

    gaps: list[RegionGap] = []
    if include_gaps:
        gaps = _region_gaps(con, destination, match_level)

    return TravelBrief(
        destination=destination,
        match_level=match_level,
        wines=wines,
        total_bottles=total_bottles,
        total_value=total_value,
        winery_count=len(winery_names),
        subregion_count=len(subregions),
        producers=producers,
        gaps=gaps,
    )


def format_travel_brief(brief: TravelBrief) -> str:
    """Format a TravelBrief as Markdown."""
    lines: list[str] = []
    wine_count = len(brief.wines)

    lines.append(f"## Wine Travel Brief: {brief.destination}")
    lines.append("")

    # --- Inventory table ---
    lines.append(
        f"### Your Cellar ({wine_count} wine{'s' if wine_count != 1 else ''}, "
        f"{brief.total_bottles} bottle{'s' if brief.total_bottles != 1 else ''})"
    )
    lines.append("")
    lines.append("| Wine | Vintage | Winery | Subregion | Score | Bottles | Status |")
    lines.append("|------|---------|--------|-----------|-------|---------|--------|")

    for w in brief.wines:
        name = w.get("wine_name") or "—"
        vintage = w.get("vintage") or "—"
        winery = w.get("winery_name") or "—"
        subregion = w.get("subregion") or "—"
        score = w.get("best_pro_score")
        score_str = f"{score:.0f}" if score else "—"
        bottles = w.get("bottles_stored", 0)
        status = (w.get("drinking_status") or "unknown").replace("_", " ").title()
        lines.append(f"| {name} | {vintage} | {winery} | {subregion} | {score_str} | {bottles} | {status} |")

    lines.append("")
    value_str = f"CHF {brief.total_value:,.0f}" if brief.total_value else "—"
    lines.append(
        f"**Summary:** {brief.winery_count} winer{'ies' if brief.winery_count != 1 else 'y'}, "
        f"{brief.subregion_count} subregion{'s' if brief.subregion_count != 1 else ''}, "
        f"{value_str} cellar value"
    )
    lines.append("")

    # --- Favorites & Highlights ---
    lines.append("### Favorites & Highlights")
    lines.append("")

    scored = [w for w in brief.wines if w.get("best_pro_score")]
    if scored:
        top = max(scored, key=lambda w: w["best_pro_score"])
        lines.append(
            f"- Highest-rated: {top.get('wine_name', '—')} {top.get('vintage', '')} ({top['best_pro_score']:.0f} pts)"
        )

    consumed = [w for w in brief.wines if (w.get("bottles_consumed") or 0) > 0]
    if consumed:
        most = max(consumed, key=lambda w: w["bottles_consumed"])
        lines.append(
            f"- Most consumed: {most.get('wine_name', '—')} "
            f"({most['bottles_consumed']} bottle{'s' if most['bottles_consumed'] != 1 else ''} consumed)"
        )

    largest = max(brief.wines, key=lambda w: w.get("bottles_stored", 0) or 0)
    if (largest.get("bottles_stored") or 0) > 0:
        lines.append(
            f"- Largest holding: {largest.get('wine_name', '—')} "
            f"({largest['bottles_stored']} bottle{'s' if largest['bottles_stored'] != 1 else ''})"
        )

    lines.append("")

    # --- Producer Visit Suggestions ---
    if brief.producers:
        lines.append("### Producer Visit Suggestions")
        lines.append("")
        for p in brief.producers:
            loc = f" ({p.subregion})" if p.subregion else ""
            lines.append(f"**{p.winery_name}**{loc} — {p.wine_count} wine{'s' if p.wine_count != 1 else ''}")
            lines.append(f"> {p.profile_excerpt}")
            lines.append("")

    # --- Regional Gaps ---
    if brief.gaps:
        lines.append("### Regional Gaps")
        lines.append("")
        subregion_gaps = [g for g in brief.gaps if g.dimension == "subregion"]
        grape_gaps = [g for g in brief.gaps if g.dimension == "grape"]
        if subregion_gaps:
            vals = ", ".join(g.value for g in subregion_gaps)
            lines.append(f"- **Subregions not represented:** {vals}")
        if grape_gaps:
            vals = ", ".join(g.value for g in grape_gaps)
            lines.append(f"- **Grapes not represented:** {vals}")
        lines.append("")

    return "\n".join(lines)
