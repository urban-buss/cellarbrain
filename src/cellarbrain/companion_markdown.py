"""Companion dossier generation for tracked wines.

Each tracked wine (a cross-vintage identity: winery + wine name) gets a
companion dossier containing agent-owned research sections.  Unlike the
per-wine dossiers, companion dossiers have no ETL-owned sections — all
content is managed by agents and preserved across ETL runs.
"""

from __future__ import annotations

import logging
import pathlib
import re
from dataclasses import dataclass

from .markdown import _extract_frontmatter_agent_fields
from .settings import Settings
from .slugify import companion_slug

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Agent section preservation (mirrors markdown.py pattern)
# ---------------------------------------------------------------------------

_AGENT_SECTION_RE = re.compile(
    r"(^## (?P<heading>[^\n]+)\n)"
    r"(?P<block><!-- source: agent:[^\n]+ -->\n.*?<!-- source: agent:[^\n]+ — end -->\n)",
    re.MULTILINE | re.DOTALL,
)


def _extract_agent_sections(content: str) -> dict[str, str]:
    """Extract heading → agent block from existing companion dossier content."""
    result: dict[str, str] = {}
    for m in _AGENT_SECTION_RE.finditer(content):
        heading = m.group("heading").strip()
        result[heading] = m.group("block")
    return result


# ---------------------------------------------------------------------------
# Vintage comparison helpers
# ---------------------------------------------------------------------------


@dataclass
class VintageStats:
    """Per-vintage statistics for the comparison matrix."""

    wine_id: int
    vintage: int | None
    drinking_status: str | None
    best_score: float | None
    best_score_max: float | None
    best_score_source: str | None  # "pro" | "personal" | None
    list_price: float | None
    list_currency: str | None
    price_per_750ml: float | None
    bottles_stored: int


def _aggregate_vintage_stats(
    related_wines: list[dict],
    tastings_by_wine: dict[int, list[dict]],
    pro_ratings_by_wine: dict[int, list[dict]],
    bottles_by_wine: dict[int, int],
) -> list[VintageStats]:
    """Build per-vintage stats for the comparison matrix."""
    results: list[VintageStats] = []
    for w in sorted(related_wines, key=lambda x: x.get("vintage") or 0):
        wid = w["wine_id"]

        # Best pro rating score
        pro_best: float | None = None
        pro_max: float | None = None
        for pr in pro_ratings_by_wine.get(wid, []):
            score = pr.get("score")
            if score is not None and (pro_best is None or score > pro_best):
                pro_best = score
                pro_max = pr.get("max_score", 100.0)

        # Best personal tasting score
        tasting_best: float | None = None
        tasting_max: float | None = None
        for t in tastings_by_wine.get(wid, []):
            score = t.get("score")
            if score is not None and (tasting_best is None or score > tasting_best):
                tasting_best = score
                tasting_max = t.get("max_score", 20.0)

        # Pick best: prefer pro when available
        if pro_best is not None:
            best_score = pro_best
            best_score_max = pro_max
            best_source = "pro"
        elif tasting_best is not None:
            best_score = tasting_best
            best_score_max = tasting_max
            best_source = "personal"
        else:
            best_score = None
            best_score_max = None
            best_source = None

        # Price (from wine row)
        list_price = w.get("list_price")
        if list_price is not None:
            list_price = float(list_price)
        price_per_750ml = w.get("price_per_750ml")
        if price_per_750ml is not None:
            price_per_750ml = float(price_per_750ml)

        results.append(
            VintageStats(
                wine_id=wid,
                vintage=w.get("vintage"),
                drinking_status=w.get("drinking_status"),
                best_score=best_score,
                best_score_max=best_score_max,
                best_score_source=best_source,
                list_price=list_price,
                list_currency=w.get("list_currency"),
                price_per_750ml=price_per_750ml,
                bottles_stored=bottles_by_wine.get(wid, 0),
            )
        )
    return results


def _select_best_value(stats: list[VintageStats]) -> VintageStats | None:
    """Find the vintage with the best score/price ratio.

    Requires both a score and a price_per_750ml. Normalises score to 0–1
    range using best_score_max before comparing.
    """
    best: VintageStats | None = None
    best_ratio: float = 0.0
    for s in stats:
        if s.best_score is None or s.price_per_750ml is None or s.price_per_750ml <= 0:
            continue
        max_score = s.best_score_max or 100.0
        normalised = s.best_score / max_score
        ratio = normalised / s.price_per_750ml
        if ratio > best_ratio:
            best_ratio = ratio
            best = s
    return best


def _suggest_drink_order(stats: list[VintageStats]) -> tuple[list[int | None], list[int | None]]:
    """Suggest drinking order across vintages.

    Returns (order, past_window) where:
    - order: vintages to drink in suggested sequence
    - past_window: vintages that are past their window (separate warning)

    Strategy: optimal/past_optimal first (oldest first), then drinkable
    (oldest first), then too_young (newest first), unknown at the end.
    """
    buckets: dict[str, list[VintageStats]] = {
        "urgent": [],  # optimal, past_optimal
        "drinkable": [],
        "young": [],  # too_young
        "unknown": [],
        "past": [],  # past_window
    }
    for s in stats:
        status = s.drinking_status
        if status in ("optimal", "past_optimal"):
            buckets["urgent"].append(s)
        elif status == "drinkable":
            buckets["drinkable"].append(s)
        elif status == "too_young":
            buckets["young"].append(s)
        elif status == "past_window":
            buckets["past"].append(s)
        else:
            buckets["unknown"].append(s)

    # Sort each bucket
    buckets["urgent"].sort(key=lambda s: s.vintage or 0)
    buckets["drinkable"].sort(key=lambda s: s.vintage or 0)
    buckets["young"].sort(key=lambda s: s.vintage or 0, reverse=True)
    buckets["unknown"].sort(key=lambda s: s.vintage or 0)
    buckets["past"].sort(key=lambda s: s.vintage or 0)

    order = [s.vintage for bucket in ("urgent", "drinkable", "young", "unknown") for s in buckets[bucket]]
    past_window = [s.vintage for s in buckets["past"]]
    return order, past_window


def _vintage_trend(stats: list[VintageStats]) -> str | None:
    """Determine vintage quality trend from normalised scores.

    Returns "improving", "declining", or "stable" when ≥3 vintages have
    scores. Uses simple linear regression slope with threshold ±0.01
    normalised points per year.
    """
    points: list[tuple[int, float]] = []
    for s in stats:
        if s.best_score is not None and s.vintage is not None and s.vintage > 0:
            max_score = s.best_score_max or 100.0
            points.append((s.vintage, s.best_score / max_score))

    if len(points) < 3:
        return None

    n = len(points)
    sum_x = sum(p[0] for p in points)
    sum_y = sum(p[1] for p in points)
    sum_xy = sum(p[0] * p[1] for p in points)
    sum_x2 = sum(p[0] ** 2 for p in points)

    denom = n * sum_x2 - sum_x**2
    if denom == 0:
        return "stable"

    slope = (n * sum_xy - sum_x * sum_y) / denom

    if slope > 0.01:
        return "improving"
    if slope < -0.01:
        return "declining"
    return "stable"


# ---------------------------------------------------------------------------
# Slug / filename helpers
# ---------------------------------------------------------------------------

# Backward-compat alias for any code importing the old private name.
_make_slug = companion_slug


def companion_dossier_slug(
    tracked_wine_id: int,
    winery_name: str | None,
    wine_name: str | None,
    slug_max_length: int = 60,
) -> str:
    """Return the Markdown filename for a tracked wine companion dossier."""
    slug = companion_slug(winery_name, wine_name, slug_max_length)
    return f"{tracked_wine_id:05d}-{slug}.md"


def _find_existing_companion(
    tracked_wine_id: int,
    directory: pathlib.Path,
) -> pathlib.Path | None:
    """Find an existing companion dossier by ID prefix."""
    prefix = f"{tracked_wine_id:05d}-"
    if not directory.exists():
        return directory / "placeholder"  # force None below
    for match in directory.glob(f"{prefix}*.md"):
        return match
    return None


# ---------------------------------------------------------------------------
# YAML helpers (same style as markdown.py)
# ---------------------------------------------------------------------------


def _yaml_str(val: object) -> str:
    """Format a value for YAML frontmatter."""
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "true" if val else "false"
    return f'"{val}"'


def _yaml_list(label: str, items: list | tuple) -> str:
    """Format a YAML list field."""
    if not items:
        return f"{label}: []\n"
    lines = f"{label}:\n"
    for item in items:
        lines += f"  - {item}\n"
    return lines


# ---------------------------------------------------------------------------
# Render companion dossier
# ---------------------------------------------------------------------------


def render_companion_dossier(
    tracked_wine: dict,
    related_wines: list[dict],
    winery_name: str | None,
    appellation: dict | None,
    settings: Settings,
    existing_content: str | None = None,
    tastings_by_wine: dict[int, list[dict]] | None = None,
    pro_ratings_by_wine: dict[int, list[dict]] | None = None,
    bottles_by_wine: dict[int, int] | None = None,
) -> str:
    """Render a companion dossier for a tracked wine."""
    preserved = _extract_agent_sections(existing_content) if existing_content else {}

    twid = tracked_wine["tracked_wine_id"]
    is_active = not tracked_wine.get("is_deleted", False)

    # Gather related wine data
    related_ids = [w["wine_id"] for w in related_wines]
    vintages = sorted(v for w in related_wines if (v := w.get("vintage")) is not None)

    # Appellation data
    country = appellation.get("country") if appellation else None
    region = appellation.get("region") if appellation else None
    classification = appellation.get("classification") if appellation else None

    # Sections — derive populated/pending from frontmatter (not heading
    # presence) so placeholder scaffolding is not mistaken for real content.
    companion_sections = settings.companion_sections
    all_keys = [s.key for s in companion_sections]
    if existing_content:
        agent_fm = _extract_frontmatter_agent_fields(existing_content)
        populated = agent_fm["agent_sections_populated"]
    else:
        populated = []
    pending = [k for k in all_keys if k not in populated]

    parts: list[str] = []

    # -- YAML frontmatter --
    parts.append("---\n")
    parts.append(f"tracked_wine_id: {twid}\n")
    parts.append(f"winery: {_yaml_str(winery_name)}\n")
    parts.append(f"wine_name: {_yaml_str(tracked_wine.get('wine_name'))}\n")
    parts.append(f"category: {_yaml_str(tracked_wine.get('category'))}\n")
    if country:
        parts.append(f"country: {_yaml_str(country)}\n")
    if region:
        parts.append(f"region: {_yaml_str(region)}\n")
    if classification:
        parts.append(f"classification: {_yaml_str(classification)}\n")
    parts.append(_yaml_list("related_wine_ids", related_ids))
    parts.append(_yaml_list("vintages_tracked", vintages))
    parts.append(f"is_active: {_yaml_str(is_active)}\n")
    parts.append(_yaml_list("agent_sections_populated", populated))
    parts.append(_yaml_list("agent_sections_pending", pending))
    parts.append(f"updated_at: {_yaml_str(tracked_wine.get('updated_at', ''))}\n")
    parts.append("---\n\n")

    # -- Title --
    title = winery_name or tracked_wine.get("wine_name", "Unknown")
    wine_name = tracked_wine.get("wine_name", "")
    if wine_name and wine_name != winery_name:
        title = f"{winery_name} {wine_name}" if winery_name else wine_name

    subtitle_parts: list[str] = []
    subtitle_parts.append("Cross-vintage research companion")
    if classification:
        subtitle_parts.append(classification)
    loc_parts = [p for p in (region, country) if p]
    if loc_parts:
        subtitle_parts.append(", ".join(loc_parts))

    parts.append(f"# {title} — Companion Dossier\n\n")
    parts.append(f"> {' · '.join(subtitle_parts)}\n\n")

    # -- Vintage Comparison matrix --
    if vintages:
        _tastings = tastings_by_wine or {}
        _ratings = pro_ratings_by_wine or {}
        _bottles = bottles_by_wine or {}
        vstats = _aggregate_vintage_stats(related_wines, _tastings, _ratings, _bottles)

        parts.append("## Vintage Comparison\n\n")
        parts.append("| Vintage | Status | Score | Price | Bottles | Notes |\n")
        parts.append("|---|---|---|---|---|---|\n")
        for s in vstats:
            v_str = str(s.vintage) if s.vintage else "NV"
            status = s.drinking_status or "—"
            if s.best_score is not None:
                max_s = int(s.best_score_max) if s.best_score_max else 100
                # Show decimal only if not whole
                score_val = int(s.best_score) if s.best_score == int(s.best_score) else s.best_score
                score_str = f"{score_val}/{max_s}"
            else:
                score_str = "—"
            if s.list_price is not None and s.list_currency:
                price_str = f"{s.list_currency} {s.list_price:.0f}"
            elif s.list_price is not None:
                price_str = f"{s.list_price:.0f}"
            else:
                price_str = "—"
            bottles_str = str(s.bottles_stored) if s.bottles_stored > 0 else "—"
            notes = "—"
            parts.append(f"| {v_str} | {status} | {score_str} | {price_str} | {bottles_str} | {notes} |\n")
        parts.append("\n")

        # Best value callout
        best_val = _select_best_value(vstats)
        if best_val is not None:
            bv_vintage = str(best_val.vintage) if best_val.vintage else "NV"
            max_s = int(best_val.best_score_max) if best_val.best_score_max else 100
            score_val = (
                int(best_val.best_score) if best_val.best_score == int(best_val.best_score) else best_val.best_score
            )
            price_str = (
                f"{best_val.list_currency} {best_val.list_price:.0f}"
                if best_val.list_currency and best_val.list_price
                else f"{best_val.price_per_750ml:.0f}"
            )
            parts.append(f"**Best value:** {bv_vintage} (score {score_val}/{max_s}, {price_str})\n\n")

        # Suggested drink order
        order, past_window = _suggest_drink_order(vstats)
        if order:
            order_strs = [str(v) if v else "NV" for v in order]
            parts.append(f"**Suggested drink order:** {' → '.join(order_strs)}\n\n")
        if past_window:
            pw_strs = [str(v) if v else "NV" for v in past_window]
            parts.append(f"> ⚠️ Past window: {', '.join(pw_strs)}\n\n")

        # Vintage trend
        trend = _vintage_trend(vstats)
        if trend:
            parts.append(f"**Vintage trend:** {trend}\n\n")

    # -- Agent sections --
    for sec in companion_sections:
        parts.append(f"## {sec.heading}\n")
        if sec.heading in preserved:
            parts.append(preserved[sec.heading])
            parts.append("\n\n")
        else:
            parts.append(f"<!-- source: {sec.tag} -->\n\n")
            parts.append("*Not yet researched. Pending agent action.*\n\n")
            parts.append(f"<!-- source: {sec.tag} — end -->\n\n")

    return "".join(parts)


# ---------------------------------------------------------------------------
# Batch generation
# ---------------------------------------------------------------------------


def generate_companion_dossiers(
    entities: dict[str, list[dict]],
    output_dir: pathlib.Path,
    settings: Settings,
) -> list[pathlib.Path]:
    """Generate companion dossiers for all active tracked wines.

    Returns the list of written file paths.
    """
    tracked_wines = entities.get("tracked_wine", [])
    if not tracked_wines:
        return []

    wines = entities.get("wine", [])
    wineries = entities.get("winery", [])
    appellations = entities.get("appellation", [])

    winery_by_id = {w["winery_id"]: w for w in wineries}
    appellation_by_id = {a["appellation_id"]: a for a in appellations}

    # Group wines by tracked_wine_id
    wines_by_tracked: dict[int, list[dict]] = {}
    for w in wines:
        twid = w.get("tracked_wine_id")
        if twid is not None and not w.get("is_deleted"):
            wines_by_tracked.setdefault(twid, []).append(w)

    # Build per-wine-id lookups for tasting, pro_rating, bottle data
    tastings_by_wine: dict[int, list[dict]] = {}
    for t in entities.get("tasting", []):
        wid = t.get("wine_id")
        if wid is not None:
            tastings_by_wine.setdefault(wid, []).append(t)

    pro_ratings_by_wine: dict[int, list[dict]] = {}
    for pr in entities.get("pro_rating", []):
        wid = pr.get("wine_id")
        if wid is not None:
            pro_ratings_by_wine.setdefault(wid, []).append(pr)

    bottles_by_wine: dict[int, int] = {}
    for b in entities.get("bottle", []):
        wid = b.get("wine_id")
        if wid is None:
            continue
        if b.get("status") == "stored" and not b.get("is_in_transit"):
            bottles_by_wine[wid] = bottles_by_wine.get(wid, 0) + 1

    subdir = settings.wishlist.wishlist_subdir
    dest_dir = output_dir / "wines" / subdir
    dest_dir.mkdir(parents=True, exist_ok=True)

    written: list[pathlib.Path] = []

    for tw in tracked_wines:
        if tw.get("is_deleted"):
            continue

        twid = tw["tracked_wine_id"]
        winery = winery_by_id.get(tw.get("winery_id"), {})
        winery_name = winery.get("name")
        appellation = appellation_by_id.get(tw.get("appellation_id"))
        related = wines_by_tracked.get(twid, [])

        slug = companion_dossier_slug(twid, winery_name, tw["wine_name"])
        dest = dest_dir / slug

        # Preserve existing content
        existing_content: str | None = None
        existing = _find_existing_companion(twid, dest_dir)
        if existing and existing.exists():
            existing_content = existing.read_text(encoding="utf-8")
            # Handle rename if slug changed
            if existing != dest and existing.exists():
                existing.rename(dest)

        content = render_companion_dossier(
            tw,
            related,
            winery_name,
            appellation,
            settings,
            existing_content,
            tastings_by_wine=tastings_by_wine,
            pro_ratings_by_wine=pro_ratings_by_wine,
            bottles_by_wine=bottles_by_wine,
        )
        dest.write_text(content, encoding="utf-8")
        written.append(dest)

    logger.info("Generated %d companion dossier(s)", len(written))
    return written


# ---------------------------------------------------------------------------
# Price tracker helper
# ---------------------------------------------------------------------------


def render_price_tracker_section(
    tracked_wine_id: int,
    data_dir: str | pathlib.Path,
) -> str:
    """Render the price tracker section content for a companion dossier.

    Returns formatted Markdown with latest prices, or a placeholder
    message if no price data exists. The agent calls this helper and
    writes the result via ``update_companion_dossier``.
    """
    from . import query as q

    try:
        return q.get_tracked_wine_prices(data_dir, tracked_wine_id)
    except Exception:
        return "*No price observations recorded yet.*"
