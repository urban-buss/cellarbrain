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

    # -- Vintages overview --
    if vintages:
        parts.append("## Vintages in Cellar\n\n")
        parts.append("| Vintage | Wine ID |\n|---|---|\n")
        for w in sorted(related_wines, key=lambda x: x.get("vintage") or 0):
            v = w.get("vintage")
            v_str = str(v) if v is not None else "NV"
            parts.append(f"| {v_str} | {w['wine_id']} |\n")
        parts.append("\n")

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
