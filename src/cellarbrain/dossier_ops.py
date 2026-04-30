"""Dossier read, write, search, and pending-research operations.

Provides the data access layer for agent-owned dossier sections, used
by both the MCP server and the CLI ``dossier`` subcommand.
"""

from __future__ import annotations

import logging
import pathlib
import re
from datetime import UTC, datetime

import duckdb
import pyarrow.parquet as pq

from .settings import Settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class WineNotFoundError(Exception):
    """Wine ID does not exist or dossier file is missing."""


class ProtectedSectionError(Exception):
    """Attempted to write to an invalid or ETL-owned section."""


class TrackedWineNotFoundError(Exception):
    """Tracked wine ID does not exist or companion dossier is missing."""


# ---------------------------------------------------------------------------
# Section mapping: key → (heading text, agent fence tag)
#
# Derived from default Settings.  Functions accept an optional ``settings``
# parameter for runtime overrides.
# ---------------------------------------------------------------------------


def _build_agent_sections(settings: Settings) -> dict[str, tuple[str, str]]:
    """Build key → (heading, tag) mapping from Settings."""
    return {sec.key: (sec.heading, sec.tag) for sec in settings.agent_sections}


_DEFAULT_SETTINGS = Settings()
AGENT_SECTIONS: dict[str, tuple[str, str]] = _build_agent_sections(_DEFAULT_SETTINGS)
ALLOWED_SECTIONS = frozenset(AGENT_SECTIONS.keys())


def _build_companion_sections(settings: Settings) -> dict[str, tuple[str, str]]:
    """Build key → (heading, tag) mapping from companion Settings."""
    return {sec.key: (sec.heading, sec.tag) for sec in settings.companion_sections}


COMPANION_SECTIONS: dict[str, tuple[str, str]] = _build_companion_sections(_DEFAULT_SETTINGS)
ALLOWED_COMPANION_SECTIONS = frozenset(COMPANION_SECTIONS.keys())

# ETL-owned and mixed-section H2 heading → stable key mapping.
# Used by _all_heading_to_key() for selective section retrieval.
_ETL_SECTION_KEYS: dict[str, str] = {
    "Identity": "identity",
    "Origin": "origin",
    "Grapes": "grapes",
    "Characteristics": "characteristics",
    "Drinking Window": "drinking_window",
    "Cellar Inventory": "cellar_inventory",
    "Purchase History": "purchase_history",
    "Consumption History": "consumption_history",
    "Owner Notes": "owner_notes",
    # Mixed-ownership sections: H2 heading (ETL scaffold) maps to agent key.
    # The H2 headings differ from the AgentSection.heading H3 sub-headings.
    "Ratings & Reviews": "ratings_reviews",
    "Tasting Notes": "tasting_notes",
    "Food Pairings": "food_pairings",
}

# Lookahead split on H2 boundaries — preserves the "## " prefix in each block.
_H2_SPLIT_RE = re.compile(r"(?=^## )", re.MULTILINE)


def _all_heading_to_key(settings: Settings | None = None) -> dict[str, str]:
    """Build heading → key map for all dossier sections.

    Merges ETL + mixed section headings from ``_ETL_SECTION_KEYS`` with
    pure agent section headings from Settings (where ``mixed=False``).
    """
    result = dict(_ETL_SECTION_KEYS)
    s = settings or _DEFAULT_SETTINGS
    for sec in s.agent_sections:
        if not sec.mixed:
            result[sec.heading] = sec.key
    return result


# ---------------------------------------------------------------------------
# Dossier path resolution
# ---------------------------------------------------------------------------


def resolve_dossier_path(
    wine_id: int,
    data_dir: str | pathlib.Path,
) -> pathlib.Path:
    """Find the on-disk path for a wine's dossier file.

    Reads ``wine.parquet`` to look up ``dossier_path``, then resolves it
    relative to ``data_dir/wines/``.

    Raises WineNotFoundError if the wine_id doesn't exist.
    """
    d = pathlib.Path(data_dir)
    wine_pq = d / "wine.parquet"
    if not wine_pq.exists():
        raise WineNotFoundError(f"wine.parquet not found in {d}. Run 'cellarbrain etl' first.")

    table = pq.read_table(wine_pq, columns=["wine_id", "dossier_path"])
    ids = table.column("wine_id").to_pylist()
    paths = table.column("dossier_path").to_pylist()

    for wid, dpath in zip(ids, paths):
        if wid == wine_id:
            wines_dir = (d / "wines").resolve()
            full = (wines_dir / dpath).resolve()
            if not full.is_relative_to(wines_dir):
                raise WineNotFoundError(f"Invalid dossier path for wine {wine_id}: path traversal detected.")
            if not full.exists():
                raise WineNotFoundError(
                    f"Dossier file not found: {full}. The file may have been deleted or the ETL has not run."
                )
            return full

    raise WineNotFoundError(f"Wine ID {wine_id} does not exist.")


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


def read_dossier(
    wine_id: int,
    data_dir: str | pathlib.Path,
) -> str:
    """Read and return the full Markdown content of a wine dossier."""
    path = resolve_dossier_path(wine_id, data_dir)
    return path.read_text(encoding="utf-8")


def read_dossier_sections(
    wine_id: int,
    data_dir: str | pathlib.Path,
    sections: list[str] | None = None,
    settings: Settings | None = None,
) -> str:
    """Read a wine dossier, optionally filtered to specific sections.

    When *sections* is ``None``, returns the full content (unchanged behaviour).
    When provided, returns frontmatter + H1 + subtitle + only the requested
    H2-level sections.

    Valid section keys:
        ETL: "identity", "origin", "grapes", "characteristics",
             "drinking_window", "cellar_inventory", "purchase_history",
             "consumption_history", "owner_notes"
        Mixed: "ratings_reviews", "tasting_notes", "food_pairings"
        Agent: "producer_profile", "vintage_report", "wine_description",
               "market_availability", "similar_wines", "agent_log"

    Raises ``ValueError`` for unrecognised section keys.
    """
    path = resolve_dossier_path(wine_id, data_dir)
    text = path.read_text(encoding="utf-8")
    if sections is None:
        return text

    # Validate section keys
    valid_keys = set(_all_heading_to_key(settings).values())
    unknown = [s for s in sections if s not in valid_keys]
    if unknown:
        raise ValueError(
            f"Unknown section(s): {', '.join(sorted(unknown))}. Valid keys: {', '.join(sorted(valid_keys))}"
        )

    return _filter_sections(text, sections, settings)


def _filter_sections(
    text: str,
    requested: list[str],
    settings: Settings | None = None,
) -> str:
    """Extract frontmatter + H1 header + requested H2 sections from *text*.

    Splits on H2 boundaries.  The first block (frontmatter + H1 + subtitle)
    is always included.  Subsequent blocks are included only if their heading
    maps to a key in *requested*.  Unknown keys in *requested* are ignored.
    """
    heading_to_key = _all_heading_to_key(settings)
    requested_set = set(requested)
    blocks = _H2_SPLIT_RE.split(text)

    # blocks[0] = everything before the first "## " (frontmatter + H1 + subtitle)
    parts = [blocks[0]]

    for block in blocks[1:]:
        # Each block starts with "## Heading\n..."
        heading_line = block.split("\n", 1)[0]
        heading = heading_line.lstrip("# ").strip()
        key = heading_to_key.get(heading)
        if key and key in requested_set:
            parts.append(block)

    return "".join(parts)


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

# Regex to find an agent fence block under a given heading.
# Captures everything from the opening fence to the closing fence.
_FENCE_RE_TEMPLATE = (
    r"(###? {heading}\n)"
    r"(.*?)"
    r"(<!-- source: {tag} -->\n)"
    r"(.*?)"
    r"(<!-- source: {tag} — end -->\n?)"
)


def _build_fence_re(heading: str, tag: str) -> re.Pattern[str]:
    """Build a regex that matches the agent fence block for a section."""
    pattern = _FENCE_RE_TEMPLATE.format(
        heading=re.escape(heading),
        tag=re.escape(tag),
    )
    return re.compile(pattern, re.DOTALL)


def update_dossier(
    wine_id: int,
    section: str,
    content: str,
    data_dir: str | pathlib.Path,
    agent_name: str = "research",
    settings: Settings | None = None,
) -> str:
    """Write content to an agent-owned section in a wine dossier.

    Only sections listed in ``ALLOWED_SECTIONS`` can be updated.
    ETL-owned sections are protected. Returns a confirmation message.
    """
    agent_sections = _build_agent_sections(settings) if settings else AGENT_SECTIONS
    allowed = frozenset(agent_sections.keys())

    if section not in allowed:
        logger.warning(
            "Protected section write attempt — wine_id=%d section=%s",
            wine_id,
            section,
        )
        raise ProtectedSectionError(
            f"Section {section!r} is not an allowed agent section. Allowed: {', '.join(sorted(allowed))}"
        )

    if not content.strip():
        raise ValueError("content must not be empty or whitespace-only")

    path = resolve_dossier_path(wine_id, data_dir)
    text = path.read_text(encoding="utf-8")

    heading, tag = agent_sections[section]

    if section == "agent_log":
        text = _append_agent_log(text, heading, tag, content, agent_name)
    else:
        text = _replace_agent_block(text, heading, tag, content)

    # Update frontmatter: move from pending → populated (or back if placeholder)
    text = _update_frontmatter_lists(
        text,
        section,
        content,
        canonical_order=list(agent_sections.keys()),
    )

    # Auto-derive food_tags and food_groups when writing food_pairings prose
    if section == "food_pairings" and _should_auto_tag(settings, data_dir):
        try:
            cat_m = _FM_CATEGORY_RE.search(text)
            wine_cat = cat_m.group(1) if cat_m else None
            new_tags, new_groups = _auto_derive_food_data(
                content,
                data_dir,
                settings,
                wine_category=wine_cat,
            )
            if new_tags:
                text = _merge_food_tags(text, new_tags)
            if new_groups:
                text = _merge_food_groups(text, new_groups)
            if new_tags or new_groups:
                logger.info(
                    "Auto-derived %d food tag(s) and %d food group(s) for wine_id=%d",
                    len(new_tags),
                    len(new_groups),
                    wine_id,
                )
        except Exception:
            logger.debug(
                "Food tag/group auto-derive failed for wine_id=%d — prose saved without tags",
                wine_id,
                exc_info=True,
            )

    path.write_text(text, encoding="utf-8")
    logger.info(
        "Dossier updated — wine_id=%d section=%s agent=%s",
        wine_id,
        section,
        agent_name,
    )
    return f"Updated section '{section}' for wine #{wine_id}."


def _replace_agent_block(
    text: str,
    heading: str,
    tag: str,
    new_content: str,
) -> str:
    """Replace the content between agent fences for a section."""
    fence_re = _build_fence_re(heading, tag)
    m = fence_re.search(text)
    if not m:
        raise ProtectedSectionError(
            f"Could not find agent fence for '{heading}' with tag '{tag}' "
            "in the dossier. The dossier may need to be regenerated."
        )

    # Rebuild: heading + pre-fence + opening fence + NEW CONTENT + closing fence
    replacement = (
        m.group(1)  # heading line
        + m.group(2)  # any text between heading and opening fence
        + m.group(3)  # opening fence
        + new_content.rstrip("\n")
        + "\n\n"
        + m.group(5)  # closing fence
    )
    return text[: m.start()] + replacement + text[m.end() :]


def _append_agent_log(
    text: str,
    heading: str,
    tag: str,
    entry: str,
    agent_name: str,
) -> str:
    """Append a timestamped entry to the Agent Log section."""
    fence_re = _build_fence_re(heading, tag)
    m = fence_re.search(text)
    if not m:
        raise ProtectedSectionError("Could not find Agent Log section in the dossier.")

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    log_entry = f"- **{now}** ({agent_name}): {entry}\n"

    # Insert the new entry just before the closing fence
    existing_content = m.group(4)
    replacement = m.group(1) + m.group(2) + m.group(3) + existing_content + log_entry + "\n" + m.group(5)
    return text[: m.start()] + replacement + text[m.end() :]


_PLACEHOLDER_SENTINEL = "*Not yet researched. Pending agent action.*"


def _update_frontmatter_lists(
    text: str,
    section: str,
    content: str = "",
    canonical_order: list[str] | None = None,
) -> str:
    """Move a section key between agent_sections_pending and agent_sections_populated.

    When *content* matches the placeholder sentinel the section is moved back
    to pending (or kept there) rather than being marked as populated.

    If *canonical_order* is provided, both lists are sorted to match that
    order so the YAML output stays stable across write/revert cycles.
    """
    is_placeholder = content.strip() == _PLACEHOLDER_SENTINEL
    # Parse current lists
    pop_re = re.compile(
        r"^(agent_sections_populated:)\s*(\[]\n|(?:\n(?:\s+-\s+.+\n)*))",
        re.MULTILINE,
    )
    pend_re = re.compile(
        r"^(agent_sections_pending:)\s*(\[]\n|(?:\n(?:\s+-\s+.+\n)*))",
        re.MULTILINE,
    )

    # Extract current populated list
    pop_m = pop_re.search(text)
    populated: list[str] = []
    if pop_m:
        populated = re.findall(r"-\s+(\S+)", pop_m.group(2))

    # Extract current pending list
    pend_m = pend_re.search(text)
    pending: list[str] = []
    if pend_m:
        pending = re.findall(r"-\s+(\S+)", pend_m.group(2))

    # Move section between pending / populated
    if is_placeholder:
        if section in populated:
            populated.remove(section)
        if section not in pending:
            pending.append(section)
    else:
        if section in pending:
            pending.remove(section)
        if section not in populated:
            populated.append(section)

    # Stable-sort both lists by canonical order so YAML stays deterministic
    if canonical_order:
        order_idx = {key: i for i, key in enumerate(canonical_order)}

        def _sort_key(k: str) -> int:
            return order_idx.get(k, len(order_idx))

        populated.sort(key=_sort_key)
        pending.sort(key=_sort_key)

    # Rebuild the YAML list strings
    def _yaml_list(items: list[str]) -> str:
        if not items:
            return " []\n"
        return "\n" + "".join(f"  - {item}\n" for item in items)

    # Replace in text
    if pop_m:
        new_pop = f"agent_sections_populated:{_yaml_list(populated)}"
        text = text[: pop_m.start()] + new_pop + text[pop_m.end() :]

    # Re-search for pending (positions may have shifted)
    pend_m = pend_re.search(text)
    if pend_m:
        new_pend = f"agent_sections_pending:{_yaml_list(pending)}"
        text = text[: pend_m.start()] + new_pend + text[pend_m.end() :]

    return text


# ---------------------------------------------------------------------------
# Food tag auto-derive helpers
# ---------------------------------------------------------------------------

_FM_CATEGORY_RE = re.compile(r"^category:\s*(\S+)", re.MULTILINE)

_FM_FOOD_TAGS_RE = re.compile(
    r"^food_tags:\s*(?:\[]\s*\n|(\n(?:\s+-\s+.+\n)*))",
    re.MULTILINE,
)
_FM_FOOD_GROUPS_RE = re.compile(
    r"^food_groups:\s*(?:\[]\s*\n|(\n(?:\s+-\s+.+\n)*))",
    re.MULTILINE,
)


def _should_auto_tag(
    settings: Settings | None,
    data_dir: str | pathlib.Path,
) -> bool:
    """Return True if auto food-tag derivation is enabled and possible."""
    if settings and not settings.sommelier.auto_food_tags:
        return False
    catalogue_path = pathlib.Path(
        settings.sommelier.food_catalogue if settings else "models/sommelier/food_catalogue.parquet"
    )
    if not catalogue_path.is_absolute():
        catalogue_path = pathlib.Path(data_dir).parent / catalogue_path
    return catalogue_path.exists()


def _auto_derive_food_data(
    prose: str,
    data_dir: str | pathlib.Path,
    settings: Settings | None = None,
    wine_category: str | None = None,
    wine_sweetness: str | None = None,
) -> tuple[list[str], list[str]]:
    """Extract food tags (dish IDs) and food groups from prose.

    Parameters ``wine_category`` and ``wine_sweetness`` enable context-aware
    filtering (e.g. excluding red_meat matches for dessert wines).

    Returns (food_tags, food_groups) tuple.
    """
    from .sommelier.catalogue import (
        deduplicate_variants,
        derive_food_groups,
        extract_food_candidates,
        extract_food_groups,
        merge_food_groups,
        resolve_food_candidates,
        validate_food_data,
    )

    candidates = extract_food_candidates(prose)
    prose_groups = extract_food_groups(prose)

    if not candidates and not prose_groups:
        return [], []

    catalogue_path = pathlib.Path(
        settings.sommelier.food_catalogue if settings else "models/sommelier/food_catalogue.parquet"
    )
    if not catalogue_path.is_absolute():
        catalogue_path = pathlib.Path(data_dir).parent / catalogue_path

    food_tags: list[str] = []
    dish_groups: list[str] = []
    food_groups: list[str] = []

    if candidates:
        con = duckdb.connect()
        try:
            con.execute(f"CREATE TABLE food_catalogue AS SELECT * FROM read_parquet('{catalogue_path}')")
            food_tags = resolve_food_candidates(
                candidates,
                con,
                wine_category=wine_category,
                wine_sweetness=wine_sweetness,
            )
            food_tags = deduplicate_variants(food_tags)
            if food_tags:
                dish_groups = derive_food_groups(food_tags, con)

            # Merge dish-derived groups + prose-extracted groups by relevance score
            food_groups = merge_food_groups(dish_groups, prose_groups)

            # Post-derivation validation (safety net)
            food_tags, food_groups = validate_food_data(
                food_tags,
                food_groups,
                prose,
                con,
                wine_sweetness=wine_sweetness,
            )
        finally:
            con.close()
    else:
        food_groups = merge_food_groups(dish_groups, prose_groups)

    return food_tags, food_groups


def _auto_derive_food_tags(
    prose: str,
    data_dir: str | pathlib.Path,
    settings: Settings | None = None,
) -> list[str]:
    """Extract food references from prose and resolve to dish_id slugs."""
    tags, _groups = _auto_derive_food_data(prose, data_dir, settings)
    return tags


def _merge_food_tags(text: str, new_tags: list[str]) -> str:
    """Merge new food tags into the frontmatter, preserving existing ones (additive)."""
    existing: list[str] = []
    m = _FM_FOOD_TAGS_RE.search(text)
    if m and m.group(1):
        existing = re.findall(r"-\s+(\S+)", m.group(1))

    merged = list(dict.fromkeys(existing + new_tags))  # dedupe, order preserved

    def _yaml_list(items: list[str]) -> str:
        if not items:
            return " []\n"
        return "\n" + "".join(f"  - {item}\n" for item in items)

    new_block = f"food_tags:{_yaml_list(merged)}"
    if m:
        return text[: m.start()] + new_block + text[m.end() :]

    # No food_tags field yet — insert before agent_sections_populated
    pop_re = re.compile(r"^agent_sections_populated:", re.MULTILINE)
    pop_m = pop_re.search(text)
    if pop_m:
        return text[: pop_m.start()] + new_block + text[pop_m.start() :]

    return text


def _merge_food_groups(text: str, new_groups: list[str]) -> str:
    """Merge new food groups into the frontmatter, preserving existing ones (additive)."""
    existing: list[str] = []
    m = _FM_FOOD_GROUPS_RE.search(text)
    if m and m.group(1):
        existing = re.findall(r"-\s+(\S+)", m.group(1))

    merged = list(dict.fromkeys(existing + new_groups))  # dedupe, order preserved

    def _yaml_list(items: list[str]) -> str:
        if not items:
            return " []\n"
        return "\n" + "".join(f"  - {item}\n" for item in items)

    new_block = f"food_groups:{_yaml_list(merged)}"
    if m:
        return text[: m.start()] + new_block + text[m.end() :]

    # No food_groups field yet — insert after food_tags (preferred) or before agent_sections_populated
    ft_re = re.compile(r"^food_tags:.*?(?=\n\S|\Z)", re.MULTILINE | re.DOTALL)
    ft_m = ft_re.search(text)
    if ft_m:
        insert_pos = ft_m.end()
        return text[:insert_pos] + "\n" + new_block + text[insert_pos:]

    pop_re = re.compile(r"^agent_sections_populated:", re.MULTILINE)
    pop_m = pop_re.search(text)
    if pop_m:
        return text[: pop_m.start()] + new_block + text[pop_m.start() :]

    return text


# ---------------------------------------------------------------------------
# Pending research
# ---------------------------------------------------------------------------

_FM_PENDING_RE = re.compile(r"^agent_sections_pending:\s*\n((?:\s+-\s+.+\n)*)", re.MULTILINE)
_FM_WINE_ID_RE = re.compile(r"^wine_id:\s*(\d+)", re.MULTILINE)
_FM_TRACKED_WINE_ID_RE = re.compile(r"^tracked_wine_id:\s*(\d+)", re.MULTILINE)


def pending_research(
    data_dir: str | pathlib.Path,
    limit: int = 20,
    section: str | None = None,
) -> str:
    """List wines with pending agent sections, sorted by priority.

    Priority: favorites first, then by bottles_in_cellar descending.
    Returns a Markdown table.

    Args:
        data_dir: Path to the data directory.
        limit: Maximum wines to return.
        section: Optional section key filter. When provided, only returns
            wines whose pending list includes this section.
    """
    d = pathlib.Path(data_dir)
    wines_dir = d / "wines"

    if not wines_dir.exists():
        return "*No wine dossiers found.*"

    # Gather pending info from dossier frontmatter (fast regex, no YAML parser).
    # Read only the first 1320 bytes — frontmatter never exceeds ~1033 B;
    # the 20 % margin guards against future growth.
    pending_data: list[dict] = []
    for md_file in sorted(wines_dir.rglob("*.md")):
        with open(md_file, encoding="utf-8") as fh:
            text = fh.read(1320)

        wid_m = _FM_WINE_ID_RE.search(text)
        if not wid_m:
            continue
        wine_id = int(wid_m.group(1))

        pend_m = _FM_PENDING_RE.search(text)
        if not pend_m:
            continue
        items = re.findall(r"-\s+(\S+)", pend_m.group(1))
        if not items:
            continue

        pending_data.append(
            {
                "wine_id": wine_id,
                "pending_count": len(items),
                "pending_sections": ", ".join(items),
                "path": str(md_file.relative_to(wines_dir)),
            }
        )

    # Filter by specific section if requested
    if section:
        pending_data = [item for item in pending_data if section in item["pending_sections"].split(", ")]

    if not pending_data:
        return "*No wines with pending research sections.*"

    # Enrich with wine metadata from Parquet
    wine_pq = d / "wine.parquet"
    wine_meta: dict[int, dict] = {}
    if wine_pq.exists():
        table = pq.read_table(
            wine_pq,
            columns=["wine_id", "is_favorite", "name", "vintage"],
        )
        for i in range(table.num_rows):
            wid = table.column("wine_id")[i].as_py()
            wine_meta[wid] = {
                "is_favorite": table.column("is_favorite")[i].as_py(),
                "name": table.column("name")[i].as_py(),
                "vintage": table.column("vintage")[i].as_py(),
            }

    # Count bottles per wine from Parquet
    bottle_counts: dict[int, int] = {}
    bottle_pq = d / "bottle.parquet"
    if bottle_pq.exists():
        con = duckdb.connect(":memory:")
        bpath = str(bottle_pq).replace("\\", "/")
        df = con.execute(f"""
            SELECT wine_id, count(*) AS cnt
            FROM read_parquet('{bpath}')
            WHERE status = 'stored'
            GROUP BY wine_id
        """).fetchdf()
        for _, row in df.iterrows():
            bottle_counts[int(row["wine_id"])] = int(row["cnt"])
        con.close()

    # Enrich and sort
    for item in pending_data:
        wid = item["wine_id"]
        meta = wine_meta.get(wid, {})
        item["is_favorite"] = meta.get("is_favorite", False)
        item["wine_name"] = meta.get("name") or ""
        item["vintage"] = meta.get("vintage")
        item["bottles"] = bottle_counts.get(wid, 0)

    pending_data.sort(
        key=lambda x: (not x["is_favorite"], -x["bottles"], x["wine_id"]),
    )

    # Truncate
    pending_data = pending_data[:limit]

    # Format as Markdown table
    lines = [
        "| wine_id | Name | Vintage | Bottles | Favorite | Pending |",
        "|---|---|---|---|---|---|",
    ]
    for item in pending_data:
        fav = "⭐" if item["is_favorite"] else ""
        lines.append(
            f"| {item['wine_id']} "
            f"| {item['wine_name'] or '—'} "
            f"| {item['vintage'] or 'NV'} "
            f"| {item['bottles']} "
            f"| {fav} "
            f"| {item['pending_count']} |"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Companion dossier operations (tracked wines)
# ---------------------------------------------------------------------------


def resolve_companion_dossier_path(
    tracked_wine_id: int,
    data_dir: str | pathlib.Path,
) -> pathlib.Path:
    """Find the on-disk path for a tracked wine's companion dossier.

    Reads ``tracked_wine.parquet`` to look up ``dossier_path``, then resolves
    it relative to ``data_dir/wines/``.

    Raises TrackedWineNotFoundError if the tracked_wine_id doesn't exist.
    """
    d = pathlib.Path(data_dir)
    tw_pq = d / "tracked_wine.parquet"
    if not tw_pq.exists():
        raise TrackedWineNotFoundError(
            f"tracked_wine.parquet not found in {d}. Run 'cellarbrain etl' with wishlist enabled first."
        )

    table = pq.read_table(tw_pq, columns=["tracked_wine_id", "dossier_path"])
    ids = table.column("tracked_wine_id").to_pylist()
    paths = table.column("dossier_path").to_pylist()

    for twid, dpath in zip(ids, paths):
        if twid == tracked_wine_id:
            wines_dir = (d / "wines").resolve()
            full = (wines_dir / dpath).resolve()
            if not full.is_relative_to(wines_dir):
                raise TrackedWineNotFoundError(
                    f"Invalid dossier path for tracked wine {tracked_wine_id}: path traversal detected."
                )
            if not full.exists():
                raise TrackedWineNotFoundError(
                    f"Companion dossier not found: {full}. The file may have been deleted or the ETL has not run."
                )
            return full

    raise TrackedWineNotFoundError(f"Tracked wine ID {tracked_wine_id} does not exist.")


def read_companion_dossier(
    tracked_wine_id: int,
    data_dir: str | pathlib.Path,
    sections: list[str] | None = None,
    settings: Settings | None = None,
) -> str:
    """Read a companion dossier, optionally filtered to specific sections.

    When *sections* is ``None``, returns the full content.  When provided,
    returns frontmatter + H1 + subtitle + only the requested H2 sections.

    Valid section keys: "producer_deep_dive", "vintage_tracker",
    "buying_guide", "price_tracker".
    """
    path = resolve_companion_dossier_path(tracked_wine_id, data_dir)
    text = path.read_text(encoding="utf-8")
    if sections is None:
        return text
    return _filter_companion_sections(text, sections, settings)


def _filter_companion_sections(
    text: str,
    requested: list[str],
    settings: Settings | None = None,
) -> str:
    """Extract frontmatter + H1 header + requested H2 sections from companion text."""
    s = settings or _DEFAULT_SETTINGS
    heading_to_key = {sec.heading: sec.key for sec in s.companion_sections}
    # Also include the fixed "Vintages in Cellar" heading
    heading_to_key["Vintages in Cellar"] = "vintages_in_cellar"
    requested_set = set(requested)
    blocks = _H2_SPLIT_RE.split(text)

    parts = [blocks[0]]
    for block in blocks[1:]:
        heading_line = block.split("\n", 1)[0]
        heading = heading_line.lstrip("# ").strip()
        key = heading_to_key.get(heading)
        if key and key in requested_set:
            parts.append(block)

    return "".join(parts)


def update_companion_dossier(
    tracked_wine_id: int,
    section: str,
    content: str,
    data_dir: str | pathlib.Path,
    settings: Settings | None = None,
) -> str:
    """Write content to an agent-owned section in a companion dossier.

    Only sections listed in ``ALLOWED_COMPANION_SECTIONS`` can be updated.
    Returns a confirmation message.
    """
    companion_sections = _build_companion_sections(settings) if settings else COMPANION_SECTIONS
    allowed = frozenset(companion_sections.keys())

    if section not in allowed:
        logger.warning(
            "Protected section write attempt — tracked_wine_id=%d section=%s",
            tracked_wine_id,
            section,
        )
        raise ProtectedSectionError(
            f"Section {section!r} is not an allowed companion section. Allowed: {', '.join(sorted(allowed))}"
        )

    if not content.strip():
        raise ValueError("content must not be empty or whitespace-only")

    path = resolve_companion_dossier_path(tracked_wine_id, data_dir)
    text = path.read_text(encoding="utf-8")

    heading, tag = companion_sections[section]
    text = _replace_agent_block(text, heading, tag, content)
    text = _update_frontmatter_lists(
        text,
        section,
        content,
        canonical_order=list(companion_sections.keys()),
    )

    path.write_text(text, encoding="utf-8")
    logger.info(
        "Companion dossier updated — tracked_wine_id=%d section=%s",
        tracked_wine_id,
        section,
    )
    return f"Updated section '{section}' for tracked wine #{tracked_wine_id}."


def pending_companion_research(
    data_dir: str | pathlib.Path,
    limit: int = 20,
) -> str:
    """List tracked wines with pending companion sections.

    Scans companion dossier frontmatter for ``agent_sections_pending`` entries.
    Returns a Markdown table sorted by tracked_wine_id.
    """
    d = pathlib.Path(data_dir)
    wishlist_dir = d / "wines" / "tracked"

    if not wishlist_dir.exists():
        return "*No companion dossiers found.*"

    pending_data: list[dict] = []
    for md_file in sorted(wishlist_dir.glob("*.md")):
        with open(md_file, encoding="utf-8") as fh:
            text = fh.read(1320)

        twid_m = _FM_TRACKED_WINE_ID_RE.search(text)
        if not twid_m:
            continue
        tracked_wine_id = int(twid_m.group(1))

        pend_m = _FM_PENDING_RE.search(text)
        if not pend_m:
            continue
        items = re.findall(r"-\s+(\S+)", pend_m.group(1))
        if not items:
            continue

        pending_data.append(
            {
                "tracked_wine_id": tracked_wine_id,
                "pending_count": len(items),
                "pending_sections": ", ".join(items),
            }
        )

    if not pending_data:
        return "*No tracked wines with pending research sections.*"

    # Enrich with metadata from tracked_wine.parquet + winery.parquet
    tw_pq = d / "tracked_wine.parquet"
    tw_meta: dict[int, dict] = {}
    if tw_pq.exists():
        table = pq.read_table(
            tw_pq,
            columns=["tracked_wine_id", "wine_name", "winery_id"],
        )
        for i in range(table.num_rows):
            twid = table.column("tracked_wine_id")[i].as_py()
            tw_meta[twid] = {
                "wine_name": table.column("wine_name")[i].as_py(),
                "winery_id": table.column("winery_id")[i].as_py(),
            }

    winery_names: dict[int, str] = {}
    winery_pq = d / "winery.parquet"
    if winery_pq.exists():
        wt = pq.read_table(winery_pq, columns=["winery_id", "name"])
        for i in range(wt.num_rows):
            winery_names[wt.column("winery_id")[i].as_py()] = wt.column("name")[i].as_py()

    for item in pending_data:
        meta = tw_meta.get(item["tracked_wine_id"], {})
        item["wine_name"] = meta.get("wine_name") or ""
        winery_id = meta.get("winery_id")
        item["winery"] = winery_names.get(winery_id, "") if winery_id else ""

    pending_data.sort(key=lambda x: x["tracked_wine_id"])
    pending_data = pending_data[:limit]

    lines = [
        "| tracked_wine_id | Winery | Wine | Pending |",
        "|---|---|---|---|",
    ]
    for item in pending_data:
        lines.append(
            f"| {item['tracked_wine_id']} "
            f"| {item['winery'] or '—'} "
            f"| {item['wine_name'] or '—'} "
            f"| {item['pending_count']} |"
        )

    return "\n".join(lines)
