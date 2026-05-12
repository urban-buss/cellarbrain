"""Guided wine education — learning path generation from cellar data.

Maps the user's collection onto pedagogical frameworks and generates
structured curricula for progressive wine understanding.  Ships with
curated frameworks for major regions (Burgundy, Piedmont, Bordeaux,
Germany/VDP) and falls back to data-driven hierarchy inference for
regions without templates.
"""

from __future__ import annotations

import logging
import pathlib
from dataclasses import dataclass, field

import duckdb

from .dossier_ops import WineNotFoundError, read_agent_section_content

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses — framework definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FrameworkLevel:
    """One level within a pedagogical hierarchy."""

    level: int
    name: str
    description: str
    match_classifications: list[str]
    match_subregions: list[str]
    match_subregion_contains: str
    key_lesson: str
    contrast_prompt: str


@dataclass(frozen=True)
class LearningFramework:
    """Curated pedagogical framework for a wine region."""

    key: str
    name: str
    match_regions: list[str]
    match_country: str
    description: str
    key_grapes: list[str]
    levels: list[FrameworkLevel]


# ---------------------------------------------------------------------------
# Dataclasses — learning path output
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LevelProgress:
    """Progress within one hierarchy level."""

    level: int
    name: str
    description: str
    key_lesson: str
    wines_owned: int
    wines_tasted: int
    subregions_covered: list[str]
    subregions_total: list[str]


@dataclass(frozen=True)
class CurriculumWine:
    """A wine selected for the tasting curriculum."""

    position: int
    wine_id: int
    wine_name: str
    vintage: int | None
    winery_name: str
    subregion: str | None
    classification: str | None
    level: int
    level_name: str
    reason: str
    what_to_notice: str


@dataclass(frozen=True)
class PurchaseSuggestion:
    """A gap in the educational framework to fill via purchase."""

    level: int
    level_name: str
    dimension: str  # "subregion", "classification", "level"
    value: str
    reason: str


@dataclass
class LearningPath:
    """Complete learning path for a wine topic."""

    topic: str
    framework_name: str
    framework_description: str
    match_type: str  # "region", "country", "subregion", "grape", "inferred"
    progress: list[LevelProgress] = field(default_factory=list)
    curriculum: list[CurriculumWine] = field(default_factory=list)
    purchase_suggestions: list[PurchaseSuggestion] = field(default_factory=list)
    dossier_insights: list[tuple[str, str]] = field(default_factory=list)
    total_wines: int = 0
    total_tasted: int = 0


# ---------------------------------------------------------------------------
# Framework loading & caching
# ---------------------------------------------------------------------------

_FRAMEWORKS_FILE = pathlib.Path(__file__).parent / "data" / "learning_frameworks.toml"
_frameworks_cache: list[LearningFramework] | None = None


def _load_frameworks(
    path: pathlib.Path | None = None,
) -> list[LearningFramework]:
    """Parse the TOML frameworks file and return a list of LearningFramework."""
    global _frameworks_cache
    if _frameworks_cache is not None and path is None:
        return _frameworks_cache

    import tomllib

    fpath = path or _FRAMEWORKS_FILE
    if not fpath.exists():
        logger.warning("Learning frameworks file not found: %s", fpath)
        return []

    with open(fpath, "rb") as f:
        data = tomllib.load(f)

    frameworks: list[LearningFramework] = []
    for key, fdef in data.items():
        levels = []
        for ldef in fdef.get("levels", []):
            levels.append(
                FrameworkLevel(
                    level=ldef["level"],
                    name=ldef["name"],
                    description=ldef.get("description", ""),
                    match_classifications=ldef.get("match_classifications", []),
                    match_subregions=ldef.get("match_subregions", []),
                    match_subregion_contains=ldef.get("match_subregion_contains", ""),
                    key_lesson=ldef.get("key_lesson", ""),
                    contrast_prompt=ldef.get("contrast_prompt", ""),
                )
            )
        levels.sort(key=lambda l: l.level)
        frameworks.append(
            LearningFramework(
                key=key,
                name=fdef["name"],
                match_regions=fdef.get("match_regions", []),
                match_country=fdef.get("match_country", ""),
                description=fdef.get("description", ""),
                key_grapes=fdef.get("key_grapes", []),
                levels=levels,
            )
        )

    if path is None:
        _frameworks_cache = frameworks
    return frameworks


# ---------------------------------------------------------------------------
# Topic resolution
# ---------------------------------------------------------------------------

_TOPIC_FILTER = """
    (strip_accents(region) ILIKE strip_accents($1)
     OR strip_accents(country) ILIKE strip_accents($1)
     OR strip_accents(subregion) ILIKE strip_accents($1)
     OR strip_accents(primary_grape) ILIKE strip_accents($1))
"""


def _match_framework(
    topic: str,
    frameworks: list[LearningFramework],
) -> LearningFramework | None:
    """Find a curated framework matching the topic string."""
    t = topic.strip().lower()
    for fw in frameworks:
        for region in fw.match_regions:
            if region.lower() == t:
                return fw
        if fw.match_country.lower() == t:
            return fw
        if fw.name.lower() == t or fw.key.lower() == t:
            return fw
    return None


def _resolve_match_type(
    con: duckdb.DuckDBPyConnection,
    topic: str,
) -> str | None:
    """Determine how the topic matches wines_full data.

    Returns ``"region"``, ``"country"``, ``"subregion"``, ``"grape"``,
    or ``None``.
    """
    row = con.execute(
        f"""
        SELECT
            CASE
                WHEN bool_or(strip_accents(region) ILIKE strip_accents($1)) THEN 'region'
                WHEN bool_or(strip_accents(country) ILIKE strip_accents($1)) THEN 'country'
                WHEN bool_or(strip_accents(subregion) ILIKE strip_accents($1)) THEN 'subregion'
                WHEN bool_or(strip_accents(primary_grape) ILIKE strip_accents($1)) THEN 'grape'
            END AS match_type
        FROM wines_full
        WHERE bottles_stored > 0
          AND {_TOPIC_FILTER}
        """,
        [topic],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0]


# ---------------------------------------------------------------------------
# Wine fetching
# ---------------------------------------------------------------------------

_EDUCATION_COLUMNS = (
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
    "drinking_status",
    "best_pro_score",
    "tasting_count",
    "price_tier",
)


def _fetch_education_wines(
    con: duckdb.DuckDBPyConnection,
    topic: str,
    *,
    limit: int = 50,
) -> list[dict]:
    """Fetch stored wines matching the topic."""
    cols = ", ".join(_EDUCATION_COLUMNS)
    sql = f"""
        SELECT {cols}
        FROM wines_full
        WHERE bottles_stored > 0
          AND {_TOPIC_FILTER}
        ORDER BY classification NULLS FIRST, subregion, best_pro_score DESC NULLS LAST
        LIMIT $2
    """
    rows = con.execute(sql, [topic, limit]).fetchall()
    desc = con.description
    col_names = [d[0] for d in desc] if desc else list(_EDUCATION_COLUMNS)
    return [dict(zip(col_names, row)) for row in rows]


def _fetch_coverage(
    con: duckdb.DuckDBPyConnection,
    topic: str,
) -> list[dict]:
    """Fetch coverage stats including consumed wines for gap analysis."""
    sql = f"""
        SELECT
            subregion,
            classification,
            primary_grape,
            COUNT(*) AS total_wines,
            COUNT(*) FILTER (WHERE bottles_stored > 0) AS stored_wines,
            COALESCE(SUM(tasting_count), 0) AS total_tastings
        FROM wines_full
        WHERE {_TOPIC_FILTER}
        GROUP BY subregion, classification, primary_grape
    """
    rows = con.execute(sql, [topic]).fetchall()
    desc = con.description
    col_names = (
        [d[0] for d in desc]
        if desc
        else [
            "subregion",
            "classification",
            "primary_grape",
            "total_wines",
            "stored_wines",
            "total_tastings",
        ]
    )
    return [dict(zip(col_names, row)) for row in rows]


# ---------------------------------------------------------------------------
# Level mapping
# ---------------------------------------------------------------------------


def _assign_level(wine: dict, framework: LearningFramework) -> int:
    """Assign a wine to a framework level, returning the level number.

    Returns 0 if the wine doesn't match any defined level.
    """
    classification = wine.get("classification") or ""
    subregion = wine.get("subregion") or ""

    # Try levels in reverse order (highest first) so that specific
    # classification matches (e.g. "Grand Cru") win over broader matches.
    for fl in reversed(framework.levels):
        # Check classification match
        if fl.match_classifications and classification in fl.match_classifications:
            return fl.level
        # Check subregion exact match
        if fl.match_subregions and subregion in fl.match_subregions:
            return fl.level

    # Check subregion_contains (looser match, try lowest levels first)
    for fl in framework.levels:
        if fl.match_subregion_contains and fl.match_subregion_contains.lower() in subregion.lower():
            return fl.level

    # Default: assign to the lowest level
    return framework.levels[0].level if framework.levels else 0


def _map_wines_to_levels(
    wines: list[dict],
    framework: LearningFramework,
) -> dict[int, list[dict]]:
    """Assign each wine to a framework level and group by level number."""
    by_level: dict[int, list[dict]] = {}
    for wine in wines:
        lvl = _assign_level(wine, framework)
        wine["_level"] = lvl
        by_level.setdefault(lvl, []).append(wine)
    return by_level


# ---------------------------------------------------------------------------
# Inferred hierarchy fallback
# ---------------------------------------------------------------------------


def _infer_hierarchy(wines: list[dict]) -> LearningFramework:
    """Build a synthetic framework from the wine data when no curated one exists.

    Groups wines by classification (if available) or subregion, creating
    pseudo-levels.
    """
    classifications = sorted({w.get("classification") or "" for w in wines})
    # Remove empty string
    classifications = [c for c in classifications if c]

    if len(classifications) >= 2:
        # Use classification as the hierarchy
        levels = []
        for i, cls in enumerate(classifications, 1):
            levels.append(
                FrameworkLevel(
                    level=i,
                    name=cls,
                    description=f"Wines classified as {cls}",
                    match_classifications=[cls],
                    match_subregions=[],
                    match_subregion_contains="",
                    key_lesson=f"Explore the {cls} tier",
                    contrast_prompt=f"What distinguishes {cls} from other levels?",
                )
            )
        # Add a level 0 for unclassified wines
        levels.insert(
            0,
            FrameworkLevel(
                level=0,
                name="Unclassified",
                description="Wines without a formal classification",
                match_classifications=[],
                match_subregions=[],
                match_subregion_contains="",
                key_lesson="The baseline — wines without formal classification hierarchy",
                contrast_prompt="How do unclassified wines compare to classified ones?",
            ),
        )
    else:
        # Fall back to subregion grouping
        subregions = sorted({w.get("subregion") or "Other" for w in wines})
        levels = []
        for i, sub in enumerate(subregions, 1):
            subs = [] if sub == "Other" else [sub]
            levels.append(
                FrameworkLevel(
                    level=i,
                    name=sub,
                    description=f"Wines from {sub}",
                    match_classifications=[],
                    match_subregions=subs,
                    match_subregion_contains="",
                    key_lesson=f"Explore the character of {sub}",
                    contrast_prompt=f"What makes {sub} distinctive?",
                )
            )

    # Determine region from wines
    regions = {w.get("region") or "" for w in wines}
    region_name = next((r for r in regions if r), "Unknown Region")

    return LearningFramework(
        key="inferred",
        name=region_name,
        match_regions=[],
        match_country="",
        description=f"Data-driven exploration of {region_name}",
        key_grapes=[],
        levels=levels,
    )


# ---------------------------------------------------------------------------
# Curriculum building
# ---------------------------------------------------------------------------

_DRINKABLE_STATUSES = frozenset({"optimal", "drinkable", "past_optimal"})


def _build_curriculum(
    wines_by_level: dict[int, list[dict]],
    framework: LearningFramework,
    lesson_size: int = 4,
) -> list[CurriculumWine]:
    """Select and order wines for maximum educational contrast.

    Picks one wine per framework level (ascending), preferring:
    - wines in drinkable/optimal status
    - untasted wines (tasting_count == 0)
    - subregion diversity across the curriculum
    """
    selected: list[CurriculumWine] = []
    used_subregions: set[str] = set()
    position = 1

    level_map = {fl.level: fl for fl in framework.levels}

    for fl in framework.levels:
        candidates = wines_by_level.get(fl.level, [])
        if not candidates:
            continue

        # Score candidates for curriculum fitness
        def _score(w: dict) -> tuple:
            status = w.get("drinking_status") or ""
            drinkable = status in _DRINKABLE_STATUSES
            untasted = (w.get("tasting_count") or 0) == 0
            sub = w.get("subregion") or ""
            diverse = sub not in used_subregions
            score = w.get("best_pro_score") or 0
            return (drinkable, untasted, diverse, score)

        best = max(candidates, key=_score)
        sub = best.get("subregion") or ""
        if sub:
            used_subregions.add(sub)

        fl_obj = level_map.get(fl.level, fl)
        reason = f"Level {fl.level}: {fl_obj.name}"
        if best.get("drinking_status") in _DRINKABLE_STATUSES:
            reason += f" — {(best.get('drinking_status') or '').replace('_', ' ')}"

        selected.append(
            CurriculumWine(
                position=position,
                wine_id=best["wine_id"],
                wine_name=best.get("wine_name") or "",
                vintage=best.get("vintage"),
                winery_name=best.get("winery_name") or "",
                subregion=best.get("subregion"),
                classification=best.get("classification"),
                level=fl.level,
                level_name=fl_obj.name,
                reason=reason,
                what_to_notice=fl_obj.contrast_prompt,
            )
        )
        position += 1

        if len(selected) >= lesson_size:
            break

    return selected


# ---------------------------------------------------------------------------
# Progress computation
# ---------------------------------------------------------------------------


def _compute_progress(
    wines_by_level: dict[int, list[dict]],
    framework: LearningFramework,
) -> list[LevelProgress]:
    """Calculate coverage and tasting progress per level."""
    progress: list[LevelProgress] = []
    for fl in framework.levels:
        level_wines = wines_by_level.get(fl.level, [])
        tasted = [w for w in level_wines if (w.get("tasting_count") or 0) > 0]
        subs_covered = sorted({w.get("subregion") or "" for w in level_wines if w.get("subregion")})
        subs_total = list(fl.match_subregions) if fl.match_subregions else subs_covered

        progress.append(
            LevelProgress(
                level=fl.level,
                name=fl.name,
                description=fl.description,
                key_lesson=fl.key_lesson,
                wines_owned=len(level_wines),
                wines_tasted=len(tasted),
                subregions_covered=subs_covered,
                subregions_total=subs_total,
            )
        )
    return progress


# ---------------------------------------------------------------------------
# Purchase gap analysis
# ---------------------------------------------------------------------------


def _identify_purchase_gaps(
    wines_by_level: dict[int, list[dict]],
    framework: LearningFramework,
) -> list[PurchaseSuggestion]:
    """Identify educational gaps in the cellar holdings."""
    suggestions: list[PurchaseSuggestion] = []

    for fl in framework.levels:
        level_wines = wines_by_level.get(fl.level, [])

        # Gap: no wines at this level
        if not level_wines:
            suggestions.append(
                PurchaseSuggestion(
                    level=fl.level,
                    level_name=fl.name,
                    dimension="level",
                    value=fl.name,
                    reason=f"No wines at this level — needed to experience: {fl.key_lesson}",
                )
            )
            continue

        # Gap: missing subregions within a level that defines them
        if fl.match_subregions:
            owned_subs = {w.get("subregion") for w in level_wines if w.get("subregion")}
            missing = sorted(set(fl.match_subregions) - owned_subs)
            for sub in missing[:3]:  # Cap at 3 suggestions per level
                suggestions.append(
                    PurchaseSuggestion(
                        level=fl.level,
                        level_name=fl.name,
                        dimension="subregion",
                        value=sub,
                        reason=f"Missing {sub} at {fl.name} level — adds geographic contrast",
                    )
                )

    return suggestions


# ---------------------------------------------------------------------------
# Dossier insights
# ---------------------------------------------------------------------------


def _extract_educational_insights(
    curriculum: list[CurriculumWine],
    data_dir: str | pathlib.Path,
    max_wines: int = 4,
) -> list[tuple[str, str]]:
    """Read producer_profile excerpts for curriculum wines.

    Returns ``(display_name, excerpt)`` tuples.  Skips wines whose
    dossier has no producer_profile content.
    """
    insights: list[tuple[str, str]] = []
    for cw in curriculum[:max_wines]:
        try:
            profile = read_agent_section_content(cw.wine_id, "producer_profile", data_dir)
        except (WineNotFoundError, Exception):
            profile = ""
        if profile:
            display = f"{cw.winery_name}"
            if cw.subregion:
                display += f" ({cw.subregion})"
            insights.append((display, profile[:300]))
    return insights


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_learning_path(
    con: duckdb.DuckDBPyConnection,
    topic: str,
    data_dir: str | pathlib.Path,
    *,
    lesson_size: int = 4,
    include_purchases: bool = True,
    include_dossier_insights: bool = True,
    frameworks_path: pathlib.Path | None = None,
) -> LearningPath | None:
    """Build a complete learning path for *topic*.

    Returns ``None`` when no stored wines match the topic.
    """
    lesson_size = max(2, min(lesson_size, 6))

    # 1. Load frameworks
    frameworks = _load_frameworks(frameworks_path)

    # 2. Resolve topic → framework + match type
    framework = _match_framework(topic, frameworks)
    match_type = _resolve_match_type(con, topic)
    if match_type is None:
        return None

    # 3. Fetch wines
    wines = _fetch_education_wines(con, topic)
    if not wines:
        return None

    # 4. Build or infer framework
    if framework is None:
        framework = _infer_hierarchy(wines)
        match_type = "inferred"

    # 5. Map wines to levels
    wines_by_level = _map_wines_to_levels(wines, framework)

    # 6. Compute progress
    progress = _compute_progress(wines_by_level, framework)

    # 7. Build curriculum
    curriculum = _build_curriculum(wines_by_level, framework, lesson_size=lesson_size)

    # 8. Identify gaps
    purchases: list[PurchaseSuggestion] = []
    if include_purchases:
        purchases = _identify_purchase_gaps(wines_by_level, framework)

    # 9. Extract dossier insights
    insights: list[tuple[str, str]] = []
    if include_dossier_insights and curriculum:
        insights = _extract_educational_insights(curriculum, data_dir)

    # 10. Assemble
    total_tasted = sum(1 for w in wines if (w.get("tasting_count") or 0) > 0)

    return LearningPath(
        topic=topic,
        framework_name=framework.name,
        framework_description=framework.description,
        match_type=match_type,
        progress=progress,
        curriculum=curriculum,
        purchase_suggestions=purchases,
        dossier_insights=insights,
        total_wines=len(wines),
        total_tasted=total_tasted,
    )


# ---------------------------------------------------------------------------
# Markdown formatter
# ---------------------------------------------------------------------------


def format_learning_path(path: LearningPath) -> str:
    """Format a LearningPath as Markdown."""
    lines: list[str] = []

    lines.append(f"## Learning Path: {path.framework_name}")
    lines.append("")
    lines.append(f"*{path.framework_description}*")
    lines.append("")

    # --- Progress table ---
    lines.append("### Your Progress")
    lines.append("")
    lines.append("| Level | Coverage | Wines Owned | Tasted |")
    lines.append("|------:|----------|:-----------:|:------:|")

    for lp in path.progress:
        if lp.subregions_total and len(lp.subregions_total) > 0:
            covered = len(lp.subregions_covered)
            total = len(lp.subregions_total)
            if covered >= total and total > 0:
                coverage = "✅"
            else:
                coverage = f"{covered}/{total} subregions"
        else:
            coverage = "✅" if lp.wines_owned > 0 else "—"

        lines.append(f"| {lp.level}. {lp.name} | {coverage} | {lp.wines_owned} | {lp.wines_tasted} |")

    lines.append("")

    # Overall progress summary
    if path.total_wines > 0:
        max_level = 0
        for lp in path.progress:
            if lp.wines_owned > 0:
                max_level = max(max_level, lp.level)
        if max_level > 0 and path.progress:
            level_name = next(
                (lp.name for lp in path.progress if lp.level == max_level),
                "",
            )
            lines.append(
                f"**Overall:** You've reached **Level {max_level}** "
                f"({level_name}) with {path.total_wines} wines, "
                f"{path.total_tasted} tasted."
            )
        else:
            lines.append(f"**Overall:** {path.total_wines} wines, {path.total_tasted} tasted.")
    lines.append("")

    # --- Curriculum / Lesson ---
    if path.curriculum:
        lines.append("### Tasting Lesson")
        lines.append("")
        lines.append("*Taste these wines in this order for maximum educational contrast:*")
        lines.append("")
        lines.append("| # | Wine | Vintage | Level | Why This Order |")
        lines.append("|--:|------|---------|-------|----------------|")
        for cw in path.curriculum:
            vintage = cw.vintage or "NV"
            lines.append(f"| {cw.position} | {cw.wine_name} | {vintage} | {cw.level_name} | {cw.reason} |")
        lines.append("")

        # What to notice
        lines.append("**What to notice:**")
        for i, cw in enumerate(path.curriculum):
            if cw.what_to_notice:
                if i > 0:
                    prev = path.curriculum[i - 1]
                    lines.append(f"- Wine {prev.position}→{cw.position}: {cw.what_to_notice}")
                else:
                    lines.append(f"- Wine {cw.position}: {cw.what_to_notice}")
        lines.append("")

    # --- Key Lessons ---
    if path.progress:
        lines.append("### Key Lessons by Level")
        lines.append("")
        for lp in path.progress:
            if lp.key_lesson:
                status = "✅" if lp.wines_owned > 0 else "🔲"
                lines.append(f"- **{lp.name}** {status}: {lp.key_lesson}")
        lines.append("")

    # --- Purchase Suggestions ---
    if path.purchase_suggestions:
        lines.append("### Purchase Suggestions")
        lines.append("")
        for ps in path.purchase_suggestions:
            lines.append(f"- **{ps.level_name}** ({ps.dimension}): {ps.value}")
            lines.append(f"  → {ps.reason}")
        lines.append("")

    # --- Dossier Insights ---
    if path.dossier_insights:
        lines.append("### Dossier Insights")
        lines.append("")
        for display_name, excerpt in path.dossier_insights:
            lines.append(f"**{display_name}**")
            lines.append(f"> {excerpt}")
            lines.append("")

    return "\n".join(lines)
