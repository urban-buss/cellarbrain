"""Food catalogue lookup and food-tag extraction from prose.

Provides two functions used by the auto-derive pipeline in
``dossier_ops``:

- ``extract_food_candidates`` — hybrid regex + keyword extraction
  that pulls food reference strings from food_pairings prose.
- ``resolve_food_candidates`` — DuckDB keyword search against the
  food catalogue to turn candidate strings into ``dish_id`` slugs.

No heavy dependencies — uses only stdlib ``re`` and an existing
DuckDB connection. Works without the ``[sommelier]`` optional extra.
"""

from __future__ import annotations

import re

_BOLD_RE = re.compile(r"\*\*([^*]+?)\*\*")
_COLON_LIST_RE = re.compile(r":\s*(.+?)(?:\.\s|\n)")
_BULLET_RE = re.compile(r"^-\s+\*?\*?([^—\n]+?)(?:\*\*|\s—|\n)", re.MULTILINE)
_SPLIT_RE = re.compile(r",\s*|\s+and\s+")

FOOD_KEYWORDS: frozenset[str] = frozenset(
    {
        "beef",
        "lamb",
        "duck",
        "chicken",
        "pork",
        "venison",
        "veal",
        "fish",
        "salmon",
        "tuna",
        "lobster",
        "shrimp",
        "oyster",
        "cheese",
        "parmesan",
        "gruyère",
        "comté",
        "fontina",
        "cheddar",
        "risotto",
        "pasta",
        "tagliatelle",
        "tajarin",
        "gnocchi",
        "truffle",
        "mushroom",
        "porcini",
        "raclette",
        "fondue",
        "carpaccio",
        "confit",
        "tartare",
        "stew",
        "ragù",
        "bourguignon",
        "osso",
        "buco",
        "braised",
        "grilled",
        "roasted",
        "chocolate",
        "dessert",
    }
)

_NOISE: frozenset[str] = frozenset(
    {
        "classic pairings",
        "regional affinity",
        "adventurous",
        "regional matches",
        "traditional pairings",
        "modern",
    }
)


def extract_food_candidates(prose: str) -> list[str]:
    """Extract food reference candidates from food_pairings prose.

    Uses a hybrid approach: regex pattern extraction for structured prose
    (bold items, bullet lists, comma-separated sequences) combined with
    keyword-anchored extraction for narrative prose.

    Returns deduplicated candidate strings suitable for catalogue search.

    Examples:
        >>> extract_food_candidates("**Duck confit**, **Raclette**")
        ['Duck confit', 'Raclette']
        >>> extract_food_candidates("- **Grilled lamb chops** with rosemary")
        ['Grilled lamb chops']
    """
    candidates: set[str] = set()

    # Regex extraction — structured patterns
    for pattern in (_BOLD_RE, _COLON_LIST_RE, _BULLET_RE):
        for m in pattern.finditer(prose):
            for part in _SPLIT_RE.split(m.group(1)):
                part = part.strip().strip("*").strip()
                if 3 <= len(part) <= 60:
                    candidates.add(part)

    # Keyword extraction — narrative anchors
    words = re.findall(r"\b[\w'-]+\b", prose.lower())
    for i, word in enumerate(words):
        if word in FOOD_KEYWORDS:
            start = max(0, i - 1)
            end = min(len(words), i + 2)
            phrase = " ".join(words[start:end])
            if 3 <= len(phrase) <= 60:
                candidates.add(phrase)

    return sorted(c for c in candidates if c.lower().rstrip(":") not in _NOISE)


# Weight-class restrictions by wine category
_WINE_CATEGORY_FILTERS: dict[str, dict[str, object]] = {
    "sweet": {
        "exclude_proteins": {"red_meat", "pork", "game"},
        "prefer_weights": {"light", "medium"},
    },
    "dessert_wine": {
        "exclude_proteins": {"red_meat", "pork", "game"},
        "prefer_weights": {"light", "medium"},
    },
}


def _filter_by_wine_context(
    dish_ids: list[str],
    con: object,
    filters: dict[str, object],
) -> list[str]:
    """Remove dish_ids whose protein is in the exclude set."""
    if not dish_ids:
        return dish_ids

    exclude_proteins = filters.get("exclude_proteins", set())
    if not exclude_proteins:
        return dish_ids

    placeholders = ", ".join("?" for _ in dish_ids)
    sql = f"SELECT dish_id, protein FROM food_catalogue WHERE dish_id IN ({placeholders})"
    try:
        rows = con.execute(sql, dish_ids).fetchall()  # type: ignore[union-attr]
    except Exception:
        return dish_ids

    excluded = {did for did, protein in rows if protein in exclude_proteins}
    return [did for did in dish_ids if did not in excluded]


def resolve_food_candidates(
    candidates: list[str],
    con: object,
    *,
    limit_per_query: int = 2,
    min_name_overlap: float = 0.5,
    wine_category: str | None = None,
    wine_sweetness: str | None = None,
) -> list[str]:
    """Resolve food candidate strings to ``dish_id`` slugs via DuckDB.

    Uses tokenized keyword matching with relevance scoring:
    - Matches against ``dish_name`` are scored 3x (primary signal)
    - Matches against ``description`` are scored 1x
    - Matches against ``ingredients`` are scored 0.5x

    Results are ranked by score; only results where at least
    ``min_name_overlap`` fraction of query tokens appear in the dish_name
    are returned (unless no such results exist, in which case the top
    overall match is returned).

    Parameters
    ----------
    candidates:
        Candidate food reference strings from ``extract_food_candidates``.
    con:
        A DuckDB connection with a ``food_catalogue`` table registered.
    limit_per_query:
        Maximum matches per candidate string.
    min_name_overlap:
        Minimum fraction of query tokens that must appear in dish_name
        for a result to be considered a "strong" match.
    wine_category:
        Wine category (e.g. ``"red"``, ``"white"``, ``"dessert_wine"``).
    wine_sweetness:
        Wine sweetness level (e.g. ``"dry"``, ``"sweet"``, ``"dessert"``).

    Returns
    -------
    Deduplicated list of ``dish_id`` slugs.
    """
    seen: set[str] = set()
    results: list[str] = []

    for candidate in candidates:
        tokens = candidate.lower().split()
        if not tokens:
            continue

        # Build scoring SQL using a subquery to avoid repeating params
        score_parts: list[str] = []
        score_params: list[object] = []
        for token in tokens:
            like = f"%{token}%"
            score_parts.append(
                "(CASE WHEN LOWER(dish_name) LIKE ? THEN 3 ELSE 0 END"
                " + CASE WHEN LOWER(description) LIKE ? THEN 1 ELSE 0 END"
                " + CASE WHEN LOWER(array_to_string(ingredients, ',')) LIKE ?"
                " THEN 0.5 ELSE 0 END)"
            )
            score_params.extend([like, like, like])

        # Compute name overlap fraction
        name_hits: list[str] = []
        name_params: list[object] = []
        for token in tokens:
            like = f"%{token}%"
            name_hits.append("CASE WHEN LOWER(dish_name) LIKE ? THEN 1 ELSE 0 END")
            name_params.append(like)

        total_score = " + ".join(score_parts)
        name_frac = f"({' + '.join(name_hits)})::FLOAT / {len(tokens)}"

        sql = (
            f"SELECT dish_id, score, name_overlap FROM ("
            f"  SELECT dish_id, ({total_score}) AS score, "
            f"  ({name_frac}) AS name_overlap "
            f"  FROM food_catalogue"
            f") sub WHERE score > 0 "
            f"ORDER BY score DESC "
            f"LIMIT ?"
        )
        params = score_params + name_params + [limit_per_query]

        try:
            rows = con.execute(sql, params).fetchall()  # type: ignore[union-attr]
        except Exception:
            continue

        # Prefer strong name matches; fall back to best overall if score is high enough
        strong = [(did, s) for did, s, no in rows if no >= min_name_overlap]
        if strong:
            for dish_id, _ in strong:
                if dish_id not in seen:
                    seen.add(dish_id)
                    results.append(dish_id)
        elif rows:
            # Only take the top-1 if score is meaningful (≥ 1 point per token)
            dish_id, score, _no = rows[0]
            if score >= len(tokens) and dish_id not in seen:
                seen.add(dish_id)
                results.append(dish_id)

    # Post-filter: exclude inappropriate proteins for sweet/dessert wines
    if wine_sweetness in ("sweet", "dessert") or wine_category == "dessert_wine":
        results = _filter_by_wine_context(
            results,
            con,
            _WINE_CATEGORY_FILTERS["sweet"],
        )

    return results


# ---------------------------------------------------------------------------
# Variant deduplication
# ---------------------------------------------------------------------------

_VARIANT_SUFFIX_RE = re.compile(r"-v\d+$|-\d+$")


def deduplicate_variants(dish_ids: list[str]) -> list[str]:
    """Collapse variant dish_ids to their canonical stems.

    Strips ``-v2``, ``-v3``, ``-2``, ``-3`` suffixes and keeps only the first
    occurrence of each stem. If the stem itself exists in the list, it
    takes priority; otherwise the first variant is kept.

    Examples:
        >>> deduplicate_variants(["daube-provencale", "daube-provencale-v2"])
        ['daube-provencale']
        >>> deduplicate_variants(["cassoulet-v2", "cassoulet"])
        ['cassoulet']
        >>> deduplicate_variants(["kleftiko", "kleftiko-greek"])
        ['kleftiko', 'kleftiko-greek']
    """
    stems_seen: dict[str, str] = {}  # stem → first dish_id
    result: list[str] = []

    for dish_id in dish_ids:
        stem = _VARIANT_SUFFIX_RE.sub("", dish_id)
        if stem not in stems_seen:
            stems_seen[stem] = dish_id
            result.append(dish_id)
        elif dish_id == stem and stems_seen[stem] != stem:
            # The canonical (no-suffix) version appeared later — swap
            idx = result.index(stems_seen[stem])
            result[idx] = dish_id
            stems_seen[stem] = dish_id

    return result


# ---------------------------------------------------------------------------
# Food-group derivation
# ---------------------------------------------------------------------------

_ALLOWED_PROTEINS: frozenset[str] = frozenset(
    {
        "red_meat",
        "poultry",
        "fish",
        "seafood",
        "pork",
        "game",
        "vegetarian",
        "vegan",
        "cheese",
    }
)

_ALLOWED_METHODS: frozenset[str] = frozenset(
    {
        "grilled",
        "braised",
        "stewed",
        "fried",
        "roasted",
        "smoked",
        "raw",
        "sautéed",
        "baked",
        "cured",
    }
)

_ALLOWED_WEIGHTS: frozenset[str] = frozenset({"light", "medium", "heavy"})

_ALLOWED_CUISINES: frozenset[str] = frozenset(
    {
        "French",
        "Italian",
        "Swiss",
        "Indian",
        "Spanish",
        "Japanese",
        "Chinese",
        "American",
        "British",
        "Mexican",
        "German",
        "Middle_Eastern",
        "Korean",
        "Thai",
        "Greek",
        "Vietnamese",
        "Austrian",
        "African",
        "Scandinavian",
        "Portuguese",
    }
)

_ALLOWED_FLAVOURS: frozenset[str] = frozenset(
    {
        "savory",
        "rich",
        "spicy",
        "creamy",
        "smoky",
        "earthy",
        "herbal",
        "tangy",
        "sweet",
        "umami",
    }
)


# ---------------------------------------------------------------------------
# Group conflict pairs (used by merge + validation)
# ---------------------------------------------------------------------------

_GROUP_CONFLICTS: list[tuple[str, str]] = [
    # Weight extremes
    ("heavy", "light"),
    # Protein mutual exclusivity
    ("red_meat", "vegetarian"),
    ("red_meat", "fish"),
    ("pork", "vegetarian"),
    ("game", "vegetarian"),
    ("poultry", "vegetarian"),
    ("fish", "vegetarian"),
]


def _conflicts_with_accepted(group: str, accepted: list[str]) -> bool:
    """Return True if *group* conflicts with any already-accepted group."""
    for a, b in _GROUP_CONFLICTS:
        if group == a and b in accepted:
            return True
        if group == b and a in accepted:
            return True
    return False


def derive_food_groups(
    dish_ids: list[str],
    con: object,
    *,
    threshold: float = 0.4,
    max_groups: int = 10,
) -> list[str]:
    """Derive food-group tags by aggregating catalogue metadata for resolved dishes.

    Looks up each dish_id in the ``food_catalogue`` table, collects the
    ``protein``, ``cooking_method``, ``weight_class``, ``cuisine``, and
    ``flavour_profile`` values, then returns groups that appear in at
    least ``threshold`` fraction of the resolved dishes.

    Flavour profile values are weighted at 0.5 (secondary signal).

    Returns at most ``max_groups`` results.

    Examples:
        >>> # Given dishes: duck-confit (poultry/heavy/French) + beef-bourguignon (red_meat/heavy/French)
        >>> derive_food_groups(["duck-confit", "beef-bourguignon"], con)
        ['heavy', 'French', 'rich', 'savory', 'poultry', 'red_meat']
    """
    if not dish_ids:
        return []

    from collections import Counter

    placeholders = ", ".join("?" for _ in dish_ids)
    sql = (
        f"SELECT protein, cooking_method, weight_class, cuisine, flavour_profile "
        f"FROM food_catalogue WHERE dish_id IN ({placeholders})"
    )
    try:
        rows = con.execute(sql, dish_ids).fetchall()  # type: ignore[union-attr]
    except Exception:
        return []

    if not rows:
        return []

    counts: Counter[str] = Counter()
    for protein, method, weight, cuisine, flavours in rows:
        if protein and protein in _ALLOWED_PROTEINS:
            counts[protein] += 1
        if method and method in _ALLOWED_METHODS:
            counts[method] += 1
        if weight and weight in _ALLOWED_WEIGHTS:
            counts[weight] += 1
        if cuisine:
            normalised = cuisine.replace(" ", "_")
            if normalised in _ALLOWED_CUISINES:
                counts[normalised] += 1
        for flav in flavours or []:
            if flav in _ALLOWED_FLAVOURS:
                counts[flav] += 0.5  # secondary signal

    min_count = max(1, len(rows) * threshold)
    groups = [group for group, count in counts.most_common() if count >= min_count]
    return groups[:max_groups]


def merge_food_groups(
    dish_groups: list[str],
    prose_groups: list[str],
    *,
    max_groups: int = 8,
) -> list[str]:
    """Merge catalogue-derived and prose-extracted groups by relevance score.

    Scoring:
    - Groups from both sources:        score = 3  (corroborated)
    - Groups from catalogue only:      score = 2  (backed by data)
    - Groups from prose only:          score = 1  (keyword signal only)

    Conflict-aware: groups that conflict with a higher-scored accepted group
    are skipped (see ``_GROUP_CONFLICTS``).

    Returns at most ``max_groups`` results, ordered by score descending,
    then by first-occurrence order within each score tier.
    """
    dish_set = set(dish_groups)
    prose_set = set(prose_groups)
    all_groups = list(dict.fromkeys(dish_groups + prose_groups))

    def _score(group: str) -> int:
        in_dish = group in dish_set
        in_prose = group in prose_set
        if in_dish and in_prose:
            return 3
        if in_dish:
            return 2
        return 1

    all_groups.sort(key=_score, reverse=True)

    # Conflict-aware selection: skip groups conflicting with higher-scored ones
    accepted: list[str] = []
    for group in all_groups:
        if len(accepted) >= max_groups:
            break
        if _conflicts_with_accepted(group, accepted):
            continue
        accepted.append(group)

    return accepted


# ---------------------------------------------------------------------------
# Post-derivation validation
# ---------------------------------------------------------------------------


def validate_food_data(
    food_tags: list[str],
    food_groups: list[str],
    prose: str,
    con: object,
    *,
    wine_sweetness: str | None = None,
) -> tuple[list[str], list[str]]:
    """Post-derivation validation pass.

    Removes food_tags and food_groups that contradict the prose or wine context.

    Rules:
    1. If prose contains negation + food keyword, remove that group.
    2. If wine is sweet/dessert, remove red_meat/braised/roasted groups
       and filter tags with excluded proteins.
    """
    # Rule 1: Detect negated keywords in prose
    negated_groups = _extract_negated_groups(prose)
    food_groups = [g for g in food_groups if g not in negated_groups]

    # Rule 2: Sweet wine exclusions
    if wine_sweetness in ("sweet", "dessert"):
        sweet_excludes = {"red_meat", "braised", "roasted", "pork", "game"}
        food_groups = [g for g in food_groups if g not in sweet_excludes]

        # Also filter tags
        if con and food_tags:
            food_tags = _filter_by_wine_context(
                food_tags,
                con,
                _WINE_CATEGORY_FILTERS["sweet"],
            )

    # Rule 3: Remove tags for negated proteins
    negated_proteins = negated_groups & _ALLOWED_PROTEINS
    if negated_proteins and con and food_tags:
        food_tags = _filter_tags_by_protein(food_tags, con, negated_proteins)

    # Rule 4: Mutual-exclusivity conflicts (weight + protein)
    food_groups = _resolve_group_conflicts(food_groups)

    return food_tags, food_groups


def _resolve_group_conflicts(groups: list[str]) -> list[str]:
    """Remove lower-priority groups that conflict with earlier ones.

    Groups are assumed to arrive in priority order (from merge_food_groups
    scoring).  When a conflict pair is detected, the later entry is dropped.
    """
    accepted: list[str] = []
    for group in groups:
        if _conflicts_with_accepted(group, accepted):
            continue
        accepted.append(group)
    return accepted


def _extract_negated_groups(prose: str) -> set[str]:
    """Find food groups that appear in negated context in the prose."""
    words = re.findall(r"\b[\w'-]+\b", prose.lower())
    negated: set[str] = set()
    suppressed: set[int] = set()

    for i, word in enumerate(words):
        if word in _NEGATION_WORDS:
            for j in range(i, min(i + _NEGATION_WINDOW + 1, len(words))):
                suppressed.add(j)

    for i in suppressed:
        if i < len(words):
            group = FOOD_GROUP_KEYWORDS.get(words[i])
            if group:
                negated.add(group)

    return negated


def _filter_tags_by_protein(
    dish_ids: list[str],
    con: object,
    excluded_proteins: set[str],
) -> list[str]:
    """Remove tags whose protein matches the exclusion set."""
    if not dish_ids:
        return dish_ids

    placeholders = ", ".join("?" for _ in dish_ids)
    sql = f"SELECT dish_id, protein FROM food_catalogue WHERE dish_id IN ({placeholders})"
    try:
        rows = con.execute(sql, dish_ids).fetchall()  # type: ignore[union-attr]
    except Exception:
        return dish_ids

    excluded = {did for did, protein in rows if protein in excluded_proteins}
    return [did for did in dish_ids if did not in excluded]


# ---------------------------------------------------------------------------
# Food-group extraction from prose
# ---------------------------------------------------------------------------

FOOD_GROUP_KEYWORDS: dict[str, str] = {
    # Protein groups
    "game": "game",
    "meat": "red_meat",
    "meats": "red_meat",
    "beef": "red_meat",
    "lamb": "red_meat",
    "steak": "red_meat",
    "venison": "game",
    "veal": "red_meat",
    "poultry": "poultry",
    "chicken": "poultry",
    "duck": "poultry",
    "turkey": "poultry",
    "fish": "fish",
    "salmon": "fish",
    "tuna": "fish",
    "seafood": "seafood",
    "lobster": "seafood",
    "shrimp": "seafood",
    "oyster": "seafood",
    "pork": "pork",
    "cheese": "cheese",
    "fondue": "cheese",
    "raclette": "cheese",
    "gruyère": "cheese",
    "parmesan": "cheese",
    "comté": "cheese",
    "vegetarian": "vegetarian",
    "vegan": "vegan",
    # Cooking styles
    "bbq": "grilled",
    "barbecue": "grilled",
    "grill": "grilled",
    "grilled": "grilled",
    "braised": "braised",
    "roasted": "roasted",
    "stewed": "stewed",
    "fried": "fried",
    "smoked": "smoked",
    "raw": "raw",
    "cured": "cured",
    "charcuterie": "cured",
    # Weight indicators
    "rich": "heavy",
    "hearty": "heavy",
    "heavy": "heavy",
    "light": "light",
    "delicate": "light",
    # Cuisine indicators
    "french": "French",
    "italian": "Italian",
    "swiss": "Swiss",
    "japanese": "Japanese",
    "asian": "Japanese",
    "indian": "Indian",
    "spanish": "Spanish",
    "mexican": "Mexican",
    "thai": "Thai",
    "chinese": "Chinese",
    "mediterranean": "Italian",
    "greek": "Greek",
}


_NEGATION_WORDS: frozenset[str] = frozenset(
    {
        "avoid",
        "not",
        "don't",
        "never",
        "without",
        "skip",
        "no",
    }
)

_NEGATION_WINDOW = 5  # words after negation to suppress

# Cuisine keywords that commonly appear as dish adjectives rather than
# indicating wine-level cuisine affinity.  Require ≥2 hits to include.
_CUISINE_GROUPS: frozenset[str] = frozenset(
    {
        "French",
        "Italian",
        "Swiss",
        "Japanese",
        "Indian",
        "Spanish",
        "Mexican",
        "Thai",
        "Chinese",
        "Greek",
    }
)


def extract_food_groups(
    prose: str,
    *,
    min_cuisine_hits: int = 2,
) -> list[str]:
    """Extract food-group labels from prose using keyword matching.

    Respects negation context: keywords within 5 words of a negation
    word (avoid, not, never, etc.) are suppressed.

    Returns groups ordered by frequency (most hits first).  Cuisine-type
    groups (French, Swiss, etc.) require at least ``min_cuisine_hits``
    keyword occurrences to be included — this prevents a single dish
    adjective like "French onion soup" from tagging the wine as French.

    Examples:
        >>> extract_food_groups("Excellent with game dishes and grilled meats")
        ['game', 'grilled', 'red_meat']
        >>> extract_food_groups("A versatile food wine")
        []
    """
    from collections import Counter

    words = re.findall(r"\b[\w'-]+\b", prose.lower())

    # Build negation windows
    suppressed: set[int] = set()
    for i, word in enumerate(words):
        if word in _NEGATION_WORDS:
            for j in range(i, min(i + _NEGATION_WINDOW + 1, len(words))):
                suppressed.add(j)

    counts: Counter[str] = Counter()
    for i, word in enumerate(words):
        if i in suppressed:
            continue
        group = FOOD_GROUP_KEYWORDS.get(word)
        if group:
            counts[group] += 1

    # Apply cuisine minimum-hit filter
    groups: list[str] = []
    for group, hits in counts.most_common():
        if group in _CUISINE_GROUPS and hits < min_cuisine_hits:
            continue
        groups.append(group)

    return groups
