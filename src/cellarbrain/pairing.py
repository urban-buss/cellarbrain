"""RAG-based food-pairing retrieval — multi-strategy SQL candidate selection.

Combines multiple retrieval strategies (category, grape, food-tag, food-group,
region-affinity) into a single ranked candidate list. No ML dependencies —
works with base DuckDB data from the wines_full view.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import duckdb

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PairingCandidate:
    """A wine candidate from RAG retrieval with relevance signals."""

    wine_id: int
    wine_name: str
    vintage: int | None
    category: str
    country: str | None
    region: str | None
    primary_grape: str | None
    bottles_stored: int
    price: float | None
    drinking_status: str | None
    best_pro_score: float | None
    match_signals: list[str] = field(default_factory=list)
    signal_count: int = 0


# --- Protein → wine category inference ------------------------------------

PROTEIN_CATEGORIES: dict[str, list[str]] = {
    "red_meat": ["Red wine"],
    "game": ["Red wine"],
    "pork": ["Red wine", "White wine"],
    "poultry": ["Red wine", "White wine"],
    "fish": ["White wine"],
    "seafood": ["White wine"],
    "vegetarian": ["Red wine", "White wine"],
    "cheese": ["White wine", "Red wine"],
}

# --- Cuisine → country/region mapping -------------------------------------

CUISINE_REGIONS: dict[str, list[str]] = {
    "French": ["France"],
    "Italian": ["Italy"],
    "Spanish": ["Spain"],
    "Swiss": ["Switzerland"],
    "Argentine": ["Argentina"],
    "South African": ["South Africa"],
    "Portuguese": ["Portugal"],
    "German": ["Germany"],
    "Austrian": ["Austria"],
    "Chilean": ["Chile"],
    "Australian": ["Australia"],
}

# --- Grape lookup by protein + weight --------------------------------------

CATEGORY_GRAPES: dict[str, list[str]] = {
    "red_meat_heavy": [
        "Cabernet Sauvignon",
        "Merlot",
        "Syrah",
        "Malbec",
        "Mourvèdre",
        "Petit Verdot",
        "Lagrein",
        "Cabernet Franc",
    ],
    "red_meat_medium": [
        "Merlot",
        "Cabernet Franc",
        "Grenache",
        "Mourvèdre",
        "Tempranillo",
        "Syrah",
        "Sangiovese",
    ],
    "game": [
        "Nebbiolo",
        "Syrah",
        "Mourvèdre",
        "Pinot Noir",
        "Cabernet Sauvignon",
        "Cornalin",
    ],
    "poultry": [
        "Pinot Noir",
        "Barbera",
        "Merlot",
        "Chardonnay",
        "Chenin Blanc",
        "Sauvignon Blanc",
    ],
    "fish": [
        "Sauvignon Blanc",
        "Chasselas",
        "Arneis",
        "Nascetta",
        "Pinot Gris",
        "Riesling",
        "Chardonnay",
    ],
    "seafood": [
        "Sauvignon Blanc",
        "Chasselas",
        "Arneis",
        "Riesling",
        "Pinot Gris",
        "Chardonnay",
    ],
    "pork": [
        "Pinot Noir",
        "Grenache",
        "Barbera",
        "Merlot",
        "Chardonnay",
        "Chenin Blanc",
    ],
    "vegetarian_heavy": [
        "Nebbiolo",
        "Sangiovese",
        "Pinot Noir",
        "Chardonnay",
        "Grenache",
        "Barbera",
    ],
    "vegetarian_light": [
        "Sauvignon Blanc",
        "Chasselas",
        "Pinot Gris",
        "Arneis",
    ],
    "cheese": [
        "Chasselas",
        "Nebbiolo",
        "Cabernet Sauvignon",
        "Tempranillo",
        "Sangiovese",
        "Pinot Noir",
    ],
    "cheese_light": [
        "Chasselas",
        "Sauvignon Blanc",
        "Arneis",
        "Pinot Gris",
    ],
    "cheese_medium": [
        "Chasselas",
        "Pinot Noir",
        "Chenin Blanc",
        "Chardonnay",
        "Nebbiolo",
        "Sangiovese",
    ],
    "cheese_heavy": [
        "Nebbiolo",
        "Cabernet Sauvignon",
        "Tempranillo",
        "Sangiovese",
        "Pinot Noir",
    ],
    "spicy": [
        "Gewürztraminer",
        "Riesling",
        "Chenin Blanc",
        "Sauvignon Blanc",
    ],
}

# --- Category normalisation ------------------------------------------------

_CATEGORY_NORM: dict[str, str] = {
    "red": "Red wine",
    "white": "White wine",
    "rose": "Rosé",
    "rosé": "Rosé",
    "sparkling": "Sparkling wine",
    "sweet": "Sweet wine",
}


def _normalise_category(cat: str | None) -> str | None:
    """Normalise short category to wines_full column value."""
    if cat is None:
        return None
    return _CATEGORY_NORM.get(cat.lower(), cat)


# --- Base filter CTE -------------------------------------------------------

_BASE_CTE = """\
WITH base AS (
    SELECT wine_id, wine_name, vintage, category, country, region,
           primary_grape, bottles_stored, price, drinking_status,
           best_pro_score, food_tags, food_groups
    FROM wines_full
    WHERE drinking_status IN ('optimal', 'drinkable')
      AND bottles_stored > 0
)"""


# --- Strategy implementations ---------------------------------------------


def _strategy_category(
    con: duckdb.DuckDBPyConnection,
    category: str,
) -> dict[int, list[str]]:
    """Strategy 1: Filter by wine category."""
    sql = f"{_BASE_CTE}\nSELECT wine_id FROM base WHERE category = ?"
    try:
        rows = con.execute(sql, [category]).fetchall()
    except Exception:
        return {}
    return {r[0]: ["category"] for r in rows}


def _strategy_grapes(
    con: duckdb.DuckDBPyConnection,
    grapes: list[str],
) -> dict[int, list[str]]:
    """Strategy 2: Filter by target grape varieties."""
    if not grapes:
        return {}
    placeholders = ", ".join("?" for _ in grapes)
    sql = f"{_BASE_CTE}\nSELECT wine_id, primary_grape FROM base WHERE primary_grape IN ({placeholders})"
    try:
        rows = con.execute(sql, grapes).fetchall()
    except Exception:
        return {}
    return {r[0]: [f"grape:{r[1]}"] for r in rows}


def _strategy_food_tags(
    con: duckdb.DuckDBPyConnection,
    keywords: list[str],
) -> dict[int, list[str]]:
    """Strategy 3: Search food_tags array for matching keywords."""
    if not keywords:
        return {}
    sql = f"""{_BASE_CTE}
SELECT wine_id, food_tags FROM base
WHERE food_tags IS NOT NULL AND len(food_tags) > 0
  AND list_has_any(food_tags, ?)"""
    try:
        rows = con.execute(sql, [keywords]).fetchall()
    except Exception:
        return {}
    results: dict[int, list[str]] = {}
    for wine_id, tags in rows:
        matched = [t for t in (tags or []) if t in keywords]
        results[wine_id] = [f"food_tag:{m}" for m in matched[:3]]
    return results


def _strategy_food_groups(
    con: duckdb.DuckDBPyConnection,
    groups: list[str],
) -> dict[int, list[str]]:
    """Strategy 4: Search food_groups array for matching group tags."""
    if not groups:
        return {}
    sql = f"""{_BASE_CTE}
SELECT wine_id, food_groups FROM base
WHERE food_groups IS NOT NULL AND len(food_groups) > 0
  AND list_has_any(food_groups, ?)"""
    try:
        rows = con.execute(sql, [groups]).fetchall()
    except Exception:
        return {}
    results: dict[int, list[str]] = {}
    for wine_id, grps in rows:
        matched = [g for g in (grps or []) if g in groups]
        results[wine_id] = [f"food_group:{m}" for m in matched[:3]]
    return results


def _strategy_region(
    con: duckdb.DuckDBPyConnection,
    cuisine: str,
) -> dict[int, list[str]]:
    """Strategy 5: Region affinity — cuisine maps to country."""
    countries = CUISINE_REGIONS.get(cuisine)
    if not countries:
        return {}
    placeholders = ", ".join("?" for _ in countries)
    sql = f"{_BASE_CTE}\nSELECT wine_id FROM base WHERE country IN ({placeholders})"
    try:
        rows = con.execute(sql, countries).fetchall()
    except Exception:
        return {}
    return {r[0]: ["region"] for r in rows}


# --- Keyword extraction from dish description ------------------------------

# --- Server-side dish classification (rule-based) -------------------------

_PROTEIN_KEYWORDS: dict[str, list[str]] = {
    "red_meat": [
        "beef",
        "steak",
        "lamb",
        "veal",
        "brisket",
        "ribeye",
        "sirloin",
        "filet",
        "burger",
        "meatball",
        "ragù",
        "ragu",
        "bolognese",
        "osso buco",
        "bresaola",
        "bündnerfleisch",
        "entrecôte",
        "chateaubriand",
        "tartare",
    ],
    "poultry": [
        "chicken",
        "turkey",
        "duck",
        "quail",
        "guinea",
        "poultry",
        "geschnetzeltes",
        "coq au vin",
        "magret",
    ],
    "fish": [
        "fish",
        "salmon",
        "tuna",
        "bass",
        "trout",
        "sole",
        "cod",
        "halibut",
        "swordfish",
        "anchovy",
        "sardine",
    ],
    "seafood": [
        "shrimp",
        "prawn",
        "lobster",
        "crab",
        "oyster",
        "mussel",
        "clam",
        "scallop",
        "calamari",
        "squid",
        "octopus",
        "sushi",
        "sashimi",
        "ceviche",
    ],
    "pork": [
        "pork",
        "ham",
        "bacon",
        "prosciutto",
        "pancetta",
        "sausage",
        "bratwurst",
        "schnitzel",
        "charcuterie",
    ],
    "game": [
        "venison",
        "boar",
        "rabbit",
        "hare",
        "pheasant",
        "pigeon",
        "wild",
    ],
    "cheese": [
        "cheese",
        "raclette",
        "fondue",
        "gruyère",
        "camembert",
        "roquefort",
        "parmesan",
        "mozzarella",
        "brie",
        "goat cheese",
        "aged cheese",
        "comté",
        "emmental",
    ],
    "vegetarian": [
        "salad",
        "vegetable",
        "mushroom",
        "truffle",
        "risotto",
        "pasta",
        "pizza",
        "tofu",
        "lentil",
        "bean",
        "eggplant",
        "aubergine",
        "ratatouille",
    ],
}

_WEIGHT_KEYWORDS: dict[str, list[str]] = {
    "heavy": [
        "braised",
        "stew",
        "grilled steak",
        "bbq",
        "aged cheese",
        "ragù",
        "bolognese",
        "confit",
        "bourguignon",
        "osso buco",
        "venison",
        "boar",
        "brisket",
        "cassoulet",
        "roast beef",
        "roast lamb",
        "roast pork",
    ],
    "light": [
        "salad",
        "sashimi",
        "carpaccio",
        "tartare",
        "steamed",
        "poached",
        "raw",
        "ceviche",
        "consommé",
        "broth",
    ],
    # "medium" is the default
}

_CUISINE_KEYWORDS: dict[str, list[str]] = {
    "Swiss": [
        "raclette",
        "fondue",
        "rösti",
        "geschnetzeltes",
        "bündnerfleisch",
        "capuns",
        "älplermagronen",
    ],
    "French": [
        "confit",
        "bourguignon",
        "bouillabaisse",
        "coq au vin",
        "ratatouille",
        "quiche",
        "cassoulet",
        "foie gras",
        "béarnaise",
        "provençal",
    ],
    "Italian": [
        "pasta",
        "risotto",
        "pizza",
        "osso buco",
        "ragù",
        "bolognese",
        "carbonara",
        "tiramisu",
        "bruschetta",
        "lasagna",
        "gnocchi",
    ],
    "Japanese": [
        "sushi",
        "sashimi",
        "ramen",
        "tempura",
        "teriyaki",
        "miso",
        "yakitori",
    ],
    "Indian": [
        "curry",
        "tikka",
        "tandoori",
        "masala",
        "biryani",
        "naan",
        "samosa",
        "dal",
    ],
    "Thai": ["thai", "pad thai", "green curry", "tom yum", "satay"],
    "Argentine": ["asado", "empanada", "chimichurri"],
    "Spanish": ["tapas", "paella", "jamón", "chorizo"],
    "Greek": ["souvlaki", "moussaka", "gyros", "tzatziki"],
    "Lebanese": ["shawarma", "falafel", "hummus", "tabbouleh"],
}


@dataclass(frozen=True)
class DishClassification:
    """Result of rule-based dish classification."""

    protein: str | None
    weight: str
    category: str | None
    cuisine: str | None


def classify_dish(dish: str) -> DishClassification:
    """Rule-based dish classification for server-side parameter inference.

    Matches keywords in the dish description to infer protein type, weight
    class, wine category, and cuisine origin. Falls back gracefully — never
    raises.

    Examples:
        >>> classify_dish("grilled lamb chops")
        DishClassification(protein='red_meat', weight='medium', ...)
        >>> classify_dish("raclette with pickles")
        DishClassification(protein='cheese', weight='medium', cuisine='Swiss', ...)
    """
    lower = dish.lower()

    protein: str | None = None
    for p, keywords in _PROTEIN_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            protein = p
            break

    weight = "medium"  # default
    for w, keywords in _WEIGHT_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            weight = w
            break

    cuisine: str | None = None
    for c, keywords in _CUISINE_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            cuisine = c
            break

    # Infer category from protein
    category: str | None = None
    if protein:
        cats = PROTEIN_CATEGORIES.get(protein, [])
        if cats:
            category = _CATEGORY_NORM.get(cats[0].split()[0].lower())

    return DishClassification(
        protein=protein,
        weight=weight,
        category=category,
        cuisine=cuisine,
    )


# --- Keyword extraction from dish description ------------------------------

_WORD_RE = re.compile(r"[a-zA-ZÀ-ÿ]+")


def _extract_keywords(dish_description: str) -> list[str]:
    """Extract food-tag-style keywords from dish description.

    Converts to slug format (lowercase, hyphenated) for matching against
    the food_tags array which contains dish_id slugs.
    """
    if not dish_description:
        return []
    words = _WORD_RE.findall(dish_description.lower())
    # Build 1-word and 2-word slug candidates
    slugs: list[str] = []
    for w in words:
        if len(w) >= 3:
            slugs.append(w)
    for i in range(len(words) - 1):
        if len(words[i]) >= 3 and len(words[i + 1]) >= 3:
            slugs.append(f"{words[i]}-{words[i + 1]}")
    return list(dict.fromkeys(slugs))  # deduplicate preserving order


# --- Grape inference -------------------------------------------------------


def _infer_grapes(protein: str | None, weight: str | None) -> list[str]:
    """Infer target grapes from protein + weight classification."""
    if not protein:
        return []
    # Try specific key first
    if weight:
        key = f"{protein}_{weight}"
        if key in CATEGORY_GRAPES:
            return CATEGORY_GRAPES[key]
    # Fall back to protein-only key
    if protein in CATEGORY_GRAPES:
        return CATEGORY_GRAPES[protein]
    return []


# --- Main retrieval function -----------------------------------------------


def retrieve_candidates(
    con: duckdb.DuckDBPyConnection,
    *,
    dish_description: str | None = None,
    category: str | None = None,
    weight: str | None = None,
    protein: str | None = None,
    cuisine: str | None = None,
    grapes: list[str] | None = None,
    limit: int = 15,
) -> list[PairingCandidate]:
    """Multi-strategy retrieval for food-pairing candidates.

    Combines results from multiple SQL strategies, deduplicates by wine_id,
    and ranks by number of matching signals (more signals = better match).

    Strategies (ORed together):
    1. Category filter (red/white/rose based on protein or explicit param)
    2. Grape filter (target grapes from pairing rules)
    3. Food tag search (dish_description keywords against food_tags array)
    4. Food group search (protein/weight/cuisine against food_groups array)
    5. Region affinity (cuisine → country mapping)

    All strategies constrain to: drinking_status IN ('optimal', 'drinkable')
    AND bottles_stored > 0.

    When *protein*, *category*, and *grapes* are all ``None`` the engine
    auto-classifies the dish server-side using :func:`classify_dish` so
    that callers (including small LLMs) can pass only *dish_description*.
    """
    # --- Auto-classify when caller provides no structured params -----------
    if not protein and not category and not grapes and dish_description:
        cls = classify_dish(dish_description)
        protein = cls.protein
        weight = weight or cls.weight
        category = cls.category
        cuisine = cuisine or cls.cuisine

    signals: dict[int, list[str]] = {}

    # Strategy 1: Category filter
    norm_category = _normalise_category(category)
    if norm_category:
        categories = [norm_category]
    elif protein:
        categories = PROTEIN_CATEGORIES.get(protein, [])
    else:
        categories = []

    for cat in categories:
        for wine_id, sigs in _strategy_category(con, cat).items():
            signals.setdefault(wine_id, []).extend(sigs)

    # Strategy 2: Grape filter
    target_grapes = grapes if grapes else _infer_grapes(protein, weight)
    if target_grapes:
        for wine_id, sigs in _strategy_grapes(con, target_grapes).items():
            signals.setdefault(wine_id, []).extend(sigs)

    # Strategy 3: Food tag keyword search
    keywords = _extract_keywords(dish_description or "")
    if keywords:
        for wine_id, sigs in _strategy_food_tags(con, keywords).items():
            signals.setdefault(wine_id, []).extend(sigs)

    # Strategy 4: Food group search
    groups: list[str] = []
    if protein:
        groups.append(protein)
    if weight:
        groups.append(weight)
    if cuisine:
        groups.append(cuisine)
    if groups:
        for wine_id, sigs in _strategy_food_groups(con, groups).items():
            signals.setdefault(wine_id, []).extend(sigs)

    # Strategy 5: Region affinity
    if cuisine:
        for wine_id, sigs in _strategy_region(con, cuisine).items():
            signals.setdefault(wine_id, []).extend(sigs)

    if not signals:
        return []

    return _merge_and_rank(signals, con, limit)


def _merge_and_rank(
    signals: dict[int, list[str]],
    con: duckdb.DuckDBPyConnection,
    limit: int = 15,
) -> list[PairingCandidate]:
    """Merge signals per wine_id, fetch metadata, rank by signal_count.

    Ranking order:
    1. signal_count DESC (more matching strategies = better)
    2. best_pro_score DESC NULLS LAST
    """
    if not signals:
        return []

    # Sort by signal count to get top candidates
    ranked_ids = sorted(signals.keys(), key=lambda wid: len(signals[wid]), reverse=True)
    top_ids = ranked_ids[: limit * 2]  # fetch extra for tie-breaking

    # Fetch metadata for top candidates
    placeholders = ", ".join("?" for _ in top_ids)
    sql = f"""{_BASE_CTE}
SELECT wine_id, wine_name, vintage, category, country, region,
       primary_grape, bottles_stored, price, drinking_status, best_pro_score
FROM base WHERE wine_id IN ({placeholders})"""

    try:
        rows = con.execute(sql, top_ids).fetchall()
    except Exception:
        logger.warning("Failed to fetch candidate metadata", exc_info=True)
        return []

    # Build candidates with metadata
    meta_map = {r[0]: r for r in rows}
    candidates: list[PairingCandidate] = []
    for wine_id in ranked_ids:
        if wine_id not in meta_map:
            continue
        r = meta_map[wine_id]
        sigs = signals[wine_id]
        candidates.append(
            PairingCandidate(
                wine_id=r[0],
                wine_name=r[1],
                vintage=r[2],
                category=r[3],
                country=r[4],
                region=r[5],
                primary_grape=r[6],
                bottles_stored=r[7],
                price=r[8],
                drinking_status=r[9],
                best_pro_score=r[10],
                match_signals=sigs,
                signal_count=len(sigs),
            )
        )
        if len(candidates) >= limit:
            break

    # Final sort: signal_count DESC, then score DESC
    candidates.sort(
        key=lambda c: (c.signal_count, c.best_pro_score or 0),
        reverse=True,
    )
    return candidates[:limit]


# --- Output formatters -----------------------------------------------------

_SIGNAL_REASON: dict[str, str] = {
    "category": "Correct wine style for this dish",
    "region": "Regional affinity — local wine for local food",
}


def _best_reason(candidate: PairingCandidate, protein: str | None) -> str:
    """Pick the most informative reason from a candidate's signals."""
    # Priority: food_tag > food_group > grape > region > category
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
            p_label = (protein or "this protein").replace("_", " ")
            return f"{grape} is a classic pairing grape for {p_label}"
    if "region" in candidate.match_signals:
        return _SIGNAL_REASON["region"]
    if "category" in candidate.match_signals:
        return _SIGNAL_REASON["category"]
    return "Matches the dish profile"


def format_table(results: list[PairingCandidate]) -> str:
    """Format candidates as a Markdown table (default/compact output)."""
    lines = [
        "| Rank | Wine | Vintage | Category | Region | Grape | Bottles | Score | Signals |",
        "|------|------|---------|----------|--------|-------|---------|-------|---------|",
    ]
    for i, c in enumerate(results, 1):
        signals_str = ", ".join(c.match_signals[:3])
        if len(c.match_signals) > 3:
            signals_str += f" (+{len(c.match_signals) - 3})"
        lines.append(
            f"| {i} | {c.wine_name} | {c.vintage or ''} | {c.category} "
            f"| {c.region or ''} | {c.primary_grape or ''} | {c.bottles_stored} "
            f"| {c.best_pro_score or ''} | {signals_str} |"
        )
    return "\n".join(lines)


def format_compact(
    results: list[PairingCandidate],
    dish: str,
    classification: DishClassification | None = None,
) -> str:
    """Format candidates as a compressed one-line-per-wine list."""
    header_parts = [f'Found {len(results)} matches for "{dish}"']
    if classification and classification.protein:
        header_parts.append(f"({classification.protein}, {classification.weight})")
    header = " ".join(header_parts) + ":"

    lines = [header]
    for i, c in enumerate(results, 1):
        score = f"{c.best_pro_score:.0f}pts" if c.best_pro_score else "unrated"
        sigs = "+".join(s.split(":")[0] for s in c.match_signals[:3])
        lines.append(
            f"{i}. {c.wine_name} {c.vintage or ''} ({c.primary_grape or '?'}) — {score}, {c.bottles_stored}btl [{sigs}]"
        )
    return "\n".join(lines)


def format_explained(
    results: list[PairingCandidate],
    dish: str,
    classification: DishClassification | None = None,
    limit: int = 5,
) -> str:
    """Format candidates as pre-ranked recommendations with rationale.

    Designed for small LLMs that cannot rerank on their own.  The output
    is ready to present to the user verbatim.
    """
    protein = classification.protein if classification else None
    top = results[:limit]

    lines = [f'## Top Pairing Recommendations for "{dish}"\n']
    for i, c in enumerate(top, 1):
        score_str = f" | {c.best_pro_score:.0f} pts" if c.best_pro_score else ""
        location = ", ".join(filter(None, [c.region, c.country]))
        signals_str = ", ".join(c.match_signals[:4])
        reason = _best_reason(c, protein)

        lines.append(f"{i}. **{c.wine_name} {c.vintage or ''}** (wine_id: {c.wine_id}) — {c.primary_grape or 'blend'}")
        lines.append(f"   {location}{score_str} | {c.bottles_stored} bottle(s)")
        lines.append(f"   Why: {reason}")
        lines.append(f"   Matched on: {signals_str}")
        lines.append("")

    if classification and classification.protein:
        lines.append(
            f"_Classification: {classification.protein}, "
            f"{classification.weight}, "
            f"{classification.category or 'auto'}"
            f"{', ' + classification.cuisine if classification.cuisine else ''}_"
        )
    return "\n".join(lines)
