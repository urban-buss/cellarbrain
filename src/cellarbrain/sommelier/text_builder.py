"""Build embedding text strings from wine and food metadata."""

from __future__ import annotations

import re

# Maps stored category codes (from wines_full) to the vocabulary form
# used during model training (build_pairings.py).
CATEGORY_DISPLAY: dict[str, str] = {
    "red": "Red wine",
    "white": "White wine",
    "rose": "Rosé wine",
}


def normalise_category(category: str | None) -> str | None:
    """Map stored category codes to training-vocabulary form.

    Examples:
        >>> normalise_category("red")
        'Red wine'
        >>> normalise_category(None) is None
        True
    """
    if category is None:
        return None
    return CATEGORY_DISPLAY.get(category.lower(), category)


_WINE_DESC_RE = re.compile(
    r"## Wine Description\n"
    r"<!-- source: agent:research -->\n"
    r"(.*?)\n"
    r"<!-- source: agent:research — end -->",
    re.DOTALL,
)


def extract_tasting_summary(dossier_text: str, max_chars: int = 200) -> str | None:
    """Extract a condensed tasting summary from dossier Wine Description.

    Returns the first ~max_chars of prose (breaking at a sentence boundary),
    stripped of markdown headers and formatting.  Returns None if the section
    is empty or still pending research.

    Examples:
        >>> extract_tasting_summary("## Wine Description\\n<!-- source: agent:research -->\\nRich and bold.\\n<!-- source: agent:research — end -->")
        'Rich and bold.'
    """
    m = _WINE_DESC_RE.search(dossier_text)
    if not m:
        return None
    prose = m.group(1).strip()
    if not prose or "Pending agent action" in prose:
        return None
    # Strip markdown headers that sometimes appear inside the section
    prose = re.sub(r"^##?\s+.*\n?", "", prose, flags=re.MULTILINE).strip()
    if not prose:
        return None
    if len(prose) <= max_chars:
        return prose
    cut = prose[:max_chars].rfind(".")
    if cut > 50:
        return prose[: cut + 1]
    return prose[:max_chars]


def build_wine_text(
    *,
    full_name: str,
    country: str | None = None,
    region: str | None = None,
    grape_summary: str | None = None,
    category: str | None = None,
    tasting_notes: str | None = None,
    food_pairings: str | None = None,
    food_groups: str | None = None,
) -> str:
    """Assemble a wine's embedding text from its metadata.

    The resulting string is what gets encoded by the sommelier model.
    Order matters for the encoder — most distinctive info first.

    Examples:
        >>> build_wine_text(full_name="Château Musar Rouge 2018",
        ...     country="Lebanon", region="Bekaa Valley",
        ...     grape_summary="Cinsault, Carignan, Cabernet Sauvignon",
        ...     category="Red wine")
        'Château Musar Rouge 2018, Bekaa Valley, Lebanon. Cinsault, Carignan, Cabernet Sauvignon. Red wine.'
        >>> build_wine_text(full_name="Test Wine 2020",
        ...     food_pairings="duck-confit, raclette, beef-bourguignon")
        'Test Wine 2020. Pairs with: duck-confit, raclette, beef-bourguignon.'
    """
    parts: list[str] = [full_name]
    if region:
        parts.append(region)
    if country:
        parts.append(country)
    location = ", ".join(parts)

    segments = [location]
    if grape_summary:
        segments.append(grape_summary)
    if category:
        segments.append(category)
    if tasting_notes:
        segments.append(tasting_notes)
    if food_pairings:
        segments.append(f"Pairs with: {food_pairings}")
    if food_groups:
        segments.append(f"Food groups: {food_groups}")

    return ". ".join(segments) + "."


def build_food_text(
    *,
    dish_name: str,
    description: str | None = None,
    ingredients: list[str] | None = None,
    cuisine: str | None = None,
    weight_class: str | None = None,
    protein: str | None = None,
    flavour_profile: list[str] | None = None,
) -> str:
    """Assemble a dish's embedding text from its catalogue metadata.

    Examples:
        >>> build_food_text(dish_name="Beef Bourguignon",
        ...     description="Braised beef in red wine sauce with mushrooms",
        ...     ingredients=["beef", "red wine", "mushrooms"],
        ...     cuisine="French", weight_class="heavy", protein="red_meat",
        ...     flavour_profile=["earthy", "rich", "herbal"])
        'Beef Bourguignon — Braised beef in red wine sauce with mushrooms. Ingredients: beef, red wine, mushrooms. Weight: heavy. Protein: red_meat. Cuisine: French. Flavours: earthy, rich, herbal.'
    """
    parts: list[str] = []

    header = dish_name
    if description:
        header += f" — {description}"
    parts.append(header)

    if ingredients is not None and len(ingredients) > 0:
        parts.append(f"Ingredients: {', '.join(ingredients)}")
    if weight_class:
        parts.append(f"Weight: {weight_class}")
    if protein:
        parts.append(f"Protein: {protein}")
    if cuisine:
        parts.append(f"Cuisine: {cuisine}")
    if flavour_profile is not None and len(flavour_profile) > 0:
        parts.append(f"Flavours: {', '.join(flavour_profile)}")

    return ". ".join(parts) + "."
