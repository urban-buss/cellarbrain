"""Build embedding text strings from wine and food metadata."""

from __future__ import annotations


def build_wine_text(
    *,
    full_name: str,
    country: str | None = None,
    region: str | None = None,
    grape_summary: str | None = None,
    category: str | None = None,
    tasting_notes: str | None = None,
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
