"""Compute derived wine properties from existing entity data.

Pure functions — no I/O, no side effects.  Computes ``full_name``,
``grape_type``, ``primary_grape``, and ``grape_summary`` for each wine entity.
"""

from __future__ import annotations

from decimal import Decimal

from .settings import CurrencyConfig, PriceTier, Settings, _default_classification_short, _default_price_tiers


# ---------------------------------------------------------------------------
# Classification abbreviation map  (§3.4)
# ---------------------------------------------------------------------------


def shorten_classification(
    classification: str | None,
    classification_short: dict[str, str] | None = None,
) -> str | None:
    """Return a shortened classification suitable for display names.

    Uses the *classification_short* mapping: if the classification is a key
    in the dict, return the mapped value.  If it is **not** in the dict,
    return ``None`` (omit from display name).  This replaces the former
    ``_OMIT_CLASSIFICATIONS`` set and ``_STRIP_PREFIX_MAP`` dict.

    Examples:
        "AOP / AOC"              → None  (not in map)
        "DOCG Riserva"           → "Riserva"
        "5ème Grand Cru Classé"  → "5ème Grand Cru Classé"
        "VDP.Große Lage"         → "Große Lage"
    """
    if not classification:
        return None
    if classification_short is None:
        classification_short = _default_classification_short()
    return classification_short.get(classification)


# ---------------------------------------------------------------------------
# Grape-type helpers  (§4)
# ---------------------------------------------------------------------------

def compute_grape_type(grapes: list[dict]) -> str:
    """Classify a wine's grape composition.

    Returns ``"varietal"`` (1 grape), ``"blend"`` (2+), or ``"unknown"`` (0).
    """
    n = len(grapes)
    if n == 0:
        return "unknown"
    if n == 1:
        return "varietal"
    return "blend"


def compute_primary_grape(grapes: list[dict]) -> str | None:
    """Determine the dominant grape, or None when ambiguous.

    Rules (evaluated in order):
    1. No grapes → None
    2. Single varietal → that grape
    3. Blend without percentages → first-mentioned grape
    4. Blend with percentages and one grape > 50 % → that grape
    5. Blend with percentages and no grape > 50 % → None
    """
    if not grapes:
        return None
    if len(grapes) == 1:
        return grapes[0]["grape_name"]

    has_pct = any(g.get("percentage") is not None for g in grapes)
    if not has_pct:
        return grapes[0]["grape_name"]

    for g in grapes:
        pct = g.get("percentage")
        if pct is not None and pct > 50:
            return g["grape_name"]
    return None


def compute_grape_summary(grapes: list[dict]) -> str | None:
    """Build a human-readable grape summary string.

    Examples:
        []                      → None
        [Nebbiolo]              → "Nebbiolo"
        [Merlot, Cab Franc]     → "Merlot / Cabernet Franc"
        [Syrah 70%, …]         → "Syrah blend"    (3+ with primary)
        [CS 48%, Merlot 42% …] → "Cabernet Sauvignon / Merlot / …" (3+ no primary)
    """
    if not grapes:
        return None
    if len(grapes) == 1:
        return grapes[0]["grape_name"]
    if len(grapes) == 2:
        return f"{grapes[0]['grape_name']} / {grapes[1]['grape_name']}"

    # 3+ grapes
    primary = compute_primary_grape(grapes)
    if primary:
        return f"{primary} blend"
    return f"{grapes[0]['grape_name']} / {grapes[1]['grape_name']} / \u2026"


# ---------------------------------------------------------------------------
# Grape-ambiguity detection  (§3.3a)
# ---------------------------------------------------------------------------

def build_grape_ambiguous_names(
    wines: list[dict],
    wine_grapes: list[dict],
    grape_names: dict[int, str],
) -> set[tuple[str, str]]:
    """Find (winery_name, wine_name) combos that need grape disambiguation.

    A combo is *grape-ambiguous* when the same (winery, name) pair exists for
    wines with different primary grapes.
    """
    # Build wine_id → list of grape dicts  (with grape_name injected)
    wg_by_wine: dict[int, list[dict]] = {}
    for wg in wine_grapes:
        wid = wg["wine_id"]
        gname = grape_names.get(wg["grape_id"])
        if gname is None:
            continue
        entry = {"grape_name": gname, "percentage": wg.get("percentage"), "sort_order": wg.get("sort_order", 0)}
        wg_by_wine.setdefault(wid, []).append(entry)

    # Sort each wine's grapes by sort_order so primary_grape logic is stable
    for glist in wg_by_wine.values():
        glist.sort(key=lambda g: g.get("sort_order", 0))

    # Group wines with a cuvée name by (winery_name, wine_name)
    groups: dict[tuple[str, str], set[str | None]] = {}
    for w in wines:
        wname = w.get("name")
        winery = w.get("_winery_name")
        if not wname or not winery:
            continue
        grapes_for_wine = wg_by_wine.get(w["wine_id"], [])
        primary = compute_primary_grape(grapes_for_wine)
        key = (winery, wname)
        groups.setdefault(key, set()).add(primary)

    return {k for k, primaries in groups.items() if len(primaries - {None}) >= 2}


# ---------------------------------------------------------------------------
# Full-name computation  (§3)
# ---------------------------------------------------------------------------

_MAX_FULL_NAME_LEN = 80


def compute_full_name(
    winery: str | None,
    name: str | None,
    subregion: str | None,
    classification: str | None,
    grape_type: str,
    primary_grape: str | None,
    grape_summary: str | None,
    vintage: int | None,
    is_nv: bool,
    *,
    name_needs_grape: bool = False,
    classification_short: dict[str, str] | None = None,
    max_full_name_length: int | None = None,
) -> str:
    """Build a retailer-style display name for a wine.

    See analysis/08-computed-wine-properties.md §3.3 for the decision tree.
    """
    short_class = shorten_classification(classification, classification_short)
    vintage_str = "NV" if is_nv else (str(vintage) if vintage else "")
    limit = max_full_name_length if max_full_name_length is not None else _MAX_FULL_NAME_LEN

    parts: list[str] = []

    if not winery:
        parts.append("Unknown Wine")
    else:
        parts.append(winery)

    if name:
        # Has cuvée name
        parts.append(name)
        if name_needs_grape and grape_summary:
            parts.append(grape_summary)
    else:
        # No cuvée name — need disambiguator
        if subregion:
            parts.append(subregion)
            if short_class:
                parts.append(short_class)
        elif short_class:
            parts.append(short_class)
        elif grape_type == "varietal" and primary_grape:
            parts.append(primary_grape)
        # else: winery-only fallback

    if vintage_str:
        parts.append(vintage_str)

    result = " ".join(parts)

    if len(result) > limit:
        # Truncate at word boundary
        truncated = result[:limit].rsplit(" ", 1)[0]
        result = truncated.rstrip() + "\u2026"

    return result


# ---------------------------------------------------------------------------
# Additional computed properties  (§10)
# ---------------------------------------------------------------------------


def compute_drinking_status(
    drink_from: int | None,
    drink_until: int | None,
    optimal_from: int | None,
    optimal_until: int | None,
    current_year: int,
) -> str:
    """Return a machine-readable drinking status enum string.

    Examples:
        (2030, 2045, 2035, 2040, 2026) → "too_young"
        (2020, 2045, 2035, 2040, 2026) → "drinkable"
        (2020, 2045, 2025, 2030, 2026) → "optimal"
        (2020, 2045, 2022, 2025, 2026) → "past_optimal"
        (2020, 2025, 2022, 2024, 2026) → "past_window"
        (None, None, None, None, 2026) → "unknown"
    """
    if drink_from is None and drink_until is None and optimal_from is None and optimal_until is None:
        return "unknown"
    if drink_until is not None and current_year > drink_until:
        return "past_window"
    if optimal_until is not None and current_year > optimal_until:
        return "past_optimal"
    if optimal_from is not None and optimal_until is not None and optimal_from <= current_year <= optimal_until:
        return "optimal"
    if drink_from is not None and current_year >= drink_from:
        if optimal_from is not None and current_year < optimal_from:
            return "drinkable"
        if optimal_from is None:
            return "drinkable"
    if drink_from is not None and current_year < drink_from:
        return "too_young"
    return "unknown"


def compute_age_years(vintage: int | None, current_year: int) -> int | None:
    """Return the wine's age in years, or None for NV wines.

    Examples:
        (2020, 2026) → 6
        (None, 2026) → None
    """
    if vintage is None:
        return None
    return current_year - vintage


def compute_price_tier(
    price: Decimal | None,
    tiers: tuple[PriceTier, ...],
) -> str:
    """Categorise a wine by catalog price into a tier label.

    Iterates the ordered tier list and returns the label of the first
    tier whose ``max`` is >= the price.  The last tier (with ``max = None``)
    is the catch-all.  Wines without price data return ``"unknown"``.

    Examples:
        (Decimal("12.50"), default_tiers) → "budget"
        (Decimal("27.00"), default_tiers) → "everyday"
        (Decimal("38.00"), default_tiers) → "premium"
        (Decimal("120"),   default_tiers) → "fine"
        (None,             default_tiers) → "unknown"
    """
    if price is None:
        return "unknown"
    for tier in tiers:
        if tier.max is None or float(price) <= tier.max:
            return tier.label
    return "unknown"


def compute_is_onsite(
    cellar_name: str | None,
    offsite_cellars: tuple[str, ...],
    in_transit_cellars: tuple[str, ...] = (),
) -> bool:
    """Return whether a bottle is physically onsite (accessible).

    A bottle is NOT onsite if its cellar is in *offsite_cellars*
    or *in_transit_cellars*.

    Examples:
        ("Main cellar", ())                          → True
        ("Remote storage", ("Remote storage",))      → False
        (None, ("Remote storage",))                  → True
        ("Orders", (), ("Orders",))                  → False
    """
    if cellar_name is None:
        return True
    if cellar_name in offsite_cellars:
        return False
    if cellar_name in in_transit_cellars:
        return False
    return True


def compute_is_in_transit(
    cellar_name: str | None,
    in_transit_cellars: tuple[str, ...],
) -> bool:
    """Return whether a bottle is in transit (ordered, not yet received).

    Examples:
        ("Main cellar", ())                                           → False
        ("99 Orders & Subscriptions", ("99 Orders & Subscriptions",)) → True
        (None, ("99 Orders & Subscriptions",))                        → False
    """
    if cellar_name is None:
        return False
    return cellar_name in in_transit_cellars


# ---------------------------------------------------------------------------
# Currency normalisation
# ---------------------------------------------------------------------------


def convert_to_default_currency(
    price: Decimal | None,
    source_currency: str | None,
    default_currency: str,
    rates: dict[str, float],
) -> Decimal | None:
    """Convert a price to the default currency using fixed exchange rates.

    Returns ``None`` when *price* or *source_currency* is ``None``.
    Returns *price* unchanged when source equals default.
    Raises ``ValueError`` when the source currency has no configured rate.

    Examples:
        >>> convert_to_default_currency(Decimal("20.00"), "EUR", "CHF", {"EUR": 0.93})
        Decimal('18.60')
        >>> convert_to_default_currency(Decimal("50.00"), "CHF", "CHF", {"EUR": 0.93})
        Decimal('50.00')
        >>> convert_to_default_currency(None, "EUR", "CHF", {"EUR": 0.93})
        (returns None)
    """
    if price is None or source_currency is None:
        return None
    if source_currency == default_currency:
        return price
    rate = rates.get(source_currency)
    if rate is None:
        raise ValueError(
            f"No exchange rate configured for {source_currency!r} → "
            f"{default_currency!r}. "
            f"Add it to [currency.rates] in cellarbrain.toml."
        )
    return (price * Decimal(str(rate))).quantize(Decimal("0.01"))


# ---------------------------------------------------------------------------
# Pipeline integration  (§6.2–6.3)
# ---------------------------------------------------------------------------

def enrich_wines(
    wines: list[dict],
    wine_grapes: list[dict],
    grape_names: dict[int, str],
    winery_names: dict[int, str],
    appellation_map: dict[int, dict],
    settings: Settings | None = None,
    current_year: int | None = None,
) -> None:
    """Add computed fields to wine entity dicts (in-place).

    Three-pass approach:
    1. Compute per-wine grape properties and inject ``_winery_name`` helper.
    2. Build the grape-ambiguous set, then compute ``full_name`` for each wine.
    3. Compute ``drinking_status``, ``age_years``, ``price_tier`` (requires
       ``current_year`` and ``settings``).
    """
    # Build wine_id → sorted grape list
    wg_by_wine: dict[int, list[dict]] = {}
    for wg in wine_grapes:
        wid = wg["wine_id"]
        gname = grape_names.get(wg["grape_id"])
        if gname is None:
            continue
        entry = {"grape_name": gname, "percentage": wg.get("percentage"), "sort_order": wg.get("sort_order", 0)}
        wg_by_wine.setdefault(wid, []).append(entry)
    for glist in wg_by_wine.values():
        glist.sort(key=lambda g: g.get("sort_order", 0))

    # Pass 1: compute grape properties per wine, inject _winery_name helper
    for w in wines:
        grapes = wg_by_wine.get(w["wine_id"], [])
        w["grape_type"] = compute_grape_type(grapes)
        w["primary_grape"] = compute_primary_grape(grapes)
        w["grape_summary"] = compute_grape_summary(grapes)
        w["_winery_name"] = winery_names.get(w.get("winery_id"))  # type: ignore[arg-type]

    # Pass 2: detect grape-ambiguous names, compute full_name
    ambiguous = build_grape_ambiguous_names(wines, wine_grapes, grape_names)

    cs = settings.classification_short if settings else None
    mfl = settings.dossier.max_full_name_length if settings else None

    for w in wines:
        winery = w.pop("_winery_name", None)
        app = appellation_map.get(w.get("appellation_id"))  # type: ignore[arg-type]
        subregion = app.get("subregion") if app else None
        classification = app.get("classification") if app else None

        needs_grape = bool(winery and w.get("name") and (winery, w["name"]) in ambiguous)

        w["full_name"] = compute_full_name(
            winery=winery,
            name=w.get("name"),
            subregion=subregion,
            classification=classification,
            grape_type=w["grape_type"],
            primary_grape=w["primary_grape"],
            grape_summary=w["grape_summary"],
            vintage=w.get("vintage"),
            is_nv=w.get("is_non_vintage", False),
            name_needs_grape=needs_grape,
            classification_short=cs,
            max_full_name_length=mfl,
        )

    # Pass 3: drinking_status, age_years, currency conversion, price_tier (when current_year given)
    if current_year is not None:
        tiers = settings.price_tiers if settings else _default_price_tiers()
        currency = settings.currency if settings else CurrencyConfig()
        for w in wines:
            w["drinking_status"] = compute_drinking_status(
                w.get("drink_from"), w.get("drink_until"),
                w.get("optimal_from"), w.get("optimal_until"),
                current_year,
            )
            w["age_years"] = compute_age_years(w.get("vintage"), current_year)
            # Currency normalisation — must happen before price_tier
            w["list_price"] = convert_to_default_currency(
                w.get("original_list_price"),
                w.get("original_list_currency"),
                currency.default,
                currency.rates,
            )
            w["list_currency"] = (
                currency.default
                if w.get("original_list_price") is not None
                else None
            )
            w["price_tier"] = compute_price_tier(w.get("list_price"), tiers)
