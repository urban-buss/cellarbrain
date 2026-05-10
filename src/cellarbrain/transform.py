"""Build normalized entities from raw CSV rows."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from . import parsers, vinocell_parsers
from .computed import classify_cellar
from .settings import CellarRule
from .slugify import make_slug

if TYPE_CHECKING:
    from .incremental import WineMatch

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Type alias for a lookup dict: display_value → id
# ---------------------------------------------------------------------------
Lookup = dict[str, int]
# For composite keys: tuple → id
CompositeLookup = dict[tuple, int]


def _wine_natural_key(row: dict) -> tuple:
    """Derive the natural key for matching wine rows across files."""
    return (
        parsers.normalize_quotes(row.get("winery") or ""),
        parsers.normalize_quotes(row.get("wine_name") or ""),
        row.get("vintage_raw") or "",
    )


def _wine_volume_key(row: dict) -> tuple:
    """Derive a volume-aware key for disambiguating multi-format wines.

    Returns a 4-tuple ``(winery, wine_name, vintage, volume)`` that
    distinguishes e.g. a 750 mL standard bottle from a Magnum of the
    same wine and vintage.
    """
    return (
        parsers.normalize_quotes(row.get("winery") or ""),
        parsers.normalize_quotes(row.get("wine_name") or ""),
        row.get("vintage_raw") or "",
        (row.get("volume_raw") or "").strip(),
    )


def _resolve_wine_id(
    row: dict,
    wine_lookup: dict[tuple, int],
    wine_volume_lookup: dict[tuple, int] | None,
) -> int | None:
    """Look up the wine_id for a CSV row, preferring volume-aware match."""
    if wine_volume_lookup is not None:
        vol_key = _wine_volume_key(row)
        if vol_key[3]:  # only try when volume_raw is present
            wine_id = wine_volume_lookup.get(vol_key)
            if wine_id is not None:
                return wine_id
    return wine_lookup.get(_wine_natural_key(row))


def build_wine_volume_lookup(
    wines_rows: list[dict],
    wine_entities: list[dict],
) -> dict[tuple, int]:
    """Build a volume-aware lookup from raw CSV rows and built wine entities.

    Returns ``{(winery, wine_name, vintage, volume) → wine_id}``.
    """
    lookup: dict[tuple, int] = {}
    for row, wine in zip(wines_rows, wine_entities):
        vk = _wine_volume_key(row)
        lookup[vk] = wine["wine_id"]
    return lookup


def wine_slug(winery: str | None, name: str | None, year: str | None) -> str:
    """Generate a stable identity slug from raw CSV values.

    Examples:
        >>> wine_slug("Château Phélan Ségur", None, "2022")
        'chateau-phelan-segur-2022'
        >>> wine_slug("Viña Seña", "Seña", "2023")
        'vina-sena-sena-2023'
        >>> wine_slug("Mionetto", None, "")
        'mionetto-nv'
    """
    vintage, is_nv = vinocell_parsers.parse_vintage(year)
    # For identity slugs, always use "nv" when no vintage is present.
    if vintage is None:
        is_nv = True
    return make_slug(winery, name, vintage, is_nv)


# ---------------------------------------------------------------------------
# Format-variant slug helpers
# ---------------------------------------------------------------------------

_FORMAT_SLUG_SUFFIX: dict[str, str] = {
    "Half Bottle": "half",
    "Magnum": "magnum",
    "Jéroboam": "jeroboam",
    "Double Magnum": "double-magnum",
    "Impériale": "imperiale",
    "Nebuchadnezzar": "nebuchadnezzar",
}


def wine_slug_with_format(
    winery: str | None,
    name: str | None,
    year: str | None,
    bottle_format: str | None,
    has_format_siblings: bool,
) -> str:
    """Generate a slug with optional format suffix for multi-format wines.

    The suffix is only appended when the wine has format siblings AND
    the format is not Standard (750 mL).  Single-format wines always
    get a clean slug regardless of their actual format.

    Examples:
        >>> wine_slug_with_format("Lageder", "COR", "2019", "Magnum", True)
        'lageder-cor-2019-magnum'
        >>> wine_slug_with_format("Lageder", "COR", "2019", "Standard", True)
        'lageder-cor-2019'
        >>> wine_slug_with_format("Lageder", "COR", "2019", "Magnum", False)
        'lageder-cor-2019'
    """
    base = wine_slug(winery, name, year)
    if not has_format_siblings or not bottle_format:
        return base
    suffix = _FORMAT_SLUG_SUFFIX.get(bottle_format)
    if suffix is None:
        return base  # Standard and unknown formats keep clean slug
    combined = f"{base}-{suffix}"
    return combined[:60]


def wine_fingerprint(row: dict) -> tuple[str, str, str, str, str]:
    """Build a disambiguation fingerprint from secondary CSV fields.

    Returns (volume, classification, grapes, category, price) — all
    normalised strings.
    """
    return (
        (row.get("volume_raw") or "").strip(),
        (row.get("classification") or "").strip(),
        (row.get("grapes_raw") or "").strip(),
        (row.get("category_raw") or "").strip(),
        (row.get("list_price_raw") or "").strip(),
    )


# ---------------------------------------------------------------------------
# Lookup entity builders
# ---------------------------------------------------------------------------


def build_wineries(wines_rows: list[dict]) -> tuple[list[dict], Lookup]:
    """Deduplicate winery names, assign IDs.

    Returns (entity_rows, name→id lookup).  Curly/typographic quotes
    in winery names are normalised to ASCII equivalents.
    """
    names: set[str] = set()
    for row in wines_rows:
        w = row.get("winery")
        if w:
            names.add(parsers.normalize_quotes(w))

    entities = []
    lookup: Lookup = {}
    for i, name in enumerate(sorted(names), start=1):
        entities.append({"winery_id": i, "name": name})
        lookup[name] = i

    return entities, lookup


def build_appellations(wines_rows: list[dict]) -> tuple[list[dict], CompositeLookup]:
    """Deduplicate (country, region, subregion, classification) combos.

    Append-only: never deletes existing entries.
    Returns (entity_rows, composite_key→id lookup).
    """
    seen: set[tuple] = set()
    for row in wines_rows:
        country = row.get("country")
        if not country:
            continue
        key = (
            country,
            row.get("region"),
            row.get("subregion"),
            row.get("classification"),
        )
        seen.add(key)

    entities = []
    lookup: CompositeLookup = {}
    for i, key in enumerate(sorted(seen, key=lambda t: tuple(s or "" for s in t)), start=1):
        entities.append(
            {
                "appellation_id": i,
                "country": key[0],
                "region": key[1],
                "subregion": key[2],
                "classification": key[3],
            }
        )
        lookup[key] = i

    return entities, lookup


def build_grapes(wines_rows: list[dict]) -> tuple[list[dict], Lookup]:
    """Parse all Grapes fields, deduplicate variety names.

    Append-only: never deletes existing entries.
    Returns (entity_rows, name→id lookup).
    """
    names: set[str] = set()
    for row in wines_rows:
        for grape_name, _pct in parsers.parse_grapes(row.get("grapes_raw")):
            names.add(grape_name)

    entities = []
    lookup: Lookup = {}
    for i, name in enumerate(sorted(names), start=1):
        entities.append({"grape_id": i, "name": name})
        lookup[name] = i

    return entities, lookup


def build_cellars(
    bottles_rows: list[dict],
    rules: tuple[CellarRule, ...] = (),
) -> tuple[list[dict], Lookup]:
    """Deduplicate cellar names, extract sort_order and location_type.

    Returns (entity_rows, name→id lookup).
    """
    names: set[str] = set()
    for row in bottles_rows:
        c = row.get("cellar")
        if c:
            names.add(c)

    entities = []
    lookup: Lookup = {}
    for i, name in enumerate(sorted(names), start=1):
        entities.append(
            {
                "cellar_id": i,
                "name": name,
                "location_type": classify_cellar(name, rules),
                "sort_order": vinocell_parsers.parse_cellar_sort_order(name),
            }
        )
        lookup[name] = i

    return entities, lookup


def build_providers(
    bottles_rows: list[dict],
    bottles_gone_rows: list[dict] | None = None,
) -> tuple[list[dict], Lookup]:
    """Deduplicate provider names from stored and gone bottle rows.

    Returns (entity_rows, name→id lookup).
    """
    names: set[str] = set()
    for row in bottles_rows:
        p = row.get("provider")
        if p:
            names.add(p)
    for row in bottles_gone_rows or []:
        p = row.get("provider")
        if p:
            names.add(p)

    entities = []
    lookup: Lookup = {}
    for i, name in enumerate(sorted(names), start=1):
        entities.append({"provider_id": i, "name": name})
        lookup[name] = i

    return entities, lookup


# ---------------------------------------------------------------------------
# Core entity builders
# ---------------------------------------------------------------------------


def build_wines(
    wines_rows: list[dict],
    winery_lookup: Lookup,
    appellation_lookup: CompositeLookup,
    *,
    id_assignments: list[WineMatch] | None = None,
) -> tuple[list[dict], dict[tuple, int]]:
    """Build wine entities from wines CSV rows.

    Returns (entity_rows, natural_key→wine_id lookup).

    When *id_assignments* is provided, each wine uses the pre-assigned
    ``wine_id`` from slug-based classification instead of row position.
    """
    entities = []
    wine_lookup: dict[tuple, int] = {}

    for i, row in enumerate(wines_rows, start=1):
        nk = _wine_natural_key(row)
        wine_id = id_assignments[i - 1].wine_id if id_assignments else i
        try:
            vintage, is_nv = vinocell_parsers.parse_vintage(row.get("vintage_raw"))

            app_key = (
                row.get("country"),
                row.get("region"),
                row.get("subregion"),
                row.get("classification"),
            )
            app_id = appellation_lookup.get(app_key) if app_key[0] else None

            wine = {
                "wine_id": wine_id,
                "wine_slug": wine_slug(
                    row.get("winery"),
                    row.get("wine_name"),
                    row.get("vintage_raw"),
                ),
                "winery_id": winery_lookup.get(parsers.normalize_quotes(row.get("winery") or "")),
                "name": vinocell_parsers.parse_wine_name(row.get("wine_name")),
                "vintage": vintage,
                "is_non_vintage": is_nv,
                "appellation_id": app_id,
                "category": vinocell_parsers.parse_category(row.get("category_raw")),
                "_raw_classification": (row.get("classification") or "").strip() or None,
                "subcategory": parsers.to_slug(row.get("subcategory_raw")),
                "specialty": parsers.to_slug(row.get("specialty_raw")),
                "sweetness": parsers.to_slug(row.get("sweetness_raw")),
                "effervescence": parsers.to_slug(row.get("effervescence_raw")),
                "volume_ml": parsers.parse_volume(row.get("volume_raw")),
                "_raw_volume": (row.get("volume_raw") or "").strip() or None,
                "container": parsers.to_slug(row.get("container_raw")),
                "hue": parsers.to_slug(row.get("hue_raw")),
                "cork": parsers.to_slug(row.get("cork_raw")),
                "alcohol_pct": parsers.parse_alcohol(row.get("alcohol_raw")),
                "acidity_g_l": parsers.parse_acidity(row.get("acidity_raw")),
                "sugar_g_l": parsers.parse_sugar(row.get("sugar_raw")),
                "ageing_type": parsers.to_slug(row.get("ageing_type_raw")),
                "ageing_months": parsers.parse_ageing_months(row.get("ageing_months_raw")),
                "farming_type": parsers.to_slug(row.get("farming_type_raw")),
                "serving_temp_c": parsers.parse_int(row.get("temperature_raw")),
                "opening_type": parsers.to_slug(row.get("opening_type_raw")),
                "opening_minutes": vinocell_parsers.parse_opening_time(row.get("opening_time_raw")),
                "drink_from": parsers.parse_int(row.get("drink_from")),
                "drink_until": parsers.parse_int(row.get("drink_until")),
                "optimal_from": parsers.parse_int(row.get("optimal_from")),
                "optimal_until": parsers.parse_int(row.get("optimal_until")),
                "original_list_price": parsers.parse_decimal(row.get("list_price_raw")),
                "original_list_currency": row.get("list_currency"),
                "comment": row.get("comment"),
                "winemaking_notes": row.get("winemaking_notes_raw"),
                "_raw_grapes": (row.get("grapes_raw") or "").strip() or None,
                "is_favorite": parsers.parse_bool(row.get("is_favorite_raw")),
                "is_wishlist": parsers.parse_bool(row.get("is_wishlist_raw")),
                "food_tags": None,
                "food_groups": None,
            }
        except ValueError as exc:
            raise ValueError(f"Wine row {i} ({nk[0]!r} / {nk[1]!r} / {nk[2]!r}): {exc}") from exc
        entities.append(wine)
        if nk in wine_lookup:
            logger.info(
                "Duplicate wine natural key %r at row %d "
                "(first seen as wine_id=%d). "
                "The later entry will take precedence.",
                nk,
                i,
                wine_lookup[nk],
            )
        wine_lookup[nk] = wine_id

    return entities, wine_lookup


def build_wine_grapes(
    wines_rows: list[dict],
    wine_lookup: dict[tuple, int],
    grape_lookup: Lookup,
    *,
    wine_volume_lookup: dict[tuple, int] | None = None,
) -> list[dict]:
    """Build wine_grape junction rows."""
    entities = []
    seen_wine_ids: set[int] = set()
    for row in wines_rows:
        wine_id = _resolve_wine_id(row, wine_lookup, wine_volume_lookup)
        if wine_id is None or wine_id in seen_wine_ids:
            continue
        seen_wine_ids.add(wine_id)
        grapes = parsers.parse_grapes(row.get("grapes_raw"))
        for order, (name, pct) in enumerate(grapes, start=1):
            grape_id = grape_lookup.get(name)
            if grape_id is None:
                continue
            entities.append(
                {
                    "wine_id": wine_id,
                    "grape_id": grape_id,
                    "percentage": pct,
                    "sort_order": order,
                }
            )
    return entities


def build_bottles(
    bottles_rows: list[dict],
    wine_lookup: dict[tuple, int],
    cellar_lookup: Lookup,
    provider_lookup: Lookup,
    *,
    wine_volume_lookup: dict[tuple, int] | None = None,
) -> list[dict]:
    """Build bottle entities from bottles CSV rows."""
    entities = []
    for i, row in enumerate(bottles_rows, start=1):
        wine_id = _resolve_wine_id(row, wine_lookup, wine_volume_lookup)
        if wine_id is None:
            nk = _wine_natural_key(row)
            logger.warning(
                "Bottle row %d: no matching wine for %r. Skipped.",
                i,
                nk,
            )
            continue

        try:
            bottle = {
                "bottle_id": i,
                "wine_id": wine_id,
                "status": "stored",
                "cellar_id": cellar_lookup.get(row.get("cellar") or ""),
                "shelf": row.get("shelf"),
                "bottle_number": parsers.parse_int(row.get("bottle_number_raw")),
                "provider_id": provider_lookup.get(row.get("provider") or ""),
                "purchase_date": parsers.parse_eu_date(row.get("purchase_date_raw")),
                "acquisition_type": vinocell_parsers.parse_acquisition_type(row.get("acquisition_type_raw")),
                "original_purchase_price": parsers.parse_decimal(row.get("purchase_price_raw")),
                "original_purchase_currency": row.get("purchase_currency"),
                "purchase_comment": row.get("purchase_comment"),
                "output_date": None,
                "output_type": None,
                "output_comment": None,
            }
        except ValueError as exc:
            nk = _wine_natural_key(row)
            raise ValueError(f"Bottle row {i} ({nk[0]!r} / {nk[1]!r} / {nk[2]!r}): {exc}") from exc
        entities.append(bottle)
    return entities


def build_bottles_gone(
    bottles_gone_rows: list[dict],
    wine_lookup: dict[tuple, int],
    provider_lookup: Lookup,
    start_id: int = 1,
    *,
    wine_volume_lookup: dict[tuple, int] | None = None,
) -> list[dict]:
    """Build bottle entities from bottles-gone CSV rows."""
    entities = []
    next_id = start_id
    for i, row in enumerate(bottles_gone_rows, start=1):
        wine_id = _resolve_wine_id(row, wine_lookup, wine_volume_lookup)
        if wine_id is None:
            nk = _wine_natural_key(row)
            logger.warning(
                "Bottles-gone row %d: no matching wine for %r. Skipped.",
                i,
                nk,
            )
            continue
        try:
            output_type = vinocell_parsers.parse_output_type(row.get("output_type_raw"))
            bottle = {
                "bottle_id": next_id,
                "wine_id": wine_id,
                "status": output_type or "removed",
                "cellar_id": None,
                "shelf": None,
                "bottle_number": parsers.parse_int(row.get("bottle_number_raw")),
                "provider_id": provider_lookup.get(row.get("provider") or ""),
                "purchase_date": parsers.parse_eu_date(row.get("purchase_date_raw")),
                "acquisition_type": vinocell_parsers.parse_acquisition_type(row.get("acquisition_type_raw")),
                "original_purchase_price": parsers.parse_decimal(row.get("purchase_price_raw")),
                "original_purchase_currency": row.get("purchase_currency"),
                "purchase_comment": row.get("purchase_comment"),
                "output_date": parsers.parse_eu_date(row.get("output_date_raw")),
                "output_type": output_type,
                "output_comment": row.get("output_comment"),
            }
        except ValueError as exc:
            nk = _wine_natural_key(row)
            raise ValueError(f"Bottles-gone row {i} ({nk[0]!r} / {nk[1]!r} / {nk[2]!r}): {exc}") from exc
        entities.append(bottle)
        next_id += 1
    return entities


def build_tastings(
    wines_rows: list[dict],
    wine_lookup: dict[tuple, int],
    *,
    wine_volume_lookup: dict[tuple, int] | None = None,
) -> list[dict]:
    """Parse tasting notes from wines field W51."""
    entities = []
    tasting_id = 0
    for i, row in enumerate(wines_rows, start=1):
        raw = row.get("tastings_raw")
        if not raw:
            continue
        wine_id = _resolve_wine_id(row, wine_lookup, wine_volume_lookup)
        if wine_id is None:
            nk = _wine_natural_key(row)
            logger.warning(
                "Tasting row %d: no matching wine for %r. Skipped.",
                i,
                nk,
            )
            continue

        for line in raw.split("\n"):
            parsed = vinocell_parsers.parse_tasting_line(line)
            if parsed:
                tasting_id += 1
                entities.append(
                    {
                        "tasting_id": tasting_id,
                        "wine_id": wine_id,
                        "tasting_date": parsed["date"],
                        "note": parsed["note"],
                        "score": parsed["score"],
                        "max_score": parsed["max_score"],
                    }
                )
    return entities


def build_pro_ratings(
    wines_rows: list[dict],
    bottles_rows: list[dict],
    wine_lookup: dict[tuple, int],
    bottles_gone_rows: list[dict] | None = None,
    *,
    wine_volume_lookup: dict[tuple, int] | None = None,
) -> list[dict]:
    """Parse professional ratings from wines W53 + bottles B49, deduplicated."""
    seen: set[tuple] = set()  # (wine_id, source, score) for dedup
    entities = []
    rating_id = 0

    # Source 1: Wine file — "pro_ratings_raw" (W53)
    for i, row in enumerate(wines_rows, start=1):
        raw = row.get("pro_ratings_raw")
        if not raw:
            continue
        wine_id = _resolve_wine_id(row, wine_lookup, wine_volume_lookup)
        if wine_id is None:
            nk = _wine_natural_key(row)
            logger.warning(
                "Pro-rating (wines) row %d: no matching wine for %r. Skipped.",
                i,
                nk,
            )
            continue
        for line in raw.split("\n"):
            parsed = vinocell_parsers.parse_pro_rating_wine(line)
            if parsed:
                dedup_key = (wine_id, parsed["source"], parsed["score"])
                if dedup_key not in seen:
                    seen.add(dedup_key)
                    rating_id += 1
                    entities.append(
                        {
                            "rating_id": rating_id,
                            "wine_id": wine_id,
                            **parsed,
                        }
                    )

    # Source 2: Bottle file — "pro_ratings_raw" (B49)
    for i, row in enumerate(bottles_rows, start=1):
        raw = row.get("pro_ratings_raw")
        if not raw:
            continue
        wine_id = _resolve_wine_id(row, wine_lookup, wine_volume_lookup)
        if wine_id is None:
            nk = _wine_natural_key(row)
            logger.warning(
                "Pro-rating (bottles) row %d: no matching wine for %r. Skipped.",
                i,
                nk,
            )
            continue
        parsed = vinocell_parsers.parse_pro_rating_bottle(raw)
        if parsed:
            dedup_key = (wine_id, parsed["source"], parsed["score"])
            if dedup_key not in seen:
                seen.add(dedup_key)
                rating_id += 1
                entities.append(
                    {
                        "rating_id": rating_id,
                        "wine_id": wine_id,
                        **parsed,
                    }
                )

    # Source 3: Bottles-gone file — "pro_ratings_raw" (same field)
    for i, row in enumerate(bottles_gone_rows or [], start=1):
        raw = row.get("pro_ratings_raw")
        if not raw:
            continue
        wine_id = _resolve_wine_id(row, wine_lookup, wine_volume_lookup)
        if wine_id is None:
            nk = _wine_natural_key(row)
            logger.warning(
                "Pro-rating (bottles-gone) row %d: no matching wine for %r. Skipped.",
                i,
                nk,
            )
            continue
        parsed = vinocell_parsers.parse_pro_rating_bottle(raw)
        if parsed:
            dedup_key = (wine_id, parsed["source"], parsed["score"])
            if dedup_key not in seen:
                seen.add(dedup_key)
                rating_id += 1
                entities.append(
                    {
                        "rating_id": rating_id,
                        "wine_id": wine_id,
                        **parsed,
                    }
                )

    return entities


# ---------------------------------------------------------------------------
# Dossier path assignment (must be called after all entities are built)
# ---------------------------------------------------------------------------


def assign_dossier_paths(entities: dict[str, list[dict]]) -> None:
    """Set ``dossier_path`` on every wine row.

    Wines with ≥1 stored bottle go to ``cellar/``, all others to ``archive/``.
    Must be called after bottles have their final stable IDs and wine_id FKs,
    and after :func:`update_format_slugs` has applied format suffixes.
    """
    stored_wine_ids: set[int] = {
        b["wine_id"] for b in entities.get("bottle", []) if b.get("status") == "stored" and not b.get("is_in_transit")
    }
    for w in entities["wine"]:
        slug = w.get("wine_slug") or ""
        fname = f"{w['wine_id']:04d}-{slug}.md"
        subfolder = "cellar" if w["wine_id"] in stored_wine_ids else "archive"
        w["dossier_path"] = f"{subfolder}/{fname}"


# ---------------------------------------------------------------------------
# Format-group assignment (must be called before assign_dossier_paths)
# ---------------------------------------------------------------------------


def assign_format_groups(wines: list[dict]) -> None:
    """Link format variants by setting ``format_group_id``.

    Groups wines by ``(winery_id, name, vintage)``.  If a group has ≥2
    members with *different* ``volume_ml`` values, the Standard (750 mL)
    variant's ``wine_id`` becomes the group ID.  All members get
    ``format_group_id`` set; single-format wines get ``None``.

    Same-volume duplicates (NK collisions where both rows share the same
    volume) are explicitly excluded — only genuine multi-format variants
    are linked.
    """
    from collections import defaultdict

    groups: dict[tuple, list[dict]] = defaultdict(list)
    for w in wines:
        if w.get("is_deleted"):
            w.setdefault("format_group_id", None)
            continue
        key = (w.get("winery_id"), w.get("name"), w.get("vintage"))
        groups[key].append(w)

    for members in groups.values():
        if len(members) < 2:
            for w in members:
                w["format_group_id"] = None
            continue

        # Only group when volumes actually differ.
        volumes = {w.get("volume_ml") for w in members}
        if len(volumes) < 2:
            for w in members:
                w["format_group_id"] = None
            continue

        # Pick primary: prefer 750 mL, else smallest volume.
        primary = next(
            (w for w in members if w.get("volume_ml") == 750),
            min(members, key=lambda w: w.get("volume_ml") or 0),
        )
        gid = primary["wine_id"]
        for w in members:
            w["format_group_id"] = gid


def update_format_slugs(wines: list[dict]) -> None:
    """Append a format suffix to ``wine_slug`` for non-primary format variants.

    Must be called after :func:`assign_format_groups` has set
    ``format_group_id`` on every wine.  Only non-Standard wines that
    belong to a format group get their slug updated.
    """
    for w in wines:
        if w.get("format_group_id") is None:
            continue
        bf = w.get("bottle_format") or ""
        suffix = _FORMAT_SLUG_SUFFIX.get(bf)
        if suffix is None:
            continue  # Standard and unknown formats keep clean slug
        base = w.get("wine_slug") or ""
        combined = f"{base}-{suffix}"
        w["wine_slug"] = combined[:60]


# ---------------------------------------------------------------------------
# Tracked wine (wishlist / favorites)
# ---------------------------------------------------------------------------

TRACKED_WINE_ID_OFFSET: int = 90_001


def build_tracked_wines(
    wines: list[dict],
    appellation_by_wine: dict[int, int | None],
) -> tuple[list[dict], dict[tuple[int, str], int]]:
    """Build tracked_wine entities by grouping wines with wishlist/favorite flags.

    A tracked_wine represents a wine identity (winery + name) across vintages.
    One is created when any related wine has ``is_wishlist`` or ``is_favorite``
    set to ``True``.

    Returns (entity_rows, (winery_id, wine_name) → tracked_wine_id lookup).
    """
    # Collect groups: (winery_id, wine_name) → representative data
    groups: dict[tuple[int, str], dict] = {}
    for w in wines:
        if w.get("is_deleted"):
            continue
        winery_id = w.get("winery_id")
        name = w.get("name")
        if winery_id is None or not name:
            continue
        if not (w.get("is_wishlist") or w.get("is_favorite")):
            continue
        key = (winery_id, name)
        if key not in groups:
            groups[key] = {
                "category": w.get("category", "unknown"),
                "appellation_id": appellation_by_wine.get(w.get("wine_id")),
            }

    entities: list[dict] = []
    lookup: dict[tuple[int, str], int] = {}

    for i, ((winery_id, wine_name), data) in enumerate(
        sorted(groups.items()),
        start=TRACKED_WINE_ID_OFFSET,
    ):
        entities.append(
            {
                "tracked_wine_id": i,
                "winery_id": winery_id,
                "wine_name": wine_name,
                "category": data["category"],
                "appellation_id": data["appellation_id"],
                "dossier_path": "",  # assigned later by assign_tracked_dossier_paths
                "is_deleted": False,
            }
        )
        lookup[(winery_id, wine_name)] = i

    return entities, lookup


def assign_tracked_wine_ids(
    wines: list[dict],
    tracked_lookup: dict[tuple[int, str], int],
) -> None:
    """Set ``tracked_wine_id`` on every wine row from the tracked_wine lookup."""
    for w in wines:
        winery_id = w.get("winery_id")
        name = w.get("name")
        if winery_id is not None and name:
            w["tracked_wine_id"] = tracked_lookup.get((winery_id, name))
        else:
            w["tracked_wine_id"] = None


def assign_tracked_dossier_paths(
    entities: dict[str, list[dict]],
    settings: object,
) -> None:
    """Set ``dossier_path`` on every tracked_wine row.

    Uses *wishlist_subdir* from settings.  Must be called after ID
    stabilisation so that ``winery_id`` values are final.
    """
    from .companion_markdown import companion_dossier_slug

    winery_name_by_id: dict[int, str] = {w["winery_id"]: w["name"] for w in entities.get("winery", [])}
    subdir = getattr(
        getattr(settings, "wishlist", None),
        "wishlist_subdir",
        "wishlist",
    )
    for tw in entities.get("tracked_wine", []):
        winery_name = winery_name_by_id.get(tw.get("winery_id"))
        slug = companion_dossier_slug(
            tw["tracked_wine_id"],
            winery_name,
            tw["wine_name"],
        )
        tw["dossier_path"] = f"{subdir}/{slug}"
