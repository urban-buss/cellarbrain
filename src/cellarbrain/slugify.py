"""URL-safe slug generation for wine identifiers."""

from __future__ import annotations

import re
import unicodedata


def make_slug(
    winery: str | None,
    name: str | None,
    vintage: int | None,
    is_non_vintage: bool,
    slug_max_length: int = 60,
) -> str:
    """Build a URL-safe slug from winery, name, and vintage.

    Examples:
        >>> make_slug("Marques De Murrieta", None, 2016, False)
        'marques-de-murrieta-2016'
        >>> make_slug("Château Phélan Ségur", None, 2020, False)
        'chateau-phelan-segur-2020'
    """
    parts: list[str] = []
    if winery:
        parts.append(winery)
    if name:
        parts.append(name)
    if is_non_vintage:
        parts.append("nv")
    elif vintage is not None:
        parts.append(str(vintage))

    raw = " ".join(parts)
    # Accent-fold to ASCII
    nfkd = unicodedata.normalize("NFKD", raw)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    # Lowercase, replace non-alphanumeric with hyphens, collapse
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return slug[:slug_max_length].rstrip("-")


def companion_slug(
    winery: str | None,
    wine_name: str | None,
    slug_max_length: int = 60,
) -> str:
    """Build a URL-safe slug from winery and wine name (no vintage).

    Examples:
        >>> companion_slug("Château Margaux", "Grand Vin")
        'chateau-margaux-grand-vin'
        >>> companion_slug("Château Phélan Ségur", None)
        'chateau-phelan-segur'
    """
    parts: list[str] = []
    if winery:
        parts.append(winery)
    if wine_name:
        parts.append(wine_name)

    raw = " ".join(parts)
    nfkd = unicodedata.normalize("NFKD", raw)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return slug[:slug_max_length].rstrip("-")
