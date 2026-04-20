"""Field-level parsers for raw CSV values.

Every parser accepts a raw string (or None) and returns the parsed value.
None/empty input always returns None (for nullable fields) or raises
ValueError (for required fields).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal


# ---------------------------------------------------------------------------
# Typographic quote normalisation
# ---------------------------------------------------------------------------

_QUOTE_MAP = str.maketrans({
    "\u2018": "'",   # LEFT SINGLE QUOTATION MARK  → apostrophe
    "\u2019": "'",   # RIGHT SINGLE QUOTATION MARK → apostrophe
    "\u201C": '"',   # LEFT DOUBLE QUOTATION MARK  → double quote
    "\u201D": '"',   # RIGHT DOUBLE QUOTATION MARK → double quote
})


def normalize_quotes(s: str) -> str:
    """Replace typographic (curly) quotes with their ASCII equivalents.

    Examples:
        >>> normalize_quotes("Château d\u2019Aiguilhe")
        "Château d'Aiguilhe"
        >>> normalize_quotes("L\u2019Oratoire")
        "L'Oratoire"
        >>> normalize_quotes("plain text")
        'plain text'
    """
    return s.translate(_QUOTE_MAP)


# ---------------------------------------------------------------------------
# Grape blend parsing
# ---------------------------------------------------------------------------

_GRAPE_RE = re.compile(r"([^,(]+?)\s*(?:\((\d+)%\))?\s*(?:,|$)")


def parse_grapes(raw: str | None) -> list[tuple[str, float | None]]:
    """Parse a grape blend string into [(name, pct), ...].

    Examples:
        "Nebbiolo"                         → [("Nebbiolo", None)]
        "Cabernet Sauvignon (100%)"        → [("Cabernet Sauvignon", 100.0)]
        "Merlot (80%), Cabernet Franc (20%)" → [("Merlot", 80.0), ("Cabernet Franc", 20.0)]
    """
    if not raw:
        return []
    results = []
    for m in _GRAPE_RE.finditer(raw):
        name = m.group(1).strip()
        pct = float(m.group(2)) if m.group(2) else None
        if name:
            results.append((name, pct))
    return results


# ---------------------------------------------------------------------------
# Unit-stripping parsers
# ---------------------------------------------------------------------------

def parse_alcohol(raw: str | None) -> float | None:
    """'14.5 %' → 14.5"""
    if not raw:
        return None
    try:
        return float(raw.replace("%", "").strip())
    except (ValueError, TypeError):
        raise ValueError(f"Cannot parse alcohol: {raw!r}")


def parse_acidity(raw: str | None) -> float | None:
    """'6.40 g/l' → 6.4"""
    if not raw:
        return None
    try:
        return float(raw.replace("g/l", "").strip())
    except (ValueError, TypeError):
        raise ValueError(f"Cannot parse acidity: {raw!r}")


def parse_sugar(raw: str | None) -> float | None:
    """'5.30 g/l' → 5.3"""
    if not raw:
        return None
    try:
        return float(raw.replace("g/l", "").strip())
    except (ValueError, TypeError):
        raise ValueError(f"Cannot parse sugar: {raw!r}")


_VOLUME_MAP: dict[str, int] = {
    "375ml": 375,
    "500ml": 500,
    "750ml": 750,
    "magnum": 1500,
}

_VOLUME_LITRE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*L", re.IGNORECASE)
_VOLUME_ML_RE = re.compile(r"(\d+)\s*mL", re.IGNORECASE)


def parse_volume(raw: str | None) -> int:
    """Parse volume string to millilitres.

    Handles: '750mL', 'Magnum', '3.0 L - Jéroboam', '375mL', etc.
    """
    if not raw:
        raise ValueError("Volume is required but got empty value")

    key = raw.strip().lower()
    if key in _VOLUME_MAP:
        return _VOLUME_MAP[key]

    # Try "X.X L" pattern (litres)
    m = _VOLUME_LITRE_RE.search(raw)
    if m:
        return int(float(m.group(1)) * 1000)

    # Try "XmL" pattern
    m = _VOLUME_ML_RE.search(raw)
    if m:
        return int(m.group(1))

    raise ValueError(f"Cannot parse volume: {raw!r}")


def parse_ageing_months(raw: str | None) -> int | None:
    """'12 Months' → 12"""
    if not raw:
        return None
    try:
        return int(raw.lower().replace("months", "").replace("month", "").strip())
    except (ValueError, TypeError):
        raise ValueError(f"Cannot parse ageing months: {raw!r}")


# ---------------------------------------------------------------------------
# Date parsers
# ---------------------------------------------------------------------------

def parse_eu_date(raw: str | None) -> date | None:
    """'16.08.2024' (DD.MM.YYYY) → date(2024, 8, 16)"""
    if not raw:
        return None
    try:
        return datetime.strptime(raw.strip(), "%d.%m.%Y").date()
    except ValueError:
        raise ValueError(f"Cannot parse date: {raw!r} (expected DD.MM.YYYY format)")


def parse_tasting_date(raw: str) -> date:
    """'21 February 2024' → date(2024, 2, 21)"""
    try:
        return datetime.strptime(raw.strip(), "%d %B %Y").date()
    except ValueError:
        raise ValueError(
            f"Cannot parse tasting date: {raw!r} "
            f"(expected 'DD Month YYYY' format, e.g. '21 February 2024')"
        )


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def to_slug(raw: str | None) -> str | None:
    """Convert a display value to a lowercase slug.

    'Dark Red' → 'dark_red', 'Biodynamic farming' → 'biodynamic_farming'
    """
    if not raw:
        return None
    return raw.strip().lower().replace(" ", "_")


def parse_decimal(raw: str | None) -> Decimal | None:
    """Parse a decimal string like '36.00' → Decimal('36.00')."""
    if not raw:
        return None
    try:
        return Decimal(raw.strip())
    except Exception:
        raise ValueError(f"Cannot parse decimal: {raw!r}")


def parse_int(raw: str | None) -> int | None:
    """Parse an integer string, returning None for empty."""
    if not raw:
        return None
    try:
        return int(raw.strip())
    except (ValueError, TypeError):
        raise ValueError(f"Cannot parse integer: {raw!r}")


def parse_bool(raw: str | None) -> bool:
    """'Yes' → True, anything else → False."""
    return raw is not None and raw.strip().lower() == "yes"
