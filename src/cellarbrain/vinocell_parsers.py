"""Vinocell-specific field parsers.

Parsers in this module understand formats and enum values that are specific
to Vinocell CSV exports (category labels, rating line layouts, acquisition
types, etc.).  Generic parsers (dates, decimals, slugs, unit stripping) live
in :mod:`cellarbrain.parsers`.
"""

from __future__ import annotations

import re

from .parsers import normalize_quotes, parse_tasting_date


# ---------------------------------------------------------------------------
# Opening-time parser
# ---------------------------------------------------------------------------

def parse_opening_time(raw: str | None) -> int | None:
    """'1h00min' → 60 (minutes)"""
    if not raw:
        return None
    m = re.match(r"(\d+)h(\d+)min", raw)
    if not m:
        raise ValueError(f"Cannot parse opening time: {raw!r}")
    return int(m.group(1)) * 60 + int(m.group(2))


# ---------------------------------------------------------------------------
# Tasting & rating parsers
# ---------------------------------------------------------------------------

_TASTING_RE = re.compile(
    r"^(\d{1,2} \w+ \d{4})\s*-\s*(.*?)(?:\s*-\s*(\d+\.\d+)/(\d+))?\s*$"
)


def parse_tasting_line(raw: str | None) -> dict | None:
    """Parse a tasting line from wines field W51.

    Format: '{day} {month} {year} - {note} - {score}/{max}'
    Returns: {'date': date, 'note': str|None, 'score': float|None, 'max_score': int|None}
    """
    if not raw:
        return None
    line = raw.strip()
    if not line:
        return None
    m = _TASTING_RE.match(line)
    if not m:
        return None
    return {
        "date": parse_tasting_date(m.group(1)),
        "note": m.group(2).strip() or None,
        "score": float(m.group(3)) if m.group(3) else None,
        "max_score": int(m.group(4)) if m.group(4) else None,
    }


_PRO_RATING_WINE_RE = re.compile(
    r"^(.+?)\s*-\s*(\d+\.\d+)/(\d+)(?:\s*-\s*(.*))?$"
)


def parse_pro_rating_wine(raw: str | None) -> dict | None:
    """Parse a pro rating from wines field W53.

    Format: '{Source} - {score}/{max} - {optional review}'
    Returns: {'source': str, 'score': float, 'max_score': int, 'review_text': str|None}
    """
    if not raw:
        return None
    line = raw.strip()
    if not line:
        return None
    m = _PRO_RATING_WINE_RE.match(line)
    if not m:
        return None
    return {
        "source": m.group(1).strip(),
        "score": float(m.group(2)),
        "max_score": int(m.group(3)),
        "review_text": m.group(4).strip() if m.group(4) and m.group(4).strip() else None,
    }


_PRO_RATING_BOTTLE_RE = re.compile(
    r"^(.+?):\s*(\d+\.\d+)/(\d+)$"
)


def parse_pro_rating_bottle(raw: str | None) -> dict | None:
    """Parse a pro rating from bottles field B49.

    Format: '{Source}: {score}/{max}'
    Returns: {'source': str, 'score': float, 'max_score': int, 'review_text': None}
    """
    if not raw:
        return None
    line = raw.strip()
    if not line:
        return None
    m = _PRO_RATING_BOTTLE_RE.match(line)
    if not m:
        return None
    return {
        "source": m.group(1).strip(),
        "score": float(m.group(2)),
        "max_score": int(m.group(3)),
        "review_text": None,
    }


# ---------------------------------------------------------------------------
# Enum / mapping parsers
# ---------------------------------------------------------------------------

_CATEGORY_MAP = {
    "Red wine": "red",
    "White wine": "white",
    "Rose wine": "rose",
    "Sparkling wine": "sparkling",
    "Dessert wine": "dessert",
    "Fortified wine": "fortified",
}


def parse_category(raw: str | None) -> str:
    """'Red wine' → 'red'"""
    if not raw or raw not in _CATEGORY_MAP:
        raise ValueError(f"Invalid category: {raw!r}")
    return _CATEGORY_MAP[raw]


def parse_vintage(raw: str | None) -> tuple[int | None, bool]:
    """Parse year field.

    Returns (vintage_int_or_none, is_non_vintage).
    '2020' → (2020, False)
    'Non vintage' → (None, True)
    '' → (None, False)
    """
    if not raw:
        return (None, False)
    if raw.strip().lower() == "non vintage":
        return (None, True)
    return (int(raw.strip()), False)


def parse_cellar_sort_order(raw: str | None) -> int:
    """Extract leading digits from cellar name for sort ordering.

    '02a Brünighof Linker Weinkühler' → 2
    """
    if not raw:
        return 0
    m = re.match(r"(\d+)", raw)
    return int(m.group(1)) if m else 0


_ACQUISITION_TYPE_MAP = {
    "Market price": "market_price",
    "Discount price": "discount_price",
    "Present": "present",
    "Free": "free",
}


def parse_acquisition_type(raw: str | None) -> str:
    """'Market price' → 'market_price'"""
    if not raw or raw not in _ACQUISITION_TYPE_MAP:
        raise ValueError(f"Invalid acquisition type: {raw!r}")
    return _ACQUISITION_TYPE_MAP[raw]


_OUTPUT_TYPE_MAP: dict[str, str] = {
    "Drunk": "drunk",
    "Offered": "offered",
    "Removed": "removed",
}


def parse_output_type(raw: str | None) -> str | None:
    """Map raw Output type to enum value."""
    if not raw:
        return None
    result = _OUTPUT_TYPE_MAP.get(raw.strip())
    if result is None:
        raise ValueError(f"Unknown output type: {raw!r}")
    return result


# ---------------------------------------------------------------------------
# Wine-name cleaning
# ---------------------------------------------------------------------------

_PLACEHOLDER_NAMES: frozenset[str] = frozenset({"new wine"})


def parse_wine_name(raw: str | None) -> str | None:
    """Return *None* for placeholder wine names, pass through real names.

    The cellar app may pre-fill ``"New wine"`` as a default name.  This is
    never an actual cuvée name so we null it out during ETL.

    Examples:
        >>> parse_wine_name("Cuvée Alpha")
        'Cuvée Alpha'
        >>> parse_wine_name("New wine")
        >>> parse_wine_name("new wine")
        >>> parse_wine_name("  New wine  ")
        >>> parse_wine_name(None)
        >>> parse_wine_name("")
    """
    if not raw or raw.strip().lower() in _PLACEHOLDER_NAMES:
        return None
    return normalize_quotes(raw)
