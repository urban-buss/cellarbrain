"""Shared price parsing and normalisation utilities."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

# Pattern: optional currency prefix, whitespace, digits with optional decimal
_PRICE_RE = re.compile(
    r"(?P<currency>CHF|EUR|USD)?\s*"
    r"(?P<amount>\d+(?:[.,]\d{1,2})?)"
    r"(?:\.\u2013|\.\u2014|\.[-–—])?",  # handles "9.–" / "9.—" trailing dash
)

_CURRENCY_ALIASES = {
    "fr.": "CHF",
    "sfr.": "CHF",
    "sfr": "CHF",
    "€": "EUR",
    "$": "USD",
}


def parse_price(raw: str) -> tuple[Decimal, str]:
    """Parse a price string into (amount, currency).

    Handles formats:
        "CHF 29.90"  → (Decimal("29.90"), "CHF")
        "29.90"      → (Decimal("29.90"), "CHF")  # default CHF
        "9.–"        → (Decimal("9.00"), "CHF")
        "CHF  22.00" → (Decimal("22.00"), "CHF")
        "EUR 42.50"  → (Decimal("42.50"), "EUR")

    Examples:
        >>> parse_price("CHF 29.90")
        (Decimal('29.90'), 'CHF')
        >>> parse_price("9.–")
        (Decimal('9.00'), 'CHF')
    """
    text = raw.strip()

    # Check for currency aliases
    currency = "CHF"
    for alias, cur in _CURRENCY_ALIASES.items():
        if text.lower().startswith(alias):
            currency = cur
            text = text[len(alias) :].strip()
            break

    # Handle trailing dash patterns (e.g. "9.–", "2.—")
    text = re.sub(r"[.,][\u2013\u2014\-–—]+$", ".00", text)

    match = _PRICE_RE.search(text)
    if not match:
        raise ValueError(f"Cannot parse price from: {raw!r}")

    if match.group("currency"):
        currency = match.group("currency")

    amount_str = match.group("amount").replace(",", ".")
    try:
        amount = Decimal(amount_str)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid price amount in: {raw!r}") from exc

    return amount, currency


def compute_per_bottle(total_price: Decimal, pack_size: int) -> Decimal:
    """Compute per-bottle price from case/pack pricing.

    Used by Millesima (6/12-packs) and Denner (6-packs).

    Examples:
        >>> compute_per_bottle(Decimal("94.80"), 6)
        Decimal('15.80')
    """
    if pack_size <= 0:
        raise ValueError(f"Invalid pack_size: {pack_size}")
    return (total_price / pack_size).quantize(Decimal("0.01"))


def compute_discount_pct(sale_price: Decimal, original_price: Decimal) -> float:
    """Compute discount percentage.

    Returns positive value for discounts (e.g. 25.0 means 25% off).

    Examples:
        >>> compute_discount_pct(Decimal("75"), Decimal("100"))
        25.0
    """
    if original_price <= 0:
        return 0.0
    return round(float((1 - sale_price / original_price) * 100), 1)
