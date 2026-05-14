"""Millesima newsletter parser (HTML extraction).

Extracts wine promotions from Millesima's HTML newsletter format.
Product blocks are identified by ``<strong>`` tags containing wine names
with vintage years, inside ``<td>`` containers with CHF prices.

HTML structure per product block:
    <td> (at level ~6 from <strong>)
        <strong>Producer : Wine Name YYYY</strong>   ← wine name with year
        <em>Region Color</em>                       ← appellation (e.g. "Haut-Médoc Rot")
        <p>CHF XXX.00</p>                          ← sale price
        <p><span style="line-through">CHF YYY.00</span></p>  ← optional original
        "Eine Kiste mit 12 Flaschen (75cl)"        ← pack info
        <a>Mehr anzeigen</a>                        ← product link

Wine name format: "Producer : Wine Name YYYY" (year at end, colon separates producer).
Prices are per-case (6 or 12 bottles); per_bottle_price is calculated.
Spirits (Whisky, Gin, Tequila, etc.) are filtered out by appellation.
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Tag

from ..models import ExtractedPromotion
from ..price_utils import compute_discount_pct
from ..registry import register
from .base import NewsletterParser

logger = logging.getLogger(__name__)

# Wine name: contains a 4-digit year (19xx or 20xx)
_YEAR_RE = re.compile(r"\b((?:19|20)\d{2})\b")

# Price pattern: "CHF 566.00" or "CHF666.00" (sometimes no space)
_PRICE_RE = re.compile(r"CHF\s*([\d]+[.,][\d]{2})")

# Pack info: "Eine Kiste mit 12 Flaschen (75cl)" / "Ein Karton mit 6 Flaschen (75cl)"
_PACK_RE = re.compile(r"(?:Kiste|Karton)\s+mit\s+(\d+)\s+Flaschen?\s*\((\d+)cl\)")

# Single bottle: "Flasche (75cl)" or "Flasche im Etui (70cl)"
_SINGLE_BOTTLE_RE = re.compile(r"Flasche(?:\s+im\s+\w+)?\s*\((\d+)cl\)")

# Spirit/non-wine appellations to exclude
_SPIRIT_KEYWORDS = re.compile(
    r"^(Whisky|Gin|Tequila|Rum|Rhum|Cognac|Brandy|Armagnac|Calvados|"
    r"Lik[öo]r|Liqueur|Grappa|Vodka|Mezcal|Sak[ée])\b",
    re.IGNORECASE,
)


def _parse_price(text: str) -> Decimal | None:
    """Parse a price value like '566.00' into Decimal.

    Examples:
        >>> _parse_price("566.00")
        Decimal('566.00')
        >>> _parse_price("23.80")
        Decimal('23.80')
    """
    text = text.strip().replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _parse_wine_name(raw: str) -> tuple[str, str, int | None]:
    """Parse a Millesima wine name into (wine_name, producer, vintage).

    Format: "Producer : Wine Name YYYY" or just "Wine Name YYYY"

    Examples:
        >>> _parse_wine_name("Château La Lagune 2016")
        ('Château La Lagune 2016', '', 2016)
        >>> _parse_wine_name("Alphonse Mellot : La Moussière 2020")
        ('Alphonse Mellot : La Moussière 2020', 'Alphonse Mellot', 2020)
    """
    vintage = None
    year_match = _YEAR_RE.search(raw)
    if year_match:
        vintage = int(year_match.group(1))

    producer = ""
    if " : " in raw:
        producer = raw.split(" : ")[0].strip()

    return raw, producer, vintage


def _parse_pack_info(text: str) -> tuple[int, int]:
    """Extract pack size and bottle ml from pack description.

    Returns (pack_size, bottle_ml).

    Examples:
        >>> _parse_pack_info("Eine Kiste mit 12 Flaschen (75cl)")
        (12, 750)
        >>> _parse_pack_info("Ein Karton mit 6 Flaschen (75cl)")
        (6, 750)
        >>> _parse_pack_info("Flasche (75cl)")
        (1, 750)
    """
    pack_match = _PACK_RE.search(text)
    if pack_match:
        return int(pack_match.group(1)), int(pack_match.group(2)) * 10

    single_match = _SINGLE_BOTTLE_RE.search(text)
    if single_match:
        return 1, int(single_match.group(1)) * 10

    return 1, 750  # default


def _extract_product_from_td(td: Tag) -> ExtractedPromotion | None:
    """Extract a single wine promotion from a product TD block."""
    text = td.get_text("\n", strip=True)
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    if len(lines) < 3:
        return None

    # First line: wine name (in <strong>)
    strong = td.find("strong")
    if not strong:
        return None

    raw_name = strong.get_text(strip=True)
    if not _YEAR_RE.search(raw_name):
        return None

    # Second line: appellation (in <em> or plain text)
    appellation = ""
    em = td.find("em")
    if em:
        appellation = em.get_text(strip=True)
    elif len(lines) > 1:
        appellation = lines[1]

    # Filter out spirits
    if _SPIRIT_KEYWORDS.match(appellation):
        return None

    wine_name, producer, vintage = _parse_wine_name(raw_name)

    # Prices
    sale_price: Decimal | None = None
    original_price: Decimal | None = None

    # Find line-through span for original price
    lt_span = td.find("span", style=lambda s: s and "line-through" in s)
    if lt_span:
        lt_text = lt_span.get_text(strip=True)
        price_match = _PRICE_RE.search(lt_text)
        if price_match:
            original_price = _parse_price(price_match.group(1))

    # Find sale price: first CHF that's NOT in a line-through span
    for p_tag in td.find_all("p"):
        p_text = p_tag.get_text(strip=True)
        if "CHF" not in p_text:
            continue
        # Skip if this p contains the line-through span
        if p_tag.find("span", style=lambda s: s and "line-through" in s):
            continue
        price_match = _PRICE_RE.search(p_text)
        if price_match:
            sale_price = _parse_price(price_match.group(1))
            break

    if sale_price is None:
        # Fallback: find any CHF in the text that isn't the original
        for match in _PRICE_RE.finditer(text):
            price = _parse_price(match.group(1))
            if price and price != original_price:
                sale_price = price
                break

    if sale_price is None:
        return None

    # Pack info
    pack_size, bottle_ml = _parse_pack_info(text)

    # Calculate per-bottle price
    per_bottle_price: Decimal | None = None
    if pack_size > 1:
        per_bottle_price = (sale_price / pack_size).quantize(Decimal("0.01"))
    else:
        per_bottle_price = sale_price

    # Calculate discount
    discount_pct: float | None = None
    if original_price and sale_price < original_price:
        discount_pct = compute_discount_pct(sale_price, original_price)

    # Product URL
    product_url = ""
    link = td.find("a", href=True)
    if link:
        href = link.get("href", "")
        if "millesima" in href:
            product_url = href

    return ExtractedPromotion(
        wine_name=wine_name,
        producer=producer,
        sale_price=per_bottle_price,
        currency="CHF",
        original_price=(original_price / pack_size).quantize(Decimal("0.01"))
        if original_price and pack_size > 1
        else original_price,
        discount_pct=discount_pct,
        vintage=vintage,
        bottle_size_ml=bottle_ml,
        product_url=product_url,
        appellation=appellation,
    )


class MillesimaParser(NewsletterParser):
    """Millesima HTML newsletter parser."""

    retailer_id = "millesima"
    retailer_name = "Millesima"
    sender_patterns = (
        "*@news.millesima.com",
        "*@millesima.com",
    )

    def can_parse(
        self,
        sender: str,
        subject: str,
        text_plain: str,
        text_html: str,
    ) -> bool:
        """Return True if HTML contains wine product blocks."""
        if not text_html:
            return False
        # Quick check: Millesima products use <strong> with year + CHF
        return bool(re.search(r"<strong>.*?(?:19|20)\d{2}", text_html) and "CHF" in text_html)

    def extract(
        self,
        text_plain: str,
        text_html: str,
        subject: str,
    ) -> list[ExtractedPromotion]:
        """Extract wine promotions from Millesima HTML newsletter."""
        if not text_html:
            return []

        soup = BeautifulSoup(text_html, "lxml")
        promotions: list[ExtractedPromotion] = []
        seen: set[str] = set()

        # Find all <strong> tags that contain a year
        for strong in soup.find_all("strong"):
            raw_name = strong.get_text(strip=True)
            if not _YEAR_RE.search(raw_name) or len(raw_name) <= 5:
                continue

            # Walk up to find the product container <td> with CHF
            product_td = _find_product_td(strong)
            if not product_td:
                continue

            promo = _extract_product_from_td(product_td)
            if not promo:
                continue

            # Deduplicate by wine name
            if promo.wine_name in seen:
                continue
            seen.add(promo.wine_name)

            promotions.append(promo)

        logger.debug(
            "Millesima: extracted %d promotions from '%s'",
            len(promotions),
            subject,
        )
        return promotions


def _find_product_td(strong: Tag) -> Tag | None:
    """Walk up from a <strong> to find the product <td> container.

    The product TD contains both the wine name and CHF pricing,
    plus pack/bottle info.
    """
    level = strong
    for _ in range(10):
        level = level.parent
        if not level or not isinstance(level, Tag):
            return None
        if level.name == "td":
            text = level.get_text()
            if "CHF" in text and ("Flasche" in text or "Kiste" in text or "Karton" in text):
                return level
    return None


register(MillesimaParser())
