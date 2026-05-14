"""Mövenpick Wein newsletter parser (HTML extraction).

Extracts wine promotions from Mövenpick's HTML newsletter format.
Product blocks are identified by ``<b>`` tags with year-prefixed wine names
(e.g. "2024 Vora") inside ``<table width="100%">`` containers.

HTML structure per product block:
    <table width="100%">
        <tr> region span (color #9d0f38, format "Country | Region")
        <tr> wine name (<b>YYYY Name</b>) + appellation in <a> link
        <tr> marketing description (17px, color #9d0f38)
        <tr> optional rating span (e.g. "Parker 94/100")
        <tr> optional discount image (Prozent_XX.png)
        <tr> sale price (24px bold, color #9d0f38)
        <tr> optional original price (<strike>CHF XX.XX</strike>)
        <tr> bottle size (<strong>75 cl</strong>) + per-litre price

Desktop/mobile duplicate views produce identical product blocks;
deduplication uses (wine_name, sale_price) as the identity key.
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

# Wine name pattern: starts with 4-digit year
_WINE_NAME_RE = re.compile(r"^((?:19|20)\d{2})\s+(.+)")

# Price pattern: "CHF  11.90" or "CHF 23.80"
_PRICE_RE = re.compile(r"CHF\s+([\d]+[.,][\d]{2})")

# Rating patterns: "Parker 94/100", "James Suckling 90/100", "WeinWisser 19.5+/20", "John Platter 5/5"
_RATING_RE = re.compile(
    r"(Parker|James Suckling|Suckling|Falstaff|Decanter|Wine Spectator|"
    r"WeinWisser|John Platter|Vinous)\s+([\d.+]+)/(\d+)"
)

# Bottle size: "75 cl" or "150 cl"
_BOTTLE_SIZE_RE = re.compile(r"(\d+)\s*cl")

# Discount image: Prozent_50.png → 50%
_DISCOUNT_IMG_RE = re.compile(r"Prozent_(\d+)")


def _parse_price(text: str) -> Decimal | None:
    """Parse a price value like '11.90' into Decimal.

    Examples:
        >>> _parse_price("11.90")
        Decimal('11.90')
        >>> _parse_price("23.80")
        Decimal('23.80')
    """
    text = text.strip().replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _extract_product_from_table(table: Tag) -> ExtractedPromotion | None:
    """Extract a single wine promotion from a product table block.

    Returns None if the block cannot be parsed as a wine product.
    """
    text = table.get_text("\n", strip=True)
    lines = [line.strip() for line in text.split("\n") if line.strip()]

    if len(lines) < 5:
        return None

    # Find wine name <b> tag with year prefix
    b_tag = table.find("b", string=_WINE_NAME_RE)
    if not b_tag:
        return None

    wine_text = b_tag.get_text(strip=True)
    name_match = _WINE_NAME_RE.match(wine_text)
    if not name_match:
        return None

    vintage = int(name_match.group(1))
    wine_name = wine_text  # Keep full "YYYY Name" as wine_name

    # Appellation: text after <b> in the same <a> link (br-separated)
    appellation = ""
    link = b_tag.find_parent("a")
    if link:
        # Get all text in the link, excluding the bold part
        link_text = link.get_text("\n", strip=True)
        parts = [p.strip() for p in link_text.split("\n") if p.strip()]
        # First part is wine name, rest is appellation
        if len(parts) > 1:
            appellation = parts[1]

    # Product URL
    product_url = ""
    if link:
        product_url = link.get("href", "")

    # Region: span with pipe separator (first line typically)
    region = ""
    for line in lines[:2]:
        if "|" in line:
            region = line
            break

    # Rating: look for rating pattern in text
    rating_score: str | None = None
    rating_source: str | None = None
    rating_match = _RATING_RE.search(text)
    if rating_match:
        rating_source = rating_match.group(1)
        rating_score = f"{rating_match.group(2)}/{rating_match.group(3)}"

    # Prices: find sale price (22-24px bold red) and original (strike)
    sale_price: Decimal | None = None
    original_price: Decimal | None = None

    # Sale price: large font span (22px in subscriptions, 24px in regular)
    sale_span = table.find(
        "span",
        style=lambda s: s and re.search(r"font-size:\s*2[2-4]px", s) is not None,
    )
    if sale_span:
        price_match = _PRICE_RE.search(sale_span.get_text())
        if price_match:
            sale_price = _parse_price(price_match.group(1))

    # Original price: <strike> element (only if it contains text)
    strike = table.find("strike")
    if strike:
        strike_text = strike.get_text(strip=True)
        if strike_text:
            price_match = _PRICE_RE.search(strike_text)
            if price_match:
                original_price = _parse_price(price_match.group(1))

    if sale_price is None:
        # Fallback: find CHF prices excluding per-litre "(CHF X / l)"
        for span in table.find_all("span", style=True):
            span_text = span.get_text(strip=True)
            if "/ l)" in span_text:
                continue
            price_match = _PRICE_RE.search(span_text)
            if price_match:
                sale_price = _parse_price(price_match.group(1))
                break

    if sale_price is None:
        return None

    # Bottle size
    bottle_size_ml = 750  # default
    size_match = _BOTTLE_SIZE_RE.search(text)
    if size_match:
        bottle_size_ml = int(size_match.group(1)) * 10

    # Discount percentage from image
    discount_pct: float | None = None
    for img in table.find_all("img"):
        src = img.get("src", "")
        disc_match = _DISCOUNT_IMG_RE.search(src)
        if disc_match:
            discount_pct = float(disc_match.group(1))
            break

    # Calculate discount from prices if not from image
    if discount_pct is None and original_price and sale_price:
        discount_pct = compute_discount_pct(sale_price, original_price)

    return ExtractedPromotion(
        wine_name=wine_name,
        producer="",
        sale_price=sale_price,
        currency="CHF",
        original_price=original_price,
        discount_pct=discount_pct,
        vintage=vintage,
        bottle_size_ml=bottle_size_ml,
        product_url=product_url,
        rating_score=rating_score,
        rating_source=rating_source,
        appellation=appellation,
        category=region,
    )


class MoevenpickParser(NewsletterParser):
    """Mövenpick Wein HTML newsletter parser."""

    retailer_id = "moevenpick"
    retailer_name = "Mövenpick Wein"
    sender_patterns = (
        "*@moevenpick-wein.com",
        "*@newsletter.moevenpick-wein.com",
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
        # Quick check: look for year-prefixed bold text pattern
        return bool(re.search(r"<b>\s*20\d{2}\s+\w", text_html))

    def extract(
        self,
        text_plain: str,
        text_html: str,
        subject: str,
    ) -> list[ExtractedPromotion]:
        """Extract wine promotions from Mövenpick HTML newsletter."""
        if not text_html:
            return []

        soup = BeautifulSoup(text_html, "lxml")
        promotions: list[ExtractedPromotion] = []
        seen: set[tuple[str, Decimal]] = set()

        # Find all <b> tags with year-prefixed wine names
        wine_bolds = soup.find_all("b", string=_WINE_NAME_RE)

        for b_tag in wine_bolds:
            # Walk up to find the product table container
            product_table = _find_product_table(b_tag)
            if not product_table:
                continue

            promo = _extract_product_from_table(product_table)
            if not promo:
                continue

            # Deduplicate (desktop/mobile duplicate views)
            key = (promo.wine_name, promo.sale_price)
            if key in seen:
                continue
            seen.add(key)

            promotions.append(promo)

        logger.debug(
            "Mövenpick: extracted %d promotions from '%s'",
            len(promotions),
            subject,
        )
        return promotions


def _find_product_table(b_tag: Tag) -> Tag | None:
    """Walk up from a wine name <b> tag to find the product table container.

    The product table is a ``<table width="100%">`` that contains both
    the wine name and a CHF price.
    """
    level = b_tag
    for _ in range(10):
        level = level.parent
        if not level or not isinstance(level, Tag):
            return None
        if level.name == "table":
            text = level.get_text(" ", strip=True)
            if "CHF" in text:
                return level
    return None


register(MoevenpickParser())
