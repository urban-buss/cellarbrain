"""Denner newsletter parser (HTML extraction).

Extracts wine promotions from Denner's HTML newsletter format.
Product sections are identified by ``<!-- sale, price -->`` HTML comments,
with product names in ``<!-- name -->``/``<!-- text -->`` comment-delimited
sections and prices in 18px-font div elements.

HTML structure per product row:
    <!-- sale, price -->
    Parent tbody contains:
        <!-- name -->   → bold 12px link with wine name
        <!-- text -->   → gray 10px link with origin (country, region, year, size)
        price divs      → 18px font-size with "X.XXstatt Y.YY" format

Products are matched positionally (1st name ↔ 1st price within each section).
Wine products are filtered by bottle size (75cl or 70cl) in the origin text.
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation

from bs4 import BeautifulSoup, Comment

from ..models import ExtractedPromotion
from ..price_utils import compute_discount_pct
from ..registry import register
from .base import NewsletterParser

logger = logging.getLogger(__name__)

# Origin parsing: "Italien, Venetien, 2023, 75 cl" or "Portugal, 6 x 75 cl"
_VINTAGE_RE = re.compile(r"\b((?:19|20)\d{2})(?:/\d{2,4})?\b")
_SIZE_RE = re.compile(r"(\d+)\s*x\s*(\d+)\s*cl|(\d+)\s*cl")

# Wine bottle sizes (standard wine = 70cl or 75cl)
_WINE_SIZES = {"70", "75"}

# Price text: "13.75statt 27.50*" or "81.–statt 137.70" or just "8.50"
_PRICE_TEXT_RE = re.compile(
    r"(?P<sale>\d+(?:[.,]\d{2}|[.,][\u2013\u2014\-–—]))\s*"
    r"(?:statt\s+(?P<orig>\d+(?:[.,]\d{2}|[.,][\u2013\u2014\-–—])))?"
    r"\*?"
)

# Non-wine product name keywords (to reject false positives)
_NON_WINE_KEYWORDS = re.compile(
    r"\b(Bier|Beer|Ice Tea|Mineralwasser|Rivella|Feldschlösschen|"
    r"Coca.Cola|Pepsi|Fanta|Sprite|Red Bull|Monster|Eistee|"
    r"Limonade|Sirup|Energy|Saft)\b",
    re.IGNORECASE,
)


def _parse_denner_price(text: str) -> Decimal:
    """Parse a Denner price string like '13.75', '81.–', '15.û'.

    Examples:
        >>> _parse_denner_price("13.75")
        Decimal('13.75')
        >>> _parse_denner_price("81.–")
        Decimal('81.00')
    """
    # Normalise dash-style zero-cents
    text = re.sub(r"[.,][\u2013\u2014\-–—û]+$", ".00", text.strip())
    text = text.replace(",", ".")
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"Cannot parse Denner price: {text!r}") from exc


def _parse_origin(origin: str) -> dict:
    """Parse origin text into components.

    Examples:
        >>> _parse_origin("Italien, Venetien, 2023, 75 cl")
        {'country': 'Italien', 'region': 'Venetien', 'vintage': 2023, 'pack_size': 1, 'bottle_ml': 750}
        >>> _parse_origin("Portugal, 6 x 75 cl")
        {'country': 'Portugal', 'region': '', 'vintage': None, 'pack_size': 6, 'bottle_ml': 750}
    """
    parts = [p.strip() for p in origin.split(",")]
    country = parts[0] if parts else ""
    region = ""
    vintage = None

    # Parse vintage
    for part in parts[1:]:
        vm = _VINTAGE_RE.search(part)
        if vm:
            vintage = int(vm.group(1))
            break

    # Parse size
    pack_size = 1
    bottle_ml = 750
    size_match = _SIZE_RE.search(origin)
    if size_match:
        if size_match.group(1):
            # "6 x 75 cl" format
            pack_size = int(size_match.group(1))
            bottle_ml = int(size_match.group(2)) * 10
        else:
            # "75 cl" format
            bottle_ml = int(size_match.group(3)) * 10

    # Region: non-vintage, non-size parts after country
    for part in parts[1:]:
        if _VINTAGE_RE.search(part):
            continue
        if _SIZE_RE.search(part):
            continue
        if part.strip():
            region = part.strip()
            break

    return {
        "country": country,
        "region": region,
        "vintage": vintage,
        "pack_size": pack_size,
        "bottle_ml": bottle_ml,
    }


def _is_wine_origin(origin: str) -> bool:
    """Check if the origin text indicates a wine product (75cl or 70cl)."""
    size_match = _SIZE_RE.search(origin)
    if not size_match:
        return False
    if size_match.group(1):
        # "N x Y cl" — check Y
        return size_match.group(2) in _WINE_SIZES
    # "Y cl"
    return size_match.group(3) in _WINE_SIZES


class DennerParser(NewsletterParser):
    """Denner HTML newsletter parser."""

    retailer_id = "denner"
    retailer_name = "Denner"
    sender_patterns = ("*@news.denner.ch", "*@denner.ch")

    def can_parse(
        self,
        sender: str,
        subject: str,
        text_plain: str,
        text_html: str,
    ) -> bool:
        """Accept Denner emails that have HTML content with sale sections."""
        if not any(p.replace("*", "") in sender.lower() for p in self.sender_patterns):
            return False
        return "<!-- sale, price -->" in text_html

    def extract(
        self,
        text_plain: str,
        text_html: str,
        subject: str,
    ) -> list[ExtractedPromotion]:
        """Parse HTML content and return wine promotions."""
        if not text_html:
            return []

        soup = BeautifulSoup(text_html, "lxml")
        results: list[ExtractedPromotion] = []
        seen: set[tuple[str, str]] = set()  # deduplicate (name, price)

        # Find all product sections anchored by <!-- sale, price --> comments
        sale_comments = soup.find_all(string=lambda t: isinstance(t, Comment) and t.strip() == "sale, price")

        for sc in sale_comments:
            parent_tbody = sc.find_parent("tbody")
            if not parent_tbody:
                continue

            # Extract product names and origins from <!-- name --> sections
            products = self._extract_products(parent_tbody)

            # Extract prices from 18px font-size divs
            prices = self._extract_prices(parent_tbody)

            # Match positionally and filter for wine
            for idx, product in enumerate(products):
                name = product["name"]
                origin = product["origin"]

                # Filter: must be wine (75cl or 70cl)
                if not _is_wine_origin(origin):
                    continue

                # Filter: reject non-wine products by name keywords
                if _NON_WINE_KEYWORDS.search(name):
                    continue

                # Get price (positional match)
                price_text = prices[idx] if idx < len(prices) else None
                if not price_text:
                    logger.debug("No price for product %r at index %d", name, idx)
                    continue

                # Parse price
                sale_price, original_price = self._parse_price_text(price_text)
                if sale_price is None:
                    continue

                # Deduplicate (desktop/mobile views produce duplicates)
                dedup_key = (name, str(sale_price))
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                # Parse origin details
                origin_info = _parse_origin(origin)

                # Compute discount percentage
                discount_pct = None
                if original_price and original_price > 0:
                    discount_pct = compute_discount_pct(sale_price, original_price)

                # Compute per-bottle price for multi-packs
                per_bottle = None
                pack_size = origin_info["pack_size"]
                if pack_size > 1:
                    per_bottle = (sale_price / pack_size).quantize(Decimal("0.01"))

                results.append(
                    ExtractedPromotion(
                        wine_name=name,
                        producer="",  # Denner doesn't separate producer from name
                        sale_price=sale_price,
                        currency="CHF",
                        original_price=original_price,
                        discount_pct=discount_pct,
                        vintage=origin_info["vintage"],
                        appellation=origin_info["region"],
                        bottle_size_ml=origin_info["bottle_ml"],
                        per_bottle_price=per_bottle,
                        product_url=product.get("url", ""),
                        category=origin_info["country"],
                        is_set=pack_size > 1,
                    )
                )

        return results

    def _extract_products(self, tbody) -> list[dict]:
        """Extract product names and origins from <!-- name --> sections."""
        products = []
        for comment in tbody.find_all(string=lambda t: isinstance(t, Comment) and t.strip() == "name"):
            name_tbody = comment.find_parent("tbody")
            if not name_tbody:
                continue

            # Name: bold 12px div with link
            name_div = name_tbody.find("div", style=lambda s: s and "font-weight: 700" in s)
            # Origin: gray 10px div with link
            text_div = name_tbody.find("div", style=lambda s: s and "color: #727272" in s)

            if not name_div:
                continue

            wine_name = name_div.get_text(strip=True)
            origin = ""
            if text_div:
                origin = text_div.get_text(strip=True).replace("\xa0", " ")

            # Extract product URL from the link
            link = name_div.find("a")
            url = link.get("href", "") if link else ""

            products.append({"name": wine_name, "origin": origin, "url": url})

        return products

    def _extract_prices(self, tbody) -> list[str]:
        """Extract price texts from 18px font-size divs."""
        prices = []
        for div in tbody.find_all(
            "div",
            style=lambda s: s and "font-size:18px" in s and "mso-line-height" in s,
        ):
            text = div.get_text(strip=True).replace("\xa0", " ")
            if re.search(r"\d", text):
                prices.append(text)
        return prices

    def _parse_price_text(self, text: str) -> tuple[Decimal | None, Decimal | None]:
        """Parse price text like '13.75statt 27.50*' into (sale, original).

        Returns (None, None) if parsing fails.
        """
        match = _PRICE_TEXT_RE.search(text)
        if not match:
            logger.debug("Cannot parse price text: %r", text)
            return None, None

        try:
            sale = _parse_denner_price(match.group("sale"))
        except ValueError:
            logger.debug("Cannot parse sale price from: %r", text)
            return None, None

        original = None
        if match.group("orig"):
            try:
                original = _parse_denner_price(match.group("orig"))
            except ValueError:
                logger.debug("Cannot parse original price from: %r", text)

        return sale, original


register(DennerParser())
