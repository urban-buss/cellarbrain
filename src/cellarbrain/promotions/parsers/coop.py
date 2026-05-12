"""Coop Mondovino newsletter parser (text/plain + HTML fallback).

Extracts promotions from the structured text/plain MIME part.
Product blocks are identified by a bottle-size line (e.g. ``75cl``,
``6x75cl``) followed by a wine name and price. The parser scans
contiguous non-blank line groups looking for this pattern.

When text/plain yields no results, an HTML fallback uses the
``<td class="h80">`` card structure used by Coop Mondovino's
Salesforce Marketing Cloud templates.

Block structure (stripped of blank lines):
    [bottle_size]              e.g. "75cl" or "6x75cl"
    [wine_name_with_year]      e.g. "Amarone della Valpolicella DOCG Palazzo Maffei 2022"
    [optional: << description >>]
    [rating_count]             e.g. "(174)"
    [optional: discount]       e.g. "50%" or "Online 25%"
    [price*]                   e.g. "82.50*"
    [optional: statt X.XX]     e.g. "statt 165.00"
    [product_url]
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal

from bs4 import BeautifulSoup

from ..models import ExtractedPromotion
from ..registry import register
from .base import NewsletterParser

logger = logging.getLogger(__name__)

# Bottle size: "75cl", "70cl", "6x75cl", "6x70cl"
_SIZE_RE = re.compile(r"^(\d+)(?:x(\d+))?(cl)$")

# Price line: bare decimal, optionally with asterisk (e.g. "82.50*", "14.95")
_PRICE_RE = re.compile(r"^(\d+\.\d{2})\*?$")

# Original price: "statt 165.00"
_STATT_RE = re.compile(r"^statt\s+(\d+\.\d{2})$")

# Discount badge: "50%" or "Online 25%"
_DISCOUNT_RE = re.compile(r"(?:Online\s+)?(\d+)%")

# Vintage in wine name
_VINTAGE_RE = re.compile(r"\b((?:19|20)\d{2})\b")

# Rating count: "(174)"
_RATING_COUNT_RE = re.compile(r"^\((\d+)\)$")

# URL
_URL_RE = re.compile(r"^https?://\S+$")

# Description: << ... >>
_DESCRIPTION_RE = re.compile(r"^<<\s*(.+?)\s*>>$")


class CoopParser(NewsletterParser):
    """Coop Mondovino text/plain parser."""

    retailer_id = "coop"
    retailer_name = "Coop Mondovino"
    sender_patterns = ("*@mondovino.ch", "*@news.mondovino.ch")

    def can_parse(
        self,
        sender: str,
        subject: str,
        text_plain: str,
        text_html: str,
    ) -> bool:
        """Accept Mondovino emails that contain product data.

        Matches on: bottle-size lines in text/plain, OR ``<td class="h80">``
        wine-name cards in the HTML part.
        """
        if "mondovino" not in sender.lower():
            return False
        # text/plain: at least one bottle-size indicator
        if re.search(r"^\s*\d+(?:x\d+)?cl\s*$", text_plain, re.MULTILINE):
            return True
        # HTML: at least one h80 wine-name cell
        return 'class="h80' in text_html

    def extract(
        self,
        text_plain: str,
        text_html: str,
        subject: str,
    ) -> list[ExtractedPromotion]:
        """Parse text/plain into promotions, falling back to HTML cards.

        The text/plain path handles Type A emails with bottle-size anchors.
        The HTML fallback handles emails where only the ``<td class="h80">``
        card structure contains product data.
        """
        results = self._extract_from_text(text_plain)
        if not results and text_html:
            results = self._extract_from_html(text_html)
        return results

    # ------------------------------------------------------------------
    # text/plain extraction (original path)
    # ------------------------------------------------------------------

    def _extract_from_text(self, text_plain: str) -> list[ExtractedPromotion]:
        """Extract promotions from the text/plain MIME part."""
        lines = [ln.strip() for ln in text_plain.splitlines() if ln.strip()]
        results: list[ExtractedPromotion] = []
        consumed: set[int] = set()

        for i, line in enumerate(lines):
            if i in consumed:
                continue
            m = _SIZE_RE.match(line)
            if m:
                promo, used = self._parse_from_size(lines, i, m)
                if promo:
                    results.append(promo)
                    consumed.update(used)

        return results

    def _parse_from_size(
        self,
        lines: list[str],
        size_idx: int,
        size_match: re.Match[str],
    ) -> tuple[ExtractedPromotion | None, set[int]]:
        """Parse a product block starting from a bottle-size line.

        Scans forward from ``size_idx`` collecting the wine name, optional
        description, rating count, optional discount badge, price, optional
        original price, and URL.

        Returns ``(promotion, consumed_indices)`` or ``(None, {})`` when
        the lines don't form a valid product block.
        """
        used: set[int] = {size_idx}

        # Parse bottle size
        pack_qty = 1
        if size_match.group(2):
            pack_qty = int(size_match.group(1))
            bottle_ml = int(size_match.group(2)) * 10
        else:
            bottle_ml = int(size_match.group(1)) * 10

        # Scan forward (up to 10 lines) for the components
        wine_name = ""
        discount_pct: float | None = None
        sale_price: Decimal | None = None
        original_price: Decimal | None = None
        product_url = ""
        window = min(size_idx + 12, len(lines))

        for j in range(size_idx + 1, window):
            if j in used:
                continue
            line = lines[j]

            # Price line
            pm = _PRICE_RE.match(line)
            if pm:
                sale_price = Decimal(pm.group(1))
                used.add(j)
                # Check next line for "statt X.XX"
                if j + 1 < len(lines):
                    sm = _STATT_RE.match(lines[j + 1])
                    if sm:
                        original_price = Decimal(sm.group(1))
                        used.add(j + 1)
                continue

            # Discount badge
            dm = _DISCOUNT_RE.match(line)
            if dm:
                discount_pct = float(dm.group(1))
                used.add(j)
                continue

            # Rating count "(174)"
            if _RATING_COUNT_RE.match(line):
                used.add(j)
                continue

            # Description "<< ... >>"
            desc_m = _DESCRIPTION_RE.match(line)
            if desc_m:
                used.add(j)
                continue

            # URL
            if _URL_RE.match(line):
                if not product_url and sale_price is not None:
                    product_url = line
                    used.add(j)
                # Stop after first URL after price
                if sale_price is not None:
                    break
                continue

            # Wine name (first non-structural line after size)
            if not wine_name:
                wine_name = line
                used.add(j)
                continue

            # Unknown line — stop scanning
            break

        if not wine_name or sale_price is None:
            return None, set()

        # Vintage from wine name
        vintage: int | None = None
        vintage_match = _VINTAGE_RE.search(wine_name)
        if vintage_match:
            vintage = int(vintage_match.group(1))

        # If no explicit badge but we have original price, compute it
        if discount_pct is None and original_price and original_price > 0:
            from ..price_utils import compute_discount_pct

            discount_pct = compute_discount_pct(sale_price, original_price)

        # Multi-pack: compute per-bottle price
        per_bottle_price: Decimal | None = None
        is_set = pack_qty > 1
        if is_set:
            per_bottle_price = (sale_price / pack_qty).quantize(Decimal("0.01"))

        return ExtractedPromotion(
            wine_name=wine_name,
            producer="",  # Coop doesn't separate producer from wine name
            sale_price=sale_price,
            currency="CHF",
            original_price=original_price,
            discount_pct=discount_pct,
            vintage=vintage,
            bottle_size_ml=bottle_ml,
            per_bottle_price=per_bottle_price,
            product_url=product_url,
            is_set=is_set,
        ), used

    # ------------------------------------------------------------------
    # HTML card extraction (fallback)
    # ------------------------------------------------------------------

    # Price in 18px font: "7.50*", "125.50*"
    _HTML_PRICE_RE = re.compile(r"(\d+\.\d{2})\*?")
    # "statt 12.95"
    _HTML_STATT_RE = re.compile(r"statt\s+(\d+\.\d{2})")
    # Discount badge: "42%"
    _HTML_DISCOUNT_RE = re.compile(r"(\d+)%")
    # Bottle size in sibling td: "75cl", "70cl"
    _HTML_SIZE_RE = re.compile(r"(\d+)\s*cl")

    def _extract_from_html(self, text_html: str) -> list[ExtractedPromotion]:
        """Extract promotions from HTML using ``<td class="h80">`` wine cards.

        Each card contains the wine name in a ``<td class="h80 ...">`` cell.
        The surrounding table rows hold bottle size, discount badge,
        sale price, and optional original price.
        """
        from ..price_utils import compute_discount_pct

        soup = BeautifulSoup(text_html, "lxml")
        wine_tds = soup.find_all("td", class_=lambda c: c and "h80" in c.split())

        results: list[ExtractedPromotion] = []
        seen: set[str] = set()

        for td in wine_tds:
            wine_name = td.get_text(strip=True)
            if not wine_name or wine_name in seen:
                continue

            # Walk up to the card container table
            card_table = td.find_parent("table")
            if card_table is None:
                continue

            # Bottle size: look for a sibling td with "NNcl" text
            bottle_size_ml = 750
            for sib_td in card_table.find_all("td"):
                sib_text = sib_td.get_text(strip=True)
                size_m = self._HTML_SIZE_RE.fullmatch(sib_text)
                if size_m:
                    bottle_size_ml = int(size_m.group(1)) * 10
                    break

            # Walk up one more level to get the full product card
            # (the card_table is the inner table; price block is in
            # the outer table's subsequent rows)
            outer_table = card_table.find_parent("table")
            search_root = outer_table if outer_table else card_table

            # Discount badge: font text matching "NN%"
            discount_pct: float | None = None
            for font in search_root.find_all("font"):
                text = font.get_text(strip=True)
                dm = self._HTML_DISCOUNT_RE.fullmatch(text)
                if dm:
                    discount_pct = float(dm.group(1))
                    break

            # Sale price: font with 18px size
            sale_price: Decimal | None = None
            original_price: Decimal | None = None
            for font in search_root.find_all("font"):
                style = font.get("style", "")
                text = font.get_text(strip=True)

                if "18px" in style:
                    pm = self._HTML_PRICE_RE.fullmatch(text)
                    if pm:
                        sale_price = Decimal(pm.group(1))
                        continue

                # Original price: "statt X.XX" in smaller font
                sm = self._HTML_STATT_RE.search(text)
                if sm and sale_price is not None:
                    original_price = Decimal(sm.group(1))

            if sale_price is None:
                continue

            seen.add(wine_name)

            # Compute discount if badge present but verify with prices
            if discount_pct is None and original_price and original_price > 0:
                discount_pct = compute_discount_pct(sale_price, original_price)

            # Vintage from wine name
            vintage: int | None = None
            vintage_match = _VINTAGE_RE.search(wine_name)
            if vintage_match:
                vintage = int(vintage_match.group(1))

            # Product URL
            product_url = ""
            for link in search_root.find_all("a", href=True):
                href = link.get("href", "")
                if "mondovino" in href:
                    product_url = href
                    break

            results.append(
                ExtractedPromotion(
                    wine_name=wine_name,
                    producer="",
                    sale_price=sale_price,
                    currency="CHF",
                    original_price=original_price,
                    discount_pct=discount_pct,
                    vintage=vintage,
                    bottle_size_ml=bottle_size_ml,
                    product_url=product_url,
                )
            )

        return results


# Auto-register on import
register(CoopParser())
