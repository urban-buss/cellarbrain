"""KapWeine newsletter parser (text/plain extraction).

Extracts promotions from the structured text/plain MIME part.
Products are separated by 28-underscore separator lines. Each block:
    Category line (e.g. "Flash Sale", "Winter Sale")
    PRODUCER
    Wine Name - Vintage
    CHF XX.XX statt CHF YY.YY
    CTA URL line
    ____________________________
    Rating line (optional, e.g. "95 Points by Tim Atkin")
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal

from ..models import ExtractedPromotion
from ..registry import register
from .base import NewsletterParser

logger = logging.getLogger(__name__)

_SEPARATOR = "____________________________"

# Regex patterns
_PRICE_RE = re.compile(r"CHF\s+(\d+[.,]\d{2})")
_STATT_RE = re.compile(r"statt\s+CHF\s+(\d+[.,]\d{2})")
_VINTAGE_RE = re.compile(r"\b((?:19|20)\d{2})\b")
_RATING_RE = re.compile(r"(\d+)\s+(?:Points?|Stars?)\s+(?:(?:by|von)\s+)?(.+)", re.IGNORECASE)
_URL_RE = re.compile(r"\(?(https?://\S+)\)?")

# Categories that indicate non-wine products
_NON_WINE_CATEGORIES = frozenset({"food", "accessoires", "zubehör"})


class KapweineParser(NewsletterParser):
    """KapWeine text/plain parser."""

    retailer_id = "kapweine"
    retailer_name = "KapWeine"
    sender_patterns = ("*@kapweine.ch",)

    def can_parse(
        self,
        sender: str,
        subject: str,
        text_plain: str,
        text_html: str,
    ) -> bool:
        """KapWeine emails must have the underscore separator in text/plain."""
        return "kapweine" in sender.lower() and _SEPARATOR in text_plain

    def extract(
        self,
        text_plain: str,
        text_html: str,
        subject: str,
    ) -> list[ExtractedPromotion]:
        """Parse text/plain blocks into promotions."""
        blocks = text_plain.split(_SEPARATOR)
        results: list[ExtractedPromotion] = []

        for i, block in enumerate(blocks):
            block = block.strip()
            if not block:
                continue

            promo = self._parse_block(block, next_block=blocks[i + 1] if i + 1 < len(blocks) else "")
            if promo:
                results.append(promo)

        return results

    def _parse_block(self, block: str, next_block: str = "") -> ExtractedPromotion | None:
        """Parse a single product block from text/plain.

        Block structure (lines):
            [0] Category (e.g. "Flash Sale", "Winter Sale")
            [1] Producer (often UPPERCASE)
            [2] Wine Name - Vintage
            [3] CHF XX.XX statt CHF YY.YY
            [4] CTA line with URL
            --- separator ---
            [next_block first line] Rating (optional)

        Examples:
            >>> parser = KapweineParser()
            >>> parser._parse_block(
            ...     "Winter Sale\\nCARINUS\\nChenin Blanc Rooidraai - 2022\\n"
            ...     "CHF 17.90 statt CHF 29.00\\n"
            ...     "Zum Angebot » (https://example.com)"
            ... )  # doctest: +SKIP
        """
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if len(lines) < 3:
            return None

        # Find price line (the anchor)
        price_idx = None
        for idx, line in enumerate(lines):
            if _PRICE_RE.search(line):
                price_idx = idx
                break

        if price_idx is None:
            return None

        # Extract price
        sale_match = _PRICE_RE.search(lines[price_idx])
        if not sale_match:
            return None
        sale_price = Decimal(sale_match.group(1).replace(",", "."))

        # Original price
        original_price: Decimal | None = None
        statt_match = _STATT_RE.search(lines[price_idx])
        if statt_match:
            original_price = Decimal(statt_match.group(1).replace(",", "."))

        # Category is three lines before price (category / producer / wine / price)
        category = lines[price_idx - 3] if price_idx >= 3 else ""

        # Producer is the line before the wine name
        producer_idx = max(0, price_idx - 2)
        producer = lines[producer_idx] if price_idx >= 2 else ""

        # Wine name + vintage
        wine_line_idx = price_idx - 1
        wine_line = lines[wine_line_idx] if wine_line_idx >= 0 else ""

        wine_name, vintage = self._parse_wine_line(wine_line)

        # Skip non-wine categories
        if category.lower() in _NON_WINE_CATEGORIES:
            return None

        # Product URL
        product_url = ""
        for line in lines[price_idx:]:
            url_match = _URL_RE.search(line)
            if url_match:
                product_url = url_match.group(1).rstrip(")")
                break

        # Rating from the next block's first line
        rating_score = ""
        rating_source = ""
        if next_block:
            first_line = next_block.strip().splitlines()[0].strip() if next_block.strip() else ""
            rating_match = _RATING_RE.match(first_line)
            if rating_match:
                rating_score = f"{rating_match.group(1)}/100"
                rating_source = rating_match.group(2).strip()

        # Compute discount
        discount_pct: float | None = None
        if original_price and original_price > 0:
            from ..price_utils import compute_discount_pct

            discount_pct = compute_discount_pct(sale_price, original_price)

        return ExtractedPromotion(
            wine_name=wine_name,
            producer=producer,
            sale_price=sale_price,
            currency="CHF",
            original_price=original_price,
            discount_pct=discount_pct,
            vintage=vintage,
            category=category,
            product_url=product_url,
            rating_score=rating_score,
            rating_source=rating_source,
        )

    def _parse_wine_line(self, line: str) -> tuple[str, int | None]:
        """Split a wine line into name and vintage.

        Examples:
            >>> KapweineParser()._parse_wine_line("Chenin Blanc Rooidraai - 2022")
            ('Chenin Blanc Rooidraai', 2022)
            >>> KapweineParser()._parse_wine_line("Sauvignon Blanc NV")
            ('Sauvignon Blanc NV', None)
        """
        vintage: int | None = None
        vintage_match = _VINTAGE_RE.search(line)
        if vintage_match:
            vintage = int(vintage_match.group(1))

        # Remove vintage and separator from name
        name = line
        if vintage:
            # Remove " - 2022" or " 2022" from end
            name = re.sub(r"\s*[-–—]\s*" + str(vintage), "", name)
            name = re.sub(r"\s+" + str(vintage) + r"$", "", name)

        return name.strip(), vintage


# Auto-register on import
register(KapweineParser())
