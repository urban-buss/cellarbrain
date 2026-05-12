"""Abstract base class for newsletter parsers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal

from ..models import ExtractedPromotion


class NewsletterParser(ABC):
    """Base class for retailer-specific newsletter parsers.

    Subclasses must define:
    - ``retailer_id``: unique string identifier (matches config key)
    - ``retailer_name``: human-readable name
    - ``sender_patterns``: tuple of glob patterns for sender matching
    - ``can_parse()``: confirm this parser handles the email
    - ``extract()``: parse email content and return promotions

    The parser receives BOTH text/plain and text/html parts. It decides
    which to use (or both). This allows KapWeine to use text/plain while
    Mövenpick uses text/html.
    """

    retailer_id: str
    retailer_name: str
    sender_patterns: tuple[str, ...]

    @abstractmethod
    def can_parse(
        self,
        sender: str,
        subject: str,
        text_plain: str,
        text_html: str,
    ) -> bool:
        """Return True if this parser can handle the given email.

        Called after sender routing confirms the retailer. Use this to
        reject emails the parser cannot handle (editorials, confirmations).
        A return of False means the email is skipped (not an error).
        """

    @abstractmethod
    def extract(
        self,
        text_plain: str,
        text_html: str,
        subject: str,
    ) -> list[ExtractedPromotion]:
        """Parse email content and return extracted promotions.

        Returns an empty list for emails with no parseable products.
        Must NOT raise exceptions for unparseable content — log and
        return empty instead.
        """

    def normalise_price(self, raw: str) -> tuple[Decimal, str]:
        """Parse a price string like 'CHF 29.90' into (Decimal, currency).

        Shared utility available to all parsers. Override for non-standard
        formats.
        """
        from ..price_utils import parse_price

        return parse_price(raw)
