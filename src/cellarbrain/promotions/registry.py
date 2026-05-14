"""Parser registry with auto-discovery from the parsers/ package."""

from __future__ import annotations

import fnmatch
import importlib
import logging
import pkgutil
from pathlib import Path

from .parsers.base import NewsletterParser

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, NewsletterParser] = {}
_discovered = False


def register(parser: NewsletterParser) -> None:
    """Register a parser instance by its retailer_id."""
    _REGISTRY[parser.retailer_id] = parser
    logger.debug("Registered parser: %s", parser.retailer_id)


def get_parser(retailer_id: str) -> NewsletterParser | None:
    """Look up a registered parser by retailer ID."""
    return _REGISTRY.get(retailer_id)


def all_parsers() -> dict[str, NewsletterParser]:
    """Return all registered parsers."""
    return dict(_REGISTRY)


def discover_parsers() -> None:
    """Import all modules in the parsers/ package to trigger registration.

    Safe to call multiple times — only discovers once per process.
    """
    global _discovered
    if _discovered:
        return

    package_path = Path(__file__).parent / "parsers"
    for info in pkgutil.iter_modules([str(package_path)]):
        if info.name.startswith("_") or info.name == "base":
            continue
        try:
            importlib.import_module(
                f".parsers.{info.name}",
                package="cellarbrain.promotions",
            )
        except Exception:
            logger.warning("Failed to import parser module: %s", info.name, exc_info=True)

    _discovered = True
    logger.info("Discovered %d newsletter parsers", len(_REGISTRY))


def route_email(
    sender: str,
    subject: str,
    text_plain: str,
    text_html: str,
    config: object,
) -> tuple[str, NewsletterParser] | None:
    """Route an email to the correct parser based on sender + config.

    Returns (retailer_id, parser) or None if no parser matches.
    """
    sender_lower = sender.lower()
    for retailer_id, rcfg in config.retailers.items():
        if not rcfg.enabled:
            continue
        if _sender_matches(sender_lower, rcfg.sender_patterns):
            parser = get_parser(rcfg.parser or retailer_id)
            if parser and parser.can_parse(sender, subject, text_plain, text_html):
                return retailer_id, parser
    return None


def _sender_matches(sender: str, patterns: tuple[str, ...] | list[str]) -> bool:
    """Check if a sender email matches any of the glob patterns."""
    return any(fnmatch.fnmatch(sender, pattern) for pattern in patterns)


def reset_registry() -> None:
    """Clear the registry (for testing only)."""
    global _discovered
    _REGISTRY.clear()
    _discovered = False
