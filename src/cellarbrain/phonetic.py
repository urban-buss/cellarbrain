"""Phonetic matching support for the wine search engine.

Provides Double Metaphone phonetic codes via the ``jellyfish`` library.
Registers a DuckDB scalar Python UDF ``dmetaphone(text)`` on connections
for phonetic WHERE clauses.

Public API:
- ``is_available()`` — always True (jellyfish is a core dependency)
- ``dmetaphone(text)`` — primary Double Metaphone code (Python-side)
- ``register_udfs(con)`` — register the UDF on a DuckDB connection
"""

from __future__ import annotations

import logging

import jellyfish

logger = logging.getLogger(__name__)


def is_available() -> bool:
    """Return True if the jellyfish library is importable.

    Always returns True since jellyfish is a core dependency as of v0.3.0.
    Kept for backward compatibility with callers.
    """
    return True


def dmetaphone(text: str | None) -> str:
    """Return the primary Double Metaphone code for *text*.

    Returns empty string for None/empty input.
    """
    if not text:
        return ""
    try:
        code = jellyfish.metaphone(text)
        return code or ""
    except Exception:
        return ""


def register_udfs(con: object) -> None:
    """Register phonetic UDFs on a DuckDB connection."""
    try:
        con.create_function("dmetaphone", dmetaphone, [str], str)  # type: ignore[attr-defined]
        logger.debug("Registered dmetaphone UDF on DuckDB connection")
    except Exception:
        logger.debug("Failed to register dmetaphone UDF", exc_info=True)
