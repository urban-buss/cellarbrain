"""Phonetic matching support for the wine search engine.

Provides Double Metaphone phonetic codes via the ``jellyfish`` library
(optional ``[search]`` extra). When available, registers a DuckDB scalar
Python UDF ``dmetaphone(text)`` on connections for phonetic WHERE clauses.

Public API:
- ``is_available()`` — True if jellyfish is importable
- ``dmetaphone(text)`` — primary Double Metaphone code (Python-side)
- ``register_udfs(con)`` — register the UDF on a DuckDB connection
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_jellyfish = None
_available: bool | None = None


def is_available() -> bool:
    """Return True if the jellyfish library is importable."""
    global _jellyfish, _available
    if _available is not None:
        return _available
    try:
        import jellyfish as _jf

        _jellyfish = _jf
        _available = True
    except ImportError:
        _available = False
    return _available


def dmetaphone(text: str | None) -> str:
    """Return the primary Double Metaphone code for *text*.

    Returns empty string for None/empty input or if jellyfish unavailable.
    """
    if not text or not is_available():
        return ""
    try:
        code = _jellyfish.metaphone(text)  # type: ignore[union-attr]
        return code or ""
    except Exception:
        return ""


def register_udfs(con: object) -> None:
    """Register phonetic UDFs on a DuckDB connection.

    Non-fatal: logs a debug message if jellyfish is not installed.
    """
    if not is_available():
        logger.debug("jellyfish not installed — phonetic UDFs not registered")
        return
    try:
        con.create_function("dmetaphone", dmetaphone, [str], str)  # type: ignore[attr-defined]
        logger.debug("Registered dmetaphone UDF on DuckDB connection")
    except Exception:
        logger.debug("Failed to register dmetaphone UDF", exc_info=True)
