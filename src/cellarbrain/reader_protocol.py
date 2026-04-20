"""Protocol definition for cellar CSV readers.

Defines the :class:`CellarReader` structural interface that any
source-specific reader module (e.g. ``vinocell_reader``) should satisfy.
Not enforced at runtime — serves as documentation and future type-check target.
"""

from __future__ import annotations

from typing import Protocol


class CellarReader(Protocol):
    """Structural interface for reading cellar CSV exports."""

    def read_wines(self, path: str) -> list[dict[str, str | None]]:
        """Read the wines export and return rows with canonical column names."""
        ...

    def read_bottles(self, path: str) -> list[dict[str, str | None]]:
        """Read the stored-bottles export and return rows with canonical column names."""
        ...

    def read_bottles_gone(self, path: str) -> list[dict[str, str | None]]:
        """Read the consumed/removed-bottles export and return rows with canonical column names."""
        ...
