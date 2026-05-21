"""Shared exception types for cellarbrain."""

from __future__ import annotations


class QueryError(Exception):
    """SQL validation or execution error."""


class DataStaleError(Exception):
    """Parquet files missing or corrupted."""


class WineNotFoundError(Exception):
    """Wine ID does not exist or dossier file is missing."""


class ProtectedSectionError(Exception):
    """Attempted to write to an invalid or ETL-owned section."""


class TrackedWineNotFoundError(Exception):
    """Tracked wine ID does not exist or companion dossier is missing."""
