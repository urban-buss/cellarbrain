"""Baseline migration — establishes version 1 for existing datasets.

This is a no-op: it stamps the version file, confirming that the existing
Parquet files conform to the initial schema definition.
"""

from __future__ import annotations

import pathlib

from ..migrate import MigrationStep


def _migrate(data_dir: pathlib.Path) -> None:
    """No-op — existing data already conforms to version 1."""


STEP = MigrationStep(
    from_version=0,
    to_version=1,
    description="Establish baseline schema version",
    entities=(),
    migrate=_migrate,
)
