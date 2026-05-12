"""Registry of all schema migrations.

Migrations are ordered sequentially. Each migration transforms the on-disk
Parquet files from version N to version N+1. The registry validates that
migrations form a contiguous chain with no gaps.
"""

from __future__ import annotations

from ..migrate import MigrationStep
from . import m001_baseline

MIGRATIONS: list[MigrationStep] = [
    m001_baseline.STEP,
]

# Validate ordering — no gaps allowed
for _i, _m in enumerate(MIGRATIONS):
    if _i > 0:
        assert _m.from_version == MIGRATIONS[_i - 1].to_version, (
            f"Migration gap: v{MIGRATIONS[_i - 1].to_version} → v{_m.from_version}"
        )
