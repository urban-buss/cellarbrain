"""Snapshot file placement for email-ingested CSV batches.

Writes batch files to a timestamped snapshot folder and flushes the
top-level ``raw/`` working-set CSVs.  Pure filesystem operations.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class SnapshotCollisionError(Exception):
    """Raised when the target snapshot folder already exists."""


def place_batch(
    batch_files: dict[str, bytes],
    raw_dir: Path,
    *,
    now: datetime | None = None,
) -> Path:
    """Write *batch_files* to a snapshot folder and flush top-level CSVs.

    Creates ``raw_dir / YYMMDD-HHMM /`` as the primary historical record,
    then replaces all ``*.csv`` files at the top level of *raw_dir* with
    copies from the snapshot.

    Parameters
    ----------
    batch_files:
        Mapping of ``{filename: raw_bytes}`` for each CSV attachment.
    raw_dir:
        Root directory for raw exports (e.g. ``Path("raw")``).
    now:
        Override current time for deterministic testing.

    Returns
    -------
    Path to the snapshot directory.

    Raises
    ------
    SnapshotCollisionError
        If the snapshot directory already exists (same-minute collision).
    """
    ts = now or datetime.now()
    snapshot_name = ts.strftime("%y%m%d-%H%M")
    snapshot_dir = raw_dir / snapshot_name

    if snapshot_dir.exists():
        raise SnapshotCollisionError(f"Snapshot folder already exists: {snapshot_dir}")

    snapshot_dir.mkdir(parents=True)

    # 1. Write to snapshot folder (primary record)
    for filename, data in batch_files.items():
        (snapshot_dir / filename).write_bytes(data)
    logger.info("Snapshot written to %s", snapshot_dir)

    # 2. Flush top-level CSV files (remove all, then write all)
    for existing in raw_dir.glob("*.csv"):
        existing.unlink()

    # 3. Copy from snapshot to top-level
    for filename in batch_files:
        shutil.copy2(snapshot_dir / filename, raw_dir / filename)
    logger.info("Flushed raw/*.csv, replaced with batch %s", snapshot_name)

    return snapshot_dir
