"""Locate bundled seed files and copy-on-first-use for mutable artefacts."""

from __future__ import annotations

import logging
import shutil
from importlib.resources import files
from pathlib import Path

logger = logging.getLogger(__name__)

_DATA_PKG = "cellarbrain.sommelier"


def bundled_food_catalogue() -> Path:
    """Return the path to the bundled food catalogue (read-only)."""
    return Path(str(files(_DATA_PKG).joinpath("data", "food_catalogue.parquet")))


def bundled_pairing_dataset() -> Path:
    """Return the path to the bundled seed pairing dataset."""
    return Path(str(files(_DATA_PKG).joinpath("data", "pairing_dataset.parquet")))


def ensure_pairing_dataset(target: Path) -> None:
    """Copy bundled seed pairing dataset to *target* if it doesn't exist.

    If the target already exists (user has accumulated custom pairs), this
    is a no-op.
    """
    if target.exists():
        return
    seed = bundled_pairing_dataset()
    if not seed.exists():
        raise FileNotFoundError(
            f"Bundled seed pairing dataset not found at {seed}. "
            "The cellarbrain package may be incomplete — reinstall with: "
            "pip install cellarbrain[ml]"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(seed), target)
    logger.info("Seeded pairing dataset from %s → %s", seed, target)
