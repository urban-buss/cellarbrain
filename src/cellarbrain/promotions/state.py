"""Processed-newsletter state persistence.

Maintains a JSON state file tracking which email UIDs have been
processed, scan history, and per-retailer statistics.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_STATE_FILE = ".promotions-state.json"


def load_state(data_dir: Path) -> dict:
    """Load the promotions state file, or return empty state."""
    path = data_dir / _STATE_FILE
    if not path.exists():
        return {"processed_uids": [], "last_scan": None, "scan_history": []}
    with open(path, encoding="utf-8") as f:
        state = json.load(f)
    state.setdefault("processed_uids", [])
    state.setdefault("last_scan", None)
    state.setdefault("scan_history", [])
    return state


def save_state(data_dir: Path, state: dict) -> None:
    """Persist the promotions state file."""
    path = data_dir / _STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    logger.debug("Saved promotions state (%d processed UIDs)", len(state.get("processed_uids", [])))
