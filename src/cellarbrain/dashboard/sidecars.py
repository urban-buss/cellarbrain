"""JSON sidecar files for dashboard-only state.

Two sidecars live under ``<data_dir>/`` and are read/written by both the
dashboard routes and the MCP layer:

- ``.consumed-pending.json`` — bottle IDs the user has marked "consumed" in
  the dashboard but which still need to be reflected in Vinocell. Cleared
  automatically by ``prune_consumed_after_etl()`` once the next ETL run
  imports the matching ``bottles-gone`` row (i.e. the bottle's status is no
  longer ``stored``).
- ``.drink-tonight.json`` — wines the user has added to the "drink tonight"
  shortlist. The dashboard owns the primary copy in ``localStorage``; this
  sidecar is the server-side mirror that lets agents read the same list via
  MCP.

Both files use atomic writes (write-temp-then-rename) and tolerate missing /
malformed files by returning empty structures.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import tempfile
from datetime import UTC, datetime

logger = logging.getLogger(__name__)

CONSUMED_PENDING_FILENAME = ".consumed-pending.json"
DRINK_TONIGHT_FILENAME = ".drink-tonight.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _atomic_write(path: pathlib.Path, payload: dict) -> None:
    """Write *payload* as JSON to *path* atomically (write-temp + rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".cb-sidecar-", suffix=".json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _safe_read(path: pathlib.Path) -> dict | None:
    """Read a JSON sidecar; return ``None`` on missing/invalid file."""
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            logger.warning("Sidecar %s does not contain a JSON object; ignoring", path)
            return None
        return data
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to read sidecar %s: %s", path, exc)
        return None


def _resolve(data_dir: str | pathlib.Path, filename: str) -> pathlib.Path:
    return pathlib.Path(data_dir) / filename


# ---------------------------------------------------------------------------
# Consumed-pending
# ---------------------------------------------------------------------------


def consumed_pending_path(data_dir: str | pathlib.Path) -> pathlib.Path:
    return _resolve(data_dir, CONSUMED_PENDING_FILENAME)


def read_consumed_pending(data_dir: str | pathlib.Path) -> list[dict]:
    """Return the list of pending-consumed bottle entries.

    Each entry is a dict with at least ``bottle_id`` (int), ``wine_id``
    (int), ``marked_at`` (ISO timestamp string), and optional ``note``.
    """
    data = _safe_read(consumed_pending_path(data_dir))
    if data is None:
        return []
    items = data.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict) and "bottle_id" in item]


def add_consumed_pending(
    data_dir: str | pathlib.Path,
    *,
    bottle_id: int,
    wine_id: int,
    note: str | None = None,
) -> list[dict]:
    """Add a bottle to the consumed-pending sidecar (idempotent).

    Returns the full updated item list.
    """
    items = read_consumed_pending(data_dir)
    if any(int(it["bottle_id"]) == int(bottle_id) for it in items):
        return items
    items.append(
        {
            "bottle_id": int(bottle_id),
            "wine_id": int(wine_id),
            "marked_at": _now_iso(),
            "note": note,
        }
    )
    _atomic_write(consumed_pending_path(data_dir), {"items": items, "updated_at": _now_iso()})
    return items


def remove_consumed_pending(data_dir: str | pathlib.Path, bottle_id: int) -> list[dict]:
    """Remove a bottle from the consumed-pending sidecar. Returns updated list."""
    items = read_consumed_pending(data_dir)
    new_items = [it for it in items if int(it["bottle_id"]) != int(bottle_id)]
    if len(new_items) == len(items):
        return items
    _atomic_write(consumed_pending_path(data_dir), {"items": new_items, "updated_at": _now_iso()})
    return new_items


def prune_consumed_after_etl(data_dir: str | pathlib.Path, agent_con) -> list[int]:
    """Remove pending entries whose bottle is no longer ``status='stored'``.

    Uses the agent DuckDB connection to look up current bottle status. Any
    bottle that is missing from ``bottles`` or whose status is not ``stored``
    is treated as resolved and removed from the sidecar.

    Returns the list of pruned bottle IDs.
    """
    items = read_consumed_pending(data_dir)
    if not items:
        return []

    bottle_ids = [int(it["bottle_id"]) for it in items]
    placeholders = ",".join("?" for _ in bottle_ids)
    query = f"SELECT bottle_id, status, is_in_transit FROM bottles_full WHERE bottle_id IN ({placeholders})"
    try:
        rows = agent_con.execute(query, bottle_ids).fetchall()
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("Could not query bottle status for prune: %s", exc)
        return []

    still_stored: set[int] = {int(r[0]) for r in rows if r[1] == "stored" and not r[2]}
    keep = [it for it in items if int(it["bottle_id"]) in still_stored]
    pruned_ids = [int(it["bottle_id"]) for it in items if int(it["bottle_id"]) not in still_stored]

    if pruned_ids:
        _atomic_write(consumed_pending_path(data_dir), {"items": keep, "updated_at": _now_iso()})
    return pruned_ids


# ---------------------------------------------------------------------------
# Drink-tonight
# ---------------------------------------------------------------------------


def drink_tonight_path(data_dir: str | pathlib.Path) -> pathlib.Path:
    return _resolve(data_dir, DRINK_TONIGHT_FILENAME)


def read_drink_tonight(data_dir: str | pathlib.Path) -> list[dict]:
    """Return the list of drink-tonight entries.

    Each entry has ``wine_id`` (int), ``added_at`` (ISO timestamp), and
    optional ``note``.
    """
    data = _safe_read(drink_tonight_path(data_dir))
    if data is None:
        return []
    items = data.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict) and "wine_id" in item]


def write_drink_tonight(data_dir: str | pathlib.Path, items: list[dict]) -> list[dict]:
    """Replace the full drink-tonight list. Returns the normalised list.

    Items are normalised to ``{wine_id: int, added_at: str, note: str|None}``
    with duplicates removed (first occurrence wins).
    """
    seen: set[int] = set()
    normalised: list[dict] = []
    for raw in items:
        if not isinstance(raw, dict) or "wine_id" not in raw:
            continue
        try:
            wine_id = int(raw["wine_id"])
        except (TypeError, ValueError):
            continue
        if wine_id in seen:
            continue
        seen.add(wine_id)
        normalised.append(
            {
                "wine_id": wine_id,
                "added_at": str(raw.get("added_at") or _now_iso()),
                "note": raw.get("note"),
            }
        )
    _atomic_write(drink_tonight_path(data_dir), {"items": normalised, "updated_at": _now_iso()})
    return normalised


def add_drink_tonight(
    data_dir: str | pathlib.Path,
    *,
    wine_id: int,
    note: str | None = None,
) -> list[dict]:
    """Append a wine to the drink-tonight list (idempotent)."""
    items = read_drink_tonight(data_dir)
    if any(int(it["wine_id"]) == int(wine_id) for it in items):
        return items
    items.append({"wine_id": int(wine_id), "added_at": _now_iso(), "note": note})
    return write_drink_tonight(data_dir, items)


def remove_drink_tonight(data_dir: str | pathlib.Path, wine_id: int) -> list[dict]:
    """Remove a wine from the drink-tonight list. Returns updated list."""
    items = read_drink_tonight(data_dir)
    new_items = [it for it in items if int(it["wine_id"]) != int(wine_id)]
    if len(new_items) == len(items):
        return items
    return write_drink_tonight(data_dir, new_items)
