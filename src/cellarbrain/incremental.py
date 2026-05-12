"""Incremental load: change detection, ID stabilization, and ETL run tracking.

On each sync run the full transform is executed from CSV, then this module:
1. Stabilises entity IDs so they stay constant across loads.
2. Diffs old vs. new data to detect inserts / updates / deletes.
3. Annotates every row with ``etl_run_id`` and ``updated_at`` metadata.
4. Produces ``change_log`` rows so a downstream agent can query what changed.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import logging
import pathlib
import struct
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pyarrow.parquet as pq

from .settings import IdentityConfig
from .transform import wine_fingerprint, wine_slug

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Entity metadata
# ---------------------------------------------------------------------------

ENTITY_ORDER: list[str] = [
    "winery",
    "appellation",
    "grape",
    "cellar",
    "provider",
    "tracked_wine",
    "wine",
    "wine_grape",
    "bottle",
    "tasting",
    "pro_rating",
]

PK_FIELD: dict[str, str | None] = {
    "winery": "winery_id",
    "appellation": "appellation_id",
    "grape": "grape_id",
    "cellar": "cellar_id",
    "provider": "provider_id",
    "tracked_wine": "tracked_wine_id",
    "wine": "wine_id",
    "wine_grape": None,  # composite PK
    "bottle": "bottle_id",
    "tasting": "tasting_id",
    "pro_rating": "rating_id",
}

NATURAL_KEY_FIELDS: dict[str, list[str]] = {
    "winery": ["name"],
    "appellation": ["country", "region", "subregion", "classification"],
    "grape": ["name"],
    "cellar": ["name"],
    "provider": ["name"],
    "tracked_wine": ["winery_id", "wine_name"],
    "wine": ["winery_id", "name", "vintage", "is_non_vintage"],
    "wine_grape": ["wine_id", "grape_id"],
    "bottle": [
        "wine_id",
        "cellar_id",
        "shelf",
        "bottle_number",
        "purchase_date",
        "provider_id",
        "status",
        "output_date",
    ],
    "tasting": ["wine_id", "tasting_date"],
    "pro_rating": ["wine_id", "source", "score"],
}

FK_REFS: dict[str, dict[str, str]] = {
    "tracked_wine": {"winery_id": "winery", "appellation_id": "appellation"},
    "wine": {
        "winery_id": "winery",
        "appellation_id": "appellation",
        "tracked_wine_id": "tracked_wine",
    },
    "wine_grape": {"wine_id": "wine", "grape_id": "grape"},
    "bottle": {"wine_id": "wine", "cellar_id": "cellar", "provider_id": "provider"},
    "tasting": {"wine_id": "wine"},
    "pro_rating": {"wine_id": "wine"},
}

_META_FIELDS = frozenset(
    {
        "etl_run_id",
        "updated_at",
        "dossier_path",
        "is_deleted",
        "food_tags",
        "food_groups",
        "format_group_id",
        "wine_slug",
        "is_in_transit",
    }
)

# Entity types that are soft-deleted (row retained with is_deleted=True) instead
# of being permanently removed from Parquet on sync.
_SOFT_DELETE_ENTITIES = frozenset({"wine", "tracked_wine"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _values_equal(a: object, b: object) -> bool:
    """Compare two field values, handling float32 precision from Parquet."""
    if isinstance(a, float) and isinstance(b, float):
        # Round both to float32 to match Parquet storage precision
        a32 = struct.unpack("f", struct.pack("f", a))[0]
        b32 = struct.unpack("f", struct.pack("f", b))[0]
        return a32 == b32
    return a == b


def compute_file_hash(path: str | pathlib.Path) -> str:
    """Return hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _natural_key(row: dict, fields: list[str]) -> tuple:
    return tuple(row.get(f) for f in fields)


def _table_to_dicts(path: pathlib.Path) -> list[dict]:
    """Read a Parquet file into a list of dicts (empty list if missing)."""
    if not path.exists():
        return []
    table = pq.read_table(path)
    cols = table.to_pydict()
    return [{c: cols[c][i] for c in cols} for i in range(table.num_rows)]


def load_existing(output_dir: pathlib.Path) -> dict[str, list[dict]]:
    """Load all existing entity Parquet files."""
    return {et: _table_to_dicts(output_dir / f"{et}.parquet") for et in ENTITY_ORDER}


def load_etl_runs(output_dir: pathlib.Path) -> list[dict]:
    return _table_to_dicts(output_dir / "etl_run.parquet")


def load_change_log(output_dir: pathlib.Path) -> list[dict]:
    return _table_to_dicts(output_dir / "change_log.parquet")


def next_run_id(output_dir: pathlib.Path) -> int:
    runs = load_etl_runs(output_dir)
    return max((r["run_id"] for r in runs), default=0) + 1


def _next_change_id(output_dir: pathlib.Path) -> int:
    changes = load_change_log(output_dir)
    return max((c["change_id"] for c in changes), default=0) + 1


# ---------------------------------------------------------------------------
# ID stabilisation
# ---------------------------------------------------------------------------


def _stabilize_entity(
    new_rows: list[dict],
    old_rows: list[dict],
    entity_type: str,
    id_remappings: dict[str, dict[int, int]],
    *,
    identity_config: IdentityConfig | None = None,
) -> tuple[list[dict], dict[int, int]]:
    """Stabilise IDs: reuse existing IDs for matching natural keys.

    Handles duplicate natural keys by positional matching within each group.
    Returns (rows_with_stable_ids, original_id→stable_id mapping).
    """
    pk = PK_FIELD[entity_type]
    nk_fields = NATURAL_KEY_FIELDS[entity_type]
    fk_refs = FK_REFS.get(entity_type, {})

    # Step 1: remap FK columns using already-stabilised parent entities
    for row in new_rows:
        for fk_field, ref_type in fk_refs.items():
            remap = id_remappings.get(ref_type)
            if remap and row.get(fk_field) is not None:
                row[fk_field] = remap.get(row[fk_field], row[fk_field])

    if pk is None:
        # Composite-PK entity (e.g. wine_grape) — nothing to stabilise
        return new_rows, {}

    # Step 2: group existing IDs by natural key (preserving order)
    old_nk_to_ids: dict[tuple, list[int]] = defaultdict(list)
    for row in old_rows:
        old_nk_to_ids[_natural_key(row, nk_fields)].append(row[pk])

    max_old_id = max((r[pk] for r in old_rows), default=0)
    next_id = max_old_id + 1

    # Step 3: assign stable IDs (positional within each natural-key group)
    nk_counter: dict[tuple, int] = defaultdict(int)
    remap: dict[int, int] = {}
    newly_assigned: list[tuple[dict, int]] = []  # (row, original_id) for fallback
    for row in new_rows:
        original_id = row[pk]
        nk = _natural_key(row, nk_fields)
        idx = nk_counter[nk]
        nk_counter[nk] += 1

        old_ids = old_nk_to_ids.get(nk, [])
        if idx < len(old_ids):
            stable = old_ids[idx]
        else:
            stable = next_id
            next_id += 1
            newly_assigned.append((row, original_id))

        remap[original_id] = stable
        row[pk] = stable

    # Step 4 (wine only): partial-NK fallback for renames.
    # When a wine's name changes, the full NK won't match.  Try matching
    # on (winery_id, vintage, is_non_vintage) — only when 1-to-1.
    if entity_type == "wine" and newly_assigned:
        _WINE_PARTIAL_KEY = ("winery_id", "vintage", "is_non_vintage")

        # Determine which old IDs were consumed by exact NK matching
        consumed_old: set[int] = set()
        for nk_key, old_ids in old_nk_to_ids.items():
            consumed_old.update(old_ids[: nk_counter.get(nk_key, 0)])

        # Index unconsumed old rows by partial key
        old_by_pk_map = {r[pk]: r for r in old_rows}
        old_partial: dict[tuple, list[int]] = defaultdict(list)
        for old_id in (r[pk] for r in old_rows):
            if old_id not in consumed_old:
                pkey = tuple(old_by_pk_map[old_id].get(f) for f in _WINE_PARTIAL_KEY)
                old_partial[pkey].append(old_id)

        # Index newly assigned new rows by partial key
        new_partial: dict[tuple, list[tuple[dict, int]]] = defaultdict(list)
        for row, original_id in newly_assigned:
            pkey = tuple(row.get(f) for f in _WINE_PARTIAL_KEY)
            new_partial[pkey].append((row, original_id))

        # Match 1-to-1 exactly, or use fuzzy name matching for N-to-N
        matched_old: set[int] = set()
        ic = identity_config or IdentityConfig()
        for pkey, new_items in new_partial.items():
            old_candidates = old_partial.get(pkey, [])
            if not old_candidates:
                continue
            if len(new_items) == 1 and len(old_candidates) == 1:
                # Structurally unambiguous: exactly one disappeared, one
                # appeared with the same (winery_id, vintage, is_non_vintage).
                row, original_id = new_items[0]
                old_stable = old_candidates[0]
                remap[original_id] = old_stable
                row[pk] = old_stable
                matched_old.add(old_stable)
            elif ic.enable_fuzzy_match:
                # Fuzzy match: best-score assignment for each new wine
                for row, original_id in new_items:
                    new_name = str(row.get("name") or "")
                    best_score = 0.0
                    best_old: int | None = None
                    for old_id in old_candidates:
                        if old_id in matched_old:
                            continue
                        old_name = str(old_by_pk_map[old_id].get("name") or "")
                        score = difflib.SequenceMatcher(
                            None,
                            new_name,
                            old_name,
                        ).ratio()
                        if score >= ic.rename_threshold and score > best_score:
                            best_score = score
                            best_old = old_id
                    if best_old is not None:
                        remap[original_id] = best_old
                        row[pk] = best_old
                        matched_old.add(best_old)

    return new_rows, remap


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------


def _diff_rows(
    new_rows: list[dict],
    old_rows: list[dict],
    entity_type: str,
    run_id: int,
    now: datetime,
) -> tuple[list[dict], list[dict]]:
    """Detect inserts / updates / deletes between *new_rows* and *old_rows*.

    Handles duplicate natural keys by positional matching within each group.
    Returns (annotated_rows, change_log_entries).
    """
    pk = PK_FIELD[entity_type]
    nk_fields = NATURAL_KEY_FIELDS[entity_type]

    # Fields to compare (everything except meta)
    compare_fields = [f for f in (new_rows[0] if new_rows else {}) if f not in _META_FIELDS]

    # Group old rows by natural key (preserve order for positional matching)
    old_groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in old_rows:
        old_groups[_natural_key(row, nk_fields)].append(row)

    nk_counter: dict[tuple, int] = defaultdict(int)
    changes: list[dict] = []

    for row in new_rows:
        nk = _natural_key(row, nk_fields)
        idx = nk_counter[nk]
        nk_counter[nk] += 1

        group = old_groups.get(nk, [])
        old_row = group[idx] if idx < len(group) else None

        if old_row is None:
            # INSERT
            row["etl_run_id"] = run_id
            row["updated_at"] = now
            changes.append(
                {
                    "run_id": run_id,
                    "entity_type": entity_type,
                    "entity_id": row.get(pk) if pk else None,
                    "change_type": "insert",
                    "changed_fields": None,
                }
            )
        else:
            changed = [f for f in compare_fields if f != pk and not _values_equal(row.get(f), old_row.get(f))]
            if changed:
                # UPDATE
                row["etl_run_id"] = run_id
                row["updated_at"] = now
                changes.append(
                    {
                        "run_id": run_id,
                        "entity_type": entity_type,
                        "entity_id": row.get(pk) if pk else None,
                        "change_type": "update",
                        "changed_fields": json.dumps(changed),
                    }
                )
            else:
                # UNCHANGED — preserve existing metadata
                row["etl_run_id"] = old_row.get("etl_run_id", run_id)
                row["updated_at"] = old_row.get("updated_at", now)
                # Revival: tombstone row matched by NK means it was soft-deleted
                # and is now reappearing in the export — treat as an update.
                if entity_type in _SOFT_DELETE_ENTITIES and old_row.get("is_deleted"):
                    row["is_deleted"] = False
                    row["etl_run_id"] = run_id
                    row["updated_at"] = now
                    changes.append(
                        {
                            "run_id": run_id,
                            "entity_type": entity_type,
                            "entity_id": row.get(pk) if pk else None,
                            "change_type": "update",
                            "changed_fields": json.dumps(["is_deleted"]),
                        }
                    )

    # DELETES — old rows beyond what the new side consumed.
    # For soft-delete entities, collect candidates; tombstones are appended
    # after the RENAMES section so that renamed rows are excluded.
    _tombstone_candidates: list[tuple[dict, object]] = []
    for nk, group in old_groups.items():
        consumed = nk_counter.get(nk, 0)
        for old_row in group[consumed:]:
            changes.append(
                {
                    "run_id": run_id,
                    "entity_type": entity_type,
                    "entity_id": old_row.get(pk) if pk else None,
                    "change_type": "delete",
                    "changed_fields": None,
                }
            )
            if entity_type in _SOFT_DELETE_ENTITIES:
                _tombstone_candidates.append((old_row, old_row.get(pk)))

    # RENAMES — same entity_id appearing as both INSERT and DELETE means
    # the natural key changed but the ID was preserved by partial-NK
    # matching in _stabilize_entity.  Merge into a single "rename" entry.
    rename_ids: set = set()
    if pk:
        insert_ids = {c["entity_id"] for c in changes if c["change_type"] == "insert"}
        delete_ids = {c["entity_id"] for c in changes if c["change_type"] == "delete"}
        rename_ids = insert_ids & delete_ids

        if rename_ids:
            old_by_pk = {r[pk]: r for r in old_rows}
            new_by_pk = {r[pk]: r for r in new_rows}
            merged: list[dict] = []
            for c in changes:
                eid = c["entity_id"]
                if eid in rename_ids:
                    if c["change_type"] == "insert":
                        old_row = old_by_pk[eid]
                        new_row = new_by_pk[eid]
                        changed = [
                            f
                            for f in compare_fields
                            if f != pk
                            and not _values_equal(
                                new_row.get(f),
                                old_row.get(f),
                            )
                        ]
                        c["change_type"] = "rename"
                        c["changed_fields"] = json.dumps(changed) if changed else None
                        merged.append(c)
                    # drop the matching DELETE
                else:
                    merged.append(c)
            changes = merged

    # SOFT-DELETE tombstones — keep deleted rows (excluding renames) with
    # is_deleted=True so they survive future writes and can be undeleted.
    for old_row, eid in _tombstone_candidates:
        if eid not in rename_ids:
            tombstone = dict(old_row)
            tombstone["is_deleted"] = True
            tombstone["etl_run_id"] = run_id
            tombstone["updated_at"] = now
            new_rows.append(tombstone)

    return new_rows, changes


# ---------------------------------------------------------------------------
# Winery rename detection (structural heuristic)
# ---------------------------------------------------------------------------


def _detect_winery_renames(
    new_wineries: list[dict],
    old_wineries: list[dict],
    new_wines: list[dict],
    old_wines: list[dict],
) -> dict[int, int]:
    """Detect winery renames via wine-count structural heuristic.

    Compares disappeared old wineries with appeared new wineries.  When
    exactly **one** disappeared and **one** appeared, and both have the
    same number of wines (> 0), treat the pair as a rename.

    Returns ``{new_winery_id: old_winery_id}`` (empty dict if no rename).
    """
    nk_fields = NATURAL_KEY_FIELDS["winery"]
    pk = PK_FIELD["winery"]

    old_nks = {_natural_key(r, nk_fields) for r in old_wineries}
    new_nks = {_natural_key(r, nk_fields) for r in new_wineries}

    disappeared = [r for r in old_wineries if _natural_key(r, nk_fields) not in new_nks]
    appeared = [r for r in new_wineries if _natural_key(r, nk_fields) not in old_nks]

    if len(disappeared) != 1 or len(appeared) != 1:
        return {}

    old_wid = disappeared[0][pk]
    new_wid = appeared[0][pk]

    old_count = sum(1 for w in old_wines if w.get("winery_id") == old_wid)
    new_count = sum(1 for w in new_wines if w.get("winery_id") == new_wid)

    if old_count > 0 and old_count == new_count:
        return {new_wid: old_wid}

    return {}


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def sync(
    new_entities: dict[str, list[dict]],
    output_dir: pathlib.Path,
    run_id: int,
    now: datetime,
    *,
    identity_config: IdentityConfig | None = None,
    skip_entities: frozenset[str] = frozenset(),
) -> tuple[dict[str, list[dict]], list[dict], dict[str, dict[int, int]]]:
    """Stabilise IDs, detect changes, annotate with ETL metadata.

    *new_entities* must come from a full fresh transform.
    Returns (stabilised_entities, change_log_rows, id_remappings).
    """
    existing = load_existing(output_dir)
    id_remappings: dict[str, dict[int, int]] = {}
    start_cid = _next_change_id(output_dir)

    stabilised: dict[str, list[dict]] = {}
    all_changes: list[dict] = []

    for entity_type in ENTITY_ORDER:
        if entity_type in skip_entities:
            stabilised[entity_type] = new_entities.get(entity_type, [])
            continue
        new_rows = [dict(r) for r in new_entities.get(entity_type, [])]
        old_rows = existing.get(entity_type, [])

        remapped, remap = _stabilize_entity(
            new_rows,
            old_rows,
            entity_type,
            id_remappings,
            identity_config=identity_config,
        )

        # Winery rename: patch stabilisation when structural heuristic fires
        if entity_type == "winery":
            winery_renames = _detect_winery_renames(
                new_entities.get("winery", []),
                old_rows,
                new_entities.get("wine", []),
                existing.get("wine", []),
            )
            pk_field = PK_FIELD["winery"]
            for new_tid, old_sid in winery_renames.items():
                wrong_sid = remap.get(new_tid)
                if wrong_sid is not None and wrong_sid != old_sid:
                    remap[new_tid] = old_sid
                    for row in remapped:
                        if row[pk_field] == wrong_sid:
                            row[pk_field] = old_sid
                            break

        if remap:
            id_remappings[entity_type] = remap

        annotated, changes = _diff_rows(
            remapped,
            old_rows,
            entity_type,
            run_id,
            now,
        )

        # Ensure every active row for soft-delete entities carries is_deleted=False.
        # Tombstone rows appended by _diff_rows already have is_deleted=True.
        if entity_type in _SOFT_DELETE_ENTITIES:
            for row in annotated:
                row.setdefault("is_deleted", False)

        stabilised[entity_type] = annotated
        all_changes.extend(changes)

    for i, ch in enumerate(all_changes, start=start_cid):
        ch["change_id"] = i

    # Apply FK remaps to skipped entities whose parent IDs were stabilised
    for entity_type in skip_entities:
        fk_refs = FK_REFS.get(entity_type, {})
        rows = stabilised.get(entity_type, [])
        for row in rows:
            for fk_field, ref_type in fk_refs.items():
                remap = id_remappings.get(ref_type)
                if remap and row.get(fk_field) is not None:
                    row[fk_field] = remap.get(row[fk_field], row[fk_field])

    return stabilised, all_changes, id_remappings


def annotate_full_load(
    entities: dict[str, list[dict]],
    output_dir: pathlib.Path,
    run_id: int,
    now: datetime,
    *,
    skip_entities: frozenset[str] = frozenset(),
) -> tuple[dict[str, list[dict]], list[dict]]:
    """Annotate every entity for a full (initial) load.

    Sets ``etl_run_id`` / ``updated_at`` on every row and generates
    insert change-log entries.
    """
    start_cid = _next_change_id(output_dir)
    annotated: dict[str, list[dict]] = {}
    all_changes: list[dict] = []

    for entity_type in ENTITY_ORDER:
        if entity_type in skip_entities:
            annotated[entity_type] = entities.get(entity_type, [])
            continue
        pk = PK_FIELD[entity_type]
        rows = entities.get(entity_type, [])
        for row in rows:
            row["etl_run_id"] = run_id
            row["updated_at"] = now
            # Mark all new rows as active for soft-delete entities.
            if entity_type in _SOFT_DELETE_ENTITIES:
                row.setdefault("is_deleted", False)
            all_changes.append(
                {
                    "run_id": run_id,
                    "entity_type": entity_type,
                    "entity_id": row.get(pk) if pk else None,
                    "change_type": "insert",
                    "changed_fields": None,
                }
            )
        annotated[entity_type] = rows

    # Carry forward existing tombstones not present in the new data.
    # This preserves soft-deleted rows across full loads.
    for entity_type in _SOFT_DELETE_ENTITIES:
        if entity_type in skip_entities:
            continue
        pk = PK_FIELD[entity_type]
        existing_rows = _table_to_dicts(output_dir / f"{entity_type}.parquet")
        new_ids: set = {r[pk] for r in annotated.get(entity_type, []) if pk}
        for old_row in existing_rows:
            if old_row.get("is_deleted") and (pk is None or old_row.get(pk) not in new_ids):
                annotated[entity_type].append(old_row)

    for i, ch in enumerate(all_changes, start=start_cid):
        ch["change_id"] = i

    return annotated, all_changes


# ---------------------------------------------------------------------------
# Slug-based wine classification (28a)
# ---------------------------------------------------------------------------


@dataclass
class WineMatch:
    """Result of matching one CSV row against existing wines."""

    csv_row_index: int
    slug: str
    wine_id: int
    status: Literal["existing", "new", "revived", "renamed"]
    old_slug: str | None = None


@dataclass
class WineDeletion:
    """An existing wine not matched by any CSV row."""

    wine_id: int
    slug: str
    was_already_deleted: bool


def _match_by_fingerprint(
    candidates: list[tuple[int, bool]],
    csv_fp: tuple[str, str, str, str, str],
    fp_index: dict[int, tuple[str, str, str, str, str]],
) -> int | None:
    """Find the best fingerprint match among candidates.

    Tries exact fingerprint match first, then cascading partial matches
    (volume → classification → grapes → category → price), then
    positional as last resort.
    """
    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0][0]

    # Exact fingerprint match
    for wid, _ in candidates:
        if fp_index.get(wid) == csv_fp:
            return wid

    # Cascading partial match
    for field_idx in range(5):
        partial = [
            (wid, d)
            for wid, d in candidates
            if fp_index.get(wid, ("",) * 5)[field_idx] == csv_fp[field_idx] and csv_fp[field_idx]
        ]
        if len(partial) == 1:
            return partial[0][0]

    # Last resort: positional (first unconsumed)
    return candidates[0][0]


def _detect_renames(
    new_matches: list[WineMatch],
    deletions: list[WineDeletion],
    wines_rows: list[dict],
    existing_wines_by_id: dict[int, dict],
) -> list[tuple[WineMatch, WineDeletion]]:
    """Find rename pairs among unmatched NEW and DELETED wines.

    A (NEW, DELETED) pair is a rename when they share the same year and
    exactly one of (winery, name) matches while the other changed.
    """
    _NV_SYNONYMS = frozenset({"", "non vintage", "nv"})

    def _normalise_year(raw: str) -> str:
        return "" if raw in _NV_SYNONYMS else raw

    new_info: list[tuple[WineMatch, str, str, str]] = []
    for m in new_matches:
        if m.status != "new":
            continue
        row = wines_rows[m.csv_row_index]
        winery = (row.get("winery") or "").strip().lower()
        name = (row.get("wine_name") or "").strip().lower()
        year = _normalise_year((row.get("vintage_raw") or "").strip().lower())
        new_info.append((m, winery, name, year))

    del_info: list[tuple[WineDeletion, str, str, str]] = []
    for d in deletions:
        if d.was_already_deleted:
            continue
        old = existing_wines_by_id.get(d.wine_id, {})
        winery = (old.get("winery_name") or "").strip().lower()
        name = (old.get("name") or "").strip().lower()
        year = _normalise_year(
            str(old.get("vintage") or old.get("year") or "").strip().lower(),
        )
        del_info.append((d, winery, name, year))

    consumed_new: set[int] = set()
    consumed_del: set[int] = set()
    pairs: list[tuple[WineMatch, WineDeletion]] = []

    for nm, n_winery, n_name, n_year in new_info:
        best: WineDeletion | None = None
        for dd, d_winery, d_name, d_year in del_info:
            if dd.wine_id in consumed_del:
                continue
            if n_year != d_year:
                continue
            same_winery = n_winery == d_winery
            same_name = n_name == d_name
            if same_name and not same_winery:
                best = dd
                break
            if same_winery and not same_name:
                best = dd
                break

        if best is not None and nm.csv_row_index not in consumed_new:
            consumed_new.add(nm.csv_row_index)
            consumed_del.add(best.wine_id)
            pairs.append((nm, best))

    return pairs


def _compute_slug_from_wine(w: dict) -> str:
    """Compute slug from an existing wine dict (Parquet row)."""
    winery = w.get("winery_name") or ""
    name = w.get("name") or ""
    vintage = w.get("vintage")
    is_nv = w.get("is_non_vintage", False)
    if vintage is not None:
        year = str(vintage)
    elif is_nv:
        year = ""
    else:
        year = ""
    return wine_slug(winery, name, year)


_FORMAT_SUFFIXES = ("-half", "-magnum", "-jeroboam", "-double-magnum", "-imperiale", "-nebuchadnezzar")


def _strip_format_suffix(slug: str) -> str:
    """Remove format suffix from a wine slug to get the base slug."""
    for suffix in _FORMAT_SUFFIXES:
        if slug.endswith(suffix):
            return slug[: -len(suffix)]
    # Handle truncated suffixes (slug hit 60-char limit mid-suffix)
    return slug.rstrip("-")


def classify_wines(
    wines_rows: list[dict],
    existing_wines: list[dict],
) -> tuple[list[WineMatch], list[WineDeletion]]:
    """Match incoming CSV rows against existing wines by slug.

    Returns (matches, deletions).
    """
    # 1. Build slug index from existing wine.parquet
    #    Strip format suffixes so format-variant wines share the same
    #    lookup key as the base slug computed from CSV rows.
    slug_index: dict[str, list[tuple[int, bool]]] = defaultdict(list)
    for w in existing_wines:
        s = w.get("wine_slug") or _compute_slug_from_wine(w)
        s = _strip_format_suffix(s)
        slug_index[s].append((w["wine_id"], w.get("is_deleted", False)))
    for s in slug_index:
        slug_index[s].sort(key=lambda t: t[0])

    # 2. Highest existing wine_id
    max_existing_id = max(
        (w["wine_id"] for w in existing_wines),
        default=0,
    )
    next_id = max_existing_id + 1

    # 3. Build fingerprint index from existing wines
    fp_index: dict[int, tuple[str, str, str, str, str]] = {}
    for w in existing_wines:
        fp_index[w["wine_id"]] = (
            w.get("_raw_volume") or "",
            w.get("_raw_classification") or "",
            w.get("_raw_grapes") or "",
            w.get("category") or "",
            str(w.get("list_price") or ""),
        )

    # 4. Classify each CSV row
    consumed_ids: set[int] = set()
    matches: list[WineMatch] = []

    for i, row in enumerate(wines_rows):
        slug = wine_slug(
            row.get("winery"),
            row.get("wine_name"),
            row.get("vintage_raw"),
        )
        csv_fp = wine_fingerprint(row)
        # Normalise category and price in the CSV fingerprint to match
        # the stored format (category slug, decimal string).
        from . import parsers, vinocell_parsers

        cat_slug = ""
        try:
            cat_slug = vinocell_parsers.parse_category(csv_fp[3]) if csv_fp[3] else ""
        except ValueError:
            cat_slug = csv_fp[3]
        price_str = ""
        if csv_fp[4]:
            try:
                price_str = str(parsers.parse_decimal(csv_fp[4]))
            except ValueError:
                price_str = csv_fp[4]
        csv_fp_norm = (csv_fp[0], csv_fp[1], csv_fp[2], cat_slug, price_str)

        existing_group = slug_index.get(slug, [])
        active = [(wid, d) for wid, d in existing_group if not d and wid not in consumed_ids]
        deleted = [(wid, d) for wid, d in existing_group if d and wid not in consumed_ids]

        matched_wid = _match_by_fingerprint(active, csv_fp_norm, fp_index)
        if matched_wid is not None:
            consumed_ids.add(matched_wid)
            matches.append(WineMatch(i, slug, matched_wid, "existing"))

        elif deleted:
            revived_wid = _match_by_fingerprint(deleted, csv_fp_norm, fp_index)
            wid = revived_wid if revived_wid is not None else deleted[0][0]
            consumed_ids.add(wid)
            matches.append(WineMatch(i, slug, wid, "revived"))

        else:
            wid = next_id
            next_id += 1
            matches.append(WineMatch(i, slug, wid, "new"))

    # 5. Detect deletions
    all_consumed = {m.wine_id for m in matches}
    deletions: list[WineDeletion] = []
    for w in existing_wines:
        if w["wine_id"] not in all_consumed:
            deletions.append(
                WineDeletion(
                    w["wine_id"],
                    w.get("wine_slug") or "",
                    w.get("is_deleted", False),
                )
            )

    # 6. Rename detection post-pass
    existing_by_id = {w["wine_id"]: w for w in existing_wines}
    rename_pairs = _detect_renames(matches, deletions, wines_rows, existing_by_id)
    for nm, dd in rename_pairs:
        logger.debug(
            "Rename detected: %r -> %r (wine_id=%d)",
            dd.slug,
            nm.slug,
            dd.wine_id,
        )
        nm.wine_id = dd.wine_id
        nm.status = "renamed"
        nm.old_slug = dd.slug
        deletions.remove(dd)
        next_id -= 1

    counts = {}
    for m in matches:
        counts[m.status] = counts.get(m.status, 0) + 1
    logger.info(
        "classify_wines: %d matched, %d new, %d revived, %d renamed, %d deleted",
        counts.get("existing", 0),
        counts.get("new", 0),
        counts.get("revived", 0),
        counts.get("renamed", 0),
        len(deletions),
    )

    return matches, deletions


def annotate_classified_wines(
    wines: list[dict],
    existing_wines: list[dict],
    matches: list[WineMatch],
    deletions: list[WineDeletion],
    run_id: int,
    now: datetime,
    *,
    fk_remappings: dict[str, dict[int, int]] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Annotate wine entities after slug-based classification.

    Handles change detection (EXISTING vs old), metadata stamping,
    soft-deletion, and tombstone carry-forward.

    When *fk_remappings* is provided, tombstone FK columns are remapped
    so they match the current run's stabilised parent IDs.

    Returns (annotated_wines, change_log_entries).
    """
    old_by_id = {w["wine_id"]: w for w in existing_wines}

    compare_fields = [f for f in (wines[0] if wines else {}) if f not in _META_FIELDS]

    changes: list[dict] = []

    for wine, match in zip(wines, matches):
        if match.status == "new":
            wine["is_deleted"] = False
            wine["etl_run_id"] = run_id
            wine["updated_at"] = now
            changes.append(
                {
                    "run_id": run_id,
                    "entity_type": "wine",
                    "entity_id": match.wine_id,
                    "change_type": "insert",
                    "changed_fields": None,
                }
            )

        elif match.status == "revived":
            wine["is_deleted"] = False
            wine["etl_run_id"] = run_id
            wine["updated_at"] = now
            changes.append(
                {
                    "run_id": run_id,
                    "entity_type": "wine",
                    "entity_id": match.wine_id,
                    "change_type": "update",
                    "changed_fields": json.dumps(["is_deleted"]),
                }
            )

        elif match.status == "renamed":
            old = old_by_id.get(match.wine_id, {})
            changed = [
                f
                for f in compare_fields
                if f != "wine_id"
                and not _values_equal(
                    wine.get(f),
                    old.get(f),
                )
            ]
            wine["is_deleted"] = False
            wine["etl_run_id"] = run_id
            wine["updated_at"] = now
            changes.append(
                {
                    "run_id": run_id,
                    "entity_type": "wine",
                    "entity_id": match.wine_id,
                    "change_type": "rename",
                    "changed_fields": json.dumps(changed) if changed else None,
                }
            )

        else:  # existing
            old = old_by_id.get(match.wine_id, {})
            changed = [
                f
                for f in compare_fields
                if f != "wine_id"
                and not _values_equal(
                    wine.get(f),
                    old.get(f),
                )
            ]
            if changed:
                wine["is_deleted"] = False
                wine["etl_run_id"] = run_id
                wine["updated_at"] = now
                changes.append(
                    {
                        "run_id": run_id,
                        "entity_type": "wine",
                        "entity_id": match.wine_id,
                        "change_type": "update",
                        "changed_fields": json.dumps(changed),
                    }
                )
            else:
                wine["is_deleted"] = old.get("is_deleted", False)
                wine["etl_run_id"] = old.get("etl_run_id", run_id)
                wine["updated_at"] = old.get("updated_at", now)

    # Soft-delete tombstones
    wine_fk_refs = FK_REFS.get("wine", {})
    for d in deletions:
        old = old_by_id.get(d.wine_id, {})
        tombstone = dict(old)
        tombstone["is_deleted"] = True
        tombstone["etl_run_id"] = run_id
        tombstone["updated_at"] = now
        # Remap FK columns so tombstone references match the current run's IDs
        if fk_remappings:
            for fk_field, ref_type in wine_fk_refs.items():
                remap = fk_remappings.get(ref_type)
                if remap and tombstone.get(fk_field) is not None:
                    tombstone[fk_field] = remap.get(
                        tombstone[fk_field],
                        tombstone[fk_field],
                    )
        wines.append(tombstone)
        if not d.was_already_deleted:
            changes.append(
                {
                    "run_id": run_id,
                    "entity_type": "wine",
                    "entity_id": d.wine_id,
                    "change_type": "delete",
                    "changed_fields": None,
                }
            )

    return wines, changes
