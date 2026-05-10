"""Unit tests for cellarbrain.incremental — change detection & ID stabilisation."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cellarbrain.incremental import (
    ENTITY_ORDER,
    WineDeletion,
    WineMatch,
    _detect_renames,
    _detect_winery_renames,
    _diff_rows,
    _match_by_fingerprint,
    _stabilize_entity,
    annotate_classified_wines,
    annotate_full_load,
    classify_wines,
    compute_file_hash,
    load_existing,
    next_run_id,
    sync,
)
from cellarbrain.settings import IdentityConfig
from cellarbrain.transform import assign_dossier_paths
from cellarbrain.writer import SCHEMAS, write_parquet

_NOW = datetime(2025, 6, 1, 12, 0, 0)
_LATER = datetime(2025, 7, 1, 12, 0, 0)
_EVEN_LATER = datetime(2025, 8, 1, 12, 0, 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_entity(tmp_path: Path, entity_type: str, rows: list[dict]) -> None:
    write_parquet(entity_type, rows, tmp_path)


def _write_empty(tmp_path: Path, entity_type: str) -> None:
    schema = SCHEMAS[entity_type]
    table = pa.table(
        {f.name: pa.array([], type=f.type) for f in schema},
        schema=schema,
    )
    pq.write_table(table, tmp_path / f"{entity_type}.parquet")


def _minimal_wine(
    wine_id: int,
    winery_id: int,
    name: str,
    vintage: int,
    *,
    appellation_id: int | None = None,
    alcohol_pct: float | None = None,
    comment: str | None = None,
    dossier_path: str | None = None,
    wine_slug: str | None = None,
    _raw_volume: str | None = None,
    _raw_classification: str | None = None,
    _raw_grapes: str | None = None,
    winery_name: str | None = None,
    run_id: int = 1,
    ts: datetime = _NOW,
) -> dict:
    from cellarbrain.transform import wine_slug as compute_slug

    slug = wine_slug or compute_slug(winery_name or "", name, str(vintage))
    d: dict = {
        "wine_id": wine_id,
        "wine_slug": slug,
        "winery_id": winery_id,
        "name": name,
        "vintage": vintage,
        "is_non_vintage": False,
        "appellation_id": appellation_id,
        "category": "red",
        "_raw_classification": _raw_classification,
        "subcategory": None,
        "specialty": None,
        "sweetness": None,
        "effervescence": None,
        "volume_ml": 750,
        "_raw_volume": _raw_volume,
        "container": None,
        "hue": None,
        "cork": None,
        "alcohol_pct": alcohol_pct,
        "acidity_g_l": None,
        "sugar_g_l": None,
        "ageing_type": None,
        "ageing_months": None,
        "farming_type": None,
        "serving_temp_c": None,
        "opening_type": None,
        "opening_minutes": None,
        "drink_from": None,
        "drink_until": None,
        "optimal_from": None,
        "optimal_until": None,
        "original_list_price": None,
        "original_list_currency": None,
        "list_price": None,
        "list_currency": None,
        "comment": comment,
        "winemaking_notes": None,
        "is_favorite": False,
        "is_wishlist": False,
        "tracked_wine_id": None,
        "full_name": f"{name} {vintage}",
        "grape_type": "unknown",
        "primary_grape": None,
        "grape_summary": None,
        "_raw_grapes": _raw_grapes,
        "drinking_status": "unknown",
        "age_years": None,
        "price_tier": "unknown",
        "bottle_format": "Standard",
        "price_per_750ml": None,
        "format_group_id": None,
        "food_tags": None,
        "is_deleted": False,
        "etl_run_id": run_id,
        "updated_at": ts,
    }
    if dossier_path is not None:
        d["dossier_path"] = dossier_path
    return d


def _minimal_bottle(
    bottle_id: int,
    wine_id: int,
    cellar_id: int,
    *,
    shelf: str = "A1",
    bottle_number: int = 1,
    provider_id: int | None = 1,
    status: str = "stored",
    output_date: date | None = None,
    output_type: str | None = None,
    output_comment: str | None = None,
    run_id: int = 1,
    ts: datetime = _NOW,
) -> dict:
    return {
        "bottle_id": bottle_id,
        "wine_id": wine_id,
        "status": status,
        "cellar_id": cellar_id,
        "shelf": shelf,
        "bottle_number": bottle_number,
        "provider_id": provider_id,
        "purchase_date": date(2024, 1, 1),
        "acquisition_type": "market_price",
        "original_purchase_price": Decimal("25.00"),
        "original_purchase_currency": "CHF",
        "purchase_price": Decimal("25.00"),
        "purchase_currency": "CHF",
        "purchase_comment": None,
        "output_date": output_date,
        "output_type": output_type,
        "output_comment": output_comment,
        "etl_run_id": run_id,
        "updated_at": ts,
    }


def _minimal_tasting(
    tasting_id: int,
    wine_id: int,
    *,
    tasting_date: date = date(2024, 6, 1),
    note: str | None = "Good",
    score: float | None = 16.0,
    max_score: int | None = 20,
    run_id: int = 1,
    ts: datetime = _NOW,
) -> dict:
    return {
        "tasting_id": tasting_id,
        "wine_id": wine_id,
        "tasting_date": tasting_date,
        "note": note,
        "score": score,
        "max_score": max_score,
        "etl_run_id": run_id,
        "updated_at": ts,
    }


def _minimal_pro_rating(
    rating_id: int,
    wine_id: int,
    *,
    source: str = "Parker",
    score: float = 92.0,
    max_score: int = 100,
    run_id: int = 1,
    ts: datetime = _NOW,
) -> dict:
    return {
        "rating_id": rating_id,
        "wine_id": wine_id,
        "source": source,
        "score": score,
        "max_score": max_score,
        "review_text": None,
        "etl_run_id": run_id,
        "updated_at": ts,
    }


def _seed_full(tmp_path: Path) -> None:
    """Write a rich initial dataset covering all entity types."""
    _write_entity(
        tmp_path,
        "winery",
        [
            {"winery_id": 1, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
            {"winery_id": 2, "name": "Beta", "etl_run_id": 1, "updated_at": _NOW},
        ],
    )
    _write_entity(
        tmp_path,
        "appellation",
        [
            {
                "appellation_id": 1,
                "country": "France",
                "region": "Bordeaux",
                "subregion": None,
                "classification": None,
                "etl_run_id": 1,
                "updated_at": _NOW,
            },
            {
                "appellation_id": 2,
                "country": "Italy",
                "region": "Piedmont",
                "subregion": None,
                "classification": None,
                "etl_run_id": 1,
                "updated_at": _NOW,
            },
        ],
    )
    _write_entity(
        tmp_path,
        "grape",
        [
            {"grape_id": 1, "name": "Merlot", "etl_run_id": 1, "updated_at": _NOW},
            {"grape_id": 2, "name": "Nebbiolo", "etl_run_id": 1, "updated_at": _NOW},
        ],
    )
    _write_entity(
        tmp_path,
        "cellar",
        [
            {
                "cellar_id": 1,
                "name": "01 Main",
                "location_type": "onsite",
                "sort_order": 1,
                "etl_run_id": 1,
                "updated_at": _NOW,
            },
        ],
    )
    _write_entity(
        tmp_path,
        "provider",
        [
            {"provider_id": 1, "name": "Wine Shop", "etl_run_id": 1, "updated_at": _NOW},
        ],
    )
    _write_entity(
        tmp_path,
        "wine",
        [
            _minimal_wine(
                100, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0, dossier_path="cellar/0100-cuvee-x-2020.md"
            ),
            _minimal_wine(101, 2, "Barolo Y", 2019, appellation_id=2, dossier_path="cellar/0101-barolo-y-2019.md"),
        ],
    )
    _write_entity(
        tmp_path,
        "wine_grape",
        [
            {"wine_id": 100, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW},
            {"wine_id": 101, "grape_id": 2, "percentage": 100.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW},
        ],
    )
    _write_entity(
        tmp_path,
        "bottle",
        [
            _minimal_bottle(200, 100, 1, shelf="A1", bottle_number=1),
            _minimal_bottle(201, 100, 1, shelf="A1", bottle_number=2),
            _minimal_bottle(202, 101, 1, shelf="B1", bottle_number=1),
        ],
    )
    _write_entity(
        tmp_path,
        "tasting",
        [
            _minimal_tasting(300, 100, tasting_date=date(2024, 6, 1)),
        ],
    )
    _write_entity(
        tmp_path,
        "pro_rating",
        [
            _minimal_pro_rating(400, 100, source="Parker", score=92.0),
        ],
    )


def _build_new_entities(**overrides: list[dict]) -> dict[str, list[dict]]:
    """Build a fresh-transform result matching the seed, with overrides."""
    base: dict[str, list[dict]] = {
        "winery": [
            {"winery_id": 1, "name": "Alpha"},
            {"winery_id": 2, "name": "Beta"},
        ],
        "appellation": [
            {"appellation_id": 1, "country": "France", "region": "Bordeaux", "subregion": None, "classification": None},
            {"appellation_id": 2, "country": "Italy", "region": "Piedmont", "subregion": None, "classification": None},
        ],
        "grape": [
            {"grape_id": 1, "name": "Merlot"},
            {"grape_id": 2, "name": "Nebbiolo"},
        ],
        "cellar": [
            {"cellar_id": 1, "name": "01 Main", "location_type": "onsite", "sort_order": 1},
        ],
        "provider": [
            {"provider_id": 1, "name": "Wine Shop"},
        ],
        "wine": [
            _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
            _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
        ],
        "wine_grape": [
            {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW},
            {"wine_id": 2, "grape_id": 2, "percentage": 100.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW},
        ],
        "bottle": [
            _minimal_bottle(1, 1, 1, shelf="A1", bottle_number=1),
            _minimal_bottle(2, 1, 1, shelf="A1", bottle_number=2),
            _minimal_bottle(3, 2, 1, shelf="B1", bottle_number=1),
        ],
        "tasting": [
            _minimal_tasting(1, 1, tasting_date=date(2024, 6, 1)),
        ],
        "pro_rating": [
            _minimal_pro_rating(1, 1, source="Parker", score=92.0),
        ],
    }
    for key, rows in overrides.items():
        base[key] = rows
    return base


# ---------------------------------------------------------------------------
# compute_file_hash
# ---------------------------------------------------------------------------


class TestComputeFileHash:
    def test_deterministic(self, tmp_path):
        p = tmp_path / "a.txt"
        p.write_text("hello")
        assert compute_file_hash(p) == compute_file_hash(p)

    def test_different_content(self, tmp_path):
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        a.write_text("hello")
        b.write_text("world")
        assert compute_file_hash(a) != compute_file_hash(b)


# ---------------------------------------------------------------------------
# next_run_id / load helpers
# ---------------------------------------------------------------------------


class TestNextRunId:
    def test_empty_dir(self, tmp_path):
        assert next_run_id(tmp_path) == 1

    def test_after_one_run(self, tmp_path):
        write_parquet(
            "etl_run",
            [
                {
                    "run_id": 1,
                    "started_at": _NOW,
                    "finished_at": _NOW,
                    "run_type": "full",
                    "wines_source_hash": "a",
                    "bottles_source_hash": "b",
                    "bottles_gone_source_hash": None,
                    "total_inserts": 1,
                    "total_updates": 0,
                    "total_deletes": 0,
                    "wines_inserted": 1,
                    "wines_updated": 0,
                    "wines_deleted": 0,
                    "wines_renamed": 0,
                }
            ],
            tmp_path,
        )
        assert next_run_id(tmp_path) == 2


class TestLoadExisting:
    def test_missing_files_gives_empty_lists(self, tmp_path):
        existing = load_existing(tmp_path)
        for et in ENTITY_ORDER:
            assert existing[et] == []


# ---------------------------------------------------------------------------
# _stabilize_entity
# ---------------------------------------------------------------------------


class TestStabilizeEntity:
    def test_identity_when_no_old(self):
        new = [
            {"winery_id": 1, "name": "Alpha"},
            {"winery_id": 2, "name": "Beta"},
        ]
        result, remap = _stabilize_entity(new, [], "winery", {})
        # IDs start at 1 (max_old=0 + 1)
        assert remap == {1: 1, 2: 2}

    def test_reuses_old_ids(self):
        old = [
            {"winery_id": 10, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
            {"winery_id": 20, "name": "Beta", "etl_run_id": 1, "updated_at": _NOW},
        ]
        new = [
            {"winery_id": 1, "name": "Alpha"},
            {"winery_id": 2, "name": "Beta"},
        ]
        result, remap = _stabilize_entity(new, old, "winery", {})
        assert remap == {1: 10, 2: 20}
        assert result[0]["winery_id"] == 10
        assert result[1]["winery_id"] == 20

    def test_new_entity_gets_next_id(self):
        old = [
            {"winery_id": 5, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
        ]
        new = [
            {"winery_id": 1, "name": "Alpha"},
            {"winery_id": 2, "name": "Gamma"},
        ]
        result, remap = _stabilize_entity(new, old, "winery", {})
        assert remap[1] == 5  # Alpha → reuse old ID
        assert remap[2] == 6  # Gamma → max(5) + 1

    def test_fk_remapping_applied(self):
        """Wine rows should have winery_id remapped before NK comparison."""
        old_wines = [
            _minimal_wine(wine_id=100, winery_id=10, name="CuvéeX", vintage=2020),
        ]
        new_wines = [
            {
                **_minimal_wine(wine_id=1, winery_id=1, name="CuvéeX", vintage=2020),
                "etl_run_id": None,
                "updated_at": None,
            },
        ]
        # winery fresh-id 1 → stable 10
        remappings = {"winery": {1: 10}}
        result, remap = _stabilize_entity(new_wines, old_wines, "wine", remappings)
        # Wine's winery_id should now be 10 (remapped), and wine_id reused
        assert result[0]["winery_id"] == 10
        assert remap[1] == 100

    def test_composite_pk_returns_empty_remap(self):
        new = [{"wine_id": 1, "grape_id": 2, "percentage": 50.0, "sort_order": 1}]
        result, remap = _stabilize_entity(new, [], "wine_grape", {})
        assert remap == {}


# ---------------------------------------------------------------------------
# _diff_rows
# ---------------------------------------------------------------------------


class TestDiffRows:
    def test_all_inserts(self):
        new = [
            {"winery_id": 1, "name": "Alpha"},
            {"winery_id": 2, "name": "Beta"},
        ]
        annotated, changes = _diff_rows(new, [], "winery", 1, _NOW)
        assert len(changes) == 2
        assert all(c["change_type"] == "insert" for c in changes)
        assert annotated[0]["etl_run_id"] == 1
        assert annotated[0]["updated_at"] == _NOW

    def test_unchanged(self):
        old = [{"winery_id": 1, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW}]
        new = [{"winery_id": 1, "name": "Alpha"}]
        annotated, changes = _diff_rows(new, old, "winery", 2, _LATER)
        assert len(changes) == 0
        # Should preserve old metadata
        assert annotated[0]["etl_run_id"] == 1
        assert annotated[0]["updated_at"] == _NOW

    def test_update_detected(self):
        old = [{"winery_id": 1, "name": "Alpha", "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW}]
        new = [{"winery_id": 1, "name": "Alpha", "sort_order": 2}]
        annotated, changes = _diff_rows(new, old, "cellar", 2, _LATER)
        assert len(changes) == 1
        assert changes[0]["change_type"] == "update"
        assert "sort_order" in json.loads(changes[0]["changed_fields"])
        assert annotated[0]["etl_run_id"] == 2
        assert annotated[0]["updated_at"] == _LATER

    def test_delete_detected(self):
        old = [
            {"winery_id": 1, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
            {"winery_id": 2, "name": "Beta", "etl_run_id": 1, "updated_at": _NOW},
        ]
        new = [{"winery_id": 1, "name": "Alpha"}]
        _, changes = _diff_rows(new, old, "winery", 2, _LATER)
        deletes = [c for c in changes if c["change_type"] == "delete"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == 2

    def test_mixed_operations(self):
        old = [
            {"winery_id": 1, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
            {"winery_id": 2, "name": "Beta", "etl_run_id": 1, "updated_at": _NOW},
        ]
        new = [
            {"winery_id": 1, "name": "Alpha"},  # unchanged
            {"winery_id": 3, "name": "Gamma"},  # insert
        ]
        _, changes = _diff_rows(new, old, "winery", 2, _LATER)
        types = {c["change_type"] for c in changes}
        assert types == {"insert", "delete"}

    def test_float32_roundtrip_not_spurious(self):
        """float32 precision loss from Parquet should not trigger updates."""
        import struct

        val64 = 14.123456789
        val32 = struct.unpack("f", struct.pack("f", val64))[0]
        old = [{"winery_id": 1, "name": "A", "alcohol": val32, "etl_run_id": 1, "updated_at": _NOW}]
        new = [{"winery_id": 1, "name": "A", "alcohol": val64}]
        _, changes = _diff_rows(new, old, "winery", 2, _LATER)
        assert len(changes) == 0

    def test_duplicate_natural_keys(self):
        """Multiple rows with the same NK are matched positionally."""
        old = [
            {"winery_id": 1, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
            {"winery_id": 2, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
        ]
        new = [
            {"winery_id": 10, "name": "Alpha"},
            {"winery_id": 20, "name": "Alpha"},
        ]
        _, changes = _diff_rows(new, old, "winery", 2, _LATER)
        assert len(changes) == 0  # no changes


# ---------------------------------------------------------------------------
# Wine rename detection (stabilisation + diff)
# ---------------------------------------------------------------------------


class TestStabilizeWineRename:
    """Partial-NK fallback in _stabilize_entity preserves wine_id on name change."""

    def test_wine_name_change_preserves_id(self):
        old = [_minimal_wine(100, 1, "Old Name", 2020)]
        new = [_minimal_wine(1, 1, "New Name", 2020)]
        result, remap = _stabilize_entity(new, old, "wine", {})
        assert remap[1] == 100
        assert result[0]["wine_id"] == 100

    def test_no_fallback_when_vintage_differs(self):
        old = [_minimal_wine(100, 1, "Old Name", 2020)]
        new = [_minimal_wine(1, 1, "New Name", 2019)]
        _, remap = _stabilize_entity(new, old, "wine", {})
        # Different vintage → no partial-NK match → new ID
        assert remap[1] != 100

    def test_no_fallback_when_ambiguous(self):
        """Two old wines with same partial key → no 1-to-1 match."""
        old = [
            _minimal_wine(100, 1, "Name A", 2020),
            _minimal_wine(101, 1, "Name B", 2020),
        ]
        new = [_minimal_wine(1, 1, "Name C", 2020)]
        _, remap = _stabilize_entity(new, old, "wine", {})
        # Ambiguous (2 old candidates) → no fallback
        assert remap[1] not in (100, 101)

    def test_exact_match_takes_priority(self):
        """If NK matches exactly, don't use fallback."""
        old = [_minimal_wine(100, 1, "Same Name", 2020)]
        new = [_minimal_wine(1, 1, "Same Name", 2020)]
        _, remap = _stabilize_entity(new, old, "wine", {})
        assert remap[1] == 100  # exact NK match

    def test_non_wine_entity_no_fallback(self):
        """Partial-NK fallback only applies to wine entity."""
        old = [{"winery_id": 10, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW}]
        new = [{"winery_id": 1, "name": "Alpha Wines"}]
        _, remap = _stabilize_entity(new, old, "winery", {})
        # Name changed → no NK match → new ID (no fallback for winery)
        assert remap[1] == 11  # max_old(10) + 1


class TestDiffRowsRename:
    """_diff_rows emits 'rename' when same entity_id appears as INSERT+DELETE."""

    def test_rename_detected(self):
        # After stabilisation, both old and new share wine_id=100
        old = [_minimal_wine(100, 1, "Old Name", 2020)]
        new = [_minimal_wine(100, 1, "New Name", 2020)]
        _, changes = _diff_rows(new, old, "wine", 2, _LATER)
        assert len(changes) == 1
        assert changes[0]["change_type"] == "rename"
        assert changes[0]["entity_id"] == 100
        changed = json.loads(changes[0]["changed_fields"])
        assert "name" in changed

    def test_rename_includes_all_changed_fields(self):
        old = [_minimal_wine(100, 1, "Old Name", 2020, comment="old note")]
        new = [_minimal_wine(100, 1, "New Name", 2020, comment="new note")]
        _, changes = _diff_rows(new, old, "wine", 2, _LATER)
        assert changes[0]["change_type"] == "rename"
        changed = json.loads(changes[0]["changed_fields"])
        assert "name" in changed
        assert "comment" in changed

    def test_normal_insert_delete_when_ids_differ(self):
        """Without partial-NK stabilisation, ids differ → normal insert+delete."""
        old = [_minimal_wine(100, 1, "Old Name", 2020)]
        new = [_minimal_wine(200, 1, "New Name", 2020)]
        _, changes = _diff_rows(new, old, "wine", 2, _LATER)
        types = {c["change_type"] for c in changes}
        assert types == {"insert", "delete"}


# ---------------------------------------------------------------------------
# Fuzzy wine rename matching
# ---------------------------------------------------------------------------


class TestFuzzyWineMatch:
    """Fuzzy matching in _stabilize_entity for wines with similar names."""

    def test_fuzzy_match_similar_name(self):
        old = [_minimal_wine(100, 1, "Reserve", 2020)]
        new = [_minimal_wine(1, 1, "Réserve", 2020)]
        # "Reserve" and "Réserve" are similar — accent-folded the same but
        # difflib scores on raw strings.  Score is well above 0.85.
        _, remap = _stabilize_entity(new, old, "wine", {})
        assert remap[1] == 100

    def test_fuzzy_match_below_threshold(self):
        """N-to-N: below-threshold names are NOT matched."""
        old = [
            _minimal_wine(100, 1, "Cuvée X", 2020),
            _minimal_wine(101, 1, "Barolo Y", 2020),
        ]
        new = [
            _minimal_wine(1, 1, "Zinfandel Z", 2020),
            _minimal_wine(2, 1, "Malbec M", 2020),
        ]
        ic = IdentityConfig(rename_threshold=0.85)
        _, remap = _stabilize_entity(new, old, "wine", {}, identity_config=ic)
        # Very different names → below threshold → no match
        assert remap[1] not in (100, 101)
        assert remap[2] not in (100, 101)

    def test_fuzzy_match_requires_same_vintage(self):
        old = [_minimal_wine(100, 1, "Reserve", 2020)]
        new = [_minimal_wine(1, 1, "Reserve", 2019)]
        _, remap = _stabilize_entity(new, old, "wine", {})
        # Different vintage → different partial key → no match
        assert remap[1] != 100

    def test_fuzzy_match_disabled_via_config(self):
        """Two wines with same partial-key but different names (2 each side)."""
        old = [
            _minimal_wine(100, 1, "Reserve A", 2020),
            _minimal_wine(101, 1, "Reserve B", 2020),
        ]
        new = [
            _minimal_wine(1, 1, "Reserva A", 2020),
            _minimal_wine(2, 1, "Reserva B", 2020),
        ]
        ic = IdentityConfig(enable_fuzzy_match=False)
        _, remap = _stabilize_entity(new, old, "wine", {}, identity_config=ic)
        # Fuzzy disabled, not 1-to-1 → no match
        assert remap[1] not in (100, 101)
        assert remap[2] not in (100, 101)

    def test_fuzzy_match_best_candidate(self):
        """Multiple old candidates — highest scoring match wins."""
        old = [
            _minimal_wine(100, 1, "Cuvée Prestige", 2020),
            _minimal_wine(101, 1, "Grand Cru", 2020),
        ]
        new = [
            _minimal_wine(1, 1, "Cuvée Prestige Reserve", 2020),
            _minimal_wine(2, 1, "Grand Cru Classé", 2020),
        ]
        ic = IdentityConfig(rename_threshold=0.5)  # low threshold for test
        _, remap = _stabilize_entity(new, old, "wine", {}, identity_config=ic)
        # "Cuvée Prestige Reserve" → "Cuvée Prestige" (best match)
        # "Grand Cru Classé" → "Grand Cru" (best match)
        assert remap[1] == 100
        assert remap[2] == 101


class TestSyncFuzzyWineRename:
    """End-to-end: fuzzy matching through sync()."""

    def test_fuzzy_rename_through_sync(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                # Slightly different name (accent change)
                _minimal_wine(1, 1, "Cuvée X Reserve", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        ic = IdentityConfig(rename_threshold=0.5)
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER, identity_config=ic)
        wine = [w for w in stabilised["wine"] if w["name"] == "Cuvée X Reserve"][0]
        assert wine["wine_id"] == 100  # preserved
        renames = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "rename"]
        assert len(renames) == 1

    def test_fuzzy_disabled_falls_back_to_insert_delete(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X Reserve", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        ic = IdentityConfig(enable_fuzzy_match=False)
        _, changes, _ = sync(ents, tmp_path, 2, _LATER, identity_config=ic)
        renames = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "rename"]
        # 1-to-1 partial key match → still detected (not fuzzy)
        assert len(renames) == 1


# ---------------------------------------------------------------------------
# annotate_full_load
# ---------------------------------------------------------------------------


class TestAnnotateFullLoad:
    def test_all_inserts(self, tmp_path):
        entities = {
            "winery": [{"winery_id": 1, "name": "A"}],
            "appellation": [],
            "grape": [],
            "cellar": [],
            "provider": [],
            "wine": [],
            "wine_grape": [],
            "bottle": [],
            "tasting": [],
            "pro_rating": [],
        }
        annotated, changes = annotate_full_load(entities, tmp_path, 1, _NOW)
        assert len(changes) == 1
        assert changes[0]["change_type"] == "insert"
        assert changes[0]["entity_type"] == "winery"
        assert annotated["winery"][0]["etl_run_id"] == 1

    def test_change_ids_sequential(self, tmp_path):
        entities = {
            "winery": [{"winery_id": 1, "name": "A"}, {"winery_id": 2, "name": "B"}],
            "appellation": [],
            "grape": [],
            "cellar": [],
            "provider": [],
            "wine": [],
            "wine_grape": [],
            "bottle": [],
            "tasting": [],
            "pro_rating": [],
        }
        _, changes = annotate_full_load(entities, tmp_path, 1, _NOW)
        assert [c["change_id"] for c in changes] == [1, 2]

    def test_full_load_preserves_tombstones(self, tmp_path):
        """Full load keeps existing tombstones not present in new data."""
        # Write existing data including a tombstone
        existing = [
            _minimal_wine(100, 1, "Cuvée X", 2020, dossier_path="cellar/0100-cuvee-x-2020.md"),
            {
                **_minimal_wine(101, 1, "Barolo Y", 2019, dossier_path="cellar/0101-barolo-y-2019.md"),
                "is_deleted": True,
            },
        ]
        schema = SCHEMAS["wine"]
        table = pa.Table.from_pylist(existing, schema=schema)
        pq.write_table(table, tmp_path / "wine.parquet")
        for et in ENTITY_ORDER:
            if et not in ("winery", "wine"):
                _write_empty(tmp_path, et)
        _write_entity(tmp_path, "winery", [{"winery_id": 1, "name": "A", "etl_run_id": 1, "updated_at": _NOW}])
        # Full load with only wine 100 (tombstone 101 not in new data)
        entities = {
            "winery": [{"winery_id": 1, "name": "A"}],
            "wine": [_minimal_wine(1, 1, "Cuvée X", 2020)],
        }
        for et in ENTITY_ORDER:
            entities.setdefault(et, [])
        annotated, _ = annotate_full_load(entities, tmp_path, 2, _LATER)
        # Tombstone for wine 101 must survive the full load
        tombstones = [w for w in annotated["wine"] if w.get("is_deleted")]
        assert len(tombstones) == 1
        assert tombstones[0]["wine_id"] == 101


# ---------------------------------------------------------------------------
# sync (end-to-end) — basic
# ---------------------------------------------------------------------------


class TestSync:
    def _seed(self, tmp_path: Path) -> None:
        _write_entity(
            tmp_path,
            "winery",
            [
                {"winery_id": 1, "name": "Alpha", "etl_run_id": 1, "updated_at": _NOW},
                {"winery_id": 2, "name": "Beta", "etl_run_id": 1, "updated_at": _NOW},
            ],
        )
        for et in ENTITY_ORDER:
            if et == "winery":
                continue
            if et == "wine":
                _write_entity(
                    tmp_path,
                    "wine",
                    [
                        _minimal_wine(100, 1, "Cuvée X", 2020, dossier_path="archive/0100-cuvee-x-2020.md"),
                    ],
                )
            else:
                _write_empty(tmp_path, et)

    def test_no_changes_on_identical_data(self, tmp_path):
        self._seed(tmp_path)
        new_entities = {
            "winery": [{"winery_id": 1, "name": "Alpha"}, {"winery_id": 2, "name": "Beta"}],
            "wine": [_minimal_wine(1, 1, "Cuvée X", 2020)],
        }
        for et in ENTITY_ORDER:
            new_entities.setdefault(et, [])

        stabilised, changes, _ = sync(new_entities, tmp_path, 2, _LATER)
        assert len(changes) == 0
        winery_ids = {r["winery_id"] for r in stabilised["winery"]}
        assert winery_ids == {1, 2}

    def test_new_winery_gets_next_id(self, tmp_path):
        self._seed(tmp_path)
        new_entities = {
            "winery": [
                {"winery_id": 1, "name": "Alpha"},
                {"winery_id": 2, "name": "Beta"},
                {"winery_id": 3, "name": "Gamma"},
            ],
            "wine": [_minimal_wine(1, 1, "Cuvée X", 2020)],
        }
        for et in ENTITY_ORDER:
            new_entities.setdefault(et, [])

        stabilised, changes, _ = sync(new_entities, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["change_type"] == "insert"]
        assert len(inserts) == 1
        assert inserts[0]["entity_type"] == "winery"
        gamma = [w for w in stabilised["winery"] if w["name"] == "Gamma"]
        assert gamma[0]["winery_id"] == 3

    def test_deleted_entity_logged(self, tmp_path):
        self._seed(tmp_path)
        new_entities = {
            "winery": [{"winery_id": 1, "name": "Alpha"}],
            "wine": [_minimal_wine(1, 1, "Cuvée X", 2020)],
        }
        for et in ENTITY_ORDER:
            new_entities.setdefault(et, [])

        _, changes, _ = sync(new_entities, tmp_path, 2, _LATER)
        deletes = [c for c in changes if c["change_type"] == "delete" and c["entity_type"] == "winery"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == 2

    def test_wine_fk_remapped(self, tmp_path):
        self._seed(tmp_path)
        new_entities = {
            "winery": [{"winery_id": 1, "name": "Alpha"}, {"winery_id": 2, "name": "Beta"}],
            "wine": [
                _minimal_wine(1, 1, "Cuvée X", 2020),
                _minimal_wine(2, 1, "Cuvée Y", 2021),
            ],
        }
        for et in ENTITY_ORDER:
            new_entities.setdefault(et, [])

        stabilised, changes, _ = sync(new_entities, tmp_path, 2, _LATER)
        new_wine = [w for w in stabilised["wine"] if w["name"] == "Cuvée Y"]
        assert len(new_wine) == 1
        assert new_wine[0]["winery_id"] == 1
        wine_inserts = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "insert"]
        assert len(wine_inserts) == 1

    def test_change_ids_continue_from_existing(self, tmp_path):
        write_parquet(
            "change_log",
            [
                {
                    "change_id": 1,
                    "run_id": 1,
                    "entity_type": "winery",
                    "entity_id": 1,
                    "change_type": "insert",
                    "changed_fields": None,
                },
                {
                    "change_id": 2,
                    "run_id": 1,
                    "entity_type": "winery",
                    "entity_id": 2,
                    "change_type": "insert",
                    "changed_fields": None,
                },
            ],
            tmp_path,
        )

        self._seed(tmp_path)
        new_entities = {
            "winery": [
                {"winery_id": 1, "name": "Alpha"},
                {"winery_id": 2, "name": "Beta"},
                {"winery_id": 3, "name": "New"},
            ],
            "wine": [_minimal_wine(1, 1, "Cuvée X", 2020)],
        }
        for et in ENTITY_ORDER:
            new_entities.setdefault(et, [])

        _, changes, _ = sync(new_entities, tmp_path, 2, _LATER)
        assert all(c["change_id"] >= 3 for c in changes)


# ---------------------------------------------------------------------------
# Full scenario tests (rich seed)
# ---------------------------------------------------------------------------


class TestSyncLookupEntities:
    """Lookup entity changes: winery, appellation, grape, cellar, provider."""

    def test_new_winery_insert(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha"},
                {"winery_id": 2, "name": "Beta"},
                {"winery_id": 3, "name": "Gamma"},
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "winery" and c["change_type"] == "insert"]
        assert len(inserts) == 1
        gamma = [w for w in stabilised["winery"] if w["name"] == "Gamma"]
        assert gamma[0]["winery_id"] == 3
        assert gamma[0]["etl_run_id"] == 2

    def test_winery_removed(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            winery=[{"winery_id": 1, "name": "Alpha"}],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        deletes = [c for c in changes if c["entity_type"] == "winery" and c["change_type"] == "delete"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == 2

    def test_new_appellation_insert(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            appellation=[
                {
                    "appellation_id": 1,
                    "country": "France",
                    "region": "Bordeaux",
                    "subregion": None,
                    "classification": None,
                },
                {
                    "appellation_id": 2,
                    "country": "Italy",
                    "region": "Piedmont",
                    "subregion": None,
                    "classification": None,
                },
                {"appellation_id": 3, "country": "Spain", "region": "Rioja", "subregion": None, "classification": None},
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "appellation" and c["change_type"] == "insert"]
        assert len(inserts) == 1

    def test_new_grape_insert(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            grape=[
                {"grape_id": 1, "name": "Merlot"},
                {"grape_id": 2, "name": "Nebbiolo"},
                {"grape_id": 3, "name": "Cabernet Sauvignon"},
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "grape" and c["change_type"] == "insert"]
        assert len(inserts) == 1
        cab = [g for g in stabilised["grape"] if g["name"] == "Cabernet Sauvignon"]
        assert cab[0]["grape_id"] == 3

    def test_new_cellar_insert(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            cellar=[
                {"cellar_id": 1, "name": "01 Main", "location_type": "onsite", "sort_order": 1},
                {"cellar_id": 2, "name": "02 Backup", "location_type": "onsite", "sort_order": 2},
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "cellar" and c["change_type"] == "insert"]
        assert len(inserts) == 1

    def test_new_provider_insert(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            provider=[
                {"provider_id": 1, "name": "Wine Shop"},
                {"provider_id": 2, "name": "Online Store"},
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "provider" and c["change_type"] == "insert"]
        assert len(inserts) == 1


class TestSyncWineChanges:
    """Wine-level changes: updates, inserts, removes."""

    def test_wine_comment_updated(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0, comment="Excellent vintage"),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        updates = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "update"]
        assert len(updates) == 1
        assert updates[0]["entity_id"] == 100
        assert "comment" in json.loads(updates[0]["changed_fields"])
        wine = [w for w in stabilised["wine"] if w["wine_id"] == 100][0]
        assert wine["comment"] == "Excellent vintage"
        assert wine["etl_run_id"] == 2
        assert wine["updated_at"] == _LATER

    def test_wine_alcohol_updated(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=15.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        updates = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "update"]
        assert len(updates) == 1
        assert "alcohol_pct" in json.loads(updates[0]["changed_fields"])

    def test_wine_appellation_changed(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=2, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        updates = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "update"]
        assert len(updates) == 1
        assert "appellation_id" in json.loads(updates[0]["changed_fields"])

    def test_new_wine_added(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 1, "Cuvée Z", 2021, appellation_id=1),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "insert"]
        assert len(inserts) == 1
        new_wine = [w for w in stabilised["wine"] if w["name"] == "Cuvée Z"]
        assert new_wine[0]["wine_id"] == 102
        assert new_wine[0]["winery_id"] == 1
        assert new_wine[0]["etl_run_id"] == 2

    def test_wine_removed(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        deletes = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "delete"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == 101
        # Tombstone retained: both wines present, removed one is_deleted=True
        assert len(stabilised["wine"]) == 2
        tombstone = [w for w in stabilised["wine"] if w["wine_id"] == 101][0]
        assert tombstone["is_deleted"] is True
        active = [w for w in stabilised["wine"] if w["wine_id"] == 100][0]
        assert active["is_deleted"] is False

    def test_wine_reappears_after_delete(self, tmp_path):
        """A wine deleted in run 2 and re-added in run 3 is un-tombstoned."""
        _seed_full(tmp_path)
        # Run 2: remove wine 101
        ents2 = _build_new_entities(
            wine=[_minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0)],
        )
        stabilised2, _, _ = sync(ents2, tmp_path, 2, _LATER)
        assign_dossier_paths(stabilised2)
        from cellarbrain.writer import write_all

        write_all(stabilised2, tmp_path)
        # Run 3: wine 101 is back
        ents3 = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        stabilised3, _, _ = sync(ents3, tmp_path, 3, _LATER)
        revived = [w for w in stabilised3["wine"] if w["wine_id"] == 101][0]
        assert revived["is_deleted"] is False
        assert revived["etl_run_id"] == 3

    def test_unchanged_wine_preserves_old_metadata(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities()
        stabilised, _, _ = sync(ents, tmp_path, 2, _LATER)
        wine = [w for w in stabilised["wine"] if w["wine_id"] == 100][0]
        assert wine["etl_run_id"] == 1
        assert wine["updated_at"] == _NOW


class TestSyncWineRename:
    """Wine name change detected as rename, preserving wine_id."""

    def test_wine_name_rename_detected(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                # Rename "Cuvée X" → "Cuvée Prestige" (same winery, vintage)
                _minimal_wine(1, 1, "Cuvée Prestige", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        renames = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "rename"]
        assert len(renames) == 1
        assert renames[0]["entity_id"] == 100
        assert "name" in json.loads(renames[0]["changed_fields"])

    def test_wine_rename_preserves_id(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée Prestige", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        stabilised, _, _ = sync(ents, tmp_path, 2, _LATER)
        wine = [w for w in stabilised["wine"] if w["name"] == "Cuvée Prestige"][0]
        assert wine["wine_id"] == 100  # preserved from old "Cuvée X"
        assert wine["etl_run_id"] == 2
        assert wine["updated_at"] == _LATER

    def test_rename_not_triggered_by_different_vintage(self, tmp_path):
        """Name + vintage both change → different partial key → insert+delete."""
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                # Replace "Cuvée X 2020" with "Cuvée Prestige 2021" (different vintage)
                _minimal_wine(1, 1, "Cuvée Prestige", 2021, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        renames = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "rename"]
        assert len(renames) == 0
        # Should be insert + delete instead
        inserts = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "insert"]
        deletes = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "delete"]
        assert len(inserts) == 1
        assert len(deletes) == 1

    def test_rename_cascades_to_bottles(self, tmp_path):
        """Bottles of a renamed wine keep referencing the stable wine_id."""
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée Prestige", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        stabilised, _, _ = sync(ents, tmp_path, 2, _LATER)
        bottles_for_renamed = [b for b in stabilised["bottle"] if b["wine_id"] == 100]
        # Originally 2 bottles for wine 100 ("Cuvée X")
        assert len(bottles_for_renamed) == 2

    def test_rename_does_not_create_tombstone(self, tmp_path):
        """A renamed wine must NOT produce an is_deleted tombstone."""
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée Prestige", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        stabilised, _, _ = sync(ents, tmp_path, 2, _LATER)
        # No tombstones — both original wines are accounted for via rename
        tombstones = [w for w in stabilised["wine"] if w.get("is_deleted")]
        assert tombstones == []


# ---------------------------------------------------------------------------
# Winery rename detection
# ---------------------------------------------------------------------------


class TestDetectWineryRenames:
    """Unit tests for the structural heuristic."""

    def test_single_rename_detected(self):
        old_w = [{"winery_id": 10, "name": "Alpha"}]
        new_w = [{"winery_id": 1, "name": "Alpha Wines"}]
        old_wines = [{"winery_id": 10}, {"winery_id": 10}]
        new_wines = [{"winery_id": 1}, {"winery_id": 1}]
        result = _detect_winery_renames(new_w, old_w, new_wines, old_wines)
        assert result == {1: 10}

    def test_no_rename_when_counts_differ(self):
        old_w = [{"winery_id": 10, "name": "Alpha"}]
        new_w = [{"winery_id": 1, "name": "Alpha Wines"}]
        old_wines = [{"winery_id": 10}, {"winery_id": 10}, {"winery_id": 10}]
        new_wines = [{"winery_id": 1}, {"winery_id": 1}]
        result = _detect_winery_renames(new_w, old_w, new_wines, old_wines)
        assert result == {}

    def test_no_rename_when_multiple_disappeared(self):
        old_w = [
            {"winery_id": 10, "name": "Alpha"},
            {"winery_id": 20, "name": "Beta"},
        ]
        new_w = [
            {"winery_id": 1, "name": "Alpha Wines"},
            {"winery_id": 2, "name": "Beta Wines"},
        ]
        old_wines = [{"winery_id": 10}]
        new_wines = [{"winery_id": 1}]
        result = _detect_winery_renames(new_w, old_w, new_wines, old_wines)
        assert result == {}

    def test_no_rename_when_zero_wines(self):
        old_w = [{"winery_id": 10, "name": "Alpha"}]
        new_w = [{"winery_id": 1, "name": "Alpha Wines"}]
        result = _detect_winery_renames(new_w, old_w, [], [])
        assert result == {}

    def test_matched_winery_not_in_disappeared(self):
        """A winery that still matches by NK is not 'disappeared'."""
        old_w = [
            {"winery_id": 10, "name": "Alpha"},
            {"winery_id": 20, "name": "Beta"},
        ]
        new_w = [
            {"winery_id": 1, "name": "Alpha Wines"},  # appeared
            {"winery_id": 2, "name": "Beta"},  # matched
        ]
        old_wines = [{"winery_id": 10}]
        new_wines = [{"winery_id": 1}]
        # Only Alpha disappeared and Alpha Wines appeared → rename
        result = _detect_winery_renames(new_w, old_w, new_wines, old_wines)
        assert result == {1: 10}


class TestSyncWineryRename:
    """Full sync tests for winery rename detection."""

    def test_winery_rename_detected(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha Wines"},  # renamed from "Alpha"
                {"winery_id": 2, "name": "Beta"},
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        renames = [c for c in changes if c["entity_type"] == "winery" and c["change_type"] == "rename"]
        assert len(renames) == 1
        assert renames[0]["entity_id"] == 1  # old stable winery_id
        assert "name" in json.loads(renames[0]["changed_fields"])

    def test_winery_rename_cascades_to_wine_ids(self, tmp_path):
        """Wines of a renamed winery keep their old stable wine_ids."""
        _seed_full(tmp_path)
        ents = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha Wines"},
                {"winery_id": 2, "name": "Beta"},
            ],
        )
        stabilised, _, _ = sync(ents, tmp_path, 2, _LATER)
        # Wine "Cuvée X" should still have wine_id=100
        wine = [w for w in stabilised["wine"] if w["name"] == "Cuvée X"][0]
        assert wine["wine_id"] == 100
        assert wine["winery_id"] == 1  # old stable winery_id

    def test_no_false_positive_when_counts_differ(self, tmp_path):
        """Winery with different wine count → no rename."""
        _seed_full(tmp_path)
        # Add a third wine to winery 1, then rename winery
        ents = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha Wines"},
                {"winery_id": 2, "name": "Beta"},
            ],
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 1, "Cuvée Z", 2021),  # extra wine
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        renames = [c for c in changes if c["entity_type"] == "winery" and c["change_type"] == "rename"]
        # Counts differ (old=1, new=2) → no rename
        assert len(renames) == 0


class TestSyncCascadingInserts:
    """New wine referencing a new winery/appellation — cascading changes."""

    def test_new_wine_with_new_winery(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha"},
                {"winery_id": 2, "name": "Beta"},
                {"winery_id": 3, "name": "Gamma"},
            ],
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 3, "Gamma Reserve", 2022, appellation_id=1),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)

        winery_inserts = [c for c in changes if c["entity_type"] == "winery" and c["change_type"] == "insert"]
        assert len(winery_inserts) == 1
        gamma_winery = [w for w in stabilised["winery"] if w["name"] == "Gamma"]
        assert gamma_winery[0]["winery_id"] == 3

        wine_inserts = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "insert"]
        assert len(wine_inserts) == 1
        new_wine = [w for w in stabilised["wine"] if w["name"] == "Gamma Reserve"]
        assert new_wine[0]["winery_id"] == 3

    def test_new_wine_with_new_appellation(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            appellation=[
                {
                    "appellation_id": 1,
                    "country": "France",
                    "region": "Bordeaux",
                    "subregion": None,
                    "classification": None,
                },
                {
                    "appellation_id": 2,
                    "country": "Italy",
                    "region": "Piedmont",
                    "subregion": None,
                    "classification": None,
                },
                {"appellation_id": 3, "country": "Spain", "region": "Rioja", "subregion": None, "classification": None},
            ],
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 1, "Rioja Reserve", 2021, appellation_id=3),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        app_inserts = [c for c in changes if c["entity_type"] == "appellation" and c["change_type"] == "insert"]
        assert len(app_inserts) == 1
        wine_inserts = [c for c in changes if c["entity_type"] == "wine" and c["change_type"] == "insert"]
        assert len(wine_inserts) == 1
        new_wine = [w for w in stabilised["wine"] if w["name"] == "Rioja Reserve"]
        assert new_wine[0]["appellation_id"] == 3


class TestSyncBottleChanges:
    """Bottle-level changes: new purchase, consumed (removed), moved."""

    def test_new_bottle_added(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            bottle=[
                _minimal_bottle(1, 1, 1, shelf="A1", bottle_number=1),
                _minimal_bottle(2, 1, 1, shelf="A1", bottle_number=2),
                _minimal_bottle(3, 2, 1, shelf="B1", bottle_number=1),
                _minimal_bottle(4, 1, 1, shelf="A1", bottle_number=3),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "bottle" and c["change_type"] == "insert"]
        assert len(inserts) == 1
        new_bottle = [b for b in stabilised["bottle"] if b["bottle_number"] == 3]
        assert new_bottle[0]["wine_id"] == 100
        assert new_bottle[0]["bottle_id"] == 203
        assert new_bottle[0]["etl_run_id"] == 2

    def test_bottle_removed_consumed(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            bottle=[
                _minimal_bottle(1, 1, 1, shelf="A1", bottle_number=1),
                _minimal_bottle(3, 2, 1, shelf="B1", bottle_number=1),
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        deletes = [c for c in changes if c["entity_type"] == "bottle" and c["change_type"] == "delete"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == 201

    def test_bottle_moved_shelf(self, tmp_path):
        """Bottle NK includes shelf, so a shelf move is delete+insert."""
        _seed_full(tmp_path)
        ents = _build_new_entities(
            bottle=[
                _minimal_bottle(1, 1, 1, shelf="C1", bottle_number=1),
                _minimal_bottle(2, 1, 1, shelf="A1", bottle_number=2),
                _minimal_bottle(3, 2, 1, shelf="B1", bottle_number=1),
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        bot_changes = [c for c in changes if c["entity_type"] == "bottle"]
        inserts = [c for c in bot_changes if c["change_type"] == "insert"]
        deletes = [c for c in bot_changes if c["change_type"] == "delete"]
        assert len(inserts) == 1
        assert len(deletes) == 1


class TestSyncTastingChanges:
    """Tasting changes: new, updated, removed."""

    def test_new_tasting_added(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            tasting=[
                _minimal_tasting(1, 1, tasting_date=date(2024, 6, 1)),
                _minimal_tasting(2, 1, tasting_date=date(2025, 3, 15), note="Improved", score=17.0),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "tasting" and c["change_type"] == "insert"]
        assert len(inserts) == 1
        new_tasting = [t for t in stabilised["tasting"] if t["tasting_date"] == date(2025, 3, 15)]
        assert new_tasting[0]["wine_id"] == 100
        assert new_tasting[0]["note"] == "Improved"
        assert new_tasting[0]["etl_run_id"] == 2

    def test_tasting_note_updated(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            tasting=[
                _minimal_tasting(1, 1, tasting_date=date(2024, 6, 1), note="Actually great", score=18.0),
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        updates = [c for c in changes if c["entity_type"] == "tasting" and c["change_type"] == "update"]
        assert len(updates) == 1
        changed = json.loads(updates[0]["changed_fields"])
        assert "note" in changed
        assert "score" in changed

    def test_tasting_removed(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(tasting=[])
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        deletes = [c for c in changes if c["entity_type"] == "tasting" and c["change_type"] == "delete"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == 300


class TestSyncProRatingChanges:
    """Pro rating changes: new, removed."""

    def test_new_pro_rating_added(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            pro_rating=[
                _minimal_pro_rating(1, 1, source="Parker", score=92.0),
                _minimal_pro_rating(2, 1, source="Suckling", score=95.0),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)
        inserts = [c for c in changes if c["entity_type"] == "pro_rating" and c["change_type"] == "insert"]
        assert len(inserts) == 1
        new_rating = [r for r in stabilised["pro_rating"] if r["source"] == "Suckling"]
        assert new_rating[0]["wine_id"] == 100
        assert new_rating[0]["etl_run_id"] == 2

    def test_pro_rating_removed(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(pro_rating=[])
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        deletes = [c for c in changes if c["entity_type"] == "pro_rating" and c["change_type"] == "delete"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == 400


class TestSyncWineGrapeChanges:
    """wine_grape (blend) changes: new grape, removed grape, pct changed."""

    def test_grape_added_to_blend(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine_grape=[
                {"wine_id": 1, "grape_id": 1, "percentage": 80.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW},
                {"wine_id": 1, "grape_id": 2, "percentage": 20.0, "sort_order": 2, "etl_run_id": 1, "updated_at": _NOW},
                {
                    "wine_id": 2,
                    "grape_id": 2,
                    "percentage": 100.0,
                    "sort_order": 1,
                    "etl_run_id": 1,
                    "updated_at": _NOW,
                },
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        wg_changes = [c for c in changes if c["entity_type"] == "wine_grape"]
        inserts = [c for c in wg_changes if c["change_type"] == "insert"]
        updates = [c for c in wg_changes if c["change_type"] == "update"]
        assert len(inserts) == 1
        assert len(updates) == 1

    def test_grape_removed_from_blend(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine_grape=[
                {
                    "wine_id": 2,
                    "grape_id": 2,
                    "percentage": 100.0,
                    "sort_order": 1,
                    "etl_run_id": 1,
                    "updated_at": _NOW,
                },
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        wg_changes = [c for c in changes if c["entity_type"] == "wine_grape"]
        deletes = [c for c in wg_changes if c["change_type"] == "delete"]
        assert len(deletes) == 1

    def test_grape_percentage_updated(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine_grape=[
                {"wine_id": 1, "grape_id": 1, "percentage": 85.0, "sort_order": 1, "etl_run_id": 1, "updated_at": _NOW},
                {
                    "wine_id": 2,
                    "grape_id": 2,
                    "percentage": 100.0,
                    "sort_order": 1,
                    "etl_run_id": 1,
                    "updated_at": _NOW,
                },
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        wg_updates = [c for c in changes if c["entity_type"] == "wine_grape" and c["change_type"] == "update"]
        assert len(wg_updates) == 1
        assert "percentage" in json.loads(wg_updates[0]["changed_fields"])


class TestSyncMultipleChanges:
    """Multiple entity types changing in a single sync run."""

    def test_new_winery_wine_bottle_tasting_in_one_run(self, tmp_path):
        _seed_full(tmp_path)
        ents = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha"},
                {"winery_id": 2, "name": "Beta"},
                {"winery_id": 3, "name": "Gamma"},
            ],
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 3, "Gamma Res", 2022),
            ],
            bottle=[
                _minimal_bottle(1, 1, 1, shelf="A1", bottle_number=1),
                _minimal_bottle(2, 1, 1, shelf="A1", bottle_number=2),
                _minimal_bottle(3, 2, 1, shelf="B1", bottle_number=1),
                _minimal_bottle(4, 3, 1, shelf="C1", bottle_number=1),
            ],
            tasting=[
                _minimal_tasting(1, 1, tasting_date=date(2024, 6, 1)),
                _minimal_tasting(2, 3, tasting_date=date(2025, 1, 1)),
            ],
        )
        stabilised, changes, _ = sync(ents, tmp_path, 2, _LATER)

        by_type = {}
        for c in changes:
            by_type.setdefault((c["entity_type"], c["change_type"]), []).append(c)

        assert len(by_type.get(("winery", "insert"), [])) == 1
        assert len(by_type.get(("wine", "insert"), [])) == 1
        assert len(by_type.get(("bottle", "insert"), [])) == 1
        assert len(by_type.get(("tasting", "insert"), [])) == 1

        new_bottle = [b for b in stabilised["bottle"] if b["shelf"] == "C1" and b["bottle_number"] == 1]
        new_wine = [w for w in stabilised["wine"] if w["name"] == "Gamma Res"]
        assert new_bottle[0]["wine_id"] == new_wine[0]["wine_id"]

    def test_simultaneous_insert_update_delete(self, tmp_path):
        """One entity inserted, another updated, another deleted — same run."""
        _seed_full(tmp_path)
        ents = _build_new_entities(
            wine=[
                # Cuvée X updated (comment changed)
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0, comment="Updated comment"),
                # Barolo Y removed (not in list)
                # New wine added
                _minimal_wine(3, 1, "New Cuvée", 2023),
            ],
        )
        _, changes, _ = sync(ents, tmp_path, 2, _LATER)
        wine_changes = [c for c in changes if c["entity_type"] == "wine"]
        types = {c["change_type"] for c in wine_changes}
        assert types == {"insert", "update", "delete"}


class TestSyncConsecutiveRuns:
    """Verify multiple sync runs accumulate correctly."""

    def test_two_syncs_stable_ids(self, tmp_path):
        _seed_full(tmp_path)

        # Sync 1: add Gamma winery + wine
        ents1 = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha"},
                {"winery_id": 2, "name": "Beta"},
                {"winery_id": 3, "name": "Gamma"},
            ],
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 3, "Gamma Res", 2022),
            ],
        )
        stable1, changes1, _ = sync(ents1, tmp_path, 2, _LATER)
        from cellarbrain.writer import write_all

        assign_dossier_paths(stable1)
        write_all(stable1, tmp_path)

        gamma_winery_id = [w for w in stable1["winery"] if w["name"] == "Gamma"][0]["winery_id"]
        gamma_wine_id = [w for w in stable1["wine"] if w["name"] == "Gamma Res"][0]["wine_id"]

        # Sync 2: same data → zero changes, same IDs
        ents2 = _build_new_entities(
            winery=[
                {"winery_id": 1, "name": "Alpha"},
                {"winery_id": 2, "name": "Beta"},
                {"winery_id": 3, "name": "Gamma"},
            ],
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 3, "Gamma Res", 2022),
            ],
        )
        stable2, changes2, _ = sync(ents2, tmp_path, 3, _EVEN_LATER)
        assert len(changes2) == 0

        gamma_w2 = [w for w in stable2["winery"] if w["name"] == "Gamma"][0]
        gamma_wine2 = [w for w in stable2["wine"] if w["name"] == "Gamma Res"][0]
        assert gamma_w2["winery_id"] == gamma_winery_id
        assert gamma_wine2["wine_id"] == gamma_wine_id

    def test_add_then_remove_across_syncs(self, tmp_path):
        _seed_full(tmp_path)

        # Sync 1: add wine
        ents1 = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
                _minimal_wine(3, 1, "Temp Wine", 2023),
            ],
        )
        stable1, _, _ = sync(ents1, tmp_path, 2, _LATER)
        from cellarbrain.writer import write_all

        assign_dossier_paths(stable1)
        write_all(stable1, tmp_path)

        temp_id = [w for w in stable1["wine"] if w["name"] == "Temp Wine"][0]["wine_id"]

        # Sync 2: remove the wine
        ents2 = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        _, changes2, _ = sync(ents2, tmp_path, 3, _EVEN_LATER)
        deletes = [c for c in changes2 if c["entity_type"] == "wine" and c["change_type"] == "delete"]
        assert len(deletes) == 1
        assert deletes[0]["entity_id"] == temp_id

    def test_update_then_no_change(self, tmp_path):
        """Sync 1: update wine. Sync 2: same data → zero changes."""
        _seed_full(tmp_path)

        # Sync 1: update wine comment
        ents1 = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0, comment="New comment"),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        stable1, changes1, _ = sync(ents1, tmp_path, 2, _LATER)
        assert any(c["change_type"] == "update" for c in changes1)
        from cellarbrain.writer import write_all

        assign_dossier_paths(stable1)
        write_all(stable1, tmp_path)

        # Sync 2: exact same (post-update) data
        ents2 = _build_new_entities(
            wine=[
                _minimal_wine(1, 1, "Cuvée X", 2020, appellation_id=1, alcohol_pct=14.0, comment="New comment"),
                _minimal_wine(2, 2, "Barolo Y", 2019, appellation_id=2),
            ],
        )
        _, changes2, _ = sync(ents2, tmp_path, 3, _EVEN_LATER)
        assert len(changes2) == 0


# ---------------------------------------------------------------------------
# Slug-based wine classification (28a)
# ---------------------------------------------------------------------------


def _existing_wine(
    wine_id: int,
    slug: str,
    *,
    winery_name: str = "",
    name: str = "",
    vintage: int | None = None,
    is_non_vintage: bool = False,
    is_deleted: bool = False,
    _raw_volume: str | None = None,
    _raw_classification: str | None = None,
    _raw_grapes: str | None = None,
    category: str = "red",
    list_price: Decimal | None = None,
) -> dict:
    """Helper to build an existing-wine dict as loaded from Parquet."""
    return {
        "wine_id": wine_id,
        "wine_slug": slug,
        "winery_name": winery_name,
        "name": name,
        "vintage": vintage,
        "is_non_vintage": is_non_vintage,
        "is_deleted": is_deleted,
        "_raw_volume": _raw_volume,
        "_raw_classification": _raw_classification,
        "_raw_grapes": _raw_grapes,
        "category": category,
        "list_price": list_price,
    }


def _csv_row(
    winery: str = "Domaine Test",
    name: str = "",
    year: str = "2020",
    *,
    volume: str = "750mL",
    classification: str = "",
    grapes: str = "",
    category: str = "Red wine",
    price: str = "",
) -> dict:
    return {
        "winery": winery,
        "wine_name": name,
        "vintage_raw": year,
        "volume_raw": volume,
        "classification": classification,
        "grapes_raw": grapes,
        "category_raw": category,
        "list_price_raw": price,
    }


class TestMatchByFingerprint:
    def test_single_candidate(self):
        fp_index = {1: ("750mL", "", "", "red", "")}
        result = _match_by_fingerprint([(1, False)], ("375mL", "", "", "red", ""), fp_index)
        assert result == 1

    def test_exact_match(self):
        fp_index = {
            1: ("375mL", "", "", "red", "32.40"),
            2: ("750mL", "", "", "red", "75.50"),
        }
        result = _match_by_fingerprint(
            [(1, False), (2, False)],
            ("750mL", "", "", "red", "75.50"),
            fp_index,
        )
        assert result == 2

    def test_volume_cascade(self):
        fp_index = {
            1: ("375mL", "", "", "red", ""),
            2: ("750mL", "", "", "red", ""),
        }
        result = _match_by_fingerprint(
            [(1, False), (2, False)],
            ("375mL", "X", "Y", "red", "10"),
            fp_index,
        )
        assert result == 1

    def test_classification_cascade(self):
        fp_index = {
            1: ("750mL", "DOP", "", "red", ""),
            2: ("750mL", "DOC", "", "red", ""),
        }
        result = _match_by_fingerprint(
            [(1, False), (2, False)],
            ("750mL", "DOC", "Merlot", "red", ""),
            fp_index,
        )
        assert result == 2

    def test_positional_fallback(self):
        fp_index = {
            1: ("750mL", "", "", "red", ""),
            2: ("750mL", "", "", "red", ""),
        }
        result = _match_by_fingerprint(
            [(1, False), (2, False)],
            ("750mL", "", "", "red", ""),
            fp_index,
        )
        assert result == 1

    def test_empty_candidates(self):
        assert _match_by_fingerprint([], ("", "", "", "", ""), {}) is None


class TestClassifyWines:
    def test_all_existing(self):
        existing = [
            _existing_wine(1, "domaine-test-2020", category="red"),
            _existing_wine(2, "chateau-x-2019", category="red"),
        ]
        csv_rows = [
            _csv_row("Domaine Test", "", "2020"),
            _csv_row("Chateau X", "", "2019"),
        ]
        matches, deletions = classify_wines(csv_rows, existing)
        assert len(matches) == 2
        assert all(m.status == "existing" for m in matches)
        assert matches[0].wine_id == 1
        assert matches[1].wine_id == 2
        assert len(deletions) == 0

    def test_new_wine_appended(self):
        existing = [_existing_wine(1, "domaine-test-2020", category="red")]
        csv_rows = [
            _csv_row("Domaine Test", "", "2020"),
            _csv_row("New Winery", "", "2021"),
        ]
        matches, deletions = classify_wines(csv_rows, existing)
        assert matches[0].status == "existing"
        assert matches[0].wine_id == 1
        assert matches[1].status == "new"
        assert matches[1].wine_id == 2
        assert len(deletions) == 0

    def test_new_wine_inserted_middle(self):
        existing = [
            _existing_wine(100, "wine-a-2020", category="red"),
            _existing_wine(101, "wine-b-2020", category="red"),
        ]
        csv_rows = [
            _csv_row("Wine A", "", "2020"),
            _csv_row("Wine C", "", "2020"),
            _csv_row("Wine B", "", "2020"),
        ]
        matches, deletions = classify_wines(csv_rows, existing)
        assert matches[0].wine_id == 100
        assert matches[0].status == "existing"
        assert matches[1].status == "new"
        assert matches[1].wine_id == 102
        assert matches[2].wine_id == 101
        assert matches[2].status == "existing"

    def test_wine_deleted(self):
        existing = [
            _existing_wine(1, "wine-a-2020", category="red"),
            _existing_wine(2, "wine-b-2020", category="red"),
        ]
        csv_rows = [_csv_row("Wine A", "", "2020")]
        matches, deletions = classify_wines(csv_rows, existing)
        assert len(matches) == 1
        assert matches[0].wine_id == 1
        assert len(deletions) == 1
        assert deletions[0].wine_id == 2
        assert deletions[0].was_already_deleted is False

    def test_wine_revived(self):
        existing = [
            _existing_wine(5, "wine-x-2020", is_deleted=True, category="red"),
        ]
        csv_rows = [_csv_row("Wine X", "", "2020")]
        matches, deletions = classify_wines(csv_rows, existing)
        assert len(matches) == 1
        assert matches[0].status == "revived"
        assert matches[0].wine_id == 5

    def test_duplicate_slugs_fingerprint(self):
        existing = [
            _existing_wine(6, "chateau-s-2011", _raw_volume="375mL", category="white"),
            _existing_wine(7, "chateau-s-2011", _raw_volume="750mL", category="white"),
        ]
        csv_rows = [
            _csv_row("Chateau S", "", "2011", volume="750mL", category="White wine"),
            _csv_row("Chateau S", "", "2011", volume="375mL", category="White wine"),
        ]
        matches, deletions = classify_wines(csv_rows, existing)
        assert matches[0].wine_id == 7  # 750mL
        assert matches[1].wine_id == 6  # 375mL
        assert all(m.status == "existing" for m in matches)

    def test_duplicate_with_one_deleted(self):
        existing = [
            _existing_wine(6, "chateau-s-2011", is_deleted=True, category="white"),
            _existing_wine(7, "chateau-s-2011", is_deleted=False, category="white"),
        ]
        csv_rows = [_csv_row("Chateau S", "", "2011", category="White wine")]
        matches, deletions = classify_wines(csv_rows, existing)
        assert matches[0].wine_id == 7
        assert matches[0].status == "existing"

    def test_empty_existing(self):
        csv_rows = [
            _csv_row("Wine A", "", "2020"),
            _csv_row("Wine B", "", "2021"),
        ]
        matches, deletions = classify_wines(csv_rows, [])
        assert len(matches) == 2
        assert all(m.status == "new" for m in matches)
        assert matches[0].wine_id == 1
        assert matches[1].wine_id == 2

    def test_already_deleted_not_in_deletions(self):
        existing = [
            _existing_wine(1, "wine-a-2020", is_deleted=True, category="red"),
        ]
        matches, deletions = classify_wines([], existing)
        assert len(deletions) == 1
        assert deletions[0].was_already_deleted is True


class TestDetectRenames:
    def test_winery_rename(self):
        new_matches = [WineMatch(0, "new-winery-2020", 99, "new")]
        deletions = [WineDeletion(5, "old-winery-2020", False)]
        csv_rows = [_csv_row("New Winery", "", "2020")]
        existing_by_id = {5: {"winery_name": "Old Winery", "name": "", "vintage": 2020}}
        pairs = _detect_renames(new_matches, deletions, csv_rows, existing_by_id)
        assert len(pairs) == 1
        assert pairs[0][0].csv_row_index == 0
        assert pairs[0][1].wine_id == 5

    def test_name_rename(self):
        new_matches = [WineMatch(0, "domaine-new-name-2020", 99, "new")]
        deletions = [WineDeletion(5, "domaine-old-name-2020", False)]
        csv_rows = [_csv_row("Domaine", "New Name", "2020")]
        existing_by_id = {5: {"winery_name": "Domaine", "name": "Old Name", "vintage": 2020}}
        pairs = _detect_renames(new_matches, deletions, csv_rows, existing_by_id)
        assert len(pairs) == 1

    def test_both_changed_not_rename(self):
        new_matches = [WineMatch(0, "new-winery-new-name-2020", 99, "new")]
        deletions = [WineDeletion(5, "old-winery-old-name-2020", False)]
        csv_rows = [_csv_row("New Winery", "New Name", "2020")]
        existing_by_id = {5: {"winery_name": "Old Winery", "name": "Old Name", "vintage": 2020}}
        pairs = _detect_renames(new_matches, deletions, csv_rows, existing_by_id)
        assert len(pairs) == 0

    def test_year_mismatch_not_rename(self):
        new_matches = [WineMatch(0, "domaine-2021", 99, "new")]
        deletions = [WineDeletion(5, "domaine-2020", False)]
        csv_rows = [_csv_row("Domaine", "", "2021")]
        existing_by_id = {5: {"winery_name": "Domaine", "name": "", "vintage": 2020}}
        pairs = _detect_renames(new_matches, deletions, csv_rows, existing_by_id)
        assert len(pairs) == 0

    def test_empty_name_matches(self):
        new_matches = [WineMatch(0, "new-winery-2020", 99, "new")]
        deletions = [WineDeletion(5, "old-winery-2020", False)]
        csv_rows = [{"winery": "New Winery", "wine_name": "", "vintage_raw": "2020"}]
        existing_by_id = {5: {"winery_name": "Old Winery", "name": "", "vintage": 2020}}
        pairs = _detect_renames(new_matches, deletions, csv_rows, existing_by_id)
        assert len(pairs) == 1

    def test_already_deleted_skipped(self):
        new_matches = [WineMatch(0, "domaine-2020", 99, "new")]
        deletions = [WineDeletion(5, "old-name-2020", True)]
        csv_rows = [_csv_row("Domaine", "", "2020")]
        existing_by_id = {5: {"winery_name": "Old Name", "name": "", "vintage": 2020}}
        pairs = _detect_renames(new_matches, deletions, csv_rows, existing_by_id)
        assert len(pairs) == 0

    def test_nv_year_normalisation(self):
        """CSV 'Non vintage' and Parquet vintage=None both normalise to ''."""
        new_matches = [WineMatch(0, "pommery-royal-nv", 99, "new")]
        deletions = [WineDeletion(5, "champagne-pommery-royal-nv", False)]
        csv_rows = [{"winery": "Pommery", "wine_name": "Royal", "vintage_raw": "Non vintage"}]
        existing_by_id = {5: {"name": "Royal", "vintage": None}}
        pairs = _detect_renames(new_matches, deletions, csv_rows, existing_by_id)
        assert len(pairs) == 1
        assert pairs[0][1].wine_id == 5

    def test_rename_integrated_in_classify(self):
        existing = [
            _existing_wine(
                5,
                "champagne-pommery-nv",
                winery_name="Champagne Pommery",
                name="",
                is_non_vintage=True,
                category="sparkling",
            ),
        ]
        csv_rows = [_csv_row("Pommery", "", "", category="Sparkling wine")]
        matches, deletions = classify_wines(csv_rows, existing)
        assert len(matches) == 1
        assert matches[0].status == "renamed"
        assert matches[0].wine_id == 5
        assert matches[0].old_slug == "champagne-pommery-nv"
        assert len(deletions) == 0


# ---------------------------------------------------------------------------
# TestLogging
# ---------------------------------------------------------------------------


class TestLogging:
    def test_classify_wines_logs_summary(self, caplog):
        existing = [
            _existing_wine(1, "domaine-test-2020", category="red"),
        ]
        csv_rows = [
            _csv_row("Domaine Test", "", "2020"),
            _csv_row("New Winery", "", "2021"),
        ]
        with caplog.at_level("INFO", logger="cellarbrain.incremental"):
            classify_wines(csv_rows, existing)
        assert "classify_wines" in caplog.text
        assert "new" in caplog.text


# ---------------------------------------------------------------------------
# annotate_classified_wines
# ---------------------------------------------------------------------------


class TestAnnotateClassifiedWines:
    def test_new_wines_stamped_as_inserts(self):
        wines = [
            _minimal_wine(1, 1, "Alpha", 2020, run_id=0, ts=datetime(2000, 1, 1)),
            _minimal_wine(2, 1, "Beta", 2021, run_id=0, ts=datetime(2000, 1, 1)),
        ]
        matches = [
            WineMatch(csv_row_index=1, slug="alpha-2020", wine_id=1, status="new"),
            WineMatch(csv_row_index=2, slug="beta-2021", wine_id=2, status="new"),
        ]
        result, changes = annotate_classified_wines(
            wines,
            [],
            matches,
            [],
            run_id=5,
            now=_NOW,
        )
        assert len(result) == 2
        assert all(w["etl_run_id"] == 5 for w in result)
        assert all(w["updated_at"] == _NOW for w in result)
        assert all(w["is_deleted"] is False for w in result)
        assert len(changes) == 2
        assert all(c["change_type"] == "insert" for c in changes)

    def test_existing_unchanged_preserves_metadata(self):
        old = _minimal_wine(1, 1, "Alpha", 2020, run_id=3, ts=_NOW)
        new = _minimal_wine(1, 1, "Alpha", 2020, run_id=0, ts=datetime(2000, 1, 1))
        matches = [
            WineMatch(csv_row_index=1, slug="alpha-2020", wine_id=1, status="existing"),
        ]
        result, changes = annotate_classified_wines(
            [new],
            [old],
            matches,
            [],
            run_id=5,
            now=_LATER,
        )
        assert result[0]["etl_run_id"] == 3
        assert result[0]["updated_at"] == _NOW
        assert len(changes) == 0

    def test_existing_changed_updates_metadata(self):
        old = _minimal_wine(1, 1, "Alpha", 2020, alcohol_pct=13.0, run_id=3, ts=_NOW)
        new = _minimal_wine(1, 1, "Alpha", 2020, alcohol_pct=14.5, run_id=0, ts=datetime(2000, 1, 1))
        matches = [
            WineMatch(csv_row_index=1, slug="alpha-2020", wine_id=1, status="existing"),
        ]
        result, changes = annotate_classified_wines(
            [new],
            [old],
            matches,
            [],
            run_id=5,
            now=_LATER,
        )
        assert result[0]["etl_run_id"] == 5
        assert result[0]["updated_at"] == _LATER
        assert len(changes) == 1
        assert changes[0]["change_type"] == "update"
        fields = json.loads(changes[0]["changed_fields"])
        assert "alcohol_pct" in fields

    def test_revived_wine(self):
        old = _minimal_wine(1, 1, "Alpha", 2020, run_id=3, ts=_NOW)
        old["is_deleted"] = True
        new = _minimal_wine(1, 1, "Alpha", 2020, run_id=0, ts=datetime(2000, 1, 1))
        matches = [
            WineMatch(csv_row_index=1, slug="alpha-2020", wine_id=1, status="revived"),
        ]
        result, changes = annotate_classified_wines(
            [new],
            [old],
            matches,
            [],
            run_id=5,
            now=_LATER,
        )
        assert result[0]["is_deleted"] is False
        assert result[0]["etl_run_id"] == 5
        assert len(changes) == 1
        assert changes[0]["change_type"] == "update"
        fields = json.loads(changes[0]["changed_fields"])
        assert "is_deleted" in fields

    def test_renamed_wine(self):
        old = _minimal_wine(1, 1, "Alpha", 2020, run_id=3, ts=_NOW)
        new = _minimal_wine(1, 1, "Alpha Reserva", 2020, run_id=0, ts=datetime(2000, 1, 1))
        matches = [
            WineMatch(csv_row_index=1, slug="alpha-reserva-2020", wine_id=1, status="renamed", old_slug="alpha-2020"),
        ]
        result, changes = annotate_classified_wines(
            [new],
            [old],
            matches,
            [],
            run_id=5,
            now=_LATER,
        )
        assert result[0]["etl_run_id"] == 5
        assert len(changes) == 1
        assert changes[0]["change_type"] == "rename"

    def test_deleted_wines_become_tombstones(self):
        old = _minimal_wine(1, 1, "Alpha", 2020, run_id=3, ts=_NOW)
        deletions = [
            WineDeletion(wine_id=1, slug="alpha-2020", was_already_deleted=False),
        ]
        result, changes = annotate_classified_wines(
            [],
            [old],
            [],
            deletions,
            run_id=5,
            now=_LATER,
        )
        assert len(result) == 1
        assert result[0]["is_deleted"] is True
        assert result[0]["etl_run_id"] == 5
        assert len(changes) == 1
        assert changes[0]["change_type"] == "delete"

    def test_already_deleted_carried_forward(self):
        old = _minimal_wine(1, 1, "Alpha", 2020, run_id=3, ts=_NOW)
        old["is_deleted"] = True
        deletions = [
            WineDeletion(wine_id=1, slug="alpha-2020", was_already_deleted=True),
        ]
        result, changes = annotate_classified_wines(
            [],
            [old],
            [],
            deletions,
            run_id=5,
            now=_LATER,
        )
        assert len(result) == 1
        assert result[0]["is_deleted"] is True
        # Carried forward = stamped with new run_id but no change_log entry
        assert len(changes) == 0


# ---------------------------------------------------------------------------
# sync / annotate_full_load with skip_entities
# ---------------------------------------------------------------------------


class TestSkipEntities:
    def test_sync_skip_entities_passes_through(self, tmp_path):
        """Skipped entities are passed through without stabilisation."""
        wine = _minimal_wine(1, 1, "Alpha", 2020)
        entities = {
            "winery": [],
            "appellation": [],
            "grape": [],
            "cellar": [],
            "provider": [],
            "tracked_wine": [],
            "wine": [wine],
            "wine_grape": [],
            "bottle": [],
            "tasting": [],
            "pro_rating": [],
        }
        result, changes, _ = sync(
            entities,
            tmp_path,
            1,
            _NOW,
            skip_entities=frozenset({"wine"}),
        )
        # Wine should be the exact same list (not re-stabilised)
        assert result["wine"] is entities["wine"]
        # No wine-related changes from sync
        wine_changes = [c for c in changes if c["entity_type"] == "wine"]
        assert len(wine_changes) == 0

    def test_annotate_full_load_skip_entities(self, tmp_path):
        """Skipped entities are not annotated by full load."""
        wine = _minimal_wine(1, 1, "Alpha", 2020, run_id=0, ts=datetime(2000, 1, 1))
        entities = {
            "winery": [],
            "appellation": [],
            "grape": [],
            "cellar": [],
            "provider": [],
            "tracked_wine": [],
            "wine": [wine],
            "wine_grape": [],
            "bottle": [],
            "tasting": [],
            "pro_rating": [],
        }
        result, changes = annotate_full_load(
            entities,
            tmp_path,
            5,
            _LATER,
            skip_entities=frozenset({"wine"}),
        )
        # Wine metadata should NOT be overwritten
        assert result["wine"][0]["etl_run_id"] == 0
        # No wine-related changes from full load
        wine_changes = [c for c in changes if c["entity_type"] == "wine"]
        assert len(wine_changes) == 0
