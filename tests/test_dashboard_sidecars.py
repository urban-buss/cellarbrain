"""Tests for the dashboard sidecar JSON helpers."""

from __future__ import annotations

import json
import pathlib

from cellarbrain import query as q
from cellarbrain.dashboard import sidecars
from dataset_factory import (
    make_bottle,
    make_cellar,
    make_wine,
    make_winery,
    write_dataset,
)

# ---------------------------------------------------------------------------
# consumed-pending — basic CRUD
# ---------------------------------------------------------------------------


class TestConsumedPending:
    def test_read_missing_file_returns_empty(self, tmp_path: pathlib.Path) -> None:
        assert sidecars.read_consumed_pending(tmp_path) == []

    def test_add_then_read(self, tmp_path: pathlib.Path) -> None:
        items = sidecars.add_consumed_pending(tmp_path, bottle_id=42, wine_id=7, note="opened")
        assert len(items) == 1
        assert items[0]["bottle_id"] == 42
        assert items[0]["wine_id"] == 7
        assert items[0]["note"] == "opened"
        assert "marked_at" in items[0]

        on_disk = sidecars.read_consumed_pending(tmp_path)
        assert on_disk == items

    def test_add_is_idempotent(self, tmp_path: pathlib.Path) -> None:
        sidecars.add_consumed_pending(tmp_path, bottle_id=1, wine_id=2)
        sidecars.add_consumed_pending(tmp_path, bottle_id=1, wine_id=2)
        assert len(sidecars.read_consumed_pending(tmp_path)) == 1

    def test_remove(self, tmp_path: pathlib.Path) -> None:
        sidecars.add_consumed_pending(tmp_path, bottle_id=1, wine_id=2)
        sidecars.add_consumed_pending(tmp_path, bottle_id=3, wine_id=4)
        remaining = sidecars.remove_consumed_pending(tmp_path, bottle_id=1)
        assert {it["bottle_id"] for it in remaining} == {3}

    def test_remove_missing_is_noop(self, tmp_path: pathlib.Path) -> None:
        result = sidecars.remove_consumed_pending(tmp_path, bottle_id=999)
        assert result == []

    def test_malformed_file_returns_empty(self, tmp_path: pathlib.Path) -> None:
        sidecars.consumed_pending_path(tmp_path).write_text("not json", encoding="utf-8")
        assert sidecars.read_consumed_pending(tmp_path) == []

    def test_atomic_write_no_temp_leftover(self, tmp_path: pathlib.Path) -> None:
        sidecars.add_consumed_pending(tmp_path, bottle_id=1, wine_id=2)
        leftovers = list(tmp_path.glob(".cb-sidecar-*"))
        assert leftovers == []


# ---------------------------------------------------------------------------
# drink-tonight — basic CRUD
# ---------------------------------------------------------------------------


class TestDrinkTonight:
    def test_read_missing_file_returns_empty(self, tmp_path: pathlib.Path) -> None:
        assert sidecars.read_drink_tonight(tmp_path) == []

    def test_add_then_read(self, tmp_path: pathlib.Path) -> None:
        items = sidecars.add_drink_tonight(tmp_path, wine_id=10, note="for dinner")
        assert items[0]["wine_id"] == 10
        assert items[0]["note"] == "for dinner"
        assert "added_at" in items[0]

    def test_add_is_idempotent(self, tmp_path: pathlib.Path) -> None:
        sidecars.add_drink_tonight(tmp_path, wine_id=10)
        sidecars.add_drink_tonight(tmp_path, wine_id=10)
        assert len(sidecars.read_drink_tonight(tmp_path)) == 1

    def test_remove(self, tmp_path: pathlib.Path) -> None:
        sidecars.add_drink_tonight(tmp_path, wine_id=10)
        sidecars.add_drink_tonight(tmp_path, wine_id=20)
        remaining = sidecars.remove_drink_tonight(tmp_path, wine_id=10)
        assert {it["wine_id"] for it in remaining} == {20}

    def test_write_dedupes_and_normalises(self, tmp_path: pathlib.Path) -> None:
        items = sidecars.write_drink_tonight(
            tmp_path,
            [
                {"wine_id": "5", "note": "a"},
                {"wine_id": 5, "note": "duplicate"},
                {"not_a_wine": True},
                {"wine_id": "bogus"},
                {"wine_id": 6, "added_at": "2024-01-01T00:00:00+00:00"},
            ],
        )
        assert [it["wine_id"] for it in items] == [5, 6]
        assert items[1]["added_at"] == "2024-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Prune after ETL
# ---------------------------------------------------------------------------


def _build_minimal_dataset(tmp_path: pathlib.Path, bottle_status: dict[int, str]) -> pathlib.Path:
    """Create a dataset with one wine and bottles whose status is set per id."""
    wines = [make_wine(wine_id=1)]
    wineries = [make_winery(winery_id=1)]
    bottles = [make_bottle(bottle_id=bid, wine_id=1, status=status) for bid, status in bottle_status.items()]
    return write_dataset(
        tmp_path,
        {"wine": wines, "winery": wineries, "bottle": bottles},
    )


class TestPruneAfterEtl:
    def test_empty_sidecar_no_op(self, tmp_path: pathlib.Path) -> None:
        _build_minimal_dataset(tmp_path, {1: "stored"})
        con = q.get_agent_connection(str(tmp_path))
        try:
            assert sidecars.prune_consumed_after_etl(tmp_path, con) == []
        finally:
            con.close()

    def test_prunes_consumed_bottles(self, tmp_path: pathlib.Path) -> None:
        _build_minimal_dataset(tmp_path, {1: "stored", 2: "drunk"})
        sidecars.add_consumed_pending(tmp_path, bottle_id=1, wine_id=1)
        sidecars.add_consumed_pending(tmp_path, bottle_id=2, wine_id=1)

        con = q.get_agent_connection(str(tmp_path))
        try:
            pruned = sidecars.prune_consumed_after_etl(tmp_path, con)
        finally:
            con.close()

        assert pruned == [2]
        remaining = sidecars.read_consumed_pending(tmp_path)
        assert {it["bottle_id"] for it in remaining} == {1}

    def test_prunes_missing_bottles(self, tmp_path: pathlib.Path) -> None:
        _build_minimal_dataset(tmp_path, {1: "stored"})
        sidecars.add_consumed_pending(tmp_path, bottle_id=999, wine_id=1)

        con = q.get_agent_connection(str(tmp_path))
        try:
            pruned = sidecars.prune_consumed_after_etl(tmp_path, con)
        finally:
            con.close()

        assert pruned == [999]
        assert sidecars.read_consumed_pending(tmp_path) == []

    def test_keeps_in_transit(self, tmp_path: pathlib.Path) -> None:
        # bottle is stored but in transit → should be considered "still pending"
        wines = [make_wine(wine_id=1)]
        wineries = [make_winery(winery_id=1)]
        cellars = [make_cellar(cellar_id=1, location_type="in_transit")]
        bottles = [make_bottle(bottle_id=1, wine_id=1, status="stored")]
        write_dataset(tmp_path, {"wine": wines, "winery": wineries, "cellar": cellars, "bottle": bottles})
        sidecars.add_consumed_pending(tmp_path, bottle_id=1, wine_id=1)

        con = q.get_agent_connection(str(tmp_path))
        try:
            pruned = sidecars.prune_consumed_after_etl(tmp_path, con)
        finally:
            con.close()

        # in-transit bottles can't be "consumed" → they get pruned (not stored)
        assert pruned == [1]


# ---------------------------------------------------------------------------
# JSON shape
# ---------------------------------------------------------------------------


class TestSidecarFormat:
    def test_consumed_pending_has_items_and_updated_at(self, tmp_path: pathlib.Path) -> None:
        sidecars.add_consumed_pending(tmp_path, bottle_id=1, wine_id=2)
        payload = json.loads(sidecars.consumed_pending_path(tmp_path).read_text(encoding="utf-8"))
        assert "items" in payload
        assert "updated_at" in payload

    def test_drink_tonight_has_items_and_updated_at(self, tmp_path: pathlib.Path) -> None:
        sidecars.add_drink_tonight(tmp_path, wine_id=1)
        payload = json.loads(sidecars.drink_tonight_path(tmp_path).read_text(encoding="utf-8"))
        assert "items" in payload
        assert "updated_at" in payload
