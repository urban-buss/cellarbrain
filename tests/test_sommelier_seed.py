"""Tests for cellarbrain.sommelier.seed — bundled seed file utilities."""

from __future__ import annotations

from pathlib import Path

from cellarbrain.sommelier.seed import (
    bundled_food_catalogue,
    bundled_pairing_dataset,
    ensure_pairing_dataset,
)


class TestBundledPaths:
    def test_food_catalogue_exists(self):
        path = bundled_food_catalogue()
        assert path.exists(), f"Bundled food catalogue not found at {path}"
        assert path.suffix == ".parquet"

    def test_pairing_dataset_exists(self):
        path = bundled_pairing_dataset()
        assert path.exists(), f"Bundled pairing dataset not found at {path}"
        assert path.suffix == ".parquet"


class TestEnsurePairingDataset:
    def test_copies_when_missing(self, tmp_path: Path):
        target = tmp_path / "sub" / "pairings.parquet"
        assert not target.exists()
        ensure_pairing_dataset(target)
        assert target.exists()
        assert target.stat().st_size > 0

    def test_noop_when_exists(self, tmp_path: Path):
        target = tmp_path / "pairings.parquet"
        target.write_bytes(b"existing data")
        ensure_pairing_dataset(target)
        assert target.read_bytes() == b"existing data"
