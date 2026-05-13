"""Tests for cellarbrain.sommelier.seed — bundled file helpers."""

from __future__ import annotations

import pathlib

from cellarbrain.sommelier.seed import (
    bundled_food_catalogue,
    bundled_pairing_dataset,
    ensure_pairing_dataset,
)


class TestBundledPaths:
    def test_bundled_food_catalogue_returns_absolute(self):
        path = bundled_food_catalogue()
        assert path.is_absolute()
        assert path.name == "food_catalogue.parquet"

    def test_bundled_pairing_dataset_returns_absolute(self):
        path = bundled_pairing_dataset()
        assert path.is_absolute()
        assert path.name == "pairing_dataset.parquet"

    def test_bundled_food_catalogue_exists(self):
        assert bundled_food_catalogue().exists()

    def test_bundled_pairing_dataset_exists(self):
        assert bundled_pairing_dataset().exists()


class TestEnsurePairingDataset:
    def test_copies_seed_when_target_missing(self, tmp_path):
        target = tmp_path / "models" / "sommelier" / "pairing_dataset.parquet"
        assert not target.exists()

        ensure_pairing_dataset(target)

        assert target.exists()
        assert target.stat().st_size > 0

    def test_noop_when_target_exists(self, tmp_path):
        target = tmp_path / "pairing_dataset.parquet"
        target.write_text("existing data")
        original_content = target.read_text()

        ensure_pairing_dataset(target)

        assert target.read_text() == original_content

    def test_creates_parent_directories(self, tmp_path):
        target = tmp_path / "deep" / "nested" / "dir" / "pairing_dataset.parquet"
        ensure_pairing_dataset(target)
        assert target.exists()


class TestSeedPathResolutionIntegration:
    def test_food_catalogue_resolvable_from_nonrepo_cwd(self, tmp_path, monkeypatch):
        """food_catalogue resolves even when CWD is not the repo root."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)

        from cellarbrain.settings import load_settings

        s = load_settings()
        cat_path = pathlib.Path(s.sommelier.food_catalogue)
        assert cat_path.is_absolute()
        assert cat_path.exists(), f"food_catalogue not found at {cat_path}"

    def test_pairing_dataset_auto_seeded_from_nonrepo_cwd(self, tmp_path, monkeypatch):
        """pairing_dataset can be auto-seeded to data_dir from any CWD."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CELLARBRAIN_CONFIG", raising=False)
        monkeypatch.delenv("CELLARBRAIN_DATA_DIR", raising=False)

        from cellarbrain.settings import load_settings

        s = load_settings()
        target = pathlib.Path(s.sommelier.pairing_dataset)
        assert not target.exists()

        ensure_pairing_dataset(target)
        assert target.exists()
