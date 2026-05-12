"""Tests for tests/smoke_helpers/verify.py — dossier integrity checks."""

from __future__ import annotations

from pathlib import Path

from dataset_factory import (
    make_bottle,
    make_cellar,
    make_wine,
    write_dataset,
)
from smoke_helpers.verify import check_dossier_integrity


def _create_dossier(output_dir: Path, wine_id: int, subdir: str = "cellar") -> None:
    """Create a minimal dossier markdown file."""
    wines_dir = output_dir / "wines" / subdir
    wines_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{wine_id:04d}-test-winery-test-wine-2020.md"
    (wines_dir / filename).write_text(f"---\nwine_id: {wine_id}\n---\n# Test Wine\n", encoding="utf-8")


class TestCheckDossierIntegrity:
    """Regression tests for check_dossier_integrity."""

    def test_passes_with_cellar_parquet(self, tmp_path: Path) -> None:
        """The check must not fail when cellar.parquet exists (issue #008)."""
        wine = make_wine(wine_id=1)
        bottle = make_bottle(bottle_id=1, wine_id=1, status="stored", cellar_id=1)
        cellar = make_cellar(cellar_id=1, location_type="onsite")

        write_dataset(
            tmp_path,
            {
                "wine": [wine],
                "bottle": [bottle],
                "cellar": [cellar],
            },
        )
        _create_dossier(tmp_path, wine_id=1, subdir="cellar")

        results = check_dossier_integrity(tmp_path)
        # All checks should pass — no "Table with name cellar does not exist" error
        failed = [r for r in results if not r.passed]
        assert not failed, f"Unexpected failures: {[r.name + ': ' + r.details for r in failed]}"

    def test_passes_without_cellar_parquet(self, tmp_path: Path) -> None:
        """The check must degrade gracefully when cellar.parquet is missing."""
        wine = make_wine(wine_id=1)
        bottle = make_bottle(bottle_id=1, wine_id=1, status="stored", cellar_id=1)

        write_dataset(
            tmp_path,
            {
                "wine": [wine],
                "bottle": [bottle],
                "cellar": [],  # write_dataset writes empty table
            },
        )
        # Remove cellar.parquet to simulate it being absent
        (tmp_path / "cellar.parquet").unlink()

        _create_dossier(tmp_path, wine_id=1, subdir="cellar")

        results = check_dossier_integrity(tmp_path)
        # Should not crash — falls back to simpler query
        failed = [r for r in results if not r.passed]
        assert not failed, f"Unexpected failures: {[r.name + ': ' + r.details for r in failed]}"

    def test_routing_detects_misrouted_wine(self, tmp_path: Path) -> None:
        """Wine with stored bottles in archive/ should be flagged."""
        wine = make_wine(wine_id=1)
        bottle = make_bottle(bottle_id=1, wine_id=1, status="stored", cellar_id=1)
        cellar = make_cellar(cellar_id=1, location_type="onsite")

        write_dataset(
            tmp_path,
            {
                "wine": [wine],
                "bottle": [bottle],
                "cellar": [cellar],
            },
        )
        # Put dossier in archive instead of cellar — misrouted
        _create_dossier(tmp_path, wine_id=1, subdir="archive")

        results = check_dossier_integrity(tmp_path)
        routing_check = next(r for r in results if "routing" in r.name.lower())
        assert not routing_check.passed

    def test_in_transit_bottles_not_counted_as_stored(self, tmp_path: Path) -> None:
        """Bottles in an in_transit cellar should not count as stored."""
        wine = make_wine(wine_id=1)
        bottle = make_bottle(bottle_id=1, wine_id=1, status="stored", cellar_id=2)
        cellar = make_cellar(cellar_id=2, location_type="in_transit")

        write_dataset(
            tmp_path,
            {
                "wine": [wine],
                "bottle": [bottle],
                "cellar": [cellar],
            },
        )
        # Wine is in archive — correct because its only bottle is in_transit
        _create_dossier(tmp_path, wine_id=1, subdir="archive")

        results = check_dossier_integrity(tmp_path)
        routing_check = next(r for r in results if "routing" in r.name.lower())
        assert routing_check.passed
