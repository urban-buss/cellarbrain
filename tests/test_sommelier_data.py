"""Validation tests for sommelier data artefacts (food catalogue & pairing dataset).

All tests are gated with skipif — they pass as 'skipped' when data files
have not been generated yet.
"""

from __future__ import annotations

import re
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from cellarbrain.sommelier.schemas import (
    COOKING_METHODS,
    FOOD_CATALOGUE_SCHEMA,
    PAIRING_DATASET_SCHEMA,
    PROTEINS,
    WEIGHT_CLASSES,
)
from cellarbrain.sommelier.text_builder import build_food_text

DATA_DIR = Path(__file__).resolve().parent.parent / "models" / "sommelier"

_has_catalogue = (DATA_DIR / "food_catalogue.parquet").exists()
_has_pairings = (DATA_DIR / "pairing_dataset.parquet").exists()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def catalogue():
    if not _has_catalogue:
        pytest.skip("food_catalogue.parquet not built yet")
    return pq.read_table(DATA_DIR / "food_catalogue.parquet")


@pytest.fixture(scope="module")
def pairings():
    if not _has_pairings:
        pytest.skip("pairing_dataset.parquet not built yet")
    return pq.read_table(DATA_DIR / "pairing_dataset.parquet")


# ---------------------------------------------------------------------------
# TestFoodCatalogueSchema
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_catalogue, reason="food_catalogue.parquet not built yet")
class TestFoodCatalogueSchema:
    def test_schema_columns(self, catalogue):
        expected = {f.name for f in FOOD_CATALOGUE_SCHEMA}
        actual = set(catalogue.schema.names)
        assert expected == actual

    def test_no_null_required_columns(self, catalogue):
        for field in FOOD_CATALOGUE_SCHEMA:
            if not field.nullable:
                col = catalogue.column(field.name)
                assert col.null_count == 0, f"{field.name} has nulls"

    def test_dish_id_unique(self, catalogue):
        ids = catalogue.column("dish_id").to_pylist()
        assert len(ids) == len(set(ids)), "Duplicate dish_id found"

    def test_dish_id_format(self, catalogue):
        slug_re = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
        for dish_id in catalogue.column("dish_id").to_pylist():
            assert slug_re.match(dish_id), f"Bad slug: {dish_id}"

    def test_weight_class_enum(self, catalogue):
        values = set(catalogue.column("weight_class").to_pylist())
        assert values <= WEIGHT_CLASSES, f"Invalid weight_class: {values - WEIGHT_CLASSES}"

    def test_protein_enum(self, catalogue):
        values = {v for v in catalogue.column("protein").to_pylist() if v is not None}
        assert values <= PROTEINS, f"Invalid protein: {values - PROTEINS}"

    def test_cooking_method_enum(self, catalogue):
        values = set(catalogue.column("cooking_method").to_pylist())
        assert values <= COOKING_METHODS, f"Invalid cooking_method: {values - COOKING_METHODS}"

    def test_ingredients_not_empty(self, catalogue):
        for i, row in enumerate(catalogue.column("ingredients").to_pylist()):
            assert len(row) >= 1, f"Row {i} has empty ingredients"

    def test_flavour_profile_not_empty(self, catalogue):
        for i, row in enumerate(catalogue.column("flavour_profile").to_pylist()):
            assert len(row) >= 1, f"Row {i} has empty flavour_profile"

    def test_description_length(self, catalogue):
        for i, desc in enumerate(catalogue.column("description").to_pylist()):
            assert 10 <= len(desc) <= 300, f"Row {i} description length {len(desc)}"


# ---------------------------------------------------------------------------
# TestFoodCatalogueCoverage
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_catalogue, reason="food_catalogue.parquet not built yet")
class TestFoodCatalogueCoverage:
    def test_minimum_dish_count(self, catalogue):
        assert len(catalogue) >= 1500

    def test_cuisine_diversity(self, catalogue):
        cuisines = set(catalogue.column("cuisine").to_pylist())
        assert len(cuisines) >= 25, f"Only {len(cuisines)} cuisines"

    def test_weight_distribution(self, catalogue):
        total = len(catalogue)
        for wc in WEIGHT_CLASSES:
            count = sum(1 for v in catalogue.column("weight_class").to_pylist() if v == wc)
            pct = count / total
            assert pct >= 0.10, f"{wc} is only {pct:.1%} of dishes"

    def test_protein_diversity(self, catalogue):
        values = {v for v in catalogue.column("protein").to_pylist() if v is not None}
        assert len(values) >= 6, f"Only {len(values)} protein types"

    def test_cooking_method_diversity(self, catalogue):
        values = set(catalogue.column("cooking_method").to_pylist())
        assert len(values) >= 8, f"Only {len(values)} cooking methods"

    def test_swiss_cuisine_present(self, catalogue):
        count = sum(1 for v in catalogue.column("cuisine").to_pylist() if v == "Swiss")
        assert count >= 30, f"Only {count} Swiss dishes"

    def test_french_cuisine_present(self, catalogue):
        count = sum(1 for v in catalogue.column("cuisine").to_pylist() if v == "French")
        assert count >= 80, f"Only {count} French dishes"

    def test_italian_cuisine_present(self, catalogue):
        count = sum(1 for v in catalogue.column("cuisine").to_pylist() if v == "Italian")
        assert count >= 80, f"Only {count} Italian dishes"

    def test_game_protein_coverage(self, catalogue):
        count = sum(1 for v in catalogue.column("protein").to_pylist() if v == "game")
        assert count >= 14, f"Only {count} game dishes, need at least 14"

    def test_cuisine_label_consistency(self, catalogue):
        """Verify well-known dishes carry the correct cuisine label."""
        expected_cuisines = {
            "satay-chicken": "Malaysian",
            "laksa": "Malaysian",
            "rendang": "Indonesian",
            "nasi-goreng": "Indonesian",
            "gado-gado": "Indonesian",
            "empanadas": "Argentine",
            "asado": "Argentine",
            "feijoada": "Brazilian",
            "acaraje": "Brazilian",
            "pao-de-queijo": "Brazilian",
            "arepa-de-queso": "Venezuelan",
            "pupusa": "Central American",
        }
        ids = catalogue.column("dish_id").to_pylist()
        cuisines = catalogue.column("cuisine").to_pylist()
        dish_to_cuisine = dict(zip(ids, cuisines))
        for dish_id, expected in expected_cuisines.items():
            if dish_id in dish_to_cuisine:
                assert dish_to_cuisine[dish_id] == expected, (
                    f"{dish_id}: expected {expected}, got {dish_to_cuisine[dish_id]}"
                )


# ---------------------------------------------------------------------------
# TestPairingDatasetSchema
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_pairings, reason="pairing_dataset.parquet not built yet")
class TestPairingDatasetSchema:
    def test_schema_columns(self, pairings):
        expected = {f.name for f in PAIRING_DATASET_SCHEMA}
        actual = set(pairings.schema.names)
        assert expected == actual

    def test_no_nulls(self, pairings):
        for name in pairings.schema.names:
            col = pairings.column(name)
            assert col.null_count == 0, f"{name} has nulls"

    def test_score_range(self, pairings):
        scores = pairings.column("pairing_score").to_pylist()
        assert all(0.0 <= s <= 1.0 for s in scores), "Score out of range"

    def test_no_duplicate_pairs(self, pairings):
        food = pairings.column("food_text").to_pylist()
        wine = pairings.column("wine_text").to_pylist()
        pairs = list(zip(food, wine))
        assert len(pairs) == len(set(pairs)), "Duplicate pairs found"

    def test_food_text_not_empty(self, pairings):
        for ft in pairings.column("food_text").to_pylist():
            assert len(ft) > 10

    def test_wine_text_not_empty(self, pairings):
        for wt in pairings.column("wine_text").to_pylist():
            assert len(wt) > 10

    def test_pairing_reason_not_empty(self, pairings):
        for reason in pairings.column("pairing_reason").to_pylist():
            assert len(reason) > 10


# ---------------------------------------------------------------------------
# TestPairingDatasetDistribution
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_pairings, reason="pairing_dataset.parquet not built yet")
class TestPairingDatasetDistribution:
    def test_minimum_pair_count(self, pairings):
        assert len(pairings) >= 8000

    def _score_pct(self, pairings, lo, hi):
        scores = pairings.column("pairing_score").to_pylist()
        count = sum(1 for s in scores if lo <= s < hi)
        return count / len(scores)

    def test_score_distribution_excellent(self, pairings):
        pct = self._score_pct(pairings, 0.8, 1.01)
        assert 0.10 <= pct <= 0.30, f"Excellent: {pct:.1%}"

    def test_score_distribution_good(self, pairings):
        pct = self._score_pct(pairings, 0.6, 0.8)
        assert 0.10 <= pct <= 0.30, f"Good: {pct:.1%}"

    def test_score_distribution_mediocre(self, pairings):
        pct = self._score_pct(pairings, 0.4, 0.6)
        assert 0.10 <= pct <= 0.30, f"Mediocre: {pct:.1%}"

    def test_score_distribution_poor(self, pairings):
        pct = self._score_pct(pairings, 0.2, 0.4)
        assert 0.10 <= pct <= 0.30, f"Poor: {pct:.1%}"

    def test_score_distribution_bad(self, pairings):
        pct = self._score_pct(pairings, 0.0, 0.2)
        assert 0.10 <= pct <= 0.30, f"Bad: {pct:.1%}"

    def test_grape_diversity(self, pairings):
        grapes = set(pairings.column("grape").to_pylist())
        assert len(grapes) >= 20, f"Only {len(grapes)} grapes"

    def test_region_diversity(self, pairings):
        regions = set(pairings.column("region").to_pylist())
        assert len(regions) >= 15, f"Only {len(regions)} regions"

    def test_style_diversity(self, pairings):
        styles = set(pairings.column("style").to_pylist())
        assert len(styles) >= 8, f"Only {len(styles)} styles"

    def test_ingredients_not_empty(self, pairings):
        for i, row in enumerate(pairings.column("ingredients").to_pylist()):
            assert len(row) >= 1, f"Row {i} has empty ingredients"

    def test_reason_score_coherence(self, pairings):
        """Verify reason text sentiment aligns with score band.

        Excellent pairs (>= 0.8) should never contain negative language;
        bad pairs (< 0.2) should never contain positive language.
        """
        positive_re = re.compile(r"\b(beautifully|excellent|perfectly|harmonises|wonderful)\b", re.IGNORECASE)
        negative_re = re.compile(r"\b(clashes|metallic|mismatch|detracts|vanishes)\b", re.IGNORECASE)
        # "overwhelms" excluded — used in positive context ("rather than overwhelms")
        scores = pairings.column("pairing_score").to_pylist()
        reasons = pairings.column("pairing_reason").to_pylist()
        bad_high, bad_low = 0, 0
        for score, reason in zip(scores, reasons):
            if score >= 0.8 and negative_re.search(reason):
                bad_high += 1
            if score < 0.2 and positive_re.search(reason):
                bad_low += 1
        total = len(scores)
        # Allow small tolerance (< 1%) for edge cases
        assert bad_high / total < 0.01, f"{bad_high} excellent pairs have negative language"
        assert bad_low / total < 0.01, f"{bad_low} bad pairs have positive language"


# ---------------------------------------------------------------------------
# TestBuildFoodTextIntegration
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_catalogue, reason="food_catalogue.parquet not built yet")
class TestBuildFoodTextIntegration:
    def test_round_trip_from_catalogue(self, catalogue):
        table = catalogue.to_pandas()
        sample = table.sample(n=min(10, len(table)), random_state=42)
        for _, row in sample.iterrows():
            result = build_food_text(
                dish_name=row["dish_name"],
                description=row["description"],
                ingredients=row["ingredients"],
                cuisine=row["cuisine"],
                weight_class=row["weight_class"],
                protein=row["protein"],
                flavour_profile=row["flavour_profile"],
            )
            assert row["dish_name"] in result
            assert len(result) > 20
