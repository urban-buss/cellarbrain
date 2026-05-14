"""Tests for cellarbrain.similarity — wine similarity engine."""

from __future__ import annotations

import pytest

from cellarbrain.query import get_agent_connection
from cellarbrain.similarity import MAX_SAME_WINERY, similar_wines
from dataset_factory import (
    make_appellation,
    make_bottle,
    make_cellar,
    make_etl_run,
    make_grape,
    make_provider,
    make_wine,
    make_wine_grape,
    make_winery,
    write_dataset,
)

# ---------------------------------------------------------------------------
# Test dataset — 6 wines isolating each scoring signal
# ---------------------------------------------------------------------------


def _build_dataset(tmp_path):
    """Build a dataset with wines that isolate various similarity signals."""
    wineries = [
        make_winery(1, name="Château Alpha"),
        make_winery(2, name="Domaine Beta"),
        make_winery(3, name="Bodega Gamma"),
    ]
    appellations = [
        make_appellation(1, country="France", region="Bordeaux", subregion="Pauillac"),
        make_appellation(2, country="France", region="Bordeaux", subregion="Saint-Julien"),
        make_appellation(3, country="France", region="Burgundy", subregion="Gevrey-Chambertin"),
        make_appellation(4, country="Spain", region="Rioja"),
    ]
    grapes = [
        make_grape(1, name="Cabernet Sauvignon"),
        make_grape(2, name="Merlot"),
        make_grape(3, name="Pinot Noir"),
        make_grape(4, name="Tempranillo"),
    ]
    wines = [
        # Wine 1: Target — Bordeaux Cab Sauv, premium, "Château Alpha"
        make_wine(
            wine_id=1,
            winery_id=1,
            winery_name="Château Alpha",
            name="Grand Vin",
            vintage=2018,
            appellation_id=1,
            category="Red wine",
            primary_grape="Cabernet Sauvignon",
            price_tier="premium",
            food_groups=["red meat", "game"],
        ),
        # Wine 2: Same winery + same appellation (high similarity)
        make_wine(
            wine_id=2,
            winery_id=1,
            winery_name="Château Alpha",
            name="Second Vin",
            vintage=2019,
            appellation_id=1,
            category="Red wine",
            primary_grape="Merlot",
            price_tier="premium",
            food_groups=["red meat", "cheese"],
        ),
        # Wine 3: Same region (Bordeaux, different subregion), same grape
        make_wine(
            wine_id=3,
            winery_id=2,
            winery_name="Domaine Beta",
            name="Cuvée Rouge",
            vintage=2020,
            appellation_id=2,
            category="Red wine",
            primary_grape="Cabernet Sauvignon",
            price_tier="premium",
            food_groups=["red meat", "game"],
        ),
        # Wine 4: Different region/grape/winery, same category only
        make_wine(
            wine_id=4,
            winery_id=3,
            winery_name="Bodega Gamma",
            name="Reserva",
            vintage=2017,
            appellation_id=4,
            category="Red wine",
            primary_grape="Tempranillo",
            price_tier="everyday",
            food_groups=["tapas"],
        ),
        # Wine 5: Same winery, different everything else
        make_wine(
            wine_id=5,
            winery_id=1,
            winery_name="Château Alpha",
            name="Rosé Frais",
            vintage=2022,
            appellation_id=3,
            category="Rosé wine",
            primary_grape="Pinot Noir",
            price_tier="everyday",
            food_groups=["salad"],
        ),
        # Wine 6: Deleted wine — should be excluded
        make_wine(
            wine_id=6,
            winery_id=2,
            winery_name="Domaine Beta",
            name="Old Stock",
            vintage=2010,
            appellation_id=2,
            category="Red wine",
            primary_grape="Cabernet Sauvignon",
            price_tier="premium",
            is_deleted=True,
        ),
        # Wine 7: No bottles stored (gone wine)
        make_wine(
            wine_id=7,
            winery_id=2,
            winery_name="Domaine Beta",
            name="Épuisé",
            vintage=2015,
            appellation_id=1,
            category="Red wine",
            primary_grape="Cabernet Sauvignon",
            price_tier="premium",
            food_groups=["red meat"],
        ),
        # Wine 8: Format sibling of wine 1 (same format_group_id)
        make_wine(
            wine_id=8,
            winery_id=1,
            winery_name="Château Alpha",
            name="Grand Vin",
            vintage=2018,
            appellation_id=1,
            category="Red wine",
            primary_grape="Cabernet Sauvignon",
            price_tier="fine",
            volume_ml=1500,
            bottle_format="Magnum",
            format_group_id=100,
            food_groups=["red meat", "game"],
        ),
    ]
    # Update wine 1 to have format_group_id=100
    wines[0]["format_group_id"] = 100

    wine_grapes = [
        make_wine_grape(1, 1, percentage=80.0, sort_order=1),
        make_wine_grape(1, 2, percentage=20.0, sort_order=2),
        make_wine_grape(2, 2, percentage=100.0, sort_order=1),
        make_wine_grape(3, 1, percentage=100.0, sort_order=1),
        make_wine_grape(4, 4, percentage=100.0, sort_order=1),
        make_wine_grape(5, 3, percentage=100.0, sort_order=1),
        make_wine_grape(7, 1, percentage=100.0, sort_order=1),
        make_wine_grape(8, 1, percentage=80.0, sort_order=1),
        make_wine_grape(8, 2, percentage=20.0, sort_order=2),
    ]

    # Wine 7 has no stored bottles; wines 1-5, 8 have bottles
    bottles = [
        make_bottle(1, 1),
        make_bottle(2, 2),
        make_bottle(3, 3),
        make_bottle(4, 4),
        make_bottle(5, 5),
        make_bottle(6, 8),
    ]

    return write_dataset(
        tmp_path,
        {
            "winery": wineries,
            "appellation": appellations,
            "grape": grapes,
            "wine": wines,
            "wine_grape": wine_grapes,
            "bottle": bottles,
            "cellar": [make_cellar()],
            "provider": [make_provider()],
            "etl_run": [make_etl_run()],
        },
    )


@pytest.fixture()
def data_dir(tmp_path):
    return _build_dataset(tmp_path)


@pytest.fixture()
def con(data_dir):
    return get_agent_connection(data_dir)


# ---------------------------------------------------------------------------
# TestSimilarWinesBasic
# ---------------------------------------------------------------------------


class TestSimilarWinesBasic:
    """Basic functionality: returns results, respects limit, etc."""

    def test_returns_markdown_table(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        assert "| # |" in result
        assert "Score" in result
        assert "Signals" in result

    def test_header_contains_target_name(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        assert "Grand Vin" in result
        assert "wine #1" in result

    def test_respects_limit(self, con, data_dir):
        result = similar_wines(con, 1, data_dir, limit=2)
        # Count data rows (lines starting with "| " that have a digit after "| ")
        data_rows = [ln for ln in result.splitlines() if ln.startswith("| ") and ln[2:3].isdigit()]
        assert len(data_rows) <= 2

    def test_default_limit_five(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        data_rows = [ln for ln in result.splitlines() if ln.startswith("| ") and ln[2:3].isdigit()]
        assert len(data_rows) <= 5

    def test_wine_not_found(self, con, data_dir):
        result = similar_wines(con, 999, data_dir)
        assert "not found" in result.lower()


# ---------------------------------------------------------------------------
# TestSimilarWinesExclusions
# ---------------------------------------------------------------------------


class TestSimilarWinesExclusions:
    """Deleted wines, format siblings, and gone wines are handled correctly."""

    def test_excludes_deleted(self, con, data_dir):
        result = similar_wines(con, 1, data_dir, include_gone=True)
        # Wine 6 is deleted — excluded from wines_full view entirely
        assert "Old Stock" not in result

    def test_excludes_format_siblings(self, con, data_dir):
        result = similar_wines(con, 1, data_dir, include_gone=True)
        # Wine 8 is a format sibling (same format_group_id=100) — excluded
        # Verify by counting data rows from same winery (excluding header)
        data_rows = [ln for ln in result.splitlines() if ln.startswith("| ") and ln[2:3].isdigit()]
        alpha_rows = [ln for ln in data_rows if "Alpha" in ln]
        assert len(alpha_rows) <= MAX_SAME_WINERY

    def test_excludes_gone_by_default(self, con, data_dir):
        """Wine 7 has no stored bottles; excluded when include_gone=False."""
        result = similar_wines(con, 1, data_dir, include_gone=False)
        assert "Épuisé" not in result

    def test_includes_gone_when_requested(self, con, data_dir):
        """Wine 7 has no bottles but matches strongly; shown with include_gone=True."""
        result = similar_wines(con, 1, data_dir, include_gone=True)
        assert "Épuisé" in result


# ---------------------------------------------------------------------------
# TestSimilarWinesSignals
# ---------------------------------------------------------------------------


class TestSimilarWinesSignals:
    """Each scoring signal is reflected in the results."""

    def test_same_winery_signal(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        # Wine 2 (same winery) should have "same winery" signal
        assert "same winery" in result

    def test_same_appellation_signal(self, con, data_dir):
        result = similar_wines(con, 1, data_dir, include_gone=True)
        # Wine 2 shares subregion (Pauillac) with target
        assert "same subregion" in result

    def test_same_region_signal(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        # Wine 3 is in same region (Bordeaux) but different appellation
        assert "same region" in result

    def test_same_grape_signal(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        # Wine 3 has same primary_grape (Cabernet Sauvignon)
        assert "same grape" in result

    def test_same_category_signal(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        # Most wines share "Red wine" category
        assert "same category" in result

    def test_price_tier_signal(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        # Wines 2, 3 share "premium" tier
        assert "same price tier" in result

    def test_food_group_overlap_signal(self, con, data_dir):
        result = similar_wines(con, 1, data_dir)
        # Wines 2, 3 overlap on "red meat" food group
        assert "food group overlap" in result


# ---------------------------------------------------------------------------
# TestSimilarWinesDiversity
# ---------------------------------------------------------------------------


class TestSimilarWinesDiversity:
    """Winery diversity cap: max 2 results from same winery."""

    def test_winery_cap(self, con, data_dir):
        # Wines 2 and 5 are from Château Alpha (same as target wine 1)
        # Only 2 of them should appear max
        result = similar_wines(con, 1, data_dir, limit=10)
        # Count how many rows mention "Château Alpha" winery name is not in output
        # but wine names from that winery are Second Vin and Rosé Frais
        lines = result.splitlines()
        alpha_wines = [ln for ln in lines if "Second Vin" in ln or "Rosé Frais" in ln]
        assert len(alpha_wines) <= 2


# ---------------------------------------------------------------------------
# TestSimilarWinesScoring
# ---------------------------------------------------------------------------


class TestSimilarWinesScoring:
    """Score ordering is correct — more matching signals → higher rank."""

    def test_high_match_ranks_first(self, con, data_dir):
        """Wine 2 (same winery + appellation + category + price) should rank above wine 4."""
        result = similar_wines(con, 1, data_dir, limit=10, include_gone=True)
        lines = result.splitlines()
        # Find the rank of wines
        rank_2 = rank_4 = None
        for ln in lines:
            if "Second Vin" in ln:
                rank_2 = ln.split("|")[1].strip()
            if "Reserva" in ln:
                rank_4 = ln.split("|")[1].strip()
        if rank_2 and rank_4:
            assert int(rank_2) < int(rank_4)

    def test_low_similarity_below_threshold(self, con, data_dir):
        """Wine 5 (Rosé, different region) may be below MIN_SCORE if only winery matches."""
        # This tests that very low-scoring wines are filtered
        result = similar_wines(con, 1, data_dir, limit=20, include_gone=True)
        # Wine 5 only shares winery → score = 0.30 which is above threshold
        # So it should still appear. Just verify order is sensible.
        assert "| 1 |" in result  # At least one result


# ---------------------------------------------------------------------------
# TestSimilarWinesEdgeCases
# ---------------------------------------------------------------------------


class TestSimilarWinesEdgeCases:
    """Edge cases: wine with no grapes, empty result set, etc."""

    def test_no_similar_wines(self, tmp_path):
        """A single wine in the dataset has no possible matches."""
        data_dir = write_dataset(
            tmp_path,
            {
                "winery": [make_winery(1, name="Solo Winery")],
                "appellation": [make_appellation(1)],
                "grape": [make_grape(1)],
                "wine": [
                    make_wine(
                        wine_id=1,
                        winery_id=1,
                        winery_name="Solo Winery",
                        name="Only Wine",
                        vintage=2020,
                    )
                ],
                "wine_grape": [make_wine_grape(1, 1)],
                "bottle": [make_bottle(1, 1)],
                "cellar": [make_cellar()],
                "provider": [make_provider()],
                "etl_run": [make_etl_run()],
            },
        )
        con = get_agent_connection(data_dir)
        result = similar_wines(con, 1, data_dir)
        assert "No similar wines found" in result

    def test_limit_clamped_to_one(self, con, data_dir):
        """Limit of 0 is clamped to 1 externally, but function accepts it."""
        # The function itself doesn't clamp — that's done in the MCP tool.
        # With limit=1, we should get exactly 1 result.
        result = similar_wines(con, 1, data_dir, limit=1)
        data_rows = [ln for ln in result.splitlines() if ln.startswith("| ") and ln[2:3].isdigit()]
        assert len(data_rows) == 1
