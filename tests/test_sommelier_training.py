"""Tests for sommelier training, indexing, and engine.

All tests that need sentence-transformers or faiss are guarded with
``pytest.importorskip`` so the regular test suite skips them cleanly.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Skip markers for optional deps
# ---------------------------------------------------------------------------


def _require_sommelier():
    pytest.importorskip("sentence_transformers")
    pytest.importorskip("faiss")


BASE_MODEL = "models/sommelier/base-model"


def _require_base_model():
    """Skip if the local base model is not present."""
    _require_sommelier()
    import pathlib

    if not pathlib.Path(BASE_MODEL).exists():
        pytest.skip(f"Base model not found at {BASE_MODEL}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tiny_pairing_parquet(tmp_path):
    """Write 20 fake pairing rows to a Parquet file."""
    food_texts = [f"dish {i} with ingredients" for i in range(20)]
    wine_texts = [f"wine {i} from region {i % 5}" for i in range(20)]
    scores = [round(0.05 + (i / 20) * 0.9, 3) for i in range(20)]
    table = pa.table(
        {
            "food_text": food_texts,
            "wine_text": wine_texts,
            "pairing_score": scores,
        }
    )
    path = tmp_path / "pairings.parquet"
    pq.write_table(table, path)
    return str(path)


@pytest.fixture()
def trained_model(tmp_path, tiny_pairing_parquet):
    """Train a 1-epoch model and return the output directory."""
    _require_base_model()
    from cellarbrain.sommelier.training import train_model

    output = str(tmp_path / "model")
    train_model(
        pairing_parquet=tiny_pairing_parquet,
        output_dir=output,
        base_model=BASE_MODEL,
        epochs=1,
        batch_size=8,
        warmup_ratio=0.1,
        eval_split=0.2,
    )
    return output


@pytest.fixture()
def food_index(tmp_path, trained_model):
    """Build a food index from 5 test dishes."""
    from cellarbrain.sommelier.index import build_index
    from cellarbrain.sommelier.model import load_model

    model = load_model(trained_model)
    texts = [
        "Grilled lamb chops with rosemary",
        "Sashimi platter with wasabi",
        "Margherita pizza with fresh basil",
        "Chocolate fondant with vanilla ice cream",
        "Caesar salad with parmesan croutons",
    ]
    ids = ["food_1", "food_2", "food_3", "food_4", "food_5"]
    idx_path = tmp_path / "food.index"
    ids_path = tmp_path / "food_ids.json"
    build_index(texts, ids, model, idx_path, ids_path)
    return idx_path, ids_path


@pytest.fixture()
def wine_index(tmp_path, trained_model):
    """Build a wine index from 5 test wines."""
    from cellarbrain.sommelier.index import build_index
    from cellarbrain.sommelier.model import load_model

    model = load_model(trained_model)
    texts = [
        "Château Margaux 2015 Red wine. France, Bordeaux. Cabernet Sauvignon.",
        "Cloudy Bay Sauvignon Blanc 2022 White wine. New Zealand, Marlborough.",
        "Barolo Riserva 2016 Red wine. Italy, Piedmont. Nebbiolo.",
        "Moët & Chandon Brut NV Sparkling wine. France, Champagne.",
        "Sauternes 2017 Dessert wine. France, Bordeaux. Sémillon.",
    ]
    ids = ["1", "2", "3", "4", "5"]
    idx_path = tmp_path / "wine.index"
    ids_path = tmp_path / "wine_ids.json"
    build_index(texts, ids, model, idx_path, ids_path)
    return idx_path, ids_path


# ---------------------------------------------------------------------------
# TestTraining
# ---------------------------------------------------------------------------


class TestTraining:
    def test_train_tiny_dataset(self, trained_model):
        """Train 1 epoch on 20 pairs — output dir must exist with model files."""
        _require_base_model()
        from pathlib import Path

        model_dir = Path(trained_model)
        assert model_dir.exists()
        assert (model_dir / "config.json").exists()
        # Model saved as safetensors or pytorch_model.bin
        has_weights = (model_dir / "model.safetensors").exists() or (model_dir / "pytorch_model.bin").exists()
        assert has_weights, "Expected model weights file"

    def test_train_returns_metrics(self, tmp_path, tiny_pairing_parquet):
        """Verify train_model returns dict with eval metric."""
        _require_base_model()
        from cellarbrain.sommelier.training import train_model

        output = str(tmp_path / "model_metrics")
        metrics = train_model(
            pairing_parquet=tiny_pairing_parquet,
            output_dir=output,
            base_model=BASE_MODEL,
            epochs=1,
            batch_size=8,
            eval_split=0.2,
        )
        assert isinstance(metrics, dict)
        assert "eval_cosine_similarity" in metrics


# ---------------------------------------------------------------------------
# TestIndexBuild
# ---------------------------------------------------------------------------


class TestIndexBuild:
    def test_build_and_search(self, tmp_path, trained_model):
        """Build index from 10 texts, search, verify result shape."""
        _require_base_model()
        from cellarbrain.sommelier.index import build_index, load_ids, load_index, search_index
        from cellarbrain.sommelier.model import load_model

        model = load_model(trained_model)
        texts = [f"food item number {i}" for i in range(10)]
        ids = [f"id_{i}" for i in range(10)]

        idx_path = tmp_path / "test.index"
        ids_path = tmp_path / "test_ids.json"
        build_index(texts, ids, model, idx_path, ids_path)

        index = load_index(idx_path)
        loaded_ids = load_ids(ids_path)

        query_vec = model.encode(["food item number 3"], normalize_embeddings=True)
        query_vec = np.ascontiguousarray(query_vec, dtype=np.float32)
        results = search_index(query_vec, index, loaded_ids, 5)

        assert len(results) == 5
        for rid, score in results:
            assert isinstance(rid, str)
            assert isinstance(score, float)

    def test_build_saves_files(self, food_index):
        """Verify index and IDs files are written to disk."""
        _require_base_model()
        idx_path, ids_path = food_index
        assert idx_path.exists()
        assert ids_path.exists()

    def test_search_returns_correct_order(self, tmp_path, trained_model):
        """Index 3 known texts, search with one of them — top result matches."""
        _require_base_model()
        from cellarbrain.sommelier.index import build_index, load_ids, load_index, search_index
        from cellarbrain.sommelier.model import load_model

        model = load_model(trained_model)
        texts = [
            "grilled steak with peppercorn sauce",
            "fresh tuna sashimi with ginger",
            "chocolate cake with cream",
        ]
        ids = ["steak", "tuna", "chocolate"]

        idx_path = tmp_path / "order.index"
        ids_path = tmp_path / "order_ids.json"
        build_index(texts, ids, model, idx_path, ids_path)

        index = load_index(idx_path)
        loaded_ids = load_ids(ids_path)

        query_vec = model.encode(
            ["grilled steak with peppercorn sauce"],
            normalize_embeddings=True,
        )
        query_vec = np.ascontiguousarray(query_vec, dtype=np.float32)
        results = search_index(query_vec, index, loaded_ids, 3)
        assert results[0][0] == "steak"
        assert results[0][1] > 0.9  # near-perfect match

    def test_load_ids_roundtrip(self, tmp_path):
        """Round-trip test: write IDs as JSON, load with load_ids."""
        from cellarbrain.sommelier.index import load_ids

        ids = ["alpha", "beta", "gamma"]
        ids_path = tmp_path / "ids.json"
        ids_path.write_text(json.dumps(ids), encoding="utf-8")
        loaded = load_ids(ids_path)
        assert loaded == ids


# ---------------------------------------------------------------------------
# TestEngine
# ---------------------------------------------------------------------------


class TestEngine:
    def test_check_availability_no_model(self, tmp_path):
        """Returns error string when model directory does not exist."""
        from cellarbrain.settings import SommelierConfig
        from cellarbrain.sommelier.engine import SommelierEngine

        cfg = SommelierConfig(model_dir=str(tmp_path / "nonexistent"))
        engine = SommelierEngine(cfg, str(tmp_path))
        err = engine.check_availability()
        assert err is not None
        assert "not trained" in err.lower()

    def test_check_availability_ok(self, trained_model, tmp_path):
        """Returns None when model directory exists."""
        _require_base_model()
        from cellarbrain.settings import SommelierConfig
        from cellarbrain.sommelier.engine import SommelierEngine

        cfg = SommelierConfig(model_dir=trained_model)
        engine = SommelierEngine(cfg, str(tmp_path))
        assert engine.check_availability() is None


# ---------------------------------------------------------------------------
# Helpers — minimal DuckDB-queryable dataset
# ---------------------------------------------------------------------------


def _make_wine_dataset(base_dir, wines_data=None):
    """Create minimal Parquet files for DuckDB wine views.

    Parameters
    ----------
    base_dir : Path
        Directory to write Parquet files into.
    wines_data : list[dict] | None
        Optional override for wine rows.  Each dict is merged with the
        default template so callers need only specify the fields they
        care about.

    Returns the data directory.
    """
    from cellarbrain import writer
    from cellarbrain.markdown import dossier_filename

    now = datetime(2025, 1, 1)
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Château Test", "etl_run_id": rid, "updated_at": now},
        {"winery_id": 2, "name": "Domaine Example", "etl_run_id": rid, "updated_at": now},
    ]
    appellations = [
        {
            "appellation_id": 1,
            "country": "France",
            "region": "Bordeaux",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "appellation_id": 2,
            "country": "Italy",
            "region": "Piedmont",
            "subregion": None,
            "classification": None,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]

    _wine_template = {
        "wine_slug": "placeholder",
        "winery_id": 1,
        "name": "Wine",
        "vintage": 2020,
        "is_non_vintage": False,
        "appellation_id": 1,
        "category": "Red wine",
        "_raw_classification": None,
        "subcategory": None,
        "specialty": None,
        "sweetness": None,
        "effervescence": None,
        "volume_ml": 750,
        "_raw_volume": None,
        "container": None,
        "hue": None,
        "cork": None,
        "alcohol_pct": 14.0,
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
        "comment": None,
        "winemaking_notes": None,
        "is_favorite": False,
        "is_wishlist": False,
        "tracked_wine_id": None,
        "full_name": "Château Test Wine 2020",
        "grape_type": "varietal",
        "primary_grape": "Merlot",
        "grape_summary": "Merlot",
        "_raw_grapes": None,
        "dossier_path": "cellar/0001-chateau-test-wine-2020.md",
        "drinking_status": "unknown",
        "age_years": 5,
        "price_tier": "unknown",
        "bottle_format": "Standard",
        "price_per_750ml": None,
        "format_group_id": None,
        "food_tags": None,
        "is_deleted": False,
        "etl_run_id": rid,
        "updated_at": now,
    }

    if wines_data is None:
        wines_data = [
            {
                "wine_id": 1,
                "wine_slug": "chateau-test-merlot-2020",
                "full_name": "Château Test Merlot 2020",
                "primary_grape": "Merlot",
                "grape_summary": "Merlot",
                "dossier_path": dossier_filename(1, "Château Test", "Merlot", 2020, False),
            },
            {
                "wine_id": 2,
                "wine_slug": "chateau-test-cabernet-2019",
                "winery_id": 1,
                "name": "Cabernet",
                "vintage": 2019,
                "full_name": "Château Test Cabernet 2019",
                "primary_grape": "Cabernet Sauvignon",
                "grape_summary": "Cabernet Sauvignon",
                "dossier_path": dossier_filename(2, "Château Test", "Cabernet", 2019, False),
            },
            {
                "wine_id": 3,
                "wine_slug": "domaine-example-nebbiolo-2018",
                "winery_id": 2,
                "name": "Nebbiolo",
                "vintage": 2018,
                "appellation_id": 2,
                "category": "Red wine",
                "full_name": "Domaine Example Nebbiolo 2018",
                "primary_grape": "Nebbiolo",
                "grape_summary": "Nebbiolo",
                "dossier_path": dossier_filename(3, "Domaine Example", "Nebbiolo", 2018, False),
            },
        ]

    wines = [{**_wine_template, **w} for w in wines_data]

    # Bottles: wine 1 has 2 stored, wine 2 has 1 consumed (0 stored), wine 3 has 1 stored
    bottles = [
        {
            "bottle_id": 1,
            "wine_id": 1,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "A1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("20.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("20.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 2,
            "wine_id": 1,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "A2",
            "bottle_number": 2,
            "provider_id": 1,
            "purchase_date": datetime(2023, 6, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("20.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("20.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 3,
            "wine_id": 2,
            "status": "consumed",
            "cellar_id": None,
            "shelf": None,
            "bottle_number": None,
            "provider_id": 1,
            "purchase_date": datetime(2022, 1, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("30.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("30.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": datetime(2024, 1, 1).date(),
            "output_type": "consumed",
            "output_comment": None,
            "is_onsite": False,
            "is_in_transit": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
        {
            "bottle_id": 4,
            "wine_id": 3,
            "status": "stored",
            "cellar_id": 1,
            "shelf": "B1",
            "bottle_number": 1,
            "provider_id": 1,
            "purchase_date": datetime(2023, 9, 1).date(),
            "acquisition_type": "purchase",
            "original_purchase_price": Decimal("50.00"),
            "original_purchase_currency": "CHF",
            "purchase_price": Decimal("50.00"),
            "purchase_currency": "CHF",
            "purchase_comment": None,
            "output_date": None,
            "output_type": None,
            "output_comment": None,
            "is_onsite": True,
            "is_in_transit": False,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]

    cellars = [
        {"cellar_id": 1, "name": "Cave", "sort_order": 1, "etl_run_id": rid, "updated_at": now},
    ]
    providers = [
        {"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now},
    ]
    grapes = [
        {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
    ]

    for name, rows in [
        ("winery", wineries),
        ("appellation", appellations),
        ("grape", grapes),
        ("wine", wines),
        ("wine_grape", wine_grapes),
        ("bottle", bottles),
        ("cellar", cellars),
        ("provider", providers),
        ("tasting", []),
        ("pro_rating", []),
    ]:
        writer.write_parquet(name, rows, base_dir)

    return base_dir


@pytest.fixture()
def wine_dataset(tmp_path):
    """Create minimal DuckDB-queryable wine dataset with 3 wines."""
    return _make_wine_dataset(tmp_path / "data")


@pytest.fixture()
def food_catalogue(tmp_path):
    """Create a small food catalogue Parquet for enrichment tests."""
    table = pa.table(
        {
            "dish_id": ["food_1", "food_2", "food_3", "food_4", "food_5"],
            "dish_name": [
                "Grilled lamb chops with rosemary",
                "Sashimi platter with wasabi",
                "Margherita pizza with fresh basil",
                "Chocolate fondant with vanilla ice cream",
                "Caesar salad with parmesan croutons",
            ],
            "description": ["d1", "d2", "d3", "d4", "d5"],
            "ingredients": [
                ["lamb", "rosemary"],
                ["tuna", "wasabi"],
                ["mozzarella", "basil"],
                ["chocolate", "cream"],
                ["romaine", "parmesan"],
            ],
            "cuisine": ["French", "Japanese", "Italian", "French", "American"],
            "weight_class": ["heavy", "light", "medium", "heavy", "light"],
            "protein": ["lamb", "fish", "cheese", "none", "chicken"],
            "cooking_method": ["grill", "raw", "bake", "bake", "toss"],
            "flavour_profile": [
                ["rich", "herby"],
                ["clean", "umami"],
                ["savoury", "tangy"],
                ["sweet", "rich"],
                ["crisp", "savoury"],
            ],
        }
    )
    path = tmp_path / "food_catalogue.parquet"
    pq.write_table(table, path)
    return str(path)


# ---------------------------------------------------------------------------
# TestETLHook
# ---------------------------------------------------------------------------


class TestETLHook:
    """Tests for ``cli._rebuild_wine_index``."""

    def test_creates_index_files(self, trained_model, wine_dataset, tmp_path):
        """Builds wine.index and wine_ids.json from stored wines."""
        _require_base_model()
        from cellarbrain.cli import _rebuild_wine_index
        from cellarbrain.settings import Settings, SommelierConfig
        from cellarbrain.sommelier.model import load_model

        model = load_model(trained_model)
        wine_dir = tmp_path / "sommelier"
        wine_dir.mkdir()

        cfg = SommelierConfig(model_dir=trained_model)
        settings = Settings(sommelier=cfg)
        _rebuild_wine_index(model, wine_dataset, wine_dir, settings)

        assert (wine_dir / "wine.index").exists()
        assert (wine_dir / "wine_ids.json").exists()

    def test_indexes_only_stored_wines(self, trained_model, wine_dataset, tmp_path):
        """Only wines with bottles_stored > 0 appear in the index."""
        _require_base_model()
        from cellarbrain.cli import _rebuild_wine_index
        from cellarbrain.settings import Settings, SommelierConfig
        from cellarbrain.sommelier.model import load_model

        model = load_model(trained_model)
        wine_dir = tmp_path / "sommelier"
        wine_dir.mkdir()

        cfg = SommelierConfig(model_dir=trained_model)
        settings = Settings(sommelier=cfg)
        _rebuild_wine_index(model, wine_dataset, wine_dir, settings)

        ids = json.loads((wine_dir / "wine_ids.json").read_text())
        # wine 1 (2 stored) + wine 3 (1 stored) = 2 entries
        # wine 2 has 0 stored (consumed) — excluded
        assert len(ids) == 2
        assert "1" in ids
        assert "3" in ids
        assert "2" not in ids

    def test_skips_empty_cellar(self, trained_model, tmp_path, capsys):
        """When no wines have stored bottles, no index is built."""
        _require_base_model()
        from cellarbrain.cli import _rebuild_wine_index
        from cellarbrain.settings import Settings, SommelierConfig
        from cellarbrain.sommelier.model import load_model

        # Dataset where all bottles are consumed
        data_dir = tmp_path / "data_empty"
        data_dir.mkdir()
        _make_wine_dataset(
            data_dir,
            wines_data=[
                {
                    "wine_id": 1,
                    "wine_slug": "test-wine-1",
                    "full_name": "Test Wine 1",
                    "dossier_path": "cellar/0001.md",
                },
            ],
        )
        # Overwrite bottle.parquet with a consumed-only bottle
        from cellarbrain import writer

        now = datetime(2025, 1, 1)
        writer.write_parquet(
            "bottle",
            [
                {
                    "bottle_id": 1,
                    "wine_id": 1,
                    "status": "consumed",
                    "cellar_id": None,
                    "shelf": None,
                    "bottle_number": None,
                    "provider_id": 1,
                    "purchase_date": datetime(2023, 1, 1).date(),
                    "acquisition_type": "purchase",
                    "original_purchase_price": Decimal("10.00"),
                    "original_purchase_currency": "CHF",
                    "purchase_price": Decimal("10.00"),
                    "purchase_currency": "CHF",
                    "purchase_comment": None,
                    "output_date": datetime(2024, 1, 1).date(),
                    "output_type": "consumed",
                    "output_comment": None,
                    "is_onsite": False,
                    "is_in_transit": False,
                    "etl_run_id": 1,
                    "updated_at": now,
                },
            ],
            data_dir,
        )

        model = load_model(trained_model)
        wine_dir = tmp_path / "sommelier_empty"
        wine_dir.mkdir()

        cfg = SommelierConfig(model_dir=trained_model)
        settings = Settings(sommelier=cfg)
        _rebuild_wine_index(model, data_dir, wine_dir, settings)

        assert not (wine_dir / "wine.index").exists()
        captured = capsys.readouterr()
        assert "skipping" in captured.out.lower()


# ---------------------------------------------------------------------------
# TestEngineIntegration
# ---------------------------------------------------------------------------


class TestEngineIntegration:
    """Integration tests for SommelierEngine with real model + indexes."""

    def test_suggest_wines_returns_scored_results(
        self,
        trained_model,
        wine_index,
        food_index,
        wine_dataset,
        food_catalogue,
    ):
        """suggest_wines returns ScoredWine list with valid scores."""
        _require_base_model()
        from cellarbrain.settings import SommelierConfig
        from cellarbrain.sommelier.engine import SommelierEngine

        idx_path, ids_path = wine_index
        food_idx, food_ids = food_index
        wine_dir = idx_path.parent

        cfg = SommelierConfig(
            model_dir=trained_model,
            food_index=str(food_idx),
            food_ids=str(food_ids),
            wine_index_dir=wine_dir.name,
            food_catalogue=food_catalogue,
        )
        engine = SommelierEngine(cfg, wine_dir.parent)
        results = engine.suggest_wines("grilled lamb with rosemary", limit=3)

        assert len(results) > 0
        assert len(results) <= 3
        for r in results:
            assert isinstance(r.wine_id, int)
            assert 0 <= r.score <= 1.0

    def test_suggest_foods_returns_scored_results(
        self,
        trained_model,
        wine_index,
        food_index,
        wine_dataset,
        food_catalogue,
    ):
        """suggest_foods returns ScoredFood list with valid scores."""
        _require_base_model()
        from cellarbrain.settings import SommelierConfig
        from cellarbrain.sommelier.engine import SommelierEngine

        idx_path, ids_path = wine_index
        food_idx, food_ids = food_index

        # Engine's wine index dir must be relative to data_dir
        # Copy food + wine indexes into wine_dataset / "sommelier"
        import shutil

        som_dir = wine_dataset / "sommelier"
        som_dir.mkdir(exist_ok=True)
        shutil.copy(idx_path, som_dir / "wine.index")
        shutil.copy(ids_path, som_dir / "wine_ids.json")

        cfg = SommelierConfig(
            model_dir=trained_model,
            food_index=str(food_idx),
            food_ids=str(food_ids),
            wine_index_dir="sommelier",
            food_catalogue=food_catalogue,
        )
        engine = SommelierEngine(cfg, wine_dataset)
        # wine_id=1 is "Château Test Merlot 2020" — has stored bottles
        results = engine.suggest_foods(wine_id=1, limit=3)

        assert len(results) > 0
        assert len(results) <= 3
        for r in results:
            assert r.dish_name  # non-empty
            assert 0 <= r.score <= 1.0

    def test_suggest_foods_unknown_wine_raises(
        self,
        trained_model,
        food_index,
        wine_dataset,
        food_catalogue,
    ):
        """suggest_foods with non-existent wine_id raises ValueError."""
        _require_base_model()
        from cellarbrain.settings import SommelierConfig
        from cellarbrain.sommelier.engine import SommelierEngine

        food_idx, food_ids = food_index

        cfg = SommelierConfig(
            model_dir=trained_model,
            food_index=str(food_idx),
            food_ids=str(food_ids),
            food_catalogue=food_catalogue,
        )
        engine = SommelierEngine(cfg, wine_dataset)
        with pytest.raises(ValueError, match="not found"):
            engine.suggest_foods(wine_id=999999)


# ---------------------------------------------------------------------------
# TestRetrain
# ---------------------------------------------------------------------------


class TestRetrain:
    """Tests for incremental retraining (loading existing model + more epochs)."""

    def test_retrain_from_existing_model(self, trained_model, tiny_pairing_parquet, tmp_path):
        """Retrain by loading an existing fine-tuned model as base_model."""
        _require_base_model()

        from cellarbrain.sommelier.training import train_model

        # Save to a new directory (avoids Windows file lock on memory-mapped weights)
        retrain_dir = tmp_path / "retrained"

        metrics = train_model(
            pairing_parquet=tiny_pairing_parquet,
            output_dir=str(retrain_dir),
            base_model=trained_model,  # use existing fine-tuned model as base
            epochs=1,
            batch_size=8,
            eval_split=0.2,
        )

        assert isinstance(metrics, dict)
        assert "eval_cosine_similarity" in metrics
        # Model dir still has weights
        has_weights = (retrain_dir / "model.safetensors").exists() or (retrain_dir / "pytorch_model.bin").exists()
        assert has_weights

    def test_retrained_model_still_encodes(self, trained_model, tiny_pairing_parquet, tmp_path):
        """After retraining, the model can still encode text."""
        _require_base_model()
        from cellarbrain.sommelier.model import load_model
        from cellarbrain.sommelier.training import train_model

        retrain_dir = tmp_path / "retrained2"

        train_model(
            pairing_parquet=tiny_pairing_parquet,
            output_dir=str(retrain_dir),
            base_model=trained_model,
            epochs=1,
            batch_size=8,
            eval_split=0.2,
        )

        model = load_model(str(retrain_dir))
        vec = model.encode(["test query"], normalize_embeddings=True)
        assert vec.shape[0] == 1
        assert vec.shape[1] > 0
