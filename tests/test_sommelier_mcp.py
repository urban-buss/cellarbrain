"""Integration tests for MCP sommelier tools (suggest_wines, suggest_foods).

These tests require the ``[sommelier]`` extra (sentence-transformers, faiss-cpu).
They train a tiny model once per module and exercise the full pipeline:
model → index → MCP tool → enriched Markdown table.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Guards — skip entire module if ML deps or base model unavailable
# ---------------------------------------------------------------------------

BASE_MODEL = "models/sommelier/base-model"

_st = pytest.importorskip("sentence_transformers")
_faiss = pytest.importorskip("faiss")

pytestmark = pytest.mark.skipif(
    not Path(BASE_MODEL).exists(),
    reason=f"Base model not found at {BASE_MODEL}",
)

# ---------------------------------------------------------------------------
# Module-scoped fixtures (train once, reuse for all tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def tiny_model(tmp_path_factory):
    """Train a 1-epoch model on 20 fake pairs."""
    from cellarbrain.sommelier.training import train_model

    tmp = tmp_path_factory.mktemp("model")
    pairs = tmp / "pairings.parquet"
    table = pa.table(
        {
            "food_text": [f"dish {i} with ingredients" for i in range(20)],
            "wine_text": [f"wine {i} from region {i % 5}" for i in range(20)],
            "pairing_score": [round(0.05 + (i / 20) * 0.9, 3) for i in range(20)],
        }
    )
    pq.write_table(table, pairs)

    output = str(tmp / "model")
    train_model(
        pairing_parquet=str(pairs),
        output_dir=output,
        base_model=BASE_MODEL,
        epochs=1,
        batch_size=8,
        warmup_ratio=0.1,
        eval_split=0.2,
    )
    return output


@pytest.fixture(scope="module")
def food_catalogue(tmp_path_factory):
    """Create a small food catalogue Parquet."""
    tmp = tmp_path_factory.mktemp("catalogue")
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
    path = tmp / "food_catalogue.parquet"
    pq.write_table(table, path)
    return str(path)


@pytest.fixture(scope="module")
def food_index(tmp_path_factory, tiny_model, food_catalogue):
    """Build a food FAISS index from the catalogue dishes."""
    from cellarbrain.sommelier.index import build_index
    from cellarbrain.sommelier.model import load_model
    from cellarbrain.sommelier.text_builder import build_food_text

    model = load_model(tiny_model)
    cat = pq.read_table(food_catalogue)

    texts = []
    ids = []
    for i in range(cat.num_rows):
        did = cat.column("dish_id")[i].as_py()
        texts.append(
            build_food_text(
                dish_name=cat.column("dish_name")[i].as_py(),
                cuisine=cat.column("cuisine")[i].as_py(),
                weight_class=cat.column("weight_class")[i].as_py(),
                protein=cat.column("protein")[i].as_py(),
                flavour_profile=cat.column("flavour_profile")[i].as_py(),
            )
        )
        ids.append(did)

    tmp = tmp_path_factory.mktemp("food_idx")
    idx_path = tmp / "food.index"
    ids_path = tmp / "food_ids.json"
    build_index(texts, ids, model, idx_path, ids_path)
    return str(idx_path), str(ids_path)


@pytest.fixture(scope="module")
def wine_dataset(tmp_path_factory):
    """Create minimal DuckDB-queryable wine dataset with 3 wines."""
    from cellarbrain import writer
    from cellarbrain.markdown import dossier_filename

    tmp = tmp_path_factory.mktemp("wine_data")
    now = datetime(2025, 1, 1)
    rid = 1

    wineries = [
        {"winery_id": 1, "name": "Château MCP", "etl_run_id": rid, "updated_at": now},
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
    ]
    grapes = [
        {"grape_id": 1, "name": "Merlot", "etl_run_id": rid, "updated_at": now},
    ]

    _template = {
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
        "full_name": "Placeholder",
        "grape_type": "varietal",
        "primary_grape": "Merlot",
        "grape_summary": "Merlot",
        "_raw_grapes": None,
        "dossier_path": "cellar/0001.md",
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

    wines = [
        {
            **_template,
            "wine_id": 1,
            "wine_slug": "chateau-mcp-merlot-2020",
            "full_name": "Château MCP Merlot 2020",
            "dossier_path": f"cellar/{dossier_filename(1, 'Château MCP', 'Merlot', 2020, False)}",
        },
    ]
    wine_grapes = [
        {"wine_id": 1, "grape_id": 1, "percentage": 100.0, "sort_order": 1, "etl_run_id": rid, "updated_at": now},
    ]
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
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    cellars = [
        {
            "cellar_id": 1,
            "name": "Cave",
            "location_type": "onsite",
            "sort_order": 1,
            "etl_run_id": rid,
            "updated_at": now,
        },
    ]
    providers = [
        {"provider_id": 1, "name": "Shop", "etl_run_id": rid, "updated_at": now},
    ]
    etl_runs = [
        {
            "run_id": 1,
            "started_at": now,
            "finished_at": now,
            "run_type": "full",
            "wines_source_hash": "abc",
            "bottles_source_hash": "def",
            "bottles_gone_source_hash": None,
            "total_inserts": 1,
            "total_updates": 0,
            "total_deletes": 0,
            "wines_inserted": 1,
            "wines_updated": 0,
            "wines_deleted": 0,
            "wines_renamed": 0,
        },
    ]
    change_logs = [
        {
            "change_id": 1,
            "run_id": 1,
            "entity_type": "wine",
            "entity_id": 1,
            "change_type": "insert",
            "changed_fields": None,
        },
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
        writer.write_parquet(name, rows, tmp)
    writer.write_parquet("etl_run", etl_runs, tmp)
    writer.write_parquet("change_log", change_logs, tmp)

    return tmp


@pytest.fixture(scope="module")
def wine_index(tmp_path_factory, tiny_model, wine_dataset):
    """Build a wine FAISS index from the test wine dataset."""
    from cellarbrain.query import get_connection
    from cellarbrain.sommelier.index import build_index
    from cellarbrain.sommelier.model import load_model
    from cellarbrain.sommelier.text_builder import build_wine_text

    model = load_model(tiny_model)
    con = get_connection(str(wine_dataset))
    rows = con.execute("""
        SELECT wine_id, wine_name, country, region, grapes, category
        FROM wines_full
        WHERE bottles_stored > 0
    """).fetchall()

    texts, ids = [], []
    for row in rows:
        wid, name, country, region, grapes, category = row
        texts.append(
            build_wine_text(
                full_name=name,
                country=country,
                region=region,
                grape_summary=grapes,
                category=category,
            )
        )
        ids.append(str(wid))

    # Put wine index inside the data dir under sommelier/
    som_dir = wine_dataset / "sommelier"
    som_dir.mkdir(exist_ok=True)
    idx_path = som_dir / "wine.index"
    ids_path = som_dir / "wine_ids.json"
    build_index(texts, ids, model, idx_path, ids_path)
    return str(idx_path), str(ids_path)


@pytest.fixture(scope="module")
def sommelier_server(
    tiny_model,
    food_catalogue,
    food_index,
    wine_index,
    wine_dataset,
):
    """Return an mcp_server module with sommelier fully wired."""
    import os
    from unittest.mock import patch

    from cellarbrain.settings import SommelierConfig

    food_idx, food_ids = food_index

    cfg_overrides = {
        "sommelier": SommelierConfig(
            enabled=True,
            model_dir=tiny_model,
            food_catalogue=food_catalogue,
            food_index=food_idx,
            food_ids=food_ids,
            wine_index_dir="sommelier",
        ),
    }

    with patch.dict(os.environ, {"CELLARBRAIN_DATA_DIR": str(wine_dataset)}):
        from cellarbrain import mcp_server

        # Reset all caches
        mcp_server._mcp_settings = None
        mcp_server._sommelier_engine = None
        mcp_server._food_catalogue_meta = None
        mcp_server._hybrid_engine = None

        # Patch settings to inject sommelier config
        original_load = mcp_server._load_mcp_settings

        def _patched_load():
            s = original_load()
            object.__setattr__(s, "sommelier", cfg_overrides["sommelier"])
            return s

        mcp_server._load_mcp_settings = _patched_load
        mcp_server._mcp_settings = None

        yield mcp_server

        # Cleanup
        mcp_server._load_mcp_settings = original_load
        mcp_server._mcp_settings = None
        mcp_server._sommelier_engine = None
        mcp_server._food_catalogue_meta = None
        mcp_server._hybrid_engine = None


# ---------------------------------------------------------------------------
# TestSuggestWines
# ---------------------------------------------------------------------------


class TestSuggestWines:
    def test_returns_markdown_table(self, sommelier_server):
        """suggest_wines returns a Markdown table with correct headers."""
        result = asyncio.run(sommelier_server.suggest_wines(food_query="grilled steak"))
        assert "| Rank |" in result
        assert "| Score |" in result
        assert "| Wine |" in result

    def test_returns_ranked_results(self, sommelier_server):
        """Results include at least rank 1."""
        result = asyncio.run(sommelier_server.suggest_wines(food_query="grilled steak"))
        assert "| 1 |" in result

    def test_limit_parameter(self, sommelier_server):
        """limit=1 returns at most 1 data row."""
        result = asyncio.run(sommelier_server.suggest_wines(food_query="grilled steak", limit=1))
        data_rows = [
            line for line in result.split("\n") if line.startswith("| ") and "Rank" not in line and "---" not in line
        ]
        assert len(data_rows) <= 1

    def test_enrichment_includes_wine_name(self, sommelier_server):
        """Enriched table includes the wine name from DuckDB."""
        result = asyncio.run(sommelier_server.suggest_wines(food_query="red meat"))
        # Our test wine is "Château MCP Merlot 2020"
        assert "Château MCP" in result or "Wine #" in result


# ---------------------------------------------------------------------------
# TestSuggestFoods
# ---------------------------------------------------------------------------


class TestSuggestFoods:
    def test_returns_markdown_table(self, sommelier_server):
        """suggest_foods returns a Markdown table with correct headers."""
        result = asyncio.run(sommelier_server.suggest_foods(wine_id=1))
        assert "| Rank |" in result
        assert "| Score |" in result
        assert "| Dish |" in result

    def test_returns_enriched_columns(self, sommelier_server):
        """Table includes cuisine and weight columns from catalogue."""
        result = asyncio.run(sommelier_server.suggest_foods(wine_id=1))
        assert "Cuisine" in result
        assert "Weight" in result
        assert "Protein" in result
        assert "Flavour Profile" in result

    def test_returns_ranked_results(self, sommelier_server):
        """Results include at least rank 1."""
        result = asyncio.run(sommelier_server.suggest_foods(wine_id=1))
        assert "| 1 |" in result

    def test_invalid_wine_id_returns_error(self, sommelier_server):
        """Non-existent wine_id returns error string."""
        result = asyncio.run(sommelier_server.suggest_foods(wine_id=999999))
        assert result.startswith("Error:")


# ---------------------------------------------------------------------------
# TestAddPairing
# ---------------------------------------------------------------------------


class TestAddPairing:
    """Integration test for add_pairing through the MCP server fixture."""

    def test_add_pairing_round_trip(self, sommelier_server, tmp_path):
        """add_pairing writes to disk and the Parquet is readable."""
        import dataclasses

        dataset_path = tmp_path / "test_pairings.parquet"
        original = sommelier_server._load_mcp_settings

        def _patched():
            s = original()
            new_som = dataclasses.replace(s.sommelier, pairing_dataset=str(dataset_path))
            object.__setattr__(s, "sommelier", new_som)
            return s

        sommelier_server._load_mcp_settings = _patched
        sommelier_server._mcp_settings = None

        try:
            result = sommelier_server.add_pairing(
                food_text="grilled lamb | ingredients: lamb, rosemary | French | heavy | lamb | grill | rich",
                wine_text="Château MCP Merlot | Merlot | Bordeaux, France | red",
                pairing_score=0.85,
                pairing_reason="Rich tannins complement lamb fat.",
            )
            assert "Pairing added" in result
            assert "1 pairs" in result

            # Verify Parquet contents
            table = pq.read_table(dataset_path)
            assert table.num_rows == 1
            assert table.column("food_text")[0].as_py().startswith("grilled lamb")
            assert table.column("pairing_score")[0].as_py() == 0.85
        finally:
            sommelier_server._load_mcp_settings = original
            sommelier_server._mcp_settings = None
