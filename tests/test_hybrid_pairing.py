"""Tests for the HybridPairingEngine — RAG + embedding re-rank."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pytest

from cellarbrain import pairing
from cellarbrain.hybrid_pairing import HybridPairingEngine, HybridResult

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pairing_con():
    """Same wines_full fixture as test_pairing.py (subset)."""
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE VIEW wines_full AS
        SELECT * FROM (VALUES
            (1, 'Château Margaux Grand Vin', 2015, 'Red wine', 'France', 'Margaux',
             'Cabernet Sauvignon', 3, 120.0, 'optimal', 92.0,
             ['duck-confit', 'beef-bourguignon']::VARCHAR[],
             ['red_meat', 'heavy', 'French']::VARCHAR[]),
            (3, 'Barolo DOCG', 2018, 'Red wine', 'Italy', 'Barolo',
             'Nebbiolo', 2, 55.0, 'optimal', 94.0,
             ['truffle-pasta', 'braised-beef']::VARCHAR[],
             ['red_meat', 'heavy', 'Italian']::VARCHAR[]),
            (5, 'Amarone della Valpolicella', 2017, 'Red wine', 'Italy', 'Valpolicella',
             'Corvina', 1, 65.0, 'optimal', 91.0,
             ['braised-beef', 'game-stew']::VARCHAR[],
             ['red_meat', 'heavy', 'Italian']::VARCHAR[])
        ) AS t(wine_id, wine_name, vintage, category, country, region,
               primary_grape, bottles_stored, price, drinking_status,
               best_pro_score, food_tags, food_groups)
    """)
    # Also need a `wines_full` row source for embed_wines (uses same view)
    yield con
    con.close()


@dataclass
class StubConfig:
    hybrid_enabled: bool = True
    rerank_pool_size: int = 30
    rerank_blend: float = 0.5


class StubSommelier:
    """Lightweight stand-in for SommelierEngine that returns scripted scores."""

    def __init__(self, available: bool = True, scores: dict[int, float] | None = None):
        self._available = available
        # Scores are cosine similarities in [-1, 1]; default 0.0
        self._scores = scores or {}

    def check_availability(self) -> str | None:
        return None if self._available else "model not trained"

    def embed_text(self, text: str):
        import numpy as np

        # Single dim is enough — we only need dot product matching with embed_wines.
        return np.array([1.0], dtype=np.float32)

    def embed_wines(self, con, wine_ids):
        import numpy as np

        # Filter to wine_ids that exist in the view (mirrors real behaviour).
        placeholders = ", ".join("?" for _ in wine_ids)
        existing = {
            r[0]
            for r in con.execute(
                f"SELECT wine_id FROM wines_full WHERE wine_id IN ({placeholders})",
                list(wine_ids),
            ).fetchall()
        }
        ordered = [wid for wid in wine_ids if wid in existing]
        vecs = np.array(
            [[self._scores.get(wid, 0.0)] for wid in ordered],
            dtype=np.float32,
        )
        return ordered, vecs


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPureRagFallback:
    """When the sommelier is missing or disabled the engine returns RAG only."""

    def test_no_sommelier(self, pairing_con):
        engine = HybridPairingEngine(None, StubConfig())
        out = engine.retrieve(pairing_con, dish_description="braised beef", limit=5)
        assert out.mode == "rag"
        assert out.fallback_reason == "no_sommelier"
        assert out.rerank_count == 0
        assert out.blend is None
        assert out.candidates  # RAG should still return something

    def test_model_unavailable(self, pairing_con):
        engine = HybridPairingEngine(StubSommelier(available=False), StubConfig())
        out = engine.retrieve(pairing_con, dish_description="braised beef", limit=5)
        assert out.mode == "rag"
        assert out.fallback_reason == "model_unavailable"

    def test_hybrid_disabled(self, pairing_con):
        engine = HybridPairingEngine(StubSommelier(), StubConfig(hybrid_enabled=False))
        out = engine.retrieve(pairing_con, dish_description="braised beef", limit=5)
        assert out.mode == "rag"
        assert out.fallback_reason == "hybrid_disabled"

    def test_no_dish_text(self, pairing_con):
        engine = HybridPairingEngine(StubSommelier(), StubConfig())
        out = engine.retrieve(
            pairing_con,
            dish_description="",
            protein="red_meat",
            category="red",
            limit=5,
        )
        assert out.mode == "rag"
        assert out.fallback_reason == "no_dish_text"

    def test_no_rag_candidates(self, pairing_con):
        engine = HybridPairingEngine(StubSommelier(), StubConfig())
        # cuisine that has no wines in the test fixture and no proteins → empty
        out = engine.retrieve(
            pairing_con,
            dish_description="something completely unmatched",
            limit=5,
        )
        # Either rag returned nothing → fallback reason 'no_rag_candidates'
        # or it found something — we only check fallback when empty.
        if not out.candidates:
            assert out.fallback_reason == "no_rag_candidates"


class TestHybridReranking:
    """Embedding scores reorder RAG candidates."""

    def test_rerank_reorders_by_embedding(self, pairing_con):
        # Make wine 5 (Amarone) the embedding favourite.
        sommelier = StubSommelier(scores={5: 0.95, 3: 0.10, 1: 0.10})
        engine = HybridPairingEngine(sommelier, StubConfig(rerank_blend=1.0))
        out = engine.retrieve(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            weight="heavy",
            limit=3,
        )
        assert out.mode == "hybrid"
        assert out.rerank_count >= 1
        assert out.blend == 1.0
        assert out.candidates[0].wine_id == 5
        # The embed:* signal is appended.
        assert any(s.startswith("embed:") for s in out.candidates[0].match_signals)

    def test_blend_zero_keeps_rag_order(self, pairing_con):
        sommelier = StubSommelier(scores={5: 0.95, 3: 0.10, 1: 0.10})
        engine = HybridPairingEngine(sommelier, StubConfig(rerank_blend=0.0))
        rag_only = pairing.retrieve_candidates(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            weight="heavy",
            limit=3,
        )
        out = engine.retrieve(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            weight="heavy",
            limit=3,
        )
        assert out.mode == "hybrid"
        # With blend=0 the ranking depends only on signal_count; ties
        # broken by best_pro_score.  Top wine_id should match RAG.
        assert out.candidates[0].wine_id == rag_only[0].wine_id

    def test_rerank_clamps_blend(self, pairing_con):
        engine = HybridPairingEngine(
            StubSommelier(scores={1: 0.5}),
            StubConfig(rerank_blend=5.0),  # out of range → clamped to 1.0
        )
        out = engine.retrieve(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            limit=3,
        )
        assert out.mode == "hybrid"
        assert out.blend == 1.0


class TestRerankErrorFallback:
    """Exceptions inside the embedding path fall back to RAG."""

    def test_embed_text_error(self, pairing_con):
        class BoomSommelier(StubSommelier):
            def embed_text(self, text):
                raise RuntimeError("model exploded")

        engine = HybridPairingEngine(BoomSommelier(), StubConfig())
        out = engine.retrieve(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            limit=3,
        )
        assert out.mode == "rag"
        assert out.fallback_reason and out.fallback_reason.startswith("rerank_error:")
        assert out.candidates  # RAG candidates still returned

    def test_embed_wines_returns_empty(self, pairing_con):
        class EmptySommelier(StubSommelier):
            def embed_wines(self, con, wine_ids):
                import numpy as np

                return [], np.empty((0, 1), dtype=np.float32)

        engine = HybridPairingEngine(EmptySommelier(), StubConfig())
        out = engine.retrieve(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            limit=3,
        )
        # When embed_wines returns nothing the reranker keeps the RAG order
        # but still reports hybrid mode (best-effort succeeded without raising).
        assert out.mode == "hybrid"
        assert out.rerank_count == 0


class TestHybridResultShape:
    def test_result_fields(self, pairing_con):
        engine = HybridPairingEngine(StubSommelier(), StubConfig())
        out = engine.retrieve(
            pairing_con,
            dish_description="braised beef",
            protein="red_meat",
            limit=2,
        )
        assert isinstance(out, HybridResult)
        assert isinstance(out.candidates, list)
        assert out.rag_count >= len(out.candidates)
