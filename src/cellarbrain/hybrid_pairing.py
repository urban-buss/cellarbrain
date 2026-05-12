"""Hybrid food-pairing engine — RAG retrieval + embedding re-ranking.

Combines the always-available SQL/RAG candidate retrieval from
:mod:`cellarbrain.pairing` with the optional sommelier embedding model
as a re-ranker.  When the model is unavailable or the hybrid path
fails, the engine transparently falls back to pure RAG so callers
never have to branch on availability.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace

import duckdb

from . import pairing
from .pairing import PairingCandidate

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HybridResult:
    """Result of a hybrid pairing retrieval call.

    Attributes:
        candidates: Final ranked list (truncated to ``limit``).
        mode: ``"rag"`` when only RAG was used, ``"hybrid"`` when
            embedding re-ranking was applied successfully.
        rag_count: Number of RAG candidates considered before re-rank.
        rerank_count: Number of candidates that received an embedding
            score (0 in pure-RAG mode).
        blend: Embedding-vs-RAG blend weight that was applied
            (0 = pure RAG, 1 = pure embedding).  ``None`` in pure-RAG
            mode.
        fallback_reason: Reason for falling back to RAG, if any.
    """

    candidates: list[PairingCandidate]
    mode: str
    rag_count: int
    rerank_count: int = 0
    blend: float | None = None
    fallback_reason: str | None = None
    extra: dict[str, object] = field(default_factory=dict)


class HybridPairingEngine:
    """Combine RAG retrieval with optional embedding re-ranking.

    Construct with the cached :class:`SommelierEngine` (or ``None``)
    and a :class:`SommelierConfig`.  Call :meth:`retrieve` for each
    pairing query.
    """

    def __init__(self, sommelier_engine, config) -> None:
        self._sommelier = sommelier_engine
        self._config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def retrieve(
        self,
        con: duckdb.DuckDBPyConnection,
        *,
        dish_description: str | None = None,
        category: str | None = None,
        weight: str | None = None,
        protein: str | None = None,
        cuisine: str | None = None,
        grapes: list[str] | None = None,
        limit: int = 15,
    ) -> HybridResult:
        """Retrieve and (optionally) re-rank pairing candidates."""
        rerank_pool = max(int(self._config.rerank_pool_size), limit)
        rag_candidates = pairing.retrieve_candidates(
            con,
            dish_description=dish_description,
            category=category,
            weight=weight,
            protein=protein,
            cuisine=cuisine,
            grapes=grapes,
            limit=rerank_pool,
        )

        # --- Decide whether to attempt re-rank ----------------------------
        skip_reason = self._skip_reason(dish_description, rag_candidates)
        if skip_reason is not None:
            return HybridResult(
                candidates=rag_candidates[:limit],
                mode="rag",
                rag_count=len(rag_candidates),
                fallback_reason=skip_reason,
            )

        # --- Attempt embedding re-rank ------------------------------------
        try:
            blend = float(self._config.rerank_blend)
            blend = max(0.0, min(1.0, blend))
            reranked, scored_count = self._rerank(con, dish_description or "", rag_candidates, blend)
        except Exception as exc:  # defensive: fall back to RAG on any error
            logger.warning("Hybrid re-rank failed, falling back to RAG: %s", exc)
            return HybridResult(
                candidates=rag_candidates[:limit],
                mode="rag",
                rag_count=len(rag_candidates),
                fallback_reason=f"rerank_error: {exc}",
            )

        return HybridResult(
            candidates=reranked[:limit],
            mode="hybrid",
            rag_count=len(rag_candidates),
            rerank_count=scored_count,
            blend=blend,
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _skip_reason(
        self,
        dish_description: str | None,
        rag_candidates: list[PairingCandidate],
    ) -> str | None:
        """Return a human-readable reason to skip re-rank, or ``None``."""
        if not getattr(self._config, "hybrid_enabled", True):
            return "hybrid_disabled"
        if self._sommelier is None:
            return "no_sommelier"
        avail = self._sommelier.check_availability()
        if avail is not None:
            return "model_unavailable"
        if not dish_description or not dish_description.strip():
            return "no_dish_text"
        if not rag_candidates:
            return "no_rag_candidates"
        return None

    def _rerank(
        self,
        con: duckdb.DuckDBPyConnection,
        dish_description: str,
        candidates: list[PairingCandidate],
        blend: float,
    ) -> tuple[list[PairingCandidate], int]:
        """Score candidates by blended (RAG, embedding) and resort.

        Returns ``(reranked_candidates, embedding_scored_count)``.
        """
        wine_ids = [c.wine_id for c in candidates]
        ordered_ids, vectors = self._sommelier.embed_wines(con, wine_ids)
        if len(ordered_ids) == 0:
            return list(candidates), 0

        query_vec = self._sommelier.embed_text(dish_description)
        # Both sides are L2-normalised → dot product == cosine similarity.
        sims = vectors @ query_vec  # shape (N,)
        # Map to 0..1 (cosine in [-1, 1]).
        emb_scores = (sims + 1.0) / 2.0
        emb_by_id: dict[int, float] = {wid: float(score) for wid, score in zip(ordered_ids, emb_scores)}

        max_signals = max((c.signal_count for c in candidates), default=1) or 1
        rescored: list[tuple[float, float, PairingCandidate]] = []
        for c in candidates:
            rag_score = c.signal_count / max_signals
            emb_score = emb_by_id.get(c.wine_id, 0.0)
            blended = blend * emb_score + (1.0 - blend) * rag_score
            new_signals = list(c.match_signals)
            if c.wine_id in emb_by_id:
                new_signals.append(f"embed:{emb_score:.2f}")
            new_candidate = replace(c, match_signals=new_signals)
            rescored.append((blended, c.best_pro_score or 0.0, new_candidate))

        rescored.sort(key=lambda t: (t[0], t[1]), reverse=True)
        return [t[2] for t in rescored], len(ordered_ids)


__all__ = ["HybridPairingEngine", "HybridResult"]
