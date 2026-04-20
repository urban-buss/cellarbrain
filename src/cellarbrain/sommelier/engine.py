"""Sommelier engine — orchestrates model, indexes, and metadata."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from .model import ModelNotTrainedError

logger = logging.getLogger(__name__)


@dataclass
class ScoredWine:
    """A wine result with its similarity score."""

    wine_id: int
    score: float
    wine_text: str


@dataclass
class ScoredFood:
    """A food result with its similarity score."""

    dish_id: str
    dish_name: str
    score: float
    food_text: str


def check_availability(model_dir: str) -> str | None:
    """Return an error message if the sommelier is not ready, else None.

    Checks whether the model directory exists. Does NOT load heavy deps.
    """
    if not Path(model_dir).exists():
        return (
            "Sommelier model not trained. "
            "Run `cellarbrain train-model` first."
        )
    return None


class SommelierEngine:
    """Orchestrates model, indexes, and metadata for pairing queries."""

    def __init__(self, config, data_dir: str | Path):
        self._config = config
        self._data_dir = Path(data_dir)
        self._model = None
        self._food_index = None
        self._food_ids: list[str] | None = None
        self._wine_index = None
        self._wine_ids: list[str] | None = None

    def check_availability(self) -> str | None:
        """Return error message if not ready, else None."""
        return check_availability(self._config.model_dir)

    def suggest_wines(self, food_query: str, limit: int = 10) -> list[ScoredWine]:
        """Encode food_query, search the wine index, return scored wines."""
        import numpy as np

        self._ensure_model()
        self._ensure_wine_index()

        vector = self._model.encode(
            [food_query], normalize_embeddings=True,
        )
        vector = np.ascontiguousarray(vector, dtype=np.float32)

        from .index import search_index
        results = search_index(vector, self._wine_index, self._wine_ids, limit)
        return [
            ScoredWine(wine_id=int(wid), score=score, wine_text="")
            for wid, score in results
        ]

    def suggest_foods(self, wine_id: int, limit: int = 10) -> list[ScoredFood]:
        """Build wine text, encode it, search the food index."""
        import numpy as np
        import pyarrow.parquet as pq

        self._ensure_model()
        self._ensure_food_index()

        # Build wine text from food catalogue metadata isn't right —
        # we need the food index searched with a wine embedding.
        # Look up wine text: encode it from metadata.
        wine_text = self._get_wine_text(wine_id)

        vector = self._model.encode(
            [wine_text], normalize_embeddings=True,
        )
        vector = np.ascontiguousarray(vector, dtype=np.float32)

        from .index import search_index
        results = search_index(vector, self._food_index, self._food_ids, limit)

        # Load food catalogue for dish names
        food_names = self._get_food_names()
        return [
            ScoredFood(
                dish_id=did, dish_name=food_names.get(did, did),
                score=score, food_text="",
            )
            for did, score in results
        ]

    def _get_wine_text(self, wine_id: int) -> str:
        """Build embedding text for a wine from DuckDB metadata."""
        from .. import query as q
        from .text_builder import build_wine_text

        con = q.get_connection(str(self._data_dir))
        rows = con.execute("""
            SELECT wine_name, country, region, grapes, category
            FROM wines_full
            WHERE wine_id = ?
        """, [wine_id]).fetchall()
        if not rows:
            raise ValueError(f"Wine {wine_id} not found in cellar data.")
        row = rows[0]
        return build_wine_text(
            full_name=row[0],
            country=row[1],
            region=row[2],
            grape_summary=row[3],
            category=row[4],
        )

    def _get_food_names(self) -> dict[str, str]:
        """Load dish_id → dish_name mapping from the food catalogue."""
        import pyarrow.parquet as pq

        table = pq.read_table(self._config.food_catalogue, columns=["dish_id", "dish_name"])
        return {
            table.column("dish_id")[i].as_py(): table.column("dish_name")[i].as_py()
            for i in range(table.num_rows)
        }

    def _ensure_model(self) -> None:
        """Lazy-load the sentence-transformers model."""
        if self._model is None:
            from .model import load_model
            self._model = load_model(self._config.model_dir)
            logger.debug("Sommelier model loaded from %s", self._config.model_dir)

    def _ensure_food_index(self) -> None:
        """Lazy-load the food FAISS index and ID mapping."""
        if self._food_index is None:
            from .index import load_ids, load_index
            self._food_index = load_index(self._config.food_index)
            self._food_ids = load_ids(self._config.food_ids)
            logger.debug("Food index loaded: %d entries", len(self._food_ids))

    def _ensure_wine_index(self) -> None:
        """Lazy-load the wine FAISS index and ID mapping."""
        if self._wine_index is None:
            from .index import load_ids, load_index

            wine_dir = self._data_dir / self._config.wine_index_dir
            idx_path = wine_dir / "wine.index"
            ids_path = wine_dir / "wine_ids.json"
            self._wine_index = load_index(idx_path)
            self._wine_ids = load_ids(ids_path)
            logger.debug("Wine index loaded: %d entries", len(self._wine_ids))
