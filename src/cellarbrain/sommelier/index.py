"""FAISS index operations — build, load, save, search."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)


class IndexNotFoundError(Exception):
    """Raised when a FAISS index file is not found."""


def _import_faiss():
    """Lazy-import faiss, raising a helpful error if missing."""
    try:
        import faiss
    except ImportError:
        raise ImportError(
            "faiss-cpu is required for the sommelier module. Install with: pip install cellarbrain[ml]"
        ) from None
    return faiss


def load_index(index_path: str | Path):
    """Load a FAISS index from disk.

    Raises IndexNotFoundError if the file does not exist.
    Raises ImportError if faiss is not installed.
    """
    path = Path(index_path)
    if not path.exists():
        raise IndexNotFoundError(
            f"FAISS index not found at {path}. Run `cellarbrain rebuild-indexes` or `cellarbrain etl` first."
        )
    faiss = _import_faiss()
    return faiss.read_index(str(path))


def load_ids(ids_path: str | Path) -> list[str]:
    """Load an ID mapping (JSON list) from disk."""
    path = Path(ids_path)
    if not path.exists():
        raise IndexNotFoundError(f"ID mapping not found at {path}. Run `cellarbrain rebuild-indexes` first.")
    return json.loads(path.read_text(encoding="utf-8"))


def build_index(
    texts: list[str],
    ids: list[str],
    model,
    index_path: str | Path,
    ids_path: str | Path,
) -> int:
    """Encode texts, build FAISS IndexFlatIP, and save to disk.

    Uses L2-normalised embeddings so inner product = cosine similarity.

    Returns the number of vectors indexed.
    """
    faiss = _import_faiss()
    idx_path = Path(index_path)
    id_path = Path(ids_path)
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    id_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Encoding %d texts for FAISS index...", len(texts))
    embeddings = model.encode(
        texts,
        normalize_embeddings=True,
        show_progress_bar=True,
    )
    embeddings = np.ascontiguousarray(embeddings, dtype=np.float32)

    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)

    faiss.write_index(index, str(idx_path))
    id_path.write_text(json.dumps(ids, ensure_ascii=False), encoding="utf-8")

    logger.info("Saved FAISS index (%d vectors, dim=%d) to %s", len(ids), dim, idx_path)
    return len(ids)


def search_index(
    query_vector: np.ndarray,
    index,
    ids: list[str],
    limit: int = 10,
) -> list[tuple[str, float]]:
    """Search a FAISS index for the nearest neighbours.

    Returns a list of (id, score) tuples, sorted by descending score.
    """
    if query_vector.ndim == 1:
        query_vector = query_vector.reshape(1, -1)
    query_vector = np.ascontiguousarray(query_vector, dtype=np.float32)

    k = min(limit, index.ntotal)
    distances, indices = index.search(query_vector, k)

    results: list[tuple[str, float]] = []
    for i, d in zip(indices[0], distances[0]):
        if i >= 0:
            results.append((ids[i], float(d)))
    return results
