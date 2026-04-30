"""Sommelier embedding model — lazy-loaded sentence-transformers wrapper."""

from __future__ import annotations

from pathlib import Path


class ModelNotTrainedError(Exception):
    """Raised when sommelier model weights are not found."""


def load_model(model_dir: str | Path):
    """Load a sentence-transformers model from disk.

    Raises ModelNotTrainedError if the directory does not exist.
    Raises ImportError if sentence-transformers is not installed.
    """
    model_path = Path(model_dir)
    if not model_path.exists():
        raise ModelNotTrainedError(f"Sommelier model not found at {model_path}. Run `cellarbrain train-model` first.")
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        raise ImportError(
            "sentence-transformers is required for the sommelier module. "
            "Install with: pip install cellarbrain[sommelier]"
        ) from None
    return SentenceTransformer(str(model_path))
