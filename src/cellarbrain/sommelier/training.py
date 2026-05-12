"""Sommelier model training — fine-tune sentence-transformers on pairing data."""

from __future__ import annotations

import logging
import math
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def train_model(
    pairing_parquet: str | Path,
    output_dir: str | Path,
    *,
    base_model: str = "models/sommelier/base-model",
    epochs: int = 10,
    batch_size: int = 32,
    warmup_ratio: float = 0.1,
    eval_split: float = 0.1,
) -> dict[str, float]:
    """Fine-tune a sentence-transformers model on food-wine pairing data.

    Loads the pairing dataset, splits into train/eval, and fine-tunes the
    base model using ``CosineSimilarityLoss``.  The trained model is saved
    to *output_dir*.

    Returns a dict of evaluation metrics from the final evaluator run.

    Raises:
        ImportError: If sentence-transformers is not installed.
        FileNotFoundError: If *pairing_parquet* does not exist.
    """
    try:
        from sentence_transformers import InputExample, SentenceTransformer
        from sentence_transformers.sentence_transformer.evaluation import (
            EmbeddingSimilarityEvaluator,
        )
        from sentence_transformers.sentence_transformer.losses import CosineSimilarityLoss
    except ImportError:
        raise ImportError(
            "sentence-transformers is required for model training. Install with: pip install cellarbrain[ml]"
        ) from None

    from torch.utils.data import DataLoader

    pairing_path = Path(pairing_parquet)
    if not pairing_path.exists():
        raise FileNotFoundError(f"Pairing dataset not found: {pairing_path}. Run build_pairings.py first.")

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    # --- Load data -----------------------------------------------------------

    table = pq.read_table(pairing_path)
    food_texts = table.column("food_text")
    wine_texts = table.column("wine_text")
    scores = table.column("pairing_score")
    n = table.num_rows

    logger.info("Loaded %d pairing examples from %s", n, pairing_path)

    # --- Build InputExample list --------------------------------------------

    examples: list[InputExample] = []
    for i in range(n):
        examples.append(
            InputExample(
                texts=[food_texts[i].as_py(), wine_texts[i].as_py()],
                label=float(scores[i].as_py()),
            )
        )

    # --- Stratified train/eval split (by score quintile) --------------------

    eval_n = max(1, int(n * eval_split))
    # Sort by score, take every Nth item for eval to get balanced distribution
    indexed = sorted(enumerate(examples), key=lambda x: x[1].label)
    step = max(1, n // eval_n)
    eval_indices = set(indexed[i][0] for i in range(0, n, step))
    # Trim to exact eval_n
    while len(eval_indices) > eval_n:
        eval_indices.pop()

    train_examples = [ex for i, ex in enumerate(examples) if i not in eval_indices]
    eval_examples = [ex for i, ex in enumerate(examples) if i in eval_indices]

    logger.info("Split: %d train, %d eval", len(train_examples), len(eval_examples))

    # --- Eval data for EmbeddingSimilarityEvaluator -------------------------

    eval_sentences1 = [ex.texts[0] for ex in eval_examples]
    eval_sentences2 = [ex.texts[1] for ex in eval_examples]
    eval_scores = [ex.label for ex in eval_examples]

    evaluator = EmbeddingSimilarityEvaluator(
        eval_sentences1,
        eval_sentences2,
        eval_scores,
        name="pairing-eval",
    )

    # --- Load base model + train --------------------------------------------

    logger.info("Loading base model: %s", base_model)
    model = SentenceTransformer(base_model)

    train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=batch_size)
    train_loss = CosineSimilarityLoss(model)

    total_steps = math.ceil(len(train_examples) / batch_size) * epochs
    warmup_steps = int(warmup_ratio * total_steps)

    logger.info(
        "Training: %d epochs, batch %d, %d total steps, %d warmup",
        epochs,
        batch_size,
        total_steps,
        warmup_steps,
    )

    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        evaluator=evaluator,
        epochs=epochs,
        warmup_steps=warmup_steps,
        output_path=str(output),
        evaluation_steps=0,  # evaluate at end of each epoch
        show_progress_bar=True,
    )

    # --- Final evaluation ---------------------------------------------------

    final_score = evaluator(model, str(output))
    metrics = {"eval_cosine_similarity": final_score}

    logger.info("Training complete. Final eval cosine similarity: %.4f", final_score)
    return metrics
