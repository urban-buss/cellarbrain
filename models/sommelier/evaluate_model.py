"""Evaluate the trained sommelier model on held-out pairing data.

Metrics
-------
- Recall@K (K=5, 10, 20): fraction of known good pairings in top-K
- Mean Reciprocal Rank (MRR): average 1/rank of correct pairing
- Score correlation: Spearman correlation between model cosine sim
  and pairing_score

Usage::

    python models/sommelier/evaluate_model.py
    python models/sommelier/evaluate_model.py --model models/sommelier/model
"""

from __future__ import annotations

import argparse
import sys

import numpy as np
import pyarrow.parquet as pq
from scipy.stats import spearmanr


def _load_eval_pairs(pairing_parquet: str, eval_split: float, seed: int = 42):
    """Load the eval split — same split as training."""
    table = pq.read_table(pairing_parquet)
    n = table.num_rows
    rng = np.random.default_rng(seed)

    # Stratified split by score quintile (same as training.py)
    scores = np.array([table.column("pairing_score")[i].as_py() for i in range(n)])
    quintiles = np.digitize(scores, np.quantile(scores, [0.2, 0.4, 0.6, 0.8]))
    indices = np.arange(n)
    eval_indices: list[int] = []
    for q_val in range(5):
        mask = quintiles == q_val
        q_idx = indices[mask]
        rng.shuffle(q_idx)
        k = max(1, int(len(q_idx) * eval_split))
        eval_indices.extend(q_idx[:k].tolist())

    eval_indices.sort()
    return table.take(eval_indices)


def evaluate(
    model_dir: str = "models/sommelier/model",
    pairing_parquet: str = "models/sommelier/pairing_dataset.parquet",
    eval_split: float = 0.1,
    ks: tuple[int, ...] = (5, 10, 20),
) -> dict[str, float]:
    """Run the full evaluation and return metrics."""
    from sentence_transformers import SentenceTransformer

    print(f"Loading model from {model_dir}...")
    model = SentenceTransformer(model_dir)

    print(f"Loading eval pairs (split={eval_split})...")
    eval_table = _load_eval_pairs(pairing_parquet, eval_split)
    n_eval = eval_table.num_rows
    print(f"  {n_eval} eval pairs")

    food_texts = [eval_table.column("food_text")[i].as_py() for i in range(n_eval)]
    wine_texts = [eval_table.column("wine_text")[i].as_py() for i in range(n_eval)]
    true_scores = np.array(
        [eval_table.column("pairing_score")[i].as_py() for i in range(n_eval)],
    )

    # Build wine corpus from all unique wine texts
    unique_wines = list(set(wine_texts))
    wine_to_idx = {w: i for i, w in enumerate(unique_wines)}

    print(f"Encoding {len(unique_wines)} unique wines...")
    wine_embeddings = model.encode(unique_wines, normalize_embeddings=True, show_progress_bar=True)

    # Evaluate retrieval for high-quality pairs (score >= 0.7)
    high_quality_mask = true_scores >= 0.7
    hq_food = [f for f, m in zip(food_texts, high_quality_mask) if m]
    hq_wine = [w for w, m in zip(wine_texts, high_quality_mask) if m]
    print(f"  {len(hq_food)} high-quality pairs (score >= 0.7)")

    if not hq_food:
        print("WARNING: No high-quality pairs for Recall/MRR evaluation.")
        return {"recall@5": 0.0, "recall@10": 0.0, "recall@20": 0.0, "mrr": 0.0}

    print("Encoding food queries...")
    food_embeddings = model.encode(hq_food, normalize_embeddings=True, show_progress_bar=True)

    # Compute cosine similarities (inner product on normalised = cosine)
    sim_matrix = food_embeddings @ wine_embeddings.T  # (n_hq, n_wines)

    # Recall@K and MRR
    reciprocal_ranks: list[float] = []
    recall_at: dict[int, int] = {k: 0 for k in ks}

    for i, correct_wine in enumerate(hq_wine):
        target_idx = wine_to_idx[correct_wine]
        ranking = np.argsort(-sim_matrix[i])
        rank = int(np.where(ranking == target_idx)[0][0]) + 1  # 1-based
        reciprocal_ranks.append(1.0 / rank)
        for k in ks:
            if rank <= k:
                recall_at[k] += 1

    n_hq = len(hq_food)
    mrr = float(np.mean(reciprocal_ranks))

    # Score correlation (all eval pairs)
    print("Computing score correlation...")
    all_food_emb = model.encode(food_texts, normalize_embeddings=True, show_progress_bar=False)
    model_sims = np.array([
        float(all_food_emb[i] @ wine_embeddings[wine_to_idx[wine_texts[i]]])
        for i in range(n_eval)
    ])
    spearman_corr, spearman_p = spearmanr(true_scores, model_sims)

    metrics: dict[str, float] = {
        "mrr": mrr,
        "spearman_r": float(spearman_corr),
        "spearman_p": float(spearman_p),
    }
    for k in ks:
        metrics[f"recall@{k}"] = recall_at[k] / n_hq

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate sommelier model")
    parser.add_argument("--model", default="models/sommelier/model", help="Model directory")
    parser.add_argument("--data", default="models/sommelier/pairing_dataset.parquet",
                        help="Pairing dataset")
    parser.add_argument("--eval-split", type=float, default=0.1)
    args = parser.parse_args()

    metrics = evaluate(
        model_dir=args.model,
        pairing_parquet=args.data,
        eval_split=args.eval_split,
    )

    print("\n--- Evaluation Results ---")
    print(f"  MRR:            {metrics['mrr']:.4f}")
    print(f"  Recall@5:       {metrics['recall@5']:.4f}")
    print(f"  Recall@10:      {metrics['recall@10']:.4f}")
    print(f"  Recall@20:      {metrics['recall@20']:.4f}")
    print(f"  Spearman r:     {metrics['spearman_r']:.4f}  (p={metrics['spearman_p']:.2e})")

    # Check against baselines
    ok = True
    if metrics["recall@10"] < 0.6:
        print(f"\n  WARNING: Recall@10 ({metrics['recall@10']:.4f}) below baseline (0.60)")
        ok = False
    if metrics["mrr"] < 0.3:
        print(f"  WARNING: MRR ({metrics['mrr']:.4f}) below baseline (0.30)")
        ok = False
    if ok:
        print("\n  All metrics meet baseline thresholds.")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
