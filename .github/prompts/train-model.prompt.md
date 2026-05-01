---
description: "Train or retrain the sommelier food-wine pairing model, rebuild indexes, and evaluate. Use when: 'train model', 'retrain', 'rebuild indexes', 'evaluate model'."
argument-hint: "Optional: reason for retraining (e.g. 'updated pairing dataset')"
agent: cellarbrain-trainer
---
Run the full sommelier model pipeline:

1. Verify prerequisites (`[sommelier]` extra, base model)
2. Train / retrain the embedding model
3. Rebuild FAISS food and wine indexes
4. Evaluate against baselines and report metrics
5. Run `pytest tests/test_sommelier.py` to validate

{{input}}
