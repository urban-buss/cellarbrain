"""AI Sommelier — embedding-based wine-food pairing retrieval.

Provides two directions:
- Food → Wine: ``suggest_wines(food_query)`` — find cellar wines that pair
  with a given dish or ingredient description.
- Wine → Food: ``suggest_foods(wine_id)`` — find dishes from the food
  catalogue that pair with a specific wine.

The module is optional — cellarbrain works without the ``[sommelier]``
extra installed.  Heavy dependencies (``sentence_transformers``, ``faiss``)
are imported lazily on first use.
"""

from __future__ import annotations
