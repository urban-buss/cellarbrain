"""PyArrow schemas for sommelier data artefacts."""

from __future__ import annotations

import pyarrow as pa

FOOD_CATALOGUE_SCHEMA = pa.schema(
    [
        pa.field("dish_id", pa.string(), nullable=False),
        pa.field("dish_name", pa.string(), nullable=False),
        pa.field("description", pa.string(), nullable=False),
        pa.field("ingredients", pa.list_(pa.string()), nullable=False),
        pa.field("cuisine", pa.string(), nullable=False),
        pa.field("weight_class", pa.string(), nullable=False),
        pa.field("protein", pa.string(), nullable=True),
        pa.field("cooking_method", pa.string(), nullable=False),
        pa.field("flavour_profile", pa.list_(pa.string()), nullable=False),
    ]
)

PAIRING_DATASET_SCHEMA = pa.schema(
    [
        pa.field("food_text", pa.string(), nullable=False),
        pa.field("ingredients", pa.list_(pa.string()), nullable=False),
        pa.field("wine_text", pa.string(), nullable=False),
        pa.field("grape", pa.string(), nullable=False),
        pa.field("region", pa.string(), nullable=False),
        pa.field("style", pa.string(), nullable=False),
        pa.field("pairing_score", pa.float64(), nullable=False),
        pa.field("pairing_reason", pa.string(), nullable=False),
    ]
)

# --- Allowed enum values for validation -----------------------------------

WEIGHT_CLASSES = frozenset({"light", "medium", "heavy"})

PROTEINS = frozenset(
    {
        "red_meat",
        "poultry",
        "fish",
        "seafood",
        "pork",
        "game",
        "vegetarian",
        "vegan",
    }
)

COOKING_METHODS = frozenset(
    {
        "braised",
        "grilled",
        "roasted",
        "fried",
        "raw",
        "steamed",
        "baked",
        "smoked",
        "stewed",
        "sautéed",
        "poached",
        "fermented",
        "cured",
        "pickled",
        "no_cook",
        "simmered",
        "pan-fried",
        "stir-fried",
    }
)
