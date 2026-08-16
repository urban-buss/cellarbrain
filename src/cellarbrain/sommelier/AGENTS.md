# Sommelier Instructions

Sommelier and ML features are optional. Core Cellarbrain commands must remain
usable without model downloads or ML extras. Do not download models, datasets,
or large artifacts without explicit approval.

Use small synthetic fixtures for unit tests, fix random seeds, record model or
embedding assumptions, and distinguish deterministic retrieval behavior from
quality evaluation. Never train on or commit production cellar data.
