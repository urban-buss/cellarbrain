# Sommelier ML Module

Semantic food-wine pairing using a fine-tuned sentence-transformer model and FAISS vector indexes.

## Prerequisites

- ETL must have been run at least once
- Python 3.11+

## Installation

```bash
pip install "cellarbrain[sommelier]"
```

Installs: `sentence-transformers`, `faiss-cpu`, `datasets`, `accelerate`

## Configuration

In `cellarbrain.toml`:

```toml
[sommelier]
enabled = true
model_dir = "models/sommelier/model"
food_catalogue = "models/sommelier/food_catalogue.parquet"
pairing_dataset = "models/sommelier/pairing_dataset.parquet"
base_model = "models/sommelier/base-model"
```

## Training the Model

```bash
# Default training (10 epochs, batch size 32)
cellarbrain train-model

# Custom parameters
cellarbrain train-model --epochs 15 --batch-size 64

# Output to a different directory
cellarbrain train-model --output /path/to/models
```

Training on Mac Mini M4 (24 GB): ~3–5 minutes CPU-only, ~4–6 GB peak memory. Output saved to `models/sommelier/model/`.

## Building FAISS Indexes

```bash
# Build both food and wine indexes
cellarbrain rebuild-indexes

# Food index only
cellarbrain rebuild-indexes --food-only

# Wine index only (useful after ETL)
cellarbrain rebuild-indexes --wine-only
```

> **Note:** The wine index is automatically rebuilt after each ETL run when `sommelier.enabled = true`.

## Retraining (Incremental)

After adding new pairing data:

```bash
cellarbrain retrain-model
cellarbrain rebuild-indexes
```

## Verifying

Via CLI:

```bash
cellarbrain query "SELECT count(*) FROM wines_stored"
```

Via MCP (start server, then ask an agent): "What wine goes with grilled lamb?" → triggers `suggest_wines` tool.

Via dashboard workbench at `/workbench`:
1. Select `suggest_wines` tool
2. Enter: `food_description = "grilled lamb with rosemary"`
3. Click Execute

## Debugging

### Check Model Status

```bash
python3 -c "
from cellarbrain.sommelier.model import load_model
model = load_model('models/sommelier/model')
print(f'Model loaded: {type(model).__name__}')
print(f'Max seq length: {model.max_seq_length}')
"
```

### Check Index Status

```bash
python3 -c "
import faiss, json
idx = faiss.read_index('models/sommelier/food.index')
print(f'Food index: {idx.ntotal} vectors, dimension {idx.d}')
with open('models/sommelier/food_ids.json') as f:
    ids = json.load(f)
print(f'Food IDs: {len(ids)} entries')
"
```

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: sentence_transformers` | Extra not installed | `pip install "cellarbrain[sommelier]"` |
| `FileNotFoundError: model/` | Model not trained | Run `cellarbrain train-model` |
| `FileNotFoundError: food.index` | Indexes not built | Run `cellarbrain rebuild-indexes` |
| Empty results from `suggest_wines` | Wine index stale | Run `cellarbrain rebuild-indexes --wine-only` |

## Next Steps

- [MCP Server](mcp-server.md) — Sommelier MCP tools (suggest_wines, suggest_foods)
- [Configuration](../configuration/overview.md) — `[sommelier]` settings
