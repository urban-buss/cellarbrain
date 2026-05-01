# Cellarbrain — OpenClaw Skills

AI sommelier skills for managing a wine cellar via the cellarbrain MCP server. Query your collection, research wines, pair food, track prices, and extract retailer data.

## Prerequisites

- Python 3.11+
- Vinocell CSV exports
- cellarbrain installed: `pip install cellarbrain`
- ETL run at least once:
  ```
  cellarbrain etl raw/export-wines.csv raw/export-bottles-stored.csv raw/export-bottles-gone.csv -o output
  ```

## MCP Server Configuration

Add to your `openclaw.json`:

```json
{
  "mcpServers": {
    "cellarbrain": {
      "command": "cellarbrain",
      "args": ["-d", "/path/to/output", "mcp"],
      "env": {}
    }
  }
}
```

Replace `/path/to/output` with your ETL output directory.

## Skills

| Skill | Description |
|-------|-------------|
| [cellarbrain](./cellarbrain/) | Core sommelier — queries, recommendations, dossier management |
| [cellar-qa](./cellar-qa/) | Q&A workflows for cellar questions, statistics, and occasion picks |
| [wine-research](./wine-research/) | Fact-only wine research populating dossier sections |
| [market-research](./market-research/) | Market pricing and availability research |
| [tracked-research](./tracked-research/) | Companion dossier research for tracked wines |
| [food-pairing](./food-pairing/) | Structured food–wine pairing framework |
| [price-tracking](./price-tracking/) | Swiss retailer price scanning and alerts |
| [shop-extraction](./shop-extraction/) | Per-shop data extraction from 17 Swiss retailers |

## Sommelier Model (Optional)

The `suggest_wines` and `suggest_foods` tools require a trained embedding model. Install ML dependencies and train:

```bash
pip install cellarbrain[sommelier]
cellarbrain train-model
cellarbrain rebuild-indexes
```

If the model is not available, skills fall back to SQL-based search.

## Customisation

Owner context (Switzerland, CHF, German notes, Swiss retailers) is embedded in the skill files. Fork and adapt for your own cellar.
